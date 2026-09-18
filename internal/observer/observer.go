// Package observer reads CloudNativePG topology without participating in
// promotion, fencing, replication configuration, or database writes.
package observer

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

var clusterResource = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"}

type Options struct {
	Namespace      string
	PollInterval   time.Duration
	TTL            time.Duration
	ProbeTimeout   time.Duration
	MaxConcurrency int
}

type Observer struct {
	kube            kubernetes.Interface
	dynamic         dynamic.Interface
	store           *discovery.Store
	opts            Options
	log             *slog.Logger
	clusterInformer cache.SharedIndexInformer
	podInformer     cache.SharedIndexInformer
	wake            chan struct{}
	ready           atomic.Bool
	known           map[string]v1.ClusterRef
	probeSlots      chan struct{}
	probe           func(context.Context, kubernetes.Interface, *corev1.Pod) (instanceStatus, error)
}

func New(kube kubernetes.Interface, dyn dynamic.Interface, store *discovery.Store, opts Options, logger *slog.Logger) (*Observer, error) {
	if kube == nil || dyn == nil || store == nil {
		return nil, fmt.Errorf("kubernetes clients and store are required")
	}
	if opts.PollInterval <= 0 || opts.ProbeTimeout <= 0 || opts.TTL <= opts.PollInterval+opts.ProbeTimeout {
		return nil, fmt.Errorf("ttl must exceed positive poll interval plus probe timeout")
	}
	if opts.MaxConcurrency < 1 || opts.MaxConcurrency > 1024 {
		return nil, fmt.Errorf("max concurrency must be between 1 and 1024")
	}
	if logger == nil {
		logger = slog.Default()
	}
	o := &Observer{kube: kube, dynamic: dyn, store: store, opts: opts, log: logger, wake: make(chan struct{}, 1), known: map[string]v1.ClusterRef{}, probeSlots: make(chan struct{}, opts.MaxConcurrency), probe: readStatus}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, 0, opts.Namespace, nil)
	o.clusterInformer = factory.ForResource(clusterResource).Informer()
	o.podInformer = coreinformers.NewFilteredPodInformer(kube, opts.Namespace, 0, cache.Indexers{}, func(options *metav1.ListOptions) { options.LabelSelector = "cnpg.io/cluster" })
	handlers := cache.ResourceEventHandlerFuncs{AddFunc: func(any) { o.Notify() }, UpdateFunc: func(old, new any) {
		if relevantChange(old, new) {
			o.Notify()
		}
	}, DeleteFunc: func(any) { o.Notify() }}
	if _, err := o.clusterInformer.AddEventHandler(handlers); err != nil {
		return nil, err
	}
	if _, err := o.podInformer.AddEventHandler(handlers); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Observer) Notify() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}
func (o *Observer) Ready() bool { return o.ready.Load() }

func (o *Observer) Run(ctx context.Context) error {
	defer o.ready.Store(false)
	var background sync.WaitGroup
	start := func(f func()) { background.Add(1); go func() { defer background.Done(); f() }() }
	start(func() { o.clusterInformer.Run(ctx.Done()) })
	start(func() { o.podInformer.Run(ctx.Done()) })
	start(func() {
		ticker := time.NewTicker(min(time.Second, o.opts.TTL/4))
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				o.store.Expire(now)
			case <-ctx.Done():
				return
			}
		}
	})
	defer background.Wait()
	if !cache.WaitForCacheSync(ctx.Done(), o.clusterInformer.HasSynced, o.podInformer.HasSynced) {
		return nil
	}
	refresh := func() {
		if err := o.collect(ctx); err != nil {
			if ctx.Err() == nil {
				o.log.Warn("topology refresh failed", "error", err)
			}
		} else {
			o.ready.Store(true)
		}
	}
	refresh()
	ticker := time.NewTicker(o.opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			refresh()
		case <-o.wake:
			// Batch watch bursts. Hooks and watches only enqueue work; none block
			// the operator's reconciliation while instance probes run.
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
			select {
			case <-o.wake:
			default:
			}
			refresh()
		}
	}
}

func (o *Observer) collect(ctx context.Context) error {
	listCtx, cancel := context.WithTimeout(ctx, o.opts.ProbeTimeout)
	list, err := o.dynamic.Resource(clusterResource).Namespace(o.opts.Namespace).List(listCtx, metav1.ListOptions{})
	cancel()
	if err != nil {
		return fmt.Errorf("list CNPG clusters: %w", err)
	}
	// Watches reduce latency, but fresh API reads establish every observation.
	// A disconnected informer cache must not perpetually renew old topology.
	active := map[string]v1.ClusterRef{}
	var wg sync.WaitGroup
	clusterSlots := make(chan struct{}, o.opts.MaxConcurrency)
	for i := range list.Items {
		cluster := list.Items[i].DeepCopy()
		enabled, params, paramErr := parameters(cluster)
		if !enabled || cluster.GetDeletionTimestamp() != nil {
			continue
		}
		ref := clusterRef(cluster)
		active[ref.Namespace+"/"+ref.Name] = ref
		if paramErr != nil {
			o.log.Warn("invalid cluster plugin parameters", "namespace", ref.Namespace, "cluster", ref.Name, "error", paramErr)
			o.store.Put(unavailable(cluster, time.Now().UTC(), o.opts.TTL, "invalid_configuration"))
			continue
		}
		select {
		case clusterSlots <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}
		wg.Add(1)
		go func() { defer wg.Done(); defer func() { <-clusterSlots }(); o.collectCluster(ctx, cluster, params) }()
	}
	for key, old := range o.known {
		if _, ok := active[key]; !ok {
			o.store.Delete(old.Namespace, old.Name)
		}
	}
	o.known = active
	wg.Wait()
	return ctx.Err()
}

func (o *Observer) listPods(ctx context.Context, cluster *unstructured.Unstructured) ([]corev1.Pod, error) {
	requestCtx, cancel := context.WithTimeout(ctx, o.opts.ProbeTimeout)
	defer cancel()
	list, err := o.kube.CoreV1().Pods(cluster.GetNamespace()).List(requestCtx, metav1.ListOptions{LabelSelector: "cnpg.io/cluster=" + cluster.GetName()})
	if err != nil {
		return nil, err
	}
	return ownedPods(cluster, list), nil
}

func (o *Observer) collectCluster(ctx context.Context, cluster *unstructured.Unstructured, params config.Parameters) {
	at := time.Now().UTC()
	ctx, cancel := context.WithTimeout(ctx, o.opts.TTL-o.opts.PollInterval)
	defer cancel()
	publishFailure := func(reason string) { o.store.Put(unavailable(cluster, at, o.opts.TTL, reason)) }
	pods, err := o.listPods(ctx, cluster)
	if err != nil {
		publishFailure("kubernetes_unavailable")
		return
	}
	connection, err := o.connectionParameters(ctx, cluster)
	if err != nil {
		o.log.Warn("connection defaults unavailable", "namespace", cluster.GetNamespace(), "cluster", cluster.GetName(), "error", err)
		publishFailure("connection_defaults_unavailable")
		return
	}
	results := make([]statusResult, len(pods))
	var probes sync.WaitGroup
	for i := range pods {
		i := i
		if !podReady(pods[i]) {
			results[i].err = fmt.Errorf("pod not ready")
			continue
		}
		probes.Add(1)
		go func() {
			defer probes.Done()
			select {
			case o.probeSlots <- struct{}{}:
			case <-ctx.Done():
				results[i].err = ctx.Err()
				return
			}
			defer func() { <-o.probeSlots }()
			probeCtx, probeCancel := context.WithTimeout(ctx, o.opts.ProbeTimeout)
			defer probeCancel()
			results[i].status, results[i].err = o.probe(probeCtx, o.kube, &pods[i])
		}()
	}
	probes.Wait()
	requestCtx, requestCancel := context.WithTimeout(ctx, o.opts.ProbeTimeout)
	latest, err := o.dynamic.Resource(clusterResource).Namespace(cluster.GetNamespace()).Get(requestCtx, cluster.GetName(), metav1.GetOptions{})
	requestCancel()
	if err != nil {
		publishFailure("kubernetes_unavailable")
		return
	}
	current, target := primaryNames(cluster)
	newCurrent, newTarget := primaryNames(latest)
	enabled, latestParams, paramErr := parameters(latest)
	if !enabled || paramErr != nil || !reflect.DeepEqual(params, latestParams) || latest.GetUID() != cluster.GetUID() || latest.GetGeneration() != cluster.GetGeneration() || latest.GetDeletionTimestamp() != nil || current != newCurrent || target != newTarget || latest.GetAnnotations()["cnpg.io/fencedInstances"] != cluster.GetAnnotations()["cnpg.io/fencedInstances"] || serverCASecret(latest) != serverCASecret(cluster) {
		publishFailure("topology_changed_during_observation")
		o.Notify()
		return
	}
	latestPods, err := o.listPods(ctx, cluster)
	if err != nil {
		publishFailure("kubernetes_unavailable")
		return
	}
	if !sameMembers(pods, latestPods) {
		publishFailure("membership_changed_during_observation")
		o.Notify()
		return
	}
	snapshot := buildSnapshot(cluster, pods, results, params, at, o.opts.TTL)
	snapshot.Connection = connection
	o.store.Put(snapshot)
}

func sameMembers(a, b []corev1.Pod) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].UID != b[i].UID || a[i].Status.PodIP != b[i].Status.PodIP || podReady(a[i]) != podReady(b[i]) || a[i].Spec.NodeName != b[i].Spec.NodeName || !reflect.DeepEqual(a[i].Labels, b[i].Labels) || !reflect.DeepEqual(a[i].Status.ContainerStatuses, b[i].Status.ContainerStatuses) {
			return false
		}
	}
	return true
}

func relevantChange(old, new any) bool {
	switch a := old.(type) {
	case *corev1.Pod:
		b, ok := new.(*corev1.Pod)
		return !ok || !sameMembers([]corev1.Pod{*a}, []corev1.Pod{*b})
	case *unstructured.Unstructured:
		b, ok := new.(*unstructured.Unstructured)
		if !ok {
			return true
		}
		ap, at := primaryNames(a)
		bp, bt := primaryNames(b)
		return a.GetUID() != b.GetUID() || a.GetGeneration() != b.GetGeneration() || ap != bp || at != bt || serverCASecret(a) != serverCASecret(b) || !reflect.DeepEqual(a.GetAnnotations(), b.GetAnnotations()) || !reflect.DeepEqual(a.GetDeletionTimestamp(), b.GetDeletionTimestamp())
	default:
		return true
	}
}
