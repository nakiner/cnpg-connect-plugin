package observer

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// applicationDatabase follows CNPG bootstrap metadata, including restores.
func applicationDatabase(cluster *unstructured.Unstructured) string {
	for _, method := range []string{"recovery", "pg_basebackup", "initdb"} {
		name, _, _ := unstructured.NestedString(cluster.Object, "spec", "bootstrap", method, "database")
		if name != "" {
			return name
		}
	}
	return ""
}

func serverCASecret(cluster *unstructured.Unstructured) string {
	name, _, _ := unstructured.NestedString(cluster.Object, "status", "certificates", "serverCASecret")
	return name
}

// CA contents are shared by Secret identity and version, never by a timer.
// Each observation retains its exact entry so publication can reject a rotation
// or deletion that happened while PostgreSQL was being probed.
type cachedCA struct {
	key             types.NamespacedName
	uid             types.UID
	resourceVersion string
	public          []byte
}

type connectionInfo struct {
	v1.ConnectionParameters
	ca *cachedCA
}

func (o *Observer) cachedConnection(ctx context.Context, cluster *unstructured.Unstructured) (connectionInfo, error) {
	if err := ctx.Err(); err != nil {
		return connectionInfo{}, err
	}
	key := types.NamespacedName{Namespace: cluster.GetNamespace(), Name: serverCASecret(cluster)}
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	if ca := o.cas[key]; o.currentCA(ca) {
		o.caHits.Add(1)
		return connectionInfo{ConnectionParameters: v1.ConnectionParameters{
			Database: applicationDatabase(cluster), ServerCAPEM: append([]byte(nil), ca.public...),
		}, ca: ca}, nil
	}
	o.caMisses.Add(1)
	if o.secretMetadata(key) == nil {
		o.caFailures.Add(1)
		return connectionInfo{}, fmt.Errorf("PostgreSQL server CA Secret is not available")
	}
	if !o.caPending[key] {
		o.caPending[key] = true
		o.caQueue.Add(key)
	}
	return connectionInfo{}, fmt.Errorf("PostgreSQL server CA is awaiting observation")
}

// currentCA is called with stateMu held. Reading the informer as well as the
// cache closes the gap between its store update and event-handler delivery.
func (o *Observer) currentCA(ca *cachedCA) bool {
	if ca == nil || o.cas[ca.key] != ca {
		return false
	}
	meta := o.secretMetadata(ca.key)
	return meta != nil && meta.UID == ca.uid && meta.ResourceVersion == ca.resourceVersion
}

func (o *Observer) secretMetadata(key types.NamespacedName) *metav1.PartialObjectMetadata {
	obj, exists, _ := o.secretInformer.GetIndexer().GetByKey(key.String())
	if !exists {
		return nil
	}
	return obj.(*metav1.PartialObjectMetadata)
}

func (o *Observer) clustersUsingCA(key types.NamespacedName) []*unstructured.Unstructured {
	objects, _ := o.clusterInformer.GetIndexer().ByIndex(serverCAIndex, key.String())
	clusters := make([]*unstructured.Unstructured, len(objects))
	for i, obj := range objects {
		clusters[i] = obj.(*unstructured.Unstructured)
	}
	return clusters
}

func (o *Observer) demandedCAClusters(key types.NamespacedName) []*unstructured.Unstructured {
	var clusters []*unstructured.Unstructured
	for _, c := range o.clustersUsingCA(key) {
		if o.store.HasDemand(c.GetNamespace(), c.GetName()) {
			clusters = append(clusters, c)
		}
	}
	return clusters
}

// The metadata informer never receives Secret data. Only referenced, demanded
// CAs enter the fetch queue; unrelated Secret events allocate no retained state.
func (o *Observer) secretEvent(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	meta, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return
	}
	key := types.NamespacedName{Namespace: meta.Namespace, Name: meta.Name}
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	if o.currentCA(o.cas[key]) {
		return // Resync, or a late event for an already replaced Secret.
	}
	delete(o.cas, key)
	if cancel := o.caReads[key]; cancel != nil {
		cancel()
	}
	if o.caPending[key] {
		o.caQueue.Add(key)
	}
	for _, c := range o.clustersUsingCA(key) {
		cluster := clusterKey{c.GetNamespace(), c.GetName()}
		o.cancelObservationLocked(cluster)
		if snapshot, exists := o.store.Get(cluster.namespace, cluster.name); exists && (snapshot.Available || len(snapshot.Connection.ServerCAPEM) != 0) {
			o.publish(unavailable(c, time.Now().UTC(), o.opts.TTL, "connection_defaults_changed"), time.Time{})
		}
		o.Notify(cluster.namespace, cluster.name)
	}
}

func (o *Observer) startCAWorkers(ctx context.Context, workers *sync.WaitGroup) {
	// Both queues use a bounded number of workers. A missing CA must not occupy
	// PostgreSQL probe capacity while client-go waits for its API rate limiter.
	context.AfterFunc(ctx, o.caQueue.ShutDown)
	for range min(32, o.opts.MaxConcurrentClusters) {
		workers.Go(func() {
			for o.workCA(ctx) {
			}
		})
	}
}

func (o *Observer) workCA(ctx context.Context) bool {
	key, shutdown := o.caQueue.Get()
	if shutdown {
		return false
	}
	defer o.caQueue.Done(key)
	if ctx.Err() != nil {
		return false
	}
	if !o.Ready() {
		o.caQueue.AddAfter(key, o.opts.PollInterval)
		return true
	}
	if o.fetchCA(ctx, key) {
		o.caQueue.Forget(key)
	} else {
		o.caQueue.AddRateLimited(key)
	}
	return true
}

func (o *Observer) fetchCA(ctx context.Context, key types.NamespacedName) bool {
	o.stateMu.Lock()
	meta := o.secretMetadata(key)
	if meta == nil || len(o.demandedCAClusters(key)) == 0 || o.currentCA(o.cas[key]) {
		delete(o.caPending, key)
		o.stateMu.Unlock()
		return true
	}
	requestCtx, cancel := context.WithTimeout(ctx, o.opts.ProbeTimeout)
	o.caReads[key] = cancel
	o.stateMu.Unlock()
	defer cancel()
	ca, err := o.readPublicCA(requestCtx, key.Namespace, key.Name)
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	delete(o.caReads, key)
	if err != nil {
		o.caFailures.Add(1)
		return false
	}
	current := o.secretMetadata(key)
	if requestCtx.Err() != nil || !o.Ready() || current == nil ||
		current.UID != meta.UID || current.ResourceVersion != meta.ResourceVersion ||
		ca.uid != current.UID || ca.resourceVersion != current.ResourceVersion {
		return false
	}
	clusters := o.demandedCAClusters(key)
	if len(clusters) == 0 {
		delete(o.caPending, key)
		return true
	}
	o.cas[key] = ca
	delete(o.caPending, key)
	for _, c := range clusters {
		o.Notify(c.GetNamespace(), c.GetName())
	}
	return true
}

func (o *Observer) readPublicCA(ctx context.Context, namespace, name string) (result *cachedCA, resultErr error) {
	defer func() {
		if resultErr != nil {
			o.caReadFailures.Add(1)
		}
	}()
	secret, err := o.kube.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read PostgreSQL server CA: %w", err)
	}
	ca := &cachedCA{key: types.NamespacedName{Namespace: namespace, Name: name}, uid: secret.UID, resourceVersion: secret.ResourceVersion}
	remaining := secret.Data["ca.crt"]
	for len(remaining) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil {
			break
		}
		remaining = rest
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return nil, fmt.Errorf("invalid PostgreSQL server CA certificate")
		}
		ca.public = append(ca.public, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes})...)
	}
	if len(ca.public) == 0 {
		return nil, fmt.Errorf("PostgreSQL server CA has no public certificates")
	}
	return ca, nil
}
