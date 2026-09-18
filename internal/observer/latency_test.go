package observer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func TestPromotionCancelsObsoleteCollectionWithoutRetryDelay(t *testing.T) {
	o, _, _ := fakeObserver(t)
	o.opts.ProbeTimeout = 3 * time.Second
	_, unsubscribe := o.store.Subscribe("test", "db")
	defer unsubscribe()

	started := make(chan context.Context, 1)
	var changed atomic.Bool
	o.probe = func(ctx context.Context, pod *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
		if !changed.Load() {
			select {
			case started <- ctx:
			default:
			}
			<-ctx.Done()
			return instanceStatus{}, ctx.Err()
		}
		primary := pod.Name == "db-2"
		status := instanceStatus{IsPrimary: &primary, SystemID: "12345", Timeline: 2, WalReceiverActive: !primary}
		if primary {
			status.Replication = []replicationStatus{
				{ApplicationName: "db-1", State: "streaming", SyncState: "sync"},
				{ApplicationName: "db-3", State: "streaming", SyncState: "async"},
			}
		}
		return status, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		o.work(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })
	var probeContext context.Context
	select {
	case probeContext = <-started:
	case <-time.After(time.Second):
		t.Fatal("old collection did not start")
	}

	old, _ := o.cluster(clusterKey{"test", "db"})
	bookkeeping := old.DeepCopy()
	_ = unstructured.SetNestedField(bookkeeping.Object, "ignored", "status", "phase")
	_ = o.clusterInformer.GetIndexer().Update(bookkeeping)
	o.clusterEvent(old, bookkeeping)
	if probeContext.Err() != nil {
		t.Fatal("unrelated bookkeeping canceled an observation")
	}

	next := bookkeeping.DeepCopy()
	_ = unstructured.SetNestedField(next.Object, "db-2", "status", "currentPrimary")
	_ = unstructured.SetNestedField(next.Object, "db-2", "status", "targetPrimary")
	changed.Store(true)
	promotedAt := time.Now()
	_ = o.clusterInformer.GetIndexer().Update(next)
	o.clusterEvent(bookkeeping, next)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("promotion waited for the obsolete three-second probe timeout")
	}
	if probeContext.Err() == nil {
		t.Fatal("obsolete request was not canceled")
	}
	key := clusterKey{"test", "db"}
	if o.queue.Len() != 1 || o.queue.NumRequeues(key) != 0 {
		t.Fatal("replacement must be queued immediately without failure backoff")
	}
	o.work(context.Background())
	snapshot, _ := o.store.Get("test", "db")
	if !snapshot.Available || snapshot.PrimaryID != "db-2-uid" {
		t.Fatalf("new primary was not verified: %+v", snapshot)
	}
	t.Logf("metadata change to verified topology: %s (obsolete probe timeout: 3s)", time.Since(promotedAt))
}

func TestUrgentCollectionRunsWhileBackgroundWorkersAndProbesAreBusy(t *testing.T) {
	o, _, _ := fakeObserver(t)
	o.opts.MaxConcurrentClusters = 8
	o.opts.MaxConcurrency = 8
	o.opts.ProbeTimeout = 3 * time.Second
	o.configureProbeLimits()
	probe := o.probe
	o.probe = func(ctx context.Context, pod *corev1.Pod, connection v1.ConnectionParameters, serverName string) (instanceStatus, error) {
		if pod.OwnerReferences[0].Name != "db" {
			<-ctx.Done()
			return instanceStatus{}, ctx.Err()
		}
		return probe(ctx, pod, connection, serverName)
	}
	for i := range 7 {
		name := fmt.Sprintf("slow-%d", i)
		addLatencyCluster(t, o, name)
		_, unsubscribe := o.store.Subscribe("test", name)
		defer unsubscribe()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	o.startWorkers(ctx, &workers)
	t.Cleanup(func() {
		cancel()
		o.queue.ShutDown()
		workers.Wait()
	})
	waitFor(t, func() bool {
		o.stateMu.Lock()
		defer o.stateMu.Unlock()
		return len(o.observations) == 7 && len(o.backgroundProbeSlots) == 7
	})
	_, unsubscribe := o.store.Subscribe("test", "db")
	defer unsubscribe()
	eventAt := time.Now()
	o.Notify("test", "db")
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	updates, stop := o.store.Subscribe("test", "db")
	defer stop()
	for {
		select {
		case snapshot := <-updates:
			if snapshot.Available {
				t.Logf("urgent topology while 7 background workers are blocked: %s", time.Since(eventAt))
				return
			}
		case <-deadline.C:
			t.Fatal("urgent collection waited for blocked background work")
		}
	}
}

func addLatencyCluster(t *testing.T, o *Observer, name string) {
	t.Helper()
	cluster, _ := o.cluster(clusterKey{"test", "db"})
	pods := o.pods(cluster)
	cluster.SetName(name)
	cluster.SetUID(types.UID(name))
	_ = unstructured.SetNestedField(cluster.Object, name+"-1", "status", "currentPrimary")
	_ = unstructured.SetNestedField(cluster.Object, name+"-1", "status", "targetPrimary")
	if err := o.clusterInformer.GetIndexer().Add(cluster); err != nil {
		t.Fatal(err)
	}
	for i := range pods {
		pod := &pods[i]
		pod.Name = fmt.Sprintf("%s-%d", name, i+1)
		pod.UID = types.UID(pod.Name)
		pod.Labels["cnpg.io/cluster"] = name
		pod.OwnerReferences[0].Name = name
		pod.OwnerReferences[0].UID = cluster.GetUID()
		if err := o.podInformer.GetIndexer().Add(pod); err != nil {
			t.Fatal(err)
		}
	}
	o.clusterEvent(nil, cluster)
}

func TestCanceledProbeAcquisitionReturnsCapacity(t *testing.T) {
	o, _, _ := fakeObserver(t)
	background, err := o.acquireProbe(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	urgent, err := o.acquireProbe(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		release, err := o.acquireProbe(ctx, false)
		if release != nil {
			release()
		}
		done <- err
	}()
	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled background probe acquired capacity")
	}
	background()
	urgent()
	if len(o.backgroundProbeSlots) != 0 || len(o.probeSlots) != 0 {
		t.Fatal("probe capacity leaked")
	}
}

func TestUrgentVerificationRetainsPriorityAcrossRetry(t *testing.T) {
	o, _, _ := fakeObserver(t)
	_, unsubscribe := o.store.Subscribe("test", "db")
	defer unsubscribe()
	o.Notify("test", "db")
	o.probe = func(context.Context, *corev1.Pod, v1.ConnectionParameters, string) (instanceStatus, error) {
		return instanceStatus{}, errors.New("promotion still in progress")
	}
	o.work(context.Background())
	done := make(chan clusterKey, 1)
	go func() {
		key, shutdown := o.priority.GetUrgent()
		if !shutdown {
			done <- key
			o.queue.Done(key)
		}
	}()
	select {
	case key := <-done:
		if key != (clusterKey{"test", "db"}) {
			t.Fatalf("wrong urgent retry: %v", key)
		}
	case <-time.After(time.Second):
		t.Fatal("promotion retry lost urgent worker eligibility")
	}
}
