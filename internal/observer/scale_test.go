package observer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// Exercises the real queue, workers, topology validation and Store fanout, with
// cached Kubernetes metadata/CA and a controlled 20ms instance-manager RTT.
// It does not substitute for a multi-node gRPC/network capacity test.
func TestScaleActiveClustersAndSubscribers(t *testing.T) {
	const clusters, servicesPerCluster, concurrency = 2000, 10, 128
	o, kube, dyn := fakeObserver(t)
	o.opts.MaxConcurrency = concurrency
	o.configureProbeLimits()
	o.opts.PollInterval = time.Hour // one deterministic refresh round
	o.opts.TTL = 2 * time.Hour
	template, _ := o.cluster(clusterKey{"test", "db"})
	_, pods, _ := fixture()
	var streams []<-chan v1.Snapshot
	var stalled []<-chan v1.Snapshot
	var unsubscribe []func()
	for i := range clusters {
		name := fmt.Sprintf("scale-%04d", i)
		c := template.DeepCopy()
		c.SetName(name)
		c.SetUID(types.UID(name))
		_ = unstructured.SetNestedField(c.Object, name+"-1", "status", "currentPrimary")
		_ = unstructured.SetNestedField(c.Object, name+"-1", "status", "targetPrimary")
		_ = o.clusterInformer.GetIndexer().Add(c)
		o.clusterEvent(nil, c)
		for j := range pods {
			p := pods[j].DeepCopy()
			p.Name = fmt.Sprintf("%s-%d", name, j+1)
			p.UID = types.UID(p.Name)
			p.Labels["cnpg.io/cluster"] = name
			p.OwnerReferences[0].Name = name
			p.OwnerReferences[0].UID = types.UID(name)
			_ = o.podInformer.GetIndexer().Add(p)
		}
		o.connections.Store(clusterKey{"test", name}, cachedConnection{parameters: v1.ConnectionParameters{Database: "app", ServerCAPEM: []byte("cached-public-CA")}, uid: string(c.GetUID()), secret: "db-ca", certificates: `{"serverCASecret":"db-ca"}`, refreshed: time.Now()})
		for j := range servicesPerCluster {
			stream, cancel := o.store.Subscribe("test", name)
			unsubscribe = append(unsubscribe, cancel)
			if j == 0 {
				streams = append(streams, stream)
			} else {
				stalled = append(stalled, stream)
			}
		}
	}
	defer func() {
		for _, cancel := range unsubscribe {
			cancel()
		}
	}()
	if o.queue.Len() != clusters {
		t.Fatalf("subscriber count multiplied work: queue=%d", o.queue.Len())
	}
	var calls, active, peak atomic.Int64
	var promoted atomic.Bool
	changedName := fmt.Sprintf("scale-%04d", clusters-1)
	o.probe = func(ctx context.Context, p *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
		n := active.Add(1)
		defer active.Add(-1)
		calls.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		timer := time.NewTimer(20 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return instanceStatus{}, ctx.Err()
		case <-timer.C:
		}
		primarySuffix := "-1"
		if p.OwnerReferences[0].Name == changedName && promoted.Load() {
			primarySuffix = "-2"
		}
		primary := strings.HasSuffix(p.Name, primarySuffix)
		s := instanceStatus{IsPrimary: &primary, SystemID: "scale", Timeline: 1, WalReceiverActive: !primary}
		if primary {
			name := p.OwnerReferences[0].Name
			for _, suffix := range []string{"-1", "-2", "-3"} {
				if suffix != primarySuffix {
					s.Replication = append(s.Replication, replicationStatus{ApplicationName: name + suffix, State: "streaming", SyncState: "sync"})
				}
			}
		}
		return s, nil
	}
	kube.ClearActions()
	dyn.ClearActions()
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	started := time.Now()
	for range concurrency {
		workers.Go(func() {
			for o.work(ctx) {
			}
		})
	}
	defer func() { cancel(); o.queue.ShutDown(); workers.Wait() }()
	waitFor(t, func() bool { return calls.Load() >= concurrency })
	eventStarted := time.Now()
	promoted.Store(true)
	previous, _ := o.cluster(clusterKey{"test", changedName})
	changed := previous.DeepCopy()
	_ = unstructured.SetNestedField(changed.Object, changedName+"-2", "status", "currentPrimary")
	_ = unstructured.SetNestedField(changed.Object, changedName+"-2", "status", "targetPrimary")
	_ = o.clusterInformer.GetIndexer().Update(changed)
	o.clusterEvent(previous, changed)
	var eventLatency time.Duration
	select {
	case <-streams[clusters-1]: // initial unavailable snapshot, if still buffered
	case <-time.After(time.Second):
		t.Fatal("missing initial state")
	}
	// Subscribe sends an initial state, and the observer replaces it atomically.
	// Read Store as well in case the stream already contained the fresh state.
	waitFor(t, func() bool {
		s, _ := o.store.Get("test", changedName)
		return s.Available && s.PrimaryID == changedName+"-2"
	})
	eventLatency = time.Since(eventStarted)
	probesAtEvent := calls.Load()
	if probesAtEvent >= clusters*3/2 {
		t.Fatalf("event processed after most backlog: probes=%d", probesAtEvent)
	}
	deadline := time.Now().Add(10 * time.Second)
	for i, stream := range streams {
		name := fmt.Sprintf("scale-%04d", i)
		for {
			if s, _ := o.store.Get("test", name); s.Available {
				break
			}
			select {
			case <-stream:
			case <-time.After(time.Until(deadline)):
				t.Fatalf("scale observation timed out at %s", name)
			}
		}
	}
	elapsed := time.Since(started)
	for _, stream := range stalled {
		select {
		case latest := <-stream:
			if !latest.Available || len(stream) != 0 {
				t.Fatal("slow subscriber did not retain exactly the latest snapshot")
			}
		default:
			t.Fatal("missing slow subscriber update")
		}
	}
	// An event can race a worker dequeuing that same key. The queue keeps one
	// dirty follow-up, so allow that one extra cluster observation, never one
	// refresh per service or duplicate event.
	if got := calls.Load(); got < clusters*3 || got > (clusters+1)*3 {
		t.Fatalf("status probes=%d; consumers must share observations", got)
	}
	if peak.Load() > concurrency {
		t.Fatalf("probe limit exceeded: %d", peak.Load())
	}
	if len(kube.Actions()) != 0 || len(dyn.Actions()) != 0 {
		t.Fatal("warm collection used Kubernetes API")
	}
	t.Logf("clusters=%d instances=%d subscriptions=%d simulated_RTT=20ms concurrency=%d peak=%d full_round=%s event_latency=%s probes_at_event=%d Kubernetes_requests=0", clusters, clusters*3, clusters*servicesPerCluster, concurrency, peak.Load(), elapsed, eventLatency, probesAtEvent)
}

func TestObservationDeadlineIncludesWaitingForProbeCapacity(t *testing.T) {
	o, _, _ := fakeObserver(t)
	o.opts.ProbeTimeout = 30 * time.Millisecond
	// All network slots occupied by other clusters; this worker must still exit.
	for range cap(o.probeSlots) {
		o.probeSlots <- struct{}{}
	}
	started := time.Now()
	o.observe(context.Background(), clusterKey{"test", "db"})
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("unbounded collection: %s", elapsed)
	}
}

func TestUnusedTransportAndCAEntriesAreReclaimed(t *testing.T) {
	o, _, _ := fakeObserver(t)
	observe(t, o)
	c, ok := o.connections.Load(clusterKey{"test", "db"})
	if !ok {
		t.Fatal("missing connection")
	}
	if _, err := o.statusClient(c.(cachedConnection).parameters, "db-rw.test.svc"); err != nil {
		t.Fatal(err)
	}
	o.pruneConnections(time.Now().Add(2 * time.Minute))
	if _, ok := o.connections.Load(clusterKey{"test", "db"}); ok {
		t.Fatal("idle CA retained")
	}
	if _, ok := o.statusClients.Load("db-rw.test.svc"); ok {
		t.Fatal("idle TLS transport retained")
	}
}
