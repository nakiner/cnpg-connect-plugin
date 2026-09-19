//go:build loadtest

package observer

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestFleetLoad is opt-in: go test -tags=loadtest -run TestFleetLoad -v ./internal/observer.
// It includes gRPC serialization and 6,000 live streams over an in-memory
// transport. Kubernetes metadata/CA are warm, and instance I/O is a 20ms fake.
// Use the compiled test in a CPU-limited Linux container to measure scheduling
// under actual cgroups; GOMAXPROCS alone does not simulate CPU throttling.
func TestFleetLoad(t *testing.T) {
	for _, interval := range []time.Duration{5 * time.Second, 100 * time.Millisecond} {
		t.Run(interval.String(), func(t *testing.T) { runFleetLoad(t, interval) })
	}
}

func runFleetLoad(t *testing.T, interval time.Duration) {
	const databases, concurrency, changes = 6000, 128, 100
	const responseTime = 20 * time.Millisecond
	slowBackground := os.Getenv("CNPG_LOAD_SLOW_BACKGROUND") == "1"
	o, kube, dynamic := fakeObserver(t)
	workersCount := o.opts.MaxConcurrentClusters
	if configured := os.Getenv("CNPG_LOAD_WORKERS"); configured != "" {
		var err error
		workersCount, err = strconv.Atoi(configured)
		if err != nil || workersCount < 1 {
			t.Fatal("invalid CNPG_LOAD_WORKERS")
		}
	}
	o.opts.PollInterval = interval
	o.opts.MaxConcurrency = concurrency
	o.opts.MaxConcurrentClusters = workersCount
	o.opts.TTL = 15 * time.Second
	o.opts.ProbeTimeout = 2 * time.Second
	o.configureProbeLimits()
	ca := testConnection(t, o).ServerCAPEM
	var promoted sync.Map
	var probes atomic.Int64
	o.probe = func(ctx context.Context, pod *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
		probes.Add(1)
		name := pod.OwnerReferences[0].Name
		_, changed := promoted.Load(name)
		delay := responseTime
		if slowBackground && !changed {
			// Ordinary requests hit their timeout. A changed cluster becomes
			// healthy, so urgent work must escape the blocked background fleet.
			delay = 10 * time.Second
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return instanceStatus{}, ctx.Err()
		case <-timer.C:
		}
		_, changed = promoted.Load(name)
		primarySuffix := "-1"
		if changed {
			primarySuffix = "-2"
		}
		return fleetStatus(pod, primarySuffix, "fleet"), nil
	}

	names := make([]string, databases)
	for i := range databases {
		name := fmt.Sprintf("fleet-%04d", i)
		names[i] = name
		cluster, pods, results := addFleetCluster(t, o, name, "db-ca")
		parameters := v1.ConnectionParameters{Database: "app", ServerCAPEM: ca}
		snapshot := buildSnapshot(cluster, pods, results, config.Parameters{}, time.Now(), o.opts.TTL)
		snapshot.Connection = parameters
		o.store.Put(snapshot)
	}
	kube.ClearActions()
	dynamic.ClearActions()

	listener := bufconn.Listen(256 << 10)
	server := grpc.NewServer()
	connectv1.RegisterTopologyServiceServer(server, discovery.NewServer(o.store, ""))
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	ctx, cancel := context.WithCancel(context.Background())
	connection, err := grpc.NewClient("passthrough:///fleet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	var readers, workers sync.WaitGroup
	defer func() {
		cancel()
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
		readers.Wait()
		o.queue.ShutDown()
		workers.Wait()
		<-serverDone
	}()
	client := connectv1.NewTopologyServiceClient(connection)
	metrics := &fleetMetrics{
		starts:          make(map[string]time.Time),
		lastObservation: make(map[string]time.Time),
		latencies:       make(chan time.Duration, changes),
	}
	for _, name := range names {
		stream, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "test", Name: name})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = stream.Recv(); err != nil {
			t.Fatal(err)
		}
		readers.Go(func() {
			for {
				snapshot, err := stream.Recv()
				if err != nil {
					return
				}
				metrics.record(snapshot)
			}
		})
	}
	if o.queue.Len() != databases {
		t.Fatalf("pending clusters=%d", o.queue.Len())
	}
	started := time.Now()
	o.startWorkers(ctx, &workers)
	time.Sleep(250 * time.Millisecond)
	for i := range changes {
		// Change distinct databases throughout the existing background backlog.
		name := names[(databases-1-i*53)%databases]
		previous, _ := o.cluster(clusterKey{"test", name})
		next := previous.DeepCopy()
		_ = unstructured.SetNestedField(next.Object, name+"-2", "status", "currentPrimary")
		_ = unstructured.SetNestedField(next.Object, name+"-2", "status", "targetPrimary")
		metrics.begin(name)
		promoted.Store(name, true)
		_ = o.clusterInformer.GetIndexer().Update(next)
		o.clusterEvent(previous, next)
		time.Sleep(10 * time.Millisecond)
	}
	latencies := make([]time.Duration, 0, changes)
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for len(latencies) < changes {
		select {
		case latency := <-metrics.latencies:
			latencies = append(latencies, latency)
		case <-deadline.C:
			t.Fatalf("received %d/%d changes", len(latencies), changes)
		}
	}
	// Observe an entire busy refresh period, not just the urgent requests.
	minimumRun := started.Add(7 * time.Second)
	if remaining := time.Until(minimumRun); remaining > 0 {
		time.Sleep(remaining)
	}
	elapsed := time.Since(started)
	metrics.mu.Lock()
	gaps := append([]time.Duration(nil), metrics.observationGaps...)
	metrics.mu.Unlock()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf("databases=%d instances=%d grpc_streams=%d GOMAXPROCS=%d "+
		"interval=%s simulated_status_RTT=%s slow_background=%t concurrency=%d cluster_workers=%d "+
		"observations_of_changes=%d event_to_grpc_p50=%s p95=%s p99=%s max=%s "+
		"refresh_gap_p50=%s p95=%s probes=%d probes_per_second=%.0f elapsed=%s heap_MiB=%.1f",
		databases, databases*3, databases, runtime.GOMAXPROCS(0),
		interval, responseTime, slowBackground, concurrency, workersCount,
		len(latencies), percentile(latencies, .5), percentile(latencies, .95),
		percentile(latencies, .99), percentile(latencies, 1),
		percentile(gaps, .5), percentile(gaps, .95), probes.Load(),
		float64(probes.Load())/elapsed.Seconds(), elapsed, float64(memory.HeapAlloc)/(1<<20),
	)
	if len(kube.Actions()) != 0 || len(dynamic.Actions()) != 0 {
		t.Fatal("warm observation used Kubernetes API")
	}
}

type fleetMetrics struct {
	mu              sync.Mutex
	starts          map[string]time.Time
	lastObservation map[string]time.Time
	observationGaps []time.Duration
	latencies       chan time.Duration
}

func (m *fleetMetrics) begin(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.starts[name] = time.Now()
}

func (m *fleetMetrics) record(snapshot *connectv1.Snapshot) {
	if !snapshot.Available {
		return
	}
	name := snapshot.Cluster.Name
	m.mu.Lock()
	defer m.mu.Unlock()
	at := snapshot.ObservedAt.AsTime()
	if previous, exists := m.lastObservation[name]; exists && at.After(previous) {
		m.observationGaps = append(m.observationGaps, at.Sub(previous))
	}
	m.lastObservation[name] = at
	if started, exists := m.starts[name]; exists && snapshot.PrimaryId == name+"-2" {
		m.latencies <- time.Since(started)
		delete(m.starts, name)
	}
}

func percentile(values []time.Duration, fraction float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	slices.Sort(sorted)
	return sorted[int(float64(len(sorted)-1)*fraction)]
}
