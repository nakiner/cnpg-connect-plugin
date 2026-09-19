//go:build loadtest

package observer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// This supplements the warm fleet test with actual client-go rate limiting and
// public-CA GETs against a local HTTP API. Metadata is already synced, responses
// are synthetic, and subscribers use Store channels rather than real gRPC.
func TestColdStartFleet(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		count  int
		shared bool
	}{
		{name: "500_unique_CA", count: 500},
		{name: "1000_unique_CA", count: 1000},
		{name: "500_shared_CA", count: 500, shared: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			runColdStartFleet(t, scenario.count, scenario.shared)
		})
	}
}

func runColdStartFleet(t *testing.T, count int, shared bool) {
	const qps, burst = 100, 200
	const apiLatency, statusLatency = 20 * time.Millisecond, 20 * time.Millisecond
	o, _, _ := fakeObserver(t)
	o.opts.PollInterval = 5 * time.Second
	o.opts.TTL = 15 * time.Second
	o.opts.ProbeTimeout = 2 * time.Second
	o.opts.MaxConcurrentClusters = 32
	o.opts.MaxConcurrency = 128
	o.configureProbeLimits()
	public := testPublicCA(t)
	var requests, activeRequests, peakRequests atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/test/secrets/cold-") {
			t.Errorf("unexpected API request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected API request", http.StatusBadRequest)
			return
		}
		requests.Add(1)
		active := activeRequests.Add(1)
		defer activeRequests.Add(-1)
		for previous := peakRequests.Load(); active > previous; previous = peakRequests.Load() {
			if peakRequests.CompareAndSwap(previous, active) {
				break
			}
		}
		timer := time.NewTimer(apiLatency)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
		w.Header().Set("Content-Type", "application/json")
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/test/secrets/")
		_ = json.NewEncoder(w).Encode(corev1.Secret{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: name, UID: types.UID(name), ResourceVersion: "1"},
			Data:       map[string][]byte{"ca.crt": public},
		})
	}))
	defer api.Close()
	var err error
	o.kube, err = kubernetes.NewForConfig(&rest.Config{Host: api.URL, QPS: qps, Burst: burst})
	if err != nil {
		t.Fatal(err)
	}
	o.probe = func(ctx context.Context, pod *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
		timer := time.NewTimer(statusLatency)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return instanceStatus{}, ctx.Err()
		case <-timer.C:
		}
		return fleetStatus(pod, "-1", "cold"), nil
	}
	var subscriptions []<-chan v1.Snapshot
	var cancels []func()
	for i := range count {
		name := fmt.Sprintf("cold-%04d", i)
		secret := name + "-ca"
		if shared {
			secret = "cold-shared-ca"
		}
		cluster, _, _ := addFleetCluster(t, o, name, secret)
		if err := o.secretInformer.GetIndexer().Add(&metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
			Namespace: "test", Name: secret, UID: types.UID(secret), ResourceVersion: "1",
		}}); err != nil {
			t.Fatal(err)
		}
		o.clusterEvent(nil, cluster)
		stream, cancel := o.store.Subscribe("test", name)
		subscriptions = append(subscriptions, stream)
		cancels = append(cancels, cancel)
	}
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	var workers, readers sync.WaitGroup
	defer func() {
		cancel()
		o.queue.ShutDown()
		workers.Wait()
		readers.Wait()
	}()
	ready := make(chan time.Duration, count)
	started := time.Now()
	for _, stream := range subscriptions {
		readers.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case snapshot := <-stream:
					if snapshot.Available {
						ready <- time.Since(started)
						return
					}
				}
			}
		})
	}
	o.startWorkers(ctx, &workers)
	latencies := make([]time.Duration, 0, count)
	for len(latencies) < count {
		select {
		case latency := <-ready:
			latencies = append(latencies, latency)
		case <-ctx.Done():
			t.Fatalf("only %d/%d databases became available: %v", len(latencies), count, ctx.Err())
		}
	}
	elapsed := time.Since(started)
	if !shared && requests.Load() != int64(count) {
		t.Fatalf("unique CA reads=%d, want one per database (%d)", requests.Load(), count)
	}
	if shared && requests.Load() != 1 {
		t.Fatalf("shared CA reads=%d, want one per Secret version", requests.Load())
	}
	if peakRequests.Load() > int64(o.opts.MaxConcurrentClusters) {
		t.Fatalf("CA request concurrency=%d exceeds worker bound", peakRequests.Load())
	}
	if !shared && float64(requests.Load()) > burst+qps*elapsed.Seconds()+1 {
		t.Fatal("requests exceeded the configured client-go API budget")
	}
	t.Logf("databases=%d shared_CA=%t kube_qps=%d kube_burst=%d API_RTT=%s status_RTT=%s CA_GETs=%d peak_CA_requests=%d cold_to_available_p50=%s p95=%s max=%s elapsed=%s",
		count, shared, qps, burst, apiLatency, statusLatency, requests.Load(), peakRequests.Load(),
		percentile(latencies, .5), percentile(latencies, .95), percentile(latencies, 1), elapsed)
}
