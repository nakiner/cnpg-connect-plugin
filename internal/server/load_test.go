//go:build loadtest

package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// TestDiscoveryRecoveryLoad uses independent TCP/TLS clients and the production
// admission/HTTP2 entrypoint. Only topology production is synthetic: it measures
// delivery and transport recovery, not Kubernetes or PostgreSQL observation.
func TestDiscoveryRecoveryLoad(t *testing.T) {
	clients := loadCount(t, "CNPG_LOAD_CLIENTS", 1000, 1, 10000)
	clusters := loadCount(t, "CNPG_LOAD_DATABASES", 100, 1, clients)
	seconds := loadCount(t, "CNPG_LOAD_SECONDS", 120, 6, 1800)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second+2*time.Minute)
	defer cancel()
	ca := newCA(t, "load-test")
	certificate, _, _ := ca.issue(t, 2, x509.ExtKeyUsageServerAuth)
	serverTLS := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: roots}
	store := discovery.NewStore()
	var sequence uint64
	publish := func() uint64 {
		sequence++
		for database := range clusters {
			now := time.Now()
			store.Put(v1.Snapshot{
				Cluster:    v1.ClusterRef{Namespace: "load", Name: fmt.Sprintf("db-%d", database), UID: strconv.Itoa(database)},
				ObservedAt: now, ValidUntil: now.Add(time.Minute), Available: true,
				PrimaryID:  strconv.FormatUint(sequence, 10),
				Connection: v1.ConnectionParameters{ServerCAPEM: bytes.Repeat([]byte("public-ca-fixture"), 128)},
			})
		}
		return sequence
	}
	publish()
	server := startLoadServer(t, store, "127.0.0.1:0", serverTLS, clients)
	defer func() { server.stop(t) }()
	address := server.address
	seen := make([]atomic.Uint64, clients)
	connections := make([]*grpc.ClientConn, 0, clients)
	var workers sync.WaitGroup
	var measuring atomic.Bool
	var latencies deliveryHistogram
	failures := make(chan error, 1)
	defer func() {
		cancel()
		for _, connection := range connections {
			_ = connection.Close()
		}
		workers.Wait()
	}()
	started := time.Now()
	for clientID := range clients {
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS.Clone())))
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, connection)
		workers.Go(func() {
			client := connectv1.NewTopologyServiceClient(connection)
			request := &connectv1.WatchTopologyRequest{Namespace: "load", Name: fmt.Sprintf("db-%d", clientID%clusters)}
			for ctx.Err() == nil {
				stream, err := client.WatchTopology(ctx, request, grpc.WaitForReady(true))
				for err == nil {
					var snapshot *connectv1.Snapshot
					snapshot, err = stream.Recv()
					if err != nil {
						break
					}
					version, parseErr := strconv.ParseUint(snapshot.PrimaryId, 10, 64)
					if parseErr != nil || version < seen[clientID].Load() || !snapshot.Available || snapshot.Cluster.Name != request.Name {
						select {
						case failures <- fmt.Errorf("client %d received invalid or regressing topology: %v", clientID, snapshot):
						default:
						}
						return
					}
					seen[clientID].Store(version)
					if measuring.Load() {
						latencies.observe(time.Since(snapshot.ObservedAt.AsTime()))
					}
				}
				// Load generators reopen interrupted streams on the same ClientConn.
				// Real library backoff and SQL recovery are tested by the live soak.
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(20+clientID%80) * time.Millisecond):
				}
			}
		})
	}
	await := func(version uint64) {
		t.Helper()
		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			remaining := 0
			for i := range seen {
				if seen[i].Load() < version {
					remaining++
				}
			}
			if remaining == 0 {
				return
			}
			select {
			case err := <-failures:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-deadline.C:
				t.Fatalf("%d clients failed to receive version %d within 30s", remaining, version)
			case <-tick.C:
			}
		}
	}
	await(sequence)
	t.Logf("clients=%d databases=%d startup=%s GOMAXPROCS=%d", clients, clusters, time.Since(started), runtime.GOMAXPROCS(0))
	var baselineHeap uint64
	for phase := range 3 {
		if phase > 0 {
			measuring.Store(false)
			started = time.Now()
			server.stop(t)
			time.Sleep(500 * time.Millisecond)
			server = startLoadServer(t, store, address, serverTLS, clients)
			await(publish())
			t.Logf("restart=%d all_clients_recovered=%s", phase, time.Since(started))
		}
		measuring.Store(true)
		end := time.NewTimer(time.Duration(seconds) * time.Second / 3)
		tick := time.NewTicker(200 * time.Millisecond)
	steady:
		for {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case err := <-failures:
				t.Fatal(err)
			case <-tick.C:
				publish()
			case <-end.C:
				break steady
			}
		}
		tick.Stop()
		await(publish())
		measuring.Store(false)
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		if phase == 0 {
			baselineHeap = memory.HeapAlloc
		}
		// Allow caches and transport buffers to settle, but reject large retained
		// growth across full reconnect cycles. This is not an RSS limit or proof
		// against small leaks; record samples for longer runs as well.
		if memory.HeapAlloc > baselineHeap+(64<<20)+uint64(clients)*16384 {
			t.Fatalf("retained heap grew from %d to %d bytes", baselineHeap, memory.HeapAlloc)
		}
		t.Logf("phase=%d heap_after_gc_bytes=%d goroutines=%d sockets=%d watches=%d", phase, memory.HeapAlloc, runtime.NumGoroutine(), server.admission.connections.Load(), len(server.admission.watches))
	}
	cancel()
	for _, connection := range connections {
		_ = connection.Close()
	}
	workers.Wait()
	server.stop(t)
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	t.Logf("delivery_samples=%d p50_upper_ms=%d p95_upper_ms=%d p99_upper_ms=%d max_us=%d", latencies.count.Load(), latencies.percentile(50), latencies.percentile(95), latencies.percentile(99), latencies.maximum.Load())
}

type loadServer struct {
	address   string
	admission *admission
	stopOnce  sync.Once
	close     func()
}

func startLoadServer(t *testing.T, store *discovery.Store, address string, tlsConfig *tls.Config, clients int) *loadServer {
	t.Helper()
	service := discovery.NewServer(store, "")
	ctx, cancel := context.WithCancel(context.Background())
	a, err := newAdmission(ctx, Limits{MaxConnections: clients + 64, MaxWatches: clients + 64, MaxRPCs: clients + 128}, service.Authorize)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	listener = a.trackConnections(netutil.LimitListener(listener, a.limits.MaxConnections))
	gRPC := grpc.NewServer(a.options()...)
	connectv1.RegisterTopologyServiceServer(gRPC, service)
	httpServer := discoveryHTTPServer(a, gRPC, tlsConfig.Clone())
	done := make(chan error, 1)
	go func() { done <- httpServer.ServeTLS(listener, "", "") }()
	return &loadServer{address: listener.Addr().String(), admission: a, close: func() {
		cancel()
		_ = httpServer.Close()
		gRPC.Stop()
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("load server stopped: %v", err)
		}
	}}
}

func (server *loadServer) stop(t *testing.T) {
	t.Helper()
	server.stopOnce.Do(server.close)
	deadline := time.Now().Add(5 * time.Second)
	for server.admission.connections.Load() != 0 || len(server.admission.rpcs) != 0 || len(server.admission.watches) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("server retained connections or RPC admission after shutdown")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func loadCount(t *testing.T, name string, fallback, minimum, maximum int) int {
	t.Helper()
	if value := os.Getenv(name); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < minimum || parsed > maximum {
			t.Fatalf("%s must be between %d and %d", name, minimum, maximum)
		}
		return parsed
	}
	return min(fallback, maximum)
}

// Fixed one-millisecond buckets keep measurement memory constant. Values at or
// above ten seconds go in the last bucket; the maximum remains exact to 1us.
type deliveryHistogram struct {
	buckets [10001]atomic.Uint64
	count   atomic.Uint64
	maximum atomic.Int64
}

func (h *deliveryHistogram) observe(elapsed time.Duration) {
	h.buckets[min(max(elapsed.Milliseconds(), 0), 10000)].Add(1)
	h.count.Add(1)
	for previous := h.maximum.Load(); elapsed.Microseconds() > previous; previous = h.maximum.Load() {
		if h.maximum.CompareAndSwap(previous, elapsed.Microseconds()) {
			break
		}
	}
}

func (h *deliveryHistogram) percentile(percent uint64) int {
	threshold := (h.count.Load()*percent + 99) / 100
	var count uint64
	for bucket := range h.buckets {
		count += h.buckets[bucket].Load()
		if count >= threshold {
			if bucket == len(h.buckets)-1 {
				return int((h.maximum.Load() + 999) / 1000)
			}
			return bucket + 1
		}
	}
	return 10001
}
