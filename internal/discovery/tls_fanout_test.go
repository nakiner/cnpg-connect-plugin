package discovery

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type loopbackDiscovery struct {
	store       *Store
	address     string
	credentials credentials.TransportCredentials
}

func startTLSDiscovery(t *testing.T, service func(*Store) connectv1.TopologyServiceServer) loopbackDiscovery {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.SharedWriteBuffer(true), grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	})))
	store := NewStore()
	connectv1.RegisterTopologyServiceServer(server, service(store))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return loopbackDiscovery{store: store, address: listener.Addr().String(), credentials: credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})}
}

func (server loopbackDiscovery) connect(t *testing.T) (connectv1.TopologyServiceClient, func()) {
	t.Helper()
	connection, err := grpc.NewClient(server.address,
		grpc.WithTransportCredentials(server.credentials.Clone()),
		grpc.WithSharedWriteBuffer(true),
		grpc.WithStaticStreamWindowSize(64<<10), grpc.WithStaticConnWindowSize(64<<10))
	if err != nil {
		t.Fatal(err)
	}
	return connectv1.NewTopologyServiceClient(connection), func() { _ = connection.Close() }
}

func TestTLSIndependentClientsFanoutAndReconnect(t *testing.T) {
	runTLSFanout(t, 32, 10)
}

// Each client has its own TCP socket, TLS session, HTTP/2 transport, and stream.
// Timing covers Store.Put through client protobuf receipt, not CNPG observation.
func runTLSFanout(t *testing.T, clients, rounds int) {
	t.Helper()
	server := startTLSDiscovery(t, func(store *Store) connectv1.TopologyServiceServer { return NewServer(store, "") })
	snapshot := sampleSnapshot()
	snapshot.Connection.ServerCAPEM = bytes.Repeat([]byte("public-ca-fixture"), 128)
	server.store.Put(snapshot)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	type receipt struct {
		primary string
		delay   time.Duration
		err     error
	}
	receipts := make(chan receipt, clients)
	var workers sync.WaitGroup
	closers := make([]func(), 0, clients)
	defer func() {
		cancel()
		for _, close := range closers {
			close()
		}
		workers.Wait()
	}()
	connectStarted := time.Now()
	for range clients {
		client, close := server.connect(t)
		closers = append(closers, close)
		stream, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "database", Name: "postgres"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = stream.Recv(); err != nil {
			t.Fatal(err)
		}
		workers.Go(func() {
			for {
				update, err := stream.Recv()
				if ctx.Err() != nil {
					return
				}
				got := receipt{err: err}
				if err == nil {
					got.primary, got.delay = update.PrimaryId, time.Since(update.ObservedAt.AsTime())
				}
				select {
				case receipts <- got:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		})
	}
	connectElapsed := time.Since(connectStarted)
	latencies := make([]time.Duration, 0, clients*rounds)
	publishLatency := make([]time.Duration, 0, rounds)
	for round := range rounds {
		snapshot.PrimaryID = fmt.Sprintf("promotion-%d", round)
		snapshot.ObservedAt = time.Now()
		snapshot.ValidUntil = snapshot.ObservedAt.Add(time.Minute)
		server.store.Put(snapshot)
		publishLatency = append(publishLatency, time.Since(snapshot.ObservedAt))
		for range clients {
			select {
			case got := <-receipts:
				if got.err != nil || got.primary != snapshot.PrimaryID {
					t.Fatalf("lost or reordered promotion: got=%q expected=%q err=%v", got.primary, snapshot.PrimaryID, got.err)
				}
				latencies = append(latencies, got.delay)
			case <-ctx.Done():
				t.Fatal("independent clients did not receive promotion")
			}
		}
	}
	// A separate reconnect wave must get the complete current snapshot and
	// release every subscription when disconnected, without extra observations.
	var reconnects sync.WaitGroup
	reconnectErrors := make(chan error, min(clients, 32))
	for range min(clients, 32) {
		reconnects.Go(func() {
			client, close := server.connect(t)
			defer close()
			stream, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "database", Name: "postgres"})
			if err != nil {
				reconnectErrors <- err
				return
			}
			got, err := stream.Recv()
			if err != nil {
				reconnectErrors <- err
				return
			}
			if got.PrimaryId != snapshot.PrimaryID || len(got.Members) != len(snapshot.Members) {
				reconnectErrors <- fmt.Errorf("reconnect did not get latest complete state")
			}
		})
	}
	reconnects.Wait()
	close(reconnectErrors)
	for err := range reconnectErrors {
		t.Fatal(err)
	}
	cancel()
	for _, close := range closers {
		close()
	}
	workers.Wait()
	waitFor(t, func() bool { return !server.store.HasDemand("database", "postgres") }, "disconnected TLS clients leaked subscribers")
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	sort.Slice(publishLatency, func(i, j int) bool { return publishLatency[i] < publishLatency[j] })
	t.Logf("independent_tls_clients=%d rounds=%d sequential_connect=%s publish_p95=%s receipt_p50=%s receipt_p95=%s receipt_p99=%s receipt_max=%s",
		clients, rounds, connectElapsed, publishLatency[(len(publishLatency)-1)*95/100], latencies[(len(latencies)-1)*50/100], latencies[(len(latencies)-1)*95/100], latencies[(len(latencies)-1)*99/100], latencies[len(latencies)-1])
}

type stalledStreamTracker struct {
	*Server
	started  atomic.Int64
	finished atomic.Int64
}

func (s *stalledStreamTracker) WatchTopology(request *connectv1.WatchTopologyRequest, stream connectv1.TopologyService_WatchTopologyServer) error {
	if request.Name == "slow" {
		stream = &trackedSend{TopologyService_WatchTopologyServer: stream, tracker: s}
	}
	return s.Server.WatchTopology(request, stream)
}

type trackedSend struct {
	connectv1.TopologyService_WatchTopologyServer
	tracker *stalledStreamTracker
}

func (s *trackedSend) Send(snapshot *connectv1.Snapshot) error {
	s.tracker.started.Add(1)
	err := s.TopologyService_WatchTopologyServer.Send(snapshot)
	s.tracker.finished.Add(1)
	return err
}

func TestTLSStalledConsumerDoesNotBlockOthersAndCancels(t *testing.T) {
	var tracker *stalledStreamTracker
	server := startTLSDiscovery(t, func(store *Store) connectv1.TopologyServiceServer {
		tracker = &stalledStreamTracker{Server: NewServer(store, "")}
		return tracker
	})
	slow := sampleSnapshot()
	slow.Cluster.Name = "slow"
	// Exceed the peer's fixed flow-control window with the first message so
	// the next Send demonstrably blocks, rather than merely delaying Recv.
	slow.Connection.ServerCAPEM = bytes.Repeat([]byte("C"), 512<<10)
	server.store.Put(slow)
	server.store.Put(sampleSnapshot())
	client, closeClient := server.connect(t)
	defer closeClient()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	slowCtx, stopSlow := context.WithCancel(ctx)
	defer stopSlow()
	if _, err := client.WatchTopology(slowCtx, &connectv1.WatchTopologyRequest{Namespace: "database", Name: "slow"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return tracker.finished.Load() >= 1 }, "first slow send never queued")
	slow.PrimaryID = "second"
	server.store.Put(slow)
	waitFor(t, func() bool { return tracker.started.Load() >= 2 }, "second slow send never started")
	if tracker.finished.Load() != 1 {
		t.Fatal("fixture did not create an HTTP/2 flow-control stall")
	}
	for i := range 100 {
		slow.PrimaryID = fmt.Sprintf("pending-%d", i)
		server.store.Put(slow)
	}
	// Another stream on the same transport and a separate TLS connection both
	// remain usable while the slow stream is blocked on flow control.
	for _, separate := range []bool{false, true} {
		fast := client
		closeFast := func() {}
		if separate {
			fast, closeFast = server.connect(t)
		}
		got, err := fast.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "database", Name: "postgres"})
		closeFast()
		if err != nil || got.PrimaryId != "pod-1" {
			t.Fatalf("slow client blocked healthy discovery: %v", err)
		}
	}
	stopSlow()
	waitFor(t, func() bool { return !server.store.HasDemand("database", "slow") }, "blocked Send ignored client cancellation")
}

func waitFor(t *testing.T, condition func() bool, failure string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(failure)
		}
		time.Sleep(time.Millisecond)
	}
}
