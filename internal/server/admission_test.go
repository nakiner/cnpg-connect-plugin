package server

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func admissionClient(t *testing.T, limits Limits, token string) (*grpc.ClientConn, *admission) {
	t.Helper()
	store := discovery.NewStore()
	now := time.Now()
	store.Put(v1.Snapshot{Cluster: v1.ClusterRef{Namespace: "test", Name: "db", UID: "uid"}, ObservedAt: now, ValidUntil: now.Add(time.Hour)})
	service := discovery.NewServer(store, token)
	a, err := newAdmission(context.Background(), limits, service.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer(a.options()...)
	connectv1.RegisterTopologyServiceServer(s, service)
	address, _ := startAdmissionServer(t, a, s, nil)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, a
}

// Exercise the same HTTP/2 entrypoint as Run. grpc.Server.Serve bypasses the
// pre-decoding admission handler and cannot test its rejection cleanup.
func startAdmissionServer(t *testing.T, a *admission, grpcServer *grpc.Server, tlsConfig *tls.Config, configure ...func(*http.Server)) (string, *http.Server) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := discoveryHTTPServer(a, grpcServer, tlsConfig)
	for _, configure := range configure {
		configure(server)
	}
	done := make(chan error, 1)
	go func() {
		if tlsConfig != nil {
			done <- server.ServeTLS(listener, "", "")
		} else {
			done <- server.Serve(listener)
		}
	}()
	t.Cleanup(func() {
		_ = server.Close()
		grpcServer.Stop()
		select {
		case err := <-done:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("HTTP/2 admission server stopped: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("HTTP/2 admission server did not stop")
		}
	})
	return listener.Addr().String(), server
}

func pendingWatch(t *testing.T, conn *grpc.ClientConn, ctx context.Context) grpc.ClientStream {
	t.Helper()
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, connectv1.TopologyService_WatchTopology_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestAdmissionRejectsUnauthenticatedBeforeRequestBody(t *testing.T) {
	conn, _ := admissionClient(t, Limits{InitialRequestTimeout: time.Second}, strings.Repeat("x", 32))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for range 16 {
		stream := pendingWatch(t, conn, ctx)
		if err := stream.RecvMsg(new(connectv1.Snapshot)); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("pre-body rejection=%v", err)
		}
	}
}

func TestAdmissionInitialRequestTimeoutAndCapacityReclamation(t *testing.T) {
	conn, a := admissionClient(t, Limits{MaxRPCs: 1, MaxWatches: 1, InitialRequestTimeout: 40 * time.Millisecond}, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for range 3 {
		stream := pendingWatch(t, conn, ctx)
		err := stream.RecvMsg(new(connectv1.Snapshot))
		if status.Code(err) != codes.Canceled && status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("incomplete request survived timeout: %v", err)
		}
		until := time.Now().Add(time.Second)
		for len(a.rpcs) != 0 && time.Now().Before(until) {
			time.Sleep(time.Millisecond)
		}
		if len(a.rpcs) != 0 || len(a.watches) != 0 {
			t.Fatal("timeout leaked admission capacity")
		}
	}
}

func TestAdmissionCompletedWatchHasNoInitialMessageDeadline(t *testing.T) {
	conn, _ := admissionClient(t, Limits{MaxRPCs: 2, MaxWatches: 1, InitialRequestTimeout: 40 * time.Millisecond}, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := connectv1.NewTopologyServiceClient(conn)
	watch, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "test", Name: "db"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = watch.Recv(); err != nil {
		t.Fatal(err)
	}
	other, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "test", Name: "db"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("watch cap not enforced: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err = client.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "test", Name: "db"}); err != nil {
		t.Fatalf("watch limit consumed unary capacity: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := watch.Recv(); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("healthy watch expired: %v", err)
	case <-time.After(60 * time.Millisecond):
	}
	cancel()
	<-done
}

func TestAdmissionGlobalRPCCapBeforeBody(t *testing.T) {
	conn, a := admissionClient(t, Limits{MaxRPCs: 1, InitialRequestTimeout: time.Second}, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pendingWatch(t, conn, ctx)
	until := time.Now().Add(time.Second)
	for len(a.rpcs) == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if len(a.rpcs) != 1 {
		t.Fatal("pending request was not accounted")
	}
	_, err := connectv1.NewTopologyServiceClient(conn).GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "test", Name: "db"})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("RPC cap not enforced before decoding: %v", err)
	}
}
