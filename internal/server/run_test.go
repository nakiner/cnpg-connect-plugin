package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type testObserver struct {
	err     error
	started chan struct{}
}

func (o testObserver) Notify(string, string) {}
func (o testObserver) Ready() bool           { return false }
func (o testObserver) Run(ctx context.Context) error {
	if o.started != nil {
		close(o.started)
	}
	if o.err != nil {
		return o.err
	}
	<-ctx.Done()
	return nil
}

func TestRunPropagatesObserverFailure(t *testing.T) {
	failure := errors.New("collection failed")
	err := Run(context.Background(), Options{PluginAddress: "127.0.0.1:0", DiscoveryAddress: "127.0.0.1:0", HealthAddress: "127.0.0.1:0"}, testObserver{err: failure}, func(*grpc.Server) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, failure) {
		t.Fatalf("observer error was lost: %v", err)
	}
}

func TestRunCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{PluginAddress: "127.0.0.1:0", DiscoveryAddress: "127.0.0.1:0", HealthAddress: "127.0.0.1:0"}, testObserver{started: started}, func(*grpc.Server) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("observer never started")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean cancellation failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not complete")
	}
}

func TestRunCancellationWithActiveWatch(t *testing.T) {
	testRunWithActiveWatch(t, false)
}

func TestRunForcesStalledWatch(t *testing.T) {
	testRunWithActiveWatch(t, true)
}

func testRunWithActiveWatch(t *testing.T, ignoreCancellation bool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serving, watching := make(chan struct{}), make(chan struct{})
	handlerExited := make(chan struct{})
	releaseHandler := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseHandler) }) }
	defer release()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{PluginAddress: "127.0.0.1:0", DiscoveryAddress: address, HealthAddress: "127.0.0.1:0"}, testObserver{started: serving}, func(server *grpc.Server) {
			server.RegisterService(&grpc.ServiceDesc{
				ServiceName: "test.Topology",
				HandlerType: (*interface{})(nil),
				Streams: []grpc.StreamDesc{{
					StreamName:    "Watch",
					ServerStreams: true,
					Handler: func(_ any, stream grpc.ServerStream) error {
						defer close(handlerExited)
						close(watching)
						if ignoreCancellation {
							<-releaseHandler
						} else {
							<-stream.Context().Done()
						}
						return nil
					},
				}},
			}, struct{}{})
		}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	select {
	case <-serving:
	case err := <-done:
		t.Fatalf("server startup failed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("server never started")
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// This client deliberately keeps its stream open throughout server shutdown.
	stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{ServerStreams: true}, "/test.Topology/Watch")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-watching:
	case <-time.After(2 * time.Second):
		t.Fatal("watch never started")
	}
	cancel()
	limit := 2 * time.Second
	if ignoreCancellation {
		limit = 12 * time.Second
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("normal shutdown with an active watch failed: %v", err)
		}
	case <-time.After(limit):
		t.Fatalf("active watch exceeded shutdown limit %s", limit)
	}
	// A deliberately uncooperative handler cannot be killed by Go. Release it
	// once bounded shutdown returns and wait for its cleanup, so this regression
	// test leaves neither a handler nor gRPC's stop waiters stuck behind it.
	release()
	select {
	case <-handlerExited:
	case <-time.After(2 * time.Second):
		t.Fatal("stream handler did not clean up after release")
	}
}

func TestRunRejectsOccupiedListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	err = Run(context.Background(), Options{PluginAddress: "127.0.0.1:0", DiscoveryAddress: listener.Addr().String(), HealthAddress: "127.0.0.1:0"}, testObserver{}, func(*grpc.Server) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "listen for discovery API") {
		t.Fatalf("listener failure was lost: %v", err)
	}
}

func TestHealthReadinessIsSeparateFromLiveness(t *testing.T) {
	ready := false
	handler := HealthHandler(func() bool { return ready })
	for _, path := range []string{"/healthz", "/livez", "/readyz"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		want := http.StatusOK
		if path == "/readyz" {
			want = http.StatusServiceUnavailable
		}
		if recorder.Code != want {
			t.Errorf("%s: got %d, want %d", path, recorder.Code, want)
		}
	}
	ready = true
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("initialized observer not ready: %d", recorder.Code)
	}
}
