package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestAdmissionCompleteMessageWithoutEndStreamStillTimesOut(t *testing.T) {
	conn, a := admissionClient(t, Limits{MaxRPCs: 1, MaxWatches: 1, InitialRequestTimeout: 80 * time.Millisecond}, "")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := time.Now()
	// Mark only the client descriptor as streaming: gRPC otherwise sets
	// END_STREAM automatically in SendMsg, even without a CloseSend call.
	// The registered server method still accepts exactly one request.
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, connectv1.TopologyService_WatchTopology_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&connectv1.WatchTopologyRequest{Namespace: "test", Name: "db"}); err != nil {
		t.Fatal(err)
	}
	err = stream.RecvMsg(new(connectv1.Snapshot))
	if status.Code(err) != codes.Canceled {
		t.Fatalf("complete message without END_STREAM escaped admission timeout: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("request waited %v instead of its initial-message timeout", elapsed)
	}
	waitAdmissionEmpty(t, a)
	if got := a.timedout.Load(); got != 1 {
		t.Fatalf("initial timeout count=%d, want 1", got)
	}
	if _, err := connectv1.NewTopologyServiceClient(conn).GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "test", Name: "db"}); err != nil {
		t.Fatalf("timed-out stream retained unary capacity: %v", err)
	}
}

func TestAdmissionHTTPBodyReadLimit(t *testing.T) {
	const limit = (64 << 10) + 5
	for _, size := range []int{limit, limit + 1, 2 * limit} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			a, err := newAdmission(context.Background(), Limits{MaxRPCs: 1}, nil)
			if err != nil {
				t.Fatal(err)
			}
			var count int64
			var readErr error
			handler := a.handler(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				count, readErr = io.Copy(io.Discard, request.Body)
			}))
			request := httptest.NewRequest(http.MethodPost, connectv1.TopologyService_WatchTopology_FullMethodName, bytes.NewReader(make([]byte, size)))
			handler.ServeHTTP(httptest.NewRecorder(), request)
			if count != limit {
				t.Fatalf("downstream consumed %d bytes, want maximum %d", count, limit)
			}
			if size == limit {
				if readErr != nil {
					t.Fatalf("maximum-sized request rejected: %v", readErr)
				}
			} else {
				var tooLarge *http.MaxBytesError
				if !errors.As(readErr, &tooLarge) || tooLarge.Limit != limit {
					t.Fatalf("oversized request did not stop at wire-body limit: %v", readErr)
				}
			}
			waitAdmissionEmpty(t, a)
		})
	}
}

func TestAdmissionRejectedRequestsReleaseHTTPContexts(t *testing.T) {
	for _, mode := range []string{"authentication", "capacity"} {
		t.Run(mode, func(t *testing.T) {
			token := ""
			want := codes.ResourceExhausted
			if mode == "authentication" {
				token = strings.Repeat("x", 32)
				want = codes.Unauthenticated
			}
			service := admissionService(token)
			observed := make(chan context.Context, 1)
			a, err := newAdmission(context.Background(), Limits{MaxRPCs: 1, InitialRequestTimeout: time.Minute}, func(ctx context.Context) error {
				observed <- ctx
				return service.Authorize(ctx)
			})
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer(a.options()...)
			connectv1.RegisterTopologyServiceServer(server, service)
			address, _ := startAdmissionServer(t, a, server, nil)
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			hold, release := context.WithCancel(context.Background())
			defer release()
			if mode == "capacity" {
				pendingWatch(t, conn, hold)
				<-observed
				waitAdmissionUsed(t, a)
			}
			for i := range 64 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				stream := pendingWatch(t, conn, ctx)
				done := make(chan error, 1)
				go func() { done <- stream.RecvMsg(new(connectv1.Snapshot)) }()
				select {
				case err := <-done:
					if status.Code(err) != want {
						cancel()
						t.Fatalf("request %d: rejection=%v, want %s", i, err, want)
					}
				case <-time.After(time.Second):
					cancel()
					t.Fatal("request without a body was not rejected promptly")
				}
				httpContext := <-observed
				if _, ok := httpContext.Deadline(); ok {
					cancel()
					t.Fatal("gRPC deadline allocated before admission")
				}
				// Check before canceling the client context or closing its shared
				// connection: handler completion must release the HTTP owner itself.
				select {
				case <-httpContext.Done():
				case <-time.After(time.Second):
					cancel()
					t.Fatalf("request %d retained its HTTP context after rejection", i)
				}
				cancel()
			}
			release()
			waitAdmissionEmpty(t, a)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if token != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
			}
			client := connectv1.NewTopologyServiceClient(conn)
			if _, err := client.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "test", Name: "db"}); err != nil {
				t.Fatalf("rejection loop poisoned valid unary calls: %v", err)
			}
			<-observed
			waitAdmissionEmpty(t, a)
			watch, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "test", Name: "db"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := watch.Recv(); err != nil {
				t.Fatalf("rejection loop poisoned valid watches: %v", err)
			}
			<-observed
			cancel()
			waitAdmissionEmpty(t, a)
		})
	}
}

func admissionService(token string) *discovery.Server {
	store := discovery.NewStore()
	now := time.Now()
	store.Put(v1.Snapshot{Cluster: v1.ClusterRef{Namespace: "test", Name: "db", UID: "uid"}, ObservedAt: now, ValidUntil: now.Add(time.Hour)})
	return discovery.NewServer(store, token)
}

func TestAdmissionHTTP2AdvertisesPerConnectionStreamLimit(t *testing.T) {
	conn, a := admissionClient(t, Limits{MaxConcurrentStreams: 1, MaxRPCs: 3, InitialRequestTimeout: time.Minute}, "")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pendingWatch(t, conn, ctx)
	waitAdmissionUsed(t, a)
	limited, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	_, err := conn.NewStream(limited, &grpc.StreamDesc{ServerStreams: true}, connectv1.TopologyService_WatchTopology_FullMethodName)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("second stream bypassed HTTP/2 connection capacity: %v", err)
	}
	if len(a.rpcs) != 1 {
		t.Fatalf("per-connection stream limit reached application admission: %d", len(a.rpcs))
	}
	cancel()
	waitAdmissionEmpty(t, a)
}

func TestAdmissionHTTP2ConnectionAgeClosesWatchAndAllowsReconnect(t *testing.T) {
	service := admissionService("")
	a, err := newAdmission(context.Background(), Limits{}, service.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(a.options()...)
	connectv1.RegisterTopologyServiceServer(server, service)
	address, _ := startAdmissionServer(t, a, server, nil, func(server *http.Server) {
		server.ConnState = connectionLifetime(200 * time.Millisecond)
	})
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := connectv1.NewTopologyServiceClient(conn)
	watch, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "test", Name: "db"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, err := watch.Recv(); err == nil || status.Code(err) == codes.DeadlineExceeded {
		t.Fatalf("long-lived watch survived its transport age: %v", err)
	}
	waitAdmissionEmpty(t, a)
	if _, err := client.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "test", Name: "db"}); err != nil {
		t.Fatalf("client could not reconnect after transport age: %v", err)
	}
}

func TestAdmissionHTTP2TLSAuthenticationAndRenewal(t *testing.T) {
	for _, mutual := range []bool{false, true} {
		t.Run(map[bool]string{false: "TLS", true: "mutual_TLS"}[mutual], func(t *testing.T) {
			ca := newCA(t, "server")
			clientCA := newCA(t, "client")
			otherCA := newCA(t, "untrusted")
			dir := t.TempDir()
			certPath, keyPath, trustPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), ""
			_, certPEM, keyPEM := ca.issue(t, 2, x509.ExtKeyUsageServerAuth)
			writeFile(t, certPath, certPEM)
			writeFile(t, keyPath, keyPEM)
			if mutual {
				trustPath = filepath.Join(dir, "client-ca.crt")
				writeFile(t, trustPath, clientCA.pem)
			}
			serverTLS, err := TLSConfig(certPath, keyPath, trustPath)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AppendCertsFromPEM(ca.pem)
			clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: roots}
			if mutual {
				cert, _, _ := clientCA.issue(t, 3, x509.ExtKeyUsageClientAuth)
				clientTLS.Certificates = []tls.Certificate{cert}
			}
			token := strings.Repeat("x", 32)
			service := admissionService(token)
			a, err := newAdmission(context.Background(), Limits{}, service.Authorize)
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer(a.options()...)
			connectv1.RegisterTopologyServiceServer(server, service)
			address, _ := startAdmissionServer(t, a, server, serverTLS)

			request := func(tlsConfig *tls.Config, bearer string) (tls.ConnectionState, error) {
				conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
				if err != nil {
					return tls.ConnectionState{}, err
				}
				defer conn.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearer)
				var remote peer.Peer
				_, err = connectv1.NewTopologyServiceClient(conn).GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "test", Name: "db"}, grpc.Peer(&remote))
				if info, ok := remote.AuthInfo.(credentials.TLSInfo); ok {
					return info.State, err
				}
				return tls.ConnectionState{}, err
			}
			state, err := request(clientTLS, token)
			if err != nil || state.NegotiatedProtocol != "h2" || state.Version != tls.VersionTLS13 || state.PeerCertificates[0].SerialNumber.Int64() != 2 {
				t.Fatalf("verified gRPC/TLS request failed: %v, state=%+v", err, state)
			}
			if _, err := request(clientTLS, "wrong-token"); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("TLS did not preserve bearer authorization: %v", err)
			}
			wrongName := clientTLS.Clone()
			wrongName.ServerName = "another-server"
			if _, err := request(wrongName, token); err == nil {
				t.Fatal("wrong server name was accepted")
			}
			wrongRoots := clientTLS.Clone()
			wrongRoots.RootCAs = x509.NewCertPool()
			wrongRoots.RootCAs.AppendCertsFromPEM(otherCA.pem)
			if _, err := request(wrongRoots, token); err == nil {
				t.Fatal("untrusted server CA was accepted")
			}
			if mutual {
				anonymous := clientTLS.Clone()
				anonymous.Certificates = nil
				if _, err := request(anonymous, token); err == nil {
					t.Fatal("missing client certificate was accepted")
				}
				untrusted := clientTLS.Clone()
				cert, _, _ := otherCA.issue(t, 4, x509.ExtKeyUsageClientAuth)
				untrusted.Certificates = []tls.Certificate{cert}
				if _, err := request(untrusted, token); err == nil {
					t.Fatal("untrusted client certificate was accepted")
				}
			}
			_, certPEM, keyPEM = ca.issue(t, 5, x509.ExtKeyUsageServerAuth)
			writeFile(t, certPath, certPEM)
			writeFile(t, keyPath, keyPEM)
			state, err = request(clientTLS, token)
			if err != nil || state.NegotiatedProtocol != "h2" || state.PeerCertificates[0].SerialNumber.Int64() != 5 {
				t.Fatalf("new gRPC connection missed mounted certificate renewal: %v", err)
			}
		})
	}
}

type lifetimeTestConn struct {
	net.Conn
	closed atomic.Int64
}

func (c *lifetimeTestConn) Close() error {
	c.closed.Add(1)
	return c.Conn.Close()
}

func TestHTTPConnectionLifetimeClosesActiveStreamsAndStopsCompletedTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const age = 5*time.Minute + 30*time.Second
		callback := connectionLifetime(age)
		active, peer := net.Pipe()
		defer peer.Close()
		tracked := &lifetimeTestConn{Conn: active}
		callback(tracked, http.StateNew)
		callback(tracked, http.StateActive)
		time.Sleep(age - time.Second)
		if tracked.closed.Load() != 0 {
			t.Fatal("connection closed before maximum age")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if tracked.closed.Load() != 1 {
			t.Fatal("active connection survived maximum age")
		}
		callback(tracked, http.StateClosed)

		completed, other := net.Pipe()
		defer other.Close()
		tracked = &lifetimeTestConn{Conn: completed}
		callback(tracked, http.StateNew)
		_ = tracked.Close()
		callback(tracked, http.StateClosed)
		time.Sleep(age)
		synctest.Wait()
		if tracked.closed.Load() != 1 {
			t.Fatal("completed connection retained its age timer")
		}
	})
}
