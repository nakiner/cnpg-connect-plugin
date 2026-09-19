package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This uses real HTTP/2 stream flow control: a complete request stops the
// initial-message timer, while a zero response window blocks the handler's
// Flush. Releasing admission counters or the RPC context alone cannot unblock
// that writer. Neither the client nor the connection is canceled before the
// writer, outer ServeHTTP, and RST_STREAM assertions.
func TestAdmissionDeadlineInterruptsBlockedHTTP2Response(t *testing.T) {
	for _, scenario := range []struct {
		name          string
		secure, watch bool
	}{
		{name: "unary_plaintext"},
		{name: "unary_TLS", secure: true},
		{name: "watch_plaintext", watch: true},
		{name: "watch_TLS", secure: true, watch: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store := discovery.NewStore()
			now := time.Now()
			snapshot := v1.Snapshot{Cluster: v1.ClusterRef{Namespace: "test", Name: "db", UID: "uid"}, ObservedAt: now, ValidUntil: now.Add(time.Hour)}
			store.Put(snapshot)
			service := discovery.NewServer(store, "")
			a, err := newAdmission(context.Background(), Limits{MaxRPCs: 4, MaxWatches: 2, InitialRequestTimeout: time.Second}, service.Authorize)
			if err != nil {
				t.Fatal(err)
			}
			grpcServer := grpc.NewServer(a.options()...)
			connectv1.RegisterTopologyServiceServer(grpcServer, service)
			probe := &admissionWriteProbe{events: make(chan string, 32), returned: make(chan struct{})}
			var serverTLS, clientTLS *tls.Config
			if scenario.secure {
				ca := newCA(t, "blocked-writer")
				certificate, _, _ := ca.issue(t, 2, x509.ExtKeyUsageServerAuth)
				serverTLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}}
				roots := x509.NewCertPool()
				roots.AppendCertsFromPEM(ca.pem)
				clientTLS = &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: roots, NextProtos: []string{"h2"}}
			}
			address, _ := startAdmissionServer(t, a, grpcServer, serverTLS, func(server *http.Server) {
				next := server.Handler
				server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("x-write-probe") != "1" {
						next.ServeHTTP(w, r)
						return
					}
					next.ServeHTTP(&admissionProbeWriter{ResponseWriter: w, probe: probe}, r)
					close(probe.returned)
				})
			})
			client := newAdmissionWriteClient(t, address, clientTLS)

			// Stream 1 never receives WINDOW_UPDATE. The HTTP/2 setting is zero
			// for all streams; only the independent controls get their own credit.
			started := time.Now()
			client.request(t, 1, scenario.watch, "50m", 0)
			var calls []string
			for {
				select {
				case call := <-probe.events:
					calls = append(calls, call)
					if call == "Flush entered" {
						goto flushing
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("response never entered Flush: %v", calls)
				}
			}
		flushing:
			client.request(t, 3, false, "5S", 65535)
			client.await(t, func() bool { return client.result(3).ended }, "healthy unary completion on the same connection")
			client.requireSnapshot(t, 3)
			if got := client.result(3).headers["grpc-status"]; got != "0" {
				t.Fatalf("healthy unary grpc-status=%q", got)
			}
			client.request(t, 5, true, "5S", 65535)
			client.await(t, func() bool { return len(client.result(5).data) != 0 }, "healthy watch snapshot on the same connection")
			client.requireSnapshot(t, 5)

			select {
			case <-probe.returned:
			case <-time.After(time.Second):
				for len(probe.events) != 0 {
					calls = append(calls, <-probe.events)
				}
				t.Fatalf("grpc-timeout=50m elapsed %v but zero-window ServeHTTP and response writer remain blocked; calls=%v; healthy unary and watch succeeded on the same TCP connection", time.Since(started), calls)
			}
			for len(probe.events) != 0 {
				calls = append(calls, <-probe.events)
			}
			entered, returned := 0, 0
			for _, call := range calls {
				switch call {
				case "Write entered", "Flush entered":
					entered++
				case "Write returned", "Flush returned":
					returned++
				}
			}
			if entered == 0 || entered != returned {
				t.Fatalf("ServeHTTP returned with an outstanding response operation: %v", calls)
			}
			client.await(t, func() bool { return client.result(1).reset != nil }, "RST_STREAM for the expired zero-window response")
			if got := *client.result(1).reset; got != http2.ErrCodeInternal {
				t.Fatalf("expired stream reset=%v, want INTERNAL_ERROR from the HTTP/2 write deadline", got)
			}
			if len(client.result(1).data) != 0 {
				t.Fatal("zero-window stream unexpectedly received DATA")
			}
			if client.result(5).ended || client.result(5).reset != nil {
				t.Fatal("RPC cancellation terminated the healthy watch")
			}
			client.request(t, 7, false, "5S", 65535)
			client.await(t, func() bool { return client.result(7).ended }, "new unary completion after the blocked stream reset")
			client.requireSnapshot(t, 7)
			if got := client.result(7).headers["grpc-status"]; got != "0" {
				t.Fatalf("subsequent unary grpc-status=%q", got)
			}
			if client.result(5).ended || client.result(5).reset != nil {
				t.Fatal("healthy watch failed after the blocked stream reset")
			}
			// Prove the existing watch still delivers updates after the reset,
			// rather than merely observing that its socket remains open.
			client.result(5).data = nil
			snapshot.ObservedAt = time.Now()
			store.Put(snapshot)
			client.await(t, func() bool { return len(client.result(5).data) != 0 }, "continued watch delivery after another stream reset")
			client.requireSnapshot(t, 5)
			deadline := time.Now().Add(time.Second)
			for (len(a.rpcs) != 1 || len(a.watches) != 1) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if len(a.rpcs) != 1 || len(a.watches) != 1 {
				t.Fatalf("only the healthy watch should retain capacity: rpcs=%d watches=%d", len(a.rpcs), len(a.watches))
			}
			t.Logf("zero-window writer and ServeHTTP returned in %v; RST_STREAM=%v; same-connection unary/watch controls remained healthy", time.Since(started), *client.result(1).reset)
		})
	}
}

func TestAdmissionDeadlineStatusCanFlush(t *testing.T) {
	service := admissionService("")
	a, err := newAdmission(context.Background(), Limits{MaxRPCs: 1}, service.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	options := append(a.options(), grpc.UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}))
	server := grpc.NewServer(options...)
	connectv1.RegisterTopologyServiceServer(server, service)
	address, _ := startAdmissionServer(t, a, server, nil)
	client := newAdmissionWriteClient(t, address, nil)
	// A peer that accepts responses still receives the ordinary terminal gRPC
	// status when its RPC deadline expires, rather than an immediate reset.
	client.request(t, 1, false, "50m", 65535)
	client.await(t, func() bool { return client.result(1).ended }, "deadline status trailers")
	result := client.result(1)
	if result.reset != nil || result.headers["grpc-status"] != strconv.Itoa(int(codes.DeadlineExceeded)) {
		t.Fatalf("deadline status lost: reset=%v headers=%v", result.reset, result.headers)
	}
	waitAdmissionEmpty(t, a)
}

type admissionWriteProbe struct {
	events   chan string
	returned chan struct{}
}

type admissionProbeWriter struct {
	http.ResponseWriter
	probe *admissionWriteProbe
}

// ResponseController must retain access to the real HTTP/2 stream through this
// observation wrapper. The probe never uses the writer after ServeHTTP returns.
func (w *admissionProbeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *admissionProbeWriter) Write(p []byte) (int, error) {
	w.probe.events <- "Write entered"
	n, err := w.ResponseWriter.Write(p)
	w.probe.events <- "Write returned"
	return n, err
}

func (w *admissionProbeWriter) Flush() {
	w.probe.events <- "Flush entered"
	w.ResponseWriter.(http.Flusher).Flush()
	w.probe.events <- "Flush returned"
}

type admissionWriteResult struct {
	headers map[string]string
	data    []byte
	ended   bool
	reset   *http2.ErrCode
}

type admissionWriteClient struct {
	conn    net.Conn
	writer  *http2.Framer
	streams map[uint32]*admissionWriteResult
	scheme  string
}

func newAdmissionWriteClient(t *testing.T, address string, tlsConfig *tls.Config) *admissionWriteClient {
	t.Helper()
	var conn net.Conn
	var err error
	if tlsConfig == nil {
		conn, err = net.DialTimeout("tcp", address, time.Second)
	} else {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", address, tlsConfig)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := &admissionWriteClient{conn: conn, writer: http2.NewFramer(conn, conn), streams: make(map[uint32]*admissionWriteResult), scheme: "http"}
	client.writer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if tlsConfig != nil {
		client.scheme = "https"
	}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	if err := client.writer.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}); err != nil {
		t.Fatal(err)
	}
	// Wait for the server to apply the zero-window setting before requests.
	for {
		frame := client.read(t, "zero-window settings acknowledgement")
		if settings, ok := frame.(*http2.SettingsFrame); ok && settings.IsAck() {
			break
		}
	}
	return client
}

func (c *admissionWriteClient) request(t *testing.T, streamID uint32, watch bool, timeout string, window uint32) {
	t.Helper()
	method := connectv1.TopologyService_GetTopology_FullMethodName
	var request proto.Message = &connectv1.GetTopologyRequest{Namespace: "test", Name: "db"}
	if watch {
		method = connectv1.TopologyService_WatchTopology_FullMethodName
		request = &connectv1.WatchTopologyRequest{Namespace: "test", Name: "db"}
	}
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: c.scheme},
		{Name: ":path", Value: method}, {Name: ":authority", Value: "localhost"},
		{Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"},
		{Name: "grpc-timeout", Value: timeout}, {Name: "x-write-probe", Value: strconv.FormatUint(uint64(streamID), 10)},
	} {
		if err := encoder.WriteField(field); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.writer.WriteHeaders(http2.HeadersFrameParam{StreamID: streamID, BlockFragment: block.Bytes(), EndHeaders: true}); err != nil {
		t.Fatal(err)
	}
	if window != 0 {
		if err := c.writer.WriteWindowUpdate(streamID, window); err != nil {
			t.Fatal(err)
		}
	}
	payload, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	message := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(message[1:5], uint32(len(payload)))
	copy(message[5:], payload)
	if err := c.writer.WriteData(streamID, true, message); err != nil {
		t.Fatal(err)
	}
}

func (c *admissionWriteClient) result(streamID uint32) *admissionWriteResult {
	result := c.streams[streamID]
	if result == nil {
		result = &admissionWriteResult{headers: make(map[string]string)}
		c.streams[streamID] = result
	}
	return result
}

func (c *admissionWriteClient) read(t *testing.T, description string) http2.Frame {
	t.Helper()
	frame, err := c.writer.ReadFrame()
	if err != nil {
		t.Fatalf("waiting for %s: %v", description, err)
	}
	result := c.result(frame.Header().StreamID)
	switch f := frame.(type) {
	case *http2.MetaHeadersFrame:
		for _, field := range f.Fields {
			result.headers[field.Name] = field.Value
		}
		result.ended = f.StreamEnded()
	case *http2.DataFrame:
		result.data = append(result.data, f.Data()...)
		result.ended = f.StreamEnded()
	case *http2.RSTStreamFrame:
		code := f.ErrCode
		result.reset = &code
	case *http2.SettingsFrame:
		if !f.IsAck() {
			if err := c.writer.WriteSettingsAck(); err != nil {
				t.Fatal(err)
			}
		}
	case *http2.GoAwayFrame:
		t.Fatalf("unexpected GOAWAY: %s", f.ErrCode)
	}
	return frame
}

func (c *admissionWriteClient) await(t *testing.T, ready func() bool, description string) {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for !ready() {
		c.read(t, description)
	}
}

func (c *admissionWriteClient) requireSnapshot(t *testing.T, streamID uint32) {
	t.Helper()
	result := c.result(streamID)
	if result.reset != nil {
		t.Fatalf("healthy stream %d reset: %s", streamID, *result.reset)
	}
	if len(result.data) < 5 || result.data[0] != 0 || int(binary.BigEndian.Uint32(result.data[1:5])) != len(result.data)-5 {
		t.Fatalf("stream %d did not receive one complete gRPC message: %x", streamID, result.data)
	}
	var snapshot connectv1.Snapshot
	if err := proto.Unmarshal(result.data[5:], &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.GetCluster().GetNamespace() != "test" || snapshot.GetCluster().GetName() != "db" {
		t.Fatalf("stream %d returned the wrong snapshot: %v", streamID, &snapshot)
	}
}
