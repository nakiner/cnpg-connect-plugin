package server

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The wire protocol remains ordinary gRPC over HTTP/2, with or without TLS.
// Admission precedes grpc-timeout allocation and protobuf decoding. Rejected
// requests therefore cannot retain gRPC deadline timers until a remote deadline.
func (a *admission) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		md := make(metadata.MD, len(r.Header))
		for name, values := range r.Header {
			md[strings.ToLower(name)] = values
		}
		ctx, err := a.admit(metadata.NewIncomingContext(r.Context(), md), r.URL.Path)
		if err != nil {
			st := status.Convert(err)
			w.Header().Set("Content-Type", "application/grpc")
			w.Header().Set("Grpc-Status", strconv.Itoa(int(st.Code())))
			w.Header().Set("Grpc-Message", url.PathEscape(st.Message()))
			w.WriteHeader(http.StatusOK)
			return
		}
		state := ctx.Value(admittedRPCKey{}).(*admittedRPC)
		defer state.finish()
		writes := &responseWriteDeadline{controller: http.NewResponseController(w)}
		state.cancelWrite = writes.cancel
		defer writes.close()
		// HTTP/2 Body.Close interrupts a blocked Read. This also covers a client
		// that sends only part of its first gRPC message, before stats.InPayload.
		// Bound writes too, including errors returned before gRPC calls TagRPC.
		stopIO := context.AfterFunc(ctx, func() {
			writes.cancel()
			_ = r.Body.Close()
		})
		defer stopIO()
		// Both methods accept one request message. ServeHTTP eagerly reads the
		// body, so bound total wire bytes as well as gRPC's decoded message size.
		// Include the five-byte gRPC frame prefix. EOF, not InPayload, completes
		// the request: non-client-streaming gRPC also waits for END_STREAM.
		body := &initialRequestBody{
			ReadCloser: http.MaxBytesReader(w, r.Body, (64<<10)+5),
			state:      state,
		}
		rpcCtx := context.WithValue(r.Context(), admittedRPCKey{}, state)
		request := r.WithContext(rpcCtx)
		request.Body = body
		next.ServeHTTP(w, request)
	})
}

// Allow terminal gRPC trailers to flush on cancellation or normal completion.
// After this grace, HTTP/2 resets only the blocked stream, waking its writer.
const canceledResponseGrace = 100 * time.Millisecond

type responseWriteDeadline struct {
	mu         sync.Mutex
	controller *http.ResponseController
	armed      bool
}

func (w *responseWriteDeadline) cancel() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.controller == nil || w.armed {
		return
	}
	w.armed = true
	// The discovery listener always uses net/http HTTP/2, which supports
	// stream-scoped write deadlines. Repeated cancellation cannot extend it.
	_ = w.controller.SetWriteDeadline(time.Now().Add(canceledResponseGrace))
}

func (w *responseWriteDeadline) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Wait for any running deadline update and fence later callbacks before
	// ServeHTTP returns: net/http may then recycle its response writer.
	w.controller = nil
}

type initialRequestBody struct {
	io.ReadCloser
	state *admittedRPC
}

func (b *initialRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.state.timer.Stop()
	}
	return n, err
}

func discoveryHTTPServer(a *admission, grpcServer *grpc.Server, tlsConfig *tls.Config) *http.Server {
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(tlsConfig == nil)
	srv := &http.Server{
		Handler:   a.handler(grpcServer),
		TLSConfig: tlsConfig,
		Protocols: protocols,
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:      a.limits.MaxConcurrentStreams,
			MaxReceiveBufferPerStream: 64 << 10,
		},
		ReadHeaderTimeout: 5 * time.Second,
		// net/http adds 320 bytes when deriving the HTTP/2 header-list limit.
		MaxHeaderBytes: (16 << 10) - 320,
		BaseContext:    func(net.Listener) context.Context { return a.ctx },
	}
	// Force long-lived streams to reconnect so rotated certificates and trust
	// take effect. Match the former maximum connection age plus grace period.
	// Unlike native gRPC's GOAWAY, HTTP/2 closes the connection at this bound.
	srv.ConnState = connectionLifetime(5*time.Minute + 30*time.Second)
	return srv
}

func connectionLifetime(age time.Duration) func(net.Conn, http.ConnState) {
	var timers sync.Map
	return func(conn net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			timers.Store(conn, time.AfterFunc(age, func() { _ = conn.Close() }))
		case http.StateClosed:
			if timer, ok := timers.LoadAndDelete(conn); ok {
				timer.(*time.Timer).Stop()
			}
		}
	}
}
