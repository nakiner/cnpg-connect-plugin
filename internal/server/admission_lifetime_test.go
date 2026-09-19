package server

import (
	"bytes"
	"context"
	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"testing"
	"time"
)

func TestAdmissionShutdownBeforeHandlerEntry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a, err := newAdmission(ctx, Limits{MaxRPCs: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.admit(context.Background(), ""); status.Code(err) != codes.Canceled {
		t.Fatalf("shutdown admission = %v", err)
	}
	waitAdmissionEmpty(t, a)
}
func TestAdmissionUnknownMethodAndDisconnectReclaimSlots(t *testing.T) {
	conn, a := admissionClient(t, Limits{MaxRPCs: 1, InitialRequestTimeout: time.Second}, "")
	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := conn.Invoke(ctx, "/unknown.Service/Method", &connectv1.GetTopologyRequest{}, &connectv1.Snapshot{})
		cancel()
		if status.Code(err) != codes.Unimplemented {
			t.Fatal(err)
		}
		waitAdmissionEmpty(t, a)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pendingWatch(t, conn, ctx)
	waitAdmissionUsed(t, a)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitAdmissionEmpty(t, a)
}
func waitAdmissionUsed(t *testing.T, a *admission) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(a.rpcs) == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("request not admitted")
}
func TestAdmissionPartialFrameTimeoutReclaimsSlots(t *testing.T) {
	a, err := newAdmission(context.Background(), Limits{MaxRPCs: 1, InitialRequestTimeout: 100 * time.Millisecond}, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(a.options()...)
	// A real handler blocked in RecvMsg exercises transport cancellation.
	srv.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Partial", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{{StreamName: "Watch", ServerStreams: true, Handler: func(_ any, s grpc.ServerStream) error { return s.RecvMsg(new(connectv1.WatchTopologyRequest)) }}}}, struct{}{})
	address, _ := startAdmissionServer(t, a, srv, nil)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = conn.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	framer := http2.NewFramer(conn, conn)
	if err = framer.WriteSettings(); err != nil {
		t.Fatal(err)
	}
	// Drain server frames so neither flow control nor the test socket blocks.
	go func() {
		for {
			if _, err := framer.ReadFrame(); err != nil {
				return
			}
		}
	}()
	for stream := uint32(1); stream <= 5; stream += 2 {
		var headers bytes.Buffer
		encoder := hpack.NewEncoder(&headers)
		for _, field := range []hpack.HeaderField{{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"}, {Name: ":path", Value: "/test.Partial/Watch"}, {Name: ":authority", Value: "localhost"}, {Name: "content-type", Value: "application/grpc"}, {Name: "te", Value: "trailers"}} {
			if err = encoder.WriteField(field); err != nil {
				t.Fatal(err)
			}
		}
		if err = framer.WriteHeaders(http2.HeadersFrameParam{StreamID: stream, BlockFragment: headers.Bytes(), EndHeaders: true}); err != nil {
			t.Fatal(err)
		}
		// gRPC advertises a 10-byte message, but receives only one body byte.
		if err = framer.WriteData(stream, false, []byte{0, 0, 0, 0, 10, 1}); err != nil {
			t.Fatal(err)
		}
		waitAdmissionUsed(t, a)
		waitAdmissionEmpty(t, a)
	}
	if got := a.timedout.Load(); got != 3 {
		t.Fatalf("timeouts=%d", got)
	}
}
