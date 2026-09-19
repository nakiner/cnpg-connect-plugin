package server

import (
	"context"
	"strings"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAdmissionWireSizeLimitsAndReclamation(t *testing.T) {
	conn, a := admissionClient(t, Limits{MaxRPCs: 1}, "")
	client := connectv1.NewTopologyServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := &connectv1.GetTopologyRequest{Namespace: "test", Name: "db"}
	if _, err := client.GetTopology(ctx, request); err != nil {
		t.Fatal(err)
	}
	_, err := client.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "test", Name: strings.Repeat("n", 65<<10)})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("request-size limit not enforced before decoding: %v", err)
	}
	largeHeaders := metadata.AppendToOutgoingContext(ctx, "x-test-size", strings.Repeat("h", 17<<10))
	if _, err := client.GetTopology(largeHeaders, request); err == nil || status.Code(err) == codes.DeadlineExceeded {
		t.Fatalf("header-size limit failed: %v", err)
	}
	for len(a.rpcs) != 0 {
		select {
		case <-ctx.Done():
			t.Fatal("oversized input leaked admission capacity")
		case <-time.After(time.Millisecond):
		}
	}
	if _, err := client.GetTopology(ctx, request); err != nil {
		t.Fatalf("oversized input poisoned healthy requests: %v", err)
	}
}
