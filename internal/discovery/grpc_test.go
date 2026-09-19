package discovery

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCTokenlessConnectionDefaults(t *testing.T) {
	s := NewStore()
	input := sampleSnapshot()
	input.Connection.Database = "app"
	input.Connection.ServerCAPEM = []byte("public CA")
	s.Put(input)
	client := testClient(t, s, "")
	ctx := rpcContext(t)
	got, err := client.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "database", Name: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetConnection().GetDatabase() != "app" || !bytes.Equal(got.GetConnection().GetServerCaPem(), input.Connection.ServerCAPEM) {
		t.Fatal("tokenless Get lost public connection defaults")
	}
	stream, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "database", Name: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	input.Connection.ServerCAPEM = []byte("rotated public CA")
	s.Put(input)
	rotated, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Revision == initial.Revision || !bytes.Equal(rotated.GetConnection().GetServerCaPem(), input.Connection.ServerCAPEM) {
		t.Fatal("tokenless watch did not carry CA rotation")
	}
}

func TestGRPCGetRequestsObservationBeforeReading(t *testing.T) {
	s := NewStore()
	input := sampleSnapshot()
	input.Available = false
	input.Reason = "awaiting_observation"
	s.Put(input)
	s.SetDemandHandler(func(namespace, name string) {
		current, found := s.Get(namespace, name)
		if !found {
			return
		}
		current.Available = true
		current.Reason = "refreshed"
		s.Put(current)
	}, time.Minute)
	client := testClient(t, s, "")
	got, err := client.GetTopology(rpcContext(t), &connectv1.GetTopologyRequest{Namespace: "database", Name: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Available || got.Reason != "refreshed" || !s.HasDemand("database", "postgres") {
		t.Fatal("GetTopology did not request an observation before retrieving the snapshot")
	}
}

func testClient(t *testing.T, store *Store, token string) connectv1.TopologyServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	connectv1.RegisterTopologyServiceServer(server, NewServer(store, token))
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(); server.Stop(); _ = listener.Close() })
	return connectv1.NewTopologyServiceClient(connection)
}

func rpcContext(t *testing.T, auth ...string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	for _, value := range auth {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", value)
	}
	return ctx
}

func TestGRPCAuthenticationProtectsBothMethods(t *testing.T) {
	s := NewStore()
	s.Put(sampleSnapshot())
	client := testClient(t, s, "secret-token")
	for _, test := range []struct {
		name string
		auth []string
		code codes.Code
	}{
		{"missing", nil, codes.Unauthenticated},
		{"wrong", []string{"Bearer wrong"}, codes.Unauthenticated},
		{"wrong scheme", []string{"Basic secret-token"}, codes.Unauthenticated},
		{"duplicate", []string{"Bearer secret-token", "Bearer wrong"}, codes.Unauthenticated},
		{"valid", []string{"Bearer secret-token"}, codes.OK},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := rpcContext(t, test.auth...)
			_, err := client.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "database", Name: "postgres"})
			if status.Code(err) != test.code {
				t.Fatalf("Get: %v", err)
			}
			stream, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "database", Name: "postgres"})
			if err == nil {
				_, err = stream.Recv()
			}
			if status.Code(err) != test.code {
				t.Fatalf("Watch: %v", err)
			}
		})
	}
}

func TestGRPCReferenceErrorsAndUnavailableSnapshot(t *testing.T) {
	s := NewStore()
	client := testClient(t, s, "")
	for _, test := range []struct {
		namespace, name string
		code            codes.Code
	}{
		{"database", "postgres", codes.NotFound}, {"", "postgres", codes.InvalidArgument},
		{"database", "../postgres", codes.InvalidArgument}, {"database", "POSTGRES", codes.InvalidArgument},
		{"database.extra", "postgres", codes.InvalidArgument}, {"-database", "postgres", codes.InvalidArgument},
		{strings.Repeat("a", 64), "postgres", codes.InvalidArgument},
		{"database", strings.Repeat("a", 64) + ".postgres", codes.InvalidArgument},
		{"database", "postgres.extra", codes.NotFound},
	} {
		ctx := rpcContext(t)
		_, err := client.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: test.namespace, Name: test.name})
		if status.Code(err) != test.code {
			t.Fatalf("Get(%q,%q): %v", test.namespace, test.name, err)
		}
		stream, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: test.namespace, Name: test.name})
		if err == nil {
			_, err = stream.Recv()
		}
		if status.Code(err) != test.code {
			t.Fatalf("Watch(%q,%q): %v", test.namespace, test.name, err)
		}
	}
	input := sampleSnapshot()
	input.ValidUntil = time.Now().Add(-time.Second)
	s.Put(input)
	got, err := client.GetTopology(rpcContext(t), &connectv1.GetTopologyRequest{Namespace: "database", Name: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Available || got.PrimaryId != "" || got.Reason != "expired" {
		t.Fatal("expired Get did not fail closed")
	}
}

func TestGRPCWatchInitialRefreshDeletionRecreationAndCancellation(t *testing.T) {
	s := NewStore()
	input := sampleSnapshot()
	s.Put(input)
	client := testClient(t, s, "secret")
	ctx, cancel := context.WithCancel(rpcContext(t, "Bearer secret"))
	defer cancel()
	stream, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "database", Name: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if initial.PrimaryId != "pod-1" || initial.Members[0].Role != connectv1.Role_ROLE_PRIMARY || initial.Members[1].SyncState != connectv1.SyncState_SYNC_STATE_SYNC {
		t.Fatal("initial protobuf mapping lost topology")
	}
	if initial.Members[0].Endpoints["internal"].Port != 5432 {
		t.Fatal("lost endpoint")
	}
	input.ValidUntil = input.ValidUntil.Add(time.Second)
	input.ObservedAt = input.ObservedAt.Add(time.Second)
	input.Members[1].ReplayLSN = "0/20"
	input.Members[0], input.Members[1] = input.Members[1], input.Members[0]
	s.Put(input)
	refreshed, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Revision != initial.Revision || !refreshed.ValidUntil.AsTime().Equal(input.ValidUntil) {
		t.Fatal("freshness, WAL progress, or ordering changed routing revision or lost freshness")
	}
	if refreshed.Members[0].ReplayLsn != "0/20" {
		t.Fatal("refresh lost WAL observation")
	}
	other := NewStore()
	other.Put(input)
	fromOther, _ := other.Get("database", "postgres")
	if fromOther.Revision == initial.Revision {
		t.Fatal("revision reused across process/store restart")
	}
	s.Expire(input.ValidUntil)
	expired, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if expired.Available || expired.PrimaryId != "" || expired.Revision == initial.Revision {
		t.Fatal("watch did not invalidate expiry")
	}
	s.Delete("database", "postgres")
	deleted, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Available || deleted.PrimaryId != "" || deleted.Reason != "deleted" || len(deleted.Members) != 0 || deleted.Revision == expired.Revision {
		t.Fatal("watch lost deletion")
	}
	if _, err := client.GetTopology(ctx, &connectv1.GetTopologyRequest{Namespace: "database", Name: "postgres"}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted cluster is still discoverable: %v", err)
	}
	input.Cluster.UID = "new-cluster"
	s.Put(input)
	recreated, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if recreated.Cluster.Uid != "new-cluster" || !recreated.Available || recreated.Revision == initial.Revision {
		t.Fatal("watch lost recreation")
	}
	// Every reconnect starts with a full current snapshot.
	reconnected, err := client.WatchTopology(ctx, &connectv1.WatchTopologyRequest{Namespace: "database", Name: "postgres"})
	if err != nil {
		t.Fatal(err)
	}
	current, err := reconnected.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision != recreated.Revision || len(current.Members) != 2 {
		t.Fatal("reconnect did not receive complete current snapshot")
	}
	cancel()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("stream cancellation: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		count := len(s.subscribers)
		s.mu.Unlock()
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disconnected streams leaked subscriptions")
		}
		time.Sleep(time.Millisecond)
	}
}
