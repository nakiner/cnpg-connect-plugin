package plugin

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nakiner/cnpg-connect-plugin/internal/config"
)

type countNotifier struct{ calls atomic.Int64 }

func (n *countNotifier) Notify() { n.calls.Add(1) }

func TestCNPGProtocol(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	notifier := &countNotifier{}
	Register(server, "test-version", notifier)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	id := identity.NewIdentityClient(conn)
	metadata, err := id.GetPluginMetadata(ctx, &identity.GetPluginMetadataRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Name != config.PluginName || metadata.Version != "test-version" || metadata.License != "UNLICENSED" {
		t.Fatalf("unexpected metadata: %v", metadata)
	}
	capabilities, err := id.GetPluginCapabilities(ctx, &identity.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(capabilities.Capabilities) != 1 || capabilities.Capabilities[0].GetService().GetType() != identity.PluginCapability_Service_TYPE_RECONCILER_HOOKS {
		t.Fatalf("advertises unsupported capabilities: %v", capabilities)
	}
	// The plugin accepts notifications independently of collection readiness.
	probe, err := id.Probe(ctx, &identity.ProbeRequest{})
	if err != nil || !probe.GetReady() {
		t.Fatalf("CNPG-I readiness must not wait for database observation: %v, %v", probe, err)
	}
	hooks := reconciler.NewReconcilerHooksClient(conn)
	kinds, err := hooks.GetCapabilities(ctx, &reconciler.ReconcilerHooksCapabilitiesRequest{})
	if err != nil || len(kinds.GetReconcilerCapabilities()) != 1 || kinds.ReconcilerCapabilities[0].Kind != reconciler.ReconcilerHooksCapability_KIND_CLUSTER {
		t.Fatalf("unexpected hook capabilities: %v, %v", kinds, err)
	}
	for _, call := range []func(context.Context, *reconciler.ReconcilerHooksRequest, ...grpc.CallOption) (*reconciler.ReconcilerHooksResult, error){hooks.Pre, hooks.Post} {
		// Hook payloads are hints only; malformed/stale state cannot interrupt
		// the operator's own reconciliation or become a trusted topology.
		result, err := call(ctx, &reconciler.ReconcilerHooksRequest{ClusterDefinition: []byte("malformed")})
		if err != nil || result.GetBehavior() != reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE {
			t.Fatalf("hook blocked reconciliation: %v, %v", result, err)
		}
	}
	if notifier.calls.Load() != 2 {
		t.Fatalf("expected both hooks to notify, got %d", notifier.calls.Load())
	}
}
