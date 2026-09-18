package plugin

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nakiner/cnpg-connect-plugin/internal/config"
)

type notification struct{ namespace, name string }

type countNotifier struct {
	mu    sync.Mutex
	calls []notification
}

func (n *countNotifier) Notify(namespace, name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, notification{namespace, name})
}

func (n *countNotifier) notifications() []notification {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]notification(nil), n.calls...)
}

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
		result, err := call(ctx, &reconciler.ReconcilerHooksRequest{
			ClusterDefinition: []byte(`{"metadata":{"namespace":"dev","name":"rent"},"status":{"currentPrimary":"untrusted"}}`),
		})
		if err != nil || result.GetBehavior() != reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE {
			t.Fatalf("hook blocked reconciliation: %v, %v", result, err)
		}
		// Hook payloads are hints only; malformed/stale state cannot interrupt
		// the operator's own reconciliation or become a trusted topology.
		result, err = call(ctx, &reconciler.ReconcilerHooksRequest{ClusterDefinition: []byte("malformed")})
		if err != nil || result.GetBehavior() != reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE {
			t.Fatalf("hook blocked reconciliation: %v, %v", result, err)
		}
	}
	if got, want := notifier.notifications(), []notification{{"dev", "rent"}, {"dev", "rent"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected targeted hook notifications: got %v, want %v", got, want)
	}
}

func TestHookInvalidPayloadsNeverEnqueue(t *testing.T) {
	notifier := &countNotifier{}
	service := &Service{notifier: notifier}
	for _, payload := range []string{
		"", "null", "malformed", "[]", "{}",
		`{"metadata":{"namespace":"dev"}}`,
		`{"metadata":{"name":"rent"}}`,
		`{"metadata":{"namespace":"dev","name":42}}`,
		`{"metadata":{"namespace":"../dev","name":"rent"}}`,
		`{"metadata":{"namespace":"dev","name":"INVALID"}}`,
		`{"metadata":{"namespace":"dev","name":"rent"}} {}`,
	} {
		for _, hook := range []func(context.Context, *reconciler.ReconcilerHooksRequest) (*reconciler.ReconcilerHooksResult, error){service.Pre, service.Post} {
			result, err := hook(context.Background(), &reconciler.ReconcilerHooksRequest{ClusterDefinition: []byte(payload)})
			if err != nil || result.GetBehavior() != reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE {
				t.Fatalf("invalid payload %q blocked reconciliation: %v, %v", payload, result, err)
			}
		}
	}
	result, err := service.Pre(context.Background(), nil)
	if err != nil || result.GetBehavior() != reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE {
		t.Fatalf("nil request blocked reconciliation: %v, %v", result, err)
	}
	if got := notifier.notifications(); len(got) != 0 {
		t.Fatalf("invalid payloads triggered observation work: %v", got)
	}
}

type queuedNotifier chan notification

func (n queuedNotifier) Notify(namespace, name string) {
	select {
	case n <- notification{namespace, name}:
	default:
	}
}

func TestHooksDoNotWaitForObservation(t *testing.T) {
	// No worker consumes this queue. Both hooks must still return once their
	// notification is queued or coalesced, even if collection cannot proceed.
	queue := make(queuedNotifier, 1)
	service := &Service{notifier: queue}
	request := &reconciler.ReconcilerHooksRequest{ClusterDefinition: []byte(`{"metadata":{"namespace":"dev","name":"rent"}}`)}
	done := make(chan error, 1)
	go func() {
		_, err := service.Pre(context.Background(), request)
		if err == nil {
			_, err = service.Post(context.Background(), request)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("hooks waited for an observation to complete")
	}
	if got := <-queue; got != (notification{"dev", "rent"}) {
		t.Fatalf("queued wrong cluster: %v", got)
	}
}
