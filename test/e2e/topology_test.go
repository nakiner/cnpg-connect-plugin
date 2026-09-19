//go:build e2e

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

const (
	expectedContext  = "kind-cnpg-connect-test"
	clusterNamespace = "databases"
	clusterName      = "app-db"
	pluginNamespace  = "cnpg-system"
	pluginName       = "connect.cnpg.io"
)

var clusterResource = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"}

// TestLiveTopologyLifecycle requires an explicitly selected, isolated kind
// cluster. It never uses the default kubeconfig or accepts another context.
// Setup/deployment is deliberately external so its exact images and manifests
// remain reviewable and the test cannot accidentally create an environment.
func TestLiveTopologyLifecycle(t *testing.T) {
	if os.Getenv("CNPG_CONNECT_E2E_KUBECONFIG") == "" {
		t.Skip("opt in with CNPG_CONNECT_E2E_KUBECONFIG for the isolated kind-cnpg-connect-test context")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	initialCluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h.clusterUID = initialCluster.GetUID()
	if h.clusterUID == "" {
		t.Fatal("test Cluster has no UID")
	}
	originalSync, _, err := unstructured.NestedFieldCopy(initialCluster.Object, "spec", "postgresql", "synchronous")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := h.patchSync(cleanupCtx, originalSync); err != nil {
			t.Errorf("restore original synchronous configuration: %v", err)
		}
	})

	t.Run("tls_discovery", func(t *testing.T) {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 15*time.Second)
		defer rpcCancel()
		wantCode := codes.OK
		if h.token != "" {
			wantCode = codes.Unauthenticated
		}
		_, err := h.client.GetTopology(rpcCtx, getRequest())
		if status.Code(err) != wantCode {
			t.Fatalf("Get without bearer token: expected %v, got %v", wantCode, err)
		}
		stream, err := h.client.WatchTopology(rpcCtx, watchRequest())
		if err == nil {
			_, err = stream.Recv()
		}
		if status.Code(err) != wantCode {
			t.Fatalf("Watch without bearer token: expected %v, got %v", wantCode, err)
		}
	})
	if t.Failed() {
		return
	}

	// Reset only the synchronous policy so this test can be rerun after a prior
	// successful or interrupted execution. Cleanup restores the original policy.
	if err := h.patchSync(ctx, nil); err != nil {
		t.Fatal(err)
	}
	initial := h.waitGet(t, ctx, "primary and two asynchronous standbys", func(snapshot *connectv1.Snapshot) bool {
		return healthyThree(snapshot) && countSync(snapshot, connectv1.SyncState_SYNC_STATE_ASYNC) == 2
	})
	h.waitPluginRegistration(t, ctx)
	t.Logf("async topology: %s", summarize(initial))

	var synchronous *connectv1.Snapshot
	t.Run("sync_membership_stream", func(t *testing.T) {
		phaseCtx, phaseCancel := context.WithTimeout(ctx, 3*time.Minute)
		defer phaseCancel()
		stream, err := h.client.WatchTopology(h.auth(phaseCtx), watchRequest())
		if err != nil {
			t.Fatal(err)
		}
		first, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if first.Revision != initial.Revision || first.PrimaryId != initial.PrimaryId || len(first.Members) != 3 {
			t.Fatalf("initial Watch differs from current complete topology: %s", summarize(first))
		}
		policy := map[string]interface{}{"method": "first", "number": int64(1), "maxStandbyNamesFromCluster": int64(1), "dataDurability": "required"}
		if err := h.patchSync(phaseCtx, policy); err != nil {
			t.Fatal(err)
		}
		synchronous = waitStream(t, stream, "one synchronous and one asynchronous standby", func(snapshot *connectv1.Snapshot) bool {
			return healthyThree(snapshot) && countSync(snapshot, connectv1.SyncState_SYNC_STATE_SYNC) == 1 && countSync(snapshot, connectv1.SyncState_SYNC_STATE_ASYNC) == 1
		})
		if synchronous.Revision == initial.Revision {
			t.Fatal("sync membership changed without a routing revision change")
		}
		t.Logf("sync topology: %s", summarize(synchronous))
	})
	if t.Failed() {
		return
	}

	var switched *connectv1.Snapshot
	t.Run("planned_switchover_stream", func(t *testing.T) {
		phaseCtx, phaseCancel := context.WithTimeout(ctx, 3*time.Minute)
		defer phaseCancel()
		stream, err := h.client.WatchTopology(h.auth(phaseCtx), watchRequest())
		if err != nil {
			t.Fatal(err)
		}
		before, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		var target *connectv1.Member
		for _, member := range before.Members {
			if member.Ready && member.Role == connectv1.Role_ROLE_STANDBY && member.SyncState == connectv1.SyncState_SYNC_STATE_SYNC {
				target = member
				break
			}
		}
		if target == nil {
			t.Fatalf("no synchronous promotion target: %s", summarize(before))
		}
		if err := h.promote(phaseCtx, target); err != nil {
			t.Fatal(err)
		}
		switched = waitStream(t, stream, "promoted primary and recovered old primary", func(snapshot *connectv1.Snapshot) bool {
			return healthyThree(snapshot) && snapshot.PrimaryId == target.Id && snapshot.PrimaryId != before.PrimaryId
		})
		if switched.Revision == before.Revision {
			t.Fatal("primary identity changed without a routing revision change")
		}
		for _, member := range switched.Members {
			if member.Id == before.PrimaryId && member.Ready && member.Role == connectv1.Role_ROLE_PRIMARY {
				t.Fatal("old primary remains eligible as primary after switchover")
			}
		}
		t.Logf("switched topology: %s", summarize(switched))
	})
	if t.Failed() {
		return
	}

	t.Run("reconnect_full_snapshot", func(t *testing.T) {
		phaseCtx, phaseCancel := context.WithTimeout(ctx, 30*time.Second)
		defer phaseCancel()
		stream, err := h.client.WatchTopology(h.auth(phaseCtx), watchRequest())
		if err != nil {
			t.Fatal(err)
		}
		current, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if !healthyThree(current) || current.PrimaryId != switched.PrimaryId || current.Cluster.Uid != string(h.clusterUID) {
			t.Fatalf("reconnect did not begin with complete current topology: %s", summarize(current))
		}
	})
	if t.Failed() {
		return
	}

	if os.Getenv("CNPG_CONNECT_E2E_REPLACE_STANDBY") == "1" {
		t.Run("standby_replacement_identity", func(t *testing.T) {
			phaseCtx, phaseCancel := context.WithTimeout(ctx, 3*time.Minute)
			defer phaseCancel()
			stream, err := h.client.WatchTopology(h.auth(phaseCtx), watchRequest())
			if err != nil {
				t.Fatal(err)
			}
			before, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			var target *connectv1.Member
			for _, member := range before.Members {
				if member.Ready && member.Role == connectv1.Role_ROLE_STANDBY && member.SyncState == connectv1.SyncState_SYNC_STATE_ASYNC {
					target = member
					break
				}
			}
			if target == nil {
				t.Fatalf("no asynchronous standby to replace: %s", summarize(before))
			}
			if err := h.deleteStandby(phaseCtx, target); err != nil {
				t.Fatal(err)
			}
			replaced := waitStream(t, stream, "replacement standby with a new Pod UID", func(snapshot *connectv1.Snapshot) bool {
				if !healthyThree(snapshot) {
					return false
				}
				for _, member := range snapshot.Members {
					if member.Name == target.Name {
						return member.Id != target.Id && member.Ready && member.Role == connectv1.Role_ROLE_STANDBY
					}
				}
				return false
			})
			if replaced.Revision == before.Revision {
				t.Fatal("Pod replacement changed identity without routing revision")
			}
			t.Logf("replacement topology: %s", summarize(replaced))
		})
	}
}

type harness struct {
	kubeconfig string
	kubernetes kubernetes.Interface
	clusters   dynamic.ResourceInterface
	client     connectv1.TopologyServiceClient
	token      string
	clusterUID types.UID
}

func newHarness(t *testing.T, ctx context.Context, options ...grpc.DialOption) *harness {
	t.Helper()
	h := &harness{kubeconfig: os.Getenv("CNPG_CONNECT_E2E_KUBECONFIG")}
	if err := h.guardContext(); err != nil {
		t.Fatal(err)
	}
	endpoint := os.Getenv("CNPG_CONNECT_E2E_ENDPOINT")
	serverName := os.Getenv("CNPG_CONNECT_E2E_SERVER_NAME")
	if endpoint == "" || serverName == "" {
		t.Fatal("CNPG_CONNECT_E2E_ENDPOINT and CNPG_CONNECT_E2E_SERVER_NAME must be explicitly supplied")
	}
	configuration, err := clientcmd.BuildConfigFromFlags("", h.kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	configuration.Timeout = 15 * time.Second
	h.kubernetes, err = kubernetes.NewForConfig(configuration)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(configuration)
	if err != nil {
		t.Fatal(err)
	}
	h.clusters = dynamicClient.Resource(clusterResource).Namespace(clusterNamespace)
	if name := os.Getenv("CNPG_CONNECT_E2E_TOKEN_SECRET"); name != "" {
		tokenSecret, err := h.kubernetes.CoreV1().Secrets(pluginNamespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("read optional discovery token Secret: %v", err)
		}
		h.token = strings.TrimSpace(string(tokenSecret.Data["token"]))
		if len(h.token) < 32 {
			t.Fatal("optional discovery token Secret must contain a token of at least 32 characters")
		}
	}
	certificate, err := h.kubernetes.CoreV1().Secrets(pluginNamespace).Get(ctx, "connect-application-tls", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("read discovery CA Secret: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificate.Data["ca.crt"]) {
		t.Fatal("discovery CA Secret has no valid ca.crt")
	}
	options = append([]grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: serverName,
	}))}, options...)
	connection, err := grpc.NewClient(endpoint, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	h.client = connectv1.NewTopologyServiceClient(connection)
	return h
}

func (h *harness) guardContext() error {
	configuration, err := clientcmd.LoadFromFile(h.kubeconfig)
	if err != nil {
		return fmt.Errorf("read explicit e2e kubeconfig: %w", err)
	}
	if configuration.CurrentContext != expectedContext {
		return fmt.Errorf("refusing e2e access: current context must be %q, got %q", expectedContext, configuration.CurrentContext)
	}
	return nil
}

func (h *harness) auth(ctx context.Context) context.Context {
	if h.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+h.token)
}

func getRequest() *connectv1.GetTopologyRequest {
	return &connectv1.GetTopologyRequest{Namespace: clusterNamespace, Name: clusterName}
}
func watchRequest() *connectv1.WatchTopologyRequest {
	return &connectv1.WatchTopologyRequest{Namespace: clusterNamespace, Name: clusterName}
}

func (h *harness) patchSync(ctx context.Context, configuration interface{}) error {
	return h.patchCluster(ctx, "", func(cluster *unstructured.Unstructured) map[string]interface{} {
		return map[string]interface{}{"metadata": map[string]interface{}{"resourceVersion": cluster.GetResourceVersion()}, "spec": map[string]interface{}{"postgresql": map[string]interface{}{"synchronous": configuration}}}
	})
}

func (h *harness) promote(ctx context.Context, target *connectv1.Member) error {
	if err := h.verifyPod(ctx, target); err != nil {
		return err
	}
	// Mirrors CNPG 1.30 kubectl-cnpg promote: targetPrimary, its timestamp, and
	// switchover phase via an optimistic-lock status patch. The operator owns
	// promotion/fencing and reconciles its Ready condition.
	// https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/cmd/plugin/promote/promote.go
	return h.patchCluster(ctx, "status", func(cluster *unstructured.Unstructured) map[string]interface{} {
		return map[string]interface{}{"metadata": map[string]interface{}{"resourceVersion": cluster.GetResourceVersion()}, "status": map[string]interface{}{
			"targetPrimary": target.Name, "targetPrimaryTimestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"phase": "Switchover in progress", "phaseReason": "Switching over to " + target.Name,
		}}
	})
}

func (h *harness) patchCluster(ctx context.Context, subresource string, build func(*unstructured.Unstructured) map[string]interface{}) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := h.guardContext(); err != nil {
			return err
		}
		cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if cluster.GetUID() != h.clusterUID || cluster.GetName() != clusterName || cluster.GetNamespace() != clusterNamespace {
			return fmt.Errorf("refusing mutation: fixed e2e Cluster identity changed")
		}
		patch, err := json.Marshal(build(cluster))
		if err != nil {
			return err
		}
		var subresources []string
		if subresource != "" {
			subresources = []string{subresource}
		}
		_, err = h.clusters.Patch(ctx, clusterName, types.MergePatchType, patch, metav1.PatchOptions{}, subresources...)
		return err
	})
}

func (h *harness) verifyPod(ctx context.Context, member *connectv1.Member) error {
	if err := h.guardContext(); err != nil {
		return err
	}
	if !strings.HasPrefix(member.Name, clusterName+"-") || member.Id == "" {
		return fmt.Errorf("refusing mutation of a Pod outside the fixed e2e Cluster")
	}
	pod, err := h.kubernetes.CoreV1().Pods(clusterNamespace).Get(ctx, member.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if string(pod.UID) != member.Id || pod.Labels["cnpg.io/cluster"] != clusterName {
		return fmt.Errorf("refusing mutation: target Pod identity or Cluster label changed")
	}
	for _, owner := range pod.OwnerReferences {
		if owner.UID == h.clusterUID && owner.Kind == "Cluster" && owner.Name == clusterName {
			return nil
		}
	}
	return fmt.Errorf("refusing mutation: target Pod is not owned by the fixed e2e Cluster")
}

func (h *harness) deleteStandby(ctx context.Context, member *connectv1.Member) error {
	if err := h.verifyPod(ctx, member); err != nil {
		return err
	}
	cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	current, _, _ := unstructured.NestedString(cluster.Object, "status", "currentPrimary")
	target, _, _ := unstructured.NestedString(cluster.Object, "status", "targetPrimary")
	if current == member.Name || target == member.Name {
		return fmt.Errorf("refusing deletion: target is a current or intended primary")
	}
	uid := types.UID(member.Id)
	return h.kubernetes.CoreV1().Pods(clusterNamespace).Delete(ctx, member.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
}

func (h *harness) waitGet(t *testing.T, parent context.Context, description string, matches func(*connectv1.Snapshot) bool) *connectv1.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last string
	for {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		snapshot, err := h.client.GetTopology(h.auth(rpcCtx), getRequest())
		rpcCancel()
		if err == nil {
			last = summarize(snapshot)
			if matches(snapshot) {
				return snapshot
			}
		} else {
			last = status.Code(err).String()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v; last=%s", description, ctx.Err(), last)
			return nil
		case <-time.After(time.Second):
		}
	}
}

func (h *harness) waitPluginRegistration(t *testing.T, parent context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	for {
		cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
		if err == nil {
			plugins, _, _ := unstructured.NestedSlice(cluster.Object, "status", "pluginStatus")
			for _, value := range plugins {
				plugin, ok := value.(map[string]interface{})
				if !ok {
					continue
				}
				name, _, _ := unstructured.NestedString(plugin, "name")
				version, _, _ := unstructured.NestedString(plugin, "version")
				if name == pluginName && version != "" {
					t.Logf("CNPG registered plugin %s version %s", name, version)
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("CNPG never recorded plugin %s in status.pluginStatus", pluginName)
			return
		case <-time.After(time.Second):
		}
	}
}

func waitStream(t *testing.T, stream connectv1.TopologyService_WatchTopologyClient, description string, matches func(*connectv1.Snapshot) bool) *connectv1.Snapshot {
	t.Helper()
	var last string
	for {
		snapshot, err := stream.Recv()
		if err != nil {
			t.Fatalf("waiting for %s: %v; last=%s", description, err, last)
			return nil
		}
		last = summarize(snapshot)
		if matches(snapshot) {
			return snapshot
		}
	}
}

func healthyThree(snapshot *connectv1.Snapshot) bool {
	if snapshot == nil || !snapshot.Available || snapshot.Transitioning || snapshot.PrimaryId == "" || len(snapshot.Members) != 3 || snapshot.ValidUntil == nil || !time.Now().Before(snapshot.ValidUntil.AsTime()) {
		return false
	}
	primaries, standbys := 0, 0
	for _, member := range snapshot.Members {
		if !member.Ready || member.Id == "" {
			return false
		}
		if member.Role == connectv1.Role_ROLE_PRIMARY {
			primaries++
			if member.Id != snapshot.PrimaryId {
				return false
			}
		}
		if member.Role == connectv1.Role_ROLE_STANDBY {
			standbys++
		}
		endpoint := member.Endpoints["internal"]
		if endpoint == nil || endpoint.Host == "" || endpoint.Port == 0 {
			return false
		}
	}
	return primaries == 1 && standbys == 2
}

func countSync(snapshot *connectv1.Snapshot, state connectv1.SyncState) int {
	count := 0
	for _, member := range snapshot.Members {
		if member.Role == connectv1.Role_ROLE_STANDBY && member.SyncState == state {
			count++
		}
	}
	return count
}

func summarize(snapshot *connectv1.Snapshot) string {
	if snapshot == nil {
		return "nil"
	}
	members := make([]string, 0, len(snapshot.Members))
	for _, member := range snapshot.Members {
		members = append(members, fmt.Sprintf("%s uid=%s role=%s sync=%s ready=%t", member.Name, member.Id, member.Role, member.SyncState, member.Ready))
	}
	return fmt.Sprintf("revision=%s available=%t transitioning=%t primary=%s members=[%s]", snapshot.Revision, snapshot.Available, snapshot.Transitioning, snapshot.PrimaryId, strings.Join(members, "; "))
}
