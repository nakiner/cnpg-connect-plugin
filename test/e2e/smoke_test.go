//go:build e2e

package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestLiveTopologySmoke is read-only and supports either enrollment mode. It is
// suitable after restarting a port forward following observer Pod replacement.
func TestLiveTopologySmoke(t *testing.T) {
	if os.Getenv("CNPG_CONNECT_E2E_KUBECONFIG") == "" {
		t.Skip("opt in with the isolated e2e kubeconfig and discovery endpoint")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	current := h.waitGet(t, ctx, "healthy topology after observer recovery", healthyThree)
	if current.Cluster == nil || current.Cluster.Uid != string(cluster.GetUID()) {
		t.Fatal("GetTopology returned a different Cluster identity")
	}
	stream, err := h.client.WatchTopology(h.auth(ctx), watchRequest())
	if err != nil {
		t.Fatal(err)
	}
	initial, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if !healthyThree(initial) || initial.Cluster == nil || initial.Cluster.Uid != current.Cluster.Uid || initial.PrimaryId != current.PrimaryId {
		t.Fatalf("WatchTopology initial snapshot differs from healthy GetTopology state: %s", summarize(initial))
	}
	t.Logf("authenticated TLS GetTopology and WatchTopology recovered: %s", summarize(initial))
}
