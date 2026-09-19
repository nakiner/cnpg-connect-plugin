//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/retry"
)

// A public bundle update must reach an existing stream without waiting for a
// cache timer. Keep the original root so the isolated database remains usable.
func TestLiveCABundleUpdate(t *testing.T) {
	if os.Getenv("CNPG_CONNECT_E2E_TLS_ROTATION") != "1" {
		t.Skip("opt in with CNPG_CONNECT_E2E_TLS_ROTATION=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	name, _, _ := unstructured.NestedString(cluster.Object, "status", "certificates", "serverCASecret")
	certificates, _, _ := unstructured.NestedFieldCopy(cluster.Object, "status", "certificates")
	secrets := h.kubernetes.CoreV1().Secrets(clusterNamespace)
	original, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	publicCA := original.Data["ca.crt"]
	if len(publicCA) == 0 {
		t.Fatal("database has no public CA")
	}
	setCA := func(ctx context.Context, bundle []byte) error {
		return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			if err := h.guardContext(); err != nil {
				return err
			}
			secret, err := secrets.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if secret.UID != original.UID {
				return fmt.Errorf("CA Secret identity changed")
			}
			secret.Data["ca.crt"] = bundle
			_, err = secrets.Update(ctx, secret, metav1.UpdateOptions{})
			return err
		})
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if err := setCA(cleanup, publicCA); err != nil {
			t.Errorf("restore database CA bundle: %v", err)
		}
	})
	stream, err := h.client.WatchTopology(h.auth(ctx), watchRequest())
	if err != nil {
		t.Fatal(err)
	}
	waitStream(t, stream, "original CA", func(s *connectv1.Snapshot) bool {
		return healthyThree(s) && bytes.Equal(s.GetConnection().GetServerCaPem(), publicCA)
	})
	// Repeating the root changes the public bundle without changing the root's
	// identity, expiry, or the Cluster's certificate configuration.
	bundle := append(bytes.Clone(publicCA), publicCA...)
	started := time.Now()
	if err := setCA(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	waitStream(t, stream, "updated CA bundle", func(s *connectv1.Snapshot) bool {
		return healthyThree(s) && bytes.Equal(s.GetConnection().GetServerCaPem(), bundle)
	})
	current, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	currentCertificates, _, _ := unstructured.NestedFieldCopy(current.Object, "status", "certificates")
	if !reflect.DeepEqual(certificates, currentCertificates) {
		t.Fatal("Cluster certificate metadata changed; Secret-only update was not isolated")
	}
	t.Logf("CA Secret update reached existing gRPC stream in %s", time.Since(started))
}
