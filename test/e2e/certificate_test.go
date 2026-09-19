//go:build e2e

package e2e

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

// TestLiveDiscoveryCertificateRotation verifies a real cert-manager leaf renewal
// and the discovery listener's hot reload, without restarting the Deployment.
func TestLiveDiscoveryCertificateRotation(t *testing.T) {
	if os.Getenv("CNPG_CONNECT_E2E_TLS_ROTATION") != "1" {
		t.Skip("opt in with CNPG_CONNECT_E2E_TLS_ROTATION=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	transport := &rotationConnectionStats{}
	h := newHarness(t, ctx, grpc.WithStatsHandler(transport))
	h.waitGet(t, ctx, "healthy topology before leaf rotation", healthyThree)
	configuration, err := clientcmd.BuildConfigFromFlags("", h.kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	configuration.Timeout = 15 * time.Second
	client, err := dynamic.NewForConfig(configuration)
	if err != nil {
		t.Fatal(err)
	}
	certificates := client.Resource(schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}).Namespace(pluginNamespace)
	original, err := certificates.Get(ctx, "connect-application", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if original.GetUID() == "" {
		t.Fatal("test Certificate has no UID")
	}
	originalNames, _, err := unstructured.NestedStringSlice(original.Object, "spec", "dnsNames")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := h.kubernetes.CoreV1().Secrets(pluginNamespace).Get(ctx, "connect-application-tls", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data["ca.crt"]) {
		t.Fatal("missing discovery CA")
	}
	serial := func(endpoint string) (string, error) {
		return rotationSerial(h.auth(ctx), endpoint, roots, os.Getenv("CNPG_CONNECT_E2E_SERVER_NAME"))
	}
	beforePods, err := h.rotationPodStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	forwards, beforeSerials := map[string]*rotationForward{}, map[string]string{}
	for name := range beforePods {
		forward := h.forwardRotationPod(t, ctx, name)
		before, err := serial(forward.endpoint)
		if err != nil {
			t.Fatalf("initial certificate on %s: %v; port-forward: %s", name, err, forward.status())
		}
		forwards[name], beforeSerials[name] = forward, before
	}
	connectionBegins, connectionEnds := transport.begins.Load(), transport.ends.Load()
	if connectionBegins == 0 {
		t.Fatal("pre-rotation gRPC transport was not established")
	}
	setNames := func(ctx context.Context, names []string) error {
		return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			if err := h.guardContext(); err != nil {
				return err
			}
			current, err := certificates.Get(ctx, original.GetName(), metav1.GetOptions{})
			if err != nil {
				return err
			}
			if current.GetUID() != original.GetUID() {
				return fmt.Errorf("Certificate identity changed")
			}
			if err := unstructured.SetNestedStringSlice(current.Object, names, "spec", "dnsNames"); err != nil {
				return err
			}
			_, err = certificates.Update(ctx, current, metav1.UpdateOptions{})
			return err
		})
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := setNames(cleanup, originalNames); err != nil {
			t.Errorf("restore Certificate SANs: %v", err)
		}
	})
	names := append(append([]string(nil), originalNames...), fmt.Sprintf("rotation-%d.invalid", time.Now().UnixNano()))
	started := time.Now()
	if err := setNames(ctx, names); err != nil {
		t.Fatal(err)
	}
	lastProbes := map[string]string{}
	for {
		allRenewed := true
		for name, forward := range forwards {
			select {
			case <-forward.done:
				t.Fatalf("certificate probe lost port-forward for %s: %s; last probes: %v", name, forward.status(), lastProbes)
			default:
			}
			current, err := serial(forward.endpoint)
			lastProbes[name] = fmt.Sprintf("before=%s current=%s error=%v", beforeSerials[name], current, err)
			if err != nil || current == beforeSerials[name] {
				allRenewed = false
			}
		}
		if allRenewed {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("certificate did not renew and hot-reload: %v; per-Pod probes: %v", ctx.Err(), lastProbes)
		case <-time.After(time.Second):
		}
	}
	// A pre-rotation gRPC connection must continue to work during leaf reload.
	h.waitGet(t, ctx, "healthy topology after leaf rotation", healthyThree)
	if transport.begins.Load() != connectionBegins || transport.ends.Load() != connectionEnds {
		t.Fatal("gRPC transport reconnected or closed during certificate rotation")
	}
	afterPods, err := h.rotationPodStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !sameRotationPods(beforePods, afterPods) {
		t.Fatal("discovery Pod identities or container restart counts changed during rotation")
	}
	t.Logf("verified leaf reload on all %d replicas in %s; unchanged Pod identities/restart counts and uninterrupted gRPC transport", len(forwards), time.Since(started).Round(time.Millisecond))
}
