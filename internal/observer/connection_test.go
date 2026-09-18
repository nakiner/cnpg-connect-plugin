package observer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func testPublicCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestApplicationDatabaseUsesBootstrapMetadata(t *testing.T) {
	for _, method := range []string{"initdb", "recovery", "pg_basebackup"} {
		t.Run(method, func(t *testing.T) {
			cluster, _, _ := fixture()
			if got := applicationDatabase(cluster); got != "" {
				t.Fatalf("inferred database from cluster name: %q", got)
			}
			_ = unstructured.SetNestedField(cluster.Object, "actual_database", "spec", "bootstrap", method, "database")
			if got := applicationDatabase(cluster); got != "actual_database" {
				t.Fatalf("database=%q", got)
			}
		})
	}
}

func TestConnectionParametersReadOnlyNamedPublicCA(t *testing.T) {
	o, kube, dyn := fakeObserver(t)
	ctx := context.Background()
	cluster, err := dyn.Resource(clusterResource).Namespace("test").Get(ctx, "db", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	public := testPublicCA(t)
	private := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("must never be published")})
	_, err = kube.CoreV1().Secrets("test").Update(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-ca", Namespace: "test"}, Data: map[string][]byte{
		"ca.crt": append(append([]byte(nil), public...), private...), "ca.key": private, "password": []byte("database-password"),
	}}, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kube.ClearActions()
	got, err := o.connectionParameters(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	if got.Database != "app" || !bytes.Equal(got.ServerCAPEM, public) {
		t.Fatal("lost public connection defaults or published unrelated Secret data")
	}
	actions := kube.Actions()
	if len(actions) != 1 || actions[0].GetVerb() != "get" || actions[0].GetResource().Resource != "secrets" || actions[0].GetNamespace() != "test" {
		t.Fatalf("unexpected API requests: %+v", actions)
	}
	cluster.SetNamespace("other")
	if _, err := o.connectionParameters(ctx, cluster); err == nil {
		t.Fatal("CA escaped the requested cluster namespace")
	}
}

func TestConnectionCARotationAndLoss(t *testing.T) {
	o, kube, _ := fakeObserver(t)
	ctx := context.Background()
	observe(t, o)
	before, _ := o.store.Get("test", "db")
	public := testPublicCA(t)
	if _, err := kube.CoreV1().Secrets("test").Update(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-ca", Namespace: "test"}, Data: map[string][]byte{"ca.crt": public}}, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	o.connections.Delete(clusterKey{"test", "db"})
	observe(t, o)
	after, _ := o.store.Get("test", "db")
	if !after.Available || after.Revision == before.Revision || !bytes.Equal(after.Connection.ServerCAPEM, public) {
		t.Fatal("CA rotation did not refresh connection defaults and revision")
	}
	if err := kube.CoreV1().Secrets("test").Delete(ctx, "db-ca", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	o.connections.Delete(clusterKey{"test", "db"})
	observe(t, o)
	failed, _ := o.store.Get("test", "db")
	if failed.Available || failed.Reason != "connection_defaults_unavailable" {
		t.Fatal("missing CA silently published unverified connection defaults")
	}
}

func TestNetworkFailureDoesNotRefetchCAAndTLSFailureDoes(t *testing.T) {
	o, kube, _ := fakeObserver(t)
	observe(t, o)
	kube.ClearActions()
	original := o.probe
	failure := error(errors.New("connection refused"))
	o.probe = func(ctx context.Context, p *corev1.Pod, c v1.ConnectionParameters, s string) (instanceStatus, error) {
		if p.Name == "db-1" {
			return instanceStatus{}, failure
		}
		return original(ctx, p, c, s)
	}
	for range 3 {
		observe(t, o)
	}
	if len(kube.Actions()) != 0 {
		t.Fatal("network outage caused CA reads")
	}
	failure = fmt.Errorf("request failed: %w", &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}})
	observe(t, o)
	o.probe = original
	observe(t, o)
	if len(kube.Actions()) != 1 {
		t.Fatalf("CA verification recovery requests=%d", len(kube.Actions()))
	}
}

func TestCARefreshesAreSpreadAcrossClusters(t *testing.T) {
	c, _, _ := fixture()
	seen := make(map[time.Duration]bool)
	for i := range 2000 {
		c.SetName(fmt.Sprint(i))
		lifetime := connectionCacheLifetime(c)
		if lifetime < 3*time.Minute || lifetime >= 5*time.Minute {
			t.Fatalf("unexpected lifetime %s", lifetime)
		}
		seen[lifetime] = true
	}
	if len(seen) < 1900 {
		t.Fatal("CA refreshes converge on the same deadline")
	}
}
