package observer

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

func TestConnectionReadCancellationIsPerWaiter(t *testing.T) {
	var group connectionReadGroup
	key := connectionReadKey{namespace: "test", secret: "shared-ca"}
	started := make(chan context.Context, 1)
	finish := make(chan struct{})
	var calls atomic.Int64
	fetch := func(ctx context.Context) ([]byte, error) {
		calls.Add(1)
		started <- ctx
		select {
		case <-finish:
			return []byte("public"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	firstCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := group.read(firstCtx, key, time.Second, fetch); first <- err }()
	requestCtx := <-started
	second := make(chan error, 1)
	go func() { _, err := group.read(context.Background(), key, time.Second, fetch); second <- err }()
	waitFor(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return group.pending[key] != nil && group.pending[key].waiters == 2
	})
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first waiter error=%v", err)
	}
	if requestCtx.Err() != nil {
		t.Fatal("one canceled waiter canceled a shared CA request")
	}
	close(finish)
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("overlapping reads=%d", calls.Load())
	}
}

func TestCanceledConnectionReadCannotRemoveItsReplacement(t *testing.T) {
	var group connectionReadGroup
	key := connectionReadKey{namespace: "test", secret: "shared-ca"}
	firstCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan context.Context, 1)
	oldFinished := make(chan struct{})
	allowOldCompletion := make(chan struct{})
	defer close(allowOldCompletion)
	first := make(chan error, 1)
	go func() {
		_, err := group.read(firstCtx, key, time.Second, func(ctx context.Context) ([]byte, error) {
			started <- ctx
			<-ctx.Done()
			<-allowOldCompletion
			close(oldFinished)
			return nil, ctx.Err()
		})
		first <- err
	}()
	requestCtx := <-started
	group.mu.Lock()
	oldCall := group.pending[key]
	group.mu.Unlock()
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("first waiter error=%v", err)
	}
	if requestCtx.Err() == nil {
		t.Fatal("last waiter did not cancel the request")
	}
	newStarted := make(chan struct{})
	allowNewCompletion := make(chan struct{})
	second := make(chan error, 1)
	go func() {
		_, err := group.read(context.Background(), key, time.Second, func(context.Context) ([]byte, error) {
			close(newStarted)
			<-allowNewCompletion
			return []byte("new CA"), nil
		})
		second <- err
	}()
	<-newStarted
	group.mu.Lock()
	replacement := group.pending[key]
	group.mu.Unlock()
	// Let the old transport finish late, after a new request owns the key.
	allowOldCompletion <- struct{}{}
	<-oldFinished
	<-oldCall.done
	group.mu.Lock()
	preserved := group.pending[key] == replacement
	group.mu.Unlock()
	if !preserved {
		t.Fatal("an old request removed its replacement")
	}
	close(allowNewCompletion)
	if err := <-second; err != nil {
		t.Fatalf("replacement read failed: %v", err)
	}
}

func TestConnectionReadsKeepNamespacesAndCertificateViewsSeparate(t *testing.T) {
	var group connectionReadGroup
	keys := []connectionReadKey{
		{namespace: "a", secret: "ca", certificates: "old"},
		{namespace: "b", secret: "ca", certificates: "old"},
		{namespace: "a", secret: "other-ca", certificates: "old"},
		{namespace: "a", secret: "ca", certificates: "rotated"},
	}
	started := make(chan struct{}, len(keys))
	finish := make(chan struct{})
	done := make(chan error, len(keys))
	for _, key := range keys {
		go func() {
			_, err := group.read(context.Background(), key, time.Second, func(context.Context) ([]byte, error) {
				started <- struct{}{}
				<-finish
				return nil, nil
			})
			done <- err
		}()
	}
	for range keys {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("unrelated CA reads were coalesced")
		}
	}
	close(finish)
	for range keys {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestCachedConnectionInvalidationNeverExtendsCAFreshness(t *testing.T) {
	o, kube, _ := fakeObserver(t)
	cluster, _ := o.cluster(clusterKey{"test", "db"})
	key := clusterKey{"test", "db"}
	ctx := context.Background()
	first, err := o.cachedConnection(ctx, cluster)
	if err != nil {
		t.Fatal(err)
	}
	kube.ClearActions()
	if _, err := o.cachedConnection(ctx, cluster); err != nil || len(kube.Actions()) != 0 {
		t.Fatal("valid per-Cluster CA cache was not reused")
	}
	for _, change := range []string{"metadata", "uid", "expiry"} {
		t.Run(change, func(t *testing.T) {
			public := testPublicCA(t)
			_, err := kube.CoreV1().Secrets("test").Update(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "db-ca", Namespace: "test"},
				Data:       map[string][]byte{"ca.crt": public},
			}, metav1.UpdateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "metadata":
				_ = unstructured.SetNestedField(cluster.Object, "rotated", "status", "certificates", "expirations", "db-ca")
			case "uid":
				cluster.SetUID(types.UID("replacement"))
			case "expiry":
				entry, _ := o.connections.Load(key)
				cached := entry.(cachedConnection)
				cached.refreshed = time.Now().Add(-5 * time.Minute)
				o.connections.Store(key, cached)
			}
			kube.ClearActions()
			got, err := o.cachedConnection(ctx, cluster)
			if err != nil || !bytes.Equal(got.ServerCAPEM, public) || len(kube.Actions()) != 1 {
				t.Fatalf("CA was not refreshed: error=%v API reads=%d", err, len(kube.Actions()))
			}
			if bytes.Equal(first.ServerCAPEM, got.ServerCAPEM) {
				t.Fatal("old CA remained after invalidation")
			}
		})
	}
	if err := kube.CoreV1().Secrets("test").Delete(ctx, "db-ca", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	entry, _ := o.connections.Load(key)
	cached := entry.(cachedConnection)
	cached.refreshed = time.Now().Add(-5 * time.Minute)
	o.connections.Store(key, cached)
	if got, err := o.cachedConnection(ctx, cluster); err == nil || len(got.ServerCAPEM) != 0 {
		t.Fatal("failed refresh served an expired CA")
	}
}

func TestChangedCertificateMetadataDoesNotJoinAnOlderRead(t *testing.T) {
	o, _, _ := fakeObserver(t)
	cluster, _ := o.cluster(clusterKey{"test", "db"})
	oldCA, newCA := testPublicCA(t), testPublicCA(t)
	firstStarted := make(chan struct{})
	finishFirst := make(chan struct{})
	var calls atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		public := newCA
		if calls.Add(1) == 1 {
			public = oldCA
			close(firstStarted)
			select {
			case <-finishFirst:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(corev1.Secret{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			Data:     map[string][]byte{"ca.crt": public},
		})
	}))
	defer api.Close()
	defer close(finishFirst)
	var err error
	o.kube, err = kubernetes.NewForConfig(&rest.Config{Host: api.URL, QPS: 100, Burst: 200})
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() {
		_, err := o.connectionParameters(context.Background(), cluster)
		first <- err
	}()
	<-firstStarted
	changed := cluster.DeepCopy()
	_ = unstructured.SetNestedField(changed.Object, "new", "status", "certificates", "expirations", "db-ca")
	got, err := o.connectionParameters(context.Background(), changed)
	if err != nil || !bytes.Equal(got.ServerCAPEM, newCA) {
		t.Fatalf("changed metadata joined old read: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("reads=%d, want distinct CA requests for changed metadata", calls.Load())
	}
	finishFirst <- struct{}{}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}
