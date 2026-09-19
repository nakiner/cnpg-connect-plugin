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
	"sync"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	metadatafake "k8s.io/client-go/metadata/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"
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

func installTestCA(t *testing.T, o *Observer, secret *corev1.Secret) {
	t.Helper()
	meta := &metav1.PartialObjectMetadata{ObjectMeta: secret.ObjectMeta}
	if err := o.secretInformer.GetIndexer().Add(meta); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: secret.Namespace, Name: secret.Name}
	o.cas[key] = &cachedCA{key: key, uid: secret.UID, resourceVersion: secret.ResourceVersion, public: secret.Data["ca.crt"]}
}

func testConnection(t *testing.T, o *Observer) connectionInfo {
	t.Helper()
	cluster, _ := o.cluster(clusterKey{"test", "db"})
	info, err := o.cachedConnection(context.Background(), cluster)
	if err != nil {
		t.Fatal(err)
	}
	return info
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
	o, kube, _ := fakeObserver(t)
	public := testPublicCA(t)
	private := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("must never be published")})
	_, err := kube.CoreV1().Secrets("test").Update(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-ca", Namespace: "test", UID: "ca-uid", ResourceVersion: "2"}, Data: map[string][]byte{
		"ca.crt": append(append([]byte(nil), public...), private...), "ca.key": private, "password": []byte("database-password"),
	}}, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	kube.ClearActions()
	got, err := o.readPublicCA(context.Background(), "test", "db-ca")
	if err != nil || !bytes.Equal(got.public, public) {
		t.Fatal("lost public CA or published unrelated Secret data", err)
	}
	actions := kube.Actions()
	if len(actions) != 1 || actions[0].GetVerb() != "get" || actions[0].GetResource().Resource != "secrets" || actions[0].GetNamespace() != "test" {
		t.Fatalf("unexpected API requests: %+v", actions)
	}
	if _, err := o.readPublicCA(context.Background(), "other", "db-ca"); err == nil {
		t.Fatal("CA escaped the requested namespace")
	}
}

func TestSecretWatchRotationDeletionAndRecreation(t *testing.T) {
	o, kube, _ := fakeObserver(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	defer func() { cancel(); <-done }()
	waitFor(t, o.Ready)
	_, unsubscribe := o.store.Subscribe("test", "db")
	defer unsubscribe()
	waitFor(t, func() bool { s, _ := o.store.Get("test", "db"); return s.Available })
	cluster, _ := o.cluster(clusterKey{"test", "db"})
	metaClient := o.metadata.(*metadatafake.FakeMetadataClient)
	for _, change := range []string{"rotate", "delete", "recreate"} {
		t.Run(change, func(t *testing.T) {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-ca", Namespace: "test", UID: "ca-uid", ResourceVersion: "2"}, Data: map[string][]byte{"ca.crt": testPublicCA(t)}}
			switch change {
			case "rotate":
				if _, err := kube.CoreV1().Secrets("test").Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "delete":
				if err := kube.CoreV1().Secrets("test").Delete(ctx, "db-ca", metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				if err := metaClient.Tracker().Delete(secretResource, "test", "db-ca"); err != nil {
					t.Fatal(err)
				}
				waitFor(t, func() bool {
					s, _ := o.store.Get("test", "db")
					return !s.Available && len(s.Connection.ServerCAPEM) == 0
				})
				return
			case "recreate":
				secret.UID, secret.ResourceVersion = "replacement-ca", "3"
				if _, err := kube.CoreV1().Secrets("test").Create(ctx, secret, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			meta := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: secret.ObjectMeta}
			var err error
			if change == "recreate" {
				err = metaClient.Tracker().Create(secretResource, meta, "test")
			} else {
				err = metaClient.Tracker().Update(secretResource, meta, "test")
			}
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool {
				s, _ := o.store.Get("test", "db")
				return s.Available && bytes.Equal(s.Connection.ServerCAPEM, secret.Data["ca.crt"])
			})
			current, _ := o.cluster(clusterKey{"test", "db"})
			if !sameClusterRoute(cluster, current) {
				t.Fatal("test unexpectedly changed Cluster metadata")
			}
		})
	}
}

func TestCAFetchesAreSharedAndOnlyDemanded(t *testing.T) {
	o, kube, _ := fakeObserver(t)
	key := types.NamespacedName{Namespace: "test", Name: "db-ca"}
	delete(o.cas, key)
	kube.ClearActions()
	if !o.fetchCA(context.Background(), key) || len(kube.Actions()) != 0 {
		t.Fatal("idle CA was fetched")
	}
	var workers sync.WaitGroup
	for i := range 100 {
		name := fmt.Sprintf("shared-%d", i)
		c, _, _ := addFleetCluster(t, o, name, "db-ca")
		o.clusterEvent(nil, c)
		_, cancel := o.store.Subscribe("test", name)
		defer cancel()
		workers.Go(func() { _, _ = o.cachedConnection(context.Background(), c) })
	}
	workers.Wait()
	if o.caQueue.Len() != 1 {
		t.Fatalf("queued CA reads=%d, want one", o.caQueue.Len())
	}
	o.workCA(context.Background())
	if len(kube.Actions()) != 1 {
		t.Fatalf("shared CA requests=%d", len(kube.Actions()))
	}
	for range 10 {
		testConnection(t, o)
	}
	if len(kube.Actions()) != 1 {
		t.Fatal("unchanged CA was fetched again")
	}
}

func TestLateCAReadCannotRestoreDeletedOrReplacedSecret(t *testing.T) {
	for _, change := range []string{"delete", "replace"} {
		t.Run(change, func(t *testing.T) {
			o, kube, _ := fakeObserver(t)
			key := types.NamespacedName{Namespace: "test", Name: "db-ca"}
			delete(o.cas, key)
			_, unsubscribe := o.store.Subscribe("test", "db")
			defer unsubscribe()
			started, release := make(chan struct{}), make(chan struct{})
			old, _ := kube.CoreV1().Secrets("test").Get(context.Background(), "db-ca", metav1.GetOptions{})
			kube.PrependReactor("get", "secrets", func(ktesting.Action) (bool, runtime.Object, error) { close(started); <-release; return true, old, nil })
			done := make(chan bool, 1)
			go func() { done <- o.fetchCA(context.Background(), key) }()
			<-started
			meta := o.secretMetadata(key).DeepCopy()
			if change == "delete" {
				_ = o.secretInformer.GetIndexer().Delete(meta)
			} else {
				meta.UID, meta.ResourceVersion = "replacement", "2"
				_ = o.secretInformer.GetIndexer().Update(meta)
			}
			o.secretEvent(meta)
			close(release)
			if <-done {
				t.Fatal("stale CA read was accepted")
			}
			if o.cas[key] != nil {
				t.Fatal("stale CA restored the cache")
			}
		})
	}
}

func TestSecretChangeDuringProbeCannotPublishOldCA(t *testing.T) {
	o, _, _ := fakeObserver(t)
	probe := o.probe
	o.probe = func(ctx context.Context, pod *corev1.Pod, c v1.ConnectionParameters, s string) (instanceStatus, error) {
		if pod.Name == "db-1" {
			meta := o.secretMetadata(types.NamespacedName{Namespace: "test", Name: "db-ca"}).DeepCopy()
			meta.ResourceVersion = "2"
			_ = o.secretInformer.GetIndexer().Update(meta) // Event delivery can lag this update.
		}
		return probe(ctx, pod, c, s)
	}
	observe(t, o)
	s, _ := o.store.Get("test", "db")
	if s.Available || len(s.Connection.ServerCAPEM) != 0 {
		t.Fatal("old CA published after Secret changed")
	}
}

func TestSecretChangeInvalidatesIdleSnapshotWithoutFetching(t *testing.T) {
	o, kube, _ := fakeObserver(t)
	observe(t, o)
	key := types.NamespacedName{Namespace: "test", Name: "db-ca"}
	meta := o.secretMetadata(key).DeepCopy()
	meta.ResourceVersion = "2"
	_ = o.secretInformer.GetIndexer().Update(meta)
	o.secretEvent(meta)
	stream, cancel := o.store.Subscribe("test", "db")
	defer cancel()
	if snapshot := <-stream; snapshot.Available || len(snapshot.Connection.ServerCAPEM) != 0 {
		t.Fatal("new subscriber received invalidated CA from an idle snapshot")
	}
	if len(kube.Actions()) != 0 {
		t.Fatal("idle Secret event fetched CA contents")
	}
}

func TestRepeatedCAMissesRespectSecretRetryDelay(t *testing.T) {
	o, kube, _ := fakeObserver(t)
	key := types.NamespacedName{Namespace: "test", Name: "db-ca"}
	delete(o.cas, key)
	o.caQueue.ShutDown()
	o.caQueue = workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[types.NamespacedName](time.Hour, time.Hour))
	_, cancel := o.store.Subscribe("test", "db")
	defer cancel()
	kube.PrependReactor("get", "secrets", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("forbidden") })
	cluster, _ := o.cluster(clusterKey{"test", "db"})
	_, _ = o.cachedConnection(context.Background(), cluster)
	o.workCA(context.Background())
	for range 100 {
		_, _ = o.cachedConnection(context.Background(), cluster)
	}
	if o.caQueue.Len() != 0 || o.caQueue.NumRequeues(key) != 1 {
		t.Fatal("cache misses bypassed the Secret retry delay")
	}
}

func TestConnectionFailuresDoNotPollUnchangedCA(t *testing.T) {
	for _, failure := range []error{errors.New("connection refused"), &tls.CertificateVerificationError{Err: x509.UnknownAuthorityError{}}} {
		o, kube, _ := fakeObserver(t)
		o.probe = func(context.Context, *corev1.Pod, v1.ConnectionParameters, string) (instanceStatus, error) {
			return instanceStatus{}, failure
		}
		for range 3 {
			observe(t, o)
		}
		if len(kube.Actions()) != 0 {
			t.Fatal("failed probe caused CA polling")
		}
	}
}
