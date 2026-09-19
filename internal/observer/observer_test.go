package observer

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic/fake"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	kubefake "k8s.io/client-go/kubernetes/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
)

func fakeObserver(t *testing.T) (*Observer, *kubefake.Clientset, *fake.FakeDynamicClient) {
	t.Helper()
	// The fake tracker does not emit the initial-events-end watch bookmark.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	cluster, pods, results := fixture()
	unstructured.RemoveNestedField(cluster.Object, "spec", "plugins")
	_ = unstructured.SetNestedField(cluster.Object, "db-ca", "status", "certificates", "serverCASecret")
	_ = unstructured.SetNestedField(cluster.Object, "app", "spec", "bootstrap", "initdb", "database")
	objects := make([]runtime.Object, 0, len(pods)+1)
	for i := range pods {
		objects = append(objects, &pods[i])
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-ca", Namespace: "test", UID: "ca-uid", ResourceVersion: "1"}, Data: map[string][]byte{"ca.crt": testPublicCA(t)}}
	objects = append(objects, secret)
	kube := kubefake.NewSimpleClientset(objects...)
	metaScheme := runtime.NewScheme()
	metaScheme.AddKnownTypeWithName(corev1.SchemeGroupVersion.WithKind("Secret"), &metav1.PartialObjectMetadata{})
	meta := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: secret.ObjectMeta}
	metaClient := metadatafake.NewSimpleMetadataClient(metaScheme, meta)
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{clusterResource: "ClusterList"}, cluster)
	o, err := New(kube, dyn, metaClient, discovery.NewStore(), Options{PollInterval: time.Second, TTL: 10 * time.Second, ProbeTimeout: time.Second, MaxConcurrency: 2}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	_ = o.clusterInformer.GetIndexer().Add(cluster.DeepCopy())
	installTestCA(t, o, secret)
	for i := range pods {
		_ = o.podInformer.GetIndexer().Add(pods[i].DeepCopy())
	}
	o.clusterEvent(nil, cluster)
	o.ready.Store(true)
	o.clusterWatch.Store(true)
	o.podWatch.Store(true)
	o.secretWatch.Store(true)
	o.probe = func(_ context.Context, pod *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
		for i := range pods {
			if pods[i].Name == pod.Name {
				return results[i].status, nil
			}
		}
		return instanceStatus{}, errors.New("not found")
	}
	t.Cleanup(func() { o.queue.ShutDown(); o.caQueue.ShutDown(); o.closeStatusClients() })
	return o, kube, dyn
}
func observe(t *testing.T, o *Observer) {
	t.Helper()
	o.observe(context.Background(), clusterKey{"test", "db"})
}
func waitFor(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestWatchInventoryDoesNotProbeUntilSubscribed(t *testing.T) {
	o, kube, dyn := fakeObserver(t)
	ctx := context.Background()
	var calls atomic.Int64
	probe := o.probe
	o.probe = func(ctx context.Context, p *corev1.Pod, c v1.ConnectionParameters, s string) (instanceStatus, error) {
		calls.Add(1)
		return probe(ctx, p, c, s)
	}
	o.opts.PollInterval = 100 * time.Millisecond
	o.ready.Store(false)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- o.Run(runCtx) }()
	t.Cleanup(func() { cancel(); <-done })
	waitFor(t, o.Ready)
	for i := range 200 {
		c, _, _ := fixture()
		c.SetName(fmt.Sprintf("idle-%d", i))
		c.SetUID(types.UID(fmt.Sprintf("idle-%d", i)))
		if _, err := dyn.Resource(clusterResource).Namespace("test").Create(ctx, c, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if i%10 == 9 {
			name := c.GetName()
			waitFor(t, func() bool { _, ok := o.store.Get("test", name); return ok })
		}
	}
	time.Sleep(250 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("idle inventory was probed")
	}
	_, unsubscribe := o.store.Subscribe("test", "db")
	waitFor(t, func() bool { s, _ := o.store.Get("test", "db"); return s.Available })
	before := calls.Load()
	// An unrelated cluster event must not schedule a refresh of db.
	idle, err := dyn.Resource(clusterResource).Namespace("test").Get(ctx, "idle-0", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(idle.Object, "idle-0-2", "status", "targetPrimary")
	if _, err = dyn.Resource(clusterResource).Namespace("test").Update(ctx, idle, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return calls.Load() > before }) // demanded fallback remains alive
	unsubscribe()
	time.Sleep(150 * time.Millisecond)
	stopped := calls.Load()
	time.Sleep(250 * time.Millisecond)
	if calls.Load() != stopped {
		t.Fatal("status probes continued after last subscriber left")
	}
	for _, a := range kube.Actions() {
		if a.GetVerb() == "get" && a.GetResource().Resource != "secrets" {
			t.Fatalf("unexpected per-observation API read: %v", a)
		}
		if a.GetSubresource() == "proxy" {
			t.Fatal("instance status passed through API proxy")
		}
	}
	var lists int
	for _, a := range dyn.Actions() {
		if a.GetVerb() == "list" {
			lists++
		}
	}
	if lists > 1 {
		t.Fatalf("global cluster lists=%d, want only informer initialization", lists)
	}
}

func TestObservationUsesCachedMetadataAndCA(t *testing.T) {
	o, kube, dyn := fakeObserver(t)
	kube.ClearActions()
	dyn.ClearActions()
	observe(t, o)
	observe(t, o)
	s, _ := o.store.Get("test", "db")
	if !s.Available || s.PrimaryID != "db-1-uid" {
		t.Fatalf("bad snapshot %+v", s)
	}
	if len(dyn.Actions()) != 0 {
		t.Fatal("observation reread cluster metadata from API")
	}
	actions := kube.Actions()
	if len(actions) != 0 {
		t.Fatalf("warm observation read the Kubernetes API: %+v", actions)
	}
}
func TestClusterDeleteAndOptOut(t *testing.T) {
	for _, mode := range []string{"delete", "annotation", "plugin"} {
		t.Run(mode, func(t *testing.T) {
			o, _, _ := fakeObserver(t)
			observe(t, o)
			old, _ := o.cluster(clusterKey{"test", "db"})
			next := old.DeepCopy()
			switch mode {
			case "delete":
				_ = o.clusterInformer.GetIndexer().Delete(next)
				o.clusterDeleted(next)
			case "annotation":
				next.SetAnnotations(map[string]string{config.EnabledAnnotation: "false"})
				_ = o.clusterInformer.GetIndexer().Update(next)
				o.clusterEvent(old, next)
			case "plugin":
				_ = unstructured.SetNestedSlice(next.Object, []any{map[string]any{"name": config.PluginName, "enabled": false}}, "spec", "plugins")
				_ = o.clusterInformer.GetIndexer().Update(next)
				o.clusterEvent(old, next)
			}
			if _, exists := o.store.Get("test", "db"); exists {
				t.Fatal("removed/disabled cluster remains discoverable")
			}
		})
	}
}
func TestDisconnectedWatchDoesNotRenewCachedTopology(t *testing.T) {
	o, _, _ := fakeObserver(t)
	observe(t, o)
	before, _ := o.store.Get("test", "db")
	o.clusterWatch.Store(false)
	observe(t, o)
	after, _ := o.store.Get("test", "db")
	if !before.ValidUntil.Equal(after.ValidUntil) {
		t.Fatal("disconnected metadata watch renewed routing")
	}
	o.store.Expire(before.ValidUntil.Add(time.Second))
	after, _ = o.store.Get("test", "db")
	if after.Available {
		t.Fatal("stale topology remains available")
	}
}
func TestTrackedWatchDisconnectAndReconnect(t *testing.T) {
	o, _, _ := fakeObserver(t)
	var healthy atomic.Bool
	for _, reason := range []string{"disconnect", "stop", "cancel with pending event"} {
		t.Run(reason, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := watch.NewRaceFreeFake()
			tracked := o.trackWatch(ctx, source, &healthy, cancel)
			if !healthy.Load() {
				t.Fatal("watch did not connect or recover")
			}
			switch reason {
			case "disconnect":
				source.Stop()
			case "stop":
				tracked.Stop()
			case "cancel with pending event":
				source.Add(&corev1.Pod{})
				cancel()
			}
			for range tracked.ResultChan() {
			}
			if healthy.Load() || !source.IsStopped() || ctx.Err() == nil {
				t.Fatal("closed watch retained health, source, or its request context")
			}
		})
	}
}
func TestChangedPrimaryDuringProbeIsNotPublished(t *testing.T) {
	o, _, _ := fakeObserver(t)
	probe := o.probe
	o.probe = func(ctx context.Context, pod *corev1.Pod, c v1.ConnectionParameters, s string) (instanceStatus, error) {
		if pod.Name == "db-1" {
			next, _ := o.cluster(clusterKey{"test", "db"})
			_ = unstructured.SetNestedField(next.Object, "db-2", "status", "targetPrimary")
			_ = o.clusterInformer.GetIndexer().Update(next)
		}
		return probe(ctx, pod, c, s)
	}
	observe(t, o)
	s, _ := o.store.Get("test", "db")
	if s.Available || s.Reason != "topology_changed_during_observation" {
		t.Fatalf("stale primary published: %+v", s)
	}
}
func TestChangedReplicaDoesNotWithdrawVerifiedPrimary(t *testing.T) {
	for _, change := range []string{"labels", "restart", "replace", "readiness", "delete"} {
		t.Run(change, func(t *testing.T) {
			o, _, _ := fakeObserver(t)
			observe(t, o)
			probe := o.probe
			o.probe = func(ctx context.Context, pod *corev1.Pod, c v1.ConnectionParameters, s string) (instanceStatus, error) {
				if pod.Name == "db-2" {
					next := pod.DeepCopy()
					switch change {
					case "labels":
						next.Labels["cnpg.io/instanceRole"] = "replica"
					case "restart":
						next.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "postgres", ContainerID: "new-container"}}
					case "replace":
						next.UID = "new-uid"
					case "readiness":
						next.Status.Conditions[0].Status = corev1.ConditionFalse
					case "delete":
						_ = o.podInformer.GetIndexer().Delete(next)
					}
					if change != "delete" {
						_ = o.podInformer.GetIndexer().Update(next)
					}
				}
				return probe(ctx, pod, c, s)
			}
			observe(t, o)
			s, _ := o.store.Get("test", "db")
			if !s.Available || s.PrimaryID != "db-1-uid" {
				t.Fatalf("replica change withdrew primary: %+v", s)
			}
			for _, m := range s.Members {
				if m.Name == "db-2" && change != "labels" && m.Ready {
					t.Fatalf("changed replica still eligible: %+v", m)
				}
			}
		})
	}
}
func TestChangedPrimaryPodIsRejected(t *testing.T) {
	o, _, _ := fakeObserver(t)
	probe := o.probe
	o.probe = func(ctx context.Context, pod *corev1.Pod, c v1.ConnectionParameters, s string) (instanceStatus, error) {
		if pod.Name == "db-1" {
			next := pod.DeepCopy()
			next.UID = "new-primary-uid"
			_ = o.podInformer.GetIndexer().Update(next)
		}
		return probe(ctx, pod, c, s)
	}
	observe(t, o)
	s, _ := o.store.Get("test", "db")
	if s.Available {
		t.Fatal("replaced primary accepted stale status")
	}
}
func TestMetadataLabelsDoNotEnqueueOrInvalidate(t *testing.T) {
	o, _, _ := fakeObserver(t)
	observe(t, o)
	_, cancel := o.store.Subscribe("test", "db")
	defer cancel()
	key, _ := o.queue.Get()
	o.queue.Done(key)
	before, _ := o.store.Get("test", "db")
	c, _ := o.cluster(clusterKey{"test", "db"})
	pods := o.pods(c)
	changed := pods[0].DeepCopy()
	changed.Labels["cnpg.io/instanceRole"] = "primary"
	o.podEvent(&pods[0], changed)
	after, _ := o.store.Get("test", "db")
	if after.Revision != before.Revision || o.queue.Len() != 0 {
		t.Fatal("bookkeeping label caused routing work")
	}
}
func TestObserverShutdown(t *testing.T) {
	o, _, _ := fakeObserver(t)
	o.ready.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	waitFor(t, o.Ready)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("observer leaked on shutdown")
	}
	if o.Ready() {
		t.Fatal("ready after shutdown")
	}
}
func TestDirectStatusHTTPValidatesPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/pg/status" {
			t.Errorf("path %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"isPrimary":true,"systemID":"123","timeLineID":1}`)
	}))
	defer server.Close()
	got, err := readStatusHTTP(context.Background(), server.Client(), server.URL+"/pg/status")
	if err != nil || got.IsPrimary == nil || !*got.IsPrimary {
		t.Fatalf("%+v %v", got, err)
	}
}
func TestInvalidOptions(t *testing.T) {
	o, _, _ := fakeObserver(t)
	_, err := New(o.kube, o.dynamic, o.metadata, o.store, Options{PollInterval: time.Second, TTL: time.Second, ProbeTimeout: time.Second, MaxConcurrency: 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "ttl") {
		t.Fatal("accepted unsafe TTL")
	}
}

func TestConcurrentClusterLimitOptions(t *testing.T) {
	base, _, _ := fakeObserver(t)
	for _, test := range []struct {
		name    string
		limit   int
		want    int
		wantErr bool
	}{
		{name: "default", limit: 0, want: 32},
		{name: "one cluster", limit: 1, want: 1},
		{name: "upper bound", limit: 1024, want: 1024},
		{name: "negative", limit: -1, wantErr: true},
		{name: "above bound", limit: 1025, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := base.opts
			options.MaxConcurrentClusters = test.limit
			observer, err := New(base.kube, base.dynamic, base.metadata, discovery.NewStore(), options, nil)
			if observer != nil {
				t.Cleanup(observer.queue.ShutDown)
				t.Cleanup(observer.caQueue.ShutDown)
			}
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "max concurrent clusters") {
					t.Fatalf("expected cluster concurrency validation error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if observer.opts.MaxConcurrentClusters != test.want {
				t.Fatalf("cluster limit=%d, want %d", observer.opts.MaxConcurrentClusters, test.want)
			}
			if cap(observer.probeSlots) != options.MaxConcurrency {
				t.Fatal("cluster concurrency changed the independent instance probe limit")
			}
		})
	}
}

func TestDirectStatusHTTPSVerifiesCAAndName(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"isPrimary":true,"systemID":"123","timeLineID":1}`)
	}))
	defer server.Close()
	o, _, _ := fakeObserver(t)
	cert := server.Certificate()
	name := "127.0.0.1"
	if len(cert.DNSNames) > 0 {
		name = cert.DNSNames[0]
	}
	public := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	for _, tc := range []struct {
		name       string
		ca         []byte
		serverName string
		wantErr    bool
	}{
		{"trusted", public, name, false}, {"wrong CA", testPublicCA(t), name, true}, {"wrong identity", public, "not-the-database.invalid", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := o.statusClient(v1.ConnectionParameters{ServerCAPEM: tc.ca}, tc.serverName)
			if err != nil {
				t.Fatal(err)
			}
			_, err = readStatusHTTP(context.Background(), client, server.URL+"/pg/status")
			if (err != nil) != tc.wantErr {
				t.Fatalf("verification error=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestSameNameRecreationDuringProbeUsesNewIdentity(t *testing.T) {
	o, _, _ := fakeObserver(t)
	probe := o.probe
	o.probe = func(ctx context.Context, p *corev1.Pod, c v1.ConnectionParameters, s string) (instanceStatus, error) {
		if p.Name == "db-1" {
			next, _ := o.cluster(clusterKey{"test", "db"})
			next.SetUID("replacement-cluster")
			_ = o.clusterInformer.GetIndexer().Update(next)
		}
		return probe(ctx, p, c, s)
	}
	observe(t, o)
	snapshot, _ := o.store.Get("test", "db")
	if snapshot.Available || snapshot.Cluster.UID != "replacement-cluster" {
		t.Fatalf("old cluster identity resurrected: %+v", snapshot)
	}
}
