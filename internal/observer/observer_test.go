package observer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

func fakeObserver(t *testing.T) (*Observer, *kubefake.Clientset, *fake.FakeDynamicClient) {
	t.Helper()
	cluster, pods, results := fixture()
	unstructured.RemoveNestedField(cluster.Object, "spec", "plugins")
	_ = unstructured.SetNestedField(cluster.Object, "db-ca", "status", "certificates", "serverCASecret")
	_ = unstructured.SetNestedField(cluster.Object, "app", "spec", "bootstrap", "initdb", "database")
	objects := make([]runtime.Object, len(pods))
	for i := range pods {
		objects[i] = &pods[i]
	}
	objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "db-ca", Namespace: "test"}, Data: map[string][]byte{"ca.crt": testPublicCA(t)}})
	kube := kubefake.NewSimpleClientset(objects...)
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{clusterResource: "ClusterList"}, cluster)
	o, err := New(kube, dyn, discovery.NewStore(), Options{PollInterval: time.Second, TTL: 10 * time.Second, ProbeTimeout: time.Second, MaxConcurrency: 2}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	o.probe = func(_ context.Context, _ kubernetes.Interface, pod *corev1.Pod) (instanceStatus, error) {
		for i := range pods {
			if pods[i].Name == pod.Name {
				return results[i].status, nil
			}
		}
		return instanceStatus{}, errors.New("not found")
	}
	return o, kube, dyn
}

func TestCollectAutomaticallyAndDelete(t *testing.T) {
	o, _, dyn := fakeObserver(t)
	if err := o.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := o.store.Get("test", "db")
	if !ok || !snapshot.Available || snapshot.PrimaryID != "db-1-uid" {
		t.Fatalf("bad snapshot %+v", snapshot)
	}
	if err := dyn.Resource(clusterResource).Namespace("test").Delete(context.Background(), "db", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := o.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := o.store.Get("test", "db"); ok {
		t.Fatal("deleted cluster remains discoverable")
	}
}

func TestExplicitOptOutRemovesAutomaticallyObservedCluster(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native=%v", native), func(t *testing.T) {
			o, _, dyn := fakeObserver(t)
			ctx := context.Background()
			if err := o.collect(ctx); err != nil {
				t.Fatal(err)
			}
			cluster, err := dyn.Resource(clusterResource).Namespace("test").Get(ctx, "db", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if native {
				_ = unstructured.SetNestedSlice(cluster.Object, []any{map[string]any{"name": config.PluginName, "enabled": false}}, "spec", "plugins")
			} else {
				cluster.SetAnnotations(map[string]string{config.EnabledAnnotation: "false"})
			}
			if _, err := dyn.Resource(clusterResource).Namespace("test").Update(ctx, cluster, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := o.collect(ctx); err != nil {
				t.Fatal(err)
			}
			if _, exists := o.store.Get("test", "db"); exists {
				t.Fatal("opted-out cluster remains discoverable")
			}
		})
	}
}

func TestAPIOutageDoesNotRenewTopology(t *testing.T) {
	o, _, dyn := fakeObserver(t)
	if err := o.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, _ := o.store.Get("test", "db")
	dyn.PrependReactor("list", "clusters", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("API unavailable") })
	if err := o.collect(context.Background()); err == nil {
		t.Fatal("expected API failure")
	}
	after, _ := o.store.Get("test", "db")
	if !before.ValidUntil.Equal(after.ValidUntil) || before.Revision != after.Revision {
		t.Fatal("outage renewed topology")
	}
	o.store.Expire(before.ValidUntil.Add(time.Second))
	expired, _ := o.store.Get("test", "db")
	if expired.Available || expired.PrimaryID != "" {
		t.Fatal("expired topology remains routable")
	}
}

func TestChangedPrimaryDuringProbeIsNotPublished(t *testing.T) {
	o, _, dyn := fakeObserver(t)
	probe := o.probe
	o.probe = func(ctx context.Context, kube kubernetes.Interface, pod *corev1.Pod) (instanceStatus, error) {
		if pod.Name == "db-1" {
			obj, err := dyn.Resource(clusterResource).Namespace("test").Get(ctx, "db", metav1.GetOptions{})
			if err != nil {
				return instanceStatus{}, err
			}
			_ = unstructured.SetNestedField(obj.Object, "db-2", "status", "targetPrimary")
			if _, err = dyn.Resource(clusterResource).Namespace("test").Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
				return instanceStatus{}, err
			}
		}
		return probe(ctx, kube, pod)
	}
	if err := o.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := o.store.Get("test", "db")
	if got.Available || got.PrimaryID != "" || got.Reason != "topology_changed_during_observation" {
		t.Fatalf("published stale primary %+v", got)
	}
}

func TestReplacedPodDuringProbeIsNotPublished(t *testing.T) {
	o, kube, _ := fakeObserver(t)
	probe := o.probe
	o.probe = func(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod) (instanceStatus, error) {
		if pod.Name == "db-2" {
			replacement := pod.DeepCopy()
			replacement.UID = "replacement-uid"
			if _, err := kube.CoreV1().Pods("test").Update(ctx, replacement, metav1.UpdateOptions{}); err != nil {
				return instanceStatus{}, err
			}
		}
		return probe(ctx, client, pod)
	}
	if err := o.collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := o.store.Get("test", "db")
	if got.Available || got.Reason != "membership_changed_during_observation" {
		t.Fatalf("published replaced member %+v", got)
	}
}

func TestObserverShutdown(t *testing.T) {
	o, _, _ := fakeObserver(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for !o.Ready() {
		select {
		case <-deadline:
			cancel()
			t.Fatal("observer never initialized")
		case <-time.After(10 * time.Millisecond):
		}
	}
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
		t.Fatal("observer ready after shutdown")
	}
}

func TestStatusProxyUsesPodSchemeAndValidatesPayload(t *testing.T) {
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				want := fmt.Sprintf("/api/v1/namespaces/test/pods/%s:db-1:8000/proxy/pg/status", scheme)
				if r.URL.Path != want {
					t.Errorf("path=%s want=%s", r.URL.Path, want)
					http.NotFound(w, r)
					return
				}
				_, _ = io.WriteString(w, `{"isPrimary":true,"systemID":"123","timeLineID":1}`)
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			_, pods, _ := fixture()
			pod := pods[0]
			if scheme == "https" {
				pod.Spec.Containers = []corev1.Container{{Name: "postgres", Command: []string{"/controller/manager", "instance", "run", "--status-port-tls"}}}
			}
			got, err := readStatus(context.Background(), client, &pod)
			if err != nil {
				t.Fatal(err)
			}
			if got.IsPrimary == nil || !*got.IsPrimary {
				t.Fatal("missing primary")
			}
		})
	}
}

func TestInvalidOptions(t *testing.T) {
	o, _, _ := fakeObserver(t)
	_, err := New(o.kube, o.dynamic, o.store, Options{PollInterval: time.Second, TTL: time.Second, ProbeTimeout: time.Second, MaxConcurrency: 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "ttl") {
		t.Fatal("accepted unsafe TTL")
	}
}
