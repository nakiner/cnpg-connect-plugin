package observer

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestCacheProjectionPreservesRoutingAndDropsLargeFields(t *testing.T) {
	cluster, pods, results := fixture()
	large := strings.Repeat("irrelevant", 1<<15)
	cluster.SetResourceVersion("cluster-rv")
	cluster.SetAnnotations(map[string]string{
		config.EnabledAnnotation:                           "true",
		config.ParametersAnnotation:                        `{"serverName":"annotation.example"}`,
		"cnpg.io/fencedInstances":                          `[]`,
		"kubectl.kubernetes.io/last-applied-configuration": large,
	})
	cluster.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "operator", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{}}`)}}})
	_ = unstructured.SetNestedSlice(cluster.Object, []any{
		map[string]any{"name": "backup.example", "parameters": map[string]any{"payload": large}},
		map[string]any{"name": config.PluginName, "enabled": true, "parameters": map[string]any{"serverName": "db.example", "externalEndpoints": `{"db-1":{"host":"db1.example","port":5432}}`}},
	}, "spec", "plugins")
	_ = unstructured.SetNestedMap(cluster.Object, map[string]any{"initdb": map[string]any{"database": "app", "postInitSQL": []any{large}}}, "spec", "bootstrap")
	_ = unstructured.SetNestedField(cluster.Object, large, "spec", "backup", "irrelevant")
	certificates := map[string]any{"serverCASecret": "db-ca", "serverTLSSecret": "db-server", "expirations": map[string]any{"db-ca": "2030-01-01T00:00:00Z"}}
	_ = unstructured.SetNestedMap(cluster.Object, certificates, "status", "certificates")
	_ = unstructured.SetNestedField(cluster.Object, large, "status", "irrelevant")
	for i := range pods {
		pods[i].ResourceVersion = "pod-rv"
		pods[i].Spec.NodeName = "node-1"
		pods[i].Labels["topology.kubernetes.io/zone"] = "zone-a"
		pods[i].Labels["topology.kubernetes.io/region"] = "region-a"
		pods[i].Annotations = map[string]string{"large": large}
		pods[i].Spec.Containers = []corev1.Container{
			{Name: "postgres", Command: []string{"/controller/manager", "instance", "run"}, Args: []string{"--status-port-tls"}, Env: []corev1.EnvVar{{Name: "LARGE", Value: large}}},
			{Name: "backup-sidecar", Env: []corev1.EnvVar{{Name: "LARGE", Value: large}}},
		}
		pods[i].Spec.Volumes = []corev1.Volume{{Name: "large", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: large}}}}
		pods[i].Status.Conditions = append(pods[i].Status.Conditions, corev1.PodCondition{Type: corev1.PodScheduled, Message: large})
		pods[i].Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "postgres", ContainerID: "container-id", Image: large}, {Name: "backup-sidecar", Image: large}}
	}
	beforeCluster := marshalCacheTest(t, cluster)
	beforePods := marshalCacheTest(t, pods)
	projected, err := transformClusterCache(cluster)
	if err != nil {
		t.Fatal(err)
	}
	cachedCluster := projected.(*unstructured.Unstructured)
	cachedPods := make([]corev1.Pod, len(pods))
	for i := range pods {
		projected, err := transformPodCache(&pods[i])
		if err != nil {
			t.Fatal(err)
		}
		cachedPods[i] = *projected.(*corev1.Pod)
		if !samePodRoute(pods[i], cachedPods[i]) || statusScheme(&cachedPods[i]) != "https" || cachedPods[i].ResourceVersion != "pod-rv" {
			t.Fatal("projection changed Pod identity, TLS, or eligibility")
		}
	}
	if !sameClusterRoute(cluster, cachedCluster) || cachedCluster.GetResourceVersion() != "cluster-rv" || serverCASecret(cachedCluster) != "db-ca" || applicationDatabase(cachedCluster) != "app" {
		t.Fatal("projection changed Cluster routing, certificate, or database fields")
	}
	_, params, err := parameters(cluster)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	before := buildSnapshot(cluster, pods, results, params, at, time.Minute)
	after := buildSnapshot(cachedCluster, cachedPods, results, params, at, time.Minute)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("cache projection changed the published snapshot: before=%+v after=%+v", before, after)
	}
	if got := ownedPods(cachedCluster, &corev1.PodList{Items: cachedPods}); len(got) != len(pods) {
		t.Fatal("projection lost Pod ownership")
	}
	if !reflect.DeepEqual(beforeCluster, marshalCacheTest(t, cluster)) || !reflect.DeepEqual(beforePods, marshalCacheTest(t, pods)) {
		t.Fatal("cache transform mutated its input")
	}
	if len(marshalCacheTest(t, cachedCluster))*20 >= len(beforeCluster) || len(marshalCacheTest(t, cachedPods))*20 >= len(beforePods) {
		t.Fatal("large irrelevant fields remained in the informer cache")
	}
	again, err := transformClusterCache(cachedCluster)
	if err != nil || !reflect.DeepEqual(cachedCluster, again) {
		t.Fatal("Cluster projection is not idempotent")
	}
	again, err = transformPodCache(&cachedPods[0])
	if err != nil || !reflect.DeepEqual(&cachedPods[0], again) {
		t.Fatal("Pod projection is not idempotent")
	}
	// No retained mutable map or pointer may refer back into the source object.
	_ = unstructured.SetNestedField(cachedCluster.Object, "changed", "status", "certificates", "serverCASecret")
	cachedPods[0].Labels["topology.kubernetes.io/zone"] = "changed"
	cachedPods[0].OwnerReferences[0].Name = "changed"
	if serverCASecret(cluster) != "db-ca" || pods[0].Labels["topology.kubernetes.io/zone"] != "zone-a" || pods[0].OwnerReferences[0].Name != "db" {
		t.Fatal("projection retained shared mutable source fields")
	}
}

func TestClusterProjectionPreservesEnrollmentErrors(t *testing.T) {
	for name, change := range map[string]func(*unstructured.Unstructured){
		"annotation disable": func(c *unstructured.Unstructured) {
			c.SetAnnotations(map[string]string{config.EnabledAnnotation: "false"})
		},
		"annotation invalid": func(c *unstructured.Unstructured) {
			unstructured.RemoveNestedField(c.Object, "spec", "plugins")
			c.SetAnnotations(map[string]string{config.ParametersAnnotation: "not json"})
		},
		"native disable": func(c *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(c.Object, []any{map[string]any{"name": config.PluginName, "enabled": false}}, "spec", "plugins")
		},
		"native malformed plugins": func(c *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(c.Object, "invalid", "spec", "plugins")
		},
		"native malformed parameters": func(c *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(c.Object, []any{map[string]any{"name": config.PluginName, "parameters": "invalid"}}, "spec", "plugins")
		},
		"native malformed enabled": func(c *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(c.Object, []any{map[string]any{"name": config.PluginName, "enabled": "invalid"}}, "spec", "plugins")
		},
		"unknown native parameter": func(c *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(c.Object, []any{map[string]any{"name": config.PluginName, "parameters": map[string]any{"unknown": "invalid"}}}, "spec", "plugins")
		},
	} {
		t.Run(name, func(t *testing.T) {
			cluster, _, _ := fixture()
			change(cluster)
			projected, err := transformClusterCache(cluster)
			if err != nil {
				t.Fatal(err)
			}
			beforeEnabled, beforeParams, beforeErr := parameters(cluster)
			afterEnabled, afterParams, afterErr := parameters(projected.(*unstructured.Unstructured))
			if beforeEnabled != afterEnabled || !reflect.DeepEqual(beforeParams, afterParams) || (beforeErr == nil) != (afterErr == nil) {
				t.Fatalf("projection changed enrollment behavior: before=(%v,%v,%v), after=(%v,%v,%v)", beforeEnabled, beforeParams, beforeErr, afterEnabled, afterParams, afterErr)
			}
		})
	}
}

func TestCacheProjectionPreservesDeletionAndFencing(t *testing.T) {
	cluster, pods, results := fixture()
	now := metav1.Now()
	cluster.SetDeletionTimestamp(&now)
	cluster.SetAnnotations(map[string]string{"cnpg.io/fencedInstances": `["db-1"]`})
	pods[0].DeletionTimestamp = &now
	projectedCluster, err := transformClusterCache(cluster)
	if err != nil {
		t.Fatal(err)
	}
	projectedPod, err := transformPodCache(&pods[0])
	if err != nil {
		t.Fatal(err)
	}
	c := projectedCluster.(*unstructured.Unstructured)
	p := projectedPod.(*corev1.Pod)
	if !c.GetDeletionTimestamp().Equal(cluster.GetDeletionTimestamp()) || !p.DeletionTimestamp.Equal(&now) || podReady(*p) {
		t.Fatal("projection changed deletion eligibility")
	}
	if got := buildSnapshot(c, pods, results, config.Parameters{}, now.Time, time.Minute); got.Available || got.Members[0].Reason != "fenced" {
		t.Fatal("projection lost fencing")
	}
}

func marshalCacheTest(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
