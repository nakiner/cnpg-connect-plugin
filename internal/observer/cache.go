package observer

import (
	"fmt"
	"slices"

	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// transformClusterCache retains only metadata consumed by topology discovery.
// In particular, backup configuration, managedFields, and unrelated plugin
// parameters must not occupy the cache for every database in the watch scope.
// It returns an independent, idempotent projection rather than mutating input.
func transformClusterCache(obj any) (any, error) {
	cluster, ok := obj.(*unstructured.Unstructured)
	if !ok || cluster == nil {
		return nil, fmt.Errorf("cluster cache received %T", obj)
	}
	projected := projectJSONFields(cluster.Object, "apiVersion", "kind")
	metadata, _ := cluster.Object["metadata"].(map[string]any)
	// Do not copy metadata wholesale: managedFields and last-applied annotations
	// can be much larger than the complete retained routing view.
	projectedMetadata := projectJSONFields(metadata, "name", "namespace", "uid", "resourceVersion", "generation", "deletionTimestamp")
	annotations, _ := metadata["annotations"].(map[string]any)
	projectedMetadata["annotations"] = projectJSONFields(annotations, config.EnabledAnnotation, config.ParametersAnnotation, "cnpg.io/fencedInstances")
	projected["metadata"] = projectedMetadata

	spec, _ := cluster.Object["spec"].(map[string]any)
	projectedSpec := make(map[string]any)
	if plugins, exists := spec["plugins"]; exists {
		if values, valid := plugins.([]any); valid {
			selected := make([]any, 0, 1)
			for _, value := range values {
				entry, valid := value.(map[string]any)
				if valid && entry["name"] == config.PluginName {
					selected = append(selected, projectJSONFields(entry, "name", "enabled", "parameters"))
					break // parameters() uses the first matching native entry.
				}
			}
			projectedSpec["plugins"] = selected
		} else {
			// Preserve malformed configuration so projection cannot turn a
			// rejected native definition into valid automatic enrollment.
			projectedSpec["plugins"] = runtime.DeepCopyJSONValue(plugins)
		}
	}
	bootstrap, _ := spec["bootstrap"].(map[string]any)
	projectedBootstrap := make(map[string]any)
	for _, method := range []string{"recovery", "pg_basebackup", "initdb"} {
		if entry, ok := bootstrap[method].(map[string]any); ok {
			projectedBootstrap[method] = projectJSONFields(entry, "database")
		}
	}
	projectedSpec["bootstrap"] = projectedBootstrap
	projected["spec"] = projectedSpec
	status, _ := cluster.Object["status"].(map[string]any)
	projected["status"] = projectJSONFields(status, "currentPrimary", "targetPrimary", "certificates")
	return &unstructured.Unstructured{Object: projected}, nil
}

func projectJSONFields(source map[string]any, names ...string) map[string]any {
	result := make(map[string]any, len(names))
	for _, name := range names {
		if value, exists := source[name]; exists {
			result[name] = runtime.DeepCopyJSONValue(value)
		}
	}
	return result
}

// transformPodCache excludes application environment, volumes, image metadata,
// and other Pod fields that cannot influence discovery's routing decisions.
func transformPodCache(obj any) (any, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return nil, fmt.Errorf("pod cache received %T", obj)
	}
	result := &corev1.Pod{
		TypeMeta: pod.TypeMeta,
		ObjectMeta: metav1.ObjectMeta{
			Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID,
			ResourceVersion: pod.ResourceVersion, Generation: pod.Generation,
			DeletionTimestamp: pod.DeletionTimestamp.DeepCopy(),
			Labels:            make(map[string]string),
		},
		Spec:   corev1.PodSpec{NodeName: pod.Spec.NodeName},
		Status: corev1.PodStatus{Phase: pod.Status.Phase, PodIP: pod.Status.PodIP},
	}
	for _, key := range []string{"cnpg.io/cluster", "cnpg.io/instanceName", "cnpg.io/instanceRole", "cnpg.io/podRole", "topology.kubernetes.io/zone", "topology.kubernetes.io/region"} {
		if value, exists := pod.Labels[key]; exists {
			result.Labels[key] = value
		}
	}
	for _, owner := range pod.OwnerReferences {
		result.OwnerReferences = append(result.OwnerReferences, *owner.DeepCopy())
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != "postgres" {
			continue
		}
		projected := corev1.Container{Name: container.Name}
		if slices.Contains(container.Command, "--status-port-tls") {
			projected.Command = []string{"--status-port-tls"}
		}
		if slices.Contains(container.Args, "--status-port-tls") {
			projected.Args = []string{"--status-port-tls"}
		}
		result.Spec.Containers = []corev1.Container{projected}
		break
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			result.Status.Conditions = []corev1.PodCondition{{Type: condition.Type, Status: condition.Status}}
			break
		}
	}
	for _, container := range pod.Status.ContainerStatuses {
		if container.Name == "postgres" {
			result.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: container.Name, ContainerID: container.ContainerID}}
			break
		}
	}
	return result, nil
}
