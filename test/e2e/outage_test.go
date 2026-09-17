//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

const deploymentName = "connect"

// TestLiveAnnotationOutage demonstrates that CNPG can fail over while the
// optional discovery observer is absent. Unlike the lifecycle test, it leaves
// annotation enrollment enabled and removes this plugin's native spec entry.
// Other plugins and all discovery parameters remain intact.
func TestLiveAnnotationOutage(t *testing.T) {
	if os.Getenv("CNPG_CONNECT_E2E_ANNOTATION_OUTAGE") != "1" || os.Getenv("CNPG_CONNECT_E2E_KUBECONFIG") == "" {
		t.Skip("opt in with CNPG_CONNECT_E2E_ANNOTATION_OUTAGE=1 and the isolated e2e kubeconfig")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	h := newHarness(t, ctx)
	cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h.clusterUID = cluster.GetUID()
	if h.clusterUID == "" {
		t.Fatal("test Cluster has no UID")
	}
	deployment, err := h.kubernetes.AppsV1().Deployments(pluginNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if deployment.UID == "" || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 1 {
		t.Fatal("fixed observer Deployment must exist with at least one replica")
	}
	deploymentUID, replicas := deployment.UID, *deployment.Spec.Replicas
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if err := h.scaleObserver(cleanupCtx, deploymentUID, replicas); err != nil {
			t.Errorf("restore observer replica count: %v", err)
			return
		}
		if err := h.waitObserver(cleanupCtx, deploymentUID, replicas); err != nil {
			t.Errorf("observer did not recover during cleanup: %v", err)
		}
	})

	enrolledAt := time.Now()
	if err := h.enableAnnotationEnrollment(ctx); err != nil {
		t.Fatal(err)
	}
	before := h.waitGet(t, ctx, "fresh healthy annotation-enrolled topology", func(snapshot *connectv1.Snapshot) bool {
		return healthyThree(snapshot) && snapshot.ObservedAt != nil && snapshot.ObservedAt.AsTime().After(enrolledAt.Add(2*time.Second))
	})
	t.Logf("annotation enrollment before outage: %s", summarize(before))
	if err := h.assertAnnotationEnrollment(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.scaleObserver(ctx, deploymentUID, 0); err != nil {
		t.Fatal(err)
	}
	stopCtx, stopCancel := context.WithTimeout(ctx, 2*time.Minute)
	err = h.waitObserver(stopCtx, deploymentUID, 0)
	stopCancel()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("discovery Deployment is scaled to zero and all its Pods are absent")

	// Resolve the current primary from Kubernetes after the observer is gone,
	// rather than routing a destructive test action using a stale snapshot.
	cluster, err = h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldPrimary, _, _ := unstructured.NestedString(cluster.Object, "status", "currentPrimary")
	targetPrimary, _, _ := unstructured.NestedString(cluster.Object, "status", "targetPrimary")
	if oldPrimary == "" || oldPrimary != targetPrimary {
		t.Fatal("refusing primary deletion while CNPG is already transitioning")
	}
	pod, err := h.kubernetes.CoreV1().Pods(clusterNamespace).Get(ctx, oldPrimary, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	member := &connectv1.Member{Name: oldPrimary, Id: string(pod.UID)}
	if err := h.verifyPod(ctx, member); err != nil {
		t.Fatal(err)
	}
	if err := h.assertAnnotationEnrollment(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.assertObserverStopped(ctx, deploymentUID); err != nil {
		t.Fatal(err)
	}
	if err := h.guardContext(); err != nil {
		t.Fatal(err)
	}
	uid := pod.UID
	if err := h.kubernetes.CoreV1().Pods(clusterNamespace).Delete(ctx, oldPrimary, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
		t.Fatal(err)
	}
	t.Logf("deleted elected primary %s uid=%s while discovery is absent", oldPrimary, uid)

	failoverCtx, failoverCancel := context.WithTimeout(ctx, 3*time.Minute)
	newPrimary, err := h.waitHealthyKubernetes(failoverCtx, oldPrimary)
	failoverCancel()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.assertObserverStopped(ctx, deploymentUID); err != nil {
		t.Fatal(err)
	}
	t.Logf("CNPG elected %s and recovered all three ready instances while discovery remained at zero replicas", newPrimary)

	if err := h.scaleObserver(ctx, deploymentUID, replicas); err != nil {
		t.Fatal(err)
	}
	recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer recoveryCancel()
	if err := h.waitObserver(recoveryCtx, deploymentUID, replicas); err != nil {
		t.Fatal(err)
	}
	if _, err := h.waitHealthyKubernetes(recoveryCtx, oldPrimary); err != nil {
		t.Fatal(err)
	}
	if err := h.assertAnnotationEnrollment(recoveryCtx); err != nil {
		t.Fatal(err)
	}
	t.Log("observer Deployment is ready again; annotation enrollment remains enabled")
	t.Log("restart any Pod-bound port forward and verify GetTopology/WatchTopology against the recovered observer")
}

func (h *harness) enableAnnotationEnrollment(ctx context.Context) error {
	var preserved map[string]string
	cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	parameters := cluster.GetAnnotations()["connect.cnpg.io/parameters"]
	if parameters != "" {
		if err := json.Unmarshal([]byte(parameters), &preserved); err != nil {
			return fmt.Errorf("existing annotation parameters are not a string map: %w", err)
		}
	}
	if preserved == nil {
		preserved = make(map[string]string)
	}
	plugins, _, err := unstructured.NestedSlice(cluster.Object, "spec", "plugins")
	if err != nil {
		return err
	}
	for _, value := range plugins {
		plugin, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(plugin, "name")
		if name != pluginName {
			continue
		}
		configuration, _, err := unstructured.NestedStringMap(plugin, "parameters")
		if err != nil {
			return err
		}
		for key, value := range configuration {
			preserved[key] = value
		}
	}
	encoded, err := json.Marshal(preserved)
	if err != nil {
		return err
	}
	return h.patchCluster(ctx, "", func(current *unstructured.Unstructured) map[string]interface{} {
		plugins, _, _ := unstructured.NestedSlice(current.Object, "spec", "plugins")
		remaining := make([]interface{}, 0, len(plugins))
		for _, value := range plugins {
			plugin, ok := value.(map[string]interface{})
			if !ok {
				remaining = append(remaining, value)
				continue
			}
			name, _, _ := unstructured.NestedString(plugin, "name")
			if name != pluginName {
				remaining = append(remaining, value)
			}
		}
		return map[string]interface{}{
			"metadata": map[string]interface{}{"resourceVersion": current.GetResourceVersion(), "annotations": map[string]interface{}{
				"connect.cnpg.io/enabled": "true", "connect.cnpg.io/parameters": string(encoded),
			}},
			"spec": map[string]interface{}{"plugins": remaining},
		}
	})
}

func (h *harness) assertAnnotationEnrollment(ctx context.Context) error {
	cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if cluster.GetUID() != h.clusterUID {
		return fmt.Errorf("test Cluster identity changed")
	}
	if cluster.GetAnnotations()["connect.cnpg.io/enabled"] != "true" {
		return fmt.Errorf("annotation enrollment is no longer enabled")
	}
	plugins, _, _ := unstructured.NestedSlice(cluster.Object, "spec", "plugins")
	for _, value := range plugins {
		plugin, ok := value.(map[string]interface{})
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(plugin, "name")
		if name == pluginName {
			return fmt.Errorf("native connect plugin entry remains; outage must not gate CNPG reconciliation")
		}
	}
	return nil
}

func (h *harness) scaleObserver(ctx context.Context, uid types.UID, replicas int32) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := h.guardContext(); err != nil {
			return err
		}
		deployment, err := h.kubernetes.AppsV1().Deployments(pluginNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if deployment.UID != uid {
			return fmt.Errorf("refusing scale mutation: fixed observer Deployment identity changed")
		}
		patch, err := json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{"resourceVersion": deployment.ResourceVersion}, "spec": map[string]interface{}{"replicas": replicas}})
		if err != nil {
			return err
		}
		_, err = h.kubernetes.AppsV1().Deployments(pluginNamespace).Patch(ctx, deploymentName, types.MergePatchType, patch, metav1.PatchOptions{})
		return err
	})
}

func (h *harness) assertObserverStopped(ctx context.Context, uid types.UID) error {
	deployment, err := h.kubernetes.AppsV1().Deployments(pluginNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if deployment.UID != uid || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 {
		return fmt.Errorf("observer is not the expected scaled-to-zero Deployment")
	}
	return h.noObserverPods(ctx, deployment)
}

func (h *harness) noObserverPods(ctx context.Context, deployment *appsv1.Deployment) error {
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return err
	}
	pods, err := h.kubernetes.CoreV1().Pods(pluginNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return err
	}
	if len(pods.Items) != 0 {
		return fmt.Errorf("observer still has %d Pods, including terminating Pods", len(pods.Items))
	}
	return nil
}

func (h *harness) waitObserver(ctx context.Context, uid types.UID, replicas int32) error {
	var last string
	for {
		deployment, err := h.kubernetes.AppsV1().Deployments(pluginNamespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err == nil {
			if deployment.UID != uid {
				return fmt.Errorf("observer Deployment identity changed")
			}
			last = fmt.Sprintf("generation=%d observed=%d ready=%d available=%d", deployment.Generation, deployment.Status.ObservedGeneration, deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas)
			if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == replicas && deployment.Status.ObservedGeneration >= deployment.Generation {
				if replicas == 0 {
					if err := h.noObserverPods(ctx, deployment); err == nil {
						return nil
					} else {
						last = err.Error()
					}
				} else if deployment.Status.ReadyReplicas >= replicas && deployment.Status.AvailableReplicas >= replicas && deployment.Status.UpdatedReplicas >= replicas {
					return nil
				}
			}
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for observer replicas=%d: %w; last=%s", replicas, ctx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

func (h *harness) waitHealthyKubernetes(ctx context.Context, previousPrimary string) (string, error) {
	var last string
	for {
		cluster, err := h.clusters.Get(ctx, clusterName, metav1.GetOptions{})
		if err == nil {
			if cluster.GetUID() != h.clusterUID {
				return "", fmt.Errorf("test Cluster identity changed")
			}
			current, _, _ := unstructured.NestedString(cluster.Object, "status", "currentPrimary")
			target, _, _ := unstructured.NestedString(cluster.Object, "status", "targetPrimary")
			ready, _, _ := unstructured.NestedInt64(cluster.Object, "status", "readyInstances")
			last = fmt.Sprintf("currentPrimary=%s targetPrimary=%s readyInstances=%d", current, target, ready)
			if current != "" && current != previousPrimary && current == target && ready == 3 {
				pods, err := h.kubernetes.CoreV1().Pods(clusterNamespace).List(ctx, metav1.ListOptions{LabelSelector: "cnpg.io/cluster=" + clusterName})
				if err == nil {
					readyPods := 0
					for _, pod := range pods.Items {
						if pod.DeletionTimestamp != nil {
							continue
						}
						for _, condition := range pod.Status.Conditions {
							if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
								readyPods++
								break
							}
						}
					}
					if readyPods == 3 {
						return current, nil
					}
					last += fmt.Sprintf(" actualReadyPods=%d", readyPods)
				}
			}
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("waiting for healthy CNPG failover: %w; last=%s", ctx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}
