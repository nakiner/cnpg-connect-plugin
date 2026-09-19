package observer

import (
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// All fleet scenarios use the same three-instance metadata and replication
// view. Each scenario controls publication, CA caching, and probe latency.
func addFleetCluster(t *testing.T, o *Observer, name, secret string) (*unstructured.Unstructured, []corev1.Pod, []statusResult) {
	t.Helper()
	cluster, _ := o.cluster(clusterKey{"test", "db"})
	cluster.SetName(name)
	cluster.SetUID(types.UID(name))
	_ = unstructured.SetNestedField(cluster.Object, name+"-1", "status", "currentPrimary")
	_ = unstructured.SetNestedField(cluster.Object, name+"-1", "status", "targetPrimary")
	_ = unstructured.SetNestedField(cluster.Object, secret, "status", "certificates", "serverCASecret")
	if err := o.clusterInformer.GetIndexer().Add(cluster); err != nil {
		t.Fatal(err)
	}
	_, pods, results := fixture()
	for i := range pods {
		pod := &pods[i]
		pod.Name = fmt.Sprintf("%s-%d", name, i+1)
		pod.UID = types.UID(pod.Name)
		pod.Labels["cnpg.io/cluster"] = name
		pod.OwnerReferences[0].Name = name
		pod.OwnerReferences[0].UID = cluster.GetUID()
		if err := o.podInformer.GetIndexer().Add(pod); err != nil {
			t.Fatal(err)
		}
	}
	for i := range results[0].status.Replication {
		results[0].status.Replication[i].ApplicationName = pods[i+1].Name
	}
	return cluster, pods, results
}

func fleetStatus(pod *corev1.Pod, primarySuffix, systemID string) instanceStatus {
	primary := strings.HasSuffix(pod.Name, primarySuffix)
	status := instanceStatus{IsPrimary: &primary, SystemID: systemID, Timeline: 1, WalReceiverActive: !primary}
	if primary {
		for _, suffix := range []string{"-1", "-2", "-3"} {
			if suffix != primarySuffix {
				status.Replication = append(status.Replication, replicationStatus{
					ApplicationName: pod.OwnerReferences[0].Name + suffix, State: "streaming", SyncState: "sync",
				})
			}
		}
	}
	return status
}
