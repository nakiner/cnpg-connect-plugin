package observer

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func clusterRef(cluster *unstructured.Unstructured) v1.ClusterRef {
	return v1.ClusterRef{Namespace: cluster.GetNamespace(), Name: cluster.GetName(), UID: string(cluster.GetUID())}
}

func primaryNames(cluster *unstructured.Unstructured) (string, string) {
	current, _, _ := unstructured.NestedString(cluster.Object, "status", "currentPrimary")
	target, _, _ := unstructured.NestedString(cluster.Object, "status", "targetPrimary")
	return current, target
}

func parameters(cluster *unstructured.Unstructured) (bool, config.Parameters, error) {
	if cluster.GetAnnotations()[config.EnabledAnnotation] == "false" {
		return false, config.Parameters{}, nil
	}
	plugins, _, err := unstructured.NestedSlice(cluster.Object, "spec", "plugins")
	if err != nil {
		return false, config.Parameters{}, err
	}
	for _, value := range plugins {
		plugin, ok := value.(map[string]any)
		if !ok || plugin["name"] != config.PluginName {
			continue
		}
		if enabled, found, err := unstructured.NestedBool(plugin, "enabled"); err != nil {
			return true, config.Parameters{}, err
		} else if found && !enabled {
			return false, config.Parameters{}, nil
		}
		values, _, err := unstructured.NestedStringMap(plugin, "parameters")
		if err != nil {
			return true, config.Parameters{}, err
		}
		parsed, err := config.ParseParameters(values)
		return true, parsed, err
	}
	// Every Cluster in the watch scope is observed by default. Parameters can
	// be supplied without adding this observer to CNPG's synchronous plugin path.
	values := map[string]string{}
	if raw := cluster.GetAnnotations()[config.ParametersAnnotation]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &values); err != nil {
			return true, config.Parameters{}, fmt.Errorf("invalid %s: %w", config.ParametersAnnotation, err)
		}
	}
	parsed, err := config.ParseParameters(values)
	return true, parsed, err
}

func podReady(pod corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func ownedPods(cluster *unstructured.Unstructured, list *corev1.PodList) []corev1.Pod {
	var result []corev1.Pod
	for _, pod := range list.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.Kind == "Cluster" && owner.UID == cluster.GetUID() && owner.APIVersion == "postgresql.cnpg.io/v1" {
				result = append(result, pod)
				break
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func unavailable(cluster *unstructured.Unstructured, at time.Time, ttl time.Duration, reason string) v1.Snapshot {
	return v1.Snapshot{APIVersion: v1.APIVersion, Cluster: clusterRef(cluster), ObservedAt: at, ValidUntil: at.Add(ttl), Reason: reason, Members: []v1.Member{}}
}

func buildSnapshot(cluster *unstructured.Unstructured, pods []corev1.Pod, results []statusResult, params config.Parameters, at time.Time, ttl time.Duration) v1.Snapshot {
	snapshot := unavailable(cluster, at, ttl, "primary_unverified")
	current, target := primaryNames(cluster)
	snapshot.Transitioning = current != target || current == ""
	var fenced []string
	if raw, ok := cluster.GetAnnotations()["cnpg.io/fencedInstances"]; ok {
		if err := json.Unmarshal([]byte(raw), &fenced); err != nil {
			snapshot.Reason = "invalid_fencing_configuration"
			return snapshot
		}
	}
	serverName := params.ServerName
	if serverName == "" {
		serverName = fmt.Sprintf("%s-rw.%s.svc", cluster.GetName(), cluster.GetNamespace())
	}
	primaryIndex, primaryCount := -1, 0
	for i, pod := range pods {
		m := v1.Member{ID: string(pod.UID), Name: pod.Name, Role: "unknown", SyncState: "unknown", Node: pod.Spec.NodeName, Zone: pod.Labels["topology.kubernetes.io/zone"], Region: pod.Labels["topology.kubernetes.io/region"], Endpoints: map[string]v1.Endpoint{}, Reason: "status_unavailable"}
		if pod.Status.PodIP != "" {
			m.Endpoints["internal"] = v1.Endpoint{Host: pod.Status.PodIP, Port: 5432, ServerName: serverName}
		}
		if endpoint, ok := params.ExternalEndpoints[pod.Name]; ok {
			if endpoint.ServerName == "" {
				endpoint.ServerName = serverName
			}
			m.Endpoints["external"] = endpoint
		}
		r := results[i]
		switch {
		case slices.Contains(fenced, "*") || slices.Contains(fenced, pod.Name):
			m.Reason = "fenced"
		case !podReady(pod):
			m.Reason = "pod_not_ready"
		case r.err != nil || r.status.IsPrimary == nil:
			m.Reason = "status_unavailable"
		case r.status.Unavailable || r.status.Rewinding:
			m.Reason = "instance_unavailable"
		default:
			m.Timeline = r.status.Timeline
			m.ReplayLSN = r.status.ReplayLSN
			if *r.status.IsPrimary {
				m.Role = "primary"
				m.Reason = "primary_unverified"
				primaryCount++
				if pod.Name == current && !snapshot.Transitioning {
					primaryIndex = i
				}
			} else {
				m.Role = "standby"
				m.Reason = "replication_unverified"
				if r.status.ReplayPaused {
					m.Reason = "replay_paused"
				} else if !r.status.WalReceiverActive {
					m.Reason = "wal_receiver_inactive"
				}
			}
		}
		snapshot.Members = append(snapshot.Members, m)
	}
	if primaryCount > 1 {
		snapshot.Reason = "multiple_primaries_observed"
		return snapshot
	}
	if snapshot.Transitioning {
		snapshot.Reason = "primary_transition"
		return snapshot
	}
	if primaryIndex < 0 {
		return snapshot
	}
	primary := results[primaryIndex].status
	snapshot.PrimaryID = snapshot.Members[primaryIndex].ID
	snapshot.Members[primaryIndex].Ready = true
	snapshot.Members[primaryIndex].Reason = ""
	snapshot.Available = true
	snapshot.Reason = ""
	// Duplicate application names cannot safely identify one replica. Treat them
	// as unknown instead of arbitrarily selecting one sender's sync state.
	replication := map[string]replicationStatus{}
	duplicates := map[string]bool{}
	for _, r := range primary.Replication {
		if _, ok := replication[r.ApplicationName]; ok {
			duplicates[r.ApplicationName] = true
		}
		replication[r.ApplicationName] = r
	}
	for i := range snapshot.Members {
		m := &snapshot.Members[i]
		r := results[i].status
		if m.Role != "standby" || m.Reason != "replication_unverified" {
			continue
		}
		// CNPG reports the checkpoint timeline. A healthy standby can retain
		// the previous value after promotion until a later checkpoint is replayed.
		// Establish replication through the current primary's sender state below.
		if r.SystemID != primary.SystemID {
			m.Reason = "database_identity_mismatch"
			continue
		}
		state, found := replication[m.Name]
		if !found || duplicates[m.Name] || state.State != "streaming" {
			continue
		}
		if !slices.Contains([]string{"sync", "quorum", "potential", "async"}, state.SyncState) {
			continue
		}
		m.SyncState = state.SyncState
		m.Ready = true
		m.Reason = ""
	}
	return snapshot
}
