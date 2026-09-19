package observer

import (
	"encoding/json"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// Negative safety evidence is independent of routing eligibility. Unreadiness,
// a container restart, a missing Pod, or a failed probe is not database fencing.
// Only an actual conflict is retained: a primary claim inconsistent with the
// Cluster at collection start. A formerly healthy primary is not by itself
// evidence of split brain after CNPG completes a failover.
// Entries live only for the current Cluster UID and are cleared by a verified
// demotion or an unambiguous same-name Pod UID replacement.
type primaryEvidence struct {
	clusterUID types.UID
	members    map[string]types.UID
}

// Caller holds stateMu. Fencing suppresses a conflict only while the fence is
// present; removing a fence without re-verification must not erase the evidence.
func (o *Observer) conflictingPrimary(observed, cluster *unstructured.Unstructured, probed, current []corev1.Pod, results []statusResult) bool {
	key := clusterKey{cluster.GetNamespace(), cluster.GetName()}
	if o.primaries == nil {
		o.primaries = make(map[clusterKey]*primaryEvidence)
	}
	evidence := o.primaries[key]
	if evidence == nil || evidence.clusterUID != cluster.GetUID() {
		evidence = &primaryEvidence{clusterUID: cluster.GetUID(), members: make(map[string]types.UID)}
		o.primaries[key] = evidence
	}
	currentUIDs := make(map[string]types.UID, len(current))
	for _, pod := range current {
		currentUIDs[pod.Name] = pod.UID
		if uid, ok := evidence.members[pod.Name]; ok && uid != pod.UID {
			delete(evidence.members, pod.Name)
		}
	}
	for i, pod := range probed {
		if uid, ok := currentUIDs[pod.Name]; ok && uid != pod.UID {
			continue
		}
		r := results[i]
		if r.err != nil || r.status.IsPrimary == nil {
			continue
		}
		if *r.status.IsPrimary {
			evidence.members[pod.Name] = pod.UID
		} else if evidence.members[pod.Name] == pod.UID {
			delete(evidence.members, pod.Name)
		}
	}
	// Keep all participants in an observed conflict, including the expected
	// primary, so changing currentPrimary cannot erase split-brain evidence.
	// Once verified demotion/replacement leaves only the expected primary,
	// there is no unresolved conflict to carry into a later failover. Compare
	// against collection-start metadata: a superseded collection can still
	// report the legitimate old primary after a promotion event arrives.
	expectedAtCollection, _ := primaryNames(observed)
	if len(evidence.members) == 1 {
		if _, ok := evidence.members[expectedAtCollection]; ok {
			clear(evidence.members)
		}
	}
	var fenced []string
	if raw := cluster.GetAnnotations()["cnpg.io/fencedInstances"]; raw != "" {
		if json.Unmarshal([]byte(raw), &fenced) != nil {
			return true
		}
	}
	expected, _ := primaryNames(cluster)
	for name := range evidence.members {
		if name != expected && !slices.Contains(fenced, "*") && !slices.Contains(fenced, name) {
			return true
		}
	}
	return false
}
