package observer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestConflictingPrimaryEvidenceSurvivesMetadataAndProbeFailure(t *testing.T) {
	for _, resolution := range []string{"verified standby", "replacement", "fenced"} {
		t.Run(resolution, func(t *testing.T) {
			o, _, _ := fakeObserver(t)
			key := clusterKey{"test", "db"}
			c, _ := o.cluster(key)
			pods := o.pods(c)
			_, _, results := fixture()
			yes := true
			results[1].status.IsPrimary = &yes
			changed := pods[1].DeepCopy()
			changed.Status.Conditions[0].Status = corev1.ConditionFalse
			_ = o.podInformer.GetIndexer().Update(changed)
			publish := func(probed []corev1.Pod, r []statusResult) bool {
				return o.publishObservation(context.Background(), key, c, probed, r, config.Parameters{}, testConnection(t, o), time.Now())
			}
			if publish(pods, results) {
				t.Fatal("readiness change discarded observed conflicting primary")
			}
			_, _, unknown := fixture()
			unknown[1].err = errors.New("unreachable")
			if publish(o.pods(c), unknown) {
				t.Fatal("unknown probe erased negative safety evidence")
			}
			switch resolution {
			case "verified standby":
				_, _, verified := fixture()
				if !publish(o.pods(c), verified) {
					t.Fatal("verified demotion did not clear conflict")
				}
			case "replacement":
				changed.UID = "replacement-uid"
				_ = o.podInformer.GetIndexer().Update(changed)
				if !publish(o.pods(c), unknown) {
					t.Fatal("replacement UID inherited prior instance conflict")
				}
			case "fenced":
				c.SetAnnotations(map[string]string{"cnpg.io/fencedInstances": `["db-2"]`})
				_ = o.clusterInformer.GetIndexer().Update(c)
				if !publish(o.pods(c), unknown) {
					t.Fatal("fenced conflict blocked healthy primary")
				}
			}
		})
	}
}

func TestCompletedFailoverDoesNotInheritHealthyPrimaryEvidence(t *testing.T) {
	for _, history := range []string{"cold", "healthy", "superseded collection", "resolved conflict"} {
		t.Run(history, func(t *testing.T) {
			o, _, _ := fakeObserver(t)
			key := clusterKey{"test", "db"}
			old, _ := o.cluster(key)
			pods := o.pods(old)
			_, _, results := fixture()
			publish := func(ctx context.Context, cluster *unstructured.Unstructured, results []statusResult) bool {
				return o.publishObservation(ctx, key, cluster, pods, results, config.Parameters{}, testConnection(t, o), time.Now())
			}
			if history == "resolved conflict" {
				_, _, conflict := fixture()
				yes := true
				conflict[1].status.IsPrimary = &yes
				if publish(context.Background(), old, conflict) {
					t.Fatal("accepted conflicting primaries")
				}
			}
			if history == "healthy" || history == "resolved conflict" {
				if !publish(context.Background(), old, results) {
					t.Fatal("healthy original primary was unavailable")
				}
			}
			next := old.DeepCopy()
			_ = unstructured.SetNestedField(next.Object, "db-2", "status", "currentPrimary")
			_ = unstructured.SetNestedField(next.Object, "db-2", "status", "targetPrimary")
			_ = o.clusterInformer.GetIndexer().Update(next)
			if history == "superseded collection" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if publish(ctx, old, results) {
					t.Fatal("canceled collection renewed routing")
				}
			}
			pods[0].Status.Conditions[0].Status = corev1.ConditionFalse
			_ = o.podInformer.GetIndexer().Update(&pods[0])
			results[0].err = errors.New("former primary remains unreachable")
			yes := true
			results[1].status.IsPrimary = &yes
			for range 3 {
				if !publish(context.Background(), next, results) {
					snapshot, _ := o.store.Get("test", "db")
					t.Fatalf("completed failover remains blocked: %s", snapshot.Reason)
				}
			}
		})
	}
}

func TestActualConflictSurvivesPrimaryChangeAndFenceRemoval(t *testing.T) {
	o, _, _ := fakeObserver(t)
	key := clusterKey{"test", "db"}
	old, _ := o.cluster(key)
	pods := o.pods(old)
	_, _, results := fixture()
	yes := true
	results[1].status.IsPrimary = &yes
	next := old.DeepCopy()
	_ = unstructured.SetNestedField(next.Object, "db-2", "status", "currentPrimary")
	_ = unstructured.SetNestedField(next.Object, "db-2", "status", "targetPrimary")
	_ = o.clusterInformer.GetIndexer().Update(next)
	// A superseded collection must still retain genuine conflicting evidence.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	o.publishObservation(ctx, key, old, pods, results, config.Parameters{}, testConnection(t, o), time.Now())
	results[0].err = errors.New("old primary unreachable")
	publish := func() bool {
		return o.publishObservation(context.Background(), key, next, pods, results, config.Parameters{}, testConnection(t, o), time.Now())
	}
	if publish() {
		t.Fatal("primary metadata change erased an actual conflict")
	}
	next.SetAnnotations(map[string]string{"cnpg.io/fencedInstances": `["db-1"]`})
	_ = o.clusterInformer.GetIndexer().Update(next)
	if !publish() {
		t.Fatal("fenced old primary blocked healthy new primary")
	}
	next.SetAnnotations(nil)
	_ = o.clusterInformer.GetIndexer().Update(next)
	if publish() {
		t.Fatal("removing fencing erased unresolved evidence")
	}
	no := false
	results[0].err = nil
	results[0].status.IsPrimary = &no
	if !publish() {
		t.Fatal("verified demotion did not resolve conflict")
	}
}

func TestConflictSurvivesPodDeletionUntilReplacement(t *testing.T) {
	o, _, _ := fakeObserver(t)
	key := clusterKey{"test", "db"}
	c, _ := o.cluster(key)
	pods := o.pods(c)
	_, _, r := fixture()
	yes := true
	r[1].status.IsPrimary = &yes
	o.publishObservation(context.Background(), key, c, pods, r, config.Parameters{}, testConnection(t, o), time.Now())
	_ = o.podInformer.GetIndexer().Delete(&pods[1])
	current := o.pods(c)
	_, _, fresh := fixture()
	mapped := matchInstanceResults(pods, current, fresh)
	if o.publishObservation(context.Background(), key, c, current, mapped, config.Parameters{}, testConnection(t, o), time.Now()) {
		t.Fatal("Pod disappearance is not verified database fencing")
	}
}
