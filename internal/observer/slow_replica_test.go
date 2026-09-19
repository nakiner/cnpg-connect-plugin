package observer

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
)

// A fast primary's sender list cannot replace the other instances' role checks.
// Even a fenced member can supply evidence needed after its fence is removed.
func TestDelayedReplicaConflictIsNotSkipped(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		name := "unfenced"
		if fenced {
			name = "initially_fenced"
		}
		t.Run(name, func(t *testing.T) {
			o, _, _ := fakeObserver(t)
			o.opts.MaxConcurrency = 4
			o.configureProbeLimits()
			key := clusterKey{"test", "db"}
			cluster, _ := o.cluster(key)
			if fenced {
				cluster.SetAnnotations(map[string]string{"cnpg.io/fencedInstances": `["db-2"]`})
				if err := o.clusterInformer.GetIndexer().Update(cluster); err != nil {
					t.Fatal(err)
				}
			}
			_, _, results := fixture()
			primaryReady, replicaStarted, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			o.probe = func(ctx context.Context, pod *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
				switch pod.Name {
				case "db-1":
					close(primaryReady)
					return results[0].status, nil
				case "db-2":
					close(replicaStarted)
					select {
					case <-release:
						return results[0].status, nil // Newly discovered conflicting primary.
					case <-ctx.Done():
						return instanceStatus{}, ctx.Err()
					}
				default:
					return results[2].status, nil
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				o.observe(ctx, key)
			}()
			t.Cleanup(func() { cancel(); <-finished })
			for _, started := range []chan struct{}{primaryReady, replicaStarted} {
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatal("instance probes did not start")
				}
			}
			select {
			case <-finished:
				t.Fatal("observation skipped the pending replica role check")
			case <-time.After(30 * time.Millisecond):
			}
			if snapshot, _ := o.store.Get("test", "db"); snapshot.Available {
				t.Fatal("published availability before the delayed role check")
			}
			close(release)
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("observation did not finish after the replica responded")
			}
			if fenced {
				if snapshot, _ := o.store.Get("test", "db"); !snapshot.Available || snapshot.Members[1].Ready {
					t.Fatal("fenced member must be excluded while the healthy primary stays available")
				}
				cluster.SetAnnotations(nil)
				if err := o.clusterInformer.GetIndexer().Update(cluster); err != nil {
					t.Fatal(err)
				}
				o.probe = func(_ context.Context, pod *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
					if pod.Name == "db-2" {
						return instanceStatus{}, errors.New("unfenced member unreachable")
					}
					if pod.Name == "db-1" {
						return results[0].status, nil
					}
					return results[2].status, nil
				}
				observe(t, o)
			}
			if snapshot, _ := o.store.Get("test", "db"); snapshot.Available || snapshot.Reason != "multiple_primaries_observed" {
				t.Fatalf("lost delayed conflicting-primary evidence: %+v", snapshot)
			}
		})
	}
}
