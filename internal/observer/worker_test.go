package observer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
)

type recordingScheduleQueue struct {
	workqueue.TypedRateLimitingInterface[clusterKey]
	delayed int
}

func (q *recordingScheduleQueue) AddAfter(clusterKey, time.Duration) {
	q.delayed++
}

func TestRemovedClusterStopsRefreshUntilMetadataReturns(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		for _, duringProbe := range []bool{false, true} {
			name := "disabled"
			if deleted {
				name = "deleted"
			}
			if duringProbe {
				name += " during probe"
			}
			t.Run(name, func(t *testing.T) {
				o, _, _ := fakeObserver(t)
				queue := &recordingScheduleQueue{TypedRateLimitingInterface: o.queue}
				o.queue = queue
				_, unsubscribe := o.store.Subscribe("test", "db")
				defer unsubscribe()
				old, _ := o.cluster(clusterKey{"test", "db"})
				removed := old.DeepCopy()
				removed.SetAnnotations(map[string]string{config.EnabledAnnotation: "false"})
				remove := func() {
					if deleted {
						_ = o.clusterInformer.GetIndexer().Delete(old)
						o.clusterDeleted(old)
						return
					}
					_ = o.clusterInformer.GetIndexer().Update(removed)
					o.clusterEvent(old, removed)
				}
				if !duringProbe {
					remove()
				}
				var calls atomic.Int64
				var once sync.Once
				probe := o.probe
				o.probe = func(ctx context.Context, pod *corev1.Pod, connection v1.ConnectionParameters, serverName string) (instanceStatus, error) {
					calls.Add(1)
					if duringProbe {
						once.Do(remove)
					}
					return probe(ctx, pod, connection, serverName)
				}
				o.work(context.Background())
				if queue.delayed != 0 || o.queue.Len() != 0 {
					t.Fatal("removed cluster scheduled another observation")
				}
				if !o.store.HasDemand("test", "db") {
					t.Fatal("removal discarded the watch subscription")
				}
				if !duringProbe && calls.Load() != 0 {
					t.Fatal("removed cluster was probed")
				}

				restored := old.DeepCopy()
				if deleted {
					restored.SetUID("replacement-cluster")
					for _, pod := range o.pods(old) {
						pod.OwnerReferences[0].UID = restored.GetUID()
						_ = o.podInformer.GetIndexer().Update(&pod)
					}
				}
				_ = o.clusterInformer.GetIndexer().Add(restored)
				if deleted {
					o.clusterEvent(nil, restored)
				} else {
					o.clusterEvent(removed, restored)
				}
				if o.queue.Len() != 1 {
					t.Fatal("metadata event did not restart the subscribed cluster")
				}
				o.work(context.Background())
				snapshot, exists := o.store.Get("test", "db")
				if !exists || !snapshot.Available || snapshot.Cluster.UID != string(restored.GetUID()) || queue.delayed != 1 {
					t.Fatal("restored cluster did not resume observation and periodic refresh")
				}
			})
		}
	}
}
