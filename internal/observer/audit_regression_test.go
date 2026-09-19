package observer

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestIdlePodChangesInvalidateBeforeReconnect(t *testing.T) {
	for _, name := range []string{"db-1", "db-2"} {
		for _, mode := range []string{"delete", "unready", "address", "container"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				o, _, _ := fakeObserver(t)
				observe(t, o)
				c, _ := o.cluster(clusterKey{"test", "db"})
				for _, pod := range o.pods(c) {
					if pod.Name != name {
						continue
					}
					next := pod.DeepCopy()
					switch mode {
					case "delete":
						next = nil
					case "unready":
						next.Status.Conditions[0].Status = corev1.ConditionFalse
					case "address":
						next.Status.PodIP = "10.0.0.99"
					case "container":
						next.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "postgres", ContainerID: "replacement"}}
					}
					if next == nil {
						_ = o.podInformer.GetIndexer().Delete(&pod)
						o.podEvent(&pod, nil)
					} else {
						_ = o.podInformer.GetIndexer().Update(next)
						o.podEvent(&pod, next)
					}
				}
				if o.queue.Len() != 0 {
					t.Fatal("idle event scheduled expensive collection")
				}
				updates, cancel := o.store.Subscribe("test", "db")
				defer cancel()
				initial := <-updates
				if name == "db-1" && initial.Available {
					t.Fatal("reconnect received known stale primary")
				}
				for _, m := range initial.Members {
					if m.Name == name && m.Ready {
						t.Fatal("reconnect received known stale member")
					}
				}
				if name == "db-2" && !initial.Available {
					t.Fatal("replica change invalidated unrelated primary")
				}
				if o.queue.Len() == 0 {
					t.Fatal("reconnect did not schedule verification")
				}

			})
		}
	}
}
