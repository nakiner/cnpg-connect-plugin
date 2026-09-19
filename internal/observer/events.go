package observer

import (
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

func cachedCluster(obj any) *unstructured.Unstructured {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	c, _ := obj.(*unstructured.Unstructured)
	return c
}

func cachedPod(obj any) *corev1.Pod {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	p, _ := obj.(*corev1.Pod)
	return p
}

func podCluster(obj any) (clusterKey, bool) {
	p := cachedPod(obj)
	if p == nil {
		return clusterKey{}, false
	}
	for _, owner := range p.OwnerReferences {
		if owner.Kind == "Cluster" && owner.APIVersion == "postgresql.cnpg.io/v1" {
			return clusterKey{p.Namespace, owner.Name}, true
		}
	}
	return clusterKey{}, false
}

func (o *Observer) clusterEvent(oldObj, newObj any) {
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	c := cachedCluster(newObj)
	if c == nil {
		return
	}
	key := clusterKey{c.GetNamespace(), c.GetName()}
	enabled, _, _ := parameters(c)
	if !enabled || c.GetDeletionTimestamp() != nil {
		o.cancelObservationLocked(key)
		delete(o.primaries, key)
		o.demandNext.Delete(key)
		o.store.Delete(key.namespace, key.name)
		return
	}
	old := cachedCluster(oldObj)
	previous, exists := o.store.ClusterIdentity(key.namespace, key.name)
	if !exists || previous.UID != string(c.GetUID()) {
		o.cancelObservationLocked(key)
		delete(o.primaries, key)
		o.demandNext.Delete(key)
		o.publish(unavailable(c, time.Now().UTC(), o.opts.TTL, "awaiting_observation"), time.Time{})
	}
	if old != nil {
		if sameClusterRoute(old, c) {
			return
		}
		o.cancelObservationLocked(key)
		o.publish(unavailable(c, time.Now().UTC(), o.opts.TTL, "topology_changed"), time.Time{})
	}
	if o.store.HasDemand(key.namespace, key.name) {
		o.Notify(key.namespace, key.name)
	}
}

func (o *Observer) clusterDeleted(obj any) {
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	c := cachedCluster(obj)
	if c == nil {
		return
	}
	key := clusterKey{c.GetNamespace(), c.GetName()}
	// A delayed tombstone must not delete a same-name replacement.
	if current, ok := o.cluster(key); ok && current.GetUID() != c.GetUID() {
		return
	}
	o.cancelObservationLocked(key)
	o.store.Delete(key.namespace, key.name)
	delete(o.primaries, key)
	o.demandNext.Delete(key)
}

func (o *Observer) podEvent(oldObj, newObj any) {
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	old, new := cachedPod(oldObj), cachedPod(newObj)
	if old != nil && new != nil && samePodRoute(*old, *new) &&
		old.Labels["topology.kubernetes.io/zone"] == new.Labels["topology.kubernetes.io/zone"] &&
		old.Labels["topology.kubernetes.io/region"] == new.Labels["topology.kubernetes.io/region"] {
		return
	}
	keys := map[clusterKey]struct{}{}
	if key, ok := podCluster(oldObj); ok {
		keys[key] = struct{}{}
	}
	if key, ok := podCluster(newObj); ok {
		keys[key] = struct{}{}
	}
	for key := range keys {
		if c, ok := o.cluster(key); ok {
			current, _ := primaryNames(c)
			if old != nil && (new == nil || !samePodRoute(*old, *new)) {
				// Invalidation is independent of consumer demand. Otherwise a
				// reconnect can receive a retained, known-invalid route before I/O.
				if old.Name == current {
					o.cancelObservationLocked(key)
					o.publish(unavailable(c, time.Now().UTC(), o.opts.TTL, "primary_changed"), time.Time{})
				} else if snapshot, exists := o.store.Get(key.namespace, key.name); exists && snapshot.Cluster.UID == string(c.GetUID()) {
					for i := range snapshot.Members {
						if snapshot.Members[i].ID == string(old.UID) {
							snapshot.Members[i].Ready = false
							snapshot.Members[i].Reason = "member_changed"
						}
					}
					// Do not extend the independent primary's observation lifetime.
					o.publish(snapshot, time.Time{})
				}
			}
		}
		// Notify itself gates expensive collection on live demand.
		o.Notify(key.namespace, key.name)
	}
}

func (o *Observer) cluster(key clusterKey) (*unstructured.Unstructured, bool) {
	obj, exists, err := o.clusterInformer.GetIndexer().GetByKey(key.String())
	if err != nil || !exists {
		return nil, false
	}
	return obj.(*unstructured.Unstructured).DeepCopy(), true
}

func (o *Observer) pods(cluster *unstructured.Unstructured) []corev1.Pod {
	objects, _ := o.podInformer.GetIndexer().ByIndex(clusterIndex, cluster.GetNamespace()+"/"+cluster.GetName())
	list := &corev1.PodList{Items: make([]corev1.Pod, 0, len(objects))}
	for _, obj := range objects {
		list.Items = append(list.Items, *obj.(*corev1.Pod).DeepCopy())
	}
	return ownedPods(cluster, list)
}

func sameClusterRoute(a, b *unstructured.Unstructured) bool {
	if a.GetUID() != b.GetUID() || a.GetDeletionTimestamp() != nil || b.GetDeletionTimestamp() != nil {
		return false
	}
	currentA, targetA := primaryNames(a)
	currentB, targetB := primaryNames(b)
	if currentA != currentB || targetA != targetB {
		return false
	}
	enabledA, paramsA, errA := parameters(a)
	enabledB, paramsB, errB := parameters(b)
	if errA != nil || errB != nil || enabledA != enabledB || !reflect.DeepEqual(paramsA, paramsB) {
		return false
	}
	if a.GetAnnotations()["cnpg.io/fencedInstances"] != b.GetAnnotations()["cnpg.io/fencedInstances"] {
		return false
	}
	if applicationDatabase(a) != applicationDatabase(b) {
		return false
	}
	certificatesA, _, _ := unstructured.NestedFieldNoCopy(a.Object, "status", "certificates")
	certificatesB, _, _ := unstructured.NestedFieldNoCopy(b.Object, "status", "certificates")
	return reflect.DeepEqual(certificatesA, certificatesB)
}

func samePodRoute(a, b corev1.Pod) bool {
	if a.Name != b.Name || a.UID != b.UID || a.Status.PodIP != b.Status.PodIP {
		return false
	}
	if podReady(a) != podReady(b) || a.Spec.NodeName != b.Spec.NodeName {
		return false
	}
	if statusScheme(&a) != statusScheme(&b) || postgresContainerID(a) != postgresContainerID(b) {
		return false
	}
	return reflect.DeepEqual(a.OwnerReferences, b.OwnerReferences)
}

func postgresContainerID(p corev1.Pod) string {
	for _, s := range p.Status.ContainerStatuses {
		if s.Name == "postgres" {
			return s.ContainerID
		}
	}
	return ""
}
