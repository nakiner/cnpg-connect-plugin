package observer

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func (o *Observer) observe(ctx context.Context, key clusterKey) bool {
	started := time.Now()
	defer o.observationDuration.UpdateDuration(started)
	// Include queueing for probe slots and CA lookup in the collection deadline.
	// A large or unreachable cluster must not monopolize a worker beyond its TTL.
	timeout := o.opts.TTL - o.opts.PollInterval
	if o.opts.ProbeTimeout <= timeout/2 {
		timeout = 2 * o.opts.ProbeTimeout
	}
	ioCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	o.stateMu.Lock()
	c, exists := o.cluster(key)
	if !exists {
		deliver := o.store.DeleteDeferred(key.namespace, key.name)
		delete(o.primaries, key)
		o.stateMu.Unlock()
		deliver()
		return true
	}
	enabled, params, paramErr := parameters(c)
	if !enabled || c.GetDeletionTimestamp() != nil {
		deliver := o.store.DeleteDeferred(key.namespace, key.name)
		delete(o.primaries, key)
		o.stateMu.Unlock()
		deliver()
		return true
	}
	o.stateMu.Unlock()
	if paramErr != nil {
		return o.publishFailure(ctx, key, "invalid_configuration", started)
	}
	current, target := primaryNames(c)
	if current == "" || current != target {
		return o.publishFailure(ctx, key, "primary_transition", started)
	}
	pods := o.pods(c)
	// Start primary verification before allocating slots to its standbys.
	for i := range pods {
		if pods[i].Name == current {
			pods[0], pods[i] = pods[i], pods[0]
			break
		}
	}
	connection, err := o.cachedConnection(ioCtx, c)
	if err != nil {
		return o.publishFailure(ctx, key, "connection_defaults_unavailable", started)
	}
	serverName := params.ServerName
	if serverName == "" {
		serverName = fmt.Sprintf("%s-rw.%s.svc", c.GetName(), c.GetNamespace())
	}
	results := o.probeInstances(ioCtx, pods, connection.ConnectionParameters, serverName, o.priority.IsUrgent(key))
	// Exhausting the I/O budget is not supersession or shutdown. Completed
	// results remain useful; incomplete probes already carry errors. Publish
	// against the original context and recheck current metadata under the lock.
	return o.publishObservation(ctx, key, c, pods, results, params, connection, started)
}

// Configuration failures use the current metadata, which may have changed while
// the observation was loading connection defaults.
func (o *Observer) publishFailure(ctx context.Context, key clusterKey, reason string, started time.Time) bool {
	var pending pendingDeliveries
	defer pending.deliver()
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	latest, exists := o.cluster(key)
	if !exists {
		pending.add(o.store.DeleteDeferred(key.namespace, key.name), nil)
		return false
	}
	enabled, _, _ := parameters(latest)
	if !enabled || latest.GetDeletionTimestamp() != nil {
		pending.add(o.store.DeleteDeferred(key.namespace, key.name), nil)
		return false
	}
	snapshot := unavailable(latest, time.Now().UTC(), o.opts.TTL, reason)
	current, target := primaryNames(latest)
	snapshot.Transitioning = current == "" || current != target
	pending.add(o.commitPublication(snapshot, started))
	return false
}

func (o *Observer) probeInstances(ctx context.Context, pods []corev1.Pod, connection v1.ConnectionParameters, serverName string, urgent bool) []statusResult {
	results := make([]statusResult, len(pods))
	// The caller puts the expected primary first. If its verification fails,
	// remaining replicas cannot make the Cluster writable. Stop their I/O but
	// keep the outer observation alive so it can publish the failure promptly.
	probeContext, stopProbes := context.WithCancel(ctx)
	defer stopProbes()
	var probes sync.WaitGroup
	for i := range pods {
		if !podReady(pods[i]) {
			results[i].err = fmt.Errorf("pod not ready")
			if i == 0 {
				stopProbes()
			}
			continue
		}
		// Acquire before spawning: waiting instances do not create one goroutine
		// each, and simultaneous network requests stay globally bounded.
		release, err := o.acquireProbe(probeContext, urgent)
		if err != nil {
			results[i].err = err
			continue
		}
		probes.Go(func() {
			defer release()
			started := time.Now()
			defer o.probeDuration.UpdateDuration(started)
			probeCtx, cancel := context.WithTimeout(probeContext, o.opts.ProbeTimeout)
			defer cancel()
			results[i].status, results[i].err = o.probe(probeCtx, &pods[i], connection, serverName)
			if i == 0 {
				primary := results[i]
				if primary.err != nil || primary.status.IsPrimary == nil || !*primary.status.IsPrimary || primary.status.Unavailable || primary.status.Rewinding {
					stopProbes()
				}
			}
		})
	}
	probes.Wait()
	return results
}

// Publish only results whose routing metadata is still current. The state lock
// orders publication against metadata events that invalidate existing routes.
func (o *Observer) publishObservation(
	ctx context.Context,
	key clusterKey,
	observed *unstructured.Unstructured,
	probedPods []corev1.Pod,
	results []statusResult,
	params config.Parameters,
	connection connectionInfo,
	started time.Time,
) bool {
	var pending pendingDeliveries
	defer pending.deliver()
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	latest, exists := o.cluster(key)
	if !exists {
		pending.add(o.store.DeleteDeferred(key.namespace, key.name), nil)
		delete(o.primaries, key)
		return true
	}
	enabled, _, _ := parameters(latest)
	if !enabled || latest.GetDeletionTimestamp() != nil {
		pending.add(o.store.DeleteDeferred(key.namespace, key.name), nil)
		delete(o.primaries, key)
		return true
	}
	latestPods := o.pods(latest)
	conflict := false
	if observed.GetUID() == latest.GetUID() {
		conflict = o.conflictingPrimary(observed, latest, probedPods, latestPods, results)
	}
	// Preserve obtained safety evidence even when a metadata event superseded
	// collection, but never renew routes after cancellation or watch failure.
	if ctx.Err() != nil || !o.Ready() {
		return false
	}
	if !sameClusterRoute(observed, latest) {
		o.Notify(key.namespace, key.name)
		pending.add(o.commitPublication(unavailable(latest, time.Now().UTC(), o.opts.TTL, "topology_changed_during_observation"), started))
		return false
	}
	if !o.currentCA(connection.ca) {
		o.Notify(key.namespace, key.name)
		pending.add(o.commitPublication(unavailable(latest, time.Now().UTC(), o.opts.TTL, "connection_defaults_changed"), started))
		return false
	}
	mapped := matchInstanceResults(probedPods, latestPods, results)
	// A changed replica is excluded until verified. It does not invalidate the
	// independently verified primary or unrelated replicas.
	snapshot := buildSnapshot(latest, latestPods, mapped, params, started.UTC(), o.opts.TTL)
	if conflict {
		snapshot.Available = false
		snapshot.PrimaryID = ""
		snapshot.Reason = "multiple_primaries_observed"
		for i := range snapshot.Members {
			snapshot.Members[i].Ready = false
		}
	}
	snapshot.Connection = connection.ConnectionParameters
	pending.add(o.commitPublication(snapshot, started))
	return snapshot.Available
}

func matchInstanceResults(probedPods, currentPods []corev1.Pod, results []statusResult) []statusResult {
	byName := make(map[string]int, len(probedPods))
	for i, pod := range probedPods {
		byName[pod.Name] = i
	}
	mapped := make([]statusResult, len(currentPods))
	for i, pod := range currentPods {
		previous, exists := byName[pod.Name]
		if !exists || !samePodRoute(probedPods[previous], pod) {
			mapped[i].err = fmt.Errorf("member changed during observation")
			continue
		}
		mapped[i] = results[previous]
	}
	return mapped
}
