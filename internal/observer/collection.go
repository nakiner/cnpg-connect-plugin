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
	// Include queueing for probe slots and CA lookup in the collection deadline.
	// A large or unreachable cluster must not monopolize a worker beyond its TTL.
	timeout := o.opts.TTL - o.opts.PollInterval
	if o.opts.ProbeTimeout <= timeout/2 {
		timeout = 2 * o.opts.ProbeTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	o.stateMu.Lock()
	c, exists := o.cluster(key)
	if !exists {
		o.store.Delete(key.namespace, key.name)
		o.stateMu.Unlock()
		return true
	}
	enabled, params, paramErr := parameters(c)
	if !enabled || c.GetDeletionTimestamp() != nil {
		o.store.Delete(key.namespace, key.name)
		o.stateMu.Unlock()
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
	connection, err := o.cachedConnection(ctx, c)
	if err != nil {
		return o.publishFailure(ctx, key, "connection_defaults_unavailable", started)
	}
	serverName := params.ServerName
	if serverName == "" {
		serverName = fmt.Sprintf("%s-rw.%s.svc", c.GetName(), c.GetNamespace())
	}
	results := o.probeInstances(ctx, pods, connection, serverName, o.priority.IsUrgent(key))
	if ctx.Err() != nil {
		return false
	}
	return o.publishObservation(ctx, key, c, pods, results, params, connection, started)
}

// Configuration failures use the current metadata, which may have changed while
// the observation was loading connection defaults.
func (o *Observer) publishFailure(ctx context.Context, key clusterKey, reason string, started time.Time) bool {
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	latest, exists := o.cluster(key)
	if !exists {
		o.store.Delete(key.namespace, key.name)
		return false
	}
	enabled, _, _ := parameters(latest)
	if !enabled || latest.GetDeletionTimestamp() != nil {
		o.store.Delete(key.namespace, key.name)
		return false
	}
	snapshot := unavailable(latest, time.Now().UTC(), o.opts.TTL, reason)
	current, target := primaryNames(latest)
	snapshot.Transitioning = current == "" || current != target
	o.publish(snapshot, started)
	return false
}

func (o *Observer) probeInstances(ctx context.Context, pods []corev1.Pod, connection v1.ConnectionParameters, serverName string, urgent bool) []statusResult {
	results := make([]statusResult, len(pods))
	var probes sync.WaitGroup
	for i := range pods {
		if !podReady(pods[i]) {
			results[i].err = fmt.Errorf("pod not ready")
			continue
		}
		// Acquire before spawning: waiting instances do not create one goroutine
		// each, and simultaneous network requests stay globally bounded.
		release, err := o.acquireProbe(ctx, urgent)
		if err != nil {
			results[i].err = err
			continue
		}
		probes.Go(func() {
			defer release()
			probeCtx, cancel := context.WithTimeout(ctx, o.opts.ProbeTimeout)
			defer cancel()
			results[i].status, results[i].err = o.probe(probeCtx, &pods[i], connection, serverName)
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
	connection v1.ConnectionParameters,
	started time.Time,
) bool {
	o.stateMu.Lock()
	defer o.stateMu.Unlock()
	// Never renew a cached observation while either metadata watch is disconnected.
	if ctx.Err() != nil || !o.Ready() {
		return false
	}
	latest, exists := o.cluster(key)
	if !exists {
		o.store.Delete(key.namespace, key.name)
		return true
	}
	enabled, _, _ := parameters(latest)
	if !enabled || latest.GetDeletionTimestamp() != nil {
		o.store.Delete(key.namespace, key.name)
		return true
	}
	if !sameClusterRoute(observed, latest) {
		o.Notify(key.namespace, key.name)
		o.publish(unavailable(latest, time.Now().UTC(), o.opts.TTL, "topology_changed_during_observation"), started)
		return false
	}
	latestPods := o.pods(latest)
	mapped := matchInstanceResults(probedPods, latestPods, results)
	current, _ := primaryNames(latest)
	for i, pod := range latestPods {
		if pod.Name == current && isCertificateError(mapped[i].err) {
			// A CA rotation may cause a TLS failure before CNPG publishes certificate
			// metadata. Refetch the named public CA on the next attempt.
			o.connections.Delete(key)
		}
	}
	// A changed replica is excluded until verified. It does not invalidate the
	// independently verified primary or unrelated replicas.
	snapshot := buildSnapshot(latest, latestPods, mapped, params, started.UTC(), o.opts.TTL)
	snapshot.Connection = connection
	o.publish(snapshot, started)
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

func (o *Observer) publish(snapshot v1.Snapshot, started time.Time) {
	// Avoid flooding logs with dormant inventory placeholders.
	changed := o.store.Put(snapshot)
	if !changed || snapshot.Reason == "awaiting_observation" {
		return
	}
	members := make([]string, 0, len(snapshot.Members))
	primary := ""
	for _, m := range snapshot.Members {
		members = append(members, fmt.Sprintf("%s:%s:%s:%t", m.Name, m.Role, m.SyncState, m.Ready))
		if m.ID == snapshot.PrimaryID {
			primary = m.Name
		}
	}
	elapsed := time.Duration(0)
	if !started.IsZero() {
		elapsed = time.Since(started)
	}
	o.log.Info("topology changed",
		"namespace", snapshot.Cluster.Namespace,
		"cluster", snapshot.Cluster.Name,
		"primary", primary,
		"available", snapshot.Available,
		"reason", snapshot.Reason,
		"members", members,
		"observation_duration", elapsed,
	)
}
