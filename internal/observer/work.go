package observer

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errObservationSuperseded = errors.New("observation superseded by metadata")

// Keep a small part of each limit available for events, even when background
// endpoints are slow. A limit of one cannot reserve independent capacity.
func reservedCapacity(limit int) int {
	if limit < 2 {
		return 0
	}
	return max(1, limit/8)
}

func (o *Observer) configureProbeLimits() {
	o.probeSlots = make(chan struct{}, o.opts.MaxConcurrency)
	background := o.opts.MaxConcurrency - reservedCapacity(o.opts.MaxConcurrency)
	o.backgroundProbeSlots = make(chan struct{}, background)
}

func (o *Observer) startWorkers(ctx context.Context, workers *sync.WaitGroup) {
	o.startCAWorkers(ctx, workers)
	reserved := reservedCapacity(o.opts.MaxConcurrentClusters)
	for i := range o.opts.MaxConcurrentClusters {
		urgentOnly := i < reserved
		workers.Go(func() {
			for o.workNext(ctx, urgentOnly) {
			}
		})
	}
}

func (o *Observer) work(ctx context.Context) bool {
	return o.workNext(ctx, false)
}

func (o *Observer) workNext(ctx context.Context, urgentOnly bool) bool {
	var key clusterKey
	var shutdown bool
	if urgentOnly {
		key, shutdown = o.priority.GetUrgent()
	} else {
		key, shutdown = o.queue.Get()
	}
	if shutdown {
		return false
	}
	defer o.queue.Done(key)
	if ctx.Err() != nil {
		return false
	}
	if !o.needsObservation(key) {
		o.queue.Forget(key)
		return true
	}
	urgent := o.priority.IsUrgent(key)
	if !o.Ready() {
		if urgent {
			o.priority.promote(key)
		}
		o.queue.AddAfter(key, o.opts.PollInterval)
		return true
	}

	observationCtx, finish := o.beginObservation(ctx, key)
	observed := o.observe(observationCtx, key)
	superseded := errors.Is(context.Cause(observationCtx), errObservationSuperseded)
	finish()
	if superseded || !o.needsObservation(key) {
		// The event already queued the replacement. Cancellation is not a failed
		// probe and must not add retry backoff or another periodic timer.
		o.queue.Forget(key)
		return true
	}
	if observed {
		o.queue.Forget(key)
		o.queue.AddAfter(key, o.opts.PollInterval)
	} else {
		// A promotion may need another verification while PostgreSQL finishes
		// changing roles. Keep its reserved capacity through the bounded retry.
		if urgent {
			o.priority.promote(key)
		}
		o.queue.AddRateLimited(key)
	}
	return true
}

func (o *Observer) beginObservation(parent context.Context, key clusterKey) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	o.stateMu.Lock()
	o.observations[key] = cancel
	o.stateMu.Unlock()
	return ctx, func() {
		cancel(nil)
		o.stateMu.Lock()
		delete(o.observations, key)
		o.stateMu.Unlock()
	}
}

// The caller holds stateMu, ordering cancellation with metadata invalidation
// and publication. The queue permits only one in-flight observation per key.
func (o *Observer) cancelObservationLocked(key clusterKey) {
	if cancel := o.observations[key]; cancel != nil {
		cancel(errObservationSuperseded)
	}
}

func (o *Observer) needsObservation(key clusterKey) bool {
	if !o.store.HasDemand(key.namespace, key.name) {
		return false
	}
	// Watchers survive deletion and opt-out, but their absent records need no
	// periodic work. A recreation or re-enable event will enqueue them again.
	_, exists := o.store.ClusterIdentity(key.namespace, key.name)
	return exists
}

func (o *Observer) acquireProbe(ctx context.Context, urgent bool) (release func(), resultErr error) {
	started := time.Now()
	defer o.probeQueueDuration.UpdateDuration(started)
	defer func() {
		if resultErr != nil {
			o.probeCancellations.Add(1)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !urgent {
		select {
		case o.backgroundProbeSlots <- struct{}{}:
		default:
			o.probeSaturation.Add(1)
			select {
			case o.backgroundProbeSlots <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	select {
	case o.probeSlots <- struct{}{}:
	default:
		o.probeSaturation.Add(1)
		select {
		case o.probeSlots <- struct{}{}:
		case <-ctx.Done():
			if !urgent {
				<-o.backgroundProbeSlots
			}
			return nil, ctx.Err()
		}
	}
	return func() {
		<-o.probeSlots
		if !urgent {
			<-o.backgroundProbeSlots
		}
	}, nil
}
