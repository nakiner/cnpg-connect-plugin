package observer

import (
	"container/list"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
)

const urgentBurstLimit = 8

// priorityQueue coalesces events like a client-go workqueue: a cluster has at
// most one observation in flight and one pending follow-up. Both worker classes
// share this state, so reserved urgent workers cannot duplicate active work.
// Ordinary workers permit one background item after eight urgent dequeues.
type priorityQueue struct {
	mu           sync.Mutex
	cond         *sync.Cond
	urgent       list.List
	background   list.List
	entries      map[clusterKey]*list.Element
	dirty        map[clusterKey]struct{}
	processing   map[clusterKey]bool // Presence means active; value is its fixed urgency.
	promoted     map[clusterKey]bool // Urgency of queued work or an active key's follow-up.
	burst        int
	shuttingDown bool
	drain        bool
}

var _ workqueue.TypedInterface[clusterKey] = (*priorityQueue)(nil)

func newObservationQueue() (workqueue.TypedRateLimitingInterface[clusterKey], *priorityQueue) {
	priority := &priorityQueue{
		entries:    make(map[clusterKey]*list.Element),
		dirty:      make(map[clusterKey]struct{}),
		processing: make(map[clusterKey]bool),
		promoted:   make(map[clusterKey]bool),
	}
	priority.cond = sync.NewCond(&priority.mu)
	delayed := workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[clusterKey]{
		Queue: priority,
	})
	retries := workqueue.NewTypedItemExponentialFailureRateLimiter[clusterKey](100*time.Millisecond, 5*time.Second)
	queue := workqueue.NewTypedRateLimitingQueueWithConfig(retries, workqueue.TypedRateLimitingQueueConfig[clusterKey]{
		DelayingQueue: delayed,
	})
	return queue, priority
}

// promote precedes Add and also moves an already queued background item. For an
// active key it affects only the next observation, leaving IsUrgent unchanged.
func (p *priorityQueue) promote(key clusterKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.shuttingDown || p.promoted[key] {
		return
	}
	p.promoted[key] = true
	if e, ok := p.entries[key]; ok {
		p.background.Remove(e)
		p.entries[key] = p.urgent.PushBack(key)
		p.cond.Broadcast()
	}
}

func (p *priorityQueue) Add(key clusterKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.shuttingDown {
		return
	}
	if _, exists := p.dirty[key]; exists {
		return
	}
	p.dirty[key] = struct{}{}
	if _, active := p.processing[key]; !active {
		p.push(key)
	}
}

// push requires mu. Broadcast lets regular workers wake for background work
// even when an urgent-only worker is also waiting on the same condition.
func (p *priorityQueue) push(key clusterKey) {
	if p.promoted[key] {
		p.entries[key] = p.urgent.PushBack(key)
	} else {
		p.entries[key] = p.background.PushBack(key)
	}
	p.cond.Broadcast()
}

func (p *priorityQueue) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}

func (p *priorityQueue) Get() (clusterKey, bool) {
	return p.get(false)
}

// GetUrgent waits for urgent work without taking background observations. Its
// returned keys use the same Done method as the ordinary rate-limited queue.
func (p *priorityQueue) GetUrgent() (clusterKey, bool) {
	return p.get(true)
}

func (p *priorityQueue) get(urgentOnly bool) (clusterKey, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.urgent.Len() == 0 && (urgentOnly || p.background.Len() == 0) {
		if p.shuttingDown {
			return clusterKey{}, true
		}
		p.cond.Wait()
	}

	urgent := p.urgent.Len() > 0 && (urgentOnly || p.burst < urgentBurstLimit || p.background.Len() == 0)
	var e *list.Element
	if urgent {
		e = p.urgent.Front()
		p.urgent.Remove(e)
		if !urgentOnly && p.burst < urgentBurstLimit {
			p.burst++
		}
	} else {
		e = p.background.Front()
		p.background.Remove(e)
		p.burst = 0
	}
	key := e.Value.(clusterKey)
	delete(p.entries, key)
	delete(p.dirty, key)
	delete(p.promoted, key)
	p.processing[key] = urgent
	return key, false
}

// IsUrgent reports the classification assigned when active work was dequeued.
// An event received while it runs can promote its follow-up, not this work.
func (p *priorityQueue) IsUrgent(key clusterKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.processing[key]
}

func (p *priorityQueue) Done(key clusterKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, active := p.processing[key]; !active {
		return
	}
	delete(p.processing, key)
	if _, dirty := p.dirty[key]; dirty {
		p.push(key)
	}
	p.cond.Broadcast()
}

// ShutDown rejects new work and wakes both worker classes. Work accepted before
// shutdown remains available to Get; urgent workers never drain background work.
func (p *priorityQueue) ShutDown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drain = false
	p.shuttingDown = true
	p.cond.Broadcast()
}

// ShutDownWithDrain waits for queued and active work to complete. Workers must
// keep calling Get and Done; ShutDown interrupts an outstanding drain wait.
func (p *priorityQueue) ShutDownWithDrain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drain = true
	p.shuttingDown = true
	p.cond.Broadcast()
	for p.drain && (len(p.entries) > 0 || len(p.processing) > 0) {
		p.cond.Wait()
	}
}

func (p *priorityQueue) ShuttingDown() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.shuttingDown
}
