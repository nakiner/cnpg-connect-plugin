// Package discovery publishes complete, expiring PostgreSQL topology snapshots.
package discovery

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
)

type clusterKey struct{ namespace, name string }

type record struct {
	snapshot v1.Snapshot
	routing  [sha256.Size]byte
	expired  bool
}

// Store owns its snapshots. Inputs, return values, and subscriber deliveries are
// independent copies, including member slices and endpoint maps.
//
// A subscriber has room for one snapshot. Slow consumers receive the latest
// complete state; no caller may depend on observing every intermediate revision.
type Store struct {
	mu          sync.Mutex
	records     map[clusterKey]*record
	subscribers map[clusterKey]map[chan v1.Snapshot]struct{}
	unaryDemand map[clusterKey]time.Time
	onDemand    func(namespace, name string)
	unaryLease  time.Duration
	processID   string
	sequence    uint64
}

func NewStore() *Store {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(fmt.Sprintf("discovery: generate revision prefix: %v", err))
	}
	return &Store{
		records:     make(map[clusterKey]*record),
		subscribers: make(map[clusterKey]map[chan v1.Snapshot]struct{}),
		unaryDemand: make(map[clusterKey]time.Time),
		processID:   hex.EncodeToString(id[:]),
	}
}

// SetDemandHandler connects consumer interest to the observer's refresh queue.
// The handler runs outside the store lock and should enqueue work without waiting
// for an observation. A positive unaryLease keeps one-shot consumers active for
// that duration after their most recent RequestRefresh.
func (s *Store) SetDemandHandler(handler func(namespace, name string), unaryLease time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onDemand = handler
	s.unaryLease = unaryLease
}

// RequestRefresh starts observations for an idle known cluster and renews its
// unary-consumer lease. Active consumers share the existing refresh schedule.
// Unknown names never allocate demand or trigger work.
// Get intentionally does not create demand: observer reads must remain passive.
func (s *Store) RequestRefresh(namespace, name string) bool {
	key := clusterKey{namespace, name}
	s.mu.Lock()
	if _, exists := s.records[key]; !exists {
		s.mu.Unlock()
		return false
	}
	now := time.Now()
	active := s.hasDemand(key, now)
	if s.unaryLease > 0 {
		s.unaryDemand[key] = now.Add(s.unaryLease)
	}
	handler := s.onDemand
	s.mu.Unlock()
	if !active && handler != nil {
		handler(namespace, name)
	}
	return true
}

// HasDemand reports whether a watcher or an unexpired unary lease needs status
// observations. Watch interest survives deletion so same-name recreation works.
func (s *Store) HasDemand(namespace, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasDemand(clusterKey{namespace, name}, time.Now())
}

// ActiveClusters returns known clusters with consumer interest. Missing records
// are omitted even when a watcher is waiting for same-name recreation.
func (s *Store) ActiveClusters() []v1.ClusterRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	active := make(map[clusterKey]struct{}, len(s.subscribers)+len(s.unaryDemand))
	for key := range s.subscribers {
		active[key] = struct{}{}
	}
	for key, deadline := range s.unaryDemand {
		if now.Before(deadline) {
			active[key] = struct{}{}
		} else {
			delete(s.unaryDemand, key)
		}
	}
	result := make([]v1.ClusterRef, 0, len(active))
	for key := range active {
		if current, exists := s.records[key]; exists {
			result = append(result, current.snapshot.Cluster)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace != result[j].Namespace {
			return result[i].Namespace < result[j].Namespace
		}
		return result[i].Name < result[j].Name
	})
	return result
}

// Put publishes an observation, including refreshes with unchanged routing.
// Revision is assigned by the store and cannot be supplied by a producer.
// Freshness timestamps must come from the actual observation, never a heartbeat.
// The result reports whether routing changed; it avoids a separate Get for logging.
func (s *Store) Put(snapshot v1.Snapshot) bool {
	// Copy and hash before taking the shared lock. Independent databases should
	// not serialize their JSON encoding or payload copying behind one another.
	snapshot = clone(snapshot)
	snapshot.APIVersion = v1.APIVersion
	key := clusterKey{snapshot.Cluster.Namespace, snapshot.Cluster.Name}
	expired := !time.Now().Before(snapshot.ValidUntil)
	if expired {
		invalidate(&snapshot)
	}
	routing := routingDigest(snapshot)

	s.mu.Lock()
	defer s.mu.Unlock()
	// The snapshot may have expired while waiting to commit it.
	if !expired && !time.Now().Before(snapshot.ValidUntil) {
		expired = true
		invalidate(&snapshot)
		routing = routingDigest(snapshot)
	}
	previous, exists := s.records[key]
	changed := !exists || previous.routing != routing
	if changed {
		snapshot.Revision = s.nextRevision()
	} else {
		snapshot.Revision = previous.snapshot.Revision
	}
	s.records[key] = &record{snapshot: snapshot, routing: routing, expired: expired}
	s.broadcast(key, snapshot)
	return changed
}

// Delete removes the current record and publishes a tombstone to watchers.
// Watchers stay subscribed so recreation under the same name is observable.
func (s *Store) Delete(namespace, name string) {
	key := clusterKey{namespace, name}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.unaryDemand, key)
	previous, exists := s.records[key]
	if !exists {
		return
	}
	deleted := clone(previous.snapshot)
	deleted.Revision = s.nextRevision()
	deleted.Available = false
	deleted.Reason = "deleted"
	deleted.PrimaryID = ""
	deleted.Transitioning = false
	deleted.Members = []v1.Member{}
	delete(s.records, key)
	s.broadcast(key, deleted)
}

// ClusterIdentity returns the record's immutable identity without copying its
// topology or creating demand. It does not report routing freshness.
func (s *Store) ClusterIdentity(namespace, name string) (v1.ClusterRef, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.records[clusterKey{namespace, name}]
	if !exists {
		return v1.ClusterRef{}, false
	}
	return current.snapshot.Cluster, true
}

// Get fails closed on stale routing even when the periodic expiry task is late.
func (s *Store) Get(namespace, name string) (v1.Snapshot, bool) {
	key := clusterKey{namespace, name}
	s.mu.Lock()
	record, exists := s.records[key]
	if !exists {
		s.mu.Unlock()
		return v1.Snapshot{}, false
	}
	s.expireRecord(key, record, time.Now())
	snapshot := record.snapshot
	s.mu.Unlock()
	// Published snapshots are immutable inside Store. Copying for the caller
	// outside the lock keeps reads of unrelated databases independent.
	return clone(snapshot), true
}

// Subscribe atomically registers a watcher and queues the current snapshot.
// Unknown clusters have no initial snapshot; watchers wait for a future Put.
// Cancellation is idempotent and closes the channel. Every subscription owns
// the copies it receives and must cancel when it no longer consumes updates.
func (s *Store) Subscribe(namespace, name string) (<-chan v1.Snapshot, func()) {
	updates, cancel, _ := s.subscribe(namespace, name, false)
	return updates, cancel
}

// subscribe optionally rejects unknown clusters in the same critical section
// that registers the watcher, avoiding a separate lookup/subscription race.
func (s *Store) subscribe(namespace, name string, requireExisting bool) (<-chan v1.Snapshot, func(), bool) {
	key := clusterKey{namespace, name}
	updates := make(chan v1.Snapshot, 1)
	s.mu.Lock()
	if _, exists := s.records[key]; !exists && requireExisting {
		s.mu.Unlock()
		return nil, func() {}, false
	}
	if current, exists := s.records[key]; exists {
		s.expireRecord(key, current, time.Now())
		updates <- clone(current.snapshot)
	}
	first := len(s.subscribers[key]) == 0
	if first {
		s.subscribers[key] = make(map[chan v1.Snapshot]struct{})
	}
	s.subscribers[key][updates] = struct{}{}
	handler := s.onDemand
	s.mu.Unlock()
	if first && handler != nil {
		handler(namespace, name)
	}

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.subscribers[key], updates)
			if len(s.subscribers[key]) == 0 {
				delete(s.subscribers, key)
			}
			close(updates)
		})
	}
	return updates, cancel, true
}

// Expire invalidates expired routing without advancing observation timestamps.
func (s *Store) Expire(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, current := range s.records {
		s.expireRecord(key, current, now)
	}
	for key, deadline := range s.unaryDemand {
		if !now.Before(deadline) {
			delete(s.unaryDemand, key)
		}
	}
}

// All helpers below that operate on store state require s.mu.
func (s *Store) hasDemand(key clusterKey, now time.Time) bool {
	deadline := s.unaryDemand[key]
	unary := now.Before(deadline)
	if !unary {
		delete(s.unaryDemand, key)
	}
	return len(s.subscribers[key]) > 0 || unary
}

func (s *Store) expireRecord(key clusterKey, current *record, now time.Time) {
	if current.expired || now.Before(current.snapshot.ValidUntil) {
		return
	}
	expired := clone(current.snapshot)
	invalidate(&expired)
	expired.Revision = s.nextRevision()
	current.snapshot = expired
	current.routing = routingDigest(expired)
	current.expired = true
	s.broadcast(key, current.snapshot)
}

func (s *Store) nextRevision() string {
	s.sequence++
	return fmt.Sprintf("%s:%d", s.processID, s.sequence)
}

func (s *Store) broadcast(key clusterKey, snapshot v1.Snapshot) {
	for updates := range s.subscribers[key] {
		// The mutex serializes all senders and cancellation. A concurrent receiver
		// may drain this slot; either way, the following send cannot block.
		select {
		case <-updates:
		default:
		}
		updates <- clone(snapshot)
	}
}

func invalidate(snapshot *v1.Snapshot) {
	snapshot.Available = false
	snapshot.Reason = "expired"
	snapshot.PrimaryID = ""
	for i := range snapshot.Members {
		snapshot.Members[i].Ready = false
		snapshot.Members[i].Reason = "expired"
	}
}

func clone(snapshot v1.Snapshot) v1.Snapshot {
	snapshot.Connection.ServerCAPEM = append([]byte(nil), snapshot.Connection.ServerCAPEM...)
	members := make([]v1.Member, len(snapshot.Members))
	for i, member := range snapshot.Members {
		members[i] = member
		members[i].Endpoints = make(map[string]v1.Endpoint, len(member.Endpoints))
		for network, endpoint := range member.Endpoints {
			members[i].Endpoints[network] = endpoint
		}
	}
	snapshot.Members = members
	return snapshot
}

func routingDigest(snapshot v1.Snapshot) [sha256.Size]byte {
	// Only members are reordered and have ReplayLSN removed. Endpoint maps and
	// CA bytes are read-only; duplicating them adds no isolation here.
	snapshot.Members = append([]v1.Member{}, snapshot.Members...)
	snapshot.Revision = ""
	snapshot.ObservedAt = time.Time{}
	snapshot.ValidUntil = time.Time{}
	for i := range snapshot.Members {
		snapshot.Members[i].ReplayLSN = ""
	}
	sort.Slice(snapshot.Members, func(i, j int) bool {
		if snapshot.Members[i].ID != snapshot.Members[j].ID {
			return snapshot.Members[i].ID < snapshot.Members[j].ID
		}
		return snapshot.Members[i].Name < snapshot.Members[j].Name
	})
	// This schema contains only JSON primitives and zero timestamps, so encoding
	// cannot fail. encoding/json sorts map keys, providing a stable digest.
	encoded, _ := json.Marshal(snapshot)
	return sha256.Sum256(encoded)
}
