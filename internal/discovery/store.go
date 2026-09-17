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
		processID:   hex.EncodeToString(id[:]),
	}
}

// Put publishes an observation, including refreshes with unchanged routing.
// Revision is assigned by the store and cannot be supplied by a producer.
// Freshness timestamps must come from the actual observation, never a heartbeat.
func (s *Store) Put(snapshot v1.Snapshot) {
	snapshot = clone(snapshot)
	snapshot.APIVersion = v1.APIVersion
	key := clusterKey{snapshot.Cluster.Namespace, snapshot.Cluster.Name}
	s.mu.Lock()
	defer s.mu.Unlock()

	expired := !time.Now().Before(snapshot.ValidUntil)
	if expired {
		invalidate(&snapshot)
	}
	routing := routingDigest(snapshot)
	previous, exists := s.records[key]
	if exists && previous.routing == routing {
		snapshot.Revision = previous.snapshot.Revision
	} else {
		snapshot.Revision = s.nextRevision()
	}
	s.records[key] = &record{snapshot: snapshot, routing: routing, expired: expired}
	s.broadcast(key, snapshot)
}

// Delete removes the current record and publishes a tombstone to watchers.
// Watchers stay subscribed so recreation under the same name is observable.
func (s *Store) Delete(namespace, name string) {
	key := clusterKey{namespace, name}
	s.mu.Lock()
	defer s.mu.Unlock()
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

// Get fails closed on stale routing even when the periodic expiry task is late.
func (s *Store) Get(namespace, name string) (v1.Snapshot, bool) {
	key := clusterKey{namespace, name}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[key]
	if !exists {
		return v1.Snapshot{}, false
	}
	s.expireRecord(key, record, time.Now())
	return clone(record.snapshot), true
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
	if s.subscribers[key] == nil {
		s.subscribers[key] = make(map[chan v1.Snapshot]struct{})
	}
	s.subscribers[key][updates] = struct{}{}
	s.mu.Unlock()

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
}

// All helpers below that operate on store state require s.mu.
func (s *Store) expireRecord(key clusterKey, current *record, now time.Time) {
	if current.expired || now.Before(current.snapshot.ValidUntil) {
		return
	}
	invalidate(&current.snapshot)
	current.snapshot.Revision = s.nextRevision()
	current.routing = routingDigest(current.snapshot)
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
	snapshot = clone(snapshot)
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
