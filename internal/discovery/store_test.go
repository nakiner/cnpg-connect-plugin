package discovery

import (
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
)

func sampleSnapshot() v1.Snapshot {
	now := time.Now().UTC()
	return v1.Snapshot{
		APIVersion: v1.APIVersion,
		Cluster:    v1.ClusterRef{Namespace: "database", Name: "postgres", UID: "cluster-uid"},
		ObservedAt: now, ValidUntil: now.Add(time.Minute), Available: true, PrimaryID: "pod-1",
		Members: []v1.Member{
			{ID: "pod-1", Name: "postgres-1", Ready: true, Role: "primary", SyncState: "unknown", Timeline: 3,
				Endpoints: map[string]v1.Endpoint{"internal": {Host: "10.1.1.1", Port: 5432}}},
			{ID: "pod-2", Name: "postgres-2", Ready: true, Role: "standby", SyncState: "sync", Timeline: 3, ReplayLSN: "0/10",
				Endpoints: map[string]v1.Endpoint{"internal": {Host: "10.1.1.2", Port: 5432}}},
		},
	}
}

func latest(t *testing.T, updates <-chan v1.Snapshot) v1.Snapshot {
	t.Helper()
	select {
	case snapshot, ok := <-updates:
		if !ok {
			t.Fatal("subscription closed unexpectedly")
		}
		return snapshot
	case <-time.After(time.Second):
		t.Fatal("no snapshot delivered")
		return v1.Snapshot{}
	}
}

func TestStoreRefreshPreservesRevisionAndDeliversFreshness(t *testing.T) {
	s := NewStore()
	input := sampleSnapshot()
	s.Put(input)
	updates, cancel := s.Subscribe("database", "postgres")
	defer cancel()
	initial := latest(t, updates)
	input.ObservedAt = input.ObservedAt.Add(time.Second)
	input.ValidUntil = input.ValidUntil.Add(time.Second)
	input.Members[1].ReplayLSN = "0/20"
	input.Members[0], input.Members[1] = input.Members[1], input.Members[0]
	s.Put(input)
	refreshed := latest(t, updates)
	if initial.Revision != refreshed.Revision {
		t.Fatal("freshness, WAL progress, or ordering changed routing revision")
	}
	if !refreshed.ValidUntil.Equal(input.ValidUntil) {
		t.Fatal("refresh did not deliver renewed freshness")
	}
	if refreshed.Members[0].ReplayLSN != "0/20" {
		t.Fatal("refresh lost WAL observation")
	}
	other := NewStore()
	other.Put(input)
	fromOther, _ := other.Get("database", "postgres")
	if fromOther.Revision == initial.Revision {
		t.Fatal("revision reused across process/store restart")
	}
}

func TestStoreRoutingChangesBumpRevision(t *testing.T) {
	cases := map[string]func(*v1.Snapshot){
		"pod replacement":     func(s *v1.Snapshot) { s.Members[1].ID = "replacement" },
		"cluster replacement": func(s *v1.Snapshot) { s.Cluster.UID = "replacement" },
		"address":             func(s *v1.Snapshot) { s.Members[0].Endpoints["internal"] = v1.Endpoint{Host: "new", Port: 5432} },
		"readiness":           func(s *v1.Snapshot) { s.Members[0].Ready = false },
		"role":                func(s *v1.Snapshot) { s.Members[0].Role = "standby" },
		"sync":                func(s *v1.Snapshot) { s.Members[1].SyncState = "async" },
		"timeline":            func(s *v1.Snapshot) { s.Members[0].Timeline++ },
		"primary":             func(s *v1.Snapshot) { s.PrimaryID = "pod-2" },
		"transition":          func(s *v1.Snapshot) { s.Transitioning = true },
		"availability":        func(s *v1.Snapshot) { s.Available = false },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			input := sampleSnapshot()
			s.Put(input)
			before, _ := s.Get("database", "postgres")
			change(&input)
			input.Revision = before.Revision
			s.Put(input)
			after, _ := s.Get("database", "postgres")
			if before.Revision == after.Revision {
				t.Fatal("routing change kept revision")
			}
		})
	}
}

func TestStoreExpiryInvalidatesOnceWithoutExtendingFreshness(t *testing.T) {
	s := NewStore()
	input := sampleSnapshot()
	s.Put(input)
	updates, cancel := s.Subscribe("database", "postgres")
	defer cancel()
	before := latest(t, updates)
	s.Expire(input.ValidUntil)
	after := latest(t, updates)
	assertExpired(t, after)
	if before.Revision == after.Revision {
		t.Fatal("expiry kept revision")
	}
	if !before.ObservedAt.Equal(after.ObservedAt) || !before.ValidUntil.Equal(after.ValidUntil) {
		t.Fatal("expiry extended freshness")
	}
	s.Expire(input.ValidUntil.Add(time.Second))
	select {
	case <-updates:
		t.Fatal("repeated expiry emitted redundant state")
	default:
	}
	input.ObservedAt = input.ObservedAt.Add(time.Second)
	input.ValidUntil = input.ValidUntil.Add(time.Minute)
	s.Put(input)
	recovered := latest(t, updates)
	if !recovered.Available || recovered.Revision == after.Revision {
		t.Fatal("fresh observation did not restore routing with new revision")
	}
}

func TestStoreReadAndSubscribeFailClosedWithoutExpiryTimer(t *testing.T) {
	for _, read := range []string{"get", "subscribe"} {
		t.Run(read, func(t *testing.T) {
			s := NewStore()
			s.Put(sampleSnapshot())
			// Advance the record's deadline without sleeping or running Expire.
			s.mu.Lock()
			s.records[clusterKey{"database", "postgres"}].snapshot.ValidUntil = time.Now().Add(-time.Second)
			s.mu.Unlock()
			var expired v1.Snapshot
			if read == "get" {
				expired, _ = s.Get("database", "postgres")
			} else {
				updates, cancel := s.Subscribe("database", "postgres")
				defer cancel()
				expired = latest(t, updates)
			}
			assertExpired(t, expired)
		})
	}
	s := NewStore()
	input := sampleSnapshot()
	input.ValidUntil = time.Now().Add(-time.Second)
	s.Put(input)
	got, _ := s.Get("database", "postgres")
	assertExpired(t, got)
}

func assertExpired(t *testing.T, snapshot v1.Snapshot) {
	t.Helper()
	if snapshot.Available || snapshot.PrimaryID != "" || snapshot.Reason != "expired" {
		t.Fatalf("unsafe expired snapshot: %+v", snapshot)
	}
	for _, member := range snapshot.Members {
		if member.Ready {
			t.Fatal("expired member remains routable")
		}
	}
}

func TestStoreDeletionAndRecreation(t *testing.T) {
	s := NewStore()
	input := sampleSnapshot()
	s.Put(input)
	updates, cancel := s.Subscribe("database", "postgres")
	defer cancel()
	before := latest(t, updates)
	s.Delete("database", "postgres")
	deleted := latest(t, updates)
	if deleted.Available || deleted.PrimaryID != "" || len(deleted.Members) != 0 || deleted.Reason != "deleted" {
		t.Fatal("unsafe deletion tombstone")
	}
	if deleted.Revision == before.Revision {
		t.Fatal("deletion kept revision")
	}
	if _, exists := s.Get("database", "postgres"); exists {
		t.Fatal("deleted cluster is still discoverable")
	}
	input.Cluster.UID = "new-cluster"
	s.Put(input)
	recreated := latest(t, updates)
	if recreated.Cluster.UID != "new-cluster" || recreated.Revision == before.Revision {
		t.Fatal("watch did not observe recreation")
	}
}

func TestStoreCopiesInputsOutputsAndEachDelivery(t *testing.T) {
	s := NewStore()
	input := sampleSnapshot()
	s.Put(input)
	a, cancelA := s.Subscribe("database", "postgres")
	defer cancelA()
	b, cancelB := s.Subscribe("database", "postgres")
	defer cancelB()
	input.Members[0].Name = "modified"
	input.Members[0].Endpoints["internal"] = v1.Endpoint{Host: "modified"}
	first := latest(t, a)
	first.Members[0].Name = "modified-again"
	first.Members[0].Endpoints["internal"] = v1.Endpoint{Host: "modified-again"}
	second := latest(t, b)
	got, _ := s.Get("database", "postgres")
	for _, snapshot := range []v1.Snapshot{second, got} {
		if snapshot.Members[0].Name != "postgres-1" || snapshot.Members[0].Endpoints["internal"].Host != "10.1.1.1" {
			t.Fatal("snapshot ownership leaked")
		}
	}
	got.Members[0].Endpoints["internal"] = v1.Endpoint{Host: "changed-get"}
	gotAgain, _ := s.Get("database", "postgres")
	if gotAgain.Members[0].Endpoints["internal"].Host != "10.1.1.1" {
		t.Fatal("Get leaked stored endpoint map")
	}
}

func TestStoreSlowConsumerGetsLatest(t *testing.T) {
	s := NewStore()
	updates, cancel := s.Subscribe("database", "postgres")
	defer cancel()
	for i := 0; i < 1000; i++ {
		input := sampleSnapshot()
		input.PrimaryID = fmt.Sprint(i)
		s.Put(input)
	}
	if got := latest(t, updates); got.PrimaryID != "999" {
		t.Fatalf("slow consumer received %q", got.PrimaryID)
	}
	select {
	case <-updates:
		t.Fatal("pending queue did not coalesce")
	default:
	}
	cancel()
	cancel()
	if _, open := <-updates; open {
		t.Fatal("cancel did not close subscription")
	}
}

func TestStoreConcurrentSubscribeAndPutHasNoGap(t *testing.T) {
	for i := 0; i < 200; i++ {
		s := NewStore()
		s.Put(sampleSnapshot())
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var updates <-chan v1.Snapshot
		var cancel func()
		go func() { defer wg.Done(); <-start; updates, cancel = s.Subscribe("database", "postgres") }()
		go func() {
			defer wg.Done()
			<-start
			input := sampleSnapshot()
			input.PrimaryID = "new-primary"
			s.Put(input)
		}()
		close(start)
		wg.Wait()
		if got := latest(t, updates); got.PrimaryID != "new-primary" {
			t.Fatalf("lost concurrent update: %q", got.PrimaryID)
		}
		cancel()
	}
}
