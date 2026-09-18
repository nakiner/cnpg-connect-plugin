package discovery

import (
	"reflect"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
)

func TestStoreDemandTracksFirstAndLastWatcherPerCluster(t *testing.T) {
	s := NewStore()
	first := sampleSnapshot()
	second := sampleSnapshot()
	second.Cluster.Namespace = "other"
	second.Cluster.UID = "other-uid"
	s.Put(first)
	s.Put(second)
	requests := make(map[clusterKey]int)
	s.SetDemandHandler(func(namespace, name string) {
		requests[clusterKey{namespace, name}]++
	}, time.Minute)

	_, cancelA := s.Subscribe("database", "postgres")
	defer cancelA()
	_, cancelB := s.Subscribe("database", "postgres")
	defer cancelB()
	_, cancelOther := s.Subscribe("other", "postgres")
	defer cancelOther()
	if requests[clusterKey{"database", "postgres"}] != 1 || requests[clusterKey{"other", "postgres"}] != 1 {
		t.Fatalf("first watchers did not trigger exactly one independent request: %v", requests)
	}
	if got := s.ActiveClusters(); !reflect.DeepEqual(got, []v1.ClusterRef{first.Cluster, second.Cluster}) {
		t.Fatalf("active cluster references: %v", got)
	}
	cancelA()
	if !s.HasDemand("database", "postgres") {
		t.Fatal("canceling one watcher discarded another watcher's interest")
	}
	cancelB()
	cancelB()
	if s.HasDemand("database", "postgres") || !s.HasDemand("other", "postgres") {
		t.Fatal("last-watcher cancellation did not remove only its cluster's demand")
	}
	if got := s.ActiveClusters(); !reflect.DeepEqual(got, []v1.ClusterRef{second.Cluster}) {
		t.Fatalf("inactive cluster remained active: %v", got)
	}
	_, cancelAgain := s.Subscribe("database", "postgres")
	defer cancelAgain()
	if requests[clusterKey{"database", "postgres"}] != 2 {
		t.Fatal("a new first watcher did not restart observations")
	}
}

func TestStoreUnknownRefreshAndPassiveGetDoNotCreateDemand(t *testing.T) {
	s := NewStore()
	called := false
	s.SetDemandHandler(func(string, string) { called = true }, time.Minute)
	if s.RequestRefresh("missing", "postgres") {
		t.Fatal("unknown cluster accepted a refresh request")
	}
	if _, _, found := s.subscribe("missing", "postgres", true); found {
		t.Fatal("required-existing subscription accepted an unknown cluster")
	}
	s.Put(sampleSnapshot())
	if _, found := s.Get("database", "postgres"); !found {
		t.Fatal("known cluster disappeared")
	}
	if called || len(s.unaryDemand) != 0 || len(s.subscribers) != 0 || len(s.ActiveClusters()) != 0 {
		t.Fatal("unknown requests or passive reads allocated demand or triggered refresh")
	}
}

func TestStoreDemandHandlerCanReadStore(t *testing.T) {
	for _, operation := range []string{"subscribe", "request"} {
		t.Run(operation, func(t *testing.T) {
			s := NewStore()
			s.Put(sampleSnapshot())
			called := make(chan bool, 1)
			s.SetDemandHandler(func(namespace, name string) {
				_, found := s.Get(namespace, name)
				called <- found && s.HasDemand(namespace, name)
			}, time.Minute)
			done := make(chan func(), 1)
			go func() {
				if operation == "subscribe" {
					_, cancel := s.Subscribe("database", "postgres")
					done <- cancel
					return
				}
				s.RequestRefresh("database", "postgres")
				done <- func() {}
			}()
			select {
			case cancel := <-done:
				cancel()
			case <-time.After(time.Second):
				t.Fatal("demand callback deadlocked while reading the store")
			}
			if !<-called {
				t.Fatal("callback ran before consumer interest was recorded")
			}
		})
	}
}

func TestStoreUnaryDemandRenewsAndExpires(t *testing.T) {
	s := NewStore()
	s.SetDemandHandler(nil, time.Minute)
	s.Put(sampleSnapshot())
	key := clusterKey{"database", "postgres"}
	if !s.RequestRefresh(key.namespace, key.name) || !s.HasDemand(key.namespace, key.name) {
		t.Fatal("known unary consumer did not acquire a lease")
	}
	s.mu.Lock()
	s.unaryDemand[key] = time.Now().Add(-time.Second)
	s.mu.Unlock()
	if s.HasDemand(key.namespace, key.name) || len(s.ActiveClusters()) != 0 {
		t.Fatal("expired unary lease still schedules observations")
	}
	if !s.RequestRefresh(key.namespace, key.name) || len(s.ActiveClusters()) != 1 {
		t.Fatal("a subsequent unary call did not restart observations")
	}
	s.mu.Lock()
	previous := s.unaryDemand[key].Add(-time.Second)
	s.unaryDemand[key] = previous
	s.mu.Unlock()
	s.RequestRefresh(key.namespace, key.name)
	s.mu.Lock()
	renewed := s.unaryDemand[key]
	s.mu.Unlock()
	if !renewed.After(previous) {
		t.Fatal("a subsequent unary call did not renew its lease")
	}
	_, cancel := s.Subscribe(key.namespace, key.name)
	defer cancel()
	s.Expire(renewed)
	if !s.HasDemand(key.namespace, key.name) {
		t.Fatal("unary lease expiry removed an active watcher")
	}
	cancel()
	if s.HasDemand(key.namespace, key.name) {
		t.Fatal("expired unary lease survived last-watcher cancellation")
	}
}

func TestStoreDeletionDropsUnaryDemandAndPreservesWatchers(t *testing.T) {
	s := NewStore()
	s.SetDemandHandler(nil, time.Minute)
	input := sampleSnapshot()
	s.Put(input)
	s.RequestRefresh("database", "postgres")
	updates, cancel := s.Subscribe("database", "postgres")
	defer cancel()
	s.Delete("database", "postgres")
	if got := latest(t, updates); got.Reason != "deleted" {
		t.Fatal("watcher did not receive the tombstone")
	}
	if len(s.unaryDemand) != 0 || !s.HasDemand("database", "postgres") || len(s.ActiveClusters()) != 0 {
		t.Fatal("deletion retained a unary lease, lost a watcher, or scheduled a missing record")
	}
	input.Cluster.UID = "replacement"
	s.Put(input)
	if got := s.ActiveClusters(); !reflect.DeepEqual(got, []v1.ClusterRef{input.Cluster}) {
		t.Fatalf("recreated cluster did not retain its existing watcher: %v", got)
	}
	cancel()
	if s.HasDemand("database", "postgres") {
		t.Fatal("old unary lease leaked into the recreated cluster")
	}
}

func TestThousandsOfUnaryConsumersShareRefreshSchedule(t *testing.T) {
	s := NewStore()
	s.Put(sampleSnapshot())
	requests := 0
	s.SetDemandHandler(func(string, string) { requests++ }, time.Minute)
	for range 20000 {
		if !s.RequestRefresh("database", "postgres") {
			t.Fatal("lost known cluster")
		}
	}
	if requests != 1 {
		t.Fatalf("unary consumers multiplied observations: %d", requests)
	}
}
