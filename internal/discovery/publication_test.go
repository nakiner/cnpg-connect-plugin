package discovery

import (
	"context"
	"sync"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
)

func TestPublicationFanoutSharesOnlyImmutablePayloads(t *testing.T) {
	store := NewStore()
	input := sampleSnapshot()
	input.Connection.ServerCAPEM = []byte("CA")
	store.Put(input)
	a, cancelA, _ := store.subscribeShared("database", "postgres")
	defer cancelA()
	b, cancelB, _ := store.subscribeShared("database", "postgres")
	defer cancelB()
	owned, cancelOwned := store.Subscribe("database", "postgres")
	defer cancelOwned()
	first, second := <-a, <-b
	if first != second {
		t.Fatal("one observation was copied for gRPC watchers")
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if first.protobuf() != second.protobuf() {
				t.Error("protobuf conversion was repeated for the same observation")
			}
		})
	}
	wg.Wait()
	copy := <-owned
	copy.Members[0].Endpoints["internal"] = v1.Endpoint{Host: "changed"}
	copy.Connection.ServerCAPEM[0] = 'X'
	input.Connection.ServerCAPEM[0] = 'Y'
	if first.protobuf().Connection.ServerCaPem[0] != 'C' || first.protobuf().Members[0].Endpoints["internal"].Host != "10.1.1.1" {
		t.Fatal("caller mutation reached shared wire data")
	}
	input.PrimaryID = "promoted"
	store.Put(input)
	newPublication := <-a
	if newPublication == first || newPublication.protobuf().PrimaryId != "promoted" || first.protobuf().PrimaryId != "pod-1" {
		t.Fatal("new publication changed an existing immutable payload")
	}
}

func TestSubscriptionRejectsDelayedOlderPublication(t *testing.T) {
	watcher := &subscription{publications: make(chan *publication, 1)}
	newer := &publication{snapshot: sampleSnapshot(), order: 2}
	watcher.deliver(newer)
	watcher.deliver(&publication{snapshot: sampleSnapshot(), order: 1})
	if got := <-watcher.publications; got != newer {
		t.Fatal("a delayed broadcast replaced a newer observation")
	}
	watcher.close()
	watcher.deliver(&publication{snapshot: sampleSnapshot(), order: 3})
	if _, open := <-watcher.publications; open {
		t.Fatal("closed subscriber accepted another publication")
	}
}

func TestDeferredDeliveryCannotRewindNewerCommit(t *testing.T) {
	for _, operation := range []string{"put", "expiry", "delete"} {
		t.Run(operation, func(t *testing.T) {
			store := NewStore()
			store.Put(sampleSnapshot())
			updates, cancel := store.Subscribe("database", "postgres")
			defer cancel()
			<-updates
			var deliver func()
			switch operation {
			case "put":
				input := sampleSnapshot()
				input.PrimaryID = "intermediate"
				_, deliver = store.PutDeferred(input)
				if got, _ := store.Get("database", "postgres"); got.PrimaryID != "intermediate" {
					t.Fatal("deferred put did not commit before delivery")
				}
			case "expiry":
				store.mu.Lock()
				store.records[clusterKey{"database", "postgres"}].snapshot.ValidUntil = time.Now().Add(-time.Second)
				store.mu.Unlock()
				var expired v1.Snapshot
				expired, _, deliver = store.GetDeferred("database", "postgres")
				assertExpired(t, expired)
			case "delete":
				deliver = store.DeleteDeferred("database", "postgres")
				if _, exists := store.Get("database", "postgres"); exists {
					t.Fatal("deferred delete retained its record")
				}
			}
			select {
			case <-updates:
				t.Fatal("commit delivered while its caller still held the producer lock")
			default:
			}
			newest := sampleSnapshot()
			newest.PrimaryID = "newest"
			store.Put(newest)
			deliver()
			if got := latest(t, updates); got.PrimaryID != "newest" {
				t.Fatalf("delayed %s rewound routing to %+v", operation, got)
			}
			deliver() // Repeated delivery is harmless too.
			select {
			case <-updates:
				t.Fatal("older commit was redelivered after the latest snapshot")
			default:
			}
		})
	}
}

func TestDeliveryDoesNotHoldGlobalStoreLock(t *testing.T) {
	store := NewStore()
	input := sampleSnapshot()
	store.Put(input)
	watcher := &subscription{snapshots: make(chan v1.Snapshot, 1)}
	cancel, _ := store.register("database", "postgres", true, watcher)
	defer cancel()
	watcher.mu.Lock()
	done := make(chan struct{})
	go func() {
		input.PrimaryID = "promoted"
		store.Put(input)
		close(done)
	}()
	// Once the new record is committed, Put is blocked on this subscriber's
	// mailbox. Reads and writes of another database must still complete.
	deadline := time.Now().Add(time.Second)
	for {
		store.mu.Lock()
		committed := store.records[clusterKey{"database", "postgres"}].snapshot.PrimaryID == "promoted"
		store.mu.Unlock()
		if committed {
			break
		}
		if time.Now().After(deadline) {
			watcher.mu.Unlock()
			t.Fatal("publication did not commit")
		}
		time.Sleep(time.Millisecond)
	}
	otherDone := make(chan struct{})
	go func() {
		other := sampleSnapshot()
		other.Cluster.Name = "unrelated"
		store.Put(other)
		_, _ = store.Get("database", "unrelated")
		close(otherDone)
	}()
	select {
	case <-otherDone:
	case <-time.After(time.Second):
		watcher.mu.Unlock()
		t.Fatal("subscriber blocked an unrelated database")
	}
	watcher.mu.Unlock()
	<-done
}

func TestConcurrentSharedSubscriptionsAndPublications(t *testing.T) {
	store := NewStore()
	store.Put(sampleSnapshot())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				updates, cancel, found := store.subscribeShared("database", "postgres")
				if !found {
					continue
				}
				if published := <-updates; published == nil || published.protobuf() == nil {
					t.Error("missing initial publication")
				}
				cancel()
				cancel()
			}
		})
	}
	for range 100 {
		store.Put(sampleSnapshot())
		store.Expire(time.Now().Add(time.Hour))
		store.Delete("database", "postgres")
	}
	store.Put(sampleSnapshot())
	wg.Wait()
	if store.HasDemand("database", "postgres") {
		t.Fatal("canceled clients leaked demand")
	}
}

type orderedTestStream struct {
	connectv1.TopologyService_WatchTopologyServer
	ctx      context.Context
	received chan string
	release  chan struct{}
}

func (s *orderedTestStream) Context() context.Context { return s.ctx }
func (s *orderedTestStream) Send(snapshot *connectv1.Snapshot) error {
	s.received <- snapshot.PrimaryId
	if snapshot.PrimaryId == "pod-1" || snapshot.PrimaryId == "latest" {
		select {
		case <-s.release:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	return nil
}

func TestWatchExpiryLookupCannotReversePublicationOrder(t *testing.T) {
	store := NewStore()
	input := sampleSnapshot()
	store.Put(input)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := &orderedTestStream{ctx: ctx, received: make(chan string, 10), release: make(chan struct{}, 2)}
	done := make(chan error, 1)
	receive := func() string {
		select {
		case primary := <-stream.received:
			return primary
		case <-ctx.Done():
			t.Fatal("watch stopped making progress")
			return ""
		}
	}
	go func() {
		done <- NewServer(store, "").WatchTopology(&connectv1.WatchTopologyRequest{Namespace: "database", Name: "postgres"}, stream)
	}()
	if got := receive(); got != "pod-1" {
		t.Fatalf("unexpected initial state %q", got)
	}
	// Expired observation 2 is pending behind the first blocked Send.
	expired := sampleSnapshot()
	expired.ValidUntil = time.Now().Add(-time.Second)
	store.Put(expired)
	store.mu.Lock()
	key := clusterKey{"database", "postgres"}
	var watcher *subscription
	for watcher = range store.subscribers[key] {
		break
	}
	// Observation 3 will be delivered to the mailbox while the expired
	// observation's Get waits on the store lock; the record is already at 4.
	olderSnapshot := sampleSnapshot()
	olderSnapshot.PrimaryID = "older"
	older := store.newPublication(olderSnapshot)
	newest := sampleSnapshot()
	newest.PrimaryID = "latest"
	store.records[key] = &record{publication: store.newPublication(newest), routing: routingDigest(newest)}
	stream.release <- struct{}{}
	deadline := time.Now().Add(time.Second)
	for len(watcher.publications) != 0 {
		if time.Now().After(deadline) {
			store.mu.Unlock()
			t.Fatal("watch never consumed pending expired publication")
		}
		time.Sleep(time.Millisecond)
	}
	watcher.deliver(older)
	store.mu.Unlock()
	if got := receive(); got != "latest" {
		t.Fatalf("expiry lookup did not read newest state: %q", got)
	}
	stream.release <- struct{}{}
	waitFor(t, func() bool { return len(watcher.publications) == 0 }, "watch did not drain superseded pending delivery")
	input.PrimaryID = "after-latest"
	store.Put(input)
	if got := receive(); got != "after-latest" {
		t.Fatalf("stream reversed after expiry lookup: %q", got)
	}
	cancel()
	<-done
}
