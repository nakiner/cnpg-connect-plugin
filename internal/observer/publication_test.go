package observer

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

type blockedLogHandler struct {
	blocked atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (*blockedLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *blockedLogHandler) Handle(context.Context, slog.Record) error {
	if h.blocked.CompareAndSwap(false, true) {
		close(h.entered)
		<-h.release
	}
	return nil
}
func (h *blockedLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *blockedLogHandler) WithGroup(string) slog.Handler      { return h }

func TestMetadataInvalidationDoesNotWaitForPublicationLog(t *testing.T) {
	for _, event := range []string{"cluster", "pod", "CA", "deletion"} {
		t.Run(event, func(t *testing.T) {
			o, _, _ := fakeObserver(t)
			cluster, _ := o.cluster(clusterKey{"test", "db"})
			other := cluster.DeepCopy()
			other.SetName("other")
			other.SetUID("other-uid")
			_ = o.clusterInformer.GetIndexer().Add(other)
			o.clusterEvent(nil, other)
			updates, unsubscribe := o.store.Subscribe("test", "db")
			defer unsubscribe()
			<-updates
			log := &blockedLogHandler{entered: make(chan struct{}), release: make(chan struct{})}
			o.log = slog.New(log)
			observationDone := make(chan struct{})
			var otherDone, invalidationDone chan struct{}
			go func() { observe(t, o); close(observationDone) }()
			defer func() {
				close(log.release)
				<-observationDone
				if otherDone != nil {
					<-otherDone
				}
				if invalidationDone != nil {
					<-invalidationDone
				}
			}()
			select {
			case <-log.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("observation did not reach its log write")
			}
			if initial := <-updates; !initial.Available {
				t.Fatal("initial observation was unavailable")
			}
			otherDone = make(chan struct{})
			go func() {
				o.publishFailure(context.Background(), clusterKey{"test", "other"}, "probe_failed", time.Now())
				close(otherDone)
			}()
			select {
			case <-otherDone:
			case <-time.After(3 * time.Second):
				t.Fatal("unrelated database publication waited for a log write")
			}
			if current, _ := o.store.Get("test", "other"); current.Reason != "probe_failed" {
				t.Fatal("unrelated database publication did not commit")
			}

			invalidationDone = make(chan struct{})
			go func() {
				defer close(invalidationDone)
				c, _ := o.cluster(clusterKey{"test", "db"})
				switch event {
				case "cluster":
					next := c.DeepCopy()
					_ = unstructured.SetNestedField(next.Object, "db-2", "status", "targetPrimary")
					_ = o.clusterInformer.GetIndexer().Update(next)
					o.clusterEvent(c, next)
				case "pod":
					old, _, _ := o.podInformer.GetIndexer().GetByKey("test/db-1")
					next := old.(*corev1.Pod).DeepCopy()
					next.Status.PodIP = "10.0.0.99"
					_ = o.podInformer.GetIndexer().Update(next)
					o.podEvent(old, next)
				case "CA":
					next := o.secretMetadata(types.NamespacedName{Namespace: "test", Name: "db-ca"}).DeepCopy()
					next.ResourceVersion = "2"
					_ = o.secretInformer.GetIndexer().Update(next)
					o.secretEvent(next)
				case "deletion":
					_ = o.clusterInformer.GetIndexer().Delete(c)
					o.clusterDeleted(c)
				}
			}()
			select {
			case <-invalidationDone:
			case <-time.After(3 * time.Second):
				t.Fatal("metadata event waited for an observation's log write")
			}
			select {
			case withdrawn := <-updates:
				if withdrawn.Available || withdrawn.PrimaryID != "" {
					t.Fatalf("metadata invalidation retained routing: %+v", withdrawn)
				}
			default:
				t.Fatal("metadata invalidation was not delivered")
			}
			if current, exists := o.store.Get("test", "db"); exists && current.Available {
				t.Fatal("store retained invalidated routing")
			}
		})
	}
}

func TestSharedCARotationDeliversEveryWithdrawalBeforeLogging(t *testing.T) {
	o, _, _ := fakeObserver(t)
	observe(t, o)
	cluster, _ := o.cluster(clusterKey{"test", "db"})
	snapshot, _ := o.store.Get("test", "db")
	updates := make(map[string]<-chan v1.Snapshot)
	for _, name := range []string{"db", "second", "third"} {
		if name != "db" {
			other := cluster.DeepCopy()
			other.SetName(name)
			other.SetUID(types.UID(name + "-uid"))
			_ = o.clusterInformer.GetIndexer().Add(other)
			snapshot.Cluster.Name = name
			snapshot.Cluster.UID = string(other.GetUID())
			o.store.Put(snapshot)
		}
		stream, cancel := o.store.Subscribe("test", name)
		defer cancel()
		<-stream
		updates[name] = stream
	}
	log := &blockedLogHandler{entered: make(chan struct{}), release: make(chan struct{})}
	o.log = slog.New(log)
	next := o.secretMetadata(types.NamespacedName{Namespace: "test", Name: "db-ca"}).DeepCopy()
	next.ResourceVersion = "2"
	_ = o.secretInformer.GetIndexer().Update(next)
	done := make(chan struct{})
	go func() { o.secretEvent(next); close(done) }()
	defer func() { close(log.release); <-done }()
	select {
	case <-log.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("CA rotation did not reach its first log write")
	}
	// All affected clients must be notified even while the first cluster's log
	// is blocked; committing every store record alone does not wake subscribers.
	for name, stream := range updates {
		select {
		case withdrawn := <-stream:
			if withdrawn.Available || withdrawn.PrimaryID != "" || withdrawn.Reason != "connection_defaults_changed" {
				t.Fatalf("%s retained routing after CA rotation: %+v", name, withdrawn)
			}
		default:
			t.Fatalf("%s withdrawal is waiting behind another cluster's log write", name)
		}
	}
}
