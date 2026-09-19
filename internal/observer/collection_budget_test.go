package observer

import (
	"context"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
	"testing"
	"time"
)

func TestCollectionBudgetKeepsCompletedPrimary(t *testing.T) {
	o, _, _ := fakeObserver(t)
	o.opts.ProbeTimeout = 20 * time.Millisecond
	o.opts.PollInterval = 10 * time.Millisecond
	o.opts.TTL = time.Second
	_, _, results := fixture()
	o.probe = func(ctx context.Context, p *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
		if p.Name == "db-1" {
			return results[0].status, nil
		}
		<-ctx.Done()
		return instanceStatus{}, ctx.Err()
	}
	for range 3 {
		if !o.observe(context.Background(), clusterKey{"test", "db"}) {
			t.Fatal("I/O budget exhaustion discarded verified primary")
		}
		got, _ := o.store.Get("test", "db")
		if !got.Available {
			t.Fatalf("primary unavailable: %s", got.Reason)
		}
		for _, m := range got.Members {
			if m.Name != "db-1" && m.Ready {
				t.Fatal("uncompleted standby became eligible")
			}
		}
	}
}

func TestCollectionParentCancellationNeverPublishes(t *testing.T) {
	o, _, _ := fakeObserver(t)
	before, _ := o.store.Get("test", "db")
	ctx, cancel := context.WithCancel(context.Background())
	_, _, results := fixture()
	o.probe = func(_ context.Context, _ *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
		cancel()
		return results[0].status, nil
	}
	if o.observe(ctx, clusterKey{"test", "db"}) {
		t.Fatal("canceled observation published")
	}
	after, _ := o.store.Get("test", "db")
	if after.Revision != before.Revision || !after.ObservedAt.Equal(before.ObservedAt) {
		t.Fatal("parent cancellation renewed topology")
	}
}
