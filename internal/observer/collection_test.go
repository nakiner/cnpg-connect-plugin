package observer

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestMatchInstanceResultsRejectsChangedMembers(t *testing.T) {
	_, pods, results := fixture()
	probeFailure := errors.New("standby unreachable")
	results[1].err = probeFailure

	replaced := *pods[0].DeepCopy()
	replaced.UID = "replacement"
	newMember := *pods[0].DeepCopy()
	newMember.Name = "new-member"
	moved := *pods[0].DeepCopy()
	moved.Status.PodIP = "10.0.0.99"
	current := []corev1.Pod{pods[1], replaced, newMember, moved, pods[0]}
	mapped := matchInstanceResults(pods, current, results)
	if !errors.Is(mapped[0].err, probeFailure) {
		t.Fatalf("reordered member lost its probe failure: %v", mapped[0].err)
	}
	for _, index := range []int{1, 2, 3} {
		if mapped[index].err == nil {
			t.Fatalf("changed member %d reused an obsolete result", index)
		}
	}
	if mapped[4].err != nil || mapped[4].status.IsPrimary != results[0].status.IsPrimary {
		t.Fatal("unchanged primary lost its verified result")
	}
}

func TestObservationDoesNotRestoreDisabledCluster(t *testing.T) {
	for _, mode := range []string{"annotation", "plugin", "deletion timestamp", "deleted"} {
		t.Run(mode, func(t *testing.T) {
			o, _, _ := fakeObserver(t)
			old, _ := o.cluster(clusterKey{"test", "db"})
			next := old.DeepCopy()
			switch mode {
			case "annotation":
				next.SetAnnotations(map[string]string{config.EnabledAnnotation: "false"})
			case "plugin":
				_ = unstructured.SetNestedSlice(next.Object, []any{map[string]any{"name": config.PluginName, "enabled": false}}, "spec", "plugins")
			case "deletion timestamp":
				now := metav1.Now()
				next.SetDeletionTimestamp(&now)
			}
			var change sync.Once
			probe := o.probe
			o.probe = func(ctx context.Context, pod *corev1.Pod, connection v1.ConnectionParameters, serverName string) (instanceStatus, error) {
				change.Do(func() {
					if mode == "deleted" {
						_ = o.clusterInformer.GetIndexer().Delete(next)
						o.clusterDeleted(next)
						return
					}
					_ = o.clusterInformer.GetIndexer().Update(next)
					o.clusterEvent(old, next)
				})
				return probe(ctx, pod, connection, serverName)
			}
			observe(t, o)
			if _, exists := o.store.Get("test", "db"); exists {
				t.Fatal("in-flight status collection restored a removed cluster")
			}
		})
	}
}

func TestLargeProbeTimeoutDoesNotExpireObservationImmediately(t *testing.T) {
	o, _, _ := fakeObserver(t)
	const maximumDuration = time.Duration(1<<63 - 1)
	o.opts.TTL = maximumDuration
	o.opts.PollInterval = time.Second
	o.opts.ProbeTimeout = maximumDuration/2 + 1
	observe(t, o)
	snapshot, exists := o.store.Get("test", "db")
	if !exists || !snapshot.Available {
		t.Fatal("doubling a large probe timeout overflowed the observation deadline")
	}
}

func TestNewRejectsOverflowingRefreshBudget(t *testing.T) {
	o, _, _ := fakeObserver(t)
	const maximumDuration = time.Duration(1<<63 - 1)
	options := Options{
		PollInterval:   maximumDuration - time.Second,
		ProbeTimeout:   2 * time.Second,
		TTL:            maximumDuration,
		MaxConcurrency: 1,
	}
	observer, err := New(o.kube, o.dynamic, o.store, options, nil)
	if observer != nil {
		observer.queue.ShutDown()
	}
	if err == nil {
		t.Fatal("accepted a refresh budget larger than the maximum duration")
	}
}

func TestFailedPrimaryCancelsSlowStandbysAndPublishes(t *testing.T) {
	for _, mode := range []string{"request failed", "wrong role", "unavailable", "rewinding", "certificate changed"} {
		t.Run(mode, func(t *testing.T) {
			o, _, _ := fakeObserver(t)
			observe(t, o) // Start with a verified, routable primary.
			o.opts.ProbeTimeout = 5 * time.Second
			o.opts.MaxConcurrency = 4 // Primary and standbys run concurrently.
			o.configureProbeLimits()
			_, _, fixtureResults := fixture()
			standbyStarted := make(chan struct{})
			var once sync.Once
			o.probe = func(ctx context.Context, pod *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
				if pod.Name != "db-1" {
					once.Do(func() { close(standbyStarted) })
					<-ctx.Done()
					return instanceStatus{}, ctx.Err()
				}
				select {
				case <-standbyStarted:
				case <-ctx.Done():
					return instanceStatus{}, ctx.Err()
				}
				status := fixtureResults[0].status
				switch mode {
				case "request failed":
					return instanceStatus{}, errors.New("primary unreachable")
				case "wrong role":
					primary := false
					status.IsPrimary = &primary
				case "unavailable":
					status.Unavailable = true
				case "rewinding":
					status.Rewinding = true
				case "certificate changed":
					return instanceStatus{}, &tls.CertificateVerificationError{Err: errors.New("CA rotated")}
				}
				return status, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan bool, 1)
			go func() { done <- o.observe(ctx, clusterKey{"test", "db"}) }()
			defer cancel()
			select {
			case available := <-done:
				if available {
					t.Fatal("failed primary remained available")
				}
			case <-time.After(time.Second):
				cancel()
				<-done
				t.Fatal("failed primary waited for the standby timeout")
			}
			snapshot, exists := o.store.Get("test", "db")
			if !exists || snapshot.Available {
				t.Fatal("probe cancellation prevented failure publication")
			}
			if len(o.probeSlots) != 0 || len(o.backgroundProbeSlots) != 0 {
				t.Fatal("canceled probes retained worker capacity")
			}
			if mode == "certificate changed" {
				if _, cached := o.connections.Load(clusterKey{"test", "db"}); cached {
					t.Fatal("certificate failure did not invalidate the cached CA")
				}
			}
		})
	}
}

func TestHealthyPrimaryStillVerifiesConflictingPrimary(t *testing.T) {
	o, _, _ := fakeObserver(t)
	_, _, results := fixture()
	primaryFinished := make(chan struct{})
	o.probe = func(ctx context.Context, pod *corev1.Pod, _ v1.ConnectionParameters, _ string) (instanceStatus, error) {
		if pod.Name == "db-1" {
			close(primaryFinished)
			return results[0].status, nil
		}
		select {
		case <-primaryFinished:
		case <-ctx.Done():
			return instanceStatus{}, ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return instanceStatus{}, err
		}
		// A second member also claims to be primary. Successful verification of
		// the expected primary must never skip this conflicting observation.
		if pod.Name == "db-2" {
			return results[0].status, nil
		}
		return results[2].status, nil
	}
	observe(t, o)
	snapshot, exists := o.store.Get("test", "db")
	if !exists || snapshot.Available || snapshot.Reason != "multiple_primaries_observed" {
		t.Fatalf("conflicting primary was not checked: %+v", snapshot)
	}
}
