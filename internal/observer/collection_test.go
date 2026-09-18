package observer

import (
	"context"
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
