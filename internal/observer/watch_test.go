package observer

import (
	"context"
	"errors"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

type delayedStopWatch struct {
	*watch.RaceFreeFakeWatcher
	stopping chan struct{}
	release  chan struct{}
}

func (w *delayedStopWatch) Stop() {
	close(w.stopping)
	<-w.release
	w.RaceFreeFakeWatcher.Stop()
}

func TestRetiringWatchCannotInvalidateReplacement(t *testing.T) {
	for _, reason := range []string{"cleanup", "error"} {
		t.Run(reason, func(t *testing.T) { testWatchReplacement(t, reason) })
	}
}

func testWatchReplacement(t *testing.T, reason string) {
	o, _, _ := fakeObserver(t)
	observe(t, o)
	before, _ := o.store.Get("test", "db")
	source := &delayedStopWatch{watch.NewRaceFreeFake(), make(chan struct{}), make(chan struct{})}
	release := sync.OnceFunc(func() { close(source.release) })
	defer release()
	old, err := o.startWatch(context.Background(), metav1.ListOptions{}, &o.secretWatch,
		func(context.Context, metav1.ListOptions) (watch.Interface, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer old.Stop()
	if reason == "cleanup" {
		old.Stop()
		<-source.stopping
	}

	replacementSource := watch.NewRaceFreeFake()
	replacement, err := o.startWatch(context.Background(), metav1.ListOptions{}, &o.secretWatch,
		func(context.Context, metav1.ListOptions) (watch.Interface, error) { return replacementSource, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		replacement.Stop()
		for range replacement.ResultChan() {
		}
	}()
	if !o.Ready() {
		t.Fatal("replacement did not restore watch health")
	}
	if reason == "error" {
		source.Error(&metav1.Status{Message: "retired watch expired"})
		<-old.ResultChan()
		if !o.Ready() {
			t.Fatal("retired watch error invalidated its healthy replacement")
		}
		old.Stop()
		<-source.stopping
	}
	release()
	for range old.ResultChan() {
	}
	if !o.Ready() {
		t.Fatal("retiring watch invalidated its healthy replacement")
	}
	observe(t, o)
	after, _ := o.store.Get("test", "db")
	if !after.ValidUntil.After(before.ValidUntil) {
		t.Fatal("replacement watch did not allow topology renewal")
	}
}

func TestCanceledWatchStartDoesNotBecomeHealthy(t *testing.T) {
	o, _, _ := fakeObserver(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := watch.NewRaceFreeFake()
	tracked, err := o.startWatch(ctx, metav1.ListOptions{}, &o.secretWatch,
		func(context.Context, metav1.ListOptions) (watch.Interface, error) {
			if o.Ready() {
				t.Error("pending replacement retained old watch health")
			}
			cancel() // Request expires before the response headers arrive.
			return source, nil
		})
	if !errors.Is(err, context.Canceled) || tracked != nil {
		t.Fatalf("startWatch = %v, %v; want nil, context.Canceled", tracked, err)
	}
	if o.Ready() || !source.IsStopped() {
		t.Fatal("canceled watch retained health or its response stream")
	}
}
