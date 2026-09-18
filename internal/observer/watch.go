package observer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func (o *Observer) initializeInformers() error {
	o.clusterInformer = cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return o.dynamic.Resource(clusterResource).Namespace(o.opts.Namespace).List(ctx, options)
		},
		WatchFuncWithContext: o.watchClusters,
	}, &unstructured.Unstructured{}, 0, cache.Indexers{})
	o.podInformer = cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = "cnpg.io/cluster"
			return o.kube.CoreV1().Pods(o.opts.Namespace).List(ctx, options)
		},
		WatchFuncWithContext: o.watchPods,
	}, &corev1.Pod{}, 0, cache.Indexers{clusterIndex: indexPodCluster})
	if err := o.clusterInformer.SetTransform(transformClusterCache); err != nil {
		return err
	}
	if err := o.podInformer.SetTransform(transformPodCache); err != nil {
		return err
	}
	if _, err := o.clusterInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { o.clusterEvent(nil, obj) },
		UpdateFunc: o.clusterEvent,
		DeleteFunc: o.clusterDeleted,
	}); err != nil {
		return err
	}
	if _, err := o.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { o.podEvent(nil, obj) },
		UpdateFunc: o.podEvent,
		DeleteFunc: func(obj any) { o.podEvent(obj, nil) },
	}); err != nil {
		return err
	}
	return nil
}

func indexPodCluster(obj any) ([]string, error) {
	if key, ok := podCluster(obj); ok {
		return []string{key.String()}, nil
	}
	return nil, nil
}

func (o *Observer) watchClusters(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	watchCtx, cancel := o.watchContext(ctx, &options)
	source, err := o.dynamic.Resource(clusterResource).Namespace(o.opts.Namespace).Watch(watchCtx, options)
	if err != nil {
		o.clusterWatch.Store(false)
		cancel()
		return nil, err
	}
	return o.trackWatch(watchCtx, source, &o.clusterWatch, cancel), nil
}

func (o *Observer) watchPods(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	options.LabelSelector = "cnpg.io/cluster"
	watchCtx, cancel := o.watchContext(ctx, &options)
	source, err := o.kube.CoreV1().Pods(o.opts.Namespace).Watch(watchCtx, options)
	if err != nil {
		o.podWatch.Store(false)
		cancel()
		return nil, err
	}
	return o.trackWatch(watchCtx, source, &o.podWatch, cancel), nil
}

// Renew quiet watches before snapshot expiry, so connectivity cannot remain
// healthy indefinitely when a peer stops sending data without closing its socket.
func (o *Observer) watchContext(ctx context.Context, options *metav1.ListOptions) (context.Context, context.CancelFunc) {
	lifetime := min(30*time.Second, o.opts.TTL/2)
	timeoutSeconds := max(int64(1), int64(lifetime/time.Second))
	options.TimeoutSeconds = &timeoutSeconds
	options.AllowWatchBookmarks = true
	return context.WithTimeout(ctx, lifetime)
}

type trackedWatch struct {
	source watch.Interface
	events chan watch.Event
	done   chan struct{}
	once   sync.Once
	cancel context.CancelFunc
}

func (w *trackedWatch) Stop() {
	w.once.Do(func() {
		close(w.done)
		w.source.Stop()
		if w.cancel != nil {
			w.cancel()
		}
	})
}

func (w *trackedWatch) ResultChan() <-chan watch.Event { return w.events }

func (o *Observer) trackWatch(ctx context.Context, source watch.Interface, healthy *atomic.Bool, cancellation ...context.CancelFunc) watch.Interface {
	w := &trackedWatch{
		source: source,
		events: make(chan watch.Event),
		done:   make(chan struct{}),
	}
	if len(cancellation) > 0 {
		w.cancel = cancellation[0]
	}
	healthy.Store(true)
	go func() {
		defer close(w.events)
		defer healthy.Store(false)
		defer w.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.done:
				return
			case event, ok := <-source.ResultChan():
				if !ok {
					return
				}
				if event.Type == watch.Error {
					healthy.Store(false)
				}
				select {
				case w.events <- event:
				case <-ctx.Done():
					return
				case <-w.done:
					return
				}
			}
		}
	}()
	return w
}
