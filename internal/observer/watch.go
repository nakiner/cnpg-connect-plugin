package observer

import (
	"context"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

var secretResource = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

const serverCAIndex = "serverCA"

func (o *Observer) initializeInformers() error {
	o.clusterInformer = cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return o.dynamic.Resource(clusterResource).Namespace(o.opts.Namespace).List(ctx, options)
		},
		WatchFuncWithContext: o.watchClusters,
	}, &unstructured.Unstructured{}, 0, cache.Indexers{serverCAIndex: indexServerCA})
	o.podInformer = cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = "cnpg.io/cluster"
			return o.kube.CoreV1().Pods(o.opts.Namespace).List(ctx, options)
		},
		WatchFuncWithContext: o.watchPods,
	}, &corev1.Pod{}, 0, cache.Indexers{clusterIndex: indexPodCluster})
	o.secretInformer = cache.NewSharedIndexInformer(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return o.metadata.Resource(secretResource).Namespace(o.opts.Namespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return o.startWatch(ctx, options, &o.secretWatch, o.metadata.Resource(secretResource).Namespace(o.opts.Namespace).Watch)
		},
	}, &metav1.PartialObjectMetadata{}, 0, cache.Indexers{})
	if err := o.secretInformer.SetTransform(func(obj any) (any, error) {
		m := obj.(*metav1.PartialObjectMetadata)
		return &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{
			Namespace: m.Namespace, Name: m.Name, UID: m.UID, ResourceVersion: m.ResourceVersion,
		}}, nil
	}); err != nil {
		return err
	}
	if _, err := o.secretInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: o.secretEvent, UpdateFunc: func(_, next any) { o.secretEvent(next) }, DeleteFunc: o.secretEvent,
	}); err != nil {
		return err
	}
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

func indexServerCA(obj any) ([]string, error) {
	c := obj.(*unstructured.Unstructured)
	enabled, _, _ := parameters(c)
	if name := serverCASecret(c); enabled && c.GetDeletionTimestamp() == nil && name != "" {
		return []string{c.GetNamespace() + "/" + name}, nil
	}
	return nil, nil
}

func indexPodCluster(obj any) ([]string, error) {
	if key, ok := podCluster(obj); ok {
		return []string{key.String()}, nil
	}
	return nil, nil
}

func (o *Observer) watchClusters(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	return o.startWatch(ctx, options, &o.clusterWatch, o.dynamic.Resource(clusterResource).Namespace(o.opts.Namespace).Watch)
}

func (o *Observer) watchPods(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	options.LabelSelector = "cnpg.io/cluster"
	return o.startWatch(ctx, options, &o.podWatch, o.kube.CoreV1().Pods(o.opts.Namespace).Watch)
}

// Renew quiet watches before snapshot expiry, so connectivity cannot remain
// healthy indefinitely when a peer stops sending data without closing its socket.
func (o *Observer) startWatch(ctx context.Context, options metav1.ListOptions, healthy *atomic.Bool, start func(context.Context, metav1.ListOptions) (watch.Interface, error)) (watch.Interface, error) {
	lifetime := min(30*time.Second, o.opts.TTL/2)
	timeoutSeconds := max(int64(1), int64(lifetime/time.Second))
	options.TimeoutSeconds = &timeoutSeconds
	options.AllowWatchBookmarks = true
	ctx, cancel := context.WithTimeout(ctx, lifetime)
	source, err := start(ctx, options)
	if err != nil {
		o.watchErrors.Add(1)
		healthy.Store(false)
		cancel()
		return nil, err
	}
	return o.trackWatch(ctx, source, healthy, cancel), nil
}

func (o *Observer) trackWatch(ctx context.Context, source watch.Interface, healthy *atomic.Bool, cancel context.CancelFunc) watch.Interface {
	events := make(chan watch.Event)
	w := watch.NewProxyWatcher(events)
	healthy.Store(true)
	o.watchStarts.Add(1)
	go func() {
		defer close(events)
		defer o.watchEnds.Add(1)
		defer healthy.Store(false)
		defer w.Stop()
		defer source.Stop()
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.StopChan():
				return
			case event, ok := <-source.ResultChan():
				if !ok {
					return
				}
				if event.Type == watch.Error {
					o.watchErrors.Add(1)
					healthy.Store(false)
				}
				select {
				case events <- event:
				case <-ctx.Done():
					return
				case <-w.StopChan():
					return
				}
			}
		}
	}()
	return w
}
