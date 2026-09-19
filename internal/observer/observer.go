// Package observer streams Kubernetes metadata and verifies demanded PostgreSQL
// topology without participating in promotion, fencing, or replication changes.
package observer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

var clusterResource = schema.GroupVersionResource{Group: "postgresql.cnpg.io", Version: "v1", Resource: "clusters"}

const clusterIndex = "cluster"

type clusterKey struct {
	namespace string
	name      string
}

func (k clusterKey) String() string { return k.namespace + "/" + k.name }

type Options struct {
	Namespace string
	// PollInterval only verifies actual PostgreSQL state for clusters with clients.
	// Kubernetes metadata is maintained by shared LIST/WATCH streams.
	PollInterval time.Duration
	TTL          time.Duration
	ProbeTimeout time.Duration
	// MaxConcurrency caps simultaneous instance status requests across all clusters.
	MaxConcurrency int
	// MaxConcurrentClusters caps whole-cluster collections; zero selects 32.
	// One eighth of each concurrency limit is reserved for urgent work, with
	// at least one reserved slot when the corresponding limit exceeds one.
	MaxConcurrentClusters int
}

type Observer struct {
	watchStarts, watchEnds, watchErrors          atomic.Uint64
	caHits, caMisses, caFailures, caReadFailures atomic.Uint64
	probeSaturation, probeCancellations          atomic.Uint64

	observationDuration *metrics.PrometheusHistogram
	probeDuration       *metrics.PrometheusHistogram
	probeQueueDuration  *metrics.PrometheusHistogram
	metrics             *metrics.Set

	kube     kubernetes.Interface
	dynamic  dynamic.Interface
	metadata metadata.Interface
	store    *discovery.Store
	opts     Options
	log      *slog.Logger

	clusterInformer cache.SharedIndexInformer
	podInformer     cache.SharedIndexInformer
	secretInformer  cache.SharedIndexInformer
	ready           atomic.Bool
	clusterWatch    atomic.Bool
	podWatch        atomic.Bool
	secretWatch     atomic.Bool

	queue      workqueue.TypedRateLimitingInterface[clusterKey]
	priority   *priorityQueue
	demandNext sync.Map // clusterKey -> earliest nonurgent refresh time

	probeSlots           chan struct{}
	backgroundProbeSlots chan struct{}
	probe                func(context.Context, *corev1.Pod, v1.ConnectionParameters, string) (instanceStatus, error)

	stateMu      sync.Mutex
	observations map[clusterKey]context.CancelCauseFunc // protected by stateMu
	primaries    map[clusterKey]*primaryEvidence        // protected by stateMu

	// CA state shares stateMu with observation invalidation and publication.
	cas       map[types.NamespacedName]*cachedCA // protected by stateMu
	caReads   map[types.NamespacedName]context.CancelFunc
	caPending map[types.NamespacedName]bool
	caQueue   workqueue.TypedRateLimitingInterface[types.NamespacedName]

	statusClientMu sync.RWMutex
	statusClients  map[string]*statusClientEntry // protected by statusClientMu
}

func New(kube kubernetes.Interface, dyn dynamic.Interface, meta metadata.Interface, store *discovery.Store, opts Options, logger *slog.Logger) (*Observer, error) {
	if kube == nil || dyn == nil || meta == nil || store == nil {
		return nil, fmt.Errorf("kubernetes clients and store are required")
	}
	if opts.PollInterval <= 0 || opts.ProbeTimeout <= 0 ||
		opts.TTL <= opts.PollInterval || opts.ProbeTimeout >= opts.TTL-opts.PollInterval {
		return nil, fmt.Errorf("ttl must exceed positive status refresh interval plus probe timeout")
	}
	if opts.MaxConcurrency < 1 || opts.MaxConcurrency > 1024 {
		return nil, fmt.Errorf("max concurrency must be between 1 and 1024")
	}
	if opts.MaxConcurrentClusters == 0 {
		opts.MaxConcurrentClusters = 32
	}
	if opts.MaxConcurrentClusters < 1 || opts.MaxConcurrentClusters > 1024 {
		return nil, fmt.Errorf("max concurrent clusters must be between 1 and 1024")
	}
	if logger == nil {
		logger = slog.Default()
	}
	queue, priority := newObservationQueue()
	caRetries := workqueue.NewTypedItemExponentialFailureRateLimiter[types.NamespacedName](100*time.Millisecond, 5*time.Second)
	o := &Observer{
		kube:         kube,
		dynamic:      dyn,
		metadata:     meta,
		store:        store,
		opts:         opts,
		log:          logger,
		observations: make(map[clusterKey]context.CancelCauseFunc),
		queue:        queue,
		priority:     priority,
		cas:          make(map[types.NamespacedName]*cachedCA),
		caReads:      make(map[types.NamespacedName]context.CancelFunc),
		caPending:    make(map[types.NamespacedName]bool),
		caQueue:      workqueue.NewTypedRateLimitingQueue(caRetries),
	}
	o.initializeMetrics()
	o.configureProbeLimits()
	o.probe = o.readStatus
	if err := o.initializeInformers(); err != nil {
		return nil, err
	}
	store.SetDemandHandler(func(namespace, name string) {
		o.enqueue(namespace, name, false)
	}, opts.TTL)
	return o, nil
}

// Notify schedules only the affected cluster. Hooks and subscriptions never
// wait for I/O, nor cause an unrelated cluster to be observed.
func (o *Observer) Notify(namespace, name string) {
	o.enqueue(namespace, name, true)
}

func (o *Observer) enqueue(namespace, name string, urgent bool) {
	if namespace == "" || name == "" || (o.opts.Namespace != "" && namespace != o.opts.Namespace) {
		return
	}
	// Ignore unknown/idle names: CNPG hooks must not allocate unbounded work.
	if !o.store.HasDemand(namespace, name) {
		return
	}
	if _, exists, _ := o.clusterInformer.GetIndexer().GetByKey(namespace + "/" + name); !exists {
		return
	}
	key := clusterKey{namespace, name}
	if urgent {
		o.priority.promote(key)
	} else {
		// Watch-only reconnect churn must not turn the periodic schedule into
		// continuous probes. The delaying queue coalesces work per key; unlike
		// dropping requests, this still wakes a new watcher after the cooldown.
		for {
			now := time.Now()
			next, loaded := o.demandNext.LoadOrStore(key, now.Add(o.opts.PollInterval))
			if !loaded {
				break
			}
			if deadline := next.(time.Time); now.Before(deadline) {
				o.queue.AddAfter(key, deadline.Sub(now))
				return
			}
			if o.demandNext.CompareAndSwap(key, next, now.Add(o.opts.PollInterval)) {
				break
			}
		}
	}
	o.queue.Add(key)
}

func (o *Observer) Ready() bool {
	return o.ready.Load() && o.clusterWatch.Load() && o.podWatch.Load() && o.secretWatch.Load()
}

func (o *Observer) Run(ctx context.Context) error {
	defer o.ready.Store(false)
	var workers sync.WaitGroup
	workers.Go(func() { o.clusterInformer.Run(ctx.Done()) })
	workers.Go(func() { o.podInformer.Run(ctx.Done()) })
	workers.Go(func() { o.secretInformer.Run(ctx.Done()) })
	defer func() {
		o.queue.ShutDown()
		o.caQueue.ShutDown()
		workers.Wait()
		o.closeStatusClients()
	}()
	if !cache.WaitForCacheSync(ctx.Done(), o.clusterInformer.HasSynced, o.podInformer.HasSynced, o.secretInformer.HasSynced) {
		return nil
	}
	o.ready.Store(true)
	o.startWorkers(ctx, &workers)
	cleanup := time.NewTicker(time.Minute)
	defer cleanup.Stop()
	expiry := time.NewTicker(min(time.Second, o.opts.TTL/4))
	defer expiry.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-expiry.C:
			o.store.Expire(now)
		case now := <-cleanup.C:
			o.pruneConnections(now)
		}
	}
}
