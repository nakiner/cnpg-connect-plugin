package observer

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// applicationDatabase follows CNPG's GetApplicationDatabaseName. The API
// object already contains CNPG bootstrap defaults; a restored cluster without
// application metadata has no inferred default database.
func applicationDatabase(cluster *unstructured.Unstructured) string {
	for _, method := range []string{"recovery", "pg_basebackup", "initdb"} {
		name, _, _ := unstructured.NestedString(cluster.Object, "spec", "bootstrap", method, "database")
		if name != "" {
			return name
		}
	}
	return ""
}

func serverCASecret(cluster *unstructured.Unstructured) string {
	name, _, _ := unstructured.NestedString(cluster.Object, "status", "certificates", "serverCASecret")
	return name
}

func (o *Observer) connectionParameters(ctx context.Context, cluster *unstructured.Unstructured) (v1.ConnectionParameters, error) {
	result := v1.ConnectionParameters{Database: applicationDatabase(cluster)}
	name := serverCASecret(cluster)
	if name == "" {
		return result, fmt.Errorf("PostgreSQL server CA not yet reported")
	}
	certificates, _, _ := unstructured.NestedFieldNoCopy(cluster.Object, "status", "certificates")
	encoded, _ := json.Marshal(certificates)
	key := connectionReadKey{namespace: cluster.GetNamespace(), secret: name, certificates: string(encoded)}
	public, err := o.connectionReads.read(ctx, key, o.opts.ProbeTimeout, func(requestCtx context.Context) ([]byte, error) {
		return o.readPublicCA(requestCtx, key.namespace, key.secret)
	})
	if err != nil {
		return result, err
	}
	// Each Cluster owns its retained bytes. The shared read only deduplicates
	// overlapping I/O; it cannot extend a Cluster's CA cache lifetime.
	result.ServerCAPEM = append([]byte(nil), public...)
	return result, nil
}

func (o *Observer) readPublicCA(ctx context.Context, namespace, name string) ([]byte, error) {
	secret, err := o.kube.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read PostgreSQL server CA: %w", err)
	}
	var public []byte
	// Only serialize public certificates, even if a malformed ca.crt contains
	// unrelated PEM blocks. Never publish other Secret keys or private material.
	remaining := secret.Data["ca.crt"]
	for len(remaining) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil {
			break
		}
		remaining = rest
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return nil, fmt.Errorf("invalid PostgreSQL server CA certificate")
		}
		public = append(public, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes})...)
	}
	if len(public) == 0 {
		return nil, fmt.Errorf("PostgreSQL server CA has no public certificates")
	}
	return public, nil
}

// Only reads of the same named Secret and certificate metadata can overlap.
// A metadata change must not join a request started for the previous view.
type connectionReadKey struct{ namespace, secret, certificates string }

type connectionRead struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	public  []byte
	err     error
}

// connectionReadGroup retains only in-flight reads, bounded by observation
// workers. An individual canceled observation leaves its peers running; when
// the last waiter leaves, both the request and any API rate-limit wait stop.
type connectionReadGroup struct {
	mu      sync.Mutex
	pending map[connectionReadKey]*connectionRead
}

func (g *connectionReadGroup) read(ctx context.Context, key connectionReadKey, timeout time.Duration, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.pending == nil {
		g.pending = make(map[connectionReadKey]*connectionRead)
	}
	call := g.pending[key]
	if call == nil {
		// A shared request has its own bounded lifetime rather than inheriting
		// the first subscriber's cancellation. The last waiter cancels it.
		requestCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		call = &connectionRead{done: make(chan struct{}), cancel: cancel}
		g.pending[key] = call
		go func() {
			defer cancel()
			public, err := fetch(requestCtx)
			g.mu.Lock()
			call.public, call.err = public, err
			if g.pending[key] == call {
				delete(g.pending, key)
			}
			close(call.done)
			g.mu.Unlock()
		}()
	}
	call.waiters++
	g.mu.Unlock()
	defer g.leave(key, call)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-call.done:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return call.public, call.err
	}
}

func (g *connectionReadGroup) leave(key connectionReadKey, call *connectionRead) {
	g.mu.Lock()
	defer g.mu.Unlock()
	call.waiters--
	if call.waiters == 0 {
		if g.pending[key] == call {
			delete(g.pending, key)
		}
		call.cancel()
	}
}

// Public CA material is reused between status samples. Certificate metadata
// changes invalidate it immediately; a failed primary TLS probe also refetches.
// The bounded fallback covers external CA rotations with unchanged metadata.
type cachedConnection struct {
	parameters   v1.ConnectionParameters
	uid          string
	secret       string
	certificates string
	refreshed    time.Time
}

func (o *Observer) cachedConnection(ctx context.Context, cluster *unstructured.Unstructured) (v1.ConnectionParameters, error) {
	key := clusterKey{cluster.GetNamespace(), cluster.GetName()}
	certificates, _, _ := unstructured.NestedFieldNoCopy(cluster.Object, "status", "certificates")
	encoded, _ := json.Marshal(certificates)
	if value, exists := o.connections.Load(key); exists {
		cached := value.(cachedConnection)
		if cached.matches(cluster, string(encoded)) {
			return cached.parameters, nil
		}
	}

	parameters, err := o.connectionParameters(ctx, cluster)
	if err != nil {
		return parameters, err
	}
	o.connections.Store(key, cachedConnection{
		parameters:   parameters,
		uid:          string(cluster.GetUID()),
		secret:       serverCASecret(cluster),
		certificates: string(encoded),
		refreshed:    time.Now(),
	})
	return parameters, nil
}

func (cached cachedConnection) matches(cluster *unstructured.Unstructured, certificates string) bool {
	if cached.uid != string(cluster.GetUID()) || cached.secret != serverCASecret(cluster) {
		return false
	}
	if cached.certificates != certificates || cached.parameters.Database != applicationDatabase(cluster) {
		return false
	}
	return time.Since(cached.refreshed) < connectionCacheLifetime(cluster)
}

// Spread periodic CA refreshes across the fleet, including simultaneous startup.
// Certificate metadata and verified TLS failures still invalidate immediately.
func connectionCacheLifetime(cluster *unstructured.Unstructured) time.Duration {
	h := fnv.New64a()
	_, _ = h.Write([]byte(cluster.GetNamespace() + "/" + cluster.GetName() + "/" + string(cluster.GetUID())))
	return 3*time.Minute + time.Duration(h.Sum64()%uint64(2*time.Minute))
}
