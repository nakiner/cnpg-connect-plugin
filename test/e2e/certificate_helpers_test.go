//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"maps"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type rotationConnectionStats struct{ begins, ends atomic.Uint64 }

func rotationSerial(ctx context.Context, endpoint string, roots *x509.CertPool, serverName string) (string, error) {
	// A raw TLS handshake can return a certificate while gRPC rejects the
	// connection for missing ALPN. Complete a real RPC before accepting evidence.
	// Each probe owns a fresh transport so it observes the current mounted leaf.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs: roots, ServerName: serverName, MinVersion: tls.VersionTLS13,
	})))
	if err != nil {
		return "", err
	}
	defer connection.Close()
	var remote peer.Peer
	if _, err := connectv1.NewTopologyServiceClient(connection).GetTopology(ctx, getRequest(), grpc.Peer(&remote)); err != nil {
		return "", fmt.Errorf("certificate probe GetTopology: %w", err)
	}
	info, ok := remote.AuthInfo.(credentials.TLSInfo)
	if !ok || info.State.NegotiatedProtocol != "h2" || len(info.State.VerifiedChains) == 0 || len(info.State.PeerCertificates) == 0 {
		return "", fmt.Errorf("certificate probe has no verified h2 TLS peer")
	}
	return info.State.PeerCertificates[0].SerialNumber.String(), nil
}

func (s *rotationConnectionStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (s *rotationConnectionStats) HandleRPC(context.Context, stats.RPCStats) {}
func (s *rotationConnectionStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (s *rotationConnectionStats) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		s.begins.Add(1)
	case *stats.ConnEnd:
		s.ends.Add(1)
	}
}

type rotationPodState struct {
	uid      string
	restarts map[string]int32
}

func (h *harness) rotationPodStates(ctx context.Context) (map[string]rotationPodState, error) {
	if err := h.guardContext(); err != nil {
		return nil, err
	}
	pods, err := h.kubernetes.CoreV1().Pods(pluginNamespace).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/instance=connect,app.kubernetes.io/name=cnpg-connect-plugin"})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no discovery replicas found")
	}
	result := make(map[string]rotationPodState, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.UID == "" || pod.DeletionTimestamp != nil || len(pod.Status.ContainerStatuses) == 0 {
			return nil, fmt.Errorf("discovery Pod %s is not stable", pod.Name)
		}
		state := rotationPodState{uid: string(pod.UID), restarts: map[string]int32{}}
		for _, container := range pod.Status.ContainerStatuses {
			if !container.Ready {
				return nil, fmt.Errorf("discovery Pod %s is not ready", pod.Name)
			}
			state.restarts[container.Name] = container.RestartCount
		}
		result[pod.Name] = state
	}
	return result, nil
}
func sameRotationPods(before, after map[string]rotationPodState) bool {
	return maps.EqualFunc(before, after, func(a, b rotationPodState) bool { return a.uid == b.uid && maps.Equal(a.restarts, b.restarts) })
}

// Per-Pod forwarding removes load-balancer sampling from certificate evidence.
// Each owned subprocess is canceled and joined at cleanup; TLS still verifies
// the discovery service identity rather than the forwarding loopback address.
func (h *harness) forwardRotationPod(t *testing.T, parent context.Context, name string) *rotationForward {
	t.Helper()
	if err := h.guardContext(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(parent)
	command := exec.CommandContext(ctx, "kubectl", "--kubeconfig", h.kubeconfig, "--context", expectedContext, "-n", pluginNamespace, "port-forward", "--address=127.0.0.1", "pod/"+name, ":8080")
	output, err := command.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	forward := &rotationForward{done: make(chan struct{})}
	command.Stderr = &forward.diagnostic
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		defer close(forward.done)
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			var port int
			if _, err := fmt.Sscanf(scanner.Text(), "Forwarding from 127.0.0.1:%d -> 8080", &port); err == nil && port > 0 {
				select {
				case ready <- net.JoinHostPort("127.0.0.1", strconv.Itoa(port)):
				default:
				}
			}
		}
		forward.err = command.Wait()
	}()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("port-forward %s: %s", name, forward.status())
		}
		cancel()
		select {
		case <-forward.done:
		case <-time.After(5 * time.Second):
			t.Error("port-forward subprocess did not stop")
		}
	})
	select {
	case endpoint := <-ready:
		forward.endpoint = endpoint
		return forward
	case <-forward.done:
		t.Fatalf("port-forward %s failed: %s", name, forward.status())
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return nil
}

// Stderr is written by exec's copier while probes inspect it on failures.
type rotationDiagnostic struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (d *rotationDiagnostic) Write(p []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.buffer.Write(p)
}

func (d *rotationDiagnostic) String() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.buffer.String()
}

type rotationForward struct {
	endpoint   string
	diagnostic rotationDiagnostic
	done       chan struct{}
	err        error // Published by closing done; read only after receiving from done.
}

func (f *rotationForward) status() string {
	select {
	case <-f.done:
		return fmt.Sprintf("exited (%v); stderr: %s", f.err, f.diagnostic.String())
	default:
		return fmt.Sprintf("running; stderr: %s", f.diagnostic.String())
	}
}
