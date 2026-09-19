//go:build e2e

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestRotationSerialProbeDeadline(t *testing.T) {
	// TCP connects, but no server completes TLS. Honor the caller's deadline.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	serial, err := rotationSerial(ctx, listener.Addr().String(), x509.NewCertPool(), "discovery.test")
	if serial != "" || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("stalled probe: serial=%q error=%v", serial, err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("probe did not honor caller deadline")
	}
}

func TestRotationForwardDiagnostics(t *testing.T) {
	forward := &rotationForward{done: make(chan struct{})}
	go func() {
		for range 100 {
			_, _ = forward.diagnostic.Write([]byte("forwarding error\n"))
		}
		forward.err = fmt.Errorf("exit status 1")
		close(forward.done)
	}()
	for range 100 {
		_ = forward.status()
	}
	<-forward.done
	got := forward.status()
	if !strings.Contains(got, "exited (exit status 1)") || !strings.Contains(got, "forwarding error") {
		t.Fatalf("missing subprocess diagnostics: %s", got)
	}
}

type rotationProbeService struct {
	connectv1.UnimplementedTopologyServiceServer
	calls atomic.Int64
}

func (s *rotationProbeService) GetTopology(ctx context.Context, request *connectv1.GetTopologyRequest) (*connectv1.Snapshot, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer local-probe-token" {
		return nil, status.Error(codes.Unauthenticated, "probe requires bearer authentication")
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "missing peer")
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || info.State.NegotiatedProtocol != "h2" || info.State.Version != tls.VersionTLS13 {
		return nil, status.Error(codes.Internal, "probe requires TLS 1.3 and h2")
	}
	if request.Namespace != clusterNamespace || request.Name != clusterName {
		return nil, status.Error(codes.InvalidArgument, "wrong topology target")
	}
	s.calls.Add(1)
	return &connectv1.Snapshot{}, nil
}

// Exercise the production TLS reload callback, real gRPC framing, and the exact
// probe used through each Pod's port-forward; no Kubernetes access is required.
func TestRotationSerialProbe(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "probe CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		t.Fatal(err)
	}
	rotate := func(serial int64) {
		t.Helper()
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{"discovery.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(certPath+".new", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(certPath+".new", certPath); err != nil {
			t.Fatal(err)
		}
	}
	rotate(2)
	cfg, err := server.TLSConfig(certPath, keyPath, "")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	service := &rotationProbeService{}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(cfg)))
	connectv1.RegisterTopologyServiceServer(srv, service)
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(listener) }()
	t.Cleanup(func() { srv.Stop(); <-done })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := &harness{token: "local-probe-token"}
	for _, want := range []string{"2", "3", "3"} {
		if want == "3" {
			rotate(3)
		}
		before := service.calls.Load()
		got, err := rotationSerial(h.auth(ctx), listener.Addr().String(), roots, "discovery.test")
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("serial = %s, want %s", got, want)
		}
		if service.calls.Load() != before+1 {
			t.Fatal("serial probe returned success without completing an authenticated h2 GetTopology RPC")
		}
	}
	for _, tc := range []struct {
		name, serverName string
		roots            *x509.CertPool
		authenticated    bool
	}{
		{"wrong name", "other.test", roots, true},
		{"untrusted CA", "discovery.test", x509.NewCertPool(), true},
		{"missing bearer", "discovery.test", roots, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			callCtx := ctx
			if tc.authenticated {
				callCtx = h.auth(callCtx)
			}
			serial, err := rotationSerial(callCtx, listener.Addr().String(), tc.roots, tc.serverName)
			if err == nil || serial != "" {
				t.Fatalf("invalid probe accepted: serial=%q, error=%v", serial, err)
			}
		})
	}
}
