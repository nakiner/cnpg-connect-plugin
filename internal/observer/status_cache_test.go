package observer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
)

const statusCacheServerName = "db-rw.test.svc"

func newStatusCacheServer(t *testing.T, beforeReply func(http.ResponseWriter, *http.Request)) (*httptest.Server, v1.ConnectionParameters, *atomic.Int64) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		DNSNames:              []string{statusCacheServerName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if beforeReply != nil {
			beforeReply(w, r)
		}
		_, _ = io.WriteString(w, `{"isPrimary":true,"systemID":"123","timeLineID":1}`)
	}))
	connections := new(atomic.Int64)
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, v1.ConnectionParameters{ServerCAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, connections
}

func TestStatusClientConcurrentColdCreationReusesConnections(t *testing.T) {
	server, parameters, connections := newStatusCacheServer(t, nil)
	o := &Observer{opts: Options{ProbeTimeout: time.Second}}
	t.Cleanup(o.closeStatusClients)
	const callers = 64
	type result struct {
		client *http.Client
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, callers)
	for range callers {
		go func() {
			<-start
			client, err := o.statusClient(parameters, statusCacheServerName)
			results <- result{client: client, err: err}
		}()
	}
	close(start)
	var client *http.Client
	for range callers {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if client == nil {
			client = got.client
		} else if client != got.client {
			t.Fatal("concurrent cold lookups created separate connection pools")
		}
	}
	for range 3 {
		if _, err := readStatusHTTP(context.Background(), client, server.URL+"/pg/status"); err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("repeated status probes opened %d connections, want one reused connection", got)
	}
}

func TestStatusClientRotationReplacesTrustWithoutWeakeningVerification(t *testing.T) {
	previousServer, previousCA, _ := newStatusCacheServer(t, nil)
	rotatedServer, rotatedCA, _ := newStatusCacheServer(t, nil)
	o := &Observer{opts: Options{ProbeTimeout: time.Second}}
	t.Cleanup(o.closeStatusClients)
	previous, err := o.statusClient(previousCA, statusCacheServerName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readStatusHTTP(context.Background(), previous, previousServer.URL+"/pg/status"); err != nil {
		t.Fatal(err)
	}
	rotated, err := o.statusClient(rotatedCA, statusCacheServerName)
	if err != nil {
		t.Fatal(err)
	}
	if rotated == previous {
		t.Fatal("CA rotation reused the old trust configuration")
	}
	if _, err := readStatusHTTP(context.Background(), rotated, rotatedServer.URL+"/pg/status"); err != nil {
		t.Fatalf("replacement client rejected the rotated CA: %v", err)
	}
	if _, err := readStatusHTTP(context.Background(), rotated, previousServer.URL+"/pg/status"); !isCertificateError(err) {
		t.Fatalf("replacement client retained retired trust: %v", err)
	}
	if _, err := readStatusHTTP(context.Background(), previous, rotatedServer.URL+"/pg/status"); !isCertificateError(err) {
		t.Fatalf("rotation mutated an existing client's trust: %v", err)
	}
	current, err := o.statusClient(rotatedCA, statusCacheServerName)
	if err != nil || current != rotated {
		t.Fatalf("rotated trust was not retained for subsequent probes: %v", err)
	}
}

func TestStatusClientCleanupDuringHotLookupsPreservesActiveRequests(t *testing.T) {
	for _, operation := range []string{"prune", "close"} {
		t.Run(operation, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			server, parameters, _ := newStatusCacheServer(t, func(_ http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("hold") == "true" {
					close(started)
					select {
					case <-release:
					case <-r.Context().Done():
					}
				}
			})
			o := &Observer{opts: Options{ProbeTimeout: 5 * time.Second}}
			t.Cleanup(o.closeStatusClients)
			client, err := o.statusClient(parameters, statusCacheServerName)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			requestDone := make(chan error, 1)
			go func() {
				_, err := readStatusHTTP(ctx, client, server.URL+"/pg/status?hold=true")
				requestDone <- err
			}()
			select {
			case <-started:
			case err := <-requestDone:
				t.Fatalf("held request did not start: %v", err)
			}
			cleanup := o.closeStatusClients
			if operation == "prune" {
				cleanup = func() { o.pruneConnections(time.Now().Add(2 * time.Minute)) }
			}
			// Cleanup must leave a request already using the old transport alive.
			cleanup()
			const callers = 16
			var workers sync.WaitGroup
			start := make(chan struct{})
			errors := make(chan error, callers)
			for range callers {
				workers.Go(func() {
					<-start
					for range 32 {
						current, err := o.statusClient(parameters, statusCacheServerName)
						if err == nil {
							_, err = readStatusHTTP(ctx, current, server.URL+"/pg/status")
						}
						if err != nil {
							errors <- fmt.Errorf("lookup/probe concurrent with %s: %w", operation, err)
							return
						}
					}
				})
			}
			workers.Go(func() {
				<-start
				for range 128 {
					cleanup()
				}
			})
			close(start)
			workers.Wait()
			close(errors)
			for err := range errors {
				t.Error(err)
			}
			unblock()
			if err := <-requestDone; err != nil {
				t.Fatalf("cleanup interrupted an active request: %v", err)
			}
			cleanup()
			fresh, err := o.statusClient(parameters, statusCacheServerName)
			if err != nil || fresh == client {
				t.Fatalf("cleanup retained the old cached transport: %v", err)
			}
			if _, err := readStatusHTTP(ctx, fresh, server.URL+"/pg/status"); err != nil {
				t.Fatalf("fresh transport after cleanup failed: %v", err)
			}
		})
	}
}
