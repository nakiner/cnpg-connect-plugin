package server

import (
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
	"testing"
	"time"
)

type testCA struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	pem         []byte
}

func newCA(t *testing.T, name string) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca testCA) issue(t *testing.T, serial int64, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{"localhost"}, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.certificate, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair, certPEM, keyPEM
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func handshake(t *testing.T, serverConfig, clientConfig *tls.Config) (tls.ConnectionState, error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverErrors := make(chan error, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			serverErrors <- err
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(2 * time.Second))
		connection := tls.Server(raw, serverConfig)
		err = connection.Handshake()
		if err == nil {
			_, err = connection.Write([]byte{1})
		}
		serverErrors <- err
	}()
	connection, clientErr := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", listener.Addr().String(), clientConfig)
	var state tls.ConnectionState
	if clientErr == nil {
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		var ack [1]byte
		_, clientErr = connection.Read(ack[:])
		state = connection.ConnectionState()
		_ = connection.Close()
	}
	serverErr := <-serverErrors
	if serverErr != nil || clientErr != nil {
		return state, fmt.Errorf("server: %v; client: %v", serverErr, clientErr)
	}
	return state, nil
}

func TestMutualTLSAndCertificateRotation(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, trustPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"), filepath.Join(dir, "client.crt")
	ca := newCA(t, "server")
	operatorCA := newCA(t, "operator-a")
	otherCA := newCA(t, "operator-b")
	_, certPEM, keyPEM := ca.issue(t, 2, x509.ExtKeyUsageServerAuth)
	writeFile(t, certPath, certPEM)
	writeFile(t, keyPath, keyPEM)
	writeFile(t, trustPath, operatorCA.pem)
	serverConfig, err := TLSConfig(certPath, keyPath, trustPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	clientA, clientAPEM, _ := operatorCA.issue(t, 3, x509.ExtKeyUsageClientAuth)
	clientB, _, _ := otherCA.issue(t, 4, x509.ExtKeyUsageClientAuth)
	client := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: roots, Certificates: []tls.Certificate{clientA}}
	state, err := handshake(t, serverConfig, client)
	if err != nil || state.Version != tls.VersionTLS13 || state.PeerCertificates[0].SerialNumber.Int64() != 2 {
		t.Fatalf("trusted TLS1.3 client failed: %v", err)
	}
	anonymous := client.Clone()
	anonymous.Certificates = nil
	if _, err := handshake(t, serverConfig, anonymous); err == nil {
		t.Fatal("anonymous client was accepted")
	}
	unknown := client.Clone()
	unknown.Certificates = []tls.Certificate{clientB}
	if _, err := handshake(t, serverConfig, unknown); err == nil {
		t.Fatal("untrusted client was accepted")
	}
	wrongUsage := client.Clone()
	serverOnlyCertificate, _, _ := operatorCA.issue(t, 6, x509.ExtKeyUsageServerAuth)
	wrongUsage.Certificates = []tls.Certificate{serverOnlyCertificate}
	if _, err := handshake(t, serverConfig, wrongUsage); err == nil {
		t.Fatal("client certificate with server-only EKU was accepted")
	}
	oldTLS := client.Clone()
	oldTLS.MinVersion, oldTLS.MaxVersion = tls.VersionTLS12, tls.VersionTLS12
	if _, err := handshake(t, serverConfig, oldTLS); err == nil {
		t.Fatal("TLS1.2 client was accepted")
	}

	// CNPG can mount the exact operator leaf instead of a CA certificate.
	writeFile(t, trustPath, clientAPEM)
	if _, err := handshake(t, serverConfig, client); err != nil {
		t.Fatalf("trusted leaf rejected: %v", err)
	}

	// Existing TLSConfig observes both new server keys and replaced trust.
	_, certPEM, keyPEM = ca.issue(t, 5, x509.ExtKeyUsageServerAuth)
	writeFile(t, certPath, certPEM)
	writeFile(t, keyPath, keyPEM)
	writeFile(t, trustPath, otherCA.pem)
	state, err = handshake(t, serverConfig, unknown)
	if err != nil || state.PeerCertificates[0].SerialNumber.Int64() != 5 {
		t.Fatalf("rotation not observed: %v", err)
	}
	if _, err := handshake(t, serverConfig, client); err == nil {
		t.Fatal("previous operator trust survived rotation")
	}
	writeFile(t, trustPath, []byte("invalid rotated trust"))
	if _, err := handshake(t, serverConfig, unknown); err == nil {
		t.Fatal("invalid rotation must fail closed")
	}
}

func TestTLSConfigValidatesStartupAndServerOnlyMode(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if _, err := TLSConfig(certPath, keyPath, ""); err == nil {
		t.Fatal("missing credentials accepted")
	}
	ca := newCA(t, "server")
	_, certPEM, keyPEM := ca.issue(t, 1, x509.ExtKeyUsageServerAuth)
	writeFile(t, certPath, certPEM)
	writeFile(t, keyPath, keyPEM)
	serverConfig, err := TLSConfig(certPath, keyPath, "")
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	if _, err := handshake(t, serverConfig, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS13}); err != nil {
		t.Fatalf("discovery TLS must accept clients without TLS certificates: %v", err)
	}
	trustPath := filepath.Join(dir, "ca.pem")
	writeFile(t, trustPath, []byte("invalid"))
	if _, err := TLSConfig(certPath, keyPath, trustPath); err == nil {
		t.Fatal("invalid trust accepted at startup")
	}
}
