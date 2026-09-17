package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"
)

// TLSConfig validates mounted credentials at startup and reloads them for each
// handshake, including the operator trust bundle. A rotation error fails closed.
// clientTrust may contain a CA or the exact trusted operator client certificate.
// An empty clientTrust configures server-only TLS for the discovery API.
func TLSConfig(certFile, keyFile, clientTrust string) (*tls.Config, error) {
	load := func() (*tls.Config, error) {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load server key pair: %w", err)
		}
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return nil, fmt.Errorf("parse server certificate: %w", err)
		}
		now := time.Now()
		if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
			return nil, fmt.Errorf("server certificate is outside its validity period")
		}
		cfg := &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2", "http/1.1"},
			// Revalidate current operator trust on every new connection, including
			// clients that attempt to resume a session from before a rotation.
			SessionTicketsDisabled: true,
		}
		if clientTrust != "" {
			pem, err := os.ReadFile(clientTrust)
			if err != nil {
				return nil, fmt.Errorf("read operator client trust: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("operator client trust contains no valid PEM certificates")
			}
			// Do not advertise a CA issuer list: CNPG commonly mounts the
			// operator's exact leaf certificate as trust, whose subject would
			// otherwise prevent Go clients selecting their own certificate.
			// Require a client certificate, then apply normal chain, validity,
			// and client-auth EKU verification against the mounted trust.
			cfg.ClientAuth = tls.RequireAnyClientCert
			cfg.VerifyConnection = func(state tls.ConnectionState) error {
				if len(state.PeerCertificates) == 0 {
					return fmt.Errorf("operator client certificate is required")
				}
				intermediates := x509.NewCertPool()
				for _, cert := range state.PeerCertificates[1:] {
					intermediates.AddCert(cert)
				}
				_, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{
					Roots:         pool,
					Intermediates: intermediates,
					KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				})
				if err != nil {
					return fmt.Errorf("verify operator client certificate: %w", err)
				}
				return nil
			}
		}
		return cfg, nil
	}
	initial, err := load()
	if err != nil {
		return nil, err
	}
	initial.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return load()
	}
	return initial, nil
}
