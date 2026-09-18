package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestVersionAndHelpDoNotRequireKubernetesOrCredentials(t *testing.T) {
	for _, flag := range []string{"--version", "--help"} {
		var out, errOut bytes.Buffer
		if err := run(context.Background(), []string{flag}, &out, &errOut); err != nil {
			t.Fatal(err)
		}
		if flag == "--version" && strings.TrimSpace(out.String()) != version {
			t.Fatalf("wrong version: %s", out.String())
		}
		if flag == "--help" && !strings.Contains(errOut.String(), "discovery-address") {
			t.Fatal("help omitted discovery flags")
		}
	}
}

func TestSecureModeRequiredByDefault(t *testing.T) {
	s, err := parseSettings(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if s.validate() == nil {
		t.Fatal("startup allowed without TLS credentials or explicit insecure mode")
	}
	if s.pluginAddress != ":9090" || s.discoveryAddress != ":8080" || s.healthAddress != ":8081" {
		t.Fatalf("unexpected production defaults: %+v", s)
	}
}

func TestDiscoveryNeedsNoTokenAndCanUseGatewayTLS(t *testing.T) {
	args := []string{"--server-cert=server.pem", "--server-key=server.key", "--client-ca=client-ca.pem"}
	for _, discovery := range [][]string{
		{"--discovery-cert=discovery.pem", "--discovery-key=discovery.key"},
		{"--discovery-plaintext"},
	} {
		s, err := parseSettings(append(args, discovery...), io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.validate(); err != nil {
			t.Fatalf("tokenless discovery %v: %v", discovery, err)
		}
	}
}

func TestInsecureModeDefaultsToLoopbackAndHonorsExplicitAddresses(t *testing.T) {
	s, err := parseSettings([]string{"--insecure"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.validate(); err != nil {
		t.Fatal(err)
	}
	if s.pluginAddress != "127.0.0.1:9090" || s.discoveryAddress != "127.0.0.1:8080" || s.healthAddress != "127.0.0.1:8081" {
		t.Fatalf("insecure defaults are exposed: %+v", s)
	}
	s, err = parseSettings([]string{"--insecure", "--discovery-address=0.0.0.0:9000"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if s.discoveryAddress != "0.0.0.0:9000" {
		t.Fatal("explicit development address ignored")
	}
}

func TestInvalidOptions(t *testing.T) {
	for _, args := range [][]string{
		{"--insecure", "--ttl=7s"},
		{"--insecure", "--probe-timeout=0s"},
		{"--insecure", "--max-concurrency=0"},
		{"--insecure", "--max-concurrency=1025"},
		{"--insecure", "--max-concurrent-clusters=0"},
		{"--insecure", "--max-concurrent-clusters=1025"},
		{"--insecure", "--poll-interval=-1s"},
		{"--insecure", "--server-cert=certificate.pem"},
		{"--insecure", "--auth-token-file=token"},
		{"--insecure", "--discovery-address="},
		{"--insecure", "--kube-api-qps=0"},
		{"--insecure", "--kube-api-qps=-1"},
		{"--insecure", "--kube-api-qps=NaN"},
		{"--insecure", "--kube-api-qps=+Inf"},
		{"--insecure", "--kube-api-qps=1e40"},
		{"--insecure", "--kube-api-qps=1e-100"},
		{"--insecure", "--kube-api-burst=0"},
		{"--insecure", "--kube-api-burst=-1"},
	} {
		s, err := parseSettings(args, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if s.validate() == nil {
			t.Errorf("invalid options accepted: %v", args)
		}
	}
}

func TestKubernetesRateLimitDefaultsAndOverrides(t *testing.T) {
	for _, tc := range []struct {
		args  []string
		qps   float64
		burst int
	}{
		{[]string{"--insecure"}, 20, 40},
		{[]string{"--insecure", "--kube-api-qps=12.5", "--kube-api-burst=25"}, 12.5, 25},
	} {
		s, err := parseSettings(tc.args, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.validate(); err != nil {
			t.Fatal(err)
		}
		if s.kubeAPIQPS != tc.qps || s.kubeAPIBurst != tc.burst {
			t.Fatalf("rate limits for %v: got %v/%v, want %v/%v", tc.args, s.kubeAPIQPS, s.kubeAPIBurst, tc.qps, tc.burst)
		}
	}
}
