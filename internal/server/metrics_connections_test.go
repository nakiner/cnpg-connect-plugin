package server

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
)

func TestAdmissionConnectionUsageIncludesPreHandshake(t *testing.T) {
	a, err := newAdmission(context.Background(), Limits{MaxConnections: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	listener := a.trackConnections(raw)
	client, err := net.Dial("tcp", raw.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var b bytes.Buffer
	a.WriteMetrics(&b)
	for _, want := range []string{"cnpg_connect_admission_in_use{kind=\"connections\"} 1\n", "cnpg_connect_admission_limit{kind=\"connections\"} 2\n"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q: %s", want, b.String())
		}
	}
	conn.Close()
	conn.Close()
	b.Reset()
	a.WriteMetrics(&b)
	if !strings.Contains(b.String(), "cnpg_connect_admission_in_use{kind=\"connections\"} 0\n") {
		t.Fatal(b.String())
	}
}
