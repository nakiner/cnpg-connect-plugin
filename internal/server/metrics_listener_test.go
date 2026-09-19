package server

import (
	"context"
	"google.golang.org/grpc"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type metricsObserver struct{ testObserver }

func (metricsObserver) WriteMetrics(w io.Writer) { io.WriteString(w, "cnpg_connect_test_observer 1\n") }
func TestRunMetricsOnlyOnHealthListener(t *testing.T) {
	addresses := make([]string, 3)
	for i := range addresses {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addresses[i] = l.Addr().String()
		l.Close()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{PluginAddress: addresses[0], DiscoveryAddress: addresses[1], HealthAddress: addresses[2]}, metricsObserver{testObserver{started: started}}, func(*grpc.Server) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("shutdown stalled")
		}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("startup stalled")
	}
	client := &http.Client{Timeout: time.Second}
	for i, address := range addresses {
		response, err := client.Get("http://" + address + "/metrics")
		if err != nil {
			if i == 2 {
				t.Fatal(err)
			}
			continue
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if i != 2 {
			if strings.Contains(string(body), "cnpg_connect_") {
				t.Fatal("public metrics exposed")
			}
			continue
		}
		for _, want := range []string{"cnpg_connect_test_observer 1", "cnpg_connect_admission_limit{kind=\"rpcs\"}"} {
			if !strings.Contains(string(body), want) {
				t.Fatalf("missing %q: %s", want, body)
			}
		}
	}
}
