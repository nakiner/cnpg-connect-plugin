package server

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHealthMetricsAdmission(t *testing.T) {
	a, err := newAdmission(context.Background(), Limits{MaxRPCs: 1, InitialRequestTimeout: 100 * time.Millisecond}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := a.admit(context.Background(), "/private/method")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.admit(context.Background(), ""); err == nil {
		t.Fatal("capacity bypass")
	}
	var b bytes.Buffer
	a.WriteMetrics(&b)
	if !strings.Contains(b.String(), "cnpg_connect_admission_in_use{kind=\"rpcs\"} 1\n") {
		t.Fatal(b.String())
	}
	<-ctx.Done()
	ctx.Value(admittedRPCKey{}).(*admittedRPC).finish()
	waitAdmissionEmpty(t, a)
	handler := HealthHandler(func() bool { return false }, func(w io.Writer) { a.WriteMetrics(w) })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 || !strings.Contains(response.Header().Get("Content-Type"), "version=0.0.4") {
		t.Fatal(response)
	}
	for _, want := range []string{"cnpg_connect_admission_rejections_total{reason=\"capacity\"} 1\n", "cnpg_connect_admission_initial_timeouts_total 1\n", "cnpg_connect_admission_in_use{kind=\"rpcs\"} 0\n"} {
		if !strings.Contains(response.Body.String(), want) {
			t.Fatalf("missing %q: %s", want, response.Body.String())
		}
	}
	if strings.Contains(response.Body.String(), "private") {
		t.Fatal("method leaked")
	}
}
func waitAdmissionEmpty(t *testing.T, a *admission) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(a.rpcs) == 0 && len(a.watches) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("admission slot leaked")
}
