package observer

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestOperationalMetricsHaveBoundedLabels(t *testing.T) {
	o, _, _ := fakeObserver(t)
	defer o.queue.ShutDown()
	o.probe = func(context.Context, *corev1.Pod, v1.ConnectionParameters, string) (instanceStatus, error) {
		primary := true
		return instanceStatus{IsPrimary: &primary, SystemID: "system", Timeline: 1}, nil
	}
	o.observe(context.Background(), clusterKey{"test", "db"})
	var output bytes.Buffer
	o.WriteMetrics(&output)
	text := output.String()
	for _, want := range []string{"cnpg_connect_observation_duration_seconds_count 1", "cnpg_connect_probes_active 0", "cnpg_connect_probe_duration_seconds_count 3", "cnpg_connect_probe_queue_seconds_count 3"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in metrics:\n%s", want, text)
		}
	}
	if strings.Contains(text, "namespace=") || strings.Contains(text, "cluster=") || strings.Contains(text, "pod=") {
		t.Fatal("metrics contain unbounded identity labels")
	}
	for _, bound := range []string{"0.001", "0.01", "0.05", "0.1", "0.5", "1", "5", "10", "30", "+Inf"} {
		if !strings.Contains(text, fmt.Sprintf("cnpg_connect_observation_duration_seconds_bucket{le=%q}", bound)) {
			t.Fatalf("missing classic histogram bucket %s", bound)
		}
	}
	if !strings.Contains(text, "# TYPE cnpg_connect_observation_duration_seconds histogram\n") {
		t.Fatal("histogram type metadata missing")
	}
	other, _, _ := fakeObserver(t)
	defer other.queue.ShutDown()
	output.Reset()
	other.WriteMetrics(&output)
	if !strings.Contains(output.String(), "cnpg_connect_observation_duration_seconds_count 0\n") {
		t.Fatal("histogram state leaked between observers")
	}
}
