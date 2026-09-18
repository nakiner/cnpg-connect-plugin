package observer

import (
	"errors"
	"strings"
	"testing"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func fixture() (*unstructured.Unstructured, []corev1.Pod, []statusResult) {
	cluster := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster",
		"metadata": map[string]any{"name": "db", "namespace": "test", "uid": "cluster-uid", "generation": int64(1)},
		"spec":     map[string]any{"plugins": []any{map[string]any{"name": config.PluginName}}},
		"status":   map[string]any{"currentPrimary": "db-1", "targetPrimary": "db-1"},
	}}
	var pods []corev1.Pod
	for _, name := range []string{"db-1", "db-2", "db-3"} {
		pods = append(pods, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test", UID: types.UID(name + "-uid"), Labels: map[string]string{"cnpg.io/cluster": "db"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "postgresql.cnpg.io/v1", Kind: "Cluster", Name: "db", UID: "cluster-uid"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0." + string(name[len(name)-1]), Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}})
	}
	primary, standby := true, false
	results := []statusResult{
		{status: instanceStatus{IsPrimary: &primary, SystemID: "12345", Timeline: 1, Replication: []replicationStatus{{ApplicationName: "db-2", State: "streaming", SyncState: "sync"}, {ApplicationName: "db-3", State: "streaming", SyncState: "async"}}}},
		{status: instanceStatus{IsPrimary: &standby, SystemID: "12345", Timeline: 1, WalReceiverActive: true}},
		{status: instanceStatus{IsPrimary: &standby, SystemID: "12345", Timeline: 1, WalReceiverActive: true}},
	}
	return cluster, pods, results
}

func TestBuildSnapshotClassifiesObservedStates(t *testing.T) {
	for _, state := range []string{"sync", "quorum", "potential", "async"} {
		t.Run(state, func(t *testing.T) {
			cluster, pods, results := fixture()
			results[0].status.Replication[0].SyncState = state
			got := buildSnapshot(cluster, pods, results, config.Parameters{}, time.Now(), 15*time.Second)
			if !got.Available || got.PrimaryID != "db-1-uid" || !got.Members[1].Ready || got.Members[1].SyncState != state {
				t.Fatalf("bad topology: %+v", got)
			}
			if got.Members[0].Endpoints["internal"].ServerName != "db-rw.test.svc" {
				t.Fatal("missing TLS identity")
			}
		})
	}
}

func TestExternalEndpointsInheritPostgresCertificateName(t *testing.T) {
	cluster, pods, results := fixture()
	params := config.Parameters{ExternalEndpoints: map[string]v1.Endpoint{
		"db-1": {Host: "primary.example.test", Port: 5432},
		"db-2": {Host: "standby.example.test", Port: 5432, ServerName: "custom.example.test"},
	}}
	got := buildSnapshot(cluster, pods, results, params, time.Now(), 15*time.Second)
	if got.Members[0].Endpoints["external"].ServerName != "db-rw.test.svc" || got.Members[1].Endpoints["external"].ServerName != "custom.example.test" {
		t.Fatal("external PostgreSQL TLS identities did not use discovered defaults or explicit override")
	}
}

func TestBuildSnapshotRejectsUnsafePrimary(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*unstructured.Unstructured, []corev1.Pod, []statusResult)
	}{
		{"transition", func(c *unstructured.Unstructured, _ []corev1.Pod, _ []statusResult) {
			_ = unstructured.SetNestedField(c.Object, "db-2", "status", "targetPrimary")
		}},
		{"missing primary", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) { r[0].err = errors.New("timeout") }},
		{"two primaries", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) {
			value := true
			r[1].status.IsPrimary = &value
		}},
		{"not ready", func(_ *unstructured.Unstructured, p []corev1.Pod, _ []statusResult) {
			p[0].Status.Conditions[0].Status = corev1.ConditionFalse
		}},
		{"fenced", func(c *unstructured.Unstructured, _ []corev1.Pod, _ []statusResult) {
			c.SetAnnotations(map[string]string{"cnpg.io/fencedInstances": `["db-1"]`})
		}},
		{"all fenced", func(c *unstructured.Unstructured, _ []corev1.Pod, _ []statusResult) {
			c.SetAnnotations(map[string]string{"cnpg.io/fencedInstances": `["*"]`})
		}},
		{"malformed fence", func(c *unstructured.Unstructured, _ []corev1.Pod, _ []statusResult) {
			c.SetAnnotations(map[string]string{"cnpg.io/fencedInstances": `not-json`})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, p, r := fixture()
			tt.mutate(c, p, r)
			got := buildSnapshot(c, p, r, config.Parameters{}, time.Now(), 15*time.Second)
			if got.PrimaryID != "" || got.Available {
				t.Fatalf("unsafe primary: %+v", got)
			}
			for _, m := range got.Members {
				if m.Ready {
					t.Fatalf("unsafe routing: %+v", m)
				}
			}
		})
	}
}

func TestBuildSnapshotRejectsUnsafeReplica(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*unstructured.Unstructured, []corev1.Pod, []statusResult)
	}{
		{"paused", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) { r[1].status.ReplayPaused = true }},
		{"no receiver", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) {
			r[1].status.WalReceiverActive = false
		}},
		{"other database", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) { r[1].status.SystemID = "98765" }},
		{"not streaming", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) {
			r[0].status.Replication[0].State = "catchup"
		}},
		{"unknown sync state", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) {
			r[0].status.Replication[0].SyncState = "new-future-state"
		}},
		{"missing status", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) {
			r[1].err = errors.New("unreachable")
		}},
		{"duplicate sender", func(_ *unstructured.Unstructured, _ []corev1.Pod, r []statusResult) {
			r[0].status.Replication = append(r[0].status.Replication, r[0].status.Replication[0])
		}},
		{"fenced", func(c *unstructured.Unstructured, _ []corev1.Pod, _ []statusResult) {
			c.SetAnnotations(map[string]string{"cnpg.io/fencedInstances": `["db-2"]`})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, p, r := fixture()
			tt.mutate(c, p, r)
			got := buildSnapshot(c, p, r, config.Parameters{}, time.Now(), 15*time.Second)
			if !got.Available || got.PrimaryID == "" {
				t.Fatal("lost healthy primary")
			}
			if got.Members[1].Ready || got.Members[1].SyncState != "unknown" {
				t.Fatalf("unsafe replica: %+v", got.Members[1])
			}
		})
	}
}

func TestBuildSnapshotAcceptsStreamingStandbyWithOlderCheckpointTimeline(t *testing.T) {
	c, p, r := fixture()
	r[0].status.Timeline = 2
	got := buildSnapshot(c, p, r, config.Parameters{}, time.Now(), 15*time.Second)
	if !got.Available || !got.Members[1].Ready || got.Members[1].SyncState != "sync" || !got.Members[2].Ready {
		t.Fatalf("excluded healthy streaming replicas after promotion: %+v", got)
	}
	if got.Members[1].Timeline != 1 {
		t.Fatal("checkpoint timeline diagnostic was altered")
	}
}

func TestDecodeStatusRejectsMissingAndMalformedData(t *testing.T) {
	for _, raw := range []string{`{}`, `null`, `{"systemID":"123","timeLineID":1}`, `{"isPrimary":true,"systemID":"123","timeLineID":1} {}`, `{"isPrimary":true,"systemID":"123","timeLineID":0}`} {
		if _, err := decodeStatus(strings.NewReader(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if _, err := decodeStatus(strings.NewReader(`{"isPrimary":false,"systemID":"123","timeLineID":1,"futureField":true}`)); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticObservationAndExplicitOptOut(t *testing.T) {
	c, _, _ := fixture()
	if enabled, _, err := parameters(c); !enabled || err != nil {
		t.Fatalf("enabled=%v err=%v", enabled, err)
	}
	c.SetAnnotations(map[string]string{config.EnabledAnnotation: "false"})
	if enabled, _, _ := parameters(c); enabled {
		t.Fatal("native registration overrode explicit observation opt-out")
	}
	c.SetAnnotations(nil)
	_ = unstructured.SetNestedSlice(c.Object, []any{map[string]any{"name": config.PluginName, "enabled": false}}, "spec", "plugins")
	if enabled, _, _ := parameters(c); enabled {
		t.Fatal("observed explicitly disabled cluster")
	}
	c.SetAnnotations(map[string]string{config.EnabledAnnotation: "true"})
	if enabled, _, _ := parameters(c); enabled {
		t.Fatal("annotation overrode explicit disabled plugin")
	}
	unstructured.RemoveNestedField(c.Object, "spec", "plugins")
	c.SetAnnotations(nil)
	if enabled, _, err := parameters(c); !enabled || err != nil {
		t.Fatalf("automatic observation failed: enabled=%v err=%v", enabled, err)
	}
	c.SetAnnotations(map[string]string{config.EnabledAnnotation: "false"})
	if enabled, _, _ := parameters(c); enabled {
		t.Fatal("observed annotation-disabled cluster")
	}
	c.SetAnnotations(map[string]string{config.ParametersAnnotation: `{"serverName":"custom.example.test"}`})
	if enabled, p, err := parameters(c); !enabled || err != nil || p.ServerName != "custom.example.test" {
		t.Fatalf("annotation parameters failed: %+v %v", p, err)
	}
	c.SetAnnotations(map[string]string{config.ParametersAnnotation: `not-json`})
	if enabled, _, err := parameters(c); !enabled || err == nil {
		t.Fatal("malformed annotation did not fail closed")
	}
}
