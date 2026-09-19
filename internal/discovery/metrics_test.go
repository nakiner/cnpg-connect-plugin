package discovery

import (
	"bytes"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"strings"
	"testing"
	"time"
)

func TestStoreMetricsFreshnessCoalescingAndStates(t *testing.T) {
	s := NewStore()
	now := time.Now()
	snapshot := v1.Snapshot{Cluster: v1.ClusterRef{Namespace: "private-ns", Name: "private-db"}, Available: true, ObservedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Hour)}
	s.Put(snapshot)
	_, cancel := s.Subscribe("private-ns", "private-db")
	s.Put(snapshot)
	var b bytes.Buffer
	s.WriteMetrics(&b)
	for _, want := range []string{"cnpg_connect_watchers 1\n", "cnpg_connect_snapshots{state=\"available\"} 1\n", "cnpg_connect_publications_total 2\n", "cnpg_connect_watch_coalesced_total 1\n", "cnpg_connect_snapshot_oldest_age_seconds "} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q: %s", want, b.String())
		}
	}
	if strings.Contains(b.String(), "private-") {
		t.Fatal("identity leak")
	}
	s.Expire(now.Add(2 * time.Hour))
	cancel()
	b.Reset()
	s.WriteMetrics(&b)
	for _, want := range []string{"cnpg_connect_watchers 0\n", "cnpg_connect_snapshots{state=\"expired\"} 1\n", "cnpg_connect_snapshot_expirations_total 1\n", "cnpg_connect_watch_coalesced_total 2\n"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q: %s", want, b.String())
		}
	}
}
