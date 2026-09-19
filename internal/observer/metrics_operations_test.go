package observer

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestMetricsCacheFailuresAndSaturation(t *testing.T) {
	o, _, _ := fakeObserver(t)
	o.opts.MaxConcurrency = 16
	o.configureProbeLimits()
	c, _, _ := fixture()
	// Missing CA metadata and named Secret failures must be counted without names.
	if _, err := o.cachedConnection(context.Background(), c); err == nil {
		t.Fatal("expected missing CA")
	}
	if _, err := o.readPublicCA(context.Background(), "secret-namespace", "secret-name"); err == nil {
		t.Fatal("expected missing secret")
	}
	observe(t, o)
	observe(t, o)
	o.opts.MaxConcurrency = 1
	o.configureProbeLimits()
	release, err := o.acquireProbe(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err = o.acquireProbe(ctx, false); err == nil {
		t.Fatal("expected saturated timeout")
	}
	release()
	o.queue.Add(clusterKey{"test", "db"})
	var b bytes.Buffer
	o.WriteMetrics(&b)
	for _, want := range []string{"cnpg_connect_ca_cache_hits_total 2\n", "cnpg_connect_ca_cache_misses_total 1\n", "cnpg_connect_ca_failures_total 1\n", "cnpg_connect_ca_read_failures_total 1\n", "cnpg_connect_probe_slot_saturation_total 1\n", "cnpg_connect_probe_slot_cancellations_total 1\n", "cnpg_connect_observation_queue_depth 1\n", "cnpg_connect_metadata_watch_up{resource=\"clusters\"} 1\n"} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("missing %q: %s", want, b.String())
		}
	}
	if strings.Contains(b.String(), "secret-") {
		t.Fatal("secret identity leaked")
	}
}
