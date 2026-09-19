package observer

import (
	"fmt"
	"io"

	"github.com/VictoriaMetrics/metrics"
)

// Fixed buckets and no identity labels keep memory and scrape size bounded.
var durationBounds = []float64{.001, .01, .05, .1, .5, 1, 5, 10, 30}

func (o *Observer) initializeMetrics() {
	o.metrics = metrics.NewSet()
	o.observationDuration = o.metrics.NewPrometheusHistogramExt("cnpg_connect_observation_duration_seconds", durationBounds)
	o.probeDuration = o.metrics.NewPrometheusHistogramExt("cnpg_connect_probe_duration_seconds", durationBounds)
	o.probeQueueDuration = o.metrics.NewPrometheusHistogramExt("cnpg_connect_probe_queue_seconds", durationBounds)
}

// WriteMetrics performs no network I/O or work scheduling. Never hold observer
// state locks while writing to an arbitrary (possibly slow) scrape writer.
func (o *Observer) WriteMetrics(w io.Writer) {
	o.store.WriteMetrics(w)
	o.metrics.WritePrometheus(w)
	set := metrics.NewSet()
	set.NewGauge("cnpg_connect_probes_active", nil).Set(float64(len(o.probeSlots)))
	for _, metric := range []struct {
		name  string
		value uint64
	}{
		{"ca_cache_hits", o.caHits.Load()}, {"ca_cache_misses", o.caMisses.Load()},
		{"metadata_watch_starts", o.watchStarts.Load()}, {"metadata_watch_ends", o.watchEnds.Load()}, {"metadata_watch_errors", o.watchErrors.Load()},
		{"ca_failures", o.caFailures.Load()}, {"ca_read_failures", o.caReadFailures.Load()},
		{"probe_slot_saturation", o.probeSaturation.Load()}, {"probe_slot_cancellations", o.probeCancellations.Load()},
	} {
		set.NewCounter("cnpg_connect_" + metric.name + "_total").Set(metric.value)
	}
	set.NewGauge("cnpg_connect_observation_queue_depth", nil).Set(float64(o.queue.Len()))
	for _, metric := range []struct {
		name string
		up   bool
	}{{"clusters", o.clusterWatch.Load()}, {"pods", o.podWatch.Load()}, {"secrets", o.secretWatch.Load()}} {
		value := 0.0
		if metric.up {
			value = 1
		}
		set.NewGauge(fmt.Sprintf("cnpg_connect_metadata_watch_up{resource=%q}", metric.name), nil).Set(value)
	}
	set.WritePrometheus(w)
}
