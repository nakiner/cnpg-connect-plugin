package discovery

import (
	"fmt"
	"io"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func init() { metrics.ExposeMetadata(true) }

// WriteMetrics aggregates metadata only, without copying payloads, expiring
// records, refreshing demand, or writing while holding the store lock.
func (s *Store) WriteMetrics(w io.Writer) {
	now := time.Now()
	var states [3]int
	var watchers int
	var oldest float64
	s.mu.Lock()
	for _, record := range s.records {
		state := 1
		if record.expired || !now.Before(record.snapshot.ValidUntil) {
			state = 2
		} else if record.snapshot.Available {
			state = 0
		}
		states[state]++
		if !record.snapshot.ObservedAt.IsZero() {
			oldest = max(oldest, now.Sub(record.snapshot.ObservedAt).Seconds())
		}
	}
	for _, subscriptions := range s.subscribers {
		watchers += len(subscriptions)
	}
	publications, expirations := s.publicationOrder, s.expirations
	s.mu.Unlock()
	// Each scrape owns its values, so concurrent scrapes cannot mix snapshots.
	// WritePrometheus buffers exposition before writing to a slow client.
	set := metrics.NewSet()
	for i, state := range []string{"available", "unavailable", "expired"} {
		set.NewGauge(fmt.Sprintf("cnpg_connect_snapshots{state=%q}", state), nil).Set(float64(states[i]))
	}
	set.NewGauge("cnpg_connect_watchers", nil).Set(float64(watchers))
	set.NewGauge("cnpg_connect_snapshot_oldest_age_seconds", nil).Set(oldest)
	for _, metric := range []struct {
		name  string
		value uint64
	}{{"publications", publications}, {"snapshot_expirations", expirations}, {"watch_coalesced", s.coalesced.Load()}} {
		set.NewCounter("cnpg_connect_" + metric.name + "_total").Set(metric.value)
	}
	set.WritePrometheus(w)
}
