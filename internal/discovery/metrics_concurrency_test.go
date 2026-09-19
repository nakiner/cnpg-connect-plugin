package discovery

import (
	"bytes"
	"fmt"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockedMetricWriter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (w *blockedMetricWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return len(p), nil
}
func TestMetricsSlowScrapeDoesNotHoldStoreLock(t *testing.T) {
	s := NewStore()
	w := &blockedMetricWriter{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { s.WriteMetrics(w); close(done) }()
	<-w.entered
	mutation := make(chan struct{})
	go func() {
		s.Put(v1.Snapshot{Cluster: v1.ClusterRef{Name: "db"}, ValidUntil: time.Now().Add(time.Hour)})
		close(mutation)
	}()
	select {
	case <-mutation:
	case <-time.After(time.Second):
		close(w.release)
		<-done
		t.Fatal("scrape held store lock")
	}
	close(w.release)
	<-done
}
func TestMetricsCardinalityAndConcurrentChurn(t *testing.T) {
	s := NewStore()
	var before, after bytes.Buffer
	s.WriteMetrics(&before)
	var workers sync.WaitGroup
	for worker := range 4 {
		workers.Go(func() {
			for i := range 100 {
				name := fmt.Sprintf("private-%d-%d", worker, i)
				s.Put(v1.Snapshot{Cluster: v1.ClusterRef{Name: name}, ValidUntil: time.Now().Add(time.Hour)})
				_, cancel := s.Subscribe("", name)
				s.WriteMetrics(io.Discard)
				cancel()
			}
		})
	}
	workers.Wait()
	s.WriteMetrics(&after)
	if strings.Count(before.String(), "\n") != strings.Count(after.String(), "\n") {
		t.Fatal("metric cardinality grew with inventory")
	}
	if strings.Contains(after.String(), "private-") {
		t.Fatal("identity leaked")
	}
}
