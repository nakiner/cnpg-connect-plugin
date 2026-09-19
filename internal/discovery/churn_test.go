package discovery

import (
	"testing"
	"time"
)

func TestWatchChurnSharesActiveUnarySchedule(t *testing.T) {
	store := NewStore()
	store.Put(sampleSnapshot())
	calls := 0
	store.SetDemandHandler(func(string, string) { calls++ }, time.Minute)
	store.RequestRefresh("database", "postgres")
	for range 1000 {
		_, cancel := store.Subscribe("database", "postgres")
		cancel()
	}
	if calls != 1 {
		t.Fatalf("watch churn scheduled %d refreshes under one unary lease", calls)
	}
}
