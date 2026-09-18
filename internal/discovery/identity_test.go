package discovery

import (
	"testing"
	"time"
)

func TestClusterIdentityIsIndependentAndDoesNotCreateDemand(t *testing.T) {
	store := NewStore()
	snapshot := sampleSnapshot()
	store.Put(snapshot)
	store.SetDemandHandler(func(string, string) {
		t.Fatal("identity lookup created observation demand")
	}, time.Minute)
	identity, exists := store.ClusterIdentity("database", "postgres")
	if !exists || identity != snapshot.Cluster {
		t.Fatalf("unexpected identity: %+v, exists=%t", identity, exists)
	}
	identity.UID = "changed by caller"
	again, _ := store.ClusterIdentity("database", "postgres")
	if again != snapshot.Cluster || store.HasDemand("database", "postgres") {
		t.Fatal("identity lookup changed stored state")
	}
	if _, exists := store.ClusterIdentity("database", "unknown"); exists {
		t.Fatal("unknown identity exists")
	}
	store.Delete("database", "postgres")
	if _, exists := store.ClusterIdentity("database", "postgres"); exists {
		t.Fatal("deleted identity remains visible")
	}
}
