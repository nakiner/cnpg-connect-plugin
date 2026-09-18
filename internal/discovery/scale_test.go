package discovery

import (
	"bytes"
	"fmt"
	"testing"
)

// Store fanout only; real gRPC serialization, sockets and clients cost extra.
// Owned models direct Go subscribers; shared models the production gRPC path.
func BenchmarkStoreFanout(b *testing.B) {
	for _, shared := range []bool{false, true} {
		for _, subscribers := range []int{10, 1000, 20000} {
			b.Run(fmt.Sprintf("shared_%t/subscribers_%d", shared, subscribers), func(b *testing.B) {
				store := NewStore()
				snapshot := sampleSnapshot()
				snapshot.Connection.ServerCAPEM = bytes.Repeat([]byte("public-ca-fixture"), 128)
				store.Put(snapshot)
				cancel := make([]func(), 0, subscribers)
				for range subscribers {
					var stop func()
					if shared {
						_, stop, _ = store.subscribeShared(snapshot.Cluster.Namespace, snapshot.Cluster.Name)
					} else {
						_, stop = store.Subscribe(snapshot.Cluster.Namespace, snapshot.Cluster.Name)
					}
					cancel = append(cancel, stop)
				}
				defer func() {
					for _, stop := range cancel {
						stop()
					}
				}()
				b.ReportAllocs()
				for b.Loop() {
					store.Put(snapshot)
				}
			})
		}
	}
}
