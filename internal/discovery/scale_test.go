package discovery

import (
	"bytes"
	"fmt"
	"testing"
)

// Includes copying CA data and member maps to stalled subscribers. This measures
// Store fanout only; real gRPC serialization, sockets and clients cost extra.
func BenchmarkStoreFanout(b *testing.B) {
	for _, subscribers := range []int{10, 1000, 20000} {
		b.Run(fmt.Sprintf("subscribers_%d", subscribers), func(b *testing.B) {
			store := NewStore()
			snapshot := sampleSnapshot()
			snapshot.Connection.ServerCAPEM = bytes.Repeat([]byte("public-ca-fixture"), 128)
			store.Put(snapshot)
			cancel := make([]func(), 0, subscribers)
			for range subscribers {
				_, stop := store.Subscribe(snapshot.Cluster.Namespace, snapshot.Cluster.Name)
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
