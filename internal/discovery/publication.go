package discovery

import (
	"sync"
	"sync/atomic"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
)

// A publication owns one immutable observation. All gRPC watchers share its
// lazily constructed protobuf; serialization never holds the store mutex.
type publication struct {
	snapshot v1.Snapshot
	order    uint64
	wireOnce sync.Once
	wire     *connectv1.Snapshot
}

func (p *publication) protobuf() *connectv1.Snapshot {
	p.wireOnce.Do(func() { p.wire = toProto(p.snapshot) })
	return p.wire
}

type notification struct {
	publication *publication
	watchers    []*subscription
}

func (n notification) deliver() {
	for _, watcher := range n.watchers {
		watcher.deliver(n.publication)
	}
}

// A subscription has exactly one pending publication. Local Go subscribers get
// owned copies; the gRPC path shares immutable publications. Its mutex protects
// mailbox replacement and close, and never covers a network write.
type subscription struct {
	coalesced    *atomic.Uint64
	mu           sync.Mutex
	order        uint64
	closed       bool
	snapshots    chan v1.Snapshot
	publications chan *publication
}

func (s *subscription) deliver(published *publication) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || published.order <= s.order {
		return
	}
	s.order = published.order
	var replaced bool
	if s.publications != nil {
		replaced = replacePending(s.publications, published)
	} else {
		replaced = replacePending(s.snapshots, clone(published.snapshot))
	}
	if replaced && s.coalesced != nil {
		s.coalesced.Add(1)
	}
}

func (s *subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.publications != nil {
		close(s.publications)
	} else {
		close(s.snapshots)
	}
}

// Only the subscription owns sends and close. A concurrent receiver can drain
// the slot, but cannot fill it, so sending after this drain never blocks.
func replacePending[T any](updates chan T, latest T) bool {
	replaced := false
	select {
	case <-updates:
		replaced = true
	default:
	}
	updates <- latest
	return replaced
}
