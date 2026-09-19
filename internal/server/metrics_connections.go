package server

import (
	"net"
	"sync"
	"sync/atomic"
)

// Count accepted sockets, including those awaiting TLS/HTTP2 handshakes.
// Wrap the limiting listener so sockets still in its backlog aren't counted.
func (a *admission) trackConnections(listener net.Listener) net.Listener {
	return &countedListener{Listener: listener, active: &a.connections}
}

type countedListener struct {
	net.Listener
	active *atomic.Int64
}

func (l *countedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.active.Add(1)
	return &countedConn{Conn: conn, active: l.active}, nil
}

type countedConn struct {
	net.Conn
	active *atomic.Int64
	once   sync.Once
	err    error
}

func (c *countedConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); c.active.Add(-1) })
	return c.err
}
