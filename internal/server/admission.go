package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// Limits are per discovery-server process, except MaxConcurrentStreams which is
// per HTTP/2 connection. Zero fields use DefaultLimits; limits cannot be disabled.
type Limits struct {
	MaxConcurrentStreams  int
	MaxConnections        int
	MaxRPCs               int
	MaxWatches            int
	InitialRequestTimeout time.Duration
}

func DefaultLimits() Limits {
	return Limits{MaxConcurrentStreams: 128, MaxConnections: 1024, MaxRPCs: 4096, MaxWatches: 2048, InitialRequestTimeout: 5 * time.Second}
}

func (l Limits) normalized() (Limits, error) {
	defaults := DefaultLimits()
	for _, v := range []struct {
		name     string
		value    *int
		fallback int
	}{
		{"max-concurrent-streams", &l.MaxConcurrentStreams, defaults.MaxConcurrentStreams},
		{"max-discovery-connections", &l.MaxConnections, defaults.MaxConnections},
		{"max-discovery-rpcs", &l.MaxRPCs, defaults.MaxRPCs},
		{"max-discovery-watches", &l.MaxWatches, defaults.MaxWatches},
	} {
		if *v.value == 0 {
			*v.value = v.fallback
		}
		if *v.value < 1 || *v.value > 1_000_000 {
			return l, fmt.Errorf("%s must be between 1 and 1000000", v.name)
		}
	}
	if l.InitialRequestTimeout == 0 {
		l.InitialRequestTimeout = defaults.InitialRequestTimeout
	}
	if l.InitialRequestTimeout < time.Millisecond || l.InitialRequestTimeout > time.Minute {
		return l, fmt.Errorf("initial-request-timeout must be between 1ms and 1m")
	}
	return l, nil
}

func (l Limits) Validate() error { _, err := l.normalized(); return err }

type admission struct {
	connections                              atomic.Int64
	ctx                                      context.Context
	limits                                   Limits
	authorize                                func(context.Context) error
	rpcs, watches                            chan struct{}
	rejectedAuth, rejectedCapacity, timedout atomic.Uint64
}

type admittedRPCKey struct{}
type admittedRPC struct {
	ctx         context.Context
	timer       *time.Timer
	finish      func()
	cancelWrite func()
}

func newAdmission(ctx context.Context, limits Limits, authorize func(context.Context) error) (*admission, error) {
	limits, err := limits.normalized()
	if err != nil {
		return nil, err
	}
	return &admission{ctx: ctx, limits: limits, authorize: authorize, rpcs: make(chan struct{}, limits.MaxRPCs), watches: make(chan struct{}, limits.MaxWatches)}, nil
}

func (a *admission) options() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.MaxRecvMsgSize(64 << 10), grpc.StatsHandler(a)}
}

// Admission runs in the HTTP/2 handler before gRPC creates a deadline context or
// reads the body. Native gRPC tap rejection does not release that deadline in
// the current dependency; the HTTP boundary owns cancellation on every path.
func (a *admission) admit(parent context.Context, method string) (context.Context, error) {
	if err := a.ctx.Err(); err != nil {
		return parent, status.FromContextError(err).Err()
	}
	if err := parent.Err(); err != nil {
		return parent, status.FromContextError(err).Err()
	}
	if a.authorize != nil {
		if err := a.authorize(parent); err != nil {
			a.rejectedAuth.Add(1)
			return parent, err
		}
	}
	select {
	case a.rpcs <- struct{}{}:
	default:
		a.rejectedCapacity.Add(1)
		return parent, status.Error(codes.ResourceExhausted, "discovery RPC capacity exhausted")
	}
	watch := method == connectv1.TopologyService_WatchTopology_FullMethodName
	if watch {
		select {
		case a.watches <- struct{}{}:
		default:
			<-a.rpcs
			a.rejectedCapacity.Add(1)
			return parent, status.Error(codes.ResourceExhausted, "discovery watch capacity exhausted")
		}
	}
	ctx, cancel := context.WithCancel(parent)
	state := &admittedRPC{ctx: ctx}
	state.timer = time.AfterFunc(a.limits.InitialRequestTimeout, func() { a.timedout.Add(1); cancel() })
	stopShutdown := context.AfterFunc(a.ctx, cancel)
	// The HTTP handler is the sole owner of this reservation. Cancellation
	// interrupts I/O; capacity is released only after ServeHTTP has returned.
	state.finish = func() {
		state.timer.Stop()
		stopShutdown()
		cancel()
		if watch {
			<-a.watches
		}
		<-a.rpcs
	}
	return context.WithValue(ctx, admittedRPCKey{}, state), nil
}

func (a *admission) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	state, _ := ctx.Value(admittedRPCKey{}).(*admittedRPC)
	if state == nil {
		return ctx
	}
	// Link admission cancellation to the handler and propagate gRPC's own
	// deadline to the response writer. Context cancellation alone cannot stop
	// a ServeHTTP response blocked on HTTP/2 flow control.
	rpcCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(state.ctx, cancel)
	context.AfterFunc(rpcCtx, func() {
		stop()
		if state.cancelWrite != nil {
			state.cancelWrite()
		}
	})
	if state.ctx.Err() != nil {
		cancel()
	}
	return rpcCtx
}
func (a *admission) HandleRPC(context.Context, stats.RPCStats)                         {}
func (a *admission) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (a *admission) HandleConn(context.Context, stats.ConnStats)                       {}
