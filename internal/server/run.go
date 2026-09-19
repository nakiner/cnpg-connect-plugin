// Package server owns listeners, transport security, and bounded shutdown.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/nakiner/cnpg-connect-plugin/internal/plugin"
)

type Observer interface {
	plugin.Notifier
	Run(context.Context) error
	Ready() bool
}

type Options struct {
	PluginAddress      string
	DiscoveryAddress   string
	HealthAddress      string
	PluginTLS          *tls.Config
	DiscoveryTLS       *tls.Config
	Version            string
	Limits             Limits
	AuthorizeDiscovery func(context.Context) error
}

// Run starts all listeners before the observer. Any server or observer failure
// shuts the whole process down; startup cannot leave a partially live plugin.
func Run(ctx context.Context, options Options, observer Observer, registerDiscovery func(*grpc.Server), logger *slog.Logger) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	admission, err := newAdmission(runCtx, options.Limits, options.AuthorizeDiscovery)
	if err != nil {
		return err
	}
	pluginListener, err := net.Listen("tcp", options.PluginAddress)
	if err != nil {
		return fmt.Errorf("listen for CNPG-I: %w", err)
	}
	defer pluginListener.Close()
	discoveryListener, err := net.Listen("tcp", options.DiscoveryAddress)
	if err != nil {
		return fmt.Errorf("listen for discovery API: %w", err)
	}
	// Bound accepted sockets before TLS or HTTP/2 handshakes allocate state.
	// Excess connections remain in the kernel backlog, not Go goroutines.
	discoveryListener = netutil.LimitListener(discoveryListener, admission.limits.MaxConnections)
	discoveryListener = admission.trackConnections(discoveryListener)
	defer discoveryListener.Close()
	healthListener, err := net.Listen("tcp", options.HealthAddress)
	if err != nil {
		return fmt.Errorf("listen for health probes: %w", err)
	}
	defer healthListener.Close()

	pluginServer := grpc.NewServer(grpcOptions(runCtx, options.PluginTLS)...)
	plugin.Register(pluginServer, options.Version, observer)
	// Discovery's HTTP handler already owns cancellation, size limits and
	// connection lifetime. Native transport options apply only to CNPG-I.
	discoveryServer := grpc.NewServer(admission.options()...)
	registerDiscovery(discoveryServer)
	discoveryHTTP := discoveryHTTPServer(admission, discoveryServer, options.DiscoveryTLS)
	healthServer := &http.Server{
		Handler: HealthHandler(observer.Ready, func(w io.Writer) {
			admission.WriteMetrics(w)
			if metrics, ok := observer.(interface{ WriteMetrics(io.Writer) }); ok {
				metrics.WriteMetrics(w)
			}
		}),
		ReadHeaderTimeout: 3 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    4 << 10,
	}
	healthServer.BaseContext = func(net.Listener) context.Context { return runCtx }

	errorsCh := make(chan error, 4)
	observerDone := make(chan struct{})
	var workers sync.WaitGroup
	start := func(name string, run func() error) {
		workers.Go(func() {
			err := run()
			if errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) {
				err = nil
			}
			if err == nil && runCtx.Err() == nil {
				err = fmt.Errorf("stopped unexpectedly")
			}
			if err != nil {
				err = fmt.Errorf("%s: %w", name, err)
			}
			errorsCh <- err
		})
	}
	start("CNPG-I server", func() error { return pluginServer.Serve(pluginListener) })
	start("discovery API", func() error {
		if options.DiscoveryTLS != nil {
			return discoveryHTTP.ServeTLS(discoveryListener, "", "")
		}
		return discoveryHTTP.Serve(discoveryListener)
	})
	start("health server", func() error { return healthServer.Serve(healthListener) })
	start("observer", func() error {
		defer close(observerDone)
		return observer.Run(runCtx)
	})
	logger.Info("listeners started", "plugin", pluginListener.Addr(), "discovery", discoveryListener.Addr(), "health", healthListener.Addr())

	var result error
	select {
	case <-ctx.Done():
	case result = <-errorsCh:
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	var shutdown sync.WaitGroup
	var forced atomic.Bool
	shutdown.Go(func() {
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			_ = healthServer.Close()
		}
	})
	shutdown.Go(func() {
		if err := discoveryHTTP.Shutdown(shutdownCtx); err != nil {
			_ = discoveryHTTP.Close()
		}
	})
	for _, srv := range []*grpc.Server{pluginServer, discoveryServer} {
		shutdown.Go(func() {
			done := make(chan struct{})
			go func() { srv.GracefulStop(); close(done) }()
			select {
			case <-done:
			case <-shutdownCtx.Done():
				forced.Store(true)
				// gRPC GracefulStop can hold its internal mutex while waiting
				// for a handler that ignores cancellation. Stop may then block
				// on the same mutex, so it cannot hold up our shutdown deadline.
				go srv.Stop()
			}
		})
	}
	shutdown.Wait()
	// Listeners have stopped and forced transport closure has been requested
	// where needed. A forced gRPC stop can still wait on an uncooperative handler;
	// its Serve worker must not extend the deadline either. Check observerDone
	// first so a completed observer does not race the expired deadline and become
	// a false shutdown failure.
	select {
	case <-observerDone:
		if !forced.Load() {
			workers.Wait()
		}
		return result
	default:
	}
	select {
	case <-observerDone:
		if !forced.Load() {
			workers.Wait()
		}
	case <-shutdownCtx.Done():
		if result == nil {
			result = fmt.Errorf("shutdown timed out: %w", shutdownCtx.Err())
		}
	}
	return result
}

func grpcOptions(runCtx context.Context, tlsConfig *tls.Config) []grpc.ServerOption {
	options := []grpc.ServerOption{
		// Idle watchers share write buffers instead of retaining one per socket.
		grpc.SharedWriteBuffer(true),
		grpc.MaxRecvMsgSize(4 << 20),
		grpc.ConnectionTimeout(5 * time.Second),
		grpc.StreamInterceptor(func(service any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			// GracefulStop waits for existing streams; it does not cancel their
			// contexts. Give watchers the process cancellation signal so they
			// can finish before the forced-stop deadline. The original stream
			// context still supplies client cancellation, deadlines and values.
			ctx, cancel := context.WithCancel(stream.Context())
			stop := context.AfterFunc(runCtx, cancel)
			defer stop()
			defer cancel()
			return handler(service, &shutdownStream{ServerStream: stream, ctx: ctx})
		}),
		// Reconnect periodically so certificate/trust rotation applies even to
		// long-lived connections. Discovery clients must resubscribe on EOF.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      5 * time.Minute,
			MaxConnectionAgeGrace: 30 * time.Second,
		}),
	}
	if tlsConfig != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	return options
}

type shutdownStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *shutdownStream) Context() context.Context { return s.ctx }

func HealthHandler(ready func() bool, metrics ...func(io.Writer)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Err() != nil {
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		for _, write := range metrics {
			write(w)
		}
	})
	live := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	}
	mux.HandleFunc("GET /healthz", live)
	mux.HandleFunc("GET /livez", live)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready() {
			http.Error(w, "initial topology collection pending", http.StatusServiceUnavailable)
			return
		}
		live(w, r)
	})
	return mux
}
