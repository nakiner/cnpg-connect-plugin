// Package server owns listeners, transport security, and bounded shutdown.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

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
	PluginAddress    string
	DiscoveryAddress string
	HealthAddress    string
	PluginTLS        *tls.Config
	DiscoveryTLS     *tls.Config
	Version          string
}

// Run starts all listeners before the observer. Any server or observer failure
// shuts the whole process down; startup cannot leave a partially live plugin.
func Run(ctx context.Context, options Options, observer Observer, registerDiscovery func(*grpc.Server), logger *slog.Logger) error {
	pluginListener, err := net.Listen("tcp", options.PluginAddress)
	if err != nil {
		return fmt.Errorf("listen for CNPG-I: %w", err)
	}
	defer pluginListener.Close()
	discoveryListener, err := net.Listen("tcp", options.DiscoveryAddress)
	if err != nil {
		return fmt.Errorf("listen for discovery API: %w", err)
	}
	defer discoveryListener.Close()
	healthListener, err := net.Listen("tcp", options.HealthAddress)
	if err != nil {
		return fmt.Errorf("listen for health probes: %w", err)
	}
	defer healthListener.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	pluginServer := grpc.NewServer(grpcOptions(runCtx, options.PluginTLS)...)
	plugin.Register(pluginServer, options.Version, observer)
	discoveryServer := grpc.NewServer(grpcOptions(runCtx, options.DiscoveryTLS)...)
	registerDiscovery(discoveryServer)
	healthServer := &http.Server{
		Handler:           HealthHandler(observer.Ready),
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
	start("discovery API", func() error { return discoveryServer.Serve(discoveryListener) })
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
	shutdown.Go(func() {
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			_ = healthServer.Close()
		}
	})
	for _, srv := range []*grpc.Server{pluginServer, discoveryServer} {
		shutdown.Go(func() {
			done := make(chan struct{})
			go func() { srv.GracefulStop(); close(done) }()
			select {
			case <-done:
			case <-shutdownCtx.Done():
				srv.Stop()
			}
		})
	}
	shutdown.Wait()
	// All listeners have now been stopped, including forced transport closure
	// at the deadline. Only the observer can still be doing unbounded work.
	// Check it before the expired deadline so successful forced closure cannot
	// race an already-completed observer and become a false shutdown failure.
	select {
	case <-observerDone:
		workers.Wait()
		return result
	default:
	}
	select {
	case <-observerDone:
		workers.Wait()
	case <-shutdownCtx.Done():
		if result == nil {
			result = fmt.Errorf("shutdown timed out: %w", shutdownCtx.Err())
		}
	}
	return result
}

func grpcOptions(runCtx context.Context, tlsConfig *tls.Config) []grpc.ServerOption {
	options := []grpc.ServerOption{
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

func HealthHandler(ready func() bool) http.Handler {
	mux := http.NewServeMux()
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
