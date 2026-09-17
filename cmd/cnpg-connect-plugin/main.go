package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"github.com/nakiner/cnpg-connect-plugin/internal/discovery"
	"github.com/nakiner/cnpg-connect-plugin/internal/observer"
	"github.com/nakiner/cnpg-connect-plugin/internal/server"
)

var version = "dev"

type settings struct {
	pluginAddress    string
	discoveryAddress string
	healthAddress    string
	serverCert       string
	serverKey        string
	clientCA         string
	discoveryCert    string
	discoveryKey     string
	authTokenFile    string
	namespace        string
	kubeconfig       string
	kubeAPIQPS       float64
	kubeAPIBurst     int
	pollInterval     time.Duration
	ttl              time.Duration
	probeTimeout     time.Duration
	maxConcurrency   int
	insecure         bool
	showVersion      bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cnpg-connect-plugin: %v\n", err)
		os.Exit(1)
	}
}

func parseSettings(args []string, stderr io.Writer) (settings, error) {
	var s settings
	flags := flag.NewFlagSet("cnpg-connect-plugin", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&s.pluginAddress, "plugin-address", ":9090", "CNPG-I gRPC listen address")
	flags.StringVar(&s.discoveryAddress, "discovery-address", ":8080", "Application discovery gRPC listen address")
	flags.StringVar(&s.healthAddress, "health-address", ":8081", "HTTP health probe listen address")
	flags.StringVar(&s.serverCert, "server-cert", "", "CNPG-I server TLS certificate PEM file")
	flags.StringVar(&s.serverKey, "server-key", "", "CNPG-I server TLS key PEM file")
	flags.StringVar(&s.clientCA, "client-ca", "", "Trusted operator client CA or client certificate PEM file")
	flags.StringVar(&s.discoveryCert, "discovery-cert", "", "Application discovery TLS certificate PEM file")
	flags.StringVar(&s.discoveryKey, "discovery-key", "", "Application discovery TLS key PEM file")
	flags.StringVar(&s.authTokenFile, "auth-token-file", "", "File containing application bearer token (at least 32 characters)")
	flags.StringVar(&s.namespace, "namespace", "", "Observe this namespace only (empty means all namespaces)")
	flags.StringVar(&s.kubeconfig, "kubeconfig", "", "Explicit kubeconfig file; default uses only in-cluster credentials")
	flags.Float64Var(&s.kubeAPIQPS, "kube-api-qps", 20, "Kubernetes API client request rate per second")
	flags.IntVar(&s.kubeAPIBurst, "kube-api-burst", 40, "Kubernetes API client request burst limit")
	flags.DurationVar(&s.pollInterval, "poll-interval", 5*time.Second, "Periodic topology observation interval")
	flags.DurationVar(&s.ttl, "ttl", 15*time.Second, "Validity period of an observed snapshot")
	flags.DurationVar(&s.probeTimeout, "probe-timeout", 2*time.Second, "Timeout for each PostgreSQL status probe")
	flags.IntVar(&s.maxConcurrency, "max-concurrency", 8, "Maximum simultaneous PostgreSQL status probes")
	flags.BoolVar(&s.insecure, "insecure", false, "Local development only: disable TLS and authentication; default addresses bind loopback")
	flags.BoolVar(&s.showVersion, "version", false, "Print version and exit")
	if err := flags.Parse(args); err != nil {
		return s, err
	}
	if flags.NArg() != 0 {
		return s, fmt.Errorf("unexpected positional arguments")
	}
	if s.insecure {
		explicit := make(map[string]bool)
		flags.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
		if !explicit["plugin-address"] {
			s.pluginAddress = "127.0.0.1:9090"
		}
		if !explicit["discovery-address"] {
			s.discoveryAddress = "127.0.0.1:8080"
		}
		if !explicit["health-address"] {
			s.healthAddress = "127.0.0.1:8081"
		}
	}
	return s, nil
}

func (s settings) validate() error {
	if !(s.kubeAPIQPS > 0) || s.kubeAPIQPS > math.MaxFloat32 || float32(s.kubeAPIQPS) == 0 {
		return fmt.Errorf("kube-api-qps must be positive, finite and representable as float32")
	}
	if s.kubeAPIBurst <= 0 {
		return fmt.Errorf("kube-api-burst must be positive")
	}
	if s.pollInterval <= 0 || s.probeTimeout <= 0 || s.ttl <= 0 || s.maxConcurrency <= 0 {
		return fmt.Errorf("poll-interval, probe-timeout, ttl and max-concurrency must be positive")
	}
	// Subtraction avoids duration overflow when checking custom values.
	if s.ttl <= s.pollInterval || s.ttl-s.pollInterval <= s.probeTimeout {
		return fmt.Errorf("ttl must exceed poll-interval plus probe-timeout")
	}
	if s.pluginAddress == "" || s.discoveryAddress == "" || s.healthAddress == "" {
		return fmt.Errorf("listener addresses must not be empty")
	}
	if s.insecure {
		if s.serverCert != "" || s.serverKey != "" || s.clientCA != "" || s.discoveryCert != "" || s.discoveryKey != "" || s.authTokenFile != "" {
			return fmt.Errorf("insecure cannot be combined with TLS or authentication options")
		}
		return nil
	}
	if s.serverCert == "" || s.serverKey == "" || s.clientCA == "" || s.discoveryCert == "" || s.discoveryKey == "" || s.authTokenFile == "" {
		return fmt.Errorf("secure mode requires server-cert, server-key, client-ca, discovery-cert, discovery-key and auth-token-file")
	}
	return nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	s, err := parseSettings(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if s.showVersion {
		_, err := fmt.Fprintln(stdout, version)
		return err
	}
	if err := s.validate(); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	runtimeOptions := server.Options{
		PluginAddress:    s.pluginAddress,
		DiscoveryAddress: s.discoveryAddress,
		HealthAddress:    s.healthAddress,
		Version:          version,
	}
	var token string
	if s.insecure {
		logger.Warn("TLS and application authentication disabled for development")
	} else {
		runtimeOptions.PluginTLS, err = server.TLSConfig(s.serverCert, s.serverKey, s.clientCA)
		if err != nil {
			return fmt.Errorf("CNPG-I TLS: %w", err)
		}
		runtimeOptions.DiscoveryTLS, err = server.TLSConfig(s.discoveryCert, s.discoveryKey, "")
		if err != nil {
			return fmt.Errorf("discovery TLS: %w", err)
		}
		token, err = server.ReadToken(s.authTokenFile)
		if err != nil {
			return err
		}
	}

	var kubeConfig *rest.Config
	if s.kubeconfig != "" {
		kubeConfig, err = clientcmd.BuildConfigFromFlags("", s.kubeconfig)
	} else {
		kubeConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		return fmt.Errorf("load Kubernetes configuration (outside Kubernetes, supply --kubeconfig explicitly): %w", err)
	}
	kubeConfig.UserAgent = "cnpg-connect-plugin/" + version
	kubeConfig.QPS = float32(s.kubeAPIQPS)
	kubeConfig.Burst = s.kubeAPIBurst
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}
	store := discovery.NewStore()
	collector, err := observer.New(kubeClient, dynamicClient, store, observer.Options{
		Namespace:      s.namespace,
		PollInterval:   s.pollInterval,
		TTL:            s.ttl,
		ProbeTimeout:   s.probeTimeout,
		MaxConcurrency: s.maxConcurrency,
	}, logger)
	if err != nil {
		return err
	}
	return server.Run(ctx, runtimeOptions, collector, func(grpcServer *grpc.Server) {
		connectv1.RegisterTopologyServiceServer(grpcServer, discovery.NewServer(store, token))
	}, logger)
}
