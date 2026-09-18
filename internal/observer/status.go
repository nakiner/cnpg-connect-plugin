package observer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"time"

	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	corev1 "k8s.io/api/core/v1"
)

// These fields are the CNPG 1.30 instance-manager /pg/status contract. Keeping
// the wire projection here avoids importing the entire operator into this plugin.
type instanceStatus struct {
	IsPrimary         *bool               `json:"isPrimary"`
	SystemID          string              `json:"systemID"`
	Timeline          int                 `json:"timeLineID"`
	ReplayLSN         string              `json:"replayLsn"`
	ReplayPaused      bool                `json:"replayPaused"`
	WalReceiverActive bool                `json:"isWalReceiverActive"`
	Rewinding         bool                `json:"isPgRewindRunning"`
	Unavailable       bool                `json:"mightBeUnavailable"`
	Replication       []replicationStatus `json:"replicationInfo"`
}

type replicationStatus struct {
	ApplicationName string `json:"applicationName"`
	State           string `json:"state"`
	SyncState       string `json:"syncState"`
}

type statusResult struct {
	status instanceStatus
	err    error
}

func decodeStatus(reader io.Reader) (instanceStatus, error) {
	var result instanceStatus
	decoder := json.NewDecoder(io.LimitReader(reader, 4<<20))
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("decode instance status: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return result, fmt.Errorf("instance status contains trailing data")
	}
	if result.IsPrimary == nil || result.SystemID == "" || result.Timeline <= 0 {
		return result, fmt.Errorf("instance status missing identity, role or timeline")
	}
	return result, nil
}

func statusScheme(pod *corev1.Pod) string {
	for _, container := range pod.Spec.Containers {
		if container.Name == "postgres" && (slices.Contains(container.Command, "--status-port-tls") || slices.Contains(container.Args, "--status-port-tls")) {
			return "https"
		}
	}
	return "http"
}

// Status requests go directly to instance managers. CNPG 1.30 exposes this
// endpoint without a client certificate; HTTPS verifies the database server CA.
// No request passes through the operator or Kubernetes Pod proxy.
type statusClientEntry struct {
	digest    [sha256.Size]byte
	client    *http.Client
	transport *http.Transport
	lastUsed  time.Time // protected by statusClientMu
}

func (o *Observer) statusClient(connection v1.ConnectionParameters, serverName string) (*http.Client, error) {
	digest := sha256.Sum256(connection.ServerCAPEM)
	o.statusClientMu.Lock()
	defer o.statusClientMu.Unlock()
	if value, ok := o.statusClients.Load(serverName); ok {
		entry := value.(*statusClientEntry)
		if entry.digest == digest {
			entry.lastUsed = time.Now()
			return entry.client, nil
		}
		entry.transport.CloseIdleConnections()
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(connection.ServerCAPEM) {
		return nil, fmt.Errorf("invalid PostgreSQL server CA")
	}
	transport := &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   o.opts.ProbeTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			ServerName: serverName,
			MinVersion: tls.VersionTLS12,
		},
		TLSHandshakeTimeout:   o.opts.ProbeTimeout,
		ResponseHeaderTimeout: o.opts.ProbeTimeout,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   2,
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	o.statusClients.Store(serverName, &statusClientEntry{
		digest:    digest,
		client:    client,
		transport: transport,
		lastUsed:  time.Now(),
	})
	return client, nil
}

func (o *Observer) closeStatusClients() {
	o.statusClientMu.Lock()
	defer o.statusClientMu.Unlock()
	o.statusClients.Range(func(key, value any) bool {
		value.(*statusClientEntry).transport.CloseIdleConnections()
		o.statusClients.Delete(key)
		return true
	})
}

// Bound retained CA/TLS state when demand ends or databases are deleted. The
// sweep also catches entries created by a probe concurrent with deletion.
func (o *Observer) pruneConnections(now time.Time) {
	o.connections.Range(func(key, value any) bool {
		k := key.(clusterKey)
		if _, exists := o.cluster(k); !exists || (!o.store.HasDemand(k.namespace, k.name) && now.Sub(value.(cachedConnection).refreshed) >= time.Minute) {
			o.connections.Delete(key)
		}
		return true
	})
	o.statusClientMu.Lock()
	defer o.statusClientMu.Unlock()
	o.statusClients.Range(func(key, value any) bool {
		entry := value.(*statusClientEntry)
		if now.Sub(entry.lastUsed) >= time.Minute {
			entry.transport.CloseIdleConnections()
			o.statusClients.Delete(key)
		}
		return true
	})
}

func (o *Observer) readStatus(ctx context.Context, pod *corev1.Pod, connection v1.ConnectionParameters, serverName string) (instanceStatus, error) {
	if net.ParseIP(pod.Status.PodIP) == nil {
		return instanceStatus{}, fmt.Errorf("instance has no valid Pod IP")
	}
	client, err := o.statusClient(connection, serverName)
	if err != nil {
		return instanceStatus{}, err
	}
	return readStatusHTTP(ctx, client, statusScheme(pod)+"://"+net.JoinHostPort(pod.Status.PodIP, "8000")+"/pg/status")
}

func readStatusHTTP(ctx context.Context, client *http.Client, address string) (instanceStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return instanceStatus{}, err
	}
	response, err := client.Do(req)
	if err != nil {
		return instanceStatus{}, fmt.Errorf("instance status request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return instanceStatus{}, fmt.Errorf("instance status HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if err != nil {
		return instanceStatus{}, err
	}
	if len(data) > 4<<20 {
		return instanceStatus{}, fmt.Errorf("instance status exceeds size limit")
	}
	return decodeStatus(bytes.NewReader(data))
}

// Ordinary network failures must not cause fleet-wide CA reads during outages.
func isCertificateError(err error) bool {
	var verification *tls.CertificateVerificationError
	return errors.As(err, &verification)
}
