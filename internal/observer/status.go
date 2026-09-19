package observer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync/atomic"
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
	lastUsed  atomic.Int64 // Unix nanoseconds; idle transport cleanup only, not CA freshness
}

func (o *Observer) statusClient(connection v1.ConnectionParameters, serverName string) (*http.Client, error) {
	digest := sha256.Sum256(connection.ServerCAPEM)
	o.statusClientMu.RLock()
	if entry := o.statusClients[serverName]; entry != nil && entry.digest == digest {
		entry.lastUsed.Store(time.Now().UnixNano())
		o.statusClientMu.RUnlock()
		return entry.client, nil
	}
	o.statusClientMu.RUnlock()
	// Parsing a new CA must not block cached clients for unrelated Clusters.
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
	entry := &statusClientEntry{
		digest:    digest,
		client:    client,
		transport: transport,
	}
	var retired *http.Transport
	o.statusClientMu.Lock()
	if existing := o.statusClients[serverName]; existing != nil {
		if existing.digest == digest {
			existing.lastUsed.Store(time.Now().UnixNano())
			o.statusClientMu.Unlock()
			// This candidate has never made a request or opened a connection.
			return existing.client, nil
		}
		retired = existing.transport
	}
	entry.lastUsed.Store(time.Now().UnixNano())
	if o.statusClients == nil {
		o.statusClients = make(map[string]*statusClientEntry)
	}
	o.statusClients[serverName] = entry
	o.statusClientMu.Unlock()
	if retired != nil {
		retired.CloseIdleConnections()
	}
	return client, nil
}

func (o *Observer) closeStatusClients() {
	o.statusClientMu.Lock()
	retired := o.statusClients
	o.statusClients = nil
	o.statusClientMu.Unlock()
	for _, entry := range retired {
		entry.transport.CloseIdleConnections()
	}
}

// Bound retained CA/TLS state when demand ends or databases are deleted. The
// sweep also catches entries created by a probe concurrent with deletion.
func (o *Observer) pruneConnections(now time.Time) {
	o.stateMu.Lock()
	for key := range o.cas {
		if len(o.demandedCAClusters(key)) == 0 {
			delete(o.cas, key)
		}
	}
	o.stateMu.Unlock()
	o.statusClientMu.Lock()
	var retired []*http.Transport
	for key, entry := range o.statusClients {
		if now.Sub(time.Unix(0, entry.lastUsed.Load())) >= time.Minute {
			retired = append(retired, entry.transport)
			delete(o.statusClients, key)
		}
	}
	o.statusClientMu.Unlock()
	for _, transport := range retired {
		transport.CloseIdleConnections()
	}
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
