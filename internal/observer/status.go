package observer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
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

// The API server proxy is also used by kubectl cnpg status. It handles access to
// instance ports without copying PostgreSQL credentials or operator private keys.
func readStatus(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod) (instanceStatus, error) {
	data, err := client.CoreV1().Pods(pod.Namespace).ProxyGet(statusScheme(pod), pod.Name, "8000", "pg/status", nil).DoRaw(ctx)
	if err != nil {
		return instanceStatus{}, fmt.Errorf("instance status request: %w", err)
	}
	if len(data) > 4<<20 {
		return instanceStatus{}, fmt.Errorf("instance status exceeds size limit")
	}
	return decodeStatus(strings.NewReader(string(data)))
}
