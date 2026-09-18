// Package v1 defines the application discovery protocol. It intentionally has no
// Kubernetes, CNPG, or gRPC dependencies so clients can remain small.
package v1

import "time"

const APIVersion = "connect.cnpg.io/v1alpha1"

type ClusterRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

type Endpoint struct {
	Host       string `json:"host"`
	Port       uint16 `json:"port"`
	ServerName string `json:"serverName,omitempty"`
}

type Member struct {
	ID        string              `json:"id"` // Kubernetes Pod UID; changes on Pod replacement.
	Name      string              `json:"name"`
	Role      string              `json:"role"`      // primary, standby, unknown
	SyncState string              `json:"syncState"` // sync, quorum, potential, async, unknown
	Ready     bool                `json:"ready"`     // Safe to consider for routing in this snapshot.
	Reason    string              `json:"reason,omitempty"`
	Endpoints map[string]Endpoint `json:"endpoints"` // internal and optional external
	Node      string              `json:"node,omitempty"`
	Zone      string              `json:"zone,omitempty"`
	Region    string              `json:"region,omitempty"`
	Timeline  int                 `json:"timeline,omitempty"`
	ReplayLSN string              `json:"replayLSN,omitempty"`
}

// ConnectionParameters contains public PostgreSQL connection defaults, never
// passwords or private keys. The database can be empty when bootstrap metadata
// does not identify an application database.
type ConnectionParameters struct {
	Database    string `json:"database,omitempty"`
	ServerCAPEM []byte `json:"serverCaPem,omitempty"`
}

// Snapshot is a complete replacement, never a delta. Revision is opaque and
// changes on topology/routing changes, including invalidation; clients must not
// compare revisions numerically. ObservedAt and ValidUntil change on successful
// refreshes even when Revision does not. Clients MUST reject expired snapshots.
type Snapshot struct {
	APIVersion    string               `json:"apiVersion"`
	Cluster       ClusterRef           `json:"cluster"`
	Revision      string               `json:"revision"`
	ObservedAt    time.Time            `json:"observedAt"`
	ValidUntil    time.Time            `json:"validUntil"`
	Available     bool                 `json:"available"`
	Reason        string               `json:"reason,omitempty"`
	PrimaryID     string               `json:"primaryId,omitempty"`
	Transitioning bool                 `json:"transitioning"`
	Members       []Member             `json:"members"`
	Connection    ConnectionParameters `json:"connection"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
