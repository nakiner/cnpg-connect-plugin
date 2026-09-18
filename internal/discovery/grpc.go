package discovery

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	v1 "github.com/nakiner/cnpg-connect-plugin/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server implements application discovery separately from the CNPG-I interface.
// Runtime owns TLS. Read-only discovery needs no separate application token by
// default; deployments can opt into the legacy shared bearer token.
type Server struct {
	connectv1.UnimplementedTopologyServiceServer
	store         *Store
	authenticated bool
	tokenDigest   [sha256.Size]byte
}

func NewServer(store *Store, token string) *Server {
	return &Server{store: store, authenticated: token != "", tokenDigest: sha256.Sum256([]byte(token))}
}

func (s *Server) GetTopology(ctx context.Context, request *connectv1.GetTopologyRequest) (*connectv1.Snapshot, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	if err := validateReference(request.GetNamespace(), request.GetName()); err != nil {
		return nil, err
	}
	s.store.RequestRefresh(request.Namespace, request.Name)
	published, exists := s.store.getPublication(request.Namespace, request.Name)
	if !exists {
		return nil, status.Error(codes.NotFound, "cluster is not observed")
	}
	return published.protobuf(), nil
}

func (s *Server) WatchTopology(request *connectv1.WatchTopologyRequest, stream connectv1.TopologyService_WatchTopologyServer) error {
	ctx := stream.Context()
	if err := s.authorize(ctx); err != nil {
		return err
	}
	if err := validateReference(request.GetNamespace(), request.GetName()); err != nil {
		return err
	}
	updates, cancel, exists := s.store.subscribeShared(request.Namespace, request.Name)
	if !exists {
		return status.Error(codes.NotFound, "cluster is not observed")
	}
	defer cancel()
	var lastSent uint64
	for {
		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case published, ok := <-updates:
			if !ok {
				return nil
			}
			snapshot := published.snapshot
			// A pending snapshot can expire while a slow transport applies flow
			// control. Recheck it before delivery; clients also enforce validUntil.
			if snapshot.Reason != "deleted" && !time.Now().Before(snapshot.ValidUntil) {
				if latest, found := s.store.getPublication(snapshot.Cluster.Namespace, snapshot.Cluster.Name); found {
					published = latest
				} else {
					continue // The queued deletion/recreation supplies the new state.
				}
			}
			// The expiry lookup can jump ahead of a concurrent notification
			// still being delivered to this mailbox. Never send that older state
			// after the newer publication obtained directly from the store.
			if published.order <= lastSent {
				continue
			}
			// Send observes the RPC context. No per-send goroutine or unbounded
			// event queue is created; the store coalesces pending observations.
			if err := stream.Send(published.protobuf()); err != nil {
				return err
			}
			lastSent = published.order
		}
	}
}

func (s *Server) authorize(ctx context.Context) error {
	if !s.authenticated {
		return nil
	}
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	digest := sha256.Sum256([]byte(token))
	valid := subtle.ConstantTimeCompare(s.tokenDigest[:], digest[:]) == 1
	if !ok || !strings.EqualFold(scheme, "Bearer") || !valid {
		return status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	return nil
}

func validateReference(namespace, name string) error {
	if !validDNSName(namespace, false) || !validDNSName(name, true) {
		return status.Error(codes.InvalidArgument, "namespace and name must be valid Kubernetes resource names")
	}
	return nil
}

func validDNSName(value string, dots bool) bool {
	if value == "" || len(value) > 253 || (!dots && len(value) > 63) {
		return false
	}
	labels := strings.Split(value, ".")
	if !dots && len(labels) != 1 {
		return false
	}
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
				return false
			}
		}
	}
	return true
}

func toProto(snapshot v1.Snapshot) *connectv1.Snapshot {
	result := &connectv1.Snapshot{
		ApiVersion:    snapshot.APIVersion,
		Cluster:       &connectv1.ClusterRef{Namespace: snapshot.Cluster.Namespace, Name: snapshot.Cluster.Name, Uid: snapshot.Cluster.UID},
		Revision:      snapshot.Revision,
		ObservedAt:    timestamppb.New(snapshot.ObservedAt),
		ValidUntil:    timestamppb.New(snapshot.ValidUntil),
		Available:     snapshot.Available,
		Reason:        snapshot.Reason,
		PrimaryId:     snapshot.PrimaryID,
		Transitioning: snapshot.Transitioning,
		Members:       make([]*connectv1.Member, 0, len(snapshot.Members)),
		Connection:    &connectv1.ConnectionParameters{Database: snapshot.Connection.Database, ServerCaPem: append([]byte(nil), snapshot.Connection.ServerCAPEM...)},
	}
	for _, member := range snapshot.Members {
		converted := &connectv1.Member{
			Id:        member.ID,
			Name:      member.Name,
			Role:      roleToProto(member.Role),
			SyncState: syncToProto(member.SyncState),
			Ready:     member.Ready,
			Reason:    member.Reason,
			Endpoints: make(map[string]*connectv1.Endpoint, len(member.Endpoints)),
			Node:      member.Node,
			Zone:      member.Zone,
			Region:    member.Region,
			Timeline:  int64(member.Timeline),
			ReplayLsn: member.ReplayLSN,
		}
		for network, endpoint := range member.Endpoints {
			converted.Endpoints[network] = &connectv1.Endpoint{Host: endpoint.Host, Port: uint32(endpoint.Port), ServerName: endpoint.ServerName}
		}
		result.Members = append(result.Members, converted)
	}
	return result
}

func roleToProto(role string) connectv1.Role {
	switch role {
	case "primary":
		return connectv1.Role_ROLE_PRIMARY
	case "standby":
		return connectv1.Role_ROLE_STANDBY
	default:
		return connectv1.Role_ROLE_UNKNOWN
	}
}

func syncToProto(state string) connectv1.SyncState {
	switch state {
	case "sync":
		return connectv1.SyncState_SYNC_STATE_SYNC
	case "quorum":
		return connectv1.SyncState_SYNC_STATE_QUORUM
	case "potential":
		return connectv1.SyncState_SYNC_STATE_POTENTIAL
	case "async":
		return connectv1.SyncState_SYNC_STATE_ASYNC
	default:
		return connectv1.SyncState_SYNC_STATE_UNKNOWN
	}
}
