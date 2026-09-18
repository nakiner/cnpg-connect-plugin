// Package plugin observes CloudNativePG role changes without modifying database
// or operator state. CNPG remains responsible for promotion and replication.
package plugin

import (
	"context"
	"encoding/json"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/reconciler"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/nakiner/cnpg-connect-plugin/internal/config"
)

// Notifier schedules an observation without waiting for it to finish.
// Implementations must return promptly and coalesce repeated notifications.
type Notifier interface {
	Notify(namespace, name string)
}

// Service implements only Identity and Cluster reconciliation notifications.
// Database state never gates operator reconciliation or the CNPG-I probe.
type Service struct {
	identity.UnimplementedIdentityServer
	reconciler.UnimplementedReconcilerHooksServer
	version  string
	notifier Notifier
}

func Register(server *grpc.Server, version string, notifier Notifier) *Service {
	service := &Service{version: version, notifier: notifier}
	identity.RegisterIdentityServer(server, service)
	reconciler.RegisterReconcilerHooksServer(server, service)
	return service
}

func (s *Service) GetPluginMetadata(context.Context, *identity.GetPluginMetadataRequest) (*identity.GetPluginMetadataResponse, error) {
	return &identity.GetPluginMetadataResponse{
		Name:          config.PluginName,
		Version:       s.version,
		DisplayName:   "CNPG Connect",
		Description:   "PostgreSQL role and topology discovery for CloudNativePG",
		ProjectUrl:    "https://github.com/nakiner/cnpg-connect-plugin",
		RepositoryUrl: "https://github.com/nakiner/cnpg-connect-plugin",
		License:       "UNLICENSED",
		LicenseUrl:    "https://github.com/nakiner/cnpg-connect-plugin#license",
		Maturity:      "alpha",
	}, nil
}

func (s *Service) GetPluginCapabilities(context.Context, *identity.GetPluginCapabilitiesRequest) (*identity.GetPluginCapabilitiesResponse, error) {
	return &identity.GetPluginCapabilitiesResponse{
		Capabilities: []*identity.PluginCapability{{
			Type: &identity.PluginCapability_Service_{
				Service: &identity.PluginCapability_Service{
					Type: identity.PluginCapability_Service_TYPE_RECONCILER_HOOKS,
				},
			},
		}},
	}, nil
}

func (s *Service) Probe(context.Context, *identity.ProbeRequest) (*identity.ProbeResponse, error) {
	return &identity.ProbeResponse{Ready: true}, nil
}

func (s *Service) GetCapabilities(context.Context, *reconciler.ReconcilerHooksCapabilitiesRequest) (*reconciler.ReconcilerHooksCapabilitiesResult, error) {
	return &reconciler.ReconcilerHooksCapabilitiesResult{
		ReconcilerCapabilities: []*reconciler.ReconcilerHooksCapability{{Kind: reconciler.ReconcilerHooksCapability_KIND_CLUSTER}},
	}, nil
}

func (s *Service) Pre(_ context.Context, request *reconciler.ReconcilerHooksRequest) (*reconciler.ReconcilerHooksResult, error) {
	return s.notify(request), nil
}

func (s *Service) Post(_ context.Context, request *reconciler.ReconcilerHooksRequest) (*reconciler.ReconcilerHooksResult, error) {
	return s.notify(request), nil
}

func (s *Service) notify(request *reconciler.ReconcilerHooksRequest) *reconciler.ReconcilerHooksResult {
	if s.notifier != nil {
		// The payload supplies only a queue key. State from a reconciliation
		// hook is not an authoritative observation and must never be published.
		var cluster struct {
			Metadata struct {
				Namespace string `json:"namespace"`
				Name      string `json:"name"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(request.GetClusterDefinition(), &cluster); err == nil &&
			len(validation.IsDNS1123Label(cluster.Metadata.Namespace)) == 0 &&
			len(validation.IsDNS1123Subdomain(cluster.Metadata.Name)) == 0 {
			s.notifier.Notify(cluster.Metadata.Namespace, cluster.Metadata.Name)
		}
	}
	return &reconciler.ReconcilerHooksResult{Behavior: reconciler.ReconcilerHooksResult_BEHAVIOR_CONTINUE}
}
