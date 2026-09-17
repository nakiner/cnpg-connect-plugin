# Deployment and operations

For the complete installation walkthrough, verification commands, upgrades, and uninstall procedure, start with the [README](../README.md).

## Process and ports

Deploy into the CNPG operator's namespace. The chart installs a ServiceAccount, observer RBAC, one Deployment, two Services, and optional cert-manager resources. It does not add a PostgreSQL sidecar, webhook, CRD, backup capability, or leader-election permissions.

| Listener | Transport | Consumer |
| --- | --- | --- |
| `:9090` | CNPG-I gRPC with mutual TLS | CNPG operator |
| `:8080` | Application gRPC with server TLS and bearer metadata | Applications |
| `:8081` | HTTP `/healthz` and `/readyz` | Kubernetes probes |

Only the CNPG-I Service carries `cnpg.io/pluginName: connect.cnpg.io` and the `pluginPort`, `pluginServerSecret`, and `pluginClientSecret` annotations. Both TLS Secret annotations refer to Secrets in the Service/operator namespace. The application Service is separate and may be a `LoadBalancer`; the health listener is not exposed by a Service.

## Observation modes and CNPG availability

The same Deployment supports two opt-in modes; no additional backend is needed.

| Mode | Cluster configuration | Availability effect |
| --- | --- | --- |
| Annotation-based observation (recommended) | `metadata.annotations["connect.cnpg.io/enabled"]: "true"`, with no native entry for this plugin | CNPG does not call this plugin for the Cluster; discovery outages do not add a dependency to its failover reconciliation |
| Native CNPG-I integration | `spec.plugins` contains an enabled `connect.cnpg.io` entry | CNPG loads and calls the plugin in its reconciliation path; plugin unavailability can delay reconciliation and failover |

In CNPG 1.30.0, the Cluster controller [loads native plugins before its inner reconcile](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/controller/cluster_controller.go#L213). A [Pre-hook error returns before status and failover processing](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/controller/cluster_controller.go#L374), and the [plugin client propagates an RPC error](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/cnpi/plugin/client/reconciler.go#L151). Returning quickly from a healthy plugin does not remove this dependency during an outage.

Use [cluster-annotation.yaml](../examples/cluster-annotation.yaml) to observe without native registration. Its optional `connect.cnpg.io/parameters` annotation is a JSON object whose values are strings, with the same keys as native plugin parameters. Consequently `externalEndpoints` must itself be a JSON-encoded string inside that object. The example shows the simpler optional `serverName` parameter; omit the parameters annotation when defaults suffice.

If any native `spec.plugins` entry exists for `connect.cnpg.io`, it takes precedence and annotations are ignored. An explicitly disabled native entry also disables observation; it does not fall back to the annotation. To switch from native mode to annotation mode, remove this plugin's native entry while preserving other plugin entries, and add the opt-in annotation. Use [cluster.yaml](../examples/cluster.yaml) when native lifecycle integration is intentional.

Both modes observe asynchronously. During a discovery outage applications still need to expire cached snapshots and recover streams; annotation mode only separates the discovery service's availability from CNPG's control path. Other native plugins keep their own reconciliation dependencies.

## Watch scope and access

Set `watchNamespace` to observe one namespace. The chart creates a Role and RoleBinding in that namespace, binding the plugin ServiceAccount in the release namespace. An empty value creates a ClusterRole/ClusterRoleBinding and watches all namespaces. Only Clusters that explicitly opt in through native registration or annotations are published.

Observer permissions are:

| Resource | Verbs |
| --- | --- |
| `postgresql.cnpg.io/clusters` | `get`, `list`, `watch` |
| Core `pods` | `get`, `list`, `watch` |
| Core `pods/proxy` | `get` |

Kubernetes RBAC cannot restrict `pods/proxy` permission to the `/pg/status` path. The implementation only requests instance status, but the service-account credential grants GET proxy access to other Pod HTTP paths in its scope. Prefer a dedicated database namespace and namespace-scoped RBAC. There is no permission to read Secrets through the API; kubelet projects the explicitly named Secrets into the Pod.

A single bearer token protects the application API and authorizes every observed Cluster; per-application or per-Cluster authorization is not implemented. Use one release per operator installation, with replicas of that release when needed. Every release advertises `connect.cnpg.io`, and CNPG [registers plugins by that name](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/cnpi/plugin/repository/setup.go#L112), so separate releases visible to one operator can replace each other's registration. Separate access boundaries require an appropriately isolated deployment/operator design, not merely another release in the same operator namespace. Do not expose the CNPG-I listener outside the operator network.

## Certificates

By default, cert-manager creates a private CA Issuer and three leaf certificates. Service DNS SANs are generated automatically. Set `tls.application.extraDnsNames` and/or `extraIPAddresses` for externally advertised discovery addresses. The operator client leaf certificate has `client auth` usage; the two server certificates have `server auth` usage.

To use an existing issuer, set `tls.certManager.createIssuer=false` and set `tls.certManager.issuerRef.name`, `kind`, and `group`. The issuer must issue both server and operator-client certificates and populate the trust material used by CNPG. Do not use a public ACME issuer for the operator-client certificate.

To supply all certificates yourself, use [values-existing-secrets.yaml](../examples/values-existing-secrets.yaml). Disable cert-manager generation and provide:

| Value | Required Secret data |
| --- | --- |
| `tls.plugin.existingServerSecret` | `tls.crt`, `tls.key`; server certificate SANs include the bare CNPG-I Service name (CNPG's default TLS server name) and its Service DNS names |
| `tls.plugin.existingClientSecret` | `tls.crt`, `tls.key`; also `ca.crt` when using the chart's default client trust projection |
| `tls.application.existingSecret` | `tls.crt`, `tls.key`; server certificate SANs cover the discovery Service and external DNS |
| `application.auth.existingSecret` | Key named by `application.auth.key`, default `token` |

CNPG 1.30.0 [reads the operator-client Secret's `tls.crt`/`tls.key` and uses the server Secret's `tls.crt` for server trust](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/controller/plugin_controller.go#L184). It does not require `ca.crt` in the server Secret for this integration.

`tls.plugin.clientCASecret` and `clientCAKey` configure the plugin's separate client trust input. By default the chart projects `ca.crt` from the operator-client Secret, so that key must exist under the default configuration. Set `clientCAKey: tls.crt` to trust the operator-client leaf directly, or use a separate Secret containing the appropriate CA bundle. The chart does not mount the operator's client private key into the plugin. Application clients obtain their server CA through your normal trust-distribution mechanism, not through the topology API.

Secret volumes use directory mounts without `subPath`. The runtime reloads server certificates, keys, and operator-client trust for new TLS handshakes; existing connections retain their original TLS session. Kubernetes Secret projection is eventually consistent. Invalid replacement TLS files reject new connections; certificate rotation must be tested against the target CNPG operator. Root CA replacement requires coordinated trust distribution to the operator and application clients.

The bearer token is read at startup. After replacing the token Secret, restart the Deployment and update clients; overlapping old/new-token acceptance is not implemented. A rolling restart can temporarily leave replicas using different tokens.

## External clients and database addresses

[values-external.yaml](../examples/values-external.yaml) exposes discovery through a LoadBalancer. Replace the example DNS, source CIDR, registry, and token Secret. Configure the LB for TCP TLS passthrough or compatible end-to-end HTTP/2 gRPC forwarding; allow long-lived streaming RPCs. External DNS is managed separately. Existing database LB configuration is independent of the discovery LB.

The `externalEndpoints` parameter is a JSON map from CNPG instance name to `{host, port, serverName}`. Set it in native `spec.plugins[].parameters` or as a JSON-encoded string within the annotation-mode parameters object. Each endpoint must always reach exactly that instance, including when its role changes. Configuration rejects assigning the same host/port pair to multiple members; the actual LB routing must also preserve member identity. A shared `ro` LB can choose a different replica from the one selected by the client; it cannot implement member-specific sync/async routing. Missing external mappings remain absent; the client must not silently use an internal address from outside Kubernetes.

Internal endpoints use Pod addresses. Their PostgreSQL TLS name defaults to `<cluster>-rw.<namespace>.svc`; the optional Cluster plugin parameter `serverName` overrides that name for custom certificates. An endpoint's `serverName` is for PostgreSQL TLS verification; it is distinct from the discovery endpoint's certificate name. Verify your database certificate SANs before using either network.

Pod replacement changes the member UID even if the instance name and advertised LB hostname are reused. Clients should invalidate role-sensitive pools using instance identity and topology changes, not only hostname changes.

## Runtime flags

| Flag | Default/purpose |
| --- | --- |
| `--plugin-address` | `:9090` CNPG-I listener |
| `--discovery-address` | `:8080` application gRPC listener |
| `--health-address` | `:8081` probe listener |
| `--server-cert`, `--server-key` | CNPG-I PEM certificate and key |
| `--client-ca` | PEM trust for CNPG operator client certificates |
| `--discovery-cert`, `--discovery-key` | Application gRPC PEM certificate and key |
| `--auth-token-file` | File containing a token of at least 32 non-whitespace characters |
| `--namespace` | Empty means all namespaces |
| `--poll-interval` | `5s` periodic observation interval |
| `--ttl` | `15s` observation validity window; must exceed poll interval plus probe timeout |
| `--probe-timeout` | `2s` per-instance status request timeout |
| `--max-concurrency` | `8` maximum parallel status probes |
| `--kube-api-qps` | `20` requests/second per Kubernetes client; positive finite float32-representable number |
| `--kube-api-burst` | `40` request burst allowance per Kubernetes client; positive integer |
| `--kubeconfig` | Optional external kubeconfig; otherwise in-cluster configuration |
| `--insecure` | Explicit local development without TLS/authentication; rejects TLS/token flags |
| `--version` | Print version and exit |

The chart exposes these rate limits as `observer.kubeAPIQPS` and `observer.kubeAPIBurst`. The typed and dynamic Kubernetes clients each use the configured limits. For larger deployments, measure observation duration and adjust API limits, probe concurrency, poll interval, and TTL together: an observation that takes too long must become unavailable rather than keeping stale routing valid.

## Deployment acceptance work

Local automated checks do not establish behavior in your environment. Before connecting production applications, exercise these scenarios against CNPG 1.30.0 and the actual LB:

- Planned switchover and abrupt primary loss, checking that invalid/ambiguous primary observations stop write selection.
- Discovery process outage during failover: verify annotation mode leaves CNPG reconciliation independent, and account for the native-mode dependency when that mode is chosen.
- Replica disconnection/rejoin, priority and quorum sync changes, and stale replication information.
- Pod replacement and Cluster deletion/recreation, including reused names and LB addresses.
- Observer RBAC loss, API server outage, instance probe timeout, and process restart; clients must expire cached data.
- CNPG-I certificate renewal, discovery certificate renewal, operator-client trust replacement, and application token rotation.
- Long-running and disconnected streaming clients, LB idle timeouts, and reconnected clients receiving a full snapshot.

The observer reads the CNPG 1.30.0 instance-manager status shape. This is a version-pinned integration; upgrading CNPG requires checking that schema and rerunning acceptance scenarios.
