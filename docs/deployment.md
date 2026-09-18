# Deployment and operations

Start with the [three-step installation](../README.md#install-in-kubernetes). This page covers optional deployment choices and operational details. Application configuration remains discovery address, namespace/Cluster, and PostgreSQL username/password when the endpoint uses publicly trusted TLS.

## Process and ports

Deploy into the CNPG operator's namespace. The chart creates a ServiceAccount, observer RBAC, one Deployment, two Services, and optional cert-manager resources.

| Listener | Transport | Consumer |
| --- | --- | --- |
| `:9090` | CNPG-I gRPC with mutual TLS | CNPG operator |
| `:8080` | Application gRPC with server TLS, or internal h2c behind a TLS Gateway | Applications via discovery endpoint |
| `:8081` | HTTP `/healthz` and `/readyz` | Kubernetes probes |

Only the CNPG-I Service carries `cnpg.io/pluginName: connect.cnpg.io` and the `pluginPort`, `pluginServerSecret`, and `pluginClientSecret` annotations. Both TLS Secret annotations refer to Secrets in the Service/operator namespace. The application Service is separate; the health listener is not exposed by a Service.

## Observation modes and CNPG availability

The default observer discovers all CNPG Clusters in its watch scope automatically. Existing databases need no enrollment annotation or native `spec.plugins` entry. Set `connect.cnpg.io/enabled: "false"` on a Cluster to exclude it from discovery.

| Mode | Cluster configuration | Availability effect |
| --- | --- | --- |
| Automatic observation (default) | None | CNPG does not call this plugin for the Cluster, keeping failover reconciliation independent of discovery |
| Native CNPG-I integration | `spec.plugins` contains an enabled `connect.cnpg.io` entry | CNPG calls the plugin in its reconciliation path; plugin unavailability can delay reconciliation and failover |

In CNPG 1.30.0, the controller [loads native plugins before its inner reconcile](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/controller/cluster_controller.go#L213). A [Pre-hook error returns before status and failover processing](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/controller/cluster_controller.go#L374), and the [plugin client propagates an RPC error](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/cnpi/plugin/client/reconciler.go#L151).

The optional `connect.cnpg.io/parameters` annotation is a JSON object whose values are strings, with the same keys as native plugin parameters. `externalEndpoints` is itself a JSON-encoded string inside that object. Omit the parameters annotation when defaults suffice. Older `connect.cnpg.io/enabled: "true"` annotations remain compatible but are no longer required.

Explicit disablement wins: either `connect.cnpg.io/enabled: "false"` or a disabled native plugin entry excludes the Cluster. When a native entry is present, its parameters take precedence over annotation parameters. To switch from native integration to automatic observation, remove this plugin's native entry while preserving other plugins. See [cluster.yaml](../examples/cluster.yaml) for optional native enrollment.

Both modes observe asynchronously and report all role transitions. The Go library expires stale snapshots and reconnects streams in either mode.

## Watch scope and access

`watchNamespace` creates a Role and RoleBinding in that namespace, binding the plugin ServiceAccount in the release namespace. The watched namespace must already exist. An empty value creates cluster-scoped RBAC and watches all namespaces. Clusters in scope are published automatically unless explicitly disabled.

| Resource | Verbs | Use |
| --- | --- | --- |
| `postgresql.cnpg.io/clusters` | `get`, `list`, `watch` | Cluster identity, configuration, and status |
| Core `pods` | `get`, `list`, `watch` | Instance identities and addresses |
| Core `pods/proxy` | `get` | CNPG instance-manager `/pg/status` |
| Core `secrets` | `get` | Referenced PostgreSQL server CA's public `ca.crt` |

The observer fetches the Secret named by `status.certificates.serverCASecret`, extracts public certificate PEM blocks from `ca.crt`, and publishes them as connection metadata. It never publishes private keys or PostgreSQL passwords. Kubernetes RBAC authorizes retrieval of the whole referenced Secret; it cannot restrict access to a single data key. There is no Secret list/watch permission. Pod proxy GET permission likewise cannot be restricted to one HTTP path. Namespace-scoped deployment limits both permissions' scope.

The application API is tokenless by default, so network access to that endpoint allows reading topology for observed Clusters. Optional bearer authentication has deployment-wide scope, not per-Cluster or per-user scope. PostgreSQL authenticates the actual SQL connections separately.

Use one release per operator installation. Every release advertises `connect.cnpg.io`, and CNPG [registers plugins by that name](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/cnpi/plugin/repository/setup.go#L112). Additional releases visible to the same operator can replace its registration; use replicas of one release instead.

## Discovery endpoint options

### TLS Gateway with an internal h2c backend

This is the [README quickstart](../README.md#2-publish-one-discovery-address). Use [values-gateway.yaml](../examples/values-gateway.yaml), provide a publicly trusted certificate on the Gateway, and route gRPC to `cnpg-connect-api:443`.

`tls.application.enabled: false` emits `--discovery-plaintext`, skips the application's Certificate and TLS volume, and declares `appProtocol: kubernetes.io/h2c` on the Service, following [Gateway API backend protocol selection](https://gateway-api.sigs.k8s.io/guides/user-guides/backend-protocol/). Only the internal Gateway-to-plugin hop is plaintext. CNPG-I still requires mutual TLS. The backend is a ClusterIP Service by default; do not turn this h2c backend into a public plaintext endpoint.

Use an HTTP/2-capable Gateway implementation, a TLS listener accepting the route's hostname/namespace, and DNS pointing at the Gateway. Long-lived gRPC streams need appropriate proxy idle timeouts; the library also handles normal stream reconnection.

### Direct TLS LoadBalancer

[values-external.yaml](../examples/values-external.yaml) exposes the discovery Service with TCP TLS passthrough. Supply a server certificate for `topology.example.com` in `tls.application.existingSecret`, or use your private PKI as described below. Replace the DNS name, Secret, and source CIDR in the example. The CNPG-I certificate issuer remains independent.

A publicly trusted application server certificate gives applications the same simple configuration as the Gateway setup. A private application certificate requires an explicit discovery CA in the client's advanced configuration. Database CA distribution still happens automatically through the authenticated discovery TLS connection.

### Private in-cluster endpoint

The default chart enables application TLS and generates a private discovery certificate with cert-manager. This needs no public hostname or Gateway, but applications must trust that discovery CA through their system roots or explicit client TLS configuration. Exporting its public certificate for a diagnostic client:

```sh
kubectl -n cnpg-system get secret cnpg-connect-ca \
  -o jsonpath='{.data.tls\.crt}' | openssl base64 -d -A > discovery-ca.crt
kubectl -n cnpg-system port-forward service/cnpg-connect-api 8443:443
```

From another terminal in the source checkout:

```sh
grpcurl -cacert discovery-ca.crt \
  -authority cnpg-connect-api.cnpg-system.svc \
  -import-path proto -proto cnpg/connect/v1/topology.proto \
  -d '{"namespace":"databases","name":"app-db"}' \
  127.0.0.1:8443 cnpg.connect.v1.TopologyService/GetTopology
```

This CA is for discovery TLS. The library obtains the separate PostgreSQL CA from the resulting snapshot.

## Certificates

By default, cert-manager creates a private CA Issuer, CNPG-I server and operator-client certificates, and an application server certificate when `tls.application.enabled` is true. DNS SANs include the Service names. `tls.application.extraDnsNames` and `extraIPAddresses` add discovery identities when generating its certificate.

To use an existing issuer, set `tls.certManager.createIssuer=false` and configure `tls.certManager.issuerRef`. The CNPG-I issuer must support server and client certificates; a public ACME issuer cannot issue the operator's client certificate. A discovery certificate can come from a separate public issuer through `tls.application.existingSecret`, or be terminated at a Gateway.

To supply all certificates yourself, use [values-existing-secrets.yaml](../examples/values-existing-secrets.yaml):

| Value | Required Secret data |
| --- | --- |
| `tls.plugin.existingServerSecret` | `tls.crt`, `tls.key`; server-auth certificate covering bare Service name `cnpg-connect` and its Service DNS names |
| `tls.plugin.existingClientSecret` | `tls.crt`, `tls.key`; client-auth certificate; also `ca.crt` when using the default client trust projection |
| `tls.application.existingSecret` | `tls.crt`, `tls.key`; server-auth certificate covering the discovery hostname; only needed when application TLS is enabled |

CNPG 1.30.0 [reads the operator-client Secret's `tls.crt`/`tls.key` and uses the server Secret's `tls.crt` for server trust](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/controller/plugin_controller.go#L184). `tls.plugin.clientCASecret` and `clientCAKey` configure the plugin's separate client trust input. The default projects `ca.crt` from the operator-client Secret; use `clientCAKey: tls.crt` to trust the client leaf directly, or specify a separate CA Secret. The operator-client private key is not mounted into the plugin.

Secret volumes use directory mounts without `subPath`. TLS files are reloaded for new handshakes after Kubernetes projects updates; existing connections retain their TLS session. The server renews connections after five minutes plus up to a 30-second grace period. Gateway certificates follow that Gateway's rotation mechanism. Private discovery root replacement requires updating client trust; publicly trusted renewals use normal system trust.

PostgreSQL public CA metadata is refreshed during observation and changes the topology revision when it changes. Applications pick it up through discovery. CNPG still manages the database's own certificate lifecycle.

## Optional discovery authentication

No discovery token is required by default. If a deployment wants a separate bearer check, create a Secret containing a token of at least 32 non-whitespace characters and set:

```yaml
application:
  auth:
    existingSecret: cnpg-connect-auth
    key: token
```

For example, from a token file supplied by your secret manager:

```sh
kubectl -n cnpg-system create secret generic cnpg-connect-auth \
  --from-file=token=/path/to/discovery-token
```

Clients then send `authorization: Bearer <token>` metadata using the library's advanced authentication option. This token authorizes every observed Cluster in the deployment's watch scope; it is unrelated to PostgreSQL credentials.

The token is read at startup. After replacing its Secret, restart the Deployment and update clients. There is no overlapping old/new-token window. Clear `application.auth.existingSecret` to return to tokenless discovery. With a TLS Gateway, bearer verification occurs on the plugin after the internal h2c hop.

## External clients and database addresses

Applications need access to both discovery and the selected PostgreSQL member. The discovery hostname can work from inside or outside Kubernetes. With no client network setting, the library tries the selected member's internal address and falls back to that same member's external address when needed; if only an external address is published, it uses that address. Both paths retain PostgreSQL TLS verification and role checks. Advanced `Discovery.Network` configuration can pin a specific endpoint network.

Connection attempts are bounded by the client's connection timeout (five seconds by default per address) and caller context. Automatic selection does not create a network route: the operator still provides reachable external addresses for clients outside the Pod network.

Configure `externalEndpoints` once on the CNPG Cluster, mapping instance names to addresses that always reach those instances regardless of role. For example:

```json
{
  "app-db-1": {"host": "pg1.example.com", "port": 5432},
  "app-db-2": {"host": "pg2.example.com", "port": 5432},
  "app-db-3": {"host": "pg3.example.com", "port": 5432}
}
```

Each entry may override `serverName`; otherwise it inherits the Cluster's configured `serverName`, defaulting to `<cluster>-rw.<namespace>.svc`. This is the PostgreSQL TLS identity, distinct from the discovery hostname. The library gets the PostgreSQL CA from discovery, so applications do not mount it separately.

For the default observation mode, use an optional parameters annotation generated from an endpoint file:

```sh
jq -cn --slurpfile endpoints external-endpoints.json \
  '{externalEndpoints:($endpoints[0] | tojson)}' > cluster-parameters.json
kubectl -n databases annotate cluster.postgresql.cnpg.io app-db \
  "connect.cnpg.io/parameters=$(cat cluster-parameters.json)" --overwrite
```

This replaces the parameters annotation; include any other parameters you use in the generated object. For native mode, `spec.plugins[].parameters.externalEndpoints` contains the map as a YAML string; see [cluster.yaml](../examples/cluster.yaml).

Duplicate host/port pairs across members are rejected. Missing mappings remain absent. A shared `rw` or `ro` LoadBalancer chooses a role/backend itself and cannot identify a specific synchronous or asynchronous member. Use per-instance Services/LBs or network routing to Pod addresses for role-aware selection. The plugin does not create these network resources.

Pod replacement changes member UID even if its name and external hostname stay the same. The library tracks topology and identity, not just hostnames, when retiring connections.

## Runtime flags

| Flag | Default/purpose |
| --- | --- |
| `--plugin-address` | `:9090` CNPG-I listener |
| `--discovery-address` | `:8080` application gRPC listener |
| `--health-address` | `:8081` probe listener |
| `--server-cert`, `--server-key` | CNPG-I PEM certificate and key |
| `--client-ca` | Trust for CNPG operator client certificates |
| `--discovery-cert`, `--discovery-key` | Application server certificate and key when application TLS is enabled |
| `--discovery-plaintext` | Internal h2c application listener behind a TLS Gateway; leaves CNPG-I mTLS enabled |
| `--auth-token-file` | Optional bearer token file; unset means tokenless discovery |
| `--namespace` | Empty watches all namespaces |
| `--poll-interval` | `5s` observation refresh |
| `--ttl` | `15s` snapshot validity; must exceed poll interval plus probe timeout |
| `--probe-timeout` | `2s` per-instance status request timeout |
| `--max-concurrency` | `8` parallel status probes |
| `--kube-api-qps`, `--kube-api-burst` | `20`, `40` per Kubernetes client |
| `--kubeconfig` | Optional explicit kubeconfig; otherwise in-cluster credentials |
| `--insecure` | Local development without either listener's TLS or bearer authentication; rejects TLS/token flags |
| `--version` | Print version and exit |

For larger deployments, size API limits, concurrency, poll interval, and TTL for the collection latency. The library's default maximum accepted snapshot TTL is 30 seconds; adjust its advanced setting if increasing the plugin TTL beyond that.

## Validation and compatibility

The observer targets the CNPG 1.30.0 instance-manager status format. See [the validation record](validation.md) for completed tests and [integration tests](../test/e2e/README.md) for lifecycle scenarios. Local tests do not establish the behavior of a particular Gateway, external database LB, or network partition. Normal unit checks do not mutate a Kubernetes Cluster.
