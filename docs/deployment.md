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

## Event delivery and scaling

The plugin maintains shared Kubernetes `LIST/WATCH` subscriptions for Clusters and CNPG Pods, with metadata held in informer caches. Initial synchronization and watch recovery can perform lists; ordinary observation does not repeatedly list every database or reread Cluster/Pod objects. Relevant changes enqueue only their namespace/Cluster key, with repeated events coalesced by the work queue. There is no global periodic topology scan or fixed 100 ms event debounce. Optional CNPG-I hooks enqueue the same key and return immediately without waiting for status collection.

`WatchTopology` starts observation for its Cluster. Multiple subscribers share the same observations. When its last subscriber disconnects, periodic collection stops after any in-flight work, unless a recent unary `GetTopology` still holds a demand lease. Each unary request keeps that Cluster active for one `ttl` period. Clusters without consumers retain metadata but do not receive periodic instance probes. The first request can return `awaiting_observation`; a stream then receives the completed snapshot, while a unary caller can request it again later.

Kubernetes watches supply primary/target-primary changes, instance identities, addresses, readiness, and configuration. CNPG 1.30's [Cluster status](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/api/v1/cluster_types.go) does not include actual `sync`, `quorum`, `potential`, or `async` state. Its [instance status endpoint](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/pkg/management/postgres/webserver/remote.go) is request/response. [CNPG-I reconciliation hooks](https://github.com/cloudnative-pg/cnpg-i/blob/v0.5.0/proto/reconciler.proto) carry Cluster definitions, not a stream of complete instance state. Unmodified CNPG 1.30 has no supported controller endpoint for subscribing to that full state; even [`kubectl cnpg status`](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/cmd/plugin/status/status.go) retrieves instance statuses. Active Clusters therefore still need bounded `/pg/status` checks: immediately after relevant events, plus a periodic refresh for replication changes without a Kubernetes event. The `observer.pollInterval` / `--poll-interval` setting controls that status refresh only, defaulting to `5s`.

Status requests go directly to each database Pod IP on TCP 8000. `--status-port-tls` in the instance container's command/arguments selects HTTPS; otherwise its configured endpoint uses HTTP. HTTPS verifies the existing PostgreSQL server CA and the configured `serverName`, defaulting to `<cluster>-rw.<namespace>.svc`. No database login, client certificate, operator private key, or Kubernetes Pod proxy is involved. Application topology delivery remains gRPC streaming.

Metadata work scales with relevant changed Clusters; status work scales with instances belonging to active Clusters, not the number of client streams. An event for one Cluster does not resample all databases. Each plugin replica has its own metadata watches, demand tracking, and status probes; subscribers on different replicas are not deduplicated. `observer.maxConcurrentClusters` limits whole-Cluster collections (`32` by default), while `observer.maxConcurrency` caps simultaneous instance probes (`128`). Separating these limits avoids filling every probe slot with partial collections from too many Clusters. The Kubernetes client QPS/burst settings (`100`/`200` in current source) apply to metadata and named CA reads, not direct status requests.

During a role change, the observer retries the affected Cluster. A changed standby cannot by itself withdraw a separately verified healthy primary; primary identity, fencing, readiness, and role checks still determine write eligibility. Metadata watch disconnection stops renewal of cached observations, and snapshots expire according to their existing validity deadline.

A changed Cluster route or primary Pod cancels the obsolete observation immediately and queues its replacement without failure backoff. Ordinary replica bookkeeping does not cancel a healthy primary observation. One eighth of each worker/probe limit is reserved for urgent work, with at least one reserved slot when the limit exceeds one. Defaults therefore allow 28 ordinary workers plus four urgent-only workers, and at most 112 background probes within the 128-probe total. Ordinary workers can also take urgent work. Both worker classes share per-Cluster coalescing, and urgent verification retries retain their priority. This bounds interference from slow background endpoints. If the expected primary fails verification, remaining probes for that Cluster are canceled and an unavailable snapshot is published promptly. When the expected primary succeeds, replicas are still checked for conflicting primary roles; a slow replica can therefore delay that observation until its request timeout.

## CNPG switchover timing

The plugin's `probeTimeout: 2s` is a maximum request duration, not an obligatory delay. CNPG 1.30 separately defaults its primary-lease retry period to two seconds. For a Cluster where clean switchover handover latency matters, this can be tuned in the database manifest:

```yaml
spec:
  primaryLease:
    retryPeriodSeconds: 1
```

The default 15-second lease and 10-second renewal deadline remain unchanged. This increases lease API traffic, so evaluate it per Cluster rather than applying it blindly across thousands of databases. Timings are captured on first lease acquisition; instances already using the prior configuration require a controlled restart before the change takes effect. It does not guarantee a one-second total promotion. `CREATE_ANY_SERVICE` and `podMonitorEnabled` in the operator chart do not tune this behavior. See [CNPG 1.30 lease tuning](https://cloudnative-pg.io/docs/1.30/failover/#tuning-the-primary-lease) and the [measured switchover stages](performance.md#live-switchover-results).

## Capacity planning

Budget periodic instance status traffic per plugin replica using:

```text
status requests/second ≈ active instances / refresh interval in seconds
concurrency needed    ≥ status requests/second × status request duration in seconds
```

These formulas estimate the budget for a desired refresh frequency. The configured interval is a delay after a successful collection; actual gaps also include collection duration and queueing. Add headroom for bursts, timeouts, and role changes. For 6,000 active Clusters with three instances each and a `5s` refresh interval, plan for roughly 3,600 direct instance requests/second. At 20 ms per request that needs at least 72 concurrent requests before headroom. The 28 ordinary workers can occupy 84 probe slots with three-instance collections, while urgent workers retain capacity for events. Size both limits for your actual instance count and request duration: raising only the collection limit can worsen event latency by creating contention for probes.

More application services subscribing to the same Cluster share that work, though each stream still consumes memory and network bandwidth. Event-driven work receives priority over periodic refresh and initial consumer collection, with ordinary workers admitting background work regularly to avoid starvation. A healthy primary promotion that produces a watched event can be observed quickly even with a slower periodic interval. A sync/async change without such an event waits for a periodic check.

For a small installation, a faster refresh can be configured directly in values:

```yaml
observer:
  pollInterval: 100ms
```

At 6,000 active three-instance Clusters, a true 100 ms fleet refresh would require about 180,000 instance requests/second and at least 3,600 simultaneous requests at 20 ms each, before processing overhead. This exceeds the supported probe cap of 1,024. It is not ten requests/second to the CNPG controller, and increasing CPU limits alone cannot provide that refresh rate.

Cold startup has a separate Kubernetes API budget. Each newly requested Cluster needs its named CA Secret fetched. Current source defaults to 100 QPS / 200 burst per Kubernetes client, up from 20 / 40 in `0.0.5`. Before network latency, the rate budget alone is roughly three seconds for 500 distinct CA reads and eight seconds for 1,000. Concurrent reads of the same namespaced Secret with identical certificate metadata share one in-flight request; different certificate metadata is isolated. There is no new shared persistent CA cache: per-Cluster freshness, metadata invalidation, and refetch on primary TLS verification failure remain in place.

Warm status checks reuse CA and TLS transport caches. Increasing status concurrency alone does not accelerate cold API reads. For 6,000 distinct CA reads, the default rate budget is about 58 seconds; the [large-installation values](../examples/values-large.yaml) use 200 QPS / 400 burst, reducing that floor to roughly 28 seconds. Queueing, request deadlines, retries, and competing traffic can make initialization longer. Apply the larger profile where the Kubernetes API has that capacity. The matching unreleased `cnpgconnect-go` source raises default startup timeout from 10 to 30 seconds; earlier caller deadlines still apply. Stagger larger fleet restarts or configure a longer startup timeout. Neither change requires a new protobuf API.

Periodic CA refresh also consumes API capacity, averaging about `active Clusters / 240` reads/second with the staggered three-to-five-minute cache: about 25 reads/second for 6,000 active Clusters. Include that load and all plugin replicas when choosing API limits.

Informer caches retain only the Cluster/Pod fields needed for routing, and exclude large Pod environments, volumes, managed fields, and unrelated Cluster configuration. Cache size still scales with watched metadata; snapshots, subscriber channels, and gRPC transports add memory proportional to active Clusters and connected clients. For a large installation, an explicit resource allocation such as the following is a starting point to measure, not a capacity guarantee:

```yaml
resources:
  requests:
    cpu: "2"
    memory: 1Gi
  limits:
    cpu: "2"
    memory: 2Gi
```

The default chart requests `250m` CPU / `256Mi` memory and limits the container to `2` CPUs / `1Gi`. The larger example reserves more CPU and memory for a busy fleet. Go 1.26 [automatically derives and updates GOMAXPROCS](https://go.dev/src/runtime/debug.go) from available CPUs, affinity, and the Linux cgroup CPU quota. CPU requests affect scheduling, not this runtime setting. Leave `GOMAXPROCS` unset and do not call `runtime.GOMAXPROCS` with a positive value if you want automatic updates. Fractional quotas are rounded up and the runtime normally keeps at least two execution threads, so GOMAXPROCS is not always numerically equal to a fractional CPU limit. Startup logs include the selected value and observation limits.

Namespace scoping can partition metadata and status work into shards, with one discovery address per shard. Coordinate operator scope and native plugin registration: the default chart advertises `connect.cnpg.io`, and multiple releases visible to one operator are not independent native registrations. Additional replicas can spread client connections but duplicate status work when clients for the same Cluster land on several replicas; there is no shared ownership across replicas. Keep TCP 8000 network access in place for every shard or replica.

The [performance guide](performance.md#synthetic-fleet-benchmark) records the 6,000-Cluster synthetic workload, event percentiles, realized refresh gaps, timeout scenarios, and commands for reproducing the two-CPU container run. With warm caches and simulated 20 ms responses, event p95 was 28.3 ms at the `5s` setting; the observed median refresh gap was 5.08 seconds. Setting `100ms` still produced a 4.92-second median gap under that load. These measurements do not establish capacity for 6,000 real databases or a strict sub-50 ms bound. More CPU alone does not remove the instance-I/O budget.

## Watch scope and access

`watchNamespace` creates a Role and RoleBinding in that namespace, binding the plugin ServiceAccount in the release namespace. The watched namespace must already exist. An empty value creates cluster-scoped RBAC and watches all namespaces. Clusters in scope are published automatically unless explicitly disabled.

| Resource | Verbs | Use |
| --- | --- | --- |
| `postgresql.cnpg.io/clusters` | `get`, `list`, `watch` | Cluster identity, configuration, and status |
| Core `pods` | `get`, `list`, `watch` | Instance identities and addresses |
| Core `secrets` | `get` | Referenced PostgreSQL server CA's public `ca.crt` |

The observer fetches the Secret named by `status.certificates.serverCASecret`, extracts public certificate PEM blocks from `ca.crt`, and publishes them as connection metadata. It never publishes private keys or PostgreSQL passwords. Kubernetes RBAC authorizes retrieval of the whole referenced Secret; it cannot restrict access to a single data key. There is no Secret list/watch or Pod proxy permission. Namespace-scoped deployment limits metadata and Secret access.

Allow plugin Pods to reach the Kubernetes API and database Pod IPs on **TCP 8000**. With NetworkPolicy, permit both plugin egress and matching database Pod ingress across namespaces. This direct instance-manager connection is separate from application discovery (`8080`/Service `443`) and PostgreSQL client traffic (`5432`). The chart does not create a NetworkPolicy. A local observer also needs a route to the Pod network; Kubernetes API access alone is insufficient.

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

PostgreSQL public CA metadata is cached for a per-Cluster interval between three and five minutes. A stable hash spreads refreshes across that window to avoid a synchronized fleet-wide Secret fetch. Changes to the Cluster's certificate metadata invalidate it immediately; a TLS certificate verification failure on the primary status probe also triggers a named Secret refetch on the next attempt. Ordinary network failures do not discard a valid CA cache entry. The bounded refresh covers rotations that do not change Cluster metadata and runs only while a Cluster has demand. A changed CA changes the topology revision, and applications receive it through discovery. CNPG still manages the database's own certificate lifecycle.

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
| `--poll-interval` | `5s` direct status refresh for Clusters with discovery consumers |
| `--ttl` | `15s` snapshot validity and unary demand lease; must exceed status refresh interval plus probe timeout |
| `--probe-timeout` | `2s` per-instance direct status request timeout |
| `--max-concurrency` | `128` parallel status probes across active Clusters |
| `--max-concurrent-clusters` | `32` whole-Cluster collections at once; independent of the instance probe limit |
| `--kube-api-qps`, `--kube-api-burst` | `100`, `200` per Kubernetes client for metadata/CA reads; excludes direct status probes |
| `--kubeconfig` | Optional explicit kubeconfig; otherwise in-cluster credentials |
| `--insecure` | Local development without either listener's TLS or bearer authentication; rejects TLS/token flags |
| `--version` | Print version and exit |

For larger deployments, size status concurrency, refresh interval, and TTL for the number of actively consumed instances and observed request duration. Increasing Kubernetes QPS does not accelerate direct status requests. Keep the TTL above refresh interval plus probe timeout; the library's default maximum accepted snapshot TTL is 30 seconds, so adjust its advanced setting if increasing the plugin TTL beyond that.

Info-level `topology changed` logs include namespace, Cluster, primary, availability/reason, member role/sync/readiness, and observation duration. Unchanged-routing freshness renewals do not produce these logs. They describe what the plugin observed; database promotion completion and an application's first successful request remain separate timestamps.

## Validation and compatibility

The observer targets the CNPG 1.30.0 instance-manager status format. See [the validation record](validation.md) for completed tests and [integration tests](../test/e2e/README.md) for lifecycle scenarios. Local tests do not establish the behavior of a particular Gateway, external database LB, or network partition. Normal unit checks do not mutate a Kubernetes Cluster.

Two live switchovers with plugin `0.0.5` and `cnpgconnect-go v0.0.5` measured 154/148 ms from PostgreSQL readiness to a usable discovery update, versus 1,573/2,784 ms with `0.0.4`. See [performance measurements](performance.md#live-switchover-results) for deployment settings, calibrated stage boundaries, application recovery, and the limits of this comparison.
