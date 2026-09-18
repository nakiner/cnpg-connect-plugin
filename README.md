# cnpg-connect-plugin

PostgreSQL topology discovery for **CloudNativePG 1.30.0**, with a **gRPC streaming API** and the companion [cnpgconnect-go](https://github.com/nakiner/cnpgconnect-go) library.

An application supplies three things:

1. The discovery address.
2. The database's Kubernetes namespace and CNPG Cluster name.
3. Its PostgreSQL username and password.

The plugin discovers the database name, PostgreSQL certificate authority, instance addresses, and current roles. The Go library establishes verified connections, follows topology changes, and reconnects automatically. No connection URL, Kubernetes credentials, discovery token, or PostgreSQL CA file is required in the normal application setup. Database credentials go directly to PostgreSQL; the discovery service never receives them.

```go
import connectpool "github.com/nakiner/cnpgconnect-go/pgxpool"

pool, err := connectpool.Open(ctx, connectpool.Config{
    Address:   "topology.example.com:443",
    Namespace: "databases",
    Cluster:   "app-db",
    Username:  "app",
    Password:  password,
})
if err != nil {
    return err
}
defer pool.Close()
```

Primary connections are the default. The library also supports standby policies, native pgx, `database/sql`, and Bun. See [Connect applications](#connect-applications).

**Release status:** the simplified setup and connection metadata described here require a new plugin and client release. Plugin `v0.0.3` predates these changes. Until the new artifacts are published, build this checkout with the [local image and chart instructions](#run-this-checkout). The OCI commands below use `CNPG_CONNECT_VERSION`, set to the published version containing these changes, without a leading `v`.

## Install in Kubernetes

The operator installs discovery once. Applications then use the three settings above. You need CNPG 1.30.0, Helm with OCI support, cert-manager, and an HTTP/2-capable Gateway with a publicly trusted certificate for the discovery hostname. Existing TLS Secrets can replace cert-manager; see [certificate options](docs/deployment.md#certificates).

The example uses release `connect` in the operator namespace `cnpg-system`, watching an existing database namespace `databases`. Keep one release per CNPG operator installation; use `replicaCount` for additional replicas.

### 1. Install the plugin

Save this as `values.yaml`, or use [examples/values-gateway.yaml](examples/values-gateway.yaml):

```yaml
fullnameOverride: cnpg-connect
watchNamespace: databases
tls:
  application:
    enabled: false # TLS terminates at the Gateway below.
```

Install the published OCI chart:

```sh
helm upgrade --install connect oci://ghcr.io/nakiner/charts/cnpg-connect-plugin \
  --version "${CNPG_CONNECT_VERSION:?Set the published release version}" \
  --namespace cnpg-system \
  -f values.yaml \
  --wait --timeout 5m
```

The chart selects the matching `ghcr.io/nakiner/cnpg-connect-plugin` image, supporting `linux/amd64` and `linux/arm64`. No `helm repo add`, separate backend, sidecar, or custom database CRD is needed. Public GHCR packages need no registry login. See [registry access and releases](docs/releasing.md) if your packages are private.

The chart still manages the separate CNPG-I certificates. `tls.application.enabled: false` changes only the application listener: it speaks HTTP/2 without TLS to the Gateway, and its Service advertises `appProtocol: kubernetes.io/h2c`. Keep that backend Service internal.

### 2. Publish one discovery address

Route your discovery hostname through an existing TLS Gateway using [Gateway API GRPCRoute](https://gateway-api.sigs.k8s.io/reference/api-types/grpcroute/). For example, save and apply this `GRPCRoute`, replacing the hostname and Gateway reference with yours:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: GRPCRoute
metadata:
  name: cnpg-connect
  namespace: cnpg-system
spec:
  parentRefs:
    - name: gateway-external
      namespace: networking
      sectionName: https
  hostnames:
    - topology.example.com
  rules:
    - backendRefs:
        - name: cnpg-connect-api
          port: 443
```

The Gateway listener must admit routes from `cnpg-system`, support gRPC/h2c upstreams, and present a certificate for `topology.example.com` trusted by application system roots. Point that DNS name at the Gateway. The Service's `443` port forwards cleartext HTTP/2 to the plugin's `8080` backend in this setup; TLS ends at the Gateway.

This single platform configuration provides a verified TLS endpoint to every application. No per-application certificate mounts or discovery tokens are needed. A direct TLS LoadBalancer and private CA deployments are also supported; see [deployment options](docs/deployment.md#discovery-endpoint-options).

### 3. Connect the application

Use the `pgxpool.Open` example at the top of this page with the discovery hostname, namespace/Cluster, and PostgreSQL login. Existing Clusters in the plugin's watch scope are discovered automatically; no annotation or native `spec.plugins` entry is needed.

The plugin reads the database name from CNPG bootstrap configuration and the public PostgreSQL CA from the Cluster's server CA Secret. It does not read database passwords. The default is the bootstrap application database. For a Cluster hosting several application databases, use the client's advanced explicit connection configuration to select another database.

Automatic observation stays outside CNPG's reconciliation path, so discovery outages do not add a dependency to CNPG failover. Explicit opt-out, optional endpoint parameters, and native CNPG-I enrollment are covered in [observation modes](docs/deployment.md#observation-modes-and-cnpg-availability).

The library tries an instance's internal address first, then its advertised external address when needed. Applications do not need a network setting. The operator provides reachable per-instance external endpoints when applications cannot reach the Pod network; see [external PostgreSQL endpoints](docs/deployment.md#external-clients-and-database-addresses).

## Verify discovery

From this source checkout, query your published address using `grpcurl`:

```sh
grpcurl \
  -import-path proto -proto cnpg/connect/v1/topology.proto \
  -d '{"namespace":"databases","name":"app-db"}' \
  topology.example.com:443 cnpg.connect.v1.TopologyService/GetTopology
```

Replace `GetTopology` with `WatchTopology` to watch the stream. The API does not require a database login or a discovery token by default. Reflection is not enabled, so supply the proto from the installed plugin release.

A healthy response has `available: true`, a future `validUntil`, a `primaryId`, eligible `members`, and `connection` metadata containing the default database and public PostgreSQL CA. The CA is encoded as base64 in protobuf JSON. Each streamed message replaces the previous snapshot; refreshes can keep the same revision while extending its validity.

The library handles stream renewal and reconnection. `grpcurl` is a diagnostic client and must be restarted when its stream ends. See the [API contract](docs/api.md) for role, freshness, and failure semantics.

## Connect applications

Use [cnpgconnect-go](https://github.com/nakiner/cnpgconnect-go) and its matching release. The simple `pgxpool.Open` call above discovers connection defaults and owns its discovery client. Keep the returned pool for the application's lifetime and close it during shutdown.

The same model is available through `github.com/nakiner/cnpgconnect-go/stdlib` for `*sql.DB`. Bun wraps that SQL handle. Applications do not need their own watchers, role-change callbacks, connection URLs, or reconnect loops.

The plugin reports transitions in every direction: synchronous → asynchronous, synchronous → primary, primary → standby, and subsequent changes. CNPG performs promotion, fencing, and replication configuration. “Read-only discovery” describes the plugin's access, not a restriction to standby databases.

Connections acquired after a topology change follow the current policy. Existing transactions can still fail during a switchover or failover; the library does not replay writes or transactions. Synchronous/quorum replication also does not guarantee that every replica has replayed a specific commit. See the library's usage documentation for additional routing policies and advanced overrides.

The library imports the canonical protobuf messages and generated streaming client from `github.com/nakiner/cnpg-connect-plugin/api/connect/v1`. Importing that package does not compile the plugin's observer or Kubernetes packages into the application.

## Architecture

```mermaid
flowchart LR
    A[Application / cnpgconnect-go] -->|Verified TLS / gRPC stream| G[TLS Gateway]
    G -->|HTTP/2 / internal Service| P[cnpg-connect-plugin]
    P -->|Read topology and public PostgreSQL CA| K[Kubernetes API]
    A -->|Verified PostgreSQL TLS / database login| D[Selected PostgreSQL instance]
    O[CNPG operator] -.->|Optional native integration / mTLS| P
```

| Listener | Service | Purpose |
| --- | --- | --- |
| CNPG-I `:9090` | `cnpg-connect:9090` | Operator integration with mutual TLS |
| Discovery `:8080` | `cnpg-connect-api:443` | Application gRPC API; TLS or internal h2c behind a TLS Gateway |
| Health `:8081` | No Service | Kubernetes HTTP liveness/readiness probes |

The application API is read-only and tokenless by default: anyone who can reach it can discover observed Clusters in the configured watch scope. Only public connection metadata is returned, never PostgreSQL passwords or private keys. An optional bearer token is available for deployments that want it; see [optional discovery authentication](docs/deployment.md#optional-discovery-authentication).

## Configuration reference

See [chart/values.yaml](chart/values.yaml) for all values and [runtime flags](docs/deployment.md#runtime-flags) for process options.

| Helm value | Default | Purpose |
| --- | --- | --- |
| `fullnameOverride` | Empty | Use `cnpg-connect` for the resource names in this guide |
| `watchNamespace` | Empty | One namespace, or all namespaces when empty |
| `replicaCount` | `1` | Independent observer replicas |
| `image.repository`, `image.tag` | GHCR image, matching chart release | Override when testing a local/custom build |
| `application.service.type`, `.port` | `ClusterIP`, `443` | Discovery Service |
| `tls.application.enabled` | `true` | Disable for an internal h2c backend behind a TLS Gateway |
| `tls.application.existingSecret` | Empty | Existing discovery server certificate when plugin TLS is enabled |
| `application.auth.existingSecret`, `.key` | Empty, `token` | Optional bearer authentication; empty means no token required |
| `tls.certManager.enabled`, `.createIssuer` | `true`, `true` | Generate CNPG-I and, when enabled, discovery certificates |
| `observer.pollInterval`, `.ttl`, `.probeTimeout` | `5s`, `15s`, `2s` | Observation refresh, expiry, and per-instance probe timeout |
| `observer.maxConcurrency` | `8` | Parallel instance probes |
| `observer.kubeAPIQPS`, `.kubeAPIBurst` | `20`, `40` | Kubernetes client request limits |
| `serviceAccount.create`, `rbac.create` | `true`, `true` | Chart-managed identity and permissions |

The observer has read access to Clusters, Pods, Pod status proxies, and Secrets in its watch scope. Secret access is `get` only and is used to fetch the referenced PostgreSQL server CA; the API publishes certificate blocks only. Set `watchNamespace` to keep this scope local to your database namespace when practical. The TTL must exceed the poll interval plus probe timeout; the client expires stale snapshots automatically.

Cluster parameters are optional: `serverName` overrides the default PostgreSQL TLS identity `<cluster>-rw.<namespace>.svc`; `externalEndpoints` provides per-instance external addresses. Set parameters with the optional `connect.cnpg.io/parameters` annotation, or in a native plugin entry when using that integration.

## Operations

Inspect the plugin with:

```sh
kubectl -n cnpg-system logs deployment/cnpg-connect --tail=100
kubectl -n cnpg-system get pods,services -l app.kubernetes.io/instance=connect
kubectl -n cnpg-system rollout status deployment/cnpg-connect --timeout=180s
```

`/healthz` checks process liveness; `/readyz` reports observer initialization. Querying topology confirms that a particular database currently has a usable routing view.

Upgrade using the same OCI command and values file with a new published `CNPG_CONNECT_VERSION`. Helm uses the matching image by default. Keep certificate, Gateway, and endpoint settings in your deployment configuration. Use `helm history connect -n cnpg-system` and `helm rollback connect REVISION -n cnpg-system --wait` to return to a previous Helm revision; rollback does not restore external Secrets or database state.

Existing installations that set `application.auth.existingSecret` retain bearer authentication until that value is cleared. To move to the simple setup, remove that setting, configure the TLS Gateway, and use a plugin/client release containing connection metadata. PostgreSQL login credentials remain unchanged.

Additional replicas observe independently; Kubernetes API traffic scales with their count. Streams reconnect to any healthy replica and receive a complete snapshot. The chart does not create a PodDisruptionBudget, HPA, or NetworkPolicy. See [deployment and operations](docs/deployment.md) for certificate rotation, private endpoint configurations, network behavior, and RBAC details.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| OCI `not found` / `unauthorized` | Selected release published successfully; chart package visibility or Helm registry login |
| `ImagePullBackOff` | Image package access, image tag, and node architecture; chart and image are separate GHCR packages |
| Certificate/Issuer kinds missing | Install cert-manager or supply existing CNPG-I TLS Secrets |
| `GRPCRoute` not accepted | Gateway listener name, allowed namespaces, hostname/certificate, and h2c upstream support |
| TLS unknown-authority / hostname error | Discovery endpoint's publicly trusted certificate and DNS name; private CA deployments need explicit client trust |
| gRPC `Unauthenticated` | An optional bearer Secret is still configured; clear it for the default tokenless setup or provide its token |
| `NotFound` | Namespace/name, watch scope, and explicit observation disablement |
| `connection_defaults_unavailable` | CNPG server CA Secret exists, contains valid public `ca.crt`, and is readable by the plugin |
| Database name cannot be discovered | Configure CNPG's bootstrap application database or use the client's advanced `ConnConfig` |
| Snapshot unavailable or expired | Primary transition, Pod readiness/fencing, instance status access, or API throttling |
| Discovery works but SQL does not | Database credentials and reachability of the advertised member addresses; external clients need external mappings |
| Streams disconnect periodically | Normal connection renewal or Gateway/LB idle timeout; the library reconnects |

## Run this checkout

Building requires Go **1.26.4+**. Run the local checks with:

```sh
make build
make check
make race
```

To deploy the current source before publishing a release, build and push your own image, then install the local chart. Replace the registry with one your Kubernetes nodes can pull from:

```sh
make image IMAGE=registry.example.com/cnpg-connect-plugin:dev
docker push registry.example.com/cnpg-connect-plugin:dev
helm upgrade --install connect ./chart \
  --namespace cnpg-system \
  -f examples/values-gateway.yaml \
  --set image.repository=registry.example.com/cnpg-connect-plugin \
  --set-string image.tag=dev \
  --wait --timeout 5m
```

The build architecture must match the nodes. For a multi-platform image, use `docker buildx build --platform linux/amd64,linux/arm64 --tag IMAGE --push .`. Configure the Gateway as above; Clusters are discovered automatically. The updated client must also be built from the matching checkout until its release is published.

For a local observer, use an explicit kubeconfig:

```sh
./bin/cnpg-connect-plugin \
  --kubeconfig=/path/to/kubeconfig --namespace=databases \
  --insecure \
  --plugin-address=127.0.0.1:9090 \
  --discovery-address=127.0.0.1:8080 \
  --health-address=127.0.0.1:8081
```

`--insecure` disables both listeners' TLS for local development; the Helm chart never enables it. Use `grpcurl -plaintext` for this local endpoint. `--discovery-plaintext`, used by the Gateway chart values, disables only application-listener TLS and preserves CNPG-I mutual TLS.

`make generate` regenerates the protocol bindings with pinned Go generators under `work/bin`. The [release workflow](https://github.com/nakiner/cnpg-connect-plugin/actions/workflows/release.yaml) publishes versioned GHCR images and OCI charts. See [release instructions](docs/releasing.md), the [validation record](docs/validation.md), and [opt-in integration tests](test/e2e/README.md).

## Uninstall

If you configured native `connect.cnpg.io` entries, remove them from Clusters before removing the plugin; preserve other plugins. The default automatic observation has no native reconciliation dependency. Move applications off this discovery endpoint, remove its `GRPCRoute`, then run:

```sh
helm uninstall connect --namespace cnpg-system --wait --timeout 5m
```

Keep the CNPG operator running while its plugin Service finalizer completes. The database and CNPG operator remain installed. Externally managed TLS/token Secrets and Gateway certificates are not owned by this release.

## License

A license has not been selected. The plugin reports `UNLICENSED` in CNPG-I metadata; no open-source license grant is included.
