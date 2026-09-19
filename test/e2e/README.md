# Live topology integration tests

These opt-in tests use a real CNPG 1.30 deployment and PostgreSQL instances. They connect over verified TLS, verify CNPG plugin registration, observe async-to-sync membership changes, request a planned switchover through CNPG's status API, and reconnect to the complete topology stream. Optional standby replacement verifies identity changes even when the Pod name stays the same.

The only supported Kubernetes context is **`kind-cnpg-connect-test`**. Tests require an explicit kubeconfig and recheck that context before every mutation. Mutations are limited to Cluster `databases/app-db`, its owned Pods, and (for separately enabled tests) Deployment `cnpg-system/connect`, its application Certificate, and the database's public CA bundle, with resource UID checks. The lifecycle test restores the original synchronous replication policy at the end; a promoted primary remains promoted.

Prepare these fixtures before running:

- CNPG 1.30 and Cluster `app-db` using a supported PostgreSQL image, namespace `databases`, three healthy instances, `1Gi` storage, native `spec.plugins` entry `connect.cnpg.io` enabled.
- Plugin Helm release in `cnpg-system`, `fullnameOverride=connect`.
- Secret `cnpg-system/connect-application-tls`, key `ca.crt`, containing the discovery listener's issuing CA.
- A running port forward or reachable discovery endpoint, with its correct TLS certificate name.

Example invocation:

```sh
CNPG_CONNECT_E2E_KUBECONFIG=/absolute/path/to/isolated-kind-kubeconfig \
CNPG_CONNECT_E2E_ENDPOINT=127.0.0.1:7443 \
CNPG_CONNECT_E2E_SERVER_NAME=connect-api.cnpg-system.svc \
CNPG_CONNECT_E2E_REPLACE_STANDBY=1 \
go test -tags=e2e -count=1 -timeout=10m -v ./test/e2e
```

Discovery is tokenless by default. To test an installation with optional bearer authentication, also set `CNPG_CONNECT_E2E_TOKEN_SECRET=cnpg-connect-auth` (or its actual Secret name); the suite then reads its `token` key and checks authentication behavior. The token must contain at least 32 characters. The private discovery CA fixture is for this isolated test setup; normal applications can use a publicly trusted discovery endpoint.

Omit `CNPG_CONNECT_E2E_REPLACE_STANDBY` to skip standby deletion/recreation. Without `CNPG_CONNECT_E2E_KUBECONFIG`, the suite skips and does not contact Kubernetes. The suite has a nine-minute overall deadline, three-minute transition deadlines, and a bounded cleanup period. It does not print credentials or create/delete Kubernetes clusters.

The switchover request follows the [CNPG 1.30 promote implementation](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/cmd/plugin/promote/promote.go): an optimistic-lock status patch updates `targetPrimary`, `targetPrimaryTimestamp`, and the switchover phase. CNPG remains responsible for fencing and promotion.

## Observer outage and independent failover

Run `TestLiveAnnotationOutage` separately, with an image supporting automatic observation. The test name and environment variable retain their earlier annotation-mode names for compatibility:

```sh
CNPG_CONNECT_E2E_KUBECONFIG=/absolute/path/to/isolated-kind-kubeconfig \
CNPG_CONNECT_E2E_ENDPOINT=127.0.0.1:7443 \
CNPG_CONNECT_E2E_SERVER_NAME=connect-api.cnpg-system.svc \
CNPG_CONNECT_E2E_ANNOTATION_OUTAGE=1 \
go test -tags=e2e -count=1 -timeout=10m -run '^TestLiveAnnotationOutage$' -v ./test/e2e
```

This test retains existing discovery parameters, removes only `connect.cnpg.io` from `spec.plugins`, and uses automatic observation without an enrollment annotation. It then scales the fixed `cnpg-system/connect` Deployment to zero, waits until its Pods are absent, deletes the elected PostgreSQL primary Pod with a UID precondition, and verifies that CNPG elects another primary and recovers all three ready instances while discovery remains absent. It restores the original observer replica count and checks Deployment readiness. Cleanup also restores replicas if an assertion fails.

**Automatic observation remains configured after this test; native enrollment is not restored.** Other native plugins are preserved. The test has an eight-minute deadline plus bounded cleanup. Restart a Pod-bound port forward after the observer restarts, then call `GetTopology` and `WatchTopology` to verify discovery recovery; the test itself uses Kubernetes readiness for its final recovery check because the original port forward can point at the deleted observer Pod.

After restarting the port forward, run the read-only recovery smoke test with the same base environment variables:

```sh
go test -tags=e2e -count=1 -timeout=2m -run '^TestLiveTopologySmoke$' -v ./test/e2e
```

`TestLiveTopologySmoke` performs verified TLS `GetTopology` and `WatchTopology` requests (with bearer metadata only when configured), verifies a healthy three-member snapshot and consistent Cluster/primary identity, and works with automatic observation or native enrollment. It does not mutate Kubernetes resources or require native plugin registration.

## Run the complete isolated flow

Prerequisites: Docker, kind, kubectl, Helm, jq, OpenSSL and Go matching `go.mod`.
Install kind with `go install sigs.k8s.io/kind@v0.33.0`, then run from this checkout:

```sh
scripts/run-isolated.sh /absolute/path/to/cnpgconnect-go work/isolated-results 3
```

Arguments are the client checkout, a **new** output directory, and the number of
client lifecycle repetitions (default 1, maximum 20). The runner builds the local
plugin and uses a temporary Go workspace to test both local modules together.
Keep both source trees stable while it runs.

The runner refuses an existing `kind-cnpg-connect-test` cluster. It creates a
private kubeconfig, installs cert-manager and CNPG 1.30, and deploys two plugin
replicas plus three PostgreSQL instances. All Kubernetes commands use that
explicit isolated context. Ports 7443 and 7541–7543 on loopback must be free.
A unique bind mount marks the owned kind node; cleanup deletes that node by
container ID even on a failed run or SIGTERM. An unrelated or replacement node
is never deleted. SIGKILL or a host crash can prevent cleanup; inspect the local
fixture before removing anything manually. Do not run two copies concurrently.

The pinned PostgreSQL image is pulled normally. For an existing image cache,
`POSTGRES_ARCHIVE=/absolute/path/postgres.tar` loads a standard Docker or OCI
image archive with `kind load image-archive`; the archive must contain the exact
reference in [the fixture](../../scripts/fixtures.yaml). Archive creation and
registry transport are outside this runner.

All assertions live in the Go suites. They cover native enrollment, sync/async
membership, standby replacement, switchover, certificate renewal on every plugin
replica, a CA Secret bundle update with unchanged Cluster certificate metadata,
failover while discovery is absent, repeated client promotion and
primary deletion, discovery outage, and an open TCP connection that stops
forwarding data. Existing pgx, SQL and prepared SQL handles must recover.
The runner rejects skipped or missing selected tests and reads metrics from
both plugin replicas.

`CNPGCONNECT_GO_E2E_RECOVERY_SLO=60s` is the default per-transition bound. It
measures action-start to successful queries, including PostgreSQL recovery; it
is not a discovery-only latency claim. The output directory contains ordinary
Go test JSON logs with individual recovery measurements, metrics, Pod/event
diagnostics, and a `result` file. Only `passed` means every selected suite and
cleanup completed. Kubeconfigs, Secrets and credentials remain temporary.
Three repetitions are smoke evidence; they do not establish a production p99.

To add sustained queries through independent pgx and SQL pools, repeated
promotions, and complete discovery outages, enable the optional recovery soak:

```sh
CNPGCONNECT_GO_E2E_SOAK=1 \
CNPGCONNECT_GO_E2E_SOAK_CLIENTS=32 \
CNPGCONNECT_GO_E2E_SOAK_POOL_SIZE=2 \
CNPGCONNECT_GO_E2E_SOAK_DURATION=3m \
scripts/run-isolated.sh /absolute/path/to/cnpgconnect-go work/isolated-soak 3
```

The Go test checks the configured connection budget against PostgreSQL's limit
and records sampled connection peaks, query latency and recovery of every pool.
The same handles survive every transition. Duration covers the workload period;
setup and the last complete fault cycle add time. This exercises application
fan-out to one database, not thousands of real databases or production network
partitions. See the client integration guide for the workload and bounds.

Both repositories run this same script from `integration.yaml` on pull requests,
weekly and on manual dispatch. Their `peer_ref` input selects the other module's
revision; use matching commits when qualifying changes before publication.
Weekly runs include the 32-pool, 64-session, three-minute recovery soak. Manual
dispatch exposes a `soak` checkbox; pull-request runs retain the shorter suites.
When selected, CI requires the soak's passing result explicitly, including when
the counterpart checkout supplies the runner.
