# Validation record

## September 19, 2026: Secret watches and dependency adoption

The current plugin/client pair passed eight suites with no failures or skips in
a disposable kind cluster: Kubernetes 1.34.0, unmodified CNPG 1.30.0, PostgreSQL
18.4, two plugin replicas and three database instances, using Go 1.27.1.
The runner removed its owned cluster after collecting both replicas' metrics.
No production cluster was changed.

A CA Secret bundle update reached an existing gRPC stream in 38.3 ms while the
Cluster's certificate metadata stayed unchanged. The test retained the existing
root and restored the bundle afterward; it tests Secret-event delivery, not a
complete root-CA migration. Discovery leaf renewal passed on both replicas.
Three client lifecycle runs preserved the same pgx, database/sql and prepared
statement handles through switchover and primary Pod deletion. Discovery outage
and stalled-connection recovery also passed. See [performance](performance.md)
for measured recovery times and their limits.

Plugin `make check race build`, client `make check`, source/binary govulncheck,
Helm checks, ShellCheck, actionlint, and tagged integration compilation passed.
Cold-start and 6,000-Cluster synthetic tests passed; 500 Clusters sharing a CA
required one Secret GET. Regression tests cover Secret-only rotations,
deletion/recreation, late reads/publications, idle-snapshot invalidation, retry
pacing, pool ownership, and borrowed connections during native pgx Reset.

The chart now needs Secret `list/watch` permissions alongside `get`. Upgrade
the chart with the image, or update manually managed RBAC. The protobuf API and
application configuration remain compatible. These results do not establish
production fleet capacity or validate an external gateway/network partition.

## September 18, 2026: unreleased scalability improvements

Current source after `0.0.5` passed `make check race build`: formatting, vet,
unit and race tests, chart lint/rendering, and binary compilation. Integration
tests also compile with the `e2e` tag; no live deployment or switchover was run
for these changes. The matching `cnpgconnect-go` changes passed `make check`,
including race tests and pgx, database/sql, and Bun example checks.

The new tests cover shared public-CA reads and cancellation, CA rotation,
transport cleanup during requests, failed-primary cancellation, immutable
snapshot fan-out, expiry/deletion/recreation ordering, reconnect bursts, and
real HTTP/2 flow-control stalls. Existing fencing and primary verification
tests continue to pass.

Sequential Linux/arm64 component runs under a two-CPU / 2 GiB container limit
completed 1,000 cold database discoveries in 8.04 seconds and delivered updates
to 500 independent TLS clients at 5.83 ms p95. The repeated 6,000-Cluster warm
simulation retained approximately 28 ms event p95. See the
[performance guide](performance.md#unreleased-scalability-changes) for settings,
commands, and exclusions. These tests do not establish sustained production
capacity or replace validation of a published build in its target cluster.

## September 18, 2026: live performance and synthetic fleet tests

Plugin `0.0.5` and `cnpgconnect-go v0.0.5` completed two planned switchovers
against CNPG 1.30.0 using automatic Cluster observation, tokenless internal
gRPC discovery, connection metadata, and the deployed application's managed
pool. Usable discovery followed PostgreSQL readiness by 154/148 ms. The
application recovered in both directions; the original primary, synchronous
standby, and test cleanup were verified. The separate 6,000-Cluster benchmark
used simulated instance I/O and an in-memory gRPC transport.

The [performance guide](performance.md) records versions, settings, before/after
measurements, test commands, and measurement limits. These runs do not validate
an external Gateway/LB, sustained production fleet capacity, or network-partition
failover.

## September 17, 2026: isolated lifecycle tests

The following record describes the earlier bearer-authenticated implementation.
Those isolated lifecycle tests support optional bearer authentication and can
be rerun against a new release; they are not a rerun of every scenario on `0.0.5`.

Local validation on 2026-09-17 used an isolated `kind-cnpg-connect-test` cluster,
with an explicit kubeconfig separate from the user's Kubernetes contexts:

- CloudNativePG 1.30.0, CNPG-I 0.5.0.
- Three PostgreSQL instances using `ghcr.io/cloudnative-pg/postgresql:18.4-system-trixie`.
- kind 0.33.0, Kubernetes 1.37.0, cert-manager 1.21.2.
- A locally built Linux ARM64 image and the chart's generated certificates.
- Application gRPC reached through a local port forward with certificate-name
  verification and bearer authentication.

The final native-mode lifecycle run passed in 30 seconds. It verified CNPG-I
registration, rejection of missing bearer credentials, initial primary plus two
async replicas, a streamed change to one sync replica, planned switchover and
old-primary recovery, a complete initial snapshot on reconnect, and standby
replacement with a changed Pod UID under the same name.

The annotation-mode outage run passed in 25 seconds. It removed native plugin
enrollment, stopped all discovery Pods, deleted the elected primary Pod, and
verified that CNPG promoted another instance and recovered all three ready
instances while discovery remained stopped. The observer then restarted
successfully. Primary loss here was a Kubernetes Pod deletion, not a simulated
node/network partition or an abrupt database-process kill.

A separate read-only TLS smoke test then confirmed that both `GetTopology` and
`WatchTopology` returned the complete recovered topology from the restarted
observer, with the new primary and replacement Pod identity.

The initial live switchover test exposed an incorrect timeline-equality check.
CNPG reports checkpoint timeline metadata, which lagged on otherwise healthy
streaming standbys. Eligibility now uses database identity and the current
primary's unique streaming sender state, with standby readiness and replay
checks. A regression test covers the lagging checkpoint timeline.

Review also exposed a graceful-shutdown timeout with active streaming RPCs.
Runtime cancellation now reaches those handlers. A regression test verifies
prompt shutdown, and another verifies bounded forced shutdown for a handler
that ignores cancellation.

Local Go checks cover role classification, fencing, freshness expiry, API
outages, changing Pod identity during observation, stream cancellation,
subscription races, TLS configuration/rotation, authentication, and shutdown
with an active stream. Helm checks cover default, external-LB and existing-TLS
configurations plus required-value validation. The image builds and starts as
an unprivileged user with a read-only filesystem and no Linux capabilities.

Passing checks: `go test -race ./...`, `go vet ./...`, protobuf lint, tagged e2e
compilation, Helm lint/render validation, and Docker build/smoke checks. The
tested local image ID is
`sha256:a3bd8cf2bbba3755cc69a38ca8764974481ee5b40969b793a3a79a77f6252481`.

See [the opt-in integration test instructions](../test/e2e/README.md) to reproduce
live checks. These tests are restricted to the named isolated kind context;
ordinary `go test ./...` does not mutate Kubernetes resources.

This is not production acceptance. The actual external LB, per-instance SQL
endpoints, sustained load, multi-node/network partitions, and coordinated
certificate/token rotation still need environment-specific validation. The Go
connection library and automatic pool replacement are a separate implementation.
