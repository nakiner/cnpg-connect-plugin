# Validation record

This record describes the earlier bearer-authenticated implementation. It does
not claim live validation of the newer automatic Cluster observation, tokenless
discovery, connection metadata, or Gateway setup. The isolated lifecycle tests support optional bearer
authentication and can be rerun against the new release.

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
