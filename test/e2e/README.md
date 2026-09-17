# Live topology integration tests

These opt-in tests use a real CNPG 1.30 deployment and PostgreSQL instances. They authenticate over TLS, verify CNPG plugin registration, observe async-to-sync membership changes, request a planned switchover through CNPG's status API, and reconnect to the complete topology stream. Optional standby replacement verifies identity changes even when the Pod name stays the same.

The only supported Kubernetes context is **`kind-cnpg-connect-test`**. Tests require an explicit kubeconfig and recheck that context before every mutation. Mutations are limited to Cluster `databases/app-db`, its owned Pods, and (for the separately enabled outage test) Deployment `cnpg-system/connect`, with resource UID checks. The lifecycle test restores the original synchronous replication policy at the end; a promoted primary remains promoted.

Prepare these fixtures before running:

- CNPG 1.30 and Cluster `app-db` using a supported PostgreSQL image, namespace `databases`, three healthy instances, `1Gi` storage, native `spec.plugins` entry `connect.cnpg.io` enabled.
- Plugin Helm release in `cnpg-system`, `fullnameOverride=connect`.
- Secret `cnpg-system/cnpg-connect-auth`, key `token`, containing at least 32 characters.
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

Omit `CNPG_CONNECT_E2E_REPLACE_STANDBY` to skip standby deletion/recreation. Without `CNPG_CONNECT_E2E_KUBECONFIG`, the suite skips and does not contact Kubernetes. The suite has a nine-minute overall deadline, three-minute transition deadlines, and a bounded cleanup period. It does not print credentials or create/delete Kubernetes clusters.

The switchover request follows the [CNPG 1.30 promote implementation](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/internal/cmd/plugin/promote/promote.go): an optimistic-lock status patch updates `targetPrimary`, `targetPrimaryTimestamp`, and the switchover phase. CNPG remains responsible for fencing and promotion.

## Observer outage and independent failover

Run `TestLiveAnnotationOutage` separately, with an image supporting annotation enrollment:

```sh
CNPG_CONNECT_E2E_KUBECONFIG=/absolute/path/to/isolated-kind-kubeconfig \
CNPG_CONNECT_E2E_ENDPOINT=127.0.0.1:7443 \
CNPG_CONNECT_E2E_SERVER_NAME=connect-api.cnpg-system.svc \
CNPG_CONNECT_E2E_ANNOTATION_OUTAGE=1 \
go test -tags=e2e -count=1 -timeout=10m -run '^TestLiveAnnotationOutage$' -v ./test/e2e
```

This test retains existing discovery parameters, removes only `connect.cnpg.io` from `spec.plugins`, and enables `connect.cnpg.io/enabled=true` annotation enrollment. It then scales the fixed `cnpg-system/connect` Deployment to zero, waits until its Pods are absent, deletes the elected PostgreSQL primary Pod with a UID precondition, and verifies that CNPG elects another primary and recovers all three ready instances while discovery remains absent. It restores the original observer replica count and checks Deployment readiness. Cleanup also restores replicas if an assertion fails.

**Annotation enrollment remains enabled after this test.** Other native plugins are preserved. The test has an eight-minute deadline plus bounded cleanup. Restart a Pod-bound port forward after the observer restarts, then call `GetTopology` and `WatchTopology` to verify discovery recovery; the test itself uses Kubernetes readiness for its final recovery check because the original port forward can point at the deleted observer Pod.

After restarting the port forward, run the read-only recovery smoke test with the same base environment variables:

```sh
go test -tags=e2e -count=1 -timeout=2m -run '^TestLiveTopologySmoke$' -v ./test/e2e
```

`TestLiveTopologySmoke` performs authenticated TLS `GetTopology` and `WatchTopology` requests, verifies a healthy three-member snapshot and consistent Cluster/primary identity, and works with either enrollment mode. It does not mutate Kubernetes resources or require native plugin registration.
