# Production operations for the unreleased hardening changes

Use this source revision with its matching client. Do not assume the features
below are in an already published image. The release process is documented in
[releases.md](releases.md); metrics are documented in [metrics.md](metrics.md).

## Deployment profile

Start from [values-production.yaml](../examples/values-production.yaml), then
adapt it to the actual cluster. It is an example, not a universally safe preset.
Its selectors assume Helm release `connect` and the default chart name. The
default resource name is `connect-cnpg-connect-plugin`, and the discovery Service
is `connect-cnpg-connect-plugin-api`. Set `fullnameOverride: cnpg-connect` to use
the shorter Service names in the README; this does not change the selectors.
If you change the release name or `nameOverride`, update the spread selector too.

- Use at least two observer replicas on separate nodes; the example uses hostname
  spread with `minDomains: 2`, `maxSkew: 1` and `matchLabelKeys: [pod-template-hash]`
  so each Deployment revision spreads its own replicas. With only one eligible hostname,
  the second replica stays pending. Reserve at least two eligible nodes and
  account for node selectors and taints. Multiple observers are independent,
  not a leader-elected consensus. This placement rule is checked when scheduling;
  it does not relocate existing Pods automatically after nodes recover.
- Use the PDB (`minAvailable: 1`) and rolling update strategy
  (`maxUnavailable: 0`, `maxSurge: 1`). Reserve capacity for the surge Pod.
  A PDB does not prevent involuntary node loss, and rollout strategy—not a PDB—
  controls Deployment-driven disruption. Keep termination grace above bounded
  server/observer shutdown.
- Install a release chart with a pinned image digest, or set image.digest to an
  audited immutable manifest digest. A tag alone is not artifact identity.
- Size process limits with workload and memory budgets. Defaults are 128 streams
  per connection, 1024 accepted sockets, 4096 total admitted RPCs, 2048 watches,
  and a five-second deadline to finish the initial request body (including
  HTTP/2 END_STREAM). Watches consume both RPC and watch
  capacity. Authentication, when configured, is checked before body reception.
  These limits are not rate limiting, per-tenant quotas, or a defense against
  every volumetric denial of service: enforce edge/LB connection and request
  budgets as well.
- Keep the health/metrics port private. Expose only the discovery service through
  the intended TLS/HTTP2 gateway or direct TLS endpoint. Tokenless discovery is
  intentional; add the optional shared bearer token and network restrictions
  when the environment requires them. Never put PostgreSQL credentials in
  discovery configuration.

## Adopting these changes

Source builds and the matching Go client now require **Go 1.27.1 or newer**.
Update application build images and CI toolchains before upgrading the library;
the unchanged protobuf API does not remove the module's new compiler requirement.
Prebuilt plugin images already include the selected toolchain's compiled binary.

Admission limits are per plugin process. Count actual application client
connections and watches, including rolling-update overlap and reconnect bursts.
Many services watching one database share observation work, but each still uses
connection/RPC/watch capacity. The defaults admit at most 1,024 sockets and 2,048
watches per replica; they do not accommodate 6,000 independent clients on one
replica. With HA, size surviving replicas for the load after another replica
fails, including uneven long-lived connection distribution. Increase
`application.limits` only with corresponding measured memory/CPU capacity.

The historical 6,000-stream benchmark shared an in-memory transport and did not
exercise these admission controls. Use it to assess observer/fan-out work, not to
size socket admission. Run representative connection/fan-out tests with the
chosen limits and resources before increasing the supported client count.

## NetworkPolicy checklist

Enable the chart policy only with a CNI that enforces it. Explicitly select
permitted discovery clients or gateway Pods; an empty discovery peer list fails
rendering. Configure monitoring and CNPG operator selectors for the deployment.

The policy permits DNS to standard kube-system/kube-dns Pods, instance status to
CNPG Pod port 8000, and the configured API server ports (defaults 443 and 6443).
Restrict apiServerCIDRs to actual control-plane addresses; an empty list permits
those ports to all IPs. Set apiServerPorts if the endpoint uses another port.
CNIs differ on pre/post-Service-DNAT enforcement: confirm both destination and
port against the actual CNI. Node-local DNS, custom DNS labels, nonstandard API
ports, proxies or unusual control-plane layouts need corresponding policy
adjustments. Do not claim protection from a successfully rendered YAML alone.
The disposable kind fixture does not verify production CNI enforcement.

## Monitoring and capacity

Scrape /metrics on the private health listener. Track observation/probe/queue
histograms, slot saturation/cancellation, CA failures, metadata-watch health,
snapshot age/state, subscriber coalescing and admission utilization/rejections.
Only fixed labels are exported. Idle-cluster snapshots intentionally get old;
age by itself is not an availability alarm. Watch renewal/reconnection is also
normal—combine error counters with effective snapshot freshness.

Keep poll/probe/TTL budgets consistent with measured API and PostgreSQL status
latency. Watch disconnect/reconnect churn must share active demand schedules,
while metadata invalidations remain urgent. A partial collection may retain safe
completed observations when its I/O budget ends, but parent cancellation must
never publish new state. Historical conflicting-primary evidence is retained
across metadata changes until safe evidence clears it.

## Failure and rollout expectations

CNPG handles promotion/fencing; discovery is not a consensus protocol. Prefer
automatic observation when avoiding a dependency on the native CNPG-I path is
important. Keep replica clocks synchronized and protect the source of discovery
TLS trust. During unavailable/expired observations, clients fail closed; they do
not replay writes or transactions. Applications still need deadlines and explicit
handling of ambiguous commit results.

Run the [complete isolated flow](../test/e2e/README.md#run-the-complete-isolated-flow)
against the exact pair of checkouts before release. It rejects skipped tests,
records bounded recovery measurements and checks leaf-certificate renewal.
Three local repetitions are not production p99 or availability evidence. Validate
root-CA migration, actual CNI/gateway behavior, representative load/soak, and the
organization's SLO on staging before promoting broadly.

Publishing additionally requires repository/package permissions and policy
verification described in [releases.md](releases.md).
Source/image publication does not itself establish a redistribution license;
the owner must make that licensing decision explicitly. Never overwrite an
existing release to bypass a failed gate.
