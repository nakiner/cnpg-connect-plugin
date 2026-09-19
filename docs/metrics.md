# Operational metrics

The plugin exposes Prometheus text format (`version=0.0.4`) at **GET /metrics on
its health listener only**. It is not registered on either discovery or CNPG-I
listeners. Scrape the configured health address using your existing monitoring
network policy. This endpoint is unauthenticated, like health probes: keep the
health port private; never expose it through a public discovery service.

All names below have the `cnpg_connect_` prefix. Counters reset on process
restart. Labels are fixed enumerations only: no namespace, cluster, pod, user,
token, method, error text, connection data, or Secret content is exported.

## Observation and Kubernetes

| Metric suffix | Type | Meaning |
|---|---|---|
| `observation_duration_seconds` | histogram | Whole collection, including CA lookup and slot waiting; all returns, including failures/cancellation |
| `probe_duration_seconds` | histogram | Executed instance-status calls, including failed/canceled calls; excludes waiting for capacity |
| `probe_queue_seconds` | histogram | Every probe-slot acquisition attempt, including canceled attempts |
| `probes_active` | gauge | Occupied global probe slots (includes a just-acquired slot before its goroutine begins) |
| `probe_slot_saturation_total` | counter | Capacity gates that could not be acquired immediately; a request can encounter both background and global gates |
| `probe_slot_cancellations_total` | counter | Slot acquisitions returning cancellation/deadline errors |
| `observation_queue_depth` | gauge | Work ready in the observation queue; excludes delayed work and in-flight observations |
| `ca_cache_hits_total` / `ca_cache_misses_total` | counter | Per-collection public-CA cache lookup outcomes |
| `ca_failures_total` | counter | Cache-miss refresh failures, including missing certificate metadata |
| `ca_read_failures_total` | counter | Actual named-Secret reads or public-certificate validation failures; shared reads count once |
| `metadata_watch_up{resource="clusters\|pods\|secrets"}` | gauge | Current Kubernetes watch health (1 or 0) |
| `metadata_watch_starts_total` | counter | Successfully established metadata watches |
| `metadata_watch_ends_total` | counter | Ended metadata watches, including planned renewals/shutdown |
| `metadata_watch_errors_total` | counter | Failed watch establishment or received Kubernetes watch error events |

Histogram buckets, in seconds: 0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30,
+Inf. Each histogram exports cumulative `_bucket{le="..."}`, `_count`, `_sum`.
Quiet metadata watches are deliberately renewed by the observer; starts/ends
are not themselves failure signals. Use watch health/errors and snapshot
freshness together rather than alerting on normal renewals.

## Discovery publication

| Metric suffix | Type | Meaning |
|---|---|---|
| `watchers` | gauge | Registered topology subscribers, including watchers waiting for recreation |
| `snapshots{state="available\|unavailable\|expired"}` | gauge | Current snapshot inventory by mutually exclusive state; expiry is evaluated at scrape time |
| `snapshot_oldest_age_seconds` | gauge | Maximum nonnegative age of a nonzero observation timestamp; zero if none |
| `publications_total` | counter | Complete publications, including same-routing refreshes, expiry invalidations and deletion tombstones |
| `snapshot_expirations_total` | counter | Existing fresh records invalidated by expiry processing; already-expired puts do not increment this |
| `watch_coalesced_total` | counter | Pending subscriber mailbox deliveries replaced by newer complete state, including refreshes/tombstones |

The oldest-age gauge includes dormant inventory: idle clusters intentionally do
not receive periodic PostgreSQL observations. An old inventory entry alone is
not evidence of an outage. State totals and age describe stored evidence, not
per-client delivery latency. Coalescing is expected for slow consumers and
never implies partial topology delivery.

## Discovery admission

| Metric suffix | Type | Meaning |
|---|---|---|
| `admission_in_use{kind="connections\|rpcs\|watches"}` | gauge | Accepted sockets (including pre-handshake), admitted RPCs, and admitted watch RPCs |
| `admission_limit{kind="connections\|rpcs\|watches"}` | gauge | Corresponding configured process limits |
| `admission_rejections_total{reason="auth\|capacity"}` | counter | Pre-body authentication/capacity rejections |
| `admission_initial_timeouts_total` | counter | Initial request timers firing before the request body finishes, including HTTP/2 END_STREAM |

Watches consume both an RPC slot and a watch slot. Excess sockets remain in the
kernel backlog and do not count as rejected RPCs. Slots are reclaimed on unknown
methods, client disconnect, partial-request timeout, and process shutdown.
Receipt of the complete request body, including HTTP/2 END_STREAM, disables the
initial-request timer; healthy long-lived watches do not expire under that timer.
A message without END_STREAM remains subject to the deadline. Total request wire
bytes are bounded to 64 KiB plus the five-byte gRPC frame prefix, in addition to
the 64 KiB decoded-message limit.

RPC cancellation, including a client-supplied gRPC deadline, allows 100 ms for
terminal trailers to flush. A stream-scoped write deadline then interrupts a
response still blocked on HTTP/2 flow control. Other streams on the connection
remain open; this ends the blocked writer rather than only releasing its slot.

## Cost and safety

Telemetry uses VictoriaMetrics/metrics for Prometheus encoding and histograms,
with independent metric sets for each observer and scrape. Histograms have fixed-size storage;
counters are atomic or guarded by existing state locks. Scrape output cardinality
and retained telemetry memory do not scale with identity count. Publication state
aggregation scans current records and subscriber groups under the store lock:
O(records + subscriber groups), without payload copies or serialization. No
network writes occur while holding that lock or histogram locks. Scrapes do not
create demand, enqueue probes, renew snapshots, or read Secrets. An already
canceled HTTP request skips collection; health HTTP writes have a three-second
server timeout. Values across different metric families are not an atomic
process-wide snapshot.

Useful signals include sustained admission usage near its limit, increases in
capacity rejections or initial-request timeouts, slot wait histograms combined
with saturation counts, CA failures, watch errors/down state, and an unexpected
rise in expired/unavailable snapshots. Choose thresholds against the configured
refresh/TTL budgets and expected dormant inventory rather than universal values.
