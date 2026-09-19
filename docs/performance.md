# Performance

The live switchover and original fleet results below describe plugin **0.0.5**, released on September 18, 2026. Later sections record the source and settings used for each qualification run. Component tests and live measurements cover different parts of the system; neither is a production latency guarantee. Historical measurements retain the toolchain and transport used at the time.

## How work scales

The plugin maintains shared Kubernetes Cluster, Pod, and Secret metadata watches. Relevant events enqueue only affected Clusters; repeated events coalesce. CA material is shared by referenced Secret and refreshed on Secret changes, without periodic CA reads. Status collection runs for Clusters with active discovery consumers, and subscribers to the same Cluster share that collection within one plugin replica. Additional replicas observe independently, so they can duplicate status work.

The default limits are 32 concurrent Cluster observations and 128 instance probes. Four workers and 16 probe slots are reserved for urgent work, within those totals. A primary or routing change cancels obsolete observations and queues a replacement immediately. Unrelated slow background checks cannot occupy the reserved capacity; a slow instance in the affected Cluster can still delay its own verification.

Kubernetes events do not carry every PostgreSQL replication-state change. The plugin also checks active instances directly on port 8000, with a `5s` background interval by default. Actual gaps include collection duration and queueing. Changing this interval does not set a fixed delay on primary-change events. Sync/async changes without a Kubernetes event wait for a periodic check. See [event delivery](deployment.md#event-delivery-and-scaling) and [capacity planning](deployment.md#capacity-planning).

## Unreleased scalability changes

The current source addresses startup bursts, failed-primary observations, and large numbers of application streams:

- **Observer publication:** metadata validation and store commits remain atomic under the observer lock. Subscriber delivery and topology logging happen after unlocking. A slow log sink no longer holds the global observer lock; logging remains synchronous and can still occupy the calling worker or event handler. A shared-CA invalidation delivers all affected notifications before logging them. Monotonic subscriber ordering rejects late notifications after invalidation or deletion.

- **CA reads:** default Kubernetes request limits increase from 20 QPS / 40 burst to 100 / 200. Clusters share a cache keyed by CA Secret identity and resource version. Secret metadata watches invalidate changed/deleted entries, and active consumers trigger a fetch. This replaces timed per-Cluster CA refreshes and requires Secret `list/watch` permissions as well as `get`.
- **Failed primaries:** when the expected primary cannot be verified, remaining instance probes are canceled and the unavailable result is published without waiting for slow standbys. A healthy primary still requires the existing replica/conflicting-primary checks. Unrelated databases retain the existing reserved urgent capacity.
- **TLS status clients:** cached transports support concurrent lookups. Parsing new trust material and closing retired idle connections happen outside the global cache lock. CA rotation still changes the trust used for new probes and never disables verification.
- **Stream fan-out:** all gRPC watchers of an observation share an immutable snapshot and a lazily built protobuf. Mailboxes hold one pending update, and publication order prevents delayed delivery from restoring an older state. Subscriber payload copying and delivery happen outside the store lock; direct Go `Store.Subscribe` callers still receive independent copies. The measured native gRPC transport shared write buffers between idle connections. Current discovery admission uses the standard library HTTP/2 server; those transport timings must be remeasured for the current server.
- **Matching Go client:** unreleased `cnpgconnect-go` raises its default startup wait to 30 seconds, honors earlier caller deadlines, retains exponential jitter when streams repeatedly send a snapshot and close, and uses a freshness deadline timer instead of a 100 ms expiry ticker. Public configuration and protobuf APIs are unchanged. SQL operations are not replayed.

Regression tests cover certificate rotation, shared CA reads and late results, transport cleanup during active requests, snapshot expiry and delete/recreate ordering, competing publications, and a client stalled by real HTTP/2 flow control. A slow stream cannot block unrelated clients, and disconnecting it releases its subscription.

### Current admission and recovery behavior

The production discovery listener performs authorization and capacity admission at
an HTTP/2 handler boundary, before gRPC creates a client-supplied deadline or
reads a request body. Both plaintext and TLS clients still use the ordinary gRPC
API. This avoids retained deadline timers on native gRPC tap rejection. Initial
request timeouts close incomplete bodies; completed watches retain no initial
message timeout. Accepted sockets, HTTP/2 streams, active RPCs, and watches each
have their own bound. Connection shutdown, rejection, malformed messages and
incomplete frames are covered by real-transport regression tests.

Canceled RPCs get a 100 ms grace to flush terminal trailers, followed by a
stream-scoped write deadline. A peer withholding its response window cannot
retain an expired RPC's writer indefinitely. Plaintext and TLS regressions
verify actual write/flush and handler termination, a stream reset, and continued
unary calls and watch updates on the same connection.

The default per-replica limits are 1,024 sockets, 4,096 RPCs, 2,048 watches, and
128 concurrent streams per connection. Raise the matching `application.limits`
values only with measurements of your actual client connection and subscription
patterns. The 6,000-Cluster component workload below bypasses those limits; it
is observer/fan-out evidence rather than qualification of 6,000 production
application connections. See [production configuration](production.md).

### Production transport under sustained load, September 19, 2026

[TestDiscoveryRecoveryLoad](../internal/server/load_test.go) exercises the actual
admission handler, connection limit, HTTP/2 server, TLS and gRPC discovery API.
The run used Go 1.27.1, gRPC 1.84.0, Linux/arm64, a two-CPU cgroup quota and a
2 GiB memory limit. Go selected `GOMAXPROCS=2` without an environment override.

- 2,000 independent TCP/TLS connections and watches, spread over 200 synthetic
  database records, with five publications per second per record.
- Three one-minute workload periods, separated by two complete discovery-server
  stops. Each stop included 500 ms of deliberate downtime. The store retained
  synthetic topology; real observer cold starts are measured separately.
- Explicit admission capacity: 2,064 connections, 2,064 watches and 2,128 RPCs.
  This is above the installation's default 1,024 connection limit.

| Measurement | Result |
| --- | ---: |
| Delivered observations | 1,800,000 |
| Delivery p50 / p95 / p99 bucket upper bounds | 37 / 57 / 93 ms |
| Maximum observed delivery | 121.7 ms |
| Initial connection of every client | 776.7 ms |
| Stop to every client receiving fresh state, two restarts | 1.806 / 1.794 s |
| Live Go heap after GC, three periods | 208.1 / 214.8 / 211.5 MiB |
| Goroutines after each period | 24,003 / 24,003 / 24,003 |
| Container memory peak | 715.2 MiB |
| CPU throttling | 8 of 1,848 periods; 8.1 ms total |

Delivery time starts immediately before each `Store.Put` and ends at client
protobuf receipt. Histograms have bounded storage. Every client must receive the
latest marker after each period/restart; topology regression and retained server
admission after shutdown fail the test. Post-GC heap growth is also bounded, but
three minutes cannot establish absence of small leaks.

**Client and server run in the same container.** Memory, CPU and goroutine counts
include all load generators and must not be treated as plugin-only resource
requirements. The payload includes roughly 2 KiB of synthetic CA data. This test
excludes Kubernetes watches, PostgreSQL status probes, application SQL pools and
external network/LB delays; the library's live recovery soak covers real pools.

To run a shorter host check, or compile it into the same resource-limited Linux
container used for the component tests below:

```sh
CNPG_LOAD_CLIENTS=2000 CNPG_LOAD_DATABASES=200 CNPG_LOAD_SECONDS=180 \
GOWORK=off go test -tags=loadtest -run '^TestDiscoveryRecoveryLoad$' \
  -count=1 -timeout=6m -v ./internal/server
```

The host command alone does not enforce a CPU or memory limit. Configurable
ranges are 1–10,000 clients, 1–client-count databases, and 6–1,800 seconds;
larger values need matching file descriptor and resource budgets.

### Many-pool recovery, September 19, 2026

The paired runner passed all nine selected suites without skips or failures
against Kubernetes 1.34.0, unmodified CNPG 1.30.0 and PostgreSQL 18.4. The fixture
had two plugin replicas and three database instances. Runtime source stayed
unchanged during the run; the disposable kind cluster was removed afterward.

The new library soak retained 32 independent application pools (16 pgx and
16 `database/sql`), with two connections and two query workers per pool. It
warmed all 64 connections before injecting faults and completed five promotion /
complete-discovery-outage cycles in 3m11s. The three-minute target finishes the
last cycle rather than stopping midway through recovery.

| Measurement | Result |
| --- | ---: |
| Promotion request to every worker querying the selected primary | 3.151–5.127 s, five samples |
| Discovery scale-up to every worker querying again | 8.637–11.878 s, five samples |
| Successful primary queries / total attempts | 68,939 / 70,140 |
| Query errors during the fault-injection run | 1,201 |
| Successful-query p50 / p95 / p99 bucket upper bounds | 8.192 / 16.384 / 262.144 ms |
| All-attempt p99 bucket upper bound, including outage deadlines | 4,194.304 ms |
| Sampled application connections on the current primary | Peak 64 across 465 samples |
| Standby results observed by the primary workload | 0 |

Each outage waited for every discovery Pod to stop and for topology to expire.
Every pool then had to reject acquisition with its request deadline while a
direct PostgreSQL connection still worked. Restoring discovery recovered the
original handles. The test checked committed sentinel data throughout, never
replayed workload queries, and verified zero application connections on the
current primary after closing the pools.

Recovery includes CNPG/PostgreSQL processing, Pod startup and client reconnect
backoff; it is not plugin-only delivery latency. The connection peak is sampled
on the current primary, not across all instances, and pgx may refill connections
gradually after a pool reset. These are short local recovery measurements, not
fleet-capacity or external-network guarantees. The existing suites separately
cover prepared statements, primary Pod loss and a discovery stall with open TCP
sockets. See the [runner instructions](../test/e2e/README.md) to repeat the soak;
weekly CI enables it, and manual CI runs can opt in.

### Slow replicas and primary safety

A ready replica may reveal a previously unknown conflicting primary even after
the expected primary has answered. Skipping a fenced replica also loses evidence
needed if it is later unfenced while unreachable. The delayed-response regression
in [slow_replica_test.go](../internal/observer/slow_replica_test.go) covers both.
The observer therefore retains the bounded role checks before publication.
Lowering `probeTimeout` trades earlier exclusion of slow instances against fewer
successful observations; it is not a free latency improvement.

Primary conflict evidence now records an actual inconsistent primary observation,
not merely the history of a once-healthy primary. A completed CNPG failover can
recover while the former primary is unreachable. Actual conflicts survive
metadata changes, failed probes, deletion and removal of temporary fencing until
verified demotion or replacement resolves them. The matching Go client accepts
explicit member withdrawals from older same-incarnation snapshots without
rolling back healthy routes or extending their TTL, and a silent reconnected
stream is retried when the cached usable topology expires.

The matching client also retries transient connection-establishment failures
during `Open`, within its existing startup deadline. Authentication, certificate
and application-hook errors remain terminal. This does not replay application
queries or transactions.

### Secret-watch and pgx qualification, September 19, 2026

After adopting Secret metadata watches, shared CA caching, VictoriaMetrics
histograms and native pgx connection retirement, the paired runner passed eight
suites with no failures or skips. It used Kubernetes 1.34.0, CNPG 1.30.0,
PostgreSQL 18.4, two plugin replicas and three database instances, with Go 1.27.1.
The disposable cluster was removed after completion.

| Event | Samples | Observed range |
| --- | ---: | ---: |
| CA Secret bundle update to existing gRPC stream | 1 | 38.3 ms |
| Planned switchover to usable application handles | 3 | 4.150–5.084 s |
| Primary Pod deletion to usable application handles | 3 | 8.231–25.111 s |
| Discovery restart | 1 | 12.238 s |
| Discovery traffic resumed after expiry | 1 | 60.1 ms |

The [CA test](../test/e2e/ca_watch_test.go) changes public bundle bytes while
retaining the existing root, verifies unchanged Cluster certificate metadata,
and restores the Secret. This measures event delivery for a bundle update.
The application recovery samples include PostgreSQL/CNPG processing and retain
the original pgx, SQL and prepared-statement handles. They are smoke samples,
not percentiles or a production latency guarantee.

The updated cold-start test made exactly one GET for 500 Clusters sharing one
CA, 500 GETs for 500 distinct CAs and 1,000 GETs for 1,000 distinct CAs. The
6,000-Cluster warm test also passed. These synthetic runs used the development
host; they are not directly comparable to the earlier two-CPU measurements.

### Earlier paired recovery qualification, September 19, 2026

Before the Secret-watch and pgx Reset changes, the September 19 plugin and client candidate passed eight suites in a disposable
kind cluster with Kubernetes 1.34.0, unmodified CNPG 1.30.0, PostgreSQL 18.4,
two plugin replicas, and a three-instance database. Both used Go 1.27.1. The
shell runner completed all eight Go suites with no skips or failures, collected
metrics from both plugin replicas, and removed its owned cluster. Runtime source
was held unchanged during the run. This candidate includes the canceled-response
write-deadline fix; focused HTTP/2 regressions also passed under race detection.

Coverage included three client lifecycle repetitions, each with a planned
switchover and primary Pod deletion, certificate renewal, plugin/discovery
outages, and a discovery stall that kept TCP sockets open. Native pgx,
`database/sql`, and prepared-statement handles recovered without replacement.
The stall test also verified that expired discovery stopped managed connections
while a direct PostgreSQL connection remained healthy.

| Recovery event | Samples | Observed range |
| --- | ---: | ---: |
| Planned switchover | 3 | 3.134–5.036 s |
| Primary Pod deletion | 3 | 20.252–24.346 s |
| Discovery restart | 1 | 9.252 s |
| Discovery traffic resumed after expiry | 1 | 44.1 ms |

Switchover and deletion timings start before the corresponding Kubernetes
operation and end when all three application handles report the new writable
primary. They include CNPG recovery, local forwarding, and probe deadlines;
they do not isolate plugin latency and are not directly comparable to the
PostgreSQL-ready intervals below. The stall recovery timing starts when traffic
resumes, after the test has already observed expiry. These small samples are
regression evidence, not production percentiles or fleet-capacity measurements.
See the [isolated runner instructions](../test/e2e/README.md) to run the paired
qualification against the source you intend to publish.

### Cold startup and independent TLS clients

Measured September 18, 2026, from unreleased source in native Linux/arm64 containers limited to **2 CPUs / 2 GiB**, using Go 1.26.4 with automatic `GOMAXPROCS`. Scenarios ran sequentially. These results supplement the earlier warm-cache test:

| Component workload | p50 | p95 | Maximum | Additional observation |
| --- | ---: | ---: | ---: | --- |
| 500 databases, distinct CA Secrets, cold cache | 0.547 s | 2.799 s | **3.048 s** | 500 CA GETs |
| 1,000 databases, distinct CA Secrets, cold cache | 3.044 s | 7.545 s | **8.042 s** | 1,000 CA GETs |
| 500 databases, shared CA and identical certificate metadata | 0.433 s | 0.815 s | **0.863 s** | 18 overlapping CA-read batches |
| 500 independent TLS clients, 30 publications to every client | 2.990 ms | **5.832 ms** | 8.823 ms | p99 7.730 ms; 15,000 receipts |

These cold-start measurements predate the Secret metadata watch and shared persistent CA cache. The [cold-start test](../internal/observer/cold_start_test.go) used the real Kubernetes REST client and its 100 QPS / 200 burst limiter against a loopback HTTP API. CA responses and simulated instance responses each took 20 ms. It started from empty CA caches with already-synchronized metadata, using 32 Cluster workers and 128 probes; readiness was observed through local Store subscriptions. Peak concurrent CA requests were 28 for distinct Secrets and one for the shared case. It excluded informer initialization, real Kubernetes server load, status TLS, application gRPC, and database connection setup. The shared case required identical Cluster certificate metadata in that earlier implementation; current sharing is by Secret identity/version.

The [TLS client test](../internal/discovery/tls_load_test.go) gives each client its own loopback TCP connection, verified TLS 1.3 session, HTTP/2 transport, and gRPC stream. Every client receives each publication before the next round begins. Timing starts before `Store.Put` and ends after the client decodes its protobuf; it excludes metadata observation and SQL. Store publication p95 was 0.250 ms. Initial connections were established sequentially in 304 ms; this does not measure simultaneous TLS handshakes. Server and test clients share the same container and CPU quota. Certificates are held in memory in this transport test; production mounted-credential reloading is covered separately by server tests. The regression suite also exercises a concurrent 32-client reconnect wave and cancellation under actual flow-control backpressure.

### Store fan-out cost

A separate Go microbenchmark on an Apple M3 Max host compared the former copied-snapshot path with the new shared gRPC path, using a roughly 2 KiB CA payload. This measures Store publication only, excluding socket writes and protobuf serialization:

| Subscribers to one database | Former publish time | Shared publish time | Former allocated bytes | Shared allocated bytes |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 0.827 ms | 0.040 ms | 3.73 MB | 17.6 KB |
| 20,000 | 16.09 ms | 0.790 ms | 74.26 MB | 173.6 KB |

At 20,000 subscribers, publication allocation count fell from 120,036 to 23. Direct Go subscribers still pay for their owned copies, outside the global mutex. These microbenchmarks do not establish capacity for 20,000 independent network clients.

The original 6,000-Cluster warm fleet test was also repeated under the same two-CPU container limit after these changes:

| Background | Refresh setting | Event p95 | p99 | Maximum | Median refresh gap |
| --- | --- | ---: | ---: | ---: | ---: |
| Healthy | `5s` | 28.4 ms | 40.7 ms | 51.9 ms | 5.05 s |
| Healthy | `100ms` | 28.1 ms | 40.8 ms | 41.2 ms | 4.86 s |
| Timed-out background; changed Clusters healthy | `5s` | 25.1 ms | 26.0 ms | 26.2 ms | No samples |
| Timed-out background; changed Clusters healthy | `100ms` | 24.6 ms | 25.7 ms | 25.9 ms | No samples |

This retains the earlier test's boundaries: fake status responses, warm caches, one in-memory gRPC transport, and 100 injected changes per scenario. The harness creates a bare gRPC server, bypassing production admission. Its 6,000 streams exceed current defaults of 128 streams per connection, 2,048 watch RPCs and 1,024 accepted connections per replica; it does not demonstrate that capacity with installation defaults. The new work reduces cold-start and high-fan-out costs; it does not materially shorten the already fast warm event path or guarantee a 50 ms maximum.

## Live switchover results

Two planned switchovers were repeated against an existing two-instance CNPG 1.30.0 Cluster: primary A → synchronous standby B, then B → A after replication recovered. The before/after deployments used plugin and `cnpgconnect-go` versions `0.0.4` and `0.0.5`, respectively. Measurements were taken on September 18, 2026.

The new plugin ran as one replica with CPU request/limit 2/2, memory request/limit 1Gi/1Gi, 32 Cluster workers, 128 probes, a `5s` background interval, `2s` probe timeout, and Kubernetes QPS/burst 20/40. Discovery used internal plaintext gRPC; PostgreSQL connections used verified TLS. The application's CPU request/limit was 250m/500m. The new Cluster configuration also used `primaryLease.retryPeriodSeconds: 1`, compared with the earlier two-second default; its 15-second lease and 10-second renewal deadline remained unchanged.

All durations below are milliseconds. Each cell is **before → after**. Some intervals contain others; the rows must not all be added together.

| Interval | A → B | B → A |
| --- | ---: | ---: |
| First relevant CNPG controller action → PostgreSQL ready | 2,642 → **1,654** | 2,657 → **1,658** |
| Primary lease acquisition | 2,012 → **1,014** | 2,015 → **1,013** |
| PostgreSQL ready → instance-manager promotion completion | 128 → **116** | 118 → **112** |
| Instance-manager completion → usable discovery received | 1,444 → **38** | 2,666 → **36** |
| PostgreSQL ready → usable discovery received | 1,573 → **154** | 2,784 → **148** |
| Discovery received → new application PostgreSQL session | 2.75 → **2.00** | 3.00 → **1.91** |
| PostgreSQL ready → successful application response, original test definition | 1,591 → **485** | 2,802 → **262** |
| PostgreSQL ready → sustained application success, same definition | 1,591 → **485** | 6,148 → **262** |

Ready-to-discovery delay fell **90.2% / 94.7%**. Plugin logs measured the observations publishing the new primary at **44.00 / 42.13 ms**. Observation start to stream receipt was **44.42 / 42.46 ms**. The observation overlaps the end of CNPG bookkeeping, which explains the shorter 38/36 ms interval after instance-manager completion.

Plugin publication logs preceded stream receipt by approximately **0.43 / 0.35 ms**. These figures include timestamp placement and should not be interpreted as isolated network latency. `observed_at` marks the observation time, not the moment the stream was sent; its age also includes observation work.

The actual application performed a user upsert followed by a read through its deployed managed pool. SQL probes sampled both instances every 100 ms after completion. Application calls used a 1.5-second deadline and a 200 ms pause after each completion. The original successful-response metric requires a request to start after the SQL probe first observes the new primary writable. During A → B, an already-running request actually succeeded **278 ms after PostgreSQL readiness**; the metric excludes it because it started 15 ms before readiness. The next request accounts for the 485 ms comparable result. The earliest successful B → A response was 262 ms.

One request per new switchover exceeded its deadline, versus two and six previously. The captured new rounds contained 80 and 121 requests. No further unavailable discovery snapshot or failed request appeared after recovery during the remaining approximately 13-second and 22-second windows. In the earlier return switchover, a later discovery withdrawal had extended sustained recovery to 6.15 seconds. The demoted members now returned through unknown, async standby, and sync standby without withdrawing the healthy primary during these tests.

### Measurement boundaries

- PostgreSQL readiness comes from its own `database system is ready to accept connections` log. CNPG completion is the instance manager's `Finished setting myself as primary` message. Controller timing starts at its old-primary-unhealthy label log, not an API audit timestamp.
- One in-cluster probe timestamped a separate `WatchTopology` stream, direct SQL observations, and responses from the real application. Its new database sessions were correlated with the application Pod's address. The application's internal topology callback was not instrumented.
- Node clocks differed by about 724 ms. Logs were mapped to the probe's monotonic timeline with nearby SQL `clock_timestamp()` samples and query midpoints. Twenty low-latency samples per target had maximum round trips of 0.380/0.616 ms and offset ranges of 0.053/0.071 ms. Current and previous container logs were combined after demotions.
- CNPG configuration, plugin implementation/resources, and the client version changed together. This comparison does not isolate each change's contribution. The roughly one-second lease acquisition is confirmed by logs; it is separate from the plugin's latency improvement.
- These are two planned switchovers in a small deployment, not latency percentiles, crash or network-partition failovers, or evidence of capacity at 6,000 real databases. All versions retained primary-role and fencing verification. No SQL replay was added.

## Synthetic fleet benchmark

The opt-in [TestFleetLoad harness](../internal/observer/load_test.go) exercises the observer, snapshot publication, protobuf serialization, and gRPC streaming. The September 18, 2026 measurements used the observer implementation released in `0.0.5`:

- 6,000 Clusters, three instances each, and one live `WatchTopology` stream per Cluster.
- One shared gRPC client connection over `bufconn`, an in-memory transport.
- Linux/arm64 container with an actual two-CPU cgroup quota and 2 GiB memory limit; Go 1.26.4 selected `GOMAXPROCS=2` automatically.
- Warm metadata and CA caches, 32 Cluster workers, 128 probe slots, and simulated 20 ms instance-status responses.
- 100 distinct primary changes injected 10 ms apart during background work; about seven seconds measured per scenario.

Event latency starts at the simulated metadata change entering the observer and ends when the stream receives a usable snapshot identifying the new primary. It excludes API-server watch delivery and real PostgreSQL promotion.

| Background status behavior | Refresh setting | Event p50 | p95 | p99 | Maximum | Median observed refresh gap |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Healthy, 20 ms | `5s` | 22.4 ms | **28.3 ms** | 37.8 ms | 55.0 ms | 5.08 s |
| Healthy, 20 ms | `100ms` | 23.0 ms | **30.7 ms** | 33.1 ms | 43.9 ms | 4.92 s |
| Background times out; changed Clusters healthy | `5s` | 22.2 ms | **25.1 ms** | 35.7 ms | 37.4 ms | No samples |
| Background times out; changed Clusters healthy | `100ms` | 22.2 ms | **25.4 ms** | 29.3 ms | 29.5 ms | No samples |

Healthy cases started approximately **3,612 / 3,679 instance probes per second**. This counts attempts, including any canceled requests; it is not a query or transaction throughput metric. In the timeout cases, ordinary fake requests waited longer than their two-second deadline while the changed Clusters responded in 20 ms. Urgent capacity kept those changes moving, but no repeated successful background-refresh gaps were sampled during the run. The harness prints `refresh_gap=0` for this empty sample set; it does not mean zero refresh latency or that fleet freshness was maintained.

The healthy runs sampled **146.5 / 189.5 MiB of Go heap allocation**. These are single-point `HeapAlloc` readings of the combined observer, fake Kubernetes clients, and in-process gRPC clients. They are not peak memory, Pod RSS, or a recommended production memory limit.

The measured `100ms` setting still produced an approximately five-second refresh gap under this load. For 18,000 active instances, an actual 100 ms fleet-wide refresh would require roughly **180,000 status requests/second**, with at least 3,600 concurrent requests at 20 ms each before overhead. Lowering the interval alone cannot provide that rate. The measured 55 ms maximum also rules out describing these results as a strict sub-50 ms guarantee.

This test excludes cold CA lookup, informer initialization, Kubernetes watch delivery, real network/TLS and instance I/O, independent application connections, many subscribers to one Cluster, and application pool reconnection. It runs too briefly to establish steady-state behavior through CA refresh cycles or sustained outages. The harness asserts that warm observation makes no Kubernetes API calls; this does not mean the live plugin never uses the Kubernetes API.

### Reproduce the component test

From the current plugin repository with Go 1.27.1, run the same component workloads below. New results describe the current code and are not reproductions of the historical toolchain. To reproduce a historical release, use its corresponding source and toolchain together:

```sh
GOWORK=off GOTOOLCHAIN=go1.27.1 go test -count=1 -tags=loadtest \
  -run '^TestFleetLoad$' -v ./internal/observer

CNPG_LOAD_SLOW_BACKGROUND=1 GOWORK=off GOTOOLCHAIN=go1.27.1 \
  go test -count=1 -tags=loadtest -run '^TestFleetLoad$' -v ./internal/observer
```

Run the additional component workloads with:

```sh
GOWORK=off GOTOOLCHAIN=go1.27.1 go test -count=1 -tags=loadtest \
  -run '^TestColdStartFleet$' -v ./internal/observer
CNPG_STREAM_CLIENTS=500 GOWORK=off GOTOOLCHAIN=go1.27.1 \
  go test -count=1 -tags=loadtest -run '^TestTLSClientLoad$' -v ./internal/discovery
GOWORK=off GOTOOLCHAIN=go1.27.1 go test -run '^$' \
  -bench '^BenchmarkStoreFanout$' -benchmem -benchtime=300ms ./internal/discovery
```

The fleet tests use fake Kubernetes clients; cold-start tests use a loopback API and real client-go throttling. None connect to a live Kubernetes cluster. A normal host run does not reproduce CPU throttling. For the measured Linux/arm64 configuration on an ARM64 Docker host, compile and run the test under an actual CPU quota:

```sh
bench_dir="$(mktemp -d)"
GOWORK=off GOTOOLCHAIN=go1.27.1 CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go test -c -tags=loadtest -o "$bench_dir/observer.test" ./internal/observer
GOWORK=off GOTOOLCHAIN=go1.27.1 CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go test -c -tags=loadtest -o "$bench_dir/discovery.test" ./internal/discovery

cat > "$bench_dir/Dockerfile" <<'EOF'
FROM scratch
COPY observer.test /observer.test
COPY discovery.test /discovery.test
ENTRYPOINT ["/observer.test"]
EOF

docker build --platform linux/arm64 -t cnpg-connect-fleet-bench "$bench_dir"
docker run --rm --platform linux/arm64 --network=none --cpus=2 --memory=2g \
  cnpg-connect-fleet-bench -test.run='^TestFleetLoad$' -test.v -test.timeout=120s
docker run --rm --platform linux/arm64 --network=none --cpus=2 --memory=2g \
  -e CNPG_LOAD_SLOW_BACKGROUND=1 cnpg-connect-fleet-bench \
  -test.run='^TestFleetLoad$' -test.v -test.timeout=120s
docker run --rm --platform linux/arm64 --network=none --cpus=2 --memory=2g \
  cnpg-connect-fleet-bench -test.run='^TestColdStartFleet$' -test.v -test.timeout=120s
docker run --rm --platform linux/arm64 --network=none --cpus=2 --memory=2g \
  -e CNPG_STREAM_CLIENTS=500 --entrypoint /discovery.test cnpg-connect-fleet-bench \
  -test.run='^TestTLSClientLoad$' -test.v -test.timeout=120s
```

On an AMD64 host, use `GOARCH=amd64` and `linux/amd64` together and record the architecture with your results. Avoid comparing emulated runs as though they were native. Leave `GOMAXPROCS` unset; setting it alone is not a substitute for a cgroup CPU quota. `CNPG_LOAD_WORKERS` is a test-only override, not a production configuration variable.

## Operational limits

The live event path is now tens of milliseconds after CNPG completes its role transition, while CNPG itself still took about 1.65 seconds from the first controller action to PostgreSQL readiness. More frequent background polling would not remove that promotion time.

The new component tests add cold API reads and independent TLS clients, but still do not establish an end-to-end production capacity limit. Informer synchronization, real API-server delays, PostgreSQL status latency, network loss, CPU throttling, and long outages all affect recovery. When the expected primary is healthy, slow members still consume verification time. At large scale, API budgets for CA reads and background instance traffic remain separate constraints; more CPU cannot remove them. Use the [capacity guide](deployment.md#capacity-planning) and [large-installation values](../examples/values-large.yaml), then size against measured refresh gaps and client counts. The unreleased changes have not yet been deployed for another live switchover test.
