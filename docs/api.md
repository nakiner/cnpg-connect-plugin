# Application discovery contract

The service is `cnpg.connect.v1.TopologyService`, defined in [topology.proto](../proto/cnpg/connect/v1/topology.proto). Both RPC requests identify a Cluster by `namespace` and `name`.

```protobuf
rpc GetTopology(GetTopologyRequest) returns (Snapshot);
rpc WatchTopology(WatchTopologyRequest) returns (stream Snapshot);
```

The application endpoint uses verified TLS, either directly on the plugin or at a Gateway forwarding to its internal h2c listener. Discovery is read-only and tokenless by default. Applications send PostgreSQL credentials only to PostgreSQL. Deployments may optionally require `authorization: Bearer <token>` metadata by configuring a bearer Secret.

Clusters in the configured watch scope are discovered automatically. An explicit `connect.cnpg.io/enabled: "false"` annotation disables observation. A disabled native plugin entry also excludes the Cluster; either explicit disablement wins. Native parameters take precedence over annotation parameters when a native entry exists. Unknown or disabled Clusters return `NotFound`. Invalid resource names return `InvalidArgument`. When optional bearer authentication is configured, missing/invalid bearer credentials return `Unauthenticated`. An observed Cluster with no safe routing view returns a snapshot marked unavailable; consumers must interpret the snapshot instead of treating a successful RPC as proof that a database is usable.

## Snapshot semantics

Each message is a complete snapshot for that Cluster, not a delta. It replaces
the previous routing view subject to freshness and withdrawal ordering; it must
not resurrect a known-withdrawn member through replay. There is no event replay
cursor or guarantee that every intermediate transition is delivered. Pending
updates may be coalesced for slow clients.

An unavailable/transitioning snapshot for the same Cluster UID revokes routes
even when another observer replica carries an older observation timestamp.
Keep the positive observation high-water mark: a whole-snapshot withdrawal needs
a strictly newer positive observation to recover. An equal-time snapshot can
withdraw individual members without making the healthy primary unavailable;
replaying the pre-withdrawal snapshot must not restore those members. A newer
observation can restore eligibility. Clock synchronization between observer
replicas remains required; this protocol is not a consensus or fencing system.

`WatchTopology` starts with the current snapshot. Successful observation refreshes continue even without a topology change: the opaque `revision` can remain the same while `observed_at` and `valid_until` advance. After a stream ends, reconnect with backoff and accept the new initial snapshot. The server also periodically renews connections (discovery connections close after at most five minutes and 30 seconds), so reconnection is normal operation. A deleted or disabled Cluster sends an unavailable tombstone to existing watchers; the subscription can subsequently observe recreation under the same name.

| Field | Meaning |
| --- | --- |
| `cluster.uid` | Kubernetes Cluster identity; changes after deletion/recreation |
| `revision` | Opaque equality token for topology/routing changes; never compare numerically or as a timestamp |
| `observed_at`, `valid_until` | UTC protobuf timestamps controlling freshness |
| `available` | Whether the observation produced a usable routing view |
| `primary_id` | Pod UID of the eligible primary, if established |
| `transitioning` | CNPG reports an ongoing primary transition |
| `connection.database` | Default application database from CNPG bootstrap configuration; empty when none is declared |
| `connection.server_ca_pem` | Public PostgreSQL server CA certificate bundle as PEM bytes (base64 in protobuf JSON) |
| `members[].id` | Pod UID; changes after Pod replacement |
| `members[].ready` | Member eligibility in this snapshot, beyond simple Pod readiness |
| `members[].reason` | Diagnostic explanation; do not build policy on free-form text |
| `members[].endpoints` | `internal` and optional `external` endpoint records |
| `members[].timeline`, `replay_lsn` | Observed PostgreSQL information where available |

The plugin reads the default database from the configured CNPG bootstrap method (`recovery.database`, `pg_basebackup.database`, or `initdb.database`). It does not infer a SQL database name from the Kubernetes Cluster name. Applications using another database can select it with the client library's advanced explicit connection configuration.

The CA comes from `ca.crt` in the Secret referenced by `status.certificates.serverCASecret`. Only public certificate PEM blocks are published, never private keys or database credentials. Secret metadata watches trigger CA refreshes, including when Cluster status is unchanged; there is no periodic CA polling. A missing or unreadable usable CA makes the snapshot unavailable with `connection_defaults_unavailable`. Database/CA changes update the revision. An older plugin may omit `connection`; clients requiring automatic defaults should report that incompatibility or use their explicit advanced connection configuration.

Role and replication state are independent protobuf enums:

| Role | Meaning |
| --- | --- |
| `ROLE_PRIMARY` | Observed primary |
| `ROLE_STANDBY` | Observed standby |
| `ROLE_UNKNOWN` | Role cannot currently be established |

| Sync state | Meaning |
| --- | --- |
| `SYNC_STATE_SYNC` | Currently synchronous priority standby |
| `SYNC_STATE_QUORUM` | Participates in the synchronous quorum |
| `SYNC_STATE_POTENTIAL` | Potential priority synchronous standby |
| `SYNC_STATE_ASYNC` | Currently asynchronous standby |
| `SYNC_STATE_UNKNOWN` | No trustworthy current standby sync classification |

Unknown state is distinct from asynchronous state. Replication settings alone do not establish a replica's current state; the observer correlates the primary's replication status with instance identity. The primary's own sync state is unknown because it is not a standby.

The reported `timeline` is [CNPG's checkpoint timeline](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.0/pkg/management/postgres/probes.go#L501), diagnostic metadata that can lag on a healthy streaming standby after promotion. Clients must not infer replica eligibility by comparing these timeline values.

A client must reject expired data and unavailable snapshots, select only eligible members, and choose a reachable endpoint for the selected member. By default, cnpgconnect-go tries the internal address before the same member's advertised external address, retaining TLS verification and role checks; an explicit client network setting pins that network. Pool updates must account for role/identity changes even when the hostname is unchanged. An active SQL transaction can fail during a role change, and a failed commit can have an ambiguous outcome. Automatic write replay is outside this protocol.

With `ANY 1 (a,b)`, both standbys may report `quorum` while one acknowledgment suffices. Even synchronous replication does not imply that every replica has replayed a given commit. Read-after-write requires primary routing or an appropriate replay-position check in the client.

## Inspect a deployed service

For an endpoint with a publicly trusted certificate, use the release-matched proto from a source checkout:

```sh
grpcurl \
  -import-path proto -proto cnpg/connect/v1/topology.proto \
  -d '{"namespace":"databases","name":"app-db"}' \
  topology.example.com:443 cnpg.connect.v1.TopologyService/GetTopology
```

Replace `GetTopology` with `WatchTopology` for the server stream. Reflection is not enabled. No discovery token or PostgreSQL login is needed unless optional bearer authentication is configured. Private discovery certificates and local port-forwarding are covered in [deployment options](deployment.md#private-in-cluster-endpoint).

Go clients import generated messages and the gRPC client from `github.com/nakiner/cnpg-connect-plugin/api/connect/v1`. This package does not import the observer or Kubernetes packages, although the plugin module declares Kubernetes dependencies. The companion [cnpgconnect-go](https://github.com/nakiner/cnpgconnect-go) library implements discovery reconnection, automatic connection defaults, role selection, and managed pgx/`database/sql`/Bun connections. Its root package is `cnpgconnectgo`.

Connection metadata and tokenless defaults are available from plugin `v0.0.4`; `v0.0.3` predates them. Plugin `v0.0.5` preserves that protobuf API while improving observation scheduling. `cnpgconnect-go v0.0.5` can therefore retain its `cnpg-connect-plugin v0.0.4` generated API dependency when connecting to plugin `0.0.5`.
