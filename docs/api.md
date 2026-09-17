# Application discovery contract

The service is `cnpg.connect.v1.TopologyService`, defined in [topology.proto](../proto/cnpg/connect/v1/topology.proto). Both RPC requests identify a Cluster by `namespace` and `name`.

```protobuf
rpc GetTopology(GetTopologyRequest) returns (Snapshot);
rpc WatchTopology(WatchTopologyRequest) returns (stream Snapshot);
```

The application listener uses TLS with bearer authentication in gRPC metadata:

```text
authorization: Bearer <token>
```

Clusters must opt in through an enabled native plugin entry or the observation annotation. A native entry takes precedence over annotations, including explicit native disablement. Unknown or disabled Clusters return `NotFound`. Invalid resource names return `InvalidArgument`. Missing/invalid credentials return `Unauthenticated`. An observed Cluster with no safe routing view returns a snapshot marked unavailable; consumers must interpret the snapshot instead of treating a successful RPC as proof that a database is usable.

## Snapshot semantics

Each message fully replaces the previous snapshot for that Cluster. There is no event replay cursor, delta format, or guarantee that every intermediate transition is delivered. Pending updates may be coalesced for slow clients.

`WatchTopology` starts with the current snapshot. Successful observation refreshes continue even without a topology change: the opaque `revision` can remain the same while `observed_at` and `valid_until` advance. After a stream ends, reconnect with backoff and accept the new initial snapshot. The server also periodically renews connections (five-minute maximum age with a 30-second grace period), so reconnection is normal operation. A deleted or disabled Cluster sends an unavailable tombstone to existing watchers; the subscription can subsequently observe recreation under the same name.

| Field | Meaning |
| --- | --- |
| `cluster.uid` | Kubernetes Cluster identity; changes after deletion/recreation |
| `revision` | Opaque equality token for topology/routing changes; never compare numerically or as a timestamp |
| `observed_at`, `valid_until` | UTC protobuf timestamps controlling freshness |
| `available` | Whether the observation produced a usable routing view |
| `primary_id` | Pod UID of the eligible primary, if established |
| `transitioning` | CNPG reports an ongoing primary transition |
| `members[].id` | Pod UID; changes after Pod replacement |
| `members[].ready` | Member eligibility in this snapshot, beyond simple Pod readiness |
| `members[].reason` | Diagnostic explanation; do not build policy on free-form text |
| `members[].endpoints` | `internal` and optional `external` endpoint records |
| `members[].timeline`, `replay_lsn` | Observed PostgreSQL information where available |

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

A client must reject expired data and unavailable snapshots, select only eligible members, and choose an endpoint from its configured network. Pool updates must account for role/identity changes even when the hostname is unchanged. An active SQL transaction can fail during a role change, and a failed commit can have an ambiguous outcome. Automatic write replay is outside this protocol.

With `ANY 1 (a,b)`, both standbys may report `quorum` while one acknowledgment suffices. Even synchronous replication does not imply that every replica has replayed a given commit. Read-after-write requires primary routing or an appropriate replay-position check in the client.

## Inspect a deployed service

Use an application discovery CA file distributed by your deployment. The following assumes the example release name `connect`, `fullnameOverride=cnpg-connect`, and a locally held `work/discovery-token` matching the Secret. It uses the checked-in proto instead of gRPC reflection.

```sh
kubectl -n cnpg-system port-forward service/cnpg-connect-api 8443:443
```

In another terminal:

```sh
grpcurl \
  -cacert /path/to/discovery-ca.crt \
  -authority cnpg-connect-api.cnpg-system.svc \
  -H "authorization: Bearer $(cat work/discovery-token)" \
  -import-path proto -proto cnpg/connect/v1/topology.proto \
  -d '{"namespace":"databases","name":"app-db"}' \
  127.0.0.1:8443 cnpg.connect.v1.TopologyService/GetTopology
```

Replace the method with `WatchTopology` to receive the server stream. For an external LB, use its address instead of `127.0.0.1:8443` and use the matching certificate DNS name as `-authority`.

Clients can import `api/connect/v1` without importing the observer or Kubernetes packages. The companion `github.com/nakiner/cnpgconnect` library, available in a separate source checkout, implements discovery reconnection, role-selection policies, and managed connections for pgx, `database/sql`, and Bun. See its usage documentation for pool invalidation and in-flight operation behavior.
