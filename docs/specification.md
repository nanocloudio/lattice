# Lattice specification

Lattice is a multi-protocol distributed data store built as a set of fluxor PIC
modules on the Clustor Raft consensus core. There is no standalone binary: a
node is a `fluxor run` over a graph config. Lattice maps client protocols onto
one KV state machine and durability contract, inheriting Clustor's ordering,
durability ledgers, strict fallback, read gates, snapshots, and security
guardrails (Clustor spec §§3–14).

This document is normative for the etcd, Redis, and Memcached KV adapters. The
data-model surfaces — PostgreSQL/MySQL SQL, MongoDB documents, Cassandra CQL,
and the RESP graph/search/vector/time-series models — reuse the same state
machine, routing, and read semantics defined here; their per-protocol contracts
live in the guides alongside this file.

Clustor stays authoritative for Raft, durability modes, WAL and snapshots, read
gates, strict fallback, ledger ordering, security primitives, and cache-state
semantics. Lattice does not redefine any of them, provides no separate edge
daemon (listeners are per-config PIC anchors), and inherits Clustor's crash-only
fault model.

## Invariants

- **KVSOURCE** — every observable effect derives from WAL entries or signed
  snapshots. No adapter introduces an alternate durable channel.
- **REV-MONOTONE** — `revision` strictly increases per KPG; every mutation has a
  unique `mod_revision`.
- **LIN-BOUND** — Lattice claims linearizable semantics only when the Clustor
  leader can serve ReadIndex and the response is fenced by the same proof and
  index. It gates linearizable reads, compare/CAS evaluation, and transaction
  fences — not non-conditional writes, which follow Clustor durability rules
  independently. When it cannot be satisfied, linearizable operations fail
  closed; snapshot-only reads are offered only where explicitly allowed.
- **TTL-DETERMINISM** — expiry is applied only from apply-loop ticks or a
  mutation touching the key (see [Deterministic time](#deterministic-time-and-ttl)).
  Wall-clock sampling never deletes a key directly.

**L1 latency target** — ≤10 ms p99 in-AZ server-side, from edge ingress to WAL
quorum durability, for single-key writes under healthy conditions. This is a
planning target and excludes client RTT; the binding requirements are the
durability and gating rules below. Snapshot-only reads do not wait for WAL and
are not subject to it.

## Terminology

| Term | Definition |
|---|---|
| KPG | Key-Value Partition Group: a tenant-scoped Clustor partition hosting one KV state machine. |
| CP-Raft | Clustor ControlPlaneRaft, holding tenant manifests, routing epochs, adapter policies, quotas, and PKI. |
| Adapter | A protocol anchor mapping an external protocol to the Lattice KV primitives. |
| revision | Monotone per-KPG commit index. A mutation's `mod_revision` is the Raft log index that performed it; a read fence returns a `header.revision` equal to the commit index used to fence the read, which may exceed any key's `mod_revision`. |
| kv_epoch | Per-tenant routing epoch from CP-Raft; fences sharding and policy. |
| snapshot-only read | A committed read taken without a ReadIndex fence; labeled per adapter. In etcd, `serializable=true` maps here. |
| kv_ttl | Key expiry, applied deterministically in the apply loop. |

Default timers, operator-configurable within CP-Raft bounds:
`lease_ttl_default_ms = 60_000` (max `600_000`), `watch_idle_timeout_ms =
300_000`, `expiry_scan_period_ms = 1_000`, `auth_cache_ttl_ms = 30_000`.

## System model

A **node** is a `fluxor run` over a graph config: it runs the enabled protocol
anchors, hosts multiple KPGs, and a control agent that maintains CP-Raft caches
and publishes readiness. Each KPG runs an apply loop, snapshots, compaction
integration, flow control, and an adapter-facing request router.

Environment: Linux ≥5.15 with io_uring and barrier-enabled NVMe; a deployment
that violates Clustor storage guardrails fails bootstrap. Excessive clock skew
fences leadership and disables time-based behavior until recovered. Each
production tenant provisions ≥3 KPG voters (5 for DR profiles). CP-Raft outages
shorter than `controlplane.cache_grace_ms` keep cached metadata; stale or
expired caches force strict fallback and LIN-BOUND failure.

## Data model

Each KPG hosts the same entities:

- **KV record** — `Key → {value_bytes, create_revision, mod_revision, version,
  lease_id?, expiry_at_ms?, metadata_flags}`. `version` increments on each
  mutation; `mod_revision` is the mutating entry's Raft log index.
- **Lease** — `LeaseID → {ttl_ms, granted_at_ms, keepalive_deadline_ms,
  attached_keys[], epoch}`; `epoch` increments on revoke/recreate fencing.
- **Watch state** — `WatchID → {key_range, start_revision, filters,
  progress_notify, last_sent_revision}`. Adapters with etcd watch semantics
  persist `last_sent_revision` so replay cannot duplicate or drop events; Redis
  and Memcached have no watch state.

## Partitioning and routing

Keys are tenant-scoped byte strings, routed by a versioned hash:

```
kpg = hash64(tenant_id, key_bytes, hash_seed_version) % tenant_kpg_count
```

Single-KPG range reads are served directly; multi-KPG ranges need a CP-Raft
fanout plan and are exposed only where adapter semantics tolerate a partial
fence. Within a KPG, operations are WAL-ordered and revisions compare; across
KPGs there is no total order, and no adapter claims global ordering outside a
single KPG.

Changing the hash seed or `tenant_kpg_count` is a CP-Raft routing-epoch
migration: CP-Raft plans, learners catch up, the epoch flips, clients
reconnect. A request carrying an older `kv_epoch` is rejected as `dirty_epoch`.

## Read semantics and LIN-BOUND

Lattice maps Clustor's read gate into adapter behavior. `can_linearize` is true
only on a leader whose read gate passes; `can_snapshot_read` is always allowed
on a leader and conditional on Clustor granting it to a follower.

| Operation class | Healthy (ReadIndex eligible) | Strict fallback / ineligible |
|---|---|---|
| Linearizable reads, compare/CAS, txn fences | Allowed | Fail closed (adapter-specific error) |
| Snapshot-only reads (where permitted) | Allowed | Allowed, labeled per adapter |
| Non-conditional writes | Allowed (configured durability) | Allowed; durability may be forced strict |
| Watch at "now" | Allowed | Fail closed, or snapshot-only with an explicit flag |

Only KPG leaders serve linearizable reads and commit-fenced transactions.
Followers serve snapshot-only reads only under `can_snapshot_read`; otherwise
they redirect or fail.

Mutation ACKs follow Clustor's durability contract and never precede quorum
durability. Adapter retries are made idempotent through an `IdemKey →
{ack_revision, outcome_digest}` table whose outcomes are deterministically
recomputable from replayed WAL — the same WAL and apply-loop state always yield
the same outcome; any eviction that is not WAL-backed requires that determinism.

## Snapshots and compaction

Snapshot emission and import are Clustor's. KV compaction is a logical policy
over Clustor's WAL compaction floor: Lattice garbage-collects historical MVCC
revisions only without violating that floor and without breaking adapter
correctness (etcd watch and historical range reads). It persists a
`kv_compaction_floor_revision` that never exceeds Clustor's effective floor.

## Deterministic time and TTL

A KPG has exactly one WAL-committed tick source: `Tick(ms)` entries written
periodically (default 1 s) or Clustor snapshot-manifest time fences. No non-WAL
time source exists. Expiry is evaluated only on a committed tick or a mutation
touching the key; lease keepalives are WAL-backed mutations under the durability
ACK rules.

## Protocol adapters

### etcd v3

gRPC endpoint with mTLS; identity from CP-Raft principal manifests. Requests
carry `kv_epoch` via SNI/tenant routing or metadata; mismatches are
`dirty_epoch`. The `mod_revision` and response `header.revision` follow the
[revision](#terminology) definition.

- **Range** — `serializable=false` requires `can_linearize` and reads at a
  ReadIndex-fenced commit index. `serializable=true` may be snapshot-only and
  may lag; per-session monotonicity is a best-effort routing optimization, not
  a guarantee. Linearizable multi-KPG ranges are rejected without a CP-Raft
  fanout plan; serializable multi-KPG ranges may merge per-KPG fences and set
  `partial_revision_fence=true`.
- **Put / Delete** — quorum-durable before success; `prev_kv` is computed in the
  apply loop.
- **Watch** — a `start_revision=0` watch requires `can_linearize`, or starts
  snapshot-only with a `watch_semantics=SNAPSHOT_ONLY` trailer. Delivery is
  per-key by `mod_revision` and per-watch by WAL order; progress notifications
  are deterministic against WAL and ticks.
- **Lease** — grant/revoke/keepalive are WAL-backed; attached keys expire only
  through tick processing, and keepalive responses reflect committed state.

**Not implemented:** `Txn` and `Auth`. The anchor dispatches
Range/Put/DeleteRange/Watch/Lease only. When `Txn` lands, its intended shape is
apply-loop evaluation at one revision fence over `mod_revision` / `create_revision`
/ `version` / `value` / `lease` compares, with cross-KPG transactions rejected as
`TxnCrossShardUnsupported`. Principal/RBAC enforcement today is `auth_manager` on
the KV path, not an etcd `Auth` RPC.

### Redis

RESP2 over TCP; RESP3 behind a CP-Raft feature gate. TLS supported, mTLS in
production. Pipelining preserves per-connection response order. Data types:
**Strings** and counters, plus `TS.*` time-series.

Implemented commands:

- Strings: `GET`, `SET` (`NX/XX`, `EX/PX`, `GET`), `SETNX`, `GETSET`, `APPEND`,
  `STRLEN`, `MGET`, `MSET`.
- Counters: `INCR`, `INCRBY`, `DECR`, `DECRBY`.
- Keyspace: `DEL`/`UNLINK`, `EXISTS`, `KEYS`, `SCAN`, `FLUSHDB`, `FLUSHALL`.
- Time-series: `TS.ADD`, `TS.RANGE`.
- Session: `MULTI`, `EXEC`, `DISCARD`, `SUBSCRIBE`, `PING`, `AUTH` (single-password
  `requirepass`).

`GET` after an acknowledged `SET` on the same connection, conditional `SET
NX/XX`, and `INCR/DECR` require `can_linearize` (their read/compare fence is what
LIN-BOUND gates) and fail closed otherwise. The causal guarantee depends on
stable routing to the same KPG leader; a leader change breaks it until a fresh
write-then-read re-establishes it. `MGET`/`MSET` are single-KPG. `MULTI/EXEC`
runs only when every queued command targets one KPG. `SCAN` is snapshot-only and
single-KPG.

**Not implemented** (return `-ERR unknown command`, never a silent degrade):
hashes, lists, sets; TTL commands (`EXPIRE`/`TTL`/…); `WATCH`; `EVAL`/`EVALSHA`;
`INFO`; `PUBLISH` (`SUBSCRIBE` holds session state but nothing is published).

### Memcached

ASCII protocol over TCP, optional UDP (off by default). Implemented: `get`,
`gets`, `set`, `add`, `replace`, `append`, `prepend`, `delete`, `incr`, `decr`,
`flush_all`, `version`, `stats`, `quit`.

`gets` returns a CAS token equal to the key's current `mod_revision` (a lossless
unsigned-64 value, monotone per key). Expiry maps to `kv_ttl` and deterministic
ticks; absolute times become deadlines applied only on a tick, and values over
the tenant TTL bound are clamped or rejected consistently per deployment.

**Not implemented:** `cas`, `touch`, `gat`/`gats`, the binary protocol, and
SASL. The `cas` write that would consume the `gets` token returns `ERROR`; its
intended shape is a revision-equality compare against `mod_revision` at a
linearizable fence.

## Error mapping

Common conditions: `dirty_epoch` (stale epoch or wrong KPG),
`LinearizabilityUnavailable` (LIN-BOUND failure), `ThrottleEnvelope` (flow
control or quota backpressure). `LinearizabilityUnavailable` and
`TxnCrossShardUnsupported` are stable API strings; clients may match them
exactly.

| Condition | etcd v3 | Redis | Memcached (today) |
|---|---|---|---|
| Routing epoch / wrong KPG | `FAILED_PRECONDITION` `{expected_epoch, observed_epoch}` | `-MOVED routing epoch advanced` | `NOT_STORED` |
| Linearizability unavailable | `UNAVAILABLE` `{reason}` | `-CLUSTERDOWN no authority available for this operation` | `SERVER_ERROR backpressure` |
| Wrong value type | — | `-WRONGTYPE …` | — |

The Redis `-MOVED`/`-CLUSTERDOWN` are not Redis Cluster redirection — they reuse
the closest standard client-retry signals so an unmodified client re-resolves
routing on an epoch flip. The Memcached adapter emits no dedicated
routing/linearizability string; the only `SERVER_ERROR` strings in code are
`backpressure`, `command too large`, `flush failed`, `not yet supported`, and
`overflow`.

## Control plane

CP-Raft is authoritative for tenant manifests (KPG count, routing epoch, hash
seed, adapter enablement, command allowlists, TTL bounds), per-tenant quota
budgets, PKI/RBAC, and feature gates (RESP3, scan, multi-KPG fanout). Cache
state follows Clustor's Fresh/Cached/Stale/Expired matrix; any move to
Stale/Expired forces strict fallback, fails LIN-BOUND for linearizable
operations, and revokes follower-read and watch-start linearization.

## Multi-tenancy and quotas

Tenants are isolated by keyspace and identity, with no cross-tenant access
unless CP-Raft defines an explicit bridge. Per-tenant token buckets bound
requests and bytes; sustained overage beyond `quota.overage_grace_ms`
disconnects Redis/Memcached clients and fails etcd requests. Clustor flow-control
throttle reasons are surfaced per adapter.

## Observability

Metrics live under `lattice.kv.*`, `lattice.adapter.<name>.*`, `lattice.quota.*`,
plus inherited `clustor.*`; the catalog is `telemetry/catalog.json`. Fields
include `lin_bound.can_linearize`, `lin_bound.failed_clause`, `read_gate.*`,
`kv.revision`, `kv.compaction_floor_revision`, `ttl.expiry_queue_depth`,
`watch.active_streams`, `lease.active_leases`, `adapter.<name>.requests_total`,
`adapter.<name>.errors_total`, `quota.throttle_total`, `dirty_epoch_total`.
`/readyz` reports the routing-epoch cache age, adapter enablement, and a digest
of the active manifest.

## Disaster recovery and upgrades

DR uses Clustor snapshot export/import; under unfenced DR, linearizable
guarantees stay unavailable until fenced promotion completes and read gates
pass. Rolling upgrades drain listeners, transfer KPG leadership, rebuild
modules, and rejoin. Any feature needing new semantics is gated in CP-Raft and
surfaced through readiness and telemetry.
