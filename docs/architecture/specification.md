# Lattice specification

Lattice is a multi-protocol data store built as a set of fluxor PIC
modules on the clustor Raft consensus substrate. There is no
standalone binary: a node is a `fluxor run` over a graph config.
Lattice maps client protocols onto one KV state machine and one
durability contract.

Clustor stays authoritative for Raft, durability modes, the WAL and
snapshots, read gates, strict fallback, and ledger ordering. Lattice
does not redefine any of them, provides no separate edge daemon
(listeners are per-config PIC anchors), and inherits the crash-only
fault model.

This document describes the implemented system. Contract points that
are design targets rather than wired behaviour are collected in
[Design targets](#design-targets-not-wired) at the end; everything
else is present-tense fact with its defining source file.

## Invariants

- **KVSOURCE** — every observable KV effect on the replicated path
  derives from committed log entries or installed snapshots. Lattice
  modules write no durable channel of their own; the WAL belongs to
  the substrate's `durability` module.
- **REV-MONOTONE** — the store revision strictly increases per
  partition; every mutation records a unique `mod_revision`.
  Source: `modules/common/kv_store.rs`.
- **LIN-BOUND** — a fenced operation (a read classified
  linearizable) succeeds only when the ReadIndex fence releases it;
  when no authority is available the operation fails closed with a
  protocol-specific error rather than degrading. Source:
  `modules/app/kv_request_router/mod.rs`.
- **TTL determinism** — every deadline is assigned and tested
  against the committed tick, which the state machine reads on the
  same channel as the commands it orders against. No apply or read
  samples a local timer. Source:
  `modules/app/kv_state_worker/mod.rs`.

## Terminology

| Term | Definition |
|---|---|
| anchor | A protocol module mapping an external wire protocol to the KV request envelope. |
| range | A half-open key span `[start, end)` in the ordered routing map, hosted by one partition. |
| KPG | Key-value partition group: the partition hosting one KV state machine instance. |
| revision | Monotone commit counter per partition. A mutation's `mod_revision` is the revision that performed it. |
| routing epoch | Counter fencing the routing map; requests routed under an older map are refused. |
| consistency class | Per-request byte: serializable (default) or linearizable. |
| identity | The `(tenant, database, keyspace)` triple prefixed to every stored key. |

## System model

A node is a `fluxor run` over a graph config: it runs the enabled
protocol anchors, the KV modules, and the clustor substrate modules,
wired by the graph's channel edges. The canonical single-node and
three-node graphs are embedded in the
[run guide](../guides/running.md). Voter count, peers, durability
mode, and listen ports are module params in that graph.

## Data model

Each partition hosts one MVCC KV store
(`modules/common/kv_store.rs`), in memory or on disk
(`kv_state_worker` param `state_store`: 0 memory, 1 ordered disk
runs with recovery before serving).

- **KV record** — key → value plus `create_revision`,
  `mod_revision`, `version` (increments per mutation), and an
  optional expiry deadline.
- **Internal key layout** — every stored key is prefixed with a
  12-byte binary identity `[tenant:u32][database:u32][keyspace:u32]`
  followed by the user key; MVCC versions order below it. Source:
  `modules/common/internal_key.rs`.
- **Lease** — lease id → TTL, grant time, keepalive deadline,
  attached keys, and a per-session epoch that advances on each
  authoritative rebind, so a keepalive carrying a stale epoch is fenced
  rather than refreshing a lease whose session has moved. Source:
  `modules/app/lease_manager/mod.rs`.
- **Watch state** — watch id → key range, filters, and
  `last_sent_revision`, the replay low-water mark. Source:
  `modules/app/watch_registry/mod.rs`.

## Partitioning and routing

The request router (`kv_request_router`) routes by key through one
of two map kinds, both defined in
`modules/common/partition_map.rs`:

- **Ordered range map** — gap-free, non-overlapping half-open key
  ranges covering the whole keyspace; the owning range is resolved
  by key bytes. Range scans are clamped to the owning range and
  continued across ranges through tagged scan cursors.
- **Hash-slot map** — `fnv1a64(key) % 1024` slots with per-slot
  bindings.

Both lookups are generation-checked: a request resolved under a
stale map generation or routing epoch is refused with the
`dirty_epoch` result rather than served against the wrong owner.
Within a partition, writes are WAL-ordered and revisions compare;
across partitions there is no total order, and multi-key operations
(`MSET`, multi-key `DELETE`/`MGET`, whole-store flush, keys-only
scans) are accepted only when one range owns the whole keyspace —
otherwise they are refused with the cross-range result.

Range **split and merge** are online operations driven by
`range_supervisor` (`modules/common/range_lifecycle.rs`): the
operation record is persisted through the KV path, pages are bulk
copied, a span barrier refuses writes into the moving span during
cutover, a catch-up pass drains the delta, and the new map publishes
with a bumped routing epoch. `placement_advisor`
(`modules/common/placement.rs`) recommends splits from observed load
and is advice-only by default: autonomous execution requires both an
explicit opt-in param and a deliberately wired command port, and even
then one command is in flight at a time.

A range's replica set can be **relocated** to a different node.
`range_supervisor` (core logic in
`modules/common/relocation_driver.rs`) drives the substrate's Raft
membership changes — add the destination as a learner, wait for
snapshot catch-up, promote it to voter through joint consensus,
remove the source voter — advancing a phase only when the
corresponding configuration change reports committed, never on
acceptance. The moved binding and leaseholder then publish with a
bumped placement epoch. The move is named either by declaration
(`reloc_*` params, starting at boot) or at runtime: a supervisor
armed with no declaration waits for a relocation command on its
command intake and takes partition, source, and target from it.

An **elastic split** (`elastic_split_driver`) moves one key span onto
another partition without copying the whole store, fencing concurrent
writes as it goes: a bulk span copy streams over the app-snapshot
chunk path while writes keep flowing; on the target's
install-complete acknowledgement the router's span barrier rises
(new writes into the span refuse retryably); after a drain window
sized to cover the graph's route-to-apply latency, a second, quiesced
copy recaptures everything that landed after the first capture; and
on its acknowledgement the cutover map publishes and the barrier
lifts. Within that drain bound an acknowledged write is captured or
retried onto the new owner, and the map never points at a span that
is not yet resident. Both providers export the span (the disk provider
merge-scans it into the same portable body; version history stays
behind, since the destination partition's revision space is
independent). Across nodes, the `span_courier` pair carries the same
chunk stream and returning acknowledgement over one TCP connection,
byte-for-byte; transport loss fails closed (an incomplete stream
aborts the install, no acknowledgement returns, the cutover never
fires).

The **rebalancer** (`modules/app/rebalancer/mod.rs`) asks the
cluster-level questions a single range cannot: it surveys the key
count of every range, reads each range's leaseholder from the map,
and reports per-range split/merge advice plus a relocation
recommendation that evens load across nodes. It follows the same
advice-only, opt-in, one-command-at-a-time contract as the advisor;
its opt-in port emits the recommended relocation as the command an
armed supervisor consumes.

**Dense hosting** lifts the router's per-partition port cost. In
`partition_fanout` mode the router prefixes each partition-bound
frame with a `[partition_id:u16]` tag and writes one multiplexed
output; `partition_demux` strips the tag and fans each frame to its
partition's port, so N partitions cost the router one output pair
instead of N. Partition 0 keeps its direct ports, targeted lifecycle
operations still address the direct ports only, and a graph that
leaves the mode off uses classic per-partition ports with untagged
frames.

## Read and write semantics

Every request carries a consistency byte
(`REQ_SERIALIZABLE`/`REQ_LINEARIZABLE`,
`modules/common/types.rs`); the router chooses one of three paths
(`modules/app/kv_request_router/mod.rs`):

| Path | Taken by | Semantics |
|---|---|---|
| consensus | writes, when the graph is replicated (`replicated: 1` and `proposal_out` wired) | Proposed through the gateway, Raft-committed, applied in log order; the acknowledgement follows quorum durability. |
| fence | reads classified linearizable (`lin_reads: 1`, or a read stamped `REQ_LINEARIZABLE`) | Held until the ReadIndex fence releases at the apply horizon; refused with the LIN-BOUND result on timeout or lost authority. |
| direct | all other reads | Served from local applied state: committed data, no fence, may lag the leader. |

The `replicated` param is checked against the wiring at init: a
graph that declares replication but leaves `proposal_out` unwired
refuses to start rather than silently serving writes locally. The
same fail-closed check applies to `lin_reads` without the fence
edges. On a graph hosting several local partitions the fence is
partition-aware end to end in fanout mode: the router tags each
ReadIndex probe with the read's partition, a demux routes it to that
partition's consensus instance, the grant streams fan back in
(matched by correlation id), and the applied-index wait and the
released read's forward both target the read's own partition. With
direct multi-partition ports the probe would reach a single consensus
instance, so that combination is refused at init.

Two consequences worth stating plainly:

- Reads are serializable by default on every surface. The shipped
  anchors stamp reads `REQ_SERIALIZABLE`; the fence engages only
  when the router is configured with `lin_reads: 1` (all reads take
  the fence) — there is no per-request linearizable read flag on the
  Redis, Memcached, or etcd wire today.
- On a follower, writes are refused (`NOT_LEADER` through the
  gateway reject edge, surfaced as the protocol's authority error)
  and direct reads are served from local applied state.

## Snapshots and compaction

Log snapshots and their transfer are the substrate's. The state
worker implements the app side: on request it encodes its state and
streams it in chunks; on install it accumulates chunks and replaces
its state at the labelled applied position. Disk state stores answer
with a small disk-resident marker (the store's on-disk runs are the
snapshot); memory-store bodies are size-bound by the worker's
transfer buffer, and an over-budget encode is refused, leaving the
WAL authoritative (the ceilings are in the
[limit register](limit_register.md)). Source:
`modules/app/kv_state_worker/mod.rs`.

MVCC garbage collection is claim-based
(`modules/common/compaction_floor.rs`): active readers, watches,
backups, and operators register retention claims; the candidate
floor is the minimum over live claims, clamped to the committed
high-water; a missing or stale claim blocks advancement (fail
closed). The committed floor moves only when the floor record
commits through the KV path. Historical reads below the floor return
the compacted result.

`backup_coordinator` (`modules/app/backup_coordinator/mod.rs`)
produces a consistent backup by scanning every record at one
protected revision; a compacted answer aborts the backup rather than
exporting a partial. Restore verifies manifest completeness and
replays pages as puts (a merge; it does not delete keys absent from
the backup).

## TTL, leases, and watches

Expiry runs on one clock, and that clock is a committed log entry.
`ttl_scheduler` proposes a tick on a cadence and the tick becomes the
cluster's time only once it returns committed, on the same channel as
the commands it is ordered against. A deadline is therefore a function
of the log position that set it: replicas applying the same log agree,
and a replay reaches the same answer again.

The proposal carries an elapsed interval added to the current
committed time rather than a reading of the proposer's own clock. Two
consequences follow. A node whose clock is wrong cannot move expiry,
and a cluster that is down does not age its TTLs — when it returns,
expiry resumes from where the log left off, so keys outlive an outage
rather than expiring unobserved during one.

A record's own deadline decides whether it is visible. The scheduler's
queue schedules the release of the slot behind it, which allocates no
revision and emits no event, so replicas may reclaim at different
moments without their answers differing.

Every graph that runs a state worker wires the clock, and the port is
declared required: a graph that omits it fails `fluxor build` rather
than running with expiry silently stopped. A required port only proves
an edge exists, so the shape is checked separately (`tools/ci/
clock_lint`, a CI gate over every config): the tick a graph applies
must be one this scheduler proposed and got back through the same
ordering its commands went through — directly on the command channel
where there is no consensus, out of the commit stream where there is.
A graph that satisfies the port by wiring some other producer to it is
refused with the reason.

Lease keepalives move the keepalive deadline against the same clock; a
keepalive carrying a stale session epoch — one left behind when the
lease was rebound to another worker — is fenced rather than refreshing
the lease. Watch delivery
(`watch_registry` + `watch_fanout`) replays a revision window
through versioned scans: every version in `(from, to]` is delivered
in order, tombstones included, and a compacted answer abandons the
backfill rather than delivering a partial tail. Watch and lease
state is held in memory: it does not survive a node restart (see
[Design targets](#design-targets-not-wired)). A record's TTL does
survive one: the deadline lives in the record, and the clock it is
measured against is restored with it. A full replay re-establishes the
clock from the ticks in the log; where the log has been compacted the
frontier travels in the snapshot header — and, for the disk provider,
in the manifest — so a store recovered from a compacted log resumes at
the time its deadlines were written against rather than at zero. On
restore the worker hands the scheduler that frontier and re-registers
the restored deadlines, since the scheduler's clock and queue live
only in its arena.

## Error mapping

Result codes are defined in `modules/common/types.rs`; each anchor
renders them on its own wire. The conditions clients most often see:

| Condition | Redis | Memcached | etcd v3 |
|---|---|---|---|
| Routing epoch advanced / stale map | `-MOVED routing epoch advanced` | `NOT_STORED` | status 13 |
| No authority for a fenced operation | `-CLUSTERDOWN no authority available for this operation` | `SERVER_ERROR backpressure` | status 13 |
| Wrong value type | `-WRONGTYPE Operation against a key holding the wrong kind of value` | `CLIENT_ERROR cannot increment or decrement non-numeric value` | — |
| Read below the compaction floor | — | — | status 11 (OUT_OF_RANGE, matching etcd's compacted error) |
| Quota refused | `-BUSY tenant quota exceeded` | — | — |
| Cross-range multi-key operation | `-ERR internal error (result 16)` | — | — |

The Redis `-MOVED`/`-CLUSTERDOWN` strings reuse the closest standard
client-retry signals so an unmodified client re-resolves routing on
an epoch flip; they are not Redis Cluster redirection and carry no
slot or address. The Memcached ASCII surface has no dedicated
routing or linearizability strings. The etcd surface emits
`grpc-status` only, never a `grpc-message` text, and currently maps
both the routing-epoch and no-authority results to status 13
(INTERNAL); a key miss is status 0 with absence expressed in the
response body. The generic `-ERR internal error` rendering of the
cross-range refusal on the Redis surface is a known gap.

## Multi-tenancy

The key layout, the request envelope, and the quota and auth tables
all carry tenant identity, but every shipped anchor stamps tenant 0:
a node today is single-tenant, and whole-store operations
(`FLUSHALL`, memcached `flush_all`) affect the one shared keyspace
across all anchors in the graph. `AUTH` authenticates a connection
(constant-time `requirepass` comparison on the Redis surface); it
does not select a tenant.

## Observability

Every module emits positional counters on its `metrics` port; the
declaration order in each module's manifest is the id contract
(`modules/common/telemetry.rs`). A module may additionally declare
richer instruments — histograms with fixed bucket bounds and
enumerated dimensions — in its manifest's `[observability]` table
(`relational_executor`'s `query_latency_us` is one). The declaration
is the contract: records carry counts only and are resolved through
the graph's exported id-table (`fluxor id-table`), riding the kernel
telemetry ring rather than the metrics port. Lattice app modules fan in through
`adapter_metrics`; substrate modules feed `operations` directly,
which serves the binary `/metrics` export and `/readyz` (one byte;
200 when telemetry is fresh and every Raft instance reports ready).
`tools/load` ships `lattice-scrape` to decode the export.
`telemetry/catalog.json` names the intended stable metric surface;
it is a catalogue, not yet a wired exporter format.

## Design targets (not wired)

**Status: design target, not wired.** The following are contract
points the implementation has not reached. They are listed here so
the rest of this document can stay strictly factual; none of them is
observable behaviour today.

- **Tenant identity on connections** — per-connection tenant
  resolution (from credentials or transport identity), tenant
  manifests, per-tenant partition counts, and tenant-scoped
  whole-store operations.
- **Control-plane policy** — a real control-plane feed driving
  routing plans, cache freshness (fresh/cached/stale/expired) with
  strict fallback on staleness, command allowlists, feature gates,
  and PKI. The `control_plane` substrate module currently emits a
  synthetic proof on a fixed schedule, and the router's control-plane
  input ports are not yet consumed.
- **Quota enforcement on live traffic** — the per-tenant token
  bucket (`modules/common/quota_bucket.rs`) and its
  throttle/disconnect envelopes exist, but no producer drives the
  refill tick in a shipped graph.
- **Durable watch/lease state** — persisting `last_sent_revision`
  and the lease table so watches and leases survive restart.
- **Per-request read consistency on the client wire** — honouring
  etcd's `serializable` flag and a Redis-side linearizable read
  opt-in, mapping them to the fence path per request.
- **Idempotent retry ledger** — a keyed table of acknowledged
  outcomes so adapter retries are deduplicated; today only an
  idempotency-key field exists on the data-surface envelope.
- **Distinct error strings** — dedicated routing-epoch and
  linearizability errors on the Memcached surface, and
  FAILED_PRECONDITION/UNAVAILABLE statuses with messages on the etcd
  surface in place of the current status-13 mapping.
- **TLS in front of the etcd anchor** — the gRPC listener is
  plaintext HTTP/2 in every shipped graph; TLS termination is
  composed by wiring the fluxor `tls` module in front of an anchor,
  as the TLS-fronted Redis graph does.
- **Cross-partition transactions** — the two-phase protocol exists
  as a config-driven demonstrator (`txn_coordinator`); it is not a
  client-facing transaction API.
