# High availability

Everything HA — quorum, leader election, failover, replication,
snapshots — is provided by the clustor substrate modules composed
into the graph, not by any lattice-specific daemon or CLI. A node is
a `fluxor run` over a graph config; the three-node graph in the
[run guide](running.md) is the reference topology.

## The model

- **Quorum consensus**: `2f+1` voters tolerate `f` failures. The
  voter count is the `voter_count` param on the `consensus` and
  `durability` modules.
- **Leader election**: automatic; when the leader dies, a surviving
  voter wins an election within a few seconds and starts accepting
  writes.
- **Writes**: commit on the leader only. A follower answers the
  protocol's authority error (Redis: `-CLUSTERDOWN no authority
  available for this operation`); clients retry against the leader.
- **Reads**: served from each node's local applied state by default
  (committed data that may lag the leader); with `lin_reads: 1`
  every read takes the leader ReadIndex fence and fails closed when
  no authority is available.

## Topologies

**Three nodes** is the recommended minimum: it tolerates one node
failure and keeps quorum with two. The validated localhost bring-up
is the run guide's [three-node section](running.md#three-nodes); a
real deployment renders the same template with each peer's LAN
address in the `peer_router` host params and runs one node per
machine.

**Five nodes** (`voter_count: 5`) tolerates two failures at the cost
of a larger commit quorum and higher write latency.

## Readiness

Each node's `operations` module serves `GET /readyz`: 200 with a
one-byte body when the node's telemetry is fresh and every Raft
instance reports ready, 503 otherwise. Point load-balancer health
checks at it so a node that has lost quorum or wedged its pipeline
is pulled from rotation.

## Rolling upgrades

There is no binary to swap: an upgrade is a rebuild and relaunch,
one node at a time.

1. Rebuild the modules with the new code
   (`fluxor modules build --all`, or `--target bcm2712` for
   bare-metal nodes).
2. Stop one node's `fluxor run`. If it was the leader, the
   surviving voters elect a new one automatically.
3. Relaunch the node and wait for its `/readyz` to answer 200. A
   durable node replays its WAL and catches up from the leader's
   log, or by snapshot transfer if it is too far behind.
4. Move on only once the cluster is back to full strength.

Committed data survives the restart through WAL replay, and so does a
key's TTL: the deadline lives in the record, and the clock it is
measured against is a committed log entry that replay rebuilds. Where
the log has been compacted past those ticks, the clock frontier comes
back with the state it belongs to — in the snapshot header, and in the
disk manifest — so a node that recovers from a compacted log resumes
at the time its deadlines were written against. That clock does not
advance while the cluster is down, so a key comes back with the term it
had left rather than an expired or a renewed one.
Watch and lease state is held in memory and does not survive, so
clients re-establish watches and re-grant leases after their node
restarts.

## Failure behaviour

### Leader failure

Writes and fenced reads fail until a new leader is elected;
election is automatic and committed entries are never lost (they are
quorum-durable before acknowledgement). Clients retry with backoff.

### Follower failure

The cluster keeps serving on quorum. The failed node catches up on
restart from the leader's log, or via snapshot transfer when its log
has been compacted past. Alert on replication lag so a follower that
stays down does not silently erode the failure budget.

### Network partition

The majority side keeps serving; the minority side can serve local
reads but commits nothing. On heal, minority nodes rejoin and catch
up. Deploy voters across failure domains so a single domain loss
cannot take the majority.

## Backup and restore

Independent of replication, `backup_coordinator` produces a
consistent backup by reading every live key, with its create and mod
revision, version and lease, at one revision R. R is held for the whole
export: the coordinator takes a retention hold (`KV_OP_RETAIN`) at the
store's revision before it reads R, renews it between pages (it lives 60 s
on the store's clock) and releases it once the manifest is durable, so no
Compact — a client's or the store's own `history_revisions` compaction —
passes R while it reads; a raw-pair client reading at one revision takes
the same hold. Only history compacted before the hold was taken aborts
a backup (never exported partially). The manifest
carries R, the consensus index the state was applied through, the
epoch the backup is fenced under and R's digest. A restore is EXACT and
goes into a fresh store (its `target` pair): every key lands with the
metadata it had (`KV_OP_RESTORE`), the store's revision passes R, its
compaction floor is R (history below R is not there, so a read or change
feed from under it answers compacted), and the restored digest must be
R's — so a revision a client holds survives a restore or a migration. The
coordinator reads the digest, revision and floor back and logs them
(`[bkp] restore complete r=… h=… rev=R at=… floor=… verified`; `h` counts
records the target already held, as a repeated or resumed restore finds
them). A store already written, or fenced by another restore, refuses it.
The target may be any store a client reaches on a raw KV pair — the
`lattice_store` stack's `pump_kv` included (`configs/store-stack-restore.yaml`,
`tests/harness/tests/live_store_restore.rs`: a fresh disk store, kept
across a restart). Backup artefacts are ordinary KV records, so they
replicate with the cluster. Source: `modules/app/backup_coordinator/mod.rs`.

## Monitoring

Scrape each node's `/metrics` (binary export; `lattice-scrape` in
`tools/load` decodes it). The signals that matter for HA are the
Raft leader/ready gauges, replication lag, WAL flush health, and
the per-adapter error counters.

## See also

- [running.md](running.md) — bring-up, smoke checks, and a
  validated failover
- [deployment.md](deployment.md) — build and configuration
- [Specification](../architecture/specification.md) — read
  semantics and durability rules
