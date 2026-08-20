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

Committed data survives the restart through WAL replay. Watch,
lease, and TTL state does not: it is held in memory, so clients
re-establish watches and re-grant leases after their node restarts.

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
consistent backup by scanning every record at one protected
revision — a scan that races compaction is aborted rather than
exported partially — and restore replays the backup's pages as puts
(a merge: keys absent from the backup are not deleted). Backup
artefacts are ordinary KV records, so they replicate with the
cluster. Source: `modules/app/backup_coordinator/mod.rs`.

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
