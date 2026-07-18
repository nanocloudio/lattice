# Lattice High Availability Guide

This document covers high availability: topologies, readiness gates, failure
scenarios, and rolling upgrades. Everything HA — quorum, leader election,
failover, snapshots, DR — is provided by the **Clustor substrate modules**
(`consensus`, `durability`, `admission`, `control_plane`, `gateway`,
`peer_router`, `operations`) composed into the graph config, not by any
Lattice-specific daemon or CLI. A node is a `fluxor run` over a graph config.

## Architecture Overview

Lattice inherits its HA model from Clustor Raft:

- **Quorum-based consensus**: `2f+1` voters tolerate `f` failures. Voter count
  is the `voter_count` param on the `consensus`/`durability` modules
  (`configs/multi-3node.yaml` uses `voter_count: 3`).
- **Leader election**: automatic failover on leader failure.
- **Linearizable reads**: require a leader ReadIndex fence (the router's
  `lin_reads` classification; the ReadIndex fast-path needs a real multi-voter
  quorum).
- **Snapshot-only reads**: served without a ReadIndex fence when explicitly
  permitted per adapter (see the specification's
  [read semantics](specification.md#read-semantics-and-lin-bound)).

## Deployment Topologies

### Three-Node Cluster (Recommended Minimum)

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Node 0     │    │  Node 1     │    │  Node 2     │
│  (Leader)   │◄──►│  (Follower) │◄──►│  (Follower) │
└─────────────┘    └─────────────┘    └─────────────┘
```

Each node runs `fluxor run` over a rendered `configs/multi-3node.yaml` with its
own `self_id` and peer list (see [deployment.md](deployment.md#multi-node)).

- Tolerates 1 node failure; maintains quorum with 2 nodes.
- `voter_count: 3` on `consensus` and `durability`.

### Five-Node Cluster (Production DR profile)

- Tolerates 2 node failures (`voter_count: 5`).
- Higher write latency due to the larger quorum. The specification recommends ≥3
  KPG voters per tenant, and 5 for DR profiles.

## Readiness Gates

The `operations` module exposes `/readyz` on its `listen_port`. It reports the
tenant routing-epoch cache age, adapter enablement states, and a digest of the
active tenant manifest used for routing (see the specification's
[observability](specification.md#observability) section). Point your load
balancer health check at this endpoint:

```
curl http://<host>:<http_port>/readyz
```

Route load-balancer health checks to `/readyz` so a node that has lost CP
freshness or leadership is pulled from rotation.

## Rolling Upgrades

There is no binary to swap and no `lattice admin` CLI. A rolling upgrade is a
rebuild-and-relaunch, one node at a time, following the Clustor upgrade guidance
in the specification's
[disaster recovery and upgrades](specification.md#disaster-recovery-and-upgrades)
section — drain listeners, transfer KPG leadership, upgrade, rejoin:

1. Rebuild the modules with the new code:
   ```bash
   fluxor modules build --target bcm2712
   ```
2. Upgrade one node at a time. Stop that node's `fluxor run`, transfer
   leadership to a peer (the substrate elects a new leader automatically when a
   voter leaves), relaunch `fluxor run` on the upgraded node, and wait for its
   `/readyz` to report ready before moving on.
3. Proceed to the next node only once the cluster is back to full quorum.

Because durability is inherited from Clustor, committed data survives a node
restart via WAL replay (see the replay handoff note in
`configs/single-replicated-lattice.yaml`).

There is no `lattice snapshot export/import`, `lattice admin
drain/shutdown/wait-ready/cluster-status`, or binary-swap flow. Snapshot
export/import and DR are Clustor substrate capabilities (see Disaster Recovery
below).

## Failure Scenarios

### Leader Failure

**Symptoms:** writes and linearizable reads fail until a new leader is elected.

**Recovery:** automatic leader election; clients retry to the new leader. No
data loss — committed entries are quorum-durable and replicated.

**Mitigation:** client retries with exponential backoff; deploy across failure
domains.

### Follower Failure

**Symptoms:** that follower falls behind; the cluster keeps serving on quorum.

**Recovery:** the node catches up from the leader's log on restart, or via
snapshot transfer if it is too far behind.

**Mitigation:** deploy across failure domains and alert on extended outages.
(Use the replication metric documented in `telemetry/catalog.json`;
see [performance.md](performance.md).)

### Network Partition

**Symptoms:** the minority partition cannot serve writes/linearizable reads; the
majority partition operates normally.

**Recovery:** on heal, minority nodes sync and rejoin.

**Mitigation:** deploy across 3+ failure domains; tune election timeouts.

### Control-Plane (CP-Raft) Unavailability

**Symptoms:** the CP cache goes stale/expired; after the grace period,
LIN-BOUND fails closed and linearizable operations are refused (Redis
`-CLUSTERDOWN`, etc.).

**Recovery:** restore CP-Raft connectivity; the cache refreshes automatically.

**Mitigation:** deploy CP-Raft with high availability. This strict-fallback
behavior is inherited from Clustor (see the specification's
[control plane](specification.md#control-plane) section).

## Disaster Recovery

DR is performed via Clustor snapshot export/import, not a Lattice CLI:

- Under unfenced DR promotion, Lattice treats linearizable guarantees as
  unavailable until fenced promotion completes and Clustor read gates pass.
- Snapshot emission and import are inherited from Clustor.

See the specification's
[disaster recovery and upgrades](specification.md#disaster-recovery-and-upgrades)
section for the normative DR and upgrade rules.

## Monitoring

Alert on the metrics defined in `telemetry/catalog.json` (dotted `lattice.*`
namespaces plus inherited `clustor.*`) and described in the specification's
[observability](specification.md#observability) section. Do not rely on metric
or alert names not present in the catalog. Useful signals
include the LIN-BOUND availability gauge, CP cache state/age, replication lag,
and per-adapter request/error counters — see the catalog for exact names.

## See Also

- [Deployment Guide](deployment.md) — build + `fluxor run` bring-up
- [Performance](performance.md) — latency/throughput targets and measurement
- [Specification](specification.md) — observability, DR/upgrades, LIN-BOUND
  semantics
