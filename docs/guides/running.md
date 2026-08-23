# Running Lattice

This guide brings up a live node on one Linux machine and smoke
checks it with a Redis client. It is self-contained: the deployment
configs are embedded below and pipe straight into `fluxor run`, so
there is nothing else to fetch. Ports and node identity come from the
command line; the config carries only the graph.

## Prerequisites

Lattice runs on the [fluxor](../../../fluxor/) runtime and composes
the clustor substrate modules. One-time setup on a development
machine:

```sh
git clone git@github.com:nanocloudio/fluxor.git ../fluxor
make -C ../fluxor install    # put the fluxor CLI on PATH
make -C ../fluxor publish    # publish SDK, runtime, foundation modules

git clone git@github.com:nanocloudio/clustor.git
make -C clustor publish      # publish the substrate module palette
                             # (run inside your clustor clone)

# in this repository
fluxor modules build --all   # build lattice's modules; pre-flight
                             # materialises pinned artefacts from fluxor.lock
```

## How replica mode works

`fluxor run --replicas N` treats its config as a template, renders it
once per replica, and spawns N processes side by side, each in its
own working directory. `-` as the config argument reads the template
from stdin, so the whole deployment is one shell command. The
template placeholders are filled per replica ordinal `i`:

| Placeholder | Value |
|---|---|
| `__SELF_ID__` | replica index `i` (0-based) |
| `__LISTEN_PORT__` | `--base-port + i` (default base 9090) |
| `__PEER<j>_PORT__` | `--base-port + j` |
| `__HTTP_PORT__` | `__LISTEN_PORT__ + --http-offset` (default 10000) |

Any other placeholder is supplied with `--var KEY=VALUE`, applied
uniformly to every replica — the single-node config below uses
`__REDIS_PORT__` this way.

## Single node

One node is its own quorum (`voter_count: 1`): a Redis `SET` is
appended to the WAL, fsynced, committed, and applied before the `+OK`
comes back. Raft wire port 9090, diagnostic HTTP port 19090, Redis
on the port you pass.

```sh
fluxor run --replicas 1 --var REDIS_PORT=6390 - <<'EOF'
target: linux
tick_us: 1000

scheduler:
  accept_cycles: true

platform:
  net: {}

modules:
  # ── Lattice edge + KV ─────────────────────────────────────
  - name: redis_edge_anchor
    params:
      listen_port: __REDIS_PORT__
  - name: kv_request_router
    params:
      lin_reads: 0
      replicated: 1
  - name: kv_state_worker
  - name: lattice_apply_bridge
  - name: ttl_scheduler
    params:
      propose_wrap: 1

  # ── Clustor substrate ─────────────────────────────────────
  - name: peer_router
    params:
      self_id: __SELF_ID__
      peer_count: 1
      listen_port: __LISTEN_PORT__
      peer0_port: __PEER0_PORT__
  - name: gateway
    params:
      self_id: __SELF_ID__
  - name: admission
  - name: control_plane
  - name: consensus
    params:
      self_id: __SELF_ID__
      voter_count: 1
      heartbeat_interval_ms: 150
  - name: durability
    params:
      fsync_mode: 1
      group_window_ms: 5
      group_max_pending: 4
      self_id: __SELF_ID__
      voter_count: 1
      partition_id: 0

  # ── Observability ─────────────────────────────────────────
  - name: adapter_metrics
  - name: operations
    params:
      listen_port: __HTTP_PORT__

wiring:
  # ── Edge: anchor ↔ router ─────────────────────────────────
  - from: linux_net.net_out
    to: redis_edge_anchor.net_in
  - from: redis_edge_anchor.net_out
    to: linux_net.net_in
  - from: redis_edge_anchor.kv_out
    to: kv_request_router.redis_in
  - from: kv_request_router.redis_out
    to: redis_edge_anchor.kv_in

  # ── Proposal ingress: router → gateway → consensus ────────
  - from: kv_request_router.proposal_out
    to: gateway.client_requests
  # Without this edge a gateway refusal (for example NOT_LEADER)
  # is dropped and the write times out instead of failing fast.
  - from: gateway.responses
    to: kv_request_router.gateway_reject
  - from: gateway.proposals_tagged
    to: consensus.proposals_tagged
  - from: consensus.proposal_assigned
    to: gateway.proposal_assigned
  - from: consensus.leader_state
    to: gateway.leader_state
  - from: admission.credits
    to: gateway.credit_supply
  - from: control_plane.routing
    to: gateway.placement

  # ── Apply path: committed entries → worker → response ─────
  - from: consensus.committed_entries
    to: lattice_apply_bridge.committed_in
  - from: lattice_apply_bridge.kv_out
    to: kv_state_worker.commands
  - from: kv_request_router.kv_out
    to: kv_state_worker.commands
  - from: kv_state_worker.responses
    to: kv_request_router.kv_in

  # ── The clock ─────────────────────────────────────────────
  # `ttl_scheduler` proposes a tick on a cadence; it becomes the
  # cluster's time only once it comes back committed, on the same
  # channel as the commands it orders against. That is what makes a
  # TTL expire at the same point of the log on every replica and
  # again on replay.
  - from: ttl_scheduler.propose_out
    to: gateway.client_requests
  - from: lattice_apply_bridge.records_out
    to: ttl_scheduler.tick_in
  - from: kv_state_worker.expiry_out
    to: ttl_scheduler.kv_expiry
  - from: ttl_scheduler.expire_out
    to: kv_state_worker.expire

  # ── Linearizable-read fence ───────────────────────────────
  - from: kv_request_router.lin_read_out
    to: consensus.read
  - from: consensus.applied
    to: kv_request_router.read_release
  - from: admission.permits
    to: consensus.read_permits

  # ── Persistence ───────────────────────────────────────────
  - from: consensus.log_append
    to: durability.entries
    rate: transaction
  - from: consensus.entry_request
    to: durability.entry_request
    buffer_bytes: 8192
  - from: durability.entry_reply
    to: consensus.entry_reply
    buffer_bytes: 8192
  - from: durability.replay_complete
    to: consensus.wal_replay_complete
    rate: transaction
  - from: durability.flushed
    to: consensus.wal_flushed
  - from: durability.installed_local
    to: consensus.snapshot_installed
  - from: consensus.wal_compact
    to: durability.compact_before
  - from: durability.quorum_durable
    to: consensus.durable

  # ── Telemetry fan-in → operations → HTTP /metrics ─────────
  - from: redis_edge_anchor.metrics
    to: adapter_metrics.events_in
  - from: kv_request_router.metrics
    to: adapter_metrics.events_in
  - from: kv_state_worker.metrics
    to: adapter_metrics.events_in
  - from: adapter_metrics.telemetry_out
    to: operations.ingest
  - from: consensus.metrics
    to: operations.ingest
  - from: admission.metrics
    to: operations.ingest
  - from: gateway.metrics
    to: operations.ingest
  - from: peer_router.metrics
    to: operations.ingest
  - from: durability.metrics
    to: operations.ingest

  # ── HTTP diagnostic surface ───────────────────────────────
  - from: linux_net.net_out
    to: operations.net_in
  - from: operations.net_out
    to: linux_net.net_in
    rate: transaction
EOF
```

Smoke checks from another terminal:

```sh
curl -i http://127.0.0.1:19090/readyz
# HTTP/1.1 200 OK once the node is ready (a few seconds)

redis-cli -p 6390 SET greeting hello-lattice
# OK             — appended, fsynced, committed, applied
redis-cli -p 6390 GET greeting
# hello-lattice
redis-cli -p 6390 INCR counter
# 1
redis-cli -p 6390 DEL greeting
# 1

curl -s http://127.0.0.1:19090/metrics | wc -c
# non-zero: the binary metrics export (decode with lattice-scrape,
# see tools/load)
```

Any RESP2 client works in place of `redis-cli`. Ctrl+C stops the
node. Each `--replicas` run starts clean: node state (the WAL under
`wal/`, Raft metadata under `raft/`) lives in a per-run scratch
directory, printed as `scratch:` at startup, along with the node's
log path.

## Three nodes

Three replicas on one machine: Raft wire ports 7000–7002, HTTP
17000–17002, Redis 6490–6492. `voter_count: 3` means majority quorum
is 2, so the cluster tolerates one node failure.

Each node needs its own Redis port, and `--replicas` applies `--var`
values uniformly, so a per-node port cannot come from a `--var`.
Render the template once per node instead and run each node yourself.
Save the template below as `cluster.yaml`:

```yaml
target: linux
tick_us: 1000

scheduler:
  accept_cycles: true

platform:
  net: {}

modules:
  # ── Lattice edge + KV ─────────────────────────────────────
  - name: redis_edge_anchor
    params:
      listen_port: __REDIS_PORT__
  - name: kv_request_router
    params:
      lin_reads: 0
      replicated: 1
  - name: kv_state_worker
  - name: lattice_apply_bridge

  # ── Clustor substrate ─────────────────────────────────────
  - name: peer_router
    params:
      self_id: __SELF_ID__
      peer_count: 3
      listen_port: __LISTEN_PORT__
      peer0_port: __PEER0_PORT__
      peer1_port: __PEER1_PORT__
      peer2_port: __PEER2_PORT__
      peer0_host: 127.0.0.1
      peer1_host: 127.0.0.1
      peer2_host: 127.0.0.1
  - name: gateway
    params:
      self_id: __SELF_ID__
  - name: admission
  - name: control_plane
  - name: consensus
    params:
      self_id: __SELF_ID__
      voter_count: 3
      peer_count: 3
      heartbeat_interval_ms: 150
      # Three runtimes share one machine's cores; widen the election
      # timeout so scheduler jitter does not churn elections.
      election_timeout_ms: 4000
      proposal_batch_timeout_ms: 1
  - name: durability
    params:
      fsync_mode: 1
      group_window_ms: 0
      group_max_pending: 8
      segment_bytes: 4194304
      self_id: __SELF_ID__
      voter_count: 3
      partition_id: 0

  # ── Observability ─────────────────────────────────────────
  - name: adapter_metrics
  - name: operations
    params:
      listen_port: __HTTP_PORT__

wiring:
  # ── Transport: linux_net ↔ peer_router ────────────────────
  # net_out fans out to peer_router, the anchor, and operations;
  # the fan-out stalls whole if any consumer ring fills, so each
  # consumer ring is sized up front.
  - from: linux_net.net_out
    to: peer_router.net_in
    buffer_bytes: 262144
  - from: peer_router.net_out
    to: linux_net.net_in

  # ── Edge: anchor ↔ router ─────────────────────────────────
  - from: linux_net.net_out
    to: redis_edge_anchor.net_in
    buffer_bytes: 262144
  - from: redis_edge_anchor.net_out
    to: linux_net.net_in
  - from: redis_edge_anchor.kv_out
    to: kv_request_router.redis_in
  - from: kv_request_router.redis_out
    to: redis_edge_anchor.kv_in

  # ── Peer traffic ──────────────────────────────────────────
  - from: peer_router.peer_rx
    to: consensus.ack
  - from: peer_router.raft_rpc
    to: consensus.rpc
  - from: consensus.rpc_out
    to: peer_router.peer_tx
  - from: consensus.net_out
    to: peer_router.repl_tx

  # ── Proposal ingress: router → gateway → consensus ────────
  - from: kv_request_router.proposal_out
    to: gateway.client_requests
  # Without this edge a gateway refusal (for example NOT_LEADER)
  # is dropped and the write times out instead of failing fast.
  - from: gateway.responses
    to: kv_request_router.gateway_reject
  - from: gateway.proposals_tagged
    to: consensus.proposals_tagged
  - from: consensus.proposal_assigned
    to: gateway.proposal_assigned
  - from: consensus.leader_state
    to: gateway.leader_state
  - from: admission.credits
    to: gateway.credit_supply
  - from: control_plane.routing
    to: gateway.placement
  - from: consensus.lag_signal
    to: admission.lag

  # ── Apply path: committed entries → worker → response ─────
  - from: consensus.committed_entries
    to: lattice_apply_bridge.committed_in
  - from: lattice_apply_bridge.kv_out
    to: kv_state_worker.commands
  - from: kv_request_router.kv_out
    to: kv_state_worker.commands
  - from: kv_state_worker.responses
    to: kv_request_router.kv_in

  # ── Linearizable-read fence ───────────────────────────────
  - from: kv_request_router.lin_read_out
    to: consensus.read
  - from: consensus.applied
    to: kv_request_router.read_release
  - from: admission.permits
    to: consensus.read_permits

  # ── Persistence ───────────────────────────────────────────
  - from: consensus.log_append
    to: durability.entries
    rate: transaction
  # A restarted replica reads replayed log bodies back out of its
  # local WAL; without this pair the apply pipeline stalls at the
  # first gap.
  - from: consensus.entry_request
    to: durability.entry_request
    buffer_bytes: 8192
  - from: durability.entry_reply
    to: consensus.entry_reply
    buffer_bytes: 8192
  # Boot handoff: Raft resumes at the WAL replay high-water before
  # proposal intake opens.
  - from: durability.replay_complete
    to: consensus.wal_replay_complete
    rate: transaction
  - from: consensus.wal_compact
    to: durability.compact_before
  - from: durability.flushed
    to: consensus.wal_flushed
  # Cross-node durability-acknowledgement fan-in — required for
  # quorum fsync; without it commit stalls on a multi-node graph.
  - from: consensus.cross_durability_ack
    to: durability.ack
  - from: durability.quorum_durable
    to: consensus.durable

  # ── Snapshots: local loop + peer transfer ─────────────────
  # The minimum follower match gates snapshot triggers so
  # compaction never outruns a live but lagging voter.
  - from: consensus.retention_floor
    to: durability.retention_floor
  - from: durability.installed_local
    to: consensus.snapshot_installed
  - from: durability.export_chunks
    to: consensus.snapshot_rx
  - from: consensus.snapshot_import
    to: durability.import_chunks
  - from: durability.manifest_auth
    to: peer_router.peer_tx
  # App snapshot bodies: the worker labels each body with its own
  # applied position, so a follower installing a snapshot can
  # rebuild KV state.
  - from: durability.app_snapshot_ctl
    to: kv_state_worker.snapshot_import
    rate: transaction
  - from: kv_state_worker.snapshot_export
    to: durability.app_snapshot_body
    rate: transaction

  # ── Telemetry fan-in → operations → HTTP /metrics ─────────
  - from: redis_edge_anchor.metrics
    to: adapter_metrics.events_in
  - from: kv_request_router.metrics
    to: adapter_metrics.events_in
  - from: kv_state_worker.metrics
    to: adapter_metrics.events_in
  - from: adapter_metrics.telemetry_out
    to: operations.ingest
  - from: consensus.metrics
    to: operations.ingest
  - from: admission.metrics
    to: operations.ingest
  - from: gateway.metrics
    to: operations.ingest
  - from: peer_router.metrics
    to: operations.ingest
  - from: durability.metrics
    to: operations.ingest

  # ── HTTP diagnostic surface ───────────────────────────────
  - from: linux_net.net_out
    to: operations.net_in
    buffer_bytes: 262144
  - from: operations.net_out
    to: linux_net.net_in
    rate: transaction
```

Render one config per node and run each in its own directory. The
durability module persists its WAL under `wal/` relative to the
process working directory, so two nodes must never share one;
`FLUXOR_PROJECT_ROOT` tells `fluxor run` where the built modules
live when the working directory is not the repository:

```sh
for i in 0 1 2; do
  mkdir -p /tmp/lattice/n$i
  fluxor render-template cluster.yaml \
    --var SELF_ID=$i --var LISTEN_PORT=$((7000+i)) \
    --var PEER0_PORT=7000 --var PEER1_PORT=7001 --var PEER2_PORT=7002 \
    --var REDIS_PORT=$((6490+i)) --var HTTP_PORT=$((17000+i)) \
    -o /tmp/lattice/n$i/node.yaml
done

# one terminal per node (or background each)
cd /tmp/lattice/n0 && FLUXOR_PROJECT_ROOT=<lattice checkout> fluxor run node.yaml
cd /tmp/lattice/n1 && FLUXOR_PROJECT_ROOT=<lattice checkout> fluxor run node.yaml
cd /tmp/lattice/n2 && FLUXOR_PROJECT_ROOT=<lattice checkout> fluxor run node.yaml
```

All three `/readyz` endpoints answer 200 once the cluster has
elected a leader:

```sh
for p in 17000 17001 17002; do
  curl -s -o /dev/null -w "$p %{http_code}\n" http://127.0.0.1:$p/readyz
done
# 17000 200
# 17001 200
# 17002 200
```

Writes commit on the leader; followers refuse them with the
authority error, and the committed value is readable from every
node. Find the leader by sweeping the `SET`:

```sh
for p in 6490 6491 6492; do
  echo -n "SET on $p: "; redis-cli -p $p SET cluster-key three-node
done
# SET on 6490: CLUSTERDOWN no authority available for this operation
# SET on 6491: OK
# SET on 6492: CLUSTERDOWN no authority available for this operation

for p in 6490 6491 6492; do
  echo -n "GET on $p: "; redis-cli -p $p GET cluster-key
done
# GET on 6490: three-node
# GET on 6491: three-node
# GET on 6492: three-node
```

To watch a failover, kill the leader's process and repeat the `SET`
sweep: within a few seconds one of the surviving nodes wins the
election and starts answering `OK`.

## Ports and other overrides

The templates carry no fixed ports. In `--replicas` mode
`--base-port` moves the Raft wire range and `--http-offset` the HTTP
range; per-node values in the by-hand flow are ordinary
`render-template --var`s. For a cluster across machines, render with
each peer's real address in place of `127.0.0.1` in the
`peer0_host`/`peer1_host`/`peer2_host` params.

## Reading the graph

The embedded configs are the canonical graph definitions: the module
set and every channel edge. Lattice contributes the Redis anchor and
the KV path (`kv_request_router`, `kv_state_worker`,
`lattice_apply_bridge`, `adapter_metrics`); the rest is the clustor
substrate palette (`peer_router`, `gateway`, `consensus`,
`durability`, `admission`, `control_plane`, `operations`). Other
protocol anchors (Memcached, etcd, SQL, document, wide-column,
models) are enabled the same way the Redis anchor is: add the module
and wire its `kv_out`/`kv_in` pair to a router ingress port. The
write path and read semantics are described in the
[specification](../architecture/specification.md).

## The diagnostic HTTP surface

Each node's `operations` module serves, on its HTTP port:

| Route | Behaviour |
|---|---|
| `GET /readyz` | 200 when ready, 503 otherwise; one-byte body |
| `GET /why` | two bytes: version, timing-pause reason |
| `GET /metrics` | binary metrics export (decode with `lattice-scrape`, see `tools/load`) |

Readiness is the substrate's: fresh telemetry from every module and
every Raft instance reporting ready. It carries no lattice-specific
detail beyond that flag.
