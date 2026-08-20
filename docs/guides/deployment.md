# Deployment

A lattice node is not a standalone binary: it is a `fluxor run` over
a graph config that composes lattice's protocol and KV modules onto
the clustor substrate. Deploying means building the PIC modules,
choosing a graph, and launching each node with `fluxor run`. The
canonical graphs, validated end to end, are embedded in the
[run guide](running.md); this page covers what surrounds them.

## Prerequisites

- The toolchain pinned by `rust-toolchain.toml` (stable channel with
  the `aarch64-unknown-none` target and `rust-src`/`rustfmt`/
  `clippy`). There is no separate MSRV; the pin is authoritative.
- A `fluxor` CLI whose wire ABI matches `fluxor.toml [required]`.
- Published fluxor and clustor artefacts in the local OCI store —
  the one-time setup in the [run guide](running.md#prerequisites).

## Building

```sh
make build                              # fluxor build
fluxor modules build --all              # PIC module .fmods (host)
fluxor modules build --target bcm2712   # Pi 5-class silicon
```

The silicon target list lives in `fluxor.toml`. After editing
anything under `modules/`, rebuild the `.fmod`s.

## Configuration

There is no `lattice.toml`. A node's configuration is its fluxor
graph YAML: the target platform, the scheduler tick, the module
list (protocol anchors, KV modules, substrate modules), per-module
`params:`, and the wiring between them. The run guide's embedded
graphs are the reference deployment; the knobs they expose are
described in [tuning.md](tuning.md). The important groups:

- **Edge enablement and ports** — which anchor modules appear under
  `modules:` and each one's `listen_port`. A surface exists in a
  deployment exactly when its anchor is in the graph.
- **Read classification** — the router's `lin_reads` (0 default:
  reads are served from local applied state; 1: every read takes the
  ReadIndex fence) and `replicated` (declares that writes go through
  Raft; checked against the wiring at init).
- **Durability** — the `durability` module's `fsync_mode`,
  `group_window_ms`, `group_max_pending`, `fence_depth`,
  `segment_bytes`, `voter_count`, `partition_id`.
- **Consensus and peers** — `self_id`, `voter_count`, `peer_count`,
  `heartbeat_interval_ms`, `election_timeout_ms`, and the
  `peer_router` port/host list.
- **State store** — the worker's `state_store` (0 memory, 1 disk
  runs with recovery before serving).
- **Diagnostics** — the `operations` module's `listen_port` for
  `/readyz` and `/metrics`.

## Transport security

Transport security is composition, not a config section: the fluxor
`tls` foundation module is wired between `linux_net` and an anchor
(`tls.clear_out` → anchor, anchor → `tls.clear_in`), so client
traffic is TLS-terminated before it reaches the protocol code. The
run guide's graphs are cleartext; add the `tls` module and its
wiring for a TLS-terminated deployment. The etcd anchor has no TLS
of its own — without the `tls` module in front of it, gRPC is
plaintext HTTP/2.

## Running

Single node and three-node bring-up, with smoke checks and
teardown, are the [run guide](running.md). There is no systemd
unit, no container image, and no `lattice` binary: a node is
exactly the `fluxor run` process over a graph config. Node state
lives relative to the process working directory: the WAL under
`wal/`, Raft metadata under `raft/`, and disk state-store runs under
`kv/`. Run each node in its own directory.

For bare-metal targets (`bcm2712`, Raspberry Pi 5-class), the same
modules are built with `fluxor modules build --target bcm2712` and
the node boots as a fluxor image on the board rather than as a Linux
process; the graph shape is the same, with the board's storage and
network modules in place of `linux_net`.

## Verifying a deployment

```sh
curl -i http://<host>:19090/readyz    # 200 + one-byte body when ready
curl -s http://<host>:19090/metrics   # binary export; decode with
                                      # lattice-scrape (tools/load)
```

Readiness reflects the substrate: fresh telemetry from every module
and every Raft instance ready. Point load-balancer health checks at
`/readyz` so a node that has lost its quorum or wedged its pipeline
is pulled from rotation.

## Troubleshooting

### Writes fail with the authority error

A Redis `SET` answering `-CLUSTERDOWN no authority available for
this operation` (or the equivalent on another surface) means this
node cannot commit: it is a follower, no leader is elected, or the
quorum is lost. Propose to the leader (sweep the nodes to find it)
or restore quorum. With `lin_reads: 1`, reads fail the same way when
the ReadIndex fence cannot complete.

### Write path wedges after a restart

A durable node replays its WAL on boot, and Raft must resume at the
replay high-water before proposal intake opens. The run guide's
graphs carry the two edges that make this work —
`durability.replay_complete → consensus.wal_replay_complete` and the
`entry_request`/`entry_reply` pair. A graph missing either looks
healthy on a fresh WAL and wedges its write path on the first
restart against an existing log.

## See also

- [running.md](running.md) — validated bring-up and smoke checks
- [tuning.md](tuning.md) — the knobs and what they trade
- [high_availability.md](high_availability.md) — topologies and
  failure behaviour
- [Specification](../architecture/specification.md) — semantics
