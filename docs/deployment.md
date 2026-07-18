# Lattice Deployment Guide

This document covers deploying Lattice. A Lattice node is **not** a standalone
binary — it is a `fluxor run` over a graph config that composes Lattice's
protocol/KV modules onto the Clustor Raft / WAL / snapshot substrate. Deploying
therefore means building the PIC modules, choosing a graph config in `configs/`,
and launching each node with `fluxor run`.

## Prerequisites

- Toolchain as pinned by `rust-toolchain.toml` (channel `stable`, with the
  `aarch64-unknown-none` bare-metal target and `rust-src`/`rustfmt`/`clippy`).
  Do not track a separate MSRV — `rust-toolchain.toml` is authoritative.
- A working `fluxor` CLI whose wire ABI matches `fluxor.toml [required]`
  (`fluxor = { abi = 1 }`).
- For bare-metal nodes: an aarch64 target board. The shipped bare-metal configs
  target `bcm2712` (Raspberry Pi 5), netbooted onto the rig (see the bare-metal
  configs and `standards/rig.md`).

## Building

The lifecycle is delegated to `fluxor` verbs via the `Makefile`:

```bash
make build      # fluxor build  — stages the SDK, builds modules + host crates
make test       # fluxor test
make lint       # fluxor lint
make ci         # fluxor ci     — full gate (module build, lints, wire/catalog)
```

Build the PIC module artifacts (`.fmod` ELFs) for a hardware target:

```bash
fluxor modules build --target bcm2712   # bcm2712 silicon = Pi 5 board
```

The build targets are declared in `fluxor.toml [ci].targets` (currently
`bcm2712`). After editing anything under `modules/`, rebuild the `.fmod`s —
`fluxor build` alone does not recompile the PIC modules.

## Configuration

There is no `lattice.toml`. A node's configuration **is** a fluxor graph YAML in
`configs/`. Each file declares the target platform, the scheduler tick, the set
of enabled modules (protocol anchors, KV worker, Clustor substrate modules), and
the wiring between them. Knobs live per-module under `params:`.

Representative configs:

| Config | Shape |
| ------ | ----- |
| `configs/single-replicated-lattice.yaml` | Single-node host bring-up: Redis edge → Raft → WAL → apply (`voter_count: 1`). |
| `configs/bare-metal-pi5.yaml` | Bare-metal Pi 5 twin of the above; durability rides the real `nvme` → `fat32` → WAL storage stack. |
| `configs/multi-3node.yaml` | 3-node cluster template with per-node placeholders (see [Multi-Node](#multi-node)). |
| `configs/single.yaml` | Single node with a `tls`-terminated transport in front of the edge anchors. |

### Real knobs (cite the config, don't invent)

These are the knobs actually present in the shipped graph YAML. Read the file
before relying on any of them; the examples below are copied from
`configs/single-replicated-lattice.yaml` and `configs/bare-metal-pi5.yaml`.

- **Platform / pacing** — top-level `target:` (`linux` for host, `pi5` for
  bare-metal) and `tick_us:` (scheduler cadence; on the Pi 5 this is the
  dominant latency lever — see the tuning notes inline in `bare-metal-pi5.yaml`).
- **Edge enablement + ports** — which protocol anchor modules are listed under
  `modules:` (e.g. `redis_edge_anchor`), and the router's `listen_port` /
  read classification (`lin_reads`), `replicated`.
- **Durability** (`durability` module `params:`) — `fsync_mode`,
  `group_window_ms`, `group_max_pending`, `fence_depth`, `segment_bytes`,
  `voter_count`, `partition_id`; bare-metal adds `root_path`, `skip_replay`.
- **Consensus** (`consensus` module) — `self_id`, `voter_count`,
  `heartbeat_interval_ms`.
- **Peer topology** (`peer_router` module) — `self_id`, `peer_count`,
  `listen_port`, `peerN_port` / `peerN_host`.
- **Observability HTTP** (`operations` module) — `listen_port` for the
  `/metrics` + `/readyz` surface, `emit_interval_ms`.

The durable write path these configs wire is: Redis edge anchor →
`kv_request_router` → `gateway` → `consensus` (Raft append → `durability` WAL →
quorum → commit) → `lattice_apply_bridge` → `kv_state_worker` → response. HA,
quorum, leadership, and snapshots are all provided by the Clustor substrate
modules (`consensus`, `durability`, `admission`, `control_plane`, `operations`,
`gateway`, `peer_router`), not by Lattice code.

## Multi-Node

`configs/multi-3node.yaml` uses `__SELF_ID__`, `__REDIS_PORT__`, `__HTTP_PORT__`,
`__LISTEN_PORT__`, and `__PEER*_PORT__`/`__PEER*_HOST__` placeholders. The fluxor
template renderer fills these per node before launching `fluxor run` — the
placeholder set and CI-render defaults are declared in `fluxor.toml
[ci.templates]` (those defaults exist to pass the render gate; they are not the
production values). Each of the three nodes runs the same rendered config with
its own identity and peer list, and the substrate forms a `voter_count: 3`
quorum.

## Transport Security (TLS / mTLS)

Transport security is a foundation `tls` module composed into the graph, not a
config file section. `configs/single.yaml` wires the fluxor `tls` foundation
module in front of the edge anchors (`tls.clear_out` → anchor, anchor →
`tls.cipher_out`), so etcd/Redis traffic is TLS-terminated before it reaches the
protocol anchor. The replicated example configs do not enable `tls`; add the
module and its wiring (as in `single.yaml`) for a TLS-terminated deployment.

## Running

Bring up a single node:

```bash
fluxor run configs/single-replicated-lattice.yaml
```

For bare-metal, the node is netbooted onto the rig rather than run locally
(see the boot/drive/scrape commands documented inline at the top of
`configs/bare-metal-pi5.yaml`, and `standards/rig.md`).

There is no systemd unit, no container image, and no `lattice start` /
`/usr/local/bin/lattice` — a node is exactly the `fluxor run` process over a
graph config.

## Verifying Deployment

The `operations` module owns the HTTP surface. With a config that binds it
(e.g. `operations.listen_port: 19090`):

```bash
# Readiness — reports the tenant routing-epoch cache age, adapter enablement
# states, and a digest of the active tenant manifest.
curl http://<host>:19090/readyz

# Metrics
curl http://<host>:19090/metrics
```

On bare-metal, telemetry is also observable over `log_net` UDP in addition to
the `/metrics` HTTP endpoint (see `configs/bare-metal-pi5.yaml`).

## Observability

- Lattice metrics live in the dotted namespaces `lattice.kv.*`,
  `lattice.adapter.<name>.*`, `lattice.quota.*`, plus inherited `clustor.*`
  metrics. The authoritative list is `telemetry/catalog.json` and the
  specification's [observability](specification.md#observability) section. Do
  not rely on metric names not present there.
- The catalog is validated in CI by `tools/ci/telemetry_guard`.

## Troubleshooting

### LIN-BOUND / strict-fallback failures

If linearizable operations fail (e.g. Redis `GET` → `-CLUSTERDOWN`), the node's
CP cache is stale/expired or no ReadIndex authority is available. This is the
inherited Clustor strict-fallback behavior (see the specification's
[control plane](specification.md#control-plane) and
[read semantics](specification.md#read-semantics-and-lin-bound) sections).
Restore control-plane freshness; the cache refreshes automatically.

### Write path wedges after a restart

A durable node replays its WAL on boot. The `durability.replay_complete` →
`consensus.wal_replay_complete` handoff must be wired or the apply pipeline
stalls after replay — see the inline note in
`configs/single-replicated-lattice.yaml`. On the perf rig, stale `.WAL`
segments from prior runs accumulate (no FS delete); `skip_replay: 1` starts
fresh for measurement (never for crash-recovery scenarios).

## See Also

- [High Availability](high_availability.md) — HA topologies and failure modes
- [Performance](performance.md) — latency/throughput targets and measurement
- [Specification](specification.md) — KV semantics, observability, DR
- [Interoperability](interop.md) — etcd/Redis/Memcached client compatibility
