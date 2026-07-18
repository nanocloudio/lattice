# Lattice Performance Guide

This document covers performance characteristics, the one sourced latency
target, tuning knobs that actually exist in the graph configs, and how
performance is measured.

## Latency target

The one authoritative figure is the L1 latency target from the specification:
≤10 ms p99 in-AZ server-side latency, measured from edge ingress receipt to WAL
quorum durability for single-key writes under healthy conditions. It excludes
client RTT, includes adapter parsing and authorization, and applies to durable
writes only — snapshot-only reads do not wait for WAL and are not subject to it.

This is a planning target, not a measured guarantee. The binding requirements
are the durability and gating rules in the specification.

## Throughput and latency planning targets

These are planning targets, not measured results. Real numbers come from the
perf harness (below). Per `standards/rig.md` conventions, a number from any
single run is a floor, not a prediction; the Pi 5 also throttles roughly 2× when
hot, so a cool-boot burst and a sustained run differ. Treat the tables below as
design goals to validate.

### Single-key write latency (planning target)

| Operation | p99 target |
| --------- | ---------- |
| Put / Delete (single key, quorum-durable) | ≤10 ms (the L1 target) |
| Linearizable Get | measure; see harness |
| Snapshot-only Get | measure; not subject to L1 |

Anything not tied to the sourced L1 figure above should be measured, not
quoted. The inline tuning notes in `configs/bare-metal-pi5.yaml` record actual
rig-observed numbers (e.g. SET p99 ~9.76 ms at `tick_us: 1000`) with their
provenance — read those rather than repeating round numbers here.

## Tuning Knobs (real config params)

All tuning is done in the fluxor graph YAML. There is no `[runtime]`, `[raft]`,
`[batching]`, `[network]`, `[paths]`, or `[listeners.grpc]` config — those
sections do not exist. The knobs that do exist (see
`configs/bare-metal-pi5.yaml`, which carries the measured rationale inline):

### Scheduler pacing — `tick_us`

The dominant latency lever on constrained targets. On the Pi 5, dropping
`tick_us` from 3000 → 1000 cut PING/GET/SET latency ~3× and brought SET p99
under the ≤10 ms L1 target. Sub-1 ms ticks were tried and reverted — the WAL
fsync step (~375–950 µs) cannot fit a 500 µs cadence. See the inline notes.

### Durability (`durability` module `params:`)

- `fsync_mode` — per-entry (0) vs group (1). Group fsync is kept: equal latency,
  higher write-throughput ceiling, durability intact (fsync-before-ack).
- `group_window_ms` / `group_max_pending` — batch window and depth. On NVMe,
  `group_window_ms: 0` (flush every poll) beat a nonzero window on both p50/p99
  and throughput ceiling.
- `fence_depth` — in-flight async fsync fences. Paired with the rate-classed
  `consensus.log_append → durability.entries` edge, this lifted durable-write
  throughput from ~1050 to ~3900/s.
- `segment_bytes` — WAL segment size; larger segments make rotation
  (synchronous snapshot-persist) rarer, keeping p99 clean.

### Consensus (`consensus` module) — `heartbeat_interval_ms`, `voter_count`.

### Storage stack (bare-metal) — the `nvme` (`queue_depth`) and `fat32` modules
provide the FS contract the WAL writes through.

### Rate classing

The `consensus.log_append → durability.entries` wiring edge carries
`rate: transaction`. Without it the WAL entry-pump gets a zero byte-grant and
processes one entry per tick regardless of backlog — the single biggest
throughput cliff. This is a wiring attribute, not a config section.

## Measurement

Real numbers are produced by the shadow-tracked perf harness and the host-side
load generator. There is no `lattice benchmark` command or standalone benchmark
binary.

- **`perf/run_l1.sh`** — L1 single-node durable baseline. Brings up one node on
  `configs/single-replicated-lattice.yaml` (real Redis → Raft → disk-WAL → apply,
  `voter_count: 1`) and drives it open-loop with `lattice-loadgen` across a
  workload matrix, saving one provenance-stamped JSON per point under
  `perf/results/`. A local run is harness-bound (driver == DUT, loopback); the
  rig-trustworthy twin drives the Pi 5 DUT (`configs/bare-metal-pi5.yaml`) from
  the dev host.
- **`perf/run_l2_3node.sh`**, **`perf/run_l4_xmachine.sh`** — multi-node and
  cross-machine scopes.
- **`tools/load/lattice-bench`** (`lattice-loadgen`) — the off-DUT load
  generator and `/metrics` scraper.

Because `tests/`, `benches/`, and the perf scripts are shadow-tracked
(`.git-shadow`), a plain checkout has none of them; CI's `shadow_guard.sh`
fails rather than passing vacuously.

## Metrics Interpretation

Lattice metrics use the dotted namespaces `lattice.kv.*`,
`lattice.adapter.<name>.*`, `lattice.quota.*`, plus inherited `clustor.*`.
The authoritative names, types, labels, and units are in `telemetry/catalog.json`
and the specification's [observability](specification.md#observability) section.
Use those; do not invent metric names.

Scrape the `operations` module's `/metrics` endpoint (its `listen_port`, e.g.
`19090`) to read them; the catalog is CI-validated by `tools/ci/telemetry_guard`.

## See Also

- [Deployment Guide](deployment.md) — build + config reference
- [High Availability](high_availability.md) — HA tuning and failure modes
- [Specification](specification.md) — observability, protocol details
