# Tuning

All tuning is done in the graph YAML: module `params:` and wiring
attributes. There is no separate tuning config. The knobs below are
the ones that exist; anything else should be measured before it is
believed. The reference graphs are in the
[run guide](running.md).

## Scheduler pacing — `tick_us`

The top-level `tick_us` sets the cooperative scheduler cadence and
is the dominant latency lever on constrained targets: every hop in
the anchor → router → consensus → WAL → apply → response pipeline is
paced by it. Lower ticks cut request latency until the WAL fsync no
longer fits the cadence; past that point a shorter tick buys nothing
and costs CPU.

## Durability (`durability` module `params:`)

- `fsync_mode` — 0 fsyncs per entry; 1 groups entries per fsync.
  Group fsync raises the write-throughput ceiling without weakening
  durability: acknowledgement still follows the fsync.
- `group_window_ms` / `group_max_pending` — the group-fsync batch
  window and depth. A zero window flushes every poll and still
  batches under burst via the pending depth; a wall-clock window
  adds its own width to acknowledgement latency.
- `fence_depth` — in-flight asynchronous fsync fences. Deeper
  fencing overlaps WAL writes with fsyncs and lifts durable-write
  throughput; it pairs with rate-classing the entry edge (below).
- `segment_bytes` — WAL segment size. Rotation runs a synchronous
  snapshot persist, so small segments put that stall into tail
  latency; larger segments make rotation rarer.

## Rate classing the WAL edge

The `consensus.log_append → durability.entries` wiring edge must
carry `rate: transaction`. Without a rate class the WAL entry pump
receives a zero byte-grant and processes one entry per tick
regardless of backlog — the single biggest write-throughput cliff in
a lattice graph. This is a wiring attribute, not a param.

## Fan-out ring sizing

`linux_net.net_out` fans out to every network consumer in the graph
(peer router, anchors, operations) through a tee that stalls whole
when any consumer's ring fills. On multi-node graphs, size each
consumer edge's `buffer_bytes` generously (the run guide's
three-node graph uses 256 KiB) so one slow or idle consumer cannot
gate the others.

## Consensus (`consensus` module `params:`)

- `heartbeat_interval_ms` — leader heartbeat cadence.
- `election_timeout_ms` — how long a follower waits before starting
  an election. Widen it when several runtimes share one machine's
  cores, where scheduler jitter can delay heartbeats and churn
  elections; tighten it on dedicated hardware for faster failover.
- `proposal_batch_timeout_ms` — how long sparse proposals are held
  for batching before append. Lower values favour latency, higher
  values throughput.

## Read classification (`kv_request_router` `params:`)

`lin_reads: 0` serves reads from local applied state with no fence:
the lowest read latency, at snapshot consistency. `lin_reads: 1`
sends every read through the leader ReadIndex fence: linearizable,
at the cost of a fence round trip per read, and reads fail closed
when no authority is available.

## Measuring

Judge changes by the node's own telemetry: scrape `/metrics` on the
`operations` port and decode with `lattice-scrape` (`tools/load`).
The export carries per-module counters and step histograms, so a
pacing change shows up as shifted step and flush distributions, not
just end-to-end numbers.
