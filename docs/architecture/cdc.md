# Change data capture

Lattice's CDC is an egress pump: a module that reads committed
changes out of a running graph and delivers them, in order and with
acknowledged durability, to a sink of the operator's choosing. The
protocol logic lives in `modules/common/cdc_feed.rs` (the feed state
machine) and `modules/common/cdc_wire.rs` (the event and sink
envelopes); the module shell is `modules/app/cdc_pump/mod.rs`. One
pump instance serves one feed.

## Position in the graph

The pump is an ordinary KV client on a spare router anchor pair — it
has no privileged read path. It reads committed change windows with
versioned scans, backfills from a snapshot scan when it starts
behind, and stores its own cursor as ordinary KV records through the
same router, with a compare-and-swap generation pointer so a
restarted or superseded pump cannot corrupt the checkpoint.

A versioned scan walks the feed's key span in key order and filters by
revision, so its length is the span's, not the window's. The disk
store bounds that walk per step (`SCAN_STEP_BLOCKS` run blocks) and
holds its position, and the state worker re-drives the paused page one
slice per step. Other commands keep flowing between slices — except a
second versioned scan, which waits at the channel head (and queues the
commands behind it) until the held one answers; a window at the
store's high-water mark is answered empty without opening a run. A
large table therefore costs the pump page latency, never the worker
its step deadline.

## Events

Each change produces one little-endian `CdcEvent`:

```
[format:u16][kind:u8][flags:u8][feed_id:u64][table_id:u32]
[key_len:u16][key][commit_ts:u64][range_id:u64]
[range_generation:u32][revision:u64][value_len:u32][value]
```

`kind` is 1 put, 2 delete, 3 resolved, 4 backfill-complete,
5 topology. Event identity is `(feed_id, table_id, key, commit_ts)`;
the range and revision fields are diagnostics, not identity.
`resolved` watermark events carry a frontier timestamp and are
broadcast to every ordering unit: a consumer that has seen
`resolved(T)` has seen every change with `commit_ts ≤ T`.

## The sink contract

Events leave the graph as `SinkPublish` frames on a channel to any
module declaring the `stream.ordered_ack` capability. The sink
answers each frame with a `SinkAck`: status 0 means durably
accepted, non-zero statuses are typed refusals, and correlation 0
carries link-down/link-up signals that force republication of every
unacknowledged event. The pump advances its durable checkpoint only
past the contiguous acknowledged prefix, so delivery is
at-least-once end to end: a sink outage stalls the feed, never loses
it.

The in-repo reference sink is `modules/app/loopback_sink`, which
validates, counts, digests, and acknowledges in order, with a
scripted link-loss param to exercise replay. MQTT delivery lives in
quantum.

## Lifecycle and retention

The feed reports its state on a gauge: created, backfill, streaming,
stalled, needs-backfill, wedged, halted. While it runs it holds a
retention claim with `compaction_coordinator` so MVCC garbage
collection cannot outrun an in-flight feed — but the claim carries a
budget and expires on its own, so a dead pump's hold lapses without
cooperation and compaction resumes.
