# Change data capture

Lattice's CDC is an egress pump: a module that reads committed
changes out of a running graph and delivers them, in order and with
acknowledged durability, to a sink of the operator's choosing. The
protocol logic lives in `modules/common/cdc_feed.rs` (the feed state
machine) and `modules/common/cdc_wire.rs` (the event envelope and
the sink message key); the module shell is `modules/app/cdc_pump/mod.rs`. One
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
commands behind it) until the held one answers. The memtable-resident
part of a walk is bounded per step too (`SCAN_STEP_RECORDS`), and a
window starting at or above the store's durable high-water mark is
answered from the memtable alone, without opening a run. A large table
therefore costs the pump page latency, never the worker its step
deadline.

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

The pump is a requester of fluxor's exchange contract
(`contracts/exchange.rs` in the fluxor SDK): `request_out` and
`response_in` wire to the `request_in` and `response_out` of any module
declaring the `stream.ordered_ack.sink` capability. Each event is one
`METHOD_PUBLISH` exchange carried whole in its request HEAD:

| Part | Value |
|---|---|
| id | the event's correlation counter, `ExchangeId::from_u64` |
| flags | `BROADCAST` for resolved watermarks and the backfill-complete marker, else none |
| target | the ordering key `[table_id:u32 BE][key]` |
| headers, peer | empty |
| resp_credit | 0 |
| body | the `CdcEvent` envelope above, exactly |

The answer decides each event. 200 means durably accepted. A 5xx (or an
ABORT from the sink) is retried after a backoff of 100 ms doubling to
5 s, and nothing new is published while a retry is owed. Any other
status is a refusal: counted, and never skipped. LINK DOWN makes every
open exchange unknowable; the pump publishes nothing more until LINK UP
and then issues each open one again, in order. It does not wait for a
LINK UP before its first publish, because LINK only reports a change.
At most 16 exchanges are open at once (`UNACKED_CAP`).

The pump advances its durable checkpoint only past the contiguous
prefix answered 200, so delivery is at-least-once end to end: a sink
outage stalls the feed, never loses it, and a refusal pins the
checkpoint before the refused event.

The in-repo reference sink is `modules/app/loopback_sink`. It validates,
counts and digests each publish and answers 200 in order, and its
`linkdown_every` param scripts link loss to exercise re-issue. MQTT,
Kafka and AMQP delivery are quantum's `mqtt_sink`, `kafka_sink` and
`amqp_sink`, wired the same way. `configs/bare-metal-pi5-cdc.yaml` uses
`mqtt_sink`.

## Subscriptions

The pump is the PUSH form of the feed. `cdc_front` serves the PULL form:
a requester's `GET /cdc/<prefix hex>` (optionally `after: <revision>`) is
answered `200` flagged `HOLD|MORE`, then one BODY per change, each the
envelope above exactly as the pump publishes it, never past the
requester's credit; a compacted `after` is 410, and the requester ends the
subscription with ABORT. The subscriber owns its position, so there is no
checkpoint; both forms read the same windows through the same router
surface. The surface is in [guides/fronts.md](../guides/fronts.md#cdc-subscriptions-cdc_front);
the protocol in `modules/common/cdc_subscribe.rs`.

## Lifecycle and retention

The feed reports its state on a gauge: created, backfill, streaming,
stalled, needs-backfill, wedged, halted. While it runs it holds a
retention claim with `compaction_coordinator` (source `WATCH`,
refreshed with an expiry `retention_budget_ms` ahead, default 60 s) so
MVCC garbage collection cannot outrun an in-flight feed. An expired
claim does not release the floor: the coordinator keeps honouring the
last floor a source asked for (and, with `WATCH` among its
`required_sources`, stops advancing until the source is fresh again),
so a pump that stops for good holds compaction at the position it
last claimed until it runs again.
