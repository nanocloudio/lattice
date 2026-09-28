# Limit register

This is the inventory of Lattice's deliberate finite bounds. A capacity or
semantic ceiling present in source but absent here is a defect. The register is
also the review point for composed deployments: the first bound encountered
across Lattice, Clustor, Fluxor, and a protocol client is the effective bound.

`Capacity` limits retained work or state; `shape` limits one key, value, row, or
request; `semantic` changes the guarantee delivered when exhausted; `pacing`
only limits work per step and must eventually make progress. “Reject” means an
explicit error; “backpressure” retains the operation for retry; “defer” moves
work to a later step; “drop/evict” is lossy and therefore called out explicitly.

Every checkable row's `Symbol` must exist in its `Source` as a `const` whose
value matches `Value`. `—` marks a derived or typed constant which still
requires review but is not checked by the simple integer-value guard.

## Profiles

A module built two ways declares some bounds per build; a machine-checked row
for such a constant names the build it describes. `bounded` is a module's
default build, every buffer inline in its state; `big` is the variant of
`document_executor_big`, whose whole-document buffers are one heap arena sized
at load. A row with no profile holds for every build.

```limit-register-profiles
bounded | not(feature = "big")
big     | feature = "big"
```

## Core state and storage

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| In-memory keys per store | Capacity | `MAX_KEYS` | modules/common/kv_store.rs | 1024 | Rejects a new key; updates to existing keys remain possible. This bounds both raw-memory mode and each replicated memory-state worker. |
| Document id key | Semantic | `MAX_DOCUMENT_ID_KEY` | modules/common/models.rs | 1024 | A policy: MongoDB's historical index-key limit; Kubernetes' longest object id (namespace + name) is about a third of it. A longer id is refused before a key is built. |
| In-memory physical-key slot | Shape | `MAX_KEY_LEN` | modules/common/kv_store.rs | 2584 | Derived: the 12-byte identity, a keyspace, and the longest model key (a document index entry at `MAX_DOCUMENT_ID_KEY`, worst-case escaped); a contract test holds it to the derivation. A longer composed key is rejected; what one replicated write carries is the consensus entry's bound. |
| In-memory value bytes | Shape | `MAX_VALUE_LEN` | modules/common/kv_store.rs | 4096 | Rejects the operation. |
| State-worker scratch | Capacity | `SCRATCH_BUF_SIZE` | modules/app/kv_state_worker/mod.rs | 8192 | Rejects an over-budget frame or memory snapshot export; the WAL remains authoritative. |
| App-snapshot body | Shape | `SNAPSHOT_BODY_MAX` | modules/app/kv_state_worker/mod.rs | — | Derived as 8164 bytes (`SCRATCH_BUF_SIZE - APP_SNAPSHOT_HDR`); this binds before Clustor's 16384-byte snapshot body. |
| Worker answer body | Shape | `RESULT_BODY_MAX` | modules/app/kv_state_worker/mod.rs | — | Derived as `SCRATCH_BUF_SIZE` less the `MSG_KV_APPLIED` envelope (8151 bytes): the store answers into exactly what can be sent, so no answer is ever built that would go unsent. |
| Snapshot chunk | Pacing | `SNAPSHOT_CHUNK_MAX` | modules/app/kv_state_worker/mod.rs | 4096 | Streams another chunk; does not cap total disk-state size. |
| Disk provider user key | Shape | `MAX_USER_KEY` | modules/common/disk_store.rs | 2584 | Equal to `kv_store::MAX_KEY_LEN` (a contract test holds them together). |
| Encoded disk key | Shape | `MAX_ENCODED_KEY` | modules/common/disk_store.rs | — | Derived: `internal_key::encoded_max_len(MAX_USER_KEY)`. Rejects keys whose escaped/on-disk representation cannot fit. |
| Disk value record | Shape | `MAX_VALUE_LEN` | modules/common/disk_store.rs | — | Derived as 4150 bytes to include metadata around the 4096-byte public value. |
| Memtable entries | Capacity | `MEMTABLE_MAX_ENTRIES` | modules/common/disk_store.rs | 512 | Backpressures mutation while flush catches up. |
| Memtable flush watermark | Pacing | `MEMTABLE_FLUSH_WATERMARK` | modules/common/disk_store.rs | 384 | Starts flush early; not a refusal ceiling. |
| Version-scan blocks per step | Pacing | `SCAN_STEP_BLOCKS` | modules/common/disk_store.rs | 2 | The walk pauses holding its position; the worker re-drives it next step. Bounds run-file I/O per step, not the window. |
| Version-scan records per step | Pacing | `SCAN_STEP_RECORDS` | modules/common/disk_store.rs | — | Derived as `MEMTABLE_MAX_ENTRIES` (512); bounds the memtable-resident part of a walk, which loads no blocks. |
| Parked version-scan page | Capacity | `SCAN_PAGE_MAX` | modules/common/disk_store.rs | 8192 | The reply prefix kept across a pause. The engine caps every page at this budget, so a park can never be refused; the cap therefore bounds one command's reply, not a refusal path. |
| Version-scan entry shrink | Shape | `VERSIONS_TRANSCODE_SLACK` | modules/common/kv_store.rs | 6 | The least a reply entry is smaller than its raw record; the engine sizes each store request by it, so a record the store hands over always fits the page (an overflow record is stashed against the held ordinal, so the cursor never runs ahead of the caller). |
| Held command frame | Shape | `HELD_FRAME_MAX` | modules/app/kv_state_worker/mod.rs | 1024 | A pausable command larger than this is not held; the request times out upstream and is re-issued. Only `KV_OP_SCAN_VERSIONS` pauses, and its frame is bounded well under this. |
| Command-log answer room | Shape | `CLOG_OUT_MAX` | modules/app/kv_state_worker/mod.rs | 65536 | The largest answer room an op is applied with (a store-provider call's); a logged op replays with the room it first had, capped here. |
| Imported host-log record | Shape | `IMPORT_RECORD_MAX` | modules/app/kv_state_worker/mod.rs | 3149824 | One record of a fluxor host `store.log` being imported (`kv_state_worker_store`): the largest object (3 MiB) and its key. A larger record stops the import and quarantines the store rather than skip it. |
| Immutable runs | Capacity | `MAX_RUNS` | modules/common/disk_store.rs | 16 | Backpressures until compaction frees a run slot. |
| Manifest files | Capacity | `MAX_MANIFEST_FILES` | modules/common/disk_store.rs | 8 | Bounds retained manifest generations; rotation/reclamation must keep pace. |
| Runs returned by listing | Capacity | `MAX_LISTED_RUNS` | modules/common/disk_store.rs | 1024 | Rejects/flags an inventory that cannot be represented; never silently truncates authoritative run state. Sized for the orphans a crash can leave of undrained retirements (recovery deletes them only if it can list them); 4 KB of stack, once, at recovery. |
| Open run descriptors | Capacity | `SLOTS` | modules/common/fd_cache.rs | 5 | Evicts the least-recently-used descriptor; data remains on disk. This can dominate cold-read latency. |
| Filesystem store path | Shape | `PATH_MAX` | modules/common/fs_run_storage.rs | 145 | Bounds the internally encoded store/run path: a named store directory (`DIR_MAX`), `/`, and an 8.3 name. |
| Named store directory | Shape | `DIR_MAX` | modules/common/fs_run_storage.rs | 128 | A worker's `store_dir`; a longer one refuses construction. |
| Replica command | Shape | `MAX_COMMAND_BYTES` | modules/common/replica_facade.rs | 4096 | Rejects a larger command locally, although Clustor's 2048-byte WAL entry binds first on the current replicated graph. |
| Replica response | Shape | `MAX_RESPONSE_BYTES` | modules/common/replica_facade.rs | 4096 | Rejects an oversized response rather than truncating it. |
| Replica watch event | Shape | `MAX_WATCH_EVENT_BYTES` | modules/common/replica_facade.rs | 4096 | Rejects an oversized event rather than truncating it. |

The raw in-memory store is therefore the fastest mode but it is deliberately
small and non-durable. Putting that store behind Clustor adds ordering,
replication, failover, and snapshot recovery, but does not remove the 1024-key
or 4096-byte-value bounds. Disk mode removes the key-count ceiling from live
state, subject to filesystem capacity and the finite memtable/run pipeline.

## Routing, placement, and transactions

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Hash slots | Topology | `SLOT_COUNT` | modules/common/partition_map.rs | 1024 | Fixed mapping granularity, not a key-count limit. |
| Partition-map key bound | Shape | `MAX_KEY_BOUND_LEN` | modules/common/partition_map.rs | 256 | Rejects an unrepresentable range boundary. |
| Replicas in placement | Topology | `MAX_REPLICAS` | modules/common/partition_map.rs | 5 | Rejects/clamps larger placement; also below Clustor's seven-node identifier envelope. |
| Ranges in a partition map | Topology | `MAX_RANGES` | modules/common/partition_map.rs | 64 | Rejects an overfull map. Scale beyond it by composing partition groups/maps, not by adding replicas to one group. |
| Partition ports on one request router | Topology | `MAX_PARTITION_PORTS` | modules/app/kv_request_router/mod.rs | 2 | Map load refuses any binding to port 2 or above. With `partition_fanout` enabled, keyed traffic for partitions beyond 0 shares one tagged port and `partition_demux` fans it out, so the keyed-path ceiling becomes the demux's; targeted lifecycle operations still address the direct ports and keep this bound. |
| Local partitions behind one demux | Topology | `MAX_LOCAL_PARTITIONS` | modules/app/partition_demux/mod.rs | 8 | A frame tagged at or above the configured partition count is dropped and counted; its port is not wired. |
| Concurrent routed requests | Capacity | `MAX_INFLIGHT` | modules/app/kv_request_router/mod.rs | 256 | Rejects busy; caller retries. |
| Commands waiting for a worker | Capacity | `KV_STASH_SLOTS` | modules/app/kv_request_router/mod.rs | 4 | A command the worker's channel has no room for waits, in order; while any waits no new request is taken, so backpressure reaches the anchors. Past it (internal re-dispatch only) the op is refused busy, never dropped silently. |
| Waiting command frame | Shape | `KV_STASH_FRAME` | modules/app/kv_request_router/mod.rs | — | Derived: the largest command the router builds and a partition tag. |
| Linearizable-read slots | Semantic | `LIN_READ_SLOTS` | modules/app/kv_request_router/mod.rs | 32 | Current code falls back to the snapshot path when full. That silently weakens a requested guarantee and should be changed to backpressure or explicit failure before claiming strict linearizability under load. |
| Linearizable-read response bytes | Shape | `LIN_READ_BUF` | modules/app/kv_request_router/mod.rs | 300 | Oversize read state cannot use the linearizable fast path. |
| Held migration operations | Capacity | `HOLD_SLOTS` | modules/app/kv_request_router/mod.rs | 48 | Rejects busy when migration hold space is exhausted. |
| Migration help operations | Capacity | `HELP_SLOTS` | modules/app/kv_request_router/mod.rs | 8 | Defers help; increases convergence latency. |
| Migration help key | Shape | `HELP_KEY_MAX` | modules/app/kv_request_router/mod.rs | 64 | Cannot represent larger keys on this path despite the store accepting user keys through 244 bytes. |
| Encoded range-map parameter | Shape | `RANGE_MAP_PARAM_MAX` | modules/app/kv_request_router/mod.rs | 2048 | Rejects an oversized map update. |
| Tracked ranges for router metrics | Observability | `MAX_TRACKED_RANGES` | modules/app/kv_request_router/mod.rs | 8 | Additional ranges still route but lose per-range telemetry; this is not a data-plane topology limit. |
| Hot-key telemetry slots | Observability | `HOT_KEY_SLOTS` | modules/app/kv_request_router/mod.rs | 4 | Evicts/aggregates observations; requests are unaffected. |
| Lifecycle ranges per operation | Shape | `MAX_LIFECYCLE_RANGES` | modules/common/range_lifecycle.rs | 2 | Bounds split/merge lifecycle payloads; larger changes must be decomposed. |
| Nodes in a relocation survey | Topology | `MAX_NODES` | modules/common/placement.rs | 32 | Distinct leaseholder nodes beyond the table are left out of the load tally; an untallied node cannot be recommended as busiest or quietest. |
| Rebalancer range-map parameter | Shape | `RANGE_MAP_PARAM_MAX` | modules/app/rebalancer/mod.rs | 2048 | Rejects an oversized encoded map. |
| Elastic-split span bound | Shape | `KEY_MAX` | modules/app/elastic_split_driver/mod.rs | 256 | Rejects a longer span boundary key. |
| Elastic-split cutover map | Shape | `MAP_MAX` | modules/app/elastic_split_driver/mod.rs | 2048 | Rejects an oversized encoded cutover map. |
| Courier frame | Shape | `FRAME_MAX` | modules/app/span_courier/mod.rs | 8192 | A larger envelope cannot cross; the stream drops whole and the downstream install aborts on the gap (fail closed, counted). |
| Courier pre-dial pending bytes | Capacity | `PEND_BUF` | modules/app/span_courier/mod.rs | 16384 | Overflow before the dial completes drops the pending stream whole; the install aborts on the gap. |
| Courier reassembly buffer | Capacity | `RASM_BUF` | modules/app/span_courier/mod.rs | — | Derived as `2 * FRAME_MAX` (16384). Overflow drops the inbound stream whole; the install aborts on the gap. |
| Dial authority | Shape | `AUTHORITY_MAX` | modules/common/authority.rs | 64 | A longer `host[:port]` refuses construction of the connector; a name is never truncated into a different name. |
| Transaction participants | Topology | `MAX_PARTICIPANTS` | modules/common/txn.rs | 8 | Rejects a wider transaction. |
| Transaction operand | Shape | `OPERAND_MAX` | modules/app/txn_coordinator/mod.rs | 64 | Rejects an oversized coordinator operand. |
| Timestamp-allocation holds | Capacity | `HOLD_CAPACITY` | modules/app/timestamp_allocator/mod.rs | 32 | Backpressures/defer allocation while held work drains. |

`TxnLimits::DEFAULT` in `modules/common/txn.rs` is part of the public policy even
though it is represented as fields rather than independent constants: 1024
keys, 1 MiB total bytes, 128 read spans, 8 participants, 30 s lifetime, 1 s
heartbeat, 4096 intents per range, 8 retries, and 4 priority pushes. Deployments
must expose any overrides and keep them consistent at every participant.

## Watches, leases, retention, backup, and CDC

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Watches | Capacity | `MAX_WATCHES` | modules/common/watch_hub.rs | 256 | Rejects watch registration. |
| Watch key | Shape | `WATCH_KEY_MAX` | modules/common/watch_hub.rs | 96 | Rejects a larger key; narrower than the core 244-byte user-key ceiling. |
| Leases | Capacity | `MAX_LEASES` | modules/app/lease_manager/mod.rs | 256 | Rejects lease creation. |
| TTL scheduling queue | Capacity | `QUEUE_CAP` | modules/app/ttl_scheduler/mod.rs | 1024 | Rejects/backpressures another scheduled expiry; callers must not accept a lease whose expiry cannot be represented. |
| Pending watch plans | Capacity | `MAX_PENDING_PLANS` | modules/app/watch_fanout/mod.rs | 16 | Backpressures/defer fan-out planning. |
| Replay window bytes | Capacity | `WINDOW_BYTES` | modules/app/watch_fanout/mod.rs | 65536 | A backfill larger than this cannot be sent in revision order: refused as compacted (the client relists), never sent out of order or cut. |
| Replay window frames | Capacity | `WINDOW_FRAMES` | modules/app/watch_fanout/mod.rs | 256 | As `WINDOW_BYTES`. |
| Replay replaced-version bytes | Capacity | `PREV_BYTES` | modules/app/watch_fanout/mod.rs | 65536 | The versions a backfill's events replaced that lie below its window; past it the backfill is refused as compacted (the client relists), never sent without its prev_kv. |
| Watch event | Shape | `WATCH_EVENT_MAX` | modules/common/wire.rs | 8704 | The largest MSG_WATCH_EVENT payload (a key, a 4 KiB value and the 4 KiB value it replaced, with metadata); every stage of the watch path holds one. |
| Replicated proposal body | Shape | `CONSENSUS_PROPOSAL_BODY_MAX` | modules/common/wire.rs | 2040 | One clustor log entry body less the gateway's correlation tag; a larger replicated write is refused as too large in each protocol's own words (etcd "request is too large", memcached "SERVER_ERROR object too large for cache"). |
| Channel frame payload | Shape | `FRAME_PAYLOAD_MAX` | modules/common/wire.rs | 8189 | One channel frame (the SDK's CHANNEL_BUFFER_SIZE, a cross-core slot) less the envelope header; a message past it crosses as MSG_PART parts. |
| Message part bytes | Shape | `PART_BYTES_MAX` | modules/common/wire.rs | — | Derived: one frame less a part's head; a logical message's size is its receiver's policy, not this. |
| Retention consumers | Capacity | `MAX_SOURCES` | modules/common/compaction_floor.rs | 4 | Cannot track another independent retention source safely. |
| Retention key/partition groups | Capacity | `MAX_KPG` | modules/common/compaction_floor.rs | 32 | Refuses/holds floor advancement rather than compacting required history. |
| Retention claims | Capacity | `MAX_RETENTION_CLAIMS` | modules/common/db_ops.rs | 64 | Rejects another claim. |
| Backup ranges | Topology | `MAX_BACKUP_RANGES` | modules/common/db_ops.rs | — | Derived from `MAX_RANGES` (64); larger maps require multiple backup coordinators. |
| Successor cursors | Capacity | `MAX_SUCCESSOR_CURSORS` | modules/common/db_ops.rs | 2 | Rejects another concurrent successor scan. |
| Backup key | Shape | `KEY_MAX` | modules/app/backup_coordinator/mod.rs | 128 | Rejects larger keys on backup even when storage accepts them. |
| Backup page buffer | Capacity | `PAGE_BUF` | modules/app/backup_coordinator/mod.rs | 3584 | Splits/defer pages; a single unrepresentable item fails explicitly. |
| Unacknowledged CDC batches | Capacity | `UNACKED_CAP` | modules/common/cdc_feed.rs | 16 | Backpressures the feed until acknowledgement. |
| CDC key | Shape | `KEY_MAX` | modules/common/cdc_feed.rs | 256 | Rejects malformed/oversized events. |
| CDC wire key | Shape | `CDC_MAX_KEY_LEN` | modules/common/cdc_wire.rs | 2584 | Any key the store holds; encoder/decoder reject rather than truncate. |
| Chunk | Shape | `CHUNK_MAX` | modules/common/kv_chunks.rs | 1970 | Derived (a contract test holds it): the largest payload one replicated `KV_OP_PUT_CHUNK` carries — the 2040-byte proposal body less the entry tag, the 35-byte command head and the op's hash and length. Nothing that travels is larger. |
| Large values per request | Shape | `MAX_LARGE_PER_REQUEST` | modules/common/kv_large.rs | 64 | A TXN with more large values is refused before any chunk is sent; one document insert batch is at most `doc_exec::MAX_INSERT_DOCS` (64). |
| Chunk request | Shape | `CHUNK_REQUEST_MAX` | modules/common/kv_large.rs | — | Derived: a chunk behind its hash and length. |
| Large values per answer | Shape | `MAX_LARGE_PER_REPLY` | modules/common/kv_large.rs | 64 | Values one answer expands, each reserved at its place before it is fetched; a page holding more is cut (partial), any other answer is refused too large. |
| etcd Txn nesting | Semantic | `ETCD_TXN_DEPTH_MAX` | modules/common/types.rs | 4 | Txns nested below the outermost; the store and the large-value layer refuse deeper ones alike. |
| Chunk sweep budget | Shape | `SWEEP_BUDGET_MAX` | modules/common/kv_store.rs | 64 | Queue entries one replicated sweep takes, so one command's apply stays bounded; the proposer sweeps again while more are due. |
| Data chunk | Shape | `DATA_MAX` | modules/common/kv_chunks.rs | — | Derived: `CHUNK_MAX` less the chunk's kind byte; a value within it is stored inline. |
| Chunk tree depth | Semantic | `MAX_DEPTH` | modules/common/kv_chunks.rs | 9 | Node levels any u64 total needs at 61-way fanout; a producer and a reader hold one node per level. Value size itself is each protocol's declared object policy, not this. |
| CDC wire value | Shape | `CDC_MAX_VALUE_LEN` | modules/common/cdc_wire.rs | 4096 | Encoder/decoder reject rather than truncate. |

## Native and protocol surfaces

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Native frame payload | Shape | `DATA_FRAME_MAX_PAYLOAD` | modules/common/data_surface.rs | 8192 | Rejects an oversized frame. |
| Pushdown key | Shape | `MAX_PUSHDOWN_KEY_LEN` | modules/common/data_surface.rs | 64 | Rejects a longer key on pushdown paths. |
| Pushdown prefix | Shape | `MAX_PUSHDOWN_PREFIX_LEN` | modules/common/data_surface.rs | 32 | Rejects a longer prefix. |
| Pushdown prefixes | Shape | `MAX_PUSHDOWN_PREFIXES` | modules/common/data_surface.rs | 4 | Rejects a more complex request. |
| Pushdown aggregates | Shape | `MAX_PUSHDOWN_AGGREGATES` | modules/common/data_surface.rs | 4 | Rejects a more complex request. |
| Placement voters | Topology | `MAX_VOTERS` | modules/common/data_surface.rs | — | Typed value 5; must agree with `MAX_REPLICAS`. |
| Native server connections | Capacity | `MAX_CONNS` | modules/app/lattice_data_anchor/mod.rs | 16 | Drops/refuses excess accepts. |
| Native server requests | Capacity | `MAX_INFLIGHT` | modules/app/lattice_data_anchor/mod.rs | 128 | Rejects busy. |
| Native client requests | Capacity | `MAX_INFLIGHT` | modules/app/lattice_data_client/mod.rs | 128 | Backpressures/rejects locally. |
| Redis connections | Capacity | `MAX_CONNS` | modules/app/redis_edge_anchor/mod.rs | 64 | Drops/refuses excess accepts. |
| Redis queued commands per connection | Capacity | `MAX_QUEUED` | modules/app/redis_edge_anchor/mod.rs | 32 | Stops reading/backpressures the connection. |
| Redis subscriptions per connection | Capacity | `MAX_SUBS` | modules/app/redis_edge_anchor/mod.rs | 16 | Rejects another subscription. |
| Memcached stream connections | Capacity | `MAX_CONNS` | modules/app/memcached_stream_anchor/mod.rs | 64 | Drops/refuses excess accepts. |
| Memcached stream requests | Capacity | `MAX_INFLIGHT` | modules/app/memcached_stream_anchor/mod.rs | 256 | Rejects busy. |
| Memcached datagram requests | Capacity | `MAX_INFLIGHT` | modules/app/memcached_datagram_anchor/mod.rs | 512 | Drops/rejects when the request table is full. |
| Memcached command tokens | Shape | `MAX_TOKENS` | modules/common/memcached_codec.rs | 32 | Rejects a command with more tokens. |
| etcd connections | Capacity | `MAX_CONNS` | modules/app/etcd_edge_anchor/mod.rs | 64 | Drops/refuses excess accepts. |
| etcd streams per connection | Capacity | `MAX_STREAMS_PER_CONN` | modules/app/etcd_edge_anchor/mod.rs | 8 | Refuses another stream. |
| etcd stream body | Shape | `STREAM_BODY_MAX` | modules/app/etcd_edge_anchor/mod.rs | 4608 | A larger request is refused as etcd refuses one ("request is too large"); the connection stays up. |
| etcd watches per stream | Capacity | `WATCHES_PER_STREAM` | modules/app/etcd_edge_anchor/mod.rs | 8 | Refuses another watch on the stream (created and canceled at once). |
| ended leases awaiting key deletion | Capacity | `LEASE_END_QUEUE` | modules/app/etcd_edge_anchor/mod.rs | 32 | Leaves further `MSG_LEASE_REVOKE`s queued on the channel until a deletion completes. |
| etcd alarms held at once | Capacity | `ALARMS_MAX` | modules/app/etcd_edge_anchor/mod.rs | 4 | A further ACTIVATE is answered but not held. |
| etcd response message | Capacity | `RESP_OUT_MAX` | modules/app/etcd_edge_anchor/mod.rs | 16000 | One encoded gRPC response in one DATA frame; the store's answer (at most 8 KiB) always fits. |
| etcd auth tokens held | Capacity | `TOKENS_MAX` | modules/app/etcd_edge_anchor/mod.rs | 64 | A new token replaces the one nearest to lapsing. |
| etcd auth token length | Shape | `TOKEN_MAX` | modules/app/etcd_edge_anchor/mod.rs | 32 | A longer `token`/`authorization` value cannot be one of ours: the request is answered as an invalid token. |
| etcd auth change staged | Capacity | `AUTH_CHANGES_MAX` | modules/app/etcd_edge_anchor/mod.rs | 3072 | An auth change touching more records is refused (ResourceExhausted). |
| etcd users | Capacity | `USERS_MAX` | modules/common/etcd_auth.rs | 32 | UserAdd past it is refused (ResourceExhausted). |
| etcd roles | Capacity | `ROLES_MAX` | modules/common/etcd_auth.rs | 32 | RoleAdd past it is refused (ResourceExhausted). |
| etcd roles per user | Capacity | `USER_ROLES_MAX` | modules/common/etcd_auth.rs | 8 | UserGrantRole past it is refused (ResourceExhausted). |
| etcd permissions per role | Capacity | `PERMS_MAX` | modules/common/etcd_auth.rs | 8 | RoleGrantPermission past it is refused (ResourceExhausted). |
| etcd permission key | Shape | `PERM_KEY_MAX` | modules/common/etcd_auth.rs | 128 | A longer key or range end is refused (InvalidArgument). |
| etcd user/role name | Shape | `NAME_MAX` | modules/common/etcd_auth.rs | 64 | A longer name is refused. |
| KV reply / watch frame envelope (etcd anchor) | Capacity | `REPLY_BUF_SIZE` | modules/app/etcd_edge_anchor/mod.rs | 8832 | Carries the worker's largest answer (8192 bytes) behind the router's head and fence tail, or the largest watch frame (`WATCH_EVENT_MAX` behind its watch id). |
| KV reply envelope (router) | Capacity | `REPLY_BUF_SIZE` | modules/app/kv_request_router/mod.rs | 8320 | The answer path's buffer; commands stay bounded by `SCRATCH_BUF_SIZE`. |
| anchor-internal KV op body | Capacity | `BODY_MAX` | modules/app/etcd_edge_anchor/mod.rs | 4000 | An internal op (lease keys, a snapshot page, an auth change's Txn) whose body exceeds it is not sent; the request answers Unavailable. |
| watch-event journal bytes per command | Capacity | `JOURNAL_BYTES` | modules/common/kv_store.rs | 16384 | The command's events fall back to the command-derived form, without metadata or `prev_kv`. |
| etcd stored key | Shape | `STORE_KEY_MAX` | modules/app/etcd_edge_anchor/mod.rs | 2572 | Any key the store holds (its physical key less the identity; a contract test holds them together): copied whole, never clipped. |
| etcd large-value requests at once | Capacity | `LG_SESSIONS` | modules/app/etcd_edge_anchor/mod.rs | 8 | Requests carried through the large-value layer together (an upload, an expansion, a Txn that reads); one that finds all busy waits in its stream and is sent when one frees. |
| etcd watch event value | Shape | `WATCH_VALUE_MAX` | modules/app/etcd_edge_anchor/mod.rs | — | Derived: a large value (and the value it replaced) fetched for a watch event, two of them and a key fitting one encoded response; a larger one cancels the watch with the reason, never a clipped value. |
| etcd watch key + range | Shape | `WATCH_KEY_MAX` | modules/app/etcd_edge_anchor/mod.rs | 96 | Refuses the watch (created and canceled), never watches a cut key; mirrors `watch_hub::WATCH_KEY_MAX`. |
| PostgreSQL connections | Capacity | `MAX_CONNS` | modules/app/pg_edge_anchor/mod.rs | 16 | Drops/refuses excess accepts. |
| MySQL connections | Capacity | `MAX_CONNS` | modules/app/mysql_edge_anchor/mod.rs | 16 | Drops/refuses excess accepts. |
| MySQL result columns | Shape | `MAX_COLS` | modules/app/mysql_edge_anchor/mod.rs | 32 | Rejects a wider result. |
| Document connections | Capacity | `MAX_CONNS` | modules/app/doc_edge_anchor/mod.rs | 8 | Drops/refuses excess accepts. |
| Document collection/name | Shape | `NAME_MAX` | modules/app/doc_edge_anchor/mod.rs | 64 | Rejects longer names. |
| Mongo reply | Shape | `SEND_BUF` | modules/app/doc_edge_anchor/mod.rs | 24576 | The largest reply and a slot's unsent bytes; a slot that overflows is closed. |
| Mongo open cursors | Capacity | `MAX_CURSORS` | modules/app/doc_edge_anchor/mod.rs | 4 | A find past them answers its first batch and no cursor. |
| Mongo batch documents | Pacing | `BATCH_DOCS` | modules/app/doc_edge_anchor/mod.rs | 101 | The default batch; `getMore` continues. |
| BSON expansion of a tree | Semantic | `BSON_EXPANSION` | modules/app/doc_edge_anchor/mod.rs | 6 | Sizes the document budget a list asks for, so a batch always renders whole. |
| Document/BSON message | Shape | `MAX_BSON` | modules/common/doc_server_codec.rs | 4096 | Rejects a larger document payload. |
| Wide-column connections | Capacity | `MAX_CONNS` | modules/app/wide_edge_anchor/mod.rs | 8 | Drops/refuses excess accepts. |
| Wide-column result rows | Shape | `MAX_RESULT_ROWS` | modules/app/wide_edge_anchor/mod.rs | 16 | Returns `result exceeds one batch`; it does not silently truncate, but paging beyond this batch is not yet provided here. |
| CQL columns | Shape | `CQL_MAX_COLS` | modules/common/cql_server_codec.rs | 16 | Rejects a wider CQL schema/result. |
| CQL name bytes | Shape | `CQL_NAME_MAX` | modules/common/cql_server_codec.rs | 48 | Rejects a longer CQL identifier. |
| CQL clustering columns | Shape | `CQL_MAX_CLUSTERING` | modules/common/cql_server_codec.rs | 4 | Rejects a wider clustering key at the protocol boundary. |
| Model/RESP connections | Capacity | `MAX_CONNS` | modules/app/model_edge_anchor/mod.rs | 8 | Drops/refuses excess accepts. |
| Search terms | Shape | `MAX_TERMS` | modules/app/model_edge_anchor/mod.rs | 32 | Rejects a more complex query. |
| Vector dimensions | Shape | `MAX_DIMS` | modules/app/model_edge_anchor/mod.rs | 64 | Rejects a wider vector. |
| Vector top-k | Shape | `MAX_K` | modules/app/model_edge_anchor/mod.rs | 16 | Rejects a larger result request. |
| Time-series downsample buckets | Shape | `MAX_DS_BUCKETS` | modules/app/model_edge_anchor/mod.rs | 64 | Rejects a larger result request. |
| Graph vertex id | Shape | `MAX_VERTEX_ID` | modules/app/model_edge_anchor/mod.rs | 32 | Rejects a longer vertex id (`ERR vertex id length`). |
| Graph path hops | Shape | `MAX_PATH_HOPS` | modules/app/model_edge_anchor/mod.rs | — | Typed `u8`, value 16. Rejects a `GRAPH.PATH` asking for a deeper search (`ERR maxhops exceeds bound`). |
| Metrics connections | Capacity | `MAX_CONNS` | modules/app/prometheus_edge_anchor/mod.rs | 8 | Drops/refuses excess accepts. |
| Metrics series per query | Capacity | `MAX_SERIES_Q` | modules/app/prometheus_edge_anchor/mod.rs | 32 | Refuses the query whole (`too many series`, HTTP 422) rather than answer from a truncated series set. |
| Metrics query grid steps | Shape | `MAX_STEPS_Q` | modules/app/prometheus_edge_anchor/mod.rs | 128 | Rejects the query (`query grid too large`). |
| Metrics samples per series window | Capacity | `MAX_SAMPLES_Q` | modules/app/prometheus_edge_anchor/mod.rs | 2048 | Refuses the query (`window too dense`, HTTP 422) rather than fold a truncated window; narrow the range or coarsen the step. |
| Metrics query grid points | Shape | `MAX_POINTS` | modules/common/tsquery_core.rs | 1024 | Refuses the query by name (`TooManyPoints`); the anchor's step cap binds first. |
| Metrics label matchers | Shape | `MAX_MATCHERS` | modules/common/tsquery_core.rs | 16 | Refuses a query with more matchers by name. |
| Metrics grouping labels | Shape | `MAX_LABELS` | modules/common/tsquery_core.rs | 32 | Refuses a wider grouping by name. |
| Redis command arguments | Shape | `MAX_ARGS` | modules/common/redis_codec.rs | 32 | Rejects a command with more arguments. |

Connection limits are per anchor module instance, not process-wide promises.
Fluxor's target-wide TCP table (currently 256 on host/aarch64 profiles) can bind
before the sum of several anchors. Receive/send buffers also impose protocol
message limits: Redis 4096/4096, Memcached 8192/8192, etcd 4096/4096,
PostgreSQL 8192/32768, MySQL 8192/32768, and the Prometheus HTTP
anchor 16384/20480 bytes.

## Relational and model execution

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| SQL text | Shape | `MAX_SQL_LEN` | modules/common/sql_core.rs | 4096 | Rejects larger statements. |
| Columns in SQL list | Shape | `MAX_LIST_COLS` | modules/common/sql_core.rs | 32 | Rejects wider statements. |
| Rows per INSERT | Shape | `MAX_INSERT_ROWS` | modules/common/sql_core.rs | 8 | Rejects a larger statement; batching is caller-managed. |
| Predicates | Shape | `MAX_PREDICATES` | modules/common/sql_core.rs | 4 | Rejects a more complex predicate. |
| IN values | Shape | `MAX_IN_VALUES` | modules/common/sql_core.rs | 16 | Rejects a longer list. |
| Schema columns | Shape | `MAX_COLUMNS` | modules/common/relational.rs | 64 | Rejects a wider schema. |
| Key columns | Shape | `MAX_KEY_COLUMNS` | modules/common/relational.rs | 8 | Rejects a wider key. |
| Text literal bytes | Shape | `MAX_TEXT_LEN` | modules/common/relational.rs | 120 | Rejects a larger literal. |
| Row bytes | Shape | `MAX_ROW_BYTES` | modules/common/relational.rs | 4096 | Rejects a larger row. |
| Foreign-key probes | Shape | `MAX_FK_PROBES` | modules/common/relational.rs | 2 | Rejects/decomposes work requiring more probes. |
| Sort rows | Capacity | `MAX_SORT_ROWS` | modules/app/relational_executor/mod.rs | 512 | Returns result-too-large; does not silently truncate. |
| Table indexes | Shape | `MAX_TABLE_INDEXES` | modules/app/relational_executor/mod.rs | 4 | Rejects another index. |
| Executor queue depth | Capacity | `QUEUE_DEPTH` | modules/app/relational_executor/mod.rs | 4 | Returns busy. |
| Aggregate items | Shape | `MAX_AGG_ITEMS` | modules/app/relational_executor/mod.rs | 8 | Rejects a wider aggregate. |
| Aggregate groups | Capacity | `MAX_GROUPS` | modules/app/relational_executor/mod.rs | 32 | Returns result-too-large. |
| Model id bytes | Shape | `MAX_MODEL_ID_LEN` | modules/common/models.rs | 48 | Rejects a longer identifier. |
| Document index value | Shape | `MAX_DOC_INDEX_VALUE_LEN` | modules/common/models.rs | 256 | Rejects a larger indexed value. |
| Wide-column clustering columns | Shape | `MAX_CLUSTERING_COLUMNS` | modules/common/models.rs | — | Derived from `MAX_KEY_COLUMNS` (8); rejects a wider clustering key. |
| Graph vertex id | Shape | `MAX_VERTEX_ID_LEN` | modules/common/models.rs | 32 | Rejects a longer id. |
| Synchronous graph indexes | Shape | `MAX_GRAPH_SYNC_INDEXES` | modules/common/models.rs | 4 | Rejects/defer additional index maintenance. |
| Search term bytes | Shape | `MAX_TERM_LEN` | modules/common/models.rs | 48 | Rejects a longer term. |
| Document tree depth | Shape | `MAX_DEPTH` | modules/common/doc_tree.rs | 32 | Rejects a document (or edit result) nesting deeper. |
| Document path segments | Shape | `MAX_PATH_SEGMENTS` | modules/common/doc_tree.rs | 32 | Rejects a longer path. |
| Document identifier value | Shape | `MAX_IDENTIFIER_LEN` | modules/common/doc_tree.rs | 48 | Rejects a longer identifier value (`MAX_MODEL_ID_LEN`). |
| Decimal literal | Shape | `MAX_DECIMAL_TEXT` | modules/common/doc_tree.rs | 64 | Rejects a longer decimal literal. |
| Decimal exponent | Shape | `MAX_DECIMAL_EXPONENT` | modules/common/doc_tree.rs | 9999 | Rejects a literal stating a larger exponent. |
| Collection name | Shape | `MAX_NAME_LEN` | modules/common/doc_catalog.rs | 64 | Rejects a longer name. |
| Paths per index | Shape | `MAX_INDEX_PATHS` | modules/common/doc_catalog.rs | 8 | Refuses the index. |
| Index entry values | Shape | `MAX_INDEX_VALUES` | modules/common/doc_ops.rs | 256 | A document whose values do not fit its entry is refused as not indexable (`models::MAX_DOC_INDEX_VALUE_LEN`). |
| Number key digits | Semantic | `MAX_KEY_DIGITS` | modules/common/doc_tree.rs | 64 | Longer exact expansions (binary64 only) are marked, not rounded: the key stays exact. |
| Field element spelling | Shape | `ELEMENT_TEXT` | modules/common/doc_render.rs | 1024 | One field-set element spelled for a dialect (a keyed element's key as JSON, a set element's value); past it the spelling answers Overflow. Kubernetes' longest key value is a 253-byte name. |
| Mongo dropIndexes names | Capacity | `DROP_NAMES` | modules/app/doc_edge_anchor/mod.rs | 16 | `*` drops at most this many per command. |
| Collection descriptor | Shape | `MAX_DESCRIPTOR_LEN` | modules/common/doc_catalog.rs | 2048 | Rejects a larger descriptor. |
| Document request | Shape | `DOC_REQUEST_MAX` | modules/common/doc_exec.rs | 8189 | One channel frame (8192, also a cross-core slot) less the envelope header: nothing larger crosses an edge. Refuses the request as malformed. |
| Document response | Shape | `DOC_RESPONSE_MAX` | modules/common/doc_exec.rs | 8189 | One frame, as a request. A list answers what fits and a continuation; one document past it answers too-large. |
| Writer (manager) name | Shape | `MAX_MANAGER_LEN` | modules/common/doc_exec.rs | 128 | Refuses the request as malformed. |
| Scope label | Shape | `MAX_SCOPE_LEN` | modules/common/doc_exec.rs | 64 | Refuses the request as malformed; an ownership entry holds it in a byte-length (Kubernetes subresources are short names). |
| Documents per insert | Shape | `MAX_INSERT_DOCS` | modules/common/doc_exec.rs | 64 | Refuses the request as malformed. |
| Filter nesting | Shape | `MAX_FILTER_DEPTH` | modules/common/doc_exec.rs | 16 | Refuses the filter as malformed. |
| Filter nodes | Shape | `MAX_FILTER_NODES` | modules/common/doc_exec.rs | 128 | Refuses the filter as malformed. |
| Mutations per update | Shape | `MAX_MUTATIONS` | modules/common/doc_exec.rs | 32 | Refuses the request as malformed. |
| Projection paths | Shape | `MAX_PROJECTION_PATHS` | modules/common/doc_exec.rs | 32 | Refuses the projection as malformed. |
| Continuation key | Shape | `MAX_CONTINUE_KEY` | modules/common/doc_exec.rs | 256 | The executor cannot mint a longer one (keys are shorter); a longer one is refused. |
| Document executor queue | Capacity | `QUEUE_DEPTH` | modules/app/document_executor/mod.rs | 4 | Answers busy. |
| Document record | Shape | `MAX_RECORD` | modules/app/document_executor/mod.rs | — | Derived: what one answer carries whole (`RESPONSE_MAX` less framing and a stamped revision); a larger document answers too-large. A record over one data chunk is stored as a chunk tree. |
| Document request | Shape | `REQUEST_MAX` | modules/app/document_executor/mod.rs | — | Derived: one channel frame (`DOC_REQUEST_MAX`); in the big build (`document_executor_big`) `MAX_DOCUMENT` and 64 KiB of framing, arriving as `wire::MSG_PART` parts. A larger request answers too-large from its first part. |
| Document answer | Shape | `RESPONSE_MAX` | modules/app/document_executor/mod.rs | — | Derived as `REQUEST_MAX`, from `DOC_RESPONSE_MAX`; an answer past one frame leaves as parts. |
| Big-build document policy | Shape | `MAX_DOCUMENT` | modules/app/document_executor/mod.rs | 1572864 | The Kubernetes object bound (etcd's 1.5 MiB), decided for the big build; its buffers are one heap arena sized from it at load. |
| Document id value | Shape | `ID_VALUE_MAX` | modules/app/document_executor/mod.rs | — | Derived: an id value is at most its key (`MAX_DOCUMENT_ID_KEY`) plus 4 bytes of framing per component (each at least 3 key bytes) and an array head, so three times the key bound. |
| Document KV key | Shape | `KEY_MAX` | modules/app/document_executor/mod.rs | — | Derived as the keyspace prefix and `DOCUMENT_KEY_MAX_LEN` (the longer of a data key and an index entry key); a longer id or shard value is refused before a key is built. |
| Document edit buffer | Shape | `DOC_BUF` | modules/app/document_executor/mod.rs | 16384 | An edit whose result does not fit answers too-large. The big build's is `MAX_RECORD` and 64 KiB of slack. |
| Document scan page | Pacing | `SCAN_PAGE` | modules/app/document_executor/mod.rs | 16 | Lists, filtered updates and drops read further pages. |
| Watch candidates | Semantic | `WATCH_CANDIDATES` | modules/app/document_executor/mod.rs | 256 | A watch answers at most the changes of this many versions (whole revisions), and says more remain. |
| Watch scan page | Pacing | `WATCH_PAGE` | modules/app/document_executor/mod.rs | 64 | A watch pass reads further pages. |
| Document-wire sessions | Capacity | `MAX_CONNS` | modules/app/doc_wire_anchor/mod.rs | 8 | Refuses excess accepts. |
| Document-wire request frame | Shape | `RECV_BUF` | modules/app/doc_wire_anchor/mod.rs | — | Derived as one `DOC_REQUEST_MAX` frame and the net frame that completes it; the anchor stops reading the network while a session lacks room for one more. A longer request streams to the executor as parts. |
| Document-wire frame | Shape | `WIRE_FRAME_MAX` | modules/app/doc_wire_anchor/mod.rs | 16777216 | MongoDB's message bound: the largest frame the wire streams; the executor refuses what its build does not take. A longer length prefix closes the session. |
| JSON document message | Shape | `JSON_MAX` | modules/app/doc_json_dialect/mod.rs | — | Derived: one channel frame; in the big build (`doc_json_dialect_big`) `MAX_DOCUMENT` and 64 KiB of the rest of the message, crossing as `wire::MSG_PART` parts. A longer request answers too-large from its first part. |
| JSON dialect document policy | Shape | `MAX_DOCUMENT` | modules/app/doc_json_dialect/mod.rs | 1572864 | The big build's: the executor big twin's bound (the decided Kubernetes object bound). |
| JSON dialect schema | Shape | `DOC_SCHEMA_MAX` | modules/app/doc_json_dialect/mod.rs | 4096 | A longer `schema` param: every request is refused (invalid) rather than served by half a schema. |
| JSON dialect list keys | Shape | `PATCH_KEYS_MAX` | modules/app/doc_json_dialect/mod.rs | 1024 | A longer `patch_keys` param: refused as the schema is. |
| JSON dialect id paths | Shape | `ID_PATHS_MAX` | modules/app/doc_json_dialect/mod.rs | 1024 | The `id_paths` param: the default paths and the collections that read theirs elsewhere (Kubernetes' cluster-scoped kinds, by name alone). Longer: refused as the schema is. |
| JSON dialect revision path | Shape | `RV_PATH_MAX` | modules/app/doc_json_dialect/mod.rs | 64 | A longer `rv_path` param: refused as the schema is. |
| JSON dialect compound id | Shape | `MAX_ID_PATHS` | modules/app/doc_json_dialect/mod.rs | 4 | An id read from more paths: every write refused (invalid). |
| JSON dialect table columns | Shape | `TABLE_MAX` | modules/app/doc_json_dialect/mod.rs | 4096 | A longer `table` param: refused as the schema is. |
| JSON dialect continuation | Shape | `CONTINUE_MAX` | modules/app/doc_json_dialect/mod.rs | 1024 | A longer continuation (as the executor stated it) is refused as malformed. |
| JSON dialect followed watches | Capacity | `MAX_SESSIONS` | modules/app/doc_json_dialect/mod.rs | 16 | A further `J_FLAG_FOLLOW` is answered busy. |
| JSON dialect followed request | Shape | `SESSION_REQ_MAX` | modules/app/doc_json_dialect/mod.rs | 1024 | A longer followed watch request is answered too-large. |
| Document-wire answers queued | Capacity | `SEND_BUF` | modules/app/doc_wire_anchor/mod.rs | — | Derived as one `DOC_RESPONSE_MAX` frame; a client that does not read its answers is closed. |
| Document-wire net chunk | Pacing | `NET_CHUNK` | modules/app/doc_wire_anchor/mod.rs | — | Derived: the net contract's `MAX_CMD_DATA`, and no more than one channel frame holds beside the connection id (`wire::FRAME_PAYLOAD_MAX` − 2); answers leave in chunks of this size. |
| Lost compare-and-set retries | Semantic | `MAX_CAS_RETRIES` | modules/app/document_executor/mod.rs | 3 | Answers conflict; the client retries. |
| Followed watches | Capacity | `MAX_FOLLOWS` | modules/app/document_executor/mod.rs | 16 | A further `FLAG_FOLLOW` is answered busy; a pull watch still serves. |
| Followed watch request | Shape | `FOLLOW_REQ_MAX` | modules/app/document_executor/mod.rs | 1024 | A longer watch request cannot be followed: answered too-large. |
| KV request payload | Shape | `KV_REQUEST_PAYLOAD_MAX` | modules/common/wire.rs | 4160 | The router carries no larger request; an executor refuses a transaction past it (a document insert batch is one) as too-large. |

The SQL surface is useful but is not yet SQLite-equivalent. Loam can compose a
single-node embedded profile around Lattice's relational primitives, but today
there is no SQLite file/API compatibility, full SQL grammar, joins, arbitrary
expressions, triggers, views, PRAGMAs, mature query planner, or equivalent ACID
and durability validation. Calling this “SQLite-style” is reasonable only for a
bounded embedded relational/KV application profile, not as a drop-in database.

## Identity and tenancy

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Principals | Capacity | `MAX_PRINCIPALS` | modules/common/auth_table.rs | 64 | Rejects another principal. |
| Principal name bytes | Shape | `PRINCIPAL_NAME_MAX` | modules/common/auth_table.rs | 32 | Rejects a longer name. |
| Credential bytes | Shape | `CRED_MAX` | modules/common/auth_table.rs | 96 | Rejects a larger credential representation. |
| Tenant quota buckets | Capacity | `MAX_TENANTS` | modules/common/quota_bucket.rs | 64 | Rejects/untracks another tenant; callers must fail closed. |
| Metrics rollup counters | Observability | `MAX_COUNTERS` | modules/common/metrics_rollup.rs | 32 | Allocation returns none, so additional protocol/op pairs are untracked; stored data is unaffected. |

These are per module instance. Multi-tenant scale needs sharding/composition;
raising them blindly increases fixed memory in every instance.

## Pacing bounds

These do not refuse a finite workload, but determine latency and throughput and
must be performance-tested: disk flush 32 records/step, disk compaction 32,
version scan 2 run blocks/step, watch replay 32, backup 24, CDC 16, index
backfill 16, range copy 16, SQL scan 64, SQL delete 32, and lease revoke 32. A pacing loop that cannot be rescheduled
is a correctness bug, not merely a performance issue.

## Composition and scaling consequences

- Single-node log mode avoids inter-node transport and consensus latency but
  retains Lattice shape/query limits. A full Clustor group adds replication and
  failover; it is not a capacity multiplier because every replica stores the
  same partition.
- Scale beyond three nodes by creating partition groups and distributing ranges
  among them. `MAX_REPLICAS=5` is the Lattice placement ceiling and Clustor
  permits seven node ids, but larger replica groups primarily add quorum cost.
  Horizontal capacity comes from more groups. The router's two partition ports
  and 64-range map are currently the first explicit control-plane constraints.
- On the replicated write path, Lattice's 4096-byte value/command aspirations
  meet Clustor's 2048-byte WAL-entry body. Unless the command is chunked above
  consensus, the smaller entry body binds first.
- Lattice's 8164-byte memory snapshot binds before Clustor's 16384-byte body.
  A 1024-key memory store cannot generally snapshot full 4096-byte values into
  that envelope, so large replicated in-memory deployments depend on WAL replay
  or need a chunked/logical snapshot design.
- Fluxor currently supplies 16 output ports per module and target-wide resource
  tables. Module composition can multiply a per-instance table, but it consumes
  ports, modules, channels, memory, and target-wide sockets; those must be sized
  as one deployment envelope.
- “Unlimited” scans/results are not present. SQL sort/group buffers, wide-row
  pages, vector top-k, time-series buckets, protocol send buffers, and client
  page sizes all impose a first binder. APIs should expose pagination/cursors or
  explicit result-too-large errors consistently.

## Known incompleteness and priorities

1. Fix linearizable-read slot exhaustion so it cannot silently fall back to a
   weaker read mode; test the full table and oversized-response cases.
2. Make the partition router/group composition scalable beyond two local ports,
   with hierarchical routing and map distribution tested beyond three nodes.
3. Reconcile replicated command/value and snapshot envelopes across Lattice and
   Clustor, preferably with chunking rather than larger fixed buffers.
4. Make every protocol expose limit discovery, pagination, retry/backpressure,
   and stable error mapping. Several anchors currently have narrower keys,
   results, or connections than the core.
5. Add deployment profiles for raw memory cache, replicated memory, single-node
   durable log, and partitioned cluster, with benchmarked capacity envelopes.
6. Define the intended Loam embedded relational contract before describing it
   as SQLite-like; add crash/transaction/compatibility suites for that contract.
7. A PromQL driver and Chronicle-based OpenTelemetry collector are future work.
   They need a time-series label model, range/instant query semantics, staleness,
   aggregation/pushdown budgets, remote-write ingestion, retention/cardinality
   controls, and visualization-facing APIs. Current model/RESP time-series
   primitives and 64-bucket result cap are foundations, not PromQL support.

## Maintenance contract

The host limit-register test checks documented literal values against source.
That is necessary but not sufficient: it proves doc-to-source consistency, not
source-to-doc completeness. Reviews must treat every new fixed table, bounded
buffer, `MAX_*`, slot/depth/capacity constant, truncation, eviction, fallback,
and target-specific resource table as a register change or an explicitly
documented pacing exemption. CI should gain a reverse source scan; until it
does, omission detection remains a review obligation.

## Module-internal bounds

These are the per-module buffers, capacities, and pacing budgets the
modules declare. They bound one module's own frames, tables, and
work-per-step rather than a deployment-visible guarantee, but the gate
checks them for the same reason: a ceiling in source and absent here is
a defect. A constant that is not a bare `usize` integer (a typed or
derived one) carries `—` and is reviewed for presence, not integer-
checked.

### Module frame, scratch, and shape bounds

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Agg Key Max | Shape | `AGG_KEY_MAX` | modules/app/relational_executor/mod.rs | 48 | Rejects an input longer than this bound. |
| Catalog Name Key Max | Shape | `CATALOG_NAME_KEY_MAX` | modules/common/relational.rs | — | Rejects an input longer than this bound. |
| Cdc Envelope Max | Shape | `CDC_ENVELOPE_MAX` | modules/common/cdc_wire.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Disk Batch Max | Shape | `DISK_BATCH_MAX` | modules/common/kv_store.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Key Max | Shape | `KEY_MAX` | modules/app/model_edge_anchor/mod.rs | 512 | Rejects an input longer than this bound. |
| Key Max | Shape | `KEY_MAX` | modules/app/prometheus_edge_anchor/mod.rs | 512 | Rejects an input longer than this bound. |
| Key Max | Shape | `KEY_MAX` | modules/app/wide_edge_anchor/mod.rs | 512 | Rejects an input longer than this bound. |
| Max Cached Name | Shape | `MAX_CACHED_NAME` | modules/app/relational_executor/mod.rs | 64 | Rejects an input longer than this bound. |
| Max Decimal Precision | Shape | `MAX_DECIMAL_PRECISION` | modules/common/relational.rs | — | Maximum NUMERIC/DECIMAL precision; a wider declaration is rejected. |
| Max Index Value Len | Shape | `MAX_INDEX_VALUE_LEN` | modules/common/db_ops.rs | 256 | Rejects an input longer than this bound. |
| Max Kv Key | Shape | `MAX_KV_KEY` | modules/app/relational_executor/mod.rs | — | Rejects an input longer than this bound. |
| Max Name Len | Shape | `MAX_NAME_LEN` | modules/common/relational.rs | 64 | Rejects an input longer than this bound. |
| Max Ordered Value Len | Shape | `MAX_ORDERED_VALUE_LEN` | modules/common/relational.rs | — | Rejects an input longer than this bound. |
| Max Plain Value Len | Shape | `MAX_PLAIN_VALUE_LEN` | modules/common/relational.rs | — | Rejects an input longer than this bound. |
| Max Primary Key Len | Shape | `MAX_PRIMARY_KEY_LEN` | modules/common/db_ops.rs | — | Rejects an input longer than this bound. |
| Max Probe Key Len | Shape | `MAX_PROBE_KEY_LEN` | modules/common/relational.rs | — | Rejects an input longer than this bound. |
| Max Record Len | Shape | `MAX_RECORD_LEN` | modules/common/disk_store.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Max Split Key Len | Shape | `MAX_SPLIT_KEY_LEN` | modules/common/range_lifecycle.rs | — | Rejects an input longer than this bound. |
| Max Wire Version | Shape | `MAX_WIRE_VERSION` | modules/common/doc_server_codec.rs | — | The highest Mongo wire protocol version the codec advertises and accepts. |
| Password Max | Shape | `PASSWORD_MAX` | modules/app/mysql_edge_anchor/mod.rs | 64 | Rejects an input longer than this bound. |
| Password Max | Shape | `PASSWORD_MAX` | modules/app/pg_edge_anchor/mod.rs | 64 | Rejects an input longer than this bound. |
| Recv Buf Size | Shape | `RECV_BUF_SIZE` | modules/app/etcd_edge_anchor/mod.rs | 20489 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Recv Buf Size | Shape | `RECV_BUF_SIZE` | modules/app/lattice_data_anchor/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Recv Buf Size | Shape | `RECV_BUF_SIZE` | modules/app/lattice_data_client/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Recv Buf Size | Shape | `RECV_BUF_SIZE` | modules/app/memcached_stream_anchor/mod.rs | 8192 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Recv Buf Size | Shape | `RECV_BUF_SIZE` | modules/app/redis_edge_anchor/mod.rs | 4096 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Requirepass Max | Shape | `REQUIREPASS_MAX` | modules/app/redis_edge_anchor/mod.rs | 128 | Rejects an input longer than this bound. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/etcd_edge_anchor/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/kv_request_router/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/lattice_data_anchor/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/lattice_data_client/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/lease_manager/mod.rs | 4096 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/memcached_stream_anchor/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/redis_edge_anchor/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/ttl_scheduler/mod.rs | 4096 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Scratch Buf Size | Shape | `SCRATCH_BUF_SIZE` | modules/app/watch_fanout/mod.rs | 8832 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Send Buf Size | Shape | `SEND_BUF_SIZE` | modules/app/etcd_edge_anchor/mod.rs | 32768 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Send Buf Size | Shape | `SEND_BUF_SIZE` | modules/app/lattice_data_anchor/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Send Buf Size | Shape | `SEND_BUF_SIZE` | modules/app/memcached_stream_anchor/mod.rs | 8192 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Send Buf Size | Shape | `SEND_BUF_SIZE` | modules/app/redis_edge_anchor/mod.rs | 4096 | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Store Id Max | Shape | `STORE_ID_MAX` | modules/common/fs_run_storage.rs | — | Rejects an input longer than this bound. |
| Ts Request Size | Shape | `TS_REQUEST_SIZE` | modules/app/kv_state_worker/mod.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Watch Export Max | Shape | `WATCH_EXPORT_MAX` | modules/common/watch_hub.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |
| Wide Schema Max | Shape | `WIDE_SCHEMA_MAX` | modules/common/cql_server_codec.rs | — | Per-module frame/scratch buffer; an over-budget frame is refused, never truncated. |

### Module capacities

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Agg Max | Capacity | `AGG_MAX` | modules/app/relational_executor/mod.rs | — | Rejects work beyond this count. |
| Max Cols | Capacity | `MAX_COLS` | modules/app/pg_edge_anchor/mod.rs | 32 | Columns per row the Postgres codec encodes; a wider row is refused. |
| Max Edge Plan Entries | Capacity | `MAX_EDGE_PLAN_ENTRIES` | modules/common/models.rs | — | Rejects work beyond this count. |
| Max Groups | Capacity | `MAX_GROUPS` | modules/common/tsquery_core.rs | 128 | Distinct groups a metrics query aggregates; beyond it the query is refused. |
| Max Join Rows | Capacity | `MAX_JOIN_ROWS` | modules/app/relational_executor/mod.rs | 128 | Rows materialised for a join side; a larger join is refused. |
| Max Per Reject | Capacity | `MAX_PER_REJECT` | modules/app/kv_request_router/mod.rs | 32 | Rejects work beyond this count. |
| Max Series | Capacity | `MAX_SERIES` | modules/common/tsquery_core.rs | 256 | Series a metrics query tracks; beyond it the query is refused. |
| Max Txn Slots | Capacity | `MAX_TXN_SLOTS` | modules/app/relational_executor/mod.rs | 2 | Concurrent coordinator transactions; a further BEGIN is refused. |
| Ps Max | Capacity | `PS_MAX` | modules/app/redis_edge_anchor/mod.rs | 64 | Pattern subscriptions per Redis connection; a further PSUBSCRIBE is refused. |
| Reject Body Max | Capacity | `REJECT_BODY_MAX` | modules/app/kv_request_router/mod.rs | 4 | Rejects work beyond this count. |
| Scan Cursor Provider Max | Capacity | `SCAN_CURSOR_PROVIDER_MAX` | modules/common/partition_map.rs | — | Rejects work beyond this count. |
| Txn Acc Max | Capacity | `TXN_ACC_MAX` | modules/app/relational_executor/mod.rs | 3600 | Rejects work beyond this count. |
| Txn Body Max | Capacity | `TXN_BODY_MAX` | modules/app/relational_executor/mod.rs | 3800 | Rejects work beyond this count. |

### Module pacing budgets

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Disk Compact Step Cap | Pacing | `DISK_COMPACT_STEP_CAP` | modules/common/kv_store.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Disk Fence Stall Budget | Pacing | `DISK_FENCE_STALL_BUDGET` | modules/app/kv_state_worker/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Disk Hard Fault Budget | Pacing | `DISK_HARD_FAULT_BUDGET` | modules/app/kv_state_worker/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Max Per Sweep | Pacing | `MAX_PER_SWEEP` | modules/app/kv_request_router/mod.rs | 32 | Bounds work per step; the held remainder is re-driven on the next tick. |
| Page Limit | Pacing | `PAGE_LIMIT` | modules/app/backup_coordinator/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Page Limit | Pacing | `PAGE_LIMIT` | modules/app/rebalancer/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Anchor Budget | Pacing | `PER_TICK_ANCHOR_BUDGET` | modules/app/etcd_edge_anchor/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Budget | Pacing | `PER_TICK_BUDGET` | modules/app/lattice_data_client/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Drain Budget | Pacing | `PER_TICK_DRAIN_BUDGET` | modules/app/kv_state_worker/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Kv Budget | Pacing | `PER_TICK_KV_BUDGET` | modules/app/lattice_data_anchor/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Kv Budget | Pacing | `PER_TICK_KV_BUDGET` | modules/app/memcached_stream_anchor/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Kv Budget | Pacing | `PER_TICK_KV_BUDGET` | modules/app/redis_edge_anchor/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Net Budget | Pacing | `PER_TICK_NET_BUDGET` | modules/app/lattice_data_anchor/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Net Budget | Pacing | `PER_TICK_NET_BUDGET` | modules/app/memcached_stream_anchor/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Net Budget | Pacing | `PER_TICK_NET_BUDGET` | modules/app/redis_edge_anchor/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Per Tick Router Budget | Pacing | `PER_TICK_ROUTER_BUDGET` | modules/app/kv_request_router/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Reap Budget | Pacing | `REAP_BUDGET` | modules/app/kv_state_worker/mod.rs | 64 | Bounds work per step; the held remainder is re-driven on the next tick. |
| Replay Page Limit | Pacing | `REPLAY_PAGE_LIMIT` | modules/app/watch_fanout/mod.rs | — | Bounds work per step; the held remainder is re-driven on the next tick. |
| Ro Stall Limit | Pacing | `RO_STALL_LIMIT` | modules/app/redis_edge_anchor/mod.rs | — | Read-only stall ticks before the anchor reconnects its link. |
| Ttl Resume Budget | Pacing | `TTL_RESUME_BUDGET` | modules/app/kv_state_worker/mod.rs | 32 | Bounds work per step; the held remainder is re-driven on the next tick. |
| Wedge Budget | Pacing | `WEDGE_BUDGET` | modules/common/cdc_feed.rs | — | Consecutive wedge retries before a feed reports STALLED. |

### Module topology bounds

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| Max Placement Replicas | Topology | `MAX_PLACEMENT_REPLICAS` | modules/common/data_surface.rs | — | Replicas a placement may name; bounded by MAX_REPLICAS. |

## Machine-checked block

The tables above carry the reasoning; prose is not parseable, so the same
ceilings are restated here in the form the `limit-register` gate reads:
`NAME | source path | right-hand side`. The right-hand side is compared
textually after whitespace normalisation, so a row records what the source
says rather than an evaluated number — `SCRATCH_BUF_SIZE - APP_SNAPSHOT_HDR`
stays as written. A `-` marks a derived or typed constant whose declaration
is required but whose value is reviewed rather than integer-checked.

Editing a constant means editing its row, and every name here also appears in
the prose above, so the readable tables and the checked list cannot diverge
into two registers.

```limit-register
MAX_KEYS | modules/common/kv_store.rs | 1024
MAX_DOCUMENT_ID_KEY | modules/common/models.rs | 1024
MAX_KEY_LEN | modules/common/kv_store.rs | 2584
MAX_VALUE_LEN | modules/common/kv_store.rs | 4096
SCRATCH_BUF_SIZE | modules/app/kv_state_worker/mod.rs | 8192
SNAPSHOT_BODY_MAX | modules/app/kv_state_worker/mod.rs | -
RESULT_BODY_MAX | modules/app/kv_state_worker/mod.rs | -
SNAPSHOT_CHUNK_MAX | modules/app/kv_state_worker/mod.rs | 4096
MAX_USER_KEY | modules/common/disk_store.rs | 2584
MAX_ENCODED_KEY | modules/common/disk_store.rs | -
MAX_VALUE_LEN | modules/common/disk_store.rs | -
MEMTABLE_MAX_ENTRIES | modules/common/disk_store.rs | 512
MEMTABLE_FLUSH_WATERMARK | modules/common/disk_store.rs | 384
SCAN_STEP_BLOCKS | modules/common/disk_store.rs | 2
SCAN_STEP_RECORDS | modules/common/disk_store.rs | -
SCAN_PAGE_MAX | modules/common/disk_store.rs | 8192
VERSIONS_TRANSCODE_SLACK | modules/common/kv_store.rs | 6
HELD_FRAME_MAX | modules/app/kv_state_worker/mod.rs | 1024
CLOG_OUT_MAX | modules/app/kv_state_worker/mod.rs | 65536
IMPORT_RECORD_MAX | modules/app/kv_state_worker/mod.rs | 3149824
MAX_RUNS | modules/common/disk_store.rs | 16
MAX_MANIFEST_FILES | modules/common/disk_store.rs | 8
MAX_LISTED_RUNS | modules/common/disk_store.rs | 1024
SLOTS | modules/common/fd_cache.rs | 5
PATH_MAX | modules/common/fs_run_storage.rs | 145
DIR_MAX | modules/common/fs_run_storage.rs | 128
MAX_COMMAND_BYTES | modules/common/replica_facade.rs | 4096
MAX_RESPONSE_BYTES | modules/common/replica_facade.rs | 4096
MAX_WATCH_EVENT_BYTES | modules/common/replica_facade.rs | 4096
SLOT_COUNT | modules/common/partition_map.rs | 1024
MAX_KEY_BOUND_LEN | modules/common/partition_map.rs | 256
MAX_REPLICAS | modules/common/partition_map.rs | 5
MAX_RANGES | modules/common/partition_map.rs | 64
MAX_PARTITION_PORTS | modules/app/kv_request_router/mod.rs | 2
MAX_LOCAL_PARTITIONS | modules/app/partition_demux/mod.rs | 8
MAX_INFLIGHT | modules/app/kv_request_router/mod.rs | 256
KV_STASH_SLOTS | modules/app/kv_request_router/mod.rs | 4
KV_STASH_FRAME | modules/app/kv_request_router/mod.rs | -
LIN_READ_SLOTS | modules/app/kv_request_router/mod.rs | 32
LIN_READ_BUF | modules/app/kv_request_router/mod.rs | 300
HOLD_SLOTS | modules/app/kv_request_router/mod.rs | 48
HELP_SLOTS | modules/app/kv_request_router/mod.rs | 8
HELP_KEY_MAX | modules/app/kv_request_router/mod.rs | 64
RANGE_MAP_PARAM_MAX | modules/app/kv_request_router/mod.rs | 2048
MAX_TRACKED_RANGES | modules/app/kv_request_router/mod.rs | 8
HOT_KEY_SLOTS | modules/app/kv_request_router/mod.rs | 4
MAX_LIFECYCLE_RANGES | modules/common/range_lifecycle.rs | 2
MAX_NODES | modules/common/placement.rs | 32
RANGE_MAP_PARAM_MAX | modules/app/rebalancer/mod.rs | 2048
KEY_MAX | modules/app/elastic_split_driver/mod.rs | 256
MAP_MAX | modules/app/elastic_split_driver/mod.rs | 2048
FRAME_MAX | modules/app/span_courier/mod.rs | 8192
PEND_BUF | modules/app/span_courier/mod.rs | 16384
RASM_BUF | modules/app/span_courier/mod.rs | -
AUTHORITY_MAX | modules/common/authority.rs | 64
MAX_PARTICIPANTS | modules/common/txn.rs | 8
OPERAND_MAX | modules/app/txn_coordinator/mod.rs | 64
HOLD_CAPACITY | modules/app/timestamp_allocator/mod.rs | 32
MAX_WATCHES | modules/common/watch_hub.rs | 256
WATCH_KEY_MAX | modules/common/watch_hub.rs | 96
MAX_LEASES | modules/app/lease_manager/mod.rs | 256
QUEUE_CAP | modules/app/ttl_scheduler/mod.rs | 1024
MAX_PENDING_PLANS | modules/app/watch_fanout/mod.rs | 16
WINDOW_BYTES | modules/app/watch_fanout/mod.rs | 65536
PREV_BYTES | modules/app/watch_fanout/mod.rs | 65536
WATCH_EVENT_MAX | modules/common/wire.rs | 8704
FRAME_PAYLOAD_MAX | modules/common/wire.rs | 8189
PART_BYTES_MAX | modules/common/wire.rs | -
CONSENSUS_PROPOSAL_BODY_MAX | modules/common/wire.rs | 2040
WINDOW_FRAMES | modules/app/watch_fanout/mod.rs | 256
MAX_SOURCES | modules/common/compaction_floor.rs | 4
MAX_KPG | modules/common/compaction_floor.rs | 32
MAX_RETENTION_CLAIMS | modules/common/db_ops.rs | 64
MAX_BACKUP_RANGES | modules/common/db_ops.rs | -
MAX_SUCCESSOR_CURSORS | modules/common/db_ops.rs | 2
KEY_MAX | modules/app/backup_coordinator/mod.rs | 128
PAGE_BUF | modules/app/backup_coordinator/mod.rs | 3584
UNACKED_CAP | modules/common/cdc_feed.rs | 16
KEY_MAX | modules/common/cdc_feed.rs | 256
CDC_MAX_KEY_LEN | modules/common/cdc_wire.rs | 2584
CHUNK_MAX | modules/common/kv_chunks.rs | 1970
DATA_MAX | modules/common/kv_chunks.rs | -
MAX_LARGE_PER_REQUEST | modules/common/kv_large.rs | 64
CHUNK_REQUEST_MAX | modules/common/kv_large.rs | -
MAX_LARGE_PER_REPLY | modules/common/kv_large.rs | 64
ETCD_TXN_DEPTH_MAX | modules/common/types.rs | 4
SWEEP_BUDGET_MAX | modules/common/kv_store.rs | 64
MAX_DEPTH | modules/common/kv_chunks.rs | 9
CDC_MAX_VALUE_LEN | modules/common/cdc_wire.rs | 4096
DATA_FRAME_MAX_PAYLOAD | modules/common/data_surface.rs | 8192
MAX_PUSHDOWN_KEY_LEN | modules/common/data_surface.rs | 64
MAX_PUSHDOWN_PREFIX_LEN | modules/common/data_surface.rs | 32
MAX_PUSHDOWN_PREFIXES | modules/common/data_surface.rs | 4
MAX_PUSHDOWN_AGGREGATES | modules/common/data_surface.rs | 4
MAX_VOTERS | modules/common/data_surface.rs | -
MAX_CONNS | modules/app/lattice_data_anchor/mod.rs | 16
MAX_INFLIGHT | modules/app/lattice_data_anchor/mod.rs | 128
MAX_INFLIGHT | modules/app/lattice_data_client/mod.rs | 128
MAX_CONNS | modules/app/redis_edge_anchor/mod.rs | 64
MAX_QUEUED | modules/app/redis_edge_anchor/mod.rs | 32
MAX_SUBS | modules/app/redis_edge_anchor/mod.rs | 16
MAX_CONNS | modules/app/memcached_stream_anchor/mod.rs | 64
MAX_INFLIGHT | modules/app/memcached_stream_anchor/mod.rs | 256
MAX_INFLIGHT | modules/app/memcached_datagram_anchor/mod.rs | 512
MAX_TOKENS | modules/common/memcached_codec.rs | 32
MAX_CONNS | modules/app/etcd_edge_anchor/mod.rs | 64
MAX_STREAMS_PER_CONN | modules/app/etcd_edge_anchor/mod.rs | 8
STREAM_BODY_MAX | modules/app/etcd_edge_anchor/mod.rs | 4608
WATCHES_PER_STREAM | modules/app/etcd_edge_anchor/mod.rs | 8
LEASE_END_QUEUE | modules/app/etcd_edge_anchor/mod.rs | 32
ALARMS_MAX | modules/app/etcd_edge_anchor/mod.rs | 4
RESP_OUT_MAX | modules/app/etcd_edge_anchor/mod.rs | 16000
TOKENS_MAX | modules/app/etcd_edge_anchor/mod.rs | 64
TOKEN_MAX | modules/app/etcd_edge_anchor/mod.rs | 32
AUTH_CHANGES_MAX | modules/app/etcd_edge_anchor/mod.rs | 3072
USERS_MAX | modules/common/etcd_auth.rs | 32
ROLES_MAX | modules/common/etcd_auth.rs | 32
USER_ROLES_MAX | modules/common/etcd_auth.rs | 8
PERMS_MAX | modules/common/etcd_auth.rs | 8
PERM_KEY_MAX | modules/common/etcd_auth.rs | 128
NAME_MAX | modules/common/etcd_auth.rs | 64
REPLY_BUF_SIZE | modules/app/etcd_edge_anchor/mod.rs | 8832
REPLY_BUF_SIZE | modules/app/kv_request_router/mod.rs | 8320
BODY_MAX | modules/app/etcd_edge_anchor/mod.rs | 4000
JOURNAL_BYTES | modules/common/kv_store.rs | 16384
STORE_KEY_MAX | modules/app/etcd_edge_anchor/mod.rs | 2572
LG_SESSIONS | modules/app/etcd_edge_anchor/mod.rs | 8
WATCH_VALUE_MAX | modules/app/etcd_edge_anchor/mod.rs | -
WATCH_KEY_MAX | modules/app/etcd_edge_anchor/mod.rs | 96
MAX_CONNS | modules/app/pg_edge_anchor/mod.rs | 16
MAX_CONNS | modules/app/mysql_edge_anchor/mod.rs | 16
MAX_COLS | modules/app/mysql_edge_anchor/mod.rs | 32
MAX_CONNS | modules/app/doc_edge_anchor/mod.rs | 8
NAME_MAX | modules/app/doc_edge_anchor/mod.rs | 64
SEND_BUF | modules/app/doc_edge_anchor/mod.rs | 24576
MAX_CURSORS | modules/app/doc_edge_anchor/mod.rs | 4
BATCH_DOCS | modules/app/doc_edge_anchor/mod.rs | 101
BSON_EXPANSION | modules/app/doc_edge_anchor/mod.rs | 6
MAX_BSON | modules/common/doc_server_codec.rs | 4096
MAX_CONNS | modules/app/wide_edge_anchor/mod.rs | 8
MAX_RESULT_ROWS | modules/app/wide_edge_anchor/mod.rs | 16
CQL_MAX_COLS | modules/common/cql_server_codec.rs | 16
CQL_NAME_MAX | modules/common/cql_server_codec.rs | 48
CQL_MAX_CLUSTERING | modules/common/cql_server_codec.rs | 4
MAX_CONNS | modules/app/model_edge_anchor/mod.rs | 8
MAX_TERMS | modules/app/model_edge_anchor/mod.rs | 32
MAX_DIMS | modules/app/model_edge_anchor/mod.rs | 64
MAX_K | modules/app/model_edge_anchor/mod.rs | 16
MAX_DS_BUCKETS | modules/app/model_edge_anchor/mod.rs | 64
MAX_VERTEX_ID | modules/app/model_edge_anchor/mod.rs | 32
MAX_PATH_HOPS | modules/app/model_edge_anchor/mod.rs | -
MAX_CONNS | modules/app/prometheus_edge_anchor/mod.rs | 8
MAX_SERIES_Q | modules/app/prometheus_edge_anchor/mod.rs | 32
MAX_STEPS_Q | modules/app/prometheus_edge_anchor/mod.rs | 128
MAX_SAMPLES_Q | modules/app/prometheus_edge_anchor/mod.rs | 2048
MAX_POINTS | modules/common/tsquery_core.rs | 1024
MAX_MATCHERS | modules/common/tsquery_core.rs | 16
MAX_LABELS | modules/common/tsquery_core.rs | 32
MAX_ARGS | modules/common/redis_codec.rs | 32
MAX_SQL_LEN | modules/common/sql_core.rs | 4096
MAX_LIST_COLS | modules/common/sql_core.rs | 32
MAX_INSERT_ROWS | modules/common/sql_core.rs | 8
MAX_PREDICATES | modules/common/sql_core.rs | 4
MAX_IN_VALUES | modules/common/sql_core.rs | 16
MAX_COLUMNS | modules/common/relational.rs | 64
MAX_KEY_COLUMNS | modules/common/relational.rs | 8
MAX_TEXT_LEN | modules/common/relational.rs | 120
MAX_ROW_BYTES | modules/common/relational.rs | 4096
MAX_FK_PROBES | modules/common/relational.rs | 2
MAX_SORT_ROWS | modules/app/relational_executor/mod.rs | 512
MAX_TABLE_INDEXES | modules/app/relational_executor/mod.rs | 4
QUEUE_DEPTH | modules/app/relational_executor/mod.rs | 4
MAX_AGG_ITEMS | modules/app/relational_executor/mod.rs | 8
MAX_GROUPS | modules/app/relational_executor/mod.rs | 32
MAX_MODEL_ID_LEN | modules/common/models.rs | 48
MAX_DOC_INDEX_VALUE_LEN | modules/common/models.rs | 256
MAX_CLUSTERING_COLUMNS | modules/common/models.rs | -
MAX_VERTEX_ID_LEN | modules/common/models.rs | 32
MAX_GRAPH_SYNC_INDEXES | modules/common/models.rs | 4
MAX_TERM_LEN | modules/common/models.rs | 48
MAX_PRINCIPALS | modules/common/auth_table.rs | 64
PRINCIPAL_NAME_MAX | modules/common/auth_table.rs | 32
CRED_MAX | modules/common/auth_table.rs | 96
MAX_TENANTS | modules/common/quota_bucket.rs | 64
MAX_COUNTERS | modules/common/metrics_rollup.rs | 32
AGG_KEY_MAX | modules/app/relational_executor/mod.rs | 48
CATALOG_NAME_KEY_MAX | modules/common/relational.rs | -
CDC_ENVELOPE_MAX | modules/common/cdc_wire.rs | -
DISK_BATCH_MAX | modules/common/kv_store.rs | -
KEY_MAX | modules/app/model_edge_anchor/mod.rs | 512
KEY_MAX | modules/app/prometheus_edge_anchor/mod.rs | 512
KEY_MAX | modules/app/wide_edge_anchor/mod.rs | 512
MAX_CACHED_NAME | modules/app/relational_executor/mod.rs | 64
MAX_DECIMAL_PRECISION | modules/common/relational.rs | -
MAX_INDEX_VALUE_LEN | modules/common/db_ops.rs | 256
MAX_KV_KEY | modules/app/relational_executor/mod.rs | -
MAX_NAME_LEN | modules/common/relational.rs | 64
MAX_ORDERED_VALUE_LEN | modules/common/relational.rs | -
MAX_PLAIN_VALUE_LEN | modules/common/relational.rs | -
MAX_PRIMARY_KEY_LEN | modules/common/db_ops.rs | -
MAX_PROBE_KEY_LEN | modules/common/relational.rs | -
MAX_RECORD_LEN | modules/common/disk_store.rs | -
MAX_SPLIT_KEY_LEN | modules/common/range_lifecycle.rs | -
MAX_WIRE_VERSION | modules/common/doc_server_codec.rs | -
PASSWORD_MAX | modules/app/mysql_edge_anchor/mod.rs | 64
PASSWORD_MAX | modules/app/pg_edge_anchor/mod.rs | 64
RECV_BUF_SIZE | modules/app/etcd_edge_anchor/mod.rs | 20489
RECV_BUF_SIZE | modules/app/lattice_data_anchor/mod.rs | -
RECV_BUF_SIZE | modules/app/lattice_data_client/mod.rs | -
RECV_BUF_SIZE | modules/app/memcached_stream_anchor/mod.rs | 8192
RECV_BUF_SIZE | modules/app/redis_edge_anchor/mod.rs | 4096
REQUIREPASS_MAX | modules/app/redis_edge_anchor/mod.rs | 128
SCRATCH_BUF_SIZE | modules/app/etcd_edge_anchor/mod.rs | -
SCRATCH_BUF_SIZE | modules/app/kv_request_router/mod.rs | -
SCRATCH_BUF_SIZE | modules/app/lattice_data_anchor/mod.rs | -
SCRATCH_BUF_SIZE | modules/app/lattice_data_client/mod.rs | -
SCRATCH_BUF_SIZE | modules/app/lease_manager/mod.rs | 4096
SCRATCH_BUF_SIZE | modules/app/memcached_stream_anchor/mod.rs | -
SCRATCH_BUF_SIZE | modules/app/redis_edge_anchor/mod.rs | -
SCRATCH_BUF_SIZE | modules/app/ttl_scheduler/mod.rs | 4096
SCRATCH_BUF_SIZE | modules/app/watch_fanout/mod.rs | 8832
SEND_BUF_SIZE | modules/app/etcd_edge_anchor/mod.rs | 32768
SEND_BUF_SIZE | modules/app/lattice_data_anchor/mod.rs | -
SEND_BUF_SIZE | modules/app/memcached_stream_anchor/mod.rs | 8192
SEND_BUF_SIZE | modules/app/redis_edge_anchor/mod.rs | 4096
STORE_ID_MAX | modules/common/fs_run_storage.rs | -
TS_REQUEST_SIZE | modules/app/kv_state_worker/mod.rs | -
WATCH_EXPORT_MAX | modules/common/watch_hub.rs | -
WIDE_SCHEMA_MAX | modules/common/cql_server_codec.rs | -
AGG_MAX | modules/app/relational_executor/mod.rs | -
MAX_COLS | modules/app/pg_edge_anchor/mod.rs | 32
MAX_EDGE_PLAN_ENTRIES | modules/common/models.rs | -
MAX_GROUPS | modules/common/tsquery_core.rs | 128
MAX_JOIN_ROWS | modules/app/relational_executor/mod.rs | 128
MAX_PER_REJECT | modules/app/kv_request_router/mod.rs | 32
MAX_SERIES | modules/common/tsquery_core.rs | 256
MAX_TXN_SLOTS | modules/app/relational_executor/mod.rs | 2
PS_MAX | modules/app/redis_edge_anchor/mod.rs | 64
REJECT_BODY_MAX | modules/app/kv_request_router/mod.rs | 4
SCAN_CURSOR_PROVIDER_MAX | modules/common/partition_map.rs | -
TXN_ACC_MAX | modules/app/relational_executor/mod.rs | 3600
TXN_BODY_MAX | modules/app/relational_executor/mod.rs | 3800
DISK_COMPACT_STEP_CAP | modules/common/kv_store.rs | -
DISK_FENCE_STALL_BUDGET | modules/app/kv_state_worker/mod.rs | -
DISK_HARD_FAULT_BUDGET | modules/app/kv_state_worker/mod.rs | -
MAX_PER_SWEEP | modules/app/kv_request_router/mod.rs | 32
PAGE_LIMIT | modules/app/backup_coordinator/mod.rs | -
PAGE_LIMIT | modules/app/rebalancer/mod.rs | -
PER_TICK_ANCHOR_BUDGET | modules/app/etcd_edge_anchor/mod.rs | -
PER_TICK_BUDGET | modules/app/lattice_data_client/mod.rs | -
PER_TICK_DRAIN_BUDGET | modules/app/kv_state_worker/mod.rs | -
PER_TICK_KV_BUDGET | modules/app/lattice_data_anchor/mod.rs | -
PER_TICK_KV_BUDGET | modules/app/memcached_stream_anchor/mod.rs | -
PER_TICK_KV_BUDGET | modules/app/redis_edge_anchor/mod.rs | -
PER_TICK_NET_BUDGET | modules/app/lattice_data_anchor/mod.rs | -
PER_TICK_NET_BUDGET | modules/app/memcached_stream_anchor/mod.rs | -
PER_TICK_NET_BUDGET | modules/app/redis_edge_anchor/mod.rs | -
PER_TICK_ROUTER_BUDGET | modules/app/kv_request_router/mod.rs | -
REAP_BUDGET | modules/app/kv_state_worker/mod.rs | 64
REPLAY_PAGE_LIMIT | modules/app/watch_fanout/mod.rs | -
RO_STALL_LIMIT | modules/app/redis_edge_anchor/mod.rs | -
TTL_RESUME_BUDGET | modules/app/kv_state_worker/mod.rs | 32
WEDGE_BUDGET | modules/common/cdc_feed.rs | -
MAX_PLACEMENT_REPLICAS | modules/common/data_surface.rs | -
MAX_DEPTH | modules/common/doc_tree.rs | 32
MAX_PATH_SEGMENTS | modules/common/doc_tree.rs | 32
MAX_IDENTIFIER_LEN | modules/common/doc_tree.rs | 48
MAX_DECIMAL_TEXT | modules/common/doc_tree.rs | 64
MAX_DECIMAL_EXPONENT | modules/common/doc_tree.rs | 9999
MAX_NAME_LEN | modules/common/doc_catalog.rs | 64
MAX_INDEX_PATHS | modules/common/doc_catalog.rs | 8
MAX_INDEX_VALUES | modules/common/doc_ops.rs | 256
MAX_KEY_DIGITS | modules/common/doc_tree.rs | 64
ELEMENT_TEXT | modules/common/doc_render.rs | 1024
DROP_NAMES | modules/app/doc_edge_anchor/mod.rs | 16
MAX_DESCRIPTOR_LEN | modules/common/doc_catalog.rs | 2048
DOC_REQUEST_MAX | modules/common/doc_exec.rs | 8189
DOC_RESPONSE_MAX | modules/common/doc_exec.rs | 8189
MAX_MANAGER_LEN | modules/common/doc_exec.rs | 128
MAX_SCOPE_LEN | modules/common/doc_exec.rs | 64
MAX_INSERT_DOCS | modules/common/doc_exec.rs | 64
MAX_FILTER_DEPTH | modules/common/doc_exec.rs | 16
MAX_FILTER_NODES | modules/common/doc_exec.rs | 128
MAX_MUTATIONS | modules/common/doc_exec.rs | 32
MAX_PROJECTION_PATHS | modules/common/doc_exec.rs | 32
MAX_CONTINUE_KEY | modules/common/doc_exec.rs | 256
QUEUE_DEPTH | modules/app/document_executor/mod.rs | 4
MAX_RECORD | modules/app/document_executor/mod.rs | -
REQUEST_MAX | modules/app/document_executor/mod.rs | -
RESPONSE_MAX | modules/app/document_executor/mod.rs | -
MAX_DOCUMENT | modules/app/document_executor/mod.rs | 1572864
ID_VALUE_MAX | modules/app/document_executor/mod.rs | -
DOC_BUF | modules/app/document_executor/mod.rs | 16384 | bounded
DOC_BUF | modules/app/document_executor/mod.rs | MAX_RECORD + 65536 | big
KEY_MAX | modules/app/document_executor/mod.rs | -
SCAN_PAGE | modules/app/document_executor/mod.rs | 16
MAX_CAS_RETRIES | modules/app/document_executor/mod.rs | 3
MAX_FOLLOWS | modules/app/document_executor/mod.rs | 16
FOLLOW_REQ_MAX | modules/app/document_executor/mod.rs | 1024
WATCH_CANDIDATES | modules/app/document_executor/mod.rs | 256
WATCH_PAGE | modules/app/document_executor/mod.rs | 64
MAX_CONNS | modules/app/doc_wire_anchor/mod.rs | 8
RECV_BUF | modules/app/doc_wire_anchor/mod.rs | -
WIRE_FRAME_MAX | modules/app/doc_wire_anchor/mod.rs | 16777216
JSON_MAX | modules/app/doc_json_dialect/mod.rs | -
MAX_DOCUMENT | modules/app/doc_json_dialect/mod.rs | 1572864
DOC_SCHEMA_MAX | modules/app/doc_json_dialect/mod.rs | 4096
PATCH_KEYS_MAX | modules/app/doc_json_dialect/mod.rs | 1024
ID_PATHS_MAX | modules/app/doc_json_dialect/mod.rs | 1024
RV_PATH_MAX | modules/app/doc_json_dialect/mod.rs | 64
MAX_ID_PATHS | modules/app/doc_json_dialect/mod.rs | 4
TABLE_MAX | modules/app/doc_json_dialect/mod.rs | 4096
CONTINUE_MAX | modules/app/doc_json_dialect/mod.rs | 1024
MAX_SESSIONS | modules/app/doc_json_dialect/mod.rs | 16
SESSION_REQ_MAX | modules/app/doc_json_dialect/mod.rs | 1024
SEND_BUF | modules/app/doc_wire_anchor/mod.rs | -
NET_CHUNK | modules/app/doc_wire_anchor/mod.rs | -
KV_REQUEST_PAYLOAD_MAX | modules/common/wire.rs | 4160
```

## Exempt names

Constants whose names read like ceilings but are not bounds: the gate's
reverse scan skips them.

```limit-register-exempt
FIELD_LIMIT | modules/common/doc_exec.rs | a request field tag (the list limit a caller states), not a bound
NUM_MAX | modules/common/doc_exec.rs | a mutation sub-operation (take the maximum), not a bound
P_WATCH_SIZE | modules/app/document_executor/mod.rs | a phase (the watch's sizing pass), not a bound
KVS_MORE_LIMIT | modules/common/types.rs | an answer's `more` code (matches lie past the request's limit), not a bound
```
