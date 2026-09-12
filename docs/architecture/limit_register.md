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

## Core state and storage

| Limit | Kind | Symbol | Source | Value | Exhaustion behaviour |
|---|---|---|---|---:|---|
| In-memory keys per store | Capacity | `MAX_KEYS` | modules/common/kv_store.rs | 1024 | Rejects a new key; updates to existing keys remain possible. This bounds both raw-memory mode and each replicated memory-state worker. |
| In-memory physical-key slot | Shape | `MAX_KEY_LEN` | modules/common/kv_store.rs | 256 | The store prepends a 12-byte tenant/database/keyspace identity, so the effective user-key ceiling is 244 bytes. A longer composed key is rejected. |
| In-memory value bytes | Shape | `MAX_VALUE_LEN` | modules/common/kv_store.rs | 4096 | Rejects the operation. |
| State-worker scratch | Capacity | `SCRATCH_BUF_SIZE` | modules/app/kv_state_worker/mod.rs | 8192 | Rejects an over-budget frame or memory snapshot export; the WAL remains authoritative. |
| App-snapshot body | Shape | `SNAPSHOT_BODY_MAX` | modules/app/kv_state_worker/mod.rs | — | Derived as 8164 bytes (`SCRATCH_BUF_SIZE - APP_SNAPSHOT_HDR`); this binds before Clustor's 16384-byte snapshot body. |
| Snapshot chunk | Pacing | `SNAPSHOT_CHUNK_MAX` | modules/app/kv_state_worker/mod.rs | 4096 | Streams another chunk; does not cap total disk-state size. |
| Encoded disk key | Shape | `MAX_ENCODED_KEY` | modules/common/disk_store.rs | 544 | Rejects keys whose escaped/on-disk representation cannot fit. |
| Disk value record | Shape | `MAX_VALUE_LEN` | modules/common/disk_store.rs | — | Derived as 4150 bytes to include metadata around the 4096-byte public value. |
| Memtable entries | Capacity | `MEMTABLE_MAX_ENTRIES` | modules/common/disk_store.rs | 512 | Backpressures mutation while flush catches up. |
| Memtable flush watermark | Pacing | `MEMTABLE_FLUSH_WATERMARK` | modules/common/disk_store.rs | 384 | Starts flush early; not a refusal ceiling. |
| Version-scan blocks per step | Pacing | `SCAN_STEP_BLOCKS` | modules/common/disk_store.rs | 2 | The walk pauses holding its position; the worker re-drives it next step. Bounds run-file I/O per step, not the window. |
| Version-scan records per step | Pacing | `SCAN_STEP_RECORDS` | modules/common/disk_store.rs | — | Derived as `MEMTABLE_MAX_ENTRIES` (512); bounds the memtable-resident part of a walk, which loads no blocks. |
| Parked version-scan page | Capacity | `SCAN_PAGE_MAX` | modules/common/disk_store.rs | 8192 | The reply prefix kept across a pause. The engine caps every page at this budget, so a park can never be refused; the cap therefore bounds one command's reply, not a refusal path. |
| Version-scan entry shrink | Shape | `VERSIONS_TRANSCODE_SLACK` | modules/common/kv_store.rs | 6 | The least a reply entry is smaller than its raw record; the engine sizes each store request by it, so a record the store hands over always fits the page (an overflow record is stashed against the held ordinal, so the cursor never runs ahead of the caller). |
| Held command frame | Shape | `HELD_FRAME_MAX` | modules/app/kv_state_worker/mod.rs | 1024 | A pausable command larger than this is not held; the request times out upstream and is re-issued. Only `KV_OP_SCAN_VERSIONS` pauses, and its frame is bounded well under this. |
| Immutable runs | Capacity | `MAX_RUNS` | modules/common/disk_store.rs | 16 | Backpressures until compaction frees a run slot. |
| Manifest files | Capacity | `MAX_MANIFEST_FILES` | modules/common/disk_store.rs | 8 | Bounds retained manifest generations; rotation/reclamation must keep pace. |
| Runs returned by listing | Capacity | `MAX_LISTED_RUNS` | modules/common/disk_store.rs | 64 | Rejects/flags an inventory that cannot be represented; never silently truncates authoritative run state. |
| Open run descriptors | Capacity | `SLOTS` | modules/common/fd_cache.rs | 5 | Evicts the least-recently-used descriptor; data remains on disk. This can dominate cold-read latency. |
| Filesystem store path | Shape | `PATH_MAX` | modules/common/fs_run_storage.rs | 17 | Bounds the internally encoded store/run path; deployments must stay within the store-id/path scheme. |
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
| Retention consumers | Capacity | `MAX_SOURCES` | modules/common/compaction_floor.rs | 4 | Cannot track another independent retention source safely. |
| Retention key/partition groups | Capacity | `MAX_KPG` | modules/common/compaction_floor.rs | 32 | Refuses/holds floor advancement rather than compacting required history. |
| Retention claims | Capacity | `MAX_RETENTION_CLAIMS` | modules/common/db_ops.rs | 64 | Rejects another claim. |
| Backup ranges | Topology | `MAX_BACKUP_RANGES` | modules/common/db_ops.rs | — | Derived from `MAX_RANGES` (64); larger maps require multiple backup coordinators. |
| Successor cursors | Capacity | `MAX_SUCCESSOR_CURSORS` | modules/common/db_ops.rs | 2 | Rejects another concurrent successor scan. |
| Backup key | Shape | `KEY_MAX` | modules/app/backup_coordinator/mod.rs | 128 | Rejects larger keys on backup even when storage accepts them. |
| Backup page buffer | Capacity | `PAGE_BUF` | modules/app/backup_coordinator/mod.rs | 3584 | Splits/defer pages; a single unrepresentable item fails explicitly. |
| Unacknowledged CDC batches | Capacity | `UNACKED_CAP` | modules/common/cdc_feed.rs | 16 | Backpressures the feed until acknowledgement. |
| CDC key | Shape | `KEY_MAX` | modules/common/cdc_feed.rs | 256 | Rejects malformed/oversized events. |
| CDC wire key | Shape | `CDC_MAX_KEY_LEN` | modules/common/cdc_wire.rs | 256 | Encoder/decoder reject rather than truncate. |
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
| etcd stream body | Shape | `STREAM_BODY_MAX` | modules/app/etcd_edge_anchor/mod.rs | 2048 | Rejects an oversized body. |
| PostgreSQL connections | Capacity | `MAX_CONNS` | modules/app/pg_edge_anchor/mod.rs | 16 | Drops/refuses excess accepts. |
| MySQL connections | Capacity | `MAX_CONNS` | modules/app/mysql_edge_anchor/mod.rs | 16 | Drops/refuses excess accepts. |
| MySQL result columns | Shape | `MAX_COLS` | modules/app/mysql_edge_anchor/mod.rs | 32 | Rejects a wider result. |
| Document connections | Capacity | `MAX_CONNS` | modules/app/doc_edge_anchor/mod.rs | 8 | Drops/refuses excess accepts. |
| Document collection/name | Shape | `NAME_MAX` | modules/app/doc_edge_anchor/mod.rs | 64 | Rejects longer names. |
| Document id | Shape | `ID_MAX` | modules/app/doc_edge_anchor/mod.rs | — | Derived as 129 bytes. |
| Document composite key | Shape | `KEY_MAX` | modules/app/doc_edge_anchor/mod.rs | 512 | Rejects an oversized encoded key. |
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
MAX_KEY_LEN | modules/common/kv_store.rs | 256
MAX_VALUE_LEN | modules/common/kv_store.rs | 4096
SCRATCH_BUF_SIZE | modules/app/kv_state_worker/mod.rs | 8192
SNAPSHOT_BODY_MAX | modules/app/kv_state_worker/mod.rs | -
SNAPSHOT_CHUNK_MAX | modules/app/kv_state_worker/mod.rs | 4096
MAX_ENCODED_KEY | modules/common/disk_store.rs | 544
MAX_VALUE_LEN | modules/common/disk_store.rs | -
MEMTABLE_MAX_ENTRIES | modules/common/disk_store.rs | 512
MEMTABLE_FLUSH_WATERMARK | modules/common/disk_store.rs | 384
SCAN_STEP_BLOCKS | modules/common/disk_store.rs | 2
SCAN_STEP_RECORDS | modules/common/disk_store.rs | -
SCAN_PAGE_MAX | modules/common/disk_store.rs | 8192
VERSIONS_TRANSCODE_SLACK | modules/common/kv_store.rs | 6
HELD_FRAME_MAX | modules/app/kv_state_worker/mod.rs | 1024
MAX_RUNS | modules/common/disk_store.rs | 16
MAX_MANIFEST_FILES | modules/common/disk_store.rs | 8
MAX_LISTED_RUNS | modules/common/disk_store.rs | 64
SLOTS | modules/common/fd_cache.rs | 5
PATH_MAX | modules/common/fs_run_storage.rs | 17
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
MAX_PARTICIPANTS | modules/common/txn.rs | 8
OPERAND_MAX | modules/app/txn_coordinator/mod.rs | 64
HOLD_CAPACITY | modules/app/timestamp_allocator/mod.rs | 32
MAX_WATCHES | modules/common/watch_hub.rs | 256
WATCH_KEY_MAX | modules/common/watch_hub.rs | 96
MAX_LEASES | modules/app/lease_manager/mod.rs | 256
QUEUE_CAP | modules/app/ttl_scheduler/mod.rs | 1024
MAX_PENDING_PLANS | modules/app/watch_fanout/mod.rs | 16
MAX_SOURCES | modules/common/compaction_floor.rs | 4
MAX_KPG | modules/common/compaction_floor.rs | 32
MAX_RETENTION_CLAIMS | modules/common/db_ops.rs | 64
MAX_BACKUP_RANGES | modules/common/db_ops.rs | -
MAX_SUCCESSOR_CURSORS | modules/common/db_ops.rs | 2
KEY_MAX | modules/app/backup_coordinator/mod.rs | 128
PAGE_BUF | modules/app/backup_coordinator/mod.rs | 3584
UNACKED_CAP | modules/common/cdc_feed.rs | 16
KEY_MAX | modules/common/cdc_feed.rs | 256
CDC_MAX_KEY_LEN | modules/common/cdc_wire.rs | 256
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
STREAM_BODY_MAX | modules/app/etcd_edge_anchor/mod.rs | 2048
MAX_CONNS | modules/app/pg_edge_anchor/mod.rs | 16
MAX_CONNS | modules/app/mysql_edge_anchor/mod.rs | 16
MAX_COLS | modules/app/mysql_edge_anchor/mod.rs | 32
MAX_CONNS | modules/app/doc_edge_anchor/mod.rs | 8
NAME_MAX | modules/app/doc_edge_anchor/mod.rs | 64
ID_MAX | modules/app/doc_edge_anchor/mod.rs | -
KEY_MAX | modules/app/doc_edge_anchor/mod.rs | 512
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
```
