# Lattice

Lattice is a multi-protocol data store for the nanocloud products,
built on the clustor Raft replication substrate and the
[fluxor](../fluxor/) runtime. Familiar client protocols — Redis
RESP, Memcached ASCII, etcd v3 gRPC, the PostgreSQL and MySQL wire
protocols, MongoDB documents, Cassandra CQL, RESP-based
graph/search/vector/time-series models, and the Prometheus
remote-write/query HTTP API — map onto one MVCC key-value
state machine with a shared revision model, replicated and made
durable by the substrate.

There is no standalone binary: a node is a `fluxor run` over a graph
config that composes lattice's protocol anchors and KV modules with
the clustor substrate modules. The same graph runs as a single node
or a three-node cluster; only the config changes.

## Quick start

```sh
fluxor modules build --all      # build the module palette
fluxor run --replicas 1 --var REDIS_PORT=6390 - <<'EOF'
<the node config — embedded in docs/guides/running.md>
EOF
```

The deployment config pipes straight into `fluxor run` from the run
guide, which carries it inline: nothing else to fetch. A node is
ready when `GET /readyz` on its HTTP port (19090 by default) returns
200, and any Redis client can then do a durable, Raft-committed
put/get round trip against the node's Redis port.
[`docs/guides/running.md`](docs/guides/running.md) has the full
config plus bring-up, smoke checks, and a three-node cluster.

## Setup

Lattice consumes fluxor, clustor, wave, and quantum through the
local OCI store (`$FLUXOR_STORE`, default
`~/.local/share/fluxor/store`); digest pins live in `fluxor.lock`.

```sh
# one-time, per developer machine
git clone git@github.com:nanocloudio/fluxor.git ../fluxor
make -C ../fluxor install    # put the fluxor CLI launcher on PATH
make -C ../fluxor publish    # publish SDK, runtime, foundation modules

git clone git@github.com:nanocloudio/clustor.git
make -C clustor publish      # publish the substrate module palette
                             # (run inside your clustor clone)

# in lattice's checkout
fluxor modules build --all   # build; pre-flight materialises pinned
                             # artefacts from fluxor.lock
```

To pick up upstream changes: `make publish` in the upstream
checkout, then `fluxor update` here to advance `fluxor.lock`, and
commit the lockfile. When iterating on several repos at once, add
them to `~/.fluxor/workspace.toml`; workspace members resolve
`:latest` automatically and `fluxor sync` writes the resolved digests
through the lockfile.

## Architecture

Client traffic enters through a per-protocol **anchor** module
(`modules/app/*_edge_anchor`), which translates wire semantics into a
common KV request envelope. The **request router**
(`kv_request_router`) routes each request by key to a partition,
sends writes into the clustor proposal pipeline, and serves reads
either directly from local state or through a ReadIndex fence. The
**apply bridge** (`lattice_apply_bridge`) re-emits Raft-committed
entries, in commit order, to the **state worker**
(`kv_state_worker`), which hosts the deterministic MVCC state machine
(`modules/common/kv_store.rs`) in memory or on disk. In a replicated
graph, replication, WAL durability, quorum tracking, leadership, and
snapshots are the substrate's, and every observable effect derives
from committed log entries or snapshots. A single-node disk worker
with nothing ordering it keeps its own write-ahead command log
(`command_log: 1`). The state worker can also serve fluxor's own
`storage.object` and `storage.namespace` store contracts
(`kv_state_worker_store`), so a graph's connectors keep the store they
reach by contract while lattice holds it; see
[`docs/architecture/store_provider.md`](docs/architecture/store_provider.md).

The normative reference is
[`docs/architecture/specification.md`](docs/architecture/specification.md):
the data model, partitioning and routing epochs, read semantics
(LIN-BOUND), snapshots and compaction, TTL, and error mapping.

## Protocol surfaces

Each surface is an anchor module enabled in the graph config. The
command sets below are what the code dispatches; the per-protocol
guides carry the detail.

| Surface | Anchor module | Commands |
|---|---|---|
| **Redis** (RESP2) | `redis_edge_anchor` | GET, SET (`NX/XX`, `EX/PX`, `KEEPTTL`, `GET`), SETNX, GETSET, APPEND, STRLEN, EXISTS, INCR/INCRBY/DECR/DECRBY, DEL/UNLINK, KEYS, SCAN, MGET, MSET, FLUSHDB/FLUSHALL, TS.ADD/TS.RANGE, SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/PUNSUBSCRIBE/PUBLISH, PING, ECHO, QUIT, SELECT, AUTH (`requirepass`), HELLO, RESET, CLIENT, COMMAND, `LATTICE.RELOCATE`/`RELOCATING`/`RELOCATED` (operator hooks) |
| **Memcached** (ASCII) | `memcached_stream_anchor`, `memcached_datagram_anchor` | Stream: get, gets, set, add, replace, append, prepend, delete, incr, decr, flush_all, version, stats, quit. Datagram: get, set, delete |
| **etcd v3** (gRPC) | `etcd_edge_anchor` | KV (Range, Put, DeleteRange, Txn, Compact), Watch, Lease (Grant, Revoke, KeepAlive, TimeToLive, Leases), Maintenance (Status, Hash, HashKV, Alarm, Defragment, MoveLeader, Snapshot), Cluster (MemberList, MemberAdd/Remove/Update/Promote), Auth (enable/disable/status, Authenticate, users, roles) |
| **Relational SQL** | `pg_edge_anchor`, `mysql_edge_anchor` → `relational_executor` | SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE, CREATE/DROP INDEX, ALTER TABLE ADD — over the PostgreSQL and MySQL/MariaDB server wire protocols |
| **Document** | `doc_edge_anchor` | MongoDB wire: find, getMore, killCursors, insert, update, delete, create, drop, createIndexes, listIndexes, dropIndexes, listCollections, hello/ismaster/ping/buildInfo, getParameter, endSessions |
| **Wide-column** | `wide_edge_anchor` | Cassandra CQL v4: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE |
| **Models (RESP)** | `model_edge_anchor` | `GRAPH.*` (VERTEX/EDGE/IN/OUT/PATH/DELVERTEX/DELEDGE), `SEARCH.*` (INDEX/QUERY/DELETE), `VECTOR.*` (ADD/COS/SIM/SIMWHERE/ANN/GET/DEL/TAG), `TS.*` (ADD/RANGE/AGG/DOWNSAMPLE/ROLLUP/GETROLLUP/TRIM), hashes (HSET/HGET/HGETALL), lists (LPUSH/RPUSH/LRANGE/LLEN), sorted sets (ZADD/ZRANGE/ZSCORE), `FEED.READ` |
| **Metrics** (Prometheus HTTP) | `prometheus_edge_anchor` | Remote-write ingest (`/api/v1/write`), PromQL/MetricsQL `query`/`query_range`, KQL (`/api/v1/kql`), labels/series discovery |

An application in the same fluxor graph reaches the store without a wire
protocol: the in-graph exchange fronts `sql_front` and `kv_front` take the
same requests and give the same answers as the protocol clients `pg_client`
and `redis_client`, so swapping a foreign server for the embedded store is
wiring alone. The store and its fronts ship composed as the published stacks
`packaging/stack/lattice_store.yaml` (single node) and
`packaging/stack/lattice_store_group3.yaml` (a replica of a three-voter group);
see [`docs/guides/fronts.md`](docs/guides/fronts.md).

What is not implemented is listed under
[Not implemented](#not-implemented).

## Change data capture

The `cdc_pump` module streams committed changes out of a running
graph: it reads change windows through the ordinary router path,
emits one event per change to any sink module declaring the
`stream.ordered_ack.sink` capability, and advances a durable
checkpoint only past the contiguous acknowledged prefix, so delivery
is at-least-once and survives sink outages. The in-repo reference
sink is `loopback_sink`; MQTT delivery lives in quantum. The
`cdc_front` module serves the same feed as subscriptions: a requester's
`GET /cdc/<prefix hex>` is held open (`HOLD|MORE`) and sent one event per
change, in the pump's envelope, under the requester's credit, resumable
with `after: <revision>`. See
[`docs/architecture/cdc.md`](docs/architecture/cdc.md) and
[`docs/guides/fronts.md`](docs/guides/fronts.md).

## Errors

The conditions clients most often see, per surface:

| Condition | Redis | Memcached | etcd v3 |
|---|---|---|---|
| Routing epoch advanced | `-MOVED routing epoch advanced` | the command's failure reply (below) | status 14, `etcdserver: leader changed` |
| No authority for a fenced operation | `-CLUSTERDOWN no authority available for this operation` | the command's failure reply (below) | status 14, `etcdserver: request timed out` |
| Wrong value type | `-WRONGTYPE …` | `CLIENT_ERROR cannot increment or decrement non-numeric value` | — |

The Redis strings reuse the closest standard client-retry signals so
an unmodified client re-resolves routing; they are not Redis Cluster
redirection. The Memcached ASCII surface has no dedicated routing or
linearizability strings: a storage command answers `NOT_STORED`, a
get answers a miss, and delete, incr and decr answer `NOT_FOUND`.
The etcd surface answers with etcd's status and `grpc-message` text;
both conditions are UNAVAILABLE, which etcd's clients retry, and
historical reads below the compaction floor return status 11
(OUT_OF_RANGE), matching etcd's compacted error.
The full mapping is in the
[specification](docs/architecture/specification.md#error-mapping).

## Status

| Surface | State |
|---|---|
| KV core (`kv_state_worker`, `kv_store.rs`) | MVCC with per-key revisions, memory and disk state stores, snapshot export/import: working. |
| Replicated write path | Anchor → router → gateway → consensus → WAL → apply bridge → worker: working, single node and three-node. |
| Linearizable reads | ReadIndex fence path (`lin_reads`, per-request consistency byte): wired. An etcd Range is linearizable unless `serializable`; the Redis and Memcached surfaces read serializable unless the router runs `lin_reads: 1`. |
| Partitioning | Ordered key-range and hash-slot maps, routing epochs, range split/merge with cutover barriers: working. |
| Watch / lease / TTL | In-memory watch registry with revision-window replay, lease table, deterministic tick-driven expiry: working; watch and lease state is held in memory and does not survive a restart. |
| Compaction / backup | Claim-based MVCC GC floor and consistent backup/restore at a protected revision: working. |
| CDC | Ordered-ack egress with backfill, resolved watermarks, retention claims: working. |
| Multi-tenancy | The protocol anchors (Redis, Memcached, etcd, SQL) stamp tenant 0. The data surface (`lattice_data_anchor`) carries each request's tenant, database and keyspace into the key layout and refuses tenant 0. |
| Store contracts | `kv_state_worker_store` serves fluxor's `storage.object` and `storage.namespace` (reads, writes, LIST, subscriptions, paged CHANGES) on a single node or as one replica of a group: working. |
| Control-plane policy | Quota and auth-principal modules are not driven by a control-plane feed; the substrate's control plane emits a synthetic proof. `placement_advisor` measures range key counts itself and reports; with `auto_execute: 1` it issues the split it recommends. |

## Not implemented

- General SQL transactions: `BEGIN` … `COMMIT` buffers INSERTs and
  commits them as one atomic batch (two-phase through
  `txn_coordinator` when they span ranges); reads, UPDATE and DELETE
  inside a transaction are refused. Cross-partition linearizable
  ranges.
- Redis transactions: `MULTI`, `EXEC`, `DISCARD`, `WATCH` and `UNWATCH`
  are refused
- etcd election and lock services
- Redis sets and streams on any surface; hashes, lists and sorted
  sets on `redis_edge_anchor` (the model surface serves them);
  `EXPIRE`/`TTL`, `WATCH`, `EVAL`; RESP3 framing (`HELLO 3` is
  accepted but both directions stay RESP2)
- Memcached `cas`/`touch`, the binary protocol, SASL, `noreply`
- Tenant identity on connections (AUTH authenticates, it does not
  select a tenant)

## Repository layout

| Path | Contents |
|---|---|
| `modules/app/` | `no_std` PIC modules: protocol anchors, KV/router/apply modules, watch/lease/TTL, compaction, backup, CDC, range lifecycle, and outbound client connectors. `fluxor modules build` packs each into a `.fmod`. |
| `packaging/stack/` | Published stacks: lattice's store with its in-graph fronts, for an application graph to include ([`docs/guides/fronts.md`](docs/guides/fronts.md)). |
| `modules/common/` | Shared `no_std` cores: the KV state machine, codecs, partition maps, wire constants. Pulled into each app module via `#[path]`. |
| `docs/` | Reference documentation, indexed by [`docs/overview.md`](docs/overview.md): architecture (`docs/architecture/`) and guides (`docs/guides/`). |
| `tools/` | Host-side helpers: `lattice-loadgen` and `lattice-scrape` (decodes the binary `/metrics` export) in `tools/load/lattice-bench`, `lattice-store-dump` (a disk store's live keys), the CI gates in `tools/ci/`, and the end-to-end scripts in `tools/e2e/`. |
| `telemetry/` | Telemetry catalog (`telemetry/catalog.json`). |
| `wire/` | Per-protocol wire catalogs. |
| `fluxor.toml` | Project manifest for the `fluxor` CLI: identity, dependencies, project policy. |
| `Makefile` | Thin alias layer over the `fluxor` CLI; `make help` lists the targets. |

## Documentation

- [`docs/guides/running.md`](docs/guides/running.md) — bring-up and
  smoke checks.
- [`docs/overview.md`](docs/overview.md) — index of everything below.
- [`docs/architecture/`](docs/architecture/) — the specification,
  the store contracts, CDC, and the limit register.
- [`docs/guides/`](docs/guides/) — deployment, high availability,
  tuning, the per-protocol guides (Redis, Memcached, etcd,
  metrics), and the in-graph fronts and store stack.

## License

See [`LICENSE`](LICENSE).
