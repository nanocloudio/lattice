# Lattice

Lattice is a multi-protocol data store for the nanocloud products,
built on the clustor Raft replication substrate and the
[fluxor](../fluxor/) runtime. Familiar client protocols — Redis
RESP, Memcached ASCII, etcd v3 gRPC, the PostgreSQL and MySQL wire
protocols, MongoDB documents, Cassandra CQL, and RESP-based
graph/search/vector/time-series models — map onto one MVCC key-value
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

To pick up new upstream changes: `make publish` in the upstream
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
(`modules/common/kv_store.rs`) in memory or on disk. Replication,
WAL durability, quorum tracking, leadership, and snapshots are the
substrate's: lattice modules never write their own durable channel,
so every observable effect derives from committed log entries or
snapshots.

The normative reference is
[`docs/architecture/specification.md`](docs/architecture/specification.md):
the data model, partitioning and routing epochs, read semantics
(LIN-BOUND), snapshots and compaction, TTL, and error mapping.

## Protocol surfaces

Each surface is an anchor module enabled in the graph config. The
command sets below are what the code dispatches; the per-protocol
guides carry the detail.

| Surface | Anchor module | Implemented today |
|---|---|---|
| **Redis** (RESP2) | `redis_edge_anchor` | GET, SET (`NX/XX`, `EX/PX`, `GET`), SETNX, GETSET, APPEND, STRLEN, EXISTS, INCR/INCRBY/DECR/DECRBY, DEL/UNLINK, KEYS, SCAN, MGET, MSET, FLUSHDB/FLUSHALL, TS.ADD/TS.RANGE, MULTI/EXEC/DISCARD, SUBSCRIBE family, PING, ECHO, QUIT, SELECT, AUTH (`requirepass`), HELLO, RESET, CLIENT, COMMAND |
| **Memcached** (ASCII) | `memcached_stream_anchor`, `memcached_datagram_anchor` | get, gets, set, add, replace, append, prepend, delete, incr, decr, flush_all, version, stats, quit |
| **etcd v3** (gRPC) | `etcd_edge_anchor` | Range, Put, DeleteRange, Watch, LeaseGrant, LeaseRevoke, LeaseKeepAlive |
| **Relational SQL** | `pg_edge_anchor`, `mysql_edge_anchor` → `relational_executor` | SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE, CREATE/DROP INDEX, ALTER TABLE ADD — over the PostgreSQL and MySQL/MariaDB server wire protocols |
| **Document** | `doc_edge_anchor` | MongoDB wire: find, insert, update, delete, hello/ismaster/ping/buildInfo |
| **Wide-column** | `wide_edge_anchor` | Cassandra CQL v4: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE |
| **Models (RESP)** | `model_edge_anchor` | `GRAPH.*` (VERTEX/EDGE/IN/OUT/DELVERTEX/DELEDGE), `SEARCH.*` (INDEX/QUERY/DELETE), `VECTOR.*` (ADD/COS/SIM/SIMWHERE/ANN/GET/DEL/TAG), `TS.*` (ADD/RANGE/AGG/DOWNSAMPLE/TRIM), `FEED.READ` |

What is not implemented is listed under
[Not implemented](#not-implemented).

## Change data capture

The `cdc_pump` module streams committed changes out of a running
graph: it reads change windows through the ordinary router path,
emits one event per change to any sink module declaring the
`stream.sink.ordered_ack` capability, and advances a durable
checkpoint only past the contiguous acknowledged prefix, so delivery
is at-least-once and survives sink outages. The in-repo reference
sink is `loopback_sink`; MQTT delivery lives in quantum. See
[`docs/architecture/cdc.md`](docs/architecture/cdc.md).

## Errors

The conditions clients most often see, per surface:

| Condition | Redis | Memcached | etcd v3 |
|---|---|---|---|
| Routing epoch advanced | `-MOVED routing epoch advanced` | `NOT_STORED` | gRPC status 13 |
| No authority for a fenced operation | `-CLUSTERDOWN no authority available for this operation` | `SERVER_ERROR backpressure` | gRPC status 13 |
| Wrong value type | `-WRONGTYPE …` | — | — |

The Redis strings reuse the closest standard client-retry signals so
an unmodified client re-resolves routing; they are not Redis Cluster
redirection. The Memcached ASCII surface has no dedicated
routing or linearizability strings. The etcd surface carries
`grpc-status` only (no `grpc-message`), and today maps both
conditions to status 13; historical reads below the compaction floor
return status 11 (OUT_OF_RANGE), matching etcd's compacted error.
The full mapping is in the
[specification](docs/architecture/specification.md#error-mapping).

## Status

| Surface | State |
|---|---|
| KV core (`kv_state_worker`, `kv_store.rs`) | MVCC with per-key revisions, memory and disk state stores, snapshot export/import: working. |
| Replicated write path | Anchor → router → gateway → consensus → WAL → apply bridge → worker: working, single node and three-node. |
| Linearizable reads | ReadIndex fence path (`lin_reads`, per-request consistency byte): wired; reads are snapshot-consistency by default. |
| Partitioning | Ordered key-range and hash-slot maps, routing epochs, range split/merge with cutover barriers: working. |
| Watch / lease / TTL | In-memory watch registry with revision-window replay, lease table, deterministic tick-driven expiry: working; state is not yet persisted across restart. |
| Compaction / backup | Claim-based MVCC GC floor and consistent backup/restore at a protected revision: working. |
| CDC | Ordered-ack egress with backfill, resolved watermarks, retention claims: working. |
| Multi-tenancy | Single tenant today: every anchor stamps tenant 0; the key layout and wire carry tenant identity for later use. |
| Control-plane policy | Quota, auth-principal, and placement-advice modules exist but are not driven by a real control-plane feed; the substrate's control plane emits a synthetic proof. |

## Not implemented

- Cross-partition transactions and cross-partition linearizable
  ranges (statement-level batches are single-range; `BEGIN`/`COMMIT`
  on the SQL surface are accepted but not transactional)
- etcd `Txn`, `Compact`, `Auth`; election/lock services; TLS on the
  gRPC listener (all shipped graphs serve plaintext HTTP/2)
- Redis hashes/lists/sets/sorted sets/streams, `EXPIRE`/`TTL`,
  `WATCH`, `EVAL`, `PUBLISH`; RESP3 framing (`HELLO 3` is accepted
  but both directions stay RESP2)
- Memcached `cas`/`touch`, the binary protocol, SASL, `noreply`
- Tenant identity on connections (AUTH authenticates, it does not
  select a tenant)

## Repository layout

| Path | Contents |
|---|---|
| `modules/app/` | `no_std` PIC modules: protocol anchors, KV/router/apply modules, watch/lease/TTL, compaction, backup, CDC, range lifecycle, and outbound client connectors. `fluxor modules build` packs each into a `.fmod`. |
| `modules/common/` | Shared `no_std` cores: the KV state machine, codecs, partition maps, wire constants. Pulled into each app module via `#[path]`. |
| `docs/` | Reference documentation, indexed by [`docs/overview.md`](docs/overview.md): architecture (`docs/architecture/`) and guides (`docs/guides/`). |
| `tools/` | Host-side helpers, including `lattice-scrape` (decodes the binary `/metrics` export). |
| `telemetry/` | Telemetry catalog (`telemetry/catalog.json`). |
| `wire/` | Per-protocol wire catalogs. |
| `fluxor.toml` | Project manifest for the `fluxor` CLI: identity, dependencies, project policy. |
| `Makefile` | Thin alias layer over the `fluxor` CLI; `make help` lists the targets. |

## Documentation

- [`docs/guides/running.md`](docs/guides/running.md) — bring-up and
  smoke checks.
- [`docs/overview.md`](docs/overview.md) — index of everything below.
- [`docs/architecture/`](docs/architecture/) — the specification,
  CDC, and the limit register.
- [`docs/guides/`](docs/guides/) — deployment, high availability,
  tuning, and the per-protocol guides (Redis, Memcached, etcd).

## License

See [`LICENSE`](LICENSE).
