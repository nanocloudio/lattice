# In-graph fronts and the store stack

An application reaches lattice the way it reaches any layer in a fluxor
graph: through exchange edges (fluxor's exchange contract,
`modules/sdk/contracts/exchange.rs`). Whether the store runs in the
application's own graph or is a foreign server across a network is a
placement choice, not a different integration:

| Placement | SQL | KV | Change feed | Route catalogue |
|---|---|---|---|---|
| A foreign server (real PostgreSQL, real Redis) | `pg_client` | `redis_client` | — | — |
| lattice's store in the same graph | `sql_front` | `kv_front` | `cdc_front` (PULL), `cdc_pump` (PUSH) | `route_catalogue_front` |

Each front takes its protocol client's requests and gives its answers —
byte-identical to what the client reads from lattice's own listener for the
same request — so an application that wires `pg_client.request_in` /
`pg_client.response_out` wires `sql_front.request_in` /
`sql_front.response_out` instead (or the store stack's
`sql_request_in` / `sql_response_out` exports) and changes nothing else.
The protocol clients stay: they are the path to foreign systems, and the
public listeners (`pg_edge_anchor`, `redis_edge_anchor`, …) are for foreign
clients.

The store and its fronts ship composed, as the published stacks
`packaging/stack/lattice_store.yaml` and
`packaging/stack/lattice_store_group3.yaml` ([below](#the-store-stack)). `examples/store_fronts/` includes them with a
requester on each front; `tools/e2e/store_fronts.sh` proves each live.

## SQL front (`sql_front`)

The exchange surface `pg_client` offers, served by the graph's own
`relational_executor`: no wire protocol, no TCP, no SCRAM. The front sends
each statement to the executor's PostgreSQL pair (`pg_in`/`pg_out`) as
`MSG_SQL_REQUEST` and renders the typed answer as PostgreSQL text, through
the PostgreSQL connector's own rendering (`pg_server_codec`) — so the answer
is what `pg_client` reads from lattice over the wire.

**Request** — identical to `pg_client`'s: method `POST`; `target` empty;
headers ignored; body the SQL text of one statement, 1..=4096 bytes, no NUL
byte. The body may arrive inline in the HEAD or in BODY records; the front
grants the credit for the rest at once.

**Answer** — one per request, no content type, no headers; identical to
`pg_client`'s:

| Status | When | Body |
|---|---|---|
| 200 | the statement completed (a write is applied: durable as the store is) | the first column of every row as PostgreSQL text, in row order, joined by `\n`; no trailing separator; a NULL first column contributes no row; a statement with no rows answers an empty body |
| 400 | the executor refused the statement | the message PostgreSQL clients get from lattice (`relation does not exist`, `duplicate key value violates primary key`, `syntax error`, …) |
| 400 | another method, a non-empty target, an empty body, a NUL in the body | empty |
| 413 | a body over 4096 bytes, a target over 64, headers over 256 | empty |
| 502 | a result set over 8192 bytes (refused whole, never truncated), an executor answer that does not decode | empty |
| 503 | four requests already held, or draining | empty |
| 504 | the executor did not answer within 15 s (the statement's effect is unknown) | empty |

The body streams under the requester's response credit. Statements run one
at a time, in arrival order, on one executor session (as `pg_client` holds
one connection), so a `BEGIN` … `COMMIT` sent as successive requests is one
session's block, as it is over `pg_client`'s connection. Besides the
statement being performed, the front holds four requests; with all four
held and complete it stops reading request records (the requester's
backpressure), and a request that opens while one of the four is still
receiving its body is answered 503. Draining answers every held request
503 and lets the one being performed finish. There is no backend link to
lose: the front writes no LINK record. Where `pg_client` answers 502 for a
server it cannot reach, the front refuses to construct without its
executor pair wired.

The front's statements carry correlation ids in a namespace of their own
and run on executor session `0xFE`, apart from the sessions the edge
anchors use.

## KV front (`kv_front`)

The exchange surface `redis_client` offers, served by the graph's own
`kv_request_router` on its Redis pair (`redis_in`/`redis_out`, requests
stamped `PROTO_REDIS`): no RESP on a socket, no TCP, no AUTH. The front
builds the command's RESP form exactly as `redis_client` does, performs it
as `redis_edge_anchor` does (`redis_codec`: the same command set, typed
operations and error texts, large values sent as chunks and read back
whole), and maps the reply with `redis_client`'s own mapping
(`resp_classify`).

**Request** — identical to `redis_client`'s: method `POST`; `target` empty;
headers ignored; body exactly one STRUCTURED command, little-endian,
`[nargs:u8]` then per argument `[len:u16][bytes]` — `SET greeting hi` is
`03 | 03 00 "SET" | 08 00 "greeting" | 02 00 "hi"`; up to 4096 bytes.

**Answer** — one per request, no content type, no headers; identical to
`redis_client`'s:

| Status | Reply | Body |
|---|---|---|
| 200 | `+OK` simple string | the text, without `+` and CRLF |
| 200 | `:N` integer | the decimal digits |
| 200 | `$n` bulk string | the `n` data bytes |
| 200 | `$-1` nil | empty |
| 200 | `*N` array | the whole RESP reply, header included, verbatim |
| 400 | `-ERR …` | the error text, without `-` and CRLF (`ERR unknown command 'NOPE'`) |
| 400 | — | another method, a non-empty target, a body that is not exactly one structured command |
| 413 | — | a body over 4096 bytes, a command whose RESP form exceeds 8192 bytes |
| 502 | — | a reply over 8192 bytes |
| 503 | — | four requests already held, or draining |
| 504 | — | no answer from the store within 15 s (the command's effect is unknown) |

Commands run one at a time, in arrival order, held as the SQL front holds
its statements. A value past one replicated entry goes to the store as
chunks under a manifest, as the Redis connector sends one. `PING`, `ECHO` and `COMMAND`
are answered by the front. A command whose meaning is a connection's state —
`AUTH`, `HELLO`, `SELECT`, `CLIENT`, `QUIT`, `RESET`, `MULTI`, `EXEC`,
`DISCARD`, `WATCH`, `UNWATCH`, `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`,
`PUNSUBSCRIBE`, `PUBLISH` — is answered 400 `ERR '<name>' needs a
connection; the KV front serves one command per exchange`: a front checks no
authority (access is the graph's wiring) and holds no session. A body of no
arguments is 400. No LINK record is written.

## CDC subscriptions (`cdc_front`)

The change feed as held exchanges: the PULL form, beside `cdc_pump`'s PUSH
form. A subscription is a GET on a feed target, answered by a response HEAD
flagged `HOLD|MORE`, then one BODY per change, never past the subscriber's
credit. Both forms read the same feed through the same router surface and
carry each change in the same envelope, byte for byte.

**Request**

| | |
|---|---|
| method | `GET` |
| target | `/cdc/<prefix hex>`: the keyrange, as `cdc_pump`'s `prefix` param spells it — 1..=256 bytes, not starting `0x00` (the system prefix), not all `0xFF` |
| `after: <revision>` | optional: resume after this commit position (decimal). Without it the feed starts at the store's head — the changes committed after the subscription opened |
| `feed-id: <n>`, `table-id: <n>` | optional: the envelope's identity fields; default the module's `feed_id` and `table_id` params (`1` and `0`, `cdc_pump`'s defaults) |
| body | none |

**Answer.** A HEAD `200` flagged `HOLD|MORE`, with the header
`revision: <n>` — the position the feed starts after — and no body. Then
each change in the keyrange is one BODY record flagged `MORE`, whose payload
is the `CdcEvent` envelope (`modules/common/cdc_wire.rs`,
[cdc.md](../architecture/cdc.md#events)) `cdc_pump` publishes for the same
change: kind put or delete, flags 0, the feed and table ids, the key, the
commit timestamp, range id 1, range generation 1, the change's revision, the
value. A BODY goes out only when the subscriber's response credit covers the
whole envelope (at most `CDC_ENVELOPE_MAX` bytes): credit is the
subscriber's backpressure, and an event is never split. A subscriber grants
more with CREDIT records.

Events arrive in revision order, one revision's changes in key order. The
`revision` field of the last event a subscriber holds is the position to
resume after; a subscriber that stopped part-way through one revision's
changes resumes `after: <revision - 1>` and receives that revision again
whole.

| Status | When |
|---|---|
| 200 `HOLD\|MORE` | the subscription is open |
| 400 | another method, a request body, a target that is not `/cdc/<hex>` of a legal prefix, a malformed `after`, `feed-id` or `table-id` |
| 410 | the `after` position is not retained: the store compacted past it (the worker's `history_revisions`) |
| 413 | a target over 517 bytes, headers over 256 |
| 502 | the store failed 16 feed reads in a row, or the keyrange holds a large value (stored as chunks), which the envelope cannot carry |
| 503 | four subscriptions already held, or draining |
| 504 | the store did not answer the opening read within 15 s |

Every refusal but 410 is marked `RAISED` (a refusal the front raises itself; 410 answers the position asked for); the `200` head is not. **Ending.** The subscriber ends a
subscription with ABORT. The front ends one with ABORT `STALLED` when the
store compacted past the position the subscriber had reached (re-subscribing
`after` it answers 410), `FAILED` for the 502 conditions, `DRAINING` when it
drains. Every exchange is answered exactly once: a terminal status, or a
held stream that only an ABORT ends.

**How the feed is read.** The front is an ordinary KV client on a free router
pair (the etcd pair by default, its `proto` param naming it), as `cdc_pump`
is: `KV_OP_SCAN_VERSIONS` windows `(from, to]` of the keyrange, and a
`KV_OP_GET` of the prefix key for the head revision a subscription without
`after` starts from. One KV request is in flight at a time, shared by the
subscriptions round-robin, and one unanswered for 3 s is issued again. The
store walks a window in key order, so a window past one page (16 changes)
is narrowed — its upper bound halved toward `from` — until it fits, and
its page is delivered sorted by revision; a window of one revision too
large for a page is walked page by page. A caught-up subscription polls
every 100 ms. The keyrange needs a store that
keeps history: the disk store (`state_store: 1`).

**One module, not a pump mode.** `cdc_pump` is a REQUESTER that owns its
position (a durable, CAS-guarded checkpoint, an owner epoch, retention
claims) and pushes one feed to one sink until each event is accepted. A
subscription is the reverse: `cdc_front` is a PROVIDER, the subscriber owns
its position (`after`), several subscribers share one module, and credit,
not acknowledgement, paces delivery. They share the envelope
(`cdc_wire.rs`) and the router surface, not a state machine, so each is its
own module.

## Route catalogue (`route_catalogue_front`)

A traffic fabric's route and endpoint catalogues as exchanges
([route_catalogue.md](../architecture/route_catalogue.md)): a publisher
POSTs a route generation (up to 16 routes, each with its endpoints, and its policies, among its records — or none, withdrawing the service), a director
GETs a revision-pinned snapshot page by page, an operator POSTs a
garbage-collection pass; a controller POSTs a declaration and appends to
an operation's status stream, and an operator reclaims a closed stream.
Records are opaque WHR1 envelopes checked structurally only. Targets name the service as
`/rc/<tenant hex32>/<service hex32>/<verb>`; integer headers are decimal.

| Request | Headers | Body | Answer |
|---|---|---|---|
| `POST …/publish` | `generation` (non-zero), `predecessor` (0: none), `endpoints` (a comma list, one count per route the generation carries, route 0 first, at most 16; empty: the withdrawn form, no route at all) | one item per record — the HEAD's inline body (held, at most 4,114 bytes, and staged once the generation's staging marker stands), then each BODY — `[kind u8][route u8][id 16][WHR1 envelope]`: kind 1 a route (id ignored), 2 an endpoint of that route, 3 a policy of the generation (route and id ignored: it is keyed by its content address); the record without MORE ends the publication, which is then sealed and activated. A withdrawn generation carries no item | `200` with `generation` and `revision` (the activation's) |
| `GET …/changes` | `after` (the revision the requester holds: its snapshot's, or the last page's `through`) | none | `200` with `revision` (the store's), `through` (every revision in `(after, through]` is on the page, whole), `more` (0/1: `through` is below `revision`) and `changes` (how many); the body is `[revision u64][index u16][count u16][kind u8][klen u16][key][vlen u32][value]` per change, little-endian, kind 0 a write and 1 a delete, in revision then key order, each numbered `index` of its revision's `count`, at most the page and the requester's response credit |
| `GET …/snapshot` | `at` (0 or absent: the store's revision, which the first page pins), `continue` (the last page's token, hex) | none | `200` with `revision`, `more` (0/1), `continue` (hex, when more), `count` and `digest` (hex, on the last page); the body is the page, `[klen u16][key][vlen u32][value]` per record, little-endian, at most 7,635 bytes and the requester's response credit; a page that fills stops after its last whole record and `continue` goes on from there |
| `POST …/gc` | `predecessor`, `keep`, `retention-ms`, `now-ms` (0: the module's clock), all required | none | `200` with `reclaimed`, `deleted` |
| `POST …/declare` | `id` (32 hex digits) | the HEAD's inline body: one placed envelope (any kind but 0–3) | `200` with `generation` (the record's), `revision`, `unchanged` (1: the same bytes were already declared) |
| `POST …/status` | `operation` (32 hex digits), `last` (0/1: the stream's last status) | the HEAD's inline body: one placed envelope | `200` with `seq` (the number it was published under) and `revision` (the head's) |
| `POST …/reclaim` | `operation` (32 hex digits: a closed stream) | none | `200` with `deleted` |

| Status | When |
|---|---|
| 400 | another target or method, a missing or malformed header, a zero generation, a malformed item, an envelope refused structurally, a record of the wrong kind, an `at` past the store's revision, a continuation that is malformed or not of this snapshot's revision and service |
| 409 | the generation's staging marker already exists (the generation is begun or was published), the active pointer is not at the predecessor named, or a racing publisher won the CAS; a declaration at or behind the generation held, or another record at it, or a racing one; a status for a closed stream, a status another occupies the next number with, or a racing append; a reclaim of an open stream |
| 410 | a snapshot revision the store has compacted; changes after a revision it has compacted |
| 413 | a record over 4,096 bytes or over what one routed put carries (an endpoint record over 4,034 bytes, a route record over 4,048), an inline HEAD item over 4,114 bytes, a target over 79 bytes, headers over 512, a response credit too small for one record |
| 422 | a referential gap: a route's endpoints or the policies read back are not those staged, a route declared but not staged (or staged but not declared), or a withdrawn generation with records staged |
| 502 | the store refused an operation or answered what cannot be read (a garbage-collection pass that fails, an active pointer that does not decode) |
| 503 | a request is already held (one at a time), or draining |

Every exchange is answered exactly once. One request is held at a time;
a HEAD arriving while one is held is answered `503` from a slot of its
own, leaving the held request undisturbed. A publication's items are
taken one at a time, each written before the next is read: the front
grants request-body credit for one item (4,114 bytes) each time it is
ready for the next, so a slow store holds the publication at the
requester rather than in memory. A requester's ABORT ends its request
unanswered; a publication's generation stays staged, invisible, for a
garbage-collection pass to reclaim. The front
is an ordinary KV client on a free client pair of `kv_request_router`, as
`cdc_front` is, with one KV request in flight at a time; a snapshot at a
past revision needs a store that keeps history (the disk store,
`state_store: 1`). Any of the four client pairs carries it — the router
answers a request on the pair it came in by — and its `proto` param names
the pair, which it must (any other value refuses construction):

| Pair | `kv_out` to | `kv_in` from | `proto` |
|---|---|---|---|
| etcd (the default) | `router.etcd_in` | `router.etcd_out` | 1 |
| redis | `router.redis_in` | `router.redis_out` | 2 |
| memcached | `router.memcached_in` | `router.memcached_out` | 3 |
| memcached_udp | `router.memcached_udp_in` | `router.memcached_udp_out` | 4 |

A pair is free when no protocol anchor or other client holds it; the
router's `internal_*` pairs belong to their own callers and are not client
pairs. `examples/store_fronts/catalogue_pairs.yaml` wires the front to the
router directly, and the harness suite `live_catalogue_pairs.rs` and
`tools/e2e/store_fronts.sh catalogue_pairs` run it on each of the four
pairs: publish, a refused duplicate, a snapshot and the changes answered on
every one. It emits `activated`,
`refused`, `pages` and `gc_passes` on `metrics` once a second. The
stacks below do not include it; a graph that includes `lattice_store`
can wire its `kv_out`/`kv_in` to the `pump_kv_in`/`pump_kv_out` exports
with `proto: 3` when no `cdc_pump` holds that pair.

## The raw KV pair (`pump_kv`)

The stacks export one of the router's client pairs raw: `pump_kv_in` /
`pump_kv_out` are `router.memcached_in` / `router.memcached_out`, the
memcached pair (`proto: 3`). It is the general in-graph KV client pair, not
a cdc_pump-only one: any module that speaks the router's KV envelopes — a
`cdc_pump`, a `cdc_front` or `route_catalogue_front` given `proto: 3`, or an
application's own store client (a management plane's store, say) — wires
its `kv_out` to `pump_kv_in` and its `kv_in` from `pump_kv_out`, one client
per pair. Nothing on it is memcached-specific: no memcached anchor holds it
in a stack, and the router serves every op on every client pair alike.

**Request** (`MSG_KV_REQUEST`, `0xC0`, `modules/common/wire.rs`): payload
`[corr_id u64][protocol u8 = 3][tenant u32][conn_id u8][consistency u8]
[op u8][body_len u16][body]`, little-endian, a body of at most 4,096 bytes
(`KV_REQUEST_PAYLOAD_MAX`). `corr_id` is the client's own and comes back
verbatim; `conn_id` names a lane: ops on one lane that may touch the same
key apply in the order sent (a multi-key op orders against everything on
its lane), different lanes are independent. `consistency` is a `REQ_*`
byte (`types.rs`; `REQ_LINEARIZABLE` fences a read through the leader on a
replicated stack with `lin_reads: 1`).

**Answer** (`MSG_KV_RESPONSE`, `0xC1`): `[corr_id u64][conn_id u8]
[result u8][revision u64][body_len u16][body][fence tail]` — `revision` the
store's revision the answer was produced at, and the fence tail
(`FenceTail`, 37 bytes) carries the applied Raft index and term on a
replicated stack. Op codes, bodies and results are `types.rs`'s
`KV_OP_*` / `KV_RESULT_*`; the ones a store client is built on:

| Op | Code | Use |
|---|---|---|
| `KV_OP_ETCD_TXN` | `0x1E` | compares (version, create, mod revision, value, lease) over keys or ranges, then one branch of Range / Put / DeleteRange / nested Txn, all at ONE revision: every conditional write. On a multi-range map it executes in the one partition its whole key set lives in (both branches, nested Txns); a key set across partitions answers `KV_RESULT_CROSS_RANGE` |
| `KV_OP_RANGE_META` | `0x1C` | live keys of a span with their create/mod revision, version and lease, at a revision |
| `KV_OP_SCAN_VERSIONS` | `0x14` | every version in a span whose revision is in `(from, to]`, deletes included (`VERSIONS_FLAG_META` adds create revision, version and lease): the watch read. `KV_RESULT_COMPACTED` once `from` is below the history kept — the stacks' `history_revisions` (65,536 by default) — which is a watcher's 410: relist |
| `KV_OP_COMPACT` | `0x21` | retire history below a revision (the stacks also compact on their own, `history_revisions`) |
| `KV_OP_HASH_KV` | `0x22` | the store's revision and a digest of its live keys |
| `KV_OP_LEASE_KEYS` / `KV_OP_LEASE_REVOKE` | `0x20` / `0x1F` | a lease's keys; delete them with it |

An op the answering layer does not implement — an op code past what its
build defines (a client newer than the store: version skew), or one no
store applies — is answered `KV_RESULT_UNSUPPORTED` (`0x1D`): by the router
before any side effect, else by the worker, which neither applies nor logs
it. Nothing was done; the same op at the same build cannot succeed. The
fronts say it as their own "unimplemented" — RESP `-ERR unsupported
operation`, memcached `SERVER_ERROR unsupported operation`, etcd gRPC 12
`UNIMPLEMENTED`, the native data surface `UnsupportedCapability` — so skew
never reads as `KV_RESULT_INTERNAL` (`0xFF`), a fault.

A lease here is a key's `lease` field (a Put's `lease` and a Txn's lease
compare); granting and keeping one alive is the client's own record, written
with the same Txns. On `lattice_store_group3` every write — an ETCD_TXN
among them — commits on a quorum before it is answered, through any
replica's pair: a follower's consensus forwards it to the leader of its
term and it applies everywhere like a leader's write. An ETCD_TXN whose
branches only read (Ranges, nested read-only Txns) is a read: routed with
the consistency it asks — a linearizable one fenced through the leader's
ReadIndex, on a follower too — never proposed. A write the group does not
take is answered typed: `KV_RESULT_NO_LEADER` (`0x1E`: not appended — no
leader past the hold, a superseded position, strict fallback; retry as
it is), `KV_RESULT_QUOTA` (leader backpressure), `KV_RESULT_TOO_LARGE`, or
`KV_RESULT_OUTCOME_UNKNOWN` (`0x1F`: forwarded to a leader that failed
before saying — it may have committed; read back before re-sending a
write that is not idempotent; also the answer to a replicated write with
no answer past the router's 12 s deadline). A write that committed across
a leader change is answered once, by the replica it was sent to; it is
never applied twice (`tests/harness/tests/cluster.rs`,
`an_etcd_txn_compare_and_put_is_linearizable_across_a_leader_change`,
`etcd_writes_through_a_follower_survive_its_leader_failing`).

A backup or any reader that reads the store at one revision for a while
holds it with `KV_OP_RETAIN` (`0x2D`, `types.rs`): body `[floor:u64]
[ttl_ms:u64][name_len:u16][name]` — `floor` `u64::MAX` takes the hold at the
store's current revision, another floor moves or renews it, 0 releases it;
an empty name is a query. While a hold lives (its TTL on the store's
clock, renewed by taking it again), no Compact — a client's or the
store's own history compaction — passes its floor, and the store's GC
retention claim (`CLAIM_SOURCE_BACKUP`) asks for no less. Answered `[floor]
[compact_revision][hold_floor]`; a floor already compacted is refused
`KV_RESULT_COMPACTED`. `backup_coordinator` takes one (`bkp/<backup id>`)
for its whole export.

Values: every point edge (redis, memcached, etcd, the KV front) writes and
reads back values up to `kv_large::VALUE_MAX` (8 KiB) whole, one past a data
chunk stored as chunks under a manifest; a larger write is refused as each
protocol refuses one, before any chunk is sent.

## The store stack

`packaging/stack/lattice_store.yaml` (`stack: lattice_store`) is lattice's
single-node store with the three fronts: the KV state worker on the disk
store with its own command log, the router, the relational executor and its
index backfill, the TTL clock, and `sql_front`, `kv_front`, `cdc_front`
wired to them.

| Param | Type | Default | Meaning |
|---|---|---|---|
| `store_dir` | string | `kv` | the disk store's directory, created by the operator; up to 128 bytes |
| `history_revisions` | integer | 65536 | history kept, in revisions; a subscription from before it answers 410; 0 keeps everything |
| `sweep_grace_ms` | integer | 300000 | how long an unreferenced large-value chunk is kept before a sweep may take it |
| `feed_id` | integer | 1 | the CDC envelope's `feed_id` when a subscription names none |
| `table_id` | integer | 0 | the CDC envelope's `table_id` when a subscription names none |

| Export | Port | Wire it to |
|---|---|---|
| `sql_request_in` | `sql_front.request_in` | a SQL requester's `request_out` |
| `sql_response_out` | `sql_front.response_out` | its `response_in` |
| `kv_request_in` | `kv_front.request_in` | a KV requester's `request_out` |
| `kv_response_out` | `kv_front.response_out` | its `response_in` |
| `cdc_request_in` | `cdc_front.request_in` | a subscriber's `request_out` |
| `cdc_response_out` | `cdc_front.response_out` | its `response_in` |
| `pump_kv_in` | `router.memcached_in` | a `cdc_pump`'s `kv_out` (`proto: 3`) |
| `pump_kv_out` | `router.memcached_out` | that pump's `kv_in` |

The stack's router instance is `router` (a `kv_request_router`). The
includer creates `store_dir` beforehand, and the graph needs an FS provider
(the Linux target supplies one).

```yaml
target: linux
include:
  - stack: lattice_store          # by name, or a path to the file
    as: store
    params: { store_dir: data/store }
modules:
  - name: app
    type: exchange_cli               # or any exchange requester
wiring:
  - { from: app.request_out, to: store.sql_request_in }
  - { from: store.sql_response_out, to: app.response_in }
```

`packaging/stack/lattice_store_group3.yaml` (`stack: lattice_store_group3`) is
the same store as one replica of a three-voter clustor group: the worker
applies the group's committed log (consensus and durability in place of its
own command log, the WAL in `wal/`). A write through the leader's fronts
commits on a quorum before it is answered, and every replica's subscriptions
see it applied. A write reaches the group through the leader's router: a
follower's gateway refuses the proposal, so a follower's fronts answer a
write 400 with the connectors' no-authority refusal (`storage unavailable` on
the SQL front, `CLUSTERDOWN no authority available for this operation` on the
KV front), while its reads and subscriptions are served from its own applied
state. Its params are `self_id` (0..=2, required), `listen_port` (9090),
`peer0`, `peer1`, `peer2` (`host:port`, required), `store_dir`, `lin_reads`
(0; 1 fences reads through the leader), `sweep_grace_ms`, `feed_id` and
`table_id`; its exports are the eight above and the peer link,
`peer_net_in` / `peer_net_out` (`peer_router.net_in` / `net_out`), which the
includer wires to `linux_net` (or through a `tls` pair). Params substitute
values only, so the group's size — `peer_router`'s `peerN` keys and the voter
count — is the stack's shape: a group of another size is another
stack, and so is a peer link with optional PKI modules (nanocloud's
replica composition).

Check either alone with `fluxor build --check packaging/stack/<file>`, and
a graph that includes one with `fluxor build --check <graph>`. The graphs in
`examples/store_fronts/` include them by path. Another project lists
`lattice` under `[dependencies]` and includes the stack by name; the
include is the reference, and `fluxor update` pins the published stack and
the modules it composes.
