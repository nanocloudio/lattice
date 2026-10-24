# In-graph fronts and the store stack

An application reaches lattice the way it reaches any layer in a fluxor
graph: through exchange edges (fluxor's exchange contract,
`modules/sdk/contracts/exchange.rs`). Whether the store runs in the
application's own graph or is a foreign server across a network is a
placement choice, not a different integration:

| Placement | SQL | KV | Change feed |
|---|---|---|---|
| A foreign server (real PostgreSQL, real Redis) | `pg_client` | `redis_client` | — |
| lattice's store in the same graph | `sql_front` | `kv_front` | `cdc_front` (PULL), `cdc_pump` (PUSH) |

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
