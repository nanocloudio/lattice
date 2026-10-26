# Lattice as fluxor's store

`kv_state_worker_store` is the state worker built with the store
provider (`modules/common/store_provider.rs`): besides everything
`kv_state_worker` does, it serves fluxor's `storage.object` (contract
`0x14`) and `storage.namespace` (`0x13`) over its own store. A graph whose
store is lattice's keeps every connector as it is — they reach the store by
contract — and gains one clock, one history and one store for them,
lattice's own surfaces and the document executor alike. Each call is
answered synchronously, inside the calling module's provider call; the wire
is fluxor's host store's (`fluxor/src/platform/linux/store.rs`) wherever a
consumer can observe it, and stricter where the host is lax: a short
argument is `EINVAL`, never a panic, and a LIST cursor is the key the next
page starts at, stable across writes.

Every contract operation is one or more KV operations applied exactly as
the worker applies a command, so a provider write is journaled, swept and
seen by lattice's own watches, feeds and protocol surfaces like any other.
A value past one chunk (`INLINE_MAX`, one `kv_chunks` chunk of data) is
uploaded as a chunk tree under a manifest straight from the caller's buffer
and read back whole through the large-value layer (`kv_large.rs`).

It is a separate module type, not a variant of `kv_state_worker`, because
only a graph that makes lattice its store carries the provider's heap: its
read slots, write answers, subscription registry, change log and a 4 MiB
value cache. Ports, params and behaviour are otherwise `kv_state_worker`'s.

## The role

| Configuration | Writes | Fence on a write |
|---|---|---|
| Single node: `state_store: 1`, `sweep_wrap: 0` | applied in the call; answered once the worker's command log holds the record durably (the store turns `command_log` on) | `LocalDurable` |
| One replica of a group: `state_store: 1`, `sweep_wrap: 1`, `prov_out` wired to consensus, its own `replica_id`, `group_durable: 1` | proposed to the group's log under this replica's origin (`origin.rs`) and answered once applied here | `ReplicatedDurable` |

The role needs the disk provider: a change feed needs the history only it
keeps. Any other configuration registers no contract and logs
`[kvw] store provider=0 (<why>)`.

| Param | Id | Default | Meaning |
|---|---|---|---|
| `store_change_log` | 25 | 0 (`CHANGE_LOG_DEFAULT`, 256 KiB) | bytes of the change log; a larger figure needs `heap.arena_bytes` raised to match |
| `store_sub_quota` | 26 | 0 (`DEFAULT_SUB_QUOTA`: 256 in the `store` build, 32 in `store_rp2350`, 4,096 in `store_server`) | subscriptions one workload owner may hold; the system owner (the node's own controllers) is not held to it |
| `history_revisions` | 12 | 0 (all the store holds) | the history a change read may reach back, in revisions: a window from before `revision - history_revisions` is LOST however much the store has yet to compact |
| `scope`, `scoped` | 17, 18 | empty | a node's private records: the graph keeps the plain names of the comma-separated `scoped` prefixes, the store keeps them under `/~node/<scope>`, so every replica holds each node's apart. A LIST, CHANGES or subscription on a scoped prefix answers in the graph's names; one on a broader prefix sees the store's. A scope that cannot be kept registers no contract |
| `import_log` | 10 | empty | a fluxor host store's `store.log` imported once, before the store serves; recorded as done in the provider's own keyspace, so a restart does not import again |
| `replica_id` | 13 | 0 | the origin a replica's proposals carry; two replicas of a group must not share it |

## Operations

| Op | Id | Argument | Answer |
|---|---|---|---|
| `PUT` | `0x1420` | `[key_len u16][key][ct_len u8][ct][body_ptr u64][body_len u64][precondition][fence_ptr u64][fence_cap u16]` | 0, and the write's fence |
| `GET` | `0x1421` | the key | a read slot's handle on the value as it is now |
| `HEAD` | `0x1422` | `[key_len u16][key][out_ptr u64][out_cap u32][fence_ptr u64][fence_cap u16]` | the 50-byte record `[size u64][mtime u64][ct_len u8][etag_len u8][etag]` |
| `RANGE_GET` | `0x1423` | on a read slot: `[off u64][len u32][out_ptr u64]` | bytes copied; 0 at or past the end |
| `DELETE` | `0x1424` | `[key_len u16][key][precondition][fence_ptr u64][fence_cap u16]` | 0, and the write's fence |
| `CLOSE` | `0x1425` | on a read slot | 0 |
| `LIST` | `0x1302` | `[prefix_len u16][prefix][cursor_len u16][cursor][out_ptr u64][out_cap u32][fence_ptr u64][fence_cap u16]` | a page of `[len u8][kind 0][key]` entries, then `[FF][FF][cursor_len u8][cursor]`: the key the next page starts at, none at the end |
| `SUBSCRIBE` | `0x1305` | `[prefix_len u16][prefix][event i32][flags u8]` (bit 0: ring now; bit 1: durable, then `[shard u16][owner_len u8][owner]`) | a subscription's handle |
| `CLOSE` | `0x1306` | on a subscription | 0 |
| `CHANGES` | `0x1307` | `[prefix_len u16][prefix][sub i32][token_len u16][token][out_ptr u64][out_cap u32][fence_ptr u64][fence_cap u16]` | one page (below) |
| `TAKE_RUNG` | `0x1309` | `[event i32][out_ptr u64][out_cap u32]` | `[more u8][count u16][handle i32]…` |
| `CAPS` | `0x13FF` | — | `SUBSCRIBE \| CHANGES` (`0x18`) |
| fence query | `0x00FE` | on a read slot or subscription | the `ViewConsistent` fence of what it observed |

A precondition is `[kind u8][len u8][bytes]`: 0 any, 1 the key must be
absent, 2 an etag (its first eight bytes, the revision it names). Keys are
1..=255 bytes (`KEY_MAX`). Read slots are handles `0..SLOTS`; subscriptions
are `SUB_BAND + id` (a registry id), so a fence query or `CLOSE` names one
thing. Every handle answers only the owner that opened it: to any other
caller it is as if it were not open, as the host stores answer.

## Answers

| Answer | Meaning |
|---|---|
| `EINVAL` | a malformed or short argument, a key past `KEY_MAX`, a subscription event that is not the calling module's, a durable subscription's owner name outside 1..=32 printable bytes or holding `/` |
| `ENXIO` | `GET` or `HEAD` of a key that is not there |
| `EEXIST` | a create-if-absent `PUT` lost to an existing key |
| `EAGAIN` on a read | the store cannot answer now — its device is busy, it is still recovering, a walk is still paused, or this step's history budget is spent; a durable `SUBSCRIBE` whose cursor read the store refuses waits the same way. Nothing moved (no handle opened or closed, no page or fence written); ask again with the same request on a later step, never within the same one |
| `EAGAIN` on a write | the etag precondition failed: the key moved |
| `EINPROGRESS` | the write is not decided yet, or not taken yet; ask again with the same request |
| `ENOMEM` | no heap for another read slot, subscription or answer; a subscription past the owner's quota; an output buffer too small for one entry |
| `EOVERFLOW` | a value or page larger than the call's lengths can carry |
| `ENOSYS` | an unknown op, a handle this caller did not open, or a durable `SUBSCRIBE` on a replica |
| `ERROR` | the store failed or is quarantined |

## Writes

A write's answer is kept for its caller, keyed by `(caller, request
bytes)`, so the re-ask after `EINPROGRESS` finds the same write and is
never applied twice, and another caller's write between a decision and its
re-ask cannot take its answer. A re-ask that matches the caller's last
request field for field and by a per-provider checksum of the value is not
hashed again, so a large value is not rehashed on every ask.

On a single node one write is in the log at a time: while the log is
finishing a record no write is taken (`EINPROGRESS`). A write is applied
only once every record before it is durable, so writes reach the store and
its log in the order they are taken and each answer's revision is exactly
its own. An answer goes when its caller collects it, when that caller asks
a different write, or after `IDLE_STEPS` steps unasked; the write stands
either way. With the heap spent on other callers' answers, a new write is
not taken (`EINPROGRESS`) — another caller's answer is never dropped. A
large value's upload resumes where it stopped when the store refuses a
chunk for now: the chunks before it are neither built nor sent again.

On a replica a write is taken when first asked, offered to the group's log
in seq order (at most `WINDOW` undecided at once, a large value's chunks
`CHUNK_WINDOW` at a time and its manifest once every chunk has applied),
applied here when it comes back, and answered once a position marker
covers it. One found lost — nothing applied for `REOFFER_STEPS` — is
offered again with every write after it, and the origin record makes a
second offer of an applied write nothing. Reads, LIST, CHANGES and
subscriptions are this replica's, at the revision it has applied.

## Subscriptions and the change log

Subscriptions are level-triggered doorbells. A subscription names the
subscriber's own kernel event; the provider never writes a consumer's
channel. Every command that stands notes the keys it changed
(`kv_store::KeyLog`); the provider takes them into its change log
(`change_ring.rs`), matches each against the registry of watched prefixes
(`store_registry.rs`), and rings each matching subscriber once — a rung
bit and a signal — until the subscriber takes it with `TAKE_RUNG` and reads
what changed with `CHANGES`. Nothing is pushed, copied per subscriber, or
lost to a full channel. A subscription whose module is gone, or whose event
slot another module now holds, is dropped instead of signalled.
`SUBSCRIBE` is idempotent on (owner, module, event, prefix, and a durable
subscription's shard and owner name), and a plain one is served while the
store is still recovering, so a consumer that subscribes at start is
registered rather than refused.

The change log is a key-only ring of `[rev u64][hint u8][kl u8][key]`
entries in apply order, bounded below by a base: it answers for every
revision above it and nothing at or below. Every way the log can fail to
hold a change — an entry evicted to make room, a command whose key list
overflowed (`KEYLOG_BYTES`) or cleared the keyspace, an install, a restart
— raises the base and is a gap: every subscriber is rung to read again.

`CHANGES` answers one bounded page: a head `[status u8][more u8][safe_rev
u64][count u32]` (status 0 a page, 1 LOST, 2 RESYNC_REQUIRED), records `[rev u64][kind u8][key_len u16][val_len
u32][key][value]` (kind 0 added, 1 modified, 2 deleted), and a trailer
`[token_len u16][token]` to pass on the next call. The token says where to
read from:

| Token | Read |
|---|---|
| empty | a relist: every entry under the prefix now, by key |
| eight bytes | a bare revision: the changes after it |
| one the provider issued | where the last page stopped — in the change log, in a relist, or in a history walk |
| `[4]` (`T_CURSOR`) | a durable subscription's cursor (below) |

A window the change log answers is served from it, each record read at the
store's current state, so applying a page's records by key is idempotent
and none is omitted. A window starting below the base is walked in the
store's history a page at a time; a window the history cannot answer
either, or a revision this provider never reached, answers status 1,
LOST. Each call is bounded (`LOG_SCAN_MAX` log entries,
`HIST_VISIT_MAX` history versions, `WALK_STEP_BUDGET` versions across all
callers per step); a page cut short ends with a token, and a read refused
for the step's budget answers `EAGAIN` and rings its subscription again
next step.

### Durable subscriptions

A subscription's registry record, its rung bit and the change log are heap
state: a restart of the provider loses them and raises the log's base, a
gap every subscriber must read across. A subscriber that must resume
exactly — a route-catalogue director, which may not serve from a table
with a hole in it — subscribes **durable**: `SUBSCRIBE` flag bit 1
(`SUB_DURABLE`), followed by `[shard u16][owner_len u8][owner]`, an owner
name of 1..=32 bytes of printable ASCII without `/` that the subscriber
keeps across its own restarts, and the catalogue shard the subscription
covers. The provider then keeps the subscription's cursor in the store
itself (`modules/common/watch_cursor.rs`), one record per (owner, shard):

```text
key    \x00lattice/watch-cursor/<owner>/<shard hex4>
record "LWC1" | version u8 = 1 | shard u16 | last_safe_rev u64
       | subscription_generation u32 | digest u32 (FNV-1a over the rest)
```

under the reserved system prefix `\x00`, which lattice's protocol surfaces
never produce (a `storage.object` caller can still name it), written by
`KV_OP_CAS` on the record's `mod_revision` witness exactly as the CDC pump
writes its checkpoint pointer, so two holders of one owner key cannot both
advance it unseen. Asking `CHANGES` on from a token, on the durable
subscription's handle and under its prefix, is the subscriber's word that
it holds everything before that token, and that is when the cursor moves: to the bare revision
asked, to the revision before a log token's (every revision before it was
delivered whole), to a history token's `since`; a relist in progress moves
nothing until its end revision is asked. A revision past the store's own
moves nothing: no subscriber holds what the store has not reached. The
cursor is monotonic and the write is best effort — a store busy now is
tried at the next ask — so a cursor can lag (a restart then re-delivers a
page, idempotent by key) but never lead. Each subscription made under an
owner key writes the `subscription_generation` it read plus one, so the
record names the subscription that last advanced it. On a replica, where
the provider's own writes are not the group's, a durable `SUBSCRIBE` is
`ENOSYS`.

The token `[T_CURSOR]` (one byte, 4) asks to go on from the cursor. The
provider decides as [`watch_cursor::resume`] does — the change log answers
(`cursor >= base`), or the store's history reaches the cursor
(`cursor >= revision - history`): the page is the one a bare-revision token
at the cursor would give, from the log or across the restart's gap from
history. Otherwise, and whenever the history walk itself finds the window
gone, the page is status 2, **`RESYNC_REQUIRED`**: `[2][reason u8][cursor
u64][count 0][token_len 0]`, the reason one of

| Reason | Code | Meaning |
|---|---|---|
| `NO_CURSOR` | 1 | nothing is held under the owner key: a first subscription — relist, then go on from the relist's revision |
| `HISTORY_COMPACTED` | 2 | the history no longer reaches the cursor (the memory store keeps none) |
| `AHEAD_OF_STORE` | 3 | the cursor is past the store's revision: the record names a revision the store never reached |
| `CORRUPT` | 4 | the record under the key is not a cursor this provider reads |
| `CONFLICT` | 5 | another holder advanced the cursor since this one read it |
| `NOT_DURABLE` | 6 | the subscription the read names is not durable |

A resync is the subscriber's cue to take a fresh snapshot (a relist) and go
on from its revision. It is never a page with a hole in it, and it is not
LOST: LOST stays the answer for a window the subscriber named itself that
compaction took. Going on from the relist's revision is the ack that
writes the cursor again: a first cursor, one over a record the decoder
refused, and one below a record past the store, so the subscription is
durable again from there. A `CONFLICT` is final for the holder that met
it: another holder owns the record, and this one's acks write nothing.
`contract_store_provider` proves a durable subscription resumes exactly
after its cursor across a provider restart on the disk store, with no
duplicate and no gap, answers each reason above, and is durable again
after each resync but a conflict.

## Which inputs gate readiness

Fluxor's composer refuses a graph in which a module claiming
`storage.object` or `storage.namespace` feeds the store that answers it
through an input the store waits on before it answers (`gates_readiness`
on the port, default `true`): the fat32-over-its-own-block-device deadlock.
Whether this provider serves is `provider_ready` — the store recovered from
its own files (disk recovery, the command log's replay, a configured
`import_log`) and no snapshot install mid-stream — so its manifest declares
`gates_readiness = false` on every input — `commands`, `read_permit`,
`lease_revoke`, `expire`, `in_epoch_events`, `durability` and
`snapshot_import`: a request pipeline that ends at the router
and the store (an HTTP chain staging bodies here, in nanocloud's planes)
feeds it without deadlocking it. A single-node worker answers a write in
the call; a replica's write is answered `EINPROGRESS` until its own
proposal returns applied and its quorum fence arrives on `durability`,
which the store's own proposal produces, not the feeding pipeline. An
install that begins on `snapshot_import` pauses serving until its stream
completes, and that stream is durability's, read from its own snapshot: a
pause, not a precondition. (Nanocloud's control plane reaches the store
from its HTTP chain through consensus and durability into
`snapshot_import`.) `contract_store_readiness_ports`
pins every input to its declaration.

## Slow stores

A store that takes long is waited on, not failed. The worker answers its
commands in channel order, and while one waits at the head of its command
channel — the device made it wait, its reply waits for the command log,
the store is making room, or it is a paused walk — the worker tells the
router at most once per `KV_WORKING_INTERVAL_MS` (1 s) with
`MSG_KV_WORKING` (`0xF4`, `[kpg_id u16][corr_id u64]`). The router renews
every direct op it routed to that partition for another
`INFLIGHT_TIMEOUT_MS` (5 s) and tells each in-graph caller on the SQL and
data-surface protocols `MSG_KV_PENDING` (`0xF5`, `[corr_id u64][conn_id
u8]`), so a caller that keeps a wedge timer starts it again. A stopped or
quarantined worker sends no notice, and the deadline answers its commands
fail-closed. Consensus and fenced ops are not renewed: what they wait on is
a leader or a read index, not the worker.

The disk store keeps the memtable draining under a long merge: a flush may
start while a compaction is paused between steps short of its manifest. Its
run goes in front of the merge's inputs, newer than every one of them, and
the merge publishes over the run set as it then stands.

The test-only fault knobs that reproduce a busy or slow device on Linux
are in the [limit register](limit_register.md#test-only-fault-injection).
