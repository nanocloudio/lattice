# Route and endpoint catalogues

Lattice stores and distributes Wormhole's route and endpoint records: a
publisher stages a route generation — up to 16 routes, each with its
endpoints, or none at all to withdraw the service — seals it with manifests and
activates it by compare-and-swap; a director takes a complete snapshot at
one revision, installs it, then consumes changes strictly after it and
exposes a generation only when it is whole. Beside the generations a
service's catalogue carries declarations (one record per id that only
goes forward — a region's or the global manifest, an address or
failure-domain policy) and operations' status streams, published gap
free. Lattice checks every record structurally and interprets none.

The cores are `modules/common/route_catalogue.rs` (the key layout, the
envelope check, the manifest digest, and the publication, snapshot and
garbage-collection machines), `modules/common/route_catalogue_reader.rs`
(the director's rule) and `modules/common/route_catalogue_front.rs` (the
requests), served by the module `modules/app/route_catalogue_front`
([guides/fronts.md](../guides/fronts.md#route-catalogue-route_catalogue_front)).
`tests/harness/tests/contract_route_catalogue.rs` drives all three
against the real disk store, `contract_catalogue_records.rs` the policies,
declarations, status streams and readiness, and `contract_catalogue_kpg.rs`
the catalogue across key/partition groups.

## Records

A route or endpoint record is Wormhole's WHR1 envelope:

```text
magic "WHR1" | kind u16 LE | flags u16 LE | record_len u32 LE
| record_generation u64 LE | operation_id [16] | payload [record_len]
| content_digest [32]
```

Lattice accepts one when, and only when: it is at most 4,096 bytes
(`RECORD_MAX`, the envelope ceiling — a record is never chunked); it is
at least a header and a digest; the magic is `WHR1`; the kind is route
(1), endpoint (2) or endpoint manifest (3); every bit of `flags` is zero
(all are reserved); `record_len` and the bytes agree exactly (no trailing
bytes, no payload cut short); and the digest is the SHA-256 of the header
and payload. The payload is not read.

A **placed record** — a generation's policy, a declaration, a status — is
the same envelope under the same checks (`validate_record`), of any kind
but the reserved zero and the three a generation's fixed places hold
(route, endpoint, endpoint manifest). The kinds are the publisher's:
Wormhole's traffic dial (`0x84`) and relay policy (`0x83`), its
attachment, chain and egress policies, its prefix authority (`0x7E`),
its failure-domain family (`0xA0`–`0xA6`, the evacuation status `0xA4`
and outcome `0xA5` among them) and its region family (`0xB0`–`0xB7`,
`RegionManifest` and `GlobalManifest` first) are all carried, none
known. A placed record's key names its kind, and a reader refuses a
record whose envelope says another.

A record is written by one `KV_OP_PUT` through the router, which carries
a request body of at most `KV_BODY_MAX` (4,122) bytes — its 4,160-byte
ingress less the envelope and the command head it re-heads a request
with — and discards a larger request without an answer. A put is the
key, the record and 15 bytes of framing, so the largest endpoint record
Lattice stores is 4,034 bytes and the largest route record 4,048; a
larger valid envelope is refused `TooLarge` (the front: `413`) before it
is sent.

The records Lattice writes itself are small fixed forms of their own:

```text
LCM1 manifest : "LCM1" | count u32 LE | digest [32]                     a route's endpoint-set manifest
LCS1 sealed   : "LCS1" | routes u32 LE | endpoints u32 LE | records u32 LE | digest [32]
                                                                         the route-generation manifest
active        : generation u64 LE | sealed_digest [32] | revision u64 LE
LCG1 marker   : "LCG1" | started_ms u64 LE | publisher [16]        a staging marker
LSH1 head     : "LSH1" | seq u64 LE | flags u32 LE | chain [32]     a status stream's head (flag bit 0: closed)
```

## Key layout

Every component is fixed-width binary, so no identifier is ever
truncated to fit and a key parses without delimiters; the longest key is
73 bytes, under the store provider's 255-byte key and the 256-byte watch
key, and a whole service's catalogue sits under one 37-byte prefix a
single watch covers.

```text
rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/pol/<kind u16 BE><id 16>  72 B  a policy of the generation
rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/r/<r u8>/ep/<endpoint 16>  73 B  route r's endpoint record (WHR1)
rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/r/<r u8>/manifest  62 B  route r's endpoint-set manifest (LCM1)
rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/r/<r u8>/route     59 B  route r's route record (WHR1)
rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/sealed             56 B  route-generation manifest (LCS1)
rc/<tenant 16>/<service 16>/staging/<generation u64 BE>                53 B  staging marker (LCG1)
rc/<tenant 16>/<service 16>/active                                     43 B  the active-generation pointer
rc/<tenant 16>/<service 16>/decl/<kind u16 BE><id 16>                  60 B  a declaration
rc/<tenant 16>/<service 16>/op/<operation 16>/<seq u64 BE>             65 B  an operation's status
rc/<tenant 16>/<service 16>/op/<operation 16>/head                     61 B  its stream's head (LSH1)
```

A generation carries up to 16 routes (`ROUTES_MAX`: a service's ports by
listener family), each in a directory of its own named by its index
`<r u8>`, `0..routes` and contiguous: its route record, its endpoint
records and its endpoint-set manifest. Generations are big-endian so they
list in order; within one, records sort `pol/…`, then each route's
directory in index order — `ep/…`, `manifest`, `route` — then `sealed`. A
policy's id is its **content address**, the first 16 bytes of the SHA-256
of its payload — the id a Wormhole route names the policy by — so a
generation's policies are immutable, resolving one is finding the record
stored under the address named, and one policy serves every route of the
generation that names it.

A generation with no route at all is the **withdrawn form**: a service
with nothing to publish (its last port gone, or no longer handed to this
catalogue) is published as the next generation, sealed over no records
(`routes`, `endpoints` and `records` 0, the all-zero chain), and
activated like any other; a reader reports it `Withdrawn`. Generations
never restart, so a service that returns continues from it. A stream's statuses list in
number order, its head after them.

## Manifest digests

A manifest's digest is a streaming SHA-256 chain over lexicographically
ordered `(record_key, record_content_digest)` pairs:

```text
chain₀ = 0³²
chainᵢ = SHA-256(chainᵢ₋₁ ‖ key_len u16 LE ‖ key ‖ digest)
```

where a record's content digest is the SHA-256 of its stored bytes. A
chain folds one record at a time and is 32 bytes of state, so it streams
through pages and travels in a snapshot's continuation; a key not
strictly after the last is refused, so disorder and duplicates cannot
digest. A route's endpoint-set manifest covers that route's endpoint
records; the sealed manifest covers every record of the generation but
itself (its policies, and each route's endpoints, manifest and route) and
counts its routes, its endpoints and its records. A status
head's chain is the same chain over the statuses it publishes. A record's own
`content_digest` field is covered by the digest of its bytes, so the
chain is as strong as a chain over the field would be.

## Publication

A `Publisher` drives one generation through the store in this order,
every step a KV operation it hands to its I/O one at a time:

1. the staging marker `staging/<g>` (when, and by whom), created only if
   absent (`KV_OP_CAS` on witness 0): a generation number belongs to the
   one publisher that began it, so a generation already begun — staged,
   sealed or active — is refused `Exists` and none of its records is
   ever written over;
2. each route's endpoint records under `gen/<g>/r/<r>/ep/`, and each
   policy under `gen/<g>/pol/<kind><content address>`, each checked
   structurally before it is written;
3. each route's record `gen/<g>/r/<r>/route`;
4. the seal, for the routes the publisher declares (one endpoint count
   each; none is the withdrawn form, which must have nothing staged):
   for each route its endpoints are read back in key order through
   `KV_OP_RANGE_META` pages — every one its manifest will count must
   exist and pass the check, and the count must be the one declared for
   that route (a staged record that did not land is a refusal, never a
   silent shortfall) — and its manifest written; then every record of
   the generation is read back and digested, each declared route's
   record and manifest must be among them and no other route's, every
   policy read back must be one staged (its key its own kind and
   address) and as many as were staged, and the sealed manifest written;
5. the activation: the active pointer is read with its `mod_revision`,
   its generation must be the expected predecessor (0 when there is
   none), and the pointer is advanced by `KV_OP_CAS` on that witness
   (a create when there is none).

The revision watchers see is the CAS's. Two publishers racing on one
predecessor both read the pointer, both build a CAS on its witness, and
the second to apply fails `Raced`: its generation stays staged, sealed
and invisible, never named by the pointer. A publisher that reads the
pointer after another has moved it is refused `Predecessor` before its
CAS. The outcomes a publication can have:

| Outcome | Meaning |
|---|---|
| `Activated` | the pointer names the generation, at `revision` |
| `Exists` | the generation's staging marker already exists: another publisher holds it, or it was begun before |
| `Envelope(…)`, `Kind` | a record refused structurally (`TooLarge` also for a put the router cannot carry), or of the wrong kind for its place |
| `Endpoints { declared, found }` | a referential gap: the endpoints read back are not the endpoints declared |
| `Policies { staged, found }` | a referential gap: the policies read back are not the policies staged |
| `NoRoute` | no route record was staged |
| `Predecessor { expected, actual }`, `Raced` | the pointer is not where the publisher expected, or a racing publisher won |
| `Store(result)`, `Answer` | the store refused an operation, or answered a page that cannot be read |
| `Sequence` | a call out of order (a stage or a seal while a write is in flight, a stage after the seal) |

A failed or abandoned generation is invisible and is reclaimed by `Gc`:
a pass lists the staging markers and removes a generation that is
neither the active one nor the active's predecessor when it is older
than the active by more than `keep` generations (`Superseded`) or its
marker is older than `retention_ms` (`Expired`); a generation's records
are deleted a batch of `DELETE_BATCH` keys at a time, its marker last, so
an interrupted pass leaves nothing the next cannot finish. A pass
reclaims at most `GC_MARKERS` (64) generations; the rest wait for the
next. An active pointer that does not decode stops the pass: without it
no generation can be told apart from the live one. The predecessor kept
beside the active is the caller's to name (the pointer does not record
it), and the pass reads the pointer once, so `retention_ms` must exceed
the longest publication: a generation whose marker has expired is fair
game even if its publisher is still sealing it. Rollback is a new
generation with the last known-good content; the pointer never moves
backwards.

## Snapshot-then-watch

A `Snapshot` pages every record under a service at one revision
(`KV_OP_RANGE_META` with `at_rev`; 0 pins the first page's revision).
Each page that stops — the store's page limit (`PAGE_LIMIT`, 8 entries),
the store's answer full, or the caller's page full — says `more` and
carries an opaque continuation: the revision, the count and digest chain
so far, and the last key. Records go into the caller's page while they
fit and the continuation resumes after the last one that did, so a page
is never cut and a store answer larger than the page never stalls the
snapshot; only a page too small for even one record is refused
(`PageTooSmall`). A continuation presented with another revision, or for
another service, is refused. The last page carries the manifest
`(count, digest)` over every record of the snapshot. A revision the
store has compacted answers `Compacted` (the front: `410`), one past the
store `Future`. The snapshot needs a store that keeps history (the disk
store, `state_store: 1`): the memory store honestly answers a past
revision as compacted.

The `Reader` is the director's rule, modelled so the director's own
table builder has it to follow: install the snapshot's records in page
order (a record out of key order is a reorder), verify the manifest
against the chain it recomputed (a disagreement is `SnapshotDigest`),
then consume `CHANGES` records strictly after the snapshot's revision —
one at or below it is already held and is nothing; the same write again
is nothing; a write with a revision below the one held for its key is a
reorder; a page whose safe revision goes backwards is a reorder; a page
status that is not a page (LOST, `RESYNC_REQUIRED`) is `Compacted`. The
active generation is exposed only when every record its sealed manifest
covers is held and the digests agree — the sealed chain over the
generation's records, the endpoint-set chain over its endpoints, the
counts, and the pointer's own copy of the sealed digest — else the
pointer is a `Gap` and the last whole generation stays exposed. Every
route's directory is checked so, each against its own manifest, and a
route past the count sealed is a gap. A withdrawn generation is exposed
as `Withdrawn`: the service publishes nothing from its revision on. A
table too small for the catalogue is `Overflow`. Every one of these is an
explicit resync; no partial generation is ever exposed, and a director
restarted takes a fresh snapshot and consumes from its revision: exactly
a fenced snapshot plus an ordered tail.

The reader holds one entry per record — its key, revision and content
digest, and for an endpoint-set or sealed manifest its decoded counts and
digest — in a table the caller sizes; record contents stream through to
the director's own table. Because each manifest's decoded form lives in
its own entry, any number of staged or sealed generations beside the
active one (a racing publisher's, one refused at activation) cannot
crowd the active one's out.

## Changes: the tail as whole revisions

A director that installed a snapshot at revision `R` can also read the
tail through the front rather than through a durable subscription:
`GET …/changes` with `after: R` answers every change under the service in
`(R, through]`, and each change carries its revision, its `index` among
that revision's changes and their `count`. A consumer applies a revision
only once it holds all `count` of its changes (Wormhole's adapter does
exactly that with its `index`/`count` change records), and every page
ends on a revision boundary, so no revision is ever split across pages.
The next page asks from the last page's `through`; `more` says whether
`through` is still below the store's revision the request found.

The `Changes` machine reads one `KV_OP_SCAN_VERSIONS` window `(after,
to]` over the service's span (`to` 0 first: the store pins its revision
and reports it). The store walks a window key by key, so only a window it
answered whole tells how many changes each revision holds: a window the
store did not answer whole (a `next_cursor`), or whose changes do not fit
the requester's page, is halved toward `after` and read again until it
fits. The answer is sorted by revision, then key. A window of one revision
that still does not fit is `413` — never a cut revision — and
[`CHANGES_LIMIT`](limit_register.md) (64) holds at least a delete batch,
so one revision of the catalogue's own writes (a collection or reclaim
batch is the largest) always fits. `after` below the store's retained
history is `410` (take a snapshot), past the store `400`. Like the
snapshot, the tail needs a store that keeps history (`state_store: 1`).

`contract_route_catalogue`'s
`the_front_serves_the_tail_as_whole_revisions_each_counted` proves it
against the disk store: three publications and two transactions after a
snapshot's revision (two writes at one revision; a delete and a write at
another) come back exactly as the store's own write log says, in revision
then key order, each revision numbered whole, through pages large and
small (a small page is served as several narrowed windows); a
page too small for one revision is `413`; nothing past the store's
revision is an empty page at it; another service's writes never appear.

## Declarations

A declaration is one placed record of a kind under an id the publisher
names, `decl/<kind><id>`: a region's `RegionManifest` under its region
identity, the service's `GlobalManifest`, an address or failure-domain
policy. A `Declarer` reads the record held with its `mod_revision` and
writes the new one by `KV_OP_CAS` on that witness (a create when there is
none) only when the new envelope's `record_generation` is later: a
declaration never goes back, and of two racing declarations one wins.

| Outcome | Meaning |
|---|---|
| declared | the record stands, at the CAS's revision |
| declared, `unchanged` | the same bytes were already declared: answered at their revision, nothing written |
| `Stale { held }` | the record held is of this generation or a later one |
| `Conflict` | the record held is of this generation with other bytes |
| `Raced` | another declaration's CAS won |
| `Envelope(…)`, `Store(…)`, `Answer` | refused structurally; the store refused; an answer that cannot be read |

The bytes are stored and served verbatim, so whatever a manifest's
payload says — each tier's digest, its generations — reaches a watcher
exactly as its publisher wrote it, its own content digest intact.

## Status streams

An operation's statuses — an evacuation's, a traffic dial's — are
published gap free, one stream per operation id under `op/<operation>/`.
A `StatusLog` appends one status at a time: it reads the head, creates
the status under the next number (`KV_OP_CAS` on witness 0, so a number
is written once), then advances the head over it by `KV_OP_CAS` on the
head's witness, its chain extended by the status. What is published is
exactly the statuses `1..=seq` the head names; a status above the head is
staged, not published. The appender's last status closes the stream
(`LSH1` flag bit 0) and a later append is refused `Closed`.

An append interrupted between its create and the head's CAS is finished
by appending the same status again — found under its number byte for
byte, it is not written twice, and the head moves over it; another status
found there is `Conflict` (one appender per operation: the controller
that runs it). A head's CAS lost to a racing append is `Raced`. Lattice
reads the envelope and nothing of the payload: which status is last is
the appender's word.

A `Reclaim` withdraws a closed stream (an open one is refused): the head
is deleted first, so a watcher sees the whole stream withdrawn at one
revision and never a published status go, then the statuses a
`DELETE_BATCH` at a time; an interrupted pass is finished by the next,
which finds no head and deletes what is left. When to reclaim is the
caller's retention.

## What a reader reports

Beside the generation it exposes, the `Reader` checks and reports what
the catalogue carries:

| Record | Checked | Reported |
|---|---|---|
| a policy | a placed record whose kind and content address are its key's (`BadRecord`); a member of its generation, covered by the sealed manifest | with its generation |
| a declaration | a placed record of its key's kind (`BadRecord`), never going back a generation (`Regression`) | `Declared { kind, id, generation, revision }`, `Undeclared` |
| a status head | statuses `1..=seq` all held, in order, their chain the head's (`StatusGap { seq }`) — at install and whenever the head moves | `Status { operation, seq, closed, revision }` |

`Reader::declaration` and `Reader::status` answer what is held;
`Reader::published` says whether a status a page carried is published
yet.

## Readiness

A director's readiness is explicit (`Reader::readiness(now_ms,
fresh_ms)`): **unready** until a snapshot is installed and again after any
resync; **fresh** while live and the consumer has told the reader, within
`fresh_ms`, that the store holds nothing newer than what was delivered
(`Reader::caught_up`, on a snapshot's last page or a change page that was
not cut short); **stale** otherwise — what is exposed is whole, and the
store has not been heard from (the RFC's "Lattice unavailable": the
current table for existing flows, new flows by the director's own policy).

## Across key/partition groups

On an ordered range map (`kv_request_router` `routing_mode: 2`) each
range is one key/partition group with its own store and its own revision
space. A single-key request goes to the range owning its key; a
`KV_OP_RANGE_META` span to the one range covering it whole, and a span
crossing ranges is refused `KV_RESULT_CROSS_RANGE` (two partitions'
revisions cannot be one snapshot). A service's whole catalogue — its
generations, declarations and streams, its one snapshot span and its one
watch prefix — lies under its service prefix, whose leading service byte
is its catalogue shard, so a map whose boundaries are only shard
boundaries (`shard_boundary(tenant, shard)`: `rc/<tenant>/<shard>`)
places every service in one group. A director keeps one reader, one
durable subscription and one revision per group. A boundary inside a
service is an explicit `CROSS_RANGE` (the front's `502`) on every
snapshot of it, never a snapshot answered from one side.
`contract_catalogue_kpg` proves it over two disk stores behind the real
`OrderedRangeMap`, routed by `route_ordered`'s rule: each service wholly
in its group, independent revisions, a split service refused, and a
group restarted from its files serving the same catalogue and the same
history — a snapshot pinned at a director's revision verifies byte for
byte, a publication it interrupted stays invisible and is reclaimed —
while the other group's director tails on without a resync.

## Durable watches

A director's subscription to the store's change feed is durable
([store_provider.md](store_provider.md#durable-subscriptions)): its cursor
is kept in the store, it resumes exactly after it across a restart of
either side, and a window the provider cannot answer whole is an explicit
`RESYNC_REQUIRED` — the reader's `Compacted`, a fresh snapshot — never a
page with a hole. Watches are partitioned by catalogue shard, a service
id's leading byte under a tenant, so a tenant's catalogue is 256 shards
whatever its size.

## Profiles

The watch, subscription, replay and partition ceilings are sized by
profile; the modules that hold them declare `[[variant]]`s `pi5` (the
default, the unsuffixed artefact), `rp2350` and `server`, and every
module of a graph is built in one profile.

| Ceiling | rp2350 | pi5 | server | Arithmetic |
|---|---:|---:|---:|---|
| `MAX_WATCHES` (`watch_hub`) | 64 | 1,024 | 8,192 | one watch per catalogue shard: 256 shards a tenant (Pi 5: 4,096 routes and 32,768 endpoints are 16 and 128 a shard; a server's 65,536 and 1,048,576 are 256 and 4,096); four tenants' directors on a Pi 5, thirty-two on a server, one tenant's 64 shards on an RP2350 (16 routes, 64 endpoints). 392 bytes a record, plus 64 for its session-worker slot: ~29 KiB, ~456 KiB, ~3.6 MiB |
| `DEFAULT_SUB_QUOTA` (`store_provider`) | 32 | 256 | 4,096 | a director's store subscriptions, one per shard: a shard's worth, one tenant's, sixteen tenants' |
| `MAX_PENDING_PLANS` (`watch_fanout`) | 4 | 16 | 256 | a reconnect storm: the ingress hosts an edge represents (8, 256, 4,096), each reconnecting at most a shard at a time |
| `WINDOW_BYTES`, `PREV_BYTES` (`watch_fanout`) | 16 KiB | 64 KiB | 1 MiB | one shard's records as one backfill window (64 × 256 B; 128 × 512 B; 4,096 × 256 B) |
| `WINDOW_FRAMES` (`watch_fanout`) | 64 | 256 | 4,096 | a shard's records and their manifests |
| `MAX_RANGES` (`partition_map`) | 16 | 64 | 128 | a map ahead of the concurrent route/table builds (1, 8, 64) |

The watch key is 256 bytes in every profile (proven end to end by
`contract_key_profiles`); the catalogue's longest key is 72. The store
provider's `KEY_MAX` is 255: fluxor's
`storage.namespace` contract carries a key's length in a byte. A host
suite proves a profile with `cargo test --features profile_server` under
`RUST_MIN_STACK=67108864` (the server hub is module state on a board and
a stack value in a test thread).

## Design notes

- Lattice's manifests chain the SHA-256 of each record's stored bytes
  rather than the envelope's `content_digest` field; the field is
  covered, and the chain streams through pages and continuations.
- The staging marker (`staging/<g>`) is Lattice's own record, so that a
  bounded retention has a time to measure from and a garbage-collection
  pass lists one small record per staged generation.
- A durable subscription is refused on a replica of a replicated store:
  the provider's own writes there are not the group's. Directors on a
  replicated control plane subscribe durably on the node that provides
  the store.
