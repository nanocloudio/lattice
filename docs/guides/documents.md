# Documents and the MongoDB surface

Lattice's document model is generic; MongoDB is one dialect of it.

| Layer | Source | Owns |
|---|---|---|
| Value tree | `modules/common/doc_tree.rs` | The canonical document: ten types, a byte format, a total order, paths |
| Collections | `modules/common/doc_catalog.rs` | What a collection declares: id and shard paths, create stamps, kept and identity paths, generated ids, guards, list keys, ownership, revision path, limits |
| Execution wire | `modules/common/doc_exec.rs` | `MSG_DOC_REQUEST` / `MSG_DOC_RESPONSE`: operations, filters, mutations, projections, continuations, outcomes |
| JSON document wire | `modules/common/doc_json.rs` | `MSG_JSON_DOC_REQUEST` / `MSG_JSON_DOC_RESPONSE`: a canonical operation with JSON bodies and the view or table it is read through, from a record adapter (Chronicle's store connectors); answers rendered by the collection's schema. Stateless: lists page by continuation, watches resume by revision |
| Operations | `modules/common/doc_ops.rs` | Filters, projections, edits, lifecycle, generators, id keys |
| Field sets | `modules/common/doc_fields.rs` | Which fields a writer owns: canonical paths through keyed and set lists, sets and their algebra (what an update changed, what an apply conflicts on or removes); a dialect spells them (`doc_render.rs`: Kubernetes' `fieldsV1`) |
| Executor | `modules/app/document_executor` | Runs operations over the KV store: every mutation one transaction compared on the revision it read. `document_executor_big` is the same source built to hold documents to 1.5 MiB, its buffers one heap arena |
| MongoDB dialect | `modules/app/doc_edge_anchor`, `modules/common/mongo_dialect.rs`, `doc_server_codec.rs` | OP_MSG, BSON ↔ tree, the query and update languages, cursors |
| JSON codec | `modules/common/json_tree.rs` | RFC 8259 text ↔ tree for JSON dialects: integers as integers, other numbers as their exact literal; rendering spells other dialects' types as JSON (shortest round-trip doubles, base64 binary, RFC 3339 timestamps) |
| Schema directives | `modules/common/doc_schema.rs` | A graph's `schema` / `patch_keys` params (`name=value;…`, nanocloud's grammar) translated into a collection's descriptor; rendering directives stay the dialect's |
| Rendering | `modules/common/doc_render.rs` | A JSON dialect's answers from the schema's templates: views, stream framing (events, bookmarks, list tails) and server-side tables, over trees |
| Document wire | `modules/app/doc_wire_anchor` | The canonical operations over TCP, `[len:u32][MSG_DOC_REQUEST]` frames; no dialect, no authentication. A frame past one channel frame (to 16 MiB) streams to the executor as `wire::MSG_PART` parts, and a large answer comes back the same way |
| JSON dialect | `modules/app/doc_json_dialect` (`_big`: to 1.5 MiB) | The JSON document wire in front of the executor: JSON ⇄ tree, the collection's descriptor from the graph's `schema` / `patch_keys` / `id_paths` / `rv_path` params (sent with every write; the first write creates the collection), and answers rendered by the schema — ownership entries placed at `owners`, a view's template, an apply's conflicts as the schema words them; lists as the client's List head, the items and the schema's `list.tail` (revision, text continuation), or as a Table with the graph's `table` columns. A write through a view changes only the view's `write.<v>` paths and is recorded under it |

A dialect translates and renders; the executor executes. The executor never
sees BSON, and the dialect never touches a KV key.

## Graphs

| Graph | What it is |
|---|---|
| `configs/single-document.yaml` | MongoDB on 27018, the executor on the router's compute ingress, the memory store |
| `configs/compute-document.yaml` | The same front and executor, storage reached over `lattice.data` (run `configs/single-data-surface.yaml` first) |
| `configs/single-document-disk.yaml` | The disk store (history: watches, lists paged across writes); MongoDB on 27018 and the document wire on 27100 |
| `configs/single-document-json.yaml` | The JSON document wire on 27200 (`doc_wire_anchor`, `message: 1`) through `doc_json_dialect` with nanocloud's Kubernetes schema as params, on the disk store |
| `configs/bare-metal-pi5-models-disk.yaml` | The Pi 5 image: MongoDB on 27017 over the disk store |

## MongoDB commands

| Command | Behaviour |
|---|---|
| `hello`, `isMaster`, `ping`, `buildInfo`, `getParameter`, `endSessions` | Answered by the dialect |
| `insert` | Atomic: every document in one transaction, or none (a duplicate `_id` fails the command, 11000). A document without `_id` gets an ObjectId |
| `find` | Filter, projection, `limit`, `batchSize`, `singleBatch`; `{_id: v}` is a point read. `sort` only `{_id: 1}` (documents come in id order); `skip` refused |
| `getMore`, `killCursors` | Cursor paging; a cursor belongs to its connection |
| `update` | Operator updates (one or `multi`), replacements (by `_id` or a filter), `upsert` with MongoDB's seed and `$setOnInsert`; `upserted` reports created ids |
| `delete` | By `_id` or filter; `limit: 0` deletes every match |
| `create`, `drop`, `listCollections` | Collection commands; the first write creates a collection |
| `createIndexes`, `listIndexes`, `dropIndexes` | Ascending single or compound indexes, `unique`; a find whose filter requires equality on an indexed field reads the index |

Queries: equality (arrays match their elements; `null` matches null or
missing), `$eq $ne $gt $gte $lt $lte $in $nin $exists $not $and $or $nor`,
dotted paths through arrays and embedded documents. Ordering comparisons hold
within one type (numbers of any width or form together).

Updates: `$set $unset $inc $mul $min $max $push $addToSet` (with `$each`)
`$pull` (by equality) `$pop $setOnInsert`.

Types: `double`, `string`, document, array, `binary`, `ObjectId`, `bool`,
`datetime`, `null`, `int32`, `int64`.

## Refused by name

These answer an error rather than an approximation: `$regex`, `$elemMatch`,
`$size`, `$type`, `$all`, `$mod`, `$expr`, `$where`, `$text`, geo operators;
`$rename`, `$currentDate`, positional (`$`) paths, `$push` modifiers other
than `$each`, conditional `$pull`, pipeline updates; `sort` other than
`{_id: 1}`, `skip`; the BSON types `regex`, `javascript`, `decimal128`,
`timestamp`, `minKey`, `maxKey` and the deprecated ones.

## Semantics, and where they differ from MongoDB

- **Inserts are atomic.** MongoDB's ordered insert keeps the documents before
  a failure; here a refused batch writes nothing and the command fails.
- **One transaction per mutation.** Every write compares the revision it read;
  a lost race re-reads and re-applies (three times), then answers
  WriteConflict (112). An update of many documents is one transaction per
  document, as in MongoDB.
- **Identity is type-faithful.** `_id: 1` and `_id: "1"` are two documents;
  `int32 1`, `int64 1` and `1.0` are one value, and integer ids of any width
  are one id.
- **Cursors read one snapshot.** Each page is read at the first page's
  revision. The memory store keeps no history, so a cursor that pages across a
  write answers "compacted"; the disk store serves it.
- **Large records are reclaimed.** A record's chunk tree goes once nothing
  can read it: on the memory store, after the worker's `sweep_grace_ms`
  (five minutes on the committed clock by default, the window an upload or a
  read in flight has); on the disk store, once the committed GC floor passes
  the write that superseded it, as with the rest of history. The state worker
  proposes the sweeps on `sweep_out`, wired beside its clock.
- **Precision.** A datetime is kept to the millisecond. A decimal written by
  another dialect (a JSON literal) renders as the nearest double, correctly
  rounded. Numbers compare exactly: the literal `0.1` and the double `0.1`
  (0.1000000000000000055…) are different values, as MongoDB's `decimal128`
  and `double` are.

## Limits

| Limit | Value | Past it |
|---|---|---|
| BSON message | 4096 bytes (`hello` advertises it) | The connection closes |
| Document | What one answer carries: about 7.9 KB of record; 1.5 MiB on `document_executor_big` (a record over one replicated write is stored as a chunk tree) | 10334 |
| Insert batch | The executor's request buffer before chunking; each document's chunks go first, then one transaction | 10334 |
| Collection name | 64 bytes | 73 |
| Open cursors | 4 per graph | A `find` needing another fails (96) until one is exhausted or killed |
| `_id` | A string, integer, ObjectId, binary or datetime of at most 1 KiB of key (`models::MAX_DOCUMENT_ID_KEY`) | 2 |

`docs/architecture/limit_register.md` lists each with its source.

## Errors

| Condition | Code |
|---|---|
| Duplicate `_id` | 11000 |
| Collection does not exist (`drop`) | 26 |
| Collection exists (`create`) | 48 |
| Unsupported operator, type, or an update that cannot apply | 2 |
| Malformed BSON or command | 9 |
| Document or batch too large | 10334 |
| Lost the write race | 112 |
| Cursor not found | 43 |
| Timed out | 50 |
| Too many open cursors | 96 |
| Store unavailable | 6 |

## Canonical operations beyond MongoDB

The document wire (and future dialects) reach operations MongoDB does not
spell: patches (RFC 7386 merge, RFC 6902 JSON, and a keyed merge over the
collection's list keys with the dialect's directive names), a delete held by
a declared guard until the guard empties (the answer says held or removed),
and watches — a collection's changes after a revision, in revision order,
paged by the requester's byte budget without splitting a revision, answered
"compacted" where the store keeps no history. A change is `EVENT_CREATE`
(its revision created the document), `EVENT_PUT` or `EVENT_DELETE` (the id);
a document in an event carries its revision at the revision path, as an
answer does.

A watch asked with `FLAG_FOLLOW` stays open: the executor keeps it (up to
`MAX_FOLLOWS`) and answers every later change in its span on the same
`corr`, each answer with `RFLAG_MORE`, until an error, `compacted` or a
cancel (`FLAG_CANCEL` on that `corr`) ends it. The worker's change
notifications (`kv_state_worker.mutations` → `document_executor.changes_in`)
say when; what is answered is always read from the store's history at the
watch's cursor, so a lost notification costs latency (every followed watch
is read again each second), never a change.

The JSON dialect's `id_paths` param is `<paths>[;<collection>,…=<paths>]…`:
the first entry's paths for every collection no later entry names
(Kubernetes: `metadata.namespace,metadata.name`, and `metadata.name` alone
for the cluster-scoped kinds). A schema's `named=<collection>|<path>:<gen>`
stamps and keeps the path in that collection only (Kubernetes' namespace
name label).

The JSON dialect answers a watch as one line per change, the schema's
`event` template with the schema's `event.types` names (creation, change,
deletion, bookmark) — a deleted document reported as its id at the id paths
and the deleting revision; with nothing new and a bookmark asked for
(`J_FLAG_BOOKMARK`, the kind and version the client states), the schema's
`bookmark`; past the store's history, the schema's `expired`. Asked with
`J_FLAG_FOLLOW` the watch stays open: the dialect follows it on the executor
and answers every later batch of changes on the client's `corr` as event
lines (`RFLAG_MORE`), with a bookmark a minute when the client asked for
them, until `J_FLAG_CANCEL` on that `corr` or an answer that ends it
(`expired`).

## Compound ids

A collection may declare several id paths: its id is the values at them,
in order (Kubernetes: `metadata.namespace`, `metadata.name`). The id keys
component by component, so ids order by their first component, then the
next, and a request naming the leading components (`FIELD_ID_PREFIX`, an
array) lists or watches exactly the documents whose ids begin with them —
a namespace is a key range. `FIELD_ID` addresses one document by the whole
array; a generated name fills the last id path.

## Field ownership and apply

A collection that declares `OWNERSHIP` records, with every document, which
writer owns which fields: one entry per manager (the request's
`FIELD_MANAGER`, required on every write), operation and scope label
(`FIELD_SCOPE_NAME`, a Kubernetes subresource), with the time and the
document's version (the text at the declared version path). The entries
are stored in the document's own record, so one compared write keeps both;
every answer carries them (`RESP_OWNERS` after the document or event).

- **An update** (insert, replace, patch) owns the fields it changed, taking
  them from whoever held them; fields the document loses leave every entry.
- **An apply** (`OP_APPLY`, the configuration in `FIELD_DOC`) merges the
  configuration over the stored document as a keyed patch (creating it
  when absent) and owns exactly the configuration's fields. A field
  another writer owns with a different value is a conflict: the apply is
  refused (`OUT_OWNERSHIP`, one `RESP_CONFLICT` per field and owner)
  unless `FLAG_FORCE` takes it. A field the manager applied before and no
  longer names is removed, unless another writer owns it or something
  below it; a list element that stays keeps its key members.
- `UNOWNED` paths (identity, bookkeeping) belong to no one.

Fields are canonical paths through keyed and set lists
(`modules/common/doc_fields.rs`); a dialect spells them (Kubernetes'
`fieldsV1`, `doc_render.rs`). An ownership record counts toward the
document's record size.

## Indexes

A collection declares secondary indexes (paths, unique or not). Every write
keeps them in its one transaction: the entries the old document had and the
new one lacks are deleted, the new ones put, and a unique entry is compared
absent — a value another document holds refuses the write (MongoDB 11000).
Every write is also compared on the collection descriptor's revision, so an
index being created fences out writes that read the schema before it; the
build then gives every stored document its entries and turns the index
ready, or — when the documents violate a unique index — withdraws it.

Keys are exact: numbers are keyed by their exact decimal value (`1`, `1.0`
and `1.00` are one value; integers past 2^53 stay apart); a missing field
indexes as null; an array makes one entry per distinct element. An object
value cannot be indexed and refuses the write.

A list (or a filtered update or delete) whose filter requires equality on a
ready single-field index reads the index, and applies the whole filter to
each document it yields. Results and continuations are the same as a scan's;
a continuation that outlives its index is refused (list again).
