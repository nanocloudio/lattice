//! Shared types and protocol constants for all Lattice fluxor modules.
//!
//! Pulled into each module via `#[path = "../../common/types.rs"] mod types;`.
//! `#![allow(dead_code)]` because each module consumes a subset.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset of the surface so single-module rustc invocations see unused items"
)]

// ── Tenant / routing identity ──────────────────────────────────────────

/// Tenant ID. Lattice does not currently support more than 2^32 tenants;
/// CP-Raft manifest enforces this bound at admission.
pub type TenantId = u32;

/// Key-partition group ID. Lattice partitions key space into KPGs that
/// each have their own deterministic state machine; routing chooses one
/// KPG per request based on tenant + key hash.
pub type KpgId = u16;

/// Monotonic per-KPG revision counter. Increments on every committed
/// mutation; etcd `mod_revision` and Memcached CAS token both project
/// from this.
pub type Revision = u64;

/// Routing epoch. Advanced by `control_plane` when the placement
/// plan changes; modules carry it on every request and reject mismatches
/// at admission (dirty-epoch rejection).
pub type RoutingEpoch = u32;

/// KV epoch — coarse generation for total replacement of the KV space
/// (snapshot install, hard reset). Increments rarely.
pub type KvEpoch = u32;

// ── Session identity (watch / lease continuity) ────────────────────────

/// Stable per-watch / per-lease identifier issued by the relevant
/// `session.worker` module (`watch_registry` or `lease_manager`). The
/// anchor reuses this ID across stream rebinds; the worker uses it as
/// the primary key in its durable record table.
pub type SessionId = u64;

/// Generation counter on a session record. Bumped whenever the durable
/// state moves to a new worker instance. Used to fence stale anchor
/// frames after rebind.
pub type SessionEpoch = u32;

/// Watch handle alias — same wire type as `SessionId` but separately
/// named for readability at call sites.
pub type WatchId = SessionId;

/// Lease handle alias — same wire type as `SessionId`.
pub type LeaseId = SessionId;

// ── Connection identity ────────────────────────────────────────────────

/// Per-connection slot index within an edge anchor's connection table.
/// 8-bit wide is enough for any anchor we run on a single core (the
/// anchor's `MAX_CONCURRENT_CONNS` is configured per target).
pub type ConnId = u8;

/// Anchor-issued correlation ID for request/response matching across
/// the in-flight pipeline (anchor → router → worker → router → anchor).
pub type CorrelationId = u64;

// ── Protocol identifiers ───────────────────────────────────────────────

pub const PROTO_ETCD: u8 = 0x01;
pub const PROTO_REDIS: u8 = 0x02;
pub const PROTO_MEMCACHED: u8 = 0x03;
pub const PROTO_MEMCACHED_UDP: u8 = 0x04;
/// Internal callers of the KV path — modules inside the graph that
/// issue KV requests on their own behalf rather than on behalf of a
/// connected client.
///
/// They need distinct protocol values for the same reason the client
/// protocols do: the router routes a response by the protocol byte of
/// its request, so two internal callers sharing one value would have
/// their replies delivered to whichever port that value maps to. They
/// are NOT a client-facing wire protocol and never reach a socket;
/// `conn_id` for these is a caller-chosen slot index with no
/// connection behind it.
pub const PROTO_INTERNAL_WATCH: u8 = 0x05;
pub const PROTO_INTERNAL_SQL: u8 = 0x06;
/// The range supervisor (Phase 4). The ONLY caller whose requests may
/// carry [`KV_OP_TARGETED`] — explicit partition addressing is a
/// lifecycle-bootstrap tool, not a client capability.
pub const PROTO_INTERNAL_LIFECYCLE: u8 = 0x07;
/// The Phase 6 index-backfill worker. Future database-operations
/// workers (resolved feeds, backup) get their OWN values — one caller
/// per protocol byte, or replies fan to the wrong module.
pub const PROTO_INTERNAL_JOB: u8 = 0x08;
/// The `lattice.data` anchor (Phase 9): compute that lives in ANOTHER
/// graph, admitted here through the internal data surface.
///
/// Unlike the internal callers above, this one shares its reply port
/// with [`PROTO_INTERNAL_SQL`] — the router's outputs are at fluxor's
/// 16-port cap. That sharing is not a compromise: `PROTO_INTERNAL_SQL`
/// is compute placed IN this graph and `PROTO_INTERNAL_DATA` is the
/// same compute placed remotely, and §30 compares those two placements
/// precisely because a deployment picks one. The router refuses at init
/// if both ingresses are wired, so the shared port always has exactly
/// one owner.
pub const PROTO_INTERNAL_DATA: u8 = 0x09;
pub const PROTO_UNKNOWN: u8 = 0xFF;

// ── Continuity classes (protocol RFC vocabulary) ───────────────────────

pub const CONTINUITY_DRAIN_ONLY: u8 = 0x01;
pub const CONTINUITY_EDGE_ANCHORED: u8 = 0x02;
pub const CONTINUITY_RESUMABLE: u8 = 0x03;
pub const CONTINUITY_REROUTABLE: u8 = 0x04;

// ── KV operation kinds ─────────────────────────────────────────────────

pub const KV_OP_GET: u8 = 0x01;
pub const KV_OP_PUT: u8 = 0x02;
pub const KV_OP_DELETE: u8 = 0x03;
pub const KV_OP_RANGE: u8 = 0x04;
pub const KV_OP_TXN: u8 = 0x05;
pub const KV_OP_CAS: u8 = 0x06;
pub const KV_OP_INCR: u8 = 0x07;
pub const KV_OP_DECR: u8 = 0x08;
pub const KV_OP_APPEND: u8 = 0x09;
pub const KV_OP_EXISTS: u8 = 0x0A;
pub const KV_OP_SCAN: u8 = 0x0B;
pub const KV_OP_FLUSH: u8 = 0x0C;
pub const KV_OP_MGET: u8 = 0x0D;
pub const KV_OP_MSET: u8 = 0x0E;
pub const KV_OP_STRLEN: u8 = 0x0F;
pub const KV_OP_PREPEND: u8 = 0x10;
/// Historical point read at an explicit MVCC revision (RFC §10/§20
/// "snapshot": serve at the supplied MVCC timestamp). Phase-2 slice B.
pub const KV_OP_GET_AT: u8 = 0x11;
/// Historical ordered scan at an explicit MVCC revision (RFC §10
/// "stable scans at one timestamp"). Phase-2 slice B.
pub const KV_OP_SCAN_AT: u8 = 0x12;
/// Half-open range scan `[start, end)` returning key AND value pairs
/// in one walk (RFC §14.4 — a relational table read is a scan of the
/// table's primary-key prefix, and it needs the row bytes, not just
/// the keys). Phase-7.
///
/// Body: `[start_len:u16 LE][start…][end_len:u16 LE][end…]`
///       `[cursor:u64 LE][limit:u16 LE]`
///
/// An empty `end` means unbounded above; an empty `start` means
/// unbounded below, so an empty pair is a full scan. Results come back
/// as [`KV_RESULT_RANGE`].
///
/// Ordering is PROVIDER-DEFINED, exactly as it is for `KV_OP_SCAN`:
/// the disk provider walks the sorted key space, the memory provider
/// walks hash slots and filters. What both providers guarantee is
/// COMPLETENESS and RESUMABILITY — every live key in the span is
/// emitted exactly once across a cursor loop. Callers that need sorted
/// output must sort, and must not infer order from the disk provider
/// happening to supply it.
pub const KV_OP_RANGE_SCAN: u8 = 0x13;
/// Every VERSION in `[start, end)` whose revision falls in the
/// half-open-below window `(from, to]` — the watch-resume backfill
/// (RFC §15). Phase-2 slice C.
///
/// Body: `[start_len:u16 LE][start…][end_len:u16 LE][end…]`
///       `[from_rev:u64 LE][to_rev:u64 LE][cursor:u64 LE][limit:u16 LE]`
///
/// Distinct from `KV_OP_SCAN_AT` in the question it asks. `SCAN_AT`
/// asks what the database LOOKED LIKE at one revision and returns one
/// winning version per key. This asks what HAPPENED across a window: a
/// key written three times inside it produced three events and yields
/// three entries, and a delete yields a tombstone entry rather than
/// nothing. A resumed watch stream that collapsed those would disagree
/// with a continuous one, which is the bug this op exists to avoid.
///
/// `KV_RESULT_COMPACTED` when `from` is below the retained floor — the
/// window is genuinely gone, and serving the surviving tail would be a
/// silently partial history the caller could not detect.
pub const KV_OP_SCAN_VERSIONS: u8 = 0x14;
/// Explicit partition addressing (Phase 4 lifecycle bootstrap only).
///
/// Body: `[partition_id:u16 LE][inner_op:u8][inner_body…]`
///
/// The router (ordered mode, [`PROTO_INTERNAL_LIFECYCLE`] ingress
/// only) unwraps this, dispatches `inner_op` + `inner_body` to the
/// named partition, and the worker never sees the wrapper. It exists
/// because a split's child-bootstrap writes must land in the TARGET
/// partition while the routing map still — correctly — names the
/// parent as the span's owner: publication comes after bootstrap, and
/// routing by the map would write the copy back into the parent. Any
/// other ingress carrying it refuses with [`KV_RESULT_CROSS_RANGE`]:
/// a client that can pick its partition can silently split a keyspace
/// in two.
pub const KV_OP_TARGETED: u8 = 0x15;

// ── Cross-range transactions (§13, Phase 5) ────────────────────────────
//
// Three ops turn a single-range `KV_OP_TXN` into a two-phase one across
// participants. They exist as separate ops rather than as flags on
// `KV_OP_TXN` because each is a distinct replicated decision with its
// own durability point: staging intents, recording the outcome, and
// making the outcome visible are three commits, not one.

/// Phase one: evaluate this participant's comparisons and STAGE its
/// writes as intents, without making them visible.
///
/// Body: `[txn_id:u128 LE][home_range_id:16][provisional_ts:u64 LE]
/// [epoch:u32 LE][txn_body…]`, where `txn_body` is exactly a
/// [`KV_OP_TXN`] body carrying only the ops that belong to THIS range.
///
/// Replies [`KV_RESULT_OK`] (prepared — every comparison held and every
/// intent is staged), [`KV_RESULT_CAS_FAILED`] (a comparison failed, or
/// another transaction already holds an intent on one of these keys:
/// either way this participant votes to abort and has staged nothing),
/// or [`KV_RESULT_INTERNAL`].
///
/// Prepare is all-or-nothing WITHIN the participant: a partial stage
/// would leave intents no coordinator knows to resolve.
pub const KV_OP_TXN_PREPARE: u8 = 0x16;

/// Phase two: make this participant's staged intents real, or drop
/// them.
///
/// Body: `[txn_id:u128 LE][home_range_id:16][epoch:u32 LE]
/// [committed:u8][commit_ts:u64 LE][key_count:u16 LE]` then
/// `[key_len:u16 LE][key]` per key.
///
/// The key list comes from the coordinator rather than from a scan: the
/// coordinator is the only party that knows the whole write set, and a
/// scan would make resolution cost proportional to the range instead of
/// to the transaction.
///
/// Replies [`KV_RESULT_INTEGER`] with the number of intents resolved.
/// Intents that do not match the named transaction, or that belong to a
/// different epoch, are LEFT IN PLACE and not counted — resolving them
/// would decide someone else's transaction.
///
/// Idempotent: re-delivering a resolve for already-resolved keys finds
/// no intents and resolves zero, which is the correct answer, not an
/// error. Redelivery is expected — this op is retried until acked.
pub const KV_OP_TXN_RESOLVE: u8 = 0x17;

/// Write or advance the home transaction record — the single authority
/// on whether a transaction committed.
///
/// Body: an encoded `TransactionRecord`.
///
/// The op refuses any status move the §13.2 lattice forbids, in
/// particular `Committed → Aborted`: a stale coordinator retry, an
/// expiry sweeper working from an old read, and a recovered participant
/// replaying an abort must all bounce off this. Replies
/// [`KV_RESULT_OK`], or [`KV_RESULT_CAS_FAILED`] for an illegal move.
///
/// Reading the record back is an ordinary `KV_OP_GET` on the same key,
/// which is what a helping reader does.
pub const KV_OP_TXN_RECORD: u8 = 0x18;

/// Execute an inner op EXACTLY ONCE under a caller-supplied identity
/// (§14, Phase 9).
///
/// Body: `[idempotency_key:u64 LE][inner_op:u8][inner_body…]`
///
/// The engine records, atomically with the mutation, that this identity
/// committed and what it answered. A redelivery of the same identity
/// replays the recorded answer and does NOT re-execute — which is the
/// whole point: `INCR` twice is a different database than `INCR` once,
/// and a client whose commit reply was lost cannot tell the two apart
/// without this.
///
/// Replies with the inner op's own result, whether executed now or
/// replayed from the record. A caller therefore cannot distinguish "I
/// did it" from "you already did it", and deliberately so: the answer
/// is the same because the STATE is the same, and inventing a
/// distinction would tempt callers to branch on it.
///
/// The record is what `data_surface::interpret_lookup` reads to
/// separate `DidNotHappen` from `Indeterminate`. A miss above the
/// retention floor is proof the op never committed; at or below it,
/// the engine has forgotten and must say so rather than guess.
pub const KV_OP_IDEMPOTENT: u8 = 0x19;

/// Live-rows snapshot scan WITH per-row commit metadata
/// — the backfill read (§8): every live (winning, non-tombstone,
/// unexpired) row in `[start, end)` as of `at_rev` (`0` = latest),
/// each with the `mod_revision` and MVCC `commit_ts` of the version
/// that produced it, so a backfilled event's identity is stable if the
/// same row is ever re-delivered.
///
/// Body: `[at_rev:u64 LE][start_len:u16 LE][start…][end_len:u16 LE]
/// [end…][cursor:u64 LE][limit:u16 LE]` (an empty `end` = unbounded
/// within the identity, as in [`KV_OP_RANGE_SCAN`]).
///
/// Answers a [`KV_RESULT_VERSIONS`] body whose entries all carry
/// `kind = `[`VERSION_KIND_PUT`], or [`KV_RESULT_COMPACTED`] when the
/// provider does not retain `at_rev`.
pub const KV_OP_SNAPSHOT_VERSIONS: u8 = 0x1A;

// ── KV op-specific body shapes ─────────────────────────────────────────
//
// The router and worker share these shapes via the `body` payload of
// MSG_KV_COMMAND. The anchor's job on the request side is to produce
// the body shape per `op`; the worker's job on the response side is to
// emit the result body shape per `result` (see KV_RESULT_* above).
//
// Body layouts by `op`:
//
//   KV_OP_GET / KV_OP_EXISTS / KV_OP_STRLEN
//     [key_len:u16 LE][key…]
//
//   KV_OP_PUT
//     [key_len:u16 LE][key…][value_len:u32 LE][value…]
//     [put_flags:u8] (bit0 = NX, bit1 = XX, bit2 = GET-and-return-prev,
//                    bit3 = KEEPTTL)
//     [expiry_ms:u64 LE] (0 = no expiry)
//
//   KV_OP_DELETE
//     [key_count:u16 LE] then per key: [key_len:u16 LE][key…]
//
//   KV_OP_INCR / KV_OP_DECR
//     [key_len:u16 LE][key…][delta:i64 LE]
//
//   KV_OP_APPEND
//     [key_len:u16 LE][key…][value_len:u32 LE][value…]
//
//   KV_OP_PREPEND
//     [key_len:u16 LE][key…][value_len:u32 LE][value…]
//     (Same wire shape as APPEND; new value = prefix || existing.)
//
//   KV_OP_MGET
//     [key_count:u16 LE] then per key: [key_len:u16 LE][key…]
//
//   KV_OP_MSET
//     [pair_count:u16 LE] then per pair:
//       [key_len:u16 LE][key…][value_len:u32 LE][value…]
//
//   KV_OP_SCAN
//     [cursor:u64 LE][limit:u16 LE]
//     (returns KV_RESULT_ARRAY with the matched keys; the next cursor
//      is stamped into the leading 8 bytes of the array body before
//      the [count:u16 LE] header.)
//
//   KV_OP_FLUSH
//     empty (flush the whole KPG keyspace)
//
//   KV_OP_GET_AT
//     [revision:u64 LE][key_len:u16 LE][key…]
//     Historical point read: the value of `key` as of MVCC revision
//     `revision` (§10 "latest and historical point reads", §20
//     "snapshot: serve at the supplied MVCC timestamp"). `revision`
//     of 0 means "latest" and is byte-identical to KV_OP_GET.
//     Visibility is the winning version whose assigned revision is
//     `<= revision`; if that version is a tombstone the key reads
//     NOT_FOUND, which is how "deleted at R" is observable before and
//     after R. A provider that cannot answer at `revision` — the disk
//     provider below its committed GC floor, or the memory provider
//     for ANY revision below its current one, since it retains no
//     history — returns `KV_RESULT_COMPACTED`. It never guesses and
//     never downgrades to latest (RFC §9: providers expose equivalent
//     logical state where they can and fail closed where they cannot).
//
//   KV_OP_SCAN_AT
//     [revision:u64 LE][cursor:u64 LE][limit:u16 LE]
//     Historical scan: the KV_OP_SCAN walk as of `revision`, same
//     `KV_RESULT_SCAN_CURSOR` reply shape and the same provider-defined
//     opaque cursor. `revision` of 0 means "latest". Same fail-closed
//     `KV_RESULT_COMPACTED` rule as KV_OP_GET_AT.
//
//   KV_OP_RANGE_SCAN
//     [start_len:u16 LE][start…][end_len:u16 LE][end…]
//     [cursor:u64 LE][limit:u16 LE]
//     Half-open `[start, end)` walk returning KEY AND VALUE pairs as
//     `KV_RESULT_RANGE`. An empty `end` is unbounded above and an empty
//     `start` unbounded below, so an empty/empty pair is a full scan
//     that differs from KV_OP_SCAN only in carrying values.
//     Completeness and resumability are guaranteed; ORDER IS NOT — see
//     the KV_OP_RANGE_SCAN doc comment. `limit` bounds the entry count
//     and the output buffer bounds the byte count, so a page can come
//     back short of `limit` with a non-zero cursor; a caller loops
//     until the cursor is 0 rather than until a page is short.
//
//   KV_OP_CAS
//     [key_len:u16 LE][key…][witness_revision:u64 LE]
//     [value_len:u32 LE][value…]
//     Atomic compare-and-set: if the record at `key` has
//     `mod_revision == witness_revision`, replace its value (and bump
//     `mod_revision`); otherwise return `KV_RESULT_CAS_FAILED`. A
//     witness of `0` (with no existing key) acts as a
//     create-if-absent: the put succeeds only when the key does not
//     exist yet. CAS does not touch TTL — the prior `expiry_ms` is
//     preserved across the swap; a non-zero witness with no matching
//     record returns `KV_RESULT_CAS_FAILED`.
//
//   KV_OP_TXN
//     [cmp_count:u16 LE] then per comparison:
//       [cmp_op:u8][key_len:u16 LE][key…][witness:u64 LE]
//     [then_count:u16 LE] then per op: [op:u8][body_len:u16 LE][body…]
//     [else_count:u16 LE] then per op: [op:u8][body_len:u16 LE][body…]
//     Single-KPG transactional eval. Every comparison must match
//     (`mod_revision` op `witness`) for the THEN branch to fire;
//     otherwise the ELSE branch fires. Comparisons against absent
//     keys treat `mod_revision` as `0`. The op list of the selected
//     branch runs in order; each op's result body is captured into the
//     `KV_RESULT_TXN` reply body so the anchor can re-encode per-op
//     responses (etcd v3 Txn → ResponseHeader + ResponseOp[]). Cross-
//     branch state is not visible: a comparison reads pre-branch
//     state, then THEN/ELSE ops apply sequentially to a single store
//     view (no nested rollback).

pub const TXN_CMP_MOD_EQUAL: u8 = 0x01;
pub const TXN_CMP_MOD_NOT_EQUAL: u8 = 0x02;
pub const TXN_CMP_MOD_GREATER: u8 = 0x03;
pub const TXN_CMP_MOD_LESS: u8 = 0x04;

pub const PUT_FLAG_NX: u8 = 0x01;
pub const PUT_FLAG_XX: u8 = 0x02;
pub const PUT_FLAG_GET: u8 = 0x04;
pub const PUT_FLAG_KEEPTTL: u8 = 0x08;

// ── KV result kinds ────────────────────────────────────────────────────
//
// Anchors map these protocol-neutral codes onto their respective wire
// formats. The shape of `body` varies by code:
//
//   KV_RESULT_OK            body empty → +OK\r\n; body non-empty → bulk string
//   KV_RESULT_NOT_FOUND     body empty → null bulk ($-1\r\n)
//   KV_RESULT_EXISTS        body empty → :1\r\n (boolean true marker)
//   KV_RESULT_WRONG_TYPE    body empty → WRONGTYPE error
//   KV_RESULT_CAS_FAILED    body empty → null bulk (Redis NX/XX semantics)
//   KV_RESULT_QUOTA         body empty → BUSY error
//   KV_RESULT_UNAUTH        body empty → NOAUTH error
//   KV_RESULT_DIRTY_EPOCH   body empty → MOVED error
//   KV_RESULT_LIN_BOUND     body empty → CLUSTERDOWN error
//   KV_RESULT_INTEGER       body = 8-byte LE i64 → :N\r\n
//   KV_RESULT_ARRAY         body = [count:u16 LE][element-len:u32 LE
//                                   element-bytes...]* — element-len
//                           = 0xFFFFFFFF marks null bulk in the array
//   KV_RESULT_INTERNAL      body empty → ERR internal error

pub const KV_RESULT_OK: u8 = 0x00;
pub const KV_RESULT_NOT_FOUND: u8 = 0x01;
pub const KV_RESULT_EXISTS: u8 = 0x02;
pub const KV_RESULT_WRONG_TYPE: u8 = 0x03;
pub const KV_RESULT_CAS_FAILED: u8 = 0x04;
pub const KV_RESULT_QUOTA: u8 = 0x05;
pub const KV_RESULT_UNAUTH: u8 = 0x06;
pub const KV_RESULT_DIRTY_EPOCH: u8 = 0x07;
pub const KV_RESULT_LIN_BOUND: u8 = 0x08;
pub const KV_RESULT_INTEGER: u8 = 0x09;
pub const KV_RESULT_ARRAY: u8 = 0x0A;
/// Body shape: `[next_cursor:u64 LE][count:u16 LE]` then
/// `[element-len:u32 LE][element-bytes...]*`. Used only by SCAN-class
/// ops so the anchor can emit a RESP `*2\r\n` reply of `(cursor,
/// keys[])` without having to remember the op per corr_id.
pub const KV_RESULT_SCAN_CURSOR: u8 = 0x0B;
/// Body shape: `[succeeded:u8][op_count:u16 LE]` then per op result:
/// `[result_code:u8][body_len:u16 LE][body…]`. Emitted by `KV_OP_TXN`
/// so the anchor can render etcd `TxnResponse` (or RESP `EXEC` reply)
/// with per-op fidelity. `succeeded` is `1` when every comparison
/// matched (THEN ran) and `0` when at least one comparison failed
/// (ELSE ran). Per-op bodies use the same layouts as if the op had
/// been issued standalone.
pub const KV_RESULT_TXN: u8 = 0x0C;
/// The requested MVCC revision is not retained by this materialization
/// provider, so the read CANNOT be answered — body empty.
///
/// This is a typed refusal, never an approximation: the alternative
/// (serving latest, or serving empty) would be a wrong answer to a
/// historical read, and RFC §21 invariant 14 requires a typed failure
/// rather than a silently weaker mode. Two things produce it:
///
/// - the disk provider asked for a revision below its committed GC
///   floor (§18) — the history was physically reclaimed;
/// - the memory provider asked for ANY revision below its current one:
///   it materializes only current values, so it has no history to
///   serve and says so instead of pretending.
///
/// Adapters map it to their protocol's compacted-revision error —
/// etcd's `ErrCompacted` (gRPC `OUT_OF_RANGE` = 11).
pub const KV_RESULT_COMPACTED: u8 = 0x0D;
/// Body shape: `[next_cursor:u64 LE][count:u16 LE]` then per entry
/// `[key_len:u16 LE][key…][value_len:u32 LE][value…]`. Emitted by
/// [`KV_OP_RANGE_SCAN`] only.
///
/// This is the one scan result that carries VALUES as well as keys, and
/// that is the whole reason it exists. `KV_RESULT_SCAN_CURSOR` returns
/// keys because its callers (Redis SCAN, etcd key-only range) then
/// issue their own point reads. A relational table read cannot afford
/// that: `SELECT * FROM t` over N rows would become N+1 round trips
/// through the router, and each of those point reads would observe a
/// DIFFERENT revision, so the result set would not correspond to any
/// single state of the database. Returning pairs from one walk keeps
/// the whole page on one MVCC revision.
///
/// `next_cursor` is `0` when the range is exhausted, matching the
/// SCAN-class convention so a caller can loop on the same rule.
pub const KV_RESULT_RANGE: u8 = 0x0E;
/// Body shape: `[next_cursor:u64 LE][count:u16 LE]` then per entry
/// `[revision:u64 LE][commit_ts:u64 LE][kind:u8][key_len:u16 LE][key…]
/// [value_len:u32 LE][value…]`. Emitted by [`KV_OP_SCAN_VERSIONS`] only.
///
/// `kind` is [`VERSION_KIND_PUT`] or [`VERSION_KIND_DELETE`]. A delete
/// entry carries a zero-length value: the event is that the key went
/// away, and there is no value to report.
///
/// Entries carry their own `revision` because that is the ONLY thing a
/// resuming watcher can checkpoint on. It acknowledges up to a revision
/// and resumes above it; without a per-entry revision it would have to
/// re-request the whole window after every disconnect.
///
/// `commit_ts` is the version's MVCC commit timestamp:
/// the cluster-wide ordering domain that survives range splits and
/// merges, where `revision` is only the per-partition apply order.
/// `0` = the write happened with no established timestamp authority.
pub const KV_RESULT_VERSIONS: u8 = 0x0F;

/// `kind` values in a [`KV_RESULT_VERSIONS`] entry.
pub const VERSION_KIND_PUT: u8 = 0x00;
pub const VERSION_KIND_DELETE: u8 = 0x01;
/// The op's key set spans more than one range on an ordered
/// multi-range map and the op has no cross-range execution: multi-key
/// writes and transactions need the Phase 5 coordinator, and a
/// version-window scan cannot merge two partitions' independent
/// revision spaces at all. A typed refusal, body empty — executing on
/// ONE of the ranges would silently drop the rest of the key set,
/// which is a wrong answer, not a degraded one.
pub const KV_RESULT_CROSS_RANGE: u8 = 0x10;

/// The key carries an unresolved write intent: a cross-range
/// transaction has staged a write here and its outcome is not yet
/// decided, or is decided somewhere this reader has not consulted.
///
/// Body: `[txn_id:u128 LE][home_range_id:16]` — the pointer to the
/// deciding authority, so the caller can go and resolve it rather than
/// only knowing that it must.
///
/// A typed, RETRYABLE refusal, and deliberately not any of the answers
/// that would be easier to give. Reporting the pre-intent value would
/// be a stale read; reporting absent would invent a delete; waiting
/// inside the worker would block a single-threaded apply loop behind a
/// transaction that may itself be waiting on this range. The reader is
/// told exactly who decides, and asks them.
pub const KV_RESULT_TXN_PENDING: u8 = 0x11;

/// The range's home partition lives in a DIFFERENT Clustor cluster
/// domain than the one this router serves (§11.3, §26.5). A
/// `partition_id` is cluster-local, so a deployment that has grown past
/// one domain's partition-ID space routes across several domains, and a
/// key can resolve to a domain this router cannot physically reach.
///
/// Body: `[cluster_domain_id:u32 LE]` — the domain that DOES own the
/// key, so the caller can redirect to that domain's gateway rather than
/// only learning it asked the wrong one. The structured redirect §12.1
/// calls for, at the domain granularity.
///
/// A typed refusal, never a silent drop and never a guess: serving the
/// key from the local domain would answer for data this domain does not
/// hold. Widening the wire `partition_id` (which would remove the need
/// for domains) is a Clustor protocol migration, deliberately postponed
/// (§28, §29) — domains are the scaling step that comes first.
pub const KV_RESULT_CROSS_DOMAIN: u8 = 0x12;

pub const KV_RESULT_INTERNAL: u8 = 0xFF;

/// Sentinel `element-len` value in `KV_RESULT_ARRAY` payloads that
/// represents a NULL bulk element (i.e. RESP `$-1\r\n` inside an
/// `*N\r\n` array). Concrete elements use the actual byte count.
pub const KV_ARRAY_ELEMENT_NULL: u32 = 0xFFFF_FFFF;

// ── Adapter consistency requirements ───────────────────────────────────

pub const REQ_SERIALIZABLE: u8 = 0x00;
pub const REQ_LINEARIZABLE: u8 = 0x01;
pub const REQ_CAS_FENCE: u8 = 0x02;
pub const REQ_TXN_FENCE: u8 = 0x03;

// ── Auth / decision kinds ──────────────────────────────────────────────

pub const AUTH_DECISION_ALLOW: u8 = 0x00;
pub const AUTH_DECISION_DENY: u8 = 0x01;
pub const AUTH_DECISION_AUDIT: u8 = 0x02;

// ── Quota throttle reasons (mirrored into adapter error envelopes) ─────

pub const THROTTLE_REQUEST_RATE: u8 = 0x01;
pub const THROTTLE_BYTE_RATE: u8 = 0x02;
pub const THROTTLE_BURST: u8 = 0x03;
pub const THROTTLE_OVERAGE: u8 = 0x04;
