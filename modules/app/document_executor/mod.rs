//! document_executor — the shared document execution engine.
//!
//! Runs canonical document operations (`MSG_DOC_REQUEST`, codec in
//! `modules/common/doc_exec.rs`) against collections as their
//! descriptors declare them, and answers `MSG_DOC_RESPONSE`. Dialects —
//! `doc_edge_anchor` for MongoDB, the Kubernetes front — translate their
//! wire into these operations and render the answers; they never touch
//! a KV key, and this module never sees BSON or JSON. It stands to the
//! document dialects exactly as `relational_executor` stands to the SQL
//! connectors: one executor, so two dialects cannot come to disagree
//! about what an operation on a collection means.
//!
//! ## Storage
//!
//! Every read and write leaves as a `MSG_KV_REQUEST` on the compute
//! ingress (`PROTO_INTERNAL_SQL`, the router's in-graph compute pair, or
//! `lattice_data_client` when compute is placed apart) and takes its
//! durability from the same path a Redis `SET` does:
//!
//! - collection descriptors under `KS_DOCUMENT_CATALOG`
//!   (`doc_catalog.rs`), ids from an `INCR` allocator;
//! - documents under `KS_DOCUMENT_DATA` at
//!   `[collection][shard key][id key]` (`models::encode_document_data_key`,
//!   the id and shard values in `doc_ops::id_key`'s ordered form), each a
//!   `doc_tree` record;
//! - reads by `KV_OP_RANGE_META`, which answers each document with its
//!   mod revision and keeps a list on one snapshot revision across pages;
//! - every mutation ONE `KV_OP_TXN` compared on the revision it read —
//!   never an unfenced connector-side read followed by a write. A
//!   write that loses the race re-reads and
//!   re-applies, up to [`MAX_CAS_RETRIES`], unless the request stated the
//!   revision it expects — then the loss is the client's `CONFLICT`.
//!
//! ## One operation at a time
//!
//! As in `relational_executor`: one operation in flight, a short queue
//! behind it (`BUSY` when full), fixed memory, a watchdog that answers
//! `TIMEOUT` when a KV reply never comes. Every operation re-reads its
//! collection's descriptor — one extra round trip, and never a stale
//! descriptor.
//!
//! ## What is declared, not coded
//!
//! Ids, shard keys, identity paths, create stamps, generated ids, values
//! kept across a replace, the revision path answers carry, and document
//! limits all come from the descriptor (`doc_catalog.rs`). The logic is
//! `doc_ops.rs`, host-tested; this module sequences it over KV.

#![no_std]
#![allow(
    unexpected_cfgs,
    reason = "`big` is document_executor_big's feature; this file is also document_executor, whose build declares none"
)]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    clippy::duplicate_mod,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, ABI fns carry a fixed arity, and the PIC build #[path]-remounts shared SDK/common code"
)]
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/doc_buf.rs"]
mod doc_buf;
use doc_buf::DocBuf;

#[path = "../../common/models.rs"]
mod models;

// `doc_exec` mounts `doc_tree`, `doc_catalog` and `doc_ops`: one mount,
// one set of types.
#[path = "../../common/doc_exec.rs"]
mod doc_exec;

#[path = "../../common/kv_large.rs"]
mod kv_large;

// A JSON collection's records (`doc_codec`), read and written as a store's
// other clients spell them: mounted beside `doc_exec`'s modules.
#[path = "../../common/json_tree.rs"]
mod json_tree;

#[path = "../../common/doc_schema.rs"]
mod doc_schema;

#[path = "../../common/doc_render.rs"]
mod doc_render;

#[path = "../../common/doc_codec.rs"]
mod doc_codec;

use doc_exec::doc_fields;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use doc_exec::doc_catalog::{
    self, catalog_name_key, catalog_names_prefix, catalog_seq_key, set_descriptor_id,
    set_index_ids, Descriptor, DescriptorWriter, FORM_TEXT, GEN_NOW, MAX_DESCRIPTOR_LEN,
};
use doc_exec::doc_fields::{
    self as fields, Actor, Applied, FieldError, Owners, Schema, Scratch, Unowned, NO_OWNERS,
};
use doc_exec::doc_ops::{
    self, apply_mutations, carry, carry_id, changed_path, doc_id, generate, generated_name,
    guard_holds, id_is_usable, id_key, id_key_of, index_values, json_patch, marked_deleting,
    matches, merge_patch, project, set_id, Buffers, Edit, Keyed, OpError, Reason, StampContext,
};
use doc_exec::doc_tree::{
    self, lookup, owned_record_head, record_parts, Path, Value, Writer, FORMAT_V1,
};
use doc_exec::*;
use types::{
    KV_OP_INCR, KV_OP_RANGE_META, KV_OP_SCAN_VERSIONS, KV_OP_TXN, KV_OP_TXN_STAGED,
    KV_RESULT_CAS_FAILED, KV_RESULT_COMPACTED, KV_RESULT_FUTURE_REV, KV_RESULT_INTEGER,
    KV_RESULT_KVS, KV_RESULT_NOT_FOUND, KV_RESULT_OK, KV_RESULT_QUOTA, KV_RESULT_TOO_LARGE,
    KV_RESULT_TXN, KV_RESULT_VERSIONS, PROTO_INTERNAL_SQL, TXN_CMP_MOD_EQUAL,
};
use wire::{MSG_DOC_REQUEST, MSG_DOC_RESPONSE, MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Capacities ────────────────────────────────────────────────────────

/// Requests waiting behind the one in flight.
const QUEUE_DEPTH: usize = 4;
/// The largest request and answer this build takes: one channel frame;
/// in the big build a message of `wire::MSG_PART` parts up to its
/// document policy ([`MAX_DOCUMENT`]).
#[cfg(not(feature = "big"))]
const REQUEST_MAX: usize = DOC_REQUEST_MAX;
#[cfg(not(feature = "big"))]
const RESPONSE_MAX: usize = DOC_RESPONSE_MAX;
/// The big build's document policy: the decided Kubernetes object bound
/// (etcd's 1.5 MiB).
#[cfg(feature = "big")]
const MAX_DOCUMENT: usize = 1572864;
#[cfg(feature = "big")]
const REQUEST_MAX: usize = MAX_DOCUMENT + 65536;
#[cfg(feature = "big")]
const RESPONSE_MAX: usize = MAX_DOCUMENT + 65536;
/// A document being built or edited (two of these alternate).
#[cfg(not(feature = "big"))]
const DOC_BUF: usize = 16384;
#[cfg(feature = "big")]
const DOC_BUF: usize = MAX_RECORD + 65536;
/// An ownership transition's field sets. They grow with a document's
/// fields — a ConfigMap of 22,000 keys holds ~300 KB of paths — and no
/// set outgrows the record it is stored in, so they are bounded as the
/// document is.
const OWN_BUF: usize = DOC_BUF;
/// Inbound envelopes: a request, or a KV answer.
const SCRATCH_BUF: usize = 24576;
/// A held scan page: a KV answer as it came, or as the large-value layer
/// expanded it.
const PAGE_BUF: usize = if LG_OUT > SCRATCH_BUF {
    LG_OUT
} else {
    SCRATCH_BUF
};
/// Outbound KV request (envelope, head, body) as the executor builds it, before the large-value layer
/// rewrites it: a transaction carrying a whole record (`MAX_RECORD`) and
/// its index work.
const ENV_BUF: usize = wire::ENVELOPE_HDR + 18 + MAX_RECORD + 4096;
/// Outbound response.
const RESP_BUF: usize = wire::ENVELOPE_HDR + RESPONSE_MAX;
/// The request in flight.
const REQ_BUF: usize = REQUEST_MAX;

/// The big build's heap: every `DocBuf` in the state, and the
/// allocator's rounding. (Sums the fields' sizes; a field added without
/// its share here fails `module_new`, never corrupts.)
#[cfg(feature = "big")]
const ARENA_BYTES: usize = 2 * REQ_BUF
    + 9 * DOC_BUF
    + 3 * MAX_RECORD
    + CODEC_SCRATCH
    + 5 * OWN_BUF
    + 3 * ENV_BUF
    + RESP_BUF
    + LG_OUT
    + PAGE_BUF
    + 64 * 1024;
#[cfg(feature = "big")]
declare_module_arena_bytes!(ARENA_BYTES);
/// A KV key: keyspace prefix and the model key.
const KEY_MAX: usize = 4 + models::DOCUMENT_KEY_MAX_LEN;
/// Documents per list or filter page.
const SCAN_PAGE: usize = 16;
/// Re-reads after a lost compare-and-set before answering `CONFLICT`.
const MAX_CAS_RETRIES: usize = 3;
/// The single database (composition-level, as on the SQL side).
const DATABASE_ID: u32 = 0;

const WEDGE_TIMEOUT_MS: u64 = 10_000;
const WEDGE_TIMER_MS: u32 = 10_500;
const TIMER_CONTRACT: u32 = 0x0006;
const TIMER_CREATE: u32 = 0x0604;
const TIMER_SET: u32 = 0x0605;
const TIMER_CANCEL: u32 = 0x0606;

// ── Phases: what the in-flight KV reply belongs to ─────────────────────

const P_IDLE: u8 = 0;
/// The collection's descriptor.
const P_DESC: u8 = 1;
/// Collection create: the id allocator.
const P_CC_ALLOC: u8 = 2;
/// Collection create: the descriptor write (absent-compared).
const P_CC_PUT: u8 = 3;
/// Collection drop: the descriptor delete.
const P_CD_DEL: u8 = 4;
/// Collection drop: a page of the collection's documents.
const P_CD_SCAN: u8 = 5;
/// Collection drop: that page's delete.
const P_CD_PAGE_DEL: u8 = 6;
/// Collection list: a page of descriptors.
const P_CL_SCAN: u8 = 7;
/// Insert: the transaction.
const P_INS_TXN: u8 = 8;
/// Insert refused: which document's id was taken.
const P_INS_PROBE: u8 = 9;
/// One document by id (get, replace, update, delete).
const P_READ: u8 = 10;
/// One document's write (replace, update, delete, upsert).
const P_WRITE: u8 = 11;
/// A page of a list or a filtered update/delete.
const P_SCAN: u8 = 12;
/// A filtered update/delete writing one matched document.
const P_SCAN_WRITE: u8 = 13;
/// A watch's first pass: which revisions fit the answer.
const P_WATCH_SIZE: u8 = 14;
/// A watch's second pass: the events up to the cut.
const P_WATCH_EMIT: u8 = 15;
/// A refused write, first question: did the document change?
const P_WHY_DOC: u8 = 16;
/// Second question: did the collection's descriptor change?
const P_WHY_DESC: u8 = 17;
/// Index create: the id; the descriptor with the index building; a page
/// of documents; one document's entries; why that write refused (the
/// document, the descriptor); the index ready; a failed build withdrawn.
const P_IX_ALLOC: u8 = 18;
const P_IX_DESC: u8 = 19;
const P_IX_SCAN: u8 = 20;
const P_IX_WRITE: u8 = 21;
const P_IX_WHY_DOC: u8 = 22;
const P_IX_WHY_DESC: u8 = 23;
const P_IX_READY: u8 = 24;
const P_IX_ABORT: u8 = 25;
/// Index drop: the descriptor without it.
const P_IX_DROP_DESC: u8 = 26;
/// A planned scan: one candidate document, fetched by id.
const P_PLAN_FETCH: u8 = 27;
/// The large-value layer: a chunk uploaded, a chunk fetched.
const P_LG_UP: u8 = 28;
const P_LG_FETCH: u8 = 29;

/// Revisions a watch's first pass keeps (the smallest ones).
const WATCH_CANDIDATES: usize = 256;
/// Versions per page of a watch pass.
const WATCH_PAGE: usize = 64;

/// Watches followed at once (`FLAG_FOLLOW`).
const MAX_FOLLOWS: usize = 16;
/// A followed watch's request, kept to re-run it from its cursor.
const FOLLOW_REQ_MAX: usize = 1024;
/// How often every followed watch is read again whatever notified it: a
/// lost change notification costs at most this much latency.
const FOLLOW_HEAL_MS: u64 = 1000;

/// A followed watch: whose it is, the request to re-run, the span its
/// changes fall in, and where the next read starts.
#[derive(Clone, Copy)]
struct Follow {
    live: bool,
    dirty: bool,
    src: u8,
    conn: u8,
    corr: u64,
    cursor: u64,
    req_len: u16,
    req: [u8; FOLLOW_REQ_MAX],
    start_len: u16,
    start: [u8; KEY_MAX],
    end_len: u16,
    end: [u8; KEY_MAX],
}

#[derive(Clone, Copy)]
struct Queued {
    len: u32,
    src: u8,
    buf: [u8; DOC_REQUEST_MAX],
}

impl Queued {
    const fn empty() -> Self {
        Queued {
            len: 0,
            src: 0,
            buf: [0; DOC_REQUEST_MAX],
        }
    }
}

#[repr(C)]
struct ExecState {
    syscalls: *const SyscallTable,
    // Inputs: dialect_in[0], dialect2_in[1], kv_in[2]
    // Outputs: dialect_out[0], dialect2_out[1], kv_out[2], metrics[3]
    d_in: [i32; 2],
    d_out: [i32; 2],
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,
    timer_fd: i32,
    inflight_since_ms: u64,
    kv_corr: u64,
    step_ctr: u32,

    queue: [Queued; QUEUE_DEPTH],
    queue_len: usize,

    // ── The job in flight ──
    src: u8,
    req_len: u32,
    req: DocBuf<REQ_BUF>,
    phase: u8,
    desc_len: u16,
    desc: [u8; MAX_DESCRIPTOR_LEN],
    /// Insert: the document being probed after a refused transaction.
    doc_index: u16,
    retries: u8,
    matched: u64,
    modified: u64,
    /// The mod revision of the document read (0: absent).
    read_rev: u64,
    /// A list's snapshot revision (0 until its first page answers).
    snap_rev: u64,
    /// Documents answered so far (lists).
    answered: u32,
    /// The key of the document being written or probed.
    key_len: u16,
    key: [u8; KEY_MAX],
    /// Where a scan resumes: after this key (empty: from the start).
    after_len: u16,
    after: [u8; KEY_MAX + 300],
    /// The held page of a filtered update/delete, and the next entry.
    page_len: u32,
    page_at: u16,
    page_more: bool,
    page: DocBuf<PAGE_BUF>,
    /// The stored document as read (a record's tree).
    stored_len: u32,
    stored: DocBuf<DOC_BUF>,
    buf_a: DocBuf<DOC_BUF>,
    buf_b: DocBuf<DOC_BUF>,
    /// The stored document's ownership record, beside `stored` (empty:
    /// none), and the one the write in flight stores with its document
    /// (empty: the stored one, carried).
    owners_len: u32,
    owners: DocBuf<MAX_RECORD>,
    next_owners_len: u32,
    next_owners: DocBuf<MAX_RECORD>,
    /// Room an ownership transition works in: three field sets and a path.
    own_a: DocBuf<OWN_BUF>,
    own_b: DocBuf<OWN_BUF>,
    own_c: DocBuf<OWN_BUF>,
    own_path: [u8; OWN_PATH],
    /// Room a JSON collection's records are read and written in.
    codec: Codec,
    /// The response under construction, at `resp[ENVELOPE_HDR..]`, and
    /// the bytes of documents the dialect can render (`FIELD_MAX_BYTES`).
    resp_len: usize,
    resp_budget: usize,
    /// Flags the answer carries beyond the outcome's (held, removed).
    resp_flags: u8,

    /// The store's revision when the job read its descriptor.
    store_rev: u64,
    /// The descriptor record's mod revision: every document write is
    /// compared on it, so a schema change (an index added) fences out a
    /// write that read the schema before it.
    desc_rev: u64,
    /// Diagnosing a refused write: a filtered write's document (not the
    /// job's own), an insert (no document to re-read), and whether the
    /// document had changed.
    why_scan: bool,
    why_insert: bool,
    why_doc_changed: bool,
    /// A drop's span in hand: 0 the documents, n the n-th index's
    /// entries; and, dropping one index, its id (0: the collection).
    drop_stage: u8,
    drop_only: u32,
    /// What a drop answers when its pages are done (a failed index build
    /// withdraws its entries, then refuses).
    drop_outcome: u8,
    /// A scan's plan, decided on its first page: the index it reads (0: the
    /// collection), whether unique, and the values it looks up; the index
    /// entry in hand (a resume starts after it).
    plan_decided: bool,
    plan_ix: u32,
    plan_unique: bool,
    plan_vlen: u16,
    plan_vals: [u8; doc_ops::MAX_INDEX_VALUES],
    plan_key_len: u16,
    plan_key: [u8; KEY_MAX + 300],
    /// An index being built, and the descriptor a write proposes.
    ix_id: u32,
    pend_len: u16,
    pend: [u8; MAX_DESCRIPTOR_LEN],
    /// A document transaction's comparisons and operations, assembled.
    tx_cmp: [u8; 2048],
    tx_ops: DocBuf<ENV_BUF>,

    // ── A watch in flight ──
    /// The window: changes after `w_from`, up to `w_hi` (the ceiling the
    /// first page pinned), and the cut the first pass chose.
    w_from: u64,
    w_hi: u64,
    w_cut: u64,
    w_cursor: u64,
    /// The first pass's smallest revisions and each event's size bound.
    w_cand: [(u64, u32); WATCH_CANDIDATES],
    w_n: usize,
    /// The smallest revision the first pass had to drop (u64::MAX: none).
    w_dropped: u64,
    resp_overflow: bool,
    resp: DocBuf<RESP_BUF>,

    env: DocBuf<ENV_BUF>,
    scratch: [u8; SCRATCH_BUF],

    // ── The large-value layer (`kv_large.rs`) ──
    /// The request as the executor wrote it (held while its chunks go
    /// out, or while a read's reply is expanded), its op, and the phase
    /// it continues in.
    lg_req_len: usize,
    lg_req: DocBuf<ENV_BUF>,
    lg_op: u8,
    lg_next: u8,
    /// The request in flight is a read whose reply may need expanding.
    lg_read: bool,
    lg_up: kv_large::Uploader,
    lg_ex: kv_large::Expander,
    /// The reply being expanded, its revision, and the expansion.
    lg_rev: u64,
    lg_out: DocBuf<LG_OUT>,
    /// Documents a phase holds while the buffers they came from are
    /// rewritten, one per role so no two held views share a buffer: the
    /// stored document read, a creation's source, the document about to
    /// be committed, the one an ownership transition rebuilt, a patch's
    /// result. (On the stack they were a whole document each.)
    hold_stored: DocBuf<DOC_BUF>,
    hold_src: DocBuf<DOC_BUF>,
    hold_doc: DocBuf<DOC_BUF>,
    hold_commit: DocBuf<DOC_BUF>,
    hold_patch: DocBuf<DOC_BUF>,

    // ── Messages past one frame (`wire::MSG_PART`) ──
    /// A request arriving in parts — one at a time, from either dialect —
    /// each dialect's reassembly, and a whole one waiting to become the
    /// job (0: none).
    inbox: DocBuf<REQ_BUF>,
    parts_in: [wire::Reassembly; 2],
    inbox_src: u8,
    inbox_ready: usize,
    /// The answer leaving: whole in one frame, or in parts; kept until
    /// its channel takes all of it. No job starts meanwhile (it would
    /// write `resp`).
    out_active: bool,
    out_dest: i32,
    out_len: usize,
    out_parts: wire::PartWriter,
    out_stream: u32,

    m_ops: u64,
    m_docs_read: u64,
    m_docs_written: u64,
    m_errors: u64,
    m_queue_rejects: u64,
    m_timeouts: u64,
    /// Change notifications (`MSG_WATCH_EVENT`, the worker's `mutations`).
    changes_in: i32,
    follows: [Follow; MAX_FOLLOWS],
    /// The followed watch whose read is the job in flight (index + 1; 0:
    /// none), the next to consider, and the last heal.
    follow_job: u8,
    follow_rr: u8,
    follow_heal_ms: u64,
    follow_buf: [u8; FOLLOW_REQ_MAX + wire::ENVELOPE_HDR + 16],
}

impl ExecState {
    fn busy(&self) -> bool {
        self.phase != P_IDLE
    }
}

// ── Envelope I/O ──────────────────────────────────────────────────────

unsafe fn read_envelope(sys: &SyscallTable, chan: i32, scratch: &mut [u8]) -> Option<(u8, usize)> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; wire::ENVELOPE_HDR];
    if (sys.channel_read)(chan, hdr.as_mut_ptr(), wire::ENVELOPE_HDR) < wire::ENVELOPE_HDR as i32 {
        return None;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len > scratch.len() {
        // Drain what cannot be held so the channel does not wedge on it;
        // the sender's watchdog answers the lost request.
        let mut sink = [0u8; 256];
        let mut left = payload_len;
        while left > 0 {
            let n = left.min(sink.len());
            if (sys.channel_read)(chan, sink.as_mut_ptr(), n) <= 0 {
                break;
            }
            left -= n;
        }
        return None;
    }
    if payload_len > 0
        && ((sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len) as usize) < payload_len
    {
        return None;
    }
    Some((hdr[0], payload_len))
}

unsafe fn write_envelope(
    sys: &SyscallTable,
    chan: i32,
    msg_type: u8,
    payload_len: usize,
    env: &mut [u8],
) -> bool {
    if chan < 0 || payload_len > u16::MAX as usize || wire::ENVELOPE_HDR + payload_len > env.len() {
        return false;
    }
    env[0] = msg_type;
    env[1] = (payload_len & 0xFF) as u8;
    env[2] = ((payload_len >> 8) & 0xFF) as u8;
    let total = wire::ENVELOPE_HDR + payload_len;
    (sys.channel_write)(chan, env.as_mut_ptr(), total) == total as i32
}

fn put(out: &mut [u8], at: &mut usize, b: &[u8]) -> Option<()> {
    let d = out.get_mut(*at..*at + b.len())?;
    d.copy_from_slice(b);
    *at += b.len();
    Some(())
}

// ── Messages (each its own function: PIC relocates no slice tables) ───

#[inline(never)]
fn msg_no_id() -> &'static [u8] {
    b"the document has no usable id"
}
#[inline(never)]
fn msg_other_id() -> &'static [u8] {
    b"the document names another id than the request"
}
#[inline(never)]
fn msg_id_mismatch() -> &'static [u8] {
    b"the document's id is not the id addressed"
}
#[inline(never)]
fn msg_identity() -> &'static [u8] {
    b"the operation would change an identity path"
}
#[inline(never)]
fn msg_not_container() -> &'static [u8] {
    b"the path runs through a value that is not an object or array"
}
#[inline(never)]
fn msg_not_number() -> &'static [u8] {
    b"the value is not a number"
}
#[inline(never)]
fn msg_not_array() -> &'static [u8] {
    b"the value is not an array"
}
#[inline(never)]
fn msg_overflow() -> &'static [u8] {
    b"integer overflow"
}
#[inline(never)]
fn msg_decimal() -> &'static [u8] {
    b"arithmetic on a decimal literal"
}
#[inline(never)]
fn msg_too_large() -> &'static [u8] {
    b"the document is larger than the collection allows"
}
#[inline(never)]
fn msg_too_large_write() -> &'static [u8] {
    b"the write is larger than one KV request carries"
}
#[inline(never)]
fn msg_too_deep() -> &'static [u8] {
    b"the document nests too deeply"
}
#[inline(never)]
fn msg_malformed() -> &'static [u8] {
    b"the operation cannot apply"
}
#[inline(never)]
fn msg_no_clock() -> &'static [u8] {
    b"no wall clock: a time stamp cannot be made"
}
#[inline(never)]
fn msg_no_entropy() -> &'static [u8] {
    b"no entropy: an identifier cannot be made"
}
#[inline(never)]
fn msg_precondition() -> &'static [u8] {
    b"a precondition did not hold"
}
#[inline(never)]
fn msg_revision() -> &'static [u8] {
    b"the document was modified: its revision is not the one stated"
}
#[inline(never)]
fn msg_exists() -> &'static [u8] {
    b"a document with this id exists"
}
#[inline(never)]
fn msg_unique() -> &'static [u8] {
    b"a unique index already holds this value"
}
#[inline(never)]
fn msg_index_exists() -> &'static [u8] {
    b"an index of this name exists with another definition"
}
#[inline(never)]
fn msg_json_index() -> &'static [u8] {
    b"a collection at the store's own keys keeps no index: its other writers could not"
}
#[inline(never)]
fn msg_no_manager() -> &'static [u8] {
    b"a write to a collection that tracks ownership names its manager"
}

fn msg_not_owned() -> &'static [u8] {
    b"apply needs a collection that tracks ownership"
}

fn msg_bad_prefix() -> &'static [u8] {
    b"an id prefix names a compound id's leading components, in an unsharded collection"
}

fn msg_bad_op() -> &'static [u8] {
    b"the operation needs fields the request does not carry"
}

// A refusal's reason, as text: one table, indexed by `Reason`'s
// declaration order (a `match` returning literals would be a table of
// absolute pointers, which a module image cannot hold).
name_table!(
    REASON_MESSAGES = [
        b"the path runs through a value that is not an object or array",
        b"the value is not a number",
        b"the value is not an array",
        b"integer overflow",
        b"arithmetic on a decimal literal",
        b"the document is larger than the collection allows",
        b"the document nests too deeply",
        b"the operation cannot apply",
        b"the patch names a path that does not exist",
        b"a test operation did not hold",
        b"the value cannot be indexed",
    ]
);

fn reason_message(r: Reason) -> &'static [u8] {
    REASON_MESSAGES.get(r as usize)
}

// ── Responses ─────────────────────────────────────────────────────────

/// Start the response for the job in flight.
fn resp_begin(exec: &mut ExecState) {
    let at = wire::ENVELOPE_HDR;
    let corr = u64::from_le_bytes(exec.req[0..8].try_into().unwrap_or([0; 8]));
    let conn = exec.req[13];
    exec.resp[at..at + 8].copy_from_slice(&corr.to_le_bytes());
    exec.resp[at + 8] = conn;
    exec.resp[at + 9] = OUT_OK;
    exec.resp[at + 10] = 0;
    exec.resp[at + 11..at + 19].copy_from_slice(&0u64.to_le_bytes());
    exec.resp_len = DOC_RESPONSE_HEAD;
    exec.resp_overflow = false;
    exec.resp_flags = 0;
}

/// Append one response field from its parts; `false` (and the sticky
/// overflow) when it does not fit.
fn resp_field(exec: &mut ExecState, tag: u8, parts: &[&[u8]]) -> bool {
    field_into(
        &mut exec.resp,
        &mut exec.resp_len,
        &mut exec.resp_overflow,
        tag,
        parts,
    )
}

/// [`resp_field`] over the response's own fields, so a part may borrow
/// another field of the state.
fn field_into(
    resp: &mut [u8],
    resp_len: &mut usize,
    overflow: &mut bool,
    tag: u8,
    parts: &[&[u8]],
) -> bool {
    let len: usize = parts.iter().map(|p| p.len()).sum();
    let at = wire::ENVELOPE_HDR + *resp_len;
    if *resp_len + FIELD_HEAD + len > RESPONSE_MAX {
        *overflow = true;
        return false;
    }
    resp[at] = tag;
    resp[at + 1..at + 5].copy_from_slice(&(len as u32).to_le_bytes());
    let mut p = at + 5;
    for part in parts {
        resp[p..p + part.len()].copy_from_slice(part);
        p += part.len();
    }
    *resp_len += FIELD_HEAD + len;
    true
}

/// Room left for one more field's body.
fn resp_room(exec: &ExecState) -> usize {
    RESPONSE_MAX.saturating_sub(exec.resp_len + FIELD_HEAD)
}

/// Send the response and end the job.
fn resp_send(exec: &mut ExecState, outcome: u8, flags: u8, revision: u64) {
    let mut flags = flags;
    if !follow_answered(exec, outcome, &mut flags, revision) {
        finish_job(exec);
        return;
    }
    let at = wire::ENVELOPE_HDR;
    exec.resp[at + 9] = outcome;
    let dry = req_flags(exec) & FLAG_DRY_RUN != 0;
    exec.resp[at + 10] = flags | exec.resp_flags | if dry { RFLAG_DRY_RUN } else { 0 };
    exec.resp[at + 11..at + 19].copy_from_slice(&revision.to_le_bytes());
    if outcome != OUT_OK {
        exec.m_errors = exec.m_errors.wrapping_add(1);
    }
    exec.out_dest = exec.d_out[(exec.src as usize) & 1];
    exec.out_len = exec.resp_len;
    exec.out_stream = exec.out_stream.wrapping_add(1);
    exec.out_parts = wire::PartWriter::new(exec.out_stream, MSG_DOC_RESPONSE, exec.resp_len);
    exec.out_active = true;
    flush_answer(exec);
    finish_job(exec);
}

/// Write the answer in `resp` while its channel takes it: one frame, or
/// its parts in order. What the channel refuses waits for the next step.
fn flush_answer(exec: &mut ExecState) {
    if !exec.out_active {
        return;
    }
    let sys = exec.syscalls;
    if sys.is_null() {
        return;
    }
    let (dest, len) = (exec.out_dest, exec.out_len);
    unsafe {
        if len <= wire::FRAME_PAYLOAD_MAX {
            if write_envelope(&*sys, dest, MSG_DOC_RESPONSE, len, &mut exec.resp) {
                exec.out_active = false;
            }
            return;
        }
        let mut frame = [0u8; wire::ENVELOPE_HDR + wire::FRAME_PAYLOAD_MAX];
        let at = wire::ENVELOPE_HDR;
        while !exec.out_parts.done() {
            let Some(msg) = exec.resp.get(at..at + len) else {
                exec.out_active = false;
                return;
            };
            let Some(n) = exec.out_parts.frame(msg, &mut frame) else {
                exec.out_active = false;
                return;
            };
            if dest < 0 || ((*sys).channel_write)(dest, frame.as_mut_ptr(), n) != n as i32 {
                return;
            }
            exec.out_parts.sent();
        }
        exec.out_active = false;
    }
}

/// One `MSG_PART` from dialect `src`: into the inbox, which holds one
/// request at a time; another request arriving in parts meanwhile is
/// answered busy from its first part (which begins with its head).
fn on_part(exec: &mut ExecState, src: u8, payload: &[u8]) {
    let s = (src & 1) as usize;
    let first = payload.get(4..8) == Some(&[0, 0, 0, 0][..]);
    let head = payload.get(wire::PART_HEAD..).unwrap_or(&[]);
    let other = s ^ 1;
    let taken = exec.inbox_ready != 0 || exec.parts_in[other].active();
    if first && taken {
        exec.parts_in[s].reset();
        exec.m_queue_rejects = exec.m_queue_rejects.wrapping_add(1);
        reject_now(exec, src, head, OUT_BUSY);
        return;
    }
    let r = {
        let ExecState {
            parts_in, inbox, ..
        } = &mut *exec;
        parts_in[s].take(payload, inbox)
    };
    match r {
        Ok(Some((MSG_DOC_REQUEST, n))) => {
            exec.inbox_ready = n;
            exec.inbox_src = src;
        }
        Ok(_) => {}
        // Past what this build takes: refused whole, from its first part.
        Err(wire::PartError::TooLarge) if first => {
            exec.m_errors = exec.m_errors.wrapping_add(1);
            reject_now(exec, src, head, OUT_TOO_LARGE);
        }
        // A broken sequence: the sender's watchdog answers it.
        Err(_) => {}
    }
}

/// Answer an outcome with an error field naming a path and why.
fn fail(exec: &mut ExecState, outcome: u8, index: u32, path: &[u8], msg: &[u8]) {
    resp_begin(exec);
    let root = [0u8];
    let path = if path.is_empty() { &root[..] } else { path };
    let pl = (path.len() as u16).to_le_bytes();
    resp_field(exec, RESP_ERROR, &[&index.to_le_bytes(), &pl, path, msg]);
    resp_send(exec, outcome, 0, 0);
}

/// Answer an outcome with nothing else.
fn fail_plain(exec: &mut ExecState, outcome: u8) {
    resp_begin(exec);
    resp_send(exec, outcome, 0, 0);
}

fn fail_op(exec: &mut ExecState, index: u32, path: &[u8], e: OpError) {
    let outcome = if e.reason == Reason::TooLarge {
        OUT_TOO_LARGE
    } else {
        OUT_INVALID
    };
    fail(exec, outcome, index, path, reason_message(e.reason));
}

/// A KV result that is not the operation's own answer, as an outcome.
fn store_outcome(result: u8) -> u8 {
    match result {
        KV_RESULT_TOO_LARGE => OUT_TOO_LARGE,
        KV_RESULT_QUOTA => OUT_BUSY,
        KV_RESULT_COMPACTED => OUT_COMPACTED,
        _ => OUT_UNAVAILABLE,
    }
}

fn finish_job(exec: &mut ExecState) {
    exec.phase = P_IDLE;
    disarm_watchdog(exec);
}

// ── Followed watches ──────────────────────────────────────────────────

/// A watch answer, for a watch followed (`FLAG_FOLLOW`): the stream stays
/// open (`RFLAG_MORE`) and the watch resumes from the answer's revision;
/// an answer other than OK ends it. `false` when the answer is not sent —
/// a re-read of a followed watch that found no change stays quiet.
fn follow_answered(exec: &mut ExecState, outcome: u8, flags: &mut u8, revision: u64) -> bool {
    let job = exec.follow_job;
    exec.follow_job = 0;
    let (op, rflags) = (req_op(exec), req_flags(exec));
    if op != OP_WATCH || rflags & FLAG_FOLLOW == 0 || rflags & FLAG_CANCEL != 0 {
        return true;
    }
    let held = job.checked_sub(1).map(|i| i as usize);
    if outcome != OUT_OK {
        if let Some(i) = held {
            exec.follows[i].live = false;
        }
        return true;
    }
    let slot = match held {
        Some(i) if exec.follows[i].live => i,
        Some(_) => return false,
        None => match follow_register(exec) {
            Some(i) => i,
            None => return true,
        },
    };
    // More changes than one answer held: read again at once.
    let more = (*flags | exec.resp_flags) & RFLAG_MORE != 0;
    let f = &mut exec.follows[slot];
    f.cursor = revision;
    f.dirty |= more;
    *flags |= RFLAG_MORE;
    held.is_none() || exec.resp_len > DOC_RESPONSE_HEAD
}

/// Keep the watch in flight as a followed one: its request without its
/// revision, and its span. The slot, or `None` when none is free.
fn follow_register(exec: &mut ExecState) -> Option<usize> {
    let slot = exec.follows.iter().position(|f| !f.live)?;
    let mut start = [0u8; KEY_MAX];
    let mut end = [0u8; KEY_MAX];
    let (sn, en) = request_span(exec, &mut start, &mut end)?;
    let rn = exec.req_len as usize;
    let rq = req_view(exec);
    let src = exec.src;
    let f = &mut exec.follows[slot];
    let head = rq.get(..DOC_REQUEST_HEAD)?;
    put_at(&mut f.req, 0, head)?;
    let mut at = DOC_REQUEST_HEAD;
    for (tag, body) in Fields::of(rq.get(DOC_REQUEST_HEAD..rn)?) {
        if tag == FIELD_AT_REVISION {
            continue;
        }
        put_at(&mut f.req, at, &[tag])?;
        put_at(&mut f.req, at + 1, &(body.len() as u32).to_le_bytes())?;
        put_at(&mut f.req, at + FIELD_HEAD, body)?;
        at += FIELD_HEAD + body.len();
    }
    f.req_len = at as u16;
    f.corr = u64::from_le_bytes(head.get(..8)?.try_into().ok()?);
    f.conn = *head.get(13)?;
    f.src = src;
    put_at(&mut f.start, 0, start.get(..sn)?)?;
    f.start_len = sn as u16;
    put_at(&mut f.end, 0, end.get(..en)?)?;
    f.end_len = en as u16;
    f.dirty = false;
    f.live = true;
    Some(slot)
}

/// `src` into `dst` at `at`, without a panic path.
fn put_at(dst: &mut [u8], at: usize, src: &[u8]) -> Option<()> {
    let d = dst.get_mut(at..at.checked_add(src.len())?)?;
    for (x, y) in d.iter_mut().zip(src) {
        *x = *y;
    }
    Some(())
}

/// End the followed watch `corr` / `conn` of dialect `src`.
fn follow_cancel(exec: &mut ExecState, src: u8, corr: u64, conn: u8) {
    for f in exec.follows.iter_mut() {
        if f.live && f.src == src && f.corr == corr && f.conn == conn {
            f.live = false;
        }
    }
}

/// Change notifications mark the followed watches whose span holds the
/// changed key; every so often all of them are read again. With nothing
/// else to do, the next marked watch is read from its cursor.
unsafe fn follow_step(exec: &mut ExecState, sys: &SyscallTable) {
    for _ in 0..16 {
        let Some((mt, len)) = read_envelope(sys, exec.changes_in, &mut exec.scratch) else {
            break;
        };
        if mt != wire::MSG_WATCH_EVENT {
            continue;
        }
        // `[kpg:2][rev:8][op:1][klen:2][key]…`
        let Some(kl) = exec
            .scratch
            .get(11..13)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
        else {
            continue;
        };
        let Some(key) = exec.scratch.get(13..(13 + kl).min(len)) else {
            continue;
        };
        for f in exec.follows.iter_mut() {
            let (s, e) = (
                &f.start[..f.start_len as usize],
                &f.end[..f.end_len as usize],
            );
            if f.live && key >= s && key < e {
                f.dirty = true;
            }
        }
    }
    let now = dev_millis(sys);
    if now.saturating_sub(exec.follow_heal_ms) >= FOLLOW_HEAL_MS {
        exec.follow_heal_ms = now;
        for f in exec.follows.iter_mut() {
            f.dirty |= f.live;
        }
    }
    if exec.busy() || exec.out_active || exec.queue_len > 0 || exec.inbox_ready != 0 {
        return;
    }
    for k in 0..MAX_FOLLOWS {
        let i = (exec.follow_rr as usize + k) % MAX_FOLLOWS;
        if !(exec.follows[i].live && exec.follows[i].dirty) {
            continue;
        }
        exec.follow_rr = ((i + 1) % MAX_FOLLOWS) as u8;
        exec.follows[i].dirty = false;
        // The request, resuming at the cursor.
        let (src, rl, cursor) = {
            let f = &exec.follows[i];
            (f.src, f.req_len as usize, f.cursor)
        };
        let ExecState {
            follows,
            follow_buf,
            ..
        } = &mut *exec;
        let built = (|| {
            put_at(follow_buf, 0, follows[i].req.get(..rl)?)?;
            put_at(follow_buf, rl, &[FIELD_AT_REVISION])?;
            put_at(follow_buf, rl + 1, &8u32.to_le_bytes())?;
            put_at(follow_buf, rl + FIELD_HEAD, &cursor.to_le_bytes())
        })();
        if built.is_none() {
            exec.follows[i].live = false;
            return;
        }
        let n = rl + FIELD_HEAD + 8;
        exec.follow_job = (i + 1) as u8;
        // `follow_buf` is written only here, between jobs.
        let req = view(&exec.follow_buf, n);
        begin_job(exec, src, req);
        return;
    }
}

/// With no job in flight, the next queued request becomes the job. Called
/// only from the top of a step — never from inside a job — so the request
/// in flight (`req`) is not rewritten while a phase reads it
/// ([`req_view`]).
fn start_queued(exec: &mut ExecState) {
    if exec.busy() || exec.out_active {
        return;
    }
    if exec.inbox_ready != 0 {
        let n = exec.inbox_ready;
        exec.inbox_ready = 0;
        // The inbox is written only by parts, which this call does not read.
        let req = view(&exec.inbox, n);
        let src = exec.inbox_src;
        begin_job(exec, src, req);
        return;
    }
    if exec.queue_len == 0 {
        return;
    }
    let next = exec.queue[0];
    let n = exec.queue_len;
    let mut i = 1;
    while i < n {
        exec.queue[i - 1] = exec.queue[i];
        i += 1;
    }
    exec.queue_len = n - 1;
    begin_job(exec, next.src, &next.buf[..next.len as usize]);
}

// ── KV requests ───────────────────────────────────────────────────────

/// Where a KV request body is staged in `env`.
const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

/// Send a KV request the executor built at `env[BODY_AT..]`, through the
/// large-value layer: a write carrying large values uploads their chunks
/// first and goes out rewritten; a read is marked, and its reply expanded
/// before the phase `next` sees it.
fn kv_send(exec: &mut ExecState, op: u8, body_len: usize, next: u8) {
    exec.lg_read = false;
    let Some(body) = exec.env.get(BODY_AT..BODY_AT + body_len) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    let large = exec.lg_up.start(op, body, kv_large::ENTRY_BODY_MAX);
    exec.lg_req[..body_len].copy_from_slice(body);
    exec.lg_req_len = body_len;
    exec.lg_op = op;
    exec.lg_next = next;
    match large {
        Ok(true) => {
            lg_upload(exec);
            return;
        }
        Ok(false) => {}
        Err(kv_large::LargeError::Overflow) => {
            fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large_write());
            return;
        }
        Err(_) => {
            fail_plain(exec, OUT_CORRUPT);
            return;
        }
    }
    let marked = {
        let ExecState {
            lg_req,
            lg_req_len,
            env,
            ..
        } = &mut *exec;
        kv_large::mark_read(op, &lg_req[..*lg_req_len], &mut env[BODY_AT..])
    };
    let len = match marked {
        Some(n) => {
            exec.lg_read = true;
            n
        }
        None => {
            exec.lg_read = op == types::KV_OP_GET;
            body_len
        }
    };
    kv_emit(exec, op, len, next);
}

/// The next of a held write's chunks, or — all stored — the write,
/// rewritten.
fn lg_upload(exec: &mut ExecState) {
    let mut out = [0u8; kv_large::CHUNK_REQUEST_MAX];
    let r = {
        let ExecState {
            lg_up,
            lg_req,
            lg_req_len,
            ..
        } = &mut *exec;
        lg_up.next_chunk(&lg_req[..*lg_req_len], &mut out)
    };
    match r {
        Ok(Some(n)) => {
            exec.env[BODY_AT..BODY_AT + n].copy_from_slice(&out[..n]);
            kv_emit(exec, kv_large::UPLOAD_OP, n, P_LG_UP);
        }
        Ok(None) => {
            let rw = {
                let ExecState {
                    lg_up,
                    lg_req,
                    lg_req_len,
                    env,
                    ..
                } = &mut *exec;
                lg_up.rewrite(&lg_req[..*lg_req_len], &mut env[BODY_AT..])
            };
            match rw {
                Ok((op, n)) => {
                    let next = exec.lg_next;
                    kv_emit(exec, op, n, next);
                }
                Err(_) => fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large_write()),
            }
        }
        Err(_) => fail_plain(exec, OUT_CORRUPT),
    }
}

/// Continue expanding the held reply: the next chunk to fetch, or the
/// expanded reply to the phase that asked.
fn lg_expand(exec: &mut ExecState) {
    let need = exec.lg_ex.step();
    match need {
        Ok(kv_large::Need::Chunk(h)) => {
            exec.env[BODY_AT..BODY_AT + h.len()].copy_from_slice(&h);
            kv_emit(exec, kv_large::FETCH_OP, h.len(), P_LG_FETCH);
        }
        Ok(kv_large::Need::Done(result, n)) => {
            exec.phase = exec.lg_next;
            exec.lg_read = false;
            let rev = exec.lg_rev;
            // `lg_out` is written only as a reply expands — never while
            // the phase that asked reads its answer.
            let body = view(&exec.lg_out, n);
            dispatch_reply(exec, result, rev, body);
        }
        Err(_) => fail_plain(exec, OUT_CORRUPT),
    }
}

/// Send the request at `env[BODY_AT..]` as it is, continuing in `next`.
fn kv_emit(exec: &mut ExecState, op: u8, body_len: usize, next: u8) {
    // The router carries nothing larger: a transaction that does not fit
    // (an insert of too many or too large documents) is refused here.
    if 18 + body_len > wire::KV_REQUEST_PAYLOAD_MAX {
        fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large_write());
        return;
    }
    exec.kv_corr = exec.kv_corr.wrapping_add(1);
    let at = wire::ENVELOPE_HDR;
    let tenant = u32::from_le_bytes(exec.req[9..13].try_into().unwrap_or([0; 4]));
    exec.env[at..at + 8].copy_from_slice(&exec.kv_corr.to_le_bytes());
    exec.env[at + 8] = PROTO_INTERNAL_SQL;
    exec.env[at + 9..at + 13].copy_from_slice(&tenant.to_le_bytes());
    exec.env[at + 13] = exec.req[13];
    exec.env[at + 14] = types::REQ_LINEARIZABLE;
    exec.env[at + 15] = op;
    exec.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    let (sent, now) = unsafe {
        let sys = exec.syscalls;
        if sys.is_null() {
            (false, 0)
        } else {
            let ok = write_envelope(
                &*sys,
                exec.kv_out,
                MSG_KV_REQUEST,
                18 + body_len,
                &mut exec.env,
            );
            (ok, dev_millis(&*sys))
        }
    };
    if sent {
        exec.phase = next;
        exec.inflight_since_ms = now;
        arm_watchdog(exec);
    } else {
        fail_plain(exec, OUT_UNAVAILABLE);
    }
}

fn arm_watchdog(exec: &mut ExecState) {
    if exec.timer_fd < 0 || exec.syscalls.is_null() {
        return;
    }
    unsafe {
        let mut ms = WEDGE_TIMER_MS.to_le_bytes();
        ((*exec.syscalls).provider_call)(exec.timer_fd, TIMER_SET, ms.as_mut_ptr(), 4);
    }
}

fn disarm_watchdog(exec: &mut ExecState) {
    if exec.timer_fd < 0 || exec.syscalls.is_null() {
        return;
    }
    unsafe {
        ((*exec.syscalls).provider_call)(exec.timer_fd, TIMER_CANCEL, core::ptr::null_mut(), 0);
    }
}

/// `[keyspace:u32 BE][body]`, the model keyspace riding the user key.
fn kv_key(out: &mut [u8], keyspace: u32, body: &[u8]) -> Option<usize> {
    let n = 4 + body.len();
    let d = out.get_mut(..n)?;
    d[..4].copy_from_slice(&keyspace.to_be_bytes());
    d[4..].copy_from_slice(body);
    Some(n)
}

/// Stage a `KV_OP_RANGE_META` body: `[start, end)` (etcd spans: `end`
/// empty = the one key, `[0]` = every key from `start`), at a revision,
/// limited.
fn stage_range_meta(
    exec: &mut ExecState,
    start: &[u8],
    end: &[u8],
    at_rev: u64,
    limit: u32,
) -> Option<usize> {
    let mut p = BODY_AT;
    let e = &mut exec.env;
    put(e, &mut p, &(start.len() as u16).to_le_bytes())?;
    put(e, &mut p, start)?;
    put(e, &mut p, &(end.len() as u16).to_le_bytes())?;
    put(e, &mut p, end)?;
    put(e, &mut p, &at_rev.to_le_bytes())?;
    put(e, &mut p, &limit.to_le_bytes())?;
    // flags 0, sort by key ascending, no revision bounds.
    put(e, &mut p, &[0, 0, 0])?;
    put(e, &mut p, &[0u8; 32])?;
    Some(p - BODY_AT)
}

/// A `KV_RESULT_KVS` answer: `(total, more, entries)`, each entry
/// `(key, value, mod_revision)`.
struct Kvs<'a> {
    more: u8,
    count: u16,
    body: &'a [u8],
}

impl<'a> Kvs<'a> {
    fn parse(b: &'a [u8]) -> Option<Self> {
        let more = *b.get(8)?;
        let count = u16::from_le_bytes([*b.get(9)?, *b.get(10)?]);
        Some(Kvs {
            more,
            count,
            body: b.get(11..)?,
        })
    }

    fn entries(&self) -> impl Iterator<Item = (&'a [u8], &'a [u8], u64)> + 'a {
        let b = self.body;
        let mut at = 0usize;
        let mut left = self.count as usize;
        core::iter::from_fn(move || {
            if left == 0 {
                return None;
            }
            let kl = u16::from_le_bytes([*b.get(at)?, *b.get(at + 1)?]) as usize;
            let key = b.get(at + 2..at + 2 + kl)?;
            let vs = at + 2 + kl;
            let vl = u32::from_le_bytes(b.get(vs..vs + 4)?.try_into().ok()?) as usize;
            let val = b.get(vs + 4..vs + 4 + vl)?;
            let ms = vs + 4 + vl;
            let mod_rev = u64::from_le_bytes(b.get(ms + 8..ms + 16)?.try_into().ok()?);
            at = ms + 32;
            left -= 1;
            Some((key, val, mod_rev))
        })
    }
}

// ── Keys ──────────────────────────────────────────────────────────────

fn catalog_key(out: &mut [u8], name: &[u8]) -> Option<usize> {
    let mut body = [0u8; 5 + doc_catalog::MAX_NAME_LEN];
    let n = catalog_name_key(&mut body, DATABASE_ID, name)?;
    kv_key(out, models::KS_DOCUMENT_CATALOG, &body[..n])
}

/// A document's key: its collection, shard key and id key.
fn data_key(out: &mut [u8], coll: u32, shard: &[u8], id: &[u8]) -> Option<usize> {
    let mut body = [0u8; models::DOCUMENT_DATA_KEY_MAX_LEN];
    let n = models::encode_document_data_key(&mut body, coll, shard, id)?;
    kv_key(out, models::KS_DOCUMENT_DATA, &body[..n])
}

/// A document's key from its shard and id keys: the engine's layout, or —
/// a JSON collection — the store's own path (`doc_ops::path_key`).
fn doc_key(desc: &Descriptor<'_>, shard: &[u8], id: &[u8], out: &mut [u8]) -> Option<usize> {
    if !doc_codec::is_json(desc) {
        return data_key(out, desc.id(), shard, id);
    }
    let mut t = [0u8; ID_VALUE_MAX];
    let mut w = Writer::new(&mut t);
    doc_ops::id_from_key(id, &mut w)?;
    let n = w.finish().ok()?;
    doc_ops::path_key(desc.name(), Value::parse(t.get(..n)?).ok()?, out)
}

/// The key span of a whole collection: `[prefix, successor)`.
fn collection_span(
    desc: &Descriptor<'_>,
    start: &mut [u8],
    end: &mut [u8],
) -> Option<(usize, usize)> {
    let sn = if doc_codec::is_json(desc) {
        doc_ops::path_prefix(desc.name(), None, start)?
    } else {
        let mut body = [0u8; 16];
        let n = models::encode_document_data_prefix(&mut body, desc.id(), None)?;
        kv_key(start, models::KS_DOCUMENT_DATA, &body[..n])?
    };
    let en = successor(&start[..sn], end)?;
    Some((sn, en))
}

/// The span a list or watch reads: the collection's, or — for a request
/// naming a compound id's leading components — the ids beginning with
/// them. `None` for a prefix the collection cannot answer (it is not a
/// compound id's, or the collection is sharded).
fn request_span(exec: &ExecState, start: &mut [u8], end: &mut [u8]) -> Option<(usize, usize)> {
    let desc = Descriptor::parse(&exec.desc[..exec.desc_len as usize]).ok()?;
    let req = decode_req(&exec.req[..exec.req_len as usize])?;
    let Some(prefix) = req.id_prefix() else {
        return collection_span(&desc, start, end);
    };
    if !desc.compound_id()
        || desc.shard_path().is_some()
        || prefix.count() as usize >= desc.id_paths().count()
    {
        return None;
    }
    if doc_codec::is_json(&desc) {
        let sn = doc_ops::path_prefix(desc.name(), Some(prefix), start)?;
        let en = successor(&start[..sn], end)?;
        return Some((sn, en));
    }
    let mut pk = [0u8; models::MAX_DOCUMENT_ID_KEY];
    let pn = id_key(prefix, &mut pk)?;
    let mut body = [0u8; models::DOCUMENT_DATA_KEY_MAX_LEN];
    let n = models::encode_document_id_prefix(&mut body, desc.id(), &[], &pk[..pn])?;
    let sn = kv_key(start, models::KS_DOCUMENT_DATA, &body[..n])?;
    let en = successor(&start[..sn], end)?;
    Some((sn, en))
}

/// The smallest key after every key under `prefix`.
fn successor(prefix: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut n = prefix.len();
    while n > 0 {
        if prefix[n - 1] != 0xFF {
            out.get_mut(..n)?.copy_from_slice(&prefix[..n]);
            out[n - 1] += 1;
            return Some(n);
        }
        n -= 1;
    }
    None
}

/// The document key for `doc` in this collection: its id's key and its
/// shard's. `None` when the id is missing or unusable.
fn key_for(desc: &Descriptor<'_>, doc: Value<'_>, out: &mut [u8]) -> Option<usize> {
    let mut idk = [0u8; models::MAX_DOCUMENT_ID_KEY];
    let idn = id_key_of(desc, doc, &mut idk)?;
    let mut shk = [0u8; models::MAX_MODEL_ID_LEN];
    let shn = match desc.shard_path() {
        Some(sp) => {
            let sv = lookup(doc, sp)?;
            if !id_is_usable(sv) {
                return None;
            }
            id_key(sv, &mut shk)?
        }
        None => 0,
    };
    doc_key(desc, &shk[..shn], &idk[..idn], out)
}

// ── Request access ────────────────────────────────────────────────────

fn req_op(exec: &ExecState) -> u8 {
    exec.req[14]
}

fn req_flags(exec: &ExecState) -> u8 {
    exec.req[15]
}

/// Copy `src` into the hold buffer `dst` and view it past the borrow of
/// the state. Each hold has one role, written only at that role's sites,
/// none of which runs while a view of the same hold taken above it is
/// still read.
fn hold<'a>(dst: &mut [u8], src: &[u8]) -> &'a [u8] {
    let n = src.len().min(dst.len());
    for (d, s) in dst.iter_mut().zip(src) {
        *d = *s;
    }
    // SAFETY: `dst` is a buffer in the module's state (it lives as long
    // as the module); see above for why it is not rewritten while viewed.
    unsafe { core::slice::from_raw_parts(dst.as_ptr(), n) }
}

/// `buf[..n]` viewed past the borrow of the state: for a buffer written
/// only at points the view's reader never reaches (see each caller).
fn view<'a>(buf: &[u8], n: usize) -> &'a [u8] {
    let n = n.min(buf.len());
    // SAFETY: as `hold`.
    unsafe { core::slice::from_raw_parts(buf.as_ptr(), n) }
}

/// A request, up to what this build takes.
fn decode_req(p: &[u8]) -> Option<DocRequest<'_>> {
    DocRequest::decode_up_to(p, REQUEST_MAX)
}

/// The request in flight, readable while the state is written: `req` is
/// written only by `begin_job`, which runs from the top of a step and
/// never while a job's phase runs ([`start_queued`]), so a view of it
/// holds for the whole of a phase. (A copy would put a whole document on
/// the stack.)
fn req_view<'a>(exec: &ExecState) -> &'a [u8] {
    let n = (exec.req_len as usize).min(exec.req.len());
    // SAFETY: `req` lives in the module's state for the module's lifetime
    // and is not written while any phase — the only callers — runs.
    unsafe { core::slice::from_raw_parts(exec.req.as_ptr(), n) }
}

// ── Jobs ──────────────────────────────────────────────────────────────

fn on_request(exec: &mut ExecState, src: u8, payload: &[u8]) {
    if exec.busy() || exec.out_active || exec.inbox_ready != 0 {
        if exec.queue_len >= QUEUE_DEPTH || payload.len() > DOC_REQUEST_MAX {
            exec.m_queue_rejects = exec.m_queue_rejects.wrapping_add(1);
            reject_now(exec, src, payload, OUT_BUSY);
            return;
        }
        let q = &mut exec.queue[exec.queue_len];
        q.src = src;
        q.len = payload.len() as u32;
        q.buf[..payload.len()].copy_from_slice(payload);
        exec.queue_len += 1;
        return;
    }
    begin_job(exec, src, payload);
}

/// Answer a request that never became the job (busy, malformed).
fn reject_now(exec: &mut ExecState, src: u8, payload: &[u8], outcome: u8) {
    if payload.len() < DOC_REQUEST_HEAD {
        return;
    }
    let at = wire::ENVELOPE_HDR;
    let mut env = [0u8; wire::ENVELOPE_HDR + DOC_RESPONSE_HEAD];
    env[at..at + 8].copy_from_slice(&payload[0..8]);
    env[at + 8] = payload[13];
    env[at + 9] = outcome;
    unsafe {
        let sys = exec.syscalls;
        if !sys.is_null() {
            let dest = exec.d_out[(src as usize) & 1];
            let _ = write_envelope(&*sys, dest, MSG_DOC_RESPONSE, DOC_RESPONSE_HEAD, &mut env);
        }
    }
}

fn begin_job(exec: &mut ExecState, src: u8, payload: &[u8]) {
    if decode_req(payload).is_none() {
        reject_now(exec, src, payload, OUT_MALFORMED);
        exec.m_errors = exec.m_errors.wrapping_add(1);
        return;
    }
    exec.m_ops = exec.m_ops.wrapping_add(1);
    exec.src = src;
    exec.req_len = payload.len() as u32;
    exec.req[..payload.len()].copy_from_slice(payload);
    exec.doc_index = 0;
    exec.retries = 0;
    exec.matched = 0;
    exec.modified = 0;
    exec.read_rev = 0;
    exec.snap_rev = 0;
    exec.answered = 0;
    exec.after_len = 0;
    exec.page_len = 0;
    exec.page_at = 0;
    exec.desc_len = 0;
    exec.store_rev = 0;
    exec.drop_stage = 0;
    exec.drop_only = 0;
    exec.drop_outcome = OUT_OK;
    exec.ix_id = 0;
    exec.plan_decided = false;
    exec.plan_ix = 0;
    resp_begin(exec);

    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    exec.resp_budget = req
        .max_bytes()
        .map(|b| b as usize)
        .unwrap_or(RESPONSE_MAX)
        .min(RESPONSE_MAX);
    if req.op == OP_WATCH && req.flags & FLAG_CANCEL != 0 {
        follow_cancel(exec, src, req.corr_id, req.conn_id);
        resp_send(exec, OUT_OK, 0, 0);
        return;
    }
    if req.op == OP_WATCH && req.flags & FLAG_FOLLOW != 0 && exec.follow_job == 0 {
        if n > FOLLOW_REQ_MAX {
            fail_plain(exec, OUT_TOO_LARGE);
            return;
        }
        if !exec.follows.iter().any(|f| !f.live) {
            exec.m_queue_rejects = exec.m_queue_rejects.wrapping_add(1);
            fail_plain(exec, OUT_BUSY);
            return;
        }
    }
    match req.op {
        OP_COLL_LIST => coll_list_page(exec),
        OP_COLL_CREATE => {
            if req.descriptor().is_none() && req.collection().is_none() {
                fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op());
                return;
            }
            coll_create(exec)
        }
        _ => {
            // Everything else names a collection: read its descriptor.
            let Some(name) = req.collection() else {
                fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op());
                return;
            };
            read_descriptor(exec, name);
        }
    }
}

fn read_descriptor(exec: &mut ExecState, name: &[u8]) {
    let mut k = [0u8; KEY_MAX];
    let Some(kn) = catalog_key(&mut k, name) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let Some(bn) = stage_range_meta(exec, &k[..kn], &[], 0, 1) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, bn, P_DESC);
}

fn on_descriptor(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    exec.store_rev = revision;
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let Some(kvs) = Kvs::parse(body) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let Some((_, rec, desc_rev)) = kvs.entries().next() else {
        // No such collection. A write that may create it does so first.
        let op = req_op(exec);
        let creates = matches!(
            op,
            OP_INDEX_CREATE
                | OP_INSERT
                | OP_REPLACE
                | OP_UPDATE
                | OP_APPLY
                | OP_PATCH_MERGE
                | OP_PATCH_JSON
                | OP_PATCH_KEYED
        );
        if creates && req_flags(exec) & FLAG_AUTO_CREATE != 0 {
            coll_create(exec);
            return;
        }
        if matches!(op, OP_COLL_DROP) {
            fail_plain(exec, OUT_NO_COLLECTION);
            return;
        }
        fail_plain(exec, OUT_NO_COLLECTION);
        return;
    };
    if Descriptor::parse(rec).is_err() || rec.len() > MAX_DESCRIPTOR_LEN {
        fail_plain(exec, OUT_CORRUPT);
        return;
    }
    exec.desc[..rec.len()].copy_from_slice(rec);
    exec.desc_len = rec.len() as u16;
    exec.desc_rev = desc_rev;
    dispatch(exec);
}

/// The descriptor is loaded: run the operation.
fn dispatch(exec: &mut ExecState) {
    match req_op(exec) {
        OP_COLL_GET => {
            let n = exec.desc_len as usize;
            let mut d = [0u8; MAX_DESCRIPTOR_LEN];
            d[..n].copy_from_slice(&exec.desc[..n]);
            resp_field(exec, RESP_DESCRIPTOR, &[&d[..n]]);
            resp_send(exec, OUT_OK, 0, 0);
        }
        OP_COLL_DROP => coll_drop(exec),
        OP_INSERT => insert(exec),
        OP_APPLY
            if !Descriptor::parse(&exec.desc[..exec.desc_len as usize])
                .is_ok_and(|d| d.ownership()) =>
        {
            fail(exec, OUT_INVALID, 0, &[], msg_not_owned());
        }
        OP_GET | OP_REPLACE | OP_DELETE | OP_UPDATE | OP_PATCH_MERGE | OP_PATCH_JSON
        | OP_PATCH_KEYED | OP_APPLY => {
            let rq = req_view(exec);
            let n = rq.len();
            let Some(req) = decode_req(&rq[..n]) else {
                fail_plain(exec, OUT_MALFORMED);
                return;
            };
            let by_filter = req.id().is_none() && req.filter().is_some();
            let by_id = !by_filter
                && (req.id().is_some()
                    || (matches!(req.op, OP_REPLACE | OP_APPLY) && req.doc().is_some()));
            if by_id {
                read_one(exec);
            } else if by_filter && req.op != OP_GET {
                scan_page(exec);
            } else {
                fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op());
            }
        }
        OP_LIST => scan_page(exec),
        OP_WATCH => watch_begin(exec),
        OP_INDEX_CREATE => ix_create(exec),
        OP_INDEX_DROP => ix_drop(exec),
        // Patches, apply and watch arrive with their own tasks; until then
        // they are refused by name, never approximated.
        _ => fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op()),
    }
}

// ── Collections ───────────────────────────────────────────────────────

fn coll_create(exec: &mut ExecState) {
    let mut k = [0u8; KEY_MAX];
    let mut body = [0u8; 8];
    let Some(bn) = catalog_seq_key(&mut body, DATABASE_ID) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    let Some(kn) = kv_key(&mut k, models::KS_DOCUMENT_CATALOG, &body[..bn]) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    // One id for the collection and one per index it declares.
    let ids = 1 + decode_req(&exec.req[..exec.req_len as usize])
        .and_then(|r| r.descriptor())
        .map_or(0, |d| d.indexes().count()) as i64;
    let mut p = BODY_AT;
    let ok = put(&mut exec.env, &mut p, &(kn as u16).to_le_bytes()).is_some()
        && put(&mut exec.env, &mut p, &k[..kn]).is_some()
        && put(&mut exec.env, &mut p, &ids.to_le_bytes()).is_some();
    if !ok {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    }
    kv_send(exec, KV_OP_INCR, p - BODY_AT, P_CC_ALLOC);
}

fn on_coll_alloc(exec: &mut ExecState, result: u8, body: &[u8]) {
    let id = match (result, body.get(..8)) {
        (KV_RESULT_INTEGER | KV_RESULT_OK, Some(b)) => {
            i64::from_le_bytes(b.try_into().unwrap_or([0; 8]))
        }
        _ => {
            fail_plain(exec, store_outcome(result));
            return;
        }
    };
    if id <= 0 || id > u32::MAX as i64 {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    }
    // The descriptor: the request's, or the default one (a Mongo-style
    // implicit creation) under the request's collection name.
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let mut rec = [0u8; MAX_DESCRIPTOR_LEN];
    let rn = match req.descriptor() {
        Some(d) => {
            let b = d.bytes();
            rec[..b.len()].copy_from_slice(b);
            b.len()
        }
        None => {
            let Some(name) = req.collection() else {
                fail_plain(exec, OUT_MALFORMED);
                return;
            };
            match DescriptorWriter::new(&mut rec, 0, name).and_then(|w| w.finish()) {
                Ok(n) => n,
                Err(_) => {
                    fail_plain(exec, OUT_MALFORMED);
                    return;
                }
            }
        }
    };
    // The allocator answered the last of the ids this create took.
    let k_ix = Descriptor::parse(&rec[..rn]).map_or(0, |d| d.indexes().count()) as i64;
    let coll = id - k_ix;
    if coll <= 0
        || set_descriptor_id(&mut rec[..rn], coll as u32).is_err()
        || set_index_ids(&mut rec[..rn], coll as u32 + 1).is_err()
    {
        fail_plain(exec, OUT_MALFORMED);
        return;
    }
    let Ok(d) = Descriptor::parse(&rec[..rn]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let mut k = [0u8; KEY_MAX];
    let Some(kn) = catalog_key(&mut k, d.name()) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    exec.desc[..rn].copy_from_slice(&rec[..rn]);
    exec.desc_len = rn as u16;
    // One put, compared absent: two creators race and one wins.
    let Some(bn) = stage_txn_put(exec, &k[..kn], 0, None, &rec[..rn]) else {
        fail_plain(exec, OUT_TOO_LARGE);
        return;
    };
    kv_send(exec, KV_OP_TXN, bn, P_CC_PUT);
}

fn on_coll_put(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    exec.desc_rev = revision;
    if result != KV_RESULT_TXN {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if body.first() != Some(&1) {
        // Lost to another creator. An implicit creation reads the
        // winner's descriptor and carries on; an explicit one is told.
        if req_op(exec) == OP_COLL_CREATE {
            fail_plain(exec, OUT_COLLECTION_EXISTS);
            return;
        }
        let rq = req_view(exec);
        let n = rq.len();
        let name = decode_req(&rq[..n]).and_then(|r| r.collection());
        match name {
            Some(nm) => {
                let mut nb = [0u8; doc_catalog::MAX_NAME_LEN];
                nb[..nm.len()].copy_from_slice(nm);
                read_descriptor(exec, &nb[..nm.len()]);
            }
            None => fail_plain(exec, OUT_MALFORMED),
        }
        return;
    }
    if req_op(exec) == OP_COLL_CREATE {
        let n = exec.desc_len as usize;
        let mut d = [0u8; MAX_DESCRIPTOR_LEN];
        d[..n].copy_from_slice(&exec.desc[..n]);
        resp_field(exec, RESP_DESCRIPTOR, &[&d[..n]]);
        resp_send(exec, OUT_OK, 0, 0);
        return;
    }
    dispatch(exec);
}

fn coll_list_page(exec: &mut ExecState) {
    let mut body = [0u8; 8];
    let mut start = [0u8; KEY_MAX];
    let mut end = [0u8; KEY_MAX];
    let Some(bn) = catalog_names_prefix(&mut body, DATABASE_ID) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    let Some(pn) = kv_key(&mut start, models::KS_DOCUMENT_CATALOG, &body[..bn]) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    let Some(en) = successor(&start[..pn], &mut end) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    let mut from = [0u8; KEY_MAX + 1];
    let fnl = if exec.after_len > 0 {
        let a = exec.after_len as usize;
        from[..a].copy_from_slice(&exec.after[..a]);
        from[a] = 0;
        a + 1
    } else {
        from[..pn].copy_from_slice(&start[..pn]);
        pn
    };
    let Some(n) = stage_range_meta(
        exec,
        &from[..fnl],
        &end[..en],
        exec.snap_rev,
        SCAN_PAGE as u32,
    ) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, n, P_CL_SCAN);
}

fn on_coll_list(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let Some(kvs) = Kvs::parse(body) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    if exec.snap_rev == 0 {
        exec.snap_rev = revision;
    }
    let mut last = [0u8; KEY_MAX];
    let mut last_len = 0usize;
    for (k, rec, _) in kvs.entries() {
        if Descriptor::parse(rec).is_ok() && !resp_field(exec, RESP_DESCRIPTOR, &[rec]) {
            break;
        }
        last[..k.len()].copy_from_slice(k);
        last_len = k.len();
    }
    if exec.resp_overflow {
        // Descriptors are small; a catalog past one answer pages.
        let mut c = [0u8; 16 + MAX_CONTINUE_KEY];
        if let Some(cn) = encode_continuation(&mut c, exec.snap_rev, &last[..last_len]) {
            exec.resp_overflow = false;
            resp_field(exec, RESP_CONTINUE, &[&c[..cn]]);
        }
        resp_send(exec, OUT_OK, RFLAG_MORE, exec.snap_rev);
        return;
    }
    if kvs.more != 0 && last_len > 0 {
        exec.after[..last_len].copy_from_slice(&last[..last_len]);
        exec.after_len = last_len as u16;
        coll_list_page(exec);
        return;
    }
    resp_send(exec, OUT_OK, 0, exec.snap_rev);
}

fn coll_drop(exec: &mut ExecState) {
    // The descriptor goes first, in one write: from then on the
    // collection does not exist, and its id is never reused. Its
    // documents are then deleted page by page.
    let n = exec.desc_len as usize;
    let mut rec = [0u8; MAX_DESCRIPTOR_LEN];
    rec[..n].copy_from_slice(&exec.desc[..n]);
    let Ok(d) = Descriptor::parse(&rec[..n]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let mut k = [0u8; KEY_MAX];
    let Some(kn) = catalog_key(&mut k, d.name()) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let Some(bn) = stage_txn_delete(exec, &k[..kn], None) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_TXN, bn, P_CD_DEL);
}

/// The key span of one index's entries.
fn index_span(index: u32, start: &mut [u8], end: &mut [u8]) -> Option<(usize, usize)> {
    let sn = kv_key(start, models::KS_DOCUMENT_INDEX, &index.to_be_bytes())?;
    let en = successor(&start[..sn], end)?;
    Some((sn, en))
}

/// Delete the next page of what a drop removes: the collection's
/// documents, then each index's entries — or, dropping one index, its
/// entries alone.
fn coll_drop_page(exec: &mut ExecState) {
    let mut start = [0u8; KEY_MAX];
    let mut end = [0u8; KEY_MAX];
    let span = {
        let Ok(d) = Descriptor::parse(&exec.desc[..exec.desc_len as usize]) else {
            fail_plain(exec, OUT_CORRUPT);
            return;
        };
        if exec.drop_only != 0 {
            if exec.drop_stage > 0 {
                None
            } else {
                index_span(exec.drop_only, &mut start, &mut end)
            }
        } else if exec.drop_stage == 0 {
            collection_span(&d, &mut start, &mut end)
        } else {
            match d.indexes().nth(exec.drop_stage as usize - 1) {
                Some(ix) => index_span(ix.id, &mut start, &mut end),
                None => None,
            }
        }
    };
    let Some((sn, en)) = span else {
        // Every span is empty: the drop is done.
        match exec.drop_outcome {
            OUT_OK => resp_send(exec, OUT_OK, 0, 0),
            OUT_EXISTS => fail(exec, OUT_EXISTS, 0, &[], msg_unique()),
            o => fail(exec, o, 0, &[], reason_message(Reason::NotIndexable)),
        }
        return;
    };
    let Some(n) = stage_range_meta(exec, &start[..sn], &end[..en], 0, SCAN_PAGE as u32) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, n, P_CD_SCAN);
}

fn on_coll_drop_scan(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let Some(kvs) = Kvs::parse(body) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    if kvs.count == 0 {
        // This span is empty: the next.
        exec.drop_stage += 1;
        coll_drop_page(exec);
        return;
    }
    // One transaction deleting the page's keys.
    let mut p = BODY_AT;
    let e = &mut exec.env;
    let ok = (|| {
        put(e, &mut p, &0u16.to_le_bytes())?;
        put(e, &mut p, &kvs.count.to_le_bytes())?;
        for (k, _, _) in kvs.entries() {
            put(e, &mut p, &[types::KV_OP_DELETE])?;
            put(e, &mut p, &((4 + k.len()) as u16).to_le_bytes())?;
            put(e, &mut p, &1u16.to_le_bytes())?;
            put(e, &mut p, &(k.len() as u16).to_le_bytes())?;
            put(e, &mut p, k)?;
        }
        put(e, &mut p, &0u16.to_le_bytes())
    })();
    if ok.is_none() {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    }
    kv_send(exec, KV_OP_TXN, p - BODY_AT, P_CD_PAGE_DEL);
}

// ── Transactions ──────────────────────────────────────────────────────

/// One put in a transaction compared on `rev` (0: absent): `[cmp][put]`.
/// The value is `prefix` then `value` (a record's format byte and tree).
fn stage_txn_put(
    exec: &mut ExecState,
    key: &[u8],
    rev: u64,
    prefix: Option<u8>,
    value: &[u8],
) -> Option<usize> {
    let mut p = BODY_AT;
    let e = &mut exec.env;
    let vlen = value.len() + prefix.is_some() as usize;
    put(e, &mut p, &1u16.to_le_bytes())?;
    put(e, &mut p, &[TXN_CMP_MOD_EQUAL])?;
    put(e, &mut p, &(key.len() as u16).to_le_bytes())?;
    put(e, &mut p, key)?;
    put(e, &mut p, &rev.to_le_bytes())?;
    put(e, &mut p, &1u16.to_le_bytes())?;
    let put_len = 2 + key.len() + 4 + vlen + 1 + 8;
    put(e, &mut p, &[types::KV_OP_PUT])?;
    put(e, &mut p, &u16::try_from(put_len).ok()?.to_le_bytes())?;
    put(e, &mut p, &(key.len() as u16).to_le_bytes())?;
    put(e, &mut p, key)?;
    put(e, &mut p, &(vlen as u32).to_le_bytes())?;
    if let Some(b) = prefix {
        put(e, &mut p, &[b])?;
    }
    put(e, &mut p, value)?;
    put(e, &mut p, &[0])?;
    put(e, &mut p, &0u64.to_le_bytes())?;
    put(e, &mut p, &0u16.to_le_bytes())?;
    Some(p - BODY_AT)
}

/// One delete in a transaction, compared on `rev` when given.
fn stage_txn_delete(exec: &mut ExecState, key: &[u8], rev: Option<u64>) -> Option<usize> {
    let mut p = BODY_AT;
    let e = &mut exec.env;
    match rev {
        Some(r) => {
            put(e, &mut p, &1u16.to_le_bytes())?;
            put(e, &mut p, &[TXN_CMP_MOD_EQUAL])?;
            put(e, &mut p, &(key.len() as u16).to_le_bytes())?;
            put(e, &mut p, key)?;
            put(e, &mut p, &r.to_le_bytes())?;
        }
        None => put(e, &mut p, &0u16.to_le_bytes())?,
    }
    put(e, &mut p, &1u16.to_le_bytes())?;
    put(e, &mut p, &[types::KV_OP_DELETE])?;
    put(e, &mut p, &((4 + key.len()) as u16).to_le_bytes())?;
    put(e, &mut p, &1u16.to_le_bytes())?;
    put(e, &mut p, &(key.len() as u16).to_le_bytes())?;
    put(e, &mut p, key)?;
    put(e, &mut p, &0u16.to_le_bytes())?;
    Some(p - BODY_AT)
}

// ── Document transactions ─────────────────────────────────────────────

/// A `KV_OP_TXN` under construction: comparisons and the THEN branch,
/// assembled into the request body at the end — staged
/// (`KV_OP_TXN_STAGED`: each op's length a `u32`, so a put carries a
/// whole record), for the large-value layer to send as the `KV_OP_TXN`.
struct Txn<'a> {
    cmp: &'a mut [u8],
    cl: usize,
    cn: u16,
    ops: &'a mut [u8],
    ol: usize,
    on: u16,
}

impl<'a> Txn<'a> {
    fn new(cmp: &'a mut [u8], ops: &'a mut [u8]) -> Self {
        Txn {
            cmp,
            cl: 0,
            cn: 0,
            ops,
            ol: 0,
            on: 0,
        }
    }

    /// The key's mod revision must be `rev` (0: absent).
    fn cmp(&mut self, key: &[u8], rev: u64) -> Option<()> {
        let c = &mut *self.cmp;
        put(c, &mut self.cl, &[TXN_CMP_MOD_EQUAL])?;
        put(c, &mut self.cl, &(key.len() as u16).to_le_bytes())?;
        put(c, &mut self.cl, key)?;
        put(c, &mut self.cl, &rev.to_le_bytes())?;
        self.cn += 1;
        Some(())
    }

    /// Put the value made of `parts` at `key`.
    fn put(&mut self, key: &[u8], parts: &[&[u8]]) -> Option<()> {
        let o = &mut *self.ops;
        let vlen: usize = parts.iter().map(|p| p.len()).sum();
        let body = 2 + key.len() + 4 + vlen + 1 + 8;
        put(o, &mut self.ol, &[types::KV_OP_PUT])?;
        put(o, &mut self.ol, &u32::try_from(body).ok()?.to_le_bytes())?;
        put(o, &mut self.ol, &(key.len() as u16).to_le_bytes())?;
        put(o, &mut self.ol, key)?;
        put(o, &mut self.ol, &(vlen as u32).to_le_bytes())?;
        for part in parts {
            put(o, &mut self.ol, part)?;
        }
        put(o, &mut self.ol, &[0])?;
        put(o, &mut self.ol, &0u64.to_le_bytes())?;
        self.on += 1;
        Some(())
    }

    fn del(&mut self, key: &[u8]) -> Option<()> {
        let o = &mut *self.ops;
        put(o, &mut self.ol, &[types::KV_OP_DELETE])?;
        put(o, &mut self.ol, &((4 + key.len()) as u32).to_le_bytes())?;
        put(o, &mut self.ol, &1u16.to_le_bytes())?;
        put(o, &mut self.ol, &(key.len() as u16).to_le_bytes())?;
        put(o, &mut self.ol, key)?;
        self.on += 1;
        Some(())
    }

    /// The request body at `env[BODY_AT..]`; its length.
    fn assemble(&self, env: &mut [u8]) -> Option<usize> {
        let mut p = BODY_AT;
        put(env, &mut p, &self.cn.to_le_bytes())?;
        put(env, &mut p, &self.cmp[..self.cl])?;
        put(env, &mut p, &self.on.to_le_bytes())?;
        put(env, &mut p, &self.ops[..self.ol])?;
        put(env, &mut p, &0u16.to_le_bytes())?;
        Some(p - BODY_AT)
    }
}

/// An index entry's KV key: a unique index's names its values alone (the
/// entry holds the document's id key), a plain index's adds the id key.
fn index_entry_key(out: &mut [u8], index: u32, values: &[u8], idk: Option<&[u8]>) -> Option<usize> {
    let mut body = [0u8; models::DOCUMENT_INDEX_KEY_MAX_LEN];
    let n = match idk {
        Some(id) => models::encode_document_index_key(&mut body, index, values, id)?,
        None => models::encode_document_index_prefix(&mut body, index, values)?,
    };
    kv_key(out, models::KS_DOCUMENT_INDEX, &body[..n])
}

/// Does `doc` carry the entry `values` under `ix`?
fn has_entry(ix: &doc_catalog::Index<'_>, doc: Option<Value<'_>>, values: &[u8]) -> bool {
    let Some(d) = doc else { return false };
    let mut found = false;
    let _ = index_values(ix, d, &mut |k| {
        found = k == values;
        !found
    });
    found
}

/// Keep every index of the collection in the transaction: the entries
/// `old` has and `new` lacks deleted, the ones `new` adds put — a unique
/// one compared absent, so a value another document holds refuses the
/// whole write.
fn txn_indexes(
    t: &mut Txn<'_>,
    desc: &Descriptor<'_>,
    old: Option<Value<'_>>,
    new: Option<Value<'_>>,
    idk: &[u8],
) -> Result<(), OpError> {
    let full = OpError::from(doc_tree::TreeError::Overflow);
    for ix in desc.indexes() {
        let unique = ix.unique();
        let mut ok = true;
        if let Some(o) = old {
            index_values(&ix, o, &mut |vals| {
                if !has_entry(&ix, new, vals) {
                    let mut k = [0u8; KEY_MAX + 300];
                    ok &= index_entry_key(&mut k, ix.id, vals, (!unique).then_some(idk))
                        .and_then(|n| t.del(&k[..n]))
                        .is_some();
                }
                ok
            })?;
        }
        if let Some(nw) = new {
            index_values(&ix, nw, &mut |vals| {
                if !has_entry(&ix, old, vals) {
                    let mut k = [0u8; KEY_MAX + 300];
                    ok &= match index_entry_key(&mut k, ix.id, vals, (!unique).then_some(idk)) {
                        Some(n) if unique => t.cmp(&k[..n], 0).and_then(|_| t.put(&k[..n], &[idk])),
                        Some(n) => t.put(&k[..n], &[]),
                        None => None,
                    }
                    .is_some();
                }
                ok
            })?;
        }
        if !ok {
            return Err(full);
        }
    }
    Ok(())
}

/// The id key of a document (its id paths' values, keyed).
fn doc_id_key(desc: &Descriptor<'_>, doc: Value<'_>, out: &mut [u8]) -> Option<usize> {
    id_key_of(desc, doc, out)
}

/// Stage one document's write: its key compared on `rev`, the new record
/// put (or, `new` absent, the document deleted), and every index kept —
/// `exec.stored` holds the document being replaced when `exec.read_rev`
/// is nonzero.
fn stage_doc_write(
    exec: &mut ExecState,
    key: &[u8],
    rev: u64,
    new: Option<&[u8]>,
) -> Result<usize, OpError> {
    let full = OpError::from(doc_tree::TreeError::Overflow);
    let ExecState {
        desc,
        desc_len,
        desc_rev,
        stored,
        stored_len,
        read_rev,
        owners,
        owners_len,
        next_owners,
        next_owners_len,
        tx_cmp,
        tx_ops,
        env,
        codec,
        ..
    } = &mut *exec;
    let desc = Descriptor::parse(&desc[..*desc_len as usize]).map_err(|_| full)?;
    // A collection that tracks ownership stores it with the document: the
    // write's own, or the stored one carried (none yet: an empty record).
    let own: Option<&[u8]> = if !desc.ownership() {
        None
    } else if *next_owners_len > 0 {
        Some(&next_owners[..*next_owners_len as usize])
    } else if *read_rev != 0 && *owners_len > 0 {
        Some(&owners[..*owners_len as usize])
    } else {
        Some(&NO_OWNERS)
    };
    let old = (*read_rev != 0).then(|| Value::trusted(&stored[..*stored_len as usize]));
    let new_v = new.map(Value::trusted);
    let mut idk = [0u8; models::MAX_DOCUMENT_ID_KEY];
    let il = new_v
        .or(old)
        .and_then(|d| doc_id_key(&desc, d, &mut idk))
        .unwrap_or(0);
    let mut t = Txn::new(tx_cmp, tx_ops);
    let mut ck = [0u8; KEY_MAX];
    let cn = catalog_key(&mut ck, desc.name()).ok_or(full)?;
    t.cmp(&ck[..cn], *desc_rev).ok_or(full)?;
    t.cmp(key, rev).ok_or(full)?;
    match (new, own) {
        // A JSON collection's record: the object as its other clients
        // read it, its ownership inside.
        (Some(d), o) if doc_codec::is_json(&desc) => {
            let (mut b, text) = codec.split();
            let owners = o.map(fields::Owners::parse).transpose().map_err(|_| full)?;
            let n = doc_codec::encode_json(&desc, Value::trusted(d), owners, &mut b, text)
                .ok_or(full)?;
            t.put(key, &[&text[..n]]).ok_or(full)?
        }
        (Some(d), Some(o)) => {
            let head = owned_record_head(d.len()).ok_or(full)?;
            if head.len() + d.len() + o.len() > MAX_RECORD {
                return Err(OpError::from(doc_tree::TreeError::Overflow));
            }
            t.put(key, &[&head, d, o]).ok_or(full)?
        }
        (Some(d), None) => t.put(key, &[&[FORMAT_V1], d]).ok_or(full)?,
        (None, _) => t.del(key).ok_or(full)?,
    }
    txn_indexes(&mut t, &desc, old, new_v, &idk[..il])?;
    t.assemble(env).ok_or(full)
}

/// Does the collection declare a unique index?
fn has_unique(exec: &ExecState) -> bool {
    Descriptor::parse(&exec.desc[..exec.desc_len as usize])
        .is_ok_and(|d| d.indexes().any(|i| i.unique()))
}

// ── Stamps ────────────────────────────────────────────────────────────

/// The time and entropy generators draw on. `None` (and the job
/// answered) when the platform has neither and the descriptor needs one.
fn stamp_context(exec: &mut ExecState, needs_clock: bool) -> Option<([u8; 32], (i64, u32))> {
    let mut random = [0u8; 32];
    let (rc, ms) = unsafe {
        let sys = &*exec.syscalls;
        (
            dev_csprng_fill(sys, random.as_mut_ptr(), random.len()),
            dev_unix_millis(sys),
        )
    };
    if rc < 0 {
        fail(exec, OUT_UNAVAILABLE, 0, &[], msg_no_entropy());
        return None;
    }
    if needs_clock && ms == 0 {
        fail(exec, OUT_UNAVAILABLE, 0, &[], msg_no_clock());
        return None;
    }
    let now = ((ms / 1000) as i64, ((ms % 1000) * 1_000_000) as u32);
    Some((random, now))
}

/// Build a new document into `bufs`: the source, its id (generated when
/// the descriptor says so and it has none), then the create stamps.
/// Answers the failure itself and returns `false`.
fn build_created(
    exec_desc: &Descriptor<'_>,
    source: Value<'_>,
    stated: Option<&DocRequest<'_>>,
    bufs: &mut Buffers<'_>,
    random: &[u8; 32],
    now: (i64, u32),
) -> Result<(), (u8, &'static [u8], u8)> {
    bufs.load(source)
        .map_err(|e| (OUT_TOO_LARGE, reason_message(e.reason), 0))?;
    if let Some(r) = stated {
        state_id(exec_desc, r, bufs).map_err(|_| (OUT_INVALID, msg_other_id(), 0))?;
    }
    // The id's last component is the one a generated name fills (a
    // compound id's name; its other components the source states).
    let last = exec_desc.id_paths().last().unwrap_or(exec_desc.id_path());
    if lookup(bufs.current(), last).is_none() {
        let Some(g) = exec_desc.generate() else {
            return Err((OUT_INVALID, msg_no_id(), 0));
        };
        let prefix = lookup(bufs.current(), g.path)
            .and_then(|v| v.as_str())
            .unwrap_or(&[]);
        let mut name = [0u8; models::MAX_DOCUMENT_ID_KEY];
        // A prefix the id cannot hold is refused, never cut short.
        let mut pre = [0u8; models::MAX_DOCUMENT_ID_KEY];
        let pl = prefix.len();
        pre.get_mut(..pl)
            .ok_or((OUT_INVALID, msg_no_id(), 0))?
            .copy_from_slice(prefix);
        let n = generated_name(
            &pre[..pl],
            g.suffix as usize,
            g.alphabet,
            &random[16..],
            &mut name,
        )
        .ok_or((OUT_INVALID, msg_no_id(), 0))?;
        let mut vb = [0u8; models::MAX_DOCUMENT_ID_KEY + 16];
        let mut w = Writer::new(&mut vb);
        w.string(&name[..n])
            .map_err(|_| (OUT_INVALID, msg_no_id(), 0))?;
        let vn = w.finish().map_err(|_| (OUT_INVALID, msg_no_id(), 0))?;
        bufs.edit(last, Edit::Set(Value::trusted(&vb[..vn])))
            .map_err(|e| (OUT_INVALID, reason_message(e.reason), 0))?;
    }
    // Stamps: each generated from the id the document now has.
    let mut idb = [0u8; ID_VALUE_MAX];
    let idn = {
        let mut w = Writer::new(&mut idb);
        doc_id(exec_desc, bufs.current(), &mut w).ok_or((OUT_INVALID, msg_no_id(), 0))?;
        w.finish().map_err(|_| (OUT_INVALID, msg_no_id(), 0))?
    };
    let mut rnd = [0u8; 16];
    rnd.copy_from_slice(&random[..16]);
    let ctx = StampContext {
        now,
        random: rnd,
        id: Some(Value::trusted(&idb[..idn])),
    };
    for s in exec_desc.create_stamps() {
        let mut vb = [0u8; ID_VALUE_MAX + MAX_DESCRIPTOR_LEN];
        let mut w = Writer::new(&mut vb);
        generate(&s.generator, &ctx, &mut w)
            .map_err(|e| (OUT_INVALID, reason_message(e.reason), 0))?;
        let vn = w.finish().map_err(|_| (OUT_INVALID, msg_malformed(), 0))?;
        bufs.edit(s.path, Edit::Set(Value::trusted(&vb[..vn])))
            .map_err(|e| (OUT_INVALID, reason_message(e.reason), 0))?;
    }
    Ok(())
}

fn needs_clock(d: &Descriptor<'_>) -> bool {
    d.create_stamps().any(|s| s.generator.kind == GEN_NOW)
}

/// Does the document fit the collection (and a KV value)?
fn fits(d: &Descriptor<'_>, doc: Value<'_>) -> bool {
    let limit = d.max_bytes().map(|m| m as usize).unwrap_or(usize::MAX);
    let n = doc.bytes().len() + 1;
    n <= limit && n <= MAX_RECORD
}

/// Largest record a document may take: what one answer carries whole —
/// a response frame less its framing and a stamped revision. A record
/// larger than one data chunk is stored as a chunk tree through the
/// large-value layer (`kv_large.rs`).
const MAX_RECORD: usize = RESPONSE_MAX - 256;
/// A reply with its manifests expanded (laid out from the reply as it
/// arrives, then filled): two whole records and their page framing.
const LG_OUT: usize = 2 * MAX_RECORD + 1024;
/// Longest field path an ownership transition builds.
const OWN_PATH: usize = 1024;
/// Room an ownership entry's fields are spelled in (`doc_codec`): one
/// entry's `fieldsV1` grows with the document's fields, so the big build
/// bounds it as it bounds the record.
#[cfg(not(feature = "big"))]
const CODEC_SCRATCH: usize = 16384;
#[cfg(feature = "big")]
const CODEC_SCRATCH: usize = DOC_BUF;

/// A JSON collection's buffers (`doc_codec`): the stored object as a
/// tree, the text a write stores, its ownership entries' text and tree,
/// an entry's room, and a field path.
struct Codec {
    tree: DocBuf<DOC_BUF>,
    text: DocBuf<MAX_RECORD>,
    own_text: DocBuf<OWN_BUF>,
    own_tree: DocBuf<OWN_BUF>,
    scratch: DocBuf<CODEC_SCRATCH>,
    path: [u8; OWN_PATH],
}

impl Codec {
    /// The codec's working buffers, and the text a write is spelled into.
    fn split(&mut self) -> (doc_codec::CodecBufs<'_>, &mut [u8]) {
        let Codec {
            tree,
            text,
            own_text,
            own_tree,
            scratch,
            path,
        } = self;
        let b = doc_codec::CodecBufs {
            tree: &mut tree[..],
            own: doc_render::OwnersBufs {
                scratch: &mut scratch[..],
                text: &mut own_text[..],
                tree: &mut own_tree[..],
            },
            path: &mut path[..],
        };
        (b, &mut text[..])
    }
}

/// Largest id value: its key (at most `models::MAX_DOCUMENT_ID_KEY`)
/// plus a value's 4 bytes of framing per component — each at least
/// three key bytes — and an array's head.
const ID_VALUE_MAX: usize = 3 * models::MAX_DOCUMENT_ID_KEY;

// ── Insert ────────────────────────────────────────────────────────────

/// The ownership of a document `who` creates, into `next_owners`: its
/// length.
fn created_owners(
    exec: &mut ExecState,
    desc: &Descriptor<'_>,
    doc: Value<'_>,
    who: &Actor<'_>,
    dir: Directives<'_>,
) -> Result<usize, FieldError> {
    let keyed = Keyed { desc: *desc, dir };
    let schema = Schema {
        keyed: &keyed,
        unowned: Unowned::Declared(*desc),
        version: desc.version_path(),
    };
    let ExecState {
        next_owners,
        own_a,
        own_b,
        own_c,
        own_path,
        ..
    } = &mut *exec;
    let mut sc = Scratch {
        a: own_a,
        b: own_b,
        c: own_c,
        path: own_path,
    };
    fields::record_update(
        Owners::parse(&NO_OWNERS)?,
        None,
        doc,
        who,
        &schema,
        &mut sc,
        next_owners,
    )
}

/// Set the id a create's request states — its whole id, or a compound
/// id's leading components — on the document in `bufs`. A component the
/// document already holds must be the same: `Err` names the path that
/// differs (or cannot be set).
fn state_id<'d>(
    desc: &Descriptor<'d>,
    req: &DocRequest<'_>,
    bufs: &mut Buffers<'_>,
) -> Result<(), Path<'d>> {
    match (req.id(), req.id_prefix(), desc.compound_id()) {
        (Some(id), _, true) | (None, Some(id), true) => {
            set_stated(desc.id_paths().zip(id.elements()), bufs)
        }
        (Some(id), _, false) => set_stated(core::iter::once((desc.id_path(), id)), bufs),
        _ => Ok(()),
    }
}

/// Each `(path, value)` onto the document, as [`state_id`] says.
fn set_stated<'d, 'v>(
    pairs: impl Iterator<Item = (Path<'d>, Value<'v>)>,
    bufs: &mut Buffers<'_>,
) -> Result<(), Path<'d>> {
    for (path, v) in pairs {
        match lookup(bufs.current(), path).map(|cur| cur.bytes() == v.bytes()) {
            Some(true) => {}
            Some(false) => return Err(path),
            None => bufs.edit(path, Edit::Set(v)).map_err(|_| path)?,
        }
    }
    Ok(())
}

fn insert(exec: &mut ExecState) {
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let count = req.docs().count();
    if count == 0 {
        fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op());
        return;
    }
    let owned = desc.ownership();
    if owned && req.manager().is_none() {
        fail(exec, OUT_INVALID, 0, &[], msg_no_manager());
        return;
    }
    let Some((random, now)) = stamp_context(exec, needs_clock(&desc) || owned) else {
        return;
    };
    let who = Actor {
        manager: req.manager().unwrap_or(&[]),
        scope: req.scope_name().unwrap_or(&[]),
        secs: now.0,
        nanos: now.1,
    };
    // One transaction: the descriptor's revision (the schema fence), every
    // document's key compared absent and put, and its index entries (a
    // unique one compared absent too).
    let (mut cl, mut cn, mut ol, mut on) = (0usize, 0u16, 0usize, 0u16);
    {
        let mut ck = [0u8; KEY_MAX];
        let fenced = catalog_key(&mut ck, desc.name()).and_then(|n| {
            let mut t = Txn::new(&mut exec.tx_cmp, &mut exec.tx_ops);
            t.cmp(&ck[..n], exec.desc_rev)?;
            (cl, cn, ol, on) = (t.cl, t.cn, t.ol, t.on);
            Some(())
        });
        if fenced.is_none() {
            fail_plain(exec, OUT_MALFORMED);
            return;
        }
    }
    for (i, src) in req.docs().enumerate() {
        let mut rnd = random;
        // Each document draws its own generated suffix.
        rnd[16] ^= i as u8;
        let (a, b) = (&mut exec.buf_a, &mut exec.buf_b);
        let mut bufs = Buffers::new(a, b);
        // The id the request states (one document: a Kubernetes create's
        // namespace, and name, from its URL) is the document's.
        let stated = (count == 1).then_some(&req);
        if let Err((outcome, msg, _)) = build_created(&desc, src, stated, &mut bufs, &rnd, now) {
            fail(exec, outcome, i as u32, &[], msg);
            return;
        }
        let docb = hold(&mut exec.hold_doc, bufs.current().bytes());
        let dl = docb.len();
        let doc = Value::trusted(&docb[..dl]);
        if !fits(&desc, doc) {
            fail(exec, OUT_TOO_LARGE, i as u32, &[], msg_too_large());
            return;
        }
        let mut k = [0u8; KEY_MAX];
        let Some(kn) = key_for(&desc, doc, &mut k) else {
            fail(
                exec,
                OUT_INVALID,
                i as u32,
                desc.id_path().bytes(),
                msg_no_id(),
            );
            return;
        };
        let mut idk = [0u8; models::MAX_DOCUMENT_ID_KEY];
        let il = doc_id_key(&desc, doc, &mut idk).unwrap_or(0);
        // Its creator owns every field it sets.
        let own = if owned {
            match created_owners(exec, &desc, doc, &who, req.directives()) {
                Ok(n) => n,
                Err(FieldError::Overflow) => {
                    fail(exec, OUT_TOO_LARGE, i as u32, &[], msg_too_large());
                    return;
                }
                Err(_) => {
                    fail(exec, OUT_INVALID, i as u32, &[], msg_too_deep());
                    return;
                }
            }
        } else {
            0
        };
        let head = owned_record_head(dl).unwrap_or([0; 5]);
        if own > 0 && head.len() + dl + own > MAX_RECORD {
            fail(exec, OUT_TOO_LARGE, i as u32, &[], msg_too_large());
            return;
        }
        let staged = {
            let mut t = Txn {
                cmp: &mut exec.tx_cmp,
                cl,
                cn,
                ops: &mut exec.tx_ops,
                ol,
                on,
            };
            let r = t
                .cmp(&k[..kn], 0)
                .and_then(|_| {
                    if doc_codec::is_json(&desc) {
                        let (mut b, text) = exec.codec.split();
                        let owners = if own > 0 {
                            Some(fields::Owners::parse(&exec.next_owners[..own]).ok()?)
                        } else {
                            None
                        };
                        let n = doc_codec::encode_json(&desc, doc, owners, &mut b, text)?;
                        t.put(&k[..kn], &[&text[..n]])
                    } else if own > 0 {
                        t.put(&k[..kn], &[&head, doc.bytes(), &exec.next_owners[..own]])
                    } else {
                        t.put(&k[..kn], &[&[FORMAT_V1], doc.bytes()])
                    }
                })
                .ok_or(OpError::from(doc_tree::TreeError::Overflow))
                .and_then(|_| txn_indexes(&mut t, &desc, None, Some(doc), &idk[..il]));
            (cl, cn, ol, on) = (t.cl, t.cn, t.ol, t.on);
            r
        };
        if let Err(e) = staged {
            fail_op(exec, i as u32, &[], e);
            return;
        }
        // The answer: the document itself when one was inserted, the ids
        // when several were.
        if count == 1 {
            // Answered after the commit, with its revision.
            exec.stored[..dl].copy_from_slice(&docb[..dl]);
            exec.stored_len = dl as u32;
            exec.owners[..own].copy_from_slice(&exec.next_owners[..own]);
            exec.owners_len = own as u32;
        } else {
            let mut ib = [0u8; ID_VALUE_MAX];
            let mut w = Writer::new(&mut ib);
            if doc_id(&desc, doc, &mut w).is_some() {
                if let Ok(n) = w.finish() {
                    resp_field(exec, RESP_ID, &[&ib[..n]]);
                }
            }
        }
    }
    let body = {
        let t = Txn {
            cmp: &mut exec.tx_cmp,
            cl,
            cn,
            ops: &mut exec.tx_ops,
            ol,
            on,
        };
        t.assemble(&mut exec.env)
    };
    let Some(body) = body else {
        fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large());
        return;
    };
    exec.matched = count as u64;
    if req.flags & FLAG_DRY_RUN != 0 {
        let c = count as u64;
        if c == 1 {
            let _ = push_doc(exec, 0);
        }
        resp_field(exec, RESP_COUNT, &[&c.to_le_bytes(), &c.to_le_bytes()]);
        resp_send(exec, OUT_OK, 0, 0);
        return;
    }
    kv_send(exec, KV_OP_TXN_STAGED, body, P_INS_TXN);
}

fn on_insert(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if result != KV_RESULT_TXN {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if body.first() == Some(&1) {
        let c = exec.matched;
        exec.m_docs_written = exec.m_docs_written.wrapping_add(c);
        // One document: answered as stored, with the transaction's
        // revision where the collection carries it.
        if c == 1 && !push_doc(exec, revision) {
            fail_plain(exec, OUT_CORRUPT);
            return;
        }
        resp_field(exec, RESP_COUNT, &[&c.to_le_bytes(), &c.to_le_bytes()]);
        resp_send(exec, OUT_OK, RFLAG_CREATED, revision);
        return;
    }
    // A key was taken: find which, so the answer names the document.
    exec.doc_index = 0;
    probe_next(exec);
}

fn probe_next(exec: &mut ExecState) {
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let i = exec.doc_index as usize;
    let Some(src) = req.docs().nth(i) else {
        // Every key probed absent: a unique index refused a value, or the
        // conflict was with a concurrent insert that has since gone (the
        // client may retry).
        why_refused(exec, false, true);
        return;
    };
    // A generated id cannot collide knowingly; probe the id as sent.
    let mut k = [0u8; KEY_MAX];
    let Some(kn) = key_for(&desc, src, &mut k) else {
        exec.doc_index += 1;
        probe_next(exec);
        return;
    };
    let Some(bn) = stage_range_meta(exec, &k[..kn], &[], 0, 1) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, bn, P_INS_PROBE);
}

fn on_insert_probe(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let found = Kvs::parse(body).is_some_and(|k| k.count > 0);
    if found {
        let i = exec.doc_index as u32;
        // Discard the staged answer: nothing was inserted.
        resp_begin(exec);
        fail(exec, OUT_EXISTS, i, &[], msg_exists());
        return;
    }
    exec.doc_index += 1;
    probe_next(exec);
}

// ── One document by id ────────────────────────────────────────────────

/// The addressed document's key: `FIELD_ID`, or a replace's document.
fn addressed_key(exec: &mut ExecState, out: &mut [u8]) -> Option<usize> {
    let rq = req_view(exec);
    let n = rq.len();
    let req = decode_req(&rq[..n])?;
    let dn = exec.desc_len as usize;
    let desc = Descriptor::parse(&exec.desc[..dn]).ok()?;
    if let Some(id) = req.id() {
        if desc.shard_path().is_some() {
            // A sharded collection addresses by document (its shard key).
            return req.doc().and_then(|d| key_for(&desc, d, out));
        }
        if !id_is_usable(id) {
            return None;
        }
        let mut idk = [0u8; models::MAX_DOCUMENT_ID_KEY];
        let idn = id_key(id, &mut idk)?;
        return doc_key(&desc, &[], &idk[..idn], out);
    }
    key_for(&desc, req.doc()?, out)
}

fn read_one(exec: &mut ExecState) {
    let mut k = [0u8; KEY_MAX];
    let Some(kn) = addressed_key(exec, &mut k) else {
        fail(exec, OUT_INVALID, 0, &[], msg_no_id());
        return;
    };
    exec.key[..kn].copy_from_slice(&k[..kn]);
    exec.key_len = kn as u16;
    let Some(bn) = stage_range_meta(exec, &k[..kn], &[], 0, 1) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, bn, P_READ);
}

fn on_read(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let Some(kvs) = Kvs::parse(body) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let entry = kvs.entries().next();
    match entry {
        Some((_, rec, mod_rev)) => {
            let ExecState {
                desc,
                desc_len,
                codec,
                stored,
                stored_len,
                owners,
                owners_len,
                ..
            } = &mut *exec;
            if hold_record(
                &desc[..*desc_len as usize],
                codec,
                rec,
                stored,
                stored_len,
                owners,
                owners_len,
            )
            .is_none()
            {
                fail_plain(exec, OUT_CORRUPT);
                return;
            }
            exec.read_rev = mod_rev;
            exec.m_docs_read = exec.m_docs_read.wrapping_add(1);
        }
        None => {
            exec.stored_len = 0;
            exec.owners_len = 0;
            exec.read_rev = 0;
        }
    }
    let _ = revision;
    match req_op(exec) {
        OP_GET => answer_get(exec, revision),
        OP_REPLACE => write_replace(exec),
        OP_UPDATE => write_update(exec),
        OP_PATCH_MERGE | OP_PATCH_JSON | OP_PATCH_KEYED | OP_APPLY => write_patch(exec),
        OP_DELETE => write_delete(exec),
        _ => fail_plain(exec, OUT_MALFORMED),
    }
}

/// Hold a stored record: its tree in `stored`, its ownership (if it
/// carries one) in `owners`.
fn hold_record(
    desc: &[u8],
    codec: &mut Codec,
    rec: &[u8],
    stored: &mut [u8],
    stored_len: &mut u32,
    owners: &mut [u8],
    owners_len: &mut u32,
) -> Option<()> {
    let d = Descriptor::parse(desc).ok()?;
    if doc_codec::is_json(&d) {
        let (mut b, _) = codec.split();
        let (dn, on) = doc_codec::decode_json(&d, rec, &mut b, stored, owners)?;
        *stored_len = dn as u32;
        *owners_len = on as u32;
        return Some(());
    }
    let (doc, own) = record_parts(rec).ok()?;
    let b = doc.bytes();
    stored.get_mut(..b.len())?.copy_from_slice(b);
    *stored_len = b.len() as u32;
    let o = own.unwrap_or(&[]);
    owners.get_mut(..o.len())?.copy_from_slice(o);
    *owners_len = o.len() as u32;
    Some(())
}

/// Answer the document in `stored`: its revision stamped where the
/// descriptor says, then projected. `false` when it cannot be (the
/// response's overflow flag says whether it was room).
fn push_doc(exec: &mut ExecState, rev: u64) -> bool {
    let ExecState {
        req,
        req_len,
        desc,
        desc_len,
        stored,
        stored_len,
        owners,
        owners_len,
        buf_a,
        buf_b,
        resp,
        resp_len,
        resp_overflow,
        ..
    } = exec;
    let Some(req) = decode_req(&req[..*req_len as usize]) else {
        return false;
    };
    let Ok(desc) = Descriptor::parse(&desc[..*desc_len as usize]) else {
        return false;
    };
    let mut bufs = Buffers::new(buf_a, buf_b);
    if bufs
        .load(Value::trusted(&stored[..*stored_len as usize]))
        .is_err()
    {
        return false;
    }
    if let Some((form, rp)) = desc.revision_path() {
        let mut vb = [0u8; 40];
        let mut w = Writer::new(&mut vb);
        let r = if form == FORM_TEXT {
            let mut t = [0u8; 20];
            let tn = fmt_u64(rev, &mut t);
            w.string(&t[..tn])
        } else {
            w.int(rev as i64)
        };
        if r.is_err() {
            return false;
        }
        let Ok(vn) = w.finish() else { return false };
        if bufs.edit(rp, Edit::Set(Value::trusted(&vb[..vn]))).is_err() {
            return false;
        }
    }
    if let Some(list) = req.projection() {
        if bufs.apply(|cur, w| project(cur, list, w)).is_err() {
            return false;
        }
    }
    // A collection that tracks ownership answers it after the document:
    // both, or neither.
    let own: &[u8] = if desc.ownership() && *owners_len > 2 {
        &owners[..*owners_len as usize]
    } else {
        &[]
    };
    let doc = bufs.current().bytes();
    let own_field = if own.is_empty() {
        0
    } else {
        FIELD_HEAD + own.len()
    };
    if *resp_len + FIELD_HEAD + 8 + doc.len() + own_field > RESPONSE_MAX {
        *resp_overflow = true;
        return false;
    }
    field_into(
        resp,
        resp_len,
        resp_overflow,
        RESP_DOC,
        &[&rev.to_le_bytes(), doc],
    ) && (own.is_empty() || field_into(resp, resp_len, resp_overflow, RESP_OWNERS, &[own]))
}

fn fmt_u64(mut v: u64, out: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut k = 0;
    loop {
        tmp[k] = b'0' + (v % 10) as u8;
        v /= 10;
        k += 1;
        if v == 0 {
            break;
        }
    }
    for j in 0..k {
        out[j] = tmp[k - 1 - j];
    }
    k
}

fn answer_get(exec: &mut ExecState, revision: u64) {
    if exec.read_rev == 0 {
        fail_plain(exec, OUT_NOT_FOUND);
        return;
    }
    let rev = exec.read_rev;
    if !push_doc(exec, rev) {
        fail_plain(
            exec,
            if exec.resp_overflow {
                OUT_TOO_LARGE
            } else {
                OUT_CORRUPT
            },
        );
        return;
    }
    resp_send(exec, OUT_OK, 0, revision);
}

/// Check the request's revision and value preconditions against the
/// stored document. Answers the refusal itself.
fn preconditions_hold(exec: &mut ExecState) -> bool {
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return false;
    };
    if let Some(want) = req.if_revision() {
        if want != exec.read_rev {
            fail(exec, OUT_CONFLICT, 0, &[], msg_revision());
            return false;
        }
    }
    let sl = exec.stored_len as usize;
    let sb = hold(&mut exec.hold_stored, &exec.stored[..sl]);
    let stored = Value::trusted(&sb[..sl]);
    for (p, v) in req.preconditions() {
        let ok = lookup(stored, p).is_some_and(|s| doc_tree::equal(s, v));
        if !ok {
            let mut pb = [0u8; 1024];
            let pl = p.bytes().len().min(pb.len());
            pb[..pl].copy_from_slice(&p.bytes()[..pl]);
            fail(exec, OUT_CONFLICT, 0, &pb[..pl], msg_precondition());
            return false;
        }
    }
    true
}

/// A lost compare-and-set: re-read and re-apply, or answer `CONFLICT`
/// when the request named its revision or the retries are spent. An
/// operation addressed by filter looks for its match again — the
/// document it found may no longer be one.
fn cas_lost(exec: &mut ExecState) {
    let (named, by_filter) = match decode_req(&exec.req[..exec.req_len as usize]) {
        Some(r) => (
            r.if_revision().is_some(),
            r.id().is_none() && r.filter().is_some(),
        ),
        None => (true, false),
    };
    if named || exec.retries as usize >= MAX_CAS_RETRIES {
        fail(exec, OUT_CONFLICT, 0, &[], msg_revision());
        return;
    }
    exec.retries += 1;
    resp_begin(exec);
    exec.matched = 0;
    exec.modified = 0;
    exec.after_len = 0;
    exec.page_len = 0;
    exec.page_at = 0;
    let _ = by_filter;
    // From the descriptor: the schema may be what moved.
    let mut nb = [0u8; doc_catalog::MAX_NAME_LEN];
    let nl = match decode_req(&exec.req[..exec.req_len as usize]).and_then(|r| r.collection()) {
        Some(n) => {
            nb[..n.len()].copy_from_slice(n);
            n.len()
        }
        None => {
            fail_plain(exec, OUT_MALFORMED);
            return;
        }
    };
    read_descriptor(exec, &nb[..nl]);
}

/// Finish a single-document write: size, identity, then the TXN.
fn commit_doc(exec: &mut ExecState, doc: &[u8], next: u8) {
    exec.next_owners_len = 0;
    let owned =
        Descriptor::parse(&exec.desc[..exec.desc_len as usize]).is_ok_and(|d| d.ownership());
    if !owned {
        commit_record(exec, doc, next);
        return;
    }
    match own_transition(exec, doc) {
        Err(()) => {}
        Ok(None) => commit_record(exec, doc, next),
        Ok(Some(n)) => {
            // An apply's removals made a new document, in `buf_a`.
            let d = hold(&mut exec.hold_commit, &exec.buf_a[..n]);
            commit_record(exec, d, next);
        }
    }
}

/// The ownership this write makes, into `next_owners`: an update takes
/// the fields it changed; an apply owns its configuration's fields,
/// conflicts refused (unless forced), and the fields it stopped naming
/// removed — the document with them removed answered in `buf_a`
/// (`Ok(Some(len))`). A refusal is answered here (`Err`).
fn own_transition(exec: &mut ExecState, doc: &[u8]) -> Result<Option<usize>, ()> {
    let rq = req_view(exec);
    let rn = rq.len();
    let Some(req) = decode_req(&rq[..rn]) else {
        fail_plain(exec, OUT_MALFORMED);
        return Err(());
    };
    let Some(manager) = req.manager() else {
        fail(exec, OUT_INVALID, 0, &[], msg_no_manager());
        return Err(());
    };
    let ms = unsafe { dev_unix_millis(&*exec.syscalls) };
    if ms == 0 {
        fail(exec, OUT_UNAVAILABLE, 0, &[], msg_no_clock());
        return Err(());
    }
    let who = Actor {
        manager,
        scope: req.scope_name().unwrap_or(&[]),
        secs: (ms / 1000) as i64,
        nanos: ((ms % 1000) * 1_000_000) as u32,
    };
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return Err(());
    };
    let keyed = Keyed {
        desc,
        dir: req.directives(),
    };
    let schema = Schema {
        keyed: &keyed,
        unowned: Unowned::Declared(desc),
        version: desc.version_path(),
    };
    let force = req.flags & FLAG_FORCE != 0;
    let config = if req.op == OP_APPLY { req.doc() } else { None };
    let mut conflicted = false;
    let r = {
        let ExecState {
            stored,
            stored_len,
            read_rev,
            owners,
            owners_len,
            next_owners,
            own_a,
            own_b,
            own_c,
            own_path,
            buf_a,
            buf_b,
            resp,
            resp_len,
            resp_overflow,
            ..
        } = &mut *exec;
        let old = (*read_rev != 0).then(|| Value::trusted(&stored[..*stored_len as usize]));
        let held: &[u8] = if *read_rev != 0 && *owners_len > 0 {
            &owners[..*owners_len as usize]
        } else {
            &NO_OWNERS
        };
        let mut sc = Scratch {
            a: own_a,
            b: own_b,
            c: own_c,
            path: own_path,
        };
        let new = Value::trusted(doc);
        Owners::parse(held).and_then(|prev| match config {
            None => fields::record_update(prev, old, new, &who, &schema, &mut sc, next_owners)
                .map(|n| (None, n)),
            Some(cfg) => fields::record_apply(
                prev,
                old,
                cfg,
                new,
                &who,
                force,
                &schema,
                &mut sc,
                (buf_a, buf_b),
                next_owners,
                |c| {
                    conflicted = true;
                    if force {
                        // Taken, not refused: nothing to report.
                        return;
                    }
                    let parts: [&[u8]; 6] = [
                        &[c.manager.len() as u8],
                        c.manager,
                        &[c.op],
                        &[c.version.len() as u8],
                        c.version,
                        c.path,
                    ];
                    field_into(resp, resp_len, resp_overflow, RESP_CONFLICT, &parts);
                },
            )
            .map(|a| match a {
                Applied::Done { doc, owners } => (Some(doc), owners),
                Applied::Conflicts(_) => (None, 0),
            }),
        })
    };
    match r {
        Ok(_) if conflicted && !force => {
            resp_send(exec, OUT_OWNERSHIP, 0, exec.read_rev);
            Err(())
        }
        Ok((d, n)) => {
            exec.next_owners_len = n as u32;
            Ok(d)
        }
        Err(FieldError::Overflow) => {
            fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large());
            Err(())
        }
        Err(FieldError::TooDeep) => {
            fail(exec, OUT_INVALID, 0, &[], msg_too_deep());
            Err(())
        }
        Err(FieldError::Malformed) => {
            fail_plain(exec, OUT_CORRUPT);
            Err(())
        }
    }
}

/// The compared write of `doc` (and the ownership in `next_owners`) over
/// the stored document.
fn commit_record(exec: &mut ExecState, doc: &[u8], next: u8) {
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let new = Value::trusted(doc);
    if !fits(&desc, new) {
        fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large());
        return;
    }
    if exec.read_rev != 0 {
        let sl = exec.stored_len as usize;
        let sb = hold(&mut exec.hold_stored, &exec.stored[..sl]);
        let stored = Value::trusted(&sb[..sl]);
        if let Some(p) = changed_path(stored, new, desc.id_paths().chain(desc.identity_paths())) {
            fail(exec, OUT_INVALID, 0, p.bytes(), msg_identity());
            return;
        }
        if doc_tree::equal(stored, new) && stored.bytes() == new.bytes() && owners_unchanged(exec) {
            // Nothing changes: no write, the stored revision stands.
            exec.matched += 1;
            finish_one(exec, doc, exec.read_rev, false);
            return;
        }
        if marked_deleting(&desc, stored) && !guard_holds(&desc, new) {
            // The guard emptied on a document marked deleting: the held
            // delete completes, in this write's compared transaction.
            exec.resp_flags |= RFLAG_REMOVED;
            if req_flags(exec) & FLAG_DRY_RUN != 0 {
                exec.matched += 1;
                finish_one(exec, doc, exec.read_rev, true);
                return;
            }
            let mut k = [0u8; KEY_MAX];
            let kn = exec.key_len as usize;
            k[..kn].copy_from_slice(&exec.key[..kn]);
            let rev = exec.read_rev;
            let bn = match stage_doc_write(exec, &k[..kn], rev, None) {
                Ok(n) => n,
                Err(e) => {
                    fail_op(exec, 0, &[], e);
                    return;
                }
            };
            exec.stored_len = doc.len() as u32;
            exec.stored[..doc.len()].copy_from_slice(doc);
            kv_send(exec, KV_OP_TXN_STAGED, bn, next);
            return;
        }
    }
    let mut k = [0u8; KEY_MAX];
    let kn = exec.key_len as usize;
    k[..kn].copy_from_slice(&exec.key[..kn]);
    // The key must be the document's own.
    let mut own = [0u8; KEY_MAX];
    match key_for(&desc, new, &mut own) {
        Some(on) if own[..on] == k[..kn] => {}
        _ => {
            fail(
                exec,
                OUT_INVALID,
                0,
                desc.id_path().bytes(),
                msg_id_mismatch(),
            );
            return;
        }
    }
    if req_flags(exec) & FLAG_DRY_RUN != 0 {
        exec.matched += 1;
        finish_one(exec, doc, exec.read_rev, true);
        return;
    }
    let rev = exec.read_rev;
    let bn = match stage_doc_write(exec, &k[..kn], rev, Some(doc)) {
        Ok(n) => n,
        Err(e) => {
            fail_op(exec, 0, &[], e);
            return;
        }
    };
    // The stored document is no longer needed: hold the new one for the
    // answer.
    exec.stored_len = doc.len() as u32;
    exec.stored[..doc.len()].copy_from_slice(doc);
    kv_send(exec, KV_OP_TXN_STAGED, bn, next);
}

/// Does the write leave the stored ownership as it was?
fn owners_unchanged(exec: &ExecState) -> bool {
    let next = &exec.next_owners[..exec.next_owners_len as usize];
    let held: &[u8] = if exec.owners_len > 0 {
        &exec.owners[..exec.owners_len as usize]
    } else {
        &NO_OWNERS
    };
    next.is_empty() || next == held
}

/// The single-document answer after a write (or a write that changed
/// nothing): the document, and the count — or, when the write created
/// it (an upsert), its id and a count of nothing matched.
fn finish_one(exec: &mut ExecState, doc: &[u8], rev: u64, modified: bool) {
    let created = exec.read_rev == 0;
    if modified && !created {
        exec.modified += 1;
    }
    if created {
        exec.matched = 0;
    }
    exec.stored_len = doc.len() as u32;
    exec.stored[..doc.len()].copy_from_slice(doc);
    // The answer shows the ownership the write stored.
    let on = exec.next_owners_len as usize;
    if on > 0 {
        let ExecState {
            owners,
            owners_len,
            next_owners,
            ..
        } = &mut *exec;
        owners[..on].copy_from_slice(&next_owners[..on]);
        *owners_len = on as u32;
    }
    let (m, md) = (exec.matched, exec.modified);
    if created {
        let ExecState {
            desc,
            desc_len,
            stored,
            stored_len,
            resp,
            resp_len,
            resp_overflow,
            ..
        } = &mut *exec;
        if let Ok(d) = Descriptor::parse(&desc[..*desc_len as usize]) {
            let mut ib = [0u8; ID_VALUE_MAX];
            let mut w = Writer::new(&mut ib);
            let doc = Value::trusted(&stored[..*stored_len as usize]);
            if doc_id(&d, doc, &mut w).is_some() {
                if let Ok(n) = w.finish() {
                    field_into(resp, resp_len, resp_overflow, RESP_ID, &[&ib[..n]]);
                }
            }
        }
    }
    if !push_doc(exec, rev) && !exec.resp_overflow {
        fail_plain(exec, OUT_CORRUPT);
        return;
    }
    resp_field(exec, RESP_COUNT, &[&m.to_le_bytes(), &md.to_le_bytes()]);
    resp_send(exec, OUT_OK, if created { RFLAG_CREATED } else { 0 }, rev);
}

fn on_write(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if result != KV_RESULT_TXN {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if body.first() != Some(&1) {
        why_refused(exec, false, false);
        return;
    }
    exec.m_docs_written = exec.m_docs_written.wrapping_add(1);
    if exec.read_rev != 0 {
        exec.matched += 1;
    }
    if req_op(exec) == OP_DELETE && exec.resp_flags & RFLAG_HELD == 0 {
        let (m, md) = (exec.matched, exec.modified + 1);
        // The deleted document, as it was.
        let rev = exec.read_rev;
        let _ = push_doc(exec, rev);
        resp_field(exec, RESP_COUNT, &[&m.to_le_bytes(), &md.to_le_bytes()]);
        resp_send(exec, OUT_OK, 0, revision);
        return;
    }
    let dl = exec.stored_len as usize;
    let doc = hold(&mut exec.hold_stored, &exec.stored[..dl]);
    finish_one(exec, &doc[..dl], revision, true);
}

// ── Replace, update, delete ───────────────────────────────────────────

fn write_replace(exec: &mut ExecState) {
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let Some(newdoc) = req.doc() else {
        fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op());
        return;
    };
    if exec.read_rev == 0 {
        if req.flags & FLAG_UPSERT == 0 {
            fail_plain(exec, OUT_NOT_FOUND);
            return;
        }
        create_at_key(exec, newdoc);
        return;
    }
    if !preconditions_hold(exec) {
        return;
    }
    scoped_write(exec, newdoc, req.write_scope(), true);
}

/// Write `newdoc` over the stored document: through the request's write
/// scope, with the stored id when it states none, carrying the declared
/// kept paths (a replace — a patch starts from the stored document, and
/// a patch that touches a kept path is an identity question, not a
/// carry), then size, identity, and the compared write.
fn scoped_write(exec: &mut ExecState, newdoc: Value<'_>, scope: Option<PathList<'_>>, keep: bool) {
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let sl = exec.stored_len as usize;
    let sb = hold(&mut exec.hold_stored, &exec.stored[..sl]);
    let stored = Value::trusted(&sb[..sl]);
    let (a, b) = (&mut exec.buf_a, &mut exec.buf_b);
    let mut bufs = Buffers::new(a, b);
    let r = (|| {
        match scope {
            // Only the scope's paths change: start from the stored
            // document and carry them in from the new one.
            Some(scope) if scope.include() => {
                bufs.load(stored)?;
                carry(&mut bufs, newdoc, scope.paths())?;
            }
            Some(scope) => {
                bufs.load(newdoc)?;
                carry(&mut bufs, stored, scope.paths())?;
            }
            None => bufs.load(newdoc)?,
        }
        // The id is the stored one where the new document states none.
        carry_id(&desc, stored, &mut bufs)?;
        if keep {
            carry(&mut bufs, stored, desc.keep_paths())?;
        }
        Ok(())
    })();
    if let Err(e) = r {
        fail_op(exec, 0, &[], e);
        return;
    }
    let cur = bufs.current().bytes();
    let doc = hold(&mut exec.hold_doc, cur);
    let dl = doc.len();
    commit_doc(exec, &doc[..dl], P_WRITE);
}

/// A patch — RFC 7386, RFC 6902, or the keyed merge the collection's list
/// keys and the request's directives describe — applied to the stored
/// document.
fn write_patch(exec: &mut ExecState) {
    if exec.read_rev == 0 {
        if req_op(exec) == OP_APPLY {
            // An apply creates what is not there: its configuration.
            let rq = req_view(exec);
            let n = rq.len();
            match decode_req(&rq[..n]).and_then(|r| r.doc()) {
                Some(cfg) => create_at_key(exec, cfg),
                None => fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op()),
            }
            return;
        }
        fail_plain(exec, OUT_NOT_FOUND);
        return;
    }
    if !preconditions_hold(exec) {
        return;
    }
    let r = {
        let ExecState {
            req,
            req_len,
            desc,
            desc_len,
            stored,
            stored_len,
            buf_a,
            buf_b,
            page,
            hold_patch,
            ..
        } = &mut *exec;
        (|| {
            let req = decode_req(&req[..*req_len as usize])
                .ok_or((0, OpError::from(doc_tree::TreeError::BadFormat)))?;
            // An apply's configuration merges as a keyed patch does.
            let patch = if req.op == OP_APPLY {
                req.doc()
            } else {
                req.patch()
            }
            .ok_or((0, OpError::from(doc_tree::TreeError::BadFormat)))?;
            let stored = Value::trusted(&stored[..*stored_len as usize]);
            let mut bufs = Buffers::new(buf_a, buf_b);
            bufs.load(stored).map_err(|e| (0, e))?;
            match req.op {
                OP_PATCH_JSON => json_patch(&mut bufs, patch, page)?,
                OP_PATCH_MERGE => bufs
                    .apply(|cur, w| merge_patch(cur, patch, None, w))
                    .map_err(|e| (0, e))?,
                _ => {
                    let d = Descriptor::parse(&desc[..*desc_len as usize])
                        .map_err(|_| (0, OpError::from(doc_tree::TreeError::BadFormat)))?;
                    let keyed = Keyed {
                        desc: d,
                        dir: req.directives(),
                    };
                    bufs.apply(|cur, w| merge_patch(cur, patch, Some(&keyed), w))
                        .map_err(|e| (0, e))?
                }
            }
            let cur = bufs.current();
            if cur.ty() != doc_tree::T_OBJECT {
                // A patch whose result is not a document (a merge patch that
                // is not an object).
                return Err((0, OpError::from(doc_tree::TreeError::BadFormat)));
            }
            let b = cur.bytes();
            hold(hold_patch, b);
            Ok(b.len())
        })()
    };
    match r {
        Ok(n) => {
            let rq = req_view(exec);
            let rl = rq.len();
            let scope = decode_req(&rq[..rl]).and_then(|r| r.write_scope());
            let pd = view(&exec.hold_patch, n);
            scoped_write(exec, Value::trusted(pd), scope, false);
        }
        Err((i, e)) => fail_op(exec, i as u32, &[], e),
    }
}

fn write_update(exec: &mut ExecState) {
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let Some(muts) = req.mutations() else {
        fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op());
        return;
    };
    if exec.read_rev == 0 {
        if req.flags & FLAG_UPSERT == 0 {
            fail_plain(exec, OUT_NOT_FOUND);
            return;
        }
        // An upsert: the seed (the dialect's, or empty) with the id,
        // then the mutations, created as an insert is.
        upsert_from_seed(exec);
        return;
    }
    if !preconditions_hold(exec) {
        return;
    }
    let sl = exec.stored_len as usize;
    let sb = hold(&mut exec.hold_stored, &exec.stored[..sl]);
    let (a, b) = (&mut exec.buf_a, &mut exec.buf_b);
    let mut bufs = Buffers::new(a, b);
    if let Err(e) = bufs.load(Value::trusted(&sb[..sl])) {
        fail_op(exec, 0, &[], e);
        return;
    }
    if let Err((i, e)) = apply_mutations(&mut bufs, muts) {
        let p = muts.iter().nth(i).map(|m| m.path.bytes()).unwrap_or(&[]);
        let mut pb = [0u8; 1024];
        let pl = p.len().min(pb.len());
        pb[..pl].copy_from_slice(&p[..pl]);
        fail_op(exec, i as u32, &pb[..pl], e);
        return;
    }
    let cur = bufs.current().bytes();
    let doc = hold(&mut exec.hold_doc, cur);
    let dl = doc.len();
    commit_doc(exec, &doc[..dl], P_WRITE);
}

fn upsert_from_seed(exec: &mut ExecState) {
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let empty = [doc_tree::T_OBJECT, 0, 0, 0, 0, 0, 0, 0, 0];
    let seed = req.doc().unwrap_or(Value::trusted(&empty));
    let (a, b) = (&mut exec.buf_a, &mut exec.buf_b);
    let mut bufs = Buffers::new(a, b);
    let r = (|| {
        bufs.load(seed)?;
        if let Some(id) = req.id() {
            set_id(&desc, id, &mut bufs)?;
        }
        if let Some(m) = req.mutations() {
            apply_mutations(&mut bufs, m).map_err(|(_, e)| e)?;
        }
        Ok::<(), OpError>(())
    })();
    if let Err(e) = r {
        fail_op(exec, 0, &[], e);
        return;
    }
    let cur = bufs.current().bytes();
    let doc = hold(&mut exec.hold_doc, cur);
    let dl = doc.len();
    create_at_key(exec, Value::trusted(&doc[..dl]));
}

/// Create `source` at `exec.key` (absent-compared), stamped as an
/// insert is.
fn create_at_key(exec: &mut ExecState, source: Value<'_>) {
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let src = hold(&mut exec.hold_src, source.bytes());
    let sl = src.len();
    let Some((random, now)) = stamp_context(exec, needs_clock(&desc)) else {
        return;
    };
    let (a, b) = (&mut exec.buf_a, &mut exec.buf_b);
    let mut bufs = Buffers::new(a, b);
    if let Err((outcome, msg, _)) = build_created(
        &desc,
        Value::trusted(&src[..sl]),
        None,
        &mut bufs,
        &random,
        now,
    ) {
        fail(exec, outcome, 0, &[], msg);
        return;
    }
    let cur = bufs.current().bytes();
    let doc = hold(&mut exec.hold_doc, cur);
    let dl = doc.len();
    exec.read_rev = 0;
    commit_doc(exec, &doc[..dl], P_WRITE);
}

fn write_delete(exec: &mut ExecState) {
    if exec.read_rev == 0 {
        fail_plain(exec, OUT_NOT_FOUND);
        return;
    }
    if !preconditions_hold(exec) {
        return;
    }
    if hold_delete(exec) {
        return;
    }
    if req_flags(exec) & FLAG_DRY_RUN != 0 {
        let rev = exec.read_rev;
        let _ = push_doc(exec, rev);
        resp_field(
            exec,
            RESP_COUNT,
            &[&1u64.to_le_bytes(), &1u64.to_le_bytes()],
        );
        resp_send(exec, OUT_OK, 0, rev);
        return;
    }
    let mut k = [0u8; KEY_MAX];
    let kn = exec.key_len as usize;
    k[..kn].copy_from_slice(&exec.key[..kn]);
    let rev = exec.read_rev;
    let bn = match stage_doc_write(exec, &k[..kn], rev, None) {
        Ok(n) => n,
        Err(e) => {
            fail_op(exec, 0, &[], e);
            return;
        }
    };
    kv_send(exec, KV_OP_TXN_STAGED, bn, P_WRITE);
}

/// A delete the collection's guard holds: mark the document deleting (its
/// deleting stamps) and keep it. `true` when the guard took the delete
/// (answered or in flight).
fn hold_delete(exec: &mut ExecState) -> bool {
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        return false;
    };
    let sl = exec.stored_len as usize;
    let sb = hold(&mut exec.hold_stored, &exec.stored[..sl]);
    let stored = Value::trusted(&sb[..sl]);
    if !guard_holds(&desc, stored) {
        return false;
    }
    exec.resp_flags |= RFLAG_HELD;
    if marked_deleting(&desc, stored) {
        // Already marked: the delete is where it was.
        let rev = exec.read_rev;
        exec.matched = 1;
        let _ = push_doc(exec, rev);
        resp_field(
            exec,
            RESP_COUNT,
            &[&1u64.to_le_bytes(), &0u64.to_le_bytes()],
        );
        resp_send(exec, OUT_OK, 0, rev);
        return true;
    }
    let needs_clock = desc.deleting_stamps().any(|s| s.generator.kind == GEN_NOW);
    let Some((random, now)) = stamp_context(exec, needs_clock) else {
        return true;
    };
    let mut rnd = [0u8; 16];
    rnd.copy_from_slice(&random[..16]);
    let mut ib = [0u8; ID_VALUE_MAX];
    let id = {
        let mut w = Writer::new(&mut ib);
        doc_id(&desc, stored, &mut w)
            .and_then(|_| w.finish().ok())
            .map(|n| Value::trusted(&ib[..n]))
    };
    let ctx = StampContext {
        now,
        random: rnd,
        id,
    };
    let (a, b) = (&mut exec.buf_a, &mut exec.buf_b);
    let mut bufs = Buffers::new(a, b);
    let r = (|| {
        bufs.load(stored)?;
        for st in desc.deleting_stamps() {
            let mut vb = [0u8; 1024];
            let mut w = Writer::new(&mut vb);
            generate(&st.generator, &ctx, &mut w)?;
            let vn = w.finish()?;
            bufs.edit(st.path, Edit::Set(Value::trusted(&vb[..vn])))?;
        }
        Ok::<(), OpError>(())
    })();
    if let Err(e) = r {
        fail_op(exec, 0, &[], e);
        return true;
    }
    let cur = bufs.current().bytes();
    let doc = hold(&mut exec.hold_doc, cur);
    let dl = doc.len();
    if req_flags(exec) & FLAG_DRY_RUN != 0 {
        exec.matched += 1;
        finish_one(exec, &doc[..dl], exec.read_rev, true);
        return true;
    }
    let mut k = [0u8; KEY_MAX];
    let kn = exec.key_len as usize;
    k[..kn].copy_from_slice(&exec.key[..kn]);
    let rev = exec.read_rev;
    // A held delete changes no one's fields: the ownership is carried.
    exec.next_owners_len = 0;
    let bn = match stage_doc_write(exec, &k[..kn], rev, Some(&doc[..dl])) {
        Ok(n) => n,
        Err(e) => {
            fail_op(exec, 0, &[], e);
            return true;
        }
    };
    exec.stored_len = dl as u32;
    exec.stored[..dl].copy_from_slice(&doc[..dl]);
    kv_send(exec, KV_OP_TXN_STAGED, bn, P_WRITE);
    true
}

// ── Watch: a collection's changes after a revision ─────────────────────
//
// Answered as a page of events in revision order, never splitting one
// revision's events: a first pass over the versions in (from, now] finds
// the largest run of whole revisions that fits the answer (the budget the
// dialect states, and the edit buffer the events are ordered through); a
// second pass emits exactly those. The answer's revision is where the
// next watch resumes; `RFLAG_MORE` says changes remain past it. History
// the store no longer holds (a memory store, or below the compaction
// floor) answers `COMPACTED`.

fn watch_begin(exec: &mut ExecState) {
    let from = decode_req(&exec.req[..exec.req_len as usize]).and_then(|r| r.at_revision());
    let Some(from) = from else {
        fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op());
        return;
    };
    exec.w_from = from;
    // The window's ceiling is named, never "now": a store resolves "now"
    // for a window from its scan memo, which an earlier request of the
    // same window may hold.
    exec.w_hi = exec.store_rev;
    if from >= exec.w_hi {
        // Nothing after `from` yet.
        resp_send(exec, OUT_OK, 0, from);
        return;
    }
    exec.w_cut = 0;
    exec.w_cursor = 0;
    exec.w_n = 0;
    exec.w_dropped = u64::MAX;
    watch_page(exec, P_WATCH_SIZE);
}

fn watch_page(exec: &mut ExecState, phase: u8) {
    let mut start = [0u8; KEY_MAX];
    let mut end = [0u8; KEY_MAX];
    let Some((sn, en)) = request_span(exec, &mut start, &mut end) else {
        fail(exec, OUT_INVALID, 0, &[], msg_bad_prefix());
        return;
    };
    let to = if phase == P_WATCH_SIZE {
        exec.w_hi
    } else {
        exec.w_cut
    };
    let (from, cursor) = (exec.w_from, exec.w_cursor);
    let mut p = BODY_AT;
    let e = &mut exec.env;
    let ok = (|| {
        put(e, &mut p, &(sn as u16).to_le_bytes())?;
        put(e, &mut p, &start[..sn])?;
        put(e, &mut p, &(en as u16).to_le_bytes())?;
        put(e, &mut p, &end[..en])?;
        put(e, &mut p, &from.to_le_bytes())?;
        put(e, &mut p, &to.to_le_bytes())?;
        put(e, &mut p, &cursor.to_le_bytes())?;
        put(e, &mut p, &(WATCH_PAGE as u16).to_le_bytes())?;
        // Each put's metadata: whether its revision created the document.
        put(e, &mut p, &[types::VERSIONS_FLAG_META])
    })();
    if ok.is_none() {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    }
    kv_send(exec, KV_OP_SCAN_VERSIONS, p - BODY_AT, phase);
}

/// One version of a `KV_RESULT_VERSIONS` page: `(revision, kind, key,
/// value)`.
/// `(revision, kind, key, value, created)`: `created` when the version's
/// revision is its key's creation (the scan's metadata says so).
type Version<'a> = (u64, u8, &'a [u8], &'a [u8], bool);

/// A `KV_RESULT_VERSIONS` page: `(next_cursor, entries)`.
fn versions(b: &[u8]) -> Option<(u64, impl Iterator<Item = Version<'_>> + '_)> {
    let next = u64::from_le_bytes(b.get(..8)?.try_into().ok()?);
    let count = u16::from_le_bytes(b.get(8..10)?.try_into().ok()?) as usize;
    let mut at = 10usize;
    let mut left = count;
    Some((
        next,
        core::iter::from_fn(move || {
            if left == 0 {
                return None;
            }
            let rev = u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?);
            let kind = *b.get(at + 16)?;
            let kl = u16::from_le_bytes(b.get(at + 17..at + 19)?.try_into().ok()?) as usize;
            let key = b.get(at + 19..at + 19 + kl)?;
            let vs = at + 19 + kl;
            let vl = u32::from_le_bytes(b.get(vs..vs + 4)?.try_into().ok()?) as usize;
            let val = b.get(vs + 4..vs + 4 + vl)?;
            at = vs + 4 + vl;
            // A put carries its metadata (`VERSIONS_FLAG_META`):
            // `[create_revision][version][lease]`.
            let mut created = false;
            if kind == types::VERSION_KIND_PUT {
                let cr = u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?);
                created = cr == rev;
                at += 24;
            }
            left -= 1;
            Some((rev, kind, key, val, created))
        }),
    ))
}

/// The answer's byte budget for events: the dialect's, and what the edit
/// buffer they are ordered through holds.
fn watch_budget(exec: &ExecState) -> usize {
    exec.resp_budget.min(DOC_BUF - 64)
}

fn on_watch_size(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if result != KV_RESULT_VERSIONS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let _ = revision;
    let Some((next, entries)) = versions(body) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    for (rev, _, _, val, _) in entries {
        // An event's answer is at most its field head, the revision and
        // kind, and the record's tree (or an id): the value plus a margin.
        let size = (val.len() + 64) as u32;
        let n = exec.w_n;
        if n < WATCH_CANDIDATES {
            exec.w_cand[n] = (rev, size);
            exec.w_n = n + 1;
            continue;
        }
        // Full: keep the smallest revisions.
        let (mut mi, mut mr) = (0usize, 0u64);
        for (i, c) in exec.w_cand.iter().enumerate() {
            if c.0 >= mr {
                mi = i;
                mr = c.0;
            }
        }
        if rev < mr {
            exec.w_dropped = exec.w_dropped.min(mr);
            exec.w_cand[mi] = (rev, size);
        } else {
            exec.w_dropped = exec.w_dropped.min(rev);
        }
    }
    if next != 0 {
        exec.w_cursor = next;
        watch_page(exec, P_WATCH_SIZE);
        return;
    }
    // Sort the candidates by revision; take whole revisions while they fit.
    let n = exec.w_n;
    let c = &mut exec.w_cand[..n];
    for i in 1..n {
        let mut j = i;
        while j > 0 && c[j - 1].0 > c[j].0 {
            c.swap(j - 1, j);
            j -= 1;
        }
    }
    let budget = watch_budget(exec);
    let (mut acc, mut cut, mut i) = (0usize, exec.w_from, 0usize);
    while i < n {
        let rev = exec.w_cand[i].0;
        if rev >= exec.w_dropped {
            break;
        }
        let mut j = i;
        let mut group = 0usize;
        while j < n && exec.w_cand[j].0 == rev {
            group += exec.w_cand[j].1 as usize;
            j += 1;
        }
        if acc + group > budget {
            break;
        }
        acc += group;
        cut = rev;
        i = j;
    }
    if n == 0 {
        // Nothing changed: resume from the ceiling.
        let hi = exec.w_hi;
        resp_send(exec, OUT_OK, 0, hi);
        return;
    }
    if cut == exec.w_from {
        // The first revision's changes alone do not fit one answer.
        fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large());
        return;
    }
    exec.w_cut = cut;
    exec.w_cursor = 0;
    watch_page(exec, P_WATCH_EMIT);
}

fn on_watch_emit(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_VERSIONS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let r = {
        let ExecState {
            req,
            req_len,
            resp,
            resp_len,
            resp_overflow,
            buf_a,
            buf_b,
            desc,
            desc_len,
            codec,
            stored,
            owners,
            ..
        } = &mut *exec;
        (|| {
            let req = decode_req(&req[..*req_len as usize])?;
            let desc = Descriptor::parse(desc.get(..*desc_len as usize)?).ok()?;
            let json = doc_codec::is_json(&desc);
            let (next, entries) = versions(body)?;
            for (rev, kind, key, val, created) in entries {
                if kind == types::VERSION_KIND_DELETE {
                    let mut w = Writer::new(&mut buf_a[..]);
                    if json {
                        // `/<collection>/<id>`: the id its path spells.
                        let parts = if desc.compound_id() {
                            desc.id_paths().count()
                        } else {
                            1
                        };
                        doc_ops::path_id(desc.name(), key, parts, &mut w)?;
                    } else {
                        // `[keyspace][collection][shard][id]`: the id key.
                        let mut shard = [0u8; models::MAX_MODEL_ID_LEN];
                        let mut idk = [0u8; models::MAX_DOCUMENT_ID_KEY];
                        let (_, _, il) =
                            models::decode_document_data_key(key.get(4..)?, &mut shard, &mut idk)?;
                        doc_ops::id_from_key(&idk[..il], &mut w)?;
                    }
                    let n = w.finish().ok()?;
                    let parts: [&[u8]; 3] = [&rev.to_le_bytes(), &[EVENT_DELETE], &buf_a[..n]];
                    field_into(resp, resp_len, resp_overflow, RESP_EVENT, &parts);
                } else {
                    let (doc, own) = if json {
                        let (mut b, _) = codec.split();
                        let (dn, on) = doc_codec::decode_json(&desc, val, &mut b, stored, owners)?;
                        (Value::trusted(&stored[..dn]), Some(&owners[..on]))
                    } else {
                        record_parts(val).ok()?
                    };
                    if req.filter().is_some_and(|f| !matches(f, doc)) {
                        continue;
                    }
                    let ev = if created { EVENT_CREATE } else { EVENT_PUT };
                    // The document as that revision wrote it, its revision
                    // stamped where the collection says, as an answer is.
                    let mut bufs = Buffers::new(&mut buf_a[..], &mut buf_b[..]);
                    bufs.load(doc).ok()?;
                    if let Some((form, rp)) = desc.revision_path() {
                        let mut vb = [0u8; 40];
                        let mut w = Writer::new(&mut vb);
                        if form == FORM_TEXT {
                            let mut t = [0u8; 20];
                            let tn = fmt_u64(rev, &mut t);
                            w.string(&t[..tn]).ok()?;
                        } else {
                            w.int(rev as i64).ok()?;
                        }
                        let vn = w.finish().ok()?;
                        bufs.edit(rp, Edit::Set(Value::trusted(&vb[..vn]))).ok()?;
                    }
                    let parts: [&[u8]; 3] = [&rev.to_le_bytes(), &[ev], bufs.current().bytes()];
                    field_into(resp, resp_len, resp_overflow, RESP_EVENT, &parts);
                    // The document's ownership follows its event.
                    if let Some(o) = own.filter(|o| o.len() > 2) {
                        field_into(resp, resp_len, resp_overflow, RESP_OWNERS, &[o]);
                    }
                }
            }
            Some(next)
        })()
    };
    let Some(next) = r else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    if exec.resp_overflow {
        fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large());
        return;
    }
    if next != 0 {
        exec.w_cursor = next;
        watch_page(exec, P_WATCH_EMIT);
        return;
    }
    watch_order_events(exec);
    let (cut, hi) = (exec.w_cut, exec.w_hi);
    let flags = if cut < hi { RFLAG_MORE } else { 0 };
    resp_send(exec, OUT_OK, flags, cut);
}

/// Put the answer's events in revision order (the store walks by key):
/// a stable sort through the edit buffer.
fn watch_order_events(exec: &mut ExecState) {
    let base = wire::ENVELOPE_HDR + DOC_RESPONSE_HEAD;
    let end = wire::ENVELOPE_HDR + exec.resp_len;
    let region = end - base;
    if region > DOC_BUF {
        return;
    }
    let mut idx = [(0u64, 0u32, 0u32); WATCH_CANDIDATES];
    let mut n = 0usize;
    let mut at = base;
    while at + FIELD_HEAD <= end && n < WATCH_CANDIDATES {
        let len =
            u32::from_le_bytes(exec.resp[at + 1..at + 5].try_into().unwrap_or([0; 4])) as usize;
        if exec.resp[at] == RESP_OWNERS && n > 0 {
            // It travels with the event before it.
            idx[n - 1].2 += (FIELD_HEAD + len) as u32;
            at += FIELD_HEAD + len;
            continue;
        }
        let rev = u64::from_le_bytes(exec.resp[at + 5..at + 13].try_into().unwrap_or([0; 8]));
        idx[n] = (rev, (at - base) as u32, (FIELD_HEAD + len) as u32);
        n += 1;
        at += FIELD_HEAD + len;
    }
    for i in 1..n {
        let mut j = i;
        while j > 0 && idx[j - 1].0 > idx[j].0 {
            idx.swap(j - 1, j);
            j -= 1;
        }
    }
    exec.buf_a[..region].copy_from_slice(&exec.resp[base..end]);
    let mut w = base;
    for &(_, off, len) in &idx[..n] {
        let (off, len) = (off as usize, len as usize);
        exec.resp[w..w + len].copy_from_slice(&exec.buf_a[off..off + len]);
        w += len;
    }
}

// ── Scans: list, filtered update and delete ──

fn scan_page(exec: &mut ExecState) {
    if Descriptor::parse(&exec.desc[..exec.desc_len as usize]).is_err() {
        fail_plain(exec, OUT_CORRUPT);
        return;
    }
    // A continuation (first page only) names the revision and where to
    // resume.
    if exec.snap_rev == 0 && exec.after_len == 0 {
        let rq = req_view(exec);
        let n = rq.len();
        if let Some((rev, key)) = decode_req(&rq[..n])
            .and_then(|r| r.continuation())
            .and_then(decode_continuation)
        {
            if key.len() <= exec.after.len() {
                exec.snap_rev = rev;
                exec.after[..key.len()].copy_from_slice(key);
                exec.after_len = key.len() as u16;
            }
        } else if let Some(r) = decode_req(&rq[..n]).and_then(|r| r.at_revision()) {
            exec.snap_rev = r;
        }
    }
    if !exec.plan_decided {
        decide_plan(exec);
        if !plan_matches_resume(exec) {
            // A continuation from the other plan: an index came or went
            // between pages. The client lists again.
            fail(exec, OUT_CONFLICT, 0, &[], msg_revision());
            return;
        }
    }
    let mut start = [0u8; KEY_MAX + 301];
    let mut end = [0u8; KEY_MAX + 300];
    let span = if exec.plan_ix != 0 {
        let vl = exec.plan_vlen as usize;
        let mut vals = [0u8; doc_ops::MAX_INDEX_VALUES];
        vals[..vl].copy_from_slice(&exec.plan_vals[..vl]);
        index_entry_key(&mut start, exec.plan_ix, &vals[..vl], None)
            .and_then(|sn| Some((sn, successor(&start[..sn], &mut end)?)))
    } else {
        request_span(exec, &mut start, &mut end)
    };
    let Some((sn, en)) = span else {
        fail(exec, OUT_INVALID, 0, &[], msg_bad_prefix());
        return;
    };
    let sn = if exec.after_len > 0 {
        let a = exec.after_len as usize;
        start[..a].copy_from_slice(&exec.after[..a]);
        start[a] = 0;
        a + 1
    } else {
        sn
    };
    // Updates and deletes write at the current revision; lists read at
    // their snapshot.
    let at = if req_op(exec) == OP_LIST {
        exec.snap_rev
    } else {
        0
    };
    let Some(n) = stage_range_meta(exec, &start[..sn], &end[..en], at, SCAN_PAGE as u32) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, n, P_SCAN);
}

fn on_scan(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if Kvs::parse(body).is_none() || body.len() > PAGE_BUF {
        fail_plain(exec, OUT_CORRUPT);
        return;
    }
    if exec.snap_rev == 0 && req_op(exec) == OP_LIST {
        exec.snap_rev = revision;
    }
    exec.page[..body.len()].copy_from_slice(body);
    exec.page_len = body.len() as u32;
    exec.page_at = 0;
    exec.page_more = Kvs::parse(body).is_some_and(|k| k.more != 0);
    scan_continue(exec);
}

/// The held page's entry `i`, its document copied into `stored` and
/// its key into `key`: `Some(mod_rev)`, `None` past the page's end, and
/// `Err` for a record that does not decode.
fn take_entry(exec: &mut ExecState, i: usize) -> Result<Option<u64>, ()> {
    let ExecState {
        page,
        page_len,
        desc,
        desc_len,
        codec,
        stored,
        stored_len,
        owners,
        owners_len,
        key,
        key_len,
        ..
    } = exec;
    let Some(kvs) = Kvs::parse(&page[..*page_len as usize]) else {
        return Err(());
    };
    let Some((k, rec, mod_rev)) = kvs.entries().nth(i) else {
        return Ok(None);
    };
    if k.len() > key.len() {
        return Err(());
    }
    hold_record(
        &desc[..*desc_len as usize],
        codec,
        rec,
        stored,
        stored_len,
        owners,
        owners_len,
    )
    .ok_or(())?;
    key[..k.len()].copy_from_slice(k);
    *key_len = k.len() as u16;
    Ok(Some(mod_rev))
}

/// Does the document in `stored` match the request's filter?
fn stored_matches(exec: &ExecState) -> bool {
    let Some(req) = decode_req(&exec.req[..exec.req_len as usize]) else {
        return false;
    };
    let doc = Value::trusted(&exec.stored[..exec.stored_len as usize]);
    req.filter().is_none_or(|f| matches(f, doc))
}

/// Walk the held page from `page_at`: answer (lists) or write (updates,
/// deletes) each match, then the next page. `after` is the last key
/// passed, where the next page (or a continuation) resumes.
fn scan_continue(exec: &mut ExecState) {
    let (limit, multi, list) = {
        let Some(req) = decode_req(&exec.req[..exec.req_len as usize]) else {
            fail_plain(exec, OUT_MALFORMED);
            return;
        };
        (
            req.limit().filter(|l| *l > 0).unwrap_or(u32::MAX),
            req.flags & FLAG_MULTI != 0 || req.op == OP_LIST,
            req.op == OP_LIST,
        )
    };
    loop {
        let i = exec.page_at as usize;
        if exec.plan_ix != 0 {
            // An index entry: fetch its document; the scan continues
            // when it answers.
            match plan_take(exec, i) {
                Ok(true) => return plan_fetch(exec),
                Ok(false) => break,
                Err(()) => {
                    fail_plain(exec, OUT_CORRUPT);
                    return;
                }
            }
        }
        let mod_rev = match take_entry(exec, i) {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(()) => {
                fail_plain(exec, OUT_CORRUPT);
                return;
            }
        };
        if !scan_doc(exec, mod_rev, limit, list) {
            return;
        }
    }
    exec.page_at = 0;
    let done_one = !multi && exec.matched > 0;
    if exec.page_more && !done_one {
        scan_page(exec);
        return;
    }
    scan_done(exec);
}

/// One document of a scan (in `stored`, its key in `key`): answered when
/// a list's, written when an update's or delete's. `false` when the job
/// moved on (an answer sent, a write in flight).
fn scan_doc(exec: &mut ExecState, mod_rev: u64, limit: u32, list: bool) -> bool {
    let hit = stored_matches(exec);
    if hit && list {
        if exec.answered >= limit {
            // The limit is reached with documents left: resume here.
            list_more(exec);
            return false;
        }
        let mark = exec.resp_len;
        let over_budget = |e: &ExecState| e.resp_len - DOC_RESPONSE_HEAD > e.resp_budget;
        if exec.answered > 0 && mark - DOC_RESPONSE_HEAD >= exec.resp_budget {
            list_more(exec);
            return false;
        }
        if !push_doc(exec, mod_rev) {
            if exec.resp_overflow && exec.answered > 0 {
                exec.resp_len = mark;
                exec.resp_overflow = false;
                list_more(exec);
                return false;
            }
            fail_plain(exec, OUT_TOO_LARGE);
            return false;
        }
        if exec.answered > 0 && over_budget(exec) {
            // Past the dialect's budget: this document starts the next
            // page.
            exec.resp_len = mark;
            list_more(exec);
            return false;
        }
        exec.answered += 1;
        exec.m_docs_read = exec.m_docs_read.wrapping_add(1);
    }
    // This entry is passed: a resume starts after it (the index entry
    // when the scan reads an index).
    {
        let ExecState {
            after,
            after_len,
            key,
            key_len,
            plan_ix,
            plan_key,
            plan_key_len,
            ..
        } = &mut *exec;
        let (src, l) = if *plan_ix != 0 {
            (&plan_key[..], *plan_key_len as usize)
        } else {
            (&key[..], *key_len as usize)
        };
        after[..l].copy_from_slice(&src[..l]);
        *after_len = l as u16;
    }
    exec.page_at += 1;
    if hit && !list {
        // An update or delete of this document; the page resumes after
        // it once the write answers. (A replace counts its one match when
        // it writes, as a replace by id does.)
        exec.read_rev = mod_rev;
        if req_op(exec) != OP_REPLACE {
            exec.matched += 1;
        }
        scan_write(exec);
        return false;
    }
    true
}

// ── Planned scans ─────────────────────────────────────────────────────

/// Choose how the scan reads: an index that answers the filter's
/// equality, or the collection.
fn decide_plan(exec: &mut ExecState) {
    exec.plan_decided = true;
    let ExecState {
        req,
        req_len,
        desc,
        desc_len,
        plan_ix,
        plan_unique,
        plan_vlen,
        plan_vals,
        ..
    } = &mut *exec;
    let plan = (|| {
        let req = decode_req(&req[..*req_len as usize])?;
        if req.id_prefix().is_some() {
            // An id prefix narrows the collection's own span; an index's
            // entries would not.
            return None;
        }
        let d = Descriptor::parse(&desc[..*desc_len as usize]).ok()?;
        doc_ops::index_plan(&d, req.filter()?, &mut plan_vals[..])
    })();
    if let Some((id, unique, n)) = plan {
        *plan_ix = id;
        *plan_unique = unique;
        *plan_vlen = n as u16;
    }
}

/// A resume point (a continuation's key) belongs to the chosen plan: an
/// index's entries or the collection's documents.
fn plan_matches_resume(exec: &ExecState) -> bool {
    let al = exec.after_len as usize;
    if al == 0 {
        return true;
    }
    let ks = u32::from_be_bytes(exec.after[..4].try_into().unwrap_or([0; 4]));
    if exec.plan_ix == 0 {
        let Ok(d) = Descriptor::parse(&exec.desc[..exec.desc_len as usize]) else {
            return false;
        };
        if doc_codec::is_json(&d) {
            let mut p = [0u8; KEY_MAX];
            return doc_ops::path_prefix(d.name(), None, &mut p)
                .is_some_and(|n| exec.after[..al].starts_with(&p[..n]));
        }
        return ks == models::KS_DOCUMENT_DATA;
    }
    ks == models::KS_DOCUMENT_INDEX && exec.after.get(4..8) == Some(&exec.plan_ix.to_be_bytes()[..])
}

/// The held index page's entry `i`: its key (the resume point) kept, the
/// document's data key into `key`. `Ok(false)` past the page's end.
fn plan_take(exec: &mut ExecState, i: usize) -> Result<bool, ()> {
    let ExecState {
        desc,
        desc_len,
        page,
        page_len,
        plan_unique,
        plan_key,
        plan_key_len,
        key,
        key_len,
        ..
    } = &mut *exec;
    let kvs = Kvs::parse(&page[..*page_len as usize]).ok_or(())?;
    let Some((ek, ev, _)) = kvs.entries().nth(i) else {
        return Ok(false);
    };
    if ek.len() > plan_key.len() {
        return Err(());
    }
    plan_key[..ek.len()].copy_from_slice(ek);
    *plan_key_len = ek.len() as u16;
    // The document's id key: a unique entry holds it, a plain entry's key
    // ends with it.
    let mut idk = [0u8; models::MAX_DOCUMENT_ID_KEY];
    let il = if *plan_unique {
        let n = ev.len();
        idk.get_mut(..n).ok_or(())?.copy_from_slice(ev);
        n
    } else {
        let mut vals = [0u8; doc_ops::MAX_INDEX_VALUES];
        let (_, _, dl) =
            models::decode_document_index_key(ek.get(4..).ok_or(())?, &mut vals, &mut idk)
                .ok_or(())?;
        dl
    };
    let coll = Descriptor::parse(&desc[..*desc_len as usize]).map_err(|_| ())?;
    let n = doc_key(&coll, &[], &idk[..il], &mut key[..]).ok_or(())?;
    *key_len = n as u16;
    Ok(true)
}

/// Read the candidate document (at a list's snapshot).
fn plan_fetch(exec: &mut ExecState) {
    let mut k = [0u8; KEY_MAX];
    let kn = exec.key_len as usize;
    k[..kn].copy_from_slice(&exec.key[..kn]);
    let at = if req_op(exec) == OP_LIST {
        exec.snap_rev
    } else {
        0
    };
    let Some(bn) = stage_range_meta(exec, &k[..kn], &[], at, 1) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, bn, P_PLAN_FETCH);
}

fn on_plan_fetch(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let got = {
        let ExecState {
            desc,
            desc_len,
            codec,
            stored,
            stored_len,
            owners,
            owners_len,
            ..
        } = &mut *exec;
        (|| {
            let kvs = Kvs::parse(body)?;
            let (_, rec, mod_rev) = kvs.entries().next()?;
            hold_record(
                &desc[..*desc_len as usize],
                codec,
                rec,
                stored,
                stored_len,
                owners,
                owners_len,
            )?;
            Some(mod_rev)
        })()
    };
    let (limit, list) = match decode_req(&exec.req[..exec.req_len as usize]) {
        Some(r) => (
            r.limit().filter(|l| *l > 0).unwrap_or(u32::MAX),
            r.op == OP_LIST,
        ),
        None => {
            fail_plain(exec, OUT_MALFORMED);
            return;
        }
    };
    match got {
        Some(mod_rev) => {
            if !scan_doc(exec, mod_rev, limit, list) {
                return;
            }
        }
        None => {
            // An entry whose document is gone at this revision cannot be:
            // entries move with their documents. Pass it.
            let pl = exec.plan_key_len as usize;
            let (after, pk) = (&mut exec.after, &exec.plan_key);
            after[..pl].copy_from_slice(&pk[..pl]);
            exec.after_len = pl as u16;
            exec.page_at += 1;
        }
    }
    scan_continue(exec);
}

fn list_more(exec: &mut ExecState) {
    let mut c = [0u8; 16 + MAX_CONTINUE_KEY];
    let snap = exec.snap_rev;
    let al = exec.after_len as usize;
    let Some(cn) = encode_continuation(&mut c, snap, &exec.after[..al]) else {
        // A resume point past what a continuation carries: the answer
        // cannot say where the list resumes, so it is refused, never
        // presented as complete.
        fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large());
        return;
    };
    resp_field(exec, RESP_CONTINUE, &[&c[..cn]]);
    resp_send(exec, OUT_OK, RFLAG_MORE, snap);
}

fn scan_done(exec: &mut ExecState) {
    let (m, md) = (exec.matched, exec.modified);
    match req_op(exec) {
        OP_LIST => {
            let snap = exec.snap_rev;
            resp_send(exec, OUT_OK, 0, snap);
        }
        _ => {
            if m == 0
                && req_flags(exec) & FLAG_UPSERT != 0
                && matches!(req_op(exec), OP_UPDATE | OP_REPLACE)
            {
                // No match: the seed (an update's) or the replacement
                // becomes a document, keyed by its own id.
                upsert_by_filter(exec);
                return;
            }
            if m == 0 && req_op(exec) == OP_REPLACE {
                resp_field(
                    exec,
                    RESP_COUNT,
                    &[&0u64.to_le_bytes(), &0u64.to_le_bytes()],
                );
                resp_send(exec, OUT_OK, 0, 0);
                return;
            }
            resp_field(exec, RESP_COUNT, &[&m.to_le_bytes(), &md.to_le_bytes()]);
            resp_send(exec, OUT_OK, 0, 0);
        }
    }
}

fn upsert_by_filter(exec: &mut ExecState) {
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let Some(seed) = req.doc() else {
        fail(exec, OUT_INVALID, 0, desc.id_path().bytes(), msg_no_id());
        return;
    };
    let mut k = [0u8; KEY_MAX];
    let Some(kn) = key_for(&desc, seed, &mut k) else {
        fail(exec, OUT_INVALID, 0, desc.id_path().bytes(), msg_no_id());
        return;
    };
    exec.key[..kn].copy_from_slice(&k[..kn]);
    exec.key_len = kn as u16;
    exec.read_rev = 0;
    if req.op == OP_REPLACE {
        let sb = seed.bytes();
        let doc = hold(&mut exec.hold_doc, sb);
        let dl = doc.len();
        create_at_key(exec, Value::trusted(&doc[..dl]));
        return;
    }
    upsert_from_seed(exec);
}

/// Update, replace or delete the matched document at `exec.key`.
fn scan_write(exec: &mut ExecState) {
    if req_op(exec) == OP_REPLACE {
        // One document, as a replace by id; its answer ends the job.
        write_replace(exec);
        return;
    }
    let rq = req_view(exec);
    let n = rq.len();
    let Some(req) = decode_req(&rq[..n]) else {
        fail_plain(exec, OUT_MALFORMED);
        return;
    };
    let mut k = [0u8; KEY_MAX];
    let kn = exec.key_len as usize;
    k[..kn].copy_from_slice(&exec.key[..kn]);
    let rev = exec.read_rev;
    if req.op == OP_DELETE {
        if req.flags & FLAG_DRY_RUN != 0 {
            exec.modified += 1;
            scan_after_write(exec);
            return;
        }
        let bn = match stage_doc_write(exec, &k[..kn], rev, None) {
            Ok(n) => n,
            Err(e) => {
                fail_op(exec, 0, &[], e);
                return;
            }
        };
        kv_send(exec, KV_OP_TXN_STAGED, bn, P_SCAN_WRITE);
        return;
    }
    let Some(muts) = req.mutations() else {
        fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op());
        return;
    };
    let dn = exec.desc_len as usize;
    let mut dbuf = [0u8; MAX_DESCRIPTOR_LEN];
    dbuf[..dn].copy_from_slice(&exec.desc[..dn]);
    let Ok(desc) = Descriptor::parse(&dbuf[..dn]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let sl = exec.stored_len as usize;
    let sb = hold(&mut exec.hold_stored, &exec.stored[..sl]);
    let stored = Value::trusted(&sb[..sl]);
    let (a, b) = (&mut exec.buf_a, &mut exec.buf_b);
    let mut bufs = Buffers::new(a, b);
    if let Err(e) = bufs.load(stored) {
        fail_op(exec, 0, &[], e);
        return;
    }
    if let Err((i, e)) = apply_mutations(&mut bufs, muts) {
        let p = muts.iter().nth(i).map(|m| m.path.bytes()).unwrap_or(&[]);
        let mut pb = [0u8; 1024];
        let pl = p.len().min(pb.len());
        pb[..pl].copy_from_slice(&p[..pl]);
        fail_op(exec, i as u32, &pb[..pl], e);
        return;
    }
    let cur = bufs.current();
    if let Some(p) = changed_path(stored, cur, desc.id_paths().chain(desc.identity_paths())) {
        // A declared path: it fits the descriptor it came from.
        let mut pb = [0u8; MAX_DESCRIPTOR_LEN];
        let pl = p.bytes().len();
        pb[..pl].copy_from_slice(p.bytes());
        fail(exec, OUT_INVALID, 0, &pb[..pl], msg_identity());
        return;
    }
    if !fits(&desc, cur) {
        fail(exec, OUT_TOO_LARGE, 0, &[], msg_too_large());
        return;
    }
    if cur.bytes() == stored.bytes() {
        scan_after_write(exec);
        return;
    }
    if req.flags & FLAG_DRY_RUN != 0 {
        exec.modified += 1;
        scan_after_write(exec);
        return;
    }
    let cb = cur.bytes();
    let doc = hold(&mut exec.hold_doc, cb);
    let dl = doc.len();
    let bn = match stage_doc_write(exec, &k[..kn], rev, Some(&doc[..dl])) {
        Ok(n) => n,
        Err(e) => {
            fail_op(exec, 0, &[], e);
            return;
        }
    };
    kv_send(exec, KV_OP_TXN_STAGED, bn, P_SCAN_WRITE);
}

fn on_scan_write(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_TXN {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if body.first() == Some(&1) {
        exec.modified += 1;
        exec.m_docs_written = exec.m_docs_written.wrapping_add(1);
    } else {
        why_refused(exec, true, false);
        return;
    }
    scan_after_write(exec);
}

fn scan_after_write(exec: &mut ExecState) {
    let multi = req_flags(exec) & FLAG_MULTI != 0;
    if !multi {
        scan_done(exec);
        return;
    }
    scan_continue(exec);
}

/// A write's transaction refused. Why: re-read the document (unless an
/// insert: its keys were probed already), then the descriptor.
fn why_refused(exec: &mut ExecState, scan: bool, insert: bool) {
    exec.why_scan = scan;
    exec.why_insert = insert;
    exec.why_doc_changed = false;
    if insert {
        why_descriptor(exec);
        return;
    }
    let mut k = [0u8; KEY_MAX];
    let kn = exec.key_len as usize;
    k[..kn].copy_from_slice(&exec.key[..kn]);
    let Some(bn) = stage_range_meta(exec, &k[..kn], &[], 0, 1) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, bn, P_WHY_DOC);
}

fn why_descriptor(exec: &mut ExecState) {
    let mut k = [0u8; KEY_MAX];
    let kn = match Descriptor::parse(&exec.desc[..exec.desc_len as usize])
        .ok()
        .and_then(|d| catalog_key(&mut k, d.name()))
    {
        Some(n) => n,
        None => {
            fail_plain(exec, OUT_CORRUPT);
            return;
        }
    };
    let Some(bn) = stage_range_meta(exec, &k[..kn], &[], 0, 1) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, bn, P_WHY_DESC);
}

/// A key's mod revision in a `RANGE_META` answer (0: absent).
fn answered_rev(body: &[u8]) -> u64 {
    Kvs::parse(body)
        .and_then(|k| k.entries().next().map(|(_, _, r)| r))
        .unwrap_or(0)
}

fn on_why_doc(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    exec.why_doc_changed = answered_rev(body) != exec.read_rev;
    why_descriptor(exec);
}

/// The verdict: the schema moved (re-read it and re-apply), the document
/// moved (a lost race), or neither — a unique index refused a value.
fn on_why_desc(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    let schema_moved = answered_rev(body) != exec.desc_rev;
    match (exec.why_scan, schema_moved, exec.why_doc_changed) {
        // A filtered write cannot re-apply one document mid-scan; the
        // client retries the whole write.
        (true, true, _) => fail(exec, OUT_CONFLICT, 0, &[], msg_revision()),
        (true, false, true) => {
            // The document changed between the scan and the write: it no
            // longer is the document that matched. Not counted; the scan
            // moves on (an update-many is not atomic across documents).
            exec.matched -= 1;
            scan_after_write(exec);
        }
        (false, true, _) | (false, false, true) => cas_lost(exec),
        (_, false, false) => {
            if has_unique(exec) {
                fail(exec, OUT_EXISTS, 0, &[], msg_unique());
            } else if exec.why_insert {
                // A concurrent insert took a key and has since gone.
                fail(exec, OUT_CONFLICT, 0, &[], msg_exists());
            } else {
                fail(exec, OUT_CONFLICT, 0, &[], msg_revision());
            }
        }
    }
}

// ── Indexes: create (with a backfill) and drop ────────────────────────
//
// A new index enters the descriptor BUILDING: from that write on, every
// document write (fenced on the descriptor's revision) keeps its entries.
// The backfill then gives every stored document its entries, each in a
// transaction compared on the document's revision — a document that
// changed meanwhile was kept by its writer. The index turns READY when
// the backfill is done; one the documents violate (a unique value held
// twice, a value that cannot be indexed) is withdrawn and refused.

/// Write a proposed descriptor (`exec.pend`), compared on the one read.
fn propose_descriptor(exec: &mut ExecState, phase: u8) {
    let r = {
        let ExecState {
            desc,
            desc_len,
            desc_rev,
            pend,
            pend_len,
            tx_cmp,
            tx_ops,
            env,
            ..
        } = &mut *exec;
        (|| {
            let d = Descriptor::parse(&desc[..*desc_len as usize]).ok()?;
            let mut ck = [0u8; KEY_MAX];
            let cn = catalog_key(&mut ck, d.name())?;
            let mut t = Txn::new(tx_cmp, tx_ops);
            t.cmp(&ck[..cn], *desc_rev)?;
            t.put(&ck[..cn], &[&pend[..*pend_len as usize]])?;
            t.assemble(env)
        })()
    };
    match r {
        Some(bn) => kv_send(exec, KV_OP_TXN_STAGED, bn, phase),
        None => fail_plain(exec, OUT_MALFORMED),
    }
}

/// A proposed descriptor was written: it is the descriptor now.
fn adopt_descriptor(exec: &mut ExecState, revision: u64) {
    let n = exec.pend_len as usize;
    let (desc, pend) = (&mut exec.desc, &exec.pend);
    desc[..n].copy_from_slice(&pend[..n]);
    exec.desc_len = n as u16;
    exec.desc_rev = revision;
}

fn ix_create(exec: &mut ExecState) {
    // The same name: the same definition is already there (nothing to do),
    // another definition is a conflict.
    let existing = {
        let req = decode_req(&exec.req[..exec.req_len as usize]);
        let d = Descriptor::parse(&exec.desc[..exec.desc_len as usize]).ok();
        match (req.and_then(|r| r.index()), d) {
            (Some(ix), Some(d)) => Some(d.indexes().find(|i| i.name == ix.name).map(|i| {
                i.unique() == ix.unique()
                    && i.paths()
                        .map(|p| p.bytes())
                        .eq(ix.paths().map(|p| p.bytes()))
            })),
            _ => None,
        }
    };
    // A JSON collection's documents are written by more than the engine,
    // so an index of them could not be kept.
    let json = Descriptor::parse(&exec.desc[..exec.desc_len as usize])
        .is_ok_and(|d| doc_codec::is_json(&d));
    match existing {
        None => fail(exec, OUT_MALFORMED, 0, &[], msg_bad_op()),
        Some(None) if json => fail(exec, OUT_INVALID, 0, &[], msg_json_index()),
        Some(Some(true)) => {
            let n = exec.desc_len as usize;
            let mut d = [0u8; MAX_DESCRIPTOR_LEN];
            d[..n].copy_from_slice(&exec.desc[..n]);
            resp_field(exec, RESP_DESCRIPTOR, &[&d[..n]]);
            resp_send(exec, OUT_OK, 0, 0);
        }
        Some(Some(false)) => fail(exec, OUT_CONFLICT, 0, &[], msg_index_exists()),
        Some(None) => allocate_id(exec, P_IX_ALLOC),
    }
}

/// One id from the collection allocator.
fn allocate_id(exec: &mut ExecState, phase: u8) {
    let mut k = [0u8; KEY_MAX];
    let mut body = [0u8; 8];
    let kn = catalog_seq_key(&mut body, DATABASE_ID)
        .and_then(|bn| kv_key(&mut k, models::KS_DOCUMENT_CATALOG, &body[..bn]));
    let Some(kn) = kn else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    let mut p = BODY_AT;
    let ok = put(&mut exec.env, &mut p, &(kn as u16).to_le_bytes()).is_some()
        && put(&mut exec.env, &mut p, &k[..kn]).is_some()
        && put(&mut exec.env, &mut p, &1i64.to_le_bytes()).is_some();
    if !ok {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    }
    kv_send(exec, KV_OP_INCR, p - BODY_AT, phase);
}

fn on_ix_alloc(exec: &mut ExecState, result: u8, body: &[u8]) {
    let id = match (result, body.get(..8)) {
        (KV_RESULT_INTEGER | KV_RESULT_OK, Some(b)) => {
            i64::from_le_bytes(b.try_into().unwrap_or([0; 8]))
        }
        _ => {
            fail_plain(exec, store_outcome(result));
            return;
        }
    };
    if id <= 0 || id > u32::MAX as i64 {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    }
    let built = {
        let ExecState {
            req,
            req_len,
            desc,
            desc_len,
            pend,
            pend_len,
            ..
        } = &mut *exec;
        (|| {
            let spec = decode_req(&req[..*req_len as usize])?.index_bytes()?;
            let mut body = [0u8; MAX_DESCRIPTOR_LEN];
            let b = body.get_mut(..spec.len())?;
            b.copy_from_slice(spec);
            b.get_mut(..4)?.copy_from_slice(&(id as u32).to_le_bytes());
            *b.get_mut(5)? = doc_catalog::INDEX_BUILDING;
            let n = doc_catalog::with_entry(
                &desc[..*desc_len as usize],
                doc_catalog::TAG_INDEX,
                &body[..spec.len()],
                &mut pend[..],
            )
            .ok()?;
            *pend_len = n as u16;
            Some(())
        })()
    };
    if built.is_none() {
        fail_plain(exec, OUT_MALFORMED);
        return;
    }
    exec.ix_id = id as u32;
    propose_descriptor(exec, P_IX_DESC);
}

/// A descriptor write answered: `true` when it took.
fn descriptor_took(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) -> bool {
    if result != KV_RESULT_TXN {
        fail_plain(exec, store_outcome(result));
        return false;
    }
    if body.first() != Some(&1) {
        // Another schema change came first.
        fail(exec, OUT_CONFLICT, 0, &[], msg_revision());
        return false;
    }
    adopt_descriptor(exec, revision);
    true
}

fn on_ix_desc(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if descriptor_took(exec, result, revision, body) {
        exec.after_len = 0;
        ix_scan_page(exec);
    }
}

fn ix_scan_page(exec: &mut ExecState) {
    let Ok(coll) = Descriptor::parse(&exec.desc[..exec.desc_len as usize]) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    let mut start = [0u8; KEY_MAX + 1];
    let mut end = [0u8; KEY_MAX];
    let Some((sn, en)) = collection_span(&coll, &mut start, &mut end) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    let sn = if exec.after_len > 0 {
        let a = exec.after_len as usize;
        start[..a].copy_from_slice(&exec.after[..a]);
        start[a] = 0;
        a + 1
    } else {
        sn
    };
    let Some(n) = stage_range_meta(exec, &start[..sn], &end[..en], 0, SCAN_PAGE as u32) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, n, P_IX_SCAN);
}

fn on_ix_scan(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if Kvs::parse(body).is_none() || body.len() > PAGE_BUF {
        fail_plain(exec, OUT_CORRUPT);
        return;
    }
    exec.page[..body.len()].copy_from_slice(body);
    exec.page_len = body.len() as u32;
    exec.page_at = 0;
    exec.page_more = Kvs::parse(body).is_some_and(|k| k.more != 0);
    ix_next(exec);
}

/// The next document of the page: its entries for the index being built.
fn ix_next(exec: &mut ExecState) {
    let i = exec.page_at as usize;
    let mod_rev = match take_entry(exec, i) {
        Ok(Some(r)) => r,
        Ok(None) => {
            if exec.page_more {
                // Resume after the page's last key.
                let kl = exec.key_len as usize;
                let (after, key) = (&mut exec.after, &exec.key);
                after[..kl].copy_from_slice(&key[..kl]);
                exec.after_len = kl as u16;
                ix_scan_page(exec);
            } else {
                ix_ready(exec);
            }
            return;
        }
        Err(()) => {
            fail_plain(exec, OUT_CORRUPT);
            return;
        }
    };
    exec.read_rev = mod_rev;
    let r = {
        let ExecState {
            desc,
            desc_len,
            desc_rev,
            stored,
            stored_len,
            key,
            key_len,
            ix_id,
            tx_cmp,
            tx_ops,
            env,
            ..
        } = &mut *exec;
        (|| -> Result<usize, OpError> {
            let full = OpError::from(doc_tree::TreeError::Overflow);
            let d = Descriptor::parse(&desc[..*desc_len as usize]).map_err(|_| full)?;
            let ix = d.indexes().find(|x| x.id == *ix_id).ok_or(full)?;
            let doc = Value::trusted(&stored[..*stored_len as usize]);
            let mut idk = [0u8; models::MAX_DOCUMENT_ID_KEY];
            let il = doc_id_key(&d, doc, &mut idk).ok_or(full)?;
            let mut ck = [0u8; KEY_MAX];
            let cn = catalog_key(&mut ck, d.name()).ok_or(full)?;
            let mut t = Txn::new(tx_cmp, tx_ops);
            t.cmp(&ck[..cn], *desc_rev).ok_or(full)?;
            t.cmp(&key[..*key_len as usize], mod_rev).ok_or(full)?;
            let unique = ix.unique();
            let mut ok = true;
            index_values(&ix, doc, &mut |vals| {
                let mut k = [0u8; KEY_MAX + 300];
                ok &= match index_entry_key(&mut k, ix.id, vals, (!unique).then_some(&idk[..il])) {
                    Some(n) if unique => t
                        .cmp(&k[..n], 0)
                        .and_then(|_| t.put(&k[..n], &[&idk[..il]])),
                    Some(n) => t.put(&k[..n], &[]),
                    None => None,
                }
                .is_some();
                ok
            })?;
            if !ok {
                return Err(full);
            }
            t.assemble(env).ok_or(full)
        })()
    };
    match r {
        Ok(bn) => kv_send(exec, KV_OP_TXN_STAGED, bn, P_IX_WRITE),
        Err(e) if e.reason == Reason::NotIndexable => ix_abort(exec, OUT_INVALID),
        Err(e) => fail_op(exec, 0, &[], e),
    }
}

fn on_ix_write(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_TXN {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if body.first() == Some(&1) {
        exec.page_at += 1;
        ix_next(exec);
        return;
    }
    // Refused: the document changed (its writer kept the index), the
    // schema changed, or a unique value is held twice.
    let mut k = [0u8; KEY_MAX];
    let kn = exec.key_len as usize;
    k[..kn].copy_from_slice(&exec.key[..kn]);
    let Some(bn) = stage_range_meta(exec, &k[..kn], &[], 0, 1) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, bn, P_IX_WHY_DOC);
}

fn on_ix_why_doc(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if answered_rev(body) != exec.read_rev {
        exec.page_at += 1;
        ix_next(exec);
        return;
    }
    let mut k = [0u8; KEY_MAX];
    let kn = match Descriptor::parse(&exec.desc[..exec.desc_len as usize])
        .ok()
        .and_then(|d| catalog_key(&mut k, d.name()))
    {
        Some(n) => n,
        None => {
            fail_plain(exec, OUT_CORRUPT);
            return;
        }
    };
    let Some(bn) = stage_range_meta(exec, &k[..kn], &[], 0, 1) else {
        fail_plain(exec, OUT_UNAVAILABLE);
        return;
    };
    kv_send(exec, KV_OP_RANGE_META, bn, P_IX_WHY_DESC);
}

fn on_ix_why_desc(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_KVS {
        fail_plain(exec, store_outcome(result));
        return;
    }
    if answered_rev(body) != exec.desc_rev {
        // Another schema change: this build cannot vouch for its entries.
        fail(exec, OUT_CONFLICT, 0, &[], msg_revision());
        return;
    }
    ix_abort(exec, OUT_EXISTS);
}

/// Withdraw the index being built, then its entries; answer `outcome`.
fn ix_abort(exec: &mut ExecState, outcome: u8) {
    let id = exec.ix_id;
    let built = {
        let ExecState {
            desc,
            desc_len,
            pend,
            pend_len,
            ..
        } = &mut *exec;
        doc_catalog::without_index(&desc[..*desc_len as usize], id, &mut pend[..])
            .map(|n| *pend_len = n as u16)
    };
    if built.is_err() {
        fail_plain(exec, OUT_CORRUPT);
        return;
    }
    exec.drop_outcome = outcome;
    propose_descriptor(exec, P_IX_ABORT);
}

fn on_ix_abort(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if descriptor_took(exec, result, revision, body) {
        exec.drop_only = exec.ix_id;
        exec.drop_stage = 0;
        coll_drop_page(exec);
    }
}

fn ix_ready(exec: &mut ExecState) {
    let n = exec.desc_len as usize;
    let (desc, pend) = (&exec.desc, &mut exec.pend);
    pend[..n].copy_from_slice(&desc[..n]);
    exec.pend_len = n as u16;
    let id = exec.ix_id;
    if doc_catalog::set_index_state(&mut exec.pend[..n], id, doc_catalog::INDEX_READY).is_err() {
        fail_plain(exec, OUT_CORRUPT);
        return;
    }
    propose_descriptor(exec, P_IX_READY);
}

fn on_ix_ready(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if descriptor_took(exec, result, revision, body) {
        let n = exec.desc_len as usize;
        let mut d = [0u8; MAX_DESCRIPTOR_LEN];
        d[..n].copy_from_slice(&exec.desc[..n]);
        resp_field(exec, RESP_DESCRIPTOR, &[&d[..n]]);
        resp_send(exec, OUT_OK, 0, revision);
    }
}

fn ix_drop(exec: &mut ExecState) {
    let found = {
        let ExecState {
            req,
            req_len,
            desc,
            desc_len,
            pend,
            pend_len,
            ..
        } = &mut *exec;
        (|| {
            let name = decode_req(&req[..*req_len as usize])?.index_name()?;
            let d = Descriptor::parse(&desc[..*desc_len as usize]).ok()?;
            let id = d.indexes().find(|i| i.name == name)?.id;
            let n =
                doc_catalog::without_index(&desc[..*desc_len as usize], id, &mut pend[..]).ok()?;
            *pend_len = n as u16;
            Some(id)
        })()
    };
    let Some(id) = found else {
        fail_plain(exec, OUT_NOT_FOUND);
        return;
    };
    exec.ix_id = id;
    propose_descriptor(exec, P_IX_DROP_DESC);
}

fn on_ix_drop_desc(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    if descriptor_took(exec, result, revision, body) {
        exec.drop_only = exec.ix_id;
        exec.drop_stage = 0;
        coll_drop_page(exec);
    }
}

// ── KV replies ────────────────────────────────────────────────────────

fn on_kv_response(exec: &mut ExecState, payload: &[u8]) {
    // [corr:u64][conn:u8][result:u8][revision:u64][body_len:u16][body][tail]
    if payload.len() < 20 {
        return;
    }
    let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
    if corr != exec.kv_corr || !exec.busy() {
        // A reply to a job the watchdog already answered.
        return;
    }
    let result = payload[9];
    let revision = u64::from_le_bytes(payload[10..18].try_into().unwrap_or([0; 8]));
    let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
    let Some(body) = payload.get(20..20 + blen) else {
        fail_plain(exec, OUT_CORRUPT);
        return;
    };
    // The large-value layer's own exchanges.
    match exec.phase {
        P_LG_UP => {
            if result == types::KV_RESULT_OK {
                lg_upload(exec);
            } else {
                fail_plain(exec, store_outcome(result));
            }
            return;
        }
        P_LG_FETCH => {
            // A chunk the manifest names and the store no longer holds is
            // a record that does not read back.
            let took = result == types::KV_RESULT_OK && {
                let ExecState { lg_ex, lg_out, .. } = &mut *exec;
                lg_ex.take(body, lg_out).is_ok()
            };
            if took {
                lg_expand(exec);
            } else {
                fail_plain(exec, OUT_CORRUPT);
            }
            return;
        }
        _ => {}
    }
    // A read's reply that holds manifests: expanded first.
    if exec.lg_read && kv_large::Expander::wants(exec.lg_op, result) {
        exec.lg_rev = revision;
        exec.lg_next = exec.phase;
        let started = {
            let ExecState {
                lg_ex,
                lg_req,
                lg_req_len,
                lg_op,
                lg_out,
                ..
            } = &mut *exec;
            lg_ex.start(*lg_op, &lg_req[..*lg_req_len], result, body, lg_out)
        };
        if started.is_err() {
            fail_plain(exec, OUT_CORRUPT);
            return;
        }
        lg_expand(exec);
        return;
    }
    dispatch_reply(exec, result, revision, body);
}

/// A reply, as the request the phase sent it for would have it.
fn dispatch_reply(exec: &mut ExecState, result: u8, revision: u64, body: &[u8]) {
    match exec.phase {
        P_DESC => on_descriptor(exec, result, revision, body),
        P_CC_ALLOC => on_coll_alloc(exec, result, body),
        P_CC_PUT => on_coll_put(exec, result, revision, body),
        P_CD_DEL => {
            if result == KV_RESULT_TXN {
                coll_drop_page(exec);
            } else {
                fail_plain(exec, store_outcome(result));
            }
        }
        P_CD_SCAN => on_coll_drop_scan(exec, result, body),
        P_CD_PAGE_DEL => {
            if result == KV_RESULT_TXN {
                coll_drop_page(exec);
            } else {
                fail_plain(exec, store_outcome(result));
            }
        }
        P_CL_SCAN => on_coll_list(exec, result, revision, body),
        P_INS_TXN => on_insert(exec, result, revision, body),
        P_INS_PROBE => on_insert_probe(exec, result, body),
        P_READ => on_read(exec, result, revision, body),
        P_WRITE => on_write(exec, result, revision, body),
        P_SCAN => on_scan(exec, result, revision, body),
        P_SCAN_WRITE => on_scan_write(exec, result, body),
        P_WATCH_SIZE => on_watch_size(exec, result, revision, body),
        P_WATCH_EMIT => on_watch_emit(exec, result, body),
        P_WHY_DOC => on_why_doc(exec, result, body),
        P_WHY_DESC => on_why_desc(exec, result, body),
        P_IX_ALLOC => on_ix_alloc(exec, result, body),
        P_IX_DESC => on_ix_desc(exec, result, revision, body),
        P_IX_SCAN => on_ix_scan(exec, result, body),
        P_IX_WRITE => on_ix_write(exec, result, body),
        P_IX_WHY_DOC => on_ix_why_doc(exec, result, body),
        P_IX_WHY_DESC => on_ix_why_desc(exec, result, body),
        P_IX_READY => on_ix_ready(exec, result, revision, body),
        P_IX_ABORT => on_ix_abort(exec, result, revision, body),
        P_IX_DROP_DESC => on_ix_drop_desc(exec, result, revision, body),
        P_PLAN_FETCH => on_plan_fetch(exec, result, body),
        _ => {}
    }
}

// ── Module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ExecState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if state.is_null() || syscalls.is_null() || state_size < core::mem::size_of::<ExecState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let exec = unsafe { &mut *state.cast::<ExecState>() };
    exec.syscalls = sys_ptr;
    exec.phase = P_IDLE;
    exec.lg_up = kv_large::Uploader::new();
    exec.lg_ex = kv_large::Expander::new();
    exec.lg_read = false;
    exec.queue_len = 0;
    exec.kv_corr = 0;
    // inputs:  dialect_in[0], dialect2_in[1], kv_in[2]
    // outputs: dialect_out[0], dialect2_out[1], kv_out[2], metrics[3]
    exec.d_in[0] = in_chan;
    exec.d_out[0] = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        exec.d_in[1] = dev_channel_port(sys, 0, 1);
        exec.kv_in = dev_channel_port(sys, 0, 2);
        exec.d_out[1] = dev_channel_port(sys, 1, 1);
        exec.kv_out = dev_channel_port(sys, 1, 2);
        exec.metrics_out = dev_channel_port(sys, 1, 3);
        exec.changes_in = dev_channel_port(sys, 0, 3);
        exec.timer_fd = (sys.provider_open)(TIMER_CONTRACT, TIMER_CREATE, core::ptr::null_mut(), 0);
    }
    // Without a KV path nothing can execute; refuse to start rather than
    // accept requests that would hang.
    if exec.kv_out < 0 || exec.kv_in < 0 {
        return -1;
    }
    // The big build's buffers, all at once: a module that cannot hold a
    // document of its policy does not start.
    #[cfg(feature = "big")]
    unsafe {
        let sys = &*sys_ptr;
        let e = &mut *exec;
        let all = [
            e.req.alloc(sys),
            e.stored.alloc(sys),
            e.buf_a.alloc(sys),
            e.buf_b.alloc(sys),
            e.owners.alloc(sys),
            e.next_owners.alloc(sys),
            e.own_a.alloc(sys),
            e.own_b.alloc(sys),
            e.own_c.alloc(sys),
            e.codec.tree.alloc(sys),
            e.codec.text.alloc(sys),
            e.codec.own_text.alloc(sys),
            e.codec.own_tree.alloc(sys),
            e.codec.scratch.alloc(sys),
            e.tx_ops.alloc(sys),
            e.resp.alloc(sys),
            e.env.alloc(sys),
            e.lg_req.alloc(sys),
            e.lg_out.alloc(sys),
            e.hold_stored.alloc(sys),
            e.hold_src.alloc(sys),
            e.hold_doc.alloc(sys),
            e.hold_commit.alloc(sys),
            e.hold_patch.alloc(sys),
            e.inbox.alloc(sys),
            e.page.alloc(sys),
        ];
        if all.iter().any(|ok| !ok) {
            return -1;
        }
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let exec = unsafe { &mut *state.cast::<ExecState>() };
    unsafe {
        let sys_ptr = exec.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;
        // An answer still leaving goes first; then KV replies, which
        // advance the job in flight.
        flush_answer(exec);
        if let Some((mt, len)) = read_envelope(sys, exec.kv_in, &mut exec.scratch) {
            if mt == MSG_KV_RESPONSE {
                let mut tmp = [0u8; SCRATCH_BUF];
                tmp[..len].copy_from_slice(&exec.scratch[..len]);
                on_kv_response(exec, &tmp[..len]);
            }
        }
        start_queued(exec);
        for src in 0..2u8 {
            let chan = exec.d_in[src as usize];
            if let Some((mt, len)) = read_envelope(sys, chan, &mut exec.scratch) {
                if mt == MSG_DOC_REQUEST {
                    let mut tmp = [0u8; DOC_REQUEST_MAX];
                    if len <= tmp.len() {
                        tmp[..len].copy_from_slice(&exec.scratch[..len]);
                        on_request(exec, src, &tmp[..len]);
                    }
                } else if mt == wire::MSG_PART {
                    // `scratch` is written only by the next read.
                    let part = view(&exec.scratch, len);
                    on_part(exec, src, part);
                }
            }
            start_queued(exec);
        }
        if exec.busy() {
            let now = dev_millis(sys);
            if now != 0 && now.saturating_sub(exec.inflight_since_ms) > WEDGE_TIMEOUT_MS {
                exec.m_timeouts = exec.m_timeouts.wrapping_add(1);
                fail_plain(exec, OUT_TIMEOUT);
            }
        }
        start_queued(exec);
        follow_step(exec, sys);
        exec.step_ctr = exec.step_ctr.wrapping_add(1);
        if exec.step_ctr.is_multiple_of(5000) {
            telemetry::emit_counters(
                sys,
                exec.metrics_out,
                &[
                    exec.m_ops,
                    exec.m_docs_read,
                    exec.m_docs_written,
                    exec.m_errors,
                    exec.m_queue_rejects,
                ],
            );
        }
    }
    0
}
