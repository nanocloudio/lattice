//! model_edge_anchor — graph, search, vector, and time-series
//! capabilities over one RESP surface (RFC database foundation
//! §14.18–§14.21, §14.19; Phase 8).
//!
//! - `TS.ADD series ts value`       — append a sample. The key
//!   `[series][bucket][timestamp]` sorts ascending so a window is a
//!   forward scan (`models::encode_timeseries_sample_key`).
//!   `TS.RANGE series from to`      — every sample in `[from, to]`,
//!   oldest-first, as a flat `[ts, value]…` array via the shared
//!   range-scan path.
//!   `TS.AGG series from to`        — `[count, sum, min, max, avg]` over
//!   the window, folded during the same scan (numeric samples only).
//!
//! One anchor, three capabilities, three SEPARATE semantic sets —
//! §14.15's rule enforced by construction: each command family
//! compiles into ITS model's canonical keys (`models.rs`) and nothing
//! is shared between them but the byte foundations. The wire is RESP
//! because the ecosystem's graph/search/vector commands live on redis
//! wires (`redis-cli` drives every one of these), and the graph has a
//! RESP codec already.
//!
//! ## Commands, and the §-rules they encode
//!
//! - `GRAPH.VERTEX g v`            — vertex record write.
//!   `GRAPH.EDGE g src label tgt`  — BOTH adjacency postings
//!   (edge-out AND edge-in) in ONE `KV_OP_TXN`: §14.18's symmetry is
//!   a transaction, not a convention, so a crash cannot leave a
//!   half-edge.
//!   `GRAPH.OUT g v` / `GRAPH.IN g v` — bounded adjacency expansion
//!   as a prefix scan (the key layout makes the bound structural).
//!   `GRAPH.PATH g src dst maxhops` — bounded breadth-first shortest-path
//!   distance over out-edges, `-1` when `dst` is unreachable within
//!   `maxhops`. Read-only; each hop is one adjacency scan and the
//!   frontier/visited sets are capped, so the whole traversal is bounded
//!   work — pattern matching and unbounded walks stay out.
//! - `SEARCH.INDEX idx doc text…`  — tokenized postings, ALL terms in
//!   ONE transaction (§14.20: postings move with the document).
//!   `SEARCH.QUERY idx term`       — one term's posting scan, doc ids
//!   in document order.
//! - `VECTOR.ADD idx entity v…`    — embedding write (f32 vector); also
//!   writes the vector's LSH bucket posting for approximate search, in
//!   the SAME transaction so the index never lags after a crash.
//!   `VECTOR.SIM idx k q…`         — EXACT k-nearest by squared L2.
//!   `VECTOR.COS idx k q…`         — EXACT k-nearest by COSINE distance
//!   (needs vector norms — see the module-local `sqrtf`). Both sweep the
//!   whole index and are labelled `exact` on the wire.
//!   `VECTOR.ANN idx k q…`         — APPROXIMATE cosine k-nearest over
//!   just the query's LSH bucket, so it reads a fraction of the index.
//!   Labelled `approximate` (§14.21's honesty rule): it may miss a
//!   neighbour that fell in an adjacent bucket, and it never claims not
//!   to. The exact paths read the authoritative embeddings and are
//!   unaffected by a stale bucket posting.
//!
//! - `FEED.READ ks from to`        — the §15 RESOLVED CHANGE FEED:
//!   every VERSION in the revision window `(from, to]` under a
//!   keyspace prefix, deletes included, with a resume token. Built on
//!   `KV_OP_SCAN_VERSIONS`, so a window whose lower bound has been
//!   reclaimed answers COMPACTED and the feed REFUSES rather than
//!   serving the surviving tail — a partial history is
//!   indistinguishable from a whole one to a consumer, which is the
//!   entire reason §15 makes it a typed failure.
//!
//! Router pairing: POSITIONAL pair #2 (the `memcached_*` port names),
//! stamped `PROTO_MEMCACHED` — sixteen router outputs are allocated
//! and a model composition has no memcached anchor.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    clippy::duplicate_mod,
    reason = "PIC build path-mounts the fluxor SDK wholesale and shares common files via #[path]; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    reason = "fluxor module ABI: raw-pointer entry points are the contract and ABI fns carry a fixed arity"
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

#[path = "../../common/net_proto.rs"]
mod net_proto;

#[path = "../../common/redis_codec.rs"]
mod redis_codec;

#[path = "../../common/models.rs"]
mod models;

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[path = "../../common/db_ops.rs"]
mod db_ops;

#[path = "../../common/hex_core.rs"]
mod hex_core;

use net_proto::{
    net_conn_id, NET_CMD_BIND, NET_CMD_CLOSE, NET_CMD_SEND, NET_CONN_LEN, NET_MSG_ACCEPTED,
    NET_MSG_BOUND, NET_MSG_CLOSED, NET_MSG_DATA, NET_MSG_ERROR,
};
use redis_codec::{
    enc_array_header, enc_bulk, enc_error, enc_integer, enc_null_bulk, enc_simple_str, parse_one,
    Argv, ParseStep,
};
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_OP_TXN, KV_RESULT_INTEGER,
    KV_RESULT_NOT_FOUND, KV_RESULT_OK, KV_RESULT_RANGE, KV_RESULT_TXN, PROTO_MEMCACHED,
    TXN_CMP_MOD_EQUAL,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Capacities ───────────────────────────────────────────────────────

const MAX_CONNS: usize = 8;
const RECV_BUF: usize = 8192;
const SEND_BUF: usize = 8192;
const SCRATCH_BUF: usize = 16384;
/// u16 conn-id sentinel: the ip stack's ids are monotone u16s, so
/// 0xFFFF is unreachable long before the tables bind.
const SLOT_FREE: u16 = 0xFFFF;
const DEFAULT_LISTEN_PORT: u16 = 6380;
const KEY_MAX: usize = 512;
/// Max vertex-id length (mirrors `models::MAX_VERTEX_ID_LEN`), for GRAPH.PATH.
const MAX_VERTEX_ID: usize = 32;
/// GRAPH.PATH: hard cap on hops so a query is bounded work.
const MAX_PATH_HOPS: u8 = 16;
/// Search: max terms per document / vector: max dimensions.
const MAX_TERMS: usize = 32;
const MAX_DIMS: usize = 64;
/// VECTOR.SIM: max k.
const MAX_K: usize = 16;

const S_READY: u8 = 0;
const S_BUSY: u8 = 1;
const S_CLOSING: u8 = 2;

const D_IDLE: u8 = 0;
const D_WRITE: u8 = 1; // any single write / TXN awaiting ack
const D_SCAN_OUT: u8 = 2; // GRAPH.OUT / GRAPH.IN / SEARCH.QUERY page
const D_VSIM: u8 = 3; // VECTOR.SIM scan page
const D_FEED: u8 = 4; // FEED.READ version-window page
const D_DELETE: u8 = 5; // a KV_OP_DELETE awaiting ack → reply an integer count
const D_VGET: u8 = 6; // VECTOR.GET embedding read → reply the vector
const D_VDEL_READ: u8 = 7; // VECTOR.DEL: embedding read to recover the LSH bucket
const D_TRIM_SCAN: u8 = 8; // TS.TRIM: a range page to delete, then loop
const D_TRIM_DEL: u8 = 9; // TS.TRIM: a page delete acked → re-scan the window
const D_DELETE_ENTITY: u8 = 10; // delete of one logical entity → reply 0/1
const D_DV_OUT_SCAN: u8 = 11; // DELVERTEX: a page of the vertex's out-edges
const D_DV_OUT_DEL: u8 = 12; // DELVERTEX: out-edge page removed → re-scan
const D_DV_IN_SCAN: u8 = 13; // DELVERTEX: a page of the vertex's in-edges
const D_DV_IN_DEL: u8 = 14; // DELVERTEX: in-edge page removed → re-scan
const D_SRCH_ISECT: u8 = 15; // SEARCH multi-term: a later term's page to intersect
const D_VMETA_SCAN: u8 = 16; // VECTOR.SIMWHERE: collect the tag's entity set
const D_PATH_SCAN: u8 = 17; // GRAPH.PATH: a BFS frontier vertex's out-edges
const D_HSET: u8 = 18; // HSET: a TXN of hash-field puts awaiting ack → reply count
const D_LIST_READ: u8 = 19; // LPUSH/RPUSH: read the list head/tail hint
const D_LIST_WRITE: u8 = 20; // LPUSH/RPUSH: CAS-append the element
const D_LLEN: u8 = 21; // LLEN: read the head/tail hint → reply length
const D_ZADD_READ: u8 = 22; // ZADD: read a member's prior score
const D_ZADD_WRITE: u8 = 23; // ZADD: (re)index the member by score
const D_ROLLUP_WRITE: u8 = 24; // TS.ROLLUP: persist the downsampled buckets

const PHASE_INIT: u8 = 0;
const PHASE_WAIT_BOUND: u8 = 1;
const PHASE_LISTENING: u8 = 2;

#[repr(C)]
struct Slot {
    conn_id: u16,
    state: u8,
    recv: [u8; RECV_BUF],
    recv_len: usize,
    send: [u8; SEND_BUF],
    send_len: usize,
}

impl Slot {
    const fn free() -> Self {
        Slot {
            conn_id: SLOT_FREE,
            state: S_READY,
            recv: [0; RECV_BUF],
            recv_len: 0,
            send: [0; SEND_BUF],
            send_len: 0,
        }
    }
}

define_params! {
    AnchorState;

    1, listen_port, u16, 6380
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 6380); };
}

#[repr(C)]
struct AnchorState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,

    listen_port: u16,
    phase: u8,
    server_conn_id: u16,
    slots: [Slot; MAX_CONNS],

    // In-flight command.
    d_phase: u8,
    d_slot: u8,
    /// What the completed op renders as (see R_*).
    reply_kind: u8,
    cursor: u64,
    /// VECTOR.SIM/COS/ANN query + top-k accumulator.
    q: [f32; MAX_DIMS],
    q_dims: u16,
    /// Distance metric: `VMETRIC_L2` (squared L2) or `VMETRIC_COS`
    /// (cosine distance, 1 − cosine similarity).
    q_metric: u8,
    /// Whether the search was approximate (`VECTOR.ANN`, a bucket scan)
    /// or exact (a full-index scan) — the wire label the caller reads.
    q_approx: u8,
    /// `|q|` for cosine, precomputed once at query time.
    q_norm: f32,
    top_entity: [[u8; 64]; MAX_K],
    top_len: [u8; MAX_K],
    top_dist: [f32; MAX_K],
    top_n: u8,
    k: u8,
    /// TS.AGG accumulators over a window — count / sum / min / max,
    /// folded per numeric sample during the shared range scan (avg is
    /// derived at render). min/max are seeded on the first sample.
    agg_count: u32,
    agg_sum: f32,
    agg_min: f32,
    agg_max: f32,
    /// Scan-collected ids for GRAPH.OUT/IN + SEARCH.QUERY:
    /// `[len:u8][bytes]` records.
    ids: [u8; 2048],
    ids_len: u16,
    ids_count: u16,
    /// The prefix the scan resumes with.
    scan_start: [u8; KEY_MAX],
    scan_start_len: u16,
    scan_end: [u8; KEY_MAX],
    scan_end_len: u16,
    /// Which id component renders (edge second-id vs posting doc id).
    id_mode: u8,
    /// FEED.READ: staged `[rev:u64][kind:u8][klen:u16][key][vlen:u32]
    /// [value]` entries, and the window it is reading.
    feed: [u8; 4096],
    feed_len: u16,
    feed_count: u16,
    feed_from: u64,
    feed_to: u64,
    /// Commit-timestamp frontier from the newest MSG_KV_RESPONSE fence
    /// tail — reported verbatim in FEED.READ replies.
    last_frontier: u64,
    /// The inbound cursor's timestamp on a CURSOR-form FEED.READ; the
    /// issued cursor falls back to it when a page renders no entries,
    /// so an idle feed's cursor never regresses its timestamp.
    feed_resume_ts: u64,

    /// Delete context. `del_index`/`del_entity` carry a VECTOR.DEL target
    /// across the embedding read that recovers its LSH bucket;
    /// `del_count` accumulates rows removed by a paged TS.TRIM so the
    /// integer reply is the true total across pages.
    del_index: u32,
    del_entity: [u8; 64],
    del_entity_len: u8,
    del_count: u64,

    /// SEARCH multi-term AND: the lowered query terms as `[len:u8][bytes]`
    /// records, the index they query, and progress. Term 0 is scanned into
    /// `ids`; each later term is scanned into `feed` and intersected in.
    srch_multi: bool,
    srch_index: u32,
    srch_terms: [u8; 512],
    srch_terms_len: u16,
    srch_n: u8,
    srch_at: u8,

    /// TS.DOWNSAMPLE: window start + bucket width, and per-bucket sum/count
    /// folded during the range scan.
    ds_from: u64,
    ds_bucket: u64,
    ds_n: u16,
    ds_sum: [f32; MAX_DS_BUCKETS],
    ds_count: [u32; MAX_DS_BUCKETS],

    /// TS.ROLLUP: when set, the downsample buckets are PERSISTED to the rollup
    /// keyspace (`ts_rollup_series`/`ts_rollup_res`) instead of replied — a
    /// pre-materialised rollup a periodic caller refreshes and `TS.GETROLLUP`
    /// reads cheaply.
    ts_rollup_persist: bool,
    ts_rollup_series: u64,
    ts_rollup_res: u32,

    /// VECTOR.SIMWHERE: when set, the SIM scan keeps only entities present
    /// in `feed` (the tag's membership set collected first).
    vfilter: bool,

    /// GRAPH.PATH bounded BFS. The queue holds `[depth:u8][vlen:u8][vbytes]`
    /// entries consumed FIFO from `path_head`; `path_visited` is the seen set
    /// as `[vlen:u8][vbytes]`. `path_depth` is the depth of the vertex whose
    /// out-edges the in-flight scan is expanding.
    path_graph: u32,
    path_dst: [u8; MAX_VERTEX_ID],
    path_dst_len: u8,
    path_maxhops: u8,
    path_depth: u8,
    path_queue: [u8; 4096],
    path_queue_len: u16,
    path_head: u16,
    path_visited: [u8; 4096],
    path_visited_len: u16,

    /// Redis list push/read. `list_op` 0 = RPUSH (grow tail), 1 = LPUSH
    /// (grow head). `list_head`/`list_tail` are the read hint; the element
    /// is CAS-written into an absent slot, retried on collision. `lrange_*`
    /// bound an `LRANGE` slice.
    list_op: u8,
    list_id: u32,
    list_val: [u8; 512],
    list_val_len: u16,
    list_retries: u8,
    list_head: i64,
    list_tail: i64,
    lrange_start: i64,
    lrange_stop: i64,

    /// Redis sorted-set `ZADD`: the target member, its new score, and the
    /// prior score read back so the old score-index entry can be removed.
    z_set_id: u32,
    z_member: [u8; MAX_VERTEX_ID],
    z_member_len: u8,
    z_score: i64,
    z_old_score: i64,
    z_old_exists: bool,

    kv_corr: u64,
    env: [u8; SCRATCH_BUF],
    scratch: [u8; SCRATCH_BUF],

    m_sessions: u64,
    m_commands: u64,
    m_errors: u64,
    step_ctr: u64,
}

const R_OK: u8 = 0;
const R_IDS: u8 = 1;
const R_VSIM: u8 = 2;

/// Id extraction mode for scan results.
const ID_EDGE_SECOND: u8 = 0;
const ID_POSTING_DOC: u8 = 1;
/// Time-series sample: emit TWO records per entry — the timestamp
/// (decoded from the key) then the raw value — so `TS.RANGE` renders a
/// flat `[ts, value, ts, value, …]` array.
const ID_TS_SAMPLE: u8 = 2;
/// Time-series aggregate: fold each sample's value into count/sum/min/max
/// (no records); `TS.AGG` renders `[count, sum, min, max, avg]`.
const ID_TS_AGG: u8 = 3;
/// Time-series downsample: fold each sample into a fixed-width time
/// bucket, replying one aggregate per bucket.
const ID_TS_DS: u8 = 4;
/// Redis hash: emit TWO records per entry — the field (decoded from the key)
/// then its value — so `HGETALL` renders a flat `[field, value, …]` array.
const ID_HASH_PAIR: u8 = 5;
/// Redis hash single field: emit only the value; `HGET` renders one bulk
/// string, or nil when the field is absent.
const ID_HASH_GET: u8 = 6;
/// Redis list element: emit only the value; `LRANGE` renders a (sliced) array.
const ID_LIST_VALUE: u8 = 7;
/// Sorted-set member (decoded from the score-index key tail); `ZRANGE` renders
/// a (sliced) array of members in ascending score order.
const ID_ZMEMBER: u8 = 8;
/// Sorted-set score value; `ZSCORE` renders the integer score as a bulk string.
const ID_ZSCORE: u8 = 9;
/// Persisted rollup bucket: emit the bucket timestamp (from the key) then its
/// value — `TS.GETROLLUP` renders a flat `[bucket_ts, value, …]` array.
const ID_ROLLUP: u8 = 10;
const MAX_DS_BUCKETS: usize = 64;

/// Vector distance metrics.
const VMETRIC_L2: u8 = 0; // squared Euclidean — smaller is nearer
const VMETRIC_COS: u8 = 1; // 1 − cosine similarity — smaller is nearer

/// Number of random hyperplanes for the LSH bucket (`VECTOR.ANN`). A
/// vector's bucket is the sign pattern of its projection onto them, so
/// two vectors in one bucket agree on every hyperplane's side — a proxy
/// for angular closeness. 16 bits = up to 65536 buckets.
const LSH_BITS: usize = 16;

impl AnchorState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.net_in = -1;
        self.net_out = -1;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.listen_port = DEFAULT_LISTEN_PORT;
        self.phase = PHASE_INIT;
        self.server_conn_id = SLOT_FREE;
        self.slots = [const { Slot::free() }; MAX_CONNS];
        self.d_phase = D_IDLE;
        self.d_slot = 0;
        self.reply_kind = R_OK;
        self.cursor = 0;
        self.q = [0.0; MAX_DIMS];
        self.q_dims = 0;
        self.q_metric = VMETRIC_L2;
        self.q_approx = 0;
        self.q_norm = 0.0;
        self.top_entity = [[0; 64]; MAX_K];
        self.top_len = [0; MAX_K];
        self.top_dist = [0.0; MAX_K];
        self.top_n = 0;
        self.k = 0;
        self.agg_count = 0;
        self.agg_sum = 0.0;
        self.agg_min = 0.0;
        self.agg_max = 0.0;
        self.ids = [0; 2048];
        self.ids_len = 0;
        self.ids_count = 0;
        self.scan_start = [0; KEY_MAX];
        self.scan_start_len = 0;
        self.scan_end = [0; KEY_MAX];
        self.scan_end_len = 0;
        self.id_mode = ID_EDGE_SECOND;
        self.feed = [0; 4096];
        self.feed_len = 0;
        self.feed_count = 0;
        self.feed_from = 0;
        self.feed_to = 0;
        self.last_frontier = 0;
        self.feed_resume_ts = 0;
        self.del_index = 0;
        self.del_entity = [0; 64];
        self.del_entity_len = 0;
        self.del_count = 0;
        self.srch_multi = false;
        self.srch_terms_len = 0;
        self.srch_n = 0;
        self.srch_at = 0;
        self.ds_from = 0;
        self.ds_bucket = 0;
        self.ds_n = 0;
        self.ts_rollup_persist = false;
        self.ts_rollup_series = 0;
        self.ts_rollup_res = 0;
        self.vfilter = false;
        self.path_graph = 0;
        self.path_dst = [0; MAX_VERTEX_ID];
        self.path_dst_len = 0;
        self.path_maxhops = 0;
        self.path_depth = 0;
        self.path_queue = [0; 4096];
        self.path_queue_len = 0;
        self.path_head = 0;
        self.path_visited = [0; 4096];
        self.path_visited_len = 0;
        self.list_op = 0;
        self.list_id = 0;
        self.list_val = [0; 512];
        self.list_val_len = 0;
        self.list_retries = 0;
        self.list_head = 0;
        self.list_tail = 0;
        self.lrange_start = 0;
        self.lrange_stop = 0;
        self.z_set_id = 0;
        self.z_member = [0; MAX_VERTEX_ID];
        self.z_member_len = 0;
        self.z_score = 0;
        self.z_old_score = 0;
        self.z_old_exists = false;
        self.kv_corr = 0;
        self.env = [0; SCRATCH_BUF];
        self.scratch = [0; SCRATCH_BUF];
        self.m_sessions = 0;
        self.m_commands = 0;
        self.m_errors = 0;
        self.step_ctr = 0;
    }

    fn find_slot(&self, conn_id: u16) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| s.conn_id == conn_id && s.conn_id != SLOT_FREE)
    }

    fn alloc_slot(&mut self, conn_id: u16) -> Option<usize> {
        let idx = self.slots.iter().position(|s| s.conn_id == SLOT_FREE)?;
        self.slots[idx] = Slot::free();
        self.slots[idx].conn_id = conn_id;
        self.m_sessions = self.m_sessions.wrapping_add(1);
        Some(idx)
    }

    fn free_slot(&mut self, idx: usize) {
        if self.d_phase != D_IDLE && self.d_slot as usize == idx {
            self.d_phase = D_IDLE;
        }
        self.slots[idx] = Slot::free();
    }
}

// ── Small helpers ────────────────────────────────────────────────────

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn id32(bytes: &[u8]) -> u32 {
    (fnv1a64(bytes) & 0xFFFF_FFFF) as u32
}

/// Parse a decimal f32: `[-]digits[.digits]`. No exponent — a bounded
/// grammar for a bounded module; anything else refuses.
fn parse_f32(b: &[u8]) -> Option<f32> {
    if b.is_empty() || b.len() > 24 {
        return None;
    }
    let (neg, rest) = if b[0] == b'-' {
        (true, &b[1..])
    } else {
        (false, b)
    };
    if rest.is_empty() {
        return None;
    }
    let mut int: f32 = 0.0;
    let mut frac: f32 = 0.0;
    let mut scale: f32 = 1.0;
    let mut seen_dot = false;
    for &c in rest {
        if c == b'.' {
            if seen_dot {
                return None;
            }
            seen_dot = true;
            continue;
        }
        if !c.is_ascii_digit() {
            return None;
        }
        let d = (c - b'0') as f32;
        if seen_dot {
            scale *= 0.1;
            frac += d * scale;
        } else {
            int = int * 10.0 + d;
        }
    }
    let v = int + frac;
    Some(if neg { -v } else { v })
}

/// Decimal u64, no sign. Used by FEED.READ's revision bounds.
fn parse_u64_dec(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.len() > 20 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(v)
}

fn user_key(out: &mut [u8], keyspace: u32, body: &[u8]) -> Option<usize> {
    let need = 4 + body.len();
    if out.len() < need {
        return None;
    }
    out[0..4].copy_from_slice(&keyspace.to_be_bytes());
    out[4..need].copy_from_slice(body);
    Some(need)
}

fn prefix_successor(prefix: &[u8], out: &mut [u8]) -> Option<usize> {
    if prefix.len() > out.len() {
        return None;
    }
    let mut n = prefix.len();
    while n > 0 {
        if prefix[n - 1] != 0xFF {
            out[..n].copy_from_slice(&prefix[..n]);
            out[n - 1] += 1;
            return Some(n);
        }
        n -= 1;
    }
    None
}

// ── Net + KV plumbing (established anchor pattern) ───────────────────

unsafe fn net_send(anchor: &mut AnchorState, cmd: u8, conn_id: u16, data: &[u8]) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let payload_len = NET_CONN_LEN + data.len();
    if payload_len + NET_FRAME_HDR > SCRATCH_BUF {
        return false;
    }
    let id = conn_id.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    *scratch = cmd;
    *scratch.add(1) = (payload_len & 0xFF) as u8;
    *scratch.add(2) = ((payload_len >> 8) & 0xFF) as u8;
    *scratch.add(NET_FRAME_HDR) = id[0];
    *scratch.add(NET_FRAME_HDR + 1) = id[1];
    if !data.is_empty() {
        core::ptr::copy_nonoverlapping(
            data.as_ptr(),
            scratch.add(NET_FRAME_HDR + NET_CONN_LEN),
            data.len(),
        );
    }
    let total = NET_FRAME_HDR + payload_len;
    ((*sys).channel_write)(anchor.net_out, scratch, total) == total as i32
}

unsafe fn net_bind(anchor: &mut AnchorState) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let port = anchor.listen_port.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    *scratch = NET_CMD_BIND;
    *scratch.add(1) = 2;
    *scratch.add(2) = 0;
    *scratch.add(NET_FRAME_HDR) = port[0];
    *scratch.add(NET_FRAME_HDR + 1) = port[1];
    let total = NET_FRAME_HDR + 2;
    ((*sys).channel_write)(anchor.net_out, scratch, total) == total as i32
}

fn send_to_slot(anchor: &mut AnchorState, idx: usize, data: &[u8]) {
    let slot = &mut anchor.slots[idx];
    if slot.send_len + data.len() > SEND_BUF {
        slot.state = S_CLOSING;
        return;
    }
    let at = slot.send_len;
    slot.send[at..at + data.len()].copy_from_slice(data);
    slot.send_len += data.len();
    flush_slot(anchor, idx);
}

fn flush_slot(anchor: &mut AnchorState, idx: usize) {
    let (conn_id, len) = (anchor.slots[idx].conn_id, anchor.slots[idx].send_len);
    if len == 0 {
        return;
    }
    let mut buf = [0u8; SEND_BUF];
    buf[..len].copy_from_slice(&anchor.slots[idx].send[..len]);
    let sent = unsafe { net_send(anchor, NET_CMD_SEND, conn_id, &buf[..len]) };
    if sent {
        anchor.slots[idx].send_len = 0;
    }
}

const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

fn kv_send(anchor: &mut AnchorState, op: u8, body_len: usize, next: u8) -> bool {
    anchor.kv_corr = anchor.kv_corr.wrapping_add(1).max(1);
    const REQ_HEAD: usize = 18;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > anchor.env.len() {
        return false;
    }
    anchor.env[at..at + 8].copy_from_slice(&anchor.kv_corr.to_le_bytes());
    // Positional pair #2 (module header): reply routing only.
    anchor.env[at + 8] = PROTO_MEMCACHED;
    anchor.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    anchor.env[at + 13] = 0;
    anchor.env[at + 14] = 0;
    anchor.env[at + 15] = op;
    anchor.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    let sent = unsafe {
        let sys = anchor.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                anchor.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut anchor.env,
            )
    };
    if sent {
        anchor.d_phase = next;
    }
    sent
}

/// Append one PUT op into a TXN body at `p`.
fn txn_put(
    anchor: &mut AnchorState,
    mut p: usize,
    end_guard: usize,
    key: &[u8],
    value: &[u8],
) -> Option<usize> {
    let put_len = 2 + key.len() + 4 + value.len() + 1 + 8;
    if p + 3 + put_len > end_guard {
        return None;
    }
    anchor.env[p] = KV_OP_PUT;
    p += 1;
    anchor.env[p..p + 2].copy_from_slice(&(put_len as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    for (i, b) in key.iter().enumerate() {
        anchor.env[p + i] = *b;
    }
    p += key.len();
    anchor.env[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    p += 4;
    for (i, b) in value.iter().enumerate() {
        anchor.env[p + i] = *b;
    }
    p += value.len();
    anchor.env[p] = 0;
    p += 1;
    anchor.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
    p += 8;
    Some(p)
}

fn stage_scan_bounds(anchor: &mut AnchorState) -> Option<usize> {
    let sn = anchor.scan_start_len as usize;
    let en = anchor.scan_end_len as usize;
    let cursor = anchor.cursor;
    let need = 2 + sn + 2 + en + 10;
    if BODY_AT + need > anchor.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&(sn as u16).to_le_bytes());
    p += 2;
    for i in 0..sn {
        anchor.env[p + i] = anchor.scan_start[i];
    }
    p += sn;
    anchor.env[p..p + 2].copy_from_slice(&(en as u16).to_le_bytes());
    p += 2;
    for i in 0..en {
        anchor.env[p + i] = anchor.scan_end[i];
    }
    p += en;
    anchor.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    anchor.env[p..p + 2].copy_from_slice(&32u16.to_le_bytes());
    Some(need)
}

// ── Replies ──────────────────────────────────────────────────────────

fn reply_raw(anchor: &mut AnchorState, data: &[u8]) {
    let idx = anchor.d_slot as usize;
    anchor.d_phase = D_IDLE;
    if anchor.slots[idx].conn_id != SLOT_FREE && anchor.slots[idx].state == S_BUSY {
        anchor.slots[idx].state = S_READY;
    }
    let mut buf = [0u8; SEND_BUF];
    let n = data.len().min(buf.len());
    buf[..n].copy_from_slice(&data[..n]);
    send_to_slot(anchor, idx, &buf[..n]);
}

fn reply_error_str(anchor: &mut AnchorState, msg: &[u8]) {
    anchor.m_errors = anchor.m_errors.wrapping_add(1);
    let mut out = [0u8; 256];
    let mut n = 0usize;
    let _ = enc_error(&mut out, &mut n, msg);
    let mut f = [0u8; 256];
    f[..n].copy_from_slice(&out[..n]);
    reply_raw(anchor, &f[..n]);
}

fn reply_ok(anchor: &mut AnchorState) {
    let mut out = [0u8; 16];
    let mut n = 0usize;
    let _ = enc_simple_str(&mut out, &mut n, b"OK");
    let mut f = [0u8; 16];
    f[..n].copy_from_slice(&out[..n]);
    reply_raw(anchor, &f[..n]);
}

/// RESP integer reply — the count semantics Redis uses for DEL and TRIM.
fn reply_int(anchor: &mut AnchorState, v: i64) {
    let mut out = [0u8; 32];
    let mut n = 0usize;
    let _ = enc_integer(&mut out, &mut n, v);
    let mut f = [0u8; 32];
    f[..n].copy_from_slice(&out[..n]);
    reply_raw(anchor, &f[..n]);
}

/// Begin a `KV_OP_DELETE` body in `anchor.env` at `BODY_AT`: write a
/// placeholder key count and return the write cursor. Keys are appended
/// with [`del_put`]; [`del_finish`] backfills the count and sends.
fn del_begin(anchor: &mut AnchorState) -> usize {
    anchor.env[BODY_AT..BODY_AT + 2].copy_from_slice(&0u16.to_le_bytes());
    BODY_AT + 2
}

/// Append one `[key_len:u16][key]` to a delete body. Returns the new
/// cursor, or `None` if it would overrun the envelope.
fn del_put(anchor: &mut AnchorState, mut p: usize, key: &[u8]) -> Option<usize> {
    if p + 2 + key.len() > BODY_AT + 3800 {
        return None;
    }
    anchor.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + key.len()].copy_from_slice(key);
    Some(p + key.len())
}

/// Backfill the key count and send the delete under `phase`.
fn del_finish(anchor: &mut AnchorState, p: usize, count: u16, phase: u8) -> bool {
    anchor.env[BODY_AT..BODY_AT + 2].copy_from_slice(&count.to_le_bytes());
    kv_send(anchor, KV_OP_DELETE, p - BODY_AT, phase)
}

// ── Command dispatch ─────────────────────────────────────────────────

fn arg<'a>(recv: &'a [u8], argv: &Argv, i: usize) -> &'a [u8] {
    argv.arg(recv, i)
}

fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

fn begin_command(anchor: &mut AnchorState, idx: usize, recv: &[u8], argv: &Argv) {
    anchor.m_commands = anchor.m_commands.wrapping_add(1);
    if argv.count == 0 {
        return;
    }
    let cmd = arg(recv, argv, 0);
    if anchor.d_phase != D_IDLE {
        let mut out = [0u8; 64];
        let mut n = 0usize;
        let _ = enc_error(&mut out, &mut n, b"BUSY one command at a time");
        send_to_slot(anchor, idx, &out[..n].to_owned_bounded());
        return;
    }
    anchor.d_slot = idx as u8;
    anchor.slots[idx].state = S_BUSY;

    // ── Graph (§14.18) ──────────────────────────────────────────────
    if eq_ci(cmd, b"GRAPH.VERTEX") {
        if argv.count != 3 {
            reply_error_str(anchor, b"ERR GRAPH.VERTEX graph vertex");
            return;
        }
        let g = id32(arg(recv, argv, 1));
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_graph_vertex_key(&mut body, g, arg(recv, argv, 2)) else {
            reply_error_str(anchor, b"ERR vertex id too long");
            return;
        };
        let mut key = [0u8; KEY_MAX + 8];
        let Some(kn) = user_key(&mut key, models::KS_GRAPH_VERTEX, &body[..bn]) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        anchor.reply_kind = R_OK;
        let end_guard = BODY_AT + 3800;
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
        anchor.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes());
        p += 2;
        let mut k = [0u8; KEY_MAX + 8];
        k[..kn].copy_from_slice(&key[..kn]);
        let Some(np) = txn_put(anchor, p, end_guard, &k[..kn], &[]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
        if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"GRAPH.EDGE") {
        if argv.count != 5 {
            reply_error_str(anchor, b"ERR GRAPH.EDGE graph source label target");
            return;
        }
        let g = id32(arg(recv, argv, 1));
        let label = id32(arg(recv, argv, 3));
        let (src, tgt) = (arg(recv, argv, 2), arg(recv, argv, 4));
        let mut out_body = [0u8; KEY_MAX];
        let mut in_body = [0u8; KEY_MAX];
        let (Some(on), Some(inn)) = (
            models::encode_graph_edge_out_key(&mut out_body, g, src, label, tgt),
            models::encode_graph_edge_in_key(&mut in_body, g, tgt, label, src),
        ) else {
            reply_error_str(anchor, b"ERR vertex id too long");
            return;
        };
        let mut out_key = [0u8; KEY_MAX + 8];
        let mut in_key = [0u8; KEY_MAX + 8];
        let (Some(okn), Some(ikn)) = (
            user_key(&mut out_key, models::KS_GRAPH_EDGE_OUT, &out_body[..on]),
            user_key(&mut in_key, models::KS_GRAPH_EDGE_IN, &in_body[..inn]),
        ) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        anchor.reply_kind = R_OK;
        // BOTH adjacency postings in one transaction: the §14.18
        // symmetry cannot be half-true after a crash.
        let end_guard = BODY_AT + 3800;
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
        anchor.env[p..p + 2].copy_from_slice(&2u16.to_le_bytes());
        p += 2;
        let mut ka = [0u8; KEY_MAX + 8];
        ka[..okn].copy_from_slice(&out_key[..okn]);
        let Some(np) = txn_put(anchor, p, end_guard, &ka[..okn], &[]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
        let mut kb = [0u8; KEY_MAX + 8];
        kb[..ikn].copy_from_slice(&in_key[..ikn]);
        let Some(np) = txn_put(anchor, p, end_guard, &kb[..ikn], &[]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
        if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"GRAPH.DELEDGE") {
        if argv.count != 5 {
            reply_error_str(anchor, b"ERR GRAPH.DELEDGE graph source label target");
            return;
        }
        let g = id32(arg(recv, argv, 1));
        let label = id32(arg(recv, argv, 3));
        let (src, tgt) = (arg(recv, argv, 2), arg(recv, argv, 4));
        let mut out_body = [0u8; KEY_MAX];
        let mut in_body = [0u8; KEY_MAX];
        let (Some(on), Some(inn)) = (
            models::encode_graph_edge_out_key(&mut out_body, g, src, label, tgt),
            models::encode_graph_edge_in_key(&mut in_body, g, tgt, label, src),
        ) else {
            reply_error_str(anchor, b"ERR vertex id too long");
            return;
        };
        let mut out_key = [0u8; KEY_MAX + 8];
        let mut in_key = [0u8; KEY_MAX + 8];
        let (Some(okn), Some(ikn)) = (
            user_key(&mut out_key, models::KS_GRAPH_EDGE_OUT, &out_body[..on]),
            user_key(&mut in_key, models::KS_GRAPH_EDGE_IN, &in_body[..inn]),
        ) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        // BOTH half-edges leave in ONE delete: §14.18's symmetry cannot be
        // half-broken after a crash, the same reason GRAPH.EDGE writes both
        // in one transaction.
        let mut ka = [0u8; KEY_MAX + 8];
        ka[..okn].copy_from_slice(&out_key[..okn]);
        let mut kb = [0u8; KEY_MAX + 8];
        kb[..ikn].copy_from_slice(&in_key[..ikn]);
        let p = del_begin(anchor);
        let Some(p) = del_put(anchor, p, &ka[..okn]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        let Some(p) = del_put(anchor, p, &kb[..ikn]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        if !del_finish(anchor, p, 2, D_DELETE_ENTITY) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"GRAPH.DELVERTEX") {
        if argv.count != 3 {
            reply_error_str(anchor, b"ERR GRAPH.DELVERTEX graph vertex");
            return;
        }
        let g = id32(arg(recv, argv, 1));
        let v = arg(recv, argv, 2);
        if v.is_empty() || v.len() > anchor.del_entity.len() {
            reply_error_str(anchor, b"ERR vertex id too long");
            return;
        }
        // Stash the target: DELVERTEX sweeps the vertex's out-edges, then
        // its in-edges (each half-edge deleted with its symmetric twin at
        // the other vertex), then the vertex record itself.
        anchor.del_index = g;
        anchor.del_entity[..v.len()].copy_from_slice(v);
        anchor.del_entity_len = v.len() as u8;
        if !start_vertex_edge_scan(anchor, models::KS_GRAPH_EDGE_OUT, D_DV_OUT_SCAN) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"GRAPH.OUT") || eq_ci(cmd, b"GRAPH.IN") {
        if argv.count != 3 {
            reply_error_str(anchor, b"ERR GRAPH.OUT|IN graph vertex");
            return;
        }
        let g = id32(arg(recv, argv, 1));
        let v = arg(recv, argv, 2);
        // The adjacency prefix: [graph][esc(vertex)][TERM] — built by
        // the vertex-key encoder, whose var component ends with the
        // same terminator the edge key uses after its first id.
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_graph_vertex_key(&mut body, g, v) else {
            reply_error_str(anchor, b"ERR vertex id too long");
            return;
        };
        let ks = if eq_ci(cmd, b"GRAPH.OUT") {
            models::KS_GRAPH_EDGE_OUT
        } else {
            models::KS_GRAPH_EDGE_IN
        };
        if !start_id_scan(anchor, ks, &body[..bn], ID_EDGE_SECOND) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // `GRAPH.PATH g src dst maxhops` — bounded breadth-first shortest-path
    // distance from `src` to `dst` following out-edges, up to `maxhops`.
    // Replies the hop distance, or -1 if `dst` is not reachable within the
    // bound. Read-only; every hop is a bounded adjacency scan, and the
    // frontier/visited sets are capped so the whole traversal is bounded
    // work — a pattern-match or unbounded walk is deliberately NOT offered.
    if eq_ci(cmd, b"GRAPH.PATH") {
        if argv.count != 5 {
            reply_error_str(anchor, b"ERR GRAPH.PATH graph src dst maxhops");
            return;
        }
        let g = id32(arg(recv, argv, 1));
        let src = arg(recv, argv, 2);
        let dst = arg(recv, argv, 3);
        let Some(maxhops) = parse_u64_dec(arg(recv, argv, 4)) else {
            reply_error_str(anchor, b"ERR maxhops is a decimal u64");
            return;
        };
        if src.is_empty()
            || dst.is_empty()
            || src.len() > MAX_VERTEX_ID
            || dst.len() > MAX_VERTEX_ID
        {
            reply_error_str(anchor, b"ERR vertex id length");
            return;
        }
        if maxhops > MAX_PATH_HOPS as u64 {
            reply_error_str(anchor, b"ERR maxhops exceeds bound");
            return;
        }
        if src == dst {
            reply_int(anchor, 0);
            return;
        }
        anchor.path_graph = g;
        anchor.path_dst[..dst.len()].copy_from_slice(dst);
        anchor.path_dst_len = dst.len() as u8;
        anchor.path_maxhops = maxhops as u8;
        anchor.path_queue_len = 0;
        anchor.path_head = 0;
        anchor.path_visited_len = 0;
        let _ = path_visited_add(anchor, src);
        if !path_enqueue(anchor, 0, src) {
            reply_error_str(anchor, b"ERR path too large");
            return;
        }
        path_dequeue_and_scan(anchor);
        return;
    }

    // ── Redis hashes ────────────────────────────────────────────────
    // `HSET h field value [field value …]` — write each field; reply the
    // number of fields written. A hash field is `KS_HASH[hash_id][field]`,
    // so HGET is a point read and HGETALL a bounded prefix scan.
    if eq_ci(cmd, b"HSET") {
        if argv.count < 4 || !(argv.count - 2).is_multiple_of(2) {
            reply_error_str(anchor, b"ERR HSET hash field value [field value ...]");
            return;
        }
        let h = id32(arg(recv, argv, 1));
        let pairs = (argv.count as usize - 2) / 2;
        anchor.reply_kind = R_OK;
        let end_guard = BODY_AT + 3800;
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // 0 comparisons
        p += 2;
        anchor.env[p..p + 2].copy_from_slice(&(pairs as u16).to_le_bytes()); // THEN
        p += 2;
        for i in 0..pairs {
            let field = arg(recv, argv, 2 + i * 2);
            let value = arg(recv, argv, 3 + i * 2);
            if field.len() > MAX_VERTEX_ID || value.len() > 512 {
                reply_error_str(anchor, b"ERR field or value too long");
                return;
            }
            let mut body = [0u8; KEY_MAX];
            let Some(bn) = models::encode_hash_field_key(&mut body, h as u32, field) else {
                reply_error_str(anchor, b"ERR internal");
                return;
            };
            let mut key = [0u8; KEY_MAX + 8];
            let Some(kn) = user_key(&mut key, models::KS_HASH, &body[..bn]) else {
                reply_error_str(anchor, b"ERR internal");
                return;
            };
            let mut vbuf = [0u8; 512];
            vbuf[..value.len()].copy_from_slice(value);
            let Some(np) = txn_put(anchor, p, end_guard, &key[..kn], &vbuf[..value.len()]) else {
                reply_error_str(anchor, b"ERR too large");
                return;
            };
            p = np;
        }
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // ELSE
        p += 2;
        // A HSET reply is the field count; stash it so the ack renders it.
        anchor.del_count = pairs as u64;
        if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_HSET) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"HGET") {
        if argv.count != 3 {
            reply_error_str(anchor, b"ERR HGET hash field");
            return;
        }
        let h = id32(arg(recv, argv, 1));
        let field = arg(recv, argv, 2);
        if field.len() > MAX_VERTEX_ID {
            reply_error_str(anchor, b"ERR field too long");
            return;
        }
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_hash_field_key(&mut body, h as u32, field) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        if !start_id_scan(anchor, models::KS_HASH, &body[..bn], ID_HASH_GET) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"HGETALL") {
        if argv.count != 2 {
            reply_error_str(anchor, b"ERR HGETALL hash");
            return;
        }
        let h = id32(arg(recv, argv, 1));
        let body = (h as u32).to_be_bytes();
        if !start_id_scan(anchor, models::KS_HASH, &body, ID_HASH_PAIR) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // ── Redis lists ─────────────────────────────────────────────────
    // `RPUSH k v` / `LPUSH k v` append/prepend one element and reply the new
    // length. The element is CAS-written into an absent index slot, so
    // concurrent pushers never lose an element — a slot collision just
    // retries against a re-read head/tail hint.
    if eq_ci(cmd, b"RPUSH") || eq_ci(cmd, b"LPUSH") {
        if argv.count != 3 {
            reply_error_str(anchor, b"ERR PUSH key value");
            return;
        }
        let value = arg(recv, argv, 2);
        if value.len() > 512 {
            reply_error_str(anchor, b"ERR value too long");
            return;
        }
        anchor.list_op = if eq_ci(cmd, b"LPUSH") { 1 } else { 0 };
        anchor.list_id = id32(arg(recv, argv, 1));
        anchor.list_val[..value.len()].copy_from_slice(value);
        anchor.list_val_len = value.len() as u16;
        anchor.list_retries = 0;
        list_start_read(anchor);
        return;
    }

    if eq_ci(cmd, b"LLEN") {
        if argv.count != 2 {
            reply_error_str(anchor, b"ERR LLEN key");
            return;
        }
        anchor.list_id = id32(arg(recv, argv, 1));
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_list_meta_key(&mut body, anchor.list_id) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        if !stage_single_scan(anchor, models::KS_LIST_META, &body[..bn], D_LLEN) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"LRANGE") {
        if argv.count != 4 {
            reply_error_str(anchor, b"ERR LRANGE key start stop");
            return;
        }
        let (Some(start), Some(stop)) = (
            parse_i64_dec(arg(recv, argv, 2)),
            parse_i64_dec(arg(recv, argv, 3)),
        ) else {
            reply_error_str(anchor, b"ERR start/stop are integers");
            return;
        };
        anchor.list_id = id32(arg(recv, argv, 1));
        anchor.lrange_start = start;
        anchor.lrange_stop = stop;
        let body = anchor.list_id.to_be_bytes();
        if !start_id_scan(anchor, models::KS_LIST, &body, ID_LIST_VALUE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // ── Redis sorted sets (integer scores) ──────────────────────────
    // `ZADD set score member` indexes `member` by `score` (re-scoring first
    // removes the stale index entry); `ZSCORE` point-reads a score; `ZRANGE`
    // returns members in ascending score order.
    if eq_ci(cmd, b"ZADD") {
        if argv.count != 4 {
            reply_error_str(anchor, b"ERR ZADD set score member");
            return;
        }
        let Some(score) = parse_i64_dec(arg(recv, argv, 2)) else {
            reply_error_str(anchor, b"ERR score is an integer");
            return;
        };
        let member = arg(recv, argv, 3);
        if member.is_empty() || member.len() > MAX_VERTEX_ID {
            reply_error_str(anchor, b"ERR member length");
            return;
        }
        anchor.z_set_id = id32(arg(recv, argv, 1));
        anchor.z_member[..member.len()].copy_from_slice(member);
        anchor.z_member_len = member.len() as u8;
        anchor.z_score = score;
        anchor.z_old_exists = false;
        anchor.z_old_score = 0;
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_zmember_key(&mut body, anchor.z_set_id, member) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        if !stage_single_scan(anchor, models::KS_ZMEMBER, &body[..bn], D_ZADD_READ) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"ZSCORE") {
        if argv.count != 3 {
            reply_error_str(anchor, b"ERR ZSCORE set member");
            return;
        }
        let set = id32(arg(recv, argv, 1));
        let member = arg(recv, argv, 2);
        if member.len() > MAX_VERTEX_ID {
            reply_error_str(anchor, b"ERR member too long");
            return;
        }
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_zmember_key(&mut body, set, member) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        if !start_id_scan(anchor, models::KS_ZMEMBER, &body[..bn], ID_ZSCORE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"ZRANGE") {
        if argv.count != 4 {
            reply_error_str(anchor, b"ERR ZRANGE set start stop");
            return;
        }
        let (Some(start), Some(stop)) = (
            parse_i64_dec(arg(recv, argv, 2)),
            parse_i64_dec(arg(recv, argv, 3)),
        ) else {
            reply_error_str(anchor, b"ERR start/stop are integers");
            return;
        };
        let set = id32(arg(recv, argv, 1));
        anchor.lrange_start = start;
        anchor.lrange_stop = stop;
        let body = set.to_be_bytes();
        if !start_id_scan(anchor, models::KS_ZSCORE, &body, ID_ZMEMBER) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // ── Search (§14.20) ─────────────────────────────────────────────
    if eq_ci(cmd, b"SEARCH.INDEX") {
        if argv.count < 4 {
            reply_error_str(anchor, b"ERR SEARCH.INDEX index doc term...");
            return;
        }
        let index_id = id32(arg(recv, argv, 1));
        let doc = arg(recv, argv, 2);
        let terms = argv.count as usize - 3;
        if terms > MAX_TERMS {
            reply_error_str(anchor, b"ERR too many terms");
            return;
        }
        anchor.reply_kind = R_OK;
        // Every posting in ONE transaction: the document's terms move
        // together (§14.20).
        let end_guard = BODY_AT + 3800;
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
        anchor.env[p..p + 2].copy_from_slice(&(terms as u16).to_le_bytes());
        p += 2;
        for t in 0..terms {
            let term = arg(recv, argv, 3 + t);
            let mut lower = [0u8; 64];
            if term.is_empty() || term.len() > 64 {
                reply_error_str(anchor, b"ERR term too long");
                return;
            }
            for (i, c) in term.iter().enumerate() {
                lower[i] = c.to_ascii_lowercase();
            }
            let mut body = [0u8; KEY_MAX];
            let Some(bn) = models::encode_search_posting_key(
                &mut body,
                index_id,
                0,
                &lower[..term.len()],
                doc,
            ) else {
                reply_error_str(anchor, b"ERR posting too long");
                return;
            };
            let mut key = [0u8; KEY_MAX + 8];
            let Some(kn) = user_key(&mut key, models::KS_SEARCH_POSTING, &body[..bn]) else {
                reply_error_str(anchor, b"ERR internal");
                return;
            };
            let mut k = [0u8; KEY_MAX + 8];
            k[..kn].copy_from_slice(&key[..kn]);
            let Some(np) = txn_put(anchor, p, end_guard, &k[..kn], &[]) else {
                reply_error_str(anchor, b"ERR document too large for one command");
                return;
            };
            p = np;
        }
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
        if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // Un-index a document: delete the postings for the terms named, the
    // exact keys `SEARCH.INDEX` wrote. The caller re-supplies the terms
    // (the anchor keeps no doc→terms manifest), so this is the inverse of
    // INDEX with the same argument shape. Replies the posting count deleted.
    if eq_ci(cmd, b"SEARCH.DELETE") {
        if argv.count < 4 {
            reply_error_str(anchor, b"ERR SEARCH.DELETE index doc term...");
            return;
        }
        let index_id = id32(arg(recv, argv, 1));
        let doc = arg(recv, argv, 2);
        let terms = argv.count as usize - 3;
        if terms > MAX_TERMS {
            reply_error_str(anchor, b"ERR too many terms");
            return;
        }
        let mut p = del_begin(anchor);
        for t in 0..terms {
            let term = arg(recv, argv, 3 + t);
            let mut lower = [0u8; 64];
            if term.is_empty() || term.len() > 64 {
                reply_error_str(anchor, b"ERR term too long");
                return;
            }
            for (i, c) in term.iter().enumerate() {
                lower[i] = c.to_ascii_lowercase();
            }
            let mut body = [0u8; KEY_MAX];
            let Some(bn) = models::encode_search_posting_key(
                &mut body,
                index_id,
                0,
                &lower[..term.len()],
                doc,
            ) else {
                reply_error_str(anchor, b"ERR posting too long");
                return;
            };
            let mut key = [0u8; KEY_MAX + 8];
            let Some(kn) = user_key(&mut key, models::KS_SEARCH_POSTING, &body[..bn]) else {
                reply_error_str(anchor, b"ERR internal");
                return;
            };
            let mut k = [0u8; KEY_MAX + 8];
            k[..kn].copy_from_slice(&key[..kn]);
            let Some(np) = del_put(anchor, p, &k[..kn]) else {
                reply_error_str(anchor, b"ERR document too large for one command");
                return;
            };
            p = np;
        }
        if !del_finish(anchor, p, terms as u16, D_DELETE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"SEARCH.QUERY") {
        // `SEARCH.QUERY index term…` — one term is a posting scan; several
        // terms are ANDed by intersecting their posting lists.
        if argv.count < 3 {
            reply_error_str(anchor, b"ERR SEARCH.QUERY index term...");
            return;
        }
        let index_id = id32(arg(recv, argv, 1));
        let nterms = argv.count as usize - 2;
        if nterms > MAX_TERMS {
            reply_error_str(anchor, b"ERR too many terms");
            return;
        }
        // Stash every lowered term as a [len][bytes] record.
        anchor.srch_index = index_id;
        anchor.srch_terms_len = 0;
        anchor.srch_n = nterms as u8;
        anchor.srch_at = 0;
        anchor.srch_multi = nterms > 1;
        for t in 0..nterms {
            let term = arg(recv, argv, 2 + t);
            if term.is_empty() || term.len() > 64 {
                reply_error_str(anchor, b"ERR term too long");
                return;
            }
            let w = anchor.srch_terms_len as usize;
            if w + 1 + term.len() > anchor.srch_terms.len() {
                reply_error_str(anchor, b"ERR query too long");
                return;
            }
            anchor.srch_terms[w] = term.len() as u8;
            for (i, c) in term.iter().enumerate() {
                anchor.srch_terms[w + 1 + i] = c.to_ascii_lowercase();
            }
            anchor.srch_terms_len = (w + 1 + term.len()) as u16;
        }
        // Scan term 0 into `ids`.
        if !start_search_term_scan(anchor, 0, D_SCAN_OUT) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // ── Vector (§14.21) ─────────────────────────────────────────────
    if eq_ci(cmd, b"VECTOR.ADD") {
        if (argv.count as usize) < 4 || argv.count as usize - 3 > MAX_DIMS {
            reply_error_str(anchor, b"ERR VECTOR.ADD index entity v...");
            return;
        }
        let index_id = id32(arg(recv, argv, 1));
        let entity = arg(recv, argv, 2);
        let dims = argv.count as usize - 3;
        let mut value = [0u8; MAX_DIMS * 4];
        let mut qv = [0.0f32; MAX_DIMS];
        for d in 0..dims {
            let Some(f) = parse_f32(arg(recv, argv, 3 + d)) else {
                reply_error_str(anchor, b"ERR vector components are decimal numbers");
                return;
            };
            qv[d] = f;
            value[d * 4..d * 4 + 4].copy_from_slice(&f.to_le_bytes());
        }
        // The authoritative embedding, and its LSH bucket posting — both
        // written in one transaction so the approximate index can never
        // lag the exact one after a crash.
        let bucket = lsh_bucket(&qv[..dims]) as u32;
        let mut ebody = [0u8; KEY_MAX];
        let mut abody = [0u8; KEY_MAX];
        let (Some(ebn), Some(abn)) = (
            models::encode_vector_embedding_key(&mut ebody, index_id, entity),
            models::encode_vector_ann_key(&mut abody, index_id, bucket, entity),
        ) else {
            reply_error_str(anchor, b"ERR entity id too long");
            return;
        };
        let mut ekey = [0u8; KEY_MAX + 8];
        let mut akey = [0u8; KEY_MAX + 8];
        let (Some(ekn), Some(akn)) = (
            user_key(&mut ekey, models::KS_VECTOR_EMBEDDING, &ebody[..ebn]),
            user_key(&mut akey, models::KS_VECTOR_ANN, &abody[..abn]),
        ) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        anchor.reply_kind = R_OK;
        let end_guard = BODY_AT + 3800;
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // 0 comparisons
        p += 2;
        anchor.env[p..p + 2].copy_from_slice(&2u16.to_le_bytes()); // THEN: 2 writes
        p += 2;
        let mut v = [0u8; MAX_DIMS * 4];
        v[..dims * 4].copy_from_slice(&value[..dims * 4]);
        let mut k1 = [0u8; KEY_MAX + 8];
        k1[..ekn].copy_from_slice(&ekey[..ekn]);
        let Some(np) = txn_put(anchor, p, end_guard, &k1[..ekn], &v[..dims * 4]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
        let mut k2 = [0u8; KEY_MAX + 8];
        k2[..akn].copy_from_slice(&akey[..akn]);
        let Some(np) = txn_put(anchor, p, end_guard, &k2[..akn], &v[..dims * 4]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // ELSE: 0
        p += 2;
        if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // Read one embedding by id — the exact vector `VECTOR.ADD` stored,
    // which the nearest-neighbour reads never surface directly. Replies an
    // array of the components, or a nil array if the entity is unknown.
    if eq_ci(cmd, b"VECTOR.GET") {
        if argv.count != 3 {
            reply_error_str(anchor, b"ERR VECTOR.GET index entity");
            return;
        }
        let index_id = id32(arg(recv, argv, 1));
        let entity = arg(recv, argv, 2);
        let mut ebody = [0u8; KEY_MAX];
        let Some(ebn) = models::encode_vector_embedding_key(&mut ebody, index_id, entity) else {
            reply_error_str(anchor, b"ERR entity id too long");
            return;
        };
        let mut ekey = [0u8; KEY_MAX + 8];
        let Some(ekn) = user_key(&mut ekey, models::KS_VECTOR_EMBEDDING, &ebody[..ebn]) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&(ekn as u16).to_le_bytes());
        p += 2;
        anchor.env[p..p + ekn].copy_from_slice(&ekey[..ekn]);
        p += ekn;
        if !kv_send(anchor, KV_OP_GET, p - BODY_AT, D_VGET) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // Delete an embedding and its LSH posting. The posting key needs the
    // vector's bucket, which is not recoverable from the id, so this reads
    // the embedding first, recomputes the SAME `lsh_bucket` the write used,
    // then deletes both keys atomically. The entity is stashed across the
    // read.
    if eq_ci(cmd, b"VECTOR.DEL") {
        if argv.count != 3 {
            reply_error_str(anchor, b"ERR VECTOR.DEL index entity");
            return;
        }
        let index_id = id32(arg(recv, argv, 1));
        let entity = arg(recv, argv, 2);
        if entity.is_empty() || entity.len() > anchor.del_entity.len() {
            reply_error_str(anchor, b"ERR entity id too long");
            return;
        }
        anchor.del_index = index_id;
        anchor.del_entity[..entity.len()].copy_from_slice(entity);
        anchor.del_entity_len = entity.len() as u8;
        let mut ebody = [0u8; KEY_MAX];
        let Some(ebn) = models::encode_vector_embedding_key(&mut ebody, index_id, entity) else {
            reply_error_str(anchor, b"ERR entity id too long");
            return;
        };
        let mut ekey = [0u8; KEY_MAX + 8];
        let Some(ekn) = user_key(&mut ekey, models::KS_VECTOR_EMBEDDING, &ebody[..ebn]) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&(ekn as u16).to_le_bytes());
        p += 2;
        anchor.env[p..p + ekn].copy_from_slice(&ekey[..ekn]);
        p += ekn;
        if !kv_send(anchor, KV_OP_GET, p - BODY_AT, D_VDEL_READ) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // `index k q…` k-nearest. SIM is squared-L2, COS is cosine; both
    // EXACT (a full-index scan). ANN is cosine over ONE LSH bucket —
    // approximate, and it says so on the wire.
    if eq_ci(cmd, b"VECTOR.SIM") {
        begin_vector_search(anchor, recv, argv, VMETRIC_L2, false);
        return;
    }
    if eq_ci(cmd, b"VECTOR.COS") {
        begin_vector_search(anchor, recv, argv, VMETRIC_COS, false);
        return;
    }
    if eq_ci(cmd, b"VECTOR.ANN") {
        begin_vector_search(anchor, recv, argv, VMETRIC_COS, true);
        return;
    }

    // `VECTOR.TAG index entity tag` — attach a metadata tag to an entity,
    // so `VECTOR.SIMWHERE` can restrict a search to entities carrying it.
    if eq_ci(cmd, b"VECTOR.TAG") {
        if argv.count != 4 {
            reply_error_str(anchor, b"ERR VECTOR.TAG index entity tag");
            return;
        }
        let index_id = id32(arg(recv, argv, 1));
        let entity = arg(recv, argv, 2);
        let tag = arg(recv, argv, 3);
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_vector_meta_key(&mut body, index_id, tag, entity) else {
            reply_error_str(anchor, b"ERR id or tag too long");
            return;
        };
        let mut key = [0u8; KEY_MAX + 8];
        let Some(kn) = user_key(&mut key, models::KS_VECTOR_META, &body[..bn]) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        anchor.reply_kind = R_OK;
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
        anchor.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes());
        p += 2;
        let mut k = [0u8; KEY_MAX + 8];
        k[..kn].copy_from_slice(&key[..kn]);
        let Some(np) = txn_put(anchor, p, BODY_AT + 3800, &k[..kn], &[]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
        if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // `VECTOR.SIMWHERE index tag k q…` — exact k-nearest (squared L2)
    // restricted to entities carrying `tag`. The tag's entity set is
    // collected first, then the embedding scan keeps only those.
    if eq_ci(cmd, b"VECTOR.SIMWHERE") {
        if argv.count < 5 {
            reply_error_str(anchor, b"ERR VECTOR.SIMWHERE index tag k q...");
            return;
        }
        let index_id = id32(arg(recv, argv, 1));
        let tag = arg(recv, argv, 2);
        let Some(kq) = parse_f32(arg(recv, argv, 3)) else {
            reply_error_str(anchor, b"ERR k must be a number");
            return;
        };
        let k = kq as usize;
        if k == 0 || k > MAX_K {
            reply_error_str(anchor, b"ERR k out of range");
            return;
        }
        let dims = argv.count as usize - 4;
        if dims == 0 || dims > MAX_DIMS {
            reply_error_str(anchor, b"ERR bad dimension count");
            return;
        }
        for d in 0..dims {
            let Some(f) = parse_f32(arg(recv, argv, 4 + d)) else {
                reply_error_str(anchor, b"ERR vector components are decimal numbers");
                return;
            };
            anchor.q[d] = f;
        }
        anchor.q_dims = dims as u16;
        anchor.k = k as u8;
        anchor.q_metric = VMETRIC_L2;
        anchor.q_approx = 0;
        anchor.q_norm = 0.0;
        anchor.top_n = 0;
        anchor.del_index = index_id; // carry the index across the meta scan
        anchor.vfilter = true;
        anchor.feed_len = 0;
        anchor.feed_count = 0;
        // Scan the tag's entity set into `feed` first.
        let mut mbody = [0u8; KEY_MAX];
        let Some(mbn) = models::encode_vector_meta_prefix(&mut mbody, index_id, tag) else {
            reply_error_str(anchor, b"ERR tag too long");
            return;
        };
        let mut start = [0u8; KEY_MAX];
        let Some(sn) = user_key(&mut start, models::KS_VECTOR_META, &mbody[..mbn]) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let mut end = [0u8; KEY_MAX];
        let Some(en) = prefix_successor(&start[..sn], &mut end) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
        anchor.scan_start_len = sn as u16;
        anchor.scan_end[..en].copy_from_slice(&end[..en]);
        anchor.scan_end_len = en as u16;
        anchor.cursor = 0;
        let Some(bn) = stage_scan_bounds(anchor) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_VMETA_SCAN) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"FEED.READ") {
        // Two forms:
        //   FEED.READ <prefix> <from_rev> <to_rev>   — open a window
        //   FEED.READ <prefix> CURSOR <hex-token>    — resume from a
        //     FeedCursor issued by a previous reply, up to now.
        // Every reply is ONE bounded page:
        //   [cursor_hex, frontier, [[rev, ts, kind, key, value]…]]
        // and the client pages by re-sending the returned cursor. An
        // empty entries array means caught up to the window bound.
        if argv.count != 4 {
            reply_error_str(
                anchor,
                b"ERR FEED.READ prefix from to | prefix CURSOR token",
            );
            return;
        }
        let prefix = arg(recv, argv, 1);
        let (from, to) = if eq_ci(arg(recv, argv, 2), b"CURSOR") {
            let tok = arg(recv, argv, 3);
            let mut cb = [0u8; db_ops::FeedCursor::WIRE_LEN];
            let Some(cn) = hex_core::hex_decode(tok, &mut cb) else {
                reply_error_str(anchor, b"ERR cursor is not hex");
                return;
            };
            let Some(cur) = db_ops::FeedCursor::decode(&cb[..cn]) else {
                reply_error_str(anchor, b"ERR cursor unreadable; resubscribe");
                return;
            };
            if cur.format_version != db_ops::FEED_EVENT_FORMAT_VERSION {
                // The token was issued against another event format.
                // Serving it would hand the consumer events it will
                // misparse — refuse loudly instead (§15).
                reply_error_str(anchor, b"ERR cursor format stale; resubscribe");
                return;
            }
            // Topology check: this composition
            // serves the single implicit range at generation 1. A
            // cursor naming any other range identity or generation
            // was issued across a topology transition and cannot be
            // resumed by revision here — the spelled TOPOLOGY refusal
            // is the in-stream transition signal in its minimal form;
            // the consumer derives successors via the §5.4 cursor
            // rules (split/merge of a fed keyrange is refused until
            // multi-range feed serving lands).
            let mut expect_range = [0u8; 16];
            expect_range[15] = 1;
            if cur.range_id != expect_range || cur.range_generation != 1 {
                reply_error_str(
                    anchor,
                    b"TOPOLOGY range changed; derive successor cursors and resubscribe",
                );
                return;
            }
            anchor.feed_resume_ts = cur.timestamp;
            (cur.revision, 0u64)
        } else {
            let (Some(from), Some(to)) = (
                parse_u64_dec(arg(recv, argv, 2)),
                parse_u64_dec(arg(recv, argv, 3)),
            ) else {
                reply_error_str(anchor, b"ERR FEED.READ needs decimal revisions");
                return;
            };
            if to != 0 && from > to {
                reply_error_str(anchor, b"ERR inverted revision window");
                return;
            }
            anchor.feed_resume_ts = 0;
            (from, to)
        };
        anchor.feed_from = from;
        anchor.feed_to = to;
        anchor.cursor = 0;
        // The span: everything under the caller's literal prefix. A
        // feed over a prefix is a feed over a keyspace region, and the
        // consumer names it explicitly rather than the server guessing.
        let mut start = [0u8; KEY_MAX];
        if prefix.len() > KEY_MAX - 8 {
            reply_error_str(anchor, b"ERR prefix too long");
            return;
        }
        start[..prefix.len()].copy_from_slice(prefix);
        let sn = prefix.len();
        let mut end = [0u8; KEY_MAX];
        let Some(en) = prefix_successor(&start[..sn], &mut end) else {
            reply_error_str(anchor, b"ERR prefix has no successor");
            return;
        };
        anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
        anchor.scan_start_len = sn as u16;
        anchor.scan_end[..en].copy_from_slice(&end[..en]);
        anchor.scan_end_len = en as u16;
        if !send_feed_scan(anchor) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // ── Time series (§14.19) ────────────────────────────────────────
    //
    // `TS.ADD series timestamp value` — append a sample.
    // `TS.RANGE series from to`       — every sample in `[from, to]`,
    //                                   oldest-first, as `[ts, value]…`.
    //
    // A sample key is `[series][bucket][timestamp]` all ascending, so a
    // window is a forward range scan (`encode_timeseries_sample_key`'s
    // whole reason). `bucket` is 0 here: a single bucket per series.
    // Bucketing is a scan-locality optimisation deferred to a follow-on;
    // the window scan is correct without it because one bucket still
    // sorts by timestamp.
    if eq_ci(cmd, b"TS.ADD") {
        if argv.count != 4 {
            reply_error_str(anchor, b"ERR TS.ADD series timestamp value");
            return;
        }
        let series = id32(arg(recv, argv, 1)) as u64;
        let Some(ts) = parse_u64_dec(arg(recv, argv, 2)) else {
            reply_error_str(anchor, b"ERR timestamp is a decimal u64");
            return;
        };
        let value = arg(recv, argv, 3);
        if value.len() > 127 {
            reply_error_str(anchor, b"ERR value too long");
            return;
        }
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_timeseries_sample_key(&mut body, series, 0, ts) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let mut key = [0u8; KEY_MAX + 8];
        let Some(kn) = user_key(&mut key, models::KS_TIMESERIES_SAMPLE, &body[..bn]) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        anchor.reply_kind = R_OK;
        let end_guard = BODY_AT + 3800;
        let mut p = BODY_AT;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // 0 comparisons
        p += 2;
        anchor.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes()); // THEN: 1 write
        p += 2;
        let mut k = [0u8; KEY_MAX + 8];
        k[..kn].copy_from_slice(&key[..kn]);
        let mut v = [0u8; 128];
        v[..value.len()].copy_from_slice(value);
        let Some(np) = txn_put(anchor, p, end_guard, &k[..kn], &v[..value.len()]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
        anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // ELSE: 0 writes
        p += 2;
        if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    if eq_ci(cmd, b"TS.RANGE") {
        if argv.count != 4 {
            reply_error_str(anchor, b"ERR TS.RANGE series from to");
            return;
        }
        let series = id32(arg(recv, argv, 1)) as u64;
        let (Some(from), Some(to)) = (
            parse_u64_dec(arg(recv, argv, 2)),
            parse_u64_dec(arg(recv, argv, 3)),
        ) else {
            reply_error_str(anchor, b"ERR from/to are decimal u64");
            return;
        };
        if from > to {
            reply_error_str(anchor, b"ERR inverted time window");
            return;
        }
        if start_ts_scan(anchor, series, from, to, ID_TS_SAMPLE) != Some(true) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // `TS.AGG series from to` — count / sum / min / max / avg over the
    // window, computed by folding each numeric sample during the same
    // range scan `TS.RANGE` uses (non-numeric samples are skipped).
    if eq_ci(cmd, b"TS.AGG") {
        if argv.count != 4 {
            reply_error_str(anchor, b"ERR TS.AGG series from to");
            return;
        }
        let series = id32(arg(recv, argv, 1)) as u64;
        let (Some(from), Some(to)) = (
            parse_u64_dec(arg(recv, argv, 2)),
            parse_u64_dec(arg(recv, argv, 3)),
        ) else {
            reply_error_str(anchor, b"ERR from/to are decimal u64");
            return;
        };
        if from > to {
            reply_error_str(anchor, b"ERR inverted time window");
            return;
        }
        if start_ts_scan(anchor, series, from, to, ID_TS_AGG) != Some(true) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // `TS.DOWNSAMPLE series from to bucket` — the window's samples folded
    // into fixed-width `bucket` time buckets, replying `[bucket_ts, avg]…`
    // for each non-empty bucket (oldest first). A rollup / downsample.
    if eq_ci(cmd, b"TS.DOWNSAMPLE") {
        if argv.count != 5 {
            reply_error_str(anchor, b"ERR TS.DOWNSAMPLE series from to bucket");
            return;
        }
        let series = id32(arg(recv, argv, 1)) as u64;
        let (Some(from), Some(to), Some(bucket)) = (
            parse_u64_dec(arg(recv, argv, 2)),
            parse_u64_dec(arg(recv, argv, 3)),
            parse_u64_dec(arg(recv, argv, 4)),
        ) else {
            reply_error_str(anchor, b"ERR from/to/bucket are decimal u64");
            return;
        };
        if from > to {
            reply_error_str(anchor, b"ERR inverted time window");
            return;
        }
        if bucket == 0 {
            reply_error_str(anchor, b"ERR bucket width must be positive");
            return;
        }
        let n = ((to - from) / bucket.max(1)) as usize + 1;
        if n > MAX_DS_BUCKETS {
            reply_error_str(anchor, b"ERR too many buckets for one window");
            return;
        }
        anchor.ds_from = from;
        anchor.ds_bucket = bucket;
        anchor.ds_n = n as u16;
        anchor.ts_rollup_persist = false;
        for i in 0..n {
            anchor.ds_sum[i] = 0.0;
            anchor.ds_count[i] = 0;
        }
        if start_ts_scan(anchor, series, from, to, ID_TS_DS) != Some(true) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // `TS.ROLLUP series from to bucket` — like TS.DOWNSAMPLE, but PERSISTS the
    // downsampled buckets into the rollup keyspace and replies the count. A
    // periodic caller refreshes rollups so `TS.GETROLLUP` reads them cheaply
    // (the pre-materialisation the raw-sample scan avoids on every query).
    if eq_ci(cmd, b"TS.ROLLUP") {
        if argv.count != 5 {
            reply_error_str(anchor, b"ERR TS.ROLLUP series from to bucket");
            return;
        }
        let series = id32(arg(recv, argv, 1)) as u64;
        let (Some(from), Some(to), Some(bucket)) = (
            parse_u64_dec(arg(recv, argv, 2)),
            parse_u64_dec(arg(recv, argv, 3)),
            parse_u64_dec(arg(recv, argv, 4)),
        ) else {
            reply_error_str(anchor, b"ERR from/to/bucket are decimal u64");
            return;
        };
        if from > to || bucket == 0 {
            reply_error_str(anchor, b"ERR window/bucket invalid");
            return;
        }
        let n = ((to - from) / bucket.max(1)) as usize + 1;
        if n > MAX_DS_BUCKETS {
            reply_error_str(anchor, b"ERR too many buckets for one window");
            return;
        }
        anchor.ds_from = from;
        anchor.ds_bucket = bucket;
        anchor.ds_n = n as u16;
        anchor.ts_rollup_persist = true;
        anchor.ts_rollup_series = series;
        anchor.ts_rollup_res = bucket as u32;
        for i in 0..n {
            anchor.ds_sum[i] = 0.0;
            anchor.ds_count[i] = 0;
        }
        if start_ts_scan(anchor, series, from, to, ID_TS_DS) != Some(true) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // `TS.GETROLLUP series resolution from to` — read the pre-materialised
    // rollup buckets for a series at a resolution, as `[bucket_ts, avg, …]`.
    if eq_ci(cmd, b"TS.GETROLLUP") {
        if argv.count != 5 {
            reply_error_str(anchor, b"ERR TS.GETROLLUP series resolution from to");
            return;
        }
        let series = id32(arg(recv, argv, 1)) as u64;
        let (Some(res), Some(from), Some(to)) = (
            parse_u64_dec(arg(recv, argv, 2)),
            parse_u64_dec(arg(recv, argv, 3)),
            parse_u64_dec(arg(recv, argv, 4)),
        ) else {
            reply_error_str(anchor, b"ERR resolution/from/to are decimal u64");
            return;
        };
        let mut sbody = [0u8; models::TIMESERIES_ROLLUP_KEY_LEN];
        let mut ebody = [0u8; models::TIMESERIES_ROLLUP_KEY_LEN];
        let (Some(sbn), Some(ebn)) = (
            models::encode_timeseries_rollup_key(&mut sbody, series, res as u32, from),
            models::encode_timeseries_rollup_key(
                &mut ebody,
                series,
                res as u32,
                to.saturating_add(1),
            ),
        ) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let mut start = [0u8; KEY_MAX];
        let mut end = [0u8; KEY_MAX];
        let (Some(sn), Some(en)) = (
            user_key(&mut start, models::KS_TIMESERIES_ROLLUP, &sbody[..sbn]),
            user_key(&mut end, models::KS_TIMESERIES_ROLLUP, &ebody[..ebn]),
        ) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
        anchor.scan_start_len = sn as u16;
        anchor.scan_end[..en].copy_from_slice(&end[..en]);
        anchor.scan_end_len = en as u16;
        anchor.cursor = 0;
        anchor.ids_len = 0;
        anchor.ids_count = 0;
        anchor.id_mode = ID_ROLLUP;
        anchor.reply_kind = R_IDS;
        let Some(bn) = stage_scan_bounds(anchor) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_SCAN_OUT) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    // `TS.TRIM series from to` — retention: delete every sample in the
    // window `[from, to]`. Pages the same range `TS.RANGE` reads, deleting
    // each page and re-scanning from the start (a delete shifts the store
    // ordinals, so re-scanning from zero is the correct resume — the same
    // shape the relational DELETE uses). Replies the count removed.
    if eq_ci(cmd, b"TS.TRIM") {
        if argv.count != 4 {
            reply_error_str(anchor, b"ERR TS.TRIM series from to");
            return;
        }
        let series = id32(arg(recv, argv, 1)) as u64;
        let (Some(from), Some(to)) = (
            parse_u64_dec(arg(recv, argv, 2)),
            parse_u64_dec(arg(recv, argv, 3)),
        ) else {
            reply_error_str(anchor, b"ERR from/to are decimal u64");
            return;
        };
        if from > to {
            reply_error_str(anchor, b"ERR inverted time window");
            return;
        }
        if !start_ts_trim(anchor, series, from, to) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }

    reply_error_str(anchor, b"ERR unknown command for the model capabilities");
}

/// Set the sample-window scan bounds and send the first TS.TRIM page.
/// `del_count` accumulates across pages so the final reply is the true
/// total. Mirrors `start_ts_scan`'s bounds, but the phase collects keys to
/// delete rather than values to render.
fn start_ts_trim(anchor: &mut AnchorState, series: u64, from: u64, to: u64) -> bool {
    let end_ts = to.saturating_add(1);
    let mut sbody = [0u8; KEY_MAX];
    let mut ebody = [0u8; KEY_MAX];
    let (Some(sbn), Some(ebn)) = (
        models::encode_timeseries_sample_key(&mut sbody, series, 0, from),
        models::encode_timeseries_sample_key(&mut ebody, series, 0, end_ts),
    ) else {
        return false;
    };
    let mut start = [0u8; KEY_MAX];
    let mut end = [0u8; KEY_MAX];
    let (Some(sn), Some(en)) = (
        user_key(&mut start, models::KS_TIMESERIES_SAMPLE, &sbody[..sbn]),
        user_key(&mut end, models::KS_TIMESERIES_SAMPLE, &ebody[..ebn]),
    ) else {
        return false;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    anchor.del_count = 0;
    let Some(bn) = stage_scan_bounds(anchor) else {
        return false;
    };
    kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_TRIM_SCAN)
}

trait ToOwnedBounded {
    fn to_owned_bounded(&self) -> [u8; 64];
}
impl ToOwnedBounded for [u8] {
    fn to_owned_bounded(&self) -> [u8; 64] {
        let mut out = [0u8; 64];
        let n = self.len().min(64);
        out[..n].copy_from_slice(&self[..n]);
        out
    }
}

/// Kick an id-collecting scan (adjacency or postings).
fn start_id_scan(anchor: &mut AnchorState, keyspace: u32, prefix_body: &[u8], mode: u8) -> bool {
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, keyspace, prefix_body) else {
        return false;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        return false;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    anchor.ids_len = 0;
    anchor.ids_count = 0;
    anchor.id_mode = mode;
    anchor.reply_kind = R_IDS;
    let Some(bn) = stage_scan_bounds(anchor) else {
        return false;
    };
    kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_SCAN_OUT)
}

// ── GRAPH.PATH bounded BFS ────────────────────────────────────────────

/// Append `[depth][vlen][vbytes]` to the frontier queue. `false` if full.
fn path_enqueue(anchor: &mut AnchorState, depth: u8, v: &[u8]) -> bool {
    let at = anchor.path_queue_len as usize;
    if v.len() > 255 || at + 2 + v.len() > anchor.path_queue.len() {
        return false;
    }
    anchor.path_queue[at] = depth;
    anchor.path_queue[at + 1] = v.len() as u8;
    anchor.path_queue[at + 2..at + 2 + v.len()].copy_from_slice(v);
    anchor.path_queue_len = (at + 2 + v.len()) as u16;
    true
}

fn path_visited_has(anchor: &AnchorState, v: &[u8]) -> bool {
    let mut p = 0usize;
    while p < anchor.path_visited_len as usize {
        let l = anchor.path_visited[p] as usize;
        p += 1;
        if p + l <= anchor.path_visited.len() && &anchor.path_visited[p..p + l] == v {
            return true;
        }
        p += l;
    }
    false
}

/// Mark `v` seen. Idempotent; `false` only if the set is full.
fn path_visited_add(anchor: &mut AnchorState, v: &[u8]) -> bool {
    if path_visited_has(anchor, v) {
        return true;
    }
    let at = anchor.path_visited_len as usize;
    if v.len() > 255 || at + 1 + v.len() > anchor.path_visited.len() {
        return false;
    }
    anchor.path_visited[at] = v.len() as u8;
    anchor.path_visited[at + 1..at + 1 + v.len()].copy_from_slice(v);
    anchor.path_visited_len = (at + 1 + v.len()) as u16;
    true
}

/// Pop the next frontier vertex and scan its out-edges. An empty queue means
/// `dst` was not reached within `maxhops` → reply -1. A vertex already at the
/// hop limit is not expanded (it is skipped).
fn path_dequeue_and_scan(anchor: &mut AnchorState) {
    loop {
        let head = anchor.path_head as usize;
        if head + 2 > anchor.path_queue_len as usize {
            reply_int(anchor, -1);
            return;
        }
        let depth = anchor.path_queue[head];
        let vlen = anchor.path_queue[head + 1] as usize;
        let voff = head + 2;
        anchor.path_head = (voff + vlen) as u16;
        if depth as u32 >= anchor.path_maxhops as u32 {
            continue;
        }
        let mut vbuf = [0u8; MAX_VERTEX_ID];
        if vlen > MAX_VERTEX_ID {
            reply_error_str(anchor, b"ERR internal");
            return;
        }
        vbuf[..vlen].copy_from_slice(&anchor.path_queue[voff..voff + vlen]);
        let mut body = [0u8; KEY_MAX];
        let Some(bn) = models::encode_graph_vertex_key(&mut body, anchor.path_graph, &vbuf[..vlen])
        else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        anchor.path_depth = depth;
        if !start_path_scan(anchor, &body[..bn]) {
            reply_error_str(anchor, b"ERR internal");
        }
        return;
    }
}

/// Stage a scan of one vertex's out-edge adjacency for the BFS.
fn start_path_scan(anchor: &mut AnchorState, prefix_body: &[u8]) -> bool {
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, models::KS_GRAPH_EDGE_OUT, prefix_body) else {
        return false;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        return false;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    let Some(bn) = stage_scan_bounds(anchor) else {
        return false;
    };
    kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_PATH_SCAN)
}

/// The (offset, len) of the idx-th stashed SEARCH term.
fn search_term(anchor: &AnchorState, idx: usize) -> Option<(usize, usize)> {
    let mut at = 0usize;
    for i in 0..anchor.srch_n as usize {
        let len = *anchor.srch_terms.get(at)? as usize;
        if i == idx {
            return Some((at + 1, len));
        }
        at += 1 + len;
    }
    None
}

/// Scan the idx-th SEARCH term's posting list. Term 0 (phase D_SCAN_OUT)
/// collects into `ids`; a later term (phase D_SRCH_ISECT) collects into
/// `feed` to be intersected.
fn start_search_term_scan(anchor: &mut AnchorState, idx: usize, phase: u8) -> bool {
    let Some((off, len)) = search_term(anchor, idx) else {
        return false;
    };
    let mut lower = [0u8; 64];
    if len > lower.len() {
        return false;
    }
    lower[..len].copy_from_slice(&anchor.srch_terms[off..off + len]);
    let mut body = [0u8; KEY_MAX];
    let Some(bn) =
        models::encode_search_posting_prefix(&mut body, anchor.srch_index, 0, &lower[..len])
    else {
        return false;
    };
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, models::KS_SEARCH_POSTING, &body[..bn]) else {
        return false;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        return false;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    if phase == D_SCAN_OUT {
        anchor.ids_len = 0;
        anchor.ids_count = 0;
        anchor.id_mode = ID_POSTING_DOC;
        anchor.reply_kind = R_IDS;
    } else {
        anchor.feed_len = 0;
        anchor.feed_count = 0;
    }
    let Some(bn2) = stage_scan_bounds(anchor) else {
        return false;
    };
    kv_send(anchor, KV_OP_RANGE_SCAN, bn2, phase)
}

/// Keep in `ids` only the doc ids also present in `feed` (an AND step).
fn intersect_ids_with_feed(anchor: &mut AnchorState) {
    let mut out = [0u8; 2048];
    let mut olen = 0usize;
    let mut ocount = 0u16;
    let mut at = 0usize;
    for _ in 0..anchor.ids_count {
        let l = anchor.ids[at] as usize;
        let rec = &anchor.ids[at + 1..at + 1 + l];
        let mut found = false;
        let mut fat = 0usize;
        for _ in 0..anchor.feed_count {
            let fl = anchor.feed[fat] as usize;
            if fl == l && anchor.feed[fat + 1..fat + 1 + fl] == *rec {
                found = true;
                break;
            }
            fat += 1 + fl;
        }
        if found && olen + 1 + l <= out.len() {
            out[olen] = l as u8;
            out[olen + 1..olen + 1 + l].copy_from_slice(rec);
            olen += 1 + l;
            ocount += 1;
        }
        at += 1 + l;
    }
    anchor.ids[..olen].copy_from_slice(&out[..olen]);
    anchor.ids_len = olen as u16;
    anchor.ids_count = ocount;
}

/// Render `ids` as a RESP array of bulk strings and reply.
fn reply_ids(anchor: &mut AnchorState) {
    let mut out = [0u8; SEND_BUF];
    let mut n = 0usize;
    let _ = enc_array_header(&mut out, &mut n, anchor.ids_count as i64);
    let mut at = 0usize;
    for _ in 0..anchor.ids_count {
        let l = anchor.ids[at] as usize;
        let mut idb = [0u8; 128];
        idb[..l].copy_from_slice(&anchor.ids[at + 1..at + 1 + l]);
        let _ = enc_bulk(&mut out, &mut n, &idb[..l]);
        at += 1 + l;
    }
    let mut f = [0u8; SEND_BUF];
    f[..n].copy_from_slice(&out[..n]);
    reply_raw(anchor, &f[..n]);
}

/// Persist the folded downsample buckets into the rollup keyspace as a single
/// transaction: one record per non-empty bucket,
/// `KS_TIMESERIES_ROLLUP[series][resolution][bucket_ts] = avg`.
fn persist_rollup(anchor: &mut AnchorState) {
    let mut nonempty: u16 = 0;
    for i in 0..anchor.ds_n as usize {
        if anchor.ds_count[i] > 0 {
            nonempty += 1;
        }
    }
    let end_guard = BODY_AT + 3800;
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // cmp_count
    p += 2;
    anchor.env[p..p + 2].copy_from_slice(&nonempty.to_le_bytes()); // then_count
    p += 2;
    for i in 0..anchor.ds_n as usize {
        if anchor.ds_count[i] == 0 {
            continue;
        }
        let bucket_ts = anchor.ds_from + (i as u64) * anchor.ds_bucket;
        let avg = anchor.ds_sum[i] / anchor.ds_count[i] as f32;
        let mut rkbody = [0u8; models::TIMESERIES_ROLLUP_KEY_LEN];
        let Some(rbn) = models::encode_timeseries_rollup_key(
            &mut rkbody,
            anchor.ts_rollup_series,
            anchor.ts_rollup_res,
            bucket_ts,
        ) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let mut rkey = [0u8; KEY_MAX];
        let Some(rkn) = user_key(&mut rkey, models::KS_TIMESERIES_ROLLUP, &rkbody[..rbn]) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let mut vb = [0u8; 32];
        let vn = fmt_f32(avg, &mut vb);
        let Some(np) = txn_put(anchor, p, end_guard, &rkey[..rkn], &vb[..vn]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
    }
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // else_count
    p += 2;
    anchor.del_count = nonempty as u64;
    if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_ROLLUP_WRITE) {
        reply_error_str(anchor, b"ERR internal");
    }
}

// ── Redis lists ───────────────────────────────────────────────────────

fn parse_i64_dec(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let (neg, digits) = if b[0] == b'-' {
        (true, &b[1..])
    } else {
        (false, b)
    };
    if digits.is_empty() {
        return None;
    }
    let mut n: i64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((c - b'0') as i64)?;
    }
    Some(if neg { -n } else { n })
}

/// Stage a bounded scan of one key's prefix under `phase`.
fn stage_single_scan(
    anchor: &mut AnchorState,
    keyspace: u32,
    prefix_body: &[u8],
    phase: u8,
) -> bool {
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, keyspace, prefix_body) else {
        return false;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        return false;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    let Some(bn) = stage_scan_bounds(anchor) else {
        return false;
    };
    kv_send(anchor, KV_OP_RANGE_SCAN, bn, phase)
}

fn list_start_read(anchor: &mut AnchorState) {
    let mut body = [0u8; KEY_MAX];
    let Some(bn) = models::encode_list_meta_key(&mut body, anchor.list_id) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    if !stage_single_scan(anchor, models::KS_LIST_META, &body[..bn], D_LIST_READ) {
        reply_error_str(anchor, b"ERR internal");
    }
}

/// Extract `(head, tail)` from a single-key meta scan page; an empty page is
/// an empty list at `(0, 0)`.
fn parse_list_meta(body: &[u8]) -> (i64, i64) {
    if body.len() < 10 {
        return (0, 0);
    }
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    if count == 0 {
        return (0, 0);
    }
    let at = 10usize;
    let Some(klen) = body
        .get(at..at + 2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
    else {
        return (0, 0);
    };
    let koff = at + 2;
    let voff = koff + klen;
    let Some(vlen) = body
        .get(voff..voff + 4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
    else {
        return (0, 0);
    };
    if vlen < 16 || voff + 4 + 16 > body.len() {
        return (0, 0);
    }
    let v = &body[voff + 4..voff + 4 + 16];
    let head = i64::from_le_bytes(v[0..8].try_into().unwrap_or([0; 8]));
    let tail = i64::from_le_bytes(v[8..16].try_into().unwrap_or([0; 8]));
    (head, tail)
}

/// Append `[cmp_op][key_len u16][key][witness u64]` to the TXN body.
fn txn_cmp(
    anchor: &mut AnchorState,
    mut p: usize,
    end_guard: usize,
    op: u8,
    key: &[u8],
    witness: u64,
) -> Option<usize> {
    if p + 1 + 2 + key.len() + 8 > end_guard {
        return None;
    }
    anchor.env[p] = op;
    p += 1;
    anchor.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    anchor.env[p..p + 8].copy_from_slice(&witness.to_le_bytes());
    p += 8;
    Some(p)
}

/// Build the CAS append: the element is written only if its index slot is
/// absent (witness mod_revision 0), and the head/tail hint is refreshed in the
/// same transaction. A slot collision runs the empty ELSE branch → a retry.
fn do_list_write(anchor: &mut AnchorState) {
    let (index, new_head, new_tail) = if anchor.list_op == 1 {
        let h = anchor.list_head - 1;
        (h, h, anchor.list_tail)
    } else {
        let t = anchor.list_tail;
        (t, anchor.list_head, t + 1)
    };
    let mut ekbody = [0u8; 12];
    let Some(ebn) = models::encode_list_entry_key(&mut ekbody, anchor.list_id, index) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    let mut ekey = [0u8; KEY_MAX];
    let Some(ekn) = user_key(&mut ekey, models::KS_LIST, &ekbody[..ebn]) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    let mut mkbody = [0u8; 4];
    let Some(mbn) = models::encode_list_meta_key(&mut mkbody, anchor.list_id) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    let mut mkey = [0u8; KEY_MAX];
    let Some(mkn) = user_key(&mut mkey, models::KS_LIST_META, &mkbody[..mbn]) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    let mut meta_val = [0u8; 16];
    meta_val[0..8].copy_from_slice(&new_head.to_le_bytes());
    meta_val[8..16].copy_from_slice(&new_tail.to_le_bytes());
    let mut vbuf = [0u8; 512];
    let vlen = anchor.list_val_len as usize;
    vbuf[..vlen].copy_from_slice(&anchor.list_val[..vlen]);

    let end_guard = BODY_AT + 3800;
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes()); // cmp_count
    p += 2;
    let Some(np) = txn_cmp(anchor, p, end_guard, TXN_CMP_MOD_EQUAL, &ekey[..ekn], 0) else {
        reply_error_str(anchor, b"ERR too large");
        return;
    };
    p = np;
    anchor.env[p..p + 2].copy_from_slice(&2u16.to_le_bytes()); // then_count
    p += 2;
    let Some(np) = txn_put(anchor, p, end_guard, &ekey[..ekn], &vbuf[..vlen]) else {
        reply_error_str(anchor, b"ERR too large");
        return;
    };
    p = np;
    let Some(np) = txn_put(anchor, p, end_guard, &mkey[..mkn], &meta_val) else {
        reply_error_str(anchor, b"ERR too large");
        return;
    };
    p = np;
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // else_count
    p += 2;
    anchor.del_count = (new_tail - new_head) as u64;
    if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_LIST_WRITE) {
        reply_error_str(anchor, b"ERR internal");
    }
}

/// Render `LRANGE`/`ZRANGE`: the collected `[sortkey:8][len][payload]` records
/// sorted by their order-preserving key, then sliced to `[start, stop]` (Redis
/// semantics: negatives count from the end). Sorting here (not relying on the
/// store's scan order) keeps the result correct on the slot-ordered memory
/// store as well as the sorted disk store.
fn reply_lrange(anchor: &mut AnchorState) {
    // Index the records.
    let mut offs = [0u16; 256];
    let mut noff = 0usize;
    let mut at = 0usize;
    while noff < offs.len() && at + 9 <= anchor.ids_len as usize {
        offs[noff] = at as u16;
        let l = anchor.ids[at + 8] as usize;
        at += 9 + l;
        noff += 1;
    }
    // Insertion sort by the 8-byte big-endian sort key.
    let mut i = 1;
    while i < noff {
        let mut j = i;
        while j > 0 {
            let a = offs[j - 1] as usize;
            let b = offs[j] as usize;
            if anchor.ids[a..a + 8] > anchor.ids[b..b + 8] {
                offs.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
        i += 1;
    }

    let n = noff as i64;
    let s = if anchor.lrange_start < 0 {
        (n + anchor.lrange_start).max(0)
    } else {
        anchor.lrange_start
    };
    let e = if anchor.lrange_stop < 0 {
        n + anchor.lrange_stop
    } else {
        anchor.lrange_stop.min(n - 1)
    };
    if n == 0 || s > e || s >= n {
        let mut out = [0u8; 16];
        let mut m = 0usize;
        let _ = enc_array_header(&mut out, &mut m, 0);
        let mut f = [0u8; 16];
        f[..m].copy_from_slice(&out[..m]);
        reply_raw(anchor, &f[..m]);
        return;
    }
    let out_count = e - s + 1;
    let mut out = [0u8; SEND_BUF];
    let mut m = 0usize;
    let _ = enc_array_header(&mut out, &mut m, out_count);
    let mut rank = s;
    while rank <= e {
        let off = offs[rank as usize] as usize;
        let l = anchor.ids[off + 8] as usize;
        let mut vb = [0u8; 128];
        let take = l.min(vb.len());
        vb[..take].copy_from_slice(&anchor.ids[off + 9..off + 9 + take]);
        let _ = enc_bulk(&mut out, &mut m, &vb[..take]);
        rank += 1;
    }
    let mut f = [0u8; SEND_BUF];
    f[..m].copy_from_slice(&out[..m]);
    reply_raw(anchor, &f[..m]);
}

// ── Redis sorted sets ─────────────────────────────────────────────────

fn fmt_i64(v: i64, out: &mut [u8]) -> usize {
    if v < 0 && !out.is_empty() {
        out[0] = b'-';
        1 + fmt_u64(v.unsigned_abs(), &mut out[1..])
    } else {
        fmt_u64(v as u64, out)
    }
}

/// The first record's value as an `i64` from a single-key scan page, if any.
fn parse_zmember_score(body: &[u8]) -> Option<i64> {
    if body.len() < 10 {
        return None;
    }
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    if count == 0 {
        return None;
    }
    let at = 10usize;
    let klen = body
        .get(at..at + 2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)?;
    let voff = at + 2 + klen;
    let vlen = body
        .get(voff..voff + 4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)?;
    if vlen < 8 || voff + 4 + 8 > body.len() {
        return None;
    }
    Some(i64::from_le_bytes(
        body[voff + 4..voff + 12].try_into().unwrap_or([0; 8]),
    ))
}

/// Append a `KV_OP_DELETE` of one key as a TXN then-op.
fn txn_delete(
    anchor: &mut AnchorState,
    mut p: usize,
    end_guard: usize,
    key: &[u8],
) -> Option<usize> {
    let del_body = 2 + 2 + key.len(); // key_count + key_len + key
    if p + 3 + del_body > end_guard {
        return None;
    }
    anchor.env[p] = KV_OP_DELETE;
    p += 1;
    anchor.env[p..p + 2].copy_from_slice(&(del_body as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes()); // one key
    p += 2;
    anchor.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    Some(p)
}

/// (Re)index the member by score: put the member→score directory record and
/// the score-index entry, removing a prior score-index entry when the score
/// changed. Reply is the added count (1 for a new member, 0 for a re-score).
fn do_zadd_write(anchor: &mut AnchorState) {
    let member_len = anchor.z_member_len as usize;
    let mut mbuf = [0u8; MAX_VERTEX_ID];
    mbuf[..member_len].copy_from_slice(&anchor.z_member[..member_len]);
    let member = &mbuf[..member_len];

    let mut mkbody = [0u8; KEY_MAX];
    let mut mkey = [0u8; KEY_MAX];
    let mut nskbody = [0u8; KEY_MAX];
    let mut nskey = [0u8; KEY_MAX];
    let (Some(mbn), Some(nsbn)) = (
        models::encode_zmember_key(&mut mkbody, anchor.z_set_id, member),
        models::encode_zscore_key(&mut nskbody, anchor.z_set_id, anchor.z_score, member),
    ) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    let (Some(mkn), Some(nskn)) = (
        user_key(&mut mkey, models::KS_ZMEMBER, &mkbody[..mbn]),
        user_key(&mut nskey, models::KS_ZSCORE, &nskbody[..nsbn]),
    ) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };

    let rescore = anchor.z_old_exists && anchor.z_old_score != anchor.z_score;
    let end_guard = BODY_AT + 3800;
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // cmp_count
    p += 2;
    let then_count: u16 = if rescore { 3 } else { 2 };
    anchor.env[p..p + 2].copy_from_slice(&then_count.to_le_bytes());
    p += 2;
    if rescore {
        let mut oskbody = [0u8; KEY_MAX];
        let mut oskey = [0u8; KEY_MAX];
        let Some(osbn) =
            models::encode_zscore_key(&mut oskbody, anchor.z_set_id, anchor.z_old_score, member)
        else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let Some(oskn) = user_key(&mut oskey, models::KS_ZSCORE, &oskbody[..osbn]) else {
            reply_error_str(anchor, b"ERR internal");
            return;
        };
        let Some(np) = txn_delete(anchor, p, end_guard, &oskey[..oskn]) else {
            reply_error_str(anchor, b"ERR too large");
            return;
        };
        p = np;
    }
    let score_bytes = anchor.z_score.to_le_bytes();
    let Some(np) = txn_put(anchor, p, end_guard, &mkey[..mkn], &score_bytes) else {
        reply_error_str(anchor, b"ERR too large");
        return;
    };
    p = np;
    let Some(np) = txn_put(anchor, p, end_guard, &nskey[..nskn], &[]) else {
        reply_error_str(anchor, b"ERR too large");
        return;
    };
    p = np;
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // else_count
    p += 2;
    anchor.del_count = if anchor.z_old_exists { 0 } else { 1 };
    if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_ZADD_WRITE) {
        reply_error_str(anchor, b"ERR internal");
    }
}

/// Scan one adjacency direction of the stashed DELVERTEX target
/// (`del_index` = graph, `del_entity` = vertex). The vertex key is exactly
/// the `[graph][vertex]` edge prefix, so it bounds the scan.
fn start_vertex_edge_scan(anchor: &mut AnchorState, keyspace: u32, phase: u8) -> bool {
    let g = anchor.del_index;
    let vlen = anchor.del_entity_len as usize;
    let mut v = [0u8; 64];
    v[..vlen].copy_from_slice(&anchor.del_entity[..vlen]);
    let mut body = [0u8; KEY_MAX];
    let Some(bn) = models::encode_graph_vertex_key(&mut body, g, &v[..vlen]) else {
        return false;
    };
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, keyspace, &body[..bn]) else {
        return false;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        return false;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    let Some(bn2) = stage_scan_bounds(anchor) else {
        return false;
    };
    kv_send(anchor, KV_OP_RANGE_SCAN, bn2, phase)
}

/// Build a `KV_OP_DELETE` over a page of a vertex's edges: each edge key
/// plus its symmetric twin at the other endpoint. `is_out` picks the
/// direction (out-edge → delete the in-edge twin, and vice versa).
/// Returns `(body_len, keys)`.
fn stage_vertex_edge_page(
    anchor: &mut AnchorState,
    body: &[u8],
    is_out: bool,
) -> Option<(usize, u16)> {
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut p = del_begin(anchor);
    let mut nkeys = 0u16;
    let mut at = 10usize;
    for _ in 0..count {
        let klen = u16::from_le_bytes([*body.get(at)?, *body.get(at + 1)?]) as usize;
        let koff = at + 2;
        let kend = koff + klen;
        let vlen = u32::from_le_bytes(body.get(kend..kend + 4)?.try_into().ok()?) as usize;
        // The edge key, copied out so `del_put` can mutate `env`.
        let mut kbuf = [0u8; KEY_MAX + 8];
        if klen > kbuf.len() || klen < 4 {
            return None;
        }
        kbuf[..klen].copy_from_slice(&body[koff..kend]);
        let np = del_put(anchor, p, &kbuf[..klen])?;
        p = np;
        nkeys += 1;
        // The symmetric twin, derived from the edge body (key minus the
        // 4-byte keyspace prefix).
        let mut symbody = [0u8; KEY_MAX];
        let sym = if is_out {
            models::edge_in_from_out(&kbuf[4..klen], &mut symbody)
        } else {
            models::edge_out_from_in(&kbuf[4..klen], &mut symbody)
        };
        if let Some(sbn) = sym {
            let ks = if is_out {
                models::KS_GRAPH_EDGE_IN
            } else {
                models::KS_GRAPH_EDGE_OUT
            };
            let mut symkey = [0u8; KEY_MAX + 8];
            if let Some(skn) = user_key(&mut symkey, ks, &symbody[..sbn]) {
                let np = del_put(anchor, p, &symkey[..skn])?;
                p = np;
                nkeys += 1;
            }
        }
        at = kend + 4 + vlen;
    }
    Some((p, nkeys))
}

/// Delete the DELVERTEX target's vertex record, replying 0/1.
fn delete_vertex_record(anchor: &mut AnchorState) {
    let g = anchor.del_index;
    let vlen = anchor.del_entity_len as usize;
    let mut v = [0u8; 64];
    v[..vlen].copy_from_slice(&anchor.del_entity[..vlen]);
    let mut body = [0u8; KEY_MAX];
    let Some(bn) = models::encode_graph_vertex_key(&mut body, g, &v[..vlen]) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    let mut key = [0u8; KEY_MAX + 8];
    let Some(kn) = user_key(&mut key, models::KS_GRAPH_VERTEX, &body[..bn]) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    let mut k = [0u8; KEY_MAX + 8];
    k[..kn].copy_from_slice(&key[..kn]);
    let p = del_begin(anchor);
    let Some(p) = del_put(anchor, p, &k[..kn]) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    if !del_finish(anchor, p, 1, D_DELETE_ENTITY) {
        reply_error_str(anchor, b"ERR internal");
    }
}

/// Stage a range scan over a series' `[from, to]` window (encoded
/// half-open `[from, to+1)` so `to` is inclusive; saturating so a window
/// ending at u64::MAX still reaches the series' end) in `id_mode`.
/// `Some(true)` dispatched, `Some(false)` backpressure, `None` an encode
/// fault the caller should ERR on. Resets both the id and aggregate
/// accumulators so the scan starts clean whichever mode it is.
fn start_ts_scan(
    anchor: &mut AnchorState,
    series: u64,
    from: u64,
    to: u64,
    id_mode: u8,
) -> Option<bool> {
    let end_ts = to.saturating_add(1);
    let mut sbody = [0u8; KEY_MAX];
    let mut ebody = [0u8; KEY_MAX];
    let (Some(sbn), Some(ebn)) = (
        models::encode_timeseries_sample_key(&mut sbody, series, 0, from),
        models::encode_timeseries_sample_key(&mut ebody, series, 0, end_ts),
    ) else {
        return None;
    };
    let mut start = [0u8; KEY_MAX];
    let mut end = [0u8; KEY_MAX];
    let (Some(sn), Some(en)) = (
        user_key(&mut start, models::KS_TIMESERIES_SAMPLE, &sbody[..sbn]),
        user_key(&mut end, models::KS_TIMESERIES_SAMPLE, &ebody[..ebn]),
    ) else {
        return None;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    anchor.ids_len = 0;
    anchor.ids_count = 0;
    anchor.agg_count = 0;
    anchor.agg_sum = 0.0;
    anchor.agg_min = 0.0;
    anchor.agg_max = 0.0;
    anchor.id_mode = id_mode;
    anchor.reply_kind = R_IDS;
    let bn = stage_scan_bounds(anchor)?;
    Some(kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_SCAN_OUT))
}

/// Set up a k-nearest search: parse `index k q…`, precompute `|q|` for
/// cosine, and stage the scan through `D_VSIM`. An EXACT search sweeps
/// the whole index's embeddings; an approximate (`VECTOR.ANN`) search
/// sweeps only the query's LSH bucket of denormalised postings.
fn begin_vector_search(
    anchor: &mut AnchorState,
    recv: &[u8],
    argv: &Argv,
    metric: u8,
    approx: bool,
) {
    if argv.count < 4 {
        reply_error_str(anchor, b"ERR VECTOR index k q...");
        return;
    }
    let index_id = id32(arg(recv, argv, 1));
    let Some(kq) = parse_f32(arg(recv, argv, 2)) else {
        reply_error_str(anchor, b"ERR k must be a number");
        return;
    };
    let k = kq as usize;
    if k == 0 || k > MAX_K {
        reply_error_str(anchor, b"ERR k out of range");
        return;
    }
    let dims = argv.count as usize - 3;
    if dims == 0 || dims > MAX_DIMS {
        reply_error_str(anchor, b"ERR bad dimension count");
        return;
    }
    let mut norm2 = 0.0f32;
    for d in 0..dims {
        let Some(f) = parse_f32(arg(recv, argv, 3 + d)) else {
            reply_error_str(anchor, b"ERR vector components are decimal numbers");
            return;
        };
        anchor.q[d] = f;
        norm2 += f * f;
    }
    anchor.q_dims = dims as u16;
    anchor.k = k as u8;
    anchor.q_metric = metric;
    anchor.q_approx = u8::from(approx);
    anchor.q_norm = if metric == VMETRIC_COS {
        sqrtf(norm2)
    } else {
        0.0
    };
    anchor.top_n = 0;
    anchor.cursor = 0;
    anchor.vfilter = false; // an unfiltered search must not inherit a tag set

    // Scan bounds: the whole index (exact) or the query's one LSH bucket
    // (ANN). The bucket is computed from the query the same way ADD
    // computed each vector's, so a near neighbour is likely in it.
    let mut prefix = [0u8; 8];
    let (ks, plen) = if approx {
        prefix[..4].copy_from_slice(&index_id.to_be_bytes());
        let b = lsh_bucket(&anchor.q[..dims]) as u32;
        prefix[4..8].copy_from_slice(&b.to_be_bytes());
        (models::KS_VECTOR_ANN, 8usize)
    } else {
        prefix[..4].copy_from_slice(&index_id.to_be_bytes());
        (models::KS_VECTOR_EMBEDDING, 4usize)
    };
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, ks, &prefix[..plen]) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    let Some(bn) = stage_scan_bounds(anchor) else {
        reply_error_str(anchor, b"ERR internal");
        return;
    };
    if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_VSIM) {
        reply_error_str(anchor, b"ERR internal");
    }
}

/// One `KV_OP_SCAN_VERSIONS` page over the staged span and window.
fn send_feed_scan(anchor: &mut AnchorState) -> bool {
    let sn = anchor.scan_start_len as usize;
    let en = anchor.scan_end_len as usize;
    let (from, to, cursor) = (anchor.feed_from, anchor.feed_to, anchor.cursor);
    // [start][end][from:u64][to:u64][cursor:u64][limit:u16]
    let need = 2 + sn + 2 + en + 8 + 8 + 8 + 2;
    if BODY_AT + need > anchor.env.len() {
        return false;
    }
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&(sn as u16).to_le_bytes());
    p += 2;
    for i in 0..sn {
        anchor.env[p + i] = anchor.scan_start[i];
    }
    p += sn;
    anchor.env[p..p + 2].copy_from_slice(&(en as u16).to_le_bytes());
    p += 2;
    for i in 0..en {
        anchor.env[p + i] = anchor.scan_end[i];
    }
    p += en;
    anchor.env[p..p + 8].copy_from_slice(&from.to_le_bytes());
    p += 8;
    anchor.env[p..p + 8].copy_from_slice(&to.to_le_bytes());
    p += 8;
    anchor.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    anchor.env[p..p + 2].copy_from_slice(&32u16.to_le_bytes());
    kv_send(anchor, types::KV_OP_SCAN_VERSIONS, need, D_FEED)
}

fn on_kv_response(anchor: &mut AnchorState, result: u8, body: &[u8]) {
    match anchor.d_phase {
        D_WRITE => match result {
            KV_RESULT_TXN if !body.is_empty() && body[0] == 1 => reply_ok(anchor),
            KV_RESULT_OK => reply_ok(anchor),
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        D_HSET => match result {
            KV_RESULT_TXN if !body.is_empty() && body[0] == 1 => {
                reply_int(anchor, anchor.del_count as i64)
            }
            KV_RESULT_OK => reply_int(anchor, anchor.del_count as i64),
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        D_ROLLUP_WRITE => match result {
            KV_RESULT_TXN if !body.is_empty() && body[0] == 1 => {
                reply_int(anchor, anchor.del_count as i64)
            }
            KV_RESULT_OK => reply_int(anchor, anchor.del_count as i64),
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        D_LIST_READ => {
            if result != KV_RESULT_RANGE {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            let (h, t) = parse_list_meta(body);
            anchor.list_head = h;
            anchor.list_tail = t;
            do_list_write(anchor);
        }
        D_LIST_WRITE => {
            if result == KV_RESULT_TXN && !body.is_empty() && body[0] == 1 {
                reply_int(anchor, anchor.del_count as i64);
            } else if result == KV_RESULT_TXN {
                // The ELSE branch ran: a concurrent pusher took the slot.
                // Re-read the hint and retry a bounded number of times.
                if anchor.list_retries < 8 {
                    anchor.list_retries += 1;
                    list_start_read(anchor);
                } else {
                    reply_error_str(anchor, b"ERR list contention");
                }
            } else {
                reply_error_str(anchor, b"ERR store unavailable");
            }
        }
        D_LLEN => {
            if result != KV_RESULT_RANGE {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            let (h, t) = parse_list_meta(body);
            reply_int(anchor, t - h);
        }
        D_ZADD_READ => {
            if result != KV_RESULT_RANGE {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            if let Some(old) = parse_zmember_score(body) {
                anchor.z_old_exists = true;
                anchor.z_old_score = old;
            }
            do_zadd_write(anchor);
        }
        D_ZADD_WRITE => match result {
            KV_RESULT_TXN if !body.is_empty() && body[0] == 1 => {
                reply_int(anchor, anchor.del_count as i64)
            }
            KV_RESULT_OK => reply_int(anchor, anchor.del_count as i64),
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        D_PATH_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            let mut at = 10usize;
            for _ in 0..count {
                let Some(klen) = body
                    .get(at..at + 2)
                    .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let koff = at + 2;
                let voff = koff + klen;
                let Some(vlen) = body
                    .get(voff..voff + 4)
                    .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let vend = voff + 4 + vlen;
                if vend > body.len() || klen < 4 {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                }
                let kbody = &body[koff + 4..koff + klen];
                let mut a = [0u8; MAX_VERTEX_ID];
                let mut nb = [0u8; MAX_VERTEX_ID];
                if let Some((_, _, _, blen)) = models::decode_graph_edge_key(kbody, &mut a, &mut nb)
                {
                    let neighbor = &nb[..blen];
                    if neighbor == &anchor.path_dst[..anchor.path_dst_len as usize] {
                        reply_int(anchor, anchor.path_depth as i64 + 1);
                        return;
                    }
                    if !path_visited_has(anchor, neighbor) {
                        let _ = path_visited_add(anchor, neighbor);
                        let d = anchor.path_depth + 1;
                        if !path_enqueue(anchor, d, neighbor) {
                            reply_error_str(anchor, b"ERR path too large");
                            return;
                        }
                    }
                }
                at = vend;
            }
            if anchor.cursor != 0 {
                let Some(bn) = stage_scan_bounds(anchor) else {
                    reply_error_str(anchor, b"ERR internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_PATH_SCAN) {
                    reply_error_str(anchor, b"ERR internal");
                }
                return;
            }
            path_dequeue_and_scan(anchor);
        }
        D_SCAN_OUT => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            let mut at = 10usize;
            for _ in 0..count {
                let Some(klen) = body
                    .get(at..at + 2)
                    .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let koff = at + 2;
                let voff = koff + klen;
                let Some(vlen) = body
                    .get(voff..voff + 4)
                    .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let vend = voff + 4 + vlen;
                if vend > body.len() || klen < 4 {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                }
                let kbody = &body[koff + 4..koff + klen];
                // Time-series sample: push the timestamp (from the key)
                // then the value (the record) as two records — a flat
                // `[ts, value]…` array. Unlike the id modes it keeps the
                // value, so it is its own arm rather than an id decoder.
                if anchor.id_mode == ID_TS_SAMPLE {
                    let Some((_series, _bucket, ts)) = models::decode_timeseries_sample_key(kbody)
                    else {
                        reply_error_str(anchor, b"ERR key corrupt");
                        return;
                    };
                    let mut tsb = [0u8; 24];
                    let tn = fmt_u64(ts, &mut tsb);
                    let value = &body[voff + 4..vend];
                    let w = anchor.ids_len as usize;
                    if tn > 127 || value.len() > 127 || w + 2 + tn + value.len() > anchor.ids.len()
                    {
                        reply_error_str(anchor, b"ERR result exceeds one batch");
                        return;
                    }
                    anchor.ids[w] = tn as u8;
                    anchor.ids[w + 1..w + 1 + tn].copy_from_slice(&tsb[..tn]);
                    let w2 = w + 1 + tn;
                    anchor.ids[w2] = value.len() as u8;
                    anchor.ids[w2 + 1..w2 + 1 + value.len()].copy_from_slice(value);
                    anchor.ids_len = (w2 + 1 + value.len()) as u16;
                    anchor.ids_count += 2; // ts record + value record
                    at = vend;
                    continue;
                }
                if anchor.id_mode == ID_ROLLUP {
                    // Bucket timestamp (from the key) then the stored value.
                    let Some((_series, _res, bucket)) = models::decode_timeseries_rollup_key(kbody)
                    else {
                        reply_error_str(anchor, b"ERR key corrupt");
                        return;
                    };
                    let mut tsb = [0u8; 24];
                    let tn = fmt_u64(bucket, &mut tsb);
                    let value = &body[voff + 4..vend];
                    let w = anchor.ids_len as usize;
                    if tn > 127 || value.len() > 127 || w + 2 + tn + value.len() > anchor.ids.len()
                    {
                        reply_error_str(anchor, b"ERR result exceeds one batch");
                        return;
                    }
                    anchor.ids[w] = tn as u8;
                    anchor.ids[w + 1..w + 1 + tn].copy_from_slice(&tsb[..tn]);
                    let w2 = w + 1 + tn;
                    anchor.ids[w2] = value.len() as u8;
                    anchor.ids[w2 + 1..w2 + 1 + value.len()].copy_from_slice(value);
                    anchor.ids_len = (w2 + 1 + value.len()) as u16;
                    anchor.ids_count += 2;
                    at = vend;
                    continue;
                }
                if anchor.id_mode == ID_HASH_PAIR {
                    // Field (the key tail after the 4-byte hash id) then value.
                    let field = if kbody.len() > 4 {
                        &kbody[4..]
                    } else {
                        &kbody[0..0]
                    };
                    let value = &body[voff + 4..vend];
                    let w = anchor.ids_len as usize;
                    if field.len() > 127
                        || value.len() > 127
                        || w + 2 + field.len() + value.len() > anchor.ids.len()
                    {
                        reply_error_str(anchor, b"ERR result exceeds one batch");
                        return;
                    }
                    anchor.ids[w] = field.len() as u8;
                    anchor.ids[w + 1..w + 1 + field.len()].copy_from_slice(field);
                    let w2 = w + 1 + field.len();
                    anchor.ids[w2] = value.len() as u8;
                    anchor.ids[w2 + 1..w2 + 1 + value.len()].copy_from_slice(value);
                    anchor.ids_len = (w2 + 1 + value.len()) as u16;
                    anchor.ids_count += 2; // field record + value record
                    at = vend;
                    continue;
                }
                if anchor.id_mode == ID_HASH_GET || anchor.id_mode == ID_ZSCORE {
                    let value = &body[voff + 4..vend];
                    let w = anchor.ids_len as usize;
                    if value.len() > 127 || w + 1 + value.len() > anchor.ids.len() {
                        reply_error_str(anchor, b"ERR result exceeds one batch");
                        return;
                    }
                    anchor.ids[w] = value.len() as u8;
                    anchor.ids[w + 1..w + 1 + value.len()].copy_from_slice(value);
                    anchor.ids_len = (w + 1 + value.len()) as u16;
                    anchor.ids_count += 1;
                    at = vend;
                    continue;
                }
                // LRANGE/ZRANGE: store `[sortkey:8][len][payload]` so the reply
                // can order by index/score in the anchor — the memory store's
                // range scan is slot-ordered, not key-ordered, so sorting here
                // (as the relational executor does for ORDER BY) makes the
                // order correct on every store.
                if anchor.id_mode == ID_LIST_VALUE || anchor.id_mode == ID_ZMEMBER {
                    // The 8-byte order-preserving sort key is the index/score
                    // component that follows the 4-byte id in the user key.
                    let (sortkey, payload): (&[u8], &[u8]) = if anchor.id_mode == ID_LIST_VALUE {
                        (&kbody[4..12], &body[voff + 4..vend])
                    } else {
                        (
                            &kbody[4..12],
                            if kbody.len() > 12 {
                                &kbody[12..]
                            } else {
                                &kbody[0..0]
                            },
                        )
                    };
                    let w = anchor.ids_len as usize;
                    if payload.len() > 127 || w + 9 + payload.len() > anchor.ids.len() {
                        reply_error_str(anchor, b"ERR result exceeds one batch");
                        return;
                    }
                    anchor.ids[w..w + 8].copy_from_slice(sortkey);
                    anchor.ids[w + 8] = payload.len() as u8;
                    anchor.ids[w + 9..w + 9 + payload.len()].copy_from_slice(payload);
                    anchor.ids_len = (w + 9 + payload.len()) as u16;
                    anchor.ids_count += 1;
                    at = vend;
                    continue;
                }
                if anchor.id_mode == ID_TS_AGG {
                    // Fold the numeric value into count/sum/min/max. A
                    // non-numeric sample is skipped, not fatal — the
                    // aggregate is over the numeric samples in the window.
                    let value = &body[voff + 4..vend];
                    if let Some(v) = parse_f32(value) {
                        if anchor.agg_count == 0 {
                            anchor.agg_min = v;
                            anchor.agg_max = v;
                        } else {
                            if v < anchor.agg_min {
                                anchor.agg_min = v;
                            }
                            if v > anchor.agg_max {
                                anchor.agg_max = v;
                            }
                        }
                        anchor.agg_count += 1;
                        anchor.agg_sum += v;
                    }
                    at = vend;
                    continue;
                }
                if anchor.id_mode == ID_TS_DS {
                    // Fold the sample into its time bucket by timestamp.
                    let Some((_series, _bucket, ts)) = models::decode_timeseries_sample_key(kbody)
                    else {
                        reply_error_str(anchor, b"ERR key corrupt");
                        return;
                    };
                    let value = &body[voff + 4..vend];
                    if let Some(v) = parse_f32(value) {
                        // `.max(1)` proves the divisor non-zero to the PIC
                        // link (the command already refuses a zero bucket).
                        let idx =
                            (ts.saturating_sub(anchor.ds_from) / anchor.ds_bucket.max(1)) as usize;
                        let idx = idx.min((anchor.ds_n as usize).saturating_sub(1));
                        anchor.ds_sum[idx] += v;
                        anchor.ds_count[idx] += 1;
                    }
                    at = vend;
                    continue;
                }
                let mut a = [0u8; 128];
                let mut b2 = [0u8; 128];
                let id_bytes: Option<(usize, [u8; 128])> = match anchor.id_mode {
                    ID_EDGE_SECOND => models::decode_graph_edge_key(kbody, &mut a, &mut b2)
                        .map(|(_, _, _, blen)| (blen, b2)),
                    _ => models::decode_search_posting_key(kbody, &mut a, &mut b2)
                        .map(|(_, _, _, dlen)| (dlen, b2)),
                };
                let Some((ilen, ibuf)) = id_bytes else {
                    reply_error_str(anchor, b"ERR key corrupt");
                    return;
                };
                let w = anchor.ids_len as usize;
                if ilen > 127 || w + 1 + ilen > anchor.ids.len() {
                    reply_error_str(anchor, b"ERR result exceeds one batch");
                    return;
                }
                anchor.ids[w] = ilen as u8;
                anchor.ids[w + 1..w + 1 + ilen].copy_from_slice(&ibuf[..ilen]);
                anchor.ids_len = (w + 1 + ilen) as u16;
                anchor.ids_count += 1;
                at = vend;
            }
            if anchor.cursor != 0 {
                let Some(bn) = stage_scan_bounds(anchor) else {
                    reply_error_str(anchor, b"ERR internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_SCAN_OUT) {
                    reply_error_str(anchor, b"ERR internal");
                }
            } else {
                if anchor.id_mode == ID_TS_DS && anchor.ts_rollup_persist {
                    persist_rollup(anchor);
                    return;
                }
                if anchor.id_mode == ID_TS_DS {
                    // Render the non-empty buckets as a flat
                    // `[bucket_start_ts, avg, …]` array, oldest first.
                    let mut out = [0u8; SEND_BUF];
                    let mut n = 0usize;
                    let mut nonempty = 0i64;
                    for i in 0..anchor.ds_n as usize {
                        if anchor.ds_count[i] > 0 {
                            nonempty += 1;
                        }
                    }
                    let _ = enc_array_header(&mut out, &mut n, nonempty * 2);
                    for i in 0..anchor.ds_n as usize {
                        if anchor.ds_count[i] == 0 {
                            continue;
                        }
                        let ts = anchor.ds_from + (i as u64) * anchor.ds_bucket;
                        let mut tb = [0u8; 24];
                        let tn = fmt_u64(ts, &mut tb);
                        let _ = enc_bulk(&mut out, &mut n, &tb[..tn]);
                        let avg = anchor.ds_sum[i] / anchor.ds_count[i] as f32;
                        let mut fb = [0u8; 32];
                        let fl = fmt_f32(avg, &mut fb);
                        let _ = enc_bulk(&mut out, &mut n, &fb[..fl]);
                    }
                    let mut f = [0u8; SEND_BUF];
                    f[..n].copy_from_slice(&out[..n]);
                    reply_raw(anchor, &f[..n]);
                    return;
                }
                if anchor.id_mode == ID_TS_AGG {
                    // Render [count, sum, min, max, avg]. Count is exact
                    // (integer); the rest are the numeric aggregate. An
                    // empty window is count 0 and zeros — an honest "no
                    // samples", not an error.
                    let mut out = [0u8; SEND_BUF];
                    let mut n = 0usize;
                    let _ = enc_array_header(&mut out, &mut n, 5);
                    let mut cb = [0u8; 24];
                    let cn = fmt_u64(anchor.agg_count as u64, &mut cb);
                    let _ = enc_bulk(&mut out, &mut n, &cb[..cn]);
                    let avg = if anchor.agg_count > 0 {
                        anchor.agg_sum / anchor.agg_count as f32
                    } else {
                        0.0
                    };
                    for v in [anchor.agg_sum, anchor.agg_min, anchor.agg_max, avg] {
                        let mut fb = [0u8; 32];
                        let fl = fmt_f32(v, &mut fb);
                        let _ = enc_bulk(&mut out, &mut n, &fb[..fl]);
                    }
                    let mut f = [0u8; SEND_BUF];
                    f[..n].copy_from_slice(&out[..n]);
                    reply_raw(anchor, &f[..n]);
                    return;
                }
                // LRANGE / ZRANGE: the collected elements/members, sliced to
                // [start, stop] with Redis semantics (negatives from the end).
                if anchor.id_mode == ID_LIST_VALUE || anchor.id_mode == ID_ZMEMBER {
                    reply_lrange(anchor);
                    return;
                }
                // ZSCORE: one member's integer score as a bulk string, or nil.
                if anchor.id_mode == ID_ZSCORE {
                    if anchor.ids_count >= 1 && anchor.ids[0] as usize >= 8 {
                        let score =
                            i64::from_le_bytes(anchor.ids[1..9].try_into().unwrap_or([0; 8]));
                        let mut sb = [0u8; 24];
                        let sn = fmt_i64(score, &mut sb);
                        let mut out = [0u8; 40];
                        let mut n = 0usize;
                        let _ = enc_bulk(&mut out, &mut n, &sb[..sn]);
                        let mut f = [0u8; 40];
                        f[..n].copy_from_slice(&out[..n]);
                        reply_raw(anchor, &f[..n]);
                    } else {
                        let mut out = [0u8; 16];
                        let mut n = 0usize;
                        let _ = enc_null_bulk(&mut out, &mut n);
                        let mut f = [0u8; 16];
                        f[..n].copy_from_slice(&out[..n]);
                        reply_raw(anchor, &f[..n]);
                    }
                    return;
                }
                // HGET: one field's value as a single bulk, or nil if absent.
                if anchor.id_mode == ID_HASH_GET {
                    if anchor.ids_count >= 1 {
                        let l = anchor.ids[0] as usize;
                        let mut vb = [0u8; 128];
                        let take = l.min(vb.len());
                        vb[..take].copy_from_slice(&anchor.ids[1..1 + take]);
                        let mut out = [0u8; 160];
                        let mut n = 0usize;
                        let _ = enc_bulk(&mut out, &mut n, &vb[..take]);
                        let mut f = [0u8; 160];
                        f[..n].copy_from_slice(&out[..n]);
                        reply_raw(anchor, &f[..n]);
                    } else {
                        let mut out = [0u8; 16];
                        let mut n = 0usize;
                        let _ = enc_null_bulk(&mut out, &mut n);
                        let mut f = [0u8; 16];
                        f[..n].copy_from_slice(&out[..n]);
                        reply_raw(anchor, &f[..n]);
                    }
                    return;
                }
                // SEARCH multi-term AND: term 0 is now in `ids`; scan each
                // remaining term and intersect it in before replying.
                if anchor.srch_multi && (anchor.srch_at as usize + 1) < anchor.srch_n as usize {
                    anchor.srch_at += 1;
                    let next = anchor.srch_at as usize;
                    if !start_search_term_scan(anchor, next, D_SRCH_ISECT) {
                        reply_error_str(anchor, b"ERR internal");
                    }
                    return;
                }
                reply_ids(anchor);
            }
        }
        D_FEED => {
            if result == types::KV_RESULT_COMPACTED {
                // §15: the window's lower bound has been reclaimed.
                // Serving the surviving tail would be a gap the
                // consumer cannot see, so the feed fails LOUDLY.
                reply_error_str(
                    anchor,
                    b"COMPACTED feed window reclaimed; resubscribe from a live revision",
                );
                return;
            }
            if result != types::KV_RESULT_VERSIONS || body.len() < 10 {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            // One KV page per client call: render as many
            // entries as the reply budget holds, DIRECTLY from the page
            // — no staging buffer, so no window-size wedge and no value
            // truncation. Entries past the budget are simply re-scanned
            // on the next call: the issued cursor names the last
            // rendered revision, and revision resume is exact.
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;

            // Pass 1: how many entries fit the reply budget, and what
            // the cursor position after the last one is.
            // Per-entry RESP cost: *5 header (4) + rev bulk (<=34) +
            // ts bulk (<=34) + kind bulk (<=12) + key bulk (klen+40) +
            // value bulk (vlen+40) — over-estimated slack, never
            // under. Fixed reply overhead: outer *3 + cursor hex bulk
            // (~110) + frontier bulk (~34).
            const REPLY_OVERHEAD: usize = 160;
            let mut fit = 0usize;
            let mut budget = SEND_BUF - REPLY_OVERHEAD;
            let mut last_rev = anchor.feed_from;
            let mut last_ts = anchor.feed_resume_ts;
            let mut at = 10usize;
            for _ in 0..count {
                if at + 23 > body.len() {
                    break;
                }
                let rev = u64::from_le_bytes(body[at..at + 8].try_into().unwrap_or([0; 8]));
                let ts = u64::from_le_bytes(body[at + 8..at + 16].try_into().unwrap_or([0; 8]));
                let klen = u16::from_le_bytes([body[at + 17], body[at + 18]]) as usize;
                let voff = at + 19 + klen;
                if voff + 4 > body.len() {
                    break;
                }
                let vlen =
                    u32::from_le_bytes(body[voff..voff + 4].try_into().unwrap_or([0; 4])) as usize;
                if voff + 4 + vlen > body.len() {
                    break;
                }
                let cost = 4 + 34 + 34 + 12 + klen + 40 + vlen + 40;
                if cost > budget {
                    break;
                }
                budget -= cost;
                fit += 1;
                last_rev = rev;
                last_ts = ts;
                at = voff + 4 + vlen;
            }

            // The resume token: a FeedCursor at the last rendered
            // position. Range identity is the single implicit range of
            // this composition until multi-range feeds land (§5.4
            // refuses split/merge of a fed range).
            let mut range_id = [0u8; 16];
            range_id[15] = 1;
            let cur = db_ops::FeedCursor {
                database_id: 0,
                partition_map_id: 0,
                range_id,
                range_generation: 1,
                revision: last_rev,
                timestamp: last_ts,
                format_version: db_ops::FEED_EVENT_FORMAT_VERSION,
            };
            let mut cw = [0u8; db_ops::FeedCursor::WIRE_LEN];
            let Some(cwn) = cur.encode(&mut cw) else {
                reply_error_str(anchor, b"ERR internal");
                return;
            };
            let mut chex = [0u8; db_ops::FeedCursor::WIRE_LEN * 2];
            let Some(chn) = hex_core::hex_encode(&cw[..cwn], &mut chex) else {
                reply_error_str(anchor, b"ERR internal");
                return;
            };

            // Pass 2: render.
            let mut out = [0u8; SEND_BUF];
            let mut n = 0usize;
            let _ = enc_array_header(&mut out, &mut n, 3);
            let _ = enc_bulk(&mut out, &mut n, &chex[..chn]);
            let mut fb = [0u8; 24];
            let fbn = fmt_u64(anchor.last_frontier, &mut fb);
            let _ = enc_bulk(&mut out, &mut n, &fb[..fbn]);
            let _ = enc_array_header(&mut out, &mut n, fit as i64);
            let mut at = 10usize;
            for _ in 0..fit {
                let rev = u64::from_le_bytes(body[at..at + 8].try_into().unwrap_or([0; 8]));
                let ts = u64::from_le_bytes(body[at + 8..at + 16].try_into().unwrap_or([0; 8]));
                let kind = body[at + 16];
                let klen = u16::from_le_bytes([body[at + 17], body[at + 18]]) as usize;
                let koff = at + 19;
                let voff = koff + klen;
                let vlen =
                    u32::from_le_bytes(body[voff..voff + 4].try_into().unwrap_or([0; 4])) as usize;
                let _ = enc_array_header(&mut out, &mut n, 5);
                let mut rb = [0u8; 24];
                let rn = fmt_u64(rev, &mut rb);
                let _ = enc_bulk(&mut out, &mut n, &rb[..rn]);
                let mut tb = [0u8; 24];
                let tn = fmt_u64(ts, &mut tb);
                let _ = enc_bulk(&mut out, &mut n, &tb[..tn]);
                // Kind is spelled, not numbered: a delete that reads
                // as an empty put is the §15 failure this avoids.
                let kname: &[u8] = if kind == types::VERSION_KIND_DELETE {
                    b"delete"
                } else {
                    b"put"
                };
                let _ = enc_bulk(&mut out, &mut n, kname);
                let _ = enc_bulk(&mut out, &mut n, &body[koff..koff + klen]);
                let _ = enc_bulk(&mut out, &mut n, &body[voff + 4..voff + 4 + vlen]);
                at = voff + 4 + vlen;
            }
            let mut f = [0u8; SEND_BUF];
            f[..n].copy_from_slice(&out[..n]);
            reply_raw(anchor, &f[..n]);
        }
        D_VSIM => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            let dims = anchor.q_dims as usize;
            let mut at = 10usize;
            for _ in 0..count {
                let Some(klen) = body
                    .get(at..at + 2)
                    .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let koff = at + 2;
                let voff = koff + klen;
                let Some(vlen) = body
                    .get(voff..voff + 4)
                    .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let vend = voff + 4 + vlen;
                if vend > body.len() || klen < 4 {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                }
                // Entity id from the key; distance from the value.
                // Entity from the key — the exact scan reads embedding
                // keys, the ANN scan reads bucket-posting keys.
                let kbody = &body[koff + 4..koff + klen];
                let mut ent = [0u8; 128];
                let decoded = if anchor.q_approx == 1 {
                    models::decode_vector_ann_key(kbody, &mut ent).map(|(_, _, e)| e)
                } else {
                    models::decode_vector_embedding_key(kbody, &mut ent).map(|(_, e)| e)
                };
                let Some(elen) = decoded else {
                    reply_error_str(anchor, b"ERR key corrupt");
                    return;
                };
                let stored_dims = vlen / 4;
                let cmp = dims.min(stored_dims);
                let dist = if anchor.q_metric == VMETRIC_COS {
                    // 1 − cosine similarity: smaller is nearer. dot(q,v)
                    // and |v| over the shared components.
                    let mut dot = 0.0f32;
                    let mut vn2 = 0.0f32;
                    for d in 0..cmp {
                        let f = f32::from_le_bytes([
                            body[voff + 4 + d * 4],
                            body[voff + 4 + d * 4 + 1],
                            body[voff + 4 + d * 4 + 2],
                            body[voff + 4 + d * 4 + 3],
                        ]);
                        dot += f * anchor.q[d];
                        vn2 += f * f;
                    }
                    let vnorm = sqrtf(vn2);
                    if stored_dims != dims || anchor.q_norm == 0.0 || vnorm == 0.0 {
                        2.0 // maximally far — undefined cosine, not a guess
                    } else {
                        1.0 - dot / (anchor.q_norm * vnorm)
                    }
                } else {
                    let mut s = 0.0f32;
                    for d in 0..cmp {
                        let f = f32::from_le_bytes([
                            body[voff + 4 + d * 4],
                            body[voff + 4 + d * 4 + 1],
                            body[voff + 4 + d * 4 + 2],
                            body[voff + 4 + d * 4 + 3],
                        ]);
                        let diff = f - anchor.q[d];
                        s += diff * diff;
                    }
                    // Dimension mismatch counts missing components against
                    // the distance rather than silently truncating.
                    if stored_dims != dims {
                        s += 1.0e9;
                    }
                    s
                };
                let el = elen.min(64);
                // VECTOR.SIMWHERE: keep only entities carrying the tag.
                if !anchor.vfilter || entity_in_feed(anchor, &ent[..el]) {
                    insert_topk(anchor, &ent[..el], dist);
                }
                at = vend;
            }
            if anchor.cursor != 0 {
                let Some(bn) = stage_scan_bounds(anchor) else {
                    reply_error_str(anchor, b"ERR internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_VSIM) {
                    reply_error_str(anchor, b"ERR internal");
                }
            } else {
                // Render [label, entity, dist, …] nearest first. The
                // label is §14.21's honesty on the wire: a full-index
                // scan is "exact"; an LSH-bucket scan is "approximate"
                // and never wears the exact label.
                let mut out = [0u8; SEND_BUF];
                let mut n = 0usize;
                let _ = enc_array_header(&mut out, &mut n, 1 + 2 * anchor.top_n as i64);
                let label: &[u8] = if anchor.q_approx == 1 {
                    b"approximate"
                } else {
                    b"exact"
                };
                let _ = enc_bulk(&mut out, &mut n, label);
                for i in 0..anchor.top_n as usize {
                    let l = anchor.top_len[i] as usize;
                    let mut e = [0u8; 64];
                    e[..l].copy_from_slice(&anchor.top_entity[i][..l]);
                    let _ = enc_bulk(&mut out, &mut n, &e[..l]);
                    let mut fb = [0u8; 32];
                    let fl = fmt_f32(anchor.top_dist[i], &mut fb);
                    let _ = enc_bulk(&mut out, &mut n, &fb[..fl]);
                }
                let mut f = [0u8; SEND_BUF];
                f[..n].copy_from_slice(&out[..n]);
                reply_raw(anchor, &f[..n]);
            }
        }
        D_DELETE => match result {
            // A delete over N keys reports how many actually existed. Redis
            // DEL/TRIM semantics: reply that integer.
            KV_RESULT_INTEGER if body.len() >= 8 => reply_int(
                anchor,
                u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8])) as i64,
            ),
            KV_RESULT_INTEGER | KV_RESULT_OK => reply_int(anchor, 0),
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        D_DELETE_ENTITY => match result {
            // One logical entity (an edge, a vector) spans several keys.
            // Report 1 if any of them existed, 0 if none — the entity count,
            // not the key count, is what the caller asked to remove.
            KV_RESULT_INTEGER if body.len() >= 8 => {
                let n = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
                reply_int(anchor, i64::from(n > 0));
            }
            KV_RESULT_OK => reply_int(anchor, 1),
            KV_RESULT_INTEGER => reply_int(anchor, 0),
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        D_DV_OUT_SCAN | D_DV_IN_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            let is_out = anchor.d_phase == D_DV_OUT_SCAN;
            let count = u16::from_le_bytes([body[8], body[9]]);
            if count == 0 {
                // This direction is exhausted. Out-edges done → sweep
                // in-edges; in-edges done → remove the vertex record.
                if is_out {
                    if !start_vertex_edge_scan(anchor, models::KS_GRAPH_EDGE_IN, D_DV_IN_SCAN) {
                        reply_error_str(anchor, b"ERR internal");
                    }
                } else {
                    delete_vertex_record(anchor);
                }
                return;
            }
            let Some((bn, keys)) = stage_vertex_edge_page(anchor, body, is_out) else {
                reply_error_str(anchor, b"ERR page corrupt");
                return;
            };
            let next = if is_out { D_DV_OUT_DEL } else { D_DV_IN_DEL };
            if keys == 0 || !del_finish(anchor, bn, keys, next) {
                reply_error_str(anchor, b"ERR internal");
            }
        }
        D_SRCH_ISECT => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            let mut at = 10usize;
            for _ in 0..count {
                let Some(klen) = body
                    .get(at..at + 2)
                    .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let koff = at + 2;
                let voff = koff + klen;
                let Some(vlen) = body
                    .get(voff..voff + 4)
                    .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let vend = voff + 4 + vlen;
                if vend > body.len() || klen < 4 {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                }
                let kbody = &body[koff + 4..koff + klen];
                let mut a = [0u8; 128];
                let mut d = [0u8; 128];
                let Some((_, _, _, dlen)) =
                    models::decode_search_posting_key(kbody, &mut a, &mut d)
                else {
                    reply_error_str(anchor, b"ERR key corrupt");
                    return;
                };
                let w = anchor.feed_len as usize;
                if dlen > 127 || w + 1 + dlen > anchor.feed.len() {
                    reply_error_str(anchor, b"ERR result exceeds one batch");
                    return;
                }
                anchor.feed[w] = dlen as u8;
                anchor.feed[w + 1..w + 1 + dlen].copy_from_slice(&d[..dlen]);
                anchor.feed_len = (w + 1 + dlen) as u16;
                anchor.feed_count += 1;
                at = vend;
            }
            if anchor.cursor != 0 {
                // More pages of this term.
                let Some(bn) = stage_scan_bounds(anchor) else {
                    reply_error_str(anchor, b"ERR internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_SRCH_ISECT) {
                    reply_error_str(anchor, b"ERR internal");
                }
                return;
            }
            // This term is fully collected: AND it into the running set.
            intersect_ids_with_feed(anchor);
            if (anchor.srch_at as usize + 1) < anchor.srch_n as usize {
                anchor.srch_at += 1;
                let next = anchor.srch_at as usize;
                if !start_search_term_scan(anchor, next, D_SRCH_ISECT) {
                    reply_error_str(anchor, b"ERR internal");
                }
            } else {
                reply_ids(anchor);
            }
        }
        D_VMETA_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            let mut at = 10usize;
            for _ in 0..count {
                let Some(klen) = body
                    .get(at..at + 2)
                    .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let koff = at + 2;
                let voff = koff + klen;
                let Some(vlen) = body
                    .get(voff..voff + 4)
                    .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let vend = voff + 4 + vlen;
                if vend > body.len() || klen < 4 {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                }
                let mut ent = [0u8; 128];
                let Some(elen) =
                    models::decode_vector_meta_key(&body[koff + 4..koff + klen], &mut ent)
                else {
                    reply_error_str(anchor, b"ERR key corrupt");
                    return;
                };
                let w = anchor.feed_len as usize;
                if elen > 127 || w + 1 + elen > anchor.feed.len() {
                    reply_error_str(anchor, b"ERR too many tagged entities");
                    return;
                }
                anchor.feed[w] = elen as u8;
                anchor.feed[w + 1..w + 1 + elen].copy_from_slice(&ent[..elen]);
                anchor.feed_len = (w + 1 + elen) as u16;
                anchor.feed_count += 1;
                at = vend;
            }
            if anchor.cursor != 0 {
                let Some(bn) = stage_scan_bounds(anchor) else {
                    reply_error_str(anchor, b"ERR internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_VMETA_SCAN) {
                    reply_error_str(anchor, b"ERR internal");
                }
                return;
            }
            // The tag set is complete: run the exact search filtered by it.
            if !send_exact_embedding_scan(anchor) {
                reply_error_str(anchor, b"ERR internal");
            }
        }
        D_DV_OUT_DEL => match result {
            // A page of out-edges (and their twins) removed; re-scan from
            // zero — the delete shifted the store ordinals.
            KV_RESULT_OK | KV_RESULT_INTEGER => {
                if !start_vertex_edge_scan(anchor, models::KS_GRAPH_EDGE_OUT, D_DV_OUT_SCAN) {
                    reply_error_str(anchor, b"ERR internal");
                }
            }
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        D_DV_IN_DEL => match result {
            KV_RESULT_OK | KV_RESULT_INTEGER => {
                if !start_vertex_edge_scan(anchor, models::KS_GRAPH_EDGE_IN, D_DV_IN_SCAN) {
                    reply_error_str(anchor, b"ERR internal");
                }
            }
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        D_VGET => {
            // The embedding value is the raw f32 vector (LE). Decode it back
            // to components and reply an array; an unknown entity is a nil
            // array, distinct from a present zero-length vector.
            match result {
                KV_RESULT_NOT_FOUND => {
                    let mut out = [0u8; 16];
                    let mut n = 0usize;
                    let _ = enc_null_bulk(&mut out, &mut n);
                    let mut f = [0u8; 16];
                    f[..n].copy_from_slice(&out[..n]);
                    reply_raw(anchor, &f[..n]);
                }
                KV_RESULT_OK => {
                    let dims = body.len() / 4;
                    let mut out = [0u8; SEND_BUF];
                    let mut n = 0usize;
                    let _ = enc_array_header(&mut out, &mut n, dims as i64);
                    for d in 0..dims {
                        let f = f32::from_le_bytes([
                            body[d * 4],
                            body[d * 4 + 1],
                            body[d * 4 + 2],
                            body[d * 4 + 3],
                        ]);
                        let mut fb = [0u8; 32];
                        let fl = fmt_f32(f, &mut fb);
                        let _ = enc_bulk(&mut out, &mut n, &fb[..fl]);
                    }
                    let mut fbuf = [0u8; SEND_BUF];
                    fbuf[..n].copy_from_slice(&out[..n]);
                    reply_raw(anchor, &fbuf[..n]);
                }
                _ => reply_error_str(anchor, b"ERR store unavailable"),
            }
        }
        D_VDEL_READ => {
            // The embedding read is back. NOT_FOUND → nothing to delete,
            // reply 0. Otherwise recompute the SAME LSH bucket the write
            // used from the stored vector, then delete embedding + posting.
            match result {
                KV_RESULT_NOT_FOUND => reply_int(anchor, 0),
                KV_RESULT_OK => {
                    let dims = (body.len() / 4).min(MAX_DIMS);
                    let mut qv = [0.0f32; MAX_DIMS];
                    for d in 0..dims {
                        qv[d] = f32::from_le_bytes([
                            body[d * 4],
                            body[d * 4 + 1],
                            body[d * 4 + 2],
                            body[d * 4 + 3],
                        ]);
                    }
                    let bucket = lsh_bucket(&qv[..dims]) as u32;
                    let index_id = anchor.del_index;
                    let elen = anchor.del_entity_len as usize;
                    let mut ent = [0u8; 64];
                    ent[..elen].copy_from_slice(&anchor.del_entity[..elen]);
                    let mut ebody = [0u8; KEY_MAX];
                    let mut abody = [0u8; KEY_MAX];
                    let (Some(ebn), Some(abn)) = (
                        models::encode_vector_embedding_key(&mut ebody, index_id, &ent[..elen]),
                        models::encode_vector_ann_key(&mut abody, index_id, bucket, &ent[..elen]),
                    ) else {
                        reply_error_str(anchor, b"ERR internal");
                        return;
                    };
                    let mut ekey = [0u8; KEY_MAX + 8];
                    let mut akey = [0u8; KEY_MAX + 8];
                    let (Some(ekn), Some(akn)) = (
                        user_key(&mut ekey, models::KS_VECTOR_EMBEDDING, &ebody[..ebn]),
                        user_key(&mut akey, models::KS_VECTOR_ANN, &abody[..abn]),
                    ) else {
                        reply_error_str(anchor, b"ERR internal");
                        return;
                    };
                    let mut ka = [0u8; KEY_MAX + 8];
                    ka[..ekn].copy_from_slice(&ekey[..ekn]);
                    let mut kb = [0u8; KEY_MAX + 8];
                    kb[..akn].copy_from_slice(&akey[..akn]);
                    let p = del_begin(anchor);
                    let Some(p) = del_put(anchor, p, &ka[..ekn]) else {
                        reply_error_str(anchor, b"ERR too large");
                        return;
                    };
                    let Some(p) = del_put(anchor, p, &kb[..akn]) else {
                        reply_error_str(anchor, b"ERR too large");
                        return;
                    };
                    // Report 1 (the entity existed), not the key count.
                    if !del_finish(anchor, p, 2, D_DELETE_ENTITY) {
                        reply_error_str(anchor, b"ERR internal");
                    }
                }
                _ => reply_error_str(anchor, b"ERR store unavailable"),
            }
        }
        D_TRIM_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error_str(anchor, b"ERR store unavailable");
                return;
            }
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            if count == 0 {
                reply_int(anchor, anchor.del_count as i64);
                return;
            }
            // Build a delete of every sample key in this page, then re-scan.
            let mut p = del_begin(anchor);
            let mut at = 10usize;
            let mut staged = 0u16;
            for _ in 0..count {
                let Some(klen) = body
                    .get(at..at + 2)
                    .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let koff = at + 2;
                let voff = koff + klen;
                let Some(vlen) = body
                    .get(voff..voff + 4)
                    .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
                else {
                    reply_error_str(anchor, b"ERR page corrupt");
                    return;
                };
                let mut kbuf = [0u8; KEY_MAX + 8];
                if klen > kbuf.len() {
                    reply_error_str(anchor, b"ERR key too long");
                    return;
                }
                kbuf[..klen].copy_from_slice(&body[koff..koff + klen]);
                let Some(np) = del_put(anchor, p, &kbuf[..klen]) else {
                    reply_error_str(anchor, b"ERR page too large");
                    return;
                };
                p = np;
                staged += 1;
                at = voff + 4 + vlen;
            }
            anchor.del_count += staged as u64;
            if !del_finish(anchor, p, staged, D_TRIM_DEL) {
                reply_error_str(anchor, b"ERR internal");
            }
        }
        D_TRIM_DEL => match result {
            // A page was removed; re-scan the window from the start. A
            // delete shifts store ordinals, so cursor 0 is the resume.
            KV_RESULT_OK | KV_RESULT_INTEGER => {
                anchor.cursor = 0;
                let Some(bn) = stage_scan_bounds(anchor) else {
                    reply_error_str(anchor, b"ERR internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_TRIM_SCAN) {
                    reply_error_str(anchor, b"ERR internal");
                }
            }
            _ => reply_error_str(anchor, b"ERR store unavailable"),
        },
        _ => {}
    }
}

/// Insert into the bounded nearest-k accumulator (ascending dist²).
/// f32 square root by Newton–Raphson — the PIC link has no `sqrt`
/// intrinsic, and cosine needs vector norms. `y ← (y + x/y)/2` converges
/// quadratically; the values here (sums of ≤64 squared components) need
/// only a handful of iterations, and 20 is comfortably past convergence.
// `!(x > 0.0)` is deliberate: it routes 0, negatives, AND NaN to the
// zero result in one test (NaN fails every ordered comparison), which a
// positive `<=` cannot express.
#[allow(
    clippy::neg_cmp_op_on_partial_ord,
    reason = "`!(x > 0.0)` routes 0, negatives, AND NaN to the zero result in one test; a positive comparison cannot express the NaN case"
)]
fn sqrtf(x: f32) -> f32 {
    if !(x > 0.0) {
        return 0.0; // 0, negative, or NaN
    }
    let mut y = x;
    let mut i = 0;
    while i < 20 {
        y = 0.5 * (y + x / y);
        i += 1;
    }
    y
}

/// The `i`-th LSH hyperplane's `d`-th component — a fixed pseudo-random
/// value in roughly `[-1, 1)`, derived by hashing `(i, d)` so the whole
/// plane set is stable without a 16×64 constant table. Stability is all
/// that matters: the same planes must classify a query and the vectors,
/// and every graph must agree, so the hash IS the shared seed.
fn lsh_component(i: usize, d: usize) -> f32 {
    // SplitMix64-style avalanche of a combined index.
    let mut z = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ (d as u64).wrapping_add(0xD1B5_4A32_D192_ED03);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    // Top 24 bits → [0,1) → [-1,1).
    let u = (z >> 40) as f32 / (1u64 << 24) as f32;
    2.0 * u - 1.0
}

/// The LSH bucket of a vector: bit `i` is the side of hyperplane `i` the
/// vector falls on (`dot ≥ 0`). Vectors in one bucket agree on every
/// plane, so they are angularly close — the basis of the approximate
/// `VECTOR.ANN` scan.
fn lsh_bucket(v: &[f32]) -> u16 {
    let mut bucket: u16 = 0;
    let mut i = 0;
    while i < LSH_BITS {
        let mut dot = 0.0f32;
        let mut d = 0;
        while d < v.len() {
            dot += v[d] * lsh_component(i, d);
            d += 1;
        }
        if dot >= 0.0 {
            bucket |= 1 << i;
        }
        i += 1;
    }
    bucket
}

/// Is `entity` in the tag membership set collected in `feed` (as
/// `[len:u8][bytes]` records)?
fn entity_in_feed(anchor: &AnchorState, entity: &[u8]) -> bool {
    let mut at = 0usize;
    for _ in 0..anchor.feed_count {
        let l = anchor.feed[at] as usize;
        if l == entity.len() && anchor.feed[at + 1..at + 1 + l] == *entity {
            return true;
        }
        at += 1 + l;
    }
    false
}

/// Send the exact embedding scan (whole index) for VECTOR.SIMWHERE, after
/// the tag set is collected. `del_index` carries the index id.
fn send_exact_embedding_scan(anchor: &mut AnchorState) -> bool {
    let mut prefix = [0u8; 4];
    prefix.copy_from_slice(&anchor.del_index.to_be_bytes());
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, models::KS_VECTOR_EMBEDDING, &prefix) else {
        return false;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        return false;
    };
    anchor.scan_start[..sn].copy_from_slice(&start[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&end[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    let Some(bn) = stage_scan_bounds(anchor) else {
        return false;
    };
    kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_VSIM)
}

fn insert_topk(anchor: &mut AnchorState, entity: &[u8], dist: f32) {
    let k = anchor.k as usize;
    let n = anchor.top_n as usize;
    // Find insertion point.
    let mut pos = n;
    for i in 0..n {
        if dist < anchor.top_dist[i] {
            pos = i;
            break;
        }
    }
    if pos >= k {
        return;
    }
    let last = if n < k { n } else { k - 1 };
    let mut i = last;
    while i > pos {
        anchor.top_entity[i] = anchor.top_entity[i - 1];
        anchor.top_len[i] = anchor.top_len[i - 1];
        anchor.top_dist[i] = anchor.top_dist[i - 1];
        i -= 1;
    }
    let l = entity.len().min(64);
    anchor.top_entity[pos] = [0; 64];
    anchor.top_entity[pos][..l].copy_from_slice(&entity[..l]);
    anchor.top_len[pos] = l as u8;
    anchor.top_dist[pos] = dist;
    if n < k {
        anchor.top_n = (n + 1) as u8;
    }
}

/// u64 → decimal text.
fn fmt_u64(mut v: u64, out: &mut [u8]) -> usize {
    if v == 0 {
        out[0] = b'0';
        return 1;
    }
    let mut digits = [0u8; 20];
    let mut d = 0usize;
    while v > 0 {
        digits[d] = b'0' + (v % 10) as u8;
        v /= 10;
        d += 1;
    }
    let n = d;
    while d > 0 {
        d -= 1;
        out[n - 1 - d] = digits[d];
    }
    n
}

/// Minimal f32 → decimal text (3 fraction digits): enough to be read
/// back by any client; never used in a key.
fn fmt_f32(v: f32, out: &mut [u8]) -> usize {
    let mut n = 0usize;
    let mut x = v;
    if x < 0.0 {
        out[n] = b'-';
        n += 1;
        x = -x;
    }
    let int = x as u64;
    let frac = ((x - int as f32) * 1000.0 + 0.5) as u64;
    let mut digits = [0u8; 20];
    let mut d = 0usize;
    let mut iv = int;
    if iv == 0 {
        digits[0] = b'0';
        d = 1;
    }
    while iv > 0 {
        digits[d] = b'0' + (iv % 10) as u8;
        iv /= 10;
        d += 1;
    }
    while d > 0 {
        d -= 1;
        out[n] = digits[d];
        n += 1;
    }
    out[n] = b'.';
    n += 1;
    out[n] = b'0' + ((frac / 100) % 10) as u8;
    out[n + 1] = b'0' + ((frac / 10) % 10) as u8;
    out[n + 2] = b'0' + (frac % 10) as u8;
    n + 3
}

// ── Net dispatch ─────────────────────────────────────────────────────

fn drain_slot(anchor: &mut AnchorState, idx: usize) {
    loop {
        let len = anchor.slots[idx].recv_len;
        if len == 0 {
            return;
        }
        let mut buf = [0u8; RECV_BUF];
        buf[..len].copy_from_slice(&anchor.slots[idx].recv[..len]);
        match parse_one(&buf[..len]) {
            ParseStep::Incomplete => return,
            ParseStep::Error(_) => {
                anchor.slots[idx].state = S_CLOSING;
                return;
            }
            ParseStep::Ready { consumed, argv } => {
                {
                    let slot = &mut anchor.slots[idx];
                    slot.recv.copy_within(consumed..len, 0);
                    slot.recv_len = len - consumed;
                }
                begin_command(anchor, idx, &buf[..len], &argv);
                if anchor.slots[idx].state != S_READY {
                    return;
                }
            }
        }
    }
}

unsafe fn dispatch_net(anchor: &mut AnchorState, msg_type: u8, payload: &[u8]) {
    match msg_type {
        NET_MSG_BOUND => {
            // BOUND payload: [conn_id:u16 LE][local_port:u16 LE].
            if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = net_conn_id(payload).unwrap_or(SLOT_FREE);
                    anchor.phase = PHASE_LISTENING;
                }
            } else if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= NET_CONN_LEN {
                // Single-anchor provider (no port in payload): claim
                // the first BOUND we see.
                anchor.server_conn_id = net_conn_id(payload).unwrap_or(SLOT_FREE);
                anchor.phase = PHASE_LISTENING;
            }
        }
        NET_MSG_ACCEPTED => {
            // ACCEPTED payload: [conn_id:u16 LE][local_port:u16 LE].
            if payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port != anchor.listen_port {
                    return;
                }
            }
            if let Some(new_id) = net_conn_id(payload) {
                if anchor.alloc_slot(new_id).is_none() {
                    let _ = net_send(anchor, NET_CMD_CLOSE, new_id, &[]);
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() > NET_CONN_LEN {
                let Some(conn_id) = net_conn_id(payload) else {
                    return;
                };
                let data = &payload[NET_CONN_LEN..];
                if let Some(idx) = anchor.find_slot(conn_id) {
                    let slot = &mut anchor.slots[idx];
                    let room = RECV_BUF - slot.recv_len;
                    if data.len() > room {
                        slot.state = S_CLOSING;
                    } else {
                        let at = slot.recv_len;
                        slot.recv[at..at + data.len()].copy_from_slice(data);
                        slot.recv_len += data.len();
                        drain_slot(anchor, idx);
                    }
                }
            }
        }
        NET_MSG_CLOSED | NET_MSG_ERROR => {
            if let Some(conn_id) = net_conn_id(payload) {
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                }
            }
        }
        _ => {}
    }
}

// ── Module ABI ───────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<AnchorState>() as u32
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
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if state.is_null() || syscalls.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<AnchorState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let anchor = unsafe { &mut *state.cast::<AnchorState>() };
    anchor.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(anchor, params, params_len) };
    }
    anchor.net_in = in_chan;
    anchor.net_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        anchor.kv_in = dev_channel_port(sys, 0, 1);
        anchor.kv_out = dev_channel_port(sys, 1, 1);
        anchor.metrics_out = dev_channel_port(sys, 1, 2);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let anchor = unsafe { &mut *state.cast::<AnchorState>() };
    let sys_ptr = anchor.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }

    if anchor.phase == PHASE_INIT && unsafe { net_bind(anchor) } {
        anchor.phase = PHASE_WAIT_BOUND;
    }

    unsafe {
        let sys = &*sys_ptr;
        for _ in 0..8 {
            let mut env = [0u8; SCRATCH_BUF];
            let Some((msg, payload)) = read_one_envelope(sys, anchor.net_in, &mut env) else {
                break;
            };
            dispatch_net(anchor, msg, payload);
        }
        let mut env = [0u8; SCRATCH_BUF];
        if let Some((msg, payload)) = read_one_envelope(sys, anchor.kv_in, &mut env) {
            if msg == MSG_KV_RESPONSE && payload.len() >= 20 {
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                let result = payload[9];
                let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
                if corr == anchor.kv_corr && payload.len() >= 20 + blen {
                    // Fence tail (wire::FenceTail) trails the body; its
                    // commit_frontier is the feed surface's resolved
                    // frontier.
                    let tail_at = 20 + blen;
                    if let Some(tail) = payload.get(tail_at..tail_at + wire::FenceTail::LEN) {
                        if let Some(t) = wire::FenceTail::decode(tail) {
                            anchor.last_frontier = t.commit_frontier;
                        }
                    }
                    let mut body = [0u8; SCRATCH_BUF];
                    body[..blen].copy_from_slice(&payload[20..20 + blen]);
                    on_kv_response(anchor, result, &body[..blen]);
                }
            }
        }
    }

    for idx in 0..MAX_CONNS {
        if anchor.slots[idx].conn_id == SLOT_FREE {
            continue;
        }
        if anchor.slots[idx].state == S_CLOSING {
            let conn = anchor.slots[idx].conn_id;
            let _ = unsafe { net_send(anchor, NET_CMD_CLOSE, conn, &[]) };
            anchor.free_slot(idx);
            continue;
        }
        flush_slot(anchor, idx);
        if anchor.slots[idx].state == S_READY && anchor.slots[idx].recv_len > 0 {
            drain_slot(anchor, idx);
        }
    }

    anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
    if anchor.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                anchor.metrics_out,
                &[anchor.m_sessions, anchor.m_commands, anchor.m_errors],
            );
        }
    }
    0
}

unsafe fn write_envelope(
    sys: &SyscallTable,
    chan: i32,
    msg_type: u8,
    payload_len: usize,
    env: &mut [u8],
) -> bool {
    if chan < 0 || payload_len > u16::MAX as usize {
        return false;
    }
    env[0] = msg_type;
    env[1] = (payload_len & 0xFF) as u8;
    env[2] = ((payload_len >> 8) & 0xFF) as u8;
    let total = wire::ENVELOPE_HDR + payload_len;
    (sys.channel_write)(chan, env.as_mut_ptr(), total) == total as i32
}

unsafe fn read_one_envelope<'a>(
    sys: &SyscallTable,
    chan: i32,
    scratch: &'a mut [u8],
) -> Option<(u8, &'a [u8])> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; 3];
    if (sys.channel_read)(chan, hdr.as_mut_ptr(), 3) < 3 {
        return None;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len > scratch.len() {
        return None;
    }
    if payload_len > 0
        && ((sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len) as usize) < payload_len
    {
        return None;
    }
    Some((hdr[0], &scratch[..payload_len]))
}
