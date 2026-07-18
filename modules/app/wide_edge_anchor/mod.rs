//! wide_edge_anchor — the wide-column capability's CQL connector
//! (RFC database foundation §14.17, Phase 8).
//!
//! A real Cassandra CQL client connects on 9042, negotiates STARTUP →
//! READY, and can CREATE TABLE, INSERT, and SELECT. The semantics are
//! §14.17's, not relational (§14.15: models share foundations, not
//! meanings): a row INSERT decomposes into CELLS — one canonical KV
//! key per column via `models::encode_wide_key` under `KS_WIDE_TABLE`,
//! plus a row-marker cell so a row of nothing but its partition key
//! still exists — and all of a statement's cells commit as ONE
//! `KV_OP_TXN`. CQL `INSERT` is an upsert by definition, so the
//! transaction carries no absence comparisons; atomicity is the point,
//! duplicate refusal is not.
//!
//! The logical cell timestamp is a COMMITTED INPUT (§14.17: "never
//! sampled independently by replicas"): each writing statement INCRs a
//! model-owned sequence through consensus and stamps every cell it
//! writes with the result. Last-write-wins between two cells compares
//! those committed values and nothing else — `SELECT` reassembly picks
//! the highest version per column, so provider scan order (unordered
//! on the memory provider) cannot change an answer.
//!
//! Like the document anchor: this connector also executes (one
//! connector today; a second triggers the §14.2 split), one statement
//! is in flight at a time, and the router pairing is POSITIONAL —
//! pair #1, the ports named `redis_in`/`redis_out`, stamped
//! `PROTO_REDIS`, because the router's sixteen outputs are allocated
//! and a wide-column composition has no redis anchor.

#![no_std]
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
#![allow(
    clippy::manual_memcpy,
    clippy::needless_range_loop,
    reason = "hand-written index loops build wire envelopes byte-by-byte throughout these modules; the explicit form is the module idiom"
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

#[path = "../../common/cql_server_codec.rs"]
mod codec;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use codec::cql_core::{cql_op, cql_parse_frame};
use codec::models::{self, encode_value_ordered, ClusteringKey, LogicalType, SortDirection, Value};
use codec::{CqlLit, CqlStatement, WideSchema};
use net_proto::{
    NET_CMD_BIND, NET_CMD_CLOSE, NET_CMD_SEND, NET_MSG_ACCEPTED, NET_MSG_BOUND, NET_MSG_CLOSED,
    NET_MSG_DATA, NET_MSG_ERROR,
};
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_INCR, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_OP_TXN,
    KV_RESULT_CAS_FAILED, KV_RESULT_EXISTS, KV_RESULT_INTEGER, KV_RESULT_NOT_FOUND, KV_RESULT_OK,
    KV_RESULT_RANGE, KV_RESULT_TXN, PROTO_REDIS, PUT_FLAG_NX,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Capacities ───────────────────────────────────────────────────────

const MAX_CONNS: usize = 8;
const RECV_BUF: usize = 8192;
const SEND_BUF: usize = 8192;
const SCRATCH_BUF: usize = 16384;
const SLOT_FREE: u8 = 0xFF;
const DEFAULT_LISTEN_PORT: u16 = 9042;
const KEY_MAX: usize = 512;
const DATABASE_ID: u32 = 0;
/// The row-marker cell's column id: present for every written row, so
/// a row whose only data is its partition key still exists.
const ROW_MARKER_COL: u32 = 0xFFFF_FFFF;
/// Bounded SELECT: partitions per reply batch.
const MAX_RESULT_ROWS: usize = 16;

// Catalog record shapes inside KS_MODEL_CATALOG's user keyspace,
// discriminated after the database id (the fixed-width catalog keys
// from `encode_model_catalog_key` start with [db][kind:u8 1..=8], so
// 0xFC..0xFE cannot collide with them).
const CAT_KIND_SEQ_TABLE: u8 = 0xFD;
const CAT_KIND_SEQ_VERSION: u8 = 0xFC;
const CAT_KIND_NAME: u8 = 0xFE;

// ── Slot / statement machine ─────────────────────────────────────────

const S_WAIT_STARTUP: u8 = 0;
const S_READY: u8 = 1;
const S_BUSY: u8 = 2;
const S_CLOSING: u8 = 3;

const D_IDLE: u8 = 0;
const D_NAME: u8 = 1; // table-name lookup
const D_ALLOC: u8 = 2; // table id INCR
const D_SCHEMA_PUT: u8 = 3;
const D_NAME_PUT: u8 = 4;
const D_SCHEMA_GET: u8 = 5; // schema read for INSERT/SELECT
const D_VERSION: u8 = 6; // cell-timestamp INCR
const D_WRITE: u8 = 7; // the cells TXN
const D_SCAN: u8 = 8; // SELECT scan page
const D_DEL_SCAN: u8 = 9; // DELETE: a row's cell page to remove, then loop
const D_DEL_DEL: u8 = 10; // DELETE: a page removed → re-scan the row

const PHASE_INIT: u8 = 0;
const PHASE_WAIT_BOUND: u8 = 1;
const PHASE_LISTENING: u8 = 2;

#[repr(C)]
struct Slot {
    conn_id: u8,
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
            state: S_WAIT_STARTUP,
            recv: [0; RECV_BUF],
            recv_len: 0,
            send: [0; SEND_BUF],
            send_len: 0,
        }
    }
}

define_params! {
    AnchorState;

    1, listen_port, u16, 9042
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 9042); };
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
    server_conn_id: u8,
    slots: [Slot; MAX_CONNS],

    // The single in-flight statement.
    d_phase: u8,
    d_slot: u8,
    d_stream: i16,
    stmt: [u8; 2048],
    stmt_len: u16,
    schema: WideSchema,
    version: u64,
    /// SELECT: scan cursor + staged cells (raw scan pairs).
    cursor: u64,
    cells: [u8; SEND_BUF],
    cells_len: u16,

    kv_corr: u64,
    env: [u8; SCRATCH_BUF],
    scratch: [u8; SCRATCH_BUF],

    m_sessions: u64,
    m_statements: u64,
    m_cells_written: u64,
    m_rows_returned: u64,
    m_errors: u64,
    step_ctr: u64,
}

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
        self.d_stream = 0;
        self.stmt = [0; 2048];
        self.stmt_len = 0;
        self.schema = WideSchema::EMPTY;
        self.version = 0;
        self.cursor = 0;
        self.cells = [0; SEND_BUF];
        self.cells_len = 0;
        self.kv_corr = 0;
        self.env = [0; SCRATCH_BUF];
        self.scratch = [0; SCRATCH_BUF];
        self.m_sessions = 0;
        self.m_statements = 0;
        self.m_cells_written = 0;
        self.m_rows_returned = 0;
        self.m_errors = 0;
        self.step_ctr = 0;
    }

    fn find_slot(&self, conn_id: u8) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| s.conn_id == conn_id && s.conn_id != SLOT_FREE)
    }

    fn alloc_slot(&mut self, conn_id: u8) -> Option<usize> {
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

// ── Net plumbing (doc anchor pattern) ────────────────────────────────

unsafe fn net_send(anchor: &mut AnchorState, cmd: u8, conn_id: u8, data: &[u8]) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let payload_len = 1 + data.len();
    if payload_len + NET_FRAME_HDR > SCRATCH_BUF {
        return false;
    }
    let scratch = anchor.scratch.as_mut_ptr();
    *scratch = cmd;
    *scratch.add(1) = (payload_len & 0xFF) as u8;
    *scratch.add(2) = ((payload_len >> 8) & 0xFF) as u8;
    *scratch.add(NET_FRAME_HDR) = conn_id;
    if !data.is_empty() {
        core::ptr::copy_nonoverlapping(data.as_ptr(), scratch.add(NET_FRAME_HDR + 1), data.len());
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

// ── KV plumbing ──────────────────────────────────────────────────────

const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

fn kv_send(anchor: &mut AnchorState, op: u8, body_len: usize, next: u8) -> bool {
    anchor.kv_corr = anchor.kv_corr.wrapping_add(1).max(1);
    const REQ_HEAD: usize = 18;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > anchor.env.len() {
        return false;
    }
    anchor.env[at..at + 8].copy_from_slice(&anchor.kv_corr.to_le_bytes());
    // Positional pair #1 (see the module header): the byte routes the
    // reply to the port this module is wired to.
    anchor.env[at + 8] = PROTO_REDIS;
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

fn stage_get(anchor: &mut AnchorState, key: &[u8]) -> Option<usize> {
    let need = 2 + key.len();
    if BODY_AT + need > anchor.env.len() {
        return None;
    }
    anchor.env[BODY_AT..BODY_AT + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    anchor.env[BODY_AT + 2..BODY_AT + need].copy_from_slice(key);
    Some(need)
}

fn stage_incr(anchor: &mut AnchorState, key: &[u8]) -> Option<usize> {
    let need = 2 + key.len() + 8;
    if BODY_AT + need > anchor.env.len() {
        return None;
    }
    anchor.env[BODY_AT..BODY_AT + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    anchor.env[BODY_AT + 2..BODY_AT + 2 + key.len()].copy_from_slice(key);
    anchor.env[BODY_AT + 2 + key.len()..BODY_AT + need].copy_from_slice(&1i64.to_le_bytes());
    Some(need)
}

fn stage_put(anchor: &mut AnchorState, key: &[u8], value: &[u8], flags: u8) -> Option<usize> {
    let need = 2 + key.len() + 4 + value.len() + 1 + 8;
    if BODY_AT + need > anchor.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    anchor.env[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    p += 4;
    anchor.env[p..p + value.len()].copy_from_slice(value);
    p += value.len();
    anchor.env[p] = flags;
    p += 1;
    anchor.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
    Some(need)
}

// ── Keys ─────────────────────────────────────────────────────────────

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

fn seq_key(out: &mut [u8], kind: u8) -> Option<usize> {
    let mut body = [0u8; 5];
    body[0..4].copy_from_slice(&DATABASE_ID.to_be_bytes());
    body[4] = kind;
    user_key(out, models::KS_MODEL_CATALOG, &body)
}

fn name_key(out: &mut [u8], name: &[u8]) -> Option<usize> {
    if name.is_empty() || name.len() > codec::CQL_NAME_MAX {
        return None;
    }
    let mut body = [0u8; 6 + codec::CQL_NAME_MAX];
    body[0..4].copy_from_slice(&DATABASE_ID.to_be_bytes());
    body[4] = CAT_KIND_NAME;
    body[5] = models::ObjectKind::WideColumnTable as u8;
    body[6..6 + name.len()].copy_from_slice(name);
    user_key(out, models::KS_MODEL_CATALOG, &body[..6 + name.len()])
}

fn schema_key(out: &mut [u8], table_id: u32) -> Option<usize> {
    let mut body = [0u8; models::MODEL_CATALOG_KEY_LEN];
    let n = models::encode_model_catalog_key(
        &mut body,
        DATABASE_ID,
        models::ObjectKind::WideColumnTable,
        table_id,
    )?;
    user_key(out, models::KS_MODEL_CATALOG, &body[..n])
}

/// Cell user key: wide key body under KS_WIDE_TABLE, with the row's
/// clustering key (empty for a partition-key-only table).
fn cell_key(
    out: &mut [u8],
    table_id: u32,
    partition: &[u8],
    clustering: &ClusteringKey<'_>,
    column_id: u32,
    version: u64,
) -> Option<usize> {
    let mut body = [0u8; KEY_MAX];
    let n = models::encode_wide_key(
        &mut body, table_id, partition, clustering, column_id, version,
    )?;
    user_key(out, models::KS_WIDE_TABLE, &body[..n])
}

// ── Value conversion ─────────────────────────────────────────────────

/// A CQL literal as a typed model value, or None on a type mismatch
/// (refused, never coerced by syntax).
fn lit_value<'a>(ty: LogicalType, lit: &CqlLit<'a>) -> Option<Value<'a>> {
    Some(match (ty, lit) {
        (LogicalType::Int, CqlLit::Int(v)) => Value::Int(i32::try_from(*v).ok()?),
        (LogicalType::BigInt, CqlLit::Int(v)) => Value::BigInt(*v),
        (LogicalType::VarChar { .. }, CqlLit::Str(s)) => Value::Text(s),
        (LogicalType::Boolean, CqlLit::Bool(b)) => Value::Boolean(*b),
        (LogicalType::Double, CqlLit::Int(v)) => Value::Double(*v as f64),
        _ => return None,
    })
}

/// Render a decoded value as CQL wire bytes.
fn render_value(v: Value<'_>, out: &mut [u8]) -> Option<usize> {
    Some(match v {
        Value::Int(i) => {
            out[..4].copy_from_slice(&i.to_be_bytes());
            4
        }
        Value::BigInt(i) => {
            out[..8].copy_from_slice(&i.to_be_bytes());
            8
        }
        Value::Boolean(b) => {
            out[0] = u8::from(b);
            1
        }
        Value::Double(d) => {
            out[..8].copy_from_slice(&d.to_be_bytes());
            8
        }
        Value::Text(s) => {
            if s.len() > out.len() {
                return None;
            }
            out[..s.len()].copy_from_slice(s);
            s.len()
        }
        _ => return None,
    })
}

/// Render a stored (ordered-encoded) cell value as CQL wire bytes.
fn render_cell(ty: LogicalType, encoded: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut text = [0u8; models::MAX_TEXT_LEN];
    let (v, _) = models::decode_value_ordered(encoded, ty, &mut text)?;
    Some(match v {
        Value::Int(i) => {
            out[..4].copy_from_slice(&i.to_be_bytes());
            4
        }
        Value::BigInt(i) => {
            out[..8].copy_from_slice(&i.to_be_bytes());
            8
        }
        Value::Boolean(b) => {
            out[0] = u8::from(b);
            1
        }
        Value::Double(d) => {
            out[..8].copy_from_slice(&d.to_be_bytes());
            8
        }
        Value::Text(s) => {
            if s.len() > out.len() {
                return None;
            }
            out[..s.len()].copy_from_slice(s);
            s.len()
        }
        _ => return None,
    })
}

// ── Reply helpers ────────────────────────────────────────────────────

fn reply_frame(anchor: &mut AnchorState, frame: &[u8]) {
    let idx = anchor.d_slot as usize;
    anchor.d_phase = D_IDLE;
    if anchor.slots[idx].conn_id != SLOT_FREE && anchor.slots[idx].state == S_BUSY {
        anchor.slots[idx].state = S_READY;
    }
    let mut buf = [0u8; SEND_BUF];
    let n = frame.len().min(buf.len());
    buf[..n].copy_from_slice(&frame[..n]);
    send_to_slot(anchor, idx, &buf[..n]);
}

fn reply_error(anchor: &mut AnchorState, code: i32, msg: &[u8]) {
    anchor.m_errors = anchor.m_errors.wrapping_add(1);
    let mut out = [0u8; 512];
    let stream = anchor.d_stream;
    if let Some(n) = codec::error(stream, code, msg, &mut out) {
        let mut f = [0u8; 512];
        f[..n].copy_from_slice(&out[..n]);
        reply_frame(anchor, &f[..n]);
    } else {
        anchor.d_phase = D_IDLE;
    }
}

fn reply_void(anchor: &mut AnchorState) {
    let mut out = [0u8; 64];
    let stream = anchor.d_stream;
    if let Some(n) = codec::result_void(stream, &mut out) {
        let mut f = [0u8; 64];
        f[..n].copy_from_slice(&out[..n]);
        reply_frame(anchor, &f[..n]);
    } else {
        anchor.d_phase = D_IDLE;
    }
}

// ── Statement driving ────────────────────────────────────────────────

fn parse_stmt<'a>(text: &'a [u8]) -> Option<CqlStatement<'a>> {
    codec::parse_cql(text).ok()
}

fn begin_statement(anchor: &mut AnchorState, idx: usize, stream: i16, text: &[u8]) {
    anchor.m_statements = anchor.m_statements.wrapping_add(1);
    if anchor.d_phase != D_IDLE {
        // 0x1001 Overloaded: retryable in every driver.
        let mut out = [0u8; 128];
        if let Some(n) = codec::error(stream, 0x1001, b"one statement at a time", &mut out) {
            send_to_slot(anchor, idx, &out[..n].to_vec_bounded());
        }
        return;
    }
    if text.len() > anchor.stmt.len() {
        let mut out = [0u8; 128];
        if let Some(n) = codec::error(stream, codec::ERR_INVALID, b"statement too long", &mut out) {
            send_to_slot(anchor, idx, &out[..n].to_vec_bounded());
        }
        return;
    }
    anchor.stmt[..text.len()].copy_from_slice(text);
    anchor.stmt_len = text.len() as u16;
    anchor.d_slot = idx as u8;
    anchor.d_stream = stream;
    anchor.slots[idx].state = S_BUSY;

    let mut stmt = [0u8; 2048];
    stmt[..text.len()].copy_from_slice(text);
    let Some(parsed) = parse_stmt(&stmt[..text.len()]) else {
        reply_error(anchor, codec::ERR_SYNTAX, b"unsupported CQL in this slice");
        return;
    };
    let table: &[u8] = match &parsed {
        CqlStatement::Create(c) => c.table,
        CqlStatement::Insert(i) => i.table,
        CqlStatement::Select(s) => s.table,
        CqlStatement::Update(u) => u.table,
        CqlStatement::Delete(d) => d.table,
    };
    let mut key = [0u8; KEY_MAX];
    let Some(kn) = name_key(&mut key, table) else {
        reply_error(anchor, codec::ERR_INVALID, b"invalid table name");
        return;
    };
    let mut k = [0u8; KEY_MAX];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_get(anchor, &k[..kn]) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    if !kv_send(anchor, KV_OP_GET, bn, D_NAME) {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
    }
}

/// Tiny helper: a bounded copy of an error frame (avoids alloc).
trait ToVecBounded {
    fn to_vec_bounded(&self) -> [u8; 128];
}
impl ToVecBounded for [u8] {
    fn to_vec_bounded(&self) -> [u8; 128] {
        let mut out = [0u8; 128];
        let n = self.len().min(128);
        out[..n].copy_from_slice(&self[..n]);
        out
    }
}

fn reparse(anchor: &AnchorState, buf: &mut [u8; 2048]) -> usize {
    let n = anchor.stmt_len as usize;
    buf[..n].copy_from_slice(&anchor.stmt[..n]);
    n
}

fn on_kv_response(anchor: &mut AnchorState, result: u8, body: &[u8]) {
    let mut stmt = [0u8; 2048];
    let sn = reparse(anchor, &mut stmt);
    let Some(parsed) = parse_stmt(&stmt[..sn]) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    match anchor.d_phase {
        D_NAME => match (&parsed, result) {
            (CqlStatement::Create(_), KV_RESULT_OK) => {
                reply_error(anchor, codec::ERR_ALREADY_EXISTS, b"table exists")
            }
            (CqlStatement::Create(_), KV_RESULT_NOT_FOUND) => {
                let mut key = [0u8; KEY_MAX];
                let Some(kn) = seq_key(&mut key, CAT_KIND_SEQ_TABLE) else {
                    reply_error(anchor, codec::ERR_SERVER, b"internal");
                    return;
                };
                let mut k = [0u8; KEY_MAX];
                k[..kn].copy_from_slice(&key[..kn]);
                let Some(bn) = stage_incr(anchor, &k[..kn]) else {
                    reply_error(anchor, codec::ERR_SERVER, b"internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_INCR, bn, D_ALLOC) {
                    reply_error(anchor, codec::ERR_SERVER, b"internal");
                }
            }
            (_, KV_RESULT_NOT_FOUND) => reply_error(anchor, codec::ERR_INVALID, b"no such table"),
            (_, KV_RESULT_OK) if body.len() >= 4 => {
                let table_id = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let mut key = [0u8; KEY_MAX];
                let Some(kn) = schema_key(&mut key, table_id) else {
                    reply_error(anchor, codec::ERR_SERVER, b"internal");
                    return;
                };
                let mut k = [0u8; KEY_MAX];
                k[..kn].copy_from_slice(&key[..kn]);
                let Some(bn) = stage_get(anchor, &k[..kn]) else {
                    reply_error(anchor, codec::ERR_SERVER, b"internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_GET, bn, D_SCHEMA_GET) {
                    reply_error(anchor, codec::ERR_SERVER, b"internal");
                }
            }
            _ => reply_error(anchor, codec::ERR_SERVER, b"store unavailable"),
        },
        D_ALLOC => {
            if result != KV_RESULT_INTEGER || body.len() < 8 {
                reply_error(anchor, codec::ERR_SERVER, b"store unavailable");
                return;
            }
            let counter = i64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            if counter <= 0 || counter > u32::MAX as i64 {
                reply_error(anchor, codec::ERR_SERVER, b"store unavailable");
                return;
            }
            let CqlStatement::Create(c) = &parsed else {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
                return;
            };
            let Some(ws) = WideSchema::from_create(counter as u32, c) else {
                reply_error(anchor, codec::ERR_INVALID, b"schema too large");
                return;
            };
            anchor.schema = ws;
            let mut rec = [0u8; codec::WIDE_SCHEMA_MAX];
            let Some(rn) = anchor.schema.encode(&mut rec) else {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
                return;
            };
            let table_id = anchor.schema.table_id;
            let mut key = [0u8; KEY_MAX];
            let Some(kn) = schema_key(&mut key, table_id) else {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
                return;
            };
            let mut k = [0u8; KEY_MAX];
            k[..kn].copy_from_slice(&key[..kn]);
            let Some(bn) = stage_put(anchor, &k[..kn], &rec[..rn], PUT_FLAG_NX) else {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
                return;
            };
            if !kv_send(anchor, KV_OP_PUT, bn, D_SCHEMA_PUT) {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
            }
        }
        D_SCHEMA_PUT => {
            if result != KV_RESULT_OK {
                reply_error(anchor, codec::ERR_SERVER, b"store unavailable");
                return;
            }
            let CqlStatement::Create(c) = &parsed else {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
                return;
            };
            let mut key = [0u8; KEY_MAX];
            let Some(kn) = name_key(&mut key, c.table) else {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
                return;
            };
            let mut k = [0u8; KEY_MAX];
            k[..kn].copy_from_slice(&key[..kn]);
            let id_be = anchor.schema.table_id.to_be_bytes();
            let Some(bn) = stage_put(anchor, &k[..kn], &id_be, PUT_FLAG_NX) else {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
                return;
            };
            if !kv_send(anchor, KV_OP_PUT, bn, D_NAME_PUT) {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
            }
        }
        D_NAME_PUT => match result {
            KV_RESULT_OK => reply_void(anchor),
            KV_RESULT_CAS_FAILED | KV_RESULT_EXISTS => {
                reply_error(anchor, codec::ERR_ALREADY_EXISTS, b"table exists")
            }
            _ => reply_error(anchor, codec::ERR_SERVER, b"store unavailable"),
        },
        D_SCHEMA_GET => {
            if result != KV_RESULT_OK {
                reply_error(anchor, codec::ERR_SERVER, b"schema missing");
                return;
            }
            let Some(ws) = WideSchema::decode(body) else {
                reply_error(anchor, codec::ERR_SERVER, b"schema corrupt");
                return;
            };
            anchor.schema = ws;
            match &parsed {
                // INSERT and UPDATE both stamp their cells with a committed
                // statement timestamp; UPDATE is an upsert of a partial row.
                CqlStatement::Insert(_) | CqlStatement::Update(_) => {
                    // Statement timestamp: a committed input.
                    let mut key = [0u8; KEY_MAX];
                    let Some(kn) = seq_key(&mut key, CAT_KIND_SEQ_VERSION) else {
                        reply_error(anchor, codec::ERR_SERVER, b"internal");
                        return;
                    };
                    let mut k = [0u8; KEY_MAX];
                    k[..kn].copy_from_slice(&key[..kn]);
                    let Some(bn) = stage_incr(anchor, &k[..kn]) else {
                        reply_error(anchor, codec::ERR_SERVER, b"internal");
                        return;
                    };
                    if !kv_send(anchor, KV_OP_INCR, bn, D_VERSION) {
                        reply_error(anchor, codec::ERR_SERVER, b"internal");
                    }
                }
                CqlStatement::Select(_) => {
                    anchor.cursor = 0;
                    anchor.cells_len = 0;
                    send_select_scan(anchor, &parsed);
                }
                CqlStatement::Delete(_) => {
                    // A row delete needs no version: it physically removes
                    // every cell of the partition, so there is no
                    // last-write-wins comparison to stamp.
                    anchor.cursor = 0;
                    send_delete_scan(anchor, &parsed);
                }
                _ => reply_error(anchor, codec::ERR_SERVER, b"internal"),
            }
        }
        D_VERSION => {
            if result != KV_RESULT_INTEGER || body.len() < 8 {
                reply_error(anchor, codec::ERR_SERVER, b"store unavailable");
                return;
            }
            let counter = i64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            if counter <= 0 {
                reply_error(anchor, codec::ERR_SERVER, b"store unavailable");
                return;
            }
            anchor.version = counter as u64;
            match &parsed {
                CqlStatement::Update(_) => send_update_txn(anchor, &parsed),
                _ => send_insert_txn(anchor, &parsed),
            }
        }
        D_WRITE => match result {
            KV_RESULT_TXN if !body.is_empty() && body[0] == 1 => {
                reply_void(anchor);
            }
            _ => reply_error(anchor, codec::ERR_SERVER, b"store unavailable"),
        },
        D_DEL_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error(anchor, codec::ERR_SERVER, b"store unavailable");
                return;
            }
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            if count == 0 {
                // The partition is empty: the row is gone.
                reply_void(anchor);
                return;
            }
            // Delete every cell key on this page, then re-scan from zero —
            // a delete shifts the store ordinals, so the resume cursor is 0
            // (the same shape TS.TRIM and the relational DELETE use).
            let mut p = BODY_AT;
            anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // count backfilled
            p += 2;
            let mut at = 10usize;
            let mut staged = 0u16;
            for _ in 0..count {
                let Some(klen) = body
                    .get(at..at + 2)
                    .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
                else {
                    reply_error(anchor, codec::ERR_SERVER, b"page corrupt");
                    return;
                };
                let koff = at + 2;
                let voff = koff + klen;
                let Some(vlen) = body
                    .get(voff..voff + 4)
                    .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
                else {
                    reply_error(anchor, codec::ERR_SERVER, b"page corrupt");
                    return;
                };
                if p + 2 + klen > BODY_AT + 3800 {
                    reply_error(anchor, codec::ERR_INVALID, b"row too large for one page");
                    return;
                }
                anchor.env[p..p + 2].copy_from_slice(&(klen as u16).to_le_bytes());
                p += 2;
                anchor.env[p..p + klen].copy_from_slice(&body[koff..koff + klen]);
                p += klen;
                staged += 1;
                at = voff + 4 + vlen;
            }
            anchor.env[BODY_AT..BODY_AT + 2].copy_from_slice(&staged.to_le_bytes());
            if !kv_send(anchor, KV_OP_DELETE, p - BODY_AT, D_DEL_DEL) {
                reply_error(anchor, codec::ERR_SERVER, b"internal");
            }
        }
        D_DEL_DEL => match result {
            KV_RESULT_OK | KV_RESULT_INTEGER => {
                anchor.cursor = 0;
                send_delete_scan(anchor, &parsed);
            }
            _ => reply_error(anchor, codec::ERR_SERVER, b"store unavailable"),
        },
        D_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error(anchor, codec::ERR_SERVER, b"store unavailable");
                return;
            }
            anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let take = (body.len() - 10).min(SEND_BUF - anchor.cells_len as usize);
            if body.len() - 10 > take {
                reply_error(anchor, codec::ERR_INVALID, b"result exceeds one batch");
                return;
            }
            let at = anchor.cells_len as usize;
            anchor.cells[at..at + take].copy_from_slice(&body[10..10 + take]);
            anchor.cells_len += take as u16;
            if anchor.cursor != 0 {
                send_select_scan(anchor, &parsed);
            } else {
                finish_select(anchor, &parsed);
            }
        }
        _ => {}
    }
}

/// Build + send the cells TXN for the INSERT.
fn send_insert_txn(anchor: &mut AnchorState, parsed: &CqlStatement<'_>) {
    let CqlStatement::Insert(ins) = parsed else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    // The partition key value must be among the inserted columns.
    let pk_name_owned = {
        let mut n = [0u8; codec::CQL_NAME_MAX];
        let name = anchor.schema.col_name(anchor.schema.pk);
        n[..name.len()].copy_from_slice(name);
        (n, name.len())
    };
    let pk_name = &pk_name_owned.0[..pk_name_owned.1];
    let pk_ty = anchor.schema.col_type(anchor.schema.pk);
    let Some(pk_at) = (0..ins.count).find(|&i| ins.cols[i] == pk_name) else {
        reply_error(
            anchor,
            codec::ERR_INVALID,
            b"insert must set the partition key",
        );
        return;
    };
    let Some(pk_value) = lit_value(pk_ty, &ins.values[pk_at]) else {
        reply_error(anchor, codec::ERR_INVALID, b"partition key type mismatch");
        return;
    };
    let mut pk_bytes = [0u8; models::MAX_ORDERED_VALUE_LEN];
    let Some(pk_len) = encode_value_ordered(&mut pk_bytes, pk_ty, pk_value) else {
        reply_error(anchor, codec::ERR_INVALID, b"partition key unencodable");
        return;
    };

    let table_id = anchor.schema.table_id;
    let version = anchor.version;

    // The clustering key from the clustering columns' inserted values.
    let cc = anchor.schema.clustering_count;
    let mut cl_types = [LogicalType::Int; codec::CQL_MAX_CLUSTERING];
    let mut cl_vals = [Value::Null; codec::CQL_MAX_CLUSTERING];
    let cl_dirs = [SortDirection::Ascending; codec::CQL_MAX_CLUSTERING];
    for j in 0..cc {
        let cidx = anchor.schema.clustering[j];
        let ty = anchor.schema.col_type(cidx);
        let cname_owned = {
            let mut n = [0u8; codec::CQL_NAME_MAX];
            let nm = anchor.schema.col_name(cidx);
            n[..nm.len()].copy_from_slice(nm);
            (n, nm.len())
        };
        let cname = &cname_owned.0[..cname_owned.1];
        let Some(pos) = (0..ins.count).find(|&i| ins.cols[i] == cname) else {
            reply_error(
                anchor,
                codec::ERR_INVALID,
                b"insert must set every clustering column",
            );
            return;
        };
        let Some(v) = lit_value(ty, &ins.values[pos]) else {
            reply_error(anchor, codec::ERR_INVALID, b"clustering key type mismatch");
            return;
        };
        cl_types[j] = ty;
        cl_vals[j] = v;
    }
    let clustering = ClusteringKey {
        types: &cl_types[..cc],
        values: &cl_vals[..cc],
        directions: &cl_dirs[..cc],
    };

    // Cells: one per inserted column that is neither the partition key nor a
    // clustering column (those live in the key), plus the row marker.
    let mut cell_count: u16 = 1;
    for i in 0..ins.count {
        if i == pk_at {
            continue;
        }
        let ci = anchor.schema.col_by_name(ins.cols[i]);
        if ci.is_some_and(|ci| anchor.schema.is_clustering(ci)) {
            continue;
        }
        cell_count += 1;
    }
    let end_guard = BODY_AT + 3800;
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // no cmps: upsert
    p += 2;
    anchor.env[p..p + 2].copy_from_slice(&cell_count.to_le_bytes());
    p += 2;

    // Row marker first.
    let mut mkey = [0u8; KEY_MAX + 64];
    let Some(mkn) = cell_key(
        &mut mkey,
        table_id,
        &pk_bytes[..pk_len],
        &clustering,
        ROW_MARKER_COL,
        version,
    ) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let Some(np) = txn_put(anchor, p, end_guard, &mkey[..mkn], &[]) else {
        reply_error(anchor, codec::ERR_INVALID, b"statement too large");
        return;
    };
    p = np;

    for i in 0..ins.count {
        if i == pk_at {
            continue;
        }
        let Some(ci) = anchor.schema.col_by_name(ins.cols[i]) else {
            reply_error(anchor, codec::ERR_INVALID, b"unknown column");
            return;
        };
        // Clustering columns are in the key, not written as cells.
        if anchor.schema.is_clustering(ci) {
            continue;
        }
        let ty = anchor.schema.col_type(ci);
        let Some(v) = lit_value(ty, &ins.values[i]) else {
            reply_error(anchor, codec::ERR_INVALID, b"column type mismatch");
            return;
        };
        let mut vbytes = [0u8; models::MAX_ORDERED_VALUE_LEN];
        let Some(vn) = encode_value_ordered(&mut vbytes, ty, v) else {
            reply_error(anchor, codec::ERR_INVALID, b"value unencodable");
            return;
        };
        let mut ckey = [0u8; KEY_MAX + 64];
        let Some(ckn) = cell_key(
            &mut ckey,
            table_id,
            &pk_bytes[..pk_len],
            &clustering,
            ci as u32,
            version,
        ) else {
            reply_error(anchor, codec::ERR_SERVER, b"internal");
            return;
        };
        let Some(np) = txn_put(anchor, p, end_guard, &ckey[..ckn], &vbytes[..vn]) else {
            reply_error(anchor, codec::ERR_INVALID, b"statement too large");
            return;
        };
        p = np;
    }
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // else branch
    p += 2;
    anchor.m_cells_written = anchor.m_cells_written.wrapping_add(u64::from(cell_count));
    if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
    }
}

/// Build + send the cells TXN for an UPDATE: the row marker plus one cell
/// per SET column, at the statement version. A partial upsert — SELECT
/// then picks the highest version per column, so the SET columns win while
/// the untouched columns keep their prior cells.
fn send_update_txn(anchor: &mut AnchorState, parsed: &CqlStatement<'_>) {
    let CqlStatement::Update(upd) = parsed else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    // A clustered table's UPDATE would need the clustering columns in the
    // WHERE to name one row; the single-key WHERE grammar cannot, so refuse.
    if anchor.schema.clustering_count > 0 {
        reply_error(
            anchor,
            codec::ERR_INVALID,
            b"UPDATE on a clustered table needs the full primary key (unsupported)",
        );
        return;
    }
    let pk_idx = anchor.schema.pk;
    let pk_name_owned = {
        let mut n = [0u8; codec::CQL_NAME_MAX];
        let name = anchor.schema.col_name(pk_idx);
        n[..name.len()].copy_from_slice(name);
        (n, name.len())
    };
    let pk_name = &pk_name_owned.0[..pk_name_owned.1];
    if upd.where_pk.0 != pk_name {
        reply_error(
            anchor,
            codec::ERR_INVALID,
            b"only partition-key equality in this slice",
        );
        return;
    }
    let pk_ty = anchor.schema.col_type(pk_idx);
    let Some(pk_value) = lit_value(pk_ty, &upd.where_pk.1) else {
        reply_error(anchor, codec::ERR_INVALID, b"partition key type mismatch");
        return;
    };
    let mut pk_bytes = [0u8; models::MAX_ORDERED_VALUE_LEN];
    let Some(pk_len) = encode_value_ordered(&mut pk_bytes, pk_ty, pk_value) else {
        reply_error(anchor, codec::ERR_INVALID, b"partition key unencodable");
        return;
    };

    let table_id = anchor.schema.table_id;
    let version = anchor.version;
    let cell_count: u16 = 1 + upd.count as u16;
    let end_guard = BODY_AT + 3800;
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // no cmps: upsert
    p += 2;
    anchor.env[p..p + 2].copy_from_slice(&cell_count.to_le_bytes());
    p += 2;

    // No clustering (refused above), so an empty clustering key.
    let empty_cl = ClusteringKey {
        types: &[],
        values: &[],
        directions: &[],
    };
    // Row marker: an UPDATE also asserts the row exists (CQL upsert).
    let mut mkey = [0u8; KEY_MAX + 64];
    let Some(mkn) = cell_key(
        &mut mkey,
        table_id,
        &pk_bytes[..pk_len],
        &empty_cl,
        ROW_MARKER_COL,
        version,
    ) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let Some(np) = txn_put(anchor, p, end_guard, &mkey[..mkn], &[]) else {
        reply_error(anchor, codec::ERR_INVALID, b"statement too large");
        return;
    };
    p = np;

    for i in 0..upd.count {
        let Some(ci) = anchor.schema.col_by_name(upd.cols[i]) else {
            reply_error(anchor, codec::ERR_INVALID, b"unknown column");
            return;
        };
        if ci == pk_idx {
            reply_error(anchor, codec::ERR_INVALID, b"cannot SET the partition key");
            return;
        }
        let ty = anchor.schema.col_type(ci);
        let Some(v) = lit_value(ty, &upd.values[i]) else {
            reply_error(anchor, codec::ERR_INVALID, b"column type mismatch");
            return;
        };
        let mut vbytes = [0u8; models::MAX_ORDERED_VALUE_LEN];
        let Some(vn) = encode_value_ordered(&mut vbytes, ty, v) else {
            reply_error(anchor, codec::ERR_INVALID, b"value unencodable");
            return;
        };
        let mut ckey = [0u8; KEY_MAX + 64];
        let Some(ckn) = cell_key(
            &mut ckey,
            table_id,
            &pk_bytes[..pk_len],
            &empty_cl,
            ci as u32,
            version,
        ) else {
            reply_error(anchor, codec::ERR_SERVER, b"internal");
            return;
        };
        let Some(np) = txn_put(anchor, p, end_guard, &ckey[..ckn], &vbytes[..vn]) else {
            reply_error(anchor, codec::ERR_INVALID, b"statement too large");
            return;
        };
        p = np;
    }
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // else branch
    p += 2;
    anchor.m_cells_written = anchor.m_cells_written.wrapping_add(u64::from(cell_count));
    if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
    }
}

/// One scan page of a DELETE's partition, to physically remove its cells.
/// The span is exactly one partition — DELETE requires the partition key.
fn send_delete_scan(anchor: &mut AnchorState, parsed: &CqlStatement<'_>) {
    let CqlStatement::Delete(del) = parsed else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let pk_idx = anchor.schema.pk;
    let pk_name_owned = {
        let mut n = [0u8; codec::CQL_NAME_MAX];
        let name = anchor.schema.col_name(pk_idx);
        n[..name.len()].copy_from_slice(name);
        (n, name.len())
    };
    if del.where_pk.0 != &pk_name_owned.0[..pk_name_owned.1] {
        reply_error(
            anchor,
            codec::ERR_INVALID,
            b"only partition-key equality in this slice",
        );
        return;
    }
    let pk_ty = anchor.schema.col_type(pk_idx);
    let Some(v) = lit_value(pk_ty, &del.where_pk.1) else {
        reply_error(anchor, codec::ERR_INVALID, b"partition key type mismatch");
        return;
    };
    let mut pk_bytes = [0u8; models::MAX_ORDERED_VALUE_LEN];
    let Some(pk_len) = encode_value_ordered(&mut pk_bytes, pk_ty, v) else {
        reply_error(anchor, codec::ERR_INVALID, b"partition key unencodable");
        return;
    };
    let empty = ClusteringKey {
        types: &[],
        values: &[],
        directions: &[],
    };
    let mut prefix_body = [0u8; KEY_MAX];
    let Some(plen) = models::encode_wide_clustering_prefix(
        &mut prefix_body,
        anchor.schema.table_id,
        &pk_bytes[..pk_len],
        &empty,
    ) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, models::KS_WIDE_TABLE, &prefix_body[..plen]) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let cursor = anchor.cursor;
    let need = 2 + sn + 2 + en + 10;
    if BODY_AT + need > anchor.env.len() {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    }
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&(sn as u16).to_le_bytes());
    p += 2;
    for i in 0..sn {
        anchor.env[p + i] = start[i];
    }
    p += sn;
    anchor.env[p..p + 2].copy_from_slice(&(en as u16).to_le_bytes());
    p += 2;
    for i in 0..en {
        anchor.env[p + i] = end[i];
    }
    p += en;
    anchor.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    anchor.env[p..p + 2].copy_from_slice(&32u16.to_le_bytes());
    if !kv_send(anchor, KV_OP_RANGE_SCAN, need, D_DEL_SCAN) {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
    }
}

/// Append one `[KV_OP_PUT][blen][put body]` to the TXN under build.
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

/// One scan page of the SELECT's span.
fn send_select_scan(anchor: &mut AnchorState, parsed: &CqlStatement<'_>) {
    let CqlStatement::Select(sel) = parsed else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let table_id = anchor.schema.table_id;
    // Span: whole table, or one partition.
    let mut prefix_body = [0u8; KEY_MAX];
    let plen = if let Some((col, lit)) = &sel.where_pk {
        let pk_name_ok = anchor.schema.col_name(anchor.schema.pk) == *col;
        if !pk_name_ok {
            reply_error(
                anchor,
                codec::ERR_INVALID,
                b"only partition-key equality in this slice",
            );
            return;
        }
        let pk_ty = anchor.schema.col_type(anchor.schema.pk);
        let Some(v) = lit_value(pk_ty, lit) else {
            reply_error(anchor, codec::ERR_INVALID, b"partition key type mismatch");
            return;
        };
        let mut pk_bytes = [0u8; models::MAX_ORDERED_VALUE_LEN];
        let Some(pk_len) = encode_value_ordered(&mut pk_bytes, pk_ty, v) else {
            reply_error(anchor, codec::ERR_INVALID, b"partition key unencodable");
            return;
        };
        let empty = ClusteringKey {
            types: &[],
            values: &[],
            directions: &[],
        };
        let Some(n) = models::encode_wide_clustering_prefix(
            &mut prefix_body,
            table_id,
            &pk_bytes[..pk_len],
            &empty,
        ) else {
            reply_error(anchor, codec::ERR_SERVER, b"internal");
            return;
        };
        n
    } else {
        // Whole table: the fixed-width table id prefix.
        prefix_body[0..4].copy_from_slice(&table_id.to_be_bytes());
        4
    };
    let mut start = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut start, models::KS_WIDE_TABLE, &prefix_body[..plen]) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let mut end = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let cursor = anchor.cursor;
    let need = 2 + sn + 2 + en + 10;
    if BODY_AT + need > anchor.env.len() {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    }
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&(sn as u16).to_le_bytes());
    p += 2;
    for i in 0..sn {
        anchor.env[p + i] = start[i];
    }
    p += sn;
    anchor.env[p..p + 2].copy_from_slice(&(en as u16).to_le_bytes());
    p += 2;
    for i in 0..en {
        anchor.env[p + i] = end[i];
    }
    p += en;
    anchor.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    anchor.env[p..p + 2].copy_from_slice(&32u16.to_le_bytes());
    if !kv_send(anchor, KV_OP_RANGE_SCAN, need, D_SCAN) {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
    }
}

/// All cells staged: reassemble rows (LWW per column) and reply.
fn finish_select(anchor: &mut AnchorState, parsed: &CqlStatement<'_>) {
    let CqlStatement::Select(sel) = parsed else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    // Which columns render, in order.
    let mut proj = [0usize; codec::CQL_MAX_COLS];
    let proj_len = if sel.col_count == 0 {
        for (i, slot) in proj.iter_mut().enumerate().take(anchor.schema.col_count) {
            *slot = i;
        }
        anchor.schema.col_count
    } else {
        let mut n = 0;
        for i in 0..sel.col_count {
            let Some(ci) = anchor.schema.col_by_name(sel.cols[i]) else {
                reply_error(anchor, codec::ERR_INVALID, b"unknown column");
                return;
            };
            proj[n] = ci;
            n += 1;
        }
        n
    };

    // The clustering columns' types/directions, for decoding cell keys.
    let cc = anchor.schema.clustering_count;
    let mut cl_types = [LogicalType::Int; codec::CQL_MAX_CLUSTERING];
    let cl_dirs = [SortDirection::Ascending; codec::CQL_MAX_CLUSTERING];
    for j in 0..cc {
        cl_types[j] = anchor.schema.col_type(anchor.schema.clustering[j]);
    }

    // Group cells by row identity — the key up to (not including) the
    // trailing [column:4][version:8], i.e. table+partition+clustering. A
    // partition-key-only table has one row per partition; a clustered table
    // has one row per (partition, clustering) tuple.
    struct Row {
        id: [u8; 128],
        id_len: usize,
        pk: [u8; 64],
        pk_len: usize,
        // Rendered clustering values, by clustering position.
        cl: [([u8; 64], usize, bool); codec::CQL_MAX_CLUSTERING],
        versions: [u64; codec::CQL_MAX_COLS],
        values: [([u8; 64], usize); codec::CQL_MAX_COLS],
        present: [bool; codec::CQL_MAX_COLS],
        marker: bool,
    }
    const EMPTY_ROW: Row = Row {
        id: [0; 128],
        id_len: 0,
        pk: [0; 64],
        pk_len: 0,
        cl: [([0; 64], 0, false); codec::CQL_MAX_CLUSTERING],
        versions: [0; codec::CQL_MAX_COLS],
        values: [([0; 64], 0); codec::CQL_MAX_COLS],
        present: [false; codec::CQL_MAX_COLS],
        marker: false,
    };
    let mut rows = [EMPTY_ROW; MAX_RESULT_ROWS];
    let mut row_count = 0usize;

    let cells_len = anchor.cells_len as usize;
    let mut at = 0usize;
    while at + 6 <= cells_len {
        let klen = u16::from_le_bytes([anchor.cells[at], anchor.cells[at + 1]]) as usize;
        let koff = at + 2;
        let voff = koff + klen;
        if voff + 4 > cells_len {
            break;
        }
        let vlen =
            u32::from_le_bytes(anchor.cells[voff..voff + 4].try_into().unwrap_or([0; 4])) as usize;
        let vend = voff + 4 + vlen;
        // The key body (after the 4-byte keyspace prefix) ends with
        // [column:4][version:8]; everything before that is the row identity.
        if vend > cells_len || klen < 4 + 12 {
            break;
        }
        let key_body = &anchor.cells[koff + 4..koff + klen];
        let id_bytes = &key_body[..key_body.len() - 12];
        // Decode partition + clustering values from the key.
        let mut pk_out = [0u8; 64];
        let mut text_out = [0u8; models::wide_scratch_len(codec::CQL_MAX_CLUSTERING)];
        let mut values_out = [Value::Null; codec::CQL_MAX_CLUSTERING];
        let decoded = models::decode_wide_key(
            key_body,
            &cl_types[..cc],
            &cl_dirs[..cc],
            &mut pk_out,
            &mut text_out,
            &mut values_out[..cc],
        );
        let Some((_tid, pk_len, _cc, column_id, version)) = decoded else {
            at = vend;
            continue;
        };
        // Find or create the row by identity.
        let mut r = row_count;
        for (i, row) in rows.iter().enumerate().take(row_count) {
            if row.id_len == id_bytes.len() && row.id[..row.id_len] == *id_bytes {
                r = i;
                break;
            }
        }
        if r == row_count {
            if row_count >= MAX_RESULT_ROWS || id_bytes.len() > rows[r].id.len() {
                reply_error(anchor, codec::ERR_INVALID, b"result exceeds one batch");
                return;
            }
            rows[r].id[..id_bytes.len()].copy_from_slice(id_bytes);
            rows[r].id_len = id_bytes.len();
            rows[r].pk[..pk_len].copy_from_slice(&pk_out[..pk_len]);
            rows[r].pk_len = pk_len;
            // Render the clustering values once.
            for j in 0..cc {
                let mut cb = [0u8; 64];
                match render_value(values_out[j], &mut cb) {
                    Some(cn) => {
                        rows[r].cl[j].0[..cn].copy_from_slice(&cb[..cn]);
                        rows[r].cl[j].1 = cn;
                        rows[r].cl[j].2 = false;
                    }
                    None => rows[r].cl[j].2 = true, // NULL
                }
            }
            row_count += 1;
        }
        if column_id == ROW_MARKER_COL {
            rows[r].marker = true;
        } else {
            let ci = column_id as usize;
            if ci < codec::CQL_MAX_COLS && (!rows[r].present[ci] || version > rows[r].versions[ci])
            {
                let take = vlen.min(64);
                rows[r].values[ci].0[..take]
                    .copy_from_slice(&anchor.cells[voff + 4..voff + 4 + take]);
                rows[r].values[ci].1 = take;
                rows[r].versions[ci] = version;
                rows[r].present[ci] = true;
            }
        }
        at = vend;
    }

    // Render.
    let mut cols_spec: [(&[u8], u16); codec::CQL_MAX_COLS] =
        [(b"".as_slice(), codec::TYPE_INT); codec::CQL_MAX_COLS];
    for (i, spec) in cols_spec.iter_mut().enumerate().take(proj_len) {
        let ci = proj[i];
        *spec = (
            anchor.schema.col_name(ci),
            codec::cql_type_of(anchor.schema.col_type(ci)),
        );
    }
    let mut body = [0u8; SEND_BUF];
    let table_id_pk = anchor.schema.pk;
    let Some(mut rr) = codec::RowsResult::new(&mut body, b"lattice", b"t", &cols_spec[..proj_len])
    else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let mut rendered = 0u64;
    for row in rows.iter().take(row_count) {
        if !row.marker {
            continue; // cells without a marker: partial artifacts
        }
        for i in 0..proj_len {
            let ci = proj[i];
            let cl_pos = anchor.schema.clustering[..cc].iter().position(|&x| x == ci);
            if ci == table_id_pk {
                // The partition key renders from the KEY bytes.
                let ty = anchor.schema.col_type(ci);
                let mut cell = [0u8; 64];
                match render_cell(ty, &row.pk[..row.pk_len], &mut cell) {
                    Some(n) => {
                        if rr.value(Some(&cell[..n])).is_none() {
                            reply_error(anchor, codec::ERR_INVALID, b"batch overflow");
                            return;
                        }
                    }
                    None => {
                        if rr.value(None).is_none() {
                            reply_error(anchor, codec::ERR_INVALID, b"batch overflow");
                            return;
                        }
                    }
                }
            } else if let Some(j) = cl_pos {
                // A clustering column renders from its captured key value.
                let (bytes, len, is_null) = &row.cl[j];
                let ok = if *is_null {
                    rr.value(None)
                } else {
                    rr.value(Some(&bytes[..*len]))
                };
                if ok.is_none() {
                    reply_error(anchor, codec::ERR_INVALID, b"batch overflow");
                    return;
                }
            } else if row.present[ci] {
                let ty = anchor.schema.col_type(ci);
                let mut cell = [0u8; 64];
                match render_cell(ty, &row.values[ci].0[..row.values[ci].1], &mut cell) {
                    Some(n) => {
                        if rr.value(Some(&cell[..n])).is_none() {
                            reply_error(anchor, codec::ERR_INVALID, b"batch overflow");
                            return;
                        }
                    }
                    None => {
                        if rr.value(None).is_none() {
                            reply_error(anchor, codec::ERR_INVALID, b"batch overflow");
                            return;
                        }
                    }
                }
            } else if rr.value(None).is_none() {
                reply_error(anchor, codec::ERR_INVALID, b"batch overflow");
                return;
            }
        }
        rr.end_row();
        rendered += 1;
    }
    let n = rr.finish();
    anchor.m_rows_returned = anchor.m_rows_returned.wrapping_add(rendered);
    let mut out = [0u8; SEND_BUF];
    let stream = anchor.d_stream;
    let Some(fl) = codec::frame_response(cql_op::RESULT, stream, &body[..n], &mut out) else {
        reply_error(anchor, codec::ERR_SERVER, b"internal");
        return;
    };
    let mut f = [0u8; SEND_BUF];
    f[..fl].copy_from_slice(&out[..fl]);
    reply_frame(anchor, &f[..fl]);
}

// ── Frame dispatch ───────────────────────────────────────────────────

fn drain_slot(anchor: &mut AnchorState, idx: usize) {
    loop {
        let len = anchor.slots[idx].recv_len;
        if len == 0 {
            return;
        }
        let mut buf = [0u8; RECV_BUF];
        buf[..len].copy_from_slice(&anchor.slots[idx].recv[..len]);
        let Some(frame) = cql_parse_frame(&buf[..len]) else {
            return; // incomplete
        };
        let used = frame.total;
        let stream = frame.stream;
        let opcode = frame.opcode;
        let mut body = [0u8; 4096];
        let blen = (frame.body_end - frame.body_start).min(body.len());
        body[..blen].copy_from_slice(&buf[frame.body_start..frame.body_start + blen]);
        {
            let slot = &mut anchor.slots[idx];
            slot.recv.copy_within(used..len, 0);
            slot.recv_len = len - used;
        }
        match opcode {
            cql_op::STARTUP => {
                let mut out = [0u8; 64];
                if let Some(n) = codec::ready(stream, &mut out) {
                    let f = out[..n].to_vec_bounded();
                    send_to_slot(anchor, idx, &f[..n.min(128)]);
                    if anchor.slots[idx].state == S_WAIT_STARTUP {
                        anchor.slots[idx].state = S_READY;
                    }
                }
            }
            cql_op::OPTIONS => {
                let mut out = [0u8; 256];
                if let Some(n) = codec::supported(stream, &mut out) {
                    let mut f = [0u8; 256];
                    f[..n].copy_from_slice(&out[..n]);
                    send_to_slot(anchor, idx, &f[..n]);
                }
            }
            cql_op::QUERY => match codec::parse_query(&body[..blen]) {
                Ok(text) => {
                    let mut t = [0u8; 2048];
                    let tn = text.len().min(t.len());
                    t[..tn].copy_from_slice(&text[..tn]);
                    begin_statement(anchor, idx, stream, &t[..tn]);
                    if anchor.slots[idx].state != S_READY {
                        return;
                    }
                }
                Err(()) => {
                    let mut out = [0u8; 128];
                    if let Some(n) = codec::error(
                        stream,
                        codec::ERR_PROTOCOL,
                        b"parameters and paging are not supported",
                        &mut out,
                    ) {
                        let f = out.to_vec_bounded();
                        send_to_slot(anchor, idx, &f[..n.min(128)]);
                    }
                }
            },
            _ => {
                let mut out = [0u8; 128];
                if let Some(n) =
                    codec::error(stream, codec::ERR_PROTOCOL, b"unsupported opcode", &mut out)
                {
                    let f = out.to_vec_bounded();
                    send_to_slot(anchor, idx, &f[..n.min(128)]);
                }
            }
        }
    }
}

unsafe fn dispatch_net(anchor: &mut AnchorState, msg_type: u8, payload: &[u8]) {
    match msg_type {
        NET_MSG_BOUND => {
            if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= 3 {
                let port = u16::from_le_bytes([payload[1], payload[2]]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = payload[0];
                    anchor.phase = PHASE_LISTENING;
                }
            } else if anchor.phase == PHASE_WAIT_BOUND && !payload.is_empty() {
                anchor.server_conn_id = payload[0];
                anchor.phase = PHASE_LISTENING;
            }
        }
        NET_MSG_ACCEPTED => {
            if payload.len() >= 3 {
                let port = u16::from_le_bytes([payload[1], payload[2]]);
                if port != anchor.listen_port {
                    return;
                }
            }
            if !payload.is_empty() {
                let new_id = payload[0];
                if anchor.alloc_slot(new_id).is_none() {
                    let _ = net_send(anchor, NET_CMD_CLOSE, new_id, &[]);
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() >= 2 {
                let conn_id = payload[0];
                let data = &payload[1..];
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
            if !payload.is_empty() {
                if let Some(idx) = anchor.find_slot(payload[0]) {
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
                &[
                    anchor.m_sessions,
                    anchor.m_statements,
                    anchor.m_cells_written,
                    anchor.m_rows_returned,
                    anchor.m_errors,
                ],
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
