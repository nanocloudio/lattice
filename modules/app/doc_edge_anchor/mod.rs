//! doc_edge_anchor — the document capability's MongoDB-wire connector
//! (RFC database foundation §14.16, Phase 8).
//!
//! A real MongoDB driver connects on 27017, runs the `hello`
//! handshake, and can `insert` and `find` against collections whose
//! documents live in the canonical KV store — the same router, the
//! same consensus path, the same durability story a Redis `SET` and a
//! SQL `INSERT` have. That is Phase 8's first acceptance clause
//! ("authoritative mutations use canonical Lattice transactions")
//! satisfied structurally: there is no document-side storage engine to
//! bypass anything with.
//!
//! ## Where the §14.2/§14.3 split sits for this capability
//!
//! The SQL surface has TWO connectors, so parsing/binding/execution
//! live in shared modules and the connectors only spell things. The
//! document capability has ONE connector today, so this anchor also
//! executes — command dispatch compiles straight into KV round trips.
//! The moment a second document connector exists, the execution half
//! moves to a shared module exactly as §14.2 demands; this is recorded
//! here and in `.context/phase8_document_plan.md` so the trigger is
//! named, not discovered.
//!
//! ## Wire pairing with the router
//!
//! The router's sixteen output ports are all allocated, so this anchor
//! uses the router's POSITIONAL pair #0 — the ports whose manifest
//! names are `etcd_in`/`etcd_out` — and stamps `PROTO_ETCD` on its
//! requests. The protocol byte is reply-routing state and the port
//! names are positional history: a document composition has no etcd
//! anchor, and the pair is wired to THIS module in its config. Loudly
//! commented there too.
//!
//! ## Semantics of the first slice, and its named refusals
//!
//! - `insert`: every document must carry `_id` (the server does not
//!   mint identities; a retry with a server-minted id is unknowable).
//!   Multi-document inserts are ONE `KV_OP_TXN` — atomic, like the SQL
//!   statement — and a duplicate `_id` anywhere fails the whole
//!   command with code 11000 and writes NOTHING.
//! - `find`: filter `{}` (or absent) scans the collection; filter
//!   `{_id: v}` is a point read. Any other filter is refused by name —
//!   an ignored filter returns wrong documents.
//! - Collections auto-create on first insert (Mongo semantics), with
//!   the name → id record living in `KS_DOCUMENT_CATALOG`.
//! - Results are one bounded batch (cursor id 0). A result set beyond
//!   the batch refuses rather than truncates.

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

#[path = "../../common/doc_server_codec.rs"]
mod codec;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use abi::contracts::net::net_proto::{
    conn_id, CMD_BIND as NET_CMD_BIND, CMD_CLOSE as NET_CMD_CLOSE, CMD_SEND as NET_CMD_SEND,
    CONN_ID_LEN, MSG_ACCEPTED as NET_MSG_ACCEPTED, MSG_BOUND as NET_MSG_BOUND,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_DATA as NET_MSG_DATA, MSG_ERROR as NET_MSG_ERROR,
};
use codec::models;
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_INCR, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_OP_TXN,
    KV_RESULT_CAS_FAILED, KV_RESULT_EXISTS, KV_RESULT_INTEGER, KV_RESULT_NOT_FOUND, KV_RESULT_OK,
    KV_RESULT_RANGE, KV_RESULT_TXN, PROTO_ETCD, PUT_FLAG_NX, TXN_CMP_MOD_EQUAL,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Capacities ───────────────────────────────────────────────────────

const MAX_CONNS: usize = 8;
const RECV_BUF: usize = 8192;
const SEND_BUF: usize = 8192;
const SCRATCH_BUF: usize = 16384;
const SLOT_FREE: u16 = 0xFFFF;
const DEFAULT_LISTEN_PORT: u16 = 27017;
// NET_FRAME_HDR comes from the SDK's runtime/net.rs include.

/// Longest collection name.
const NAME_MAX: usize = 64;
/// Longest `_id` identity ([type] + value).
const ID_MAX: usize = 1 + 128;
/// KV user-key scratch.
const KEY_MAX: usize = 512;
/// The single database id (composition-level, like the SQL side).
const DATABASE_ID: u32 = 0;

// ── Catalog keys (model-owned, §14.23) ───────────────────────────────
//
// KS_DOCUMENT_CATALOG holds two record shapes, discriminated by the
// byte after the database id (fixed width first, so they scan apart):
//   [db:u32 BE][0x01][name…]  → collection name record, value = id BE
//   [db:u32 BE][0x00]         → the collection-id allocator (INCR)
const CAT_KIND_SEQ: u8 = 0x00;
const CAT_KIND_NAME: u8 = 0x01;

// ── Slot / phases ────────────────────────────────────────────────────

const S_READY: u8 = 0;
const S_BUSY: u8 = 1; // this slot owns the in-flight command
const S_CLOSING: u8 = 2;

/// Anchor-level command machine (one command in flight, like the SQL
/// executor and for the same bounded-memory reasons).
const D_IDLE: u8 = 0;
const D_CAT_GET: u8 = 1; // collection name lookup
const D_CAT_ALLOC: u8 = 2; // collection id INCR
const D_CAT_PUT: u8 = 3; // collection name record write
const D_INSERT: u8 = 4; // the insert TXN
const D_FIND_GET: u8 = 5; // point read by _id
const D_FIND_SCAN: u8 = 6; // collection scan page
const D_DELETE: u8 = 7; // delete by _id
const D_UPD_READ: u8 = 8; // update: read to learn whether the doc matched
const D_UPDATE: u8 = 9; // update: the replacement PUT

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

    1, listen_port, u16, 27017
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 27017); };
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

    // ── The single in-flight command ────────────────────────────────
    d_phase: u8,
    /// Slot index owning the command.
    d_slot: u8,
    /// The request id to respond to.
    d_request: i32,
    /// The command body, copied out of the slot (the slot's recv
    /// buffer advances as soon as the message is consumed).
    cmd: [u8; codec::MAX_BSON],
    cmd_len: u16,
    /// Lower-cased command name.
    cmd_name: [u8; 32],
    cmd_name_len: u8,
    collection: [u8; NAME_MAX],
    collection_len: u8,
    collection_id: u32,
    /// find: scan cursor + reply staging.
    cursor: u64,
    reply: [u8; SEND_BUF],
    reply_used: u16,
    reply_docs: u16,

    kv_corr: u64,
    env: [u8; SCRATCH_BUF],
    scratch: [u8; SCRATCH_BUF],

    m_sessions: u64,
    m_commands: u64,
    m_docs_written: u64,
    m_docs_returned: u64,
    m_errors: u64,
    step_ctr: u64,
    /// dev_millis deadline for the in-flight command (0 = none). A lost
    /// KV reply otherwise latches `d_phase` busy forever — every later
    /// command then gets the busy refusal until reboot. Mirrors the
    /// relational executor's statement watchdog.
    cmd_deadline_ms: u64,
    m_cmd_timeouts: u64,
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
        self.d_request = 0;
        self.cmd = [0; codec::MAX_BSON];
        self.cmd_len = 0;
        self.cmd_name = [0; 32];
        self.cmd_name_len = 0;
        self.collection = [0; NAME_MAX];
        self.collection_len = 0;
        self.collection_id = 0;
        self.cursor = 0;
        self.reply = [0; SEND_BUF];
        self.reply_used = 0;
        self.reply_docs = 0;
        self.kv_corr = 0;
        self.env = [0; SCRATCH_BUF];
        self.scratch = [0; SCRATCH_BUF];
        self.m_sessions = 0;
        self.m_commands = 0;
        self.m_docs_written = 0;
        self.m_docs_returned = 0;
        self.m_errors = 0;
        self.step_ctr = 0;
        self.cmd_deadline_ms = 0;
        self.m_cmd_timeouts = 0;
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
        // A dying slot that owns the in-flight command abandons it; the
        // KV reply is dropped by slot check on arrival.
        if self.d_phase != D_IDLE && self.d_slot as usize == idx {
            self.d_phase = D_IDLE;
        }
        self.slots[idx] = Slot::free();
    }
}

// ── Net plumbing (pg_edge_anchor pattern) ────────────────────────────

unsafe fn net_send(anchor: &mut AnchorState, cmd: u8, conn_id: u16, data: &[u8]) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let payload_len = CONN_ID_LEN + data.len();
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
            scratch.add(NET_FRAME_HDR + CONN_ID_LEN),
            data.len(),
        );
    }
    let total = NET_FRAME_HDR + payload_len;
    ((*sys).channel_write)(anchor.net_out, scratch, total) == total as i32
}

unsafe fn net_bind(anchor: &mut AnchorState) -> bool {
    // BIND's payload is the two port bytes ALONE — no conn byte
    // (there is no connection yet). `net_send` prepends one, so this
    // frame is built directly.
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

/// Queue reply bytes on the slot and flush.
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
    // Positional pair #0: see the module header. The byte routes the
    // REPLY to the port this module is wired to; nothing etcd-shaped
    // is implied or consumed.
    anchor.env[at + 8] = PROTO_ETCD;
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

fn seq_key(out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 5];
    body[0..4].copy_from_slice(&DATABASE_ID.to_be_bytes());
    body[4] = CAT_KIND_SEQ;
    user_key(out, models::KS_DOCUMENT_CATALOG, &body)
}

fn name_key(out: &mut [u8], name: &[u8]) -> Option<usize> {
    if name.is_empty() || name.len() > NAME_MAX {
        return None;
    }
    let mut body = [0u8; 5 + NAME_MAX];
    body[0..4].copy_from_slice(&DATABASE_ID.to_be_bytes());
    body[4] = CAT_KIND_NAME;
    body[5..5 + name.len()].copy_from_slice(name);
    user_key(out, models::KS_DOCUMENT_CATALOG, &body[..5 + name.len()])
}

fn doc_key(out: &mut [u8], collection_id: u32, id: &[u8]) -> Option<usize> {
    let mut body = [0u8; KEY_MAX];
    let n = models::encode_document_data_key(&mut body, collection_id, b"", id)?;
    user_key(out, models::KS_DOCUMENT_DATA, &body[..n])
}

fn collection_bounds(
    collection_id: u32,
    start: &mut [u8],
    end: &mut [u8],
) -> Option<(usize, usize)> {
    let mut body = [0u8; KEY_MAX];
    let n = models::encode_document_data_prefix(&mut body, collection_id, None)?;
    let sn = user_key(start, models::KS_DOCUMENT_DATA, &body[..n])?;
    let en = prefix_successor(&start[..sn], end)?;
    Some((sn, en))
}

/// `[keyspace:u32 BE][body…]` — the same user-key composition the SQL
/// side uses (`sql_exec::kv_key`), duplicated here because this module
/// must not mount the SQL tree to borrow four lines.
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

// ── Command replies ──────────────────────────────────────────────────

fn reply_body(anchor: &mut AnchorState, body_len: usize) {
    let idx = anchor.d_slot as usize;
    let request = anchor.d_request;
    let mut body = [0u8; SEND_BUF];
    body[..body_len].copy_from_slice(&anchor.reply[..body_len]);
    let mut msg = [0u8; SEND_BUF];
    let Some(n) = codec::frame_reply(request, &body[..body_len], &mut msg) else {
        anchor.slots[idx].state = S_CLOSING;
        anchor.d_phase = D_IDLE;
        return;
    };
    anchor.d_phase = D_IDLE;
    if anchor.slots[idx].state == S_BUSY {
        anchor.slots[idx].state = S_READY;
    }
    send_to_slot(anchor, idx, &msg[..n]);
}

fn reply_error(anchor: &mut AnchorState, code: i32, msg: &[u8]) {
    anchor.m_errors = anchor.m_errors.wrapping_add(1);
    let mut body = [0u8; 256];
    let Some(n) = codec::error_body(code, msg, &mut body) else {
        anchor.d_phase = D_IDLE;
        return;
    };
    anchor.reply[..n].copy_from_slice(&body[..n]);
    reply_body(anchor, n);
}

// ── Command dispatch ─────────────────────────────────────────────────

/// A complete OP_MSG arrived on `idx`; run it (or queue-refuse).
fn begin_command(anchor: &mut AnchorState, idx: usize, request_id: i32, body: &[u8]) {
    anchor.m_commands = anchor.m_commands.wrapping_add(1);
    let mut name = [0u8; 32];
    let Some(nl) = codec::command_name(body, &mut name) else {
        anchor.slots[idx].state = S_CLOSING;
        return;
    };
    let cmd = &name[..nl];

    // Stateless commands answer inline, in any state.
    let inline: Option<usize> = match cmd {
        b"hello" | b"ismaster" => {
            let mut b = [0u8; 512];
            codec::hello_body(&mut b).inspect(|&n| {
                anchor.reply[..n].copy_from_slice(&b[..n]);
            })
        }
        b"ping" | b"buildinfo" | b"getparameter" | b"endsessions" => {
            let mut b = [0u8; 64];
            codec::ok_body(&mut b).inspect(|&n| {
                anchor.reply[..n].copy_from_slice(&b[..n]);
            })
        }
        _ => None,
    };
    if let Some(n) = inline {
        let saved_slot = anchor.d_slot;
        let saved_req = anchor.d_request;
        let saved_phase = anchor.d_phase;
        anchor.d_slot = idx as u8;
        anchor.d_request = request_id;
        anchor.d_phase = D_IDLE;
        reply_body(anchor, n);
        // reply_body cleared d_phase; restore the in-flight command's
        // ownership if one was running for ANOTHER slot.
        if saved_phase != D_IDLE && saved_slot as usize != idx {
            anchor.d_slot = saved_slot;
            anchor.d_request = saved_req;
            anchor.d_phase = saved_phase;
        }
        return;
    }

    // Stateful commands: one in flight, whole-anchor, like the SQL
    // executor. A second one gets a retryable server-busy error.
    //
    // Do NOT touch `d_slot`/`d_request` here: they belong to the
    // in-flight command, and its KV reply is routed through them — a
    // busy refusal that overwrote them would strand the owning
    // connection reply-less. The busy reply below frames the refused
    // request's id directly and leaves the in-flight routing alone.
    if anchor.d_phase != D_IDLE {
        let mut b = [0u8; 128];
        if let Some(n) = codec::error_body(50, b"server busy, retry", &mut b) {
            anchor.reply[..n].copy_from_slice(&b[..n]);
            let mut msg = [0u8; 512];
            if let Some(m) = codec::frame_reply(request_id, &b[..n], &mut msg) {
                send_to_slot(anchor, idx, &msg[..m]);
            }
        }
        return;
    }

    if body.len() > anchor.cmd.len() {
        anchor.slots[idx].state = S_CLOSING;
        return;
    }
    anchor.cmd[..body.len()].copy_from_slice(body);
    anchor.cmd_len = body.len() as u16;
    anchor.cmd_name[..nl].copy_from_slice(cmd);
    anchor.cmd_name_len = nl as u8;
    anchor.d_slot = idx as u8;
    anchor.d_request = request_id;
    anchor.slots[idx].state = S_BUSY;
    // Command watchdog: see `cmd_deadline_ms`.
    anchor.cmd_deadline_ms = unsafe { dev_millis(&*anchor.syscalls) }.wrapping_add(10_000);

    match cmd {
        b"insert" | b"find" | b"delete" | b"update" => {
            // All need the collection id first.
            let collection = match codec::get_str(body, cmd) {
                Some(c) if !c.is_empty() && c.len() <= NAME_MAX => c,
                _ => {
                    reply_error(anchor, 73, b"invalid collection name");
                    return;
                }
            };
            let mut cname = [0u8; NAME_MAX];
            cname[..collection.len()].copy_from_slice(collection);
            anchor.collection[..collection.len()].copy_from_slice(collection);
            anchor.collection_len = collection.len() as u8;
            let mut key = [0u8; KEY_MAX];
            let Some(kn) = name_key(&mut key, &cname[..collection.len()]) else {
                reply_error(anchor, 73, b"invalid collection name");
                return;
            };
            let mut k = [0u8; KEY_MAX];
            k[..kn].copy_from_slice(&key[..kn]);
            let Some(bn) = stage_get(anchor, &k[..kn]) else {
                reply_error(anchor, 8, b"internal");
                return;
            };
            if !kv_send(anchor, KV_OP_GET, bn, D_CAT_GET) {
                reply_error(anchor, 8, b"internal");
            }
        }
        _ => {
            reply_error(
                anchor,
                59,
                b"no such command in this document capability slice",
            );
        }
    }
}

fn is_insert(anchor: &AnchorState) -> bool {
    &anchor.cmd_name[..anchor.cmd_name_len as usize] == b"insert"
}

fn is_delete(anchor: &AnchorState) -> bool {
    &anchor.cmd_name[..anchor.cmd_name_len as usize] == b"delete"
}

fn is_update(anchor: &AnchorState) -> bool {
    &anchor.cmd_name[..anchor.cmd_name_len as usize] == b"update"
}

/// Stage a single-key `KV_OP_DELETE` body: `[count:u16=1][klen:u16][key]`,
/// the multi-key delete format the KV worker expects.
fn stage_delete(anchor: &mut AnchorState, key: &[u8]) -> Option<usize> {
    let need = 2 + 2 + key.len();
    if BODY_AT + need > anchor.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes());
    p += 2;
    anchor.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + key.len()].copy_from_slice(key);
    Some(need)
}

/// Extract the `_id` named by the first element of a `deletes`/`updates`
/// array — the `{q: {_id: v}}` filter this slice supports. `out` receives
/// the encoded id; returns its length, or `None` for any richer filter.
fn first_spec_id(cmd: &[u8], array_name: &[u8], out: &mut [u8]) -> Option<usize> {
    let arr = codec::get_doc(cmd, array_name)?;
    let mut id_len = None;
    let mut idbuf = [0u8; ID_MAX];
    codec::each_array_doc(arr, |spec| {
        // First spec only: this slice does not batch multi-statement writes.
        let Some(q) = codec::get_doc(spec, b"q") else {
            return false;
        };
        id_len = codec::extract_id(q, &mut idbuf);
        false // stop after the first
    });
    let n = id_len?;
    out[..n].copy_from_slice(&idbuf[..n]);
    Some(n)
}

/// Delete one document by `{_id}`. Refuses any richer filter by name.
fn send_delete(anchor: &mut AnchorState) {
    let cmd_len = anchor.cmd_len as usize;
    let mut cmd = [0u8; codec::MAX_BSON];
    cmd[..cmd_len].copy_from_slice(&anchor.cmd[..cmd_len]);
    let mut id = [0u8; ID_MAX];
    let Some(idn) = first_spec_id(&cmd[..cmd_len], b"deletes", &mut id) else {
        reply_error(anchor, 2, b"only {q: {_id: v}} deletes in this slice");
        return;
    };
    let mut key = [0u8; KEY_MAX];
    let Some(kn) = doc_key(&mut key, anchor.collection_id, &id[..idn]) else {
        reply_error(anchor, 2, b"unsupported _id");
        return;
    };
    let mut k = [0u8; KEY_MAX];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_delete(anchor, &k[..kn]) else {
        reply_error(anchor, 8, b"internal");
        return;
    };
    if !kv_send(anchor, KV_OP_DELETE, bn, D_DELETE) {
        reply_error(anchor, 8, b"internal");
    }
}

/// Update one document by `{_id}`, replacing it with the spec's `u` doc.
/// Reads first so a miss reports `n: 0` instead of upserting.
fn send_update(anchor: &mut AnchorState) {
    let cmd_len = anchor.cmd_len as usize;
    let mut cmd = [0u8; codec::MAX_BSON];
    cmd[..cmd_len].copy_from_slice(&anchor.cmd[..cmd_len]);
    let mut id = [0u8; ID_MAX];
    let Some(idn) = first_spec_id(&cmd[..cmd_len], b"updates", &mut id) else {
        reply_error(anchor, 2, b"only {q: {_id: v}} updates in this slice");
        return;
    };
    let mut key = [0u8; KEY_MAX];
    let Some(kn) = doc_key(&mut key, anchor.collection_id, &id[..idn]) else {
        reply_error(anchor, 2, b"unsupported _id");
        return;
    };
    let mut k = [0u8; KEY_MAX];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_get(anchor, &k[..kn]) else {
        reply_error(anchor, 8, b"internal");
        return;
    };
    if !kv_send(anchor, KV_OP_GET, bn, D_UPD_READ) {
        reply_error(anchor, 8, b"internal");
    }
}

/// The update's read is back and matched: PUT the replacement `u` doc.
/// The update's document exists (`fetched`): write the new value. A `u`
/// with `$set`/`$inc` modifies `fetched`; a plain `u` replaces it wholesale.
fn send_update_put(anchor: &mut AnchorState, fetched: &[u8]) {
    let cmd_len = anchor.cmd_len as usize;
    let mut cmd = [0u8; codec::MAX_BSON];
    cmd[..cmd_len].copy_from_slice(&anchor.cmd[..cmd_len]);
    let mut fetch = [0u8; codec::MAX_BSON];
    let fl = fetched.len().min(fetch.len());
    fetch[..fl].copy_from_slice(&fetched[..fl]);
    let mut id = [0u8; ID_MAX];
    let mut idn = None;
    let mut uval = [0u8; codec::MAX_BSON];
    let mut ul = None;
    if let Some(arr) = codec::get_doc(&cmd[..cmd_len], b"updates") {
        codec::each_array_doc(arr, |spec| {
            if let Some(q) = codec::get_doc(spec, b"q") {
                idn = codec::extract_id(q, &mut id);
            }
            if let Some(u) = codec::get_doc(spec, b"u") {
                // Operator update ($set/$inc) rewrites the fetched doc;
                // a plain document is a wholesale replacement.
                let set = codec::get_doc(u, b"$set");
                let inc = codec::get_doc(u, b"$inc");
                if set.is_some() || inc.is_some() {
                    if let Some(n) = codec::apply_update_ops(&fetch[..fl], set, inc, &mut uval) {
                        ul = Some(n);
                    }
                } else if u.len() <= uval.len() {
                    uval[..u.len()].copy_from_slice(u);
                    ul = Some(u.len());
                }
            }
            false // first spec only
        });
    }
    let (Some(idn), Some(ul)) = (idn, ul) else {
        reply_error(anchor, 9, b"update spec needs {q: {_id}} and a valid u");
        return;
    };
    let mut key = [0u8; KEY_MAX];
    let Some(kn) = doc_key(&mut key, anchor.collection_id, &id[..idn]) else {
        reply_error(anchor, 2, b"unsupported _id");
        return;
    };
    let mut k = [0u8; KEY_MAX];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_put(anchor, &k[..kn], &uval[..ul], 0) else {
        reply_error(anchor, 8, b"internal");
        return;
    };
    if !kv_send(anchor, KV_OP_PUT, bn, D_UPDATE) {
        reply_error(anchor, 8, b"internal");
    }
}

/// Build + send the insert TXN from the staged command.
fn send_insert_txn(anchor: &mut AnchorState) {
    let cmd_len = anchor.cmd_len as usize;
    let mut cmd = [0u8; codec::MAX_BSON];
    cmd[..cmd_len].copy_from_slice(&anchor.cmd[..cmd_len]);
    let Some(arr) = codec::get_doc(&cmd[..cmd_len], b"documents") else {
        reply_error(anchor, 9, b"insert needs a documents array");
        return;
    };
    // Pass 1: count + validate ids. Pass 2: build the TXN body.
    let mut count: u16 = 0;
    let mut ok = true;
    codec::each_array_doc(arr, |d| {
        let mut id = [0u8; ID_MAX];
        if codec::extract_id(d, &mut id).is_none() {
            ok = false;
            return false;
        }
        count += 1;
        true
    });
    if !ok || count == 0 {
        reply_error(anchor, 2, b"every document needs a supported _id");
        return;
    }

    let collection_id = anchor.collection_id;
    let end_guard = BODY_AT + 3800;
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&count.to_le_bytes());
    p += 2;
    let mut fail = false;
    codec::each_array_doc(arr, |d| {
        let mut id = [0u8; ID_MAX];
        let Some(idn) = codec::extract_id(d, &mut id) else {
            fail = true;
            return false;
        };
        let mut key = [0u8; KEY_MAX];
        let Some(kn) = doc_key(&mut key, collection_id, &id[..idn]) else {
            fail = true;
            return false;
        };
        if p + 1 + 2 + kn + 8 > end_guard {
            fail = true;
            return false;
        }
        anchor.env[p] = TXN_CMP_MOD_EQUAL;
        p += 1;
        anchor.env[p..p + 2].copy_from_slice(&(kn as u16).to_le_bytes());
        p += 2;
        anchor.env[p..p + kn].copy_from_slice(&key[..kn]);
        p += kn;
        anchor.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
        p += 8;
        true
    });
    if !fail {
        anchor.env[p..p + 2].copy_from_slice(&count.to_le_bytes());
        p += 2;
        codec::each_array_doc(arr, |d| {
            let mut id = [0u8; ID_MAX];
            let Some(idn) = codec::extract_id(d, &mut id) else {
                fail = true;
                return false;
            };
            let mut key = [0u8; KEY_MAX];
            let Some(kn) = doc_key(&mut key, collection_id, &id[..idn]) else {
                fail = true;
                return false;
            };
            let put_len = 2 + kn + 4 + d.len() + 1 + 8;
            if p + 1 + 2 + put_len > end_guard {
                fail = true;
                return false;
            }
            anchor.env[p] = KV_OP_PUT;
            p += 1;
            anchor.env[p..p + 2].copy_from_slice(&(put_len as u16).to_le_bytes());
            p += 2;
            anchor.env[p..p + 2].copy_from_slice(&(kn as u16).to_le_bytes());
            p += 2;
            anchor.env[p..p + kn].copy_from_slice(&key[..kn]);
            p += kn;
            anchor.env[p..p + 4].copy_from_slice(&(d.len() as u32).to_le_bytes());
            p += 4;
            for (i, &b) in d.iter().enumerate() {
                anchor.env[p + i] = b;
            }
            p += d.len();
            anchor.env[p] = 0;
            p += 1;
            anchor.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
            p += 8;
            true
        });
    }
    if fail {
        reply_error(anchor, 10334, b"insert too large for one atomic command");
        return;
    }
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
    p += 2;
    anchor.reply_docs = count;
    if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_INSERT) {
        reply_error(anchor, 8, b"internal");
    }
}

/// Start the find: point read on `{_id: v}`, scan otherwise; refuse
/// any other filter by name.
fn send_find(anchor: &mut AnchorState) {
    let cmd_len = anchor.cmd_len as usize;
    let mut cmd = [0u8; codec::MAX_BSON];
    cmd[..cmd_len].copy_from_slice(&anchor.cmd[..cmd_len]);
    let filter = codec::get_doc(&cmd[..cmd_len], b"filter");
    let empty = match filter {
        None => true,
        Some(f) => f.len() <= 5,
    };
    if empty {
        anchor.cursor = 0;
        anchor.reply_used = 0;
        anchor.reply_docs = 0;
        send_find_scan(anchor);
        return;
    }
    let f = filter.unwrap_or(&[]);
    // Exactly `{_id: v}` — a point read on the key. A crude length check
    // confirms the filter is JUST the id element and nothing else.
    let mut id = [0u8; ID_MAX];
    if let Some(idn) = codec::extract_id(f, &mut id) {
        let expected = 4 + 1 + 4 + idn.saturating_sub(1) + 1; // len + ty + "_id\0" + value + term
        if f.len() == expected {
            let mut key = [0u8; KEY_MAX];
            let Some(kn) = doc_key(&mut key, anchor.collection_id, &id[..idn]) else {
                reply_error(anchor, 2, b"unsupported _id");
                return;
            };
            let mut k = [0u8; KEY_MAX];
            k[..kn].copy_from_slice(&key[..kn]);
            let Some(bn) = stage_get(anchor, &k[..kn]) else {
                reply_error(anchor, 8, b"internal");
                return;
            };
            if !kv_send(anchor, KV_OP_GET, bn, D_FIND_GET) {
                reply_error(anchor, 8, b"internal");
            }
            return;
        }
    }
    // Any other filter is a field query: scan the collection and keep the
    // documents that match, provided every operator is one we evaluate.
    if !codec::filter_is_supported(f) {
        reply_error(anchor, 2, b"unsupported query operator in this slice");
        return;
    }
    anchor.cursor = 0;
    anchor.reply_used = 0;
    anchor.reply_docs = 0;
    send_find_scan(anchor);
}

fn send_find_scan(anchor: &mut AnchorState) {
    let mut start = [0u8; KEY_MAX];
    let mut end = [0u8; KEY_MAX];
    let Some((sn, en)) = collection_bounds(anchor.collection_id, &mut start, &mut end) else {
        reply_error(anchor, 8, b"internal");
        return;
    };
    let cursor = anchor.cursor;
    let need = 2 + sn + 2 + en + 10;
    if BODY_AT + need > anchor.env.len() {
        reply_error(anchor, 8, b"internal");
        return;
    }
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&(sn as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + sn].copy_from_slice(&start[..sn]);
    p += sn;
    anchor.env[p..p + 2].copy_from_slice(&(en as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + en].copy_from_slice(&end[..en]);
    p += en;
    anchor.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    anchor.env[p..p + 2].copy_from_slice(&8u16.to_le_bytes());
    if !kv_send(anchor, KV_OP_RANGE_SCAN, need, D_FIND_SCAN) {
        reply_error(anchor, 8, b"internal");
    }
}

/// Assemble the staged find docs into a cursor reply.
fn finish_find(anchor: &mut AnchorState) {
    let used = anchor.reply_used as usize;
    let mut staged = [0u8; SEND_BUF];
    staged[..used].copy_from_slice(&anchor.reply[..used]);
    let mut body = [0u8; SEND_BUF];
    let Some(mut fr) = codec::FindReply::new(&mut body) else {
        reply_error(anchor, 8, b"internal");
        return;
    };
    let mut at = 0usize;
    while at + 4 <= used {
        let dlen = i32::from_le_bytes([staged[at], staged[at + 1], staged[at + 2], staged[at + 3]])
            as usize;
        if dlen < 5 || at + dlen > used {
            break;
        }
        if fr.push(&staged[at..at + dlen]).is_none() {
            reply_error(anchor, 10334, b"result exceeds one batch");
            return;
        }
        at += dlen;
    }
    // ns: "lattice.<collection>"
    let clen = anchor.collection_len as usize;
    let mut ns = [0u8; 8 + NAME_MAX];
    ns[..8].copy_from_slice(b"lattice.");
    ns[8..8 + clen].copy_from_slice(&anchor.collection[..clen]);
    let Some(n) = fr.finish(&ns[..8 + clen]) else {
        reply_error(anchor, 8, b"internal");
        return;
    };
    anchor.m_docs_returned = anchor
        .m_docs_returned
        .wrapping_add(u64::from(anchor.reply_docs));
    anchor.reply[..n].copy_from_slice(&body[..n]);
    reply_body(anchor, n);
}

// ── KV response handling ─────────────────────────────────────────────

fn on_kv_response(anchor: &mut AnchorState, result: u8, body: &[u8]) {
    match anchor.d_phase {
        D_CAT_GET => match result {
            KV_RESULT_OK if body.len() >= 4 => {
                anchor.collection_id = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                if is_insert(anchor) {
                    send_insert_txn(anchor);
                } else if is_delete(anchor) {
                    send_delete(anchor);
                } else if is_update(anchor) {
                    send_update(anchor);
                } else {
                    send_find(anchor);
                }
            }
            KV_RESULT_NOT_FOUND => {
                if is_delete(anchor) {
                    // Nothing to delete in a collection that does not exist.
                    let mut b = [0u8; 32];
                    match codec::delete_body(0, &mut b) {
                        Some(bl) => {
                            anchor.reply[..bl].copy_from_slice(&b[..bl]);
                            reply_body(anchor, bl);
                        }
                        None => reply_error(anchor, 8, b"internal"),
                    }
                } else if is_update(anchor) {
                    // No document matched: n:0, and no upsert.
                    let mut b = [0u8; 48];
                    match codec::update_body(0, 0, &mut b) {
                        Some(bl) => {
                            anchor.reply[..bl].copy_from_slice(&b[..bl]);
                            reply_body(anchor, bl);
                        }
                        None => reply_error(anchor, 8, b"internal"),
                    }
                } else if is_insert(anchor) {
                    // Auto-create: allocate an id.
                    let mut key = [0u8; KEY_MAX];
                    let Some(kn) = seq_key(&mut key) else {
                        reply_error(anchor, 8, b"internal");
                        return;
                    };
                    let mut k = [0u8; KEY_MAX];
                    k[..kn].copy_from_slice(&key[..kn]);
                    let Some(bn) = stage_incr(anchor, &k[..kn]) else {
                        reply_error(anchor, 8, b"internal");
                        return;
                    };
                    if !kv_send(anchor, KV_OP_INCR, bn, D_CAT_ALLOC) {
                        reply_error(anchor, 8, b"internal");
                    }
                } else {
                    // find on a missing collection: empty batch, like
                    // MongoDB.
                    anchor.reply_used = 0;
                    anchor.reply_docs = 0;
                    finish_find(anchor);
                }
            }
            _ => reply_error(anchor, 8, b"store unavailable"),
        },
        D_CAT_ALLOC => {
            if result != KV_RESULT_INTEGER || body.len() < 8 {
                reply_error(anchor, 8, b"store unavailable");
                return;
            }
            let counter = i64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            if counter <= 0 || counter > u32::MAX as i64 {
                reply_error(anchor, 8, b"store unavailable");
                return;
            }
            anchor.collection_id = counter as u32;
            let clen = anchor.collection_len as usize;
            let mut cname = [0u8; NAME_MAX];
            cname[..clen].copy_from_slice(&anchor.collection[..clen]);
            let mut key = [0u8; KEY_MAX];
            let Some(kn) = name_key(&mut key, &cname[..clen]) else {
                reply_error(anchor, 8, b"internal");
                return;
            };
            let mut k = [0u8; KEY_MAX];
            k[..kn].copy_from_slice(&key[..kn]);
            let id_be = anchor.collection_id.to_be_bytes();
            let Some(bn) = stage_put(anchor, &k[..kn], &id_be, PUT_FLAG_NX) else {
                reply_error(anchor, 8, b"internal");
                return;
            };
            if !kv_send(anchor, KV_OP_PUT, bn, D_CAT_PUT) {
                reply_error(anchor, 8, b"internal");
            }
        }
        D_CAT_PUT => match result {
            KV_RESULT_OK => send_insert_txn(anchor),
            // Lost a create race (single-executor today, but the code
            // must not assume that): re-read the winner's id.
            KV_RESULT_CAS_FAILED | KV_RESULT_EXISTS => {
                let clen = anchor.collection_len as usize;
                let mut cname = [0u8; NAME_MAX];
                cname[..clen].copy_from_slice(&anchor.collection[..clen]);
                let mut key = [0u8; KEY_MAX];
                let Some(kn) = name_key(&mut key, &cname[..clen]) else {
                    reply_error(anchor, 8, b"internal");
                    return;
                };
                let mut k = [0u8; KEY_MAX];
                k[..kn].copy_from_slice(&key[..kn]);
                let Some(bn) = stage_get(anchor, &k[..kn]) else {
                    reply_error(anchor, 8, b"internal");
                    return;
                };
                if !kv_send(anchor, KV_OP_GET, bn, D_CAT_GET) {
                    reply_error(anchor, 8, b"internal");
                }
            }
            _ => reply_error(anchor, 8, b"store unavailable"),
        },
        D_INSERT => match result {
            KV_RESULT_TXN if !body.is_empty() => {
                if body[0] == 1 {
                    let n = anchor.reply_docs;
                    anchor.m_docs_written = anchor.m_docs_written.wrapping_add(u64::from(n));
                    let mut b = [0u8; 64];
                    if let Some(bl) = codec::insert_body(i32::from(n), &mut b) {
                        anchor.reply[..bl].copy_from_slice(&b[..bl]);
                        reply_body(anchor, bl);
                    } else {
                        reply_error(anchor, 8, b"internal");
                    }
                } else {
                    reply_error(anchor, 11000, b"duplicate _id; nothing was written");
                }
            }
            _ => reply_error(anchor, 8, b"store unavailable"),
        },
        D_FIND_GET => match result {
            KV_RESULT_OK => {
                let used = body.len().min(SEND_BUF);
                anchor.reply[..used].copy_from_slice(&body[..used]);
                anchor.reply_used = used as u16;
                anchor.reply_docs = 1;
                finish_find(anchor);
            }
            KV_RESULT_NOT_FOUND => {
                anchor.reply_used = 0;
                anchor.reply_docs = 0;
                finish_find(anchor);
            }
            _ => reply_error(anchor, 8, b"store unavailable"),
        },
        D_FIND_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                reply_error(anchor, 8, b"store unavailable");
                return;
            }
            anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            // The field filter (if any) is re-read from the stashed command
            // and applied to each scanned document.
            let cmd_len = anchor.cmd_len as usize;
            let mut cmd = [0u8; codec::MAX_BSON];
            cmd[..cmd_len].copy_from_slice(&anchor.cmd[..cmd_len]);
            let filter = codec::get_doc(&cmd[..cmd_len], b"filter").unwrap_or(&[]);
            let has_filter = filter.len() > 5; // an empty doc is 5 bytes
            let mut at = 10usize;
            for _ in 0..count {
                if body.len() < at + 2 {
                    reply_error(anchor, 8, b"page corrupt");
                    return;
                }
                let klen = u16::from_le_bytes([body[at], body[at + 1]]) as usize;
                let voff = at + 2 + klen;
                if body.len() < voff + 4 {
                    reply_error(anchor, 8, b"page corrupt");
                    return;
                }
                let vlen =
                    u32::from_le_bytes(body[voff..voff + 4].try_into().unwrap_or([0; 4])) as usize;
                let vend = voff + 4 + vlen;
                if body.len() < vend {
                    reply_error(anchor, 8, b"page corrupt");
                    return;
                }
                // Keep only documents the field filter matches.
                if has_filter && !codec::doc_matches_filter(&body[voff + 4..vend], filter) {
                    at = vend;
                    continue;
                }
                let used = anchor.reply_used as usize;
                if used + vlen > SEND_BUF {
                    reply_error(anchor, 10334, b"result exceeds one batch");
                    return;
                }
                anchor.reply[used..used + vlen].copy_from_slice(&body[voff + 4..vend]);
                anchor.reply_used = (used + vlen) as u16;
                anchor.reply_docs += 1;
                at = vend;
            }
            if anchor.cursor != 0 {
                send_find_scan(anchor);
            } else {
                finish_find(anchor);
            }
        }
        D_DELETE => {
            // The store reports how many keys it removed (0 or 1 here).
            let n = match result {
                KV_RESULT_INTEGER if body.len() >= 8 => {
                    u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8])) as i32
                }
                KV_RESULT_OK => 1,
                KV_RESULT_INTEGER => 0,
                _ => {
                    reply_error(anchor, 8, b"store unavailable");
                    return;
                }
            };
            anchor.m_docs_written = anchor.m_docs_written.wrapping_add(n.max(0) as u64);
            let mut b = [0u8; 32];
            match codec::delete_body(n, &mut b) {
                Some(bl) => {
                    anchor.reply[..bl].copy_from_slice(&b[..bl]);
                    reply_body(anchor, bl);
                }
                None => reply_error(anchor, 8, b"internal"),
            }
        }
        D_UPD_READ => match result {
            // The document exists: replace or modify it. A miss is n:0.
            KV_RESULT_OK => send_update_put(anchor, body),
            KV_RESULT_NOT_FOUND => {
                let mut b = [0u8; 48];
                match codec::update_body(0, 0, &mut b) {
                    Some(bl) => {
                        anchor.reply[..bl].copy_from_slice(&b[..bl]);
                        reply_body(anchor, bl);
                    }
                    None => reply_error(anchor, 8, b"internal"),
                }
            }
            _ => reply_error(anchor, 8, b"store unavailable"),
        },
        D_UPDATE => match result {
            KV_RESULT_OK => {
                anchor.m_docs_written = anchor.m_docs_written.wrapping_add(1);
                let mut b = [0u8; 48];
                match codec::update_body(1, 1, &mut b) {
                    Some(bl) => {
                        anchor.reply[..bl].copy_from_slice(&b[..bl]);
                        reply_body(anchor, bl);
                    }
                    None => reply_error(anchor, 8, b"internal"),
                }
            }
            _ => reply_error(anchor, 8, b"store unavailable"),
        },
        _ => {}
    }
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
        match codec::parse_op_msg(&buf[..len]) {
            Ok(None) => return,
            Err(()) => {
                anchor.slots[idx].state = S_CLOSING;
                return;
            }
            Ok(Some((req, used))) => {
                let request_id = req.request_id;
                let mut body = [0u8; codec::MAX_BSON];
                let blen = req.body.len();
                body[..blen].copy_from_slice(req.body);
                let slot = &mut anchor.slots[idx];
                slot.recv.copy_within(used..len, 0);
                slot.recv_len = len - used;
                begin_command(anchor, idx, request_id, &body[..blen]);
                if anchor.slots[idx].state != S_READY {
                    return; // busy or closing: stop consuming
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
                    anchor.server_conn_id = conn_id(payload);
                    anchor.phase = PHASE_LISTENING;
                }
            } else if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= CONN_ID_LEN {
                // Single-anchor provider (no port in payload): claim
                // the first BOUND we see.
                anchor.server_conn_id = conn_id(payload);
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
            if payload.len() >= CONN_ID_LEN {
                let new_id = conn_id(payload);
                if anchor.alloc_slot(new_id).is_none() {
                    let _ = net_send(anchor, NET_CMD_CLOSE, new_id, &[]);
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() > CONN_ID_LEN {
                let conn_id = conn_id(payload);
                let data = &payload[CONN_ID_LEN..];
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
            if payload.len() >= CONN_ID_LEN {
                let conn_id = conn_id(payload);
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
    // inputs: net_in[0], kv_in[1]; outputs: net_out[0], kv_out[1],
    // metrics[2].
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

    // Net events.
    unsafe {
        let sys = &*sys_ptr;
        for _ in 0..8 {
            let mut env = [0u8; SCRATCH_BUF];
            let Some((msg, payload)) = read_one_envelope(sys, anchor.net_in, &mut env) else {
                break;
            };
            dispatch_net(anchor, msg, payload);
        }
        // KV replies.
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

    // Sweep closing slots + retry sends + resume queued input.
    // Command watchdog: an in-flight command whose KV reply was lost
    // must not hold the whole anchor busy forever.
    if anchor.d_phase != D_IDLE {
        let now = unsafe { dev_millis(&*anchor.syscalls) };
        if anchor.cmd_deadline_ms != 0 && now > anchor.cmd_deadline_ms {
            anchor.m_cmd_timeouts = anchor.m_cmd_timeouts.wrapping_add(1);
            reply_error(anchor, 50, b"statement timeout");
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
                    anchor.m_commands,
                    anchor.m_docs_written,
                    anchor.m_docs_returned,
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
        // Drain the oversized payload to stay frame-aligned: leaving it
        // in the channel would desync every subsequent read.
        let mut left = payload_len;
        let mut sink = [0u8; 256];
        while left > 0 {
            let take = left.min(sink.len());
            if ((sys.channel_read)(chan, sink.as_mut_ptr(), take) as usize) < take {
                break;
            }
            left -= take;
        }
        return None;
    }
    if payload_len > 0
        && ((sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len) as usize) < payload_len
    {
        return None;
    }
    Some((hdr[0], &scratch[..payload_len]))
}
