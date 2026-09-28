//! doc_edge_anchor — the MongoDB dialect of the document model.
//!
//! A real MongoDB driver connects on 27017, runs the `hello` handshake,
//! and works with collections whose documents the shared
//! `document_executor` stores. This module is the dialect only: OP_MSG
//! framing (`doc_server_codec.rs`), and the translation of commands,
//! BSON, queries, updates and projections into canonical document
//! operations and back (`mongo_dialect.rs`). It holds no document
//! semantics and touches no KV key — the executor does both, for every
//! dialect alike, so MongoDB and Kubernetes cannot come to disagree about
//! what a collection holds.
//!
//! ## Commands
//!
//! | Command | Operation |
//! |---|---|
//! | `insert` | one `OP_INSERT` of every document: atomic, so a duplicate `_id` anywhere writes nothing and the command fails (11000); an `_id`-less document gets a minted ObjectId |
//! | `find` | `{_id: v}` → `OP_GET`; any other query → `OP_LIST` with its filter, projection and limit; batches past the first continue with `getMore` |
//! | `getMore`, `killCursors` | the open cursor's next page (its continuation), or its release |
//! | `update` | per statement: an operator update → `OP_UPDATE` (by `_id` or filter, `multi`, `upsert` with MongoDB's seed); a replacement → `OP_REPLACE` |
//! | `delete` | per statement: `OP_DELETE` by `_id` or by filter (`limit: 0` = every match) |
//! | `create`, `drop`, `listCollections` | the collection operations |
//! | `hello`, `isMaster`, `ping`, `buildInfo`, `getParameter`, `endSessions` | answered here |
//!
//! Collections are MongoDB's: `_id` identifies, nothing else is declared,
//! and the first write creates one (`FLAG_AUTO_CREATE`). Anything the
//! dialect cannot state canonically is refused by name, never
//! approximated (see `mongo_dialect.rs`).
//!
//! ## Batches
//!
//! A tree page renders as BSON up to [`BSON_EXPANSION`] times larger (an
//! array of nulls is the worst case), so every list asks the executor
//! for at most `reply room / BSON_EXPANSION` bytes of documents
//! (`FIELD_MAX_BYTES`). A batch therefore always renders whole; the
//! executor always answers at least one document, and one document
//! (4 KiB of tree at most) always fits.
//!
//! One command is in flight at a time (the executor serialises anyway);
//! a second answers a retryable busy error.

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

// One mount chain: the dialect mounts the OP_MSG codec and the document
// wire beneath it.
#[path = "../../common/mongo_dialect.rs"]
mod mongo;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use abi::contracts::net::net_proto::{
    conn_id, CMD_BIND as NET_CMD_BIND, CMD_CLOSE as NET_CMD_CLOSE, CMD_SEND as NET_CMD_SEND,
    CONN_ID_LEN, MAX_CMD_DATA, MSG_ACCEPTED as NET_MSG_ACCEPTED, MSG_BOUND as NET_MSG_BOUND,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_DATA as NET_MSG_DATA, MSG_ERROR as NET_MSG_ERROR,
};
use mongo::codec;
use mongo::codec::mongo_core::{bson, bson_bool, bson_close, bson_i32, bson_open, bson_str};
use mongo::doc_exec::doc_tree::{Value, Writer};
use mongo::doc_exec::*;
use mongo::Refusal;
use wire::{MSG_DOC_REQUEST, MSG_DOC_RESPONSE};

// ── Capacities ───────────────────────────────────────────────────────

const MAX_CONNS: usize = 8;
const RECV_BUF: usize = 8192;
/// A slot's unsent bytes, and the largest reply.
const SEND_BUF: usize = 24576;
/// The most reply bytes one `CMD_SEND` carries: the net contract's
/// `MAX_CMD_DATA`, and no more than one channel frame holds beside the
/// connection id — a frame past its channel can never be written.
const NET_CHUNK: usize = if MAX_CMD_DATA < wire::FRAME_PAYLOAD_MAX - CONN_ID_LEN {
    MAX_CMD_DATA
} else {
    wire::FRAME_PAYLOAD_MAX - CONN_ID_LEN
};
const SCRATCH_BUF: usize = 32768;
const SLOT_FREE: u16 = 0xFFFF;
const DEFAULT_LISTEN_PORT: u16 = 27017;
/// Longest collection name.
const NAME_MAX: usize = 64;
/// How much larger a tree page can render as BSON (see the module docs).
const BSON_EXPANSION: usize = 6;
/// Documents a list answers per page, at most.
const BATCH_DOCS: usize = 101;
/// Open cursors (`find` batches awaiting `getMore`).
const MAX_CURSORS: usize = 4;
/// The one database this dialect reports in namespaces.
const DB_NAME: &[u8] = b"lattice";

const S_READY: u8 = 0;
const S_BUSY: u8 = 1;
const S_CLOSING: u8 = 2;

const PHASE_INIT: u8 = 0;
const PHASE_WAIT_BOUND: u8 = 1;
const PHASE_LISTENING: u8 = 2;

/// What the in-flight executor request is for.
const D_IDLE: u8 = 0;
const D_INSERT: u8 = 1;
const D_FIND: u8 = 2;
const D_GET_MORE: u8 = 3;
const D_UPDATE: u8 = 4;
const D_DELETE: u8 = 5;
const D_CREATE: u8 = 6;
const D_DROP: u8 = 7;
const D_LIST_COLLECTIONS: u8 = 8;
/// `createIndexes`: one index at a time.
const D_CREATE_INDEXES: u8 = 9;
/// `listIndexes`: the collection's descriptor.
const D_LIST_INDEXES: u8 = 10;
/// `dropIndexes` by `*` or by key: the descriptor, to learn the names.
const D_DROP_INDEXES_LIST: u8 = 11;
/// `dropIndexes`: one index at a time.
const D_DROP_INDEX: u8 = 12;
/// Index names a `dropIndexes` drops.
const DROP_NAMES: usize = 16;

const CMD_TIMEOUT_MS: u64 = 12_000;

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

/// An open cursor: the list request it pages (without its
/// continuation), where it resumes, and how many documents it may still
/// answer.
#[repr(C)]
struct Cursor {
    id: i64,
    slot: u8,
    req_len: u16,
    req: [u8; DOC_REQUEST_MAX],
    cont_len: u16,
    cont: [u8; 16 + MAX_CONTINUE_KEY],
    remaining: u32,
    coll_len: u8,
    coll: [u8; NAME_MAX],
}

impl Cursor {
    const fn free() -> Self {
        Cursor {
            id: 0,
            slot: 0,
            req_len: 0,
            req: [0; DOC_REQUEST_MAX],
            cont_len: 0,
            cont: [0; 16 + MAX_CONTINUE_KEY],
            remaining: 0,
            coll_len: 0,
            coll: [0; NAME_MAX],
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
    exec_in: i32,
    exec_out: i32,
    metrics_out: i32,

    listen_port: u16,
    phase: u8,
    server_conn_id: u16,
    slots: [Slot; MAX_CONNS],
    cursors: [Cursor; MAX_CURSORS],
    next_cursor: i64,

    // ── The in-flight command ───────────────────────────────────────
    d_phase: u8,
    d_slot: u8,
    d_request: i32,
    /// The command body (the slot's buffer moves on).
    cmd: [u8; codec::MAX_BSON],
    cmd_len: u16,
    /// Update/delete: the statement in flight, and the running tallies.
    stmt: u16,
    n: i64,
    n_modified: i64,
    /// `upserted: [{index, _id}]`, staged as BSON array elements.
    upserted_len: u16,
    upserted_count: u16,
    upserted: [u8; 1024],
    /// `dropIndexes`: the names to drop, and how many.
    drop_names: [[u8; 64]; DROP_NAMES],
    drop_name_len: [u8; DROP_NAMES],
    drop_count: u8,
    /// The first write error: statement index, code, message.
    err_index: i32,
    err_code: i32,
    err_msg: &'static [u8],
    /// find: the limit left after this batch (0 = none), and whether the
    /// batch must be the only one.
    find_limit: u32,
    single_batch: bool,
    /// getMore: the cursor paged.
    d_cursor: u8,
    corr: u64,
    deadline_ms: u64,

    /// ObjectId minting: the process's random bytes and a counter.
    oid_random: [u8; 5],
    oid_counter: u32,

    req: [u8; DOC_REQUEST_MAX],
    reply: [u8; SEND_BUF],
    env: [u8; SCRATCH_BUF],
    scratch: [u8; SCRATCH_BUF],

    m_sessions: u64,
    m_commands: u64,
    m_docs_written: u64,
    m_docs_returned: u64,
    m_errors: u64,
    step_ctr: u64,
}

impl AnchorState {
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
        // A connection's cursors die with it.
        for c in self.cursors.iter_mut() {
            if c.id != 0 && c.slot as usize == idx {
                c.id = 0;
            }
        }
        self.slots[idx] = Slot::free();
    }
}

// ── Net plumbing ─────────────────────────────────────────────────────

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
    // A frame at a time; what the channel has no room for waits in the
    // slot for a later step.
    while anchor.slots[idx].send_len > 0 {
        let conn_id = anchor.slots[idx].conn_id;
        let n = anchor.slots[idx].send_len.min(NET_CHUNK);
        let mut buf = [0u8; NET_CHUNK];
        buf[..n].copy_from_slice(&anchor.slots[idx].send[..n]);
        if !unsafe { net_send(anchor, NET_CMD_SEND, conn_id, &buf[..n]) } {
            return;
        }
        let s = &mut anchor.slots[idx];
        let rest = s.send_len - n;
        let mut k = 0;
        while k < rest {
            s.send[k] = s.send[n + k];
            k += 1;
        }
        s.send_len = rest;
    }
}

// ── Replies ──────────────────────────────────────────────────────────

/// Frame `reply[..body_len]` to the in-flight command and end it.
fn reply_body(anchor: &mut AnchorState, body_len: usize) {
    let idx = anchor.d_slot as usize;
    let request = anchor.d_request;
    let mut msg = [0u8; SEND_BUF];
    let framed = codec::frame_reply(request, &anchor.reply[..body_len], &mut msg);
    anchor.d_phase = D_IDLE;
    let Some(n) = framed else {
        anchor.slots[idx].state = S_CLOSING;
        return;
    };
    if anchor.slots[idx].state == S_BUSY {
        anchor.slots[idx].state = S_READY;
    }
    send_to_slot(anchor, idx, &msg[..n]);
}

fn reply_error(anchor: &mut AnchorState, code: i32, msg: &[u8]) {
    anchor.m_errors = anchor.m_errors.wrapping_add(1);
    let mut body = [0u8; 512];
    let Some(n) = codec::error_body(code, msg, &mut body) else {
        anchor.d_phase = D_IDLE;
        return;
    };
    anchor.reply[..n].copy_from_slice(&body[..n]);
    reply_body(anchor, n);
}

fn reply_refusal(anchor: &mut AnchorState, r: Refusal) {
    reply_error(anchor, r.code, r.msg);
}

// ── Executor requests ────────────────────────────────────────────────

/// Start a request in `req` with its head.
fn req_begin(anchor: &mut AnchorState, op: u8, flags: u8) -> usize {
    anchor.corr = anchor.corr.wrapping_add(1).max(1);
    let corr = anchor.corr;
    let slot = anchor.d_slot;
    let r = &mut anchor.req;
    r[0..8].copy_from_slice(&corr.to_le_bytes());
    r[8] = DIALECT_MONGO;
    r[9..13].copy_from_slice(&0u32.to_le_bytes());
    r[13] = slot;
    r[14] = op;
    r[15] = flags;
    DOC_REQUEST_HEAD
}

/// Append a field; `None` when the request is full.
fn req_field(anchor: &mut AnchorState, at: &mut usize, tag: u8, parts: &[&[u8]]) -> Option<()> {
    let len: usize = parts.iter().map(|p| p.len()).sum();
    if *at + FIELD_HEAD + len > DOC_REQUEST_MAX {
        return None;
    }
    anchor.req[*at] = tag;
    anchor.req[*at + 1..*at + 5].copy_from_slice(&(len as u32).to_le_bytes());
    let mut p = *at + 5;
    for part in parts {
        anchor.req[p..p + part.len()].copy_from_slice(part);
        p += part.len();
    }
    *at = p;
    Some(())
}

/// Send the request in `req[..len]` and wait for its answer in `phase`.
fn req_send(anchor: &mut AnchorState, len: usize, phase: u8) {
    if len > DOC_REQUEST_MAX {
        reply_refusal(
            anchor,
            mongo::Refusal {
                code: mongo::CODE_BSON_TOO_LARGE,
                msg: mongo::msg_too_large(),
            },
        );
        return;
    }
    let at = wire::ENVELOPE_HDR;
    let (env, req) = (&mut anchor.env, &anchor.req);
    env[at..at + len].copy_from_slice(&req[..len]);
    let sent = unsafe {
        let sys = anchor.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                anchor.exec_out,
                MSG_DOC_REQUEST,
                len,
                &mut anchor.env,
            )
    };
    if !sent {
        reply_error(
            anchor,
            mongo::CODE_HOST_UNREACHABLE,
            b"the document executor is not reachable",
        );
        return;
    }
    anchor.d_phase = phase;
    anchor.deadline_ms = unsafe { dev_millis(&*anchor.syscalls) }.wrapping_add(CMD_TIMEOUT_MS);
}

/// The collection a command names (`{<cmd>: "<collection>"}`).
fn collection_of<'a>(body: &'a [u8], cmd: &[u8]) -> Option<&'a [u8]> {
    let mut it = mongo::elements(body).ok()?;
    let (_, ty, v) = it.next()?.ok()?;
    let _ = cmd;
    if ty != bson::STRING {
        return None;
    }
    let c = mongo::str_value(v)?;
    (!c.is_empty() && c.len() <= NAME_MAX && !mongo::doc_exec::doc_tree::has_nul(c)).then_some(c)
}

fn mint_object_id(anchor: &mut AnchorState) -> [u8; 12] {
    let secs = unsafe { dev_unix_millis(&*anchor.syscalls) / 1000 } as u32;
    anchor.oid_counter = anchor.oid_counter.wrapping_add(1) & 0x00FF_FFFF;
    mongo::object_id(secs, anchor.oid_random, anchor.oid_counter)
}

// ── Command dispatch ─────────────────────────────────────────────────

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
        let (saved_slot, saved_req, saved_phase) =
            (anchor.d_slot, anchor.d_request, anchor.d_phase);
        anchor.d_slot = idx as u8;
        anchor.d_request = request_id;
        reply_body(anchor, n);
        if saved_phase != D_IDLE {
            anchor.d_slot = saved_slot;
            anchor.d_request = saved_req;
            anchor.d_phase = saved_phase;
        }
        return;
    }
    if anchor.d_phase != D_IDLE {
        // One command in flight. The refusal frames this request's id
        // and leaves the in-flight command's routing alone.
        let mut b = [0u8; 128];
        if let Some(n) =
            codec::error_body(mongo::CODE_WRITE_CONFLICT, b"server busy, retry", &mut b)
        {
            let mut msg = [0u8; 256];
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
    anchor.d_slot = idx as u8;
    anchor.d_request = request_id;
    anchor.slots[idx].state = S_BUSY;
    anchor.stmt = 0;
    anchor.n = 0;
    anchor.n_modified = 0;
    anchor.upserted_len = 0;
    anchor.upserted_count = 0;
    anchor.err_index = -1;

    let mut c = [0u8; codec::MAX_BSON];
    let bl = body.len();
    c[..bl].copy_from_slice(body);
    let body = &c[..bl];
    match cmd {
        b"insert" => cmd_insert(anchor, body),
        b"find" => cmd_find(anchor, body),
        b"getmore" => cmd_get_more(anchor, body),
        b"killcursors" => cmd_kill_cursors(anchor, body),
        b"update" => next_update(anchor),
        b"delete" => next_delete(anchor),
        b"create" => cmd_collection(anchor, body, OP_COLL_CREATE, D_CREATE),
        b"drop" => cmd_collection(anchor, body, OP_COLL_DROP, D_DROP),
        b"createindexes" => next_create_index(anchor),
        b"listindexes" => cmd_collection(anchor, body, OP_COLL_GET, D_LIST_INDEXES),
        b"dropindexes" => cmd_drop_indexes(anchor, body),
        b"listcollections" => {
            let at = req_begin(anchor, OP_COLL_LIST, 0);
            req_send(anchor, at, D_LIST_COLLECTIONS);
        }
        _ => reply_error(anchor, mongo::CODE_COMMAND_NOT_FOUND, b"no such command"),
    }
}

fn cmd_collection(anchor: &mut AnchorState, body: &[u8], op: u8, phase: u8) {
    let Some(coll) = collection_of(body, b"") else {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    };
    let mut cb = [0u8; NAME_MAX];
    cb[..coll.len()].copy_from_slice(coll);
    let mut at = req_begin(anchor, op, 0);
    if req_field(anchor, &mut at, FIELD_COLLECTION, &[&cb[..coll.len()]]).is_none() {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    }
    req_send(anchor, at, phase);
}

fn cmd_insert(anchor: &mut AnchorState, body: &[u8]) {
    let Some(coll) = collection_of(body, b"insert") else {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    };
    let mut cb = [0u8; NAME_MAX];
    cb[..coll.len()].copy_from_slice(coll);
    let cl = coll.len();
    let docs = match mongo::find(body, b"documents") {
        Ok(Some((bson::ARRAY, v))) => v,
        Ok(_) => {
            reply_error(
                anchor,
                mongo::CODE_FAILED_TO_PARSE,
                b"insert needs a documents array",
            );
            return;
        }
        Err(e) => return reply_refusal(anchor, e.into()),
    };
    let mut at = req_begin(anchor, OP_INSERT, FLAG_AUTO_CREATE);
    if req_field(anchor, &mut at, FIELD_COLLECTION, &[&cb[..cl]]).is_none() {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    }
    let elems = match mongo::elements(docs) {
        Ok(e) => e,
        Err(e) => return reply_refusal(anchor, e.into()),
    };
    let mut count = 0;
    for e in elems {
        let (_, ty, d) = match e {
            Ok(x) => x,
            Err(e) => return reply_refusal(anchor, e.into()),
        };
        if ty != bson::DOC {
            reply_error(
                anchor,
                mongo::CODE_FAILED_TO_PARSE,
                b"a document to insert is not a document",
            );
            return;
        }
        let oid = mint_object_id(anchor);
        let mut tree = [0u8; codec::MAX_BSON * 2];
        let n = match mongo::insert_tree(d, &oid, &mut tree) {
            Ok(n) => n,
            Err(r) => return reply_refusal(anchor, r),
        };
        if req_field(anchor, &mut at, FIELD_DOC, &[&tree[..n]]).is_none() {
            reply_refusal(
                anchor,
                Refusal {
                    code: mongo::CODE_BSON_TOO_LARGE,
                    msg: mongo::msg_too_large(),
                },
            );
            return;
        }
        count += 1;
    }
    if count == 0 {
        reply_error(
            anchor,
            mongo::CODE_FAILED_TO_PARSE,
            b"insert needs at least one document",
        );
        return;
    }
    anchor.n = count;
    req_send(anchor, at, D_INSERT);
}

/// The document budget a list asks for: what renders into one reply.
fn list_budget() -> u32 {
    ((SEND_BUF - 1024) / BSON_EXPANSION) as u32
}

fn cmd_find(anchor: &mut AnchorState, body: &[u8]) {
    let Some(coll) = collection_of(body, b"find") else {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    };
    let mut cb = [0u8; NAME_MAX];
    cb[..coll.len()].copy_from_slice(coll);
    let cl = coll.len();
    let opt = |name: &[u8]| mongo::find(body, name).ok().flatten();
    // Options with no canonical meaning are refused, not ignored.
    if let Some((ty, v)) = opt(b"skip") {
        if mongo::int_value(ty, v).unwrap_or(1) != 0 {
            reply_error(
                anchor,
                mongo::CODE_BAD_VALUE,
                b"lattice: skip is not supported",
            );
            return;
        }
    }
    if let Some((bson::DOC, s)) = opt(b"sort") {
        let only_id = match mongo::elements(s) {
            Ok(mut it) => match (it.next(), it.next()) {
                (None, _) => true,
                (Some(Ok((b"_id", t, v))), None) => mongo::int_value(t, v) == Some(1),
                _ => false,
            },
            Err(_) => false,
        };
        if !only_id {
            reply_error(
                anchor,
                mongo::CODE_BAD_VALUE,
                b"lattice: sort other than {_id: 1} is not supported",
            );
            return;
        }
    }
    let limit = opt(b"limit")
        .and_then(|(t, v)| mongo::int_value(t, v))
        .unwrap_or(0);
    let batch = opt(b"batchSize")
        .and_then(|(t, v)| mongo::int_value(t, v))
        .unwrap_or(BATCH_DOCS as i64);
    anchor.single_batch =
        limit < 0 || opt(b"singleBatch").is_some_and(|(t, v)| mongo::truthy(t, v));
    let limit = limit.unsigned_abs().min(u32::MAX as u64) as u32;
    anchor.find_limit = limit;
    let filter = match opt(b"filter") {
        Some((bson::DOC, f)) => Some(f),
        _ => None,
    };
    let projection = match opt(b"projection") {
        Some((bson::DOC, p)) => Some(p),
        _ => None,
    };

    // A point read when the query names one `_id` exactly.
    let exact = match filter.map(mongo::exact_id) {
        Some(Ok(e)) => e,
        Some(Err(r)) => return reply_refusal(anchor, r),
        None => None,
    };
    let op = if exact.is_some() { OP_GET } else { OP_LIST };
    let mut at = req_begin(anchor, op, 0);
    if req_field(anchor, &mut at, FIELD_COLLECTION, &[&cb[..cl]]).is_none() {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    }
    let mut buf = [0u8; DOC_REQUEST_MAX];
    if let Some((ty, v)) = exact {
        let n = match mongo::value_tree_bytes(ty, v, &mut buf) {
            Ok(n) => n,
            Err(r) => return reply_refusal(anchor, r),
        };
        let _ = req_field(anchor, &mut at, FIELD_ID, &[&buf[..n]]);
    } else if let Some(f) = filter {
        let n = match mongo::translate_filter(f, &mut buf) {
            Ok(n) => n,
            Err(r) => return reply_refusal(anchor, r),
        };
        if req_field(anchor, &mut at, FIELD_FILTER, &[&buf[..n]]).is_none() {
            reply_refusal(
                anchor,
                Refusal {
                    code: mongo::CODE_BSON_TOO_LARGE,
                    msg: mongo::msg_too_large(),
                },
            );
            return;
        }
    }
    if let Some(p) = projection {
        match mongo::translate_projection(p, &mut buf) {
            Ok(Some(n)) => {
                let _ = req_field(anchor, &mut at, FIELD_PROJECTION, &[&buf[..n]]);
            }
            Ok(None) => {}
            Err(r) => return reply_refusal(anchor, r),
        }
    }
    if op == OP_LIST {
        let mut per = if batch > 0 {
            batch as u32
        } else {
            BATCH_DOCS as u32
        };
        if limit > 0 {
            per = per.min(limit);
        }
        let _ = req_field(anchor, &mut at, FIELD_LIMIT, &[&per.to_le_bytes()]);
        let _ = req_field(
            anchor,
            &mut at,
            FIELD_MAX_BYTES,
            &[&list_budget().to_le_bytes()],
        );
        // A cursor pages this request: keep it (without a continuation).
        if let Some(ci) = anchor.cursors.iter().position(|c| c.id == 0) {
            let (cur, req) = (&mut anchor.cursors[ci], &anchor.req);
            cur.req[..at].copy_from_slice(&req[..at]);
            cur.req_len = at as u16;
            cur.coll[..cl].copy_from_slice(&cb[..cl]);
            cur.coll_len = cl as u8;
            anchor.d_cursor = ci as u8;
        } else {
            anchor.d_cursor = u8::MAX;
        }
    }
    req_send(anchor, at, D_FIND);
}

fn cmd_get_more(anchor: &mut AnchorState, body: &[u8]) {
    let id = match mongo::elements(body).ok().and_then(|mut it| it.next()) {
        Some(Ok((_, bson::INT64, v))) => i64::from_le_bytes(v.try_into().unwrap_or([0; 8])),
        _ => {
            reply_error(
                anchor,
                mongo::CODE_FAILED_TO_PARSE,
                b"getMore needs a cursor id",
            );
            return;
        }
    };
    let slot = anchor.d_slot;
    let Some(ci) = anchor
        .cursors
        .iter()
        .position(|c| c.id == id && id != 0 && c.slot == slot)
    else {
        reply_error(anchor, mongo::CODE_CURSOR_NOT_FOUND, b"cursor id not found");
        return;
    };
    anchor.d_cursor = ci as u8;
    anchor.single_batch = false;
    anchor.find_limit = anchor.cursors[ci].remaining;
    let batch = mongo::find(body, b"batchSize")
        .ok()
        .flatten()
        .and_then(|(t, v)| mongo::int_value(t, v))
        .unwrap_or(BATCH_DOCS as i64);
    // The kept request, with a fresh correlation, a batch-sized limit
    // and the cursor's continuation.
    let n = anchor.cursors[ci].req_len as usize;
    let (req, cur) = (&mut anchor.req, &anchor.cursors[ci]);
    req[..n].copy_from_slice(&cur.req[..n]);
    anchor.corr = anchor.corr.wrapping_add(1).max(1);
    let corr = anchor.corr;
    anchor.req[0..8].copy_from_slice(&corr.to_le_bytes());
    // Drop the kept limit and budget; restate them.
    let mut kept = [0u8; DOC_REQUEST_MAX];
    let mut kl = DOC_REQUEST_HEAD;
    kept[..kl].copy_from_slice(&anchor.req[..kl]);
    if let Some(r) = DocRequest::decode(&anchor.req[..n]) {
        for (tag, b) in r.fields() {
            if matches!(tag, FIELD_LIMIT | FIELD_MAX_BYTES | FIELD_CONTINUE) {
                continue;
            }
            let end = kl + FIELD_HEAD + b.len();
            kept[kl] = tag;
            kept[kl + 1..kl + 5].copy_from_slice(&(b.len() as u32).to_le_bytes());
            kept[kl + 5..end].copy_from_slice(b);
            kl = end;
        }
    }
    anchor.req[..kl].copy_from_slice(&kept[..kl]);
    let mut at = kl;
    let mut per = if batch > 0 {
        batch as u32
    } else {
        BATCH_DOCS as u32
    };
    if anchor.find_limit > 0 {
        per = per.min(anchor.find_limit);
    }
    let _ = req_field(anchor, &mut at, FIELD_LIMIT, &[&per.to_le_bytes()]);
    let _ = req_field(
        anchor,
        &mut at,
        FIELD_MAX_BYTES,
        &[&list_budget().to_le_bytes()],
    );
    let cl = anchor.cursors[ci].cont_len as usize;
    let mut cont = [0u8; 16 + MAX_CONTINUE_KEY];
    cont[..cl].copy_from_slice(&anchor.cursors[ci].cont[..cl]);
    let _ = req_field(anchor, &mut at, FIELD_CONTINUE, &[&cont[..cl]]);
    req_send(anchor, at, D_GET_MORE);
}

fn cmd_kill_cursors(anchor: &mut AnchorState, body: &[u8]) {
    let slot = anchor.d_slot;
    let mut killed = [0i64; MAX_CURSORS];
    let mut k = 0usize;
    if let Ok(Some((bson::ARRAY, ids))) = mongo::find(body, b"cursors") {
        if let Ok(it) = mongo::elements(ids) {
            for (_, ty, v) in it.flatten() {
                if ty != bson::INT64 {
                    continue;
                }
                let id = i64::from_le_bytes(v.try_into().unwrap_or([0; 8]));
                if let Some(c) = anchor
                    .cursors
                    .iter_mut()
                    .find(|c| c.id == id && id != 0 && c.slot == slot)
                {
                    c.id = 0;
                    if k < killed.len() {
                        killed[k] = id;
                        k += 1;
                    }
                }
            }
        }
    }
    match codec::kill_cursors_body(&killed[..k], &mut anchor.reply) {
        Some(n) => reply_body(anchor, n),
        None => reply_error(
            anchor,
            mongo::CODE_FAILED_TO_PARSE,
            b"malformed killCursors",
        ),
    }
}

fn fmt_index(i: usize, out: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut v = i;
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

// ── update / delete: one statement at a time ─────────────────────────

/// The command's statement `i` in its `updates` / `deletes` array.
fn statement<'a>(body: &'a [u8], array: &[u8], i: u16) -> Result<Option<&'a [u8]>, Refusal> {
    let Some((bson::ARRAY, arr)) = mongo::find(body, array)? else {
        return Err(Refusal {
            code: mongo::CODE_FAILED_TO_PARSE,
            msg: mongo::msg_malformed(),
        });
    };
    match mongo::elements(arr)?.nth(i as usize) {
        None => Ok(None),
        Some(e) => {
            let (_, ty, v) = e?;
            if ty != bson::DOC {
                return Err(Refusal {
                    code: mongo::CODE_FAILED_TO_PARSE,
                    msg: mongo::msg_malformed(),
                });
            }
            Ok(Some(v))
        }
    }
}

/// Record a statement's failure; ordered writes stop at the first.
fn statement_failed(anchor: &mut AnchorState, r: Refusal) {
    anchor.err_index = anchor.stmt as i32;
    anchor.err_code = r.code;
    anchor.err_msg = r.msg;
    finish_writes(anchor);
}

fn next_update(anchor: &mut AnchorState) {
    let bl = anchor.cmd_len as usize;
    let mut body = [0u8; codec::MAX_BSON];
    body[..bl].copy_from_slice(&anchor.cmd[..bl]);
    let body = &body[..bl];
    let Some(coll) = collection_of(body, b"update") else {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    };
    let mut cb = [0u8; NAME_MAX];
    cb[..coll.len()].copy_from_slice(coll);
    let cl = coll.len();
    let st = match statement(body, b"updates", anchor.stmt) {
        Ok(Some(s)) => s,
        Ok(None) => return finish_writes(anchor),
        Err(r) => return reply_refusal(anchor, r),
    };
    if let Err(r) = send_update(anchor, &cb[..cl], st) {
        statement_failed(anchor, r);
    }
}

fn send_update(anchor: &mut AnchorState, coll: &[u8], st: &[u8]) -> Result<(), Refusal> {
    let bad = || Refusal {
        code: mongo::CODE_FAILED_TO_PARSE,
        msg: mongo::msg_malformed(),
    };
    let q = match mongo::find(st, b"q")? {
        Some((bson::DOC, q)) => q,
        _ => return Err(bad()),
    };
    let (uty, u) = mongo::find(st, b"u")?.ok_or_else(bad)?;
    if uty != bson::DOC {
        // A pipeline update.
        return Err(Refusal {
            code: mongo::CODE_FAILED_TO_PARSE,
            msg: mongo::msg_unsupported_update(),
        });
    }
    let upsert = mongo::find(st, b"upsert")?.is_some_and(|(t, v)| mongo::truthy(t, v));
    let multi = mongo::find(st, b"multi")?.is_some_and(|(t, v)| mongo::truthy(t, v));
    let operators = mongo::is_operator_update(u)?;
    if !operators && multi {
        return Err(Refusal {
            code: mongo::CODE_FAILED_TO_PARSE,
            msg: mongo::msg_mixed_update(),
        });
    }
    let exact = mongo::exact_id(q)?;
    let op = if operators { OP_UPDATE } else { OP_REPLACE };
    // Only an upsert creates a collection (MongoDB's implicit
    // creation); an update of a missing one matches nothing.
    let mut flags = 0;
    if upsert {
        flags |= FLAG_UPSERT | FLAG_AUTO_CREATE;
    }
    if multi {
        flags |= FLAG_MULTI;
    }
    let mut at = req_begin(anchor, op, flags);
    let full = || Refusal {
        code: mongo::CODE_BSON_TOO_LARGE,
        msg: mongo::msg_too_large(),
    };
    req_field(anchor, &mut at, FIELD_COLLECTION, &[coll]).ok_or_else(full)?;
    let mut buf = [0u8; DOC_REQUEST_MAX];
    match (exact, multi) {
        (Some((ty, v)), false) => {
            let n = mongo::value_tree_bytes(ty, v, &mut buf)?;
            req_field(anchor, &mut at, FIELD_ID, &[&buf[..n]]).ok_or_else(full)?;
        }
        _ => {
            let n = mongo::translate_filter(q, &mut buf)?;
            req_field(anchor, &mut at, FIELD_FILTER, &[&buf[..n]]).ok_or_else(full)?;
        }
    }
    if operators {
        let n = mongo::translate_update(u, &mut buf)?.unwrap_or_else(|| {
            // Only `$setOnInsert`: no mutations.
            buf[..2].copy_from_slice(&0u16.to_le_bytes());
            2
        });
        req_field(anchor, &mut at, FIELD_MUTATIONS, &[&buf[..n]]).ok_or_else(full)?;
    }
    // The document: a replacement, or an upsert's seed — each carrying
    // the `_id` the query names, or one minted here.
    let mut seed = [0u8; DOC_REQUEST_MAX];
    let mut scratch = [0u8; 8192];
    let sn = if operators {
        if !upsert {
            0
        } else {
            mongo::upsert_seed(q, Some(u), &mut seed, &mut scratch)?
        }
    } else {
        mongo::upsert_seed(q, None, &mut seed, &mut scratch).ok();
        let oid = mint_object_id(anchor);
        // The replacement, with `_id` from the query when it names one.
        let mut id_src = [0u8; 256];
        let idn = match exact {
            Some((ty, v)) => mongo::value_tree_bytes(ty, v, &mut id_src)?,
            None => 0,
        };
        replacement_tree(u, &id_src[..idn], upsert.then_some(&oid), &mut seed)?
    };
    if sn > 0 {
        let sn = if operators {
            ensure_id(anchor, &mut seed, sn)?
        } else {
            sn
        };
        req_field(anchor, &mut at, FIELD_DOC, &[&seed[..sn]]).ok_or_else(full)?;
    }
    req_send(anchor, at, D_UPDATE);
    Ok(())
}

/// A replacement document as a tree, `_id` first when given (the query's)
/// or minted (an upsert with none).
fn replacement_tree(
    u: &[u8],
    id: &[u8],
    minted: Option<&[u8; 12]>,
    out: &mut [u8],
) -> Result<usize, Refusal> {
    let mut w = Writer::new(out);
    w.begin_object()?;
    let has_id = mongo::find(u, b"_id")?.is_some();
    if !has_id && !id.is_empty() {
        w.key(b"_id")?;
        w.value(Value::trusted(id))?;
    } else if !has_id {
        if let Some(oid) = minted {
            w.key(b"_id")?;
            w.identifier(oid)?;
        }
    }
    for e in mongo::elements(u)? {
        let (name, ty, v) = e?;
        w.key(name)?;
        mongo::value_to_tree(ty, v, &mut w)?;
    }
    w.end()?;
    Ok(w.finish()?)
}

/// Give an upsert seed an `_id` when the query named none.
fn ensure_id(anchor: &mut AnchorState, seed: &mut [u8], n: usize) -> Result<usize, Refusal> {
    let has = Value::trusted(&seed[..n]).member(b"_id").is_some();
    if has {
        return Ok(n);
    }
    let oid = mint_object_id(anchor);
    let mut out = [0u8; DOC_REQUEST_MAX];
    let mut w = Writer::new(&mut out);
    w.begin_object()?;
    w.key(b"_id")?;
    w.identifier(&oid)?;
    for (k, v) in Value::trusted(&seed[..n]).members() {
        w.key(k)?;
        w.value(v)?;
    }
    w.end()?;
    let m = w.finish()?;
    seed.get_mut(..m)
        .ok_or(Refusal {
            code: mongo::CODE_BSON_TOO_LARGE,
            msg: mongo::msg_too_large(),
        })?
        .copy_from_slice(&out[..m]);
    Ok(m)
}

fn next_delete(anchor: &mut AnchorState) {
    let bl = anchor.cmd_len as usize;
    let mut body = [0u8; codec::MAX_BSON];
    body[..bl].copy_from_slice(&anchor.cmd[..bl]);
    let body = &body[..bl];
    let Some(coll) = collection_of(body, b"delete") else {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    };
    let mut cb = [0u8; NAME_MAX];
    cb[..coll.len()].copy_from_slice(coll);
    let cl = coll.len();
    let st = match statement(body, b"deletes", anchor.stmt) {
        Ok(Some(s)) => s,
        Ok(None) => return finish_writes(anchor),
        Err(r) => return reply_refusal(anchor, r),
    };
    let r = (|| -> Result<(), Refusal> {
        let bad = || Refusal {
            code: mongo::CODE_FAILED_TO_PARSE,
            msg: mongo::msg_malformed(),
        };
        let q = match mongo::find(st, b"q")? {
            Some((bson::DOC, q)) => q,
            _ => return Err(bad()),
        };
        let one = match mongo::find(st, b"limit")? {
            Some((t, v)) => mongo::int_value(t, v).ok_or_else(bad)? == 1,
            None => return Err(bad()),
        };
        let exact = mongo::exact_id(q)?;
        let flags = if one { 0 } else { FLAG_MULTI };
        let mut at = req_begin(anchor, OP_DELETE, flags);
        let full = || Refusal {
            code: mongo::CODE_BSON_TOO_LARGE,
            msg: mongo::msg_too_large(),
        };
        req_field(anchor, &mut at, FIELD_COLLECTION, &[&cb[..cl]]).ok_or_else(full)?;
        let mut buf = [0u8; DOC_REQUEST_MAX];
        match exact {
            Some((ty, v)) if one => {
                let n = mongo::value_tree_bytes(ty, v, &mut buf)?;
                req_field(anchor, &mut at, FIELD_ID, &[&buf[..n]]).ok_or_else(full)?;
            }
            _ => {
                let n = mongo::translate_filter(q, &mut buf)?;
                req_field(anchor, &mut at, FIELD_FILTER, &[&buf[..n]]).ok_or_else(full)?;
            }
        }
        // A delete reply shows none of the document: answer only `_id`.
        let mut proj = [0u8; 16];
        let pn = id_projection(&mut proj)?;
        req_field(anchor, &mut at, FIELD_PROJECTION, &[&proj[..pn]]).ok_or_else(full)?;
        req_send(anchor, at, D_DELETE);
        Ok(())
    })();
    if let Err(r) = r {
        statement_failed(anchor, r);
    }
}

/// The canonical projection keeping only `_id`.
fn id_projection(out: &mut [u8]) -> Result<usize, Refusal> {
    let mut p = [0u8; 8];
    let pn = mongo::dotted_path(b"_id", &mut p)?;
    let n = 2 + 2 + pn;
    let d = out.get_mut(..n).ok_or(Refusal {
        code: mongo::CODE_BSON_TOO_LARGE,
        msg: mongo::msg_too_large(),
    })?;
    d[0] = MODE_INCLUDE;
    d[1] = 1;
    d[2..4].copy_from_slice(&(pn as u16).to_le_bytes());
    d[4..].copy_from_slice(&p[..pn]);
    Ok(n)
}

/// Reply to an update/delete: the tallies, `upserted`, and the first
/// write error.
fn finish_writes(anchor: &mut AnchorState) {
    let mut name = [0u8; 32];
    let is_update = codec::command_name(&anchor.cmd[..anchor.cmd_len as usize], &mut name)
        .is_some_and(|n| &name[..n] == b"update");
    let error = (anchor.err_index >= 0).then_some(codec::WriteError {
        index: anchor.err_index,
        code: anchor.err_code,
        msg: anchor.err_msg,
    });
    let ul = anchor.upserted_len as usize;
    let built = codec::write_result_body(
        anchor.n as i32,
        is_update.then_some(anchor.n_modified as i32),
        &anchor.upserted[..ul],
        error,
        &mut anchor.reply,
    );
    match built {
        Some(n) => reply_body(anchor, n),
        None => reply_error(anchor, 8, b"internal"),
    }
}

// ── Answers ──────────────────────────────────────────────────────────

fn on_doc_response(anchor: &mut AnchorState, payload: &[u8]) {
    let Some(r) = DocResponse::decode(payload) else {
        reply_error(anchor, 8, b"a malformed answer from the document executor");
        return;
    };
    if r.corr_id != anchor.corr || anchor.d_phase == D_IDLE {
        return;
    }
    match anchor.d_phase {
        D_INSERT => {
            if r.outcome == OUT_OK {
                anchor.m_docs_written = anchor.m_docs_written.wrapping_add(anchor.n as u64);
                let n = anchor.n;
                anchor.reply_insert(n);
            } else {
                // Inserts are atomic: a refused batch wrote nothing, so
                // the command itself failed — a command error, not a
                // partial `writeErrors` answer.
                reply_refusal(anchor, mongo::outcome_refusal(r.outcome));
            }
        }
        D_FIND | D_GET_MORE => answer_batch(anchor, &r),
        D_UPDATE => {
            match r.outcome {
                OUT_OK | OUT_NOT_FOUND | OUT_NO_COLLECTION => {
                    let (m, md) = r.count().unwrap_or((0, 0));
                    anchor.n += m as i64;
                    anchor.n_modified += md as i64;
                    if let Some(id) = r.id() {
                        // An upsert created the document: `upserted`.
                        anchor.n += 1;
                        stage_upserted(anchor, id);
                    }
                }
                o => return statement_failed(anchor, mongo::outcome_refusal(o)),
            }
            anchor.stmt += 1;
            next_update(anchor);
        }
        D_DELETE => {
            match r.outcome {
                OUT_OK | OUT_NOT_FOUND | OUT_NO_COLLECTION => {
                    let (_, md) = r.count().unwrap_or((0, 0));
                    anchor.n += md as i64;
                }
                o => return statement_failed(anchor, mongo::outcome_refusal(o)),
            }
            anchor.stmt += 1;
            next_delete(anchor);
        }
        D_CREATE | D_DROP => {
            if r.outcome != OUT_OK {
                return reply_refusal(anchor, mongo::outcome_refusal(r.outcome));
            }
            let mut b = [0u8; 64];
            match codec::ok_body(&mut b) {
                Some(n) => {
                    anchor.reply[..n].copy_from_slice(&b[..n]);
                    reply_body(anchor, n);
                }
                None => reply_error(anchor, 8, b"internal"),
            }
        }
        D_LIST_COLLECTIONS => answer_collections(anchor, &r),
        D_CREATE_INDEXES => match r.outcome {
            OUT_OK => {
                anchor.n = r
                    .descriptors()
                    .next()
                    .map_or(0, |d| d.indexes().count() as i64 + 1);
                anchor.stmt += 1;
                next_create_index(anchor);
            }
            OUT_CONFLICT => reply_error(
                anchor,
                mongo::CODE_INDEX_KEY_SPECS_CONFLICT,
                b"an index of this name exists with different options",
            ),
            o => reply_refusal(anchor, mongo::outcome_refusal(o)),
        },
        D_LIST_INDEXES => answer_indexes(anchor, &r),
        D_DROP_INDEXES_LIST => on_drop_indexes_list(anchor, &r),
        D_DROP_INDEX => match r.outcome {
            OUT_OK => {
                anchor.stmt += 1;
                next_drop_index(anchor);
            }
            OUT_NOT_FOUND => reply_error(anchor, mongo::CODE_INDEX_NOT_FOUND, b"index not found"),
            o => reply_refusal(anchor, mongo::outcome_refusal(o)),
        },
        _ => {}
    }
}

impl AnchorState {
    fn reply_insert(&mut self, n: i64) {
        match codec::write_result_body(n as i32, None, &[], None, &mut self.reply) {
            Some(len) => reply_body(self, len),
            None => reply_error(self, 8, b"internal"),
        }
    }
}

/// `{index, _id}` for the statement just upserted.
fn stage_upserted(anchor: &mut AnchorState, id: Value<'_>) {
    let at = anchor.upserted_len as usize;
    let mut pos = at;
    let (position, stmt) = (anchor.upserted_count as u32, anchor.stmt as i32);
    let written = codec::upserted_element(
        position,
        stmt,
        |out, pos| mongo::write_element(b"_id", id, out, pos).ok(),
        &mut anchor.upserted,
        &mut pos,
    );
    if written.is_some() {
        anchor.upserted_len = pos as u16;
        anchor.upserted_count += 1;
    }
}

/// A `find`/`getMore` batch: `{cursor: {firstBatch|nextBatch: [...], id,
/// ns}, ok: 1}`.
fn answer_batch(anchor: &mut AnchorState, r: &DocResponse<'_>) {
    let first = anchor.d_phase == D_FIND;
    match r.outcome {
        OUT_OK => {}
        // A point read of nothing, or a list of a collection that does
        // not exist: an empty batch, as MongoDB answers.
        OUT_NOT_FOUND | OUT_NO_COLLECTION => {}
        o => return reply_refusal(anchor, mongo::outcome_refusal(o)),
    }
    let ci = anchor.d_cursor as usize;
    // The cursor: open while the executor has more and the limit allows.
    let mut count = 0u32;
    for _ in r.docs() {
        count += 1;
    }
    let mut remaining = anchor.find_limit;
    if remaining > 0 {
        remaining = remaining.saturating_sub(count);
    }
    let more = r.flags & RFLAG_MORE != 0
        && !anchor.single_batch
        && (anchor.find_limit == 0 || remaining > 0);
    if more && ci >= MAX_CURSORS {
        // More documents and no cursor to reach them: answering this batch
        // as the whole result would silently truncate it.
        return reply_error(
            anchor,
            mongo::CODE_OPERATION_FAILED,
            b"lattice: too many open cursors; exhaust or kill one",
        );
    }
    let id = if more {
        let cont = r.continuation().unwrap_or(&[]);
        let c = &mut anchor.cursors[ci];
        if c.id == 0 {
            anchor.next_cursor = anchor.next_cursor.wrapping_add(1).max(1);
            c.id = anchor.next_cursor;
            c.slot = anchor.d_slot;
        }
        c.cont[..cont.len()].copy_from_slice(cont);
        c.cont_len = cont.len() as u16;
        c.remaining = remaining;
        c.id
    } else {
        if ci < MAX_CURSORS {
            anchor.cursors[ci].id = 0;
        }
        0
    };
    let mut ns = [0u8; 8 + NAME_MAX];
    let nl = if ci < MAX_CURSORS {
        let c = &anchor.cursors[ci];
        let cl = c.coll_len as usize;
        ns[..DB_NAME.len()].copy_from_slice(DB_NAME);
        ns[DB_NAME.len()] = b'.';
        ns[DB_NAME.len() + 1..DB_NAME.len() + 1 + cl].copy_from_slice(&c.coll[..cl]);
        DB_NAME.len() + 1 + cl
    } else {
        ns[..DB_NAME.len()].copy_from_slice(DB_NAME);
        DB_NAME.len()
    };
    let built = (|| {
        let mut fr = codec::FindReply::new(&mut anchor.reply, !first)?;
        for (_, d) in r.docs() {
            fr.push_with(|out, pos| mongo::tree_to_bson(d, out, pos).ok())?;
        }
        fr.finish(&ns[..nl], id)
    })();
    match built {
        Some(n) => {
            anchor.m_docs_returned = anchor.m_docs_returned.wrapping_add(count as u64);
            reply_body(anchor, n);
        }
        // The budget the list asked for makes this unreachable; a batch
        // that still did not render is refused, never truncated.
        None => reply_error(
            anchor,
            mongo::CODE_BSON_TOO_LARGE,
            b"a batch did not render",
        ),
    }
}

// ── Indexes ──────────────────────────────────────────────────────────

/// `createIndexes`: the command's next spec, or the answer.
fn next_create_index(anchor: &mut AnchorState) {
    let bl = anchor.cmd_len as usize;
    let mut body = [0u8; codec::MAX_BSON];
    body[..bl].copy_from_slice(&anchor.cmd[..bl]);
    let body = &body[..bl];
    let Some(coll) = collection_of(body, b"createIndexes") else {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    };
    let mut cb = [0u8; NAME_MAX];
    cb[..coll.len()].copy_from_slice(coll);
    let cl = coll.len();
    let spec = match statement(body, b"indexes", anchor.stmt) {
        Ok(Some(s)) => s,
        Ok(None) => {
            let after = anchor.n as i32;
            let mut out = [0u8; 128];
            let mut pos = 0usize;
            let built = (|| {
                let root = bson_open(&mut out, &mut pos)?;
                bson_i32(&mut out, &mut pos, b"numIndexesAfter", after)?;
                bson_bool(&mut out, &mut pos, b"createdCollectionAutomatically", false)?;
                bson_i32(&mut out, &mut pos, b"ok", 1)?;
                bson_close(&mut out, &mut pos, root)
            })();
            match built {
                Some(n) => {
                    anchor.reply[..n].copy_from_slice(&out[..n]);
                    reply_body(anchor, n);
                }
                None => reply_error(anchor, 8, b"internal"),
            }
            return;
        }
        Err(r) => return reply_refusal(anchor, r),
    };
    let mut ix = [0u8; 2048];
    let n = match mongo::translate_index_spec(spec, &mut ix) {
        Ok(n) => n,
        Err(r) => return reply_refusal(anchor, r),
    };
    let mut at = req_begin(anchor, OP_INDEX_CREATE, FLAG_AUTO_CREATE);
    if req_field(anchor, &mut at, FIELD_COLLECTION, &[&cb[..cl]]).is_none()
        || req_field(anchor, &mut at, FIELD_INDEX, &[&ix[..n]]).is_none()
    {
        reply_refusal(
            anchor,
            Refusal {
                code: mongo::CODE_BSON_TOO_LARGE,
                msg: mongo::msg_too_large(),
            },
        );
        return;
    }
    req_send(anchor, at, D_CREATE_INDEXES);
}

/// `listIndexes`: `_id_` and each declared index.
fn answer_indexes(anchor: &mut AnchorState, r: &DocResponse<'_>) {
    if r.outcome != OUT_OK {
        return reply_refusal(anchor, mongo::outcome_refusal(r.outcome));
    }
    let built = (|| {
        let mut fr = codec::FindReply::new(&mut anchor.reply, false)?;
        fr.push_with(|out, pos| mongo::write_index_doc(None, out, pos))?;
        if let Some(d) = r.descriptors().next() {
            for ix in d.indexes() {
                fr.push_with(|out, pos| mongo::write_index_doc(Some(&ix), out, pos))?;
            }
        }
        fr.finish(b"lattice.$cmd.listIndexes", 0)
    })();
    match built {
        Some(n) => reply_body(anchor, n),
        None => reply_error(
            anchor,
            mongo::CODE_BSON_TOO_LARGE,
            b"the index list did not render",
        ),
    }
}

/// `dropIndexes`: a name drops that index; `*` every index but `_id_`;
/// a key document the index with that key.
fn cmd_drop_indexes(anchor: &mut AnchorState, body: &[u8]) {
    let Some(coll) = collection_of(body, b"dropIndexes") else {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    };
    let mut cb = [0u8; NAME_MAX];
    cb[..coll.len()].copy_from_slice(coll);
    let cl = coll.len();
    anchor.drop_count = 0;
    match mongo::find(body, b"index") {
        Ok(Some((bson::STRING, v))) => {
            let name = mongo::str_value(v).unwrap_or(b"");
            if name == b"_id_" {
                reply_error(
                    anchor,
                    mongo::CODE_INVALID_OPTIONS,
                    b"cannot drop _id index",
                );
                return;
            }
            if name != b"*" {
                if name.is_empty() || name.len() > 64 {
                    reply_error(anchor, mongo::CODE_INDEX_NOT_FOUND, b"index not found");
                    return;
                }
                anchor.drop_names[0][..name.len()].copy_from_slice(name);
                anchor.drop_name_len[0] = name.len() as u8;
                anchor.drop_count = 1;
                next_drop_index(anchor);
                return;
            }
        }
        Ok(Some((bson::DOC, _))) => {}
        _ => {
            reply_error(
                anchor,
                mongo::CODE_FAILED_TO_PARSE,
                b"dropIndexes needs an index",
            );
            return;
        }
    }
    // `*` or a key document: the descriptor names them.
    let mut at = req_begin(anchor, OP_COLL_GET, 0);
    let _ = req_field(anchor, &mut at, FIELD_COLLECTION, &[&cb[..cl]]);
    req_send(anchor, at, D_DROP_INDEXES_LIST);
}

fn on_drop_indexes_list(anchor: &mut AnchorState, r: &DocResponse<'_>) {
    if r.outcome != OUT_OK {
        return reply_refusal(anchor, mongo::outcome_refusal(r.outcome));
    }
    let bl = anchor.cmd_len as usize;
    let mut body = [0u8; codec::MAX_BSON];
    body[..bl].copy_from_slice(&anchor.cmd[..bl]);
    // The key document, when the command named one: its rendering is the
    // index's `key`, compared field by field.
    let key = match mongo::find(&body[..bl], b"index") {
        Ok(Some((bson::DOC, k))) => Some(k),
        _ => None,
    };
    let mut n = 0usize;
    if let Some(d) = r.descriptors().next() {
        for ix in d.indexes() {
            let hit = match key {
                None => true,
                Some(k) => {
                    let mut doc = [0u8; 512];
                    let mut pos = 0usize;
                    mongo::write_index_doc(Some(&ix), &mut doc, &mut pos).is_some()
                        && mongo::find(&doc[..pos], b"key")
                            .ok()
                            .flatten()
                            .is_some_and(|(_, v)| v == k)
                }
            };
            if hit && n < DROP_NAMES && ix.name.len() <= 64 {
                anchor.drop_names[n][..ix.name.len()].copy_from_slice(ix.name);
                anchor.drop_name_len[n] = ix.name.len() as u8;
                n += 1;
            }
        }
    }
    if key.is_some() && n == 0 {
        reply_error(anchor, mongo::CODE_INDEX_NOT_FOUND, b"index not found");
        return;
    }
    anchor.drop_count = n as u8;
    anchor.stmt = 0;
    next_drop_index(anchor);
}

/// Drop the next named index, or answer.
fn next_drop_index(anchor: &mut AnchorState) {
    let i = anchor.stmt as usize;
    if i >= anchor.drop_count as usize {
        let mut b = [0u8; 64];
        match codec::ok_body(&mut b) {
            Some(n) => {
                anchor.reply[..n].copy_from_slice(&b[..n]);
                reply_body(anchor, n);
            }
            None => reply_error(anchor, 8, b"internal"),
        }
        return;
    }
    let bl = anchor.cmd_len as usize;
    let mut body = [0u8; codec::MAX_BSON];
    body[..bl].copy_from_slice(&anchor.cmd[..bl]);
    let Some(coll) = collection_of(&body[..bl], b"dropIndexes") else {
        reply_error(anchor, 73, b"invalid collection name");
        return;
    };
    let mut cb = [0u8; NAME_MAX];
    cb[..coll.len()].copy_from_slice(coll);
    let cl = coll.len();
    let nl = anchor.drop_name_len[i] as usize;
    let mut name = [0u8; 64];
    name[..nl].copy_from_slice(&anchor.drop_names[i][..nl]);
    let mut at = req_begin(anchor, OP_INDEX_DROP, 0);
    let _ = req_field(anchor, &mut at, FIELD_COLLECTION, &[&cb[..cl]]);
    let _ = req_field(anchor, &mut at, FIELD_INDEX_NAME, &[&name[..nl]]);
    req_send(anchor, at, D_DROP_INDEX);
}

fn answer_collections(anchor: &mut AnchorState, r: &DocResponse<'_>) {
    if r.outcome != OUT_OK {
        return reply_refusal(anchor, mongo::outcome_refusal(r.outcome));
    }
    let built = (|| {
        let mut fr = codec::FindReply::new(&mut anchor.reply, false)?;
        for d in r.descriptors() {
            fr.push_with(|out, pos| {
                let e = bson_open(out, pos)?;
                bson_str(out, pos, b"name", d.name())?;
                bson_str(out, pos, b"type", b"collection")?;
                bson_close(out, pos, e).map(|_| ())
            })?;
        }
        fr.finish(b"lattice.$cmd.listCollections", 0)
    })();
    match built {
        Some(n) => reply_body(anchor, n),
        None => reply_error(
            anchor,
            mongo::CODE_BSON_TOO_LARGE,
            b"the collection list did not render",
        ),
    }
}

// ── Sessions ─────────────────────────────────────────────────────────

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
                let rest = len - used;
                let mut i = 0;
                while i < rest {
                    slot.recv[i] = slot.recv[used + i];
                    i += 1;
                }
                slot.recv_len = rest;
                begin_command(anchor, idx, request_id, &body[..blen]);
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
            if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = conn_id(payload);
                    anchor.phase = PHASE_LISTENING;
                }
            } else if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= CONN_ID_LEN {
                anchor.server_conn_id = conn_id(payload);
                anchor.phase = PHASE_LISTENING;
            }
        }
        NET_MSG_ACCEPTED => {
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
    if state.is_null() || syscalls.is_null() || state_size < core::mem::size_of::<AnchorState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let anchor = unsafe { &mut *state.cast::<AnchorState>() };
    // The state arrives zeroed; set what is not zero.
    anchor.syscalls = sys_ptr;
    anchor.listen_port = DEFAULT_LISTEN_PORT;
    anchor.phase = PHASE_INIT;
    anchor.server_conn_id = SLOT_FREE;
    for s in anchor.slots.iter_mut() {
        s.conn_id = SLOT_FREE;
        s.state = S_READY;
        s.recv_len = 0;
        s.send_len = 0;
    }
    anchor.d_phase = D_IDLE;
    anchor.err_msg = b"";
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(anchor, params, params_len) };
    }
    // inputs: net_in[0], exec_in[1]; outputs: net_out[0], exec_out[1],
    // metrics[2].
    anchor.net_in = in_chan;
    anchor.net_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        anchor.exec_in = dev_channel_port(sys, 0, 1);
        anchor.exec_out = dev_channel_port(sys, 1, 1);
        anchor.metrics_out = dev_channel_port(sys, 1, 2);
        // ObjectIds carry five process-random bytes.
        let _ = dev_csprng_fill(sys, anchor.oid_random.as_mut_ptr(), 5);
        let mut c = [0u8; 4];
        let _ = dev_csprng_fill(sys, c.as_mut_ptr(), 4);
        anchor.oid_counter = u32::from_le_bytes(c) & 0x00FF_FFFF;
    }
    if anchor.exec_out < 0 || anchor.exec_in < 0 {
        return -1;
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
            let mut env = [0u8; RECV_BUF + 16];
            let Some((msg, payload)) = read_one_envelope(sys, anchor.net_in, &mut env) else {
                break;
            };
            dispatch_net(anchor, msg, payload);
        }
        let mut env = [0u8; DOC_RESPONSE_MAX];
        if let Some((msg, payload)) = read_one_envelope(sys, anchor.exec_in, &mut env) {
            if msg == MSG_DOC_RESPONSE {
                on_doc_response(anchor, payload);
            }
        }
    }
    // The executor answers every request (its own watchdog included); a
    // lost answer must still not hold the anchor busy forever.
    if anchor.d_phase != D_IDLE {
        let now = unsafe { dev_millis(&*anchor.syscalls) };
        if anchor.deadline_ms != 0 && now > anchor.deadline_ms {
            reply_error(
                anchor,
                mongo::CODE_MAX_TIME_EXPIRED,
                b"operation exceeded time limit",
            );
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
    if chan < 0 || payload_len > u16::MAX as usize || wire::ENVELOPE_HDR + payload_len > env.len() {
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
