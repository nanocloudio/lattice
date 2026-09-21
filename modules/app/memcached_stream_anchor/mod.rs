//! memcached_stream_anchor — Memcached ASCII TCP edge anchor.
//!
//! Continuity class: `drain_only`. CAS-token correctness comes from
//! `kv_state_worker`'s `mod_revision` plumbed through MSG_KV_RESPONSE,
//! not from connection continuity.
//!
//! ## What this module owns
//!
//! - TCP bind + accept against foundation/ip (same NET_CMD_* state
//!   machine as the redis anchor; collapses to a thin facade once the
//!   foundation `stream_anchor_core` is promoted).
//! - Per-connection slot table with a CRLF-line ASCII parser and a
//!   peek-ahead for the trailing data block on storage commands.
//! - Inline session commands: version, quit, stats, flush_all.
//! - Real ops forwarded as MSG_KV_REQUEST envelopes on `kv_out`:
//!   get / gets / set / add / replace / append / prepend / cas /
//!   delete / incr / decr.
//! - A per-corr_id inflight table mapping `corr_id → MemOp` so the
//!   response decoder knows how to format each KV_RESULT_* code back
//!   onto the memcached wire.
//!
//! ## Wire shape recap (Memcached ASCII)
//!
//!   get  k1 [k2 ...]\r\n         → VALUE k flags bytes\r\n<data>\r\n…END\r\n
//!   gets k1 [k2 ...]\r\n         → VALUE k flags bytes cas\r\n<data>\r\n…END\r\n
//!   set  k flags exp bytes\r\n<data>\r\n  → STORED\r\n / NOT_STORED\r\n
//!   add  k flags exp bytes\r\n<data>\r\n  → STORED / NOT_STORED  (NX semantics)
//!   replace k flags exp bytes\r\n<data>\r\n → STORED / NOT_STORED  (XX semantics)
//!   delete k\r\n                → DELETED\r\n / NOT_FOUND\r\n
//!   incr k delta\r\n            → <new value>\r\n / NOT_FOUND\r\n
//!   decr k delta\r\n            → <new value>\r\n / NOT_FOUND\r\n
//!   flush_all\r\n               → OK\r\n
//!   version\r\n                 → VERSION 0.2.0\r\n
//!   stats\r\n                   → STAT k v\r\n… END\r\n
//!   quit\r\n                    → close

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

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[path = "../../common/collections.rs"]
mod collections;

#[path = "../../common/memcached_codec.rs"]
mod memcached_codec;

use memcached_codec::{
    append, append_decimal_u64, encode_reply, eq_ascii_ci, find_crlf, itoa_i64, itoa_u64,
    parse_i64_dec, parse_line, parse_u32_dec, token_bytes, Line, MemOp, Token, MAX_TOKENS,
};

use collections::InflightTable;
use types::{
    KV_OP_APPEND, KV_OP_DECR, KV_OP_DELETE, KV_OP_FLUSH, KV_OP_GET, KV_OP_INCR, KV_OP_PREPEND,
    KV_OP_PUT, KV_RESULT_CAS_FAILED, KV_RESULT_INTEGER, KV_RESULT_INTERNAL, KV_RESULT_NOT_FOUND,
    KV_RESULT_OK, KV_RESULT_WRONG_TYPE, PROTO_MEMCACHED, PUT_FLAG_NX, PUT_FLAG_XX,
    REQ_LINEARIZABLE, REQ_SERIALIZABLE,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── NET protocol constants ────────────────────────────────────────────
//
// The SDK's Stream Surface contract, reached through the `abi` mount.

use abi::contracts::net::net_proto::{
    conn_id, CMD_BIND as NET_CMD_BIND, CMD_CLOSE as NET_CMD_CLOSE, CMD_SEND as NET_CMD_SEND,
    CONN_ID_LEN, MSG_ACCEPTED as NET_MSG_ACCEPTED, MSG_BOUND as NET_MSG_BOUND,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_DATA as NET_MSG_DATA, MSG_ERROR as NET_MSG_ERROR,
};

// ── Capacities ────────────────────────────────────────────────────────

/// Active client slots — same rationale as `redis_edge_anchor`. 64
/// slots × ~16 KiB per slot ≈ 1 MiB of state arena.
const MAX_CONNS: usize = 64;
const RECV_BUF_SIZE: usize = 8192;
const SEND_BUF_SIZE: usize = 8192;
const SCRATCH_BUF_SIZE: usize = 8192 + 64;
const MAX_INFLIGHT: usize = 256;
const DEFAULT_LISTEN_PORT: u16 = 11211;

define_params! {
    AnchorState;

    // TCP port the anchor binds for memcached clients.
    //
    // This anchor had no params block at all, so it always bound 11211
    // and could not be relocated — which meant no harness could run it
    // without owning the machine's memcached port, and two graphs could
    // never coexist. Every other anchor here is overridable; this one
    // was the exception by omission rather than by decision.
    1, listen_port, u16, DEFAULT_LISTEN_PORT
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, DEFAULT_LISTEN_PORT); };
}
const DEFAULT_TENANT: u32 = 0;
const SLOT_FREE: u16 = 0xFFFF;

// ── Slot ──────────────────────────────────────────────────────────────

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotPhase {
    Open = 0,
    Closing = 1,
}

#[repr(C)]
struct Slot {
    conn_id: u16,
    phase: SlotPhase,
    // Number of GET requests currently in flight for the current
    // `get`/`gets` batch on this slot. `END\r\n` is emitted only when
    // this drains to 0 AND `pending_get_emit_end` is true — guarantees
    // VALUE lines precede the END terminator on the wire.
    pending_gets: u16,
    pending_get_emit_end: bool,
    _pad: u8,
    tenant: u32,
    recv_buf: [u8; RECV_BUF_SIZE],
    recv_len: usize,
    send_buf: [u8; SEND_BUF_SIZE],
    send_len: usize,
}

impl Slot {
    const fn free() -> Self {
        Self {
            conn_id: SLOT_FREE,
            phase: SlotPhase::Open,
            pending_gets: 0,
            pending_get_emit_end: false,
            _pad: 0,
            tenant: DEFAULT_TENANT,
            recv_buf: [0; RECV_BUF_SIZE],
            recv_len: 0,
            send_buf: [0; SEND_BUF_SIZE],
            send_len: 0,
        }
    }

    fn reset_session(&mut self) {
        self.tenant = DEFAULT_TENANT;
        self.recv_len = 0;
        self.send_len = 0;
        self.pending_gets = 0;
        self.pending_get_emit_end = false;
    }
}

// ── Anchor ────────────────────────────────────────────────────────────

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum AnchorPhase {
    Init = 0,
    WaitBound = 1,
    Listening = 2,
    Error = 0xFF,
}

#[repr(C)]
struct InflightVal {
    op: u8,
    conn_id: u16,
    // For multi-key GET we'd need the full key list; Phase 2 keeps GET
    // single-key (memcached `get k1 k2 k3` becomes one round-trip per
    // key — semantically equivalent, slightly chattier).
    key_len: u16,
    key: [u8; 256],
}

impl InflightVal {
    const fn new() -> Self {
        Self {
            op: 0,
            conn_id: 0,
            key_len: 0,
            key: [0; 256],
        }
    }
}

#[repr(C)]
struct AnchorState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,
    routed_in: i32,
    listen_port: u16,
    phase: AnchorPhase,
    _pad0: u8,
    server_conn_id: u16,
    _pad1: [u8; 3],
    corr_seq: u64,
    // STATS counters (memcached `stats` command). `curr_connections` is
    // computed from the slot table on demand; these three accumulate
    // over the anchor's lifetime.
    cmd_get: u64,
    cmd_set: u64,
    total_connections: u64,
    // Phase-14 telemetry. The three STATS counters above double as the
    // module-scope metrics emitted on `metrics_out`; ids follow the
    // manifest `[observability] metrics` order (0=cmd_get, 1=cmd_set,
    // 2=total_connections). `step_ctr` paces the coarse emit cadence.
    step_ctr: u64,
    inflight: InflightTable<InflightVal, MAX_INFLIGHT>,
    slots: [Slot; MAX_CONNS],
    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl AnchorState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.net_in = -1;
        self.net_out = -1;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.routed_in = -1;
        self.listen_port = DEFAULT_LISTEN_PORT;
        self.phase = AnchorPhase::Init;
        self._pad0 = 0;
        self.server_conn_id = 0;
        self._pad1 = [0; 3];
        self.corr_seq = 0;
        self.cmd_get = 0;
        self.cmd_set = 0;
        self.total_connections = 0;
        self.step_ctr = 0;
        self.inflight = InflightTable::new();
        let mut i = 0;
        while i < MAX_CONNS {
            self.slots[i] = Slot::free();
            i += 1;
        }
    }

    fn alloc_slot(&mut self, conn_id: u16) -> Option<usize> {
        for i in 0..MAX_CONNS {
            if self.slots[i].conn_id == SLOT_FREE {
                self.slots[i].conn_id = conn_id;
                self.slots[i].phase = SlotPhase::Open;
                self.slots[i].reset_session();
                self.total_connections += 1;
                return Some(i);
            }
        }
        None
    }

    fn find_slot(&self, conn_id: u16) -> Option<usize> {
        for (i, slot) in self.slots.iter().enumerate() {
            if slot.conn_id == conn_id {
                return Some(i);
            }
        }
        None
    }

    fn free_slot(&mut self, idx: usize) {
        if idx < MAX_CONNS {
            self.slots[idx].conn_id = SLOT_FREE;
            self.slots[idx].phase = SlotPhase::Open;
            self.slots[idx].reset_session();
        }
    }

    fn next_corr(&mut self) -> u64 {
        // Top byte = protocol id; see redis_edge_anchor::next_corr for
        // why each anchor must namespace its own range.
        self.corr_seq = self.corr_seq.wrapping_add(1);
        if self.corr_seq == 0 {
            self.corr_seq = 1;
        }
        (u64::from(PROTO_MEMCACHED) << 56) | (self.corr_seq & 0x00FF_FFFF_FFFF_FFFF)
    }
}

fn put_u8(buf: &mut [u8], at: &mut usize, v: u8) -> bool {
    if *at + 1 > buf.len() {
        return false;
    }
    buf[*at] = v;
    *at += 1;
    true
}

fn put_u16(buf: &mut [u8], at: &mut usize, v: u16) -> bool {
    if *at + 2 > buf.len() {
        return false;
    }
    buf[*at..*at + 2].copy_from_slice(&v.to_le_bytes());
    *at += 2;
    true
}

fn put_u32(buf: &mut [u8], at: &mut usize, v: u32) -> bool {
    if *at + 4 > buf.len() {
        return false;
    }
    buf[*at..*at + 4].copy_from_slice(&v.to_le_bytes());
    *at += 4;
    true
}

fn put_u64(buf: &mut [u8], at: &mut usize, v: u64) -> bool {
    if *at + 8 > buf.len() {
        return false;
    }
    buf[*at..*at + 8].copy_from_slice(&v.to_le_bytes());
    *at += 8;
    true
}

fn put_i64(buf: &mut [u8], at: &mut usize, v: i64) -> bool {
    if *at + 8 > buf.len() {
        return false;
    }
    buf[*at..*at + 8].copy_from_slice(&v.to_le_bytes());
    *at += 8;
    true
}

fn put_bytes(buf: &mut [u8], at: &mut usize, bytes: &[u8]) -> bool {
    if *at + bytes.len() > buf.len() {
        return false;
    }
    buf[*at..*at + bytes.len()].copy_from_slice(bytes);
    *at += bytes.len();
    true
}

fn put_key(buf: &mut [u8], at: &mut usize, key: &[u8]) -> bool {
    if key.len() > u16::MAX as usize {
        return false;
    }
    put_u16(buf, at, key.len() as u16) && put_bytes(buf, at, key)
}

fn put_value_u32(buf: &mut [u8], at: &mut usize, value: &[u8]) -> bool {
    if value.len() > u32::MAX as usize {
        return false;
    }
    put_u32(buf, at, value.len() as u32) && put_bytes(buf, at, value)
}

/// Stamp a MSG_KV_REQUEST envelope into `scratch` and channel_write.
/// Returns `true` on success. Records `(corr_id → InflightVal { op,
/// conn_id, key })` so the response decoder can format the reply.
unsafe fn send_envelope(
    anchor: &mut AnchorState,
    slot_idx: usize,
    op: u8,
    op_consistency: u8,
    mem_op: MemOp,
    key_for_inflight: &[u8],
    body: &[u8],
) -> bool {
    let sys_ptr = anchor.syscalls;
    if sys_ptr.is_null() || anchor.kv_out < 0 {
        return false;
    }
    let corr = anchor.next_corr();
    let tenant = anchor.slots[slot_idx].tenant;
    let conn_id = anchor.slots[slot_idx].conn_id;

    // Envelope head: [corr:8][proto:1][tenant:4][conn:1][cons:1][op:1][body_len:2] = 18
    let head_len = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    let payload_len = head_len + body.len();
    if payload_len > u16::MAX as usize {
        return false;
    }
    let total = 3 + payload_len;
    if total > anchor.scratch.len() {
        return false;
    }

    let scratch = &mut anchor.scratch[..total];
    scratch[0] = MSG_KV_REQUEST;
    scratch[1] = (payload_len & 0xFF) as u8;
    scratch[2] = ((payload_len >> 8) & 0xFF) as u8;
    let mut p = 3;
    scratch[p..p + 8].copy_from_slice(&corr.to_le_bytes());
    p += 8;
    scratch[p] = PROTO_MEMCACHED;
    p += 1;
    scratch[p..p + 4].copy_from_slice(&tenant.to_le_bytes());
    p += 4;
    // The head's conn byte is this anchor's SLOT INDEX (a caller-
    // chosen routing slot the router echoes) — NOT the net conn id,
    // which is u16 now and does not fit the byte.
    scratch[p] = slot_idx as u8;
    p += 1;
    scratch[p] = op_consistency;
    p += 1;
    scratch[p] = op;
    p += 1;
    scratch[p] = (body.len() & 0xFF) as u8;
    scratch[p + 1] = ((body.len() >> 8) & 0xFF) as u8;
    p += 2;
    scratch[p..p + body.len()].copy_from_slice(body);

    // Record inflight metadata BEFORE the write — if the write
    // backpressures we still need to know how to handle a (hypothetical)
    // arriving response. On failure we remove the entry.
    let mut inflight = InflightVal::new();
    inflight.op = mem_op as u8;
    inflight.conn_id = conn_id;
    let kn = key_for_inflight.len().min(inflight.key.len());
    inflight.key_len = kn as u16;
    inflight.key[..kn].copy_from_slice(&key_for_inflight[..kn]);
    if anchor.inflight.insert(corr, inflight).is_err() {
        return false;
    }

    let n = ((*sys_ptr).channel_write)(anchor.kv_out, anchor.scratch.as_mut_ptr(), total);
    if n != total as i32 {
        let _ = anchor.inflight.remove(corr);
        return false;
    }
    true
}

// ── Per-command handlers ──────────────────────────────────────────────
//
// Each handler operates on tokens already parsed from the first line
// of recv_buf. For storage commands (set/add/replace/append/prepend/
// cas) the caller has also verified that the trailing data block is
// fully present.
//
// They take `&mut AnchorState` because forwarding requires the inflight
// table + the channel writes via `send_envelope`. Inline responses go
// to `slot.send_buf` directly.

unsafe fn handle_version(slot: &mut Slot) {
    let _ = append(&mut slot.send_buf, &mut slot.send_len, b"VERSION 0.2.0\r\n");
}

unsafe fn handle_quit(slot: &mut Slot) {
    slot.phase = SlotPhase::Closing;
}

/// Emit one `STAT <name> <val>\r\n` line with a decimal value.
unsafe fn stat_u64(slot: &mut Slot, name: &[u8], val: u64) {
    let _ = append(&mut slot.send_buf, &mut slot.send_len, b"STAT ");
    let _ = append(&mut slot.send_buf, &mut slot.send_len, name);
    let _ = append(&mut slot.send_buf, &mut slot.send_len, b" ");
    let _ = append_decimal_u64(&mut slot.send_buf, &mut slot.send_len, val);
    let _ = append(&mut slot.send_buf, &mut slot.send_len, b"\r\n");
}

/// Emit one `STAT <name> <val>\r\n` line with a byte-string value.
unsafe fn stat_bytes(slot: &mut Slot, name: &[u8], val: &[u8]) {
    let _ = append(&mut slot.send_buf, &mut slot.send_len, b"STAT ");
    let _ = append(&mut slot.send_buf, &mut slot.send_len, name);
    let _ = append(&mut slot.send_buf, &mut slot.send_len, b" ");
    let _ = append(&mut slot.send_buf, &mut slot.send_len, val);
    let _ = append(&mut slot.send_buf, &mut slot.send_len, b"\r\n");
}

/// `stats\r\n` — report the general-purpose counters the anchor keeps
/// locally. Per-item cache stats (get_hits/get_misses/bytes) live in
/// kv_state_worker, not the edge, so they are not reported here; the
/// anchor owns connection and command-dispatch counters only. Real
/// clients (incl. the `memcache` crate's `.stats()`) parse the whole
/// `STAT k v` block into a map, so an honest subset is a valid reply.
unsafe fn handle_stats(anchor: &mut AnchorState, slot_idx: usize) {
    let mut curr_connections: u64 = 0;
    for slot in anchor.slots.iter() {
        if slot.conn_id != SLOT_FREE {
            curr_connections += 1;
        }
    }
    let cmd_get = anchor.cmd_get;
    let cmd_set = anchor.cmd_set;
    let total_connections = anchor.total_connections;

    let s = &mut anchor.slots[slot_idx];
    stat_bytes(s, b"version", b"1.6.0-lattice");
    stat_u64(s, b"pointer_size", 64);
    stat_u64(s, b"curr_connections", curr_connections);
    stat_u64(s, b"total_connections", total_connections);
    stat_u64(s, b"cmd_get", cmd_get);
    stat_u64(s, b"cmd_set", cmd_set);
    let _ = append(&mut s.send_buf, &mut s.send_len, b"END\r\n");
}

unsafe fn handle_get(anchor: &mut AnchorState, slot_idx: usize, line: &Line, gets: bool) {
    // For simplicity Phase 2 issues one envelope per key; a real
    // implementation would batch via KV_OP_MGET.
    let mem_op = if gets { MemOp::Gets } else { MemOp::Get };
    let recv_buf = anchor.slots[slot_idx].recv_buf;
    let mut dispatched: u16 = 0;
    let mut i = 1u8;
    while (i as usize) < line.count as usize {
        let key = &recv_buf[line.tokens[i as usize].offset as usize
            ..line.tokens[i as usize].offset as usize + line.tokens[i as usize].len as usize];
        let mut body = [0u8; 512];
        let mut at = 0;
        if !put_key(&mut body, &mut at, key) {
            let _ = append(
                &mut anchor.slots[slot_idx].send_buf,
                &mut anchor.slots[slot_idx].send_len,
                b"ERROR\r\n",
            );
            return;
        }
        if !send_envelope(
            anchor,
            slot_idx,
            KV_OP_GET,
            REQ_SERIALIZABLE,
            mem_op,
            key,
            &body[..at],
        ) {
            let _ = append(
                &mut anchor.slots[slot_idx].send_buf,
                &mut anchor.slots[slot_idx].send_len,
                b"SERVER_ERROR backpressure\r\n",
            );
            return;
        }
        dispatched = dispatched.saturating_add(1);
        i += 1;
    }
    // END\r\n is the protocol's batch terminator and must come AFTER
    // every per-key VALUE line. Record the pending count + mark this
    // slot to emit END when the response handler drains the last GET.
    // For an empty key list (shouldn't happen — the line parser
    // requires at least one key after `get`), emit END immediately.
    let slot = &mut anchor.slots[slot_idx];
    if dispatched == 0 {
        let _ = append(&mut slot.send_buf, &mut slot.send_len, b"END\r\n");
    } else {
        slot.pending_gets = slot.pending_gets.saturating_add(dispatched);
        slot.pending_get_emit_end = true;
    }
}

unsafe fn handle_storage(
    anchor: &mut AnchorState,
    slot_idx: usize,
    line: &Line,
    cmd_bytes: &[u8],
    data_start: usize,
    data_len: usize,
) {
    // set k flags exp bytes\r\n<data>\r\n
    // add / replace / append / prepend follow the same shape.
    // We parse the four tokens (key flags exp bytes), pull the data
    // block, and stamp a KV_OP_PUT (or KV_OP_APPEND for append/prepend).
    if line.count < 5 {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"CLIENT_ERROR bad command line\r\n",
        );
        return;
    }
    let recv_buf = anchor.slots[slot_idx].recv_buf;
    let key = &recv_buf[line.tokens[1].offset as usize
        ..line.tokens[1].offset as usize + line.tokens[1].len as usize];
    // tokens 2, 3, 4 are flags, exp, bytes — we honour exp (expiry)
    // but ignore the memcached flags field (Lattice doesn't surface it
    // back via GET in Phase 2; full flags support requires extending
    // KV_OP_PUT body shape).
    let _flags = parse_u32_dec(token_bytes(&recv_buf, line.tokens[2])).unwrap_or(0);
    let exp = parse_u32_dec(token_bytes(&recv_buf, line.tokens[3])).unwrap_or(0);
    let _bytes = parse_u32_dec(token_bytes(&recv_buf, line.tokens[4])).unwrap_or(0);
    let value = &recv_buf[data_start..data_start + data_len];

    let (op, put_flags, mem_op) = if eq_ascii_ci(cmd_bytes, b"set") {
        (KV_OP_PUT, 0u8, MemOp::Set)
    } else if eq_ascii_ci(cmd_bytes, b"add") {
        (KV_OP_PUT, PUT_FLAG_NX, MemOp::Add)
    } else if eq_ascii_ci(cmd_bytes, b"replace") {
        (KV_OP_PUT, PUT_FLAG_XX, MemOp::Replace)
    } else if eq_ascii_ci(cmd_bytes, b"append") {
        (KV_OP_APPEND, 0u8, MemOp::Append)
    } else if eq_ascii_ci(cmd_bytes, b"prepend") {
        (KV_OP_PREPEND, 0u8, MemOp::Prepend)
    } else {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"ERROR\r\n",
        );
        return;
    };

    let mut body = [0u8; SCRATCH_BUF_SIZE - 64];
    let mut at = 0;
    if op == KV_OP_PUT {
        let expiry_ms: u64 = if exp == 0 {
            0
        } else {
            (exp as u64).saturating_mul(1000)
        };
        if !put_key(&mut body, &mut at, key)
            || !put_value_u32(&mut body, &mut at, value)
            || !put_u8(&mut body, &mut at, put_flags)
            || !put_u64(&mut body, &mut at, expiry_ms)
        {
            let _ = append(
                &mut anchor.slots[slot_idx].send_buf,
                &mut anchor.slots[slot_idx].send_len,
                b"SERVER_ERROR command too large\r\n",
            );
            return;
        }
    } else {
        // KV_OP_APPEND / KV_OP_PREPEND share the body shape
        //   [klen][key][vlen:u32][value]
        if !put_key(&mut body, &mut at, key) || !put_value_u32(&mut body, &mut at, value) {
            let _ = append(
                &mut anchor.slots[slot_idx].send_buf,
                &mut anchor.slots[slot_idx].send_len,
                b"SERVER_ERROR command too large\r\n",
            );
            return;
        }
    }

    if !send_envelope(
        anchor,
        slot_idx,
        op,
        REQ_LINEARIZABLE,
        mem_op,
        key,
        &body[..at],
    ) {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"SERVER_ERROR backpressure\r\n",
        );
    }
}

unsafe fn handle_delete(anchor: &mut AnchorState, slot_idx: usize, line: &Line) {
    if line.count < 2 {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"CLIENT_ERROR bad command line\r\n",
        );
        return;
    }
    let recv_buf = anchor.slots[slot_idx].recv_buf;
    let key = &recv_buf[line.tokens[1].offset as usize
        ..line.tokens[1].offset as usize + line.tokens[1].len as usize];
    let mut body = [0u8; 512];
    let mut at = 0;
    if !put_u16(&mut body, &mut at, 1) || !put_key(&mut body, &mut at, key) {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"SERVER_ERROR command too large\r\n",
        );
        return;
    }
    if !send_envelope(
        anchor,
        slot_idx,
        KV_OP_DELETE,
        REQ_LINEARIZABLE,
        MemOp::Delete,
        key,
        &body[..at],
    ) {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"SERVER_ERROR backpressure\r\n",
        );
    }
}

unsafe fn handle_incr_decr(anchor: &mut AnchorState, slot_idx: usize, line: &Line, is_incr: bool) {
    if line.count < 3 {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"CLIENT_ERROR bad command line\r\n",
        );
        return;
    }
    let recv_buf = anchor.slots[slot_idx].recv_buf;
    let key = &recv_buf[line.tokens[1].offset as usize
        ..line.tokens[1].offset as usize + line.tokens[1].len as usize];
    let delta_bytes = token_bytes(&recv_buf, line.tokens[2]);
    let Some(delta) = parse_i64_dec(delta_bytes) else {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"CLIENT_ERROR invalid numeric delta\r\n",
        );
        return;
    };
    let mut body = [0u8; 512];
    let mut at = 0;
    if !put_key(&mut body, &mut at, key) || !put_i64(&mut body, &mut at, delta) {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"SERVER_ERROR command too large\r\n",
        );
        return;
    }
    let (op, mem_op) = if is_incr {
        (KV_OP_INCR, MemOp::Incr)
    } else {
        (KV_OP_DECR, MemOp::Decr)
    };
    if !send_envelope(
        anchor,
        slot_idx,
        op,
        REQ_LINEARIZABLE,
        mem_op,
        key,
        &body[..at],
    ) {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"SERVER_ERROR backpressure\r\n",
        );
    }
}

unsafe fn handle_flush_all(anchor: &mut AnchorState, slot_idx: usize) {
    // No body — KV_OP_FLUSH body shape is empty.
    let key: [u8; 0] = [];
    if !send_envelope(
        anchor,
        slot_idx,
        KV_OP_FLUSH,
        REQ_LINEARIZABLE,
        MemOp::FlushAll,
        &key,
        &[],
    ) {
        let _ = append(
            &mut anchor.slots[slot_idx].send_buf,
            &mut anchor.slots[slot_idx].send_len,
            b"SERVER_ERROR backpressure\r\n",
        );
    }
}

// ── Per-tick command dispatch ─────────────────────────────────────────

unsafe fn dispatch_one(anchor: &mut AnchorState, slot_idx: usize) -> bool {
    // Returns true if we consumed bytes (try again next iteration);
    // false if we need more data.
    let snapshot_len = anchor.slots[slot_idx].recv_len;
    if snapshot_len == 0 {
        return false;
    }
    // Inspect the leading line WITHOUT consuming, so that for storage
    // commands we can check whether the trailing data block is fully
    // present before committing.
    let line = {
        let buf = &anchor.slots[slot_idx].recv_buf[..snapshot_len];
        parse_line(buf)
    };
    let Some(line) = line else {
        return false;
    };
    if line.count == 0 {
        // Empty line — just consume it.
        let slot = &mut anchor.slots[slot_idx];
        let consumed = line.line_end;
        if consumed >= slot.recv_len {
            slot.recv_len = 0;
        } else {
            slot.recv_buf.copy_within(consumed..slot.recv_len, 0);
            slot.recv_len -= consumed;
        }
        return true;
    }

    // Extract command name from the recv_buf safely (copy a few bytes).
    let cmd_view = line.tokens[0];
    let mut cmd_buf = [0u8; 16];
    let cmd_len = (cmd_view.len as usize).min(16);
    cmd_buf[..cmd_len].copy_from_slice(
        &anchor.slots[slot_idx].recv_buf
            [cmd_view.offset as usize..cmd_view.offset as usize + cmd_len],
    );
    let cmd = &cmd_buf[..cmd_len];

    // Storage commands need a trailing data block.
    let is_storage = eq_ascii_ci(cmd, b"set")
        || eq_ascii_ci(cmd, b"add")
        || eq_ascii_ci(cmd, b"replace")
        || eq_ascii_ci(cmd, b"append")
        || eq_ascii_ci(cmd, b"prepend");

    if is_storage {
        if line.count < 5 {
            let s = &mut anchor.slots[slot_idx];
            let _ = append(
                &mut s.send_buf,
                &mut s.send_len,
                b"CLIENT_ERROR bad command line\r\n",
            );
            let consumed = line.line_end;
            if consumed >= s.recv_len {
                s.recv_len = 0;
            } else {
                s.recv_buf.copy_within(consumed..s.recv_len, 0);
                s.recv_len -= consumed;
            }
            return true;
        }
        let bytes_token = line.tokens[4];
        let bytes_value =
            parse_u32_dec(token_bytes(&anchor.slots[slot_idx].recv_buf, bytes_token)).unwrap_or(0);
        let data_start = line.line_end;
        let data_total = data_start + bytes_value as usize + 2; // includes trailing \r\n
        if anchor.slots[slot_idx].recv_len < data_total {
            // Not yet — wait for more bytes.
            return false;
        }
        anchor.cmd_set += 1;
        handle_storage(
            anchor,
            slot_idx,
            &line,
            cmd,
            data_start,
            bytes_value as usize,
        );
        // Consume header line + data + trailing CRLF
        let slot = &mut anchor.slots[slot_idx];
        if data_total >= slot.recv_len {
            slot.recv_len = 0;
        } else {
            slot.recv_buf.copy_within(data_total..slot.recv_len, 0);
            slot.recv_len -= data_total;
        }
        return true;
    }

    // Single-line commands.
    if eq_ascii_ci(cmd, b"get") {
        anchor.cmd_get += (line.count as u64).saturating_sub(1);
        handle_get(anchor, slot_idx, &line, false);
    } else if eq_ascii_ci(cmd, b"gets") {
        anchor.cmd_get += (line.count as u64).saturating_sub(1);
        handle_get(anchor, slot_idx, &line, true);
    } else if eq_ascii_ci(cmd, b"delete") {
        handle_delete(anchor, slot_idx, &line);
    } else if eq_ascii_ci(cmd, b"incr") {
        handle_incr_decr(anchor, slot_idx, &line, true);
    } else if eq_ascii_ci(cmd, b"decr") {
        handle_incr_decr(anchor, slot_idx, &line, false);
    } else if eq_ascii_ci(cmd, b"flush_all") {
        handle_flush_all(anchor, slot_idx);
    } else if eq_ascii_ci(cmd, b"version") {
        let s = &mut anchor.slots[slot_idx];
        handle_version(s);
    } else if eq_ascii_ci(cmd, b"stats") {
        handle_stats(anchor, slot_idx);
    } else if eq_ascii_ci(cmd, b"quit") {
        let s = &mut anchor.slots[slot_idx];
        handle_quit(s);
    } else {
        let s = &mut anchor.slots[slot_idx];
        let _ = append(&mut s.send_buf, &mut s.send_len, b"ERROR\r\n");
    }

    // Consume the line.
    let slot = &mut anchor.slots[slot_idx];
    let consumed = line.line_end;
    if consumed >= slot.recv_len {
        slot.recv_len = 0;
    } else {
        slot.recv_buf.copy_within(consumed..slot.recv_len, 0);
        slot.recv_len -= consumed;
    }
    true
}

// ── KV response decoder ───────────────────────────────────────────────

unsafe fn poll_kv_in(anchor: &mut AnchorState) {
    if anchor.kv_in < 0 {
        return;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return;
    }
    const PER_TICK_KV_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    while processed < PER_TICK_KV_BUDGET {
        let poll = ((*sys).channel_poll)(anchor.kv_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return;
        }
        if !handle_kv_response(anchor) {
            return;
        }
        processed += 1;
    }
}

unsafe fn handle_kv_response(anchor: &mut AnchorState) -> bool {
    let sys = anchor.syscalls;
    let mut hdr = [0u8; 3];
    let n = ((*sys).channel_read)(anchor.kv_in, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    if hdr[0] != MSG_KV_RESPONSE {
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    const RESP_HEAD: usize = 8 + 1 + 1 + 8 + 2;
    if payload_len < RESP_HEAD || payload_len > anchor.scratch.len() {
        return false;
    }
    let n2 = ((*sys).channel_read)(anchor.kv_in, anchor.scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }
    let p = &anchor.scratch[..payload_len];
    let corr_id = u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]]);
    let conn_id = p[8];
    let result = p[9];
    let revision = u64::from_le_bytes([p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17]]);
    let body_len = u16::from_le_bytes([p[18], p[19]]) as usize;
    let body_off = 20;
    if body_off + body_len > payload_len {
        return false;
    }

    // Copy body before mutating slots (scratch + slots both live in AnchorState).
    let mut body = [0u8; 4096];
    let blen = body_len.min(4096);
    body[..blen].copy_from_slice(&anchor.scratch[body_off..body_off + blen]);

    let Some(inflight) = anchor.inflight.remove(corr_id) else {
        return true;
    };
    // The head's conn byte is the SLOT INDEX this anchor stamped on
    // the request (the router echoes it). Validate occupancy — a slot
    // freed while a reply was in flight must not resurrect.
    let slot_idx = conn_id as usize;
    if slot_idx >= MAX_CONNS || anchor.slots[slot_idx].conn_id == SLOT_FREE {
        return true;
    }

    let slot = &mut anchor.slots[slot_idx];
    let mem_op = MemOp::from_u8(inflight.op).unwrap_or(MemOp::Get);
    let key = &inflight.key[..inflight.key_len as usize];
    encode_reply(
        &mut slot.send_buf,
        &mut slot.send_len,
        mem_op,
        key,
        result,
        revision,
        &body[..blen],
    );
    // GET/GETS batches: decrement the pending counter and emit the
    // terminating END\r\n once the final response lands. This ordering
    // (VALUE lines first, then END) is what real memcached clients
    // require — the dispatch-side shortcut of emitting END synchronously
    // caused VALUE bytes to arrive after END and desynchronised parsers.
    if matches!(mem_op, MemOp::Get | MemOp::Gets) && slot.pending_gets > 0 {
        slot.pending_gets -= 1;
        if slot.pending_gets == 0 && slot.pending_get_emit_end {
            slot.pending_get_emit_end = false;
            let _ = append(&mut slot.send_buf, &mut slot.send_len, b"END\r\n");
        }
    }
    true
}

// ── NET-side handling ─────────────────────────────────────────────────

unsafe fn poll_net_in(anchor: &mut AnchorState) {
    if anchor.net_in < 0 {
        return;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return;
    }
    // Drain up to PER_TICK_NET_BUDGET frames per step. The memcache ASCII
    // client (and most line-oriented protocols) emits multiple TCP segments
    // per logical command — header line, value, trailing CRLF — so a
    // one-frame-per-tick limit caps throughput at ~tick_hz / segments_per_op.
    // Loop until the channel drains or the budget is hit, whichever first.
    const PER_TICK_NET_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    while processed < PER_TICK_NET_BUDGET {
        let poll = ((*sys).channel_poll)(anchor.net_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return;
        }
        let buf = anchor.scratch.as_mut_ptr();
        let (msg_type, payload_len) = net_read_frame(&*sys, anchor.net_in, buf, SCRATCH_BUF_SIZE);
        if msg_type == 0 && payload_len == 0 {
            return;
        }
        let mut tmp = [0u8; SCRATCH_BUF_SIZE];
        let copy_len = payload_len.min(SCRATCH_BUF_SIZE);
        core::ptr::copy_nonoverlapping(buf.add(NET_FRAME_HDR), tmp.as_mut_ptr(), copy_len);
        let payload = &tmp[..copy_len];

        dispatch_net_frame(anchor, msg_type, payload);
        processed += 1;
    }
}

unsafe fn dispatch_net_frame(anchor: &mut AnchorState, msg_type: u8, payload: &[u8]) {
    match msg_type {
        NET_MSG_BOUND => {
            // Multi-anchor graphs share `net_out` (broadcast); the
            // BOUND frame carries `[conn_id:u16 LE][local_port:2 LE]` so
            // we only claim the listener whose port matches ours.
            if anchor.phase == AnchorPhase::WaitBound && payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = conn_id(payload);
                    anchor.phase = AnchorPhase::Listening;
                }
            } else if anchor.phase == AnchorPhase::WaitBound && payload.len() >= CONN_ID_LEN {
                anchor.server_conn_id = conn_id(payload);
                anchor.phase = AnchorPhase::Listening;
            }
        }
        NET_MSG_ACCEPTED => {
            // ACCEPTED payload: [conn:u16 LE][local_port:u16 LE].
            if payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port != anchor.listen_port {
                    return;
                }
            }
            if payload.len() >= CONN_ID_LEN {
                let new_id = conn_id(payload);
                if anchor.alloc_slot(new_id).is_none() {
                    let _ = net_send_close(anchor, new_id);
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() > CONN_ID_LEN {
                let conn_id = conn_id(payload);
                let data_slice = &payload[CONN_ID_LEN..];
                if let Some(idx) = anchor.find_slot(conn_id) {
                    handle_client_data(anchor, idx, data_slice);
                }
            }
        }
        NET_MSG_CLOSED => {
            if payload.len() >= CONN_ID_LEN {
                let conn_id = conn_id(payload);
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                }
            }
        }
        NET_MSG_ERROR => {
            // MSG_ERROR is BROADCAST to every anchor sharing linux_net's
            // net_out, so it also carries errors for conns we don't own
            // (notably peer_router's outbound-dial failures on a
            // Raft-dialing node). A blanket `phase = Error` let one such
            // foreign error permanently stop this anchor from serving —
            // dead on every dialing node, fine on pure acceptors. Only
            // react to an error for a conn WE own: free that slot, keep
            // listening. See redis_edge_anchor for the same fix + trace.
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

unsafe fn net_send_bind(anchor: &mut AnchorState) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let port = anchor.listen_port.to_le_bytes();
    let payload = [port[0], port[1]];
    let scratch = anchor.scratch.as_mut_ptr();
    let wrote = net_write_frame(
        &*sys,
        anchor.net_out,
        NET_CMD_BIND,
        payload.as_ptr(),
        2,
        scratch,
        SCRATCH_BUF_SIZE,
    );
    wrote > 0
}

unsafe fn net_send_close(anchor: &mut AnchorState, conn_id: u16) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let payload = conn_id.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    let wrote = net_write_frame(
        &*sys,
        anchor.net_out,
        NET_CMD_CLOSE,
        payload.as_ptr(),
        CONN_ID_LEN,
        scratch,
        SCRATCH_BUF_SIZE,
    );
    wrote > 0
}

unsafe fn net_send_data(anchor: &mut AnchorState, conn_id: u16, data: &[u8]) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let payload_len = CONN_ID_LEN + data.len();
    if payload_len + NET_FRAME_HDR > SCRATCH_BUF_SIZE {
        return false;
    }
    let id = conn_id.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    *scratch = NET_CMD_SEND;
    *scratch.add(1) = (payload_len & 0xFF) as u8;
    *scratch.add(2) = ((payload_len >> 8) & 0xFF) as u8;
    *scratch.add(NET_FRAME_HDR) = id[0];
    *scratch.add(NET_FRAME_HDR + 1) = id[1];
    core::ptr::copy_nonoverlapping(
        data.as_ptr(),
        scratch.add(NET_FRAME_HDR + CONN_ID_LEN),
        data.len(),
    );
    let total = NET_FRAME_HDR + payload_len;
    let n = ((*sys).channel_write)(anchor.net_out, scratch, total);
    n == total as i32
}

fn handle_client_data(anchor: &mut AnchorState, slot_idx: usize, data: &[u8]) {
    {
        let slot = &mut anchor.slots[slot_idx];
        let avail = RECV_BUF_SIZE - slot.recv_len;
        if data.len() > avail {
            let _ = append(
                &mut slot.send_buf,
                &mut slot.send_len,
                b"SERVER_ERROR overflow\r\n",
            );
            slot.phase = SlotPhase::Closing;
            return;
        }
        let n = slot.recv_len;
        slot.recv_buf[n..n + data.len()].copy_from_slice(data);
        slot.recv_len = n + data.len();
    }

    // Drain as many complete commands as possible.
    unsafe { while dispatch_one(anchor, slot_idx) {} }
}

unsafe fn flush_slots(anchor: &mut AnchorState) {
    let mut i = 0;
    while i < MAX_CONNS {
        let (conn_id, send_len, phase) = {
            let s = &anchor.slots[i];
            (s.conn_id, s.send_len, s.phase)
        };
        if conn_id == SLOT_FREE {
            i += 1;
            continue;
        }
        if send_len > 0 {
            let mut tmp = [0u8; SEND_BUF_SIZE];
            tmp[..send_len].copy_from_slice(&anchor.slots[i].send_buf[..send_len]);
            if net_send_data(anchor, conn_id, &tmp[..send_len]) {
                anchor.slots[i].send_len = 0;
            }
        }
        if phase == SlotPhase::Closing && anchor.slots[i].send_len == 0 {
            let _ = net_send_close(anchor, conn_id);
            anchor.free_slot(i);
        }
        i += 1;
    }
}

// ── Module ABI ────────────────────────────────────────────────────────

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

    // Manifest ports:
    //   in:  net_in[0], routed_in[1]
    //   out: net_out[0], kv_out[1], metrics[2]
    // (kv_in is not in the manifest — it's the same channel as
    //  routed_in. The manifest wiring uses `routed_in` for that role.)
    unsafe {
        let sys = &*sys_ptr;
        anchor.routed_in = dev_channel_port(sys, 0, 1);
        anchor.kv_in = anchor.routed_in;
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
    match anchor.phase {
        AnchorPhase::Init => unsafe {
            if net_send_bind(anchor) {
                anchor.phase = AnchorPhase::WaitBound;
            }
        },
        AnchorPhase::WaitBound | AnchorPhase::Listening => {}
        AnchorPhase::Error => return -1,
    }
    unsafe {
        poll_net_in(anchor);
        poll_kv_in(anchor);
        flush_slots(anchor);

        // Phase-14: emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
        if anchor.step_ctr.is_multiple_of(5000) && !anchor.syscalls.is_null() {
            telemetry::emit_counters(
                &*anchor.syscalls,
                anchor.metrics_out,
                &[anchor.cmd_get, anchor.cmd_set, anchor.total_connections],
            );
        }
    }
    0
}
