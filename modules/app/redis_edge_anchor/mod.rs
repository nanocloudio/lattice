//! redis_edge_anchor — RESP2 stream anchor for Redis-compatible clients.
//!
//! Continuity class: `drain_only` (sticky routing is an optimisation,
//! not a guarantee). CAS correctness comes from `kv_state_worker`
//! revision fences downstream, not from connection continuity here.
//!
//! ## What this module owns
//!
//! - TCP bind + accept against foundation/ip (NET_CMD_BIND /
//!   NET_MSG_BOUND / NET_MSG_ACCEPTED state machine — the same one
//!   foundation/http uses today; collapses to a thin facade once the
//!   foundation `stream_anchor_core` is promoted).
//! - Per-connection slot table with RESP2 parser state and pipeline
//!   ordering. RESP3 negotiation via HELLO is recognised; the parser
//!   currently treats only RESP2 — RESP3 framing extensions land in
//!   a follow-up.
//! - Inline session commands: PING, ECHO, QUIT, COMMAND, CLIENT,
//!   HELLO, AUTH, SELECT, RESET. AUTH validates against the configured
//!   `requirepass` param (empty => open server); ACL/username-scoped
//!   principals are the auth_manager Phase-5 path.
//! - MULTI / EXEC / DISCARD queue state in the slot (per
//!   `.context/native_fluxor.md`: anchor owns pipelining order and
//!   MULTI queueing; kv_state_worker owns the txn fence).
//! - PUBSUB session state in the slot. PUBLISH still routes through
//!   kv_out because delivery is durable via kv_state_worker.
//!
//! ## What this module forwards
//!
//! Every other command (GET, SET, DEL, INCR, MGET, …) is wrapped in
//! a MSG_KV_REQUEST envelope on `kv_out`. Responses arrive on `kv_in`
//! as MSG_KV_RESPONSE; the anchor looks up the slot by `corr_id`,
//! RESP-encodes the result, and ships it via NET_CMD_SEND.
//!
//! ## Phase 0 scope
//!
//! Worked-example module of the Fluxor-native migration (see
//! `.context/migration_plan.md`). The KV envelope round-trip is
//! wired; the kv_request_router stub is still a no-op, so forwarded
//! commands will time out until Phase 1 fills in the router. The
//! inline command set is enough to PING / AUTH / HELLO / QUIT a
//! Redis client end-to-end against this module today.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface. unreachable_patterns: defensive `_ => Error` arms in match statements are intentional — adding a new variant should not silently bypass the error path"
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

#[path = "../../common/redis_codec.rs"]
mod redis_codec;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use redis_codec::{
    enc_array_header, enc_bulk, enc_error, enc_integer, enc_null_bulk, enc_raw, enc_simple_str,
    eq_ascii_ci, itoa, parse_i64, parse_one, ArgView, Argv, Built, ParseStep,
};
use types::PROTO_REDIS;
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── NET protocol constants (foundation/ip Stream Surface v1) ──────────
//
// Shared with every TCP edge anchor via `modules/common/net_proto.rs`.

#[path = "../../common/net_proto.rs"]
mod net_proto;
use net_proto::{
    net_conn_id, NET_CMD_BIND, NET_CMD_CLOSE, NET_CMD_SEND, NET_CONN_LEN, NET_MSG_ACCEPTED,
    NET_MSG_BOUND, NET_MSG_CLOSED, NET_MSG_DATA, NET_MSG_ERROR,
};

// ── Per-slot capacities ───────────────────────────────────────────────

/// Active client slots. Sized for the bare-metal Pi 5 deployment
/// (`configs/bare-metal-pi5.yaml`) where concurrent client connection
/// count is the throughput axis. 64 slots × ~8 KiB per slot ≈ 512 KiB
/// of state arena — comfortable for the 8 GB Pi 5.
const MAX_CONNS: usize = 64;

/// Per-slot recv buffer (partial RESP frames accumulate here).
const RECV_BUF_SIZE: usize = 4096;

/// Per-slot send buffer (RESP-encoded responses awaiting wire delivery).
const SEND_BUF_SIZE: usize = 4096;

/// Scratch for NET_CMD frames and KV envelopes.
const SCRATCH_BUF_SIZE: usize = 4096 + 32;

// `MAX_ARGS` (the per-command argv cap) lives in `redis_codec.rs`
// alongside the parser that enforces it. The anchor's MULTI queue
// and pubsub caps below are anchor-specific session state and stay
// here.

/// MULTI queue depth per slot.
const MAX_QUEUED: usize = 32;

/// PubSub subscription cap per slot.
const MAX_SUBS: usize = 16;

const DEFAULT_LISTEN_PORT: u16 = 6379;
const DEFAULT_TENANT: u32 = 0;

/// Max configured `requirepass` length. 128 bytes covers redis password
/// conventions with room to spare.
const REQUIREPASS_MAX: usize = 128;

define_params! {
    AnchorState;

    // TCP port the anchor binds for RESP clients. Overridable so
    // multi-replica localhost bring-up (L2: 3× fluxor-linux, one
    // anchor each) doesn't collide on 6379.
    1, listen_port, u16, 6379
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 6379); };

    // Server password (redis `requirepass`). Empty => no auth required
    // (open/anonymous). When set, unauthenticated conns get NOAUTH on
    // every command except AUTH/HELLO/QUIT/RESET, and AUTH validates
    // against it in constant time.
    2, requirepass, blob, 0
        => |s, d, len| {
            let take = (len).min(REQUIREPASS_MAX);
            for i in 0..take { s.requirepass[i] = *d.add(i); }
            s.requirepass_len = take as u8;
        };
}

/// u16 conn-id sentinel: the ip stack's ids are monotone u16s, so
/// 0xFFFF is unreachable long before the tables bind.
const SLOT_FREE: u16 = 0xFFFF;

// ── Per-conn response reorder buffer (task #17) ───────────────────────
//
// The router (kv_request_router) now lets genuinely-independent same-conn
// ops (different keys) dispatch across its DIRECT / CONSENSUS / FENCE
// paths concurrently instead of serializing every path transition — see
// its per-key dependency barrier. Concurrent paths complete out of order
// (a fast DIRECT read overtakes a slow CONSENSUS write on a *different*
// key), so the anchor must restore per-conn FIFO at the client edge.
//
// Each command a slot issues reserves an ordered reorder entry. Forwarded
// ops carry the router `corr_id` and fill their entry when MSG_KV_RESPONSE
// arrives; inline replies (PING, etc.) fill immediately. The flusher only
// releases a contiguous run of ready entries from the head, so the client
// always sees replies in issue order regardless of completion order.
//
// Read-your-writes and no-reflecting-later-writes are NOT this buffer's
// job — the router keeps same-key cross-path ops serialized. This buffer
// only reorders responses whose ops the router already proved independent.

/// In-flight reply depth per conn. The command loop forwards a whole
/// recv burst before any response drains, so the ring must hold the full
/// pipeline, not just the out-of-order tail. A 4 KiB recv_buf holds ~128
/// minimal commands; 256 covers that with headroom and matches the
/// router's MAX_INFLIGHT. A client pipelining deeper than this is
/// fail-closed (correctness over unbounded buffering). In-order
/// completion (the common case) costs O(1) per reply and no arena.
const RO_DEPTH: usize = 256;

/// Per-slot arena for stashed out-of-order reply bytes (entries that
/// arrived before the reorder head). Sized for a couple of max replies;
/// overflow fails the conn closed.
const RO_ARENA: usize = 8192;

#[derive(Clone, Copy)]
struct RoEntry {
    /// Router correlation id for a forwarded op; 0 for an inline reply
    /// (already ready at push time, no async match needed).
    corr: u64,
    /// Reply bytes have arrived.
    ready: bool,
    /// Stashed reply lives in `ro_arena[off..off+len]` (only set for a
    /// ready entry that was NOT the head when it filled).
    off: u16,
    len: u16,
}

impl RoEntry {
    const fn empty() -> Self {
        Self {
            corr: 0,
            ready: false,
            off: 0,
            len: 0,
        }
    }
}

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
    protocol: u8,
    auth: u8,

    db_index: u16,
    tenant: u32,

    in_multi: bool,
    multi_count: u16,
    in_pubsub: bool,
    sub_count: u16,

    recv_buf: [u8; RECV_BUF_SIZE],
    recv_len: usize,

    send_buf: [u8; SEND_BUF_SIZE],
    send_len: usize,

    // Reply reorder ring (issue order). See RoEntry / RO_DEPTH above.
    ro: [RoEntry; RO_DEPTH],
    ro_head: u16,
    ro_tail: u16,
    ro_count: u16,
    ro_arena: [u8; RO_ARENA],
    ro_used: u16,
    /// Set when the reorder buffer overflowed (ring or arena) — the conn
    /// can no longer guarantee order, so it is closed.
    ro_failed: bool,
    /// Steps the ring head has been un-ready without any release. A lost
    /// or dropped response (router inflight-full drop, force-settle) would
    /// otherwise stall the whole conn head-of-line forever; past
    /// RO_STALL_LIMIT the conn is failed closed. Reset on every release.
    ro_stall: u16,
}

impl Slot {
    const fn free() -> Self {
        Self {
            conn_id: SLOT_FREE,
            phase: SlotPhase::Open,
            protocol: 2,
            auth: 0,
            db_index: 0,
            tenant: DEFAULT_TENANT,
            in_multi: false,
            multi_count: 0,
            in_pubsub: false,
            sub_count: 0,
            recv_buf: [0; RECV_BUF_SIZE],
            recv_len: 0,
            send_buf: [0; SEND_BUF_SIZE],
            send_len: 0,
            ro: [RoEntry::empty(); RO_DEPTH],
            ro_head: 0,
            ro_tail: 0,
            ro_count: 0,
            ro_arena: [0; RO_ARENA],
            ro_used: 0,
            ro_failed: false,
            ro_stall: 0,
        }
    }

    fn reset_session(&mut self) {
        self.protocol = 2;
        self.auth = 0;
        self.db_index = 0;
        self.tenant = DEFAULT_TENANT;
        self.in_multi = false;
        self.multi_count = 0;
        self.in_pubsub = false;
        self.sub_count = 0;
        self.recv_len = 0;
        self.send_len = 0;
        self.ro_reset();
    }

    /// Clear all reorder state (session reset / slot reuse).
    fn ro_reset(&mut self) {
        self.ro = [RoEntry::empty(); RO_DEPTH];
        self.ro_head = 0;
        self.ro_tail = 0;
        self.ro_count = 0;
        self.ro_used = 0;
        self.ro_failed = false;
        self.ro_stall = 0;
    }

    /// True while any issued reply is still pending release — inline
    /// replies must then also route through the ring to keep order.
    fn ro_active(&self) -> bool {
        self.ro_count > 0
    }

    /// Reserve the next ordered reorder slot for a forwarded op (fills
    /// later, keyed by `corr`). Returns false if the ring is full — the
    /// caller fails the conn closed.
    fn ro_reserve(&mut self, corr: u64) -> bool {
        if self.ro_failed {
            return false;
        }
        if self.ro_count as usize >= RO_DEPTH {
            self.ro_failed = true;
            return false;
        }
        let idx = self.ro_tail as usize;
        self.ro[idx] = RoEntry {
            corr,
            ready: false,
            off: 0,
            len: 0,
        };
        self.ro_tail = ((self.ro_tail as usize + 1) % RO_DEPTH) as u16;
        self.ro_count += 1;
        true
    }

    /// Append an already-ready reply (inline command) into the ring in
    /// issue order. Returns false on overflow (caller fails the conn).
    fn ro_push_ready(&mut self, reply: &[u8]) -> bool {
        if self.ro_failed || self.ro_count as usize >= RO_DEPTH {
            self.ro_failed = true;
            return false;
        }
        let idx = self.ro_tail as usize;
        self.ro[idx] = RoEntry {
            corr: 0,
            ready: true,
            off: 0,
            len: 0,
        };
        self.ro_tail = ((self.ro_tail as usize + 1) % RO_DEPTH) as u16;
        self.ro_count += 1;
        // If it landed at the head it can flush straight away; otherwise
        // stash. `ro_stash_and_mark` handles both, keyed by ring position.
        if !self.ro_stash(idx, reply) {
            return false;
        }
        self.ro_drain();
        true
    }

    /// Fill a forwarded op's reply by `corr`, then release any newly
    /// contiguous head run. Returns false on arena overflow.
    fn ro_fill(&mut self, corr: u64, reply: &[u8]) -> bool {
        if self.ro_failed {
            return false;
        }
        // Locate the (unique, oldest) not-ready entry with this corr.
        let mut found: Option<usize> = None;
        for step in 0..self.ro_count as usize {
            let idx = (self.ro_head as usize + step) % RO_DEPTH;
            if self.ro[idx].corr == corr && !self.ro[idx].ready {
                found = Some(idx);
                break;
            }
        }
        let Some(idx) = found else {
            // No matching outstanding entry (late/duplicate reply after a
            // reset). Drop it; ordering of live entries is unaffected.
            return true;
        };
        if !self.ro_stash(idx, reply) {
            return false;
        }
        self.ro_drain();
        true
    }

    /// Mark ring entry `idx` ready and record its reply. The head is
    /// still stashed (drained immediately after by `ro_drain`); keeping
    /// one path is simpler than a special-case direct flush. Returns
    /// false on arena overflow.
    fn ro_stash(&mut self, idx: usize, reply: &[u8]) -> bool {
        let len = reply.len();
        if len > u16::MAX as usize || self.ro_used as usize + len > RO_ARENA {
            self.ro_failed = true;
            return false;
        }
        let off = self.ro_used as usize;
        self.ro_arena[off..off + len].copy_from_slice(reply);
        self.ro_used += len as u16;
        self.ro[idx].off = off as u16;
        self.ro[idx].len = len as u16;
        self.ro[idx].ready = true;
        true
    }

    /// Release the contiguous run of ready entries at the head into
    /// send_buf, in issue order. Compacts the arena once the run drains.
    fn ro_drain(&mut self) {
        let start_count = self.ro_count;
        while self.ro_count > 0 {
            let head = self.ro_head as usize;
            if !self.ro[head].ready {
                break;
            }
            let off = self.ro[head].off as usize;
            let len = self.ro[head].len as usize;
            // Append to send_buf (bounded — drop on overflow keeps the
            // wire well-formed; the client sees a truncated pipeline and
            // reconnects, same failure mode as any send_buf overflow).
            if self.send_len + len <= SEND_BUF_SIZE {
                self.send_buf[self.send_len..self.send_len + len]
                    .copy_from_slice(&self.ro_arena[off..off + len]);
                self.send_len += len;
            }
            self.ro[head] = RoEntry::empty();
            self.ro_head = ((head + 1) % RO_DEPTH) as u16;
            self.ro_count -= 1;
        }
        if self.ro_count < start_count {
            // Made progress — reset the head-of-line stall watchdog.
            self.ro_stall = 0;
        }
        if self.ro_count == 0 {
            // Ring empty → reclaim the whole arena.
            self.ro_used = 0;
        } else {
            self.ro_compact();
        }
    }

    /// Rebuild the arena from the still-stashed (ready, not-yet-released)
    /// entries so drained bytes are reclaimed even under a conn that
    /// never fully quiesces. O(RO_DEPTH × reply) and bounded.
    fn ro_compact(&mut self) {
        let mut scratch = [0u8; RO_ARENA];
        let mut used = 0usize;
        for step in 0..self.ro_count as usize {
            let idx = (self.ro_head as usize + step) % RO_DEPTH;
            if self.ro[idx].ready {
                let off = self.ro[idx].off as usize;
                let len = self.ro[idx].len as usize;
                scratch[used..used + len].copy_from_slice(&self.ro_arena[off..off + len]);
                self.ro[idx].off = used as u16;
                used += len;
            }
        }
        self.ro_arena[..used].copy_from_slice(&scratch[..used]);
        self.ro_used = used as u16;
    }
}

/// Constant-time byte-slice equality. Used for password comparison so a
/// match doesn't leak position-of-first-difference timing. Differing
/// lengths are unequal (and compared in constant time within `a`).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// True iff `pass` matches the configured `requirepass`. Always false
/// when no password is configured — an unconfigured server has no valid
/// AUTH credential (callers gate on `requirepass_len` for open access).
fn requirepass_matches(anchor: &AnchorState, pass: &[u8]) -> bool {
    let n = anchor.requirepass_len as usize;
    n != 0 && ct_eq(pass, &anchor.requirepass[..n])
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
struct AnchorState {
    syscalls: *const SyscallTable,

    // Ports — discovered in module_new via dev_channel_port.
    net_in: i32,
    net_out: i32,
    kv_in: i32,
    kv_out: i32,
    auth_request_out: i32,
    auth_decision_in: i32,
    disconnect_in: i32,
    metrics_out: i32,

    listen_port: u16,
    phase: AnchorPhase,
    _pad0: u8,

    server_conn_id: u16,
    _pad1: [u8; 3],

    corr_seq: u64,

    // Configured AUTH password (redis `requirepass`). `requirepass_len`
    // == 0 means no auth is required and the server is open (the
    // anonymous Phase-0 model). See `handle_auth`.
    requirepass: [u8; REQUIREPASS_MAX],
    requirepass_len: u8,
    _pad2: [u8; 7],

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=commands, 1=forwarded, 2=net_errors,
    // 3=auth_failures).
    m_commands: u64,
    m_forwarded: u64,
    m_net_errors: u64,
    m_auth_failures: u64,
    step_ctr: u64,

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
        self.auth_request_out = -1;
        self.auth_decision_in = -1;
        self.disconnect_in = -1;
        self.metrics_out = -1;

        self.listen_port = DEFAULT_LISTEN_PORT;
        self.phase = AnchorPhase::Init;
        self._pad0 = 0;
        self.server_conn_id = 0;
        self._pad1 = [0; 3];
        self.corr_seq = 0;
        self.requirepass = [0; REQUIREPASS_MAX];
        self.requirepass_len = 0;
        self._pad2 = [0; 7];
        self.m_commands = 0;
        self.m_forwarded = 0;
        self.m_net_errors = 0;
        self.m_auth_failures = 0;
        self.step_ctr = 0;

        let mut i = 0;
        while i < MAX_CONNS {
            self.slots[i] = Slot::free();
            i += 1;
        }
    }

    fn alloc_slot(&mut self, conn_id: u16) -> Option<usize> {
        for (i, slot) in self.slots.iter_mut().enumerate() {
            if slot.conn_id == SLOT_FREE {
                slot.conn_id = conn_id;
                slot.phase = SlotPhase::Open;
                slot.reset_session();
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
        // Top byte = protocol id. Each anchor namespaces its own
        // corr_id range so the shared router inflight table (keyed by
        // full u64) can't collide across anchors. PROTO_REDIS = 0x02
        // → ids land in 0x02_00_00_00_00_00_00_01 .. 0x02_FF_FF_FF_FF_FF_FF_FF.
        self.corr_seq = self.corr_seq.wrapping_add(1);
        if self.corr_seq == 0 {
            self.corr_seq = 1;
        }
        (u64::from(PROTO_REDIS) << 56) | (self.corr_seq & 0x00FF_FFFF_FFFF_FFFF)
    }
}

// ── Command dispatch ──────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Dispatch {
    /// Response encoded into slot.send_buf. Caller flushes.
    Handled,
    /// Forwarded as MSG_KV_REQUEST on `kv_out`. Response arrives later
    /// on `kv_in` and is encoded then.
    Forwarded,
    /// Unrecoverable — close the slot.
    Fatal,
}

/// Copy the command name (capped at 32 bytes) out of recv_buf so the
/// dispatcher can drop the recv_buf borrow before mutating the slot
/// for response encoding.
fn copy_cmd_name(slot: &Slot, argv: &Argv) -> ([u8; 32], usize) {
    let v = argv.args[0];
    let mut out = [0u8; 32];
    let len = (v.len as usize).min(32);
    out[..len].copy_from_slice(&slot.recv_buf[v.offset as usize..v.offset as usize + len]);
    (out, len)
}

fn dispatch(anchor: &mut AnchorState, slot_idx: usize, argv: &Argv) -> Dispatch {
    if argv.count == 0 {
        return Dispatch::Handled;
    }
    anchor.m_commands += 1;

    let (cmd_buf, cmd_len) = copy_cmd_name(&anchor.slots[slot_idx], argv);
    let cmd = &cmd_buf[..cmd_len];

    // Inline session commands.
    if eq_ascii_ci(cmd, b"PING") {
        return handle_ping(&mut anchor.slots[slot_idx], argv);
    }
    if eq_ascii_ci(cmd, b"ECHO") {
        return handle_echo(&mut anchor.slots[slot_idx], argv);
    }
    if eq_ascii_ci(cmd, b"QUIT") {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_simple_str(&mut s.send_buf, &mut s.send_len, b"OK");
        s.phase = SlotPhase::Closing;
        return Dispatch::Handled;
    }
    if eq_ascii_ci(cmd, b"COMMAND") {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_array_header(&mut s.send_buf, &mut s.send_len, 0);
        return Dispatch::Handled;
    }
    if eq_ascii_ci(cmd, b"CLIENT") {
        return handle_client(&mut anchor.slots[slot_idx], argv);
    }
    if eq_ascii_ci(cmd, b"HELLO") {
        return handle_hello(anchor, slot_idx, argv);
    }
    if eq_ascii_ci(cmd, b"AUTH") {
        return handle_auth(anchor, slot_idx, argv);
    }
    if eq_ascii_ci(cmd, b"SELECT") {
        return handle_select(&mut anchor.slots[slot_idx], argv);
    }
    if eq_ascii_ci(cmd, b"RESET") {
        let s = &mut anchor.slots[slot_idx];
        s.reset_session();
        let _ = enc_simple_str(&mut s.send_buf, &mut s.send_len, b"RESET");
        return Dispatch::Handled;
    }
    if eq_ascii_ci(cmd, b"MULTI") {
        return handle_multi(&mut anchor.slots[slot_idx]);
    }
    if eq_ascii_ci(cmd, b"DISCARD") {
        return handle_discard(&mut anchor.slots[slot_idx]);
    }
    if eq_ascii_ci(cmd, b"EXEC") {
        return handle_exec(&mut anchor.slots[slot_idx]);
    }
    if eq_ascii_ci(cmd, b"SUBSCRIBE")
        || eq_ascii_ci(cmd, b"PSUBSCRIBE")
        || eq_ascii_ci(cmd, b"UNSUBSCRIBE")
        || eq_ascii_ci(cmd, b"PUNSUBSCRIBE")
    {
        return handle_subscribe_family(&mut anchor.slots[slot_idx], cmd, argv);
    }

    // MULTI queueing.
    {
        let auth_required = anchor.requirepass_len > 0;
        let s = &mut anchor.slots[slot_idx];
        if s.in_multi {
            if s.multi_count as usize >= MAX_QUEUED {
                let _ = enc_error(
                    &mut s.send_buf,
                    &mut s.send_len,
                    b"ERR transaction too large",
                );
                s.in_multi = false;
                s.multi_count = 0;
                return Dispatch::Handled;
            }
            s.multi_count += 1;
            let _ = enc_simple_str(&mut s.send_buf, &mut s.send_len, b"QUEUED");
            return Dispatch::Handled;
        }
        if auth_required && s.auth != 1 {
            let _ = enc_error(
                &mut s.send_buf,
                &mut s.send_len,
                b"NOAUTH Authentication required.",
            );
            return Dispatch::Handled;
        }
    }

    forward_to_router(anchor, slot_idx, cmd, argv)
}

fn handle_ping(slot: &mut Slot, argv: &Argv) -> Dispatch {
    if argv.count >= 2 {
        let v = argv.args[1];
        let off = v.offset as usize;
        let len = v.len as usize;
        // Disjoint field borrow: recv_buf shared, send_buf + send_len mut.
        let payload = &slot.recv_buf[off..off + len];
        if !enc_bulk(&mut slot.send_buf, &mut slot.send_len, payload) {
            return Dispatch::Fatal;
        }
    } else {
        let _ = enc_simple_str(&mut slot.send_buf, &mut slot.send_len, b"PONG");
    }
    Dispatch::Handled
}

fn handle_echo(slot: &mut Slot, argv: &Argv) -> Dispatch {
    if argv.count < 2 {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR wrong number of arguments for 'echo'",
        );
        return Dispatch::Handled;
    }
    let v = argv.args[1];
    let off = v.offset as usize;
    let len = v.len as usize;
    let payload = &slot.recv_buf[off..off + len];
    if !enc_bulk(&mut slot.send_buf, &mut slot.send_len, payload) {
        return Dispatch::Fatal;
    }
    Dispatch::Handled
}

fn handle_client(slot: &mut Slot, argv: &Argv) -> Dispatch {
    if argv.count < 2 {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR wrong number of arguments for 'client'",
        );
        return Dispatch::Handled;
    }
    // Copy subcommand out of recv_buf so we can mutate send_buf freely.
    let v = argv.args[1];
    let off = v.offset as usize;
    let len = v.len as usize;
    let mut sub = [0u8; 32];
    let actual = len.min(32);
    sub[..actual].copy_from_slice(&slot.recv_buf[off..off + actual]);
    let sub = &sub[..actual];

    let conn_id_as_int = slot.conn_id as i64;

    if eq_ascii_ci(sub, b"GETNAME") {
        let _ = enc_null_bulk(&mut slot.send_buf, &mut slot.send_len);
    } else if eq_ascii_ci(sub, b"SETNAME")
        || eq_ascii_ci(sub, b"NO-EVICT")
        || eq_ascii_ci(sub, b"NO-TOUCH")
        || eq_ascii_ci(sub, b"REPLY")
    {
        let _ = enc_simple_str(&mut slot.send_buf, &mut slot.send_len, b"OK");
    } else if eq_ascii_ci(sub, b"ID") {
        let _ = enc_integer(&mut slot.send_buf, &mut slot.send_len, conn_id_as_int);
    } else {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR unknown CLIENT subcommand",
        );
    }
    Dispatch::Handled
}

fn handle_hello(anchor: &mut AnchorState, slot_idx: usize, argv: &Argv) -> Dispatch {
    let auth_required = anchor.requirepass_len > 0;

    // Parse the optional protover and an optional `AUTH user pass`
    // clause: `HELLO [protover [AUTH username password] [SETNAME name]]`.
    let mut new_protocol = anchor.slots[slot_idx].protocol;
    let mut proto_bad = false;
    let mut auth_ok = !auth_required; // open server: already authenticated
    {
        let s = &anchor.slots[slot_idx];
        if argv.count >= 2 {
            let v = argv.args[1];
            let bytes = &s.recv_buf[v.offset as usize..v.offset as usize + v.len as usize];
            // protover is optional; only validate when it parses numeric.
            if let Some(n) = parse_i64(bytes) {
                if n == 2 || n == 3 {
                    new_protocol = n as u8;
                } else {
                    proto_bad = true;
                }
            }
        }
        let mut i = 2usize;
        while i < argv.count as usize {
            let tok = argv.args[i];
            let name = &s.recv_buf[tok.offset as usize..tok.offset as usize + tok.len as usize];
            if eq_ascii_ci(name, b"AUTH") && i + 2 < argv.count as usize {
                let pass_tok = argv.args[i + 2];
                let lo = pass_tok.offset as usize;
                let pass = &s.recv_buf[lo..lo + pass_tok.len as usize];
                auth_ok = !auth_required || requirepass_matches(anchor, pass);
                i += 3;
                continue;
            }
            i += 1;
        }
    }

    if proto_bad {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_error(
            &mut s.send_buf,
            &mut s.send_len,
            b"NOPROTO unsupported protocol version",
        );
        return Dispatch::Handled;
    }
    if !auth_ok {
        anchor.m_auth_failures += 1;
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_error(
            &mut s.send_buf,
            &mut s.send_len,
            b"WRONGPASS invalid username-password pair or user is disabled.",
        );
        return Dispatch::Handled;
    }

    let slot = &mut anchor.slots[slot_idx];
    slot.protocol = new_protocol;
    slot.auth = 1;

    let conn_id_as_int = slot.conn_id as i64;
    let proto_as_int = new_protocol as i64;

    let pairs_count: i64 = 7;
    let _ = enc_array_header(&mut slot.send_buf, &mut slot.send_len, pairs_count * 2);
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"server");
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"lattice");
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"version");
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"0.2.0");
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"proto");
    let _ = enc_integer(&mut slot.send_buf, &mut slot.send_len, proto_as_int);
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"id");
    let _ = enc_integer(&mut slot.send_buf, &mut slot.send_len, conn_id_as_int);
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"mode");
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"standalone");
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"role");
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"master");
    let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, b"modules");
    let _ = enc_array_header(&mut slot.send_buf, &mut slot.send_len, 0);
    Dispatch::Handled
}

/// `AUTH password` or `AUTH username password`. Validates against the
/// configured `requirepass` in constant time. Matches real-redis
/// semantics: an unconfigured server rejects AUTH outright rather than
/// silently accepting any password (the old stub returned OK for
/// everything, which masked misconfiguration). Username, if supplied, is
/// ignored — single-password auth only; ACL users are the auth_manager
/// Phase-5 path.
fn handle_auth(anchor: &mut AnchorState, slot_idx: usize, argv: &Argv) -> Dispatch {
    if argv.count < 2 {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_error(
            &mut s.send_buf,
            &mut s.send_len,
            b"ERR wrong number of arguments for 'auth' command",
        );
        return Dispatch::Handled;
    }

    if anchor.requirepass_len == 0 {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_error(&mut s.send_buf, &mut s.send_len,
            b"ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?");
        return Dispatch::Handled;
    }

    // Password is the final argument (`AUTH pass` or `AUTH user pass`).
    let pass_arg = argv.args[argv.count as usize - 1];
    let ok = {
        let s = &anchor.slots[slot_idx];
        let lo = pass_arg.offset as usize;
        let hi = lo + pass_arg.len as usize;
        requirepass_matches(anchor, &s.recv_buf[lo..hi])
    };

    if !ok {
        anchor.m_auth_failures += 1;
    }
    let s = &mut anchor.slots[slot_idx];
    if ok {
        s.auth = 1;
        let _ = enc_simple_str(&mut s.send_buf, &mut s.send_len, b"OK");
    } else {
        let _ = enc_error(
            &mut s.send_buf,
            &mut s.send_len,
            b"WRONGPASS invalid username-password pair or user is disabled.",
        );
    }
    Dispatch::Handled
}

fn handle_select(slot: &mut Slot, argv: &Argv) -> Dispatch {
    if argv.count < 2 {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR wrong number of arguments for 'select'",
        );
        return Dispatch::Handled;
    }
    let v = argv.args[1];
    let idx_opt = parse_i64(&slot.recv_buf[v.offset as usize..v.offset as usize + v.len as usize]);
    let Some(idx) = idx_opt else {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR invalid DB index",
        );
        return Dispatch::Handled;
    };
    if !(0..=15).contains(&idx) {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR DB index is out of range",
        );
        return Dispatch::Handled;
    }
    slot.db_index = idx as u16;
    let _ = enc_simple_str(&mut slot.send_buf, &mut slot.send_len, b"OK");
    Dispatch::Handled
}

fn handle_multi(slot: &mut Slot) -> Dispatch {
    if slot.in_multi {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR MULTI calls can not be nested",
        );
        return Dispatch::Handled;
    }
    slot.in_multi = true;
    slot.multi_count = 0;
    let _ = enc_simple_str(&mut slot.send_buf, &mut slot.send_len, b"OK");
    Dispatch::Handled
}

fn handle_discard(slot: &mut Slot) -> Dispatch {
    if !slot.in_multi {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR DISCARD without MULTI",
        );
        return Dispatch::Handled;
    }
    slot.in_multi = false;
    slot.multi_count = 0;
    let _ = enc_simple_str(&mut slot.send_buf, &mut slot.send_len, b"OK");
    Dispatch::Handled
}

fn handle_exec(slot: &mut Slot) -> Dispatch {
    if !slot.in_multi {
        let _ = enc_error(
            &mut slot.send_buf,
            &mut slot.send_len,
            b"ERR EXEC without MULTI",
        );
        return Dispatch::Handled;
    }
    let count = slot.multi_count as i64;
    slot.in_multi = false;
    slot.multi_count = 0;
    let _ = enc_array_header(&mut slot.send_buf, &mut slot.send_len, count);
    let mut i = 0;
    while i < count {
        let _ = enc_simple_str(&mut slot.send_buf, &mut slot.send_len, b"OK");
        i += 1;
    }
    Dispatch::Handled
}

fn handle_subscribe_family(slot: &mut Slot, cmd: &[u8], argv: &Argv) -> Dispatch {
    let is_sub = eq_ascii_ci(cmd, b"SUBSCRIBE") || eq_ascii_ci(cmd, b"PSUBSCRIBE");
    let is_psub = eq_ascii_ci(cmd, b"PSUBSCRIBE") || eq_ascii_ci(cmd, b"PUNSUBSCRIBE");

    if is_sub {
        slot.in_pubsub = true;
    }
    let n_args = (argv.count as usize).saturating_sub(1);
    let mut i = 0;
    while i < n_args {
        if is_sub {
            slot.sub_count = slot.sub_count.saturating_add(1).min(MAX_SUBS as u16);
        } else if slot.sub_count > 0 {
            slot.sub_count -= 1;
        }
        let v = argv.args[i + 1];
        let off = v.offset as usize;
        let len = v.len as usize;
        let kind: &[u8] = if is_sub {
            if is_psub {
                b"psubscribe"
            } else {
                b"subscribe"
            }
        } else if is_psub {
            b"punsubscribe"
        } else {
            b"unsubscribe"
        };
        let _ = enc_array_header(&mut slot.send_buf, &mut slot.send_len, 3);
        let _ = enc_bulk(&mut slot.send_buf, &mut slot.send_len, kind);
        let channel = &slot.recv_buf[off..off + len];
        if !enc_bulk(&mut slot.send_buf, &mut slot.send_len, channel) {
            return Dispatch::Fatal;
        }
        let sub_count = slot.sub_count as i64;
        let _ = enc_integer(&mut slot.send_buf, &mut slot.send_len, sub_count);
        i += 1;
    }
    if slot.sub_count == 0 {
        slot.in_pubsub = false;
    }
    Dispatch::Handled
}

// ── Router envelope builder ───────────────────────────────────────────

fn forward_to_router(
    anchor: &mut AnchorState,
    slot_idx: usize,
    cmd: &[u8],
    argv: &Argv,
) -> Dispatch {
    if anchor.kv_out < 0 {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_error(&mut s.send_buf, &mut s.send_len, b"ERR router not wired");
        return Dispatch::Handled;
    }

    let mut body = [0u8; SCRATCH_BUF_SIZE - 64];
    // Codec body builder operates on the raw recv_buf slice; the argv
    // views are offsets into it. We pass the full recv_buf because
    // ArgView offsets are absolute, not relative.
    let built = redis_codec::build_op_body(&anchor.slots[slot_idx].recv_buf, cmd, argv, &mut body);

    let (op, consistency, body_len) = match built {
        Built::Ok {
            op,
            consistency,
            body_len,
        } => (op, consistency, body_len),
        Built::BadArgs(msg) => {
            let s = &mut anchor.slots[slot_idx];
            let _ = enc_error(&mut s.send_buf, &mut s.send_len, msg);
            return Dispatch::Handled;
        }
        Built::Unknown => {
            let s = &mut anchor.slots[slot_idx];
            let _ = enc_raw(&mut s.send_buf, &mut s.send_len, b"-ERR unknown command '");
            let _ = enc_raw(&mut s.send_buf, &mut s.send_len, cmd);
            let _ = enc_raw(&mut s.send_buf, &mut s.send_len, b"'\r\n");
            return Dispatch::Handled;
        }
    };

    let corr = anchor.next_corr();
    let tenant = anchor.slots[slot_idx].tenant;

    // Envelope head: [corr:8][proto:1][tenant:4][conn:1][cons:1][op:1][body_len:2] = 18
    let head_len = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    let payload_len = head_len + body_len;
    if payload_len > u16::MAX as usize {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_error(&mut s.send_buf, &mut s.send_len, b"ERR command too large");
        return Dispatch::Handled;
    }
    let total = 3 + payload_len;
    if total > anchor.scratch.len() {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_error(&mut s.send_buf, &mut s.send_len, b"ERR scratch overflow");
        return Dispatch::Handled;
    }

    let scratch = &mut anchor.scratch[..total];
    scratch[0] = MSG_KV_REQUEST;
    scratch[1] = (payload_len & 0xFF) as u8;
    scratch[2] = ((payload_len >> 8) & 0xFF) as u8;

    let mut p = 3;
    scratch[p..p + 8].copy_from_slice(&corr.to_le_bytes());
    p += 8;
    scratch[p] = PROTO_REDIS;
    p += 1;
    scratch[p..p + 4].copy_from_slice(&tenant.to_le_bytes());
    p += 4;
    // The head's conn byte is this anchor's SLOT INDEX (a caller-
    // chosen routing slot the router echoes) — NOT the net conn id,
    // which is u16 now and does not fit the byte.
    scratch[p] = slot_idx as u8;
    p += 1;
    scratch[p] = consistency;
    p += 1;
    scratch[p] = op;
    p += 1;
    scratch[p] = (body_len & 0xFF) as u8;
    scratch[p + 1] = ((body_len >> 8) & 0xFF) as u8;
    p += 2;
    scratch[p..p + body_len].copy_from_slice(&body[..body_len]);

    let sys = anchor.syscalls;
    let kv_out = anchor.kv_out;
    if sys.is_null() {
        return Dispatch::Fatal;
    }
    let written = unsafe { ((*sys).channel_write)(kv_out, anchor.scratch.as_mut_ptr(), total) };
    if written != total as i32 {
        let s = &mut anchor.slots[slot_idx];
        let _ = enc_error(
            &mut s.send_buf,
            &mut s.send_len,
            b"BUSY router backpressured",
        );
        return Dispatch::Handled;
    }
    // Reserve the ordered reorder slot for this in-flight op (issue order).
    // The response fills it later by `corr` in `handle_kv_response`; the
    // flusher only releases from the head, preserving per-conn FIFO even
    // though the router may now complete this op out of order relative to
    // an independent same-conn op on another path. Ring full → the conn
    // has pipelined past our bound; fail it closed (order can't be kept).
    if !anchor.slots[slot_idx].ro_reserve(corr) {
        anchor.slots[slot_idx].phase = SlotPhase::Closing;
        return Dispatch::Fatal;
    }
    anchor.m_forwarded += 1;
    Dispatch::Forwarded
}

// ── KV response decoder ───────────────────────────────────────────────

/// Returns true iff it drained the full per-tick budget with input still
/// ready — i.e. productive work happened AND more remains, the "Burst" signal.
unsafe fn poll_kv_in(anchor: &mut AnchorState) -> bool {
    if anchor.kv_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    const PER_TICK_KV_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    while processed < PER_TICK_KV_BUDGET {
        let poll = ((*sys).channel_poll)(anchor.kv_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return false; // drained → no re-pass needed
        }
        if !handle_kv_response(anchor) {
            return false; // couldn't make progress (backpressure/error)
        }
        processed += 1;
    }
    true // hit the budget with more queued → Burst
}

unsafe fn handle_kv_response(anchor: &mut AnchorState) -> bool {
    let sys = anchor.syscalls;
    let mut hdr = [0u8; 3];
    let n = ((*sys).channel_read)(anchor.kv_in, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    if hdr[0] != MSG_KV_RESPONSE {
        return false; // unknown envelope on kv_in
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    // [corr:8][conn_id:1][result:1][revision:8][body_len:2]
    const RESP_HEAD: usize = 8 + 1 + 1 + 8 + 2;
    if payload_len < RESP_HEAD || payload_len > anchor.scratch.len() {
        return false;
    }
    let n2 = ((*sys).channel_read)(anchor.kv_in, anchor.scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    let p = &anchor.scratch[..payload_len];
    let corr = u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]]);
    let conn_id = p[8];
    let result = p[9];
    // p[10..18] = revision (unused in Phase 1 RESP mapping; surfaces
    // later when WATCH revisions and INCR-style numeric responses need it).
    let body_len = u16::from_le_bytes([p[18], p[19]]) as usize;
    let body_off = 20;
    if body_off + body_len > payload_len {
        return false;
    }

    // The head's conn byte is the SLOT INDEX this anchor stamped on
    // the request (the router echoes it). Validate occupancy — a slot
    // freed while a reply was in flight must not resurrect.
    let slot_idx = conn_id as usize;
    if slot_idx >= MAX_CONNS || anchor.slots[slot_idx].conn_id == SLOT_FREE {
        return true;
    }

    // Copy the body out of scratch before mutably borrowing the slot
    // (scratch and slots both live on AnchorState; can't mutably
    // borrow one while reading the other).
    let mut body = [0u8; 4096];
    let blen = body_len.min(4096);
    body[..blen].copy_from_slice(&anchor.scratch[body_off..body_off + blen]);

    // Encode the RESP reply into a temp, then release it through the
    // reorder ring by `corr`, NOT straight to send_buf — the router may
    // have completed this op ahead of an earlier same-conn op on another
    // path. `ro_fill` flushes to send_buf only when this reply reaches
    // the head, preserving per-conn FIFO. Every forwarded op reserves its
    // entry in `forward_to_router`, so a live op always matches; a reply
    // with no matching entry is a late arrival after a session reset and
    // is correctly dropped (that session's pending replies were discarded).
    let mut reply = [0u8; SEND_BUF_SIZE];
    let mut reply_len = 0usize;
    redis_codec::encode_kv_reply(&mut reply, &mut reply_len, result, &body[..blen]);

    let slot = &mut anchor.slots[slot_idx];
    if !slot.ro_fill(corr, &reply[..reply_len]) {
        // Reorder buffer overflowed → order can no longer be guaranteed.
        slot.phase = SlotPhase::Closing;
    }
    true
}

// ── NET-side handling ─────────────────────────────────────────────────

/// Returns true iff it drained the full per-tick budget with input still
/// ready (productive work + more pending) — the Burst signal.
unsafe fn poll_net_in(anchor: &mut AnchorState) -> bool {
    if anchor.net_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    // Drain up to PER_TICK_NET_BUDGET frames per step. A single tick that
    // processes only one frame caps the anchor's throughput at
    // (tick_hz / segments_per_op) — too low for clients that split a
    // logical command into multiple TCP segments.
    const PER_TICK_NET_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    while processed < PER_TICK_NET_BUDGET {
        let poll = ((*sys).channel_poll)(anchor.net_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return false;
        }
        let buf = anchor.scratch.as_mut_ptr();
        let (msg_type, payload_len) = net_read_frame(&*sys, anchor.net_in, buf, SCRATCH_BUF_SIZE);
        if msg_type == 0 && payload_len == 0 {
            return false;
        }
        // Copy payload out of scratch so we can mutate slots without
        // aliasing scratch (the slot table and scratch share AnchorState).
        let mut tmp = [0u8; SCRATCH_BUF_SIZE];
        let copy_len = payload_len.min(SCRATCH_BUF_SIZE);
        core::ptr::copy_nonoverlapping(buf.add(NET_FRAME_HDR), tmp.as_mut_ptr(), copy_len);
        let payload = &tmp[..copy_len];

        dispatch_net_frame(anchor, msg_type, payload);
        processed += 1;
    }
    true // hit the budget with more queued → Burst
}

unsafe fn dispatch_net_frame(anchor: &mut AnchorState, msg_type: u8, payload: &[u8]) {
    let sys = anchor.syscalls;
    match msg_type {
        NET_MSG_BOUND => {
            // Multi-anchor graphs share `net_out` (broadcast); the
            // BOUND frame carries `[conn_id:1][local_port:2 LE]` so
            // we only claim the listener whose port matches ours.
            // Skip otherwise — another anchor on this channel will.
            // BOUND payload: [conn_id:u16 LE][local_port:u16 LE].
            if anchor.phase == AnchorPhase::WaitBound && payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = net_conn_id(payload).unwrap_or(SLOT_FREE);
                    anchor.phase = AnchorPhase::Listening;
                    dev_log(&*sys, 3, b"[redis_anc] bound".as_ptr(), 17);
                }
            } else if anchor.phase == AnchorPhase::WaitBound && payload.len() >= NET_CONN_LEN {
                // Single-anchor provider (no port in payload): claim
                // the first BOUND we see.
                anchor.server_conn_id = net_conn_id(payload).unwrap_or(SLOT_FREE);
                anchor.phase = AnchorPhase::Listening;
                dev_log(&*sys, 3, b"[redis_anc] bound".as_ptr(), 17);
            }
        }
        NET_MSG_ACCEPTED => {
            // Per-port filter: ACCEPTED carries the parent listener's
            // local_port at payload[2..4]. Only claim conn_ids whose
            // listener port matches ours so we don't double-allocate
            // against another anchor sharing `net_out`.
            if payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port != anchor.listen_port {
                    // Belongs to another anchor on this broadcast channel.
                    return;
                }
            }
            if let Some(new_id) = net_conn_id(payload) {
                if anchor.alloc_slot(new_id).is_some() {
                    dev_log(&*sys, 3, b"[redis_anc] accepted".as_ptr(), 20);
                } else {
                    let _ = net_send_close_pic(anchor, new_id);
                    dev_log(
                        &*sys,
                        2,
                        b"[redis_anc] slots full: closed new client".as_ptr(),
                        41,
                    );
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() > NET_CONN_LEN {
                if let Some(conn_id) = net_conn_id(payload) {
                    let data_slice = &payload[NET_CONN_LEN..];
                    if let Some(idx) = anchor.find_slot(conn_id) {
                        handle_client_data(anchor, idx, data_slice);
                    }
                }
            }
        }
        NET_MSG_CLOSED => {
            if let Some(conn_id) = net_conn_id(payload) {
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                    dev_log(&*sys, 3, b"[redis_anc] client closed".as_ptr(), 25);
                }
            }
        }
        NET_MSG_ERROR => {
            // MSG_ERROR is BROADCAST to every anchor sharing linux_net's
            // net_out, so it also carries errors for conns we don't own —
            // notably peer_router's outbound-dial failures on a
            // Raft-dialing node. The old blanket `phase = Error` let one
            // such foreign error permanently stop redis from accepting
            // clients (observed: redis dead on every dialing node, fine on
            // pure-acceptor nodes). Only react to an error for a conn WE
            // own: free that client slot and keep listening. Errors for
            // other conns (peer dials, http) are ignored.
            if let Some(conn_id) = net_conn_id(payload) {
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                    anchor.m_net_errors += 1;
                    dev_log(&*sys, 2, b"[redis_anc] client net error".as_ptr(), 27);
                }
            }
        }
        _ => { /* unknown opcode — ignore */ }
    }
}

unsafe fn net_send_bind_pic(anchor: &mut AnchorState) -> bool {
    let sys = anchor.syscalls;
    let out_chan = anchor.net_out;
    if sys.is_null() || out_chan < 0 {
        return false;
    }
    let port = anchor.listen_port.to_le_bytes();
    let payload = [port[0], port[1]];
    let scratch = anchor.scratch.as_mut_ptr();
    let wrote = net_write_frame(
        &*sys,
        out_chan,
        NET_CMD_BIND,
        payload.as_ptr(),
        2,
        scratch,
        SCRATCH_BUF_SIZE,
    );
    wrote > 0
}

unsafe fn net_send_close_pic(anchor: &mut AnchorState, conn_id: u16) -> bool {
    let sys = anchor.syscalls;
    let out_chan = anchor.net_out;
    if sys.is_null() || out_chan < 0 {
        return false;
    }
    let payload = conn_id.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    let wrote = net_write_frame(
        &*sys,
        out_chan,
        NET_CMD_CLOSE,
        payload.as_ptr(),
        NET_CONN_LEN,
        scratch,
        SCRATCH_BUF_SIZE,
    );
    wrote > 0
}

unsafe fn net_send_data_pic(anchor: &mut AnchorState, conn_id: u16, data: &[u8]) -> bool {
    let sys = anchor.syscalls;
    let out_chan = anchor.net_out;
    if sys.is_null() || out_chan < 0 {
        return false;
    }
    let payload_len = NET_CONN_LEN + data.len();
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
        scratch.add(NET_FRAME_HDR + NET_CONN_LEN),
        data.len(),
    );
    let total = NET_FRAME_HDR + payload_len;
    let n = ((*sys).channel_write)(out_chan, scratch, total);
    n == total as i32
}

/// Append `data` to slot.recv_buf and parse out as many commands as
/// possible.
fn handle_client_data(anchor: &mut AnchorState, slot_idx: usize, data: &[u8]) {
    {
        let slot = &mut anchor.slots[slot_idx];
        let avail = RECV_BUF_SIZE - slot.recv_len;
        if data.len() > avail {
            let _ = enc_error(
                &mut slot.send_buf,
                &mut slot.send_len,
                b"ERR receive buffer full",
            );
            slot.phase = SlotPhase::Closing;
            return;
        }
        let n = slot.recv_len;
        slot.recv_buf[n..n + data.len()].copy_from_slice(data);
        slot.recv_len = n + data.len();
    }

    loop {
        let outcome = {
            let slot = &anchor.slots[slot_idx];
            if slot.recv_len == 0 {
                break;
            }
            match parse_one(&slot.recv_buf[..slot.recv_len]) {
                ParseStep::Incomplete => StepResult::Wait,
                ParseStep::Error(reply) => StepResult::Err(reply),
                ParseStep::Ready { consumed, argv } => StepResult::Ready(consumed, argv),
            }
        };

        match outcome {
            StepResult::Wait => break,
            StepResult::Err(reply) => {
                let slot = &mut anchor.slots[slot_idx];
                let _ = enc_raw(&mut slot.send_buf, &mut slot.send_len, reply);
                slot.phase = SlotPhase::Closing;
                slot.recv_len = 0;
                break;
            }
            StepResult::Ready(consumed, argv) => {
                // Snapshot before dispatch so an inline reply written to
                // send_buf can be re-sequenced through the reorder ring if
                // earlier forwarded ops on this conn are still in flight.
                let was_active = anchor.slots[slot_idx].ro_active();
                let pre_len = anchor.slots[slot_idx].send_len;
                let outcome = dispatch(anchor, slot_idx, &argv);
                if outcome == Dispatch::Fatal {
                    anchor.slots[slot_idx].phase = SlotPhase::Closing;
                    break;
                }
                // Inline (Handled) reply while forwarded ops are pending:
                // move the bytes dispatch just appended into the ring so
                // they release only after the earlier ops, not ahead of
                // them. Forwarded ops reserved their own slot already.
                if outcome == Dispatch::Handled && was_active {
                    let slot = &mut anchor.slots[slot_idx];
                    if slot.send_len > pre_len {
                        let mut reply = [0u8; SEND_BUF_SIZE];
                        let rlen = slot.send_len - pre_len;
                        reply[..rlen].copy_from_slice(&slot.send_buf[pre_len..slot.send_len]);
                        slot.send_len = pre_len; // retract from send_buf
                        if !slot.ro_push_ready(&reply[..rlen]) {
                            slot.phase = SlotPhase::Closing;
                            break;
                        }
                    }
                }
                let slot = &mut anchor.slots[slot_idx];
                if consumed >= slot.recv_len {
                    slot.recv_len = 0;
                } else {
                    slot.recv_buf.copy_within(consumed..slot.recv_len, 0);
                    slot.recv_len -= consumed;
                }
            }
        }
    }
}

enum StepResult {
    Wait,
    Err(&'static [u8]),
    Ready(usize, Argv),
}

/// Flush pending send_buf bytes for each slot as NET_CMD_SEND frames.
/// If a slot is Closing and its send_buf drains, close the connection.
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
            // Copy out so net_send_data_pic can use anchor.scratch
            // without aliasing slot.send_buf.
            let mut tmp = [0u8; SEND_BUF_SIZE];
            tmp[..send_len].copy_from_slice(&anchor.slots[i].send_buf[..send_len]);
            if net_send_data_pic(anchor, conn_id, &tmp[..send_len]) {
                anchor.slots[i].send_len = 0;
            }
        }
        if phase == SlotPhase::Closing && anchor.slots[i].send_len == 0 {
            let _ = net_send_close_pic(anchor, conn_id);
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

    // Discover additional ports.
    // input order  (manifest): net_in[0], kv_in[1], auth_decision[2], disconnect[3]
    // output order (manifest): net_out[0], kv_out[1], auth_request[2], metrics[3]
    unsafe {
        let sys = &*sys_ptr;
        anchor.kv_in = dev_channel_port(sys, 0, 1);
        anchor.auth_decision_in = dev_channel_port(sys, 0, 2);
        anchor.disconnect_in = dev_channel_port(sys, 0, 3);
        anchor.kv_out = dev_channel_port(sys, 1, 1);
        anchor.auth_request_out = dev_channel_port(sys, 1, 2);
        anchor.metrics_out = dev_channel_port(sys, 1, 3);
    }
    0
}

/// Scheduler re-step signal: returning this re-runs the exec-order pass this
/// tick (see fluxor scheduler `StepOutcome::Burst`). Used to drain a backlogged
/// input within one tick instead of one frame per tick.
const STEP_BURST: i32 = 2;

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let anchor = unsafe { &mut *state.cast::<AnchorState>() };

    match anchor.phase {
        AnchorPhase::Init => unsafe {
            if net_send_bind_pic(anchor) {
                anchor.phase = AnchorPhase::WaitBound;
            }
        },
        AnchorPhase::WaitBound | AnchorPhase::Listening => { /* event-driven below */ }
        AnchorPhase::Error => return -1,
    }

    let more = unsafe {
        // Burst-on-productive-work: if either input loop drained its full
        // per-tick budget with more still queued, return STEP_BURST so the
        // scheduler re-passes and the whole pipeline drains this tick instead
        // of one frame/tick. This is what lets the graph batch under load.
        let m_net = poll_net_in(anchor);
        let m_kv = poll_kv_in(anchor);
        ro_stall_sweep(anchor);
        flush_slots(anchor);

        // Phase-14: emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
        if anchor.step_ctr.is_multiple_of(5000) && !anchor.syscalls.is_null() {
            telemetry::emit_counters(
                &*anchor.syscalls,
                anchor.metrics_out,
                &[
                    anchor.m_commands,
                    anchor.m_forwarded,
                    anchor.m_net_errors,
                    anchor.m_auth_failures,
                ],
            );
        }
        m_net || m_kv
    };
    if more {
        STEP_BURST
    } else {
        0
    }
}

/// Steps a conn's reorder ring may sit with an un-ready head before it is
/// failed closed. A lost / dropped response (router inflight-full drop or
/// force-settle) would otherwise stall the whole conn head-of-line
/// forever. Set well above the slowest legitimate op (a quorum write +
/// fence is tens of steps at tick_us=1000); reset on every release.
const RO_STALL_LIMIT: u16 = 8192;

/// Per-step head-of-line watchdog for the reorder ring. Runs after
/// response draining so a ring that released this step is not penalised.
unsafe fn ro_stall_sweep(anchor: &mut AnchorState) {
    for i in 0..MAX_CONNS {
        let slot = &mut anchor.slots[i];
        if slot.conn_id == SLOT_FREE || slot.phase == SlotPhase::Closing {
            continue;
        }
        if slot.ro_count == 0 {
            slot.ro_stall = 0;
            continue;
        }
        slot.ro_stall = slot.ro_stall.saturating_add(1);
        if slot.ro_stall > RO_STALL_LIMIT || slot.ro_failed {
            // Order can no longer be guaranteed — drop the pending replies
            // and close. The client reconnects; same failure mode as any
            // send_buf overflow / protocol error.
            slot.ro_reset();
            slot.phase = SlotPhase::Closing;
        }
    }
}
