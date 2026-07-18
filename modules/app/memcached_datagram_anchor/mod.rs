//! memcached_datagram_anchor — Memcached UDP anchor.
//!
//! Continuity class: `reroutable`. Each UDP request datagram carries
//! a 2-byte request id; the server echoes the same id on the reply.
//! There's no per-client connection state, so reroute / replica
//! migration is transparent to the wire.
//!
//! ## Memcached UDP framing
//!
//! Per memcached.org/protocol — UDP, each datagram (request *and*
//! response) carries an 8-byte frame header followed by the same
//! ASCII protocol the TCP path uses:
//!
//!   `[req_id:u16 BE][seq:u16 BE][total:u16 BE][reserved:u16 = 0]`
//!
//! `req_id` is opaque to the server (echoed verbatim). `seq` and
//! `total` would allow split requests across multiple datagrams; this
//! anchor only handles `seq = 0 && total = 1` (the overwhelming common
//! case) and drops anything else — clients that need to send a body
//! larger than ~1400 bytes must use the TCP anchor instead.
//!
//! ## Supported commands (Phase 1)
//!
//! `get k\r\n` → `VALUE k flags bytes\r\n<data>\r\nEND\r\n`
//! `set k flags exp bytes\r\n<data>\r\n` → `STORED\r\n` / `NOT_STORED\r\n`
//! `delete k\r\n` → `DELETED\r\n` / `NOT_FOUND\r\n`
//!
//! All other commands are answered with `ERROR\r\n` (memcached's
//! generic unknown-command reply). Multi-key `get`, `incr` / `decr`,
//! `add` / `replace` / `append` / `prepend`, and `cas` land later.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
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

use collections::InflightTable;
use memcached_codec::{
    encode_reply, eq_ascii_ci, find_crlf, parse_line, parse_u32_dec, token_bytes, Line, MemOp,
};
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_PUT, KV_RESULT_OK, PROTO_MEMCACHED_UDP, REQ_LINEARIZABLE,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// Datagram opcodes + POLL_IN/POLL_OUT come from the SDK runtime
// (path-included above); the rtp module uses the same constants
// without redeclaring them.

// ── Capacities ────────────────────────────────────────────────────────

const RECV_BUF: usize = 2048;
const SEND_BUF: usize = 2048;
const SCRATCH: usize = 4096;
const MAX_INFLIGHT: usize = 512;

const MEMCACHED_UDP_HEADER: usize = 8;
const DEFAULT_PORT: u16 = 11211;

// ── Inflight table value ─────────────────────────────────────────────

#[derive(Clone, Copy)]
#[repr(C)]
struct InflightVal {
    op: u8,
    /// Echo of the inbound `req_id` so the response datagram carries
    /// the matching memcached UDP frame id.
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
    key_len: u16,
    key: [u8; 256],
}

impl InflightVal {
    const fn new() -> Self {
        Self {
            op: 0,
            req_id: 0,
            src_addr: [0; 4],
            src_port: 0,
            key_len: 0,
            key: [0; 256],
        }
    }
}

// ── Anchor state ──────────────────────────────────────────────────────

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum BindPhase {
    Init = 0,
    Pending = 1,
    Bound = 2,
    Error = 0xFF,
}

#[repr(C)]
struct AnchorState {
    syscalls: *const SyscallTable,

    // Datagram provider channels (port 0 = dgram_in, port 0 out = dgram_out).
    dgram_in: i32,
    dgram_out: i32,
    routed_in: i32,
    kv_out: i32,
    metrics_out: i32,

    bind_phase: BindPhase,
    ep_id: u8,
    local_port: u16,

    corr_seq: u64,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=requests, 1=forwarded, 2=errors).
    m_requests: u64,
    m_forwarded: u64,
    m_errors: u64,
    step_ctr: u64,

    inflight: InflightTable<InflightVal, MAX_INFLIGHT>,

    /// Re-entry buffer for partial CMD_DG_SEND_TO writes (backpressure).
    /// Currently unused — channel_write is best-effort; a failed write
    /// drops the response and the client will retransmit on its own
    /// (memcached UDP timeout is client-side, ~50 ms).
    _pending: u32,

    recv: [u8; RECV_BUF],
    send: [u8; SEND_BUF],
    scratch: [u8; SCRATCH],
}

impl AnchorState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.dgram_in = -1;
        self.dgram_out = -1;
        self.routed_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.bind_phase = BindPhase::Init;
        self.ep_id = 0xFF;
        self.local_port = 0;
        self.corr_seq = 0;
        self.m_requests = 0;
        self.m_forwarded = 0;
        self.m_errors = 0;
        self.step_ctr = 0;
        self.inflight = InflightTable::new();
        self._pending = 0;
    }

    fn next_corr(&mut self) -> u64 {
        // Top byte = protocol id so the shared router inflight table
        // (keyed by full u64) can't collide with other anchors. See
        // redis_edge_anchor::next_corr for the rationale.
        self.corr_seq = self.corr_seq.wrapping_add(1);
        if self.corr_seq == 0 {
            self.corr_seq = 1;
        }
        (u64::from(PROTO_MEMCACHED_UDP) << 56) | (self.corr_seq & 0x00FF_FFFF_FFFF_FFFF)
    }
}

// ── Channel helpers ──────────────────────────────────────────────────

unsafe fn channel_read_frame(sys: &SyscallTable, chan: i32, buf: &mut [u8]) -> Option<(u8, usize)> {
    let mut hdr = [0u8; 3];
    let n = (sys.channel_read)(chan, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return None;
    }
    let msg_type = hdr[0];
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len == 0 {
        return Some((msg_type, 0));
    }
    if payload_len > buf.len() {
        return None;
    }
    let n2 = (sys.channel_read)(chan, buf.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return None;
    }
    Some((msg_type, payload_len))
}

unsafe fn channel_write_frame(
    sys: &SyscallTable,
    chan: i32,
    msg_type: u8,
    payload: &[u8],
    scratch: &mut [u8],
) -> bool {
    if chan < 0 || payload.len() > u16::MAX as usize {
        return false;
    }
    let total = 3 + payload.len();
    if total > scratch.len() {
        return false;
    }
    scratch[0] = msg_type;
    scratch[1] = (payload.len() & 0xFF) as u8;
    scratch[2] = ((payload.len() >> 8) & 0xFF) as u8;
    scratch[3..total].copy_from_slice(payload);
    (sys.channel_write)(chan, scratch.as_mut_ptr(), total) == total as i32
}

// ── Bind state machine ───────────────────────────────────────────────

unsafe fn try_bind(anchor: &mut AnchorState) {
    if anchor.bind_phase != BindPhase::Init {
        return;
    }
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.dgram_out < 0 {
        return;
    }
    let mut body = [0u8; 3];
    body[0..2].copy_from_slice(&DEFAULT_PORT.to_le_bytes());
    body[2] = 0; // flags
    let mut scratch = [0u8; 16];
    if channel_write_frame(&*sys, anchor.dgram_out, DG_CMD_BIND, &body, &mut scratch) {
        anchor.bind_phase = BindPhase::Pending;
    }
}

// ── Inbound: provider → anchor ───────────────────────────────────────

unsafe fn drain_dgram_in(anchor: &mut AnchorState) -> bool {
    if anchor.dgram_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let sys = &*sys;
    let poll = (sys.channel_poll)(anchor.dgram_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    // Read into recv buffer (separate from scratch so request parsing
    // and outbound envelope build don't alias).
    let mut local = [0u8; RECV_BUF];
    let (msg_type, plen) = match channel_read_frame(sys, anchor.dgram_in, &mut local) {
        Some(p) => p,
        None => return false,
    };
    match msg_type {
        DG_MSG_BOUND => {
            // Payload: [ep_id:1][local_port:u16 LE]
            if plen >= 3 {
                anchor.ep_id = local[0];
                anchor.local_port = u16::from_le_bytes([local[1], local[2]]);
                anchor.bind_phase = BindPhase::Bound;
            }
        }
        DG_MSG_RX_FROM => {
            handle_rx_from(anchor, &local[..plen]);
        }
        DG_MSG_CLOSED | DG_MSG_ERROR => {
            anchor.bind_phase = BindPhase::Error;
        }
        _ => {}
    }
    true
}

unsafe fn handle_rx_from(anchor: &mut AnchorState, payload: &[u8]) {
    // [ep_id:1][af:1][src_addr:4 BE][src_port:2 LE][memcached_udp_frame…]
    if payload.len() < DG_V4_PREFIX + MEMCACHED_UDP_HEADER + 2 {
        return;
    }
    let ep_id = payload[0];
    let af = payload[1];
    if ep_id != anchor.ep_id || af != DG_AF_INET {
        return;
    }
    let src_addr = [payload[2], payload[3], payload[4], payload[5]];
    let src_port = u16::from_le_bytes([payload[6], payload[7]]);
    let dgram = &payload[DG_V4_PREFIX..];

    // Memcached UDP frame header: [req_id:u16 BE][seq:u16 BE][total:u16 BE][reserved:u16].
    let req_id = u16::from_be_bytes([dgram[0], dgram[1]]);
    let seq = u16::from_be_bytes([dgram[2], dgram[3]]);
    let total = u16::from_be_bytes([dgram[4], dgram[5]]);
    if seq != 0 || total != 1 {
        // Multi-datagram requests not supported. Echo an ERROR.
        send_error_reply(anchor, req_id, src_addr, src_port);
        return;
    }
    let ascii = &dgram[MEMCACHED_UDP_HEADER..];
    let _ = find_crlf(ascii); // CRLF must be present
    let Some(line) = parse_line(ascii) else {
        send_error_reply(anchor, req_id, src_addr, src_port);
        return;
    };
    dispatch_command(anchor, &line, ascii, req_id, src_addr, src_port);
}

// ── Command dispatch ─────────────────────────────────────────────────

unsafe fn dispatch_command(
    anchor: &mut AnchorState,
    line: &Line,
    request: &[u8],
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
) {
    anchor.m_requests += 1;
    if line.count == 0 {
        send_error_reply(anchor, req_id, src_addr, src_port);
        return;
    }
    let cmd = token_bytes(request, line.tokens[0]);
    if eq_ascii_ci(cmd, b"get") {
        // get <key>
        if line.count < 2 {
            send_error_reply(anchor, req_id, src_addr, src_port);
            return;
        }
        let key = token_bytes(request, line.tokens[1]);
        let mut body = [0u8; 258];
        let mut p = 0;
        body[p] = (key.len() & 0xFF) as u8;
        body[p + 1] = ((key.len() >> 8) & 0xFF) as u8;
        p += 2;
        body[p..p + key.len()].copy_from_slice(key);
        p += key.len();
        send_envelope(
            anchor,
            KV_OP_GET,
            MemOp::Get,
            key,
            req_id,
            src_addr,
            src_port,
            &body[..p],
        );
    } else if eq_ascii_ci(cmd, b"set") {
        // set <key> <flags> <exp> <bytes>\r\n<data>\r\n
        if line.count < 5 {
            send_error_reply(anchor, req_id, src_addr, src_port);
            return;
        }
        let key = token_bytes(request, line.tokens[1]);
        let bytes_tok = token_bytes(request, line.tokens[4]);
        let Some(value_len) = parse_u32_dec(bytes_tok) else {
            send_error_reply(anchor, req_id, src_addr, src_port);
            return;
        };
        let data_start = line.line_end;
        if data_start + value_len as usize + 2 > request.len() {
            send_error_reply(anchor, req_id, src_addr, src_port);
            return;
        }
        let value = &request[data_start..data_start + value_len as usize];
        // KV_OP_PUT body: [key_len:u16][key…][value_len:u32][value…]
        //                  [put_flags:u8][expiry_ms:u64]
        let need = 2 + key.len() + 4 + value.len() + 1 + 8;
        if need > anchor.send.len() {
            send_error_reply(anchor, req_id, src_addr, src_port);
            return;
        }
        let mut buf = [0u8; 1600];
        let mut p = 0;
        buf[p] = (key.len() & 0xFF) as u8;
        buf[p + 1] = ((key.len() >> 8) & 0xFF) as u8;
        p += 2;
        buf[p..p + key.len()].copy_from_slice(key);
        p += key.len();
        let vlen = (value.len() as u32).to_le_bytes();
        buf[p..p + 4].copy_from_slice(&vlen);
        p += 4;
        buf[p..p + value.len()].copy_from_slice(value);
        p += value.len();
        buf[p] = 0;
        p += 1;
        buf[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
        p += 8;
        send_envelope(
            anchor,
            KV_OP_PUT,
            MemOp::Set,
            key,
            req_id,
            src_addr,
            src_port,
            &buf[..p],
        );
    } else if eq_ascii_ci(cmd, b"delete") {
        if line.count < 2 {
            send_error_reply(anchor, req_id, src_addr, src_port);
            return;
        }
        let key = token_bytes(request, line.tokens[1]);
        // KV_OP_DELETE body: [n_keys:u16 = 1][key_len:u16][key…]
        let mut buf = [0u8; 260];
        let mut p = 0;
        buf[p] = 1;
        buf[p + 1] = 0;
        p += 2;
        buf[p] = (key.len() & 0xFF) as u8;
        buf[p + 1] = ((key.len() >> 8) & 0xFF) as u8;
        p += 2;
        buf[p..p + key.len()].copy_from_slice(key);
        p += key.len();
        send_envelope(
            anchor,
            KV_OP_DELETE,
            MemOp::Delete,
            key,
            req_id,
            src_addr,
            src_port,
            &buf[..p],
        );
    } else {
        send_error_reply(anchor, req_id, src_addr, src_port);
    }
}

// ── MSG_KV_REQUEST emitter ───────────────────────────────────────────

unsafe fn send_envelope(
    anchor: &mut AnchorState,
    op: u8,
    mem_op: MemOp,
    key_for_inflight: &[u8],
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
    body: &[u8],
) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.kv_out < 0 {
        return false;
    }
    let sys = &*sys;
    let corr = anchor.next_corr();

    // Envelope head: [corr:8][proto:1][tenant:4][conn:1][cons:1][op:1][body_len:2] = 18
    const HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    let payload_len = HEAD + body.len();
    if payload_len > u16::MAX as usize {
        return false;
    }
    let total = 3 + payload_len;
    if total > anchor.scratch.len() {
        return false;
    }
    let out = &mut anchor.scratch[..total];
    out[0] = MSG_KV_REQUEST;
    out[1] = (payload_len & 0xFF) as u8;
    out[2] = ((payload_len >> 8) & 0xFF) as u8;
    let mut p = 3;
    out[p..p + 8].copy_from_slice(&corr.to_le_bytes());
    p += 8;
    out[p] = PROTO_MEMCACHED_UDP;
    p += 1;
    // Tenant 0 — UDP memcached has no auth path; multi-tenancy lands
    // when the TCP anchor's tenant resolution is shared.
    out[p..p + 4].copy_from_slice(&0u32.to_le_bytes());
    p += 4;
    // conn_id is unused for datagram — we look up the response peer
    // from inflight metadata, not from a slot table. Stamp 0.
    out[p] = 0;
    p += 1;
    out[p] = REQ_LINEARIZABLE;
    p += 1;
    out[p] = op;
    p += 1;
    out[p] = (body.len() & 0xFF) as u8;
    out[p + 1] = ((body.len() >> 8) & 0xFF) as u8;
    p += 2;
    out[p..p + body.len()].copy_from_slice(body);

    let mut inflight = InflightVal::new();
    inflight.op = mem_op as u8;
    inflight.req_id = req_id;
    inflight.src_addr = src_addr;
    inflight.src_port = src_port;
    let kn = key_for_inflight.len().min(inflight.key.len());
    inflight.key_len = kn as u16;
    inflight.key[..kn].copy_from_slice(&key_for_inflight[..kn]);
    if anchor.inflight.insert(corr, inflight).is_err() {
        return false;
    }
    if (sys.channel_write)(anchor.kv_out, anchor.scratch.as_mut_ptr(), total) != total as i32 {
        let _ = anchor.inflight.remove(corr);
        return false;
    }
    anchor.m_forwarded += 1;
    true
}

// ── Response path: routed_in → datagram out ─────────────────────────

unsafe fn drain_routed_in(anchor: &mut AnchorState) -> bool {
    if anchor.routed_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let sys = &*sys;
    let poll = (sys.channel_poll)(anchor.routed_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    let n = (sys.channel_read)(anchor.routed_in, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    if hdr[0] != MSG_KV_RESPONSE {
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    // MSG_KV_RESPONSE head: [corr:8][conn:1][result:1][rev:8][body_len:2] = 20
    const HEAD: usize = 8 + 1 + 1 + 8 + 2;
    if payload_len < HEAD || payload_len > anchor.scratch.len() {
        return false;
    }
    let mut buf = [0u8; SCRATCH];
    let n2 = (sys.channel_read)(anchor.routed_in, buf.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }
    let resp = &buf[..payload_len];
    let corr = u64::from_le_bytes([
        resp[0], resp[1], resp[2], resp[3], resp[4], resp[5], resp[6], resp[7],
    ]);
    let _conn_id = resp[8];
    let result = resp[9];
    let revision = u64::from_le_bytes([
        resp[10], resp[11], resp[12], resp[13], resp[14], resp[15], resp[16], resp[17],
    ]);
    let body_len = u16::from_le_bytes([resp[18], resp[19]]) as usize;
    if 20 + body_len > payload_len {
        return false;
    }
    let body = &resp[20..20 + body_len];

    let Some(meta) = anchor.inflight.remove(corr) else {
        return true; // unmatched (timed out / dropped earlier)
    };
    let Some(mem_op) = MemOp::from_u8(meta.op) else {
        return true;
    };

    // Encode the memcached ASCII reply.
    let mut reply = [0u8; 1500];
    let mut reply_len = 0usize;
    encode_reply(
        &mut reply,
        &mut reply_len,
        mem_op,
        &meta.key[..meta.key_len as usize],
        result,
        revision,
        body,
    );
    let _ = result;

    send_reply_datagram(
        anchor,
        meta.req_id,
        meta.src_addr,
        meta.src_port,
        &reply[..reply_len],
    );
    true
}

unsafe fn send_error_reply(
    anchor: &mut AnchorState,
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
) {
    anchor.m_errors += 1;
    send_reply_datagram(anchor, req_id, src_addr, src_port, b"ERROR\r\n");
}

unsafe fn send_reply_datagram(
    anchor: &mut AnchorState,
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
    payload: &[u8],
) {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.dgram_out < 0 || anchor.bind_phase != BindPhase::Bound {
        return;
    }
    let sys = &*sys;
    // CMD_DG_SEND_TO IPv4 body:
    //   [ep_id:1][af:1=4][addr:4 BE][port:2 LE][memcached_udp_frame…]
    // memcached_udp_frame: [req_id:u16 BE][seq:u16 BE=0][total:u16 BE=1][reserved:u16=0][ascii…]
    let total = DG_V4_PREFIX + MEMCACHED_UDP_HEADER + payload.len();
    if total > anchor.send.len() {
        return;
    }
    let out = &mut anchor.send[..total];
    out[0] = anchor.ep_id;
    out[1] = DG_AF_INET;
    out[2..6].copy_from_slice(&src_addr);
    out[6..8].copy_from_slice(&src_port.to_le_bytes());
    // memcached UDP frame header — big-endian per memcached protocol.
    out[8..10].copy_from_slice(&req_id.to_be_bytes());
    out[10..12].copy_from_slice(&0u16.to_be_bytes()); // seq
    out[12..14].copy_from_slice(&1u16.to_be_bytes()); // total
    out[14..16].copy_from_slice(&0u16.to_be_bytes()); // reserved
    out[16..16 + payload.len()].copy_from_slice(payload);

    let mut scratch = [0u8; SCRATCH];
    let _ = channel_write_frame(sys, anchor.dgram_out, DG_CMD_SEND_TO, out, &mut scratch);
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
    _params: *const u8,
    _params_len: usize,
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
    anchor.dgram_in = in_chan;
    anchor.dgram_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        // inputs:  dgram_in[0], routed_in[1]
        // outputs: dgram_out[0], kv_out[1], metrics[2]
        anchor.routed_in = dev_channel_port(sys, 0, 1);
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

    unsafe {
        try_bind(anchor);

        // Bounded per-tick drain — same shape as memcached_stream_anchor.
        const BUDGET: u32 = 32;
        let mut budget = BUDGET;
        while budget > 0 {
            let a = drain_dgram_in(anchor);
            let b = drain_routed_in(anchor);
            if !a && !b {
                break;
            }
            budget -= 1;
        }

        // Phase-14: emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
        if anchor.step_ctr.is_multiple_of(5000) && !anchor.syscalls.is_null() {
            telemetry::emit_counters(
                &*anchor.syscalls,
                anchor.metrics_out,
                &[anchor.m_requests, anchor.m_forwarded, anchor.m_errors],
            );
        }
    }
    0
}
