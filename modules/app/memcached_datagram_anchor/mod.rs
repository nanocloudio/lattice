//! memcached_datagram_anchor — Memcached UDP anchor.
//!
//! Continuity class: `reroutable`. Each UDP request datagram carries
//! a 2-byte request id; the server echoes the same id on every reply
//! datagram. There's no per-client connection state, so reroute /
//! replica migration is transparent to the wire.
//!
//! ## Memcached UDP framing
//!
//! Per memcached's UDP protocol, each datagram (request *and* response)
//! carries an 8-byte frame header followed by the same ASCII protocol
//! the TCP path uses (`memcached_codec`'s UDP framing):
//!
//!   `[req_id:u16 BE][seq:u16 BE][total:u16 BE][reserved:u16 = 0]`
//!
//! `req_id` is opaque to the server (echoed verbatim). A request is one
//! datagram (`seq = 0`, `total = 1`); anything else is answered
//! `ERROR` (as memcached: a request larger than a datagram is TCP's).
//!
//! A reply is cut into as many datagrams as it needs, each at most
//! `UDP_DATAGRAM_MAX` (1400) bytes with its header, numbered
//! `seq = 0..total` under the request's id, so a `get` answers any value
//! up to the point protocols' bound (`kv_large::VALUE_MAX`) — a value
//! stored as chunks under a manifest is fetched whole through a
//! `kv_large` session first. The client reassembles by sequence number,
//! in any arrival order. Loss is memcached's: nothing is retransmitted;
//! a client missing a datagram times out and sends the request again.
//!
//! A reply whose datagrams the provider channel cannot take at once is
//! parked in one of `TX_SLOTS` reply slots and sent on, in order, over
//! the next steps; with every slot parked, the rest of a reply is
//! dropped (counted `reply_dropped`) and the client's retry is the
//! recovery. The state a reply holds is bounded by those slots.
//!
//! ## Supported commands
//!
//! `get k\r\n` → `VALUE k flags bytes\r\n<data>\r\nEND\r\n` / `END\r\n`
//! `set k flags exp bytes\r\n<data>\r\n` → `STORED\r\n` / `NOT_STORED\r\n`
//! `delete k\r\n` → `DELETED\r\n` / `NOT_FOUND\r\n`
//! `version\r\n` → `VERSION 0.2.0\r\n`
//!
//! A `set` carries its value in its one request datagram, inline: a
//! value past one data chunk (`kv_chunks::DATA_MAX`) is answered
//! `SERVER_ERROR object too large for cache` (write it over TCP; every
//! value the store holds reads back here). All other commands are
//! answered with `ERROR\r\n` (memcached's generic unknown-command
//! reply). Multi-key `get`, `incr` / `decr`, `add` / `replace` /
//! `append` / `prepend`, and `cas` are TCP-only.

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
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/kv_large.rs"]
mod kv_large;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[path = "../../common/collections.rs"]
mod collections;

#[path = "../../common/memcached_codec.rs"]
mod memcached_codec;

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox, ENVELOPE_HDR};

use collections::InflightTable;
use memcached_codec::{
    append, encode_get_answer, encode_reply, eq_ascii_ci, parse_line, parse_u32_dec, token_bytes,
    udp_datagrams, udp_frame, udp_header, Line, MemOp, GET_ANSWER_OVERHEAD, UDP_DATAGRAM_MAX,
    UDP_FRAME_HEADER,
};
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_PUT, KV_RESULT_INTERNAL, KV_RESULT_MANIFEST, KV_RESULT_OK,
    KV_RESULT_TOO_LARGE, PROTO_MEMCACHED_UDP, REQ_LINEARIZABLE, REQ_SERIALIZABLE,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// Datagram opcodes + POLL_IN/POLL_OUT come from the SDK runtime
// (path-included above); the rtp module uses the same constants
// without redeclaring them.

// ── Capacities ────────────────────────────────────────────────────────

/// One request datagram as the provider frames it (`[ep][af][addr][port]`
/// then the memcached frame): a request is one datagram, so a `set`'s
/// value is bounded by it.
const RECV_BUF: usize = 2048;
/// A `KV_OP_PUT` body built from one request datagram.
const PUT_BODY_MAX: usize = RECV_BUF + 16;
/// A `MSG_KV_REQUEST` envelope: frame header, request head, body.
const REQ_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
const ENV_BUF: usize = 3 + REQ_HEAD + PUT_BODY_MAX;
/// Every envelope the router writes is read whole.
const ROUTED_BUF: usize = ENVELOPE_HDR + wire::FRAME_PAYLOAD_MAX;
const MAX_INFLIGHT: usize = 512;
/// The longest key a request or a session keeps.
const KEY_MAX: usize = 256;

/// A large GET's value, fetched whole: the point protocols' bound and
/// the TCP anchor's slack (`memcached_stream_anchor::LG_OUT`).
const LG_OUT: usize = kv_large::VALUE_MAX + 256;
/// Large-value reads at once (a GET answered by a manifest, expanded).
/// A GET that finds every one busy is answered `SERVER_ERROR`.
const LG_SESSIONS: usize = 4;
/// A session's next exchange: a chunk's hash.
const LG_SEND: usize = 64;
/// The conn byte of chunk exchanges (requests stamp 0).
const LG_CHUNK_CONN: u8 = u8::MAX;
/// A session's exchange waiting for room on `kv_out`.
const LG_UNSENT: u64 = 1;
/// A session that has not finished in this long is abandoned unanswered:
/// its client has long since timed out and asked again.
const LG_STALL_MS: u64 = 5_000;

/// One whole reply: a `VALUE` block at `LG_OUT` with its key and `END`.
const REPLY_MAX: usize = LG_OUT + KEY_MAX + GET_ANSWER_OVERHEAD;
/// Replies parked for room on `dgram_out`.
const TX_SLOTS: usize = 4;
/// One `CMD_DG_SEND_TO` frame: header, IPv4 prefix, one datagram.
const DG_FRAME: usize = 3 + DG_V4_PREFIX + UDP_DATAGRAM_MAX;

const _: () = assert!(REPLY_MAX.div_ceil(UDP_DATAGRAM_MAX - UDP_FRAME_HEADER) <= u16::MAX as usize);
const _: () = assert!(REQ_HEAD + PUT_BODY_MAX <= wire::KV_REQUEST_PAYLOAD_MAX);

const DEFAULT_PORT: u16 = 11211;

define_params! {
    AnchorState;

    // UDP port the anchor binds for memcached clients (memcached's own
    // 11211 by default; a harness or a second graph moves it).
    1, listen_port, u16, DEFAULT_PORT
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, DEFAULT_PORT); };
}

// ── Inflight table value ─────────────────────────────────────────────

#[derive(Clone, Copy)]
#[repr(C)]
struct InflightVal {
    op: u8,
    /// Echo of the inbound `req_id` so the response datagrams carry
    /// the matching memcached UDP frame id.
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
    key_len: u16,
    key: [u8; KEY_MAX],
}

impl InflightVal {
    const fn new() -> Self {
        Self {
            op: 0,
            req_id: 0,
            src_addr: [0; 4],
            src_port: 0,
            key_len: 0,
            key: [0; KEY_MAX],
        }
    }
}

/// A reply waiting for room on `dgram_out`: its datagrams from `next`.
struct TxSlot {
    live: bool,
    req_id: u16,
    addr: [u8; 4],
    port: u16,
    next: u16,
    total: u16,
    len: usize,
    buf: [u8; REPLY_MAX],
}

/// One large GET: the manifest's value fetched chunk by chunk, then
/// answered to whom it names.
struct LgSlot {
    session: kv_large::Session,
    /// The exchange in flight; [`LG_UNSENT`] waiting for room; 0 = free.
    corr: u64,
    unsent_op: u8,
    unsent_len: usize,
    req_id: u16,
    addr: [u8; 4],
    port: u16,
    key_len: u16,
    key: [u8; KEY_MAX],
    since_ms: u64,
    send: [u8; LG_SEND],
    out: [u8; LG_OUT],
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
    /// Each input's envelope reading (`common/envelope.rs`).
    dgram_inbox: Inbox,
    routed_inbox: Inbox,

    bind_phase: BindPhase,
    ep_id: u8,
    listen_port: u16,
    local_port: u16,

    corr_seq: u64,

    // Telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=requests, 1=forwarded, 2=errors,
    // 3=reply_datagrams, 4=reply_dropped).
    m_requests: u64,
    m_forwarded: u64,
    m_errors: u64,
    m_datagrams: u64,
    m_dropped: u64,
    step_ctr: u64,

    inflight: InflightTable<InflightVal, MAX_INFLIGHT>,

    tx: [TxSlot; TX_SLOTS],
    lg: [LgSlot; LG_SESSIONS],

    /// The reply being rendered.
    reply: [u8; REPLY_MAX],
    /// The routed envelope being read.
    routed: [u8; ROUTED_BUF],
    /// The KV envelope being written.
    env: [u8; ENV_BUF],
    /// The datagram frame being written.
    dg: [u8; DG_FRAME],
}

impl AnchorState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.dgram_in = -1;
        self.dgram_out = -1;
        self.routed_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.dgram_inbox = Inbox::new();
        self.routed_inbox = Inbox::new();
        self.bind_phase = BindPhase::Init;
        self.ep_id = 0xFF;
        self.listen_port = DEFAULT_PORT;
        self.local_port = 0;
        self.corr_seq = 0;
        self.m_requests = 0;
        self.m_forwarded = 0;
        self.m_errors = 0;
        self.m_datagrams = 0;
        self.m_dropped = 0;
        self.step_ctr = 0;
        self.inflight = InflightTable::new();
        for t in self.tx.iter_mut() {
            t.live = false;
        }
        for l in self.lg.iter_mut() {
            l.session = kv_large::Session::new();
            l.corr = 0;
            l.unsent_op = 0;
        }
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

    fn now_ms(&self) -> u64 {
        if self.syscalls.is_null() {
            0
        } else {
            unsafe { dev_millis(&*self.syscalls) }
        }
    }
}

// ── Channel helpers ──────────────────────────────────────────────────

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
    body[0..2].copy_from_slice(&anchor.listen_port.to_le_bytes());
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
    let mut local = [0u8; ENVELOPE_HDR + RECV_BUF];
    let (msg_type, plen) =
        match read_envelope(&mut anchor.dgram_inbox, sys, anchor.dgram_in, &mut local) {
            Some(p) => p,
            None => return false,
        };
    match msg_type {
        DG_MSG_BOUND => {
            // Payload: [ep_id:1][local_port:u16 LE]. A provider output
            // fanned to several consumers shows each of them every
            // BOUND: claim only the one for the port asked for.
            if plen >= 3 && anchor.bind_phase == BindPhase::Pending {
                let (ep, port) = (local[0], u16::from_le_bytes([local[1], local[2]]));
                if anchor.listen_port == 0 || port == anchor.listen_port {
                    anchor.ep_id = ep;
                    anchor.local_port = port;
                    anchor.bind_phase = BindPhase::Bound;
                }
            }
        }
        DG_MSG_RX_FROM => {
            handle_rx_from(anchor, &local[..plen]);
        }
        DG_MSG_CLOSED | DG_MSG_ERROR => {
            // [ep_id:1]…: only this anchor's endpoint (or, while its bind
            // is pending, a refused bind's 0xFF) ends it — another
            // consumer's on a shared provider does not.
            if plen >= 1 {
                let ep = local[0];
                let ours = (anchor.bind_phase == BindPhase::Bound && ep == anchor.ep_id)
                    || (anchor.bind_phase == BindPhase::Pending && ep == 0xFF);
                if ours {
                    anchor.bind_phase = BindPhase::Error;
                }
            }
        }
        _ => {}
    }
    true
}

unsafe fn handle_rx_from(anchor: &mut AnchorState, payload: &[u8]) {
    // [ep_id:1][af:1][src_addr:4 BE][src_port:2 LE][memcached_udp_frame…]
    if payload.len() < DG_V4_PREFIX + UDP_FRAME_HEADER + 2 {
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
    let Some((req_id, seq, total)) = udp_header(dgram) else {
        return;
    };
    if seq != 0 || total != 1 {
        // A request is one datagram.
        send_error_reply(anchor, req_id, src_addr, src_port);
        return;
    }
    let ascii = &dgram[UDP_FRAME_HEADER..];
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
        if line.count != 2 {
            send_error_reply(anchor, req_id, src_addr, src_port);
            return;
        }
        let key = token_bytes(request, line.tokens[1]);
        if key.len() > KEY_MAX {
            send_text(
                anchor,
                req_id,
                src_addr,
                src_port,
                b"CLIENT_ERROR bad command line format\r\n",
            );
            return;
        }
        let mut body = [0u8; 2 + KEY_MAX];
        body[0..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        body[2..2 + key.len()].copy_from_slice(key);
        send_envelope(
            anchor,
            KV_OP_GET,
            MemOp::Get,
            key,
            req_id,
            src_addr,
            src_port,
            2 + key.len(),
            &body,
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
        let value_len = value_len as usize;
        let data_start = line.line_end;
        if key.len() > KEY_MAX || data_start + value_len + 2 > request.len() {
            send_text(
                anchor,
                req_id,
                src_addr,
                src_port,
                b"CLIENT_ERROR bad data chunk\r\n",
            );
            return;
        }
        // The value rides inline in this one datagram's PUT: one past a
        // data chunk would need the chunk upload a request datagram has
        // no room to drive.
        if value_len > kv_large::kv_chunks::DATA_MAX {
            send_text(
                anchor,
                req_id,
                src_addr,
                src_port,
                b"SERVER_ERROR object too large for cache\r\n",
            );
            return;
        }
        let value = &request[data_start..data_start + value_len];
        // KV_OP_PUT body: [key_len:u16][key…][value_len:u32][value…]
        //                  [put_flags:u8][expiry_ms:u64]
        let need = 2 + key.len() + 4 + value.len() + 1 + 8;
        let mut buf = [0u8; PUT_BODY_MAX];
        if need > buf.len() {
            send_text(
                anchor,
                req_id,
                src_addr,
                src_port,
                b"CLIENT_ERROR bad data chunk\r\n",
            );
            return;
        }
        let mut p = 0;
        buf[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        p += 2;
        buf[p..p + key.len()].copy_from_slice(key);
        p += key.len();
        buf[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
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
            p,
            &buf,
        );
    } else if eq_ascii_ci(cmd, b"delete") {
        if line.count < 2 {
            send_error_reply(anchor, req_id, src_addr, src_port);
            return;
        }
        let key = token_bytes(request, line.tokens[1]);
        if key.len() > KEY_MAX {
            send_text(
                anchor,
                req_id,
                src_addr,
                src_port,
                b"CLIENT_ERROR bad command line format\r\n",
            );
            return;
        }
        // KV_OP_DELETE body: [n_keys:u16 = 1][key_len:u16][key…]
        let mut buf = [0u8; 4 + KEY_MAX];
        buf[0..2].copy_from_slice(&1u16.to_le_bytes());
        buf[2..4].copy_from_slice(&(key.len() as u16).to_le_bytes());
        buf[4..4 + key.len()].copy_from_slice(key);
        send_envelope(
            anchor,
            KV_OP_DELETE,
            MemOp::Delete,
            key,
            req_id,
            src_addr,
            src_port,
            4 + key.len(),
            &buf,
        );
    } else if eq_ascii_ci(cmd, b"version") {
        send_text(anchor, req_id, src_addr, src_port, b"VERSION 0.2.0\r\n");
    } else {
        send_error_reply(anchor, req_id, src_addr, src_port);
    }
}

// ── MSG_KV_REQUEST emitter ───────────────────────────────────────────

/// Write `MSG_KV_REQUEST` (`op`, `body[..len]`) under `corr` into
/// `env`: the envelope's length.
fn build_request(
    env: &mut [u8],
    corr: u64,
    conn: u8,
    consistency: u8,
    op: u8,
    body: &[u8],
) -> Option<usize> {
    let payload_len = REQ_HEAD + body.len();
    if payload_len > wire::KV_REQUEST_PAYLOAD_MAX {
        return None;
    }
    let total = 3 + payload_len;
    let out = env.get_mut(..total)?;
    out[0] = MSG_KV_REQUEST;
    out[1..3].copy_from_slice(&(payload_len as u16).to_le_bytes());
    out[3..11].copy_from_slice(&corr.to_le_bytes());
    out[11] = PROTO_MEMCACHED_UDP;
    // Tenant 0 — UDP memcached has no auth path.
    out[12..16].copy_from_slice(&0u32.to_le_bytes());
    // conn byte: no slot table here (the reply's peer is in the inflight
    // entry); chunk fetches stamp their own.
    out[16] = conn;
    out[17] = consistency;
    out[18] = op;
    out[19..21].copy_from_slice(&(body.len() as u16).to_le_bytes());
    out[21..total].copy_from_slice(body);
    Some(total)
}

unsafe fn send_envelope(
    anchor: &mut AnchorState,
    op: u8,
    mem_op: MemOp,
    key_for_inflight: &[u8],
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
    body_len: usize,
    body: &[u8],
) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.kv_out < 0 {
        return false;
    }
    let sys = &*sys;
    let corr = anchor.next_corr();
    let Some(total) = build_request(
        &mut anchor.env,
        corr,
        0,
        REQ_LINEARIZABLE,
        op,
        &body[..body_len],
    ) else {
        return false;
    };

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
    if (sys.channel_write)(anchor.kv_out, anchor.env.as_mut_ptr(), total) != total as i32 {
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
    let Some((msg_type, payload_len)) = read_envelope(
        &mut anchor.routed_inbox,
        sys,
        anchor.routed_in,
        &mut anchor.routed,
    ) else {
        return false;
    };
    if msg_type != MSG_KV_RESPONSE {
        return true;
    }
    // MSG_KV_RESPONSE head: [corr:8][conn:1][result:1][rev:8][body_len:2] = 20
    const HEAD: usize = 8 + 1 + 1 + 8 + 2;
    if payload_len < HEAD {
        return true;
    }
    let resp = &anchor.routed[..payload_len];
    let corr = u64::from_le_bytes([
        resp[0], resp[1], resp[2], resp[3], resp[4], resp[5], resp[6], resp[7],
    ]);
    let result = resp[9];
    let revision = u64::from_le_bytes([
        resp[10], resp[11], resp[12], resp[13], resp[14], resp[15], resp[16], resp[17],
    ]);
    let body_len = u16::from_le_bytes([resp[18], resp[19]]) as usize;
    if HEAD + body_len > payload_len {
        return true;
    }
    // The body, out of `routed` (the handlers below write the state).
    let mut body = [0u8; wire::KV_RESPONSE_BODY_MAX];
    let blen = body_len.min(body.len());
    body[..blen].copy_from_slice(&anchor.routed[HEAD..HEAD + blen]);
    let body = &body[..blen];

    // An exchange of a large-value session.
    if let Some(i) = anchor
        .lg
        .iter()
        .position(|l| l.corr == corr && l.corr > LG_UNSENT)
    {
        lg_reply(anchor, i, result, body);
        return true;
    }

    let Some(meta) = anchor.inflight.remove(corr) else {
        return true; // unmatched (timed out / dropped earlier)
    };
    let Some(mem_op) = MemOp::from_u8(meta.op) else {
        return true;
    };
    let key = &meta.key[..meta.key_len as usize];

    if mem_op == MemOp::Get && result == KV_RESULT_MANIFEST && !body.is_empty() {
        lg_expand(anchor, &meta, result, body);
        return true;
    }

    let mut n = 0usize;
    if mem_op == MemOp::Get {
        encode_get_answer(
            &mut anchor.reply,
            &mut n,
            false,
            key,
            result,
            revision,
            body,
        );
    } else {
        encode_reply(
            &mut anchor.reply,
            &mut n,
            mem_op,
            key,
            result,
            revision,
            body,
        );
    }
    send_reply(anchor, meta.req_id, meta.src_addr, meta.src_port, n);
    true
}

unsafe fn send_error_reply(
    anchor: &mut AnchorState,
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
) {
    anchor.m_errors += 1;
    send_text(anchor, req_id, src_addr, src_port, b"ERROR\r\n");
}

/// A reply the anchor writes itself.
unsafe fn send_text(
    anchor: &mut AnchorState,
    req_id: u16,
    src_addr: [u8; 4],
    src_port: u16,
    text: &[u8],
) {
    let n = text.len().min(REPLY_MAX);
    anchor.reply[..n].copy_from_slice(&text[..n]);
    send_reply(anchor, req_id, src_addr, src_port, n);
}

// ── Reply datagrams ───────────────────────────────────────────────────

/// Datagram `seq` of `reply` to `(addr, port)` on `dgram_out`: `false`
/// when the channel had no room for it.
unsafe fn emit_datagram(
    sys: &SyscallTable,
    chan: i32,
    ep_id: u8,
    addr: [u8; 4],
    port: u16,
    req_id: u16,
    seq: u16,
    reply: &[u8],
    dg: &mut [u8; DG_FRAME],
) -> bool {
    // CMD_DG_SEND_TO IPv4 body:
    //   [ep_id:1][af:1=4][addr:4 BE][port:2 LE][memcached_udp_frame…]
    const AT: usize = 3 + DG_V4_PREFIX;
    let Some(n) = udp_frame(req_id, seq, reply, &mut dg[AT..]) else {
        return true; // nothing to send: never a stall
    };
    let body = DG_V4_PREFIX + n;
    dg[0] = DG_CMD_SEND_TO;
    dg[1..3].copy_from_slice(&(body as u16).to_le_bytes());
    dg[3] = ep_id;
    dg[4] = DG_AF_INET;
    dg[5..9].copy_from_slice(&addr);
    dg[9..11].copy_from_slice(&port.to_le_bytes());
    let total = 3 + body;
    (sys.channel_write)(chan, dg.as_mut_ptr(), total) == total as i32
}

/// Send `reply[..len]` as its datagrams: at once while the channel takes
/// them, the rest parked in a reply slot. Behind a parked reply a new one
/// is parked whole (no reply overtakes another's room); with no slot free
/// what is left is dropped — the client's retry recovers it.
unsafe fn send_reply(anchor: &mut AnchorState, req_id: u16, addr: [u8; 4], port: u16, len: usize) {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.dgram_out < 0 || anchor.bind_phase != BindPhase::Bound {
        return;
    }
    let sys = &*sys;
    let Some(total) = udp_datagrams(len) else {
        return;
    };
    let mut seq = 0u16;
    if !anchor.tx.iter().any(|t| t.live) {
        let AnchorState {
            reply,
            dg,
            dgram_out,
            ep_id,
            ..
        } = &mut *anchor;
        while seq < total {
            if !emit_datagram(
                sys,
                *dgram_out,
                *ep_id,
                addr,
                port,
                req_id,
                seq,
                &reply[..len],
                dg,
            ) {
                break;
            }
            seq += 1;
        }
        anchor.m_datagrams += u64::from(seq);
        if seq == total {
            return;
        }
    }
    let Some(t) = anchor.tx.iter().position(|t| !t.live) else {
        anchor.m_dropped += u64::from(total - seq);
        return;
    };
    let AnchorState { reply, tx, .. } = &mut *anchor;
    let slot = &mut tx[t];
    slot.live = true;
    slot.req_id = req_id;
    slot.addr = addr;
    slot.port = port;
    slot.next = seq;
    slot.total = total;
    slot.len = len;
    slot.buf[..len].copy_from_slice(&reply[..len]);
}

/// Send on the parked replies, oldest slot first, while the channel
/// takes their datagrams.
unsafe fn flush_tx(anchor: &mut AnchorState) {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.dgram_out < 0 || anchor.bind_phase != BindPhase::Bound {
        return;
    }
    let sys = &*sys;
    let AnchorState {
        tx,
        dg,
        dgram_out,
        ep_id,
        m_datagrams,
        ..
    } = &mut *anchor;
    for t in tx.iter_mut() {
        if !t.live {
            continue;
        }
        while t.next < t.total {
            if !emit_datagram(
                sys,
                *dgram_out,
                *ep_id,
                t.addr,
                t.port,
                t.req_id,
                t.next,
                &t.buf[..t.len],
                dg,
            ) {
                return;
            }
            t.next += 1;
            *m_datagrams += 1;
        }
        t.live = false;
    }
}

// ── Large GETs ────────────────────────────────────────────────────────

/// Expand a GET's manifest answer through a free session; with none free
/// the GET is answered `SERVER_ERROR` (the client may ask again).
unsafe fn lg_expand(anchor: &mut AnchorState, meta: &InflightVal, result: u8, body: &[u8]) {
    let Some(i) = anchor.lg.iter().position(|l| l.corr == 0) else {
        anchor.m_errors += 1;
        send_text(
            anchor,
            meta.req_id,
            meta.src_addr,
            meta.src_port,
            b"SERVER_ERROR too many large reads in flight\r\n",
        );
        return;
    };
    let now = anchor.now_ms();
    let act = {
        let l = &mut anchor.lg[i];
        l.corr = LG_UNSENT;
        l.unsent_op = 0;
        l.unsent_len = 0;
        l.req_id = meta.req_id;
        l.addr = meta.src_addr;
        l.port = meta.src_port;
        l.key_len = meta.key_len;
        l.key = meta.key;
        l.since_ms = now;
        let LgSlot {
            session, send, out, ..
        } = l;
        session.expand(KV_OP_GET, &[], result, body, send, out)
    };
    lg_act(anchor, i, act);
}

/// Carry out what session `i` asks: its next exchange, or its answer.
unsafe fn lg_act(
    anchor: &mut AnchorState,
    i: usize,
    act: Result<kv_large::Act, kv_large::LargeError>,
) {
    match act {
        Ok(kv_large::Act::Send(op, n)) => lg_send(anchor, i, op, n),
        Ok(kv_large::Act::Done(result, n)) => lg_finish(anchor, i, result, n),
        Ok(kv_large::Act::AsIs) => lg_finish(anchor, i, KV_RESULT_INTERNAL, 0),
        Err(kv_large::LargeError::Overflow) => lg_finish(anchor, i, KV_RESULT_TOO_LARGE, 0),
        Err(_) => lg_finish(anchor, i, KV_RESULT_INTERNAL, 0),
    }
}

/// Send session `i`'s exchange (`op`, its `send[..n]`); with no room on
/// `kv_out` it waits for [`lg_step`].
unsafe fn lg_send(anchor: &mut AnchorState, i: usize, op: u8, n: usize) {
    if n > LG_SEND {
        lg_finish(anchor, i, KV_RESULT_INTERNAL, 0);
        return;
    }
    let corr = anchor.next_corr();
    let built = {
        let AnchorState { lg, env, .. } = &mut *anchor;
        build_request(
            env,
            corr,
            LG_CHUNK_CONN,
            REQ_SERIALIZABLE,
            op,
            &lg[i].send[..n],
        )
    };
    let Some(total) = built else {
        lg_finish(anchor, i, KV_RESULT_INTERNAL, 0);
        return;
    };
    let (sys, kv_out) = (anchor.syscalls, anchor.kv_out);
    let wrote = !sys.is_null()
        && kv_out >= 0
        && ((*sys).channel_write)(kv_out, anchor.env.as_mut_ptr(), total) == total as i32;
    let l = &mut anchor.lg[i];
    if wrote {
        l.corr = corr;
        l.unsent_op = 0;
    } else {
        l.corr = LG_UNSENT;
        l.unsent_op = op;
        l.unsent_len = n;
    }
}

/// A reply to session `i`'s exchange.
unsafe fn lg_reply(anchor: &mut AnchorState, i: usize, result: u8, body: &[u8]) {
    let act = {
        let LgSlot {
            session, send, out, ..
        } = &mut anchor.lg[i];
        session.reply(&[], &[], result, body, send, out)
    };
    lg_act(anchor, i, act);
}

/// Answer session `i`'s GET (`result`, its value in `out[..n]`) and free
/// the session.
unsafe fn lg_finish(anchor: &mut AnchorState, i: usize, result: u8, n: usize) {
    let mut len = 0usize;
    let (req_id, addr, port) = {
        let AnchorState { lg, reply, .. } = &mut *anchor;
        let l = &mut lg[i];
        let key = &l.key[..(l.key_len as usize).min(KEY_MAX)];
        let value = &l.out[..n.min(LG_OUT)];
        let result = if result == KV_RESULT_MANIFEST {
            KV_RESULT_INTERNAL
        } else {
            result
        };
        encode_get_answer(reply, &mut len, false, key, result, 0, value);
        l.corr = 0;
        l.unsent_op = 0;
        l.session.reset();
        (l.req_id, l.addr, l.port)
    };
    send_reply(anchor, req_id, addr, port, len);
}

/// Each step: resend exchanges that found no room, and abandon sessions
/// that never came back (unanswered: the client has asked again).
unsafe fn lg_step(anchor: &mut AnchorState) {
    if !anchor.lg.iter().any(|l| l.corr != 0) {
        return;
    }
    let now = anchor.now_ms();
    for i in 0..LG_SESSIONS {
        let l = &anchor.lg[i];
        if l.corr == 0 {
            continue;
        }
        if now.saturating_sub(l.since_ms) > LG_STALL_MS {
            let l = &mut anchor.lg[i];
            l.corr = 0;
            l.unsent_op = 0;
            l.session.reset();
            anchor.m_errors += 1;
        } else if l.corr == LG_UNSENT && l.unsent_op != 0 {
            let (op, n) = (l.unsent_op, l.unsent_len);
            lg_send(anchor, i, op, n);
        }
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
        flush_tx(anchor);
        lg_step(anchor);

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

        // Emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
        if anchor.step_ctr.is_multiple_of(5000) && !anchor.syscalls.is_null() {
            telemetry::emit_counters(
                &*anchor.syscalls,
                anchor.metrics_out,
                &[
                    anchor.m_requests,
                    anchor.m_forwarded,
                    anchor.m_errors,
                    anchor.m_datagrams,
                    anchor.m_dropped,
                ],
            );
        }
    }
    0
}
