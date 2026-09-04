//! Span courier — cross-node transport for the elastic split's chunk
//! stream.
//!
//! The elastic split ships a key span between workers as a stream of
//! app-snapshot envelopes (`RESET`, `CHUNK`) answered by an
//! install-complete ack. Inside one process those ride ordinary channel
//! edges; between nodes this courier carries the SAME envelopes over one
//! TCP connection, byte-for-byte — it never parses them, so every
//! envelope type crosses unchanged and the worker-side contract is
//! identical either way.
//!
//! One instance runs per side, `mode` selecting the role:
//!
//! - **ship** (`mode: 0`, the source node): dials `endpoint` at boot and
//!   redials on loss. Envelopes arriving on `local_in` (the source
//!   worker's `snapshot_export`) are streamed to the peer; envelopes
//!   arriving FROM the peer (the target's install ack) are re-emitted on
//!   `local_out` (the split driver's `ack_in`).
//! - **receive** (`mode: 1`, the target node): binds `listen_port`.
//!   Envelopes arriving from the peer are re-emitted on `local_out` (the
//!   target worker's `snapshot_import`); envelopes on `local_in` (the
//!   worker's `install_ack_out`) are streamed back to the peer.
//!
//! The TCP payload is the raw envelope stream (`[msg][len:u16 LE]
//! [payload…]`, self-framing); the courier reassembles frame boundaries
//! from the byte stream and forwards only COMPLETE envelopes. Bytes that
//! cannot be shipped (dial not yet up and the pending buffer full, or a
//! frame torn by a dropped connection) are counted and dropped WHOLE —
//! the worker's strict in-order chunk accumulation then aborts the
//! install (no ack, no cutover), so transport loss fails closed rather
//! than installing a gap.

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
include!("../../common/hex_core.rs");

#[path = "../../common/net_proto.rs"]
mod net_proto;
#[path = "../../common/telemetry.rs"]
mod telemetry;

use net_proto::{
    net_conn_id, NET_CMD_BIND, NET_CMD_CONNECT, NET_CMD_SEND, NET_CONN_LEN, NET_MSG_ACCEPTED,
    NET_MSG_BOUND, NET_MSG_CLOSED, NET_MSG_CONNOK, NET_MSG_DATA, NET_MSG_ERROR, NET_SOCK_STREAM,
};

/// Envelope TLV header width on the local channels and inside the TCP
/// stream.
const ENV_HDR: usize = 3;
/// Largest single envelope the courier carries: a snapshot CHUNK
/// (header + 28-byte app-snapshot header + 4096-byte chunk) with margin.
const FRAME_MAX: usize = 8192;
/// Reassembly buffer for the inbound TCP byte stream.
const RASM_BUF: usize = 2 * FRAME_MAX;
/// Pending outbound bytes while the dial is still in flight.
const PEND_BUF: usize = 16384;
/// NET-frame scratch (`[type][len][conn_id][bytes…]`).
const NET_BUF: usize = FRAME_MAX + 16;
/// Redial cadence after a lost or failed dial.
const REDIAL_MS: u64 = 1000;

const MODE_SHIP: u8 = 0;
const MODE_RECEIVE: u8 = 1;

define_params! {
    CourierState;

    // 0 = ship (dial out, source node), 1 = receive (listen, target node).
    1, mode, u8, 0
        => |s, d, len| { s.mode = p_u8(d, len, 0, 0); };
    // Ship mode: hex of [ip:4][port:2 LE], the receive side's listener.
    2, endpoint, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ep_hex_len as usize) < 16 {
            s.ep_hex[s.ep_hex_len as usize] = *d.add(i);
            s.ep_hex_len += 1;
            i += 1;
        }
    };
    // Receive mode: TCP port to listen on.
    3, listen_port, u16, 7400
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 7400); };
}

#[repr(C)]
struct CourierState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    local_in: i32,
    local_out: i32,
    metrics_out: i32,

    mode: u8,
    ep_hex: [u8; 16],
    ep_hex_len: u16,
    ip: [u8; 4],
    port: u16,
    listen_port: u16,

    bound: u8,
    connected: u8,
    conn_id: u16,
    dial_ms: u64,

    pend: [u8; PEND_BUF],
    pend_len: u32,
    rasm: [u8; RASM_BUF],
    rasm_len: u32,
    fbuf: [u8; FRAME_MAX],
    nbuf: [u8; NET_BUF],

    step_ctr: u64,
    m_shipped: u64,
    m_delivered: u64,
    m_dropped: u64,
}

impl CourierState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.net_in = -1;
        self.net_out = -1;
        self.local_in = -1;
        self.local_out = -1;
        self.metrics_out = -1;
        self.mode = MODE_SHIP;
        self.ep_hex = [0; 16];
        self.ep_hex_len = 0;
        self.ip = [0; 4];
        self.port = 0;
        self.listen_port = 7400;
        self.bound = 0;
        self.connected = 0;
        self.conn_id = 0;
        self.dial_ms = 0;
        self.pend = [0; PEND_BUF];
        self.pend_len = 0;
        self.rasm = [0; RASM_BUF];
        self.rasm_len = 0;
        self.fbuf = [0; FRAME_MAX];
        self.nbuf = [0; NET_BUF];
        self.step_ctr = 0;
        self.m_shipped = 0;
        self.m_delivered = 0;
        self.m_dropped = 0;
    }
}

/// Stream `bytes` to the peer as one `NET_CMD_SEND`, staged in `nbuf`.
/// Takes the fields it needs individually so callers can hand it one
/// module-owned buffer while reading from another.
unsafe fn tcp_send_raw(
    sys: &SyscallTable,
    net_out: i32,
    conn_id: u16,
    nbuf: &mut [u8; NET_BUF],
    bytes: &[u8],
) -> bool {
    let total = ENV_HDR + NET_CONN_LEN + bytes.len();
    if net_out < 0 || total > NET_BUF {
        return false;
    }
    nbuf[0] = NET_CMD_SEND;
    let plen = (NET_CONN_LEN + bytes.len()) as u16;
    nbuf[1..3].copy_from_slice(&plen.to_le_bytes());
    nbuf[3..5].copy_from_slice(&conn_id.to_le_bytes());
    nbuf[5..5 + bytes.len()].copy_from_slice(bytes);
    (sys.channel_write)(net_out, nbuf.as_mut_ptr(), total) == total as i32
}

/// Append inbound TCP bytes to the reassembly buffer and forward every
/// COMPLETE envelope to `local_out`. A stream that would overflow the
/// buffer (a torn or hostile frame) is dropped whole and counted — the
/// downstream install then aborts on the gap, fail closed.
unsafe fn deliver_stream(s: &mut CourierState, sys: &SyscallTable, bytes: &[u8]) {
    if s.rasm_len as usize + bytes.len() > RASM_BUF {
        s.rasm_len = 0;
        s.m_dropped = s.m_dropped.wrapping_add(1);
        return;
    }
    let at = s.rasm_len as usize;
    s.rasm[at..at + bytes.len()].copy_from_slice(bytes);
    s.rasm_len += bytes.len() as u32;
    loop {
        let have = s.rasm_len as usize;
        if have < ENV_HDR {
            return;
        }
        let flen = ENV_HDR + (u16::from_le_bytes([s.rasm[1], s.rasm[2]]) as usize);
        if flen > FRAME_MAX {
            // Not an envelope stream we can carry; drop it whole.
            s.rasm_len = 0;
            s.m_dropped = s.m_dropped.wrapping_add(1);
            return;
        }
        if have < flen {
            return;
        }
        if s.local_out >= 0 {
            s.fbuf[..flen].copy_from_slice(&s.rasm[..flen]);
            (sys.channel_write)(s.local_out, s.fbuf.as_mut_ptr(), flen);
            s.m_delivered = s.m_delivered.wrapping_add(1);
        }
        s.rasm.copy_within(flen..have, 0);
        s.rasm_len = (have - flen) as u32;
    }
}

/// One attempt to read a raw envelope (header INCLUDED) from `chan`.
enum RawRead {
    /// Nothing readable.
    Empty,
    /// A complete envelope of this many bytes is in the buffer.
    Frame(usize),
    /// An envelope was consumed but cannot be carried (oversize, or the
    /// channel delivered fewer payload bytes than the header promised).
    /// The payload was drained so the channel stays frame-aligned; the
    /// caller counts the drop.
    Dropped,
}

/// Read one raw envelope (header INCLUDED) from `chan` into `buf`. An
/// envelope larger than `buf` is consumed and discarded WHOLE rather
/// than left half-read to desynchronise every frame after it.
unsafe fn read_envelope_raw(sys: &SyscallTable, chan: i32, buf: &mut [u8]) -> RawRead {
    if chan < 0 {
        return RawRead::Empty;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return RawRead::Empty;
    }
    if (sys.channel_read)(chan, buf.as_mut_ptr(), ENV_HDR) < ENV_HDR as i32 {
        return RawRead::Empty;
    }
    let plen = u16::from_le_bytes([buf[1], buf[2]]) as usize;
    if ENV_HDR + plen > buf.len() {
        // Drain the payload in buffer-sized bites and drop it whole.
        let mut left = plen;
        while left > 0 {
            let take = left.min(buf.len());
            let n = (sys.channel_read)(chan, buf.as_mut_ptr(), take);
            if n <= 0 {
                break;
            }
            left -= (n as usize).min(left);
        }
        return RawRead::Dropped;
    }
    if plen > 0 && ((sys.channel_read)(chan, buf.as_mut_ptr().add(ENV_HDR), plen) as usize) < plen {
        return RawRead::Dropped;
    }
    RawRead::Frame(ENV_HDR + plen)
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<CourierState>() as u32
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
    if state_size < core::mem::size_of::<CourierState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let s = unsafe { &mut *state.cast::<CourierState>() };
    s.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(s, params, params_len) };
    }
    s.net_in = in_chan;
    s.net_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        s.local_in = dev_channel_port(sys, 0, 1);
        s.local_out = dev_channel_port(sys, 1, 1);
        s.metrics_out = dev_channel_port(sys, 1, 2);
    }
    if s.mode == MODE_SHIP {
        // Decode [ip:4][port:2 LE] from the endpoint hex.
        let mut ep = [0u8; 8];
        if s.ep_hex_len != 12 {
            return -1;
        }
        for (i, byte) in ep.iter_mut().enumerate().take(6) {
            let hi = hex_nibble(s.ep_hex[i * 2]);
            let lo = hex_nibble(s.ep_hex[i * 2 + 1]);
            let (Some(hi), Some(lo)) = (hi, lo) else {
                return -1;
            };
            *byte = (hi << 4) | lo;
        }
        s.ip.copy_from_slice(&ep[0..4]);
        s.port = u16::from_le_bytes([ep[4], ep[5]]);
        if s.port == 0 {
            return -1;
        }
    }
    0
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<CourierState>() };
    let sys_ptr = s.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    let sys = unsafe { &*sys_ptr };
    let now = unsafe { dev_millis(sys) };

    // Receive mode: one BIND at boot.
    if s.mode == MODE_RECEIVE && s.bound == 0 {
        let port = s.listen_port.to_le_bytes();
        unsafe {
            net_write_frame(
                sys,
                s.net_out,
                NET_CMD_BIND,
                port.as_ptr(),
                2,
                s.nbuf.as_mut_ptr(),
                NET_BUF,
            );
        }
        s.bound = 1;
    }
    // Ship mode: dial (and redial on loss) on a fixed cadence.
    if s.mode == MODE_SHIP
        && s.connected == 0
        && now.wrapping_sub(s.dial_ms) >= REDIAL_MS
        && s.net_out >= 0
    {
        let mut payload = [0u8; 8];
        payload[0] = NET_SOCK_STREAM;
        payload[1] = s.ip[3];
        payload[2] = s.ip[2];
        payload[3] = s.ip[1];
        payload[4] = s.ip[0];
        let port = s.port.to_le_bytes();
        payload[5] = port[0];
        payload[6] = port[1];
        payload[7] = 0;
        unsafe {
            net_write_frame(
                sys,
                s.net_out,
                NET_CMD_CONNECT,
                payload.as_ptr(),
                8,
                s.nbuf.as_mut_ptr(),
                NET_BUF,
            );
        }
        s.dial_ms = now.max(1);
    }

    // Drain NET events.
    for _ in 0..64 {
        let (msg, plen) = unsafe {
            let poll = (sys.channel_poll)(s.net_in, POLL_IN);
            if poll <= 0 || (poll as u32) & POLL_IN == 0 {
                break;
            }
            net_read_frame(sys, s.net_in, s.nbuf.as_mut_ptr(), NET_BUF)
        };
        if msg == 0 {
            break;
        }
        let payload_start = ENV_HDR;
        match msg {
            NET_MSG_CONNOK => {
                if let Some(cid) = net_conn_id(&s.nbuf[payload_start..payload_start + plen]) {
                    s.conn_id = cid;
                    s.connected = 1;
                    unsafe { dev_log(sys, 3, b"[courier] connok".as_ptr(), 16) };
                    // Flush everything queued while the dial was still
                    // in flight.
                    let pl = s.pend_len as usize;
                    let mut at = 0usize;
                    while at < pl {
                        let take = (pl - at).min(FRAME_MAX);
                        let ok = unsafe {
                            tcp_send_raw(
                                sys,
                                s.net_out,
                                s.conn_id,
                                &mut s.nbuf,
                                &s.pend[at..at + take],
                            )
                        };
                        if !ok {
                            s.m_dropped = s.m_dropped.wrapping_add(1);
                            break;
                        }
                        at += take;
                    }
                    s.pend_len = 0;
                }
            }
            NET_MSG_ACCEPTED => {
                // ACCEPTED carries [conn_id:u16][local_port:u16]. The NET
                // provider fans events to every subscriber, so adopt ONLY
                // connections accepted on OUR listener — everything else
                // (the data anchor's clients) is someone else's traffic.
                if s.mode == MODE_RECEIVE && plen >= 4 {
                    let local_port =
                        u16::from_le_bytes([s.nbuf[payload_start + 2], s.nbuf[payload_start + 3]]);
                    if local_port == s.listen_port {
                        if let Some(cid) = net_conn_id(&s.nbuf[payload_start..payload_start + plen])
                        {
                            s.conn_id = cid;
                            s.connected = 1;
                            s.rasm_len = 0;
                            unsafe { dev_log(sys, 3, b"[courier] accept".as_ptr(), 16) };
                        }
                    }
                }
            }
            NET_MSG_DATA => {
                if plen > NET_CONN_LEN {
                    let same = net_conn_id(&s.nbuf[payload_start..payload_start + plen])
                        == Some(s.conn_id);
                    if same && s.connected == 1 {
                        let mut data = [0u8; NET_BUF];
                        let dl = plen - NET_CONN_LEN;
                        data[..dl].copy_from_slice(
                            &s.nbuf[payload_start + NET_CONN_LEN..payload_start + plen],
                        );
                        unsafe { deliver_stream(s, sys, &data[..dl]) };
                    }
                }
            }
            NET_MSG_CLOSED | NET_MSG_ERROR => {
                if net_conn_id(&s.nbuf[payload_start..payload_start + plen]) == Some(s.conn_id)
                    || s.connected == 0
                {
                    s.connected = 0;
                    s.conn_id = 0;
                    s.rasm_len = 0;
                }
            }
            NET_MSG_BOUND => {}
            _ => {}
        }
    }

    // Drain local envelopes toward the peer.
    for _ in 0..16 {
        let mut frame = [0u8; FRAME_MAX];
        let flen = match unsafe { read_envelope_raw(sys, s.local_in, &mut frame) } {
            RawRead::Empty => break,
            RawRead::Dropped => {
                s.m_dropped = s.m_dropped.wrapping_add(1);
                continue;
            }
            RawRead::Frame(n) => n,
        };
        if s.connected == 1 {
            if unsafe { tcp_send_raw(sys, s.net_out, s.conn_id, &mut s.nbuf, &frame[..flen]) } {
                s.m_shipped = s.m_shipped.wrapping_add(1);
            } else {
                s.m_dropped = s.m_dropped.wrapping_add(1);
            }
        } else if s.mode == MODE_SHIP && (s.pend_len as usize) + flen <= PEND_BUF {
            let at = s.pend_len as usize;
            s.pend[at..at + flen].copy_from_slice(&frame[..flen]);
            s.pend_len += flen as u32;
        } else {
            // No connection and no room: drop WHOLE — the strict chunk
            // accumulator downstream aborts on the gap, fail closed.
            s.pend_len = 0;
            s.m_dropped = s.m_dropped.wrapping_add(1);
        }
    }

    s.step_ctr = s.step_ctr.wrapping_add(1);
    if s.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                sys,
                s.metrics_out,
                &[s.m_shipped, s.m_delivered, s.m_dropped],
            );
        }
    }
    0
}
