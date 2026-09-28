//! doc_wire_anchor — the document model's canonical wire over TCP.
//!
//! A client sends `[len:u32 LE][MSG_DOC_REQUEST payload]` frames and reads
//! `[len:u32 LE][MSG_DOC_RESPONSE payload]` frames back; the payloads are
//! exactly `modules/common/doc_exec.rs`'s. There is no dialect: the client
//! states canonical operations over canonical trees and reads canonical
//! answers, so tools, tests, and graphs that already speak the document
//! wire reach `document_executor` without translating twice.
//!
//! The anchor frames and forwards; it validates nothing a request says
//! (the executor refuses a malformed one) beyond the frame. Requests are
//! forwarded as they arrive — the executor queues and answers busy — and
//! the request's `conn` byte is the anchor's (the session's slot), which
//! routes the answer back; a client correlates by `corr`. An answer for a
//! session that has closed is dropped.
//!
//! With `message = 1` the frames carry the JSON document wire
//! (`MSG_JSON_DOC_REQUEST` / `MSG_JSON_DOC_RESPONSE`, `doc_json.rs`) to
//! `doc_json_dialect` instead: the same head and framing, JSON bodies.
//!
//! No authentication: bind it where only trusted peers reach.

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

#[path = "../../common/doc_exec.rs"]
mod doc_exec;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use abi::contracts::net::net_proto::{
    conn_id, CMD_BIND as NET_CMD_BIND, CMD_CLOSE as NET_CMD_CLOSE, CMD_SEND as NET_CMD_SEND,
    CONN_ID_LEN, MAX_CMD_DATA, MSG_ACCEPTED as NET_MSG_ACCEPTED, MSG_BOUND as NET_MSG_BOUND,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_DATA as NET_MSG_DATA, MSG_ERROR as NET_MSG_ERROR,
};
use doc_exec::{DOC_REQUEST_HEAD, DOC_REQUEST_MAX, DOC_RESPONSE_HEAD, DOC_RESPONSE_MAX};
use wire::{MSG_DOC_REQUEST, MSG_DOC_RESPONSE, MSG_JSON_DOC_REQUEST, MSG_JSON_DOC_RESPONSE};

/// Sessions.
const MAX_CONNS: usize = 8;
/// A session's inbound bytes: one whole request frame, or — streaming a
/// large one — a part not yet whole and the net frame that completes it.
const RECV_BUF: usize = 4 + DOC_REQUEST_MAX + wire::FRAME_PAYLOAD_MAX;
/// A session's unsent bytes: one whole answer frame, or a part of a
/// larger one beside what is still going out.
const SEND_BUF: usize = 2 * (4 + DOC_RESPONSE_MAX);
/// The largest frame this wire carries: MongoDB's message bound. A frame
/// past one channel frame streams to the executor as `wire::MSG_PART`
/// parts (one session at a time), whose build decides what it takes.
const WIRE_FRAME_MAX: usize = 16777216;
/// The most answer bytes one `CMD_SEND` carries: the net contract's
/// `MAX_CMD_DATA`, and no more than one channel frame holds beside the
/// connection id — a frame past its channel can never be written, and
/// the session would stall at the first full chunk.
const NET_CHUNK: usize = if MAX_CMD_DATA < wire::FRAME_PAYLOAD_MAX - CONN_ID_LEN {
    MAX_CMD_DATA
} else {
    wire::FRAME_PAYLOAD_MAX - CONN_ID_LEN
};
const SLOT_FREE: u16 = 0xFFFF;
const DEFAULT_LISTEN_PORT: u16 = 27100;

const PHASE_INIT: u8 = 0;
const PHASE_WAIT_BOUND: u8 = 1;
const PHASE_LISTENING: u8 = 2;

#[repr(C)]
struct Slot {
    conn_id: u16,
    closing: bool,
    /// Streaming a large request: its bytes still to come (the frame's
    /// length prefix already consumed).
    up_left: usize,
    recv_len: usize,
    recv: [u8; RECV_BUF],
    send_len: usize,
    send: [u8; SEND_BUF],
}

define_params! {
    AnchorState;

    1, listen_port, u16, 27100
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 27100); };
    2, message, u8, 0
        => |s, d, len| { s.json = p_u8(d, len, 0, 0) == 1; };
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
    /// Frames carry the JSON document wire rather than the canonical one.
    json: bool,
    phase: u8,
    slots: [Slot; MAX_CONNS],
    env: [u8; wire::ENVELOPE_HDR + DOC_RESPONSE_MAX],
    net: [u8; NET_FRAME_HDR + CONN_ID_LEN + NET_CHUNK],
    /// The session streaming a large request to the executor (one at a
    /// time: parts on one channel must not interleave), and the stream.
    up_slot: usize,
    up_total: usize,
    up_off: usize,
    up_stream: u32,
    /// The large answer streaming to a session, and one part the session
    /// had no room for (nothing more is read from the executor until it
    /// goes).
    down_slot: usize,
    down_stream: u32,
    down_active: bool,
    down_hold: [u8; wire::FRAME_PAYLOAD_MAX],
    down_hold_len: usize,
    m_sessions: u64,
    m_requests: u64,
    m_answers: u64,
    m_refused: u64,
    step_ctr: u64,
}

unsafe fn net_send(a: &mut AnchorState, cmd: u8, conn: u16, data: &[u8]) -> bool {
    let sys = a.syscalls;
    if sys.is_null() || a.net_out < 0 || data.len() > NET_CHUNK {
        return false;
    }
    let payload = CONN_ID_LEN + data.len();
    let n = &mut a.net;
    n[0] = cmd;
    n[1] = (payload & 0xFF) as u8;
    n[2] = (payload >> 8) as u8;
    n[NET_FRAME_HDR..NET_FRAME_HDR + 2].copy_from_slice(&conn.to_le_bytes());
    n[NET_FRAME_HDR + CONN_ID_LEN..NET_FRAME_HDR + payload].copy_from_slice(data);
    let total = NET_FRAME_HDR + payload;
    ((*sys).channel_write)(a.net_out, n.as_mut_ptr(), total) == total as i32
}

unsafe fn net_bind(a: &mut AnchorState) -> bool {
    let sys = a.syscalls;
    if sys.is_null() || a.net_out < 0 {
        return false;
    }
    let port = a.listen_port.to_le_bytes();
    let n = &mut a.net;
    n[0] = NET_CMD_BIND;
    n[1] = 2;
    n[2] = 0;
    n[NET_FRAME_HDR] = port[0];
    n[NET_FRAME_HDR + 1] = port[1];
    let total = NET_FRAME_HDR + 2;
    ((*sys).channel_write)(a.net_out, n.as_mut_ptr(), total) == total as i32
}

/// Send what a session has queued, a net frame at a time, keeping what
/// the channel does not take.
fn flush(a: &mut AnchorState, i: usize) {
    while a.slots[i].send_len > 0 {
        let n = a.slots[i].send_len.min(NET_CHUNK);
        let conn = a.slots[i].conn_id;
        let mut chunk = [0u8; NET_CHUNK];
        chunk[..n].copy_from_slice(&a.slots[i].send[..n]);
        if !unsafe { net_send(a, NET_CMD_SEND, conn, &chunk[..n]) } {
            return;
        }
        let s = &mut a.slots[i];
        let rest = s.send_len - n;
        let mut k = 0;
        while k < rest {
            s.send[k] = s.send[n + k];
            k += 1;
        }
        s.send_len = rest;
    }
}

/// Forward every whole request frame a session has received.
fn drain(a: &mut AnchorState, i: usize) {
    loop {
        let s = &a.slots[i];
        if s.recv_len < 4 {
            return;
        }
        if s.up_left > 0 {
            if !stream_up(a, i) {
                return;
            }
            continue;
        }
        let len = u32::from_le_bytes([s.recv[0], s.recv[1], s.recv[2], s.recv[3]]) as usize;
        if len > DOC_REQUEST_MAX && (DOC_REQUEST_HEAD..=WIRE_FRAME_MAX).contains(&len) {
            // A large request: streamed, once no other session streams.
            if a.up_slot != usize::MAX && a.up_slot != i {
                return;
            }
            a.up_slot = i;
            a.up_total = len;
            a.up_off = 0;
            a.up_stream = a.up_stream.wrapping_add(1);
            let s = &mut a.slots[i];
            s.up_left = len;
            shift(s, 4);
            continue;
        }
        if !(DOC_REQUEST_HEAD..=DOC_REQUEST_MAX).contains(&len) {
            // Not a frame this wire carries: the session is unrecoverable.
            a.slots[i].closing = true;
            a.m_refused = a.m_refused.wrapping_add(1);
            return;
        }
        if s.recv_len < 4 + len {
            return;
        }
        let at = wire::ENVELOPE_HDR;
        a.env[at..at + len].copy_from_slice(&a.slots[i].recv[4..4 + len]);
        // The conn byte is the anchor's: it routes the answer back.
        a.env[at + 13] = i as u8;
        a.env[0] = request_type(a);
        a.env[1] = (len & 0xFF) as u8;
        a.env[2] = (len >> 8) as u8;
        let total = at + len;
        let sent = unsafe {
            let sys = a.syscalls;
            !sys.is_null()
                && ((*sys).channel_write)(a.exec_out, a.env.as_mut_ptr(), total) == total as i32
        };
        if !sent {
            // The executor's channel is full: keep the frame and retry on a
            // later step.
            return;
        }
        a.m_requests = a.m_requests.wrapping_add(1);
        let s = &mut a.slots[i];
        let rest = s.recv_len - 4 - len;
        let mut k = 0;
        while k < rest {
            s.recv[k] = s.recv[4 + len + k];
            k += 1;
        }
        s.recv_len = rest;
    }
}

/// Whether every session has room for one more net frame.
fn net_room(a: &AnchorState) -> bool {
    a.slots
        .iter()
        .all(|s| s.conn_id == SLOT_FREE || s.recv_len + wire::FRAME_PAYLOAD_MAX <= RECV_BUF)
}

/// The message a request frame is forwarded as.
fn request_type(a: &AnchorState) -> u8 {
    if a.json {
        MSG_JSON_DOC_REQUEST
    } else {
        MSG_DOC_REQUEST
    }
}

/// Drop `n` bytes from the front of a session's inbound buffer.
fn shift(s: &mut Slot, n: usize) {
    let rest = s.recv_len.saturating_sub(n);
    let mut k = 0;
    while k < rest {
        s.recv[k] = s.recv[n + k];
        k += 1;
    }
    s.recv_len = rest;
}

/// Send the next part of session `i`'s large request: a full part's worth
/// once it has arrived, or the last. `false` when it must wait (bytes not
/// here yet, or the executor's channel full).
fn stream_up(a: &mut AnchorState, i: usize) -> bool {
    let avail = a.slots[i].recv_len.min(a.slots[i].up_left);
    let n = avail.min(wire::PART_BYTES_MAX);
    if n == 0 || (n < wire::PART_BYTES_MAX && n < a.slots[i].up_left) {
        return false;
    }
    let payload = wire::PART_HEAD + n;
    let at = wire::ENVELOPE_HDR;
    let rt = request_type(a);
    let e = &mut a.env;
    e[0] = wire::MSG_PART;
    e[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
    e[at..at + 4].copy_from_slice(&a.up_stream.to_le_bytes());
    e[at + 4..at + 8].copy_from_slice(&(a.up_off as u32).to_le_bytes());
    e[at + 8..at + 12].copy_from_slice(&(a.up_total as u32).to_le_bytes());
    e[at + 12] = rt;
    let b = at + wire::PART_HEAD;
    e[b..b + n].copy_from_slice(&a.slots[i].recv[..n]);
    // The conn byte is the anchor's: it routes the answer back.
    if a.up_off <= 13 && a.up_off + n > 13 {
        e[b + 13 - a.up_off] = i as u8;
    }
    let total = at + payload;
    let sent = unsafe {
        let sys = a.syscalls;
        !sys.is_null()
            && ((*sys).channel_write)(a.exec_out, a.env.as_mut_ptr(), total) == total as i32
    };
    if !sent {
        return false;
    }
    a.up_off += n;
    let s = &mut a.slots[i];
    s.up_left -= n;
    shift(s, n);
    if s.up_left == 0 {
        a.up_slot = usize::MAX;
        a.m_requests = a.m_requests.wrapping_add(1);
    }
    true
}

/// A part of a large answer: to the session its first part names, the
/// frame's length first. `false` when the session has no room yet (the
/// part is kept, and nothing more is read until it goes).
fn on_answer_part(a: &mut AnchorState, p: &[u8]) -> bool {
    let rd = |at: usize| -> usize {
        p.get(at..at + 4)
            .and_then(|b| b.try_into().ok())
            .map_or(0, |b: [u8; 4]| u32::from_le_bytes(b) as usize)
    };
    let (stream, off, total) = (rd(0) as u32, rd(4), rd(8));
    let Some(bytes) = p.get(wire::PART_HEAD..) else {
        return true;
    };
    if off == 0 {
        let Some(&conn) = bytes.get(8) else {
            return true;
        };
        a.down_slot = conn as usize;
        a.down_stream = stream;
        a.down_active = true;
    } else if !a.down_active || stream != a.down_stream {
        return true;
    }
    let i = a.down_slot;
    if i >= MAX_CONNS || a.slots[i].conn_id == SLOT_FREE {
        a.down_active = off + bytes.len() < total;
        return true;
    }
    let prefix = if off == 0 { 4 } else { 0 };
    let s = &mut a.slots[i];
    if s.send_len + prefix + bytes.len() > SEND_BUF {
        return false;
    }
    let at = s.send_len;
    if off == 0 {
        s.send[at..at + 4].copy_from_slice(&(total as u32).to_le_bytes());
    }
    s.send[at + prefix..at + prefix + bytes.len()].copy_from_slice(bytes);
    s.send_len += prefix + bytes.len();
    if off + bytes.len() >= total {
        a.down_active = false;
        a.m_answers = a.m_answers.wrapping_add(1);
    }
    flush(a, i);
    true
}

/// An answer from the executor: to the session its conn byte names.
fn on_answer(a: &mut AnchorState, payload: &[u8]) {
    if payload.len() < DOC_RESPONSE_HEAD {
        return;
    }
    let i = payload[8] as usize;
    if i >= MAX_CONNS || a.slots[i].conn_id == SLOT_FREE {
        return;
    }
    let s = &mut a.slots[i];
    if s.send_len + 4 + payload.len() > SEND_BUF {
        // The client is not reading its answers: close it rather than
        // drop one silently.
        s.closing = true;
        return;
    }
    let at = s.send_len;
    s.send[at..at + 4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    s.send[at + 4..at + 4 + payload.len()].copy_from_slice(payload);
    s.send_len += 4 + payload.len();
    a.m_answers = a.m_answers.wrapping_add(1);
    flush(a, i);
}

unsafe fn on_net(a: &mut AnchorState, msg: u8, p: &[u8]) {
    match msg {
        NET_MSG_BOUND => {
            if a.phase == PHASE_WAIT_BOUND
                && (p.len() < 4 || u16::from_le_bytes([p[2], p[3]]) == a.listen_port)
            {
                a.phase = PHASE_LISTENING;
            }
        }
        NET_MSG_ACCEPTED => {
            if p.len() >= 4 && u16::from_le_bytes([p[2], p[3]]) != a.listen_port {
                return;
            }
            if p.len() < CONN_ID_LEN {
                return;
            }
            let id = conn_id(p);
            match a.slots.iter().position(|s| s.conn_id == SLOT_FREE) {
                Some(i) => {
                    let s = &mut a.slots[i];
                    s.conn_id = id;
                    s.closing = false;
                    s.recv_len = 0;
                    s.send_len = 0;
                    s.up_left = 0;
                    a.m_sessions = a.m_sessions.wrapping_add(1);
                }
                None => {
                    let _ = net_send(a, NET_CMD_CLOSE, id, &[]);
                }
            }
        }
        NET_MSG_DATA => {
            if p.len() <= CONN_ID_LEN {
                return;
            }
            let id = conn_id(p);
            let data = &p[CONN_ID_LEN..];
            let Some(i) = a.slots.iter().position(|s| s.conn_id == id) else {
                return;
            };
            let s = &mut a.slots[i];
            if s.recv_len + data.len() > RECV_BUF {
                s.closing = true;
                return;
            }
            s.recv[s.recv_len..s.recv_len + data.len()].copy_from_slice(data);
            s.recv_len += data.len();
            drain(a, i);
        }
        NET_MSG_CLOSED | NET_MSG_ERROR => {
            if p.len() >= CONN_ID_LEN {
                let id = conn_id(p);
                if let Some(i) = a.slots.iter().position(|s| s.conn_id == id) {
                    a.slots[i].conn_id = SLOT_FREE;
                    if a.up_slot == i {
                        a.up_slot = usize::MAX;
                    }
                }
            }
        }
        _ => {}
    }
}

unsafe fn read_envelope<'a>(
    sys: &SyscallTable,
    chan: i32,
    buf: &'a mut [u8],
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
    let n = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if n > buf.len() {
        let mut left = n;
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
    if n > 0 && ((sys.channel_read)(chan, buf.as_mut_ptr(), n) as usize) < n {
        return None;
    }
    Some((hdr[0], &buf[..n]))
}

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
    let a = unsafe { &mut *state.cast::<AnchorState>() };
    a.syscalls = sys_ptr;
    a.listen_port = DEFAULT_LISTEN_PORT;
    a.phase = PHASE_INIT;
    for s in a.slots.iter_mut() {
        s.conn_id = SLOT_FREE;
        s.closing = false;
        s.recv_len = 0;
        s.send_len = 0;
        s.up_left = 0;
    }
    a.up_slot = usize::MAX;
    a.down_active = false;
    a.down_hold_len = 0;
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(a, params, params_len) };
    }
    // inputs: net_in[0], exec_in[1]; outputs: net_out[0], exec_out[1], metrics[2]
    a.net_in = in_chan;
    a.net_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        a.exec_in = dev_channel_port(sys, 0, 1);
        a.exec_out = dev_channel_port(sys, 1, 1);
        a.metrics_out = dev_channel_port(sys, 1, 2);
    }
    if a.exec_in < 0 || a.exec_out < 0 {
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
    let a = unsafe { &mut *state.cast::<AnchorState>() };
    let sys_ptr = a.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    if a.phase == PHASE_INIT && unsafe { net_bind(a) } {
        a.phase = PHASE_WAIT_BOUND;
    }
    unsafe {
        let sys = &*sys_ptr;
        for _ in 0..8 {
            // A session that cannot take one more net frame holds the
            // transport: the bytes wait in the socket, and TCP windows the
            // peer down, rather than arriving where there is no room.
            if !net_room(a) {
                break;
            }
            let mut buf = [0u8; RECV_BUF];
            let Some((msg, p)) = read_envelope(sys, a.net_in, &mut buf) else {
                break;
            };
            on_net(a, msg, p);
        }
        // A held answer part first; nothing more is read until it goes.
        if a.down_hold_len > 0 {
            let mut part = [0u8; wire::FRAME_PAYLOAD_MAX];
            let n = a.down_hold_len;
            part[..n].copy_from_slice(&a.down_hold[..n]);
            if on_answer_part(a, &part[..n]) {
                a.down_hold_len = 0;
            }
        }
        for _ in 0..4 {
            if a.down_hold_len > 0 {
                break;
            }
            let mut buf = [0u8; DOC_RESPONSE_MAX];
            let Some((msg, p)) = read_envelope(sys, a.exec_in, &mut buf) else {
                break;
            };
            if msg == MSG_DOC_RESPONSE || msg == MSG_JSON_DOC_RESPONSE {
                on_answer(a, p);
            } else if msg == wire::MSG_PART && !on_answer_part(a, p) {
                a.down_hold[..p.len()].copy_from_slice(p);
                a.down_hold_len = p.len();
            }
        }
    }
    for i in 0..MAX_CONNS {
        if a.slots[i].conn_id == SLOT_FREE {
            continue;
        }
        if a.slots[i].closing {
            let conn = a.slots[i].conn_id;
            let _ = unsafe { net_send(a, NET_CMD_CLOSE, conn, &[]) };
            a.slots[i].conn_id = SLOT_FREE;
            if a.up_slot == i {
                a.up_slot = usize::MAX;
            }
            continue;
        }
        flush(a, i);
        drain(a, i);
    }
    a.step_ctr = a.step_ctr.wrapping_add(1);
    if a.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                a.metrics_out,
                &[a.m_sessions, a.m_requests, a.m_answers, a.m_refused],
            );
        }
    }
    0
}
