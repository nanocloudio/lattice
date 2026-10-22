//! Redis connector — a GENUINE per-protocol Fluxor foundation module.
//!
//! Unlike a generic transport driven by encode/decode bytecode, this module owns
//! the Redis protocol as compiled code: a PERSISTENT connection (open across many
//! requests, not reconnect-per-call) and a multi-round-trip `AUTH` handshake
//! (send AUTH, branch on the server's `+OK`/`-ERR`, only then serve requests).
//! Those are reply-dependent, stateful behaviours a stateless codec cannot
//! express — which is the whole reason a real connector is a module.
//!
//! The protocol logic lives in the shared `redis_core.rs`, the exchange
//! provider half in `query_exchange.rs`; this file is the I/O pump that maps
//! their actions onto net_proto frames and exchange records.
//!
//! # Ports
//!
//! `net_in`/`net_out` (transport), and the exchange provider pair
//! `request_in` (`ExchangeRequest`) / `response_out` (`ExchangeResponse`).
//! A module with `request_in` wired and `response_out` unwired refuses to
//! construct.
//!
//! # Params
//!
//! `authority` (`host[:port]`, port 6379 when omitted), `password` (optional;
//! enables AUTH).
//!
//! # Request
//!
//! Method POST; `target` empty; headers ignored; body = exactly one
//! STRUCTURED command, little-endian:
//!
//! ```text
//! [nargs:u8] then nargs × ( [len:u16][arg bytes] )
//! ```
//!
//! e.g. `SET greeting hi` is `03 | 03 00 "SET" | 08 00 "greeting" | 02 00 "hi"`.
//! The module builds the RESP wire bytes itself, so a requester sends
//! arguments, never protocol bytes. Body up to 4096 bytes.
//!
//! # Answer
//!
//! One answer per request, no content type, no headers:
//!
//! | Status | Reply | Body |
//! |---|---|---|
//! | 200 | `+OK` simple string | the text, without `+` and CRLF (`OK`) |
//! | 200 | `:N` integer | the decimal digits as sent (`42`, `-1`) |
//! | 200 | `$n` bulk string | the `n` data bytes |
//! | 200 | `$-1` nil | empty |
//! | 200 | `*N` array | the whole RESP reply, header included, verbatim |
//! | 400 | `-ERR …` error | the error text, without `-` and CRLF (`ERR unknown command …`) |
//! | 400 | — | another method, a non-empty target, a body that is not exactly one structured command |
//! | 413 | — | a body over 4096 bytes, or a command whose RESP form exceeds 8192 bytes |
//! | 502 | — | the server could not be reached, or refused AUTH, before the link was ever up (the `-ERR` text as the body when AUTH was refused); or a reply over 8192 bytes (the connection is closed) |
//! | 503 | — | four requests already held, or draining |
//! | 504 | — | no reply within 15 s (the connection is closed; the command's effect is unknown) |
//!
//! The body streams under the requester's response credit.
//!
//! The same requests and answers are served in-graph by `kv_front` over
//! lattice's own KV path (`docs/guides/fronts.md`): an application swaps a
//! foreign Redis for the embedded store by wiring alone. The front writes no
//! LINK record (it has no connection to lose) and answers a command whose
//! meaning is a connection's state (AUTH, SELECT, MULTI, pub/sub) 400.
//!
//! # LINK
//!
//! The connection opens on the first request. Once a session has been ready,
//! losing it (a close, a transport error, a failed reconnect) writes LINK DOWN:
//! every request held is dropped unanswered for the requester to re-issue, and
//! request records are read and dropped until LINK UP. The module reconnects
//! on its own (1 s, doubling to 30 s), re-running AUTH, and writes LINK UP once
//! the new session is ready.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "the fluxor SDK + shared cores are include!'d wholesale; each module consumes only a subset"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    clippy::duplicate_mod,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, ABI fns carry a fixed arity, and the PIC build #[path]-remounts shared SDK/common code"
)]
use core::ffi::c_void;

#[allow(
    unused_imports,
    dead_code,
    reason = "shared SDK surface across modules"
)]
#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

// Shared cores.
include!("../../common/resp_core.rs");
include!("../../common/redis_core.rs");

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox};

#[path = "../../common/authority.rs"]
mod authority;
use authority::{Authority, PORT_REDIS};

#[path = "../../common/query_exchange.rs"]
mod query_exchange;
use query_exchange::exchange::{status, METHOD_POST, RECORD_MAX};
use query_exchange::QueryDesk;

use abi::contracts::net::net_proto::{
    conn_id, connected_parts, error_parts, RequesterTag, CMD_CLOSE as NET_CMD_CLOSE,
    CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CMD_SEND as NET_CMD_SEND, CONNECT_TO_MAX, CONN_ID_LEN,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_CONNECTED as NET_MSG_CONNECTED, MSG_DATA as NET_MSG_DATA,
    MSG_ERROR as NET_MSG_ERROR, REQUESTER_TAG_NONE,
};

const NET_BUF: usize = 2048;
/// Longest structured command one request carries.
const CMD_MAX: usize = 4096;
/// The RESP form of a command (or AUTH) being written.
const REQ_BUF: usize = 8192;
/// One whole reply.
const REPLY_BUF: usize = 8192;
const PASS_BUF: usize = 128;
/// Requests held at once: one performed, the rest queued in arrival order.
const SLOTS: usize = 4;
/// A request target is refused unless empty; this bounds what is read.
const TARGET_MAX: usize = 64;
const QUEUE: usize = 8;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;
const PROBE_MIN_MS: u64 = 1_000;
const PROBE_MAX_MS: u64 = 30_000;

#[repr(C)]
struct RedisState {
    syscalls: *const SyscallTable,
    net_in: i32,
    /// The transport input's envelope reading (`common/envelope.rs`).
    net_inbox: Inbox,
    net_out: i32,
    request_in: i32,
    response_out: i32,

    authority: Authority,
    password: [u8; PASS_BUF],
    password_len: u16,

    phase: Phase,
    conn_id: u16,
    tag: RequesterTag,
    started_ms: u64,
    draining: u8,

    // Reconnect after a LINK DOWN: the next attempt and the current wait.
    probing: u8,
    probe_at_ms: u64,
    backoff_ms: u64,

    // The structured command being performed (kept so it can be sent after
    // the AUTH handshake completes).
    cmd: [u8; CMD_MAX],
    cmd_len: u16,
    has_cmd: u8,

    // Transient flush buffer: the RESP bytes currently being written (AUTH or
    // the encoded command).
    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,

    reply: [u8; REPLY_BUF],
    reply_len: u16,
    // The answer body: the reply's value.
    result: [u8; REPLY_BUF],

    nbuf: [u8; NET_BUF],
    completed: u32,
    errors: u32,
    reconnects: u32,

    desk: QueryDesk<SLOTS, TARGET_MAX, CMD_MAX, QUEUE>,
    outbox: ExchangeOutbox,
    // The request record being read, and the response record being written.
    rec: [u8; RECORD_MAX],
    out: [u8; RECORD_MAX],
}

define_params! {
    RedisState;

    // Tag 1 is retired.
    // The peer, `host[:port]`; the port defaults to 6379.
    3, authority, str, 0 => |s, d, len| {
        if len > 0 {
            s.authority.set(core::slice::from_raw_parts(d, len));
        }
    };

    // Optional password. When present, the module runs the AUTH handshake before
    // serving any request.
    2, password, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.password_len as usize) < PASS_BUF {
            s.password[s.password_len as usize] = *d.add(i);
            s.password_len += 1;
            i += 1;
        }
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<RedisState>() as u32
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
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<RedisState>() {
            return -2;
        }
        let s = &mut *(state as *mut RedisState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_inbox = Inbox::new();
        s.net_out = out_chan;
        s.request_in = dev_channel_port(sys, 0, 1);
        s.response_out = dev_channel_port(sys, 1, 1);
        s.authority.clear();
        s.password_len = 0;
        s.phase = Phase::Disconnected;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.started_ms = 0;
        s.draining = 0;
        s.probing = 0;
        s.probe_at_ms = 0;
        s.backoff_ms = 0;
        s.cmd_len = 0;
        s.has_cmd = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.reply_len = 0;
        s.completed = 0;
        s.errors = 0;
        s.reconnects = 0;
        s.desk.reset(METHOD_POST, 0);
        s.outbox = ExchangeOutbox::new();
        parse_tlv(s, params, params_len);
        if !s.authority.adopt(PORT_REDIS) {
            let m = b"[redis] refusing to construct: authority (host[:port]) is required";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if s.request_in >= 0 && s.response_out < 0 {
            let m = b"[redis] refusing to construct: request_in is wired but response_out is not";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        dev_log(sys, 3, b"[redis] init".as_ptr(), 12);
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_drain"]
pub extern "C" fn module_drain(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut RedisState);
        s.draining = 1; // stop accepting new work; finish in-flight, then Done
        0
    }
}

/// Close the connection (if one is open) and return to Disconnected.
unsafe fn close_conn(s: &mut RedisState) {
    let sys = &*s.syscalls;
    if s.conn_id != 0 {
        let close = s.conn_id.to_le_bytes();
        net_write_frame(
            sys,
            s.net_out,
            NET_CMD_CLOSE,
            close.as_ptr(),
            CONN_ID_LEN,
            s.nbuf.as_mut_ptr(),
            NET_BUF,
        );
    }
    s.conn_id = 0;
    s.phase = Phase::Disconnected;
    s.reply_len = 0;
    s.req_len = 0;
    s.req_sent = 0;
}

/// The session failed or dropped. After the link was up this is a LINK DOWN
/// (every request held is dropped for the requester to re-issue) and the
/// module reconnects on its own; before it ever came up nothing was
/// reported, so the request being performed is answered 502 here, with the
/// first `text_len` bytes of `result` as the body.
unsafe fn connection_lost(s: &mut RedisState, now: u64, text_len: usize) {
    close_conn(s);
    s.errors = s.errors.wrapping_add(1);
    s.reconnects = s.reconnects.wrapping_add(1);
    if s.probing == 1 {
        s.probe_at_ms = now.wrapping_add(s.backoff_ms);
        s.backoff_ms = (s.backoff_ms * 2).min(PROBE_MAX_MS);
        return;
    }
    if s.desk.link_down() {
        let sys = &*s.syscalls;
        dev_log(sys, 2, b"[redis] link down".as_ptr(), 17);
        s.has_cmd = 0;
        s.probing = 1;
        s.probe_at_ms = now.wrapping_add(PROBE_MIN_MS);
        s.backoff_ms = (PROBE_MIN_MS * 2).min(PROBE_MAX_MS);
        return;
    }
    if s.has_cmd == 1 {
        s.desk.finish(status::BAD_GATEWAY, text_len.min(REPLY_BUF));
        s.has_cmd = 0;
    }
}

/// A session is ready: the link is up.
unsafe fn session_ready(s: &mut RedisState) {
    if s.probing == 1 {
        let sys = &*s.syscalls;
        dev_log(sys, 3, b"[redis] link up".as_ptr(), 15);
    }
    s.probing = 0;
    s.backoff_ms = 0;
    s.desk.link_up();
}

/// Copy the classified reply's value into the answer buffer.
fn reply_value(s: &mut RedisState) -> (RespReply, usize) {
    let r = resp_classify(&s.reply[..s.reply_len as usize]);
    let n = if r.val_end <= s.reply_len as usize && r.val_end >= r.val_start {
        let n = r.val_end - r.val_start;
        let (reply, result) = (&s.reply, &mut s.result);
        result[..n].copy_from_slice(&reply[r.val_start..r.val_end]);
        n
    } else {
        0
    };
    (r, n)
}

/// Feed one event into the state machine and execute the resulting action.
unsafe fn feed(s: &mut RedisState, sys: &SyscallTable, ev: Ev, now: u64) {
    let needs_auth = s.password_len > 0;
    let prev = s.phase;
    let (action, next) = redis_transition(prev, ev, needs_auth, s.has_cmd == 1);
    s.phase = next;
    if matches!(prev, Phase::Connecting | Phase::Authing)
        && matches!(next, Phase::Ready | Phase::Awaiting)
    {
        session_ready(s);
    }
    match action {
        Act::Connect => {
            let mut payload = [0u8; CONNECT_TO_MAX];
            let n = s.authority.connect_record(&mut payload, Some(s.tag));
            if n > 0 {
                net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CONNECT_TO,
                    payload.as_ptr(),
                    n,
                    s.nbuf.as_mut_ptr(),
                    NET_BUF,
                );
            }
            s.started_ms = now;
        }
        Act::SendAuth => {
            s.reply_len = 0;
            let mut out = [0u8; 160];
            if let Some(n) = resp_auth(&s.password[..s.password_len as usize], &mut out) {
                let n = n.min(REQ_BUF);
                s.req[..n].copy_from_slice(&out[..n]);
                s.req_len = n as u16;
            } else {
                s.req_len = 0;
            }
            s.req_sent = 0;
            s.started_ms = now;
        }
        Act::SendRequest => {
            s.reply_len = 0;
            let cmd_len = s.cmd_len as usize;
            // The command was checked to encode when it was taken.
            let (cmd, req) = (&s.cmd, &mut s.req);
            s.req_len = resp_cmd_structured(&cmd[..cmd_len], req).unwrap_or(0) as u16;
            s.req_sent = 0;
            s.started_ms = now;
        }
        Act::DeliverReply => {
            let (r, n) = reply_value(s);
            let code = if r.is_error() {
                status::BAD_REQUEST
            } else {
                status::OK
            };
            s.desk.finish(code, n);
            s.reply_len = 0;
            s.has_cmd = 0;
            s.completed = s.completed.wrapping_add(1);
        }
        Act::FailRequest => {
            // AUTH refused: the server's text explains it.
            let text = if prev == Phase::Authing && ev == Ev::ReplyErr {
                reply_value(s).1
            } else {
                0
            };
            connection_lost(s, now, text);
        }
        Act::Opened | Act::None => {}
    }
}

/// Place owed response records until the port is full or nothing is owed.
unsafe fn flush_answers(s: &mut RedisState) {
    let sys = &*s.syscalls;
    if s.response_out < 0 {
        return;
    }
    loop {
        if !s.outbox.flush(sys, s.response_out, &s.out) {
            return;
        }
        let Some(n) = s.desk.next_record(&s.result, &mut s.out) else {
            return;
        };
        if !s.outbox.send(sys, s.response_out, &s.out, n) {
            return;
        }
    }
}

/// Read request records while the desk can take them; after a LINK DOWN,
/// read and drop them until the LINK UP is away.
unsafe fn read_requests(s: &mut RedisState) {
    let sys = &*s.syscalls;
    if s.request_in < 0 {
        return;
    }
    loop {
        let discarding = s.desk.discarding();
        if !discarding && !s.desk.can_take() {
            return;
        }
        let poll = (sys.channel_poll)(s.request_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.request_in, s.rec.as_mut_ptr(), RECORD_MAX);
        if n <= 0 {
            return;
        }
        if !discarding {
            s.desk.accept(&s.rec[..n as usize]);
        }
    }
}

/// Take the oldest complete request, when none is being performed.
unsafe fn take_request(s: &mut RedisState) {
    if s.has_cmd == 1 || s.probing == 1 {
        return;
    }
    let Some(at) = s.desk.next_request() else {
        return;
    };
    let Some((target, body)) = s.desk.request(at).map(|r| (r.target.len(), r.body)) else {
        return;
    };
    if target != 0 || structured_len(body) != Some(body.len()) {
        s.desk.refuse(at, status::BAD_REQUEST);
        return;
    }
    if resp_cmd_structured(body, &mut s.req).is_none() {
        s.desk.refuse(at, status::TOO_LARGE);
        return;
    }
    let n = body.len();
    s.cmd[..n].copy_from_slice(body);
    s.cmd_len = n as u16;
    s.has_cmd = 1;
    s.desk.start(at);
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut RedisState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. Answers first, then requests in, and the next one to perform.
        flush_answers(s);
        if s.draining == 1 {
            s.desk.drain();
        }
        read_requests(s);
        take_request(s);

        // 2. A queued command drives connect (if disconnected) or send (if
        //    ready); after a LINK DOWN the module reconnects on its own.
        if s.has_cmd == 1 && matches!(s.phase, Phase::Disconnected | Phase::Ready) {
            feed(s, sys, Ev::HaveRequest, now);
        } else if s.probing == 1 && s.phase == Phase::Disconnected && now >= s.probe_at_ms {
            feed(s, sys, Ev::Probe, now);
        }

        // 3. Drain network events into the state machine.
        if s.net_in >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.net_in, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let Some((msg, plen)) = read_envelope(&mut s.net_inbox, sys, s.net_in, &mut s.nbuf)
                else {
                    break;
                };
                let payload = s.nbuf.as_ptr();
                let pl = core::slice::from_raw_parts(payload, plen);
                match msg {
                    NET_MSG_CONNECTED if s.phase == Phase::Connecting => {
                        if plen >= CONN_ID_LEN {
                            let (id, tag) = connected_parts(pl);
                            if tag == s.tag || tag == REQUESTER_TAG_NONE {
                                s.conn_id = id;
                                feed(s, sys, Ev::Connected, now);
                            }
                        }
                    }
                    NET_MSG_DATA if s.phase == Phase::Authing || s.phase == Phase::Awaiting => {
                        if plen > CONN_ID_LEN && conn_id(pl) == s.conn_id {
                            let data_len = plen - CONN_ID_LEN;
                            let space = REPLY_BUF - s.reply_len as usize;
                            if data_len > space {
                                // A reply larger than the buffer can never
                                // complete: it cannot be carried back.
                                overflowed(s, now);
                                break;
                            }
                            core::ptr::copy_nonoverlapping(
                                payload.add(CONN_ID_LEN),
                                s.reply.as_mut_ptr().add(s.reply_len as usize),
                                data_len,
                            );
                            s.reply_len += data_len as u16;
                            let r = resp_classify(&s.reply[..s.reply_len as usize]);
                            if r.kind != RespKind::Incomplete {
                                let ev = if r.is_error() {
                                    Ev::ReplyErr
                                } else {
                                    Ev::ReplyOk
                                };
                                feed(s, sys, ev, now);
                            } else if s.reply_len as usize == REPLY_BUF {
                                overflowed(s, now);
                                break;
                            }
                        }
                    }
                    NET_MSG_CLOSED if s.phase != Phase::Disconnected => {
                        if plen >= CONN_ID_LEN && conn_id(pl) == s.conn_id {
                            feed(s, sys, Ev::PeerClosed, now);
                        }
                    }
                    NET_MSG_ERROR if plen > CONN_ID_LEN => {
                        // A tagged error is a connect-phase failure and is
                        // ours by tag alone; an untagged one belongs to an
                        // established connection and routes by conn id.
                        let (id, _errno, tag) = error_parts(pl);
                        let ours = if tag == REQUESTER_TAG_NONE {
                            (s.phase == Phase::Connecting)
                                || (s.phase != Phase::Disconnected && id == s.conn_id)
                        } else {
                            s.phase == Phase::Connecting && tag == s.tag
                        };
                        if ours {
                            feed(s, sys, Ev::NetError, now);
                        }
                    }
                    _ => {}
                }
            }
        }

        // 4. Pump the flush buffer (AUTH or command) over the open connection.
        if (s.phase == Phase::Authing || s.phase == Phase::Awaiting) && s.conn_id != 0 {
            let max_chunk = NET_BUF - NET_FRAME_HDR - CONN_ID_LEN;
            while s.req_sent < s.req_len {
                let poll = (sys.channel_poll)(s.net_out, 0x02);
                if poll <= 0 || (poll as u32 & 0x02) == 0 {
                    break;
                }
                let remaining = (s.req_len - s.req_sent) as usize;
                let chunk = if remaining < max_chunk {
                    remaining
                } else {
                    max_chunk
                };
                let total_payload = chunk + CONN_ID_LEN;
                let id = s.conn_id.to_le_bytes();
                s.nbuf[0] = NET_CMD_SEND;
                s.nbuf[1] = (total_payload & 0xff) as u8;
                s.nbuf[2] = (total_payload >> 8) as u8;
                s.nbuf[3] = id[0];
                s.nbuf[4] = id[1];
                core::ptr::copy_nonoverlapping(
                    s.req.as_ptr().add(s.req_sent as usize),
                    s.nbuf.as_mut_ptr().add(NET_FRAME_HDR + CONN_ID_LEN),
                    chunk,
                );
                (sys.channel_write)(s.net_out, s.nbuf.as_ptr(), NET_FRAME_HDR + total_payload);
                s.req_sent += chunk as u16;
            }
        }

        // 5. Timeouts. A command the server has not answered is answered 504
        //    and its connection closed (the next request reconnects); a
        //    connect or AUTH that stalls is a lost session.
        if s.phase == Phase::Awaiting && now.wrapping_sub(s.started_ms) > REPLY_TIMEOUT_MS {
            s.desk.finish(status::TIMEOUT, 0);
            s.has_cmd = 0;
            s.errors = s.errors.wrapping_add(1);
            close_conn(s);
        } else if matches!(s.phase, Phase::Connecting | Phase::Authing) {
            let budget = if s.phase == Phase::Connecting {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                feed(s, sys, Ev::NetError, now);
            }
        }

        flush_answers(s);

        // 6. Drain: once no work remains, close any idle socket and report Done.
        if s.draining == 1
            && s.has_cmd == 0
            && s.desk.idle()
            && !s.outbox.holding()
            && matches!(s.phase, Phase::Disconnected | Phase::Ready)
        {
            close_conn(s);
            return 1; // StepOutcome::Done
        }
        0
    }
}

/// The server sent more than one reply buffer holds.
unsafe fn overflowed(s: &mut RedisState, now: u64) {
    if s.phase == Phase::Awaiting {
        s.desk.finish(status::BAD_GATEWAY, 0);
        s.has_cmd = 0;
        s.errors = s.errors.wrapping_add(1);
        close_conn(s);
    } else {
        connection_lost(s, now, 0);
    }
}
