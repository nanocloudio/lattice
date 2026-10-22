//! kv_front — lattice's KV path as an in-graph exchange provider, spoken the
//! way `redis_client` speaks it.
//!
//! The same exchange surface `redis_client` offers, served by the graph's own
//! `kv_request_router` instead of a Redis server across a network: no RESP on
//! a socket, no TCP, no AUTH. An application that wires
//! `redis_client.request_in` / `redis_client.response_out` wires
//! `kv_front.request_in` / `kv_front.response_out` instead and changes nothing
//! else. The command handling and reply encoding are `redis_edge_anchor`'s
//! (`common/redis_codec.rs`) and the answer mapping is `redis_client`'s
//! (`common/redis_core.rs`); `common/kv_front_core.rs` joins them.
//!
//! # Ports
//!
//! The exchange provider pair `request_in` (`ExchangeRequest`) /
//! `response_out` (`ExchangeResponse`), and a router anchor pair: `kv_out`
//! carries `MSG_KV_REQUEST` stamped `PROTO_REDIS` to
//! `kv_request_router.redis_in`, `kv_in` takes `MSG_KV_RESPONSE` from
//! `kv_request_router.redis_out`. A module with the router pair unwired, or
//! `request_in` wired and `response_out` not, refuses to construct.
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
//! Body up to 4096 bytes. A value past one replicated entry goes to the
//! store as chunks under a manifest, as the Redis connector sends one.
//!
//! # Answer
//!
//! One answer per request, no content type, no headers:
//!
//! | Status | Reply | Body |
//! |---|---|---|
//! | 200 | `+OK` simple string | the text, without `+` and CRLF (`OK`) |
//! | 200 | `:N` integer | the decimal digits (`42`, `-1`) |
//! | 200 | `$n` bulk string | the `n` data bytes |
//! | 200 | `$-1` nil | empty |
//! | 200 | `*N` array | the whole RESP reply, header included, verbatim |
//! | 400 | `-ERR …` error | the error text, without `-` and CRLF (`ERR unknown command 'NOPE'`) |
//! | 400 | — | another method, a non-empty target, a body that is not exactly one structured command, a command of no arguments |
//! | 413 | — | a body over 4096 bytes, or a command whose RESP form exceeds 8192 bytes |
//! | 502 | — | a reply over 8192 bytes |
//! | 503 | — | four requests already held, or draining |
//! | 504 | — | no answer from the store within 15 s (the command's effect is unknown) |
//!
//! `PING`, `ECHO` and `COMMAND` are answered by the front, as the connector
//! answers them. A command whose meaning is a connection's state (`AUTH`,
//! `HELLO`, `SELECT`, `CLIENT`, `QUIT`, `RESET`, `MULTI`, `EXEC`, `DISCARD`,
//! `WATCH`, `UNWATCH`, the pub/sub commands) is answered 400
//! `ERR '<name>' needs a connection; the KV front serves one command per
//! exchange`. Commands are performed one at a time in arrival order. There is
//! no backend link to lose, so the module writes no LINK record.

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

#[path = "../../common/wire.rs"]
mod wire;
use wire::{FRAME_PAYLOAD_MAX, MSG_KV_REQUEST, MSG_KV_RESPONSE};

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox, ENVELOPE_HDR};

#[path = "../../common/kv_front_core.rs"]
mod kv_front_core;
use kv_front_core::kv_large::{self, Act, LargeError, Session};
use kv_front_core::types::{
    KV_RESULT_INTERNAL, KV_RESULT_TOO_LARGE, PROTO_REDIS, REQ_SERIALIZABLE,
};
use kv_front_core::{
    Answer, Plan, COMMAND_MAX, OP_BODY_MAX, REPLY_MAX, REPLY_RENDER_MAX, RESP_COMMAND_MAX,
};

#[path = "../../common/query_exchange.rs"]
mod query_exchange;
use query_exchange::exchange::{status, METHOD_POST, RECORD_MAX};
use query_exchange::QueryDesk;

/// Requests held at once: one performed, the rest queued in arrival order.
const SLOTS: usize = 4;
/// A request target is refused unless empty; this bounds what is read.
const TARGET_MAX: usize = 64;
const QUEUE: usize = 8;
/// The store's answer deadline for one command, its chunks included.
const REPLY_TIMEOUT_MS: u64 = 15_000;
/// The request as it goes out (reads marked, large values rewritten), and
/// each exchange of a large value's layer (a chunk, a fetch).
const WORK_MAX: usize = kv_large::mark_room(OP_BODY_MAX);
/// A large value's answer, expanded: the reply bound, so one past it is
/// answered as too large rather than cut.
const EXPAND_MAX: usize = REPLY_MAX;
/// The `MSG_KV_REQUEST` head: corr, protocol, tenant, conn, consistency,
/// op, body length.
const KV_REQ_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
/// The `MSG_KV_RESPONSE` head: corr, conn, result, revision, body length.
const KV_RESP_HEAD: usize = 8 + 1 + 1 + 8 + 2;
/// One outgoing KV request envelope.
const KV_ENV_MAX: usize = ENVELOPE_HDR + KV_REQ_HEAD + WORK_MAX;
/// One incoming KV response, whole.
const KV_IN_MAX: usize = ENVELOPE_HDR + FRAME_PAYLOAD_MAX;

#[repr(C)]
struct FrontState {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    kv_in: i32,
    kv_out: i32,
    /// `kv_in`'s envelope reading (`common/envelope.rs`).
    kv_inbox: Inbox,

    draining: u8,
    /// A command is being performed.
    has_cmd: u8,
    /// Its current exchange's envelope is still to be written to `kv_out`.
    pending: u8,
    consistency: u8,
    corr: u64,
    corr_seq: u64,
    started_ms: u64,
    body_len: u16,
    env_len: u16,
    completed: u32,
    errors: u32,

    desk: QueryDesk<SLOTS, TARGET_MAX, COMMAND_MAX, QUEUE>,
    outbox: ExchangeOutbox,
    session: Session,
    /// The command's RESP form.
    resp: [u8; RESP_COMMAND_MAX],
    /// The typed operation body, held while the command is performed.
    body: [u8; OP_BODY_MAX],
    work: [u8; WORK_MAX],
    send: [u8; WORK_MAX],
    expanded: [u8; EXPAND_MAX],
    /// The RESP reply.
    reply: [u8; REPLY_RENDER_MAX],
    /// The answer body.
    result: [u8; REPLY_MAX],
    env: [u8; KV_ENV_MAX],
    kv_buf: [u8; KV_IN_MAX],
    // The request record being read, and the response record being written.
    rec: [u8; RECORD_MAX],
    out: [u8; RECORD_MAX],
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<FrontState>() as u32
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
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<FrontState>() {
            return -2;
        }
        let s = &mut *(state as *mut FrontState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        // inputs: request_in[0], kv_in[1]; outputs: response_out[0], kv_out[1]
        s.request_in = in_chan;
        s.response_out = out_chan;
        s.kv_in = dev_channel_port(sys, 0, 1);
        s.kv_out = dev_channel_port(sys, 1, 1);
        s.kv_inbox = Inbox::new();
        s.draining = 0;
        s.has_cmd = 0;
        s.pending = 0;
        s.consistency = 0;
        s.corr = 0;
        s.corr_seq = 0;
        s.started_ms = 0;
        s.body_len = 0;
        s.env_len = 0;
        s.completed = 0;
        s.errors = 0;
        s.desk.reset(METHOD_POST, 0);
        s.outbox = ExchangeOutbox::new();
        s.session = Session::new();
        if s.kv_in < 0 || s.kv_out < 0 {
            let m = b"[kv_front] refusing to construct: kv_in and kv_out must both be wired to kv_request_router";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if s.request_in >= 0 && s.response_out < 0 {
            let m =
                b"[kv_front] refusing to construct: request_in is wired but response_out is not";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        dev_log(sys, 3, b"[kv_front] init".as_ptr(), 15);
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_drain"]
pub extern "C" fn module_drain(state: *mut u8) -> i32 {
    unsafe {
        (*(state as *mut FrontState)).draining = 1;
        0
    }
}

/// Place owed response records until the port is full or nothing is owed.
unsafe fn flush_answers(s: &mut FrontState) {
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

/// Read request records while the desk can take them.
unsafe fn read_requests(s: &mut FrontState) {
    let sys = &*s.syscalls;
    if s.request_in < 0 {
        return;
    }
    while s.desk.can_take() {
        let poll = (sys.channel_poll)(s.request_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.request_in, s.rec.as_mut_ptr(), RECORD_MAX);
        if n <= 0 {
            return;
        }
        s.desk.accept(&s.rec[..n as usize]);
    }
}

/// Answer the command being performed with the RESP reply in
/// `reply[..len]`, by `redis_client`'s mapping.
fn answer_reply(s: &mut FrontState, len: usize) {
    let (code, start, end) = match kv_front_core::answer(&s.reply[..len]) {
        Answer::Ok { start, end } => (status::OK, start, end),
        Answer::Refused { start, end } => (status::BAD_REQUEST, start, end),
        Answer::TooLarge => {
            s.errors = s.errors.wrapping_add(1);
            (status::BAD_GATEWAY, 0, 0)
        }
    };
    let n = end - start;
    {
        let (reply, result) = (&s.reply, &mut s.result);
        result[..n].copy_from_slice(&reply[start..end]);
    }
    s.desk.finish(code, n);
    s.has_cmd = 0;
    s.pending = 0;
    s.completed = s.completed.wrapping_add(1);
}

/// Answer the command being performed with the store's answer.
fn answer_result(s: &mut FrontState, result: u8, body_at: usize, body_len: usize, expanded: bool) {
    let len = {
        let src = if expanded {
            &s.expanded[..body_len]
        } else {
            &s.kv_buf[body_at..body_at + body_len]
        };
        kv_front_core::render(result, src, &mut s.reply)
    };
    answer_reply(s, len);
}

/// Stage the next exchange of the command (`op` over `send[..n]`) as the
/// envelope owed to the router.
fn stage(s: &mut FrontState, op: u8, n: usize) {
    let chunk = op == kv_large::UPLOAD_OP || op == kv_large::FETCH_OP;
    let consistency = if chunk {
        REQ_SERIALIZABLE
    } else {
        s.consistency
    };
    s.corr_seq = s.corr_seq.wrapping_add(1);
    if s.corr_seq & 0x00FF_FFFF_FFFF_FFFF == 0 {
        s.corr_seq = 1;
    }
    // The Redis connector's namespace: the router's inflight table is keyed
    // by the whole id, and only one of the two holds the router's Redis pair.
    let corr = (u64::from(PROTO_REDIS) << 56) | (s.corr_seq & 0x00FF_FFFF_FFFF_FFFF);
    let payload = KV_REQ_HEAD + n;
    let e = &mut s.env;
    e[0] = MSG_KV_REQUEST;
    e[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
    e[3..11].copy_from_slice(&corr.to_le_bytes());
    e[11] = PROTO_REDIS;
    e[12..16].copy_from_slice(&0u32.to_le_bytes());
    e[16] = 0;
    e[17] = consistency;
    e[18] = op;
    e[19..21].copy_from_slice(&(n as u16).to_le_bytes());
    e[ENVELOPE_HDR + KV_REQ_HEAD..ENVELOPE_HDR + payload].copy_from_slice(&s.send[..n]);
    s.env_len = (ENVELOPE_HDR + payload) as u16;
    s.corr = corr;
    s.pending = 1;
}

/// Carry out what the large-value session asks next.
fn act(s: &mut FrontState, act: Result<Act, LargeError>, as_is: Option<(u8, usize, usize)>) {
    match act {
        Ok(Act::Send(op, n)) => stage(s, op, n),
        Ok(Act::Done(result, n)) => answer_result(s, result, 0, n.min(EXPAND_MAX), true),
        Ok(Act::AsIs) => match as_is {
            Some((result, at, len)) => answer_result(s, result, at, len, false),
            None => answer_result(s, KV_RESULT_INTERNAL, 0, 0, true),
        },
        Err(LargeError::Overflow) => answer_result(s, KV_RESULT_TOO_LARGE, 0, 0, true),
        Err(_) => answer_result(s, KV_RESULT_INTERNAL, 0, 0, true),
    }
}

/// Take the oldest complete request, when none is being performed.
unsafe fn take_request(s: &mut FrontState, now: u64) {
    if s.has_cmd == 1 {
        return;
    }
    let Some(at) = s.desk.next_request() else {
        return;
    };
    let planned = {
        let Some(r) = s.desk.request(at) else {
            return;
        };
        if !r.target.is_empty() {
            Plan::BadRequest
        } else {
            kv_front_core::plan(r.body, &mut s.resp, &mut s.body, &mut s.reply)
        }
    };
    match planned {
        Plan::BadRequest => s.desk.refuse(at, status::BAD_REQUEST),
        Plan::TooLarge => s.desk.refuse(at, status::TOO_LARGE),
        Plan::Reply(len) => {
            s.desk.start(at);
            s.has_cmd = 1;
            answer_reply(s, len);
        }
        Plan::Forward {
            op,
            consistency,
            len,
        } => {
            s.desk.start(at);
            s.has_cmd = 1;
            s.started_ms = now;
            s.consistency = consistency;
            s.body_len = len as u16;
            s.session.reset();
            let begun = {
                let FrontState {
                    session,
                    body,
                    work,
                    send,
                    ..
                } = &mut *s;
                session.begin(op, &body[..len], work, send)
            };
            act(s, begun, None);
        }
    }
}

/// Write the owed envelope to the router; a full channel keeps it owed.
unsafe fn send_exchange(s: &mut FrontState) {
    if s.pending == 0 {
        return;
    }
    let sys = &*s.syscalls;
    let len = s.env_len as usize;
    if (sys.channel_write)(s.kv_out, s.env.as_ptr(), len) == len as i32 {
        s.pending = 0;
    }
}

/// Read the router's answers; the one for the exchange in flight moves the
/// command on, any other (an exchange abandoned at its deadline) is dropped.
unsafe fn read_answers(s: &mut FrontState) {
    let sys = &*s.syscalls;
    while let Some((mt, len)) = read_envelope(&mut s.kv_inbox, sys, s.kv_in, &mut s.kv_buf) {
        if mt != MSG_KV_RESPONSE || len < KV_RESP_HEAD || s.has_cmd == 0 || s.pending == 1 {
            continue;
        }
        let corr = u64::from_le_bytes(s.kv_buf[0..8].try_into().unwrap_or([0; 8]));
        if corr != s.corr {
            continue;
        }
        let result = s.kv_buf[9];
        let body_len = u16::from_le_bytes([s.kv_buf[18], s.kv_buf[19]]) as usize;
        if KV_RESP_HEAD + body_len > len {
            continue;
        }
        let next = {
            let FrontState {
                session,
                body,
                work,
                send,
                expanded,
                kv_buf,
                body_len: held,
                ..
            } = &mut *s;
            session.reply(
                &body[..*held as usize],
                work,
                result,
                &kv_buf[KV_RESP_HEAD..KV_RESP_HEAD + body_len],
                send,
                expanded,
            )
        };
        act(s, next, Some((result, KV_RESP_HEAD, body_len)));
        return;
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut FrontState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. Answers first: an owed record leaves before more work starts.
        flush_answers(s);
        if s.draining == 1 {
            s.desk.drain();
        }

        // 2. Requests in, the next exchange out, the router's answer back.
        read_requests(s);
        take_request(s, now);
        send_exchange(s);
        read_answers(s);
        send_exchange(s);

        // 3. A command the store has not answered in time is answered 504;
        //    a late answer no longer matches and is dropped.
        if s.has_cmd == 1 && now.wrapping_sub(s.started_ms) > REPLY_TIMEOUT_MS {
            s.desk.finish(status::TIMEOUT, 0);
            s.session.reset();
            s.has_cmd = 0;
            s.pending = 0;
            s.errors = s.errors.wrapping_add(1);
        }

        flush_answers(s);

        // 4. Drain: once nothing is held or owed, report Done.
        if s.draining == 1 && s.has_cmd == 0 && s.desk.idle() && !s.outbox.holding() {
            return 1;
        }
        0
    }
}
