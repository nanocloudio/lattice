//! sql_front — lattice's relational path as an in-graph exchange provider.
//!
//! The same exchange surface `pg_client` offers, served by the graph's own
//! `relational_executor` instead of a PostgreSQL server across a network: no
//! wire protocol, no TCP, no SCRAM. An application that wires
//! `pg_client.request_in` / `pg_client.response_out` wires
//! `sql_front.request_in` / `sql_front.response_out` instead and changes
//! nothing else — the requests it sends and the answers it reads are the
//! same bytes.
//!
//! # Ports
//!
//! The exchange provider pair `request_in` (`ExchangeRequest`) /
//! `response_out` (`ExchangeResponse`), and the executor pair: `sql_out`
//! carries `MSG_SQL_REQUEST` to `relational_executor.pg_in`, `sql_in` takes
//! `MSG_SQL_RESPONSE` from `relational_executor.pg_out`. A module with the
//! executor pair unwired, or `request_in` wired and `response_out` not,
//! refuses to construct.
//!
//! # Request
//!
//! Method POST; `target` empty; headers ignored; body = the SQL text of one
//! statement (1..=4096 bytes, no NUL byte). The body may arrive inline in
//! the HEAD or in BODY records (the module grants the credit for the rest at
//! once).
//!
//! # Answer
//!
//! One answer per request, no content type, no headers:
//!
//! | Status | When | Body |
//! |---|---|---|
//! | 200 | the statement completed — for a write, it is applied (durable as the graph's store is durable) | the first column of every row as PostgreSQL text, in row order, joined by `\n` (0x0A); no trailing separator; a NULL first column contributes no row; a statement with no rows answers an empty body |
//! | 400 | the executor refused the statement | the message text `pg_edge_anchor` sends for it (`relation does not exist`, `duplicate key value violates primary key`, …) |
//! | 400 | another method, a non-empty target, an empty body, a NUL in the body | empty |
//! | 413 | a body over 4096 bytes, a target over 64, headers over 256 | empty |
//! | 502 | a result set over 8192 bytes (refused whole, never truncated), or an executor answer that does not decode | empty |
//! | 503 | four requests already held, or draining | empty |
//! | 504 | the executor did not answer within 15 s (the statement's effect is unknown) | empty |
//!
//! The body streams under the requester's response credit. Statements are
//! performed one at a time in arrival order, on one executor session, so a
//! `BEGIN` … `COMMIT` sent as successive requests is one session's block — as
//! it is over `pg_client`'s one connection. There is no backend link to lose, so
//! the module writes no LINK record.

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
use wire::{MSG_SQL_REQUEST, MSG_SQL_RESPONSE};

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox, ENVELOPE_HDR};

#[path = "../../common/sql_answer.rs"]
mod sql_answer;
use sql_answer::{sql_exec, Answer, ANSWER_MAX};

#[path = "../../common/query_exchange.rs"]
mod query_exchange;
use query_exchange::exchange::{status, METHOD_POST, RECORD_MAX};
use query_exchange::QueryDesk;

/// Longest SQL text one request carries (`pg_client`'s).
const QUERY_MAX: usize = 4096;
/// Requests held at once: one performed, the rest queued in arrival order.
const SLOTS: usize = 4;
/// A request target is refused unless empty; this bounds what is read.
const TARGET_MAX: usize = 64;
const QUEUE: usize = 8;
/// The executor's answer deadline.
const REPLY_TIMEOUT_MS: u64 = 15_000;
/// The outgoing statement envelope: the request head and the SQL.
const REQ_ENV_MAX: usize = ENVELOPE_HDR + sql_exec::SQL_REQUEST_HEAD + QUERY_MAX;
/// The largest executor answer read whole: its response envelope
/// (`relational_executor`'s `RESP_BUF`) behind the envelope header.
const RESP_ENV_MAX: usize = ENVELOPE_HDR + 28672;
/// The executor session this front's statements run on. One session for
/// every request, as `pg_client` holds one connection; distinct from the
/// slot indices `pg_edge_anchor` and `mysql_edge_anchor` use, so a
/// transaction opened here is never another connector's.
const FRONT_SESSION: u8 = 0xFE;
/// Correlation ids carry this bit, disjoint from the small counters the
/// edge anchors use, so a front sharing an executor with one is never
/// answered with another's statement.
const CORR_BASE: u64 = 0x4000_0000_0000_0000;

#[repr(C)]
struct FrontState {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    sql_in: i32,
    sql_out: i32,
    /// `sql_in`'s envelope reading (`common/envelope.rs`).
    sql_inbox: Inbox,

    draining: u8,
    /// A statement is being performed.
    has_query: u8,
    /// Its envelope is still to be written to `sql_out`.
    pending: u8,
    corr: u64,
    corr_seq: u64,
    started_ms: u64,
    req_len: u16,
    completed: u32,
    errors: u32,

    desk: QueryDesk<SLOTS, TARGET_MAX, QUERY_MAX, QUEUE>,
    outbox: ExchangeOutbox,
    /// The statement envelope being written.
    req: [u8; REQ_ENV_MAX],
    /// The answer body.
    result: [u8; ANSWER_MAX],
    /// One executor answer.
    resp: [u8; RESP_ENV_MAX],
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
        // inputs: request_in[0], sql_in[1]; outputs: response_out[0], sql_out[1]
        s.request_in = in_chan;
        s.response_out = out_chan;
        s.sql_in = dev_channel_port(sys, 0, 1);
        s.sql_out = dev_channel_port(sys, 1, 1);
        s.sql_inbox = Inbox::new();
        s.draining = 0;
        s.has_query = 0;
        s.pending = 0;
        s.corr = 0;
        s.corr_seq = 0;
        s.started_ms = 0;
        s.req_len = 0;
        s.completed = 0;
        s.errors = 0;
        s.desk.reset(METHOD_POST, 0);
        s.outbox = ExchangeOutbox::new();
        if s.sql_in < 0 || s.sql_out < 0 {
            let m = b"[sql_front] refusing to construct: sql_in and sql_out must both be wired to relational_executor";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if s.request_in >= 0 && s.response_out < 0 {
            let m =
                b"[sql_front] refusing to construct: request_in is wired but response_out is not";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        dev_log(sys, 3, b"[sql_front] init".as_ptr(), 16);
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

/// Whether `b` holds a NUL. A plain scan: the library's search lowers to a
/// `memchr` a PIC module cannot link.
fn has_nul(b: &[u8]) -> bool {
    let mut i = 0usize;
    while i < b.len() {
        if b[i] == 0 {
            return true;
        }
        i += 1;
    }
    false
}

/// Take the oldest complete request, when none is being performed: its
/// statement becomes the envelope owed to the executor.
unsafe fn take_request(s: &mut FrontState, now: u64) {
    if s.has_query == 1 {
        return;
    }
    let Some(at) = s.desk.next_request() else {
        return;
    };
    s.corr_seq = s.corr_seq.wrapping_add(1) & !CORR_BASE;
    let corr = CORR_BASE | s.corr_seq.max(1);
    let encoded = {
        let Some(r) = s.desk.request(at) else {
            return;
        };
        if !r.target.is_empty() || r.body.is_empty() || has_nul(r.body) {
            None
        } else {
            sql_exec::encode_sql_request(
                &mut s.req[ENVELOPE_HDR..],
                corr,
                sql_exec::DIALECT_POSTGRES,
                0,
                FRONT_SESSION,
                r.body,
            )
        }
    };
    let Some(n) = encoded else {
        s.desk.refuse(at, status::BAD_REQUEST);
        return;
    };
    s.req[0] = MSG_SQL_REQUEST;
    s.req[1..3].copy_from_slice(&(n as u16).to_le_bytes());
    s.req_len = (ENVELOPE_HDR + n) as u16;
    s.corr = corr;
    s.has_query = 1;
    s.pending = 1;
    s.started_ms = now;
    s.desk.start(at);
}

/// Write the statement envelope to the executor; a full channel keeps it
/// owed for the next step.
unsafe fn send_statement(s: &mut FrontState) {
    if s.pending == 0 {
        return;
    }
    let sys = &*s.syscalls;
    let len = s.req_len as usize;
    if (sys.channel_write)(s.sql_out, s.req.as_ptr(), len) == len as i32 {
        s.pending = 0;
    }
}

/// Read the executor's answers; the one for the statement being performed
/// answers its request, any other (a statement abandoned at its deadline)
/// is dropped.
unsafe fn read_answers(s: &mut FrontState) {
    let sys = &*s.syscalls;
    while let Some((mt, len)) = read_envelope(&mut s.sql_inbox, sys, s.sql_in, &mut s.resp) {
        if mt != MSG_SQL_RESPONSE || s.has_query == 0 || s.pending == 1 {
            continue;
        }
        let Some(h) = sql_exec::decode_response_header(&s.resp[..len]) else {
            continue;
        };
        if h.corr_id != s.corr {
            continue;
        }
        let (code, n) = match sql_answer::answer(&s.resp[..len], &mut s.result) {
            Answer::Rows(n) => (status::OK, n),
            Answer::Refused(n) => (status::BAD_REQUEST, n),
            Answer::TooLarge | Answer::Undecodable => {
                s.errors = s.errors.wrapping_add(1);
                (status::BAD_GATEWAY, 0)
            }
        };
        s.desk.finish(code, n);
        s.has_query = 0;
        s.completed = s.completed.wrapping_add(1);
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

        // 2. Requests in, the next statement out, the executor's answer back.
        read_requests(s);
        take_request(s, now);
        send_statement(s);
        read_answers(s);

        // 3. A statement the executor has not answered in time is answered
        //    504; its late answer no longer matches and is dropped.
        if s.has_query == 1 && now.wrapping_sub(s.started_ms) > REPLY_TIMEOUT_MS {
            s.desk.finish(status::TIMEOUT, 0);
            s.has_query = 0;
            s.pending = 0;
            s.errors = s.errors.wrapping_add(1);
        }

        flush_answers(s);

        // 4. Drain: once nothing is held or owed, report Done.
        if s.draining == 1 && s.has_query == 0 && s.desk.idle() && !s.outbox.holding() {
            return 1;
        }
        0
    }
}
