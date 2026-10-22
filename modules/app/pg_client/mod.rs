//! Postgres connector — a GENUINE per-protocol Fluxor foundation module, and the
//! sharpest proof of the codec-vs-protocol line: it performs the SCRAM-SHA-256
//! authentication handshake. That is a multi-round-trip exchange in which the
//! client proof depends on the server's *random* salt and nonce and requires
//! PBKDF2 (thousands of HMAC iterations) plus an HMAC/XOR chain — reply-dependent
//! cryptographic computation a stateless encode/decode bytecode program cannot
//! express. So this MUST be a compiled module.
//!
//! The crypto lives in the host-tested `scram_core.rs` (verified against RFC 7677
//! vectors); the wire framing in `pg_core.rs`; the exchange provider half in
//! `query_exchange.rs`; this file is the connection state machine that drives
//! startup -> SASL(SCRAM) -> ready -> simple query.
//!
//! # Ports
//!
//! `net_in`/`net_out` (transport), and the exchange provider pair
//! `request_in` (`ExchangeRequest`) / `response_out` (`ExchangeResponse`).
//! A module with `request_in` wired and `response_out` unwired refuses to
//! construct: its answers would have nowhere to go.
//!
//! # Params
//!
//! `authority` (`host[:port]`, port 5432 when omitted), `user`, `database`,
//! `password`.
//!
//! # Request
//!
//! Method POST; `target` empty; headers ignored; body = the SQL text of one
//! simple query (1..=4096 bytes, no NUL byte), sent to the server verbatim.
//! The body may arrive inline in the HEAD or in BODY records (the module
//! grants the credit for the rest at once).
//!
//! # Answer
//!
//! One answer per request, no content type, no headers:
//!
//! | Status | When | Body |
//! |---|---|---|
//! | 200 | the query completed — for a write, the server has committed it (the answer follows ReadyForQuery) | the first column of every row, in row order, joined by `\n` (0x0A); no trailing separator; a NULL first column contributes no row; a statement with no rows answers an empty body |
//! | 400 | the server refused the query (ErrorResponse) | the server's message text (the `M` field), UTF-8 as sent |
//! | 400 | another method, a non-empty target, an empty body, a NUL in the body | empty |
//! | 413 | a body over 4096 bytes, a target over 64, headers over 256 | empty |
//! | 502 | the server could not be reached or refused the session before the link was ever up (the server's message text when it sent one), or a result set over 8192 bytes (refused whole, never truncated) | as stated |
//! | 503 | four requests already held, or draining | empty |
//! | 504 | the server did not finish the query within 15 s (the connection is closed; the query's effect is unknown) | empty |
//!
//! The body streams under the requester's response credit: the HEAD carries
//! as much as `resp_credit` allows, BODY records the rest as CREDIT arrives.
//!
//! The same requests and answers are served in-graph by `sql_front` over
//! lattice's own relational executor (`docs/guides/fronts.md`): an
//! application swaps a foreign PostgreSQL for the embedded store by wiring
//! alone. The front writes no LINK record (it has no connection to lose) and
//! answers the executor's refusals with the message lattice's PostgreSQL
//! connector sends.
//!
//! # LINK
//!
//! The connection opens on the first request. Once it has reached
//! ReadyForQuery, losing it (a close, a transport error, a failed lazy
//! reconnect) writes LINK DOWN: every request held is dropped unanswered for
//! the requester to re-issue, and request records are read and dropped until
//! LINK UP. The module reconnects on its own (1 s, doubling to 30 s) and
//! writes LINK UP once the new session is ready.

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

// Shared, host-tested cores.
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/b64.rs");
include!("../../common/scram_core.rs");
include!("../../common/pg_core.rs");
include!("../../common/pg_reply.rs");

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox};

#[path = "../../common/authority.rs"]
mod authority;
use authority::{Authority, PORT_PG};

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
/// Longest SQL text one request carries.
const QUERY_MAX: usize = 4096;
/// Outgoing message buffer: the Query message is the SQL plus 6 bytes.
const REQ_BUF: usize = 4160;
/// Server bytes accumulated toward complete messages.
const ACC_BUF: usize = 16384;
/// Largest answer body: a result set, or an error's message text.
const RESULT_MAX: usize = 8192;
const NAME_BUF: usize = 128;
/// Requests held at once: one performed, the rest queued in arrival order.
const SLOTS: usize = 4;
/// A request target is refused unless empty; this bounds what is read.
const TARGET_MAX: usize = 64;
const QUEUE: usize = 8;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;
const PROBE_MIN_MS: u64 = 1_000;
const PROBE_MAX_MS: u64 = 30_000;

// Connection phases.
const DISCONNECTED: u8 = 0;
const CONNECTING: u8 = 1;
const STARTUP: u8 = 2; // startup sent, awaiting AuthenticationSASL/Ok
const SASL_CONT: u8 = 3; // SASLInitial sent, awaiting server-first (R/11)
const SASL_FINAL: u8 = 4; // SASLResponse sent, awaiting server-sig (R/12) + Ok
const AUTH_OK: u8 = 5; // authenticated, awaiting ReadyForQuery (Z)
const READY: u8 = 6; // idle, connection open — reuse it
const QUERYING: u8 = 7; // query sent, collecting rows until Z

#[repr(C)]
struct PgState {
    syscalls: *const SyscallTable,
    net_in: i32,
    /// The transport input's envelope reading (`common/envelope.rs`).
    net_inbox: Inbox,
    net_out: i32,
    request_in: i32,
    response_out: i32,

    authority: Authority,
    user: [u8; NAME_BUF],
    user_len: u16,
    database: [u8; NAME_BUF],
    database_len: u16,
    password: [u8; NAME_BUF],
    password_len: u16,

    phase: u8,
    conn_id: u16,
    tag: RequesterTag,
    started_ms: u64,
    draining: u8,
    nonce_ctr: u32,

    // Reconnect after a LINK DOWN: the next attempt and the current wait.
    probing: u8,
    probe_at_ms: u64,
    backoff_ms: u64,

    // SCRAM: the client-first-bare (for AuthMessage) + expected server signature.
    cfirst: [u8; 128],
    cfirst_len: u16,
    server_sig: [u8; 32],

    // The SQL query being performed.
    query: [u8; QUERY_MAX],
    query_len: u16,
    has_query: u8,
    // The answer body: the result set (first column of every row,
    // newline-joined), or the server's error text.
    result: [u8; RESULT_MAX],
    result_len: u16,
    have_result: u8,
    // Set when a row would overflow `result`: the whole answer is refused
    // (502) rather than truncated to a plausible-looking prefix.
    result_overflow: u8,
    // Set when the server refused the query; `result` holds its message.
    query_error: u8,

    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,
    // Accumulated server bytes (may hold several messages), and one
    // message body copied out of it for dispatch.
    acc: [u8; ACC_BUF],
    acc_len: u32,
    msg: [u8; ACC_BUF],

    nbuf: [u8; NET_BUF],
    completed: u32,
    errors: u32,

    desk: QueryDesk<SLOTS, TARGET_MAX, QUERY_MAX, QUEUE>,
    outbox: ExchangeOutbox,
    // The request record being read, and the response record being written.
    rec: [u8; RECORD_MAX],
    out: [u8; RECORD_MAX],
}

define_params! {
    PgState;

    // Tags 1 and 5 are retired.
    // The peer, `host[:port]`; the port defaults to 5432.
    6, authority, str, 0 => |s, d, len| {
        if len > 0 {
            s.authority.set(core::slice::from_raw_parts(d, len));
        }
    };
    2, user, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.user_len as usize) < NAME_BUF {
            s.user[s.user_len as usize] = *d.add(i); s.user_len += 1; i += 1;
        }
    };
    3, database, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.database_len as usize) < NAME_BUF {
            s.database[s.database_len as usize] = *d.add(i); s.database_len += 1; i += 1;
        }
    };
    4, password, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.password_len as usize) < NAME_BUF {
            s.password[s.password_len as usize] = *d.add(i); s.password_len += 1; i += 1;
        }
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<PgState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_drain"]
pub extern "C" fn module_drain(state: *mut u8) -> i32 {
    unsafe {
        (*(state as *mut PgState)).draining = 1;
        0
    }
}

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
        if state_size < core::mem::size_of::<PgState>() {
            return -2;
        }
        let s = &mut *(state as *mut PgState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_inbox = Inbox::new();
        s.net_out = out_chan;
        s.request_in = dev_channel_port(sys, 0, 1);
        s.response_out = dev_channel_port(sys, 1, 1);
        s.authority.clear();
        s.user_len = 0;
        s.database_len = 0;
        s.password_len = 0;
        s.phase = DISCONNECTED;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.started_ms = 0;
        s.draining = 0;
        s.nonce_ctr = 0;
        s.probing = 0;
        s.probe_at_ms = 0;
        s.backoff_ms = 0;
        s.cfirst_len = 0;
        s.query_len = 0;
        s.has_query = 0;
        s.result_len = 0;
        s.have_result = 0;
        s.result_overflow = 0;
        s.query_error = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.acc_len = 0;
        s.completed = 0;
        s.errors = 0;
        s.desk.reset(METHOD_POST, 0);
        s.outbox = ExchangeOutbox::new();
        parse_tlv(s, params, params_len);
        if !s.authority.adopt(PORT_PG) {
            let m = b"[pg] refusing to construct: authority (host[:port]) is required";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if s.request_in >= 0 && s.response_out < 0 {
            let m = b"[pg] refusing to construct: request_in is wired but response_out is not";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        dev_log(sys, 3, b"[pg] init".as_ptr(), 9);
        0
    }
}

/// A SCRAM nonce of printable characters, derived from time/tag/counter. Unique
/// per handshake (which is all SCRAM requires of the client nonce).
fn gen_nonce(seed: u64, out: &mut [u8]) -> usize {
    const CS: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut x = seed | 1;
    let n = out.len().min(20);
    for slot in out.iter_mut().take(n) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *slot = CS[(x % 62) as usize];
    }
    n
}

/// Queue `bytes` as the outgoing message and reset the send pump.
unsafe fn stage_send(s: &mut PgState, bytes_len: usize, now: u64) {
    s.req_len = bytes_len as u16;
    s.req_sent = 0;
    s.started_ms = now;
}

/// Send the StartupMessage.
unsafe fn send_startup(s: &mut PgState, now: u64) {
    let user = {
        let mut u = [0u8; NAME_BUF];
        let n = s.user_len as usize;
        u[..n].copy_from_slice(&s.user[..n]);
        (u, n)
    };
    let db = {
        let mut d = [0u8; NAME_BUF];
        let n = s.database_len as usize;
        d[..n].copy_from_slice(&s.database[..n]);
        (d, n)
    };
    if let Some(n) = pg_startup(&user.0[..user.1], &db.0[..db.1], &mut s.req) {
        stage_send(s, n, now);
        s.phase = STARTUP;
    } else {
        connection_lost(s, now, 0);
    }
}

/// Close the connection (if one is open) and return to DISCONNECTED.
unsafe fn close_conn(s: &mut PgState) {
    let sys = &*s.syscalls;
    if s.conn_id != 0 {
        let close = s.conn_id.to_le_bytes();
        net_write_frame(
            sys,
            s.net_out,
            NET_CMD_CLOSE,
            close.as_ptr(),
            2,
            s.nbuf.as_mut_ptr(),
            NET_BUF,
        );
    }
    s.conn_id = 0;
    s.phase = DISCONNECTED;
    s.acc_len = 0;
    s.req_len = 0;
    s.req_sent = 0;
}

/// The connection failed or dropped. After the link was up this is a LINK
/// DOWN (every request held is dropped for the requester to re-issue) and
/// reconnecting is the module's own business; before it ever came up there
/// was nothing to report, so the request being performed is answered 502
/// here, with the first `text_len` bytes of `result` (the server's message,
/// when it sent one) as the body.
unsafe fn connection_lost(s: &mut PgState, now: u64, text_len: usize) {
    close_conn(s);
    s.errors = s.errors.wrapping_add(1);
    if s.probing == 1 {
        s.probe_at_ms = now.wrapping_add(s.backoff_ms);
        s.backoff_ms = (s.backoff_ms * 2).min(PROBE_MAX_MS);
        return;
    }
    if s.desk.link_down() {
        let sys = &*s.syscalls;
        dev_log(sys, 2, b"[pg] link down".as_ptr(), 14);
        s.has_query = 0;
        s.probing = 1;
        s.probe_at_ms = now.wrapping_add(PROBE_MIN_MS);
        s.backoff_ms = (PROBE_MIN_MS * 2).min(PROBE_MAX_MS);
        return;
    }
    if s.has_query == 1 {
        s.desk.finish(status::BAD_GATEWAY, text_len.min(RESULT_MAX));
        s.has_query = 0;
    }
}

/// The session reached ReadyForQuery after the handshake: the link is up.
unsafe fn session_ready(s: &mut PgState) {
    if s.probing == 1 {
        let sys = &*s.syscalls;
        dev_log(sys, 3, b"[pg] link up".as_ptr(), 12);
    }
    s.probing = 0;
    s.backoff_ms = 0;
    s.desk.link_up();
}

/// Handle one framed server message (its body in `s.msg[..len]`); returns
/// false on a handshake failure, with the server's message (if any) left in
/// `result` and its length returned through `text_len`.
unsafe fn on_message(s: &mut PgState, tag: u8, len: usize, now: u64, text_len: &mut usize) -> bool {
    if tag == b'E' {
        let span = pg_error_message(&s.msg[..len]);
        if s.phase == QUERYING {
            // A statement-level error (duplicate key, missing relation, …)
            // is an ordinary query outcome: the server stays open and
            // ReadyForQuery follows, which completes the exchange with 400
            // and the server's message as the body.
            if s.query_error == 0 {
                s.query_error = 1;
                s.result_len = match span {
                    Some((a, b)) => {
                        let n = (b - a).min(RESULT_MAX);
                        let (msg, result) = (&s.msg, &mut s.result);
                        result[..n].copy_from_slice(&msg[a..a + n]);
                        n as u16
                    }
                    None => 0,
                };
            }
            s.errors = s.errors.wrapping_add(1);
            return true;
        }
        // An ErrorResponse during startup/auth ends the session.
        *text_len = match span {
            Some((a, b)) => {
                let n = (b - a).min(RESULT_MAX);
                let (msg, result) = (&s.msg, &mut s.result);
                result[..n].copy_from_slice(&msg[a..a + n]);
                n
            }
            None => 0,
        };
        return false;
    }
    match s.phase {
        STARTUP if tag == b'R' => match pg_auth_type(&s.msg[..len]) {
            Some(10) if pg_sasl_has_scram_sha256(&s.msg[..len]) => {
                // Build client-first with a fresh nonce, send SASLInitialResponse.
                s.nonce_ctr = s.nonce_ctr.wrapping_add(1);
                let seed = now ^ ((s.tag as u64) << 32) ^ (s.nonce_ctr as u64);
                let mut nonce = [0u8; 20];
                let nn = gen_nonce(seed, &mut nonce);
                let mut cf = [0u8; 128];
                let user_n = s.user_len as usize;
                if let Some(cn) = scram_client_first(&s.user[..user_n], &nonce[..nn], &mut cf) {
                    s.cfirst[..cn].copy_from_slice(&cf[..cn]);
                    s.cfirst_len = cn as u16;
                    if let Some(n) = pg_sasl_initial(b"SCRAM-SHA-256", &cf[..cn], &mut s.req) {
                        stage_send(s, n, now);
                        s.phase = SASL_CONT;
                        return true;
                    }
                }
                false
            }
            Some(0) => {
                s.phase = AUTH_OK; // trust auth — no handshake
                true
            }
            _ => false,
        },
        SASL_CONT if tag == b'R' => {
            if pg_auth_type(&s.msg[..len]) == Some(11) {
                let bare_len = s.cfirst_len as usize;
                let mut bare = [0u8; 128];
                bare[..bare_len].copy_from_slice(&s.cfirst[..bare_len]);
                let mut cfinal = [0u8; 256];
                let mut ssig = [0u8; 32];
                // client-first-bare is cfirst[3..] (after "n,,").
                if let Some(fnl) = scram_build_client_final(
                    &bare[3..bare_len],
                    &s.msg[4..len], // server-first, after the auth type
                    &s.password[..s.password_len as usize],
                    &mut cfinal,
                    &mut ssig,
                ) {
                    s.server_sig = ssig;
                    if let Some(n) = pg_sasl_response(&cfinal[..fnl], &mut s.req) {
                        stage_send(s, n, now);
                        s.phase = SASL_FINAL;
                        return true;
                    }
                }
                return false;
            }
            true
        }
        SASL_FINAL if tag == b'R' => {
            match pg_auth_type(&s.msg[..len]) {
                Some(12) => true, // SASLFinal (server signature); accepted
                Some(0) => {
                    s.phase = AUTH_OK;
                    true
                }
                _ => true,
            }
        }
        _ => {
            // After auth: a ReadyForQuery moves us to READY; DataRows during a
            // query are captured; the closing ReadyForQuery answers.
            if tag == b'Z' {
                let was = s.phase;
                if was == QUERYING {
                    finish_query(s);
                }
                s.phase = READY;
                if was != QUERYING && was != READY {
                    session_ready(s);
                }
            } else if s.phase == QUERYING
                && tag == b'D'
                && s.result_overflow == 0
                && s.query_error == 0
            {
                if let Some((cs, ce)) = pg_datarow_col0(&s.msg[..len]) {
                    let (msg, result) = (&s.msg, &mut s.result);
                    match pg_reply_append(
                        result,
                        s.result_len as usize,
                        s.have_result == 1,
                        &msg[cs..ce],
                    ) {
                        Some(n) => {
                            s.result_len = n as u16;
                            s.have_result = 1;
                        }
                        None => s.result_overflow = 1,
                    }
                }
            }
            true
        }
    }
}

/// The query's ReadyForQuery arrived: answer it.
unsafe fn finish_query(s: &mut PgState) {
    let (code, len) = if s.query_error == 1 {
        (status::BAD_REQUEST, s.result_len as usize)
    } else if s.result_overflow == 1 {
        // The set did not fit: refused whole. A truncated result would be
        // indistinguishable from a complete one downstream.
        s.errors = s.errors.wrapping_add(1);
        (status::BAD_GATEWAY, 0)
    } else {
        (status::OK, s.result_len as usize)
    };
    s.desk.finish(code, len);
    s.has_query = 0;
    s.completed = s.completed.wrapping_add(1);
}

/// Place owed response records until the port is full or nothing is owed.
unsafe fn flush_answers(s: &mut PgState) {
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
unsafe fn read_requests(s: &mut PgState) {
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

/// Whether `b` holds a NUL — the simple-query terminator, so SQL text that
/// carries one would be cut short by the server. A plain scan: the
/// library's search lowers to a `memchr` a PIC module cannot link.
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

/// Take the oldest complete request, when none is being performed.
unsafe fn take_request(s: &mut PgState) {
    if s.has_query == 1 || s.probing == 1 {
        return;
    }
    let Some(at) = s.desk.next_request() else {
        return;
    };
    let Some((target, body)) = s.desk.request(at).map(|r| (r.target.len(), r.body)) else {
        return;
    };
    if target != 0 || body.is_empty() || has_nul(body) {
        s.desk.refuse(at, status::BAD_REQUEST);
        return;
    }
    let n = body.len();
    s.query[..n].copy_from_slice(body);
    s.query_len = n as u16;
    s.has_query = 1;
    s.result_len = 0;
    s.have_result = 0;
    s.result_overflow = 0;
    s.query_error = 0;
    s.desk.start(at);
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut PgState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. Answers first: an owed record leaves before more work starts.
        flush_answers(s);
        if s.draining == 1 {
            s.desk.drain();
        }

        // 2. Requests in, and the next one to perform.
        read_requests(s);
        take_request(s);

        // 3. A query drives connect (if down) or send (if ready); after a
        //    LINK DOWN the module reconnects on its own cadence.
        let connect = (s.has_query == 1 || (s.probing == 1 && now >= s.probe_at_ms))
            && s.phase == DISCONNECTED;
        if connect {
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
            s.phase = CONNECTING;
            s.started_ms = now;
        } else if s.has_query == 1 && s.phase == READY {
            let qn = s.query_len as usize;
            let mut q = [0u8; QUERY_MAX];
            q[..qn].copy_from_slice(&s.query[..qn]);
            if let Some(n) = pg_query(&q[..qn], &mut s.req) {
                stage_send(s, n, now);
                s.phase = QUERYING;
            }
        }

        // 4. Drain network events.
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
                    NET_MSG_CONNECTED if s.phase == CONNECTING => {
                        if plen >= CONN_ID_LEN {
                            let (id, tag) = connected_parts(pl);
                            if tag == s.tag || tag == REQUESTER_TAG_NONE {
                                s.conn_id = id;
                                send_startup(s, now);
                            }
                        }
                    }
                    NET_MSG_DATA if s.phase != DISCONNECTED => {
                        if plen > CONN_ID_LEN && conn_id(pl) == s.conn_id {
                            let data_len = plen - CONN_ID_LEN;
                            let space = ACC_BUF - s.acc_len as usize;
                            if data_len > space {
                                // A server message larger than the
                                // accumulator can never complete: the
                                // answer cannot be carried back.
                                overflowed(s, now);
                                break;
                            }
                            core::ptr::copy_nonoverlapping(
                                payload.add(CONN_ID_LEN),
                                s.acc.as_mut_ptr().add(s.acc_len as usize),
                                data_len,
                            );
                            s.acc_len += data_len as u32;
                            drain_messages(s, now);
                        }
                    }
                    NET_MSG_CLOSED if s.phase != DISCONNECTED => {
                        if plen >= CONN_ID_LEN && conn_id(pl) == s.conn_id {
                            connection_lost(s, now, 0);
                        }
                    }
                    NET_MSG_ERROR if plen > CONN_ID_LEN => {
                        // A tagged error is a connect-phase failure and is
                        // ours by tag alone; an untagged one belongs to an
                        // established connection and routes by conn id.
                        let (id, _errno, tag) = error_parts(pl);
                        let ours = if tag == REQUESTER_TAG_NONE {
                            (s.phase == CONNECTING) || (s.phase != DISCONNECTED && id == s.conn_id)
                        } else {
                            s.phase == CONNECTING && tag == s.tag
                        };
                        if ours {
                            connection_lost(s, now, 0);
                        }
                    }
                    _ => {}
                }
            }
        }

        // 5. Send pump.
        if s.conn_id != 0 && s.req_sent < s.req_len {
            // CMD_SEND payload is `[conn_id:2 LE][data…]`.
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
                let total_payload = chunk + 2;
                let cid = s.conn_id.to_le_bytes();
                s.nbuf[0] = NET_CMD_SEND;
                s.nbuf[1] = (total_payload & 0xff) as u8;
                s.nbuf[2] = (total_payload >> 8) as u8;
                s.nbuf[3] = cid[0];
                s.nbuf[4] = cid[1];
                core::ptr::copy_nonoverlapping(
                    s.req.as_ptr().add(s.req_sent as usize),
                    s.nbuf.as_mut_ptr().add(NET_FRAME_HDR + 2),
                    chunk,
                );
                (sys.channel_write)(s.net_out, s.nbuf.as_ptr(), NET_FRAME_HDR + total_payload);
                s.req_sent += chunk as u16;
            }
        }

        // 6. Timeouts. A query the server has not finished is answered 504
        //    and its connection closed (the next request reconnects); a
        //    handshake that stalls is a lost connection.
        if s.phase == QUERYING && now.wrapping_sub(s.started_ms) > REPLY_TIMEOUT_MS {
            s.desk.finish(status::TIMEOUT, 0);
            s.has_query = 0;
            s.errors = s.errors.wrapping_add(1);
            close_conn(s);
        } else if !matches!(s.phase, DISCONNECTED | READY | QUERYING) {
            let budget = if s.phase == CONNECTING {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                connection_lost(s, now, 0);
            }
        }

        flush_answers(s);

        // 7. Drain: report Done once idle.
        if s.draining == 1
            && s.has_query == 0
            && s.desk.idle()
            && !s.outbox.holding()
            && matches!(s.phase, DISCONNECTED | READY)
        {
            close_conn(s);
            return 1;
        }
        0
    }
}

/// The server sent more than the accumulator holds: whatever is in flight
/// cannot be answered from it.
unsafe fn overflowed(s: &mut PgState, _now: u64) {
    s.errors = s.errors.wrapping_add(1);
    if s.phase == QUERYING {
        s.desk.finish(status::BAD_GATEWAY, 0);
        s.has_query = 0;
    }
    close_conn(s);
}

/// Process every complete message in the accumulation buffer, then compact it.
unsafe fn drain_messages(s: &mut PgState, now: u64) {
    let mut off = 0usize;
    loop {
        let avail = &s.acc[off..s.acc_len as usize];
        let m = match pg_next_msg(avail) {
            Some(m) => m,
            None => break, // need more bytes
        };
        // Copy the body out (borrow-free) then dispatch.
        let bstart = off + m.body_start;
        let bend = off + m.body_end;
        let blen = bend - bstart;
        s.msg[..blen].copy_from_slice(&s.acc[bstart..bend]);
        let tag = s.acc[off];
        let mut text_len = 0usize;
        if !on_message(s, tag, blen, now, &mut text_len) {
            connection_lost(s, now, text_len);
            return;
        }
        if s.phase == DISCONNECTED {
            return;
        }
        off += m.total;
        if off >= s.acc_len as usize {
            break;
        }
    }
    // Compact any consumed prefix (manual loop: no copy_within panic path).
    if off > 0 {
        let rem = s.acc_len as usize - off;
        let mut k = 0usize;
        while k < rem {
            s.acc[k] = s.acc[off + k];
            k += 1;
        }
        s.acc_len = rem as u32;
    }
}
