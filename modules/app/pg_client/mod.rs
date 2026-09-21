//! Postgres connector — a GENUINE per-protocol Fluxor foundation module, and the
//! sharpest proof of the codec-vs-protocol line: it performs the SCRAM-SHA-256
//! authentication handshake. That is a multi-round-trip exchange in which the
//! client proof depends on the server's *random* salt and nonce and requires
//! PBKDF2 (thousands of HMAC iterations) plus an HMAC/XOR chain — reply-dependent
//! cryptographic computation a stateless encode/decode bytecode program cannot
//! express. So this MUST be a compiled module.
//!
//! The crypto lives in the host-tested `scram_core.rs` (verified against RFC 7677
//! vectors); the wire framing in `pg_core.rs`; this file is the connection state
//! machine that drives startup -> SASL(SCRAM) -> ready -> simple query.
//!
//! Ports:  net_in/net_out (transport), request_in (SQL query text),
//!         reply_out (result set: first column of EVERY row, newline-joined —
//!         one reply record per query; a set that overflows the reply buffer
//!         is DROPPED with `errors` counted, never truncated: a consumer
//!         must not mistake a prefix for the whole set).
//! Params: `authority` (`host[:port]`, port 5432 when omitted), `user`, `database`, `password`,
//!         `cid_len` (0 = off; N = the first N bytes of each request record are
//!         an opaque correlation prefix, echoed at the front of the reply —
//!         and every query then yields a reply, a row-less one being the bare
//!         prefix — so request/response chains can correlate across this hop).

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

// Shared, host-tested cores — identical source to the `chronicle-bytecode` crate.
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/b64.rs");
include!("../../common/scram_core.rs");
include!("../../common/pg_core.rs");
include!("../../common/pg_reply.rs");

#[path = "../../common/authority.rs"]
mod authority;
use authority::{Authority, PORT_PG};

use abi::contracts::net::net_proto::{
    conn_id, connected_parts, error_parts, CMD_CLOSE as NET_CMD_CLOSE,
    CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CMD_SEND as NET_CMD_SEND, CONNECT_TO_MAX, CONN_ID_LEN,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_CONNECTED as NET_MSG_CONNECTED, MSG_DATA as NET_MSG_DATA,
    MSG_ERROR as NET_MSG_ERROR, REQUESTER_TAG_NONE,
};

const NET_BUF: usize = 2048;
const REQ_BUF: usize = 2048;
const ACC_BUF: usize = 16384;
const PEND_BUF: usize = 4096;
const NAME_BUF: usize = 128;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;

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
    net_out: i32,
    request_in: i32,
    reply_out: i32,

    authority: Authority,
    user: [u8; NAME_BUF],
    user_len: u16,
    database: [u8; NAME_BUF],
    database_len: u16,
    password: [u8; NAME_BUF],
    password_len: u16,

    phase: u8,
    conn_id: u16,
    tag: u8,
    started_ms: u64,
    draining: u8,
    nonce_ctr: u32,

    // SCRAM: the client-first-bare (for AuthMessage) + expected server signature.
    cfirst: [u8; 128],
    cfirst_len: u16,
    server_sig: [u8; 32],

    // The queued SQL query.
    query: [u8; 1024],
    query_len: u16,
    has_query: u8,
    // The accumulated result set: first column of every row, newline-joined.
    result: [u8; 1024],
    result_len: u16,
    have_result: u8,
    // Set when a row would overflow `result`: the whole reply is refused at
    // emit (fail closed) rather than truncated to a plausible-looking prefix.
    result_overflow: u8,

    // Opt-in correlation prefix (`cid_len` param, 0 = off): the first
    // `cid_cfg` bytes of each request record are OPAQUE — carried here, not
    // sent to the server — and echoed verbatim at the front of the reply
    // record. With it on, every query yields a reply (a row-less INSERT
    // replies with just the prefix), so a request/response upstream like
    // wave's HANDLER_APP can correlate across this connector.
    cid: [u8; 16],
    cid_have: u8,
    cid_cfg: u8,

    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,
    // Accumulated server bytes (may hold several messages).
    acc: [u8; ACC_BUF],
    acc_len: u32,

    pending: [u8; PEND_BUF],
    pending_len: u16,
    pending_off: u16,

    nbuf: [u8; NET_BUF],
    completed: u32,
    errors: u32,
}

define_params! {
    PgState;

    // Tag 1 is retired.
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
    5, cid_len, u32, 0 => |s, d, len| {
        s.cid_cfg = p_u32(d, len, 0, 16).min(16) as u8;
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
        s.net_out = out_chan;
        s.request_in = dev_channel_port(sys, 0, 1);
        s.reply_out = dev_channel_port(sys, 1, 1);
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
        s.cfirst_len = 0;
        s.query_len = 0;
        s.has_query = 0;
        s.result_len = 0;
        s.have_result = 0;
        s.result_overflow = 0;
        s.cid = [0u8; 16];
        s.cid_have = 0;
        s.cid_cfg = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.acc_len = 0;
        s.pending_len = 0;
        s.pending_off = 0;
        s.completed = 0;
        s.errors = 0;
        parse_tlv(s, params, params_len);
        if !s.authority.adopt(PORT_PG) {
            let m = b"[pg] refusing to construct: authority (host[:port]) is required";
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
        fail(s, now);
    }
}

/// Reset on error / disconnect.
unsafe fn fail(s: &mut PgState, _now: u64) {
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
    s.has_query = 0;
    s.errors = s.errors.wrapping_add(1);
}

/// Handle one framed server message; returns false on a fatal error.
unsafe fn on_message(s: &mut PgState, tag: u8, body: &[u8], now: u64) -> bool {
    if tag == b'E' {
        // A statement-level error (duplicate key, missing relation, …) is
        // an ORDINARY query outcome: the server stays open and
        // ReadyForQuery follows, which completes the round trip — with a
        // cid prefix the echoed reply IS the completion signal the
        // upstream request/response chain is waiting on. Treating it as
        // fatal closed the connection and DROPPED the pending reply, so
        // the caller hung to its own timeout. Only a handshake-phase
        // error is fatal.
        if s.phase == QUERYING {
            s.errors = s.errors.wrapping_add(1);
            return true;
        }
        return false; // ErrorResponse during startup/auth
    }
    match s.phase {
        STARTUP if tag == b'R' => match pg_auth_type(body) {
            Some(10) if pg_sasl_has_scram_sha256(body) => {
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
            if pg_auth_type(body) == Some(11) {
                let server_first = &body[4..]; // after the auth_type
                let bare_len = s.cfirst_len as usize;
                let mut bare = [0u8; 128];
                bare[..bare_len].copy_from_slice(&s.cfirst[..bare_len]);
                let mut cfinal = [0u8; 256];
                let mut ssig = [0u8; 32];
                // client-first-bare is cfirst[3..] (after "n,,").
                if let Some(fnl) = scram_build_client_final(
                    &bare[3..bare_len],
                    server_first,
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
            match pg_auth_type(body) {
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
            // query are captured; the closing ReadyForQuery emits the result.
            if tag == b'Z' {
                if s.phase == QUERYING {
                    emit_result(s);
                }
                s.phase = READY;
            } else if s.phase == QUERYING && tag == b'D' && s.result_overflow == 0 {
                if let Some((cs, ce)) = pg_datarow_col0(body) {
                    // Split-borrow: the column bytes live in `acc` (via `body`),
                    // the accumulation in `result` — disjoint state fields.
                    let mut col = [0u8; 1024];
                    let rl = ce - cs;
                    if rl > col.len() {
                        // A column wider than the staging copy cannot be
                        // appended whole; refuse the reply, never clip it.
                        s.result_overflow = 1;
                    } else {
                        col[..rl].copy_from_slice(&body[cs..ce]);
                        match pg_reply_append(
                            &mut s.result,
                            s.result_len as usize,
                            s.have_result == 1,
                            &col[..rl],
                        ) {
                            Some(n) => {
                                s.result_len = n as u16;
                                s.have_result = 1;
                            }
                            None => s.result_overflow = 1,
                        }
                    }
                }
            }
            true
        }
    }
}

unsafe fn emit_result(s: &mut PgState) {
    let sys = &*s.syscalls;
    if s.result_overflow == 1 {
        // The set did not fit: refuse the whole reply. A truncated result
        // would be indistinguishable from a complete one downstream.
        s.errors = s.errors.wrapping_add(1);
    } else if s.reply_out >= 0 && s.cid_have == 1 {
        // Correlated reply: [cid prefix][rows]. Emitted even when the query
        // produced no rows (an INSERT) — the prefix alone IS the completion
        // signal the upstream request/response chain is waiting on.
        let poll = (sys.channel_poll)(s.reply_out, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            let cid = s.cid_cfg as usize;
            let rl = s.result_len as usize;
            s.nbuf[..cid].copy_from_slice(&s.cid[..cid]);
            s.nbuf[cid..cid + rl].copy_from_slice(&s.result[..rl]);
            (sys.channel_write)(s.reply_out, s.nbuf.as_ptr(), cid + rl);
        }
    } else if s.reply_out >= 0 && s.have_result == 1 {
        let poll = (sys.channel_poll)(s.reply_out, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            (sys.channel_write)(s.reply_out, s.result.as_ptr(), s.result_len as usize);
        }
    }
    s.cid_have = 0;
    s.have_result = 0;
    s.result_len = 0;
    s.result_overflow = 0;
    s.has_query = 0;
    s.completed = s.completed.wrapping_add(1);
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut PgState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. Load the next SQL query when none is in flight.
        if s.has_query == 0
            && s.draining == 0
            && s.request_in >= 0
            && s.pending_off >= s.pending_len
        {
            let poll = (sys.channel_poll)(s.request_in, 0x01);
            if poll > 0 && (poll as u32 & 0x01) != 0 {
                let n = (sys.channel_read)(s.request_in, s.pending.as_mut_ptr(), PEND_BUF);
                if n > 0 {
                    s.pending_len = n as u16;
                    s.pending_off = 0;
                }
            }
        }
        if s.has_query == 0 && s.pending_off < s.pending_len {
            let base = s.pending_off as usize;
            let end = s.pending_len as usize;
            // Peel the opaque correlation prefix (if configured) before the
            // SQL text; a record no longer than the prefix carries no query.
            let cid = s.cid_cfg as usize;
            let start = if cid > 0 && end - base > cid {
                s.cid[..cid].copy_from_slice(&s.pending[base..base + cid]);
                s.cid_have = 1;
                base + cid
            } else {
                s.cid_have = 0;
                base
            };
            let n = (end - start).min(1024);
            s.query[..n].copy_from_slice(&s.pending[start..start + n]);
            s.query_len = n as u16;
            s.has_query = 1;
            s.pending_off = end as u16;
        }

        // 2. A queued query drives connect (if down) or send (if ready).
        if s.has_query == 1 {
            if s.phase == DISCONNECTED {
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
            } else if s.phase == READY {
                let qn = s.query_len as usize;
                let mut q = [0u8; 1024];
                q[..qn].copy_from_slice(&s.query[..qn]);
                if let Some(n) = pg_query(&q[..qn], &mut s.req) {
                    stage_send(s, n, now);
                    s.phase = QUERYING;
                }
            }
        }

        // 3. Drain network events.
        if s.net_in >= 0 {
            loop {
                let poll = (sys.channel_poll)(s.net_in, 0x01);
                if poll <= 0 || (poll as u32 & 0x01) == 0 {
                    break;
                }
                let (msg, plen) = net_read_frame(sys, s.net_in, s.nbuf.as_mut_ptr(), NET_BUF);
                if msg == 0 {
                    break;
                }
                let payload = s.nbuf.as_ptr().add(NET_FRAME_HDR);
                let pl = core::slice::from_raw_parts(payload, plen.min(NET_BUF - NET_FRAME_HDR));
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
                            let take = if data_len < space { data_len } else { space };
                            core::ptr::copy_nonoverlapping(
                                payload.add(CONN_ID_LEN),
                                s.acc.as_mut_ptr().add(s.acc_len as usize),
                                take,
                            );
                            s.acc_len += take as u32;
                            drain_messages(s, now);
                        }
                    }
                    NET_MSG_CLOSED if s.phase != DISCONNECTED => {
                        if plen >= CONN_ID_LEN && conn_id(pl) == s.conn_id {
                            fail(s, now);
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
                            fail(s, now);
                        }
                    }
                    _ => {}
                }
            }
        }

        // 4. Send pump.
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

        // 5. Timeouts.
        if !matches!(s.phase, DISCONNECTED | READY) {
            let budget = if s.phase == CONNECTING {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                fail(s, now);
            }
        }

        // 6. Drain: report Done once idle.
        if s.draining == 1
            && s.has_query == 0
            && s.pending_off >= s.pending_len
            && matches!(s.phase, DISCONNECTED | READY)
        {
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
                s.conn_id = 0;
            }
            return 1;
        }
        0
    }
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
        let mut body = [0u8; 1024];
        let take = blen.min(body.len());
        body[..take].copy_from_slice(&s.acc[bstart..bstart + take]);
        let tag = s.acc[off];
        if !on_message(s, tag, &body[..take], now) {
            fail(s, now);
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
