//! MongoDB connector — a GENUINE per-protocol Fluxor foundation module whose
//! point is SHARED-CORE REUSE. MongoDB authenticates with SCRAM-SHA-256, so this
//! module drives the SAME host-verified `scram_core` (SHA-256/HMAC/PBKDF2, client
//! proof over the server's random salt/nonce) that the Postgres connector uses —
//! over a completely different wire encoding (BSON/OP_MSG rather than Postgres'
//! typed message stream). That a codec cannot do the crypto is settled by pg;
//! this shows the compiled-module protocol logic composing with a shared core
//! instead of re-implementing it.
//!
//! Flow: connect -> saslStart(client-first) -> saslContinue(client-final) ->
//! authenticated -> ping -> ready. Protocol logic in `scram_core.rs` +
//! `mongo_core.rs`, the exchange provider half in `query_exchange.rs`; this
//! file is the I/O pump.
//!
//! # Ports
//!
//! `net_in`/`net_out` (transport), `status_out` (auth/ping/insert status
//! lines, TextPlain), and the exchange provider pair `request_in`
//! (`ExchangeRequest`) / `response_out` (`ExchangeResponse`). A module with
//! `request_in` wired and `response_out` unwired refuses to construct.
//!
//! # Params
//!
//! `authority` (`host[:port]`, port 27017 when omitted), `user`, `database`,
//! `password`, `collection` (enables inserts; without it every request is
//! refused 400 and the module only authenticates and pings).
//!
//! # Request
//!
//! Method POST; `target` empty; headers ignored; body = the value to insert,
//! at least one byte, stored as `{ value: <body> }` (a BSON string) in
//! `database.collection`; the whole insert command must fit 512 BSON bytes. Requests are performed one at a time, in arrival
//! order, once the session is authenticated; until then they wait.
//!
//! # Answer
//!
//! One answer per request, no content type, no headers:
//!
//! | Status | When | Body |
//! |---|---|---|
//! | 200 | the server acknowledged the insert (`ok` set, no write errors) | empty |
//! | 400 | the server refused the insert | its `errmsg` when the reply names one at top level, else empty |
//! | 400 | another method, a non-empty target, an empty body, no `collection` configured | empty |
//! | 413 | a body whose insert command exceeds 512 BSON bytes | empty |
//! | 503 | four requests already held, or draining | empty |
//! | 504 | no reply within 15 s (the connection is closed and reopened; the insert's effect is unknown) | empty |
//!
//! # LINK
//!
//! The module connects at boot and reconnects on its own after any failure
//! (1 s, doubling to 30 s). Once the session has been ready, losing it writes
//! LINK DOWN: every request held is dropped unanswered for the requester to
//! re-issue, and request records are read and dropped until LINK UP, which
//! the next ready session writes.

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
include!("../../common/mongo_core.rs");

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox};

#[path = "../../common/authority.rs"]
mod authority;
use authority::{Authority, PORT_MONGO};

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
const REQ_BUF: usize = 1024;
const ACC_BUF: usize = 8192;
const NAME_BUF: usize = 128;
/// Longest value one insert request carries; the BSON insert command it
/// becomes must also fit its 512-byte build buffer.
const VALUE_MAX: usize = 512;
/// The answer body: a refusal's `errmsg`.
const RESULT_MAX: usize = 512;
/// Requests held at once: one performed, the rest queued in arrival order.
const SLOTS: usize = 4;
/// A request target is refused unless empty; this bounds what is read.
const TARGET_MAX: usize = 64;
const QUEUE: usize = 8;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;
const RETRY_MIN_MS: u64 = 1_000;
const RETRY_MAX_MS: u64 = 30_000;

#[repr(C)]
struct MongoState {
    syscalls: *const SyscallTable,
    net_in: i32,
    /// The transport input's envelope reading (`common/envelope.rs`).
    net_inbox: Inbox,
    net_out: i32,
    request_in: i32,
    status_out: i32,
    response_out: i32,

    authority: Authority,
    user: [u8; NAME_BUF],
    user_len: u16,
    database: [u8; NAME_BUF],
    database_len: u16,
    password: [u8; NAME_BUF],
    password_len: u16,

    phase: MPhase,
    conn_id: u16,
    tag: RequesterTag,
    req_id: i32,
    conversation_id: i32,
    started_ms: u64,
    draining: u8,
    nonce_ctr: u32,

    // SCRAM: client-first (for the bare, needed by client-final) + expected sig.
    cfirst: [u8; 128],
    cfirst_len: u16,
    server_sig: [u8; 32],

    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,
    acc: [u8; ACC_BUF],
    acc_len: u32,

    nbuf: [u8; NET_BUF],
    authed: u32,
    errors: u32,

    // Insert mode: when `collection` is set the module, once authenticated,
    // inserts each request's body as `{ value: <body> }` into the collection.
    collection: [u8; NAME_BUF],
    collection_len: u16,
    inserting: u8,
    inserted: u32,

    // The next connect attempt after a failure, and the current wait.
    retry_at_ms: u64,
    backoff_ms: u64,

    desk: QueryDesk<SLOTS, TARGET_MAX, VALUE_MAX, QUEUE>,
    outbox: ExchangeOutbox,
    result: [u8; RESULT_MAX],
    // The request record being read, and the response record being written.
    rec: [u8; RECORD_MAX],
    out: [u8; RECORD_MAX],
}

define_params! {
    MongoState;

    // Tag 1 is retired.
    // The peer, `host[:port]`; the port defaults to 27017.
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
    5, collection, str, 0 => |s, d, len| {
        // Gate on len>0: set_defaults() fires every closure, so an absent
        // param must not flip the module into insert mode.
        if len > 0 {
            let mut i = 0usize;
            while i < len && (s.collection_len as usize) < NAME_BUF {
                s.collection[s.collection_len as usize] = *d.add(i); s.collection_len += 1; i += 1;
            }
            s.inserting = 1;
        }
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<MongoState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_drain"]
pub extern "C" fn module_drain(state: *mut u8) -> i32 {
    unsafe {
        (*(state as *mut MongoState)).draining = 1;
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
        if state_size < core::mem::size_of::<MongoState>() {
            return -2;
        }
        let s = &mut *(state as *mut MongoState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_inbox = Inbox::new();
        s.net_out = out_chan;
        s.request_in = dev_channel_port(sys, 0, 1);
        s.status_out = dev_channel_port(sys, 1, 1);
        s.response_out = dev_channel_port(sys, 1, 2);
        s.authority.clear();
        s.user_len = 0;
        s.database_len = 0;
        s.password_len = 0;
        s.phase = MPhase::Disconnected;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.req_id = 0;
        s.conversation_id = 0;
        s.started_ms = 0;
        s.draining = 0;
        s.nonce_ctr = 0;
        s.cfirst_len = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.acc_len = 0;
        s.authed = 0;
        s.errors = 0;
        s.collection_len = 0;
        s.inserting = 0;
        s.inserted = 0;
        s.retry_at_ms = 0;
        s.backoff_ms = RETRY_MIN_MS;
        s.desk.reset(METHOD_POST, 0);
        s.outbox = ExchangeOutbox::new();
        parse_tlv(s, params, params_len);
        if !s.authority.adopt(PORT_MONGO) {
            let m = b"[mongo] refusing to construct: authority (host[:port]) is required";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if s.request_in >= 0 && s.response_out < 0 {
            let m = b"[mongo] refusing to construct: request_in is wired but response_out is not";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        dev_log(sys, 3, b"[mongo] init".as_ptr(), 12);
        0
    }
}

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

unsafe fn stage_send(s: &mut MongoState, n: usize, now: u64) {
    s.req_len = n as u16;
    s.req_sent = 0;
    s.started_ms = now;
    s.acc_len = 0;
}

unsafe fn emit_status(s: &mut MongoState, text: &[u8]) {
    let sys = &*s.syscalls;
    if s.status_out >= 0 {
        let poll = (sys.channel_poll)(s.status_out, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            (sys.channel_write)(s.status_out, text.as_ptr(), text.len());
        }
    }
}

unsafe fn feed(s: &mut MongoState, sys: &SyscallTable, ev: MEv, now: u64) {
    let (action, next) = mongo_transition(s.phase, ev);
    let db_len = s.database_len as usize;
    let mut db = [0u8; NAME_BUF];
    db[..db_len].copy_from_slice(&s.database[..db_len]);
    match action {
        MAct::Connect => {
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
        MAct::SendSaslStart => {
            s.nonce_ctr = s.nonce_ctr.wrapping_add(1);
            let seed = now ^ ((s.tag as u64) << 32) ^ (s.nonce_ctr as u64);
            let mut nonce = [0u8; 20];
            let nn = gen_nonce(seed, &mut nonce);
            let ul = s.user_len as usize;
            let mut user = [0u8; NAME_BUF];
            user[..ul].copy_from_slice(&s.user[..ul]);
            let mut cf = [0u8; 128];
            if let Some(cn) = scram_client_first(&user[..ul], &nonce[..nn], &mut cf) {
                s.cfirst[..cn].copy_from_slice(&cf[..cn]);
                s.cfirst_len = cn as u16;
                s.req_id = s.req_id.wrapping_add(1);
                let mut out = [0u8; REQ_BUF];
                if let Some(n) = mongo_sasl_start(s.req_id, &db[..db_len], &cf[..cn], &mut out) {
                    s.req[..n].copy_from_slice(&out[..n]);
                    stage_send(s, n, now);
                }
            }
        }
        MAct::SendSaslContinue => {
            // The server-first payload was captured into acc-derived scratch by
            // the caller (see on_reply); recompute client-final here.
            // (handled in on_reply which stashed nothing — build below.)
        }
        MAct::DeliverReply => {}
        MAct::Fail => {
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
            s.acc_len = 0;
            s.req_len = 0;
            s.req_sent = 0;
            s.errors = s.errors.wrapping_add(1);
            s.retry_at_ms = now.wrapping_add(s.backoff_ms);
            s.backoff_ms = (s.backoff_ms * 2).min(RETRY_MAX_MS);
            // A session that had been ready is a LINK DOWN: whatever was
            // held — an insert awaiting its ack included — is now the
            // requester's to re-issue.
            if s.desk.link_down() {
                dev_log(sys, 2, b"[mongo] link down".as_ptr(), 17);
            }
            emit_status(s, b"mongo: auth failed\n");
        }
        MAct::None => {}
    }
    s.phase = next;
}

/// Dispatch one complete OP_MSG reply body according to the current phase.
unsafe fn on_reply(s: &mut MongoState, body: &[u8], now: u64) {
    let sys = &*s.syscalls;
    let db_len = s.database_len as usize;
    let mut db = [0u8; NAME_BUF];
    db[..db_len].copy_from_slice(&s.database[..db_len]);
    match s.phase {
        MPhase::AwaitSaslStart => {
            if !bson_ok(body) {
                feed(s, sys, MEv::AuthFailed, now);
                return;
            }
            let conv = bson_get_i32(body, b"conversationId").unwrap_or(0);
            s.conversation_id = conv;
            // server-first payload → compute client-final via shared scram_core.
            if let Some((ps, pe)) = bson_get_bin(body, b"payload") {
                let bare_len = s.cfirst_len as usize;
                let mut bare = [0u8; 128];
                bare[..bare_len].copy_from_slice(&s.cfirst[..bare_len]);
                let pl = s.password_len as usize;
                let mut pw = [0u8; NAME_BUF];
                pw[..pl].copy_from_slice(&s.password[..pl]);
                let mut cfinal = [0u8; 256];
                let mut ssig = [0u8; 32];
                if let Some(fnl) = scram_build_client_final(
                    &bare[3..bare_len],
                    &body[ps..pe],
                    &pw[..pl],
                    &mut cfinal,
                    &mut ssig,
                ) {
                    s.server_sig = ssig;
                    s.req_id = s.req_id.wrapping_add(1);
                    let mut out = [0u8; REQ_BUF];
                    if let Some(n) = mongo_sasl_continue(
                        s.req_id,
                        &db[..db_len],
                        s.conversation_id,
                        &cfinal[..fnl],
                        &mut out,
                    ) {
                        s.req[..n].copy_from_slice(&out[..n]);
                        stage_send(s, n, now);
                        s.phase = MPhase::AwaitSaslContinue;
                        return;
                    }
                }
            }
            feed(s, sys, MEv::AuthFailed, now);
        }
        MPhase::AwaitSaslContinue => {
            // ok=true means the server accepted our proof (server-final present);
            // with skipEmptyExchange it also sets done=true. Treat proof
            // acceptance as authenticated.
            if bson_ok(body) {
                // Authenticated. Confirm with a ping.
                s.authed = s.authed.wrapping_add(1);
                s.phase = MPhase::Ready;
                s.req_id = s.req_id.wrapping_add(1);
                let mut out = [0u8; REQ_BUF];
                if let Some(n) = mongo_ping(s.req_id, &db[..db_len], &mut out) {
                    s.req[..n].copy_from_slice(&out[..n]);
                    stage_send(s, n, now);
                    s.phase = MPhase::AwaitCommand;
                }
            } else {
                feed(s, sys, MEv::AuthFailed, now);
            }
        }
        MPhase::AwaitCommand => {
            if bson_ok(body) {
                if s.inserting != 0 {
                    // Insert mode: skip idle-Ready, wait for requests.
                    emit_status(s, b"mongo: authenticated, insert ready\n");
                    s.phase = MPhase::InsertIdle;
                } else {
                    emit_status(s, b"mongo: authenticated, ping ok\n");
                    s.phase = MPhase::Ready;
                }
            } else {
                emit_status(s, b"mongo: ping failed\n");
                s.phase = MPhase::Ready;
            }
            // The session is ready: the link is up.
            s.backoff_ms = RETRY_MIN_MS;
            if s.desk.link_is_down() {
                dev_log(sys, 3, b"[mongo] link up".as_ptr(), 15);
            }
            s.desk.link_up();
        }
        MPhase::InsertWait => {
            // Acknowledged only with `ok` set and no write errors; a reply
            // carrying a `writeErrors` array reads as not-ok here, since
            // the reader stops at the array.
            if bson_ok(body) && bson_find(body, b"writeErrors").is_none() {
                s.inserted = s.inserted.wrapping_add(1);
                emit_status(s, b"mongo: inserted\n");
                s.desk.finish(status::OK, 0);
            } else {
                s.errors = s.errors.wrapping_add(1);
                emit_status(s, b"mongo: insert failed\n");
                let n = match bson_get_str(body, b"errmsg") {
                    Some((a, b)) => {
                        let n = (b - a).min(RESULT_MAX);
                        s.result[..n].copy_from_slice(&body[a..a + n]);
                        n
                    }
                    None => 0,
                };
                s.desk.finish(status::BAD_REQUEST, n);
            }
            s.phase = MPhase::InsertIdle;
        }
        _ => {}
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut MongoState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // Answers first, then requests in.
        flush_answers(s);
        if s.draining == 1 {
            s.desk.drain();
        }
        read_requests(s);

        // Authenticate on boot, and again after a failure once its wait is
        // over.
        if s.phase == MPhase::Disconnected
            && s.draining == 0
            && s.user_len > 0
            && now >= s.retry_at_ms
        {
            feed(s, sys, MEv::Start, now);
        }

        // Inserts: once authenticated, perform the oldest complete request.
        // One in flight at a time.
        take_request(s, now);

        // Connected → begin SASL.
        // Drain network events.
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
                    NET_MSG_CONNECTED if s.phase == MPhase::Connecting => {
                        if plen >= CONN_ID_LEN {
                            let (id, tag) = connected_parts(pl);
                            if tag == s.tag || tag == REQUESTER_TAG_NONE {
                                s.conn_id = id;
                                feed(s, sys, MEv::Connected, now);
                            }
                        }
                    }
                    NET_MSG_DATA if s.phase != MPhase::Disconnected => {
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
                            if let Some(total) = mongo_reply_len(&s.acc[..s.acc_len as usize]) {
                                if let Some((bs, be)) = mongo_reply_body(&s.acc[..total]) {
                                    let mut body = [0u8; 2048];
                                    let blen = (be - bs).min(body.len());
                                    body[..blen].copy_from_slice(&s.acc[bs..bs + blen]);
                                    let rem = s.acc_len as usize - total;
                                    let mut k = 0usize;
                                    while k < rem {
                                        s.acc[k] = s.acc[total + k];
                                        k += 1;
                                    }
                                    s.acc_len = rem as u32;
                                    on_reply(s, &body[..blen], now);
                                }
                            }
                        }
                    }
                    NET_MSG_CLOSED if s.phase != MPhase::Disconnected => {
                        if plen >= CONN_ID_LEN && conn_id(pl) == s.conn_id {
                            feed(s, sys, MEv::PeerClosed, now);
                        }
                    }
                    NET_MSG_ERROR if plen > CONN_ID_LEN => {
                        // A tagged error is a connect-phase failure and is
                        // ours by tag alone; an untagged one belongs to an
                        // established connection and routes by conn id.
                        let (id, _errno, tag) = error_parts(pl);
                        let ours = if tag == REQUESTER_TAG_NONE {
                            (s.phase == MPhase::Connecting)
                                || (s.phase != MPhase::Disconnected && id == s.conn_id)
                        } else {
                            s.phase == MPhase::Connecting && tag == s.tag
                        };
                        if ours {
                            feed(s, sys, MEv::NetError, now);
                        }
                    }
                    _ => {}
                }
            }
        }

        // Send pump.
        if s.conn_id != 0 && s.req_sent < s.req_len {
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
                s.nbuf[0] = NET_CMD_SEND;
                s.nbuf[1] = (total_payload & 0xff) as u8;
                s.nbuf[2] = (total_payload >> 8) as u8;
                s.nbuf[NET_FRAME_HDR..NET_FRAME_HDR + CONN_ID_LEN]
                    .copy_from_slice(&s.conn_id.to_le_bytes());
                core::ptr::copy_nonoverlapping(
                    s.req.as_ptr().add(s.req_sent as usize),
                    s.nbuf.as_mut_ptr().add(NET_FRAME_HDR + CONN_ID_LEN),
                    chunk,
                );
                (sys.channel_write)(s.net_out, s.nbuf.as_ptr(), NET_FRAME_HDR + total_payload);
                s.req_sent += chunk as u16;
            }
        }

        // Timeouts. An insert the server has not acknowledged is answered
        // 504 and its connection reopened; any other stalled phase is a
        // lost session.
        if s.phase == MPhase::InsertWait && now.wrapping_sub(s.started_ms) > REPLY_TIMEOUT_MS {
            s.desk.finish(status::TIMEOUT, 0);
            s.errors = s.errors.wrapping_add(1);
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
            s.acc_len = 0;
            s.req_len = 0;
            s.req_sent = 0;
            s.phase = MPhase::Disconnected;
        } else if !matches!(
            s.phase,
            MPhase::Disconnected | MPhase::Ready | MPhase::InsertIdle
        ) {
            let budget = if s.phase == MPhase::Connecting {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                feed(s, sys, MEv::NetError, now);
            }
        }

        flush_answers(s);

        // Drain.
        if s.draining == 1
            && s.desk.idle()
            && !s.outbox.holding()
            && matches!(
                s.phase,
                MPhase::Disconnected | MPhase::Ready | MPhase::InsertIdle
            )
        {
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
                s.conn_id = 0;
            }
            return 1;
        }
        0
    }
}

/// Place owed response records until the port is full or nothing is owed.
unsafe fn flush_answers(s: &mut MongoState) {
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
unsafe fn read_requests(s: &mut MongoState) {
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

/// Perform the oldest complete request: refused at once without a
/// `collection`, otherwise inserted when the session is idle.
unsafe fn take_request(s: &mut MongoState, now: u64) {
    if s.inserting != 0 && s.phase != MPhase::InsertIdle {
        return;
    }
    let Some(at) = s.desk.next_request() else {
        return;
    };
    let Some((target, value)) = s.desk.request(at).map(|r| (r.target.len(), r.body)) else {
        return;
    };
    if s.inserting == 0 || target != 0 || value.is_empty() {
        s.desk.refuse(at, status::BAD_REQUEST);
        return;
    }
    let dl = s.database_len as usize;
    let cl = s.collection_len as usize;
    let mut out = [0u8; REQ_BUF];
    let Some(bn) = mongo_insert_body(
        s.req_id.wrapping_add(1),
        &s.database[..dl],
        &s.collection[..cl],
        value,
        &mut out,
    ) else {
        s.desk.refuse(at, status::TOO_LARGE);
        return;
    };
    s.req_id = s.req_id.wrapping_add(1);
    s.req[..bn].copy_from_slice(&out[..bn]);
    stage_send(s, bn, now);
    s.phase = MPhase::InsertWait;
    s.desk.start(at);
}
