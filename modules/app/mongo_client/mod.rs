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
//! authenticated -> ping -> emit ok. Protocol logic in `scram_core.rs` +
//! `mongo_core.rs`; this file is the I/O pump.
//!
//! Ports:  net_in/net_out (transport), status_out (auth/ping result).
//! Params: `endpoint` (hex `[ip:4][port:2 LE]`), `user`, `database`, `password`.

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
include!("../../common/hex_core.rs");

const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;

const NET_BUF: usize = 2048;
const REQ_BUF: usize = 1024;
const ACC_BUF: usize = 8192;
const NAME_BUF: usize = 128;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;

#[repr(C)]
struct MongoState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    publish_in: i32,
    status_out: i32,

    ip: [u8; 4],
    port: u16,
    ep_hex: [u8; 16],
    ep_hex_len: u16,
    user: [u8; NAME_BUF],
    user_len: u16,
    database: [u8; NAME_BUF],
    database_len: u16,
    password: [u8; NAME_BUF],
    password_len: u16,

    phase: MPhase,
    conn_id: u8,
    tag: u8,
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

    // INSERT-sink mode: when `collection` is set the module, once authenticated,
    // inserts each publish_in message as `{ value: <msg> }` into the collection.
    collection: [u8; NAME_BUF],
    collection_len: u16,
    inserting: u8,
    inserted: u32,
}

define_params! {
    MongoState;

    1, endpoint, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ep_hex_len as usize) < 16 {
            s.ep_hex[s.ep_hex_len as usize] = *d.add(i); s.ep_hex_len += 1; i += 1;
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
        s.net_out = out_chan;
        s.publish_in = dev_channel_port(sys, 0, 1);
        s.status_out = dev_channel_port(sys, 1, 1);
        s.ip = [0u8; 4];
        s.port = 0;
        s.ep_hex_len = 0;
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
        parse_tlv(s, params, params_len);
        let mut ep = [0u8; 8];
        if let Some(n) = hex_decode(&s.ep_hex[..s.ep_hex_len as usize], &mut ep) {
            if n >= 6 {
                s.ip = [ep[0], ep[1], ep[2], ep[3]];
                s.port = u16::from_le_bytes([ep[4], ep[5]]);
            }
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
            let mut payload = [0u8; 8];
            payload[0] = SOCK_TYPE_STREAM;
            payload[1] = s.ip[3];
            payload[2] = s.ip[2];
            payload[3] = s.ip[1];
            payload[4] = s.ip[0];
            let port = s.port.to_le_bytes();
            payload[5] = port[0];
            payload[6] = port[1];
            payload[7] = s.tag;
            net_write_frame(
                sys,
                s.net_out,
                NET_CMD_CONNECT,
                payload.as_ptr(),
                8,
                s.nbuf.as_mut_ptr(),
                NET_BUF,
            );
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
                let close = [s.conn_id];
                net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CLOSE,
                    close.as_ptr(),
                    1,
                    s.nbuf.as_mut_ptr(),
                    NET_BUF,
                );
            }
            s.conn_id = 0;
            s.acc_len = 0;
            s.req_len = 0;
            s.req_sent = 0;
            s.errors = s.errors.wrapping_add(1);
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
                    // Insert-sink mode: skip idle-Ready, wait for messages.
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
        }
        MPhase::InsertWait => {
            if bson_ok(body) {
                s.inserted = s.inserted.wrapping_add(1);
                emit_status(s, b"mongo: inserted\n");
            } else {
                s.errors = s.errors.wrapping_add(1);
                emit_status(s, b"mongo: insert failed\n");
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

        // Authenticate on boot.
        if s.phase == MPhase::Disconnected && s.draining == 0 && s.user_len > 0 {
            feed(s, sys, MEv::Start, now);
        }

        // Insert-sink: once authenticated, pull one message off publish_in and
        // insert it as `{ value: <msg> }`. One in flight at a time.
        if s.inserting != 0 && s.phase == MPhase::InsertIdle && s.publish_in >= 0 {
            let poll = (sys.channel_poll)(s.publish_in, 0x01);
            if poll > 0 && (poll as u32 & 0x01) != 0 {
                let mut m = [0u8; 256];
                let n = (sys.channel_read)(s.publish_in, m.as_mut_ptr(), m.len());
                if n > 0 {
                    let dl = s.database_len as usize;
                    let mut db = [0u8; NAME_BUF];
                    db[..dl].copy_from_slice(&s.database[..dl]);
                    let cl = s.collection_len as usize;
                    let mut coll = [0u8; NAME_BUF];
                    coll[..cl].copy_from_slice(&s.collection[..cl]);
                    s.req_id = s.req_id.wrapping_add(1);
                    let mut out = [0u8; REQ_BUF];
                    if let Some(bn) = mongo_insert_body(
                        s.req_id,
                        &db[..dl],
                        &coll[..cl],
                        &m[..n as usize],
                        &mut out,
                    ) {
                        s.req[..bn].copy_from_slice(&out[..bn]);
                        stage_send(s, bn, now);
                        s.phase = MPhase::InsertWait;
                    }
                }
            }
        }

        // Connected → begin SASL.
        // Drain network events.
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
                match msg {
                    NET_MSG_CONNECTED if s.phase == MPhase::Connecting => {
                        if plen >= 2 && *payload.add(1) == s.tag {
                            s.conn_id = *payload;
                            feed(s, sys, MEv::Connected, now);
                        }
                    }
                    NET_MSG_DATA if s.phase != MPhase::Disconnected => {
                        if plen > 1 && *payload == s.conn_id {
                            let data_len = plen - 1;
                            let space = ACC_BUF - s.acc_len as usize;
                            let take = if data_len < space { data_len } else { space };
                            core::ptr::copy_nonoverlapping(
                                payload.add(1),
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
                        if plen >= 1 && *payload == s.conn_id {
                            feed(s, sys, MEv::PeerClosed, now);
                        }
                    }
                    NET_MSG_ERROR => {
                        let ours = (s.phase == MPhase::Connecting
                            && plen >= 3
                            && *payload.add(2) == s.tag)
                            || (s.phase != MPhase::Disconnected
                                && plen >= 1
                                && *payload == s.conn_id);
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
            let max_chunk = NET_BUF - NET_FRAME_HDR - 1;
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
                let total_payload = chunk + 1;
                s.nbuf[0] = NET_CMD_SEND;
                s.nbuf[1] = (total_payload & 0xff) as u8;
                s.nbuf[2] = (total_payload >> 8) as u8;
                s.nbuf[3] = s.conn_id;
                core::ptr::copy_nonoverlapping(
                    s.req.as_ptr().add(s.req_sent as usize),
                    s.nbuf.as_mut_ptr().add(NET_FRAME_HDR + 1),
                    chunk,
                );
                (sys.channel_write)(s.net_out, s.nbuf.as_ptr(), NET_FRAME_HDR + total_payload);
                s.req_sent += chunk as u16;
            }
        }

        // Timeouts on any in-flight exchange.
        if !matches!(s.phase, MPhase::Disconnected | MPhase::Ready) {
            let budget = if s.phase == MPhase::Connecting {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                feed(s, sys, MEv::NetError, now);
            }
        }

        // Drain.
        if s.draining == 1 && matches!(s.phase, MPhase::Disconnected | MPhase::Ready) {
            if s.conn_id != 0 {
                let close = [s.conn_id];
                net_write_frame(
                    sys,
                    s.net_out,
                    NET_CMD_CLOSE,
                    close.as_ptr(),
                    1,
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
