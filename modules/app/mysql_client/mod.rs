//! MySQL connector — a GENUINE per-protocol Fluxor foundation module adding an
//! auth CLASS the others don't have: `mysql_native_password`, a SHA-1
//! challenge-response. The server's initial handshake carries a random 20-byte
//! scramble; the client proves the password with
//!   token = SHA1(pw) XOR SHA1(scramble ++ SHA1(SHA1(pw)))
//! — reply-dependent hashing over a server-chosen nonce, in MySQL's own binary
//! packet framing. Not expressible in a stateless codec, so it is a compiled
//! module.
//!
//! Protocol logic in the host-tested `mysql_core.rs`; this file is the I/O pump:
//! connect -> parse server handshake -> send native-auth response -> OK/ERR.
//!
//! Ports:  net_in/net_out (transport), status_out (auth result).
//! Params: `authority` (`host[:port]`, port 3306 when omitted), `user`, `database`, `password`.
//! Scope: `mysql_native_password` and `caching_sha2_password` FAST path (the
//! server-advertised plugin is spoken from the first response; AuthSwitch to
//! either plugin is honoured). The sha2 FULL path (cold credential cache)
//! needs the password over TLS and is surfaced as a distinct failure status
//! rather than silently downgraded.

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

include!("../../common/mysql_core.rs");

#[path = "../../common/authority.rs"]
mod authority;
use authority::{Authority, PORT_MYSQL};

use abi::contracts::net::net_proto::{
    conn_id, connected_parts, error_parts, CMD_CLOSE as NET_CMD_CLOSE,
    CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CMD_SEND as NET_CMD_SEND, CONNECT_TO_MAX, CONN_ID_LEN,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_CONNECTED as NET_MSG_CONNECTED, MSG_DATA as NET_MSG_DATA,
    MSG_ERROR as NET_MSG_ERROR, REQUESTER_TAG_NONE,
};

const NET_BUF: usize = 2048;
const REQ_BUF: usize = 512;
const ACC_BUF: usize = 4096;
const NAME_BUF: usize = 128;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;

#[repr(C)]
struct MysqlState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    status_out: i32,

    authority: Authority,
    user: [u8; NAME_BUF],
    user_len: u16,
    database: [u8; NAME_BUF],
    database_len: u16,
    password: [u8; NAME_BUF],
    password_len: u16,

    phase: MyPhase,
    conn_id: u16,
    tag: u8,
    started_ms: u64,
    draining: u8,

    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,
    acc: [u8; ACC_BUF],
    acc_len: u32,

    nbuf: [u8; NET_BUF],
    authed: u32,
    errors: u32,
    /// The auth plugin in play — seeded from the server handshake's advertised
    /// plugin, updated by an AuthSwitchRequest.
    plugin: MyAuthPlugin,
}

define_params! {
    MysqlState;

    // Tag 1 is retired.
    // The peer, `host[:port]`; the port defaults to 3306.
    5, authority, str, 0 => |s, d, len| {
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
    core::mem::size_of::<MysqlState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_drain"]
pub extern "C" fn module_drain(state: *mut u8) -> i32 {
    unsafe {
        (*(state as *mut MysqlState)).draining = 1;
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
        if state_size < core::mem::size_of::<MysqlState>() {
            return -2;
        }
        let s = &mut *(state as *mut MysqlState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.status_out = dev_channel_port(sys, 1, 1);
        s.authority.clear();
        s.user_len = 0;
        s.database_len = 0;
        s.password_len = 0;
        s.phase = MyPhase::Disconnected;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.started_ms = 0;
        s.draining = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.acc_len = 0;
        s.authed = 0;
        s.plugin = MyAuthPlugin::Native;
        s.errors = 0;
        parse_tlv(s, params, params_len);
        if !s.authority.adopt(PORT_MYSQL) {
            let m = b"[mysql] refusing to construct: authority (host[:port]) is required";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        dev_log(sys, 3, b"[mysql] init".as_ptr(), 12);
        0
    }
}

unsafe fn emit_status(s: &mut MysqlState, text: &[u8]) {
    let sys = &*s.syscalls;
    if s.status_out >= 0 {
        let poll = (sys.channel_poll)(s.status_out, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            (sys.channel_write)(s.status_out, text.as_ptr(), text.len());
        }
    }
}

unsafe fn feed(
    s: &mut MysqlState,
    sys: &SyscallTable,
    ev: MyEv,
    now: u64,
    scramble: Option<[u8; 20]>,
) {
    let (action, next) = mysql_transition(s.phase, ev);
    match action {
        MyAct::Connect => {
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
        MyAct::SendHandshakeResponse => {
            if let Some(scr) = scramble {
                let ul = s.user_len as usize;
                let dl = s.database_len as usize;
                let pl = s.password_len as usize;
                let mut u = [0u8; NAME_BUF];
                u[..ul].copy_from_slice(&s.user[..ul]);
                let mut db = [0u8; NAME_BUF];
                db[..dl].copy_from_slice(&s.database[..dl]);
                let mut pw = [0u8; NAME_BUF];
                pw[..pl].copy_from_slice(&s.password[..pl]);
                let mut out = [0u8; REQ_BUF];
                if let Some(n) = mysql_handshake_response(
                    &u[..ul],
                    &pw[..pl],
                    &db[..dl],
                    &scr,
                    s.plugin,
                    &mut out,
                ) {
                    s.req[..n].copy_from_slice(&out[..n]);
                    s.req_len = n as u16;
                    s.req_sent = 0;
                    s.started_ms = now;
                    s.acc_len = 0;
                }
            }
        }
        MyAct::Fail => {
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
            emit_status(s, b"mysql: auth failed\n");
        }
        MyAct::None => {}
    }
    if next == MyPhase::Ready && s.phase != MyPhase::Ready {
        s.authed = s.authed.wrapping_add(1);
        match s.plugin {
            MyAuthPlugin::CachingSha2 => emit_status(s, b"mysql: authenticated (caching_sha2)\n"),
            _ => emit_status(s, b"mysql: authenticated (native)\n"),
        }
    }
    s.phase = next;
}

/// Handle one complete MySQL packet body according to the phase. `seq` is the
/// packet's sequence id (an AuthSwitchResponse must reply with `seq + 1`).
unsafe fn on_packet(s: &mut MysqlState, body: &[u8], seq: u8, now: u64) {
    let sys = &*s.syscalls;
    match s.phase {
        MyPhase::AwaitHandshake => match mysql_parse_handshake(body) {
            Some((scramble, MyAuthPlugin::Other)) => {
                let _ = scramble;
                emit_status(s, b"mysql: server auth plugin unsupported\n");
                feed(s, sys, MyEv::AuthErr, now, None);
            }
            Some((scramble, plugin)) => {
                s.plugin = plugin;
                feed(s, sys, MyEv::GotHandshake, now, Some(scramble));
            }
            None => feed(s, sys, MyEv::AuthErr, now, None),
        },
        MyPhase::AwaitAuthResult => match mysql_reply_kind(body) {
            MyReply::Ok => feed(s, sys, MyEv::AuthOk, now, None),
            MyReply::MoreData => match body.get(1) {
                // caching_sha2 fast-auth success — the OK packet follows.
                Some(3) => {}
                // Full authentication: the server's credential cache is cold and
                // it wants the password over a secure transport. Deliberately NOT
                // implemented without TLS — fail loudly, never downgrade.
                Some(4) => {
                    emit_status(
                        s,
                        b"mysql: caching_sha2 full auth required (cold server cache; compose a TLS transport)\n",
                    );
                    feed(s, sys, MyEv::AuthErr, now, None);
                }
                _ => feed(s, sys, MyEv::AuthErr, now, None),
            },
            MyReply::AuthSwitch => match mysql_parse_auth_switch(body) {
                Some((MyAuthPlugin::Other, _)) | None => {
                    emit_status(s, b"mysql: auth switch to unsupported plugin\n");
                    feed(s, sys, MyEv::AuthErr, now, None);
                }
                Some((plugin, nonce)) => {
                    s.plugin = plugin;
                    let pl = s.password_len as usize;
                    let mut pw = [0u8; NAME_BUF];
                    pw[..pl].copy_from_slice(&s.password[..pl]);
                    let mut out = [0u8; REQ_BUF];
                    if let Some(n) = mysql_auth_switch_response(
                        &pw[..pl],
                        plugin,
                        &nonce,
                        seq.wrapping_add(1),
                        &mut out,
                    ) {
                        s.req[..n].copy_from_slice(&out[..n]);
                        s.req_len = n as u16;
                        s.req_sent = 0;
                        s.started_ms = now;
                    } else {
                        feed(s, sys, MyEv::AuthErr, now, None);
                    }
                }
            },
            _ => feed(s, sys, MyEv::AuthErr, now, None),
        },
        _ => {}
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut MysqlState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        if s.phase == MyPhase::Disconnected && s.draining == 0 && s.user_len > 0 {
            feed(s, sys, MyEv::Start, now, None);
        }

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
                    NET_MSG_CONNECTED if s.phase == MyPhase::Connecting => {
                        if plen >= CONN_ID_LEN {
                            let (id, tag) = connected_parts(pl);
                            if tag == s.tag || tag == REQUESTER_TAG_NONE {
                                s.conn_id = id;
                                feed(s, sys, MyEv::Connected, now, None);
                            }
                        }
                    }
                    NET_MSG_DATA if s.phase != MyPhase::Disconnected => {
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
                            // Drain EVERY complete packet in the accumulator: caching_sha2 fast auth
                            // sends AuthMoreData and OK in one segment, so stopping after one
                            // packet would strand the OK until the next (never-arriving) read.
                            while let Some((ps, pe, seq, total)) =
                                mysql_packet(&s.acc[..s.acc_len as usize])
                            {
                                let mut body = [0u8; ACC_BUF];
                                let blen = (pe - ps).min(body.len());
                                body[..blen].copy_from_slice(&s.acc[ps..ps + blen]);
                                let rem = s.acc_len as usize - total;
                                let mut k = 0usize;
                                while k < rem {
                                    s.acc[k] = s.acc[total + k];
                                    k += 1;
                                }
                                s.acc_len = rem as u32;
                                on_packet(s, &body[..blen], seq, now);
                            }
                        }
                    }
                    NET_MSG_CLOSED if s.phase != MyPhase::Disconnected => {
                        if plen >= CONN_ID_LEN && conn_id(pl) == s.conn_id {
                            feed(s, sys, MyEv::PeerClosed, now, None);
                        }
                    }
                    NET_MSG_ERROR if plen > CONN_ID_LEN => {
                        // A tagged error is a connect-phase failure and is
                        // ours by tag alone; an untagged one belongs to an
                        // established connection and routes by conn id.
                        let (id, _errno, tag) = error_parts(pl);
                        let ours = if tag == REQUESTER_TAG_NONE {
                            (s.phase == MyPhase::Connecting)
                                || (s.phase != MyPhase::Disconnected && id == s.conn_id)
                        } else {
                            s.phase == MyPhase::Connecting && tag == s.tag
                        };
                        if ours {
                            feed(s, sys, MyEv::NetError, now, None);
                        }
                    }
                    _ => {}
                }
            }
        }

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

        if !matches!(s.phase, MyPhase::Disconnected | MyPhase::Ready) {
            let budget = if s.phase == MyPhase::Connecting {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                feed(s, sys, MyEv::NetError, now, None);
            }
        }

        if s.draining == 1 && matches!(s.phase, MyPhase::Disconnected | MyPhase::Ready) {
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
