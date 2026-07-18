//! Cassandra CQL connector — a GENUINE per-protocol Fluxor foundation module.
//! CQL is a stream-multiplexed binary FRAME protocol: every frame carries a
//! stream id so many in-flight queries share one connection, and the connection
//! opens with a negotiation — the client sends STARTUP, the server answers READY
//! (open) or AUTHENTICATE, which forces a PLAIN AUTH_RESPONSE / AUTH_SUCCESS
//! exchange. A staged negotiation with a reply-dependent branch over a
//! stream-tagged frame layer is a stateful session, not request/reply.
//!
//! Protocol logic in the host-tested `cql_core.rs`; this file is the I/O pump:
//! connect -> STARTUP -> READY | (AUTHENTICATE -> AUTH_RESPONSE -> AUTH_SUCCESS).
//!
//! Ports:  net_in/net_out (transport), status_out (session status).
//! Params: `endpoint` (hex `[ip:4][port:2 LE]`), `user`, `pass` (used only if the
//!         server sends AUTHENTICATE).

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

include!("../../common/cql_core.rs");
include!("../../common/hex_core.rs");

const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;

const NET_BUF: usize = 2048;
const REQ_BUF: usize = 512;
const ACC_BUF: usize = 8192;
const NAME_BUF: usize = 128;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;

#[repr(C)]
struct CqlState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    status_out: i32,

    ip: [u8; 4],
    port: u16,
    ep_hex: [u8; 16],
    ep_hex_len: u16,
    user: [u8; NAME_BUF],
    user_len: u16,
    pass: [u8; NAME_BUF],
    pass_len: u16,

    phase: CPhase,
    conn_id: u8,
    tag: u8,
    started_ms: u64,
    draining: u8,

    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,
    acc: [u8; ACC_BUF],
    acc_len: u32,

    nbuf: [u8; NET_BUF],
    ready: u32,
    errors: u32,
}

define_params! {
    CqlState;

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
    3, pass, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.pass_len as usize) < NAME_BUF {
            s.pass[s.pass_len as usize] = *d.add(i); s.pass_len += 1; i += 1;
        }
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<CqlState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_drain"]
pub extern "C" fn module_drain(state: *mut u8) -> i32 {
    unsafe {
        (*(state as *mut CqlState)).draining = 1;
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
        if state_size < core::mem::size_of::<CqlState>() {
            return -2;
        }
        let s = &mut *(state as *mut CqlState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.status_out = dev_channel_port(sys, 1, 1);
        s.ip = [0u8; 4];
        s.port = 0;
        s.ep_hex_len = 0;
        s.user_len = 0;
        s.pass_len = 0;
        s.phase = CPhase::Disconnected;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.started_ms = 0;
        s.draining = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.acc_len = 0;
        s.ready = 0;
        s.errors = 0;
        parse_tlv(s, params, params_len);
        let mut ep = [0u8; 8];
        if let Some(n) = hex_decode(&s.ep_hex[..s.ep_hex_len as usize], &mut ep) {
            if n >= 6 {
                s.ip = [ep[0], ep[1], ep[2], ep[3]];
                s.port = u16::from_le_bytes([ep[4], ep[5]]);
            }
        }
        dev_log(sys, 3, b"[cql] init".as_ptr(), 10);
        0
    }
}

unsafe fn emit_status(s: &mut CqlState, text: &[u8]) {
    let sys = &*s.syscalls;
    if s.status_out >= 0 {
        let poll = (sys.channel_poll)(s.status_out, 0x02);
        if poll > 0 && (poll as u32 & 0x02) != 0 {
            (sys.channel_write)(s.status_out, text.as_ptr(), text.len());
        }
    }
}

unsafe fn feed(s: &mut CqlState, sys: &SyscallTable, ev: CEv, now: u64) {
    let (action, next) = cql_transition(s.phase, ev);
    match action {
        CAct::Connect => {
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
        CAct::SendStartup => {
            let mut out = [0u8; REQ_BUF];
            if let Some(n) = cql_startup(&mut out) {
                s.req[..n].copy_from_slice(&out[..n]);
                s.req_len = n as u16;
                s.req_sent = 0;
                s.started_ms = now;
                s.acc_len = 0;
            }
        }
        CAct::SendAuthResponse => {
            let ul = s.user_len as usize;
            let pl = s.pass_len as usize;
            let mut u = [0u8; NAME_BUF];
            u[..ul].copy_from_slice(&s.user[..ul]);
            let mut pw = [0u8; NAME_BUF];
            pw[..pl].copy_from_slice(&s.pass[..pl]);
            let mut out = [0u8; REQ_BUF];
            if let Some(n) = cql_auth_response(&u[..ul], &pw[..pl], &mut out) {
                s.req[..n].copy_from_slice(&out[..n]);
                s.req_len = n as u16;
                s.req_sent = 0;
                s.started_ms = now;
            }
        }
        CAct::Fail => {
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
            emit_status(s, b"cassandra: connect failed\n");
        }
        CAct::None => {}
    }
    if next == CPhase::Ready && s.phase != CPhase::Ready {
        s.ready = s.ready.wrapping_add(1);
        emit_status(s, b"cassandra: session ready\n");
    }
    s.phase = next;
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut CqlState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        if s.phase == CPhase::Disconnected && s.draining == 0 && s.ep_hex_len > 0 {
            feed(s, sys, CEv::Start, now);
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
                match msg {
                    NET_MSG_CONNECTED if s.phase == CPhase::Connecting => {
                        if plen >= 2 && *payload.add(1) == s.tag {
                            s.conn_id = *payload;
                            feed(s, sys, CEv::Connected, now);
                        }
                    }
                    NET_MSG_DATA if s.phase != CPhase::Disconnected => {
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
                            // Process every complete frame. A `while let`
                            // here would hold the `s.acc` borrow from the
                            // scrutinee across the body (2021 temporary
                            // scope), colliding with the compaction below.
                            #[allow(
                                clippy::while_let_loop,
                                reason = "a while-let would hold the s.acc borrow from the scrutinee across the body's in-place compaction (2021 temporary scope)"
                            )]
                            loop {
                                let f = match cql_parse_frame(&s.acc[..s.acc_len as usize]) {
                                    Some(f) => f,
                                    None => break,
                                };
                                if let Some(ev) = cql_classify(f.opcode) {
                                    feed(s, sys, ev, now);
                                }
                                let total = f.total;
                                let rem = s.acc_len as usize - total;
                                let mut k = 0usize;
                                while k < rem {
                                    s.acc[k] = s.acc[total + k];
                                    k += 1;
                                }
                                s.acc_len = rem as u32;
                                if s.acc_len == 0 {
                                    break;
                                }
                            }
                        }
                    }
                    NET_MSG_CLOSED if s.phase != CPhase::Disconnected => {
                        if plen >= 1 && *payload == s.conn_id {
                            feed(s, sys, CEv::PeerClosed, now);
                        }
                    }
                    NET_MSG_ERROR => {
                        let ours = (s.phase == CPhase::Connecting
                            && plen >= 3
                            && *payload.add(2) == s.tag)
                            || (s.phase != CPhase::Disconnected
                                && plen >= 1
                                && *payload == s.conn_id);
                        if ours {
                            feed(s, sys, CEv::NetError, now);
                        }
                    }
                    _ => {}
                }
            }
        }

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

        if !matches!(s.phase, CPhase::Disconnected | CPhase::Ready) {
            let budget = if s.phase == CPhase::Connecting {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                feed(s, sys, CEv::NetError, now);
            }
        }

        if s.draining == 1 && matches!(s.phase, CPhase::Disconnected | CPhase::Ready) {
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
