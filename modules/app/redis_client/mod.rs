//! Redis connector — a GENUINE per-protocol Fluxor foundation module.
//!
//! Unlike a generic transport driven by encode/decode bytecode, this module owns
//! the Redis protocol as compiled code: a PERSISTENT connection (open across many
//! requests, not reconnect-per-call) and a multi-round-trip `AUTH` handshake
//! (send AUTH, branch on the server's `+OK`/`-ERR`, only then serve requests).
//! Those are reply-dependent, stateful behaviours a stateless codec cannot
//! express — which is the whole reason a real connector is a module.
//!
//! The protocol logic lives in the shared, host-tested `redis_core.rs`; this file
//! is the I/O pump that maps its actions onto net_proto frames.
//!
//! Ports:  net_in/net_out (transport), request_in (structured commands),
//!         reply_out (reply values).
//! Params: `endpoint` (hex `[ip:4][port:2 LE]`), `password` (optional; enables AUTH).
//! request_in frame: `[nargs:u8]` then per arg `[len:u16 LE][bytes]` — the module
//!         builds the RESP wire bytes, so upstream sends args, not protocol bytes.

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
include!("../../common/resp_core.rs");
include!("../../common/redis_core.rs");
include!("../../common/hex_core.rs");

const NET_CMD_SEND: u8 = 0x11;
const NET_CMD_CLOSE: u8 = 0x12;
const NET_CMD_CONNECT: u8 = 0x13;
const NET_MSG_DATA: u8 = 0x02;
const NET_MSG_CLOSED: u8 = 0x03;
const NET_MSG_CONNECTED: u8 = 0x05;
const NET_MSG_ERROR: u8 = 0x06;

const NET_BUF: usize = 2048;
const REQ_BUF: usize = 4096;
const REPLY_BUF: usize = 8192;
const PEND_BUF: usize = 8192;
const PASS_BUF: usize = 128;
const CONNECT_TIMEOUT_MS: u64 = 10_000;
const REPLY_TIMEOUT_MS: u64 = 15_000;

#[repr(C)]
struct RedisState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    request_in: i32,
    reply_out: i32,

    ip: [u8; 4],
    port: u16,
    ep_hex: [u8; 16],
    ep_hex_len: u16,
    password: [u8; PASS_BUF],
    password_len: u16,

    phase: Phase,
    conn_id: u8,
    tag: u8,
    started_ms: u64,
    draining: u8,

    // The structured command currently in flight (kept so it can be (re)sent
    // after the AUTH handshake completes), and whether one is queued.
    cmd: [u8; REQ_BUF],
    cmd_len: u16,
    has_cmd: u8,

    // Transient flush buffer: the RESP bytes currently being written (AUTH or the
    // encoded command).
    req: [u8; REQ_BUF],
    req_len: u16,
    req_sent: u16,

    reply: [u8; REPLY_BUF],
    reply_len: u16,

    pending: [u8; PEND_BUF],
    pending_len: u16,
    pending_off: u16,

    nbuf: [u8; NET_BUF],
    completed: u32,
    errors: u32,
    reconnects: u32,
}

define_params! {
    RedisState;

    // Endpoint: hex of [ip:4][port:2 LE]. The connector owns the endpoint.
    1, endpoint, str, 0 => |s, d, len| {
        let mut i = 0usize;
        while i < len && (s.ep_hex_len as usize) < 16 {
            s.ep_hex[s.ep_hex_len as usize] = *d.add(i);
            s.ep_hex_len += 1;
            i += 1;
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
        s.net_out = out_chan;
        s.request_in = dev_channel_port(sys, 0, 1);
        s.reply_out = dev_channel_port(sys, 1, 1);
        s.ip = [0u8; 4];
        s.port = 0;
        s.ep_hex_len = 0;
        s.password_len = 0;
        s.phase = Phase::Disconnected;
        s.conn_id = 0;
        s.tag = dev_requester_tag(sys);
        s.started_ms = 0;
        s.draining = 0;
        s.cmd_len = 0;
        s.has_cmd = 0;
        s.req_len = 0;
        s.req_sent = 0;
        s.reply_len = 0;
        s.pending_len = 0;
        s.pending_off = 0;
        s.completed = 0;
        s.errors = 0;
        s.reconnects = 0;
        parse_tlv(s, params, params_len);
        let mut ep = [0u8; 8];
        if let Some(n) = hex_decode(&s.ep_hex[..s.ep_hex_len as usize], &mut ep) {
            if n >= 6 {
                s.ip = [ep[0], ep[1], ep[2], ep[3]];
                s.port = u16::from_le_bytes([ep[4], ep[5]]);
            }
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

/// Feed one event into the state machine and execute the resulting action.
unsafe fn feed(s: &mut RedisState, sys: &SyscallTable, ev: Ev, now: u64) {
    let needs_auth = s.password_len > 0;
    let (action, next) = redis_transition(s.phase, ev, needs_auth);
    match action {
        Act::Connect => {
            // CONNECT payload: [sock_type][ip host-order (reversed octets)][port LE][tag].
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
            // Encode the queued structured command into the flush buffer.
            let mut tmp = [0u8; REQ_BUF];
            let encoded = {
                let src = &s.cmd[..cmd_len];
                resp_cmd_structured(src, &mut tmp)
            };
            match encoded {
                Some(n) => {
                    s.req[..n].copy_from_slice(&tmp[..n]);
                    s.req_len = n as u16;
                }
                None => s.req_len = 0,
            }
            s.req_sent = 0;
            s.started_ms = now;
        }
        Act::DeliverReply => {
            let r = resp_classify(&s.reply[..s.reply_len as usize]);
            if s.reply_out >= 0 && r.val_end <= s.reply_len as usize && r.val_end >= r.val_start {
                let poll = (sys.channel_poll)(s.reply_out, 0x02);
                if poll > 0 && (poll as u32 & 0x02) != 0 {
                    (sys.channel_write)(
                        s.reply_out,
                        s.reply.as_ptr().add(r.val_start),
                        r.val_end - r.val_start,
                    );
                }
            }
            s.reply_len = 0;
            s.has_cmd = 0;
            s.completed = s.completed.wrapping_add(1);
        }
        Act::FailRequest => {
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
            s.reply_len = 0;
            s.has_cmd = 0;
            s.conn_id = 0;
            s.errors = s.errors.wrapping_add(1);
        }
        Act::None => {}
    }
    if next == Phase::Disconnected && s.phase != Phase::Disconnected {
        s.reconnects = s.reconnects.wrapping_add(1);
    }
    s.phase = next;
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut RedisState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. Load the next structured command when none is in flight.
        if s.has_cmd == 0 && s.draining == 0 && s.request_in >= 0 {
            if s.pending_off >= s.pending_len {
                let poll = (sys.channel_poll)(s.request_in, 0x01);
                if poll > 0 && (poll as u32 & 0x01) != 0 {
                    let n = (sys.channel_read)(s.request_in, s.pending.as_mut_ptr(), PEND_BUF);
                    if n > 0 {
                        s.pending_len = n as u16;
                        s.pending_off = 0;
                    }
                }
            }
            let base = s.pending_off as usize;
            let plen = s.pending_len as usize;
            if base < plen {
                // One structured command: [nargs:u8] then [len:u16][bytes] per arg.
                if let Some(clen) = structured_len(&s.pending[base..plen]) {
                    if clen <= REQ_BUF {
                        s.cmd[..clen].copy_from_slice(&s.pending[base..base + clen]);
                        s.cmd_len = clen as u16;
                        s.has_cmd = 1;
                        s.pending_off = (base + clen) as u16;
                    } else {
                        s.pending_off = s.pending_len; // oversized: skip the batch
                    }
                } else {
                    s.pending_off = s.pending_len; // malformed: drop the batch
                }
            }
        }

        // 2. A queued command drives connect (if disconnected) or send (if ready).
        if s.has_cmd == 1 && matches!(s.phase, Phase::Disconnected | Phase::Ready) {
            feed(s, sys, Ev::HaveRequest, now);
        }

        // 3. Drain network events into the state machine.
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
                    NET_MSG_CONNECTED if s.phase == Phase::Connecting => {
                        if plen >= 2 && *payload.add(1) == s.tag {
                            s.conn_id = *payload;
                            feed(s, sys, Ev::Connected, now);
                        }
                    }
                    NET_MSG_DATA if s.phase == Phase::Authing || s.phase == Phase::Awaiting => {
                        if plen > 1 && *payload == s.conn_id {
                            let data_len = plen - 1;
                            let space = REPLY_BUF - s.reply_len as usize;
                            let take = if data_len < space { data_len } else { space };
                            core::ptr::copy_nonoverlapping(
                                payload.add(1),
                                s.reply.as_mut_ptr().add(s.reply_len as usize),
                                take,
                            );
                            s.reply_len += take as u16;
                            let r = resp_classify(&s.reply[..s.reply_len as usize]);
                            if r.kind != RespKind::Incomplete {
                                let ev = if r.is_error() {
                                    Ev::ReplyErr
                                } else {
                                    Ev::ReplyOk
                                };
                                feed(s, sys, ev, now);
                            }
                        }
                    }
                    NET_MSG_CLOSED if s.phase != Phase::Disconnected => {
                        if plen >= 1 && *payload == s.conn_id {
                            feed(s, sys, Ev::PeerClosed, now);
                        }
                    }
                    NET_MSG_ERROR => {
                        let ours =
                            (s.phase == Phase::Connecting && plen >= 3 && *payload.add(2) == s.tag)
                                || (s.phase != Phase::Disconnected
                                    && plen >= 1
                                    && *payload == s.conn_id);
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

        // 5. Timeouts on any active exchange.
        if matches!(
            s.phase,
            Phase::Connecting | Phase::Authing | Phase::Awaiting
        ) {
            let budget = if s.phase == Phase::Connecting {
                CONNECT_TIMEOUT_MS
            } else {
                REPLY_TIMEOUT_MS
            };
            if now.wrapping_sub(s.started_ms) > budget {
                feed(s, sys, Ev::NetError, now);
            }
        }

        // 6. Drain: once no work remains, close any idle socket and report Done.
        if s.draining == 1
            && s.has_cmd == 0
            && s.pending_off >= s.pending_len
            && matches!(s.phase, Phase::Disconnected | Phase::Ready)
        {
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
            return 1; // StepOutcome::Done
        }
        0
    }
}

/// Byte length of one structured command `[nargs:u8]{[len:u16][bytes]}` at the
/// start of `buf`, or `None` if truncated/malformed. Lets the step loop split a
/// batch of concatenated commands.
fn structured_len(buf: &[u8]) -> Option<usize> {
    let nargs = *buf.first()? as usize;
    let mut off = 1usize;
    for _ in 0..nargs {
        let lo = *buf.get(off)?;
        let hi = *buf.get(off + 1)?;
        let alen = u16::from_le_bytes([lo, hi]) as usize;
        off += 2 + alen;
        if off > buf.len() {
            return None;
        }
    }
    Some(off)
}
