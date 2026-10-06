//! exchange_cli — stdin chunks in, exchange requests out, answers as text.
//!
//! The requester the standalone client examples put in front of a
//! provider (`pg_client`, `redis_client`, `mongo_client`): see
//! `manifest.toml` for the ports.
//!
//! # Parameters
//!
//! | Tag | Name   | Type | Default  | Description                               |
//! |-----|--------|------|----------|-------------------------------------------|
//! | 1   | method | u32  | 3 (POST) | Exchange method constant of each request  |
//! | 2   | target | str  | (empty)  | Request target, up to 128 bytes           |
//!
//! A body that does not fit one request record is written to `text_out`
//! as `413 request too large` and never sent.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    clippy::duplicate_mod,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, ABI fns carry a fixed arity, and the PIC build #[path]-remounts shared SDK/common code"
)]
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

use abi::contracts::exchange as ex;

/// The longest request target the `target` param holds.
const TARGET_MAX: usize = 128;
/// The longest answer body collected for printing.
const ANSWER_MAX: usize = 8192;

#[repr(C)]
struct CliState {
    syscalls: *const SyscallTable,
    body_in: i32,
    response_in: i32,
    request_out: i32,
    text_out: i32,

    method: u8,
    target: [u8; TARGET_MAX],
    target_len: u16,

    /// The request record in flight (kept whole for a re-issue).
    req: [u8; ex::RECORD_MAX],
    req_len: u16,
    /// 0 idle, 1 to send, 2 sent and awaiting its answer, 3 held by a LINK DOWN.
    phase: u8,
    /// The provider's link is down: nothing is sent until LINK UP.
    down: u8,
    next_id: u64,
    id: ex::ExchangeId,

    status: u16,
    answer: [u8; ANSWER_MAX],
    answer_len: u16,
    /// The text line being written, and how much of it is out.
    line: [u8; ANSWER_MAX + 16],
    line_len: u16,

    rec: [u8; ex::RECORD_MAX],
    input: [u8; ex::RECORD_MAX],
}

define_params! {
    CliState;

    1, method, u32, 3
        => |s, d, len| { s.method = p_u32(d, len, 0, 3) as u8; };

    2, target, str, 0 => |s, d, len| {
        let n = len.min(TARGET_MAX);
        let mut i = 0usize;
        while i < n {
            s.target[i] = *d.add(i);
            i += 1;
        }
        s.target_len = n as u16;
    };
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<CliState>() as u32
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
    if state.is_null() || syscalls.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<CliState>() {
        return -2;
    }
    unsafe {
        let s = &mut *state.cast::<CliState>();
        let sys = &*syscalls.cast::<SyscallTable>();
        s.syscalls = sys;
        s.body_in = in_chan;
        s.request_out = out_chan;
        s.response_in = dev_channel_port(sys, 0, 1);
        s.text_out = dev_channel_port(sys, 1, 1);
        s.method = ex::METHOD_POST;
        s.target_len = 0;
        s.req_len = 0;
        s.phase = 0;
        s.down = 0;
        s.next_id = 1;
        s.answer_len = 0;
        s.line_len = 0;
        parse_tlv(s, params, params_len);
        if s.request_out < 0 || s.response_in < 0 {
            let m = b"[exchange_cli] refusing to construct: request_out and response_in must both be wired";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
    }
    0
}

unsafe fn readable(sys: &SyscallTable, chan: i32) -> bool {
    if chan < 0 {
        return false;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    poll > 0 && (poll as u32) & POLL_IN != 0
}

/// Write the pending text line, all or nothing.
unsafe fn flush_line(s: &mut CliState, sys: &SyscallTable) -> bool {
    if s.line_len == 0 {
        return true;
    }
    if s.text_out < 0 {
        s.line_len = 0;
        return true;
    }
    if (sys.channel_write)(s.text_out, s.line.as_ptr(), s.line_len as usize) > 0 {
        s.line_len = 0;
        return true;
    }
    false
}

/// Stage `status` and `body` as one text line.
fn stage_line(s: &mut CliState, status: u16, body_len: usize) {
    let mut n = 0usize;
    if status != ex::status::OK {
        let mut digits = [b'0'; 3];
        let mut v = status;
        for d in digits.iter_mut().rev() {
            *d = b'0' + (v % 10) as u8;
            v /= 10;
        }
        s.line[..3].copy_from_slice(&digits);
        s.line[3] = b' ';
        n = 4;
    }
    s.line[n..n + body_len].copy_from_slice(&s.answer[..body_len]);
    n += body_len;
    s.line[n] = b'\n';
    s.line_len = (n + 1) as u16;
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<CliState>() };
    let sys = unsafe { &*s.syscalls };
    unsafe {
        if !flush_line(s, sys) {
            return 0;
        }

        // Answers, LINK records.
        while readable(sys, s.response_in) {
            let n = (sys.channel_read)(s.response_in, s.rec.as_mut_ptr(), ex::RECORD_MAX);
            if n <= 0 {
                break;
            }
            // Offsets, not borrows: the record buffer is read again below.
            let base = s.rec.as_ptr() as usize;
            let (more, data_at, data_len) = match ex::parse_response(&s.rec[..n as usize]) {
                Some(ex::Record::Link { state }) => {
                    if state == ex::link::DOWN {
                        s.down = 1;
                        if s.phase == 2 {
                            s.phase = 3;
                        }
                    } else if state == ex::link::UP {
                        s.down = 0;
                        if s.phase == 3 {
                            s.phase = 1;
                        }
                    }
                    let text: &[u8] = if state == ex::link::DOWN {
                        b"LINK DOWN\n"
                    } else {
                        b"LINK UP\n"
                    };
                    s.line[..text.len()].copy_from_slice(text);
                    s.line_len = text.len() as u16;
                    break;
                }
                Some(ex::Record::Head(h)) if h.id == s.id && s.phase == 2 => {
                    s.status = h.status;
                    s.answer_len = 0;
                    let at = h.body.as_ptr() as usize - base;
                    (h.flags & ex::flag::MORE != 0, at, h.body.len())
                }
                Some(ex::Record::Body { id, flags, data }) if id == s.id && s.phase == 2 => {
                    let at = data.as_ptr() as usize - base;
                    (flags & ex::flag::MORE != 0, at, data.len())
                }
                Some(ex::Record::Abort { id, .. }) if id == s.id && s.phase == 2 => {
                    s.status = ex::status::BAD_GATEWAY;
                    s.answer_len = 0;
                    (false, 0, 0)
                }
                _ => continue,
            };
            let have = s.answer_len as usize;
            let take = data_len.min(ANSWER_MAX - have);
            let (rec, answer) = (&s.rec, &mut s.answer);
            answer[have..have + take].copy_from_slice(&rec[data_at..data_at + take]);
            s.answer_len = (have + take) as u16;
            if !more {
                s.phase = 0;
                stage_line(s, s.status, s.answer_len as usize);
                break;
            }
        }
        if !flush_line(s, sys) {
            return 0;
        }

        // The next body becomes the next request.
        if s.phase == 0 && readable(sys, s.body_in) {
            let n = (sys.channel_read)(s.body_in, s.input.as_mut_ptr(), ex::RECORD_MAX);
            if n > 0 {
                s.id = ex::ExchangeId::from_u64(s.next_id);
                s.next_id = s.next_id.wrapping_add(1);
                let head = ex::RequestHead {
                    id: s.id,
                    flags: 0,
                    method: s.method,
                    target: &s.target[..s.target_len as usize],
                    headers: &[],
                    peer: &[],
                    resp_credit: ANSWER_MAX as u32,
                    body: &s.input[..n as usize],
                };
                match ex::write_request_head(&head, &mut s.req) {
                    Some(len) => {
                        s.req_len = len as u16;
                        s.phase = 1;
                    }
                    None => {
                        s.answer_len = 0;
                        let msg = b"request too large";
                        s.answer[..msg.len()].copy_from_slice(msg);
                        stage_line(s, ex::status::TOO_LARGE, msg.len());
                    }
                }
            }
        }
        if s.phase == 1
            && s.down == 0
            && (sys.channel_write)(s.request_out, s.req.as_ptr(), s.req_len as usize) > 0
        {
            s.phase = 2;
        }
    }
    0
}
