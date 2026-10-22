//! exchange_cli — stdin chunks in, exchange requests out, answers as text.
//!
//! The requester the standalone client examples put in front of a
//! provider (`pg_client`, `redis_client`, `mongo_client`, and lattice's
//! in-graph fronts): see `manifest.toml` for the ports.
//!
//! # Parameters
//!
//! | Tag | Name        | Type | Default  | Description                               |
//! |-----|-------------|------|----------|-------------------------------------------|
//! | 1   | method      | u32  | 3 (POST) | Exchange method constant of each request  |
//! | 2   | target      | str  | (empty)  | Request target, up to 128 bytes           |
//! | 3   | headers     | str  | (empty)  | Request header block, `name: value` lines each ended `\r\n`, up to 256 bytes |
//! | 4   | hold        | u32  | 0        | 1: send one request at start, with no body, and read a held answer |
//! | 5   | abort_after | u32  | 0        | With `hold`: ABORT the exchange after this many BODY records (0: never) |
//! | 6   | select      | str  | (empty)  | Take only stdin chunks that begin with these bytes (up to 8), the prefix dropped from the body — so one stdin can feed several requesters |
//!
//! A `target`, `headers` or `select` past its bound refuses construction. A
//! body that does not fit one request record is written to `text_out` as
//! `413 request too large` and never sent. The module grants 8192 bytes of
//! response credit per request, so an answer is at most that.
//!
//! # Held answers
//!
//! With `hold: 1` the module issues its one request when it starts and
//! prints a held answer as it streams: its HEAD as `HOLD <status>` and its
//! header lines (one space between them), every BODY record as `BODY ` and the
//! record's bytes in hex, a provider ABORT as `ABORT <reason>`, and the
//! ABORT it sends itself after `abort_after` records as `ABORTED`. Each
//! BODY's bytes are granted back as CREDIT once printed, so the window the
//! HEAD opened stays open. An answer that is not held prints as any other.

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
/// The longest request header block the `headers` param holds.
const HEADERS_MAX: usize = 256;
/// The longest `select` prefix.
const SELECT_MAX: usize = 8;
/// The longest text line: a whole record in hex, and its label.
const LINE_MAX: usize = 2 * ex::RECORD_MAX + 16;

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
    headers: [u8; HEADERS_MAX],
    headers_len: u16,
    /// A string param past its bound: the module refuses to construct.
    bad_param: u8,
    /// `select` param: the prefix a stdin chunk carries to be this module's.
    select: [u8; SELECT_MAX],
    select_len: u8,
    /// `hold` param: one request at start, its answer read as held.
    hold: u8,
    abort_after: u32,
    /// BODY records of the held answer printed so far.
    bodies: u32,
    /// Response-body bytes printed and owed back to the provider as CREDIT.
    regrant: u32,

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
    line: [u8; LINE_MAX],
    line_len: u16,

    rec: [u8; ex::RECORD_MAX],
    input: [u8; ex::RECORD_MAX],
}

define_params! {
    CliState;

    1, method, u32, 3
        => |s, d, len| { s.method = p_u32(d, len, 0, 3) as u8; };

    2, target, str, 0 => |s, d, len| {
        if len > TARGET_MAX {
            s.bad_param = 1;
        }
        let n = len.min(TARGET_MAX);
        let mut i = 0usize;
        while i < n {
            s.target[i] = *d.add(i);
            i += 1;
        }
        s.target_len = n as u16;
    };

    3, headers, str, 0 => |s, d, len| {
        if len > HEADERS_MAX {
            s.bad_param = 1;
        }
        let n = len.min(HEADERS_MAX);
        let mut i = 0usize;
        while i < n {
            s.headers[i] = *d.add(i);
            i += 1;
        }
        s.headers_len = n as u16;
    };

    4, hold, u32, 0
        => |s, d, len| { s.hold = p_u32(d, len, 0, 0) as u8; };

    5, abort_after, u32, 0
        => |s, d, len| { s.abort_after = p_u32(d, len, 0, 0); };

    6, select, str, 0 => |s, d, len| {
        if len > SELECT_MAX {
            s.bad_param = 1;
        }
        let n = len.min(SELECT_MAX);
        let mut i = 0usize;
        while i < n {
            s.select[i] = *d.add(i);
            i += 1;
        }
        s.select_len = n as u8;
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
        s.headers_len = 0;
        s.select_len = 0;
        s.bad_param = 0;
        s.hold = 0;
        s.abort_after = 0;
        s.bodies = 0;
        s.regrant = 0;
        s.req_len = 0;
        s.phase = 0;
        s.down = 0;
        s.next_id = 1;
        s.answer_len = 0;
        s.line_len = 0;
        parse_tlv(s, params, params_len);
        if s.bad_param == 1 {
            let m =
                b"[exchange_cli] refusing to construct: target, headers or select past its bound";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if s.hold == 1 {
            // The one request goes out at once, with no body.
            stage_request(s, 0);
        }
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
        n = put_status(&mut s.line, status);
        s.line[3] = b' ';
        n += 1;
    }
    s.line[n..n + body_len].copy_from_slice(&s.answer[..body_len]);
    n += body_len;
    s.line[n] = b'\n';
    s.line_len = (n + 1) as u16;
}

/// Whether `head` is the `select` prefix. A plain loop: slice equality
/// lowers to a `bcmp` a PIC module cannot link.
fn selected(head: &[u8], select: &[u8]) -> bool {
    let mut i = 0usize;
    while i < select.len() {
        if head[i] != select[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// `status` as three digits at the start of `out`.
fn put_status(out: &mut [u8], status: u16) -> usize {
    let mut v = status;
    for d in out[..3].iter_mut().rev() {
        *d = b'0' + (v % 10) as u8;
        v /= 10;
    }
    3
}

/// Append `text` to the staged line.
fn line_push(s: &mut CliState, text: &[u8]) {
    let n = s.line_len as usize;
    let take = text.len().min(LINE_MAX - n);
    s.line[n..n + take].copy_from_slice(&text[..take]);
    s.line_len = (n + take) as u16;
}

/// Stage a held answer's HEAD: `HOLD <status>` and its header lines.
fn stage_hold(s: &mut CliState, status: u16, hdr_at: usize, hdr_len: usize) {
    s.line_len = 0;
    line_push(s, b"HOLD ");
    let mut digits = [0u8; 3];
    put_status(&mut digits, status);
    line_push(s, &digits);
    let mut i = 0usize;
    while i < hdr_len {
        let c = s.rec[hdr_at + i];
        if c == b'\r' {
            i += 1;
            continue;
        }
        if i == 0 {
            line_push(s, b" ");
        }
        // Each line's end is a space; the block's last, nothing.
        if c != b'\n' {
            line_push(s, &[c]);
        } else if i + 1 < hdr_len {
            line_push(s, b" ");
        }
        i += 1;
    }
    line_push(s, b"\n");
}

/// Stage one BODY record of a held answer: `BODY ` and its bytes in hex.
fn stage_body_hex(s: &mut CliState, at: usize, len: usize) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    s.line_len = 0;
    line_push(s, b"BODY ");
    let mut n = s.line_len as usize;
    let mut i = 0usize;
    while i < len && n + 3 <= LINE_MAX {
        let b = s.rec[at + i];
        s.line[n] = HEX[(b >> 4) as usize];
        s.line[n + 1] = HEX[(b & 0x0F) as usize];
        n += 2;
        i += 1;
    }
    s.line[n] = b'\n';
    s.line_len = (n + 1) as u16;
}

/// Build the next request — `body_len` bytes of `input` as its body — into
/// `req`, ready to send; one that does not fit is reported, never sent.
fn stage_request(s: &mut CliState, body_len: usize) {
    s.id = ex::ExchangeId::from_u64(s.next_id);
    s.next_id = s.next_id.wrapping_add(1);
    let head = ex::RequestHead {
        id: s.id,
        flags: 0,
        method: s.method,
        target: &s.target[..s.target_len as usize],
        headers: &s.headers[..s.headers_len as usize],
        peer: &[],
        resp_credit: ANSWER_MAX as u32,
        body: &s.input[..body_len],
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

/// With `hold`: end the held exchange once `abort_after` BODY records are
/// printed, and grant back what was printed. Returns false while a record
/// it owes could not be placed.
unsafe fn settle_held(s: &mut CliState, sys: &SyscallTable) -> bool {
    if s.phase != 2 {
        return true;
    }
    if s.abort_after != 0 && s.bodies >= s.abort_after {
        let mut rec = [0u8; ex::HDR + 1];
        let Some(n) = ex::write_abort(&s.id, ex::abort::PEER_GONE, &mut rec) else {
            return true;
        };
        if (sys.channel_write)(s.request_out, rec.as_ptr(), n) <= 0 {
            return false;
        }
        s.phase = 0;
        s.regrant = 0;
        let text = b"ABORTED\n";
        s.line[..text.len()].copy_from_slice(text);
        s.line_len = text.len() as u16;
        return true;
    }
    if s.regrant > 0 {
        let mut rec = [0u8; ex::HDR + 4];
        if let Some(n) = ex::write_credit(&s.id, s.regrant, &mut rec) {
            if (sys.channel_write)(s.request_out, rec.as_ptr(), n) <= 0 {
                return false;
            }
        }
        s.regrant = 0;
    }
    true
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
        if !settle_held(s, sys) || !flush_line(s, sys) {
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
                    if s.hold == 1 && h.flags & ex::flag::HOLD != 0 {
                        let (status, at, len) = (
                            h.status,
                            h.headers.as_ptr() as usize - base,
                            h.headers.len(),
                        );
                        stage_hold(s, status, at, len);
                        break;
                    }
                    s.status = h.status;
                    s.answer_len = 0;
                    let at = h.body.as_ptr() as usize - base;
                    (h.flags & ex::flag::MORE != 0, at, h.body.len())
                }
                Some(ex::Record::Body { id, flags, data }) if id == s.id && s.phase == 2 => {
                    let at = data.as_ptr() as usize - base;
                    if s.hold == 1 {
                        let len = data.len();
                        stage_body_hex(s, at, len);
                        s.bodies = s.bodies.wrapping_add(1);
                        s.regrant = s.regrant.saturating_add(len as u32);
                        if flags & ex::flag::MORE == 0 {
                            s.phase = 0;
                        }
                        break;
                    }
                    (flags & ex::flag::MORE != 0, at, data.len())
                }
                Some(ex::Record::Abort { id, reason }) if id == s.id && s.phase == 2 => {
                    if s.hold == 1 {
                        s.phase = 0;
                        s.line_len = 0;
                        line_push(s, b"ABORT ");
                        let mut digits = [0u8; 3];
                        put_status(&mut digits, u16::from(reason));
                        line_push(s, &digits);
                        line_push(s, b"\n");
                        break;
                    }
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
        if s.hold == 0 && s.phase == 0 && readable(sys, s.body_in) {
            let n = (sys.channel_read)(s.body_in, s.input.as_mut_ptr(), ex::RECORD_MAX);
            let sel = s.select_len as usize;
            if n > 0 && (n as usize) >= sel && selected(&s.input[..sel], &s.select[..sel]) {
                s.input.copy_within(sel..n as usize, 0);
                stage_request(s, n as usize - sel);
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
