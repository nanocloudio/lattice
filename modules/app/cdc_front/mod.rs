//! cdc_front — the CDC feed as held exchange subscriptions.
//!
//! The PULL form of the change feed, beside `cdc_pump`'s PUSH form. The
//! pump is a REQUESTER: one feed per instance, durably checkpointed, each
//! event published to a `stream.ordered_ack` sink and retried until the sink
//! accepts it. This module is a PROVIDER: any requester in the graph opens a
//! subscription with a GET on a feed target, holds it as long as it wants,
//! and is sent one BODY per change under its own credit; the subscriber owns
//! its position and resumes from it with an `after` header. Both read the
//! same feed through the same router surface and carry each change in the
//! same envelope, byte for byte.
//!
//! This file is deliberately thin: channels, params, the syscall table. The
//! subscription protocol — the request, the statuses, credit, resume, window
//! narrowing — lives in `modules/common/cdc_subscribe.rs`
//! ([`cdc_subscribe::Subscriptions`]), which documents the surface and which
//! the host suite drives against a simulated provider
//! (`tests/harness/tests/contract_cdc_subscribe.rs`).
//!
//! # Ports
//!
//! The exchange provider pair `request_in` (`ExchangeRequest`) /
//! `response_out` (`ExchangeResponse`), and a router anchor pair: `kv_out`
//! carries `MSG_KV_REQUEST` (stamped with the `proto` param) into a free
//! router pair's input, `kv_in` takes `MSG_KV_RESPONSE` from its output. A
//! module with the router pair unwired, or `request_in` wired and
//! `response_out` not, refuses to construct.
//!
//! # Params
//!
//! | Tag | Name | Type | Default | Description |
//! |---|---|---|---|---|
//! | 1 | feed_id | u32 | 1 | The envelope's `feed_id` when a request names none (`cdc_pump`'s default) |
//! | 2 | table_id | u32 | 0 | The envelope's `table_id` when a request names none; 0 for raw-KV feeds |
//! | 3 | proto | u32 | 1 | Protocol byte of the wired router pair (1 = the etcd pair), as `cdc_pump`'s |

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

#[path = "../../common/cdc_subscribe.rs"]
mod cdc_subscribe;
use cdc_subscribe::exchange::RECORD_MAX;
use cdc_subscribe::wire::{FRAME_PAYLOAD_MAX, MSG_KV_RESPONSE};
use cdc_subscribe::{SubscribeIo, Subscriptions};

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox, ENVELOPE_HDR};

/// The `MSG_KV_RESPONSE` head: corr, conn, result, revision, body length.
const KV_RESP_HEAD: usize = 8 + 1 + 1 + 8 + 2;
/// One incoming KV response, whole.
const KV_IN_MAX: usize = ENVELOPE_HDR + FRAME_PAYLOAD_MAX;
/// Records written per step: a step's work stays bounded while a
/// subscriber with credit is being caught up.
const RECORDS_PER_STEP: usize = 16;

#[repr(C)]
struct FrontState {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    kv_in: i32,
    kv_out: i32,
    /// `kv_in`'s envelope reading (`common/envelope.rs`).
    kv_inbox: Inbox,
    draining: u8,
    outbox: ExchangeOutbox,
    subs: Subscriptions,
    kv_buf: [u8; KV_IN_MAX],
    // The request record being read, and the response record being written.
    rec: [u8; RECORD_MAX],
    out: [u8; RECORD_MAX],
}

define_params! {
    FrontState;

    1, feed_id, u32, 1
        => |s, d, len| { s.subs.feed_id = u64::from(p_u32(d, len, 0, 1)); };

    2, table_id, u32, 0
        => |s, d, len| { s.subs.table_id = p_u32(d, len, 0, 0); };

    3, proto, u32, 1
        => |s, d, len| { s.subs.proto = p_u32(d, len, 0, 1) as u8; };
}

/// [`SubscribeIo`] over the kernel channel syscalls.
struct ShellIo<'a> {
    sys: &'a SyscallTable,
    kv_out: i32,
}

impl SubscribeIo for ShellIo<'_> {
    fn send_kv(&mut self, frame: &mut [u8]) -> bool {
        // SAFETY: `frame` is a live, exclusively borrowed buffer of exactly
        // `frame.len()` bytes; the kernel write only reads it.
        unsafe {
            (self.sys.channel_write)(self.kv_out, frame.as_mut_ptr(), frame.len())
                == frame.len() as i32
        }
    }
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
        if state_size < core::mem::size_of::<FrontState>() {
            return -2;
        }
        let s = &mut *(state as *mut FrontState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        // inputs: request_in[0], kv_in[1]; outputs: response_out[0], kv_out[1]
        s.request_in = in_chan;
        s.response_out = out_chan;
        s.kv_in = dev_channel_port(sys, 0, 1);
        s.kv_out = dev_channel_port(sys, 1, 1);
        s.kv_inbox = Inbox::new();
        s.draining = 0;
        s.outbox = ExchangeOutbox::new();
        s.subs.reset();
        s.subs.feed_id = 1;
        s.subs.table_id = 0;
        s.subs.proto = 1;
        parse_tlv(s, params, params_len);
        if s.kv_in < 0 || s.kv_out < 0 {
            let m = b"[cdc_front] refusing to construct: kv_in and kv_out must both be wired to a kv_request_router pair";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if s.request_in >= 0 && s.response_out < 0 {
            let m =
                b"[cdc_front] refusing to construct: request_in is wired but response_out is not";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        dev_log(sys, 3, b"[cdc_front] init".as_ptr(), 16);
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

/// Place records until the port is full, nothing can be written, or the
/// step's budget is spent.
unsafe fn flush_records(s: &mut FrontState, now: u64) {
    let sys = &*s.syscalls;
    if s.response_out < 0 {
        return;
    }
    for _ in 0..RECORDS_PER_STEP {
        if !s.outbox.flush(sys, s.response_out, &s.out) {
            return;
        }
        let Some(n) = s.subs.next_record(now, &mut s.out) else {
            return;
        };
        if !s.outbox.send(sys, s.response_out, &s.out, n) {
            return;
        }
    }
}

/// Read request records while the subscriptions can take them.
unsafe fn read_requests(s: &mut FrontState, now: u64) {
    let sys = &*s.syscalls;
    if s.request_in < 0 {
        return;
    }
    while s.subs.can_take() {
        let poll = (sys.channel_poll)(s.request_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.request_in, s.rec.as_mut_ptr(), RECORD_MAX);
        if n <= 0 {
            return;
        }
        s.subs.accept(&s.rec[..n as usize], now);
    }
}

/// Hand the router's answers to the subscriptions.
unsafe fn read_answers(s: &mut FrontState, now: u64) {
    let sys = &*s.syscalls;
    while let Some((mt, len)) = read_envelope(&mut s.kv_inbox, sys, s.kv_in, &mut s.kv_buf) {
        if mt != MSG_KV_RESPONSE || len < KV_RESP_HEAD {
            continue;
        }
        let b = &s.kv_buf[..len];
        let corr = u64::from_le_bytes(b[0..8].try_into().unwrap_or([0; 8]));
        let result = b[9];
        let revision = u64::from_le_bytes(b[10..18].try_into().unwrap_or([0; 8]));
        let body_len = u16::from_le_bytes([b[18], b[19]]) as usize;
        let Some(body) = b.get(KV_RESP_HEAD..KV_RESP_HEAD + body_len) else {
            continue;
        };
        s.subs.on_kv_response(now, corr, result, revision, body);
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut FrontState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. Records owed first.
        flush_records(s, now);
        if s.draining == 1 {
            s.subs.drain();
        }

        // 2. Requests in, the router's answers in, the next feed read out.
        read_requests(s, now);
        read_answers(s, now);
        let mut io = ShellIo {
            sys,
            kv_out: s.kv_out,
        };
        s.subs.step(&mut io, now);

        flush_records(s, now);

        // 3. Drain: once nothing is held or owed, report Done.
        if s.draining == 1 && s.subs.idle() && !s.outbox.holding() {
            return 1;
        }
        0
    }
}
