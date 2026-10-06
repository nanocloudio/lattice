//! loopback_sink — reference `stream.ordered_ack.sink` provider. See
//! manifest.toml for the contract stance; the records are the fluxor
//! SDK's exchange contract, the event envelope it checks lives in
//! `modules/common/cdc_wire.rs`, and the provider half (collecting,
//! refusing, LINK) in `modules/common/query_exchange.rs`.
//!
//! The module is deliberately the SIMPLEST thing that conforms:
//! validate, count, digest, answer 200 in order. Its one non-trivial
//! feature is scripted link loss (`linkdown_every`) so the pump's
//! re-issue path is exercised in CI rather than trusted.
//!
//! Request: METHOD_PUBLISH; `target` = the ordering key (1..=512 bytes,
//! or any length with `flag::BROADCAST`); body = the record (up to 8192
//! bytes). Answer: 200 with an empty body on acceptance; 400 for another
//! method or flag, or an empty target without BROADCAST; 413 past a
//! bound; 503 when both collecting slots are taken.

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

#[path = "../../common/cdc_wire.rs"]
mod cdc_wire;

#[path = "../../common/query_exchange.rs"]
mod query_exchange;
use query_exchange::exchange::{flag, status, KEY_MAX, METHOD_PUBLISH, PAYLOAD_MAX, RECORD_MAX};
use query_exchange::QueryDesk;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use cdc_wire::{
    resolved_frontier, CdcEvent, CDC_FLAG_BACKFILL, CDC_KIND_DELETE, CDC_KIND_PUT,
    CDC_KIND_RESOLVED,
};

/// Request records read per step — bounded, never a drain-until-empty.
const PER_STEP_BUDGET: u32 = 8;

/// Publishes collected at once.
const SLOTS: usize = 2;
const QUEUE: usize = 6;

const EMIT_EVERY: u64 = 5000;

#[repr(C)]
struct SinkState {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    metrics_out: i32,

    // ── params ──
    /// Write LINK DOWN after every N acceptances (0 = never).
    linkdown_every: u32,
    /// Steps the link stays down before LINK UP.
    linkdown_hold_steps: u32,
    /// Log every delivered event (`[sink] dlv …`) so an end-to-end
    /// test can assert exact delivery from the boot log. 0 = off.
    log_deliveries: u32,

    // ── link-loss script state ──
    acks_since_down: u32,
    down_steps_left: u32,
    /// The scripted outage is due once the last answer has left.
    outage_due: bool,

    // ── counters (manifest [observability] order) ──
    m_delivered: u64,
    m_acked: u64,
    m_refused: u64,
    m_dropped_down: u64,
    m_linkdowns: u64,
    m_bytes: u64,
    /// Rolling FNV-1a over every delivered (target, body) — the content
    /// digest end-to-end tests compare.
    m_key_hash: u64,
    /// Envelope contract violations (see `check_envelope`): a
    /// streaming data event delivered at or below the highest resolved
    /// frontier already delivered. Must stay 0; the live suite asserts
    /// on the `[sink] VIOLATION` log line this counts.
    m_violations: u64,
    /// Highest resolved-watermark frontier delivered so far.
    max_resolved: u64,
    step_ctr: u64,

    desk: QueryDesk<SLOTS, KEY_MAX, PAYLOAD_MAX, QUEUE>,
    outbox: ExchangeOutbox,
    // The request record being read, and the response record being written.
    rec: [u8; RECORD_MAX],
    out: [u8; RECORD_MAX],
}

impl SinkState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.request_in = -1;
        self.response_out = -1;
        self.metrics_out = -1;
        self.linkdown_every = 0;
        self.linkdown_hold_steps = 50;
        self.log_deliveries = 0;
        self.acks_since_down = 0;
        self.down_steps_left = 0;
        self.outage_due = false;
        self.m_delivered = 0;
        self.m_acked = 0;
        self.m_refused = 0;
        self.m_dropped_down = 0;
        self.m_linkdowns = 0;
        self.m_bytes = 0;
        self.m_key_hash = 0xcbf2_9ce4_8422_2325;
        self.m_violations = 0;
        self.max_resolved = 0;
        self.step_ctr = 0;
        self.desk.reset(METHOD_PUBLISH, flag::BROADCAST);
        // The backend is the module itself: up from the start, and LINK
        // reports only a change, so nothing is written for it.
        self.desk.link_up();
        self.outbox = ExchangeOutbox::new();
    }
}

define_params! {
    SinkState;

    // Scripted link loss: write LINK DOWN after every N acceptances; 0 = a
    // permanently healthy link.
    1, linkdown_every, u32, 0
        => |s, d, len| { s.linkdown_every = p_u32(d, len, 0, 0); };

    // Steps the link stays down before LINK UP re-opens it.
    2, linkdown_hold_steps, u32, 50
        => |s, d, len| { s.linkdown_hold_steps = p_u32(d, len, 0, 50); };

    // Log every delivered event to the boot log (test witness).
    3, log_deliveries, u32, 0
        => |s, d, len| { s.log_deliveries = p_u32(d, len, 0, 0); };
}

fn fnv_fold(h: u64, bytes: &[u8]) -> u64 {
    let mut h = h;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Append `v` as decimal digits at `buf[at..]`, returning the new end.
fn put_dec(buf: &mut [u8], at: usize, v: u64) -> usize {
    let mut digits = [0u8; 20];
    let mut d = 0;
    let mut x = v;
    loop {
        digits[d] = b'0' + (x % 10) as u8;
        x /= 10;
        d += 1;
        if x == 0 {
            break;
        }
    }
    let mut n = at;
    while d > 0 {
        d -= 1;
        buf[n] = digits[d];
        n += 1;
    }
    n
}

/// Delivery-time envelope check + optional delivery log. The reference
/// sink doubles as the contract's independent witness: it decodes each
/// payload as a `CdcEvent` and enforces the consumer-side watermark
/// rule — once `resolved(T)` has been delivered, no STREAMING put or
/// delete at or below T may arrive (backfill events are exempt: a
/// lapse-relatch legitimately re-delivers old timestamps under the
/// `CDC_FLAG_BACKFILL` flag). A violation is counted AND logged so the
/// live suite can assert its absence from the boot log alone.
unsafe fn check_envelope(s: &mut SinkState, sys: &SyscallTable, target: &[u8], body: &[u8]) {
    let Some(ev) = CdcEvent::decode(body) else {
        // Not a CDC envelope — the port contract is generic ordered
        // acceptance, so opaque bodies pass through unchecked.
        return;
    };
    if ev.kind == CDC_KIND_RESOLVED {
        if let Some(f) = resolved_frontier(&ev) {
            if f > s.max_resolved {
                s.max_resolved = f;
            }
        }
    } else if (ev.kind == CDC_KIND_PUT || ev.kind == CDC_KIND_DELETE)
        && ev.flags & CDC_FLAG_BACKFILL == 0
        && s.max_resolved > 0
        && ev.commit_ts <= s.max_resolved
    {
        s.m_violations = s.m_violations.wrapping_add(1);
        let mut m = *b"[sink] VIOLATION ts=00000000000000000000 wm=00000000000000000000";
        let n = put_dec(&mut m, 20, ev.commit_ts);
        // Rewrite the wm section right after the ts digits.
        m[n..n + 4].copy_from_slice(b" wm=");
        let n2 = put_dec(&mut m, n + 4, s.max_resolved);
        dev_log(sys, 2, m.as_ptr(), n2);
    }
    if s.log_deliveries != 0 {
        // `[sink] dlv <kind> <hex target (≤48 B)> ts=<dec>` — the
        // exact-delivery witness the live suite greps for.
        let mut m = [0u8; 160];
        let head = b"[sink] dlv ";
        m[..head.len()].copy_from_slice(head);
        let mut n = head.len();
        m[n] = b'0' + ev.kind.min(9);
        n += 1;
        m[n] = b' ';
        n += 1;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for &b in target.iter().take(48) {
            m[n] = HEX[(b >> 4) as usize];
            m[n + 1] = HEX[(b & 0x0F) as usize];
            n += 2;
        }
        m[n..n + 4].copy_from_slice(b" ts=");
        n = put_dec(&mut m, n + 4, ev.commit_ts);
        dev_log(sys, 2, m.as_ptr(), n);
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<SinkState>() as u32
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
    if state_size < core::mem::size_of::<SinkState>() {
        return -1;
    }
    unsafe {
        let s = &mut *state.cast::<SinkState>();
        let sys = &*syscalls.cast::<SyscallTable>();
        s.init(sys);
        s.request_in = in_chan;
        s.response_out = out_chan;
        s.metrics_out = dev_channel_port(sys, 1, 1);
        parse_tlv(s, params, params_len);
        if s.request_in < 0 || s.response_out < 0 {
            let m = b"[sink] refusing to construct: request_in and response_out must both be wired";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
    }
    0
}

/// Place owed response records until the port is full or nothing is owed.
unsafe fn flush_answers(s: &mut SinkState, sys: &SyscallTable) {
    loop {
        if !s.outbox.flush(sys, s.response_out, &s.out) {
            return;
        }
        // Every answer here carries an empty body.
        let Some(n) = s.desk.next_record(&[], &mut s.out) else {
            return;
        };
        if !s.outbox.send(sys, s.response_out, &s.out, n) {
            return;
        }
    }
}

/// Accept every complete publish: validate, count, digest, answer 200.
unsafe fn deliver(s: &mut SinkState, sys: &SyscallTable) {
    while let Some(at) = s.desk.next_request() {
        let Some(r) = s.desk.request(at) else {
            return;
        };
        let broadcast = r.flags & flag::BROADCAST != 0;
        if r.target.is_empty() && !broadcast {
            // No ordering key: a refusal, never a silent drop.
            s.desk.refuse(at, status::BAD_REQUEST);
            s.m_refused = s.m_refused.wrapping_add(1);
            continue;
        }
        // "Durable acceptance" for the loopback backend IS this
        // bookkeeping — the strongest level this backend offers.
        let mut target = [0u8; KEY_MAX];
        let tl = r.target.len();
        target[..tl].copy_from_slice(r.target);
        let body_len = r.body.len();
        s.m_key_hash = fnv_fold(fnv_fold(s.m_key_hash, r.target), r.body);
        s.m_delivered = s.m_delivered.wrapping_add(1);
        s.m_bytes = s.m_bytes.wrapping_add(body_len as u64);
        // The envelope check reads the body from the slot before it frees.
        let mut body = [0u8; PAYLOAD_MAX];
        body[..body_len].copy_from_slice(r.body);
        s.desk.start(at);
        check_envelope(s, sys, &target[..tl], &body[..body_len]);
        s.desk.finish(status::OK, 0);
        s.m_acked = s.m_acked.wrapping_add(1);
        flush_answers(s, sys);
        if s.linkdown_every != 0 {
            s.acks_since_down += 1;
            if s.acks_since_down >= s.linkdown_every {
                s.acks_since_down = 0;
                // Scripted outage AFTER this answer leaves: every exchange
                // the pump has open beyond it becomes unknowable.
                s.outage_due = true;
                return;
            }
        }
        if s.desk.busy() {
            return;
        }
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<SinkState>() };
    let sys_ptr = s.syscalls;
    if sys_ptr.is_null() {
        return 0;
    }
    let sys = unsafe { &*sys_ptr };

    unsafe {
        flush_answers(s, sys);

        // Link lifecycle: a due outage starts once the answer before it has
        // left; an outage counts down and re-opens.
        if s.outage_due && !s.desk.busy() {
            s.outage_due = false;
            if s.desk.link_down() {
                s.down_steps_left = s.linkdown_hold_steps.max(1);
                s.m_linkdowns = s.m_linkdowns.wrapping_add(1);
            }
        }
        if s.desk.link_is_down() {
            if s.down_steps_left > 0 {
                s.down_steps_left -= 1;
            }
            if s.down_steps_left == 0 {
                s.desk.link_up();
                let m = b"[sink] LINK_UP announced";
                dev_log(sys, 2, m.as_ptr(), m.len());
            }
        }
        flush_answers(s, sys);

        let mut budget = PER_STEP_BUDGET;
        while budget > 0 {
            budget -= 1;
            let discarding = s.desk.discarding();
            if !discarding && (!s.desk.can_take() || s.outage_due) {
                break;
            }
            let poll = (sys.channel_poll)(s.request_in, POLL_IN);
            if poll <= 0 || (poll as u32) & POLL_IN == 0 {
                break;
            }
            let n = (sys.channel_read)(s.request_in, s.rec.as_mut_ptr(), RECORD_MAX);
            if n <= 0 {
                break;
            }
            if discarding {
                // Down: the exchange was already invalidated by the LINK
                // DOWN; the pump re-issues it after LINK UP.
                s.m_dropped_down = s.m_dropped_down.wrapping_add(1);
                continue;
            }
            s.desk.accept(&s.rec[..n as usize]);
            flush_answers(s, sys);
            deliver(s, sys);
        }
    }

    s.step_ctr = s.step_ctr.wrapping_add(1);
    if s.step_ctr.is_multiple_of(EMIT_EVERY) {
        unsafe {
            telemetry::emit_counters(
                sys,
                s.metrics_out,
                &[
                    s.m_delivered,
                    s.m_acked,
                    s.m_refused,
                    s.m_dropped_down,
                    s.m_linkdowns,
                    s.m_bytes,
                    s.m_key_hash,
                    s.m_violations,
                ],
            );
        }
    }
    0
}
