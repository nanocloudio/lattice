//! loopback_sink — reference `stream.ordered_ack` provider. See
//! manifest.toml for the contract stance; the wire layouts and
//! contract text live in `modules/common/cdc_wire.rs`.
//!
//! The module is deliberately the SIMPLEST thing that conforms:
//! validate, count, digest, ack in order. Its one non-trivial feature
//! is scripted link loss (`linkdown_every`) so the pump's replay path
//! is exercised in CI rather than trusted.

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

#[path = "../../../target/fluxor/fluxor-abi/sdk/contracts/exchange.rs"]
mod exchange;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use cdc_wire::{
    resolved_frontier, CdcEvent, CDC_FLAG_BACKFILL, CDC_KIND_DELETE, CDC_KIND_PUT,
    CDC_KIND_RESOLVED,
};
use exchange::{
    Ack, Publish, MSG_ACK, MSG_PUBLISH, REFUSE_UNROUTABLE, STATUS_LINK_DOWN, STATUS_LINK_UP,
    STATUS_OK,
};

/// Publishes consumed per step — bounded, never a drain-until-empty.
const PER_STEP_BUDGET: u32 = 8;

/// Scratch: one worst-case publish frame plus envelope headroom.
const SCRATCH: usize = exchange::PUBLISH_FRAME_MAX + 16;

const EMIT_EVERY: u64 = 5000;

#[repr(C)]
struct SinkState {
    syscalls: *const SyscallTable,
    publish_in: i32,
    ack_out: i32,
    metrics_out: i32,

    // ── params ──
    /// Emit LINK_DOWN after every N acks (0 = never).
    linkdown_every: u32,
    /// Steps the link stays down before LINK_UP.
    linkdown_hold_steps: u32,
    /// Log every delivered event (`[sink] dlv …`) so an end-to-end
    /// test can assert exact delivery from the boot log. 0 = off.
    log_deliveries: u32,

    // ── link-loss script state ──
    acks_since_down: u32,
    down_steps_left: u32,
    /// The link is up. Starts DOWN so the very first signal a pump
    /// sees is LINK_UP — the contract's "(re)connected and writable"
    /// gate, which also proves the pump waits for it.
    link_up: bool,

    // ── counters (manifest [observability] order) ──
    m_delivered: u64,
    m_acked: u64,
    m_refused: u64,
    m_dropped_down: u64,
    m_linkdowns: u64,
    m_bytes: u64,
    /// Rolling FNV-1a over every delivered (msg_key, payload) — the
    /// content digest end-to-end tests compare.
    m_key_hash: u64,
    /// Envelope contract violations (see `check_envelope`): a
    /// streaming data event delivered at or below the highest resolved
    /// frontier already delivered. Must stay 0; the live suite asserts
    /// on the `[sink] VIOLATION` log line this counts.
    m_violations: u64,
    /// Highest resolved-watermark frontier delivered so far.
    max_resolved: u64,
    step_ctr: u64,
}

impl SinkState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.publish_in = -1;
        self.ack_out = -1;
        self.metrics_out = -1;
        self.linkdown_every = 0;
        self.linkdown_hold_steps = 50;
        self.log_deliveries = 0;
        self.acks_since_down = 0;
        self.down_steps_left = 0;
        self.link_up = false;
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
    }
}

define_params! {
    SinkState;

    // Scripted link loss: emit LINK_DOWN after every N acks; 0 = a
    // permanently healthy link.
    1, linkdown_every, u32, 0
        => |s, d, len| { s.linkdown_every = p_u32(d, len, 0, 0); };

    // Steps the link stays down before LINK_UP re-opens it.
    2, linkdown_hold_steps, u32, 50
        => |s, d, len| { s.linkdown_hold_steps = p_u32(d, len, 0, 50); };

    // Log every delivered event to the boot log (test witness).
    3, log_deliveries, u32, 0
        => |s, d, len| { s.log_deliveries = p_u32(d, len, 0, 0); };
}

unsafe fn read_envelope(sys: &SyscallTable, chan: i32, scratch: &mut [u8]) -> Option<(u8, usize)> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; 3];
    if (sys.channel_read)(chan, hdr.as_mut_ptr(), 3) < 3 {
        return None;
    }
    let mt = hdr[0];
    let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if len > scratch.len() {
        return None;
    }
    if len == 0 {
        return Some((mt, 0));
    }
    if ((sys.channel_read)(chan, scratch.as_mut_ptr(), len) as usize) < len {
        return None;
    }
    Some((mt, len))
}

unsafe fn send_ack(s: &mut SinkState, sys: &SyscallTable, ack: Ack) -> bool {
    let mut buf = [0u8; 3 + exchange::ACK_WIRE_LEN];
    buf[0] = MSG_ACK;
    buf[1] = exchange::ACK_WIRE_LEN as u8;
    buf[2] = 0;
    if ack.encode(&mut buf[3..]).is_none() {
        return false;
    }
    (sys.channel_write)(s.ack_out, buf.as_mut_ptr(), buf.len()) == buf.len() as i32
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
unsafe fn check_envelope(s: &mut SinkState, sys: &SyscallTable, publ: &Publish<'_>) {
    let Some(ev) = CdcEvent::decode(publ.payload) else {
        // Not a CDC envelope — the port contract is generic ordered
        // ack, so opaque payloads pass through unchecked.
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
        // `[sink] dlv <kind> <hex msg_key (≤48 B)> ts=<dec>` — the
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
        for &b in publ.msg_key.iter().take(48) {
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
        s.publish_in = in_chan;
        s.ack_out = out_chan;
        s.metrics_out = dev_channel_port(sys, 1, 1);
        parse_tlv(s, params, params_len);
    }
    0
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

    // Link lifecycle. The initial LINK_UP is announced once the ack
    // channel exists; a scripted outage counts down and re-opens.
    if !s.link_up {
        if s.down_steps_left > 0 {
            s.down_steps_left -= 1;
        }
        if s.down_steps_left == 0 && unsafe { send_ack(s, sys, Ack::link(STATUS_LINK_UP).unwrap()) }
        {
            s.link_up = true;
            unsafe {
                let m = b"[sink] LINK_UP announced";
                dev_log(sys, 2, m.as_ptr(), m.len());
            }
        }
    }

    let mut budget = PER_STEP_BUDGET;
    while budget > 0 {
        budget -= 1;
        let mut scratch = [0u8; SCRATCH];
        let Some((mt, len)) = (unsafe { read_envelope(sys, s.publish_in, &mut scratch) }) else {
            break;
        };
        if mt != MSG_PUBLISH {
            continue;
        }
        if !s.link_up {
            // Down: the publish was already invalidated by LINK_DOWN;
            // dropping it silently HERE is the contract-conformant move —
            // the pump replays after LINK_UP.
            s.m_dropped_down = s.m_dropped_down.wrapping_add(1);
            continue;
        }
        let Some(publ) = Publish::decode(&scratch[..len]) else {
            // Malformed frame with no recoverable corr — count it;
            // there is nothing addressable to refuse.
            s.m_refused = s.m_refused.wrapping_add(1);
            continue;
        };
        if publ.msg_key.is_empty() {
            // No routing key: typed refusal, never a silent drop.
            if unsafe { send_ack(s, sys, Ack::reply(publ.corr, REFUSE_UNROUTABLE).unwrap()) } {
                s.m_refused = s.m_refused.wrapping_add(1);
            }
            continue;
        }
        // "Durable acceptance" for the loopback backend IS this
        // bookkeeping — the strongest level this backend offers.
        s.m_delivered = s.m_delivered.wrapping_add(1);
        s.m_bytes = s.m_bytes.wrapping_add(publ.payload.len() as u64);
        s.m_key_hash = fnv_fold(fnv_fold(s.m_key_hash, publ.msg_key), publ.payload);
        unsafe { check_envelope(s, sys, &publ) };
        if unsafe { send_ack(s, sys, Ack::reply(publ.corr, STATUS_OK).unwrap()) } {
            s.m_acked = s.m_acked.wrapping_add(1);
            // Scripted outage AFTER acking: every corr the pump has in
            // flight beyond this point becomes unknowable.
            if s.linkdown_every != 0 {
                s.acks_since_down += 1;
                if s.acks_since_down >= s.linkdown_every {
                    s.acks_since_down = 0;
                    if unsafe { send_ack(s, sys, Ack::link(STATUS_LINK_DOWN).unwrap()) } {
                        s.link_up = false;
                        s.down_steps_left = s.linkdown_hold_steps.max(1);
                        s.m_linkdowns = s.m_linkdowns.wrapping_add(1);
                    }
                    break;
                }
            }
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
