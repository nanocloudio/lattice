//! placement_advisor — the §12.4 load-policy recommender as a running
//! module (RFC database foundation Phase 10's hot-range policy).
//!
//! Observes each range's key count by scanning it, asks
//! `placement::recommend` what §12.4 permits saying about that load,
//! and REPORTS the answer. It has no path to the lifecycle machinery
//! and no port that could reach one: §12.4 requires that "execution
//! remains bounded by operator policy", and an advisor wired to the
//! supervisor would turn every policy bug into a topology change. An
//! operator reads the recommendation and declares the operation in a
//! config, which is the same manual gate the split and merge pumps
//! already run behind.
//!
//! ## What it can and cannot measure, stated rather than implied
//!
//! Key count is observable here: it is a scan. Write rate and hot-key
//! concentration are NOT — nothing in this graph attributes writes to
//! ranges yet — so the advisor reports them as UNMEASURED (`0`) rather
//! than as zero-load. A range with no attributed writes is not a quiet
//! range, it is an unmeasured one, and the contract honours that: an
//! unmeasured concentration cannot satisfy the hot-key clause.
//!
//! What it CAN still recommend is the point. §12.4 lists "size, key
//! count" among the valid inputs on their own, so an oversized range
//! is worth splitting whether or not a load signal exists — a quiet
//! oversized range is still oversized. Gating splits on load as well
//! as size would not have been conservative, it would have been
//! SILENT: the split half would never fire anywhere the write rate is
//! unmeasured, which is everywhere in this graph.
//!
//! ## The load signal, and a correction worth reading
//!
//! Two rates are available. The MEASURED one is the router's per-range
//! write count, differenced across surveys — the real signal, since
//! the router is the only place that sees both a write and its range.
//! The DERIVED one is how fast this range's key count grows.
//!
//! An earlier version of this file claimed the derived rate was a
//! strict LOWER BOUND on the write rate, on the reasoning that every
//! new key costs at least one write. **That claim was wrong**, and
//! comparing the two rates side by side is what exposed it: one write
//! can create SEVERAL keys. A single `KV_OP_TXN` inserting a row plus
//! its secondary-index entries moves the key count by more than one,
//! so growth can EXCEED the write rate rather than under-report it.
//!
//! This matters because the safety argument rested on that bound. A
//! lower bound could only ever make the advisor quieter; a rate that
//! may over-report can make it MORE eager, which is the direction
//! every policy knob here is built to avoid. So the derived rate is
//! now what it always actually was — an ESTIMATE OF UNKNOWN DIRECTION,
//! useful when nothing better exists and never preferred over a
//! measurement. Both are reported, and a divergence is informative:
//! growth above the counted rate means batched writes, growth below
//! means overwrites.
//!
//! Per-KEY concentration remains unavailable from either: the router
//! counts writes per RANGE, so §12.4's hot-key clause still needs a
//! signal neither of these provides.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
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

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/placement.rs"]
mod placement;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use placement::{forecast_to_ceiling, recommend, Forecast, LoadPolicy, RangeLoad, Recommendation};

// ── The router's per-range write counters (§12.4's measured signal) ──
//
// Frame layout, fixed by fluxor's telemetry contract:
//   [0] signal (2 = metric)  [1] kind  [2..4] module  [4..12] t_micros
//   [12..14] metric id       [14..16] pad            [16..24] value
// 24 bytes, little-endian throughout.
//
// Parsing this here — rather than having the router PUT its counts to
// a key the advisor reads — was a deliberate choice. The router's
// property that EVERY BYTE IT WRITES IS A CLIENT'S is load-bearing for
// reasoning about the write path: it is why the router can never
// itself cause a durability bug. Spending that invariant to feed a
// recommender, the least safety-critical consumer here, is a bad
// trade at any implementation cost. This dependency instead sits in an
// advisory module and degrades gracefully — see `measured_rate`.
const TM_SIGNAL_METRIC: u8 = 2;
const TM_SCALAR_SIZE: usize = 24;
/// Metric ids on the router's port: routed, rejects, lin_reads, then
/// the per-range write counts.
const TM_ID_RANGE0_WRITES: u16 = 3;
/// Range 0's busiest-key share, percent. ⚠ UNDER-reports on
/// txn-heavy workloads — the router samples only ops with a single
/// extractable key, and a multi-op txn (SQL row + index entries,
/// graph edge pair, document insert) contributes nothing. Verified
/// live: 400 writes at one key produced no hot-key report. See
/// HOT_KEY_SLOTS in kv_request_router. The router derives it with
/// Space-Saving, whose counts are OVER-estimates — which is the safe
/// direction here, because over-reporting concentration can only make
/// the advisor REFUSE a split (`HotKeyNotSplittable`), never propose
/// one. An under-estimate would have let a genuinely unsplittable hot
/// key be recommended for a split it cannot benefit from.
const TM_ID_RANGE0_HOTKEY: u16 = 5;
use types::{KV_OP_SCAN, KV_RESULT_SCAN_CURSOR, PROTO_INTERNAL_JOB};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

const ENV_BUF: usize = 4096;
/// Poll cadence: slow on purpose. A recommender that samples faster
/// than an operator can act adds noise, not signal.
const POLL_MS: u64 = 30_000;
/// Keys counted per scan page.
const PAGE_LIMIT: u16 = 256;

const S_IDLE: u8 = 0;
const S_COUNT: u8 = 1;

define_params! {
    AdvisorState;

    // Operator policy (§12.4). Each is a REFUSAL threshold: raising
    // min_split_keys or cooldown_ms can only make the advisor quieter.
    1, min_split_keys, u32, 1024
        => |s, d, len| { s.min_split_keys = p_u32(d, len, 0, 1024); };
    2, max_merge_keys, u32, 512
        => |s, d, len| { s.max_merge_keys = p_u32(d, len, 0, 512); };
    3, cooldown_ms, u32, 60000
        => |s, d, len| { s.cooldown_ms = p_u32(d, len, 0, 60000); };
}

#[repr(C)]
struct AdvisorState {
    syscalls: *const SyscallTable,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,

    min_split_keys: u32,
    max_merge_keys: u32,
    cooldown_ms: u32,

    state: u8,
    last_poll_ms: u64,
    boot_ms: u64,
    corr: u64,
    cursor: u64,
    counted: u64,
    /// Previous survey's key count and the millisecond it completed,
    /// for the growth-derived load signal. `prev_at_ms == 0` means no
    /// prior survey — and NO SIGNAL, reported as unmeasured rather
    /// than as zero growth.
    prev_counted: u64,
    prev_at_ms: u64,
    /// Router-counted writes for range 0, and the previous sample, for
    /// differencing into a rate. `counted_at_ms == 0` means the router
    /// has not been heard from — reported as absent, never as zero.
    counted_writes: u64,
    prev_counted_writes: u64,
    counted_at_ms: u64,
    prev_counted_at_ms: u64,
    /// Busiest-key share for range 0, percent. `0` = no evidence, and
    /// the placement contract already reads 0 as unmeasured rather
    /// than as proven-low concentration.
    busiest_percent: u8,
    /// Count of concentration frames actually RECEIVED. Distinguishes
    /// "the signal says zero" from "the signal never arrived" — the
    /// two look identical in a percentage and demand opposite fixes.
    hotkey_frames: u64,
    signal_in: i32,

    env: [u8; ENV_BUF],

    m_surveys: u64,
    m_recommendations: u64,
    m_keys: u64,
    step_ctr: u32,
}

impl AdvisorState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.min_split_keys = 1024;
        self.max_merge_keys = 512;
        self.cooldown_ms = 60_000;
        self.state = S_IDLE;
        self.last_poll_ms = 0;
        self.boot_ms = 0;
        self.corr = 0;
        self.cursor = 0;
        self.counted = 0;
        self.prev_counted = 0;
        self.prev_at_ms = 0;
        self.counted_writes = 0;
        self.prev_counted_writes = 0;
        self.counted_at_ms = 0;
        self.prev_counted_at_ms = 0;
        self.busiest_percent = 0;
        self.hotkey_frames = 0;
        self.signal_in = -1;
        self.env = [0; ENV_BUF];
        self.m_surveys = 0;
        self.m_recommendations = 0;
        self.m_keys = 0;
        self.step_ctr = 0;
    }

    fn policy(&self) -> LoadPolicy {
        LoadPolicy {
            min_split_keys: u64::from(self.min_split_keys),
            max_merge_keys: u64::from(self.max_merge_keys),
            hot_write_rate: LoadPolicy::DEFAULT.hot_write_rate,
            hot_key_percent: LoadPolicy::DEFAULT.hot_key_percent,
            max_concurrent: LoadPolicy::DEFAULT.max_concurrent,
            cooldown_ms: u64::from(self.cooldown_ms),
            // Size is the ONLY signal this graph can attribute to a
            // range today, so splits must be reachable through it.
            split_on_size: true,
        }
    }
}

const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

fn kv_send(a: &mut AdvisorState, op: u8, body_len: usize, next: u8) -> bool {
    a.corr = a.corr.wrapping_add(1).max(1);
    const REQ_HEAD: usize = 18;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > a.env.len() {
        return false;
    }
    a.env[at..at + 8].copy_from_slice(&a.corr.to_le_bytes());
    a.env[at + 8] = PROTO_INTERNAL_JOB;
    a.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    a.env[at + 13] = 0;
    a.env[at + 14] = 0;
    a.env[at + 15] = op;
    a.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    let sent = unsafe {
        let sys = a.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                a.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut a.env,
            )
    };
    if sent {
        a.state = next;
    }
    sent
}

/// SCAN body: `[cursor:u64][limit:u16]`.
fn send_count(a: &mut AdvisorState) -> bool {
    let cursor = a.cursor;
    let need = 10;
    if BODY_AT + need > a.env.len() {
        return false;
    }
    a.env[BODY_AT..BODY_AT + 8].copy_from_slice(&cursor.to_le_bytes());
    a.env[BODY_AT + 8..BODY_AT + 10].copy_from_slice(&PAGE_LIMIT.to_le_bytes());
    kv_send(a, KV_OP_SCAN, need, S_COUNT)
}

/// Emit one recommendation as an operator-readable line. Reporting IS
/// the product here, so the wording states the answer AND why it is
/// not an action.
/// Right-aligned zero-padded decimal into an exact-width slot. Doing
/// this once beats open-coding index arithmetic per field — the first
/// attempt here wrote its digits over the labels.
fn write_dec(slot: &mut [u8], mut v: u64) {
    let mut i = slot.len();
    while i > 0 {
        i -= 1;
        slot[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
}

fn report(a: &AdvisorState, r: Recommendation, keys: u64, rate: u32, measured: bool) {
    // Report the OBSERVATION alongside the verdict. A recommender whose
    // inputs are invisible cannot be checked, and an unverifiable
    // recommendation is worth about as much as a green phase log on a
    // pump that moved nothing.
    unsafe {
        if !a.syscalls.is_null() {
            let mut m = *b"[padv] survey keys=0000000 rate=00000/s (derived, estimate)";
            // Digit slots, counted once rather than guessed: the label
            // is 19 bytes, so keys occupy [19,26) and rate [32,37).
            write_dec(&mut m[19..26], keys.min(9_999_999));
            write_dec(&mut m[32..37], u64::from(rate).min(99_999));
            if !measured {
                // First survey: no predecessor, so no rate exists. Say
                // so rather than printing a zero that reads as "idle".
                let tail = b"[padv] survey rate UNMEASURED (first survey has no predecessor)";
                dev_log(&*a.syscalls, 2, tail.as_ptr(), tail.len());
            } else {
                dev_log(&*a.syscalls, 2, m.as_ptr(), m.len());
            }
        }
    }
    report_verdict(a, r)
}

fn report_verdict(a: &AdvisorState, r: Recommendation) {
    let msg: &[u8] = match r {
        Recommendation::None => b"[padv] within policy; no recommendation",
        Recommendation::Split { .. } => {
            b"[padv] RECOMMEND split (operator must declare it; this module cannot act)"
        }
        Recommendation::Merge { .. } => {
            b"[padv] RECOMMEND merge (operator must declare it; this module cannot act)"
        }
        Recommendation::HotKeyNotSplittable { .. } => {
            b"[padv] hot key: splitting will NOT help (12.4) - no recommendation"
        }
        Recommendation::Cooldown { .. } => b"[padv] within cooldown since the last operation",
        Recommendation::OperationInFlight => b"[padv] a lifecycle operation is in flight",
    };
    unsafe {
        if !a.syscalls.is_null() {
            dev_log(&*a.syscalls, 2, msg.as_ptr(), msg.len());
        }
    }
}

fn on_kv_response(a: &mut AdvisorState, result: u8, body: &[u8]) {
    if a.state != S_COUNT {
        return;
    }
    if result != KV_RESULT_SCAN_CURSOR || body.len() < 10 {
        a.state = S_IDLE;
        return;
    }
    a.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let count = u16::from_le_bytes([body[8], body[9]]);
    a.counted = a.counted.saturating_add(u64::from(count));
    if a.cursor != 0 {
        if !send_count(a) {
            a.state = S_IDLE;
        }
        return;
    }

    // The survey is complete. Key count is measured; write rate and
    // hot-key concentration are NOT, and are reported as unmeasured
    // (0) rather than as zero — see the module docs.
    // Growth since the previous survey, in keys per second. NOT a
    // bound in either direction — see the module docs: one TXN write
    // can create a row and its index entries, so growth may exceed the
    // write rate. The first survey has no predecessor and therefore NO
    // signal — reported as unmeasured, not as zero growth.
    let measured = a.prev_at_ms != 0;
    let write_rate = if !measured {
        0
    } else {
        let elapsed_ms = a.last_poll_ms.saturating_sub(a.prev_at_ms).max(1);
        let grown = a.counted.saturating_sub(a.prev_counted);
        // Shrinkage (deletes, or a merge moving keys away) is not
        // negative load; it is simply no growth evidence.
        ((grown.saturating_mul(1000)) / elapsed_ms).min(u64::from(u32::MAX)) as u32
    };
    a.prev_counted = a.counted;
    a.prev_at_ms = a.last_poll_ms;

    // The MEASURED rate, if the router has been heard from twice.
    // Kept ALONGSIDE the growth estimate rather than replacing it: a
    // counted rate that diverges from the growth lower bound is either
    // overwrite traffic (expected, and exactly what growth cannot see)
    // or a counting bug — and only having both distinguishes them.
    let measured_rate = if a.prev_counted_at_ms == 0 || a.counted_at_ms <= a.prev_counted_at_ms {
        None
    } else {
        let ms = a.counted_at_ms - a.prev_counted_at_ms;
        let d = a.counted_writes.saturating_sub(a.prev_counted_writes);
        Some(((d.saturating_mul(1000)) / ms).min(u64::from(u32::MAX)) as u32)
    };
    // Prefer the MEASUREMENT always; the derived rate is a fallback
    // with no guaranteed direction, so it must never displace a
    // counted one. Degrading to a blinder-but-working signal is the
    // whole reason this dependency was allowed to live here at all.
    let effective_rate = measured_rate.unwrap_or(write_rate);
    // Shift the survey-spaced sample now that the rate is computed.
    if a.counted_at_ms != 0 {
        a.prev_counted_writes = a.counted_writes;
        a.prev_counted_at_ms = a.counted_at_ms;
    }
    if let Some(m) = measured_rate {
        let mut msg = *b"[padv] router-counted rate=00000/s (measured)";
        write_dec(&mut msg[27..32], u64::from(m));
        unsafe {
            if !a.syscalls.is_null() {
                dev_log(&*a.syscalls, 2, msg.as_ptr(), msg.len());
            }
        }
    }

    {
        // Report arrival separately from value: a 0% concentration and
        // a never-delivered metric are indistinguishable downstream,
        // and they need opposite fixes.
        let mut m = *b"[padv] concentration frames=0000000 pct=000";
        write_dec(&mut m[28..35], a.hotkey_frames.min(9_999_999));
        write_dec(&mut m[40..43], u64::from(a.busiest_percent));
        unsafe {
            if !a.syscalls.is_null() {
                dev_log(&*a.syscalls, 2, m.as_ptr(), m.len());
            }
        }
    }

    let load = RangeLoad {
        range_index: 0,
        key_count: a.counted,
        write_rate: effective_rate,
        // §12.4's hot-key clause, now measured: the router's
        // Space-Saving table over its own dispatch stream. Still 0
        // when nothing has been observed, which the contract reads as
        // unmeasured rather than as proven-low.
        busiest_key_percent: a.busiest_percent,
        since_last_op_ms: a
            .last_poll_ms
            .saturating_sub(a.boot_ms)
            .max(u64::from(a.cooldown_ms)),
    };
    let policy = a.policy();
    let r = recommend(&load, None, &policy, 0);
    a.m_surveys = a.m_surveys.wrapping_add(1);
    a.m_keys = a.counted;
    if r.is_actionable() {
        a.m_recommendations = a.m_recommendations.wrapping_add(1);
    }
    report(a, r, a.counted, write_rate, measured);

    // Capacity forecast (Phase 10): when does this range reach the
    // size at which a split is warranted? Reported separately from the
    // recommendation because a forecast is not an instruction — it is
    // the lead time an operator schedules against.
    let f = forecast_to_ceiling(
        a.counted,
        effective_rate,
        policy.min_split_keys,
        measured || measured_rate.is_some(),
    );
    let msg: &[u8] = match f {
        Forecast::Unmeasured => b"[padv] forecast: no growth signal yet",
        Forecast::NotGrowing => b"[padv] forecast: flat; no split projected",
        Forecast::AlreadyExceeded => b"[padv] forecast: already at the split ceiling",
        Forecast::Seconds(_) => b"[padv] forecast: split ceiling reached in (see rate)",
    };
    unsafe {
        if !a.syscalls.is_null() {
            dev_log(&*a.syscalls, 2, msg.as_ptr(), msg.len());
        }
    }
    a.counted = 0;
    a.state = S_IDLE;
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<AdvisorState>() as u32
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
    if state_size < core::mem::size_of::<AdvisorState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let a = unsafe { &mut *state.cast::<AdvisorState>() };
    a.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(a, params, params_len) };
    }
    a.kv_in = in_chan;
    a.kv_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        a.metrics_out = dev_channel_port(sys, 1, 1);
        // Input index 1: the router's metrics port. Absent in
        // compositions that do not wire it, which is why every use of
        // the counted signal is conditional.
        a.signal_in = dev_channel_port(sys, 0, 1);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let a = unsafe { &mut *state.cast::<AdvisorState>() };
    let sys_ptr = a.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    let now = unsafe { dev_millis(&*sys_ptr) };
    if a.boot_ms == 0 {
        a.boot_ms = now.max(1);
    }

    unsafe {
        let sys = &*sys_ptr;
        let mut env = [0u8; ENV_BUF];
        if let Some((msg, payload)) = read_one_envelope(sys, a.kv_in, &mut env) {
            if msg == MSG_KV_RESPONSE && payload.len() >= 20 {
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                let result = payload[9];
                let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
                if corr == a.corr && payload.len() >= 20 + blen {
                    let mut body = [0u8; ENV_BUF];
                    body[..blen].copy_from_slice(&payload[20..20 + blen]);
                    on_kv_response(a, result, &body[..blen]);
                }
            }
        }
    }

    // Drain whatever the router has emitted since the last step.
    unsafe {
        let sys = &*sys_ptr;
        let mut frame = [0u8; TM_SCALAR_SIZE];
        while a.signal_in >= 0 {
            let poll = (sys.channel_poll)(a.signal_in, POLL_IN);
            if poll <= 0 || (poll as u32) & POLL_IN == 0 {
                break;
            }
            let n = (sys.channel_read)(a.signal_in, frame.as_mut_ptr(), TM_SCALAR_SIZE);
            if (n as usize) < TM_SCALAR_SIZE {
                break;
            }
            if frame[0] == TM_SIGNAL_METRIC {
                let id = u16::from_le_bytes([frame[12], frame[13]]);
                if id == TM_ID_RANGE0_HOTKEY {
                    let v = u64::from_le_bytes(frame[16..24].try_into().unwrap_or([0; 8]));
                    a.busiest_percent = v.min(100) as u8;
                    a.hotkey_frames = a.hotkey_frames.saturating_add(1);
                } else if id == TM_ID_RANGE0_WRITES {
                    // Record the LATEST value only. The previous
                    // sample is shifted at SURVEY boundaries, not
                    // here: the router emits far faster than the
                    // survey interval, so differencing consecutive
                    // frames measures microseconds apart and rounds
                    // every real rate to zero. That is exactly the
                    // divergence-from-growth the report was built to
                    // expose, and it showed up on the first live run.
                    a.counted_writes =
                        u64::from_le_bytes(frame[16..24].try_into().unwrap_or([0; 8]));
                    a.counted_at_ms = now.max(1);
                }
            }
        }
    }

    if a.state == S_IDLE && now.wrapping_sub(a.last_poll_ms) >= POLL_MS {
        a.last_poll_ms = now;
        a.cursor = 0;
        a.counted = 0;
        let _ = send_count(a);
    }

    a.step_ctr = a.step_ctr.wrapping_add(1);
    if a.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                a.metrics_out,
                &[a.m_surveys, a.m_recommendations, a.m_keys],
            );
        }
    }
    0
}

unsafe fn write_envelope(
    sys: &SyscallTable,
    chan: i32,
    msg_type: u8,
    payload_len: usize,
    env: &mut [u8],
) -> bool {
    if chan < 0 || payload_len > u16::MAX as usize {
        return false;
    }
    env[0] = msg_type;
    env[1] = (payload_len & 0xFF) as u8;
    env[2] = ((payload_len >> 8) & 0xFF) as u8;
    let total = wire::ENVELOPE_HDR + payload_len;
    (sys.channel_write)(chan, env.as_mut_ptr(), total) == total as i32
}

unsafe fn read_one_envelope<'a>(
    sys: &SyscallTable,
    chan: i32,
    scratch: &'a mut [u8],
) -> Option<(u8, &'a [u8])> {
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
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len > scratch.len() {
        return None;
    }
    if payload_len > 0
        && ((sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len) as usize) < payload_len
    {
        return None;
    }
    Some((hdr[0], &scratch[..payload_len]))
}
