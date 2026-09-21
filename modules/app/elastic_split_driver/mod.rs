//! Elastic-split driver.
//!
//! Moves a key SPAN onto a demand-provisioned partition and cuts routing
//! over to it by shipping the span's bytes over the app-snapshot chunk
//! stream rather than copying the whole store. With the install ack wired
//! the flow is barrier-fenced: a write concurrent with the move is
//! captured by a copy pass or refused retryably by the barrier, provided
//! the drain window covers the graph's route-to-apply latency (see step
//! 3):
//!
//!   1. `start_delay_ms` after boot — send one
//!      `MSG_APP_SNAPSHOT_SPAN_REQUEST` to the SOURCE worker for
//!      `[span_start, span_end)`; the source encodes exactly that span
//!      and streams it to the TARGET worker, which installs it. Writes
//!      keep flowing during this bulk copy.
//!   2. on the target's install ack (`MSG_APP_SNAPSHOT_INSTALLED` on
//!      `ack_in`) — raise the router's `MSG_SPAN_BARRIER` over the span:
//!      new writes into it now refuse retryably, so nothing more can
//!      land behind the copy.
//!   3. `barrier_settle_ms` later — send a SECOND span request. The
//!      quiesced re-copy is the catch-up pass: it recaptures every write
//!      that landed after step 1's capture. The window is a TIMED drain:
//!      it must exceed the time a write already routed past the router
//!      takes to apply on the source worker (in-process channels apply
//!      within a few ticks; the 300 ms default is orders of magnitude
//!      above that). A write still in flight past the window would be
//!      missed — size the knob to the composition.
//!   4. on the second ack — publish the cutover `MSG_RANGE_MAP_UPDATE`
//!      binding the span to the target, then CLEAR the barrier (same
//!      channel, so the router sees map-then-clear in order). Retried
//!      writes now route to the target, which holds the complete span.
//!
//! `settle_ms` is a FALLBACK timeout per waiting phase for a graph that
//! leaves the ack unwired (or loses one): the flow still advances, and an
//! ackless graph degrades to the unfenced timed cutover.
//!
//! Pure orchestration: the span bytes and the cutover map are supplied as
//! params, decoded once at init. The barrier span is the same span in
//! ROUTER key form — the router fences on request key bytes, which carry
//! no identity prefix, so the stored-key bounds strip theirs.

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

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use wire::{MSG_APP_SNAPSHOT_INSTALLED, MSG_APP_SNAPSHOT_SPAN_REQUEST};

/// Envelope type for a full range-map publication on the router's
/// `map_update` port (owned by `partition_map::MSG_RANGE_MAP_UPDATE`;
/// mirrored here to avoid mounting the whole map module just for a byte).
const MSG_RANGE_MAP_UPDATE: u8 = 0xE3;

const ENV_BUF: usize = 4096;
const KEY_MAX: usize = 256;
const MAP_MAX: usize = 2048;

define_params! {
    DriverState;

    1, start_delay_ms, u32, 3000
        => |s, d, len| { s.start_delay_ms = p_u32(d, len, 0, 3000); };
    2, settle_ms, u32, 2000
        => |s, d, len| { s.settle_ms = p_u32(d, len, 0, 2000); };
    // Span bounds (STORED-key bytes), hex. Empty = MIN/MAX.
    3, span_start, str, 0
        => |s, d, len| { append_hex(&mut s.span_start_hex, &mut s.span_start_hex_len, KEY_MAX * 2, d, len); };
    4, span_end, str, 0
        => |s, d, len| { append_hex(&mut s.span_end_hex, &mut s.span_end_hex_len, KEY_MAX * 2, d, len); };
    // Post-cutover RANGE_MAP_UPDATE_V1 frame, hex.
    5, cutover_map, str_chunked, 0
        => |s, d, len| { append_hex(&mut s.map_hex, &mut s.map_hex_len, MAP_MAX * 2, d, len); };
    // Drain window between raising the barrier and the catch-up copy:
    // long enough for writes already routed past the router to apply.
    6, barrier_settle_ms, u32, 300
        => |s, d, len| { s.barrier_settle_ms = p_u32(d, len, 0, 300); };
}

/// Envelope type for a SPAN_BARRIER frame on the router's `map_update`
/// port (owned by `partition_map::MSG_SPAN_BARRIER`; mirrored like the
/// map-update type above).
const MSG_SPAN_BARRIER: u8 = 0xE4;
/// `partition_map::SPAN_BARRIER_VERSION`, mirrored.
const SPAN_BARRIER_VERSION: u16 = 1;
/// Width of the stored-key identity prefix the barrier bounds strip.
const IDENT_LEN: usize = 12;

// Driver phases.
const P_WAIT: u8 = 0; // before start_delay
const P_COPY1: u8 = 1; // bulk copy sent, awaiting ack
const P_BARRIER: u8 = 2; // barrier raised, draining in-flight writes
const P_COPY2: u8 = 3; // catch-up copy sent, awaiting ack
const P_DONE: u8 = 4; // cutover published, barrier cleared

fn append_hex(buf: &mut [u8], cur: &mut u16, cap: usize, d: *const u8, len: usize) {
    let at = *cur as usize;
    if *cur == u16::MAX || at + len > cap {
        *cur = u16::MAX;
        return;
    }
    unsafe {
        for i in 0..len {
            buf[at + i] = *d.add(i);
        }
    }
    *cur = (at + len) as u16;
}

#[repr(C)]
struct DriverState {
    syscalls: *const SyscallTable,
    snap_ctl_out: i32,
    ack_in: i32,
    ack_seen: u8,
    map_out: i32,
    metrics_out: i32,

    start_delay_ms: u32,
    settle_ms: u32,
    barrier_settle_ms: u32,
    span_start_hex: [u8; KEY_MAX * 2],
    span_start_hex_len: u16,
    span_end_hex: [u8; KEY_MAX * 2],
    span_end_hex_len: u16,
    map_hex: [u8; MAP_MAX * 2],
    map_hex_len: u16,

    span_start: [u8; KEY_MAX],
    span_start_len: u16,
    span_end: [u8; KEY_MAX],
    span_end_len: u16,
    map: [u8; MAP_MAX],
    map_len: u16,

    boot_ms: u64,
    phase: u8,
    phase_ms: u64,
    fenced: u8,
    step_ctr: u64,
    env: [u8; ENV_BUF],
    m_span_requests: u64,
    m_cutovers: u64,
}

impl DriverState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.snap_ctl_out = -1;
        self.ack_in = -1;
        self.ack_seen = 0;
        self.map_out = -1;
        self.metrics_out = -1;
        self.start_delay_ms = 3000;
        self.settle_ms = 2000;
        self.span_start_hex = [0; KEY_MAX * 2];
        self.span_start_hex_len = 0;
        self.span_end_hex = [0; KEY_MAX * 2];
        self.span_end_hex_len = 0;
        self.map_hex = [0; MAP_MAX * 2];
        self.map_hex_len = 0;
        self.span_start = [0; KEY_MAX];
        self.span_start_len = 0;
        self.span_end = [0; KEY_MAX];
        self.span_end_len = 0;
        self.map = [0; MAP_MAX];
        self.map_len = 0;
        self.boot_ms = 0;
        self.phase = P_WAIT;
        self.phase_ms = 0;
        self.fenced = 0;
        self.step_ctr = 0;
        self.barrier_settle_ms = 300;
        self.env = [0; ENV_BUF];
        self.m_span_requests = 0;
        self.m_cutovers = 0;
    }
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Decode `hex[..hlen]` into `out`, returning the byte length or None.
fn hex_decode(hex: &[u8], hlen: u16, out: &mut [u8]) -> Option<usize> {
    if hlen == u16::MAX || !(hlen as usize).is_multiple_of(2) {
        return None;
    }
    let n = hlen as usize / 2;
    if n > out.len() {
        return None;
    }
    for i in 0..n {
        out[i] = (hex_nibble(hex[i * 2])? << 4) | hex_nibble(hex[i * 2 + 1])?;
    }
    Some(n)
}

/// Read one envelope `[msg][len][payload]` from `chan`, or None.
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

/// Send `[msg_type][len][payload]` on `chan`; payload staged at
/// `env[ENVELOPE_HDR..]`.
unsafe fn write_env(sys: &SyscallTable, chan: i32, msg: u8, plen: usize, env: &mut [u8]) -> bool {
    if chan < 0 || plen > u16::MAX as usize {
        return false;
    }
    env[0] = msg;
    env[1] = (plen & 0xFF) as u8;
    env[2] = ((plen >> 8) & 0xFF) as u8;
    let total = wire::ENVELOPE_HDR + plen;
    (sys.channel_write)(chan, env.as_mut_ptr(), total) == total as i32
}

/// Build and send the span-copy request:
/// `[term:8=0][index:8=0][start_len:u16][start][end_len:u16][end]`.
fn send_span_request(s: &mut DriverState) -> bool {
    let at = wire::ENVELOPE_HDR;
    let sl = s.span_start_len as usize;
    let el = s.span_end_len as usize;
    let plen = 16 + 2 + sl + 2 + el;
    if at + plen > s.env.len() {
        return false;
    }
    let mut n = at;
    s.env[n..n + 16].copy_from_slice(&[0u8; 16]); // term, index = 0
    n += 16;
    s.env[n..n + 2].copy_from_slice(&(sl as u16).to_le_bytes());
    n += 2;
    s.env[n..n + sl].copy_from_slice(&s.span_start[..sl]);
    n += sl;
    s.env[n..n + 2].copy_from_slice(&(el as u16).to_le_bytes());
    n += 2;
    s.env[n..n + el].copy_from_slice(&s.span_end[..el]);
    unsafe {
        let sys = s.syscalls;
        !sys.is_null()
            && write_env(
                &*sys,
                s.snap_ctl_out,
                MSG_APP_SNAPSHOT_SPAN_REQUEST,
                plen,
                &mut s.env,
            )
    }
}

/// Publish a SPAN_BARRIER frame over the moving span. Bounds go in
/// ROUTER key form — the router fences on the request's key bytes, which
/// carry no identity prefix, so stored-key bounds strip theirs (an empty
/// bound stays empty: unbounded).
fn publish_barrier(s: &mut DriverState, set: bool) -> bool {
    fn router_bound(b: &[u8]) -> &[u8] {
        if b.len() >= IDENT_LEN {
            &b[IDENT_LEN..]
        } else {
            b
        }
    }
    let at = wire::ENVELOPE_HDR;
    let sl = router_bound(&s.span_start[..s.span_start_len as usize]).len();
    let el = router_bound(&s.span_end[..s.span_end_len as usize]).len();
    let plen = 3 + 2 + sl + 2 + el;
    if at + plen > s.env.len() {
        return false;
    }
    let mut n = at;
    s.env[n..n + 2].copy_from_slice(&SPAN_BARRIER_VERSION.to_le_bytes());
    n += 2;
    s.env[n] = u8::from(set);
    n += 1;
    s.env[n..n + 2].copy_from_slice(&(sl as u16).to_le_bytes());
    n += 2;
    let start_off = if s.span_start_len as usize >= IDENT_LEN {
        IDENT_LEN
    } else {
        0
    };
    for i in 0..sl {
        s.env[n + i] = s.span_start[start_off + i];
    }
    n += sl;
    s.env[n..n + 2].copy_from_slice(&(el as u16).to_le_bytes());
    n += 2;
    let end_off = if s.span_end_len as usize >= IDENT_LEN {
        IDENT_LEN
    } else {
        0
    };
    for i in 0..el {
        s.env[n + i] = s.span_end[end_off + i];
    }
    unsafe {
        let sys = s.syscalls;
        !sys.is_null() && write_env(&*sys, s.map_out, MSG_SPAN_BARRIER, plen, &mut s.env)
    }
}

/// Publish the cutover map frame verbatim as a MSG_RANGE_MAP_UPDATE.
fn publish_cutover(s: &mut DriverState) -> bool {
    let at = wire::ENVELOPE_HDR;
    let ml = s.map_len as usize;
    if ml == 0 || at + ml > s.env.len() {
        return false;
    }
    s.env[at..at + ml].copy_from_slice(&s.map[..ml]);
    unsafe {
        let sys = s.syscalls;
        !sys.is_null() && write_env(&*sys, s.map_out, MSG_RANGE_MAP_UPDATE, ml, &mut s.env)
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<DriverState>() as u32
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
    if state_size < core::mem::size_of::<DriverState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let s = unsafe { &mut *state.cast::<DriverState>() };
    s.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(s, params, params_len) };
    }
    s.ack_in = in_chan;
    s.snap_ctl_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        s.map_out = dev_channel_port(sys, 1, 1);
        s.metrics_out = dev_channel_port(sys, 1, 2);
    }
    // Decode the span bounds and cutover map once. Empty bounds are legal
    // (MIN/MAX); an invalid map or hex refuses the whole driver.
    match hex_decode(&s.span_start_hex, s.span_start_hex_len, &mut s.span_start) {
        Some(n) => s.span_start_len = n as u16,
        None if s.span_start_hex_len == 0 => s.span_start_len = 0,
        None => return -1,
    }
    match hex_decode(&s.span_end_hex, s.span_end_hex_len, &mut s.span_end) {
        Some(n) => s.span_end_len = n as u16,
        None if s.span_end_hex_len == 0 => s.span_end_len = 0,
        None => return -1,
    }
    match hex_decode(&s.map_hex, s.map_hex_len, &mut s.map) {
        Some(n) => s.map_len = n as u16,
        None => return -1, // the cutover map is required
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<DriverState>() };
    let sys_ptr = s.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    let now = unsafe { dev_millis(&*sys_ptr) };
    if s.boot_ms == 0 {
        s.boot_ms = now.max(1);
    }

    // Drain one install-complete ack per step while a copy is in flight.
    // Each ack means the whole span (as of that copy's capture) is
    // resident on the target.
    let mut acked = false;
    if (s.phase == P_COPY1 || s.phase == P_COPY2) && s.ack_in >= 0 {
        unsafe {
            let sys = &*sys_ptr;
            let mut env = [0u8; 64];
            if let Some((msg, _payload)) = read_one_envelope(sys, s.ack_in, &mut env) {
                if msg == MSG_APP_SNAPSHOT_INSTALLED {
                    acked = true;
                }
            }
        }
    }
    let phase_timed_out = s.phase_ms != 0 && now.wrapping_sub(s.phase_ms) >= u64::from(s.settle_ms);

    match s.phase {
        P_WAIT => {
            if now.wrapping_sub(s.boot_ms) >= u64::from(s.start_delay_ms) && send_span_request(s) {
                s.phase = P_COPY1;
                s.phase_ms = now.max(1);
                s.m_span_requests = s.m_span_requests.wrapping_add(1);
            }
        }
        P_COPY1 => {
            if acked {
                // Bulk copy resident: fence the span so nothing more can
                // land behind the catch-up capture.
                if publish_barrier(s, true) {
                    s.fenced = 1;
                    s.phase = P_BARRIER;
                    s.phase_ms = now.max(1);
                }
            } else if phase_timed_out {
                // No ack ever arrived (ack unwired or lost): degrade to
                // the unfenced timed cutover rather than wedge.
                if publish_cutover(s) {
                    s.phase = P_DONE;
                    s.m_cutovers = s.m_cutovers.wrapping_add(1);
                }
            }
        }
        P_BARRIER => {
            if now.wrapping_sub(s.phase_ms) >= u64::from(s.barrier_settle_ms)
                && send_span_request(s)
            {
                s.phase = P_COPY2;
                s.phase_ms = now.max(1);
                s.m_span_requests = s.m_span_requests.wrapping_add(1);
            }
        }
        P_COPY2 => {
            // The quiesced re-copy is resident (or the ack was lost and
            // the timeout fires): cut routing over, then lift the
            // barrier on the same ordered channel so retried writes meet
            // the new map first.
            if (acked || phase_timed_out) && publish_cutover(s) {
                s.m_cutovers = s.m_cutovers.wrapping_add(1);
                if s.fenced == 1 && publish_barrier(s, false) {
                    s.fenced = 0;
                }
                s.phase = P_DONE;
            }
        }
        P_DONE => {
            // A failed clear is retried until it lands — a stuck barrier
            // would starve the span's writes forever.
            if s.fenced == 1 && publish_barrier(s, false) {
                s.fenced = 0;
            }
        }
        _ => {}
    }

    s.step_ctr = s.step_ctr.wrapping_add(1);
    if s.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(&*sys_ptr, s.metrics_out, &[s.m_span_requests, s.m_cutovers]);
        }
    }
    0
}
