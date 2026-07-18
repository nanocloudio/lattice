//! adapter_metrics — high-cardinality adapter observability rollup.
//!
//! Phase 6b minimal implementation. Consumes per-anchor counter
//! envelopes (`MSG_METRIC_DELTA`) and maintains a fixed-size table of
//! `(protocol, op, count)` rollups. Every `SNAPSHOT_INTERVAL` ticks
//! it emits a `MSG_METRIC_SNAPSHOT` envelope carrying the full table
//! to whoever's subscribed (substrate `telemetry`).
//!
//! Wire shapes:
//!   in  MSG_METRIC_DELTA    `[protocol:u8][op:u8][delta:u32 LE]`
//!   out MSG_METRIC_SNAPSHOT `[count:u16 LE]
//!                            [(protocol:u8, op:u8, total:u64 LE) × count]`
//!
//! These message-type bytes live in this module's local namespace
//! today (no formal wire.rs entry yet — telemetry is consumer-facing
//! and isn't part of the substrate envelope contract).

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

const MSG_METRIC_DELTA: u8 = 0xE8;
const MSG_METRIC_SNAPSHOT: u8 = 0xE9;

/// Emit a snapshot every N ticks. At tick_us=1000 that's once per
/// second; the consumer (telemetry) decides whether to forward.
const SNAPSHOT_INTERVAL: u32 = 1000;

#[path = "../../common/metrics_rollup.rs"]
mod metrics_rollup;
use metrics_rollup::{encode_snapshot, record_delta, MetricsTable};

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[repr(C)]
struct MetricsState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    // Dedicated `metrics` output port (output index 2), discovered in
    // module_new; -1 when unwired. Distinct from the app-metric rollup
    // path on out_chan/telemetry_out.
    metrics_out: i32,
    tick_since_snapshot: u32,
    // Phase-14 counters (ids follow manifest `[observability] metrics`).
    m_deltas_in: u64,
    m_snapshots_out: u64,
    step_ctr: u64,
    table: MetricsTable,
}

impl MetricsState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.in_chan = -1;
        self.out_chan = -1;
        self.metrics_out = -1;
        self.tick_since_snapshot = 0;
        self.m_deltas_in = 0;
        self.m_snapshots_out = 0;
        self.step_ctr = 0;
        self.table.init();
    }
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

unsafe fn write_envelope(sys: &SyscallTable, chan: i32, mt: u8, body: &[u8]) -> bool {
    if chan < 0 || body.len() > 512 {
        return false;
    }
    let mut buf = [0u8; 600];
    let total = 3 + body.len();
    if total > buf.len() {
        return false;
    }
    buf[0] = mt;
    buf[1] = (body.len() & 0xFF) as u8;
    buf[2] = ((body.len() >> 8) & 0xFF) as u8;
    buf[3..total].copy_from_slice(body);
    (sys.channel_write)(chan, buf.as_mut_ptr(), total) == total as i32
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<MetricsState>() as u32
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
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if state.is_null() || syscalls.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<MetricsState>() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<MetricsState>() };
    s.init(syscalls.cast::<SyscallTable>());
    s.in_chan = in_chan;
    s.out_chan = out_chan;
    // Dedicated telemetry port (output index 2: telemetry_out, http_out,
    // metrics).
    s.metrics_out = unsafe { dev_channel_port(&*syscalls.cast::<SyscallTable>(), 1, 2) };
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<MetricsState>() };
    s.tick_since_snapshot = s.tick_since_snapshot.wrapping_add(1);
    let sys = s.syscalls;
    if sys.is_null() {
        return 0;
    }
    let mut scratch = [0u8; 64];
    unsafe {
        if let Some((mt, len)) = read_envelope(&*sys, s.in_chan, &mut scratch) {
            if mt == MSG_METRIC_DELTA && len >= 6 {
                let proto = scratch[0];
                let op = scratch[1];
                let delta = u32::from_le_bytes([scratch[2], scratch[3], scratch[4], scratch[5]]);
                record_delta(&mut s.table, proto, op, delta);
                s.m_deltas_in += 1;
            }
        }
        if s.tick_since_snapshot >= SNAPSHOT_INTERVAL {
            s.tick_since_snapshot = 0;
            let mut snap = [0u8; 512];
            if let Some(n) = encode_snapshot(&s.table, &mut snap) {
                s.m_snapshots_out += 1;
                let _ = write_envelope(&*sys, s.out_chan, MSG_METRIC_SNAPSHOT, &snap[..n]);
            }
        }

        // Phase-14: emit this module's own operational counters on its
        // dedicated metrics port at a coarse cadence (no-op until wired).
        s.step_ctr = s.step_ctr.wrapping_add(1);
        if s.step_ctr.is_multiple_of(5000) {
            telemetry::emit_counters(&*sys, s.metrics_out, &[s.m_deltas_in, s.m_snapshots_out]);
        }
    }
    0
}
