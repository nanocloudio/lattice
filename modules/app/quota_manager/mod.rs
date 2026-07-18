//! quota_manager — per-tenant token buckets + throttle emission.
//!
//! Phase 6b minimal implementation. Each tenant has a token bucket
//! that refills at `rate_per_tick` and caps at `burst`. Inbound
//! `MSG_QUOTA_TICK` (synthetic stand-in for "request executed")
//! decrements the bucket; underflow emits a `MSG_QUOTA_THROTTLE`
//! envelope. Repeated overage triggers `MSG_QUOTA_DISCONNECT`.
//!
//! Wire shapes (`modules/common/wire.rs`):
//!   in  MSG_QUOTA_TICK    `[tenant_id:u32 LE][cost:u16 LE]`
//!   out MSG_QUOTA_THROTTLE `[tenant_id:u32 LE][reason:u8][retry_after_ms:u32 LE]`
//!   out MSG_QUOTA_DISCONNECT `[conn_id:u8][reason:u8]`

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

use wire::{MSG_QUOTA_DISCONNECT, MSG_QUOTA_THROTTLE};

const MSG_QUOTA_TICK: u8 = 0xDC;

const THROTTLE_REASON_RATE: u8 = 1;
const DISCONNECT_REASON_NOISY: u8 = 1;

#[path = "../../common/quota_bucket.rs"]
mod quota_bucket;
use quota_bucket::{consume, refill_all, QuotaTable};

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[repr(C)]
struct QuotaState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    // Dedicated `metrics` output port (output index 3), discovered in
    // module_new; -1 when unwired.
    metrics_out: i32,
    _pad: i32,
    // Phase-14 counters (ids follow manifest `[observability] metrics`).
    m_checks: u64,
    m_throttles: u64,
    m_disconnects: u64,
    step_ctr: u64,
    table: QuotaTable,
}

impl QuotaState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.in_chan = -1;
        self.out_chan = -1;
        self.metrics_out = -1;
        self._pad = 0;
        self.m_checks = 0;
        self.m_throttles = 0;
        self.m_disconnects = 0;
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
    if chan < 0 || body.len() > 64 {
        return false;
    }
    let mut buf = [0u8; 80];
    let total = 3 + body.len();
    buf[0] = mt;
    buf[1] = (body.len() & 0xFF) as u8;
    buf[2] = ((body.len() >> 8) & 0xFF) as u8;
    buf[3..total].copy_from_slice(body);
    (sys.channel_write)(chan, buf.as_mut_ptr(), total) == total as i32
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<QuotaState>() as u32
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
    if state_size < core::mem::size_of::<QuotaState>() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<QuotaState>() };
    s.init(syscalls.cast::<SyscallTable>());
    s.in_chan = in_chan;
    s.out_chan = out_chan;
    // Dedicated telemetry port (output index 3: throttle_out, disconnect_out,
    // quota_decision, metrics).
    s.metrics_out = unsafe { dev_channel_port(&*syscalls.cast::<SyscallTable>(), 1, 3) };
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<QuotaState>() };
    refill_all(&mut s.table);
    let sys = s.syscalls;
    if sys.is_null() {
        return 0;
    }
    let mut scratch = [0u8; 64];
    unsafe {
        if let Some((mt, len)) = read_envelope(&*sys, s.in_chan, &mut scratch) {
            if mt == MSG_QUOTA_TICK && len >= 6 {
                let tenant = u32::from_le_bytes([scratch[0], scratch[1], scratch[2], scratch[3]]);
                let cost = u16::from_le_bytes([scratch[4], scratch[5]]);
                let (allow, disc) = consume(&mut s.table, tenant, cost);
                s.m_checks += 1;
                if !allow {
                    s.m_throttles += 1;
                    let mut body = [0u8; 9];
                    body[0..4].copy_from_slice(&tenant.to_le_bytes());
                    body[4] = THROTTLE_REASON_RATE;
                    body[5..9].copy_from_slice(&100u32.to_le_bytes());
                    let _ = write_envelope(&*sys, s.out_chan, MSG_QUOTA_THROTTLE, &body);
                    if disc {
                        s.m_disconnects += 1;
                        let body = [0u8, DISCONNECT_REASON_NOISY];
                        let _ = write_envelope(&*sys, s.out_chan, MSG_QUOTA_DISCONNECT, &body);
                    }
                }
            }
        }

        // Phase-14: emit module-scope counters on the metrics port at a
        // coarse cadence (no-op until wired). ids follow manifest order.
        s.step_ctr = s.step_ctr.wrapping_add(1);
        if s.step_ctr.is_multiple_of(5000) {
            telemetry::emit_counters(
                &*sys,
                s.metrics_out,
                &[s.m_checks, s.m_throttles, s.m_disconnects],
            );
        }
    }
    0
}
