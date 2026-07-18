//! auth_manager — adapter-facing auth and identity translation.
//!
//! Phase 6b minimal implementation. Maintains a static principal
//! table keyed by `(protocol, credential bytes)` and answers
//! `MSG_AUTH_REQUEST` envelopes with `MSG_AUTH_DECISION`. Real
//! deployments would source the table from a CP-Raft principal
//! manifest; today we ship with an in-memory list seeded at
//! `module_new` via `params` so tests can drive it deterministically.
//!
//! Wire shapes (see `modules/common/wire.rs`):
//!
//!   MSG_AUTH_REQUEST  payload:
//!     `[protocol:u8][tenant_id:u32 LE][cred_len:u16 LE][cred…]`
//!
//!   MSG_AUTH_DECISION payload:
//!     `[decision:u8][principal_len:u8][principal…]`
//!     `decision`: 0 = deny, 1 = allow.

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

use wire::{MSG_AUTH_DECISION, MSG_AUTH_REQUEST};

#[path = "../../common/auth_table.rs"]
mod auth_table;
use auth_table::{decide, AuthTable, PrincipalEntry, MAX_PRINCIPALS};

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[repr(C)]
struct AuthState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    // Dedicated `metrics` output port (output index 3), discovered in
    // module_new; -1 when unwired.
    metrics_out: i32,
    _pad: i32,
    // Phase-14 counters (ids follow manifest `[observability] metrics`).
    m_requests: u64,
    m_allows: u64,
    m_denies: u64,
    step_ctr: u64,
    table: AuthTable,
}

impl AuthState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.in_chan = -1;
        self.out_chan = -1;
        self.metrics_out = -1;
        self._pad = 0;
        self.m_requests = 0;
        self.m_allows = 0;
        self.m_denies = 0;
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
    let n = (sys.channel_read)(chan, hdr.as_mut_ptr(), 3);
    if n < 3 {
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
    let n2 = (sys.channel_read)(chan, scratch.as_mut_ptr(), len);
    if (n2 as usize) < len {
        return None;
    }
    Some((mt, len))
}

unsafe fn write_envelope(sys: &SyscallTable, chan: i32, mt: u8, body: &[u8]) -> bool {
    if chan < 0 || body.len() > u16::MAX as usize {
        return false;
    }
    let mut buf = [0u8; 256];
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
    core::mem::size_of::<AuthState>() as u32
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
    if state_size < core::mem::size_of::<AuthState>() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<AuthState>() };
    s.init(syscalls.cast::<SyscallTable>());
    s.in_chan = in_chan;
    s.out_chan = out_chan;
    // Dedicated telemetry port (output index 3: auth_decision,
    // decision_request, audit, metrics).
    s.metrics_out = unsafe { dev_channel_port(&*syscalls.cast::<SyscallTable>(), 1, 3) };
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<AuthState>() };
    let sys = s.syscalls;
    if sys.is_null() {
        return 0;
    }
    let mut scratch = [0u8; 256];
    unsafe {
        if let Some((mt, len)) = read_envelope(&*sys, s.in_chan, &mut scratch) {
            if mt == MSG_AUTH_REQUEST && len >= 7 {
                let protocol = scratch[0];
                let _tenant = u32::from_le_bytes([scratch[1], scratch[2], scratch[3], scratch[4]]);
                let cred_len = u16::from_le_bytes([scratch[5], scratch[6]]) as usize;
                if 7 + cred_len <= len {
                    let cred = &scratch[7..7 + cred_len];
                    let mut out = [0u8; 64];
                    if let Some(n) = decide(&s.table, protocol, cred, &mut out) {
                        s.m_requests += 1;
                        if out[0] == 1 {
                            s.m_allows += 1;
                        } else {
                            s.m_denies += 1;
                        }
                        let _ = write_envelope(&*sys, s.out_chan, MSG_AUTH_DECISION, &out[..n]);
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
                &[s.m_requests, s.m_allows, s.m_denies],
            );
        }
    }
    0
}
