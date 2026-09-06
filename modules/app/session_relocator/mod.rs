//! session_relocator — turns a placement decision into a worker
//! relocation on the edge anchors.
//!
//! A placement event advances the cluster placement epoch and *causes
//! affected sessions to rebind*; it does not itself stamp any session's
//! epoch. This module is that cause. On a `MSG_PLACEMENT_EPOCH_EVENT` (or an operator
//! `MSG_SESSION_RELOCATE` command on `cmd_in`) it emits
//! `MSG_SESSION_RELOCATE{target_worker}` to each wired anchor, which
//! drives a per-session SessionCtrlV1 handoff from the source worker
//! onto the target — each rebind advancing only that session's epoch.
//!
//! It carries no session state: the anchor owns the handoff machine,
//! the workers own the movable state, and the directory owns the
//! binding. The relocator only decides *when* and *to which worker*.
//!
//! Ports:
//! - **`epoch_events`** — `MSG_PLACEMENT_EPOCH_EVENT` from substrate
//!   `control_plane`.
//! - **`cmd_in`** — operator `MSG_SESSION_RELOCATE` (a manual drain of
//!   a worker for maintenance).
//! - **`relocate_out`** — `MSG_SESSION_RELOCATE` to the anchors.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    clippy::duplicate_mod,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, and ABI fns carry a fixed arity"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/session_core.rs"]
mod session_core;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use session_core::{placement_event_epoch, MSG_PLACEMENT_EPOCH_EVENT_CP};
use wire::{MSG_PLACEMENT_EPOCH_EVENT, MSG_SESSION_RELOCATE};

const SCRATCH_BUF_SIZE: usize = 256;

#[repr(C)]
struct RelocatorState {
    syscalls: *const SyscallTable,

    epoch_events_in: i32,
    cmd_in: i32,
    relocate_out: i32,
    metrics_out: i32,

    /// The worker the next placement event moves sessions ONTO. It
    /// alternates: a graph has an active worker and a standby, and a
    /// placement event drains whichever is currently active onto the
    /// other. Starts at 1 (drain worker 0 onto worker 1 first).
    next_target: u8,
    /// Placement epoch we last acted on (monotonic guard).
    last_epoch: u32,

    m_relocations: u64,
    step_ctr: u64,
    /// Test/bring-up self-trigger: emit one relocation once `step_ctr`
    /// reaches this (0 = disabled). Production drives relocation from
    /// placement events, not this.
    fire_after_steps: u64,
    fired: bool,

    scratch: [u8; SCRATCH_BUF_SIZE],
}

define_params! {
    RelocatorState;

    // Number of anchor workers to alternate between (2 by default).
    1, workers, u8, 2
        => |s, d, len| { let _ = p_u8(d, len, 0, 2); let _ = s; };

    // Self-fire after this many steps (0 = disabled). Bring-up / tests.
    2, fire_after_steps, u32, 0
        => |s, d, len| { s.fire_after_steps = p_u32(d, len, 0, 0) as u64; };
}

impl RelocatorState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.epoch_events_in = -1;
        self.cmd_in = -1;
        self.relocate_out = -1;
        self.metrics_out = -1;
        self.next_target = 1;
        self.last_epoch = 0;
        self.m_relocations = 0;
        self.step_ctr = 0;
        self.fire_after_steps = 0;
        self.fired = false;
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
    let msg_type = hdr[0];
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len > scratch.len() {
        return None;
    }
    if payload_len == 0 {
        return Some((msg_type, 0));
    }
    let n2 = (sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return None;
    }
    Some((msg_type, payload_len))
}

fn emit_relocate(st: &mut RelocatorState, target: u8) {
    let mut buf = [0u8; 4];
    buf[0] = MSG_SESSION_RELOCATE;
    buf[1] = 1;
    buf[2] = 0;
    buf[3] = target;
    unsafe {
        let sys = st.syscalls;
        if !sys.is_null() && st.relocate_out >= 0 {
            let _ = ((*sys).channel_write)(st.relocate_out, buf.as_mut_ptr(), 4);
        }
    }
    st.m_relocations = st.m_relocations.wrapping_add(1);
    // Next placement event drains the other way.
    st.next_target ^= 1;
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<RelocatorState>() as u32
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
    if state_size < core::mem::size_of::<RelocatorState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let st = unsafe { &mut *state.cast::<RelocatorState>() };
    st.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(st, params, params_len) };
    }

    // inputs: epoch_events[0], cmd_in[1]; outputs: relocate_out[0], metrics[1]
    st.epoch_events_in = in_chan;
    st.relocate_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        st.cmd_in = dev_channel_port(sys, 0, 1);
        st.metrics_out = dev_channel_port(sys, 1, 1);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let st = unsafe { &mut *state.cast::<RelocatorState>() };
    unsafe {
        let sys_ptr = st.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;

        if let Some((mt, len)) = read_envelope(sys, st.epoch_events_in, &mut st.scratch) {
            if mt == MSG_PLACEMENT_EPOCH_EVENT || mt == MSG_PLACEMENT_EPOCH_EVENT_CP {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&st.scratch[..len]);
                if let Some(e) = placement_event_epoch(mt, &tmp[..len]) {
                    if e > st.last_epoch {
                        st.last_epoch = e;
                        let target = st.next_target;
                        emit_relocate(st, target);
                    }
                }
            }
        }
        if let Some((mt, len)) = read_envelope(sys, st.cmd_in, &mut st.scratch) {
            // Operator-driven drain: the command names the target worker.
            if mt == MSG_SESSION_RELOCATE && len >= 1 {
                let target = st.scratch[0];
                emit_relocate(st, target);
            }
        }

        st.step_ctr = st.step_ctr.wrapping_add(1);
        if !st.fired && st.fire_after_steps != 0 && st.step_ctr >= st.fire_after_steps {
            st.fired = true;
            let target = st.next_target;
            emit_relocate(st, target);
        }
        if st.step_ctr.is_multiple_of(5000) && !st.syscalls.is_null() {
            telemetry::emit_counters(&*st.syscalls, st.metrics_out, &[st.m_relocations]);
        }
    }
    0
}
