//! Partition demultiplexer — dense partition hosting.
//!
//! In `partition_fanout` mode the `kv_request_router` keeps partition 0
//! on its direct ports and prefixes every frame bound for a higher
//! partition with `[partition_id:u16 LE]`, writing it to a single
//! multiplexed output. This module strips the tag and forwards the frame
//! to `part<partition_id>_out`, so N partitions cost the router ONE
//! output port instead of N — lifting the 2-partition ceiling that the
//! fluxor 16-output-port cap otherwise imposes on the keyed data path.
//!
//! Purely mechanical and stateless per frame: read one envelope, take the
//! partition id off the front of its payload, re-emit the SAME envelope
//! type with the remaining payload on the owning egress. A tag naming a
//! partition this instance does not serve (out of range or unwired) is
//! dropped and counted — never folded onto a wrong partition, which would
//! corrupt another range's log or store.

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

const ENV_BUF: usize = 4096;
/// Egress ports part0_out..part7_out.
const MAX_LOCAL_PARTITIONS: usize = 8;

define_params! {
    DemuxState;

    // Partitions this instance serves (1..=8). A tag at or above this is
    // dropped: the port for it is not wired.
    1, n_partitions, u8, 2
        => |s, d, len| { s.n_partitions = p_u8(d, len, 0, 2); };
}

#[repr(C)]
struct DemuxState {
    syscalls: *const SyscallTable,
    tagged_in: i32,
    outs: [i32; MAX_LOCAL_PARTITIONS],
    metrics_out: i32,
    n_partitions: u8,
    step_ctr: u64,
    m_routed: u64,
    m_dropped: u64,
    env: [u8; ENV_BUF],
}

impl DemuxState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.tagged_in = -1;
        self.outs = [-1; MAX_LOCAL_PARTITIONS];
        self.metrics_out = -1;
        self.n_partitions = 2;
        self.step_ctr = 0;
        self.m_routed = 0;
        self.m_dropped = 0;
        self.env = [0; ENV_BUF];
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<DemuxState>() as u32
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
    if state_size < core::mem::size_of::<DemuxState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let s = unsafe { &mut *state.cast::<DemuxState>() };
    s.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(s, params, params_len) };
    }
    s.tagged_in = in_chan;
    // Egress ports: part0_out is the module's primary output (out_chan);
    // part1_out..part7_out follow on output group 1.
    s.outs[0] = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        for i in 1..MAX_LOCAL_PARTITIONS {
            s.outs[i] = dev_channel_port(sys, 1, i as u8);
        }
        // metrics is the output AFTER the 8 partition egresses.
        s.metrics_out = dev_channel_port(sys, 1, MAX_LOCAL_PARTITIONS as u8);
    }
    if s.n_partitions == 0 || s.n_partitions as usize > MAX_LOCAL_PARTITIONS {
        return -1;
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<DemuxState>() };
    let sys_ptr = s.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }

    // Fan a bounded number of frames per step so one busy demux cannot
    // starve the scheduler.
    unsafe {
        let sys = &*sys_ptr;
        for _ in 0..16 {
            let mut scratch = [0u8; ENV_BUF];
            let Some((msg, payload)) = read_one_envelope(sys, s.tagged_in, &mut scratch) else {
                break;
            };
            if payload.len() < 2 {
                s.m_dropped = s.m_dropped.wrapping_add(1);
                continue;
            }
            let pid = u16::from_le_bytes([payload[0], payload[1]]) as usize;
            let body = &payload[2..];
            if pid >= s.n_partitions as usize || pid >= MAX_LOCAL_PARTITIONS || s.outs[pid] < 0 {
                // A tag we cannot serve is dropped, never re-homed: folding
                // it onto another partition would corrupt that range.
                s.m_dropped = s.m_dropped.wrapping_add(1);
                continue;
            }
            let at = wire::ENVELOPE_HDR;
            if at + body.len() > s.env.len() {
                s.m_dropped = s.m_dropped.wrapping_add(1);
                continue;
            }
            s.env[at..at + body.len()].copy_from_slice(body);
            if write_envelope(sys, s.outs[pid], msg, body.len(), &mut s.env) {
                s.m_routed = s.m_routed.wrapping_add(1);
            } else {
                s.m_dropped = s.m_dropped.wrapping_add(1);
            }
        }
    }

    s.step_ctr = s.step_ctr.wrapping_add(1);
    if s.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(&*sys_ptr, s.metrics_out, &[s.m_routed, s.m_dropped]);
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
