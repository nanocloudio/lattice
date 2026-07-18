//! compaction_coordinator — retention claims and the MVCC GC floor.
//!
//! Two jobs, deliberately kept apart because they carry different
//! authority (see `modules/common/compaction_floor.rs` module docs):
//!
//! 1. **Advisory floor aggregation** (pre-existing). Consumes
//!    `MSG_RETENTION_FLOOR` from multiple sources, keeps the per-kpg
//!    minimum, and emits `MSG_COMPACTION_FLOOR` on change. A hint for
//!    the substrate; nothing irreversible hangs off it.
//!
//! 2. **The MVCC GC floor** (RFC §18, §21 invariant 13). Collects
//!    fenced `MSG_RETENTION_CLAIM` records, computes a candidate floor
//!    clamped to the timestamp allocator's COMMITTED high water, and
//!    **proposes** it (`MSG_GC_FLOOR_PROPOSE`). It never advances the
//!    floor locally. Only a `MSG_GC_FLOOR_COMMITTED` record coming back
//!    through consensus moves `committed_floor`, and only that value is
//!    republished as the authoritative `MSG_COMPACTION_FLOOR` that
//!    `kv_state_worker` is allowed to compact behind.
//!
//! The operator claim is synthesized locally from the
//! `operator_retain_revisions` param rather than arriving as a message:
//! it IS configuration. Its default retains everything, so a graph that
//! has not opted into GC proposes nothing at all.
//!
//! Wire shapes (`modules/common/wire.rs`):
//!   in  MSG_RETENTION_FLOOR   `[source:u8][kpg_id:u16 LE][floor:u64 LE]`
//!   in  MSG_RETENTION_CLAIM   encoded `RetentionClaim` (40 B)
//!   in  MSG_TS_LEASE          encoded `TimestampLease` (40 B)
//!   in  MSG_GC_FLOOR_COMMITTED encoded `GcFloorRecord` (32 B)
//!   out MSG_COMPACTION_FLOOR  `[kpg_id:u16 LE][floor_revision:u64 LE]`
//!   out MSG_GC_FLOOR_PROPOSE  encoded `GcFloorRecord` (32 B)

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

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

use wire::{
    MSG_COMPACTION_FLOOR, MSG_GC_FLOOR_COMMITTED, MSG_GC_FLOOR_PROPOSE, MSG_RETENTION_CLAIM,
    MSG_RETENTION_FLOOR,
};

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/compaction_floor.rs"]
mod compaction_floor;
use compaction_floor::{
    apply, ClaimTable, CompactionTable, FloorBlocked, GcFloorRecord, RetentionClaim,
    CLAIM_SOURCE_OPERATOR, DEFAULT_REQUIRED_SOURCES, GC_CLAIM_WIRE_LEN, GC_FLOOR_WIRE_LEN,
};

// Read-only mount: the allocator's committed record is what establishes
// the high water this module clamps against. `mvcc.rs` is the authority
// for that layout; decoding it anywhere else would fork the format.
#[path = "../../common/mvcc.rs"]
mod mvcc;
use mvcc::{TimestampLease, LEASE_WIRE_LEN, MSG_TS_LEASE};

#[path = "../../common/telemetry.rs"]
mod telemetry;

/// Steps between GC-floor evaluations. The floor is a slow control
/// loop — proposing on every step would put a consensus round trip in
/// the way of every claim update for no benefit.
const PROPOSE_INTERVAL_STEPS: u64 = 1000;

/// Scratch large enough for the biggest inbound payload.
const SCRATCH: usize = 64;

define_params! {
    CompState;

    // How many revisions below the allocator's committed high water the
    // operator wants retained. The DEFAULT retains everything
    // (`u32::MAX` below any realistic high water clamps the operator
    // claim to 0), so GC is inert until a deployment opts in — the
    // conservative direction, since over-retention only costs space
    // while under-retention destroys history someone is owed.
    1, operator_retain_revisions, u32, 0xFFFF_FFFF
        => |s, d, len| { s.operator_retain_revisions = p_u32(d, len, 0, 0xFFFF_FFFF); };

    // Identity stamped into proposed floor records so a committed
    // record names who decided it.
    2, proposer_id, u32, 1
        => |s, d, len| { s.proposer_id = p_u32(d, len, 0, 1); };

    // Bit set of claim sources that MUST be fresh before the floor may
    // advance (see `compaction_floor::source_bit`). Default = the two
    // sources that exist today: active-read (bit 0) + operator (bit 1).
    3, required_sources, u8, 3
        => |s, d, len| { s.required_sources = p_u8(d, len, 0, 3); };
}

#[repr(C)]
struct CompState {
    syscalls: *const SyscallTable,
    in_chan: i32,
    out_chan: i32,
    // Additional inputs, discovered in module_new; -1 when unwired.
    watch_floor_in: i32,
    lease_floor_in: i32,
    claims_in: i32,
    gc_committed_in: i32,
    // Dedicated `metrics` output port (output index 1), and the GC
    // proposal port (output index 2).
    metrics_out: i32,
    gc_propose_out: i32,

    // Params.
    operator_retain_revisions: u32,
    proposer_id: u32,
    required_sources: u8,
    _pad: [u8; 3],

    // Phase-14 counters (ids follow manifest `[observability] metrics`).
    m_updates: u64,
    m_floors_emitted: u64,
    m_claims: u64,
    m_proposals: u64,
    m_commits: u64,
    m_blocked: u64,
    step_ctr: u64,
    table: CompactionTable,
    claims: ClaimTable,
}

impl CompState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.in_chan = -1;
        self.out_chan = -1;
        self.watch_floor_in = -1;
        self.lease_floor_in = -1;
        self.claims_in = -1;
        self.gc_committed_in = -1;
        self.metrics_out = -1;
        self.gc_propose_out = -1;
        self.operator_retain_revisions = u32::MAX;
        self.proposer_id = 1;
        self.required_sources = DEFAULT_REQUIRED_SOURCES;
        self._pad = [0; 3];
        self.m_updates = 0;
        self.m_floors_emitted = 0;
        self.m_claims = 0;
        self.m_proposals = 0;
        self.m_commits = 0;
        self.m_blocked = 0;
        self.step_ctr = 0;
        self.table.init();
        self.claims.init(DEFAULT_REQUIRED_SOURCES);
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
    if chan < 0 || body.len() > SCRATCH {
        return false;
    }
    let mut buf = [0u8; 3 + SCRATCH];
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
    core::mem::size_of::<CompState>() as u32
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
    if state_size < core::mem::size_of::<CompState>() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<CompState>() };
    s.init(syscalls.cast::<SyscallTable>());
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(s, params, params_len) };
    }
    s.claims.init(s.required_sources);
    s.in_chan = in_chan;
    s.out_chan = out_chan;
    unsafe {
        let sys = &*syscalls.cast::<SyscallTable>();
        s.watch_floor_in = dev_channel_port(sys, 0, 1);
        s.lease_floor_in = dev_channel_port(sys, 0, 2);
        s.claims_in = dev_channel_port(sys, 0, 3);
        s.gc_committed_in = dev_channel_port(sys, 0, 4);
        // Dedicated telemetry port (output index 1), GC proposals (2).
        s.metrics_out = dev_channel_port(sys, 1, 1);
        s.gc_propose_out = dev_channel_port(sys, 1, 2);
    }
    0
}

/// Handle one decoded envelope from any inbound port. Message TYPE, not
/// port, decides the meaning — a claim source and a floor declaration
/// can share a channel without ambiguity.
unsafe fn handle(s: &mut CompState, sys: &SyscallTable, mt: u8, body: &[u8]) {
    match mt {
        MSG_RETENTION_FLOOR if body.len() >= 11 => {
            let source = body[0];
            let kpg = u16::from_le_bytes([body[1], body[2]]);
            let floor = u64::from_le_bytes([
                body[3], body[4], body[5], body[6], body[7], body[8], body[9], body[10],
            ]);
            s.m_updates += 1;
            if let Some((kpg_id, agg)) = apply(&mut s.table, source, kpg, floor) {
                s.m_floors_emitted += 1;
                let mut out = [0u8; 10];
                out[0..2].copy_from_slice(&kpg_id.to_le_bytes());
                out[2..10].copy_from_slice(&agg.to_le_bytes());
                let _ = write_envelope(sys, s.out_chan, MSG_COMPACTION_FLOOR, &out);
            }
        }
        MSG_RETENTION_CLAIM if body.len() == GC_CLAIM_WIRE_LEN => {
            if let Some(claim) = RetentionClaim::decode(body) {
                if s.claims.observe_claim(&claim) {
                    s.m_claims += 1;
                }
            }
        }
        // The allocator's committed record is the ONLY thing that
        // establishes the clamp. An uncommitted grant would let a
        // leader authorize reclaiming history it had not durably
        // reserved (`mvcc.rs`, durability before issuance).
        MSG_TS_LEASE if body.len() == LEASE_WIRE_LEN => {
            if let Some(lease) = TimestampLease::decode(body) {
                s.claims.observe_high_water(lease.high_water());
            }
        }
        // The committed decision. This is the one path that advances
        // anything, and the floor it publishes downstream is the only
        // floor a provider may reclaim behind (§21 invariant 13).
        MSG_GC_FLOOR_COMMITTED if body.len() == GC_FLOOR_WIRE_LEN => {
            if let Some(rec) = GcFloorRecord::decode(body) {
                if let Some(floor) = s.claims.observe_committed_floor(&rec) {
                    s.m_commits += 1;
                    let mut out = [0u8; 10];
                    out[0..2].copy_from_slice(&rec.kpg_id.to_le_bytes());
                    out[2..10].copy_from_slice(&floor.to_le_bytes());
                    let _ = write_envelope(sys, s.out_chan, MSG_COMPACTION_FLOOR, &out);
                }
            }
        }
        _ => {}
    }
}

/// Re-publish the locally configured operator claim, then try to
/// propose a floor for every kpg we hold claims for.
///
/// The operator claim never expires (it is configuration, not a live
/// holder) and is derived from the COMMITTED high water, so it moves
/// only when the allocator's durable position moves.
unsafe fn evaluate(s: &mut CompState, sys: &SyscallTable, now_ms: u64) {
    if !s.claims.high_water_established {
        return;
    }
    let operator_floor = s
        .claims
        .committed_high_water
        .saturating_sub(u64::from(s.operator_retain_revisions));
    let len = s.claims.len as usize;
    let mut i = 0usize;
    while i < len {
        let kpg_id = s.claims.entries[i].kpg_id;
        // The sequence advances once per evaluation so republishing the
        // same configured floor is not rejected as non-monotone.
        let claim = RetentionClaim {
            kpg_id,
            source: CLAIM_SOURCE_OPERATOR,
            claim_id: u64::from(s.proposer_id),
            floor_revision: operator_floor,
            expiry_unix_ms: 0,
            seq: (s.step_ctr / PROPOSE_INTERVAL_STEPS) as u32,
        };
        let _ = s.claims.observe_claim(&claim);

        match s.claims.plan_propose(kpg_id, now_ms, s.proposer_id) {
            Ok(rec) => {
                let mut buf = [0u8; GC_FLOOR_WIRE_LEN];
                if rec.encode(&mut buf).is_some()
                    && write_envelope(sys, s.gc_propose_out, MSG_GC_FLOOR_PROPOSE, &buf)
                {
                    s.m_proposals += 1;
                } else {
                    // The proposal never left; do not sit in
                    // `AwaitingCommit` waiting for a confirmation that
                    // can never arrive.
                    s.claims.entries[i].awaiting_commit = false;
                    s.claims.entries[i].proposed_floor = 0;
                }
            }
            Err(FloorBlocked::NoAdvance { .. }) | Err(FloorBlocked::ProposalOutstanding) => {}
            Err(_) => s.m_blocked += 1,
        }
        i += 1;
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<CompState>() };
    let sys = s.syscalls;
    if sys.is_null() {
        return 0;
    }
    let mut scratch = [0u8; SCRATCH];
    unsafe {
        let sysr = &*sys;
        // One envelope per wired input per step — bounded work, and no
        // input can starve another.
        for chan in [
            s.in_chan,
            s.watch_floor_in,
            s.lease_floor_in,
            s.claims_in,
            s.gc_committed_in,
        ] {
            if let Some((mt, len)) = read_envelope(sysr, chan, &mut scratch) {
                handle(s, sysr, mt, &scratch[..len]);
            }
        }

        s.step_ctr = s.step_ctr.wrapping_add(1);
        if s.step_ctr.is_multiple_of(PROPOSE_INTERVAL_STEPS) {
            let now_ms = dev_millis(sysr);
            evaluate(s, sysr, now_ms);
        }

        // Phase-14: emit module-scope counters on the metrics port at a
        // coarse cadence (no-op until wired). ids follow manifest order.
        if s.step_ctr.is_multiple_of(5000) {
            telemetry::emit_counters(
                sysr,
                s.metrics_out,
                &[
                    s.m_updates,
                    s.m_floors_emitted,
                    s.m_claims,
                    s.m_proposals,
                    s.m_commits,
                    s.m_blocked,
                ],
            );
        }
    }
    0
}
