//! timestamp_allocator — the replicated MVCC timestamp authority
//! (RFC database foundation §10, Phase 2 slice 1).
//!
//! # Core invariant: durability before issuance
//!
//! **A timestamp interval is handed out only after the high water
//! covering it has been durably committed.** Nothing this module has
//! merely *proposed* is issuable. Concretely, `try_issue` cuts only from
//! `[issued_high_water, committed_high_water)`, and
//! `committed_high_water` moves for exactly one reason: a record arrived
//! on `committed_state`.
//!
//! The failure this prevents is not hypothetical. If the allocator
//! granted from an advance it had only proposed, and that proposal were
//! lost to a leadership change before commit, the next allocator would
//! recover a lower high water and re-issue the very same timestamps —
//! two different writes at one MVCC timestamp, which is silent
//! corruption of every snapshot read taken afterwards. §21 invariant 3
//! (apply is deterministic from prior state plus committed command) and
//! invariant 2 (nothing claims durability before the durable proof) are
//! the same rule seen from the two ends.
//!
//! So the request path is:
//!
//! ```text
//!   request → covered by the committed reserve?
//!             ├─ yes → grant immediately, advance issued_high_water
//!             └─ no  → HOLD the request in a bounded queue,
//!                      propose a high-water advance,
//!                      wait for it on committed_state,
//!                      then grant.
//! ```
//!
//! and there is no fourth branch. A full hold queue is a typed
//! `REJECT_QUEUE_FULL`, not a grant from thin air (§21 invariant 14:
//! exhaustion produces bounded backpressure or typed failure, never a
//! weaker consistency mode).
//!
//! # Restart and leadership change
//!
//! On init the allocator knows **nothing** — not even that it is the
//! same process that ran a second ago. It starts `Unestablished` and
//! answers every request with `REJECT_NOT_READY`. Only committed records
//! establish it, and because those records belong to a previous
//! incarnation, folding them in adopts the high water but *no reserve*
//! (`issued_high_water` is snapped up to `committed_high_water`) and
//! bumps this allocator's epoch strictly past every epoch it saw. Its
//! first grant therefore cannot happen until its own, epoch-bumped
//! advance has committed. That is §10's "move beyond every previously
//! issued interval" made structural rather than checked.
//!
//! # Bounds
//!
//! Fixed hold queue (`HOLD_CAPACITY`), no allocation, at most one
//! inbound message consumed and at most one outbound envelope written
//! per step, one advance proposal outstanding at a time.
//!
//! Ports:  in  0 `lease_request`, 1 `committed_state`
//!         out 0 `lease_grant`, 1 `propose_out`, 2 `metrics`
//! Params: `allocator_id`, `advance_chunk`.
//!
//! Record format, succession rules, and the reserve state machine live
//! in `modules/common/mvcc.rs` and are host-tested in
//! `tests/contract_mvcc.rs`; this file is the I/O pump over them.

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

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/mvcc.rs"]
mod mvcc;

use mvcc::{
    encode_grant, encode_reject, IssueError, LeaseRequest, ReservePhase, ReserveState,
    TimestampLease, DEFAULT_ADVANCE_CHUNK, ISSUED_TO_NONE, LEASE_GRANT_WIRE_LEN,
    LEASE_REJECT_WIRE_LEN, LEASE_REQUEST_WIRE_LEN, LEASE_WIRE_LEN, MSG_TS_LEASE,
    MSG_TS_LEASE_GRANT, MSG_TS_LEASE_REJECT, MSG_TS_LEASE_REQUEST, MSG_WAL_REPLAY_COMPLETE,
    REJECT_QUEUE_FULL,
};

#[path = "../../common/telemetry.rs"]
mod telemetry;

/// Requests held while an advance commits. Fixed; overflow is a typed
/// reject, never a grow.
const HOLD_CAPACITY: usize = 32;

/// Scratch big enough for the largest inbound payload (a 40-byte lease
/// record) with headroom.
const SCRATCH: usize = 64;

/// Metric emit cadence, in steps. Matches the other app modules.
const EMIT_EVERY: u64 = 5000;

/// Expiry stamped into granted leases.
///
/// Deliberately zero in v1: the only clock this module could read is a
/// LOCAL one, and a locally sampled value written into a replicated
/// record is exactly the non-determinism §21 invariant 3 forbids and
/// `mvcc::HYBRID_NOT_ENABLED` warns about. Lease expiry becomes real
/// when it can be driven from the replicated tick source
/// (`ttl_scheduler`'s `MSG_LEASE_TICK`) — a later Phase-2 slice. Until
/// then a zero expiry means "no administrative deadline", and nothing in
/// the ordering path ever reads the field anyway.
const LEASE_EXPIRY_NONE: u64 = 0;

#[repr(C)]
struct AllocState {
    syscalls: *const SyscallTable,
    // in 0 / out 0 (module_new args), remaining ports discovered by index.
    request_in: i32,
    grant_out: i32,
    committed_in: i32,
    propose_out: i32,
    metrics_out: i32,

    // ── params ──
    allocator_id: u32,
    advance_chunk: u32,
    /// Auto-grant target: `issued_to` identity for the
    /// unsolicited worker leases this allocator proposes as replicated
    /// `MSG_TS_LEASE_GRANT` records. `0` = auto-grant disabled
    /// (request/reply only).
    auto_grant_to: u32,
    /// Timestamps per auto-granted lease.
    auto_grant_size: u32,
    /// Steps between bootstrap probes while `Unestablished`, and
    /// between reserve top-up checks once established. Grants
    /// themselves are demand-driven (the worker requests on
    /// `lease_request` when low), so an idle graph proposes nothing
    /// after the initial establishment + first grant.
    auto_grant_interval_steps: u32,
    /// Wrap outbound proposals in clustor's `MSG_CLIENT_PROPOSAL`
    /// envelope with `LATTICE_RECORD_TAG` (`1`, for graphs whose
    /// `propose_out` is wired to `gateway.client_requests`), or emit
    /// bare records (`0`, the contract-test surface).
    propose_wrap: u32,

    /// Step counter driving the auto-grant/bootstrap cadence.
    auto_ctr: u32,

    // ── Phase-14 counters (ids follow manifest `[observability] metrics`) ──
    m_grants: u64,
    m_proposals: u64,
    m_holds: u64,
    m_rejects: u64,
    step_ctr: u64,

    // ── the §10 state machine ──
    reserve: ReserveState,

    // Bounded FIFO of held requests: `hold[head .. head+len)` modulo
    // capacity. FIFO because a starved requester must eventually be
    // served; LIFO would let a busy stream indefinitely postpone one.
    hold: [LeaseRequest; HOLD_CAPACITY],
    hold_head: u16,
    hold_len: u16,
}

impl AllocState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.request_in = -1;
        self.grant_out = -1;
        self.committed_in = -1;
        self.propose_out = -1;
        self.metrics_out = -1;
        self.allocator_id = 1;
        self.advance_chunk = DEFAULT_ADVANCE_CHUNK as u32;
        self.auto_grant_to = 0;
        self.auto_grant_size = DEFAULT_ADVANCE_CHUNK as u32;
        self.auto_grant_interval_steps = 2000;
        self.propose_wrap = 0;
        self.auto_ctr = 0;
        self.m_grants = 0;
        self.m_proposals = 0;
        self.m_holds = 0;
        self.m_rejects = 0;
        self.step_ctr = 0;
        self.reserve.init(1);
        self.hold = [LeaseRequest {
            corr_id: 0,
            requester: 0,
            size: 0,
        }; HOLD_CAPACITY];
        self.hold_head = 0;
        self.hold_len = 0;
    }

    /// Push one request onto the hold queue. `false` at capacity — the
    /// caller answers with `REJECT_QUEUE_FULL`.
    fn hold_push(&mut self, req: LeaseRequest) -> bool {
        if self.hold_len as usize >= HOLD_CAPACITY {
            return false;
        }
        let idx = (self.hold_head as usize + self.hold_len as usize) % HOLD_CAPACITY;
        self.hold[idx] = req;
        self.hold_len += 1;
        true
    }

    /// Oldest held request, without removing it.
    fn hold_peek(&self) -> Option<LeaseRequest> {
        if self.hold_len == 0 {
            return None;
        }
        Some(self.hold[self.hold_head as usize])
    }

    fn hold_pop(&mut self) {
        if self.hold_len == 0 {
            return;
        }
        self.hold_head = ((self.hold_head as usize + 1) % HOLD_CAPACITY) as u16;
        self.hold_len -= 1;
    }
}

define_params! {
    AllocState;

    // Allocator identity (§10: leases carry identities). Distinct per
    // allocator instance; a change of identity in the committed record
    // is what a successor detects as a leadership change, so two live
    // allocators sharing an id would be indistinguishable in the log.
    1, allocator_id, u32, 1
        => |s, d, len| { s.allocator_id = p_u32(d, len, 0, 1); };

    // Timestamps reserved per high-water advance. Larger = fewer
    // consensus round trips per grant, more timestamps stranded by a
    // crash. Clamped to mvcc::MAX_LEASE_INTERVAL at use.
    2, advance_chunk, u32, 65536
        => |s, d, len| { s.advance_chunk = p_u32(d, len, 0, 65536); };

    // Auto-grant target identity: non-zero enables the
    // unsolicited replicated worker-lease loop (see AllocState docs).
    3, auto_grant_to, u32, 0
        => |s, d, len| { s.auto_grant_to = p_u32(d, len, 0, 0); };

    // Timestamps per auto-granted worker lease.
    4, auto_grant_size, u32, 65536
        => |s, d, len| { s.auto_grant_size = p_u32(d, len, 0, 65536); };

    // Steps between auto-grant proposals / bootstrap probes.
    5, auto_grant_interval_steps, u32, 2000
        => |s, d, len| { s.auto_grant_interval_steps = p_u32(d, len, 0, 2000); };

    // 1 = wrap proposals for gateway.client_requests (see AllocState).
    6, propose_wrap, u32, 0
        => |s, d, len| { s.propose_wrap = p_u32(d, len, 0, 0); };
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
    let mut buf = [0u8; SCRATCH + 3];
    let total = 3 + body.len();
    buf[0] = mt;
    buf[1] = (body.len() & 0xFF) as u8;
    buf[2] = ((body.len() >> 8) & 0xFF) as u8;
    buf[3..total].copy_from_slice(body);
    (sys.channel_write)(chan, buf.as_mut_ptr(), total) == total as i32
}

/// Emit one grant. Returns false if the frame could not be written.
unsafe fn send_grant(
    s: &mut AllocState,
    sys: &SyscallTable,
    corr_id: u64,
    lease: &TimestampLease,
) -> bool {
    let mut body = [0u8; LEASE_GRANT_WIRE_LEN];
    if encode_grant(corr_id, lease, &mut body).is_none() {
        return false;
    }
    let ok = write_envelope(sys, s.grant_out, MSG_TS_LEASE_GRANT, &body);
    if ok {
        s.m_grants += 1;
    }
    ok
}

/// Emit one typed rejection.
unsafe fn send_reject(s: &mut AllocState, sys: &SyscallTable, corr_id: u64, reason: u8) -> bool {
    let mut body = [0u8; LEASE_REJECT_WIRE_LEN];
    if encode_reject(corr_id, reason, &mut body).is_none() {
        return false;
    }
    let ok = write_envelope(sys, s.grant_out, MSG_TS_LEASE_REJECT, &body);
    if ok {
        s.m_rejects += 1;
    }
    ok
}

/// Write one record proposal on `propose_out` — bare (`[record_type]`
/// envelope, the contract-test surface) or, with `propose_wrap`,
/// wrapped for clustor's gateway as
/// `MSG_CLIENT_PROPOSAL [conn_id=0][LATTICE_RECORD_TAG][record_type]
/// [record…]` so the committed entry body demuxes at
/// `lattice_apply_bridge` (see `wire::LATTICE_RECORD_TAG`).
unsafe fn write_record_proposal(
    s: &mut AllocState,
    sys: &SyscallTable,
    record_type: u8,
    record: &[u8],
) -> bool {
    if s.propose_wrap == 0 {
        return write_envelope(sys, s.propose_out, record_type, record);
    }
    let payload_len = 3 + record.len();
    let total = 3 + payload_len;
    let mut buf = [0u8; SCRATCH + 16];
    if total > buf.len() {
        return false;
    }
    buf[0] = wire::MSG_CLIENT_PROPOSAL;
    buf[1] = (payload_len & 0xFF) as u8;
    buf[2] = ((payload_len >> 8) & 0xFF) as u8;
    buf[3] = 0; // conn_id: allocator-originated, no client connection
    buf[4] = wire::LATTICE_RECORD_TAG;
    buf[5] = record_type;
    buf[6..6 + record.len()].copy_from_slice(record);
    (sys.channel_write)(s.propose_out, buf.as_mut_ptr(), total) == total as i32
}

/// Propose a high-water advance large enough to serve `min_size`.
///
/// The proposal is only a proposal: `plan_advance` records it as
/// `pending_end` and moves the phase to `AwaitingCommit`, and nothing
/// about it becomes issuable until it returns on `committed_state`.
/// Returns true when an envelope was written.
unsafe fn propose_advance(s: &mut AllocState, sys: &SyscallTable, min_size: u64) -> bool {
    let chunk = s.advance_chunk as u64;
    let rec = match s.reserve.plan_advance(min_size, chunk) {
        Ok(r) => r,
        Err(_) => return false,
    };
    let mut body = [0u8; LEASE_WIRE_LEN];
    if rec.encode(&mut body).is_none() {
        return false;
    }
    if write_record_proposal(s, sys, MSG_TS_LEASE, &body) {
        s.m_proposals += 1;
        true
    } else {
        // The proposal never left. Roll the phase back so a later step
        // retries rather than waiting forever on a commit that can
        // never arrive.
        s.reserve.phase = ReservePhase::Ready;
        s.reserve.pending_end = 0;
        false
    }
}

/// The auto-grant / bootstrap cadence: while
/// `Unestablished`, propose an establishment probe
/// (`ReserveState::plan_bootstrap`); once established, cut a worker
/// lease from the committed reserve and propose it as a replicated
/// `MSG_TS_LEASE_GRANT` record `[corr_id=0][lease:40]` — the bridge
/// forwards it in-band to the state worker, which assigns per-write
/// commit timestamps from it in committed-log order. When the reserve
/// cannot cover a lease, propose an advance instead and grant on a
/// later cadence tick.
unsafe fn auto_grant_tick(s: &mut AllocState, sys: &SyscallTable) {
    if s.auto_grant_to == 0 || s.propose_out < 0 {
        return;
    }
    let interval = s.auto_grant_interval_steps.max(1);
    s.auto_ctr = s.auto_ctr.wrapping_add(1);
    if !u64::from(s.auto_ctr).is_multiple_of(u64::from(interval)) {
        return;
    }
    match s.reserve.phase {
        ReservePhase::Unestablished => {
            let Ok(probe) = s.reserve.plan_bootstrap(s.advance_chunk as u64) else {
                return;
            };
            let mut body = [0u8; LEASE_WIRE_LEN];
            if probe.encode(&mut body).is_some()
                && write_record_proposal(s, sys, MSG_TS_LEASE, &body)
            {
                s.m_proposals += 1;
            }
        }
        // Once established, grants are DEMAND-driven: the worker asks
        // on `lease_request` when its reserve runs low, and `serve`
        // answers with a replicated grant record. The cadence's only
        // established-phase job is keeping enough committed reserve
        // that a request can be served without waiting a full
        // propose/commit round trip.
        ReservePhase::Ready => {
            let size = u64::from(s.auto_grant_size.max(1));
            if s.reserve.free() < size {
                propose_advance(s, sys, size);
            }
        }
        ReservePhase::AwaitingCommit => {}
    }
}

/// Serve one request that is not already queued. Returns true when an
/// envelope was written (a grant, a rejection, or an advance proposal).
unsafe fn serve(s: &mut AllocState, sys: &SyscallTable, req: LeaseRequest, queued: bool) -> bool {
    match s
        .reserve
        .try_issue(req.size as u64, req.requester, LEASE_EXPIRY_NONE)
    {
        // A worker lease (requester == auto_grant_to) is
        // not answered directly: it is proposed as a replicated
        // MSG_TS_LEASE_GRANT record so it reaches the state worker
        // in-band, in committed-log order. Everyone else gets the
        // classic direct grant on `lease_grant`.
        Ok(lease) if s.auto_grant_to != 0 && req.requester == s.auto_grant_to => {
            let mut body = [0u8; LEASE_GRANT_WIRE_LEN];
            let ok = encode_grant(0, &lease, &mut body).is_some()
                && write_record_proposal(s, sys, MSG_TS_LEASE_GRANT, &body);
            if ok {
                s.m_grants += 1;
            }
            // A grant that failed to write strands its interval —
            // holes are harmless; rolling back risks double issuance.
            ok
        }
        Ok(lease) => send_grant(s, sys, req.corr_id, &lease),
        Err(IssueError::Insufficient { .. }) | Err(IssueError::ProposalOutstanding) => {
            // NOT a licence to issue. Hold the request and get the high
            // water advanced durably first.
            if !queued && !s.hold_push(req) {
                return send_reject(s, sys, req.corr_id, REJECT_QUEUE_FULL);
            }
            if !queued {
                s.m_holds += 1;
            }
            propose_advance(s, sys, req.size as u64)
        }
        Err(e) => {
            // NotReady / BadRequest / Overflow: every one is a typed
            // refusal, and each has a reject reason by construction.
            let reason = e.reject_reason().unwrap_or(mvcc::REJECT_BAD_REQUEST);
            send_reject(s, sys, req.corr_id, reason)
        }
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<AllocState>() as u32
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
    if state_size < core::mem::size_of::<AllocState>() {
        return -1;
    }
    unsafe {
        let s = &mut *state.cast::<AllocState>();
        let sys = &*syscalls.cast::<SyscallTable>();
        s.init(sys);
        s.request_in = in_chan;
        s.grant_out = out_chan;
        s.committed_in = dev_channel_port(sys, 0, 1);
        s.propose_out = dev_channel_port(sys, 1, 1);
        s.metrics_out = dev_channel_port(sys, 1, 2);
        parse_tlv(s, params, params_len);
        // The reserve carries the allocator identity into every record
        // it produces, so it is (re)seeded after params are applied.
        // Phase stays `Unestablished`: params tell us who we are, not
        // what was already issued.
        s.reserve.init(s.allocator_id);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<AllocState>() };
    let sys = s.syscalls;
    if sys.is_null() {
        return 0;
    }
    unsafe {
        let sys = &*sys;
        // One bounded action per step, in priority order:
        //   1. drain the oldest held request if the committed reserve
        //      now covers it (held requests are already-accepted work);
        //   2. otherwise fold in one committed record — this is what
        //      makes held work drainable, so it must never be starved
        //      by step 1, hence "acted" rather than an early return;
        //   3. otherwise accept one new request.
        let mut acted = drain_one_held(s, sys);
        if !acted {
            acted = pump_committed(s, sys);
        }
        if !acted {
            pump_request(s, sys);
        }

        // The unsolicited worker-lease cadence runs
        // alongside the request/reply pump; it is time-driven, not
        // demand-driven, so it must tick every step regardless of
        // whether the pump acted.
        auto_grant_tick(s, sys);

        // Phase-14: emit module-scope counters on the metrics port at a
        // coarse cadence (no-op until wired). ids follow manifest order;
        // `epoch` and `high_water` are levels, not rates.
        s.step_ctr = s.step_ctr.wrapping_add(1);
        if s.step_ctr.is_multiple_of(EMIT_EVERY) {
            telemetry::emit_counters(
                sys,
                s.metrics_out,
                &[
                    s.m_grants,
                    s.m_proposals,
                    s.m_holds,
                    s.m_rejects,
                    s.reserve.epoch as u64,
                    s.reserve.committed_high_water,
                ],
            );
        }
    }
    0
}

/// Try to serve the oldest held request. Returns true when an envelope
/// was written; false (leaving the request queued) when the reserve
/// still cannot cover it and an advance is already in flight.
unsafe fn drain_one_held(s: &mut AllocState, sys: &SyscallTable) -> bool {
    let Some(req) = s.hold_peek() else {
        return false;
    };
    if s.reserve.phase == ReservePhase::Unestablished {
        return false;
    }
    // Peek at issuability before dequeuing: `serve` re-queues nothing.
    let free_enough = s.reserve.free() >= req.size as u64;
    if !free_enough {
        // Still waiting on durability. Propose if nothing is in flight;
        // otherwise take no action and let the committed pump run.
        if s.reserve.phase == ReservePhase::Ready {
            return propose_advance(s, sys, req.size as u64);
        }
        return false;
    }
    s.hold_pop();
    serve(s, sys, req, true)
}

/// Fold one committed record into the reserve. This is the only path
/// that advances the durable high water.
unsafe fn pump_committed(s: &mut AllocState, sys: &SyscallTable) -> bool {
    let mut scratch = [0u8; SCRATCH];
    let Some((mt, len)) = read_envelope(sys, s.committed_in, &mut scratch) else {
        return false;
    };
    if mt == MSG_WAL_REPLAY_COMPLETE {
        // Empty-log bootstrap. Safe ONLY because this port delivers the
        // replayed committed records before this signal — see
        // `ReserveState::establish_empty_log` and the manifest's wiring
        // note. No-op on an allocator the records already established.
        s.reserve.establish_empty_log();
        return true;
    }
    if mt != MSG_TS_LEASE || len != LEASE_WIRE_LEN {
        return true; // consumed, ignored — fail closed on anything else
    }
    let Some(rec) = TimestampLease::decode(&scratch[..LEASE_WIRE_LEN]) else {
        return true;
    };
    // A record that breaks §10 succession is DROPPED, never repaired and
    // never worked around from local state: the committed log is the
    // only authority for what was already issued.
    let _ = s.reserve.observe_committed(&rec);
    true
}

/// Accept one new lease request.
unsafe fn pump_request(s: &mut AllocState, sys: &SyscallTable) -> bool {
    let mut scratch = [0u8; SCRATCH];
    let Some((mt, len)) = read_envelope(sys, s.request_in, &mut scratch) else {
        return false;
    };
    if mt != MSG_TS_LEASE_REQUEST || len != LEASE_REQUEST_WIRE_LEN {
        return true;
    }
    let Some(req) = LeaseRequest::decode(&scratch[..LEASE_REQUEST_WIRE_LEN]) else {
        return true;
    };
    serve(s, sys, req, false);
    true
}
