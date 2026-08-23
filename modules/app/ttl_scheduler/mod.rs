//! ttl_scheduler — deterministic expiry service.
//!
//! Phase 4 module. Consumes deterministic-time tick envelopes
//! from the substrate (or, in single-node mode, from a graph-
//! local emitter wired to `tick_in`), maintains a deadline queue
//! of lease + KV-record expiries via the dual-target
//! `ttl_scheduler::TtlQueue` facade, and emits expiration events
//! back into the KV path / lease manager.
//!
//! Two-way contract:
//!
//! - `tick_in` carries `MSG_LEASE_TICK { tick_ms: u64 }`. The
//!   module updates its `now_ms` and drains any expired entries.
//! - `tick_out` re-emits the same tick to `lease_manager` so the
//!   lease module can advance its own deadline scan.
//! - `lease_deadlines` carries `MSG_LEASE_CTRL` envelopes —
//!   lease_manager forwards its grant/keepalive deadlines here so
//!   the central queue knows about them. (Lease_manager keeps its
//!   own per-record bookkeeping for the wire response; ttl_scheduler
//!   keeps the centralized priority queue.)
//! - `kv_expiry` carries KV-record expiry registrations from
//!   `kv_state_worker` (SET with EX/PX): body is
//!   `[deadline_ms:u64 LE][key_hash:u64 LE]`.
//! - `expire_out` emits expired-record events back into the KV
//!   path. Body: `[kind:u8][payload:u64 LE]` where kind matches
//!   `EXPIRY_KIND_*`.

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

#[path = "../../common/ttl_scheduler.rs"]
mod ttl_scheduler;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use ttl_scheduler::{ExpiryEntry, TtlQueue, EXPIRY_KIND_KV, EXPIRY_KIND_LEASE};
use wire::{MSG_LEASE_CTRL, MSG_LEASE_TICK, MSG_TTL_CLOCK_RESUME, MSG_TTL_REGISTER};

const SCRATCH_BUF_SIZE: usize = 4096;
const QUEUE_CAP: usize = 1024;

/// Expiry events emitted per committed tick. A bound on WORK, not on
/// how many entries may expire: whatever is left over stays in the
/// queue at its own deadline and fires on the next tick.
const EXPIRE_PER_TICK: usize = 32;

/// Default milliseconds between proposed ticks. The clock's resolution
/// and the log's tick rate are the same number: shorter means finer
/// expiry granularity and more committed entries on an idle cluster.
const DEFAULT_TICK_INTERVAL_MS: u32 = 250;

/// Wire constant for `expire_out` body shape — re-use the
/// `MSG_RETENTION_FLOOR`-adjacent block in `wire.rs` (0xE0..). We
/// re-purpose 0xE2 for now since `MSG_COMPACTION_FLOOR` is 0xE1
/// and we don't want to collide.
const MSG_TTL_EXPIRED: u8 = 0xE2;

#[repr(C)]
struct TtlState {
    syscalls: *const SyscallTable,

    tick_in: i32,
    lease_deadlines_in: i32,
    kv_expiry_in: i32,
    expire_out: i32,
    tick_out: i32,
    metrics_out: i32,
    propose_out: i32,

    /// Milliseconds between tick proposals (param `tick_interval_ms`).
    tick_interval_ms: u32,
    /// `1` = wrap a proposal for `gateway.client_requests`; `0` = emit
    /// the bare `MSG_LEASE_TICK` envelope (param `propose_wrap`).
    propose_wrap: u32,
    /// Local reading at which the last tick was proposed. The device
    /// timer is read here and nowhere else in the TTL path, and only to
    /// measure an INTERVAL — never to name a point in time. What the
    /// module proposes is the committed clock plus the interval that
    /// has passed since it last proposed.
    ///
    /// Naming a point in time would not survive: the device millisecond
    /// counter runs from process start, so a restarted node's readings
    /// are far below the clock the log already established, and every
    /// one of them would be discarded as backwards, freezing expiry for
    /// as long as the previous incarnation had been up.
    last_propose_ms: u64,
    /// Highest value this module has proposed. Proposals build on this
    /// rather than on `now_ms` so that a slow commit path makes the
    /// clock late, not slow — the latch takes the maximum, so the
    /// cluster catches up rather than losing the interval.
    proposed_ms: u64,

    /// Deterministic time advanced by the most recent tick.
    now_ms: u64,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=scheduled, 1=expired).
    m_scheduled: u64,
    m_expired: u64,
    step_ctr: u64,

    queue: TtlQueue<QUEUE_CAP>,
    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl TtlState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.tick_in = -1;
        self.lease_deadlines_in = -1;
        self.kv_expiry_in = -1;
        self.expire_out = -1;
        self.tick_out = -1;
        self.metrics_out = -1;
        self.propose_out = -1;
        self.last_propose_ms = 0;
        self.proposed_ms = 0;
        self.now_ms = 0;
        self.m_scheduled = 0;
        self.m_expired = 0;
        self.step_ctr = 0;
        self.queue.init();
    }
}

define_params! {
    TtlState;

    // Milliseconds between proposed ticks. This is the clock's
    // resolution: a TTL cannot expire more precisely than one interval,
    // and every interval costs one committed entry even on an idle
    // cluster.
    1, tick_interval_ms, u32, DEFAULT_TICK_INTERVAL_MS
        => |s, d, len| { s.tick_interval_ms = p_u32(d, len, 0, DEFAULT_TICK_INTERVAL_MS); };

    // 1 = wrap the tick for `gateway.client_requests`, so it becomes an
    // entry every replica commits at the same log position. 0 = emit
    // the bare envelope, for a graph with no replication, where the
    // command channel itself is the order everything agrees on.
    2, propose_wrap, u32, 0
        => |s, d, len| { s.propose_wrap = p_u32(d, len, 0, 0); };
}

/// Propose the next tick once `tick_interval_ms` of local time has
/// passed since the last one.
///
/// The proposal is the cluster's current time plus the interval that
/// elapsed locally. Two consequences are the point of the design:
///
/// - Only an INTERVAL is taken from the device timer, so a node whose
///   timer starts at a different origin — every restarted node — still
///   proposes values that continue the clock the log established, and a
///   node whose wall clock is wrong cannot drag expiry anywhere.
/// - Cluster time advances only while some node is proposing and its
///   proposals are committing. A cluster that is down does not age its
///   TTLs; when it returns, expiry resumes from where the log left off.
///   Keys therefore outlive an outage rather than expiring unseen
///   during one, which is the safe direction for a store that answers
///   nothing while it is down.
///
/// A proposal is only a proposal. It becomes the time when it comes
/// back committed on `tick_in`, so a node that is not the leader
/// contributes nothing and simply follows.
unsafe fn maybe_propose_tick(sched: &mut TtlState) {
    if sched.propose_out < 0 || sched.syscalls.is_null() {
        return;
    }
    let sys = &*sched.syscalls;
    let now = dev_millis(sys);
    let interval = u64::from(sched.tick_interval_ms.max(1));
    let elapsed = if sched.last_propose_ms == 0 {
        interval
    } else {
        now.wrapping_sub(sched.last_propose_ms)
    };
    if elapsed < interval {
        return;
    }
    let base = if sched.proposed_ms > sched.now_ms {
        sched.proposed_ms
    } else {
        sched.now_ms
    };
    let value = base.saturating_add(elapsed);
    let body = value.to_le_bytes();
    let ok = if sched.propose_wrap == 0 {
        write_envelope(sys, sched.propose_out, MSG_LEASE_TICK, &body)
    } else {
        // `MSG_CLIENT_PROPOSAL [conn_id=0][LATTICE_RECORD_TAG]
        // [MSG_LEASE_TICK][tick_ms:u64]` — the same record envelope
        // `timestamp_allocator` uses, so the committed entry demuxes at
        // `lattice_apply_bridge` with no new entry kind.
        let payload_len = 3 + body.len();
        let total = 3 + payload_len;
        let mut buf = [0u8; 32];
        buf[0] = wire::MSG_CLIENT_PROPOSAL;
        buf[1] = (payload_len & 0xFF) as u8;
        buf[2] = ((payload_len >> 8) & 0xFF) as u8;
        buf[3] = 0;
        buf[4] = wire::LATTICE_RECORD_TAG;
        buf[5] = MSG_LEASE_TICK;
        buf[6..6 + body.len()].copy_from_slice(&body);
        (sys.channel_write)(sched.propose_out, buf.as_mut_ptr(), total) == total as i32
    };
    if ok {
        sched.last_propose_ms = now;
        sched.proposed_ms = value;
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

unsafe fn write_envelope(sys: &SyscallTable, chan: i32, msg_type: u8, payload: &[u8]) -> bool {
    if chan < 0 || payload.len() > u16::MAX as usize {
        return false;
    }
    let mut buf = [0u8; SCRATCH_BUF_SIZE + 3];
    let total = 3 + payload.len();
    if total > buf.len() {
        return false;
    }
    buf[0] = msg_type;
    buf[1] = (payload.len() & 0xFF) as u8;
    buf[2] = ((payload.len() >> 8) & 0xFF) as u8;
    buf[3..total].copy_from_slice(payload);
    (sys.channel_write)(chan, buf.as_mut_ptr(), total) == total as i32
}

fn handle_tick(sched: &mut TtlState, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let tick_ms = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    if tick_ms > sched.now_ms {
        sched.now_ms = tick_ms;
    }
    // Forward the tick to lease_manager so its own scan runs at
    // the same `now_ms`. The value is a latch on both sides, so a tick
    // the channel refuses is superseded by the next one rather than
    // lost: what would be lost is an EVENT, and this is a level.
    unsafe {
        let sys = sched.syscalls;
        if !sys.is_null() {
            let mut body = [0u8; 8];
            body.copy_from_slice(&sched.now_ms.to_le_bytes());
            let _ = write_envelope(&*sys, sched.tick_out, MSG_LEASE_TICK, &body);
        }
    }
    // Fire expired entries, popping each only once `expire_out` has
    // taken it. An expiry IS an event — nothing re-derives it — so an
    // entry beyond this step's budget, or one whose write is refused,
    // stays queued at its deadline and fires on a later tick instead of
    // vanishing.
    let mut fired = 0usize;
    let now = sched.now_ms;
    let sys = sched.syscalls;
    let expire_out = sched.expire_out;
    fired += sched
        .queue
        .drain_due_while(now, EXPIRE_PER_TICK, |entry| unsafe {
            if expire_out < 0 {
                // No consumer is wired: there is nobody to retain it for,
                // and holding it would fill the queue against a channel
                // that will never drain.
                return true;
            }
            if sys.is_null() {
                return false;
            }
            emit_expired(&*sys, expire_out, entry)
        });
    sched.m_expired += fired as u64;
}

/// Write one expiry event: `[kind:1][payload:8]`. Returns whether the
/// channel took it — the caller leaves the entry queued if not.
unsafe fn emit_expired(sys: &SyscallTable, expire_out: i32, entry: &ExpiryEntry) -> bool {
    let mut body = [0u8; 9];
    body[0] = entry.kind;
    body[1..9].copy_from_slice(&entry.payload.to_le_bytes());
    write_envelope(sys, expire_out, MSG_TTL_EXPIRED, &body)
}

fn handle_lease_deadline(sched: &mut TtlState, payload: &[u8]) {
    // Lease manager forwards its grant/keepalive: same body as
    // `MSG_LEASE_CTRL`. We register the deadline in our central
    // queue so we can drive the scan from one place. For grant /
    // keepalive (`ctrl 0 / 2`) insert; for revoke (`ctrl 1`)
    // remove.
    if payload.len() < 17 {
        return;
    }
    let ctrl = payload[0];
    let lease_id = u64::from_le_bytes([
        payload[1], payload[2], payload[3], payload[4], payload[5], payload[6], payload[7],
        payload[8],
    ]);
    let ttl_ms = u32::from_le_bytes([payload[9], payload[10], payload[11], payload[12]]);
    match ctrl {
        0 | 2 => {
            // Grant or keepalive: (re)insert with `now + ttl`.
            let _ = sched.queue.remove(EXPIRY_KIND_LEASE, lease_id);
            let deadline = sched.now_ms.saturating_add(u64::from(ttl_ms));
            let _ = sched
                .queue
                .insert(ExpiryEntry::new(deadline, EXPIRY_KIND_LEASE, lease_id));
            sched.m_scheduled += 1;
        }
        1 => {
            let _ = sched.queue.remove(EXPIRY_KIND_LEASE, lease_id);
        }
        _ => {}
    }
}

/// Adopt a clock frontier a restored `kv_state_worker` restored from a
/// snapshot (`MSG_TTL_CLOCK_RESUME`, `[frontier_ms:u64]`).
///
/// The scheduler's `now_ms` and its queue live only in this arena, so a
/// restart re-enters at zero while the log's clock is wherever the
/// snapshot left it. Proposing from zero would put every proposal below
/// the committed frontier, where the worker's monotone latch discards
/// it, and cluster time would stand still for as long as the previous
/// incarnation had been up.
///
/// Latching it here is safe precisely because it changes only what this
/// module PROPOSES: a proposal is time only once it comes back
/// committed on `tick_in`, which is the same test every other tick
/// passes. `now_ms` moves too, so a deadline registered right behind
/// this message is measured against the restored frontier and not
/// against zero.
fn handle_clock_resume(sched: &mut TtlState, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let frontier = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    if frontier > sched.now_ms {
        sched.now_ms = frontier;
    }
    if frontier > sched.proposed_ms {
        sched.proposed_ms = frontier;
    }
}

fn handle_kv_expiry(sched: &mut TtlState, payload: &[u8]) {
    // [deadline_ms:8][key_hash:8]
    if payload.len() < 16 {
        return;
    }
    let deadline = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let key_hash = u64::from_le_bytes([
        payload[8],
        payload[9],
        payload[10],
        payload[11],
        payload[12],
        payload[13],
        payload[14],
        payload[15],
    ]);
    if deadline == 0 {
        // 0 means "cancel any pending expiry for this key".
        let _ = sched.queue.remove(EXPIRY_KIND_KV, key_hash);
    } else {
        let _ = sched.queue.remove(EXPIRY_KIND_KV, key_hash);
        let _ = sched
            .queue
            .insert(ExpiryEntry::new(deadline, EXPIRY_KIND_KV, key_hash));
        sched.m_scheduled += 1;
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<TtlState>() as u32
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
    if state_size < core::mem::size_of::<TtlState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let sched = unsafe { &mut *state.cast::<TtlState>() };
    sched.init(sys_ptr);

    // inputs: tick_in[0], lease_deadlines[1], kv_expiry[2]
    // outputs: expire_out[0], tick_out[1], metrics[2]
    sched.tick_in = in_chan;
    sched.expire_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        sched.lease_deadlines_in = dev_channel_port(sys, 0, 1);
        sched.kv_expiry_in = dev_channel_port(sys, 0, 2);
        sched.tick_out = dev_channel_port(sys, 1, 1);
        sched.metrics_out = dev_channel_port(sys, 1, 2);
        sched.propose_out = dev_channel_port(sys, 1, 3);
    }
    // Guarded exactly as `kv_state_worker` guards its own: a graph that
    // declares no params passes a null pointer, and the TLV walk must
    // not be reached with one.
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(sched, params, params_len) };
    } else {
        unsafe { set_defaults(sched) };
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let sched = unsafe { &mut *state.cast::<TtlState>() };
    unsafe {
        let sys_ptr = sched.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;
        if let Some((mt, len)) = read_envelope(sys, sched.tick_in, &mut sched.scratch) {
            if mt == MSG_LEASE_TICK {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&sched.scratch[..len]);
                handle_tick(sched, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, sched.lease_deadlines_in, &mut sched.scratch) {
            if mt == MSG_LEASE_CTRL {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&sched.scratch[..len]);
                handle_lease_deadline(sched, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, sched.kv_expiry_in, &mut sched.scratch) {
            let mut tmp = [0u8; SCRATCH_BUF_SIZE];
            tmp[..len].copy_from_slice(&sched.scratch[..len]);
            if mt == MSG_TTL_REGISTER {
                handle_kv_expiry(sched, &tmp[..len]);
            } else if mt == MSG_TTL_CLOCK_RESUME {
                handle_clock_resume(sched, &tmp[..len]);
            }
        }
        maybe_propose_tick(sched);

        // Phase-14: emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        sched.step_ctr = sched.step_ctr.wrapping_add(1);
        if sched.step_ctr.is_multiple_of(5000) && !sched.syscalls.is_null() {
            telemetry::emit_counters(
                &*sched.syscalls,
                sched.metrics_out,
                &[sched.m_scheduled, sched.m_expired],
            );
        }
    }
    0
}
