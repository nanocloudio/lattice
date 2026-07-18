//! lease_manager — durable lease state owner.
//!
//! Phase 4 module. Maintains `LeaseID → LeaseState{ttl_ms,
//! granted_at_ms, keepalive_deadline_ms, attached_keys[], epoch}`.
//! Wire envelopes (recap from `modules/common/wire.rs`):
//!
//! - **`ctrl_in`** — `MSG_LEASE_CTRL`: anchor-issued grant/revoke/
//!   keepalive/time_to_live commands.
//!   Body: `[ctrl:u8][lease_id:u64 LE][ttl_ms:u32 LE][tenant_id:u32 LE]`.
//! - **`tick`** — `MSG_LEASE_TICK` from `ttl_scheduler`. Body:
//!   `[tick_ms:u64 LE]`. Drives deadline scanning.
//! - **`responses`** — `MSG_LEASE_STATE` back to the anchor.
//! - **`revoke`** — `MSG_LEASE_REVOKE` out to `kv_state_worker`.
//!
//! Phase 4 backs the lease table with the dual-target
//! `ttl_scheduler::TtlQueue` for deadline ordering. Attached-keys
//! aren't tracked in Phase 4 — they land when etcd Put-with-lease
//! is wired through (substrate gap for the `MSG_LEASE_REVOKE` body
//! shape).

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

#[path = "../../common/ttl_scheduler.rs"]
mod ttl_scheduler;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use ttl_scheduler::{ExpiryEntry, TtlQueue, EXPIRY_KIND_LEASE};
use wire::{
    MSG_LEASE_CTRL, MSG_LEASE_REVOKE, MSG_LEASE_STATE, MSG_LEASE_TICK, MSG_PLACEMENT_EPOCH_EVENT,
};

const SCRATCH_BUF_SIZE: usize = 4096;
const MAX_LEASES: usize = 256;

const LEASE_CTRL_GRANT: u8 = 0;
const LEASE_CTRL_REVOKE: u8 = 1;
const LEASE_CTRL_KEEPALIVE: u8 = 2;
const LEASE_CTRL_TIME_TO_LIVE: u8 = 3;

#[repr(C)]
#[derive(Clone, Copy)]
struct LeaseRecord {
    /// 0 = free slot.
    lease_id: u64,
    tenant_id: u32,
    ttl_ms: u32,
    granted_at_ms: u64,
    keepalive_deadline_ms: u64,
    /// Bumped on every keepalive (so stale ack frames at the
    /// anchor can be filtered by epoch).
    session_epoch: u32,
}

impl LeaseRecord {
    const fn free() -> Self {
        Self {
            lease_id: 0,
            tenant_id: 0,
            ttl_ms: 0,
            granted_at_ms: 0,
            keepalive_deadline_ms: 0,
            session_epoch: 0,
        }
    }
}

#[repr(C)]
struct LeaseManagerState {
    syscalls: *const SyscallTable,

    ctrl_in: i32,
    tick_in: i32,
    epoch_events_in: i32,
    responses_out: i32,
    revoke_out: i32,
    metrics_out: i32,

    /// Monotonic deterministic time from the apply pipeline tick.
    /// Updated on every `MSG_LEASE_TICK` envelope.
    now_ms: u64,
    /// Next lease id to issue.
    next_lease_id: u64,
    /// Cluster-wide placement epoch as last reported by substrate
    /// `control_plane.epoch_events`. Stamped into newly-granted
    /// leases and bumped on every active record when an epoch event
    /// lands. Starts at 1 so pre-substrate single-node graphs still
    /// produce valid (non-zero) session_epoch values.
    cluster_epoch: u32,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=grants, 1=revokes, 2=expiries).
    m_grants: u64,
    m_revokes: u64,
    m_expiries: u64,
    step_ctr: u64,

    leases: [LeaseRecord; MAX_LEASES],
    expiry: TtlQueue<MAX_LEASES>,

    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl LeaseManagerState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.ctrl_in = -1;
        self.tick_in = -1;
        self.epoch_events_in = -1;
        self.responses_out = -1;
        self.revoke_out = -1;
        self.metrics_out = -1;
        self.now_ms = 0;
        self.next_lease_id = 1;
        self.cluster_epoch = 1;
        self.m_grants = 0;
        self.m_revokes = 0;
        self.m_expiries = 0;
        self.step_ctr = 0;
        let mut i = 0;
        while i < MAX_LEASES {
            self.leases[i] = LeaseRecord::free();
            i += 1;
        }
        self.expiry.init();
    }

    fn alloc(&mut self) -> Option<usize> {
        self.leases.iter().position(|r| r.lease_id == 0)
    }

    fn find(&self, lease_id: u64) -> Option<usize> {
        if lease_id == 0 {
            return None;
        }
        self.leases.iter().position(|r| r.lease_id == lease_id)
    }

    fn issue_id(&mut self) -> u64 {
        let id = self.next_lease_id;
        self.next_lease_id = self.next_lease_id.wrapping_add(1);
        if self.next_lease_id == 0 {
            self.next_lease_id = 1;
        }
        id
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

// ── Ctrl handler ──────────────────────────────────────────────────────

fn handle_ctrl(mgr: &mut LeaseManagerState, payload: &[u8]) {
    // [ctrl:1][lease_id:8][ttl_ms:4][tenant_id:4]
    if payload.len() < 17 {
        return;
    }
    let ctrl = payload[0];
    let lease_id = u64::from_le_bytes([
        payload[1], payload[2], payload[3], payload[4], payload[5], payload[6], payload[7],
        payload[8],
    ]);
    let ttl_ms = u32::from_le_bytes([payload[9], payload[10], payload[11], payload[12]]);
    let tenant_id = u32::from_le_bytes([payload[13], payload[14], payload[15], payload[16]]);

    match ctrl {
        LEASE_CTRL_GRANT => {
            let id = if lease_id == 0 {
                mgr.issue_id()
            } else {
                lease_id
            };
            // Reject if id already taken.
            if mgr.find(id).is_some() {
                emit_state(mgr, id, 0, 0, 0, 0);
                return;
            }
            let Some(idx) = mgr.alloc() else {
                return;
            };
            let now = mgr.now_ms;
            let deadline = now.saturating_add(u64::from(ttl_ms));
            let epoch = mgr.cluster_epoch;
            mgr.leases[idx] = LeaseRecord {
                lease_id: id,
                tenant_id,
                ttl_ms,
                granted_at_ms: now,
                keepalive_deadline_ms: deadline,
                session_epoch: epoch,
            };
            let _ = mgr
                .expiry
                .insert(ExpiryEntry::new(deadline, EXPIRY_KIND_LEASE, id));
            mgr.m_grants += 1;
            emit_state(mgr, id, epoch, ttl_ms, now, deadline);
        }
        LEASE_CTRL_REVOKE => {
            if let Some(idx) = mgr.find(lease_id) {
                mgr.leases[idx] = LeaseRecord::free();
                let _ = mgr.expiry.remove(EXPIRY_KIND_LEASE, lease_id);
                mgr.m_revokes += 1;
                emit_revoke(mgr, lease_id);
                emit_state(mgr, lease_id, 0, 0, 0, 0);
            }
        }
        LEASE_CTRL_KEEPALIVE => {
            if let Some(idx) = mgr.find(lease_id) {
                let now = mgr.now_ms;
                let ttl = mgr.leases[idx].ttl_ms;
                let deadline = now.saturating_add(u64::from(ttl));
                mgr.leases[idx].keepalive_deadline_ms = deadline;
                // session_epoch is now substrate-driven (see
                // `handle_epoch_event`). Keepalive just refreshes the
                // expiry deadline; the epoch only moves when a
                // placement-router event lands.
                let _ = mgr.expiry.remove(EXPIRY_KIND_LEASE, lease_id);
                let _ = mgr
                    .expiry
                    .insert(ExpiryEntry::new(deadline, EXPIRY_KIND_LEASE, lease_id));
                let epoch = mgr.leases[idx].session_epoch;
                let granted = mgr.leases[idx].granted_at_ms;
                emit_state_epoch(mgr, lease_id, epoch, ttl, granted, deadline);
            }
        }
        LEASE_CTRL_TIME_TO_LIVE => {
            if let Some(idx) = mgr.find(lease_id) {
                let l = mgr.leases[idx];
                emit_state_epoch(
                    mgr,
                    lease_id,
                    l.session_epoch,
                    l.ttl_ms,
                    l.granted_at_ms,
                    l.keepalive_deadline_ms,
                );
            }
        }
        _ => {}
    }
}

fn emit_state(
    mgr: &mut LeaseManagerState,
    lease_id: u64,
    epoch: u32,
    ttl_ms: u32,
    granted_at_ms: u64,
    keepalive_deadline_ms: u64,
) {
    emit_state_epoch(
        mgr,
        lease_id,
        epoch,
        ttl_ms,
        granted_at_ms,
        keepalive_deadline_ms,
    );
}

fn emit_state_epoch(
    mgr: &mut LeaseManagerState,
    lease_id: u64,
    epoch: u32,
    ttl_ms: u32,
    granted_at_ms: u64,
    keepalive_deadline_ms: u64,
) {
    // [lease_id:8][session_epoch:4][ttl_ms:4][granted_at:8]
    // [keepalive_deadline:8] = 32 bytes
    let mut body = [0u8; 32];
    body[0..8].copy_from_slice(&lease_id.to_le_bytes());
    body[8..12].copy_from_slice(&epoch.to_le_bytes());
    body[12..16].copy_from_slice(&ttl_ms.to_le_bytes());
    body[16..24].copy_from_slice(&granted_at_ms.to_le_bytes());
    body[24..32].copy_from_slice(&keepalive_deadline_ms.to_le_bytes());
    unsafe {
        let sys = mgr.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, mgr.responses_out, MSG_LEASE_STATE, &body);
        }
    }
}

fn emit_revoke(mgr: &mut LeaseManagerState, lease_id: u64) {
    // [lease_id:8][kpg_id:2][key_len:2][key…]
    // Attached-keys list is Phase 5 scope; Phase 4 emits a
    // key-less revoke envelope (worker treats it as "evict every
    // record tagged with this lease_id" when key_len == 0).
    let mut body = [0u8; 12];
    body[0..8].copy_from_slice(&lease_id.to_le_bytes());
    body[8] = 0;
    body[9] = 0;
    body[10] = 0;
    body[11] = 0;
    unsafe {
        let sys = mgr.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, mgr.revoke_out, MSG_LEASE_REVOKE, &body);
        }
    }
}

// ── Tick handler ──────────────────────────────────────────────────────

fn handle_tick(mgr: &mut LeaseManagerState, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let tick_ms = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    if tick_ms > mgr.now_ms {
        mgr.now_ms = tick_ms;
    }
    // Drain due leases.
    let mut to_revoke: [u64; 32] = [0; 32];
    let mut count = 0usize;
    mgr.expiry.drain_due(mgr.now_ms, |entry| {
        if count < to_revoke.len() {
            to_revoke[count] = entry.payload;
            count += 1;
        }
    });
    let mut i = 0;
    while i < count {
        let id = to_revoke[i];
        if let Some(idx) = mgr.find(id) {
            mgr.leases[idx] = LeaseRecord::free();
            mgr.m_expiries += 1;
            emit_revoke(mgr, id);
        }
        i += 1;
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<LeaseManagerState>() as u32
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
    if state_size < core::mem::size_of::<LeaseManagerState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let mgr = unsafe { &mut *state.cast::<LeaseManagerState>() };
    mgr.init(sys_ptr);

    // inputs: ctrl_in[0], tick[1], in_epoch_events[2]
    // outputs: responses[0], revoke[1], metrics[2]
    mgr.ctrl_in = in_chan;
    mgr.responses_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        mgr.tick_in = dev_channel_port(sys, 0, 1);
        mgr.epoch_events_in = dev_channel_port(sys, 0, 2);
        mgr.revoke_out = dev_channel_port(sys, 1, 1);
        mgr.metrics_out = dev_channel_port(sys, 1, 2);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let mgr = unsafe { &mut *state.cast::<LeaseManagerState>() };
    unsafe {
        let sys_ptr = mgr.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;
        if let Some((mt, len)) = read_envelope(sys, mgr.ctrl_in, &mut mgr.scratch) {
            if mt == MSG_LEASE_CTRL {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&mgr.scratch[..len]);
                handle_ctrl(mgr, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, mgr.tick_in, &mut mgr.scratch) {
            if mt == MSG_LEASE_TICK {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&mgr.scratch[..len]);
                handle_tick(mgr, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, mgr.epoch_events_in, &mut mgr.scratch) {
            if mt == MSG_PLACEMENT_EPOCH_EVENT {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&mgr.scratch[..len]);
                handle_epoch_event(mgr, &tmp[..len]);
            }
        }

        // Phase-14: emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        mgr.step_ctr = mgr.step_ctr.wrapping_add(1);
        if mgr.step_ctr.is_multiple_of(5000) && !mgr.syscalls.is_null() {
            telemetry::emit_counters(
                &*mgr.syscalls,
                mgr.metrics_out,
                &[mgr.m_grants, mgr.m_revokes, mgr.m_expiries],
            );
        }
    }
    0
}

/// Substrate-driven epoch advancement. Wire shape:
/// `[prev_epoch:u32 LE][new_epoch:u32 LE]`. Monotonic — out-of-order
/// or duplicate events are no-ops.
///
/// On a real advance, every active lease's `session_epoch` is set to
/// `new_epoch` and a fresh `MSG_LEASE_STATE` is emitted for each so
/// the anchor (and any client tracking the lease) sees the new epoch.
fn handle_epoch_event(mgr: &mut LeaseManagerState, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let _prev = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let new_epoch = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
    if new_epoch <= mgr.cluster_epoch {
        return;
    }
    mgr.cluster_epoch = new_epoch;
    let mut i = 0;
    while i < MAX_LEASES {
        if mgr.leases[i].lease_id != 0 {
            mgr.leases[i].session_epoch = new_epoch;
            let l = mgr.leases[i];
            emit_state_epoch(
                mgr,
                l.lease_id,
                l.session_epoch,
                l.ttl_ms,
                l.granted_at_ms,
                l.keepalive_deadline_ms,
            );
        }
        i += 1;
    }
}
