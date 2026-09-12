//! lease_manager — durable lease state owner and SessionCtrlV1 session
//! worker.
//!
//! A lease is a session: `session_id = [anchor_id:8][lease_id:8]`. The
//! anchor ATTACHes it before the grant and DETACHes it on revoke or
//! disconnect, and it can be handed off between managers like any other
//! session (`session_worker::SessionWorker`). One channel pair carries
//! both SessionCtrlV1 frames (`0x70..=0x9F`) and the data-plane
//! `MSG_LEASE_CTRL` grant / revoke / keepalive / time_to_live
//! envelopes, each opening with the session header the anchor stamps.
//!
//! Fencing the keepalive stream by the session epoch is what lets it
//! survive a worker move. A LeaseKeepAlive interruption expires the
//! lease and a client loses its lock, so a keepalive at the current
//! epoch refreshes the deadline while one at a stale epoch is refused by
//! `admit` — a keepalive racing a handoff cannot refresh a lease the
//! session has already moved past. The deadline itself crosses in the
//! handoff blob, so the receiving manager expires the lease on the same
//! schedule.
//!
//! Ports:
//! - **`ctrl_in`** — from the etcd anchor; SessionCtrlV1 replies and
//!   `MSG_LEASE_STATE` go back on **`ctrl_out`** and **`responses`**.
//! - **`tick`** — the committed clock from `ttl_scheduler`.
//! - **`in_epoch_events`** — the substrate placement epoch, held, never
//!   stamped into a session.
//! - **`revoke`** — lease-attached-key revokes to `kv_state_worker`.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
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

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/ttl_scheduler.rs"]
mod ttl_scheduler;

#[path = "../../common/session_worker.rs"]
mod session_worker;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use session_worker::session_core::session_ctrl as sc;
use session_worker::session_core::{
    placement_event_epoch, session_app_id, worker_id, SessionId, CLASS_LEASE,
};
use session_worker::{SessionWorker, WorkerAction, WPHASE_DRAINING};
use ttl_scheduler::{ExpiryEntry, TtlQueue, EXPIRY_KIND_LEASE};
use wire::{
    LEASE_STATE_GONE, LEASE_STATE_OK, MSG_LEASE_CTRL, MSG_LEASE_REVOKE, MSG_LEASE_STATE,
    MSG_LEASE_TICK, MSG_PLACEMENT_EPOCH_EVENT,
};

const SCRATCH_BUF_SIZE: usize = 4096;
const MAX_LEASES: usize = 256;
const REVOKE_PER_TICK: usize = 32;

const LEASE_CTRL_GRANT: u8 = 0;
const LEASE_CTRL_REVOKE: u8 = 1;
const LEASE_CTRL_KEEPALIVE: u8 = 2;
const LEASE_CTRL_TIME_TO_LIVE: u8 = 3;

/// The application record a lease session carries: everything the TTL
/// machine needs, and the blob that crosses a handoff.
#[repr(C)]
#[derive(Clone, Copy)]
struct LeaseRecord {
    /// 0 = free slot.
    lease_id: u64,
    tenant_id: u32,
    ttl_ms: u32,
    granted_at_ms: u64,
    keepalive_deadline_ms: u64,
}

impl LeaseRecord {
    const fn free() -> Self {
        Self {
            lease_id: 0,
            tenant_id: 0,
            ttl_ms: 0,
            granted_at_ms: 0,
            keepalive_deadline_ms: 0,
        }
    }

    /// `[magic "LLR1":4][lease_id:8][tenant:4][ttl_ms:4][granted:8][deadline:8]`
    const BLOB_LEN: usize = 4 + 8 + 4 + 4 + 8 + 8;

    fn export(&self, out: &mut [u8; Self::BLOB_LEN]) {
        out[0..4].copy_from_slice(b"LLR1");
        out[4..12].copy_from_slice(&self.lease_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.tenant_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.ttl_ms.to_le_bytes());
        out[20..28].copy_from_slice(&self.granted_at_ms.to_le_bytes());
        out[28..36].copy_from_slice(&self.keepalive_deadline_ms.to_le_bytes());
    }

    fn import(src: &[u8]) -> Option<Self> {
        if src.len() < Self::BLOB_LEN || &src[0..4] != b"LLR1" {
            return None;
        }
        Some(Self {
            lease_id: u64::from_le_bytes(slice8(src, 4)),
            tenant_id: u32::from_le_bytes(slice4(src, 12)),
            ttl_ms: u32::from_le_bytes(slice4(src, 16)),
            granted_at_ms: u64::from_le_bytes(slice8(src, 20)),
            keepalive_deadline_ms: u64::from_le_bytes(slice8(src, 28)),
        })
    }
}

#[repr(C)]
struct LeaseManagerState {
    syscalls: *const SyscallTable,

    ctrl_in: i32,
    tick_in: i32,
    epoch_events_in: i32,
    ctrl_out: i32,
    responses_out: i32,
    revoke_out: i32,
    metrics_out: i32,

    worker_ordinal: u8,

    /// Monotonic deterministic time from the apply pipeline tick.
    now_ms: u64,

    m_grants: u64,
    m_revokes: u64,
    m_expiries: u64,
    step_ctr: u64,

    /// One lease record per session slot (index-aligned with `sw`).
    leases: [LeaseRecord; MAX_LEASES],
    expiry: TtlQueue<MAX_LEASES>,
    sw: SessionWorker<MAX_LEASES>,

    scratch: [u8; SCRATCH_BUF_SIZE],
}

define_params! {
    LeaseManagerState;

    1, worker_id, u8, 0
        => |s, d, len| { s.worker_ordinal = p_u8(d, len, 0, 0); };
}

impl LeaseManagerState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.ctrl_in = -1;
        self.tick_in = -1;
        self.epoch_events_in = -1;
        self.ctrl_out = -1;
        self.responses_out = -1;
        self.revoke_out = -1;
        self.metrics_out = -1;
        self.worker_ordinal = 0;
        self.now_ms = 0;
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
        self.sw.init(worker_id(CLASS_LEASE, 0));
    }

    fn find_lease(&self, lease_id: u64) -> Option<usize> {
        if lease_id == 0 {
            return None;
        }
        self.leases.iter().position(|r| r.lease_id == lease_id)
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

fn slice4(buf: &[u8], at: usize) -> [u8; 4] {
    let mut out = [0u8; 4];
    if at + 4 <= buf.len() {
        out.copy_from_slice(&buf[at..at + 4]);
    }
    out
}

fn slice8(buf: &[u8], at: usize) -> [u8; 8] {
    let mut out = [0u8; 8];
    if at + 8 <= buf.len() {
        out.copy_from_slice(&buf[at..at + 8]);
    }
    out
}

macro_rules! ctrl_sink {
    ($mgr:expr) => {{
        let sys = $mgr.syscalls;
        let chan = $mgr.ctrl_out;
        move |msg: u8, payload: &[u8]| -> bool {
            if sys.is_null() {
                return false;
            }
            unsafe { write_envelope(&*sys, chan, msg, payload) }
        }
    }};
}

// ── Session control ───────────────────────────────────────────────────

fn handle_session_frame(mgr: &mut LeaseManagerState, msg_type: u8, payload: &[u8]) {
    let mut sink = ctrl_sink!(mgr);
    let action = mgr.sw.handle_frame(msg_type, payload, &mut sink);
    match action {
        WorkerAction::None | WorkerAction::Attached(_) => {}
        WorkerAction::Detached(idx, _) | WorkerAction::ImportDiscarded(idx) => {
            drop_lease(mgr, idx);
        }
        WorkerAction::Drain(idx) => {
            // A lease record is settled the instant DRAIN lands
            // (its only outbound is the reply the anchor already has),
            // so export immediately.
            let mut blob = [0u8; LeaseRecord::BLOB_LEN];
            mgr.leases[idx].export(&mut blob);
            let mut sink = ctrl_sink!(mgr);
            if mgr.sw.declare_drained(idx, &mut sink) {
                let _ = mgr.sw.export(idx, &blob, &mut sink);
            }
        }
        WorkerAction::Imported(idx) => {
            match LeaseRecord::import(mgr.sw.import_blob()) {
                Some(rec) => {
                    mgr.leases[idx] = rec;
                    // Re-arm the deadline the blob carried.
                    let _ = mgr.expiry.remove(EXPIRY_KIND_LEASE, rec.lease_id);
                    let _ = mgr.expiry.insert(ExpiryEntry::new(
                        rec.keepalive_deadline_ms,
                        EXPIRY_KIND_LEASE,
                        rec.lease_id,
                    ));
                }
                None => mgr.sw.forget(idx),
            }
            mgr.sw.import_applied(idx);
        }
        WorkerAction::Resumed(_, _)
        | WorkerAction::Reinstated(_)
        | WorkerAction::EpochBumped(_, _) => {}
    }
}

fn drop_lease(mgr: &mut LeaseManagerState, idx: usize) {
    let lease_id = mgr.leases[idx].lease_id;
    if lease_id != 0 {
        let _ = mgr.expiry.remove(EXPIRY_KIND_LEASE, lease_id);
        mgr.leases[idx] = LeaseRecord::free();
    }
}

// ── Data plane ────────────────────────────────────────────────────────

fn handle_ctrl(mgr: &mut LeaseManagerState, payload: &[u8]) {
    // [session_id:16][epoch:4][ctrl:1][lease_id:8][ttl_ms:4][tenant:4]
    if payload.len() < sc::SESSION_HEADER + 1 + 8 + 4 + 4 {
        return;
    }
    let sid = session_worker::sid_of(payload);
    let epoch = sc::epoch(payload);
    let at = sc::SESSION_HEADER;
    let ctrl = payload[at];
    let lease_id = u64::from_le_bytes(slice8(payload, at + 1));
    let ttl_ms = u32::from_le_bytes(slice4(payload, at + 9));
    let tenant_id = u32::from_le_bytes(slice4(payload, at + 13));

    // A session-scoped command is admitted only at the session's
    // current epoch — this is the keepalive fence. A command with a
    // zero session id (a client-supplied lease the anchor never
    // sessionized) is handled lease-first without a session.
    let has_session = sid != [0u8; 16];
    let idx = if has_session {
        match mgr.sw.admit(&sid, epoch) {
            Some(i) => {
                mgr.sw.consumed(i);
                Some(i)
            }
            None => return, // stale / future / unknown — fenced
        }
    } else {
        None
    };

    match ctrl {
        LEASE_CTRL_GRANT => grant(mgr, idx, &sid, epoch, lease_id, ttl_ms, tenant_id),
        LEASE_CTRL_KEEPALIVE => keepalive(mgr, &sid, epoch, lease_id),
        LEASE_CTRL_REVOKE => revoke(mgr, &sid, epoch, lease_id),
        LEASE_CTRL_TIME_TO_LIVE => time_to_live(mgr, &sid, epoch, lease_id),
        _ => {}
    }
}

fn grant(
    mgr: &mut LeaseManagerState,
    idx: Option<usize>,
    sid: &SessionId,
    epoch: u32,
    lease_id: u64,
    ttl_ms: u32,
    tenant_id: u32,
) {
    let id = if lease_id == 0 {
        // Derive from the session identity so it round-trips.
        session_app_id(sid)
    } else {
        lease_id
    };
    if id == 0 || mgr.find_lease(id).is_some() {
        emit_state(mgr, sid, epoch, id, LEASE_STATE_GONE, 0, 0, 0);
        return;
    }
    // Where does the record live? On the session slot if attached; else
    // the first free lease slot with no session.
    let slot = match idx {
        Some(i) => i,
        None => match mgr.leases.iter().position(|r| r.lease_id == 0) {
            Some(i) => i,
            None => return,
        },
    };
    let now = mgr.now_ms;
    let deadline = now.saturating_add(u64::from(ttl_ms));
    mgr.leases[slot] = LeaseRecord {
        lease_id: id,
        tenant_id,
        ttl_ms,
        granted_at_ms: now,
        keepalive_deadline_ms: deadline,
    };
    let _ = mgr
        .expiry
        .insert(ExpiryEntry::new(deadline, EXPIRY_KIND_LEASE, id));
    mgr.m_grants += 1;
    emit_state(mgr, sid, epoch, id, LEASE_STATE_OK, ttl_ms, now, deadline);
}

fn keepalive(mgr: &mut LeaseManagerState, sid: &SessionId, epoch: u32, lease_id: u64) {
    // Reaching here means the session already passed `admit` at the
    // current epoch (or it is a sessionless client-supplied lease):
    // a stale-epoch keepalive never gets this far, so it never
    // refreshes a lease the session has moved past.
    let id = if lease_id != 0 {
        lease_id
    } else {
        session_app_id(sid)
    };
    let Some(slot) = mgr.find_lease(id) else {
        emit_state(mgr, sid, epoch, id, LEASE_STATE_GONE, 0, 0, 0);
        return;
    };
    let now = mgr.now_ms;
    let ttl = mgr.leases[slot].ttl_ms;
    let deadline = now.saturating_add(u64::from(ttl));
    mgr.leases[slot].keepalive_deadline_ms = deadline;
    let _ = mgr.expiry.remove(EXPIRY_KIND_LEASE, id);
    let _ = mgr
        .expiry
        .insert(ExpiryEntry::new(deadline, EXPIRY_KIND_LEASE, id));
    let granted = mgr.leases[slot].granted_at_ms;
    emit_state(mgr, sid, epoch, id, LEASE_STATE_OK, ttl, granted, deadline);
}

fn revoke(mgr: &mut LeaseManagerState, sid: &SessionId, epoch: u32, lease_id: u64) {
    let id = if lease_id != 0 {
        lease_id
    } else {
        session_app_id(sid)
    };
    if let Some(slot) = mgr.find_lease(id) {
        mgr.leases[slot] = LeaseRecord::free();
        let _ = mgr.expiry.remove(EXPIRY_KIND_LEASE, id);
        mgr.m_revokes += 1;
        let _ = emit_revoke(mgr, id);
    }
    emit_state(mgr, sid, epoch, id, LEASE_STATE_GONE, 0, 0, 0);
}

fn time_to_live(mgr: &mut LeaseManagerState, sid: &SessionId, epoch: u32, lease_id: u64) {
    let id = if lease_id != 0 {
        lease_id
    } else {
        session_app_id(sid)
    };
    match mgr.find_lease(id) {
        Some(slot) => {
            let l = mgr.leases[slot];
            emit_state(
                mgr,
                sid,
                epoch,
                id,
                LEASE_STATE_OK,
                l.ttl_ms,
                l.granted_at_ms,
                l.keepalive_deadline_ms,
            );
        }
        None => emit_state(mgr, sid, epoch, id, LEASE_STATE_GONE, 0, 0, 0),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one call per MSG_LEASE_STATE frame; the parameters are the frame's fields in wire order"
)]
fn emit_state(
    mgr: &mut LeaseManagerState,
    sid: &SessionId,
    epoch: u32,
    lease_id: u64,
    status: u8,
    ttl_ms: u32,
    granted_at_ms: u64,
    keepalive_deadline_ms: u64,
) {
    // [session_id:16][epoch:4][lease_id:8][status:1][ttl_ms:4]
    // [granted_at:8][keepalive_deadline:8] = 49 bytes
    let mut body = [0u8; sc::SESSION_HEADER + 8 + 1 + 4 + 8 + 8];
    sc::put_session_header(&mut body, sid, epoch);
    let mut p = sc::SESSION_HEADER;
    body[p..p + 8].copy_from_slice(&lease_id.to_le_bytes());
    p += 8;
    body[p] = status;
    p += 1;
    body[p..p + 4].copy_from_slice(&ttl_ms.to_le_bytes());
    p += 4;
    body[p..p + 8].copy_from_slice(&granted_at_ms.to_le_bytes());
    p += 8;
    body[p..p + 8].copy_from_slice(&keepalive_deadline_ms.to_le_bytes());
    unsafe {
        let sys = mgr.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, mgr.responses_out, MSG_LEASE_STATE, &body);
        }
    }
}

fn emit_revoke(mgr: &mut LeaseManagerState, lease_id: u64) -> bool {
    let mut body = [0u8; 12];
    body[0..8].copy_from_slice(&lease_id.to_le_bytes());
    unsafe {
        let sys = mgr.syscalls;
        if sys.is_null() {
            return false;
        }
        if mgr.revoke_out < 0 {
            return true;
        }
        write_envelope(&*sys, mgr.revoke_out, MSG_LEASE_REVOKE, &body)
    }
}

// ── Tick handler ──────────────────────────────────────────────────────

fn handle_tick(mgr: &mut LeaseManagerState, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let tick_ms = u64::from_le_bytes(slice8(payload, 0));
    if tick_ms > mgr.now_ms {
        mgr.now_ms = tick_ms;
    }
    let mut fired = 0usize;
    while fired < REVOKE_PER_TICK {
        let Some(entry) = mgr.expiry.peek() else {
            break;
        };
        if entry.deadline_ms > mgr.now_ms {
            break;
        }
        let id = entry.payload;
        let known = mgr.find_lease(id);
        if known.is_some() && !emit_revoke(mgr, id) {
            break;
        }
        if let Some(slot) = known {
            // The lease expired. If a session fronts it, the anchor
            // learns from the DETACH the module issues; if not, the
            // slot is just freed.
            let sid = *mgr.sw.slots[slot].session_id();
            if mgr.sw.slots[slot].in_use {
                let mut sink = ctrl_sink!(mgr);
                let _ = mgr.sw.declare_drained(slot, &mut sink); // no-op if not draining
                mgr.sw.forget(slot);
                let _ = sid;
            }
            mgr.leases[slot] = LeaseRecord::free();
            mgr.m_expiries += 1;
        }
        let _ = mgr.expiry.pop();
        fired += 1;
    }
}

fn handle_epoch_event(_mgr: &mut LeaseManagerState, msg_type: u8, payload: &[u8]) {
    let _ = placement_event_epoch(msg_type, payload);
    // Placement epoch is not held here; the anchor drives per-session
    // rebinds. Kept as a wired input for symmetry with watch_registry.
}

// ── Module ABI ────────────────────────────────────────────────────────

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
    params: *const u8,
    params_len: usize,
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
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(mgr, params, params_len) };
    }
    mgr.sw
        .init(worker_id(CLASS_LEASE, u16::from(mgr.worker_ordinal)));

    // inputs: ctrl_in[0], tick[1], in_epoch_events[2]
    // outputs: ctrl_out[0], responses[1], revoke[2], metrics[3]
    mgr.ctrl_in = in_chan;
    mgr.ctrl_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        mgr.tick_in = dev_channel_port(sys, 0, 1);
        mgr.epoch_events_in = dev_channel_port(sys, 0, 2);
        mgr.responses_out = dev_channel_port(sys, 1, 1);
        mgr.revoke_out = dev_channel_port(sys, 1, 2);
        mgr.metrics_out = dev_channel_port(sys, 1, 3);
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
            let mut tmp = [0u8; SCRATCH_BUF_SIZE];
            tmp[..len].copy_from_slice(&mgr.scratch[..len]);
            if (0x70..=0x9F).contains(&mt) {
                handle_session_frame(mgr, mt, &tmp[..len]);
            } else if mt == MSG_LEASE_CTRL {
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
                handle_epoch_event(mgr, mt, &tmp[..len]);
            }
        }

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
