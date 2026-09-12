//! watch_registry — durable watch state owner and SessionCtrlV1
//! session worker.
//!
//! Owns the `WatchID → WatchRecord` table via the `watch_hub` facade
//! and answers Fluxor's session control plane for every watch it
//! fronts (`session_worker::SessionWorker`). One channel pair carries
//! both: SessionCtrlV1 frames (`0x70..=0x9F`) for attach / detach /
//! drain / export / resume, and the data-plane `MSG_WATCH_CTRL`
//! envelopes that define a watch or ping its progress, each opening
//! with the session header the anchor stamps.
//!
//! - **`ctrl_in`** — from the etcd anchor. Replies (`MSG_SC_*`) go
//!   back on **`ctrl_out`**.
//! - **`mutations`** — `MSG_WATCH_EVENT` from `kv_state_worker`; every
//!   in-service watch it matches gets a personalised copy on
//!   **`events_out`**.
//! - **`progress`** — `MSG_WATCH_PROGRESS` from `watch_fanout`: one
//!   ack per framed event advances the watch's `last_sent_revision`
//!   and its delivery cursor in the same step; a `COMPACTED` ack ends
//!   a watch whose replay window fell behind the compaction floor.
//! - **`in_epoch_events`** — the substrate's placement epoch. Held,
//!   never stamped into a session.
//!
//! Emits **`retention`** floors to `compaction_coordinator` and
//! **`replay_plan`** to the fanout whenever a session (re)enters
//! service here — after an import commits, or after a refused handoff
//! reinstates the exporter — so the window the session missed while
//! it was out of service is backfilled from history.
//!
//! ## Handoff
//!
//! `CMD_SC_DRAIN` takes a watch out of service; once every event it
//! emitted has been acked (`WatchRecord::outbound_settled`) the
//! registry declares `DRAINED` and exports the record with its
//! cursors. The importing registry holds the record dormant until
//! `CMD_SC_RESUME` commits it at the new epoch.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::manual_memcpy,
    clippy::needless_range_loop,
    clippy::duplicate_mod,
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    reason = "hand-written index loops build wire envelopes byte-by-byte throughout these modules; raw-pointer ABI entry points are the contract; the PIC build #[path]-remounts shared code"
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

#[path = "../../common/watch_hub.rs"]
mod watch_hub;

#[path = "../../common/session_worker.rs"]
mod session_worker;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use session_worker::session_core::session_ctrl as sc;
use session_worker::session_core::{placement_event_epoch, worker_id, SessionId, CLASS_WATCH};
use session_worker::{SessionWorker, WorkerAction, WPHASE_DRAINING};
use watch_hub::{
    match_event, MatchedWatch, WatchHub, WatchRecord, MAX_WATCHES, WATCH_EVENT_DELETE,
    WATCH_EVENT_PUT, WATCH_EXPORT_MAX, WATCH_PHASE_ACTIVE, WATCH_PHASE_DRAINED,
    WATCH_PHASE_DRAINING, WATCH_PHASE_IMPORTED,
};
use wire::{
    MSG_PLACEMENT_EPOCH_EVENT, MSG_RETENTION_FLOOR, MSG_WATCH_CTRL, MSG_WATCH_EVENT,
    MSG_WATCH_PROGRESS, MSG_WATCH_REPLAY_PLAN, WATCH_CTRL_DEFINE, WATCH_CTRL_PROGRESS,
    WATCH_PROGRESS_COMPACTED, WATCH_PROGRESS_DELIVERED,
};

const SCRATCH_BUF_SIZE: usize = 4096;

#[repr(C)]
struct RegistryState {
    syscalls: *const SyscallTable,

    // Inputs (manifest order: ctrl_in[0], mutations[1], progress[2],
    // in_epoch_events[3])
    ctrl_in: i32,
    mutations_in: i32,
    progress_in: i32,
    epoch_events_in: i32,
    // Outputs (manifest order: ctrl_out[0], replay_plan[1],
    // retention[2], events_out[3], metrics[4])
    ctrl_out: i32,
    replay_plan_out: i32,
    retention_out: i32,
    events_out: i32,
    metrics_out: i32,

    /// Small ordinal that names this registry among its siblings; the
    /// 8-byte `worker_id` the contract carries is derived from it.
    worker_ordinal: u8,

    // Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=registered, 1=cancelled).
    m_registered: u64,
    m_cancelled: u64,
    step_ctr: u64,

    hub: WatchHub,
    sw: SessionWorker<MAX_WATCHES>,
    scratch: [u8; SCRATCH_BUF_SIZE],
}

define_params! {
    RegistryState;

    // Which of the anchor's workers this instance is (0 = the one new
    // sessions attach to, 1 = the standby). Names the instance on the
    // control plane; a graph with one registry leaves it at 0.
    1, worker_id, u8, 0
        => |s, d, len| { s.worker_ordinal = p_u8(d, len, 0, 0); };
}

impl RegistryState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.ctrl_in = -1;
        self.mutations_in = -1;
        self.progress_in = -1;
        self.epoch_events_in = -1;
        self.ctrl_out = -1;
        self.replay_plan_out = -1;
        self.retention_out = -1;
        self.events_out = -1;
        self.metrics_out = -1;
        self.worker_ordinal = 0;
        self.m_registered = 0;
        self.m_cancelled = 0;
        self.step_ctr = 0;
        self.hub.init();
        self.sw.init(worker_id(CLASS_WATCH, 0));
    }
}

// ── Helpers ───────────────────────────────────────────────────────────

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

/// A sink over `ctrl_out` for the worker core's replies.
macro_rules! ctrl_sink {
    ($reg:expr) => {{
        let sys = $reg.syscalls;
        let chan = $reg.ctrl_out;
        move |msg: u8, payload: &[u8]| -> bool {
            if sys.is_null() {
                return false;
            }
            unsafe { write_envelope(&*sys, chan, msg, payload) }
        }
    }};
}

// ── Session control ───────────────────────────────────────────────────

fn handle_session_frame(reg: &mut RegistryState, msg_type: u8, payload: &[u8]) {
    let mut sink = ctrl_sink!(reg);
    let action = reg.sw.handle_frame(msg_type, payload, &mut sink);
    match action {
        WorkerAction::None | WorkerAction::Attached(_) => {}
        WorkerAction::Detached(idx, _) | WorkerAction::ImportDiscarded(idx) => {
            let sid = *reg.sw.slots[idx].session_id();
            drop_record(reg, &sid);
        }
        WorkerAction::Drain(idx) => {
            let sid = *reg.sw.slots[idx].session_id();
            if let Some(rec) = reg.hub.find_session_mut(&sid) {
                rec.phase = WATCH_PHASE_DRAINING;
            }
            // Settled already (nothing in flight)? Export right away.
            try_export(reg, idx);
        }
        WorkerAction::Imported(idx) => {
            let blob = reg.sw.import_blob();
            match WatchRecord::import(blob) {
                Some(rec) if reg.hub.adopt(rec).is_ok() => {}
                _ => {
                    // The transfer verified but the record does not
                    // fit here; the anchor will find no RESUMED and
                    // refuse, which returns the session to the exporter.
                    reg.sw.forget(idx);
                }
            }
            reg.sw.import_applied(idx);
        }
        WorkerAction::Resumed(idx, new_epoch) => {
            let sid = *reg.sw.slots[idx].session_id();
            let mut plan: Option<(u64, u64)> = None;
            if let Some(rec) = reg.hub.find_session_mut(&sid) {
                rec.binding.adopt_epoch(new_epoch);
                rec.binding.anchor_id = reg.sw.slots[idx].binding.anchor_id;
                rec.phase = WATCH_PHASE_ACTIVE;
                rec.in_consumed = reg.sw.slots[idx].in_consumed;
                plan = Some((rec.watch_id, rec.last_sent_revision as u64));
            }
            if let Some((wid, from)) = plan {
                emit_replay_plan(reg, wid, from, 0);
            }
            emit_retention_floor(reg);
        }
        WorkerAction::Reinstated(idx) => {
            let sid = *reg.sw.slots[idx].session_id();
            let mut plan: Option<(u64, u64)> = None;
            if let Some(rec) = reg.hub.find_session_mut(&sid) {
                rec.phase = WATCH_PHASE_ACTIVE;
                plan = Some((rec.watch_id, rec.last_sent_revision as u64));
            }
            if let Some((wid, from)) = plan {
                emit_replay_plan(reg, wid, from, 0);
            }
        }
        WorkerAction::EpochBumped(idx, new_epoch) => {
            let sid = *reg.sw.slots[idx].session_id();
            if let Some(rec) = reg.hub.find_session_mut(&sid) {
                rec.binding.adopt_epoch(new_epoch);
            }
        }
    }
}

/// Declare `idx` drained and export its record once every emitted
/// event has been acked. Called on DRAIN and again each step while
/// the session is draining.
fn try_export(reg: &mut RegistryState, idx: usize) {
    if reg.sw.slots[idx].phase != WPHASE_DRAINING {
        return;
    }
    let sid = *reg.sw.slots[idx].session_id();
    let (settled, out_acked) = match reg.hub.find_session(&sid) {
        Some(rec) => (rec.outbound_settled(), rec.out_acked),
        // No record: nothing to move, export an empty blob so the
        // anchor's cursor check sees exactly what it delivered.
        None => (true, 0),
    };
    if !settled {
        return;
    }
    reg.sw.slots[idx].out_produced = out_acked;
    let mut sink = ctrl_sink!(reg);
    if !reg.sw.declare_drained(idx, &mut sink) {
        return;
    }
    let mut blob = [0u8; WATCH_EXPORT_MAX];
    let n = match reg.hub.find_session_mut(&sid) {
        Some(rec) => {
            rec.phase = WATCH_PHASE_DRAINED;
            rec.in_consumed = reg.sw.slots[idx].in_consumed;
            rec.export(&mut blob)
        }
        None => 0,
    };
    let _ = reg.sw.export(idx, &blob[..n], &mut sink);
}

fn drop_record(reg: &mut RegistryState, sid: &SessionId) {
    let wid = reg.hub.find_session(sid).map(|r| r.watch_id);
    if let Some(wid) = wid {
        let _ = reg.hub.cancel(wid);
        reg.m_cancelled += 1;
        emit_retention_floor(reg);
    }
}

// ── Data plane ────────────────────────────────────────────────────────

fn handle_ctrl(reg: &mut RegistryState, payload: &[u8]) {
    // [session_id:16][epoch:4][ctrl:1]…
    if payload.len() < sc::SESSION_HEADER + 1 {
        return;
    }
    let sid = session_worker::sid_of(payload);
    let epoch = sc::epoch(payload);
    let Some(idx) = reg.sw.admit(&sid, epoch) else {
        // Stale, future or unknown: fenced.
        return;
    };
    reg.sw.consumed(idx);
    match payload[sc::SESSION_HEADER] {
        WATCH_CTRL_DEFINE => handle_define(reg, idx, &sid, &payload[sc::SESSION_HEADER + 1..]),
        WATCH_CTRL_PROGRESS => {
            // Per etcd a progress ping yields a no-op WatchResponse at
            // the current revision; without a current-revision input
            // there is nothing to frame. Consumed and counted.
        }
        _ => {}
    }
}

fn handle_define(reg: &mut RegistryState, idx: usize, sid: &SessionId, def: &[u8]) {
    // [watch_id:8][tenant:4][start_rev:8][filters:1][progress:1]
    // [key_len:2][key…][range_end_len:2][range_end…]
    if def.len() < 8 + 4 + 8 + 1 + 1 + 2 {
        return;
    }
    let watch_id = u64::from_le_bytes(slice8(def, 0));
    let tenant_id = u32::from_le_bytes(slice4(def, 8));
    let start_rev = i64::from_le_bytes(slice8(def, 12));
    let filters = def[20];
    let progress_notify = def[21] != 0;
    let key_len = u16::from_le_bytes([def[22], def[23]]) as usize;
    let key_off = 24;
    if key_off + key_len + 2 > def.len() {
        return;
    }
    let key = &def[key_off..key_off + key_len];
    let re_off = key_off + key_len;
    let range_end_len = u16::from_le_bytes([def[re_off], def[re_off + 1]]) as usize;
    let re_start = re_off + 2;
    if re_start + range_end_len > def.len() {
        return;
    }
    let range_end = &def[re_start..re_start + range_end_len];
    let anchor = reg.sw.slots[idx].binding.anchor_id;
    if reg
        .hub
        .register(
            watch_id,
            tenant_id,
            key,
            range_end,
            start_rev,
            filters,
            progress_notify,
            *sid,
            anchor,
        )
        .is_ok()
    {
        if let Some(rec) = reg.hub.find_session_mut(sid) {
            rec.in_consumed = reg.sw.slots[idx].in_consumed;
        }
        reg.m_registered += 1;
        emit_retention_floor(reg);
    }
}

/// Emit a replay plan for `watch_id` over `(from, to]`.
///
/// The watch's key span is looked up here and shipped with the plan.
/// The fanout holds no watch table by design, so if the span did not
/// travel with the plan the only backfill it could perform would be a
/// full-keyspace scan filtered afterwards — one client reconnecting
/// would read the whole database.
fn emit_replay_plan(reg: &mut RegistryState, watch_id: u64, from: u64, to: u64) {
    let mut body = [0u8; 24 + 4 + watch_hub::WATCH_KEY_MAX];
    body[0..8].copy_from_slice(&watch_id.to_le_bytes());
    body[8..16].copy_from_slice(&from.to_le_bytes());
    body[16..24].copy_from_slice(&to.to_le_bytes());
    let mut n = 24usize;
    match reg.hub.get(watch_id) {
        Some(rec) => {
            let k = rec.key();
            let r = rec.range_end();
            body[n..n + 2].copy_from_slice(&(k.len() as u16).to_le_bytes());
            n += 2;
            body[n..n + k.len()].copy_from_slice(k);
            n += k.len();
            body[n..n + 2].copy_from_slice(&(r.len() as u16).to_le_bytes());
            n += 2;
            body[n..n + r.len()].copy_from_slice(r);
            n += r.len();
        }
        None => {
            body[n..n + 4].fill(0);
            n += 4;
        }
    }
    unsafe {
        let sys = reg.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(
                &*sys,
                reg.replay_plan_out,
                MSG_WATCH_REPLAY_PLAN,
                &body[..n],
            );
        }
    }
}

fn emit_retention_floor(reg: &mut RegistryState) {
    let floor = match reg.hub.retention_floor() {
        Some(f) => f as u64,
        None => return,
    };
    // [source:1][kpg_id:2][floor_revision:8] = 11 bytes; source =
    // 0x01 (watch_registry).
    let mut body = [0u8; 11];
    body[0] = 0x01;
    body[1] = 0;
    body[2] = 0;
    body[3..11].copy_from_slice(&floor.to_le_bytes());
    unsafe {
        let sys = reg.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, reg.retention_out, MSG_RETENTION_FLOOR, &body);
        }
    }
}

// ── Mutation dispatcher ───────────────────────────────────────────────

fn handle_mutation(reg: &mut RegistryState, payload: &[u8]) {
    // Inbound from kv_state_worker:
    //   [kpg:2][revision:8][op:1][key_len:2][key…][value_len:2][value…]
    if payload.len() < 2 + 8 + 1 + 2 {
        return;
    }
    let revision = i64::from_le_bytes(slice8(payload, 2));
    let op = payload[10];
    let key_len = u16::from_le_bytes([payload[11], payload[12]]) as usize;
    let key_off = 13;
    if key_off + key_len > payload.len() {
        return;
    }
    let key = &payload[key_off..key_off + key_len];
    let event_kind = match op {
        types::KV_OP_PUT => WATCH_EVENT_PUT,
        types::KV_OP_DELETE => WATCH_EVENT_DELETE,
        _ => return,
    };

    // For each matching in-service watch emit a personalised envelope
    // downstream with the watch_id prepended so the anchor can route
    // to the right open stream. Wire shape on `events_out` (consumed
    // by watch_fanout and forwarded verbatim as MSG_WATCH_FRAME):
    //
    //   [watch_id:8][kpg:2][rev:8][op:1][klen:2][k][vlen:2][v]
    let sys = reg.syscalls;
    let events_out = reg.events_out;
    let mut out_buf = [0u8; SCRATCH_BUF_SIZE];
    let body_len = payload.len();
    if 8 + body_len > out_buf.len() {
        return;
    }
    out_buf[8..8 + body_len].copy_from_slice(payload);

    const MAX_MATCHES_PER_EVENT: usize = 32;
    let mut emitted = 0usize;
    let mut matched_ids = [0u64; MAX_MATCHES_PER_EVENT];
    match_event(&reg.hub, 0, revision, event_kind, key, |m| {
        if emitted < MAX_MATCHES_PER_EVENT {
            matched_ids[emitted] = m.watch_id;
            emitted += 1;
        }
    });

    if emitted == 0 || sys.is_null() {
        return;
    }
    for i in 0..emitted {
        let wid = matched_ids[i];
        out_buf[0..8].copy_from_slice(&wid.to_le_bytes());
        let ok =
            unsafe { write_envelope(&*sys, events_out, MSG_WATCH_EVENT, &out_buf[..8 + body_len]) };
        if ok {
            if let Some(rec) = reg.hub.get_mut(wid) {
                rec.out_emitted = rec.out_emitted.wrapping_add(1);
            }
        }
    }
}

// ── Progress ack from fanout ──────────────────────────────────────────

fn handle_progress_ack(reg: &mut RegistryState, payload: &[u8]) {
    // [watch_id:8][revision:8][kind:1]
    if payload.len() < 17 {
        return;
    }
    let watch_id = u64::from_le_bytes(slice8(payload, 0));
    let rev = i64::from_le_bytes(slice8(payload, 8));
    match payload[16] {
        WATCH_PROGRESS_DELIVERED => {
            if reg.hub.mark_delivered(watch_id, rev).is_ok() {
                emit_retention_floor(reg);
            }
        }
        WATCH_PROGRESS_COMPACTED => {
            // The replay window fell behind the compaction floor: the
            // watch was refused explicitly on the wire by the fanout's
            // frame; here it ends. The anchor drops its session from
            // the same frame, so no DETACH exchange follows.
            let sid = reg.hub.get(watch_id).map(|r| r.binding.session_id);
            if let Some(sid) = sid {
                if let Some(idx) = reg.sw.find(&sid) {
                    reg.sw.forget(idx);
                }
                drop_record(reg, &sid);
            }
        }
        _ => {}
    }
}

/// Substrate-driven placement epoch. Held on the hub; no session is
/// fenced by it — sessions whose placement moved are rebound by the
/// anchor / directory, one handoff each.
fn handle_epoch_event(reg: &mut RegistryState, msg_type: u8, payload: &[u8]) {
    if let Some(new_epoch) = placement_event_epoch(msg_type, payload) {
        let _ = reg.hub.advance_placement_epoch(new_epoch);
    }
}

// ── Module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<RegistryState>() as u32
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
    if state_size < core::mem::size_of::<RegistryState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let reg = unsafe { &mut *state.cast::<RegistryState>() };
    reg.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(reg, params, params_len) };
    }
    reg.sw
        .init(worker_id(CLASS_WATCH, u16::from(reg.worker_ordinal)));

    reg.ctrl_in = in_chan;
    reg.ctrl_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        reg.mutations_in = dev_channel_port(sys, 0, 1);
        reg.progress_in = dev_channel_port(sys, 0, 2);
        reg.epoch_events_in = dev_channel_port(sys, 0, 3);
        reg.replay_plan_out = dev_channel_port(sys, 1, 1);
        reg.retention_out = dev_channel_port(sys, 1, 2);
        reg.events_out = dev_channel_port(sys, 1, 3);
        reg.metrics_out = dev_channel_port(sys, 1, 4);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let reg = unsafe { &mut *state.cast::<RegistryState>() };
    unsafe {
        let sys_ptr = reg.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;

        // Drain at most one envelope per input per tick; the
        // scheduler ticks again to drain more.
        if let Some((mt, len)) = read_envelope(sys, reg.ctrl_in, &mut reg.scratch) {
            let mut tmp = [0u8; SCRATCH_BUF_SIZE];
            tmp[..len].copy_from_slice(&reg.scratch[..len]);
            if (0x70..=0x9F).contains(&mt) {
                handle_session_frame(reg, mt, &tmp[..len]);
            } else if mt == MSG_WATCH_CTRL {
                handle_ctrl(reg, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, reg.mutations_in, &mut reg.scratch) {
            if mt == MSG_WATCH_EVENT {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&reg.scratch[..len]);
                handle_mutation(reg, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, reg.progress_in, &mut reg.scratch) {
            if mt == MSG_WATCH_PROGRESS {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&reg.scratch[..len]);
                handle_progress_ack(reg, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, reg.epoch_events_in, &mut reg.scratch) {
            if mt == MSG_PLACEMENT_EPOCH_EVENT {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&reg.scratch[..len]);
                handle_epoch_event(reg, mt, &tmp[..len]);
            }
        }

        // Sessions draining toward a handoff export as soon as their
        // last emitted event is acked.
        let mut i = 0;
        while i < MAX_WATCHES {
            if reg.sw.slots[i].in_use && reg.sw.slots[i].phase == WPHASE_DRAINING {
                try_export(reg, i);
            }
            i += 1;
        }

        reg.step_ctr = reg.step_ctr.wrapping_add(1);
        if reg.step_ctr.is_multiple_of(5000) && !reg.syscalls.is_null() {
            telemetry::emit_counters(
                &*reg.syscalls,
                reg.metrics_out,
                &[reg.m_registered, reg.m_cancelled],
            );
        }
    }
    0
}
