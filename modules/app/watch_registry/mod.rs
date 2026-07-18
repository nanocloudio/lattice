//! watch_registry — durable watch state owner.
//!
//! Phase 4 module. Owns the `WatchID → WatchState` table via the
//! `watch_hub` facade. Processes:
//!
//! - **`ctrl_in`** — `MSG_WATCH_CTRL` envelopes from the etcd anchor.
//!   `ctrl = 0` create, `1` resume, `2` cancel, `3` progress_notify
//!   ping. Responds with `MSG_WATCH_CTRL_ACK` carrying the assigned
//!   `(watch_id, session_epoch)`.
//!
//! - **`mutations`** — `MSG_WATCH_EVENT` envelopes from
//!   `kv_state_worker`. Walks the active-watch table, picks
//!   matching watches via the facade's `match_event`, forwards the
//!   event to `events_out` for the fanout to frame and deliver.
//!
//! - **`progress`** — ack envelopes from `watch_fanout` confirming
//!   delivery up to a given revision. Bumps each watch's
//!   `last_sent_revision`.
//!
//! Emits **`retention`** records to `compaction_coordinator` so the
//! KV compaction floor never advances past a watch's pending
//! replay window. Phase 5's compaction wiring consumes them.
//!
//! Phase 8a wired the substrate-driven path: `control_plane.epoch_events`
//! feeds `in_epoch_events`, and on each `MSG_PLACEMENT_EPOCH_EVENT`
//! every active watch's `session_epoch` is bumped via
//! `WatchHub::advance_cluster_epoch` and a fresh `MSG_WATCH_REPLAY_PLAN`
//! is emitted so the fanout knows to resend frames buffered under the
//! old epoch. The legacy `ctrl = 1` (client-driven resume) path stays
//! intact for the case where a single client wants to re-bind without
//! the whole placement moving.

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
    reason = "hand-written index loops build wire envelopes byte-by-byte throughout these modules; the explicit form is the module idiom"
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

#[path = "../../common/watch_hub.rs"]
mod watch_hub;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use watch_hub::{
    match_event, pack_binding, MatchedWatch, WatchHub, WATCH_EVENT_DELETE, WATCH_EVENT_PUT,
};
use wire::{
    MSG_PLACEMENT_EPOCH_EVENT, MSG_RETENTION_FLOOR, MSG_WATCH_CTRL, MSG_WATCH_CTRL_ACK,
    MSG_WATCH_EVENT, MSG_WATCH_REPLAY_PLAN,
};

const SCRATCH_BUF_SIZE: usize = 4096;

// ── Wire field shapes (recap from modules/common/wire.rs) ─────────────
//
// MSG_WATCH_CTRL payload:
//   [ctrl:u8][session_id:u64 LE][session_epoch:u32 LE]
//   [tenant_id:u32 LE][kpg_id:u16 LE][filter_len:u16 LE][filter…]
// where `filter` (when present) is the layout:
//   [start_revision:i64 LE][filter_bits:u8][progress_notify:u8]
//   [conn_id:u8][stream_id:u32 LE][key_len:u16 LE][key…]
//   [range_end_len:u16 LE][range_end…]
//
// MSG_WATCH_CTRL_ACK payload:
//   [ctrl:u8][watch_id:u64 LE][session_epoch:u32 LE][status:u8]
// where `status` = 0 OK, 1 not-found, 2 table-full, 3 bad-key,
// 4 bad-filter, 5 invalid-id.
//
// MSG_WATCH_EVENT payload:
//   [kpg_id:u16 LE][revision:u64 LE][op:u8][key_len:u16 LE][key…]
//   [value_len:u16 LE][value…]
//
// (we forward this verbatim to fanout via `events_out` so the
// frame builder can re-shape it into etcd-v3 WatchResponse.)
//
// MSG_WATCH_REPLAY_PLAN payload:
//   [session_id:u64 LE][from_revision:u64 LE][to_revision:u64 LE]
//   [key_len:u16 LE][key…][range_end_len:u16 LE][range_end…]

const WATCH_CTRL_CREATE: u8 = 0;
const WATCH_CTRL_RESUME: u8 = 1;
const WATCH_CTRL_CANCEL: u8 = 2;
const WATCH_CTRL_PROGRESS: u8 = 3;

const ACK_STATUS_OK: u8 = 0;
const ACK_STATUS_NOT_FOUND: u8 = 1;
const ACK_STATUS_TABLE_FULL: u8 = 2;
const ACK_STATUS_BAD_KEY: u8 = 3;
const ACK_STATUS_BAD_FILTER: u8 = 4;
const ACK_STATUS_INVALID_ID: u8 = 5;

#[repr(C)]
struct RegistryState {
    syscalls: *const SyscallTable,

    // Inputs (manifest order: ctrl_in[0], mutations[1], progress[2],
    // in_epoch_events[3])
    ctrl_in: i32,
    mutations_in: i32,
    progress_in: i32,
    epoch_events_in: i32,
    // Outputs (manifest order: replay_plan[0], retention[1],
    // events_out[2], metrics[3])
    replay_plan_out: i32,
    retention_out: i32,
    events_out: i32,
    metrics_out: i32,
    /// `ctrl_in` is the kernel's first-input-channel, also exposed
    /// to the anchor as `MSG_WATCH_CTRL_ACK` recipient. We send
    /// acks back through the same channel by treating its output
    /// twin as the first output port.
    ack_out: i32,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=registered, 1=cancelled).
    m_registered: u64,
    m_cancelled: u64,
    step_ctr: u64,

    hub: WatchHub,
    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl RegistryState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.ctrl_in = -1;
        self.mutations_in = -1;
        self.progress_in = -1;
        self.epoch_events_in = -1;
        self.replay_plan_out = -1;
        self.retention_out = -1;
        self.events_out = -1;
        self.metrics_out = -1;
        self.ack_out = -1;
        self.m_registered = 0;
        self.m_cancelled = 0;
        self.step_ctr = 0;
        self.hub.init();
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

// ── Ctrl handler ──────────────────────────────────────────────────────

fn handle_ctrl(reg: &mut RegistryState, payload: &[u8]) {
    if payload.is_empty() {
        return;
    }
    let ctrl = payload[0];
    match ctrl {
        WATCH_CTRL_CREATE => handle_create(reg, payload),
        WATCH_CTRL_RESUME => handle_resume(reg, payload),
        WATCH_CTRL_CANCEL => handle_cancel(reg, payload),
        WATCH_CTRL_PROGRESS => handle_progress_notify(reg, payload),
        _ => {}
    }
}

fn handle_create(reg: &mut RegistryState, payload: &[u8]) {
    // [ctrl:1][sid:8][sepoch:4][tenant:4][kpg:2][filter_len:2][filter…]
    if payload.len() < 1 + 8 + 4 + 4 + 2 + 2 {
        return;
    }
    // The anchor pre-allocates the watch_id and ships it in
    // `session_id`; the registry treats that as authoritative so the
    // anchor's open-stream lookup (`find_watch_stream(watch_id)`) and
    // the registry's hub use the same id. A `session_id == 0` falls
    // back to a hub-issued id.
    let session_id = u64::from_le_bytes(slice8(payload, 1));
    let _session_epoch = u32::from_le_bytes(slice4(payload, 9));
    let tenant_id = u32::from_le_bytes(slice4(payload, 13));
    let _kpg_id = u16::from_le_bytes([payload[17], payload[18]]);
    let filter_len = u16::from_le_bytes([payload[19], payload[20]]) as usize;
    let filter_off = 21;
    if filter_off + filter_len > payload.len() {
        return;
    }
    let filter = &payload[filter_off..filter_off + filter_len];
    // filter: [start_rev:8][filter_bits:1][progress:1][conn:1]
    //         [stream:4][key_len:2][key…][range_end_len:2][range_end…]
    if filter.len() < 8 + 1 + 1 + 1 + 4 + 2 {
        send_ack(
            reg,
            ctrl_id_for(WATCH_CTRL_CREATE),
            0,
            0,
            ACK_STATUS_BAD_KEY,
        );
        return;
    }
    let start_rev = i64::from_le_bytes(slice8(filter, 0));
    let filter_bits = filter[8];
    let progress_notify = filter[9] != 0;
    let conn_id = filter[10];
    let stream_id = u32::from_le_bytes(slice4(filter, 11));
    let key_len = u16::from_le_bytes([filter[15], filter[16]]) as usize;
    let key_off = 17;
    if key_off + key_len + 2 > filter.len() {
        send_ack(
            reg,
            ctrl_id_for(WATCH_CTRL_CREATE),
            0,
            0,
            ACK_STATUS_BAD_KEY,
        );
        return;
    }
    let key = &filter[key_off..key_off + key_len];
    let re_off = key_off + key_len;
    let range_end_len = u16::from_le_bytes([filter[re_off], filter[re_off + 1]]) as usize;
    let re_start = re_off + 2;
    if re_start + range_end_len > filter.len() {
        send_ack(
            reg,
            ctrl_id_for(WATCH_CTRL_CREATE),
            0,
            0,
            ACK_STATUS_BAD_KEY,
        );
        return;
    }
    let range_end = &filter[re_start..re_start + range_end_len];

    let binding = pack_binding(conn_id, stream_id);
    match reg.hub.register(
        session_id,
        tenant_id,
        key,
        range_end,
        start_rev,
        filter_bits,
        progress_notify,
        binding,
    ) {
        Ok(id) => {
            // `register` stamps `cluster_epoch` into the record; the
            // hub-side substrate-driven path keeps that authoritative,
            // so we read it back without a defensive fallback.
            let epoch = reg.hub.cluster_epoch;
            reg.m_registered += 1;
            send_ack(
                reg,
                ctrl_id_for(WATCH_CTRL_CREATE),
                id,
                epoch,
                ACK_STATUS_OK,
            );
            emit_retention_floor(reg);
        }
        Err(err) => {
            let status = match err {
                watch_hub::WatchError::TableFull => ACK_STATUS_TABLE_FULL,
                watch_hub::WatchError::BadFilter => ACK_STATUS_BAD_FILTER,
                watch_hub::WatchError::KeyTooLarge => ACK_STATUS_BAD_KEY,
                watch_hub::WatchError::InvalidId => ACK_STATUS_INVALID_ID,
                watch_hub::WatchError::NotFound => ACK_STATUS_NOT_FOUND,
            };
            send_ack(reg, ctrl_id_for(WATCH_CTRL_CREATE), 0, 0, status);
        }
    }
}

fn handle_resume(reg: &mut RegistryState, payload: &[u8]) {
    if payload.len() < 1 + 8 + 4 {
        return;
    }
    let watch_id = u64::from_le_bytes(slice8(payload, 1));
    // session_epoch from caller is informational; we always bump.
    // The optional anchor binding is appended after the kpg field.
    let new_binding = if payload.len() >= 21 + 5 {
        let conn_id = payload[21];
        let stream_id = u32::from_le_bytes(slice4(payload, 22));
        pack_binding(conn_id, stream_id)
    } else {
        watch_hub::ANCHOR_UNBOUND
    };
    match reg.hub.rebind(watch_id, new_binding) {
        Ok(epoch) => {
            send_ack(
                reg,
                ctrl_id_for(WATCH_CTRL_RESUME),
                watch_id,
                epoch,
                ACK_STATUS_OK,
            );
            // Backfill the gap the client missed while disconnected.
            // `from` is the watch's own `last_sent_revision` — the
            // highest revision the fanout CONFIRMED was on the wire,
            // not the highest the registry matched. Using the matched
            // revision would silently drop the events that were framed
            // but never delivered, which is exactly the window a resume
            // exists to recover. `to = 0` means "up to now", so the
            // backfill meets the live tail without a second round trip
            // to discover the current revision.
            let from = reg
                .hub
                .get(watch_id)
                .map(|r| r.last_sent_revision as u64)
                .unwrap_or(0);
            emit_replay_plan(reg, watch_id, from, 0, epoch);
        }
        Err(_) => send_ack(
            reg,
            ctrl_id_for(WATCH_CTRL_RESUME),
            watch_id,
            0,
            ACK_STATUS_NOT_FOUND,
        ),
    }
}

fn handle_cancel(reg: &mut RegistryState, payload: &[u8]) {
    if payload.len() < 1 + 8 {
        return;
    }
    let watch_id = u64::from_le_bytes(slice8(payload, 1));
    match reg.hub.cancel(watch_id) {
        Ok(_last) => {
            reg.m_cancelled += 1;
            send_ack(
                reg,
                ctrl_id_for(WATCH_CTRL_CANCEL),
                watch_id,
                0,
                ACK_STATUS_OK,
            );
            emit_retention_floor(reg);
        }
        Err(_) => send_ack(
            reg,
            ctrl_id_for(WATCH_CTRL_CANCEL),
            watch_id,
            0,
            ACK_STATUS_NOT_FOUND,
        ),
    }
}

fn handle_progress_notify(reg: &mut RegistryState, payload: &[u8]) {
    // Per etcd: a `progress_notify` ping triggers a no-op
    // WatchResponse carrying the current revision. With no
    // substrate-side current-revision input yet, this is recorded
    // and acked but no frame is forwarded.
    if payload.len() < 1 + 8 {
        return;
    }
    let watch_id = u64::from_le_bytes(slice8(payload, 1));
    // The progress ping is only valid for a known watch_id. Look it
    // up and surface the substrate-stamped epoch verbatim; for an
    // unknown id we ack with NOT_FOUND so the anchor can drop the
    // pending request rather than swallow it silently.
    let (epoch, status) = match reg.hub.get(watch_id) {
        Some(rec) => (rec.session_epoch, ACK_STATUS_OK),
        None => (reg.hub.cluster_epoch, ACK_STATUS_NOT_FOUND),
    };
    send_ack(
        reg,
        ctrl_id_for(WATCH_CTRL_PROGRESS),
        watch_id,
        epoch,
        status,
    );
}

const fn ctrl_id_for(ctrl: u8) -> u8 {
    ctrl
}

fn send_ack(reg: &mut RegistryState, ctrl: u8, watch_id: u64, session_epoch: u32, status: u8) {
    // [ctrl:1][watch_id:8][session_epoch:4][status:1] = 14 bytes
    let mut body = [0u8; 14];
    body[0] = ctrl;
    body[1..9].copy_from_slice(&watch_id.to_le_bytes());
    body[9..13].copy_from_slice(&session_epoch.to_le_bytes());
    body[13] = status;
    unsafe {
        let sys = reg.syscalls;
        if !sys.is_null() {
            // The ack flows back to the anchor via the dedicated
            // ack output channel.
            let _ = write_envelope(&*sys, reg.ack_out, MSG_WATCH_CTRL_ACK, &body);
        }
    }
}

/// Emit a replay plan for `watch_id` over `(from, to]`.
///
/// The watch's key span is looked up here and shipped with the plan.
/// The fanout holds no watch table by design, so if the span did not
/// travel with the plan the only backfill it could perform would be a
/// full-keyspace scan filtered afterwards — one client reconnecting
/// would read the whole database. The registry is the component that
/// already knows the span, so it is the component that pays nothing to
/// send it.
fn emit_replay_plan(reg: &mut RegistryState, watch_id: u64, from: u64, to: u64, _epoch: u32) {
    let mut body = [0u8; 24 + 4 + watch_hub::WATCH_KEY_MAX];
    body[0..8].copy_from_slice(&watch_id.to_le_bytes());
    body[8..16].copy_from_slice(&from.to_le_bytes());
    body[16..24].copy_from_slice(&to.to_le_bytes());
    let mut n = 24usize;
    // A watch that has vanished between the trigger and here gets an
    // empty span rather than a wrong one. The fanout treats an empty
    // key with an empty range_end as "nothing to backfill", which is
    // the correct reading: there is no watch to backfill for.
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
    let _kpg_id = u16::from_le_bytes([payload[0], payload[1]]);
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

    // For each matching watch emit a personalised envelope downstream
    // with the watch_id prepended so the anchor can route to the right
    // open stream. Wire shape on `events_out` (consumed by watch_fanout
    // and forwarded verbatim as MSG_WATCH_FRAME):
    //
    //   [watch_id:8][kpg:2][rev:8][op:1][klen:2][k][vlen:2][v]
    //
    // The anchor reads `watch_id` to find the stream, then builds the
    // etcd-v3 WatchResponse protobuf from the trailing fields. The
    // fanout stays transformation-free; only the framing layer differs.
    let sys = reg.syscalls;
    let events_out = reg.events_out;
    let mut out_buf = [0u8; SCRATCH_BUF_SIZE];
    let body_len = payload.len();
    if 8 + body_len > out_buf.len() {
        return;
    }
    out_buf[8..8 + body_len].copy_from_slice(payload);

    // Collect matched watch_ids into a fixed array (the closure can't
    // borrow `out_buf` mutably and emit per-match without aliasing the
    // hub borrow). 32 matches per event is generous — each etcd client
    // typically has 1-2 watches; redis pub-sub fan-out would be the
    // first workload to bump up against this.
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
        unsafe {
            let _ = write_envelope(&*sys, events_out, MSG_WATCH_EVENT, &out_buf[..8 + body_len]);
        }
    }
}

// ── Progress ack from fanout ──────────────────────────────────────────

fn handle_progress_ack(reg: &mut RegistryState, payload: &[u8]) {
    // [watch_id:8][revision:8]
    if payload.len() < 16 {
        return;
    }
    let watch_id = u64::from_le_bytes(slice8(payload, 0));
    let rev = i64::from_le_bytes(slice8(payload, 8));
    if reg.hub.mark_delivered(watch_id, rev).is_ok() {
        emit_retention_floor(reg);
    }
}

/// Substrate-driven epoch advancement. The wire payload (matching
/// `control_plane.epoch_events`) is
/// `[prev_epoch:u32 LE][new_epoch:u32 LE]`. We trust `new_epoch` as
/// authoritative — `prev_epoch` is informational and the hub's own
/// monotonic guard rejects any non-advancing event.
///
/// For each active watch this bumps `session_epoch` to `new_epoch`,
/// then emits a `MSG_WATCH_REPLAY_PLAN` so the fanout can re-issue
/// any frames buffered under the old epoch.
fn handle_epoch_event(reg: &mut RegistryState, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let _prev = u32::from_le_bytes(slice4(payload, 0));
    let new_epoch = u32::from_le_bytes(slice4(payload, 4));
    let fenced = reg.hub.advance_cluster_epoch(new_epoch);
    if fenced == 0 {
        return;
    }
    // Emit a replay plan per active watch so the fanout knows to
    // resend anything queued under the prior epoch. `to=0` is the
    // "live-tail" sentinel: the fanout backfills from `from` and
    // resumes streaming as fresh events land.
    let mut i = 0;
    while i < watch_hub::MAX_WATCHES {
        let rec = &reg.hub.records[i];
        if rec.in_use() {
            let from = rec.last_sent_revision as u64;
            let watch_id = rec.watch_id;
            let epoch = rec.session_epoch;
            emit_replay_plan(reg, watch_id, from, 0, epoch);
        }
        i += 1;
    }
}

// ── slice helpers ─────────────────────────────────────────────────────

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
    _params: *const u8,
    _params_len: usize,
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

    reg.ctrl_in = in_chan;
    // The first output channel doubles as the ack-out path back
    // to the anchor. We treat replay_plan as port 0 per manifest
    // and use a separate dedicated ack delivery via port -1 (the
    // anchor side knows to read the channel back).
    reg.ack_out = out_chan;

    unsafe {
        let sys = &*sys_ptr;
        reg.mutations_in = dev_channel_port(sys, 0, 1);
        reg.progress_in = dev_channel_port(sys, 0, 2);
        reg.epoch_events_in = dev_channel_port(sys, 0, 3);
        reg.replay_plan_out = dev_channel_port(sys, 1, 0);
        reg.retention_out = dev_channel_port(sys, 1, 1);
        reg.events_out = dev_channel_port(sys, 1, 2);
        reg.metrics_out = dev_channel_port(sys, 1, 3);
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
            if mt == MSG_WATCH_CTRL {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&reg.scratch[..len]);
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
        if let Some((_mt, len)) = read_envelope(sys, reg.progress_in, &mut reg.scratch) {
            let mut tmp = [0u8; SCRATCH_BUF_SIZE];
            tmp[..len].copy_from_slice(&reg.scratch[..len]);
            handle_progress_ack(reg, &tmp[..len]);
        }
        if let Some((mt, len)) = read_envelope(sys, reg.epoch_events_in, &mut reg.scratch) {
            if mt == MSG_PLACEMENT_EPOCH_EVENT {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&reg.scratch[..len]);
                handle_epoch_event(reg, &tmp[..len]);
            }
        }

        // Phase-14: emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
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
