//! watch_fanout — high-rate event fanout for active watches.
//!
//! Consumes `MSG_WATCH_EVENT` envelopes from `watch_registry`, frames
//! them as `MSG_WATCH_FRAME` envelopes the `etcd_edge_anchor` writes to
//! the wire, and reports delivery back to the registry via `progress`.
//! One event per frame; the etcd anchor stitches frames into a
//! `WatchResponse`.
//!
//! ## Replay
//!
//! On `MSG_WATCH_REPLAY_PLAN` the fanout backfills the window the
//! client missed while disconnected. It issues `KV_OP_SCAN_VERSIONS`
//! over the plan's key span and `(from, to]` revision window, and
//! turns each returned version into the same `MSG_WATCH_FRAME` a live
//! event would have produced — so a resumed stream and a continuous
//! one are indistinguishable to the client, which is the entire point.
//!
//! Two properties the implementation depends on:
//!
//! - **Every version, not the survivor.** `KV_OP_SCAN_VERSIONS` reports
//!   what HAPPENED across the window rather than what the database
//!   looked like at the end of it. A key written three times produced
//!   three events and the client receives three. Deletes arrive as
//!   tombstones; suppressing them would leave a resumed client
//!   believing a deleted key still exists.
//!
//! - **Refusal is reported, never approximated.** If the window has
//!   been reclaimed the provider says `COMPACTED`, and the fanout
//!   abandons the backfill and counts it rather than delivering the
//!   surviving tail. A partial history is indistinguishable from a
//!   whole one to the client, so offering it would convert a visible
//!   failure into a silent one.
//!
//! Backfills are serialized: one in flight, the rest queued. That keeps
//! the module's memory fixed and stops a reconnect storm from issuing
//! an unbounded number of concurrent scans. When the queue is full a
//! plan is dropped AND COUNTED — the metric is the point, because a
//! silently dropped plan is a client that quietly misses events.

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

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[path = "../../common/watch_replay.rs"]
mod watch_replay;

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox, ENVELOPE_HDR};

use types::{
    KV_OP_RANGE_META, KV_OP_SCAN_VERSIONS, KV_RESULT_COMPACTED, KV_RESULT_KVS, KV_RESULT_VERSIONS,
    PROTO_INTERNAL_WATCH, VERSIONS_FLAG_META,
};
use watch_replay::{
    build_frame, parse_replay_plan, walk_versions_page_meta, ReplayPlan, ENTRY_FLAG_MANIFEST,
    FRAME_OP_DELETE, FRAME_OP_PUT_MANIFEST, PLAN_KEY_MAX, SCAN_BODY_MAX,
};
use wire::{
    MSG_KV_REQUEST, MSG_KV_RESPONSE, MSG_WATCH_EVENT, MSG_WATCH_FRAME, MSG_WATCH_PROGRESS,
    MSG_WATCH_REPLAY_PLAN, WATCH_FRAME_OP_COMPACTED, WATCH_PROGRESS_COMPACTED,
    WATCH_PROGRESS_DELIVERED,
};

/// One envelope in or out: the largest watch event behind its watch id
/// (`wire::WATCH_EVENT_MAX` + 8), and the largest KV reply a backfill
/// page reads (the router's 8320).
const SCRATCH_BUF_SIZE: usize = 8832;

/// Queued replay plans. One is in flight at a time; this bounds how
/// many reconnects can be absorbed before plans start being dropped.
/// Sized against the etcd anchor's stream count rather than its
/// connection count — a reconnect storm re-registers watches, and each
/// re-registration is one plan.
const MAX_PENDING_PLANS: usize = 16;

/// Versions requested per backfill page. Bounded so one page fits the
/// scratch buffer with room for the largest entry, and so a single
/// module_step never spends its budget draining one client's history.
const REPLAY_PAGE_LIMIT: u16 = 32;

/// Bytes of the versions a window's events replaced that lie below the
/// window (read back one point read each: the window holds the rest).
const PREV_BYTES: usize = 65536;

/// Bytes of one backfill window held for ordering. The provider walks a
/// span key by key (and a key's versions newest-first); a watch delivers
/// in revision order, so the whole window is gathered, sorted, then sent.
/// A window past this (or past `WINDOW_FRAMES`) is refused as compacted —
/// the client relists — never sent out of order or cut.
const WINDOW_BYTES: usize = 65536;
/// Frames one backfill window holds for ordering.
const WINDOW_FRAMES: usize = 256;

#[repr(C)]
struct FanoutState {
    syscalls: *const SyscallTable,

    events_in: i32,
    replay_plan_in: i32,
    kv_in: i32,
    /// Each input's envelope reading (`common/envelope.rs`).
    events_inbox: Inbox,
    replay_plan_inbox: Inbox,
    kv_inbox: Inbox,
    frames_out: i32,
    progress_out: i32,
    kv_out: i32,
    metrics_out: i32,

    /// Count of frames emitted since boot (for the metrics surface).
    frames_emitted: u64,
    /// Replay plans received that could not be serviced: no KV path
    /// wired, a queue overflow, or a window the provider had already
    /// reclaimed. Counted rather than swallowed — an unserviced plan is
    /// a client silently missing events, so it must be visible.
    skipped_replays: u64,

    /// The backfill currently in flight, and the plans waiting behind
    /// it. `active` gates both: exactly one scan is outstanding at a
    /// time, so the module's KV load stays bounded no matter how many
    /// clients reconnect at once.
    active: bool,
    current: ReplayPlan,
    pending: [ReplayPlan; MAX_PENDING_PLANS],
    pending_len: usize,
    /// Correlation id for the in-flight scan. Monotonic, so a late
    /// response from an abandoned backfill is recognised and dropped
    /// rather than injected into the current one — which would deliver
    /// one watcher's history under another watcher's id.
    corr_seq: u64,

    /// The in-flight backfill's frames, gathered across its pages to be
    /// sent in revision order: frame bytes packed in `win`, and per frame
    /// (offset, length, revision).
    win: [u8; WINDOW_BYTES],
    win_len: usize,
    win_idx: [(u32, u16, u64); WINDOW_FRAMES],
    win_n: usize,
    /// Per window frame (in revision order, once sorted): where the
    /// version it replaced comes from — `PREV_NONE`, `PREV_WINDOW` (the
    /// frame `prev_ref` names), `PREV_STORED` (at `prev_ref` in
    /// `prev_buf`, `[vlen:2][value][create][mod][version][lease]`) or
    /// `PREV_FETCH` (a point read still to make).
    prev_kind: [u8; WINDOW_FRAMES],
    prev_ref: [u32; WINDOW_FRAMES],
    prev_buf: [u8; PREV_BYTES],
    prev_len: usize,
    /// Gathering pages (false), or reading back replaced versions (true)
    /// from `fill_at` on.
    filling: bool,
    fill_at: usize,

    // Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=events, 1=delivered, 2=dropped, 3=replayed,
    // 4=replays_unserviceable).
    m_events: u64,
    m_delivered: u64,
    m_dropped: u64,
    m_replayed: u64,
    m_replays_unserviceable: u64,
    step_ctr: u64,

    /// An envelope read whole: header room, then the payload.
    scratch: [u8; ENVELOPE_HDR + SCRATCH_BUF_SIZE],
}

impl FanoutState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.events_in = -1;
        self.replay_plan_in = -1;
        self.kv_in = -1;
        self.events_inbox = Inbox::new();
        self.replay_plan_inbox = Inbox::new();
        self.kv_inbox = Inbox::new();
        self.frames_out = -1;
        self.progress_out = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.frames_emitted = 0;
        self.skipped_replays = 0;
        self.active = false;
        self.current = ReplayPlan::empty();
        self.pending_len = 0;
        self.corr_seq = 0;
        self.win_len = 0;
        self.win_n = 0;
        self.prev_len = 0;
        self.filling = false;
        self.fill_at = 0;
        self.m_events = 0;
        self.m_delivered = 0;
        self.m_dropped = 0;
        self.m_replayed = 0;
        self.m_replays_unserviceable = 0;
        self.step_ctr = 0;
    }
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

fn handle_event(fanout: &mut FanoutState, event_payload: &[u8]) {
    // Frame the event for the anchor. The fanout doesn't know which
    // anchor stream to target (the routing is per-watch, owned by
    // watch_registry); for the single-watch-per-stream case the
    // anchor decodes the event and looks up its own pending
    // streams. The frame body is `MSG_WATCH_EVENT`'s payload
    // verbatim — the anchor's protobuf encoder rebuilds the
    // etcd-v3 WatchResponse from `[kpg, rev, op, key, value]`.
    fanout.m_events += 1;
    unsafe {
        let sys = fanout.syscalls;
        if sys.is_null() {
            fanout.m_dropped += 1;
            return;
        }
        if write_envelope(&*sys, fanout.frames_out, MSG_WATCH_FRAME, event_payload) {
            fanout.frames_emitted = fanout.frames_emitted.wrapping_add(1);
            fanout.m_delivered += 1;
            // One ack per framed event: it is the watch's acked
            // revision and its delivery cursor at once, so the registry
            // advances both in one step and they cannot disagree at a
            // handoff. The event body
            // is `[watch_id:8][kpg:2][rev:8]…`.
            if event_payload.len() >= 18 {
                let mut wid = [0u8; 8];
                wid.copy_from_slice(&event_payload[0..8]);
                let mut rev = [0u8; 8];
                rev.copy_from_slice(&event_payload[10..18]);
                emit_progress(
                    fanout,
                    u64::from_le_bytes(wid),
                    u64::from_le_bytes(rev),
                    WATCH_PROGRESS_DELIVERED,
                );
            }
        } else {
            fanout.m_dropped += 1;
        }
    }
}

/// Emit one `MSG_WATCH_PROGRESS` back to the registry.
fn emit_progress(fanout: &mut FanoutState, watch_id: u64, revision: u64, kind: u8) {
    let mut body = [0u8; 17];
    body[0..8].copy_from_slice(&watch_id.to_le_bytes());
    body[8..16].copy_from_slice(&revision.to_le_bytes());
    body[16] = kind;
    unsafe {
        let sys = fanout.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, fanout.progress_out, MSG_WATCH_PROGRESS, &body);
        }
    }
}

/// Refuse a watch explicitly because its replay window fell behind the
/// compaction floor: a `WATCH_FRAME_OP_COMPACTED` frame to the anchor
/// (which cancels the client stream with the compact revision) and a
/// `WATCH_PROGRESS_COMPACTED` ack to the registry (which ends the
/// record). Silent short-resume is exactly what must not happen.
fn refuse_compacted(fanout: &mut FanoutState, watch_id: u64, compact_rev: u64) {
    // [watch_id:8][kpg:2][rev:8][op:1][klen:2][vlen:2] = a minimal frame.
    let mut body = [0u8; 23];
    body[0..8].copy_from_slice(&watch_id.to_le_bytes());
    body[10..18].copy_from_slice(&compact_rev.to_le_bytes());
    body[18] = WATCH_FRAME_OP_COMPACTED;
    unsafe {
        let sys = fanout.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, fanout.frames_out, MSG_WATCH_FRAME, &body);
        }
    }
    emit_progress(fanout, watch_id, compact_rev, WATCH_PROGRESS_COMPACTED);
}

/// Record a plan we could not service. Always paired with a reason in
/// the caller, because the metric's job is to make a client that
/// silently missed events visible.
fn unserviceable(fanout: &mut FanoutState) {
    fanout.skipped_replays = fanout.skipped_replays.wrapping_add(1);
    fanout.m_replays_unserviceable = fanout.m_replays_unserviceable.wrapping_add(1);
}

fn handle_replay_plan(fanout: &mut FanoutState, payload: &[u8]) {
    let Some(plan) = parse_replay_plan(payload) else {
        unserviceable(fanout);
        return;
    };
    // No KV path in this graph: the composition has no history provider
    // to replay from. Honest refusal, not a silent no-op.
    if fanout.kv_out < 0 {
        unserviceable(fanout);
        return;
    }
    // Nothing asked for: a closed window (the client missed nothing) or
    // an absent span (the watch vanished before the plan was built).
    // Both are legitimate no-ops, not failures — see
    // `ReplayPlan::is_empty`.
    if plan.is_empty() {
        return;
    }
    if fanout.active {
        if fanout.pending_len >= MAX_PENDING_PLANS {
            unserviceable(fanout);
            return;
        }
        fanout.pending[fanout.pending_len] = plan;
        fanout.pending_len += 1;
        return;
    }
    fanout.current = plan;
    fanout.active = true;
    issue_backfill_page(fanout);
}

/// Issue the next `KV_OP_SCAN_VERSIONS` page for the in-flight plan.
/// A write failure abandons the backfill rather than retrying forever:
/// a full outbound channel that stays full is a wedged graph, and
/// spinning on it would convert that into a livelock.
fn issue_backfill_page(fanout: &mut FanoutState) {
    fanout.corr_seq = fanout.corr_seq.wrapping_add(1);
    let corr = fanout.corr_seq;
    let plan = fanout.current;

    let mut body = [0u8; SCAN_BODY_MAX + 1];
    let Some(n) = plan.scan_body(REPLAY_PAGE_LIMIT, &mut body[..SCAN_BODY_MAX]) else {
        finish_backfill(fanout, false);
        return;
    };
    // Each version with its own metadata, as a live event carries it; a
    // large value's version as its manifest (marked), which the anchor
    // resolves as it does a live one's.
    body[n] = VERSIONS_FLAG_META | types::VERSIONS_FLAG_MANIFESTS;
    let n = n + 1;

    // MSG_KV_REQUEST payload:
    //   [corr:8][protocol:1][tenant:4][conn:1][consistency:1][op:1]
    //   [body_len:2][body]
    let mut env = [0u8; 18 + SCAN_BODY_MAX + 1];
    env[0..8].copy_from_slice(&corr.to_le_bytes());
    env[8] = PROTO_INTERNAL_WATCH;
    env[9..13].copy_from_slice(&0u32.to_le_bytes());
    // `conn_id` is a caller-chosen slot, not a connection. One backfill
    // is in flight at a time, so slot 0 is the only one needed.
    env[13] = 0;
    env[14] = 0; // default consistency: this reads committed history
    env[15] = KV_OP_SCAN_VERSIONS;
    env[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    env[18..18 + n].copy_from_slice(&body[..n]);

    let ok = unsafe {
        let sys = fanout.syscalls;
        !sys.is_null() && write_envelope(&*sys, fanout.kv_out, MSG_KV_REQUEST, &env[..18 + n])
    };
    if !ok {
        finish_backfill(fanout, false);
    }
}

/// End the in-flight backfill and start the next queued plan, if any.
/// `serviced` distinguishes "the window was delivered" from "we gave
/// up", which is the difference the metrics must preserve.
fn finish_backfill(fanout: &mut FanoutState, serviced: bool) {
    if !serviced {
        unserviceable(fanout);
    }
    fanout.win_len = 0;
    fanout.win_n = 0;
    fanout.prev_len = 0;
    fanout.filling = false;
    fanout.fill_at = 0;
    fanout.active = false;
    if fanout.pending_len > 0 {
        fanout.current = fanout.pending[0];
        let n = fanout.pending_len;
        let mut i = 1;
        while i < n {
            fanout.pending[i - 1] = fanout.pending[i];
            i += 1;
        }
        fanout.pending_len = n - 1;
        fanout.active = true;
        issue_backfill_page(fanout);
    }
}

/// Consume one `MSG_KV_RESPONSE` page and re-emit its versions as watch
/// frames.
fn handle_kv_response(fanout: &mut FanoutState, payload: &[u8]) {
    // [corr:8][conn:1][result:1][revision:8][body_len:2][body]
    if payload.len() < 20 {
        return;
    }
    let mut corr_bytes = [0u8; 8];
    corr_bytes.copy_from_slice(&payload[0..8]);
    let corr = u64::from_le_bytes(corr_bytes);
    // A response from a backfill we already abandoned must not be
    // injected into the current one — the frames would carry the wrong
    // watch_id and the client would receive another watcher's history.
    if !fanout.active || corr != fanout.corr_seq {
        return;
    }
    let result = payload[9];
    // A replaced version read back below the window (a point read's
    // answer, not a page).
    if fanout.filling {
        let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
        let body = payload.get(20..20 + blen).unwrap_or(&[]);
        fill_answer(fanout, result, body);
        return;
    }
    if result == KV_RESULT_COMPACTED {
        // The window is genuinely gone. Delivering the surviving tail
        // would hand the client a partial history it could not tell
        // from a complete one. Refuse the watch explicitly — cancel the
        // client stream with the compact revision and end the record —
        // rather than resuming short in silence.
        let compact_rev = u64::from_le_bytes([
            payload[10],
            payload[11],
            payload[12],
            payload[13],
            payload[14],
            payload[15],
            payload[16],
            payload[17],
        ]);
        refuse_compacted(fanout, fanout.current.watch_id, compact_rev);
        finish_backfill(fanout, false);
        return;
    }
    if result != KV_RESULT_VERSIONS {
        finish_backfill(fanout, false);
        return;
    }
    let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
    if payload.len() < 20 + blen || blen < 10 {
        finish_backfill(fanout, false);
        return;
    }
    let body = &payload[20..20 + blen];
    let watch_id = fanout.current.watch_id;

    // Each version framed straight into the window — the base frame and
    // its `[create][version][lease]` — to be sorted, given the version it
    // replaced, and sent once the window is whole. A window past its
    // bounds is refused as compacted at the newest revision seen: the
    // client relists, never receives it cut.
    let mut overflow = false;
    let mut newest = 0u64;
    let next_cursor = walk_versions_page_meta(body, true, |entry, meta| {
        newest = newest.max(entry.revision);
        let at = fanout.win_len;
        if overflow || fanout.win_n == WINDOW_FRAMES {
            overflow = true;
            return;
        }
        let Some(n) = build_frame(watch_id, 0, &entry, &mut fanout.win[at..]) else {
            overflow = true;
            return;
        };
        if at + n + 24 > WINDOW_BYTES {
            overflow = true;
            return;
        }
        for (i, v) in meta.unwrap_or([0; 3]).iter().enumerate() {
            fanout.win[at + n + i * 8..at + n + i * 8 + 8].copy_from_slice(&v.to_le_bytes());
        }
        fanout.win_idx[fanout.win_n] = (at as u32, (n + 24) as u16, entry.revision);
        fanout.win_n += 1;
        fanout.win_len = at + n + 24;
    });
    let Some(next_cursor) = next_cursor else {
        // A malformed page abandons the backfill rather than delivering
        // the entries decoded so far — a truncated history is
        // indistinguishable from a complete one once it reaches the
        // client.
        finish_backfill(fanout, false);
        return;
    };
    if overflow {
        let seen = fanout.win_idx[..fanout.win_n]
            .iter()
            .map(|e| e.2)
            .max()
            .unwrap_or(0);
        refuse_compacted(fanout, watch_id, seen.max(newest));
        finish_backfill(fanout, false);
        return;
    }
    if next_cursor == 0 {
        sort_window(fanout);
        plan_prevs(fanout);
        fanout.filling = true;
        fanout.fill_at = 0;
        next_fill(fanout);
    } else {
        fanout.current.cursor = next_cursor;
        issue_backfill_page(fanout);
    }
}

/// A window frame's parts: `(kind, key, value, [create, version, lease])`.
fn frame_parts(f: &[u8]) -> Option<(u8, &[u8], &[u8], [u64; 3])> {
    let kind = *f.get(18)?;
    let kl = u16::from_le_bytes([*f.get(19)?, *f.get(20)?]) as usize;
    let key = f.get(21..21 + kl)?;
    let vo = 21 + kl;
    let vl = u16::from_le_bytes([*f.get(vo)?, *f.get(vo + 1)?]) as usize;
    let value = f.get(vo + 2..vo + 2 + vl)?;
    let m = vo + 2 + vl;
    let rd = |at: usize| -> Option<u64> {
        Some(u64::from_le_bytes(f.get(at..at + 8)?.try_into().ok()?))
    };
    Some((kind, key, value, [rd(m)?, rd(m + 8)?, rd(m + 16)?]))
}

const PREV_NONE: u8 = 0;
const PREV_WINDOW: u8 = 1;
const PREV_STORED: u8 = 2;
const PREV_FETCH: u8 = 3;

/// Where each frame's replaced version comes from: the key's previous
/// frame in the window, nothing (a creation, or a put after a delete),
/// or a point read below the window.
fn plan_prevs(fanout: &mut FanoutState) {
    for i in 0..fanout.win_n {
        let (at, len, _) = fanout.win_idx[i];
        let fi = &fanout.win[at as usize..at as usize + len as usize];
        let Some((kind, key, _, meta)) = frame_parts(fi) else {
            fanout.prev_kind[i] = PREV_NONE;
            continue;
        };
        let mut found = None;
        let mut j = i;
        while j > 0 {
            j -= 1;
            let (bt, bl, _) = fanout.win_idx[j];
            let fj = &fanout.win[bt as usize..bt as usize + bl as usize];
            if frame_parts(fj).is_some_and(|(_, k, _, _)| k == key) {
                found = Some(j);
                break;
            }
        }
        let (k, r) = match found {
            Some(j) => {
                let (bt, bl, _) = fanout.win_idx[j];
                let fj = &fanout.win[bt as usize..bt as usize + bl as usize];
                let deleted = frame_parts(fj).is_some_and(|(kd, _, _, _)| kd == FRAME_OP_DELETE);
                if deleted {
                    (PREV_NONE, 0)
                } else {
                    (PREV_WINDOW, j as u32)
                }
            }
            // A put at version 1 created the key: nothing was replaced.
            None if kind != FRAME_OP_DELETE && meta[1] == 1 => (PREV_NONE, 0),
            None => (PREV_FETCH, 0),
        };
        fanout.prev_kind[i] = k;
        fanout.prev_ref[i] = r;
    }
}

/// Read back the next replaced version below the window, or send the
/// window when none is left.
fn next_fill(fanout: &mut FanoutState) {
    while fanout.fill_at < fanout.win_n && fanout.prev_kind[fanout.fill_at] != PREV_FETCH {
        fanout.fill_at += 1;
    }
    if fanout.fill_at == fanout.win_n {
        send_window(fanout);
        finish_backfill(fanout, true);
        return;
    }
    let (at, len, rev) = fanout.win_idx[fanout.fill_at];
    let mut key = [0u8; PLAN_KEY_MAX + 160];
    let kl = {
        let f = &fanout.win[at as usize..at as usize + len as usize];
        match frame_parts(f) {
            Some((_, k, _, _)) if k.len() <= key.len() => {
                key[..k.len()].copy_from_slice(k);
                k.len()
            }
            _ => {
                finish_backfill(fanout, false);
                return;
            }
        }
    };
    // KV_OP_RANGE_META for the one key as of the revision before:
    // [klen][key][elen=0][rev][limit=1][flags,target,order][4 × bound].
    let mut body = [0u8; 2 + PLAN_KEY_MAX + 160 + 2 + 8 + 4 + 3 + 32];
    let mut n = 0usize;
    body[0..2].copy_from_slice(&(kl as u16).to_le_bytes());
    body[2..2 + kl].copy_from_slice(&key[..kl]);
    n += 2 + kl + 2;
    body[n..n + 8].copy_from_slice(&rev.saturating_sub(1).to_le_bytes());
    n += 8;
    body[n..n + 4].copy_from_slice(&1u32.to_le_bytes());
    n += 4;
    // A large value read as its manifest (the entry then ends with its
    // flags), passed on marked.
    body[n] = types::RANGE_META_FLAG_MANIFESTS;
    n += 3 + 32;
    fanout.corr_seq = fanout.corr_seq.wrapping_add(1);
    let mut env = [0u8; 18 + 2 + PLAN_KEY_MAX + 160 + 2 + 8 + 4 + 3 + 32];
    env[0..8].copy_from_slice(&fanout.corr_seq.to_le_bytes());
    env[8] = PROTO_INTERNAL_WATCH;
    env[15] = KV_OP_RANGE_META;
    env[16..18].copy_from_slice(&(n as u16).to_le_bytes());
    env[18..18 + n].copy_from_slice(&body[..n]);
    let ok = unsafe {
        let sys = fanout.syscalls;
        !sys.is_null() && write_envelope(&*sys, fanout.kv_out, MSG_KV_REQUEST, &env[..18 + n])
    };
    if !ok {
        finish_backfill(fanout, false);
    }
}

/// The version a frame replaced, read back: kept, and the next.
fn fill_answer(fanout: &mut FanoutState, result: u8, body: &[u8]) {
    let i = fanout.fill_at;
    if result == KV_RESULT_COMPACTED {
        let (_, _, rev) = fanout.win_idx[i];
        refuse_compacted(fanout, fanout.current.watch_id, rev);
        finish_backfill(fanout, false);
        return;
    }
    if result != KV_RESULT_KVS || body.len() < 11 {
        finish_backfill(fanout, false);
        return;
    }
    let n = u16::from_le_bytes([body[9], body[10]]);
    fanout.prev_kind[i] = PREV_NONE;
    if n >= 1 {
        // [klen:2][key][vlen:4][value][create][mod][version][lease][flags]
        let kl = u16::from_le_bytes([body[11], body[12]]) as usize;
        let vo = 13 + kl;
        let Some(vl) = body
            .get(vo..vo + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        else {
            finish_backfill(fanout, false);
            return;
        };
        let (Some(value), Some(meta), Some(&flags)) = (
            body.get(vo + 4..vo + 4 + vl),
            body.get(vo + 4 + vl..vo + 4 + vl + 32),
            body.get(vo + 4 + vl + 32),
        ) else {
            finish_backfill(fanout, false);
            return;
        };
        let at = fanout.prev_len;
        if at + 2 + vl + 33 > PREV_BYTES {
            refuse_compacted(fanout, fanout.current.watch_id, fanout.win_idx[i].2);
            finish_backfill(fanout, false);
            return;
        }
        // `[vlen:2][value][meta:32][flags:1]`.
        fanout.prev_buf[at..at + 2].copy_from_slice(&(vl as u16).to_le_bytes());
        fanout.prev_buf[at + 2..at + 2 + vl].copy_from_slice(value);
        fanout.prev_buf[at + 2 + vl..at + 2 + vl + 32].copy_from_slice(meta);
        fanout.prev_buf[at + 2 + vl + 32] = flags;
        fanout.prev_len = at + 2 + vl + 33;
        fanout.prev_kind[i] = PREV_STORED;
        fanout.prev_ref[i] = at as u32;
    }
    fanout.fill_at += 1;
    next_fill(fanout);
}

/// A frame's revision: `[watch_id:8][kpg:2][rev:8]…`.
fn frame_rev(f: &[u8]) -> u64 {
    match f.get(10..18) {
        Some(b) => u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
        None => 0,
    }
}

/// Order the gathered window by revision (stable: a revision's frames
/// keep the order the provider gave them).
fn sort_window(fanout: &mut FanoutState) {
    let n = fanout.win_n;
    for i in 1..n {
        let e = fanout.win_idx[i];
        let mut j = i;
        while j > 0 && fanout.win_idx[j - 1].2 > e.2 {
            fanout.win_idx[j] = fanout.win_idx[j - 1];
            j -= 1;
        }
        fanout.win_idx[j] = e;
    }
}

/// Send the window, each frame with its metadata tail and the version
/// it replaced — the shape a live event has.
fn send_window(fanout: &mut FanoutState) {
    let mut frame = [0u8; 8 + wire::WATCH_EVENT_MAX];
    for i in 0..fanout.win_n {
        let (at, len, _) = fanout.win_idx[i];
        let (at, len) = (at as usize, len as usize);
        let mut n = len;
        frame[..len].copy_from_slice(&fanout.win[at..at + len]);
        // The replaced version: its value and `[create][mod][version][lease]`.
        let mut pv = [0u8; 4096];
        let mut pm = [0u64; 4];
        let mut pl = usize::MAX;
        // The replaced value is a large value's manifest.
        let mut p_manifest = false;
        match fanout.prev_kind[i] {
            PREV_WINDOW => {
                let j = fanout.prev_ref[i] as usize;
                let (bt, bl, brev) = fanout.win_idx[j];
                let fj = &fanout.win[bt as usize..bt as usize + bl as usize];
                if let Some((kind, _, v, m)) = frame_parts(fj) {
                    if v.len() <= pv.len() {
                        pv[..v.len()].copy_from_slice(v);
                        pl = v.len();
                        pm = [m[0], brev, m[1], m[2]];
                        p_manifest = kind == FRAME_OP_PUT_MANIFEST;
                    }
                }
            }
            PREV_STORED => {
                let r = fanout.prev_ref[i] as usize;
                let b = &fanout.prev_buf;
                let vl = u16::from_le_bytes([b[r], b[r + 1]]) as usize;
                if vl <= pv.len() {
                    pv[..vl].copy_from_slice(&b[r + 2..r + 2 + vl]);
                    pl = vl;
                    let m = r + 2 + vl;
                    for (k, x) in pm.iter_mut().enumerate() {
                        let mut e = [0u8; 8];
                        e.copy_from_slice(&b[m + k * 8..m + k * 8 + 8]);
                        *x = u64::from_le_bytes(e);
                    }
                    p_manifest = b[m + 32] & ENTRY_FLAG_MANIFEST != 0;
                }
            }
            _ => {}
        }
        if pl == usize::MAX {
            frame[n] = 0;
            n += 1;
        } else {
            frame[n] = if p_manifest {
                types::JOURNAL_PREV_MANIFEST
            } else {
                types::JOURNAL_PREV
            };
            frame[n + 1..n + 3].copy_from_slice(&(pl as u16).to_le_bytes());
            n += 3;
            frame[n..n + pl].copy_from_slice(&pv[..pl]);
            n += pl;
            for x in pm {
                frame[n..n + 8].copy_from_slice(&x.to_le_bytes());
                n += 8;
            }
        }
        handle_event(fanout, &frame[..n]);
        fanout.m_replayed = fanout.m_replayed.wrapping_add(1);
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<FanoutState>() as u32
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
    if state_size < core::mem::size_of::<FanoutState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let fanout = unsafe { &mut *state.cast::<FanoutState>() };
    fanout.init(sys_ptr);

    // inputs:  events_in[0], replay_plan[1], kv_in[2]
    // outputs: frames_out[0], progress[1], kv_out[2], metrics[3]
    fanout.events_in = in_chan;
    fanout.frames_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        fanout.replay_plan_in = dev_channel_port(sys, 0, 1);
        fanout.kv_in = dev_channel_port(sys, 0, 2);
        fanout.progress_out = dev_channel_port(sys, 1, 1);
        fanout.kv_out = dev_channel_port(sys, 1, 2);
        fanout.metrics_out = dev_channel_port(sys, 1, 3);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let fanout = unsafe { &mut *state.cast::<FanoutState>() };
    unsafe {
        let sys_ptr = fanout.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;
        if let Some((mt, len)) = read_envelope(
            &mut fanout.events_inbox,
            sys,
            fanout.events_in,
            &mut fanout.scratch,
        ) {
            if mt == MSG_WATCH_EVENT {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&fanout.scratch[..len]);
                handle_event(fanout, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(
            &mut fanout.replay_plan_inbox,
            sys,
            fanout.replay_plan_in,
            &mut fanout.scratch,
        ) {
            if mt == MSG_WATCH_REPLAY_PLAN {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&fanout.scratch[..len]);
                handle_replay_plan(fanout, &tmp[..len]);
            }
        }
        if let Some((mt, len)) =
            read_envelope(&mut fanout.kv_inbox, sys, fanout.kv_in, &mut fanout.scratch)
        {
            if mt == MSG_KV_RESPONSE {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&fanout.scratch[..len]);
                handle_kv_response(fanout, &tmp[..len]);
            }
        }

        // Emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        fanout.step_ctr = fanout.step_ctr.wrapping_add(1);
        if fanout.step_ctr.is_multiple_of(5000) && !fanout.syscalls.is_null() {
            telemetry::emit_counters(
                &*fanout.syscalls,
                fanout.metrics_out,
                &[
                    fanout.m_events,
                    fanout.m_delivered,
                    fanout.m_dropped,
                    fanout.m_replayed,
                    fanout.m_replays_unserviceable,
                ],
            );
        }
    }
    0
}
