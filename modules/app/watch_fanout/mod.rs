//! watch_fanout — high-rate event fanout for active watches.
//!
//! Phase 4 module. Consumes `MSG_WATCH_EVENT` envelopes from
//! `watch_registry`, frames them as `MSG_WATCH_FRAME` envelopes
//! the `etcd_edge_anchor` can write to the wire, and reports
//! delivery back to the registry via `progress`.
//!
//! Phase 4 scope: one event-per-frame (no batching) for the
//! common-case single-watch single-event path. Batching arrives
//! when the etcd anchor stitches frames into a `WatchResponse`
//! with multiple events.
//!
//! ## Replay (Phase-2 slice C)
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

use types::{KV_OP_SCAN_VERSIONS, KV_RESULT_COMPACTED, KV_RESULT_VERSIONS, PROTO_INTERNAL_WATCH};
use watch_replay::{
    build_frame, parse_replay_plan, walk_versions_page, ReplayPlan, PLAN_KEY_MAX, SCAN_BODY_MAX,
};
use wire::{
    MSG_KV_REQUEST, MSG_KV_RESPONSE, MSG_WATCH_EVENT, MSG_WATCH_FRAME, MSG_WATCH_REPLAY_PLAN,
};

const SCRATCH_BUF_SIZE: usize = 4096;

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

/// Staging buffer per replayed frame. Bounds the largest event this
/// module can re-emit; an entry that does not fit fails the backfill
/// rather than being skipped, because a skipped event is a hole the
/// client cannot see.
const FRAME_BUF: usize = 512;

#[repr(C)]
struct FanoutState {
    syscalls: *const SyscallTable,

    events_in: i32,
    replay_plan_in: i32,
    kv_in: i32,
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

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=events, 1=delivered, 2=dropped, 3=replayed,
    // 4=replays_unserviceable).
    m_events: u64,
    m_delivered: u64,
    m_dropped: u64,
    m_replayed: u64,
    m_replays_unserviceable: u64,
    step_ctr: u64,

    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl FanoutState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.events_in = -1;
        self.replay_plan_in = -1;
        self.kv_in = -1;
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
        self.m_events = 0;
        self.m_delivered = 0;
        self.m_dropped = 0;
        self.m_replayed = 0;
        self.m_replays_unserviceable = 0;
        self.step_ctr = 0;
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
        } else {
            fanout.m_dropped += 1;
        }
    }
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

    let mut body = [0u8; SCAN_BODY_MAX];
    let Some(n) = plan.scan_body(REPLAY_PAGE_LIMIT, &mut body) else {
        finish_backfill(fanout, false);
        return;
    };

    // MSG_KV_REQUEST payload:
    //   [corr:8][protocol:1][tenant:4][conn:1][consistency:1][op:1]
    //   [body_len:2][body]
    let mut env = [0u8; 18 + SCAN_BODY_MAX];
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
    if result == KV_RESULT_COMPACTED {
        // The window is genuinely gone. Delivering the surviving tail
        // would hand the client a partial history it could not tell
        // from a complete one, so the backfill fails visibly instead.
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

    // Frame first, deliver second. The walker borrows `body`, which
    // lives inside the response buffer, so the frames are staged here
    // and shipped after the walk rather than emitted inside it.
    let mut staged = [[0u8; FRAME_BUF]; REPLAY_PAGE_LIMIT as usize];
    let mut staged_len = [0usize; REPLAY_PAGE_LIMIT as usize];
    let mut staged_n = 0usize;
    let mut overflow = false;
    let next_cursor = walk_versions_page(body, |entry| {
        if staged_n >= staged.len() {
            overflow = true;
            return;
        }
        match build_frame(watch_id, 0, &entry, &mut staged[staged_n]) {
            Some(n) => {
                staged_len[staged_n] = n;
                staged_n += 1;
            }
            // An entry too large to frame cannot be delivered, and
            // skipping it would leave a hole in the history the client
            // could not detect. Fail the backfill instead.
            None => overflow = true,
        }
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
        finish_backfill(fanout, false);
        return;
    }
    for i in 0..staged_n {
        handle_event(fanout, &staged[i][..staged_len[i]]);
        fanout.m_replayed = fanout.m_replayed.wrapping_add(1);
    }

    if next_cursor == 0 {
        finish_backfill(fanout, true);
    } else {
        fanout.current.cursor = next_cursor;
        issue_backfill_page(fanout);
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
        if let Some((mt, len)) = read_envelope(sys, fanout.events_in, &mut fanout.scratch) {
            if mt == MSG_WATCH_EVENT {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&fanout.scratch[..len]);
                handle_event(fanout, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, fanout.replay_plan_in, &mut fanout.scratch) {
            if mt == MSG_WATCH_REPLAY_PLAN {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&fanout.scratch[..len]);
                handle_replay_plan(fanout, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, fanout.kv_in, &mut fanout.scratch) {
            if mt == MSG_KV_RESPONSE {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&fanout.scratch[..len]);
                handle_kv_response(fanout, &tmp[..len]);
            }
        }

        // Phase-14: emit module-scope counters on `metrics_out` at a
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
