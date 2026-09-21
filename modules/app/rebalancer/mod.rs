//! Cluster-wide rebalancer.
//!
//! The placement advisor (`placement_advisor`) sizes ONE range (range 0)
//! and can recommend a split. A cluster also needs the questions a single
//! range cannot answer: is EVERY range within policy, and is load spread
//! evenly across NODES? This module answers both. Each poll it:
//!
//!   1. surveys the key count of every range in the map — a bounded
//!      `RANGE_SCAN` per range on the router's job path;
//!   2. reads each range's leaseholder node straight from the map;
//!   3. runs the per-range recommender (`placement::recommend`) for
//!      split/merge, and the cluster-wide `placement::recommend_relocation`
//!      for node-load balance;
//!   4. REPORTS the advice.
//!
//! Advice-only by default, exactly like the advisor: with `auto_execute
//! = 1` AND `exec_out` wired it emits at most one
//! `MSG_PLACEMENT_RELOCATE_CMD` per survey (split/merge findings stay
//! advice). Everything the recommender decides is host-tested
//! in `modules/common/placement.rs`; this module is only the survey I/O
//! and the reporting around it.

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

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/partition_map.rs"]
mod partition_map;

#[path = "../../common/placement.rs"]
mod placement;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use partition_map::OrderedRangeMap;
use placement::{
    recommend, recommend_relocation, ClusterRecommendation, LoadPolicy, RangeLoad, RangePlacement,
    Recommendation,
};
use types::{KV_OP_RANGE_SCAN, KV_RESULT_RANGE, PROTO_INTERNAL_JOB};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE, MSG_PLACEMENT_RELOCATE_CMD, MSG_PLACEMENT_SPLIT_CMD};

const ENV_BUF: usize = 4096;
/// Records requested per survey page. Only the count matters, but the
/// worker returns the records too, so keep pages small to bound the reply.
const PAGE_LIMIT: u16 = 256;
/// Ranges the survey tracks. The ordered map allows more, but a placement
/// plane hosts a bounded set and the survey state must be fixed-size.
const RB_MAX_RANGES: usize = 16;
const RANGE_MAP_PARAM_MAX: usize = 2048;

const S_IDLE: u8 = 0;
const S_SCAN: u8 = 1;

// A range with no recent lifecycle op: the recommender's cooldown clause
// must not gate a survey that does not track per-range op timestamps yet.
const NO_RECENT_OP_MS: u64 = 10_000_000;

define_params! {
    RebalState;

    // The ordered range map, hex (TLV str chunks append). Same encoding the
    // router and range_supervisor consume.
    1, range_map, str_chunked, 0
        => |s, d, len| {
            let at = s.range_map_hex_len as usize;
            if s.range_map_hex_len == u16::MAX || at + len > RANGE_MAP_PARAM_MAX * 2 {
                s.range_map_hex_len = u16::MAX;
            } else {
                unsafe { for i in 0..len { s.range_map_hex[at + i] = *d.add(i); } }
                s.range_map_hex_len = (at + len) as u16;
            }
        };

    2, min_split_keys, u32, 1024
        => |s, d, len| { s.min_split_keys = p_u32(d, len, 0, 1024); };
    3, max_merge_keys, u32, 512
        => |s, d, len| { s.max_merge_keys = p_u32(d, len, 0, 512); };
    // Node-load skew tolerance (ranges). Clamped up to 2 by the core.
    4, skew, u32, 2
        => |s, d, len| { s.skew = p_u32(d, len, 0, 2); };
    5, poll_ms, u32, 30000
        => |s, d, len| { s.poll_ms = p_u32(d, len, 0, 30000); };
    // Opt-in execution; placement stays operator-gated. 0 = advice-only
    // (the default).
    6, auto_execute, u8, 0
        => |s, d, len| { s.auto_execute = p_u8(d, len, 0, 0); };
}

#[repr(C)]
struct RebalState {
    syscalls: *const SyscallTable,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,
    exec_out: i32,

    corr: u64,
    boot_ms: u64,
    last_poll_ms: u64,
    step_ctr: u64,
    state: u8,

    range_map_hex: [u8; RANGE_MAP_PARAM_MAX * 2],
    range_map_hex_len: u16,
    map: OrderedRangeMap,

    min_split_keys: u32,
    max_merge_keys: u32,
    skew: u32,
    poll_ms: u32,
    auto_execute: u8,

    // Survey progress.
    cur_range: u8,
    n_ranges: u8,
    scan_cursor: u64,
    counts: [u64; RB_MAX_RANGES],

    // Once-per-survey execution latch.
    fired_this_survey: u8,

    env: [u8; ENV_BUF],

    m_surveys: u64,
    m_recommendations: u64,
    m_ranges: u64,
}

impl RebalState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.exec_out = -1;
        self.corr = 0;
        self.boot_ms = 0;
        self.last_poll_ms = 0;
        self.step_ctr = 0;
        self.state = S_IDLE;
        self.range_map_hex = [0; RANGE_MAP_PARAM_MAX * 2];
        self.range_map_hex_len = 0;
        self.map = OrderedRangeMap::new(1);
        self.min_split_keys = 1024;
        self.max_merge_keys = 512;
        self.skew = 2;
        self.poll_ms = 30000;
        self.auto_execute = 0;
        self.cur_range = 0;
        self.n_ranges = 0;
        self.scan_cursor = 0;
        self.counts = [0; RB_MAX_RANGES];
        self.fired_this_survey = 0;
        self.env = [0; ENV_BUF];
        self.m_surveys = 0;
        self.m_recommendations = 0;
        self.m_ranges = 0;
    }

    fn policy(&self) -> LoadPolicy {
        LoadPolicy {
            min_split_keys: u64::from(self.min_split_keys),
            max_merge_keys: u64::from(self.max_merge_keys),
            // Write rate / hot-key are not attributed per range in this
            // survey, so leave the load-based thresholds at defaults; the
            // survey drives split on SIZE (split_on_size = true).
            hot_write_rate: LoadPolicy::DEFAULT.hot_write_rate,
            hot_key_percent: LoadPolicy::DEFAULT.hot_key_percent,
            max_concurrent: 1,
            cooldown_ms: LoadPolicy::DEFAULT.cooldown_ms,
            split_on_size: true,
        }
    }
}

const REQ_HEAD: usize = 18;

/// Send a KV request on the job path: `[corr:8][proto][..pad..][op][blen:2]`
/// head (the placement_advisor's exact framing), body already staged at
/// `env[ENVELOPE_HDR + REQ_HEAD ..]`.
fn kv_send(s: &mut RebalState, op: u8, body_len: usize, next: u8) -> bool {
    s.corr = s.corr.wrapping_add(1).max(1);
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > s.env.len() {
        return false;
    }
    s.env[at..at + 8].copy_from_slice(&s.corr.to_le_bytes());
    s.env[at + 8] = PROTO_INTERNAL_JOB;
    s.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    s.env[at + 13] = 0;
    s.env[at + 14] = 0;
    s.env[at + 15] = op;
    s.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    let sent = unsafe {
        let sys = s.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                s.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut s.env,
            )
    };
    if sent {
        s.state = next;
    }
    sent
}

/// Issue a bounded `RANGE_SCAN` over range `cur_range`'s `[start, end)`
/// bounds. The router routes it to that range's partition, so the reply
/// counts exactly that range's live pairs.
fn send_range_scan(s: &mut RebalState) -> bool {
    let idx = s.cur_range as usize;
    if idx >= s.map.len() {
        return false;
    }
    // Copy bounds out first (borrow of the map ends before we touch env).
    let mut sk = [0u8; partition_map::MAX_KEY_BOUND_LEN];
    let mut ek = [0u8; partition_map::MAX_KEY_BOUND_LEN];
    let (sk_len, ek_len) = {
        let d = &s.map.ranges()[idx];
        let s0 = d.start_key();
        let e0 = d.end_key();
        sk[..s0.len()].copy_from_slice(s0);
        ek[..e0.len()].copy_from_slice(e0);
        (s0.len(), e0.len())
    };
    let body_at = wire::ENVELOPE_HDR + REQ_HEAD;
    let need = 2 + sk_len + 2 + ek_len + 8 + 2;
    if body_at + need > s.env.len() {
        return false;
    }
    let mut n = body_at;
    s.env[n..n + 2].copy_from_slice(&(sk_len as u16).to_le_bytes());
    n += 2;
    s.env[n..n + sk_len].copy_from_slice(&sk[..sk_len]);
    n += sk_len;
    s.env[n..n + 2].copy_from_slice(&(ek_len as u16).to_le_bytes());
    n += 2;
    s.env[n..n + ek_len].copy_from_slice(&ek[..ek_len]);
    n += ek_len;
    s.env[n..n + 8].copy_from_slice(&s.scan_cursor.to_le_bytes());
    n += 8;
    s.env[n..n + 2].copy_from_slice(&PAGE_LIMIT.to_le_bytes());
    kv_send(s, KV_OP_RANGE_SCAN, need, S_SCAN)
}

fn start_survey(s: &mut RebalState) {
    let len = s.map.len().min(RB_MAX_RANGES);
    if len == 0 {
        return;
    }
    s.cur_range = 0;
    s.n_ranges = len as u8;
    s.scan_cursor = 0;
    s.counts = [0; RB_MAX_RANGES];
    s.fired_this_survey = 0;
    let _ = send_range_scan(s);
}

fn on_kv_response(s: &mut RebalState, result: u8, body: &[u8]) {
    if s.state != S_SCAN {
        return;
    }
    if result != KV_RESULT_RANGE || body.len() < 10 {
        // A range that cannot be surveyed is skipped rather than wedging
        // the whole survey; its count stays 0 (reported, not acted on).
        advance_range(s);
        return;
    }
    s.scan_cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let pairs = u16::from_le_bytes([body[8], body[9]]);
    let idx = s.cur_range as usize;
    if idx < RB_MAX_RANGES {
        s.counts[idx] = s.counts[idx].saturating_add(u64::from(pairs));
    }
    if s.scan_cursor != 0 {
        // More pages in this range.
        if !send_range_scan(s) {
            advance_range(s);
        }
        return;
    }
    advance_range(s);
}

/// Move to the next range, or finish the survey when the last range's
/// pages are drained.
fn advance_range(s: &mut RebalState) {
    s.cur_range = s.cur_range.saturating_add(1);
    s.scan_cursor = 0;
    if (s.cur_range as usize) < (s.n_ranges as usize) {
        if !send_range_scan(s) {
            finish_survey(s);
        }
        return;
    }
    finish_survey(s);
}

fn write_dec(slot: &mut [u8], mut v: u64) {
    let mut i = slot.len();
    while i > 0 {
        i -= 1;
        slot[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
}

fn log(s: &RebalState, msg: &[u8]) {
    unsafe {
        if !s.syscalls.is_null() {
            dev_log(&*s.syscalls, 2, msg.as_ptr(), msg.len());
        }
    }
}

fn finish_survey(s: &mut RebalState) {
    let n = s.n_ranges as usize;
    s.m_surveys = s.m_surveys.wrapping_add(1);
    s.m_ranges = n as u64;
    let policy = s.policy();

    // Per-range split/merge advice. Report the observation with the
    // verdict so the recommendation is checkable (an invisible input is
    // an uncheckable recommendation).
    let mut first_actionable: Option<(u8, bool)> = None; // (range_index, is_split)
    for i in 0..n {
        let load = RangeLoad {
            range_index: i as u8,
            key_count: s.counts[i],
            write_rate: 0,
            busiest_key_percent: 0,
            since_last_op_ms: NO_RECENT_OP_MS,
        };
        let neighbour = if i + 1 < n {
            Some(RangeLoad {
                range_index: (i + 1) as u8,
                key_count: s.counts[i + 1],
                write_rate: 0,
                busiest_key_percent: 0,
                since_last_op_ms: NO_RECENT_OP_MS,
            })
        } else {
            None
        };
        let r = recommend(&load, neighbour.as_ref(), &policy, 0);
        let verdict: &[u8] = match r {
            Recommendation::None => b"within-policy",
            Recommendation::Split { .. } => b"RECOMMEND-SPLIT",
            Recommendation::Merge { .. } => b"RECOMMEND-MERGE",
            Recommendation::HotKeyNotSplittable { .. } => b"hot-key-unsplittable",
            Recommendation::Cooldown { .. } => b"cooldown",
            Recommendation::OperationInFlight => b"op-in-flight",
        };
        // "[rbal] range NN keys=NNNNNNN <verdict>"
        let mut m = *b"[rbal] range 00 keys=0000000 ...................";
        write_dec(&mut m[13..15], (i as u64).min(99));
        write_dec(&mut m[21..28], s.counts[i].min(9_999_999));
        let vt = &mut m[29..29 + verdict.len().min(18)];
        vt[..verdict.len().min(18)].copy_from_slice(&verdict[..verdict.len().min(18)]);
        log(s, &m[..29 + verdict.len().min(18)]);

        if first_actionable.is_none() {
            match r {
                Recommendation::Split { range_index } => {
                    first_actionable = Some((range_index, true));
                    s.m_recommendations = s.m_recommendations.wrapping_add(1);
                }
                Recommendation::Merge { range_index } => {
                    first_actionable = Some((range_index, false));
                    s.m_recommendations = s.m_recommendations.wrapping_add(1);
                }
                _ => {}
            }
        }
    }

    // Cluster-wide relocation advice from the map's leaseholders.
    let mut placements = [RangePlacement {
        range_index: 0,
        leaseholder: 0,
    }; RB_MAX_RANGES];
    for (i, slot) in placements.iter_mut().enumerate().take(n) {
        *slot = RangePlacement {
            range_index: i as u8,
            leaseholder: s.map.ranges()[i].leaseholder,
        };
    }
    let cluster = recommend_relocation(&placements[..n], s.skew, 0, 1);
    match cluster {
        ClusterRecommendation::Balanced => {
            log(s, b"[rbal] node load balanced; no relocation");
        }
        ClusterRecommendation::OperationInFlight => {
            log(s, b"[rbal] relocation held: an operation is in flight");
        }
        ClusterRecommendation::Relocate(adv) => {
            s.m_recommendations = s.m_recommendations.wrapping_add(1);
            let mut m = *b"[rbal] RECOMMEND-RELOCATE range 00 node 000->000";
            write_dec(&mut m[32..34], u64::from(adv.range_index).min(99));
            write_dec(&mut m[40..43], u64::from(adv.from_node).min(999));
            write_dec(&mut m[45..48], u64::from(adv.to_node).min(999));
            log(s, &m);
        }
    }

    // Opt-in execution: at most one command per survey, and only a
    // relocation — split/merge findings are reported as advice above.
    if s.auto_execute == 1 && s.exec_out >= 0 && s.fired_this_survey == 0 {
        if let ClusterRecommendation::Relocate(adv) = cluster {
            if emit_relocate(s, adv.range_index, adv.from_node, adv.to_node) {
                s.fired_this_survey = 1;
            }
        }
    }

    s.state = S_IDLE;
}

/// Emit the relocation command frame on `exec_out`:
/// `[partition_id:u16][source_node:u8][target_node:u8]`. The partition id
/// is the moved range's binding partition. An ARMED op_kind-3
/// `range_supervisor` consumes it on `cmd_in` and drives the move.
fn emit_relocate(s: &mut RebalState, range_index: u8, from_node: u32, to_node: u32) -> bool {
    if s.exec_out < 0 {
        return false;
    }
    let idx = range_index as usize;
    if idx >= s.map.len() {
        return false;
    }
    let partition = s.map.ranges()[idx].binding.partition_id;
    let at = wire::ENVELOPE_HDR;
    let payload = 4;
    if at + payload > s.env.len() {
        return false;
    }
    s.env[at..at + 2].copy_from_slice(&partition.to_le_bytes());
    s.env[at + 2] = (from_node & 0xFF) as u8;
    s.env[at + 3] = (to_node & 0xFF) as u8;
    unsafe {
        let sys = s.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                s.exec_out,
                MSG_PLACEMENT_RELOCATE_CMD,
                payload,
                &mut s.env,
            )
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<RebalState>() as u32
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
    if state_size < core::mem::size_of::<RebalState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let s = unsafe { &mut *state.cast::<RebalState>() };
    s.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(s, params, params_len) };
    }
    s.kv_in = in_chan;
    s.kv_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        s.metrics_out = dev_channel_port(sys, 1, 1);
        s.exec_out = dev_channel_port(sys, 1, 2);
    }
    // Decode the map once. Without a valid map there is nothing to survey.
    if !decode_range_map(s) {
        return -1;
    }
    0
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn decode_range_map(s: &mut RebalState) -> bool {
    let hl = s.range_map_hex_len;
    if hl == 0 || hl == u16::MAX || !(hl as usize).is_multiple_of(2) {
        return false;
    }
    let bytes = hl as usize / 2;
    if bytes > RANGE_MAP_PARAM_MAX {
        return false;
    }
    let mut frame = [0u8; RANGE_MAP_PARAM_MAX];
    for (i, out) in frame[..bytes].iter_mut().enumerate() {
        let hi = match hex_nibble(s.range_map_hex[i * 2]) {
            Some(v) => v,
            None => return false,
        };
        let lo = match hex_nibble(s.range_map_hex[i * 2 + 1]) {
            Some(v) => v,
            None => return false,
        };
        *out = (hi << 4) | lo;
    }
    s.map.apply_full_update(&frame[..bytes]).is_some()
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<RebalState>() };
    let sys_ptr = s.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    let now = unsafe { dev_millis(&*sys_ptr) };
    if s.boot_ms == 0 {
        s.boot_ms = now.max(1);
    }

    // Drain one KV response per step (single in-flight corr).
    unsafe {
        let sys = &*sys_ptr;
        let mut env = [0u8; ENV_BUF];
        if let Some((msg, payload)) = read_one_envelope(sys, s.kv_in, &mut env) {
            if msg == MSG_KV_RESPONSE && payload.len() >= 20 {
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                let result = payload[9];
                let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
                if corr == s.corr && payload.len() >= 20 + blen {
                    let mut body = [0u8; ENV_BUF];
                    body[..blen].copy_from_slice(&payload[20..20 + blen]);
                    on_kv_response(s, result, &body[..blen]);
                }
            }
        }
    }

    if s.state == S_IDLE && now.wrapping_sub(s.last_poll_ms) >= u64::from(s.poll_ms) {
        s.last_poll_ms = now;
        start_survey(s);
    }

    s.step_ctr = s.step_ctr.wrapping_add(1);
    if s.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                s.metrics_out,
                &[s.m_surveys, s.m_recommendations, s.m_ranges],
            );
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
