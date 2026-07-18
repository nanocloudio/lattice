//! range_supervisor — the §12 lifecycle pump (RFC database foundation
//! Phase 4).
//!
//! Drives ONE declared lifecycle operation through the state machines
//! in `modules/common/range_lifecycle.rs`. That file owns every
//! transition rule, fence, and map transformation; this module is the
//! thin pump it was written for — its whole job is to turn each
//! machine phase into real KV effects and feed the resulting EVENTS
//! back through `advance_split`.
//!
//! ## What a split physically is, in this composition
//!
//! Static graphs provision partitions at config time, so a split moves
//! a span onto a PRE-PROVISIONED, EMPTY partition. There is no clustor
//! snapshot transfer between groups; the copy runs through the
//! canonical KV path in bounded pages, which keeps exactly one
//! durability story for every byte moved (§14.1's rule, applied to
//! lifecycle). Concretely:
//!
//! 1. The operation record is persisted (`KS_LIFECYCLE_OP`) — always
//!    to partition 0, the metadata home, via `KV_OP_TARGETED` so no
//!    map state or barrier can strand the supervisor's own record.
//!    Every phase advance re-persists the record BEFORE the phase is
//!    acted on: events name committed facts, and a crash resumes from
//!    the record (§12.1 "resumable by operation ID").
//! 2. Bulk copy: `TARGETED RANGE_SCAN` pages the parent's span while
//!    writes continue; each page's pairs are `TARGETED PUT` into the
//!    child. Overwrites are idempotent, so a crash mid-copy re-runs.
//! 3. Cutover barrier (`MSG_SPAN_BARRIER`): the router refuses writes
//!    into the moving span, retryably. Bounded foreground impact: the
//!    window covers only the catch-up pass, not the bulk copy.
//! 4. Catch-up pass: the same copy loop, now against a write-quiesced
//!    span — after it, child and parent agree byte for byte.
//! 5. Publication: `apply_to_map` composes the post-split map (the
//!    contract validates it; a map that would violate §21 invariants
//!    4/5 refuses rather than publishes), `MSG_RANGE_MAP_UPDATE`
//!    replaces the router's map atomically, and the barrier clears.
//! 6. Tombstone: the copied span is deleted out of the parent in
//!    bounded pages. Until it finishes the parent over-retains, which
//!    costs disk, not correctness — reads route by the NEW map.
//!
//! ## Restart truths
//!
//! The router boots from its STATIC `range_map` param — the pre-split
//! map. So on resume at any published phase, the FIRST act is
//! republishing the post-split map (recomputed from the record and the
//! initial map — `apply_to_map` is deterministic). Pre-publication
//! resume re-runs from the bulk copy; the record's phase only ever
//! advances once the effects it names are durable.
//!
//! ## Named refusals
//!
//! - MERGE is driven (`op_kind: 2`): the physical work is the split's
//!   inverse — the source range's span is copied into the SURVIVING
//!   target's partition, quiesced behind the same cutover barrier,
//!   caught up, and the two descriptors are replaced by one. The
//!   §12.2 ordering that matters is preserved by the contract, not by
//!   this pump: ownership of the combined interval flips at
//!   `TargetOwns`, one phase BEFORE the descriptors are published, so
//!   routing lags the truth rather than leading it.
//! - Relocation: still not driven. It changes replicas, not key
//!   bounds, and a single-node static composition has nowhere to
//!   relocate TO — so `op_kind: 3` refuses at init rather than
//!   pretending. Named, not hidden.
//! - One operation at a time, like the executor: the record slot is
//!   singular by design.

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

// range_lifecycle mounts partition_map itself; one mount chain, one
// set of types (the same rule sql_exec follows for sql_core).
#[path = "../../common/range_lifecycle.rs"]
mod range_lifecycle;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use range_lifecycle::partition_map::{
    self, encode_range_map_update, OrderedRangeMap, SpanBarrier, MSG_RANGE_MAP_UPDATE,
    MSG_SPAN_BARRIER,
};
use range_lifecycle::{
    advance_merge, advance_split, apply_to_map, LifecycleOperation, MergeEvent, MergePhase, Phase,
    SplitEvent, SplitPhase, TargetPlacement,
};
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_OP_TARGETED, KV_RESULT_NOT_FOUND,
    KV_RESULT_OK, KV_RESULT_RANGE, PROTO_INTERNAL_LIFECYCLE,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Capacities ────────────────────────────────────────────────────────

/// Keyspace of lifecycle operation records (§11.4 metadata maps).
const KS_LIFECYCLE_OP: u32 = 0x8000_0008;

/// Copy-page size: pairs per RANGE_SCAN request. Small enough that a
/// page of worst-case rows fits every hop's 4 KiB scratch.
const COPY_PAGE_LIMIT: u16 = 16;

/// Staged page buffer (the RANGE_SCAN response body).
/// Staged page. MUST be at least the envelope's payload capacity: a
/// scan reply can fill it, and a page copied SHORT cuts its last
/// record's value in half — which then lands as a value that cannot
/// be decoded, far from the truncation that caused it.
const PAGE_BUF: usize = 4096 + 64;

/// Outbound envelope scratch.
const ENV_BUF: usize = 4096;

/// Ticks to wait after posting a barrier/map frame before relying on
/// it: the router drains `map_update` every step, so 4 ticks is a
/// generous ordering margin for an in-process channel.
const SETTLE_TICKS: u32 = 4;

/// Hex-string params arrive TLV-chunked; cap the accumulated forms.
const SPLIT_KEY_MAX: usize = partition_map::MAX_KEY_BOUND_LEN;
const RANGE_MAP_PARAM_MAX: usize = 2048;

// ── Module driver states ─────────────────────────────────────────────

const S_IDLE: u8 = 0; // no operation declared, or finished
const S_WAIT_DELAY: u8 = 1; // manual-gate delay before starting
const S_LOAD: u8 = 2; // GET the op record (resume check)
const S_PERSIST: u8 = 3; // PUT the record; on ack, act on new phase
const S_COPY_SCAN: u8 = 4; // RANGE_SCAN page in flight
const S_COPY_PUT: u8 = 5; // PUT of one staged pair in flight
const S_SETTLE: u8 = 6; // waiting out SETTLE_TICKS after a frame
const S_TOMB_SCAN: u8 = 7; // tombstone: scan page in flight
const S_TOMB_DEL: u8 = 8; // tombstone: delete in flight
const S_FAULT: u8 = 9; // refused config / stuck; logged once

define_params! {
    SupState;

    // 0 = idle (module inert), 1 = split. Merge (2) / relocate (3)
    // are contract-complete but not pumped yet: declaring them REFUSES
    // at init rather than half-running.
    1, op_kind, u8, 0
        => |s, d, len| { s.op_kind = p_u8(d, len, 0, 0); };

    // Split key, hex (user-key bytes). TLV str chunks append.
    2, split_key, str, 0
        => |s, d, len| {
            let at = s.split_key_hex_len as usize;
            if at + len <= SPLIT_KEY_MAX * 2 {
                unsafe { for i in 0..len { s.split_key_hex[at + i] = *d.add(i); } }
                s.split_key_hex_len = (at + len) as u16;
            } else { s.split_key_hex_len = u16::MAX; }
        };

    // Partition the right child lands on (pre-provisioned, empty).
    3, target_partition, u16, 1
        => |s, d, len| { s.target_partition = p_u16(d, len, 0, 1); };

    // Manual execution gate (§12.4): the operator declared the op in
    // the config; it starts this long after boot. Gives a live test —
    // and a cautious operator — a window to verify the graph first.
    4, start_delay_ms, u32, 2000
        => |s, d, len| { s.start_delay_ms = p_u32(d, len, 0, 2000); };

    // The INITIAL ordered range map (same hex frame the router's
    // `range_map` param carries). Post-split maps are computed from
    // it + the operation record, so both modules agree on generation
    // and epoch without a side channel.
    5, range_map, str, 0
        => |s, d, len| {
            let at = s.range_map_hex_len as usize;
            if at + len <= RANGE_MAP_PARAM_MAX * 2 {
                unsafe { for i in 0..len { s.range_map_hex[at + i] = *d.add(i); } }
                s.range_map_hex_len = (at + len) as u16;
            } else { s.range_map_hex_len = u16::MAX; }
        };
}

#[repr(C)]
struct SupState {
    syscalls: *const SyscallTable,
    kv_in: i32,
    kv_out: i32,
    map_out: i32,
    metrics_out: i32,

    op_kind: u8,
    split_key_hex: [u8; SPLIT_KEY_MAX * 2],
    split_key_hex_len: u16,
    target_partition: u16,
    start_delay_ms: u32,
    range_map_hex: [u8; RANGE_MAP_PARAM_MAX * 2],
    range_map_hex_len: u16,

    split_key: [u8; SPLIT_KEY_MAX],
    split_key_len: u16,
    /// The initial (pre-operation) map, decoded from params.
    initial_map: OrderedRangeMap,
    /// The in-flight (or resumed) operation record.
    op: LifecycleOperation,
    op_live: u8,

    state: u8,
    /// Where a completed PERSIST / SETTLE goes next (SplitPhase byte).
    resume_phase: u8,
    settle_left: u32,
    boot_ms: u64,
    started: u8,

    corr: u64,
    /// Copy loop: current scan cursor and span.
    scan_cursor: u64,
    /// Staged page: `[klen u16][key][vlen u32][value]` pairs.
    page: [u8; PAGE_BUF],
    page_len: u16,
    page_at: u16,
    page_pairs: u16,
    /// True while the copy loop is the post-barrier catch-up pass.
    catchup: u8,
    /// The op record has been probed at least once this boot.
    loaded_once: u8,
    /// The cutover barrier is believed raised in the router.
    barrier_up: u8,
    /// Parent (source) partition, from the initial map.
    source_partition: u16,

    env: [u8; ENV_BUF],

    m_phase: u64,
    m_copied: u64,
    m_deleted: u64,
    m_errors: u64,
    step_ctr: u32,
    logged_fault: u8,
}

impl SupState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.kv_in = -1;
        self.kv_out = -1;
        self.map_out = -1;
        self.metrics_out = -1;
        self.op_kind = 0;
        self.split_key_hex = [0; SPLIT_KEY_MAX * 2];
        self.split_key_hex_len = 0;
        self.target_partition = 1;
        self.start_delay_ms = 2000;
        self.range_map_hex = [0; RANGE_MAP_PARAM_MAX * 2];
        self.range_map_hex_len = 0;
        self.split_key = [0; SPLIT_KEY_MAX];
        self.split_key_len = 0;
        self.initial_map = OrderedRangeMap::new(1);
        self.op = LifecycleOperation::EMPTY;
        self.op_live = 0;
        self.state = S_IDLE;
        self.resume_phase = 0;
        self.settle_left = 0;
        self.boot_ms = 0;
        self.started = 0;
        self.corr = 0;
        self.scan_cursor = 0;
        self.page = [0; PAGE_BUF];
        self.page_len = 0;
        self.page_at = 0;
        self.page_pairs = 0;
        self.catchup = 0;
        self.loaded_once = 0;
        self.barrier_up = 0;
        self.source_partition = 0;
        self.env = [0; ENV_BUF];
        self.m_phase = 0;
        self.m_copied = 0;
        self.m_deleted = 0;
        self.m_errors = 0;
        self.step_ctr = 0;
        self.logged_fault = 0;
    }
}

// ── Hex decode ───────────────────────────────────────────────────────

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn hex_decode(src: &[u8], out: &mut [u8]) -> Option<usize> {
    if !src.len().is_multiple_of(2) || src.len() / 2 > out.len() {
        return None;
    }
    for i in 0..src.len() / 2 {
        out[i] = (hex_nibble(src[i * 2])? << 4) | hex_nibble(src[i * 2 + 1])?;
    }
    Some(src.len() / 2)
}

// ── KV request plumbing ──────────────────────────────────────────────

/// Offset in `env` where a KV request body is staged.
const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

/// Send one TARGETED KV request: `[partition][inner_op][inner_body…]`
/// staged at `BODY_AT`. Returns false on a full channel (retried next
/// step by re-entering the same state).
fn kv_send_targeted(sup: &mut SupState, partition: u16, inner_op: u8, inner_len: usize) -> bool {
    sup.corr = sup.corr.wrapping_add(1).max(1);
    let corr = sup.corr;
    const REQ_HEAD: usize = 18;
    let body_len = 3 + inner_len;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > sup.env.len() {
        return false;
    }
    // The TARGETED wrapper sits BEFORE the staged inner body, so the
    // stage helpers write at BODY_AT + 3.
    sup.env[at..at + 8].copy_from_slice(&corr.to_le_bytes());
    sup.env[at + 8] = PROTO_INTERNAL_LIFECYCLE;
    sup.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    sup.env[at + 13] = 0; // conn slot
    sup.env[at + 14] = 0; // default consistency
    sup.env[at + 15] = KV_OP_TARGETED;
    sup.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    sup.env[BODY_AT..BODY_AT + 2].copy_from_slice(&partition.to_le_bytes());
    sup.env[BODY_AT + 2] = inner_op;
    unsafe {
        let sys = sup.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                sup.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut sup.env,
            )
    }
}

/// Inner-body staging area (after the 3-byte TARGETED wrapper).
const INNER_AT: usize = BODY_AT + 3;

fn op_record_key(op_id: &[u8; 16], out: &mut [u8]) -> usize {
    out[0..4].copy_from_slice(&KS_LIFECYCLE_OP.to_be_bytes());
    out[4..20].copy_from_slice(op_id);
    20
}

/// Stage + send: GET the op record from partition 0.
fn send_load_record(sup: &mut SupState) -> bool {
    let mut key = [0u8; 20];
    let id = derived_op_id(sup);
    let kn = op_record_key(&id, &mut key);
    let need = 2 + kn;
    if INNER_AT + need > sup.env.len() {
        return false;
    }
    sup.env[INNER_AT..INNER_AT + 2].copy_from_slice(&(kn as u16).to_le_bytes());
    sup.env[INNER_AT + 2..INNER_AT + 2 + kn].copy_from_slice(&key[..kn]);
    kv_send_targeted(sup, 0, KV_OP_GET, need)
}

/// Stage + send: PUT the op record (phase already set on `sup.op`).
fn send_persist_record(sup: &mut SupState) -> bool {
    let mut key = [0u8; 20];
    let kn = op_record_key(&sup.op.operation_id.clone(), &mut key);
    let mut rec = [0u8; range_lifecycle::LIFECYCLE_OP_MAX_WIRE_LEN];
    let Some(rn) = sup.op.encode(&mut rec) else {
        return false;
    };
    let need = 2 + kn + 4 + rn + 1 + 8;
    if INNER_AT + need > sup.env.len() {
        return false;
    }
    let mut n = INNER_AT;
    sup.env[n..n + 2].copy_from_slice(&(kn as u16).to_le_bytes());
    n += 2;
    sup.env[n..n + kn].copy_from_slice(&key[..kn]);
    n += kn;
    sup.env[n..n + 4].copy_from_slice(&(rn as u32).to_le_bytes());
    n += 4;
    sup.env[n..n + rn].copy_from_slice(&rec[..rn]);
    n += rn;
    sup.env[n] = 0; // plain PUT: the record advances in place
    n += 1;
    sup.env[n..n + 8].copy_from_slice(&0u64.to_le_bytes());
    n += 8;
    kv_send_targeted(sup, 0, KV_OP_PUT, n - INNER_AT)
}

/// Stage + send: one RANGE_SCAN page of the moving span against the
/// parent partition.
fn send_copy_scan(sup: &mut SupState) -> bool {
    let sk_len = move_span_start_len(sup);
    let need = 2 + sk_len + 2 + 10;
    if INNER_AT + need > sup.env.len() {
        return false;
    }
    let mut n = INNER_AT;
    let split_key = move_span_start_bytes(sup);
    let cursor: u64 = sup.scan_cursor;
    let part: u16 = sup.source_partition;
    sup.env[n..n + 2].copy_from_slice(&(sk_len as u16).to_le_bytes());
    n += 2;
    sup.env[n..n + sk_len].copy_from_slice(&split_key[..sk_len]);
    n += sk_len;
    // Parent end bound = MAX within the parent range: the span moving
    // to the child is [split_key, parent.end). The parent is the range
    // containing the split key in the INITIAL map.
    let end_len = parent_end_len(sup);
    sup.env[n..n + 2].copy_from_slice(&(end_len as u16).to_le_bytes());
    n += 2;
    if end_len > 0 {
        let mut e = [0u8; SPLIT_KEY_MAX];
        e[..end_len].copy_from_slice(&parent_end_bytes(sup)[..end_len]);
        sup.env[n..n + end_len].copy_from_slice(&e[..end_len]);
        n += end_len;
    }
    sup.env[n..n + 8].copy_from_slice(&cursor.to_le_bytes());
    n += 8;
    sup.env[n..n + 2].copy_from_slice(&COPY_PAGE_LIMIT.to_le_bytes());
    n += 2;
    kv_send_targeted(sup, part, KV_OP_RANGE_SCAN, n - INNER_AT)
}

fn parent_range_index(sup: &SupState) -> Option<usize> {
    let key = &sup.split_key[..sup.split_key_len as usize];
    let (first, _) = sup.initial_map.covering(key, &[])?;
    Some(first)
}

/// The upper bound of the span this operation moves. It MUST resolve
/// through the same range index the start does — a split's parent, a
/// merge's source — or the copy and the tombstone disagree about the
/// span and the tombstone deletes what the copy never moved.
fn move_span_index(sup: &SupState) -> Option<usize> {
    if sup.op_kind == 2 {
        merge_source_index(sup)
    } else {
        parent_range_index(sup)
    }
}

fn parent_end_len(sup: &SupState) -> usize {
    move_span_index(sup)
        .map(|i| sup.initial_map.ranges()[i].end_key().len())
        .unwrap_or(0)
}

/// The SOURCE range of a merge. The declared key is the BOUNDARY being
/// removed, and a boundary is the start of the right-hand range — so
/// the range that disappears is the one BEFORE it. Reading the key as
/// the containing range would merge the wrong pair (or, at the last
/// range, refuse a perfectly legal merge).
fn merge_source_index(sup: &SupState) -> Option<usize> {
    let idx = parent_range_index(sup)?;
    let ranges = sup.initial_map.ranges();
    let key = &sup.split_key[..sup.split_key_len as usize];
    if idx > 0 && ranges[idx].start_key() == key {
        Some(idx - 1)
    } else {
        Some(idx)
    }
}

/// The span this operation MOVES. A split moves the tail of its
/// parent, `[split_key, parent.end)`. A merge moves the whole source
/// range, `[source.start, source.end)` — the source's start, not the
/// declared boundary, or the head of the range would be left behind.
fn move_span_start_len(sup: &SupState) -> usize {
    if sup.op_kind == 2 {
        merge_source_index(sup)
            .map(|i| sup.initial_map.ranges()[i].start_key().len())
            .unwrap_or(0)
    } else {
        sup.split_key_len as usize
    }
}

fn move_span_start_bytes(sup: &SupState) -> [u8; SPLIT_KEY_MAX] {
    let mut out = [0u8; SPLIT_KEY_MAX];
    if sup.op_kind == 2 {
        if let Some(i) = merge_source_index(sup) {
            let s = sup.initial_map.ranges()[i].start_key();
            out[..s.len()].copy_from_slice(s);
        }
    } else {
        let n = sup.split_key_len as usize;
        out[..n].copy_from_slice(&sup.split_key[..n]);
    }
    out
}

fn parent_end_bytes(sup: &SupState) -> [u8; SPLIT_KEY_MAX] {
    let mut out = [0u8; SPLIT_KEY_MAX];
    if let Some(i) = move_span_index(sup) {
        let e = sup.initial_map.ranges()[i].end_key();
        out[..e.len()].copy_from_slice(e);
    }
    out
}

/// Stage + send: PUT (copy) or DELETE (tombstone) for the pair at
/// `page_at`. Returns None on a malformed page (fault), Some(sent).
fn send_pair(sup: &mut SupState, delete: bool) -> Option<bool> {
    let at = sup.page_at as usize;
    let page_len = sup.page_len as usize;
    if at + 6 > page_len {
        return None;
    }
    let klen = u16::from_le_bytes([sup.page[at], sup.page[at + 1]]) as usize;
    let koff = at + 2;
    if koff + klen + 4 > page_len {
        return None;
    }
    let vlen = u32::from_le_bytes([
        sup.page[koff + klen],
        sup.page[koff + klen + 1],
        sup.page[koff + klen + 2],
        sup.page[koff + klen + 3],
    ]) as usize;
    let voff = koff + klen + 4;
    if voff + vlen > page_len {
        return None;
    }

    // Instrumentation over inference: report what each copied pair
    // ACTUALLY measures, so a wrong value length names itself instead
    // of being deduced from a decode failure three hops later.
    unsafe {
        if !sup.syscalls.is_null() && !delete {
            let mut msg = *b"[rsup] copy k=0000 v=00000";
            let k = klen.min(9999) as u32;
            let v = vlen.min(99999) as u32;
            msg[14] = b'0' + ((k / 1000) % 10) as u8;
            msg[15] = b'0' + ((k / 100) % 10) as u8;
            msg[16] = b'0' + ((k / 10) % 10) as u8;
            msg[17] = b'0' + (k % 10) as u8;
            msg[21] = b'0' + ((v / 10000) % 10) as u8;
            msg[22] = b'0' + ((v / 1000) % 10) as u8;
            msg[23] = b'0' + ((v / 100) % 10) as u8;
            msg[24] = b'0' + ((v / 10) % 10) as u8;
            msg[25] = b'0' + (v % 10) as u8;
            dev_log(&*sup.syscalls, 2, msg.as_ptr(), msg.len());
        }
    }
    let mut n = INNER_AT;
    let sent = if delete {
        let need = 2 + 2 + klen;
        if INNER_AT + need > sup.env.len() {
            return Some(false);
        }
        sup.env[n..n + 2].copy_from_slice(&1u16.to_le_bytes());
        n += 2;
        sup.env[n..n + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        n += 2;
        for i in 0..klen {
            sup.env[n + i] = sup.page[koff + i];
        }
        n += klen;
        kv_send_targeted(sup, sup.source_partition, KV_OP_DELETE, n - INNER_AT)
    } else {
        let need = 2 + klen + 4 + vlen + 1 + 8;
        if INNER_AT + need > sup.env.len() {
            return Some(false);
        }
        sup.env[n..n + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        n += 2;
        for i in 0..klen {
            sup.env[n + i] = sup.page[koff + i];
        }
        n += klen;
        sup.env[n..n + 4].copy_from_slice(&(vlen as u32).to_le_bytes());
        n += 4;
        for i in 0..vlen {
            sup.env[n + i] = sup.page[voff + i];
        }
        n += vlen;
        sup.env[n] = 0; // plain PUT: idempotent overwrite
        n += 1;
        sup.env[n..n + 8].copy_from_slice(&0u64.to_le_bytes());
        n += 8;
        kv_send_targeted(sup, sup.target_partition, KV_OP_PUT, n - INNER_AT)
    };
    // Advance past this pair only once the send succeeded; a full
    // channel retries the same pair next step.
    if sent {
        sup.page_at = (voff + vlen) as u16;
        sup.page_pairs = sup.page_pairs.saturating_sub(1);
    }
    Some(sent)
}

// ── Map / barrier frames ─────────────────────────────────────────────

fn send_barrier(sup: &mut SupState, set: bool) -> bool {
    let sk_len = move_span_start_len(sup);
    let end_len = parent_end_len(sup);
    let end = parent_end_bytes(sup);
    let key = move_span_start_bytes(sup);
    let b = SpanBarrier {
        set,
        start: &key[..sk_len],
        end: &end[..end_len],
    };
    let mut frame = [0u8; 3 + 4 + 2 * SPLIT_KEY_MAX];
    let Some(fl) = b.encode(&mut frame) else {
        return false;
    };
    let at = wire::ENVELOPE_HDR;
    if at + fl > sup.env.len() {
        return false;
    }
    sup.env[at..at + fl].copy_from_slice(&frame[..fl]);
    unsafe {
        let sys = sup.syscalls;
        !sys.is_null() && write_envelope(&*sys, sup.map_out, MSG_SPAN_BARRIER, fl, &mut sup.env)
    }
}

/// Publish the INITIAL (pre-operation) map — the correct live map for
/// every pre-publication phase, and the release for a router running
/// `await_map: 1`.
fn send_initial_map(sup: &mut SupState) -> bool {
    let mut frame = [0u8; RANGE_MAP_PARAM_MAX];
    let Some(fl) = encode_range_map_update(sup.initial_map.ranges(), &mut frame) else {
        return false;
    };
    let at = wire::ENVELOPE_HDR;
    if at + fl > sup.env.len() {
        return false;
    }
    sup.env[at..at + fl].copy_from_slice(&frame[..fl]);
    unsafe {
        let sys = sup.syscalls;
        !sys.is_null() && write_envelope(&*sys, sup.map_out, MSG_RANGE_MAP_UPDATE, fl, &mut sup.env)
    }
}

/// Publish the post-operation map: apply_to_map at the published
/// phase, encoded as one RANGE_MAP_UPDATE frame.
fn send_published_map(sup: &mut SupState) -> bool {
    // Kind-aware: apply_to_map refuses a phase from the other machine,
    // and a hard-coded split phase here stalls a merge SILENTLY at
    // TargetOwns — no state change, no log, no fault.
    let publish_at = if sup.op_kind == 2 {
        Phase::Merge(MergePhase::DescriptorsReplaced)
    } else {
        Phase::Split(SplitPhase::DescriptorsPublished)
    };
    let Ok(map) = apply_to_map(&sup.initial_map, &sup.op, publish_at) else {
        return false;
    };
    let mut frame = [0u8; RANGE_MAP_PARAM_MAX];
    let Some(fl) = encode_range_map_update(map.ranges(), &mut frame) else {
        return false;
    };
    let at = wire::ENVELOPE_HDR;
    if at + fl > sup.env.len() {
        return false;
    }
    sup.env[at..at + fl].copy_from_slice(&frame[..fl]);
    unsafe {
        let sys = sup.syscalls;
        !sys.is_null() && write_envelope(&*sys, sup.map_out, MSG_RANGE_MAP_UPDATE, fl, &mut sup.env)
    }
}

// ── Operation identity / construction ────────────────────────────────

/// Deterministic operation id from the declared split: FNV-1a over the
/// split key, twice with different seeds, so a restart derives the
/// SAME id and resumes the SAME record (§12.1 "resumable by operation
/// ID" without a coordination service to mint ids).
fn derived_op_id(sup: &SupState) -> [u8; 16] {
    let key = &sup.split_key[..sup.split_key_len as usize];
    let mut out = [0u8; 16];
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    out[0..8].copy_from_slice(&h.to_le_bytes());
    let mut h2: u64 = h ^ 0x9E37_79B9_7F4A_7C15;
    for &b in key {
        h2 ^= b as u64;
        h2 = h2.wrapping_mul(0x100000001b3);
    }
    out[8..16].copy_from_slice(&h2.to_le_bytes());
    if out == [0u8; 16] {
        out[0] = 1;
    }
    out
}

/// Child identities: parent-id-derived, distinct, never equal to the
/// parent (flip disjoint bytes deterministically).
fn child_ids(parent: &[u8; 16]) -> ([u8; 16], [u8; 16]) {
    let mut left = *parent;
    let mut right = *parent;
    left[0] ^= 0x55;
    left[15] ^= 0x01;
    right[0] ^= 0xAA;
    right[15] ^= 0x02;
    (left, right)
}

/// Build the fresh Validated record from params + the initial map.
///
/// Split: the range containing the split key becomes two. Merge: that
/// same range is the SOURCE, and its right-hand neighbour is the
/// surviving TARGET — so the declared "split key" names the boundary
/// being removed, which is the same declaration read the other way.
fn build_operation(sup: &SupState) -> Option<LifecycleOperation> {
    if sup.op_kind == 2 {
        return build_merge_operation(sup);
    }
    let idx = parent_range_index(sup)?;
    let parent = &sup.initial_map.ranges()[idx];
    let (left_id, right_id) = child_ids(&parent.range_id);
    let targets = [
        TargetPlacement {
            range_id: left_id,
            partition_id: parent.binding.partition_id,
            partition_incarnation: parent.binding.partition_incarnation,
        },
        TargetPlacement {
            range_id: right_id,
            partition_id: sup.target_partition,
            partition_incarnation: 1,
        },
    ];
    LifecycleOperation::new(
        derived_op_id(sup),
        Phase::Split(SplitPhase::Validated),
        &[parent.range_id],
        &targets,
        &sup.split_key[..sup.split_key_len as usize],
        0,
        parent.generation,
        parent.generation.checked_add(1)?,
        parent.binding.placement_epoch,
        parent.binding.partition_incarnation,
        0,
    )
}

/// §12.2: source ‖ target, adjacent, target survives.
fn build_merge_operation(sup: &SupState) -> Option<LifecycleOperation> {
    let idx = merge_source_index(sup)?;
    let ranges = sup.initial_map.ranges();
    if idx + 1 >= ranges.len() {
        return None;
    }
    let source = &ranges[idx];
    let target = &ranges[idx + 1];
    let next_gen = source.generation.max(target.generation).checked_add(1)?;
    LifecycleOperation::new(
        derived_op_id(sup),
        Phase::Merge(MergePhase::Validated),
        &[source.range_id],
        &[TargetPlacement {
            range_id: target.range_id,
            partition_id: target.binding.partition_id,
            partition_incarnation: target.binding.partition_incarnation,
        }],
        &[],
        0,
        source.generation,
        next_gen,
        source.binding.placement_epoch,
        source.binding.partition_incarnation,
        0,
    )
}

// ── The pump ─────────────────────────────────────────────────────────

fn fault(sup: &mut SupState, what: &[u8]) {
    sup.state = S_FAULT;
    sup.m_errors = sup.m_errors.wrapping_add(1);
    if sup.logged_fault == 0 {
        sup.logged_fault = 1;
        unsafe {
            if !sup.syscalls.is_null() {
                dev_log(&*sup.syscalls, 3, what.as_ptr(), what.len());
            }
        }
    }
}

/// Persist the record at `phase`, then continue from it when the PUT
/// acks (handled in `on_kv_response`).
fn persist_phase(sup: &mut SupState, phase: SplitPhase) {
    if sup.op.set_phase(Phase::Split(phase)).is_err() {
        fault(sup, b"[rsup] phase kind mismatch");
        return;
    }
    sup.m_phase = phase as u8 as u64;
    // §24: every lifecycle transition is operator-visible. One line
    // per advance; the phase byte is the SplitPhase discriminant.
    unsafe {
        if !sup.syscalls.is_null() {
            let mut msg = *b"[rsup] split phase -> 0";
            let last = msg.len() - 1;
            msg[last] = b'0' + (phase as u8);
            dev_log(&*sup.syscalls, 2, msg.as_ptr(), msg.len());
        }
    }
    if send_persist_record(sup) {
        sup.state = S_PERSIST;
        sup.resume_phase = phase as u8;
    }
    // else: full channel; module_step retries via act_on_phase re-entry
}

/// Persist the record at a MERGE phase, then continue on the ack.
fn persist_merge_phase(sup: &mut SupState, phase: MergePhase) {
    if sup.op.set_phase(Phase::Merge(phase)).is_err() {
        fault(sup, b"[rsup] phase kind mismatch");
        return;
    }
    sup.m_phase = phase as u8 as u64;
    unsafe {
        if !sup.syscalls.is_null() {
            let mut msg = *b"[rsup] merge phase -> 0";
            let last = msg.len() - 1;
            msg[last] = b'0' + (phase as u8);
            dev_log(&*sup.syscalls, 2, msg.as_ptr(), msg.len());
        }
    }
    if send_persist_record(sup) {
        sup.state = S_PERSIST;
        sup.resume_phase = 0x80 | (phase as u8);
    }
}

/// The MERGE arm of `act_on_phase`. The physical work mirrors the
/// split's, because it IS the split's inverse: copy the source span
/// into the surviving target's partition, quiesce it behind the same
/// cutover barrier, catch up, publish the combined descriptor, then
/// tombstone the source's copy.
fn act_on_merge_phase(sup: &mut SupState, phase: MergePhase) {
    match phase {
        MergePhase::Validated => {
            let Ok(next) = advance_merge(&sup.op, MergeEvent::MergeCommitted) else {
                fault(sup, b"[rsup] illegal merge advance from Validated");
                return;
            };
            persist_merge_phase(sup, next);
        }
        MergePhase::Committed => {
            // §12.2 step 2: quiesce the source's writes behind the
            // barrier BEFORE any transfer, so the copy reads a span
            // nothing is still writing to.
            if send_barrier(sup, true) {
                sup.barrier_up = 1;
                sup.state = S_SETTLE;
                sup.settle_left = SETTLE_TICKS;
                sup.resume_phase = 0x80 | (MergePhase::Quiesced as u8);
            }
        }
        MergePhase::Quiesced => {
            // Transfer the source span into the target partition.
            sup.catchup = 0;
            sup.scan_cursor = 0;
            if send_copy_scan(sup) {
                sup.state = S_COPY_SCAN;
            }
        }
        MergePhase::Transferred => {
            let Ok(next) = advance_merge(&sup.op, MergeEvent::TargetOwnershipCommitted) else {
                fault(sup, b"[rsup] illegal merge advance from Transferred");
                return;
            };
            persist_merge_phase(sup, next);
        }
        MergePhase::TargetOwns => {
            // Publication: two descriptors become one.
            if send_published_map(sup) {
                sup.state = S_SETTLE;
                sup.settle_left = SETTLE_TICKS;
                sup.resume_phase = 0x80 | (MergePhase::DescriptorsReplaced as u8);
            }
        }
        MergePhase::DescriptorsReplaced => {
            // Tombstone the source's now-duplicated copy.
            sup.scan_cursor = 0;
            if send_copy_scan(sup) {
                sup.state = S_TOMB_SCAN;
            }
        }
        MergePhase::SourceTombstoned => {
            sup.state = S_IDLE;
            sup.op_live = 0;
        }
    }
}

/// Take the next physical action demanded by the record's phase.
fn act_on_phase(sup: &mut SupState) {
    if let Phase::Merge(p) = sup.op.phase() {
        act_on_merge_phase(sup, p);
        return;
    }
    let phase = match sup.op.phase() {
        Phase::Split(p) => p,
        _ => {
            fault(sup, b"[rsup] non-split op resumed");
            return;
        }
    };
    match phase {
        SplitPhase::Validated => {
            // The record is durable: that IS §12.1 step 2's committed
            // prepare in this composition.
            let Ok(next) = advance_split(&sup.op, SplitEvent::PrepareCommitted) else {
                fault(sup, b"[rsup] illegal advance from Validated");
                return;
            };
            persist_phase(sup, next);
        }
        SplitPhase::Prepared => {
            // Copy-based bootstrap has no snapshot apply-index; the
            // boundary is "the copy starts now" (documented in the
            // module header as a divergence from clustor snapshots).
            let Ok(next) = advance_split(&sup.op, SplitEvent::BoundaryEstablished) else {
                fault(sup, b"[rsup] illegal advance from Prepared");
                return;
            };
            persist_phase(sup, next);
        }
        SplitPhase::BoundaryEstablished => {
            // Bulk copy.
            sup.catchup = 0;
            sup.scan_cursor = 0;
            if send_copy_scan(sup) {
                sup.state = S_COPY_SCAN;
            }
        }
        SplitPhase::ChildrenBootstrapped => {
            // Raise the barrier, settle, then advance.
            if send_barrier(sup, true) {
                sup.barrier_up = 1;
                sup.state = S_SETTLE;
                sup.settle_left = SETTLE_TICKS;
                sup.resume_phase = SplitPhase::CutoverBarrier as u8;
            }
        }
        SplitPhase::CutoverBarrier => {
            // Catch-up pass under the barrier. On resume after a crash
            // the barrier is re-raised first (router state is
            // volatile) — `resume_entry` handles that.
            sup.catchup = 1;
            sup.scan_cursor = 0;
            if send_copy_scan(sup) {
                sup.state = S_COPY_SCAN;
            }
        }
        SplitPhase::Finalized => {
            // Publish the post-split map, then clear the barrier.
            if send_published_map(sup) {
                sup.state = S_SETTLE;
                sup.settle_left = SETTLE_TICKS;
                sup.resume_phase = SplitPhase::DescriptorsPublished as u8;
            }
        }
        SplitPhase::DescriptorsPublished => {
            // Tombstone: delete the moved span out of the parent.
            sup.scan_cursor = 0;
            if send_copy_scan(sup) {
                sup.state = S_TOMB_SCAN;
            }
        }
        SplitPhase::TombstoneRetained => {
            sup.state = S_IDLE;
            sup.op_live = 0;
        }
    }
}

/// Copy/tombstone loop: a page landed; drive pairs, then the next
/// page, then the phase advance.
fn on_page_done(sup: &mut SupState) {
    if let Phase::Merge(p) = sup.op.phase() {
        if sup.scan_cursor != 0 {
            let tombstoning = sup.state == S_TOMB_DEL || sup.state == S_TOMB_SCAN;
            if send_copy_scan(sup) {
                sup.state = if tombstoning {
                    S_TOMB_SCAN
                } else {
                    S_COPY_SCAN
                };
            }
            return;
        }
        let ev = match p {
            MergePhase::Quiesced => MergeEvent::StateTransferred,
            MergePhase::DescriptorsReplaced => MergeEvent::SourceTombstoned,
            _ => {
                fault(sup, b"[rsup] merge page in an unexpected phase");
                return;
            }
        };
        let Ok(next) = advance_merge(&sup.op, ev) else {
            fault(sup, b"[rsup] illegal merge advance at page end");
            return;
        };
        persist_merge_phase(sup, next);
        return;
    }
    if sup.scan_cursor != 0 {
        let ok = send_copy_scan(sup);
        if ok {
            sup.state = if sup.state == S_TOMB_DEL || sup.state == S_TOMB_SCAN {
                S_TOMB_SCAN
            } else {
                S_COPY_SCAN
            };
        }
        return;
    }
    // Span exhausted: advance the machine.
    let tombstoning = sup.state == S_TOMB_DEL || sup.state == S_TOMB_SCAN;
    if tombstoning {
        let Ok(next) = advance_split(&sup.op, SplitEvent::TombstoneFloorsSatisfied) else {
            fault(sup, b"[rsup] illegal advance at tombstone");
            return;
        };
        persist_phase(sup, next);
    } else if sup.catchup == 0 {
        let Ok(next) = advance_split(&sup.op, SplitEvent::ChildSnapshotsInstalled) else {
            fault(sup, b"[rsup] illegal advance at bootstrap");
            return;
        };
        persist_phase(sup, next);
    } else {
        // Catch-up complete under the barrier: parent and child agree
        // on the span, which is this composition's SplitFinalize.
        let Ok(next) = advance_split(&sup.op, SplitEvent::FinalizeCommitted) else {
            fault(sup, b"[rsup] illegal advance at catch-up");
            return;
        };
        persist_phase(sup, next);
    }
}

/// One KV response arrived for our single in-flight corr.
fn on_kv_response(sup: &mut SupState, result: u8, body: &[u8]) {
    match sup.state {
        S_LOAD => {
            if result == KV_RESULT_OK {
                // Record exists (GET returns raw value bytes): decode
                // and resume.
                let Some(op) = range_lifecycle::LifecycleOperation::decode(body) else {
                    fault(sup, b"[rsup] op record corrupt");
                    return;
                };
                sup.op = op;
                sup.op_live = 1;
                resume_entry(sup);
            } else if result == KV_RESULT_NOT_FOUND && sup.loaded_once == 2 {
                // Second probe, after the gate delay: genuinely fresh.
                let Some(op) = build_operation(sup) else {
                    fault(sup, b"[rsup] split declaration invalid");
                    return;
                };
                sup.op = op;
                sup.op_live = 1;
                // Kind-aware: a merge record refuses a split phase,
                // and silently faulting there would look like the
                // supervisor never started at all.
                if sup.op_kind == 2 {
                    persist_merge_phase(sup, MergePhase::Validated);
                } else {
                    persist_phase(sup, SplitPhase::Validated);
                }
            } else if result == KV_RESULT_NOT_FOUND {
                // No record at boot: nothing ever ran (or the WAL tail
                // is still replaying — the second probe settles it),
                // so the static boot map IS the live map — confirm it
                // (releases `await_map`), then wait out the manual
                // gate before deciding to create.
                if send_initial_map(sup) {
                    sup.state = S_SETTLE;
                    sup.settle_left = SETTLE_TICKS;
                    sup.resume_phase = 0;
                } else {
                    sup.state = S_WAIT_DELAY;
                }
            } else {
                // Store not ready yet (replay, etc.) — retry.
                sup.state = S_WAIT_DELAY;
            }
        }
        S_PERSIST => {
            if result == KV_RESULT_OK {
                act_on_phase(sup);
            } else {
                sup.m_errors = sup.m_errors.wrapping_add(1);
                // Retry the persist next step.
                if send_persist_record(sup) {
                    sup.state = S_PERSIST;
                }
            }
        }
        S_COPY_SCAN | S_TOMB_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                sup.m_errors = sup.m_errors.wrapping_add(1);
                // Retry the page.
                let _ = send_copy_scan(sup);
                return;
            }
            sup.scan_cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let pairs = u16::from_le_bytes([body[8], body[9]]);
            let take = body.len() - 10;
            if take > PAGE_BUF {
                // Fail closed. Copying a truncated page would write a
                // half-record and report success — the worst outcome
                // available here.
                fault(sup, b"[rsup] scan page exceeds the staging buffer");
                return;
            }
            sup.page[..take].copy_from_slice(&body[10..10 + take]);
            sup.page_len = take as u16;
            sup.page_at = 0;
            sup.page_pairs = pairs;
            if pairs == 0 {
                on_page_done(sup);
            } else {
                let delete = sup.state == S_TOMB_SCAN;
                match send_pair(sup, delete) {
                    None => fault(sup, b"[rsup] scan page malformed"),
                    Some(true) => {
                        sup.state = if delete { S_TOMB_DEL } else { S_COPY_PUT };
                    }
                    Some(false) => { /* full channel: retried in step */ }
                }
            }
        }
        S_COPY_PUT | S_TOMB_DEL => {
            if result != KV_RESULT_OK && result != KV_RESULT_NOT_FOUND {
                sup.m_errors = sup.m_errors.wrapping_add(1);
            } else if sup.state == S_COPY_PUT {
                sup.m_copied = sup.m_copied.wrapping_add(1);
            } else {
                sup.m_deleted = sup.m_deleted.wrapping_add(1);
            }
            if sup.page_pairs == 0 {
                on_page_done(sup);
            } else {
                let delete = sup.state == S_TOMB_DEL;
                if send_pair(sup, delete).is_none() {
                    fault(sup, b"[rsup] scan page malformed");
                }
            }
        }
        _ => {}
    }
}

/// Resume entry from a loaded record: re-establish the volatile router
/// state the phase assumes, then act.
fn resume_entry(sup: &mut SupState) {
    if let Phase::Merge(p) = sup.op.phase() {
        sup.m_phase = p as u8 as u64;
        let sent = if p.is_published() {
            send_published_map(sup)
        } else {
            send_initial_map(sup)
        };
        if sent {
            sup.state = S_SETTLE;
            sup.settle_left = SETTLE_TICKS;
            sup.resume_phase = 0x80 | (p as u8);
        }
        return;
    }
    let phase = match sup.op.phase() {
        Phase::Split(p) => p,
        _ => {
            fault(sup, b"[rsup] non-split op resumed");
            return;
        }
    };
    sup.m_phase = phase as u8 as u64;
    // The router boots from its static param map and, under
    // `await_map: 1`, quarantines client traffic until WE say which
    // map is live. Published phases republish the post-split map;
    // pre-publication phases confirm the initial map. Either way the
    // settle window orders the map ahead of everything that follows.
    let sent = if phase.is_published() {
        send_published_map(sup)
    } else {
        send_initial_map(sup)
    };
    if sent {
        sup.state = S_SETTLE;
        sup.settle_left = SETTLE_TICKS;
        sup.resume_phase = phase as u8;
    }
}

/// A settle window elapsed: continue from `resume_phase`.
fn on_settled(sup: &mut SupState) {
    if sup.resume_phase & 0x80 != 0 {
        // Merge continuation: the settled frame was a barrier or a map
        // publication; commit the fact it established, or re-enter.
        let want = sup.resume_phase & 0x7F;
        let cur = match sup.op.phase() {
            Phase::Merge(p) => p,
            _ => {
                fault(sup, b"[rsup] merge settle on a non-merge op");
                return;
            }
        };
        if want == MergePhase::Quiesced as u8 && cur == MergePhase::Committed {
            let Ok(next) = advance_merge(&sup.op, MergeEvent::WritesQuiesced) else {
                fault(sup, b"[rsup] illegal advance to quiesced");
                return;
            };
            persist_merge_phase(sup, next);
        } else if want == MergePhase::DescriptorsReplaced as u8 && cur == MergePhase::TargetOwns {
            if send_barrier(sup, false) {
                sup.barrier_up = 0;
            }
            let Ok(next) = advance_merge(&sup.op, MergeEvent::DescriptorsReplaced) else {
                fault(sup, b"[rsup] illegal advance to replaced");
                return;
            };
            persist_merge_phase(sup, next);
        } else {
            act_on_phase(sup);
        }
        return;
    }
    if sup.resume_phase == 0 {
        // Fresh boot, no record: the initial map is confirmed live;
        // now wait out the manual-gate delay before creating the op.
        sup.state = S_WAIT_DELAY;
        return;
    }
    let Some(phase) = SplitPhase::from_u8(sup.resume_phase) else {
        fault(sup, b"[rsup] bad resume phase");
        return;
    };
    match phase {
        SplitPhase::CutoverBarrier => {
            if sup.op.phase() == Phase::Split(SplitPhase::ChildrenBootstrapped) {
                // Barrier is up; commit that fact.
                let Ok(next) = advance_split(&sup.op, SplitEvent::CutoverBarrierCommitted) else {
                    fault(sup, b"[rsup] illegal advance to barrier");
                    return;
                };
                persist_phase(sup, next);
            } else if sup.barrier_up == 0 {
                // Resume path: the barrier is router-volatile, so
                // re-raise it (behind its own settle) before the
                // catch-up pass trusts it.
                if send_barrier(sup, true) {
                    sup.barrier_up = 1;
                    sup.state = S_SETTLE;
                    sup.settle_left = SETTLE_TICKS;
                }
            } else {
                act_on_phase(sup);
            }
        }
        SplitPhase::DescriptorsPublished => {
            if sup.op.phase() == Phase::Split(SplitPhase::Finalized) {
                // Map published; clear the barrier, then commit.
                if send_barrier(sup, false) {
                    sup.barrier_up = 0;
                }
                let Ok(next) = advance_split(&sup.op, SplitEvent::DescriptorsPublished) else {
                    fault(sup, b"[rsup] illegal advance to published");
                    return;
                };
                persist_phase(sup, next);
            } else {
                act_on_phase(sup);
            }
        }
        other => {
            // Republish-at-resume path for published phases.
            let _ = other;
            act_on_phase(sup);
        }
    }
}

// ── Module ABI ───────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<SupState>() as u32
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
    if state_size < core::mem::size_of::<SupState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let sup = unsafe { &mut *state.cast::<SupState>() };
    sup.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(sup, params, params_len) };
    }
    sup.kv_in = in_chan;
    sup.kv_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        sup.map_out = dev_channel_port(sys, 1, 1);
        sup.metrics_out = dev_channel_port(sys, 1, 2);
    }

    // Decode declarations. An inert supervisor (op_kind 0) is legal —
    // wire it into a graph before you need it.
    if sup.op_kind == 0 {
        sup.state = S_IDLE;
        return 0;
    }
    if sup.op_kind != 1 && sup.op_kind != 2 {
        // Relocation: contract-complete, no target on a single-node
        // static composition (see the module docs).
        return -1;
    }
    let (hex_len, map_hex_len) = (sup.split_key_hex_len, sup.range_map_hex_len);
    if hex_len == 0 || hex_len == u16::MAX || map_hex_len == 0 || map_hex_len == u16::MAX {
        return -1;
    }
    let mut key = [0u8; SPLIT_KEY_MAX];
    let mut key_hex = [0u8; SPLIT_KEY_MAX * 2];
    key_hex[..hex_len as usize].copy_from_slice(&sup.split_key_hex[..hex_len as usize]);
    let Some(kn) = hex_decode(&key_hex[..hex_len as usize], &mut key) else {
        return -1;
    };
    sup.split_key[..kn].copy_from_slice(&key[..kn]);
    sup.split_key_len = kn as u16;

    let mut frame = [0u8; RANGE_MAP_PARAM_MAX];
    let mut frame_hex = [0u8; RANGE_MAP_PARAM_MAX * 2];
    frame_hex[..map_hex_len as usize].copy_from_slice(&sup.range_map_hex[..map_hex_len as usize]);
    let Some(fl) = hex_decode(&frame_hex[..map_hex_len as usize], &mut frame) else {
        return -1;
    };
    if sup.initial_map.apply_full_update(&frame[..fl]).is_none() {
        return -1;
    }
    // The partition the copy READS FROM must be the one holding the
    // moving span — a split's parent, a merge's SOURCE. Resolving it
    // through the containing range instead made the merge scan the
    // TARGET partition for a span that lives in the source, find
    // nothing, copy nothing, and advance through every phase reporting
    // success. Zero copied records is what the instrumentation showed;
    // no amount of reasoning about span bounds would have.
    let Some(idx) = move_span_index(sup) else {
        return -1;
    };
    sup.source_partition = sup.initial_map.ranges()[idx].binding.partition_id;
    // The declaration must FORM a valid operation now, not at first
    // use (validate_split runs inside apply_to_map on every phase, but
    // a config that can never publish should refuse boot).
    if build_operation(sup).is_none() {
        return -1;
    }
    sup.state = S_WAIT_DELAY;
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let sup = unsafe { &mut *state.cast::<SupState>() };
    let sys_ptr = sup.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    let now = unsafe { dev_millis(&*sys_ptr) };
    if sup.boot_ms == 0 {
        sup.boot_ms = now.max(1);
    }

    // Drain one KV response per step (single in-flight corr).
    unsafe {
        let sys = &*sys_ptr;
        let mut env = [0u8; ENV_BUF];
        if let Some((msg, payload)) = read_one_envelope(sys, sup.kv_in, &mut env) {
            // MSG_KV_RESPONSE head: [corr:8][conn:1][result:1][rev:8][blen:2]
            if msg == MSG_KV_RESPONSE && payload.len() >= 20 {
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                let result = payload[9];
                let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
                if corr == sup.corr && payload.len() >= 20 + blen {
                    let mut body = [0u8; ENV_BUF];
                    body[..blen].copy_from_slice(&payload[20..20 + blen]);
                    on_kv_response(sup, result, &body[..blen]);
                }
            }
        }
    }

    match sup.state {
        S_WAIT_DELAY => {
            if sup.op_kind == 0 {
            } else if sup.loaded_once == 0 {
                // Probe the record IMMEDIATELY at boot: a resumed
                // operation must republish its map before anything
                // else serves, and the manual-gate delay applies only
                // to CREATING a new operation.
                if send_load_record(sup) {
                    sup.loaded_once = 1;
                    sup.state = S_LOAD;
                }
            } else if now.wrapping_sub(sup.boot_ms) >= sup.start_delay_ms as u64 {
                // Probe AGAIN before creating: the boot-time probe can
                // race the WAL tail replay and read a false absence —
                // creating a duplicate operation off that would re-run
                // a split that already published. The gate delay has
                // now also given replay time to finish.
                if send_load_record(sup) {
                    sup.loaded_once = 2;
                    sup.state = S_LOAD;
                }
            }
        }
        S_SETTLE => {
            if sup.settle_left > 0 {
                sup.settle_left -= 1;
            } else {
                on_settled(sup);
            }
        }
        _ => {}
    }

    sup.step_ctr = sup.step_ctr.wrapping_add(1);
    if sup.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                sup.metrics_out,
                &[sup.m_phase, sup.m_copied, sup.m_deleted, sup.m_errors],
            );
        }
    }
    0
}

/// Stamp the 3-byte envelope header over a payload already staged at
/// `env[ENVELOPE_HDR..]` and write it. Mirrors the executor's helper.
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

/// Read one envelope from `chan`. Mirrors the router's helper.
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
