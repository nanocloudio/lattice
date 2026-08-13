//! lattice_apply_bridge — Clustor commit-pipeline ⇄ Lattice KV adapter.
//!
//! Phase 7. Consumes `MSG_COMMITTED_ENTRY` envelopes from
//! `consensus.committed_entries` and re-emits each entry's body
//! as `MSG_KV_COMMAND` on `kv_out`, preserving raft commit-index
//! order. The downstream `kv_state_worker.commands` port is the
//! same channel it already reads from in the single-node graph —
//! the bridge swaps the source from "router" to "raft-committed".
//!
//! Per-entry envelope shape (from Clustor's consensus):
//!   `[term:u64 LE][index:u64 LE][body…]`
//! The body is the verbatim MSG_KV_COMMAND payload the lattice
//! proposer originally submitted (see `kv_request_router`'s
//! Phase 7 path, which would route writes through a the gateway codec
//! proposal envelope instead of straight to the worker).
//!
//! The bridge does NOT re-order, batch, or reshape — it's the
//! 1-to-1 envelope translator that lets the existing KV state
//! machine consume Raft-committed entries unchanged.

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

use wire::{
    LATTICE_ENTRY_TAG, LATTICE_RECORD_TAG, MSG_APP_APPLIED_POS, MSG_KV_COMMAND, MSG_TS_LEASE_GRANT,
};

/// Clustor's `MSG_COMMITTED_ENTRY` opcode (see
/// `deps/clustor/modules/sdk/wire/wire.rs`). Inlined here to avoid
/// build-time coupling to a sibling crate's source path.
const MSG_COMMITTED_ENTRY: u8 = 0x24;

/// `[term:u64 LE][index:u64 LE]` = 16 bytes before the body.
const COMMITTED_HEADER_LEN: usize = 16;

const SCRATCH: usize = 4096;

#[repr(C)]
struct BridgeState {
    syscalls: *const SyscallTable,
    committed_in: i32,
    kv_out: i32,
    metrics_out: i32,
    /// Committed lattice RECORD entries (`LATTICE_RECORD_TAG`) re-emit
    /// here as `[record_msg_type]` envelopes in commit order — the
    /// committed_state feed for `timestamp_allocator`. Unwired (-1) in
    /// graphs without a record
    /// consumer; records are then counted, not silently dropped.
    records_out: i32,
    /// Last raft index we forwarded — drives gap detection. The
    /// consensus streams in strict commit-index order so this
    /// MUST be monotonically increasing; a gap means the upstream
    /// dropped or re-ordered an entry, which we count and surface
    /// to telemetry rather than silently mis-applying.
    last_index: u64,

    /// Term/index of the last entry successfully written to `kv_out`,
    /// and the last position actually published to the worker. These
    /// trail `last_index` whenever a forward is dropped.
    pos_term: u64,
    pos_index: u64,
    published_index: u64,
    /// Per-source forwarded count and gap count. `forwarded` is the
    /// Phase-14 `applied` telemetry counter (one per entry written to
    /// `kv_out`); `gaps` is retained for gap detection.
    forwarded: u64,
    gaps: u64,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=applied [=forwarded], 1=dropped).
    dropped: u64,
    /// Committed entries whose body did not carry `LATTICE_ENTRY_TAG`
    /// and were therefore not applied — clustor-internal entries (admin /
    /// config change) or pre-tag segments being replayed. A non-zero,
    /// growing value under pure KV load means the tag contract broke.
    untagged: u64,
    /// Record entries forwarded on `records_out` (and, for worker
    /// leases, in-band on `kv_out`).
    records: u64,
    /// Record entries that could not be forwarded (port unwired or
    /// backpressure). The position still advances — a record the
    /// consumer misses is re-derived from a later one (allocator
    /// grants supersede each other), never a state-machine gap.
    records_dropped: u64,
    step_ctr: u64,

    scratch: [u8; SCRATCH],
}

impl BridgeState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.committed_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.records_out = -1;
        self.last_index = 0;
        self.pos_term = 0;
        self.pos_index = 0;
        self.published_index = 0;
        self.forwarded = 0;
        self.gaps = 0;
        self.dropped = 0;
        self.untagged = 0;
        self.records = 0;
        self.records_dropped = 0;
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

unsafe fn write_envelope(
    sys: &SyscallTable,
    chan: i32,
    mt: u8,
    body: &[u8],
    scratch: &mut [u8],
) -> bool {
    if chan < 0 || body.len() > u16::MAX as usize {
        return false;
    }
    let total = 3 + body.len();
    if total > scratch.len() {
        return false;
    }
    scratch[0] = mt;
    scratch[1] = (body.len() & 0xFF) as u8;
    scratch[2] = ((body.len() >> 8) & 0xFF) as u8;
    scratch[3..total].copy_from_slice(body);
    (sys.channel_write)(chan, scratch.as_mut_ptr(), total) == total as i32
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<BridgeState>() as u32
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
    if state_size < core::mem::size_of::<BridgeState>() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<BridgeState>() };
    s.init(syscalls.cast::<SyscallTable>());
    s.committed_in = in_chan;
    s.kv_out = out_chan;
    unsafe {
        let sys = &*s.syscalls;
        // metrics on output port index 1; committed_in is port 0
        // input (in_chan), kv_out is output port 0, records_out is
        // output port index 2.
        s.metrics_out = dev_channel_port(sys, 1, 1);
        s.records_out = dev_channel_port(sys, 1, 2);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<BridgeState>() };
    let sys = s.syscalls;
    if sys.is_null() {
        return 0;
    }
    // Drain up to N committed entries per tick; raft's commit
    // horizon can advance by many entries in one tick under load.
    const PER_TICK_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    let mut tmp_body = [0u8; SCRATCH];
    while processed < PER_TICK_BUDGET {
        let mut scratch = [0u8; SCRATCH];
        let (mt, len) = match unsafe { read_envelope(&*sys, s.committed_in, &mut scratch) } {
            Some(p) => p,
            None => break,
        };
        if mt != MSG_COMMITTED_ENTRY || len < COMMITTED_HEADER_LEN {
            processed += 1;
            continue;
        }
        let term = u64::from_le_bytes([
            scratch[0], scratch[1], scratch[2], scratch[3], scratch[4], scratch[5], scratch[6],
            scratch[7],
        ]);
        let index = u64::from_le_bytes([
            scratch[8],
            scratch[9],
            scratch[10],
            scratch[11],
            scratch[12],
            scratch[13],
            scratch[14],
            scratch[15],
        ]);
        // Gap detection — raft commits arrive in strict index order.
        if s.last_index != 0 && index != s.last_index.wrapping_add(1) {
            s.gaps = s.gaps.wrapping_add(1);
        }
        s.last_index = index;

        // Every lattice-replicated entry body is
        // `[LATTICE_ENTRY_TAG][kv_command_payload…]` — the tag exists so
        // the body can never begin with one of clustor's in-band entry
        // markers (see `wire::LATTICE_ENTRY_TAG`). Strip it here.
        //
        // A record entry (`LATTICE_RECORD_TAG`) is a
        // replicated control record, not state-machine input: body
        // after the tag is `[record_msg_type][record…]`. Forward it on
        // `records_out` in commit order; a worker lease
        // (MSG_TS_LEASE_GRANT) ALSO goes in-band on `kv_out` so the
        // state worker adopts it strictly ordered against the commands
        // around it — that ordering is what makes per-write commit
        // timestamps identical on every replica and every replay.
        let tagged_len = len - COMMITTED_HEADER_LEN;
        if tagged_len >= 2 && scratch[COMMITTED_HEADER_LEN] == LATTICE_RECORD_TAG {
            let record_type = scratch[COMMITTED_HEADER_LEN + 1];
            let rec_off = COMMITTED_HEADER_LEN + 2;
            let rec_len = len - rec_off;
            tmp_body[..rec_len].copy_from_slice(&scratch[rec_off..len]);
            let mut out_scratch = [0u8; SCRATCH];
            let mut ok = true;
            if s.records_out >= 0 {
                ok &= unsafe {
                    write_envelope(
                        &*sys,
                        s.records_out,
                        record_type,
                        &tmp_body[..rec_len],
                        &mut out_scratch,
                    )
                };
            } else {
                ok = false;
            }
            if record_type == MSG_TS_LEASE_GRANT {
                ok &= unsafe {
                    write_envelope(
                        &*sys,
                        s.kv_out,
                        record_type,
                        &tmp_body[..rec_len],
                        &mut out_scratch,
                    )
                };
            }
            if ok {
                s.records = s.records.wrapping_add(1);
            } else {
                s.records_dropped = s.records_dropped.wrapping_add(1);
            }
            // The position advances either way: like an internal
            // entry, a record is fully accounted for at the log level
            // the moment it is demuxed, and holding the applied
            // position back would starve the linearizable-read fence
            // (see the untagged arm below). A dropped record strands
            // at most one lease interval; grants supersede.
            s.pos_term = term;
            s.pos_index = index;
            processed += 1;
            continue;
        }

        // An untagged body is NOT ours: it is a clustor-internal entry
        // (admin / config change), or a segment written by a pre-tag
        // build being replayed. Applying it as a KV command would feed
        // the deterministic state machine garbage, so skip and count it
        // rather than guessing.
        if tagged_len < 1 || scratch[COMMITTED_HEADER_LEN] != LATTICE_ENTRY_TAG {
            s.untagged = s.untagged.wrapping_add(1);
            // The position still advances. "Applied through index N" is a
            // claim about the LOG, not about how many entries happened to
            // be ours: a clustor-internal entry is fully accounted for the
            // moment we decline to hand it to the state machine, and
            // nothing downstream will ever have more to say about it.
            //
            // Holding the position back here instead would leave it short
            // by the count of every internal entry ever committed, forever.
            // The linearizable-read fence compares the worker's applied
            // position against a commit index that DOES count those
            // entries, so a permanent shortfall means no fence is ever
            // satisfiable and every linearizable read fails closed at its
            // deadline — which is exactly what it did.
            s.pos_term = term;
            s.pos_index = index;
            processed += 1;
            continue;
        }
        let body_len = tagged_len - 1;
        tmp_body[..body_len].copy_from_slice(&scratch[COMMITTED_HEADER_LEN + 1..len]);

        let mut out_scratch = [0u8; SCRATCH];
        let ok = unsafe {
            write_envelope(
                &*sys,
                s.kv_out,
                MSG_KV_COMMAND,
                &tmp_body[..body_len],
                &mut out_scratch,
            )
        };
        if ok {
            s.forwarded = s.forwarded.wrapping_add(1);
            // Only a command the worker actually received may advance
            // the position it will label snapshots with. A dropped
            // write (backpressure) leaves the position behind, which
            // is what we want: the next batch re-establishes it.
            s.pos_term = term;
            s.pos_index = index;
        } else {
            // Single chokepoint: a committed entry dropped on `kv_out`
            // backpressure (its position stays behind for re-forward).
            s.dropped = s.dropped.wrapping_add(1);
        }
        processed += 1;
    }

    // Publish the applied position for this batch, in-band on `kv_out`
    // so the worker sees it strictly after the commands it covers.
    // See `wire::MSG_APP_APPLIED_POS` for why in-band matters.
    if s.pos_index != 0 && s.pos_index != s.published_index {
        let mut pos = [0u8; 16];
        pos[..8].copy_from_slice(&s.pos_term.to_le_bytes());
        pos[8..].copy_from_slice(&s.pos_index.to_le_bytes());
        let mut out_scratch = [0u8; SCRATCH];
        let ok =
            unsafe { write_envelope(&*sys, s.kv_out, MSG_APP_APPLIED_POS, &pos, &mut out_scratch) };
        if ok {
            s.published_index = s.pos_index;
        }
    }

    s.step_ctr = s.step_ctr.wrapping_add(1);
    if s.step_ctr.is_multiple_of(5000) && !s.syscalls.is_null() {
        unsafe {
            telemetry::emit_counters(
                &*s.syscalls,
                s.metrics_out,
                &[
                    s.forwarded,
                    s.dropped,
                    s.untagged,
                    s.records,
                    s.records_dropped,
                ],
            );
        }
    }
    0
}
