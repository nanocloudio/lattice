//! kv_state_worker — the deterministic KV state machine host (memory
//! or ordered-disk state-store provider).
//!
//! Thin wrapper around `modules/common/kv_store.rs`: this mod.rs
//! owns only the channel-I/O surface (drain MSG_KV_COMMAND, dispatch
//! to the single semantic engine `kv_store::apply_mat`, emit
//! MSG_KV_APPLIED). All real KV logic (storage layout, revisions, CAS
//! witnesses, op apply functions) lives in `kv_store.rs` so it
//! compiles into both this no_std PIC module and the host-side
//! integration tests at `tests/integration_kv.rs` /
//! `tests/integration_kv_disk.rs`.
//!
//! ## State-store provider selection (RFC database foundation §6, §30)
//!
//! The `state_store` param selects the physical materialization
//! provider at graph construction — never a different engine:
//!
//! - `0` (memory, default): the bounded in-memory `KvStore` table.
//!   Exactly the historical behaviour.
//! - `1` (disk): the ordered disk provider (`kv_store::disk_store`)
//!   over the Fluxor FS contract (`modules/common/fs_run_storage.rs`).
//!   The module recovers the on-disk materialization before serving
//!   (E_AGAIN-patient while the FS provider initialises, quarantined —
//!   never serving — on a hard storage fault), applies committed
//!   mutations through the provider's batch contract, and drives
//!   flush/compaction at step boundaries.
//!
//! ### Disk-mode step-cost bounds
//!
//! No step blocks unboundedly; the two heavyweight actions are
//! explicitly bounded and scheduled one-per-step where possible:
//!
//! - **Flush** (`DiskStore::flush`, driven from `disk_maintenance` or
//!   in-line when a command hits memtable `Backpressure`): writes the
//!   whole memtable — at most `MEMTABLE_MAX_ENTRIES` (512) records ≈
//!   2.4 MB in ~600 bounded `FS_WRITE` appends plus one fsync and one
//!   manifest publish. Worst-case step time is therefore one memtable
//!   drain (single-digit ms on the linux provider, tens of ms on
//!   FAT32-class media). The advisory watermark (75%) keeps the
//!   common flush at ≤ 384 records.
//! - **Compaction** runs `COMPACT_STEP_RECORDS` (128) input records
//!   per step via `disk_maintenance`; only when a command would
//!   otherwise fail (memtable AND run set full) is it driven to
//!   completion in-line, bounded by the provider's fixed logical
//!   capacity (≤ 8704 records ⇒ ≤ 68 steps).
//!
//! ## Scope
//!
//! The same op coverage documented in `kv_store.rs`. Real Raft
//! integration arrives via `lattice_apply_bridge` feeding `commands`.

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

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/kv_store.rs"]
mod kv_store;

#[path = "../../common/fd_cache.rs"]
mod fd_cache;

#[path = "../../common/fs_run_storage.rs"]
mod fs_run_storage;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use fs_run_storage::FsRunStorage;
use kv_store::disk_store::state_store::{
    KvStateStore, Progress, SnapshotChunk, SnapshotRequest, StoreError,
};
use kv_store::disk_store::{DiskState, DiskStore};
use kv_store::{DiskMaterializer, KvStore, Materializer};
use types::{
    KV_OP_APPEND, KV_OP_CAS, KV_OP_DECR, KV_OP_DELETE, KV_OP_EXISTS, KV_OP_FLUSH, KV_OP_GET,
    KV_OP_GET_AT, KV_OP_INCR, KV_OP_MGET, KV_OP_MSET, KV_OP_PREPEND, KV_OP_PUT, KV_OP_RANGE,
    KV_OP_SCAN, KV_OP_SCAN_AT, KV_OP_STRLEN, KV_OP_TXN, KV_OP_TXN_PREPARE, KV_OP_TXN_RECORD,
    KV_OP_TXN_RESOLVE, KV_RESULT_INTEGER, KV_RESULT_OK,
};
use wire::{
    APP_SNAPSHOT_HDR, MSG_APP_APPLIED_POS, MSG_APP_SNAPSHOT_CHUNK, MSG_APP_SNAPSHOT_REQUEST,
    MSG_APP_SNAPSHOT_RESET, MSG_GC_FLOOR_COMMITTED, MSG_KV_APPLIED, MSG_KV_COMMAND,
    MSG_PLACEMENT_EPOCH_EVENT, MSG_RETENTION_CLAIM, MSG_WATCH_EVENT,
};

#[path = "../../common/compaction_floor.rs"]
mod compaction_floor;
use compaction_floor::{
    GcFloorRecord, RetentionClaim, CLAIM_SOURCE_ACTIVE_READ, GC_CLAIM_WIRE_LEN, GC_FLOOR_WIRE_LEN,
};

const SCRATCH_BUF_SIZE: usize = 8192;

// ── state_store provider selection (see module docs) ──────────────────

const STATE_STORE_MEMORY: u8 = 0;
const STATE_STORE_DISK: u8 = 1;

/// Disk-provider lifecycle. The worker never serves commands from a
/// disk store it has not successfully recovered — fail closed.
const DISK_PHASE_RECOVERING: u8 = 0;
const DISK_PHASE_SERVING: u8 = 1;
const DISK_PHASE_QUARANTINED: u8 = 2;

/// Consecutive HARD (non-`E_AGAIN`) flush/compact faults tolerated
/// before the store quarantines.
///
/// A failed flush or compaction changes NOTHING durable — the manifest
/// publish is the last step of both, so on any earlier fault the
/// previously published state is still exactly what recovery would
/// find, and the memtable still holds the unflushed records. Retrying
/// is therefore free of correctness risk, while quarantining on the
/// first fault is permanent: nothing ever leaves `QUARANTINED`. A
/// device-level hiccup (an NVMe status code surfaced as EIO, a poll
/// budget expiring under WAL contention) must not cost the node its
/// whole store. Recovery validation is NOT covered by this budget:
/// `recover()` failing hard still quarantines immediately, because
/// there the fault means the on-disk state did not validate.
const DISK_HARD_FAULT_BUDGET: u32 = 64;

/// Consecutive steps an exclusion fence may hold off the write path
/// **without any storage progress** before the worker force-releases it.
///
/// "Without progress" is load-bearing, not decoration. A legitimate
/// compaction holds `compact.active` continuously across every one of
/// its steps, and it is bounded per step (`COMPACT_STEP_RECORDS = 128`),
/// so merging a full run set of compaction-sized runs can legitimately
/// run to four figures of steps. A purely elapsed budget would abort
/// those healthy compactions, the worker would restart them from
/// scratch, and the run set would never drain — a livelock strictly
/// worse than the wedge being fixed. So every successful storage action
/// (recover, compact step, flush) resets the counter, and only a fence
/// that is held while NOTHING advances can trip it.
///
/// **The invariant: no snapshot, install or compaction activity may
/// block the write path for more than a bounded number of steps.**
///
/// v1's provider excludes concurrent apply from a capture/install/
/// compaction by refusing `apply`/`flush` with `Backpressure`. That is
/// a safe simplification only while every fence is guaranteed to be
/// released, and one is not: an install opened by `MSG_APP_SNAPSHOT_
/// RESET` whose chunk stream is then abandoned — a sequence gap, a
/// leader that stops sending, a peer that dies — leaves nothing that
/// would ever clear it. `apply` and `flush` then refuse every write
/// forever, the memtable pins at the flush watermark, and the node is
/// wedged with no fault and no phase change.
///
/// So the fence gets a deadline. A snapshot, an install and a
/// compaction are all retryable; serving writes is not optional. When
/// those conflict the fence yields — see `DiskStore::abort_fences`.
///
/// 2000 steps is ~2 s at `tick_us: 1000`: far longer than any healthy
/// capture (which completes within a single step) or bounded
/// compaction step sequence, and far shorter than a human notices.
const DISK_FENCE_STALL_BUDGET: u32 = 2000;

/// Disk-store observability cadence, in MILLISECONDS of wall clock.
///
/// This is deliberately not a step count. A step cadence measures the
/// wrong thing: it goes silent exactly when a step stops completing,
/// which is the failure mode you most need to see. A 5000-step cadence
/// looks like ~5 s at `tick_us: 1000` only while steps are cheap — if a
/// single step blocks for seconds inside the FS provider, 5000 steps is
/// hours away and the module appears to have nothing to say. Wall clock
/// keeps reporting at a fixed rate no matter how slow the steps get.
const DISK_OBSERVE_INTERVAL_MS: u64 = 1000;

/// Heartbeat log cadence (ms). Slower than the metric emit — this line
/// is unconditional in disk mode, so an ABSENT heartbeat proves the log
/// channel or the module step itself is dead, and a FROZEN one proves
/// the module is blocked mid-step. Both are otherwise indistinguishable
/// from a healthy idle worker.
const DISK_HEARTBEAT_LOG_MS: u64 = 2000;

/// Log level for disk-store diagnostics. Level 2 matches the existing
/// `[kvw] disk recover ok` line — the bare-metal debug config is known
/// to carry level 2 to the UDP drain.
const DISK_LOG_LEVEL: u8 = 2;

define_params! {
    WorkerState;

    // Physical state-store provider, selected at graph construction
    // (RFC database foundation §6/§30). 0 = bounded in-memory table
    // (default — the historical behaviour, no FS use at all); 1 = the
    // ordered disk provider over the Fluxor FS contract (requires the
    // `fs` resource claim in the manifest and an FS provider in the
    // graph).
    1, state_store, u8, 0, enum { memory=0, disk=1 }
        => |s, d, len| { s.state_store = p_u8(d, len, 0, 0); };

    // Disk-provider file layout (mirrors durability's `root_path`):
    // 0 = files under `kv/` in the process working directory (linux);
    // 1 = 8.3 names at the FS root (bare-metal FAT32).
    2, root_path, u8, 0
        => |s, d, len| { s.root_path = p_u8(d, len, 0, 0); };

    // What this worker publishes as its §18 ACTIVE-READ retention
    // claim — the oldest revision it still needs retained.
    //
    // 0 = `pin` (DEFAULT): claim revision 0, i.e. "retain everything".
    //     GC then never advances for this range. This is the correct
    //     default until the substrate can tell us which application
    //     snapshot is DURABLE: WAL replay after a restart re-runs the
    //     committed stream from the newest durable snapshot and the
    //     disk provider serves those replayed commands as-of reads
    //     (see `kv_store::DiskMaterializer`), so a floor above the
    //     replay start would make replay itself read `Compacted`. Over-
    //     retention costs disk; under-retention costs recovery.
    // 1 = `snapshot`: claim the engine revision at the last snapshot
    //     this worker exported. Tighter, and safe only in a composition
    //     where an exported snapshot is known to be retained.
    3, gc_claim_mode, u8, 0, enum { pin=0, snapshot=1 }
        => |s, d, len| { s.gc_claim_mode = p_u8(d, len, 0, 0); };

    // KPG id stamped on the retention claim this worker publishes.
    // Which disk store this worker instance owns. Multi-range graphs
    // (RFC §11, Phase 3) run one worker per range in one process; each
    // needs its own file namespace: 0 = the historical `kv/`,
    // 1..=9 = `kv<id>/`. Refused at init on the FAT32 root layout
    // (root_path = 1) with a non-zero id — two stores at the FS root
    // would silently share file names.
    5, store_id, u8, 0
        => |s, d, len| { s.store_id = p_u8(d, len, 0, 0); };

    4, gc_claim_kpg, u16, 0
        => |s, d, len| { s.gc_claim_kpg = p_u16(d, len, 0, 0); };
}

/// Steps between retention-claim republications. The claim carries a
/// freshness bound, so it must be refreshed faster than that bound
/// expires or the coordinator will (correctly) treat this worker as a
/// stale source and refuse to advance the floor.
const GC_CLAIM_INTERVAL_STEPS: u64 = 1000;

/// Freshness bound stamped on published claims, in ms. Generous
/// relative to the republication cadence: a claim going stale must mean
/// the worker actually stopped, not that a step ran late.
const GC_CLAIM_TTL_MS: u64 = 30_000;

const GC_CLAIM_MODE_PIN: u8 = 0;
const GC_CLAIM_MODE_SNAPSHOT: u8 = 1;

/// Max body bytes per emitted snapshot chunk. Matches clustor's
/// `durability::MAX_CHUNK_BODY` so a chunk always fits the
/// engine's staging buffer.
const SNAPSHOT_CHUNK_MAX: usize = 4096;

#[repr(C)]
struct WorkerState {
    syscalls: *const SyscallTable,

    commands_in: i32,
    read_permit_in: i32,
    durability_in: i32,
    lease_revoke_in: i32,
    expire_in: i32,
    epoch_events_in: i32,
    snapshot_import_in: i32,
    responses_out: i32,
    mutations_out: i32,
    compaction_floor_out: i32,
    snapshot_export_out: i32,
    metrics_out: i32,

    /// Cluster-wide placement epoch as last reported by substrate
    /// `control_plane.epoch_events`. Used to stamp newly-minted
    /// sessions / mutations; observed in `mutations_out` envelopes so
    /// the watch_registry can fence stale frames. Defaults to 1
    /// pre-substrate so single-node graphs keep working.
    cluster_epoch: u32,

    // ── state_store provider selection (params; see define_params!) ──
    state_store: u8,
    root_path: u8,
    store_id: u8,
    /// Disk-provider lifecycle (`DISK_PHASE_*`). Meaningless in
    /// memory mode.
    disk_phase: u8,
    /// Disk provider: engine revision mirror (seeded from
    /// `DiskState::highest_revision` after recover/install).
    disk_revision: u64,
    /// Disk provider: recovered high-water revision. During WAL
    /// replay the engine re-runs every committed command in full but
    /// the provider suppresses physical writes already reflected in
    /// the recovered materialization and serves reads as-of the
    /// replay position — see `kv_store::DiskMaterializer` (RFC §4.3
    /// replay idempotence). NOTE the engine revision counter is
    /// reset to 0 before replay so the re-run recomputes it
    /// deterministically over the command stream.
    disk_replay_floor: u64,
    /// Disk provider: advisory flush request from the last apply;
    /// serviced by `disk_maintenance` at a step boundary.
    disk_flush_wanted: bool,
    /// Disk provider: a CHUNKED flush is in flight — `disk_maintenance`
    /// is driving `flush_step` one bounded slice per step. Distinct
    /// from `disk_flush_wanted`, which only says a flush is desired:
    /// once this is set the flush must be driven to completion (or
    /// failure) regardless of the watermark, because a run file is
    /// already open.
    disk_flush_active: bool,
    /// Bounded steps taken by the flush currently in flight.
    disk_flush_chunks: u32,
    /// Bounded compaction steps run since init. Compaction has its own
    /// fences and its own publish, so "is compaction the new cost" must
    /// be answerable separately from the flush.
    disk_compact_steps: u64,
    /// Wall clock at the first chunk, for the total `ms=` on the end
    /// line.
    disk_flush_t0: u64,
    /// Disk provider: a bounded compaction is in flight (one
    /// `COMPACT_STEP_RECORDS` step per module step).
    disk_compact_active: bool,
    disk_compact_cursor: u64,
    /// Floor the in-flight compaction was STARTED at. Pinned for the
    /// life of that compaction — the provider refuses a mid-flight
    /// floor change (`StoreError::Malformed`), and rightly so: a
    /// compaction that changed its retention target halfway would have
    /// reclaimed part of its input under one rule and part under
    /// another.
    disk_compact_floor: u64,

    // ── MVCC GC floor (§18, §21 invariant 13) ─────────────────────
    /// The highest GC floor this worker has observed as COMMITTED.
    ///
    /// **This is the whole gate.** `DiskStore::compact(floor)` is
    /// called with exactly two values and no others: this one, and the
    /// provider's own current `compaction_floor` (a same-floor merge,
    /// which reclaims nothing new). There is no path from a claim, a
    /// proposal, a local computation, or an operator param to a
    /// physical reclamation — only a committed record moves this field,
    /// and only this field authorizes advancing the floor.
    gc_committed_floor: u64,
    /// Committed floor records observed (manifest id 15).
    gc_floor_commits: u64,
    /// Retention claims published (manifest id 16).
    gc_claims_published: u64,
    /// Engine revision at the last snapshot export — the tighter
    /// active-read claim under `gc_claim_mode = snapshot`.
    gc_snapshot_revision: u64,
    gc_claim_seq: u32,
    gc_claim_mode: u8,
    gc_claim_kpg: u16,
    _gc_pad: u8,
    /// Consecutive steps the disk provider has spent unable to make
    /// storage progress (flush/compact failing, or `recover()` still
    /// waiting on the FS provider). Reset by any success. Drives both
    /// the bounded pre-quarantine retry budget and the periodic
    /// degraded-state re-log.
    disk_degraded_steps: u32,
    /// Consecutive HARD (non-`E_AGAIN`) flush/compact faults. Bounded
    /// by `DISK_HARD_FAULT_BUDGET` before the store quarantines.
    disk_hard_faults: u32,

    // ── Disk observability (manifest ids 9-13) ────────────────────
    /// Incremented BEFORE `store.flush()` is called.
    disk_flush_attempts: u64,
    /// Incremented after `store.flush()` returns `Ok`.
    disk_flush_completions: u64,
    /// Incremented after `store.flush()` returns a hard `Err`.
    disk_flush_failures: u64,
    /// Incremented when the flush was refused without any I/O being
    /// attempted (`Backpressure`: capture/install/compact in flight, or
    /// the run set is full).
    disk_flush_backpressure: u64,
    /// In-command `DiskMaterializer::make_room` flush faults. A wholly
    /// separate flush path from `disk_maintenance` — see its docs.
    disk_cmd_flush_faults: u64,
    /// Consecutive steps an exclusion fence has held off the write path
    /// WITHOUT any storage progress. Reset when no fence is active, and
    /// by every successful recover/compact-step/flush.
    disk_fence_steps: u32,
    /// Fences force-released by the watchdog (manifest id 14).
    disk_fence_aborts: u64,
    /// `dev_millis` of the last metric emit / heartbeat log.
    disk_observe_ms: u64,
    disk_heartbeat_ms: u64,
    /// FS-backed storage beneath the disk provider.
    fs_storage: FsRunStorage,

    /// Per-KPG keyed store. Kept in-line in the kernel-allocated
    /// state arena; size dominates the module's state budget
    /// (~4.5 MB at MAX_KEYS=1024, MAX_VALUE_LEN=4096).
    store: KvStore,

    /// Ordered disk provider state (~2.5 MB), used only when
    /// `state_store = 1`. Lives in the same kernel-zeroed arena; the
    /// zeroed form is a valid empty state.
    disk_state: DiskState,

    /// The raft position whose effects are fully folded into `store`,
    /// as published in-band by `lattice_apply_bridge`. Snapshot bodies
    /// are labelled with this — see `wire::MSG_APP_SNAPSHOT_BODY`.
    /// Zero means "nothing replicated applied yet", in which case no
    /// snapshot body is offered.
    applied_term: u64,
    applied_index: u64,

    /// Highest `applied_index` already reported upstream on `responses`.
    /// The router needs this position to enforce the applied-index half
    /// of the linearizable-read fence: consensus releasing a read proves
    /// only that CONSENSUS applied through the fence index, and this
    /// worker is a separate module fed over a channel. Without the
    /// report the router cannot tell whether the write a linearizable
    /// read must observe has reached this store yet, and would serve a
    /// false absence. Reported on change only — it moves in batches.
    published_applied_index: u64,
    /// Monotonic count of catalog mutations applied by this worker
    /// (RFC §14.2's schema generation). Reported on every
    /// `MSG_KV_APPLIED` so a compute-side cache of table name -> id can
    /// tell whether the mapping it used was still valid when the read
    /// executed. Never reset except on construction: a generation that
    /// went backwards would validate a stale mapping.
    catalog_generation: u64,

    /// Inbound snapshot chunk accumulator, plus whether an install is
    /// in progress. `importing` is set by RESET and cleared on `done`
    /// or on any gap/overflow.
    importing: bool,
    import_len: usize,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=applied, 1=reads, 2=snapshot_chunks).
    m_applied: u64,
    m_reads: u64,
    m_snapshot_chunks: u64,
    step_ctr: u64,

    import_buf: [u8; SNAPSHOT_BODY_MAX],

    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl WorkerState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.commands_in = -1;
        self.read_permit_in = -1;
        self.durability_in = -1;
        self.lease_revoke_in = -1;
        self.expire_in = -1;
        self.epoch_events_in = -1;
        self.snapshot_import_in = -1;
        self.responses_out = -1;
        self.mutations_out = -1;
        self.compaction_floor_out = -1;
        self.snapshot_export_out = -1;
        self.metrics_out = -1;
        self.cluster_epoch = 1;
        self.applied_term = 0;
        self.applied_index = 0;
        self.published_applied_index = 0;
        self.catalog_generation = 0;
        self.importing = false;
        self.import_len = 0;
        self.m_applied = 0;
        self.m_reads = 0;
        self.m_snapshot_chunks = 0;
        self.step_ctr = 0;
        self.state_store = STATE_STORE_MEMORY;
        self.root_path = 0;
        self.store_id = 0;
        self.disk_phase = DISK_PHASE_RECOVERING;
        self.disk_revision = 0;
        self.disk_replay_floor = 0;
        self.disk_flush_wanted = false;
        self.disk_flush_active = false;
        self.disk_flush_chunks = 0;
        self.disk_compact_steps = 0;
        self.disk_flush_t0 = 0;
        self.disk_compact_active = false;
        self.disk_compact_cursor = 0;
        self.disk_compact_floor = 0;
        self.gc_committed_floor = 0;
        self.gc_floor_commits = 0;
        self.gc_claims_published = 0;
        self.gc_snapshot_revision = 0;
        self.gc_claim_seq = 0;
        self.gc_claim_mode = GC_CLAIM_MODE_PIN;
        self.gc_claim_kpg = 0;
        self._gc_pad = 0;
        self.disk_degraded_steps = 0;
        self.disk_hard_faults = 0;
        self.disk_flush_attempts = 0;
        self.disk_flush_completions = 0;
        self.disk_flush_failures = 0;
        self.disk_flush_backpressure = 0;
        self.disk_cmd_flush_faults = 0;
        self.disk_fence_steps = 0;
        self.disk_fence_aborts = 0;
        self.disk_observe_ms = 0;
        self.disk_heartbeat_ms = 0;
        self.store.init();
        self.disk_state.init();
    }
}

unsafe fn drain_commands(worker: &mut WorkerState) -> bool {
    let sys_ptr = worker.syscalls;
    if sys_ptr.is_null() || worker.commands_in < 0 {
        return false;
    }
    if worker.state_store == STATE_STORE_DISK && worker.disk_phase != DISK_PHASE_SERVING {
        // Fail closed: a disk store that has not recovered (or has
        // quarantined on a storage fault) never serves. Commands stay
        // queued so backpressure propagates upstream.
        return false;
    }
    let sys = &*sys_ptr;
    let poll = (sys.channel_poll)(worker.commands_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    let n = (sys.channel_read)(worker.commands_in, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if hdr[0] == MSG_APP_APPLIED_POS {
        // In-band applied-position marker from lattice_apply_bridge.
        // Arriving here means every command ahead of it on this
        // channel has already been applied to `store`.
        if payload_len != 16 || payload_len > SCRATCH_BUF_SIZE {
            return false;
        }
        let mut pos = [0u8; 16];
        if ((sys.channel_read)(worker.commands_in, pos.as_mut_ptr(), 16) as usize) < 16 {
            return false;
        }
        let term = u64::from_le_bytes([
            pos[0], pos[1], pos[2], pos[3], pos[4], pos[5], pos[6], pos[7],
        ]);
        let index = u64::from_le_bytes([
            pos[8], pos[9], pos[10], pos[11], pos[12], pos[13], pos[14], pos[15],
        ]);
        // Monotonic — a stale marker after a restore must not rewind us.
        if index > worker.applied_index {
            worker.applied_term = term;
            worker.applied_index = index;
        }
        return true;
    }
    if hdr[0] != MSG_KV_COMMAND {
        return false;
    }
    // MSG_KV_COMMAND head layout owned by wire::KvCommandHead — kpg +
    // the §23 canonical identity the store is scoped to.
    if !(wire::KvCommandHead::LEN..=SCRATCH_BUF_SIZE).contains(&payload_len) {
        return false;
    }
    let mut in_buf = [0u8; SCRATCH_BUF_SIZE];
    let n2 = (sys.channel_read)(worker.commands_in, in_buf.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    let cmd = &in_buf[..payload_len];
    let Some(head) = wire::KvCommandHead::decode(cmd) else {
        return false;
    };
    let corr_id = head.corr_id;
    let kpg_id = head.kpg_id;
    let conn_id = head.conn_id;
    let consistency = head.consistency;
    let op = head.op;
    let id_tenant = head.tenant;
    let id_database = head.database;
    let id_keyspace = head.keyspace;
    let body_len = head.body_len as usize;
    let body_off = wire::KvCommandHead::LEN;
    if body_off + body_len > payload_len {
        return false;
    }
    let body = &cmd[body_off..body_off + body_len];

    let mut result_body = [0u8; SCRATCH_BUF_SIZE];
    // TIMER::MILLIS (0x0602) — monotonic wall clock for TTL enforcement.
    // `kv_store::apply` uses this as `now_ms` for absolute-deadline
    // comparisons and lazy expiry-on-touch.
    let now_ms = dev_millis(sys);
    // Read policy (RFC §8/§20). `MSG_KV_COMMAND` carries the
    // `Consistency` byte but not yet a `read_timestamp` field, so the
    // only way a request reaches this worker asking for a historical
    // revision today is the native `KV_OP_GET_AT` / `KV_OP_SCAN_AT`
    // ops, which carry it in the op body. Threading the policy through
    // anyway means the envelope extension is a one-line change here
    // rather than a re-plumb of the engine.
    // Catalog generation (RFC §14.2's schema generation, used by
    // relational_executor to validate a cached table name -> id
    // mapping).
    //
    // Bumped HERE, at the one place every mutation passes through,
    // rather than at the sites that happen to write catalog keys. The
    // catalog is storage-side and more than one compute graph can write
    // it, so a generation maintained by any single writer would miss
    // the others — which is precisely the case a name cache has to
    // survive.
    if catalog_mutation(op, body) {
        worker.catalog_generation = worker.catalog_generation.wrapping_add(1);
    }

    let policy = kv_store::ReadPolicy {
        consistency,
        read_timestamp: 0,
    };
    // ONE semantic engine (`kv_store::apply_mat_ctx`), provider selected
    // at graph construction: the interpreter is identical on both arms —
    // only the physical materializer differs.
    let (result, result_body_len, result_revision) = if worker.state_store == STATE_STORE_DISK {
        let store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        let mut mat = DiskMaterializer::new(store, worker.disk_revision, worker.disk_replay_floor);
        mat.flush_wanted = worker.disk_flush_wanted;
        mat.set_key_identity(id_tenant, id_database, id_keyspace);
        let (r, n) = kv_store::apply_mat_ctx(&mut mat, op, body, &mut result_body, now_ms, policy);
        worker.disk_revision = mat.revision;
        worker.disk_flush_wanted = mat.flush_wanted;
        // Latch both outcome flags before any `worker` field is touched
        // again: `mat` mutably borrows `worker.disk_state`, so reading
        // them up front is what releases the borrow.
        let mat_backpressure = mat.backpressure;
        let mat_fault = mat.fault;
        if mat_backpressure {
            // Retryable, NOT a fault: the memtable filled while the
            // chunked flush was still draining it. Counted on the same
            // metric as a refused maintenance flush (id 12) and logged
            // once per occurrence, because "writes are being refused"
            // must never be a silent state — that silence is what made
            // the 384-write wedge unreadable in the first place.
            worker.disk_flush_backpressure = worker.disk_flush_backpressure.wrapping_add(1);
            let mut buf = [0u8; 80];
            let mut n = write_prefix(&mut buf, 0, b"[kvw] disk flush BACKPRESSURE mem=");
            n = write_dec(&mut buf, n, u64::from(worker.disk_state.mem_count));
            n = write_prefix(&mut buf, n, b" chunks=");
            n = write_dec(&mut buf, n, u64::from(worker.disk_flush_chunks));
            log_line(worker, &buf, n);
        }
        if mat_fault {
            // In-command storage fault, from `DiskMaterializer::
            // make_room` — a flush/compact driven synchronously by a
            // write that found the memtable FULL (512), entirely
            // separate from `disk_maintenance`'s watermark flush (384).
            // This command still answers (KV_RESULT_INTERNAL from the
            // engine); the question is what happens to the store.
            //
            // It used to quarantine unconditionally, which meant this
            // path silently bypassed the whole `DISK_HARD_FAULT_BUDGET`
            // and never touched a counter — one transient device
            // refusal here killed the store with no trace. Same rules
            // as maintenance now: transient is patience, hard faults
            // spend the budget, and either way it is counted.
            worker.disk_cmd_flush_faults = worker.disk_cmd_flush_faults.wrapping_add(1);
            worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
            if !worker.fs_storage.last_was_transient() {
                worker.disk_hard_faults = worker.disk_hard_faults.saturating_add(1);
                if worker.disk_hard_faults >= DISK_HARD_FAULT_BUDGET {
                    log_disk_errno(worker, b"[kvw] disk QUARANTINE cmd-flush errno=");
                    worker.disk_phase = DISK_PHASE_QUARANTINED;
                }
            }
        }
        (r, n, worker.disk_revision)
    } else {
        worker
            .store
            .set_key_identity(id_tenant, id_database, id_keyspace);
        let (r, n) = kv_store::apply_mat_ctx(
            &mut worker.store,
            op,
            body,
            &mut result_body,
            now_ms,
            policy,
        );
        (r, n, worker.store.revision)
    };
    // Single chokepoint: every command reaching the store is applied here.
    // `reads` is the read-op subset of `applied` (the &mut store borrow has
    // ended, so the counter writes are borrow-safe).
    worker.m_applied = worker.m_applied.wrapping_add(1);
    // Temporary: did the cross-range transaction ops reach the apply
    // chokepoint at all, and what did they answer? "the entry
    // committed" and "the worker applied it" are different claims.
    if (0x16..=0x18).contains(&op) {
        let mut m = *b"[kvw] txnop op=000 res=000";
        m[15] = b'0' + (op / 100) % 10;
        m[16] = b'0' + (op / 10) % 10;
        m[17] = b'0' + op % 10;
        m[23] = b'0' + (result / 100) % 10;
        m[24] = b'0' + (result / 10) % 10;
        m[25] = b'0' + result % 10;
        unsafe { dev_log(&*worker.syscalls, 2, m.as_ptr(), m.len()) };
    }
    if matches!(
        op,
        KV_OP_GET
            | KV_OP_RANGE
            | KV_OP_EXISTS
            | KV_OP_SCAN
            | KV_OP_MGET
            | KV_OP_STRLEN
            | KV_OP_GET_AT
            | KV_OP_SCAN_AT
    ) {
        worker.m_reads = worker.m_reads.wrapping_add(1);
    }

    // Build MSG_KV_APPLIED. The head layout is owned by
    // wire::KvAppliedHead; the body follows, then [catalog_generation:u64
    // LE]. The generation goes AFTER the body for the same reason the
    // fence tail does on MSG_KV_RESPONSE: the router reads body_len at a
    // fixed offset and slices, so appending is invisible to it until it
    // is taught to look.
    const APP_TAIL: usize = 8;
    let resp_payload_len = wire::KvAppliedHead::LEN + result_body_len + APP_TAIL;
    if resp_payload_len > u16::MAX as usize {
        return false;
    }
    let total = 3 + resp_payload_len;
    if total > worker.scratch.len() {
        return false;
    }
    let scratch = &mut worker.scratch[..total];
    scratch[0] = MSG_KV_APPLIED;
    scratch[1] = (resp_payload_len & 0xFF) as u8;
    scratch[2] = ((resp_payload_len >> 8) & 0xFF) as u8;
    wire::KvAppliedHead {
        corr_id,
        kpg_id,
        conn_id,
        result,
        revision: result_revision,
        body_len: result_body_len as u16,
    }
    .encode(&mut scratch[3..]);
    let mut p = 3 + wire::KvAppliedHead::LEN;
    scratch[p..p + result_body_len].copy_from_slice(&result_body[..result_body_len]);
    p += result_body_len;
    scratch[p..p + 8].copy_from_slice(&worker.catalog_generation.to_le_bytes());

    if worker.responses_out < 0 {
        return false;
    }
    let n = (sys.channel_write)(worker.responses_out, worker.scratch.as_mut_ptr(), total);
    let ok = n == total as i32;

    // After the response lands, fan a `MSG_WATCH_EVENT` envelope out to
    // watch_registry for any successful PUT / DELETE. This is the
    // event-emission half of the substrate-fenced watch path: anchor
    // creates the watch, registry stamps the epoch (Phase 8a), fanout
    // pushes frames (Phase 5b), and now the worker actually publishes
    // mutations. INCR/DECR/APPEND/PREPEND/MSET/CAS/TXN are mutations
    // too but live on the redis-protocol surface only; they'll fold
    // in once the watch matcher learns to project compound ops.
    if ok && worker.mutations_out >= 0 {
        emit_watch_event(worker, op, kpg_id, result, result_revision, body);
    }
    ok
}

/// Emit `MSG_WATCH_EVENT` for the just-applied mutation, if any. Wire
/// shape (`modules/common/wire.rs::MSG_WATCH_EVENT`):
///
///   `[kpg_id:u16 LE][revision:u64 LE][op:u8]
///    [key_len:u16 LE][key…][value_len:u16 LE][value…]`
///
/// `op` is the inbound `KV_OP_*` byte verbatim — `watch_registry`
/// translates it into the `WATCH_EVENT_*` filter taxonomy. Keeping
/// the worker's op byte avoids a redundant remap that would have to
/// stay in lockstep with the registry's matcher.
unsafe fn emit_watch_event(
    worker: &mut WorkerState,
    op: u8,
    kpg_id: u16,
    result: u8,
    revision: u64,
    cmd_body: &[u8],
) {
    let success = matches!(result, KV_RESULT_OK | KV_RESULT_INTEGER);
    if !success {
        return;
    }
    let (key, value) = match op {
        KV_OP_PUT => {
            // PUT body: [key_len:u16 LE][key…][value_len:u32 LE][value…]
            //           [put_flags:u8][expiry_ms:u64 LE]
            if cmd_body.len() < 2 {
                return;
            }
            let key_len = u16::from_le_bytes([cmd_body[0], cmd_body[1]]) as usize;
            if 2 + key_len + 4 > cmd_body.len() {
                return;
            }
            let key = &cmd_body[2..2 + key_len];
            let vlen_off = 2 + key_len;
            let value_len = u32::from_le_bytes([
                cmd_body[vlen_off],
                cmd_body[vlen_off + 1],
                cmd_body[vlen_off + 2],
                cmd_body[vlen_off + 3],
            ]) as usize;
            let value_off = vlen_off + 4;
            if value_off + value_len > cmd_body.len() {
                return;
            }
            (key, &cmd_body[value_off..value_off + value_len])
        }
        KV_OP_DELETE => {
            // DELETE body: [key_len:u16 LE][key…]
            if cmd_body.len() < 2 {
                return;
            }
            let key_len = u16::from_le_bytes([cmd_body[0], cmd_body[1]]) as usize;
            if 2 + key_len > cmd_body.len() {
                return;
            }
            (&cmd_body[2..2 + key_len], &[][..])
        }
        _ => return,
    };
    let event_op = op;

    // Cap key+value at u16::MAX (wire-format ceiling); watch_hub also
    // bounds the stored key length, so anything larger would be silently
    // dropped by the registry. Better to skip-and-account than spam an
    // oversize envelope no consumer can accept.
    if key.len() > u16::MAX as usize || value.len() > u16::MAX as usize {
        return;
    }
    // Envelope: [head:3][kpg:2][rev:8][op:1][klen:2][k][vlen:2][v]
    let payload_len = 2 + 8 + 1 + 2 + key.len() + 2 + value.len();
    let total = 3 + payload_len;
    if total > worker.scratch.len() || payload_len > u16::MAX as usize {
        return;
    }
    let buf = &mut worker.scratch[..total];
    buf[0] = MSG_WATCH_EVENT;
    buf[1] = (payload_len & 0xFF) as u8;
    buf[2] = ((payload_len >> 8) & 0xFF) as u8;
    let mut p = 3;
    buf[p..p + 2].copy_from_slice(&kpg_id.to_le_bytes());
    p += 2;
    buf[p..p + 8].copy_from_slice(&revision.to_le_bytes());
    p += 8;
    buf[p] = event_op;
    p += 1;
    buf[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    buf[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    buf[p..p + 2].copy_from_slice(&(value.len() as u16).to_le_bytes());
    p += 2;
    buf[p..p + value.len()].copy_from_slice(value);

    let sys = worker.syscalls;
    if sys.is_null() {
        return;
    }
    let _ = ((*sys).channel_write)(worker.mutations_out, worker.scratch.as_mut_ptr(), total);
}

/// The two relational CATALOG keyspaces.
///
/// **Byte-identical to `relational::KS_RELATIONAL_CATALOG` and
/// `KS_RELATIONAL_CATALOG_NAME` by requirement.** Inlined rather than
/// mounted: this worker is the one semantic KV state machine and knows
/// nothing else about the relational layer, and `#[path]`-mounting
/// `relational.rs` for two integers would pull its whole dependency
/// tree into a module that must stay model-agnostic.
/// `tests/contract_relational.rs` asserts they agree, so the
/// duplication cannot drift silently.
const KS_RELATIONAL_CATALOG: u32 = 0x8000_0001;
const KS_RELATIONAL_CATALOG_NAME: u32 = 0x8000_0007;

/// Does this command mutate a relational CATALOG keyspace?
///
/// Body layouts are PER-OPCODE, and this function learned that twice:
///
/// - Its first version read the keyspace at bytes 8..12, from
///   `internal_key`'s tenant|database|keyspace prefix — but that prefix
///   is added by the STORE and does not exist in the command body. The
///   right layout of the wrong layer; nothing ever matched.
/// - Its second read `[key_len:u16][key…]` for every op — but DELETE is
///   count-prefixed multi-key (`[count:u16][key_len:u16][key]…`, redis
///   DEL is variadic), so a DROP TABLE's catalog deletes parsed the
///   count as a key length and never matched either. CREATE bumped and
///   DROP did not, which is the worst half-working shape.
///
/// So: single-key ops check their one key; DELETE walks its list; and
/// ops whose bodies this function does not model (the TXN family, MSET,
/// FLUSH) count as catalog mutations UNCONDITIONALLY. That errs in the
/// only safe direction — a spurious bump makes a compute-side cache
/// revalidate, which costs one lookup, while a missed bump serves a
/// stale table id, which is a wrong answer. INSERTs travel as TXN and
/// therefore bump spuriously; the cache still wins on consecutive
/// reads, and correctness never depends on this function being clever.
///
/// The keyspace sits at bytes 0..4 BE of each key (`sql_exec::kv_key`:
/// `[keyspace:u32 BE][body…]`). A non-relational caller's key can
/// start with these bytes by chance; same safe direction.
fn catalog_mutation(op: u8, body: &[u8]) -> bool {
    fn key_hits(key: &[u8]) -> bool {
        if key.len() < 4 {
            return false;
        }
        let ks = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        ks == KS_RELATIONAL_CATALOG || ks == KS_RELATIONAL_CATALOG_NAME
    }
    match op {
        KV_OP_PUT | KV_OP_CAS | KV_OP_INCR | KV_OP_DECR | KV_OP_APPEND | KV_OP_PREPEND => {
            if body.len() < 2 {
                return false;
            }
            let n = u16::from_le_bytes([body[0], body[1]]) as usize;
            body.get(2..2 + n).is_some_and(key_hits)
        }
        KV_OP_DELETE => {
            if body.len() < 2 {
                return false;
            }
            let count = u16::from_le_bytes([body[0], body[1]]) as usize;
            let mut at = 2;
            for _ in 0..count.min(64) {
                let Some(l) = body.get(at..at + 2) else {
                    return false;
                };
                let n = u16::from_le_bytes([l[0], l[1]]) as usize;
                let Some(key) = body.get(at + 2..at + 2 + n) else {
                    return false;
                };
                if key_hits(key) {
                    return true;
                }
                at += 2 + n;
            }
            false
        }
        // Opaque multi-op bodies: assume the worst, safely.
        KV_OP_MSET | KV_OP_FLUSH | KV_OP_TXN | KV_OP_TXN_PREPARE | KV_OP_TXN_RESOLVE
        | KV_OP_TXN_RECORD => true,
        _ => false,
    }
}

// ── Module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<WorkerState>() as u32
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
    if state_size < core::mem::size_of::<WorkerState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let worker = unsafe { &mut *state.cast::<WorkerState>() };
    worker.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(worker, params, params_len) };
    }
    if !worker
        .fs_storage
        .init(sys_ptr, worker.root_path, worker.store_id)
    {
        // A store layout that cannot separate instances (FAT32 root +
        // non-zero store_id, or an out-of-range id) must refuse to
        // start rather than silently share file names with another
        // worker.
        return -1;
    }

    // EFFECTIVE-CONFIG REPORT (standards/rig.md §7a). Unconditional and
    // mode-independent: it names what the module ACTUALLY parsed out of
    // its params TLV, not what the config intended. A worker that came
    // up in memory mode because `state_store` never landed is now
    // visibly a memory-mode worker instead of being indistinguishable
    // from a worker that is silent for any other reason. Emitted from
    // `module_new` so it lands even if the module never gets a step.
    unsafe {
        let mut buf = [0u8; 64];
        let mut n = write_prefix(&mut buf, 0, b"[kvw] init state_store=");
        n = write_dec(&mut buf, n, u64::from(worker.state_store));
        n = write_prefix(&mut buf, n, b" root_path=");
        n = write_dec(&mut buf, n, u64::from(worker.root_path));
        n = write_prefix(&mut buf, n, b" store_id=");
        n = write_dec(&mut buf, n, u64::from(worker.store_id));
        n = write_prefix(&mut buf, n, b" plen=");
        n = write_dec(&mut buf, n, params_len as u64);
        dev_log(&*sys_ptr, DISK_LOG_LEVEL, buf.as_ptr(), n);
    }

    worker.commands_in = in_chan;
    worker.responses_out = out_chan;

    unsafe {
        let sys = &*sys_ptr;
        worker.read_permit_in = dev_channel_port(sys, 0, 1);
        worker.durability_in = dev_channel_port(sys, 0, 2);
        worker.lease_revoke_in = dev_channel_port(sys, 0, 3);
        worker.expire_in = dev_channel_port(sys, 0, 4);
        worker.epoch_events_in = dev_channel_port(sys, 0, 5);
        worker.snapshot_import_in = dev_channel_port(sys, 0, 6);
        worker.mutations_out = dev_channel_port(sys, 1, 1);
        worker.compaction_floor_out = dev_channel_port(sys, 1, 2);
        worker.snapshot_export_out = dev_channel_port(sys, 1, 3);
        worker.metrics_out = dev_channel_port(sys, 1, 4);
    }
    0
}

/// Maximum commands processed per scheduler tick. Bounded so a busy
/// command channel can't starve the rest of the graph. At
/// `tick_us = 250` (the apply domain in `bare-metal-pi5.yaml`) this
/// keeps the per-step budget at roughly 32 × ~5 µs ≈ 160 µs — well
/// inside one tick. The pre-bound implementation processed one
/// command per tick, which capped redis throughput at ~940 op/s
/// (host Linux, 1 ms ticks); the bound below removes that ceiling
/// for the common case where multiple clients have requests queued.
const PER_TICK_DRAIN_BUDGET: u32 = 32;

/// Microseconds of one `module_step` the command drain may consume.
///
/// A COUNT budget alone is not a bound, because the per-command cost is
/// not a constant — it moves by orders of magnitude the instant the
/// first flush publishes a run.
///
/// Every write goes through `apply_put` -> `store.get_live()`: the
/// engine must read the current record to produce `create_revision`,
/// `version` and `prev_kv`. While the whole dataset is still in the
/// memtable that read is a binary search. Once a run file exists it
/// becomes a merge over the memtable PLUS every run — real synchronous
/// FAT32/NVMe sector reads. So the step immediately after
/// `release_flushed()` drains a backlog accumulated over the flush's 15
/// steps, and each of those commands now pays disk I/O it did not pay
/// before. 32 x (a few hundred us) is how a step that looks bounded
/// blows a 16 ms burst ceiling.
///
/// 1500 us leaves the rest of the module's 12000/16000 declared budget
/// to maintenance (measured at <=1 ms per phase since the A/B manifest
/// landed) with a wide margin. The budget is checked BEFORE starting
/// each additional command and never interrupts one in flight, so a
/// step always makes at least one command of progress and the true
/// worst case is 1500 us plus one command.
const DRAIN_BUDGET_US: u64 = 1500;

/// Only log a drain that went genuinely long.
///
/// Under saturation a step routinely reaches the budget and stops — on
/// host that lands at ~2100 us (1500 budget + the one command already
/// in flight), and logging that would emit a line per step. 5000 us is
/// well clear of normal operation and still less than half the module's
/// declared 12000 us deadline, so the line is a WARNING with room to
/// act on rather than a post-mortem.
///
/// Read the pair: `n` large with a big `us` is aggregate cost (the
/// budget is mis-sized); `n=1` with a big `us` is ONE command that is
/// itself too slow — which no per-step budget can bound, and which
/// would point at the read path's merge over run files.
const DRAIN_LOG_US: u64 = 5000;

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let worker = unsafe { &mut *state.cast::<WorkerState>() };
    unsafe {
        if worker.state_store == STATE_STORE_DISK {
            // Observe FIRST, before any storage action. If
            // `disk_maintenance` blocks inside the FS provider and never
            // returns, this step's snapshot has already been published —
            // so the scrape/log shows the state going INTO the flush and
            // `flush_attempts` exceeding `flush_completions + failures +
            // backpressure` pins the block to the flush call itself.
            // Emitting after the action would report nothing at all,
            // which is exactly the blackout this replaced.
            disk_observe(worker);
            disk_maintenance(worker);
        }
        drain_epoch_events(worker);
        drain_snapshot_ctl(worker);
        drain_gc_floor(worker);
        publish_retention_claim(worker);
    }
    let drain_t0 = unsafe { now_us(worker) };
    let scans0 = worker.disk_state.scans_opened.get();
    let skips0 = worker.disk_state.scans_skipped.get();
    let blocks0 = worker.disk_state.blocks_read.get();
    let (reads0, rbytes0, opens0) = worker.fs_storage.read_counters();
    let mut budget = PER_TICK_DRAIN_BUDGET;
    let mut drained = 0u32;
    let mut spilled = false;
    while budget > 0 {
        // Time check FIRST for every command after the first: a step
        // must always make some progress, but it must never START work
        // it has no budget left for. Whatever is left stays queued and
        // is drained next step — the channel is the spill buffer.
        if drained > 0 && unsafe { now_us(worker) }.wrapping_sub(drain_t0) >= DRAIN_BUDGET_US {
            spilled = true;
            break;
        }
        let made_progress = unsafe { drain_commands(worker) };
        if !made_progress {
            break;
        }
        budget -= 1;
        drained += 1;
    }
    // Report the applied position upstream whenever it moved. This is
    // the half of the linearizable-read fence that only this module can
    // supply: consensus knows when IT applied through the fence index,
    // but the store a read actually queries lives here, behind a
    // channel. Reporting on change keeps it off the hot path — the
    // position advances once per committed batch, not once per command.
    //
    // Best-effort: on backpressure `published_applied_index` stays put
    // and the next step retries. A late report can only make the router
    // hold a linearizable read longer (and, past its deadline, fail it
    // closed); it can never release one early.
    unsafe {
        if worker.applied_index > worker.published_applied_index && worker.responses_out >= 0 {
            let sys = &*worker.syscalls;
            let mut env = [0u8; 19];
            env[0] = MSG_APP_APPLIED_POS;
            env[1] = 16;
            env[2] = 0;
            env[3..11].copy_from_slice(&worker.applied_term.to_le_bytes());
            env[11..19].copy_from_slice(&worker.applied_index.to_le_bytes());
            if (sys.channel_write)(worker.responses_out, env.as_mut_ptr(), 19) == 19 {
                worker.published_applied_index = worker.applied_index;
            }
        }
    }
    unsafe {
        let drain_us = now_us(worker).wrapping_sub(drain_t0);
        if drain_us >= DRAIN_LOG_US {
            let mut buf = [0u8; 96];
            let mut n = write_prefix(&mut buf, 0, b"[kvw] drain n=");
            n = write_dec(&mut buf, n, u64::from(drained));
            n = write_prefix(&mut buf, n, b" us=");
            n = write_dec(&mut buf, n, drain_us);
            n = write_prefix(&mut buf, n, b" spill=");
            n = write_dec(&mut buf, n, u64::from(spilled));
            n = write_prefix(&mut buf, n, b" mem=");
            n = write_dec(&mut buf, n, u64::from(worker.disk_state.mem_count));
            n = write_prefix(&mut buf, n, b" runs=");
            n = write_dec(&mut buf, n, u64::from(worker.disk_state.run_count));
            // Which runs the lookups actually touched. `scan` counts run
            // scans OPENED (block I/O); `skip` counts those the Bloom
            // filter ruled out for free. `n=1` with a big `us` and
            // `scan>=1` is a POSITIVE lookup walking blocks — the case a
            // sparse index would fix. `n=1`, big `us`, `scan=0` means
            // the read path was not involved and the cost is elsewhere.
            n = write_prefix(&mut buf, n, b" scan=");
            n = write_dec(
                &mut buf,
                n,
                worker.disk_state.scans_opened.get().wrapping_sub(scans0),
            );
            n = write_prefix(&mut buf, n, b" skip=");
            n = write_dec(
                &mut buf,
                n,
                worker.disk_state.scans_skipped.get().wrapping_sub(skips0),
            );
            // Run BLOCKS physically loaded. `scan=1 blocks=1` is the
            // sparse index doing its job; `scan=1 blocks=12` means the
            // index is not being consulted and the lookup walked the
            // whole run.
            n = write_prefix(&mut buf, n, b" blocks=");
            n = write_dec(
                &mut buf,
                n,
                worker.disk_state.blocks_read.get().wrapping_sub(blocks0),
            );
            // Storage-level read CALLS and bytes. Each call is a full
            // FS_OPEN + FS_SEEK + FS_READ + FS_CLOSE, and FS_OPEN on
            // FAT32 linearly scans the directory — which the run files
            // keep growing. `us / reads` is therefore the cost of one
            // open, and it is the number that decides whether the
            // remaining time is the block reads or the opens around
            // them. Counted at the storage layer so a footer read or a
            // whole-file verify cannot hide behind the block counter.
            let (reads1, rbytes1, opens1) = worker.fs_storage.read_counters();
            n = write_prefix(&mut buf, n, b" reads=");
            n = write_dec(&mut buf, n, reads1.wrapping_sub(reads0));
            n = write_prefix(&mut buf, n, b" rkb=");
            n = write_dec(&mut buf, n, rbytes1.wrapping_sub(rbytes0) / 1024);
            // FS_OPEN calls the read path actually issued. `reads`
            // climbing while `opens` stays near zero IS the descriptor
            // cache working — proof rather than an inference from
            // timing.
            n = write_prefix(&mut buf, n, b" opens=");
            n = write_dec(&mut buf, n, opens1.wrapping_sub(opens0));
            log_line(worker, &buf, n);
        }
    }
    unsafe {
        worker.step_ctr = worker.step_ctr.wrapping_add(1);
        // Memory mode only — in disk mode `disk_observe` already emits
        // ids 0-2 on the wall-clock cadence at the top of the step.
        if worker.state_store != STATE_STORE_DISK
            && worker.step_ctr.is_multiple_of(5000)
            && !worker.syscalls.is_null()
        {
            telemetry::emit_counters(
                &*worker.syscalls,
                worker.metrics_out,
                &[worker.m_applied, worker.m_reads, worker.m_snapshot_chunks],
            );
            // Memory mode reports its effective config too — a scrape
            // must never have to infer the provider from an ABSENCE.
            emit_effective_config_gauges(
                &*worker.syscalls,
                worker.metrics_out,
                worker.state_store,
                worker.root_path,
                worker.store_id,
            );
        }
    }
    0
}

/// Ids 17-18: the EFFECTIVE `state_store` / `root_path` this instance
/// parsed out of its params TLV (standards/rig.md §7a). Emitted from
/// BOTH the disk and the memory cadence, so `cfg_state_store` is always
/// present in a scrape and a config/runtime disagreement is one value
/// away instead of an inference from missing metrics.
unsafe fn emit_effective_config_gauges(
    sys: &SyscallTable,
    chan: i32,
    state_store: u8,
    root_path: u8,
    store_id: u8,
) {
    telemetry::emit_gauges(
        sys,
        chan,
        17,
        &[
            u64::from(state_store),
            u64::from(root_path),
            u64::from(store_id),
        ],
    );
}

/// One bounded disk-provider storage action per step (disk mode only):
///
/// - `RECOVERING`: attempt `DiskStore::recover()`. E_AGAIN-patient —
///   while the FS provider is still initialising the attempt repeats
///   next step (the WAL's wait idiom); any hard fault QUARANTINES the
///   store (fail closed — never serve over a store that failed §9.4
///   validation). Success seeds the engine revision from the
///   recovered `highest_revision` and opens command intake.
/// - `SERVING` with a compaction in flight: one bounded
///   `COMPACT_STEP_RECORDS` compact step.
/// - `SERVING` with `flush_wanted`: one memtable flush (worst-case
///   cost documented in the module docs); a full run set starts the
///   bounded compaction instead.
unsafe fn disk_maintenance(worker: &mut WorkerState) {
    disk_fence_watchdog(worker);
    match worker.disk_phase {
        DISK_PHASE_RECOVERING => {
            // Preallocate the manifest A/B pair BEFORE recovering, one
            // slot per step. Creating a file on FAT32 measured 159 ms
            // on the rig; doing both here in one step would blow the
            // deadline at boot and kill the module before it ever
            // served — the same failure the A/B pair removes from the
            // publish path. One bounded action per step, exactly like
            // every other storage action in this function.
            {
                let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                if !store.prepared() {
                    match store.prepare_step() {
                        Ok(Progress::Done) => {}
                        Ok(Progress::InProgress { .. }) => return,
                        Err(_) => {
                            worker.disk_degraded_steps =
                                worker.disk_degraded_steps.saturating_add(1);
                            if !worker.fs_storage.last_was_transient() {
                                log_disk_errno(worker, b"[kvw] disk QUARANTINE prepare errno=");
                                worker.disk_phase = DISK_PHASE_QUARANTINED;
                            }
                            return;
                        }
                    }
                }
            }
            let recovered = {
                let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                store.recover().is_ok()
            };
            if recovered {
                // Replay idempotence (RFC §4.3): the WAL re-feeds the
                // committed stream from the start, so the engine
                // revision counter restarts at 0 and is recomputed
                // deterministically over the replayed commands, while
                // writes the materialization already reflects (ts at
                // or below the recovered high-water, verified
                // byte-for-byte) are suppressed and reads serve
                // as-of the replay position. See DiskMaterializer.
                worker.disk_replay_floor = worker.disk_state.highest_revision;
                worker.disk_revision = 0;
                worker.disk_phase = DISK_PHASE_SERVING;
                worker.disk_degraded_steps = 0;
                worker.disk_hard_faults = 0;
                worker.disk_fence_steps = 0;
                if !worker.syscalls.is_null() {
                    let sys = &*worker.syscalls;
                    dev_log(sys, 2, b"[kvw] disk recover ok".as_ptr(), 21);
                }
            } else if !worker.fs_storage.last_was_transient() {
                log_disk_errno(worker, b"[kvw] disk QUARANTINE recover errno=");
                worker.disk_phase = DISK_PHASE_QUARANTINED;
            } else {
                // E_AGAIN: stay in RECOVERING and retry next step. Count
                // it so a provider that never finishes initialising is
                // visible in the periodic degraded line rather than
                // looking like a silent idle module.
                worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
            }
        }
        DISK_PHASE_SERVING => {
            // A COMMITTED GC floor above the provider's current one is
            // the only thing that starts a floor-ADVANCING compaction
            // (§18 / §21 inv. 13). `gc_committed_floor` is written by
            // exactly one function — `drain_gc_floor` — and only from a
            // committed record, so there is no route from a claim or a
            // proposal to this line.
            // A chunked flush already holds an open run file. It must
            // be driven to completion FIRST, and no compaction may
            // start meanwhile: `compact` refuses with `Backpressure`
            // while a flush is in flight, and this arm `return`s on
            // that refusal — so entering it would starve the very
            // flush that has to finish before compaction can proceed.
            // Flush first, compact after, and the cycle cannot form.
            if !worker.disk_flush_active
                && !worker.disk_compact_active
                && worker.gc_committed_floor > worker.disk_state.compaction_floor
            {
                worker.disk_compact_active = true;
                worker.disk_compact_cursor = 0;
                worker.disk_compact_floor = worker.gc_committed_floor;
            }
            if worker.disk_compact_active && !worker.disk_flush_active {
                // Pinned at start — the provider refuses a mid-flight
                // floor change, and a compaction must not reclaim half
                // its input under one retention rule and half under
                // another. A floor committed while this one runs is
                // picked up by the next cycle.
                let floor = worker.disk_compact_floor;
                let cursor = worker.disk_compact_cursor;
                let step_t0 = now_us(worker);
                let outcome = {
                    let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                    store.compact(floor, cursor)
                };
                worker.disk_compact_steps = worker.disk_compact_steps.wrapping_add(1);
                // Compaction is bounded per step like the flush, but it
                // merges over run FILES and publishes a manifest, so its
                // step can be long for reasons the flush's cannot. Timed
                // and reported on the same threshold as the drain, so a
                // slow compaction step names itself instead of looking
                // like a mystery gap between heartbeats.
                {
                    let step_us = now_us(worker).wrapping_sub(step_t0);
                    if step_us >= DRAIN_LOG_US {
                        let mut cbuf = [0u8; 96];
                        let mut cn = write_prefix(&mut cbuf, 0, b"[kvw] disk compact step us=");
                        cn = write_dec(&mut cbuf, cn, step_us);
                        cn = write_prefix(&mut cbuf, cn, b" cur=");
                        cn = write_dec(&mut cbuf, cn, cursor);
                        cn = write_prefix(&mut cbuf, cn, b" runs=");
                        cn = write_dec(&mut cbuf, cn, u64::from(worker.disk_state.run_count));
                        log_line(worker, &cbuf, cn);
                    }
                }
                match outcome {
                    Ok(Progress::Done) => {
                        worker.disk_compact_active = false;
                        worker.disk_degraded_steps = 0;
                        worker.disk_hard_faults = 0;
                        worker.disk_fence_steps = 0;
                    }
                    Ok(Progress::InProgress { cursor: c }) => {
                        // Real forward progress: the fence is doing its
                        // job, so the starvation budget starts over.
                        worker.disk_compact_cursor = c;
                        worker.disk_degraded_steps = 0;
                        worker.disk_hard_faults = 0;
                        worker.disk_fence_steps = 0;
                    }
                    // Busy (snapshot/install in flight): retry later.
                    //
                    // Counted, because this is the worst silent stall
                    // in the module: `disk_compact_active` stays set,
                    // so this branch `return`s every step and the flush
                    // below never runs at all. A capture or install
                    // that never clears therefore wedges the store with
                    // no fault, no phase change and no I/O — precisely
                    // the shape that reads as a healthy idle worker.
                    Err(StoreError::Backpressure) => {
                        worker.disk_flush_backpressure =
                            worker.disk_flush_backpressure.wrapping_add(1);
                        worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
                    }
                    Err(_) if worker.fs_storage.last_was_transient() => {
                        // Transient FS backpressure (E_AGAIN: the FS
                        // provider is still initialising, or the block
                        // device it sits on is busy — the WAL's async
                        // writes share that device). The WAL's wait
                        // idiom, not a fault: restart this compaction
                        // from scratch on a later step. Nothing durable
                        // changed — compaction publishes last.
                        worker.disk_compact_active = false;
                        worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
                    }
                    Err(_) => {
                        // Hard fault. Compaction is pure maintenance:
                        // the pre-compaction manifest is still the
                        // published truth, so retry within budget
                        // rather than losing the store outright.
                        worker.disk_compact_active = false;
                        worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
                        worker.disk_hard_faults = worker.disk_hard_faults.saturating_add(1);
                        if worker.disk_hard_faults >= DISK_HARD_FAULT_BUDGET {
                            log_disk_errno(worker, b"[kvw] disk QUARANTINE compact errno=");
                            worker.disk_phase = DISK_PHASE_QUARANTINED;
                        }
                    }
                }
                return; // one bounded storage action per step
            }
            if worker.disk_flush_wanted || worker.disk_flush_active {
                // ONE BOUNDED CHUNK OF THE FLUSH PER STEP.
                //
                // This used to be a single `store.flush()`: the whole
                // memtable (up to MEMTABLE_FLUSH_WATERMARK = 384
                // records, ~1.7 MB) plus a fresh run file plus the
                // manifest plus an fsync, inside ONE `module_step`. It
                // was the only action in this function that was not
                // bounded, and on FAT32-over-NVMe it blew the step
                // deadline and got the module TERMINATED (see the
                // manifest's step-deadline note) — the graph served
                // exactly 384 writes and died. No deadline could cover
                // it: the config validator caps the burst at
                // 16 x tick_us, and the flush costs more than that.
                //
                // `flush_step` writes at most FLUSH_STEP_RECORDS per
                // call and carries its partial block across calls, so
                // the run is byte-identical to the one the single-call
                // path produced (the golden vectors in
                // tests/contract_disk_store.rs are unchanged). Only the
                // final chunk writes the footer, fsyncs and publishes,
                // so atomicity is untouched: a crash at any chunk
                // boundary leaves the previous manifest as truth and
                // the partial run as a recover()-collected orphan.
                //
                // `attempts` is still counted BEFORE the first chunk,
                // so `attempts > completions + failures + backpressure`
                // still means "entered and never returned".
                if !worker.disk_flush_active {
                    worker.disk_flush_attempts = worker.disk_flush_attempts.wrapping_add(1);
                    worker.disk_flush_active = true;
                    worker.disk_flush_chunks = 0;
                    worker.disk_flush_t0 = now_ms(worker);
                    let mut buf = [0u8; 64];
                    let mut n = write_prefix(&mut buf, 0, b"[kvw] disk flush begin mem=");
                    n = write_dec(&mut buf, n, u64::from(worker.disk_state.mem_count));
                    n = write_prefix(&mut buf, n, b" runs=");
                    n = write_dec(&mut buf, n, u64::from(worker.disk_state.run_count));
                    log_line(worker, &buf, n);
                }
                let chunk_t0 = now_ms(worker);
                let (outcome, phase) = {
                    let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                    let r = store.flush_step();
                    (r, store.flush_phase())
                };
                worker.disk_flush_chunks = worker.disk_flush_chunks.wrapping_add(1);
                let chunk_ms = now_ms(worker).wrapping_sub(chunk_t0);
                // Per-chunk cost, so one capture shows both the slice
                // cost and (on the last chunk) the irreducible
                // footer+fsync+publish tail.
                {
                    let mut buf = [0u8; 96];
                    let mut n = write_prefix(&mut buf, 0, b"[kvw] disk flush chunk k=");
                    n = write_dec(&mut buf, n, u64::from(worker.disk_flush_chunks));
                    // 0 = records, 1 = run fence poll, 2 = manifest
                    // fence poll. Separates "records are slow" from
                    // "the device fence is slow" in one capture.
                    n = write_prefix(&mut buf, n, b" ph=");
                    n = write_dec(&mut buf, n, u64::from(phase));
                    n = write_prefix(&mut buf, n, b" ms=");
                    n = write_dec(&mut buf, n, chunk_ms);
                    n = write_prefix(&mut buf, n, b" mem=");
                    n = write_dec(&mut buf, n, u64::from(worker.disk_state.mem_count));
                    log_line(worker, &buf, n);
                }
                if matches!(outcome, Ok(Progress::InProgress { .. })) {
                    // Real forward progress. Reset the starvation
                    // counters exactly as the compaction step does — a
                    // healthy multi-step flush must never look like a
                    // stall to the watchdog.
                    worker.disk_degraded_steps = 0;
                    worker.disk_hard_faults = 0;
                    worker.disk_fence_steps = 0;
                    return; // one bounded storage action per step
                }
                worker.disk_flush_active = false;
                {
                    let ms = now_ms(worker).wrapping_sub(worker.disk_flush_t0);
                    let mut buf = [0u8; 96];
                    let mut n = write_prefix(&mut buf, 0, b"[kvw] disk flush end ok=");
                    n = write_dec(&mut buf, n, u64::from(outcome.is_ok()));
                    n = write_prefix(&mut buf, n, b" chunks=");
                    n = write_dec(&mut buf, n, u64::from(worker.disk_flush_chunks));
                    n = write_prefix(&mut buf, n, b" ms=");
                    n = write_dec(&mut buf, n, ms);
                    n = write_prefix(&mut buf, n, b" mem=");
                    n = write_dec(&mut buf, n, u64::from(worker.disk_state.mem_count));
                    n = write_prefix(&mut buf, n, b" runs=");
                    n = write_dec(&mut buf, n, u64::from(worker.disk_state.run_count));
                    log_line(worker, &buf, n);
                }
                let outcome = outcome.map(|_| ());
                match outcome {
                    Ok(()) => {
                        worker.disk_flush_wanted = false;
                        worker.disk_degraded_steps = 0;
                        worker.disk_hard_faults = 0;
                        worker.disk_fence_steps = 0;
                        worker.disk_flush_completions =
                            worker.disk_flush_completions.wrapping_add(1);
                    }
                    Err(StoreError::Backpressure) => {
                        // Refused before any I/O: the run set is full,
                        // or a capture/install/compaction holds the
                        // store. Run set full → merge in bounded steps
                        // and retry later; anything else just retries.
                        //
                        // This branch used to change NOTHING when the
                        // run set was not full — no counter, no log, no
                        // phase change — so a capture or install that
                        // never completed left the worker retrying a
                        // permanently-refused flush in total silence.
                        // It is counted and degraded-marked now, so the
                        // stall shows up on both channels.
                        worker.disk_flush_backpressure =
                            worker.disk_flush_backpressure.wrapping_add(1);
                        worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
                        if worker.disk_state.run_count as usize >= kv_store::disk_store::MAX_RUNS {
                            worker.disk_compact_active = true;
                            worker.disk_compact_cursor = 0;
                            // Space-driven merge, NOT a retention
                            // decision: run at the floor already in
                            // force so it reclaims nothing new. A full
                            // run set is a reason to merge; it is never
                            // a reason to destroy history.
                            worker.disk_compact_floor = worker.disk_state.compaction_floor;
                        }
                    }
                    Err(_) if worker.fs_storage.last_was_transient() => {
                        // Transient FS backpressure — retry the flush
                        // next step. `flush_inner` re-derives the same
                        // run id and `create_fresh` clears the orphan
                        // left by this attempt, so the retry is clean.
                        worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
                    }
                    Err(_) => {
                        // Hard fault. A failed flush publishes nothing:
                        // the memtable still holds every record and the
                        // last published manifest is untouched, so the
                        // durable state is exactly what recovery would
                        // find. Retry within budget — quarantining on
                        // the first device hiccup is a permanent,
                        // unrecoverable loss of the store.
                        worker.disk_flush_failures = worker.disk_flush_failures.wrapping_add(1);
                        worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
                        worker.disk_hard_faults = worker.disk_hard_faults.saturating_add(1);
                        if worker.disk_hard_faults >= DISK_HARD_FAULT_BUDGET {
                            log_disk_errno(worker, b"[kvw] disk QUARANTINE flush errno=");
                            worker.disk_phase = DISK_PHASE_QUARANTINED;
                        }
                    }
                }
            }
        }
        _ => {} // QUARANTINED: hold everything (fail closed)
    }
}

/// Bounded-starvation watchdog: enforce that no exclusion fence holds
/// off the write path for more than `DISK_FENCE_STALL_BUDGET` steps.
///
/// See that constant for the reasoning. In short: `apply` and `flush`
/// are refused with `Backpressure` while a capture, install or
/// compaction is active, and an abandoned install stream leaves that
/// fence latched with nothing left to clear it. Every fence is safely
/// abortable and every one of those operations is retryable, so the
/// fence loses the tie against serving writes.
///
/// The abort is loud — one log line naming which fences were held — and
/// counted, because reaching this point always means something upstream
/// (a snapshot stream, a compaction) failed to finish and that is worth
/// investigating even though the store recovers.
///
/// A CHUNKED FLUSH IS DELIBERATELY NOT A FENCE HERE, and this is the
/// load-bearing consequence of chunking. The three fences are watched
/// because they REFUSE `apply` — they stop the graph serving writes. A
/// flush no longer does: `apply` is accepted throughout, right up to
/// the memtable's capacity. So a flush in flight is not starving
/// anything and must not be counted against the stall budget, or this
/// watchdog would abort a perfectly healthy 12-chunk flush partway
/// through and orphan its run file every single time.
///
/// The flush is not unwatched, though. Each successful `flush_step`
/// resets `disk_degraded_steps` / `disk_hard_faults` /
/// `disk_fence_steps` exactly as a compaction step does, so a flush
/// that stops making progress still shows up as degraded steps and
/// still spends the hard-fault budget through the error arms below.
unsafe fn disk_fence_watchdog(worker: &mut WorkerState) {
    let held = {
        let store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        if !store.any_fence_active() {
            0u8
        } else {
            (u8::from(store.capture_active()))
                | (u8::from(store.install_active()) << 1)
                | (u8::from(store.compact_active()) << 2)
        }
    };
    if held == 0 {
        worker.disk_fence_steps = 0;
        return;
    }
    worker.disk_fence_steps = worker.disk_fence_steps.saturating_add(1);
    if worker.disk_fence_steps < DISK_FENCE_STALL_BUDGET {
        return;
    }
    {
        let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        store.abort_fences();
    }
    worker.disk_fence_steps = 0;
    worker.disk_fence_aborts = worker.disk_fence_aborts.wrapping_add(1);
    // An aborted install leaves the store EMPTY by REPLACE semantics,
    // so the position it was going to adopt must not be claimed and any
    // half-accumulated chunk stream is dropped.
    if held & 0b010 != 0 {
        worker.importing = false;
        worker.import_len = 0;
        worker.applied_term = 0;
        worker.applied_index = 0;
    }
    // A compaction abandoned mid-flight must not be resumed: its cursor
    // refers to a fence that no longer exists.
    if held & 0b100 != 0 {
        worker.disk_compact_active = false;
        worker.disk_compact_cursor = 0;
    }
    if !worker.syscalls.is_null() {
        let mut buf = [0u8; 96];
        let mut n = write_prefix(&mut buf, 0, b"[kvw] disk FENCE ABORT held=");
        n = write_dec(&mut buf, n, u64::from(held));
        n = write_prefix(&mut buf, n, b" (1=capture 2=install 4=compact) steps=");
        n = write_dec(&mut buf, n, u64::from(DISK_FENCE_STALL_BUDGET));
        dev_log(&*worker.syscalls, DISK_LOG_LEVEL, buf.as_ptr(), n);
    }
}

/// Disk state-store observability, on a WALL-CLOCK cadence, emitted at
/// the top of every step BEFORE any storage action is attempted.
///
/// Two channels, because they fail differently:
///
/// - **Metrics** (`metrics_out` → adapter_metrics → operations
///   `/metrics`). Per rig.md §5 the scrape survives a wedged request
///   path, so this is the reliable channel. Ids follow the manifest's
///   `[observability] metrics` order: 0-2 the pre-existing counters,
///   9-13 the disk counters, 3-8 the disk gauges.
/// - **A heartbeat log line**, unconditional whenever the disk store is
///   selected. Unconditional is the point: a line that only appears when
///   something is wrong cannot distinguish "healthy" from "the log
///   channel is dead" from "the module is blocked mid-step". With an
///   always-on heartbeat, ABSENT means the channel or the step loop is
///   dead, FROZEN means the module is blocked inside a step, and
///   ADVANCING means the numbers on it are the truth.
unsafe fn disk_observe(worker: &mut WorkerState) {
    if worker.syscalls.is_null() {
        return;
    }
    let sys = &*worker.syscalls;
    let now = dev_millis(sys);

    if now.wrapping_sub(worker.disk_observe_ms) >= DISK_OBSERVE_INTERVAL_MS {
        worker.disk_observe_ms = now;
        telemetry::emit_counters(
            sys,
            worker.metrics_out,
            &[worker.m_applied, worker.m_reads, worker.m_snapshot_chunks],
        );
        telemetry::emit_gauges(
            sys,
            worker.metrics_out,
            3,
            &[
                u64::from(worker.disk_phase),
                u64::from(worker.disk_degraded_steps),
                u64::from(worker.disk_hard_faults),
                // Metrics are unsigned and every errno here is <= 0, so
                // the magnitude is emitted and the sign is implied.
                // 0 = none, 11 = E_AGAIN, 2 = ENOENT, 5 = EIO,
                // 28 = ENOSPC, 38 = ENOSYS.
                errno_magnitude(worker.fs_storage.last_errno()),
                u64::from(worker.disk_state.mem_count),
                u64::from(worker.disk_state.run_count),
            ],
        );
        // Counters share the same declaration order, continuing at 9.
        emit_counters_at(
            sys,
            worker.metrics_out,
            9,
            &[
                worker.disk_flush_attempts,
                worker.disk_flush_completions,
                worker.disk_flush_failures,
                worker.disk_flush_backpressure,
                worker.disk_cmd_flush_faults,
                worker.disk_fence_aborts,
                worker.gc_floor_commits,
                worker.gc_claims_published,
            ],
        );
        emit_effective_config_gauges(
            sys,
            worker.metrics_out,
            worker.state_store,
            worker.root_path,
            worker.store_id,
        );
    }

    if now.wrapping_sub(worker.disk_heartbeat_ms) < DISK_HEARTBEAT_LOG_MS {
        return;
    }
    worker.disk_heartbeat_ms = now;
    // ph=phase fw=flush_wanted ca=compact_active deg=degraded_steps
    // hard=hard_faults errno=last_errno mem=memtable runs=run_files
    // fa/fc/ff/fb=flush attempts/completions/failures/backpressure
    let mut buf = [0u8; 288];
    // `ss`/`rp` are the EFFECTIVE params (rig.md §7a) — repeated on
    // every heartbeat so a capture that misses the init line still
    // proves which provider this worker actually selected.
    let mut n = write_prefix(&mut buf, 0, b"[kvw] disk hb ss=");
    n = write_dec(&mut buf, n, u64::from(worker.state_store));
    n = write_prefix(&mut buf, n, b" rp=");
    n = write_dec(&mut buf, n, u64::from(worker.root_path));
    n = write_prefix(&mut buf, n, b" ph=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_phase));
    n = write_prefix(&mut buf, n, b" fw=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_flush_wanted));
    n = write_prefix(&mut buf, n, b" ca=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_compact_active));
    n = write_prefix(&mut buf, n, b" deg=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_degraded_steps));
    n = write_prefix(&mut buf, n, b" hard=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_hard_faults));
    n = write_prefix(&mut buf, n, b" errno=");
    n = write_signed(&mut buf, n, worker.fs_storage.last_errno());
    n = write_prefix(&mut buf, n, b" mem=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_state.mem_count));
    n = write_prefix(&mut buf, n, b" runs=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_state.run_count));
    n = write_prefix(&mut buf, n, b" fa=");
    n = write_dec(&mut buf, n, worker.disk_flush_attempts);
    n = write_prefix(&mut buf, n, b" fc=");
    n = write_dec(&mut buf, n, worker.disk_flush_completions);
    n = write_prefix(&mut buf, n, b" ff=");
    n = write_dec(&mut buf, n, worker.disk_flush_failures);
    n = write_prefix(&mut buf, n, b" fb=");
    n = write_dec(&mut buf, n, worker.disk_flush_backpressure);
    n = write_prefix(&mut buf, n, b" cf=");
    n = write_dec(&mut buf, n, worker.disk_cmd_flush_faults);
    // Which exclusion fence (if any) is refusing writes right now, and
    // for how long — the direct readout of the starvation invariant.
    n = write_prefix(&mut buf, n, b" fence=");
    let store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
    let held = (u8::from(store.capture_active()))
        | (u8::from(store.install_active()) << 1)
        | (u8::from(store.compact_active()) << 2);
    n = write_dec(&mut buf, n, u64::from(held));
    n = write_prefix(&mut buf, n, b" fs=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_fence_steps));
    n = write_prefix(&mut buf, n, b" fab=");
    n = write_dec(&mut buf, n, worker.disk_fence_aborts);
    // CUMULATIVE lookup/storage totals.
    //
    // The per-drain line only fires above its threshold and, crucially,
    // a step that is KILLED never emits its own line — the numbers for
    // the fatal step are lost. These totals ride the 2 s heartbeat, so a
    // capture shows the TREND right up to the kill even though the last
    // step is silent. That is what makes "does `op` stay near zero" and
    // "does `sc`/`bl` climb with run count" answerable from a capture
    // that ends in a termination.
    //
    //   rd = storage read calls   op = FS_OPEN calls (cache misses)
    //   bl = run blocks loaded    sc = run scans opened
    //   sk = run scans the Bloom skipped
    //   cs = bounded compaction steps run
    let (rd, _rb, op) = worker.fs_storage.read_counters();
    n = write_prefix(&mut buf, n, b" rd=");
    n = write_dec(&mut buf, n, rd);
    n = write_prefix(&mut buf, n, b" op=");
    n = write_dec(&mut buf, n, op);
    n = write_prefix(&mut buf, n, b" bl=");
    n = write_dec(&mut buf, n, worker.disk_state.blocks_read.get());
    n = write_prefix(&mut buf, n, b" sc=");
    n = write_dec(&mut buf, n, worker.disk_state.scans_opened.get());
    n = write_prefix(&mut buf, n, b" sk=");
    n = write_dec(&mut buf, n, worker.disk_state.scans_skipped.get());
    n = write_prefix(&mut buf, n, b" cs=");
    n = write_dec(&mut buf, n, worker.disk_compact_steps);
    dev_log(sys, DISK_LOG_LEVEL, buf.as_ptr(), n);
}

/// Magnitude of a (non-positive) errno for unsigned metric transport.
fn errno_magnitude(e: i32) -> u64 {
    if e < 0 {
        (-(e as i64)) as u64
    } else {
        e as u64
    }
}

/// `telemetry::emit_counters` with a manifest id offset, so one module
/// can emit two disjoint counter ranges out of the same declaration
/// order (ids 0-2 and 9-13 here).
unsafe fn emit_counters_at(sys: &SyscallTable, chan: i32, base: u16, values: &[u64]) {
    if chan < 0 {
        return;
    }
    let me = dev_self_index(sys);
    if me < 0 {
        return;
    }
    let t = dev_micros(sys);
    let counter = abi::contracts::telemetry::METRIC_COUNTER;
    let mut i: u16 = 0;
    while (i as usize) < values.len() {
        dev_telemetry_metric(
            sys,
            chan,
            me as u16,
            t,
            counter,
            base + i,
            values[i as usize],
        );
        i += 1;
    }
}

/// Append `src` to `buf` at `at`, bounded; returns the new length.
fn write_prefix(buf: &mut [u8], at: usize, src: &[u8]) -> usize {
    let mut n = at;
    for &b in src {
        if n >= buf.len() {
            break;
        }
        buf[n] = b;
        n += 1;
    }
    n
}

/// Append `v` in decimal, bounded; returns the new length.
fn write_dec(buf: &mut [u8], at: usize, v: u64) -> usize {
    let mut digits = [0u8; 20];
    let mut d = 0usize;
    let mut v = v;
    loop {
        digits[d] = b'0' + (v % 10) as u8;
        d += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    let mut n = at;
    while d > 0 && n < buf.len() {
        d -= 1;
        buf[n] = digits[d];
        n += 1;
    }
    n
}

/// Append a possibly-negative errno in decimal; returns the new length.
fn write_signed(buf: &mut [u8], at: usize, v: i32) -> usize {
    let mut n = at;
    let mag = if v < 0 {
        if n < buf.len() {
            buf[n] = b'-';
            n += 1;
        }
        (-(v as i64)) as u64
    } else {
        v as u64
    };
    write_dec(buf, n, mag)
}

/// Log a static prefix plus the storage backend's last raw errno
/// (decimal, possibly negative) — quarantine diagnostics.
/// Monotonic MICROseconds, or 0 when the syscall table is unset. The
/// drain budget needs sub-millisecond resolution — at ms granularity a
/// 1500 us bound is unmeasurable.
unsafe fn now_us(worker: &WorkerState) -> u64 {
    if worker.syscalls.is_null() {
        return 0;
    }
    dev_micros(&*worker.syscalls)
}

/// Monotonic ms, or 0 when the syscall table is unset. Used only for
/// diagnostic durations, so a 0 is a missing measurement, never a
/// control-flow input.
unsafe fn now_ms(worker: &WorkerState) -> u64 {
    if worker.syscalls.is_null() {
        return 0;
    }
    dev_millis(&*worker.syscalls)
}

/// Null-checked `dev_log` at `DISK_LOG_LEVEL`.
unsafe fn log_line(worker: &WorkerState, buf: &[u8], n: usize) {
    if worker.syscalls.is_null() {
        return;
    }
    dev_log(&*worker.syscalls, DISK_LOG_LEVEL, buf.as_ptr(), n);
}

unsafe fn log_disk_errno(worker: &mut WorkerState, prefix: &[u8]) {
    if worker.syscalls.is_null() {
        return;
    }
    let mut buf = [0u8; 96];
    let mut n = write_prefix(&mut buf, 0, prefix);
    n = write_signed(&mut buf, n, worker.fs_storage.last_errno());
    dev_log(&*worker.syscalls, DISK_LOG_LEVEL, buf.as_ptr(), n);
}

/// Pull at most one `MSG_GC_FLOOR_COMMITTED` per tick off the
/// `durability` port and adopt it as the authorized reclamation floor.
///
/// **This is the only writer of `gc_committed_floor`, and
/// `gc_committed_floor` is the only thing that lets compaction advance
/// (§21 invariant 13).** Everything else in the retention path —
/// claims, candidates, operator config, the coordinator's proposal —
/// produces *inputs to a decision*. This produces the decision.
///
/// Monotone: a floor at-or-below the one we already hold is ignored,
/// because a floor that retreated would suggest history came back, and
/// it never does. Anything that does not decode as a well-formed
/// record is dropped entirely rather than partially believed.
unsafe fn drain_gc_floor(worker: &mut WorkerState) {
    let sys_ptr = worker.syscalls;
    if sys_ptr.is_null() || worker.durability_in < 0 {
        return;
    }
    let sys = &*sys_ptr;
    let poll = (sys.channel_poll)(worker.durability_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return;
    }
    let mut hdr = [0u8; 3];
    if (sys.channel_read)(worker.durability_in, hdr.as_mut_ptr(), 3) < 3 {
        return;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len == 0 || payload_len > 128 {
        return;
    }
    let mut buf = [0u8; 128];
    if ((sys.channel_read)(worker.durability_in, buf.as_mut_ptr(), payload_len) as usize)
        < payload_len
    {
        return;
    }
    if hdr[0] != MSG_GC_FLOOR_COMMITTED || payload_len != GC_FLOOR_WIRE_LEN {
        return; // other durability traffic, or a malformed record
    }
    let Some(rec) = GcFloorRecord::decode(&buf[..GC_FLOOR_WIRE_LEN]) else {
        return;
    };
    if rec.floor_revision > worker.gc_committed_floor {
        worker.gc_committed_floor = rec.floor_revision;
        worker.gc_floor_commits = worker.gc_floor_commits.wrapping_add(1);
    }
}

/// Publish this worker's §18 ACTIVE-READ retention claim.
///
/// Republished on a cadence rather than on change because the claim
/// carries a freshness bound: the coordinator must be able to tell "the
/// worker needs nothing older" from "the worker is gone", and only a
/// heartbeat distinguishes those. Going quiet therefore *blocks* floor
/// advancement rather than unblocking it — the fail-closed direction.
unsafe fn publish_retention_claim(worker: &mut WorkerState) {
    if worker.compaction_floor_out < 0 || worker.syscalls.is_null() {
        return;
    }
    if !worker.step_ctr.is_multiple_of(GC_CLAIM_INTERVAL_STEPS) {
        return;
    }
    let sys = &*worker.syscalls;
    let floor = match worker.gc_claim_mode {
        // Tighter claim: nothing below the last exported snapshot is
        // needed to reconstruct this range.
        GC_CLAIM_MODE_SNAPSHOT => worker.gc_snapshot_revision,
        // `pin` (default): retain everything. See the param docs.
        _ => 0,
    };
    worker.gc_claim_seq = worker.gc_claim_seq.wrapping_add(1);
    let claim = RetentionClaim {
        kpg_id: worker.gc_claim_kpg,
        source: CLAIM_SOURCE_ACTIVE_READ,
        claim_id: u64::from(worker.cluster_epoch),
        floor_revision: floor,
        expiry_unix_ms: dev_millis(sys).saturating_add(GC_CLAIM_TTL_MS),
        seq: worker.gc_claim_seq,
    };
    let mut body = [0u8; GC_CLAIM_WIRE_LEN];
    if claim.encode(&mut body).is_none() {
        return;
    }
    let total = 3 + GC_CLAIM_WIRE_LEN;
    if total > worker.scratch.len() {
        return;
    }
    worker.scratch[0] = MSG_RETENTION_CLAIM;
    worker.scratch[1] = (GC_CLAIM_WIRE_LEN & 0xFF) as u8;
    worker.scratch[2] = ((GC_CLAIM_WIRE_LEN >> 8) & 0xFF) as u8;
    worker.scratch[3..total].copy_from_slice(&body);
    let n = (sys.channel_write)(
        worker.compaction_floor_out,
        worker.scratch.as_mut_ptr(),
        total,
    );
    if n == total as i32 {
        worker.gc_claims_published = worker.gc_claims_published.wrapping_add(1);
    }
}

/// Pull at most one `MSG_PLACEMENT_EPOCH_EVENT` per tick and advance
/// the worker's `cluster_epoch`. Monotonic — a stale or duplicate
/// event is a no-op. Wire shape:
///   `[prev_epoch:u32 LE][new_epoch:u32 LE]`.
unsafe fn drain_epoch_events(worker: &mut WorkerState) {
    let sys_ptr = worker.syscalls;
    if sys_ptr.is_null() || worker.epoch_events_in < 0 {
        return;
    }
    let sys = &*sys_ptr;
    let poll = (sys.channel_poll)(worker.epoch_events_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return;
    }
    let mut hdr = [0u8; 3];
    if (sys.channel_read)(worker.epoch_events_in, hdr.as_mut_ptr(), 3) < 3 {
        return;
    }
    if hdr[0] != MSG_PLACEMENT_EPOCH_EVENT {
        return;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if !(8..=64).contains(&payload_len) {
        return;
    }
    let mut buf = [0u8; 64];
    if ((sys.channel_read)(worker.epoch_events_in, buf.as_mut_ptr(), payload_len) as usize)
        < payload_len
    {
        return;
    }
    let new_epoch = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if new_epoch > worker.cluster_epoch {
        worker.cluster_epoch = new_epoch;
    }
}

/// Largest state body this worker can move. Clustor's `durability`
/// caps a snapshot body at 16 KiB, but our own transfer buffer is the
/// tighter constraint.
const SNAPSHOT_BODY_MAX: usize = SCRATCH_BUF_SIZE - APP_SNAPSHOT_HDR;

/// Service one app-snapshot control message per tick.
///
/// Implements the app side of clustor RFC §2.1. All three inbound
/// opcodes share `snapshot_import`:
///   * `MSG_APP_SNAPSHOT_REQUEST` — encode current state and reply on
///     `snapshot_export` as a `MSG_APP_SNAPSHOT_CHUNK` stream.
///   * `MSG_APP_SNAPSHOT_RESET` — discard state; chunks follow.
///   * `MSG_APP_SNAPSHOT_CHUNK` — accumulate, and on `done` install.
///
/// Both directions fail CLOSED. A state too large to encode produces
/// no reply at all, which stalls compaction (log keeps growing — safe)
/// rather than shipping a truncated body that would silently lose keys
/// on the next restore. A malformed or short chunk stream leaves the
/// store freshly initialised rather than half-applied; the node then
/// catches up from the leader, which is the correct recovery.
unsafe fn drain_snapshot_ctl(worker: &mut WorkerState) {
    let sys_ptr = worker.syscalls;
    if sys_ptr.is_null() || worker.snapshot_import_in < 0 {
        return;
    }
    let sys = &*sys_ptr;
    let poll = (sys.channel_poll)(worker.snapshot_import_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return;
    }
    let mut hdr = [0u8; 3];
    if (sys.channel_read)(worker.snapshot_import_in, hdr.as_mut_ptr(), 3) < 3 {
        return;
    }
    let kind = hdr[0];
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if !(16..=SCRATCH_BUF_SIZE).contains(&payload_len) {
        return;
    }
    if ((sys.channel_read)(
        worker.snapshot_import_in,
        worker.scratch.as_mut_ptr(),
        payload_len,
    ) as usize)
        < payload_len
    {
        return;
    }
    let term = le_u64(&worker.scratch[0..8]);
    let index = le_u64(&worker.scratch[8..16]);

    match kind {
        MSG_APP_SNAPSHOT_REQUEST => {
            if worker.snapshot_export_out < 0 || worker.applied_index == 0 {
                // Nothing replicated applied yet — there is no position
                // we could honestly label a body with, so stay silent.
                return;
            }
            emit_snapshot_chunks(worker, sys);
        }
        MSG_APP_SNAPSHOT_RESET => {
            // Discard state up front so a chunk stream that never
            // completes can't leave a half-old, half-new store behind.
            if worker.state_store == STATE_STORE_DISK {
                if worker.disk_phase != DISK_PHASE_SERVING {
                    return; // fail closed: not safe to install yet
                }
                // Begin the provider's REPLACE install with an empty
                // first chunk: this destroys the on-disk
                // materialization now (the §17.1 semantics) exactly as
                // `store.init()` does for the memory table.
                let req = SnapshotRequest {
                    applied_index: index,
                    range_generation: worker.disk_state.range_generation,
                };
                let started = {
                    let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                    store.install(
                        req,
                        SnapshotChunk {
                            offset: 0,
                            data: &[],
                            last: false,
                        },
                    )
                };
                if started.is_err() {
                    // Busy (capture in flight) or storage fault: do not
                    // start accumulating a stream we cannot install.
                    return;
                }
            } else {
                worker.store.init();
            }
            worker.applied_term = term;
            worker.applied_index = index;
            worker.import_len = 0;
            worker.importing = true;
        }
        MSG_APP_SNAPSHOT_CHUNK => {
            if !worker.importing || payload_len < APP_SNAPSHOT_HDR {
                return;
            }
            let offset = le_u64(&worker.scratch[16..24]) as usize;
            let done = worker.scratch[24] != 0;
            let body_len = payload_len - APP_SNAPSHOT_HDR;
            // Strict in-order accumulation; a gap aborts the install.
            if offset != worker.import_len || offset + body_len > SNAPSHOT_BODY_MAX {
                worker.importing = false;
                worker.import_len = 0;
                // Release the provider's install fence too. Dropping
                // only the worker-side accumulator left `install.active`
                // latched inside the store with no code path remaining
                // that would ever clear it — and that fence refuses
                // BOTH `apply` and `flush`, so the write path was dead
                // from this line onward. The watchdog would eventually
                // catch it; releasing it at the source means the store
                // is never starved in the first place.
                if worker.state_store == STATE_STORE_DISK {
                    let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                    store.abort_fences();
                }
                worker.applied_term = 0;
                worker.applied_index = 0;
                return;
            }
            worker.import_buf[offset..offset + body_len]
                .copy_from_slice(&worker.scratch[APP_SNAPSHOT_HDR..payload_len]);
            worker.import_len = offset + body_len;
            if done {
                let total = worker.import_len;
                let installed = if worker.state_store == STATE_STORE_DISK {
                    // Finish the provider install begun at RESET: one
                    // final chunk carrying the whole accumulated body.
                    // The provider re-validates every record and
                    // aborts to an EMPTY store on any inconsistency.
                    let req = SnapshotRequest {
                        applied_index: worker.applied_index,
                        range_generation: worker.disk_state.range_generation,
                    };
                    let outcome = {
                        let mut store =
                            DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                        store.install(
                            req,
                            SnapshotChunk {
                                offset: 0,
                                data: &worker.import_buf[..total],
                                last: true,
                            },
                        )
                    };
                    if matches!(outcome, Ok(Progress::Done)) {
                        // Snapshot install: the state corresponds to
                        // the RESET's applied position and the WAL
                        // resumes AFTER it, so the revision counter
                        // continues from the snapshot's high-water —
                        // no replay window (floor == revision).
                        worker.disk_revision = worker.disk_state.highest_revision;
                        worker.disk_replay_floor = worker.disk_state.highest_revision;
                        true
                    } else {
                        false
                    }
                } else {
                    worker.store.snapshot_decode(&worker.import_buf[..total])
                };
                if installed {
                    // State now reflects the RESET's (term, index), which
                    // we already adopted; nothing further to do.
                } else {
                    // The failed install left the store EMPTY (both
                    // providers guarantee this), so there is no partial
                    // state to unwind — but we must not claim the
                    // snapshot's position.
                    worker.applied_term = 0;
                    worker.applied_index = 0;
                }
                worker.importing = false;
                worker.import_len = 0;
            }
        }
        _ => {}
    }
}

/// Encode the store and ship it as a `MSG_APP_SNAPSHOT_CHUNK` stream
/// labelled with the worker's applied position. Silent (no chunks at
/// all) if the state doesn't fit the export budget — see the
/// fail-closed note on `drain_snapshot_ctl`.
unsafe fn emit_snapshot_chunks(worker: &mut WorkerState, sys: &SyscallTable) {
    let mut body = [0u8; SNAPSHOT_BODY_MAX];
    let encoded = if worker.state_store == STATE_STORE_DISK {
        disk_snapshot_encode(worker, &mut body)
    } else {
        worker.store.snapshot_encode(&mut body)
    };
    let Some(total) = encoded else {
        return;
    };
    let term = worker.applied_term;
    let index = worker.applied_index;
    let mut sent = 0usize;
    loop {
        let chunk = (total - sent).min(SNAPSHOT_CHUNK_MAX);
        let done = sent + chunk == total;
        let payload = APP_SNAPSHOT_HDR + chunk;
        worker.scratch[0] = MSG_APP_SNAPSHOT_CHUNK;
        worker.scratch[1] = (payload & 0xFF) as u8;
        worker.scratch[2] = ((payload >> 8) & 0xFF) as u8;
        worker.scratch[3..11].copy_from_slice(&term.to_le_bytes());
        worker.scratch[11..19].copy_from_slice(&index.to_le_bytes());
        worker.scratch[19..27].copy_from_slice(&(sent as u64).to_le_bytes());
        worker.scratch[27] = u8::from(done);
        worker.scratch[28..31].copy_from_slice(&[0, 0, 0]);
        worker.scratch[31..31 + chunk].copy_from_slice(&body[sent..sent + chunk]);
        let n = (sys.channel_write)(
            worker.snapshot_export_out,
            worker.scratch.as_mut_ptr(),
            3 + payload,
        );
        if n != (3 + payload) as i32 {
            // Partial stream — the engine's offset check will reject
            // it and the next trigger re-requests.
            return;
        }
        // Single chokepoint: one per snapshot chunk successfully emitted.
        worker.m_snapshot_chunks = worker.m_snapshot_chunks.wrapping_add(1);
        sent += chunk;
        if done {
            // The engine revision this snapshot body represents. Under
            // `gc_claim_mode = snapshot` this becomes the worker's
            // active-read claim: nothing below it is needed to
            // reconstruct the range from this snapshot plus the WAL
            // tail. Recorded only on a COMPLETE stream — a partial one
            // reconstructs nothing.
            worker.gc_snapshot_revision = if worker.state_store == STATE_STORE_DISK {
                worker.disk_revision
            } else {
                worker.store.revision
            };
            return;
        }
    }
}

/// Drive the disk provider's bounded snapshot capture to completion
/// into `out` (the app-snapshot body buffer). Fails CLOSED like the
/// memory `snapshot_encode`: a state too large for the export budget
/// aborts the capture (releasing the provider's capture fence) and
/// returns `None` — no chunks at all, the WAL stays authoritative.
unsafe fn disk_snapshot_encode(worker: &mut WorkerState, out: &mut [u8]) -> Option<usize> {
    if worker.disk_phase != DISK_PHASE_SERVING {
        return None;
    }
    let req = SnapshotRequest {
        applied_index: worker.applied_index,
        range_generation: worker.disk_state.range_generation,
    };
    let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
    let mut total = 0usize;
    let mut cursor = 0u64;
    loop {
        match store.snapshot(req, cursor, &mut out[total..]) {
            Ok((n, Progress::Done)) => return Some(total + n),
            Ok((n, Progress::InProgress { cursor: c })) => {
                total += n;
                cursor = c;
                if n == 0 {
                    // No forward progress — buffer exhausted.
                    store.snapshot_abort();
                    return None;
                }
            }
            Err(_) => {
                store.snapshot_abort();
                return None;
            }
        }
    }
}

/// Read a little-endian u64 from an 8-byte slice.
fn le_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}
