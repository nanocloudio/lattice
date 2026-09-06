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
//! No step blocks unboundedly. Every action whose natural size is the
//! store's — not the command's — is a state machine driven one bounded
//! slice per step, and the command drain itself is time-budgeted:
//!
//! - **Flush** (`disk_maintenance`, or in-line when a command hits
//!   memtable `Backpressure`): `FLUSH_STEP_RECORDS` (32) records per
//!   step into the run file; the run and manifest durability barriers
//!   are submit/poll fences that span steps rather than block one.
//! - **Compaction**: `COMPACT_STEP_RECORDS` (32) input records per
//!   step; only when a command would otherwise fail (memtable AND run
//!   set full) is it driven to completion in-line, bounded by the
//!   provider's fixed logical capacity.
//! - **Version scan** (`KV_OP_SCAN_VERSIONS` — the page read behind
//!   the CDC pump, watch replay, and the model feed): a key-ordered
//!   walk of the whole span, filtered by revision, so its length is
//!   the span's and not the window's. The provider stops at
//!   `SCAN_STEP_BLOCKS` loaded blocks (or `SCAN_STEP_RECORDS` visited
//!   records) and keeps its position; the engine reports the pause as
//!   `KV_RESULT_PAUSED`; this module holds the command frame
//!   and re-applies it one slice per step until the page is complete,
//!   with other commands flowing between slices (`hold_command` /
//!   `drive_held_command`). One slot: a re-issue of the held request
//!   is adopted into it; a different pausable request waits on the
//!   channel, and the commands behind it wait with it.
//! - **Command drain**: `PER_TICK_DRAIN_BUDGET` commands or
//!   `DRAIN_BUDGET_US` per step, whichever first; the channel is the
//!   spill buffer.
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
use kv_store::disk_store::{
    DiskState, DiskStore, SNAP_FLAG_DISK_RESIDENT, SNAP_FORMAT_VERSION, SNAP_HDR_CLOCK_OFF,
    SNAP_HDR_LEN, SNAP_MAGIC,
};
use kv_store::{DiskMaterializer, KvStore, Materializer, MAX_KEYS};
use types::{
    KV_OP_APPEND, KV_OP_CAS, KV_OP_DECR, KV_OP_DELETE, KV_OP_EXISTS, KV_OP_FLUSH, KV_OP_GET,
    KV_OP_GET_AT, KV_OP_IDEMPOTENT, KV_OP_INCR, KV_OP_MGET, KV_OP_MSET, KV_OP_PREPEND, KV_OP_PUT,
    KV_OP_RANGE, KV_OP_SCAN, KV_OP_SCAN_AT, KV_OP_SCAN_VERSIONS, KV_OP_STRLEN, KV_OP_TXN,
    KV_OP_TXN_PREPARE, KV_OP_TXN_RECORD, KV_OP_TXN_RESOLVE, KV_RESULT_INTEGER, KV_RESULT_OK,
    KV_RESULT_PAUSED,
};
use wire::{
    APP_SNAPSHOT_HDR, MSG_APP_APPLIED_POS, MSG_APP_SNAPSHOT_CHUNK, MSG_APP_SNAPSHOT_DURABLE,
    MSG_APP_SNAPSHOT_INSTALLED, MSG_APP_SNAPSHOT_REQUEST, MSG_APP_SNAPSHOT_RESET,
    MSG_APP_SNAPSHOT_SPAN_REQUEST, MSG_GC_FLOOR_COMMITTED, MSG_KV_APPLIED, MSG_KV_COMMAND,
    MSG_LEASE_TICK, MSG_PLACEMENT_EPOCH_EVENT, MSG_RETENTION_CLAIM, MSG_TS_LEASE_GRANT,
    MSG_TS_LEASE_REQUEST, MSG_TTL_CLOCK_RESUME, MSG_TTL_REGISTER, MSG_WATCH_EVENT,
    TS_LEASE_GRANT_WIRE_LEN,
};

#[path = "../../common/compaction_floor.rs"]
mod compaction_floor;
use compaction_floor::{
    GcFloorRecord, RetentionClaim, CLAIM_SOURCE_ACTIVE_READ, GC_CLAIM_WIRE_LEN, GC_FLOOR_WIRE_LEN,
};

const SCRATCH_BUF_SIZE: usize = 8192;

/// Largest command frame the hold slot keeps across steps. The only
/// pausable op, `KV_OP_SCAN_VERSIONS`, carries two user keys
/// (`MAX_KEY_LEN` each) and four integers behind the command head;
/// 1024 covers that with room, and is what keeps the slot cheap enough
/// to live in the arena unconditionally.
const HELD_FRAME_MAX: usize = 1024;

// ── MVCC commit-timestamp assignment ────────────────────

/// Does this op consume one commit timestamp from the worker lease?
/// The mutating command set, minus the cross-range transaction ops:
/// resolve stamps its versions from the transaction's own commit
/// timestamp (carried in the resolve record), and prepare/record write
/// only staged intents / txn records, which are not user-visible
/// versions. One timestamp per COMMAND — a multi-key MSET or a TXN's
/// sub-writes share it. FLUSH is included: it deletes every key as
/// versioned tombstones, so it produces user-visible versions and
/// consumes a timestamp like any multi-key mutation.
const fn op_consumes_timestamp(op: u8) -> bool {
    matches!(
        op,
        KV_OP_PUT
            | KV_OP_DELETE
            | KV_OP_INCR
            | KV_OP_DECR
            | KV_OP_APPEND
            | KV_OP_PREPEND
            | KV_OP_MSET
            | KV_OP_CAS
            | KV_OP_TXN
            | KV_OP_FLUSH
            | KV_OP_IDEMPOTENT
    )
}

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

/// Run count at which the SERVING phase arms a space-driven background
/// merge (same merge a full run set forces, same floor — pure run-count
/// hygiene). Bounds the READ side: a version scan walks every run's
/// blocks in one step, so per-scan cost scales with run count and an
/// unmerged store eventually crosses the module step deadline (measured
/// on the pi5 rig: terminated worker, "storage unavailable"). Three
/// keeps steady-state scans at one-or-two runs plus the memtable.
const RUN_MERGE_THRESHOLD: usize = 3;

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

    // Which disk store this worker instance owns. Multi-range graphs
    // run one worker per range in one process; each
    // needs its own file namespace: 0 = the historical `kv/`,
    // 1..=9 = `kv<id>/`. Refused at init on the FAT32 root layout
    // (root_path = 1) with a non-zero id — two stores at the FS root
    // would silently share file names.
    5, store_id, u8, 0
        => |s, d, len| { s.store_id = p_u8(d, len, 0, 0); };

    // KPG id stamped on the retention claim this worker publishes.
    4, gc_claim_kpg, u16, 0
        => |s, d, len| { s.gc_claim_kpg = p_u16(d, len, 0, 0); };

    // The Raft partition this worker serves. Stamped into every applied-
    // position report so the router's linearizable-read fence can track a
    // per-partition applied index and satisfy a read against the SAME
    // partition it must observe — not the max across a dense host's
    // partitions. Default 0 for single-partition graphs.
    6, partition_id, u16, 0
        => |s, d, len| { s.partition_id = p_u16(d, len, 0, 0); };
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
    expiry_out: i32,
    /// Elastic-split install-complete ack (index 7); -1 unless wired.
    install_ack_out: i32,

    /// The one clock this state machine has: the millisecond reading
    /// carried by the most recent committed `MSG_LEASE_TICK`, which
    /// arrives in-band on `commands` and is therefore ordered against
    /// the mutations around it. Every TTL decision — deadline
    /// assignment on a PUT, expiry filtering on a read, the
    /// reclamation sweep — is taken against this value, so two
    /// replicas applying the same log reach the same answer, and a
    /// replay reaches it again.
    ///
    /// Monotone: a tick that would move it backwards is ignored. Zero
    /// until the first tick commits, which is the correct reading for
    /// a log that has not yet established a time: no deadline has
    /// passed.
    committed_now_ms: u64,
    /// Slot the next reclamation sweep resumes from.
    reap_cursor: u32,
    /// A snapshot install has left the scheduler owed a clock frontier
    /// (`MSG_TTL_CLOCK_RESUME`). Cleared once the write is accepted;
    /// retried every step until then, because until the scheduler has
    /// it, every tick it proposes is below the restored frontier and is
    /// discarded, and cluster time stops moving.
    clock_resume_pending: bool,
    /// Slot the incremental expiry-queue rebuild resumes from.
    /// `>= MAX_KEYS` means the rebuild is complete.
    ttl_resume_cursor: u32,
    /// Slots the latched sweep still owes a visit. An expiry event sets
    /// it to the table size; each step spends at most `REAP_BUDGET` of
    /// it. Until it reaches zero the sweep continues on its own, so a
    /// single expiry event reclaims every record the committed clock
    /// has put past its deadline rather than one arbitrary window of
    /// the table.
    reap_remaining: u32,

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
    /// Raft partition served (param 6); stamped into applied-position
    /// reports for the router's per-partition lin-read fence.
    partition_id: u16,
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
    /// A command the engine paused (`KV_RESULT_PAUSED`) is held here
    /// and re-applied one bounded slice per step until it answers.
    held_active: bool,
    held_len: u16,
    /// Version-scan slices that paused (cumulative), and frames the
    /// hold slot could not take because they were oversized.
    m_scan_pauses: u64,
    m_scan_hold_refusals: u64,
    /// Re-issued requests adopted into the hold slot (`adopt_retry`).
    m_scan_retries_adopted: u64,
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
    /// Engine revision certified DURABLE by the last acknowledged snapshot
    /// — the tighter active-read claim under `gc_claim_mode = snapshot`.
    /// Advanced only on `MSG_APP_SNAPSHOT_DURABLE`, never on local export.
    gc_snapshot_revision: u64,
    /// The raft index a complete-but-not-yet-acknowledged export labeled,
    /// and the engine revision it represents. Promoted into
    /// `gc_snapshot_revision` when a durable ack at or past this index
    /// arrives; a superseding export overwrites the pair. Zero index means
    /// nothing awaits acknowledgement.
    gc_snapshot_pending_index: u64,
    gc_snapshot_pending_revision: u64,
    gc_claim_seq: u32,
    gc_claim_mode: u8,
    gc_claim_kpg: u16,
    _gc_pad: u8,

    // ── MVCC commit-timestamp assignment ────────────
    /// Next unassigned commit timestamp from the current in-band
    /// worker lease, and the lease's exclusive end. Assignment order
    /// IS committed-log order (grants and commands share the commands
    /// channel), so every replica and every replay assigns the same
    /// timestamp to the same command — this is derived-from-log state,
    /// deliberately not persisted anywhere.
    ts_lease_next: u64,
    ts_lease_end: u64,
    /// Output port for demand-driven lease requests to the allocator
    /// (unreplicated hint; the grant comes back replicated, in-band).
    lease_request_out: i32,
    /// Step stamp of the last request sent, for bounded retry.
    ts_request_step: u64,
    /// Free-running step counter (drives the retry cadence).
    step_ctr_ts: u64,
    /// The range's commit-timestamp frontier: highest
    /// commit timestamp bound to any applied write, monotone by
    /// construction with in-band lease assignment. Reported on every
    /// `MSG_KV_APPLIED` head; derived from the log, never persisted.
    ts_frontier: u64,
    /// Write commands stamped with a real timestamp (manifest id 20).
    m_ts_stamped: u64,
    /// Write commands applied with no timestamp available — lease
    /// exhausted or never granted (manifest id 21). Non-zero under
    /// steady load means the allocator cadence is undersized.
    m_ts_unstamped: u64,
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
    /// App-snapshot captures REFUSED (no chunks emitted): a memory-store
    /// body over the export budget, or a disk store with nothing durably
    /// covered yet. The WAL stays authoritative either way; this counter
    /// is the accounted capacity denial that makes the refusal visible
    /// (durability's side sees only a capture timeout).
    m_snapshot_refusals: u64,
    snap_refusal_logged: bool,
    step_ctr: u64,

    import_buf: [u8; SNAPSHOT_BODY_MAX],

    held_frame: [u8; HELD_FRAME_MAX],

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
        self.expiry_out = -1;
        self.install_ack_out = -1;
        self.committed_now_ms = 0;
        self.reap_cursor = 0;
        self.reap_remaining = 0;
        self.clock_resume_pending = false;
        self.ttl_resume_cursor = MAX_KEYS as u32;
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
        self.m_snapshot_refusals = 0;
        self.snap_refusal_logged = false;
        self.step_ctr = 0;
        self.state_store = STATE_STORE_MEMORY;
        self.root_path = 0;
        self.store_id = 0;
        self.partition_id = 0;
        self.disk_phase = DISK_PHASE_RECOVERING;
        self.disk_revision = 0;
        self.disk_replay_floor = 0;
        self.disk_flush_wanted = false;
        self.disk_flush_active = false;
        self.disk_flush_chunks = 0;
        self.disk_compact_steps = 0;
        self.disk_flush_t0 = 0;
        self.held_active = false;
        self.held_len = 0;
        self.m_scan_pauses = 0;
        self.m_scan_hold_refusals = 0;
        self.m_scan_retries_adopted = 0;
        self.disk_compact_active = false;
        self.disk_compact_cursor = 0;
        self.disk_compact_floor = 0;
        self.gc_committed_floor = 0;
        self.gc_floor_commits = 0;
        self.gc_claims_published = 0;
        self.gc_snapshot_revision = 0;
        self.gc_snapshot_pending_index = 0;
        self.gc_snapshot_pending_revision = 0;
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
        self.ts_lease_next = 0;
        self.ts_lease_end = 0;
        self.lease_request_out = -1;
        self.ts_request_step = 0;
        self.step_ctr_ts = 0;
        self.ts_frontier = 0;
        self.m_ts_stamped = 0;
        self.m_ts_unstamped = 0;
        self.store.init();
        self.disk_state.init();
    }
}

/// May a command be applied to the store right now? Shared by the
/// channel drain and the held-command re-drive, so a paused command
/// is never advanced through a state a fresh one would wait out.
fn store_serving(worker: &WorkerState) -> bool {
    if worker.state_store == STATE_STORE_DISK && worker.disk_phase != DISK_PHASE_SERVING {
        // Fail closed: a disk store that has not recovered (or has
        // quarantined on a storage fault) never serves. Commands stay
        // queued so backpressure propagates upstream.
        return false;
    }
    if worker.importing {
        // A snapshot install (boot restore or leader catch-up) is mid-
        // stream: the ctl channel drains ONE frame per step, so the
        // WAL-tail commands that follow the snapshot may already be
        // queued here. Hold them (backpressure) until the install
        // completes — applying a tail command before the adoption
        // re-seeds the revision clock would materialize it below the
        // store's high-water.
        return false;
    }
    true
}

unsafe fn drain_commands(worker: &mut WorkerState) -> bool {
    let sys_ptr = worker.syscalls;
    if sys_ptr.is_null() || worker.commands_in < 0 {
        return false;
    }
    if !store_serving(worker) {
        return false;
    }
    let sys = &*sys_ptr;
    let poll = (sys.channel_poll)(worker.commands_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    if worker.held_active && head_is_pausable(worker) {
        // Another pausable command at the head while the slot is
        // taken. The common case is a RETRY of the held request — the
        // caller's phase timeout passed while the walk was still
        // paging, so it re-issued the same request under a new
        // correlation. Adopt it: same bytes, new identity, and the
        // reply goes where it is still wanted. A genuinely different
        // request waits its turn on the channel; ordinary commands
        // behind it wait with it (the channel is FIFO), the price of a
        // single slot, paid only while two different version scans
        // overlap.
        return adopt_retry(worker);
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
            if worker.state_store == STATE_STORE_DISK {
                // Stamp the store's pending applied position: every
                // command ahead of this marker is in the memtable, so
                // the NEXT flush's frozen prefix covers exactly this
                // position and the manifest publish records it.
                worker.disk_state.pending_applied_index = index;
                worker.disk_state.pending_applied_term = term;
            }
        }
        return true;
    }
    if hdr[0] == MSG_TS_LEASE_GRANT {
        // In-band replicated worker lease, forwarded by
        // lattice_apply_bridge in committed-log order:
        // `[corr_id:u64 = 0][TimestampLease:40]`. Arriving HERE, on the
        // commands channel, is the contract — the lease takes effect
        // exactly between the commands it was committed between, so
        // per-write timestamp assignment below is a pure function of
        // the log on every replica and every replay.
        if payload_len != TS_LEASE_GRANT_WIRE_LEN || payload_len > SCRATCH_BUF_SIZE {
            return false;
        }
        let mut grant = [0u8; TS_LEASE_GRANT_WIRE_LEN];
        if ((sys.channel_read)(
            worker.commands_in,
            grant.as_mut_ptr(),
            TS_LEASE_GRANT_WIRE_LEN,
        ) as usize)
            < TS_LEASE_GRANT_WIRE_LEN
        {
            return false;
        }
        // TimestampLease wire (mvcc.rs): version:u16 @0, payload_len:u16
        // @2, interval_start:u64 @12, interval_end:u64 @20 — offsets
        // within the 40-byte lease, which begins at byte 8 of the frame.
        let lease = &grant[8..];
        let version = u16::from_le_bytes([lease[0], lease[1]]);
        let start = u64::from_le_bytes([
            lease[12], lease[13], lease[14], lease[15], lease[16], lease[17], lease[18], lease[19],
        ]);
        let end = u64::from_le_bytes([
            lease[20], lease[21], lease[22], lease[23], lease[24], lease[25], lease[26], lease[27],
        ]);
        // Adopt only forward: a well-formed successor lease always
        // starts at or above the previous one's end (allocator §10
        // succession), so anything else is a duplicate or corruption
        // and is dropped rather than rewound — a timestamp domain must
        // never move backwards.
        if version == 1 && start < end && start >= worker.ts_lease_end {
            // Timestamp 0 is the "no timestamp" sentinel; a lease that
            // covers it starts issuing at 1.
            worker.ts_lease_next = start.max(1);
            worker.ts_lease_end = end;
        }
        return true;
    }
    if hdr[0] == MSG_LEASE_TICK {
        // The authoritative clock, arriving on the SAME channel as the
        // commands it orders against: `[tick_ms:u64]`, proposed by
        // `ttl_scheduler` and delivered here only after it committed.
        // Reading it anywhere else — a local timer, a side channel —
        // would give each replica its own notion of now, which is the
        // one thing a replicated TTL cannot have.
        if payload_len != 8 || payload_len > SCRATCH_BUF_SIZE {
            return false;
        }
        let mut tick = [0u8; 8];
        if ((sys.channel_read)(worker.commands_in, tick.as_mut_ptr(), 8) as usize) < 8 {
            return false;
        }
        let tick_ms = u64::from_le_bytes(tick);
        if tick_ms > worker.committed_now_ms {
            worker.committed_now_ms = tick_ms;
        }
        // The disk provider persists the frontier with its manifest, so
        // a store recovered from runs alone resumes at the clock its
        // records' deadlines were written against.
        worker.disk_state.committed_clock_ms = worker.committed_now_ms;
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

    match apply_command_frame(worker, &in_buf[..payload_len]) {
        CmdOutcome::Answered(ok) => ok,
        CmdOutcome::Refused => false,
        CmdOutcome::Paused => {
            hold_command(worker, &in_buf[..payload_len]);
            true
        }
    }
}

/// What applying one `MSG_KV_COMMAND` frame came to.
enum CmdOutcome {
    /// Applied and answered (`true` = the reply was written).
    Answered(bool),
    /// Malformed or undeliverable; dropped without a reply.
    Refused,
    /// The engine returned `KV_RESULT_PAUSED`: the command consumed its
    /// step budget mid-reply and the provider holds its position. The
    /// frame must be re-applied, byte for byte, on a later step.
    Paused,
}

/// Apply one `MSG_KV_COMMAND` frame (`wire::KvCommandHead` + op body)
/// to the store and write its `MSG_KV_APPLIED` reply. Called from the
/// channel drain for a fresh frame and from [`drive_held_command`] for
/// a paused one — the same bytes either way, which is what lets the
/// provider resume: it recognises the request it paused on.
unsafe fn apply_command_frame(worker: &mut WorkerState, cmd: &[u8]) -> CmdOutcome {
    let sys = &*worker.syscalls;
    let payload_len = cmd.len();
    let Some(head) = wire::KvCommandHead::decode(cmd) else {
        return CmdOutcome::Refused;
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
        return CmdOutcome::Refused;
    }
    let body = &cmd[body_off..body_off + body_len];

    let mut result_body = [0u8; SCRATCH_BUF_SIZE];
    // The committed clock. `kv_store::apply` uses this as `now_ms` for
    // absolute-deadline assignment and expiry filtering, so a deadline
    // is a function of the log position that set it and never of the
    // applying node's local timer.
    let now_ms = worker.committed_now_ms;
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
    // MVCC commit-timestamp assignment. Precedence:
    // a non-zero head timestamp (stamped into the committed bytes by
    // the proposer) wins; otherwise mutating commands consume the next
    // timestamp from the in-band worker lease, in log order. The
    // cross-range resolve path is deliberately NOT in the consuming
    // set: its versions carry the transaction's own commit timestamp
    // from the resolve record (`apply_txn_resolve` re-binds it), and
    // prepare's staged intents are not user-visible versions.
    let commit_ts = if head.commit_ts != 0 {
        head.commit_ts
    } else if op_consumes_timestamp(op) {
        if worker.ts_lease_next != 0 && worker.ts_lease_next < worker.ts_lease_end {
            let ts = worker.ts_lease_next;
            worker.ts_lease_next += 1;
            worker.m_ts_stamped = worker.m_ts_stamped.wrapping_add(1);
            ts
        } else {
            worker.m_ts_unstamped = worker.m_ts_unstamped.wrapping_add(1);
            0
        }
    } else if op == KV_OP_TXN_RESOLVE && body.len() >= 45 {
        // The resolve record carries the transaction's own commit
        // timestamp at body[37..45]; the engine re-binds it per staged
        // op (`apply_txn_resolve`). Peeked here only so the frontier
        // accounts for it.
        u64::from_le_bytes([
            body[37], body[38], body[39], body[40], body[41], body[42], body[43], body[44],
        ])
    } else {
        0
    };
    if commit_ts > worker.ts_frontier {
        worker.ts_frontier = commit_ts;
    }
    // ONE semantic engine (`kv_store::apply_mat_ctx`), provider selected
    // at graph construction: the interpreter is identical on both arms —
    // only the physical materializer differs.
    let (result, result_body_len, result_revision) = if worker.state_store == STATE_STORE_DISK {
        let store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        let mut mat = DiskMaterializer::new(store, worker.disk_revision, worker.disk_replay_floor);
        mat.flush_wanted = worker.disk_flush_wanted;
        mat.set_key_identity(id_tenant, id_database, id_keyspace);
        mat.set_commit_ts(commit_ts);
        let (r, n) = kv_store::apply_mat_ctx(&mut mat, op, body, &mut result_body, now_ms, policy);
        worker.disk_revision = mat.revision;
        worker.disk_flush_wanted = mat.flush_wanted;
        // Latch both outcome flags before any `worker` field is touched
        // again: `mat` mutably borrows `worker.disk_state`, so reading
        // them up front is what releases the borrow.
        let mat_backpressure = mat.backpressure;
        let mat_fault = mat.fault;
        let mat_reply_revision = mat.reply_revision;
        if r == KV_RESULT_PAUSED {
            return CmdOutcome::Paused;
        }
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
        // A historical read answers as-of the revision it pinned, not
        // the one the engine has reached since.
        let as_of = if mat_reply_revision != 0 {
            mat_reply_revision
        } else {
            worker.disk_revision
        };
        (r, n, as_of)
    } else {
        worker
            .store
            .set_key_identity(id_tenant, id_database, id_keyspace);
        worker.store.set_commit_ts(commit_ts);
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
    if op == KV_OP_PUT && result == KV_RESULT_OK {
        publish_expiry_registration(
            worker,
            body,
            now_ms,
            &kv_store::ident_bytes(id_tenant, id_database, id_keyspace),
        );
    }
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
        return CmdOutcome::Refused;
    }
    let total = 3 + resp_payload_len;
    if total > worker.scratch.len() {
        return CmdOutcome::Refused;
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
        commit_frontier: worker.ts_frontier,
        body_len: result_body_len as u16,
    }
    .encode(&mut scratch[3..]);
    let mut p = 3 + wire::KvAppliedHead::LEN;
    scratch[p..p + result_body_len].copy_from_slice(&result_body[..result_body_len]);
    p += result_body_len;
    scratch[p..p + 8].copy_from_slice(&worker.catalog_generation.to_le_bytes());

    if worker.responses_out < 0 {
        return CmdOutcome::Refused;
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
    CmdOutcome::Answered(ok)
}

/// Keep a paused command's frame for re-application. One slot: the
/// drain refuses to admit a second pausable command while it is
/// occupied (see [`drain_commands`]), so occupancy is never a race.
unsafe fn hold_command(worker: &mut WorkerState, cmd: &[u8]) {
    // A frame that does not fit cannot be held; the caller's page
    // request times out upstream and is re-issued (the provider may
    // have parked a page prefix before the refusal — harmless, since a
    // re-issue with the same window and cursor reclaims it). Bounded
    // by construction: the only pausable op carries two user keys and
    // four integers.
    if cmd.len() > HELD_FRAME_MAX {
        worker.m_scan_hold_refusals = worker.m_scan_hold_refusals.wrapping_add(1);
        return;
    }
    worker.held_frame[..cmd.len()].copy_from_slice(cmd);
    worker.held_len = cmd.len() as u16;
    worker.held_active = true;
    worker.m_scan_pauses = worker.m_scan_pauses.wrapping_add(1);
}

/// The frame at the channel head is pausable and the slot is taken:
/// consume it if it is the held request re-issued (identical but for
/// `corr_id`), replacing the held identity. Otherwise leave it.
unsafe fn adopt_retry(worker: &mut WorkerState) -> bool {
    let sys = &*worker.syscalls;
    const CORR: core::ops::Range<usize> = 0..8;
    let held_len = worker.held_len as usize;
    let mut frame = [0u8; 3 + HELD_FRAME_MAX];
    let n = (sys.channel_peek)(worker.commands_in, frame.as_mut_ptr(), 3 + held_len);
    if n != (3 + held_len) as i32 {
        return false;
    }
    let payload_len = u16::from_le_bytes([frame[1], frame[2]]) as usize;
    let cand = &frame[3..3 + held_len];
    let held = &worker.held_frame[..held_len];
    let same = payload_len == held_len && cand[CORR.end..] == held[CORR.end..];
    if !same {
        return false;
    }
    // Consume it (header + payload) and adopt its correlation.
    let mut sink = [0u8; 3 + HELD_FRAME_MAX];
    let got = (sys.channel_read)(worker.commands_in, sink.as_mut_ptr(), 3 + held_len);
    if got != (3 + held_len) as i32 {
        return false;
    }
    worker.held_frame[CORR].copy_from_slice(&sink[3 + CORR.start..3 + CORR.end]);
    worker.m_scan_retries_adopted = worker.m_scan_retries_adopted.wrapping_add(1);
    true
}

/// Re-apply the held command — one bounded slice of its walk per
/// step. Runs at the head of every drain, before any fresh command, so
/// a paused scan is never starved by the queue behind it, and other
/// commands keep flowing between its slices (replies correlate by
/// `corr_id`; nothing upstream assumes reply order).
unsafe fn drive_held_command(worker: &mut WorkerState) -> bool {
    if !worker.held_active || !store_serving(worker) {
        return false;
    }
    let len = worker.held_len as usize;
    let mut frame = [0u8; HELD_FRAME_MAX];
    frame[..len].copy_from_slice(&worker.held_frame[..len]);
    match apply_command_frame(worker, &frame[..len]) {
        CmdOutcome::Paused => {}
        CmdOutcome::Answered(_) | CmdOutcome::Refused => worker.held_active = false,
    }
    true
}

/// Is the frame at the head of `commands` a command that may pause?
/// Peeked, not read: while a paused command is held, its successor of
/// the same kind stays on the channel — the channel is the spill
/// buffer — until the slot frees. Every other frame is admitted.
unsafe fn head_is_pausable(worker: &mut WorkerState) -> bool {
    let sys = &*worker.syscalls;
    const PEEK: usize = 3 + wire::KvCommandHead::LEN;
    let mut hdr = [0u8; PEEK];
    let n = (sys.channel_peek)(worker.commands_in, hdr.as_mut_ptr(), PEEK);
    if n < PEEK as i32 || hdr[0] != MSG_KV_COMMAND {
        return false;
    }
    wire::KvCommandHead::decode(&hdr[3..]).is_some_and(|h| h.op == KV_OP_SCAN_VERSIONS)
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
        worker.lease_request_out = dev_channel_port(sys, 1, 5);
        worker.expiry_out = dev_channel_port(sys, 1, 6);
        worker.install_ack_out = dev_channel_port(sys, 1, 7);
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

/// Number of slots one reclamation sweep visits.
const REAP_BUDGET: usize = 64;

/// `ttl_scheduler`'s expiry-event kinds (`common/ttl_scheduler.rs`).
/// Only the KV kind names a record here; a lease expiry is the lease
/// manager's business and reaches this worker as a revoke instead.
const EXPIRY_KIND_KV: u8 = 0x02;

/// Register (or cancel) a KV record's expiry with `ttl_scheduler`:
/// `[deadline_ms:u64][key_hash:u64]`, deadline `0` = cancel. The hash
/// is taken over the CANONICAL key (§23 identity prefix + user key),
/// which is both what keeps two tenants' entries apart and what lets
/// the resulting expiry event name the record's slot.
///
/// The deadline is computed from the committed clock, so every replica
/// registers the same value from the same log position. The queue that
/// receives it schedules reclamation; visibility is decided by the
/// record's own deadline at apply time, never by this message arriving.
unsafe fn publish_expiry_registration(
    worker: &mut WorkerState,
    body: &[u8],
    now_ms: u64,
    ident: &[u8; kv_store::IDENT_LEN],
) {
    if worker.expiry_out < 0 || worker.syscalls.is_null() {
        return;
    }
    let Some((key_hash, ttl_ms)) = kv_store::put_ttl_registration(body, ident) else {
        return;
    };
    if ttl_ms == 0 {
        // Nothing to schedule, and nothing to withdraw either: a queue
        // entry left over from an earlier TTL on this key fires a sweep
        // that finds the record unexpired and frees nothing. Staying
        // silent here keeps the ordinary write off this path entirely.
        return;
    }
    let deadline = now_ms.saturating_add(ttl_ms);
    let sys = &*worker.syscalls;
    let mut frame = [0u8; 3 + 16];
    frame[0] = MSG_TTL_REGISTER;
    frame[1] = 16;
    frame[2] = 0;
    frame[3..11].copy_from_slice(&deadline.to_le_bytes());
    frame[11..19].copy_from_slice(&key_hash.to_le_bytes());
    let _ = (sys.channel_write)(worker.expiry_out, frame.as_mut_ptr(), frame.len());
}

/// Re-registrations emitted per step while rebuilding the scheduler's
/// expiry queue. A bound on work, not on how many records may carry a
/// deadline: the rebuild resumes from its cursor on the next step.
const TTL_RESUME_BUDGET: usize = 32;

/// Arm the post-install rebuild of everything about expiry that lives
/// outside this worker.
///
/// A snapshot restores the store and — since the frontier travels in
/// its header — this worker's clock. It restores nothing at all in
/// `ttl_scheduler`, whose `now_ms` and expiry queue are arena-only.
/// Left alone, the scheduler would resume from zero, propose ticks far
/// below the frontier just restored, have every one of them discarded
/// as backwards, and hold cluster time still for as long as the
/// previous incarnation had been up. So the worker hands it both
/// halves: the frontier first, then the deadlines.
fn resume_expiry_service(worker: &mut WorkerState) {
    worker.clock_resume_pending = true;
    // The disk provider reclaims through compaction and keeps no
    // slot-addressed arena to walk; only the memory store rebuilds a
    // queue.
    worker.ttl_resume_cursor = if worker.state_store == STATE_STORE_DISK {
        MAX_KEYS as u32
    } else {
        0
    };
    // Reclaim anything the restored frontier has already put past its
    // deadline, without waiting for the rebuilt queue to say so.
    worker.reap_remaining = MAX_KEYS as u32;
}

/// Drive the post-install rebuild forward by one step's worth.
///
/// Every write is offered again next step if the channel refuses it —
/// the frontier because the clock does not restart without it, and the
/// registrations because the queue is the only thing that will ever
/// schedule those records for reclamation.
unsafe fn drive_expiry_resume(worker: &mut WorkerState) {
    if !worker.clock_resume_pending && worker.ttl_resume_cursor >= MAX_KEYS as u32 {
        return;
    }
    if worker.expiry_out < 0 || worker.syscalls.is_null() {
        // No scheduler is wired to this graph; there is nothing to
        // rebuild and nobody to retain the work for.
        worker.clock_resume_pending = false;
        worker.ttl_resume_cursor = MAX_KEYS as u32;
        return;
    }
    let sys = &*worker.syscalls;
    if worker.clock_resume_pending {
        let mut frame = [0u8; 3 + 8];
        frame[0] = MSG_TTL_CLOCK_RESUME;
        frame[1] = 8;
        frame[2] = 0;
        frame[3..11].copy_from_slice(&worker.committed_now_ms.to_le_bytes());
        if (sys.channel_write)(worker.expiry_out, frame.as_mut_ptr(), frame.len())
            != frame.len() as i32
        {
            return;
        }
        worker.clock_resume_pending = false;
    }
    // The frontier goes first and the deadlines follow, so the
    // scheduler can never insert a restored deadline against a clock of
    // zero and fire it immediately.
    let mut budget = TTL_RESUME_BUDGET;
    while budget > 0 && worker.ttl_resume_cursor < MAX_KEYS as u32 {
        let slot = worker.ttl_resume_cursor as usize;
        if let Some((deadline, key_hash)) = worker.store.expiring_at(slot) {
            let mut frame = [0u8; 3 + 16];
            frame[0] = MSG_TTL_REGISTER;
            frame[1] = 16;
            frame[2] = 0;
            frame[3..11].copy_from_slice(&deadline.to_le_bytes());
            frame[11..19].copy_from_slice(&key_hash.to_le_bytes());
            if (sys.channel_write)(worker.expiry_out, frame.as_mut_ptr(), frame.len())
                != frame.len() as i32
            {
                return; // retry this slot next step
            }
            budget -= 1;
        }
        worker.ttl_resume_cursor += 1;
    }
}

/// Consume expiry events from `ttl_scheduler` and release the slots
/// they name.
///
/// This is reclamation, not mutation: the sweep only frees records the
/// committed clock has already put past their deadline, so it allocates
/// no revision and emits no watch event, and a replica running it
/// earlier or later than another cannot make their observable states
/// differ. The disk provider reclaims through compaction instead and is
/// left alone here.
unsafe fn drain_expire(worker: &mut WorkerState) {
    if worker.syscalls.is_null() {
        return;
    }
    let sys = &*worker.syscalls;
    if worker.expire_in >= 0 {
        let poll = (sys.channel_poll)(worker.expire_in, POLL_IN);
        if poll > 0 && (poll as u32) & POLL_IN != 0 {
            let mut hdr = [0u8; 3];
            if (sys.channel_read)(worker.expire_in, hdr.as_mut_ptr(), 3) < 3 {
                return;
            }
            let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
            let mut body = [0u8; 32];
            if payload_len > body.len() {
                return;
            }
            if payload_len > 0
                && ((sys.channel_read)(worker.expire_in, body.as_mut_ptr(), payload_len) as usize)
                    < payload_len
            {
                return;
            }
            if worker.state_store == STATE_STORE_DISK {
                // The disk provider reclaims through compaction.
                return;
            }
            // `[kind:1][payload:8]`. For a KV expiry the payload is the
            // key's `fnv1a64` hash — the identity the record is stored
            // under — so the named record is reclaimed directly instead
            // of hoping a cursor sweep wanders onto it.
            if payload_len >= 9 && body[0] == EXPIRY_KIND_KV {
                let key_hash = le_u64(&body[1..9]);
                worker
                    .store
                    .reap_expired_hash(worker.committed_now_ms, key_hash);
            }
            // …and latch a table sweep behind it. The named record is
            // the common case, but an event that never arrived at all
            // would otherwise leave a slot allocated forever in a
            // bounded table. The latch spends a table's worth of slot
            // VISITS, a budget at a time, across the steps that follow;
            // a visit that reclaims re-examines its slot rather than
            // advancing, so a sweep can end short of a full lap — but
            // only by having freed a slot for each step it fell short,
            // and the next event latches another.
            worker.reap_remaining = MAX_KEYS as u32;
        }
    }
    if worker.state_store == STATE_STORE_DISK || worker.reap_remaining == 0 {
        return;
    }
    let budget = REAP_BUDGET.min(worker.reap_remaining as usize);
    let (cursor, _released) =
        worker
            .store
            .reap_expired(worker.committed_now_ms, worker.reap_cursor as usize, budget);
    worker.reap_cursor = cursor as u32;
    worker.reap_remaining = worker.reap_remaining.saturating_sub(budget as u32);
}

/// Demand-driven commit-timestamp lease refill. When the
/// in-band lease runs low, ask the allocator for the next one. The
/// request is an unreplicated HINT — lost requests are retried on a
/// bounded cadence, and the grant itself arrives replicated on the
/// commands channel, so restarts and failovers need no special case.
/// An idle worker with reserve left sends nothing, which is what keeps
/// an idle graph's log quiet.
unsafe fn maybe_request_ts_lease(worker: &mut WorkerState) {
    /// Ask when fewer than this many timestamps remain.
    const TS_LOW_WATER: u64 = 16384;
    /// Lease size requested (allocator clamps to its own bounds).
    const TS_REQUEST_SIZE: u32 = 65536;
    /// Steps between retries while low (~0.5 s at the 1 ms tick).
    const TS_REQUEST_RETRY_STEPS: u64 = 500;

    worker.step_ctr_ts = worker.step_ctr_ts.wrapping_add(1);
    if worker.lease_request_out < 0 || worker.syscalls.is_null() {
        return;
    }
    let remaining = worker.ts_lease_end.saturating_sub(worker.ts_lease_next);
    if remaining >= TS_LOW_WATER {
        return;
    }
    if worker.ts_request_step != 0
        && worker.step_ctr_ts.wrapping_sub(worker.ts_request_step) < TS_REQUEST_RETRY_STEPS
    {
        return;
    }
    worker.ts_request_step = worker.step_ctr_ts;
    let sys = &*worker.syscalls;
    // MSG_TS_LEASE_REQUEST [corr_id:u64][requester:u32][size:u32].
    let mut frame = [0u8; 3 + 16];
    frame[0] = MSG_TS_LEASE_REQUEST;
    frame[1] = 16;
    frame[2] = 0;
    frame[3..11].copy_from_slice(&worker.step_ctr_ts.to_le_bytes());
    frame[11..15].copy_from_slice(&1u32.to_le_bytes());
    frame[15..19].copy_from_slice(&TS_REQUEST_SIZE.to_le_bytes());
    let _ = (sys.channel_write)(worker.lease_request_out, frame.as_mut_ptr(), frame.len());
}

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
            drain_retirements(worker);
        }
        drain_epoch_events(worker);
        drain_snapshot_ctl(worker);
        drain_gc_floor(worker);
        drain_expire(worker);
        drive_expiry_resume(worker);
        publish_retention_claim(worker);
        maybe_request_ts_lease(worker);
    }
    let drain_t0 = unsafe { now_us(worker) };
    let scans0 = worker.disk_state.scans_opened.get();
    let skips0 = worker.disk_state.scans_skipped.get();
    let blocks0 = worker.disk_state.blocks_read.get();
    let (reads0, rbytes0, opens0) = worker.fs_storage.read_counters();
    let mut budget = PER_TICK_DRAIN_BUDGET;
    let mut drained = 0u32;
    let mut spilled = false;
    // A held (paused) command gets its slice FIRST, inside the drain
    // window so its cost counts against `DRAIN_BUDGET_US` like any
    // other command's.
    if unsafe { drive_held_command(worker) } {
        budget -= 1;
        drained += 1;
    }
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
            // Payload `[partition_id:u16][term:u64][index:u64]` (18 bytes).
            // The partition leads so the router fences per-partition; a
            // reader written before this field expected 16 bytes, but the
            // only consumer is this repo's router, updated in lockstep.
            let mut env = [0u8; 21];
            env[0] = MSG_APP_APPLIED_POS;
            env[1] = 18;
            env[2] = 0;
            env[3..5].copy_from_slice(&worker.partition_id.to_le_bytes());
            env[5..13].copy_from_slice(&worker.applied_term.to_le_bytes());
            env[13..21].copy_from_slice(&worker.applied_index.to_le_bytes());
            if (sys.channel_write)(worker.responses_out, env.as_mut_ptr(), 21) == 21 {
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
            // App-snapshot refusal counter (id 22) — the memory store
            // is exactly where an over-budget body can refuse.
            emit_counters_at(
                &*worker.syscalls,
                worker.metrics_out,
                22,
                &[worker.m_snapshot_refusals],
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
                // Replay idempotence (RFC §4.3): seed the clock for a
                // FULL replay — the WAL re-feeds the committed stream
                // from the start, the revision counter restarts at 0
                // and is recomputed deterministically over the
                // replayed commands, and writes the materialization
                // already reflects (ts at or below the recovered
                // high-water, verified byte-for-byte) are suppressed.
                // If the WAL was instead TRUNCATED behind a snapshot
                // marker, durability installs that marker (RESET +
                // chunk) before replaying the tail, and the adoption
                // path re-seeds the clock at the high-water so the
                // tail's commands recompute their original revisions.
                worker.disk_replay_floor = worker.disk_state.highest_revision;
                worker.disk_revision = 0;
                worker.disk_phase = DISK_PHASE_SERVING;
                // Resume the committed clock the recovered runs were
                // written against (the manifest carries it). A WAL
                // replay on top only moves it forward, so this can
                // never place the clock ahead of the log — but without
                // it a store whose WAL was compacted past its last tick
                // comes back at zero, and every record that had already
                // expired is visible again.
                if worker.disk_state.committed_clock_ms > worker.committed_now_ms {
                    worker.committed_now_ms = worker.disk_state.committed_clock_ms;
                }
                resume_expiry_service(worker);
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
            // Space-driven BACKGROUND merge, armed well before the run set
            // fills. Without it runs only ever merge at MAX_RUNS (a refused
            // flush) or on a committed GC floor (which needs GC enablement),
            // so on a steady write trickle the run count grows without bound
            // — and the READ side pays for it: every merged walk opens every
            // run, and a version scan visits every run's blocks in its span.
            // Merging at 3 runs keeps the per-walk run count a constant.
            // Same merge the MAX_RUNS branch runs: floor already in force,
            // reclaims nothing, pure run-count hygiene.
            if !worker.disk_flush_active
                && !worker.disk_compact_active
                && worker.disk_state.run_count as usize >= RUN_MERGE_THRESHOLD
            {
                worker.disk_compact_active = true;
                worker.disk_compact_cursor = 0;
                worker.disk_compact_floor = worker.disk_state.compaction_floor;
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

/// Drain ONE queued retired-run deletion per step, only when no other
/// storage action ran. Each FAT32 delete frees a whole cluster chain
/// synchronously (~35 ms on the rig), which is why they queue instead
/// of running inside the compaction's manifest-adoption step.
unsafe fn drain_retirements(worker: &mut WorkerState) {
    if worker.state_store != STATE_STORE_DISK
        || worker.disk_phase != DISK_PHASE_SERVING
        || worker.disk_flush_active
        || worker.disk_flush_wanted
        || worker.disk_compact_active
        || worker.disk_state.retire_count == 0
    {
        return;
    }
    let t0 = now_us(worker);
    let outcome = {
        let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        store.retire_step()
    };
    let us = now_us(worker).wrapping_sub(t0);
    let mut buf = [0u8; 96];
    let mut n = write_prefix(&mut buf, 0, b"[kvw] disk retire us=");
    n = write_dec(&mut buf, n, us);
    n = write_prefix(&mut buf, n, b" ok=");
    n = write_dec(&mut buf, n, u64::from(matches!(outcome, Ok(true))));
    n = write_prefix(&mut buf, n, b" left=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_state.retire_count));
    log_line(worker, &buf, n);
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
        // Commit-timestamp assignment counters (ids 20, 21) and the
        // app-snapshot refusal counter (id 22).
        emit_counters_at(
            sys,
            worker.metrics_out,
            20,
            &[
                worker.m_ts_stamped,
                worker.m_ts_unstamped,
                worker.m_snapshot_refusals,
            ],
        );
    }

    if now.wrapping_sub(worker.disk_heartbeat_ms) < DISK_HEARTBEAT_LOG_MS {
        return;
    }
    worker.disk_heartbeat_ms = now;
    // ph=phase fw=flush_wanted ca=compact_active deg=degraded_steps
    // hard=hard_faults errno=last_errno mem=memtable runs=run_files
    // fa/fc/ff/fb=flush attempts/completions/failures/backpressure
    let mut buf = [0u8; 320];
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
    // The committed GC floor (§18) — 0 means no floor has ever
    // committed, i.e. the reclamation loop is NOT running.
    n = write_prefix(&mut buf, n, b" gcf=");
    n = write_dec(&mut buf, n, worker.gc_committed_floor);
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
    //   held = a paused command is being re-driven right now
    //   sp   = version-scan slices that paused (cumulative)
    //   sa   = re-issued requests adopted into the hold slot
    //   hr   = pausable frames refused by the slot (oversized)
    n = write_prefix(&mut buf, n, b" held=");
    n = write_dec(&mut buf, n, u64::from(worker.held_active));
    n = write_prefix(&mut buf, n, b" sp=");
    n = write_dec(&mut buf, n, worker.m_scan_pauses);
    n = write_prefix(&mut buf, n, b" sa=");
    n = write_dec(&mut buf, n, worker.m_scan_retries_adopted);
    n = write_prefix(&mut buf, n, b" hr=");
    n = write_dec(&mut buf, n, worker.m_scan_hold_refusals);
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
    if worker.state_store == STATE_STORE_DISK && worker.disk_phase != DISK_PHASE_SERVING {
        // Not safe to act on ANY snapshot-ctl message yet — and the
        // boot-restore RESET from durability may already be queued.
        // Leave the channel unread (backpressure) rather than draining
        // and dropping: a consumed-then-ignored RESET would silently
        // lose the whole boot restore.
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
        MSG_APP_SNAPSHOT_SPAN_REQUEST => {
            // Elastic split: ship ONLY the requested span to a
            // demand-provisioned partition. Body after [term:8][index:8]:
            // [start_len:u16][start][end_len:u16][end]. Bounds are copied
            // out first because the emit path reuses `worker.scratch`.
            // No applied_index gate: a span copy is labelled state, not a
            // raft snapshot of THIS partition — a direct-apply source has a
            // valid store at index 0.
            if worker.snapshot_export_out < 0 {
                return;
            }
            let mut start = [0u8; 256];
            let mut end = [0u8; 256];
            let (sl, el) = {
                let p = &worker.scratch;
                if payload_len < 20 {
                    return;
                }
                let sl = u16::from_le_bytes([p[16], p[17]]) as usize;
                let el_off = 18 + sl;
                if sl > 256 || el_off + 2 > payload_len {
                    return;
                }
                let el = u16::from_le_bytes([p[el_off], p[el_off + 1]]) as usize;
                if el > 256 || el_off + 2 + el > payload_len {
                    return;
                }
                start[..sl].copy_from_slice(&p[18..18 + sl]);
                end[..el].copy_from_slice(&p[el_off + 2..el_off + 2 + el]);
                (sl, el)
            };
            emit_span_snapshot_chunks(worker, sys, &start[..sl], &end[..el]);
        }
        MSG_APP_SNAPSHOT_RESET => {
            if worker.state_store == STATE_STORE_DISK {
                if worker.disk_phase != DISK_PHASE_SERVING {
                    return; // fail closed: not safe to install yet
                }
                // NOTHING destructive here. The body may be a
                // DISK-RESIDENT marker (the store already holds the
                // state and must be ADOPTED, not destroyed), and which
                // kind it is only becomes known once the stream header
                // arrives — so the provider install (which destroys the
                // materialization as its first act) is deferred to the
                // completed stream in the CHUNK arm.
            } else {
                // Memory table: discard up front so a chunk stream that
                // never completes can't leave a half-old, half-new
                // store behind (re-init is free here).
                worker.store.init();
            }
            worker.applied_term = term;
            worker.applied_index = index;
            worker.import_len = 0;
            worker.importing = true;
        }
        MSG_APP_SNAPSHOT_DURABLE => {
            // Durability certifies the snapshot at `index` is persisted
            // (body written crash-atomically AND its boot pointer). Only
            // now may the GC-snapshot claim advance. Promote the pending
            // export iff this ack covers the position it was labelled with;
            // a superseding export left a higher pending index and waits
            // for its own ack, and an ack for a snapshot this worker never
            // labelled (pending == 0) is ignored. Under-advancing is safe;
            // over-advancing past a non-durable snapshot is the bug.
            if worker.gc_snapshot_pending_index != 0 && index >= worker.gc_snapshot_pending_index {
                worker.gc_snapshot_revision = worker.gc_snapshot_pending_revision;
                worker.gc_snapshot_pending_index = 0;
            }
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
                // DISK-RESIDENT marker: the body certifies that the
                // state up to its applied position lives in THIS
                // node's manifest-named runs. Adopt the local store
                // (identity-checked) instead of destroying it. A node
                // whose local store doesn't match fails CLOSED: applied
                // stays 0 and the store is untouched — it must catch up
                // from the leader, not fabricate state.
                if worker.state_store == STATE_STORE_DISK
                    && total >= SNAP_HDR_LEN
                    && u32::from_le_bytes([
                        worker.import_buf[0],
                        worker.import_buf[1],
                        worker.import_buf[2],
                        worker.import_buf[3],
                    ]) == SNAP_MAGIC
                    && u16::from_le_bytes([worker.import_buf[4], worker.import_buf[5]])
                        == SNAP_FORMAT_VERSION
                    && u16::from_le_bytes([worker.import_buf[6], worker.import_buf[7]])
                        & SNAP_FLAG_DISK_RESIDENT
                        != 0
                {
                    let hdr_gen = u32::from_le_bytes([
                        worker.import_buf[16],
                        worker.import_buf[17],
                        worker.import_buf[18],
                        worker.import_buf[19],
                    ]);
                    let adopted = worker.disk_phase == DISK_PHASE_SERVING
                        && hdr_gen == worker.disk_state.range_generation
                        && worker.disk_state.generation > 0;
                    // The marker's clock frontier stands whether or not
                    // the local store is adopted: it is a fact about
                    // the log this node is following, and the records
                    // it certifies carry deadlines measured against it.
                    let frontier = le_u64(&worker.import_buf[SNAP_HDR_CLOCK_OFF..SNAP_HDR_LEN]);
                    if frontier > worker.committed_now_ms {
                        worker.committed_now_ms = frontier;
                        worker.disk_state.committed_clock_ms = frontier;
                    }
                    if adopted {
                        // Keep the RESET's (term, index) and the store
                        // exactly as recovered, and RESUME the revision
                        // clock at the store's high-water: the WAL tail
                        // beyond the marker replays on top, each command
                        // recomputing the same revision it was
                        // originally assigned (floor+1, floor+2, …).
                        // Without the re-seed the clock restarts at the
                        // tail and every run version sits invisibly
                        // above the read horizon.
                        worker.disk_revision = worker.disk_state.highest_revision;
                        worker.disk_replay_floor = worker.disk_state.highest_revision;
                    } else {
                        worker.applied_term = 0;
                        worker.applied_index = 0;
                    }
                    if adopted {
                        // The scheduler's queue and clock are arena-only
                        // and did not survive whatever brought this node
                        // back; hand it the frontier.
                        resume_expiry_service(worker);
                    }
                    worker.importing = false;
                    worker.import_len = 0;
                    return;
                }
                let installed = if worker.state_store == STATE_STORE_DISK {
                    // Full-fidelity body: run the provider's REPLACE
                    // install as one self-starting final chunk (it
                    // destroys the materialization first, re-validates
                    // every record, and aborts to an EMPTY store on any
                    // inconsistency).
                    let req = SnapshotRequest {
                        applied_index: worker.applied_index,
                        range_generation: worker.disk_state.range_generation,
                    };
                    // Stamp BEFORE installing: the install flushes
                    // through `flush_inner` when its staging memtable
                    // fills, and every manifest those flushes publish
                    // must carry the SNAPSHOT's applied position, not
                    // whatever the store held before the RESET.
                    worker.disk_state.pending_applied_index = worker.applied_index;
                    worker.disk_state.pending_applied_term = worker.applied_term;
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
                        // `install` adopted the header's frontier into
                        // the store; the state machine must run on the
                        // same one.
                        if worker.disk_state.committed_clock_ms > worker.committed_now_ms {
                            worker.committed_now_ms = worker.disk_state.committed_clock_ms;
                        }
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
                    let ok = worker.store.snapshot_decode(&worker.import_buf[..total]);
                    if ok {
                        // The records are back; so must be the clock
                        // they were deadlined against. Their `expiry_ms`
                        // is ABSOLUTE against it, so resuming at zero
                        // would make every already-expired record
                        // visible again and re-age the survivors from
                        // the start.
                        let frontier = KvStore::snapshot_clock_ms(&worker.import_buf[..total]);
                        if frontier > worker.committed_now_ms {
                            worker.committed_now_ms = frontier;
                        }
                    }
                    ok
                };
                if installed {
                    // State now reflects the RESET's (term, index), which
                    // we already adopted (pending pair stamped above).
                    // Put the scheduler back in agreement with the store:
                    // its clock and its expiry queue are arena-only, so
                    // nothing else rebuilds them.
                    resume_expiry_service(worker);
                    // Elastic split: tell an orchestrator the install is
                    // resident, so an elastic-split cutover gates on the
                    // span actually landing rather than a wall-clock guess.
                    // Unwired (index 7 < 0) in every graph but the split.
                    if worker.install_ack_out >= 0 {
                        let mut ack = [0u8; 3 + 10];
                        ack[0] = MSG_APP_SNAPSHOT_INSTALLED;
                        ack[1] = 10;
                        ack[2] = 0;
                        ack[3..5].copy_from_slice(&worker.partition_id.to_le_bytes());
                        ack[5..13].copy_from_slice(&worker.applied_index.to_le_bytes());
                        (sys.channel_write)(worker.install_ack_out, ack.as_mut_ptr(), 13);
                    }
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
    let (encoded, term, index) = if worker.state_store == STATE_STORE_DISK {
        // The disk store's durable state cannot fit an app-snapshot
        // body (16 KiB cap on the durability side) and does not need
        // to: the manifest-named runs ARE the snapshot. Emit a
        // DISK-RESIDENT marker labelled at the manifest's durable
        // applied position — durability may compact the WAL below it
        // because the runs alone reconstruct that prefix; the WAL tail
        // beyond it stays authoritative for the memtable content.
        (
            disk_snapshot_marker(worker, &mut body),
            worker.disk_state.durable_applied_term,
            worker.disk_state.durable_applied_index,
        )
    } else {
        (
            worker
                .store
                .snapshot_encode(&mut body, worker.committed_now_ms),
            worker.applied_term,
            worker.applied_index,
        )
    };
    let Some(total) = encoded else {
        // Accounted capacity denial — the fail-closed path is silent on
        // the wire by design (durability re-requests on the next
        // rotation), so the refusal must be visible HERE. The memory
        // store's over-budget encode is the anomalous case and gets a
        // one-shot log; a disk store refusing before its first durable
        // flush is normal early-boot state.
        worker.m_snapshot_refusals = worker.m_snapshot_refusals.wrapping_add(1);
        if worker.state_store != STATE_STORE_DISK && !worker.snap_refusal_logged {
            worker.snap_refusal_logged = true;
            dev_log(sys, 2, b"[kvw] snap body over budget".as_ptr(), 27);
        }
        return;
    };
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
            // `gc_claim_mode = snapshot` this is the candidate active-read
            // claim: nothing below it is needed to reconstruct the range
            // from this snapshot plus the WAL tail. Recorded only on a
            // COMPLETE stream — a partial one reconstructs nothing.
            //
            // It is held PENDING against `index` (the raft position this
            // body is labelled with), NOT promoted into the live claim
            // here: a completed export is not yet a DURABLE snapshot, and
            // advancing the GC floor onto one a crash could lose lets WAL
            // replay read compacted-away state. `MSG_APP_SNAPSHOT_DURABLE`
            // from durability promotes it once the body and its boot
            // pointer are persisted.
            worker.gc_snapshot_pending_index = index;
            worker.gc_snapshot_pending_revision = if worker.state_store == STATE_STORE_DISK {
                worker.disk_revision
            } else {
                worker.store.revision
            };
            return;
        }
    }
}

/// Elastic split: stream ONLY the keys in `[start, end)` as a
/// `MSG_APP_SNAPSHOT_CHUNK` sequence — the elastic split's span copy to a
/// demand-provisioned partition. Both providers export: the memory store
/// encodes its records directly, the disk provider merge-scans the span
/// and streams the same portable body (current state; version history
/// stays behind, as the target's revision space is independent).
///
/// Unlike [`emit_snapshot_chunks`] this is a COPY to ANOTHER partition, not
/// a snapshot of THIS worker's own state, so it deliberately touches none of
/// this worker's GC-snapshot bookkeeping. An empty span still emits one
/// header-only chunk, so the target's install path always runs.
unsafe fn emit_span_snapshot_chunks(
    worker: &mut WorkerState,
    sys: &SyscallTable,
    start: &[u8],
    end: &[u8],
) {
    if worker.snapshot_export_out < 0 {
        return;
    }
    let mut body = [0u8; SNAPSHOT_BODY_MAX];
    let encoded = if worker.state_store == STATE_STORE_DISK {
        let store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        let mut mat = DiskMaterializer::new(store, worker.disk_revision, worker.disk_replay_floor);
        mat.snapshot_encode_span(&mut body, worker.committed_now_ms, start, end)
    } else {
        worker
            .store
            .snapshot_encode_span(&mut body, worker.committed_now_ms, start, end)
    };
    let Some(total) = encoded else {
        worker.m_snapshot_refusals = worker.m_snapshot_refusals.wrapping_add(1);
        return;
    };
    // The target is a demand-provisioned (empty) partition: send a RESET
    // first so its CHUNK path is armed (`importing`) and any prior state is
    // discarded, then the span body. Labelled at (0, 0): this is a COPY of
    // another range's keys, not a raft position the target should adopt —
    // the target keeps its own apply position and simply gains the keys.
    let (term, index) = (0u64, 0u64);
    {
        let reset_payload = APP_SNAPSHOT_HDR;
        worker.scratch[0] = MSG_APP_SNAPSHOT_RESET;
        worker.scratch[1] = (reset_payload & 0xFF) as u8;
        worker.scratch[2] = ((reset_payload >> 8) & 0xFF) as u8;
        worker.scratch[3..11].copy_from_slice(&term.to_le_bytes());
        worker.scratch[11..19].copy_from_slice(&index.to_le_bytes());
        // The RESET arm reads only term+index (16 bytes); pad the rest.
        for b in worker.scratch[19..3 + reset_payload].iter_mut() {
            *b = 0;
        }
        let n = (sys.channel_write)(
            worker.snapshot_export_out,
            worker.scratch.as_mut_ptr(),
            3 + reset_payload,
        );
        if n != (3 + reset_payload) as i32 {
            return;
        }
    }
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
            return;
        }
        worker.m_snapshot_chunks = worker.m_snapshot_chunks.wrapping_add(1);
        sent += chunk;
        if done {
            return;
        }
    }
}

/// Build the DISK-RESIDENT marker body (one snapshot-stream header,
/// `SNAP_FLAG_DISK_RESIDENT` set, zero entries) certifying that the
/// state up to the manifest's durable applied position lives in this
/// node's runs. Fails CLOSED (`None` — no chunks, WAL stays
/// authoritative) while nothing is durably covered yet: before the
/// first flush publish there is no honest position to certify.
unsafe fn disk_snapshot_marker(worker: &mut WorkerState, out: &mut [u8]) -> Option<usize> {
    if worker.disk_phase != DISK_PHASE_SERVING
        || worker.disk_state.durable_applied_index == 0
        || out.len() < SNAP_HDR_LEN
    {
        return None;
    }
    out[0..4].copy_from_slice(&SNAP_MAGIC.to_le_bytes());
    out[4..6].copy_from_slice(&SNAP_FORMAT_VERSION.to_le_bytes());
    out[6..8].copy_from_slice(&SNAP_FLAG_DISK_RESIDENT.to_le_bytes());
    out[8..16].copy_from_slice(&worker.disk_state.durable_applied_index.to_le_bytes());
    out[16..20].copy_from_slice(&worker.disk_state.range_generation.to_le_bytes());
    out[20..24].copy_from_slice(&0u32.to_le_bytes());
    out[24..32].copy_from_slice(&worker.disk_state.highest_revision.to_le_bytes());
    out[32..40].copy_from_slice(&worker.disk_state.compaction_floor.to_le_bytes());
    out[SNAP_HDR_CLOCK_OFF..SNAP_HDR_LEN].copy_from_slice(&worker.committed_now_ms.to_le_bytes());
    Some(SNAP_HDR_LEN)
}

/// Read a little-endian u64 from an 8-byte slice.
fn le_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}
