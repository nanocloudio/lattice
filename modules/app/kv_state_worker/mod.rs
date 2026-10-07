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
//! ## State-store provider selection
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
    unexpected_cfgs,
    reason = "`store_provider` is kv_state_worker_store's feature; this file is also kv_state_worker, whose build declares none"
)]
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

#[path = "../../common/command_log.rs"]
mod command_log;

#[cfg(feature = "store_provider")]
#[path = "../../common/kv_large.rs"]
mod kv_large;

#[cfg(feature = "store_provider")]
#[path = "../../common/doc_tree.rs"]
mod doc_tree;

#[cfg(feature = "store_provider")]
#[path = "../../common/store_provider.rs"]
mod store_provider;

// Every replica applies a store provider's writes by their origin's
// record, whether or not it provides the store itself.
#[path = "../../common/origin.rs"]
mod origin;

use fs_run_storage::FsRunStorage;
use kv_store::disk_store::state_store::{
    KvStateStore, Progress, SnapshotChunk, SnapshotRequest, StoreError,
};
use kv_store::disk_store::{
    DiskState, DiskStore, SNAP_FLAG_DISK_RESIDENT, SNAP_FORMAT_VERSION, SNAP_HDR_CLOCK_OFF,
    SNAP_HDR_LEN, SNAP_MAGIC,
};
use kv_store::{DiskMaterializer, EventJournal, KvStore, Materializer, MAX_KEYS};
use types::{
    KV_OP_APPEND, KV_OP_CAS, KV_OP_DECR, KV_OP_DELETE, KV_OP_DELETE_META, KV_OP_EXISTS,
    KV_OP_FLUSH, KV_OP_GET, KV_OP_GET_AT, KV_OP_IDEMPOTENT, KV_OP_INCR, KV_OP_MGET, KV_OP_MSET,
    KV_OP_PREPEND, KV_OP_PUT, KV_OP_RANGE, KV_OP_SCAN, KV_OP_SCAN_AT, KV_OP_SCAN_VERSIONS,
    KV_OP_STRLEN, KV_OP_TXN, KV_OP_TXN_PREPARE, KV_OP_TXN_RECORD, KV_OP_TXN_RESOLVE,
    KV_RESULT_INTEGER, KV_RESULT_KVS, KV_RESULT_OK, KV_RESULT_PAUSED,
};
use types::{KV_OP_COMPACT, KV_OP_ETCD_TXN, KV_OP_LEASE_REVOKE, KV_RESULT_ETCD_TXN};
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
            // A manifest record is the user-visible version of a large
            // value, stamped as a PUT is (its chunks are content, not
            // versions).
            | types::KV_OP_PUT_MANIFEST
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

    // Physical state-store provider, selected at graph construction.
    // 0 = bounded in-memory table
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

    // What this worker publishes as its ACTIVE-READ retention
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

    // How a chunk sweep this worker proposes on `sweep_out` reaches the
    // ordering its commands come through: 0 = a bare `MSG_KV_COMMAND`
    // (a single-node graph wires `sweep_out` to `commands`); 1 = a
    // `MSG_CLIENT_PROPOSAL` record for consensus, which commits it back
    // through `lattice_apply_bridge`.
    7, sweep_wrap, u8, 0, enum { command=0, proposal=1 }
        => |s, d, len| { s.sweep_wrap = p_u8(d, len, 0, 0); };

    // How long, on the committed clock, an unreferenced chunk is kept
    // before a sweep may take it: the longest a producer may take between
    // uploading a value's chunks and committing the record that names
    // them, and a reader between reading a manifest and fetching its
    // chunks. A transfer that outlasts it fails (the commit finds a chunk
    // missing; the fetch finds none) rather than reading a wrong value.
    // The worker's own write-ahead log (`common/command_log.rs`), for a
    // disk worker nothing orders — no consensus WAL in front of it: 0 =
    // off (the WAL is clustor's, or the store is volatile); 1 = every
    // op that is not a pure read is logged and made durable before it
    // is answered, and replayed on top of the recovered runs after a
    // restart. Refused with `sweep_wrap: 1` (consensus orders the
    // worker; its WAL is the authority). `kv_state_worker_store` turns
    // it on whenever it provides the store contracts.
    9, command_log, u8, 0, enum { off=0, on=1 }
        => |s, d, len| { s.command_log = p_u8(d, len, 0, 0); };

    // A fluxor host store's `store.log` to import, once, into this
    // store (kv_state_worker_store: a store moving onto lattice keeps its
    // contents). Replayed before the store serves; recorded as done in
    // the provider's own keyspace, so a restart does not import again
    // (an import a crash interrupted runs again — puts and deletes in
    // order come to the same state). Empty: none.
    // The disk store's directory (linux layout): a path the FS provider
    // resolves — absolute, or relative to the working directory — in
    // place of `kv/` (a service whose working directory is not writable
    // keeps its store where its state lives). Created by the operator,
    // as `kv/` is. Empty: `kv/` (or `kv<store_id>/`).
    11, store_dir, str, 0
        => |s, d, len| {
            if len <= s.store_dir.len() {
                // SAFETY: the params TLV names `len` bytes at `d`.
                let src = unsafe { core::slice::from_raw_parts(d, len) };
                for (x, y) in s.store_dir.iter_mut().zip(src) {
                    *x = *y;
                }
                s.store_dir_len = len as u8;
            }
        };

    10, import_log, str, 0
        => |s, d, len| {
            if len <= s.import_path.len() {
                // SAFETY: the params TLV names `len` bytes at `d`.
                let src = unsafe { core::slice::from_raw_parts(d, len) };
                for (x, y) in s.import_path.iter_mut().zip(src) {
                    *x = *y;
                }
                s.import_path_len = len as u16;
            }
        };

    // History kept, in revisions, by a worker with its own command log
    // (nothing orders it, so nothing else compacts it): once the store
    // passes the last compaction by this much and an eighth more, it
    // compacts to its revision less this — etcd's revision auto-compaction,
    // and fluxor's host store's change window (65536). A watch or change
    // read from before it answers compacted (Kubernetes' 410). 0: keep
    // everything.
    12, history_revisions, u32, 0
        => |s, d, len| { s.history_revisions = p_u32(d, len, 0, 0); };

    8, sweep_grace_ms, u32, 300000
        => |s, d, len| { s.sweep_grace_ms = p_u32(d, len, 0, 300_000); };

    // This replica's id in its group (consensus's `self_id`): the origin
    // the writes its store provider proposes carry (`common/origin.rs`).
    // Two replicas of one group must not share it.
    13, replica_id, u16, 0
        => |s, d, len| { s.replica_id = p_u16(d, len, 0, 0); };

    // The group as a replicated store provider's write fences name it:
    // the configuration's generation, the voters a commit needs, and
    // whether a commit is on a quorum's media (0 = ordered only, the
    // fence `RevisionMonotone`; 1 = a disk WAL a quorum fsyncs, the fence
    // `ReplicatedDurable`).
    14, group_epoch, u32, 0
        => |s, d, len| { s.group_epoch = p_u32(d, len, 0, 0); };
    15, group_quorum, u8, 1
        => |s, d, len| { s.group_quorum = p_u8(d, len, 0, 1); };
    16, group_durable, u8, 0, enum { ordered=0, quorum_fsync=1 }
        => |s, d, len| { s.group_durable = p_u8(d, len, 0, 0); };

    // A store provider's scope: the node whose private records the keys
    // under `scoped` (comma-separated prefixes) are. The graph names them
    // plainly; the store keeps them under `/~node/<scope>`, so each node
    // of a group keeps its own apart from every other's
    // (`store_provider::Scope`). Empty: no scope.
    17, scope, str, 0
        => |s, d, len| {
            if len <= s.scope_name.len() {
                // SAFETY: the params TLV names `len` bytes at `d`.
                let src = unsafe { core::slice::from_raw_parts(d, len) };
                for (x, y) in s.scope_name.iter_mut().zip(src) {
                    *x = *y;
                }
                s.scope_name_len = len as u8;
            }
        };
    18, scoped, str, 0
        => |s, d, len| {
            if len <= s.scoped.len() {
                // SAFETY: the params TLV names `len` bytes at `d`.
                let src = unsafe { core::slice::from_raw_parts(d, len) };
                for (x, y) in s.scoped.iter_mut().zip(src) {
                    *x = *y;
                }
                s.scoped_len = len as u16;
            }
        };

    // A store provider's volume: with one it is a keyed provider of
    // storage.object and storage.namespace, reached by that name's selector,
    // leaving the graph's default providers to others (Loam's, beside it).
    // Empty: the default provider.
    19, volume, str, 0
        => |s, d, len| {
            if len > 0 && len <= 32 {
                // SAFETY: the params TLV names `len` bytes at `d`.
                let src = unsafe { core::slice::from_raw_parts(d, len) };
                s.selector = abi::kernel_abi::provider_selector::hash(src);
            }
        };
}

/// Steps between chunk sweeps while writes may have queued chunk work.
/// A sweep with nothing due takes no revision; it costs one ordered
/// entry.
const SWEEP_INTERVAL_STEPS: u64 = 1000;
/// Steps between chunk sweeps while the queues hold only entries that
/// wait for the floor or their grace.
const SWEEP_WAIT_STEPS: u64 = 10_000;
/// Steps after which a proposed sweep that never came back is proposed
/// again (consensus may drop a proposal: a lost leadership, a full
/// queue).
const SWEEP_RETRY_STEPS: u64 = 5000;

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
    /// Chunk sweeps this worker proposes (index 8); -1 unless wired.
    sweep_out: i32,
    /// Param `sweep_wrap`: 0 = bare command, 1 = consensus proposal.
    sweep_wrap: u8,
    /// Param `sweep_grace_ms`.
    sweep_grace_ms: u32,
    /// Writes a replicated store provider proposes (index 9); -1 unless
    /// wired.
    prov_out: i32,
    /// Params `replica_id`, `group_epoch`, `group_quorum`, `group_durable`.
    replica_id: u16,
    group_epoch: u32,
    group_quorum: u8,
    group_durable: u8,
    /// A write has applied since the last sweep: chunk work may be queued.
    sweep_armed: bool,
    /// The last applied sweep's `kv_store::SWEEP_*` flags.
    sweep_flags: u8,
    /// A proposed sweep has not come back yet.
    sweep_inflight: bool,
    /// The step the last sweep was proposed at.
    sweep_step: u64,
    /// The floor the last sweep named.
    sweep_floor: u64,
    /// Sweeps proposed / applied.
    sweeps_proposed: u64,
    sweeps_applied: u64,

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
    /// replay position — see `kv_store::DiskMaterializer` (replay
    /// idempotence). NOTE the engine revision counter is
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
    /// Ops that wrote more versions than `kv_store::write_bound` allows.
    m_write_bound_exceeded: u64,
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

    // ── MVCC GC floor ──────────────────────────────────────────────
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
    /// (the schema generation). Reported on every
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

    // Telemetry. Monotonic counters emitted on `metrics_out` at
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
    /// A reply `responses` had no room for (`reply_stash_len` bytes):
    /// written before anything else, and no command is taken while it
    /// waits — a command applied must not lose its answer, and answers
    /// keep their order.
    reply_stash: [u8; SCRATCH_BUF_SIZE],
    reply_stash_len: usize,
    /// The disk provider's watch-event journal for the command in flight
    /// (the memory provider keeps its own, `store.events`).
    events: EventJournal,

    /// `store_dir` param: the disk store's directory.
    store_dir: [u8; 128],
    store_dir_len: u8,
    /// `import_log` param: the host store log to import (store twin).
    import_path: [u8; 255],
    import_path_len: u16,
    /// `scope` / `scoped` params: this node's private records.
    scope_name: [u8; 63],
    scope_name_len: u8,
    scoped: [u8; 512],
    scoped_len: u16,
    /// `provider_selector::hash(volume)`, or 0: the default provider.
    selector: u32,

    /// `command_log` param, and the log's life: off, replaying its tail
    /// after recovery, or live (every applied op logged).
    command_log: u8,
    clog_phase: u8,
    clog: command_log::CommandLog,
    clog_replay: command_log::Replay,
    /// The durable revision the log was last reclaimed at.
    clog_reclaimed: u64,
    clog_appends: u64,
    /// `history_revisions` param, and the revision last compacted to.
    history_revisions: u32,
    compacted_to: u64,
    /// A bulk load is running (the store.log import): its ops are logged
    /// without a sync each, and synced once, before its end is recorded.
    clog_defer: bool,
    clog_replayed: u64,
    /// A replayed record as read, and its answer's room: the module's
    /// heap, taken at construction when the log is on (apart from this
    /// state, so a replay holds them while it applies).
    clog_buf: *mut u8,
    clog_out: *mut u8,

    /// One `MSG_WATCH_EVENT` envelope as the journal frames it.
    event_buf: [u8; 3 + wire::WATCH_EVENT_MAX],

    /// The store contracts this worker serves (`kv_state_worker_store`).
    #[cfg(feature = "store_provider")]
    prov: ProviderHeap,
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
        self.sweep_out = -1;
        self.reply_stash_len = 0;
        self.sweep_wrap = 0;
        self.sweep_grace_ms = 300_000;
        self.prov_out = -1;
        self.replica_id = 0;
        self.group_epoch = 0;
        self.group_quorum = 1;
        self.group_durable = 0;
        self.sweep_armed = false;
        self.sweep_flags = 0;
        self.sweep_inflight = false;
        self.sweep_step = 0;
        self.sweep_floor = 0;
        self.sweeps_proposed = 0;
        self.sweeps_applied = 0;
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
        self.events.clear();
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
        self.m_write_bound_exceeded = 0;
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
    if worker.command_log != 0 && worker.clog_phase != CLOG_LIVE {
        // The log's tail has not replayed: the store is behind what it
        // answered before the restart.
        return false;
    }
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

// ── The command log (`common/command_log.rs`) ─────────────────────────

const CLOG_OFF: u8 = 0;
const CLOG_REPLAYING: u8 = 1;
const CLOG_LIVE: u8 = 2;
/// The largest answer room an op is applied with (a provider call's).
const CLOG_OUT_MAX: usize = 65536;
/// The command log's heap: a replayed record and its answer's room.
const CLOG_ARENA: usize = command_log::PAYLOAD_MAX + CLOG_OUT_MAX;
/// Logged ops one step replays.
const CLOG_REPLAY_PER_STEP: u32 = 64;

/// Ops that only read: the log holds every other op.
const fn op_only_reads(op: u8) -> bool {
    matches!(
        op,
        KV_OP_GET
            | KV_OP_RANGE
            | KV_OP_EXISTS
            | KV_OP_SCAN
            | KV_OP_MGET
            | KV_OP_STRLEN
            | KV_OP_GET_AT
            | KV_OP_SCAN_AT
            | types::KV_OP_RANGE_SCAN
            | KV_OP_SCAN_VERSIONS
            | types::KV_OP_SNAPSHOT_VERSIONS
            | types::KV_OP_IDEMPOTENT_LOOKUP
            | types::KV_OP_RANGE_META
            | types::KV_OP_LEASE_KEYS
            | types::KV_OP_HASH_KV
            | types::KV_OP_GET_CHUNK
    )
}

/// Once a step, with the disk store serving: after recovery, find the
/// log and replay its tail (the clock seeded where it starts), a bounded
/// slice per step; then keep it reclaimed behind every published flush.
unsafe fn drive_command_log(worker: &mut WorkerState) {
    if worker.command_log == 0 || worker.disk_phase != DISK_PHASE_SERVING {
        return;
    }
    match worker.clog_phase {
        CLOG_OFF => {
            let high = worker.disk_state.highest_revision;
            match worker.clog.recover(&mut worker.fs_storage, high) {
                Ok(start) => {
                    // No log: the runs are the whole store (a new one, or
                    // one every op of which a flush holds).
                    worker.disk_revision = start.unwrap_or(high);
                    worker.clog_replay = command_log::Replay::new();
                    worker.clog_phase = CLOG_REPLAYING;
                }
                Err(_) => {
                    log_disk_errno(
                        worker,
                        b"[kvw] command log recover failed, QUARANTINE errno=",
                    );
                    worker.disk_phase = DISK_PHASE_QUARANTINED;
                }
            }
        }
        CLOG_REPLAYING => {
            for _ in 0..CLOG_REPLAY_PER_STEP {
                // SAFETY: `PAYLOAD_MAX` bytes of this module's heap, taken
                // at construction (the log is on only if they were).
                let buf =
                    core::slice::from_raw_parts_mut(worker.clog_buf, command_log::PAYLOAD_MAX);
                let next = worker
                    .clog_replay
                    .next(&worker.clog, &worker.fs_storage, buf);
                let rec = match next {
                    Ok(Some(r)) => r,
                    Ok(None) => {
                        worker.clog.replayed(&worker.clog_replay);
                        worker.clog_phase = CLOG_LIVE;
                        worker.clog_reclaimed = worker.disk_state.durable_highest_revision;
                        let mut buf = [0u8; 80];
                        let mut n = write_prefix(&mut buf, 0, b"[kvw] command log replayed n=");
                        n = write_dec(&mut buf, n, worker.clog_replayed);
                        n = write_prefix(&mut buf, n, b" rev=");
                        n = write_dec(&mut buf, n, worker.disk_revision);
                        log_line(worker, &buf, n);
                        return;
                    }
                    Err(_) => {
                        log_disk_errno(worker, b"[kvw] command log CORRUPT, QUARANTINE errno=");
                        worker.disk_phase = DISK_PHASE_QUARANTINED;
                        return;
                    }
                };
                let (op, identity, consistency, commit_ts) =
                    (rec.op, rec.identity, rec.consistency, rec.commit_ts);
                let room = (rec.room as usize).min(CLOG_OUT_MAX);
                let body = rec.body;
                // Each op runs under the clock it first ran under.
                worker.committed_now_ms = rec.now_ms;
                // The answer's room as it first ran; the answer itself
                // was given before the restart.
                // SAFETY: `CLOG_OUT_MAX` bytes of this module's heap.
                let out = core::slice::from_raw_parts_mut(worker.clog_out, room);
                // A logged op waits for room as a live one does, and is
                // applied on a later step (maintenance makes the room).
                if !admit(worker, op, body) {
                    return;
                }
                let a = apply_to_store(worker, op, identity, consistency, commit_ts, body, out);
                if a.paused || worker.disk_phase != DISK_PHASE_SERVING {
                    // A logged op never pauses (only reads do); a fault
                    // has quarantined the store.
                    return;
                }
                worker.clog_replayed = worker.clog_replayed.wrapping_add(1);
            }
        }
        _ => {
            retain_history(worker);
            let durable = worker.disk_state.durable_highest_revision;
            if durable > worker.clog_reclaimed
                && worker.clog.reclaim(&mut worker.fs_storage, durable).is_ok()
            {
                worker.clog_reclaimed = durable;
            }
        }
    }
}

/// Keep `history_revisions` of history: a Compact through the worker's own
/// logged ops, once the revision is that far (and an eighth more, so a
/// compaction is not issued per write) past the last one.
unsafe fn retain_history(worker: &mut WorkerState) {
    let keep = u64::from(worker.history_revisions);
    let rev = worker.disk_revision;
    if keep == 0 || rev <= worker.compacted_to.saturating_add(keep + keep / 8) {
        return;
    }
    let target = rev - keep;
    if !admit(worker, KV_OP_COMPACT, &target.to_le_bytes()) {
        return;
    }
    let mut ans = [0u8; 16];
    let consistency = kv_store::ReadPolicy::LATEST.consistency;
    let a = apply_to_store(
        worker,
        KV_OP_COMPACT,
        (0, 0, 0),
        consistency,
        0,
        &target.to_le_bytes(),
        &mut ans,
    );
    if a.result == KV_RESULT_OK || a.result == types::KV_RESULT_COMPACTED {
        worker.compacted_to = target;
    }
}

/// The memtable entries `op` adds at most ([`kv_store::write_bound`]): an
/// origin entry's are its op's and its origin record's.
fn write_bound(op: u8, body: &[u8]) -> Option<usize> {
    match op {
        types::KV_OP_ORIGIN_WRITE => kv_store::write_bound(KV_OP_TXN, body).map(|b| b + 1),
        // The write, its origin's record, and the import's marker.
        types::KV_OP_ORIGIN_IMPORT => body
            .get(origin::IMPORT_MARK_LEN..)
            .and_then(|txn| kv_store::write_bound(KV_OP_TXN, txn))
            .map(|b| b + 2),
        types::KV_OP_ORIGIN_CHUNK => kv_store::write_bound(types::KV_OP_PUT_CHUNK, body),
        types::KV_OP_ORIGIN_REGISTER => Some(1),
        _ => kv_store::write_bound(op, body),
    }
}

/// May a write be applied now? A disk store admits a write only with
/// room for every memtable entry it can add ([`write_bound`]):
/// the engine applies an op one version at a time, so a write that met a
/// full memtable part-way would leave its earlier versions applied and
/// the rest refused. An op the body does not bound (or one larger than
/// the memtable) is admitted only while no flush holds the memtable —
/// then making room is a synchronous flush inside the op. Reads, and the
/// memory store, always pass. A write not admitted asks for a flush; the
/// caller waits (a command stays on its channel, a provider call drives
/// maintenance) and asks again.
unsafe fn admit(worker: &mut WorkerState, op: u8, body: &[u8]) -> bool {
    if worker.state_store != STATE_STORE_DISK || op_only_reads(op) {
        return true;
    }
    let s = &worker.disk_state;
    if s.apply_excluded() {
        return false;
    }
    let full = kv_store::disk_store::MEMTABLE_MAX_ENTRIES;
    let room = full.saturating_sub(s.mem_count as usize);
    match write_bound(op, body).filter(|b| *b <= full) {
        Some(b) if b <= room => true,
        Some(_) => {
            worker.disk_flush_wanted = true;
            false
        }
        None => !s.flush_active(),
    }
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
    // A write waits on the channel, unread, until the store has room for
    // it: the channel stays FIFO, so nothing behind it (a later write, a
    // clock tick, a position marker) passes it.
    if !head_admitted(worker) {
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
            if worker.state_store == STATE_STORE_DISK {
                // Stamp the store's pending applied position: every
                // command ahead of this marker is in the memtable, so
                // the NEXT flush's frozen prefix covers exactly this
                // position and the manifest publish records it.
                worker.disk_state.pending_applied_index = index;
                worker.disk_state.pending_applied_term = term;
            }
            // Every write the store provider proposed that has applied by
            // here is decided at this position.
            #[cfg(feature = "store_provider")]
            if worker.prov.provides {
                store_provider::origin_position(&mut *worker.prov.state, term, index);
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
        // starts at or above the previous one's end (allocator
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
    // the canonical identity the store is scoped to.
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

/// Keep the reply in `scratch[..total]` until `responses` has room.
fn stash_reply(worker: &mut WorkerState, total: usize) -> bool {
    let WorkerState {
        reply_stash,
        scratch,
        ..
    } = &mut *worker;
    match (reply_stash.get_mut(..total), scratch.get(..total)) {
        (Some(d), Some(src)) => {
            d.copy_from_slice(src);
            worker.reply_stash_len = total;
            true
        }
        _ => false,
    }
}

/// Write a waiting reply: whether none waits now.
unsafe fn flush_reply(worker: &mut WorkerState) -> bool {
    let len = worker.reply_stash_len;
    if len == 0 {
        return true;
    }
    if worker.syscalls.is_null() || worker.responses_out < 0 {
        return false;
    }
    let sys = &*worker.syscalls;
    if (sys.channel_write)(worker.responses_out, worker.reply_stash.as_mut_ptr(), len) == len as i32
    {
        worker.reply_stash_len = 0;
        return true;
    }
    false
}

/// The revision the store has reached.
fn store_revision(worker: &WorkerState) -> u64 {
    if worker.state_store == STATE_STORE_DISK {
        worker.disk_revision
    } else {
        worker.store.revision
    }
}

/// Propose a chunk sweep when one may have work (large values): the
/// store keeps a chunk while any record or
/// retained history can name it, and a sweep — one ordered command,
/// applied alike by every replica — reclaims what nothing can.
///
/// The floor is the one this worker saw commit: the committed GC floor on
/// the disk store, whose history readers and replay read as-of; the
/// memory store keeps no history, so every release is due at once (its
/// grace, on the committed clock, is what covers a reader or producer
/// mid-transfer). The proposal decides nothing: the floor travels in the
/// command, and a floor that committed is safe on any replica.
unsafe fn propose_chunk_sweep(worker: &mut WorkerState) {
    if worker.sweep_out < 0 || worker.syscalls.is_null() {
        return;
    }
    let floor = if worker.state_store == STATE_STORE_DISK {
        worker.gc_committed_floor
    } else {
        u64::MAX
    };
    let since = worker.step_ctr.wrapping_sub(worker.sweep_step);
    if worker.sweep_inflight && since < SWEEP_RETRY_STEPS {
        return;
    }
    let due = worker.sweep_flags & kv_store::SWEEP_MORE != 0
        || (worker.sweep_armed && since >= SWEEP_INTERVAL_STEPS)
        || (worker.sweep_flags & kv_store::SWEEP_WAITING != 0
            && (since >= SWEEP_WAIT_STEPS || floor > worker.sweep_floor));
    if !due {
        return;
    }
    // Unreferenced chunks queued before the grace began. A clock that has
    // not started names 0, which takes none.
    let queued_before_ms = worker
        .committed_now_ms
        .saturating_sub(u64::from(worker.sweep_grace_ms));
    let mut body = [0u8; kv_store::SWEEP_BODY_LEN];
    body[..8].copy_from_slice(&floor.to_le_bytes());
    body[8..16].copy_from_slice(&queued_before_ms.to_le_bytes());
    // As many queue entries as the store lets one sweep take.
    let budget = u16::try_from(kv_store::SWEEP_BUDGET_MAX).unwrap_or(u16::MAX);
    body[16..].copy_from_slice(&budget.to_le_bytes());
    let head = wire::KvCommandHead {
        corr_id: 0,
        kpg_id: worker.gc_claim_kpg,
        conn_id: 0,
        consistency: 0,
        op: types::KV_OP_CHUNK_SWEEP,
        tenant: 0,
        database: 0,
        keyspace: 0,
        commit_ts: 0,
        body_len: body.len() as u16,
    };
    // [type][len:u16] + (proposal: [conn:u16=0][LATTICE_ENTRY_TAG]) +
    // head + body.
    const CMD_LEN: usize = wire::KvCommandHead::LEN + kv_store::SWEEP_BODY_LEN;
    let mut frame = [0u8; 3 + 3 + CMD_LEN];
    let pre = if worker.sweep_wrap == 0 {
        frame[0] = MSG_KV_COMMAND;
        3
    } else {
        frame[0] = wire::MSG_CLIENT_PROPOSAL;
        frame[5] = wire::LATTICE_ENTRY_TAG;
        6
    };
    let payload_len = pre - 3 + CMD_LEN;
    frame[1..3].copy_from_slice(&(payload_len as u16).to_le_bytes());
    if !head.encode(&mut frame[pre..]) {
        return;
    }
    frame[pre + wire::KvCommandHead::LEN..pre + CMD_LEN].copy_from_slice(&body);
    let total = pre + CMD_LEN;
    let sys = &*worker.syscalls;
    if (sys.channel_write)(worker.sweep_out, frame.as_mut_ptr(), total) == total as i32 {
        worker.sweep_inflight = true;
        worker.sweep_step = worker.step_ctr;
        worker.sweep_floor = floor;
        worker.sweep_armed = false;
        worker.sweep_flags = 0;
        worker.sweeps_proposed = worker.sweeps_proposed.wrapping_add(1);
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

    // A store provider's write: applied by its origin's record, and
    // answered (to its origin) by the provider, never here.
    if matches!(
        op,
        types::KV_OP_ORIGIN_REGISTER
            | types::KV_OP_ORIGIN_WRITE
            | types::KV_OP_ORIGIN_IMPORT
            | types::KV_OP_ORIGIN_CHUNK
    ) {
        apply_origin_entry(worker, &head, body);
        return CmdOutcome::Answered(true);
    }

    // The store answers into exactly the room `MSG_KV_APPLIED` has for a
    // body: an answer that filled the whole scratch could not be sent,
    // and the command would go unanswered.
    const RESULT_BODY_MAX: usize = SCRATCH_BUF_SIZE - 3 - wire::KvAppliedHead::LEN - 8;
    let mut result_body = [0u8; RESULT_BODY_MAX];
    let now_ms = worker.committed_now_ms;
    let applied = if origin::is_provider_keyspace(id_keyspace) {
        // The store provider's own records (its origins, its import):
        // only the worker writes or reads them.
        Applied {
            result: types::KV_RESULT_RESERVED,
            len: 0,
            revision: store_revision(worker),
            paused: false,
            busy: false,
            changed: false,
        }
    } else {
        apply_to_store(
            worker,
            op,
            (id_tenant, id_database, id_keyspace),
            consistency,
            head.commit_ts,
            body,
            &mut result_body,
        )
    };
    if applied.paused {
        return CmdOutcome::Paused;
    }
    let (result, result_body_len, result_revision) =
        (applied.result, applied.len, applied.revision);
    // A sweep is store maintenance this worker (or a replica's) proposed:
    // nobody awaits its answer, which says only what is left to do.
    if op == types::KV_OP_CHUNK_SWEEP {
        worker.sweeps_applied = worker.sweeps_applied.wrapping_add(1);
        worker.sweep_inflight = false;
        worker.sweep_flags = if result == KV_RESULT_OK {
            result_body[0]
        } else {
            0
        };
        return CmdOutcome::Answered(true);
    }
    // A write may have queued chunk work: a release, an upload.
    if applied.changed {
        worker.sweep_armed = true;
    }
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
    let ok = n == total as i32 || stash_reply(worker, total);

    // After the response lands, fan a `MSG_WATCH_EVENT` envelope out to
    // watch_registry for any successful PUT / DELETE. This is the
    // event-emission half of the substrate-fenced watch path: anchor
    // creates the watch, registry stamps the epoch, fanout pushes
    // frames, and the worker publishes the
    // mutations. INCR/DECR/APPEND/PREPEND/MSET/CAS/TXN are mutations
    // too but live on the redis-protocol surface only; they'll fold
    // in once the watch matcher learns to project compound ops.
    // Watch events are the flat protocol keyspace's: a reserved keyspace
    // (a model's, etcd's auth records) is no watcher's business, and a
    // watch matches keys, not identities.
    if ok && id_database == 0 && id_keyspace == 0 {
        publish_mutations(
            worker,
            op,
            kpg_id,
            result,
            result_revision,
            body,
            &result_body[..result_body_len],
        );
    }
    CmdOutcome::Answered(ok)
}

/// The store an origin entry applies to: this worker's, under the flat
/// identity, as a command applies.
struct OriginStore<'w> {
    worker: &'w mut WorkerState,
    kpg_id: u16,
}

impl origin::Store for OriginStore<'_> {
    fn apply(&mut self, keyspace: u32, op: u8, body: &[u8], out: &mut [u8]) -> (u8, usize) {
        let consistency = kv_store::ReadPolicy::LATEST.consistency;
        // SAFETY: the worker's own state, applying a command.
        let a =
            unsafe { apply_to_store(self.worker, op, (0, 0, keyspace), consistency, 0, body, out) };
        if a.changed {
            self.worker.sweep_armed = true;
        }
        (a.result, a.len)
    }

    fn revision(&self) -> u64 {
        store_revision(self.worker)
    }

    fn wrote(&mut self, txn: &[u8], result: u8, answer: &[u8]) {
        let revision = store_revision(self.worker);
        // SAFETY: the worker's own state and channels.
        unsafe {
            publish_mutations(
                self.worker,
                KV_OP_TXN,
                self.kpg_id,
                result,
                revision,
                txn,
                answer,
            );
        }
    }
}

/// Apply an origin entry (`common/origin.rs`) and tell this worker's store
/// provider what it came to. A store failing one is said: the replica can
/// no longer follow the log faithfully.
unsafe fn apply_origin_entry(worker: &mut WorkerState, head: &wire::KvCommandHead, body: &[u8]) {
    let mut answer = [0u8; 512];
    let a = origin::apply_entry(
        &mut OriginStore {
            worker: &mut *worker,
            kpg_id: head.kpg_id,
        },
        head.op,
        head.corr_id,
        body,
        &mut answer,
    );
    if matches!(a, origin::Applied::Faulted | origin::Applied::Refused) {
        let (replica, inc, seq) = origin::split(head.corr_id);
        let mut buf = [0u8; 112];
        let mut n = write_prefix(&mut buf, 0, b"[kvw] origin entry ");
        n = if matches!(a, origin::Applied::Faulted) {
            write_prefix(&mut buf, n, b"faulted")
        } else {
            write_prefix(&mut buf, n, b"refused")
        };
        n = write_prefix(&mut buf, n, b" op=");
        n = write_dec(&mut buf, n, u64::from(head.op));
        n = write_prefix(&mut buf, n, b" replica=");
        n = write_dec(&mut buf, n, u64::from(replica));
        n = write_prefix(&mut buf, n, b" inc=");
        n = write_dec(&mut buf, n, u64::from(inc));
        n = write_prefix(&mut buf, n, b" seq=");
        n = write_dec(&mut buf, n, u64::from(seq));
        log_line(worker, &buf, n);
    }
    #[cfg(feature = "store_provider")]
    if worker.prov.provides {
        store_provider::origin_applied(&mut *worker.prov.state, &a);
    }
}

/// Fan what a command changed out on `mutations_out` as
/// `MSG_WATCH_EVENT`s (the flat protocol keyspace's changes only: the
/// caller checks the identity). The store's journal says what changed,
/// with each key's metadata and the version it replaced; a command it
/// could not journal whole falls back to reading the events off the
/// command.
unsafe fn publish_mutations(
    worker: &mut WorkerState,
    op: u8,
    kpg_id: u16,
    result: u8,
    revision: u64,
    body: &[u8],
    result_body: &[u8],
) {
    if worker.mutations_out < 0 {
        return;
    }
    let disk = worker.state_store == STATE_STORE_DISK;
    let (jlen, joverflow) = if disk {
        (worker.events.len, worker.events.overflow)
    } else {
        (worker.store.events.len, worker.store.events.overflow)
    };
    if jlen > 0 && !joverflow {
        emit_journal(worker, kpg_id, revision, disk);
    } else {
        emit_watch_event(worker, op, kpg_id, result, revision, body, result_body);
    }
}

/// What applying one op to the store came to.
struct Applied {
    result: u8,
    /// The answer's length in `out`.
    len: usize,
    /// The revision the answer is as of.
    revision: u64,
    /// The engine paused a walk on its step budget: apply the same op
    /// again to continue it.
    paused: bool,
    /// A write refused for now: the memtable is full while a flush
    /// drains it (retry; the flush finishes on its own steps).
    busy: bool,
    /// The store's revision moved (a write may have queued chunk work).
    changed: bool,
}

/// Apply one op to the store under `identity`: the one place a command,
/// and every call the store provider makes, reaches the store. Stamps
/// the commit timestamp, keeps the catalog generation, counts, and
/// accounts for disk faults, exactly as it always has for commands.
unsafe fn apply_to_store(
    worker: &mut WorkerState,
    op: u8,
    identity: (u32, u32, u32),
    consistency: u8,
    head_commit_ts: u64,
    body: &[u8],
    out: &mut [u8],
) -> Applied {
    // The committed clock. `kv_store::apply` uses this as `now_ms` for
    // absolute-deadline assignment and expiry filtering, so a deadline
    // is a function of the log position that set it and never of the
    // applying node's local timer.
    let now_ms = worker.committed_now_ms;
    // Read policy. `MSG_KV_COMMAND` carries the
    // `Consistency` byte but not yet a `read_timestamp` field, so the
    // only way a request reaches this worker asking for a historical
    // revision today is the native `KV_OP_GET_AT` / `KV_OP_SCAN_AT`
    // ops, which carry it in the op body. Threading the policy through
    // anyway means the envelope extension is a one-line change here
    // rather than a re-plumb of the engine.
    // Catalog generation (the schema generation, used by
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
    let commit_ts = if worker.clog_phase == CLOG_REPLAYING {
        // A logged op replays with exactly the timestamp it ran under.
        head_commit_ts
    } else if head_commit_ts != 0 {
        head_commit_ts
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
    let revision_before = store_revision(worker);
    let mut busy = false;
    // ONE semantic engine (`kv_store::apply_mat_ctx`), provider selected
    // at graph construction: the interpreter is identical on both arms —
    // only the physical materializer differs.
    let (id_tenant, id_database, id_keyspace) = identity;
    let (mut result, mut len, revision) = if kv_store::kv_chunks::is_chunk_keyspace(id_keyspace) {
        // The store's own chunk records: only the chunk ops, which bind
        // these identities themselves, write or read them.
        let rev = if worker.state_store == STATE_STORE_DISK {
            worker.disk_revision
        } else {
            worker.store.revision
        };
        (types::KV_RESULT_RESERVED, 0, rev)
    } else if worker.state_store == STATE_STORE_DISK {
        let store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        let mut mat = DiskMaterializer::new(store, worker.disk_revision, worker.disk_replay_floor);
        mat.flush_wanted = worker.disk_flush_wanted;
        mat.set_key_identity(id_tenant, id_database, id_keyspace);
        mat.set_commit_ts(commit_ts);
        worker.events.clear();
        if worker.mutations_out >= 0 {
            mat.events = Some(&mut worker.events);
        }
        let mem_before = mat.store.state.mem_count as usize;
        let (r, n) = kv_store::apply_mat_ctx(&mut mat, op, body, out, now_ms, policy);
        let mem_after = mat.store.state.mem_count as usize;
        worker.disk_revision = mat.revision;
        worker.disk_flush_wanted = mat.flush_wanted;
        // Latch both outcome flags before any `worker` field is touched
        // again: `mat` mutably borrows `worker.disk_state`, so reading
        // them up front is what releases the borrow.
        let mat_backpressure = mat.backpressure;
        let mat_fault = mat.fault;
        let mat_reply_revision = mat.reply_revision;
        if r == KV_RESULT_PAUSED {
            return Applied {
                result: r,
                len: 0,
                revision: worker.disk_revision,
                paused: true,
                busy: false,
                changed: false,
            };
        }
        busy = mat_backpressure;
        // An op wrote more versions than its bound said it could: the
        // bound is wrong, and admission could let that op meet a full
        // memtable part-way. Said, and counted, every time.
        if let Some(bound) = kv_store::write_bound(op, body) {
            let wrote = mem_after.saturating_sub(mem_before);
            if mem_after >= mem_before && wrote > bound {
                worker.m_write_bound_exceeded = worker.m_write_bound_exceeded.wrapping_add(1);
                let mut buf = [0u8; 80];
                let mut n = write_prefix(&mut buf, 0, b"[kvw] write bound exceeded op=");
                n = write_dec(&mut buf, n, u64::from(op));
                n = write_prefix(&mut buf, n, b" wrote=");
                n = write_dec(&mut buf, n, wrote as u64);
                n = write_prefix(&mut buf, n, b" bound=");
                n = write_dec(&mut buf, n, bound as u64);
                log_line(worker, &buf, n);
            }
        }
        if mat_backpressure {
            // A write refused part-way. Every write is admitted only with
            // room for all it adds, so this is an admission bound broken:
            // the versions before the refusal are applied and the rest are
            // not — an op half done. The store stops rather than answer,
            // log or retry a half-applied op (a retry would see its own
            // partial writes as done).
            worker.disk_flush_backpressure = worker.disk_flush_backpressure.wrapping_add(1);
            let mut buf = [0u8; 96];
            let mut n = write_prefix(
                &mut buf,
                0,
                b"[kvw] write refused part-way after admission, QUARANTINE op=",
            );
            n = write_dec(&mut buf, n, u64::from(op));
            n = write_prefix(&mut buf, n, b" mem=");
            n = write_dec(&mut buf, n, u64::from(worker.disk_state.mem_count));
            log_line(worker, &buf, n);
            worker.disk_phase = DISK_PHASE_QUARANTINED;
        }
        if mat_fault {
            // In-command storage fault, from `DiskMaterializer::
            // make_room` — a flush/compact driven synchronously by a
            // write that found the memtable FULL (512), entirely
            // separate from `disk_maintenance`'s watermark flush (384).
            // This command still answers (KV_RESULT_INTERNAL from the
            // engine); the question is what happens to the store.
            //
            // Quarantining unconditionally here would silently bypass
            // the whole `DISK_HARD_FAULT_BUDGET` and never touch a
            // counter — one transient device refusal would kill the
            // store with no trace. Same rules as maintenance apply:
            // transient is patience, hard faults spend the budget,
            // and either way it is counted.
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
        worker.store.events.clear();
        worker.store.journaling = worker.mutations_out >= 0;
        let (r, n) = kv_store::apply_mat_ctx(&mut worker.store, op, body, out, now_ms, policy);
        (r, n, worker.store.revision)
    };
    // The worker's own log, when nothing orders it: the op is durable
    // before anyone is answered. A log that cannot take it stops the
    // store — an answer the log does not hold would be undone by a crash.
    if worker.clog_phase == CLOG_LIVE && !op_only_reads(op) && !busy {
        let rec = command_log::Record {
            commit_ts,
            now_ms,
            identity,
            consistency,
            op,
            room: out.len() as u32,
            body,
        };
        let frozen = worker.disk_state.last_freeze();
        let mut appended = worker.clog.append(
            &mut worker.fs_storage,
            &rec,
            revision_before,
            frozen,
            !worker.clog_defer,
        );
        // Every segment is taken: reclaim those the runs already hold (as
        // the step's maintenance would) and try once more. A writer that
        // keeps the store busy for many flushes inside one step reaches
        // this before any step runs maintenance. `Full` is answered before
        // a byte is written, so the retry appends the record once.
        if appended == Err(command_log::LogError::Full) {
            let durable = worker.disk_state.durable_highest_revision;
            if durable > worker.clog_reclaimed
                && worker.clog.reclaim(&mut worker.fs_storage, durable).is_ok()
            {
                worker.clog_reclaimed = durable;
                appended = worker.clog.append(
                    &mut worker.fs_storage,
                    &rec,
                    revision_before,
                    frozen,
                    !worker.clog_defer,
                );
            }
        }
        match appended {
            Ok(()) => worker.clog_appends = worker.clog_appends.wrapping_add(1),
            Err(_) => {
                log_disk_errno(
                    worker,
                    b"[kvw] command log append failed, QUARANTINE errno=",
                );
                worker.disk_phase = DISK_PHASE_QUARANTINED;
                result = types::KV_RESULT_INTERNAL;
                len = 0;
            }
        }
    }
    // Single chokepoint: every command reaching the store is applied here.
    worker.m_applied = worker.m_applied.wrapping_add(1);
    // An etcd Compact is a committed command: it authorizes reclaiming
    // history below its revision, exactly as a committed GC floor does.
    if op == KV_OP_COMPACT && result == KV_RESULT_OK && body.len() >= 8 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&body[..8]);
        let rev = u64::from_le_bytes(b);
        if rev > worker.gc_committed_floor {
            worker.gc_committed_floor = rev;
            worker.gc_floor_commits = worker.gc_floor_commits.wrapping_add(1);
        }
    }
    Applied {
        result,
        len,
        revision,
        paused: false,
        busy,
        changed: store_revision(worker) != revision_before,
    }
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

/// Is the frame at the head of `commands` admitted ([`admit`])? Peeked,
/// not read. Anything but a well-formed command passes (it is read and
/// handled, or refused, as before).
unsafe fn head_admitted(worker: &mut WorkerState) -> bool {
    // The memory store admits everything: no peek at all.
    if worker.state_store != STATE_STORE_DISK {
        return true;
    }
    let sys = &*worker.syscalls;
    const HEAD: usize = 3 + wire::KvCommandHead::LEN;
    let mut hdr = [0u8; HEAD];
    let n = (sys.channel_peek)(worker.commands_in, hdr.as_mut_ptr(), HEAD);
    if n < HEAD as i32 || hdr[0] != MSG_KV_COMMAND {
        return true;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if !(wire::KvCommandHead::LEN..=SCRATCH_BUF_SIZE).contains(&payload_len) {
        return true;
    }
    let Some(head) = wire::KvCommandHead::decode(&hdr[3..]) else {
        return true;
    };
    // A read needs no room: the rest of the frame is not peeked.
    if op_only_reads(head.op) {
        return true;
    }
    let mut frame = [0u8; 3 + SCRATCH_BUF_SIZE];
    let got = (sys.channel_peek)(worker.commands_in, frame.as_mut_ptr(), 3 + payload_len);
    if got != (3 + payload_len) as i32 {
        return true;
    }
    let body_off = 3 + wire::KvCommandHead::LEN;
    let body = frame
        .get(body_off..body_off + head.body_len as usize)
        .unwrap_or(&[]);
    admit(worker, head.op, body)
}

/// Emit `MSG_WATCH_EVENT` for the just-applied mutation, if any: one
/// per PUT, one per key a DELETE removed. Wire shape
/// (`modules/common/wire.rs::MSG_WATCH_EVENT`):
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
    result_body: &[u8],
) {
    let success = matches!(
        result,
        KV_RESULT_OK | KV_RESULT_INTEGER | KV_RESULT_KVS | KV_RESULT_ETCD_TXN
    );
    if !success {
        return;
    }
    match op {
        // etcd's Txn: each write of the branch that ran, as its own op
        // would have emitted it (nested transactions recurse).
        KV_OP_ETCD_TXN => emit_txn_events(worker, kpg_id, revision, cmd_body, result_body),
        // etcd's DeleteRange: the answer lists the deleted keys
        // (`[deleted:u64][more:u8][n:u16]` + KVS entries); one DELETE
        // event per key, as KV_OP_DELETE emits them.
        // etcd's DeleteRange, and a lease's keys deleted with the lease.
        KV_OP_DELETE_META | KV_OP_LEASE_REVOKE => {
            if result_body.len() < 11 {
                return;
            }
            let deleted = u64::from_le_bytes([
                result_body[0],
                result_body[1],
                result_body[2],
                result_body[3],
                result_body[4],
                result_body[5],
                result_body[6],
                result_body[7],
            ]);
            if deleted == 0 {
                return;
            }
            let n = u16::from_le_bytes([result_body[9], result_body[10]]) as usize;
            let mut p = 11usize;
            for _ in 0..n {
                if p + 2 > result_body.len() {
                    return;
                }
                let kl = u16::from_le_bytes([result_body[p], result_body[p + 1]]) as usize;
                let vat = p + 2 + kl;
                if vat + 4 > result_body.len() {
                    return;
                }
                let vl = u32::from_le_bytes([
                    result_body[vat],
                    result_body[vat + 1],
                    result_body[vat + 2],
                    result_body[vat + 3],
                ]) as usize;
                // Emitted as a plain DELETE, the op the registry's filter
                // taxonomy knows.
                emit_one_event(
                    worker,
                    kpg_id,
                    revision,
                    KV_OP_DELETE,
                    &result_body[p + 2..vat],
                    &[],
                );
                p = vat + 4 + vl + 32;
            }
        }
        // A manifest PUT has a PUT's body; its event keeps its op, so a
        // watcher knows the value is a large value's manifest.
        KV_OP_PUT | types::KV_OP_PUT_MANIFEST => {
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
            emit_one_event(
                worker,
                kpg_id,
                revision,
                op,
                key,
                &cmd_body[value_off..value_off + value_len],
            );
        }
        KV_OP_DELETE => {
            // DELETE body: [key_count:u16 LE] then per key
            // [key_len:u16 LE][key…] (types.rs). The reply's INTEGER is
            // how many existed: a delete of nothing is no event (etcd
            // emits DELETE only for a key that was there).
            let deleted = if result == KV_RESULT_INTEGER && result_body.len() >= 8 {
                i64::from_le_bytes([
                    result_body[0],
                    result_body[1],
                    result_body[2],
                    result_body[3],
                    result_body[4],
                    result_body[5],
                    result_body[6],
                    result_body[7],
                ])
            } else {
                1
            };
            if deleted <= 0 || cmd_body.len() < 2 {
                return;
            }
            let count = u16::from_le_bytes([cmd_body[0], cmd_body[1]]) as usize;
            let mut p = 2;
            for _ in 0..count {
                if p + 2 > cmd_body.len() {
                    return;
                }
                let kl = u16::from_le_bytes([cmd_body[p], cmd_body[p + 1]]) as usize;
                p += 2;
                if p + kl > cmd_body.len() {
                    return;
                }
                emit_one_event(worker, kpg_id, revision, op, &cmd_body[p..p + kl], &[]);
                p += kl;
            }
        }
        _ => {}
    }
}

/// The watch events of a `KV_OP_ETCD_TXN`: the compares are skipped, the
/// branch the answer says ran is walked op by op beside its per-op
/// answers (types.rs), and each op emits as it would alone.
unsafe fn emit_txn_events(
    worker: &mut WorkerState,
    kpg_id: u16,
    revision: u64,
    body: &[u8],
    result: &[u8],
) {
    let rd16 = |b: &[u8], at: usize| {
        b.get(at..at + 2)
            .map(|x| u16::from_le_bytes([x[0], x[1]]) as usize)
    };
    let rd32 = |b: &[u8], at: usize| {
        b.get(at..at + 4)
            .map(|x| u32::from_le_bytes([x[0], x[1], x[2], x[3]]) as usize)
    };
    let (Some(&succeeded), Some(&count)) = (result.first(), result.get(1)) else {
        return;
    };
    let Some(&ncmp) = body.first() else {
        return;
    };
    let mut off = 1usize;
    for _ in 0..ncmp {
        // [target][result][key_len][key][end_len][end][operand:8][vlen:4][value]
        off += 2;
        let Some(kl) = rd16(body, off) else { return };
        off += 2 + kl;
        let Some(el) = rd16(body, off) else { return };
        off += 2 + el + 8;
        let Some(vl) = rd32(body, off) else { return };
        off += 4 + vl;
    }
    // The `then` branch, then (when it did not run) the `else` branch.
    let skip_branch = |off: &mut usize| -> Option<()> {
        let n = *body.get(*off)?;
        *off += 1;
        for _ in 0..n {
            let len = rd32(body, *off + 1)?;
            *off += 5 + len;
        }
        Some(())
    };
    if succeeded == 0 && skip_branch(&mut off).is_none() {
        return;
    }
    let Some(&n) = body.get(off) else { return };
    off += 1;
    let mut rat = 2usize;
    for _ in 0..n.min(count) {
        let (Some(&kind), Some(len)) = (body.get(off), rd32(body, off + 1)) else {
            return;
        };
        let (Some(&r), Some(rlen)) = (result.get(rat + 1), rd32(result, rat + 2)) else {
            return;
        };
        let (Some(op_body), Some(op_result)) = (
            body.get(off + 5..off + 5 + len),
            result.get(rat + 6..rat + 6 + rlen),
        ) else {
            return;
        };
        let op = match kind {
            2 => KV_OP_PUT,
            3 => KV_OP_DELETE_META,
            4 => KV_OP_ETCD_TXN,
            _ => 0,
        };
        if op != 0 {
            emit_watch_event(worker, op, kpg_id, r, revision, op_body, op_result);
        }
        off += 5 + len;
        rat += 6 + rlen;
    }
}

/// The `MSG_WATCH_EVENT`s of one command's journal ([`EventJournal`]),
/// each with its metadata tail: `[create:8][version:8][lease:8][prev:1]`
/// and, when `prev` is not 0 (2: its value is a manifest), the replaced
/// version
/// `[pvlen:2][pvalue][pcreate:8][pmod:8][pversion:8][please:8]`. A put
/// that created its key names this revision as its create revision.
unsafe fn emit_journal(worker: &mut WorkerState, kpg_id: u16, revision: u64, disk: bool) {
    let sys = worker.syscalls;
    if sys.is_null() {
        return;
    }
    let chan = worker.mutations_out;
    let journal = if disk {
        &worker.events
    } else {
        &worker.store.events
    };
    let j = &journal.buf[..journal.len.min(journal.buf.len())];
    let scratch = &mut worker.event_buf;
    let rd16 = |at: usize| -> Option<usize> {
        Some(u16::from_le_bytes([*j.get(at)?, *j.get(at + 1)?]) as usize)
    };
    let mut at = 0usize;
    while at < j.len() {
        let Some(klen) = rd16(at + 1) else { return };
        let Some(vlen) = rd16(at + 3 + klen) else {
            return;
        };
        let meta_at = at + 5 + klen + vlen;
        let Some(&has_prev) = j.get(meta_at + 24) else {
            return;
        };
        let mut end = meta_at + 25;
        if has_prev != 0 {
            let Some(pvlen) = rd16(end) else { return };
            end += 2 + pvlen + 32;
        }
        if end > j.len() {
            return;
        }
        // [kpg:2][rev:8] + the entry from its op byte on.
        let entry = &j[at..end];
        let payload_len = 2 + 8 + entry.len();
        let total = 3 + payload_len;
        if payload_len <= u16::MAX as usize && total <= scratch.len() {
            let b = &mut scratch[..total];
            b[0] = MSG_WATCH_EVENT;
            b[1..3].copy_from_slice(&(payload_len as u16).to_le_bytes());
            b[3..5].copy_from_slice(&kpg_id.to_le_bytes());
            b[5..13].copy_from_slice(&revision.to_le_bytes());
            b[13..].copy_from_slice(entry);
            // A put that created its key: its create revision is this one.
            let c = 13 + (meta_at - at);
            if b[c..c + 8] == [0; 8] && entry[0] == KV_OP_PUT {
                b[c..c + 8].copy_from_slice(&revision.to_le_bytes());
            }
            let _ = ((*sys).channel_write)(chan, scratch.as_mut_ptr(), total);
        }
        at = end;
    }
}

/// One `MSG_WATCH_EVENT` envelope on `mutations_out`.
unsafe fn emit_one_event(
    worker: &mut WorkerState,
    kpg_id: u16,
    revision: u64,
    event_op: u8,
    key: &[u8],
    value: &[u8],
) {
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

// ── Fluxor's store: the storage.object / storage.namespace provider ──
//
// The `kv_state_worker_store` build (feature `store_provider`) serves
// fluxor's store contracts over this worker's store, in the calling
// module's provider call (`modules/common/store_provider.rs`): a graph
// whose store is lattice's reaches it by contract, as it reached the host
// store. The role is a disk worker's (a change feed needs the history only
// the disk provider keeps). A single-node worker answers a write in the
// call; a worker whose commands consensus orders (`sweep_wrap` 1) is one
// replica of a replicated store, and proposes a write on `prov_out`
// (wired to consensus's proposals) and answers it once it has applied
// here (`common/origin.rs`). Any other configuration provides nothing,
// and says so.

/// The provider's buffers: a request or event; a store answer; an answer
/// expanded (the largest object, 3 MiB, and its page); one pump's changes.
#[cfg(feature = "store_provider")]
const PROVIDER_REQ: usize = store_provider::REQ_MIN;
#[cfg(feature = "store_provider")]
const PROVIDER_ANS: usize = 65536;
#[cfg(feature = "store_provider")]
const PROVIDER_CACHE: usize = (4 << 20) + 65536;
#[cfg(feature = "store_provider")]
const PROVIDER_FEED: usize = store_provider::FEED_MIN;
/// One host store log record: the largest object (3 MiB) and its key
/// (3 MiB + 4 KiB).
#[cfg(feature = "store_provider")]
const IMPORT_RECORD_MAX: usize = 3149824;
#[cfg(feature = "store_provider")]
const IMPORT_NONE: u8 = 0;
#[cfg(feature = "store_provider")]
const IMPORT_CHECK: u8 = 1;
#[cfg(feature = "store_provider")]
const IMPORT_RUN: u8 = 2;
/// Host log records one step imports.
#[cfg(feature = "store_provider")]
const IMPORT_PER_STEP: u32 = 64;
/// The heap: the provider, its buffers, and the allocator's rounding.
#[cfg(feature = "store_provider")]
const ARENA_BYTES: usize = core::mem::size_of::<store_provider::Provider>()
    + PROVIDER_REQ
    + PROVIDER_ANS
    + PROVIDER_CACHE
    + PROVIDER_FEED
    + IMPORT_RECORD_MAX
    + CLOG_ARENA
    + 64 * 1024;
/// Without the provider, the heap is the command log's (when it is on).
#[cfg(not(feature = "store_provider"))]
const ARENA_BYTES: usize = CLOG_ARENA + 16 * 1024;
declare_module_arena_bytes!(ARENA_BYTES);

/// The provider, held on the heap beside the worker's state (so a call
/// holds both at once), and its buffers.
#[cfg(feature = "store_provider")]
struct ProviderHeap {
    /// This worker serves the contracts (decided at construction).
    provides: bool,
    /// Subscribers have been started at the store's revision.
    open: bool,
    /// The last store call was refused for now (flush backpressure).
    busy: bool,
    /// Construction is not finished: the store is not yet ready (the FS
    /// provider is still coming up), and the loader calls again.
    pending: bool,
    /// The host store log import: not wanted or done, checking whether an
    /// earlier run finished it, or importing (from `import_at`).
    import_phase: u8,
    import_at: u64,
    import_n: u64,
    /// The CRC state of the log's bytes imported so far, and whether the
    /// store holds an import marker.
    import_crc: u32,
    import_marked: bool,
    /// One host log record (its key and value).
    import_buf: *mut u8,
    state: *mut store_provider::Provider,
    req: *mut u8,
    ans: *mut u8,
    cache: *mut u8,
    feed: *mut u8,
    m_calls: u64,
    m_not_serving: u64,
    m_refused: u64,
}

/// The worker's store, as the provider applies to it: every op under the
/// flat protocol identity, exactly as a command is applied, and its
/// changes published on `mutations_out` as a command's are.
#[cfg(feature = "store_provider")]
struct WorkerBackend<'w> {
    worker: &'w mut WorkerState,
}

#[cfg(feature = "store_provider")]
impl store_provider::Backend for WorkerBackend<'_> {
    fn apply(&mut self, op: u8, body: &[u8], out: &mut [u8]) -> (u8, usize) {
        let w = &mut *self.worker;
        // SAFETY: the worker's own state and syscalls, inside a provider
        // call (no step of this module is running).
        unsafe {
            let consistency = kv_store::ReadPolicy::LATEST.consistency;
            // A write is accepted in the call: one the store has no room
            // for yet waits here while the store makes room — the flush or
            // merge driven on — and is applied once, whole ([`admit`]).
            let mut rounds = 0u32;
            while !admit(w, op, body) {
                if rounds >= BUSY_ROUNDS || w.disk_phase != DISK_PHASE_SERVING {
                    w.prov.busy = true;
                    return (types::KV_RESULT_INTERNAL, 0);
                }
                disk_maintenance(w);
                rounds += 1;
            }
            let a = apply_to_store(w, op, (0, 0, 0), consistency, 0, body, out);
            w.prov.busy = a.busy;
            if a.paused {
                return (KV_RESULT_PAUSED, 0);
            }
            if a.changed {
                w.sweep_armed = true;
            }
            // A result no provider op expects (a refusal the contract has
            // no word for) is said here, with the op that met it.
            if !matches!(
                a.result,
                KV_RESULT_OK
                    | KV_RESULT_KVS
                    | types::KV_RESULT_TXN
                    | types::KV_RESULT_VERSIONS
                    | types::KV_RESULT_COMPACTED
                    | types::KV_RESULT_NOT_FOUND
            ) {
                let mut buf = [0u8; 64];
                let mut n = write_prefix(&mut buf, 0, b"[kvw] store op=");
                n = write_dec(&mut buf, n, u64::from(op));
                n = write_prefix(&mut buf, n, b" answered result=");
                n = write_dec(&mut buf, n, u64::from(a.result));
                n = write_prefix(&mut buf, n, b" body=");
                n = write_dec(&mut buf, n, body.len() as u64);
                log_line(w, &buf, n);
            }
            if op_consumes_timestamp(op) {
                let kpg = w.gc_claim_kpg;
                let answer = out.get(..a.len).unwrap_or(&[]);
                publish_mutations(w, op, kpg, a.result, a.revision, body, answer);
            }
            (a.result, a.len)
        }
    }

    fn revision(&self) -> u64 {
        store_revision(self.worker)
    }

    fn history(&self) -> u64 {
        u64::from(self.worker.history_revisions)
    }

    fn durable(&self) -> bool {
        self.worker.state_store == STATE_STORE_DISK
    }

    fn busy(&self) -> bool {
        self.worker.prov.busy
    }

    fn write(&mut self, chan: i32, bytes: &[u8]) -> i32 {
        // SAFETY: the worker's syscall table; the channel copies the bytes.
        unsafe {
            ((*self.worker.syscalls).channel_write)(chan, bytes.as_ptr() as *mut u8, bytes.len())
        }
    }

    fn replicated(&self) -> bool {
        self.worker.sweep_wrap != 0
    }

    fn propose(&mut self, op: u8, corr: u64, parts: &[&[u8]]) -> bool {
        // SAFETY: the worker's own state and syscalls.
        unsafe { propose_entry(self.worker, op, corr, parts) }
    }

    fn caller(&self) -> [u8; 8] {
        let mut owner = [0u8; 8];
        // SAFETY: the worker's syscall table; 8 writable bytes.
        let rc = unsafe {
            ((*self.worker.syscalls).provider_query)(
                -1,
                CALLER_OWNER,
                owner.as_mut_ptr(),
                owner.len(),
            )
        };
        if rc < owner.len() as i32 {
            return [0; 8];
        }
        owner
    }

    fn group(&self) -> store_provider::Group {
        store_provider::Group {
            epoch: self.worker.group_epoch,
            quorum: self.worker.group_quorum,
            durable: self.worker.group_durable != 0,
        }
    }
}

/// `query_key::CALLER_OWNER`: the module a provider call serves.
#[cfg(feature = "store_provider")]
const CALLER_OWNER: u32 = 9;
/// `kernel_abi::RANDOM_FILL`.
#[cfg(feature = "store_provider")]
const RANDOM_FILL: u32 = 0x0C3C;

// An origin entry is the largest proposal the replicated path carries.
#[cfg(feature = "store_provider")]
const _: () = assert!(
    store_provider::ENTRY_BODY_MAX
        == wire::CONSENSUS_PROPOSAL_BODY_MAX - 1 - wire::KvCommandHead::LEN
);

/// Offer one origin entry to consensus on `prov_out`, whole or not at
/// all: `[MSG_CLIENT_PROPOSAL][len][LATTICE_ENTRY_TAG][head][body]`, the
/// body `parts` one after another.
#[cfg(feature = "store_provider")]
unsafe fn propose_entry(worker: &mut WorkerState, op: u8, corr: u64, parts: &[&[u8]]) -> bool {
    if worker.prov_out < 0 || worker.syscalls.is_null() {
        return false;
    }
    let body_len: usize = parts.iter().map(|p| p.len()).sum();
    if body_len > store_provider::ENTRY_BODY_MAX {
        return false;
    }
    let mut frame = [0u8; 3 + wire::CONSENSUS_PROPOSAL_BODY_MAX];
    let payload = 1 + wire::KvCommandHead::LEN + body_len;
    frame[0] = wire::MSG_CLIENT_PROPOSAL;
    frame[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
    frame[3] = wire::LATTICE_ENTRY_TAG;
    let head = wire::KvCommandHead {
        corr_id: corr,
        kpg_id: worker.gc_claim_kpg,
        conn_id: 0,
        consistency: 0,
        op,
        tenant: 0,
        database: 0,
        keyspace: 0,
        commit_ts: 0,
        body_len: body_len as u16,
    };
    if !head.encode(&mut frame[4..]) {
        return false;
    }
    let mut at = 4 + wire::KvCommandHead::LEN;
    for p in parts {
        let Some(d) = frame.get_mut(at..at + p.len()) else {
            return false;
        };
        d.copy_from_slice(p);
        at += p.len();
    }
    let sys = &*worker.syscalls;
    (sys.channel_write)(worker.prov_out, frame.as_mut_ptr(), at) == at as i32
}

/// Take the provider's heap and decide the role, at construction.
#[cfg(feature = "store_provider")]
unsafe fn provider_new(worker: &mut WorkerState) {
    let sys = &*worker.syscalls;
    let replicated = worker.sweep_wrap != 0;
    // A replica proposes its writes, registered under a nonce of its own
    // (a restarted replica's must differ); a host store it imports goes
    // through the log too, a record at a time.
    let mut nonce = [0u8; 8];
    let random = !replicated || (sys.provider_call)(-1, RANDOM_FILL, nonce.as_mut_ptr(), 8) == 0;
    let wanted =
        worker.state_store == STATE_STORE_DISK && (!replicated || (worker.prov_out >= 0 && random));
    let p = &mut worker.prov;
    if wanted {
        p.state = (sys.heap_alloc)(core::mem::size_of::<store_provider::Provider>() as u32)
            .cast::<store_provider::Provider>();
        p.req = (sys.heap_alloc)(PROVIDER_REQ as u32);
        p.ans = (sys.heap_alloc)(PROVIDER_ANS as u32);
        p.cache = (sys.heap_alloc)(PROVIDER_CACHE as u32);
        p.feed = (sys.heap_alloc)(PROVIDER_FEED as u32);
        p.provides = !p.state.is_null()
            && !p.req.is_null()
            && !p.ans.is_null()
            && !p.cache.is_null()
            && !p.feed.is_null();
        if p.provides {
            store_provider::Provider::clear(p.state);
            if replicated {
                (*p.state).replicate(worker.replica_id, u64::from_le_bytes(nonce));
            }
            // A scope that cannot be kept is refused rather than ignored:
            // the node's records would land in every node's names.
            if worker.scope_name_len > 0 {
                let name = worker
                    .scope_name
                    .get(..worker.scope_name_len as usize)
                    .unwrap_or(&[]);
                let scoped = worker
                    .scoped
                    .get(..worker.scoped_len as usize)
                    .unwrap_or(&[]);
                p.provides = (*p.state).scope(name, scoped);
                if !p.provides {
                    let m = b"[kvw] scope refused: a name with no '/', prefixes from '/'";
                    dev_log(sys, DISK_LOG_LEVEL, m.as_ptr(), m.len());
                }
            }
        }
        if p.provides && worker.import_path_len > 0 {
            p.import_buf = (sys.heap_alloc)(IMPORT_RECORD_MAX as u32);
            p.provides = !p.import_buf.is_null();
            p.import_phase = IMPORT_CHECK;
        }
    }
    let mut buf = [0u8; 96];
    let mut n = write_prefix(&mut buf, 0, b"[kvw] store provider=");
    n = write_dec(&mut buf, n, u64::from(worker.prov.provides));
    if !worker.prov.provides {
        n = if wanted {
            write_prefix(&mut buf, n, b" (heap refused)")
        } else if worker.state_store != STATE_STORE_DISK {
            write_prefix(&mut buf, n, b" (needs state_store=disk: its history)")
        } else if worker.prov_out < 0 {
            write_prefix(&mut buf, n, b" (replicated: prov_out unwired)")
        } else {
            write_prefix(&mut buf, n, b" (replicated: no random nonce)")
        };
    } else if replicated {
        n = write_prefix(&mut buf, n, b" replica=");
        n = write_dec(&mut buf, n, u64::from(worker.replica_id));
    }
    dev_log(sys, DISK_LOG_LEVEL, buf.as_ptr(), n);
}

/// The provider's buffers, as slices of its heap.
///
/// # Safety
/// `p.provides`: every buffer was allocated at its size.
#[cfg(feature = "store_provider")]
unsafe fn provider_bufs(p: &ProviderHeap) -> store_provider::Bufs<'static> {
    store_provider::Bufs {
        req: core::slice::from_raw_parts_mut(p.req, PROVIDER_REQ),
        ans: core::slice::from_raw_parts_mut(p.ans, PROVIDER_ANS),
        cache: core::slice::from_raw_parts_mut(p.cache, PROVIDER_CACHE),
        feed: core::slice::from_raw_parts_mut(p.feed, PROVIDER_FEED),
    }
}

/// Whether the store is serving; the first time it is, subscribers start
/// at its revision.
#[cfg(feature = "store_provider")]
unsafe fn provider_ready(worker: &mut WorkerState) -> bool {
    if !worker.prov.provides || !store_serving(worker) || worker.prov.import_phase != IMPORT_NONE {
        return false;
    }
    if !worker.prov.open {
        let pv = &mut *worker.prov.state;
        pv.open(&WorkerBackend { worker });
        worker.prov.open = true;
    }
    true
}

/// Once a step: every command this step applied reaches subscribers, and
/// those owed LOST are told as their channels allow.
#[cfg(feature = "store_provider")]
unsafe fn provider_step(worker: &mut WorkerState) {
    if worker.prov.provides && store_serving(worker) && worker.prov.import_phase != IMPORT_NONE {
        // A replica imports through the log: its registration, and the
        // offering of what it proposed, go on meanwhile.
        if worker.sweep_wrap != 0 {
            let pv = &mut *worker.prov.state;
            store_provider::origin_step(
                pv,
                &mut WorkerBackend {
                    worker: &mut *worker,
                },
            );
        }
        provider_import(worker);
        return;
    }
    if !provider_ready(worker) {
        return;
    }
    let pv = &mut *worker.prov.state;
    let mut bufs = provider_bufs(&worker.prov);
    let mut b = WorkerBackend {
        worker: &mut *worker,
    };
    store_provider::pump(pv, &mut b, &mut bufs);
    store_provider::origin_step(pv, &mut b);
    if worker.step_ctr.is_multiple_of(PROVIDER_HB_STEPS) {
        provider_heartbeat(worker);
    }
}

/// Steps between store provider heartbeats.
#[cfg(feature = "store_provider")]
const PROVIDER_HB_STEPS: u64 = 5000;

/// `[kvw] store hb`: the calls served, and where a replica's writes stand.
#[cfg(feature = "store_provider")]
unsafe fn provider_heartbeat(worker: &WorkerState) {
    let p = &worker.prov;
    let mut buf = [0u8; 224];
    let mut n = write_prefix(&mut buf, 0, b"[kvw] store hb calls=");
    n = write_dec(&mut buf, n, p.m_calls);
    n = write_prefix(&mut buf, n, b" not_serving=");
    n = write_dec(&mut buf, n, p.m_not_serving);
    n = write_prefix(&mut buf, n, b" refused=");
    n = write_dec(&mut buf, n, p.m_refused);
    if worker.sweep_wrap != 0 {
        let r = (*p.state).report();
        n = write_prefix(&mut buf, n, b" reg=");
        n = write_dec(&mut buf, n, u64::from(r.registered));
        n = write_prefix(&mut buf, n, b" inc=");
        n = write_dec(&mut buf, n, u64::from(r.inc));
        n = write_prefix(&mut buf, n, b" hwm=");
        n = write_dec(&mut buf, n, u64::from(r.hwm));
        n = write_prefix(&mut buf, n, b" next=");
        n = write_dec(&mut buf, n, u64::from(r.next_seq));
        n = write_prefix(&mut buf, n, b" held=");
        n = write_dec(&mut buf, n, u64::from(r.held));
        n = write_prefix(&mut buf, n, b" in_log=");
        n = write_dec(&mut buf, n, u64::from(r.in_log));
        n = write_prefix(&mut buf, n, b" uploaded=");
        n = write_dec(&mut buf, n, r.uploaded);
    }
    log_line(worker, &buf, n);
}

/// A snapshot replaced the store: what became of the writes the store
/// provider proposed is read from its origin record.
#[cfg(feature = "store_provider")]
unsafe fn provider_resync(worker: &mut WorkerState) {
    if !worker.prov.provides || worker.sweep_wrap == 0 {
        return;
    }
    let (term, index) = (worker.applied_term, worker.applied_index);
    let (kpg_id, replica) = (worker.gc_claim_kpg, worker.replica_id);
    let rec = origin::read_record(
        &mut OriginStore {
            worker: &mut *worker,
            kpg_id,
        },
        replica,
    );
    let pv = &mut *worker.prov.state;
    match rec {
        Ok(r) => store_provider::origin_resync(pv, r.as_ref(), term, index),
        Err(()) => {
            let m = b"[kvw] origin record unreadable after a snapshot install";
            log_line(worker, m, m.len());
        }
    }
}

/// Storage actions a provider write may drive while the store makes room
/// for it (each a bounded slice of a flush or merge).
#[cfg(feature = "store_provider")]
const BUSY_ROUNDS: u32 = 1 << 16;

/// Storage actions construction may take before it leaves the rest to the
/// loader's next call (each is a bounded slice of recovery, replay or
/// import, and every one moves the store on).
#[cfg(feature = "store_provider")]
const READY_ROUNDS: u32 = 1 << 20;

/// Make the store ready before the provider exists: the loader registers
/// it when construction returns Ready, and a consumer's very first call
/// must find the store serving — a host store is ready at open (it
/// replays its log as it opens), and the store's consumers read "ask
/// again" as "absent". So the runs are recovered, the command log's tail
/// replayed and any import done here. `0`: ready (or not a provider);
/// `1`: the FS provider is still coming up — the loader calls again;
/// `-1`: the store cannot serve.
#[cfg(feature = "store_provider")]
unsafe fn provider_make_ready(worker: &mut WorkerState) -> i32 {
    worker.prov.pending = false;
    if !worker.prov.provides {
        return 0;
    }
    for _ in 0..READY_ROUNDS {
        if worker.disk_phase == DISK_PHASE_QUARANTINED {
            return -1;
        }
        if store_serving(worker) {
            if worker.prov.import_phase == IMPORT_NONE {
                let m = b"[kvw] store ready at construction";
                dev_log(&*worker.syscalls, DISK_LOG_LEVEL, m.as_ptr(), m.len());
                return 0;
            }
            // A replica imports through the log, which runs only once the
            // graph steps: it imports then, answering ask-again meanwhile,
            // as it does before its own writes can be decided. Its change
            // feed opens now, so a consumer that subscribed while it
            // imported hears what the import brought once it serves.
            if worker.sweep_wrap != 0 {
                let pv = &mut *worker.prov.state;
                pv.open(&WorkerBackend {
                    worker: &mut *worker,
                });
                worker.prov.open = true;
                let m = b"[kvw] store ready at construction, importing through the log";
                dev_log(&*worker.syscalls, DISK_LOG_LEVEL, m.as_ptr(), m.len());
                return 0;
            }
            provider_import(worker);
            // An import writes as commands do: the memtable flushed and the
            // command log reclaimed behind it, or a long host log fills the
            // command log and the store quarantines itself mid-import.
            disk_maintenance(worker);
            drive_command_log(worker);
            // And the merged-away runs deleted as they retire: nothing
            // else steps while construction imports.
            if !worker.disk_flush_active && !worker.disk_compact_active {
                drain_retirements_now(worker);
            }
            continue;
        }
        let phase = worker.disk_phase;
        disk_maintenance(worker);
        drive_command_log(worker);
        if phase == DISK_PHASE_RECOVERING
            && worker.disk_phase == DISK_PHASE_RECOVERING
            && worker.fs_storage.last_was_again()
        {
            // The FS provider is not up yet: ask to be called again.
            worker.prov.pending = true;
            return 1;
        }
    }
    worker.prov.pending = true;
    1
}

/// The host store log import, a bounded slice per step, before the store
/// serves (see the `import_log` param). A single store applies each record
/// directly and marks the import done at its end; a replica proposes each
/// record through the group's log with its progress marker, so every
/// replica records how far each replica's import of its own node's log
/// has come, and a restart resumes from the marker the log committed.
#[cfg(feature = "store_provider")]
unsafe fn provider_import(worker: &mut WorkerState) {
    let meta = (0, 0, origin::KS_PROVIDER_META);
    let consistency = kv_store::ReadPolicy::LATEST.consistency;
    let mut ans = [0u8; 64];
    // A replica reads how far the import has come once its registration
    // has applied here: everything the log committed before it has too,
    // so the marker is the group's, not a stale view of it.
    if worker.sweep_wrap != 0 && !(*worker.prov.state).report().registered {
        return;
    }
    if worker.prov.import_phase == IMPORT_CHECK {
        let mut body = [0u8; 2 + 32];
        // A replica's own import: it imports its node's log.
        let own = origin::imported_key(worker.replica_id);
        let k: &[u8] = if worker.sweep_wrap != 0 {
            &own
        } else {
            origin::IMPORTED_KEY
        };
        let mut at = 0usize;
        for b in (k.len() as u16).to_le_bytes().iter().chain(k.iter()) {
            if let Some(d) = body.get_mut(at) {
                *d = *b;
            }
            at += 1;
        }
        let a = apply_to_store(
            worker,
            KV_OP_GET,
            meta,
            consistency,
            0,
            body.get(..at).unwrap_or(&[]),
            &mut ans,
        );
        // The marker is how far the log was imported, `[offset:u64]`, and
        // the CRC state of those bytes, `[crc:u32]`: the import resumes
        // there — a harness appends records between runs, as it did to
        // the host store's own log — once the log is shown to be the one
        // it imported (its first `offset` bytes hash as they did).
        let (offset, crc) = if a.result == KV_RESULT_OK && a.len >= 12 {
            worker.prov.import_marked = true;
            (
                le_u64(&ans[..8]),
                u32::from_le_bytes([ans[8], ans[9], ans[10], ans[11]]),
            )
        } else {
            (0, kv_store::disk_store::CRC32_INIT)
        };
        if offset > 0 {
            let path_len = worker.import_path_len as usize;
            let path = core::slice::from_raw_parts(worker.import_path.as_ptr(), path_len);
            // SAFETY: `IMPORT_RECORD_MAX` bytes of this module's heap.
            let buf = core::slice::from_raw_parts_mut(worker.prov.import_buf, IMPORT_RECORD_MAX);
            let mut state = kv_store::disk_store::CRC32_INIT;
            let mut at = 0u64;
            while at < offset {
                let want = ((offset - at) as usize).min(buf.len());
                let Some(dst) = buf.get_mut(..want) else {
                    break;
                };
                match worker.fs_storage.read_path(path, at, dst) {
                    Ok(n) if n > 0 => {
                        state =
                            kv_store::disk_store::crc32_update(state, dst.get(..n).unwrap_or(&[]));
                        at += n as u64;
                    }
                    _ => break,
                }
            }
            if at != offset || state != crc {
                log_disk_errno(
                    worker,
                    b"[kvw] import: store.log is not the log imported (shorter, or rewritten), QUARANTINE errno=",
                );
                worker.disk_phase = DISK_PHASE_QUARANTINED;
                return;
            }
        }
        worker.prov.import_at = offset;
        worker.prov.import_crc = crc;
        worker.prov.import_phase = IMPORT_RUN;
        return;
    }
    let path_len = worker.import_path_len as usize;
    let path = core::slice::from_raw_parts(worker.import_path.as_ptr(), path_len);
    // SAFETY: `IMPORT_RECORD_MAX` bytes of this module's heap, apart from
    // the state.
    let buf = core::slice::from_raw_parts_mut(worker.prov.import_buf, IMPORT_RECORD_MAX);
    // Nobody is answered during an import, and a single store's runs
    // again whole until its end is recorded: its ops need be durable only
    // by then. A replica's records are the log's, each with its marker.
    worker.clog_defer = worker.sweep_wrap == 0;
    import_slice(worker, meta, consistency, path, buf);
    worker.clog_defer = false;
}

/// One slice of the import (see [`provider_import`]).
#[cfg(feature = "store_provider")]
unsafe fn import_slice(
    worker: &mut WorkerState,
    meta: (u32, u32, u32),
    consistency: u8,
    path: &[u8],
    buf: &mut [u8],
) {
    for _ in 0..IMPORT_PER_STEP {
        let at = worker.prov.import_at;
        let mut head = [0u8; store_provider::STORE_LOG_HEAD];
        let got = match worker.fs_storage.read_path(path, at, &mut head) {
            Ok(n) => n,
            // No log at all: nothing to import.
            Err(kv_store::disk_store::run_storage::StorageError::NotFound) => 0,
            Err(_) => {
                log_disk_errno(worker, b"[kvw] import read failed, QUARANTINE errno=");
                worker.disk_phase = DISK_PHASE_QUARANTINED;
                return;
            }
        };
        let rec = if got == head.len() {
            store_provider::store_log_head(&head)
        } else {
            None
        };
        // The log's end (or its torn tail, as the host reads it).
        let Some((op, kl, vl)) = rec.filter(|r| r.1 + r.2 <= IMPORT_RECORD_MAX) else {
            if got == head.len() {
                log_disk_errno(
                    worker,
                    b"[kvw] import: a record past what one holds, QUARANTINE errno=",
                );
                worker.disk_phase = DISK_PHASE_QUARANTINED;
                return;
            }
            import_done(worker, meta, consistency);
            return;
        };
        let Some(body) = buf.get_mut(..kl + vl) else {
            return;
        };
        let mut filled = 0usize;
        while filled < body.len() {
            let from = at + (store_provider::STORE_LOG_HEAD + filled) as u64;
            let rest = body.get_mut(filled..).unwrap_or(&mut []);
            match worker.fs_storage.read_path(path, from, rest) {
                Ok(0) | Err(_) => break,
                Ok(n) => filled += n,
            }
        }
        if filled < body.len() {
            import_done(worker, meta, consistency);
            return;
        }
        let next_at = at + (store_provider::STORE_LOG_HEAD + kl + vl) as u64;
        let crc = kv_store::disk_store::crc32_update(worker.prov.import_crc, &head);
        let next_crc = kv_store::disk_store::crc32_update(crc, body);
        let (key, value) = body.split_at(kl);
        let value = (op != store_provider::STORE_LOG_DELETE).then_some(value);
        let pv = &mut *worker.prov.state;
        let mut b = WorkerBackend {
            worker: &mut *worker,
        };
        let done = if b.worker.sweep_wrap != 0 {
            // Through the log, the import's marker with it.
            match store_provider::import_record(pv, &mut b, key, value, next_at, next_crc) {
                0 => Ok(()),
                e => Err(e),
            }
        } else {
            let mut bufs = provider_bufs(&b.worker.prov);
            match value {
                None => store_provider::delete_key(
                    pv,
                    &mut b,
                    &mut bufs,
                    key,
                    store_provider::PRE_ANY,
                    0,
                ),
                Some(v) => store_provider::put_value(
                    pv,
                    &mut b,
                    &mut bufs,
                    key,
                    v,
                    store_provider::PRE_ANY,
                    0,
                ),
            }
        };
        match done {
            Ok(()) => {}
            // The store is making room, or the log has not decided it yet:
            // the same record next step.
            Err(store_provider::EAGAIN | store_provider::EINPROGRESS) => return,
            Err(_) => {
                log_disk_errno(worker, b"[kvw] import refused a record, QUARANTINE errno=");
                worker.disk_phase = DISK_PHASE_QUARANTINED;
                return;
            }
        }
        worker.prov.import_crc = next_crc;
        worker.prov.import_at = next_at;
        worker.prov.import_n = worker.prov.import_n.wrapping_add(1);
    }
}

/// The log is imported to its end: record how far (and the CRC state of
/// those bytes), so the next start resumes there rather than repeating
/// it, and serve.
#[cfg(feature = "store_provider")]
unsafe fn import_done(worker: &mut WorkerState, meta: (u32, u32, u32), consistency: u8) {
    worker.prov.import_phase = IMPORT_NONE;
    worker.clog_defer = false;
    // A replica's marker applied with each record it imported.
    if worker.sweep_wrap != 0 {
        if worker.prov.import_n > 0 {
            import_logged(worker);
        }
        return;
    }
    if worker.prov.import_n == 0 && worker.prov.import_marked {
        return;
    }
    // What was imported is durable before its end is recorded.
    if worker.clog_phase == CLOG_LIVE && worker.clog.sync(&mut worker.fs_storage).is_err() {
        log_disk_errno(
            worker,
            b"[kvw] import could not make itself durable, QUARANTINE errno=",
        );
        worker.disk_phase = DISK_PHASE_QUARANTINED;
        return;
    }
    let k = origin::IMPORTED_KEY;
    let mut body = [0u8; 2 + 32 + 4 + 12 + 1 + 8];
    let mut at = 0usize;
    let offset = worker.prov.import_at.to_le_bytes();
    let crc = worker.prov.import_crc.to_le_bytes();
    let parts: [&[u8]; 5] = [
        &(k.len() as u16).to_le_bytes(),
        k,
        &12u32.to_le_bytes(),
        &offset,
        &crc,
    ];
    for part in parts {
        for b in part {
            if let Some(d) = body.get_mut(at) {
                *d = *b;
            }
            at += 1;
        }
    }
    at += 9; // flags 0, expiry 0 (zeroed)
    let mut ans = [0u8; 64];
    let a = apply_to_store(
        worker,
        KV_OP_PUT,
        meta,
        consistency,
        0,
        body.get(..at).unwrap_or(&[]),
        &mut ans,
    );
    if a.result != KV_RESULT_OK {
        log_disk_errno(
            worker,
            b"[kvw] import could not record its end, QUARANTINE errno=",
        );
        worker.disk_phase = DISK_PHASE_QUARANTINED;
        return;
    }
    worker.prov.import_marked = true;
    import_logged(worker);
}

/// `[kvw] store.log imported n=`: the records this start imported.
#[cfg(feature = "store_provider")]
unsafe fn import_logged(worker: &WorkerState) {
    let mut buf = [0u8; 64];
    let mut n = write_prefix(&mut buf, 0, b"[kvw] store.log imported n=");
    n = write_dec(&mut buf, n, worker.prov.import_n);
    log_line(worker, &buf, n);
}

/// The selector this store provider answers to (its `volume`), or 0.
#[cfg(feature = "store_provider")]
#[no_mangle]
#[link_section = ".text.module_provider_selector"]
pub extern "C" fn module_provider_selector(state: *mut u8) -> u32 {
    if state.is_null() {
        return 0;
    }
    // SAFETY: the loader passes this module's own initialised state.
    unsafe { (*state.cast::<WorkerState>()).selector }
}

/// `[0x14, 0x13]`: objects and their namespace, from one store — or
/// nothing, when this worker does not provide (see above).
#[cfg(feature = "store_provider")]
#[no_mangle]
#[link_section = ".text.module_provides_contracts"]
pub extern "C" fn module_provides_contracts(state: *mut u8, out: *mut u16, cap: u32) -> u32 {
    if state.is_null() || out.is_null() || cap < 2 {
        return 0;
    }
    // SAFETY: the kernel passes this module's state after `module_new`,
    // and `cap` u16s at `out`.
    unsafe {
        let worker = &*state.cast::<WorkerState>();
        if !worker.prov.provides {
            return 0;
        }
        *out = store_provider::CONTRACT_OBJECT;
        *out.add(1) = store_provider::CONTRACT_NAMESPACE;
    }
    2
}

/// A store contract call, answered in the call. Until the disk store has
/// recovered every call is `EAGAIN` (ask again); a quarantined store
/// answers `ERROR`.
#[cfg(feature = "store_provider")]
#[no_mangle]
#[link_section = ".text.module_provider_dispatch"]
pub extern "C" fn module_provider_dispatch(
    state: *mut u8,
    handle: i32,
    op: u32,
    arg: *mut u8,
    arg_len: usize,
) -> i32 {
    if state.is_null() {
        return store_provider::EINVAL;
    }
    // SAFETY: the kernel passes this module's own state, initialised by
    // `module_new`; the provider and its buffers are this module's heap,
    // apart from the state.
    unsafe {
        let worker = &mut *state.cast::<WorkerState>();
        if worker.syscalls.is_null() || !worker.prov.provides {
            return store_provider::ENOSYS;
        }
        worker.prov.m_calls = worker.prov.m_calls.wrapping_add(1);
        if op == store_provider::NS_CAPS {
            return store_provider::NS_CAPS_SERVED;
        }
        if !provider_ready(worker) && store_provider::needs_store(op, arg, arg_len) {
            worker.prov.m_not_serving = worker.prov.m_not_serving.wrapping_add(1);
            return if worker.disk_phase == DISK_PHASE_QUARANTINED {
                store_provider::ERROR
            } else if matches!(op, store_provider::OBJ_PUT | store_provider::OBJ_DELETE) {
                // Not taken yet: asked again, it is taken once the store
                // serves (`EAGAIN` would say an etag was lost).
                store_provider::EINPROGRESS
            } else {
                store_provider::EAGAIN
            };
        }
        let pv = &mut *worker.prov.state;
        let mut bufs = provider_bufs(&worker.prov);
        let rc = store_provider::dispatch(
            pv,
            &mut WorkerBackend {
                worker: &mut *worker,
            },
            &mut bufs,
            handle,
            op,
            arg,
            arg_len,
        );
        // A call refused for a reason other than the contract's own answers
        // (absent, exists, moved, no such op) is the store failing a
        // consumer, which the consumer sees only as a failure of its own:
        // said here, with what was asked. A change read whose batch
        // outgrows the reader's buffer (`ENOMEM` on CHANGES) is the
        // contract's answer too: the reader grows its buffer and asks
        // again, so it is not said.
        let changes_grow = rc == store_provider::ENOMEM && op == store_provider::NS_CHANGES;
        if rc < 0
            && !changes_grow
            && !matches!(
                rc,
                store_provider::ENXIO
                    | store_provider::EEXIST
                    | store_provider::EAGAIN
                    | store_provider::EINPROGRESS
                    | store_provider::ENOSYS
            )
        {
            worker.prov.m_refused = worker.prov.m_refused.wrapping_add(1);
            let mut buf = [0u8; 80];
            let mut n = write_prefix(&mut buf, 0, b"[kvw] store call refused op=");
            n = write_dec(&mut buf, n, u64::from(op));
            n = write_prefix(&mut buf, n, b" rc=");
            n = write_signed(&mut buf, n, rc);
            n = write_prefix(&mut buf, n, b" arg_len=");
            n = write_dec(&mut buf, n, arg_len as u64);
            log_line(worker, &buf, n);
        }
        rc
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
    // A construction the loader is completing (the store twin making its
    // store ready): carry on from where the last call stopped.
    #[cfg(feature = "store_provider")]
    if worker.prov.pending {
        return unsafe { provider_make_ready(worker) };
    }
    worker.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(worker, params, params_len) };
    }
    let dir_len = worker.store_dir_len as usize;
    if !worker
        .fs_storage
        .init(sys_ptr, worker.root_path, worker.store_id)
        || (dir_len > 0
            && !worker
                .fs_storage
                .set_dir(&worker.store_dir[..dir_len.min(128)]))
    {
        // A store layout that cannot separate instances (FAT32 root +
        // non-zero store_id, or an out-of-range id) must refuse to
        // start rather than silently share file names with another
        // worker.
        return -1;
    }

    // EFFECTIVE-CONFIG REPORT. Unconditional and
    // mode-independent: it names what the module ACTUALLY parsed out of
    // its params TLV, not what the config intended. A worker that came
    // up in memory mode because `state_store` never landed is
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
        worker.sweep_out = dev_channel_port(sys, 1, 8);
        worker.prov_out = dev_channel_port(sys, 1, 9);
    }
    // The store twin logs whenever it can provide: a store answers a
    // write in the call, and the log is what makes that answer hold.
    #[cfg(feature = "store_provider")]
    if worker.state_store == STATE_STORE_DISK && worker.sweep_wrap == 0 {
        worker.command_log = 1;
    }
    if worker.command_log != 0 && !unsafe { command_log_new(worker) } {
        return -1;
    }
    #[cfg(feature = "store_provider")]
    unsafe {
        provider_new(worker);
        provider_make_ready(worker)
    }
    #[cfg(not(feature = "store_provider"))]
    0
}

/// Set the command log up at construction: `false` (the module refuses to
/// start) for a configuration it cannot serve.
unsafe fn command_log_new(worker: &mut WorkerState) -> bool {
    let sys = &*worker.syscalls;
    // One line per refusal (a `match` choosing among literals would be a
    // table of pointers a module image cannot hold).
    if worker.state_store != STATE_STORE_DISK {
        let m = b"[kvw] command_log needs state_store=disk";
        dev_log(sys, DISK_LOG_LEVEL, m.as_ptr(), m.len());
        return false;
    }
    if worker.sweep_wrap != 0 {
        let m = b"[kvw] command_log refused: consensus orders this worker (sweep_wrap=1)";
        dev_log(sys, DISK_LOG_LEVEL, m.as_ptr(), m.len());
        return false;
    }
    worker.clog_buf = (sys.heap_alloc)(command_log::PAYLOAD_MAX as u32);
    worker.clog_out = (sys.heap_alloc)(CLOG_OUT_MAX as u32);
    if worker.clog_buf.is_null() || worker.clog_out.is_null() {
        let m = b"[kvw] command_log: heap refused";
        dev_log(sys, DISK_LOG_LEVEL, m.as_ptr(), m.len());
        return false;
    }
    true
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
/// is taken over the CANONICAL key (identity prefix + user key),
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
            drive_command_log(worker);
            drain_retirements(worker);
        }
        drain_epoch_events(worker);
        drain_snapshot_ctl(worker);
        drain_gc_floor(worker);
        drain_expire(worker);
        drive_expiry_resume(worker);
        publish_retention_claim(worker);
        maybe_request_ts_lease(worker);
        propose_chunk_sweep(worker);
    }
    let drain_t0 = unsafe { now_us(worker) };
    let scans0 = worker.disk_state.scans_opened.get();
    let skips0 = worker.disk_state.scans_skipped.get();
    let blocks0 = worker.disk_state.blocks_read.get();
    let (reads0, rbytes0, opens0) = worker.fs_storage.read_counters();
    // A reply still waiting for room goes first; until it is written no
    // command is taken (the channel holds them).
    let mut budget = if unsafe { flush_reply(worker) } {
        PER_TICK_DRAIN_BUDGET
    } else {
        0
    };
    let mut drained = 0u32;
    let mut spilled = false;
    // A held (paused) command gets its slice FIRST, inside the drain
    // window so its cost counts against `DRAIN_BUDGET_US` like any
    // other command's.
    if budget > 0 && unsafe { drive_held_command(worker) } {
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
        if worker.reply_stash_len != 0 {
            break;
        }
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
    #[cfg(feature = "store_provider")]
    unsafe {
        provider_step(worker);
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
/// parsed out of its params TLV. Emitted from
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
///   store (fail closed — never serve over a store that failed
///   recovery validation). Success seeds the engine revision from the
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
                // Replay idempotence: seed the clock for a
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
            // (the GC-floor contract). `gc_committed_floor` is written by
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
                // Where the store's compaction stands: a write that needed
                // room may have driven it on, or to its end.
                let (floor, cursor) = match worker.disk_state.compaction_in_flight() {
                    Some(at) => at,
                    None if worker.disk_compact_cursor != 0 => {
                        // Finished for us: the merge is published.
                        worker.disk_compact_active = false;
                        return;
                    }
                    None => (worker.disk_compact_floor, 0),
                };
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
                // A single `store.flush()` here — the whole memtable
                // (up to MEMTABLE_FLUSH_WATERMARK = 384 records,
                // ~1.7 MB) plus a fresh run file plus the manifest
                // plus an fsync, inside ONE `module_step` — would be
                // the only unbounded action in this function, and on
                // FAT32-over-NVMe it blows the step deadline and gets
                // the module TERMINATED (see the manifest's
                // step-deadline note): the graph serves exactly 384
                // writes and dies. No deadline could cover it: the
                // config validator caps the burst at
                // 16 x tick_us, and the flush costs more than that.
                //
                // `flush_step` writes at most FLUSH_STEP_RECORDS per
                // call and carries its partial block across calls, so
                // the run is byte-identical to a single-call flush
                // (the golden vectors in
                // tests/contract_disk_store.rs pin it). Only the
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
                        // Left unrecorded — no counter, no log, no
                        // phase change — a capture or install that
                        // never completes leaves the worker retrying a
                        // permanently-refused flush in total silence.
                        // It is counted and degraded-marked, so the
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

/// Delete one queued retired run file now, whatever else is wanted: the
/// construction-time import, where no step runs to drain them.
#[cfg(feature = "store_provider")]
unsafe fn drain_retirements_now(worker: &mut WorkerState) {
    if worker.state_store != STATE_STORE_DISK
        || worker.disk_phase != DISK_PHASE_SERVING
        || worker.disk_state.retire_count == 0
    {
        return;
    }
    let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
    let _ = store.retire_step();
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
///   `/metrics`). The scrape survives a wedged request
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
        // Commit-timestamp assignment counters (ids 20, 21), the
        // app-snapshot refusal counter (id 22) and the chunk sweeps
        // (ids 23, 24).
        emit_counters_at(
            sys,
            worker.metrics_out,
            20,
            &[
                worker.m_ts_stamped,
                worker.m_ts_unstamped,
                worker.m_snapshot_refusals,
                worker.sweeps_proposed,
                worker.sweeps_applied,
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
    // `ss`/`rp` are the EFFECTIVE params — repeated on
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
    // The committed GC floor — 0 means no floor has ever
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
/// (the reclamation gate).** Everything else in the retention path —
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

/// Publish this worker's ACTIVE-READ retention claim.
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
///   `[kpg_id:u16 LE][epoch:u32 LE][reason:u8]`.
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
    if !(7..=64).contains(&payload_len) {
        return;
    }
    let mut buf = [0u8; 64];
    if ((sys.channel_read)(worker.epoch_events_in, buf.as_mut_ptr(), payload_len) as usize)
        < payload_len
    {
        return;
    }
    let new_epoch = u32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]);
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
/// Implements the app side of clustor's app-snapshot contract. All
/// three inbound opcodes share `snapshot_import`:
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
                    // The store provider's writes the snapshot skipped
                    // are decided in its origin record.
                    #[cfg(feature = "store_provider")]
                    provider_resync(worker);
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
