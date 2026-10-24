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
//! - `0` (memory, default): the bounded in-memory `KvStore` table; no
//!   FS use at all.
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

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Chan, Inbox, ENVELOPE_HDR};

#[cfg(feature = "store_provider")]
#[path = "../../common/kv_large.rs"]
mod kv_large;

#[cfg(feature = "store_provider")]
#[path = "../../common/doc_tree.rs"]
mod doc_tree;

#[cfg(feature = "store_provider")]
#[path = "../../common/store_provider.rs"]
mod store_provider;

#[cfg(feature = "store_provider")]
#[path = "../../common/change_ring.rs"]
mod change_ring;

#[cfg(feature = "store_provider")]
#[path = "../../common/store_registry.rs"]
mod store_registry;

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
use kv_store::{
    DiskMaterializer, EventJournal, KvStore, Materializer, MAX_KEYS, MAX_KEY_LEN,
    SNAPSHOT_FLAG_SPAN_MORE,
};
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
    MSG_APP_SNAPSHOT_EXPORT_CHUNK, MSG_APP_SNAPSHOT_EXPORT_REQUEST, MSG_APP_SNAPSHOT_INSTALLED,
    MSG_APP_SNAPSHOT_REQUEST, MSG_APP_SNAPSHOT_RESET, MSG_APP_SNAPSHOT_SPAN_REQUEST,
    MSG_GC_FLOOR_COMMITTED, MSG_KV_APPLIED, MSG_KV_COMMAND, MSG_KV_WORKING, MSG_LEASE_TICK,
    MSG_PLACEMENT_EPOCH_EVENT, MSG_RETENTION_CLAIM, MSG_TS_LEASE_GRANT, MSG_TS_LEASE_REQUEST,
    MSG_TTL_CLOCK_RESUME, MSG_TTL_REGISTER, MSG_WATCH_EVENT, TS_LEASE_GRANT_WIRE_LEN,
    XFER_CHUNK_BODY,
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
    // 0 = bounded in-memory table (default, no FS use at all); 1 = the
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

    // TEST ONLY — never in a shipped graph. kv_state_worker_store's store
    // provider makes one store step in N busy: from that worker step to its
    // next, every read it is asked (GET, HEAD, RANGE_GET, LIST, CHANGES)
    // answers EAGAIN with nothing moved — the board's store while its
    // device is taken by a write, reproduced on Linux so an E2E proves its
    // consumers hold a read that waits rather than read it as absent
    // (`store_provider::Fault`). 0 (default): off.
    // 1: refused (every step busy), and off. Ignored by kv_state_worker (no
    // provider). Logged at construction as `[kvw] FAULT INJECTION ON …` and
    // counted on `[kvw] store hb … fault=`.
    20, fault_read_eagain_every, u32, 0
        => |s, d, len| { s.fault_read_eagain_every = p_u32(d, len, 0, 0); };
    // TEST ONLY, with `fault_read_eagain_every`: a busy step reaches the
    // store itself — every store op a read applies answers as the worker's
    // store does while its device makes the op wait (`KV_RESULT_INTERNAL`,
    // busy) — instead of ending at dispatch, so the provider's own read
    // paths must answer EAGAIN. 0 (default): at dispatch.
    21, fault_busy_reaches_store, u8, 0
        => |s, d, len| { s.fault_busy_reaches_store = p_u8(d, len, 0, 0); };
    // TEST ONLY: every single-store write the provider applies keeps the
    // log pending this many further worker steps, so another caller's
    // write lands between a write and its re-ask (the board's device
    // holding the record). 0 (default): off.
    22, fault_log_hold_steps, u32, 0
        => |s, d, len| { s.fault_log_hold_steps = p_u32(d, len, 0, 0); };
    // TEST ONLY: a slow device. One write command in
    // `fault_slow_write_every` (default 1: every one) waits this many ms at
    // the head of the command channel before it is applied, as a loaded
    // board's store keeps a write waiting — the router must keep it in
    // flight, never answer it fail-closed for taking long. Applies to every
    // worker type. 0 (default): off.
    23, fault_slow_write_ms, u32, 0
        => |s, d, len| { s.fault_slow_write_ms = p_u32(d, len, 0, 0); };
    24, fault_slow_write_every, u32, 0
        => |s, d, len| { s.fault_slow_write_every = p_u32(d, len, 0, 0); };

    // kv_state_worker_store's change log: the bytes of the key-only ring
    // its subscriptions are rung from and CHANGES is served from. A read
    // from before what it holds walks the store's history. 0 (default):
    // `store_provider::CHANGE_LOG_DEFAULT`. A larger figure needs the
    // instance's heap raised to match (`heap.arena_bytes`).
    25, store_change_log, u32, 0
        => |s, d, len| { s.store_change_log = p_u32(d, len, 0, 0); };
    // kv_state_worker_store: the subscriptions one workload owner may hold
    // (the system owner, which holds the node's own controllers, is not
    // held to it). 0 (default): `store_provider::DEFAULT_SUB_QUOTA`.
    26, store_sub_quota, u32, 0
        => |s, d, len| { s.store_sub_quota = p_u32(d, len, 0, 0); };
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
    /// Install-complete ack (index 7); -1 unless wired.
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
    /// Disk provider: a snapshot capture is owed once the flush now
    /// wanted publishes. A capture request that found the manifest
    /// behind the applied position asked for that flush rather than
    /// answering with a stale (or no) position — see
    /// `drain_snapshot_ctl`.
    disk_capture_after_flush: bool,
    /// The FS errno at the last refusal `log_disk_errno` reported
    /// (telemetry `qe=`).
    disk_quarantine_errno: i32,
    /// The capture path as counts (`cap=` on the disk heartbeat):
    /// requests seen, flushes asked for, markers emitted.
    m_cap_req: u32,
    m_cap_ask: u32,
    m_cap_emit: u32,
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
    /// When the router was last told a waiting command is still being
    /// worked on (`MSG_KV_WORKING`, see [`say_working`]).
    working_said_ms: u64,
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
    /// Origin entries the log committed that this store skipped as stale
    /// or out of order (telemetry; `Duplicate` re-offers are not counted).
    m_origin_skipped: u64,
    /// Times the command at the head of the channel waited for room
    /// (telemetry: a stalled apply shows here first).
    m_head_blocked: u64,
    /// The last command message type and KV op drained (telemetry: the
    /// `[kvw] drain` line names what a slow drain was doing).
    last_msg: u8,
    last_op: u8,

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
    /// A PORTABLE install (another replica's export) streams chunk by
    /// chunk into the disk store's REPLACE install rather than
    /// accumulating in `import_buf`, which then holds only a chunk the
    /// store has not taken yet (`import_pend_len` bytes at
    /// `import_pend_off`, the last when `import_pend_done`).
    import_stream: bool,
    /// Each input's envelope reading (`common/envelope.rs`).
    commands_inbox: Inbox,
    expire_inbox: Inbox,
    durability_inbox: Inbox,
    epoch_events_inbox: Inbox,
    snapshot_import_inbox: Inbox,
    /// An install at or below the applied position (nonzero: its index):
    /// the store already holds that state and the entries after it, so
    /// the stream is read and dropped, and acknowledged at its end.
    import_covered: u64,
    import_pend_len: usize,
    import_pend_off: u64,
    import_pend_done: bool,
    /// The store revision the last applied-position marker left: where a
    /// log entry's writes end. An export is read as of it, and begins only
    /// while nothing past that marker is applied.
    rev_at_applied: u64,
    /// A portable export for another replica (`MSG_APP_SNAPSHOT_EXPORT_
    /// REQUEST`), streamed a step at a time: `xfer_buf[xfer_pos..
    /// xfer_len]` is what the store produced and the channel has not taken
    /// yet; `xfer_off` the stream offset of `xfer_pos`.
    xfer_active: bool,
    xfer_begun: bool,
    xfer_done: bool,
    xfer_id: u32,
    xfer_term: u64,
    xfer_index: u64,
    xfer_off: u64,
    xfer_len: usize,
    xfer_pos: usize,
    xfer_since_ms: u64,
    /// A span copy for an elastic split (`MSG_APP_SNAPSHOT_SPAN_REQUEST`),
    /// streamed a batch at a time: a RESET, then one span body per batch
    /// until the one without `SNAPSHOT_FLAG_SPAN_MORE`.
    /// `span_buf[..span_len]` is the batch in flight (`span_len == 0`:
    /// none yet), `span_sent` bytes of it taken by the channel; the next
    /// batch starts at `span_cursor` and the span ends before `span_end`.
    span_active: bool,
    span_reset_sent: bool,
    span_more: bool,
    span_len: usize,
    span_sent: usize,
    span_since_ms: u64,
    span_cursor_len: usize,
    span_end_len: usize,
    /// The span install under way has taken a batch with more to follow:
    /// the next batch's body is merged into the store, not a replacement.
    import_span_merge: bool,
    m_xfer_exports: u64,
    m_xfer_installs: u64,

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
    xfer_buf: [u8; SNAPSHOT_BODY_MAX],
    span_buf: [u8; SNAPSHOT_BODY_MAX],
    span_cursor: [u8; MAX_KEY_LEN],
    span_end: [u8; SPAN_BOUND_MAX],

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
    /// TEST ONLY: the store provider's injected read waits (param 20),
    /// where they meet a read (21), and its held log records (22).
    fault_read_eagain_every: u32,
    fault_busy_reaches_store: u8,
    fault_log_hold_steps: u32,
    /// TEST ONLY: the slow device (params 23, 24), and the write at the
    /// channel head it is holding: its correlation, the writes counted, and
    /// until when it waits.
    fault_slow_write_ms: u32,
    fault_slow_write_every: u32,
    /// `store_change_log` param (bytes; 0: the default).
    store_change_log: u32,
    /// `store_sub_quota` param (0: the default).
    store_sub_quota: u32,
    /// The keys the disk store's commands in flight changed, for the store
    /// provider's change log (the memory store keeps its own,
    /// `store.keylog`); and where the open command scope began in it, so an
    /// undone command takes its keys back with it.
    keylog: kv_store::KeyLog,
    scope_keylog_len: usize,
    scope_keylog_overflow: bool,
    slow_seen: bool,
    slow_corr: u64,
    slow_count: u32,
    slow_until_ms: u64,

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
    /// The command scope ([`apply_to_store`]): the outermost apply of a
    /// command (or an origin entry's several) marks where the memtable
    /// and these worker fields stood, so a command its device made wait is
    /// undone whole and applied again on a later step.
    scope_depth: u8,
    scope_waited: bool,
    scope_mark: kv_store::disk_store::ApplyMark,
    scope_revision: u64,
    scope_catalog: u64,
    scope_lease_next: u64,
    scope_stamped: u64,
    scope_unstamped: u64,
    /// A channel command applied whose command-log record is pending: its
    /// reply waits in `reply_stash` and its frame at the channel's head
    /// until the log settles ([`finish_logged_command`]); then its
    /// mutations are published (`clog_hold_publish`), the reply sent and
    /// the frame taken.
    clog_hold: bool,
    clog_hold_publish: bool,
    clog_hold_op: u8,
    clog_hold_result: u8,
    clog_hold_kpg: u16,
    clog_hold_revision: u64,
    clog_hold_answer_at: usize,
    clog_hold_answer_len: usize,
    /// Commands undone and asked again because the device made them wait,
    /// and maintenance actions that waited on the device (each asked again
    /// where it stood).
    m_cmd_waits: u64,
    disk_io_waits: u64,
    /// The import's end is applied and waits for the command log to hold
    /// its record durably before the import is recorded done.
    import_end_pending: bool,
    /// An origin entry's watch events, held until the entry can no longer
    /// be undone ([`apply_origin_entry`]): the journal its TXN left, the
    /// TXN, its result and answer. Published at the entry's end, or
    /// dropped with the entry if a device wait undoes it — a watcher never
    /// hears of a write that was taken back.
    ev_held: bool,
    ev_journal: kv_store::EventJournal,
    ev_txn: [u8; SCRATCH_BUF_SIZE],
    ev_txn_len: usize,
    ev_answer: [u8; ORIGIN_ANSWER_ROOM],
    ev_answer_len: usize,
    ev_result: u8,
    ev_kpg: u16,
    ev_revision: u64,
    /// A second write's events in one entry (origin.rs writes once per
    /// entry, so never): counted, and published with the entry's end.
    m_ev_second: u64,

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
        self.import_stream = false;
        self.import_covered = 0;
        self.commands_inbox = Inbox::new();
        self.expire_inbox = Inbox::new();
        self.durability_inbox = Inbox::new();
        self.epoch_events_inbox = Inbox::new();
        self.snapshot_import_inbox = Inbox::new();
        self.import_pend_len = 0;
        self.import_pend_off = 0;
        self.import_pend_done = false;
        self.rev_at_applied = 0;
        self.xfer_active = false;
        self.xfer_begun = false;
        self.xfer_done = false;
        self.xfer_len = 0;
        self.xfer_pos = 0;
        self.span_active = false;
        self.span_reset_sent = false;
        self.span_more = false;
        self.span_len = 0;
        self.span_sent = 0;
        self.span_cursor_len = 0;
        self.span_end_len = 0;
        self.import_span_merge = false;
        self.m_xfer_exports = 0;
        self.m_xfer_installs = 0;
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
        self.m_head_blocked = 0;
        self.m_origin_skipped = 0;
        self.last_msg = 0;
        self.last_op = 0;
        self.published_applied_index = 0;
        self.catalog_generation = 0;
        self.importing = false;
        self.import_len = 0;
        self.m_applied = 0;
        self.events.clear();
        self.keylog.clear();
        self.keylog.on = false;
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
        self.disk_capture_after_flush = false;
        self.disk_quarantine_errno = 0;
        self.m_cap_req = 0;
        self.m_cap_ask = 0;
        self.m_cap_emit = 0;
        self.disk_flush_chunks = 0;
        self.disk_compact_steps = 0;
        self.disk_flush_t0 = 0;
        self.held_active = false;
        self.working_said_ms = 0;
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
                // The device deferred the listing or a header read: asked
                // again next step (the listing continues where it stood).
                Err(command_log::LogError::Storage(
                    kv_store::disk_store::run_storage::StorageError::Again,
                )) => {
                    worker.disk_io_waits = worker.disk_io_waits.wrapping_add(1);
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
                // Where the replay stands before this record: a record the
                // device makes wait — its read, or its op's — is read and
                // applied again from here next step.
                let at = worker.clog_replay;
                let next = worker
                    .clog_replay
                    .next(&worker.clog, &worker.fs_storage, buf);
                if matches!(
                    next,
                    Err(command_log::LogError::Storage(
                        kv_store::disk_store::run_storage::StorageError::Again
                    ))
                ) {
                    worker.clog_replay = at;
                    worker.disk_io_waits = worker.disk_io_waits.wrapping_add(1);
                    return;
                }
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
                if a.waited {
                    // Undone whole: the same record next step.
                    worker.clog_replay = at;
                    return;
                }
                if a.paused || worker.disk_phase != DISK_PHASE_SERVING {
                    // A logged op never pauses (only reads do); a fault
                    // has quarantined the store.
                    return;
                }
                worker.clog_replayed = worker.clog_replayed.wrapping_add(1);
            }
        }
        _ => {
            // A record the log took and the device has not finished with
            // is asked on every step, so an answer waiting on it (a held
            // reply, a store write answered EINPROGRESS) is released as
            // soon as the device allows, whether or not anyone asks.
            if !settle_log(worker) {
                return;
            }
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
    // Nothing is read until the whole envelope is on the channel
    // (`common/envelope.rs`); a write then waits there, unread, until the
    // store has room for it: the channel stays FIFO, so nothing behind it
    // (a later write, a clock tick, a position marker) passes it.
    let mut frame = [0u8; ENVELOPE_HDR + SCRATCH_BUF_SIZE];
    let handle = worker.commands_in;
    let Some((_, len)) = worker
        .commands_inbox
        .peek(&mut Chan { sys, handle }, &mut frame)
    else {
        return false;
    };
    let total = ENVELOPE_HDR + len;
    if worker.held_active && frame_is_pausable(&frame[..total]) {
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
        return adopt_retry(worker, &frame[ENVELOPE_HDR..total]);
    }
    if slow_write_waits(worker, &frame[..total]) || !frame_admitted(worker, &frame[..total]) {
        worker.m_head_blocked = worker.m_head_blocked.wrapping_add(1);
        return false;
    }
    if frame[0] == MSG_KV_COMMAND && len >= wire::KvCommandHead::LEN {
        // A command is applied from the peeked copy and taken off the
        // channel only once it has been: one the device made wait (undone
        // whole) stays at the head and is applied again next step, and so
        // does one whose reply waits for the command log — nothing behind
        // either passes it.
        let outcome = apply_command_frame(worker, &frame[ENVELOPE_HDR..total]);
        match outcome {
            CmdOutcome::Waited | CmdOutcome::Logged => return false,
            CmdOutcome::Paused => hold_command(worker, &frame[ENVELOPE_HDR..total]),
            CmdOutcome::Answered(_) | CmdOutcome::Refused => {}
        }
        if !worker
            .commands_inbox
            .take(&mut Chan { sys, handle }, len, &mut frame)
        {
            return false;
        }
        return match outcome {
            CmdOutcome::Answered(ok) => ok,
            CmdOutcome::Paused => true,
            _ => false,
        };
    }
    if !worker
        .commands_inbox
        .take(&mut Chan { sys, handle }, len, &mut frame)
    {
        return false;
    }
    let kind = frame[0];
    let payload = &frame[ENVELOPE_HDR..total];
    worker.last_msg = kind;
    if kind == MSG_APP_APPLIED_POS {
        // In-band applied-position marker from lattice_apply_bridge.
        // Arriving here means every command ahead of it on this
        // channel has already been applied to `store`.
        if payload.len() != 16 {
            return true;
        }
        let term = le_u64(&payload[..8]);
        let index = le_u64(&payload[8..16]);
        // Monotonic — a stale marker after a restore must not rewind us.
        if index > worker.applied_index {
            worker.applied_term = term;
            worker.applied_index = index;
            worker.rev_at_applied = if worker.state_store == STATE_STORE_DISK {
                worker.disk_revision
            } else {
                worker.store.revision
            };
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
    if kind == MSG_TS_LEASE_GRANT {
        // In-band replicated worker lease, forwarded by
        // lattice_apply_bridge in committed-log order:
        // `[corr_id:u64 = 0][TimestampLease:40]`. Arriving HERE, on the
        // commands channel, is the contract — the lease takes effect
        // exactly between the commands it was committed between, so
        // per-write timestamp assignment below is a pure function of
        // the log on every replica and every replay.
        if payload.len() != TS_LEASE_GRANT_WIRE_LEN {
            return true;
        }
        // TimestampLease wire (mvcc.rs): version:u16 @0, payload_len:u16
        // @2, interval_start:u64 @12, interval_end:u64 @20 — offsets
        // within the 40-byte lease, which begins at byte 8 of the frame.
        let lease = &payload[8..];
        let version = u16::from_le_bytes([lease[0], lease[1]]);
        let start = le_u64(&lease[12..20]);
        let end = le_u64(&lease[20..28]);
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
    if kind == MSG_LEASE_TICK {
        // The authoritative clock, arriving on the SAME channel as the
        // commands it orders against: `[tick_ms:u64]`, proposed by
        // `ttl_scheduler` and delivered here only after it committed.
        // Reading it anywhere else — a local timer, a side channel —
        // would give each replica its own notion of now, which is the
        // one thing a replicated TTL cannot have.
        if payload.len() != 8 {
            return true;
        }
        let tick_ms = le_u64(payload);
        if tick_ms > worker.committed_now_ms {
            worker.committed_now_ms = tick_ms;
        }
        // The disk provider persists the frontier with its manifest, so
        // a store recovered from runs alone resumes at the clock its
        // records' deadlines were written against.
        worker.disk_state.committed_clock_ms = worker.committed_now_ms;
        return true;
    }
    // MSG_KV_COMMAND head layout owned by wire::KvCommandHead — kpg +
    // the canonical identity the store is scoped to.
    // Anything else — a command frame too short for its head among it —
    // is taken and passed over, as it always was. (A whole command is
    // applied above, before it is taken.)
    let _ = payload;
    true
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
    if !finish_logged_command(worker) {
        return false;
    }
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

/// Finish a channel command whose reply waits for the command log
/// ([`CmdOutcome::Logged`]): once the log holds its record where it was
/// asked to (written, and durable), its mutations are published and its
/// frame taken off the channel; the stashed reply then goes as any stashed
/// reply does. `false` while it waits. A log that cannot take the record
/// stops the store, and the reply is never sent.
unsafe fn finish_logged_command(worker: &mut WorkerState) -> bool {
    if !worker.clog_hold {
        return true;
    }
    if !settle_log(worker) {
        if worker.disk_phase == DISK_PHASE_QUARANTINED {
            worker.reply_stash_len = 0;
            worker.clog_hold = false;
        }
        return false;
    }
    let sys_ptr = worker.syscalls;
    if sys_ptr.is_null() || worker.commands_in < 0 {
        return false;
    }
    let sys = &*sys_ptr;
    let handle = worker.commands_in;
    let mut frame = [0u8; ENVELOPE_HDR + SCRATCH_BUF_SIZE];
    let Some((_, len)) = worker
        .commands_inbox
        .peek(&mut Chan { sys, handle }, &mut frame)
    else {
        return false;
    };
    let total = ENVELOPE_HDR + len;
    if worker.clog_hold_publish {
        worker.clog_hold_publish = false;
        let payload = frame.get(ENVELOPE_HDR..total).unwrap_or(&[]);
        if let Some(head) = wire::KvCommandHead::decode(payload) {
            let body_at = wire::KvCommandHead::LEN;
            let body = payload
                .get(body_at..body_at + head.body_len as usize)
                .unwrap_or(&[]);
            let mut answer = [0u8; SCRATCH_BUF_SIZE];
            let at = worker.clog_hold_answer_at;
            let n = worker.clog_hold_answer_len.min(SCRATCH_BUF_SIZE);
            for (d, b) in answer
                .iter_mut()
                .zip(worker.reply_stash.iter().skip(at))
                .take(n)
            {
                *d = *b;
            }
            publish_mutations(
                worker,
                worker.clog_hold_op,
                worker.clog_hold_kpg,
                worker.clog_hold_result,
                worker.clog_hold_revision,
                body,
                answer.get(..n).unwrap_or(&[]),
            );
        }
    }
    if !worker
        .commands_inbox
        .take(&mut Chan { sys, handle }, len, &mut frame)
    {
        return false;
    }
    worker.clog_hold = false;
    true
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
    /// The device made the command wait and it was undone whole: nothing
    /// was answered. The frame stays where it is (at the channel's head)
    /// and is applied again on a later step; nothing behind it passes it.
    Waited,
    /// Applied, and its reply stashed until the command log has its
    /// record where it was asked to be ([`finish_logged_command`]). The
    /// frame stays at the channel's head meanwhile.
    Logged,
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
    worker.last_op = op;
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
        if !apply_origin_entry(worker, &head, body) {
            return CmdOutcome::Waited;
        }
        return CmdOutcome::Answered(true);
    }

    // The store answers into the room both hops of the answer have for a
    // body: `MSG_KV_APPLIED` out of this scratch, and the router's
    // `MSG_KV_RESPONSE` re-heading of it, whose head and fence tail are
    // longer. An answer past either could not be sent on, and the command
    // would go unanswered.
    const APPLIED_BODY_MAX: usize = SCRATCH_BUF_SIZE - 3 - wire::KvAppliedHead::LEN - 8;
    const RESULT_BODY_MAX: usize = if APPLIED_BODY_MAX < wire::KV_RESPONSE_BODY_MAX {
        APPLIED_BODY_MAX
    } else {
        wire::KV_RESPONSE_BODY_MAX
    };
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
            waited: false,
            logging: false,
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
    if applied.waited {
        return CmdOutcome::Waited;
    }
    let (result, result_body_len, result_revision) =
        (applied.result, applied.len, applied.revision);
    // A sweep is store maintenance this worker (or a replica's) proposed:
    // nobody awaits its answer, which says only what is left to do. One
    // refused says nothing of the queues, so they are taken to hold work
    // and swept again on the waiting cadence: a store full of what only
    // the sweep reclaims may see no write land to arm it.
    if op == types::KV_OP_CHUNK_SWEEP {
        worker.sweeps_applied = worker.sweeps_applied.wrapping_add(1);
        worker.sweep_inflight = false;
        worker.sweep_flags = if result == KV_RESULT_OK {
            result_body[0]
        } else {
            kv_store::SWEEP_WAITING
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
    if applied.logging {
        // The op stands but its log record is not yet where it must be
        // before anyone is answered: the reply waits in the stash, its
        // mutations wait to be published, and the frame waits at the
        // channel's head, until the log settles.
        if !stash_reply(worker, total) {
            return CmdOutcome::Refused;
        }
        worker.clog_hold = true;
        worker.clog_hold_publish = id_database == 0 && id_keyspace == 0;
        worker.clog_hold_op = op;
        worker.clog_hold_kpg = kpg_id;
        worker.clog_hold_result = result;
        worker.clog_hold_revision = result_revision;
        worker.clog_hold_answer_at = 3 + wire::KvAppliedHead::LEN;
        worker.clog_hold_answer_len = result_body_len;
        return CmdOutcome::Logged;
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
        let w = &mut *self.worker;
        if w.scope_depth > 0 && w.state_store == STATE_STORE_DISK {
            // Inside the entry's undo scope: held, published only once
            // the entry stands (`apply_origin_entry`).
            if w.ev_held {
                w.m_ev_second = w.m_ev_second.wrapping_add(1);
                return;
            }
            w.ev_held = true;
            w.ev_journal.buf = w.events.buf;
            w.ev_journal.len = w.events.len;
            w.ev_journal.overflow = w.events.overflow;
            w.ev_txn_len = txn.len().min(SCRATCH_BUF_SIZE);
            for (d, b) in w.ev_txn.iter_mut().zip(txn.iter()) {
                *d = *b;
            }
            w.ev_answer_len = answer.len().min(ORIGIN_ANSWER_ROOM);
            for (d, b) in w.ev_answer.iter_mut().zip(answer.iter()) {
                *d = *b;
            }
            w.ev_result = result;
            w.ev_kpg = self.kpg_id;
            w.ev_revision = revision;
            return;
        }
        // SAFETY: the worker's own state and channels.
        unsafe {
            publish_mutations(w, KV_OP_TXN, self.kpg_id, result, revision, txn, answer);
        }
    }
}

/// Apply an origin entry (`common/origin.rs`) and tell this worker's store
/// provider what it came to. A store failing one is said: the replica can
/// no longer follow the log faithfully.
unsafe fn apply_origin_entry(
    worker: &mut WorkerState,
    head: &wire::KvCommandHead,
    body: &[u8],
) -> bool {
    let mut answer = [0u8; ORIGIN_ANSWER_ROOM];
    // The entry's several applies are one command: a wait in any of them
    // undoes the entry whole, and the same entry is applied again later.
    let outer = worker.state_store == STATE_STORE_DISK && worker.scope_depth == 0;
    if outer {
        scope_begin(worker);
        worker.ev_held = false;
    }
    worker.scope_depth = worker.scope_depth.saturating_add(1);
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
    worker.scope_depth = worker.scope_depth.saturating_sub(1);
    if outer && worker.scope_waited {
        let undone = scope_end(worker, Applied::waited(worker.disk_revision));
        if undone.waited {
            // Taken back whole: so are the events it would have published.
            worker.ev_held = false;
            return false;
        }
    }
    if outer {
        // The entry stands: nothing can take it back now, so its events
        // go out.
        publish_held_events(worker);
    }
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
    // A write the log committed that this store did not apply (stale or
    // out of order for its origin) is invisible otherwise: named, up to a
    // bound, so a replica that diverges says where.
    if let origin::Applied::Skipped {
        replica,
        inc,
        seq,
        verdict,
    } = a
    {
        if !matches!(verdict, origin::Verdict::Duplicate) {
            worker.m_origin_skipped = worker.m_origin_skipped.wrapping_add(1);
            if worker.m_origin_skipped <= ORIGIN_SKIP_LOG_MAX as u64 {
                let mut buf = [0u8; 112];
                let mut n = write_prefix(&mut buf, 0, b"[kvw] origin entry skipped ");
                n = write_prefix(
                    &mut buf,
                    n,
                    if matches!(verdict, origin::Verdict::Gap) {
                        b"gap"
                    } else {
                        b"stale"
                    },
                );
                n = write_prefix(&mut buf, n, b" replica=");
                n = write_dec(&mut buf, n, u64::from(replica));
                n = write_prefix(&mut buf, n, b" inc=");
                n = write_dec(&mut buf, n, u64::from(inc));
                n = write_prefix(&mut buf, n, b" seq=");
                n = write_dec(&mut buf, n, u64::from(seq));
                log_line(worker, &buf, n);
            }
        }
    }
    #[cfg(feature = "store_provider")]
    if worker.prov.provides {
        store_provider::origin_applied(&mut *worker.prov.state, &a);
    }
    true
}

/// Publish an origin entry's held watch events (see `WorkerState::ev_held`).
unsafe fn publish_held_events(worker: &mut WorkerState) {
    if !worker.ev_held {
        return;
    }
    worker.ev_held = false;
    worker.events.buf = worker.ev_journal.buf;
    worker.events.len = worker.ev_journal.len;
    worker.events.overflow = worker.ev_journal.overflow;
    let txn = worker.ev_txn;
    let answer = worker.ev_answer;
    let (tl, al) = (worker.ev_txn_len, worker.ev_answer_len);
    publish_mutations(
        worker,
        KV_OP_TXN,
        worker.ev_kpg,
        worker.ev_result,
        worker.ev_revision,
        txn.get(..tl).unwrap_or(&[]),
        answer.get(..al).unwrap_or(&[]),
    );
}

/// The answer room an origin entry's apply has.
const ORIGIN_ANSWER_ROOM: usize = 512;

/// Skipped origin entries logged by name; the rest are counted.
const ORIGIN_SKIP_LOG_MAX: usize = 32;

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
    /// The device made the op wait (a storage read or write answered
    /// `Again`): it was undone whole — nothing of it stands — and is to be
    /// applied again, unchanged, on a later step. Nothing is answered.
    waited: bool,
    /// Applied, and its command-log record is the log's but not yet where
    /// it was asked to be (written, and durable): the answer waits for
    /// `CommandLog::settle`.
    logging: bool,
}

impl Applied {
    /// Undone for a device wait (see `waited`).
    fn waited(revision: u64) -> Self {
        Applied {
            result: types::KV_RESULT_INTERNAL,
            len: 0,
            revision,
            paused: false,
            busy: true,
            changed: false,
            waited: true,
            logging: false,
        }
    }
}

/// Apply one op to the store under `identity`: the one place a command,
/// and every call the store provider makes, reaches the store. Stamps
/// the commit timestamp, keeps the catalog generation, counts, and
/// accounts for disk faults, exactly as it always has for commands.
///
/// No storage call waits on the device, so an op's read (or the write
/// that makes it room) may be answered "not now". Such an op is undone
/// WHOLE — the records it added, the revision, the timestamp it took,
/// the catalog generation — and answered `waited`: nothing of it stands,
/// nothing is answered, and the caller applies the same op again, from
/// the start, on a later step, by when the device has fetched what the
/// op was waiting for (the block cache keeps every block already read).
/// The outermost apply of a command is the scope the undo covers; an
/// origin entry's several applies share one ([`scope_begin`]).
///
/// A mutating op is not applied while the command log holds a record
/// the device has not finished with: it waits (`waited`) for the log.
unsafe fn apply_to_store(
    worker: &mut WorkerState,
    op: u8,
    identity: (u32, u32, u32),
    consistency: u8,
    head_commit_ts: u64,
    body: &[u8],
    out: &mut [u8],
) -> Applied {
    if worker.state_store != STATE_STORE_DISK {
        return apply_to_store_once(worker, op, identity, consistency, head_commit_ts, body, out);
    }
    if worker.clog_phase == CLOG_LIVE && !op_only_reads(op) && !settle_log(worker) {
        return Applied::waited(store_revision(worker));
    }
    let outer = worker.scope_depth == 0;
    if outer {
        scope_begin(worker);
    }
    worker.scope_depth = worker.scope_depth.saturating_add(1);
    let a = if worker.scope_waited {
        // An earlier apply of this command waited: the command is undone
        // at its end, and this part of it is not applied at all.
        Applied::waited(store_revision(worker))
    } else {
        apply_to_store_once(worker, op, identity, consistency, head_commit_ts, body, out)
    };
    worker.scope_depth = worker.scope_depth.saturating_sub(1);
    if outer {
        let a = scope_end(worker, a);
        #[cfg(feature = "store_provider")]
        provider_take(worker);
        return a;
    }
    a
}

/// A command stood (or was undone): its keys go into the store provider's
/// change log now, so a step applying many commands never stages more than
/// one command's keys. Not inside a provider call — the provider takes its
/// own writes' keys when it pumps, and its state is the call's then.
#[cfg(feature = "store_provider")]
unsafe fn provider_take(worker: &mut WorkerState) {
    if !worker.prov.provides || worker.prov.in_call || !worker.prov.open {
        return;
    }
    let pv = &mut *worker.prov.state;
    store_provider::take(pv, &mut WorkerBackend { worker });
}

/// Open a command scope: mark where the store and the worker fields an
/// undo restores stand.
unsafe fn scope_begin(worker: &mut WorkerState) {
    worker.scope_waited = false;
    worker.scope_keylog_len = worker.keylog.len;
    worker.scope_keylog_overflow = worker.keylog.overflow;
    worker.scope_mark = worker.disk_state.apply_mark();
    worker.scope_revision = worker.disk_revision;
    worker.scope_catalog = worker.catalog_generation;
    worker.scope_lease_next = worker.ts_lease_next;
    worker.scope_stamped = worker.m_ts_stamped;
    worker.scope_unstamped = worker.m_ts_unstamped;
}

/// Close a command scope: a command the device made wait is undone to
/// the mark and answered `waited`. One that cannot be undone (a record
/// overwritten in place, or a flushed prefix released, since the mark) is
/// not half kept and answered: the store fails closed, as an op the log
/// could not take does.
unsafe fn scope_end(worker: &mut WorkerState, a: Applied) -> Applied {
    if !worker.scope_waited {
        return a;
    }
    worker.scope_waited = false;
    worker.m_cmd_waits = worker.m_cmd_waits.wrapping_add(1);
    let mark = worker.scope_mark;
    if worker.disk_state.undo_to(&mark) {
        worker.disk_revision = worker.scope_revision;
        worker.catalog_generation = worker.scope_catalog;
        worker.ts_lease_next = worker.scope_lease_next;
        worker.m_ts_stamped = worker.scope_stamped;
        worker.m_ts_unstamped = worker.scope_unstamped;
        worker.events.clear();
        // Its keys go with it: the command never happened.
        worker.keylog.len = worker.scope_keylog_len;
        worker.keylog.overflow = worker.scope_keylog_overflow;
        return Applied::waited(worker.disk_revision);
    }
    log_disk_errno(
        worker,
        b"[kvw] a command the device made wait could not be undone, QUARANTINE errno=",
    );
    worker.disk_phase = DISK_PHASE_QUARANTINED;
    Applied {
        result: types::KV_RESULT_INTERNAL,
        len: 0,
        revision: worker.disk_revision,
        paused: false,
        busy: false,
        changed: false,
        waited: false,
        logging: false,
    }
}

/// Ask the command log's pending record on: `true` once nothing is
/// pending. A log that cannot take the record (a storage fault) stops the
/// store — the op it records is applied and must never be answered.
unsafe fn settle_log(worker: &mut WorkerState) -> bool {
    // A stopped store's log took a record it could not keep: nothing it
    // holds is ever answered as kept (the failed record is gone from the
    // log, so "nothing pending" would be a lie).
    if worker.disk_phase == DISK_PHASE_QUARANTINED {
        return false;
    }
    if !worker.clog.pending() {
        return true;
    }
    match worker.clog.settle(&mut worker.fs_storage) {
        Ok(()) => true,
        Err(command_log::LogError::Pending) => false,
        Err(_) => {
            log_disk_errno(
                worker,
                b"[kvw] command log append failed, QUARANTINE errno=",
            );
            worker.disk_phase = DISK_PHASE_QUARANTINED;
            false
        }
    }
}

/// [`apply_to_store`] for one op, inside its scope.
unsafe fn apply_to_store_once(
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
    // Read policy. `MSG_KV_COMMAND` carries the `Consistency` byte but
    // no `read_timestamp` field, so a request asks this worker for a
    // historical revision only through the native `KV_OP_GET_AT` /
    // `KV_OP_SCAN_AT` ops, which carry it in the op body. The policy is
    // threaded through all the same, so a read timestamp in the envelope
    // would be one line here rather than a re-plumb of the engine.
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
    let mut logging = false;
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
        let agains0 = worker.fs_storage.agains();
        let store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        let mut mat = DiskMaterializer::new(store, worker.disk_revision, worker.disk_replay_floor);
        mat.flush_wanted = worker.disk_flush_wanted;
        mat.set_key_identity(id_tenant, id_database, id_keyspace);
        mat.set_commit_ts(commit_ts);
        worker.events.clear();
        if worker.mutations_out >= 0 {
            mat.events = Some(&mut worker.events);
        }
        if worker.keylog.on {
            mat.keylog = Some(&mut worker.keylog);
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
                waited: false,
                logging: false,
            };
        }
        if (mat_fault || mat_backpressure) && worker.fs_storage.agains() != agains0 {
            // The device made the op wait: not a fault, and not a write
            // refused part-way. The scope undoes it whole (`scope_end`)
            // and the caller applies it again later.
            worker.scope_waited = true;
            return Applied::waited(worker.disk_revision);
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
            // Taken: the log holds the record and finishes it on later
            // steps. The op stands; its answer waits for the log.
            Err(command_log::LogError::Pending) => {
                worker.clog_appends = worker.clog_appends.wrapping_add(1);
                logging = true;
            }
            // Another record still pending (the gate in `apply_to_store`
            // makes this unreachable): the op is undone and asked again.
            Err(command_log::LogError::Busy) => {
                worker.scope_waited = true;
                return Applied::waited(worker.disk_revision);
            }
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
        waited: false,
        logging,
    }
}

/// TEST ONLY (`fault_slow_write_ms`): whether the write command at the
/// channel head (`frame`, peeked whole) is one the slow device keeps
/// waiting now. Each write that reaches the head is counted once; one in
/// `fault_slow_write_every` waits `fault_slow_write_ms` there, unapplied.
unsafe fn slow_write_waits(worker: &mut WorkerState, frame: &[u8]) -> bool {
    if worker.fault_slow_write_ms == 0 || worker.syscalls.is_null() {
        return false;
    }
    if frame.first() != Some(&MSG_KV_COMMAND) {
        return false;
    }
    let Some(head) = frame
        .get(ENVELOPE_HDR..)
        .and_then(wire::KvCommandHead::decode)
    else {
        return false;
    };
    if op_only_reads(head.op) {
        return false;
    }
    let now = dev_millis(&*worker.syscalls);
    if !worker.slow_seen || head.corr_id != worker.slow_corr {
        worker.slow_seen = true;
        worker.slow_corr = head.corr_id;
        worker.slow_count = worker.slow_count.wrapping_add(1);
        let every = worker.fault_slow_write_every.max(1);
        worker.slow_until_ms = if worker.slow_count.is_multiple_of(every) {
            now.saturating_add(u64::from(worker.fault_slow_write_ms))
        } else {
            0
        };
    }
    now < worker.slow_until_ms
}

/// Tell the router, at most once per `KV_WORKING_INTERVAL_MS`, that a
/// command it sent here is still being worked on (`MSG_KV_WORKING`): one
/// still waits at the command channel's head after this step's drain — the
/// device made it wait, its reply waits for the command log, the store is
/// making room for it, or the step's budget ran out before it — or a paused
/// walk is held. Commands are taken in channel order and each one taken is
/// answered (a paused walk from the hold slot), so the router keeps every
/// direct op it routed here in flight rather than answering it fail-closed
/// for being slow. A worker that has stopped says nothing, and the router's
/// deadline answers its commands — and so does a quarantined disk store:
/// it never serves again, and the commands at its head are no one's work.
unsafe fn say_working(worker: &mut WorkerState) {
    let sys_ptr = worker.syscalls;
    if sys_ptr.is_null() || worker.responses_out < 0 || worker.commands_in < 0 {
        return;
    }
    if worker.state_store == STATE_STORE_DISK && worker.disk_phase == DISK_PHASE_QUARANTINED {
        return;
    }
    let sys = &*sys_ptr;
    let now = dev_millis(sys);
    if now.wrapping_sub(worker.working_said_ms) < wire::KV_WORKING_INTERVAL_MS {
        return;
    }
    let mut frame = [0u8; ENVELOPE_HDR + SCRATCH_BUF_SIZE];
    let handle = worker.commands_in;
    let head = match worker
        .commands_inbox
        .peek(&mut Chan { sys, handle }, &mut frame)
    {
        Some((MSG_KV_COMMAND, len)) => frame
            .get(ENVELOPE_HDR..ENVELOPE_HDR + len)
            .and_then(wire::KvCommandHead::decode),
        _ => None,
    };
    let held = if worker.held_active {
        wire::KvCommandHead::decode(&worker.held_frame[..worker.held_len as usize])
    } else {
        None
    };
    let Some(cmd) = head.or(held) else {
        return;
    };
    let mut env = [0u8; ENVELOPE_HDR + wire::KV_WORKING_LEN];
    env[0] = MSG_KV_WORKING;
    env[1] = wire::KV_WORKING_LEN as u8;
    env[3..5].copy_from_slice(&cmd.kpg_id.to_le_bytes());
    env[5..13].copy_from_slice(&cmd.corr_id.to_le_bytes());
    if (sys.channel_write)(worker.responses_out, env.as_mut_ptr(), env.len()) == env.len() as i32 {
        worker.working_said_ms = now;
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

/// The command at the channel head (`cand`, its payload, peeked whole) is
/// pausable and the slot is taken: consume it if it is the held request
/// re-issued (identical but for `corr_id`), replacing the held identity.
/// Otherwise leave it.
unsafe fn adopt_retry(worker: &mut WorkerState, cand: &[u8]) -> bool {
    const CORR: core::ops::Range<usize> = 0..8;
    let held_len = worker.held_len as usize;
    let held = &worker.held_frame[..held_len];
    if cand.len() != held_len || cand[CORR.end..] != held[CORR.end..] {
        return false;
    }
    // Consume it (header + payload) and adopt its correlation.
    let sys = &*worker.syscalls;
    let handle = worker.commands_in;
    let mut sink = [0u8; ENVELOPE_HDR + HELD_FRAME_MAX];
    if !worker
        .commands_inbox
        .take(&mut Chan { sys, handle }, held_len, &mut sink)
    {
        return false;
    }
    worker.held_frame[CORR].copy_from_slice(&cand[CORR]);
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
        // Still paging, or the device made this slice wait: held, and
        // re-driven next step.
        CmdOutcome::Paused | CmdOutcome::Waited => {}
        CmdOutcome::Answered(_) | CmdOutcome::Refused | CmdOutcome::Logged => {
            worker.held_active = false;
        }
    }
    true
}

/// Is `frame` (the whole envelope at the head of `commands`, peeked) a
/// command that may pause? While a paused command is held, its successor
/// of the same kind stays on the channel — the channel is the spill
/// buffer — until the slot frees. Every other frame is admitted.
fn frame_is_pausable(frame: &[u8]) -> bool {
    frame.first() == Some(&MSG_KV_COMMAND)
        && frame
            .get(ENVELOPE_HDR..)
            .and_then(wire::KvCommandHead::decode)
            .is_some_and(|h| h.op == KV_OP_SCAN_VERSIONS)
}

/// Is `frame` (the whole frame at the head of `commands`, peeked) admitted
/// ([`admit`])? Anything but a well-formed write passes: it is read and
/// handled, or refused, as before.
unsafe fn frame_admitted(worker: &mut WorkerState, frame: &[u8]) -> bool {
    // The memory store admits everything.
    if worker.state_store != STATE_STORE_DISK || frame.first() != Some(&MSG_KV_COMMAND) {
        return true;
    }
    let Some(head) = frame
        .get(ENVELOPE_HDR..)
        .and_then(wire::KvCommandHead::decode)
    else {
        return true;
    };
    // A read needs no room.
    if op_only_reads(head.op) {
        return true;
    }
    let body_off = ENVELOPE_HDR + wire::KvCommandHead::LEN;
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
/// The change log's bytes when the `store_change_log` param names none.
#[cfg(feature = "store_provider")]
const PROVIDER_LOG: usize = store_provider::CHANGE_LOG_DEFAULT;
/// Heap held back for the subscription registry's pages beyond the
/// declared demand (`heap_per_demand` adds that to the instance's heap).
#[cfg(feature = "store_provider")]
const REGISTRY_RESERVE: usize = 64 * 1024;
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
/// The provider's state, rounded so the buffers carved after it start on
/// the allocator's 16-byte grain.
#[cfg(feature = "store_provider")]
const PROVIDER_STATE: usize = core::mem::size_of::<store_provider::Provider>().next_multiple_of(16);
/// The provider and its buffers: one allocation (`provider_new`), the
/// import's record added to it when there is a host log to import.
#[cfg(feature = "store_provider")]
const PROVIDER_HEAP: usize = PROVIDER_STATE + PROVIDER_REQ + PROVIDER_ANS + PROVIDER_CACHE;
/// The heap: the provider's allocation, the command log's, and the
/// allocator's rounding.
#[cfg(feature = "store_provider")]
const ARENA_BYTES: usize =
    PROVIDER_HEAP + PROVIDER_LOG + REGISTRY_RESERVE + IMPORT_RECORD_MAX + CLOG_ARENA + 64 * 1024;
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
    /// A provider call is being served (its state is the call's).
    in_call: bool,
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
    m_calls: u64,
    m_not_serving: u64,
    /// Calls by op class (telemetry): get, range, put, delete, list,
    /// subscribe, changes, other.
    m_ops: [u32; 8],
    m_refused: u64,
    /// `Ready` has been signalled to the scheduler (once, see `module_step`).
    signalled: bool,
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
            let t0 = dev_micros(&*w.syscalls);
            while !admit(w, op, body) {
                if rounds >= BUSY_ROUNDS
                    || w.disk_phase != DISK_PHASE_SERVING
                    || dev_micros(&*w.syscalls).saturating_sub(t0) > BUSY_CALL_BUDGET_US
                {
                    w.prov.busy = true;
                    return (types::KV_RESULT_INTERNAL, 0);
                }
                disk_maintenance(w);
                rounds += 1;
                if w.disk_state.io_wait {
                    // The making of room waits on the device, which moves
                    // only between steps: asking again inside this call
                    // cannot help. The caller asks again.
                    w.prov.busy = true;
                    return (types::KV_RESULT_INTERNAL, 0);
                }
            }
            let a = apply_to_store(w, op, (0, 0, 0), consistency, 0, body, out);
            w.prov.busy = a.busy || a.waited;
            if a.paused {
                return (KV_RESULT_PAUSED, 0);
            }
            if a.waited {
                // Undone whole: the caller asks again (EINPROGRESS for a
                // write, EAGAIN for a read).
                return (types::KV_RESULT_INTERNAL, 0);
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

    fn log_pending(&mut self) -> bool {
        // SAFETY: the worker's own state and syscalls, inside a provider
        // call (no step of this module is running).
        unsafe { !settle_log(self.worker) }
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

    fn module(&self) -> Option<[u8; 8]> {
        self.query(-1, CALLER_MODULE, [0; 8])
    }

    fn module_live(&self, module: [u8; 8]) -> bool {
        let mut rec = module;
        // SAFETY: the worker's syscall table; 8 readable, writable bytes.
        let rc = unsafe {
            ((*self.worker.syscalls).provider_query)(-1, MODULE_LIVE, rec.as_mut_ptr(), rec.len())
        };
        rc == 1
    }

    fn event_owner(&self, event: i32) -> Option<[u8; 8]> {
        self.query(event, EVENT_OWNER, [0; 8])
    }

    fn signal(&mut self, event: i32) -> i32 {
        // SAFETY: the worker's syscall table; SIGNAL takes no argument.
        unsafe {
            ((*self.worker.syscalls).provider_call)(event, EVENT_SIGNAL, core::ptr::null_mut(), 0)
        }
    }

    fn alloc(&mut self, bytes: usize) -> *mut u8 {
        // SAFETY: the worker's syscall table; the block is the provider's
        // for the module's life (registry pages are never returned).
        unsafe { ((*self.worker.syscalls).heap_alloc)(bytes as u32) }
    }

    fn take_keys<F: FnMut(u64, u8, &[u8])>(&mut self, mut f: F) -> Option<(bool, u64)> {
        let w = &mut *self.worker;
        // Mid-command (an origin entry's applies, a scope not yet closed),
        // or a command whose record the log has not yet made durable: its
        // keys are not the store's for good yet.
        if w.scope_depth > 0 || w.clog_hold || (w.clog_phase == CLOG_LIVE && w.clog.pending()) {
            return None;
        }
        let rev = store_revision(w);
        let kl = if w.state_store == STATE_STORE_DISK {
            &mut w.keylog
        } else {
            &mut w.store.keylog
        };
        for (r, h, k) in kl.entries() {
            f(r, h, k);
        }
        let complete = !kl.overflow;
        kl.clear();
        Some((complete, rev))
    }

    fn ledger_deny(&mut self) {
        let mut pool = POOL_STORE_SUBS.to_le_bytes();
        // SAFETY: the worker's syscall table; `[pool u16]`.
        unsafe {
            ((*self.worker.syscalls).provider_call)(-1, LEDGER_DENY, pool.as_mut_ptr(), pool.len());
        }
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
/// `query_key::CALLER_MODULE`, `MODULE_LIVE`, `EVENT_OWNER`: the calling
/// module's occupancy, whether one still stands, and whose an event is.
#[cfg(feature = "store_provider")]
const CALLER_MODULE: u32 = 10;
#[cfg(feature = "store_provider")]
const MODULE_LIVE: u32 = 11;
#[cfg(feature = "store_provider")]
const EVENT_OWNER: u32 = 12;
/// `kernel_abi::event::SIGNAL`.
#[cfg(feature = "store_provider")]
const EVENT_SIGNAL: u32 = 0x0B01;
/// `internal::reconfigure::OWNER_RELEASED`, and its `[slot][0][generation]`
/// argument.
#[cfg(feature = "store_provider")]
const OWNER_RELEASED: u32 = 0x0C22;
/// `resource::POOL_SAMPLE` (the ledger asks how much of a pool is in use),
/// `resource::LEDGER_DENY` (a refusal counted against it), and the pool
/// this provider serves, `resource::POOL_STORE_SUBS`.
#[cfg(feature = "store_provider")]
const POOL_SAMPLE: u32 = 0x0C25;
#[cfg(feature = "store_provider")]
const LEDGER_DENY: u32 = 0x0C24;
#[cfg(feature = "store_provider")]
const POOL_STORE_SUBS: u16 = 0x000A;

#[cfg(feature = "store_provider")]
impl WorkerBackend<'_> {
    /// An 8-byte record from `provider_query(handle, key)`, or `None` when
    /// the kernel answers none.
    fn query(&self, handle: i32, key: u32, mut rec: [u8; 8]) -> Option<[u8; 8]> {
        // SAFETY: the worker's syscall table; 8 writable bytes.
        let rc = unsafe {
            ((*self.worker.syscalls).provider_query)(handle, key, rec.as_mut_ptr(), rec.len())
        };
        // All zero is a record too: module 0 at generation 0.
        (rc >= rec.len() as i32).then_some(rec)
    }
}
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
        // The provider and its buffers, the import's record among them when
        // there is a host log to import, as ONE allocation carved in order.
        // The heap may be elastic, growing in segments that are not
        // contiguous, and an allocation cannot span two: taken one by one,
        // the buffers would strand each segment's tail and a heap declared
        // at their sum would refuse the last.
        let importing = worker.import_path_len > 0;
        let log = if worker.store_change_log == 0 {
            PROVIDER_LOG
        } else {
            worker.store_change_log as usize
        };
        let total = PROVIDER_HEAP + log + if importing { IMPORT_RECORD_MAX } else { 0 };
        let base = (sys.heap_alloc)(total as u32);
        p.provides = !base.is_null();
        if p.provides {
            p.state = base.cast::<store_provider::Provider>();
            let mut at = base.add(PROVIDER_STATE);
            p.req = at;
            at = at.add(PROVIDER_REQ);
            p.ans = at;
            at = at.add(PROVIDER_ANS);
            p.cache = at;
            at = at.add(PROVIDER_CACHE);
            let log_at = at;
            if importing {
                p.import_buf = at.add(log);
            }
            store_provider::Provider::clear(p.state);
            (*p.state).attach_log(log_at, log);
            (*p.state).set_quota(worker.store_sub_quota);
            // Every key a command changes is noted for the change log.
            worker.keylog.clear();
            worker.keylog.on = true;
            worker.keylog.want = (0, 0, 0);
        }
        if p.provides {
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
        if p.provides && worker.fault_read_eagain_every != 0 {
            let every = worker.fault_read_eagain_every;
            let on = (*p.state).fault_read_eagain_every(every);
            let mut m = [0u8; 128];
            let what: &[u8] = if on {
                b"[kvw] FAULT INJECTION ON (test only): one store step in N busy, every read EAGAIN, N="
            } else {
                b"[kvw] fault injection REFUSED (N=1: every step busy) N="
            };
            let mut k = write_prefix(&mut m, 0, what);
            k = write_dec(&mut m, k, u64::from(every));
            if on && worker.fault_busy_reaches_store != 0 {
                k = write_prefix(&mut m, k, b" (in the store)");
            }
            dev_log(sys, 1, m.as_ptr(), k);
        }
        if p.provides {
            (*p.state).fault_busy_reaches_store(worker.fault_busy_reaches_store != 0);
        }
        if p.provides && worker.fault_log_hold_steps != 0 {
            let hold = worker.fault_log_hold_steps;
            (*p.state).fault_log_hold_steps(hold);
            let mut m = [0u8; 96];
            let mut k = write_prefix(
                &mut m,
                0,
                b"[kvw] FAULT INJECTION ON (test only): each write's log record held, steps=",
            );
            k = write_dec(&mut m, k, u64::from(hold));
            dev_log(sys, 1, m.as_ptr(), k);
        }
        if p.provides && worker.import_path_len > 0 {
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
    let mut buf = [0u8; 640];
    let mut n = write_prefix(&mut buf, 0, b"[kvw] store hb calls=");
    n = write_dec(&mut buf, n, p.m_calls);
    n = write_prefix(&mut buf, n, b" not_serving=");
    n = write_dec(&mut buf, n, p.m_not_serving);
    n = write_prefix(&mut buf, n, b" refused=");
    n = write_dec(&mut buf, n, p.m_refused);
    {
        let pv = &*p.state;
        n = write_prefix(&mut buf, n, b" subs=");
        n = write_dec(&mut buf, n, pv.subscriptions() as u64);
        let m = pv.m;
        n = write_prefix(&mut buf, n, b" rung=");
        n = write_dec(&mut buf, n, u64::from(m.rung));
        n = write_prefix(&mut buf, n, b" pending=");
        n = write_dec(&mut buf, n, pv.rung() as u64);
        n = write_prefix(&mut buf, n, b" taken=");
        n = write_dec(&mut buf, n, u64::from(m.taken));
        n = write_prefix(&mut buf, n, b" gaps=");
        n = write_dec(&mut buf, n, u64::from(m.gaps));
        n = write_prefix(&mut buf, n, b" reaped=");
        n = write_dec(&mut buf, n, u64::from(m.reaped));
        n = write_prefix(&mut buf, n, b" sub_refused=");
        n = write_dec(&mut buf, n, u64::from(m.refused));
        n = write_prefix(&mut buf, n, b" owner_checks_failed=");
        n = write_dec(&mut buf, n, u64::from(m.owner_checks_failed));
        n = write_prefix(&mut buf, n, b" walk_refused=");
        n = write_dec(&mut buf, n, u64::from(m.walk_refused));
        let (base, frontier, used) = pv.log_span();
        n = write_prefix(&mut buf, n, b" frontier_lag=");
        n = write_dec(&mut buf, n, store_revision(worker).saturating_sub(frontier));
        n = write_prefix(&mut buf, n, b" log_base=");
        n = write_dec(&mut buf, n, base);
        n = write_prefix(&mut buf, n, b" log_bytes=");
        n = write_dec(&mut buf, n, used);
        n = write_prefix(&mut buf, n, b" pages=");
        n = write_dec(&mut buf, n, pv.registry_pages() as u64);
        n = write_prefix(&mut buf, n, b" hashed=");
        n = write_dec(&mut buf, n, u64::from(pv.m_ident_hashed));
        let (fault_on, injected) = pv.fault_injected();
        if fault_on {
            n = write_prefix(&mut buf, n, b" fault=");
            n = write_dec(&mut buf, n, u64::from(injected));
        }
    }
    n = write_prefix(&mut buf, n, b" ops=");
    for (i, c) in p.m_ops.iter().enumerate() {
        if i > 0 {
            n = write_prefix(&mut buf, n, b"/");
        }
        n = write_dec(&mut buf, n, u64::from(*c));
    }
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
        n = write_prefix(&mut buf, n, b" ostep=");
        n = write_dec(&mut buf, n, u64::from(r.step));
        n = write_prefix(&mut buf, n, b" rtry=");
        n = write_dec(&mut buf, n, u64::from(r.reg_tries));
        n = write_prefix(&mut buf, n, b" rsent=");
        n = write_dec(&mut buf, n, u64::from(r.reg_sent));
        n = write_prefix(&mut buf, n, b" rseen=");
        n = write_dec(&mut buf, n, u64::from(r.reg_seen));
        n = write_prefix(&mut buf, n, b" rmiss=");
        n = write_dec(&mut buf, n, u64::from(r.reg_miss));
        n = write_prefix(&mut buf, n, b" ch=");
        n = write_dec(&mut buf, n, u64::from(r.chunks_held));
        n = write_prefix(&mut buf, n, b"/");
        n = write_dec(&mut buf, n, u64::from(r.chunks_offered));
        n = write_prefix(&mut buf, n, b" chref=");
        n = write_dec(&mut buf, n, u64::from(r.chunk_refused));
        n = write_prefix(&mut buf, n, b" chidle=");
        n = write_dec(&mut buf, n, u64::from(r.chunk_idle));
    }
    n = write_prefix(&mut buf, n, b" ai=");
    n = write_dec(&mut buf, n, worker.applied_index);
    n = write_prefix(&mut buf, n, b" hblk=");
    n = write_dec(&mut buf, n, worker.m_head_blocked);
    n = write_prefix(&mut buf, n, b" big=");
    n = write_dec(&mut buf, n, u64::from(worker.commands_inbox.oversize));
    n = write_prefix(&mut buf, n, b" oskip=");
    n = write_dec(&mut buf, n, worker.m_origin_skipped);
    log_line(worker, &buf, n);
}

/// A snapshot replaced the store: the change log no longer answers for
/// what came before (a gap — every subscriber reads again), and what
/// became of the writes the store provider proposed is read from its
/// origin record.
#[cfg(feature = "store_provider")]
unsafe fn provider_resync(worker: &mut WorkerState) {
    if !worker.prov.provides {
        return;
    }
    // Keys staged before the install name a store that is gone.
    worker.keylog.clear();
    worker.store.keylog.clear();
    let rev = store_revision(worker);
    (*worker.prov.state).log_gap(rev);
    if worker.sweep_wrap == 0 {
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

/// And the time it may spend doing so, in microseconds: this runs inside
/// the CALLER's step, under the caller's step deadline (2 ms by default).
/// Past it the write answers not-yet (`EINPROGRESS`: asked again with the
/// same request) and the worker's own steps finish the flush. On a host each round is a page-cache write
/// and the budget is rarely met; on bare metal the disk only moves when
/// fat32 and nvme step, which they cannot while this spins, so without the
/// bound the call ran until the step guard cut the caller off mid-way
/// through the store's own maintenance.
#[cfg(feature = "store_provider")]
const BUSY_CALL_BUDGET_US: u64 = 1_000;

/// Storage actions construction may take before it leaves the rest to the
/// loader's next call (each is a bounded slice of recovery, replay or
/// import, and every one moves the store on).
#[cfg(feature = "store_provider")]
const READY_ROUNDS: u32 = 1 << 20;

/// Make the store ready at construction where it can be: a host store
/// recovers as it opens, so the runs are recovered, the command log's tail
/// replayed and any import done here, and a consumer's first call finds it
/// serving.
///
/// Where it cannot — its FS provider is not up yet, as on bare metal, where
/// the disk mounts only once the graph steps (nvme, then fat32) — the store
/// is constructed anyway and recovers through its own steps. It is
/// deferred-ready (`module_deferred_ready`): it signals Ready the first step
/// it serves, and the kernel holds every module requiring its contracts
/// until then (fluxor's contract-readiness gate), so no consumer meets it
/// unready. Waiting here instead could never end on metal: the loader
/// re-asks a pending construction without stepping the disk below it.
///
/// `0`: constructed (ready, or recovering through its steps; or not a
/// provider); `-1`: the store cannot serve.
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
        // The device moves only between steps: once it has made a round
        // wait, more rounds inside this call cannot help, and the steps
        // carry on from where this one stood.
        let agains0 = worker.fs_storage.agains();
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
            if worker.fs_storage.agains() != agains0 {
                let m = b"[kvw] store importing after construction (disk waits): Ready once done";
                dev_log(&*worker.syscalls, DISK_LOG_LEVEL, m.as_ptr(), m.len());
                return 0;
            }
            continue;
        }
        disk_maintenance(worker);
        drive_command_log(worker);
        if worker.fs_storage.agains() != agains0 {
            // The FS provider is not up yet, or its device makes recovery
            // wait: recover through the steps.
            let m = b"[kvw] store recovering after construction (disk waits): Ready once it serves";
            dev_log(&*worker.syscalls, DISK_LOG_LEVEL, m.as_ptr(), m.len());
            return 0;
        }
    }
    // Construction's share of the recovery is spent; the steps finish it.
    let m = b"[kvw] store recovering after construction: Ready once it serves";
    dev_log(&*worker.syscalls, DISK_LOG_LEVEL, m.as_ptr(), m.len());
    0
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
            // Not read yet: the same record next step.
            Err(kv_store::disk_store::run_storage::StorageError::Again) => return,
            Err(_) => {
                log_disk_errno(worker, b"[kvw] import read failed, QUARANTINE errno=");
                worker.disk_phase = DISK_PHASE_QUARANTINED;
                return;
            }
        };
        // A short read is what the provider had cached, not the log's end:
        // read on (each read moves bytes, finds the end, or waits).
        let mut got = got;
        while got > 0 && got < head.len() {
            let more = head.get_mut(got..).unwrap_or(&mut []);
            match worker.fs_storage.read_path(path, at + got as u64, more) {
                Ok(0) => break,
                Ok(n) => got += n,
                // Not read yet: the same record next step.
                Err(kv_store::disk_store::run_storage::StorageError::Again) => return,
                Err(_) => {
                    log_disk_errno(worker, b"[kvw] import read failed, QUARANTINE errno=");
                    worker.disk_phase = DISK_PHASE_QUARANTINED;
                    return;
                }
            }
        }
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
                Ok(0) => break,
                Ok(n) => filled += n,
                // Not read yet: the same record next step, from its head.
                Err(kv_store::disk_store::run_storage::StorageError::Again) => return,
                Err(_) => break,
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
    worker.clog_defer = false;
    if worker.import_end_pending {
        // The end's record is applied; it is recorded once the log holds
        // it durably.
        if !settle_log(worker) {
            return;
        }
        worker.import_end_pending = false;
        worker.prov.import_phase = IMPORT_NONE;
        worker.prov.import_marked = true;
        import_logged(worker);
        return;
    }
    // What was imported is durable before its end is recorded. A fence
    // the device has not finished is asked again next step, the import
    // standing at its end meanwhile.
    if worker.sweep_wrap == 0 && worker.clog_phase == CLOG_LIVE {
        match worker.clog.sync(&mut worker.fs_storage) {
            Ok(()) => {}
            Err(command_log::LogError::Pending) => return,
            Err(_) => {
                log_disk_errno(
                    worker,
                    b"[kvw] import could not make itself durable, QUARANTINE errno=",
                );
                worker.disk_phase = DISK_PHASE_QUARANTINED;
                return;
            }
        }
    }
    worker.prov.import_phase = IMPORT_NONE;
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
    if a.waited {
        // Undone: the end is recorded on a later step.
        worker.prov.import_phase = IMPORT_RUN;
        return;
    }
    if a.result != KV_RESULT_OK {
        log_disk_errno(
            worker,
            b"[kvw] import could not record its end, QUARANTINE errno=",
        );
        worker.disk_phase = DISK_PHASE_QUARANTINED;
        return;
    }
    if a.logging {
        // Recorded once the log holds the end's record durably.
        worker.import_end_pending = true;
        worker.prov.import_phase = IMPORT_RUN;
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

/// Marks this provider as the ledger's sampler for the pool it serves
/// (`store_subs`): the kernel asks `POOL_SAMPLE` once a PSTATUS round.
#[cfg(feature = "store_provider")]
#[no_mangle]
#[link_section = ".text.module_samples_pool"]
pub extern "C" fn module_samples_pool() -> u32 {
    1
}

/// Subscribes this provider to `OWNER_RELEASED`: a released owner's
/// subscriptions are dropped at once (see `module_provider_dispatch`).
#[cfg(feature = "store_provider")]
#[no_mangle]
#[link_section = ".text.module_observes_owner_release"]
pub extern "C" fn module_observes_owner_release() -> u32 {
    1
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
        if op == POOL_SAMPLE {
            // The ledger's question: the subscriptions held, which is what
            // this provider serves of `store_subs`.
            if arg.is_null() || arg_len < 2 {
                return store_provider::EINVAL;
            }
            let pool = u16::from_le_bytes([*arg, *arg.add(1)]);
            if pool != POOL_STORE_SUBS {
                return store_provider::ENOSYS;
            }
            return (*worker.prov.state).subscriptions() as i32;
        }
        if op == OWNER_RELEASED {
            // An owner going away: its subscriptions go now. (Before the
            // readiness gate: a release never waits for the store. The lazy
            // check before every signal would drop them anyway; this returns
            // their memory early.)
            if !arg.is_null() && arg_len >= 8 {
                let mut owner = [0u8; 8];
                core::ptr::copy_nonoverlapping(arg as *const u8, owner.as_mut_ptr(), 8);
                (*worker.prov.state).owner_released(owner);
            }
            return 0;
        }
        worker.prov.m_calls = worker.prov.m_calls.wrapping_add(1);
        let class = match op {
            store_provider::OBJ_GET => 0,
            store_provider::OBJ_RANGE_GET => 1,
            store_provider::OBJ_PUT => 2,
            store_provider::OBJ_DELETE => 3,
            store_provider::NS_LIST => 4,
            store_provider::NS_SUBSCRIBE => 5,
            store_provider::NS_CHANGES => 6,
            _ => 7,
        };
        worker.prov.m_ops[class] = worker.prov.m_ops[class].wrapping_add(1);
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
        worker.prov.in_call = true;
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
        worker.prov.in_call = false;
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
        if worker.fault_slow_write_ms != 0 {
            let mut m = [0u8; 112];
            let mut k = write_prefix(
                &mut m,
                0,
                b"[kvw] FAULT INJECTION ON (test only): slow device, a write waits ms=",
            );
            k = write_dec(&mut m, k, u64::from(worker.fault_slow_write_ms));
            k = write_prefix(&mut m, k, b" one in=");
            k = write_dec(&mut m, k, u64::from(worker.fault_slow_write_every.max(1)));
            dev_log(&*sys_ptr, 1, m.as_ptr(), k);
        }
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
    // Both buffers as one allocation: the heap may be elastic, growing in
    // segments an allocation cannot span, and buffers taken one by one
    // would strand a segment's tail the declared arena does not cover.
    let base = (sys.heap_alloc)(CLOG_ARENA as u32);
    worker.clog_buf = base;
    worker.clog_out = if base.is_null() {
        core::ptr::null_mut()
    } else {
        base.add(command_log::PAYLOAD_MAX)
    };
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
/// steps, and each of those commands pays disk I/O the ones before the
/// flush did not. 32 x (a few hundred us) is how a step that looks bounded
/// blows a 16 ms burst ceiling.
///
/// 1500 us leaves the rest of the module's 12000/16000 declared budget
/// to maintenance (measured at <=1 ms per phase with the A/B manifest)
/// with a wide margin. The budget is checked BEFORE starting
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
        let mut env = [0u8; ENVELOPE_HDR + 32];
        if let Some((_, payload_len)) =
            read_envelope(&mut worker.expire_inbox, sys, worker.expire_in, &mut env)
        {
            let body = &env[..payload_len];
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

/// The store variant is deferred-ready: it is not `Ready` until its disk
/// store serves, and the kernel holds back every module that requires a
/// contract it provides until then (fluxor's contract-readiness gate). On
/// Linux the store recovers at construction and is ready on its first step;
/// on bare metal its disk mounts only once the graph steps, and a connector
/// whose first call met `EAGAIN` would otherwise conclude the store refused
/// it.
///
/// Ready means the DISK serves, not that the store has imported a host log
/// through the replicated log: that import needs the consensus path, which
/// is downstream of this module on its edges and so is held by the kernel's
/// edge gate until this module is Ready. Waiting for the import here would
/// deadlock the graph; calls meanwhile are answered `EAGAIN`, as before.
#[cfg(feature = "store_provider")]
#[no_mangle]
#[link_section = ".text.module_deferred_ready"]
pub extern "C" fn module_deferred_ready() -> u32 {
    1
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    #[cfg(feature = "store_provider")]
    if !state.is_null() {
        // SAFETY: the kernel passes this module's own state.
        let worker = unsafe { &mut *state.cast::<WorkerState>() };
        if worker.prov.provides && !worker.prov.state.is_null() {
            // A step for the provider: held write answers age, and an
            // injected wait (test only) lasts until this step.
            unsafe { (*worker.prov.state).step() };
        }
    }
    let rc = step(state);
    #[cfg(feature = "store_provider")]
    if rc == 0 && !state.is_null() {
        // SAFETY: the kernel passes this module's own state.
        let worker = unsafe { &mut *state.cast::<WorkerState>() };
        // A worker that registered no contract gates nothing: Ready at once,
        // so a configuration that serves no store never freezes the graph
        // behind it (it says why in `[kvw] store provider=0 (...)`).
        let serving = worker.state_store != STATE_STORE_DISK
            || worker.disk_phase == DISK_PHASE_QUARANTINED
            || store_serving(worker);
        if !worker.prov.signalled && (!worker.prov.provides || serving) {
            worker.prov.signalled = true;
            if !worker.syscalls.is_null() {
                let m = b"[kvw] store serving: Ready";
                unsafe { dev_log(&*worker.syscalls, DISK_LOG_LEVEL, m.as_ptr(), m.len()) };
            }
            return 3;
        }
    }
    rc
}

fn step(state: *mut u8) -> i32 {
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
            // Emitting after the action would report nothing at all: a
            // blackout exactly when the report is needed.
            disk_observe(worker);
            disk_maintenance(worker);
            drive_command_log(worker);
            drain_retirements(worker);
        }
        drain_epoch_events(worker);
        drain_snapshot_ctl(worker);
        export_step(worker);
        span_export_step(worker);
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
    unsafe { say_working(worker) };
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
            // The partition leads so the router fences per-partition.
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
            let mut buf = [0u8; 160];
            let mut n = write_prefix(&mut buf, 0, b"[kvw] drain n=");
            n = write_dec(&mut buf, n, u64::from(drained));
            n = write_prefix(&mut buf, n, b" msg=");
            n = write_dec(&mut buf, n, u64::from(worker.last_msg));
            n = write_prefix(&mut buf, n, b" op=");
            n = write_dec(&mut buf, n, u64::from(worker.last_op));
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
                        // The device could not take the slot's action now
                        // (or is still mounting): asked again next step,
                        // where it stands.
                        Err(StoreError::Backpressure) => {
                            worker.disk_io_waits = worker.disk_io_waits.wrapping_add(1);
                            worker.disk_degraded_steps =
                                worker.disk_degraded_steps.saturating_add(1);
                            return;
                        }
                        Err(_) => {
                            worker.disk_degraded_steps =
                                worker.disk_degraded_steps.saturating_add(1);
                            log_disk_errno(worker, b"[kvw] disk QUARANTINE prepare errno=");
                            worker.disk_phase = DISK_PHASE_QUARANTINED;
                            return;
                        }
                    }
                }
            }
            // One bounded slice of recovery per step; a storage call the
            // device cannot take now is asked again next step, recovery
            // standing exactly where it was (its listing, the run being
            // verified, the delete in flight).
            let outcome = {
                let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                store.recover_step()
            };
            if matches!(outcome, Ok(Progress::InProgress { .. })) {
                return;
            }
            let recovered = matches!(outcome, Ok(Progress::Done));
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
            } else if outcome == Err(StoreError::Backpressure) {
                // The device (or a provider still mounting) made recovery
                // wait: stay in RECOVERING; the next step continues it.
                // Counted, so a provider that never comes up is visible in
                // the periodic degraded line rather than looking like a
                // silent idle module.
                worker.disk_io_waits = worker.disk_io_waits.wrapping_add(1);
                worker.disk_degraded_steps = worker.disk_degraded_steps.saturating_add(1);
            } else {
                log_disk_errno(worker, b"[kvw] disk QUARANTINE recover errno=");
                worker.disk_phase = DISK_PHASE_QUARANTINED;
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
            // A memtable that must drain goes before a merge: a write that
            // waits for room (and the command log behind it) waits on the
            // flush alone, never on a merge of every run (seconds on a
            // loaded board). The store lets a flush run while the merge is
            // paused short of its manifest, and the merge publishes over
            // the flushed run (`DiskState::compact_pausable`). With the run
            // set full the flush cannot start: the merge goes first.
            let flush_first = worker.disk_flush_active
                || (worker.disk_flush_wanted
                    && worker.disk_state.flush_may_start()
                    && (worker.disk_state.run_count as usize) < kv_store::disk_store::MAX_RUNS);
            if worker.disk_compact_active && !worker.disk_flush_active && !flush_first {
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
                    // The device could not take the merge's storage action
                    // now (its block device is busy — the WAL's writes
                    // share it — or the provider is still mounting). The
                    // merge stands exactly where it was and the next step
                    // asks the same action again: nothing is restarted.
                    // A wait is the device's pace, not a stalled fence.
                    Err(StoreError::Backpressure)
                        if worker.disk_state.io_wait
                            && worker.disk_state.compaction_in_flight().is_some() =>
                    {
                        worker.disk_io_waits = worker.disk_io_waits.wrapping_add(1);
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
                if outcome == Err(StoreError::Backpressure) && worker.disk_state.flush_active() {
                    // The device could not take the flush's storage action
                    // now. The flush stands where it was — its run file,
                    // its blocks, its fence — and the next step asks the
                    // same action again; nothing is written twice and
                    // nothing restarts, however long the device takes.
                    worker.disk_io_waits = worker.disk_io_waits.wrapping_add(1);
                    return;
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
                        if worker.disk_capture_after_flush {
                            // The capture that asked for this flush: the
                            // manifest now names the applied position
                            // the memtable held when it froze.
                            worker.disk_capture_after_flush = false;
                            worker.m_cap_emit = worker.m_cap_emit.wrapping_add(1);
                            emit_snapshot_chunks(worker, &*worker.syscalls);
                        }
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
/// See that constant for the reasoning. In short: a snapshot capture or
/// an install refuses `apply` and `flush` with `Backpressure` (a transfer
/// capture refuses neither), a compaction refuses a flush once it stands
/// at its manifest, and an abandoned install stream leaves its fence
/// latched with nothing left to clear it. Every fence is safely
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
/// because they hold off the write path — `apply` refused outright, or
/// the flush a full memtable waits for. A flush does not: `apply` is
/// accepted throughout, right up to the memtable's capacity. So a flush
/// in flight is not starving
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
    let mut buf = [0u8; 448];
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
    // Device waits: storage actions asked again where they stood (iow),
    // `Again` answers from the FS provider (ag), and commands undone and
    // applied again (cw).
    n = write_prefix(&mut buf, n, b" iow=");
    n = write_dec(&mut buf, n, worker.disk_io_waits);
    n = write_prefix(&mut buf, n, b" ag=");
    n = write_dec(&mut buf, n, worker.fs_storage.agains());
    n = write_prefix(&mut buf, n, b" cw=");
    n = write_dec(&mut buf, n, worker.m_cmd_waits);
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
    //   hit = run blocks served from the block cache (no storage call)
    n = write_prefix(&mut buf, n, b" hit=");
    n = write_dec(&mut buf, n, worker.disk_state.block_cache.hits.get());
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
    //   rr   = the recover step refused at (0 = recovered; DiskState docs)
    //   qe   = the FS errno at the last quarantine-class refusal logged
    n = write_prefix(&mut buf, n, b" rr=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_state.recover_refusal));
    n = write_prefix(&mut buf, n, b" qe=");
    n = write_signed(&mut buf, n, worker.disk_quarantine_errno);
    //   rm/rl/rf = recovery: the run a manifest named but the listing
    //   lacked, runs listed, fallbacks to the other manifest slot
    n = write_prefix(&mut buf, n, b" rm=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_state.recover_missing_id));
    n = write_prefix(&mut buf, n, b" rl=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_state.recover_listed));
    n = write_prefix(&mut buf, n, b" rf=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_state.recover_fallbacks));
    //   sw = orphan runs a recovery deleted because its listing overflowed
    n = write_prefix(&mut buf, n, b" sw=");
    n = write_dec(&mut buf, n, u64::from(worker.disk_state.recover_swept));
    //   ma/mb = each manifest slot at the last recovery: verdict | gen << 4
    //   (7 valid, 6 CRC, 1 absent …); ra/rb = the highest run it names
    n = write_prefix(&mut buf, n, b" ma=");
    n = write_dec(
        &mut buf,
        n,
        u64::from(worker.disk_state.manifest_seen[0].get()),
    );
    n = write_prefix(&mut buf, n, b" mb=");
    n = write_dec(
        &mut buf,
        n,
        u64::from(worker.disk_state.manifest_seen[1].get()),
    );
    n = write_prefix(&mut buf, n, b" ra=");
    n = write_dec(
        &mut buf,
        n,
        u64::from(worker.disk_state.manifest_runs[0].get()),
    );
    n = write_prefix(&mut buf, n, b" rb=");
    n = write_dec(
        &mut buf,
        n,
        u64::from(worker.disk_state.manifest_runs[1].get()),
    );
    //   cap  = snapshot captures: requests seen / flushes asked / markers emitted
    n = write_prefix(&mut buf, n, b" cap=");
    n = write_dec(&mut buf, n, u64::from(worker.m_cap_req));
    n = write_prefix(&mut buf, n, b"/");
    n = write_dec(&mut buf, n, u64::from(worker.m_cap_ask));
    n = write_prefix(&mut buf, n, b"/");
    n = write_dec(&mut buf, n, u64::from(worker.m_cap_emit));
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
    // Kept for the heartbeat (`qe=`): the line itself is emitted once,
    // at boot, before a rig's capture attaches.
    worker.disk_quarantine_errno = worker.fs_storage.last_errno();
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
    let mut buf = [0u8; ENVELOPE_HDR + 128];
    let Some((kind, payload_len)) = read_envelope(
        &mut worker.durability_inbox,
        sys,
        worker.durability_in,
        &mut buf,
    ) else {
        return;
    };
    if kind != MSG_GC_FLOOR_COMMITTED || payload_len != GC_FLOOR_WIRE_LEN {
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
    let mut buf = [0u8; ENVELOPE_HDR + 64];
    let Some((kind, payload_len)) = read_envelope(
        &mut worker.epoch_events_inbox,
        sys,
        worker.epoch_events_in,
        &mut buf,
    ) else {
        return;
    };
    if kind != MSG_PLACEMENT_EPOCH_EVENT || payload_len < 7 {
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
/// Longest span bound a span request may carry.
const SPAN_BOUND_MAX: usize = 256;
/// Span-copy chunk frames one step writes at most (the channel's room
/// bounds it sooner).
const SPAN_FRAMES_PER_STEP: usize = 8;

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
    if worker.span_active {
        // A span copy owns `snapshot_export` until its last batch is
        // taken: an answer to anything read now would interleave with it.
        // The channel holds what arrives meanwhile.
        return;
    }
    let sys = &*sys_ptr;
    // A portable chunk the store refused for now is retried before the
    // next is read: the channel holds the rest of the stream meanwhile.
    if worker.import_pend_len > 0 && !install_pending(worker, sys) {
        return;
    }
    let mut env = [0u8; ENVELOPE_HDR + SCRATCH_BUF_SIZE];
    let Some((kind, payload_len)) = read_envelope(
        &mut worker.snapshot_import_inbox,
        sys,
        worker.snapshot_import_in,
        &mut env,
    ) else {
        return;
    };
    if payload_len < 16 {
        return;
    }
    worker.scratch[..payload_len].copy_from_slice(&env[..payload_len]);
    let term = le_u64(&worker.scratch[0..8]);
    let index = le_u64(&worker.scratch[8..16]);

    match kind {
        MSG_APP_SNAPSHOT_REQUEST => {
            worker.m_cap_req = worker.m_cap_req.wrapping_add(1);
            if worker.snapshot_export_out < 0 || worker.applied_index == 0 {
                // Nothing replicated applied yet — there is no position
                // we could honestly label a body with, so stay silent.
                return;
            }
            // The disk store's marker is labelled at the MANIFEST's
            // applied position, which only a flush publish moves. An
            // idle control plane writes too little to reach the flush
            // watermark, so without this the manifest stays at 0 (or
            // far behind), the marker is withheld (or stale), no
            // snapshot ever lands, and the log is replayed whole at
            // every boot. Ask for the flush now and answer when it
            // publishes; an empty memtable has nothing to publish, so
            // the marker stands at what the manifest already says.
            if worker.state_store == STATE_STORE_DISK
                && worker.disk_state.durable_applied_index < worker.applied_index
                && worker.disk_state.mem_count > 0
            {
                worker.disk_flush_wanted = true;
                worker.disk_capture_after_flush = true;
                worker.m_cap_ask = worker.m_cap_ask.wrapping_add(1);
                return;
            }
            worker.m_cap_emit = worker.m_cap_emit.wrapping_add(1);
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
            if worker.snapshot_export_out < 0 || payload_len < 20 {
                return;
            }
            let p = &worker.scratch;
            let sl = u16::from_le_bytes([p[16], p[17]]) as usize;
            let el_off = 18 + sl;
            if sl > SPAN_BOUND_MAX || el_off + 2 > payload_len {
                return;
            }
            let el = u16::from_le_bytes([p[el_off], p[el_off + 1]]) as usize;
            if el > SPAN_BOUND_MAX || el_off + 2 + el > payload_len {
                return;
            }
            worker.span_cursor[..sl].copy_from_slice(&p[18..18 + sl]);
            worker.span_end[..el].copy_from_slice(&p[el_off + 2..el_off + 2 + el]);
            worker.span_cursor_len = sl;
            worker.span_end_len = el;
            worker.span_active = true;
            worker.span_reset_sent = false;
            worker.span_more = false;
            worker.span_len = 0;
            worker.span_sent = 0;
            worker.span_since_ms = dev_millis(sys);
            span_export_step(worker);
        }
        MSG_APP_SNAPSHOT_EXPORT_REQUEST => {
            if worker.snapshot_export_out < 0 || payload_len < 20 {
                return;
            }
            // A new ask supersedes one under way: its peer gave up on it.
            export_abort(worker);
            worker.xfer_id = u32::from_le_bytes([
                worker.scratch[16],
                worker.scratch[17],
                worker.scratch[18],
                worker.scratch[19],
            ]);
            worker.xfer_active = true;
            worker.xfer_begun = false;
            worker.xfer_since_ms = dev_millis(sys);
        }
        MSG_APP_SNAPSHOT_RESET => {
            // A snapshot at or below the applied position is state this
            // store already holds, and more: raft keeps its log past such
            // an install (it is covered by its own log), so the entries
            // after the snapshot are never delivered again. Installed, it
            // would rewind the store below them and lose them. The store
            // is left as it stands; the stream is drained and
            // acknowledged, so the hand-off completes.
            if !worker.importing && worker.applied_index != 0 && index <= worker.applied_index {
                worker.import_covered = index;
                let mut buf = [0u8; 96];
                let mut n = write_prefix(&mut buf, 0, b"[kvw] install covered: index=");
                n = write_dec(&mut buf, n, index);
                n = write_prefix(&mut buf, n, b" applied=");
                n = write_dec(&mut buf, n, worker.applied_index);
                log_line(worker, &buf, n);
                return;
            }
            worker.import_covered = 0;
            // A new install supersedes a portable one still streaming.
            if worker.import_stream {
                if worker.state_store == STATE_STORE_DISK {
                    let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                    store.abort_fences();
                }
                worker.import_stream = false;
                worker.import_pend_len = 0;
            }
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
            worker.import_span_merge = false;
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
            // over-advancing past a non-durable snapshot is never safe.
            if worker.gc_snapshot_pending_index != 0 && index >= worker.gc_snapshot_pending_index {
                worker.gc_snapshot_revision = worker.gc_snapshot_pending_revision;
                worker.gc_snapshot_pending_index = 0;
            }
        }
        MSG_APP_SNAPSHOT_CHUNK => {
            if worker.import_covered != 0 && index == worker.import_covered {
                if payload_len >= APP_SNAPSHOT_HDR && worker.scratch[24] != 0 {
                    worker.import_covered = 0;
                    ack_install(worker, index);
                }
                return;
            }
            if !worker.importing || payload_len < APP_SNAPSHOT_HDR {
                return;
            }
            let offset = le_u64(&worker.scratch[16..24]) as usize;
            let done = worker.scratch[24] != 0;
            let body_len = payload_len - APP_SNAPSHOT_HDR;
            // A disk store's own body is a DISK-RESIDENT marker (header
            // only); anything else is a portable copy, which streams
            // straight into the store's REPLACE install.
            if worker.state_store == STATE_STORE_DISK && offset == 0 && !worker.import_stream {
                let b = &worker.scratch[APP_SNAPSHOT_HDR..payload_len];
                let marker = b.len() >= 8
                    && u32::from_le_bytes([b[0], b[1], b[2], b[3]]) == SNAP_MAGIC
                    && u16::from_le_bytes([b[6], b[7]]) & SNAP_FLAG_DISK_RESIDENT != 0;
                if !marker {
                    worker.import_stream = true;
                    worker.import_len = 0;
                }
            }
            if worker.import_stream {
                if body_len > SNAPSHOT_BODY_MAX {
                    import_stream_fail(worker);
                    return;
                }
                worker.import_buf[..body_len]
                    .copy_from_slice(&worker.scratch[APP_SNAPSHOT_HDR..payload_len]);
                worker.import_pend_len = body_len;
                worker.import_pend_off = offset as u64;
                worker.import_pend_done = done;
                if body_len == 0 && !done {
                    worker.import_pend_len = 0;
                    return;
                }
                // An empty last chunk still ends the stream.
                if body_len == 0 {
                    worker.import_pend_len = usize::MAX;
                }
                install_pending(worker, sys);
                return;
            }
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
                // (identity-checked) instead of destroying it — but only
                // if the manifest recovery served covers that position.
                // The log behind the marker is compacted, so a store
                // recovered at an older generation (recovery serves the
                // other manifest slot when the newer one's runs did not
                // survive) cannot be brought forward by replay: adopting
                // it would label it at the marker's index and skip the
                // writes between. A node whose local store doesn't match
                // fails CLOSED: applied stays 0 and the store is
                // untouched — it must catch up from the leader, not
                // fabricate state.
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
                    let marker_index = le_u64(&worker.import_buf[8..16]);
                    let adopted = worker.disk_phase == DISK_PHASE_SERVING
                        && hdr_gen == worker.disk_state.range_generation
                        && worker.disk_state.generation > 0
                        && marker_index <= worker.disk_state.durable_applied_index;
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
                        let r = store.install(
                            req,
                            SnapshotChunk {
                                offset: 0,
                                data: &worker.import_buf[..total],
                                last: true,
                            },
                        );
                        if r == Err(StoreError::Backpressure) && store.install_active() {
                            // One-shot: nothing asks this chunk again, so an
                            // install the device made wait part-way is
                            // abandoned — EMPTY, as every failed install
                            // leaves the store — and the snapshot is asked
                            // for again.
                            store.abort_fences();
                        }
                        r
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
                    let body = &worker.import_buf[..total];
                    let ok = if worker.import_span_merge {
                        worker.store.snapshot_merge(body)
                    } else {
                        worker.store.snapshot_decode(body)
                    };
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
                worker.import_span_merge = false;
                if installed
                    && worker.state_store != STATE_STORE_DISK
                    && u16::from_le_bytes([worker.import_buf[6], worker.import_buf[7]])
                        & SNAPSHOT_FLAG_SPAN_MORE
                        != 0
                {
                    // One batch of a span copy: the next adds to it, and
                    // only the copy's last batch completes the install.
                    worker.import_span_merge = true;
                    worker.import_len = 0;
                    return;
                }
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
                    ack_install(worker, worker.applied_index);
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

/// Hand the pending portable chunk to the store's REPLACE install. False
/// while the store refuses it for now (`Backpressure`: a flush or a
/// compaction still running) — it stays pending and nothing more is read.
/// On the last chunk the install is the RESET's position, as a marker
/// adopted is; any failure leaves the store EMPTY and claims nothing.
unsafe fn install_pending(worker: &mut WorkerState, sys: &SyscallTable) -> bool {
    let len = if worker.import_pend_len == usize::MAX {
        0
    } else {
        worker.import_pend_len
    };
    let off = worker.import_pend_off;
    let last = worker.import_pend_done;
    if off == 0 {
        // Every manifest the install's flushes publish carries the
        // snapshot's position, not the store's before the RESET.
        worker.disk_state.pending_applied_index = worker.applied_index;
        worker.disk_state.pending_applied_term = worker.applied_term;
    }
    let req = SnapshotRequest {
        applied_index: worker.applied_index,
        range_generation: worker.disk_state.range_generation,
    };
    let outcome = {
        let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        store.install(
            req,
            SnapshotChunk {
                offset: off,
                data: &worker.import_buf[..len],
                last,
            },
        )
    };
    match outcome {
        Err(StoreError::Backpressure) => false,
        Ok(Progress::InProgress { .. }) => {
            worker.import_pend_len = 0;
            // A stream that lands chunk after chunk is progress, not a
            // fence starving the store (the watchdog's case).
            worker.disk_fence_steps = 0;
            true
        }
        Ok(Progress::Done) => {
            worker.import_pend_len = 0;
            worker.import_stream = false;
            worker.importing = false;
            worker.disk_fence_steps = 0;
            if worker.disk_state.committed_clock_ms > worker.committed_now_ms {
                worker.committed_now_ms = worker.disk_state.committed_clock_ms;
            }
            // The state is the RESET's position; the log resumes after it,
            // each command recomputing the revision it had on the leader.
            worker.disk_revision = worker.disk_state.highest_revision;
            worker.disk_replay_floor = worker.disk_state.highest_revision;
            worker.rev_at_applied = worker.disk_revision;
            worker.m_xfer_installs = worker.m_xfer_installs.wrapping_add(1);
            resume_expiry_service(worker);
            #[cfg(feature = "store_provider")]
            provider_resync(worker);
            dev_log(
                sys,
                2,
                b"[kvw] portable snapshot installed".as_ptr(),
                b"[kvw] portable snapshot installed".len(),
            );
            ack_install(worker, worker.applied_index);
            true
        }
        Err(_) => {
            import_stream_fail(worker);
            true
        }
    }
}

/// A portable install that cannot complete: the store is EMPTY (REPLACE),
/// so nothing of the snapshot's position is claimed; the sender's next
/// RESET starts over.
unsafe fn import_stream_fail(worker: &mut WorkerState) {
    {
        let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        store.abort_fences();
    }
    worker.import_stream = false;
    worker.importing = false;
    worker.import_pend_len = 0;
    worker.applied_term = 0;
    worker.applied_index = 0;
    if !worker.syscalls.is_null() {
        let sys = &*worker.syscalls;
        dev_log(
            sys,
            1,
            b"[kvw] portable snapshot refused".as_ptr(),
            b"[kvw] portable snapshot refused".len(),
        );
    }
}

/// How long an export may go without the channel taking a byte before it
/// is abandoned (its peer is gone; the hold it keeps on compaction must
/// not outlive it).
const XFER_IDLE_MS: u64 = 30_000;
/// Merged records one export step visits.
const XFER_STEP_RECORDS: u32 = 64;

/// Say the store holds the state of the install at `index`: what lets
/// durability tell raft the install landed, and an elastic split's
/// cutover gate on the span arriving.
unsafe fn ack_install(worker: &mut WorkerState, index: u64) {
    if worker.install_ack_out < 0 || worker.syscalls.is_null() {
        return;
    }
    let mut ack = [0u8; 3 + 10];
    ack[0] = MSG_APP_SNAPSHOT_INSTALLED;
    ack[1] = 10;
    ack[3..5].copy_from_slice(&worker.partition_id.to_le_bytes());
    ack[5..13].copy_from_slice(&index.to_le_bytes());
    ((*worker.syscalls).channel_write)(worker.install_ack_out, ack.as_mut_ptr(), 13);
}

/// Abandon the export under way, if any.
unsafe fn export_abort(worker: &mut WorkerState) {
    if worker.xfer_active && worker.xfer_begun && worker.state_store == STATE_STORE_DISK {
        let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
        store.transfer_abort();
    }
    worker.xfer_active = false;
    worker.xfer_begun = false;
    worker.xfer_done = false;
    worker.xfer_len = 0;
    worker.xfer_pos = 0;
}

/// One step of a portable export: begin it once the worker stands at an
/// entry boundary, then hand the store's stream out in
/// `XFER_CHUNK_BODY`-sized `MSG_APP_SNAPSHOT_EXPORT_CHUNK`s for as long as
/// the channel takes them, a bounded walk at a time. Writes go on
/// throughout: the copy is read as of the position it is labelled with.
unsafe fn export_step(worker: &mut WorkerState) {
    if !worker.xfer_active || worker.syscalls.is_null() {
        return;
    }
    let sys = &*worker.syscalls;
    let now = dev_millis(sys);
    if now.saturating_sub(worker.xfer_since_ms) > XFER_IDLE_MS {
        export_abort(worker);
        dev_log(
            sys,
            1,
            b"[kvw] export abandoned: not taken".as_ptr(),
            b"[kvw] export abandoned: not taken".len(),
        );
        return;
    }
    if !worker.xfer_begun {
        // Labelled where the worker stands; begun only while nothing past
        // that marker is applied, so the copy and its label agree.
        let rev_now = if worker.state_store == STATE_STORE_DISK {
            worker.disk_revision
        } else {
            worker.store.revision
        };
        if worker.applied_index == 0 || rev_now != worker.rev_at_applied {
            return;
        }
        worker.xfer_term = worker.applied_term;
        worker.xfer_index = worker.applied_index;
        worker.xfer_off = 0;
        worker.xfer_len = 0;
        worker.xfer_pos = 0;
        worker.xfer_done = false;
        if worker.state_store == STATE_STORE_DISK {
            let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
            if store
                .transfer_begin(worker.applied_index, worker.rev_at_applied)
                .is_err()
            {
                return;
            }
        } else {
            // The memory store's copy is one encode (bounded, as its own
            // snapshot body is).
            match worker
                .store
                .snapshot_encode(&mut worker.xfer_buf, worker.committed_now_ms)
            {
                Some(n) => {
                    worker.xfer_len = n;
                    worker.xfer_done = true;
                }
                None => {
                    export_abort(worker);
                    dev_log(
                        sys,
                        1,
                        b"[kvw] export over budget".as_ptr(),
                        b"[kvw] export over budget".len(),
                    );
                    return;
                }
            }
        }
        worker.xfer_begun = true;
        worker.m_xfer_exports = worker.m_xfer_exports.wrapping_add(1);
    }
    for _ in 0..8 {
        if worker.xfer_pos < worker.xfer_len
            || (worker.xfer_done && worker.xfer_pos == worker.xfer_len)
        {
            let chunk = (worker.xfer_len - worker.xfer_pos).min(XFER_CHUNK_BODY);
            let last = worker.xfer_done && worker.xfer_pos + chunk == worker.xfer_len;
            let payload = APP_SNAPSHOT_HDR + chunk;
            let mut frame = [0u8; 3 + APP_SNAPSHOT_HDR + XFER_CHUNK_BODY];
            frame[0] = MSG_APP_SNAPSHOT_EXPORT_CHUNK;
            frame[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
            frame[3..11].copy_from_slice(&worker.xfer_term.to_le_bytes());
            frame[11..19].copy_from_slice(&worker.xfer_index.to_le_bytes());
            frame[19..27].copy_from_slice(&worker.xfer_off.to_le_bytes());
            frame[27] = u8::from(last);
            frame[28..31].copy_from_slice(&worker.xfer_id.to_le_bytes()[..3]);
            let p = worker.xfer_pos;
            frame[31..31 + chunk].copy_from_slice(&worker.xfer_buf[p..p + chunk]);
            if (sys.channel_write)(worker.snapshot_export_out, frame.as_mut_ptr(), 3 + payload)
                != (3 + payload) as i32
            {
                return; // the channel is full: the same chunk next step
            }
            worker.xfer_since_ms = now;
            worker.xfer_pos += chunk;
            worker.xfer_off += chunk as u64;
            if last {
                worker.xfer_active = false;
                worker.xfer_begun = false;
                return;
            }
            continue;
        }
        // Refill from the store's stream.
        worker.xfer_pos = 0;
        worker.xfer_len = 0;
        let res = {
            let mut store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
            store.transfer_step(&mut worker.xfer_buf, XFER_STEP_RECORDS)
        };
        match res {
            Ok((n, Progress::Done)) => {
                worker.xfer_len = n;
                worker.xfer_done = true;
            }
            Ok((n, Progress::InProgress { .. })) => {
                worker.xfer_len = n;
                if n == 0 {
                    return; // the counting pass: a bounded walk per step
                }
            }
            Err(StoreError::Backpressure) => return,
            Err(_) => {
                export_abort(worker);
                dev_log(
                    sys,
                    1,
                    b"[kvw] export failed".as_ptr(),
                    b"[kvw] export failed".len(),
                );
                return;
            }
        }
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

/// Elastic split: stream ONLY the keys of the requested span as a
/// `MSG_APP_SNAPSHOT_CHUNK` sequence — the span copy to a
/// demand-provisioned partition. Both providers export: the memory store
/// encodes its records directly, the disk provider merge-scans the span
/// and streams the same portable body (current state; version history
/// stays behind, as the target's revision space is independent).
///
/// The copy is a RESET, then the span in key-ordered batches of at most
/// `SNAPSHOT_BODY_MAX` bytes, each a complete span body sent as its own
/// chunk sequence, every one but the last flagged
/// `SNAPSHOT_FLAG_SPAN_MORE`. A batch is encoded only once the previous one
/// has been taken, so a span of any size moves at the pace the channel
/// drains, and writes go on between batches: a key written behind the
/// cursor is caught by the split's fenced re-copy, which the router holds
/// the span's writes for.
///
/// Unlike [`emit_snapshot_chunks`] this is a COPY to ANOTHER partition, not
/// a snapshot of THIS worker's own state, so it deliberately touches none of
/// this worker's GC-snapshot bookkeeping. An empty span still emits one
/// header-only body, so the target's install path always runs.
unsafe fn span_export_step(worker: &mut WorkerState) {
    if !worker.span_active || worker.syscalls.is_null() {
        return;
    }
    let sys = &*worker.syscalls;
    let now = dev_millis(sys);
    if now.saturating_sub(worker.span_since_ms) > XFER_IDLE_MS {
        worker.span_active = false;
        let m = b"[kvw] span copy abandoned: not taken";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return;
    }
    // Labelled at (0, 0): this is a COPY of another range's keys, not a
    // raft position the target should adopt — the target keeps its own
    // apply position and simply gains the keys.
    let (term, index) = (0u64, 0u64);
    if !worker.span_reset_sent {
        // The target is a demand-provisioned (empty) partition: the RESET
        // arms its CHUNK path (`importing`) and discards any prior state.
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
            return; // the channel is full: the RESET next step
        }
        worker.span_reset_sent = true;
        worker.span_since_ms = now;
    }
    for _ in 0..SPAN_FRAMES_PER_STEP {
        if worker.span_len == 0 {
            let mut next = [0u8; MAX_KEY_LEN];
            let cl = worker.span_cursor_len;
            let el = worker.span_end_len;
            let encoded = if worker.state_store == STATE_STORE_DISK {
                let store = DiskStore::new(&mut worker.disk_state, &mut worker.fs_storage);
                let mut mat =
                    DiskMaterializer::new(store, worker.disk_revision, worker.disk_replay_floor);
                mat.snapshot_encode_span_batch(
                    &mut worker.span_buf,
                    worker.committed_now_ms,
                    &worker.span_cursor[..cl],
                    &worker.span_end[..el],
                    &mut next,
                )
            } else {
                worker.store.snapshot_encode_span_batch(
                    &mut worker.span_buf,
                    worker.committed_now_ms,
                    &worker.span_cursor[..cl],
                    &worker.span_end[..el],
                    &mut next,
                )
            };
            let Some((len, more)) = encoded else {
                // Fail closed: the target holds an armed, incomplete
                // install it never acknowledges.
                worker.m_snapshot_refusals = worker.m_snapshot_refusals.wrapping_add(1);
                worker.span_active = false;
                let m = b"[kvw] span copy refused";
                dev_log(sys, 1, m.as_ptr(), m.len());
                return;
            };
            if let Some(nl) = more {
                worker.span_cursor[..nl].copy_from_slice(&next[..nl]);
                worker.span_cursor_len = nl;
            }
            worker.span_more = more.is_some();
            worker.span_len = len;
            worker.span_sent = 0;
        }
        let sent = worker.span_sent;
        let chunk = (worker.span_len - sent).min(SNAPSHOT_CHUNK_MAX);
        let done = sent + chunk == worker.span_len;
        let payload = APP_SNAPSHOT_HDR + chunk;
        worker.scratch[0] = MSG_APP_SNAPSHOT_CHUNK;
        worker.scratch[1] = (payload & 0xFF) as u8;
        worker.scratch[2] = ((payload >> 8) & 0xFF) as u8;
        worker.scratch[3..11].copy_from_slice(&term.to_le_bytes());
        worker.scratch[11..19].copy_from_slice(&index.to_le_bytes());
        worker.scratch[19..27].copy_from_slice(&(sent as u64).to_le_bytes());
        worker.scratch[27] = u8::from(done);
        worker.scratch[28..31].copy_from_slice(&[0, 0, 0]);
        worker.scratch[31..31 + chunk].copy_from_slice(&worker.span_buf[sent..sent + chunk]);
        let n = (sys.channel_write)(
            worker.snapshot_export_out,
            worker.scratch.as_mut_ptr(),
            3 + payload,
        );
        if n != (3 + payload) as i32 {
            return; // the channel is full: the same chunk next step
        }
        worker.m_snapshot_chunks = worker.m_snapshot_chunks.wrapping_add(1);
        worker.span_since_ms = now;
        worker.span_sent = sent + chunk;
        if done {
            worker.span_len = 0;
            if !worker.span_more {
                worker.span_active = false;
                return;
            }
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
