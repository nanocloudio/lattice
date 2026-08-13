//! Ordered disk state-store provider — Phase 1 of the RFC database
//! foundation (§9, §9.2, §9.3, §9.4, §30).
//!
//! Log-structured, single level, v1: one bounded sorted memtable plus a
//! bounded set of immutable sorted run files named by a versioned,
//! checksummed, generation-numbered manifest. All I/O goes through the
//! `RunStorage` trait (`run_storage.rs`) so the identical provider
//! logic compiles into the no_std PIC module (Fluxor FS opcodes) and
//! into host tests (`std::fs`). Clustor's per-partition WAL remains the
//! sole authoritative logical WAL (§9.3): every file written here is a
//! discardable materialization, rebuilt from an authorized snapshot
//! plus committed-entry replay.
//!
//! Every on-disk format below is versioned; changing any layout is a
//! persistent-format break and requires a version bump plus an explicit
//! migration. The golden expectations live in
//! `tests/contract_disk_store.rs` — never "update" them to match a
//! layout change.
//!
//! ## Committed batch format v1 (`apply` input, little-endian)
//!
//! ```text
//! header  [magic:u32 = "LBAT"][format:u16 = 1][op_count:u16]
//! per op  [key_len:u16][value_len:u32][encoded_internal_key][value]
//! ```
//!
//! Each op is a materialization Put of one encoded internal key
//! (`internal_key.rs` v1 — the key embeds the MVCC timestamp and
//! `ValueKind`). A tombstone is a Put whose key has
//! `ValueKind::PointTombstone` and an empty value, or exactly an
//! 8-byte MVCC commit timestamp; any other non-empty
//! tombstone value is `Malformed`. Unknown magic/format or any
//! truncation is `Malformed` and applies nothing (validate-then-apply,
//! never half a batch).
//!
//! ## Run file format (little-endian)
//!
//! ```text
//! header  [magic:u32 = "LRUN"][format:u16 = 1][flags:u16 = 0]
//!         [key_format:u16][contract:u16][rsvd:u32]                 (16 B)
//! block*  [payload_len:u32][record_count:u32][payload][crc32:u32]
//! footer  [magic:u32 = "LRNF"][record_count:u32][block_count:u32]
//!         [smallest_key_off:u64][largest_key_off:u64]
//!         [bloom:1024][file_crc:u32]                             (1056 B)
//! ```
//!
//! The footer carries a 1 KiB per-run Bloom filter over record key
//! PREFIXES. Without it a point lookup scanned a
//! run's blocks sequentially — `Merge::init` opens each run and reads
//! forward to the start bound — and on FAT32/NVMe the rig measured ONE
//! such command at 11-15 ms against a single run, past the burst
//! ceiling and therefore unbounded by any per-step drain budget. See
//! `BLOOM_BITS` for the sizing and false-positive arithmetic. The
//! filter is persisted rather than rebuilt at recovery because
//! rebuilding means a full record scan of every run on the boot path;
//! it carries no checksum of its own because it lies inside the region
//! `file_crc` already covers, and `verify_run` validates the whole file
//! before the filter is ever consulted.
//!
//! Block payloads hold whole records (`[key_len:u16][value_len:u32]
//! [key][value]`) in ascending encoded-key order; a block closes once
//! its payload would exceed `BLOCK_TARGET` (a single oversized record
//! may exceed the target on its own — payload is bounded by
//! `BLOCK_MAX_PAYLOAD`). The per-block `crc32` covers the payload
//! bytes. `smallest_key_off` / `largest_key_off` are the file offsets
//! of the blocks holding the first and last record. `file_crc` is the
//! CRC32 of every preceding byte of the file (header, blocks, and the
//! footer up to the crc field).
//!
//! ## Manifest format (little-endian; A/B slot pair)
//!
//! ```text
//! header  [magic:u32 = "LMAN"][format:u16 = 1][key_format:u16]
//!         [contract:u16][flags:u16 = 0][generation:u32]
//!         [run_count:u32][compaction_floor:u64]
//!         [applied_index:u64][applied_term:u64]
//!         [highest_revision:u64]                                   (52 B)
//! entry*  [run_id:u32][record_count:u32][file_len:u64][digest:u32] (20 B)
//! trailer [crc32:u32]  — over every preceding byte
//! padding zeroes to MANIFEST_SLOT_LEN                     (fixed size)
//! ```
//!
//! Entries are newest-first; `digest` is the run's `file_crc`.
//!
//! **Why a fixed A/B pair.** Giving every generation its own file, with
//! the FILENAME carrying the generation, put a file CREATE on the
//! publish path, and a FAT32 create was measured at **159 ms** on
//! the Pi 5 rig — a linear directory scan, a dirent patch, a cluster
//! allocation writing its FAT entry into every FAT copy as a
//! read-modify-write, and a first-cluster zero-fill, every one a
//! synchronous spin-polled single-sector write. That single allocation
//! blew the module's step deadline and got it terminated on its first
//! publish. Two fixed, preallocated slots replace it:
//! `MANIFEST_SLOT_A`/`_B`, each exactly `MANIFEST_SLOT_LEN` bytes,
//! written IN PLACE and alternated. The generation lives in the body, the slot is padded to a constant length so a write
//! never extends the file, and the publish path performs no create, no
//! unlink and no directory enumeration at all.
//!
//! **Atomicity (this replaces "older manifests are deleted LAST").** A
//! publish only ever writes the INACTIVE slot — the one that is not
//! currently in force — so the manifest being served is never the
//! manifest being overwritten, and a torn or interrupted write can
//! damage only the half nobody is reading. Both slots are independently
//! checksummed over their declared extent, and each carries its own
//! generation in its body, so the pair is self-describing: recovery
//! reads both, discards any that fail magic/version/CRC (an absent,
//! zero-filled, or half-written slot simply fails), and adopts the
//! valid one with the HIGHER internal generation. There is therefore no
//! instant at which zero valid manifests exist — the old one stays
//! intact and authoritative until the new one is proven durable — which
//! is exactly the guarantee delete-last used to provide, obtained now
//! without deleting anything. The new manifest is written only after
//! the run it names is proven durable, so whichever slot recovery
//! picks, every run it references is on media.
//!
//! Recovery (§9.4 steps 1-4, `recover()`): the valid slot with the
//! highest internal generation wins; runs it does not reference are
//! orphans and are deleted; a referenced-but-missing or corrupt run
//! means QUARANTINE — `StoreError::StorageFault`, never a guess.
//!
//! ## Snapshot stream format v1 (little-endian)
//!
//! ```text
//! header  [magic:u32 = "LSNP"][format:u16 = 1][flags:u16 = 0]
//!         [applied_index:u64][range_generation:u32][entry_count:u32]
//!         [highest_revision:u64][compaction_floor:u64]             (40 B)
//! entry*  [key_len:u16][value_len:u32][key][value]
//! ```
//!
//! Full-fidelity logical content (every surviving version, tombstones
//! included) in ascending encoded-key order. Per-chunk integrity is
//! owned by the Clustor snapshot channel; the install side re-validates
//! every record's bounds and key encoding and fails closed on any
//! inconsistency.
//!
//! ## Capacity arithmetic (all bounds are compile-time constants)
//!
//! ```text
//! MemEntry        = 2 + 4 + MAX_ENCODED_KEY + MAX_VALUE_LEN
//!                 = 6 + 544 + 4142            ≈ 4692 B (repr(C) pad)
//! memtable        = MEMTABLE_MAX_ENTRIES * MemEntry
//!                 = 512 * 4692                ≈ 2.40 MB
//! provider total  ≈ memtable + order (1 KB) + install staging (4.7 KB)
//!                 + run metadata              ≈ 2.5 MB
//! logical bound   = (MAX_RUNS + 1) * MEMTABLE_MAX_ENTRIES
//!                 = 17 * 512 = 8704 versions before compaction is due
//! ```
//!
//! The provider struct is ~2.4 MB: heap-allocate it zeroed (see
//! `tests/contract_disk_store.rs::fresh_state`) exactly like `KvStore`.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC module and host tests; each consumer uses a subset of the surface"
)]

#[path = "internal_key.rs"]
pub mod internal_key;
#[path = "run_storage.rs"]
pub mod run_storage;
#[path = "state_store.rs"]
pub mod state_store;

use core::cell::Cell;
use internal_key::{ValueKind, IDENTITY_PREFIX_LEN, VERSION_SUFFIX_LEN};
use run_storage::{FileKind, FsyncState, FsyncTicket, RunStorage, StorageError};
use state_store::{
    ApplyResult, KeySpan, KvStateStore, Progress, Revision, ScanProgress, SnapshotChunk,
    SnapshotRequest, StoreError,
};

// ── Capacities ────────────────────────────────────────────────────────

/// Maximum encoded internal key length the provider stores. Covers a
/// user key of `kv_store::MAX_KEY_LEN` (256) bytes at worst-case
/// escaping: `internal_key::encoded_max_len(256)` = 535, rounded up.
/// Longer keys are rejected `Malformed` — fail closed, never truncate.
pub const MAX_ENCODED_KEY: usize = 544;
/// Maximum value length: `kv_store::MAX_VALUE_LEN` (4096) plus the
/// engine's 54-byte record-metadata prefix (`kv_store::DISK_META_LEN`)
/// that the disk composition stores inside the value payload.
pub const MAX_VALUE_LEN: usize = 4096 + 54;
/// Memtable slots. 512 * ~4.6 KB ≈ 2.36 MB — see the module-doc
/// arithmetic.
pub const MEMTABLE_MAX_ENTRIES: usize = 512;
/// Advisory flush high-water mark (75%): `apply` starts reporting
/// `flush_wanted` here. The memtable still accepts batches until it is
/// FULL, at which point `apply` returns `Backpressure` (never drops).
pub const MEMTABLE_FLUSH_WATERMARK: usize = 384;
/// Bounded number of live run files. Flush past this is
/// `Backpressure`; `compact` merges all runs back into one.
pub const MAX_RUNS: usize = 16;
/// Smallest legal encoded internal key: identity prefix + terminator +
/// version suffix (empty user key).
pub const MIN_ENCODED_KEY: usize = IDENTITY_PREFIX_LEN + 2 + VERSION_SUFFIX_LEN;

// ── Batch format v1 ───────────────────────────────────────────────────

/// "LBAT" — committed materialization batch magic.
pub const BATCH_MAGIC: u32 = 0x4C42_4154;
pub const BATCH_FORMAT_VERSION: u16 = 1;
/// `[magic:4][format:2][op_count:2]`
pub const BATCH_HDR_LEN: usize = 8;

// ── Run file format ───────────────────────────────────────────────────

/// "LRUN" — run file magic.
pub const RUN_MAGIC: u32 = 0x4C52_554E;
pub const RUN_FORMAT_VERSION: u16 = 1;

// ── Per-run Bloom filter ──────────────────────────────────────────────
//
// WHY. A point lookup used to cost a sequential block scan of every
// run: `Merge::init` opens each run and reads forward until it reaches
// the start bound. On FAT32/NVMe each block read is a synchronous
// sector read plus a cluster-chain walk, and the rig measured a SINGLE
// such command at **11-15 ms** against just ONE run — past the burst
// ceiling, so no per-step drain budget could bound it.
//
// What makes that especially wasteful is the workload: every write
// calls `get_live` for `create_revision`/`version`/`prev_kv`, and in a
// fresh-key workload the answer is almost always "absent". The old code
// scanned an entire run to conclude a key was never in it. A Bloom
// filter answers exactly that question with ZERO I/O.
//
// SIZING. A run holds at most `MEMTABLE_MAX_ENTRIES` (512) records, and
// there are at most `MAX_RUNS` (16) of them. At m = 8192 bits per run
// that is 16 bits per key; with k = 6 probes the false-positive rate is
// (1 - e^(-k*n/m))^k = (1 - e^-0.375)^6 ~= **0.09%**. Optimal k for
// m/n = 16 would be 11, giving ~0.046%, but 6 probes is half the hash
// work for a rate already two orders of magnitude below the point where
// it would matter. Cost: 1 KiB per run, 16 KiB for a full run set —
// against a `DiskState` that is already ~2.4 MB.
//
// A false positive is SAFE BY CONSTRUCTION: it costs exactly the scan
// this code performed unconditionally before, and the scan is still the
// thing that decides the answer. The filter can only ever skip work,
// never change a result. A false NEGATIVE would be a correctness bug,
// and cannot occur: every key written into a run sets its bits before
// the run is published.
// ── Per-run sparse block index ────────────────────────────────────────
//
// WHY. With the Bloom filter absorbing the negative case and the memo
// collapsing three reads to one, the rig measured the remaining cost
// exactly: `n=1 us=15209 scan=1` — ONE run scan, ~15 ms, over the
// 12000 us declared deadline. A point lookup still walked the run's
// blocks from the start, ~12 sequential sector reads on FAT32, to reach
// a key it could have gone straight to.
//
// The index stores one entry per indexed block: that block's first key
// (truncated) plus the block's file offset. A lookup binary-searches it
// in memory and starts the scan at the right block.
//
// SIZING. `MEMTABLE_MAX_ENTRIES` (512) records and a `BLOCK_TARGET` of
// 4 KiB means ~13 blocks for a typical run, but the worst case is one
// oversized record per block — 512 blocks. A fixed 32-entry table
// covers the typical run one-entry-per-block; beyond that the index
// DECIMATES (keep every other entry, double the stride) so it stays
// bounded and simply becomes sparser: at 512 blocks the stride is 16,
// so a lookup scans at most 16 blocks instead of 512.
//
// TRUNCATION. Entries hold the first `INDEX_KEY_BYTES` of the encoded
// key. That is lossy, so the seek is deliberately CONSERVATIVE: it
// returns the last block whose truncated key is STRICTLY LESS than the
// query's, and the scan proceeds forward from there. Truncation is
// monotone (a <= b implies trunc(a) <= trunc(b)) and strict inequality
// on a common prefix implies strict inequality overall, so every block
// before that point provably holds only keys < the query. A tie in the
// truncated bytes therefore never decides anything — it just widens the
// scan by a block or two, which is a cost, never a wrong answer. The
// index can only ever narrow where the scan STARTS; the scan itself
// still decides the result.
/// Entries in one run's sparse index.
pub const SPARSE_INDEX_ENTRIES: usize = 32;
/// Bytes of the encoded key stored per entry. `IDENTITY_PREFIX_LEN` is
/// 12, so this keeps 20 bytes of escaped user key — enough to separate
/// realistic keys while keeping the footer at a few sectors.
pub const INDEX_KEY_BYTES: usize = 32;
/// `[block_off:u32][key_prefix:INDEX_KEY_BYTES]`
pub const INDEX_ENTRY_LEN: usize = 4 + INDEX_KEY_BYTES;
/// `[count:u32][stride:u32]` then the entries.
pub const RUN_INDEX_BYTES: usize = 8 + SPARSE_INDEX_ENTRIES * INDEX_ENTRY_LEN;

/// Number of entries currently held.
fn idx_count(buf: &[u8; RUN_INDEX_BYTES]) -> usize {
    rd_u32(buf, 0) as usize
}

/// Blocks per entry: 1 while the table has room, doubling on each
/// decimation.
fn idx_stride(buf: &[u8; RUN_INDEX_BYTES]) -> u32 {
    let s = rd_u32(buf, 4);
    if s == 0 {
        1
    } else {
        s
    }
}

fn idx_entry_off(buf: &[u8; RUN_INDEX_BYTES], i: usize) -> u64 {
    u64::from(rd_u32(buf, 8 + i * INDEX_ENTRY_LEN))
}

fn idx_entry_key(buf: &[u8; RUN_INDEX_BYTES], i: usize) -> &[u8] {
    let at = 8 + i * INDEX_ENTRY_LEN + 4;
    &buf[at..at + INDEX_KEY_BYTES]
}

/// Record block `block_ordinal` (0-based) at `off` with first key
/// `key`. Admits only every `stride`-th block; on overflow it keeps
/// every other entry and doubles the stride, so the table is bounded
/// and stays uniformly spread.
fn idx_push(buf: &mut [u8; RUN_INDEX_BYTES], block_ordinal: u32, off: u64, key: &[u8]) {
    let stride = idx_stride(buf);
    if !block_ordinal.is_multiple_of(stride) {
        return;
    }
    let mut count = idx_count(buf);
    if count == SPARSE_INDEX_ENTRIES {
        // Decimate in place: keep entries 0, 2, 4, ... and double the
        // stride. The retained entries stay correct — they are still
        // real block boundaries, just further apart.
        let mut w = 0usize;
        let mut r = 0usize;
        while r < count {
            if w != r {
                let (src, dst) = (8 + r * INDEX_ENTRY_LEN, 8 + w * INDEX_ENTRY_LEN);
                buf.copy_within(src..src + INDEX_ENTRY_LEN, dst);
            }
            w += 1;
            r += 2;
        }
        count = w;
        let new_stride = stride.saturating_mul(2);
        buf[4..8].copy_from_slice(&new_stride.to_le_bytes());
        buf[0..4].copy_from_slice(&(count as u32).to_le_bytes());
        if !block_ordinal.is_multiple_of(new_stride) {
            return;
        }
    }
    let at = 8 + count * INDEX_ENTRY_LEN;
    buf[at..at + 4].copy_from_slice(&(off as u32).to_le_bytes());
    let n = key.len().min(INDEX_KEY_BYTES);
    buf[at + 4..at + 4 + n].copy_from_slice(&key[..n]);
    for b in buf.iter_mut().skip(at + 4 + n).take(INDEX_KEY_BYTES - n) {
        *b = 0;
    }
    buf[0..4].copy_from_slice(&((count + 1) as u32).to_le_bytes());
}

/// File offset the scan for `key` may safely START at.
///
/// The LAST entry whose truncated key is strictly less than the query's
/// truncated key; `RUN_HDR_LEN` when there is none. See the truncation
/// note above for why strictness is what makes this safe.
fn idx_seek(buf: &[u8; RUN_INDEX_BYTES], key: &[u8]) -> u64 {
    let count = idx_count(buf);
    if count == 0 {
        return RUN_HDR_LEN as u64;
    }
    let mut probe = [0u8; INDEX_KEY_BYTES];
    let n = key.len().min(INDEX_KEY_BYTES);
    probe[..n].copy_from_slice(&key[..n]);
    // Largest i with entry_key[i] < probe.
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = (lo + hi) / 2;
        if idx_entry_key(buf, mid) < &probe[..] {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        RUN_HDR_LEN as u64
    } else {
        idx_entry_off(buf, lo - 1)
    }
}

/// Bits per run filter.
pub const BLOOM_BITS: usize = 8192;
/// Bytes per run filter (`BLOOM_BITS / 8`).
pub const BLOOM_BYTES: usize = BLOOM_BITS / 8;
/// Probes per key. See the rate arithmetic above.
pub const BLOOM_HASHES: u32 = 6;

/// FNV-1a 64 over the key PREFIX (identity + user key + terminator —
/// i.e. the encoded key minus its version suffix). The prefix is what a
/// point lookup probes with, so it is what the filter must be built
/// over: a filter keyed on full encoded keys would answer "absent" for
/// every lookup, because the query never carries a version suffix.
fn bloom_hash(prefix: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in prefix {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Kirsch-Mitzenmacher double hashing: `h1 + i*h2` over one 64-bit
/// hash. `| 1` keeps the stride odd so the probes cannot collapse onto
/// one bit when `h2` happens to share a factor with `BLOOM_BITS`.
#[inline]
fn bloom_bit(h: u64, i: u32) -> usize {
    let h1 = h as u32 as u64;
    let h2 = (h >> 32) | 1;
    (h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % BLOOM_BITS as u64) as usize
}

fn bloom_insert(filter: &mut [u8; BLOOM_BYTES], prefix: &[u8]) {
    let h = bloom_hash(prefix);
    for i in 0..BLOOM_HASHES {
        let bit = bloom_bit(h, i);
        filter[bit / 8] |= 1 << (bit % 8);
    }
}

/// `false` = DEFINITELY absent from this run (skip it entirely).
/// `true` = maybe present; the caller must scan to find out.
fn bloom_maybe_contains(filter: &[u8; BLOOM_BYTES], prefix: &[u8]) -> bool {
    let h = bloom_hash(prefix);
    for i in 0..BLOOM_HASHES {
        let bit = bloom_bit(h, i);
        if filter[bit / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
    }
    true
}
/// `[magic:4][format:2][flags:2][key_format:2][contract:2][rsvd:4]`
pub const RUN_HDR_LEN: usize = 16;
/// "LRNF" — run footer magic.
pub const RUN_FOOTER_MAGIC: u32 = 0x4C52_4E46;
/// `[magic:4][record_count:4][block_count:4][smallest:8][largest:8][crc:4]`
pub const RUN_FOOTER_LEN: usize = 32 + BLOOM_BYTES + RUN_INDEX_BYTES;
/// Offset of the Bloom filter inside the footer.
pub const RUN_FOOTER_BLOOM_OFF: usize = 28;
/// Offset of the sparse block index inside the footer.
pub const RUN_FOOTER_INDEX_OFF: usize = RUN_FOOTER_BLOOM_OFF + BLOOM_BYTES;
/// Fixed per-record prefix: `[key_len:u16][value_len:u32]`.
pub const RECORD_FIXED: usize = 6;
/// Worst-case single record.
pub const MAX_RECORD_LEN: usize = RECORD_FIXED + MAX_ENCODED_KEY + MAX_VALUE_LEN;
/// A block closes once its payload would exceed this.
pub const BLOCK_TARGET: usize = 4096;
/// `[payload_len:4][record_count:4]` before + `[crc:4]` after payload.
pub const BLOCK_OVERHEAD: usize = 12;
/// A single record may exceed `BLOCK_TARGET`; payload is bounded by
/// the larger of the two.
pub const BLOCK_MAX_PAYLOAD: usize = if BLOCK_TARGET > MAX_RECORD_LEN {
    BLOCK_TARGET
} else {
    MAX_RECORD_LEN
};

// ── Manifest format (A/B slots) ───────────────────────────────────────

/// "LMAN" — manifest magic.
pub const MANIFEST_MAGIC: u32 = 0x4C4D_414E;
pub const MANIFEST_FORMAT_VERSION: u16 = 1;

/// The manifest A/B pair. Two FIXED, PREALLOCATED files, alternated on
/// every publish. Nothing on the publish path creates, unlinks or
/// enumerates — a FAT32 create was measured at **159 ms** on the Pi 5
/// rig (linear directory scan, dirent patch, cluster allocation writing
/// its FAT entry into every FAT copy as a read-modify-write, and a
/// first-cluster zero-fill, every one a synchronous spin-polled
/// single-sector write). Publishing is now: write one fixed-size record
/// in place, fence it. That is all.
pub const MANIFEST_SLOT_A: u32 = 1;
pub const MANIFEST_SLOT_B: u32 = 2;
/// Every slot write is exactly this many bytes at offset 0, zero-padded.
/// Fixed size means the file never extends (so it never allocates after
/// preallocation) and a shorter manifest can never leave a readable
/// tail of the previous, longer one behind it.
pub const MANIFEST_SLOT_LEN: usize = MANIFEST_HDR_LEN + MAX_RUNS * MANIFEST_ENTRY_LEN + 4;
/// See the module-doc layout (44 bytes): the fixed fields through
/// `floor`, then `[applied_index:u64][applied_term:u64]` — the raft
/// position DURABLY covered by the named run set (the applied position
/// at the freeze of the newest flushed run). This is what lets the WAL
/// compact honestly: entries at or below it are reconstructible from
/// the runs alone.
pub const MANIFEST_HDR_LEN: usize = 52;
/// `[run_id:4][record_count:4][file_len:8][digest:4]`
pub const MANIFEST_ENTRY_LEN: usize = 20;
pub const MANIFEST_MAX_LEN: usize = MANIFEST_HDR_LEN + MAX_RUNS * MANIFEST_ENTRY_LEN + 4;
/// Recovery list buffer for manifest ids; steady state is 1 (plus a
/// possible torn newer generation).
pub const MAX_MANIFEST_FILES: usize = 8;
/// Recovery list buffer for run ids: live runs plus orphans.
pub const MAX_LISTED_RUNS: usize = 64;

// ── Snapshot stream format v1 ─────────────────────────────────────────

/// "LSNP" — snapshot stream magic.
pub const SNAP_MAGIC: u32 = 0x4C53_4E50;
pub const SNAP_FORMAT_VERSION: u16 = 1;
/// See the module-doc layout (40 bytes).
pub const SNAP_HDR_LEN: usize = 40;
/// Stream-header flag bit: the body is a DISK-RESIDENT MARKER, not a
/// record stream. `entry_count` is 0 and the state it certifies lives
/// in the local store's manifest-named runs. Install of a marker
/// ADOPTS the local store (identity-checked via `range_generation`)
/// instead of replacing it; a node without that store fails closed.
pub const SNAP_FLAG_DISK_RESIDENT: u16 = 0x0001;

/// Input records consumed per bounded `compact` step.
// TUNE (rig, 2026-08-16): 128 records/step ≈ 6-8 ms of synchronous
// device time EVERY worker step during a merge. The WAL (durability)
// shares the same blocking storage path; its appends/fsyncs queue
// behind those bursts, and with an unlucky alignment its own step
// crosses the 12/16 ms deadline and the KERNEL TERMINATES DURABILITY
// (observed as module-18 timeout kills at varying op counts). 32
// bounds a merge step to ~2 ms of device time, leaving the tick's
// deadline headroom to whoever shares the device; compactions take
// proportionally more steps, which is scheduling, not extra work.
pub const COMPACT_STEP_RECORDS: u64 = 32;

/// Memtable records written per bounded `flush_step` call.
///
/// Sized from measurement, not taste. A whole-memtable flush of 384
/// records costs **3 ms on host** (page-cache writes, ext4, no FAT32
/// directory work) — call it ~8 us/record. At 32 records a chunk is
/// ~0.25 ms on host, which leaves 8x headroom against the kernel's
/// DEFAULT step deadline of 2000 us; even a 6x penalty for the Pi 5's
/// FAT32-over-NVMe path keeps a chunk near 1.5 ms and still inside the
/// default. A full memtable therefore flushes in 12 chunks.
///
/// This bound has NO effect on the bytes written. The partial block
/// buffer persists in `FlushState` across steps, so block boundaries
/// are decided purely by `BLOCK_TARGET` and record sizes exactly as
/// they were when the flush was a single call — a run file is
/// byte-identical whether it was produced in one step or twelve. That
/// is what lets the golden vectors in `tests/contract_disk_store.rs`
/// stand unchanged across this refactor, and it is why the bound is
/// safe to re-tune later without a format break.
pub const FLUSH_STEP_RECORDS: u32 = 32;

// ── Flush tail phases ─────────────────────────────────────────────────
//
// The tail of a flush is a state machine, not a straight line, because
// both of its durability barriers are submit/poll fences that span
// scheduler steps. The phase order IS the recovery argument:
//
//   RECORDS      write the frozen memtable prefix, N records per step
//   RUN_SYNC     footer written, run fence SUBMITTED — poll until durable
//   MANIFEST_SYNC  run IS durable, manifest appended, its fence SUBMITTED
//   (done)       manifest durable -> delete older manifests, release
//
// Read down that list and every crash point is covered. Before
// MANIFEST_SYNC no manifest names the new run, so the previous manifest
// is still the truth and the run is an orphan. At MANIFEST_SYNC the run
// is already durable, so whichever manifest recovery picks references
// bytes that exist. Older manifests die only after the new one is
// durable, so there is never a window with no valid manifest.
/// Writing the frozen memtable prefix into the run.
const FLUSH_PHASE_RECORDS: u8 = 0;
/// Footer written; the run's durability fence is outstanding.
const FLUSH_PHASE_RUN_SYNC: u8 = 1;
/// Run durable; writing the manifest slot IN PLACE and submitting its
/// fence. There is no create phase any more — that phase measured
/// 159 ms on the rig and is the reason the A/B pair exists.
const FLUSH_PHASE_MAN_WRITE: u8 = 2;
/// Manifest written; its durability fence is outstanding.
const FLUSH_PHASE_MAN_SYNC: u8 = 3;

/// Merging input records into the output run.
const COMPACT_PHASE_MERGE: u8 = 0;
/// Merge input exhausted; the run FOOTER (bloom + index + CRC, one
/// sizeable append) is written in its OWN step — on FAT32 that append
/// plus the fsync submit measured ~140 ms together, far past the
/// module step deadline when run inside the final merge step.
const COMPACT_PHASE_FOOTER: u8 = 4;
/// Output run footer written; its durability fence is outstanding.
const COMPACT_PHASE_RUN_SYNC: u8 = 1;
/// Writing the manifest slot in place + submitting its fence.
const COMPACT_PHASE_MAN_WRITE: u8 = 2;
/// Manifest durability fence outstanding.
const COMPACT_PHASE_MAN_SYNC: u8 = 3;

// ── CRC32 (IEEE, poly 0xEDB88320) ─────────────────────────────────────
//
// No CRC implementation exists elsewhere in the repo (grepped before
// writing this one); table-driven, generated at compile time.

const fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

static CRC32_TABLE: [u32; 256] = crc32_table();

/// Rolling CRC32 state seed.
pub const CRC32_INIT: u32 = 0xFFFF_FFFF;

/// Fold `data` into a rolling CRC32 state (start from `CRC32_INIT`,
/// finish with `crc32_finish`).
pub fn crc32_update(mut state: u32, data: &[u8]) -> u32 {
    for &b in data {
        state = CRC32_TABLE[((state ^ b as u32) & 0xFF) as usize] ^ (state >> 8);
    }
    state
}

pub fn crc32_finish(state: u32) -> u32 {
    !state
}

/// One-shot CRC32 of `data`.
pub fn crc32(data: &[u8]) -> u32 {
    crc32_finish(crc32_update(CRC32_INIT, data))
}

// ── Little-endian slice readers ───────────────────────────────────────

fn rd_u16(src: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([src[at], src[at + 1]])
}

fn rd_u32(src: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([src[at], src[at + 1], src[at + 2], src[at + 3]])
}

fn rd_u64(src: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&src[at..at + 8]);
    u64::from_le_bytes(b)
}

/// Panic-free bounded copy for dynamic-length record staging. Copies
/// `min(dst, src)` bytes; every call site passes equal lengths by
/// construction — the clamp exists solely so the no_std PIC build
/// stays free of `copy_from_slice` panic paths (the module link
/// carries no panic runtime).
#[inline(always)]
fn copy_bytes(dst: &mut [u8], src: &[u8]) {
    let n = if dst.len() < src.len() {
        dst.len()
    } else {
        src.len()
    };
    let mut i = 0;
    while i < n {
        dst[i] = src[i];
        i += 1;
    }
}

/// Panic-free positioned copy: `dst[at..at + src.len()] = src`, with
/// out-of-range silently skipped (impossible by construction at every
/// call site — see `copy_bytes`).
#[inline(always)]
fn copy_at(dst: &mut [u8], at: usize, src: &[u8]) {
    let Some(end) = at.checked_add(src.len()) else {
        return;
    };
    if let Some(d) = dst.get_mut(at..end) {
        copy_bytes(d, src);
    }
}

// ── Encoded-key accessors ─────────────────────────────────────────────

/// MVCC timestamp embedded in a full encoded internal key (stored
/// bit-inverted big-endian; see `internal_key.rs`).
fn key_timestamp(key: &[u8]) -> u64 {
    let at = key.len() - VERSION_SUFFIX_LEN;
    let mut b = [0u8; 8];
    b.copy_from_slice(&key[at..at + 8]);
    !u64::from_be_bytes(b)
}

/// Raw `ValueKind` byte of a full encoded internal key.
fn key_kind_byte(key: &[u8]) -> u8 {
    key[key.len() - 1]
}

/// Normalize the contract's revision argument: `0` and `u64::MAX` both
/// mean "latest" (see `state_store.rs` docs).
fn normalize_revision(revision: Revision) -> u64 {
    if revision == 0 {
        u64::MAX
    } else {
        revision
    }
}

fn map_storage(_e: StorageError) -> StoreError {
    StoreError::StorageFault
}

/// Create `(kind, id)` for a *fresh* write, tolerating the debris of an
/// earlier attempt that failed part-way.
///
/// Every write sequence here (flush, compact, manifest publish) picks an
/// id/generation that is NOT yet referenced by durable state — publish is
/// always the last step, and `next_run_id` / `generation` only advance
/// once a publish has landed. So an existing file at that id is *always*
/// an unreferenced orphan from an attempt that faulted after `create()`
/// but before publish.
///
/// Without this, one transient backend failure mid-flush wedges the store
/// forever: the retry re-derives the same id, `create()` answers
/// `AlreadyExists`, and that maps to `StorageFault` — which the hosting
/// worker treats as a hard fault and quarantines on. That is the bare-metal
/// FAT32 wedge; on linux the backend never failed transiently, so the retry
/// path was never taken. Deleting the orphan first makes the retry clean.
fn create_fresh<S: RunStorage + ?Sized>(
    storage: &mut S,
    kind: FileKind,
    id: u32,
) -> Result<(), StoreError> {
    match storage.create(kind, id) {
        Ok(()) => Ok(()),
        Err(StorageError::AlreadyExists) => {
            match storage.delete(kind, id) {
                Ok(()) | Err(StorageError::NotFound) => {}
                Err(e) => return Err(map_storage(e)),
            }
            storage.create(kind, id).map_err(map_storage)
        }
        Err(e) => Err(map_storage(e)),
    }
}

// ── State structs (all POD; the zeroed form is a valid empty state) ──

/// One memtable slot: a full encoded internal key plus its value. The
/// slab is append-only between flushes; ordering lives in
/// `DiskState::order`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MemEntry {
    pub key_len: u16,
    pub value_len: u32,
    pub key: [u8; MAX_ENCODED_KEY],
    pub value: [u8; MAX_VALUE_LEN],
}

/// Metadata for one live run file (mirrors a manifest entry).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RunMeta {
    pub id: u32,
    pub record_count: u32,
    pub file_len: u64,
    pub digest: u32,
    pub _pad: u32,
}

impl RunMeta {
    pub const fn empty() -> Self {
        Self {
            id: 0,
            record_count: 0,
            file_len: 0,
            digest: 0,
            _pad: 0,
        }
    }
}

/// Multi-step snapshot capture bookkeeping.
#[repr(C)]
#[derive(Clone, Copy)]
struct CaptureState {
    active: bool,
    header_sent: bool,
    _pad: [u8; 2],
    range_generation: u32,
    applied_index: u64,
    /// Records emitted so far; also the resume-cursor value.
    next_ordinal: u64,
    total_entries: u64,
}

/// Multi-step snapshot install bookkeeping. `partial` stages one
/// header or record straddling chunk boundaries.
#[repr(C)]
#[derive(Clone, Copy)]
struct InstallState {
    active: bool,
    header_done: bool,
    _pad: [u8; 2],
    range_generation: u32,
    applied_index: u64,
    expected_offset: u64,
    entry_count: u32,
    entries_done: u32,
    snap_highest_revision: u64,
    snap_floor: u64,
    partial_len: u32,
    _pad2: u32,
    partial: [u8; MAX_RECORD_LEN],
}

/// Multi-step compaction bookkeeping. Each step writes only COMPLETE
/// blocks, so no partial block buffer needs to persist between steps.
#[repr(C)]
#[derive(Clone, Copy)]
struct CompactState {
    active: bool,
    /// `COMPACT_PHASE_*` — the same fenced tail as a flush, for the
    /// same reason (see `FLUSH_PHASE_*`).
    phase: u8,
    _pad: [u8; 2],
    ticket: FsyncTicket,
    pending_gen: u32,
    pending_slot: u32,
    pending_len: u64,
    pending_crc: u32,
    out_run_id: u32,
    floor: u64,
    /// Input records already decided in earlier steps (resume cursor).
    input_ordinal: u64,
    out_records: u32,
    out_blocks: u32,
    out_len: u64,
    crc_state: u32,
    _pad2: u32,
    smallest_off: u64,
    largest_off: u64,
    /// Incremental filter for the compaction output run.
    bloom: [u8; BLOOM_BYTES],
    /// Incremental sparse index for the compaction output run.
    index: [u8; RUN_INDEX_BYTES],
    blk_first_key: [u8; INDEX_KEY_BYTES],
    blk_has_first: bool,
    /// A step boundary saved exact per-run merge positions below —
    /// [`compact_step`] resumes by SEEKING, not by replaying the walk
    /// from record 0 (the replay made per-step cost grow linearly with
    /// the cursor — O(n²) total — which on FAT32-over-NVMe crossed the
    /// module step deadline around cursor ≈ 400 and got the worker
    /// TERMINATED mid-compaction, killing the graph under sustained
    /// write load).
    resume_valid: bool,
    /// Per-run resume block offsets (`u64::MAX` = run exhausted).
    run_block: [u64; MAX_RUNS],
    /// Per-run record index within the resume block.
    run_rec: [u32; MAX_RUNS],
    run_pos_count: u8,
    /// Carried dedup-walk context: the prefix run and its
    /// below-floor latch cross step boundaries with the positions —
    /// without them a resumed step would re-decide rule (a)/(b) from
    /// scratch and keep or drop the wrong versions.
    carry_kept_below_floor: bool,
    carry_prefix_len: u16,
    carry_prefix: [u8; MAX_ENCODED_KEY],
}

/// Multi-step flush bookkeeping.
///
/// Unlike `CompactState` this DOES carry the partial block buffer
/// across steps. Compaction can end every step on a block boundary
/// because nothing pins its output layout; a flush cannot, because the
/// run it produces must be byte-identical to the one the single-call
/// path produced before flushing was chunked (golden vectors). Holding
/// `block`/`fill`/`block_recs` here makes the step bound purely a
/// scheduling decision with no reach into the file format.
///
/// `slots` is the FROZEN sorted slot list captured at flush start:
/// `order[0..freeze_count]` as it stood then. The flush walks its own
/// snapshot so that concurrent `mem_insert` calls — which shift
/// `order` and append new slots — cannot make it skip or repeat a
/// record. Slots `0..freeze_count` are the flushed set; every write
/// admitted during the flush lands in a slot at or above it.
#[repr(C)]
struct FlushState {
    active: bool,
    /// `FLUSH_PHASE_*`. The tail of a flush is a state machine because
    /// its two durability barriers are submit/poll fences that span
    /// steps; see `flush_step`.
    phase: u8,
    _pad: [u8; 2],
    /// Outstanding fence ticket for the current phase.
    ticket: FsyncTicket,
    /// Manifest generation being published.
    pending_gen: u32,
    /// Which A/B slot that generation was written into. Adopted as the
    /// active slot only once its fence polls durable.
    pending_slot: u32,
    /// Run metadata decided at footer time, published only once the
    /// run's fence polls durable.
    pending_len: u64,
    pending_crc: u32,
    run_id: u32,
    /// `mem_count` at flush start: the size of the frozen prefix.
    freeze_count: u32,
    /// Applied position sampled from `pending_applied_*` at flush
    /// start. The frozen prefix contains every apply up to exactly
    /// this position (later applies land above the prefix), so it is
    /// what the manifest publish records as durably covered.
    applied_at_freeze: u64,
    term_at_freeze: u64,
    /// `highest_revision` at flush start. The frozen prefix plus the
    /// existing runs contain every version at or below it, so it is
    /// the read floor the manifest must restore after a truncated-WAL
    /// restart — without it the revision clock restarts at the replay
    /// tail and every run version sits invisibly "in the future".
    revision_at_freeze: u64,
    /// Records already written into the run.
    cursor: u32,
    out_blocks: u32,
    crc_state: u32,
    block_recs: u32,
    fill: u32,
    /// Bounded steps taken so far — the diagnostic the module logs.
    chunks: u32,
    out_len: u64,
    smallest_off: u64,
    largest_off: u64,
    slots: [u16; MEMTABLE_MAX_ENTRIES],
    /// Built INCREMENTALLY as records are written, one key at a time,
    /// so the filter costs six bit-sets per record instead of a second
    /// pass over the run at the end — no new unbounded step.
    bloom: [u8; BLOOM_BYTES],
    /// Sparse index, built one block at a time as blocks are closed.
    index: [u8; RUN_INDEX_BYTES],
    /// First key of the block currently being filled (truncated), and
    /// whether it has been captured yet.
    blk_first_key: [u8; INDEX_KEY_BYTES],
    blk_has_first: bool,
    block: [u8; BLOCK_MAX_PAYLOAD],
}

/// The provider's whole in-memory state (~2.4 MB; heap-allocate
/// zeroed, then `init()`). Zeroed IS the valid empty state — `init`
/// exists for explicit reuse.
#[repr(C)]
pub struct DiskState {
    pub mem_count: u32,
    pub run_count: u32,
    pub next_run_id: u32,
    /// Last published manifest generation (0 = none yet).
    pub generation: u32,
    /// Identity fence checked against `SnapshotRequest` (§17.1).
    pub range_generation: u32,
    /// Which manifest slot currently holds the authoritative manifest
    /// (`MANIFEST_SLOT_A`/`_B`). The next publish writes the OTHER one,
    /// so a torn write can never touch the manifest in force.
    pub manifest_slot: u32,
    /// Preallocation progress bitmask: bit 0 = slot A ensured, bit 1 =
    /// slot B. Driven one slot per `prepare_step` so the one-off
    /// creates never land in the same scheduler step.
    pub prepared: u8,
    _pad: u32,
    pub highest_revision: u64,
    pub compaction_floor: u64,
    /// Raft position covered by the PUBLISHED manifest's run set —
    /// what the last manifest recorded (recovered at boot, advanced on
    /// flush publish). Everything at or below it is reconstructible
    /// from the runs alone; the WAL tail beyond it is authoritative
    /// for the rest. 0 = nothing durably covered yet.
    pub durable_applied_index: u64,
    pub durable_applied_term: u64,
    /// Applied position stamped by the HOST (the module applying
    /// committed entries) after each apply — the freshest position the
    /// memtable content corresponds to. Sampled into `FlushState` at
    /// flush start; never persisted directly.
    pub pending_applied_index: u64,
    pub pending_applied_term: u64,
    /// Revision high-water recorded by the PUBLISHED manifest (the
    /// run-set's max version). Compaction republishes it unchanged;
    /// only a flush publish advances it.
    pub durable_highest_revision: u64,
    /// Run files retired by a completed compaction, awaiting physical
    /// deletion. Drained ONE per maintenance step ([`DiskStore::
    /// retire_step`]): a FAT32 delete frees the whole FAT chain
    /// synchronously (~35 ms measured on the rig), and doing three of
    /// them inside the manifest-adoption step blew the module step
    /// deadline and got the worker terminated. Correctness does not
    /// depend on the queue: entries lost to a crash are unreferenced
    /// orphans, which recovery already collects.
    pub retire_ids: [u32; MAX_RUNS],
    pub retire_count: u8,
    pub runs: [RunMeta; MAX_RUNS],
    /// Per-run Bloom filters, index-aligned with `runs[0..run_count]`
    /// (newest first). Shifted in lockstep with `runs` on publish, so a
    /// filter is never consulted for the wrong run.
    run_blooms: [[u8; BLOOM_BYTES]; MAX_RUNS],
    /// Per-run sparse block indexes, index-aligned with `runs` exactly
    /// as `run_blooms` is.
    run_index: [[u8; RUN_INDEX_BYTES]; MAX_RUNS],
    // ── Single-command read memo ──────────────────────────────────
    //
    // A SET of an EXISTING key costs THREE identical reads of the same
    // record: `apply_put` -> `get_live`, then `update_existing` ->
    // `read_latest`, then the expiry path -> `read_latest` again. While
    // everything lived in the memtable that was three binary searches
    // and invisible. Once a run exists each one is a full sequential
    // block scan of that run, and the rig measured a single such
    // command at **46 ms** — three scans at ~15 ms, which is exactly
    // three times the 11-15 ms a single scan cost before.
    //
    // The memo collapses them to one. It is deliberately the SMALLEST
    // possible cache: one entry, scoped to a single command, killed by
    // any write. `DiskMaterializer::new` clears it (one per command)
    // and `put_version` clears it on every successful apply, so a
    // read-after-write inside one command — a TXN touching the same key
    // twice — re-reads rather than serving a stale record. A stale hit
    // here would be silent corruption, so the invalidation is
    // deliberately blunt rather than clever.
    //
    // Storage lives HERE rather than in `DiskMaterializer` because the
    // materializer is stack-allocated per command in a PIC module whose
    // frame already carries two 8 KiB scratch buffers; ~4.4 KB more on
    // that stack is a risk, while in this arena-resident struct it is
    // noise against 2.4 MB.
    pub memo_valid: bool,
    pub memo_found: bool,
    memo_key_len: u16,
    memo_len: u32,
    memo_key: [u8; MAX_ENCODED_KEY],
    memo_val: [u8; MAX_VALUE_LEN],

    /// Diagnostic counters, monotonic: run scans actually OPENED (each
    /// one is block I/O) versus run scans the Bloom filter skipped
    /// outright. The hosting module reports the delta over a drain
    /// window so a slow command names its own cause — `scan` high is a
    /// positive lookup walking blocks, `scan=0` means the read path was
    /// not involved at all.
    pub scans_opened: Cell<u64>,
    pub scans_skipped: Cell<u64>,
    /// Run BLOCKS physically loaded. Distinguishes "the index sent us
    /// straight to the block" from "we walked the whole run".
    pub blocks_read: Cell<u64>,
    /// Sorted view over the slab: `order[0..mem_count]` are slot
    /// indices in ascending encoded-key order.
    pub order: [u16; MEMTABLE_MAX_ENTRIES],
    capture: CaptureState,
    compact: CompactState,
    install: InstallState,
    flush: FlushState,
    pub entries: [MemEntry; MEMTABLE_MAX_ENTRIES],
}

impl DiskState {
    /// Reset to the empty state, preserving nothing but the struct.
    pub fn init(&mut self) {
        self.mem_count = 0;
        self.run_count = 0;
        self.next_run_id = 0;
        self.generation = 0;
        self.range_generation = 0;
        self.manifest_slot = MANIFEST_SLOT_A;
        self.prepared = 0;
        self.highest_revision = 0;
        self.compaction_floor = 0;
        self.durable_applied_index = 0;
        self.durable_applied_term = 0;
        self.pending_applied_index = 0;
        self.pending_applied_term = 0;
        self.durable_highest_revision = 0;
        self.retire_ids = [0; MAX_RUNS];
        self.retire_count = 0;
        self.runs = [RunMeta::empty(); MAX_RUNS];
        self.capture.active = false;
        self.compact.active = false;
        self.install.active = false;
        self.flush.active = false;
        self.memo_valid = false;
    }

    /// Serve a latest-view read of `key` from the single-command memo,
    /// or `None` if it does not hold this key.
    pub fn memo_get(&self, key: &[u8], out: &mut [u8]) -> Option<Option<usize>> {
        if !self.memo_valid || self.memo_key_len as usize != key.len() {
            return None;
        }
        if self.memo_key[..key.len()] != *key {
            return None;
        }
        if !self.memo_found {
            return Some(None);
        }
        let n = self.memo_len as usize;
        if n > out.len() {
            return None;
        }
        out[..n].copy_from_slice(&self.memo_val[..n]);
        Some(Some(n))
    }

    /// Record a latest-view read result. `None` = the key is absent.
    pub fn memo_put(&mut self, key: &[u8], found: Option<&[u8]>) {
        if key.len() > MAX_ENCODED_KEY {
            self.memo_valid = false;
            return;
        }
        self.memo_key[..key.len()].copy_from_slice(key);
        self.memo_key_len = key.len() as u16;
        match found {
            Some(v) if v.len() <= MAX_VALUE_LEN => {
                self.memo_val[..v.len()].copy_from_slice(v);
                self.memo_len = v.len() as u32;
                self.memo_found = true;
            }
            Some(_) => {
                self.memo_valid = false;
                return;
            }
            None => {
                self.memo_len = 0;
                self.memo_found = false;
            }
        }
        self.memo_valid = true;
    }

    /// Kill the memo. Called on every write and once per command.
    pub fn memo_clear(&mut self) {
        self.memo_valid = false;
    }

    /// Diagnostic counter bumps. `Merge::init` takes `&DiskState`
    /// (reads must not require exclusive access), so the counters are
    /// `Cell`s — interior mutability, not a cast through a shared
    /// reference, which would be UB and which the optimiser is entitled
    /// to (and did) elide. `Cell<u64>` is `repr(transparent)`, so the
    /// zeroed `DiskState` stays a valid empty state.
    fn bump_opened(&self) {
        self.scans_opened
            .set(self.scans_opened.get().wrapping_add(1));
    }

    fn bump_skipped(&self) {
        self.scans_skipped
            .set(self.scans_skipped.get().wrapping_add(1));
    }

    fn add_blocks(&self, n: u64) {
        self.blocks_read.set(self.blocks_read.get().wrapping_add(n));
    }

    /// The slot the next publish writes into: never the active one.
    fn inactive_manifest_slot(&self) -> u32 {
        if self.manifest_slot == MANIFEST_SLOT_A {
            MANIFEST_SLOT_B
        } else {
            MANIFEST_SLOT_A
        }
    }

    fn mem_key(&self, pos: usize) -> &[u8] {
        let e = &self.entries[self.order[pos] as usize];
        &e.key[..e.key_len as usize]
    }

    fn mem_value(&self, pos: usize) -> &[u8] {
        let e = &self.entries[self.order[pos] as usize];
        &e.value[..e.value_len as usize]
    }

    /// First position in `order[0..mem_count]` whose key is `>= key`.
    fn mem_lower_bound(&self, key: &[u8]) -> usize {
        let mut lo = 0usize;
        let mut hi = self.mem_count as usize;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.mem_key(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Release the flushed prefix (slots `0..freeze`) from the
    /// memtable, keeping every record admitted DURING the flush.
    ///
    /// Slot allocation here is strictly "next slot == current count",
    /// so `order[0..mem_count]` is always a permutation of slots
    /// `0..mem_count`, and the frozen prefix is exactly slots
    /// `0..freeze`. Releasing it means sliding the surviving entries
    /// down by `freeze` and renumbering the sorted view to match.
    ///
    /// The memmove is bounded by construction: writes are only admitted
    /// up to `MEMTABLE_MAX_ENTRIES`, and a flush starts at
    /// `MEMTABLE_FLUSH_WATERMARK`, so at most `512 - 384 = 128` entries
    /// can survive a flush. That is a bounded ~590 KB in-memory copy at
    /// the tail of a step that has just done real I/O, not a new
    /// unbounded action.
    fn release_flushed(&mut self, freeze: u32) {
        let count = self.mem_count as usize;
        let freeze = (freeze as usize).min(count);
        if freeze == count {
            self.mem_count = 0;
            return;
        }
        let survivors = count - freeze;
        // Compact the slab: surviving slots `freeze..count` move to
        // `0..survivors`, so slot `s` becomes `s - freeze`.
        self.entries.copy_within(freeze..count, 0);
        // Rebuild the sorted view by keeping only surviving slots, in
        // their existing (already sorted) relative order.
        let mut w = 0usize;
        for r in 0..count {
            let slot = self.order[r] as usize;
            if slot >= freeze {
                self.order[w] = (slot - freeze) as u16;
                w += 1;
            }
        }
        self.mem_count = survivors as u32;
    }

    /// Insert or overwrite one (encoded key, value). Overwriting the
    /// exact same encoded key is deterministic replay, not an error.
    ///
    /// The explicit bound guards restate invariants every caller has
    /// already validated; they exist so the no_std PIC build stays
    /// free of panic paths (the module link carries no panic runtime).
    fn mem_insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        if key.len() > MAX_ENCODED_KEY || value.len() > MAX_VALUE_LEN {
            return Err(StoreError::Malformed);
        }
        let pos = self.mem_lower_bound(key);
        let count = self.mem_count as usize;
        if count > MEMTABLE_MAX_ENTRIES || pos > count {
            return Err(StoreError::StorageFault);
        }
        if pos < count && self.mem_key(pos) == key {
            // In-place overwrite of an already-present encoded key.
            // Reachable only from replay repair (`put_version` falls
            // through to an exact-encoded-key rewrite when the
            // materialization does NOT verify byte-for-byte).
            //
            // If that slot is inside a running flush's frozen prefix,
            // the run being written may already contain the OLD bytes,
            // and completing the flush would publish them and then drop
            // the corrected memtable entry — the repair would be
            // silently undone. So the flush loses: abandon it here.
            //
            // Abandoning is free and safe. Nothing has been published:
            // the partial run is an unreferenced orphan that `recover()`
            // collects, and the next attempt re-derives the same run id
            // (`next_run_id` only advances on publish) so `create_fresh`
            // truncates it. The memtable still holds every record.
            if self.flush.active && (self.order[pos] as u32) < self.flush.freeze_count {
                self.flush.active = false;
            }
            let e = &mut self.entries[self.order[pos] as usize];
            e.value_len = value.len() as u32;
            e.value[..value.len()].copy_from_slice(value);
            return Ok(());
        }
        if count >= MEMTABLE_MAX_ENTRIES {
            return Err(StoreError::Backpressure);
        }
        let slot = count as u16;
        {
            let e = &mut self.entries[count];
            e.key_len = key.len() as u16;
            e.value_len = value.len() as u32;
            e.key[..key.len()].copy_from_slice(key);
            e.value[..value.len()].copy_from_slice(value);
        }
        self.order.copy_within(pos..count, pos + 1);
        self.order[pos] = slot;
        self.mem_count += 1;
        Ok(())
    }
}

// ── Run-file sequential reader ────────────────────────────────────────

/// Cursor over one run file's records, loading and CRC-checking one
/// block at a time. Lives on the caller's stack (~4.7 KB each).
#[derive(Clone, Copy)]
struct RunScan {
    blocks_loaded: u32,
    run_id: u32,
    has_cur: bool,
    exhausted: bool,
    /// File offset of the block currently in `buf` (resume capture).
    cur_block_off: u64,
    /// Index within the current block of the CURRENT record;
    /// `u32::MAX` = block loaded, nothing parsed yet.
    cur_rec_idx: u32,
    next_block_off: u64,
    data_end: u64,
    payload_len: usize,
    pos: usize,
    key_off: usize,
    key_len: usize,
    val_off: usize,
    val_len: usize,
    buf: [u8; BLOCK_MAX_PAYLOAD],
}

impl RunScan {
    const fn idle() -> Self {
        Self {
            blocks_loaded: 0,
            run_id: 0,
            has_cur: false,
            exhausted: true,
            cur_block_off: 0,
            cur_rec_idx: u32::MAX,
            next_block_off: 0,
            data_end: 0,
            payload_len: 0,
            pos: 0,
            key_off: 0,
            key_len: 0,
            val_off: 0,
            val_len: 0,
            buf: [0u8; BLOCK_MAX_PAYLOAD],
        }
    }

    fn open(meta: &RunMeta) -> Self {
        Self::open_at(meta, RUN_HDR_LEN as u64)
    }

    /// Open the run positioned at a block boundary. `start_off` comes
    /// from the sparse index and is validated against the data region;
    /// anything out of range falls back to the head of the file, so a
    /// corrupt index costs a full scan and never a bad read.
    fn open_at(meta: &RunMeta, start_off: u64) -> Self {
        let mut rs = Self::idle();
        rs.run_id = meta.id;
        rs.data_end = meta.file_len.saturating_sub(RUN_FOOTER_LEN as u64);
        rs.next_block_off = if start_off >= RUN_HDR_LEN as u64 && start_off < rs.data_end {
            start_off
        } else {
            RUN_HDR_LEN as u64
        };
        rs.exhausted = meta.record_count == 0;
        rs.cur_block_off = rs.next_block_off;
        rs.cur_rec_idx = u32::MAX;
        rs
    }

    /// The scan's resume position: `(block_off, rec_idx)` of the
    /// CURRENT (unconsumed) record, `(u64::MAX, 0)` when exhausted, or
    /// `(next_block_off, u32::MAX)` when positioned before any record.
    /// [`Self::resume_to`] inverts this exactly.
    fn save_pos(&self) -> (u64, u32) {
        if self.exhausted {
            (u64::MAX, 0)
        } else if self.has_cur {
            (self.cur_block_off, self.cur_rec_idx)
        } else {
            (self.next_block_off, u32::MAX)
        }
    }

    /// Re-open the run standing on the exact record `save_pos`
    /// captured: load the saved block and advance `rec_idx + 1`
    /// records. The run file is immutable while a compaction holds the
    /// store's exclusion fence, so a saved offset that no longer lands
    /// on a block boundary is corruption, not drift — fail closed
    /// rather than silently walking from the head (which would emit
    /// wrong records).
    fn resume_to<S: RunStorage>(
        meta: &RunMeta,
        block_off: u64,
        rec_idx: u32,
        storage: &S,
    ) -> Result<Self, StoreError> {
        if block_off == u64::MAX {
            return Ok(Self::idle());
        }
        let data_end = meta.file_len.saturating_sub(RUN_FOOTER_LEN as u64);
        if block_off < RUN_HDR_LEN as u64 || block_off > data_end {
            return Err(StoreError::StorageFault);
        }
        let mut rs = Self::open_at(meta, block_off);
        if rec_idx != u32::MAX {
            let mut advanced = 0u64;
            while advanced <= u64::from(rec_idx) {
                if !rs.next(storage)? {
                    return Err(StoreError::StorageFault);
                }
                advanced += 1;
            }
        }
        Ok(rs)
    }

    /// Blocks physically loaded since the last take. The hosting module
    /// reports this per drain window: `scan=1 blocks=1` is the index
    /// doing its job, `scan=1 blocks=12` is a full walk.
    fn take_blocks(&mut self) -> u64 {
        let n = self.blocks_loaded;
        self.blocks_loaded = 0;
        u64::from(n)
    }

    fn cur_key(&self) -> &[u8] {
        &self.buf[self.key_off..self.key_off + self.key_len]
    }

    fn cur_value(&self) -> &[u8] {
        &self.buf[self.val_off..self.val_off + self.val_len]
    }

    /// Advance to the next record; `Ok(false)` = exhausted. Fails
    /// closed on any block CRC mismatch or malformed record framing.
    fn next<S: RunStorage>(&mut self, storage: &S) -> Result<bool, StoreError> {
        if self.exhausted {
            self.has_cur = false;
            return Ok(false);
        }
        if self.pos >= self.payload_len {
            // Load the next block.
            if self.next_block_off >= self.data_end {
                self.exhausted = true;
                self.has_cur = false;
                return Ok(false);
            }
            let mut hdr = [0u8; 8];
            read_exact(
                storage,
                FileKind::Run,
                self.run_id,
                self.next_block_off,
                &mut hdr,
            )?;
            self.blocks_loaded = self.blocks_loaded.saturating_add(1);
            let payload_len = rd_u32(&hdr, 0) as usize;
            let rec_count = rd_u32(&hdr, 4);
            if payload_len == 0 || payload_len > BLOCK_MAX_PAYLOAD || rec_count == 0 {
                return Err(StoreError::StorageFault);
            }
            let block_end = self.next_block_off + (BLOCK_OVERHEAD + payload_len) as u64;
            if block_end > self.data_end {
                return Err(StoreError::StorageFault);
            }
            read_exact(
                storage,
                FileKind::Run,
                self.run_id,
                self.next_block_off + 8,
                &mut self.buf[..payload_len],
            )?;
            let mut crc_b = [0u8; 4];
            read_exact(
                storage,
                FileKind::Run,
                self.run_id,
                self.next_block_off + 8 + payload_len as u64,
                &mut crc_b,
            )?;
            if crc32(&self.buf[..payload_len]) != rd_u32(&crc_b, 0) {
                return Err(StoreError::StorageFault);
            }
            self.payload_len = payload_len;
            self.pos = 0;
            self.cur_block_off = self.next_block_off;
            self.cur_rec_idx = u32::MAX;
            self.next_block_off = block_end;
        }
        // Parse one record at `pos`.
        if self.pos + RECORD_FIXED > self.payload_len {
            return Err(StoreError::StorageFault);
        }
        let klen = rd_u16(&self.buf, self.pos) as usize;
        let vlen = rd_u32(&self.buf, self.pos + 2) as usize;
        if !(MIN_ENCODED_KEY..=MAX_ENCODED_KEY).contains(&klen)
            || vlen > MAX_VALUE_LEN
            || self.pos + RECORD_FIXED + klen + vlen > self.payload_len
        {
            return Err(StoreError::StorageFault);
        }
        self.key_off = self.pos + RECORD_FIXED;
        self.key_len = klen;
        self.val_off = self.key_off + klen;
        self.val_len = vlen;
        self.pos = self.val_off + vlen;
        self.cur_rec_idx = self.cur_rec_idx.wrapping_add(1);
        self.has_cur = true;
        Ok(true)
    }
}

/// Fill `out` exactly from (kind, id) at `offset`; anything short is a
/// `StorageFault` (the caller always knows how many bytes must exist).
fn read_exact<S: RunStorage>(
    storage: &S,
    kind: FileKind,
    id: u32,
    offset: u64,
    out: &mut [u8],
) -> Result<(), StoreError> {
    let mut done = 0usize;
    while done < out.len() {
        let n = storage
            .read_at(kind, id, offset + done as u64, &mut out[done..])
            .map_err(map_storage)?;
        if n == 0 {
            return Err(StoreError::StorageFault);
        }
        done += n;
    }
    Ok(())
}

// ── Bounded k-way merge ───────────────────────────────────────────────

/// Merge cursor across the memtable and every live run, yielding
/// records in ascending encoded-key order with duplicates deduped
/// (memtable shadows runs; a newer run shadows an older run). Lives on
/// the caller's stack (~75 KB: MAX_RUNS block buffers).
struct Merge {
    include_mem: bool,
    mem_pos: usize,
    run_count: usize,
    runs: [RunScan; MAX_RUNS],
}

impl Merge {
    /// Position every source at its first record with key
    /// `>= start_bound` (runs have no index blocks in v1 — deliberate
    /// simplification over §9.2's "should" — so this is a sequential
    /// skip).
    fn init<S: RunStorage>(
        state: &DiskState,
        storage: &S,
        include_mem: bool,
        start_bound: &[u8],
    ) -> Result<Self, StoreError> {
        Self::init_filtered(state, storage, include_mem, start_bound, None)
    }

    /// `Merge` for an EXACT-KEY probe. `point_key` is the full lookup
    /// prefix; any run whose Bloom filter rules it out is skipped
    /// without a single read.
    ///
    /// Only legal for a point lookup. A range scan must NOT use this:
    /// its start bound is a lower bound, so a run whose filter lacks
    /// that exact key may still hold later keys the scan must return.
    /// Passing `Some(..)` there would silently drop records.
    fn init_point<S: RunStorage>(
        state: &DiskState,
        storage: &S,
        point_key: &[u8],
    ) -> Result<Self, StoreError> {
        Self::init_filtered(state, storage, true, point_key, Some(point_key))
    }

    fn init_filtered<S: RunStorage>(
        state: &DiskState,
        storage: &S,
        include_mem: bool,
        start_bound: &[u8],
        point_key: Option<&[u8]>,
    ) -> Result<Self, StoreError> {
        let mut m = Self {
            include_mem,
            mem_pos: if include_mem {
                state.mem_lower_bound(start_bound)
            } else {
                0
            },
            run_count: state.run_count as usize,
            runs: [RunScan::idle(); MAX_RUNS],
        };
        for (i, rs) in m.runs.iter_mut().take(m.run_count).enumerate() {
            if let Some(key) = point_key {
                if !bloom_maybe_contains(&state.run_blooms[i], key) {
                    state.bump_skipped();
                    // DEFINITELY not in this run. Leave the scan idle:
                    // it contributes no head, so the merge behaves
                    // exactly as if the run held nothing at or after the
                    // bound — which, for this key, it does.
                    *rs = RunScan::idle();
                    continue;
                }
            }
            state.bump_opened();
            *rs = match point_key {
                // Binary-search the sparse index and start at the right
                // block instead of walking from the head of the run.
                Some(key) => RunScan::open_at(&state.runs[i], idx_seek(&state.run_index[i], key)),
                None => RunScan::open(&state.runs[i]),
            };
            while rs.next(storage)? {
                if rs.cur_key() >= start_bound {
                    break;
                }
            }
            state.add_blocks(rs.take_blocks());
        }
        Ok(m)
    }

    /// Rebuild the compaction merge at the exact positions a paused
    /// [`compact_step`] saved: one `open_at` + at most one block's
    /// records advanced per run — O(runs) per step, independent of how
    /// far the compaction has progressed. Ties re-resolve identically
    /// (memtable excluded, runs newest-first by index), so the resumed
    /// merge continues the same sequence the paused one would have.
    fn resume_compact<S: RunStorage>(state: &DiskState, storage: &S) -> Result<Self, StoreError> {
        let mut m = Self {
            include_mem: false,
            mem_pos: 0,
            run_count: state.run_count as usize,
            runs: [RunScan::idle(); MAX_RUNS],
        };
        if state.compact.run_pos_count as usize != m.run_count {
            // The run set changed under a paused compaction — the
            // exclusion fence forbids that, so this is corruption.
            return Err(StoreError::StorageFault);
        }
        for i in 0..m.run_count {
            m.runs[i] = RunScan::resume_to(
                &state.runs[i],
                state.compact.run_block[i],
                state.compact.run_rec[i],
                storage,
            )?;
            state.add_blocks(m.runs[i].take_blocks());
        }
        Ok(m)
    }

    /// Copy the next merged record into `key_out`/`val_out` and return
    /// `(key_len, value_len)`, or `None` when every source is
    /// exhausted.
    fn next<S: RunStorage>(
        &mut self,
        state: &DiskState,
        storage: &S,
        key_out: &mut [u8],
        val_out: &mut [u8],
    ) -> Result<Option<(usize, usize)>, StoreError> {
        // Phase 1: pick the smallest head. Priority on ties: memtable,
        // then runs newest-first (runs[0] is newest).
        let mut winner: Option<usize> = None; // 0 = memtable, 1+i = run i
        let klen;
        {
            let mut best: Option<&[u8]> = None;
            if self.include_mem && self.mem_pos < state.mem_count as usize {
                best = Some(state.mem_key(self.mem_pos));
                winner = Some(0);
            }
            for (i, rs) in self.runs.iter().take(self.run_count).enumerate() {
                if rs.has_cur {
                    let k = rs.cur_key();
                    let better = match best {
                        None => true,
                        Some(b) => k < b,
                    };
                    if better {
                        best = Some(k);
                        winner = Some(1 + i);
                    }
                }
            }
            let Some(best_key) = best else {
                return Ok(None);
            };
            klen = best_key.len();
            key_out[..klen].copy_from_slice(best_key);
        }
        // Phase 2: copy the winner's value.
        let win = winner.unwrap_or(0);
        let vlen = if win == 0 {
            let v = state.mem_value(self.mem_pos);
            val_out[..v.len()].copy_from_slice(v);
            v.len()
        } else {
            let v = self.runs[win - 1].cur_value();
            val_out[..v.len()].copy_from_slice(v);
            v.len()
        };
        // Phase 3: advance every source whose head equals the winner
        // key (dedupe — the highest-priority copy was emitted).
        if self.include_mem
            && self.mem_pos < state.mem_count as usize
            && state.mem_key(self.mem_pos) == &key_out[..klen]
        {
            self.mem_pos += 1;
        }
        for rs in self.runs.iter_mut().take(self.run_count) {
            if rs.has_cur && rs.cur_key() == &key_out[..klen] {
                rs.next(storage)?;
                state.add_blocks(rs.take_blocks());
            }
        }
        Ok(Some((klen, vlen)))
    }
}

// ── Run-file writer helpers ───────────────────────────────────────────

/// Append the run-file header, seeding the rolling whole-file CRC.
fn write_run_header<S: RunStorage>(
    storage: &mut S,
    id: u32,
    crc_state: &mut u32,
    file_len: &mut u64,
) -> Result<(), StoreError> {
    let mut hdr = [0u8; RUN_HDR_LEN];
    hdr[0..4].copy_from_slice(&RUN_MAGIC.to_le_bytes());
    hdr[4..6].copy_from_slice(&RUN_FORMAT_VERSION.to_le_bytes());
    hdr[6..8].copy_from_slice(&0u16.to_le_bytes());
    hdr[8..10].copy_from_slice(&internal_key::KEY_FORMAT_VERSION.to_le_bytes());
    hdr[10..12].copy_from_slice(&state_store::STATE_STORE_CONTRACT_VERSION.to_le_bytes());
    hdr[12..16].copy_from_slice(&0u32.to_le_bytes());
    storage
        .append(FileKind::Run, id, &hdr)
        .map_err(map_storage)?;
    *crc_state = crc32_update(*crc_state, &hdr);
    *file_len += RUN_HDR_LEN as u64;
    Ok(())
}

/// Append one complete block; returns the block's file offset.
fn write_run_block<S: RunStorage>(
    storage: &mut S,
    id: u32,
    payload: &[u8],
    rec_count: u32,
    crc_state: &mut u32,
    file_len: &mut u64,
) -> Result<u64, StoreError> {
    let off = *file_len;
    let mut hdr = [0u8; 8];
    hdr[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    hdr[4..8].copy_from_slice(&rec_count.to_le_bytes());
    let crc = crc32(payload).to_le_bytes();
    storage
        .append(FileKind::Run, id, &hdr)
        .map_err(map_storage)?;
    storage
        .append(FileKind::Run, id, payload)
        .map_err(map_storage)?;
    storage
        .append(FileKind::Run, id, &crc)
        .map_err(map_storage)?;
    *crc_state = crc32_update(*crc_state, &hdr);
    *crc_state = crc32_update(*crc_state, payload);
    *crc_state = crc32_update(*crc_state, &crc);
    *file_len += (BLOCK_OVERHEAD + payload.len()) as u64;
    Ok(off)
}

/// Append the footer (closing the rolling CRC) and return the run's
/// final `(file_len, file_crc)`.
#[expect(
    clippy::too_many_arguments,
    reason = "plain writer helper: every argument is one footer field; bundling them into a struct would only rename the call site"
)]
fn write_run_footer<S: RunStorage>(
    storage: &mut S,
    id: u32,
    record_count: u32,
    block_count: u32,
    smallest_off: u64,
    largest_off: u64,
    bloom: &[u8; BLOOM_BYTES],
    index: &[u8; RUN_INDEX_BYTES],
    mut crc_state: u32,
    file_len: u64,
) -> Result<(u64, u32), StoreError> {
    let mut foot = [0u8; RUN_FOOTER_LEN];
    foot[0..4].copy_from_slice(&RUN_FOOTER_MAGIC.to_le_bytes());
    foot[4..8].copy_from_slice(&record_count.to_le_bytes());
    foot[8..12].copy_from_slice(&block_count.to_le_bytes());
    foot[12..20].copy_from_slice(&smallest_off.to_le_bytes());
    foot[20..28].copy_from_slice(&largest_off.to_le_bytes());
    // The filter is PERSISTED rather than rebuilt at recovery. Rebuilding
    // would mean a full record scan of every run at startup — up to 16
    // runs of ~35 KB each — which is exactly the class of unbounded
    // boot-path work this whole effort has been removing. Persisting
    // costs one bounded 1 KiB read per run instead. It needs no
    // checksum of its own: it sits inside the region the footer's
    // `file_crc` already covers, so a corrupt filter fails the same
    // whole-file validation that a corrupt block does, and
    // `verify_run` rejects the run before the filter is ever consulted.
    foot[RUN_FOOTER_BLOOM_OFF..RUN_FOOTER_BLOOM_OFF + BLOOM_BYTES].copy_from_slice(bloom);
    foot[RUN_FOOTER_INDEX_OFF..RUN_FOOTER_INDEX_OFF + RUN_INDEX_BYTES].copy_from_slice(index);
    crc_state = crc32_update(crc_state, &foot[..RUN_FOOTER_LEN - 4]);
    let file_crc = crc32_finish(crc_state);
    foot[RUN_FOOTER_LEN - 4..].copy_from_slice(&file_crc.to_le_bytes());
    storage
        .append(FileKind::Run, id, &foot)
        .map_err(map_storage)?;
    Ok((file_len + RUN_FOOTER_LEN as u64, file_crc))
}

// ── The provider ──────────────────────────────────────────────────────

/// Ordered disk state-store provider. Borrows its (heap-allocated)
/// state and its storage backend; implements `KvStateStore` plus the
/// inherent `flush()` / `recover()` lifecycle methods.
pub struct DiskStore<'a, S: RunStorage> {
    pub state: &'a mut DiskState,
    pub storage: &'a mut S,
}

impl<'a, S: RunStorage> DiskStore<'a, S> {
    pub fn new(state: &'a mut DiskState, storage: &'a mut S) -> Self {
        Self { state, storage }
    }

    /// Serialise a manifest into `buf`, returning its length. The bytes
    /// are format v1 exactly as before — this is a pure extraction so
    /// the sync and submit/poll publishers cannot drift apart.
    fn encode_manifest(
        buf: &mut [u8; MANIFEST_MAX_LEN],
        new_gen: u32,
        runs: &[RunMeta],
        floor: u64,
        applied_index: u64,
        applied_term: u64,
        highest_revision: u64,
    ) -> usize {
        buf[0..4].copy_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        buf[4..6].copy_from_slice(&MANIFEST_FORMAT_VERSION.to_le_bytes());
        buf[6..8].copy_from_slice(&internal_key::KEY_FORMAT_VERSION.to_le_bytes());
        buf[8..10].copy_from_slice(&state_store::STATE_STORE_CONTRACT_VERSION.to_le_bytes());
        buf[10..12].copy_from_slice(&0u16.to_le_bytes());
        buf[12..16].copy_from_slice(&new_gen.to_le_bytes());
        buf[16..20].copy_from_slice(&(runs.len() as u32).to_le_bytes());
        buf[20..28].copy_from_slice(&floor.to_le_bytes());
        buf[28..36].copy_from_slice(&applied_index.to_le_bytes());
        buf[36..44].copy_from_slice(&applied_term.to_le_bytes());
        buf[44..52].copy_from_slice(&highest_revision.to_le_bytes());
        let mut p = MANIFEST_HDR_LEN;
        for r in runs {
            buf[p..p + 4].copy_from_slice(&r.id.to_le_bytes());
            buf[p + 4..p + 8].copy_from_slice(&r.record_count.to_le_bytes());
            buf[p + 8..p + 16].copy_from_slice(&r.file_len.to_le_bytes());
            buf[p + 16..p + 20].copy_from_slice(&r.digest.to_le_bytes());
            p += MANIFEST_ENTRY_LEN;
        }
        let crc = crc32(&buf[..p]).to_le_bytes();
        buf[p..p + 4].copy_from_slice(&crc);
        // Zero-pad to the fixed slot size. The reader derives the CRC
        // position from `run_count`, so the padding is inert; writing a
        // constant length is what keeps the file from ever extending.
        let end = p + 4;
        for b in buf.iter_mut().take(MANIFEST_SLOT_LEN).skip(end) {
            *b = 0;
        }
        MANIFEST_SLOT_LEN
    }

    /// Write the manifest bytes into the already-created file and
    /// SUBMIT its durability fence, returning the ticket without
    /// waiting.
    fn manifest_write_submit(
        storage: &mut S,
        slot: u32,
        new_gen: u32,
        runs: &[RunMeta],
        floor: u64,
        applied_index: u64,
        applied_term: u64,
        highest_revision: u64,
    ) -> Result<FsyncTicket, StoreError> {
        let mut buf = [0u8; MANIFEST_MAX_LEN];
        let p = Self::encode_manifest(
            &mut buf,
            new_gen,
            runs,
            floor,
            applied_index,
            applied_term,
            highest_revision,
        );
        // IN PLACE, at offset 0, into a file that already exists at its
        // final size. No create, no unlink, no directory enumeration.
        match storage.write_at(FileKind::Manifest, slot, 0, &buf[..p]) {
            Ok(()) => {}
            Err(StorageError::NotFound) => {
                // FAIL-SAFE, not the expected path. `prepare_step` runs
                // during RECOVERING and creates both slots before this
                // store ever serves, so on a real device this arm is
                // dead. It exists so a caller that skipped preparation
                // still produces a CORRECT store rather than a silently
                // unpublishable one — at the cost of one create, which
                // is precisely the 159 ms this design removes. If this
                // ever fires on hardware, the boot path is wrong.
                Self::manifest_slot_prepare(storage, slot)?;
                storage
                    .write_at(FileKind::Manifest, slot, 0, &buf[..p])
                    .map_err(map_storage)?;
            }
            Err(e) => return Err(map_storage(e)),
        }
        storage
            .fsync_submit(FileKind::Manifest, slot)
            .map_err(map_storage)
    }

    /// Ensure one manifest slot file exists at its final size.
    ///
    /// Idempotent and NEVER destructive: an existing slot is left
    /// exactly as it is, because it may be the authoritative manifest.
    /// Only a slot that is genuinely absent is created and zero-filled.
    /// This is the one place a manifest file is ever created, and it is
    /// off the serving path (see `prepare_step`).
    fn manifest_slot_prepare(storage: &mut S, slot: u32) -> Result<(), StoreError> {
        match storage.create(FileKind::Manifest, slot) {
            Ok(()) => {}
            Err(StorageError::AlreadyExists) => return Ok(()),
            Err(e) => return Err(map_storage(e)),
        }
        let zero = [0u8; MANIFEST_SLOT_LEN];
        storage
            .write_at(FileKind::Manifest, slot, 0, &zero)
            .map_err(map_storage)?;
        storage.fsync(FileKind::Manifest, slot).map_err(map_storage)
    }

    /// Flush the memtable into one immutable sorted run file, publish
    /// a new manifest, then release the flushed records (in exactly
    /// that order: the memtable is only released once its contents are
    /// durably referenced).
    ///
    /// Drives `flush_step` to completion. Unbounded by construction, so
    /// it must NOT be called from a scheduled `module_step` on a real
    /// device — `disk_maintenance` drives the chunked form instead.
    /// Retained because it is the natural shape for host tests and for
    /// the install path, both of which run outside the step budget.
    pub fn flush(&mut self) -> Result<(), StoreError> {
        loop {
            match self.flush_step()? {
                Progress::Done => return Ok(()),
                Progress::InProgress { .. } => {}
            }
        }
    }

    /// One bounded slice of a flush: at most `FLUSH_STEP_RECORDS`
    /// memtable records written into the run.
    ///
    /// Call repeatedly until `Progress::Done`. The first call freezes
    /// the record set (see `FlushState::slots`) and creates the run
    /// file; the last writes the footer, fsyncs the run, publishes the
    /// manifest and releases the flushed records from the memtable.
    ///
    /// ATOMICITY IS UNCHANGED, and it is what makes the chunking safe:
    /// publication is still a single indivisible tail — the run is
    /// written and fsynced IN FULL before the manifest naming it is
    /// appended, and the superseding manifest deletes older
    /// generations LAST. A crash at any chunk boundary therefore finds
    /// the PREVIOUS manifest intact and the partial run unreferenced,
    /// which `recover()` collects as an orphan. Chunking adds
    /// boundaries only in the region where nothing is yet published,
    /// so it cannot create a new torn state.
    ///
    /// Writes continue to be ACCEPTED during a flush, up to the
    /// memtable's capacity — that is the entire point of chunking.
    /// They land in slots at or above the frozen prefix and are left in
    /// the memtable when the flushed records are released.
    pub fn flush_step(&mut self) -> Result<Progress, StoreError> {
        self.flush_step_guarded(false)
    }

    /// Flush body that tolerates an active install (the install stream
    /// flushes through this when its staging memtable fills).
    fn flush_inner(&mut self) -> Result<(), StoreError> {
        loop {
            match self.flush_step_guarded(true)? {
                Progress::Done => return Ok(()),
                Progress::InProgress { .. } => {}
            }
        }
    }

    fn flush_step_guarded(&mut self, ignore_install: bool) -> Result<Progress, StoreError> {
        if !self.state.flush.active {
            if self.state.compact.active
                || self.state.capture.active
                || (self.state.install.active && !ignore_install)
            {
                return Err(StoreError::Backpressure);
            }
            if self.state.mem_count == 0 {
                return Ok(Progress::Done);
            }
            if self.state.run_count as usize >= MAX_RUNS {
                // Run set is full: compaction must merge before another
                // flush can be accepted. Never drop.
                return Err(StoreError::Backpressure);
            }
            let id = self.state.next_run_id.max(1);
            create_fresh(self.storage, FileKind::Run, id)?;
            let mut crc_state = CRC32_INIT;
            let mut file_len = 0u64;
            write_run_header(self.storage, id, &mut crc_state, &mut file_len)?;
            let freeze = self.state.mem_count;
            self.state.flush.active = true;
            self.state.flush.applied_at_freeze = self.state.pending_applied_index;
            self.state.flush.term_at_freeze = self.state.pending_applied_term;
            self.state.flush.revision_at_freeze = self.state.highest_revision;
            self.state.flush.phase = FLUSH_PHASE_RECORDS;
            self.state.flush.ticket = 0;
            self.state.flush.pending_gen = 0;
            self.state.flush.pending_slot = 0;
            self.state.flush.pending_len = 0;
            self.state.flush.pending_crc = 0;
            self.state.flush.run_id = id;
            self.state.flush.freeze_count = freeze;
            self.state.flush.cursor = 0;
            self.state.flush.out_blocks = 0;
            self.state.flush.crc_state = crc_state;
            self.state.flush.block_recs = 0;
            self.state.flush.fill = 0;
            self.state.flush.chunks = 0;
            self.state.flush.bloom = [0u8; BLOOM_BYTES];
            self.state.flush.index = [0u8; RUN_INDEX_BYTES];
            self.state.flush.blk_has_first = false;
            self.state.flush.out_len = file_len;
            self.state.flush.smallest_off = 0;
            self.state.flush.largest_off = 0;
            // Freeze the sorted view. From here the flush reads its own
            // snapshot, so concurrent inserts cannot disturb it.
            self.state.flush.slots[..freeze as usize]
                .copy_from_slice(&self.state.order[..freeze as usize]);
        }
        let phase = self.state.flush.phase;
        let stepped = match phase {
            FLUSH_PHASE_RUN_SYNC => self.flush_poll_run(),
            FLUSH_PHASE_MAN_WRITE => self.flush_manifest_write(),
            FLUSH_PHASE_MAN_SYNC => self.flush_poll_manifest(),
            _ => self.flush_step_inner(),
        };
        match stepped {
            Ok(p) => Ok(p),
            Err(e) => {
                // Fail closed: abandon the half-written run (an orphan
                // for `recover()`), keep every record in the memtable.
                // Nothing was published, so the previous manifest is
                // still the truth.
                self.state.flush.active = false;
                self.state.flush.phase = FLUSH_PHASE_RECORDS;
                self.state.flush.ticket = 0;
                Err(e)
            }
        }
    }

    fn flush_step_inner(&mut self) -> Result<Progress, StoreError> {
        let id = self.state.flush.run_id;
        let freeze = self.state.flush.freeze_count as usize;
        let mut written = 0u32;
        while (self.state.flush.cursor as usize) < freeze {
            if written >= FLUSH_STEP_RECORDS {
                self.state.flush.chunks += 1;
                return Ok(Progress::InProgress {
                    cursor: u64::from(self.state.flush.cursor),
                });
            }
            let slot = self.state.flush.slots[self.state.flush.cursor as usize] as usize;
            let (key_len, value_len) = {
                let e = &self.state.entries[slot];
                (e.key_len as usize, e.value_len as usize)
            };
            let rec_len = RECORD_FIXED + key_len + value_len;
            let fill = self.state.flush.fill as usize;
            if fill > 0 && fill + rec_len > BLOCK_TARGET {
                self.flush_write_block()?;
            }
            let fill = self.state.flush.fill as usize;
            {
                // Copy through a scratch record to keep the borrow of
                // `entries` disjoint from the borrow of `flush.block`.
                let e = &self.state.entries[slot];
                let mut hdr = [0u8; RECORD_FIXED];
                hdr[0..2].copy_from_slice(&(key_len as u16).to_le_bytes());
                hdr[2..6].copy_from_slice(&(value_len as u32).to_le_bytes());
                let key = e.key[..key_len].as_ptr();
                let value = e.value[..value_len].as_ptr();
                // SAFETY: `key`/`value` point into `self.state.entries`
                // and the destination is `self.state.flush.block`;
                // these are distinct fields of the same struct, so the
                // regions cannot overlap. Lengths are the slot's own
                // recorded lengths, both bounded by the block buffer
                // check below.
                if fill + rec_len > BLOCK_MAX_PAYLOAD {
                    return Err(StoreError::StorageFault);
                }
                unsafe {
                    let dst = self.state.flush.block.as_mut_ptr().add(fill);
                    core::ptr::copy_nonoverlapping(hdr.as_ptr(), dst, RECORD_FIXED);
                    core::ptr::copy_nonoverlapping(key, dst.add(RECORD_FIXED), key_len);
                    core::ptr::copy_nonoverlapping(
                        value,
                        dst.add(RECORD_FIXED + key_len),
                        value_len,
                    );
                }
            }
            // First record of a fresh block: remember its key for the
            // sparse index entry written when this block closes.
            if !self.state.flush.blk_has_first {
                let e = &self.state.entries[slot];
                let n = key_len.min(INDEX_KEY_BYTES);
                let mut k = [0u8; INDEX_KEY_BYTES];
                k[..n].copy_from_slice(&e.key[..n]);
                self.state.flush.blk_first_key = k;
                self.state.flush.blk_has_first = true;
            }
            // Incremental filter build: the record's PREFIX (encoded
            // key minus its version suffix) is what point lookups probe
            // with, so that is what goes in.
            {
                let e = &self.state.entries[slot];
                let plen = key_len.saturating_sub(VERSION_SUFFIX_LEN);
                let mut pbuf = [0u8; MAX_ENCODED_KEY];
                pbuf[..plen].copy_from_slice(&e.key[..plen]);
                bloom_insert(&mut self.state.flush.bloom, &pbuf[..plen]);
            }
            self.state.flush.fill += rec_len as u32;
            self.state.flush.block_recs += 1;
            self.state.flush.cursor += 1;
            written += 1;
        }
        // Records exhausted: close the last block, then hand off to the
        // fenced tail. Everything below spans steps.
        if self.state.flush.fill > 0 {
            self.flush_write_block()?;
        }
        let record_count = self.state.flush.freeze_count;
        let (final_len, file_crc) = write_run_footer(
            self.storage,
            id,
            record_count,
            self.state.flush.out_blocks,
            self.state.flush.smallest_off,
            self.state.flush.largest_off,
            &self.state.flush.bloom,
            &self.state.flush.index,
            self.state.flush.crc_state,
            self.state.flush.out_len,
        )?;
        // SUBMIT the run's durability fence and LEAVE. This is the
        // 163 ms that killed the module: it is now spread across later
        // steps as cheap polls instead of one blocking call.
        let ticket = self
            .storage
            .fsync_submit(FileKind::Run, id)
            .map_err(map_storage)?;
        self.state.flush.ticket = ticket;
        self.state.flush.pending_len = final_len;
        self.state.flush.pending_crc = file_crc;
        self.state.flush.phase = FLUSH_PHASE_RUN_SYNC;
        self.state.flush.chunks += 1;
        Ok(Progress::InProgress {
            cursor: u64::from(self.state.flush.cursor),
        })
    }

    /// Poll the run's fence. NOTHING is published or released here —
    /// the manifest naming this run is not even written until the run
    /// is proven durable, which is the ordering recovery depends on.
    fn flush_poll_run(&mut self) -> Result<Progress, StoreError> {
        let id = self.state.flush.run_id;
        match self
            .storage
            .fsync_poll(FileKind::Run, id, self.state.flush.ticket)
            .map_err(map_storage)?
        {
            FsyncState::Pending => {
                self.state.flush.chunks += 1;
                return Ok(Progress::InProgress {
                    cursor: u64::from(self.state.flush.cursor),
                });
            }
            FsyncState::Durable => {}
        }
        // The run is on non-volatile media. Only NOW may a manifest
        // name it — but allocating that file is its own step.
        self.state.flush.pending_gen = self.state.generation + 1;
        self.state.flush.phase = FLUSH_PHASE_MAN_WRITE;
        self.state.flush.chunks += 1;
        Ok(Progress::InProgress {
            cursor: u64::from(self.state.flush.cursor),
        })
    }

    /// Write the manifest bytes and submit its fence.
    fn flush_manifest_write(&mut self) -> Result<Progress, StoreError> {
        let meta = RunMeta {
            id: self.state.flush.run_id,
            record_count: self.state.flush.freeze_count,
            file_len: self.state.flush.pending_len,
            digest: self.state.flush.pending_crc,
            _pad: 0,
        };
        let mut new_runs = [RunMeta::empty(); MAX_RUNS];
        new_runs[0] = meta;
        let old_count = self.state.run_count as usize;
        new_runs[1..1 + old_count].copy_from_slice(&self.state.runs[..old_count]);
        let slot = self.state.inactive_manifest_slot();
        let ticket = Self::manifest_write_submit(
            self.storage,
            slot,
            self.state.flush.pending_gen,
            &new_runs[..old_count + 1],
            self.state.compaction_floor,
            self.state.flush.applied_at_freeze,
            self.state.flush.term_at_freeze,
            self.state.flush.revision_at_freeze,
        )?;
        self.state.flush.pending_slot = slot;
        self.state.flush.ticket = ticket;
        self.state.flush.phase = FLUSH_PHASE_MAN_SYNC;
        self.state.flush.chunks += 1;
        Ok(Progress::InProgress {
            cursor: u64::from(self.state.flush.cursor),
        })
    }

    /// Poll the manifest's fence and, only on `Durable`, commit: retire
    /// older manifests, adopt the new run set, release the flushed
    /// records.
    ///
    /// THE INTERLOCK LIVES HERE. Every irreversible act of a flush —
    /// deleting the previous manifest, advancing `generation` and
    /// `next_run_id`, and above all releasing the memtable's frozen
    /// prefix — happens on this line and no earlier. A fence that has
    /// only been SUBMITTED proves nothing, so nothing may be spent
    /// against it. If the machine dies anywhere before this point the
    /// memtable content is still in the WAL-replayable log and the
    /// previous manifest is still the truth.
    fn flush_poll_manifest(&mut self) -> Result<Progress, StoreError> {
        let new_gen = self.state.flush.pending_gen;
        // The FILE id is the SLOT, not the generation — under A/B the
        // generation lives in the body and names no file at all.
        match self
            .storage
            .fsync_poll(
                FileKind::Manifest,
                self.state.flush.pending_slot,
                self.state.flush.ticket,
            )
            .map_err(map_storage)?
        {
            FsyncState::Pending => {
                self.state.flush.chunks += 1;
                return Ok(Progress::InProgress {
                    cursor: u64::from(self.state.flush.cursor),
                });
            }
            FsyncState::Durable => {}
        }
        let meta = RunMeta {
            id: self.state.flush.run_id,
            record_count: self.state.flush.freeze_count,
            file_len: self.state.flush.pending_len,
            digest: self.state.flush.pending_crc,
            _pad: 0,
        };
        let mut new_runs = [RunMeta::empty(); MAX_RUNS];
        new_runs[0] = meta;
        let old_count = self.state.run_count as usize;
        new_runs[1..1 + old_count].copy_from_slice(&self.state.runs[..old_count]);
        // The filters MUST move exactly as `runs` moves — an
        // index-misaligned filter would answer for the wrong run, and a
        // false negative there is a silent lost read, not a slow one.
        let mut new_blooms = [[0u8; BLOOM_BYTES]; MAX_RUNS];
        new_blooms[0] = self.state.flush.bloom;
        new_blooms[1..1 + old_count].copy_from_slice(&self.state.run_blooms[..old_count]);
        self.state.run_blooms = new_blooms;
        let mut new_index = [[0u8; RUN_INDEX_BYTES]; MAX_RUNS];
        new_index[0] = self.state.flush.index;
        new_index[1..1 + old_count].copy_from_slice(&self.state.run_index[..old_count]);
        self.state.run_index = new_index;
        self.state.runs = new_runs;
        self.state.run_count = (old_count + 1) as u32;
        self.state.generation = new_gen;
        self.state.manifest_slot = self.state.flush.pending_slot;
        self.state.next_run_id = self.state.flush.run_id + 1;
        self.state.flush.active = false;
        self.state.durable_applied_index = self.state.flush.applied_at_freeze;
        self.state.durable_applied_term = self.state.flush.term_at_freeze;
        self.state.durable_highest_revision = self.state.flush.revision_at_freeze;
        // Back to the resting phase. Leaving it at MANIFEST_SYNC would
        // make `flush_phase()` lie to diagnostics after a completed
        // flush, and would leave a stale ticket one missed `active`
        // check away from being polled.
        self.state.flush.phase = FLUSH_PHASE_RECORDS;
        self.state.flush.ticket = 0;
        self.state.flush.chunks += 1;
        self.state.release_flushed(self.state.flush.freeze_count);
        Ok(Progress::Done)
    }

    /// Write the buffered block, tracking offsets exactly as the
    /// single-call flush did.
    fn flush_write_block(&mut self) -> Result<(), StoreError> {
        let id = self.state.flush.run_id;
        let fill = self.state.flush.fill as usize;
        let recs = self.state.flush.block_recs;
        let mut crc_state = self.state.flush.crc_state;
        let mut file_len = self.state.flush.out_len;
        // The payload must be handed to `write_run_block` as a slice
        // while `self.storage` is also borrowed mutably, so it goes via
        // a stack copy — the same ~4 KB the single-call path kept on
        // the stack for its whole run.
        let mut payload = [0u8; BLOCK_MAX_PAYLOAD];
        payload[..fill].copy_from_slice(&self.state.flush.block[..fill]);
        let off = write_run_block(
            self.storage,
            id,
            &payload[..fill],
            recs,
            &mut crc_state,
            &mut file_len,
        )?;
        if self.state.flush.out_blocks == 0 {
            self.state.flush.smallest_off = off;
        }
        self.state.flush.largest_off = off;
        {
            let ordinal = self.state.flush.out_blocks;
            let first = self.state.flush.blk_first_key;
            idx_push(&mut self.state.flush.index, ordinal, off, &first);
        }
        self.state.flush.blk_has_first = false;
        self.state.flush.out_blocks += 1;
        self.state.flush.crc_state = crc_state;
        self.state.flush.out_len = file_len;
        self.state.flush.fill = 0;
        self.state.flush.block_recs = 0;
        Ok(())
    }

    /// Is a chunked flush in flight? Callers use this to tell "the
    /// memtable is full and a flush is already working on it"
    /// (legitimate, retryable backpressure) from "the memtable is full
    /// and nothing is happening" (a fault).
    pub fn flush_in_progress(&self) -> bool {
        self.state.flush.active
    }

    /// Bounded steps taken by the current or most recent flush.
    pub fn flush_chunks(&self) -> u32 {
        self.state.flush.chunks
    }

    /// Which phase of the flush tail is running (`FLUSH_PHASE_*`):
    /// 0 = writing records, 1 = awaiting the run's durability fence,
    /// 2 = awaiting the manifest's. Logged per chunk so a capture
    /// separates "the records are slow" from "the fence is slow" —
    /// the distinction that took a rig cycle to establish.
    pub fn flush_phase(&self) -> u8 {
        self.state.flush.phase
    }

    /// ONE bounded preparation action per call; drive to
    /// `Progress::Done` before `recover()`.
    ///
    /// Ensures the manifest A/B pair exists at full size. This is the
    /// only place a manifest file is ever created, and it is
    /// deliberately OFF the serving path — a FAT32 create measured
    /// 159 ms on the Pi 5 rig, so doing both in one scheduler step would
    /// reintroduce at startup exactly the step-deadline kill the A/B
    /// pair exists to remove. One slot per call, one call per step.
    ///
    /// Idempotent and never destructive: an existing slot is left
    /// untouched, because it may be the authoritative manifest.
    pub fn prepare_step(&mut self) -> Result<Progress, StoreError> {
        if self.state.prepared & 1 == 0 {
            Self::manifest_slot_prepare(self.storage, MANIFEST_SLOT_A)?;
            self.state.prepared |= 1;
            return Ok(Progress::InProgress { cursor: 1 });
        }
        if self.state.prepared & 2 == 0 {
            Self::manifest_slot_prepare(self.storage, MANIFEST_SLOT_B)?;
            self.state.prepared |= 2;
            return Ok(Progress::InProgress { cursor: 2 });
        }
        Ok(Progress::Done)
    }

    /// Has the A/B pair been ensured?
    pub fn prepared(&self) -> bool {
        self.state.prepared & 3 == 3
    }

    /// §9.4 steps 1-4: load the newest checksum-valid manifest,
    /// validate every referenced run (identity, sizes, checksums,
    /// format versions), quarantine on any inconsistency, and remove
    /// orphan files. The memtable starts empty; committed-entry replay
    /// (step 5) is the caller's job.
    ///
    /// On `Err(StorageFault)` the state is left EMPTY and the on-disk
    /// files untouched (quarantine — never guess, never delete
    /// evidence). The caller must not serve.
    pub fn recover(&mut self) -> Result<(), StoreError> {
        let range_gen = self.state.range_generation;
        let prepared = self.state.prepared;
        self.state.init();
        self.state.range_generation = range_gen;
        // Preallocation is a property of the FILESYSTEM, not of the
        // recovered logical state: re-creating the pair after every
        // recover would put a 159 ms create back on the boot path.
        self.state.prepared = prepared;

        // A/B SELECTION. Read both fixed slots, validate each
        // independently, and take the valid one with the HIGHER
        // INTERNAL generation. No enumeration, no deletion: the pair is
        // permanent, so recovery knows both names a priori.
        let mut winner_gen: Option<u32> = None;
        let mut winner_slot = MANIFEST_SLOT_A;
        let mut floor = 0u64;
        let mut run_count = 0usize;
        let mut runs = [RunMeta::empty(); MAX_RUNS];
        let mut applied = (0u64, 0u64);
        let mut man_revision = 0u64;
        for slot in [MANIFEST_SLOT_A, MANIFEST_SLOT_B] {
            let Some((gen, f, rc, rs, ai, at, hr)) = self.read_manifest(slot)? else {
                continue;
            };
            if winner_gen.is_none_or(|w| gen > w) {
                winner_gen = Some(gen);
                winner_slot = slot;
                floor = f;
                run_count = rc;
                runs = rs;
                applied = (ai, at);
                man_revision = hr;
            }
        }

        let mut run_ids = [0u32; MAX_LISTED_RUNS];
        let run_n = self
            .storage
            .list(FileKind::Run, &mut run_ids)
            .map_err(map_storage)?;
        let run_ids = &run_ids[..run_n];
        let mut max_seen_id = 0u32;
        for &id in run_ids {
            max_seen_id = max_seen_id.max(id);
        }

        if let Some(gen) = winner_gen {
            // Every referenced run must exist and validate; anything
            // else is quarantine, not cleanup.
            for meta in runs.iter().take(run_count) {
                if !run_ids.contains(&meta.id) {
                    return Err(StoreError::StorageFault);
                }
                self.verify_run(meta)?;
                max_seen_id = max_seen_id.max(meta.id);
            }
            // Orphans (present but unreferenced) are deleted.
            for &id in run_ids {
                if !runs.iter().take(run_count).any(|m| m.id == id) {
                    match self.storage.delete(FileKind::Run, id) {
                        Ok(()) | Err(StorageError::NotFound) => {}
                        Err(e) => return Err(map_storage(e)),
                    }
                }
            }
            self.state.runs = runs;
            self.state.run_count = run_count as u32;
            // Load each run's persisted filter. One bounded 1 KiB read
            // per run — versus the full record scan of every run that
            // rebuilding from keys would cost on the boot path.
            // `verify_run` has already validated the whole file
            // (including this region, which the file CRC covers), so
            // these bytes are known good before they are trusted.
            for (i, meta) in runs.iter().enumerate().take(run_count) {
                let off = meta.file_len - (RUN_FOOTER_LEN - RUN_FOOTER_BLOOM_OFF) as u64;
                let mut filter = [0u8; BLOOM_BYTES];
                read_exact(&*self.storage, FileKind::Run, meta.id, off, &mut filter)?;
                self.state.run_blooms[i] = filter;
                let ioff = meta.file_len - (RUN_FOOTER_LEN - RUN_FOOTER_INDEX_OFF) as u64;
                let mut index = [0u8; RUN_INDEX_BYTES];
                read_exact(&*self.storage, FileKind::Run, meta.id, ioff, &mut index)?;
                self.state.run_index[i] = index;
            }
            self.state.generation = gen;
            self.state.manifest_slot = winner_slot;
            self.state.compaction_floor = floor;
            self.state.durable_applied_index = applied.0;
            self.state.durable_applied_term = applied.1;
            // Baseline for the next flush: nothing newer than the
            // manifest has been applied yet at recovery time.
            self.state.pending_applied_index = applied.0;
            self.state.pending_applied_term = applied.1;
            // Restore the revision clock to the run-set's high water:
            // the WAL tail beyond the manifest replays ON TOP of this,
            // so run content stays visible and new versions keep
            // ascending. Without this, a truncated WAL restarts the
            // clock at the tail and every run version is "future".
            self.state.durable_highest_revision = man_revision;
            self.state.highest_revision = self.state.highest_revision.max(man_revision);
        } else {
            // No usable manifest: the durable state is empty; every
            // run present was never published and is an orphan.
            for &id in run_ids {
                match self.storage.delete(FileKind::Run, id) {
                    Ok(()) | Err(StorageError::NotFound) => {}
                    Err(e) => return Err(map_storage(e)),
                }
            }
        }
        self.state.next_run_id = max_seen_id + 1;

        // Rebuild `highest_revision` from the surviving records (the
        // walk also re-validates every block CRC).
        let mut highest = 0u64;
        let mut m = Merge::init(self.state, &*self.storage, false, b"")?;
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        while let Some((klen, _)) = m.next(self.state, &*self.storage, &mut kbuf, &mut vbuf)? {
            highest = highest.max(key_timestamp(&kbuf[..klen]));
        }
        self.state.highest_revision = highest;
        Ok(())
    }

    /// Read + checksum-validate one manifest file. `Ok(None)` = the
    /// file is torn/corrupt (a recovery candidate to skip), `Err` = the
    /// backend itself failed.
    /// Read and fully validate one manifest SLOT.
    ///
    /// `Ok(Some((generation, floor, run_count, runs)))` when the slot
    /// holds a structurally valid, checksum-clean manifest; `Ok(None)`
    /// for anything else — absent, never written, half written,
    /// wrong format, bad CRC. Never an error for "this slot is not
    /// usable": that is the normal state of the inactive half of the
    /// pair on first boot, and of a slot caught mid-publish by a crash.
    ///
    /// The generation comes from the BODY, not the filename. Under the
    /// A/B scheme the filename is a fixed slot id and carries no
    /// ordering information at all.
    #[allow(
        clippy::type_complexity,
        reason = "single internal caller destructures in place; a named struct would outlive its one use"
    )]
    fn read_manifest(
        &self,
        slot: u32,
    ) -> Result<Option<(u32, u64, usize, [RunMeta; MAX_RUNS], u64, u64, u64)>, StoreError> {
        let mut buf = [0u8; MANIFEST_SLOT_LEN];
        let mut len = 0usize;
        loop {
            if len == buf.len() {
                break;
            }
            let n =
                match self
                    .storage
                    .read_at(FileKind::Manifest, slot, len as u64, &mut buf[len..])
                {
                    Ok(n) => n,
                    // An absent slot is a normal first-boot state.
                    Err(StorageError::NotFound) => return Ok(None),
                    Err(e) => return Err(map_storage(e)),
                };
            if n == 0 {
                break;
            }
            len += n;
        }
        if len < MANIFEST_HDR_LEN + 4 {
            return Ok(None);
        }
        let body = &buf[..len];
        if rd_u32(body, 0) != MANIFEST_MAGIC
            || rd_u16(body, 4) != MANIFEST_FORMAT_VERSION
            || rd_u16(body, 6) != internal_key::KEY_FORMAT_VERSION
            || rd_u16(body, 8) != state_store::STATE_STORE_CONTRACT_VERSION
        {
            return Ok(None);
        }
        let generation = rd_u32(body, 12);
        let run_count = rd_u32(body, 16) as usize;
        let floor = rd_u64(body, 20);
        let applied_index = rd_u64(body, 28);
        let applied_term = rd_u64(body, 36);
        let highest_revision = rd_u64(body, 44);
        if run_count > MAX_RUNS {
            return Ok(None);
        }
        // The CRC sits immediately after the declared entries; the rest
        // of the fixed-size slot is inert padding. Length-driven, not
        // file-size-driven — a slot is always MANIFEST_SLOT_LEN long
        // regardless of how many runs it names.
        let end = MANIFEST_HDR_LEN + run_count * MANIFEST_ENTRY_LEN;
        if len < end + 4 {
            return Ok(None);
        }
        if crc32(&body[..end]) != rd_u32(body, end) {
            return Ok(None);
        }
        let mut runs = [RunMeta::empty(); MAX_RUNS];
        let mut p = MANIFEST_HDR_LEN;
        for meta in runs.iter_mut().take(run_count) {
            meta.id = rd_u32(body, p);
            meta.record_count = rd_u32(body, p + 4);
            meta.file_len = rd_u64(body, p + 8);
            meta.digest = rd_u32(body, p + 16);
            p += MANIFEST_ENTRY_LEN;
        }
        Ok(Some((
            generation,
            floor,
            run_count,
            runs,
            applied_index,
            applied_term,
            highest_revision,
        )))
    }

    /// Validate one referenced run end-to-end: exact length,
    /// header/footer framing and versions, and the whole-file CRC
    /// against both the stored footer field and the manifest digest.
    fn verify_run(&self, meta: &RunMeta) -> Result<(), StoreError> {
        if meta.file_len < (RUN_HDR_LEN + RUN_FOOTER_LEN) as u64 {
            return Err(StoreError::StorageFault);
        }
        // Rolling CRC over [0, file_len - 4).
        let mut crc_state = CRC32_INIT;
        let mut chunk = [0u8; 4096];
        let crc_end = meta.file_len - 4;
        let mut off = 0u64;
        while off < crc_end {
            let want = (crc_end - off).min(chunk.len() as u64) as usize;
            read_exact(
                &*self.storage,
                FileKind::Run,
                meta.id,
                off,
                &mut chunk[..want],
            )?;
            crc_state = crc32_update(crc_state, &chunk[..want]);
            off += want as u64;
        }
        let computed = crc32_finish(crc_state);
        let mut tail = [0u8; 4];
        read_exact(&*self.storage, FileKind::Run, meta.id, crc_end, &mut tail)?;
        if computed != rd_u32(&tail, 0) || computed != meta.digest {
            return Err(StoreError::StorageFault);
        }
        // The file must end exactly at file_len.
        let mut probe = [0u8; 1];
        let n = self
            .storage
            .read_at(FileKind::Run, meta.id, meta.file_len, &mut probe)
            .map_err(map_storage)?;
        if n != 0 {
            return Err(StoreError::StorageFault);
        }
        // Header + footer framing.
        let mut hdr = [0u8; RUN_HDR_LEN];
        read_exact(&*self.storage, FileKind::Run, meta.id, 0, &mut hdr)?;
        if rd_u32(&hdr, 0) != RUN_MAGIC
            || rd_u16(&hdr, 4) != RUN_FORMAT_VERSION
            || rd_u16(&hdr, 8) != internal_key::KEY_FORMAT_VERSION
            || rd_u16(&hdr, 10) != state_store::STATE_STORE_CONTRACT_VERSION
        {
            return Err(StoreError::StorageFault);
        }
        let mut foot = [0u8; RUN_FOOTER_LEN];
        read_exact(
            &*self.storage,
            FileKind::Run,
            meta.id,
            meta.file_len - RUN_FOOTER_LEN as u64,
            &mut foot,
        )?;
        if rd_u32(&foot, 0) != RUN_FOOTER_MAGIC || rd_u32(&foot, 4) != meta.record_count {
            return Err(StoreError::StorageFault);
        }
        Ok(())
    }

    /// Abort an in-progress multi-step snapshot capture (e.g. the
    /// caller's export buffer cannot hold the whole stream). Clears
    /// the capture fence so `apply`/`flush` stop returning
    /// `Backpressure`; the caller re-requests a capture later. No
    /// on-disk state is touched — capture is read-only.
    pub fn snapshot_abort(&mut self) {
        self.state.capture.active = false;
    }

    /// Abort a half-done install: the store is left EMPTY (the caller
    /// re-requests the snapshot; a partial install must never be
    /// mistaken for a good one).
    fn install_abort(&mut self) {
        self.state.install.active = false;
        self.state.mem_count = 0;
    }

    /// Is a snapshot capture holding the apply/flush exclusion?
    pub fn capture_active(&self) -> bool {
        self.state.capture.active
    }

    /// Is a REPLACE install holding the apply/flush exclusion?
    pub fn install_active(&self) -> bool {
        self.state.install.active
    }

    /// Is a compaction holding the flush exclusion?
    pub fn compact_active(&self) -> bool {
        self.state.compact.active
    }

    /// Any fence that can refuse `apply`/`flush` with `Backpressure`.
    pub fn any_fence_active(&self) -> bool {
        self.capture_active() || self.install_active() || self.compact_active()
    }

    /// Release EVERY exclusion fence, unconditionally.
    ///
    /// The bounded-starvation escape hatch. v1 excludes concurrent
    /// apply from a capture/install/compaction by refusing writes with
    /// `Backpressure`, which is only a safe simplification while the
    /// fence is guaranteed to be released — and one of them is not:
    /// an install opened by a RESET whose chunk stream is then
    /// abandoned (a gap, a leader that stops sending, a peer that
    /// dies) has no code path left that would ever clear it. The store
    /// then refuses every write forever.
    ///
    /// So the hosting module runs a watchdog and calls this once its
    /// budget is spent. Every one of the three is safely abortable:
    ///
    /// - **capture** is read-only; the caller re-requests it later.
    /// - **compaction** publishes last, so the pre-compaction manifest
    ///   is still the published truth; the half-written output run
    ///   becomes an orphan that `recover()` collects.
    /// - **install** is REPLACE: it destroyed local state up front by
    ///   design, so aborting leaves the store EMPTY and forces a
    ///   re-install. That is strictly better than a node that is
    ///   wedged forever, and the range was not serving during the
    ///   install anyway.
    ///
    /// A snapshot, a compaction and an install are all retryable.
    /// Serving writes is not optional. When those conflict, the fence
    /// yields.
    pub fn abort_fences(&mut self) {
        self.state.capture.active = false;
        self.state.compact.active = false;
        if self.state.install.active {
            self.install_abort();
        }
    }
}

// ── KvStateStore implementation ───────────────────────────────────────

impl<S: RunStorage> KvStateStore for DiskStore<'_, S> {
    /// `key` is the encoded internal-key PREFIX (through the
    /// terminator, no version suffix — `internal_key::encode_prefix`).
    /// Newest version with timestamp `<= revision` wins; a
    /// `PointTombstone` found first is `NotFound`.
    fn get_at(
        &self,
        key: &[u8],
        revision: Revision,
        value_out: &mut [u8],
    ) -> Result<usize, StoreError> {
        if key.len() < IDENTITY_PREFIX_LEN + 2
            || key.len() + VERSION_SUFFIX_LEN > MAX_ENCODED_KEY
            || key[key.len() - 2..] != internal_key::KEY_TERMINATOR
        {
            return Err(StoreError::Malformed);
        }
        if revision != 0 && revision < self.state.compaction_floor {
            return Err(StoreError::Compacted);
        }
        let revn = normalize_revision(revision);
        let mut m = Merge::init_point(self.state, &*self.storage, key)?;
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        loop {
            let Some((klen, vlen)) = m.next(self.state, &*self.storage, &mut kbuf, &mut vbuf)?
            else {
                return Err(StoreError::NotFound);
            };
            if klen < key.len() || kbuf[..key.len()] != *key {
                return Err(StoreError::NotFound);
            }
            // Prefix match implies an exact version of this user key
            // (the terminator forbids prefix ambiguity); anything else
            // is a corrupt store.
            if klen != key.len() + VERSION_SUFFIX_LEN {
                return Err(StoreError::StorageFault);
            }
            if key_timestamp(&kbuf[..klen]) > revn {
                continue; // newer than the requested revision
            }
            if key_kind_byte(&kbuf[..klen]) == ValueKind::PointTombstone as u8 {
                return Err(StoreError::NotFound);
            }
            if vlen > value_out.len() {
                return Err(StoreError::OutputTooSmall);
            }
            value_out[..vlen].copy_from_slice(&vbuf[..vlen]);
            return Ok(vlen);
        }
    }

    /// Bounded merged scan. Output entries are
    /// `[key_len:u16 LE][value_len:u32 LE][full encoded key][value]`
    /// (the FULL internal key of the winning version, so callers see
    /// the timestamp). The resume cursor is the count of entries
    /// already emitted since `span.start` — an opaque ordinal that
    /// stays logically stable across flush and floor-respecting
    /// compaction because neither changes visible content. Resume
    /// re-walks the merge and skips that many visible entries (bounded
    /// by the provider's fixed capacities).
    fn scan_at(
        &self,
        span: KeySpan<'_>,
        revision: Revision,
        resume_cursor: u64,
        out: &mut [u8],
    ) -> Result<ScanProgress, StoreError> {
        if revision != 0 && revision < self.state.compaction_floor {
            return Err(StoreError::Compacted);
        }
        let revn = normalize_revision(revision);
        let mut m = Merge::init(self.state, &*self.storage, true, span.start)?;
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut prefix = [0u8; MAX_ENCODED_KEY];
        let mut prefix_len = 0usize;
        let mut decided = false;
        let mut skipped = 0u64;
        let mut emitted = 0usize;
        let mut pos = 0usize;
        loop {
            let Some((klen, vlen)) = m.next(self.state, &*self.storage, &mut kbuf, &mut vbuf)?
            else {
                return Ok(ScanProgress {
                    entries: emitted,
                    bytes: pos,
                    progress: Progress::Done,
                });
            };
            if !span.end.is_empty() && kbuf[..klen] >= *span.end {
                return Ok(ScanProgress {
                    entries: emitted,
                    bytes: pos,
                    progress: Progress::Done,
                });
            }
            if klen < MIN_ENCODED_KEY {
                return Err(StoreError::StorageFault);
            }
            let plen = klen - VERSION_SUFFIX_LEN;
            if plen != prefix_len || kbuf[..plen] != prefix[..prefix_len] {
                prefix[..plen].copy_from_slice(&kbuf[..plen]);
                prefix_len = plen;
                decided = false;
            }
            if decided {
                continue; // an older version of an already-decided key
            }
            if key_timestamp(&kbuf[..klen]) > revn {
                continue; // not yet visible at this revision
            }
            decided = true;
            if key_kind_byte(&kbuf[..klen]) == ValueKind::PointTombstone as u8 {
                continue; // deleted at this revision
            }
            if skipped < resume_cursor {
                skipped += 1;
                continue; // already emitted in an earlier step
            }
            let need = RECORD_FIXED + klen + vlen;
            if pos + need > out.len() {
                if pos == 0 {
                    return Err(StoreError::OutputTooSmall);
                }
                return Ok(ScanProgress {
                    entries: emitted,
                    bytes: pos,
                    progress: Progress::InProgress {
                        cursor: resume_cursor + emitted as u64,
                    },
                });
            }
            out[pos..pos + 2].copy_from_slice(&(klen as u16).to_le_bytes());
            out[pos + 2..pos + 6].copy_from_slice(&(vlen as u32).to_le_bytes());
            out[pos + 6..pos + 6 + klen].copy_from_slice(&kbuf[..klen]);
            out[pos + 6 + klen..pos + need].copy_from_slice(&vbuf[..vlen]);
            pos += need;
            emitted += 1;
        }
    }

    fn scan_versions(
        &self,
        span: KeySpan<'_>,
        from_revision: Revision,
        to_revision: Revision,
        resume_cursor: u64,
        out: &mut [u8],
    ) -> Result<ScanProgress, StoreError> {
        // The window's LOWER bound is what the floor has to clear.
        // Checking the upper bound instead would accept a window whose
        // early events had already been reclaimed, and the caller would
        // receive a partial history it could not tell from a whole one.
        if from_revision < self.state.compaction_floor {
            return Err(StoreError::Compacted);
        }
        let hi = normalize_revision(to_revision);
        if from_revision >= hi {
            // Empty or inverted window: nothing happened in it. This is
            // a legitimate answer (a watcher that reconnects instantly
            // has missed nothing), not an error.
            return Ok(ScanProgress {
                entries: 0,
                bytes: 0,
                progress: Progress::Done,
            });
        }
        let mut m = Merge::init(self.state, &*self.storage, true, span.start)?;
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut skipped = 0u64;
        let mut emitted = 0usize;
        let mut pos = 0usize;
        loop {
            let Some((klen, vlen)) = m.next(self.state, &*self.storage, &mut kbuf, &mut vbuf)?
            else {
                return Ok(ScanProgress {
                    entries: emitted,
                    bytes: pos,
                    progress: Progress::Done,
                });
            };
            if !span.end.is_empty() && kbuf[..klen] >= *span.end {
                return Ok(ScanProgress {
                    entries: emitted,
                    bytes: pos,
                    progress: Progress::Done,
                });
            }
            if klen < MIN_ENCODED_KEY {
                return Err(StoreError::StorageFault);
            }
            // No per-key `decided` latch and no tombstone filter — that
            // is the entire difference from `scan_at`. Every version in
            // the window is an event that happened, including deletes,
            // and including several versions of the same key.
            let ts = key_timestamp(&kbuf[..klen]);
            if ts <= from_revision || ts > hi {
                continue;
            }
            if skipped < resume_cursor {
                skipped += 1;
                continue; // already emitted in an earlier step
            }
            let need = RECORD_FIXED + klen + vlen;
            if pos + need > out.len() {
                if pos == 0 {
                    return Err(StoreError::OutputTooSmall);
                }
                return Ok(ScanProgress {
                    entries: emitted,
                    bytes: pos,
                    progress: Progress::InProgress {
                        cursor: resume_cursor + emitted as u64,
                    },
                });
            }
            out[pos..pos + 2].copy_from_slice(&(klen as u16).to_le_bytes());
            out[pos + 2..pos + 6].copy_from_slice(&(vlen as u32).to_le_bytes());
            out[pos + 6..pos + 6 + klen].copy_from_slice(&kbuf[..klen]);
            out[pos + 6 + klen..pos + need].copy_from_slice(&vbuf[..vlen]);
            pos += need;
            emitted += 1;
        }
    }

    /// Materialize one committed batch (format v1, module docs).
    /// Validate-then-apply: a `Malformed` batch mutates nothing.
    fn apply(&mut self, batch: &[u8]) -> Result<ApplyResult, StoreError> {
        if self.state.install.active || self.state.capture.active {
            return Err(StoreError::Backpressure);
        }
        if batch.len() < BATCH_HDR_LEN
            || rd_u32(batch, 0) != BATCH_MAGIC
            || rd_u16(batch, 4) != BATCH_FORMAT_VERSION
        {
            return Err(StoreError::Malformed);
        }
        let op_count = rd_u16(batch, 6) as usize;
        // Pass 1: validate every op and count the new-slot demand.
        let mut p = BATCH_HDR_LEN;
        let mut new_needed = 0usize;
        let mut batch_max = 0u64;
        let mut user_key = [0u8; MAX_ENCODED_KEY];
        for _ in 0..op_count {
            if p + RECORD_FIXED > batch.len() {
                return Err(StoreError::Malformed);
            }
            let klen = rd_u16(batch, p) as usize;
            let vlen = rd_u32(batch, p + 2) as usize;
            if !(MIN_ENCODED_KEY..=MAX_ENCODED_KEY).contains(&klen)
                || vlen > MAX_VALUE_LEN
                || p + RECORD_FIXED + klen + vlen > batch.len()
            {
                return Err(StoreError::Malformed);
            }
            let key = &batch[p + RECORD_FIXED..p + RECORD_FIXED + klen];
            let Some(decoded) = internal_key::decode(key, &mut user_key) else {
                return Err(StoreError::Malformed);
            };
            // A tombstone is empty (historical layout) or carries
            // exactly its 8-byte MVCC commit timestamp.
            // Anything else is malformed.
            if decoded.kind == ValueKind::PointTombstone && vlen != 0 && vlen != 8 {
                return Err(StoreError::Malformed);
            }
            batch_max = batch_max.max(decoded.mvcc_timestamp);
            let at = self.state.mem_lower_bound(key);
            if !(at < self.state.mem_count as usize && self.state.mem_key(at) == key) {
                // Duplicate keys inside one batch are counted twice —
                // a conservative over-estimate that can only cause an
                // early (retryable) Backpressure, never a drop.
                new_needed += 1;
            }
            p += RECORD_FIXED + klen + vlen;
        }
        if p != batch.len() {
            return Err(StoreError::Malformed); // trailing garbage
        }
        if self.state.mem_count as usize + new_needed > MEMTABLE_MAX_ENTRIES {
            return Err(StoreError::Backpressure);
        }
        // Pass 2: apply (cannot fail after the capacity pre-check).
        let mut p = BATCH_HDR_LEN;
        for _ in 0..op_count {
            let klen = rd_u16(batch, p) as usize;
            let vlen = rd_u32(batch, p + 2) as usize;
            let key_at = p + RECORD_FIXED;
            let key = &batch[key_at..key_at + klen];
            let value = &batch[key_at + klen..key_at + klen + vlen];
            self.state.mem_insert(key, value)?;
            p += RECORD_FIXED + klen + vlen;
        }
        self.state.highest_revision = self.state.highest_revision.max(batch_max);
        Ok(ApplyResult {
            revision: batch_max,
            flush_wanted: self.state.mem_count as usize >= MEMTABLE_FLUSH_WATERMARK,
        })
    }

    /// Capture the full logical content (snapshot stream v1) in
    /// bounded chunks. The cursor is the count of records already
    /// emitted; the view stays stable because `apply`/`flush` are
    /// rejected with `Backpressure` while a capture is active.
    fn snapshot(
        &mut self,
        request: SnapshotRequest,
        cursor: u64,
        out: &mut [u8],
    ) -> Result<(usize, Progress), StoreError> {
        if self.state.install.active || self.state.compact.active || self.state.flush.active {
            return Err(StoreError::Backpressure);
        }
        if !self.state.capture.active {
            if cursor != 0 {
                return Err(StoreError::Malformed);
            }
            if request.range_generation != self.state.range_generation {
                return Err(StoreError::FenceMismatch);
            }
            // Count total records once (full fidelity: every surviving
            // version, tombstones included).
            let mut total = 0u64;
            let mut m = Merge::init(self.state, &*self.storage, true, b"")?;
            let mut kbuf = [0u8; MAX_ENCODED_KEY];
            let mut vbuf = [0u8; MAX_VALUE_LEN];
            while m
                .next(self.state, &*self.storage, &mut kbuf, &mut vbuf)?
                .is_some()
            {
                total += 1;
            }
            self.state.capture = CaptureState {
                active: true,
                header_sent: false,
                _pad: [0; 2],
                range_generation: request.range_generation,
                applied_index: request.applied_index,
                next_ordinal: 0,
                total_entries: total,
            };
        } else {
            if request.applied_index != self.state.capture.applied_index
                || request.range_generation != self.state.capture.range_generation
            {
                return Err(StoreError::FenceMismatch);
            }
            if cursor != self.state.capture.next_ordinal {
                return Err(StoreError::Malformed);
            }
        }
        let mut pos = 0usize;
        if !self.state.capture.header_sent {
            if out.len() < SNAP_HDR_LEN {
                return Err(StoreError::OutputTooSmall);
            }
            out[0..4].copy_from_slice(&SNAP_MAGIC.to_le_bytes());
            out[4..6].copy_from_slice(&SNAP_FORMAT_VERSION.to_le_bytes());
            out[6..8].copy_from_slice(&0u16.to_le_bytes());
            out[8..16].copy_from_slice(&self.state.capture.applied_index.to_le_bytes());
            out[16..20].copy_from_slice(&self.state.capture.range_generation.to_le_bytes());
            out[20..24].copy_from_slice(&(self.state.capture.total_entries as u32).to_le_bytes());
            out[24..32].copy_from_slice(&self.state.highest_revision.to_le_bytes());
            out[32..40].copy_from_slice(&self.state.compaction_floor.to_le_bytes());
            pos = SNAP_HDR_LEN;
            self.state.capture.header_sent = true;
        }
        // Re-walk the merge, skip the already-emitted ordinal, emit
        // whole records while they fit.
        let mut m = Merge::init(self.state, &*self.storage, true, b"")?;
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut ordinal = 0u64;
        loop {
            let Some((klen, vlen)) = m.next(self.state, &*self.storage, &mut kbuf, &mut vbuf)?
            else {
                self.state.capture.active = false;
                return Ok((pos, Progress::Done));
            };
            if ordinal < self.state.capture.next_ordinal {
                ordinal += 1;
                continue;
            }
            let need = RECORD_FIXED + klen + vlen;
            if pos + need > out.len() {
                if pos == 0 {
                    return Err(StoreError::OutputTooSmall);
                }
                return Ok((
                    pos,
                    Progress::InProgress {
                        cursor: self.state.capture.next_ordinal,
                    },
                ));
            }
            out[pos..pos + 2].copy_from_slice(&(klen as u16).to_le_bytes());
            out[pos + 2..pos + 6].copy_from_slice(&(vlen as u32).to_le_bytes());
            out[pos + 6..pos + 6 + klen].copy_from_slice(&kbuf[..klen]);
            out[pos + 6 + klen..pos + need].copy_from_slice(&vbuf[..vlen]);
            pos += need;
            ordinal += 1;
            self.state.capture.next_ordinal = ordinal;
        }
    }

    /// Install an authorized snapshot (REPLACE semantics): the first
    /// chunk destroys local state, the stream is re-validated record
    /// by record, and any inconsistency aborts to an EMPTY store —
    /// a partial install is never mistaken for a good one.
    fn install(
        &mut self,
        request: SnapshotRequest,
        chunk: SnapshotChunk<'_>,
    ) -> Result<Progress, StoreError> {
        if self.state.capture.active || self.state.compact.active || self.state.flush.active {
            return Err(StoreError::Backpressure);
        }
        if !self.state.install.active {
            if chunk.offset != 0 {
                return Err(StoreError::Malformed);
            }
            if request.range_generation != self.state.range_generation {
                return Err(StoreError::FenceMismatch);
            }
            // Destroy the local materialization (files first: a crash
            // mid-install must recover to "no usable manifest" =
            // empty, forcing a re-install rather than serving a mix).
            let mut ids = [0u32; MAX_MANIFEST_FILES];
            let n = self
                .storage
                .list(FileKind::Manifest, &mut ids)
                .map_err(map_storage)?;
            for &id in ids.iter().take(n) {
                match self.storage.delete(FileKind::Manifest, id) {
                    Ok(()) | Err(StorageError::NotFound) => {}
                    Err(e) => return Err(map_storage(e)),
                }
            }
            let mut rids = [0u32; MAX_LISTED_RUNS];
            let rn = self
                .storage
                .list(FileKind::Run, &mut rids)
                .map_err(map_storage)?;
            for &id in rids.iter().take(rn) {
                match self.storage.delete(FileKind::Run, id) {
                    Ok(()) | Err(StorageError::NotFound) => {}
                    Err(e) => return Err(map_storage(e)),
                }
            }
            let range_gen = self.state.range_generation;
            self.state.init();
            self.state.range_generation = range_gen;
            self.state.install.active = true;
            self.state.install.header_done = false;
            self.state.install.range_generation = request.range_generation;
            self.state.install.applied_index = request.applied_index;
            self.state.install.expected_offset = 0;
            self.state.install.entry_count = 0;
            self.state.install.entries_done = 0;
            self.state.install.partial_len = 0;
        } else {
            if request.applied_index != self.state.install.applied_index
                || request.range_generation != self.state.install.range_generation
            {
                self.install_abort();
                return Err(StoreError::FenceMismatch);
            }
            if chunk.offset != self.state.install.expected_offset {
                self.install_abort();
                return Err(StoreError::Malformed);
            }
        }
        // Stream bytes through the partial-record staging buffer.
        let mut in_pos = 0usize;
        loop {
            if !self.state.install.header_done {
                let have = self.state.install.partial_len as usize;
                let take = (SNAP_HDR_LEN - have).min(chunk.data.len() - in_pos);
                self.state.install.partial[have..have + take]
                    .copy_from_slice(&chunk.data[in_pos..in_pos + take]);
                self.state.install.partial_len += take as u32;
                in_pos += take;
                if (self.state.install.partial_len as usize) < SNAP_HDR_LEN {
                    break; // need more bytes
                }
                let mut hdr = [0u8; SNAP_HDR_LEN];
                hdr.copy_from_slice(&self.state.install.partial[..SNAP_HDR_LEN]);
                if rd_u32(&hdr, 0) != SNAP_MAGIC || rd_u16(&hdr, 4) != SNAP_FORMAT_VERSION {
                    self.install_abort();
                    return Err(StoreError::Malformed);
                }
                if rd_u64(&hdr, 8) != self.state.install.applied_index
                    || rd_u32(&hdr, 16) != self.state.install.range_generation
                {
                    self.install_abort();
                    return Err(StoreError::FenceMismatch);
                }
                self.state.install.entry_count = rd_u32(&hdr, 20);
                self.state.install.snap_highest_revision = rd_u64(&hdr, 24);
                self.state.install.snap_floor = rd_u64(&hdr, 32);
                self.state.install.header_done = true;
                self.state.install.partial_len = 0;
                continue;
            }
            // Records. Stage the fixed prefix first, then the body.
            let have = self.state.install.partial_len as usize;
            if have < RECORD_FIXED {
                let take = (RECORD_FIXED - have).min(chunk.data.len() - in_pos);
                self.state.install.partial[have..have + take]
                    .copy_from_slice(&chunk.data[in_pos..in_pos + take]);
                self.state.install.partial_len += take as u32;
                in_pos += take;
                if (self.state.install.partial_len as usize) < RECORD_FIXED {
                    break;
                }
            }
            let klen = rd_u16(&self.state.install.partial, 0) as usize;
            let vlen = rd_u32(&self.state.install.partial, 2) as usize;
            if !(MIN_ENCODED_KEY..=MAX_ENCODED_KEY).contains(&klen) || vlen > MAX_VALUE_LEN {
                self.install_abort();
                return Err(StoreError::Malformed);
            }
            let rec_len = RECORD_FIXED + klen + vlen;
            let have = self.state.install.partial_len as usize;
            let take = (rec_len - have).min(chunk.data.len() - in_pos);
            self.state.install.partial[have..have + take]
                .copy_from_slice(&chunk.data[in_pos..in_pos + take]);
            self.state.install.partial_len += take as u32;
            in_pos += take;
            if (self.state.install.partial_len as usize) < rec_len {
                break;
            }
            // One complete record: re-validate the key encoding, then
            // insert (flushing to a run when the memtable fills).
            {
                let mut user_key = [0u8; MAX_ENCODED_KEY];
                let key_ok = internal_key::decode(
                    &self.state.install.partial[RECORD_FIXED..RECORD_FIXED + klen],
                    &mut user_key,
                )
                .is_some();
                if !key_ok {
                    self.install_abort();
                    return Err(StoreError::Malformed);
                }
            }
            if self.state.mem_count as usize >= MEMTABLE_MAX_ENTRIES {
                if let Err(e) = self.flush_inner() {
                    self.install_abort();
                    return Err(e);
                }
            }
            // Copy out of the staging buffer to release the borrow.
            let mut rec = [0u8; MAX_RECORD_LEN];
            rec[..rec_len].copy_from_slice(&self.state.install.partial[..rec_len]);
            let (key, value) = rec[RECORD_FIXED..rec_len].split_at(klen);
            if let Err(e) = self.state.mem_insert(key, value) {
                self.install_abort();
                return Err(e);
            }
            self.state.install.entries_done += 1;
            self.state.install.partial_len = 0;
            if in_pos >= chunk.data.len() {
                break;
            }
        }
        self.state.install.expected_offset += chunk.data.len() as u64;
        if chunk.last {
            let complete = self.state.install.header_done
                && self.state.install.partial_len == 0
                && self.state.install.entries_done == self.state.install.entry_count;
            if !complete {
                self.install_abort();
                return Err(StoreError::Malformed);
            }
            self.state.highest_revision = self.state.install.snap_highest_revision;
            self.state.compaction_floor = self.state.install.snap_floor;
            self.state.install.active = false;
            return Ok(Progress::Done);
        }
        Ok(Progress::InProgress {
            cursor: self.state.install.expected_offset,
        })
    }

    /// Merge every run into one, dropping (a) versions shadowed by a
    /// newer version at-or-below `floor` and (b) tombstones at-or-below
    /// `floor` with nothing beneath them. Bounded: each call consumes
    /// at most `COMPACT_STEP_RECORDS` input records and writes only
    /// complete blocks; the cursor is the count of input records
    /// already decided. Shadowing considers RUN content only (a
    /// memtable version additionally shadowing run versions is left
    /// for the next cycle — conservative, never wrong). The floor is
    /// trusted, never computed (§18).
    fn compact(&mut self, floor: Revision, cursor: u64) -> Result<Progress, StoreError> {
        if self.state.install.active || self.state.capture.active || self.state.flush.active {
            return Err(StoreError::Backpressure);
        }
        if !self.state.compact.active {
            if cursor != 0 {
                return Err(StoreError::Malformed);
            }
            if floor < self.state.compaction_floor {
                return Err(StoreError::Malformed); // floors never retreat
            }
            if self.state.run_count == 0 {
                self.state.compaction_floor = floor;
                return Ok(Progress::Done);
            }
            let id = self.state.next_run_id.max(1);
            create_fresh(self.storage, FileKind::Run, id)?;
            let mut crc_state = CRC32_INIT;
            let mut file_len = 0u64;
            if let Err(e) = write_run_header(self.storage, id, &mut crc_state, &mut file_len) {
                self.state.compact.active = false;
                return Err(e);
            }
            self.state.next_run_id = id + 1;
            self.state.compact = CompactState {
                active: true,
                phase: COMPACT_PHASE_MERGE,
                _pad: [0; 2],
                ticket: 0,
                pending_gen: 0,
                pending_slot: 0,
                pending_len: 0,
                pending_crc: 0,
                out_run_id: id,
                floor,
                input_ordinal: 0,
                out_records: 0,
                out_blocks: 0,
                out_len: file_len,
                crc_state,
                _pad2: 0,
                smallest_off: 0,
                largest_off: 0,
                bloom: [0u8; BLOOM_BYTES],
                index: [0u8; RUN_INDEX_BYTES],
                blk_first_key: [0u8; INDEX_KEY_BYTES],
                blk_has_first: false,
                resume_valid: false,
                run_block: [0; MAX_RUNS],
                run_rec: [0; MAX_RUNS],
                run_pos_count: 0,
                carry_kept_below_floor: false,
                carry_prefix_len: 0,
                carry_prefix: [0u8; MAX_ENCODED_KEY],
            };
        } else {
            if floor != self.state.compact.floor {
                self.state.compact.active = false;
                return Err(StoreError::Malformed);
            }
            if cursor != self.state.compact.input_ordinal {
                self.state.compact.active = false;
                return Err(StoreError::Malformed);
            }
        }
        // A fenced tail is outstanding: poll it instead of re-walking
        // the merge. Same submit/poll split as the flush, same reason —
        // the publish fsync is far too long for one step.
        if self.state.compact.phase != COMPACT_PHASE_MERGE {
            let polled = match self.state.compact.phase {
                COMPACT_PHASE_FOOTER => self.compact_finalize(),
                COMPACT_PHASE_RUN_SYNC => self.compact_poll_run(),
                COMPACT_PHASE_MAN_WRITE => self.compact_manifest_write(),
                _ => self.compact_poll_manifest(),
            };
            return match polled {
                Ok(p) => Ok(p),
                Err(e) => {
                    self.state.compact.active = false;
                    self.state.compact.phase = COMPACT_PHASE_MERGE;
                    Err(e)
                }
            };
        }
        // One bounded step. Re-walk the run-only merge from the start:
        // records before the resume point are RE-DECIDED (to rebuild
        // the per-prefix shadowing context) but not re-emitted.
        let step_result = self.compact_step();
        match step_result {
            Ok(p) => Ok(p),
            Err(e) => {
                // Fail closed: drop the half-written output run (it
                // becomes an orphan for recover()) and reset.
                self.state.compact.active = false;
                Err(e)
            }
        }
    }
}

impl<S: RunStorage> DiskStore<'_, S> {
    fn compact_step(&mut self) -> Result<Progress, StoreError> {
        let floor = self.state.compact.floor;
        let resume = self.state.compact.input_ordinal;
        // Resume by SEEKING to the saved per-run positions (O(runs)),
        // never by replaying the merge from record 0 (O(resume) per
        // step, O(n²) total — the shape that outgrew the module step
        // deadline on FAT32 and got the worker terminated).
        let resuming = self.state.compact.resume_valid;
        if !resuming && resume != 0 {
            return Err(StoreError::StorageFault);
        }
        let mut m = if resuming {
            Merge::resume_compact(self.state, &*self.storage)?
        } else {
            Merge::init(self.state, &*self.storage, false, b"")?
        };
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut prefix = [0u8; MAX_ENCODED_KEY];
        let mut prefix_len = 0usize;
        let mut kept_below_floor = false;
        if resuming {
            prefix_len = self.state.compact.carry_prefix_len as usize;
            prefix[..prefix_len].copy_from_slice(&self.state.compact.carry_prefix[..prefix_len]);
            kept_below_floor = self.state.compact.carry_kept_below_floor;
        }
        let mut processed = resume;
        let mut block = [0u8; BLOCK_MAX_PAYLOAD];
        let mut fill = 0usize;
        let mut block_recs = 0u32;
        loop {
            let next = m.next(self.state, &*self.storage, &mut kbuf, &mut vbuf)?;
            let Some((klen, vlen)) = next else {
                // Input exhausted: close the last block, then hand the
                // footer + fsync + publish tail to its own phases —
                // each is a separate step with its own deadline
                // budget.
                if fill > 0 {
                    self.compact_write_block(&block[..fill], block_recs)?;
                }
                self.state.compact.resume_valid = false;
                self.state.compact.phase = COMPACT_PHASE_FOOTER;
                return Ok(Progress::InProgress {
                    cursor: self.state.compact.input_ordinal,
                });
            };
            if klen < MIN_ENCODED_KEY {
                return Err(StoreError::StorageFault);
            }
            let plen = klen - VERSION_SUFFIX_LEN;
            if plen != prefix_len || kbuf[..plen] != prefix[..prefix_len] {
                prefix[..plen].copy_from_slice(&kbuf[..plen]);
                prefix_len = plen;
                kept_below_floor = false;
            }
            let ts = key_timestamp(&kbuf[..klen]);
            let keep = if ts > floor {
                true
            } else if !kept_below_floor {
                kept_below_floor = true;
                // Rule (b): the newest at-or-below-floor version being
                // a tombstone means nothing survives beneath it.
                key_kind_byte(&kbuf[..klen]) != ValueKind::PointTombstone as u8
            } else {
                false // rule (a): shadowed below the floor
            };
            processed += 1;
            if keep {
                let rec_len = RECORD_FIXED + klen + vlen;
                if fill > 0 && fill + rec_len > BLOCK_TARGET {
                    self.compact_write_block(&block[..fill], block_recs)?;
                    fill = 0;
                    block_recs = 0;
                }
                if !self.state.compact.blk_has_first {
                    let n = klen.min(INDEX_KEY_BYTES);
                    let mut k = [0u8; INDEX_KEY_BYTES];
                    k[..n].copy_from_slice(&kbuf[..n]);
                    self.state.compact.blk_first_key = k;
                    self.state.compact.blk_has_first = true;
                }
                copy_at(&mut block, fill, &(klen as u16).to_le_bytes());
                copy_at(&mut block, fill + 2, &(vlen as u32).to_le_bytes());
                copy_at(&mut block, fill + 6, &kbuf[..klen]);
                copy_at(&mut block, fill + 6 + klen, &vbuf[..vlen]);
                // Same incremental filter build as the flush path: the
                // merged output run gets its own filter, one key at a
                // time, so no extra pass is ever needed.
                bloom_insert(
                    &mut self.state.compact.bloom,
                    &kbuf[..klen.saturating_sub(VERSION_SUFFIX_LEN)],
                );
                fill += rec_len;
                block_recs += 1;
                self.state.compact.out_records += 1;
            }
            if processed - resume >= COMPACT_STEP_RECORDS {
                // Step budget spent: close the partial block so no
                // block state needs to persist across steps, and save
                // the exact merge positions + dedup-walk context so
                // the next step SEEKS here instead of replaying.
                if fill > 0 {
                    self.compact_write_block(&block[..fill], block_recs)?;
                }
                self.state.compact.input_ordinal = processed;
                self.state.compact.resume_valid = true;
                self.state.compact.run_pos_count = m.run_count as u8;
                for i in 0..m.run_count {
                    let (b, r) = m.runs[i].save_pos();
                    self.state.compact.run_block[i] = b;
                    self.state.compact.run_rec[i] = r;
                }
                self.state.compact.carry_prefix_len = prefix_len as u16;
                self.state.compact.carry_prefix[..prefix_len]
                    .copy_from_slice(&prefix[..prefix_len]);
                self.state.compact.carry_kept_below_floor = kept_below_floor;
                return Ok(Progress::InProgress { cursor: processed });
            }
        }
    }

    fn compact_write_block(&mut self, payload: &[u8], rec_count: u32) -> Result<(), StoreError> {
        let mut crc_state = self.state.compact.crc_state;
        let mut file_len = self.state.compact.out_len;
        let off = write_run_block(
            self.storage,
            self.state.compact.out_run_id,
            payload,
            rec_count,
            &mut crc_state,
            &mut file_len,
        )?;
        if self.state.compact.out_blocks == 0 {
            self.state.compact.smallest_off = off;
        }
        self.state.compact.largest_off = off;
        {
            let ordinal = self.state.compact.out_blocks;
            let first = self.state.compact.blk_first_key;
            idx_push(&mut self.state.compact.index, ordinal, off, &first);
        }
        self.state.compact.blk_has_first = false;
        self.state.compact.out_blocks += 1;
        self.state.compact.crc_state = crc_state;
        self.state.compact.out_len = file_len;
        Ok(())
    }

    /// Close the output run and SUBMIT its durability fence. Nothing is
    /// published here — the manifest is not written until the run is
    /// proven durable, and the inputs are not deleted until the
    /// manifest is.
    fn compact_finalize(&mut self) -> Result<Progress, StoreError> {
        let c = self.state.compact;
        if c.out_records == 0 {
            // Everything compacted away: no output run at all, so there
            // is no run fence to wait on. Go straight to the manifest.
            match self.storage.delete(FileKind::Run, c.out_run_id) {
                Ok(()) | Err(StorageError::NotFound) => {}
                Err(e) => return Err(map_storage(e)),
            }
            self.state.compact.pending_gen = self.state.generation + 1;
            self.state.compact.phase = COMPACT_PHASE_MAN_WRITE;
            return Ok(Progress::InProgress {
                cursor: c.input_ordinal,
            });
        }
        let (final_len, file_crc) = write_run_footer(
            self.storage,
            c.out_run_id,
            c.out_records,
            c.out_blocks,
            c.smallest_off,
            c.largest_off,
            &self.state.compact.bloom,
            &self.state.compact.index,
            c.crc_state,
            c.out_len,
        )?;
        let ticket = self
            .storage
            .fsync_submit(FileKind::Run, c.out_run_id)
            .map_err(map_storage)?;
        self.state.compact.ticket = ticket;
        self.state.compact.pending_len = final_len;
        self.state.compact.pending_crc = file_crc;
        self.state.compact.phase = COMPACT_PHASE_RUN_SYNC;
        Ok(Progress::InProgress {
            cursor: c.input_ordinal,
        })
    }

    fn compact_poll_run(&mut self) -> Result<Progress, StoreError> {
        let c = self.state.compact;
        match self
            .storage
            .fsync_poll(FileKind::Run, c.out_run_id, c.ticket)
            .map_err(map_storage)?
        {
            FsyncState::Pending => {
                return Ok(Progress::InProgress {
                    cursor: c.input_ordinal,
                })
            }
            FsyncState::Durable => {}
        }
        self.state.compact.pending_gen = self.state.generation + 1;
        self.state.compact.phase = COMPACT_PHASE_MAN_WRITE;
        Ok(Progress::InProgress {
            cursor: c.input_ordinal,
        })
    }

    fn compact_manifest_write(&mut self) -> Result<Progress, StoreError> {
        let c = self.state.compact;
        let slot = self.state.inactive_manifest_slot();
        let ticket = if c.out_records == 0 {
            Self::manifest_write_submit(
                self.storage,
                slot,
                c.pending_gen,
                &[],
                c.floor,
                self.state.durable_applied_index,
                self.state.durable_applied_term,
                self.state.durable_highest_revision,
            )?
        } else {
            let meta = RunMeta {
                id: c.out_run_id,
                record_count: c.out_records,
                file_len: c.pending_len,
                digest: c.pending_crc,
                _pad: 0,
            };
            Self::manifest_write_submit(
                self.storage,
                slot,
                c.pending_gen,
                &[meta],
                c.floor,
                self.state.durable_applied_index,
                self.state.durable_applied_term,
                self.state.durable_highest_revision,
            )?
        };
        self.state.compact.pending_slot = slot;
        self.state.compact.ticket = ticket;
        self.state.compact.phase = COMPACT_PHASE_MAN_SYNC;
        Ok(Progress::InProgress {
            cursor: c.input_ordinal,
        })
    }

    /// The compaction interlock, mirroring the flush's: the merged-away
    /// INPUT runs are deleted only once the manifest that stops
    /// referencing them is itself durable. Deleting them against a
    /// merely-submitted fence would, on a crash, leave the old manifest
    /// as truth while pointing at runs that no longer exist —
    /// quarantine on the next boot.
    fn compact_poll_manifest(&mut self) -> Result<Progress, StoreError> {
        let c = self.state.compact;
        match self
            .storage
            .fsync_poll(FileKind::Manifest, c.pending_slot, c.ticket)
            .map_err(map_storage)?
        {
            FsyncState::Pending => {
                return Ok(Progress::InProgress {
                    cursor: c.input_ordinal,
                })
            }
            FsyncState::Durable => {}
        }
        let old_count = self.state.run_count as usize;
        let old_runs = self.state.runs;
        self.state.runs = [RunMeta::empty(); MAX_RUNS];
        self.state.run_blooms = [[0u8; BLOOM_BYTES]; MAX_RUNS];
        self.state.run_index = [[0u8; RUN_INDEX_BYTES]; MAX_RUNS];
        if c.out_records == 0 {
            self.state.run_count = 0;
        } else {
            self.state.run_blooms[0] = c.bloom;
            self.state.run_index[0] = self.state.compact.index;
            self.state.runs[0] = RunMeta {
                id: c.out_run_id,
                record_count: c.out_records,
                file_len: c.pending_len,
                digest: c.pending_crc,
                _pad: 0,
            };
            self.state.run_count = 1;
        }
        self.state.generation = c.pending_gen;
        self.state.manifest_slot = c.pending_slot;
        self.state.compaction_floor = c.floor;
        self.state.compact.active = false;
        self.state.compact.phase = COMPACT_PHASE_MERGE;
        // The merged-away inputs are unreferenced now. QUEUE their
        // deletion instead of doing it here: each FAT32 delete walks
        // and frees the file's whole cluster chain synchronously, and
        // several of them in this one step is exactly the deadline
        // overrun that used to kill the worker at manifest adoption.
        // The maintenance loop drains one per step; a crash merely
        // leaves orphans for recovery.
        for meta in old_runs.iter().take(old_count) {
            if c.out_records != 0 && meta.id == c.out_run_id {
                continue;
            }
            if (self.state.retire_count as usize) < MAX_RUNS {
                self.state.retire_ids[self.state.retire_count as usize] = meta.id;
                self.state.retire_count += 1;
            }
            // A full queue would only mean deletions are outpaced by
            // compactions, which the one-per-step drain plus the
            // 3-run merge threshold makes impossible; dropping the id
            // would still only orphan a file for recovery.
        }
        Ok(Progress::Done)
    }

    /// Physically delete ONE queued retired run file. Returns whether
    /// a deletion was performed. Bounded on purpose: one FAT-chain
    /// free per module step.
    pub fn retire_step(&mut self) -> Result<bool, StoreError> {
        if self.state.retire_count == 0 {
            return Ok(false);
        }
        self.state.retire_count -= 1;
        let id = self.state.retire_ids[self.state.retire_count as usize];
        match self.storage.delete(FileKind::Run, id) {
            Ok(()) | Err(StorageError::NotFound) => Ok(true),
            Err(e) => {
                // Put it back; the next step retries. A permanently
                // undeletable file degrades to an orphan, not a fault.
                self.state.retire_ids[self.state.retire_count as usize] = id;
                self.state.retire_count += 1;
                Err(map_storage(e))
            }
        }
    }
}
