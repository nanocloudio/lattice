//! Ordered disk state-store provider.
//!
//! Log-structured, single level, v1: one bounded sorted memtable plus a
//! bounded set of immutable sorted run files named by a versioned,
//! checksummed, generation-numbered manifest. All I/O goes through the
//! `RunStorage` trait (`run_storage.rs`) so the identical provider
//! logic compiles into the no_std PIC module (Fluxor FS opcodes) and
//! into host tests (`std::fs`). Clustor's per-partition WAL remains the
//! sole authoritative logical WAL: every file written here is a
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
//! run's blocks sequentially — `Merge::fill_filtered` opens each run and reads
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
//!         [highest_revision:u64][committed_clock_ms:u64]           (60 B)
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
//! is exactly the guarantee a delete-last scheme provides, obtained
//! without deleting anything. The new manifest is written only after
//! the run it names is proven durable, so whichever slot recovery
//! picks, every run it references is on media.
//!
//! Recovery (`recover()`): the valid slot with the
//! highest internal generation wins; runs it does not reference are
//! orphans and are deleted; a referenced-but-missing or corrupt run
//! means QUARANTINE — `StoreError::StorageFault`, never a guess.
//!
//! ## Snapshot stream format v1 (little-endian)
//!
//! ```text
//! header  [magic:u32 = "LSNP"][format:u16 = 1][flags:u16 = 0]
//!         [applied_index:u64][range_generation:u32][entry_count:u32]
//!         [highest_revision:u64][compaction_floor:u64]
//!         [committed_clock_ms:u64]                                 (48 B)
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

use core::cell::{Cell, UnsafeCell};
use internal_key::{ValueKind, IDENTITY_PREFIX_LEN, VERSION_SUFFIX_LEN};
use run_storage::{FileKind, FsyncState, FsyncTicket, RunStorage, StorageError};
use state_store::{
    ApplyResult, KeySpan, KvStateStore, Progress, Revision, ScanProgress, SnapshotChunk,
    SnapshotRequest, StoreError,
};

// ── Capacities ────────────────────────────────────────────────────────

/// The longest key the provider is handed: `kv_store::MAX_KEY_LEN` (this
/// file is also mounted alone; a contract test holds the two equal).
pub const MAX_USER_KEY: usize = 2584;
/// Maximum encoded internal key length the provider stores: a user key
/// of [`MAX_USER_KEY`] bytes at worst-case escaping. Longer keys are
/// rejected `Malformed` — fail closed, never truncate.
pub const MAX_ENCODED_KEY: usize = internal_key::encoded_max_len(MAX_USER_KEY);
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
/// The run header's format tag: a sanity check that fails a garbage file
/// closed, not a compatibility axis — the layout changes in place.
pub const RUN_FORMAT_VERSION: u16 = 1;

// ── Per-run Bloom filter ──────────────────────────────────────────────
//
// WHY. Without the filter a point lookup costs a sequential block
// scan of every run: `Merge::fill_filtered` opens each run and reads forward
// until it reaches
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
//
// DENSITY. An API server's keys share long prefixes — `/events/default/
// ...`, `/replicasets.apps/default/...` — so an index whose entries keep
// too few key bytes ties across a whole region, the seek lands at the
// region's head, and a lookup walks it: with 32 entries of 20 escaped
// user-key bytes, one event in a 3,000-event run cost 141 blocks, every
// block a synchronous device read on bare metal. 256 entries of 64
// escaped user-key bytes put the same lookup at one block.
/// Entries in one run's sparse index.
pub const SPARSE_INDEX_ENTRIES: usize = 256;
/// Bytes of the encoded key stored per entry. `IDENTITY_PREFIX_LEN` is
/// 12, so this keeps 64 bytes of escaped user key.
pub const INDEX_KEY_BYTES: usize = 76;
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
/// The `[payload_len:4][record_count:4]` header in front of a block's
/// payload.
const BLOCK_HDR: usize = 8;
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
/// See the module-doc layout: the fixed fields through
/// `floor`, then `[applied_index:u64][applied_term:u64]` — the raft
/// position DURABLY covered by the named run set (the applied position
/// at the freeze of the newest flushed run). This is what lets the WAL
/// compact honestly: entries at or below it are reconstructible from
/// the runs alone.
pub const MANIFEST_HDR_LEN: usize = 60;
/// `[run_id:4][record_count:4][file_len:8][digest:4]`
pub const MANIFEST_ENTRY_LEN: usize = 20;
pub const MANIFEST_MAX_LEN: usize = MANIFEST_HDR_LEN + MAX_RUNS * MANIFEST_ENTRY_LEN + 4;
/// Recovery list buffer for manifest ids; steady state is 1 (plus a
/// possible torn newer generation).
pub const MAX_MANIFEST_FILES: usize = 8;
/// Recovery list buffer for run ids: live runs plus orphans. Orphans are
/// what a crash leaves of retirements not yet drained; recovery deletes
/// them, and it must see them all to do so (a listing that overflows is a
/// fault, never a truncation), so the bound is generous: it costs 4 KB of
/// stack, once, at recovery.
pub const MAX_LISTED_RUNS: usize = 1024;
/// Orphans one overflowed recovery queues for deletion. The queue outlives
/// a recovery attempt: a deleted the FS defers (E_AGAIN — on a large FAT32
/// root an unlink's directory walk spans steps) is asked again by the next
/// attempt BEFORE anything is listed, because a listing between would walk
/// the same directory and the deferred unlink would never finish.
pub const SWEEP_BATCH: usize = 128;

// ── Snapshot stream format v1 ─────────────────────────────────────────

/// "LSNP" — snapshot stream magic.
pub const SNAP_MAGIC: u32 = 0x4C53_4E50;
pub const SNAP_FORMAT_VERSION: u16 = 1;
/// See the module-doc layout (48 bytes).
pub const SNAP_HDR_LEN: usize = 48;
/// Byte offset of the committed clock frontier in a snapshot stream
/// header. Record deadlines are absolute against it, so a receiver
/// that installs the body without adopting it resurrects everything
/// that had already expired.
pub const SNAP_HDR_CLOCK_OFF: usize = 40;
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

/// Records the recovery walk (rebuilding `highest_revision`) visits per
/// `recover_step`; it resumes at the key it reached. Before recovery was
/// stepped the whole walk ran inside one call — on a warm cache the
/// whole store in one module step.
pub const RECOVER_WALK_RECORDS: u32 = 512;

/// 4 KiB chunks of a run recovery verifies per `recover_step` (256 KiB);
/// the CRC and position carry across steps.
pub const VERIFY_STEP_CHUNKS: u32 = 64;

// ── Recovery phases (`RecoverState::phase`) ───────────────────────────
/// Not started (or finished): the next `recover_step` begins afresh.
const REC_IDLE: u8 = 0;
/// Finishing a sweep an earlier attempt queued.
const REC_SWEEP: u8 = 1;
/// Listing the run files.
const REC_LIST: u8 = 2;
/// The listing overflowed: queueing the unreferenced listed runs.
const REC_OVERFLOW: u8 = 3;
/// Choosing the manifest slot.
const REC_SELECT: u8 = 4;
/// Verifying the chosen slot's runs.
const REC_VERIFY: u8 = 5;
/// Deleting the runs no manifest names.
const REC_ORPHANS: u8 = 6;
/// Loading each run's filter and index.
const REC_FILTERS: u8 = 7;
/// Walking the records for the revision high-water.
const REC_WALK: u8 = 8;

// `RecoverState::vstage`: a run's verification.
/// Its header (and minimum length).
const VERIFY_HEADER: u8 = 0;
/// The rolling CRC over its body.
const VERIFY_BODY: u8 = 1;
/// The stored CRC, the exact end, the footer framing.
const VERIFY_TAIL: u8 = 2;

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

// Stages inside a phase. Every stage is ONE storage action that may
// answer `Again`; a stage that does is asked again, as it stands, on a
// later step — the phase and stage stay where they are, and nothing
// already done is done twice. The phase numbers above stay what
// diagnostics and tests read; the stages are the finer position.
//
// `FLUSH_PHASE_RECORDS` / `COMPACT_PHASE_MERGE`:
/// Create the output run file (`create_fresh`).
const STAGE_CREATE: u8 = 0;
/// Append the run header.
const STAGE_HEADER: u8 = 1;
/// Records / merge into blocks.
const STAGE_BODY: u8 = 2;
// `FLUSH_PHASE_RECORDS` (after the body) / `COMPACT_PHASE_FOOTER`:
/// Append the footer, part by part (`part`: 0 head, 1 bloom, 2 index,
/// 3 file CRC).
const STAGE_FOOTER: u8 = 3;
/// Submit the run's durability fence.
const STAGE_RUN_SUBMIT: u8 = 4;
// `*_PHASE_MAN_WRITE`:
/// Write the manifest slot in place.
const STAGE_MAN_WRITE: u8 = 0;
/// Submit its durability fence.
const STAGE_MAN_SUBMIT: u8 = 1;
/// (first, after the run's fence) Publish the run's NAME durably: a
/// manifest may name only a run a power cut cannot lose by name.
const STAGE_RUN_NAME: u8 = 2;
// `*_PHASE_MAN_SYNC`:
/// Poll the manifest's fence.
const STAGE_MAN_POLL: u8 = 0;
/// Fenced; publish the slot's name when this publish created the slot.
const STAGE_MAN_NAME: u8 = 1;
/// `prep_stage`: a manifest slot being prepared is created, then
/// zero-filled, then fenced.
const PREP_CREATE: u8 = 0;
const PREP_ZERO: u8 = 1;
const PREP_SYNC: u8 = 2;
/// … and its name published.
const PREP_NAME: u8 = 3;
/// `InstallState::destroy`: listing then deleting the manifests, then
/// the runs (0 = done).
const DESTROY_LIST_MAN: u8 = 1;
const DESTROY_DEL_MAN: u8 = 2;
const DESTROY_LIST_RUN: u8 = 3;
/// … with the manifests' removal published between (slot A, then B).
const DESTROY_NAME_A: u8 = 5;
const DESTROY_NAME_B: u8 = 6;
const DESTROY_DEL_RUN: u8 = 4;

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

/// The slicing-by-8 tables: `[k][b]` is the CRC of byte `b` followed by
/// `k` zero bytes, so eight bytes fold in one step. Every run block is
/// verified on every load, and a byte at a time the check was most of a
/// point read's cost; eight at a time, the same CRC for a fraction.
const fn crc32_tables8() -> [[u32; 256]; 8] {
    let t0 = crc32_table();
    let mut t = [[0u32; 256]; 8];
    t[0] = t0;
    let mut i = 0;
    while i < 256 {
        let mut k = 1;
        while k < 8 {
            let prev = t[k - 1][i];
            t[k][i] = (prev >> 8) ^ t0[(prev & 0xFF) as usize];
            k += 1;
        }
        i += 1;
    }
    t
}

static CRC32_T8: [[u32; 256]; 8] = crc32_tables8();

/// Rolling CRC32 state seed.
pub const CRC32_INIT: u32 = 0xFFFF_FFFF;

/// Fold `data` into a rolling CRC32 state (start from `CRC32_INIT`,
/// finish with `crc32_finish`): eight bytes a step, then the rest one at
/// a time.
pub fn crc32_update(mut state: u32, data: &[u8]) -> u32 {
    let t = &CRC32_T8;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        let w: [u8; 8] = c.try_into().unwrap_or([0; 8]);
        let lo = state ^ u32::from_le_bytes([w[0], w[1], w[2], w[3]]);
        let hi = u32::from_le_bytes([w[4], w[5], w[6], w[7]]);
        state = t[7][(lo & 0xFF) as usize]
            ^ t[6][((lo >> 8) & 0xFF) as usize]
            ^ t[5][((lo >> 16) & 0xFF) as usize]
            ^ t[4][(lo >> 24) as usize]
            ^ t[3][(hi & 0xFF) as usize]
            ^ t[2][((hi >> 8) & 0xFF) as usize]
            ^ t[1][((hi >> 16) & 0xFF) as usize]
            ^ t[0][(hi >> 24) as usize];
    }
    for &b in chunks.remainder() {
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

fn map_storage(e: StorageError) -> StoreError {
    match e {
        // Not a fault: the provider continues the call next step.
        StorageError::Again => StoreError::Backpressure,
        _ => StoreError::StorageFault,
    }
}

/// A manifest slot's bit in `DiskState::slot_unnamed`.
fn slot_bit(slot: u32) -> u8 {
    if slot == MANIFEST_SLOT_A {
        1
    } else {
        2
    }
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
        // `Again` maps to Backpressure: the flush is not started, stays
        // wanted, and the next step asks the same create again — which is
        // what lets a provider's saved directory scan continue.
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

/// Blocks the version scan may CROSS in one call before it pauses
/// (`ScanProgress::paused`): loaded from a run file or served by the
/// block cache alike. This is the step bound for a walk whose length is
/// otherwise proportional to the stored keyspan: a `(from, to]` window is
/// answered by walking the whole span in key order and filtering by
/// timestamp, so the records that qualify can be arbitrarily far apart.
/// A cached block skips the storage call but not the copy, the CRC check
/// and the record decode, so a walk over a cached run is bounded the
/// same way — otherwise a run the cache holds whole would be walked in
/// one call.
///
/// Sized from the pi5 rig: a block from storage is ONE speculative FS
/// read (header, payload and CRC together, ~190 us on FAT32/NVMe), so two
/// blocks is ~0.4 ms of I/O — comfortably inside the worker's 1.5 ms
/// drain budget with the rest of the step's work. Resume is free in the
/// steady state (the merge is arena-resident and continues in place),
/// so the budget is not paying a re-positioning tax; when the run set
/// has changed underneath a paused scan the rebuild costs one indexed
/// block per run on top.
pub const SCAN_STEP_BLOCKS: usize = 2;

/// Records the version scan may VISIT (read from any source, whether
/// or not they qualify) in one call before it pauses. Bounds the
/// memtable-resident part of a walk, which loads no blocks and would
/// otherwise escape `SCAN_STEP_BLOCKS`: a full memtable is
/// `MEMTABLE_MAX_ENTRIES` compares, tens of microseconds.
pub const SCAN_STEP_RECORDS: u32 = MEMTABLE_MAX_ENTRIES as u32;

/// Older versions of one key a current-value range walk passes over in a
/// row before it seeks past the key. Retained history keeps every version
/// of a key, newest first, so a key rewritten often (a node's lease) is a
/// run of records the walk would read block by block only to skip; past
/// this many the walk re-seeks every source through its sparse index to
/// the key's last possible version. Below it the versions sit in the block
/// already loaded and the seek would cost more than it saves.
pub const SKIP_STALE_VERSIONS: usize = 4;

/// Bytes of transcoded reply page the hosting engine may park across a
/// pause (`kv_store::scan_versions_op`). One command reply buffer.
pub const SCAN_PAGE_MAX: usize = 8192;

/// Resumable range-scan bookkeeping — the position memo behind
/// [`KvStateStore::scan_at`], the read every SQL scan, RANGE op and
/// snapshot page goes through.
///
/// A page's cursor is an ordinal (visible entries already emitted
/// since the span start). Without a memo every page re-walks from the
/// span start and skip-counts to its cursor, so a full read of a span
/// costs O(n²/page) — hundreds of milliseconds per page on FAT32 once a
/// few thousand records live in runs. The memo remembers where the
/// ordinal physically is: the last key consumed, the per-key decision
/// state at that point, the live merge standing just past it, and the
/// record that closed the last page (pulled, not emitted). A page then
/// costs its own records and nothing else.
///
/// Same validity rules as [`ScanState`]: the key survives anything,
/// the live merge only an unchanged run set. Same single-memo contract:
/// a caller paging two spans alternately re-walks each time, exactly
/// and slowly.
#[repr(C)]
#[derive(Clone, Copy)]
struct RangeMemo {
    valid: bool,
    live: bool,
    /// The per-key latch as of just after `last_key`: its prefix has
    /// been decided (emitted, or a tombstone, at this revision).
    decided: bool,
    _pad: u8,
    span_start_len: u16,
    span_end_len: u16,
    last_key_len: u16,
    pending_klen: u16,
    pending_vlen: u16,
    _pad2: u16,
    generation: u32,
    _pad3: u32,
    /// The normalized revision the walk answers at.
    revision: u64,
    /// Ordinal of the NEXT visible entry to emit.
    ordinal: u64,
    span_start: [u8; MAX_ENCODED_KEY],
    span_end: [u8; MAX_ENCODED_KEY],
    last_key: [u8; MAX_ENCODED_KEY],
    pending_key: [u8; MAX_ENCODED_KEY],
    pending_val: [u8; MAX_VALUE_LEN],
    merge: Merge,
}

/// Resumable version-scan bookkeeping — the position memo behind
/// [`KvStateStore::scan_versions`].
///
/// A window is walked in key order and its page cursor is an ORDINAL
/// (the count of qualifying records already emitted). Without a memo,
/// every call — every page, and every record within a page when the
/// caller drains one record at a time — re-walks from the span start
/// and skip-counts, which is quadratic in the window and, on FAT32,
/// re-reads every block each time. The memo remembers where ordinal
/// `ordinal` physically is: the last key visited, and the live merge
/// standing just past it.
///
/// Validity has two levels. The KEY memo (`valid`) survives anything:
/// a flush, a compaction, interleaved writes — the window's bounds are
/// frozen and the walk is key-ordered, so "resume just past this key"
/// is exact regardless of where the records now physically live. The
/// LIVE merge (`live`) is exact only while the run set it was opened
/// against is the run set in force (`generation` unchanged); otherwise
/// it is reopened at the key — one block per run up front, the rest of
/// the positioning inside the next call's budget.
///
/// ONE memo. It is an accelerator, never the guarantee: a request the
/// memo does not fit walks fresh and skip-counts, which is exact and
/// slow. Progress across pauses is the HOST's guarantee — it drives a
/// paused request to its answer before it admits another pausable one
/// (`kv_state_worker::drain_commands` / `head_is_pausable`). Two paused
/// windows alternating request by request would restart each other's
/// walk on every call.
///
/// Arena-resident (~90 KB against a ~2.5 MB state) for the same reason
/// the read memo is: a per-command stack object cannot outlive the
/// command, and this one has to.
#[repr(C)]
#[derive(Clone, Copy)]
struct ScanState {
    valid: bool,
    live: bool,
    /// A transcoded partial page is parked in `page` (engine-level).
    page_valid: bool,
    _pad: u8,
    span_start_len: u16,
    span_end_len: u16,
    last_key_len: u16,
    page_emitted: u16,
    /// Manifest generation the live merge was opened against.
    generation: u32,
    page_len: u32,
    /// The parked page is a COMPLETE reply (its cursor in
    /// `page_next_cursor`), kept so a retry of the same request is
    /// answered from it without moving the walk.
    page_complete: bool,
    /// The parked page's request shape: its entry limit, and whether
    /// its PUT entries carry metadata. A page answers only the request
    /// it was built for.
    page_shape: u8,
    page_limit: u16,
    /// A record the merge yielded that did not fit the caller's
    /// output: kept here, yielded first on the next call, so the merge
    /// stays live across a page boundary instead of being rebuilt.
    pending_klen: u16,
    pending_vlen: u16,
    /// Ordinal of the NEXT record to emit — what a caller resuming
    /// here passes as its cursor.
    ordinal: u64,
    from: u64,
    hi: u64,
    /// Ordinal the parked page STARTED at (the request cursor).
    page_ordinal: u64,
    page_next_cursor: u64,
    span_start: [u8; MAX_ENCODED_KEY],
    span_end: [u8; MAX_ENCODED_KEY],
    last_key: [u8; MAX_ENCODED_KEY],
    pending_key: [u8; MAX_ENCODED_KEY],
    pending_val: [u8; MAX_VALUE_LEN],
    merge: Merge,
    page: [u8; SCAN_PAGE_MAX],
}

impl ScanState {
    /// Record `key` as the last key the walk consumed — the exact
    /// resume point for whichever way the next call rebuilds.
    fn note_visited(&mut self, key: &[u8]) {
        self.last_key[..key.len()].copy_from_slice(key);
        self.last_key_len = key.len() as u16;
    }
}

/// The request a parked version page answers: its window `(from, hi]`,
/// the cursor it started at, and its shape (entry limit, the request's
/// entry flags).
pub struct ScanPageKey {
    pub from: u64,
    pub hi: u64,
    pub ordinal: u64,
    pub limit: u16,
    pub shape: u8,
}

/// Multi-step snapshot capture bookkeeping.
#[repr(C)]
#[derive(Clone, Copy)]
struct CaptureState {
    active: bool,
    header_sent: bool,
    /// A TRANSFER capture (`transfer_begin`): a portable copy for another
    /// replica, read as of `as_of` while writes go on — not the exclusive
    /// local capture, which holds apply and flush off for its duration.
    transfer: bool,
    /// Transfer: the counting pass (the header states the total) is
    /// under way; the emitting pass follows it.
    counting: bool,
    range_generation: u32,
    applied_index: u64,
    /// Records emitted so far; also the resume-cursor value.
    next_ordinal: u64,
    total_entries: u64,
    /// Transfer: the revision the copy is read as of — the store's
    /// highest when it began. A version above it is a write applied
    /// since, which the receiver replays from the log.
    as_of: u64,
    /// Transfer: the internal key a pass resumes AFTER (empty: the
    /// start), so each step walks on from where the last one stopped
    /// instead of from the first key.
    resume_len: u32,
    resume: [u8; MAX_ENCODED_KEY],
}

/// Multi-step snapshot install bookkeeping. `partial` stages one
/// header or record straddling chunk boundaries.
#[repr(C)]
#[derive(Clone, Copy)]
struct InstallState {
    active: bool,
    header_done: bool,
    /// `DESTROY_*`: the local files are being removed before the stream
    /// is taken (0 once they are).
    destroy: u8,
    _pad: u8,
    /// The listing being deleted, and how far.
    destroy_n: u32,
    destroy_at: u32,
    range_generation: u32,
    applied_index: u64,
    expected_offset: u64,
    entry_count: u32,
    entries_done: u32,
    snap_highest_revision: u64,
    /// Committed clock frontier carried by the installing snapshot's
    /// header; adopted with the rest of the state, since the installed
    /// records' deadlines are absolute against it.
    snap_clock_ms: u64,
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
    /// `STAGE_*` within the phase, and the footer part being appended.
    stage: u8,
    part: u8,
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
    /// The runs this merge takes in: the oldest `inputs` of the run set,
    /// fixed when it starts. A flush may publish while the merge is paused
    /// between steps (see [`DiskStore::flush_may_start`]); its run goes in
    /// front, newer than every input, so the inputs stay the run set's
    /// last `inputs` entries and the merge's saved positions stay theirs.
    inputs: u8,
    /// Carried dedup-walk context: the prefix run and its
    /// below-floor latch cross step boundaries with the positions —
    /// without them a resumed step would re-decide rule (a)/(b) from
    /// scratch and keep or drop the wrong versions.
    carry_kept_below_floor: bool,
    carry_prefix_len: u16,
    carry_prefix: [u8; MAX_ENCODED_KEY],
}

/// What a manifest says (format v1, module docs): its generation, the
/// runs it names, and the positions it records.
#[derive(Clone, Copy)]
struct ManifestBody<'a> {
    generation: u32,
    runs: &'a [RunMeta],
    floor: u64,
    applied_index: u64,
    applied_term: u64,
    highest_revision: u64,
    committed_clock_ms: u64,
}

/// A memtable position to undo a command to ([`DiskState::apply_mark`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApplyMark {
    mem_count: u32,
    overwrites: u32,
    releases: u32,
    highest_revision: u64,
    compact_revision: u64,
}

/// Recovery as a resumable walk: every storage call it makes may answer
/// `Again`, and recovery is then asked again where it stood — the run
/// listing continues into the same buffer, a run's verification from the
/// byte it reached, the orphan deletes from the one in flight. Nothing a
/// step established is redone.
#[repr(C)]
#[derive(Clone, Copy)]
struct RecoverState {
    /// `REC_*` (`REC_IDLE`: not started, or finished).
    phase: u8,
    /// `verify_run`'s check within a run (`VERIFY_*`).
    vstage: u8,
    /// A slot was refused because its runs did not validate
    /// (`bad_slot` names it); a winner was chosen (`winner_*`).
    has_bad: bool,
    has_winner: bool,
    /// Runs the listing held.
    listed: u32,
    /// Position within the phase: the run verified, the orphan deleted,
    /// the filter loaded.
    idx: u32,
    bad_slot: u32,
    bad_gen: u32,
    bad_rr: u32,
    winner_gen: u32,
    winner_slot: u32,
    run_count: u32,
    max_seen_id: u32,
    /// `verify_run`'s rolling CRC and the byte it has reached.
    vcrc: u32,
    _pad: u32,
    voff: u64,
    floor: u64,
    applied_index: u64,
    applied_term: u64,
    man_revision: u64,
    man_clock: u64,
    /// The record walk's high-water so far, and the key it reached.
    highest: u64,
    walk_started: bool,
    _pad2: u8,
    walk_key_len: u16,
    _pad3: u32,
    runs: [RunMeta; MAX_RUNS],
    walk_key: [u8; MAX_ENCODED_KEY],
    /// The run listing: kept here, not on the stack, because a listing
    /// the FS answers part-way is continued into the same buffer.
    ids: [u32; MAX_LISTED_RUNS],
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
    /// `STAGE_*` within the phase, and the footer part being appended.
    stage: u8,
    part: u8,
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
    /// Committed clock frontier at flush start. Published with the
    /// manifest so the recovered store's `now` matches the run set the
    /// manifest names — the same discipline as `applied_at_freeze`.
    clock_at_freeze: u64,
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
    /// The last step `recover` was on (telemetry `rr=`): 0 once it
    /// succeeded, else the step it refused at — 1 manifest slots, 2 run
    /// listing, 3 a referenced run absent, 4x `verify_run` (41 too
    /// short, 42 header, 43 read, 44 CRC, 45 trailing bytes, 46 footer),
    /// 5 orphan delete, 6 filter load, 7 unpublished-run delete, 8 the
    /// record walk. A quarantine with no errno is explained by this.
    pub recover_refusal: u32,
    /// Orphan runs deleted by recoveries whose run listing overflowed.
    /// Kept across recovery attempts, as the queue below is.
    pub recover_swept: u32,
    /// Orphans queued by an overflowed listing, `sweep_ids[sweep_at..sweep_count]`
    /// still to delete.
    pub sweep_ids: [u32; SWEEP_BATCH],
    pub sweep_count: u16,
    pub sweep_at: u16,
    /// The run a manifest named that the listing did not hold (`rm=`),
    /// how many runs the listing held (`rl=`), and how many times
    /// recovery served the OTHER manifest slot because the higher
    /// generation's runs did not validate (`rf=`).
    pub recover_missing_id: u32,
    pub recover_listed: u32,
    pub recover_fallbacks: u32,
    /// What the last recovery found in each manifest slot (A, B):
    /// `verdict | (generation << 4)`, verdict 0 untried, 1 absent, 2 short,
    /// 3 magic/version, 4 run count, 5 short for its entries, 6 CRC, 7 valid
    /// (telemetry `ma=`/`mb=`), and the highest run id it names (`ra=`/`rb=`).
    pub manifest_seen: [core::cell::Cell<u32>; 2],
    pub manifest_runs: [core::cell::Cell<u32>; 2],
    pub mem_count: u32,
    pub run_count: u32,
    pub next_run_id: u32,
    /// Last published manifest generation (0 = none yet).
    pub generation: u32,
    /// Identity fence checked against `SnapshotRequest`.
    pub range_generation: u32,
    /// Which manifest slot currently holds the authoritative manifest
    /// (`MANIFEST_SLOT_A`/`_B`). The next publish writes the OTHER one,
    /// so a torn write can never touch the manifest in force.
    pub manifest_slot: u32,
    /// Preallocation progress bitmask: bit 0 = slot A ensured, bit 1 =
    /// slot B. Driven one slot per `prepare_step` so the one-off
    /// creates never land in the same scheduler step.
    pub prepared: u8,
    /// Where the slot being prepared stands (`PREP_*`).
    prep_stage: u8,
    /// The last flush, compaction or recovery step ended waiting on the
    /// device — a storage `Again`, or a fence not yet durable. Asking
    /// again within the same step cannot progress; the caller asks on a
    /// later one. (`flush`, the host-test driver, ignores it.)
    pub io_wait: bool,
    /// Manifest slots (bit 0 A, bit 1 B) a publish had to create (the
    /// fail-safe in `manifest_write`) whose name is not yet published.
    slot_unnamed: u8,
    pub highest_revision: u64,
    pub compaction_floor: u64,
    /// The revision a Compact command (etcd's) has retired history
    /// below. Reads under it are refused at once; the physical
    /// compaction to it follows as maintenance. Rebuilt by replaying the
    /// command, never persisted on its own.
    pub compact_revision: u64,
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
    /// Committed clock frontier stamped by the HOST on every tick it
    /// applies — the state machine's `now` the durable state
    /// corresponds to.
    ///
    /// It is persisted for the same reason the applied position is:
    /// record deadlines are ABSOLUTE against this clock, so a store
    /// recovered without it would resume at zero, make every record
    /// whose deadline had passed visible again, and then re-age the
    /// survivors from the start. The WAL tail usually carries a tick
    /// that would repair this; "usually" is not a recovery guarantee.
    pub committed_clock_ms: u64,
    /// Revision high-water recorded by the PUBLISHED manifest (the
    /// run-set's max version). Compaction republishes it unchanged;
    /// only a flush publish advances it.
    pub durable_highest_revision: u64,
    /// Committed clock frontier recorded by the PUBLISHED manifest.
    /// Compaction republishes it unchanged; only a flush publish
    /// advances it, so it always names the clock the durable run set
    /// actually corresponds to.
    pub durable_clock_ms: u64,
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
    /// Retired runs left as orphans for recovery because the queue was
    /// full and their delete could not be taken at adoption.
    pub retire_orphaned: u32,
    /// Memtable records overwritten in place, and flushed prefixes
    /// released, since `init` (monotonic): what [`DiskState::undo_to`]
    /// checks a mark against.
    overwrites: u32,
    releases: u32,
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
    /// Recovery's position across steps ([`DiskStore::recover_step`]).
    rec: RecoverState,
    scan: ScanState,
    range: RangeMemo,
    /// The point read's merge, kept between reads: positioned in place
    /// (no merge built and copied per read), and each of its run scans
    /// keeps the verified block it last read, so a hot key read again
    /// parses its blocks where they lie. Behind a cell because a point
    /// read borrows the state shared; no point read nests another.
    /// Lent, between reads, to the whole-store walks (recovery, snapshot
    /// capture, compaction), which run no point read inside them: no
    /// merge is ever built on the stack, where two of them would outgrow
    /// a bare-metal kernel stack.
    point: UnsafeCell<Merge>,
    /// The last blocks read from runs (see [`BlockCache`]).
    pub block_cache: BlockCache,
    pub entries: [MemEntry; MEMTABLE_MAX_ENTRIES],
}

impl DiskState {
    /// The revision the most recent flush froze the memtable at (0: none
    /// yet): everything at or below it is in that flush's run once it
    /// publishes. A command log opens a segment there
    /// (`command_log.rs`).
    pub fn last_freeze(&self) -> u64 {
        self.flush.revision_at_freeze
    }

    /// Is a flush draining the memtable? Its frozen prefix stays counted
    /// in `mem_count` until the run publishes, and meanwhile a write that
    /// finds the memtable full is refused (backpressure).
    pub fn flush_active(&self) -> bool {
        self.flush.active
    }

    /// Is a snapshot capture or a REPLACE install holding the apply
    /// exclusion? Every write is refused (backpressure) until it ends.
    pub fn apply_excluded(&self) -> bool {
        (self.capture.active && !self.capture.transfer) || self.install.active
    }

    /// A compaction under way: `(its floor, the input ordinal it has
    /// reached)` — the cursor its next step takes. Whoever next needs it
    /// finished (maintenance's next step, or a write making room) drives
    /// this one on; starting another would abort it.
    pub fn compaction_in_flight(&self) -> Option<(u64, u64)> {
        self.compact
            .active
            .then_some((self.compact.floor, self.compact.input_ordinal))
    }

    /// A compaction under way that a flush may run beside: it is paused
    /// between steps short of its manifest — merging, or closing and
    /// fencing its output run — so it has touched neither the manifest nor
    /// the run set, and it will publish over whatever the run set is then
    /// (its inputs, and any run flushed meanwhile in front of them). A
    /// memtable that must drain therefore never waits out a merge.
    pub fn compact_pausable(&self) -> bool {
        self.compact.active
            && matches!(
                self.compact.phase,
                COMPACT_PHASE_MERGE | COMPACT_PHASE_FOOTER | COMPACT_PHASE_RUN_SYNC
            )
    }

    /// Whether a flush may start now beside whatever else is in flight (it
    /// may be refused still for want of a run slot, or an empty memtable).
    pub fn flush_may_start(&self) -> bool {
        let merge_allows = !self.compact.active || self.compact_pausable();
        let capture_allows = !self.capture.active || self.capture.transfer;
        !self.flush.active && merge_allows && capture_allows && !self.install.active
    }

    /// The revision below which a historical read is refused: the
    /// physical floor, or a Compact's, whichever is higher.
    pub fn read_floor(&self) -> u64 {
        self.compaction_floor.max(self.compact_revision)
    }

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
        self.compact_revision = 0;
        self.durable_applied_index = 0;
        self.durable_applied_term = 0;
        self.pending_applied_index = 0;
        self.pending_applied_term = 0;
        self.committed_clock_ms = 0;
        self.durable_highest_revision = 0;
        self.durable_clock_ms = 0;
        self.retire_ids = [0; MAX_RUNS];
        self.retire_count = 0;
        self.runs = [RunMeta::empty(); MAX_RUNS];
        self.capture.active = false;
        self.compact.active = false;
        self.install.active = false;
        self.flush.active = false;
        self.scan_reset();
        self.range_reset();
        // No block held: an id may name another file after a reset.
        *self.point.get_mut() = Merge::idle();
        self.memo_valid = false;
    }

    /// Where the memtable stands before a command is applied: a command
    /// whose storage read the device deferred is undone to here and
    /// applied again, whole, on a later step ([`Self::undo_to`]).
    pub fn apply_mark(&self) -> ApplyMark {
        ApplyMark {
            mem_count: self.mem_count,
            overwrites: self.overwrites,
            releases: self.releases,
            highest_revision: self.highest_revision,
            compact_revision: self.compact_revision,
        }
    }

    /// Undo every write since `mark` — the records a command added — so
    /// the command can be applied again from the start once the device
    /// has the bytes its reads were waiting for. `true` when undone;
    /// `false` when it cannot be (a record was overwritten in place, or a
    /// flushed prefix was released, since the mark): the caller then
    /// fails closed exactly as it would have on the fault.
    ///
    /// A flush that started during the command froze the command's own
    /// records into its prefix: it is abandoned (nothing it wrote is
    /// published; its run is an orphan, and the next flush re-derives the
    /// same id).
    pub fn undo_to(&mut self, mark: &ApplyMark) -> bool {
        if self.overwrites != mark.overwrites
            || self.releases != mark.releases
            || self.mem_count < mark.mem_count
        {
            return false;
        }
        if self.flush.active && self.flush.freeze_count > mark.mem_count {
            self.flush.active = false;
            self.flush.phase = FLUSH_PHASE_RECORDS;
            self.flush.stage = STAGE_CREATE;
        }
        let count = self.mem_count as usize;
        let keep = mark.mem_count;
        let mut w = 0usize;
        for r in 0..count.min(MEMTABLE_MAX_ENTRIES) {
            let slot = self.order[r];
            if u32::from(slot) < keep {
                self.order[w] = slot;
                w += 1;
            }
        }
        self.mem_count = keep;
        self.highest_revision = mark.highest_revision;
        self.compact_revision = mark.compact_revision;
        // The single-command memo may describe a record just removed. The
        // walk memos are key-exact and re-derive their memtable position
        // on every call, so they stand.
        self.memo_valid = false;
        true
    }

    /// Forget the range-scan position. The next page walks from the
    /// span start (skip-counting to its cursor): slow, and exact.
    pub fn range_reset(&mut self) {
        self.range.valid = false;
        self.range.live = false;
        self.range.pending_klen = 0;
    }

    // ── Version-scan page memo (engine-level parking) ─────────────
    //
    // The engine transcodes store records into its reply page; when
    // the store pauses mid-page the transcoded prefix has to survive
    // to the next step, and the engine is stack-allocated per command.
    // It parks the prefix here, keyed by the request (window + cursor),
    // and takes it back on the re-drive. A COMPLETE page is parked too:
    // a caller that lost the reply (a timeout retry under a new
    // correlation) asks for the same request again, and answering it
    // from the parked page keeps the walk where it is.

    /// Does the parked page belong to the request `key` over `span`?
    fn scan_page_is(&self, span: KeySpan<'_>, key: &ScanPageKey) -> bool {
        let sc = &self.scan;
        sc.page_valid
            && sc.page_ordinal == key.ordinal
            && sc.page_limit == key.limit
            && sc.page_shape == key.shape
            && sc.from == key.from
            && sc.hi == key.hi
            && sc.span_start[..sc.span_start_len as usize] == *span.start
            && sc.span_end[..sc.span_end_len as usize] == *span.end
    }

    /// Park `page` for the request `key` in the memo's window: the
    /// transcoded prefix of an unfinished reply (`complete = false`),
    /// or a finished reply whose cursor is `next_cursor`. A page over
    /// `SCAN_PAGE_MAX` is not parked.
    pub fn scan_page_park(
        &mut self,
        key: &ScanPageKey,
        emitted: u16,
        page: &[u8],
        complete: bool,
        next_cursor: u64,
    ) {
        if page.len() > SCAN_PAGE_MAX {
            self.scan.page_valid = false;
            return;
        }
        self.scan.page[..page.len()].copy_from_slice(page);
        self.scan.page_len = page.len() as u32;
        self.scan.page_emitted = emitted;
        self.scan.page_ordinal = key.ordinal;
        self.scan.page_limit = key.limit;
        self.scan.page_shape = key.shape;
        self.scan.page_complete = complete;
        self.scan.page_next_cursor = next_cursor;
        self.scan.page_valid = true;
    }

    /// Take back the parked page for the request `key` over `span`:
    /// `(emitted, bytes, complete, next_cursor)` copied into `out`, or
    /// `None` when nothing matching is parked. An unfinished page is
    /// taken exactly once; a complete one stays parked, so retries
    /// answer alike for as long as the park survives — until the memo
    /// is displaced or the store's high water moves past the pinned
    /// ceiling, after which a retry re-walks a fresh window.
    pub fn scan_page_take(
        &mut self,
        span: KeySpan<'_>,
        key: &ScanPageKey,
        out: &mut [u8],
    ) -> Option<(u16, usize, bool, u64)> {
        if !self.scan_page_is(span, key) || self.scan.page_len as usize > out.len() {
            return None;
        }
        let sc = &mut self.scan;
        let n = sc.page_len as usize;
        out[..n].copy_from_slice(&sc.page[..n]);
        if !sc.page_complete {
            sc.page_valid = false;
        }
        Some((sc.page_emitted, n, sc.page_complete, sc.page_next_cursor))
    }

    /// Forget the version-scan position. The next request walks from
    /// the span start (skip-counting to its cursor): slow, and exact.
    pub fn scan_reset(&mut self) {
        self.scan.valid = false;
        self.scan.live = false;
        self.scan.page_valid = false;
        self.scan.pending_klen = 0;
    }

    /// The ceiling pinned for the window `(from, ..]` over `span` that
    /// is currently open, if one is, for the request at `cursor`. While
    /// the scan memo holds the window — a paused walk being re-driven,
    /// or its next page — a caller that asked for "up to now" without
    /// naming the ceiling it was told gets the SAME window each time:
    /// "now" moves with every write that lands while the window is
    /// open, and a window re-pinned to it would count its ordinals
    /// against a different set of records on every call. The pin lives
    /// at most as long as the memo: once the walk completes (or another
    /// span displaces it), a fresh unversioned request re-resolves
    /// "now".
    ///
    /// A first page (`cursor` 0) opens a window rather than continuing
    /// one, so it is pinned only when it IS the paused request being
    /// re-driven (its unfinished page is parked). Otherwise it is an
    /// independent request that happens to name the same window as a
    /// walk its caller abandoned, and "now" is now.
    pub fn scan_window_ceiling(&self, span: KeySpan<'_>, from: u64, cursor: u64) -> Option<u64> {
        let sc = &self.scan;
        let continues = cursor != 0 || (sc.page_valid && !sc.page_complete && sc.page_ordinal == 0);
        (sc.valid
            && continues
            && sc.from == from
            && sc.span_start[..sc.span_start_len as usize] == *span.start
            && sc.span_end[..sc.span_end_len as usize] == *span.end)
            .then_some(sc.hi)
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

    /// Diagnostic counter bumps. `Merge::fill_filtered` takes `&DiskState`
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

    /// The memtable as a merge source (see [`MemView`]).
    fn mem_view(&self) -> MemView<'_> {
        MemView {
            count: self.mem_count as usize,
            order: &self.order,
            entries: &self.entries,
            blocks_read: &self.blocks_read,
            cache: &self.block_cache,
        }
    }

    /// The version scan's memo, and the sources its merge opens from.
    fn scan_parts(&mut self) -> (&mut ScanState, MergeSources<'_>) {
        let Self {
            scan,
            mem_count,
            order,
            entries,
            blocks_read,
            run_count,
            runs,
            run_index,
            scans_opened,
            block_cache,
            ..
        } = self;
        (
            scan,
            MergeSources {
                mem: MemView {
                    count: *mem_count as usize,
                    order,
                    entries,
                    blocks_read,
                    cache: block_cache,
                },
                run_count: *run_count as usize,
                runs,
                run_index,
                scans_opened,
            },
        )
    }

    /// The range read's memo, and the sources its merge opens from.
    fn range_parts(&mut self) -> (&mut RangeMemo, MergeSources<'_>) {
        let Self {
            range,
            mem_count,
            order,
            entries,
            blocks_read,
            run_count,
            runs,
            run_index,
            scans_opened,
            block_cache,
            ..
        } = self;
        (
            range,
            MergeSources {
                mem: MemView {
                    count: *mem_count as usize,
                    order,
                    entries,
                    blocks_read,
                    cache: block_cache,
                },
                run_count: *run_count as usize,
                runs,
                run_index,
                scans_opened,
            },
        )
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
        self.releases = self.releases.wrapping_add(1);
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
            self.overwrites = self.overwrites.wrapping_add(1);
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

// ── Run-block cache ───────────────────────────────────────────────────
//
// A run is immutable: a block at an offset of a run (id + digest) never
// changes, so blocks read may be kept and handed back without a storage
// call. The call is what a lookup costs on the board — ~0.7 ms per block
// on FAT32/NVMe, made inside the step of whichever module asked.
//
// The cache holds a store's hot read set, not one write's: a store with
// history serves many hot keys spread over large runs — a node's lease,
// every watcher's registration, the RBAC index. A few blocks cover one
// write's own reads of a hot key and miss most of the rest; on a Pi 5
// control plane eight blocks missed four reads in five, ~80 synchronous
// storage calls a second, enough to run the store's readers past their
// step deadline. 256 blocks is ~1.2 MiB of the store's ~7.8 MiB state.
//
// Keys are scanned apart from the blocks, so a lookup walks a few KiB
// rather than every cached block. Eviction is CLOCK: a block served since
// the hand last passed it is spared once, so a burst of one-off reads
// does not flush the keys that are read over and over.

/// Blocks the cache keeps.
pub const BLOCK_CACHE_SLOTS: usize = 256;

/// Which block a slot holds. `run_id == 0` is an empty slot.
#[repr(C)]
#[derive(Clone, Copy)]
struct CacheKey {
    run_id: u32,
    run_digest: u32,
    off: u64,
    /// The stored length.
    len: u32,
    /// Served since the clock hand last passed: spared once.
    referenced: u32,
}

/// The cache. Interior mutability through `UnsafeCell`, as `point` is
/// held: the only reader and writer is `RunScan::next`, which never
/// re-enters, and the store is single-threaded.
#[repr(C)]
pub struct BlockCache {
    keys: UnsafeCell<[CacheKey; BLOCK_CACHE_SLOTS]>,
    /// Each slot's whole stored block (header, payload, CRC) as it was
    /// read and verified.
    bufs: UnsafeCell<[[u8; BLOCK_OVERHEAD + BLOCK_MAX_PAYLOAD]; BLOCK_CACHE_SLOTS]>,
    /// The clock hand: the next slot `put` considers.
    hand: Cell<u16>,
    /// Blocks served from the cache (the `disk hb` reports it beside
    /// `bl`, the blocks that went to storage).
    pub hits: Cell<u64>,
}

impl BlockCache {
    /// The block at `off` of run `(id, digest)` into `dst`, if cached:
    /// the stored length.
    fn get(&self, run_id: u32, run_digest: u32, off: u64, dst: &mut [u8]) -> Option<usize> {
        if run_id == 0 {
            return None;
        }
        // SAFETY: see the type; no other reference to the slots exists
        // while these do.
        let keys = unsafe { &mut *self.keys.get() };
        let bufs = unsafe { &*self.bufs.get() };
        for (k, b) in keys.iter_mut().zip(bufs.iter()) {
            if k.run_id == run_id && k.run_digest == run_digest && k.off == off {
                let n = k.len as usize;
                if n > dst.len() || n > b.len() {
                    return None;
                }
                dst[..n].copy_from_slice(&b[..n]);
                k.referenced = 1;
                self.hits.set(self.hits.get().wrapping_add(1));
                return Some(n);
            }
        }
        None
    }

    /// Mark the block at `off` of run `(id, digest)` wanted, if cached,
    /// without copying it: it was served from elsewhere (a scanner's held
    /// block), and is as hot as one served from here.
    fn touch(&self, run_id: u32, run_digest: u32, off: u64) {
        if run_id == 0 {
            return;
        }
        // SAFETY: as `get`.
        let keys = unsafe { &mut *self.keys.get() };
        for k in keys.iter_mut() {
            if k.run_id == run_id && k.run_digest == run_digest && k.off == off {
                k.referenced = 1;
                return;
            }
        }
    }

    /// Keep a verified block in the first slot the clock hand finds empty
    /// or unreferenced, sparing (and clearing) referenced ones on the way.
    fn put(&self, run_id: u32, run_digest: u32, off: u64, block: &[u8]) {
        if run_id == 0 || block.is_empty() {
            return;
        }
        // SAFETY: as `get`.
        let keys = unsafe { &mut *self.keys.get() };
        let bufs = unsafe { &mut *self.bufs.get() };
        let mut i = (self.hand.get() as usize) % BLOCK_CACHE_SLOTS;
        // Two passes at most: the first clears every reference it meets.
        let mut tries = 0usize;
        while tries < 2 * BLOCK_CACHE_SLOTS {
            let k = &mut keys[i];
            if k.run_id == 0 || k.referenced == 0 {
                break;
            }
            k.referenced = 0;
            i = (i + 1) % BLOCK_CACHE_SLOTS;
            tries += 1;
        }
        let b = &mut bufs[i];
        let n = block.len().min(b.len());
        b[..n].copy_from_slice(&block[..n]);
        keys[i] = CacheKey {
            run_id,
            run_digest,
            off,
            len: n as u32,
            referenced: 0,
        };
        self.hand.set(((i + 1) % BLOCK_CACHE_SLOTS) as u16);
    }
}

// ── Run-file sequential reader ────────────────────────────────────────

/// Cursor over one run file's records, loading and CRC-checking one
/// block at a time. Lives on the caller's stack (~4.7 KB each), or in
/// the arena as part of the resumable version scan's merge.
#[repr(C)]
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
    /// The run file's length: the clamp for a block read's speculative
    /// length.
    file_end: u64,
    payload_len: usize,
    pos: usize,
    key_off: usize,
    key_len: usize,
    val_off: usize,
    val_len: usize,
    /// The block `buf` holds, verified: its run and offset (`held_run`
    /// 0: none). Run files are immutable once published and a run id
    /// names one file, so a scan positioned on that block again — a hot
    /// key's next point read — parses it where it lies, with no read and
    /// no CRC. Cleared before any read into `buf`.
    held_run: u32,
    held_off: u64,
    /// The digest of the run file the scan is open on, and of the one
    /// the held block came from: an id names one file only while it is
    /// published — a restore or install may publish the same id again
    /// with other contents — so a held block counts only for the same id
    /// AND the same file.
    run_digest: u32,
    held_digest: u32,
    /// One whole block as read from the file: header, payload, CRC.
    buf: [u8; BLOCK_OVERHEAD + BLOCK_MAX_PAYLOAD],
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
            file_end: 0,
            payload_len: 0,
            pos: 0,
            key_off: 0,
            key_len: 0,
            val_off: 0,
            val_len: 0,
            held_run: 0,
            held_off: 0,
            run_digest: 0,
            held_digest: 0,
            buf: [0u8; BLOCK_OVERHEAD + BLOCK_MAX_PAYLOAD],
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
        rs.run_digest = meta.digest;
        rs.data_end = meta.file_len.saturating_sub(RUN_FOOTER_LEN as u64);
        rs.file_end = meta.file_len;
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

    /// [`Self::idle`] in place: every position reset, the block buffer —
    /// and the block it holds (`held_*`) — left as they are, so no 4 KB
    /// buffer is built and copied per run per read.
    fn make_idle(&mut self) {
        self.blocks_loaded = 0;
        self.run_id = 0;
        self.has_cur = false;
        self.exhausted = true;
        self.cur_block_off = 0;
        self.cur_rec_idx = u32::MAX;
        self.next_block_off = 0;
        self.data_end = 0;
        self.file_end = 0;
        self.payload_len = 0;
        self.pos = 0;
        self.key_off = 0;
        self.key_len = 0;
        self.val_off = 0;
        self.val_len = 0;
    }

    /// [`Self::open_at`] in place (see [`Self::make_idle`]).
    fn reopen_at(&mut self, meta: &RunMeta, start_off: u64) {
        self.make_idle();
        self.run_id = meta.id;
        self.run_digest = meta.digest;
        self.data_end = meta.file_len.saturating_sub(RUN_FOOTER_LEN as u64);
        self.file_end = meta.file_len;
        self.next_block_off = if start_off >= RUN_HDR_LEN as u64 && start_off < self.data_end {
            start_off
        } else {
            RUN_HDR_LEN as u64
        };
        self.exhausted = meta.record_count == 0;
        self.cur_block_off = self.next_block_off;
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
        cache: &BlockCache,
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
                if !rs.next(storage, cache)? {
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
    fn next<S: RunStorage>(&mut self, storage: &S, cache: &BlockCache) -> Result<bool, StoreError> {
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
            if self.held_run != 0
                && self.held_run == self.run_id
                && self.held_digest == self.run_digest
                && self.held_off == self.next_block_off
            {
                // The block is already here, verified when it was read.
                // The cache still learns it was wanted: a repeat served
                // from here never reaches `get`, and a slot the clock
                // hand sees unreferenced is the next one evicted.
                cache.touch(self.run_id, self.run_digest, self.next_block_off);
                let payload_len = rd_u32(&self.buf, 0) as usize;
                let block_end = self.next_block_off + (BLOCK_OVERHEAD + payload_len) as u64;
                if payload_len == 0 || payload_len > BLOCK_MAX_PAYLOAD || block_end > self.data_end
                {
                    return Err(StoreError::StorageFault);
                }
                self.payload_len = 8 + payload_len;
                self.pos = 8;
                self.cur_block_off = self.next_block_off;
                self.cur_rec_idx = u32::MAX;
                self.next_block_off = block_end;
                return self.parse_record();
            }
            // ONE read per block: header, payload and CRC together.
            // The payload length is not known until the header is
            // parsed, so the read is speculative — the largest block
            // there can be, clamped to the file — and the surplus is
            // ignored. Every storage read is a full FS call (~190 us on
            // FAT32/NVMe, rig-measured); separate header/payload/CRC
            // reads would triple the dominant cost of every run-backed
            // lookup, so the block travels in one.
            let remaining = self.file_end.saturating_sub(self.next_block_off);
            let want = (BLOCK_OVERHEAD + BLOCK_MAX_PAYLOAD).min(remaining as usize);
            if want < BLOCK_OVERHEAD {
                return Err(StoreError::StorageFault);
            }
            self.held_run = 0;
            // The record in hand has been consumed: until the next block
            // is in, the scan holds none. A read the device defers leaves
            // it exactly so — positioned before `next_block_off` with no
            // current record — and the same `next` asked again loads that
            // block (`Merge::next` does so before choosing a head).
            self.has_cur = false;
            // The cache first: a block it holds was verified when it
            // was read, and costs no storage call now.
            let cached = cache.get(
                self.run_id,
                self.run_digest,
                self.next_block_off,
                &mut self.buf[..want],
            );
            if cached.is_none() {
                read_exact(
                    storage,
                    FileKind::Run,
                    self.run_id,
                    self.next_block_off,
                    &mut self.buf[..want],
                )?;
                self.blocks_loaded = self.blocks_loaded.saturating_add(1);
            }
            let payload_len = rd_u32(&self.buf, 0) as usize;
            let rec_count = rd_u32(&self.buf, 4);
            if payload_len == 0 || payload_len > BLOCK_MAX_PAYLOAD || rec_count == 0 {
                return Err(StoreError::StorageFault);
            }
            let block_end = self.next_block_off + (BLOCK_OVERHEAD + payload_len) as u64;
            if block_end > self.data_end || BLOCK_OVERHEAD + payload_len > want {
                return Err(StoreError::StorageFault);
            }
            let payload = 8..8 + payload_len;
            if crc32(&self.buf[payload.clone()]) != rd_u32(&self.buf, payload.end) {
                return Err(StoreError::StorageFault);
            }
            if cached.is_none() {
                cache.put(
                    self.run_id,
                    self.run_digest,
                    self.next_block_off,
                    &self.buf[..BLOCK_OVERHEAD + payload_len],
                );
            }
            self.held_run = self.run_id;
            self.held_digest = self.run_digest;
            self.held_off = self.next_block_off;
            // `pos`/`payload_len` index into `buf`, whose first 8 bytes
            // are the block header.
            self.payload_len = payload.end;
            self.pos = payload.start;
            self.cur_block_off = self.next_block_off;
            self.cur_rec_idx = u32::MAX;
            self.next_block_off = block_end;
        }
        self.parse_record()
    }

    /// Parse the record at `pos` of the loaded block: the current record.
    fn parse_record(&mut self) -> Result<bool, StoreError> {
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
/// The memtable as a merge source: the three fields a walk reads, and
/// the block counter it reports into. Borrowed field-by-field so a
/// `Merge` that lives INSIDE `DiskState` (the resumable version scan)
/// can advance while the memtable is read — disjoint borrows of one
/// struct, no copy of the 76 KB merge in and out per step.
struct MemView<'a> {
    count: usize,
    order: &'a [u16; MEMTABLE_MAX_ENTRIES],
    entries: &'a [MemEntry; MEMTABLE_MAX_ENTRIES],
    blocks_read: &'a Cell<u64>,
    cache: &'a BlockCache,
}

impl MemView<'_> {
    fn key(&self, pos: usize) -> &[u8] {
        let e = &self.entries[self.order[pos] as usize];
        &e.key[..e.key_len as usize]
    }

    fn value(&self, pos: usize) -> &[u8] {
        let e = &self.entries[self.order[pos] as usize];
        &e.value[..e.value_len as usize]
    }

    /// First position whose key is `>= key` (`count` when none).
    fn lower_bound(&self, key: &[u8]) -> usize {
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.key(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn add_blocks(&self, n: u64) {
        self.blocks_read.set(self.blocks_read.get().wrapping_add(n));
    }
}

/// What a merge is opened from: the memtable and the published runs —
/// every part of the state but the merges themselves, so a merge the
/// state holds is opened in place beside them ([`DiskState::scan_parts`],
/// [`DiskState::range_parts`]).
struct MergeSources<'a> {
    mem: MemView<'a>,
    run_count: usize,
    runs: &'a [RunMeta; MAX_RUNS],
    run_index: &'a [[u8; RUN_INDEX_BYTES]; MAX_RUNS],
    scans_opened: &'a Cell<u64>,
}

/// `#[repr(C)]` + `Copy` because one instance is arena-resident: the
/// version scan keeps its merge in `DiskState::scan` across steps, and
/// the zeroed arena must be a valid (idle) value.
#[repr(C)]
#[derive(Clone, Copy)]
struct Merge {
    include_mem: bool,
    mem_pos: usize,
    run_count: usize,
    runs: [RunScan; MAX_RUNS],
}

impl Merge {
    /// A merge with no source positioned: every run scan idle.
    const fn idle() -> Self {
        Self {
            include_mem: false,
            mem_pos: 0,
            run_count: 0,
            runs: [RunScan::idle(); MAX_RUNS],
        }
    }

    /// Position every source at its first record with key
    /// `>= start_bound`. Each run is opened at the block its sparse
    /// index names for the bound and advanced from there, so the
    /// positioning cost is O(blocks that hold the bound), not O(blocks
    /// before it).
    ///
    /// In place, into a merge the state holds: no merge (16 run scans,
    /// each with its block) is ever built on the stack and copied out.
    ///
    /// `point_key`: an EXACT-KEY probe (the full lookup prefix) — any run
    /// whose Bloom filter rules it out is skipped without a read. Only
    /// legal for a point lookup: a range scan's start bound is a lower
    /// bound, and a run whose filter lacks that exact key may still hold
    /// later keys the scan must return.
    fn fill_filtered<S: RunStorage>(
        &mut self,
        state: &DiskState,
        storage: &S,
        include_mem: bool,
        start_bound: &[u8],
        point_key: Option<&[u8]>,
    ) -> Result<(), StoreError> {
        self.include_mem = include_mem;
        self.mem_pos = if include_mem {
            state.mem_lower_bound(start_bound)
        } else {
            0
        };
        self.run_count = state.run_count as usize;
        for (i, rs) in self.runs.iter_mut().take(self.run_count).enumerate() {
            if let Some(key) = point_key {
                if !bloom_maybe_contains(&state.run_blooms[i], key) {
                    state.bump_skipped();
                    // DEFINITELY not in this run. Leave the scan idle:
                    // it contributes no head, so the merge behaves
                    // exactly as if the run held nothing at or after the
                    // bound — which, for this key, it does.
                    rs.make_idle();
                    continue;
                }
            }
            state.bump_opened();
            // Binary-search the sparse index and start at the right
            // block instead of walking from the head of the run. Legal
            // for a lower bound as much as for a point key: `idx_seek`
            // answers "the block a scan for this key may safely START
            // at", which is exactly the range-scan question. (The Bloom
            // pre-check above is the one point-only optimisation.)
            rs.reopen_at(&state.runs[i], idx_seek(&state.run_index[i], start_bound));
            while rs.next(storage, &state.block_cache)? {
                if rs.cur_key() >= start_bound {
                    break;
                }
            }
            state.add_blocks(rs.take_blocks());
        }
        Ok(())
    }

    /// Open every source for a walk that will START at `bound` — but
    /// do not position: each run is opened at the block its sparse
    /// index names for `bound` (one block loaded, the first record of
    /// that block as its head), the memtable at its lower bound. The
    /// caller skips keys below `bound` as it walks, so the positioning
    /// cost — up to one index stride of blocks per run — is paid inside
    /// the caller's step budget rather than up front. `include_runs =
    /// false` walks the memtable alone.
    ///
    /// In place, into a merge the state holds: a merge is 16 run scans,
    /// each with its block, and one built on the stack and moved into the
    /// state cost two of them (~600 KiB) in the caller's frames — past a
    /// bare-metal kernel stack when the read arrives as a provider call
    /// inside another module's step.
    fn open_lazy_in<S: RunStorage>(
        &mut self,
        src: &MergeSources<'_>,
        storage: &S,
        include_runs: bool,
        bound: &[u8],
    ) -> Result<(), StoreError> {
        self.include_mem = true;
        self.mem_pos = src.mem.lower_bound(bound);
        self.run_count = if include_runs { src.run_count } else { 0 };
        for (i, rs) in self.runs.iter_mut().take(self.run_count).enumerate() {
            src.scans_opened.set(src.scans_opened.get().wrapping_add(1));
            rs.reopen_at(&src.runs[i], idx_seek(&src.run_index[i], bound));
            rs.next(storage, src.mem.cache)?;
            src.mem.add_blocks(rs.take_blocks());
        }
        Ok(())
    }

    /// Open a compaction's inputs for its first step: the oldest
    /// `compact.inputs` runs, newest first as every merge orders them, from
    /// their first records; the memtable is not an input.
    fn fill_compact_in<S: RunStorage>(
        &mut self,
        state: &DiskState,
        storage: &S,
    ) -> Result<(), StoreError> {
        self.include_mem = false;
        self.mem_pos = 0;
        let Some(base) = compact_base(state) else {
            return Err(StoreError::StorageFault);
        };
        self.run_count = state.compact.inputs as usize;
        for (i, rs) in self.runs.iter_mut().take(self.run_count).enumerate() {
            state.bump_opened();
            rs.reopen_at(
                &state.runs[base + i],
                idx_seek(&state.run_index[base + i], b""),
            );
            rs.next(storage, &state.block_cache)?;
            state.add_blocks(rs.take_blocks());
        }
        Ok(())
    }

    /// Rebuild the compaction merge at the exact positions a paused
    /// [`compact_step`] saved: one `open_at` + at most one block's
    /// records advanced per run — O(runs) per step, independent of how
    /// far the compaction has progressed. Ties re-resolve identically
    /// (memtable excluded, runs newest-first by index), so the resumed
    /// merge continues the same sequence the paused one would have.
    fn resume_compact_in<S: RunStorage>(
        &mut self,
        state: &DiskState,
        storage: &S,
    ) -> Result<(), StoreError> {
        self.include_mem = false;
        self.mem_pos = 0;
        let Some(base) = compact_base(state) else {
            return Err(StoreError::StorageFault);
        };
        self.run_count = state.compact.inputs as usize;
        if state.compact.run_pos_count as usize != self.run_count {
            // The inputs changed under a paused compaction — only runs
            // newer than them may be added meanwhile, so this is
            // corruption.
            return Err(StoreError::StorageFault);
        }
        for i in 0..self.run_count {
            self.runs[i] = RunScan::resume_to(
                &state.runs[base + i],
                state.compact.run_block[i],
                state.compact.run_rec[i],
                storage,
                &state.block_cache,
            )?;
            state.add_blocks(self.runs[i].take_blocks());
        }
        Ok(())
    }

    /// Copy the next merged record into `key_out`/`val_out` and return
    /// `(key_len, value_len)`, or `None` when every source is
    /// exhausted.
    fn next<S: RunStorage>(
        &mut self,
        mem: &MemView<'_>,
        storage: &S,
        key_out: &mut [u8],
        val_out: &mut [u8],
    ) -> Result<Option<(usize, usize)>, StoreError> {
        // Step 0: a source whose advance last time could not load its
        // next block (the device deferred the read) loads it now, before
        // any head is chosen — so a deferred read never lets a later key
        // jump ahead of the records that block holds, and a record is
        // never emitted twice or skipped. An `Err` here emits nothing.
        for rs in self.runs.iter_mut().take(self.run_count) {
            if !rs.has_cur && !rs.exhausted {
                rs.next(storage, mem.cache)?;
                mem.add_blocks(rs.take_blocks());
            }
        }
        // Step 1: pick the smallest head. Priority on ties: memtable,
        // then runs newest-first (runs[0] is newest).
        let mut winner: Option<usize> = None; // 0 = memtable, 1+i = run i
        let klen;
        {
            let mut best: Option<&[u8]> = None;
            if self.include_mem && self.mem_pos < mem.count {
                best = Some(mem.key(self.mem_pos));
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
        // Step 2: copy the winner's value.
        let win = winner.unwrap_or(0);
        let vlen = if win == 0 {
            let v = mem.value(self.mem_pos);
            val_out[..v.len()].copy_from_slice(v);
            v.len()
        } else {
            let v = self.runs[win - 1].cur_value();
            val_out[..v.len()].copy_from_slice(v);
            v.len()
        };
        // Step 3: advance every source whose head equals the winner
        // key (dedupe — the highest-priority copy was emitted).
        if self.include_mem && self.mem_pos < mem.count && mem.key(self.mem_pos) == &key_out[..klen]
        {
            self.mem_pos += 1;
        }
        //
        // The winner is already in `key_out`/`val_out` and the sources
        // are past it, so a block load the device defers here does not
        // fail this call: that source is left holding no record, and the
        // next call loads its block first (step 0). Only a real fault —
        // a block that does not verify — ends the walk.
        for rs in self.runs.iter_mut().take(self.run_count) {
            if rs.has_cur && rs.cur_key() == &key_out[..klen] {
                match rs.next(storage, mem.cache) {
                    Ok(_) | Err(StoreError::Backpressure) => {}
                    Err(e) => return Err(e),
                }
                mem.add_blocks(rs.take_blocks());
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

/// Append one complete block, framed in place: `framed` holds the
/// block's payload at `[BLOCK_HDR..BLOCK_HDR + payload_len]` and room for
/// its header before and its CRC after; both are filled here and the
/// whole block goes out as ONE append. One call, so a block either lands
/// whole (`Ok`) or is asked again as it stands (`Backpressure` from a
/// storage `Again`: the backend continues the bytes it already took, and
/// `crc_state`/`file_len` move only on `Ok`). Returns the block's file
/// offset.
fn write_run_block<S: RunStorage>(
    storage: &mut S,
    id: u32,
    framed: &mut [u8],
    payload_len: usize,
    rec_count: u32,
    crc_state: &mut u32,
    file_len: &mut u64,
) -> Result<u64, StoreError> {
    let off = *file_len;
    let total = BLOCK_OVERHEAD + payload_len;
    let Some(block) = framed.get_mut(..total) else {
        return Err(StoreError::StorageFault);
    };
    let crc = crc32(block.get(BLOCK_HDR..BLOCK_HDR + payload_len).unwrap_or(&[]));
    copy_at(block, 0, &(payload_len as u32).to_le_bytes());
    copy_at(block, 4, &rec_count.to_le_bytes());
    copy_at(block, BLOCK_HDR + payload_len, &crc.to_le_bytes());
    storage
        .append(FileKind::Run, id, block)
        .map_err(map_storage)?;
    *crc_state = crc32_update(*crc_state, block);
    *file_len += total as u64;
    Ok(off)
}

/// The footer's parts, appended in order under the run's rolling CRC.
const FOOTER_PARTS: u8 = 4;

/// Append footer part `part` (0 the head fields, 1 the Bloom filter, 2
/// the sparse index, 3 the file CRC) and fold it into `crc_state` — the
/// file CRC itself is the finished state, and folds nothing. One append
/// per part, each asked again as it stands on `Backpressure`, so no
/// footer-sized buffer (21 KiB with the index) sits on the stack and no
/// part is ever appended twice.
#[expect(
    clippy::too_many_arguments,
    reason = "plain writer helper: every argument is one footer field; bundling them into a struct would only rename the call site"
)]
fn write_run_footer_part<S: RunStorage>(
    storage: &mut S,
    id: u32,
    part: u8,
    record_count: u32,
    block_count: u32,
    smallest_off: u64,
    largest_off: u64,
    bloom: &[u8; BLOOM_BYTES],
    index: &[u8; RUN_INDEX_BYTES],
    crc_state: &mut u32,
) -> Result<(), StoreError> {
    match part {
        0 => {
            let mut head = [0u8; RUN_FOOTER_BLOOM_OFF];
            copy_at(&mut head, 0, &RUN_FOOTER_MAGIC.to_le_bytes());
            copy_at(&mut head, 4, &record_count.to_le_bytes());
            copy_at(&mut head, 8, &block_count.to_le_bytes());
            copy_at(&mut head, 12, &smallest_off.to_le_bytes());
            copy_at(&mut head, 20, &largest_off.to_le_bytes());
            storage
                .append(FileKind::Run, id, &head)
                .map_err(map_storage)?;
            *crc_state = crc32_update(*crc_state, &head);
        }
        // The filter is PERSISTED rather than rebuilt at recovery.
        // Rebuilding would mean a full record scan of every run at
        // startup — up to 16 runs of ~35 KB each — which is exactly the
        // class of unbounded boot-path work this whole effort has been
        // removing. Persisting costs one bounded 1 KiB read per run
        // instead. It needs no checksum of its own: it sits inside the
        // region the footer's `file_crc` already covers, so a corrupt
        // filter fails the same whole-file validation that a corrupt
        // block does, and `verify_run` rejects the run before the filter
        // is ever consulted.
        1 => {
            storage
                .append(FileKind::Run, id, bloom)
                .map_err(map_storage)?;
            *crc_state = crc32_update(*crc_state, bloom);
        }
        2 => {
            storage
                .append(FileKind::Run, id, index)
                .map_err(map_storage)?;
            *crc_state = crc32_update(*crc_state, index);
        }
        _ => {
            let file_crc = crc32_finish(*crc_state);
            storage
                .append(FileKind::Run, id, &file_crc.to_le_bytes())
                .map_err(map_storage)?;
        }
    }
    Ok(())
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
    fn encode_manifest(buf: &mut [u8; MANIFEST_MAX_LEN], m: &ManifestBody<'_>) -> usize {
        let ManifestBody {
            generation: new_gen,
            runs,
            floor,
            applied_index,
            applied_term,
            highest_revision,
            committed_clock_ms,
        } = *m;
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
        buf[52..60].copy_from_slice(&committed_clock_ms.to_le_bytes());
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

    /// Write the manifest bytes into the already-created slot file, in
    /// place at offset 0. The bytes are a pure function of the arguments,
    /// so the write asked again after `Backpressure` (a storage `Again`)
    /// is the same write, which the backend continues.
    fn manifest_write(
        storage: &mut S,
        slot: u32,
        m: &ManifestBody<'_>,
    ) -> Result<bool, StoreError> {
        let mut buf = [0u8; MANIFEST_MAX_LEN];
        let p = Self::encode_manifest(&mut buf, m);
        let bytes = buf.get(..p).unwrap_or(&[]);
        // IN PLACE, at offset 0, into a file that already exists at its
        // final size. No create, no unlink, no directory enumeration.
        match storage.write_at(FileKind::Manifest, slot, 0, bytes) {
            Ok(()) => Ok(false),
            Err(StorageError::NotFound) => {
                // FAIL-SAFE, not the expected path. `prepare_step` runs
                // during RECOVERING and creates both slots before this
                // store ever serves (and an install, which deletes them,
                // publishes through here), so on a real device this arm
                // costs one create, which is precisely what the A/B pair
                // removes from the serving path. The slot is created and
                // the same write asked again; the fence that follows
                // covers the bytes and the size both.
                // `Ok(true)`: the slot's name is new, and is published
                // after its fence, before the publish is adopted.
                match storage.create(FileKind::Manifest, slot) {
                    Ok(()) | Err(StorageError::AlreadyExists) => {}
                    Err(e) => return Err(map_storage(e)),
                }
                storage
                    .write_at(FileKind::Manifest, slot, 0, bytes)
                    .map_err(map_storage)?;
                Ok(true)
            }
            Err(e) => Err(map_storage(e)),
        }
    }

    /// A publish that had to create its manifest slot publishes the
    /// slot's name, after its fence and before it is adopted: recovery
    /// must be able to FIND the manifest it is told is in force.
    fn publish_slot_name(&mut self, slot: u32) -> Result<(), StoreError> {
        if self.state.slot_unnamed & slot_bit(slot) != 0 {
            self.storage
                .fsync_name(FileKind::Manifest, slot)
                .map_err(map_storage)?;
            self.state.slot_unnamed &= !slot_bit(slot);
        }
        Ok(())
    }

    /// One action of ensuring a manifest slot file exists at its final
    /// size: `Ok(true)` once it does.
    ///
    /// Idempotent and NEVER destructive: an existing slot is left
    /// exactly as it is, because it may be the authoritative manifest.
    /// Only a slot that is genuinely absent is created, zero-filled and
    /// fenced — three storage actions, each asked again as it stands
    /// (`prep_stage`), so a zero-fill the device takes part-way is
    /// continued, never skipped and never started over. This is the one
    /// place a manifest file is ever created, and it is off the serving
    /// path (see `prepare_step`).
    fn manifest_slot_prepare(&mut self, slot: u32) -> Result<bool, StoreError> {
        match self.state.prep_stage {
            PREP_CREATE => {
                match self.storage.create(FileKind::Manifest, slot) {
                    Ok(()) => {}
                    // Never touched — it may be the manifest in force — but
                    // its name is published (a no-op for one that already
                    // is), so a slot a crash caught before its fence counts
                    // only once it can be found.
                    Err(StorageError::AlreadyExists) => {
                        self.state.prep_stage = PREP_NAME;
                        return Ok(false);
                    }
                    Err(e) => return Err(map_storage(e)),
                }
                self.state.prep_stage = PREP_ZERO;
                Ok(false)
            }
            PREP_ZERO => {
                let zero = [0u8; MANIFEST_SLOT_LEN];
                self.storage
                    .write_at(FileKind::Manifest, slot, 0, &zero)
                    .map_err(map_storage)?;
                self.state.prep_stage = PREP_SYNC;
                Ok(false)
            }
            PREP_SYNC => {
                self.storage
                    .fsync(FileKind::Manifest, slot)
                    .map_err(map_storage)?;
                self.state.prep_stage = PREP_NAME;
                Ok(false)
            }
            _ => {
                self.storage
                    .fsync_name(FileKind::Manifest, slot)
                    .map_err(map_storage)?;
                self.state.prep_stage = PREP_CREATE;
                Ok(true)
            }
        }
    }

    /// Flush the memtable into one immutable sorted run file, publish
    /// a new manifest, then release the flushed records (in exactly
    /// that order: the memtable is only released once its contents are
    /// durably referenced).
    ///
    /// Drives `flush_step` to completion, asking a step that waits on the
    /// device (a storage `Again`, a fence not yet durable) again at once.
    /// HOST ONLY: on a real device the device moves only between steps,
    /// so this would spin — module code drives [`Self::flush_step`] once
    /// per step, or [`Self::flush_drive`] where a caller must make room
    /// inside one step. It is the natural shape for host tests, whose
    /// backends move on every call.
    pub fn flush(&mut self) -> Result<(), StoreError> {
        loop {
            match self.flush_step() {
                Ok(Progress::Done) => return Ok(()),
                Ok(Progress::InProgress { .. }) => {}
                Err(StoreError::Backpressure) if self.state.flush.active => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// Drive a flush on as far as it goes without waiting on the device:
    /// `Ok` once it has published, `Err(Backpressure)` as soon as a step
    /// must wait (storage `Again`, or a fence still pending) — the flush
    /// stays in flight where it stood and a later step continues it — and
    /// any other error as the step reports it. Never spins.
    pub fn flush_drive(&mut self) -> Result<(), StoreError> {
        self.flush_drive_guarded(false)
    }

    fn flush_drive_guarded(&mut self, ignore_install: bool) -> Result<(), StoreError> {
        loop {
            match self.flush_step_guarded(ignore_install)? {
                Progress::Done => return Ok(()),
                Progress::InProgress { .. } if self.state.io_wait => {
                    return Err(StoreError::Backpressure);
                }
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
    /// Every storage action of the flush is one stage of its phase. A
    /// stage the device cannot take now answers `Err(Backpressure)` with
    /// the flush STILL IN FLIGHT (`flush_in_progress()`), standing at that
    /// stage, and [`DiskState::io_wait`] set: the next call asks the same
    /// action again, which the storage continues — nothing already
    /// written is written again and nothing is started over. A
    /// `Backpressure` with no flush in flight is a refusal before any I/O
    /// (run set full, an exclusion fence). Any other error abandons the
    /// flush: nothing was published, the partial run is an orphan.
    ///
    /// Writes continue to be ACCEPTED during a flush, up to the
    /// memtable's capacity — that is the entire point of chunking.
    /// They land in slots at or above the frozen prefix and are left in
    /// the memtable when the flushed records are released.
    pub fn flush_step(&mut self) -> Result<Progress, StoreError> {
        self.flush_step_guarded(false)
    }

    /// Flush body that tolerates an active install (the install stream
    /// flushes through this when its staging memtable fills), driven as
    /// far as it goes without waiting (see [`Self::flush_drive`]).
    fn flush_inner(&mut self) -> Result<(), StoreError> {
        self.flush_drive_guarded(true)
    }

    fn flush_step_guarded(&mut self, ignore_install: bool) -> Result<Progress, StoreError> {
        self.state.io_wait = false;
        if !self.state.flush.active {
            if (self.state.compact.active && !self.state.compact_pausable())
                || (self.state.capture.active && !self.state.capture.transfer)
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
            let freeze = self.state.mem_count;
            self.state.flush.active = true;
            self.state.flush.applied_at_freeze = self.state.pending_applied_index;
            self.state.flush.term_at_freeze = self.state.pending_applied_term;
            self.state.flush.revision_at_freeze = self.state.highest_revision;
            self.state.flush.clock_at_freeze = self.state.committed_clock_ms;
            self.state.flush.phase = FLUSH_PHASE_RECORDS;
            self.state.flush.stage = STAGE_CREATE;
            self.state.flush.part = 0;
            self.state.flush.ticket = 0;
            self.state.flush.pending_gen = 0;
            self.state.flush.pending_slot = 0;
            self.state.flush.pending_len = 0;
            self.state.flush.pending_crc = 0;
            self.state.flush.run_id = id;
            self.state.flush.freeze_count = freeze;
            self.state.flush.cursor = 0;
            self.state.flush.out_blocks = 0;
            self.state.flush.crc_state = CRC32_INIT;
            self.state.flush.block_recs = 0;
            self.state.flush.fill = 0;
            self.state.flush.chunks = 0;
            self.state.flush.bloom = [0u8; BLOOM_BYTES];
            self.state.flush.index.fill(0);
            self.state.flush.blk_has_first = false;
            self.state.flush.out_len = 0;
            self.state.flush.smallest_off = 0;
            self.state.flush.largest_off = 0;
            // Freeze the sorted view. From here the flush reads its own
            // snapshot, so concurrent inserts cannot disturb it.
            for (d, o) in self
                .state
                .flush
                .slots
                .iter_mut()
                .zip(self.state.order.iter())
                .take(freeze as usize)
            {
                *d = *o;
            }
        }
        let phase = self.state.flush.phase;
        let stepped = match phase {
            FLUSH_PHASE_RUN_SYNC => self.flush_poll_run(),
            FLUSH_PHASE_MAN_WRITE => self.flush_manifest_write(),
            FLUSH_PHASE_MAN_SYNC => self.flush_poll_manifest(),
            _ => self.flush_run_stages(),
        };
        match stepped {
            Ok(p) => Ok(p),
            Err(StoreError::Backpressure) => {
                // The device could not take this stage now. The flush
                // stands where it is; the next step asks the same action.
                self.state.io_wait = true;
                Err(StoreError::Backpressure)
            }
            Err(e) => {
                // Fail closed: abandon the half-written run (an orphan
                // for `recover()`), keep every record in the memtable.
                // Nothing was published, so the previous manifest is
                // still the truth.
                self.state.flush.active = false;
                self.state.flush.phase = FLUSH_PHASE_RECORDS;
                self.state.flush.stage = STAGE_CREATE;
                self.state.flush.ticket = 0;
                Err(e)
            }
        }
    }

    /// The run file's stages: create, header, records, footer, fence
    /// submit. Each storage action moves the stage on only once it has
    /// landed.
    fn flush_run_stages(&mut self) -> Result<Progress, StoreError> {
        let id = self.state.flush.run_id;
        if self.state.flush.stage == STAGE_CREATE {
            create_fresh(self.storage, FileKind::Run, id)?;
            self.state.flush.stage = STAGE_HEADER;
        }
        if self.state.flush.stage == STAGE_HEADER {
            let mut crc_state = CRC32_INIT;
            let mut file_len = 0u64;
            write_run_header(self.storage, id, &mut crc_state, &mut file_len)?;
            self.state.flush.crc_state = crc_state;
            self.state.flush.out_len = file_len;
            self.state.flush.stage = STAGE_BODY;
            // The create and the header are the first chunk: on FAT32 a
            // create is the step's whole budget.
            self.state.flush.chunks += 1;
            return Ok(Progress::InProgress {
                cursor: u64::from(self.state.flush.cursor),
            });
        }
        if self.state.flush.stage == STAGE_BODY {
            match self.flush_step_records()? {
                Some(p) => return Ok(p),
                None => {
                    self.state.flush.stage = STAGE_FOOTER;
                    self.state.flush.part = 0;
                }
            }
        }
        if self.state.flush.stage == STAGE_FOOTER {
            while self.state.flush.part < FOOTER_PARTS {
                let mut crc = self.state.flush.crc_state;
                write_run_footer_part(
                    self.storage,
                    id,
                    self.state.flush.part,
                    self.state.flush.freeze_count,
                    self.state.flush.out_blocks,
                    self.state.flush.smallest_off,
                    self.state.flush.largest_off,
                    &self.state.flush.bloom,
                    &self.state.flush.index,
                    &mut crc,
                )?;
                self.state.flush.crc_state = crc;
                self.state.flush.part += 1;
            }
            self.state.flush.pending_len = self.state.flush.out_len + RUN_FOOTER_LEN as u64;
            self.state.flush.pending_crc = crc32_finish(self.state.flush.crc_state);
            self.state.flush.stage = STAGE_RUN_SUBMIT;
        }
        // SUBMIT the run's durability fence and LEAVE. This is the
        // 163 ms that killed the module: it is now spread across later
        // steps as cheap polls instead of one blocking call.
        let ticket = self
            .storage
            .fsync_submit(FileKind::Run, id)
            .map_err(map_storage)?;
        self.state.flush.ticket = ticket;
        self.state.flush.phase = FLUSH_PHASE_RUN_SYNC;
        self.state.flush.stage = STAGE_CREATE;
        self.state.flush.chunks += 1;
        Ok(Progress::InProgress {
            cursor: u64::from(self.state.flush.cursor),
        })
    }

    /// Write at most `FLUSH_STEP_RECORDS` records into the run's blocks:
    /// `Some(progress)` when the step's budget is spent, `None` when the
    /// frozen records are all written (the last block closed).
    fn flush_step_records(&mut self) -> Result<Option<Progress>, StoreError> {
        let freeze = self.state.flush.freeze_count as usize;
        let mut written = 0u32;
        while (self.state.flush.cursor as usize) < freeze {
            if written >= FLUSH_STEP_RECORDS {
                self.state.flush.chunks += 1;
                return Ok(Some(Progress::InProgress {
                    cursor: u64::from(self.state.flush.cursor),
                }));
            }
            let slot = self.state.flush.slots[self.state.flush.cursor as usize] as usize;
            let (key_len, value_len) = {
                let e = &self.state.entries[slot];
                (e.key_len as usize, e.value_len as usize)
            };
            let rec_len = RECORD_FIXED + key_len + value_len;
            let fill = self.state.flush.fill as usize;
            if fill > 0 && fill + rec_len > BLOCK_TARGET {
                // A block the device cannot take now is asked again next
                // step from this same record: nothing below has moved.
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
        // Records exhausted: close the last block. The footer and the
        // fence submit follow as their own stages.
        if self.state.flush.fill > 0 {
            self.flush_write_block()?;
        }
        Ok(None)
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
                self.state.io_wait = true;
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
        self.state.flush.stage = STAGE_RUN_NAME;
        self.state.flush.chunks += 1;
        Ok(Progress::InProgress {
            cursor: u64::from(self.state.flush.cursor),
        })
    }

    /// Write the manifest bytes, then submit its fence: two stages, each
    /// asked again as it stands.
    fn flush_manifest_write(&mut self) -> Result<Progress, StoreError> {
        let slot = self.state.inactive_manifest_slot();
        if self.state.flush.stage == STAGE_RUN_NAME {
            // The run's bytes are durable; now its name. Only then may a
            // manifest name it.
            self.storage
                .fsync_name(FileKind::Run, self.state.flush.run_id)
                .map_err(map_storage)?;
            self.state.flush.stage = STAGE_MAN_WRITE;
        }
        if self.state.flush.stage == STAGE_MAN_WRITE {
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
            for (d, r) in new_runs
                .iter_mut()
                .skip(1)
                .zip(self.state.runs.iter())
                .take(old_count)
            {
                *d = *r;
            }
            let body = ManifestBody {
                generation: self.state.flush.pending_gen,
                runs: new_runs.get(..old_count + 1).unwrap_or(&[]),
                floor: self.state.compaction_floor,
                applied_index: self.state.flush.applied_at_freeze,
                applied_term: self.state.flush.term_at_freeze,
                highest_revision: self.state.flush.revision_at_freeze,
                committed_clock_ms: self.state.flush.clock_at_freeze,
            };
            if Self::manifest_write(self.storage, slot, &body)? {
                self.state.slot_unnamed |= slot_bit(slot);
            }
            self.state.flush.stage = STAGE_MAN_SUBMIT;
        }
        let ticket = self
            .storage
            .fsync_submit(FileKind::Manifest, slot)
            .map_err(map_storage)?;
        self.state.flush.pending_slot = slot;
        self.state.flush.ticket = ticket;
        self.state.flush.phase = FLUSH_PHASE_MAN_SYNC;
        self.state.flush.stage = STAGE_MAN_POLL;
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
        if self.state.flush.stage == STAGE_MAN_POLL {
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
                    self.state.io_wait = true;
                    self.state.flush.chunks += 1;
                    return Ok(Progress::InProgress {
                        cursor: u64::from(self.state.flush.cursor),
                    });
                }
                FsyncState::Durable => {}
            }
            // The ticket is collected: never polled again.
            self.state.flush.stage = STAGE_MAN_NAME;
        }
        self.publish_slot_name(self.state.flush.pending_slot)?;
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
        // In place: sixteen indexes are ~320 KiB, which no
        // stack frame may hold on bare metal.
        self.state.run_index.copy_within(0..old_count, 1);
        self.state.run_index[0].copy_from_slice(&self.state.flush.index);
        for slot in self.state.run_index.iter_mut().skip(1 + old_count) {
            slot.fill(0);
        }
        self.state.runs = new_runs;
        self.state.run_count = (old_count + 1) as u32;
        self.state.generation = new_gen;
        self.state.manifest_slot = self.state.flush.pending_slot;
        self.state.next_run_id = self.state.flush.run_id + 1;
        self.state.flush.active = false;
        self.state.durable_applied_index = self.state.flush.applied_at_freeze;
        self.state.durable_applied_term = self.state.flush.term_at_freeze;
        self.state.durable_highest_revision = self.state.flush.revision_at_freeze;
        self.state.durable_clock_ms = self.state.flush.clock_at_freeze;
        // Back to the resting phase. Leaving it at MANIFEST_SYNC would
        // make `flush_phase()` lie to diagnostics after a completed
        // flush, and would leave a stale ticket one missed `active`
        // check away from being polled.
        self.state.flush.phase = FLUSH_PHASE_RECORDS;
        self.state.flush.stage = STAGE_CREATE;
        self.state.flush.ticket = 0;
        self.state.flush.chunks += 1;
        self.state.release_flushed(self.state.flush.freeze_count);
        Ok(Progress::Done)
    }

    /// Write the buffered block, tracking offsets exactly as the
    /// single-call flush did. Nothing moves until the block has landed.
    fn flush_write_block(&mut self) -> Result<(), StoreError> {
        let id = self.state.flush.run_id;
        let fill = self.state.flush.fill as usize;
        let recs = self.state.flush.block_recs;
        let mut crc_state = self.state.flush.crc_state;
        let mut file_len = self.state.flush.out_len;
        // The payload must be handed to `write_run_block` as a slice
        // while `self.storage` is also borrowed mutably, so it goes via
        // a stack copy — framed, so the block goes out as one append.
        let mut framed = [0u8; BLOCK_OVERHEAD + BLOCK_MAX_PAYLOAD];
        for (d, b) in framed
            .iter_mut()
            .skip(BLOCK_HDR)
            .zip(self.state.flush.block.iter())
            .take(fill)
        {
            *d = *b;
        }
        let off = write_run_block(
            self.storage,
            id,
            &mut framed,
            fill,
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
        // A storage `Again` comes back as `Backpressure` with the slot's
        // stage where it stood; the next call asks the same action.
        for (bit, slot) in [(1u8, MANIFEST_SLOT_A), (2u8, MANIFEST_SLOT_B)] {
            if self.state.prepared & bit == 0 {
                if self.manifest_slot_prepare(slot)? {
                    self.state.prepared |= bit;
                    self.state.prep_stage = PREP_CREATE;
                }
                return Ok(Progress::InProgress {
                    cursor: u64::from(self.state.prepared),
                });
            }
        }
        Ok(Progress::Done)
    }

    /// Has the A/B pair been ensured?
    pub fn prepared(&self) -> bool {
        self.state.prepared & 3 == 3
    }

    /// Recovery: load the newest checksum-valid manifest,
    /// validate every referenced run (identity, sizes, checksums,
    /// format versions), quarantine on any inconsistency, and remove
    /// orphan files. The memtable starts empty; committed-entry replay
    /// (step 5) is the caller's job.
    ///
    /// Drives [`Self::recover_step`] to its end, asking a step that waits
    /// on the device again at once — HOST ONLY, like [`Self::flush`]: the
    /// module drives `recover_step` once per step.
    ///
    /// On `Err(StorageFault)` the state is left EMPTY and the on-disk
    /// files untouched (quarantine — never guess, never delete
    /// evidence). The caller must not serve.
    pub fn recover(&mut self) -> Result<(), StoreError> {
        loop {
            match self.recover_step() {
                Ok(Progress::Done) => return Ok(()),
                Ok(Progress::InProgress { .. }) => {}
                Err(StoreError::Backpressure) => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// Is a recovery part-way (asked again, it continues)?
    pub fn recover_in_progress(&self) -> bool {
        self.state.rec.phase != REC_IDLE
    }

    /// One bounded slice of recovery (see [`Self::recover`]).
    ///
    /// `Ok(InProgress)` — more to do; `Err(Backpressure)` — a storage
    /// call answered `Again`: recovery stands exactly where it was
    /// (`io_wait`), and the next call asks the same call again — the
    /// listing continues into the same buffer, a run's verification from
    /// the byte it reached, the delete in flight is the one asked. No
    /// step established is redone. Any other error ends the attempt (the
    /// state left EMPTY, the next call starts afresh) — a quarantine.
    pub fn recover_step(&mut self) -> Result<Progress, StoreError> {
        self.state.io_wait = false;
        match self.recover_advance() {
            Ok(true) => {
                self.state.rec.phase = REC_IDLE;
                self.state.recover_refusal = 0;
                Ok(Progress::Done)
            }
            Ok(false) => Ok(Progress::InProgress {
                cursor: u64::from(self.state.rec.phase),
            }),
            Err(StoreError::Backpressure) => {
                self.state.io_wait = true;
                Err(StoreError::Backpressure)
            }
            Err(e) => {
                // Quarantine. The next attempt starts from the top; the
                // sweep queue (`sweep_*`) survives it, as it always has.
                self.state.rec.phase = REC_IDLE;
                Err(e)
            }
        }
    }

    /// Run recovery's phases on from where they stand: `Ok(true)` once
    /// recovered, `Ok(false)` when a bounded slice was spent.
    fn recover_advance(&mut self) -> Result<bool, StoreError> {
        if self.state.rec.phase == REC_IDLE {
            let range_gen = self.state.range_generation;
            let prepared = self.state.prepared;
            let (swept, sweep_ids, sweep_count, sweep_at) = (
                self.state.recover_swept,
                self.state.sweep_ids,
                self.state.sweep_count,
                self.state.sweep_at,
            );
            self.state.init();
            self.state.range_generation = range_gen;
            self.state.recover_swept = swept;
            self.state.sweep_ids = sweep_ids;
            self.state.sweep_count = sweep_count;
            self.state.sweep_at = sweep_at;
            // Preallocation is a property of the FILESYSTEM, not of the
            // recovered logical state: re-creating the pair after every
            // recover would put a 159 ms create back on the boot path.
            self.state.prepared = prepared;
            self.state.rec.phase = REC_SWEEP;
            self.state.rec.has_bad = false;
            self.state.rec.has_winner = false;
            self.state.rec.listed = 0;
            self.state.rec.idx = 0;
            self.state.rec.max_seen_id = 0;
            self.state.rec.walk_started = false;
            self.state.rec.highest = 0;
        }
        loop {
            match self.state.rec.phase {
                REC_SWEEP => {
                    // A sweep an earlier attempt queued finishes before
                    // anything is listed (see SWEEP_BATCH).
                    self.drain_sweep()?;
                    self.state.rec.phase = REC_LIST;
                }
                REC_LIST => {
                    self.state.recover_refusal = 2;
                    match self.storage.list(FileKind::Run, &mut self.state.rec.ids) {
                        Ok(n) => {
                            self.state.rec.listed = n as u32;
                            self.state.recover_listed = n as u32;
                            let mut max_seen = 0u32;
                            for &id in self.state.rec.ids.iter().take(n) {
                                max_seen = max_seen.max(id);
                            }
                            self.state.rec.max_seen_id = max_seen;
                            self.state.rec.phase = REC_SELECT;
                        }
                        // More runs than a listing holds: a store whose writes
                        // outran its retirements (2026-10-14, pi5-a: 13 009 run
                        // files, a write storm upstream). Quarantining here
                        // would make the orphans permanent, so the listed runs
                        // neither manifest slot names are deleted and the root
                        // listed again.
                        Err(StorageError::Overflow) => {
                            self.state.rec.phase = REC_OVERFLOW;
                        }
                        Err(e) => return Err(map_storage(e)),
                    }
                }
                REC_OVERFLOW => {
                    let listed = self.state.rec.ids;
                    if self.queue_listed_orphans(&listed)? == 0 {
                        return Err(StoreError::StorageFault);
                    }
                    self.state.rec.phase = REC_SWEEP;
                }
                REC_SELECT => {
                    // A/B SELECTION. Read both fixed slots, validate each
                    // independently, and take the valid one with the HIGHER
                    // INTERNAL generation (skipping one already refused). No
                    // enumeration, no deletion: the pair is permanent, so
                    // recovery knows both names a priori. Reading a slot is
                    // pure, so a read the device defers re-reads both.
                    self.state.recover_refusal = 1;
                    let mut found = false;
                    for slot in [MANIFEST_SLOT_A, MANIFEST_SLOT_B] {
                        if self.state.rec.has_bad && self.state.rec.bad_slot == slot {
                            continue;
                        }
                        let Some((gen, f, rc, rs, ai, at, hr, clock)) = self.read_manifest(slot)?
                        else {
                            continue;
                        };
                        if !found || gen > self.state.rec.winner_gen {
                            found = true;
                            let r = &mut self.state.rec;
                            r.winner_gen = gen;
                            r.winner_slot = slot;
                            r.floor = f;
                            r.run_count = rc as u32;
                            r.runs = rs;
                            r.applied_index = ai;
                            r.applied_term = at;
                            r.man_revision = hr;
                            r.man_clock = clock;
                        }
                    }
                    self.state.rec.has_winner = found;
                    self.state.rec.idx = 0;
                    self.state.rec.vstage = VERIFY_HEADER;
                    if found {
                        self.state.rec.phase = REC_VERIFY;
                    } else if self.state.rec.has_bad {
                        // The only usable slot named what the disk does not
                        // hold: quarantine, never an empty store in its place.
                        self.state.recover_refusal = self.state.rec.bad_rr;
                        return Err(StoreError::StorageFault);
                    } else {
                        // No usable manifest: the durable state is empty;
                        // every run present was never published and is an
                        // orphan.
                        self.state.rec.phase = REC_ORPHANS;
                    }
                }
                REC_VERIFY => {
                    // Every referenced run must exist and validate.
                    let i = self.state.rec.idx as usize;
                    if i >= self.state.rec.run_count as usize {
                        if self.state.rec.has_bad {
                            self.state.recover_fallbacks =
                                self.state.recover_fallbacks.wrapping_add(1);
                        }
                        self.state.rec.idx = 0;
                        self.state.rec.phase = REC_ORPHANS;
                        continue;
                    }
                    let meta = self
                        .state
                        .rec
                        .runs
                        .get(i)
                        .copied()
                        .unwrap_or(RunMeta::empty());
                    self.state.recover_refusal = 3;
                    let listed = self.state.rec.listed as usize;
                    let held = self
                        .state
                        .rec
                        .ids
                        .iter()
                        .take(listed)
                        .any(|&id| id == meta.id);
                    let verdict = if held {
                        self.verify_run_step(&meta)?
                    } else {
                        self.state.recover_missing_id = meta.id;
                        Verify::Bad
                    };
                    match verdict {
                        Verify::Slice => return Ok(false),
                        Verify::Good => {
                            self.state.rec.max_seen_id = self.state.rec.max_seen_id.max(meta.id);
                            self.state.rec.idx += 1;
                            self.state.rec.vstage = VERIFY_HEADER;
                        }
                        Verify::Bad => {
                            // The higher generation names a run the disk does
                            // not hold, or one that does not validate: the
                            // OTHER slot — the previous durable state — is
                            // served instead, loudly counted
                            // (`recover_fallbacks`): a generation whose runs
                            // did not outlive the power cut that followed its
                            // publish is a torn publish, and the log replays
                            // on top of the older manifest from its applied
                            // index. Only with no usable slot at all is it a
                            // quarantine. The next publish's generation stays
                            // above both.
                            if self.state.rec.has_bad {
                                return Err(StoreError::StorageFault);
                            }
                            self.state.rec.has_bad = true;
                            self.state.rec.bad_slot = self.state.rec.winner_slot;
                            self.state.rec.bad_gen = self.state.rec.winner_gen;
                            self.state.rec.bad_rr = self.state.recover_refusal;
                            self.state.rec.phase = REC_SELECT;
                        }
                    }
                }
                REC_ORPHANS => {
                    // Orphans (present but unreferenced) are deleted — every
                    // listed run when no manifest is usable.
                    self.state.recover_refusal = if self.state.rec.has_winner { 5 } else { 7 };
                    let listed = self.state.rec.listed as usize;
                    while (self.state.rec.idx as usize) < listed {
                        let id = self
                            .state
                            .rec
                            .ids
                            .get(self.state.rec.idx as usize)
                            .copied()
                            .unwrap_or(0);
                        let rc = self.state.rec.run_count as usize;
                        let referenced = self.state.rec.has_winner
                            && self.state.rec.runs.iter().take(rc).any(|m| m.id == id);
                        if !referenced {
                            // `Again`: asked again next step, here. A real
                            // refusal ends the attempt with the run queued,
                            // so the next attempt asks for it before it
                            // lists anything (SWEEP_BATCH).
                            match self.storage.delete(FileKind::Run, id) {
                                Ok(()) | Err(StorageError::NotFound) => {}
                                Err(StorageError::Again) => return Err(StoreError::Backpressure),
                                Err(e) => {
                                    if let Some(slot) = self.state.sweep_ids.get_mut(0) {
                                        *slot = id;
                                        self.state.sweep_count = 1;
                                        self.state.sweep_at = 0;
                                    }
                                    return Err(map_storage(e));
                                }
                            }
                        }
                        self.state.rec.idx += 1;
                    }
                    self.state.rec.idx = 0;
                    if self.state.rec.has_winner {
                        let rc = self.state.rec.run_count as usize;
                        self.state.runs = self.state.rec.runs;
                        self.state.run_count = rc as u32;
                        self.state.rec.phase = REC_FILTERS;
                    } else {
                        self.state.next_run_id = self.state.rec.max_seen_id + 1;
                        self.state.rec.phase = REC_WALK;
                    }
                }
                REC_FILTERS => {
                    // Load each run's persisted filter. One bounded 1 KiB
                    // read per run — versus the full record scan of every
                    // run that rebuilding from keys would cost on the boot
                    // path. `verify_run` has already validated the whole
                    // file (including this region, which the file CRC
                    // covers), so these bytes are known good before they
                    // are trusted.
                    self.state.recover_refusal = 6;
                    let rc = self.state.run_count as usize;
                    while (self.state.rec.idx as usize) < rc {
                        let i = self.state.rec.idx as usize;
                        let meta = self.state.runs.get(i).copied().unwrap_or(RunMeta::empty());
                        let foot = meta.file_len - RUN_FOOTER_LEN as u64;
                        let off = foot + RUN_FOOTER_BLOOM_OFF as u64;
                        let Some(bloom) = self.state.run_blooms.get_mut(i) else {
                            return Err(StoreError::StorageFault);
                        };
                        read_exact(&*self.storage, FileKind::Run, meta.id, off, bloom)?;
                        let ioff = off + BLOOM_BYTES as u64;
                        let Some(index) = self.state.run_index.get_mut(i) else {
                            return Err(StoreError::StorageFault);
                        };
                        read_exact(&*self.storage, FileKind::Run, meta.id, ioff, index)?;
                        self.state.rec.idx += 1;
                    }
                    let r = self.state.rec;
                    self.state.generation = r.winner_gen.max(if r.has_bad { r.bad_gen } else { 0 });
                    self.state.manifest_slot = r.winner_slot;
                    self.state.compaction_floor = r.floor;
                    self.state.durable_applied_index = r.applied_index;
                    self.state.durable_applied_term = r.applied_term;
                    // Baseline for the next flush: nothing newer than the
                    // manifest has been applied yet at recovery time.
                    self.state.pending_applied_index = r.applied_index;
                    self.state.pending_applied_term = r.applied_term;
                    // Restore the revision clock to the run-set's high
                    // water: the WAL tail beyond the manifest replays ON TOP
                    // of this, so run content stays visible and new versions
                    // keep ascending. Without this, a truncated WAL restarts
                    // the clock at the tail and every run version is
                    // "future".
                    self.state.durable_highest_revision = r.man_revision;
                    self.state.highest_revision = self.state.highest_revision.max(r.man_revision);
                    // Resume the clock the runs' deadlines are measured
                    // against. A WAL tick replayed on top only ever moves
                    // it forward (the host latches the maximum), so
                    // recovering it here can never place the clock ahead of
                    // the log.
                    self.state.durable_clock_ms = r.man_clock;
                    self.state.committed_clock_ms = self.state.committed_clock_ms.max(r.man_clock);
                    self.state.next_run_id = r.max_seen_id + 1;
                    self.state.rec.phase = REC_WALK;
                }
                REC_WALK => {
                    // Rebuild `highest_revision` from the surviving records
                    // (the walk also re-validates every block CRC), a
                    // bounded slice per call, resuming at the key it
                    // reached (inclusive: a key seen twice changes no max).
                    self.state.recover_refusal = 8;
                    if self.recover_walk()? {
                        self.state.highest_revision = self.state.rec.highest;
                        return Ok(true);
                    }
                    return Ok(false);
                }
                _ => {
                    self.state.rec.phase = REC_IDLE;
                    return Err(StoreError::StorageFault);
                }
            }
        }
    }

    /// One slice of the recovery walk: `Ok(true)` at its end.
    fn recover_walk(&mut self) -> Result<bool, StoreError> {
        let mut bound = [0u8; MAX_ENCODED_KEY];
        let bl = if self.state.rec.walk_started {
            let n = (self.state.rec.walk_key_len as usize).min(MAX_ENCODED_KEY);
            for (d, b) in bound.iter_mut().zip(self.state.rec.walk_key.iter()).take(n) {
                *d = *b;
            }
            n
        } else {
            0
        };
        // SAFETY: the state's merge, lent to this walk (see `point`): no
        // point read runs inside a whole-store walk, nor one walk inside
        // another, so nothing else holds it while this reference lives.
        let m = unsafe { &mut *self.state.point.get() };
        m.fill_filtered(
            self.state,
            &*self.storage,
            false,
            bound.get(..bl).unwrap_or(&[]),
            None,
        )?;
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut visited = 0u32;
        loop {
            let next = m.next(&self.state.mem_view(), &*self.storage, &mut kbuf, &mut vbuf)?;
            let Some((klen, _)) = next else {
                return Ok(true);
            };
            let ts = key_timestamp(&kbuf[..klen]);
            if ts > self.state.rec.highest {
                self.state.rec.highest = ts;
            }
            for (d, b) in self
                .state
                .rec
                .walk_key
                .iter_mut()
                .zip(kbuf.iter())
                .take(klen)
            {
                *d = *b;
            }
            self.state.rec.walk_key_len = klen as u16;
            self.state.rec.walk_started = true;
            visited += 1;
            if visited >= RECOVER_WALK_RECORDS {
                return Ok(false);
            }
        }
    }

    /// One bounded slice of validating a referenced run end to end:
    /// exact length, header/footer framing and versions, and the
    /// whole-file CRC against both the stored footer field and the
    /// manifest digest. Continues from `rec.vstage`/`voff`/`vcrc`; sets
    /// `recover_refusal` to the check being made (41 too short, 42
    /// header, 43 a read, 44 CRC, 45 trailing bytes, 46 footer), so a
    /// refusal says which.
    fn verify_run_step(&mut self, meta: &RunMeta) -> Result<Verify, StoreError> {
        let foot_len = RUN_FOOTER_LEN as u64;
        let id = meta.id;
        let storage = &*self.storage;
        let rec = &mut self.state.rec;
        let refusal = &mut self.state.recover_refusal;
        if rec.vstage == VERIFY_HEADER {
            *refusal = 41;
            if meta.file_len < RUN_HDR_LEN as u64 + foot_len {
                return Ok(Verify::Bad);
            }
            // Header framing first.
            let mut hdr = [0u8; RUN_HDR_LEN];
            *refusal = 43;
            read_or_bad(storage, id, 0, &mut hdr)?;
            *refusal = 42;
            if rd_u32(&hdr, 0) != RUN_MAGIC
                || rd_u16(&hdr, 4) != RUN_FORMAT_VERSION
                || rd_u16(&hdr, 8) != internal_key::KEY_FORMAT_VERSION
                || rd_u16(&hdr, 10) != state_store::STATE_STORE_CONTRACT_VERSION
            {
                return Ok(Verify::Bad);
            }
            rec.vstage = VERIFY_BODY;
            rec.voff = 0;
            rec.vcrc = CRC32_INIT;
        }
        if rec.vstage == VERIFY_BODY {
            // Rolling CRC over [0, file_len - 4), a bounded number of
            // chunks per call; the position and CRC carry across calls.
            let mut chunk = [0u8; 4096];
            let crc_end = meta.file_len - 4;
            let mut chunks = 0u32;
            while rec.voff < crc_end {
                if chunks >= VERIFY_STEP_CHUNKS {
                    return Ok(Verify::Slice);
                }
                let want = (crc_end - rec.voff).min(chunk.len() as u64) as usize;
                *refusal = 43;
                let part = chunk.get_mut(..want).unwrap_or(&mut []);
                if read_or_bad(storage, id, rec.voff, part)? == Verify::Bad {
                    return Ok(Verify::Bad);
                }
                rec.vcrc = crc32_update(rec.vcrc, part);
                rec.voff += want as u64;
                chunks += 1;
            }
            rec.vstage = VERIFY_TAIL;
        }
        // The tail checks are a handful of small reads, made together;
        // one the device defers re-makes them all (they are pure).
        let computed = crc32_finish(rec.vcrc);
        let crc_end = meta.file_len - 4;
        let mut tail = [0u8; 4];
        *refusal = 43;
        if read_or_bad(storage, id, crc_end, &mut tail)? == Verify::Bad {
            return Ok(Verify::Bad);
        }
        *refusal = 44;
        if computed != rd_u32(&tail, 0) || computed != meta.digest {
            return Ok(Verify::Bad);
        }
        // The file must end exactly at file_len.
        let mut probe = [0u8; 1];
        *refusal = 43;
        let n = match storage.read_at(FileKind::Run, id, meta.file_len, &mut probe) {
            Ok(n) => n,
            Err(StorageError::Again) => return Err(StoreError::Backpressure),
            Err(_) => return Ok(Verify::Bad),
        };
        *refusal = 45;
        if n != 0 {
            return Ok(Verify::Bad);
        }
        // Footer framing: its magic and record count lead it.
        let mut foot = [0u8; 8];
        *refusal = 43;
        if read_or_bad(storage, id, meta.file_len - foot_len, &mut foot)? == Verify::Bad {
            return Ok(Verify::Bad);
        }
        *refusal = 46;
        if rd_u32(&foot, 0) != RUN_FOOTER_MAGIC || rd_u32(&foot, 4) != meta.record_count {
            return Ok(Verify::Bad);
        }
        Ok(Verify::Good)
    }

    fn note_manifest(&self, slot: u32, verdict: u32, generation: u32, top_run: u32) {
        let i = if slot == MANIFEST_SLOT_A { 0 } else { 1 };
        self.state.manifest_seen[i].set(verdict | (generation << 4));
        self.state.manifest_runs[i].set(top_run);
    }

    /// Queue (up to SWEEP_BATCH of) the runs in `listed` that neither
    /// manifest slot names; the count queued. A recovery whose run listing
    /// overflowed calls it: the listing holds a full table of ids, and every
    /// one a manifest does not reference is an orphan however many more
    /// there are.
    fn queue_listed_orphans(&mut self, listed: &[u32]) -> Result<usize, StoreError> {
        let mut keep = [0u32; 2 * MAX_RUNS];
        let mut kept = 0usize;
        for slot in [MANIFEST_SLOT_A, MANIFEST_SLOT_B] {
            if let Some((_, _, rc, rs, ..)) = self.read_manifest(slot)? {
                for meta in rs.iter().take(rc) {
                    if let Some(k) = keep.get_mut(kept) {
                        *k = meta.id;
                        kept += 1;
                    }
                }
            }
        }
        let keep = keep.get(..kept).unwrap_or(&[]);
        let mut n = 0usize;
        for &id in listed {
            if n == SWEEP_BATCH {
                break;
            }
            if keep.contains(&id) {
                continue;
            }
            if let Some(slot) = self.state.sweep_ids.get_mut(n) {
                *slot = id;
                n += 1;
            }
        }
        self.state.sweep_count = n as u16;
        self.state.sweep_at = 0;
        Ok(n)
    }

    /// Delete the queued orphans in order. A delete that fails (E_AGAIN
    /// included) returns with the queue where it stood, so the next
    /// attempt asks for that same run first.
    fn drain_sweep(&mut self) -> Result<(), StoreError> {
        while self.state.sweep_at < self.state.sweep_count {
            let id = self
                .state
                .sweep_ids
                .get(usize::from(self.state.sweep_at))
                .copied()
                .unwrap_or(0);
            match self.storage.delete(FileKind::Run, id) {
                Ok(()) | Err(StorageError::NotFound) => {
                    self.state.sweep_at += 1;
                    self.state.recover_swept = self.state.recover_swept.wrapping_add(1);
                }
                Err(e) => return Err(map_storage(e)),
            }
        }
        self.state.sweep_count = 0;
        self.state.sweep_at = 0;
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
        reason = "two internal callers destructure in place; a named struct would outlive those uses"
    )]
    fn read_manifest(
        &self,
        slot: u32,
    ) -> Result<Option<(u32, u64, usize, [RunMeta; MAX_RUNS], u64, u64, u64, u64)>, StoreError>
    {
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
                    Err(StorageError::NotFound) => {
                        self.note_manifest(slot, 1, 0, 0);
                        return Ok(None);
                    }
                    Err(e) => return Err(map_storage(e)),
                };
            if n == 0 {
                break;
            }
            len += n;
        }
        if len < MANIFEST_HDR_LEN + 4 {
            self.note_manifest(slot, 2, 0, 0);
            return Ok(None);
        }
        let body = &buf[..len];
        if rd_u32(body, 0) != MANIFEST_MAGIC
            || rd_u16(body, 4) != MANIFEST_FORMAT_VERSION
            || rd_u16(body, 6) != internal_key::KEY_FORMAT_VERSION
            || rd_u16(body, 8) != state_store::STATE_STORE_CONTRACT_VERSION
        {
            self.note_manifest(slot, 3, 0, 0);
            return Ok(None);
        }
        let generation = rd_u32(body, 12);
        let run_count = rd_u32(body, 16) as usize;
        let floor = rd_u64(body, 20);
        let applied_index = rd_u64(body, 28);
        let applied_term = rd_u64(body, 36);
        let highest_revision = rd_u64(body, 44);
        let committed_clock_ms = rd_u64(body, 52);
        if run_count > MAX_RUNS {
            self.note_manifest(slot, 4, generation, 0);
            return Ok(None);
        }
        // The CRC sits immediately after the declared entries; the rest
        // of the fixed-size slot is inert padding. Length-driven, not
        // file-size-driven — a slot is always MANIFEST_SLOT_LEN long
        // regardless of how many runs it names.
        let end = MANIFEST_HDR_LEN + run_count * MANIFEST_ENTRY_LEN;
        if len < end + 4 {
            self.note_manifest(slot, 5, generation, 0);
            return Ok(None);
        }
        if crc32(&body[..end]) != rd_u32(body, end) {
            self.note_manifest(slot, 6, generation, 0);
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
        let top = runs.iter().take(run_count).map(|m| m.id).max().unwrap_or(0);
        self.note_manifest(slot, 7, generation, top);
        Ok(Some((
            generation,
            floor,
            run_count,
            runs,
            applied_index,
            applied_term,
            highest_revision,
            committed_clock_ms,
        )))
    }
}

/// A range page the device made wait after `emitted` entries: those are
/// the page (the caller asks on from its cursor, which the memo names), or
/// with none, `Backpressure` — ask the same request again.
fn range_held(emitted: usize, pos: usize, resume_cursor: u64) -> Result<ScanProgress, StoreError> {
    if emitted == 0 {
        return Err(StoreError::Backpressure);
    }
    Ok(ScanProgress {
        entries: emitted,
        bytes: pos,
        progress: Progress::InProgress {
            cursor: resume_cursor + emitted as u64,
        },
        paused: false,
    })
}

/// A capture step that met `e` after emitting `pos` bytes: those bytes
/// are the step's answer (the capture's position covers exactly them) and
/// the walk resumes next step; with nothing emitted, the error itself.
fn held_back(pos: usize, cursor: u64, e: StoreError) -> Result<(usize, Progress), StoreError> {
    if pos > 0 && e == StoreError::Backpressure {
        Ok((pos, Progress::InProgress { cursor }))
    } else {
        Err(e)
    }
}

/// What one slice of a run's verification found.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Verify {
    /// Whole and valid.
    Good,
    /// Absent, short, torn or corrupt: refused.
    Bad,
    /// A bounded slice done; asked again, it continues.
    Slice,
}

/// Fill `out` exactly from run `id` at `offset` for verification:
/// `Good` when it read whole, `Bad` when the bytes are not there (short,
/// or the file refuses), `Err(Backpressure)` when the device cannot say
/// yet (ask again).
fn read_or_bad<S: RunStorage>(
    storage: &S,
    id: u32,
    offset: u64,
    out: &mut [u8],
) -> Result<Verify, StoreError> {
    match read_exact(storage, FileKind::Run, id, offset, out) {
        Ok(()) => Ok(Verify::Good),
        Err(StoreError::Backpressure) => Err(StoreError::Backpressure),
        Err(_) => Ok(Verify::Bad),
    }
}

impl<'a, S: RunStorage> DiskStore<'a, S> {
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
        self.state.install.destroy = 0;
        // A flush the install had in flight wrote the records being
        // dropped: abandoned with them (its run is an orphan).
        self.state.flush.active = false;
        self.state.flush.phase = FLUSH_PHASE_RECORDS;
        self.state.flush.stage = STAGE_CREATE;
        self.state.mem_count = 0;
    }

    /// Remove the local materialization before an install takes its
    /// stream: every manifest file, then every run — files first, so a
    /// crash mid-install recovers to "no usable manifest" = empty,
    /// forcing a re-install rather than serving a mix. Each listing and
    /// each delete may answer `Again`; the destroy then stands where it
    /// was and the next call continues it (the listing into the same
    /// buffer, the delete in flight asked again).
    fn install_destroy(&mut self) -> Result<(), StoreError> {
        loop {
            match self.state.install.destroy {
                DESTROY_LIST_MAN | DESTROY_LIST_RUN => {
                    let kind = if self.state.install.destroy == DESTROY_LIST_MAN {
                        FileKind::Manifest
                    } else {
                        FileKind::Run
                    };
                    let n = self
                        .storage
                        .list(kind, &mut self.state.rec.ids)
                        .map_err(map_storage)?;
                    self.state.install.destroy_n = n as u32;
                    self.state.install.destroy_at = 0;
                    self.state.install.destroy += 1;
                }
                DESTROY_DEL_MAN | DESTROY_DEL_RUN => {
                    let kind = if self.state.install.destroy == DESTROY_DEL_MAN {
                        FileKind::Manifest
                    } else {
                        FileKind::Run
                    };
                    while self.state.install.destroy_at < self.state.install.destroy_n {
                        let id = self
                            .state
                            .rec
                            .ids
                            .get(self.state.install.destroy_at as usize)
                            .copied()
                            .unwrap_or(0);
                        match self.storage.delete(kind, id) {
                            Ok(()) | Err(StorageError::NotFound) => {}
                            Err(e) => return Err(map_storage(e)),
                        }
                        self.state.install.destroy_at += 1;
                    }
                    if self.state.install.destroy == DESTROY_DEL_RUN {
                        self.state.install.destroy = 0;
                        return Ok(());
                    }
                    self.state.install.destroy = DESTROY_NAME_A;
                }
                // The manifests' removal is published before anything else
                // is: a power cut must never bring back a pre-install
                // manifest — its generation may stand above the install's
                // own — over a half-installed store.
                DESTROY_NAME_A | DESTROY_NAME_B => {
                    let slot = if self.state.install.destroy == DESTROY_NAME_A {
                        MANIFEST_SLOT_A
                    } else {
                        MANIFEST_SLOT_B
                    };
                    match self.storage.fsync_name(FileKind::Manifest, slot) {
                        Ok(()) | Err(StorageError::NotFound) => {}
                        Err(e) => return Err(map_storage(e)),
                    }
                    self.state.install.destroy = if slot == MANIFEST_SLOT_A {
                        DESTROY_NAME_B
                    } else {
                        DESTROY_LIST_RUN
                    };
                }
                _ => {
                    self.state.install.destroy = 0;
                    return Ok(());
                }
            }
        }
    }

    /// Is a snapshot capture holding the apply/flush exclusion? A
    /// transfer capture holds neither (see [`Self::transfer_active`]).
    pub fn capture_active(&self) -> bool {
        self.state.capture.active && !self.state.capture.transfer
    }

    /// Is a transfer capture under way? It refuses no write; it holds
    /// compaction off, so no version at or below its revision is
    /// reclaimed while it reads.
    pub fn transfer_active(&self) -> bool {
        self.state.capture.active && self.state.capture.transfer
    }

    /// Begin a TRANSFER capture: a portable copy of the store for another
    /// replica, labelled `applied_index` and read as of `as_of` — the
    /// revision that position left the store at (the caller's: only it
    /// knows where a log entry's writes end). Unlike the local capture it
    /// excludes nothing but
    /// compaction — writes are applied throughout, above the copy's
    /// revision, and the receiver replays them from the log after it.
    /// Stepped by [`Self::transfer_step`]; abandoned with
    /// [`Self::transfer_abort`].
    pub fn transfer_begin(&mut self, applied_index: u64, as_of: u64) -> Result<(), StoreError> {
        if self.state.capture.active || self.state.install.active || self.state.compact.active {
            return Err(StoreError::Backpressure);
        }
        if as_of > self.state.highest_revision {
            return Err(StoreError::Malformed);
        }
        self.state.capture.active = true;
        self.state.capture.transfer = true;
        self.state.capture.counting = true;
        self.state.capture.header_sent = false;
        self.state.capture.range_generation = self.state.range_generation;
        self.state.capture.applied_index = applied_index;
        self.state.capture.next_ordinal = 0;
        self.state.capture.total_entries = 0;
        self.state.capture.as_of = as_of;
        self.state.capture.resume_len = 0;
        Ok(())
    }

    /// Abandon a transfer capture (the receiver went away, or the caller
    /// starts over). Read-only: nothing on disk changes.
    pub fn transfer_abort(&mut self) {
        if self.state.capture.transfer {
            self.state.capture.active = false;
            self.state.capture.transfer = false;
        }
    }

    /// One bounded step of a transfer capture: visits at most `budget`
    /// merged records, resuming after the last one visited. The counting
    /// pass writes nothing (`(0, InProgress)`); the emitting pass writes
    /// the snapshot stream v1 — the header (stating the count), then the
    /// records at or below the copy's revision, whole — into `out`, and
    /// answers `Done` with the stream's last bytes. The stream installs
    /// with [`StateStore::install`] like any other.
    pub fn transfer_step(
        &mut self,
        out: &mut [u8],
        budget: u32,
    ) -> Result<(usize, Progress), StoreError> {
        if !(self.state.capture.active && self.state.capture.transfer) {
            return Err(StoreError::Malformed);
        }
        // A flush in flight has its records in two places at once; the
        // walk waits for it to publish.
        if self.state.flush.active {
            return Err(StoreError::Backpressure);
        }
        let mut pos = 0usize;
        if !self.state.capture.counting && !self.state.capture.header_sent {
            if out.len() < SNAP_HDR_LEN {
                return Err(StoreError::OutputTooSmall);
            }
            out[0..4].copy_from_slice(&SNAP_MAGIC.to_le_bytes());
            out[4..6].copy_from_slice(&SNAP_FORMAT_VERSION.to_le_bytes());
            out[6..8].copy_from_slice(&0u16.to_le_bytes());
            out[8..16].copy_from_slice(&self.state.capture.applied_index.to_le_bytes());
            out[16..20].copy_from_slice(&self.state.capture.range_generation.to_le_bytes());
            out[20..24].copy_from_slice(&(self.state.capture.total_entries as u32).to_le_bytes());
            out[24..32].copy_from_slice(&self.state.capture.as_of.to_le_bytes());
            out[32..40].copy_from_slice(&self.state.compaction_floor.to_le_bytes());
            out[SNAP_HDR_CLOCK_OFF..SNAP_HDR_LEN]
                .copy_from_slice(&self.state.committed_clock_ms.to_le_bytes());
            pos = SNAP_HDR_LEN;
            self.state.capture.header_sent = true;
        }
        let as_of = self.state.capture.as_of;
        let mut resume = [0u8; MAX_ENCODED_KEY];
        let rl = self.state.capture.resume_len as usize;
        resume[..rl].copy_from_slice(&self.state.capture.resume[..rl]);
        // SAFETY: the state's merge, lent to this walk (see `point`): no
        // point read runs inside a whole-store walk, nor one walk inside
        // another, so nothing else holds it while this reference lives.
        let m = unsafe { &mut *self.state.point.get() };
        // A read the device cannot take now ends the step with what it
        // has emitted (the header included): the capture's position moves
        // only with each record emitted, so the next step resumes after
        // the last one — nothing emitted is lost, nothing is sent twice.
        let cursor = self.state.capture.next_ordinal;
        if let Err(e) = m.fill_filtered(self.state, &*self.storage, true, &resume[..rl], None) {
            return held_back(pos, cursor, e);
        }
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut visited = 0u32;
        loop {
            if visited >= budget {
                return Ok((
                    pos,
                    Progress::InProgress {
                        cursor: self.state.capture.next_ordinal,
                    },
                ));
            }
            let next = match m.next(&self.state.mem_view(), &*self.storage, &mut kbuf, &mut vbuf) {
                Ok(n) => n,
                Err(e) => return held_back(pos, self.state.capture.next_ordinal, e),
            };
            let Some((klen, vlen)) = next else {
                if self.state.capture.counting {
                    // Counted: the emitting pass starts from the first key.
                    self.state.capture.counting = false;
                    self.state.capture.resume_len = 0;
                    return Ok((pos, Progress::InProgress { cursor: 0 }));
                }
                if self.state.capture.next_ordinal != self.state.capture.total_entries {
                    // The set read as of the revision changed under the
                    // walk — never by design; refuse rather than ship a
                    // copy the receiver would reject at its end.
                    self.transfer_abort();
                    return Err(StoreError::Malformed);
                }
                self.state.capture.active = false;
                self.state.capture.transfer = false;
                return Ok((pos, Progress::Done));
            };
            // The bound is inclusive: the key last visited comes back first.
            if rl > 0 && kbuf[..klen] == resume[..rl] {
                continue;
            }
            visited += 1;
            // A version newer than the copy's revision was applied since.
            let at_or_below = klen >= internal_key::VERSION_SUFFIX_LEN && {
                let ts_at = klen - internal_key::VERSION_SUFFIX_LEN;
                let mut ts = [0u8; 8];
                ts.copy_from_slice(&kbuf[ts_at..ts_at + 8]);
                !u64::from_be_bytes(ts) <= as_of
            };
            if self.state.capture.counting {
                if at_or_below {
                    self.state.capture.total_entries += 1;
                }
            } else if at_or_below {
                let need = RECORD_FIXED + klen + vlen;
                if pos + need > out.len() {
                    if pos == 0 {
                        return Err(StoreError::OutputTooSmall);
                    }
                    // Not taken: the next step resumes before it.
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
                self.state.capture.next_ordinal += 1;
            }
            self.state.capture.resume[..klen].copy_from_slice(&kbuf[..klen]);
            self.state.capture.resume_len = klen as u32;
        }
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

impl<S: RunStorage> DiskStore<'_, S> {
    /// The budgeted body of [`KvStateStore::scan_versions`], once the
    /// window has been checked. Every return leaves `state.scan` exact
    /// for its ordinal; an `Err` may not (the caller resets it).
    fn scan_versions_walk(
        &mut self,
        span: KeySpan<'_>,
        from_revision: Revision,
        hi: u64,
        resume_cursor: u64,
        limit: usize,
        out: &mut [u8],
    ) -> Result<ScanProgress, StoreError> {
        // Every block this call crosses counts, positioning included —
        // loaded from storage or served by the block cache alike.
        let blocks0 = self
            .state
            .blocks_read
            .get()
            .wrapping_add(self.state.block_cache.hits.get());
        // No run holds a version above the durable high-water mark, so
        // a window that starts at or above it is answered from the
        // memtable alone — zero run I/O for a feed that is caught up
        // with everything flushed.
        let include_runs = from_revision < self.state.durable_highest_revision;

        // ── Position: resume from the memo, or start a fresh walk ──
        let sc = &self.state.scan;
        let memo_hit = sc.valid
            && sc.from == from_revision
            && sc.hi == hi
            && sc.ordinal <= resume_cursor
            && sc.span_start[..sc.span_start_len as usize] == *span.start
            && sc.span_end[..sc.span_end_len as usize] == *span.end;
        // Qualifying records still to skip before emitting: the whole
        // cursor on a fresh walk, the gap between the memo's position
        // and the cursor on a hit (zero in the steady state).
        let mut to_skip;
        // Keys at or below the resume key are behind the walk. On a
        // fresh walk the bound is the span start (exclusive: it is the
        // first key wanted); on a hit it is the last key consumed
        // (inclusive: it has been consumed).
        let bound_inclusive;
        let mut boundbuf = [0u8; MAX_ENCODED_KEY];
        let bound_len;
        if memo_hit {
            to_skip = resume_cursor - sc.ordinal;
            if sc.pending_klen > 0 {
                // The walk stands past the stashed record, which is
                // yielded from the stash below; the sources resume
                // after it, not after the last key emitted.
                bound_len = sc.pending_klen as usize;
                boundbuf[..bound_len].copy_from_slice(&sc.pending_key[..bound_len]);
                bound_inclusive = true;
            } else if sc.last_key_len > 0 {
                bound_len = sc.last_key_len as usize;
                boundbuf[..bound_len].copy_from_slice(&sc.last_key[..bound_len]);
                bound_inclusive = true;
            } else {
                bound_len = span.start.len();
                boundbuf[..bound_len].copy_from_slice(span.start);
                bound_inclusive = false;
            }
            if !(sc.live && sc.generation == self.state.generation) {
                // The run set changed under the memo (a flush published,
                // a compaction adopted). The key is still exact; reopen
                // at it — one block per run now, the rest of the
                // positioning inside this call's budget.
                let generation = self.state.generation;
                let (sc, src) = self.state.scan_parts();
                // Not live until it is whole: a failed open leaves the
                // key memo, which the next call reopens from.
                sc.live = false;
                sc.merge.open_lazy_in(
                    &src,
                    &*self.storage,
                    include_runs,
                    &boundbuf[..bound_len],
                )?;
                sc.live = true;
                sc.generation = generation;
            }
        } else {
            bound_len = span.start.len();
            boundbuf[..bound_len].copy_from_slice(span.start);
            bound_inclusive = false;
            let generation = self.state.generation;
            let (sc, src) = self.state.scan_parts();
            sc.live = false;
            sc.merge
                .open_lazy_in(&src, &*self.storage, include_runs, span.start)?;
            sc.valid = true;
            sc.live = true;
            sc.page_valid = false;
            sc.pending_klen = 0;
            sc.generation = generation;
            sc.from = from_revision;
            sc.hi = hi;
            sc.ordinal = 0;
            sc.last_key_len = 0;
            sc.span_start_len = span.start.len() as u16;
            sc.span_start[..span.start.len()].copy_from_slice(span.start);
            sc.span_end_len = span.end.len() as u16;
            sc.span_end[..span.end.len()].copy_from_slice(span.end);
            to_skip = resume_cursor;
        }
        let bound = &boundbuf[..bound_len];

        // ── Walk, bounded ─────────────────────────────────────────
        // Disjoint borrows of the arena: the merge advances in place
        // while the memtable is read through `MemView`.
        let DiskState {
            scan,
            order,
            entries,
            mem_count,
            blocks_read,
            block_cache,
            ..
        } = &mut *self.state;
        let mem = MemView {
            count: *mem_count as usize,
            order,
            entries,
            blocks_read,
            cache: block_cache,
        };
        // The memtable position is re-derived on every call: entries
        // admitted or released since the merge was built have shifted
        // it, and the key memo — not the index — is what is exact.
        scan.merge.mem_pos = mem.lower_bound(bound);
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut visited = 0u32;
        let mut emitted = 0usize;
        let mut pos = 0usize;
        // `ordinal` tracks the merge position in qualifying records; it
        // equals `resume_cursor + emitted` once the skip is spent.
        let mut ordinal = resume_cursor - to_skip;
        let in_progress = |scan: &mut ScanState, ordinal, emitted: usize, pos, paused| {
            scan.ordinal = ordinal;
            Ok(ScanProgress {
                entries: emitted,
                bytes: pos,
                progress: Progress::InProgress {
                    cursor: resume_cursor + emitted as u64,
                },
                paused,
            })
        };
        loop {
            // A record stashed by the previous call (it did not fit
            // that page) comes first; otherwise the merge's next.
            let (klen, vlen) = if scan.pending_klen > 0 {
                let (kl, vl) = (scan.pending_klen as usize, scan.pending_vlen as usize);
                kbuf[..kl].copy_from_slice(&scan.pending_key[..kl]);
                vbuf[..vl].copy_from_slice(&scan.pending_val[..vl]);
                scan.pending_klen = 0;
                (kl, vl)
            } else {
                // Budget check FIRST for every record after the first:
                // a call always makes progress, but never starts a
                // block it has no budget for. Pausing leaves the merge
                // standing just past the last key consumed, so the next
                // call continues in place.
                if visited > 0
                    && (mem
                        .blocks_read
                        .get()
                        .wrapping_add(mem.cache.hits.get())
                        .wrapping_sub(blocks0)
                        >= SCAN_STEP_BLOCKS as u64
                        || visited >= SCAN_STEP_RECORDS)
                {
                    return in_progress(scan, ordinal, emitted, pos, true);
                }
                let next = match scan.merge.next(&mem, &*self.storage, &mut kbuf, &mut vbuf) {
                    Ok(n) => n,
                    // The device deferred a block read. The merge holds
                    // its place (a deferred load is retried before any
                    // head is chosen) and the memo stands at the last key
                    // consumed: pause exactly as the step budget does, and
                    // the re-driven command continues here.
                    Err(StoreError::Backpressure) => {
                        return in_progress(scan, ordinal, emitted, pos, true);
                    }
                    Err(e) => return Err(e),
                };
                let Some(next) = next else {
                    scan.valid = false;
                    return Ok(ScanProgress::done(emitted, pos));
                };
                if next.0 < MIN_ENCODED_KEY {
                    return Err(StoreError::StorageFault);
                }
                // Behind the walk: the lazily opened sources' lead-in,
                // or the resume key itself. Costs its block, so it is
                // a visit for the budget; it moves no memo.
                let k = &kbuf[..next.0];
                if *k < *bound || (bound_inclusive && *k == *bound) {
                    visited += 1;
                    continue;
                }
                next
            };
            if !span.end.is_empty() && kbuf[..klen] >= *span.end {
                scan.valid = false;
                return Ok(ScanProgress::done(emitted, pos));
            }
            visited += 1;
            // No per-key `decided` latch and no tombstone filter — that
            // is the entire difference from `scan_at`. Every version in
            // the window is an event that happened, including deletes,
            // and including several versions of the same key.
            let ts = key_timestamp(&kbuf[..klen]);
            if ts <= from_revision || ts > hi {
                scan.note_visited(&kbuf[..klen]);
                continue;
            }
            if to_skip > 0 {
                to_skip -= 1;
                ordinal += 1;
                scan.note_visited(&kbuf[..klen]);
                continue; // already emitted in an earlier step
            }
            let need = RECORD_FIXED + klen + vlen;
            if pos + need > out.len() {
                // Pulled, not emitted: stash it for the next call. The
                // merge stays live, the memo stays at the previous key
                // and ordinal, and nothing is rebuilt at the page edge.
                scan.pending_klen = klen as u16;
                scan.pending_vlen = vlen as u16;
                scan.pending_key[..klen].copy_from_slice(&kbuf[..klen]);
                scan.pending_val[..vlen].copy_from_slice(&vbuf[..vlen]);
                if pos == 0 {
                    scan.ordinal = ordinal;
                    return Err(StoreError::OutputTooSmall);
                }
                return in_progress(scan, ordinal, emitted, pos, false);
            }
            out[pos..pos + 2].copy_from_slice(&(klen as u16).to_le_bytes());
            out[pos + 2..pos + 6].copy_from_slice(&(vlen as u32).to_le_bytes());
            out[pos + 6..pos + 6 + klen].copy_from_slice(&kbuf[..klen]);
            out[pos + 6 + klen..pos + need].copy_from_slice(&vbuf[..vlen]);
            pos += need;
            emitted += 1;
            ordinal += 1;
            scan.note_visited(&kbuf[..klen]);
            if emitted >= limit {
                // Exactly the caller's page, and not a record more: the
                // merge stands just past the last emitted record, so the
                // memo names the ordinal the caller will ask for next
                // and the next page continues in place.
                return in_progress(scan, ordinal, emitted, pos, false);
            }
        }
    }
}

impl<S: RunStorage> DiskStore<'_, S> {
    /// The body of [`KvStateStore::scan_at`] once the floor has been
    /// checked. Every `Ok` return leaves `state.range` exact for its
    /// ordinal; an `Err` may not (the caller resets it).
    fn scan_at_walk(
        &mut self,
        span: KeySpan<'_>,
        revn: u64,
        resume_cursor: u64,
        limit: usize,
        out: &mut [u8],
    ) -> Result<ScanProgress, StoreError> {
        // ── Position: resume from the memo, or start a fresh walk ──
        let rm = &self.state.range;
        let memo_hit = rm.valid
            && rm.revision == revn
            && rm.ordinal <= resume_cursor
            && rm.span_start[..rm.span_start_len as usize] == *span.start
            && rm.span_end[..rm.span_end_len as usize] == *span.end;
        let mut to_skip;
        let mut bound_inclusive;
        let mut boundbuf = [0u8; MAX_ENCODED_KEY];
        let mut bound_len;
        // Per-key decision state, restored from the memo on a hit.
        let mut prefix = [0u8; MAX_ENCODED_KEY];
        let mut prefix_len = 0usize;
        let mut decided = false;
        if memo_hit {
            to_skip = resume_cursor - rm.ordinal;
            if rm.pending_klen > 0 {
                bound_len = rm.pending_klen as usize;
                boundbuf[..bound_len].copy_from_slice(&rm.pending_key[..bound_len]);
                bound_inclusive = true;
            } else if rm.last_key_len > 0 {
                bound_len = rm.last_key_len as usize;
                boundbuf[..bound_len].copy_from_slice(&rm.last_key[..bound_len]);
                bound_inclusive = true;
            } else {
                bound_len = span.start.len();
                boundbuf[..bound_len].copy_from_slice(span.start);
                bound_inclusive = false;
            }
            if rm.last_key_len as usize >= MIN_ENCODED_KEY {
                prefix_len = rm.last_key_len as usize - VERSION_SUFFIX_LEN;
                prefix[..prefix_len].copy_from_slice(&rm.last_key[..prefix_len]);
                decided = rm.decided;
            }
            if !(rm.live && rm.generation == self.state.generation) {
                let generation = self.state.generation;
                let (rm, src) = self.state.range_parts();
                // Not live until it is whole (as the version scan's).
                rm.live = false;
                rm.merge
                    .open_lazy_in(&src, &*self.storage, true, &boundbuf[..bound_len])?;
                rm.live = true;
                rm.generation = generation;
            }
        } else {
            bound_len = span.start.len();
            boundbuf[..bound_len].copy_from_slice(span.start);
            bound_inclusive = false;
            let generation = self.state.generation;
            let (rm, src) = self.state.range_parts();
            rm.live = false;
            rm.merge
                .open_lazy_in(&src, &*self.storage, true, span.start)?;
            rm.valid = true;
            rm.live = true;
            rm.pending_klen = 0;
            rm.decided = false;
            rm.generation = generation;
            rm.revision = revn;
            rm.ordinal = 0;
            rm.last_key_len = 0;
            rm.span_start_len = span.start.len() as u16;
            rm.span_start[..span.start.len()].copy_from_slice(span.start);
            rm.span_end_len = span.end.len() as u16;
            rm.span_end[..span.end.len()].copy_from_slice(span.end);
            to_skip = resume_cursor;
        }
        // ── Walk ─────────────────────────────────────────────────
        let storage = &*self.storage;
        let (rm, src) = self.state.range_parts();
        let mem = &src.mem;
        rm.merge.mem_pos = mem.lower_bound(&boundbuf[..bound_len]);
        // Older versions of the key last decided, pulled and passed over
        // in a row (see `SKIP_STALE_VERSIONS`).
        let mut stale = 0usize;
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut emitted = 0usize;
        let mut pos = 0usize;
        let mut ordinal = resume_cursor - to_skip;
        loop {
            let (klen, vlen) = if rm.pending_klen > 0 {
                let (kl, vl) = (rm.pending_klen as usize, rm.pending_vlen as usize);
                kbuf[..kl].copy_from_slice(&rm.pending_key[..kl]);
                vbuf[..vl].copy_from_slice(&rm.pending_val[..vl]);
                rm.pending_klen = 0;
                (kl, vl)
            } else {
                let next = match rm.merge.next(mem, storage, &mut kbuf, &mut vbuf) {
                    Ok(n) => n,
                    // The device deferred a block. The merge holds its
                    // place and the memo stands at the last record
                    // processed: the page so far is the answer, or — with
                    // nothing in it — `Backpressure`, and the same request
                    // asked again continues here (`scan_at` keeps the memo).
                    Err(StoreError::Backpressure) => {
                        return range_held(emitted, pos, resume_cursor);
                    }
                    Err(e) => return Err(e),
                };
                let Some(next) = next else {
                    rm.valid = false;
                    return Ok(ScanProgress::done(emitted, pos));
                };
                if next.0 < MIN_ENCODED_KEY {
                    return Err(StoreError::StorageFault);
                }
                let k = &kbuf[..next.0];
                let bound = &boundbuf[..bound_len];
                if *k < *bound || (bound_inclusive && *k == *bound) {
                    continue; // the lazily opened sources' lead-in
                }
                next
            };
            if !span.end.is_empty() && kbuf[..klen] >= *span.end {
                rm.valid = false;
                return Ok(ScanProgress::done(emitted, pos));
            }
            // The decision state as of BEFORE this record — what the
            // memo must hold if this record ends up stashed unprocessed.
            let decided_before = decided;
            let plen = klen - VERSION_SUFFIX_LEN;
            if plen != prefix_len || kbuf[..plen] != prefix[..prefix_len] {
                prefix[..plen].copy_from_slice(&kbuf[..plen]);
                prefix_len = plen;
                decided = false;
                stale = 0;
            }
            let mut emit = true;
            if decided {
                emit = false; // an older version of an already-decided key
                stale += 1;
                if stale >= SKIP_STALE_VERSIONS {
                    // A key rewritten often keeps every retained version,
                    // newest first, and a walk that pulls them one by one
                    // to pass over them reads every block they fill — a
                    // node's lease is dozens. Seek every source past this
                    // key instead: its sparse index names the block, the
                    // memtable its bound.
                    boundbuf[..plen].copy_from_slice(&kbuf[..plen]);
                    for b in &mut boundbuf[plen..plen + VERSION_SUFFIX_LEN] {
                        *b = 0xFF;
                    }
                    bound_len = plen + VERSION_SUFFIX_LEN;
                    bound_inclusive = true;
                    // Not live until whole: a seek the device defers part-way
                    // is reopened from the key memo by the next call.
                    rm.live = false;
                    match rm
                        .merge
                        .open_lazy_in(&src, storage, true, &boundbuf[..bound_len])
                    {
                        Ok(()) => rm.live = true,
                        Err(StoreError::Backpressure) => {
                            rm.last_key[..klen].copy_from_slice(&kbuf[..klen]);
                            rm.last_key_len = klen as u16;
                            rm.decided = decided;
                            rm.ordinal = ordinal;
                            return range_held(emitted, pos, resume_cursor);
                        }
                        Err(e) => return Err(e),
                    }
                    stale = 0;
                }
            } else if key_timestamp(&kbuf[..klen]) > revn {
                emit = false; // not yet visible at this revision
            } else {
                decided = true;
                if key_kind_byte(&kbuf[..klen]) == ValueKind::PointTombstone as u8 {
                    emit = false; // deleted at this revision
                } else if to_skip > 0 {
                    to_skip -= 1;
                    ordinal += 1;
                    emit = false; // already emitted in an earlier step
                }
            }
            if emit {
                let need = RECORD_FIXED + klen + vlen;
                if pos + need > out.len() {
                    // Pulled, not emitted: stash it, unprocessed, for
                    // the next page. The merge stays live and the memo
                    // stays at the previous record's state.
                    rm.pending_klen = klen as u16;
                    rm.pending_vlen = vlen as u16;
                    rm.pending_key[..klen].copy_from_slice(&kbuf[..klen]);
                    rm.pending_val[..vlen].copy_from_slice(&vbuf[..vlen]);
                    rm.decided = decided_before;
                    rm.ordinal = ordinal;
                    if pos == 0 {
                        return Err(StoreError::OutputTooSmall);
                    }
                    return Ok(ScanProgress {
                        entries: emitted,
                        bytes: pos,
                        progress: Progress::InProgress {
                            cursor: resume_cursor + emitted as u64,
                        },
                        paused: false,
                    });
                }
                out[pos..pos + 2].copy_from_slice(&(klen as u16).to_le_bytes());
                out[pos + 2..pos + 6].copy_from_slice(&(vlen as u32).to_le_bytes());
                out[pos + 6..pos + 6 + klen].copy_from_slice(&kbuf[..klen]);
                out[pos + 6 + klen..pos + need].copy_from_slice(&vbuf[..vlen]);
                pos += need;
                emitted += 1;
                ordinal += 1;
            }
            rm.last_key[..klen].copy_from_slice(&kbuf[..klen]);
            rm.last_key_len = klen as u16;
            rm.decided = decided;
            rm.ordinal = ordinal;
            if emitted >= limit {
                // Exactly the caller's page: the memo names the ordinal
                // the caller asks for next, so the next page continues
                // in place instead of re-walking to a cursor it ran past.
                return Ok(ScanProgress {
                    entries: emitted,
                    bytes: pos,
                    progress: Progress::InProgress {
                        cursor: resume_cursor + emitted as u64,
                    },
                    paused: false,
                });
            }
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
        if revision != 0 && revision < self.state.read_floor() {
            return Err(StoreError::Compacted);
        }
        let revn = normalize_revision(revision);
        // Seek to the version wanted, not to the key. Versions sort
        // newest first (the timestamp is stored inverted), so a walk
        // started at the key's prefix reads every version NEWER than
        // `revn` before reaching the one it wants — for an as-of read
        // of a hot key (replay re-running an old write against a lease
        // renewed thousands of times since) that is the whole key's
        // history, hundreds of blocks per lookup. The bound
        // `prefix ++ !revn` sorts after every newer version and before
        // every version at or below `revn` (the kind byte that follows
        // only orders among equals), so the first record at or past it
        // is the answer, and the sparse index lands within one stride
        // of it. The Bloom pre-check still asks about the key.
        let mut bound = [0u8; MAX_ENCODED_KEY];
        bound[..key.len()].copy_from_slice(key);
        bound[key.len()..key.len() + 8].copy_from_slice(&(!revn).to_be_bytes());
        let bound = &bound[..key.len() + 8];
        // SAFETY: the point merge is only ever borrowed here, for this
        // read, and no point read runs inside another — the reference
        // ends with this function; `fill_filtered` and `next` read the
        // rest of the state, never `point`.
        let m = unsafe { &mut *self.state.point.get() };
        m.fill_filtered(self.state, &*self.storage, true, bound, Some(key))?;
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        loop {
            let Some((klen, vlen)) =
                m.next(&self.state.mem_view(), &*self.storage, &mut kbuf, &mut vbuf)?
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
    /// compaction because neither changes visible content. The page
    /// after the last one answered continues from the position memo
    /// ([`RangeMemo`]); any other cursor re-walks the merge and skips
    /// that many visible entries (bounded by the provider's fixed
    /// capacities).
    fn scan_at(
        &mut self,
        span: KeySpan<'_>,
        revision: Revision,
        resume_cursor: u64,
        limit: usize,
        out: &mut [u8],
    ) -> Result<ScanProgress, StoreError> {
        if revision != 0 && revision < self.state.read_floor() {
            return Err(StoreError::Compacted);
        }
        let r = self.scan_at_walk(
            span,
            normalize_revision(revision),
            resume_cursor,
            limit,
            out,
        );
        if matches!(r, Err(e) if e != StoreError::OutputTooSmall && e != StoreError::Backpressure) {
            // Records were consumed and the ordinal not published:
            // forget the position rather than resume it wrongly. A device
            // wait is not that: the memo stands at the last record
            // processed, and the next call resumes there.
            self.state.range_reset();
        }
        r
    }

    fn scan_versions(
        &mut self,
        span: KeySpan<'_>,
        from_revision: Revision,
        to_revision: Revision,
        resume_cursor: u64,
        limit: usize,
        out: &mut [u8],
    ) -> Result<ScanProgress, StoreError> {
        // The window's LOWER bound is what the floor has to clear.
        // Checking the upper bound instead would accept a window whose
        // early events had already been reclaimed, and the caller would
        // receive a partial history it could not tell from a whole one.
        // A Compact at C still serves history from C on: a window opening
        // at C (`from` = C - 1) is whole.
        if from_revision < self.state.compaction_floor
            || from_revision.saturating_add(1) < self.state.compact_revision
        {
            return Err(StoreError::Compacted);
        }
        let hi = normalize_revision(to_revision);
        if from_revision >= hi {
            // Empty or inverted window: nothing happened in it. This is
            // a legitimate answer (a watcher that reconnects instantly
            // has missed nothing), not an error.
            return Ok(ScanProgress::done(0, 0));
        }
        if from_revision >= self.state.highest_revision {
            // Nothing has been materialized above `from`, so no record
            // can qualify: the answer is known without opening a single
            // run. This is the state a caught-up feed spends nearly all
            // its time in, and it must cost nothing — a walk here is
            // O(stored) work to prove an empty window empty.
            self.state.scan_reset();
            return Ok(ScanProgress::done(0, 0));
        }
        let r = self.scan_versions_walk(span, from_revision, hi, resume_cursor, limit, out);
        if matches!(r, Err(e) if e != StoreError::OutputTooSmall && e != StoreError::Backpressure) {
            // A fault mid-walk leaves the memo's position and its
            // ordinal out of step (records were consumed, the ordinal
            // was not published). Forget it: the next request walks
            // fresh and exact. `OutputTooSmall` is not a fault — the
            // record that did not fit is stashed and the memo is exact —
            // and neither is `Backpressure` (the device deferred the
            // reopen: the key memo stands, not live, and the next call
            // reopens from it).
            self.state.scan_reset();
        }
        r
    }

    /// Materialize one committed batch (format v1, module docs).
    /// Validate-then-apply: a `Malformed` batch mutates nothing.
    fn apply(&mut self, batch: &[u8]) -> Result<ApplyResult, StoreError> {
        // A transfer capture reads as of its revision: writes go on above it.
        if self.state.install.active || (self.state.capture.active && !self.state.capture.transfer)
        {
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
        if self.state.capture.transfer {
            return Err(StoreError::Backpressure);
        }
        if !self.state.capture.active {
            if cursor != 0 {
                return Err(StoreError::Malformed);
            }
            if request.range_generation != self.state.range_generation {
                return Err(StoreError::FenceMismatch);
            }
            // The capture holds its exclusion from here: the count and the
            // records it emits are over one view, however many steps the
            // device makes the walks take.
            self.state.capture = CaptureState {
                active: true,
                header_sent: false,
                transfer: false,
                counting: true,
                as_of: 0,
                resume_len: 0,
                resume: [0; MAX_ENCODED_KEY],
                range_generation: request.range_generation,
                applied_index: request.applied_index,
                next_ordinal: 0,
                total_entries: 0,
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
        let mut kbuf = [0u8; MAX_ENCODED_KEY];
        let mut vbuf = [0u8; MAX_VALUE_LEN];
        let mut resume = [0u8; MAX_ENCODED_KEY];
        if self.state.capture.counting {
            // Count total records once (full fidelity: every surviving
            // version, tombstones included), resuming at the key the count
            // reached when the device made it wait — `Ok((0, InProgress))`
            // then, the same request asked again continuing it.
            let rl = self.state.capture.resume_len as usize;
            resume[..rl].copy_from_slice(&self.state.capture.resume[..rl]);
            // SAFETY: the state's merge, lent to this walk (see `point`): no
            // point read runs inside a whole-store walk, nor one walk inside
            // another, so nothing else holds it while this reference lives.
            let m = unsafe { &mut *self.state.point.get() };
            let counted = (|| -> Result<bool, StoreError> {
                m.fill_filtered(self.state, &*self.storage, true, &resume[..rl], None)?;
                loop {
                    let Some((klen, _)) =
                        m.next(&self.state.mem_view(), &*self.storage, &mut kbuf, &mut vbuf)?
                    else {
                        return Ok(true);
                    };
                    // The bound is inclusive: the key last counted comes
                    // back first.
                    if rl > 0 && kbuf[..klen] == resume[..rl] {
                        continue;
                    }
                    self.state.capture.total_entries += 1;
                    self.state.capture.resume[..klen].copy_from_slice(&kbuf[..klen]);
                    self.state.capture.resume_len = klen as u32;
                }
            })();
            match counted {
                Ok(true) => {
                    self.state.capture.counting = false;
                    self.state.capture.resume_len = 0;
                }
                Ok(false) => {}
                Err(StoreError::Backpressure) => {
                    return Ok((0, Progress::InProgress { cursor: 0 }));
                }
                Err(e) => {
                    self.state.capture.active = false;
                    return Err(e);
                }
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
            out[SNAP_HDR_CLOCK_OFF..SNAP_HDR_LEN]
                .copy_from_slice(&self.state.committed_clock_ms.to_le_bytes());
            pos = SNAP_HDR_LEN;
            self.state.capture.header_sent = true;
        }
        // Resume the walk AT the last key emitted (inclusive, skipped), not
        // from the first record: each chunk costs its own records, not the
        // whole prefix again. The view is stable — the capture excludes
        // writes and flushes — so the key is exact.
        let rl = self.state.capture.resume_len as usize;
        resume[..rl].copy_from_slice(&self.state.capture.resume[..rl]);
        // SAFETY: the state's merge, lent to this walk (see `point`): no
        // point read runs inside a whole-store walk, nor one walk inside
        // another, so nothing else holds it while this reference lives.
        let m = unsafe { &mut *self.state.point.get() };
        // As in `transfer_step`: a read the device defers ends the step
        // with what it emitted; the position moved only with them.
        if let Err(e) = m.fill_filtered(self.state, &*self.storage, true, &resume[..rl], None) {
            return held_back(pos, self.state.capture.next_ordinal, e);
        }
        loop {
            let next = match m.next(&self.state.mem_view(), &*self.storage, &mut kbuf, &mut vbuf) {
                Ok(n) => n,
                Err(e) => return held_back(pos, self.state.capture.next_ordinal, e),
            };
            let Some((klen, vlen)) = next else {
                self.state.capture.active = false;
                return Ok((pos, Progress::Done));
            };
            if rl > 0 && kbuf[..klen] == resume[..rl] {
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
            self.state.capture.next_ordinal += 1;
            self.state.capture.resume[..klen].copy_from_slice(&kbuf[..klen]);
            self.state.capture.resume_len = klen as u32;
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
        if self.state.capture.active
            || self.state.compact.active
            || (self.state.flush.active && !self.state.install.active)
        {
            return Err(StoreError::Backpressure);
        }
        if !self.state.install.active {
            if chunk.offset != 0 {
                return Err(StoreError::Malformed);
            }
            if request.range_generation != self.state.range_generation {
                return Err(StoreError::FenceMismatch);
            }
            // REPLACE: the install holds the store from here (writes and
            // flushes refuse) while the local files are destroyed.
            self.state.install.active = true;
            self.state.install.destroy = DESTROY_LIST_MAN;
            self.state.install.destroy_n = 0;
            self.state.install.destroy_at = 0;
            self.state.install.range_generation = request.range_generation;
            self.state.install.applied_index = request.applied_index;
            // A recovery part-way is moot: everything it read is going.
            self.state.rec.phase = REC_IDLE;
        } else if request.applied_index != self.state.install.applied_index
            || request.range_generation != self.state.install.range_generation
        {
            self.install_abort();
            return Err(StoreError::FenceMismatch);
        }
        if self.state.install.destroy != 0 {
            // `Again` → `Backpressure`: the same chunk asked again
            // continues the destroy where it stood.
            match self.install_destroy() {
                Ok(()) => {}
                Err(StoreError::Backpressure) => return Err(StoreError::Backpressure),
                Err(e) => {
                    self.install_abort();
                    return Err(e);
                }
            }
            let range_gen = self.state.range_generation;
            self.state.init();
            self.state.range_generation = range_gen;
            self.state.install.active = true;
            self.state.install.destroy = 0;
            self.state.install.header_done = false;
            self.state.install.range_generation = request.range_generation;
            self.state.install.applied_index = request.applied_index;
            self.state.install.expected_offset = 0;
            self.state.install.entry_count = 0;
            self.state.install.entries_done = 0;
            self.state.install.partial_len = 0;
        }
        // A chunk is taken from where the stream has been consumed to: the
        // same chunk asked again after a `Backpressure` (the store made
        // room part-way through it) skips what it already took. A chunk
        // that starts past that point, or ends before it, is a gap.
        let exp = self.state.install.expected_offset;
        let end = chunk.offset + chunk.data.len() as u64;
        if chunk.offset > exp || end < exp {
            self.install_abort();
            return Err(StoreError::Malformed);
        }
        // The install's own flush, left in flight by an earlier call, goes
        // on before anything more is taken.
        if self.state.flush.active {
            match self.flush_inner() {
                Ok(()) => {}
                Err(StoreError::Backpressure) => return Err(StoreError::Backpressure),
                Err(e) => {
                    self.install_abort();
                    return Err(e);
                }
            }
        }
        // Stream bytes through the partial-record staging buffer.
        let mut in_pos = (exp - chunk.offset) as usize;
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
                self.state.install.snap_clock_ms = rd_u64(&hdr, SNAP_HDR_CLOCK_OFF);
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
                match self.flush_inner() {
                    Ok(()) => {}
                    Err(StoreError::Backpressure) => {
                        // The flush waits on the device. The record stays
                        // staged whole; the stream is consumed up to it,
                        // and the same chunk asked again continues here.
                        self.state.install.expected_offset = chunk.offset + in_pos as u64;
                        return Err(StoreError::Backpressure);
                    }
                    Err(e) => {
                        self.install_abort();
                        return Err(e);
                    }
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
        self.state.install.expected_offset = end;
        if chunk.last {
            let complete = self.state.install.header_done
                && self.state.install.partial_len == 0
                && self.state.install.entries_done == self.state.install.entry_count;
            if !complete {
                self.install_abort();
                return Err(StoreError::Malformed);
            }
            self.state.highest_revision = self.state.install.snap_highest_revision;
            self.state.committed_clock_ms = self
                .state
                .committed_clock_ms
                .max(self.state.install.snap_clock_ms);
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
    /// trusted, never computed.
    fn compact(&mut self, floor: Revision, cursor: u64) -> Result<Progress, StoreError> {
        self.state.io_wait = false;
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
            self.state.next_run_id = id + 1;
            self.state.compact = CompactState {
                active: true,
                phase: COMPACT_PHASE_MERGE,
                stage: STAGE_CREATE,
                part: 0,
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
                out_len: 0,
                crc_state: CRC32_INIT,
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
                inputs: self.state.run_count as u8,
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
        // Every storage action is one stage; one the device cannot take
        // now answers `Backpressure` with the compaction still in flight,
        // standing where it was (`io_wait`), and is asked again on the
        // next call. Any other error abandons it: compaction publishes
        // last, so the pre-compaction manifest is still the truth and the
        // half-written output run is an orphan for `recover()`.
        let stepped = match self.state.compact.phase {
            COMPACT_PHASE_MERGE => self.compact_merge_stages(),
            COMPACT_PHASE_FOOTER => self.compact_finalize(),
            COMPACT_PHASE_RUN_SYNC => self.compact_poll_run(),
            COMPACT_PHASE_MAN_WRITE => self.compact_manifest_write(),
            _ => self.compact_poll_manifest(),
        };
        match stepped {
            Ok(p) => Ok(p),
            Err(StoreError::Backpressure) => {
                self.state.io_wait = true;
                Err(StoreError::Backpressure)
            }
            Err(e) => {
                self.state.compact.active = false;
                self.state.compact.phase = COMPACT_PHASE_MERGE;
                self.state.compact.stage = STAGE_CREATE;
                Err(e)
            }
        }
    }
}

/// Where a compaction's inputs begin in the run set: the runs before it
/// were flushed while it was paused, and are newer. `None` when the run
/// set holds fewer runs than its inputs (never, short of corruption).
fn compact_base(state: &DiskState) -> Option<usize> {
    (state.run_count as usize).checked_sub(state.compact.inputs as usize)
}

impl<S: RunStorage> DiskStore<'_, S> {
    /// The output run's create and header, then the bounded merge steps.
    fn compact_merge_stages(&mut self) -> Result<Progress, StoreError> {
        let id = self.state.compact.out_run_id;
        if self.state.compact.stage == STAGE_CREATE {
            create_fresh(self.storage, FileKind::Run, id)?;
            self.state.compact.stage = STAGE_HEADER;
        }
        if self.state.compact.stage == STAGE_HEADER {
            let mut crc_state = CRC32_INIT;
            let mut file_len = 0u64;
            write_run_header(self.storage, id, &mut crc_state, &mut file_len)?;
            self.state.compact.crc_state = crc_state;
            self.state.compact.out_len = file_len;
            self.state.compact.stage = STAGE_BODY;
            return Ok(Progress::InProgress { cursor: 0 });
        }
        self.compact_step()
    }

    /// One bounded merge step, ATOMIC against the device: the step's
    /// records are decided from the saved merge positions and the ONE
    /// block they fill is appended as the step's last act; only once it
    /// has landed are the positions, the counts and the block's index
    /// entry saved. A step whose read or write the device cannot take now
    /// therefore leaves nothing behind, and the next call — from the same
    /// saved positions, over the same immutable runs — decides the same
    /// records into the same block bytes and asks the same append again,
    /// which the storage continues. Blocks already read stay in the
    /// block cache, so the repeated step does not read them again.
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
        // SAFETY: the state's merge, lent to this walk (see `point`): no
        // point read runs inside a whole-store walk, nor one walk inside
        // another, so nothing else holds it while this reference lives.
        let m = unsafe { &mut *self.state.point.get() };
        if resuming {
            m.resume_compact_in(self.state, &*self.storage)?;
        } else {
            m.fill_compact_in(self.state, &*self.storage)?;
        }
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
        // The step's block, framed: payload from BLOCK_HDR.
        let mut block = [0u8; BLOCK_OVERHEAD + BLOCK_MAX_PAYLOAD];
        let mut fill = 0usize;
        let mut block_recs = 0u32;
        let mut first_key = [0u8; INDEX_KEY_BYTES];
        // The merge positions before the record in hand: where the next
        // step resumes when this one ends before taking it.
        let mut at_block = [0u64; MAX_RUNS];
        let mut at_rec = [0u32; MAX_RUNS];
        loop {
            for i in 0..m.run_count.min(MAX_RUNS) {
                let (b, r) = m.runs[i].save_pos();
                at_block[i] = b;
                at_rec[i] = r;
            }
            let next = m.next(&self.state.mem_view(), &*self.storage, &mut kbuf, &mut vbuf)?;
            let Some((klen, vlen)) = next else {
                // Input exhausted: close the last block, then hand the
                // footer + fsync + publish tail to its own phases —
                // each is a separate step with its own deadline
                // budget.
                if fill > 0 {
                    self.compact_write_block(&mut block, fill, block_recs, &first_key)?;
                }
                self.state.compact.input_ordinal = processed;
                self.state.compact.resume_valid = false;
                self.state.compact.phase = COMPACT_PHASE_FOOTER;
                self.state.compact.stage = STAGE_FOOTER;
                self.state.compact.part = 0;
                return Ok(Progress::InProgress {
                    cursor: self.state.compact.input_ordinal,
                });
            };
            if klen < MIN_ENCODED_KEY {
                return Err(StoreError::StorageFault);
            }
            // Decide the record against a copy of the dedup context: it is
            // committed only if the record is taken into this step.
            let plen = klen - VERSION_SUFFIX_LEN;
            let new_prefix = plen != prefix_len || kbuf[..plen] != prefix[..prefix_len];
            let mut kept = if new_prefix { false } else { kept_below_floor };
            let ts = key_timestamp(&kbuf[..klen]);
            let keep = if ts > floor {
                true
            } else if !kept {
                kept = true;
                // Rule (b): the newest at-or-below-floor version being
                // a tombstone means nothing survives beneath it.
                key_kind_byte(&kbuf[..klen]) != ValueKind::PointTombstone as u8
            } else {
                false // rule (a): shadowed below the floor
            };
            let rec_len = RECORD_FIXED + klen + vlen;
            if keep && fill > 0 && fill + rec_len > BLOCK_TARGET {
                // The block is full: this record opens the next step. The
                // step ends BEFORE it, at the positions saved above, with
                // the dedup context as it stood before it.
                self.compact_write_block(&mut block, fill, block_recs, &first_key)?;
                self.compact_save(
                    processed,
                    &at_block,
                    &at_rec,
                    m.run_count,
                    &prefix,
                    prefix_len,
                    kept_below_floor,
                );
                return Ok(Progress::InProgress { cursor: processed });
            }
            if new_prefix {
                prefix[..plen].copy_from_slice(&kbuf[..plen]);
                prefix_len = plen;
            }
            kept_below_floor = kept;
            processed += 1;
            if keep {
                if fill == 0 {
                    let n = klen.min(INDEX_KEY_BYTES);
                    first_key = [0u8; INDEX_KEY_BYTES];
                    first_key[..n].copy_from_slice(&kbuf[..n]);
                }
                let at = BLOCK_HDR + fill;
                copy_at(&mut block, at, &(klen as u16).to_le_bytes());
                copy_at(&mut block, at + 2, &(vlen as u32).to_le_bytes());
                copy_at(&mut block, at + 6, &kbuf[..klen]);
                copy_at(&mut block, at + 6 + klen, &vbuf[..vlen]);
                fill += rec_len;
                block_recs += 1;
            }
            if processed - resume >= COMPACT_STEP_RECORDS {
                // Step budget spent: close the partial block so no
                // block state needs to persist across steps, and save
                // the exact merge positions + dedup-walk context so
                // the next step SEEKS here instead of replaying.
                if fill > 0 {
                    self.compact_write_block(&mut block, fill, block_recs, &first_key)?;
                }
                for i in 0..m.run_count.min(MAX_RUNS) {
                    let (b, r) = m.runs[i].save_pos();
                    at_block[i] = b;
                    at_rec[i] = r;
                }
                self.compact_save(
                    processed,
                    &at_block,
                    &at_rec,
                    m.run_count,
                    &prefix,
                    prefix_len,
                    kept_below_floor,
                );
                return Ok(Progress::InProgress { cursor: processed });
            }
        }
    }

    /// A merge step has landed: save where the next one resumes.
    #[expect(
        clippy::too_many_arguments,
        reason = "the resume point is these fields; a struct would only rename them"
    )]
    fn compact_save(
        &mut self,
        processed: u64,
        at_block: &[u64; MAX_RUNS],
        at_rec: &[u32; MAX_RUNS],
        run_count: usize,
        prefix: &[u8; MAX_ENCODED_KEY],
        prefix_len: usize,
        kept_below_floor: bool,
    ) {
        let c = &mut self.state.compact;
        c.input_ordinal = processed;
        c.resume_valid = true;
        c.run_pos_count = run_count as u8;
        c.run_block = *at_block;
        c.run_rec = *at_rec;
        c.carry_prefix_len = prefix_len as u16;
        for (d, b) in c
            .carry_prefix
            .iter_mut()
            .zip(prefix.iter())
            .take(prefix_len)
        {
            *d = *b;
        }
        c.carry_kept_below_floor = kept_below_floor;
    }

    /// Append the step's block (framed in `block`, payload `fill` bytes)
    /// and, only once it has landed, account for it: the output run's
    /// CRC, length, block count, index entry, filter and record count.
    fn compact_write_block(
        &mut self,
        block: &mut [u8; BLOCK_OVERHEAD + BLOCK_MAX_PAYLOAD],
        fill: usize,
        rec_count: u32,
        first_key: &[u8; INDEX_KEY_BYTES],
    ) -> Result<(), StoreError> {
        let mut crc_state = self.state.compact.crc_state;
        let mut file_len = self.state.compact.out_len;
        let off = write_run_block(
            self.storage,
            self.state.compact.out_run_id,
            block,
            fill,
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
            idx_push(&mut self.state.compact.index, ordinal, off, first_key);
        }
        // Same incremental filter build as the flush path, over the
        // block's records now that they are in the run.
        let mut p = BLOCK_HDR;
        let end = BLOCK_HDR + fill;
        while p + RECORD_FIXED <= end {
            let klen = rd_u16(block, p) as usize;
            let vlen = rd_u32(block, p + 2) as usize;
            let k0 = p + RECORD_FIXED;
            if let Some(key) = block.get(k0..k0 + klen.saturating_sub(VERSION_SUFFIX_LEN)) {
                bloom_insert(&mut self.state.compact.bloom, key);
            }
            p = k0 + klen + vlen;
        }
        self.state.compact.blk_has_first = false;
        self.state.compact.out_blocks += 1;
        self.state.compact.out_records += rec_count;
        self.state.compact.crc_state = crc_state;
        self.state.compact.out_len = file_len;
        Ok(())
    }

    /// Close the output run and SUBMIT its durability fence: the footer,
    /// part by part, then the submit — each asked again as it stands.
    /// Nothing is published here — the manifest is not written until the
    /// run is proven durable, and the inputs are not deleted until the
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
            self.state.compact.stage = STAGE_MAN_WRITE;
            return Ok(Progress::InProgress {
                cursor: c.input_ordinal,
            });
        }
        if self.state.compact.stage == STAGE_FOOTER {
            while self.state.compact.part < FOOTER_PARTS {
                let mut crc = self.state.compact.crc_state;
                write_run_footer_part(
                    self.storage,
                    c.out_run_id,
                    self.state.compact.part,
                    c.out_records,
                    c.out_blocks,
                    c.smallest_off,
                    c.largest_off,
                    &self.state.compact.bloom,
                    &self.state.compact.index,
                    &mut crc,
                )?;
                self.state.compact.crc_state = crc;
                self.state.compact.part += 1;
            }
            self.state.compact.pending_len = c.out_len + RUN_FOOTER_LEN as u64;
            self.state.compact.pending_crc = crc32_finish(self.state.compact.crc_state);
            self.state.compact.stage = STAGE_RUN_SUBMIT;
        }
        let ticket = self
            .storage
            .fsync_submit(FileKind::Run, c.out_run_id)
            .map_err(map_storage)?;
        self.state.compact.ticket = ticket;
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
                self.state.io_wait = true;
                return Ok(Progress::InProgress {
                    cursor: c.input_ordinal,
                });
            }
            FsyncState::Durable => {}
        }
        self.state.compact.pending_gen = self.state.generation + 1;
        self.state.compact.phase = COMPACT_PHASE_MAN_WRITE;
        self.state.compact.stage = STAGE_RUN_NAME;
        Ok(Progress::InProgress {
            cursor: c.input_ordinal,
        })
    }

    /// Publish the output run's name, write the manifest slot, then submit
    /// its fence: three stages.
    fn compact_manifest_write(&mut self) -> Result<Progress, StoreError> {
        let slot = self.state.inactive_manifest_slot();
        if self.state.compact.stage == STAGE_RUN_NAME {
            if self.state.compact.out_records != 0 {
                self.storage
                    .fsync_name(FileKind::Run, self.state.compact.out_run_id)
                    .map_err(map_storage)?;
            }
            self.state.compact.stage = STAGE_MAN_WRITE;
        }
        let c = self.state.compact;
        if c.stage == STAGE_MAN_WRITE {
            let meta = RunMeta {
                id: c.out_run_id,
                record_count: c.out_records,
                file_len: c.pending_len,
                digest: c.pending_crc,
                _pad: 0,
            };
            // The runs flushed while the merge was paused stay, newest
            // first, ahead of its output; the inputs it merged are gone.
            let base = compact_base(self.state).ok_or(StoreError::StorageFault)?;
            let mut metas = [RunMeta::empty(); MAX_RUNS];
            metas[..base].copy_from_slice(&self.state.runs[..base]);
            let mut n = base;
            if c.out_records != 0 {
                metas[n] = meta;
                n += 1;
            }
            let body = ManifestBody {
                generation: c.pending_gen,
                runs: metas.get(..n).unwrap_or(&[]),
                floor: c.floor,
                applied_index: self.state.durable_applied_index,
                applied_term: self.state.durable_applied_term,
                highest_revision: self.state.durable_highest_revision,
                committed_clock_ms: self.state.durable_clock_ms,
            };
            if Self::manifest_write(self.storage, slot, &body)? {
                self.state.slot_unnamed |= slot_bit(slot);
            }
            self.state.compact.stage = STAGE_MAN_SUBMIT;
        }
        let ticket = self
            .storage
            .fsync_submit(FileKind::Manifest, slot)
            .map_err(map_storage)?;
        self.state.compact.pending_slot = slot;
        self.state.compact.ticket = ticket;
        self.state.compact.phase = COMPACT_PHASE_MAN_SYNC;
        self.state.compact.stage = STAGE_MAN_POLL;
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
        if c.stage == STAGE_MAN_POLL {
            match self
                .storage
                .fsync_poll(FileKind::Manifest, c.pending_slot, c.ticket)
                .map_err(map_storage)?
            {
                FsyncState::Pending => {
                    self.state.io_wait = true;
                    return Ok(Progress::InProgress {
                        cursor: c.input_ordinal,
                    });
                }
                FsyncState::Durable => {}
            }
            self.state.compact.stage = STAGE_MAN_NAME;
        }
        self.publish_slot_name(c.pending_slot)?;
        let base = compact_base(self.state).ok_or(StoreError::StorageFault)?;
        let old_count = self.state.run_count as usize;
        let old_runs = self.state.runs;
        // The runs flushed while the merge was paused keep their places
        // (and their filters and indexes); the inputs give way to the
        // output, at the end, the oldest.
        for i in base..MAX_RUNS {
            self.state.runs[i] = RunMeta::empty();
            self.state.run_blooms[i] = [0u8; BLOOM_BYTES];
            self.state.run_index[i].fill(0);
        }
        if c.out_records == 0 {
            self.state.run_count = base as u32;
        } else {
            self.state.run_blooms[base] = c.bloom;
            self.state.run_index[base] = self.state.compact.index;
            self.state.runs[base] = RunMeta {
                id: c.out_run_id,
                record_count: c.out_records,
                file_len: c.pending_len,
                digest: c.pending_crc,
                _pad: 0,
            };
            self.state.run_count = base as u32 + 1;
        }
        self.state.generation = c.pending_gen;
        self.state.manifest_slot = c.pending_slot;
        self.state.compaction_floor = c.floor;
        self.state.compact.active = false;
        self.state.compact.phase = COMPACT_PHASE_MERGE;
        self.state.compact.stage = STAGE_CREATE;
        // The merged-away inputs are unreferenced now. QUEUE their
        // deletion instead of doing it here: each FAT32 delete walks
        // and frees the file's whole cluster chain synchronously, and
        // several of them in this one step is exactly the deadline
        // overrun that would kill the worker at manifest adoption.
        // The maintenance loop drains one per step; a crash merely
        // leaves orphans for recovery.
        for meta in old_runs.iter().take(old_count).skip(base) {
            if c.out_records != 0 && meta.id == c.out_run_id {
                continue;
            }
            if (self.state.retire_count as usize) < MAX_RUNS {
                self.state.retire_ids[self.state.retire_count as usize] = meta.id;
                self.state.retire_count += 1;
            } else {
                // The queue is full: deletions are being outpaced (a long
                // import, a burst of writes that never lets the drain
                // run). Asked now; the merge is ALREADY adopted, so a
                // delete the device cannot take now (or refuses) is not a
                // failure of this compaction — the file is left an
                // unreferenced orphan, which recovery deletes (a listing
                // too long to hold is swept in batches, `SWEEP_BATCH`).
                match self.storage.delete(FileKind::Run, meta.id) {
                    Ok(()) | Err(StorageError::NotFound) => {}
                    Err(_) => {
                        self.state.retire_orphaned = self.state.retire_orphaned.wrapping_add(1);
                    }
                }
            }
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
