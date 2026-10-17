//! `FsRunStorage` — `RunStorage` over the Fluxor FS provider contract
//! (`target/fluxor/fluxor-abi/sdk/contracts/storage/fs.rs`), for the PIC
//! module build of the ordered disk state-store provider
//! (`disk_store.rs`).
//!
//! Every call is bounded and never waits on the device: the FS
//! provider answers each opcode now, with `EAGAIN` (-11, nothing moved,
//! ask again) or — for the multi-step operations (`OPEN_CREATE`,
//! `UNLINK`, `FSYNC`) — `EINPROGRESS` (-36, taken; re-ask the
//! byte-identical request, a later answer decides), and `READ`/`WRITE`
//! may move a short count (see the fs contract,
//! `target/fluxor/fluxor-abi/sdk/contracts/storage/fs.rs`). This backend
//! turns all of that into the `RunStorage` contract's
//! [`StorageError::Again`], and keeps the call's progress so the SAME call
//! asked again continues it rather than starting over:
//!
//! - a write part-way keeps `(at, len, done)` on its write slot and the
//!   repeated ask writes `data[done..]` at `at + done`;
//! - an `EINPROGRESS` operation keeps `(op, kind, id)` in `inflight`, and
//!   the repeated ask re-issues exactly the same opcode and arg bytes
//!   (skipping any preamble — the existence probe — that would make it a
//!   different request);
//! - a directory walk keeps its directory fd and the count found, and the
//!   repeated ask (with the caller's `out` intact) reads on.
//!
//! No call loops on the provider waiting for progress — the device only
//! moves between steps. `ENFILE` (the provider's open-file table
//! momentarily full) and `EBUSY` on `UNLINK` move nothing either and are
//! answered as `Again` too.
//!
//! ## Write slots and fences
//!
//! Writes go through two cached write descriptors: one for the store's
//! data files (runs, manifests) and one for the command log, so a log
//! append in the middle of a flush never closes the run's descriptor. That
//! matters for durability, not only for speed: a fence (`FSYNC`, or an
//! `FSYNC_SUBMIT` ticket) covers the writes made through ITS descriptor —
//! a ticket is valid only on the descriptor that produced it — and `CLOSE`
//! never reports a writeback failure. A slot closed while it holds writes
//! no fence has covered (or an outstanding ticket) records that file as
//! LOST; the next fence asked for it answers `NotDurable`, so the caller
//! fails closed instead of trusting a fence on a fresh descriptor that
//! never saw those bytes.
//!
//! ## File naming (K-prefix = KV engine namespace)
//!
//! Files live beside the WAL segments and snapshot manifests, so names
//! must be 8.3-conforming at the FAT32 root and must not collide with
//! the WAL's `<p:1hex><seq:7hex>.WAL` segments or the snapshot `.SNP`
//! manifests. The `K` prefix reserves a namespace for the KV engine:
//!
//! ```text
//! run file   (FileKind::Run,      id)  → "K<id:7hex>.KRN"
//! manifest   (FileKind::Manifest, gen) → "K<gen:7hex>.KMN"
//! ```
//!
//! 7 hex digits cover 268M run ids / manifest generations. On
//! `target: linux` (`root_path = 0`, the default) the same names live
//! under a `kv/` directory in the process working directory — the
//! exact split `durability` uses for its segments (`wal/` on linux,
//! FAT32-root 8.3 with `root_path = 1` on bare metal). The operator
//! (or test harness) creates `kv/` once; the Linux FS provider's
//! `OPEN_CREATE` creates files but not parent directories.
//!
//! ## Enumeration
//!
//! `list()` walks the directory through the FS contract's directory
//! surface — `FS_OPENDIR` + repeated `FS_READDIR` batches (the same
//! one-shot scan idiom as `deps/fluxor/modules/foundation/bank/mod.rs::
//! fs_scan_dir`) — matching the fixed name shapes above
//! (case-insensitively: FAT32 8.3 entries come back uppercase). No
//! auxiliary id-hint file is needed because the contract has a real
//! directory-list opcode on both providers.
//!
//! ## Error mapping
//!
//! Negative errnos map onto the typed `StorageError` set, always failing
//! closed; the waits above map to `Again`. The raw errno of the last
//! failure is retained in `last_errno`, and every `Again` answered is
//! counted ([`FsRunStorage::agains`]) so the hosting module can tell "the
//! device made this command wait" from a fault.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC module; each consumer uses a subset of the surface"
)]

// Like `telemetry.rs`, this file is `#[path]`-mounted into a module
// crate that has already mounted `abi` and (via `kv_store.rs`) the
// disk provider — it reaches both through `crate::` so there is
// exactly ONE `RunStorage` trait instance in the crate.
use crate::abi::SyscallTable;
use crate::fd_cache::{FdCache, SLOTS as FD_SLOTS};
use crate::kv_store::disk_store::run_storage::{
    FileKind, FsyncState, FsyncTicket, RunStorage, StorageError,
};
use core::cell::Cell;

// FS opcodes (`modules/sdk/contracts/storage/fs.rs`) — same local-const
// idiom as clustor's durability WAL.
const FS_OPEN: u32 = 0x0900;
const FS_READ: u32 = 0x0901;
const FS_SEEK: u32 = 0x0902;
const FS_CLOSE: u32 = 0x0903;
const FS_STAT: u32 = 0x0904;
const FS_FSYNC: u32 = 0x0905;
const FS_WRITE: u32 = 0x0906;
const FS_OPENDIR: u32 = 0x0907;
const FS_READDIR: u32 = 0x0908;
/// Write-side opener. `FS_OPEN` is read-only-if-exists per the FS
/// contract, so every write path opens through the write tier.
const FS_OPEN_CREATE: u32 = 0x0909;
const FS_UNLINK: u32 = 0x090A;
/// Durable publication of a path's directory entry (`fs.rs::FSYNC_NAME`).
const FS_FSYNC_NAME: u32 = 0x0912;
/// FS capability-discovery opcode + bits (`fs.rs::{CAPS, caps}`).
const FS_CAPS: u32 = 0x09FF;
/// `last_errno` after `list` overflowed its table: a sentinel, not a
/// provider errno (those are small negatives).
const FS_E_LIST_OVERFLOW: i32 = -1000;
/// `last_errno` when a fence is asked for a file whose unfenced writes
/// (or outstanding ticket) went with a closed descriptor: a sentinel.
const FS_E_FENCE_LOST: i32 = -1001;
const FS_CAP_OPEN_CREATE: u32 = 1 << 2;
const FS_CAP_WRITE: u32 = 1 << 3;
const FS_CAP_UNLINK: u32 = 1 << 5;
/// `caps::FSYNC_NAME`: required — a store whose runs could vanish by name
/// after a power cut cannot honour its manifest.
const FS_CAP_FSYNC_NAME: u32 = 1 << 11;
/// Non-blocking durability fence (`fs.rs::{FSYNC_SUBMIT, FSYNC_POLL}`
/// + `caps::FSYNC_ASYNC`). Same opcodes and same capability bit
///   clustor's WAL uses on this device — see
///   `clustor/.context/rfc_async_wal_fsync.md`.
const FS_FSYNC_SUBMIT: u32 = 0x0910;
const FS_FSYNC_POLL: u32 = 0x0911;
const FS_CAP_FSYNC_ASYNC: u32 = 1 << 10;
/// `FSYNC_POLL` return codes: 0 = durable, 1 = still pending,
/// negative = the fenced write FAILED.
const FS_FENCE_DURABLE: i32 = 0;
const FS_FENCE_PENDING: i32 = 1;

/// EAGAIN: not now, nothing moved — ask again on a later step.
pub const FS_E_AGAIN: i32 = -11;
/// EINPROGRESS: the provider took the operation; re-ask the
/// byte-identical request on a later step, a later answer decides.
pub const FS_E_INPROGRESS: i32 = -36;
/// ENOENT from the provider.
const FS_E_NOENT: i32 = -2;
/// EBUSY: `FS_UNLINK` against a file some handle still has open.
const FS_E_BUSY: i32 = -16;
/// ENFILE: the provider's fixed open-file table is momentarily full.
const FS_E_NFILE: i32 = -23;
/// The linux FS provider reports a missing file on `FS_OPEN` as
/// ENODEV (its documented "loud on missing" policy predates a
/// distinct ENOENT there); fat32 reports ENOENT. Both mean "no such
/// file" for an open-by-path probe. The kernel's no-provider reply is
/// ENOSYS, so the ambiguity is harmless.
const FS_E_NODEV: i32 = -19;

/// `FS_OPEN` errno that means "the file does not exist".
fn open_says_missing(rc: i32) -> bool {
    rc == FS_E_NOENT || rc == FS_E_NODEV
}

/// An answer that moved nothing decisive: ask again later.
fn is_wait(rc: i32) -> bool {
    rc == FS_E_AGAIN || rc == FS_E_INPROGRESS
}

/// A named store directory, when one is set ([`FsRunStorage::set_dir`]).
pub const DIR_MAX: usize = 128;
/// The longest path built: the directory ([`DIR_MAX`]), `/`, and an 8.3
/// name (16).
const PATH_MAX: usize = 145;

/// Highest `store_id` the linux layout can express: `kv/`, `kv1/` …
/// `kv9/` — one digit, so the 8.3-adjacent path stays fixed-width.
/// Far above any realistic per-process range count (a graph hosts a
/// handful of statically provisioned ranges; thousands of ranges are
/// dense-hosting territory, not one-directory-each territory).
pub const STORE_ID_MAX: u8 = 9;
/// FS_READDIR batch buffer. Each entry is `[len:1][type:1][name:≤255]`;
/// our names are 12 bytes so this holds dozens per batch.
const READDIR_BUF: usize = 512;

/// The write slot data files (runs, manifests) use; the command log has
/// the other one, so its appends never close a run part-way through.
const SLOT_DATA: usize = 0;
const SLOT_LOG: usize = 1;

/// One cached write-side fd. Flush/compact write many appends to one
/// file back-to-back; caching the open fd (plus the append cursor)
/// avoids re-open + FS_STAT per append, and keeps the descriptor a fence
/// must be asked on (see the module docs). Invalidated on create/delete
/// of the same file and on any fault. Read paths never use it.
#[repr(C)]
#[derive(Clone, Copy)]
struct WriteSlot {
    fd: i32,
    /// [`kind_code`] of the cached file.
    kind: u8,
    /// Bytes have been written through `fd` that no fence has covered.
    unfenced: u8,
    /// An `FSYNC_SUBMIT` ticket from `fd` has not been polled durable.
    ticket_open: u8,
    /// `len` is the file's size (`FS_STAT` answered); 0 = ask it again.
    sized: u8,
    id: u32,
    /// Append cursor == current file length.
    len: u64,
    /// A write part-way: `pend_done` bytes of the `pend_len` bytes asked
    /// at `pend_at` are written (`pend_len == 0`: none). `pend_print`
    /// fingerprints the data, so a different write of the same shape is
    /// never taken for the one part-way.
    pend_at: u64,
    pend_len: u32,
    pend_done: u32,
    pend_print: u32,
    _pad: u32,
}

impl WriteSlot {
    const fn empty() -> Self {
        Self {
            fd: -1,
            kind: 0,
            unfenced: 0,
            ticket_open: 0,
            sized: 0,
            id: 0,
            len: 0,
            pend_at: 0,
            pend_len: 0,
            pend_done: 0,
            pend_print: 0,
            _pad: 0,
        }
    }

    fn holds(&self, code: u8, id: u32) -> bool {
        self.fd >= 0 && self.kind == code && self.id == id
    }
}

/// A file whose unfenced writes went with a closed descriptor.
#[repr(C)]
#[derive(Clone, Copy)]
struct Lost {
    valid: u8,
    kind: u8,
    _pad: u16,
    id: u32,
}

/// The `EINPROGRESS` operation the provider has taken for this backend:
/// asked again, it is re-issued byte for byte.
const OP_NONE: u8 = 0;
/// `OPEN_CREATE` of a file the existence probe found missing.
const OP_CREATE: u8 = 1;
/// `OPEN_CREATE` of an existing file, for writing.
const OP_OPEN_WRITE: u8 = 2;
const OP_UNLINK: u8 = 3;
const OP_FSYNC: u8 = 4;
const OP_FSYNC_NAME: u8 = 5;

#[repr(C)]
#[derive(Clone, Copy)]
struct Inflight {
    op: u8,
    kind: u8,
    _pad: u16,
    id: u32,
}

impl Inflight {
    const fn none() -> Self {
        Self {
            op: OP_NONE,
            kind: 0,
            _pad: 0,
            id: 0,
        }
    }
}

/// A directory walk part-way (`fd >= 0`): which kind it lists and how
/// many ids it has placed in the caller's `out`.
#[repr(C)]
#[derive(Clone, Copy)]
struct ListWalk {
    fd: i32,
    kind: u8,
    _pad: [u8; 3],
    count: u32,
    /// The caller's `out` (address and length): a walk continues only
    /// into the buffer that holds what it already found.
    out_at: usize,
    out_len: usize,
}

impl ListWalk {
    const fn idle() -> Self {
        Self {
            fd: -1,
            kind: 0,
            _pad: [0; 3],
            count: 0,
            out_at: 0,
            out_len: 0,
        }
    }
}

/// `RunStorage` over Fluxor FS provider calls. All-POD; the zeroed
/// form is valid *after* `init()` (which stores the syscall table and
/// resets fds to -1).
#[repr(C)]
pub struct FsRunStorage {
    syscalls: *const SyscallTable,
    /// 1 = 8.3 names at the FS root (bare-metal FAT32); 0 = the same
    /// names under `kv/` (linux layout). Mirrors durability's
    /// `root_path` param.
    root_path: u8,
    /// Which store this instance is (multi-range graphs run one
    /// `kv_state_worker` per range in one process; each needs its own
    /// files). 0 = the historical `kv/` directory; 1..=`STORE_ID_MAX`
    /// = `kv<id>/`. Only meaningful on the linux layout — `init`
    /// fails closed on `root_path = 1` with a non-zero id, because at
    /// the FAT32 root two stores would silently share the `K` names.
    store_id: u8,
    /// The directory the store's files live in, when named
    /// ([`FsRunStorage::set_dir`]; linux layout): in place of `kv/`.
    dir_len: u8,
    dir: [u8; DIR_MAX],
    /// FS_CAPS probe: 0 = not probed, else `bits | PROBED`.
    caps_probe: u32,
    /// Write descriptors: [`SLOT_DATA`], [`SLOT_LOG`].
    slots: [WriteSlot; 2],
    /// Files whose fences can no longer be proven, per slot.
    lost: [Lost; 2],
    /// The provider operation part-way (`EINPROGRESS`, or an
    /// `OPEN_CREATE` that answered `EAGAIN` after the probe said missing).
    inflight: Inflight,
    /// Raw errno of the most recent provider failure (0 = none).
    /// `Cell` because read-side methods take `&self` per the contract.
    last_errno: Cell<i32>,
    /// `Again` answers given since `init` (monotonic).
    agains: Cell<u64>,
    /// The directory walk part-way, if any.
    walk: Cell<ListWalk>,
    /// `read_at` CALLS and bytes. Every call is a full
    /// FS_OPEN + FS_SEEK + FS_READ + FS_CLOSE cycle, and FS_OPEN on
    /// FAT32 is a LINEAR directory scan — so on that backend the call
    /// COUNT, not the byte count, is what a read costs. Counting them
    /// here (rather than at the block layer) means footer reads,
    /// `verify_run`'s whole-file walk, and block reads are all caught
    /// by the same number: nothing in the read path can hide from it.
    reads: Cell<u64>,
    read_bytes: Cell<u64>,
    /// Actual `FS_OPEN` calls issued by the READ path. With the
    /// descriptor cache engaged this stays near zero in steady state
    /// while `reads` keeps climbing; that divergence is the proof the
    /// cache is working, rather than something to infer from timing.
    opens: Cell<u64>,
    /// Open read descriptors, one per run/manifest, bounded at
    /// `FD_SLOTS`. `Cell` because `read_at` takes `&self` per the
    /// `RunStorage` contract.
    read_fds: Cell<FdCache>,
}

const CAPS_PROBED: u32 = 1 << 31;

/// Cheap fingerprint of a write's bytes: its length and up to 8 bytes
/// from each end. Guards a part-way write against being continued by a
/// different write of the same offset and length.
fn fingerprint(data: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5 ^ (data.len() as u32);
    for b in data.iter().take(8).chain(data.iter().rev().take(8)) {
        h = (h ^ u32::from(*b)).wrapping_mul(0x0100_0193);
    }
    h
}

impl FsRunStorage {
    /// Prepare an (arena-zeroed or reused) instance for use. Returns
    /// `false` (instance unusable, caller must fail its own init) on a
    /// layout that cannot separate stores: `root_path = 1` with a
    /// non-zero `store_id`, or an id above [`STORE_ID_MAX`].
    #[must_use]
    pub fn init(&mut self, syscalls: *const SyscallTable, root_path: u8, store_id: u8) -> bool {
        if store_id > STORE_ID_MAX || (root_path != 0 && store_id != 0) {
            return false;
        }
        self.syscalls = syscalls;
        self.root_path = root_path;
        self.store_id = store_id;
        self.dir_len = 0;
        self.caps_probe = 0;
        self.slots = [WriteSlot::empty(); 2];
        self.lost = [Lost {
            valid: 0,
            kind: 0,
            _pad: 0,
            id: 0,
        }; 2];
        self.inflight = Inflight::none();
        self.last_errno = Cell::new(0);
        self.agains = Cell::new(0);
        self.walk = Cell::new(ListWalk::idle());
        self.reads = Cell::new(0);
        self.read_bytes = Cell::new(0);
        self.opens = Cell::new(0);
        // Descriptors from a previous life are already gone, so this is
        // the one place a reset without closing is correct.
        self.read_fds = Cell::new(FdCache::new());
        true
    }

    /// Keep the store's files in `dir` (a path the FS provider resolves:
    /// absolute, or relative to the process's working directory) instead
    /// of `kv/`. `false` for a directory on the FAT32 root layout, or one
    /// longer than [`DIR_MAX`], or holding a NUL.
    #[must_use]
    #[allow(
        clippy::manual_contains,
        reason = "slice::contains links core's memchr, which PIC modules cannot resolve"
    )]
    pub fn set_dir(&mut self, dir: &[u8]) -> bool {
        let dir = dir.strip_suffix(b"/").unwrap_or(dir);
        if self.root_path != 0
            || dir.is_empty()
            || dir.len() > DIR_MAX
            || dir.iter().any(|b| *b == 0)
        {
            return false;
        }
        for (x, y) in self.dir.iter_mut().zip(dir) {
            *x = *y;
        }
        self.dir_len = dir.len() as u8;
        true
    }

    /// The directory's path into `out`: its length.
    fn dir_path(&self, out: &mut [u8; PATH_MAX]) -> usize {
        if self.root_path != 0 {
            out[0] = b'/';
            return 1;
        }
        if self.dir_len > 0 {
            let n = self.dir_len as usize;
            for (x, y) in out.iter_mut().zip(self.dir.iter().take(n)) {
                *x = *y;
            }
            return n;
        }
        out[0] = b'k';
        out[1] = b'v';
        if self.store_id != 0 {
            out[2] = b'0' + self.store_id;
            return 3;
        }
        2
    }

    /// Monotonic `(read_at calls, bytes, FS_OPEN calls)` since `init`.
    pub fn read_counters(&self) -> (u64, u64, u64) {
        (self.reads.get(), self.read_bytes.get(), self.opens.get())
    }

    /// `Again` answers given since `init`. The hosting module samples it
    /// around a command: a rise means the device made the command wait
    /// (it is held and asked again), not that the store is faulty.
    pub fn agains(&self) -> u64 {
        self.agains.get()
    }

    /// Answer `Again` for provider errno `rc`: counted, errno kept.
    fn again<T>(&self, rc: i32) -> Result<T, StorageError> {
        self.last_errno.set(rc);
        self.agains.set(self.agains.get().wrapping_add(1));
        Err(StorageError::Again)
    }

    /// Close and forget the cached read descriptor for one file.
    ///
    /// MUST be called before anything that changes what `(kind, id)`
    /// names — a descriptor that outlives its file would read another
    /// run's bytes and answer confidently with them.
    fn read_fd_drop(&self, kind: FileKind, id: u32) {
        let mut c = self.read_fds.get();
        let victim = c.drop_entry(kind_code(kind), id);
        self.read_fds.set(c);
        if let Some(fd) = victim {
            self.close_fd(fd);
        }
    }

    /// Close and forget every cached read descriptor.
    fn read_fd_drop_all(&self) {
        let mut c = self.read_fds.get();
        let mut fds = [0i32; FD_SLOTS];
        let n = c.take_all(&mut fds);
        self.read_fds.set(c);
        for &fd in fds.iter().take(n) {
            self.close_fd(fd);
        }
    }

    /// Close one cached read descriptor to make room for a write
    /// descriptor, keeping this backend inside the open-file budget it
    /// has always had (the read cache's `FD_SLOTS` plus one writer): the
    /// provider's table is shared with the WAL.
    fn make_room_for_writer(&self) {
        let writers = self.slots.iter().filter(|w| w.fd >= 0).count();
        if writers == 0 {
            return;
        }
        let mut c = self.read_fds.get();
        if c.len() + writers <= FD_SLOTS {
            return;
        }
        let victim = c.evict_lru();
        self.read_fds.set(c);
        if let Some(fd) = victim {
            self.close_fd(fd);
        }
    }

    /// `CLOSE` never waits and always releases the descriptor; it
    /// reports no writeback outcome, so its answer is not consulted.
    fn close_fd(&self, fd: i32) {
        if fd < 0 {
            return;
        }
        let sys = self.sys();
        // SAFETY: closing a descriptor this backend opened and owns.
        unsafe { (sys.provider_call)(fd, FS_CLOSE, core::ptr::null_mut(), 0) };
    }

    /// Raw errno of the most recent failed provider call (0 if the
    /// last call succeeded).
    pub fn last_errno(&self) -> i32 {
        self.last_errno.get()
    }

    /// True when the most recent failure was a wait (`EAGAIN` or
    /// `EINPROGRESS`): the call answered `Again`.
    pub fn last_was_again(&self) -> bool {
        is_wait(self.last_errno.get())
    }

    /// True when the most recent failure is one that a later step can
    /// reasonably expect to succeed — "wait", not "this store is
    /// broken". The hosting module must not quarantine on these.
    ///
    /// Every such errno is answered as `Again` now (the call keeps its
    /// progress and is asked again); this stays wider than
    /// [`last_was_again`] because `EBUSY` and `ENFILE` are self-clearing
    /// too. On bare-metal FAT32 the KV engine is the *second* client of
    /// one provider instance whose resources durability is also
    /// consuming:
    ///
    /// - `EBUSY` — `FS_UNLINK` refuses while any open handle still
    ///   references that file. Clears as soon as the other handle closes.
    /// - `ENFILE` — the provider's fixed open-file table (8 slots on
    ///   fat32) is momentarily exhausted; durability holds several
    ///   across a segment rotation or snapshot persist. Clears as soon
    ///   as one is released.
    pub fn last_was_transient(&self) -> bool {
        matches!(
            self.last_errno.get(),
            FS_E_AGAIN | FS_E_INPROGRESS | FS_E_BUSY | FS_E_NFILE
        )
    }

    fn sys(&self) -> &SyscallTable {
        // SAFETY: `init` stored a live kernel syscall table pointer per
        // the module ABI; it stays valid for the module's lifetime.
        unsafe { &*self.syscalls }
    }

    /// Build the path for (kind, id) into `out`; returns its length.
    /// See the module docs for the 8.3 naming scheme.
    fn encode_path(&self, kind: FileKind, id: u32, out: &mut [u8; PATH_MAX]) -> usize {
        let mut i = 0usize;
        if self.root_path == 0 {
            i = self.dir_path(out);
            out[i] = b'/';
            i += 1;
        }
        out[i] = b'K';
        i += 1;
        for digit in (0..7).rev() {
            let nibble = ((id >> (digit * 4)) & 0xF) as u8;
            out[i] = if nibble < 10 {
                b'0' + nibble
            } else {
                b'a' + nibble - 10
            };
            i += 1;
        }
        // By value: a `match` choosing among slice literals lowers to a
        // table of pointers, which a module image cannot hold.
        let ext: [u8; 4] = match kind {
            FileKind::Run => *b".KRN",
            FileKind::Manifest => *b".KMN",
            FileKind::Log => *b".KLG",
        };
        for &b in &ext {
            out[i] = b;
            i += 1;
        }
        i
    }

    /// Probe the provider capability bitmap once; the write tier
    /// (OPEN_CREATE + WRITE + UNLINK + FSYNC_NAME) is required — the provider
    /// manages its own file set, so a backend that can't delete can't
    /// honour the manifest-publish contract. Fail closed if absent.
    ///
    /// `EAGAIN` (the volume not attached yet) is "ask again", never a
    /// bitmap: nothing is latched until the provider has answered.
    fn require_write_tier(&mut self) -> Result<(), StorageError> {
        if self.caps_probe & CAPS_PROBED == 0 {
            let mut buf = [0u8; 4];
            let sys = self.sys();
            // SAFETY: `buf` is a valid 4-byte output buffer for the
            // duration of the call, per the CAPS wire shape.
            let rc = unsafe { (sys.provider_call)(-1, FS_CAPS, buf.as_mut_ptr(), 4) };
            if is_wait(rc) {
                return self.again(rc);
            }
            if rc < 0 {
                self.last_errno.set(rc);
                return Err(StorageError::IoFault);
            }
            self.caps_probe = u32::from_le_bytes(buf) | CAPS_PROBED;
        }
        let need = FS_CAP_OPEN_CREATE | FS_CAP_WRITE | FS_CAP_UNLINK | FS_CAP_FSYNC_NAME;
        if self.caps_probe & need != need {
            self.last_errno.set(-38); // ENOSYS-shaped: hard, not again
            return Err(StorageError::IoFault);
        }
        Ok(())
    }

    /// FS_OPEN existence probe. `Ok(true)` = exists (fd closed again),
    /// `Ok(false)` = ENOENT, `Again` = the provider cannot say yet,
    /// `Err` = any other failure.
    fn exists(&self, kind: FileKind, id: u32) -> Result<bool, StorageError> {
        let mut path = [0u8; PATH_MAX];
        let plen = self.encode_path(kind, id, &mut path);
        let sys = self.sys();
        // SAFETY: `path[..plen]` is a valid UTF-8 path buffer for the
        // duration of the call; FS_OPEN reads it and returns an fd.
        let fd = unsafe { (sys.provider_call)(-1, FS_OPEN, path.as_mut_ptr(), plen) };
        if fd >= 0 {
            self.close_fd(fd);
            return Ok(true);
        }
        if open_says_missing(fd) {
            return Ok(false);
        }
        if is_wait(fd) || fd == FS_E_NFILE {
            return self.again(fd);
        }
        self.last_errno.set(fd);
        Err(StorageError::IoFault)
    }

    /// Read up to `out.len()` bytes at `offset` of the file at `path` (a
    /// file outside the store's own layout, named as the FS provider
    /// names it): the count read (short when the rest is not cached yet),
    /// 0 at its end; `NotFound` when there is no such file; `Again` when
    /// nothing could be read yet — ask again. One open per call — for a
    /// one-off reader (an import), not the store's own hot path.
    pub fn read_path(
        &self,
        path: &[u8],
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize, StorageError> {
        let Ok(at) = u32::try_from(offset) else {
            self.last_errno.set(-27);
            return Err(StorageError::IoFault);
        };
        let mut p = [0u8; 256];
        let Some(dst) = p.get_mut(..path.len()) else {
            return Err(StorageError::IoFault);
        };
        for (x, y) in dst.iter_mut().zip(path) {
            *x = *y;
        }
        let sys = self.sys();
        // SAFETY: `p` holds the path for the duration of the call.
        let fd = unsafe { (sys.provider_call)(-1, FS_OPEN, p.as_mut_ptr(), path.len()) };
        if fd < 0 {
            if is_wait(fd) || fd == FS_E_NFILE {
                return self.again(fd);
            }
            self.last_errno.set(fd);
            return Err(if open_says_missing(fd) {
                StorageError::NotFound
            } else {
                StorageError::IoFault
            });
        }
        let seek = at.to_le_bytes();
        // SAFETY: `fd` from FS_OPEN; a 4-byte LE position; `out` is valid
        // for writes of its length.
        let n = unsafe {
            let rc = (sys.provider_call)(fd, FS_SEEK, seek.as_ptr() as *mut u8, 4);
            if rc < 0 {
                rc
            } else {
                (sys.provider_call)(fd, FS_READ, out.as_mut_ptr(), out.len())
            }
        };
        self.close_fd(fd);
        if is_wait(n) {
            return self.again(n);
        }
        if n < 0 {
            self.last_errno.set(n);
            return Err(StorageError::IoFault);
        }
        Ok(n as usize)
    }

    /// Close write slot `i`. A slot that holds writes no fence has
    /// covered, or an outstanding ticket, records its file as LOST: a
    /// fence asked for it later cannot be proven on a new descriptor.
    fn close_slot(&mut self, i: usize) {
        let Some(w) = self.slots.get(i).copied() else {
            return;
        };
        if w.fd >= 0 {
            if w.unfenced != 0 || w.ticket_open != 0 {
                if let Some(l) = self.lost.get_mut(i) {
                    *l = Lost {
                        valid: 1,
                        kind: w.kind,
                        _pad: 0,
                        id: w.id,
                    };
                }
            }
            self.close_fd(w.fd);
        }
        if let Some(s) = self.slots.get_mut(i) {
            *s = WriteSlot::empty();
        }
    }

    /// Close slot `i` because its file is being replaced or removed: the
    /// file's old bytes need no fence any more.
    fn forget_slot_file(&mut self, i: usize, code: u8, id: u32) {
        if self.slots.get(i).is_some_and(|w| w.holds(code, id)) {
            if let Some(w) = self.slots.get_mut(i) {
                w.unfenced = 0;
                w.ticket_open = 0;
            }
            self.close_slot(i);
        }
        if let Some(l) = self.lost.get_mut(i) {
            if l.valid != 0 && l.kind == code && l.id == id {
                l.valid = 0;
            }
        }
    }

    /// Was this file's fence lost with a closed descriptor? Clears it:
    /// the caller is told once, and fails closed on it.
    fn take_lost(&mut self, i: usize, code: u8, id: u32) -> bool {
        match self.lost.get_mut(i) {
            Some(l) if l.valid != 0 && l.kind == code && l.id == id => {
                l.valid = 0;
                true
            }
            _ => false,
        }
    }

    /// A fence on slot `i` failed: close it with its file LOST, so no
    /// later fence vouches for the bytes this one could not.
    fn fence_failed(&mut self, i: usize) {
        if let Some(w) = self.slots.get_mut(i) {
            w.unfenced = 1;
        }
        self.close_slot(i);
    }

    /// Open (kind, id) for writing in its slot, reusing a cached
    /// descriptor. The file must already exist (`create` is the only
    /// birth path). `Ok` = slot `i` holds it with its length known.
    fn open_for_write(&mut self, kind: FileKind, id: u32) -> Result<usize, StorageError> {
        let code = kind_code(kind);
        let i = slot_of(kind);
        let held = self.slots.get(i).is_some_and(|w| w.holds(code, id));
        if !held {
            self.close_slot(i);
            // An OPEN_CREATE of this file the provider took and has not
            // decided is asked again as it is: the existence probe in
            // front of it would make it a different request.
            let me = Inflight {
                op: OP_OPEN_WRITE,
                kind: code,
                _pad: 0,
                id,
            };
            let resuming = self.inflight_is(me);
            if !resuming && !self.exists(kind, id)? {
                return Err(StorageError::NotFound);
            }
            self.make_room_for_writer();
            let mut path = [0u8; PATH_MAX];
            let plen = self.encode_path(kind, id, &mut path);
            let sys = self.sys();
            // SAFETY: valid path buffer; OPEN_CREATE returns a write fd.
            let fd = unsafe { (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), plen) };
            if is_wait(fd) || fd == FS_E_NFILE {
                if fd == FS_E_INPROGRESS {
                    self.inflight = me;
                }
                return self.again(fd);
            }
            self.inflight_clear(me);
            if fd < 0 {
                self.last_errno.set(fd);
                return Err(StorageError::IoFault);
            }
            if let Some(w) = self.slots.get_mut(i) {
                *w = WriteSlot::empty();
                w.fd = fd;
                w.kind = code;
                w.id = id;
            }
        }
        if self.slots.get(i).is_some_and(|w| w.sized == 0) {
            let fd = self.slots.get(i).map_or(-1, |w| w.fd);
            // FS_STAT output: [size:u64][mtime:u64], or the narrow
            // [size:u32][mtime:u32] — the answer says which.
            let mut stat = [0u8; 16];
            let sys = self.sys();
            // SAFETY: 16-byte stat output buffer per the contract shape.
            let rc = unsafe { (sys.provider_call)(fd, FS_STAT, stat.as_mut_ptr(), 16) };
            if is_wait(rc) {
                // Kept open: the size is asked again next time.
                return self.again(rc);
            }
            let len = match rc {
                16 => u64::from_le_bytes([
                    stat[0], stat[1], stat[2], stat[3], stat[4], stat[5], stat[6], stat[7],
                ]),
                8 => u64::from(u32::from_le_bytes([stat[0], stat[1], stat[2], stat[3]])),
                _ => {
                    self.last_errno.set(if rc < 0 { rc } else { -5 });
                    self.close_slot(i);
                    return Err(StorageError::IoFault);
                }
            };
            if let Some(w) = self.slots.get_mut(i) {
                w.len = len;
                w.sized = 1;
            }
        }
        Ok(i)
    }

    fn inflight_is(&self, me: Inflight) -> bool {
        self.inflight.op == me.op && self.inflight.kind == me.kind && self.inflight.id == me.id
    }

    fn inflight_clear(&mut self, me: Inflight) {
        if self.inflight_is(me) {
            self.inflight = Inflight::none();
        }
    }

    /// The write descriptor slot `i` holds, or -1.
    fn slot_fd(&self, i: usize) -> i32 {
        self.slots.get(i).map_or(-1, |w| w.fd)
    }
}

/// A file kind as the descriptor caches key it: runs, manifests and log
/// segments share ids, so the kind is part of a file's identity.
fn kind_code(kind: FileKind) -> u8 {
    match kind {
        FileKind::Run => 0,
        FileKind::Manifest => 1,
        FileKind::Log => 2,
    }
}

/// The write slot a kind's files use.
fn slot_of(kind: FileKind) -> usize {
    match kind {
        FileKind::Log => SLOT_LOG,
        FileKind::Run | FileKind::Manifest => SLOT_DATA,
    }
}

impl RunStorage for FsRunStorage {
    fn create(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        // A create at this id means the bytes behind it are new.
        self.read_fd_drop(kind, id);
        self.require_write_tier()?;
        let code = kind_code(kind);
        let i = slot_of(kind);
        // A create the provider has taken (`EINPROGRESS`), or whose
        // OPEN_CREATE answered `EAGAIN` once the probe had said the file
        // is missing, is asked again as it is: another lookup in between
        // would be a different request, and on fat32 would discard the
        // directory scan the provider is carrying across steps.
        //
        // Only THAT create is resumed: the one for the same file. Any
        // other create keeps its existence check — at boot, while fat32
        // is still mounting, the store's first calls all answer
        // `EAGAIN`, and a create that skipped the check there would hand
        // back an existing manifest slot as new.
        let me = Inflight {
            op: OP_CREATE,
            kind: code,
            _pad: 0,
            id,
        };
        let resuming = self.inflight_is(me);
        if !resuming {
            if self.inflight.op == OP_CREATE {
                // A different create abandons the one part-way.
                self.inflight = Inflight::none();
            }
            if self.exists(kind, id)? {
                return Err(StorageError::AlreadyExists);
            }
        }
        // The slot is about to hold this new file.
        self.forget_slot_file(i, code, id);
        self.close_slot(i);
        self.make_room_for_writer();
        let mut path = [0u8; PATH_MAX];
        let plen = self.encode_path(kind, id, &mut path);
        let sys = self.sys();
        // SAFETY: valid path buffer; OPEN_CREATE creates the file.
        let fd = unsafe { (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), plen) };
        if is_wait(fd) || fd == FS_E_NFILE {
            // The probe said missing and nothing else creates our names,
            // so the create asked again skips it (see above). ENFILE moved
            // nothing: the probe is simply repeated.
            if fd != FS_E_NFILE {
                self.inflight = me;
            }
            return self.again(fd);
        }
        self.inflight_clear(me);
        if fd < 0 {
            self.last_errno.set(fd);
            return Err(StorageError::IoFault);
        }
        if let Some(w) = self.slots.get_mut(i) {
            *w = WriteSlot::empty();
            w.fd = fd;
            w.kind = code;
            w.id = id;
            w.sized = 1;
        }
        self.last_errno.set(0);
        Ok(())
    }

    fn write_at(
        &mut self,
        kind: FileKind,
        id: u32,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        self.write_seeked(kind, id, Some(offset), data)
    }

    fn append(&mut self, kind: FileKind, id: u32, data: &[u8]) -> Result<(), StorageError> {
        self.write_seeked(kind, id, None, data)
    }

    fn fsync(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        self.require_write_tier()?;
        let code = kind_code(kind);
        if self.take_lost(slot_of(kind), code, id) {
            self.last_errno.set(FS_E_FENCE_LOST);
            return Err(StorageError::NotDurable);
        }
        let i = self.open_for_write(kind, id)?;
        let fd = self.slot_fd(i);
        let me = Inflight {
            op: OP_FSYNC,
            kind: code,
            _pad: 0,
            id,
        };
        let sys = self.sys();
        // SAFETY: fd from open_for_write; FSYNC takes no arg. Asked again
        // after EINPROGRESS this is the byte-identical request.
        let rc = unsafe { (sys.provider_call)(fd, FS_FSYNC, core::ptr::null_mut(), 0) };
        if is_wait(rc) {
            if rc == FS_E_INPROGRESS {
                self.inflight = me;
            }
            return self.again(rc);
        }
        self.inflight_clear(me);
        if rc != 0 {
            // The barrier failed: those bytes are unprovable, and stay so
            // — the file is LOST, and a later fence on a fresh descriptor
            // answers `NotDurable` rather than vouching for them.
            self.last_errno.set(rc);
            self.fence_failed(i);
            return Err(StorageError::NotDurable);
        }
        if let Some(w) = self.slots.get_mut(i) {
            w.unfenced = 0;
        }
        self.last_errno.set(0);
        Ok(())
    }

    fn fsync_name(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        self.require_write_tier()?;
        let me = Inflight {
            op: OP_FSYNC_NAME,
            kind: kind_code(kind),
            _pad: 0,
            id,
        };
        let mut path = [0u8; PATH_MAX];
        let plen = self.encode_path(kind, id, &mut path);
        let sys = self.sys();
        // SAFETY: valid path buffer; FSYNC_NAME reads it. Asked again after
        // EINPROGRESS this is the byte-identical request.
        let rc = unsafe { (sys.provider_call)(-1, FS_FSYNC_NAME, path.as_mut_ptr(), plen) };
        if is_wait(rc) {
            if rc == FS_E_INPROGRESS {
                self.inflight = me;
            }
            return self.again(rc);
        }
        self.inflight_clear(me);
        match rc {
            0 => {
                self.last_errno.set(0);
                Ok(())
            }
            FS_E_NOENT => {
                self.last_errno.set(rc);
                Err(StorageError::NotFound)
            }
            e => {
                self.last_errno.set(e);
                Err(StorageError::NotDurable)
            }
        }
    }

    fn fsync_async_supported(&self) -> bool {
        self.caps_probe & CAPS_PROBED != 0 && self.caps_probe & FS_CAP_FSYNC_ASYNC != 0
    }

    /// Open a non-blocking durability fence over this file.
    ///
    /// This is the call that takes the 163 ms run fsync off the
    /// scheduler step. When the provider lacks `FS_CAP_FSYNC_ASYNC`
    /// (the linux host provider) it is the plain barrier, asked again
    /// until it decides — correct either way.
    fn fsync_submit(&mut self, kind: FileKind, id: u32) -> Result<FsyncTicket, StorageError> {
        self.require_write_tier()?;
        if self.caps_probe & FS_CAP_FSYNC_ASYNC == 0 {
            self.fsync(kind, id)?;
            return Ok(0);
        }
        let code = kind_code(kind);
        if self.take_lost(slot_of(kind), code, id) {
            self.last_errno.set(FS_E_FENCE_LOST);
            return Err(StorageError::NotDurable);
        }
        let i = self.open_for_write(kind, id)?;
        let fd = self.slot_fd(i);
        let sys = self.sys();
        let mut tb = [0u8; 8];
        // SAFETY: fd from open_for_write; FSYNC_SUBMIT writes an 8-byte
        // ticket into `tb` per the fs contract wire shape.
        let rc = unsafe { (sys.provider_call)(fd, FS_FSYNC_SUBMIT, tb.as_mut_ptr(), 8) };
        if is_wait(rc) {
            // No fence slot free yet (or no fence could be opened without
            // waiting): nothing was opened; ask again.
            return self.again(rc);
        }
        if rc != 0 {
            self.last_errno.set(rc);
            self.fence_failed(i);
            return Err(StorageError::NotDurable);
        }
        if let Some(w) = self.slots.get_mut(i) {
            w.unfenced = 0;
            w.ticket_open = 1;
        }
        self.last_errno.set(0);
        Ok(FsyncTicket::from_le_bytes(tb))
    }

    fn fsync_poll(
        &mut self,
        kind: FileKind,
        id: u32,
        ticket: FsyncTicket,
    ) -> Result<FsyncState, StorageError> {
        if self.caps_probe & FS_CAP_FSYNC_ASYNC == 0 {
            // The submit already performed the barrier.
            return Ok(FsyncState::Durable);
        }
        let code = kind_code(kind);
        let i = slot_of(kind);
        // A ticket is valid only on the descriptor that produced it. If
        // that descriptor is gone, nothing can prove the fence: fail
        // closed (never a false durable, never a poll on another fd).
        if !self.slots.get(i).is_some_and(|w| w.holds(code, id)) {
            self.take_lost(i, code, id);
            self.last_errno.set(FS_E_FENCE_LOST);
            return Err(StorageError::NotDurable);
        }
        let fd = self.slot_fd(i);
        let sys = self.sys();
        let mut tb = ticket.to_le_bytes();
        // SAFETY: fd holds the ticket; FSYNC_POLL reads the 8-byte
        // ticket from `tb`.
        let rc = unsafe { (sys.provider_call)(fd, FS_FSYNC_POLL, tb.as_mut_ptr(), 8) };
        match rc {
            FS_FENCE_DURABLE => {
                if let Some(w) = self.slots.get_mut(i) {
                    w.ticket_open = 0;
                }
                self.last_errno.set(0);
                Ok(FsyncState::Durable)
            }
            FS_FENCE_PENDING | FS_E_AGAIN => Ok(FsyncState::Pending),
            _ => {
                // A fenced write FAILED. Fail closed exactly as a failed
                // synchronous fsync does — the caller abandons the
                // unpublished run rather than retrying in a way that
                // masks the gap.
                self.last_errno.set(rc);
                self.fence_failed(i);
                Err(StorageError::NotDurable)
            }
        }
    }

    fn read_at(
        &self,
        kind: FileKind,
        id: u32,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize, StorageError> {
        if offset > u64::from(u32::MAX) {
            self.last_errno.set(-27);
            return Err(StorageError::IoFault);
        }
        let sys = self.sys();
        let code = kind_code(kind);
        // Reuse an already-open descriptor if we hold one. This is the
        // whole point: the rig measured ~42 ms per read_at, essentially
        // all of it the FS_OPEN (a linear FAT32 directory scan that
        // grows with the run files), against a 4 KiB read.
        let mut cached = self.read_fds.get();
        let hit = cached.lookup(code, id);
        self.read_fds.set(cached);
        let fd = match hit {
            Some(fd) => fd,
            None => {
                let mut path = [0u8; PATH_MAX];
                let plen = self.encode_path(kind, id, &mut path);
                // SAFETY: valid path buffer; FS_OPEN returns a read fd.
                let fd = unsafe { (sys.provider_call)(-1, FS_OPEN, path.as_mut_ptr(), plen) };
                self.opens.set(self.opens.get().wrapping_add(1));
                if fd < 0 {
                    if is_wait(fd) || fd == FS_E_NFILE {
                        // Not up yet, or the provider's 8-entry table
                        // (shared with the WAL) momentarily full: ask
                        // again. The cache never makes the latter worse —
                        // it holds at most FD_SLOTS descriptors and evicts
                        // its own before opening another.
                        return self.again(fd);
                    }
                    self.last_errno.set(fd);
                    return Err(if open_says_missing(fd) {
                        StorageError::NotFound
                    } else {
                        StorageError::IoFault
                    });
                }
                // Retain it now, whatever the read answers: a read that
                // waits is asked again on the same descriptor. `insert`
                // hands back whatever it displaced so the close is
                // explicit and the cache never exceeds its budget.
                let mut c = self.read_fds.get();
                let evicted = c.insert(code, id, fd);
                self.read_fds.set(c);
                if let Some(old) = evicted {
                    self.close_fd(old);
                }
                fd
            }
        };
        let seek = (offset as u32).to_le_bytes();
        // SAFETY: fd is open for read; 4-byte LE seek position.
        let rc = unsafe { (sys.provider_call)(fd, FS_SEEK, seek.as_ptr() as *mut u8, 4) };
        let n = if rc < 0 {
            rc
        } else {
            // SAFETY: `out` is valid for writes of its length. A short
            // count is what the provider had cached; the caller asks for
            // the rest.
            unsafe { (sys.provider_call)(fd, FS_READ, out.as_mut_ptr(), out.len()) }
        };
        self.reads.set(self.reads.get().wrapping_add(1));
        if is_wait(n) {
            // Nothing read yet; the provider is fetching it.
            return self.again(n);
        }
        if n < 0 {
            // The handle is suspect: close it and forget it rather than
            // caching a descriptor we just failed on.
            self.read_fd_drop(kind, id);
            self.last_errno.set(n);
            return Err(StorageError::IoFault);
        }
        self.read_bytes
            .set(self.read_bytes.get().wrapping_add(n as u64));
        self.last_errno.set(0);
        Ok(n as usize)
    }

    fn delete(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        self.require_write_tier()?;
        let code = kind_code(kind);
        // The file is going: its descriptor closes and its bytes need no
        // fence (its name goes with them).
        self.forget_slot_file(slot_of(kind), code, id);
        // BEFORE the unlink, for two reasons: run ids are reused after
        // compaction retires a run, so a surviving descriptor would
        // later read another run's bytes; and the provider returns
        // EBUSY for an unlink against a file some handle still holds
        // open.
        self.read_fd_drop(kind, id);
        let me = Inflight {
            op: OP_UNLINK,
            kind: code,
            _pad: 0,
            id,
        };
        let resuming = self.inflight_is(me);
        let mut path = [0u8; PATH_MAX];
        let plen = self.encode_path(kind, id, &mut path);
        let sys = self.sys();
        // SAFETY: valid path buffer; UNLINK removes by path. Asked again
        // after EINPROGRESS this is the byte-identical request.
        let rc = unsafe { (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), plen) };
        if is_wait(rc) || rc == FS_E_BUSY {
            if rc == FS_E_INPROGRESS {
                self.inflight = me;
            }
            return self.again(rc);
        }
        self.inflight_clear(me);
        match rc {
            0 => {
                self.last_errno.set(0);
                Ok(())
            }
            // The unlink this backend started was decided while nobody
            // was asking, and that decision was dropped for another
            // client's operation: the name is gone because it was
            // removed. Not "it never existed".
            FS_E_NOENT if resuming => {
                self.last_errno.set(0);
                Ok(())
            }
            FS_E_NOENT => {
                self.last_errno.set(rc);
                Err(StorageError::NotFound)
            }
            e => {
                self.last_errno.set(e);
                Err(StorageError::IoFault)
            }
        }
    }

    fn list(&self, kind: FileKind, out: &mut [u32]) -> Result<usize, StorageError> {
        let want_ext: [u8; 3] = match kind {
            FileKind::Run => *b"KRN",
            FileKind::Manifest => *b"KMN",
            FileKind::Log => *b"KLG",
        };
        let code = kind_code(kind);
        let sys = self.sys();
        let mut walk = self.walk.get();
        let out_at = out.as_ptr() as usize;
        if walk.fd >= 0 && (walk.kind != code || walk.out_at != out_at || walk.out_len != out.len())
        {
            // A walk of another kind, or into another buffer, part-way is
            // abandoned: only the buffer it filled holds what it found.
            self.close_fd(walk.fd);
            walk = ListWalk::idle();
        }
        if walk.fd < 0 {
            // Directory to scan: `kv` on linux layout, the FS root on
            // bare-metal FAT32.
            let mut dpath = [0u8; PATH_MAX];
            let dlen = self.dir_path(&mut dpath);
            // SAFETY: valid path buffer; OPENDIR returns a dir fd.
            let dir_fd = unsafe { (sys.provider_call)(-1, FS_OPENDIR, dpath.as_mut_ptr(), dlen) };
            if is_wait(dir_fd) || dir_fd == FS_E_NFILE {
                return self.again(dir_fd);
            }
            if dir_fd < 0 {
                self.last_errno.set(dir_fd);
                // A missing directory (`kv/` not created on linux) is a
                // deployment fault, not an empty store — fail loudly
                // rather than reporting healthy emptiness.
                return Err(StorageError::IoFault);
            }
            walk = ListWalk {
                fd: dir_fd,
                kind: code,
                _pad: [0; 3],
                count: 0,
                out_at,
                out_len: out.len(),
            };
        }
        let dir_fd = walk.fd;
        let mut count = walk.count as usize;
        let mut overflow = false;
        let mut buf = [0u8; READDIR_BUF];
        loop {
            // SAFETY: `buf` is a valid output buffer of READDIR_BUF
            // bytes; the provider fills whole entries only.
            let n =
                unsafe { (sys.provider_call)(dir_fd, FS_READDIR, buf.as_mut_ptr(), READDIR_BUF) };
            if is_wait(n) {
                // The cursor did not move: the walk stands here, with
                // what it found already in `out`, and continues when the
                // caller asks again.
                walk.count = count as u32;
                self.walk.set(walk);
                return self.again(n);
            }
            if n <= 0 {
                if n < 0 {
                    self.last_errno.set(n);
                    self.close_fd(dir_fd);
                    self.walk.set(ListWalk::idle());
                    return Err(StorageError::IoFault);
                }
                break; // drained
            }
            let n = n as usize;
            if n < 2 {
                break;
            }
            let batch = u16::from_le_bytes([buf[0], buf[1]]) as usize;
            let mut pos = 2usize;
            let mut emitted = 0usize;
            while emitted < batch && pos + 2 <= n {
                let name_len = buf[pos] as usize;
                let entry_type = buf[pos + 1];
                pos += 2;
                if pos + name_len > n {
                    break;
                }
                let name = &buf[pos..pos + name_len];
                pos += name_len;
                emitted += 1;
                if entry_type == 1 {
                    continue; // directory
                }
                if let Some(id) = parse_kv_name(name, &want_ext) {
                    let Some(slot) = out.get_mut(count) else {
                        overflow = true;
                        break;
                    };
                    *slot = id;
                    count += 1;
                }
            }
            if overflow || batch == 0 {
                break;
            }
        }
        self.close_fd(dir_fd);
        self.walk.set(ListWalk::idle());
        if overflow {
            // Not the provider's errno: a listing that outgrew the
            // caller's table, or a READDIR whose cursor never advances.
            self.last_errno.set(FS_E_LIST_OVERFLOW);
            return Err(StorageError::Overflow);
        }
        self.last_errno.set(0);
        Ok(count)
    }
}

/// Parse `"K<7hex>.<ext>"` (case-insensitive; FAT32 8.3 entries come
/// back uppercase) into the file id. `None` = not one of ours.
fn parse_kv_name(name: &[u8], want_ext: &[u8; 3]) -> Option<u32> {
    if name.len() != 12 {
        return None;
    }
    if !name[0].eq_ignore_ascii_case(&b'K') || name[8] != b'.' {
        return None;
    }
    for i in 0..3 {
        if name[9 + i].to_ascii_uppercase() != want_ext[i] {
            return None;
        }
    }
    let mut id = 0u32;
    for &b in &name[1..8] {
        let nibble = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            _ => return None,
        };
        id = (id << 4) | u32::from(nibble);
    }
    Some(id)
}

impl FsRunStorage {
    /// Shared write body for `append` (`at = None`: the file's end) and
    /// `write_at`. A write the provider took part of keeps its progress
    /// on the slot; the same write asked again continues after it.
    fn write_seeked(
        &mut self,
        kind: FileKind,
        id: u32,
        at: Option<u64>,
        data: &[u8],
    ) -> Result<(), StorageError> {
        // The file's content is changing under any cached reader.
        self.read_fd_drop(kind, id);
        self.require_write_tier()?;
        let i = self.open_for_write(kind, id)?;
        let Some(w) = self.slots.get(i).copied() else {
            return Err(StorageError::IoFault);
        };
        let len = data.len();
        let print = fingerprint(data);
        // The write part-way, if this is it: same shape, same bytes, and
        // (for `write_at`) the same offset.
        let resume = w.pend_len != 0
            && w.pend_len as usize == len
            && w.pend_print == print
            && at.is_none_or(|a| a == w.pend_at);
        let (start, mut done) = if resume {
            (w.pend_at, w.pend_done as usize)
        } else {
            (at.unwrap_or(w.len), 0usize)
        };
        if start > u64::from(u32::MAX) - len as u64 || len > u32::MAX as usize {
            // The 4-byte FS_SEEK arg caps files at 4 GiB — far beyond
            // any bounded run/manifest, but fail closed regardless.
            self.last_errno.set(-27); // EFBIG-shaped: hard
            return Err(StorageError::IoFault);
        }
        let fd = w.fd;
        while done < len {
            let seek = ((start + done as u64) as u32).to_le_bytes();
            let sys = self.sys();
            // SAFETY: fd from open_for_write; seek arg is a 4-byte LE
            // position per the WAL's established FS_SEEK usage.
            let rc = unsafe { (sys.provider_call)(fd, FS_SEEK, seek.as_ptr() as *mut u8, 4) };
            let rest = data.get(done..).unwrap_or(&[]);
            let moved = if rc < 0 {
                rc
            } else {
                // SAFETY: `rest` is valid for reads of its length;
                // FS_WRITE copies it out before returning (the provider
                // never keeps a pointer into it).
                unsafe { (sys.provider_call)(fd, FS_WRITE, rest.as_ptr() as *mut u8, rest.len()) }
            };
            if is_wait(moved) || moved == 0 {
                // Nothing more moved now. What did move is the
                // provider's; the same write asked again continues.
                self.note_pending(i, start, len, done, print);
                return self.again(if moved == 0 { FS_E_AGAIN } else { moved });
            }
            if moved < 0 {
                // A torn tail the caller treats as unusable until
                // recovery revalidates it. Drop the slot so the stale
                // cursor can't mask the tear.
                self.last_errno.set(moved);
                if let Some(s) = self.slots.get_mut(i) {
                    s.pend_len = 0;
                }
                self.close_slot(i);
                return Err(StorageError::IoFault);
            }
            done += (moved as usize).min(rest.len());
            if let Some(s) = self.slots.get_mut(i) {
                s.unfenced = 1;
                s.len = s.len.max(start + done as u64);
            }
            if done < len {
                // Short: backpressure. Not asked again inside this call —
                // the device moves between steps.
                self.note_pending(i, start, len, done, print);
                return self.again(FS_E_AGAIN);
            }
        }
        if let Some(s) = self.slots.get_mut(i) {
            s.pend_len = 0;
            s.pend_done = 0;
            s.len = s.len.max(start + len as u64);
        }
        self.last_errno.set(0);
        Ok(())
    }

    fn note_pending(&mut self, i: usize, at: u64, len: usize, done: usize, print: u32) {
        if let Some(s) = self.slots.get_mut(i) {
            s.pend_at = at;
            s.pend_len = len as u32;
            s.pend_done = done as u32;
            s.pend_print = print;
        }
    }
}
