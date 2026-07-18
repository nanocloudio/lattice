//! `FsRunStorage` — `RunStorage` over the Fluxor FS provider contract
//! (`target/fluxor/fluxor-abi/sdk/contracts/storage/fs.rs`), for the PIC
//! module build of the ordered disk state-store provider
//! (`disk_store.rs`, RFC database foundation §9.5).
//!
//! Every call is synchronous *in-step* and bounded — the same idiom as
//! clustor's `durability` WAL (`deps/clustor/modules/app/durability/
//! wal.rs`): one `provider_call` sequence per operation, no internal
//! retry loops. The FS provider is `fat32` on bare metal and
//! `linux_fs_dispatch` on `target: linux`; both implement the opcodes
//! used here (probed once via `FS_CAPS`).
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
//! ## Error mapping and E_AGAIN
//!
//! Negative errnos map onto the typed `StorageError` set, always
//! failing closed. `FS_E_AGAIN` (-11) — the provider is present but
//! still initialising (fat32 reading the BPB on a pi5 cold boot) — is
//! surfaced as `IoFault` like any other fault, but the raw errno of
//! the last failure is retained in `last_errno` so the hosting module
//! can distinguish "retry next step" (the WAL's E_AGAIN-patient wait)
//! from a hard fault that must quarantine. This keeps the `RunStorage`
//! contract's "no internal retry loops" rule intact: patience lives in
//! the module step loop, not here.

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
/// FS capability-discovery opcode + bits (`fs.rs::{CAPS, caps}`).
const FS_CAPS: u32 = 0x09FF;
const FS_CAP_OPEN_CREATE: u32 = 1 << 2;
const FS_CAP_WRITE: u32 = 1 << 3;
const FS_CAP_UNLINK: u32 = 1 << 5;
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

/// FS E_AGAIN: provider present but still initialising. See module
/// docs — surfaced as `IoFault` with `last_errno == FS_E_AGAIN` so the
/// hosting module can wait (retry next step) instead of quarantining.
pub const FS_E_AGAIN: i32 = -11;
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

/// `"kv9/" + 8.3 name` worst case.
const PATH_MAX: usize = 17;

/// Highest `store_id` the linux layout can express: `kv/`, `kv1/` …
/// `kv9/` — one digit, so the 8.3-adjacent path stays fixed-width.
/// Far above any realistic per-process range count (a graph hosts a
/// handful of statically provisioned ranges; thousands of ranges are
/// §11.6 dense-hosting territory, not one-directory-each territory).
pub const STORE_ID_MAX: u8 = 9;
/// FS_READDIR batch buffer. Each entry is `[len:1][type:1][name:≤255]`;
/// our names are 12 bytes so this holds dozens per batch.
const READDIR_BUF: usize = 512;

/// One cached write-side fd. Flush/compact write many appends to one
/// file back-to-back; caching the open fd (plus the append cursor)
/// avoids re-open + FS_STAT per append. Invalidated on create/delete
/// of the same file and on any fault. Read paths never use the cache.
#[repr(C)]
struct WriteCache {
    fd: i32,
    kind_run: bool,
    id: u32,
    /// Append cursor == current file length.
    len: u64,
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
    /// FS_CAPS probe: 0 = not probed, else `bits | PROBED`.
    caps_probe: u32,
    cache: WriteCache,
    /// Raw errno of the most recent provider failure (0 = none).
    /// `Cell` because read-side methods take `&self` per the contract.
    last_errno: Cell<i32>,
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
        self.caps_probe = 0;
        self.cache = WriteCache {
            fd: -1,
            kind_run: false,
            id: 0,
            len: 0,
        };
        self.last_errno = Cell::new(0);
        self.reads = Cell::new(0);
        self.read_bytes = Cell::new(0);
        self.opens = Cell::new(0);
        // Descriptors from a previous life are already gone, so this is
        // the one place a reset without closing is correct.
        self.read_fds = Cell::new(FdCache::new());
        true
    }

    /// Monotonic `(read_at calls, bytes, FS_OPEN calls)` since `init`.
    pub fn read_counters(&self) -> (u64, u64, u64) {
        (self.reads.get(), self.read_bytes.get(), self.opens.get())
    }

    /// Close and forget the cached read descriptor for one file.
    ///
    /// MUST be called before anything that changes what `(kind, id)`
    /// names — a descriptor that outlives its file would read another
    /// run's bytes and answer confidently with them.
    fn read_fd_drop(&self, kind: FileKind, id: u32) {
        let mut c = self.read_fds.get();
        let victim = c.drop_entry(matches!(kind, FileKind::Run), id);
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

    fn close_fd(&self, fd: i32) {
        if fd < 0 {
            return;
        }
        let sys = self.sys();
        // SAFETY: closing a descriptor this cache opened and owns.
        unsafe { (sys.provider_call)(fd, FS_CLOSE, core::ptr::null_mut(), 0) };
    }

    /// Raw errno of the most recent failed provider call (0 if the
    /// last call succeeded). `FS_E_AGAIN` here means "provider still
    /// initialising — retry next step".
    pub fn last_errno(&self) -> i32 {
        self.last_errno.get()
    }

    /// True when the most recent failure was the transient
    /// provider-initialising errno.
    pub fn last_was_again(&self) -> bool {
        self.last_errno.get() == FS_E_AGAIN
    }

    /// True when the most recent failure is one that a later step can
    /// reasonably expect to succeed — "wait", not "this store is
    /// broken". The hosting module must not quarantine on these.
    ///
    /// Wider than [`last_was_again`] because `E_AGAIN` is not the only
    /// self-clearing condition a shared FS provider produces. On
    /// bare-metal FAT32 the KV engine is the *second* client of one
    /// provider instance whose resources durability is also consuming:
    ///
    /// - `EBUSY` — `FS_UNLINK` refuses while any open handle still
    ///   references that file. Hit by `create_fresh`'s orphan cleanup
    ///   and by `recover()`'s orphan-run deletion. Clears as soon as
    ///   the other handle closes.
    /// - `ENFILE` — the provider's fixed open-file table (8 slots on
    ///   fat32) is momentarily exhausted; durability holds several
    ///   across a segment rotation or snapshot persist. Clears as soon
    ///   as one is released.
    ///
    /// Treating either as a hard fault quarantines a perfectly healthy
    /// store because a neighbour module was mid-operation.
    pub fn last_was_transient(&self) -> bool {
        matches!(self.last_errno.get(), FS_E_AGAIN | FS_E_BUSY | FS_E_NFILE)
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
            out[i] = b'k';
            out[i + 1] = b'v';
            i += 2;
            if self.store_id != 0 {
                out[i] = b'0' + self.store_id;
                i += 1;
            }
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
        let ext: &[u8; 4] = match kind {
            FileKind::Run => b".KRN",
            FileKind::Manifest => b".KMN",
        };
        for &b in ext {
            out[i] = b;
            i += 1;
        }
        i
    }

    /// Probe the provider capability bitmap once; the write tier
    /// (OPEN_CREATE + WRITE + UNLINK) is required — the provider
    /// manages its own file set, so a backend that can't delete can't
    /// honour the manifest-publish contract. Fail closed if absent.
    fn require_write_tier(&mut self) -> Result<(), StorageError> {
        if self.caps_probe & CAPS_PROBED == 0 {
            let mut buf = [0u8; 4];
            let sys = self.sys();
            // SAFETY: `buf` is a valid 4-byte output buffer for the
            // duration of the call, per the CAPS wire shape.
            let rc = unsafe { (sys.provider_call)(-1, FS_CAPS, buf.as_mut_ptr(), 4) };
            if rc < 0 {
                self.last_errno.set(rc);
                return Err(StorageError::IoFault);
            }
            self.caps_probe = u32::from_le_bytes(buf) | CAPS_PROBED;
        }
        let need = FS_CAP_OPEN_CREATE | FS_CAP_WRITE | FS_CAP_UNLINK;
        if self.caps_probe & need != need {
            self.last_errno.set(-38); // ENOSYS-shaped: hard, not again
            return Err(StorageError::IoFault);
        }
        Ok(())
    }

    /// FS_OPEN existence probe. `Ok(true)` = exists (fd closed again),
    /// `Ok(false)` = ENOENT, `Err` = any other failure (incl. E_AGAIN).
    fn exists(&self, kind: FileKind, id: u32) -> Result<bool, StorageError> {
        let mut path = [0u8; PATH_MAX];
        let plen = self.encode_path(kind, id, &mut path);
        let sys = self.sys();
        // SAFETY: `path[..plen]` is a valid UTF-8 path buffer for the
        // duration of the call; FS_OPEN reads it and returns an fd.
        let fd = unsafe { (sys.provider_call)(-1, FS_OPEN, path.as_mut_ptr(), plen) };
        if fd >= 0 {
            // SAFETY: `fd` came from FS_OPEN above; CLOSE takes no arg.
            unsafe { (sys.provider_call)(fd, FS_CLOSE, core::ptr::null_mut(), 0) };
            return Ok(true);
        }
        if open_says_missing(fd) {
            return Ok(false);
        }
        self.last_errno.set(fd);
        Err(StorageError::IoFault)
    }

    fn drop_cache(&mut self) {
        if self.cache.fd >= 0 {
            let fd = self.cache.fd;
            let sys = self.sys();
            // SAFETY: cached fd from a prior FS_OPEN_CREATE.
            unsafe { (sys.provider_call)(fd, FS_CLOSE, core::ptr::null_mut(), 0) };
        }
        self.cache.fd = -1;
    }

    /// Open (kind, id) for appending, reusing the single-slot cache.
    /// The file must already exist (`create` is the only birth path).
    fn open_for_write(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        let kind_run = matches!(kind, FileKind::Run);
        if self.cache.fd >= 0 && self.cache.kind_run == kind_run && self.cache.id == id {
            return Ok(());
        }
        self.drop_cache();
        if !self.exists(kind, id)? {
            return Err(StorageError::NotFound);
        }
        let mut path = [0u8; PATH_MAX];
        let plen = self.encode_path(kind, id, &mut path);
        let sys = self.sys();
        // SAFETY: valid path buffer; OPEN_CREATE returns a write fd.
        let fd = unsafe { (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), plen) };
        if fd < 0 {
            self.last_errno.set(fd);
            return Err(StorageError::IoFault);
        }
        // FS_STAT output: [size:u32 LE][mtime:u32 LE].
        let mut stat = [0u8; 8];
        // SAFETY: 8-byte stat output buffer per the contract shape.
        let rc = unsafe { (sys.provider_call)(fd, FS_STAT, stat.as_mut_ptr(), 8) };
        if rc < 0 {
            self.last_errno.set(rc);
            // SAFETY: closing the fd we just opened.
            unsafe { (sys.provider_call)(fd, FS_CLOSE, core::ptr::null_mut(), 0) };
            return Err(StorageError::IoFault);
        }
        self.cache = WriteCache {
            fd,
            kind_run,
            id,
            len: u64::from(u32::from_le_bytes([stat[0], stat[1], stat[2], stat[3]])),
        };
        Ok(())
    }
}

impl RunStorage for FsRunStorage {
    fn create(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        // A create at this id means the bytes behind it are new.
        self.read_fd_drop(kind, id);
        self.require_write_tier()?;
        if self.exists(kind, id)? {
            return Err(StorageError::AlreadyExists);
        }
        // A stale cache slot can't refer to this id (it didn't exist),
        // but drop it anyway so the append path re-opens fresh.
        self.drop_cache();
        let mut path = [0u8; PATH_MAX];
        let plen = self.encode_path(kind, id, &mut path);
        let sys = self.sys();
        // SAFETY: valid path buffer; OPEN_CREATE creates the file.
        let fd = unsafe { (sys.provider_call)(-1, FS_OPEN_CREATE, path.as_mut_ptr(), plen) };
        if fd < 0 {
            self.last_errno.set(fd);
            return Err(StorageError::IoFault);
        }
        self.cache = WriteCache {
            fd,
            kind_run: matches!(kind, FileKind::Run),
            id,
            len: 0,
        };
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
        self.write_seeked(kind, id, offset, data)
    }

    fn append(&mut self, kind: FileKind, id: u32, data: &[u8]) -> Result<(), StorageError> {
        let at = self.cache.len;
        self.write_seeked(kind, id, at, data)
    }

    fn fsync(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        self.require_write_tier()?;
        self.open_for_write(kind, id)?;
        let fd = self.cache.fd;
        let sys = self.sys();
        // SAFETY: fd from open_for_write; FSYNC takes no arg.
        let rc = unsafe { (sys.provider_call)(fd, FS_FSYNC, core::ptr::null_mut(), 0) };
        if rc != 0 {
            self.last_errno.set(rc);
            self.drop_cache();
            return Err(StorageError::NotDurable);
        }
        self.last_errno.set(0);
        Ok(())
    }

    fn fsync_async_supported(&self) -> bool {
        self.caps_probe & CAPS_PROBED != 0 && self.caps_probe & FS_CAP_FSYNC_ASYNC != 0
    }

    /// Open a non-blocking durability fence over this file.
    ///
    /// This is the call that takes the 163 ms run fsync off the
    /// scheduler step. When the provider lacks `FS_CAP_FSYNC_ASYNC`
    /// (the linux host provider) it degrades to the synchronous
    /// barrier, which is correct — just blocking — and is what host
    /// runs have always done.
    fn fsync_submit(&mut self, kind: FileKind, id: u32) -> Result<FsyncTicket, StorageError> {
        self.require_write_tier()?;
        if self.caps_probe & FS_CAP_FSYNC_ASYNC == 0 {
            self.fsync(kind, id)?;
            return Ok(0);
        }
        self.open_for_write(kind, id)?;
        let fd = self.cache.fd;
        let sys = self.sys();
        let mut tb = [0u8; 8];
        // SAFETY: fd from open_for_write; FSYNC_SUBMIT writes an 8-byte
        // ticket into `tb` per the fs contract wire shape.
        let rc = unsafe { (sys.provider_call)(fd, FS_FSYNC_SUBMIT, tb.as_mut_ptr(), 8) };
        if rc != 0 {
            // Includes FS_E_AGAIN (the provider's scratch-flush ring is
            // full). Reported as NotDurable with the errno preserved so
            // the caller's transient/hard split still works: a transient
            // refusal retries the whole fence, and because nothing has
            // been published the retry is clean.
            self.last_errno.set(rc);
            self.drop_cache();
            return Err(StorageError::NotDurable);
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
            // The submit already performed the synchronous barrier.
            return Ok(FsyncState::Durable);
        }
        self.open_for_write(kind, id)?;
        let fd = self.cache.fd;
        let sys = self.sys();
        let mut tb = ticket.to_le_bytes();
        // SAFETY: fd from open_for_write; FSYNC_POLL reads the 8-byte
        // ticket from `tb`.
        let rc = unsafe { (sys.provider_call)(fd, FS_FSYNC_POLL, tb.as_mut_ptr(), 8) };
        match rc {
            FS_FENCE_DURABLE => {
                self.last_errno.set(0);
                Ok(FsyncState::Durable)
            }
            FS_FENCE_PENDING => Ok(FsyncState::Pending),
            _ => {
                // A fenced write FAILED. Fail closed exactly as a failed
                // synchronous fsync does — the caller abandons the
                // unpublished run rather than retrying in a way that
                // masks the gap.
                self.last_errno.set(rc);
                self.drop_cache();
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
        let kind_run = matches!(kind, FileKind::Run);
        // Reuse an already-open descriptor if we hold one. This is the
        // whole point: the rig measured ~42 ms per read_at, essentially
        // all of it the FS_OPEN (a linear FAT32 directory scan that
        // grows with the run files), against a 4 KiB read.
        let mut cached = self.read_fds.get();
        let hit = cached.lookup(kind_run, id);
        self.read_fds.set(cached);
        let (fd, from_cache) = match hit {
            Some(fd) => (fd, true),
            None => {
                let mut path = [0u8; PATH_MAX];
                let plen = self.encode_path(kind, id, &mut path);
                // SAFETY: valid path buffer; FS_OPEN returns a read fd.
                let fd = unsafe { (sys.provider_call)(-1, FS_OPEN, path.as_mut_ptr(), plen) };
                self.opens.set(self.opens.get().wrapping_add(1));
                if fd < 0 {
                    self.last_errno.set(fd);
                    return Err(if open_says_missing(fd) {
                        StorageError::NotFound
                    } else {
                        // Includes ENFILE when the provider's 8-entry
                        // table (shared with the WAL) is momentarily
                        // full. The caller already treats that as
                        // transient and retries; the cache never makes
                        // it worse, because it holds at most FD_SLOTS
                        // descriptors and evicts its own before opening
                        // another.
                        StorageError::IoFault
                    });
                }
                (fd, false)
            }
        };
        let seek = (offset as u32).to_le_bytes();
        // SAFETY: fd is open for read; 4-byte LE seek position.
        let rc = unsafe { (sys.provider_call)(fd, FS_SEEK, seek.as_ptr() as *mut u8, 4) };
        let n = if rc < 0 {
            rc
        } else {
            // SAFETY: `out` is valid for writes of its length.
            unsafe { (sys.provider_call)(fd, FS_READ, out.as_mut_ptr(), out.len()) }
        };
        self.reads.set(self.reads.get().wrapping_add(1));
        if n < 0 {
            // The handle is suspect: close it and forget it rather than
            // caching a descriptor we just failed on.
            if from_cache {
                self.read_fd_drop(kind, id);
            } else {
                self.close_fd(fd);
            }
            self.last_errno.set(n);
            return Err(StorageError::IoFault);
        }
        if !from_cache {
            // Retain it. `insert` hands back whatever it displaced so
            // the close is explicit and the cache never exceeds its
            // budget.
            let mut c = self.read_fds.get();
            let evicted = c.insert(kind_run, id, fd);
            self.read_fds.set(c);
            if let Some(old) = evicted {
                self.close_fd(old);
            }
        }
        self.read_bytes
            .set(self.read_bytes.get().wrapping_add(n as u64));
        self.last_errno.set(0);
        Ok(n as usize)
    }

    fn delete(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError> {
        self.require_write_tier()?;
        let kind_run = matches!(kind, FileKind::Run);
        if self.cache.fd >= 0 && self.cache.kind_run == kind_run && self.cache.id == id {
            self.drop_cache();
        }
        // BEFORE the unlink, for two reasons: run ids are reused after
        // compaction retires a run, so a surviving descriptor would
        // later read another run's bytes; and the provider returns
        // EBUSY for an unlink against a file some handle still holds
        // open.
        self.read_fd_drop(kind, id);
        let mut path = [0u8; PATH_MAX];
        let plen = self.encode_path(kind, id, &mut path);
        let sys = self.sys();
        // SAFETY: valid path buffer; UNLINK removes by path.
        let rc = unsafe { (sys.provider_call)(-1, FS_UNLINK, path.as_mut_ptr(), plen) };
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
                Err(StorageError::IoFault)
            }
        }
    }

    fn list(&self, kind: FileKind, out: &mut [u32]) -> Result<usize, StorageError> {
        // Directory to scan: `kv` on linux layout, the FS root on
        // bare-metal FAT32.
        let mut dpath = [0u8; PATH_MAX];
        let dlen = if self.root_path == 0 {
            dpath[0] = b'k';
            dpath[1] = b'v';
            if self.store_id != 0 {
                dpath[2] = b'0' + self.store_id;
                3
            } else {
                2
            }
        } else {
            dpath[0] = b'/';
            1
        };
        let sys = self.sys();
        // SAFETY: valid path buffer; OPENDIR returns a dir fd.
        let dir_fd = unsafe { (sys.provider_call)(-1, FS_OPENDIR, dpath.as_mut_ptr(), dlen) };
        if dir_fd < 0 {
            self.last_errno.set(dir_fd);
            // A missing directory (`kv/` not created on linux) is a
            // deployment fault, not an empty store — fail loudly
            // rather than reporting healthy emptiness.
            return Err(StorageError::IoFault);
        }
        let want_ext: &[u8; 3] = match kind {
            FileKind::Run => b"KRN",
            FileKind::Manifest => b"KMN",
        };
        let mut count = 0usize;
        let mut overflow = false;
        let mut buf = [0u8; READDIR_BUF];
        loop {
            // SAFETY: `buf` is a valid output buffer of READDIR_BUF
            // bytes; the provider fills whole entries only.
            let n =
                unsafe { (sys.provider_call)(dir_fd, FS_READDIR, buf.as_mut_ptr(), READDIR_BUF) };
            if n <= 0 {
                if n < 0 {
                    self.last_errno.set(n);
                    // SAFETY: closing the dir fd we opened.
                    unsafe { (sys.provider_call)(dir_fd, FS_CLOSE, core::ptr::null_mut(), 0) };
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
                if let Some(id) = parse_kv_name(name, want_ext) {
                    if count >= out.len() {
                        overflow = true;
                        break;
                    }
                    out[count] = id;
                    count += 1;
                }
            }
            if overflow || batch == 0 {
                break;
            }
        }
        // SAFETY: closing the dir fd we opened.
        unsafe { (sys.provider_call)(dir_fd, FS_CLOSE, core::ptr::null_mut(), 0) };
        if overflow {
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
    /// Shared seek+write body for `append` and `write_at`.
    fn write_seeked(
        &mut self,
        kind: FileKind,
        id: u32,
        at: u64,
        data: &[u8],
    ) -> Result<(), StorageError> {
        // The file's content is changing under any cached reader.
        self.read_fd_drop(kind, id);
        self.require_write_tier()?;
        self.open_for_write(kind, id)?;
        if at > u64::from(u32::MAX) - data.len() as u64 {
            // The 4-byte FS_SEEK arg caps files at 4 GiB — far beyond
            // any bounded run/manifest, but fail closed regardless.
            self.last_errno.set(-27); // EFBIG-shaped: hard
            return Err(StorageError::IoFault);
        }
        let fd = self.cache.fd;
        let seek = (at as u32).to_le_bytes();
        let sys = self.sys();
        // SAFETY: fd from open_for_write; seek arg is a 4-byte LE
        // position per the WAL's established FS_SEEK usage.
        let rc = unsafe { (sys.provider_call)(fd, FS_SEEK, seek.as_ptr() as *mut u8, 4) };
        if rc < 0 {
            self.last_errno.set(rc);
            self.drop_cache();
            return Err(StorageError::IoFault);
        }
        // SAFETY: `data` is valid for reads of its length; FS_WRITE
        // copies it out before returning.
        let w = unsafe { (sys.provider_call)(fd, FS_WRITE, data.as_ptr() as *mut u8, data.len()) };
        if w < 0 || (w as usize) != data.len() {
            // Short write = torn tail; the caller treats the file as
            // unusable until recovery revalidates it. Drop the cache so
            // the stale cursor can't mask the tear.
            self.last_errno.set(if w < 0 { w } else { -5 });
            self.drop_cache();
            return Err(StorageError::IoFault);
        }
        self.cache.len = self.cache.len.max(at + data.len() as u64);
        self.last_errno.set(0);
        Ok(())
    }
}
