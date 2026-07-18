//! `RunStorage` — the storage-backend boundary beneath the ordered
//! disk state-store provider (`disk_store.rs`, RFC database foundation
//! §9.5).
//!
//! The provider performs ALL of its I/O through this trait so that the
//! exact same provider logic compiles into two targets:
//!
//! - the no_std PIC module build, where each call maps onto Fluxor
//!   `file.data` / `storage.block` opcodes (submitted and polled by the
//!   hosting module — the trait itself stays synchronous and each call
//!   is small and bounded so that mapping is mechanical);
//! - host-side cargo tests, where a `std::fs` implementation (plus a
//!   fault-injection wrapper) lives in `tests/contract_disk_store.rs`.
//!
//! File identity is deliberately minimal: a `FileKind` (run vs
//! manifest) plus a `u32` id. The provider owns id assignment — run ids
//! are monotonically increasing, manifest ids ARE the manifest
//! generation number — so the backend never interprets ids beyond
//! namespacing files by them.
//!
//! Contract rules:
//!
//! - Every operation is synchronous *at this layer* and bounded: one
//!   create/delete, one contiguous append, one contiguous read, one
//!   durability barrier. No operation may be internally retried into
//!   an unbounded loop by the backend.
//! - Fail closed. A backend must never report success for work it did
//!   not durably perform; `fsync` in particular must return
//!   `NotDurable` rather than silently no-op when the barrier cannot
//!   be guaranteed.
//! - `append` is all-or-error from the caller's perspective, but the
//!   provider is written to tolerate torn physical tails after a
//!   crash: recovery validates checksums and discards unreferenced
//!   files rather than trusting append atomicity.

#![allow(
    dead_code,
    reason = "shared via #[path] into the disk provider and host tests; each consumer uses a subset of the surface"
)]

/// Which namespace a file id lives in. Run files hold immutable sorted
/// data; manifest files name the active runs (id = generation number).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    /// Immutable sorted run file (`disk_store.rs` run format v1).
    Run,
    /// Versioned manifest file (`disk_store.rs` manifest format v1).
    Manifest,
}

/// Typed backend error. Fail closed: callers treat every variant as
/// "the bytes are not where I need them to be" and never downgrade an
/// error into an empty/absent result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageError {
    /// The (kind, id) file does not exist.
    NotFound,
    /// `create` of a (kind, id) that already exists.
    AlreadyExists,
    /// Any I/O fault: short write, device error, revoked handle.
    IoFault,
    /// A durability barrier could not be guaranteed. Distinct from
    /// `IoFault` so callers can surface "your data may not survive
    /// power loss" precisely.
    NotDurable,
    /// `list` output slice too small for the number of files present.
    Overflow,
}

/// State of a durability fence opened by [`RunStorage::fsync_submit`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncState {
    /// The barrier has not completed. The caller must poll again on a
    /// later step; NOTHING may be published or released on the
    /// strength of a fence in this state.
    Pending,
    /// Every byte the fence covers is on non-volatile media.
    Durable,
}

/// Opaque durability-fence ticket.
pub type FsyncTicket = u64;

/// Bounded storage backend. See module docs for the dual-target
/// contract.
///
/// ## Durability barriers: synchronous and submit/poll
///
/// `fsync` is the synchronous barrier and remains the contract's
/// baseline. It is also, on real hardware, the single longest
/// operation in the provider: a run-file fsync on FAT32-over-NVMe was
/// **measured at 163 ms** on the Pi 5 rig — 10x the largest step
/// deadline the scheduler will accept, which killed the hosting module
/// outright even though the flush itself succeeded.
///
/// So the trait also carries a non-blocking form: `fsync_submit` opens
/// a fence and returns a ticket, `fsync_poll` reports whether it has
/// completed. A caller driven by a scheduler steps out between the two
/// and the barrier leaves the step budget entirely. This mirrors the
/// FS contract's `FSYNC_SUBMIT`/`FSYNC_POLL` (0x0910/0x0911, capability
/// `FS_CAP_FSYNC_ASYNC`) and the proven idiom in clustor's WAL on this
/// exact device.
///
/// The DEFAULT implementations make the async shape universal without
/// requiring every backend to implement it: `fsync_submit` performs the
/// synchronous `fsync` and `fsync_poll` answers `Durable` immediately.
/// A backend without the capability therefore runs byte-for-byte the
/// old path while its caller runs the same state machine — which means
/// the state machine is exercised by every host test rather than only
/// on hardware.
pub trait RunStorage {
    /// Create an empty (kind, id) file. Errors with `AlreadyExists`
    /// if the id is already taken in that namespace.
    fn create(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError>;

    /// Append `data` at the end of the file. All-or-error: on `Ok` the
    /// backend has accepted every byte (durability still requires
    /// `fsync`); on `Err` the caller must treat the file as torn and
    /// unusable until recovery revalidates it.
    fn append(&mut self, kind: FileKind, id: u32, data: &[u8]) -> Result<(), StorageError>;

    /// Durability barrier: on `Ok` every previously appended byte of
    /// this file (and its directory entry) survives power loss.
    fn fsync(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError>;

    /// Does this backend implement a genuinely non-blocking
    /// `fsync_submit`/`fsync_poll` pair?
    ///
    /// Purely informational — callers must run the submit/poll state
    /// machine either way, because the default implementations below
    /// are a correct (if blocking) realisation of it. Used for
    /// diagnostics, so an operator can tell "the fence completed on the
    /// first poll because the device is fast" from "…because this
    /// backend has no async tier".
    fn fsync_async_supported(&self) -> bool {
        false
    }

    /// Open a durability fence over this file and return a ticket
    /// WITHOUT waiting for it to complete.
    ///
    /// The default performs the synchronous barrier and returns a
    /// ticket that `fsync_poll` will immediately call `Durable` — the
    /// transparent fallback for backends with no async tier.
    fn fsync_submit(&mut self, kind: FileKind, id: u32) -> Result<FsyncTicket, StorageError> {
        self.fsync(kind, id)?;
        Ok(0)
    }

    /// Poll a ticket from `fsync_submit`.
    ///
    /// `Pending` means keep polling and publish NOTHING. An `Err` means
    /// the fence failed: the covered bytes are not durable and the
    /// caller must fail closed exactly as it would for a failed
    /// synchronous `fsync` — never retry in a way that masks it.
    fn fsync_poll(
        &mut self,
        kind: FileKind,
        id: u32,
        ticket: FsyncTicket,
    ) -> Result<FsyncState, StorageError> {
        let _ = (kind, id, ticket);
        Ok(FsyncState::Durable)
    }

    /// Overwrite `data` at `offset`, extending the file if needed.
    ///
    /// Exists for FIXED-SLOT files that are written in place rather
    /// than created anew: the manifest A/B pair. Creating a file on
    /// FAT32 was **measured at 159 ms** on the Pi 5 rig — a linear
    /// directory scan, a dirent patch, a cluster allocation writing its
    /// FAT entry into every FAT copy as a read-modify-write, and a
    /// first-cluster zero-fill, every one of them a synchronous
    /// spin-polled single-sector write. Rewriting an already-allocated
    /// file touches none of that.
    fn write_at(
        &mut self,
        kind: FileKind,
        id: u32,
        offset: u64,
        data: &[u8],
    ) -> Result<(), StorageError>;

    /// Read up to `out.len()` bytes at `offset`. Returns the byte
    /// count actually read; `0` means at-or-past end of file. Short
    /// reads are legal at EOF only — callers use `Ok(0)` to probe for
    /// file end and otherwise loop to fill.
    fn read_at(
        &self,
        kind: FileKind,
        id: u32,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize, StorageError>;

    /// Delete the (kind, id) file. Deleting a missing file is
    /// `NotFound` — callers that tolerate it say so explicitly.
    fn delete(&mut self, kind: FileKind, id: u32) -> Result<(), StorageError>;

    /// Enumerate existing file ids of `kind` into `out` (order
    /// unspecified). Returns the count, or `Overflow` if `out` cannot
    /// hold every id — never a silent truncation.
    fn list(&self, kind: FileKind, out: &mut [u32]) -> Result<usize, StorageError>;
}
