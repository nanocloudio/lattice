//! `KvStateStore` — the shared state-store contract (RFC database
//! foundation §9).
//!
//! There is ONE Lattice semantic engine (`kv_store.rs::apply` today) and
//! one committed command format. Memory and disk are replaceable
//! *physical materialization providers* beneath that engine — never
//! different KV products. This file defines the provider contract both
//! must satisfy, plus the bounded progress types that keep every method
//! resumable inside a cooperative Fluxor step.
//!
//! Contract rules (§9, §9.3, §9.4):
//!
//! - The engine must not contain conditionals that change command
//!   *meaning* according to the provider.
//! - Clustor's per-partition WAL is the sole authoritative logical WAL.
//!   A provider may keep a local crash-consistency journal solely to
//!   publish file manifests atomically; it carries no acknowledgement
//!   authority and is always discardable.
//! - All methods are bounded or resumable: no method may block a
//!   cooperative step on I/O. Disk providers submit/poll async storage
//!   operations and report `InProgress` until complete.
//! - Given the same authorized snapshot and committed command suffix,
//!   memory and disk providers must expose equivalent logical state
//!   (differentially tested — `tests/contract_state_store.rs`).

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

/// Monotonic per-range revision (mirrors `types::Revision`).
pub type Revision = u64;
/// Logical MVCC timestamp (§10). Assigned before proposal, encoded in
/// the committed command; never derived from a local clock during apply.
pub type MvccTimestamp = u64;

/// Version of the provider contract itself. Recorded alongside
/// `internal_key::KEY_FORMAT_VERSION` in manifests.
pub const STATE_STORE_CONTRACT_VERSION: u16 = 1;

// ── Spans ─────────────────────────────────────────────────────────────

/// Half-open span `[start, end)` over *encoded internal key prefixes*
/// (see `internal_key::encode_prefix`). An empty `end` means "to the
/// end of the keyspace component".
#[derive(Clone, Copy, Debug)]
pub struct KeySpan<'a> {
    pub start: &'a [u8],
    pub end: &'a [u8],
}

// ── Results and progress ──────────────────────────────────────────────

/// Typed provider error. Providers fail closed: an error must never be
/// silently downgraded to an empty/absent result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// Key absent (or not visible at the requested revision).
    NotFound,
    /// Requested revision is below the provider's compaction floor.
    Compacted,
    /// Output buffer too small for the value/entry.
    OutputTooSmall,
    /// Provider capacity exhausted (bounded arenas / memtable full).
    /// The caller must apply backpressure, not drop.
    Backpressure,
    /// Malformed input (bad span, bad snapshot chunk, bad batch).
    Malformed,
    /// Underlying storage fault; the range must quarantine, not guess.
    StorageFault,
    /// Snapshot/install generation or identity mismatch (§17.1).
    FenceMismatch,
}

/// Bounded, resumable operation progress. Any long-running provider
/// operation (scan, flush-backed apply, snapshot, install, compaction)
/// returns `InProgress` rather than blocking a cooperative step; the
/// caller re-invokes with the returned cursor on a later step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
    /// Operation fully complete.
    Done,
    /// More work remains; resume from `cursor` (provider-defined,
    /// opaque, valid only for this provider instance and generation).
    InProgress { cursor: u64 },
}

/// Result of one bounded scan step.
#[derive(Clone, Copy, Debug)]
pub struct ScanProgress {
    /// Entries written into the caller's output this step.
    pub entries: usize,
    /// Bytes written into the caller's output this step.
    pub bytes: usize,
    /// Whether the span is exhausted or the caller must resume.
    pub progress: Progress,
    /// The provider stopped at its per-call step budget, not because
    /// the output filled or the span ended. `progress` is
    /// `InProgress` and may carry ZERO new entries; the caller re-calls
    /// with the cursor it names, on a later step, and the provider
    /// continues from where it stood. Never set with `Done`.
    pub paused: bool,
}

impl ScanProgress {
    /// The span is exhausted after `entries` entries / `bytes` bytes.
    pub const fn done(entries: usize, bytes: usize) -> Self {
        Self {
            entries,
            bytes,
            progress: Progress::Done,
            paused: false,
        }
    }
}

/// Result of applying one committed batch.
#[derive(Clone, Copy, Debug)]
pub struct ApplyResult {
    /// Highest revision materialized by this apply.
    pub revision: Revision,
    /// Provider needs a flush cycle before it can accept sustained
    /// further batches (advisory backpressure signal, not an error).
    pub flush_wanted: bool,
}

/// Snapshot capture request (§17.1). `applied_index` is the exact
/// Clustor apply prefix the snapshot must represent — never "roughly
/// now".
#[derive(Clone, Copy, Debug)]
pub struct SnapshotRequest {
    pub applied_index: u64,
    pub range_generation: u32,
}

/// One bounded chunk of snapshot state (capture or install side).
#[derive(Clone, Copy, Debug)]
pub struct SnapshotChunk<'a> {
    pub offset: u64,
    pub data: &'a [u8],
    pub last: bool,
}

// ── The provider contract ─────────────────────────────────────────────

/// Physical state-store provider contract (§9). Implemented by the
/// bounded in-memory table (current `KvStore`) and by the ordered disk
/// provider (Phase 1). The semantic engine calls exactly this surface;
/// deployment composition selects the provider at graph construction.
pub trait KvStateStore {
    /// Point read of `key`'s newest version visible at `revision`.
    /// Copies the value into `value_out` and returns its length.
    fn get_at(
        &self,
        key: &[u8],
        revision: Revision,
        value_out: &mut [u8],
    ) -> Result<usize, StoreError>;

    /// Bounded ordered scan of `span` at `revision`. Writes length-
    /// prefixed entries into `out` and reports resumable progress.
    /// `&mut self`: the provider keeps the position the next page
    /// continues from. `limit` is the most entries the caller can take;
    /// the provider stops exactly there so its position is the caller's
    /// next cursor, never ahead of it.
    fn scan_at(
        &mut self,
        span: KeySpan<'_>,
        revision: Revision,
        resume_cursor: u64,
        limit: usize,
        out: &mut [u8],
    ) -> Result<ScanProgress, StoreError>;

    /// Bounded ordered scan of every VERSION in `span` whose assigned
    /// revision falls in `(from_revision, to_revision]`.
    ///
    /// This is deliberately a different question from
    /// [`scan_at`](Self::scan_at), and the difference is the whole
    /// reason it exists. `scan_at` answers "what did the database look
    /// like at revision R" — one winning version per key. A watch
    /// resume asks "what HAPPENED between R1 and R2", and the answer is
    /// every version in between: a key written three times in the
    /// window produced three events, and a watcher that reconnects must
    /// receive three, not the survivor. Collapsing them would silently
    /// rewrite history into its own outcome.
    ///
    /// Tombstones are emitted, not filtered. A delete is an event a
    /// watcher must see; suppressing it would make a resumed stream
    /// disagree with a continuous one about whether a key still exists.
    /// Entries carry the full encoded internal key, so the caller reads
    /// the revision and the value kind off the key exactly as the
    /// provider's own merge walk does.
    ///
    /// Fails `Compacted` when `from_revision` is below the retained
    /// floor. That refusal is the contract: the provider genuinely no
    /// longer holds the window, and serving the part it still has would
    /// hand back a silently incomplete history that the caller could
    /// not distinguish from a complete one.
    ///
    /// Step-bounded. The walk is in key order over the whole span
    /// (records are stored by key, not by revision), so a window's
    /// qualifying records can be arbitrarily far apart; the provider
    /// stops at its per-call budget and reports `paused`, keeping its
    /// position, and the caller re-calls with the same cursor on a
    /// later step. `&mut self` because that position is provider
    /// state. `limit` is the most entries the caller can take: the
    /// provider stops exactly there, so its position stays the
    /// caller's next cursor rather than running ahead of it.
    fn scan_versions(
        &mut self,
        span: KeySpan<'_>,
        from_revision: Revision,
        to_revision: Revision,
        resume_cursor: u64,
        limit: usize,
        out: &mut [u8],
    ) -> Result<ScanProgress, StoreError>;

    /// Materialize one committed batch (already ordered and durable by
    /// Clustor). Deterministic: outcome depends only on prior state and
    /// the batch bytes.
    fn apply(&mut self, batch: &[u8]) -> Result<ApplyResult, StoreError>;

    /// Capture a snapshot chunk for `request`, resuming from `cursor`.
    /// The provider must present a stable view at the request's applied
    /// index across the whole (multi-step) capture.
    fn snapshot(
        &mut self,
        request: SnapshotRequest,
        cursor: u64,
        out: &mut [u8],
    ) -> Result<(usize, Progress), StoreError>;

    /// Install one chunk of an authorized snapshot. Providers validate
    /// identity/generation before the first chunk and fail closed with
    /// `FenceMismatch` on any inconsistency.
    fn install(
        &mut self,
        request: SnapshotRequest,
        chunk: SnapshotChunk<'_>,
    ) -> Result<Progress, StoreError>;

    /// Reclaim history below `floor` in bounded steps. Must never
    /// advance past the retention floor handed down by the compaction
    /// coordinator (§18) — the provider trusts, and never computes, the
    /// floor.
    fn compact(&mut self, floor: Revision, cursor: u64) -> Result<Progress, StoreError>;
}
