//! MVCC timestamp authority — RFC database-foundation §10.
//!
//! Phase 2 slice 1: the contracts and the executable monotonicity rules
//! behind the replicated timestamp allocator. This file is contracts
//! only — no ports, no I/O, no queueing. The PIC module that owns the
//! ports is `modules/app/timestamp_allocator/`; it is a thin pump over
//! the state machine defined here.
//!
//! §10 states the requirement the whole file exists to make executable:
//!
//! > The first implementation should provide a replicated timestamp
//! > allocator … which leases monotonically ordered timestamp intervals
//! > to transaction coordinators. Leases have upper bounds, identities,
//! > and epochs. **After restart or leadership change, an allocator must
//! > move beyond every previously issued interval before issuing more
//! > timestamps.**
//!
//! Three rules encode that, and `validate_successor` enforces all three
//! as one function:
//!
//! 1. **Intervals never overlap and never move backwards.** Every lease
//!    is half-open `[interval_start, interval_end)`, and a successor
//!    must satisfy `next.interval_start >= prev.interval_end`. Equality
//!    is the normal case (leases are cut from a dense sequence); strict
//!    inequality is the recovery case (a gap is always safe, reuse never
//!    is).
//! 2. **Epochs never move backwards.** `next.epoch >= prev.epoch`.
//! 3. **A change of allocator identity requires a strict epoch bump.**
//!    `prev.allocator_id != next.allocator_id` implies
//!    `next.epoch > prev.epoch`. Without this, two allocators that both
//!    believed themselves authoritative at the same epoch would be
//!    indistinguishable in the committed record, and the record is the
//!    only thing recovery has to work from.
//!
//! ## Durability before issuance
//!
//! The other half of §10 — and §21 invariant 3 — is that a timestamp may
//! only be handed out after the high water covering it is *durably
//! committed*. [`ReserveState`] is the state machine for that: it tracks
//! a `committed_high_water` (established only from committed records)
//! separately from an `issued_high_water` (how far into that committed
//! reserve we have actually handed out), and [`ReserveState::try_issue`]
//! can only ever cut from the gap between them. An outstanding proposal
//! contributes nothing to the issuable range until it comes back as a
//! committed record.
//!
//! Golden vectors: `tests/contract_mvcc.rs`. The `TimestampLease` layout
//! is a persistent, replicated format: changing it is a format break
//! that requires an `MVCC_LEASE_VERSION` bump plus an explicit
//! migration — never an update to the vector.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

/// Version of the `TimestampLease` wire encoding. Bump on ANY layout
/// change; decoders fail closed on an unknown version.
pub const MVCC_LEASE_VERSION: u16 = 1;

/// **Hybrid logical clocks are NOT enabled in v1, and physical time is
/// not the serialization authority.**
///
/// §10 permits a hybrid logical representation "later … provided clock
/// uncertainty is bounded and the same monotonicity and recovery
/// invariants are proven", and closes with the rule this constant
/// exists to keep visible: *physical time must never silently become
/// the serialization authority.*
///
/// So, explicitly, for `MVCC_LEASE_VERSION` 1:
///
/// - A timestamp is a pure logical counter. Nothing in this file reads
///   a clock, and no ordering decision anywhere may consult one.
/// - `lease_expiry_unix_ms` is an *administrative* field only: it bounds
///   how long a holder may keep an already-issued interval before
///   returning for another, and it is carried in the committed record so
///   operators and reapers can reason about abandoned leases. It has no
///   role in ordering, in `validate_successor`, or in visibility. A
///   comparison of two timestamps NEVER consults it.
/// - Adding an HLC means a new `MVCC_LEASE_VERSION`, a new record shape
///   carrying an explicit uncertainty bound, and a re-proof of the
///   monotonicity and recovery invariants against that bound. It is not
///   a compatible extension of this record, and it must not arrive as
///   "the physical component was already there".
pub const HYBRID_NOT_ENABLED: bool = true;

/// Largest interval a single lease may cover. Bounds both the blast
/// radius of one lost lease (timestamps skipped forever) and the size of
/// the reserve a crashed holder can strand. Fail-closed: a request for
/// more is rejected, never clamped — clamping would hand back a lease
/// smaller than the caller asked for under the same identity.
pub const MAX_LEASE_INTERVAL: u64 = 1 << 20;

/// Default size of one high-water advance. Advances are amortized: the
/// allocator reserves in chunks so most requests are served from an
/// already-committed reserve without a round trip through consensus.
pub const DEFAULT_ADVANCE_CHUNK: u64 = 1 << 16;

/// `issued_to` value meaning "reserved, not yet issued to anyone". A
/// high-water advance is committed under this holder; the grant that
/// later cuts an interval out of it names the real holder. Holder ids
/// are therefore required to be non-zero.
pub const ISSUED_TO_NONE: u32 = 0;

// ── Envelope message types ────────────────────────────────────────────
//
// Allocated in the 0xE0 operational band of `modules/common/wire.rs`
// (0xE0/0xE1 retention floors, 0xE2 map update, 0xE5 applied pos).
// Defined here rather than there because these discriminants and the
// record layout below are one contract.

/// Coordinator → allocator: ask for an interval of `size` timestamps.
///
/// Payload (`LEASE_REQUEST_WIRE_LEN`, integers LE):
///
/// ```text
/// [corr_id:u64][requester:u32][size:u32]
/// ```
///
/// `requester` is the holder identity stamped into the granted lease's
/// `issued_to`; it MUST be non-zero (see [`ISSUED_TO_NONE`]).
pub const MSG_TS_LEASE_REQUEST: u8 = 0xE3;

/// Allocator → coordinator: a granted interval.
///
/// Payload (`LEASE_GRANT_WIRE_LEN`, integers LE):
///
/// ```text
/// [corr_id:u64][encoded TimestampLease: LEASE_WIRE_LEN bytes]
/// ```
pub const MSG_TS_LEASE_GRANT: u8 = 0xE4;

/// Allocator → coordinator: typed refusal. Every rejection is explicit;
/// the allocator never answers a request by issuing something weaker
/// (§21 invariant 14).
///
/// Payload (`LEASE_REJECT_WIRE_LEN`, integers LE):
///
/// ```text
/// [corr_id:u64][reason:u8]
/// ```
pub const MSG_TS_LEASE_REJECT: u8 = 0xE6;

/// The replicated allocator record itself, in both directions:
/// allocator → consensus as a *proposal* to advance the high water, and
/// consensus → allocator as the *committed* confirmation (including the
/// whole recovered history on restart).
///
/// Payload: one encoded [`TimestampLease`] (`LEASE_WIRE_LEN` bytes).
///
/// The proposal and the confirmation are byte-identical by design: the
/// durable record IS the statement "timestamps below `interval_end` are
/// spoken for by allocator `allocator_id` at epoch `epoch`", and
/// recovery reconstructs the allocator's position from nothing but these
/// records — never from local state.
pub const MSG_TS_LEASE: u8 = 0xE7;

/// Substrate `durability` → allocator: WAL replay is complete.
///
/// DEFINED BY CLUSTOR (`clustor/modules/common/wire.rs::
/// MSG_WAL_REPLAY_COMPLETE`, payload `[term:u64 LE][high_water_index:u64
/// LE]`); mirrored here verbatim like the snapshot opcodes in
/// `modules/common/wire.rs`, and must not be renumbered independently.
///
/// The allocator uses it for one thing only: the empty-log bootstrap.
/// A fresh cluster has no committed allocator record, so nothing can
/// ever establish the allocator and it would refuse forever — correctly,
/// because "I have seen no records" and "records exist that I have not
/// replayed yet" are indistinguishable from inside the module. This
/// signal is the substrate distinguishing them. See
/// [`ReserveState::establish_empty_log`] for the ordering precondition
/// it carries.
pub const MSG_WAL_REPLAY_COMPLETE: u8 = 0x2D;

/// The allocator is not yet established: no committed record has been
/// observed, so it cannot know what a previous incarnation issued.
pub const REJECT_NOT_READY: u8 = 1;
/// The hold queue is full; the caller must retry.
pub const REJECT_QUEUE_FULL: u8 = 2;
/// Malformed request: zero size, size above [`MAX_LEASE_INTERVAL`], or a
/// zero (`ISSUED_TO_NONE`) requester.
pub const REJECT_BAD_REQUEST: u8 = 3;
/// The logical timestamp space is exhausted — the requested interval
/// would overflow `u64`. Fail closed; there is no wraparound.
pub const REJECT_EXHAUSTED: u8 = 4;

/// Wire length of `MSG_TS_LEASE_REQUEST`.
pub const LEASE_REQUEST_WIRE_LEN: usize = 16;
/// Wire length of `MSG_TS_LEASE_GRANT`.
pub const LEASE_GRANT_WIRE_LEN: usize = 8 + LEASE_WIRE_LEN;
/// Wire length of `MSG_TS_LEASE_REJECT`.
pub const LEASE_REJECT_WIRE_LEN: usize = 9;
/// Wire length of one encoded [`TimestampLease`].
pub const LEASE_WIRE_LEN: usize = 40;

// ── Timestamp lease record ────────────────────────────────────────────

/// One replicated timestamp-allocator record (§10).
///
/// Wire layout (`MVCC_LEASE_VERSION` 1, all integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]      // 0..2, 2..4   payload_len = 36
/// [allocator_id:u32]                  // 4..8
/// [epoch:u32]                         // 8..12
/// [interval_start:u64]                // 12..20
/// [interval_end:u64]                  // 20..28   exclusive
/// [issued_to:u32]                     // 28..32
/// [lease_expiry_unix_ms:u64]          // 32..40
/// ```
///
/// Total `LEASE_WIRE_LEN` = 40 bytes, fixed. Decode fails closed on an
/// unknown version, a declared length that disagrees with the fixed
/// payload size, and any length other than exactly 40.
///
/// Semantics of each field:
///
/// - `allocator_id`: which allocator instance spoke. Identity, not
///   placement — see rule 3 in the module docs.
/// - `epoch`: the allocator's incarnation. Monotone across restarts and
///   leadership changes; strictly bumped when identity changes.
/// - `interval_start` / `interval_end`: half-open `[start, end)` over
///   logical MVCC timestamps. `end` is the exclusive upper bound and
///   doubles as the high water this record establishes.
/// - `issued_to`: holder, or [`ISSUED_TO_NONE`] for a bare reservation.
/// - `lease_expiry_unix_ms`: administrative only — see
///   [`HYBRID_NOT_ENABLED`]. Never consulted for ordering.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimestampLease {
    pub allocator_id: u32,
    pub epoch: u32,
    pub interval_start: u64,
    pub interval_end: u64,
    pub issued_to: u32,
    pub lease_expiry_unix_ms: u64,
}

impl TimestampLease {
    /// All-zero record. Not a valid lease (its interval is empty); used
    /// as the initial value of fixed-size state fields.
    pub const EMPTY: TimestampLease = TimestampLease {
        allocator_id: 0,
        epoch: 0,
        interval_start: 0,
        interval_end: 0,
        issued_to: 0,
        lease_expiry_unix_ms: 0,
    };

    /// Number of timestamps this lease covers. Zero for a degenerate
    /// (invalid) record; saturating rather than wrapping so a corrupt
    /// inverted interval reports nothing rather than a huge count.
    pub fn size(&self) -> u64 {
        self.interval_end.saturating_sub(self.interval_start)
    }

    /// The high water this record establishes: the first timestamp NOT
    /// covered by it.
    pub fn high_water(&self) -> u64 {
        self.interval_end
    }

    /// A record is well-formed when its interval is non-empty and within
    /// the size bound. Checked on decode's callers rather than in
    /// `decode` itself so a decoder can still read (and report) a
    /// structurally intact but semantically invalid record.
    pub fn is_well_formed(&self) -> bool {
        self.interval_start < self.interval_end && self.size() <= MAX_LEASE_INTERVAL
    }

    /// Serialize to the versioned wire form. Returns the encoded length,
    /// or `None` if `out` is too small (fail closed — never truncate a
    /// record).
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < LEASE_WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&MVCC_LEASE_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((LEASE_WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.allocator_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.epoch.to_le_bytes());
        out[12..20].copy_from_slice(&self.interval_start.to_le_bytes());
        out[20..28].copy_from_slice(&self.interval_end.to_le_bytes());
        out[28..32].copy_from_slice(&self.issued_to.to_le_bytes());
        out[32..40].copy_from_slice(&self.lease_expiry_unix_ms.to_le_bytes());
        Some(LEASE_WIRE_LEN)
    }

    /// Parse the versioned wire form. `src` must be exactly one encoded
    /// record. Fails closed on a length other than `LEASE_WIRE_LEN`
    /// (truncation at ANY byte, or trailing bytes), an unknown version,
    /// or a declared payload length that disagrees with the fixed
    /// layout.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != LEASE_WIRE_LEN {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != MVCC_LEASE_VERSION {
            return None;
        }
        let payload_len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if payload_len != LEASE_WIRE_LEN - 4 {
            return None;
        }
        Some(Self {
            allocator_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            epoch: u32::from_le_bytes(src[8..12].try_into().ok()?),
            interval_start: u64::from_le_bytes(src[12..20].try_into().ok()?),
            interval_end: u64::from_le_bytes(src[20..28].try_into().ok()?),
            issued_to: u32::from_le_bytes(src[28..32].try_into().ok()?),
            lease_expiry_unix_ms: u64::from_le_bytes(src[32..40].try_into().ok()?),
        })
    }
}

// ── Request / grant / reject frames ───────────────────────────────────

/// One decoded `MSG_TS_LEASE_REQUEST`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseRequest {
    pub corr_id: u64,
    pub requester: u32,
    pub size: u32,
}

impl LeaseRequest {
    /// Serialize `[corr_id:u64][requester:u32][size:u32]`, all LE.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < LEASE_REQUEST_WIRE_LEN {
            return None;
        }
        out[0..8].copy_from_slice(&self.corr_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.requester.to_le_bytes());
        out[12..16].copy_from_slice(&self.size.to_le_bytes());
        Some(LEASE_REQUEST_WIRE_LEN)
    }

    /// Parse exactly one request frame. Fails closed on any other
    /// length; semantic validation (non-zero size and requester) is the
    /// allocator's, and produces `REJECT_BAD_REQUEST`.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != LEASE_REQUEST_WIRE_LEN {
            return None;
        }
        Some(Self {
            corr_id: u64::from_le_bytes(src[0..8].try_into().ok()?),
            requester: u32::from_le_bytes(src[8..12].try_into().ok()?),
            size: u32::from_le_bytes(src[12..16].try_into().ok()?),
        })
    }
}

/// Serialize a grant frame: `[corr_id:u64][lease:40]`.
pub fn encode_grant(corr_id: u64, lease: &TimestampLease, out: &mut [u8]) -> Option<usize> {
    if out.len() < LEASE_GRANT_WIRE_LEN {
        return None;
    }
    out[0..8].copy_from_slice(&corr_id.to_le_bytes());
    lease.encode(&mut out[8..LEASE_GRANT_WIRE_LEN])?;
    Some(LEASE_GRANT_WIRE_LEN)
}

/// Parse exactly one grant frame. Fails closed on any other length or a
/// lease body that does not decode.
pub fn decode_grant(src: &[u8]) -> Option<(u64, TimestampLease)> {
    if src.len() != LEASE_GRANT_WIRE_LEN {
        return None;
    }
    let corr_id = u64::from_le_bytes(src[0..8].try_into().ok()?);
    Some((corr_id, TimestampLease::decode(&src[8..])?))
}

/// Serialize a reject frame: `[corr_id:u64][reason:u8]`.
pub fn encode_reject(corr_id: u64, reason: u8, out: &mut [u8]) -> Option<usize> {
    if out.len() < LEASE_REJECT_WIRE_LEN {
        return None;
    }
    out[0..8].copy_from_slice(&corr_id.to_le_bytes());
    out[8] = reason;
    Some(LEASE_REJECT_WIRE_LEN)
}

/// Parse exactly one reject frame.
pub fn decode_reject(src: &[u8]) -> Option<(u64, u8)> {
    if src.len() != LEASE_REJECT_WIRE_LEN {
        return None;
    }
    Some((u64::from_le_bytes(src[0..8].try_into().ok()?), src[8]))
}

// ── Typed errors ──────────────────────────────────────────────────────

/// A §10 monotonicity violation found by [`validate_successor`]. Each
/// variant names exactly one rule; a caller that sees any of them must
/// fail closed — there is no repair, because the committed record is the
/// only authority for what was already issued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseError {
    /// The predecessor record is degenerate: `start >= end`.
    PrevEmptyInterval,
    /// The successor record is degenerate: `start >= end`. This is also
    /// how a `u64`-boundary overflow surfaces if a caller builds an
    /// interval by wrapping arithmetic instead of [`next_interval`].
    EmptyInterval,
    /// The successor's interval exceeds [`MAX_LEASE_INTERVAL`].
    IntervalTooLarge { size: u64 },
    /// `next.interval_start < prev.interval_end`: the successor would
    /// re-issue timestamps the predecessor already covered. This is the
    /// violation §10's restart/leadership rule exists to prevent.
    IntervalRegression { prev_end: u64, next_start: u64 },
    /// `next.epoch < prev.epoch`: incarnations moved backwards.
    EpochRegression { prev_epoch: u32, next_epoch: u32 },
    /// The allocator identity changed without a strict epoch bump, so
    /// two allocators claim authority at the same epoch.
    EpochNotBumped {
        prev_allocator: u32,
        next_allocator: u32,
        epoch: u32,
    },
    /// The logical timestamp or epoch space is exhausted. Fail closed;
    /// there is no wraparound.
    Overflow,
}

/// Why an issuance or advance could not proceed. Distinct from
/// [`LeaseError`]: these are *liveness* answers about the allocator's
/// current state, not statements that an invariant was broken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IssueError {
    /// No committed record has been observed yet, so the allocator does
    /// not know what previous incarnations issued. Fail closed.
    NotReady,
    /// Zero size, a size above [`MAX_LEASE_INTERVAL`], or a zero
    /// ([`ISSUED_TO_NONE`]) requester identity.
    BadRequest,
    /// The committed reserve does not cover this request: the caller
    /// must propose a high-water advance and wait for it to commit.
    /// **Never** a licence to issue from an uncommitted advance.
    Insufficient { free: u64, wanted: u64 },
    /// An advance is already outstanding; one at a time.
    ProposalOutstanding,
    /// `u64` timestamp space exhausted.
    Overflow,
}

impl IssueError {
    /// The `MSG_TS_LEASE_REJECT` reason byte a caller reports for this
    /// condition. `Insufficient` and `ProposalOutstanding` have no
    /// mapping — they mean "hold the request", not "refuse it".
    pub const fn reject_reason(&self) -> Option<u8> {
        match self {
            IssueError::NotReady => Some(REJECT_NOT_READY),
            IssueError::BadRequest => Some(REJECT_BAD_REQUEST),
            IssueError::Overflow => Some(REJECT_EXHAUSTED),
            IssueError::Insufficient { .. } | IssueError::ProposalOutstanding => None,
        }
    }
}

// ── Monotonicity rules ────────────────────────────────────────────────

/// The next half-open interval above `prev_high_water`, or `None` if it
/// cannot be formed. Fails closed on a zero size, a size above
/// [`MAX_LEASE_INTERVAL`], and on `u64` overflow — the logical timestamp
/// space ends, it does not wrap.
///
/// Note the returned interval starts *at* the previous high water: the
/// high water is the first timestamp not yet spoken for, so `[hw, hw+n)`
/// is contiguous with, and disjoint from, everything below it.
pub fn next_interval(prev_high_water: u64, size: u64) -> Option<(u64, u64)> {
    if size == 0 || size > MAX_LEASE_INTERVAL {
        return None;
    }
    let end = prev_high_water.checked_add(size)?;
    Some((prev_high_water, end))
}

/// Does `lease` cover `ts`? Half-open: `start <= ts < end`.
pub fn lease_covers(lease: &TimestampLease, ts: u64) -> bool {
    lease.interval_start <= ts && ts < lease.interval_end
}

/// Do two leases overlap? Used by the property tests to assert the
/// global no-overlap property directly, independently of the succession
/// rule that is supposed to guarantee it.
pub fn leases_overlap(a: &TimestampLease, b: &TimestampLease) -> bool {
    a.interval_start < b.interval_end && b.interval_start < a.interval_end
}

/// Enforce §10's core invariant on a `prev → next` succession.
///
/// `prev` is the newest previously issued record — on restart, the
/// highest-`interval_end` record recovered from the committed log, NOT
/// anything remembered locally. `next` is the record about to be
/// proposed or the one just observed as committed.
///
/// Rules, checked in this order (first failure wins, so the error names
/// the most fundamental problem):
///
/// 1. both records have non-empty intervals within the size bound;
/// 2. `next.interval_start >= prev.interval_end` — move beyond EVERY
///    previously issued interval, never into one;
/// 3. `next.epoch >= prev.epoch` — incarnations never regress;
/// 4. if `prev.allocator_id != next.allocator_id`, then strictly
///    `next.epoch > prev.epoch` — a leadership change is always visible
///    in the record as a new epoch.
///
/// Note rules 2 and 3 are independent: a same-epoch successor from the
/// same allocator is the normal dense case, and a strictly higher epoch
/// with a contiguous interval is the normal restart case. Neither
/// implies the other, and neither alone is sufficient.
pub fn validate_successor(prev: &TimestampLease, next: &TimestampLease) -> Result<(), LeaseError> {
    if prev.interval_start >= prev.interval_end {
        return Err(LeaseError::PrevEmptyInterval);
    }
    if next.interval_start >= next.interval_end {
        return Err(LeaseError::EmptyInterval);
    }
    if next.size() > MAX_LEASE_INTERVAL {
        return Err(LeaseError::IntervalTooLarge { size: next.size() });
    }
    if next.interval_start < prev.interval_end {
        return Err(LeaseError::IntervalRegression {
            prev_end: prev.interval_end,
            next_start: next.interval_start,
        });
    }
    if next.epoch < prev.epoch {
        return Err(LeaseError::EpochRegression {
            prev_epoch: prev.epoch,
            next_epoch: next.epoch,
        });
    }
    if prev.allocator_id != next.allocator_id && next.epoch == prev.epoch {
        return Err(LeaseError::EpochNotBumped {
            prev_allocator: prev.allocator_id,
            next_allocator: next.allocator_id,
            epoch: prev.epoch,
        });
    }
    Ok(())
}

/// Build the successor of `prev` for `allocator_id`, applying the §10
/// rules rather than merely checking them: the interval starts at
/// `prev`'s high water, and the epoch is bumped strictly when the
/// allocator identity differs.
///
/// The result is guaranteed to satisfy [`validate_successor`] against
/// `prev`; the tests assert that rather than trusting it.
pub fn next_lease(
    prev: &TimestampLease,
    allocator_id: u32,
    size: u64,
    issued_to: u32,
    lease_expiry_unix_ms: u64,
) -> Result<TimestampLease, LeaseError> {
    if prev.interval_start >= prev.interval_end {
        return Err(LeaseError::PrevEmptyInterval);
    }
    let (start, end) = next_interval(prev.interval_end, size).ok_or(LeaseError::Overflow)?;
    let epoch = if allocator_id == prev.allocator_id {
        prev.epoch
    } else {
        prev.epoch.checked_add(1).ok_or(LeaseError::Overflow)?
    };
    Ok(TimestampLease {
        allocator_id,
        epoch,
        interval_start: start,
        interval_end: end,
        issued_to,
        lease_expiry_unix_ms,
    })
}

// ── Reserve state machine (durability before issuance) ────────────────

/// Allocator readiness. Zero-valued `Unestablished` so a zeroed state
/// buffer (how PIC module state arrives) decodes as "knows nothing",
/// which is the fail-closed position.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservePhase {
    /// No committed record observed. Cannot issue anything: a previous
    /// incarnation may have issued arbitrarily far ahead.
    Unestablished = 0,
    /// Established, no advance outstanding.
    Ready = 1,
    /// An advance has been proposed and not yet observed as committed.
    /// Its range contributes NOTHING to what may be issued.
    AwaitingCommit = 2,
}

/// The durability-before-issuance state machine (§10, §21 invariant 3).
///
/// Two water marks, and the whole safety argument is the gap between
/// them:
///
/// - `committed_high_water` — the highest `interval_end` observed in a
///   **committed** record. Everything below it is durably spoken for.
/// - `issued_high_water` — how far the allocator has actually handed out
///   from within that committed reserve. Always
///   `issued_high_water <= committed_high_water`.
///
/// [`try_issue`](Self::try_issue) cuts only from
/// `[issued_high_water, committed_high_water)`. A proposal in flight is
/// recorded as `pending_end` and is deliberately *not* consulted by
/// `try_issue`: an uncommitted advance can be lost by a leadership
/// change, and a timestamp issued from it would then be re-issued by the
/// next allocator.
///
/// Recovery is the same mechanism, not a special case. A fresh allocator
/// starts `Unestablished` and refuses everything. Each committed record
/// it replays advances `committed_high_water`; because those records
/// belong to a *previous* incarnation, `issued_high_water` is snapped up
/// to `committed_high_water` — the allocator inherits the high water but
/// none of the reserve, so its first grant necessarily waits on a fresh,
/// self-proposed, committed advance carrying its own strictly higher
/// epoch. That is §10's "move beyond every previously issued interval"
/// expressed as arithmetic rather than as a check.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ReserveState {
    /// This allocator's identity (from module params).
    pub allocator_id: u32,
    /// This allocator's incarnation. Always strictly above every epoch
    /// seen in a foreign committed record.
    pub epoch: u32,
    /// Durably committed high water. Only advanced by committed records.
    pub committed_high_water: u64,
    /// How far into the committed reserve has been handed out.
    pub issued_high_water: u64,
    /// `interval_end` of the outstanding proposal, or 0 when none.
    pub pending_end: u64,
    /// Newest record observed, for successor validation.
    pub prev: TimestampLease,
    pub phase: ReservePhase,
    pub has_prev: bool,
}

impl ReserveState {
    /// A fresh, knowing-nothing allocator.
    pub const fn new(allocator_id: u32) -> Self {
        Self {
            allocator_id,
            epoch: 0,
            committed_high_water: 0,
            issued_high_water: 0,
            pending_end: 0,
            prev: TimestampLease::EMPTY,
            phase: ReservePhase::Unestablished,
            has_prev: false,
        }
    }

    /// Reset to the fresh state (PIC modules initialise in place).
    pub fn init(&mut self, allocator_id: u32) {
        *self = Self::new(allocator_id);
    }

    /// Timestamps available for immediate issuance from the committed
    /// reserve. Zero while `Unestablished`.
    pub fn free(&self) -> u64 {
        self.committed_high_water
            .saturating_sub(self.issued_high_water)
    }

    /// Fold one **committed** record into the state. This is the ONLY
    /// path that advances `committed_high_water`, and therefore the only
    /// path that makes anything issuable.
    ///
    /// Two cases:
    ///
    /// - **Ours** (`allocator_id` and `epoch` both match, and we are
    ///   established): the confirmation of an advance we proposed. The
    ///   reserve grows; `issued_high_water` is untouched, so the newly
    ///   committed span becomes issuable.
    /// - **Anyone else's** (including every record replayed on restart):
    ///   a foreign or previous authority spoke. The high water is
    ///   adopted, `issued_high_water` is snapped up to it — we inherit
    ///   NO reserve from another incarnation — our epoch is bumped
    ///   strictly past theirs, and any proposal of ours in flight is
    ///   abandoned (it was made under a now-superseded epoch; if it
    ///   commits later it arrives here as another record and is folded
    ///   in the same way).
    ///
    /// Fails closed with a [`LeaseError`] if the record breaks §10
    /// succession against the newest record already observed. A caller
    /// that gets an error must drop the record and count it — it must
    /// NOT fall back to local state.
    pub fn observe_committed(&mut self, rec: &TimestampLease) -> Result<(), LeaseError> {
        if rec.interval_start >= rec.interval_end {
            return Err(LeaseError::EmptyInterval);
        }
        if rec.size() > MAX_LEASE_INTERVAL {
            return Err(LeaseError::IntervalTooLarge { size: rec.size() });
        }
        if self.has_prev {
            validate_successor(&self.prev, rec)?;
        }

        let ours = self.phase != ReservePhase::Unestablished
            && rec.allocator_id == self.allocator_id
            && rec.epoch == self.epoch;

        if rec.interval_end > self.committed_high_water {
            self.committed_high_water = rec.interval_end;
        }
        if ours {
            // Our own advance is now durable. If it covers the proposal
            // we were waiting on, we are free to propose again.
            if self.phase == ReservePhase::AwaitingCommit && rec.interval_end >= self.pending_end {
                self.pending_end = 0;
                self.phase = ReservePhase::Ready;
            }
        } else {
            // Someone else's interval. Inherit the high water, never the
            // reserve, and move our epoch strictly past theirs.
            self.issued_high_water = self.committed_high_water;
            let bumped = rec.epoch.checked_add(1).ok_or(LeaseError::Overflow)?;
            if bumped > self.epoch {
                self.epoch = bumped;
            }
            self.pending_end = 0;
            self.phase = ReservePhase::Ready;
        }
        self.prev = *rec;
        self.has_prev = true;
        Ok(())
    }

    /// Establish an allocator over a committed log that is **proven to
    /// contain no allocator records**, at high water 0.
    ///
    /// This is the empty-log bootstrap and the ONLY way an allocator
    /// becomes issuable without a committed record. It carries a
    /// precondition the caller must actually hold:
    ///
    /// > every committed allocator record that exists has already been
    /// > delivered to [`observe_committed`](Self::observe_committed).
    ///
    /// In the graph that is `MSG_WAL_REPLAY_COMPLETE` arriving on the
    /// same `committed_state` port as the replayed records, after them —
    /// which is exactly what the substrate emits it for. Calling this on
    /// a log whose records have not all been seen re-issues timestamps
    /// and is the corruption this whole file exists to prevent.
    ///
    /// No-op once established: a live allocator's position is already
    /// authoritative, and the signal may legitimately arrive after
    /// records have set it.
    pub fn establish_empty_log(&mut self) {
        if self.phase == ReservePhase::Unestablished {
            // Nothing was ever issued, so high water 0 with no reserve
            // is the truthful position. Epoch stays 0: there is no
            // previous incarnation to move past.
            self.phase = ReservePhase::Ready;
            self.issued_high_water = self.committed_high_water;
            self.pending_end = 0;
        }
    }

    /// Cut an interval out of the **already committed** reserve.
    ///
    /// Returns `Insufficient` when the reserve does not cover the
    /// request — that is a signal to propose an advance and hold the
    /// request, never to issue anyway.
    pub fn try_issue(
        &mut self,
        size: u64,
        issued_to: u32,
        lease_expiry_unix_ms: u64,
    ) -> Result<TimestampLease, IssueError> {
        if self.phase == ReservePhase::Unestablished {
            return Err(IssueError::NotReady);
        }
        if size == 0 || size > MAX_LEASE_INTERVAL || issued_to == ISSUED_TO_NONE {
            return Err(IssueError::BadRequest);
        }
        let end = self
            .issued_high_water
            .checked_add(size)
            .ok_or(IssueError::Overflow)?;
        if end > self.committed_high_water {
            return Err(IssueError::Insufficient {
                free: self.free(),
                wanted: size,
            });
        }
        let lease = TimestampLease {
            allocator_id: self.allocator_id,
            epoch: self.epoch,
            interval_start: self.issued_high_water,
            interval_end: end,
            issued_to,
            lease_expiry_unix_ms,
        };
        self.issued_high_water = end;
        Ok(lease)
    }

    /// Build the record to propose so that a later `try_issue(min_size)`
    /// can succeed. The advance runs from the current committed high
    /// water up by at least `chunk`, and by more if `min_size` needs it.
    ///
    /// Marks the state `AwaitingCommit`; the returned record is NOT
    /// applied to the reserve. Only [`observe_committed`](Self::observe_committed)
    /// does that, and only when consensus hands the record back.
    pub fn plan_advance(
        &mut self,
        min_size: u64,
        chunk: u64,
    ) -> Result<TimestampLease, IssueError> {
        if self.phase == ReservePhase::Unestablished {
            return Err(IssueError::NotReady);
        }
        if self.phase == ReservePhase::AwaitingCommit {
            return Err(IssueError::ProposalOutstanding);
        }
        if min_size == 0 || min_size > MAX_LEASE_INTERVAL {
            return Err(IssueError::BadRequest);
        }
        // Enough to cover the deficit the caller could not serve, and at
        // least `chunk` so advances amortize across many requests.
        let need_end = self
            .issued_high_water
            .checked_add(min_size)
            .ok_or(IssueError::Overflow)?;
        let deficit = need_end.saturating_sub(self.committed_high_water);
        let mut span = if deficit > chunk { deficit } else { chunk };
        if span > MAX_LEASE_INTERVAL {
            span = MAX_LEASE_INTERVAL;
        }
        if span < deficit {
            // The deficit itself exceeds the per-record bound; the
            // caller asked for more than one advance can cover.
            return Err(IssueError::BadRequest);
        }
        let (start, end) =
            next_interval(self.committed_high_water, span).ok_or(IssueError::Overflow)?;
        self.pending_end = end;
        self.phase = ReservePhase::AwaitingCommit;
        Ok(TimestampLease {
            allocator_id: self.allocator_id,
            epoch: self.epoch,
            interval_start: start,
            interval_end: end,
            issued_to: ISSUED_TO_NONE,
            lease_expiry_unix_ms: 0,
        })
    }

    /// Build an ESTABLISHMENT probe record to propose while
    /// `Unestablished` — the bootstrap for compositions where no
    /// ordered replay-complete signal reaches the allocator's
    /// `committed_state` port.
    ///
    /// Safety does not rest on the caller knowing whether the log is
    /// empty. The probe is proposed through consensus like any record,
    /// so it lands AFTER every record already in the log, and
    /// [`observe_committed`](Self::observe_committed) folds records in
    /// log order:
    ///
    /// - **Log empty**: the probe is the first record. Folding it
    ///   establishes the allocator (foreign branch — the probe's epoch
    ///   is not this incarnation's post-fold epoch), adopting its high
    ///   water with no reserve. Correct: nothing was ever issued.
    /// - **Log non-empty**: the earlier records fold first and
    ///   establish the allocator; when the probe arrives its interval
    ///   `[seen_high_water, …)` built below is either a valid successor
    ///   (harmless extra advance) or — if it raced a concurrent record
    ///   — an interval regression that `validate_successor` refuses,
    ///   so every replica DROPS it identically. Either way nothing is
    ///   ever re-issued.
    ///
    /// The caller retries on a coarse cadence while `Unestablished`;
    /// duplicate probes are refused by succession the same way.
    ///
    /// Shared precondition with [`establish_empty_log`]
    /// (Self::establish_empty_log): the committed log must still
    /// CONTAIN every allocator record ever committed. WAL compaction
    /// that discards lease records without carrying the high water in
    /// the snapshot would let a probe re-issue timestamps — the same
    /// hazard the replay-complete path has, gated today by the pinned
    /// compaction floor (§18).
    pub fn plan_bootstrap(&mut self, chunk: u64) -> Result<TimestampLease, IssueError> {
        if self.phase != ReservePhase::Unestablished {
            return Err(IssueError::BadRequest);
        }
        if chunk == 0 || chunk > MAX_LEASE_INTERVAL {
            return Err(IssueError::BadRequest);
        }
        // Strictly above anything observed so far; epoch strictly above
        // every observed epoch. `epoch + 1` because a fresh allocator's
        // epoch is 0 and a probe at epoch 0 could tie a genuine epoch-0
        // predecessor's records instead of superseding them.
        let (start, end) =
            next_interval(self.committed_high_water, chunk).ok_or(IssueError::Overflow)?;
        let epoch = self.epoch.checked_add(1).ok_or(IssueError::Overflow)?;
        Ok(TimestampLease {
            allocator_id: self.allocator_id,
            epoch,
            interval_start: start,
            interval_end: end,
            issued_to: ISSUED_TO_NONE,
            lease_expiry_unix_ms: 0,
        })
    }
}
