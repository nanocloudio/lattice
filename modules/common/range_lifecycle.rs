//! Range-lifecycle contracts — RFC database foundation §12.
//!
//! Phase 4 slice 1: the versioned lifecycle-operation record and the
//! three executable state machines (split, merge, relocation). This
//! file is contracts only — no ports, no I/O, no module. The range
//! supervisor is a later slice; it will be a thin pump over the
//! functions defined here, in the same relationship the transaction
//! coordinator has to `txn.rs` and the timestamp allocator has to
//! `mvcc.rs`.
//!
//! §12's opening two sentences are the whole reason this file exists:
//!
//! > Every lifecycle operation is an explicit state machine with
//! > generation fencing. **Routing metadata is published only after the
//! > new ownership state is durable.**
//!
//! and, from §12.1:
//!
//! > **No phase may expose overlapping writable ownership.** A failed
//! > split is resumable by operation ID or is rolled back before
//! > metadata publication.
//!
//! Those become [`writable_ranges_at`] / [`apply_to_map`] and
//! [`rollback_target`] / [`is_resumable`] respectively.
//!
//! ## The publication point is the hinge of the whole design
//!
//! Each machine has exactly one phase at which replacement descriptors
//! enter the routing map ([`SplitPhase::DescriptorsPublished`],
//! [`MergePhase::DescriptorsReplaced`],
//! [`RelocatePhase::DescriptorPublished`]). Before it, the routing map
//! is **byte-for-byte the pre-operation map** — a partially applied map
//! is never published, so §21 invariants 4 and 5 hold trivially at
//! every intermediate phase and an abandoned operation needs no map
//! repair. After it, the operation is **forward-only**: descriptors
//! have been observed, caches hold them, and unwinding would resurrect
//! ownership a client already stopped believing in.
//! [`rollback_target`] returns `None` exactly on the published side of
//! that line.
//!
//! This is why [`apply_to_map`] takes a phase at all: it is not "apply
//! the diff for this step", it is "what does the published routing map
//! look like while the operation sits at this phase", and the answer is
//! *unchanged* for every pre-publication phase.
//!
//! ## Writable ownership is a separate question from routing
//!
//! Routing metadata lags the truth on purpose. The authoritative flip
//! of writable ownership happens at the *consensus* step, one or more
//! phases before publication:
//!
//! - split: at [`SplitPhase::Finalized`] — §12.1 step 7's `SplitFinalize`
//!   is what gives "each child one authoritative bound and generation";
//! - merge: at [`MergePhase::Quiesced`] the source stops accepting
//!   writes, and at [`MergePhase::TargetOwns`] the target owns the
//!   combined interval (§12.2 steps 2 and 5);
//! - relocation: never — §12.3 changes replicas, not key bounds, so the
//!   writable set is the same single range at every phase.
//!
//! [`writable_ranges_at`] reports that separately from [`apply_to_map`],
//! and the split machine's answer is *always exactly one* of
//! {parent} or {children} — [`WritableOwner::SourceAndTargets`] is
//! unreachable for [`LifecycleKind::Split`] at every phase. That is the
//! executable form of "no phase may expose overlapping writable
//! ownership".
//!
//! ## Conflating the three operations is the named hazard
//!
//! §12.3 closes with "split, merge, and relocation must not be
//! conflated. Each has different rollback, availability, and stale-
//! message hazards." So the record's shape is per-kind and validated
//! closed: a split carries a split key and two fresh child identities,
//! a merge carries one surviving target, and a relocation carries the
//! *same* range id with a new physical binding. Any other combination
//! fails to construct and fails to decode.
//!
//! Golden vectors: `tests/contract_range_lifecycle.rs`. The
//! [`LifecycleOperation`] is a replicated, persistent record: if a
//! change here makes a vector fail, that change is a format break
//! requiring a [`LIFECYCLE_OP_VERSION`] bump plus an explicit
//! migration — never an update to the vector.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

// `pub`: consumers of this contract (the range supervisor) need the
// map/barrier frame surface through the SAME mount chain — a second
// `#[path]` mount would produce a non-unifying duplicate type tree.
#[path = "partition_map.rs"]
pub mod partition_map;

#[allow(
    unused_imports,
    reason = "re-export surface: consumers of this contract file need the partition-map types it composes with without a second #[path] include"
)]
pub use partition_map::{
    Binding, Lifecycle, MapViolation, OrderedRangeMap, RangeDescriptor, Replica, ReplicaRole,
    RetryClass, RoutingKind, MAX_KEY_BOUND_LEN, MAX_RANGES, MAX_REPLICAS,
};

/// Version of the [`LifecycleOperation`] wire encoding. Bump on ANY
/// layout change; decoders fail closed on an unknown version.
pub const LIFECYCLE_OP_VERSION: u16 = 1;

/// Maximum source or target ranges named by one lifecycle operation.
///
/// Two: a split names two children, and no operation defined by §12
/// names more. This is a *format* bound — widening it (an N-way merge,
/// say) is a version bump, not a policy change.
pub const MAX_LIFECYCLE_RANGES: usize = 2;

/// Maximum split-key length, bytes. Matches
/// [`partition_map::MAX_KEY_BOUND_LEN`] because the split key becomes a
/// range bound: a key that cannot be a bound cannot be a split point.
pub const MAX_SPLIT_KEY_LEN: usize = MAX_KEY_BOUND_LEN;

/// Wire length of one source entry: `[range_id:16]`.
pub const SOURCE_WIRE_LEN: usize = 16;
/// Wire length of one target entry:
/// `[range_id:16][partition_id:u16][partition_incarnation:u32]`.
pub const TARGET_WIRE_LEN: usize = 22;
/// Fixed part of an encoded [`LifecycleOperation`]: header plus every
/// field except the variable source array, target array, and split key.
pub const LIFECYCLE_OP_FIXED_WIRE_LEN: usize = 58;
/// Largest possible encoded [`LifecycleOperation`].
pub const LIFECYCLE_OP_MAX_WIRE_LEN: usize = LIFECYCLE_OP_FIXED_WIRE_LEN
    + MAX_LIFECYCLE_RANGES * SOURCE_WIRE_LEN
    + MAX_LIFECYCLE_RANGES * TARGET_WIRE_LEN
    + MAX_SPLIT_KEY_LEN;

// ── Kinds ─────────────────────────────────────────────────────────────

/// Which §12 operation this record describes.
///
/// Discriminants start at 1 so a zeroed buffer never decodes as a valid
/// kind — a zeroed record is not "a split", it is garbage.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LifecycleKind {
    /// §12.1 — one parent becomes two children at a split key.
    Split = 1,
    /// §12.2 — two adjacent ranges become one surviving target.
    Merge = 2,
    /// §12.3 — one range changes replicas, never key bounds.
    Relocate = 3,
}

impl LifecycleKind {
    pub const ALL: [Self; 3] = [Self::Split, Self::Merge, Self::Relocate];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Split),
            2 => Some(Self::Merge),
            3 => Some(Self::Relocate),
            _ => None,
        }
    }
}

// ── Split phases and events (§12.1) ───────────────────────────────────

/// Split phase lattice, mirroring §12.1's numbered steps.
///
/// Step 1 (validate policy, key choice, capacity, placement; allocate
/// fresh identities) is what *constructs* the record, so
/// [`SplitPhase::Validated`] is the initial phase rather than an
/// event-driven one — an operation that has not been validated has no
/// record.
///
/// ```text
/// Validated              1. policy/key/capacity/placement validated, identities allocated
/// Prepared               2. SplitPrepare committed in the parent
/// BoundaryEstablished    3. exact parent apply index + engine snapshot boundary fixed
/// ChildrenBootstrapped   4-5. child snapshots built, installed, mutation tail caught up
/// CutoverBarrier         6. committed barrier; no new ambiguous writes
/// Finalized              7. SplitFinalize committed — OWNERSHIP FLIPS HERE
/// DescriptorsPublished   8-9. replacement descriptors published — PUBLICATION POINT
/// TombstoneRetained      10. safety floors satisfied; terminal
/// ```
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SplitPhase {
    Validated = 1,
    Prepared = 2,
    BoundaryEstablished = 3,
    ChildrenBootstrapped = 4,
    CutoverBarrier = 5,
    Finalized = 6,
    DescriptorsPublished = 7,
    TombstoneRetained = 8,
}

impl SplitPhase {
    pub const ALL: [Self; 8] = [
        Self::Validated,
        Self::Prepared,
        Self::BoundaryEstablished,
        Self::ChildrenBootstrapped,
        Self::CutoverBarrier,
        Self::Finalized,
        Self::DescriptorsPublished,
        Self::TombstoneRetained,
    ];
    /// The initial phase: a record exists only after §12.1 step 1.
    pub const INITIAL: Self = Self::Validated;
    /// The phase at which replacement descriptors enter the routing
    /// map (§12.1 step 8). At and after this phase the operation is
    /// forward-only.
    pub const PUBLICATION: Self = Self::DescriptorsPublished;

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Validated),
            2 => Some(Self::Prepared),
            3 => Some(Self::BoundaryEstablished),
            4 => Some(Self::ChildrenBootstrapped),
            5 => Some(Self::CutoverBarrier),
            6 => Some(Self::Finalized),
            7 => Some(Self::DescriptorsPublished),
            8 => Some(Self::TombstoneRetained),
            _ => None,
        }
    }

    /// Have replacement descriptors been published? Past this line the
    /// only legal direction is forward.
    pub const fn is_published(self) -> bool {
        (self as u8) >= (Self::PUBLICATION as u8)
    }

    /// Terminal: no event leaves this phase.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::TombstoneRetained)
    }

    /// The single event that moves this phase forward, or `None` if
    /// terminal.
    pub const fn forward_event(self) -> Option<SplitEvent> {
        match self {
            Self::Validated => Some(SplitEvent::PrepareCommitted),
            Self::Prepared => Some(SplitEvent::BoundaryEstablished),
            Self::BoundaryEstablished => Some(SplitEvent::ChildSnapshotsInstalled),
            Self::ChildrenBootstrapped => Some(SplitEvent::CutoverBarrierCommitted),
            Self::CutoverBarrier => Some(SplitEvent::FinalizeCommitted),
            Self::Finalized => Some(SplitEvent::DescriptorsPublished),
            Self::DescriptorsPublished => Some(SplitEvent::TombstoneFloorsSatisfied),
            Self::TombstoneRetained => None,
        }
    }

    /// The event that produced this phase, or `None` for the initial
    /// phase. Redelivering it is an idempotent no-op (§21 invariant 17:
    /// remote mutations stay idempotently identifiable).
    pub const fn producing_event(self) -> Option<SplitEvent> {
        match self {
            Self::Validated => None,
            Self::Prepared => Some(SplitEvent::PrepareCommitted),
            Self::BoundaryEstablished => Some(SplitEvent::BoundaryEstablished),
            Self::ChildrenBootstrapped => Some(SplitEvent::ChildSnapshotsInstalled),
            Self::CutoverBarrier => Some(SplitEvent::CutoverBarrierCommitted),
            Self::Finalized => Some(SplitEvent::FinalizeCommitted),
            Self::DescriptorsPublished => Some(SplitEvent::DescriptorsPublished),
            Self::TombstoneRetained => Some(SplitEvent::TombstoneFloorsSatisfied),
        }
    }
}

/// Split events — the durable facts that move [`SplitPhase`] forward.
/// Every one names something that became *committed or verified*, never
/// something merely attempted: a state machine driven by intentions
/// cannot be resumed from a crash.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SplitEvent {
    /// §12.1 step 2: `SplitPrepare` committed in the parent.
    PrepareCommitted = 1,
    /// §12.1 step 3: exact parent apply index and engine snapshot
    /// boundary established.
    BoundaryEstablished = 2,
    /// §12.1 steps 4-5: child snapshots built, installed, validated,
    /// and the bounded mutation tail caught up.
    ChildSnapshotsInstalled = 3,
    /// §12.1 step 6: cutover barrier committed; no new ambiguous
    /// writes.
    CutoverBarrierCommitted = 4,
    /// §12.1 step 7: `SplitFinalize` committed.
    FinalizeCommitted = 5,
    /// §12.1 step 8: replacement descriptors published atomically.
    DescriptorsPublished = 6,
    /// §12.1 step 10: descriptor-cache, snapshot, transaction, and
    /// replay safety floors all satisfied.
    TombstoneFloorsSatisfied = 7,
}

impl SplitEvent {
    pub const ALL: [Self; 7] = [
        Self::PrepareCommitted,
        Self::BoundaryEstablished,
        Self::ChildSnapshotsInstalled,
        Self::CutoverBarrierCommitted,
        Self::FinalizeCommitted,
        Self::DescriptorsPublished,
        Self::TombstoneFloorsSatisfied,
    ];
}

// ── Merge phases and events (§12.2) ───────────────────────────────────

/// Merge phase lattice, mirroring §12.2's numbered steps.
///
/// ```text
/// Validated            compatibility + adjacency checked (§12.2 preamble)
/// Committed            1. shared merge operation committed; target chosen
/// Quiesced             2. source writes quiesced/generation-fenced at a committed boundary
/// Transferred          3-4. snapshot + tail transferred; intents/watches/leases/timestamps preserved
/// TargetOwns           5. target ownership of the combined interval committed — OWNERSHIP FLIPS HERE
/// DescriptorsReplaced  6. both descriptors atomically replaced — PUBLICATION POINT
/// SourceTombstoned     7. source tombstoned for later GC; terminal
/// ```
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MergePhase {
    Validated = 1,
    Committed = 2,
    Quiesced = 3,
    Transferred = 4,
    TargetOwns = 5,
    DescriptorsReplaced = 6,
    SourceTombstoned = 7,
}

impl MergePhase {
    pub const ALL: [Self; 7] = [
        Self::Validated,
        Self::Committed,
        Self::Quiesced,
        Self::Transferred,
        Self::TargetOwns,
        Self::DescriptorsReplaced,
        Self::SourceTombstoned,
    ];
    pub const INITIAL: Self = Self::Validated;
    /// §12.2 step 6. At and after this phase the operation is
    /// forward-only.
    pub const PUBLICATION: Self = Self::DescriptorsReplaced;

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Validated),
            2 => Some(Self::Committed),
            3 => Some(Self::Quiesced),
            4 => Some(Self::Transferred),
            5 => Some(Self::TargetOwns),
            6 => Some(Self::DescriptorsReplaced),
            7 => Some(Self::SourceTombstoned),
            _ => None,
        }
    }

    pub const fn is_published(self) -> bool {
        (self as u8) >= (Self::PUBLICATION as u8)
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::SourceTombstoned)
    }

    pub const fn forward_event(self) -> Option<MergeEvent> {
        match self {
            Self::Validated => Some(MergeEvent::MergeCommitted),
            Self::Committed => Some(MergeEvent::WritesQuiesced),
            Self::Quiesced => Some(MergeEvent::StateTransferred),
            Self::Transferred => Some(MergeEvent::TargetOwnershipCommitted),
            Self::TargetOwns => Some(MergeEvent::DescriptorsReplaced),
            Self::DescriptorsReplaced => Some(MergeEvent::SourceTombstoned),
            Self::SourceTombstoned => None,
        }
    }

    pub const fn producing_event(self) -> Option<MergeEvent> {
        match self {
            Self::Validated => None,
            Self::Committed => Some(MergeEvent::MergeCommitted),
            Self::Quiesced => Some(MergeEvent::WritesQuiesced),
            Self::Transferred => Some(MergeEvent::StateTransferred),
            Self::TargetOwns => Some(MergeEvent::TargetOwnershipCommitted),
            Self::DescriptorsReplaced => Some(MergeEvent::DescriptorsReplaced),
            Self::SourceTombstoned => Some(MergeEvent::SourceTombstoned),
        }
    }
}

/// Merge events — the durable facts that move [`MergePhase`] forward.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MergeEvent {
    /// §12.2 step 1: shared merge operation committed.
    MergeCommitted = 1,
    /// §12.2 step 2: new writes quiesced or generation-fenced at a
    /// committed boundary.
    WritesQuiesced = 2,
    /// §12.2 steps 3-4: source snapshot and mutation tail transferred;
    /// intents, watches, leases, retention claims, and timestamps
    /// resolved or preserved without changing their meaning.
    StateTransferred = 3,
    /// §12.2 step 5: target ownership of the combined interval
    /// committed.
    TargetOwnershipCommitted = 4,
    /// §12.2 step 6: both descriptors atomically replaced.
    DescriptorsReplaced = 5,
    /// §12.2 step 7: source group tombstoned.
    SourceTombstoned = 6,
}

impl MergeEvent {
    pub const ALL: [Self; 6] = [
        Self::MergeCommitted,
        Self::WritesQuiesced,
        Self::StateTransferred,
        Self::TargetOwnershipCommitted,
        Self::DescriptorsReplaced,
        Self::SourceTombstoned,
    ];
}

// ── Relocation phases and events (§12.3) ──────────────────────────────

/// Relocation phase lattice, mirroring §12.3's numbered steps.
///
/// ```text
/// LearnerAdded         1. destination added as a learner
/// SnapshotInstalled    2. verified snapshot installed; WAL tail caught up
/// JointConsensus       3. Clustor joint consensus entered
/// Promoted             4. learner promoted, old replica removed
/// EpochAdvanced        5a. placement epoch advanced
/// DescriptorPublished  5b. new descriptor published — PUBLICATION POINT
/// OldStateRemovable    6. membership + retention floors permit deletion; terminal
/// ```
///
/// Note what is *absent*: no phase touches key bounds. Relocation moves
/// replicas. [`validate_relocation`] makes that a typed refusal rather
/// than a convention.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RelocatePhase {
    LearnerAdded = 1,
    SnapshotInstalled = 2,
    JointConsensus = 3,
    Promoted = 4,
    EpochAdvanced = 5,
    DescriptorPublished = 6,
    OldStateRemovable = 7,
}

impl RelocatePhase {
    pub const ALL: [Self; 7] = [
        Self::LearnerAdded,
        Self::SnapshotInstalled,
        Self::JointConsensus,
        Self::Promoted,
        Self::EpochAdvanced,
        Self::DescriptorPublished,
        Self::OldStateRemovable,
    ];
    pub const INITIAL: Self = Self::LearnerAdded;
    /// §12.3 step 5's publish half. At and after this phase the
    /// operation is forward-only.
    pub const PUBLICATION: Self = Self::DescriptorPublished;

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::LearnerAdded),
            2 => Some(Self::SnapshotInstalled),
            3 => Some(Self::JointConsensus),
            4 => Some(Self::Promoted),
            5 => Some(Self::EpochAdvanced),
            6 => Some(Self::DescriptorPublished),
            7 => Some(Self::OldStateRemovable),
            _ => None,
        }
    }

    pub const fn is_published(self) -> bool {
        (self as u8) >= (Self::PUBLICATION as u8)
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::OldStateRemovable)
    }

    pub const fn forward_event(self) -> Option<RelocateEvent> {
        match self {
            Self::LearnerAdded => Some(RelocateEvent::SnapshotInstalled),
            Self::SnapshotInstalled => Some(RelocateEvent::JointConsensusEntered),
            Self::JointConsensus => Some(RelocateEvent::LearnerPromoted),
            Self::Promoted => Some(RelocateEvent::PlacementEpochAdvanced),
            Self::EpochAdvanced => Some(RelocateEvent::DescriptorPublished),
            Self::DescriptorPublished => Some(RelocateEvent::RetentionFloorsPermit),
            Self::OldStateRemovable => None,
        }
    }

    pub const fn producing_event(self) -> Option<RelocateEvent> {
        match self {
            Self::LearnerAdded => None,
            Self::SnapshotInstalled => Some(RelocateEvent::SnapshotInstalled),
            Self::JointConsensus => Some(RelocateEvent::JointConsensusEntered),
            Self::Promoted => Some(RelocateEvent::LearnerPromoted),
            Self::EpochAdvanced => Some(RelocateEvent::PlacementEpochAdvanced),
            Self::DescriptorPublished => Some(RelocateEvent::DescriptorPublished),
            Self::OldStateRemovable => Some(RelocateEvent::RetentionFloorsPermit),
        }
    }
}

/// Relocation events — the durable facts that move [`RelocatePhase`]
/// forward.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RelocateEvent {
    /// §12.3 step 2: verified snapshot installed and WAL tail caught up.
    SnapshotInstalled = 1,
    /// §12.3 step 3: Clustor joint consensus entered.
    JointConsensusEntered = 2,
    /// §12.3 step 4: learner promoted and old replica removed.
    LearnerPromoted = 3,
    /// §12.3 step 5a: placement epoch advanced.
    PlacementEpochAdvanced = 4,
    /// §12.3 step 5b: new descriptor published.
    DescriptorPublished = 5,
    /// §12.3 step 6: membership and retention floors permit deleting
    /// the old state.
    RetentionFloorsPermit = 6,
}

impl RelocateEvent {
    pub const ALL: [Self; 6] = [
        Self::SnapshotInstalled,
        Self::JointConsensusEntered,
        Self::LearnerPromoted,
        Self::PlacementEpochAdvanced,
        Self::DescriptorPublished,
        Self::RetentionFloorsPermit,
    ];
}

// ── Kind-tagged phase and event ───────────────────────────────────────

/// A phase together with the machine it belongs to.
///
/// The record stores `kind` and `phase` as two bytes, but in memory the
/// kind is *derived* from the phase: there is no representable
/// `LifecycleOperation` whose kind byte and phase byte disagree, so the
/// conflated-operation hazard §12.3 warns about cannot be constructed,
/// only decoded — and [`LifecycleOperation::decode`] rejects it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Phase {
    Split(SplitPhase),
    Merge(MergePhase),
    Relocate(RelocatePhase),
}

impl Phase {
    pub const fn kind(self) -> LifecycleKind {
        match self {
            Self::Split(_) => LifecycleKind::Split,
            Self::Merge(_) => LifecycleKind::Merge,
            Self::Relocate(_) => LifecycleKind::Relocate,
        }
    }

    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Split(p) => p as u8,
            Self::Merge(p) => p as u8,
            Self::Relocate(p) => p as u8,
        }
    }

    /// Decode a phase byte in the context of its kind. Fails closed on
    /// a byte that is not a phase of *that* machine — `phase = 8` is a
    /// valid split phase and an invalid merge phase.
    pub const fn from_u8(kind: LifecycleKind, b: u8) -> Option<Self> {
        match kind {
            LifecycleKind::Split => match SplitPhase::from_u8(b) {
                Some(p) => Some(Self::Split(p)),
                None => None,
            },
            LifecycleKind::Merge => match MergePhase::from_u8(b) {
                Some(p) => Some(Self::Merge(p)),
                None => None,
            },
            LifecycleKind::Relocate => match RelocatePhase::from_u8(b) {
                Some(p) => Some(Self::Relocate(p)),
                None => None,
            },
        }
    }

    /// The initial phase of `kind`'s machine.
    pub const fn initial(kind: LifecycleKind) -> Self {
        match kind {
            LifecycleKind::Split => Self::Split(SplitPhase::INITIAL),
            LifecycleKind::Merge => Self::Merge(MergePhase::INITIAL),
            LifecycleKind::Relocate => Self::Relocate(RelocatePhase::INITIAL),
        }
    }

    pub const fn is_published(self) -> bool {
        match self {
            Self::Split(p) => p.is_published(),
            Self::Merge(p) => p.is_published(),
            Self::Relocate(p) => p.is_published(),
        }
    }

    pub const fn is_terminal(self) -> bool {
        match self {
            Self::Split(p) => p.is_terminal(),
            Self::Merge(p) => p.is_terminal(),
            Self::Relocate(p) => p.is_terminal(),
        }
    }
}

/// An event together with the machine it belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LifecycleEvent {
    Split(SplitEvent),
    Merge(MergeEvent),
    Relocate(RelocateEvent),
}

impl LifecycleEvent {
    pub const fn kind(self) -> LifecycleKind {
        match self {
            Self::Split(_) => LifecycleKind::Split,
            Self::Merge(_) => LifecycleKind::Merge,
            Self::Relocate(_) => LifecycleKind::Relocate,
        }
    }
}

// ── Typed errors ──────────────────────────────────────────────────────

/// Every way a lifecycle contract check can refuse.
///
/// There is deliberately no catch-all variant: a refusal must name what
/// it refused, and a new refusal must be a visible addition here rather
/// than a reuse of something vague.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    /// The event belongs to a different machine than the operation
    /// (§12.3: "split, merge, and relocation must not be conflated").
    KindMismatch {
        expected: LifecycleKind,
        actual: LifecycleKind,
    },
    /// A split transition outside the legal lattice — including any
    /// event delivered to the terminal phase.
    IllegalSplitTransition { from: SplitPhase, event: SplitEvent },
    /// A merge transition outside the legal lattice.
    IllegalMergeTransition { from: MergePhase, event: MergeEvent },
    /// A relocation transition outside the legal lattice.
    IllegalRelocateTransition {
        from: RelocatePhase,
        event: RelocateEvent,
    },
    /// Rollback requested at or after the publication point. §12.1
    /// permits rollback only "before metadata publication"; once
    /// replacement descriptors are observable the only path is forward.
    RollbackAfterPublication { kind: LifecycleKind, phase: u8 },
    /// §21 invariant 6 / §11.3: the operation was validated against a
    /// descriptor generation that is no longer current.
    StaleGeneration { current: u32, presented: u32 },
    /// §21 invariant 6: the operation was validated at a placement
    /// epoch that is no longer current.
    StaleEpoch { current: u32, presented: u32 },
    /// §11.3: the operation names a partition incarnation that is no
    /// longer current — a delayed message or file from a previously
    /// reused physical ID.
    StaleIncarnation { current: u32, presented: u32 },
    /// The operation's source range is not in the supplied map.
    SourceNotInMap,
    /// The operation's target range is not in the supplied map (merge:
    /// the surviving target must already exist).
    TargetNotInMap,
    /// The supplied map already violates §21 invariants 4/5. Nothing is
    /// published off a broken map.
    InputMapInvalid { violation: MapViolation },
    /// The map that *would* be published violates §21 invariants 4/5.
    /// It is refused, not published.
    OutputMapInvalid { violation: MapViolation },
    /// The post-transition map needs more descriptors than
    /// [`MAX_RANGES`].
    MapCapacityExceeded,
    /// The split key is empty. An empty bound means MIN, which cannot
    /// be an interior split point.
    SplitKeyEmpty,
    /// The split key is not strictly inside the parent's bounds. A key
    /// at or below `start_key` would make an empty left child; a key at
    /// or above `end_key` would make an empty right child — either way
    /// a published map would have a degenerate or overlapping interval.
    SplitKeyOutOfBounds,
    /// Merge sources are not adjacent: neither `source.end == target.
    /// start` nor `target.end == source.start`. Merging them would
    /// swallow the keys in between (§21 invariant 4).
    NotAdjacent,
    /// Merge participants disagree on database, partition map, routing
    /// kind, or cluster domain (§12.2 preamble).
    IncompatibleKeyspace,
    /// Merge participants disagree on replica placement (§12.2
    /// preamble: "placement policy … compatible").
    IncompatiblePlacement,
    /// A participant descriptor is not `Active`. A lifecycle operation
    /// starts from live ranges, never from a pending or tombstoned one.
    NotActive { lifecycle: Lifecycle },
    /// Relocation changed key bounds (§12.3: "relocation changes
    /// replicas, not key bounds").
    BoundsChanged,
    /// Relocation left the replica set and binding untouched — there is
    /// nothing to relocate, and publishing a new generation for a
    /// no-op would invalidate every cached descriptor for free.
    PlacementUnchanged,
    /// Relocation named a different logical range in its target. A
    /// relocation moves *this* range; renaming it is a split or merge
    /// wearing a disguise.
    RangeIdentityChanged,
    /// A counter that must advance would wrap: generation, placement
    /// epoch, or routing epoch at `u32::MAX`.
    CounterExhausted,
    /// The record's fields do not form a legal operation of its kind:
    /// wrong source/target counts, a split key on a merge, a zero
    /// operation id, a non-advancing generation, duplicate identities.
    MalformedOperation,
}

impl LifecycleError {
    /// The fencing errors map onto [`RetryClass::StaleRoute`]: the
    /// caller's view of the topology is stale and must be refreshed
    /// before retrying. Everything else is a contract violation, not a
    /// retry.
    pub const fn retry_class(&self) -> Option<RetryClass> {
        match self {
            Self::StaleGeneration { .. }
            | Self::StaleEpoch { .. }
            | Self::StaleIncarnation { .. } => Some(RetryClass::StaleRoute),
            _ => None,
        }
    }
}

// ── Target placement ──────────────────────────────────────────────────

/// One target range of a lifecycle operation: its logical identity plus
/// the physical binding it will hold (§11.3, §12.1 step 1 "allocate
/// fresh range identities and incarnations").
///
/// Wire layout ([`TARGET_WIRE_LEN`] = 22, integers LE):
///
/// ```text
/// [range_id:16]                 // 0..16   logical identity
/// [partition_id:u16]            // 16..18  Clustor physical binding
/// [partition_incarnation:u32]   // 18..22  fences reused physical IDs
/// ```
///
/// For a relocation the `range_id` is the *same* range and only the
/// binding changes; for a split the ids are fresh; for a merge it is
/// the surviving target's existing identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetPlacement {
    pub range_id: [u8; 16],
    pub partition_id: u16,
    pub partition_incarnation: u32,
}

impl TargetPlacement {
    pub const EMPTY: TargetPlacement = TargetPlacement {
        range_id: [0; 16],
        partition_id: 0,
        partition_incarnation: 0,
    };
}

// ── Lifecycle operation record ────────────────────────────────────────

/// The durable record of one in-flight §12 operation.
///
/// This is the thing §12.1 means by "resumable by operation ID": after
/// a crash the supervisor reloads this record, reads its phase, and
/// continues — or, if the phase is pre-publication, rolls back. It is
/// replicated, so it is versioned and fail-closed.
///
/// Wire layout ([`LIFECYCLE_OP_VERSION`] 1, all integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [operation_id:16]
/// [kind:u8][phase:u8]
/// [source_count:u8]  source_count × [range_id:16]
/// [target_count:u8]  target_count × [range_id:16][partition_id:u16][partition_incarnation:u32]
/// [split_key_len:u16][split_key...]
/// [apply_index_boundary:u64]
/// [source_generation:u32][next_generation:u32]
/// [placement_epoch:u32][partition_incarnation:u32]
/// [started_unix_ms:u64]
/// ```
///
/// Field notes:
///
/// - `apply_index_boundary` is §12.1 step 3's "exact parent apply
///   index": the apply prefix the child snapshots describe, and the
///   point after which the bounded mutation tail begins. It is zero
///   until [`SplitEvent::BoundaryEstablished`] fixes it. Merge reuses
///   it for the source's transfer boundary (§12.2 step 3); relocation
///   for the snapshot's apply index (§12.3 step 2).
/// - `source_generation`, `placement_epoch`, and `partition_incarnation`
///   are the *fences*: the values the operation was validated against.
///   [`stale_op_rejected`] compares them to the live values, so a
///   delayed operation from a superseded topology can never mutate
///   current state (§21 invariant 6, §11.3).
/// - `next_generation` is the generation the published descriptors will
///   carry. It must be strictly greater than `source_generation`, so a
///   cached pre-operation descriptor can never match a post-operation
///   one.
/// - `started_unix_ms` is wall time for operator observability and
///   cooldown policy (§12.4) only. It is never an input to a decision:
///   §21 invariant 3 forbids local time from affecting logical outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LifecycleOperation {
    /// Globally unique operation identity — the handle §12.1 means by
    /// "resumable by operation ID". All-zero is rejected.
    pub operation_id: [u8; 16],
    phase: Phase,
    source_count: u8,
    sources: [[u8; 16]; MAX_LIFECYCLE_RANGES],
    target_count: u8,
    targets: [TargetPlacement; MAX_LIFECYCLE_RANGES],
    split_key: [u8; MAX_SPLIT_KEY_LEN],
    split_key_len: u16,
    pub apply_index_boundary: u64,
    pub source_generation: u32,
    pub next_generation: u32,
    pub placement_epoch: u32,
    pub partition_incarnation: u32,
    pub started_unix_ms: u64,
}

impl LifecycleOperation {
    pub const EMPTY: LifecycleOperation = LifecycleOperation {
        operation_id: [0; 16],
        phase: Phase::Split(SplitPhase::Validated),
        source_count: 0,
        sources: [[0; 16]; MAX_LIFECYCLE_RANGES],
        target_count: 0,
        targets: [TargetPlacement::EMPTY; MAX_LIFECYCLE_RANGES],
        split_key: [0; MAX_SPLIT_KEY_LEN],
        split_key_len: 0,
        apply_index_boundary: 0,
        source_generation: 0,
        next_generation: 0,
        placement_epoch: 0,
        partition_incarnation: 0,
        started_unix_ms: 0,
    };

    /// Build a lifecycle operation. Fails closed (`None`) on anything
    /// that is not a legal record of its kind — see [`shape_ok`].
    #[allow(
        clippy::too_many_arguments,
        reason = "contract constructor mirrors the §12 record shape one-to-one"
    )]
    pub fn new(
        operation_id: [u8; 16],
        phase: Phase,
        sources: &[[u8; 16]],
        targets: &[TargetPlacement],
        split_key: &[u8],
        apply_index_boundary: u64,
        source_generation: u32,
        next_generation: u32,
        placement_epoch: u32,
        partition_incarnation: u32,
        started_unix_ms: u64,
    ) -> Option<Self> {
        if sources.len() > MAX_LIFECYCLE_RANGES || targets.len() > MAX_LIFECYCLE_RANGES {
            return None;
        }
        if split_key.len() > MAX_SPLIT_KEY_LEN {
            return None;
        }
        let mut op = Self::EMPTY;
        op.operation_id = operation_id;
        op.phase = phase;
        op.sources[..sources.len()].copy_from_slice(sources);
        op.source_count = sources.len() as u8;
        op.targets[..targets.len()].copy_from_slice(targets);
        op.target_count = targets.len() as u8;
        op.split_key[..split_key.len()].copy_from_slice(split_key);
        op.split_key_len = split_key.len() as u16;
        op.apply_index_boundary = apply_index_boundary;
        op.source_generation = source_generation;
        op.next_generation = next_generation;
        op.placement_epoch = placement_epoch;
        op.partition_incarnation = partition_incarnation;
        op.started_unix_ms = started_unix_ms;
        if !op.shape_ok() {
            return None;
        }
        Some(op)
    }

    /// Which §12 operation this is. Derived from the phase, so it can
    /// never disagree with it.
    pub const fn kind(&self) -> LifecycleKind {
        self.phase.kind()
    }

    pub const fn phase(&self) -> Phase {
        self.phase
    }

    /// Move the record to `phase`. Refuses a phase from another
    /// machine — the record's kind is immutable once constructed.
    pub fn set_phase(&mut self, phase: Phase) -> Result<(), LifecycleError> {
        if phase.kind() != self.kind() {
            return Err(LifecycleError::KindMismatch {
                expected: self.kind(),
                actual: phase.kind(),
            });
        }
        self.phase = phase;
        Ok(())
    }

    pub fn sources(&self) -> &[[u8; 16]] {
        &self.sources[..self.source_count as usize]
    }

    pub fn targets(&self) -> &[TargetPlacement] {
        &self.targets[..self.target_count as usize]
    }

    /// The single source (parent / merge source / relocating range).
    /// Every §12 operation has exactly one today; the array shape is a
    /// format allowance for a future N-way merge.
    pub fn source_range_id(&self) -> &[u8; 16] {
        &self.sources[0]
    }

    /// Split key, empty for merge and relocation.
    pub fn split_key(&self) -> &[u8] {
        &self.split_key[..self.split_key_len as usize]
    }

    /// Per-kind shape validation — the executable form of §12.3's "must
    /// not be conflated".
    ///
    /// Common to all kinds: a non-zero operation id, exactly one
    /// source, and a strictly advancing generation.
    ///
    /// - **Split**: two targets with distinct fresh identities, neither
    ///   equal to the parent, and a non-empty split key.
    /// - **Merge**: one target, distinct from the source, no split key.
    /// - **Relocate**: one target whose identity *equals* the source —
    ///   the same logical range, new physical binding — and no split
    ///   key.
    fn shape_ok(&self) -> bool {
        if self.operation_id == [0u8; 16] {
            return false;
        }
        if self.source_count != 1 {
            return false;
        }
        if self.next_generation <= self.source_generation {
            return false;
        }
        let src = self.sources[0];
        match self.kind() {
            LifecycleKind::Split => {
                self.target_count == 2
                    && self.split_key_len != 0
                    && self.targets[0].range_id != self.targets[1].range_id
                    && self.targets[0].range_id != src
                    && self.targets[1].range_id != src
            }
            LifecycleKind::Merge => {
                self.target_count == 1 && self.split_key_len == 0 && self.targets[0].range_id != src
            }
            LifecycleKind::Relocate => {
                self.target_count == 1 && self.split_key_len == 0 && self.targets[0].range_id == src
            }
        }
    }

    /// Exact encoded length of this operation.
    pub fn wire_len(&self) -> usize {
        LIFECYCLE_OP_FIXED_WIRE_LEN
            + self.source_count as usize * SOURCE_WIRE_LEN
            + self.target_count as usize * TARGET_WIRE_LEN
            + self.split_key_len as usize
    }

    /// Serialize to the versioned wire form. Returns the encoded
    /// length, or `None` if `out` is too small (fail closed — never
    /// truncate).
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&LIFECYCLE_OP_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        let mut n = 4;
        out[n..n + 16].copy_from_slice(&self.operation_id);
        n += 16;
        out[n] = self.kind() as u8;
        n += 1;
        out[n] = self.phase.as_u8();
        n += 1;
        out[n] = self.source_count;
        n += 1;
        for s in self.sources() {
            out[n..n + 16].copy_from_slice(s);
            n += 16;
        }
        out[n] = self.target_count;
        n += 1;
        for t in self.targets() {
            out[n..n + 16].copy_from_slice(&t.range_id);
            n += 16;
            out[n..n + 2].copy_from_slice(&t.partition_id.to_le_bytes());
            n += 2;
            out[n..n + 4].copy_from_slice(&t.partition_incarnation.to_le_bytes());
            n += 4;
        }
        out[n..n + 2].copy_from_slice(&self.split_key_len.to_le_bytes());
        n += 2;
        out[n..n + self.split_key_len as usize].copy_from_slice(self.split_key());
        n += self.split_key_len as usize;
        out[n..n + 8].copy_from_slice(&self.apply_index_boundary.to_le_bytes());
        n += 8;
        out[n..n + 4].copy_from_slice(&self.source_generation.to_le_bytes());
        n += 4;
        out[n..n + 4].copy_from_slice(&self.next_generation.to_le_bytes());
        n += 4;
        out[n..n + 4].copy_from_slice(&self.placement_epoch.to_le_bytes());
        n += 4;
        out[n..n + 4].copy_from_slice(&self.partition_incarnation.to_le_bytes());
        n += 4;
        out[n..n + 8].copy_from_slice(&self.started_unix_ms.to_le_bytes());
        n += 8;
        debug_assert_eq!(n, total);
        Some(n)
    }

    /// Parse the versioned wire form. `src` must be exactly one encoded
    /// operation. Fails closed on unknown version, declared-length
    /// mismatch, truncation, trailing bytes, an unknown kind, a phase
    /// that does not belong to that kind, oversized arrays or split
    /// key, and any record whose fields do not form a legal operation
    /// of its kind ([`shape_ok`]).
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < 4 {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != LIFECYCLE_OP_VERSION {
            return None;
        }
        let payload_len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if src.len() != 4 + payload_len {
            return None; // truncated or trailing bytes
        }
        fn take<'a>(p: &'a [u8], n: &mut usize, k: usize) -> Option<&'a [u8]> {
            let s = p.get(*n..n.checked_add(k)?)?;
            *n += k;
            Some(s)
        }
        let p = &src[4..];
        let mut n = 0usize;
        let take = |n: &mut usize, k: usize| take(p, n, k);

        let operation_id: [u8; 16] = take(&mut n, 16)?.try_into().ok()?;
        let kind = LifecycleKind::from_u8(take(&mut n, 1)?[0])?;
        let phase = Phase::from_u8(kind, take(&mut n, 1)?[0])?;

        let source_count = take(&mut n, 1)?[0] as usize;
        if source_count == 0 || source_count > MAX_LIFECYCLE_RANGES {
            return None;
        }
        let mut sources = [[0u8; 16]; MAX_LIFECYCLE_RANGES];
        for s in sources.iter_mut().take(source_count) {
            *s = take(&mut n, 16)?.try_into().ok()?;
        }

        let target_count = take(&mut n, 1)?[0] as usize;
        if target_count == 0 || target_count > MAX_LIFECYCLE_RANGES {
            return None;
        }
        let mut targets = [TargetPlacement::EMPTY; MAX_LIFECYCLE_RANGES];
        for t in targets.iter_mut().take(target_count) {
            t.range_id = take(&mut n, 16)?.try_into().ok()?;
            t.partition_id = u16::from_le_bytes(take(&mut n, 2)?.try_into().ok()?);
            t.partition_incarnation = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        }

        let split_key_len = u16::from_le_bytes(take(&mut n, 2)?.try_into().ok()?) as usize;
        if split_key_len > MAX_SPLIT_KEY_LEN {
            return None;
        }
        let split_key_src = take(&mut n, split_key_len)?;
        // Borrow of `p` ends here; copy the key out before continuing.
        let mut split_key = [0u8; MAX_SPLIT_KEY_LEN];
        split_key[..split_key_len].copy_from_slice(split_key_src);

        let apply_index_boundary = u64::from_le_bytes(take(&mut n, 8)?.try_into().ok()?);
        let source_generation = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        let next_generation = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        let placement_epoch = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        let partition_incarnation = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        let started_unix_ms = u64::from_le_bytes(take(&mut n, 8)?.try_into().ok()?);
        if n != payload_len {
            return None; // declared length disagrees with content
        }
        let op = Self {
            operation_id,
            phase,
            source_count: source_count as u8,
            sources,
            target_count: target_count as u8,
            targets,
            split_key,
            split_key_len: split_key_len as u16,
            apply_index_boundary,
            source_generation,
            next_generation,
            placement_epoch,
            partition_incarnation,
            started_unix_ms,
        };
        if !op.shape_ok() {
            return None;
        }
        Some(op)
    }
}

// ── State machines ────────────────────────────────────────────────────

/// Advance the split machine (§12.1).
///
/// Exactly two events are legal in any phase:
///
/// - the phase's [`SplitPhase::forward_event`], which moves it on;
/// - the phase's [`SplitPhase::producing_event`], which is an
///   *idempotent redelivery* of the fact that already put it here and
///   returns the phase unchanged. Committed events are replayed by
///   Raft apply and by resume-after-crash, so a machine that faulted on
///   redelivery would be unable to recover (§21 invariant 17).
///
/// Everything else — every backwards event, every skipped step, and
/// every event delivered to the terminal phase — is
/// [`LifecycleError::IllegalSplitTransition`].
pub fn advance_split(
    op: &LifecycleOperation,
    event: SplitEvent,
) -> Result<SplitPhase, LifecycleError> {
    let from = match op.phase() {
        Phase::Split(p) => p,
        other => {
            return Err(LifecycleError::KindMismatch {
                expected: other.kind(),
                actual: LifecycleKind::Split,
            })
        }
    };
    if from.producing_event() == Some(event) {
        return Ok(from);
    }
    match from.forward_event() {
        Some(e) if e == event => Ok(next_split(from)),
        _ => Err(LifecycleError::IllegalSplitTransition { from, event }),
    }
}

/// Successor of a non-terminal split phase. Private: the only way to
/// move is [`advance_split`], which checks the event first.
const fn next_split(p: SplitPhase) -> SplitPhase {
    match p {
        SplitPhase::Validated => SplitPhase::Prepared,
        SplitPhase::Prepared => SplitPhase::BoundaryEstablished,
        SplitPhase::BoundaryEstablished => SplitPhase::ChildrenBootstrapped,
        SplitPhase::ChildrenBootstrapped => SplitPhase::CutoverBarrier,
        SplitPhase::CutoverBarrier => SplitPhase::Finalized,
        SplitPhase::Finalized => SplitPhase::DescriptorsPublished,
        SplitPhase::DescriptorsPublished => SplitPhase::TombstoneRetained,
        SplitPhase::TombstoneRetained => SplitPhase::TombstoneRetained,
    }
}

/// Advance the merge machine (§12.2). Same legality rule as
/// [`advance_split`].
pub fn advance_merge(
    op: &LifecycleOperation,
    event: MergeEvent,
) -> Result<MergePhase, LifecycleError> {
    let from = match op.phase() {
        Phase::Merge(p) => p,
        other => {
            return Err(LifecycleError::KindMismatch {
                expected: other.kind(),
                actual: LifecycleKind::Merge,
            })
        }
    };
    if from.producing_event() == Some(event) {
        return Ok(from);
    }
    match from.forward_event() {
        Some(e) if e == event => Ok(next_merge(from)),
        _ => Err(LifecycleError::IllegalMergeTransition { from, event }),
    }
}

const fn next_merge(p: MergePhase) -> MergePhase {
    match p {
        MergePhase::Validated => MergePhase::Committed,
        MergePhase::Committed => MergePhase::Quiesced,
        MergePhase::Quiesced => MergePhase::Transferred,
        MergePhase::Transferred => MergePhase::TargetOwns,
        MergePhase::TargetOwns => MergePhase::DescriptorsReplaced,
        MergePhase::DescriptorsReplaced => MergePhase::SourceTombstoned,
        MergePhase::SourceTombstoned => MergePhase::SourceTombstoned,
    }
}

/// Advance the relocation machine (§12.3). Same legality rule as
/// [`advance_split`].
pub fn advance_relocate(
    op: &LifecycleOperation,
    event: RelocateEvent,
) -> Result<RelocatePhase, LifecycleError> {
    let from = match op.phase() {
        Phase::Relocate(p) => p,
        other => {
            return Err(LifecycleError::KindMismatch {
                expected: other.kind(),
                actual: LifecycleKind::Relocate,
            })
        }
    };
    if from.producing_event() == Some(event) {
        return Ok(from);
    }
    match from.forward_event() {
        Some(e) if e == event => Ok(next_relocate(from)),
        _ => Err(LifecycleError::IllegalRelocateTransition { from, event }),
    }
}

const fn next_relocate(p: RelocatePhase) -> RelocatePhase {
    match p {
        RelocatePhase::LearnerAdded => RelocatePhase::SnapshotInstalled,
        RelocatePhase::SnapshotInstalled => RelocatePhase::JointConsensus,
        RelocatePhase::JointConsensus => RelocatePhase::Promoted,
        RelocatePhase::Promoted => RelocatePhase::EpochAdvanced,
        RelocatePhase::EpochAdvanced => RelocatePhase::DescriptorPublished,
        RelocatePhase::DescriptorPublished => RelocatePhase::OldStateRemovable,
        RelocatePhase::OldStateRemovable => RelocatePhase::OldStateRemovable,
    }
}

/// Kind-dispatching form of the three machines. An event from a
/// different machine than the operation is
/// [`LifecycleError::KindMismatch`] — §12.3's "must not be conflated"
/// enforced at the dispatch point rather than left to the caller.
pub fn advance(op: &LifecycleOperation, event: LifecycleEvent) -> Result<Phase, LifecycleError> {
    if event.kind() != op.kind() {
        return Err(LifecycleError::KindMismatch {
            expected: op.kind(),
            actual: event.kind(),
        });
    }
    match event {
        LifecycleEvent::Split(e) => advance_split(op, e).map(Phase::Split),
        LifecycleEvent::Merge(e) => advance_merge(op, e).map(Phase::Merge),
        LifecycleEvent::Relocate(e) => advance_relocate(op, e).map(Phase::Relocate),
    }
}

// ── Resume and rollback (§12.1: "resumable … or rolled back") ─────────

/// Can an operation sitting at `phase` be picked up and driven forward
/// after a crash or leader change?
///
/// Every phase except the terminal one: §12.1's "resumable by operation
/// ID" applies on both sides of the publication point, because after
/// publication forward is the *only* direction and resumption is how it
/// gets there. A terminal phase has nothing left to do.
pub const fn is_resumable(phase: Phase) -> bool {
    !phase.is_terminal()
}

/// The phase an operation rolls back to, or `None` if rollback is
/// illegal.
///
/// §12.1: "A failed split is resumable by operation ID **or is rolled
/// back before metadata publication**." That clause draws a hard line:
///
/// - **Before publication** the routing map still names only the
///   pre-operation ranges ([`apply_to_map`] returns it unchanged), so
///   unwinding is a purely local matter of discarding child groups,
///   snapshots, and barriers. Rollback targets the machine's initial
///   phase, from which the operation may be abandoned outright.
///   Rolling back *from* the initial phase is idempotent, not an error.
/// - **At or after publication** replacement descriptors have been
///   observed: clients cache them, participants fence transactions on
///   the new generations, and the source group may already be
///   tombstoned. Unwinding would resurrect ownership the cluster has
///   stopped believing in — a second authority for keys that already
///   moved, violating §21 invariant 4. So this returns `None`, and the
///   only path is forward.
pub const fn rollback_target(phase: Phase) -> Option<Phase> {
    if phase.is_published() {
        return None;
    }
    Some(Phase::initial(phase.kind()))
}

/// [`rollback_target`] as a typed refusal, for callers that want the
/// reason rather than an `Option`.
pub fn rollback(phase: Phase) -> Result<Phase, LifecycleError> {
    rollback_target(phase).ok_or(LifecycleError::RollbackAfterPublication {
        kind: phase.kind(),
        phase: phase.as_u8(),
    })
}

// ── Writable ownership (§12.1: "no overlapping writable ownership") ───

/// Who holds writable authority at a given phase.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WritableOwner {
    /// The source range(s) only.
    Source = 1,
    /// The target range(s) only.
    Targets = 2,
    /// Source and targets both — legal ONLY for a merge before its
    /// quiesce boundary, where the two ranges are *adjacent and
    /// disjoint* and each still serves its own interval. Unreachable
    /// for a split, which is the whole content of §12.1's "no phase may
    /// expose overlapping writable ownership".
    SourceAndTargets = 3,
}

/// The bounded set of ranges that may accept writes at a given phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WritableSet {
    pub owner: WritableOwner,
    count: u8,
    ranges: [[u8; 16]; MAX_LIFECYCLE_RANGES],
}

impl WritableSet {
    pub fn ranges(&self) -> &[[u8; 16]] {
        &self.ranges[..self.count as usize]
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn contains(&self, range_id: &[u8; 16]) -> bool {
        self.ranges().iter().any(|r| r == range_id)
    }

    fn of(owner: WritableOwner, ids: &[[u8; 16]]) -> Self {
        let mut s = Self {
            owner,
            count: ids.len() as u8,
            ranges: [[0u8; 16]; MAX_LIFECYCLE_RANGES],
        };
        s.ranges[..ids.len()].copy_from_slice(ids);
        s
    }
}

/// Which ranges may accept writes while `op` sits at `phase`.
///
/// This is the executable form of §12.1's "**No phase may expose
/// overlapping writable ownership**", and it is deliberately separate
/// from [`apply_to_map`]: routing metadata lags the truth (§12
/// preamble), so the phase at which ownership *moves* is earlier than
/// the phase at which descriptors are *published*, and both facts have
/// to be stateable independently.
///
/// **Split** — the set is always exactly one of {parent} or {children},
/// never both and never empty:
///
/// ```text
/// Validated … CutoverBarrier   {parent}    children are learners installing snapshots;
///                                          at CutoverBarrier new ambiguous writes are
///                                          barred but in-flight transactions still
///                                          finish under the parent generation (§12.1 step 6)
/// Finalized … TombstoneRetained {children} SplitFinalize gave each child one
///                                          authoritative bound (§12.1 step 7)
/// ```
///
/// The flip is at `Finalized`, not at publication: publication is when
/// *routing* learns about it, and a router that is behind gets a stale-
/// route redirect from the parent generation (§12.1 step 9), never a
/// second writable owner.
///
/// **Merge** — before the quiesce boundary source and target are two
/// live, adjacent, disjoint ranges, each writable on its own interval
/// ([`WritableOwner::SourceAndTargets`]); from `Quiesced` on the source
/// accepts nothing and only the target is writable. The bounded
/// unavailability of the source interval between `Quiesced` and
/// `TargetOwns` is §12.2 step 2's explicit cost.
///
/// **Relocate** — always exactly {the range}. Relocation changes
/// replicas, not bounds; there is no second owner at any phase.
pub fn writable_ranges_at(
    op: &LifecycleOperation,
    phase: Phase,
) -> Result<WritableSet, LifecycleError> {
    if phase.kind() != op.kind() {
        return Err(LifecycleError::KindMismatch {
            expected: op.kind(),
            actual: phase.kind(),
        });
    }
    let src = *op.source_range_id();
    Ok(match phase {
        Phase::Split(p) => {
            if (p as u8) < (SplitPhase::Finalized as u8) {
                WritableSet::of(WritableOwner::Source, &[src])
            } else {
                WritableSet::of(
                    WritableOwner::Targets,
                    &[op.targets[0].range_id, op.targets[1].range_id],
                )
            }
        }
        Phase::Merge(p) => {
            if (p as u8) < (MergePhase::Quiesced as u8) {
                WritableSet::of(
                    WritableOwner::SourceAndTargets,
                    &[src, op.targets[0].range_id],
                )
            } else {
                WritableSet::of(WritableOwner::Targets, &[op.targets[0].range_id])
            }
        }
        Phase::Relocate(_) => WritableSet::of(WritableOwner::Source, &[src]),
    })
}

// ── Per-kind precondition checks ──────────────────────────────────────

/// §12.1 step 1's key check: is `split_key` a legal interior split
/// point of `parent`?
///
/// The parent must be `Active`, and the key must be strictly inside its
/// bounds. Both children must be non-empty intervals — a key equal to
/// `start_key` would produce an empty left child, and a key at or above
/// a non-MAX `end_key` an empty right child; either is a
/// [`MapViolation::InvertedBounds`] waiting to be published.
pub fn validate_split(parent: &RangeDescriptor, split_key: &[u8]) -> Result<(), LifecycleError> {
    if parent.lifecycle != Lifecycle::Active {
        return Err(LifecycleError::NotActive {
            lifecycle: parent.lifecycle,
        });
    }
    if split_key.is_empty() {
        return Err(LifecycleError::SplitKeyEmpty);
    }
    if split_key <= parent.start_key() {
        return Err(LifecycleError::SplitKeyOutOfBounds);
    }
    // An empty end bound means MAX, so every key is below it.
    if !parent.end_key().is_empty() && split_key >= parent.end_key() {
        return Err(LifecycleError::SplitKeyOutOfBounds);
    }
    Ok(())
}

/// §12.2's compatibility preamble: "Adjacent ranges may merge only when
/// their database, partition map, format, placement policy, and
/// protected-state constraints are compatible."
///
/// The descriptor-observable subset of that is checked here and each
/// clause refuses on its own:
///
/// 1. both `Active` — a lifecycle operation starts from live ranges;
/// 2. same `database_id`, `partition_map_id`, `routing_kind`, and
///    Clustor `cluster_id` (a range cannot merge across cluster
///    domains — §11.3's physical binding is cluster-local);
/// 3. same placement policy, approximated by an identical voter/learner
///    node set and replica count (§11.5);
/// 4. **adjacency**: `source.end == target.start` or `target.end ==
///    source.start`, with the shared bound non-empty. Merging
///    non-adjacent ranges would swallow every key in between, giving
///    them a second owner and breaking §21 invariant 4.
///
/// Format version and protected-state constraints live in engine
/// manifests and retention claims, not in the descriptor, so they
/// remain the supervisor's checks — this function is the part that can
/// be made executable from contracts alone.
pub fn validate_merge(
    source: &RangeDescriptor,
    target: &RangeDescriptor,
) -> Result<(), LifecycleError> {
    if source.lifecycle != Lifecycle::Active {
        return Err(LifecycleError::NotActive {
            lifecycle: source.lifecycle,
        });
    }
    if target.lifecycle != Lifecycle::Active {
        return Err(LifecycleError::NotActive {
            lifecycle: target.lifecycle,
        });
    }
    if source.database_id != target.database_id
        || source.partition_map_id != target.partition_map_id
        || source.routing_kind != target.routing_kind
        || source.binding.cluster_id != target.binding.cluster_id
    {
        return Err(LifecycleError::IncompatibleKeyspace);
    }
    if !same_placement(source, target) {
        return Err(LifecycleError::IncompatiblePlacement);
    }
    if merge_order(source, target).is_none() {
        return Err(LifecycleError::NotAdjacent);
    }
    Ok(())
}

/// Placement-policy compatibility (§11.5): same replica count, and the
/// same `(node_id, role, failure_domain)` multiset. Order is not
/// significant — a descriptor may list the same replicas in a
/// different order without that being a policy difference.
fn same_placement(a: &RangeDescriptor, b: &RangeDescriptor) -> bool {
    if a.replicas().len() != b.replicas().len() {
        return false;
    }
    // Bounded by MAX_REPLICAS (5); the quadratic scan is cheaper than
    // sorting into scratch and needs no allocation.
    for ra in a.replicas() {
        let in_a = a.replicas().iter().filter(|x| *x == ra).count();
        let in_b = b.replicas().iter().filter(|x| *x == ra).count();
        if in_a != in_b {
            return false;
        }
    }
    true
}

/// Adjacency test and ordering: `Some(true)` if `source` immediately
/// precedes `target`, `Some(false)` if `target` immediately precedes
/// `source`, `None` if they are not adjacent.
///
/// The shared bound must be non-empty: an empty end bound means MAX,
/// and nothing can start at MAX.
fn merge_order(source: &RangeDescriptor, target: &RangeDescriptor) -> Option<bool> {
    if !source.end_key().is_empty() && source.end_key() == target.start_key() {
        return Some(true);
    }
    if !target.end_key().is_empty() && target.end_key() == source.start_key() {
        return Some(false);
    }
    None
}

/// §12.3's one-line rule made a typed refusal: **relocation changes
/// replicas, not key bounds.**
///
/// `pre` and `post` are the descriptor before and after the operation.
/// Refuses if:
///
/// - the logical identity changed ([`LifecycleError::RangeIdentityChanged`])
///   — a renamed range is a split or merge in disguise, and §12.3 says
///   the three must not be conflated;
/// - either key bound changed ([`LifecycleError::BoundsChanged`]) — this
///   is the hazard the RFC calls out, because a bounds change smuggled
///   through the relocation path skips every ownership barrier the
///   split and merge machines exist to provide;
/// - nothing about the placement changed ([`LifecycleError::PlacementUnchanged`])
///   — publishing a new generation for a no-op invalidates every cached
///   descriptor in the cluster and buys nothing.
///
/// A changed replica set, leaseholder, partition binding, or placement
/// epoch is exactly what this operation is *for* and is accepted.
pub fn validate_relocation(
    pre: &RangeDescriptor,
    post: &RangeDescriptor,
) -> Result<(), LifecycleError> {
    if pre.range_id != post.range_id {
        return Err(LifecycleError::RangeIdentityChanged);
    }
    if pre.start_key() != post.start_key() || pre.end_key() != post.end_key() {
        return Err(LifecycleError::BoundsChanged);
    }
    let placement_moved = pre.replicas() != post.replicas()
        || pre.leaseholder != post.leaseholder
        || pre.binding.partition_id != post.binding.partition_id
        || pre.binding.partition_incarnation != post.binding.partition_incarnation
        || pre.binding.placement_epoch != post.binding.placement_epoch;
    if !placement_moved {
        return Err(LifecycleError::PlacementUnchanged);
    }
    Ok(())
}

// ── Fencing (§21 invariant 6, §11.3) ──────────────────────────────────

/// §21 invariant 6 — "a stale generation, placement epoch, or partition
/// incarnation cannot mutate current state" — and §11.3's incarnation
/// rule: "a `partition_incarnation` prevents delayed messages or files
/// from a previously reused physical ID from becoming valid."
///
/// `Ok(())` means the operation's fences match the live topology and it
/// may proceed. `Err` names which fence rejected it, with both values,
/// and maps onto [`RetryClass::StaleRoute`] via
/// [`LifecycleError::retry_class`].
///
/// All three comparisons are **exact equality, not "behind"**, for the
/// same reason `partition_map::lookup_generation_checked` is: a value
/// *ahead* of the live one is not evidence of the future, it is
/// evidence that the caller is reading a different world — a restored
/// backup, a partitioned metadata replica, or a corrupt record. Fail
/// closed on both directions.
///
/// The incarnation check is the one that specifically defeats physical-
/// ID reuse: a partition id may be recycled after a range is
/// tombstoned, so an operation validated against incarnation *n* must
/// never apply to incarnation *n+1* even though the `partition_id`,
/// cluster, and key bounds all still match.
pub fn stale_op_rejected(
    op: &LifecycleOperation,
    current_generation: u32,
    current_epoch: u32,
    current_incarnation: u32,
) -> Result<(), LifecycleError> {
    if op.source_generation != current_generation {
        return Err(LifecycleError::StaleGeneration {
            current: current_generation,
            presented: op.source_generation,
        });
    }
    if op.placement_epoch != current_epoch {
        return Err(LifecycleError::StaleEpoch {
            current: current_epoch,
            presented: op.placement_epoch,
        });
    }
    if op.partition_incarnation != current_incarnation {
        return Err(LifecycleError::StaleIncarnation {
            current: current_incarnation,
            presented: op.partition_incarnation,
        });
    }
    Ok(())
}

// ── Map application (§21 invariants 4 and 5) ──────────────────────────

/// The published routing map while `op` sits at `phase`.
///
/// This is where §21 invariants 4 and 5 get enforced mechanically. Both
/// the input and the produced map are run through
/// [`OrderedRangeMap::validate`], so a transition that *would* create a
/// gap, an overlap, an ordering inversion, or an inactive descriptor is
/// **refused rather than published**.
///
/// The phase semantics follow §12's "routing metadata is published only
/// after the new ownership state is durable":
///
/// - **before the publication point** the result is the input map
///   copied unchanged. A half-applied map never exists, so there is no
///   intermediate state in which invariants 4/5 could be violated and
///   no map repair to do if the operation is abandoned;
/// - **at and after the publication point** the result is the
///   replacement map: parent → two children (split), source+target →
///   one merged descriptor (merge), or the same descriptor with a new
///   binding, generation, and placement epoch (relocate).
///
/// The `routing_epoch` advances by one exactly when the map changes,
/// so a cached descriptor from before publication is fenced by
/// `lookup_generation_checked` on both the epoch and the generation.
///
/// Preconditions checked before anything is built: the source (and, for
/// a merge, the target) must be present in the map, and the per-kind
/// checks [`validate_split`] / [`validate_merge`] / [`validate_relocation`]
/// must pass.
pub fn apply_to_map(
    map: &OrderedRangeMap,
    op: &LifecycleOperation,
    phase: Phase,
) -> Result<OrderedRangeMap, LifecycleError> {
    if phase.kind() != op.kind() {
        return Err(LifecycleError::KindMismatch {
            expected: op.kind(),
            actual: phase.kind(),
        });
    }
    map.validate()
        .map_err(|violation| LifecycleError::InputMapInvalid { violation })?;

    let source_index = index_of(map, op.source_range_id()).ok_or(LifecycleError::SourceNotInMap)?;

    // Per-kind preconditions run at EVERY phase, not just at
    // publication: an operation whose shape could never be published
    // must be refused while it is still cheap to refuse, and the
    // property tests rely on that to prove no bad map is ever reachable.
    match op.kind() {
        LifecycleKind::Split => {
            validate_split(&map.ranges()[source_index], op.split_key())?;
        }
        LifecycleKind::Merge => {
            let target_index =
                index_of(map, &op.targets[0].range_id).ok_or(LifecycleError::TargetNotInMap)?;
            validate_merge(&map.ranges()[source_index], &map.ranges()[target_index])?;
        }
        LifecycleKind::Relocate => {
            let pre = map.ranges()[source_index];
            let post = relocated_descriptor(&pre, op)?;
            validate_relocation(&pre, &post)?;
        }
    }

    if !phase.is_published() {
        return copy_map(map, map.routing_epoch);
    }

    let next_epoch = map
        .routing_epoch
        .checked_add(1)
        .ok_or(LifecycleError::CounterExhausted)?;
    let out = match op.kind() {
        LifecycleKind::Split => split_map(map, op, source_index, next_epoch)?,
        LifecycleKind::Merge => merge_map(map, op, source_index, next_epoch)?,
        LifecycleKind::Relocate => relocate_map(map, op, source_index, next_epoch)?,
    };
    out.validate()
        .map_err(|violation| LifecycleError::OutputMapInvalid { violation })?;
    Ok(out)
}

/// Index of the descriptor with this logical identity, if present.
fn index_of(map: &OrderedRangeMap, range_id: &[u8; 16]) -> Option<usize> {
    map.ranges().iter().position(|d| &d.range_id == range_id)
}

/// Byte-identical copy of a map at a chosen routing epoch.
/// `OrderedRangeMap` has private fields, so the copy goes through the
/// public `push` — which also re-checks capacity.
fn copy_map(map: &OrderedRangeMap, routing_epoch: u32) -> Result<OrderedRangeMap, LifecycleError> {
    let mut out = OrderedRangeMap::new(routing_epoch);
    for d in map.ranges() {
        out.push(*d).ok_or(LifecycleError::MapCapacityExceeded)?;
    }
    Ok(out)
}

/// Replacement map for a published split: the parent descriptor is
/// replaced in place by two children that tile its interval exactly —
/// `[start, split_key)` and `[split_key, end)`. Tiling by construction
/// is what makes a gap or overlap unreachable; `validate_split` is what
/// makes a degenerate child unreachable.
fn split_map(
    map: &OrderedRangeMap,
    op: &LifecycleOperation,
    parent_index: usize,
    routing_epoch: u32,
) -> Result<OrderedRangeMap, LifecycleError> {
    let parent = map.ranges()[parent_index];
    let lo = child_descriptor(
        &parent,
        &op.targets[0],
        parent.start_key(),
        op.split_key(),
        op.next_generation,
    )?;
    let hi = child_descriptor(
        &parent,
        &op.targets[1],
        op.split_key(),
        parent.end_key(),
        op.next_generation,
    )?;
    let mut out = OrderedRangeMap::new(routing_epoch);
    for (i, d) in map.ranges().iter().enumerate() {
        if i == parent_index {
            out.push(lo).ok_or(LifecycleError::MapCapacityExceeded)?;
            out.push(hi).ok_or(LifecycleError::MapCapacityExceeded)?;
        } else {
            out.push(*d).ok_or(LifecycleError::MapCapacityExceeded)?;
        }
    }
    Ok(out)
}

/// One split child: the parent's keyspace identity, replica set, and
/// leaseholder, with a fresh logical identity, a fresh physical
/// binding, the operation's `next_generation`, and the half-open
/// sub-interval it owns.
///
/// The children inherit the parent's replica set because §12.1 step 5
/// bootstraps them from the parent's snapshot; moving replicas at the
/// same time would be a relocation, and §12.3 forbids conflating the
/// two.
fn child_descriptor(
    parent: &RangeDescriptor,
    target: &TargetPlacement,
    start_key: &[u8],
    end_key: &[u8],
    generation: u32,
) -> Result<RangeDescriptor, LifecycleError> {
    RangeDescriptor::new(
        parent.database_id,
        parent.partition_map_id,
        parent.routing_kind,
        target.range_id,
        start_key,
        end_key,
        generation,
        Lifecycle::Active,
        Binding {
            cluster_id: parent.binding.cluster_id,
            partition_id: target.partition_id,
            partition_incarnation: target.partition_incarnation,
            placement_epoch: parent.binding.placement_epoch,
        },
        parent.replicas(),
        parent.leaseholder,
    )
    .ok_or(LifecycleError::MalformedOperation)
}

/// Replacement map for a published merge: the two adjacent descriptors
/// collapse into one covering their union, carrying the surviving
/// target's logical identity and binding, at `next_generation`. The
/// merged descriptor takes the earlier of the two positions so the map
/// stays ascending.
fn merge_map(
    map: &OrderedRangeMap,
    op: &LifecycleOperation,
    source_index: usize,
    routing_epoch: u32,
) -> Result<OrderedRangeMap, LifecycleError> {
    let target_index =
        index_of(map, &op.targets[0].range_id).ok_or(LifecycleError::TargetNotInMap)?;
    let source = map.ranges()[source_index];
    let target = map.ranges()[target_index];
    let source_first = merge_order(&source, &target).ok_or(LifecycleError::NotAdjacent)?;
    let (start, end) = if source_first {
        (source.start_key(), target.end_key())
    } else {
        (target.start_key(), source.end_key())
    };
    let merged = RangeDescriptor::new(
        target.database_id,
        target.partition_map_id,
        target.routing_kind,
        target.range_id,
        start,
        end,
        op.next_generation,
        Lifecycle::Active,
        target.binding,
        target.replicas(),
        target.leaseholder,
    )
    .ok_or(LifecycleError::MalformedOperation)?;

    let keep = source_index.min(target_index);
    let drop = source_index.max(target_index);
    let mut out = OrderedRangeMap::new(routing_epoch);
    for (i, d) in map.ranges().iter().enumerate() {
        if i == keep {
            out.push(merged)
                .ok_or(LifecycleError::MapCapacityExceeded)?;
        } else if i == drop {
            continue;
        } else {
            out.push(*d).ok_or(LifecycleError::MapCapacityExceeded)?;
        }
    }
    Ok(out)
}

/// Replacement map for a published relocation: the same descriptor at
/// the same bounds with the new binding, generation, and advanced
/// placement epoch.
fn relocate_map(
    map: &OrderedRangeMap,
    op: &LifecycleOperation,
    index: usize,
    routing_epoch: u32,
) -> Result<OrderedRangeMap, LifecycleError> {
    let pre = map.ranges()[index];
    let post = relocated_descriptor(&pre, op)?;
    validate_relocation(&pre, &post)?;
    let mut out = OrderedRangeMap::new(routing_epoch);
    for (i, d) in map.ranges().iter().enumerate() {
        let d = if i == index { post } else { *d };
        out.push(d).ok_or(LifecycleError::MapCapacityExceeded)?;
    }
    Ok(out)
}

/// The post-relocation descriptor: identical bounds and identity, new
/// physical binding from the operation's target placement, generation
/// `next_generation`, and placement epoch advanced by one (§12.3 step
/// 5, the `EpochAdvanced` phase). Advancing rather than carrying the
/// epoch is what makes a message fenced at the old epoch stale.
fn relocated_descriptor(
    pre: &RangeDescriptor,
    op: &LifecycleOperation,
) -> Result<RangeDescriptor, LifecycleError> {
    let t = &op.targets[0];
    let placement_epoch = op
        .placement_epoch
        .checked_add(1)
        .ok_or(LifecycleError::CounterExhausted)?;
    RangeDescriptor::new(
        pre.database_id,
        pre.partition_map_id,
        pre.routing_kind,
        pre.range_id,
        pre.start_key(),
        pre.end_key(),
        op.next_generation,
        Lifecycle::Active,
        Binding {
            cluster_id: pre.binding.cluster_id,
            partition_id: t.partition_id,
            partition_incarnation: t.partition_incarnation,
            placement_epoch,
        },
        pre.replicas(),
        pre.leaseholder,
    )
    .ok_or(LifecycleError::MalformedOperation)
}
