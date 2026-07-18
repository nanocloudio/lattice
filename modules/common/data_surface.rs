//! The `lattice.data` contract surface — RFC database foundation
//! §14.7-§14.14, §11.5, §26 phases 9-10, §21 invariants 15/16/17.
//!
//! Phases 9 and 10 separate relational compute from Lattice storage.
//! This file is the contract that boundary is made of: the operation
//! vocabulary, the request/response framing, the session and authority
//! model, cache-coherence keys, the bounded pushdown program, and the
//! placement/upgrade admission rules. Contracts only — no ports, no
//! I/O, no clocks, no module. It stands to `lattice_data_client` and
//! `lattice_data_anchor` exactly as `txn.rs` stands to the transaction
//! coordinator.
//!
//! ## 1. The surface is not a transparent FIFO (§28, §14.8)
//!
//! §28 rejects "treat remote database calls as transparent FIFOs"
//! because that composition "hides ambiguous delivery, stale routing,
//! cancellation, and transaction recovery that must remain explicit".
//! A FIFO gives a caller exactly two facts — bytes arrived, or the pipe
//! hung up — and neither is an outcome. Four rules replace them, and
//! each is an executable function here rather than a paragraph:
//!
//! - **Every retryable mutation carries a discovery identity.**
//!   [`admit_request`] REFUSES a mutation whose
//!   [`db_context::RequestContext::idempotency_key`] is zero. The
//!   refusal happens at admission, before the request is sent, because
//!   the alternative is sending it and hoping — and "hoping" is the
//!   FIFO behaviour §28 rejects. Given an admitted mutation,
//!   [`outcome_is_discoverable`] names *how* a lost response is
//!   resolved: [`Discoverability::ViaTransactionStatus`] for a
//!   transactional mutation (§13.2's home record IS the outcome), or
//!   [`Discoverability::ViaIdempotencyIdentity`] otherwise. It can
//!   only return [`Discoverability::Undiscoverable`] for a request
//!   admission already rejected, and a test pins that.
//! - **A disconnect is classified, not collapsed.**
//!   [`classify_disconnect`] returns [`Disconnect::Resumable`],
//!   [`Disconnect::MustRediscover`], or
//!   [`Disconnect::Indeterminate`] — never "closed". `Resumable`
//!   requires a continuation token that is well formed *for this
//!   operation* ([`continuation_is_well_formed`]); a scan whose token
//!   names another range or an unknown event format is not resumable
//!   and saying so is the whole point.
//! - **Cancellation is not abort.** [`classify_termination`] returns a
//!   typed [`Termination`]: `OperationCancelled` carries the
//!   transaction status *unchanged*, so a cancelled statement inside a
//!   `Pending` transaction is still `Pending`. Only an explicit abort
//!   produces `TransactionAborted`. §14.11's "client disconnect never
//!   decides a transaction outcome" is exactly this distinction, and
//!   [`cancellation_decides_transaction`] states it as `false`.
//! - **Scan resumption composes rather than re-derives.**
//!   [`scan_resume_valid`] delegates to [`db_ops::cursor_is_resumable`]
//!   and only adds the mapping from its answer to the
//!   [`DataRetryClass`] the response must carry. The retention-floor
//!   rule has exactly one implementation in this repository.
//!
//! ## 2. Composition (§8, §11.3, §13.2, §15)
//!
//! The request envelope is [`db_context::RequestContext`]. It already
//! carries tenant/database/keyspace, request identity, deadline,
//! consistency, durability, routing epoch, range generation,
//! transaction identity, read timestamp, idempotency key, and response
//! limits; restating them here would create a second definition of the
//! same fields and therefore a second place for them to drift.
//! [`DataRequest`] adds only what §14.8 asks for beyond it: session
//! identity and epoch, cancellation identity, partition-map revision,
//! range identity, physical partition incarnation, transaction epoch,
//! isolation, and the row/work/chunk bounds.
//!
//! Continuation tokens are [`db_ops::FeedCursor`] — §15's resume-token
//! field list is already database, partition-map identity, range
//! identity and generation, timestamp/revision, and format version,
//! which is precisely what a scan resume needs. Range identity comes
//! from [`db_ops::RangeDescriptor`] and transaction status from
//! [`txn::TxnStatus`].
//!
//! ## 3. Byte layouts
//!
//! Every wire record documents its layout above its `encode`. All
//! integers are little-endian and every record is
//! `[version:u16][payload_len:u16]` framed, so an unknown version or a
//! length disagreement fails closed before any field is read.
//!
//! Golden vectors: `tests/contract_data_surface.rs`. Every record here
//! crosses a version boundary between independently deployed graphs, so
//! a change that makes a vector fail is a format break requiring a
//! version bump plus an explicit migration — never an update to the
//! vector.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]
#![allow(
    clippy::duplicate_mod,
    reason = "this contract composes three committed contract files (db_context, db_ops, txn) whose own #[path] trees overlap; the alternative — re-declaring RequestContext, TxnStatus and friends locally — would fork committed golden formats, which is strictly worse. See the mount comment below for why the overlap is safe here."
)]

// Three contract files are mounted: `db_context` for the request
// envelope, `db_ops` for the continuation token and the range
// descriptor, and `txn` for the transaction status.
//
// `db_ops` mounts `internal_key` and `range_lifecycle` →
// `partition_map` → `db_context` beneath it, and `txn` mounts its own
// `db_context`, `internal_key`, and `mvcc`, so `db_context.rs` is
// compiled in three module paths. That is deliberate and safe here
// because nothing in this file passes a value from one tree into
// another: the only items taken from `db_ops` are `FeedCursor`,
// `RangeDescriptor`, and `cursor_is_resumable` (which compose only
// with each other), and the only item taken from `txn` is `TxnStatus`,
// a self-contained enum. `db_context`'s types are used solely as this
// file's own envelope.
//
// `relational.rs` is deliberately NOT mounted — it would be a second
// copy of the whole `db_ops` tree for one enum. Its `Isolation` is
// re-declared below with a documented byte-identity requirement
// instead, the same pattern `relational.rs` itself uses for the
// key-escape constants.
#[path = "db_context.rs"]
mod db_context;
#[path = "db_ops.rs"]
mod db_ops;
#[path = "txn.rs"]
mod txn;

#[allow(
    unused_imports,
    reason = "re-export surface: the data client and anchor compose this contract without a second #[path] include"
)]
pub use db_context::{Consistency, Durability, RequestContext, RetryClass};
#[allow(
    unused_imports,
    reason = "re-export surface: the data client and anchor compose this contract without a second #[path] include"
)]
pub use db_ops::{
    cursor_is_resumable, Binding, FeedCursor, FeedFailure, Lifecycle, RangeDescriptor,
    RefreshReason, Replica, ReplicaRole, Resumability, RoutingKind, Timestamp,
    FEED_EVENT_FORMAT_VERSION, MAX_KEY_BOUND_LEN, MAX_REPLICAS,
};
#[allow(
    unused_imports,
    reason = "re-export surface: transaction-outcome discovery is the caller's job and needs the status enum"
)]
pub use txn::TxnStatus;

// ══════════════════════════════════════════════════════════════════════
// 0. Versions and bounds
// ══════════════════════════════════════════════════════════════════════

/// Fixed leading tags on the `lattice.data` encodings.
///
/// These are CORRUPTION CHECKS, not versions. Each format has exactly
/// one layout; when a layout changes, it changes in place and every
/// deployment is rebuilt. Nothing here is ever bumped, and there is no
/// second layout for a decoder to tell apart from this one.
///
/// They differ per type on purpose: these records travel the same
/// transport and land in adjacent keyspaces, so a response decoded
/// where a request was expected must fail closed rather than read
/// plausible garbage out of the wrong field offsets.
pub const DATA_REQUEST_TAG: u16 = 0x5144; // "DQ"
/// See [`DATA_REQUEST_TAG`].
pub const DATA_RESPONSE_TAG: u16 = 0x5244; // "DR"
/// See [`DATA_REQUEST_TAG`].
pub const SESSION_DESCRIPTOR_TAG: u16 = 0x5353; // "SS"
/// See [`DATA_REQUEST_TAG`].
pub const CACHE_ENTRY_KEY_TAG: u16 = 0x4b43; // "CK"
/// See [`DATA_REQUEST_TAG`].
pub const PUSHDOWN_TAG: u16 = 0x4450; // "PD"
/// See [`DATA_REQUEST_TAG`].
pub const PLACEMENT_CONSTRAINTS_TAG: u16 = 0x4350; // "PC"

/// Maximum key-bound length inside a [`PushdownProgram`]. Deliberately
/// smaller than [`MAX_KEY_BOUND_LEN`]: a pushdown span is a query
/// fragment carried on every scan request, and bounding it separately
/// keeps the program under 512 bytes.
pub const MAX_PUSHDOWN_KEY_LEN: usize = 64;

/// Maximum length of one pushdown prefix filter.
pub const MAX_PUSHDOWN_PREFIX_LEN: usize = 32;

/// Maximum prefix filters in one program.
pub const MAX_PUSHDOWN_PREFIXES: usize = 4;

/// Maximum registered partial aggregates in one program.
pub const MAX_PUSHDOWN_AGGREGATES: usize = 4;

/// Maximum replicas a [`PlacementCandidate`] or [`RollingUpgradePlan`]
/// describes. Same bound as [`MAX_REPLICAS`] — placement decides the
/// contents of a range's replica set, so it cannot need more entries
/// than a descriptor can hold.
pub const MAX_PLACEMENT_REPLICAS: usize = MAX_REPLICAS;

/// §11.5: "The default data range has three voters on distinct eligible
/// failure domains."
pub const DEFAULT_VOTERS: u8 = 3;

/// §11.5: "Five voters may be used where the failure objective
/// justifies the write and resource cost."
pub const MAX_VOTERS: u8 = 5;

// ══════════════════════════════════════════════════════════════════════
// 1. Operation vocabulary (§14.8)
// ══════════════════════════════════════════════════════════════════════

/// The six operation families §14.8 names. The family is derived from
/// the opcode's high nibble rather than carried separately, so a
/// request cannot claim a family its opcode does not belong to.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpFamily {
    Database = 0x00,
    Routing = 0x01,
    Read = 0x02,
    Transaction = 0x03,
    Metadata = 0x04,
    Control = 0x05,
}

/// The `lattice.data` operation vocabulary (§14.8).
///
/// Opcodes are `0xFN` where `F` is the [`OpFamily`] discriminant, which
/// makes [`DataOp::family`] a shift and keeps future additions inside
/// their family's block. Discriminant `0x00` is deliberately unused so
/// a zeroed buffer never decodes as a valid operation.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataOp {
    // database
    Open = 0x01,
    Capabilities = 0x02,
    Close = 0x03,
    // routing
    ResolveSpan = 0x11,
    SubscribeTopology = 0x12,
    // read
    GetAt = 0x21,
    BatchGet = 0x22,
    ScanOpen = 0x23,
    ScanNext = 0x24,
    ScanClose = 0x25,
    /// §15/§26.6 record change feed: read the versions of a key span in
    /// a revision window `(from, to]`, resumable by advancing `from`
    /// past the highest revision returned. Carries a `KV_OP_SCAN_VERSIONS`
    /// body; answers `KV_RESULT_VERSIONS` (per-entry revision + kind).
    /// The poll-based shape a request/response surface can carry — a
    /// subscription's stream is `SubscribeTopology`/`SubscribeCatalog`.
    FeedRead = 0x26,
    // transaction
    Begin = 0x31,
    TxnGet = 0x32,
    TxnScan = 0x33,
    Write = 0x34,
    Prepare = 0x35,
    Commit = 0x36,
    Abort = 0x37,
    Status = 0x38,
    // metadata
    CatalogRevision = 0x41,
    SubscribeCatalog = 0x42,
    // control
    Cancel = 0x51,
    KeepAlive = 0x52,
    SessionClose = 0x53,
}

impl DataOp {
    /// Every operation, in opcode order. Tests iterate this to prove
    /// the family matrix is total — an operation added without a rule
    /// fails the matrix test rather than silently defaulting to
    /// "no requirements".
    pub const ALL: [Self; 24] = [
        Self::Open,
        Self::Capabilities,
        Self::Close,
        Self::ResolveSpan,
        Self::SubscribeTopology,
        Self::GetAt,
        Self::BatchGet,
        Self::ScanOpen,
        Self::ScanNext,
        Self::ScanClose,
        Self::FeedRead,
        Self::Begin,
        Self::TxnGet,
        Self::TxnScan,
        Self::Write,
        Self::Prepare,
        Self::Commit,
        Self::Abort,
        Self::Status,
        Self::CatalogRevision,
        Self::SubscribeCatalog,
        Self::Cancel,
        Self::KeepAlive,
        Self::SessionClose,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Open),
            0x02 => Some(Self::Capabilities),
            0x03 => Some(Self::Close),
            0x11 => Some(Self::ResolveSpan),
            0x12 => Some(Self::SubscribeTopology),
            0x21 => Some(Self::GetAt),
            0x22 => Some(Self::BatchGet),
            0x23 => Some(Self::ScanOpen),
            0x24 => Some(Self::ScanNext),
            0x25 => Some(Self::ScanClose),
            0x26 => Some(Self::FeedRead),
            0x31 => Some(Self::Begin),
            0x32 => Some(Self::TxnGet),
            0x33 => Some(Self::TxnScan),
            0x34 => Some(Self::Write),
            0x35 => Some(Self::Prepare),
            0x36 => Some(Self::Commit),
            0x37 => Some(Self::Abort),
            0x38 => Some(Self::Status),
            0x41 => Some(Self::CatalogRevision),
            0x42 => Some(Self::SubscribeCatalog),
            0x51 => Some(Self::Cancel),
            0x52 => Some(Self::KeepAlive),
            0x53 => Some(Self::SessionClose),
            _ => None,
        }
    }

    /// The family this opcode belongs to, derived from its high nibble.
    pub const fn family(self) -> OpFamily {
        match (self as u8) >> 4 {
            0x00 => OpFamily::Database,
            0x01 => OpFamily::Routing,
            0x02 => OpFamily::Read,
            0x03 => OpFamily::Transaction,
            0x04 => OpFamily::Metadata,
            // Unreachable for every `ALL` member; `Control` is the
            // safest fallback because control operations carry the
            // narrowest authority.
            _ => OpFamily::Control,
        }
    }

    /// Does this operation change durable state?
    ///
    /// `Begin` counts: §13.2's home transaction record is durable the
    /// moment a transaction exists, and a lost `begin` response leaves
    /// a real record behind that the caller must be able to find.
    /// `Abort` counts for the same reason — a durable abort is a
    /// decision, not a cleanup.
    pub const fn is_mutation(self) -> bool {
        matches!(
            self,
            Self::Begin | Self::Write | Self::Prepare | Self::Commit | Self::Abort
        )
    }

    /// Does this operation carry a continuation token *in the request*?
    /// `scan_next` resumes from one and `scan_close` releases the scan
    /// the token names.
    pub const fn consumes_continuation(self) -> bool {
        matches!(self, Self::ScanNext | Self::ScanClose)
    }

    /// May this operation's response carry a continuation token?
    pub const fn produces_continuation(self) -> bool {
        matches!(
            self,
            Self::ScanOpen
                | Self::ScanNext
                | Self::TxnScan
                | Self::SubscribeTopology
                | Self::SubscribeCatalog
        )
    }

    /// Is this a long-lived stream with no request deadline? §14.8
    /// bounds every operation, but a subscription's bound is its
    /// chunk/row budget and its cancellation identity, not a wall-clock
    /// deadline.
    pub const fn is_subscription(self) -> bool {
        matches!(self, Self::SubscribeTopology | Self::SubscribeCatalog)
    }

    /// Does this operation require an already-resolved route (§11.3)?
    /// `resolve_span` is what *produces* one, so it is excluded.
    pub const fn requires_resolved_route(self) -> bool {
        matches!(
            self,
            Self::GetAt
                | Self::BatchGet
                | Self::ScanOpen
                | Self::FeedRead
                | Self::TxnGet
                | Self::TxnScan
                | Self::Write
        )
    }

    /// Does this operation act inside an existing transaction?
    pub const fn requires_transaction(self) -> bool {
        matches!(
            self,
            Self::TxnGet
                | Self::TxnScan
                | Self::Write
                | Self::Prepare
                | Self::Commit
                | Self::Abort
                | Self::Status
        )
    }

    /// Does this operation return rows, and therefore need explicit
    /// cardinality and byte bounds (§21 invariant 14)?
    pub const fn returns_rows(self) -> bool {
        matches!(
            self,
            Self::GetAt
                | Self::BatchGet
                | Self::ScanOpen
                | Self::ScanNext
                | Self::FeedRead
                | Self::TxnGet
                | Self::TxnScan
        )
    }

    /// May a [`PushdownProgram`] accompany this operation? Only a scan
    /// open: §14.13's pushdown exists to avoid "one request per row",
    /// and a point read has no rows to avoid.
    pub const fn accepts_pushdown(self) -> bool {
        matches!(self, Self::ScanOpen)
    }
}

/// Isolation levels the surface represents (§14.8's "consistency,
/// isolation, read timestamp, and minimum durability").
///
/// [`db_context::RequestContext`] carries consistency, read timestamp,
/// and durability but not isolation, so this is the one §14.8 field the
/// envelope cannot supply.
///
/// **Byte-identical to `relational::Isolation` by requirement.** The
/// two enums describe the same thing at two layers and a connector
/// copies the byte across without translation; changing either
/// discriminant without the other is a format break.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    ReadCommitted = 1,
    RepeatableRead = 2,
    Serializable = 3,
}

impl Isolation {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::ReadCommitted),
            2 => Some(Self::RepeatableRead),
            3 => Some(Self::Serializable),
            _ => None,
        }
    }
}

// ── Retry classification ──────────────────────────────────────────────

/// §14.8's response classification: "stale route, contention,
/// unavailable authority, deadline, cancellation, unsupported
/// capability, and ambiguous client delivery".
///
/// This is a *superset* of [`db_context::RetryClass`]. The native
/// context classification has no cancellation class, because a local
/// request that is cancelled simply does not return; across the
/// separated boundary cancellation is an observable outcome that must
/// be distinguishable from an abort and from a deadline
/// (§28's rejected "transparent FIFO"). [`DataRetryClass::from_native`]
/// and [`DataRetryClass::to_native`] are the total/partial mapping
/// between the two, and `to_native` returning `None` for `Cancelled` is
/// the honest statement that the native enum cannot express it.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataRetryClass {
    /// Route metadata was stale; refresh descriptors and retry.
    StaleRoute = 0x01,
    /// Retryable contention (conflict, push, lock timeout).
    RetryableContention = 0x02,
    /// The owning authority (quorum, transaction home) is unavailable.
    UnavailableAuthority = 0x03,
    /// Deadline expired before a decisive outcome.
    DeadlineExpired = 0x04,
    /// The caller cancelled. NOT an abort: see [`classify_termination`].
    Cancelled = 0x05,
    /// Malformed input; retrying identically cannot succeed.
    Malformed = 0x06,
    /// Required capability not supported by this composition.
    UnsupportedCapability = 0x07,
    /// The mutation may have applied but the response was lost. The
    /// outcome is DISCOVERABLE — see [`outcome_is_discoverable`].
    AmbiguousDelivery = 0x08,
}

impl DataRetryClass {
    pub const ALL: [Self; 8] = [
        Self::StaleRoute,
        Self::RetryableContention,
        Self::UnavailableAuthority,
        Self::DeadlineExpired,
        Self::Cancelled,
        Self::Malformed,
        Self::UnsupportedCapability,
        Self::AmbiguousDelivery,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::StaleRoute),
            0x02 => Some(Self::RetryableContention),
            0x03 => Some(Self::UnavailableAuthority),
            0x04 => Some(Self::DeadlineExpired),
            0x05 => Some(Self::Cancelled),
            0x06 => Some(Self::Malformed),
            0x07 => Some(Self::UnsupportedCapability),
            0x08 => Some(Self::AmbiguousDelivery),
            _ => None,
        }
    }

    /// Widen a native §8 classification to the surface classification.
    /// Total — every native class has a surface counterpart.
    pub const fn from_native(native: RetryClass) -> Self {
        match native {
            RetryClass::StaleRoute => Self::StaleRoute,
            RetryClass::RetryableContention => Self::RetryableContention,
            RetryClass::UnavailableQuorum => Self::UnavailableAuthority,
            RetryClass::DeadlineExpired => Self::DeadlineExpired,
            RetryClass::Malformed => Self::Malformed,
            RetryClass::UnsupportedCapability => Self::UnsupportedCapability,
            RetryClass::IndeterminateDelivery => Self::AmbiguousDelivery,
        }
    }

    /// Narrow to the native §8 classification. Partial: `Cancelled` has
    /// no native counterpart and must NOT be folded into
    /// `DeadlineExpired` or `Malformed` — a caller that cancels needs
    /// to know its own cancellation came back, not a fabricated
    /// failure.
    pub const fn to_native(self) -> Option<RetryClass> {
        match self {
            Self::StaleRoute => Some(RetryClass::StaleRoute),
            Self::RetryableContention => Some(RetryClass::RetryableContention),
            Self::UnavailableAuthority => Some(RetryClass::UnavailableQuorum),
            Self::DeadlineExpired => Some(RetryClass::DeadlineExpired),
            Self::Cancelled => None,
            Self::Malformed => Some(RetryClass::Malformed),
            Self::UnsupportedCapability => Some(RetryClass::UnsupportedCapability),
            Self::AmbiguousDelivery => Some(RetryClass::IndeterminateDelivery),
        }
    }
}

/// The typed outcome of one request (§14.8 "request identity and typed
/// outcome").
///
/// Four values, not two. `Indeterminate` and `Cancelled` are separate
/// from `Refused` because collapsing them is exactly the FIFO failure:
/// an indeterminate mutation may have applied and a cancelled one was
/// stopped by its own caller, and neither is "it failed".
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The operation completed; the response fences describe it.
    Ok = 0x01,
    /// The operation did not run, and definitively did not apply.
    Refused = 0x02,
    /// The operation may or may not have applied. Discoverable.
    Indeterminate = 0x03,
    /// The caller's cancellation stopped it. No durable decision was
    /// made by the cancellation itself.
    Cancelled = 0x04,
}

impl Outcome {
    pub const ALL: [Self; 4] = [
        Self::Ok,
        Self::Refused,
        Self::Indeterminate,
        Self::Cancelled,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Ok),
            0x02 => Some(Self::Refused),
            0x03 => Some(Self::Indeterminate),
            0x04 => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// Which retry classes may accompany this outcome.
    ///
    /// The agreement is enforced in [`DataResponse::decode`], so a peer
    /// cannot ship `Ok` with `AmbiguousDelivery` attached (which a
    /// caller might read as success) or `Indeterminate` with
    /// `Malformed` attached (which a caller might read as "safe to
    /// retry blindly").
    pub const fn admits(self, retry: Option<DataRetryClass>) -> bool {
        match (self, retry) {
            (Self::Ok, None) => true,
            (Self::Indeterminate, Some(DataRetryClass::AmbiguousDelivery)) => true,
            (Self::Cancelled, Some(DataRetryClass::Cancelled)) => true,
            (Self::Refused, Some(c)) => !matches!(
                c,
                DataRetryClass::AmbiguousDelivery | DataRetryClass::Cancelled
            ),
            _ => false,
        }
    }
}

// ── Source-aware fences (§14.8, §21 invariant 11) ─────────────────────

/// Which authority produced a fence. §21 invariant 11: "a storage fence
/// is valid only with the correct source identity and epoch" — so the
/// kind travels with the identity and the epoch, never alone.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FenceSource {
    /// Range leader ReadIndex or apply fence.
    RangeLeader = 0x01,
    /// Declared follower fence (§14.10's "declared follower fence").
    Follower = 0x02,
    /// Explicitly stale read-only observer (§11.5).
    Observer = 0x03,
    /// The transaction home range (§13.2).
    TransactionHome = 0x04,
    /// Control-plane metadata authority (catalog, partition map).
    ControlPlane = 0x05,
}

impl FenceSource {
    pub const ALL: [Self; 5] = [
        Self::RangeLeader,
        Self::Follower,
        Self::Observer,
        Self::TransactionHome,
        Self::ControlPlane,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::RangeLeader),
            0x02 => Some(Self::Follower),
            0x03 => Some(Self::Observer),
            0x04 => Some(Self::TransactionHome),
            0x05 => Some(Self::ControlPlane),
            _ => None,
        }
    }

    /// May a response from this source claim
    /// [`Durability::ReplicatedDurable`]? §21 invariant 2: only the
    /// authority that observed the quorum durable proof may, and a
    /// follower or observer never observes one.
    pub const fn may_claim_replicated_durable(self) -> bool {
        matches!(self, Self::RangeLeader | Self::TransactionHome)
    }
}

/// A source-aware fence: kind, identity, and epoch together.
///
/// Wire layout (embedded in [`DataResponse`], integers LE):
///
/// ```text
/// [kind:u8][source_id:u32][source_epoch:u64]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceFence {
    pub kind: FenceSource,
    /// Node or group identity of the source.
    pub source_id: u32,
    /// The source's epoch (Raft term, lease epoch, control-plane
    /// revision). A fence with the right identity and a stale epoch is
    /// still stale.
    pub source_epoch: u64,
}

impl SourceFence {
    /// Encoded size inside a response payload.
    pub const WIRE_LEN: usize = 13;

    pub const EMPTY: SourceFence = SourceFence {
        kind: FenceSource::RangeLeader,
        source_id: 0,
        source_epoch: 0,
    };

    /// §21 invariant 11 made executable. A fence is accepted only when
    /// its kind AND identity match what the caller expected and its
    /// epoch is at least the caller's last observed epoch. All three
    /// conditions, because any one alone admits a stale or misattributed
    /// fence.
    pub const fn is_valid_for(
        &self,
        expected_kind: FenceSource,
        expected_source_id: u32,
        min_epoch: u64,
    ) -> bool {
        self.kind as u8 == expected_kind as u8
            && self.source_id == expected_source_id
            && self.source_id != 0
            && self.source_epoch >= min_epoch
            && self.source_epoch != 0
    }
}

// ══════════════════════════════════════════════════════════════════════
// 2. Request framing (§14.8)
// ══════════════════════════════════════════════════════════════════════

/// Request flag bits. Unknown bits are REJECTED by
/// [`DataRequest::decode`]: a flag this build does not understand may
/// carry a semantic the peer relies on, and silently ignoring it is how
/// two graphs end up disagreeing about what a request meant.
pub const REQ_FLAG_PUSHDOWN: u8 = 0b0000_0001;
/// The caller will accept a partial result and a continuation token
/// rather than a single bounded response.
pub const REQ_FLAG_ALLOW_CONTINUATION: u8 = 0b0000_0010;
/// Mask of every flag this version defines.
pub const REQ_FLAG_KNOWN: u8 = REQ_FLAG_PUSHDOWN | REQ_FLAG_ALLOW_CONTINUATION;

/// One `lattice.data` request (§14.8).
///
/// Wire layout (integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [op:u8][isolation:u8][flags:u8][reserved:u8]      4
/// [session_id:u64]                                  8
/// [session_epoch:u32]                               4
/// [cancellation_id:u64]                             8
/// [partition_map_revision:u32]                      4
/// [range_id:16]                                    16
/// [range_generation:u32]                            4
/// [partition_incarnation:u32]                       4
/// [transaction_epoch:u32]                           4
/// [max_rows:u32]                                    4
/// [max_work_units:u32]                              4
/// [max_chunks:u16]                                  2
/// [context: RequestContext::WIRE_LEN]              74
/// [continuation_present:u8]                         1
/// [continuation: FeedCursor::WIRE_LEN]             50
/// ```
///
/// The continuation slot is fixed-width and zero-filled when absent so
/// the record stays a fixed layout; `continuation_present` is the only
/// thing that decides whether the 50 bytes are parsed.
///
/// A [`PushdownProgram`] is NOT embedded. It is a variable-length
/// record carried alongside a `scan_open` and flagged by
/// [`REQ_FLAG_PUSHDOWN`], which keeps `DataRequest` fixed-size for the
/// 22 operations that never push anything down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataRequest {
    pub op: DataOp,
    pub isolation: Isolation,
    pub flags: u8,
    /// §14.9: the session directory maps `session_id` to a worker.
    /// Zero only on [`DataOp::Open`], which is what allocates one.
    pub session_id: u64,
    /// §14.9: stale `session_epoch` values are fenced. See
    /// [`fence_stale_session`].
    pub session_epoch: u32,
    /// §14.8's "cancellation identity". Distinct from `request_id`
    /// because one cancellation identity may cover a statement made of
    /// several requests.
    pub cancellation_id: u64,
    /// §11.3 partition-map revision the caller resolved against.
    pub partition_map_revision: u32,
    pub range_id: [u8; 16],
    pub range_generation: u32,
    /// §14.8's "physical partition incarnation" (§21 invariant 6).
    pub partition_incarnation: u32,
    /// Transaction epoch where applicable; 0 = none.
    pub transaction_epoch: u32,
    /// §14.8's "maximum rows"; 0 = unbounded, which
    /// [`admit_request`] refuses for row-returning operations.
    pub max_rows: u32,
    /// §14.8's "maximum work" in abstract units.
    pub max_work_units: u32,
    /// §14.8's "maximum response chunks".
    pub max_chunks: u16,
    /// The native envelope (§8) — tenant, database, keyspace, request
    /// identity, deadline, consistency, durability, routing epoch,
    /// range generation, transaction identity, read timestamp,
    /// idempotency key, and response limits.
    pub context: RequestContext,
    /// Resume token for `scan_next` / `scan_close`.
    pub continuation: Option<FeedCursor>,
}

/// Offset of the embedded [`RequestContext`] inside the payload.
const REQ_CONTEXT_OFF: usize = 4 + 8 + 4 + 8 + 4 + 16 + 4 + 4 + 4 + 4 + 4 + 2;
/// Offset of the continuation-present byte inside the payload.
const REQ_CONT_OFF: usize = REQ_CONTEXT_OFF + RequestContext::WIRE_LEN;
/// Payload length: fixed prefix + context + continuation slot.
const REQ_PAYLOAD_LEN: usize = REQ_CONT_OFF + 1 + FeedCursor::WIRE_LEN;

impl DataRequest {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + REQ_PAYLOAD_LEN;

    pub const EMPTY: DataRequest = DataRequest {
        op: DataOp::KeepAlive,
        isolation: Isolation::Serializable,
        flags: 0,
        session_id: 0,
        session_epoch: 0,
        cancellation_id: 0,
        partition_map_revision: 0,
        range_id: [0; 16],
        range_generation: 0,
        partition_incarnation: 0,
        transaction_epoch: 0,
        max_rows: 0,
        max_work_units: 0,
        max_chunks: 0,
        context: RequestContext {
            tenant_id: 0,
            database_id: 0,
            keyspace_id: 0,
            request_id: 0,
            deadline_unix_ms: 0,
            consistency: Consistency::Linearizable,
            durability: Durability::ReplicatedDurable,
            routing_epoch: 0,
            range_generation: 0,
            transaction_id: 0,
            read_timestamp: 0,
            idempotency_key: 0,
            max_response_bytes: 0,
            max_keys: 0,
        },
        continuation: None,
    };

    /// Does this request carry an accompanying pushdown program?
    pub const fn has_pushdown(&self) -> bool {
        self.flags & REQ_FLAG_PUSHDOWN != 0
    }

    /// Serialize to the versioned wire form.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&DATA_REQUEST_TAG.to_le_bytes());
        out[2..4].copy_from_slice(&(REQ_PAYLOAD_LEN as u16).to_le_bytes());
        let p = &mut out[4..Self::WIRE_LEN];
        p[0] = self.op as u8;
        p[1] = self.isolation as u8;
        p[2] = self.flags;
        p[3] = 0;
        p[4..12].copy_from_slice(&self.session_id.to_le_bytes());
        p[12..16].copy_from_slice(&self.session_epoch.to_le_bytes());
        p[16..24].copy_from_slice(&self.cancellation_id.to_le_bytes());
        p[24..28].copy_from_slice(&self.partition_map_revision.to_le_bytes());
        p[28..44].copy_from_slice(&self.range_id);
        p[44..48].copy_from_slice(&self.range_generation.to_le_bytes());
        p[48..52].copy_from_slice(&self.partition_incarnation.to_le_bytes());
        p[52..56].copy_from_slice(&self.transaction_epoch.to_le_bytes());
        p[56..60].copy_from_slice(&self.max_rows.to_le_bytes());
        p[60..64].copy_from_slice(&self.max_work_units.to_le_bytes());
        p[64..66].copy_from_slice(&self.max_chunks.to_le_bytes());
        self.context
            .encode(&mut p[REQ_CONTEXT_OFF..REQ_CONTEXT_OFF + RequestContext::WIRE_LEN])?;
        match self.continuation {
            Some(cursor) => {
                p[REQ_CONT_OFF] = 1;
                cursor.encode(&mut p[REQ_CONT_OFF + 1..REQ_CONT_OFF + 1 + FeedCursor::WIRE_LEN])?;
            }
            None => {
                p[REQ_CONT_OFF] = 0;
                for b in &mut p[REQ_CONT_OFF + 1..REQ_CONT_OFF + 1 + FeedCursor::WIRE_LEN] {
                    *b = 0;
                }
            }
        }
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown surface
    /// version, length disagreement, unknown opcode, unknown isolation,
    /// unknown flag bits, a non-zero reserved byte, an unreadable
    /// embedded context, an out-of-range continuation-present byte, or
    /// a present continuation that does not itself decode.
    ///
    /// Exact length, not "at least": a trailing byte means the sender
    /// framed something this build cannot see.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != DATA_REQUEST_TAG {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != REQ_PAYLOAD_LEN {
            return None;
        }
        let p = &src[4..];
        let op = DataOp::from_u8(p[0])?;
        let isolation = Isolation::from_u8(p[1])?;
        let flags = p[2];
        if flags & !REQ_FLAG_KNOWN != 0 || p[3] != 0 {
            return None;
        }
        let context = RequestContext::decode(
            &p[REQ_CONTEXT_OFF..REQ_CONTEXT_OFF + RequestContext::WIRE_LEN],
        )?;
        let continuation = match p[REQ_CONT_OFF] {
            0 => None,
            1 => Some(FeedCursor::decode(
                &p[REQ_CONT_OFF + 1..REQ_CONT_OFF + 1 + FeedCursor::WIRE_LEN],
            )?),
            _ => return None,
        };
        Some(Self {
            op,
            isolation,
            flags,
            session_id: u64::from_le_bytes(p[4..12].try_into().ok()?),
            session_epoch: u32::from_le_bytes(p[12..16].try_into().ok()?),
            cancellation_id: u64::from_le_bytes(p[16..24].try_into().ok()?),
            partition_map_revision: u32::from_le_bytes(p[24..28].try_into().ok()?),
            range_id: p[28..44].try_into().ok()?,
            range_generation: u32::from_le_bytes(p[44..48].try_into().ok()?),
            partition_incarnation: u32::from_le_bytes(p[48..52].try_into().ok()?),
            transaction_epoch: u32::from_le_bytes(p[52..56].try_into().ok()?),
            max_rows: u32::from_le_bytes(p[56..60].try_into().ok()?),
            max_work_units: u32::from_le_bytes(p[60..64].try_into().ok()?),
            max_chunks: u16::from_le_bytes(p[64..66].try_into().ok()?),
            context,
            continuation,
        })
    }
}

// ══════════════════════════════════════════════════════════════════════
// 3. Response framing (§14.8)
// ══════════════════════════════════════════════════════════════════════

/// The fenced observation a response reports (§14.8's "serving range
/// identity and generation; observed revision or resolved timestamp;
/// applied index and source-aware consistency/durability fences").
///
/// Split out of [`DataResponse`] as its own type because §21 invariant
/// 16 is stated over exactly these fields: "combined and separated
/// placements execute the same canonical command and return equivalent
/// fenced observations". Having the invariant's subject be a value you
/// can compare with `==` is the difference between an invariant and a
/// paragraph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedFences {
    pub serving_range_id: [u8; 16],
    pub serving_range_generation: u32,
    /// Apply-order position observed by this response.
    pub observed_revision: u64,
    /// MVCC timestamp the read resolved at.
    pub resolved_timestamp: Timestamp,
    /// Clustor applied index behind the answer.
    pub applied_index: u64,
    pub fence: SourceFence,
    /// The durability class actually ACHIEVED, which may be weaker than
    /// the class requested. §21 invariant 2 lives here.
    pub durability_achieved: Durability,
}

impl ObservedFences {
    pub const EMPTY: ObservedFences = ObservedFences {
        serving_range_id: [0; 16],
        serving_range_generation: 0,
        observed_revision: 0,
        resolved_timestamp: 0,
        applied_index: 0,
        fence: SourceFence::EMPTY,
        durability_achieved: Durability::Volatile,
    };

    /// §21 invariant 2: a response may claim
    /// [`Durability::ReplicatedDurable`] only from a source that can
    /// observe a quorum durable proof.
    pub const fn durability_claim_is_sound(&self) -> bool {
        match self.durability_achieved {
            Durability::ReplicatedDurable => self.fence.kind.may_claim_replicated_durable(),
            Durability::Volatile | Durability::ReplicatedVolatile => true,
        }
    }
}

/// One `lattice.data` response (§14.8).
///
/// Wire layout (integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [request_id:u64]                                  8
/// [session_id:u64]                                  8
/// [outcome:u8]                                      1
/// [retry_class:u8]          (0 = none)              1
/// [durability_achieved:u8]                          1
/// [reserved:u8]                                     1
/// [fence_kind:u8][fence_source_id:u32]
/// [fence_source_epoch:u64]                         13
/// [serving_range_id:16]                            16
/// [serving_range_generation:u32]                    4
/// [observed_revision:u64]                           8
/// [resolved_timestamp:u64]                          8
/// [applied_index:u64]                               8
/// [continuation_present:u8]                         1
/// [continuation: FeedCursor::WIRE_LEN]             50
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataResponse {
    /// §24's correlation identity, echoed from
    /// [`RequestContext::request_id`].
    pub request_id: u64,
    pub session_id: u64,
    pub outcome: Outcome,
    /// Present exactly when [`Outcome::admits`] says it must be.
    pub retry: Option<DataRetryClass>,
    pub fences: ObservedFences,
    /// RFC §14.2's schema generation, current when this response's
    /// command executed. Not part of [`ObservedFences`] because it is
    /// not compared for §21 invariant 16 equivalence — a schema
    /// generation is the same across placements of one range by
    /// construction, and folding it into the compared fences would make
    /// a legitimate mid-flight DDL look like a placement divergence.
    /// The compute-side catalog cache validates a cached name -> id
    /// mapping against it; zero means "no generation available", which
    /// disables the cache rather than trusting an unverified mapping.
    pub catalog_generation: u64,
    /// §14.8's "bounded continuation or resume token where applicable".
    pub continuation: Option<FeedCursor>,
}

const RESP_FENCE_OFF: usize = 8 + 8 + 4;
// … serving_range_id(16) + generation(4) + revision(8) + resolved_ts(8)
// + applied_index(8) + catalog_generation(8).
const RESP_CONT_OFF: usize = RESP_FENCE_OFF + SourceFence::WIRE_LEN + 16 + 4 + 8 + 8 + 8 + 8;
const RESP_PAYLOAD_LEN: usize = RESP_CONT_OFF + 1 + FeedCursor::WIRE_LEN;

impl DataResponse {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + RESP_PAYLOAD_LEN;

    pub const EMPTY: DataResponse = DataResponse {
        request_id: 0,
        session_id: 0,
        outcome: Outcome::Ok,
        retry: None,
        fences: ObservedFences::EMPTY,
        catalog_generation: 0,
        continuation: None,
    };

    /// Serialize to the versioned wire form. Refuses to encode a
    /// response whose outcome and retry class disagree — an inconsistent
    /// response must not exist even transiently on the wire.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN || !self.outcome.admits(self.retry) {
            return None;
        }
        out[0..2].copy_from_slice(&DATA_RESPONSE_TAG.to_le_bytes());
        out[2..4].copy_from_slice(&(RESP_PAYLOAD_LEN as u16).to_le_bytes());
        let p = &mut out[4..Self::WIRE_LEN];
        p[0..8].copy_from_slice(&self.request_id.to_le_bytes());
        p[8..16].copy_from_slice(&self.session_id.to_le_bytes());
        p[16] = self.outcome as u8;
        p[17] = match self.retry {
            Some(c) => c as u8,
            None => 0,
        };
        p[18] = self.fences.durability_achieved as u8;
        p[19] = 0;
        let f = RESP_FENCE_OFF;
        p[f] = self.fences.fence.kind as u8;
        p[f + 1..f + 5].copy_from_slice(&self.fences.fence.source_id.to_le_bytes());
        p[f + 5..f + 13].copy_from_slice(&self.fences.fence.source_epoch.to_le_bytes());
        let r = f + SourceFence::WIRE_LEN;
        p[r..r + 16].copy_from_slice(&self.fences.serving_range_id);
        p[r + 16..r + 20].copy_from_slice(&self.fences.serving_range_generation.to_le_bytes());
        p[r + 20..r + 28].copy_from_slice(&self.fences.observed_revision.to_le_bytes());
        p[r + 28..r + 36].copy_from_slice(&self.fences.resolved_timestamp.to_le_bytes());
        p[r + 36..r + 44].copy_from_slice(&self.fences.applied_index.to_le_bytes());
        p[r + 44..r + 52].copy_from_slice(&self.catalog_generation.to_le_bytes());
        match self.continuation {
            Some(cursor) => {
                p[RESP_CONT_OFF] = 1;
                cursor
                    .encode(&mut p[RESP_CONT_OFF + 1..RESP_CONT_OFF + 1 + FeedCursor::WIRE_LEN])?;
            }
            None => {
                p[RESP_CONT_OFF] = 0;
                for b in &mut p[RESP_CONT_OFF + 1..RESP_CONT_OFF + 1 + FeedCursor::WIRE_LEN] {
                    *b = 0;
                }
            }
        }
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// length disagreement, unknown outcome/retry/durability/fence
    /// discriminants, a non-zero reserved byte, an
    /// outcome/retry-class disagreement, or an unsound durability claim
    /// (§21 invariant 2).
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != DATA_RESPONSE_TAG {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != RESP_PAYLOAD_LEN {
            return None;
        }
        let p = &src[4..];
        let outcome = Outcome::from_u8(p[16])?;
        let retry = match p[17] {
            0 => None,
            b => Some(DataRetryClass::from_u8(b)?),
        };
        if !outcome.admits(retry) || p[19] != 0 {
            return None;
        }
        let durability_achieved = Durability::from_u8(p[18])?;
        let f = RESP_FENCE_OFF;
        let fence = SourceFence {
            kind: FenceSource::from_u8(p[f])?,
            source_id: u32::from_le_bytes(p[f + 1..f + 5].try_into().ok()?),
            source_epoch: u64::from_le_bytes(p[f + 5..f + 13].try_into().ok()?),
        };
        let r = f + SourceFence::WIRE_LEN;
        let fences = ObservedFences {
            serving_range_id: p[r..r + 16].try_into().ok()?,
            serving_range_generation: u32::from_le_bytes(p[r + 16..r + 20].try_into().ok()?),
            observed_revision: u64::from_le_bytes(p[r + 20..r + 28].try_into().ok()?),
            resolved_timestamp: u64::from_le_bytes(p[r + 28..r + 36].try_into().ok()?),
            applied_index: u64::from_le_bytes(p[r + 36..r + 44].try_into().ok()?),
            fence,
            durability_achieved,
        };
        let catalog_generation = u64::from_le_bytes(p[r + 44..r + 52].try_into().ok()?);
        if !fences.durability_claim_is_sound() {
            return None;
        }
        let continuation = match p[RESP_CONT_OFF] {
            0 => None,
            1 => Some(FeedCursor::decode(
                &p[RESP_CONT_OFF + 1..RESP_CONT_OFF + 1 + FeedCursor::WIRE_LEN],
            )?),
            _ => return None,
        };
        Some(Self {
            request_id: u64::from_le_bytes(p[0..8].try_into().ok()?),
            session_id: u64::from_le_bytes(p[8..16].try_into().ok()?),
            outcome,
            retry,
            fences,
            catalog_generation,
            continuation,
        })
    }
}

// ══════════════════════════════════════════════════════════════════════
// 3a. The two consistency vocabularies (§20)
// ══════════════════════════════════════════════════════════════════════

/// The adapter-requirement bytes that ride the native `MSG_KV_REQUEST`
/// envelope.
///
/// **Byte-identical to `types::REQ_*` by requirement**, and re-declared
/// here rather than mounted for the same reason [`Isolation`] is: this
/// file must not pull in the KV state machine's vocabulary wholesale.
/// `tests/live_data_surface.rs` mounts both and asserts they agree, so
/// the duplication cannot drift silently.
///
/// ## Why two vocabularies exist at all
///
/// They answer different questions. `REQ_*` asks what the ADAPTER
/// REQUIRES — serializable execution, a linearizable read, a CAS fence,
/// a transaction fence. [`Consistency`] asks what READ POLICY to serve
/// under. A CAS fence is not a read policy and `BoundedStale` is not
/// something an adapter requires.
///
/// They also overlap at exactly one value, `0x01`, and nowhere else,
/// which makes them look interchangeable in the one case anybody tests
/// first. Passing one where the other belongs cost a full debugging
/// session: `REQ_SERIALIZABLE` is `0x00`, which is not a [`Consistency`]
/// discriminant at all, so every statement crossing the separated
/// boundary was refused before it left the compute graph.
pub const REQ_SERIALIZABLE: u8 = 0x00;
/// See [`REQ_SERIALIZABLE`].
pub const REQ_LINEARIZABLE: u8 = 0x01;
/// See [`REQ_SERIALIZABLE`].
pub const REQ_CAS_FENCE: u8 = 0x02;
/// See [`REQ_SERIALIZABLE`].
pub const REQ_TXN_FENCE: u8 = 0x03;

/// The read policy an adapter requirement implies (compute → storage).
///
/// Only [`REQ_LINEARIZABLE`] asks for the fence. The router serves every
/// other requirement with an ordinary local read, so they map to the
/// WEAKEST policy — mapping them to something stronger would have the
/// separated path promise a guarantee the combined path never made, and
/// §21 invariant 16 compares those two paths.
///
/// `None` for a requirement this build does not know: a value we cannot
/// read might be asking for something stronger than what we would
/// substitute, so it is refused rather than defaulted.
pub const fn read_policy(requirement: u8) -> Option<Consistency> {
    match requirement {
        REQ_LINEARIZABLE => Some(Consistency::Linearizable),
        REQ_SERIALIZABLE | REQ_CAS_FENCE | REQ_TXN_FENCE => Some(Consistency::Eventual),
        _ => None,
    }
}

/// The adapter requirement a read policy implies (storage → router).
///
/// The inverse of [`read_policy`] on the values that round-trip, and
/// the two MUST stay inverses: a request that leaves compute as
/// `REQ_LINEARIZABLE` has to reach the router as `REQ_LINEARIZABLE`, or
/// the separated path silently serves a weaker read than the combined
/// one — which is exactly the difference §30's proof is measuring, so
/// the proof would be measuring the bug.
pub const fn adapter_requirement(policy: Consistency) -> u8 {
    match policy {
        Consistency::Linearizable => REQ_LINEARIZABLE,
        Consistency::BoundedStale | Consistency::Snapshot | Consistency::Eventual => {
            REQ_SERIALIZABLE
        }
    }
}

/// The isolation an adapter requirement implies. `REQ_SERIALIZABLE` and
/// the transaction fence both say the caller wants serializable
/// execution; the rest are satisfied by read-committed.
pub const fn isolation_for(requirement: u8) -> Isolation {
    match requirement {
        REQ_SERIALIZABLE | REQ_TXN_FENCE => Isolation::Serializable,
        _ => Isolation::ReadCommitted,
    }
}

// ══════════════════════════════════════════════════════════════════════
// 3b. Transport framing (§14.8)
// ══════════════════════════════════════════════════════════════════════

/// [`DataRequest`] and [`DataResponse`] are fixed-width records that say
/// what an operation *is*. Neither carries keys, values, or rows — and
/// deliberately so: the 23 operations share one envelope precisely
/// because the envelope is the part that does not vary. The bytes an
/// operation acts on vary in every dimension.
///
/// A frame is therefore the record plus one bounded payload:
///
/// ```text
/// request:  [DataRequest::WIRE_LEN][kv_op:u8][payload_len:u16][payload…]
/// response: [DataResponse::WIRE_LEN][kv_result:u8][payload_len:u16][payload…]
/// ```
///
/// ## Why the request carries a canonical KV opcode
///
/// §14.2 and §19 put the compile step in the *compute* layer: relational
/// and model capability modules "compile authoritative mutations into
/// canonical KV operations". By the time a request reaches this wire
/// that compilation has already happened, so the frame carries its
/// result rather than asking the storage side to re-derive it from a
/// [`DataOp`] that was never meant to encode it. `DataOp::Write` is one
/// opcode covering put, delete, CAS, and increment; a storage-side
/// guess between them would be a second compiler, in the one place that
/// must not have opinions about what the compute layer meant.
///
/// The anchor still cross-checks the pair — a non-mutating [`DataOp`]
/// carrying a mutating KV opcode is refused, because agreement between
/// the two is exactly what the caller could otherwise lie about. The
/// KV opcode vocabulary itself lives in `types.rs` and is deliberately
/// NOT mounted here: this contract describes the surface, not the
/// state machine behind it, and mounting the KV vocabulary would let
/// the two drift into one.
pub const DATA_FRAME_MAX_PAYLOAD: usize = 8192;

/// Bytes of framing a request adds beyond the payload.
pub const REQUEST_FRAME_HEADER_LEN: usize = DataRequest::WIRE_LEN + 1 + 2;
/// Bytes of framing a response adds beyond the payload.
pub const RESPONSE_FRAME_HEADER_LEN: usize = DataResponse::WIRE_LEN + 1 + 2;

/// Largest legal encoded request frame.
pub const REQUEST_FRAME_MAX_LEN: usize = REQUEST_FRAME_HEADER_LEN + DATA_FRAME_MAX_PAYLOAD;
/// Largest legal encoded response frame.
pub const RESPONSE_FRAME_MAX_LEN: usize = RESPONSE_FRAME_HEADER_LEN + DATA_FRAME_MAX_PAYLOAD;

/// A decoded request frame: the record, the compiled KV opcode, and the
/// operand bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestFrame<'a> {
    pub request: DataRequest,
    /// The canonical KV opcode the compute layer compiled to. Opaque
    /// here — see [`DATA_FRAME_MAX_PAYLOAD`] for why this contract does
    /// not know the vocabulary.
    pub kv_op: u8,
    pub payload: &'a [u8],
}

/// A decoded response frame: the record and the result bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResponseFrame<'a> {
    pub response: DataResponse,
    /// The canonical KV result code, mirroring the request frame's
    /// compiled opcode.
    ///
    /// [`Outcome`] and this byte answer different questions and
    /// collapsing them loses one of the answers. `Outcome::Ok` means the
    /// operation reached its authority and that authority answered;
    /// `kv_result` is *what it answered*. A read that found nothing and
    /// a read that found an empty value are both `Outcome::Ok` with an
    /// empty payload, and only this byte tells them apart.
    ///
    /// Zero (`KV_RESULT_OK`) when there was no canonical execution to
    /// report — an admission refusal, or an operation this anchor
    /// answered itself. The outcome is what distinguishes those; a
    /// result code for a command that never ran would be a fiction.
    pub kv_result: u8,
    pub payload: &'a [u8],
}

/// Encode a request frame. Returns the encoded length.
pub fn encode_request_frame(
    request: &DataRequest,
    kv_op: u8,
    payload: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    if payload.len() > DATA_FRAME_MAX_PAYLOAD {
        return None;
    }
    let total = REQUEST_FRAME_HEADER_LEN + payload.len();
    if out.len() < total {
        return None;
    }
    request.encode(&mut out[..DataRequest::WIRE_LEN])?;
    let p = DataRequest::WIRE_LEN;
    out[p] = kv_op;
    out[p + 1..p + 3].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    out[p + 3..total].copy_from_slice(payload);
    Some(total)
}

/// Decode a request frame.
///
/// Exact length, not "at least": the declared payload length must
/// account for every remaining byte. A frame with trailing bytes is one
/// this build cannot fully see, and reading the part it understands
/// while ignoring the rest is how two graphs come to disagree about
/// what a request meant.
pub fn decode_request_frame(src: &[u8]) -> Option<RequestFrame<'_>> {
    if src.len() < REQUEST_FRAME_HEADER_LEN {
        return None;
    }
    let request = DataRequest::decode(&src[..DataRequest::WIRE_LEN])?;
    let p = DataRequest::WIRE_LEN;
    let kv_op = src[p];
    // A zero opcode is not a vocabulary member in any of this
    // repository's opcode spaces; rejecting it here means a zeroed or
    // truncated-then-padded buffer cannot decode as a valid frame.
    if kv_op == 0 {
        return None;
    }
    let payload_len = u16::from_le_bytes(src[p + 1..p + 3].try_into().ok()?) as usize;
    if payload_len > DATA_FRAME_MAX_PAYLOAD {
        return None;
    }
    if src.len() != REQUEST_FRAME_HEADER_LEN + payload_len {
        return None;
    }
    Some(RequestFrame {
        request,
        kv_op,
        payload: &src[REQUEST_FRAME_HEADER_LEN..],
    })
}

/// Encode a response frame. Returns the encoded length.
///
/// Inherits [`DataResponse::encode`]'s refusal to serialize an
/// outcome/retry-class disagreement, so an inconsistent response cannot
/// reach the wire inside a frame either.
pub fn encode_response_frame(
    response: &DataResponse,
    kv_result: u8,
    payload: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    if payload.len() > DATA_FRAME_MAX_PAYLOAD {
        return None;
    }
    let total = RESPONSE_FRAME_HEADER_LEN + payload.len();
    if out.len() < total {
        return None;
    }
    response.encode(&mut out[..DataResponse::WIRE_LEN])?;
    let p = DataResponse::WIRE_LEN;
    out[p] = kv_result;
    out[p + 1..p + 3].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    out[p + 3..total].copy_from_slice(payload);
    Some(total)
}

/// Decode a response frame. Exact-length for the same reason
/// [`decode_request_frame`] is.
pub fn decode_response_frame(src: &[u8]) -> Option<ResponseFrame<'_>> {
    if src.len() < RESPONSE_FRAME_HEADER_LEN {
        return None;
    }
    let response = DataResponse::decode(&src[..DataResponse::WIRE_LEN])?;
    let p = DataResponse::WIRE_LEN;
    let kv_result = src[p];
    let payload_len = u16::from_le_bytes(src[p + 1..p + 3].try_into().ok()?) as usize;
    if payload_len > DATA_FRAME_MAX_PAYLOAD {
        return None;
    }
    if src.len() != RESPONSE_FRAME_HEADER_LEN + payload_len {
        return None;
    }
    Some(ResponseFrame {
        response,
        kv_result,
        payload: &src[RESPONSE_FRAME_HEADER_LEN..],
    })
}

// ── §21 invariant 16: combined and separated equivalence ──────────────

/// Where the canonical command actually executed (§14.7). The two
/// compositions differ only in whether the contract was serialized.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    /// Relational modules and range services in one graph; a local fast
    /// path may skip serialization.
    Combined = 1,
    /// Relational compute graph talking to a storage graph over
    /// `lattice.data`.
    Separated = 2,
}

/// §21 invariant 16 made executable.
///
/// Two executions of the *same* canonical request are equivalent when
/// their fenced observations are field-for-field identical. Nothing
/// weaker: a combined execution that reported a stronger durability
/// class, a later applied index, or a different serving generation
/// would be §14.7's forbidden "local fast path [that] gains stronger
/// semantics than the remote path".
pub const fn fences_equivalent(a: &ObservedFences, b: &ObservedFences) -> bool {
    a.serving_range_generation == b.serving_range_generation
        && a.observed_revision == b.observed_revision
        && a.resolved_timestamp == b.resolved_timestamp
        && a.applied_index == b.applied_index
        && a.fence.kind as u8 == b.fence.kind as u8
        && a.fence.source_id == b.fence.source_id
        && a.fence.source_epoch == b.fence.source_epoch
        && a.durability_achieved as u8 == b.durability_achieved as u8
        && same_range(&a.serving_range_id, &b.serving_range_id)
}

/// Byte-wise range identity comparison usable from a `const fn`.
const fn same_range(a: &[u8; 16], b: &[u8; 16]) -> bool {
    let mut i = 0;
    while i < 16 {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// §14.7's local-fast-path rule: the fast path must "consume and
/// produce the same versioned contract and must not gain stronger
/// semantics than the remote path".
///
/// Takes the SAME request and the two responses, so a caller cannot
/// compare responses to different commands and call it equivalence.
pub fn placement_equivalence_holds(
    request: &DataRequest,
    combined: &DataResponse,
    separated: &DataResponse,
) -> bool {
    combined.request_id == request.context.request_id
        && separated.request_id == request.context.request_id
        && combined.outcome as u8 == separated.outcome as u8
        && fences_equivalent(&combined.fences, &separated.fences)
}

// ══════════════════════════════════════════════════════════════════════
// 4. Admission — the family matrix and invariant 17's refusal
// ══════════════════════════════════════════════════════════════════════

/// Why a request is refused before it is sent.
///
/// Every variant is a *required field* rule from §14.8's request list
/// or a bound from §21 invariant 14. They are separate variants rather
/// than one `Malformed` because the client's recovery differs: a
/// missing route is a `resolve_span` away, a missing idempotency
/// identity is a caller bug, and a missing bound is a policy error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    /// Opcode's family does not match its declared use.
    UnknownOperation,
    /// Every operation except `open` needs an established session.
    MissingSessionIdentity,
    /// `open` allocates the session; presenting one is a protocol error.
    SessionIdentityOnOpen,
    /// A session identity without an epoch cannot be fenced (§14.9).
    MissingSessionEpoch,
    MissingTenant,
    MissingDatabase,
    /// §24: every request is correlatable.
    MissingRequestIdentity,
    /// §11.3: the operation acts on a resolved route.
    MissingRoute {
        have_generation: u32,
    },
    /// `scan_next` / `scan_close` without their token.
    MissingContinuation,
    /// A continuation on an operation that cannot consume one.
    UnexpectedContinuation,
    MissingTransactionIdentity,
    /// `begin` allocates the transaction; presenting one is an error.
    TransactionIdentityOnBegin,
    /// **§21 invariant 17.** A retryable mutation with no idempotency
    /// identity has no discoverable outcome, so it is refused HERE and
    /// never sent.
    MissingIdempotencyIdentity {
        op: DataOp,
    },
    /// A bounded operation with no deadline (§8).
    MissingDeadline,
    /// `cancel` with nothing to cancel.
    MissingCancellationIdentity,
    /// §21 invariant 14: an unbounded result is not a weaker mode, it is
    /// a refusal.
    UnboundedRows,
    UnboundedResponseBytes,
    UnboundedWork,
    /// Pushdown flagged on an operation that does not accept one.
    PushdownNotPermitted {
        op: DataOp,
    },
    /// [`Consistency::Snapshot`] without the timestamp it must serve at.
    MissingReadTimestamp,
}

impl AdmissionError {
    /// The retry class a refusal for this reason must carry.
    ///
    /// An admission failure always produces [`Outcome::Refused`] — the
    /// request never ran, so there is nothing to be indeterminate
    /// about — and [`Outcome::admits`] permits any class but
    /// `AmbiguousDelivery` and `Cancelled` alongside it. This function
    /// exists so the anchor does not pick one: the classification a
    /// caller retries on is part of the contract, not a local decision
    /// at the refusal site, and two refusal sites choosing differently
    /// for the same reason is a bug the caller cannot see.
    ///
    /// Three reasons are not `Malformed`:
    ///
    /// - [`AdmissionError::MissingRoute`] is [`DataRetryClass::StaleRoute`].
    ///   The caller's request is well formed; its routing knowledge is
    ///   not. Telling it `Malformed` would send it to fix a message
    ///   that has nothing wrong with it, when refreshing descriptors
    ///   and retrying is what actually succeeds.
    /// - [`AdmissionError::PushdownNotPermitted`] is
    ///   [`DataRetryClass::UnsupportedCapability`]: the caller asked
    ///   for a capability this operation does not have, which is a
    ///   negotiation answer rather than a syntax error.
    /// - [`AdmissionError::MissingIdempotencyIdentity`] stays
    ///   `Malformed` on purpose even though it is §21 invariant 17's
    ///   refusal. Retrying identically genuinely cannot succeed, and
    ///   that is precisely what `Malformed` means.
    pub const fn refusal_class(&self) -> DataRetryClass {
        match self {
            Self::MissingRoute { .. } => DataRetryClass::StaleRoute,
            Self::PushdownNotPermitted { .. } => DataRetryClass::UnsupportedCapability,
            _ => DataRetryClass::Malformed,
        }
    }
}

/// Admit a request, or refuse it with the reason (§14.8, §21 invariants
/// 14 and 17).
///
/// ### Check order is part of the contract
///
/// Identity and framing first, then routing, then transaction identity,
/// then the idempotency rule, then bounds. The idempotency rule is
/// checked *before* bounds so that a mutation with neither an
/// idempotency identity nor a row bound reports the discovery failure —
/// the one that would have made its outcome unknowable — rather than
/// the bound.
pub fn admit_request(request: &DataRequest) -> Result<(), AdmissionError> {
    let op = request.op;
    let ctx = &request.context;

    // ── Session identity ──
    match op {
        DataOp::Open => {
            if request.session_id != 0 {
                return Err(AdmissionError::SessionIdentityOnOpen);
            }
        }
        _ => {
            if request.session_id == 0 {
                return Err(AdmissionError::MissingSessionIdentity);
            }
            if request.session_epoch == 0 {
                return Err(AdmissionError::MissingSessionEpoch);
            }
        }
    }

    // ── Envelope identity ──
    if ctx.tenant_id == 0 {
        return Err(AdmissionError::MissingTenant);
    }
    if ctx.database_id == 0 {
        return Err(AdmissionError::MissingDatabase);
    }
    if ctx.request_id == 0 {
        return Err(AdmissionError::MissingRequestIdentity);
    }

    // ── Routing (§11.3) ──
    if op.requires_resolved_route()
        && (request.range_id == [0u8; 16] || request.range_generation == 0)
    {
        return Err(AdmissionError::MissingRoute {
            have_generation: request.range_generation,
        });
    }

    // ── Continuation ──
    if op.consumes_continuation() && request.continuation.is_none() {
        return Err(AdmissionError::MissingContinuation);
    }
    if !op.consumes_continuation() && request.continuation.is_some() {
        return Err(AdmissionError::UnexpectedContinuation);
    }

    // ── Transaction identity (§13.2) ──
    if op == DataOp::Begin && ctx.transaction_id != 0 {
        return Err(AdmissionError::TransactionIdentityOnBegin);
    }
    if op.requires_transaction() && ctx.transaction_id == 0 {
        return Err(AdmissionError::MissingTransactionIdentity);
    }

    // ── §21 invariant 17: discoverability at admission ──
    //
    // §14.8: "idempotency identity for every retryable mutation". Not
    // "for every mutation without a transaction": a transaction
    // identity makes the *transaction's* outcome discoverable, but a
    // retried `write` inside a live transaction still needs to be
    // distinguishable from a duplicate. One rule, no exceptions, so
    // there is no case where a caller has to reason about which kind of
    // identity it happens to have.
    if op.is_mutation() && ctx.idempotency_key == 0 {
        return Err(AdmissionError::MissingIdempotencyIdentity { op });
    }

    // ── Deadline and cancellation ──
    if !op.is_subscription() && op != DataOp::KeepAlive && ctx.deadline_unix_ms == 0 {
        return Err(AdmissionError::MissingDeadline);
    }
    if op == DataOp::Cancel && request.cancellation_id == 0 {
        return Err(AdmissionError::MissingCancellationIdentity);
    }

    // ── Bounds (§21 invariant 14) ──
    if op.returns_rows() {
        if request.max_rows == 0 {
            return Err(AdmissionError::UnboundedRows);
        }
        if ctx.max_response_bytes == 0 {
            return Err(AdmissionError::UnboundedResponseBytes);
        }
        if request.max_work_units == 0 {
            return Err(AdmissionError::UnboundedWork);
        }
    }

    // ── Pushdown placement (§14.13) ──
    if request.has_pushdown() && !op.accepts_pushdown() {
        return Err(AdmissionError::PushdownNotPermitted { op });
    }

    // ── Snapshot reads must name their timestamp (§20) ──
    if ctx.consistency as u8 == Consistency::Snapshot as u8
        && op.returns_rows()
        && ctx.read_timestamp == 0
    {
        return Err(AdmissionError::MissingReadTimestamp);
    }

    Ok(())
}

// ══════════════════════════════════════════════════════════════════════
// 5. Not a transparent FIFO (§28, §14.8, §21 invariant 17)
// ══════════════════════════════════════════════════════════════════════

/// One operation the caller believes is in flight, reduced to the
/// identities that decide what a failure means.
///
/// This exists so [`outcome_is_discoverable`] and
/// [`classify_disconnect`] take the same subject: the question "what
/// does losing this mean?" has one input, not two overlapping ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InflightOp {
    pub op: DataOp,
    pub session_id: u64,
    pub session_epoch: u32,
    pub database_id: u32,
    pub range_id: [u8; 16],
    pub range_generation: u32,
    /// 0 = not transactional.
    pub transaction_id: u64,
    /// 0 = no idempotency identity. For a mutation this is the state
    /// [`admit_request`] refuses.
    pub idempotency_key: u64,
    pub cancellation_id: u64,
    /// The token the caller holds for a resumable stream.
    pub continuation: Option<FeedCursor>,
}

impl InflightOp {
    pub const EMPTY: InflightOp = InflightOp {
        op: DataOp::KeepAlive,
        session_id: 0,
        session_epoch: 0,
        database_id: 0,
        range_id: [0; 16],
        range_generation: 0,
        transaction_id: 0,
        idempotency_key: 0,
        cancellation_id: 0,
        continuation: None,
    };

    /// Project a request into the in-flight record. The client keeps
    /// this, not the whole request, so a pending-operation table stays
    /// small and bounded.
    pub fn from_request(request: &DataRequest) -> Self {
        Self {
            op: request.op,
            session_id: request.session_id,
            session_epoch: request.session_epoch,
            database_id: request.context.database_id,
            range_id: request.range_id,
            range_generation: request.range_generation,
            transaction_id: request.context.transaction_id,
            idempotency_key: request.context.idempotency_key,
            cancellation_id: request.cancellation_id,
            continuation: request.continuation,
        }
    }
}

/// How the transport failed. Kept as an input because the *same*
/// operation is discoverable in different ways depending on whether the
/// request was known-unsent or possibly-applied — and collapsing the
/// two is the FIFO mistake.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportFailure {
    /// The request provably never left the client (send failed, session
    /// already fenced). Nothing to discover; it did not apply.
    NeverSent = 1,
    /// The request was sent and no response arrived. THIS is the case
    /// that makes invariant 17 necessary.
    ResponseLost = 2,
    /// The session was fenced or closed underneath the operation.
    SessionLost = 3,
}

impl TransportFailure {
    pub const ALL: [Self; 3] = [Self::NeverSent, Self::ResponseLost, Self::SessionLost];
}

/// How a caller finds out what actually happened (§14.8, §21 invariant
/// 17).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Discoverability {
    /// Nothing to discover: the operation could not have changed state,
    /// or provably never left the client. Retry freely.
    NotRequired,
    /// Ask the transaction home range (§13.2). Its `TxnStatus` IS the
    /// outcome; there is no other place to look.
    ViaTransactionStatus { transaction_id: u64 },
    /// Look the idempotency identity up in the owning range. Applies to
    /// a non-transactional retryable mutation.
    ViaIdempotencyIdentity { idempotency_key: u64 },
    /// **Must never be produced for an admitted request.** A mutation
    /// with no discovery identity at all — precisely what
    /// [`AdmissionError::MissingIdempotencyIdentity`] prevents. It is a
    /// variant rather than a panic so the impossible case is a value a
    /// test can assert never occurs downstream of [`admit_request`].
    Undiscoverable { op: DataOp },
}

/// §21 invariant 17 made executable: "every remote mutation is
/// idempotently identifiable and its outcome remains discoverable after
/// compute or transport failure".
///
/// A lost response for a mutation is never "assume it failed" and never
/// "assume it succeeded". It is one of two lookups, and which one
/// depends only on whether the mutation was transactional.
///
/// Note the ordering: the transaction status is preferred over the
/// idempotency identity when both exist, because §13.2's home record is
/// the *authority* for the outcome while the idempotency identity only
/// tells you whether this particular request was applied.
pub fn outcome_is_discoverable(
    op: &InflightOp,
    transport_failure: TransportFailure,
) -> Discoverability {
    if !op.op.is_mutation() {
        return Discoverability::NotRequired;
    }
    if transport_failure == TransportFailure::NeverSent {
        return Discoverability::NotRequired;
    }
    if op.op.requires_transaction() && op.transaction_id != 0 {
        return Discoverability::ViaTransactionStatus {
            transaction_id: op.transaction_id,
        };
    }
    if op.idempotency_key != 0 {
        return Discoverability::ViaIdempotencyIdentity {
            idempotency_key: op.idempotency_key,
        };
    }
    Discoverability::Undiscoverable { op: op.op }
}

// ── Disconnect classification ─────────────────────────────────────────

/// What the caller must refresh before it can proceed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rediscovery {
    /// Refetch descriptors (§11.3) and retry.
    Route,
    /// Ask the home range for the transaction's status (§13.2).
    TransactionOutcome { transaction_id: u64 },
    /// Look up the idempotency identity in the owning range.
    IdempotentOutcome { idempotency_key: u64 },
    /// Refetch the catalog revision and re-subscribe (§14.12).
    CatalogRevision,
    /// Re-open the session; the old one is fenced (§14.9).
    Session { session_id: u64 },
}

/// The answer to "the connection dropped — now what?" (§14.8's "network
/// loss cannot be reduced to FIFO hangup").
///
/// Three values, and the boundary between them is the contract:
/// `Resumable` is a promise that no rows are skipped, `MustRediscover`
/// is a promise that a definite answer exists somewhere, and
/// `Indeterminate` is the honest admission that neither holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disconnect {
    /// Continue from the token. Only ever returned when the token is
    /// well formed for THIS operation.
    Resumable { from: FeedCursor },
    /// A definite answer exists; go and get it.
    MustRediscover(Rediscovery),
    /// No resume point and no discovery identity.
    Indeterminate { retry: DataRetryClass },
}

/// Is this continuation token usable for this operation?
///
/// Structural only — it answers "is this token about this operation at
/// all", not "is the history still there". The second question needs a
/// descriptor and a retention floor and is [`scan_resume_valid`]'s job.
/// Keeping them apart matters: a token that fails HERE was never a
/// resume point, while a token that fails there was one and has expired.
pub fn continuation_is_well_formed(cursor: &FeedCursor, op: &InflightOp) -> bool {
    cursor.range_id != [0u8; 16]
        && cursor.format_version == FEED_EVENT_FORMAT_VERSION
        && cursor.database_id == op.database_id
        && cursor.range_id == op.range_id
        && cursor.range_generation == op.range_generation
}

/// Classify a disconnect (§14.8, §28).
///
/// ### Order of reasoning
///
/// 1. A mutation's outcome discovery outranks everything: an
///    undecided transaction is the one thing a caller must never guess
///    about.
/// 2. A stream with a well-formed token resumes.
/// 3. A route-dependent read refreshes its route.
/// 4. A subscription refreshes its source.
/// 5. Anything left is `Indeterminate`, which for a session-scoped
///    operation means re-opening the session.
///
/// `Resumable` is never returned for an operation whose token is
/// missing or ill formed — that is the property the seeded property
/// test in `tests/contract_data_surface.rs` exists to hold.
pub fn classify_disconnect(op: &InflightOp) -> Disconnect {
    // 1. Mutations: discover, never guess.
    if op.op.is_mutation() {
        return match outcome_is_discoverable(op, TransportFailure::ResponseLost) {
            Discoverability::ViaTransactionStatus { transaction_id } => {
                Disconnect::MustRediscover(Rediscovery::TransactionOutcome { transaction_id })
            }
            Discoverability::ViaIdempotencyIdentity { idempotency_key } => {
                Disconnect::MustRediscover(Rediscovery::IdempotentOutcome { idempotency_key })
            }
            // An unadmitted mutation. There is nothing to look up, and
            // saying so is the only honest answer.
            Discoverability::Undiscoverable { .. } | Discoverability::NotRequired => {
                Disconnect::Indeterminate {
                    retry: DataRetryClass::AmbiguousDelivery,
                }
            }
        };
    }

    // 2. Streams with a usable token.
    if op.op.consumes_continuation() || op.op.produces_continuation() {
        if let Some(cursor) = op.continuation {
            if continuation_is_well_formed(&cursor, op) {
                return Disconnect::Resumable { from: cursor };
            }
        }
        // A stream without a usable token is not resumable. Topology
        // and catalog subscriptions restart from their source; a scan
        // restarts from its route.
        return match op.op {
            DataOp::SubscribeCatalog => Disconnect::MustRediscover(Rediscovery::CatalogRevision),
            DataOp::SubscribeTopology => Disconnect::MustRediscover(Rediscovery::Route),
            _ => Disconnect::MustRediscover(Rediscovery::Route),
        };
    }

    // 3. Route-dependent reads.
    if op.op.requires_resolved_route() {
        return Disconnect::MustRediscover(Rediscovery::Route);
    }

    // 4. Metadata.
    if op.op == DataOp::CatalogRevision {
        return Disconnect::MustRediscover(Rediscovery::CatalogRevision);
    }

    // 5. Session-scoped remainder. `status` is special: it is a pure
    //    lookup of a durable record, so it is always re-askable.
    match op.op {
        DataOp::Status => Disconnect::MustRediscover(Rediscovery::TransactionOutcome {
            transaction_id: op.transaction_id,
        }),
        DataOp::Open => Disconnect::Indeterminate {
            retry: DataRetryClass::UnavailableAuthority,
        },
        _ => Disconnect::MustRediscover(Rediscovery::Session {
            session_id: op.session_id,
        }),
    }
}

// ── Cancellation is not abort (§14.8, §14.11) ─────────────────────────

/// What ended an operation.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminationSignal {
    /// The caller sent `control.cancel`.
    ClientCancel = 1,
    /// The request deadline expired.
    DeadlineExpired = 2,
    /// The transport dropped.
    TransportLoss = 3,
    /// The caller sent `transaction.abort`.
    ExplicitAbort = 4,
}

impl TerminationSignal {
    pub const ALL: [Self; 4] = [
        Self::ClientCancel,
        Self::DeadlineExpired,
        Self::TransportLoss,
        Self::ExplicitAbort,
    ];
}

/// The typed proof that cancellation is not abort.
///
/// `OperationCancelled` carries the transaction status **unchanged**.
/// That is the whole content of §14.11's "client disconnect never
/// decides a transaction outcome": stopping an operation is a local
/// event, and deciding a transaction is a durable replicated one. The
/// two are different variants so no caller can accidentally treat one
/// as the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Termination {
    /// The operation stopped. The transaction, if any, is untouched.
    OperationCancelled {
        signal: TerminationSignal,
        /// The status as it was and still is.
        transaction_status: Option<TxnStatus>,
        retry: DataRetryClass,
    },
    /// A durable abort decision was recorded in the home range. Terminal.
    TransactionAborted { transaction_id: u64 },
}

impl Termination {
    /// Did this termination decide the transaction's outcome?
    pub const fn decided_transaction(&self) -> bool {
        matches!(self, Self::TransactionAborted { .. })
    }
}

/// §14.11's rule as a named constant: no cancellation, deadline, or
/// disconnect decides a transaction outcome. Only an explicit abort
/// that reaches the home range does.
pub const fn cancellation_decides_transaction() -> bool {
    false
}

/// Classify what a termination signal did (§14.8, §14.11).
///
/// `transaction_status` is the status observed *before* the signal.
/// Only [`TerminationSignal::ExplicitAbort`] produces
/// [`Termination::TransactionAborted`], and even then only when the
/// transaction was still undecided — aborting a `Committed` transaction
/// does not un-commit it (§21 invariant 9), so it reports the cancelled
/// operation with the terminal status intact.
pub fn classify_termination(
    signal: TerminationSignal,
    transaction_status: Option<TxnStatus>,
    transaction_id: u64,
) -> Termination {
    match signal {
        TerminationSignal::ExplicitAbort => match transaction_status {
            Some(status) if status.is_undecided() && transaction_id != 0 => {
                Termination::TransactionAborted { transaction_id }
            }
            other => Termination::OperationCancelled {
                signal,
                transaction_status: other,
                retry: DataRetryClass::RetryableContention,
            },
        },
        TerminationSignal::ClientCancel => Termination::OperationCancelled {
            signal,
            transaction_status,
            retry: DataRetryClass::Cancelled,
        },
        TerminationSignal::DeadlineExpired => Termination::OperationCancelled {
            signal,
            transaction_status,
            retry: DataRetryClass::DeadlineExpired,
        },
        TerminationSignal::TransportLoss => Termination::OperationCancelled {
            signal,
            transaction_status,
            retry: DataRetryClass::AmbiguousDelivery,
        },
    }
}

// ── Scan resumption (§15, §18, composed) ──────────────────────────────

/// The answer [`scan_resume_valid`] gives, paired with the
/// [`DataRetryClass`] the response must carry.
///
/// The [`Resumability`] half is [`db_ops::cursor_is_resumable`]'s answer
/// verbatim; the retry class is the only thing this layer adds, because
/// a change feed has no `lattice.data` response to classify and a
/// separated scan does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanResume {
    /// Resume from the token as-is.
    Resume,
    /// Refetch metadata first; no rows are skipped.
    RefreshThenResume {
        reason: RefreshReason,
        retry: DataRetryClass,
    },
    /// The scan is broken and must restart from a fresh open.
    Broken {
        failure: FeedFailure,
        retry: DataRetryClass,
    },
}

impl ScanResume {
    pub const fn is_resumable(&self) -> bool {
        matches!(self, Self::Resume)
    }
}

/// Validate a scan resume (§14.8's "resume scans where permitted").
///
/// Composes [`db_ops::cursor_is_resumable`] rather than restating it.
/// The retention-floor rule, the check ORDER, and the refusal to
/// downgrade an unrecoverable cursor into a later resume point all live
/// in `db_ops` and have exactly one implementation in this repository;
/// duplicating them here would create a second place for the most
/// safety-critical comparison in the change-feed contract to drift.
pub fn scan_resume_valid(
    cursor: &FeedCursor,
    current: &RangeDescriptor,
    retention_floor: Timestamp,
) -> ScanResume {
    match cursor_is_resumable(cursor, current, retention_floor) {
        Resumability::Ok => ScanResume::Resume,
        Resumability::NeedsRefresh(reason) => ScanResume::RefreshThenResume {
            reason,
            // A generation move or a retired range IS a stale route:
            // the caller's remedy is `routing.resolve_span`.
            retry: DataRetryClass::StaleRoute,
        },
        Resumability::Unrecoverable(failure) => ScanResume::Broken {
            failure,
            retry: match failure {
                // The history or the event format this token needs is
                // gone from this composition — a capability statement,
                // not a caller error.
                FeedFailure::BelowRetentionFloor { .. }
                | FeedFailure::UnknownFormatVersion { .. } => DataRetryClass::UnsupportedCapability,
                // The token was compared against the wrong thing.
                // Retrying it identically cannot help.
                FeedFailure::DatabaseMismatch { .. }
                | FeedFailure::PartitionMapMismatch { .. }
                | FeedFailure::RangeMismatch => DataRetryClass::Malformed,
            },
        },
    }
}

// ══════════════════════════════════════════════════════════════════════
// 6. Session and worker lifecycle (§14.9, §14.11)
// ══════════════════════════════════════════════════════════════════════

/// How a SQL session is attached to a worker (§14.9).
///
/// §14.9: "Short operations may drain; durable cursors and sessions are
/// resumable; deployments requiring connection preservation use
/// edge-anchored workers."
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentKind {
    /// The session lives at the protocol anchor that owns the client
    /// transport. Connection preservation across worker replacement.
    EdgeAnchored = 1,
    /// Movable session state: exported when its continuity contract
    /// requires it, then rebound to a replacement worker.
    Movable = 2,
    /// The session drains its short operations and then fails cleanly.
    /// §14.9's "short operations may drain".
    DrainOnly = 3,
}

impl AttachmentKind {
    pub const ALL: [Self; 3] = [Self::EdgeAnchored, Self::Movable, Self::DrainOnly];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::EdgeAnchored),
            2 => Some(Self::Movable),
            3 => Some(Self::DrainOnly),
            _ => None,
        }
    }
}

/// What a replacement worker does with this session (§14.11's "after
/// compute failure, a replacement worker ... resumes only explicitly
/// resumable operations, and fails all others cleanly").
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerLossAction {
    /// The transport is at the anchor; rebind a fresh worker behind it.
    Reattach = 1,
    /// Import the exported session state into the replacement worker.
    MigrateState = 2,
    /// Drain in-flight short operations, then fail the session cleanly.
    /// "Cleanly" is the point: a typed failure, not a silent hang.
    DrainAndFail = 3,
}

/// A session as the session directory knows it (§14.9).
///
/// Wire layout (integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [session_id:u64][session_epoch:u32][owning_worker:u32]
/// [attachment:u8][tenant_id:u32][database_id:u32]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionDescriptor {
    pub session_id: u64,
    /// Bumped on every rebind. Stale values are fenced by
    /// [`fence_stale_session`].
    pub session_epoch: u32,
    /// The worker currently owning this session's movable state.
    pub owning_worker: u32,
    pub attachment: AttachmentKind,
    pub tenant_id: u32,
    pub database_id: u32,
}

impl SessionDescriptor {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + 25;

    pub const EMPTY: SessionDescriptor = SessionDescriptor {
        session_id: 0,
        session_epoch: 0,
        owning_worker: 0,
        attachment: AttachmentKind::DrainOnly,
        tenant_id: 0,
        database_id: 0,
    };

    /// What a replacement worker does with this session.
    pub const fn worker_loss_action(&self) -> WorkerLossAction {
        match self.attachment {
            AttachmentKind::EdgeAnchored => WorkerLossAction::Reattach,
            AttachmentKind::Movable => WorkerLossAction::MigrateState,
            AttachmentKind::DrainOnly => WorkerLossAction::DrainAndFail,
        }
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&SESSION_DESCRIPTOR_TAG.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        let p = &mut out[4..Self::WIRE_LEN];
        p[0..8].copy_from_slice(&self.session_id.to_le_bytes());
        p[8..12].copy_from_slice(&self.session_epoch.to_le_bytes());
        p[12..16].copy_from_slice(&self.owning_worker.to_le_bytes());
        p[16] = self.attachment as u8;
        p[17..21].copy_from_slice(&self.tenant_id.to_le_bytes());
        p[21..25].copy_from_slice(&self.database_id.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// length disagreement, unknown attachment kind, or a zero session
    /// identity — a zeroed buffer is not "session 0", it is garbage.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != SESSION_DESCRIPTOR_TAG {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != Self::WIRE_LEN - 4 {
            return None;
        }
        let p = &src[4..];
        let session_id = u64::from_le_bytes(p[0..8].try_into().ok()?);
        if session_id == 0 {
            return None;
        }
        Some(Self {
            session_id,
            session_epoch: u32::from_le_bytes(p[8..12].try_into().ok()?),
            owning_worker: u32::from_le_bytes(p[12..16].try_into().ok()?),
            attachment: AttachmentKind::from_u8(p[16])?,
            tenant_id: u32::from_le_bytes(p[17..21].try_into().ok()?),
            database_id: u32::from_le_bytes(p[21..25].try_into().ok()?),
        })
    }
}

/// Why a presented session identity is refused (§14.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionError {
    /// The directory has no such session.
    UnknownSession { session_id: u64 },
    /// The presented epoch is behind the directory's. A stale worker
    /// still holding the old epoch cannot act.
    StaleEpoch { presented: u32, current: u32 },
    /// The presented epoch is AHEAD of the directory's. Fails closed:
    /// an epoch the directory never issued is either a forgery or
    /// evidence that the directory lost state, and neither is a reason
    /// to accept it.
    FutureEpoch { presented: u32, current: u32 },
    /// Epoch 0 is never issued.
    UninitializedEpoch,
}

/// Fence a presented session epoch against the directory (§14.9's "the
/// session directory maps `session_id` to worker and fences stale
/// `session_epoch` values").
pub fn fence_stale_session(
    presented_session_id: u64,
    presented_epoch: u32,
    current: &SessionDescriptor,
) -> Result<(), SessionError> {
    if presented_session_id != current.session_id || current.session_id == 0 {
        return Err(SessionError::UnknownSession {
            session_id: presented_session_id,
        });
    }
    if presented_epoch == 0 || current.session_epoch == 0 {
        return Err(SessionError::UninitializedEpoch);
    }
    if presented_epoch < current.session_epoch {
        return Err(SessionError::StaleEpoch {
            presented: presented_epoch,
            current: current.session_epoch,
        });
    }
    if presented_epoch > current.session_epoch {
        return Err(SessionError::FutureEpoch {
            presented: presented_epoch,
            current: current.session_epoch,
        });
    }
    Ok(())
}

// ── The §14.11 authority table ────────────────────────────────────────

/// Every kind of state the separated composition holds (§14.11's table,
/// left column).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateKind {
    /// Rows, indexes, and catalog records.
    RowsIndexesCatalogRecords = 1,
    /// Transaction outcome.
    TransactionOutcome = 2,
    /// Transaction intents.
    TransactionIntents = 3,
    /// Schema and backfill jobs.
    SchemaAndBackfillJobs = 4,
    /// Durable sequences and surviving advisory locks.
    DurableSequencesAndLocks = 5,
    /// Query plan and decoded catalog.
    QueryPlanAndDecodedCatalog = 6,
    /// Query intermediate spill.
    QueryIntermediateSpill = 7,
    /// Client transport attachment.
    ClientTransportAttachment = 8,
    /// Movable SQL session state.
    MovableSqlSessionState = 9,
}

impl StateKind {
    /// Every row of §14.11's table. The tests iterate this, so a state
    /// kind added without an authority mapping fails to compile and one
    /// added without a recoverability answer fails the invariant-15
    /// test.
    pub const ALL: [Self; 9] = [
        Self::RowsIndexesCatalogRecords,
        Self::TransactionOutcome,
        Self::TransactionIntents,
        Self::SchemaAndBackfillJobs,
        Self::DurableSequencesAndLocks,
        Self::QueryPlanAndDecodedCatalog,
        Self::QueryIntermediateSpill,
        Self::ClientTransportAttachment,
        Self::MovableSqlSessionState,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::RowsIndexesCatalogRecords),
            2 => Some(Self::TransactionOutcome),
            3 => Some(Self::TransactionIntents),
            4 => Some(Self::SchemaAndBackfillJobs),
            5 => Some(Self::DurableSequencesAndLocks),
            6 => Some(Self::QueryPlanAndDecodedCatalog),
            7 => Some(Self::QueryIntermediateSpill),
            8 => Some(Self::ClientTransportAttachment),
            9 => Some(Self::MovableSqlSessionState),
            _ => None,
        }
    }
}

/// Who holds a state kind (§14.11's table, right column).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Authority {
    /// Lattice ranges — including the transactional and lease records
    /// that hold durable sequences and surviving advisory locks.
    LatticeRange = 1,
    /// The transaction's home range (§13.2).
    TransactionHomeRange = 2,
    /// The participant ranges holding the intents.
    ParticipantRange = 3,
    /// Lattice system ranges.
    LatticeSystemRange = 4,
    /// Compute cache. Disposable.
    ComputeCacheDisposable = 5,
    /// Compute-local spill. Disposable.
    ComputeLocalSpill = 6,
    /// The protocol anchor owning client transport attachment.
    ProtocolAnchor = 7,
    /// The session worker holding movable SQL session state.
    SessionWorker = 8,
}

impl Authority {
    pub const ALL: [Self; 8] = [
        Self::LatticeRange,
        Self::TransactionHomeRange,
        Self::ParticipantRange,
        Self::LatticeSystemRange,
        Self::ComputeCacheDisposable,
        Self::ComputeLocalSpill,
        Self::ProtocolAnchor,
        Self::SessionWorker,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::LatticeRange),
            2 => Some(Self::TransactionHomeRange),
            3 => Some(Self::ParticipantRange),
            4 => Some(Self::LatticeSystemRange),
            5 => Some(Self::ComputeCacheDisposable),
            6 => Some(Self::ComputeLocalSpill),
            7 => Some(Self::ProtocolAnchor),
            8 => Some(Self::SessionWorker),
            _ => None,
        }
    }

    /// Is this authority a durable Lattice authority — the kind whose
    /// loss would lose a committed outcome?
    pub const fn is_durable(self) -> bool {
        matches!(
            self,
            Self::LatticeRange
                | Self::TransactionHomeRange
                | Self::ParticipantRange
                | Self::LatticeSystemRange
        )
    }

    /// Does this authority live in the relational-compute graph
    /// (including its protocol anchors and session workers)?
    ///
    /// §21 invariant 15 is exactly the statement that these two
    /// predicates never both hold, and a test asserts it over
    /// [`Authority::ALL`].
    pub const fn is_compute_side(self) -> bool {
        matches!(
            self,
            Self::ComputeCacheDisposable
                | Self::ComputeLocalSpill
                | Self::ProtocolAnchor
                | Self::SessionWorker
        )
    }
}

/// §14.11's authority table as a total function.
///
/// | State | Authority |
/// |---|---|
/// | Rows, indexes, catalog records | [`Authority::LatticeRange`] |
/// | Transaction outcome | [`Authority::TransactionHomeRange`] |
/// | Transaction intents | [`Authority::ParticipantRange`] |
/// | Schema and backfill jobs | [`Authority::LatticeSystemRange`] |
/// | Durable sequences and surviving advisory locks | [`Authority::LatticeRange`] |
/// | Query plan and decoded catalog | [`Authority::ComputeCacheDisposable`] |
/// | Query intermediate spill | [`Authority::ComputeLocalSpill`] |
/// | Client transport attachment | [`Authority::ProtocolAnchor`] |
/// | Movable SQL session state | [`Authority::SessionWorker`] |
///
/// Durable sequences and surviving advisory locks map to
/// `LatticeRange` rather than a distinct authority because §14.11
/// places them in "Lattice transactional or lease records" — ordinary
/// range-held records, reached through the ordinary transaction path.
pub const fn authority_of(state_kind: StateKind) -> Authority {
    match state_kind {
        StateKind::RowsIndexesCatalogRecords => Authority::LatticeRange,
        StateKind::TransactionOutcome => Authority::TransactionHomeRange,
        StateKind::TransactionIntents => Authority::ParticipantRange,
        StateKind::SchemaAndBackfillJobs => Authority::LatticeSystemRange,
        StateKind::DurableSequencesAndLocks => Authority::LatticeRange,
        StateKind::QueryPlanAndDecodedCatalog => Authority::ComputeCacheDisposable,
        StateKind::QueryIntermediateSpill => Authority::ComputeLocalSpill,
        StateKind::ClientTransportAttachment => Authority::ProtocolAnchor,
        StateKind::MovableSqlSessionState => Authority::SessionWorker,
    }
}

/// What a replacement worker does with this state kind after compute
/// loss (§14.11's recovery paragraph).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Nothing to do — the state never left the Lattice authority that
    /// holds it. Compute failure did not touch it.
    HeldByLattice = 1,
    /// Reload it from its durable authority ("reloads catalog and
    /// routing state").
    Reload = 2,
    /// Throw it away and recompute ("warming disposable caches").
    Discard = 3,
    /// Ask the durable record by identity ("checks durable transaction
    /// status by identity").
    DiscoverByIdentity = 4,
    /// Rebind a replacement worker behind the surviving anchor.
    Reattach = 5,
    /// Import the exported session state, or fail the session cleanly.
    MigrateOrFailCleanly = 6,
}

/// The recovery action for one state kind (§14.11).
pub const fn recovery_action(state_kind: StateKind) -> RecoveryAction {
    match state_kind {
        // Durable Lattice authorities: compute loss is not an event
        // that reaches them at all.
        StateKind::RowsIndexesCatalogRecords
        | StateKind::SchemaAndBackfillJobs
        | StateKind::DurableSequencesAndLocks => RecoveryAction::HeldByLattice,
        // Discoverable by identity, not by guessing.
        StateKind::TransactionOutcome | StateKind::TransactionIntents => {
            RecoveryAction::DiscoverByIdentity
        }
        // Compute cache: reloadable from the catalog/routing authority.
        StateKind::QueryPlanAndDecodedCatalog => RecoveryAction::Reload,
        // Spill: pure intermediate; recompute.
        StateKind::QueryIntermediateSpill => RecoveryAction::Discard,
        StateKind::ClientTransportAttachment => RecoveryAction::Reattach,
        StateKind::MovableSqlSessionState => RecoveryAction::MigrateOrFailCleanly,
    }
}

/// §21 invariant 15 made executable: "relational compute holds no
/// unique durable database outcome or storage file".
///
/// This returns `true` for EVERY state kind, and that totality is the
/// invariant, not an oversight:
///
/// - the four durable kinds are held by Lattice ranges, so losing
///   compute cannot lose them — there is nothing to recover;
/// - the four compute-side kinds are disposable or reconstructable by
///   construction, which is *why* §14.7 permits compute to be replaced
///   at all.
///
/// If a future state kind cannot answer `true`, that kind is a unique
/// durable outcome living in compute, and the correct fix is to move it
/// to a Lattice authority — never to make this function partial.
pub const fn compute_loss_is_recoverable(state_kind: StateKind) -> bool {
    let authority = authority_of(state_kind);
    // A durable Lattice authority is untouched by compute loss; a
    // compute-side authority is disposable or exportable. There is no
    // third case, and the `&&` shape says so rather than hiding it
    // behind a bare `true`.
    authority.is_durable() || authority.is_compute_side()
}

// ══════════════════════════════════════════════════════════════════════
// 7. Cache coherence (§14.12)
// ══════════════════════════════════════════════════════════════════════

/// The key a compute metadata cache entry is filed under (§14.12:
/// "compute metadata caches key entries by catalog revision,
/// partition-map revision, range generation, and schema format").
///
/// All four dimensions, not a subset: each one can move independently,
/// and an entry keyed on three of them survives a change to the fourth.
///
/// Wire layout (integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [catalog_revision:u64][partition_map_revision:u32]
/// [range_generation:u32][schema_format_version:u16]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheEntryKey {
    pub catalog_revision: u64,
    pub partition_map_revision: u32,
    pub range_generation: u32,
    pub schema_format_version: u16,
}

impl CacheEntryKey {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + 18;

    pub const EMPTY: CacheEntryKey = CacheEntryKey {
        catalog_revision: 0,
        partition_map_revision: 0,
        range_generation: 0,
        schema_format_version: 0,
    };

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&CACHE_ENTRY_KEY_TAG.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        let p = &mut out[4..Self::WIRE_LEN];
        p[0..8].copy_from_slice(&self.catalog_revision.to_le_bytes());
        p[8..12].copy_from_slice(&self.partition_map_revision.to_le_bytes());
        p[12..16].copy_from_slice(&self.range_generation.to_le_bytes());
        p[16..18].copy_from_slice(&self.schema_format_version.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// length disagreement, or a zero catalog revision — revision 0 is
    /// never published, so a zeroed buffer must not decode into a key
    /// that could match a live entry.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != CACHE_ENTRY_KEY_TAG {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != Self::WIRE_LEN - 4 {
            return None;
        }
        let p = &src[4..];
        let catalog_revision = u64::from_le_bytes(p[0..8].try_into().ok()?);
        if catalog_revision == 0 {
            return None;
        }
        Some(Self {
            catalog_revision,
            partition_map_revision: u32::from_le_bytes(p[8..12].try_into().ok()?),
            range_generation: u32::from_le_bytes(p[12..16].try_into().ok()?),
            schema_format_version: u16::from_le_bytes(p[16..18].try_into().ok()?),
        })
    }
}

/// The fences a compute cache currently believes (§14.12). Carries the
/// subscription health alongside the revisions, because §14.12's rule
/// is that "a gap or stale source fence forces refresh rather than
/// best-effort continuation" — so the gap must be part of the input, not
/// something the caller is trusted to check separately.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CurrentFences {
    pub key: CacheEntryKey,
    /// A catalog or topology subscription reported a gap.
    pub subscription_gap: bool,
    /// The fence of the source that last refreshed this cache.
    pub source: SourceFence,
    /// The source identity the cache expects.
    pub expected_source_id: u32,
    /// The source kind the cache expects.
    pub expected_source_kind: FenceSource,
    /// The lowest source epoch still acceptable.
    pub min_source_epoch: u64,
}

impl CurrentFences {
    pub const EMPTY: CurrentFences = CurrentFences {
        key: CacheEntryKey::EMPTY,
        subscription_gap: false,
        source: SourceFence::EMPTY,
        expected_source_id: 0,
        expected_source_kind: FenceSource::ControlPlane,
        min_source_epoch: 0,
    };
}

/// Why a cache entry must be refreshed. Separate variants per dimension
/// so a test can move one fence at a time and see exactly that one
/// reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheValidity {
    Valid,
    CatalogRevisionMoved {
        entry: u64,
        current: u64,
    },
    PartitionMapRevisionMoved {
        entry: u32,
        current: u32,
    },
    RangeGenerationMoved {
        entry: u32,
        current: u32,
    },
    SchemaFormatChanged {
        entry: u16,
        current: u16,
    },
    /// §14.12: "a gap ... forces refresh rather than best-effort
    /// continuation".
    SubscriptionGap,
    /// §14.12 + §21 invariant 11: a fence from the wrong source, or
    /// from the right source at a stale epoch.
    StaleSourceFence,
}

/// Classify a cache entry against the current fences (§14.12).
///
/// ### Check order is part of the contract
///
/// The subscription gap and the source fence are checked FIRST. If the
/// invalidation channel itself is broken, the four revision comparisons
/// are comparing against numbers that may already be wrong, and a
/// "Valid" answer derived from untrustworthy inputs is precisely the
/// best-effort continuation §14.12 rejects.
pub fn cache_validity(entry: &CacheEntryKey, current: &CurrentFences) -> CacheValidity {
    if current.subscription_gap {
        return CacheValidity::SubscriptionGap;
    }
    if !current.source.is_valid_for(
        current.expected_source_kind,
        current.expected_source_id,
        current.min_source_epoch,
    ) {
        return CacheValidity::StaleSourceFence;
    }
    if entry.catalog_revision != current.key.catalog_revision {
        return CacheValidity::CatalogRevisionMoved {
            entry: entry.catalog_revision,
            current: current.key.catalog_revision,
        };
    }
    if entry.partition_map_revision != current.key.partition_map_revision {
        return CacheValidity::PartitionMapRevisionMoved {
            entry: entry.partition_map_revision,
            current: current.key.partition_map_revision,
        };
    }
    if entry.range_generation != current.key.range_generation {
        return CacheValidity::RangeGenerationMoved {
            entry: entry.range_generation,
            current: current.key.range_generation,
        };
    }
    if entry.schema_format_version != current.key.schema_format_version {
        return CacheValidity::SchemaFormatChanged {
            entry: entry.schema_format_version,
            current: current.key.schema_format_version,
        };
    }
    CacheValidity::Valid
}

/// §14.12's rule in one predicate.
pub fn cache_entry_valid(entry: &CacheEntryKey, current_fences: &CurrentFences) -> bool {
    matches!(cache_validity(entry, current_fences), CacheValidity::Valid)
}

// ── Cacheable and non-cacheable observations (§14.12) ─────────────────

/// An immutable MVCC observation (§14.12: "a later cache may store
/// immutable MVCC observations keyed by range identity, generation,
/// key/span, and read timestamp").
///
/// It is cacheable precisely because it carries its own fences: the
/// range it came from, that range's generation, and the timestamp it
/// was read at. Given those three, the observation is immutable — MVCC
/// history at a timestamp does not change — so retaining it across
/// requests is sound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MvccObservation {
    pub range_id: [u8; 16],
    pub range_generation: u32,
    /// Never 0: an MVCC observation without a read timestamp is not an
    /// MVCC observation. [`MvccObservation::cache_key`] returns `None`
    /// if it is.
    pub read_timestamp: Timestamp,
    /// Stable digest of the key or span observed.
    pub span_digest: u64,
    pub catalog_revision: u64,
    pub partition_map_revision: u32,
    pub schema_format_version: u16,
}

impl MvccObservation {
    pub const EMPTY: MvccObservation = MvccObservation {
        range_id: [0; 16],
        range_generation: 0,
        read_timestamp: 0,
        span_digest: 0,
        catalog_revision: 0,
        partition_map_revision: 0,
        schema_format_version: 0,
    };

    /// The cache key for this observation, or `None` if it is not a
    /// well-formed MVCC observation (null range, zero generation, or
    /// zero read timestamp).
    pub const fn cache_key(&self) -> Option<CacheEntryKey> {
        if self.read_timestamp == 0
            || self.range_generation == 0
            || same_range(&self.range_id, &[0u8; 16])
        {
            return None;
        }
        Some(CacheEntryKey {
            catalog_revision: self.catalog_revision,
            partition_map_revision: self.partition_map_revision,
            range_generation: self.range_generation,
            schema_format_version: self.schema_format_version,
        })
    }
}

/// An unfenced "latest value" observation (§14.12: "An unfenced 'latest
/// value' cannot be retained across requests").
///
/// The rule is enforced by TYPE, not by discipline: this struct has no
/// `cache_key` method, no read timestamp field to key on, and no
/// conversion into [`MvccObservation`]. There is no expression in this
/// module that turns a `LatestObservation` into a [`CacheEntryKey`], so
/// a caller cannot file one in a cache even by accident. Promoting a
/// latest read to a cacheable one requires *re-reading it at a
/// timestamp*, which is a different operation with a different result —
/// exactly as it should be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatestObservation {
    pub range_id: [u8; 16],
    pub range_generation: u32,
    pub span_digest: u64,
    /// The apply-order position this answer was true at. Enough to
    /// report freshness in a response; NOT enough to make the value
    /// immutable, because the next apply changes it.
    pub observed_revision: u64,
}

impl LatestObservation {
    pub const EMPTY: LatestObservation = LatestObservation {
        range_id: [0; 16],
        range_generation: 0,
        span_digest: 0,
        observed_revision: 0,
    };

    /// Stated as a constant so the rule is greppable and citable rather
    /// than implied by the absence of a method.
    pub const CACHEABLE: bool = false;
}

/// The two observation kinds a data client can hold.
///
/// [`Observation::cacheable`] is the single entry point to the cache,
/// and it returns `None` for every `Latest` value. §14.12's rule is
/// therefore not a convention a future author must remember; it is the
/// only behaviour the type admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Observation {
    Immutable(MvccObservation),
    Latest(LatestObservation),
}

impl Observation {
    /// The cache key, or `None` when the observation may not be
    /// retained across requests.
    pub const fn cacheable(&self) -> Option<CacheEntryKey> {
        match self {
            Self::Immutable(o) => o.cache_key(),
            // §14.12. Not "usually not"; never.
            Self::Latest(_) => None,
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// 8. Bounded pushdown (§14.13)
// ══════════════════════════════════════════════════════════════════════

/// The registered deterministic partial aggregates (§14.13's
/// "explicitly registered deterministic partial aggregates").
///
/// A closed registry with stable IDs is the entire mechanism by which
/// §28's "no arbitrary connector SQL or opaque executable code" is
/// enforced: an aggregate is either a member of this enum or it is
/// refused. Adding one is a deliberate act with a version bump, not
/// something a connector can do by sending bytes.
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisteredAggregate {
    /// Row count.
    Count = 0x0001,
    /// Sum over a signed 64-bit projection column.
    SumI64 = 0x0002,
    /// Lexicographic minimum of a byte projection.
    MinBytes = 0x0003,
    /// Lexicographic maximum of a byte projection.
    MaxBytes = 0x0004,
}

impl RegisteredAggregate {
    pub const ALL: [Self; 4] = [Self::Count, Self::SumI64, Self::MinBytes, Self::MaxBytes];

    pub const fn from_u16(v: u16) -> Option<Self> {
        match v {
            0x0001 => Some(Self::Count),
            0x0002 => Some(Self::SumI64),
            0x0003 => Some(Self::MinBytes),
            0x0004 => Some(Self::MaxBytes),
            _ => None,
        }
    }
}

/// The server-side bounds a program is admitted against (§14.13:
/// "bounded by rows, bytes, memory, and work").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushdownLimits {
    pub max_rows: u32,
    pub max_bytes: u32,
    pub max_work_units: u32,
}

impl PushdownLimits {
    pub const EMPTY: PushdownLimits = PushdownLimits {
        max_rows: 0,
        max_bytes: 0,
        max_work_units: 0,
    };
}

/// A bounded pushdown program (§14.13).
///
/// §14.13 permits exactly seven things to be pushed down: key spans,
/// MVCC timestamps, index lookup, prefix filters, projection, limit,
/// and registered deterministic partial aggregates. This record has a
/// field for each and nothing else. There is no expression tree, no
/// bytecode, and no filter language, because §28 rejects "arbitrary
/// connector SQL or opaque executable code" and the reliable way to
/// reject something is to have nowhere to put it.
///
/// `opaque_payload_len` exists solely so that a peer attempting to
/// smuggle such a payload is refused EXPLICITLY by
/// [`validate_pushdown`] rather than by silent truncation. It must
/// always be zero.
///
/// Wire layout (integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [catalog_revision:u64]                            8
/// [schema_format_version:u16]                       2
/// [mvcc_timestamp:u64]                              8
/// [index_id:u32]            (0 = no index lookup)   4
/// [projection_mask:u64]                             8
/// [limit_rows:u32]                                  4
/// [max_rows:u32]                                    4
/// [max_bytes:u32]                                   4
/// [max_work_units:u32]                              4
/// [cancellation_id:u64]                             8
/// [opaque_payload_len:u16]  (MUST be 0)             2
/// [aggregate_count:u8][prefix_count:u8]             2
/// [start_len:u16][end_len:u16]                      4
/// [aggregate_id:u16] * aggregate_count
/// [start_key bytes][end_key bytes]
/// ([prefix_len:u16][prefix bytes]) * prefix_count
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushdownProgram {
    /// §14.13's "schema-bound": the catalog revision this program was
    /// compiled against. A storage graph at a different revision refuses
    /// it rather than executing a plan built for another schema.
    pub catalog_revision: u64,
    pub schema_format_version: u16,
    /// The MVCC timestamp the scan reads at. Never 0 — an unfenced
    /// "latest" pushdown has no reproducible result (§14.12).
    pub mvcc_timestamp: Timestamp,
    /// Secondary index to look up through; 0 = primary scan.
    pub index_id: u32,
    /// Bit per projected column.
    pub projection_mask: u64,
    /// The program's own row limit.
    pub limit_rows: u32,
    /// Hard bounds the program declares for itself. All three must be
    /// non-zero: 0 means "no bound", and §21 invariant 14 has no such
    /// mode.
    pub max_rows: u32,
    pub max_bytes: u32,
    pub max_work_units: u32,
    /// §14.13's "cancellable".
    pub cancellation_id: u64,
    /// MUST be 0. See the type docs.
    pub opaque_payload_len: u16,
    aggregates: [u16; MAX_PUSHDOWN_AGGREGATES],
    aggregate_count: u8,
    start_key: [u8; MAX_PUSHDOWN_KEY_LEN],
    start_key_len: u16,
    end_key: [u8; MAX_PUSHDOWN_KEY_LEN],
    end_key_len: u16,
    prefixes: [[u8; MAX_PUSHDOWN_PREFIX_LEN]; MAX_PUSHDOWN_PREFIXES],
    prefix_lens: [u16; MAX_PUSHDOWN_PREFIXES],
    prefix_count: u8,
}

/// Fixed part of an encoded program: header + every scalar field.
pub const PUSHDOWN_FIXED_WIRE_LEN: usize = 4 + 8 + 2 + 8 + 4 + 8 + 4 + 4 + 4 + 4 + 8 + 2 + 2 + 4;
/// Worst-case encoded program length.
pub const PUSHDOWN_MAX_WIRE_LEN: usize = PUSHDOWN_FIXED_WIRE_LEN
    + MAX_PUSHDOWN_AGGREGATES * 2
    + 2 * MAX_PUSHDOWN_KEY_LEN
    + MAX_PUSHDOWN_PREFIXES * (2 + MAX_PUSHDOWN_PREFIX_LEN);

impl PushdownProgram {
    pub const EMPTY: PushdownProgram = PushdownProgram {
        catalog_revision: 0,
        schema_format_version: 0,
        mvcc_timestamp: 0,
        index_id: 0,
        projection_mask: 0,
        limit_rows: 0,
        max_rows: 0,
        max_bytes: 0,
        max_work_units: 0,
        cancellation_id: 0,
        opaque_payload_len: 0,
        aggregates: [0; MAX_PUSHDOWN_AGGREGATES],
        aggregate_count: 0,
        start_key: [0; MAX_PUSHDOWN_KEY_LEN],
        start_key_len: 0,
        end_key: [0; MAX_PUSHDOWN_KEY_LEN],
        end_key_len: 0,
        prefixes: [[0; MAX_PUSHDOWN_PREFIX_LEN]; MAX_PUSHDOWN_PREFIXES],
        prefix_lens: [0; MAX_PUSHDOWN_PREFIXES],
        prefix_count: 0,
    };

    /// Set the key span. `end` empty means "unbounded above", which is
    /// legal for a span but not for the row/byte/work budgets.
    pub fn set_span(&mut self, start: &[u8], end: &[u8]) -> Option<()> {
        if start.len() > MAX_PUSHDOWN_KEY_LEN || end.len() > MAX_PUSHDOWN_KEY_LEN {
            return None;
        }
        self.start_key = [0; MAX_PUSHDOWN_KEY_LEN];
        self.end_key = [0; MAX_PUSHDOWN_KEY_LEN];
        self.start_key[..start.len()].copy_from_slice(start);
        self.end_key[..end.len()].copy_from_slice(end);
        self.start_key_len = start.len() as u16;
        self.end_key_len = end.len() as u16;
        Some(())
    }

    pub fn start_key(&self) -> &[u8] {
        &self.start_key[..self.start_key_len as usize]
    }

    pub fn end_key(&self) -> &[u8] {
        &self.end_key[..self.end_key_len as usize]
    }

    /// Append a prefix filter. Refuses beyond [`MAX_PUSHDOWN_PREFIXES`].
    pub fn push_prefix(&mut self, prefix: &[u8]) -> Option<()> {
        let i = self.prefix_count as usize;
        if i >= MAX_PUSHDOWN_PREFIXES || prefix.len() > MAX_PUSHDOWN_PREFIX_LEN {
            return None;
        }
        self.prefixes[i] = [0; MAX_PUSHDOWN_PREFIX_LEN];
        self.prefixes[i][..prefix.len()].copy_from_slice(prefix);
        self.prefix_lens[i] = prefix.len() as u16;
        self.prefix_count += 1;
        Some(())
    }

    pub fn prefix_count(&self) -> usize {
        self.prefix_count as usize
    }

    pub fn prefix(&self, i: usize) -> Option<&[u8]> {
        if i >= self.prefix_count as usize {
            return None;
        }
        Some(&self.prefixes[i][..self.prefix_lens[i] as usize])
    }

    /// Append a raw aggregate ID.
    ///
    /// Deliberately takes a `u16` rather than a [`RegisteredAggregate`]:
    /// the decoder must be able to reconstruct a program containing an
    /// ID this build does not know, so that [`validate_pushdown`] can
    /// refuse it by name. A typed-only setter would make the
    /// unregistered case unrepresentable and therefore untestable.
    pub fn push_aggregate_id(&mut self, id: u16) -> Option<()> {
        let i = self.aggregate_count as usize;
        if i >= MAX_PUSHDOWN_AGGREGATES {
            return None;
        }
        self.aggregates[i] = id;
        self.aggregate_count += 1;
        Some(())
    }

    /// Append a registered aggregate.
    pub fn push_aggregate(&mut self, aggregate: RegisteredAggregate) -> Option<()> {
        self.push_aggregate_id(aggregate as u16)
    }

    pub fn aggregate_ids(&self) -> &[u16] {
        &self.aggregates[..self.aggregate_count as usize]
    }

    /// Encoded length of this program.
    pub fn wire_len(&self) -> usize {
        PUSHDOWN_FIXED_WIRE_LEN
            + self.aggregate_count as usize * 2
            + self.start_key_len as usize
            + self.end_key_len as usize
            + (0..self.prefix_count as usize)
                .map(|i| 2 + self.prefix_lens[i] as usize)
                .sum::<usize>()
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total || total > PUSHDOWN_MAX_WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&PUSHDOWN_TAG.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        let p = &mut out[4..total];
        p[0..8].copy_from_slice(&self.catalog_revision.to_le_bytes());
        p[8..10].copy_from_slice(&self.schema_format_version.to_le_bytes());
        p[10..18].copy_from_slice(&self.mvcc_timestamp.to_le_bytes());
        p[18..22].copy_from_slice(&self.index_id.to_le_bytes());
        p[22..30].copy_from_slice(&self.projection_mask.to_le_bytes());
        p[30..34].copy_from_slice(&self.limit_rows.to_le_bytes());
        p[34..38].copy_from_slice(&self.max_rows.to_le_bytes());
        p[38..42].copy_from_slice(&self.max_bytes.to_le_bytes());
        p[42..46].copy_from_slice(&self.max_work_units.to_le_bytes());
        p[46..54].copy_from_slice(&self.cancellation_id.to_le_bytes());
        p[54..56].copy_from_slice(&self.opaque_payload_len.to_le_bytes());
        p[56] = self.aggregate_count;
        p[57] = self.prefix_count;
        p[58..60].copy_from_slice(&self.start_key_len.to_le_bytes());
        p[60..62].copy_from_slice(&self.end_key_len.to_le_bytes());
        let mut at = 62;
        for i in 0..self.aggregate_count as usize {
            p[at..at + 2].copy_from_slice(&self.aggregates[i].to_le_bytes());
            at += 2;
        }
        let sl = self.start_key_len as usize;
        p[at..at + sl].copy_from_slice(&self.start_key[..sl]);
        at += sl;
        let el = self.end_key_len as usize;
        p[at..at + el].copy_from_slice(&self.end_key[..el]);
        at += el;
        for i in 0..self.prefix_count as usize {
            let l = self.prefix_lens[i] as usize;
            p[at..at + 2].copy_from_slice(&self.prefix_lens[i].to_le_bytes());
            at += 2;
            p[at..at + l].copy_from_slice(&self.prefixes[i][..l]);
            at += l;
        }
        Some(total)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// length disagreement, counts beyond their bounds, key or prefix
    /// lengths beyond their bounds, or a length prefix that overruns the
    /// buffer.
    ///
    /// Aggregate IDs are NOT validated here — an unregistered ID decodes
    /// fine and is refused by [`validate_pushdown`], so the refusal is
    /// reported as `UnregisteredAggregate` (which names the offending
    /// ID) rather than as an unreadable record.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < PUSHDOWN_FIXED_WIRE_LEN || src.len() > PUSHDOWN_MAX_WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != PUSHDOWN_TAG {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != src.len() - 4 {
            return None;
        }
        let p = &src[4..];
        let aggregate_count = p[56];
        let prefix_count = p[57];
        if aggregate_count as usize > MAX_PUSHDOWN_AGGREGATES
            || prefix_count as usize > MAX_PUSHDOWN_PREFIXES
        {
            return None;
        }
        let start_key_len = u16::from_le_bytes(p[58..60].try_into().ok()?) as usize;
        let end_key_len = u16::from_le_bytes(p[60..62].try_into().ok()?) as usize;
        if start_key_len > MAX_PUSHDOWN_KEY_LEN || end_key_len > MAX_PUSHDOWN_KEY_LEN {
            return None;
        }
        let mut program = Self {
            catalog_revision: u64::from_le_bytes(p[0..8].try_into().ok()?),
            schema_format_version: u16::from_le_bytes(p[8..10].try_into().ok()?),
            mvcc_timestamp: u64::from_le_bytes(p[10..18].try_into().ok()?),
            index_id: u32::from_le_bytes(p[18..22].try_into().ok()?),
            projection_mask: u64::from_le_bytes(p[22..30].try_into().ok()?),
            limit_rows: u32::from_le_bytes(p[30..34].try_into().ok()?),
            max_rows: u32::from_le_bytes(p[34..38].try_into().ok()?),
            max_bytes: u32::from_le_bytes(p[38..42].try_into().ok()?),
            max_work_units: u32::from_le_bytes(p[42..46].try_into().ok()?),
            cancellation_id: u64::from_le_bytes(p[46..54].try_into().ok()?),
            opaque_payload_len: u16::from_le_bytes(p[54..56].try_into().ok()?),
            ..Self::EMPTY
        };
        let mut at = 62;
        for _ in 0..aggregate_count {
            if at + 2 > p.len() {
                return None;
            }
            program.push_aggregate_id(u16::from_le_bytes(p[at..at + 2].try_into().ok()?))?;
            at += 2;
        }
        if at + start_key_len + end_key_len > p.len() {
            return None;
        }
        let start = &p[at..at + start_key_len];
        let end = &p[at + start_key_len..at + start_key_len + end_key_len];
        program.set_span(start, end)?;
        at += start_key_len + end_key_len;
        for _ in 0..prefix_count {
            if at + 2 > p.len() {
                return None;
            }
            let l = u16::from_le_bytes(p[at..at + 2].try_into().ok()?) as usize;
            at += 2;
            if l > MAX_PUSHDOWN_PREFIX_LEN || at + l > p.len() {
                return None;
            }
            program.push_prefix(&p[at..at + l])?;
            at += l;
        }
        if at != p.len() {
            return None;
        }
        Some(program)
    }
}

/// Why a pushdown program is refused (§14.13, §28).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushdownError {
    /// §14.13 "schema-bound": compiled against a different catalog.
    StaleCatalogRevision {
        program: u64,
        current: u64,
    },
    SchemaFormatMismatch {
        program: u16,
        current: u16,
    },
    /// §28: an aggregate that is not in the registry. The one refusal
    /// that keeps "no arbitrary connector SQL" true.
    UnregisteredAggregate {
        id: u16,
    },
    /// §28: an opaque payload — connector SQL, bytecode, a filter
    /// expression — has no representation here and is refused by name.
    OpaquePayloadPresent {
        bytes: u16,
    },
    /// §21 invariant 14: 0 is not "unlimited", it is a refusal.
    UnboundedRows,
    UnboundedBytes,
    UnboundedWork,
    /// The program declares a bound above what the server admits.
    RowsExceedLimit {
        program: u32,
        limit: u32,
    },
    BytesExceedLimit {
        program: u32,
        limit: u32,
    },
    WorkExceedsLimit {
        program: u32,
        limit: u32,
    },
    /// The program's own limit is above its own row bound.
    LimitExceedsRowBound {
        limit: u32,
        max_rows: u32,
    },
    /// §14.13 "cancellable".
    MissingCancellationIdentity,
    /// §14.12: an unfenced "latest" pushdown is not reproducible.
    MissingMvccTimestamp,
    /// `start >= end` with a bounded end: an empty or inverted span is
    /// a compiler bug, not a zero-row scan.
    InvalidSpan,
    /// The server itself declared no limits.
    LimitsNotConfigured,
}

/// Validate a pushdown program against the server's limits and the
/// storage graph's catalog revision (§14.13, §28).
///
/// ### Check order is part of the contract
///
/// 1. schema binding — a program compiled against another catalog is
///    describing columns that may not exist, so nothing else it says
///    can be trusted;
/// 2. the opaque payload — the §28 refusal, checked before any bound so
///    that smuggled executable content is never reported as merely
///    "too large";
/// 3. the aggregate registry;
/// 4. boundedness, then the server's ceilings;
/// 5. determinism prerequisites (MVCC timestamp) and cancellability.
pub fn validate_pushdown(
    program: &PushdownProgram,
    limits: &PushdownLimits,
    catalog_revision: u64,
    schema_format_version: u16,
) -> Result<(), PushdownError> {
    // 1. Schema binding.
    if program.catalog_revision != catalog_revision {
        return Err(PushdownError::StaleCatalogRevision {
            program: program.catalog_revision,
            current: catalog_revision,
        });
    }
    if program.schema_format_version != schema_format_version {
        return Err(PushdownError::SchemaFormatMismatch {
            program: program.schema_format_version,
            current: schema_format_version,
        });
    }

    // 2. §28: no opaque executable content.
    if program.opaque_payload_len != 0 {
        return Err(PushdownError::OpaquePayloadPresent {
            bytes: program.opaque_payload_len,
        });
    }

    // 3. Registered aggregates only.
    for &id in program.aggregate_ids() {
        if RegisteredAggregate::from_u16(id).is_none() {
            return Err(PushdownError::UnregisteredAggregate { id });
        }
    }

    // 4. Boundedness, then ceilings.
    if limits.max_rows == 0 || limits.max_bytes == 0 || limits.max_work_units == 0 {
        return Err(PushdownError::LimitsNotConfigured);
    }
    if program.max_rows == 0 {
        return Err(PushdownError::UnboundedRows);
    }
    if program.max_bytes == 0 {
        return Err(PushdownError::UnboundedBytes);
    }
    if program.max_work_units == 0 {
        return Err(PushdownError::UnboundedWork);
    }
    if program.max_rows > limits.max_rows {
        return Err(PushdownError::RowsExceedLimit {
            program: program.max_rows,
            limit: limits.max_rows,
        });
    }
    if program.max_bytes > limits.max_bytes {
        return Err(PushdownError::BytesExceedLimit {
            program: program.max_bytes,
            limit: limits.max_bytes,
        });
    }
    if program.max_work_units > limits.max_work_units {
        return Err(PushdownError::WorkExceedsLimit {
            program: program.max_work_units,
            limit: limits.max_work_units,
        });
    }
    if program.limit_rows == 0 || program.limit_rows > program.max_rows {
        return Err(PushdownError::LimitExceedsRowBound {
            limit: program.limit_rows,
            max_rows: program.max_rows,
        });
    }

    // 5. Determinism and cancellability.
    if program.mvcc_timestamp == 0 {
        return Err(PushdownError::MissingMvccTimestamp);
    }
    if program.cancellation_id == 0 {
        return Err(PushdownError::MissingCancellationIdentity);
    }
    let (start, end) = (program.start_key(), program.end_key());
    if !end.is_empty() && start >= end {
        return Err(PushdownError::InvalidSpan);
    }
    Ok(())
}

/// §14.13's closing rule: "Pushdown must never block Raft apply or
/// compaction."
///
/// A named `false` rather than a comment, so the rule has a call site a
/// test can assert on and a reviewer can grep for. A pushdown program
/// runs against MVCC state at a fixed timestamp; it takes no apply-path
/// lock, holds no compaction claim beyond the ordinary protected
/// timestamp its read timestamp implies, and its bounds exist precisely
/// so it can be abandoned mid-flight.
pub const fn pushdown_may_block_apply() -> bool {
    false
}

// ══════════════════════════════════════════════════════════════════════
// 9. Placement and fleet (§11.5, §14.14, phase 10)
// ══════════════════════════════════════════════════════════════════════

/// Provider capability bits (§11.5's "provider capabilities and format
/// versions"). A candidate must offer every bit the constraints
/// require.
pub const CAP_DISK_STATE_STORE: u32 = 0b0000_0001;
pub const CAP_ENCRYPTION_AT_REST: u32 = 0b0000_0010;
pub const CAP_SNAPSHOT_STREAMING: u32 = 0b0000_0100;
pub const CAP_MVCC_HISTORY: u32 = 0b0000_1000;

/// One node considered for a replica (§11.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CandidateReplica {
    pub node_id: u32,
    /// §11.5's "node and failure-domain diversity". Two voters sharing
    /// a domain is the violation the default rule exists to prevent.
    pub failure_domain: u32,
    pub role: ReplicaRole,
    /// §11.5's "persistent storage capacity".
    pub free_capacity_mib: u64,
    /// §11.5's "write endurance", as remaining percent.
    pub write_endurance_pct: u8,
    /// §11.5's "number of leaders and hot ranges per node".
    pub leader_count: u32,
    /// §11.5's "tenant isolation"; 0 = shared node.
    pub tenant_id: u32,
    /// §11.5's "residency policy"; 0 = unrestricted.
    pub residency_zone: u32,
    /// §11.5's "provider capabilities".
    pub capabilities: u32,
    /// §11.5's "format versions".
    pub storage_format_version: u16,
    /// §11.5's "control-plane reserve and recovery headroom" still
    /// available on this node.
    pub control_plane_reserve_mib: u64,
}

impl CandidateReplica {
    pub const EMPTY: CandidateReplica = CandidateReplica {
        node_id: 0,
        failure_domain: 0,
        role: ReplicaRole::Voter,
        free_capacity_mib: 0,
        write_endurance_pct: 0,
        leader_count: 0,
        tenant_id: 0,
        residency_zone: 0,
        capabilities: 0,
        storage_format_version: 0,
        control_plane_reserve_mib: 0,
    };
}

/// A proposed replica set (§11.5). Fixed capacity; a proposal larger
/// than a descriptor can hold is not a placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementCandidate {
    replicas: [CandidateReplica; MAX_PLACEMENT_REPLICAS],
    count: u8,
}

impl PlacementCandidate {
    pub const EMPTY: PlacementCandidate = PlacementCandidate {
        replicas: [CandidateReplica::EMPTY; MAX_PLACEMENT_REPLICAS],
        count: 0,
    };

    pub fn push(&mut self, replica: CandidateReplica) -> Option<()> {
        let i = self.count as usize;
        if i >= MAX_PLACEMENT_REPLICAS {
            return None;
        }
        self.replicas[i] = replica;
        self.count += 1;
        Some(())
    }

    pub fn replicas(&self) -> &[CandidateReplica] {
        &self.replicas[..self.count as usize]
    }

    /// Quorum participants only. Learners and observers do not vote
    /// (§11.5) and therefore do not count towards the voter rule or the
    /// failure-domain diversity rule.
    pub fn voter_count(&self) -> u8 {
        self.replicas()
            .iter()
            .filter(|r| r.role as u8 == ReplicaRole::Voter as u8)
            .count() as u8
    }
}

/// The placement policy a candidate is admitted against (§11.5, §14.14).
///
/// Wire layout (integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [required_voters:u8][min_distinct_failure_domains:u8]
/// [min_endurance_pct:u8][reserved:u8]                4
/// [min_free_capacity_mib:u64]                        8
/// [max_leaders_per_node:u32]                         4
/// [tenant_id:u32]                                    4
/// [residency_zone:u32]                               4
/// [required_capabilities:u32]                        4
/// [required_format_version:u16]                      2
/// [control_plane_reserve_mib:u64]                    8
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementConstraints {
    pub required_voters: u8,
    /// Distinct failure domains required across the voters. Equal to
    /// `required_voters` for the §11.5 default rule.
    pub min_distinct_failure_domains: u8,
    pub min_endurance_pct: u8,
    pub min_free_capacity_mib: u64,
    pub max_leaders_per_node: u32,
    /// 0 = shared placement permitted; non-zero = every replica must be
    /// dedicated to this tenant.
    pub tenant_id: u32,
    /// 0 = unrestricted residency.
    pub residency_zone: u32,
    pub required_capabilities: u32,
    pub required_format_version: u16,
    pub control_plane_reserve_mib: u64,
}

impl PlacementConstraints {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + 38;

    /// §11.5's default: "three voters on distinct eligible failure
    /// domains". The other fields default to "unconstrained" so that a
    /// deployment adds requirements deliberately — except the voter and
    /// diversity rule, which is never optional.
    pub const DEFAULT: PlacementConstraints = PlacementConstraints {
        required_voters: DEFAULT_VOTERS,
        min_distinct_failure_domains: DEFAULT_VOTERS,
        min_endurance_pct: 0,
        min_free_capacity_mib: 0,
        max_leaders_per_node: u32::MAX,
        tenant_id: 0,
        residency_zone: 0,
        required_capabilities: 0,
        required_format_version: 0,
        control_plane_reserve_mib: 0,
    };

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&PLACEMENT_CONSTRAINTS_TAG.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        let p = &mut out[4..Self::WIRE_LEN];
        p[0] = self.required_voters;
        p[1] = self.min_distinct_failure_domains;
        p[2] = self.min_endurance_pct;
        p[3] = 0;
        p[4..12].copy_from_slice(&self.min_free_capacity_mib.to_le_bytes());
        p[12..16].copy_from_slice(&self.max_leaders_per_node.to_le_bytes());
        p[16..20].copy_from_slice(&self.tenant_id.to_le_bytes());
        p[20..24].copy_from_slice(&self.residency_zone.to_le_bytes());
        p[24..28].copy_from_slice(&self.required_capabilities.to_le_bytes());
        p[28..30].copy_from_slice(&self.required_format_version.to_le_bytes());
        p[30..38].copy_from_slice(&self.control_plane_reserve_mib.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// length disagreement, a non-zero reserved byte, a voter count
    /// outside `1..=`[`MAX_VOTERS`], an even voter count (a quorum
    /// needs an odd membership), a diversity requirement above the
    /// voter count, or an endurance percentage above 100.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != PLACEMENT_CONSTRAINTS_TAG {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != Self::WIRE_LEN - 4 {
            return None;
        }
        let p = &src[4..];
        let required_voters = p[0];
        let min_distinct_failure_domains = p[1];
        let min_endurance_pct = p[2];
        if p[3] != 0
            || required_voters == 0
            || required_voters > MAX_VOTERS
            || required_voters.is_multiple_of(2)
            || min_distinct_failure_domains > required_voters
            || min_endurance_pct > 100
        {
            return None;
        }
        Some(Self {
            required_voters,
            min_distinct_failure_domains,
            min_endurance_pct,
            min_free_capacity_mib: u64::from_le_bytes(p[4..12].try_into().ok()?),
            max_leaders_per_node: u32::from_le_bytes(p[12..16].try_into().ok()?),
            tenant_id: u32::from_le_bytes(p[16..20].try_into().ok()?),
            residency_zone: u32::from_le_bytes(p[20..24].try_into().ok()?),
            required_capabilities: u32::from_le_bytes(p[24..28].try_into().ok()?),
            required_format_version: u16::from_le_bytes(p[28..30].try_into().ok()?),
            control_plane_reserve_mib: u64::from_le_bytes(p[30..38].try_into().ok()?),
        })
    }
}

/// Why a placement is refused (§11.5). One variant per policy
/// dimension, each naming the offending node, so an operator sees which
/// rule and which machine rather than "placement failed".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementError {
    VoterCountMismatch {
        want: u8,
        got: u8,
    },
    /// The §11.5 default rule's violation: two voters in one domain.
    FailureDomainCollision {
        domain: u32,
        node_a: u32,
        node_b: u32,
    },
    /// Fewer distinct domains than required even without an exact
    /// collision pair (a diversity requirement below the voter count).
    InsufficientFailureDomains {
        want: u8,
        got: u8,
    },
    InsufficientCapacity {
        node: u32,
        free_mib: u64,
        want_mib: u64,
    },
    InsufficientEndurance {
        node: u32,
        pct: u8,
        want_pct: u8,
    },
    LeaderCountExceeded {
        node: u32,
        leaders: u32,
        limit: u32,
    },
    TenantIsolationViolation {
        node: u32,
        node_tenant: u32,
        want_tenant: u32,
    },
    ResidencyViolation {
        node: u32,
        zone: u32,
        want_zone: u32,
    },
    MissingCapability {
        node: u32,
        missing: u32,
    },
    FormatVersionMismatch {
        node: u32,
        have: u16,
        want: u16,
    },
    ControlPlaneReserveViolation {
        node: u32,
        reserve_mib: u64,
        want_mib: u64,
    },
    /// A candidate with no replicas at all.
    EmptyCandidate,
}

/// Admit a placement, or refuse it with the offending rule (§11.5).
///
/// ### Check order is part of the contract
///
/// The voter count and failure-domain diversity are checked FIRST,
/// because they are the availability objective; a set that satisfies
/// every resource constraint but puts three voters in one rack has not
/// placed a range, it has placed a single point of failure. Per-node
/// eligibility follows, in the order §11.5 lists it.
pub fn placement_is_admissible(
    candidate: &PlacementCandidate,
    constraints: &PlacementConstraints,
) -> Result<(), PlacementError> {
    let replicas = candidate.replicas();
    if replicas.is_empty() {
        return Err(PlacementError::EmptyCandidate);
    }

    // ── Voter count ──
    let voters = candidate.voter_count();
    if voters != constraints.required_voters {
        return Err(PlacementError::VoterCountMismatch {
            want: constraints.required_voters,
            got: voters,
        });
    }

    // ── Failure-domain diversity across voters (§11.5 default rule) ──
    //
    // Any two voters sharing a domain is reported as the collision it
    // is, naming both nodes. Because that returns, reaching the count
    // check means every voter is in its own domain — so `distinct` is
    // the voter count, and the second check only bites when the policy
    // asks for more domains than it asks for voters (a misconfiguration
    // that would otherwise pass silently).
    let mut distinct: u8 = 0;
    for (i, a) in replicas.iter().enumerate() {
        if a.role as u8 != ReplicaRole::Voter as u8 {
            continue;
        }
        for b in replicas.iter().take(i) {
            if b.role as u8 == ReplicaRole::Voter as u8 && b.failure_domain == a.failure_domain {
                return Err(PlacementError::FailureDomainCollision {
                    domain: a.failure_domain,
                    node_a: b.node_id,
                    node_b: a.node_id,
                });
            }
        }
        distinct += 1;
    }
    if distinct < constraints.min_distinct_failure_domains {
        return Err(PlacementError::InsufficientFailureDomains {
            want: constraints.min_distinct_failure_domains,
            got: distinct,
        });
    }

    // ── Per-node eligibility, in §11.5's order ──
    for r in replicas {
        if constraints.tenant_id != 0 && r.tenant_id != constraints.tenant_id {
            return Err(PlacementError::TenantIsolationViolation {
                node: r.node_id,
                node_tenant: r.tenant_id,
                want_tenant: constraints.tenant_id,
            });
        }
        if constraints.residency_zone != 0 && r.residency_zone != constraints.residency_zone {
            return Err(PlacementError::ResidencyViolation {
                node: r.node_id,
                zone: r.residency_zone,
                want_zone: constraints.residency_zone,
            });
        }
        let missing = constraints.required_capabilities & !r.capabilities;
        if missing != 0 {
            return Err(PlacementError::MissingCapability {
                node: r.node_id,
                missing,
            });
        }
        if constraints.required_format_version != 0
            && r.storage_format_version != constraints.required_format_version
        {
            return Err(PlacementError::FormatVersionMismatch {
                node: r.node_id,
                have: r.storage_format_version,
                want: constraints.required_format_version,
            });
        }
        if r.free_capacity_mib < constraints.min_free_capacity_mib {
            return Err(PlacementError::InsufficientCapacity {
                node: r.node_id,
                free_mib: r.free_capacity_mib,
                want_mib: constraints.min_free_capacity_mib,
            });
        }
        if r.write_endurance_pct < constraints.min_endurance_pct {
            return Err(PlacementError::InsufficientEndurance {
                node: r.node_id,
                pct: r.write_endurance_pct,
                want_pct: constraints.min_endurance_pct,
            });
        }
        if r.leader_count > constraints.max_leaders_per_node {
            return Err(PlacementError::LeaderCountExceeded {
                node: r.node_id,
                leaders: r.leader_count,
                limit: constraints.max_leaders_per_node,
            });
        }
        if r.control_plane_reserve_mib < constraints.control_plane_reserve_mib {
            return Err(PlacementError::ControlPlaneReserveViolation {
                node: r.node_id,
                reserve_mib: r.control_plane_reserve_mib,
                want_mib: constraints.control_plane_reserve_mib,
            });
        }
    }
    Ok(())
}

// ── Rolling format upgrade (phase 10) ─────────────────────────────────

/// Phases of a rolling storage-format upgrade (§26 phase 10's "rolling
/// format upgrade").
///
/// The ordering is the safety property: every replica must be *able to
/// read* the new format before any replica *writes* it, because a
/// replica that cannot read what its peers write cannot apply, snapshot,
/// or recover.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpgradePhase {
    /// The target format is declared; nothing has changed.
    Announced = 1,
    /// Every replica has been upgraded to a build that can READ the
    /// target format.
    ReadersUpgraded = 2,
    /// Every replica runs the new build.
    WritersUpgraded = 3,
    /// The target format is enabled for writes. THE GATE.
    FormatEnabled = 4,
    /// The old format is no longer produced anywhere. Terminal.
    Complete = 5,
}

impl UpgradePhase {
    pub const ALL: [Self; 5] = [
        Self::Announced,
        Self::ReadersUpgraded,
        Self::WritersUpgraded,
        Self::FormatEnabled,
        Self::Complete,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Announced),
            2 => Some(Self::ReadersUpgraded),
            3 => Some(Self::WritersUpgraded),
            4 => Some(Self::FormatEnabled),
            5 => Some(Self::Complete),
            _ => None,
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// One replica's readiness for the target format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaReadiness {
    pub node_id: u32,
    /// The highest storage format version this replica can READ.
    pub max_readable_format: u16,
    /// The format version this replica currently WRITES.
    pub writing_format: u16,
}

impl ReplicaReadiness {
    pub const EMPTY: ReplicaReadiness = ReplicaReadiness {
        node_id: 0,
        max_readable_format: 0,
        writing_format: 0,
    };
}

/// Why an upgrade step is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpgradeError {
    /// Phases advance by exactly one and never go backwards.
    IllegalTransition {
        from: UpgradePhase,
        to: UpgradePhase,
    },
    /// **The phase-10 gate.** A replica cannot read the target format,
    /// so enabling it would strand that replica.
    ReplicaCannotReadFormat {
        node: u32,
        max_readable: u16,
        required: u16,
    },
    /// A replica still writes a format others were not upgraded for.
    ReplicaWritesUnexpectedFormat { node: u32, writing: u16 },
    /// Downgrades are not a rolling upgrade.
    NotAnUpgrade { from: u16, to: u16 },
    /// No replicas listed: an upgrade of nothing is not evidence.
    EmptyPlan,
}

/// A rolling format upgrade in progress (§26 phase 10).
///
/// Carries no wire form on purpose: this is control-plane planning
/// state evaluated in-process by the placement/upgrade coordinator and
/// it never crosses `lattice.data`. Giving it an encoding would invite
/// someone to persist a plan and treat it as authority, and the
/// authority for what a replica can read is the replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RollingUpgradePlan {
    pub from_format: u16,
    pub to_format: u16,
    pub phase: UpgradePhase,
    replicas: [ReplicaReadiness; MAX_PLACEMENT_REPLICAS],
    count: u8,
}

impl RollingUpgradePlan {
    pub const EMPTY: RollingUpgradePlan = RollingUpgradePlan {
        from_format: 0,
        to_format: 0,
        phase: UpgradePhase::Announced,
        replicas: [ReplicaReadiness::EMPTY; MAX_PLACEMENT_REPLICAS],
        count: 0,
    };

    pub fn new(from_format: u16, to_format: u16) -> Self {
        Self {
            from_format,
            to_format,
            ..Self::EMPTY
        }
    }

    pub fn push(&mut self, replica: ReplicaReadiness) -> Option<()> {
        let i = self.count as usize;
        if i >= MAX_PLACEMENT_REPLICAS {
            return None;
        }
        self.replicas[i] = replica;
        self.count += 1;
        Some(())
    }

    pub fn replicas(&self) -> &[ReplicaReadiness] {
        &self.replicas[..self.count as usize]
    }

    /// May the plan advance to `to`? Strictly one step forward.
    pub fn may_transition(&self, to: UpgradePhase) -> Result<(), UpgradeError> {
        if self.phase.is_terminal() || (to as u8) != (self.phase as u8) + 1 {
            return Err(UpgradeError::IllegalTransition {
                from: self.phase,
                to,
            });
        }
        Ok(())
    }

    /// **The phase-10 gate**: a format-version upgrade may not proceed
    /// while any replica cannot read the new format.
    ///
    /// Checked over EVERY replica in the plan, voters and non-voters
    /// alike: a learner that cannot read the new format cannot catch
    /// up, and an observer that cannot read it cannot serve. "Quorum
    /// can read it" is not the condition.
    pub fn may_enable_format(&self) -> Result<(), UpgradeError> {
        if self.to_format <= self.from_format {
            return Err(UpgradeError::NotAnUpgrade {
                from: self.from_format,
                to: self.to_format,
            });
        }
        if self.count == 0 {
            return Err(UpgradeError::EmptyPlan);
        }
        for r in self.replicas() {
            if r.max_readable_format < self.to_format {
                return Err(UpgradeError::ReplicaCannotReadFormat {
                    node: r.node_id,
                    max_readable: r.max_readable_format,
                    required: self.to_format,
                });
            }
        }
        Ok(())
    }

    /// Advance the plan. Enabling the format runs the gate first, so a
    /// lagging replica blocks the transition rather than being
    /// discovered afterwards.
    pub fn advance(&mut self, to: UpgradePhase) -> Result<(), UpgradeError> {
        self.may_transition(to)?;
        if to == UpgradePhase::FormatEnabled {
            self.may_enable_format()?;
        }
        self.phase = to;
        Ok(())
    }
}

// ── Independent scaling (§14.14) ──────────────────────────────────────

/// A compute-graph lifecycle action (§14.14).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(
    clippy::enum_variant_names,
    reason = "every §14.14 lifecycle action acts on a compute worker; naming them Add/Drain/Start/Replace would lose the subject the RFC names"
)]
pub enum ComputeLifecycle {
    /// "Add read or write-capable compute workers without creating new
    /// data copies."
    AddWorker = 1,
    /// "Drain or suspend compute while Lattice storage remains
    /// available and durable."
    DrainWorker = 2,
    /// "Start compute by loading catalog/routing revisions and warming
    /// disposable caches."
    StartWorker = 3,
    /// Replace a failed worker (§14.11's recovery paragraph).
    ReplaceWorker = 4,
}

impl ComputeLifecycle {
    pub const ALL: [Self; 4] = [
        Self::AddWorker,
        Self::DrainWorker,
        Self::StartWorker,
        Self::ReplaceWorker,
    ];

    /// §14.14's headline property: none of these actions copies a
    /// database file. "Adding a reader copies no database files" is
    /// phase 9's acceptance criterion and it is true for every action
    /// here, because compute holds no storage file at all (§21
    /// invariant 15).
    pub const fn copies_database_files(self) -> bool {
        false
    }

    /// Does this action require Lattice storage to be quiesced? No:
    /// §14.14 requires storage to remain "available and durable"
    /// throughout.
    pub const fn requires_storage_quiesce(self) -> bool {
        false
    }
}

/// §14.14's writer policy: "Begin with one active relational writer per
/// database plus many readers; allow multiple writer workers only after
/// cross-range transaction and compatibility invariants pass their
/// evidence gates."
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriterPolicy {
    pub active_writers: u16,
    pub readers: u16,
    /// Whether the cross-range transaction and compatibility evidence
    /// gates have passed. Until they do, a second writer is refused.
    pub multi_writer_gates_passed: bool,
}

impl WriterPolicy {
    pub const INITIAL: WriterPolicy = WriterPolicy {
        active_writers: 1,
        readers: 0,
        multi_writer_gates_passed: false,
    };

    /// May this policy admit another writer?
    pub const fn may_add_writer(&self) -> bool {
        self.active_writers == 0 || self.multi_writer_gates_passed
    }

    /// Readers are never gated — adding one copies no data (§14.14).
    pub const fn may_add_reader(&self) -> bool {
        true
    }
}

// ── Idempotency records (§9 Phase 9) ─────────────────────────────────

// `IdempotencyRecord`, `IdempotencyLookup` and `interpret_lookup` moved
// to `txn.rs` and are re-exported below. They belong with the
// transaction contracts — an idempotency record IS the durable identity
// of a committed transaction — and the engine needs them without
// mounting this whole module, which drags in db_ops and partition_map
// for a 17-byte record.
#[allow(
    unused_imports,
    reason = "re-export surface: consumers that mount this contract use a subset, exactly as with the db_context and db_ops re-exports above"
)]
pub use txn::{
    interpret_lookup, IdempotencyLookup, IdempotencyRecord, IDEMPOTENCY_RECORD_WIRE_LEN,
};
