//! Cross-range transaction contracts — RFC database foundation §13.
//!
//! Phase 5 slice 1: the durable record shapes and the *decision rules*
//! for cross-range serializable transactions. This file is contracts
//! only — no ports, no I/O, no routing, no module. The transaction
//! coordinator module is a later slice; it will be a thin pump over the
//! functions defined here, in the same relationship `timestamp_allocator`
//! has to `mvcc.rs`.
//!
//! §13.2 fixes the shape of the protocol, and two sentences of it are
//! the whole reason this file exists:
//!
//! > Replicate the final participant proof and `COMMITTED` state in the
//! > home range. **Treat that home transaction record as the decision
//! > authority.** … Readers that encounter an intent consult, push, or
//! > help resolve the authoritative transaction record.
//!
//! and
//!
//! > If the client cannot learn the result before deadline, the response
//! > must say that delivery is indeterminate, not that the transaction
//! > necessarily failed.
//!
//! Those become [`resolve_intent`] and [`retry_class_for`] respectively.
//!
//! ## What is authoritative, and what is only a hint
//!
//! - The [`TransactionRecord`] in the **home range** is the sole
//!   authority for the outcome (§21 invariant 9). Nothing else decides:
//!   not the client's disconnect, not a participant's local view, not
//!   the presence or absence of an intent, not elapsed wall time.
//! - An [`IntentRecord`] in a participant range is a *provisional* value
//!   plus a **pointer to that authority**. It carries `home_range_id` for
//!   exactly one purpose: a reader that trips over an intent must know
//!   where to go and ask (§13.2 step 8).
//! - An intent whose identity does not match the record it was resolved
//!   against never produces a value. It produces a typed error
//!   ([`IntentResolution::WrongTransaction`] /
//!   [`IntentResolution::StaleEpoch`]) and the reader fails closed.
//!
//! ## The one thing that must never happen
//!
//! §21 invariant 10 — *intents needed by an undecided transaction cannot
//! be compacted or lost* — has a reader-side twin that this file makes
//! executable: **an undecided intent must never resolve to "absent".**
//! Resolving `PENDING`/`STAGING` to absent silently loses a write that
//! may still commit, and no later step can detect it. So
//! [`IntentResolution::Pending`] is a distinct answer meaning *go consult
//! or push the home record*, and [`IntentResolution::is_absent`] is true
//! for `Abort` and for nothing else.
//!
//! ## Timestamps are leased, never invented
//!
//! `read_timestamp` and `provisional_commit_timestamp` are logical MVCC
//! timestamps (§10) that must come from a `mvcc::TimestampLease` held by
//! the coordinator. A coordinator that invents a timestamp breaks
//! serializability in a way no downstream check can catch, so
//! [`timestamps_leased`] is provided to assert the pairing explicitly
//! against the leases, and zero is rejected everywhere as "never leased"
//! (a lease covering timestamp 0 is not issuable: the allocator's first
//! interval starts at its committed high water and grants are cut above
//! it).
//!
//! Golden vectors: `tests/contract_txn.rs`. [`TransactionRecord`] and
//! [`IntentRecord`] are replicated, persistent formats. If a change here
//! makes a vector fail, the vector is what gets updated — and every
//! deployment carrying the old bytes is rebuilt from empty. There is no
//! version field to bump and no migration path: the formats change in
//! place. [`TXN_RECORD_TAG`] and [`INTENT_RECORD_TAG`] are fixed
//! corruption checks, never discriminators between two live layouts.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

#[path = "db_context.rs"]
mod db_context;
#[path = "internal_key.rs"]
mod internal_key;
#[path = "mvcc.rs"]
mod mvcc;

pub use db_context::RetryClass;
pub use internal_key::ValueKind;
pub use mvcc::TimestampLease;

/// Fixed leading tag on an encoded [`TransactionRecord`]. This is a
/// CORRUPTION CHECK, not a version: it never changes, and a decoder
/// that sees anything else is looking at bytes that are not a
/// transaction record. When the layout changes, the layout changes —
/// there is no second layout to tell apart from this one.
pub const TXN_RECORD_TAG: u16 = 0x5852; // "RX"
/// The same fixed tag for [`IntentRecord`]. Distinct from
/// [`TXN_RECORD_TAG`] so that an intent read where a transaction record
/// was expected — the two live in different ranges — fails closed
/// rather than decoding into plausible garbage.
pub const INTENT_RECORD_TAG: u16 = 0x5849; // "IX"

/// Hard ceiling on participants in one cross-range transaction (§13.4).
///
/// This is a *format* bound, not a policy knob: it fixes the record's
/// maximum encoded size, so it can only change with a version bump.
/// [`TxnLimits::max_participants`] is the policy knob and may only ever
/// be configured at or below this value.
pub const MAX_PARTICIPANTS: usize = 8;

/// Wire length of one participant entry: `[range_id:16][generation:u32]
/// [prepared:u8]`.
pub const PARTICIPANT_WIRE_LEN: usize = 21;
/// Fixed part of an encoded [`TransactionRecord`]: header + every field
/// except the variable participant array.
pub const TXN_RECORD_FIXED_WIRE_LEN: usize = 78;
/// Largest possible encoded [`TransactionRecord`].
pub const TXN_RECORD_MAX_WIRE_LEN: usize =
    TXN_RECORD_FIXED_WIRE_LEN + MAX_PARTICIPANTS * PARTICIPANT_WIRE_LEN;
/// Encoded length of an [`IntentRecord`] — fixed, no variable part.
pub const INTENT_WIRE_LEN: usize = 49;

// ── Status ────────────────────────────────────────────────────────────

/// Cross-range transaction status (§13.2). The status byte in the home
/// record IS the outcome; there is no other place to look.
///
/// Discriminants start at 1 so a zeroed buffer never decodes as a valid
/// status — a zeroed record is not "a pending transaction", it is
/// garbage, and [`TxnStatus::from_u8`] says so.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TxnStatus {
    /// Reads and intent replication in progress. Undecided.
    Pending = 1,
    /// All participants asked to prepare; proof not yet complete.
    /// **Still undecided** — staging is a step towards a decision, not a
    /// decision. A reader that treats `Staging` as committed reads a
    /// value that may never exist.
    Staging = 2,
    /// Durably committed. TERMINAL: no transition leaves this state.
    Committed = 3,
    /// Durably aborted. TERMINAL: no transition leaves this state.
    Aborted = 4,
}

impl TxnStatus {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Pending),
            2 => Some(Self::Staging),
            3 => Some(Self::Committed),
            4 => Some(Self::Aborted),
            _ => None,
        }
    }

    /// Terminal states are durable decisions. Nothing — not a stale
    /// retry, not a recovered coordinator, not an expiry sweep — may
    /// move a transaction out of one (§21 invariant 9).
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Aborted)
    }

    /// The outcome is not yet decided. `Pending` and `Staging` both
    /// answer "ask again"; neither may be interpreted as an outcome.
    pub const fn is_undecided(self) -> bool {
        matches!(self, Self::Pending | Self::Staging)
    }
}

/// What a provisional value in a participant range *means* once its
/// transaction commits: a write or a deletion.
///
/// Kept distinct from [`ValueKind`] because an intent is one physical
/// record kind (`ValueKind::Intent`) regardless of which logical
/// mutation it will become; [`IntentKind::resolved_value_kind`] is the
/// mapping applied at resolution time.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IntentKind {
    /// Provisional live value.
    Write = 1,
    /// Provisional deletion — resolves to a point tombstone, NOT to
    /// "absent" (an absent key and a deleted key differ to a watcher,
    /// to a compare-and-set, and to a change feed).
    Delete = 2,
}

impl IntentKind {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Write),
            2 => Some(Self::Delete),
            _ => None,
        }
    }

    /// The [`ValueKind`] this intent becomes when its transaction
    /// commits.
    pub const fn resolved_value_kind(self) -> ValueKind {
        match self {
            Self::Write => ValueKind::Value,
            Self::Delete => ValueKind::PointTombstone,
        }
    }
}

// ── Typed errors ──────────────────────────────────────────────────────

/// Every way a transaction contract check can refuse (§13.4: "exceeding
/// a bound returns a typed error before silent resource expansion").
///
/// There is deliberately no catch-all variant: a caller must be able to
/// name what it refused, and a new refusal must be a visible addition
/// here rather than a reuse of something vague.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnError {
    /// A status transition outside the legal lattice — including any
    /// attempt to leave a terminal state (`Committed`/`Aborted`).
    IllegalTransition { from: TxnStatus, to: TxnStatus },
    /// `may_commit` on a record that is not `Staging`. Commit is only
    /// reachable from a completed prepare phase.
    NotStaging { status: TxnStatus },
    /// The record names no participants. A cross-range transaction with
    /// an empty participant set has nothing to prove and must not be
    /// committed through this path (§13.1 is the single-range path).
    NoParticipants,
    /// More participants than the format or the configured limit allows.
    TooManyParticipants { count: u32, limit: u32 },
    /// The same range appears twice in the participant set — its
    /// prepared flag and generation would be ambiguous.
    DuplicateParticipant { index: u8 },
    /// A participant has not reported prepared.
    ParticipantNotPrepared { index: u8 },
    /// A participant range's descriptor generation moved after prepare:
    /// the range split, merged, or relocated under the transaction, so
    /// the prepared proof no longer describes the range being committed
    /// (§13.3, §21 invariant 6).
    ParticipantGenerationChanged {
        index: u8,
        prepared: u32,
        observed: u32,
    },
    /// A participant in the record was not present in the observations
    /// supplied to `may_commit`. Fail closed: an unobserved participant
    /// is not a satisfied one.
    ParticipantUnobserved { index: u8 },
    /// An operation named a range that is not in the participant set.
    UnknownParticipant,
    /// The intents observed in a participant range were written under a
    /// different transaction epoch than the record is committing at —
    /// committing would make abandoned intents visible.
    ParticipantEpochMismatch {
        index: u8,
        record_epoch: u32,
        observed_epoch: u32,
    },
    /// `provisional_commit_timestamp < read_timestamp`: the transaction
    /// would commit before its own snapshot.
    CommitTimestampRegression { read: u64, commit: u64 },
    /// A timestamp is zero, or outside the lease it claims to come
    /// from. Timestamps are leased (§10), never invented.
    UnleasedTimestamp { timestamp: u64 },
    /// The transaction id is zero. Zero is reserved for "not
    /// transactional" (`db_context::RequestContext::transaction_id`).
    ZeroTransactionId,
    /// `now >= expiry_unix_ms`: the transaction's lease on its own
    /// intents has lapsed and it may have been aborted by a reaper.
    Expired {
        expiry_unix_ms: u64,
        now_unix_ms: u64,
    },
    /// Declared lifetime (begin → expiry) exceeds the configured bound.
    LifetimeExceeded { lifetime_ms: u64, limit_ms: u64 },
    /// §13.4 key-count bound.
    TooManyKeys { keys: u32, limit: u32 },
    /// §13.4 byte-volume bound.
    TooManyBytes { bytes: u32, limit: u32 },
    /// §13.4 read-span bound.
    TooManyReadSpans { spans: u32, limit: u32 },
    /// §13.4 concurrent-intents-per-range bound.
    TooManyIntents { intents: u32, limit: u32 },
    /// §13.4 retry-count bound.
    TooManyRetries { retries: u32, limit: u32 },
    /// §13.4 priority-push bound. Exhausting it is how victim selection
    /// terminates instead of pushing forever.
    TooManyPriorityPushes { pushes: u32, limit: u32 },
}

// ── Participant ───────────────────────────────────────────────────────

/// One participant range of a cross-range transaction (§13.2).
///
/// Wire layout (`PARTICIPANT_WIRE_LEN` = 21, integers LE):
///
/// ```text
/// [range_id:16]        // 0..16   logical range identity (§11.3)
/// [generation:u32]     // 16..20  descriptor generation at prepare
/// [prepared:u8]        // 20      0 = not prepared, 1 = prepared
/// ```
///
/// `generation` is captured when the participant prepares and is the
/// fence that makes §13.3's "range splits and relocations preserve
/// transaction routing through descriptor generations" executable: if
/// the live descriptor generation no longer matches, the prepared proof
/// describes a range that no longer exists in that shape and the commit
/// must be refused rather than applied to whatever is there now.
///
/// `prepared` decodes fail-closed: any byte other than 0 or 1 is
/// rejected, so a corrupt or partially-written record can never read as
/// "prepared".
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Participant {
    pub range_id: [u8; 16],
    pub generation: u32,
    pub prepared: bool,
}

impl Participant {
    pub const EMPTY: Participant = Participant {
        range_id: [0; 16],
        generation: 0,
        prepared: false,
    };
}

/// A live observation of a participant range, supplied to
/// [`may_commit`]. These are *facts about the world now* — they cannot
/// be read out of the record, which is why `may_commit` takes them
/// rather than conjuring them.
///
/// - `generation`: the participant's current descriptor generation.
/// - `intent_epoch`: the transaction epoch the intents actually present
///   in that range were written under.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedParticipant {
    pub range_id: [u8; 16],
    pub generation: u32,
    pub intent_epoch: u32,
}

// ── Transaction record ────────────────────────────────────────────────

/// The authoritative home-range transaction record (§13.2, §21
/// invariant 9).
///
/// Wire layout (`TXN_RECORD_VERSION` 1, all integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]            // 0..2, 2..4
/// [transaction_id:u128]                     // 4..20
/// [home_range_id:16]                        // 20..36
/// [read_timestamp:u64]                      // 36..44
/// [provisional_commit_timestamp:u64]        // 44..52
/// [priority:u32]                            // 52..56
/// [epoch:u32]                               // 56..60
/// [status:u8]                               // 60
/// [participant_count:u8]                    // 61
/// [heartbeat_unix_ms:u64]                   // 62..70
/// [expiry_unix_ms:u64]                      // 70..78
/// [participant × count: 21 bytes each]      // 78..
/// ```
///
/// `payload_len` = `74 + count * 21`, and decode requires `src.len()`
/// to equal `4 + payload_len` exactly: truncation at any byte and any
/// trailing byte are equally fatal.
///
/// Field semantics:
///
/// - `transaction_id`: globally unique, non-zero. Wide (128-bit) for the
///   same reason `range_id` is: it is a logical identity that must not
///   collide across coordinators, restarts, or clusters, and it is the
///   idempotency identity a retrying client uses to *discover* the
///   durable decision (§13.2, §21 invariant 17).
/// - `home_range_id`: which range holds this record. Copied into every
///   intent so a reader can find the authority.
/// - `read_timestamp`: the snapshot every read was taken at (§10).
/// - `provisional_commit_timestamp`: the timestamp the writes become
///   visible at. "Provisional" until `status == Committed`; after that
///   it is the commit timestamp, full stop, and it may be *later* than
///   the read timestamp because §13.2 step 5 can push it forward to
///   preserve serializability.
/// - `priority`: contention/victim-selection rank (§13.4). Higher wins
///   a push; carried in the record so victim selection is decided from
///   replicated state, not from a local guess.
/// - `epoch`: the transaction's incarnation. Bumped when a coordinator
///   restarts or the transaction is pushed and must re-lay its intents.
///   Intents from an older epoch are abandoned and must never resolve.
/// - `status`: the outcome (see [`TxnStatus`]).
/// - `heartbeat_unix_ms` / `expiry_unix_ms`: administrative liveness
///   only, exactly as `mvcc::TimestampLease::lease_expiry_unix_ms` is —
///   they bound how long an abandoned transaction can hold intents
///   before a reaper may *replicate* an abort. They take part in NO
///   ordering or visibility decision. Expiry does not abort anything by
///   itself; only a replicated `Aborted` status does (§21 invariant 12:
///   expiry is a replicated decision, not a local-clock side effect).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransactionRecord {
    pub transaction_id: u128,
    pub home_range_id: [u8; 16],
    pub read_timestamp: u64,
    pub provisional_commit_timestamp: u64,
    pub priority: u32,
    pub epoch: u32,
    pub status: TxnStatus,
    pub heartbeat_unix_ms: u64,
    pub expiry_unix_ms: u64,
    participant_count: u8,
    participants: [Participant; MAX_PARTICIPANTS],
}

impl TransactionRecord {
    /// A zero record with no participants. Not a valid transaction (its
    /// id is zero); used to initialise fixed-size state fields.
    pub const EMPTY: TransactionRecord = TransactionRecord {
        transaction_id: 0,
        home_range_id: [0; 16],
        read_timestamp: 0,
        provisional_commit_timestamp: 0,
        priority: 0,
        epoch: 0,
        status: TxnStatus::Pending,
        heartbeat_unix_ms: 0,
        expiry_unix_ms: 0,
        participant_count: 0,
        participants: [Participant::EMPTY; MAX_PARTICIPANTS],
    };

    /// Begin a transaction record. Fails closed on a zero transaction id
    /// or an unleased (zero) read timestamp.
    pub fn begin(
        transaction_id: u128,
        home_range_id: [u8; 16],
        read_timestamp: u64,
        priority: u32,
        epoch: u32,
        heartbeat_unix_ms: u64,
        expiry_unix_ms: u64,
    ) -> Result<Self, TxnError> {
        if transaction_id == 0 {
            return Err(TxnError::ZeroTransactionId);
        }
        if read_timestamp == 0 {
            return Err(TxnError::UnleasedTimestamp { timestamp: 0 });
        }
        Ok(Self {
            transaction_id,
            home_range_id,
            read_timestamp,
            provisional_commit_timestamp: read_timestamp,
            priority,
            epoch,
            status: TxnStatus::Pending,
            heartbeat_unix_ms,
            expiry_unix_ms,
            participant_count: 0,
            participants: [Participant::EMPTY; MAX_PARTICIPANTS],
        })
    }

    pub fn participant_count(&self) -> usize {
        self.participant_count as usize
    }

    pub fn participants(&self) -> &[Participant] {
        &self.participants[..self.participant_count as usize]
    }

    /// Position of `range_id` in the participant set, if present.
    pub fn participant_index(&self, range_id: &[u8; 16]) -> Option<usize> {
        self.participants()
            .iter()
            .position(|p| &p.range_id == range_id)
    }

    /// Add a participant at its prepare-time descriptor generation.
    /// Fails closed when the set is full or the range is already
    /// present — a second entry for one range would make its prepared
    /// flag ambiguous.
    pub fn add_participant(
        &mut self,
        range_id: [u8; 16],
        generation: u32,
    ) -> Result<usize, TxnError> {
        if let Some(i) = self.participant_index(&range_id) {
            return Err(TxnError::DuplicateParticipant { index: i as u8 });
        }
        let i = self.participant_count as usize;
        if i >= MAX_PARTICIPANTS {
            return Err(TxnError::TooManyParticipants {
                count: (i + 1) as u32,
                limit: MAX_PARTICIPANTS as u32,
            });
        }
        self.participants[i] = Participant {
            range_id,
            generation,
            prepared: false,
        };
        self.participant_count += 1;
        Ok(i)
    }

    /// Record that a participant reported prepared, at the descriptor
    /// generation it prepared under. The generation is re-stamped here
    /// (not merely compared) because prepare is the moment the proof is
    /// taken; [`may_commit`] then fences that stamp against a live
    /// observation.
    pub fn mark_prepared(
        &mut self,
        range_id: &[u8; 16],
        generation: u32,
    ) -> Result<usize, TxnError> {
        let i = self
            .participant_index(range_id)
            .ok_or(TxnError::UnknownParticipant)?;
        self.participants[i].generation = generation;
        self.participants[i].prepared = true;
        Ok(i)
    }

    /// Encoded length of this record.
    pub fn wire_len(&self) -> usize {
        TXN_RECORD_FIXED_WIRE_LEN + self.participant_count as usize * PARTICIPANT_WIRE_LEN
    }

    /// Serialize to the versioned wire form. Returns the encoded length,
    /// or `None` if `out` is too small (fail closed — never truncate a
    /// record).
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&TXN_RECORD_TAG.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        out[4..20].copy_from_slice(&self.transaction_id.to_le_bytes());
        out[20..36].copy_from_slice(&self.home_range_id);
        out[36..44].copy_from_slice(&self.read_timestamp.to_le_bytes());
        out[44..52].copy_from_slice(&self.provisional_commit_timestamp.to_le_bytes());
        out[52..56].copy_from_slice(&self.priority.to_le_bytes());
        out[56..60].copy_from_slice(&self.epoch.to_le_bytes());
        out[60] = self.status as u8;
        out[61] = self.participant_count;
        out[62..70].copy_from_slice(&self.heartbeat_unix_ms.to_le_bytes());
        out[70..78].copy_from_slice(&self.expiry_unix_ms.to_le_bytes());
        let mut n = TXN_RECORD_FIXED_WIRE_LEN;
        for p in self.participants() {
            out[n..n + 16].copy_from_slice(&p.range_id);
            out[n + 16..n + 20].copy_from_slice(&p.generation.to_le_bytes());
            out[n + 20] = u8::from(p.prepared);
            n += PARTICIPANT_WIRE_LEN;
        }
        Some(total)
    }

    /// Parse the versioned wire form. `src` must be exactly one encoded
    /// record. Fails closed on: any length other than the exact encoded
    /// length (truncation at ANY byte, or trailing bytes), an unknown
    /// version, a declared payload length that disagrees with the
    /// participant count, an unknown status byte, a participant count
    /// above [`MAX_PARTICIPANTS`], and a prepared byte other than 0/1.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < TXN_RECORD_FIXED_WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != TXN_RECORD_TAG {
            return None;
        }
        let count = src[61] as usize;
        if count > MAX_PARTICIPANTS {
            return None;
        }
        let total = TXN_RECORD_FIXED_WIRE_LEN + count * PARTICIPANT_WIRE_LEN;
        if src.len() != total {
            return None;
        }
        let payload_len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if payload_len != total - 4 {
            return None;
        }
        let mut rec = Self {
            transaction_id: u128::from_le_bytes(src[4..20].try_into().ok()?),
            home_range_id: src[20..36].try_into().ok()?,
            read_timestamp: u64::from_le_bytes(src[36..44].try_into().ok()?),
            provisional_commit_timestamp: u64::from_le_bytes(src[44..52].try_into().ok()?),
            priority: u32::from_le_bytes(src[52..56].try_into().ok()?),
            epoch: u32::from_le_bytes(src[56..60].try_into().ok()?),
            status: TxnStatus::from_u8(src[60])?,
            heartbeat_unix_ms: u64::from_le_bytes(src[62..70].try_into().ok()?),
            expiry_unix_ms: u64::from_le_bytes(src[70..78].try_into().ok()?),
            participant_count: count as u8,
            participants: [Participant::EMPTY; MAX_PARTICIPANTS],
        };
        let mut n = TXN_RECORD_FIXED_WIRE_LEN;
        for p in rec.participants.iter_mut().take(count) {
            let prepared = match src[n + 20] {
                0 => false,
                1 => true,
                _ => return None,
            };
            *p = Participant {
                range_id: src[n..n + 16].try_into().ok()?,
                generation: u32::from_le_bytes(src[n + 16..n + 20].try_into().ok()?),
                prepared,
            };
            n += PARTICIPANT_WIRE_LEN;
        }
        Some(rec)
    }
}

// ── Intent record ─────────────────────────────────────────────────────

/// The provisional value replicated into a participant range (§13.2
/// step 3), stored under `internal_key::ValueKind::Intent`.
///
/// Wire layout (`INTENT_RECORD_VERSION` 1, all integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]      // 0..2, 2..4   payload_len = 45
/// [transaction_id:u128]               // 4..20
/// [home_range_id:16]                  // 20..36
/// [provisional_timestamp:u64]         // 36..44
/// [epoch:u32]                         // 44..48
/// [intent_kind:u8]                    // 48
/// ```
///
/// Total `INTENT_WIRE_LEN` = 49 bytes, fixed. The user value itself is
/// NOT part of this record: it lives in the value payload the intent
/// header prefixes, so an intent is resolved by rewriting a 49-byte
/// header rather than by moving bytes.
///
/// `home_range_id` is duplicated from the transaction record on purpose.
/// It is the reader's only route to the authority (§13.2 step 8): a
/// reader that finds an intent in a range knows nothing about the
/// transaction except what this record tells it, and "go ask the home
/// range" is unusable without knowing which range that is.
///
/// `epoch` pins the intent to one incarnation of the transaction. When a
/// coordinator restarts or is pushed it bumps the epoch and re-lays its
/// intents; the old ones are abandoned and
/// [`resolve_intent`] refuses them with [`IntentResolution::StaleEpoch`]
/// rather than resolving them to anything.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntentRecord {
    pub transaction_id: u128,
    pub home_range_id: [u8; 16],
    pub provisional_timestamp: u64,
    pub epoch: u32,
    pub kind: IntentKind,
}

impl IntentRecord {
    /// The physical value kind an intent is stored under, before
    /// resolution. Always `Intent` — a write intent and a delete intent
    /// are the same physical record kind.
    pub const STORED_VALUE_KIND: ValueKind = ValueKind::Intent;

    /// Build the intent a transaction lays in a participant range.
    /// Fails closed on a zero transaction id or an unleased (zero)
    /// provisional timestamp.
    pub fn new(record: &TransactionRecord, kind: IntentKind) -> Result<Self, TxnError> {
        if record.transaction_id == 0 {
            return Err(TxnError::ZeroTransactionId);
        }
        if record.provisional_commit_timestamp == 0 {
            return Err(TxnError::UnleasedTimestamp { timestamp: 0 });
        }
        Ok(Self {
            transaction_id: record.transaction_id,
            home_range_id: record.home_range_id,
            provisional_timestamp: record.provisional_commit_timestamp,
            epoch: record.epoch,
            kind,
        })
    }

    /// Serialize to the versioned wire form.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < INTENT_WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&INTENT_RECORD_TAG.to_le_bytes());
        out[2..4].copy_from_slice(&((INTENT_WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..20].copy_from_slice(&self.transaction_id.to_le_bytes());
        out[20..36].copy_from_slice(&self.home_range_id);
        out[36..44].copy_from_slice(&self.provisional_timestamp.to_le_bytes());
        out[44..48].copy_from_slice(&self.epoch.to_le_bytes());
        out[48] = self.kind as u8;
        Some(INTENT_WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on any length other
    /// than [`INTENT_WIRE_LEN`], an unknown version, a declared payload
    /// length that disagrees with the fixed layout, and an unknown
    /// intent kind.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != INTENT_WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != INTENT_RECORD_TAG {
            return None;
        }
        let payload_len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if payload_len != INTENT_WIRE_LEN - 4 {
            return None;
        }
        Some(Self {
            transaction_id: u128::from_le_bytes(src[4..20].try_into().ok()?),
            home_range_id: src[20..36].try_into().ok()?,
            provisional_timestamp: u64::from_le_bytes(src[36..44].try_into().ok()?),
            epoch: u32::from_le_bytes(src[44..48].try_into().ok()?),
            kind: IntentKind::from_u8(src[48])?,
        })
    }
}

// ── Intent resolution (the reader's rule) ─────────────────────────────

/// What a reader that encountered an intent may conclude, having
/// consulted the home transaction record (§13.2 step 8, §21 invariants
/// 9 and 10).
///
/// The five answers are exhaustive and none of them is "absent" except
/// [`IntentResolution::Abort`]. That is the point: an undecided
/// transaction's intent resolving to absent silently loses a write, and
/// a mismatched intent resolving to absent silently loses a *different*
/// transaction's write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntentResolution {
    /// The transaction committed. The value is visible at this
    /// timestamp — the home record's commit timestamp, which may be
    /// LATER than the intent's provisional timestamp (§13.2 step 5).
    Commit(u64),
    /// The transaction aborted. The intent is not a value and never
    /// was; the key reads as whatever it was before the intent.
    Abort,
    /// Undecided (`Pending` or `Staging`). The reader must consult,
    /// push, or help resolve — it must NOT guess, and it must not treat
    /// this as absent.
    Pending,
    /// The record consulted does not belong to this intent (transaction
    /// id or home range mismatch). A typed error, never a value: the
    /// reader has asked the wrong authority and must find the right one
    /// or fail closed.
    WrongTransaction,
    /// The intent belongs to a different incarnation of the transaction
    /// than the record describes. Abandoned intents are removed by
    /// cleanup, never resolved to a value.
    StaleEpoch { intent: u32, record: u32 },
}

impl IntentResolution {
    /// The transaction is decided one way or the other.
    pub const fn is_decided(self) -> bool {
        matches!(self, Self::Commit(_) | Self::Abort)
    }

    /// The key reads as absent-because-of-this-intent. True for `Abort`
    /// and for NOTHING else — see §21 invariant 10 in the module docs.
    pub const fn is_absent(self) -> bool {
        matches!(self, Self::Abort)
    }

    /// A value is visible; the timestamp it is visible at.
    pub const fn visible_at(self) -> Option<u64> {
        match self {
            Self::Commit(ts) => Some(ts),
            _ => None,
        }
    }

    /// The reader must go and do something (consult/push, or refresh its
    /// idea of where the authority is) before it can answer.
    pub const fn requires_action(self) -> bool {
        !self.is_decided()
    }
}

/// The reader's rule (§13.2 step 8): resolve `intent` against the
/// **home** transaction record it names.
///
/// `record` MUST be the record read from `intent.home_range_id`. The
/// function checks that pairing rather than assuming it — a reader that
/// resolves an intent against a record from the wrong range, or against
/// a record for a different transaction, gets
/// [`IntentResolution::WrongTransaction`] and no value.
///
/// Checks, in order (the order matters — identity before outcome, so a
/// mismatched record can never contribute its status):
///
/// 1. `transaction_id` must match → else `WrongTransaction`.
/// 2. `home_range_id` must match → else `WrongTransaction`.
/// 3. `epoch` must match exactly → else `StaleEpoch`. Both directions
///    are errors: a lower intent epoch is an abandoned intent, and a
///    higher one means the reader is holding a stale record and must
///    re-read rather than decide.
/// 4. Status: `Committed` → visible at the record's commit timestamp;
///    `Aborted` → absent; `Pending`/`Staging` → `Pending`.
///
/// Note step 4 takes the timestamp from the **record**, not the intent.
/// The intent's provisional timestamp is a hint written before the
/// decision; the record's is the decision.
pub fn resolve_intent(intent: &IntentRecord, record: &TransactionRecord) -> IntentResolution {
    if intent.transaction_id != record.transaction_id {
        return IntentResolution::WrongTransaction;
    }
    if intent.home_range_id != record.home_range_id {
        return IntentResolution::WrongTransaction;
    }
    if intent.epoch != record.epoch {
        return IntentResolution::StaleEpoch {
            intent: intent.epoch,
            record: record.epoch,
        };
    }
    match record.status {
        TxnStatus::Committed => IntentResolution::Commit(record.provisional_commit_timestamp),
        TxnStatus::Aborted => IntentResolution::Abort,
        TxnStatus::Pending | TxnStatus::Staging => IntentResolution::Pending,
    }
}

// ── Status lattice ────────────────────────────────────────────────────

/// The legal status lattice (§13.2), and the executable form of §21
/// invariant 9's consequence: **a committed transaction can never be
/// un-committed.**
///
/// Legal transitions, and only these:
///
/// ```text
/// Pending  → Staging      prepare phase begins
/// Staging  → Committed    participant proof complete, decision made
/// Pending  → Aborted      abandoned or refused before prepare
/// Staging  → Aborted      prepare failed, conflict lost, expiry reaped
/// X        → X            idempotent redelivery of the same record
/// ```
///
/// Everything else is rejected, in particular:
///
/// - `Committed → Aborted`. A stale coordinator retry, an expiry
///   sweeper working from an old read, or a recovered participant
///   replaying an abort must all bounce off this. The record is the
///   authority and it has already spoken; a reader that already
///   resolved an intent to a value can never be told otherwise.
/// - `Aborted → Committed`, for the mirror-image reason.
/// - `Pending → Committed`. Commit is only reachable through a
///   completed prepare phase; skipping `Staging` would commit without
///   participant proof.
/// - Any backwards move (`Staging → Pending`, `Committed → Staging`, …).
///
/// The `X → X` self-transitions are legal because replicated records
/// are redelivered: a re-proposed identical status is a no-op, not a
/// violation, and §21 invariant 17 requires remote mutations to stay
/// idempotently identifiable. A self-transition changes nothing, so it
/// does not weaken terminality.
pub fn validate_status_transition(from: TxnStatus, to: TxnStatus) -> Result<(), TxnError> {
    let legal = match (from, to) {
        // Idempotent redelivery of an already-durable status.
        (a, b) if a == b => true,
        (TxnStatus::Pending, TxnStatus::Staging) => true,
        (TxnStatus::Pending, TxnStatus::Aborted) => true,
        (TxnStatus::Staging, TxnStatus::Committed) => true,
        (TxnStatus::Staging, TxnStatus::Aborted) => true,
        // Terminal states have no outgoing edges, and nothing moves
        // backwards.
        _ => false,
    };
    if legal {
        Ok(())
    } else {
        Err(TxnError::IllegalTransition { from, to })
    }
}

// ── Commit preconditions ──────────────────────────────────────────────

/// May this record move to `Committed` (§13.2 steps 4–6)?
///
/// Every precondition below is checked independently and reports its own
/// typed error, so a refusal names the reason rather than a generic
/// "cannot commit":
///
/// 1. status is `Staging` — commit is only reachable through prepare;
/// 2. the participant set is non-empty and within [`MAX_PARTICIPANTS`];
/// 3. no range appears twice;
/// 4. every participant reported prepared;
/// 5. every participant was observed, and its descriptor generation is
///    unchanged since prepare (§13.3: a split/merge/relocation under the
///    transaction invalidates the proof);
/// 6. every participant's intents were written at this record's epoch;
/// 7. both timestamps are non-zero (leased) and
///    `commit_timestamp >= read_timestamp`;
/// 8. the transaction has not expired (`now < expiry_unix_ms`).
///
/// `observed` is a live view of the participant ranges — see
/// [`ObservedParticipant`]. It is a parameter, not something read out of
/// the record, because "generation unchanged since prepare" and "not
/// expired" are facts about the world now, and fail-closed checking
/// cannot invent them.
///
/// `now_unix_ms` is used ONLY for the expiry check, and expiry here means
/// "refuse to commit", never "abort". An abort is a replicated decision
/// (§21 invariant 12); this function never produces one.
pub fn may_commit(
    record: &TransactionRecord,
    observed: &[ObservedParticipant],
    now_unix_ms: u64,
) -> Result<(), TxnError> {
    if record.status != TxnStatus::Staging {
        return Err(TxnError::NotStaging {
            status: record.status,
        });
    }
    let count = record.participant_count();
    if count == 0 {
        return Err(TxnError::NoParticipants);
    }
    if count > MAX_PARTICIPANTS {
        return Err(TxnError::TooManyParticipants {
            count: count as u32,
            limit: MAX_PARTICIPANTS as u32,
        });
    }
    let parts = record.participants();
    for (i, p) in parts.iter().enumerate() {
        // Duplicate ranges make the prepared flag ambiguous; check
        // before trusting any of them.
        if parts[..i].iter().any(|q| q.range_id == p.range_id) {
            return Err(TxnError::DuplicateParticipant { index: i as u8 });
        }
        if !p.prepared {
            return Err(TxnError::ParticipantNotPrepared { index: i as u8 });
        }
        let obs = observed
            .iter()
            .find(|o| o.range_id == p.range_id)
            .ok_or(TxnError::ParticipantUnobserved { index: i as u8 })?;
        if obs.generation != p.generation {
            return Err(TxnError::ParticipantGenerationChanged {
                index: i as u8,
                prepared: p.generation,
                observed: obs.generation,
            });
        }
        if obs.intent_epoch != record.epoch {
            return Err(TxnError::ParticipantEpochMismatch {
                index: i as u8,
                record_epoch: record.epoch,
                observed_epoch: obs.intent_epoch,
            });
        }
    }
    if record.read_timestamp == 0 {
        return Err(TxnError::UnleasedTimestamp { timestamp: 0 });
    }
    if record.provisional_commit_timestamp == 0 {
        return Err(TxnError::UnleasedTimestamp { timestamp: 0 });
    }
    if record.provisional_commit_timestamp < record.read_timestamp {
        return Err(TxnError::CommitTimestampRegression {
            read: record.read_timestamp,
            commit: record.provisional_commit_timestamp,
        });
    }
    if record.expiry_unix_ms != 0 && now_unix_ms >= record.expiry_unix_ms {
        return Err(TxnError::Expired {
            expiry_unix_ms: record.expiry_unix_ms,
            now_unix_ms,
        });
    }
    Ok(())
}

/// Assert that a record's timestamps really came from leases (§10).
///
/// A coordinator holds one or more `TimestampLease`s and cuts its read
/// and commit timestamps from them. This checks the pairing explicitly:
/// each timestamp must be covered by the lease it is claimed to come
/// from, and zero — which no lease can cover, because a granted interval
/// is cut above a committed high water — is always refused.
///
/// The two leases may be the same lease; a transaction whose commit
/// timestamp was pushed forward (§13.2 step 5) will typically present a
/// later one.
pub fn timestamps_leased(
    record: &TransactionRecord,
    read_lease: &TimestampLease,
    commit_lease: &TimestampLease,
) -> Result<(), TxnError> {
    if !mvcc::lease_covers(read_lease, record.read_timestamp) {
        return Err(TxnError::UnleasedTimestamp {
            timestamp: record.read_timestamp,
        });
    }
    if !mvcc::lease_covers(commit_lease, record.provisional_commit_timestamp) {
        return Err(TxnError::UnleasedTimestamp {
            timestamp: record.provisional_commit_timestamp,
        });
    }
    if record.provisional_commit_timestamp < record.read_timestamp {
        return Err(TxnError::CommitTimestampRegression {
            read: record.read_timestamp,
            commit: record.provisional_commit_timestamp,
        });
    }
    Ok(())
}

// ── Outcome classification ────────────────────────────────────────────

/// How a cross-range transaction ended, from the coordinator's point of
/// view, at the moment it must answer the client (§13.2, §22).
///
/// The distinction that carries the most weight is between
/// [`TxnOutcome::DeadlineNoDecision`] and
/// [`TxnOutcome::OutcomeUnknown`]:
///
/// - `DeadlineNoDecision` — the deadline passed and the coordinator
///   *knows* no `Committed` record was replicated. Nothing happened;
///   `DeadlineExpired` is the honest answer.
/// - `OutcomeUnknown` — intents were replicated, a commit may or may not
///   have been recorded, and the coordinator cannot learn which before
///   the deadline. §13.2: *"the response must say that delivery is
///   indeterminate, not that the transaction necessarily failed."* This
///   maps to [`RetryClass::IndeterminateDelivery`], whose contract is
///   that the outcome remains discoverable through the transaction id
///   (§21 invariant 17).
///
/// Collapsing the second into the first is the specific bug this enum
/// exists to prevent: a client told "failed" retries a transaction that
/// may already have committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnOutcome {
    /// Durably committed. Nothing to retry.
    Committed,
    /// Durably aborted; the client may start a new transaction.
    Aborted,
    /// Lost a write-write conflict or a priority push.
    Conflict,
    /// The read set could not be refreshed at a later timestamp
    /// (§13.2 step 5).
    ReadRefreshFailed,
    /// The transaction's own expiry lapsed and it was reaped.
    Expired,
    /// A participant range split, merged, or relocated; descriptors must
    /// be refreshed before retrying.
    ParticipantMoved,
    /// A participant's or the home range's quorum was unavailable and
    /// the decision could not be replicated.
    QuorumUnavailable,
    /// Deadline passed with no decision replicated — proven, not
    /// assumed.
    DeadlineNoDecision,
    /// The decision may exist but could not be learned before the
    /// deadline. NOT a failure.
    OutcomeUnknown,
    /// A §13.4 bound was exceeded, or the request was malformed.
    BoundExceeded,
    /// Serializable execution was required and this composition cannot
    /// provide it (§13.3: snapshot isolation must not be labelled
    /// serializable).
    Unsupported,
}

/// Map an outcome onto the `db_context` retry vocabulary.
///
/// `None` for [`TxnOutcome::Committed`]: success is not a retry class.
/// Every other outcome has exactly one class, and
/// [`TxnOutcome::OutcomeUnknown`] is the only one that maps to
/// [`RetryClass::IndeterminateDelivery`].
pub const fn retry_class_for(outcome: TxnOutcome) -> Option<RetryClass> {
    match outcome {
        TxnOutcome::Committed => None,
        // An abort is a decided, retryable contention outcome: the
        // client may run the transaction again from the top.
        TxnOutcome::Aborted
        | TxnOutcome::Conflict
        | TxnOutcome::ReadRefreshFailed
        | TxnOutcome::Expired => Some(RetryClass::RetryableContention),
        TxnOutcome::ParticipantMoved => Some(RetryClass::StaleRoute),
        TxnOutcome::QuorumUnavailable => Some(RetryClass::UnavailableQuorum),
        TxnOutcome::DeadlineNoDecision => Some(RetryClass::DeadlineExpired),
        TxnOutcome::OutcomeUnknown => Some(RetryClass::IndeterminateDelivery),
        TxnOutcome::BoundExceeded => Some(RetryClass::Malformed),
        TxnOutcome::Unsupported => Some(RetryClass::UnsupportedCapability),
    }
}

// ── Bounds (§13.4) ────────────────────────────────────────────────────

/// Configurable transaction bounds (§13.4). Exceeding any of them
/// returns a typed [`TxnError`] *before* the resource is allocated —
/// never a silent expansion, and never a downgrade to a weaker mode
/// (§21 invariant 14).
///
/// Defaults are deliberately conservative: they are sized so one
/// transaction cannot dominate a range's apply budget or intent space on
/// the pi5-class nodes Lattice targets first, and every one of them is
/// meant to be raised knowingly per deployment rather than discovered by
/// hitting it.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxnLimits {
    /// Maximum distinct keys written by one transaction.
    pub max_keys: u32,
    /// Maximum total byte volume of one transaction's writes.
    pub max_bytes: u32,
    /// Maximum bounded read spans recorded for conflict validation
    /// (§13.3). Each span costs memory in the timestamp cache.
    pub max_read_spans: u32,
    /// Maximum participant ranges. Must be `<= MAX_PARTICIPANTS`, which
    /// is the format bound.
    pub max_participants: u32,
    /// Maximum declared lifetime, begin → expiry, milliseconds.
    pub max_lifetime_ms: u64,
    /// Heartbeat interval, milliseconds. Must divide comfortably into
    /// `max_lifetime_ms`; a transaction that misses heartbeats becomes
    /// reapable.
    pub heartbeat_interval_ms: u64,
    /// Maximum concurrent intents one range may hold for one tenant.
    pub max_intents_per_range: u32,
    /// Maximum client retries of one transaction identity.
    pub max_retries: u32,
    /// Maximum priority pushes before victim selection must terminate
    /// (§13.4: "ensure eventual victim selection … avoid unbounded wait
    /// queues").
    pub max_priority_pushes: u32,
}

impl TxnLimits {
    /// Conservative defaults. Rationale per field:
    ///
    /// - `max_keys` 1024 / `max_bytes` 1 MiB — one transaction's writes
    ///   must fit comfortably inside a bounded apply step and a single
    ///   replicated proposal.
    /// - `max_read_spans` 128 — the read set is re-validated on refresh;
    ///   an unbounded one turns refresh into a scan.
    /// - `max_participants` 8 — equals the format bound
    ///   [`MAX_PARTICIPANTS`]; commit latency is the max over
    ///   participants, so width is a latency multiplier.
    /// - `max_lifetime_ms` 30_000 with `heartbeat_interval_ms` 1_000 —
    ///   30 heartbeats of slack before a live transaction looks dead.
    /// - `max_intents_per_range` 4096 — bounds how much provisional
    ///   state a range must carry (and a reader must resolve) before
    ///   backpressure.
    /// - `max_retries` 8 / `max_priority_pushes` 4 — small, so
    ///   contention terminates in a typed error instead of a livelock.
    pub const DEFAULT: TxnLimits = TxnLimits {
        max_keys: 1024,
        max_bytes: 1 << 20,
        max_read_spans: 128,
        max_participants: MAX_PARTICIPANTS as u32,
        max_lifetime_ms: 30_000,
        heartbeat_interval_ms: 1_000,
        max_intents_per_range: 4096,
        max_retries: 8,
        max_priority_pushes: 4,
    };
}

/// What a transaction has actually consumed so far, measured by the
/// coordinator. Paired with [`TxnLimits`] by [`bounds_check`].
///
/// `began_unix_ms` is administrative, like the record's heartbeat and
/// expiry fields: it exists to bound lifetime, and takes part in no
/// ordering or visibility decision.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct TxnUsage {
    /// When the transaction began, unix ms.
    pub began_unix_ms: u64,
    /// Distinct keys written.
    pub keys: u32,
    /// Byte volume written.
    pub bytes: u32,
    /// Bounded read spans recorded.
    pub read_spans: u32,
    /// Concurrent intents held in the most loaded participant range.
    pub intents_in_range: u32,
    /// Retries of this transaction identity so far.
    pub retries: u32,
    /// Priority pushes performed so far.
    pub priority_pushes: u32,
}

/// Enforce the §13.4 bounds. Each limit is checked in isolation and
/// returns its own typed error, so a refusal is diagnosable and a test
/// can violate exactly one thing at a time.
///
/// `usage` is a parameter for the same reason `observed` is on
/// [`may_commit`]: the record does not carry its own resource
/// consumption, and a bound that cannot be measured cannot be enforced.
///
/// Lifetime is checked as the *declared* span `expiry_unix_ms -
/// began_unix_ms`, so an over-long transaction is refused when it is
/// created rather than when it eventually overruns. An `expiry_unix_ms`
/// of 0 means "no declared expiry", which the lifetime bound refuses:
/// an unbounded transaction is exactly what §13.4 exists to prevent.
pub fn bounds_check(
    record: &TransactionRecord,
    usage: &TxnUsage,
    limits: &TxnLimits,
) -> Result<(), TxnError> {
    if usage.keys > limits.max_keys {
        return Err(TxnError::TooManyKeys {
            keys: usage.keys,
            limit: limits.max_keys,
        });
    }
    if usage.bytes > limits.max_bytes {
        return Err(TxnError::TooManyBytes {
            bytes: usage.bytes,
            limit: limits.max_bytes,
        });
    }
    if usage.read_spans > limits.max_read_spans {
        return Err(TxnError::TooManyReadSpans {
            spans: usage.read_spans,
            limit: limits.max_read_spans,
        });
    }
    let participants = record.participant_count() as u32;
    let participant_limit = if limits.max_participants > MAX_PARTICIPANTS as u32 {
        // The format bound always wins: a configured limit above it
        // cannot be honoured by the record encoding.
        MAX_PARTICIPANTS as u32
    } else {
        limits.max_participants
    };
    if participants > participant_limit {
        return Err(TxnError::TooManyParticipants {
            count: participants,
            limit: participant_limit,
        });
    }
    if usage.intents_in_range > limits.max_intents_per_range {
        return Err(TxnError::TooManyIntents {
            intents: usage.intents_in_range,
            limit: limits.max_intents_per_range,
        });
    }
    if usage.retries > limits.max_retries {
        return Err(TxnError::TooManyRetries {
            retries: usage.retries,
            limit: limits.max_retries,
        });
    }
    if usage.priority_pushes > limits.max_priority_pushes {
        return Err(TxnError::TooManyPriorityPushes {
            pushes: usage.priority_pushes,
            limit: limits.max_priority_pushes,
        });
    }
    // Saturating: an expiry at or before the begin time yields 0, which
    // passes the bound but is refused below as an undeclared expiry.
    let lifetime_ms = record.expiry_unix_ms.saturating_sub(usage.began_unix_ms);
    if record.expiry_unix_ms == 0 || lifetime_ms > limits.max_lifetime_ms {
        return Err(TxnError::LifetimeExceeded {
            lifetime_ms,
            limit_ms: limits.max_lifetime_ms,
        });
    }
    Ok(())
}

/// Has the transaction's administrative expiry lapsed? A true answer
/// licenses a reaper to *propose* an abort; it is never itself an
/// outcome (§21 invariant 12). `expiry_unix_ms == 0` means no declared
/// expiry and never expires.
pub fn is_expired(record: &TransactionRecord, now_unix_ms: u64) -> bool {
    record.expiry_unix_ms != 0 && now_unix_ms >= record.expiry_unix_ms
}

// ── Idempotency records (§14, Phase 9) ────────────────────────────────
//
// Moved here from `data_surface.rs`, which re-exports them. An
// idempotency record IS the durable identity of a committed mutation,
// so it belongs with the transaction contracts — and the engine needs
// it without mounting data_surface, whose own dependency tree (db_ops,
// partition_map) is a heavy price for a 17-byte record.

/// The durable answer to "did my mutation happen?", stored in the
/// owning range under the identity `Discoverability::ViaIdempotencyIdentity`
/// names. Without this record that variant points at nothing, which is
/// the gap this contract closes at its layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdempotencyRecord {
    pub idempotency_key: u64,
    /// Revision the mutation committed at. Non-zero for every recorded
    /// outcome: a committed mutation always has a revision, so zero is
    /// available as the "not a real record" tripwire.
    pub committed_revision: u64,
    /// Result code the original request returned, replayed verbatim to
    /// a retry so the second answer equals the first.
    pub result: u8,
}

/// Wire length of one [`IdempotencyRecord`].
pub const IDEMPOTENCY_RECORD_WIRE_LEN: usize = 8 + 8 + 1;

impl IdempotencyRecord {
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < IDEMPOTENCY_RECORD_WIRE_LEN || self.committed_revision == 0 {
            return None;
        }
        out[0..8].copy_from_slice(&self.idempotency_key.to_be_bytes());
        out[8..16].copy_from_slice(&self.committed_revision.to_be_bytes());
        out[16] = self.result;
        Some(IDEMPOTENCY_RECORD_WIRE_LEN)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < IDEMPOTENCY_RECORD_WIRE_LEN {
            return None;
        }
        let committed_revision = u64::from_be_bytes(src[8..16].try_into().ok()?);
        if committed_revision == 0 {
            return None;
        }
        Some(Self {
            idempotency_key: u64::from_be_bytes(src[0..8].try_into().ok()?),
            committed_revision,
            result: src[16],
        })
    }
}

/// What a lookup of an idempotency identity is allowed to conclude.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdempotencyLookup {
    /// The record exists: the mutation happened, and this is the
    /// answer it gave the first time.
    Committed { committed_revision: u64, result: u8 },
    /// No record, and the identity is NEWER than the retention floor —
    /// so a record would still be here if one had ever been written.
    /// Only under that condition may absence mean "did not happen".
    DidNotHappen,
    /// No record, and the identity is at or below the retention floor.
    /// The record may have existed and been reclaimed, so absence
    /// proves NOTHING. §21 invariant 17 requires the outcome remain
    /// discoverable; once it is not, the honest answer is that it is
    /// unknown — never the cheerful "did not happen" that would invite
    /// a duplicate retry of a mutation that already committed.
    Indeterminate,
}

/// Interpret a lookup result against the retention floor.
///
/// The floor argument is what makes this contract worth having: the
/// same "no record found" byte means two opposite things depending on
/// whether the record COULD still be there, and collapsing them is how
/// an at-most-once mutation quietly becomes at-least-once.
pub fn interpret_lookup(
    record: Option<&IdempotencyRecord>,
    identity_revision: u64,
    retention_floor: u64,
) -> IdempotencyLookup {
    match record {
        Some(r) => IdempotencyLookup::Committed {
            committed_revision: r.committed_revision,
            result: r.result,
        },
        None if identity_revision > retention_floor => IdempotencyLookup::DidNotHappen,
        None => IdempotencyLookup::Indeterminate,
    }
}

/// Outcome byte for a `Committed` lookup.
pub const IDEMPOTENCY_LOOKUP_COMMITTED: u8 = 0;
/// Outcome byte for a `DidNotHappen` lookup.
pub const IDEMPOTENCY_LOOKUP_DID_NOT_HAPPEN: u8 = 1;
/// Outcome byte for an `Indeterminate` lookup.
pub const IDEMPOTENCY_LOOKUP_INDETERMINATE: u8 = 2;

/// Wire length of an encoded [`IdempotencyLookup`]:
/// `[outcome:u8][committed_revision:u64 LE][result:u8]`. The revision and
/// result bytes are zero for the two absent outcomes; a decoder keys off
/// `outcome` and must not read them as meaningful there.
pub const IDEMPOTENCY_LOOKUP_WIRE_LEN: usize = 1 + 8 + 1;

impl IdempotencyLookup {
    /// Encode into `out`; returns the byte count, or `None` if `out` is
    /// too small.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < IDEMPOTENCY_LOOKUP_WIRE_LEN {
            return None;
        }
        let (outcome, rev, result) = match *self {
            IdempotencyLookup::Committed {
                committed_revision,
                result,
            } => (IDEMPOTENCY_LOOKUP_COMMITTED, committed_revision, result),
            IdempotencyLookup::DidNotHappen => (IDEMPOTENCY_LOOKUP_DID_NOT_HAPPEN, 0, 0),
            IdempotencyLookup::Indeterminate => (IDEMPOTENCY_LOOKUP_INDETERMINATE, 0, 0),
        };
        out[0] = outcome;
        out[1..9].copy_from_slice(&rev.to_le_bytes());
        out[9] = result;
        Some(IDEMPOTENCY_LOOKUP_WIRE_LEN)
    }

    /// Decode a wire form produced by [`encode`](Self::encode). Unknown
    /// outcome bytes are rejected rather than guessed.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < IDEMPOTENCY_LOOKUP_WIRE_LEN {
            return None;
        }
        let rev = u64::from_le_bytes(src[1..9].try_into().ok()?);
        match src[0] {
            IDEMPOTENCY_LOOKUP_COMMITTED => Some(IdempotencyLookup::Committed {
                committed_revision: rev,
                result: src[9],
            }),
            IDEMPOTENCY_LOOKUP_DID_NOT_HAPPEN => Some(IdempotencyLookup::DidNotHappen),
            IDEMPOTENCY_LOOKUP_INDETERMINATE => Some(IdempotencyLookup::Indeterminate),
            _ => None,
        }
    }
}
