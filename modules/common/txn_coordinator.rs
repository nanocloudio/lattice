//! The cross-range transaction coordinator's decision logic (§13.2),
//! as a pure state machine.
//!
//! Separated from the module that will drive it for the same reason the
//! range-lifecycle machine is: the bugs in two-phase commit are
//! decision bugs, not plumbing bugs, and a decision machine that needs
//! a live three-node graph to exercise is a decision machine that does
//! not get exercised. Everything here is `no_std`, allocation-free, and
//! driven by explicit events.
//!
//! The coordinator owns one transaction at a time and drives it through
//! §13.2:
//!
//! ```text
//!   Begin        write the home record as Pending
//!   Prepare      ask every participant to stage; collect votes
//!   Stage        record moves Pending → Staging
//!   Decide       may_commit() gates Staging → Committed
//!   Resolve      tell every participant the outcome, until acked
//! ```
//!
//! Three rules run through all of it, and every one of them is a rule
//! about what the coordinator must NOT do:
//!
//! 1. **A decision is a replicated record, never an inference.** The
//!    coordinator does not abort by giving up; it aborts by writing
//!    `Aborted` and having that write acked. A transaction whose
//!    coordinator vanished is not aborted — it is undecided, and
//!    someone else has to finish it.
//!
//! 2. **Committed is terminal, from the instant it is durable.** After
//!    the commit record is acked, no timeout, no refused participant
//!    and no lost reply can produce an abort. The only remaining work
//!    is resolving intents, retried forever. Before that instant, a
//!    timeout may abort — that is the whole difference the durability
//!    point makes, and [`Coordinator::deadline`] is written around it.
//!
//! 3. **Unknown is an outcome, not a failure.** A coordinator that
//!    cannot learn the decision before its deadline reports
//!    [`TxnOutcome::OutcomeUnknown`], which maps to
//!    `RetryClass::IndeterminateDelivery` — the client re-presents its
//!    idempotency identity rather than guessing.

#![allow(
    dead_code,
    reason = "shared via #[path] into the coordinator module and host tests; each consumer uses a subset"
)]

#[path = "txn.rs"]
mod txn;

pub use txn::{
    may_commit, ObservedParticipant, Participant, TransactionRecord, TxnError, TxnOutcome,
    TxnStatus, MAX_PARTICIPANTS,
};

/// What the driver should do next. The coordinator never performs I/O;
/// it names the next action and waits to be told what happened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Write the home transaction record at this status (`KV_OP_TXN_RECORD`).
    WriteRecord(TxnStatus),
    /// Send `KV_OP_TXN_PREPARE` to this participant index.
    Prepare(usize),
    /// Send `KV_OP_TXN_RESOLVE` to this participant index. `committed`
    /// is the decision the record already carries.
    Resolve { index: usize, committed: bool },
    /// Nothing to send; waiting on a reply.
    Wait,
    /// Terminal. The transaction has this outcome and the coordinator
    /// is free.
    Done(TxnOutcome),
}

/// Where the transaction is in §13.2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Nothing started.
    Idle,
    /// The `Pending` record is written but not yet acked.
    OpeningRecord,
    /// Collecting participant votes.
    Preparing,
    /// Every vote is in and they were all yes; moving the record to
    /// `Staging`.
    Staging,
    /// The record is `Staging`; `may_commit` gates the next move.
    Deciding,
    /// The decision is durable. Resolving intents until every
    /// participant acks.
    Resolving,
    /// Terminal.
    Done,
}

/// Per-participant progress. Deliberately three independent flags
/// rather than one enum: a participant can be voted-and-not-resolved,
/// and conflating "replied" with "prepared" is how a refusal turns into
/// a commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
struct Slot {
    /// A prepare reply arrived (either vote).
    voted: bool,
    /// That reply was a yes.
    prepared: bool,
    /// A resolve for this participant has been acked.
    resolved: bool,
    /// A prepare has been sent and not yet answered.
    prepare_inflight: bool,
    /// A resolve has been sent and not yet acked.
    resolve_inflight: bool,
}

/// One in-flight cross-range transaction.
pub struct Coordinator {
    pub record: TransactionRecord,
    phase: Phase,
    slots: [Slot; MAX_PARTICIPANTS],
    count: usize,
    /// Set once the record's `Committed` or `Aborted` write is ACKED.
    /// This is the durability point rule 2 is written around.
    decided: bool,
    /// The decision, valid only when `decided`.
    committed: bool,
    /// Terminal outcome, valid only in `Phase::Done`.
    outcome: TxnOutcome,
    /// Latched by a deadline or a failed commit gate. Sticky on
    /// purpose: once the coordinator has concluded it must not commit,
    /// no later event may talk it back into committing.
    force_abort: bool,
}

impl Coordinator {
    /// Open a transaction over `participants`. Refuses an empty or
    /// over-large participant set up front rather than discovering it
    /// at commit time, when intents are already staged.
    pub fn begin(
        record: TransactionRecord,
        participants: &[Participant],
    ) -> Result<Self, TxnError> {
        if participants.is_empty() {
            return Err(TxnError::NoParticipants);
        }
        if participants.len() > MAX_PARTICIPANTS {
            return Err(TxnError::TooManyParticipants {
                count: participants.len() as u32,
                limit: MAX_PARTICIPANTS as u32,
            });
        }
        let mut record = record;
        for p in participants {
            record.add_participant(p.range_id, p.generation)?;
        }
        record.status = TxnStatus::Pending;
        Ok(Self {
            record,
            phase: Phase::OpeningRecord,
            slots: [Slot::default(); MAX_PARTICIPANTS],
            count: participants.len(),
            decided: false,
            committed: false,
            outcome: TxnOutcome::OutcomeUnknown,
            force_abort: false,
        })
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// Has a decision been durably recorded? After this is true, rule 2
    /// applies: nothing can turn a commit into an abort.
    pub fn is_decided(&self) -> bool {
        self.decided
    }

    pub fn participant_count(&self) -> usize {
        self.count
    }

    /// The next action, and a claim on it: `Prepare` and `Resolve`
    /// mark the slot in-flight, so a second call returns the NEXT
    /// unsent participant rather than the same one twice. That is what
    /// makes a drain loop — call until `Wait` — send each participant
    /// exactly one message per round. `on_send_lost` releases a claim
    /// when a send did not make it.
    pub fn step(&mut self) -> Step {
        match self.phase {
            Phase::Idle => Step::Wait,
            Phase::OpeningRecord => Step::WriteRecord(TxnStatus::Pending),
            Phase::Preparing => {
                for i in 0..self.count {
                    let s = self.slots[i];
                    if !s.voted && !s.prepare_inflight {
                        self.slots[i].prepare_inflight = true;
                        return Step::Prepare(i);
                    }
                }
                Step::Wait
            }
            Phase::Staging => Step::WriteRecord(TxnStatus::Staging),
            Phase::Deciding => Step::WriteRecord(if self.commit_is_permitted() {
                TxnStatus::Committed
            } else {
                TxnStatus::Aborted
            }),
            Phase::Resolving => {
                for i in 0..self.count {
                    let s = self.slots[i];
                    if !s.resolved && !s.resolve_inflight {
                        self.slots[i].resolve_inflight = true;
                        return Step::Resolve {
                            index: i,
                            committed: self.committed,
                        };
                    }
                }
                Step::Wait
            }
            Phase::Done => Step::Done(self.outcome),
        }
    }

    /// The home record write at `status` was acked.
    pub fn on_record_acked(&mut self, status: TxnStatus) {
        match (self.phase, status) {
            (Phase::OpeningRecord, TxnStatus::Pending) => {
                self.record.status = TxnStatus::Pending;
                self.phase = Phase::Preparing;
            }
            (Phase::Staging, TxnStatus::Staging) => {
                self.record.status = TxnStatus::Staging;
                self.phase = Phase::Deciding;
            }
            (Phase::Deciding, TxnStatus::Committed) => {
                // THE durability point. From here nothing aborts.
                self.record.status = TxnStatus::Committed;
                self.decided = true;
                self.committed = true;
                self.outcome = TxnOutcome::Committed;
                self.phase = Phase::Resolving;
            }
            (Phase::Deciding, TxnStatus::Aborted) => {
                self.record.status = TxnStatus::Aborted;
                self.decided = true;
                self.committed = false;
                self.outcome = TxnOutcome::Aborted;
                self.phase = Phase::Resolving;
            }
            // Anything else is a stale or duplicated ack. Ignore it:
            // acks are redelivered, and acting on one out of phase is
            // how a coordinator walks backwards.
            _ => {}
        }
    }

    /// The home record write was REFUSED by the lattice (the store
    /// returned `KV_RESULT_CAS_FAILED` for an illegal status move).
    ///
    /// This is not a transport failure — it means someone else already
    /// moved this record, so this coordinator is not the authority it
    /// thought it was. It must go and learn the real status rather than
    /// retry its own write.
    pub fn on_record_refused(&mut self) {
        self.phase = Phase::Done;
        self.outcome = TxnOutcome::OutcomeUnknown;
    }

    /// A participant answered the prepare. `prepared` is its vote.
    pub fn on_prepare_reply(&mut self, index: usize, prepared: bool) {
        if index >= self.count || self.phase != Phase::Preparing {
            return;
        }
        let s = &mut self.slots[index];
        s.prepare_inflight = false;
        // First vote wins. A redelivered reply must not flip a no into
        // a yes.
        if s.voted {
            return;
        }
        s.voted = true;
        s.prepared = prepared;
        if prepared {
            let range_id = self.record.participants()[index].range_id;
            let generation = self.record.participants()[index].generation;
            let _ = self.record.mark_prepared(&range_id, generation);
        }
        if (0..self.count).all(|i| self.slots[i].voted) {
            // Staging asserts that participant proof is complete. A
            // transaction carrying a refusal has no such proof, so it
            // must NOT pass through Staging on its way to Aborted —
            // that would write a false claim into a durable record that
            // outlives this coordinator, and `may_commit` reads the
            // status as evidence. Pending → Aborted is the legal direct
            // path and is the honest one.
            self.phase = if (0..self.count).all(|i| self.slots[i].prepared) {
                Phase::Staging
            } else {
                Phase::Deciding
            };
        }
    }

    /// A participant acked its resolve.
    pub fn on_resolve_acked(&mut self, index: usize) {
        if index >= self.count || self.phase != Phase::Resolving {
            return;
        }
        self.slots[index].resolve_inflight = false;
        self.slots[index].resolved = true;
        if (0..self.count).all(|i| self.slots[i].resolved) {
            self.phase = Phase::Done;
        }
    }

    /// A send was lost; allow it to be re-issued. Retry, not decision:
    /// this never changes a vote or an outcome.
    pub fn on_send_lost(&mut self, index: usize) {
        if index >= self.count {
            return;
        }
        self.slots[index].prepare_inflight = false;
        self.slots[index].resolve_inflight = false;
    }

    /// The coordinator's deadline passed.
    ///
    /// Rule 2 lives here. BEFORE the decision is durable, a deadline
    /// aborts — the coordinator writes `Aborted`, which is a real
    /// replicated decision, and the transaction is properly dead.
    /// AFTER it is durable, a deadline changes nothing about the
    /// outcome: a committed transaction stays committed and its intents
    /// must still be resolved, so the coordinator keeps going. It never
    /// reports a commit as unknown, because the commit is a fact it
    /// already holds.
    pub fn on_deadline(&mut self) {
        if self.decided {
            // Committed or aborted — the outcome is known and the
            // remaining work is resolution, which is retried forever.
            return;
        }
        match self.phase {
            Phase::Done => {}
            // No decision was ever replicated, and this coordinator
            // cannot make one now without knowing the votes. Drive an
            // abort through the record so the decision is a fact rather
            // than this coordinator's private conclusion.
            _ => {
                self.phase = Phase::Deciding;
                self.force_abort = true;
            }
        }
    }

    /// Can the transaction commit? Every participant voted yes, and
    /// `may_commit` — the §13.2 gate — agrees, given what this
    /// coordinator observed of the participants.
    fn commit_is_permitted(&self) -> bool {
        if self.force_abort {
            return false;
        }
        (0..self.count).all(|i| self.slots[i].prepared)
    }

    /// Verify the commit gate against freshly observed participants
    /// (generation and intent epoch), which is the check that catches a
    /// participant that split, merged or moved between prepare and
    /// decide. Call before `WriteRecord(Committed)` is sent.
    ///
    /// Returns the reason on refusal rather than a bare `false`: a
    /// coordinator that aborts should be able to say which invariant
    /// stopped it.
    pub fn check_commit_gate(
        &self,
        observed: &[ObservedParticipant],
        now_unix_ms: u64,
    ) -> Result<(), TxnError> {
        may_commit(&self.record, observed, now_unix_ms)
    }

    /// Record that the freshly observed participants did not satisfy
    /// the gate — the transaction must abort, and it aborts by writing
    /// the record, not by this call.
    pub fn refuse_commit(&mut self) {
        self.force_abort = true;
    }

    /// The terminal outcome, once `Phase::Done`.
    pub fn outcome(&self) -> Option<TxnOutcome> {
        if self.phase == Phase::Done {
            Some(self.outcome)
        } else {
            None
        }
    }
}
