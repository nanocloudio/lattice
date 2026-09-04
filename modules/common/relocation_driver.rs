//! Relocation orchestration core — the pure decision logic that moves a
//! range's replica set to a different node by driving clustor's Raft
//! membership changes, advancing the `RelocatePhase` machine as clustor
//! reports each config change committed. `no_std`, no I/O: the
//! `range_supervisor` module wraps this with the actual channel sends and
//! committed-entry observation, exactly as the metrics anchor wraps
//! `tsquery_core`. Host-tested against the clustor membership API.
//!
//! ## The mapping
//!
//! Lattice's `RelocatePhase` (`range_lifecycle.rs`) is a per-replica move; it
//! maps one-to-one onto clustor's membership admin ops (`wire.rs` in
//! `deps/clustor`, values pinned below), submitted as `MSG_ADMIN_COMMAND` to
//! `operations.admin_requests`:
//!
//! ```text
//!   (start)            → ADD_LEARNER(target)   → [committed] → LearnerAdded
//!   LearnerAdded       → await catch-up (clustor installs the snapshot and
//!                        the promotion gate opens)              → SnapshotInstalled
//!   SnapshotInstalled  → ADD_VOTER(target)     → [joint committed] → JointConsensus
//!   JointConsensus     → REMOVE_VOTER(source)  → [C_new committed]  → Promoted
//!   Promoted           → advance placement epoch (lattice-local)   → EpochAdvanced
//!   EpochAdvanced      → publish new Binding/leaseholder           → DescriptorPublished
//!   DescriptorPublished→ await retention floors                    → OldStateRemovable (done)
//! ```
//!
//! Completion is *observed*, never pushed: clustor emits no migration event,
//! only a committed `CONFIG_CHANGE_MAGIC` entry (`MSG_CONFIG_COMMITTED`). And
//! `ADD_VOTER` is rejected until the learner's log is within `catchup_lag_max`
//! of the leader tip — so the driver re-issues it (the `AwaitCatchup` action)
//! rather than assuming success.

#![allow(
    dead_code,
    reason = "shared via #[path] into the range_supervisor module and host tests; each consumer uses a subset of the surface"
)]

#[path = "range_lifecycle.rs"]
pub mod range_lifecycle;

use range_lifecycle::RelocatePhase;

// ── clustor admin op codes (deps/clustor/modules/common/wire.rs) ──────
//
// Membership ops. Bodies carry a single `[replica_id:u8]`; the target Raft
// partition is named by the admin envelope's `target_partition`, which the
// wrapping module supplies.

/// `ADMIN_OP_ADD_VOTER` (clustor `wire.rs`). Rejected until the target is
/// a caught-up learner (the promotion gate).
pub const ADMIN_OP_ADD_VOTER: u8 = 0x06;
/// `ADMIN_OP_REMOVE_VOTER` (clustor `wire.rs`).
pub const ADMIN_OP_REMOVE_VOTER: u8 = 0x07;
/// `ADMIN_OP_ADD_LEARNER` (clustor `wire.rs`).
pub const ADMIN_OP_ADD_LEARNER: u8 = 0x0c;
/// `ADMIN_OP_REMOVE_LEARNER` (clustor `wire.rs`).
pub const ADMIN_OP_REMOVE_LEARNER: u8 = 0x0d;

/// Admin acceptance/commit statuses (clustor `wire.rs`). The gate returns
/// `REJECTED` while the learner is behind; `NOT_LEADER` when this node is not
/// the partition leader — both mean "retry", not "fail".
pub const ADMIN_STATUS_OK: u8 = 0x00;
pub const ADMIN_STATUS_DUPLICATE: u8 = 0x01;
pub const ADMIN_STATUS_UNSUPPORTED: u8 = 0x80;
pub const ADMIN_STATUS_REJECTED: u8 = 0x81;
pub const ADMIN_STATUS_NOT_LEADER: u8 = 0x82;

/// The parameters of one relocation: move the range served by `partition`'s
/// Raft group from replica `source` to replica `target`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Relocation {
    pub partition: u16,
    pub source: u8,
    pub target: u8,
}

/// What the driver should do to make progress from the current phase. The
/// wrapping module turns `IssueAdmin` into a `MSG_ADMIN_COMMAND` and the other
/// variants into lattice-local map/epoch work.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DriverAction {
    /// Submit a clustor membership op for `partition` naming `replica`, then
    /// wait for its committed `CONFIG_CHANGE`.
    IssueAdmin { op: u8, partition: u16, replica: u8 },
    /// The learner is added; wait for clustor to install the snapshot and the
    /// promotion gate to open. In practice the module re-issues `ADD_VOTER`
    /// and treats `REJECTED` as "still catching up".
    AwaitCatchup,
    /// Advance the placement epoch (lattice-local, `partition_map`).
    AdvanceEpoch,
    /// Publish the new descriptor carrying the moved `Binding`/`leaseholder` —
    /// the point at which the move becomes visible to routing.
    PublishDescriptor,
    /// Membership is settled and published; wait until retention floors permit
    /// deleting the old replica's state.
    AwaitRetention,
    /// Relocation complete; the operation record may be retired.
    Done,
}

/// The action that advances a relocation whose latest completed phase is
/// `phase` (or `None` before it has begun). This is the whole decision table;
/// the module calls it after each observed event to learn the next step.
pub fn action_for(phase: Option<RelocatePhase>, r: &Relocation) -> DriverAction {
    match phase {
        // Not started: add the destination as a learner.
        None => DriverAction::IssueAdmin {
            op: ADMIN_OP_ADD_LEARNER,
            partition: r.partition,
            replica: r.target,
        },
        // Learner added: wait for snapshot install + catch-up.
        Some(RelocatePhase::LearnerAdded) => DriverAction::AwaitCatchup,
        // Caught up: promote the learner to a voter (enters joint consensus).
        Some(RelocatePhase::SnapshotInstalled) => DriverAction::IssueAdmin {
            op: ADMIN_OP_ADD_VOTER,
            partition: r.partition,
            replica: r.target,
        },
        // Joint entered: remove the source voter (leader auto-queues C_new).
        Some(RelocatePhase::JointConsensus) => DriverAction::IssueAdmin {
            op: ADMIN_OP_REMOVE_VOTER,
            partition: r.partition,
            replica: r.source,
        },
        // Promoted / old voter removed: the placement epoch advances.
        Some(RelocatePhase::Promoted) => DriverAction::AdvanceEpoch,
        // Epoch advanced: publish the new descriptor (publication point).
        Some(RelocatePhase::EpochAdvanced) => DriverAction::PublishDescriptor,
        // Published: wait for retention floors before deleting old state.
        Some(RelocatePhase::DescriptorPublished) => DriverAction::AwaitRetention,
        // Terminal.
        Some(RelocatePhase::OldStateRemovable) => DriverAction::Done,
    }
}

/// The phase reached once the action for `phase` has been confirmed (its
/// clustor config change committed, or its lattice-local step done). Threads
/// the existing `RelocatePhase` ordering so the two stay in lockstep.
pub fn advance(phase: Option<RelocatePhase>) -> Option<RelocatePhase> {
    match phase {
        None => Some(RelocatePhase::LearnerAdded),
        Some(RelocatePhase::LearnerAdded) => Some(RelocatePhase::SnapshotInstalled),
        Some(RelocatePhase::SnapshotInstalled) => Some(RelocatePhase::JointConsensus),
        Some(RelocatePhase::JointConsensus) => Some(RelocatePhase::Promoted),
        Some(RelocatePhase::Promoted) => Some(RelocatePhase::EpochAdvanced),
        Some(RelocatePhase::EpochAdvanced) => Some(RelocatePhase::DescriptorPublished),
        Some(RelocatePhase::DescriptorPublished) => Some(RelocatePhase::OldStateRemovable),
        Some(RelocatePhase::OldStateRemovable) => Some(RelocatePhase::OldStateRemovable),
    }
}

// ── Wire contract with clustor (deps/clustor/modules/common/wire.rs) ──────

/// `CONFIG_CHANGE_MAGIC` (clustor `wire.rs`): a committed Raft entry whose
/// body begins with this magic IS a membership change; the driver advances
/// `RelocatePhase` when it sees the one it issued commit.
pub const CONFIG_CHANGE_MAGIC: [u8; 8] = [0xCC, 0x46, 0x47, 0x21, 0x9E, 0x1F, 0x5C, 0xA7];
/// `MSG_CONFIG_COMMITTED` (clustor `wire.rs`) — carries a config-change body
/// verbatim.
pub const MSG_CONFIG_COMMITTED: u8 = 0x1B;
/// `MSG_ADMIN_COMMAND` (clustor `wire.rs`) — the envelope type the wrapping
/// module stamps over `encode_admin_command`'s bytes onto clustor
/// `admin_requests`.
pub const MSG_ADMIN_COMMAND: u8 = 0x12;
/// `MSG_COMMITTED_ENTRY` (clustor `wire.rs`) — the per-entry stream on
/// `consensus.committed_entries` the module observes for confirmations.
pub const MSG_COMMITTED_ENTRY: u8 = 0x24;
/// `COMMITTED_ENTRY_HDR` (clustor `wire.rs`): the multi-slot engine writes
/// each committed-entry payload as
/// `[partition_id:u16][term:u64][index:u64][body…]` (the partitioned form
/// `consensus/apply.rs` emits); the config-change body, when present,
/// begins at this offset with `CONFIG_CHANGE_MAGIC`.
pub const COMMITTED_ENTRY_HDR: usize = 18;
pub const CONFIG_CHANGE_OP_JOINT: u8 = 0x01;
pub const CONFIG_CHANGE_OP_NEW: u8 = 0x02;
pub const CONFIG_CHANGE_OP_LEARNER: u8 = 0x03;
/// Max voters in a config-change id list (clustor caps a group's members).
pub const MAX_CONFIG_VOTERS: usize = 16;

/// Encode the admin-command payload for a membership op:
/// `[conn_id:u16 LE][op_code:u8][replica_id:u8]` (clustor `admin.rs::on_command`
/// envelope). The target Raft partition is the operations module's
/// `target_partition` field, not part of this payload.
pub fn encode_admin_command(out: &mut [u8], conn_id: u16, op: u8, replica: u8) -> Option<usize> {
    if out.len() < 4 {
        return None;
    }
    out[..2].copy_from_slice(&conn_id.to_le_bytes());
    out[2] = op;
    out[3] = replica;
    Some(4)
}

/// A decoded committed config change.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ConfigChange {
    pub op: u8,
    voters: [u8; MAX_CONFIG_VOTERS],
    n: u8,
}

impl ConfigChange {
    pub fn voters(&self) -> &[u8] {
        &self.voters[..self.n as usize]
    }
    pub fn contains(&self, id: u8) -> bool {
        // Manual scan, not `slice::contains`: the latter lowers to `memchr`,
        // which is not linked into a PIC `no_std` module.
        let mut i = 0;
        while i < self.n as usize {
            if self.voters[i] == id {
                return true;
            }
            i += 1;
        }
        false
    }
}

/// True if `buf` is a committed config-change entry body.
pub fn is_config_change(buf: &[u8]) -> bool {
    buf.len() >= 8 && buf[..8] == CONFIG_CHANGE_MAGIC
}

/// Decode a committed config-change body
/// `[MAGIC:8][op:u8][voter_count:u8][voter_ids:u8…]` into its op + id set.
pub fn parse_config_change(body: &[u8]) -> Option<ConfigChange> {
    if !is_config_change(body) || body.len() < 10 {
        return None;
    }
    let op = body[8];
    let count = body[9] as usize;
    if count > MAX_CONFIG_VOTERS || 10 + count > body.len() {
        return None;
    }
    let mut voters = [0u8; MAX_CONFIG_VOTERS];
    voters[..count].copy_from_slice(&body[10..10 + count]);
    Some(ConfigChange {
        op,
        voters,
        n: count as u8,
    })
}

/// Does a committed config change confirm the transition out of `phase` for
/// relocation `r`? This is how the module decides an `IssueAdmin` step landed:
///
/// - `SnapshotInstalled → JointConsensus`: a `JOINT` change that now includes
///   the target voter.
/// - `JointConsensus → Promoted`: a `NEW` change that no longer includes the
///   source voter (the joint exit that finalises the move).
///
/// The learner-add (`None → LearnerAdded`) is confirmed by a `LEARNER` change
/// including the target; other phases advance on lattice-local work, not a
/// config change.
pub fn config_confirms(phase: Option<RelocatePhase>, r: &Relocation, cc: &ConfigChange) -> bool {
    match phase {
        None => cc.op == CONFIG_CHANGE_OP_LEARNER && cc.contains(r.target),
        Some(RelocatePhase::SnapshotInstalled) => {
            cc.op == CONFIG_CHANGE_OP_JOINT && cc.contains(r.target)
        }
        Some(RelocatePhase::JointConsensus) => {
            cc.op == CONFIG_CHANGE_OP_NEW && !cc.contains(r.source)
        }
        _ => false,
    }
}

/// Extract `(partition_id, ConfigChange)` from a `MSG_COMMITTED_ENTRY` payload
/// `[partition_id:u16][term:u64][index:u64][body…]` when the committed body is a
/// membership config change; `None` for an ordinary application entry or a short
/// frame. This is the module's whole read of the committed stream — it keeps the
/// 18-byte header layout in the host-tested core rather than in the module.
pub fn committed_config_change(payload: &[u8]) -> Option<(u16, ConfigChange)> {
    if payload.len() < COMMITTED_ENTRY_HDR {
        return None;
    }
    let partition = u16::from_le_bytes([payload[0], payload[1]]);
    let cc = parse_config_change(&payload[COMMITTED_ENTRY_HDR..])?;
    Some((partition, cc))
}

/// Whether an admin acceptance status means "retry the same op" (the learner
/// is still catching up, or leadership moved) rather than a hard failure.
pub fn status_is_retryable(status: u8) -> bool {
    matches!(status, ADMIN_STATUS_REJECTED | ADMIN_STATUS_NOT_LEADER)
}

/// Whether the relocation is at or past its publication point — after this it
/// is forward-only (an abort would leave a published map inconsistent). Uses
/// the machine's own `is_published` boundary (`DescriptorPublished`).
pub fn is_committed_forward(phase: Option<RelocatePhase>) -> bool {
    phase.map(RelocatePhase::is_published).unwrap_or(false)
}
