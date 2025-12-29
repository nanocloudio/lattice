//! Revision model and fencing.
//!
//! Per §5.1, revision is a monotone per-KPG commit sequence number equal
//! to the Raft commit index for that KPG. Clustor's ReadIndex returns
//! a commit index that Lattice uses directly as a revision fence.

use serde::{Deserialize, Serialize};

/// A revision fence for read operations.
///
/// For linearizable reads, this must be obtained via ReadIndex.
/// For snapshot-only reads, this may be the current committed revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RevisionFence {
    /// The commit index / revision value.
    pub revision: u64,
}

impl RevisionFence {
    /// Create a new revision fence.
    pub const fn new(revision: u64) -> Self {
        Self { revision }
    }

    /// Create a zero revision fence (for bootstrap).
    pub const fn zero() -> Self {
        Self { revision: 0 }
    }

    /// Check if this fence is at or after the given revision.
    pub const fn is_at_or_after(&self, revision: u64) -> bool {
        self.revision >= revision
    }

    /// Get the revision value.
    pub const fn get(&self) -> u64 {
        self.revision
    }
}

impl std::fmt::Display for RevisionFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rev:{}", self.revision)
    }
}

impl From<u64> for RevisionFence {
    fn from(revision: u64) -> Self {
        Self::new(revision)
    }
}

/// Compaction floor tracking.
///
/// Per §9.5, Lattice MUST persist a kv_compaction_floor_revision and ensure
/// it never exceeds Clustor's effective compaction floor index mapping.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CompactionFloor {
    /// The KV compaction floor revision.
    pub kv_floor: u64,

    /// Clustor's compaction floor (must be >= kv_floor).
    pub clustor_floor: u64,
}

impl CompactionFloor {
    /// Create a new compaction floor.
    pub const fn new(kv_floor: u64, clustor_floor: u64) -> Self {
        Self {
            kv_floor,
            clustor_floor,
        }
    }

    /// Check if a revision has been compacted.
    pub const fn is_compacted(&self, revision: u64) -> bool {
        revision < self.kv_floor
    }

    /// Advance the KV floor, respecting Clustor's floor.
    pub fn advance_to(&mut self, new_floor: u64) -> bool {
        if new_floor <= self.clustor_floor && new_floor > self.kv_floor {
            self.kv_floor = new_floor;
            true
        } else {
            false
        }
    }

    /// Update Clustor's floor.
    pub fn update_clustor_floor(&mut self, new_clustor_floor: u64) {
        self.clustor_floor = new_clustor_floor;
    }
}

impl Default for CompactionFloor {
    fn default() -> Self {
        Self {
            kv_floor: 0,
            clustor_floor: u64::MAX,
        }
    }
}

/// Revision tracker for a KPG.
///
/// Tracks the committed revision and provides fencing utilities
/// for linearizable operations per §5.1.
#[derive(Debug, Clone)]
pub struct RevisionTracker {
    /// Current committed revision (highest applied log index).
    committed: u64,

    /// Last compacted revision.
    compaction_floor: CompactionFloor,

    /// Pending read fences waiting for commits.
    pending_fences: Vec<PendingFence>,
}

/// A pending fence waiting for a revision to be reached.
#[derive(Debug, Clone)]
pub struct PendingFence {
    /// Required revision for this fence.
    pub required_revision: u64,
    /// Client-provided request ID for correlation.
    pub request_id: u64,
}

impl RevisionTracker {
    /// Create a new revision tracker.
    pub fn new() -> Self {
        Self {
            committed: 0,
            compaction_floor: CompactionFloor::default(),
            pending_fences: Vec::new(),
        }
    }

    /// Get the current committed revision.
    pub fn committed(&self) -> u64 {
        self.committed
    }

    /// Get the current revision fence.
    pub fn fence(&self) -> RevisionFence {
        RevisionFence::new(self.committed)
    }

    /// Get the compaction floor.
    pub fn compaction_floor(&self) -> &CompactionFloor {
        &self.compaction_floor
    }

    /// Advance the committed revision.
    ///
    /// Per REV-MONOTONE invariant, revision must always increase.
    /// Returns the previous revision.
    pub fn advance(&mut self, new_revision: u64) -> u64 {
        let prev = self.committed;
        debug_assert!(
            new_revision >= self.committed,
            "REV-MONOTONE violation: {} < {}",
            new_revision,
            self.committed
        );
        self.committed = new_revision;
        prev
    }

    /// Check if a revision is available (committed and not compacted).
    pub fn is_available(&self, revision: u64) -> RevisionAvailability {
        if revision > self.committed {
            RevisionAvailability::NotYetCommitted
        } else if self.compaction_floor.is_compacted(revision) {
            RevisionAvailability::Compacted
        } else {
            RevisionAvailability::Available
        }
    }

    /// Add a pending fence that needs to wait for a revision.
    pub fn add_pending_fence(&mut self, required_revision: u64, request_id: u64) {
        self.pending_fences.push(PendingFence {
            required_revision,
            request_id,
        });
    }

    /// Take fences that are now satisfied by the current committed revision.
    pub fn take_satisfied_fences(&mut self) -> Vec<PendingFence> {
        let committed = self.committed;
        let (satisfied, remaining): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending_fences)
            .into_iter()
            .partition(|f| f.required_revision <= committed);
        self.pending_fences = remaining;
        satisfied
    }

    /// Check if any fences are pending.
    pub fn has_pending_fences(&self) -> bool {
        !self.pending_fences.is_empty()
    }

    /// Get count of pending fences.
    pub fn pending_fence_count(&self) -> usize {
        self.pending_fences.len()
    }

    /// Update the Clustor compaction floor.
    pub fn update_clustor_floor(&mut self, floor: u64) {
        self.compaction_floor.update_clustor_floor(floor);
    }

    /// Attempt to advance the KV compaction floor.
    pub fn advance_kv_floor(&mut self, new_floor: u64) -> bool {
        self.compaction_floor.advance_to(new_floor)
    }
}

impl Default for RevisionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Availability status for a revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionAvailability {
    /// Revision is available for reads.
    Available,
    /// Revision has not yet been committed.
    NotYetCommitted,
    /// Revision has been compacted.
    Compacted,
}

impl RevisionAvailability {
    /// Check if the revision is available.
    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available)
    }
}

/// ReadIndex fence for linearizable reads.
///
/// Per Clustor's ReadIndex pattern, this fence ensures the read
/// occurs after all writes prior to the ReadIndex call have committed.
#[derive(Debug, Clone)]
pub struct ReadIndexFence {
    /// The fence revision from ReadIndex.
    pub fence: RevisionFence,
    /// Whether the fence has been satisfied.
    pub satisfied: bool,
    /// Request correlation ID.
    pub request_id: u64,
}

impl ReadIndexFence {
    /// Create a new ReadIndex fence.
    pub fn new(fence: RevisionFence, request_id: u64) -> Self {
        Self {
            fence,
            satisfied: false,
            request_id,
        }
    }

    /// Check if this fence is satisfied at the given revision.
    pub fn check_satisfied(&mut self, committed_revision: u64) -> bool {
        if committed_revision >= self.fence.revision {
            self.satisfied = true;
        }
        self.satisfied
    }

    /// Mark the fence as satisfied.
    pub fn mark_satisfied(&mut self) {
        self.satisfied = true;
    }
}

/// Revision range for range queries.
#[derive(Debug, Clone, Copy)]
pub struct RevisionRange {
    /// Start revision (inclusive).
    pub start: u64,
    /// End revision (exclusive), None means current.
    pub end: Option<u64>,
}

impl RevisionRange {
    /// Create a range from start to current.
    pub fn from(start: u64) -> Self {
        Self { start, end: None }
    }

    /// Create a range from start to end.
    pub fn between(start: u64, end: u64) -> Self {
        Self {
            start,
            end: Some(end),
        }
    }

    /// Check if a revision is in this range.
    pub fn contains(&self, revision: u64) -> bool {
        revision >= self.start && self.end.is_none_or(|e| revision < e)
    }
}

/// Compaction request for history cleanup.
#[derive(Debug, Clone)]
pub struct CompactionRequest {
    /// Target revision to compact up to.
    pub revision: u64,
    /// Whether this is a physical (immediate) compaction.
    pub physical: bool,
}

impl CompactionRequest {
    /// Create a logical compaction request.
    pub fn logical(revision: u64) -> Self {
        Self {
            revision,
            physical: false,
        }
    }

    /// Create a physical compaction request.
    pub fn physical(revision: u64) -> Self {
        Self {
            revision,
            physical: true,
        }
    }
}

/// Compaction result.
#[derive(Debug, Clone)]
pub struct CompactionResult {
    /// Revision that was compacted to.
    pub compacted_revision: u64,
    /// Number of history entries removed.
    pub entries_removed: usize,
}
