//! Log compaction and cleanup.
//!
//! Per §9.5, KV compaction is layered on Clustor's WAL compaction floor.
//! Lattice MUST persist a kv_compaction_floor_revision and ensure it
//! never exceeds Clustor's effective compaction floor index mapping.
//!
//! # Watch Coordination
//!
//! Compaction must preserve revisions needed by active watch streams.
//! The compaction coordinator tracks the minimum revision required by
//! all active watches and ensures compaction does not advance past it.
//!
//! # Compaction Policy
//!
//! Compaction policies define:
//! - Minimum revisions to retain (for historical queries)
//! - Minimum time to retain (for watch reconnection)
//! - Maximum compaction interval (for space reclamation)

use crate::kpg::revision::CompactionFloor;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Compaction policy for a KPG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionPolicy {
    /// Current compaction floor.
    pub floor: CompactionFloor,

    /// Minimum revisions to retain.
    pub min_retain_revisions: u64,

    /// Minimum time to retain in milliseconds.
    pub min_retain_ms: u64,

    /// Maximum time between compactions in milliseconds.
    pub max_compaction_interval_ms: u64,

    /// Whether compaction is enabled.
    pub enabled: bool,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            floor: CompactionFloor::default(),
            min_retain_revisions: 1000,
            min_retain_ms: 300_000,                // 5 minutes
            max_compaction_interval_ms: 3_600_000, // 1 hour
            enabled: true,
        }
    }
}

impl CompactionPolicy {
    /// Create a new compaction policy with custom retention.
    pub fn with_retention(min_revisions: u64, min_ms: u64) -> Self {
        Self {
            min_retain_revisions: min_revisions,
            min_retain_ms: min_ms,
            ..Default::default()
        }
    }

    /// Check if a revision can be compacted.
    pub fn can_compact(&self, revision: u64) -> bool {
        if !self.enabled {
            return false;
        }
        revision < self.floor.kv_floor
    }

    /// Propose a new compaction floor, respecting constraints.
    pub fn propose_floor(&self, target: u64, current_revision: u64) -> Option<u64> {
        if !self.enabled {
            return None;
        }

        // Don't compact too close to the current revision
        let min_floor = current_revision.saturating_sub(self.min_retain_revisions);
        let proposed = target.min(min_floor);

        // Don't exceed Clustor's floor
        let clamped = proposed.min(self.floor.clustor_floor);

        // Only propose if it advances the floor
        if clamped > self.floor.kv_floor {
            Some(clamped)
        } else {
            None
        }
    }

    /// Update the KV compaction floor.
    pub fn advance_floor(&mut self, new_floor: u64) -> bool {
        self.floor.advance_to(new_floor)
    }

    /// Update Clustor's compaction floor.
    pub fn update_clustor_floor(&mut self, clustor_floor: u64) {
        self.floor.update_clustor_floor(clustor_floor);
    }

    /// Disable compaction.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Enable compaction.
    pub fn enable(&mut self) {
        self.enabled = true;
    }
}

/// Watch reference tracker for compaction coordination.
///
/// Tracks the minimum revision required by each active watch
/// to ensure compaction does not advance past watched revisions.
#[derive(Debug, Default)]
pub struct WatchRevisionTracker {
    /// Map of watch_id -> minimum required revision.
    watches: RwLock<HashMap<i64, u64>>,

    /// Minimum revision across all watches (cached).
    min_watch_revision: AtomicU64,
}

impl WatchRevisionTracker {
    /// Create a new watch revision tracker.
    pub fn new() -> Self {
        Self {
            watches: RwLock::new(HashMap::new()),
            min_watch_revision: AtomicU64::new(u64::MAX),
        }
    }

    /// Register a watch with its start revision.
    pub fn register_watch(&self, watch_id: i64, start_revision: u64) {
        let mut watches = self.watches.write().unwrap();
        watches.insert(watch_id, start_revision);
        self.update_min_revision(&watches);
    }

    /// Update a watch's required revision (e.g., after delivering events).
    pub fn update_watch(&self, watch_id: i64, new_revision: u64) {
        let mut watches = self.watches.write().unwrap();
        if let Some(rev) = watches.get_mut(&watch_id) {
            if new_revision > *rev {
                *rev = new_revision;
                self.update_min_revision(&watches);
            }
        }
    }

    /// Unregister a watch.
    pub fn unregister_watch(&self, watch_id: i64) {
        let mut watches = self.watches.write().unwrap();
        watches.remove(&watch_id);
        self.update_min_revision(&watches);
    }

    /// Get the minimum revision required by any watch.
    pub fn min_required_revision(&self) -> Option<u64> {
        let min = self.min_watch_revision.load(Ordering::Acquire);
        if min == u64::MAX {
            None
        } else {
            Some(min)
        }
    }

    /// Get the number of active watches.
    pub fn watch_count(&self) -> usize {
        self.watches.read().unwrap().len()
    }

    /// Update the cached minimum revision.
    fn update_min_revision(&self, watches: &HashMap<i64, u64>) {
        let min = watches.values().copied().min().unwrap_or(u64::MAX);
        self.min_watch_revision.store(min, Ordering::Release);
    }
}

/// Compaction coordinator that manages compaction with watch awareness.
///
/// This coordinator ensures that:
/// 1. Compaction respects the policy constraints
/// 2. Watched revisions are preserved
/// 3. Clustor's floor is never exceeded
pub struct CompactionCoordinator {
    /// Compaction policy.
    policy: Arc<RwLock<CompactionPolicy>>,

    /// Watch revision tracker.
    watch_tracker: Arc<WatchRevisionTracker>,

    /// Last compaction timestamp.
    last_compaction_ms: AtomicU64,

    /// Current revision (for policy evaluation).
    current_revision: AtomicU64,
}

impl CompactionCoordinator {
    /// Create a new compaction coordinator.
    pub fn new(policy: CompactionPolicy) -> Self {
        Self {
            policy: Arc::new(RwLock::new(policy)),
            watch_tracker: Arc::new(WatchRevisionTracker::new()),
            last_compaction_ms: AtomicU64::new(0),
            current_revision: AtomicU64::new(0),
        }
    }

    /// Get the watch tracker for registering/updating watches.
    pub fn watch_tracker(&self) -> Arc<WatchRevisionTracker> {
        self.watch_tracker.clone()
    }

    /// Update the current revision.
    pub fn set_current_revision(&self, revision: u64) {
        self.current_revision.store(revision, Ordering::Release);
    }

    /// Get the current KV compaction floor.
    pub fn kv_floor(&self) -> u64 {
        self.policy.read().unwrap().floor.kv_floor
    }

    /// Update Clustor's compaction floor.
    pub fn update_clustor_floor(&self, floor: u64) {
        self.policy.write().unwrap().update_clustor_floor(floor);
    }

    /// Check if a revision has been compacted.
    pub fn is_compacted(&self, revision: u64) -> bool {
        self.policy.read().unwrap().can_compact(revision)
    }

    /// Try to compact up to the target revision.
    ///
    /// Returns the new floor if compaction succeeded, None otherwise.
    pub fn try_compact(&self, target_revision: u64, current_tick_ms: u64) -> Option<u64> {
        let current_revision = self.current_revision.load(Ordering::Acquire);

        // Calculate effective target considering watch constraints
        let watch_min = self.watch_tracker.min_required_revision();
        let effective_target = match watch_min {
            Some(min_rev) => target_revision.min(min_rev.saturating_sub(1)),
            None => target_revision,
        };

        let mut policy = self.policy.write().unwrap();

        // Propose a new floor
        let proposed = policy.propose_floor(effective_target, current_revision)?;

        // Advance the floor
        if policy.advance_floor(proposed) {
            self.last_compaction_ms
                .store(current_tick_ms, Ordering::Release);
            Some(proposed)
        } else {
            None
        }
    }

    /// Check if compaction is overdue based on the max interval.
    pub fn is_compaction_overdue(&self, current_tick_ms: u64) -> bool {
        let policy = self.policy.read().unwrap();
        if !policy.enabled {
            return false;
        }

        let last = self.last_compaction_ms.load(Ordering::Acquire);
        if last == 0 {
            return true;
        }

        current_tick_ms.saturating_sub(last) > policy.max_compaction_interval_ms
    }

    /// Get compaction statistics.
    pub fn stats(&self) -> CompactionStats {
        let policy = self.policy.read().unwrap();
        CompactionStats {
            kv_floor: policy.floor.kv_floor,
            clustor_floor: policy.floor.clustor_floor,
            current_revision: self.current_revision.load(Ordering::Acquire),
            active_watches: self.watch_tracker.watch_count(),
            min_watch_revision: self.watch_tracker.min_required_revision(),
            last_compaction_ms: self.last_compaction_ms.load(Ordering::Acquire),
            enabled: policy.enabled,
        }
    }

    /// Update the compaction policy.
    pub fn update_policy(&self, new_policy: CompactionPolicy) {
        let mut policy = self.policy.write().unwrap();
        // Preserve current floor state
        let current_floor = policy.floor;
        *policy = new_policy;
        policy.floor = current_floor;
    }
}

impl Default for CompactionCoordinator {
    fn default() -> Self {
        Self::new(CompactionPolicy::default())
    }
}

/// Compaction statistics.
#[derive(Debug, Clone)]
pub struct CompactionStats {
    /// Current KV compaction floor.
    pub kv_floor: u64,
    /// Clustor's compaction floor.
    pub clustor_floor: u64,
    /// Current revision.
    pub current_revision: u64,
    /// Number of active watches.
    pub active_watches: usize,
    /// Minimum revision required by watches.
    pub min_watch_revision: Option<u64>,
    /// Last compaction timestamp.
    pub last_compaction_ms: u64,
    /// Whether compaction is enabled.
    pub enabled: bool,
}
