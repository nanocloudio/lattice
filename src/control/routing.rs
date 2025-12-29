//! Routing metadata and epoch validation.
//!
//! Keys are routed to KPGs using a versioned hash per §8.1:
//! `kpg = hash64(tenant_id, key_bytes, hash_seed_version) % tenant_kpg_count`
//!
//! Any request with an older kv_epoch MUST be rejected as dirty_epoch.
//!
//! # Epoch Migration
//!
//! When CP-Raft signals a routing epoch change:
//! 1. Requests with the old epoch are rejected with DirtyEpoch
//! 2. Clients must refresh routing metadata and retry
//! 3. The error message includes the expected epoch for client guidance

use crate::core::error::{LatticeError, LatticeResult};
use serde::{Deserialize, Serialize};
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use twox_hash::XxHash64;

/// Routing metadata for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTable {
    /// Tenant identifier.
    pub tenant_id: String,
    /// Current routing epoch (kv_epoch).
    pub kv_epoch: u64,
    /// Hash seed version for routing stability.
    pub hash_seed_version: u64,
    /// Number of KPGs for this tenant.
    pub kpg_count: u32,
}

impl RoutingTable {
    /// Create a new routing table.
    pub fn new(tenant_id: String, kv_epoch: u64, hash_seed_version: u64, kpg_count: u32) -> Self {
        Self {
            tenant_id,
            kv_epoch,
            hash_seed_version,
            kpg_count,
        }
    }

    /// Route a key to its KPG index.
    ///
    /// Uses xxHash64 for consistent hashing:
    /// `kpg = hash64(tenant_id, key_bytes, hash_seed_version) % tenant_kpg_count`
    pub fn route_key(&self, key: &[u8]) -> u32 {
        let mut hasher = XxHash64::with_seed(self.hash_seed_version);
        hasher.write(self.tenant_id.as_bytes());
        hasher.write(key);
        let hash = hasher.finish();
        (hash % u64::from(self.kpg_count)) as u32
    }

    /// Check if a range of keys spans multiple KPGs.
    ///
    /// Returns the set of KPG indices that the range touches.
    pub fn route_range(&self, start_key: &[u8], end_key: Option<&[u8]>) -> Vec<u32> {
        // For prefix ranges or single keys, we can sometimes determine
        // that only one KPG is involved. For arbitrary ranges, we must
        // assume all KPGs could be involved.

        match end_key {
            None => {
                // Single key
                vec![self.route_key(start_key)]
            }
            Some([]) => {
                // Empty end_key means "all keys with this prefix"
                // This could span all KPGs
                (0..self.kpg_count).collect()
            }
            Some(_end) => {
                // Arbitrary range - conservatively assume all KPGs
                // A more sophisticated implementation could analyze the range
                (0..self.kpg_count).collect()
            }
        }
    }

    /// Validate that a request's epoch matches the current epoch.
    ///
    /// Returns `Err(DirtyEpoch)` if the epochs don't match.
    pub fn validate_epoch(&self, request_epoch: u64) -> LatticeResult<()> {
        if request_epoch != self.kv_epoch {
            return Err(LatticeError::dirty_epoch(self.kv_epoch, request_epoch));
        }
        Ok(())
    }

    /// Get the KPG ID for a key.
    pub fn get_kpg_id(&self, key: &[u8]) -> KpgId {
        KpgId::new(&self.tenant_id, self.route_key(key))
    }

    /// Check if a transaction spans multiple KPGs.
    ///
    /// Returns TxnCrossShardUnsupported if keys route to different KPGs.
    pub fn validate_single_kpg(&self, keys: &[&[u8]]) -> LatticeResult<u32> {
        if keys.is_empty() {
            return Ok(0);
        }

        let first_kpg = self.route_key(keys[0]);
        for key in keys.iter().skip(1) {
            if self.route_key(key) != first_kpg {
                return Err(LatticeError::TxnCrossShardUnsupported);
            }
        }
        Ok(first_kpg)
    }
}

/// A KPG identifier combining tenant and partition index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KpgId {
    pub tenant_id: String,
    pub partition: u32,
}

impl KpgId {
    pub fn new(tenant_id: impl Into<String>, partition: u32) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            partition,
        }
    }
}

impl std::fmt::Display for KpgId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.tenant_id, self.partition)
    }
}

/// Epoch migration state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EpochMigrationState {
    /// No migration in progress.
    Stable,
    /// Migration announced, accepting both epochs temporarily.
    Transitioning,
    /// Old epoch fully rejected.
    Completed,
}

/// Routing epoch tracker for handling migrations.
///
/// This tracks the current epoch and handles the transition period
/// when CP-Raft announces a new epoch.
pub struct RoutingEpochTracker {
    /// Current active epoch.
    current_epoch: AtomicU64,

    /// Previous epoch (during transition).
    previous_epoch: AtomicU64,

    /// Migration state.
    state: Arc<RwLock<EpochMigrationState>>,

    /// Tenant ID.
    tenant_id: String,
}

impl RoutingEpochTracker {
    /// Create a new epoch tracker.
    pub fn new(tenant_id: impl Into<String>, initial_epoch: u64) -> Self {
        Self {
            current_epoch: AtomicU64::new(initial_epoch),
            previous_epoch: AtomicU64::new(0),
            state: Arc::new(RwLock::new(EpochMigrationState::Stable)),
            tenant_id: tenant_id.into(),
        }
    }

    /// Get the current epoch.
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Acquire)
    }

    /// Get the migration state.
    pub fn state(&self) -> EpochMigrationState {
        *self.state.read().unwrap()
    }

    /// Start epoch migration.
    ///
    /// This is called when CP-Raft announces a new routing epoch.
    pub fn start_migration(&self, new_epoch: u64) {
        let current = self.current_epoch.load(Ordering::Acquire);
        if new_epoch > current {
            self.previous_epoch.store(current, Ordering::Release);
            self.current_epoch.store(new_epoch, Ordering::Release);
            *self.state.write().unwrap() = EpochMigrationState::Transitioning;

            tracing::info!(
                tenant_id = %self.tenant_id,
                old_epoch = current,
                new_epoch = new_epoch,
                "routing epoch migration started"
            );
        }
    }

    /// Complete epoch migration.
    ///
    /// This is called after the transition period to stop accepting
    /// the old epoch.
    pub fn complete_migration(&self) {
        *self.state.write().unwrap() = EpochMigrationState::Completed;

        tracing::info!(
            tenant_id = %self.tenant_id,
            epoch = self.current_epoch(),
            "routing epoch migration completed"
        );

        // After some delay, return to Stable state
        *self.state.write().unwrap() = EpochMigrationState::Stable;
    }

    /// Validate a request epoch.
    ///
    /// During transition, both current and previous epochs are accepted.
    /// In stable/completed state, only the current epoch is accepted.
    pub fn validate(&self, request_epoch: u64) -> LatticeResult<EpochValidation> {
        let current = self.current_epoch.load(Ordering::Acquire);
        let state = self.state();

        if request_epoch == current {
            return Ok(EpochValidation::Current);
        }

        if state == EpochMigrationState::Transitioning {
            let previous = self.previous_epoch.load(Ordering::Acquire);
            if request_epoch == previous {
                return Ok(EpochValidation::PreviousDuringTransition);
            }
        }

        // Reject stale epoch
        Err(LatticeError::dirty_epoch(current, request_epoch))
    }

    /// Force upgrade to current epoch.
    ///
    /// Returns the current epoch for client retry guidance.
    pub fn required_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Acquire)
    }
}

/// Result of epoch validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochValidation {
    /// Request uses the current epoch.
    Current,
    /// Request uses previous epoch, accepted during transition.
    PreviousDuringTransition,
}

/// Client retry guidance for epoch errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochRetryGuidance {
    /// The expected epoch to use.
    pub expected_epoch: u64,

    /// Whether the client should refresh routing metadata.
    pub refresh_routing: bool,

    /// Suggested retry delay in milliseconds.
    pub retry_delay_ms: u64,

    /// Human-readable message.
    pub message: String,
}

impl EpochRetryGuidance {
    /// Create guidance for a dirty epoch error.
    pub fn for_dirty_epoch(expected: u64, observed: u64) -> Self {
        Self {
            expected_epoch: expected,
            refresh_routing: true,
            retry_delay_ms: 100,
            message: format!(
                "Routing epoch mismatch: expected {}, got {}. Refresh routing metadata and retry.",
                expected, observed
            ),
        }
    }
}

/// Compute the hash for routing a key.
///
/// This is the core routing function exposed for testing and debugging.
pub fn compute_routing_hash(tenant_id: &str, key: &[u8], hash_seed_version: u64) -> u64 {
    let mut hasher = XxHash64::with_seed(hash_seed_version);
    hasher.write(tenant_id.as_bytes());
    hasher.write(key);
    hasher.finish()
}

/// Compute the KPG index for a key.
pub fn compute_kpg_index(
    tenant_id: &str,
    key: &[u8],
    hash_seed_version: u64,
    kpg_count: u32,
) -> u32 {
    let hash = compute_routing_hash(tenant_id, key, hash_seed_version);
    (hash % u64::from(kpg_count)) as u32
}
