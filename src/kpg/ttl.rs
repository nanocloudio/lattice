//! Deterministic TTL and expiration handling.
//!
//! Per §9.6, all TTL evaluation occurs deterministically via committed ticks:
//! - `Tick(ms)` entries committed periodically to the WAL
//! - No wall-clock sampling during apply
//! - TTL-DETERMINISM invariant: expiration via apply-loop ticks only
//!
//! # Architecture
//!
//! - [`TickEmitter`] - Leader-only tick emission (Task 74)
//! - [`ExpirationQueue`] - Priority queue for efficient expiry scans (Task 78)
//! - [`TtlEnforcer`] - TTL bounds enforcement (Task 80)
//! - [`ExpirationProcessor`] - Tick processing and expiration evaluation (Tasks 75, 76, 77, 79)

use crate::control::capabilities::TtlBounds;
use crate::core::time::{Tick, TickSource, TickSourceConfig, WalTickSource};
use serde::{Deserialize, Serialize};
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};

// ============================================================================
// Tick Emission (Task 74)
// ============================================================================

/// Configuration for tick emission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickEmitterConfig {
    /// Emission period in milliseconds (default 1000ms = 1s).
    pub period_ms: u64,
    /// Whether tick emission is enabled.
    pub enabled: bool,
}

impl Default for TickEmitterConfig {
    fn default() -> Self {
        Self {
            period_ms: 1000, // 1 second per specification
            enabled: true,
        }
    }
}

/// Tick emitter for leader-only periodic tick WAL entries.
///
/// Per Task 74:
/// - Leader-only tick emission
/// - Periodic WAL commit
/// - Operator-configurable period (expiry_scan_period_ms)
pub struct TickEmitter {
    /// Configuration.
    config: TickEmitterConfig,
    /// Tick source for generating tick values.
    tick_source: WalTickSource,
    /// Last emitted tick.
    last_emitted: Option<Tick>,
    /// Whether this node is the leader.
    is_leader: bool,
    /// Total ticks emitted.
    ticks_emitted: AtomicU64,
}

impl TickEmitter {
    /// Create a new tick emitter with the given configuration.
    pub fn new(config: TickEmitterConfig) -> Self {
        let tick_source = WalTickSource::new(config.period_ms);
        Self {
            config,
            tick_source,
            last_emitted: None,
            is_leader: false,
            ticks_emitted: AtomicU64::new(0),
        }
    }

    /// Create from tick source configuration.
    pub fn from_tick_source_config(config: &TickSourceConfig) -> Self {
        Self::new(TickEmitterConfig {
            period_ms: config.period_ms,
            enabled: true,
        })
    }

    /// Set whether this node is the leader.
    pub fn set_leader(&mut self, is_leader: bool) {
        self.is_leader = is_leader;
    }

    /// Check if this node is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// Check if a tick should be emitted.
    ///
    /// Returns Some(tick_ms) if a tick should be committed to WAL.
    /// Only returns Some when this node is the leader.
    pub fn should_emit(&self) -> Option<u64> {
        if !self.is_leader || !self.config.enabled {
            return None;
        }

        self.tick_source
            .should_emit(self.last_emitted)
            .map(|t| t.ms)
    }

    /// Record that a tick was emitted.
    pub fn record_emitted(&mut self, tick_ms: u64) {
        self.last_emitted = Some(Tick::new(tick_ms));
        self.ticks_emitted.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the last emitted tick.
    pub fn last_emitted(&self) -> Option<Tick> {
        self.last_emitted
    }

    /// Get the configured emission period.
    pub fn period_ms(&self) -> u64 {
        self.config.period_ms
    }

    /// Get the total number of ticks emitted.
    pub fn ticks_emitted(&self) -> u64 {
        self.ticks_emitted.load(Ordering::Relaxed)
    }

    /// Get statistics.
    pub fn stats(&self) -> TickEmitterStats {
        TickEmitterStats {
            is_leader: self.is_leader,
            enabled: self.config.enabled,
            period_ms: self.config.period_ms,
            last_emitted_ms: self.last_emitted.map(|t| t.ms),
            ticks_emitted: self.ticks_emitted.load(Ordering::Relaxed),
        }
    }
}

impl Default for TickEmitter {
    fn default() -> Self {
        Self::new(TickEmitterConfig::default())
    }
}

/// Tick emitter statistics.
#[derive(Debug, Clone)]
pub struct TickEmitterStats {
    /// Whether this node is the leader.
    pub is_leader: bool,
    /// Whether emission is enabled.
    pub enabled: bool,
    /// Emission period in milliseconds.
    pub period_ms: u64,
    /// Last emitted tick value.
    pub last_emitted_ms: Option<u64>,
    /// Total ticks emitted.
    pub ticks_emitted: u64,
}

// ============================================================================
// Expiration Queue (Task 78)
// ============================================================================

/// An entry in the expiration queue.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExpiryEntry {
    /// Expiry time in milliseconds.
    pub expiry_ms: u64,
    /// Key that expires at this time.
    pub key: Vec<u8>,
    /// Revision when this expiry was set.
    pub set_revision: u64,
}

impl Ord for ExpiryEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering for min-heap (earliest expiry first)
        other
            .expiry_ms
            .cmp(&self.expiry_ms)
            .then_with(|| other.key.cmp(&self.key))
    }
}

impl PartialOrd for ExpiryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Priority queue for efficient expiry scans.
///
/// Per Task 78:
/// - Priority queue ordered by expiry_at_ms
/// - Efficient scan during tick processing
/// - ttl.expiry_queue_depth metric
pub struct ExpirationQueue {
    /// Min-heap of expiry entries (earliest first).
    heap: BinaryHeap<ExpiryEntry>,
    /// Map from key to current expiry (for lazy removal validation).
    key_expiry: std::collections::HashMap<Vec<u8>, u64>,
}

impl ExpirationQueue {
    /// Create a new expiration queue.
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            key_expiry: std::collections::HashMap::new(),
        }
    }

    /// Add a key with an expiry time.
    pub fn add(&mut self, key: Vec<u8>, expiry_ms: u64, set_revision: u64) {
        // Update the current expiry for this key (old heap entries become stale)
        self.key_expiry.insert(key.clone(), expiry_ms);

        let entry = ExpiryEntry {
            expiry_ms,
            key,
            set_revision,
        };

        self.heap.push(entry);
    }

    /// Remove a key from the queue.
    ///
    /// Note: This marks the key as removed but doesn't actually remove from heap
    /// (lazy removal for efficiency). The entry will be skipped during collection.
    pub fn remove(&mut self, key: &[u8]) -> bool {
        self.key_expiry.remove(key).is_some()
    }

    /// Collect all keys that have expired at or before the given tick.
    ///
    /// This is efficient because we stop as soon as we see a non-expired entry.
    pub fn collect_expired(&mut self, current_tick_ms: u64) -> Vec<Vec<u8>> {
        let mut expired = Vec::new();

        while let Some(entry) = self.heap.peek() {
            // Skip stale entries (key was updated with different expiry or removed)
            let current_expiry = self.key_expiry.get(&entry.key).copied();
            if current_expiry != Some(entry.expiry_ms) {
                // Stale entry - remove from heap and continue
                self.heap.pop();
                continue;
            }

            if entry.expiry_ms <= current_tick_ms {
                let entry = self.heap.pop().unwrap();
                self.key_expiry.remove(&entry.key);
                expired.push(entry.key);
            } else {
                // All remaining entries are in the future
                break;
            }
        }

        expired
    }

    /// Peek at the next expiry time without removing.
    pub fn peek_next_expiry(&self) -> Option<u64> {
        // Skip stale entries
        for entry in self.heap.iter() {
            if self.key_expiry.get(&entry.key) == Some(&entry.expiry_ms) {
                return Some(entry.expiry_ms);
            }
        }
        None
    }

    /// Get the queue depth (number of entries with pending expiry).
    pub fn depth(&self) -> usize {
        self.key_expiry.len()
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.key_expiry.is_empty()
    }

    /// Clear the queue.
    pub fn clear(&mut self) {
        self.heap.clear();
        self.key_expiry.clear();
    }
}

impl Default for ExpirationQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// TTL Bounds Enforcement (Task 80)
// ============================================================================

/// TTL enforcement behavior when a TTL exceeds bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TtlExceedsBehavior {
    /// Clamp the TTL to the maximum allowed.
    Clamp,
    /// Reject the request with an error.
    Reject,
}

impl Default for TtlExceedsBehavior {
    fn default() -> Self {
        Self::Clamp
    }
}

/// TTL enforcer for tenant-specific bounds.
///
/// Per Task 80:
/// - Maximum TTL from tenant manifest
/// - Clamp or reject behavior (implementation-defined, consistent)
/// - Configuration option
pub struct TtlEnforcer {
    /// TTL bounds.
    bounds: TtlBounds,
    /// Behavior when TTL exceeds maximum.
    exceeds_behavior: TtlExceedsBehavior,
}

impl TtlEnforcer {
    /// Create a new TTL enforcer with default bounds.
    pub fn new() -> Self {
        Self {
            bounds: TtlBounds::default(),
            exceeds_behavior: TtlExceedsBehavior::default(),
        }
    }

    /// Create an enforcer with custom bounds.
    pub fn with_bounds(bounds: TtlBounds) -> Self {
        Self {
            bounds,
            exceeds_behavior: TtlExceedsBehavior::default(),
        }
    }

    /// Create an enforcer with custom bounds and behavior.
    pub fn with_behavior(bounds: TtlBounds, behavior: TtlExceedsBehavior) -> Self {
        Self {
            bounds,
            exceeds_behavior: behavior,
        }
    }

    /// Update the TTL bounds.
    pub fn set_bounds(&mut self, bounds: TtlBounds) {
        self.bounds = bounds;
    }

    /// Update the exceeds behavior.
    pub fn set_behavior(&mut self, behavior: TtlExceedsBehavior) {
        self.exceeds_behavior = behavior;
    }

    /// Get the current bounds.
    pub fn bounds(&self) -> &TtlBounds {
        &self.bounds
    }

    /// Get the exceeds behavior.
    pub fn behavior(&self) -> TtlExceedsBehavior {
        self.exceeds_behavior
    }

    /// Enforce TTL bounds on a requested TTL.
    ///
    /// Returns Ok(adjusted_ttl_ms) if the TTL is valid or was clamped.
    /// Returns Err(TtlBoundsViolation) if the TTL is rejected.
    pub fn enforce(&self, requested_ttl_ms: u64) -> Result<u64, TtlBoundsViolation> {
        // Check minimum
        if requested_ttl_ms < self.bounds.min_ms {
            match self.exceeds_behavior {
                TtlExceedsBehavior::Clamp => return Ok(self.bounds.min_ms),
                TtlExceedsBehavior::Reject => {
                    return Err(TtlBoundsViolation::BelowMinimum {
                        requested: requested_ttl_ms,
                        minimum: self.bounds.min_ms,
                    });
                }
            }
        }

        // Check maximum
        if requested_ttl_ms > self.bounds.max_ms {
            match self.exceeds_behavior {
                TtlExceedsBehavior::Clamp => return Ok(self.bounds.max_ms),
                TtlExceedsBehavior::Reject => {
                    return Err(TtlBoundsViolation::AboveMaximum {
                        requested: requested_ttl_ms,
                        maximum: self.bounds.max_ms,
                    });
                }
            }
        }

        Ok(requested_ttl_ms)
    }

    /// Get the default TTL from bounds.
    pub fn default_ttl(&self) -> u64 {
        self.bounds.default_ms
    }

    /// Apply default TTL if none specified.
    pub fn with_default(&self, ttl_ms: Option<u64>) -> u64 {
        ttl_ms.unwrap_or(self.bounds.default_ms)
    }
}

impl Default for TtlEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

/// TTL bounds violation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TtlBoundsViolation {
    /// Requested TTL is below the minimum.
    BelowMinimum { requested: u64, minimum: u64 },
    /// Requested TTL is above the maximum.
    AboveMaximum { requested: u64, maximum: u64 },
}

impl std::fmt::Display for TtlBoundsViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BelowMinimum { requested, minimum } => {
                write!(f, "TTL {}ms is below minimum {}ms", requested, minimum)
            }
            Self::AboveMaximum { requested, maximum } => {
                write!(f, "TTL {}ms is above maximum {}ms", requested, maximum)
            }
        }
    }
}

impl std::error::Error for TtlBoundsViolation {}

// ============================================================================
// Expiration Processing (Tasks 75, 76, 77, 79)
// ============================================================================

/// Result of processing a tick for expirations.
#[derive(Debug, Clone, Default)]
pub struct TickProcessingResult {
    /// Keys expired due to key TTL.
    pub expired_keys: Vec<Vec<u8>>,
    /// Leases expired due to lease deadline.
    pub expired_leases: Vec<i64>,
    /// Keys deleted due to lease expiration.
    pub lease_cascade_keys: Vec<Vec<u8>>,
    /// Total keys deleted (includes cascade).
    pub total_deletions: usize,
}

impl TickProcessingResult {
    /// Create an empty result.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Merge another result into this one.
    pub fn merge(&mut self, other: TickProcessingResult) {
        self.expired_keys.extend(other.expired_keys);
        self.expired_leases.extend(other.expired_leases);
        self.lease_cascade_keys.extend(other.lease_cascade_keys);
        self.total_deletions += other.total_deletions;
    }
}

/// Expiration processor for tick-based expiration evaluation.
///
/// Per Tasks 75, 76, 77, 79:
/// - Process Tick entries during apply
/// - Trigger expiration scans
/// - Update current logical time
/// - Evaluate expiry_at_ms against committed tick
/// - Delete expired keys during tick processing
/// - Evaluate keepalive_deadline_ms against committed tick
/// - Expire leases and cascade delete attached keys
pub struct ExpirationProcessor {
    /// Current committed tick.
    current_tick: Tick,
    /// Expiration queue for key TTLs.
    key_expiry_queue: ExpirationQueue,
    /// Whether mutation-triggered expiration is enabled.
    mutation_expiry_enabled: bool,
    /// Statistics.
    stats: ExpirationStats,
}

impl ExpirationProcessor {
    /// Create a new expiration processor.
    pub fn new() -> Self {
        Self {
            current_tick: Tick::zero(),
            key_expiry_queue: ExpirationQueue::new(),
            mutation_expiry_enabled: true,
            stats: ExpirationStats::new(),
        }
    }

    /// Get the current tick.
    pub fn current_tick(&self) -> Tick {
        self.current_tick
    }

    /// Advance the current tick.
    pub fn advance_tick(&mut self, tick: Tick) {
        self.current_tick = tick;
        self.stats.ticks_processed += 1;
    }

    /// Schedule a key for expiration.
    pub fn schedule_expiry(&mut self, key: Vec<u8>, expiry_ms: u64, revision: u64) {
        self.key_expiry_queue.add(key, expiry_ms, revision);
    }

    /// Cancel a key's expiration (e.g., key was deleted or updated).
    pub fn cancel_expiry(&mut self, key: &[u8]) {
        self.key_expiry_queue.remove(key);
    }

    /// Collect all keys that have expired at the current tick.
    pub fn collect_expired_keys(&mut self) -> Vec<Vec<u8>> {
        let expired = self.key_expiry_queue.collect_expired(self.current_tick.ms);
        self.stats.keys_expired += expired.len() as u64;
        expired
    }

    /// Check if a key is expired at the current tick (mutation-triggered).
    ///
    /// Per Task 77: Evaluate expiration on key access.
    pub fn is_expired_at_access(&self, expiry_at: Option<Tick>) -> bool {
        if !self.mutation_expiry_enabled {
            return false;
        }
        expiry_at.is_some_and(|expiry| self.current_tick.is_at_or_after(expiry))
    }

    /// Get the expiry queue depth.
    pub fn queue_depth(&self) -> usize {
        self.key_expiry_queue.depth()
    }

    /// Get the next expiry time.
    pub fn next_expiry(&self) -> Option<u64> {
        self.key_expiry_queue.peek_next_expiry()
    }

    /// Enable or disable mutation-triggered expiration.
    pub fn set_mutation_expiry_enabled(&mut self, enabled: bool) {
        self.mutation_expiry_enabled = enabled;
    }

    /// Get statistics.
    pub fn stats(&self) -> &ExpirationStats {
        &self.stats
    }

    /// Record a lease expiration.
    pub fn record_lease_expired(&mut self, _lease_id: i64, keys_deleted: usize) {
        self.stats.leases_expired += 1;
        self.stats.lease_cascade_deletions += keys_deleted as u64;
    }
}

impl Default for ExpirationProcessor {
    fn default() -> Self {
        Self::new()
    }
}

/// Expiration statistics.
#[derive(Debug, Clone, Default)]
pub struct ExpirationStats {
    /// Total ticks processed.
    pub ticks_processed: u64,
    /// Total keys expired (via tick processing).
    pub keys_expired: u64,
    /// Total leases expired.
    pub leases_expired: u64,
    /// Total keys deleted via lease cascade.
    pub lease_cascade_deletions: u64,
    /// Current expiry queue depth.
    pub queue_depth: usize,
}

impl ExpirationStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Update queue depth.
    pub fn update_queue_depth(&mut self, depth: usize) {
        self.queue_depth = depth;
    }
}

// ============================================================================
// Lease Deadline Enforcement (Task 79)
// ============================================================================

/// Lease expiration entry for deadline tracking.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LeaseExpiryEntry {
    /// Deadline in milliseconds.
    pub deadline_ms: u64,
    /// Lease ID.
    pub lease_id: i64,
}

impl Ord for LeaseExpiryEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering for min-heap (earliest deadline first)
        other
            .deadline_ms
            .cmp(&self.deadline_ms)
            .then_with(|| other.lease_id.cmp(&self.lease_id))
    }
}

impl PartialOrd for LeaseExpiryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Lease deadline queue for efficient expiration.
///
/// Per Task 79:
/// - Evaluate keepalive_deadline_ms against committed tick
/// - Expire leases and detach keys
/// - Cascade deletion
pub struct LeaseDeadlineQueue {
    /// Min-heap of lease expiry entries (earliest deadline first).
    heap: BinaryHeap<LeaseExpiryEntry>,
    /// Set of lease IDs in the queue.
    leases_in_queue: std::collections::HashSet<i64>,
}

impl LeaseDeadlineQueue {
    /// Create a new lease deadline queue.
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            leases_in_queue: std::collections::HashSet::new(),
        }
    }

    /// Add or update a lease deadline.
    pub fn upsert(&mut self, lease_id: i64, deadline_ms: u64) {
        // Mark old entry as stale (lazy removal)
        self.leases_in_queue.remove(&lease_id);

        let entry = LeaseExpiryEntry {
            deadline_ms,
            lease_id,
        };

        self.leases_in_queue.insert(lease_id);
        self.heap.push(entry);
    }

    /// Remove a lease from the queue.
    pub fn remove(&mut self, lease_id: i64) -> bool {
        self.leases_in_queue.remove(&lease_id)
    }

    /// Collect all leases that have expired at or before the given tick.
    pub fn collect_expired(&mut self, current_tick_ms: u64) -> Vec<i64> {
        let mut expired = Vec::new();

        while let Some(entry) = self.heap.peek() {
            // Skip entries for leases no longer in queue (lazy removal)
            if !self.leases_in_queue.contains(&entry.lease_id) {
                self.heap.pop();
                continue;
            }

            if entry.deadline_ms <= current_tick_ms {
                let entry = self.heap.pop().unwrap();
                self.leases_in_queue.remove(&entry.lease_id);
                expired.push(entry.lease_id);
            } else {
                break;
            }
        }

        expired
    }

    /// Get the queue depth.
    pub fn depth(&self) -> usize {
        self.leases_in_queue.len()
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.leases_in_queue.is_empty()
    }

    /// Clear the queue.
    pub fn clear(&mut self) {
        self.heap.clear();
        self.leases_in_queue.clear();
    }
}

impl Default for LeaseDeadlineQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Combined TTL Manager
// ============================================================================

/// Combined TTL manager for deterministic expiration.
///
/// This integrates all TTL-related functionality:
/// - Tick emission (leader only)
/// - Key expiration queue
/// - Lease deadline queue
/// - TTL bounds enforcement
pub struct TtlManager {
    /// Tick emitter.
    pub tick_emitter: TickEmitter,
    /// Key expiration queue.
    pub key_expiry: ExpirationQueue,
    /// Lease deadline queue.
    pub lease_deadlines: LeaseDeadlineQueue,
    /// TTL enforcer.
    pub ttl_enforcer: TtlEnforcer,
    /// Expiration processor.
    pub processor: ExpirationProcessor,
}

impl TtlManager {
    /// Create a new TTL manager with default configuration.
    pub fn new() -> Self {
        Self {
            tick_emitter: TickEmitter::default(),
            key_expiry: ExpirationQueue::new(),
            lease_deadlines: LeaseDeadlineQueue::new(),
            ttl_enforcer: TtlEnforcer::default(),
            processor: ExpirationProcessor::new(),
        }
    }

    /// Create with custom TTL bounds.
    pub fn with_bounds(bounds: TtlBounds) -> Self {
        Self {
            tick_emitter: TickEmitter::default(),
            key_expiry: ExpirationQueue::new(),
            lease_deadlines: LeaseDeadlineQueue::new(),
            ttl_enforcer: TtlEnforcer::with_bounds(bounds),
            processor: ExpirationProcessor::new(),
        }
    }

    /// Set whether this node is the leader.
    pub fn set_leader(&mut self, is_leader: bool) {
        self.tick_emitter.set_leader(is_leader);
    }

    /// Update TTL bounds from tenant manifest.
    pub fn update_bounds(&mut self, bounds: TtlBounds) {
        self.ttl_enforcer.set_bounds(bounds);
    }

    /// Check if a tick should be emitted.
    pub fn should_emit_tick(&self) -> Option<u64> {
        self.tick_emitter.should_emit()
    }

    /// Process a tick, advancing time and collecting expirations.
    ///
    /// Returns the keys and leases that have expired.
    pub fn process_tick(&mut self, tick_ms: u64) -> TickProcessingResult {
        let tick = Tick::new(tick_ms);
        self.processor.advance_tick(tick);

        // Collect expired keys
        let expired_keys = self.key_expiry.collect_expired(tick_ms);

        // Collect expired leases
        let expired_leases = self.lease_deadlines.collect_expired(tick_ms);

        let total_deletions = expired_keys.len();

        TickProcessingResult {
            expired_keys,
            expired_leases,
            lease_cascade_keys: Vec::new(), // Filled in by caller
            total_deletions,
        }
    }

    /// Schedule a key for expiration.
    pub fn schedule_key_expiry(&mut self, key: Vec<u8>, expiry_ms: u64, revision: u64) {
        self.key_expiry.add(key.clone(), expiry_ms, revision);
        self.processor.schedule_expiry(key, expiry_ms, revision);
    }

    /// Cancel a key's expiration.
    pub fn cancel_key_expiry(&mut self, key: &[u8]) {
        self.key_expiry.remove(key);
        self.processor.cancel_expiry(key);
    }

    /// Schedule a lease deadline.
    pub fn schedule_lease_deadline(&mut self, lease_id: i64, deadline_ms: u64) {
        self.lease_deadlines.upsert(lease_id, deadline_ms);
    }

    /// Cancel a lease's deadline (lease revoked).
    pub fn cancel_lease_deadline(&mut self, lease_id: i64) {
        self.lease_deadlines.remove(lease_id);
    }

    /// Get combined statistics.
    pub fn stats(&self) -> TtlManagerStats {
        TtlManagerStats {
            tick_emitter: self.tick_emitter.stats(),
            key_expiry_queue_depth: self.key_expiry.depth(),
            lease_deadline_queue_depth: self.lease_deadlines.depth(),
            expiration: self.processor.stats().clone(),
            current_tick_ms: self.processor.current_tick().ms,
        }
    }
}

impl Default for TtlManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Combined TTL manager statistics.
#[derive(Debug, Clone)]
pub struct TtlManagerStats {
    /// Tick emitter stats.
    pub tick_emitter: TickEmitterStats,
    /// Key expiry queue depth.
    pub key_expiry_queue_depth: usize,
    /// Lease deadline queue depth.
    pub lease_deadline_queue_depth: usize,
    /// Expiration processor stats.
    pub expiration: ExpirationStats,
    /// Current tick in milliseconds.
    pub current_tick_ms: u64,
}
