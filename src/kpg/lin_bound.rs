//! LIN-BOUND linearizability gate.
//!
//! LIN-BOUND gates operations that require linearizable semantics per §9.1:
//! - Linearizable reads
//! - Compare/CAS evaluation
//! - Transaction fences
//!
//! LIN-BOUND does NOT gate writes directly; writes follow Clustor's durability
//! rules independently.
//!
//! # Predicates
//!
//! - [`can_linearize`] - Wraps Clustor's read gate predicate, leader-only check
//! - [`can_snapshot_read`] - Role-based snapshot read capability
//!
//! # Gates
//!
//! - [`LinBoundGate`] - Linearizability enforcement for various operation types
//!
//! # Cache Coupling
//!
//! When CP-Raft cache degrades to Stale/Expired:
//! - `can_linearize` returns false (strict fallback coupling)
//! - Follower read capabilities are revoked
//! - Watch-start linearization guarantees are revoked

use crate::control::cache::{CacheState, CpCacheAgent};
use crate::core::error::{LatticeError, LatticeResult, LinearizabilityFailureReason};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// Role and Read Mode
// ============================================================================

/// Role of a KPG node in the Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KpgRole {
    /// The elected leader, can serve linearizable operations.
    Leader,
    /// A voting follower, can serve snapshot-only reads if allowed.
    Follower,
    /// A non-voting learner, can serve snapshot-only reads if allowed.
    Learner,
    /// Role is unknown or transitioning.
    Unknown,
}

impl KpgRole {
    /// Check if this role can potentially serve linearizable operations.
    pub fn can_be_linearizable(&self) -> bool {
        matches!(self, Self::Leader)
    }

    /// Check if this role can potentially serve snapshot-only reads.
    pub fn can_be_snapshot_reader(&self) -> bool {
        matches!(self, Self::Leader | Self::Follower | Self::Learner)
    }
}

impl std::fmt::Display for KpgRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leader => write!(f, "leader"),
            Self::Follower => write!(f, "follower"),
            Self::Learner => write!(f, "learner"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Read semantics mode for operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadSemantics {
    /// Linearizable read (requires ReadIndex fence).
    Linearizable,
    /// Snapshot-only read (no ReadIndex required).
    SnapshotOnly,
}

impl ReadSemantics {
    /// Check if this is linearizable semantics.
    pub fn is_linearizable(&self) -> bool {
        matches!(self, Self::Linearizable)
    }
}

// ============================================================================
// Linearizability Predicates (Tasks 63, 64)
// ============================================================================

/// Result of the can_linearize predicate.
#[derive(Debug, Clone)]
pub struct LinearizeResult {
    /// Whether linearization is available.
    pub available: bool,
    /// Reason for unavailability (if not available).
    pub failure_reason: Option<LinearizabilityFailureReason>,
    /// Current node role.
    pub role: KpgRole,
    /// Cache state at time of check.
    pub cache_state: CacheState,
}

impl LinearizeResult {
    /// Create a successful result.
    pub fn available(role: KpgRole, cache_state: CacheState) -> Self {
        Self {
            available: true,
            failure_reason: None,
            role,
            cache_state,
        }
    }

    /// Create a failed result.
    pub fn unavailable(
        reason: LinearizabilityFailureReason,
        role: KpgRole,
        cache_state: CacheState,
    ) -> Self {
        Self {
            available: false,
            failure_reason: Some(reason),
            role,
            cache_state,
        }
    }

    /// Convert to a LatticeResult.
    pub fn to_result(&self) -> LatticeResult<()> {
        if self.available {
            Ok(())
        } else {
            Err(LatticeError::linearizability_unavailable(
                self.failure_reason
                    .unwrap_or(LinearizabilityFailureReason::ControlPlaneUnavailable),
            ))
        }
    }
}

/// Check if linearizable operations can be served.
///
/// Per §9.1, `can_linearize = clustor_read_gate_predicate() == true` (leader only).
///
/// Returns true only if:
/// 1. This node is the leader for the KPG
/// 2. Clustor's read gate predicate passes (ReadIndex eligible)
/// 3. CP-Raft cache is Fresh or Cached (not Stale/Expired)
pub fn can_linearize(
    role: KpgRole,
    read_gate_passes: bool,
    cache_state: CacheState,
) -> LinearizeResult {
    // Check role first
    if !role.can_be_linearizable() {
        return LinearizeResult::unavailable(
            LinearizabilityFailureReason::NotLeader,
            role,
            cache_state,
        );
    }

    // Check cache state (strict fallback coupling)
    if cache_state.forces_strict_fallback() {
        return LinearizeResult::unavailable(
            LinearizabilityFailureReason::StrictFallback,
            role,
            cache_state,
        );
    }

    // Check Clustor read gate predicate
    if !read_gate_passes {
        return LinearizeResult::unavailable(
            LinearizabilityFailureReason::ProofMismatch,
            role,
            cache_state,
        );
    }

    LinearizeResult::available(role, cache_state)
}

/// Result of the can_snapshot_read predicate.
#[derive(Debug, Clone)]
pub struct SnapshotReadResult {
    /// Whether snapshot read is available.
    pub available: bool,
    /// Current node role.
    pub role: KpgRole,
    /// Whether follower reads are explicitly allowed.
    pub follower_reads_allowed: bool,
}

impl SnapshotReadResult {
    /// Create a successful result.
    pub fn available(role: KpgRole, follower_reads_allowed: bool) -> Self {
        Self {
            available: true,
            role,
            follower_reads_allowed,
        }
    }

    /// Create an unavailable result.
    pub fn unavailable(role: KpgRole, follower_reads_allowed: bool) -> Self {
        Self {
            available: false,
            role,
            follower_reads_allowed,
        }
    }
}

/// Check if snapshot-only reads can be served.
///
/// Per §9.1:
/// - Leader snapshot-only always allowed
/// - Follower snapshot-only conditional on Clustor grant and cache state
pub fn can_snapshot_read(
    role: KpgRole,
    cache_state: CacheState,
    follower_reads_configured: bool,
) -> SnapshotReadResult {
    match role {
        KpgRole::Leader => {
            // Leader can always serve snapshot reads
            SnapshotReadResult::available(role, true)
        }
        KpgRole::Follower | KpgRole::Learner => {
            // Follower/Learner needs explicit configuration and fresh cache
            if follower_reads_configured && cache_state.allows_follower_reads() {
                SnapshotReadResult::available(role, true)
            } else {
                SnapshotReadResult::unavailable(role, follower_reads_configured)
            }
        }
        KpgRole::Unknown => SnapshotReadResult::unavailable(role, false),
    }
}

// ============================================================================
// LIN-BOUND Gate (Tasks 65, 66, 67)
// ============================================================================

/// Operation type for LIN-BOUND gating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatedOperation {
    /// Linearizable read (GET with serializable=false).
    LinearizableRead,
    /// Compare/CAS operation evaluation.
    Compare,
    /// Transaction fence evaluation.
    TransactionFence,
    /// Watch start with linearizable semantics.
    WatchStart,
}

impl std::fmt::Display for GatedOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LinearizableRead => write!(f, "linearizable_read"),
            Self::Compare => write!(f, "compare"),
            Self::TransactionFence => write!(f, "transaction_fence"),
            Self::WatchStart => write!(f, "watch_start"),
        }
    }
}

/// LIN-BOUND gate for linearizability enforcement.
///
/// The gate enforces that operations requiring linearizable semantics
/// can only proceed when `can_linearize` returns true.
///
/// Per §9.1, LIN-BOUND gates:
/// - Linearizable reads
/// - Compare/CAS evaluation
/// - Transaction fences
///
/// LIN-BOUND does NOT gate non-conditional writes.
pub struct LinBoundGate {
    /// Current node role.
    role: KpgRole,
    /// Whether Clustor's read gate predicate passes.
    read_gate_passes: bool,
    /// Reference to CP cache agent for cache state.
    cache_agent: Option<Arc<CpCacheAgent>>,
    /// Override cache state (for testing or when cache agent unavailable).
    override_cache_state: Option<CacheState>,
    /// Whether follower reads are configured.
    follower_reads_configured: bool,
    /// Telemetry counters.
    stats: LinBoundStats,
}

impl LinBoundGate {
    /// Create a new LIN-BOUND gate.
    pub fn new() -> Self {
        Self {
            role: KpgRole::Unknown,
            read_gate_passes: false,
            cache_agent: None,
            override_cache_state: None,
            follower_reads_configured: false,
            stats: LinBoundStats::new(),
        }
    }

    /// Create a gate with a cache agent.
    pub fn with_cache_agent(cache_agent: Arc<CpCacheAgent>) -> Self {
        Self {
            role: KpgRole::Unknown,
            read_gate_passes: false,
            cache_agent: Some(cache_agent),
            override_cache_state: None,
            follower_reads_configured: false,
            stats: LinBoundStats::new(),
        }
    }

    /// Set the current node role.
    pub fn set_role(&mut self, role: KpgRole) {
        self.role = role;
    }

    /// Update the Clustor read gate predicate status.
    pub fn set_read_gate(&mut self, passes: bool) {
        self.read_gate_passes = passes;
    }

    /// Set follower reads configuration.
    pub fn set_follower_reads_configured(&mut self, configured: bool) {
        self.follower_reads_configured = configured;
    }

    /// Override cache state (for testing).
    pub fn override_cache_state(&mut self, state: CacheState) {
        self.override_cache_state = Some(state);
    }

    /// Clear cache state override.
    pub fn clear_cache_override(&mut self) {
        self.override_cache_state = None;
    }

    /// Get the effective cache state.
    fn effective_cache_state(&self) -> CacheState {
        if let Some(state) = self.override_cache_state {
            return state;
        }
        if let Some(agent) = &self.cache_agent {
            return agent.overall_state();
        }
        CacheState::Fresh // Default to Fresh if no cache agent
    }

    /// Get the current role.
    pub fn role(&self) -> KpgRole {
        self.role
    }

    /// Check if linearization is currently available.
    pub fn can_linearize(&self) -> LinearizeResult {
        can_linearize(
            self.role,
            self.read_gate_passes,
            self.effective_cache_state(),
        )
    }

    /// Check if snapshot reads are currently available.
    pub fn can_snapshot_read(&self) -> SnapshotReadResult {
        can_snapshot_read(
            self.role,
            self.effective_cache_state(),
            self.follower_reads_configured,
        )
    }

    /// Gate a linearizable read operation.
    ///
    /// Per §9.1 and Task 65, requires `can_linearize=true`.
    /// Fails closed with appropriate error.
    pub fn gate_linearizable_read(&self) -> LatticeResult<()> {
        self.stats.increment_check(GatedOperation::LinearizableRead);

        let result = self.can_linearize();
        if result.available {
            self.stats.increment_pass(GatedOperation::LinearizableRead);
            Ok(())
        } else {
            self.stats.increment_fail(
                GatedOperation::LinearizableRead,
                result
                    .failure_reason
                    .unwrap_or(LinearizabilityFailureReason::ControlPlaneUnavailable),
            );
            result.to_result()
        }
    }

    /// Gate a compare/CAS operation.
    ///
    /// Per §9.1 and Task 66, CAS evaluation requires `can_linearize=true`.
    /// Note: LIN-BOUND does not gate non-conditional writes.
    pub fn gate_compare(&self) -> LatticeResult<()> {
        self.stats.increment_check(GatedOperation::Compare);

        let result = self.can_linearize();
        if result.available {
            self.stats.increment_pass(GatedOperation::Compare);
            Ok(())
        } else {
            self.stats.increment_fail(
                GatedOperation::Compare,
                result
                    .failure_reason
                    .unwrap_or(LinearizabilityFailureReason::ControlPlaneUnavailable),
            );
            result.to_result()
        }
    }

    /// Gate a transaction fence operation.
    ///
    /// Per §9.1 and Task 67, transaction fence evaluation requires `can_linearize=true`.
    /// Fails closed with LinearizabilityUnavailable.
    pub fn gate_transaction_fence(&self) -> LatticeResult<()> {
        self.stats.increment_check(GatedOperation::TransactionFence);

        let result = self.can_linearize();
        if result.available {
            self.stats.increment_pass(GatedOperation::TransactionFence);
            Ok(())
        } else {
            self.stats.increment_fail(
                GatedOperation::TransactionFence,
                result
                    .failure_reason
                    .unwrap_or(LinearizabilityFailureReason::ControlPlaneUnavailable),
            );
            result.to_result()
        }
    }

    /// Gate a watch start operation with linearizable semantics.
    ///
    /// Per §10.1.6, start_revision=0 requires `can_linearize=true`.
    pub fn gate_watch_start(&self) -> LatticeResult<()> {
        self.stats.increment_check(GatedOperation::WatchStart);

        let result = self.can_linearize();
        if result.available {
            self.stats.increment_pass(GatedOperation::WatchStart);
            Ok(())
        } else {
            self.stats.increment_fail(
                GatedOperation::WatchStart,
                result
                    .failure_reason
                    .unwrap_or(LinearizabilityFailureReason::ControlPlaneUnavailable),
            );
            result.to_result()
        }
    }

    /// Get telemetry statistics.
    pub fn stats(&self) -> &LinBoundStats {
        &self.stats
    }
}

impl Default for LinBoundGate {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Snapshot-Only Read Path (Task 68)
// ============================================================================

/// Snapshot-only read configuration.
#[derive(Debug, Clone)]
pub struct SnapshotReadConfig {
    /// Whether snapshot-only reads are enabled.
    pub enabled: bool,
    /// Per-adapter snapshot read configuration.
    pub adapter_config: SnapshotAdapterConfig,
}

impl Default for SnapshotReadConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            adapter_config: SnapshotAdapterConfig::default(),
        }
    }
}

/// Per-adapter snapshot read configuration.
#[derive(Debug, Clone, Default)]
pub struct SnapshotAdapterConfig {
    /// etcd: Allow serializable=true reads.
    pub etcd_serializable: bool,
    /// Redis: Commands that may be served as snapshot-only.
    pub redis_snapshot_commands: Vec<String>,
    /// Memcached: Allow snapshot-only gets.
    pub memcached_snapshot_get: bool,
}

/// Snapshot-only read path handler.
///
/// Per Task 68, serves from committed state without ReadIndex fence.
/// Responses are labeled for telemetry.
pub struct SnapshotReadPath {
    /// Configuration.
    config: SnapshotReadConfig,
    /// Current committed revision.
    committed_revision: u64,
    /// Statistics.
    stats: SnapshotReadStats,
}

impl SnapshotReadPath {
    /// Create a new snapshot read path.
    pub fn new(config: SnapshotReadConfig) -> Self {
        Self {
            config,
            committed_revision: 0,
            stats: SnapshotReadStats::new(),
        }
    }

    /// Check if snapshot reads are enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Update the committed revision.
    pub fn update_revision(&mut self, revision: u64) {
        self.committed_revision = revision;
    }

    /// Get the current snapshot revision.
    pub fn snapshot_revision(&self) -> u64 {
        self.committed_revision
    }

    /// Record a snapshot read for telemetry.
    pub fn record_read(&self, adapter: &str) {
        self.stats.increment_reads(adapter);
    }

    /// Get statistics.
    pub fn stats(&self) -> &SnapshotReadStats {
        &self.stats
    }
}

impl Default for SnapshotReadPath {
    fn default() -> Self {
        Self::new(SnapshotReadConfig::default())
    }
}

/// Snapshot read statistics.
#[derive(Debug)]
pub struct SnapshotReadStats {
    /// Total snapshot reads.
    total_reads: AtomicU64,
    /// etcd snapshot reads.
    etcd_reads: AtomicU64,
    /// Redis snapshot reads.
    redis_reads: AtomicU64,
    /// Memcached snapshot reads.
    memcached_reads: AtomicU64,
}

impl SnapshotReadStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self {
            total_reads: AtomicU64::new(0),
            etcd_reads: AtomicU64::new(0),
            redis_reads: AtomicU64::new(0),
            memcached_reads: AtomicU64::new(0),
        }
    }

    /// Increment reads for an adapter.
    pub fn increment_reads(&self, adapter: &str) {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
        match adapter {
            "etcd" => self.etcd_reads.fetch_add(1, Ordering::Relaxed),
            "redis" => self.redis_reads.fetch_add(1, Ordering::Relaxed),
            "memcached" => self.memcached_reads.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    /// Get total reads.
    pub fn total_reads(&self) -> u64 {
        self.total_reads.load(Ordering::Relaxed)
    }

    /// Get adapter-specific reads.
    pub fn adapter_reads(&self, adapter: &str) -> u64 {
        match adapter {
            "etcd" => self.etcd_reads.load(Ordering::Relaxed),
            "redis" => self.redis_reads.load(Ordering::Relaxed),
            "memcached" => self.memcached_reads.load(Ordering::Relaxed),
            _ => 0,
        }
    }
}

impl Default for SnapshotReadStats {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Strict Fallback Coupling (Task 69)
// ============================================================================

/// Strict fallback coupling handler.
///
/// Per Task 69, detects strict fallback from Clustor and:
/// - Forces LIN-BOUND failure
/// - Forces stricter durability mode
pub struct StrictFallbackCoupling {
    /// Whether strict fallback is active.
    strict_fallback_active: bool,
    /// Forced durability mode during strict fallback.
    forced_durability: DurabilityMode,
}

/// Durability mode for writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DurabilityMode {
    /// Normal durability (configurable).
    #[default]
    Normal,
    /// Strict durability (forced during fallback).
    Strict,
}

impl StrictFallbackCoupling {
    /// Create a new strict fallback handler.
    pub fn new() -> Self {
        Self {
            strict_fallback_active: false,
            forced_durability: DurabilityMode::Normal,
        }
    }

    /// Activate strict fallback mode.
    pub fn activate(&mut self) {
        self.strict_fallback_active = true;
        self.forced_durability = DurabilityMode::Strict;
    }

    /// Deactivate strict fallback mode.
    pub fn deactivate(&mut self) {
        self.strict_fallback_active = false;
        self.forced_durability = DurabilityMode::Normal;
    }

    /// Check if strict fallback is active.
    pub fn is_active(&self) -> bool {
        self.strict_fallback_active
    }

    /// Get the current durability mode.
    pub fn durability_mode(&self) -> DurabilityMode {
        self.forced_durability
    }

    /// Update from cache state.
    ///
    /// Per §12.2, Stale/Expired caches force strict fallback.
    pub fn update_from_cache(&mut self, cache_state: CacheState) {
        if cache_state.forces_strict_fallback() {
            self.activate();
        } else {
            self.deactivate();
        }
    }

    /// Check if LIN-BOUND should fail.
    pub fn should_fail_lin_bound(&self) -> bool {
        self.strict_fallback_active
    }
}

impl Default for StrictFallbackCoupling {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Follower Read Routing (Task 70)
// ============================================================================

/// Follower read routing decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowerReadDecision {
    /// Serve the read locally.
    ServeLocal,
    /// Redirect to leader.
    RedirectToLeader,
    /// Fail the operation.
    Fail,
}

/// Follower read router.
///
/// Per Task 70:
/// - Detects follower role
/// - Redirects or fails for linearizable operations
/// - Serves snapshot-only reads per can_snapshot_read rules
pub struct FollowerReadRouter {
    /// Whether follower reads are enabled.
    follower_reads_enabled: bool,
    /// Current node role.
    role: KpgRole,
    /// Current cache state.
    cache_state: CacheState,
}

impl FollowerReadRouter {
    /// Create a new follower read router.
    pub fn new(follower_reads_enabled: bool) -> Self {
        Self {
            follower_reads_enabled,
            role: KpgRole::Unknown,
            cache_state: CacheState::Fresh,
        }
    }

    /// Update the current role.
    pub fn set_role(&mut self, role: KpgRole) {
        self.role = role;
    }

    /// Update the cache state.
    pub fn set_cache_state(&mut self, state: CacheState) {
        self.cache_state = state;
    }

    /// Route a read operation.
    pub fn route_read(&self, semantics: ReadSemantics) -> FollowerReadDecision {
        match self.role {
            KpgRole::Leader => {
                // Leader always serves reads
                FollowerReadDecision::ServeLocal
            }
            KpgRole::Follower | KpgRole::Learner => {
                match semantics {
                    ReadSemantics::Linearizable => {
                        // Cannot serve linearizable reads on follower
                        FollowerReadDecision::RedirectToLeader
                    }
                    ReadSemantics::SnapshotOnly => {
                        // Check if snapshot reads are allowed
                        let result = can_snapshot_read(
                            self.role,
                            self.cache_state,
                            self.follower_reads_enabled,
                        );
                        if result.available {
                            FollowerReadDecision::ServeLocal
                        } else {
                            FollowerReadDecision::RedirectToLeader
                        }
                    }
                }
            }
            KpgRole::Unknown => FollowerReadDecision::Fail,
        }
    }

    /// Check if we should redirect to leader.
    pub fn should_redirect(&self, semantics: ReadSemantics) -> bool {
        matches!(
            self.route_read(semantics),
            FollowerReadDecision::RedirectToLeader
        )
    }
}

impl Default for FollowerReadRouter {
    fn default() -> Self {
        Self::new(false)
    }
}

// ============================================================================
// Per-Session Monotonicity (Task 71)
// ============================================================================

/// Session monotonicity tracker.
///
/// Per Task 71, provides best-effort sticky session routing.
/// Note: This is explicitly documented as having no semantic guarantee.
#[derive(Debug)]
pub struct SessionMonotonicity {
    /// Last seen revision per session.
    session_revisions: std::collections::HashMap<u64, u64>,
    /// Maximum tracked sessions.
    max_sessions: usize,
}

impl SessionMonotonicity {
    /// Create a new session monotonicity tracker.
    pub fn new(max_sessions: usize) -> Self {
        Self {
            session_revisions: std::collections::HashMap::new(),
            max_sessions,
        }
    }

    /// Record a session's observed revision.
    pub fn record(&mut self, session_id: u64, revision: u64) {
        if self.session_revisions.len() >= self.max_sessions
            && !self.session_revisions.contains_key(&session_id)
        {
            // Evict oldest session (simple eviction strategy)
            if let Some(&oldest_key) = self.session_revisions.keys().next() {
                self.session_revisions.remove(&oldest_key);
            }
        }

        self.session_revisions
            .entry(session_id)
            .and_modify(|rev| {
                if revision > *rev {
                    *rev = revision;
                }
            })
            .or_insert(revision);
    }

    /// Get the last known revision for a session.
    pub fn last_revision(&self, session_id: u64) -> Option<u64> {
        self.session_revisions.get(&session_id).copied()
    }

    /// Check if a revision satisfies monotonicity for a session.
    ///
    /// Returns true if the given revision is >= the session's last seen revision.
    /// Returns true if the session has no recorded revision.
    pub fn satisfies_monotonicity(&self, session_id: u64, revision: u64) -> bool {
        self.session_revisions
            .get(&session_id)
            .is_none_or(|&last| revision >= last)
    }

    /// Remove a session.
    pub fn remove_session(&mut self, session_id: u64) {
        self.session_revisions.remove(&session_id);
    }

    /// Clear all sessions.
    pub fn clear(&mut self) {
        self.session_revisions.clear();
    }

    /// Get session count.
    pub fn session_count(&self) -> usize {
        self.session_revisions.len()
    }
}

impl Default for SessionMonotonicity {
    fn default() -> Self {
        Self::new(10000)
    }
}

// ============================================================================
// Read Semantics Telemetry (Task 72)
// ============================================================================

/// LIN-BOUND telemetry statistics.
///
/// Per Task 72, tracks:
/// - lin_bound.can_linearize
/// - lin_bound.failed_clause
/// - read_gate.* (mirrors Clustor)
#[derive(Debug)]
pub struct LinBoundStats {
    // Gate check counters
    linearizable_read_checks: AtomicU64,
    linearizable_read_passes: AtomicU64,
    compare_checks: AtomicU64,
    compare_passes: AtomicU64,
    txn_fence_checks: AtomicU64,
    txn_fence_passes: AtomicU64,
    watch_start_checks: AtomicU64,
    watch_start_passes: AtomicU64,

    // Failure reason counters
    fail_not_leader: AtomicU64,
    fail_strict_fallback: AtomicU64,
    fail_proof_mismatch: AtomicU64,
    fail_control_plane: AtomicU64,
}

impl LinBoundStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self {
            linearizable_read_checks: AtomicU64::new(0),
            linearizable_read_passes: AtomicU64::new(0),
            compare_checks: AtomicU64::new(0),
            compare_passes: AtomicU64::new(0),
            txn_fence_checks: AtomicU64::new(0),
            txn_fence_passes: AtomicU64::new(0),
            watch_start_checks: AtomicU64::new(0),
            watch_start_passes: AtomicU64::new(0),
            fail_not_leader: AtomicU64::new(0),
            fail_strict_fallback: AtomicU64::new(0),
            fail_proof_mismatch: AtomicU64::new(0),
            fail_control_plane: AtomicU64::new(0),
        }
    }

    /// Increment check counter for an operation.
    fn increment_check(&self, op: GatedOperation) {
        match op {
            GatedOperation::LinearizableRead => {
                self.linearizable_read_checks
                    .fetch_add(1, Ordering::Relaxed);
            }
            GatedOperation::Compare => {
                self.compare_checks.fetch_add(1, Ordering::Relaxed);
            }
            GatedOperation::TransactionFence => {
                self.txn_fence_checks.fetch_add(1, Ordering::Relaxed);
            }
            GatedOperation::WatchStart => {
                self.watch_start_checks.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Increment pass counter for an operation.
    fn increment_pass(&self, op: GatedOperation) {
        match op {
            GatedOperation::LinearizableRead => {
                self.linearizable_read_passes
                    .fetch_add(1, Ordering::Relaxed);
            }
            GatedOperation::Compare => {
                self.compare_passes.fetch_add(1, Ordering::Relaxed);
            }
            GatedOperation::TransactionFence => {
                self.txn_fence_passes.fetch_add(1, Ordering::Relaxed);
            }
            GatedOperation::WatchStart => {
                self.watch_start_passes.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Increment failure counter for a reason.
    fn increment_fail(&self, _op: GatedOperation, reason: LinearizabilityFailureReason) {
        match reason {
            LinearizabilityFailureReason::NotLeader => {
                self.fail_not_leader.fetch_add(1, Ordering::Relaxed);
            }
            LinearizabilityFailureReason::StrictFallback => {
                self.fail_strict_fallback.fetch_add(1, Ordering::Relaxed);
            }
            LinearizabilityFailureReason::ProofMismatch => {
                self.fail_proof_mismatch.fetch_add(1, Ordering::Relaxed);
            }
            LinearizabilityFailureReason::ControlPlaneUnavailable => {
                self.fail_control_plane.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get check count for an operation.
    pub fn checks(&self, op: GatedOperation) -> u64 {
        match op {
            GatedOperation::LinearizableRead => {
                self.linearizable_read_checks.load(Ordering::Relaxed)
            }
            GatedOperation::Compare => self.compare_checks.load(Ordering::Relaxed),
            GatedOperation::TransactionFence => self.txn_fence_checks.load(Ordering::Relaxed),
            GatedOperation::WatchStart => self.watch_start_checks.load(Ordering::Relaxed),
        }
    }

    /// Get pass count for an operation.
    pub fn passes(&self, op: GatedOperation) -> u64 {
        match op {
            GatedOperation::LinearizableRead => {
                self.linearizable_read_passes.load(Ordering::Relaxed)
            }
            GatedOperation::Compare => self.compare_passes.load(Ordering::Relaxed),
            GatedOperation::TransactionFence => self.txn_fence_passes.load(Ordering::Relaxed),
            GatedOperation::WatchStart => self.watch_start_passes.load(Ordering::Relaxed),
        }
    }

    /// Get failure count by reason.
    pub fn failures_by_reason(&self, reason: LinearizabilityFailureReason) -> u64 {
        match reason {
            LinearizabilityFailureReason::NotLeader => self.fail_not_leader.load(Ordering::Relaxed),
            LinearizabilityFailureReason::StrictFallback => {
                self.fail_strict_fallback.load(Ordering::Relaxed)
            }
            LinearizabilityFailureReason::ProofMismatch => {
                self.fail_proof_mismatch.load(Ordering::Relaxed)
            }
            LinearizabilityFailureReason::ControlPlaneUnavailable => {
                self.fail_control_plane.load(Ordering::Relaxed)
            }
        }
    }

    /// Get total failure count.
    pub fn total_failures(&self) -> u64 {
        self.fail_not_leader.load(Ordering::Relaxed)
            + self.fail_strict_fallback.load(Ordering::Relaxed)
            + self.fail_proof_mismatch.load(Ordering::Relaxed)
            + self.fail_control_plane.load(Ordering::Relaxed)
    }

    /// Get the can_linearize pass rate (0.0 to 1.0).
    pub fn can_linearize_rate(&self) -> f64 {
        let total_checks = self.linearizable_read_checks.load(Ordering::Relaxed)
            + self.compare_checks.load(Ordering::Relaxed)
            + self.txn_fence_checks.load(Ordering::Relaxed)
            + self.watch_start_checks.load(Ordering::Relaxed);

        let total_passes = self.linearizable_read_passes.load(Ordering::Relaxed)
            + self.compare_passes.load(Ordering::Relaxed)
            + self.txn_fence_passes.load(Ordering::Relaxed)
            + self.watch_start_passes.load(Ordering::Relaxed);

        if total_checks == 0 {
            1.0
        } else {
            total_passes as f64 / total_checks as f64
        }
    }
}

impl Default for LinBoundStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Read gate telemetry (mirrors Clustor).
#[derive(Debug, Default)]
pub struct ReadGateStats {
    /// Total read gate checks.
    pub checks: AtomicU64,
    /// Read gate passes.
    pub passes: AtomicU64,
    /// Read gate failures.
    pub failures: AtomicU64,
}

impl ReadGateStats {
    /// Create new stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a read gate check result.
    pub fn record(&self, passed: bool) {
        self.checks.fetch_add(1, Ordering::Relaxed);
        if passed {
            self.passes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get pass rate.
    pub fn pass_rate(&self) -> f64 {
        let checks = self.checks.load(Ordering::Relaxed);
        if checks == 0 {
            1.0
        } else {
            self.passes.load(Ordering::Relaxed) as f64 / checks as f64
        }
    }
}

/// Combined read semantics telemetry.
#[derive(Debug, Default)]
pub struct ReadSemanticsTelemetry {
    /// LIN-BOUND stats.
    pub lin_bound: LinBoundStats,
    /// Read gate stats (mirrors Clustor).
    pub read_gate: ReadGateStats,
    /// Snapshot read stats.
    pub snapshot_reads: SnapshotReadStats,
}

impl ReadSemanticsTelemetry {
    /// Create new telemetry.
    pub fn new() -> Self {
        Self::default()
    }
}
