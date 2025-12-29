//! CP-Raft cache agent and freshness semantics.
//!
//! The cache agent watches CP-Raft for tenant manifests, routing epochs,
//! adapter policies, quotas, and PKI material. Cache state follows Clustor's
//! Fresh/Cached/Stale/Expired matrix per §12.2.
//!
//! # LIN-BOUND Coupling
//!
//! When cache state degrades to Stale or Expired:
//! - Linearizable operations MUST fail with LinearizabilityUnavailable
//! - Follower-read capabilities are revoked
//! - Watch-start linearization guarantees are revoked
//! - Strict fallback behavior is forced

use crate::control::api::{TenantManifest, TenantPki};
use crate::control::placement::PlacementTable;
use crate::control::routing::RoutingTable;
use crate::core::error::{LatticeError, LatticeResult, LinearizabilityFailureReason};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Cache state following Clustor's cache state matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheState {
    /// Recently fetched, within TTL.
    Fresh,
    /// Valid but approaching TTL.
    Cached,
    /// Past TTL but within grace period.
    Stale,
    /// Past grace period, must refresh or fail.
    Expired,
}

impl CacheState {
    /// Check if this cache state allows linearizable operations.
    ///
    /// Per §12.2, Stale/Expired caches force LIN-BOUND failure.
    pub fn allows_linearizable(&self) -> bool {
        matches!(self, Self::Fresh | Self::Cached)
    }

    /// Check if this cache state allows follower reads.
    ///
    /// Per §12.2, Stale/Expired caches revoke follower-read capabilities.
    pub fn allows_follower_reads(&self) -> bool {
        matches!(self, Self::Fresh | Self::Cached)
    }

    /// Check if strict fallback should be forced.
    ///
    /// Per §12.2, Stale/Expired caches force strict fallback behavior.
    pub fn forces_strict_fallback(&self) -> bool {
        matches!(self, Self::Stale | Self::Expired)
    }

    /// Check if watch start linearization is available.
    ///
    /// Per §12.2, Stale/Expired caches revoke watch-start linearization.
    pub fn allows_watch_linearization(&self) -> bool {
        matches!(self, Self::Fresh | Self::Cached)
    }
}

/// A cached value with freshness tracking.
#[derive(Debug, Clone)]
pub struct CachedValue<T> {
    /// The cached value.
    pub value: T,
    /// When the value was fetched.
    pub fetched_at: Instant,
    /// TTL in milliseconds.
    pub ttl_ms: u64,
    /// Grace period in milliseconds (for Stale state).
    pub grace_ms: u64,
}

impl<T> CachedValue<T> {
    /// Create a new cached value.
    pub fn new(value: T, ttl_ms: u64, grace_ms: u64) -> Self {
        Self {
            value,
            fetched_at: Instant::now(),
            ttl_ms,
            grace_ms,
        }
    }

    /// Get the current cache state.
    pub fn state(&self) -> CacheState {
        let elapsed_ms = self.fetched_at.elapsed().as_millis() as u64;

        if elapsed_ms <= self.ttl_ms / 2 {
            CacheState::Fresh
        } else if elapsed_ms <= self.ttl_ms {
            CacheState::Cached
        } else if elapsed_ms <= self.ttl_ms + self.grace_ms {
            CacheState::Stale
        } else {
            CacheState::Expired
        }
    }

    /// Check if the cache needs refresh (Stale or Expired).
    pub fn needs_refresh(&self) -> bool {
        matches!(self.state(), CacheState::Stale | CacheState::Expired)
    }

    /// Get a reference to the cached value.
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Update the cached value and reset the timestamp.
    pub fn update(&mut self, value: T) {
        self.value = value;
        self.fetched_at = Instant::now();
    }

    /// Get age in milliseconds.
    pub fn age_ms(&self) -> u64 {
        self.fetched_at.elapsed().as_millis() as u64
    }
}

/// Configuration for the CP cache agent.
#[derive(Debug, Clone)]
pub struct CpCacheConfig {
    /// Cache TTL in seconds.
    pub cache_ttl_seconds: u64,

    /// Grace period in seconds (for Stale state).
    pub grace_seconds: u64,

    /// Background refresh interval in seconds.
    pub refresh_interval_seconds: u64,

    /// CP-Raft endpoints for external mode.
    pub endpoints: Vec<String>,

    /// Whether we're in embedded mode.
    pub embedded_mode: bool,
}

impl Default for CpCacheConfig {
    fn default() -> Self {
        Self {
            cache_ttl_seconds: 5,
            grace_seconds: 10,
            refresh_interval_seconds: 2,
            endpoints: Vec::new(),
            embedded_mode: true,
        }
    }
}

impl CpCacheConfig {
    /// Get TTL in milliseconds.
    pub fn ttl_ms(&self) -> u64 {
        self.cache_ttl_seconds * 1000
    }

    /// Get grace period in milliseconds.
    pub fn grace_ms(&self) -> u64 {
        self.grace_seconds * 1000
    }
}

/// CP-Raft cache agent that maintains tenant configuration.
///
/// The cache agent watches CP-Raft for configuration changes and
/// maintains local cached copies with freshness tracking.
pub struct CpCacheAgent {
    /// Cache configuration.
    config: CpCacheConfig,

    /// Cached tenant manifests.
    pub(crate) manifests: Arc<RwLock<HashMap<String, CachedValue<TenantManifest>>>>,

    /// Cached routing tables.
    routing_tables: Arc<RwLock<HashMap<String, CachedValue<RoutingTable>>>>,

    /// Cached placement tables.
    placement_tables: Arc<RwLock<HashMap<String, CachedValue<PlacementTable>>>>,

    /// Cached PKI material.
    pki_cache: Arc<RwLock<HashMap<String, CachedValue<TenantPki>>>>,

    /// Overall cache state (worst of all tenants).
    overall_state: Arc<RwLock<CacheState>>,

    /// Whether the agent is running.
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl CpCacheAgent {
    /// Create a new cache agent.
    pub fn new(config: CpCacheConfig) -> Self {
        Self {
            config,
            manifests: Arc::new(RwLock::new(HashMap::new())),
            routing_tables: Arc::new(RwLock::new(HashMap::new())),
            placement_tables: Arc::new(RwLock::new(HashMap::new())),
            pki_cache: Arc::new(RwLock::new(HashMap::new())),
            overall_state: Arc::new(RwLock::new(CacheState::Fresh)),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Get the cache configuration.
    pub fn config(&self) -> &CpCacheConfig {
        &self.config
    }

    /// Get the overall cache state.
    pub fn overall_state(&self) -> CacheState {
        *self.overall_state.read().unwrap()
    }

    /// Check if linearizable operations are allowed.
    pub fn allows_linearizable(&self) -> bool {
        self.overall_state().allows_linearizable()
    }

    /// Require linearizable capability, returning error if unavailable.
    ///
    /// This is the LIN-BOUND enforcement point. Call this before any
    /// operation that requires linearizability guarantees.
    pub fn require_linearizable(&self) -> LatticeResult<()> {
        let state = self.overall_state();
        if state.allows_linearizable() {
            Ok(())
        } else {
            Err(LatticeError::linearizability_unavailable(
                LinearizabilityFailureReason::ControlPlaneUnavailable,
            ))
        }
    }

    /// Require follower read capability, returning error if unavailable.
    pub fn require_follower_read(&self) -> LatticeResult<()> {
        let state = self.overall_state();
        if state.allows_follower_reads() {
            Ok(())
        } else {
            Err(LatticeError::linearizability_unavailable(
                LinearizabilityFailureReason::StrictFallback,
            ))
        }
    }

    /// Require watch linearization capability, returning error if unavailable.
    pub fn require_watch_linearization(&self) -> LatticeResult<()> {
        let state = self.overall_state();
        if state.allows_watch_linearization() {
            Ok(())
        } else {
            Err(LatticeError::linearizability_unavailable(
                LinearizabilityFailureReason::ControlPlaneUnavailable,
            ))
        }
    }

    /// Get cached tenant manifest.
    pub fn get_manifest(&self, tenant_id: &str) -> Option<TenantManifest> {
        let manifests = self.manifests.read().unwrap();
        manifests.get(tenant_id).map(|cv| cv.value.clone())
    }

    /// Get tenant manifest with state check.
    pub fn get_manifest_checked(&self, tenant_id: &str) -> LatticeResult<TenantManifest> {
        let manifests = self.manifests.read().unwrap();
        match manifests.get(tenant_id) {
            Some(cv) => {
                if cv.state().allows_linearizable() {
                    Ok(cv.value.clone())
                } else {
                    Err(LatticeError::linearizability_unavailable(
                        LinearizabilityFailureReason::ControlPlaneUnavailable,
                    ))
                }
            }
            None => Err(LatticeError::Internal {
                message: format!("tenant {} not found in cache", tenant_id),
            }),
        }
    }

    /// Get cached routing table.
    pub fn get_routing(&self, tenant_id: &str) -> Option<RoutingTable> {
        let tables = self.routing_tables.read().unwrap();
        tables.get(tenant_id).map(|cv| cv.value.clone())
    }

    /// Get routing table with state check.
    pub fn get_routing_checked(&self, tenant_id: &str) -> LatticeResult<RoutingTable> {
        let tables = self.routing_tables.read().unwrap();
        match tables.get(tenant_id) {
            Some(cv) => {
                if cv.state().allows_linearizable() {
                    Ok(cv.value.clone())
                } else {
                    Err(LatticeError::linearizability_unavailable(
                        LinearizabilityFailureReason::ControlPlaneUnavailable,
                    ))
                }
            }
            None => Err(LatticeError::Internal {
                message: format!("routing for tenant {} not found", tenant_id),
            }),
        }
    }

    /// Get cached placement table.
    pub fn get_placement(&self, tenant_id: &str) -> Option<PlacementTable> {
        let tables = self.placement_tables.read().unwrap();
        tables.get(tenant_id).map(|cv| cv.value.clone())
    }

    /// Get cached PKI material.
    pub fn get_pki(&self, tenant_id: &str) -> Option<TenantPki> {
        let pki = self.pki_cache.read().unwrap();
        pki.get(tenant_id).map(|cv| cv.value.clone())
    }

    /// Update cached manifest from CP-Raft.
    pub fn update_manifest(&self, manifest: TenantManifest) {
        let mut manifests = self.manifests.write().unwrap();
        let tenant_id = manifest.tenant_id.clone();

        // Also update routing table
        let routing = RoutingTable::new(
            manifest.tenant_id.clone(),
            manifest.routing_epoch,
            manifest.hash_seed_version,
            manifest.kpg_count,
        );

        manifests.insert(
            tenant_id.clone(),
            CachedValue::new(manifest, self.config.ttl_ms(), self.config.grace_ms()),
        );

        drop(manifests);

        let mut routing_tables = self.routing_tables.write().unwrap();
        routing_tables.insert(
            tenant_id,
            CachedValue::new(routing, self.config.ttl_ms(), self.config.grace_ms()),
        );

        drop(routing_tables);
        self.update_overall_state();
    }

    /// Update cached placement from CP-Raft.
    pub fn update_placement(&self, placement: PlacementTable) {
        let mut tables = self.placement_tables.write().unwrap();
        tables.insert(
            placement.tenant_id.clone(),
            CachedValue::new(placement, self.config.ttl_ms(), self.config.grace_ms()),
        );
        drop(tables);
        self.update_overall_state();
    }

    /// Update cached PKI material.
    pub fn update_pki(&self, pki: TenantPki) {
        let mut cache = self.pki_cache.write().unwrap();
        cache.insert(
            pki.tenant_id.clone(),
            CachedValue::new(pki, self.config.ttl_ms(), self.config.grace_ms()),
        );
        drop(cache);
        self.update_overall_state();
    }

    /// Remove tenant from cache.
    pub fn remove_tenant(&self, tenant_id: &str) {
        self.manifests.write().unwrap().remove(tenant_id);
        self.routing_tables.write().unwrap().remove(tenant_id);
        self.placement_tables.write().unwrap().remove(tenant_id);
        self.pki_cache.write().unwrap().remove(tenant_id);
        self.update_overall_state();
    }

    /// Update overall cache state based on all cached items.
    fn update_overall_state(&self) {
        let mut worst_state = CacheState::Fresh;

        // Check manifests
        for cv in self.manifests.read().unwrap().values() {
            let state = cv.state();
            if state as u8 > worst_state as u8 {
                worst_state = state;
            }
        }

        // Check routing
        for cv in self.routing_tables.read().unwrap().values() {
            let state = cv.state();
            if state as u8 > worst_state as u8 {
                worst_state = state;
            }
        }

        *self.overall_state.write().unwrap() = worst_state;
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let manifests = self.manifests.read().unwrap();
        let routing = self.routing_tables.read().unwrap();
        let placement = self.placement_tables.read().unwrap();

        CacheStats {
            tenant_count: manifests.len(),
            overall_state: self.overall_state(),
            manifests_fresh: manifests
                .values()
                .filter(|cv| cv.state() == CacheState::Fresh)
                .count(),
            manifests_cached: manifests
                .values()
                .filter(|cv| cv.state() == CacheState::Cached)
                .count(),
            manifests_stale: manifests
                .values()
                .filter(|cv| cv.state() == CacheState::Stale)
                .count(),
            manifests_expired: manifests
                .values()
                .filter(|cv| cv.state() == CacheState::Expired)
                .count(),
            routing_tables: routing.len(),
            placement_tables: placement.len(),
        }
    }

    /// Check if the agent is running.
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Scan for stale entries and trigger refresh.
    pub fn scan_for_refresh(&self) -> Vec<String> {
        let manifests = self.manifests.read().unwrap();
        manifests
            .iter()
            .filter(|(_, cv)| cv.needs_refresh())
            .map(|(id, _)| id.clone())
            .collect()
    }
}

impl Default for CpCacheAgent {
    fn default() -> Self {
        Self::new(CpCacheConfig::default())
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of cached tenants.
    pub tenant_count: usize,
    /// Overall cache state.
    pub overall_state: CacheState,
    /// Number of fresh manifests.
    pub manifests_fresh: usize,
    /// Number of cached manifests.
    pub manifests_cached: usize,
    /// Number of stale manifests.
    pub manifests_stale: usize,
    /// Number of expired manifests.
    pub manifests_expired: usize,
    /// Number of cached routing tables.
    pub routing_tables: usize,
    /// Number of cached placement tables.
    pub placement_tables: usize,
}

/// Manifest change event for watch subscribers.
#[derive(Debug, Clone)]
pub enum ManifestChange {
    /// A new manifest was added.
    Added(Box<TenantManifest>),
    /// An existing manifest was updated.
    Updated {
        old: Box<TenantManifest>,
        new: Box<TenantManifest>,
    },
    /// A manifest was removed.
    Removed(String),
}

/// Type alias for manifest change subscriber callbacks.
type ManifestChangeSubscriber = Box<dyn Fn(ManifestChange) + Send + Sync>;

/// Manifest watcher that subscribes to CP-Raft manifest changes.
///
/// This watcher monitors the cache agent for manifest updates and
/// propagates changes to registered subscribers.
pub struct ManifestWatcher {
    /// Cache agent to watch.
    cache_agent: Arc<CpCacheAgent>,

    /// Known manifest versions (tenant_id -> updated_at_ms).
    known_versions: std::sync::RwLock<std::collections::HashMap<String, u64>>,

    /// Change subscribers (callbacks).
    subscribers: std::sync::RwLock<Vec<ManifestChangeSubscriber>>,
}

impl ManifestWatcher {
    /// Create a new manifest watcher.
    pub fn new(cache_agent: Arc<CpCacheAgent>) -> Self {
        Self {
            cache_agent,
            known_versions: std::sync::RwLock::new(std::collections::HashMap::new()),
            subscribers: std::sync::RwLock::new(Vec::new()),
        }
    }

    /// Subscribe to manifest changes.
    pub fn subscribe<F>(&self, callback: F)
    where
        F: Fn(ManifestChange) + Send + Sync + 'static,
    {
        self.subscribers.write().unwrap().push(Box::new(callback));
    }

    /// Check for changes and notify subscribers.
    ///
    /// This should be called periodically or when the cache is updated.
    pub fn check_for_changes(&self) -> Vec<ManifestChange> {
        let mut changes = Vec::new();
        let mut known = self.known_versions.write().unwrap();

        // Get current tenant list
        let current_tenants: std::collections::HashSet<String> = self
            .cache_agent
            .manifests
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect();

        // Check for added/updated manifests
        let manifests = self.cache_agent.manifests.read().unwrap();
        for (tenant_id, cv) in manifests.iter() {
            let manifest = &cv.value;
            match known.get(tenant_id) {
                None => {
                    // New manifest
                    known.insert(tenant_id.clone(), manifest.updated_at_ms);
                    changes.push(ManifestChange::Added(Box::new(manifest.clone())));
                }
                Some(&version) if version != manifest.updated_at_ms => {
                    // Updated manifest - get old version
                    if let Some(old_cv) = manifests.get(tenant_id) {
                        known.insert(tenant_id.clone(), manifest.updated_at_ms);
                        changes.push(ManifestChange::Updated {
                            old: Box::new(old_cv.value.clone()),
                            new: Box::new(manifest.clone()),
                        });
                    }
                }
                _ => {}
            }
        }
        drop(manifests);

        // Check for removed manifests
        let known_tenants: Vec<String> = known.keys().cloned().collect();
        for tenant_id in known_tenants {
            if !current_tenants.contains(&tenant_id) {
                known.remove(&tenant_id);
                changes.push(ManifestChange::Removed(tenant_id));
            }
        }

        // Notify subscribers
        let subscribers = self.subscribers.read().unwrap();
        for change in &changes {
            for subscriber in subscribers.iter() {
                subscriber(change.clone());
            }
        }

        changes
    }

    /// Get the number of known tenants.
    pub fn known_tenant_count(&self) -> usize {
        self.known_versions.read().unwrap().len()
    }

    /// Clear known versions (e.g., on reconnect).
    pub fn reset(&self) {
        self.known_versions.write().unwrap().clear();
    }
}
