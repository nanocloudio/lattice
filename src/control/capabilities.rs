//! Adapter capability registry.
//!
//! Per-tenant adapter enablement, command allowlists, and feature gates
//! are stored in CP-Raft and cached locally per §12.1.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Adapter types supported by Lattice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AdapterType {
    /// etcd v3 gRPC adapter.
    Etcd,
    /// Redis RESP2/RESP3 adapter.
    Redis,
    /// Memcached ASCII/binary adapter.
    Memcached,
}

impl std::fmt::Display for AdapterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Etcd => write!(f, "etcd"),
            Self::Redis => write!(f, "redis"),
            Self::Memcached => write!(f, "memcached"),
        }
    }
}

/// Feature gates that can be enabled per tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeatureGate {
    /// RESP3 protocol support for Redis adapter.
    Resp3,
    /// Binary protocol support for Memcached adapter.
    MemcachedBinary,
    /// Multi-KPG range fanout for serializable reads.
    MultiKpgFanout,
    /// Scripting support (future).
    Scripting,
    /// Snapshot manifest time fences.
    SnapshotManifestTicks,
}

/// Tenant capabilities from CP-Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantCapabilities {
    /// Tenant identifier.
    pub tenant_id: String,

    /// Enabled adapters.
    pub enabled_adapters: HashSet<AdapterType>,

    /// Enabled feature gates.
    pub feature_gates: HashSet<FeatureGate>,

    /// Command allowlists per adapter (empty means all allowed).
    pub command_allowlists: CommandAllowlists,

    /// TTL bounds in milliseconds.
    pub ttl_bounds: TtlBounds,
}

/// Command allowlists per adapter.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommandAllowlists {
    /// Allowed etcd operations (empty means all allowed).
    pub etcd: HashSet<String>,

    /// Allowed Redis commands (empty means all allowed).
    pub redis: HashSet<String>,

    /// Allowed Memcached commands (empty means all allowed).
    pub memcached: HashSet<String>,
}

/// TTL bounds for the tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlBounds {
    /// Minimum TTL in milliseconds.
    pub min_ms: u64,
    /// Maximum TTL in milliseconds.
    pub max_ms: u64,
    /// Default TTL in milliseconds.
    pub default_ms: u64,
}

impl Default for TtlBounds {
    fn default() -> Self {
        Self {
            min_ms: 0,
            max_ms: 600_000,    // 10 minutes
            default_ms: 60_000, // 1 minute
        }
    }
}

impl TenantCapabilities {
    /// Create new tenant capabilities with defaults.
    pub fn new(tenant_id: impl Into<String>) -> Self {
        let mut enabled_adapters = HashSet::new();
        enabled_adapters.insert(AdapterType::Etcd);

        Self {
            tenant_id: tenant_id.into(),
            enabled_adapters,
            feature_gates: HashSet::new(),
            command_allowlists: CommandAllowlists::default(),
            ttl_bounds: TtlBounds::default(),
        }
    }

    /// Check if an adapter is enabled for this tenant.
    pub fn is_adapter_enabled(&self, adapter: AdapterType) -> bool {
        self.enabled_adapters.contains(&adapter)
    }

    /// Check if a feature gate is enabled.
    pub fn is_feature_enabled(&self, feature: FeatureGate) -> bool {
        self.feature_gates.contains(&feature)
    }

    /// Check if a command is allowed for an adapter.
    ///
    /// Returns true if the allowlist is empty (all allowed) or if the
    /// command is in the allowlist.
    pub fn is_command_allowed(&self, adapter: AdapterType, command: &str) -> bool {
        let allowlist = match adapter {
            AdapterType::Etcd => &self.command_allowlists.etcd,
            AdapterType::Redis => &self.command_allowlists.redis,
            AdapterType::Memcached => &self.command_allowlists.memcached,
        };

        allowlist.is_empty() || allowlist.contains(command)
    }

    /// Enable an adapter.
    pub fn enable_adapter(&mut self, adapter: AdapterType) {
        self.enabled_adapters.insert(adapter);
    }

    /// Disable an adapter.
    pub fn disable_adapter(&mut self, adapter: AdapterType) {
        self.enabled_adapters.remove(&adapter);
    }

    /// Enable a feature gate.
    pub fn enable_feature(&mut self, feature: FeatureGate) {
        self.feature_gates.insert(feature);
    }

    /// Disable a feature gate.
    pub fn disable_feature(&mut self, feature: FeatureGate) {
        self.feature_gates.remove(&feature);
    }

    /// Set command allowlist for an adapter.
    pub fn set_command_allowlist(&mut self, adapter: AdapterType, commands: HashSet<String>) {
        match adapter {
            AdapterType::Etcd => self.command_allowlists.etcd = commands,
            AdapterType::Redis => self.command_allowlists.redis = commands,
            AdapterType::Memcached => self.command_allowlists.memcached = commands,
        }
    }

    /// Clamp a TTL to the tenant's bounds.
    pub fn clamp_ttl(&self, ttl_ms: u64) -> u64 {
        ttl_ms.clamp(self.ttl_bounds.min_ms, self.ttl_bounds.max_ms)
    }

    /// Check if a TTL is within bounds.
    pub fn is_ttl_valid(&self, ttl_ms: u64) -> bool {
        ttl_ms >= self.ttl_bounds.min_ms && ttl_ms <= self.ttl_bounds.max_ms
    }
}

/// Capability registry that caches tenant capabilities from CP-Raft.
///
/// This registry maintains per-tenant capabilities and provides
/// fast lookup for adapter, command, and feature gate checks.
pub struct CapabilityRegistry {
    /// Cached capabilities per tenant.
    capabilities: std::sync::RwLock<std::collections::HashMap<String, TenantCapabilities>>,
}

impl CapabilityRegistry {
    /// Create a new capability registry.
    pub fn new() -> Self {
        Self {
            capabilities: std::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get capabilities for a tenant.
    pub fn get(&self, tenant_id: &str) -> Option<TenantCapabilities> {
        self.capabilities.read().unwrap().get(tenant_id).cloned()
    }

    /// Update capabilities for a tenant.
    pub fn update(&self, capabilities: TenantCapabilities) {
        self.capabilities
            .write()
            .unwrap()
            .insert(capabilities.tenant_id.clone(), capabilities);
    }

    /// Remove capabilities for a tenant.
    pub fn remove(&self, tenant_id: &str) {
        self.capabilities.write().unwrap().remove(tenant_id);
    }

    /// Check if an adapter is enabled for a tenant.
    pub fn is_adapter_enabled(&self, tenant_id: &str, adapter: AdapterType) -> bool {
        self.get(tenant_id)
            .is_some_and(|c| c.is_adapter_enabled(adapter))
    }

    /// Check if a command is allowed for a tenant and adapter.
    pub fn is_command_allowed(&self, tenant_id: &str, adapter: AdapterType, command: &str) -> bool {
        self.get(tenant_id)
            .is_some_and(|c| c.is_command_allowed(adapter, command))
    }

    /// Check if a feature is enabled for a tenant.
    pub fn is_feature_enabled(&self, tenant_id: &str, feature: FeatureGate) -> bool {
        self.get(tenant_id)
            .is_some_and(|c| c.is_feature_enabled(feature))
    }

    /// Get all tenant IDs in the registry.
    pub fn tenant_ids(&self) -> Vec<String> {
        self.capabilities.read().unwrap().keys().cloned().collect()
    }

    /// Get count of registered tenants.
    pub fn tenant_count(&self) -> usize {
        self.capabilities.read().unwrap().len()
    }
}

impl Default for CapabilityRegistry {
    fn default() -> Self {
        Self::new()
    }
}
