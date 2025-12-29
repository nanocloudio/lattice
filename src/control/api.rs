//! CP-Raft API types.
//!
//! These types represent the data stored in CP-Raft (ControlPlaneRaft):
//! - Tenant manifests with KPG configuration
//! - Adapter policies with command allowlists
//! - Quota configuration with token buckets
//!
//! CP-Raft is the source of truth for tenant configuration, and these
//! types are cached locally by the CpCacheAgent.

use crate::control::capabilities::{AdapterType, FeatureGate, TtlBounds};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Tenant manifest stored in CP-Raft.
///
/// This is the authoritative definition of a tenant's configuration,
/// including KPG count, routing epoch, adapter enablement, and quotas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantManifest {
    /// Unique tenant identifier.
    pub tenant_id: String,

    /// Number of KPGs for this tenant.
    pub kpg_count: u32,

    /// Current routing epoch (incremented on rebalance).
    pub routing_epoch: u64,

    /// Hash seed version for routing stability.
    pub hash_seed_version: u64,

    /// Enabled adapters for this tenant.
    pub adapter_enablement: HashSet<AdapterType>,

    /// Adapter-specific policies.
    pub adapter_policies: HashMap<AdapterType, AdapterPolicy>,

    /// Quota configuration.
    pub quota: QuotaConfig,

    /// TTL bounds for leases.
    pub ttl_bounds: TtlBounds,

    /// Enabled feature gates.
    pub feature_gates: HashSet<FeatureGate>,

    /// Tenant creation timestamp (ms since epoch).
    pub created_at_ms: u64,

    /// Last update timestamp (ms since epoch).
    pub updated_at_ms: u64,

    /// Whether the tenant is active.
    pub active: bool,
}

impl TenantManifest {
    /// Create a new tenant manifest with defaults.
    pub fn new(tenant_id: impl Into<String>, kpg_count: u32) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut adapter_enablement = HashSet::new();
        adapter_enablement.insert(AdapterType::Etcd);

        Self {
            tenant_id: tenant_id.into(),
            kpg_count,
            routing_epoch: 1,
            hash_seed_version: 1,
            adapter_enablement,
            adapter_policies: HashMap::new(),
            quota: QuotaConfig::default(),
            ttl_bounds: TtlBounds::default(),
            feature_gates: HashSet::new(),
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            active: true,
        }
    }

    /// Check if an adapter is enabled.
    pub fn is_adapter_enabled(&self, adapter: AdapterType) -> bool {
        self.adapter_enablement.contains(&adapter)
    }

    /// Get the policy for an adapter.
    pub fn get_adapter_policy(&self, adapter: AdapterType) -> Option<&AdapterPolicy> {
        self.adapter_policies.get(&adapter)
    }

    /// Check if a feature gate is enabled.
    pub fn is_feature_enabled(&self, feature: FeatureGate) -> bool {
        self.feature_gates.contains(&feature)
    }

    /// Enable an adapter.
    pub fn enable_adapter(&mut self, adapter: AdapterType) {
        self.adapter_enablement.insert(adapter);
        self.touch();
    }

    /// Disable an adapter.
    pub fn disable_adapter(&mut self, adapter: AdapterType) {
        self.adapter_enablement.remove(&adapter);
        self.touch();
    }

    /// Set adapter policy.
    pub fn set_adapter_policy(&mut self, adapter: AdapterType, policy: AdapterPolicy) {
        self.adapter_policies.insert(adapter, policy);
        self.touch();
    }

    /// Update timestamp.
    fn touch(&mut self) {
        self.updated_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }

    /// Increment the routing epoch (e.g., on rebalance).
    pub fn increment_routing_epoch(&mut self) {
        self.routing_epoch += 1;
        self.touch();
    }

    /// Increment the hash seed version (rare, for routing changes).
    pub fn increment_hash_seed_version(&mut self) {
        self.hash_seed_version += 1;
        self.touch();
    }
}

/// Adapter policy defining command restrictions and behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterPolicy {
    /// Allowed commands (empty means all allowed).
    pub command_allowlist: HashSet<String>,

    /// Denied commands (takes precedence over allowlist).
    pub command_denylist: HashSet<String>,

    /// Maximum key size in bytes.
    pub max_key_size: u32,

    /// Maximum value size in bytes.
    pub max_value_size: u32,

    /// Maximum keys per transaction.
    pub max_txn_keys: u32,

    /// Whether to allow range scans.
    pub allow_range_scans: bool,

    /// Whether to allow watch streams.
    pub allow_watches: bool,

    /// Maximum concurrent watches per connection.
    pub max_watches_per_connection: u32,
}

impl Default for AdapterPolicy {
    fn default() -> Self {
        Self {
            command_allowlist: HashSet::new(),
            command_denylist: HashSet::new(),
            max_key_size: 8192,        // 8 KB
            max_value_size: 1_048_576, // 1 MB
            max_txn_keys: 128,
            allow_range_scans: true,
            allow_watches: true,
            max_watches_per_connection: 100,
        }
    }
}

impl AdapterPolicy {
    /// Check if a command is allowed.
    pub fn is_command_allowed(&self, command: &str) -> bool {
        // Denylist takes precedence
        if self.command_denylist.contains(command) {
            return false;
        }
        // Empty allowlist means all allowed
        self.command_allowlist.is_empty() || self.command_allowlist.contains(command)
    }

    /// Check if a key size is within limits.
    pub fn is_key_size_valid(&self, size: usize) -> bool {
        size <= self.max_key_size as usize
    }

    /// Check if a value size is within limits.
    pub fn is_value_size_valid(&self, size: usize) -> bool {
        size <= self.max_value_size as usize
    }
}

/// Quota configuration using token bucket semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Requests per second limit.
    pub requests_per_second: u64,

    /// Request burst capacity.
    pub request_burst: u64,

    /// Bytes per second limit.
    pub bytes_per_second: u64,

    /// Bytes burst capacity.
    pub bytes_burst: u64,

    /// Grace period before throttling (ms).
    pub overage_grace_ms: u64,

    /// Whether quota enforcement is enabled.
    pub enabled: bool,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            requests_per_second: 10_000,
            request_burst: 20_000,
            bytes_per_second: 100_000_000, // 100 MB/s
            bytes_burst: 200_000_000,      // 200 MB
            overage_grace_ms: 1000,
            enabled: true,
        }
    }
}

impl QuotaConfig {
    /// Create an unlimited quota (for development/testing).
    pub fn unlimited() -> Self {
        Self {
            requests_per_second: u64::MAX,
            request_burst: u64::MAX,
            bytes_per_second: u64::MAX,
            bytes_burst: u64::MAX,
            overage_grace_ms: 0,
            enabled: false,
        }
    }

    /// Create a quota with custom limits.
    pub fn with_limits(rps: u64, bps: u64) -> Self {
        Self {
            requests_per_second: rps,
            request_burst: rps * 2,
            bytes_per_second: bps,
            bytes_burst: bps * 2,
            ..Default::default()
        }
    }
}

/// Token bucket for rate limiting.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    /// Tokens per second refill rate.
    pub rate: u64,

    /// Maximum capacity (burst).
    pub capacity: u64,

    /// Current tokens.
    pub tokens: u64,

    /// Last refill timestamp (ms).
    pub last_refill_ms: u64,
}

impl TokenBucket {
    /// Create a new token bucket.
    pub fn new(rate: u64, capacity: u64) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            rate,
            capacity,
            tokens: capacity,
            last_refill_ms: now_ms,
        }
    }

    /// Create from quota config (for requests).
    pub fn from_quota_requests(quota: &QuotaConfig) -> Self {
        Self::new(quota.requests_per_second, quota.request_burst)
    }

    /// Create from quota config (for bytes).
    pub fn from_quota_bytes(quota: &QuotaConfig) -> Self {
        Self::new(quota.bytes_per_second, quota.bytes_burst)
    }

    /// Try to consume tokens.
    ///
    /// Returns true if tokens were available, false if rate limited.
    pub fn try_consume(&mut self, tokens: u64) -> bool {
        self.refill();

        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    /// Check if tokens are available without consuming.
    pub fn check(&mut self, tokens: u64) -> bool {
        self.refill();
        self.tokens >= tokens
    }

    /// Refill tokens based on elapsed time.
    pub fn refill(&mut self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let elapsed_ms = now_ms.saturating_sub(self.last_refill_ms);
        if elapsed_ms == 0 {
            return;
        }

        // Calculate tokens to add (rate is per second, elapsed is in ms)
        let tokens_to_add = (self.rate * elapsed_ms) / 1000;
        self.tokens = self.tokens.saturating_add(tokens_to_add).min(self.capacity);
        self.last_refill_ms = now_ms;
    }

    /// Get current fill percentage (0.0 to 1.0).
    pub fn fill_ratio(&mut self) -> f64 {
        self.refill();
        if self.capacity == 0 {
            1.0
        } else {
            self.tokens as f64 / self.capacity as f64
        }
    }
}

/// PKI material for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantPki {
    /// Tenant identifier.
    pub tenant_id: String,

    /// CA certificate chain (PEM encoded).
    pub ca_chain_pem: Option<String>,

    /// Allowed client certificate CNs.
    pub allowed_cns: HashSet<String>,

    /// Certificate expiry timestamp (ms).
    pub expires_at_ms: Option<u64>,
}

impl TenantPki {
    /// Create PKI config with no mTLS.
    pub fn no_mtls(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            ca_chain_pem: None,
            allowed_cns: HashSet::new(),
            expires_at_ms: None,
        }
    }

    /// Check if mTLS is required.
    pub fn requires_mtls(&self) -> bool {
        self.ca_chain_pem.is_some()
    }

    /// Check if a CN is allowed.
    pub fn is_cn_allowed(&self, cn: &str) -> bool {
        self.allowed_cns.is_empty() || self.allowed_cns.contains(cn)
    }
}

/// RBAC role for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantRole {
    /// Role name.
    pub name: String,

    /// Permissions (key patterns to operations).
    pub permissions: Vec<RolePermission>,
}

/// A single permission entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolePermission {
    /// Key prefix pattern.
    pub key_pattern: String,

    /// Whether the pattern is a prefix (otherwise exact match).
    pub is_prefix: bool,

    /// Allowed operations.
    pub operations: HashSet<PermissionOp>,
}

/// Permission operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PermissionOp {
    /// Read permission.
    Read,
    /// Write permission.
    Write,
    /// Delete permission.
    Delete,
    /// Watch permission.
    Watch,
    /// Lease management permission.
    Lease,
}

impl TenantRole {
    /// Create a new role with no permissions.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            permissions: Vec::new(),
        }
    }

    /// Add a permission to this role.
    pub fn add_permission(&mut self, permission: RolePermission) {
        self.permissions.push(permission);
    }

    /// Check if this role has permission for an operation on a key.
    pub fn has_permission(&self, key: &[u8], op: PermissionOp) -> bool {
        let key_str = String::from_utf8_lossy(key);
        for perm in &self.permissions {
            if perm.operations.contains(&op) {
                if perm.is_prefix {
                    if key_str.starts_with(&perm.key_pattern) {
                        return true;
                    }
                } else if key_str == perm.key_pattern {
                    return true;
                }
            }
        }
        false
    }
}

impl RolePermission {
    /// Create a new prefix permission.
    pub fn prefix(pattern: impl Into<String>, ops: impl IntoIterator<Item = PermissionOp>) -> Self {
        Self {
            key_pattern: pattern.into(),
            is_prefix: true,
            operations: ops.into_iter().collect(),
        }
    }

    /// Create a new exact-match permission.
    pub fn exact(key: impl Into<String>, ops: impl IntoIterator<Item = PermissionOp>) -> Self {
        Self {
            key_pattern: key.into(),
            is_prefix: false,
            operations: ops.into_iter().collect(),
        }
    }
}

/// Quota enforcer that tracks request and byte usage per tenant.
///
/// This enforcer uses token buckets from the manifest quota configuration
/// and applies the overage grace period before throttling.
pub struct QuotaEnforcer {
    /// Request token buckets per tenant.
    request_buckets: std::sync::RwLock<HashMap<String, TokenBucket>>,

    /// Byte token buckets per tenant.
    byte_buckets: std::sync::RwLock<HashMap<String, TokenBucket>>,

    /// Overage start times per tenant (ms since epoch).
    overage_starts: std::sync::RwLock<HashMap<String, u64>>,

    /// Grace period in milliseconds.
    overage_grace_ms: u64,
}

impl QuotaEnforcer {
    /// Create a new quota enforcer with the given grace period.
    pub fn new(overage_grace_ms: u64) -> Self {
        Self {
            request_buckets: std::sync::RwLock::new(HashMap::new()),
            byte_buckets: std::sync::RwLock::new(HashMap::new()),
            overage_starts: std::sync::RwLock::new(HashMap::new()),
            overage_grace_ms,
        }
    }

    /// Initialize quota for a tenant from their manifest.
    pub fn init_tenant(&self, tenant_id: &str, quota: &QuotaConfig) {
        if !quota.enabled {
            return;
        }

        let request_bucket = TokenBucket::from_quota_requests(quota);
        let byte_bucket = TokenBucket::from_quota_bytes(quota);

        self.request_buckets
            .write()
            .unwrap()
            .insert(tenant_id.to_string(), request_bucket);
        self.byte_buckets
            .write()
            .unwrap()
            .insert(tenant_id.to_string(), byte_bucket);
    }

    /// Remove quota tracking for a tenant.
    pub fn remove_tenant(&self, tenant_id: &str) {
        self.request_buckets.write().unwrap().remove(tenant_id);
        self.byte_buckets.write().unwrap().remove(tenant_id);
        self.overage_starts.write().unwrap().remove(tenant_id);
    }

    /// Try to consume quota for a request.
    ///
    /// Returns Ok(()) if allowed, Err with remaining grace time if in grace period,
    /// or panics if hard throttled (in practice, return an error type).
    pub fn try_consume(&self, tenant_id: &str, request_count: u64, byte_count: u64) -> QuotaResult {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Check request quota
        let request_ok = {
            let mut buckets = self.request_buckets.write().unwrap();
            if let Some(bucket) = buckets.get_mut(tenant_id) {
                bucket.try_consume(request_count)
            } else {
                true // No quota configured = unlimited
            }
        };

        // Check byte quota
        let byte_ok = {
            let mut buckets = self.byte_buckets.write().unwrap();
            if let Some(bucket) = buckets.get_mut(tenant_id) {
                bucket.try_consume(byte_count)
            } else {
                true // No quota configured = unlimited
            }
        };

        if request_ok && byte_ok {
            // Clear any overage state
            self.overage_starts.write().unwrap().remove(tenant_id);
            return QuotaResult::Allowed;
        }

        // Check grace period
        let mut overage_starts = self.overage_starts.write().unwrap();
        let overage_start = *overage_starts
            .entry(tenant_id.to_string())
            .or_insert(now_ms);

        let elapsed = now_ms.saturating_sub(overage_start);
        if elapsed < self.overage_grace_ms {
            QuotaResult::GracePeriod {
                remaining_ms: self.overage_grace_ms - elapsed,
            }
        } else {
            QuotaResult::Throttled
        }
    }

    /// Get quota statistics for a tenant.
    pub fn stats(&self, tenant_id: &str) -> Option<QuotaStats> {
        let request_buckets = self.request_buckets.read().unwrap();
        let byte_buckets = self.byte_buckets.read().unwrap();

        let request_bucket = request_buckets.get(tenant_id)?;
        let byte_bucket = byte_buckets.get(tenant_id)?;

        Some(QuotaStats {
            request_tokens: request_bucket.tokens,
            request_capacity: request_bucket.capacity,
            byte_tokens: byte_bucket.tokens,
            byte_capacity: byte_bucket.capacity,
        })
    }
}

impl Default for QuotaEnforcer {
    fn default() -> Self {
        Self::new(1000) // 1 second grace period
    }
}

/// Result of a quota check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaResult {
    /// Request is allowed.
    Allowed,
    /// Request is in grace period.
    GracePeriod { remaining_ms: u64 },
    /// Request is throttled.
    Throttled,
}

impl QuotaResult {
    /// Check if this result allows the request to proceed.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed | Self::GracePeriod { .. })
    }

    /// Check if this result throttles the request.
    pub fn is_throttled(&self) -> bool {
        matches!(self, Self::Throttled)
    }
}

/// Quota statistics for a tenant.
#[derive(Debug, Clone)]
pub struct QuotaStats {
    /// Current request tokens.
    pub request_tokens: u64,
    /// Request token capacity.
    pub request_capacity: u64,
    /// Current byte tokens.
    pub byte_tokens: u64,
    /// Byte token capacity.
    pub byte_capacity: u64,
}

/// PKI and RBAC cache for tenant authentication and authorization.
///
/// This cache stores PKI material and role definitions from CP-Raft
/// and provides fast lookup for authentication and permission checks.
pub struct PkiRbacCache {
    /// PKI material per tenant.
    pki: std::sync::RwLock<HashMap<String, TenantPki>>,

    /// Roles per tenant (role_name -> role).
    roles: std::sync::RwLock<HashMap<String, HashMap<String, TenantRole>>>,

    /// User-to-role bindings per tenant (user_id -> role_names).
    user_bindings: std::sync::RwLock<HashMap<String, HashMap<String, Vec<String>>>>,
}

impl PkiRbacCache {
    /// Create a new PKI/RBAC cache.
    pub fn new() -> Self {
        Self {
            pki: std::sync::RwLock::new(HashMap::new()),
            roles: std::sync::RwLock::new(HashMap::new()),
            user_bindings: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Update PKI material for a tenant.
    pub fn update_pki(&self, pki: TenantPki) {
        self.pki.write().unwrap().insert(pki.tenant_id.clone(), pki);
    }

    /// Get PKI material for a tenant.
    pub fn get_pki(&self, tenant_id: &str) -> Option<TenantPki> {
        self.pki.read().unwrap().get(tenant_id).cloned()
    }

    /// Remove PKI material for a tenant.
    pub fn remove_pki(&self, tenant_id: &str) {
        self.pki.write().unwrap().remove(tenant_id);
    }

    /// Update a role for a tenant.
    pub fn update_role(&self, tenant_id: &str, role: TenantRole) {
        let mut roles = self.roles.write().unwrap();
        roles
            .entry(tenant_id.to_string())
            .or_default()
            .insert(role.name.clone(), role);
    }

    /// Get a role for a tenant.
    pub fn get_role(&self, tenant_id: &str, role_name: &str) -> Option<TenantRole> {
        self.roles
            .read()
            .unwrap()
            .get(tenant_id)
            .and_then(|roles| roles.get(role_name).cloned())
    }

    /// Remove a role for a tenant.
    pub fn remove_role(&self, tenant_id: &str, role_name: &str) {
        if let Some(roles) = self.roles.write().unwrap().get_mut(tenant_id) {
            roles.remove(role_name);
        }
    }

    /// Bind a user to roles for a tenant.
    pub fn bind_user(&self, tenant_id: &str, user_id: &str, role_names: Vec<String>) {
        let mut bindings = self.user_bindings.write().unwrap();
        bindings
            .entry(tenant_id.to_string())
            .or_default()
            .insert(user_id.to_string(), role_names);
    }

    /// Get roles bound to a user for a tenant.
    pub fn get_user_roles(&self, tenant_id: &str, user_id: &str) -> Vec<String> {
        self.user_bindings
            .read()
            .unwrap()
            .get(tenant_id)
            .and_then(|bindings| bindings.get(user_id).cloned())
            .unwrap_or_default()
    }

    /// Check if a user has permission for an operation on a key.
    pub fn has_permission(
        &self,
        tenant_id: &str,
        user_id: &str,
        key: &[u8],
        op: PermissionOp,
    ) -> bool {
        let role_names = self.get_user_roles(tenant_id, user_id);
        let roles = self.roles.read().unwrap();

        if let Some(tenant_roles) = roles.get(tenant_id) {
            for role_name in role_names {
                if let Some(role) = tenant_roles.get(&role_name) {
                    if role.has_permission(key, op) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check mTLS authentication for a tenant.
    pub fn authenticate_mtls(&self, tenant_id: &str, cn: &str) -> bool {
        self.get_pki(tenant_id)
            .is_some_and(|pki| pki.is_cn_allowed(cn))
    }

    /// Remove all data for a tenant.
    pub fn remove_tenant(&self, tenant_id: &str) {
        self.pki.write().unwrap().remove(tenant_id);
        self.roles.write().unwrap().remove(tenant_id);
        self.user_bindings.write().unwrap().remove(tenant_id);
    }
}

impl Default for PkiRbacCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Break-glass override policy.
///
/// Break-glass overrides allow operators to bypass normal access controls
/// in emergency situations. All overrides are logged to an audit ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakGlassPolicy {
    /// Whether break-glass is enabled for the tenant.
    pub enabled: bool,

    /// Required approval count (e.g., 2 for two-person rule).
    pub required_approvals: u32,

    /// Maximum duration for an override in seconds.
    pub max_duration_seconds: u64,

    /// Notification endpoints for break-glass events.
    pub notification_endpoints: Vec<String>,
}

impl Default for BreakGlassPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            required_approvals: 2,
            max_duration_seconds: 3600, // 1 hour
            notification_endpoints: Vec::new(),
        }
    }
}

/// An active break-glass override.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakGlassOverride {
    /// Override identifier.
    pub override_id: String,

    /// Tenant this override applies to.
    pub tenant_id: String,

    /// User who initiated the override.
    pub initiated_by: String,

    /// Reason for the override.
    pub reason: String,

    /// When the override was initiated (ms since epoch).
    pub initiated_at_ms: u64,

    /// When the override expires (ms since epoch).
    pub expires_at_ms: u64,

    /// Approvers who have approved this override.
    pub approvers: Vec<String>,

    /// Whether the override is active.
    pub active: bool,
}

impl BreakGlassOverride {
    /// Create a new break-glass override request.
    pub fn new(
        tenant_id: impl Into<String>,
        initiated_by: impl Into<String>,
        reason: impl Into<String>,
        duration_seconds: u64,
    ) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            override_id: format!("bg-{}", now_ms),
            tenant_id: tenant_id.into(),
            initiated_by: initiated_by.into(),
            reason: reason.into(),
            initiated_at_ms: now_ms,
            expires_at_ms: now_ms + (duration_seconds * 1000),
            approvers: Vec::new(),
            active: false,
        }
    }

    /// Add an approval to this override.
    pub fn add_approval(&mut self, approver: impl Into<String>) {
        self.approvers.push(approver.into());
    }

    /// Check if this override has enough approvals.
    pub fn has_enough_approvals(&self, required: u32) -> bool {
        self.approvers.len() >= required as usize
    }

    /// Activate this override if it has enough approvals.
    pub fn try_activate(&mut self, required: u32) -> bool {
        if self.has_enough_approvals(required) {
            self.active = true;
            true
        } else {
            false
        }
    }

    /// Check if this override is expired.
    pub fn is_expired(&self) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now_ms > self.expires_at_ms
    }

    /// Check if this override is currently valid (active and not expired).
    pub fn is_valid(&self) -> bool {
        self.active && !self.is_expired()
    }

    /// Revoke this override.
    pub fn revoke(&mut self) {
        self.active = false;
    }
}

/// Break-glass override ledger.
///
/// Maintains active overrides and audit log entries.
pub struct BreakGlassLedger {
    /// Active overrides per tenant.
    active_overrides: std::sync::RwLock<HashMap<String, Vec<BreakGlassOverride>>>,

    /// Audit log entries.
    audit_log: std::sync::RwLock<Vec<BreakGlassAuditEntry>>,

    /// Break-glass policies per tenant.
    policies: std::sync::RwLock<HashMap<String, BreakGlassPolicy>>,
}

impl BreakGlassLedger {
    /// Create a new break-glass ledger.
    pub fn new() -> Self {
        Self {
            active_overrides: std::sync::RwLock::new(HashMap::new()),
            audit_log: std::sync::RwLock::new(Vec::new()),
            policies: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Set the break-glass policy for a tenant.
    pub fn set_policy(&self, tenant_id: &str, policy: BreakGlassPolicy) {
        self.policies
            .write()
            .unwrap()
            .insert(tenant_id.to_string(), policy);
    }

    /// Get the break-glass policy for a tenant.
    pub fn get_policy(&self, tenant_id: &str) -> Option<BreakGlassPolicy> {
        self.policies.read().unwrap().get(tenant_id).cloned()
    }

    /// Request a new break-glass override.
    pub fn request_override(&self, override_req: BreakGlassOverride) -> bool {
        let policy = match self.get_policy(&override_req.tenant_id) {
            Some(p) if p.enabled => p,
            _ => return false,
        };

        // Check max duration
        let duration_ms = override_req.expires_at_ms - override_req.initiated_at_ms;
        if duration_ms > policy.max_duration_seconds * 1000 {
            return false;
        }

        // Log the request
        self.log_event(BreakGlassAuditEntry {
            timestamp_ms: override_req.initiated_at_ms,
            tenant_id: override_req.tenant_id.clone(),
            override_id: override_req.override_id.clone(),
            event_type: BreakGlassEventType::Requested,
            actor: override_req.initiated_by.clone(),
            details: override_req.reason.clone(),
        });

        // Store the override
        self.active_overrides
            .write()
            .unwrap()
            .entry(override_req.tenant_id.clone())
            .or_default()
            .push(override_req);

        true
    }

    /// Approve a break-glass override.
    pub fn approve_override(&self, tenant_id: &str, override_id: &str, approver: &str) -> bool {
        let policy = match self.get_policy(tenant_id) {
            Some(p) => p,
            None => return false,
        };

        let mut overrides = self.active_overrides.write().unwrap();
        let Some(tenant_overrides) = overrides.get_mut(tenant_id) else {
            return false;
        };

        let Some(override_entry) = tenant_overrides
            .iter_mut()
            .find(|o| o.override_id == override_id)
        else {
            return false;
        };

        override_entry.add_approval(approver);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.log_event(BreakGlassAuditEntry {
            timestamp_ms: now_ms,
            tenant_id: tenant_id.to_string(),
            override_id: override_id.to_string(),
            event_type: BreakGlassEventType::Approved,
            actor: approver.to_string(),
            details: format!(
                "Approval {}/{}",
                override_entry.approvers.len(),
                policy.required_approvals
            ),
        });

        // Try to activate if enough approvals
        if override_entry.try_activate(policy.required_approvals) {
            self.log_event(BreakGlassAuditEntry {
                timestamp_ms: now_ms,
                tenant_id: tenant_id.to_string(),
                override_id: override_id.to_string(),
                event_type: BreakGlassEventType::Activated,
                actor: "system".to_string(),
                details: "Override activated after sufficient approvals".to_string(),
            });
        }

        true
    }

    /// Check if there is an active override for a tenant.
    pub fn has_active_override(&self, tenant_id: &str) -> bool {
        let overrides = self.active_overrides.read().unwrap();
        overrides
            .get(tenant_id)
            .is_some_and(|list| list.iter().any(|o| o.is_valid()))
    }

    /// Revoke all overrides for a tenant.
    pub fn revoke_all(&self, tenant_id: &str, actor: &str) {
        let mut overrides = self.active_overrides.write().unwrap();
        if let Some(tenant_overrides) = overrides.get_mut(tenant_id) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            for override_entry in tenant_overrides.iter_mut() {
                if override_entry.active {
                    override_entry.revoke();
                    self.log_event(BreakGlassAuditEntry {
                        timestamp_ms: now_ms,
                        tenant_id: tenant_id.to_string(),
                        override_id: override_entry.override_id.clone(),
                        event_type: BreakGlassEventType::Revoked,
                        actor: actor.to_string(),
                        details: "Manual revocation".to_string(),
                    });
                }
            }
        }
    }

    /// Clean up expired overrides.
    pub fn cleanup_expired(&self) {
        let mut overrides = self.active_overrides.write().unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for (tenant_id, tenant_overrides) in overrides.iter_mut() {
            for override_entry in tenant_overrides.iter_mut() {
                if override_entry.active && override_entry.is_expired() {
                    override_entry.active = false;
                    self.log_event(BreakGlassAuditEntry {
                        timestamp_ms: now_ms,
                        tenant_id: tenant_id.clone(),
                        override_id: override_entry.override_id.clone(),
                        event_type: BreakGlassEventType::Expired,
                        actor: "system".to_string(),
                        details: "Override expired".to_string(),
                    });
                }
            }
        }
    }

    /// Get recent audit log entries.
    pub fn get_audit_log(&self, limit: usize) -> Vec<BreakGlassAuditEntry> {
        let log = self.audit_log.read().unwrap();
        log.iter().rev().take(limit).cloned().collect()
    }

    /// Log an audit event.
    fn log_event(&self, entry: BreakGlassAuditEntry) {
        self.audit_log.write().unwrap().push(entry);
    }
}

impl Default for BreakGlassLedger {
    fn default() -> Self {
        Self::new()
    }
}

/// Break-glass audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakGlassAuditEntry {
    /// Timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Tenant ID.
    pub tenant_id: String,
    /// Override ID.
    pub override_id: String,
    /// Event type.
    pub event_type: BreakGlassEventType,
    /// Actor who triggered the event.
    pub actor: String,
    /// Event details.
    pub details: String,
}

/// Break-glass event types for audit logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BreakGlassEventType {
    /// Override was requested.
    Requested,
    /// Override was approved.
    Approved,
    /// Override was activated.
    Activated,
    /// Override was revoked.
    Revoked,
    /// Override expired.
    Expired,
    /// Override was used for an operation.
    Used,
}
