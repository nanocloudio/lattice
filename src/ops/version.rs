//! Version gates and compatibility checking.
//!
//! Per §14, version gates control feature availability and provide fault injection hooks.

use std::collections::HashMap;
use std::sync::RwLock;

/// Version information for the Lattice binary.
#[derive(Debug, Clone)]
pub struct VersionInfo {
    /// Major version.
    pub major: u32,
    /// Minor version.
    pub minor: u32,
    /// Patch version.
    pub patch: u32,
    /// Git commit hash (short).
    pub git_commit: Option<String>,
    /// Build timestamp.
    pub build_timestamp: Option<String>,
    /// Features enabled at compile time.
    pub features: Vec<String>,
}

impl VersionInfo {
    /// Create a new version info.
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            git_commit: None,
            build_timestamp: None,
            features: Vec::new(),
        }
    }

    /// Get the current Lattice version.
    pub fn current() -> Self {
        Self {
            major: 0,
            minor: 1,
            patch: 0,
            git_commit: option_env!("GIT_COMMIT").map(String::from),
            build_timestamp: option_env!("BUILD_TIMESTAMP").map(String::from),
            features: Self::enabled_features(),
        }
    }

    /// Get enabled feature flags.
    fn enabled_features() -> Vec<String> {
        let mut features = Vec::new();

        // Add features conditionally
        if cfg!(feature = "tcp-tls") {
            features.push("tcp-tls".to_string());
        }
        if cfg!(feature = "grpc") {
            features.push("grpc".to_string());
        }
        if cfg!(feature = "telemetry") {
            features.push("telemetry".to_string());
        }
        if cfg!(feature = "admin-http") {
            features.push("admin-http".to_string());
        }
        if cfg!(feature = "snapshot-crypto") {
            features.push("snapshot-crypto".to_string());
        }

        features
    }

    /// Get version as a semver string.
    pub fn semver(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }

    /// Get full version string with git commit.
    pub fn full_version(&self) -> String {
        match &self.git_commit {
            Some(commit) => format!("{}-{}", self.semver(), commit),
            None => self.semver(),
        }
    }

    /// Check if this version is compatible with another.
    pub fn is_compatible_with(&self, other: &VersionInfo) -> bool {
        // Major version must match, and our minor must be >= other's minor
        self.major == other.major && self.minor >= other.minor
    }

    /// Check if a feature is enabled.
    pub fn has_feature(&self, feature: &str) -> bool {
        self.features.iter().any(|f| f == feature)
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        Self::current()
    }
}

impl std::fmt::Display for VersionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.full_version())
    }
}

/// Feature gate for runtime feature control.
#[derive(Debug, Clone)]
pub struct FeatureGate {
    /// Gate name.
    pub name: String,
    /// Whether the gate is enabled.
    pub enabled: bool,
    /// Minimum version required.
    pub min_version: Option<(u32, u32)>,
    /// Description.
    pub description: String,
}

impl FeatureGate {
    /// Create a new feature gate.
    pub fn new(name: impl Into<String>, enabled: bool) -> Self {
        Self {
            name: name.into(),
            enabled,
            min_version: None,
            description: String::new(),
        }
    }

    /// Set minimum version requirement.
    pub fn with_min_version(mut self, major: u32, minor: u32) -> Self {
        self.min_version = Some((major, minor));
        self
    }

    /// Set description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Check if gate passes for the given version.
    pub fn passes(&self, version: &VersionInfo) -> bool {
        if !self.enabled {
            return false;
        }
        match self.min_version {
            Some((major, minor)) => version.major >= major && version.minor >= minor,
            None => true,
        }
    }
}

/// Version gate registry.
#[derive(Debug)]
pub struct VersionGateRegistry {
    /// Feature gates.
    gates: RwLock<HashMap<String, FeatureGate>>,
    /// Current version.
    version: VersionInfo,
    /// Fault injection hooks.
    fault_hooks: RwLock<HashMap<String, FaultHook>>,
}

impl VersionGateRegistry {
    /// Create a new version gate registry.
    pub fn new() -> Self {
        let mut registry = Self {
            gates: RwLock::new(HashMap::new()),
            version: VersionInfo::current(),
            fault_hooks: RwLock::new(HashMap::new()),
        };
        registry.register_default_gates();
        registry
    }

    /// Create with a specific version.
    pub fn with_version(version: VersionInfo) -> Self {
        let mut registry = Self {
            gates: RwLock::new(HashMap::new()),
            version,
            fault_hooks: RwLock::new(HashMap::new()),
        };
        registry.register_default_gates();
        registry
    }

    /// Register default feature gates.
    fn register_default_gates(&mut self) {
        let default_gates = vec![
            FeatureGate::new("etcd_adapter", true).with_description("etcd v3 gRPC adapter"),
            FeatureGate::new("redis_adapter", false)
                .with_min_version(0, 2)
                .with_description("Redis RESP adapter (future)"),
            FeatureGate::new("memcached_adapter", false)
                .with_min_version(0, 3)
                .with_description("Memcached adapter (future)"),
            FeatureGate::new("multi_kpg_txn", false)
                .with_min_version(0, 2)
                .with_description("Cross-KPG transactions"),
            FeatureGate::new("multi_kpg_linearizable_range", false)
                .with_min_version(0, 2)
                .with_description("Linearizable multi-KPG range queries"),
            FeatureGate::new("snapshot_crypto", true).with_description("Encrypted snapshots"),
            FeatureGate::new("follower_reads", true)
                .with_description("Snapshot-only reads from followers"),
            FeatureGate::new("audit_logging", true).with_description("Security audit logging"),
        ];

        let mut gates = self.gates.write().unwrap();
        for gate in default_gates {
            gates.insert(gate.name.clone(), gate);
        }
    }

    /// Check if a feature gate passes.
    pub fn check(&self, name: &str) -> bool {
        let gates = self.gates.read().unwrap();
        gates.get(name).is_some_and(|g| g.passes(&self.version))
    }

    /// Get a feature gate.
    pub fn get(&self, name: &str) -> Option<FeatureGate> {
        let gates = self.gates.read().unwrap();
        gates.get(name).cloned()
    }

    /// Register a custom feature gate.
    pub fn register(&self, gate: FeatureGate) {
        let mut gates = self.gates.write().unwrap();
        gates.insert(gate.name.clone(), gate);
    }

    /// Enable a feature gate.
    pub fn enable(&self, name: &str) {
        let mut gates = self.gates.write().unwrap();
        if let Some(gate) = gates.get_mut(name) {
            gate.enabled = true;
        }
    }

    /// Disable a feature gate.
    pub fn disable(&self, name: &str) {
        let mut gates = self.gates.write().unwrap();
        if let Some(gate) = gates.get_mut(name) {
            gate.enabled = false;
        }
    }

    /// Get all registered gates.
    pub fn all_gates(&self) -> Vec<FeatureGate> {
        let gates = self.gates.read().unwrap();
        gates.values().cloned().collect()
    }

    /// Get current version.
    pub fn version(&self) -> &VersionInfo {
        &self.version
    }

    // === Fault Injection ===

    /// Register a fault hook.
    pub fn register_fault_hook(&self, hook: FaultHook) {
        let mut hooks = self.fault_hooks.write().unwrap();
        hooks.insert(hook.name.clone(), hook);
    }

    /// Check if a fault should be injected.
    pub fn should_inject_fault(&self, name: &str) -> bool {
        let hooks = self.fault_hooks.read().unwrap();
        hooks.get(name).is_some_and(|h| h.should_trigger())
    }

    /// Get fault hook configuration.
    pub fn get_fault_hook(&self, name: &str) -> Option<FaultHook> {
        let hooks = self.fault_hooks.read().unwrap();
        hooks.get(name).cloned()
    }

    /// List all fault hooks.
    pub fn list_fault_hooks(&self) -> Vec<FaultHook> {
        let hooks = self.fault_hooks.read().unwrap();
        hooks.values().cloned().collect()
    }

    /// Clear all fault hooks.
    pub fn clear_fault_hooks(&self) {
        let mut hooks = self.fault_hooks.write().unwrap();
        hooks.clear();
    }
}

impl Default for VersionGateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Fault injection hook for testing.
#[derive(Debug, Clone)]
pub struct FaultHook {
    /// Hook name.
    pub name: String,
    /// Whether the hook is enabled.
    pub enabled: bool,
    /// Probability of triggering (0.0 - 1.0).
    pub probability: f64,
    /// Maximum number of triggers (None = unlimited).
    pub max_triggers: Option<u64>,
    /// Current trigger count.
    pub trigger_count: u64,
    /// Fault type.
    pub fault_type: FaultType,
}

impl FaultHook {
    /// Create a new fault hook.
    pub fn new(name: impl Into<String>, fault_type: FaultType) -> Self {
        Self {
            name: name.into(),
            enabled: false,
            probability: 1.0,
            max_triggers: None,
            trigger_count: 0,
            fault_type,
        }
    }

    /// Enable the hook.
    pub fn enable(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Set probability.
    pub fn with_probability(mut self, p: f64) -> Self {
        self.probability = p.clamp(0.0, 1.0);
        self
    }

    /// Set max triggers.
    pub fn with_max_triggers(mut self, n: u64) -> Self {
        self.max_triggers = Some(n);
        self
    }

    /// Check if the fault should trigger.
    pub fn should_trigger(&self) -> bool {
        if !self.enabled {
            return false;
        }
        if let Some(max) = self.max_triggers {
            if self.trigger_count >= max {
                return false;
            }
        }
        if self.probability >= 1.0 {
            return true;
        }
        // Simple random check
        rand_probability(self.probability)
    }

    /// Record a trigger.
    pub fn record_trigger(&mut self) {
        self.trigger_count += 1;
    }
}

/// Types of injectable faults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultType {
    /// Delay execution.
    Delay,
    /// Return error.
    Error,
    /// Panic.
    Panic,
    /// Drop request.
    Drop,
    /// Corrupt data.
    Corrupt,
    /// Timeout.
    Timeout,
}

impl std::fmt::Display for FaultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delay => write!(f, "delay"),
            Self::Error => write!(f, "error"),
            Self::Panic => write!(f, "panic"),
            Self::Drop => write!(f, "drop"),
            Self::Corrupt => write!(f, "corrupt"),
            Self::Timeout => write!(f, "timeout"),
        }
    }
}

/// Simple probability check using system time as pseudo-random source.
fn rand_probability(p: f64) -> bool {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let random = (nanos as f64) / (u32::MAX as f64);
    random < p
}

/// Compatibility check result.
#[derive(Debug, Clone)]
pub struct CompatibilityCheck {
    /// Whether versions are compatible.
    pub compatible: bool,
    /// Reason if not compatible.
    pub reason: Option<String>,
    /// Local version.
    pub local: VersionInfo,
    /// Remote version.
    pub remote: VersionInfo,
}

impl CompatibilityCheck {
    /// Perform compatibility check.
    pub fn check(local: &VersionInfo, remote: &VersionInfo) -> Self {
        let compatible = local.is_compatible_with(remote);
        let reason = if !compatible {
            Some(format!(
                "Version mismatch: local {} vs remote {}",
                local.semver(),
                remote.semver()
            ))
        } else {
            None
        };

        Self {
            compatible,
            reason,
            local: local.clone(),
            remote: remote.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_info_semver() {
        let v = VersionInfo::new(0, 1, 5);
        assert_eq!(v.semver(), "0.1.5");
    }

    #[test]
    fn test_version_info_full_version() {
        let mut v = VersionInfo::new(0, 1, 0);
        v.git_commit = Some("abc1234".to_string());
        assert_eq!(v.full_version(), "0.1.0-abc1234");
    }

    #[test]
    fn test_version_compatibility() {
        let v1 = VersionInfo::new(0, 1, 0);
        let v2 = VersionInfo::new(0, 1, 5);
        let v3 = VersionInfo::new(0, 2, 0);
        let v4 = VersionInfo::new(1, 0, 0);

        assert!(v2.is_compatible_with(&v1)); // Same major, higher minor
        assert!(!v1.is_compatible_with(&v3)); // Same major, lower minor
        assert!(!v1.is_compatible_with(&v4)); // Different major
    }

    #[test]
    fn test_feature_gate_passes() {
        let version = VersionInfo::new(0, 2, 0);

        let gate = FeatureGate::new("test", true).with_min_version(0, 1);
        assert!(gate.passes(&version));

        let gate2 = FeatureGate::new("test", true).with_min_version(0, 3);
        assert!(!gate2.passes(&version));

        let gate3 = FeatureGate::new("test", false);
        assert!(!gate3.passes(&version));
    }

    #[test]
    fn test_version_gate_registry_check() {
        let registry = VersionGateRegistry::new();

        assert!(registry.check("etcd_adapter"));
        assert!(!registry.check("redis_adapter")); // min_version 0.2
        assert!(!registry.check("nonexistent"));
    }

    #[test]
    fn test_version_gate_enable_disable() {
        let registry = VersionGateRegistry::new();

        registry.disable("etcd_adapter");
        assert!(!registry.check("etcd_adapter"));

        registry.enable("etcd_adapter");
        assert!(registry.check("etcd_adapter"));
    }

    #[test]
    fn test_fault_hook() {
        let hook = FaultHook::new("test_fault", FaultType::Error)
            .enable()
            .with_probability(1.0);

        assert!(hook.should_trigger());

        let disabled_hook = FaultHook::new("test_fault", FaultType::Error);
        assert!(!disabled_hook.should_trigger());
    }

    #[test]
    fn test_fault_hook_max_triggers() {
        let mut hook = FaultHook::new("test_fault", FaultType::Error)
            .enable()
            .with_max_triggers(2);

        assert!(hook.should_trigger());
        hook.record_trigger();
        assert!(hook.should_trigger());
        hook.record_trigger();
        assert!(!hook.should_trigger()); // Reached max
    }

    #[test]
    fn test_compatibility_check() {
        let local = VersionInfo::new(0, 2, 0);
        let remote = VersionInfo::new(0, 1, 0);

        let check = CompatibilityCheck::check(&local, &remote);
        assert!(check.compatible);

        let remote2 = VersionInfo::new(1, 0, 0);
        let check2 = CompatibilityCheck::check(&local, &remote2);
        assert!(!check2.compatible);
    }
}
