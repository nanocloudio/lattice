//! Configuration parsing and validation.
//!
//! Lattice configuration is loaded from TOML files with CLI overrides.
//! Configuration sections mirror the specification's architectural components.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Top-level Lattice configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Control plane configuration.
    pub control_plane: ControlPlaneConfig,

    /// Listener configuration for protocol adapters.
    pub listeners: ListenerConfig,

    /// Durability and replication configuration.
    pub durability: DurabilityConfig,

    /// Telemetry and observability configuration.
    #[serde(default)]
    pub telemetry: TelemetryConfig,

    /// Tenant defaults and constraints.
    #[serde(default)]
    pub tenants: TenantsConfig,

    /// Filesystem paths.
    #[serde(default)]
    pub paths: PathConfig,

    /// Disaster recovery configuration.
    #[serde(default)]
    pub dr: DrConfig,
}

/// Control plane (CP-Raft) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlPlaneConfig {
    /// Mode: "embedded" or "external".
    #[serde(default = "default_cp_mode")]
    pub mode: String,

    /// CP-Raft HTTP endpoints for external mode.
    #[serde(default)]
    pub endpoints: Vec<String>,

    /// Cache TTL in seconds.
    #[serde(default = "default_cache_ttl_seconds")]
    pub cache_ttl_seconds: u64,

    /// Embedded CP HTTP bind address.
    #[serde(default = "default_embedded_http_bind")]
    pub embedded_http_bind: String,

    /// Embedded CP Raft bind address.
    #[serde(default = "default_embedded_raft_bind")]
    pub embedded_raft_bind: String,

    /// Bootstrap tenants for embedded mode.
    #[serde(default)]
    pub bootstrap_tenants: Vec<BootstrapTenant>,
}

/// Bootstrap tenant definition for embedded CP mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapTenant {
    pub tenant_id: String,
    pub tenant_kpg_count: u32,
    #[serde(default)]
    pub adapters: Vec<String>,
}

/// Listener configuration for protocol adapters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListenerConfig {
    /// gRPC listener for etcd v3 adapter.
    #[serde(default)]
    pub grpc: Option<GrpcListenerConfig>,

    /// TCP listener for Redis adapter (future).
    #[serde(default)]
    pub redis: Option<TcpListenerConfig>,

    /// TCP listener for Memcached adapter (future).
    #[serde(default)]
    pub memcached: Option<TcpListenerConfig>,
}

/// gRPC listener configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcListenerConfig {
    /// Bind address (e.g., "0.0.0.0:2379").
    pub bind: String,

    /// TLS certificate chain path.
    pub tls_chain_path: Option<String>,

    /// TLS private key path.
    pub tls_key_path: Option<String>,

    /// Client CA path for mTLS.
    pub client_ca_path: Option<String>,

    /// Default tenant for unauthenticated requests.
    #[serde(default)]
    pub default_tenant: Option<String>,

    /// Allow insecure (plaintext) connections for development/testing.
    #[serde(default)]
    pub insecure: bool,
}

/// TCP listener configuration (for future Redis/Memcached adapters).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpListenerConfig {
    /// Bind address.
    pub bind: String,

    /// TLS certificate chain path.
    pub tls_chain_path: Option<String>,

    /// TLS private key path.
    pub tls_key_path: Option<String>,

    /// Client CA path for mTLS.
    pub client_ca_path: Option<String>,

    /// Default tenant for unauthenticated requests.
    #[serde(default)]
    pub default_tenant: Option<String>,
}

/// Durability and replication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurabilityConfig {
    /// Durability mode: "strict" or "group-fsync".
    #[serde(default = "default_durability_mode")]
    pub durability_mode: String,

    /// Commit visibility: "durable_only" or "committed".
    #[serde(default = "default_commit_visibility")]
    pub commit_visibility: String,

    /// Quorum size for writes.
    #[serde(default = "default_quorum_size")]
    pub quorum_size: u32,

    /// This replica's identifier.
    #[serde(default = "default_replica_id")]
    pub replica_id: String,

    /// Peer replica addresses.
    #[serde(default)]
    pub peer_replicas: Vec<String>,
}

/// Telemetry and observability configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Metrics HTTP bind address.
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: String,

    /// Log level: "trace", "debug", "info", "warn", "error".
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            metrics_bind: default_metrics_bind(),
            log_level: default_log_level(),
        }
    }
}

/// Tenant defaults and constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantsConfig {
    /// Default KPG count per tenant.
    #[serde(default = "default_tenant_kpg_count")]
    pub tenant_kpg_count: u32,

    /// Default lease TTL in milliseconds.
    #[serde(default = "default_lease_ttl_default_ms")]
    pub lease_ttl_default_ms: u64,

    /// Maximum lease TTL in milliseconds.
    #[serde(default = "default_lease_ttl_max_ms")]
    pub lease_ttl_max_ms: u64,

    /// Watch idle timeout in milliseconds.
    #[serde(default = "default_watch_idle_timeout_ms")]
    pub watch_idle_timeout_ms: u64,

    /// Expiry scan period in milliseconds (deterministic tick interval).
    #[serde(default = "default_expiry_scan_period_ms")]
    pub expiry_scan_period_ms: u64,

    /// Auth cache TTL in milliseconds.
    #[serde(default = "default_auth_cache_ttl_ms")]
    pub auth_cache_ttl_ms: u64,
}

impl Default for TenantsConfig {
    fn default() -> Self {
        Self {
            tenant_kpg_count: default_tenant_kpg_count(),
            lease_ttl_default_ms: default_lease_ttl_default_ms(),
            lease_ttl_max_ms: default_lease_ttl_max_ms(),
            watch_idle_timeout_ms: default_watch_idle_timeout_ms(),
            expiry_scan_period_ms: default_expiry_scan_period_ms(),
            auth_cache_ttl_ms: default_auth_cache_ttl_ms(),
        }
    }
}

/// Filesystem path configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathConfig {
    /// Storage directory for WAL and snapshots.
    #[serde(default = "default_storage_dir")]
    pub storage_dir: String,

    /// CP certificate path (optional).
    pub cp_certs: Option<String>,

    /// Telemetry catalog path (optional).
    pub telemetry_catalog: Option<String>,
}

impl Default for PathConfig {
    fn default() -> Self {
        Self {
            storage_dir: default_storage_dir(),
            cp_certs: None,
            telemetry_catalog: None,
        }
    }
}

/// Disaster recovery configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DrConfig {
    /// Enable periodic snapshot export.
    #[serde(default)]
    pub export_enabled: bool,

    /// Export interval in seconds.
    #[serde(default)]
    pub export_interval_seconds: Option<u64>,

    /// Export destination path.
    #[serde(default)]
    pub export_path: Option<String>,
}

// Default value functions

fn default_cp_mode() -> String {
    "embedded".to_string()
}

fn default_cache_ttl_seconds() -> u64 {
    5
}

fn default_embedded_http_bind() -> String {
    "127.0.0.1:19000".to_string()
}

fn default_embedded_raft_bind() -> String {
    "127.0.0.1:19001".to_string()
}

fn default_durability_mode() -> String {
    "strict".to_string()
}

fn default_commit_visibility() -> String {
    "durable_only".to_string()
}

fn default_quorum_size() -> u32 {
    1
}

fn default_replica_id() -> String {
    "local".to_string()
}

fn default_metrics_bind() -> String {
    "0.0.0.0:9090".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_tenant_kpg_count() -> u32 {
    1
}

fn default_lease_ttl_default_ms() -> u64 {
    60_000
}

fn default_lease_ttl_max_ms() -> u64 {
    600_000
}

fn default_watch_idle_timeout_ms() -> u64 {
    300_000
}

fn default_expiry_scan_period_ms() -> u64 {
    1_000
}

fn default_auth_cache_ttl_ms() -> u64 {
    30_000
}

fn default_storage_dir() -> String {
    "data".to_string()
}

impl Config {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        let config: Config =
            toml::from_str(&content).with_context(|| "failed to parse config file")?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a TOML string.
    pub fn from_toml(content: &str) -> Result<Self> {
        let config: Config = toml::from_str(content).with_context(|| "failed to parse config")?;
        config.validate()?;
        Ok(config)
    }

    /// Apply CLI overrides to the configuration.
    pub fn apply_overrides(&mut self, overrides: &ConfigOverrides) {
        if let Some(ref log_level) = overrides.log_level {
            self.telemetry.log_level = log_level.clone();
        }
        if let Some(ref storage_dir) = overrides.storage_dir {
            self.paths.storage_dir = storage_dir.clone();
        }
        if let Some(ref metrics_bind) = overrides.metrics_bind {
            self.telemetry.metrics_bind = metrics_bind.clone();
        }
        if let Some(ref grpc_bind) = overrides.grpc_bind {
            if let Some(ref mut grpc) = self.listeners.grpc {
                grpc.bind = grpc_bind.clone();
            } else {
                self.listeners.grpc = Some(GrpcListenerConfig {
                    bind: grpc_bind.clone(),
                    tls_chain_path: None,
                    tls_key_path: None,
                    client_ca_path: None,
                    default_tenant: None,
                    insecure: false,
                });
            }
        }
    }

    /// Validate configuration consistency.
    pub fn validate(&self) -> Result<()> {
        self.validate_control_plane()?;
        self.validate_durability()?;
        self.validate_telemetry()?;
        self.validate_tenants()?;
        self.validate_listeners()?;
        Ok(())
    }

    fn validate_control_plane(&self) -> Result<()> {
        // Validate control plane mode
        if self.control_plane.mode != "embedded" && self.control_plane.mode != "external" {
            anyhow::bail!(
                "control_plane.mode must be 'embedded' or 'external', got: {}",
                self.control_plane.mode
            );
        }

        // External mode requires endpoints
        if self.control_plane.mode == "external" && self.control_plane.endpoints.is_empty() {
            anyhow::bail!("control_plane.endpoints required for external mode");
        }

        // Validate cache TTL bounds
        if self.control_plane.cache_ttl_seconds == 0 {
            anyhow::bail!("control_plane.cache_ttl_seconds must be > 0");
        }

        Ok(())
    }

    fn validate_durability(&self) -> Result<()> {
        // Validate durability mode
        if self.durability.durability_mode != "strict"
            && self.durability.durability_mode != "group-fsync"
        {
            anyhow::bail!(
                "durability.durability_mode must be 'strict' or 'group-fsync', got: {}",
                self.durability.durability_mode
            );
        }

        // Validate commit visibility
        if self.durability.commit_visibility != "durable_only"
            && self.durability.commit_visibility != "committed"
        {
            anyhow::bail!(
                "durability.commit_visibility must be 'durable_only' or 'committed', got: {}",
                self.durability.commit_visibility
            );
        }

        // Validate quorum size
        if self.durability.quorum_size == 0 {
            anyhow::bail!("durability.quorum_size must be > 0");
        }

        Ok(())
    }

    fn validate_telemetry(&self) -> Result<()> {
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.telemetry.log_level.as_str()) {
            anyhow::bail!(
                "telemetry.log_level must be one of {:?}, got: {}",
                valid_levels,
                self.telemetry.log_level
            );
        }
        Ok(())
    }

    fn validate_tenants(&self) -> Result<()> {
        // Validate timer constraints from specification
        if self.tenants.lease_ttl_default_ms > self.tenants.lease_ttl_max_ms {
            anyhow::bail!(
                "lease_ttl_default_ms ({}) cannot exceed lease_ttl_max_ms ({})",
                self.tenants.lease_ttl_default_ms,
                self.tenants.lease_ttl_max_ms
            );
        }

        // Validate KPG count
        if self.tenants.tenant_kpg_count == 0 {
            anyhow::bail!("tenants.tenant_kpg_count must be > 0");
        }

        Ok(())
    }

    fn validate_listeners(&self) -> Result<()> {
        // Validate gRPC listener TLS configuration
        if let Some(ref grpc) = self.listeners.grpc {
            self.validate_tls_config(
                "listeners.grpc",
                &grpc.tls_chain_path,
                &grpc.tls_key_path,
                &grpc.client_ca_path,
            )?;
        }

        // Validate Redis listener TLS configuration (future)
        if let Some(ref redis) = self.listeners.redis {
            self.validate_tls_config(
                "listeners.redis",
                &redis.tls_chain_path,
                &redis.tls_key_path,
                &redis.client_ca_path,
            )?;
        }

        // Validate Memcached listener TLS configuration (future)
        if let Some(ref memcached) = self.listeners.memcached {
            self.validate_tls_config(
                "listeners.memcached",
                &memcached.tls_chain_path,
                &memcached.tls_key_path,
                &memcached.client_ca_path,
            )?;
        }

        Ok(())
    }

    fn validate_tls_config(
        &self,
        prefix: &str,
        chain_path: &Option<String>,
        key_path: &Option<String>,
        _client_ca_path: &Option<String>,
    ) -> Result<()> {
        // If either chain or key is specified, both must be specified
        match (chain_path, key_path) {
            (Some(_), None) => {
                anyhow::bail!(
                    "{}.tls_key_path required when tls_chain_path is set",
                    prefix
                );
            }
            (None, Some(_)) => {
                anyhow::bail!(
                    "{}.tls_chain_path required when tls_key_path is set",
                    prefix
                );
            }
            (Some(chain), Some(key)) => {
                // Validate paths exist (only in non-test environments)
                #[cfg(not(test))]
                {
                    if !std::path::Path::new(chain).exists() {
                        anyhow::bail!("{}.tls_chain_path does not exist: {}", prefix, chain);
                    }
                    if !std::path::Path::new(key).exists() {
                        anyhow::bail!("{}.tls_key_path does not exist: {}", prefix, key);
                    }
                }
                let _ = (chain, key); // suppress unused warning in test builds
            }
            (None, None) => {
                // No TLS configured, which is valid for development
            }
        }
        Ok(())
    }
}

/// CLI override options that can be applied to configuration.
#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    /// Override log level.
    pub log_level: Option<String>,
    /// Override storage directory.
    pub storage_dir: Option<String>,
    /// Override metrics bind address.
    pub metrics_bind: Option<String>,
    /// Override gRPC bind address.
    pub grpc_bind: Option<String>,
}
