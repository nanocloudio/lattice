//! Security manager and credentials.
//!
//! Handles mTLS identity management and credential rotation per §10.
//!
//! # Features
//!
//! - MtlsIdentityManager for server and client identity management
//! - Credential rotation handling with graceful reload
//! - Certificate expiry monitoring
//! - Security event logging

use super::tls::{TlsIdentity, TrustStore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Security manager configuration.
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// Whether mTLS is required.
    pub require_mtls: bool,
    /// Certificate expiry warning threshold.
    pub expiry_warning_days: u32,
    /// Enable automatic credential reload.
    pub auto_reload: bool,
    /// Credential check interval.
    pub check_interval: Duration,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            require_mtls: true,
            expiry_warning_days: 30,
            auto_reload: true,
            check_interval: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// Security manager for mTLS identities.
#[derive(Debug)]
pub struct SecurityManager {
    /// Configuration.
    config: SecurityConfig,
    /// Identity manager.
    identity_manager: RwLock<MtlsIdentityManager>,
    /// Statistics.
    stats: SecurityStats,
}

/// Security statistics.
#[derive(Debug)]
struct SecurityStats {
    /// Number of credential reloads.
    reloads: AtomicU64,
    /// Number of authentication failures.
    auth_failures: AtomicU64,
    /// Number of authorization failures.
    authz_failures: AtomicU64,
}

impl SecurityStats {
    fn new() -> Self {
        Self {
            reloads: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            authz_failures: AtomicU64::new(0),
        }
    }
}

impl SecurityManager {
    /// Create a new security manager with configuration.
    pub fn with_config(config: SecurityConfig) -> Self {
        Self {
            config,
            identity_manager: RwLock::new(MtlsIdentityManager::new()),
            stats: SecurityStats::new(),
        }
    }

    /// Create a new security manager with mTLS requirement.
    pub fn new(require_mtls: bool) -> Self {
        let config = SecurityConfig {
            require_mtls,
            ..Default::default()
        };
        Self::with_config(config)
    }

    /// Check if mTLS is required.
    pub fn requires_mtls(&self) -> bool {
        self.config.require_mtls
    }

    /// Get the identity manager.
    pub fn identity_manager(&self) -> &RwLock<MtlsIdentityManager> {
        &self.identity_manager
    }

    /// Check if credentials need rotation.
    pub fn check_credentials(&self) -> CredentialStatus {
        let manager = self.identity_manager.read().unwrap();

        let server_expiring = manager.server_identity.as_ref().is_some_and(|id| {
            id.is_expiring_soon(Duration::from_secs(
                self.config.expiry_warning_days as u64 * 86400,
            ))
        });

        let client_expiring = manager.client_identity.as_ref().is_some_and(|id| {
            id.is_expiring_soon(Duration::from_secs(
                self.config.expiry_warning_days as u64 * 86400,
            ))
        });

        CredentialStatus {
            server_expiring,
            client_expiring,
            last_reload: manager.last_reload,
            reload_count: self.stats.reloads.load(Ordering::Relaxed),
        }
    }

    /// Reload credentials from disk.
    pub fn reload_credentials(&self) -> anyhow::Result<()> {
        let mut manager = self.identity_manager.write().unwrap();
        manager.reload()?;
        self.stats.reloads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Record an authentication failure.
    pub fn record_auth_failure(&self) {
        self.stats.auth_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an authorization failure.
    pub fn record_authz_failure(&self) {
        self.stats.authz_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Get security metrics.
    pub fn metrics(&self) -> SecurityMetrics {
        SecurityMetrics {
            credential_reloads: self.stats.reloads.load(Ordering::Relaxed),
            auth_failures: self.stats.auth_failures.load(Ordering::Relaxed),
            authz_failures: self.stats.authz_failures.load(Ordering::Relaxed),
            mtls_required: self.config.require_mtls,
        }
    }
}

impl Default for SecurityManager {
    fn default() -> Self {
        Self::new(true) // Production default: require mTLS
    }
}

/// Credential status information.
#[derive(Debug, Clone)]
pub struct CredentialStatus {
    /// Server certificate expiring soon.
    pub server_expiring: bool,
    /// Client certificate expiring soon.
    pub client_expiring: bool,
    /// Last credential reload time.
    pub last_reload: Option<Instant>,
    /// Number of reloads performed.
    pub reload_count: u64,
}

/// Security metrics.
#[derive(Debug, Clone)]
pub struct SecurityMetrics {
    /// Number of credential reloads.
    pub credential_reloads: u64,
    /// Number of authentication failures.
    pub auth_failures: u64,
    /// Number of authorization failures.
    pub authz_failures: u64,
    /// Whether mTLS is required.
    pub mtls_required: bool,
}

/// mTLS identity manager.
///
/// Manages server and client identities for mTLS connections.
#[derive(Debug)]
pub struct MtlsIdentityManager {
    /// Server identity for TLS listener.
    pub server_identity: Option<TlsIdentity>,
    /// Client identity for outbound mTLS.
    pub client_identity: Option<TlsIdentity>,
    /// Server CA trust store.
    pub server_ca: Option<TrustStore>,
    /// Client CA trust store (for verifying client certs).
    pub client_ca: Option<TrustStore>,
    /// Path to server cert (for reload).
    server_cert_path: Option<String>,
    /// Path to server key (for reload).
    server_key_path: Option<String>,
    /// Path to client cert (for reload).
    client_cert_path: Option<String>,
    /// Path to client key (for reload).
    client_key_path: Option<String>,
    /// Last reload time.
    last_reload: Option<Instant>,
}

impl MtlsIdentityManager {
    /// Create a new empty identity manager.
    pub fn new() -> Self {
        Self {
            server_identity: None,
            client_identity: None,
            server_ca: None,
            client_ca: None,
            server_cert_path: None,
            server_key_path: None,
            client_cert_path: None,
            client_key_path: None,
            last_reload: None,
        }
    }

    /// Load server identity from file paths.
    pub fn load_server_identity(&mut self, cert_path: &str, key_path: &str) -> anyhow::Result<()> {
        let identity = TlsIdentity::load(cert_path, key_path)?;
        self.server_identity = Some(identity);
        self.server_cert_path = Some(cert_path.to_string());
        self.server_key_path = Some(key_path.to_string());
        self.last_reload = Some(Instant::now());
        Ok(())
    }

    /// Load client identity from file paths.
    pub fn load_client_identity(&mut self, cert_path: &str, key_path: &str) -> anyhow::Result<()> {
        let identity = TlsIdentity::load(cert_path, key_path)?;
        self.client_identity = Some(identity);
        self.client_cert_path = Some(cert_path.to_string());
        self.client_key_path = Some(key_path.to_string());
        self.last_reload = Some(Instant::now());
        Ok(())
    }

    /// Load server CA trust store.
    pub fn load_server_ca(&mut self, ca_path: &str) -> anyhow::Result<()> {
        let trust_store = TrustStore::load(ca_path)?;
        self.server_ca = Some(trust_store);
        Ok(())
    }

    /// Load client CA trust store.
    pub fn load_client_ca(&mut self, ca_path: &str) -> anyhow::Result<()> {
        let trust_store = TrustStore::load(ca_path)?;
        self.client_ca = Some(trust_store);
        Ok(())
    }

    /// Reload identities from stored paths.
    pub fn reload(&mut self) -> anyhow::Result<()> {
        if let (Some(cert_path), Some(key_path)) = (&self.server_cert_path, &self.server_key_path) {
            let identity = TlsIdentity::load(cert_path, key_path)?;
            self.server_identity = Some(identity);
        }

        if let (Some(cert_path), Some(key_path)) = (&self.client_cert_path, &self.client_key_path) {
            let identity = TlsIdentity::load(cert_path, key_path)?;
            self.client_identity = Some(identity);
        }

        self.last_reload = Some(Instant::now());
        Ok(())
    }

    /// Check if server identity is loaded.
    pub fn has_server_identity(&self) -> bool {
        self.server_identity.is_some()
    }

    /// Check if client identity is loaded.
    pub fn has_client_identity(&self) -> bool {
        self.client_identity.is_some()
    }

    /// Get the server common name.
    pub fn server_common_name(&self) -> Option<&str> {
        self.server_identity
            .as_ref()
            .and_then(|id| id.common_name.as_deref())
    }

    /// Get the client common name.
    pub fn client_common_name(&self) -> Option<&str> {
        self.client_identity
            .as_ref()
            .and_then(|id| id.common_name.as_deref())
    }
}

impl Default for MtlsIdentityManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Client identity extracted from mTLS certificate.
#[derive(Debug, Clone)]
pub struct ClientIdentity {
    /// Common name from certificate.
    pub common_name: String,
    /// Tenant ID (if derivable from certificate).
    pub tenant_id: Option<String>,
    /// Principal ID (if derivable from certificate).
    pub principal_id: Option<String>,
}

impl ClientIdentity {
    /// Create a new client identity.
    pub fn new(common_name: impl Into<String>) -> Self {
        Self {
            common_name: common_name.into(),
            tenant_id: None,
            principal_id: None,
        }
    }

    /// Create with tenant and principal.
    pub fn with_tenant_principal(
        common_name: impl Into<String>,
        tenant_id: impl Into<String>,
        principal_id: impl Into<String>,
    ) -> Self {
        Self {
            common_name: common_name.into(),
            tenant_id: Some(tenant_id.into()),
            principal_id: Some(principal_id.into()),
        }
    }

    /// Parse identity from certificate common name.
    ///
    /// Expected format: "tenant_id/principal_id" or just "principal_id"
    pub fn from_common_name(cn: &str) -> Self {
        if let Some(slash_pos) = cn.find('/') {
            let tenant = &cn[..slash_pos];
            let principal = &cn[slash_pos + 1..];
            Self::with_tenant_principal(cn, tenant, principal)
        } else {
            Self {
                common_name: cn.to_string(),
                tenant_id: None,
                principal_id: Some(cn.to_string()),
            }
        }
    }
}

/// Security event for audit logging.
#[derive(Debug, Clone)]
pub enum SecurityEvent {
    /// Credential reload.
    CredentialReload {
        identity_type: IdentityType,
        common_name: Option<String>,
    },
    /// Authentication success.
    AuthSuccess { identity: ClientIdentity },
    /// Authentication failure.
    AuthFailure { reason: String },
    /// Authorization failure.
    AuthzFailure {
        identity: ClientIdentity,
        resource: String,
        action: String,
    },
    /// Certificate expiry warning.
    CertExpiryWarning {
        identity_type: IdentityType,
        common_name: Option<String>,
        days_remaining: u32,
    },
}

/// Type of identity (server or client).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityType {
    /// Server identity for TLS listener.
    Server,
    /// Client identity for outbound connections.
    Client,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_manager_new() {
        let manager = SecurityManager::new(true);
        assert!(manager.requires_mtls());

        let manager = SecurityManager::new(false);
        assert!(!manager.requires_mtls());
    }

    #[test]
    fn test_security_config_default() {
        let config = SecurityConfig::default();
        assert!(config.require_mtls);
        assert_eq!(config.expiry_warning_days, 30);
        assert!(config.auto_reload);
    }

    #[test]
    fn test_mtls_identity_manager_new() {
        let manager = MtlsIdentityManager::new();
        assert!(!manager.has_server_identity());
        assert!(!manager.has_client_identity());
        assert!(manager.server_common_name().is_none());
    }

    #[test]
    fn test_client_identity_from_common_name() {
        // With tenant/principal format
        let identity = ClientIdentity::from_common_name("acme/alice");
        assert_eq!(identity.common_name, "acme/alice");
        assert_eq!(identity.tenant_id, Some("acme".to_string()));
        assert_eq!(identity.principal_id, Some("alice".to_string()));

        // Without tenant
        let identity = ClientIdentity::from_common_name("bob");
        assert_eq!(identity.common_name, "bob");
        assert_eq!(identity.tenant_id, None);
        assert_eq!(identity.principal_id, Some("bob".to_string()));
    }

    #[test]
    fn test_credential_status() {
        let manager = SecurityManager::new(true);
        let status = manager.check_credentials();

        assert!(!status.server_expiring);
        assert!(!status.client_expiring);
        assert!(status.last_reload.is_none());
        assert_eq!(status.reload_count, 0);
    }

    #[test]
    fn test_security_metrics() {
        let manager = SecurityManager::new(true);

        manager.record_auth_failure();
        manager.record_auth_failure();
        manager.record_authz_failure();

        let metrics = manager.metrics();
        assert_eq!(metrics.auth_failures, 2);
        assert_eq!(metrics.authz_failures, 1);
        assert!(metrics.mtls_required);
    }
}
