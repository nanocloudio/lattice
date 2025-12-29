//! TLS and mTLS configuration.
//!
//! Production profiles require mTLS for all protocol adapters per §10.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      TLS Configuration                       │
//! ├─────────────────────────────────────────────────────────────┤
//! │   TlsConfig        - File paths and requirements            │
//! │   TlsIdentity      - Loaded certificate chain + key         │
//! │   TlsServerBuilder - Builds rustls ServerConfig             │
//! │   TlsClientBuilder - Builds rustls ClientConfig             │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use anyhow::{Context, Result};
use std::path::Path;
use std::time::Duration;

/// TLS configuration for a listener.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Certificate chain path.
    pub cert_chain_path: String,
    /// Private key path.
    pub key_path: String,
    /// Client CA path for mTLS (optional).
    pub client_ca_path: Option<String>,
    /// Require client certificates.
    pub require_client_cert: bool,
    /// Minimum TLS version (default: TLS 1.2).
    pub min_tls_version: TlsVersion,
    /// ALPN protocols (e.g., ["h2", "http/1.1"]).
    pub alpn_protocols: Vec<String>,
}

/// TLS version enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersion {
    /// TLS 1.2
    Tls12,
    /// TLS 1.3
    Tls13,
}

impl Default for TlsVersion {
    fn default() -> Self {
        Self::Tls12
    }
}

impl TlsConfig {
    /// Create a new TLS configuration.
    pub fn new(cert_chain_path: String, key_path: String) -> Self {
        Self {
            cert_chain_path,
            key_path,
            client_ca_path: None,
            require_client_cert: false,
            min_tls_version: TlsVersion::Tls12,
            alpn_protocols: Vec::new(),
        }
    }

    /// Enable mTLS with the given client CA.
    pub fn with_mtls(mut self, client_ca_path: String) -> Self {
        self.client_ca_path = Some(client_ca_path);
        self.require_client_cert = true;
        self
    }

    /// Set minimum TLS version.
    pub fn with_min_version(mut self, version: TlsVersion) -> Self {
        self.min_tls_version = version;
        self
    }

    /// Set ALPN protocols.
    pub fn with_alpn(mut self, protocols: Vec<String>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Validate that TLS files exist.
    pub fn validate(&self) -> Result<()> {
        if !Path::new(&self.cert_chain_path).exists() {
            anyhow::bail!("TLS certificate chain not found: {}", self.cert_chain_path);
        }
        if !Path::new(&self.key_path).exists() {
            anyhow::bail!("TLS private key not found: {}", self.key_path);
        }
        if let Some(ref ca_path) = self.client_ca_path {
            if !Path::new(ca_path).exists() {
                anyhow::bail!("TLS client CA not found: {}", ca_path);
            }
        }
        Ok(())
    }

    /// Load the certificate chain.
    pub fn load_cert_chain(&self) -> Result<Vec<u8>> {
        std::fs::read(&self.cert_chain_path)
            .with_context(|| format!("failed to read cert chain: {}", self.cert_chain_path))
    }

    /// Load the private key.
    pub fn load_key(&self) -> Result<Vec<u8>> {
        std::fs::read(&self.key_path)
            .with_context(|| format!("failed to read private key: {}", self.key_path))
    }

    /// Load the client CA bundle.
    pub fn load_client_ca(&self) -> Result<Option<Vec<u8>>> {
        match &self.client_ca_path {
            Some(path) => {
                let data = std::fs::read(path)
                    .with_context(|| format!("failed to read client CA: {}", path))?;
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }
}

/// Loaded TLS identity (certificate chain + private key).
#[derive(Debug, Clone)]
pub struct TlsIdentity {
    /// Certificate chain in PEM format.
    pub cert_chain_pem: Vec<u8>,
    /// Private key in PEM format.
    pub key_pem: Vec<u8>,
    /// Common name from the certificate.
    pub common_name: Option<String>,
    /// Expiration time (if parsed).
    pub expires_at: Option<std::time::SystemTime>,
}

impl TlsIdentity {
    /// Load a TLS identity from file paths.
    pub fn load(cert_chain_path: &str, key_path: &str) -> Result<Self> {
        let cert_chain_pem = std::fs::read(cert_chain_path)
            .with_context(|| format!("failed to read cert chain: {}", cert_chain_path))?;
        let key_pem = std::fs::read(key_path)
            .with_context(|| format!("failed to read private key: {}", key_path))?;

        // Parse common name from certificate (simplified - real implementation would use x509-parser)
        let common_name = Self::parse_common_name(&cert_chain_pem);

        Ok(Self {
            cert_chain_pem,
            key_pem,
            common_name,
            expires_at: None,
        })
    }

    /// Parse common name from PEM certificate (simplified).
    fn parse_common_name(cert_pem: &[u8]) -> Option<String> {
        // In a real implementation, we would parse the X.509 certificate
        // For now, just look for CN= in the PEM data as a simple heuristic
        let cert_str = String::from_utf8_lossy(cert_pem);
        if let Some(cn_pos) = cert_str.find("CN=") {
            let remainder = &cert_str[cn_pos + 3..];
            let end = remainder.find([',', '\n', '/']).unwrap_or(remainder.len());
            Some(remainder[..end].trim().to_string())
        } else {
            None
        }
    }

    /// Check if the certificate is about to expire.
    pub fn is_expiring_soon(&self, threshold: Duration) -> bool {
        if let Some(expires_at) = self.expires_at {
            if let Ok(duration) = expires_at.duration_since(std::time::SystemTime::now()) {
                return duration < threshold;
            }
            return true; // Already expired
        }
        false
    }
}

/// Trust store for CA certificates.
#[derive(Debug, Clone)]
pub struct TrustStore {
    /// CA certificates in PEM format.
    pub ca_certs_pem: Vec<u8>,
    /// Number of certificates loaded.
    pub cert_count: usize,
}

impl TrustStore {
    /// Load a trust store from a CA bundle file.
    pub fn load(ca_path: &str) -> Result<Self> {
        let ca_certs_pem = std::fs::read(ca_path)
            .with_context(|| format!("failed to read CA bundle: {}", ca_path))?;

        // Count certificates (count "BEGIN CERTIFICATE" occurrences)
        let cert_str = String::from_utf8_lossy(&ca_certs_pem);
        let cert_count = cert_str.matches("BEGIN CERTIFICATE").count();

        Ok(Self {
            ca_certs_pem,
            cert_count,
        })
    }

    /// Create an empty trust store.
    pub fn empty() -> Self {
        Self {
            ca_certs_pem: Vec::new(),
            cert_count: 0,
        }
    }
}

/// Builder for TLS server configuration.
#[derive(Debug)]
pub struct TlsServerBuilder {
    /// Server identity.
    identity: Option<TlsIdentity>,
    /// Client CA trust store (for mTLS).
    client_ca: Option<TrustStore>,
    /// Require client certificates.
    require_client_cert: bool,
    /// Minimum TLS version.
    min_version: TlsVersion,
    /// ALPN protocols.
    alpn_protocols: Vec<String>,
}

impl TlsServerBuilder {
    /// Create a new TLS server builder.
    pub fn new() -> Self {
        Self {
            identity: None,
            client_ca: None,
            require_client_cert: false,
            min_version: TlsVersion::Tls12,
            alpn_protocols: Vec::new(),
        }
    }

    /// Set the server identity.
    pub fn identity(mut self, identity: TlsIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Set the client CA for mTLS verification.
    pub fn client_ca(mut self, trust_store: TrustStore) -> Self {
        self.client_ca = Some(trust_store);
        self
    }

    /// Require client certificates.
    pub fn require_client_cert(mut self, require: bool) -> Self {
        self.require_client_cert = require;
        self
    }

    /// Set minimum TLS version.
    pub fn min_version(mut self, version: TlsVersion) -> Self {
        self.min_version = version;
        self
    }

    /// Set ALPN protocols.
    pub fn alpn(mut self, protocols: Vec<String>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Build from a TlsConfig.
    pub fn from_config(config: &TlsConfig) -> Result<Self> {
        let identity = TlsIdentity::load(&config.cert_chain_path, &config.key_path)?;

        let mut builder = Self::new()
            .identity(identity)
            .require_client_cert(config.require_client_cert)
            .min_version(config.min_tls_version)
            .alpn(config.alpn_protocols.clone());

        if let Some(ref ca_path) = config.client_ca_path {
            let trust_store = TrustStore::load(ca_path)?;
            builder = builder.client_ca(trust_store);
        }

        Ok(builder)
    }

    /// Build the server configuration.
    ///
    /// Returns a configuration suitable for use with rustls.
    /// In a real implementation, this would return Arc<rustls::ServerConfig>.
    pub fn build(self) -> Result<TlsServerConfig> {
        let identity = self
            .identity
            .ok_or_else(|| anyhow::anyhow!("server identity required"))?;

        Ok(TlsServerConfig {
            identity,
            client_ca: self.client_ca,
            require_client_cert: self.require_client_cert,
            min_version: self.min_version,
            alpn_protocols: self.alpn_protocols,
        })
    }
}

impl Default for TlsServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// TLS server configuration (abstracted).
#[derive(Debug, Clone)]
pub struct TlsServerConfig {
    /// Server identity.
    pub identity: TlsIdentity,
    /// Client CA trust store.
    pub client_ca: Option<TrustStore>,
    /// Require client certificates.
    pub require_client_cert: bool,
    /// Minimum TLS version.
    pub min_version: TlsVersion,
    /// ALPN protocols.
    pub alpn_protocols: Vec<String>,
}

impl TlsServerConfig {
    /// Check if mTLS is enabled.
    pub fn is_mtls(&self) -> bool {
        self.require_client_cert && self.client_ca.is_some()
    }
}

/// Builder for TLS client configuration.
#[derive(Debug)]
pub struct TlsClientBuilder {
    /// Client identity (for mTLS).
    identity: Option<TlsIdentity>,
    /// Server CA trust store.
    server_ca: Option<TrustStore>,
    /// Server name for SNI.
    server_name: Option<String>,
    /// Minimum TLS version.
    min_version: TlsVersion,
    /// ALPN protocols.
    alpn_protocols: Vec<String>,
}

impl TlsClientBuilder {
    /// Create a new TLS client builder.
    pub fn new() -> Self {
        Self {
            identity: None,
            server_ca: None,
            server_name: None,
            min_version: TlsVersion::Tls12,
            alpn_protocols: Vec::new(),
        }
    }

    /// Set the client identity for mTLS.
    pub fn identity(mut self, identity: TlsIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Set the server CA trust store.
    pub fn server_ca(mut self, trust_store: TrustStore) -> Self {
        self.server_ca = Some(trust_store);
        self
    }

    /// Set the server name for SNI.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Set minimum TLS version.
    pub fn min_version(mut self, version: TlsVersion) -> Self {
        self.min_version = version;
        self
    }

    /// Set ALPN protocols.
    pub fn alpn(mut self, protocols: Vec<String>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Build the client configuration.
    pub fn build(self) -> Result<TlsClientConfig> {
        Ok(TlsClientConfig {
            identity: self.identity,
            server_ca: self.server_ca,
            server_name: self.server_name,
            min_version: self.min_version,
            alpn_protocols: self.alpn_protocols,
        })
    }
}

impl Default for TlsClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// TLS client configuration (abstracted).
#[derive(Debug)]
pub struct TlsClientConfig {
    /// Client identity (for mTLS).
    pub identity: Option<TlsIdentity>,
    /// Server CA trust store.
    pub server_ca: Option<TrustStore>,
    /// Server name for SNI.
    pub server_name: Option<String>,
    /// Minimum TLS version.
    pub min_version: TlsVersion,
    /// ALPN protocols.
    pub alpn_protocols: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_new() {
        let config = TlsConfig::new("cert.pem".to_string(), "key.pem".to_string());
        assert_eq!(config.cert_chain_path, "cert.pem");
        assert_eq!(config.key_path, "key.pem");
        assert!(!config.require_client_cert);
        assert!(config.client_ca_path.is_none());
    }

    #[test]
    fn test_tls_config_mtls() {
        let config = TlsConfig::new("cert.pem".to_string(), "key.pem".to_string())
            .with_mtls("ca.pem".to_string());

        assert!(config.require_client_cert);
        assert_eq!(config.client_ca_path, Some("ca.pem".to_string()));
    }

    #[test]
    fn test_tls_config_alpn() {
        let config = TlsConfig::new("cert.pem".to_string(), "key.pem".to_string())
            .with_alpn(vec!["h2".to_string(), "http/1.1".to_string()]);

        assert_eq!(config.alpn_protocols.len(), 2);
    }

    #[test]
    fn test_trust_store_empty() {
        let store = TrustStore::empty();
        assert_eq!(store.cert_count, 0);
        assert!(store.ca_certs_pem.is_empty());
    }

    #[test]
    fn test_tls_server_builder() {
        let identity = TlsIdentity {
            cert_chain_pem: b"cert".to_vec(),
            key_pem: b"key".to_vec(),
            common_name: Some("test".to_string()),
            expires_at: None,
        };

        let config = TlsServerBuilder::new()
            .identity(identity)
            .require_client_cert(true)
            .min_version(TlsVersion::Tls13)
            .alpn(vec!["h2".to_string()])
            .build()
            .expect("should build");

        assert!(config.require_client_cert);
        assert_eq!(config.min_version, TlsVersion::Tls13);
        assert_eq!(config.alpn_protocols, vec!["h2".to_string()]);
    }

    #[test]
    fn test_tls_identity_parse_cn() {
        // This is a simplified test - real CN parsing would be more robust
        let cert_pem =
            b"-----BEGIN CERTIFICATE-----\nCN=test.example.com,O=Test\n-----END CERTIFICATE-----";
        let cn = TlsIdentity::parse_common_name(cert_pem);
        assert_eq!(cn, Some("test.example.com".to_string()));
    }

    #[test]
    fn test_tls_server_config_is_mtls() {
        let identity = TlsIdentity {
            cert_chain_pem: b"cert".to_vec(),
            key_pem: b"key".to_vec(),
            common_name: None,
            expires_at: None,
        };

        let config_without_mtls = TlsServerConfig {
            identity: identity.clone(),
            client_ca: None,
            require_client_cert: false,
            min_version: TlsVersion::Tls12,
            alpn_protocols: Vec::new(),
        };
        assert!(!config_without_mtls.is_mtls());

        let config_with_mtls = TlsServerConfig {
            identity,
            client_ca: Some(TrustStore::empty()),
            require_client_cert: true,
            min_version: TlsVersion::Tls12,
            alpn_protocols: Vec::new(),
        };
        assert!(config_with_mtls.is_mtls());
    }
}
