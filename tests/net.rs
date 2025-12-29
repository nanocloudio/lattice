//! Networking integration tests.
//!
//! Tests for TLS, listeners, and security components.

mod common;

use std::net::SocketAddr;

use lattice::net::listeners::{
    BackpressureConfig, BackpressureManager, GrpcListenerConfig, GrpcListenerState, ListenerState,
    RequestRouter, RoutingTable,
};
use lattice::net::security::{
    ClientIdentity, CredentialStatus, IdentityType, MtlsIdentityManager, SecurityConfig,
    SecurityEvent, SecurityManager, SecurityMetrics,
};
use lattice::net::tls::{
    TlsClientBuilder, TlsConfig, TlsIdentity, TlsServerBuilder, TlsServerConfig, TlsVersion,
    TrustStore,
};

// ============================================================================
// TLS Config Tests
// ============================================================================

#[test]
fn tls_config_new() {
    let config = TlsConfig::new(
        "/path/to/cert.pem".to_string(),
        "/path/to/key.pem".to_string(),
    );
    assert_eq!(config.cert_chain_path, "/path/to/cert.pem");
    assert_eq!(config.key_path, "/path/to/key.pem");
    assert!(config.client_ca_path.is_none());
}

#[test]
fn tls_config_with_mtls() {
    let config = TlsConfig::new(
        "/path/to/cert.pem".to_string(),
        "/path/to/key.pem".to_string(),
    )
    .with_mtls("/path/to/ca.pem".to_string());

    assert!(config.require_client_cert);
    assert_eq!(config.client_ca_path, Some("/path/to/ca.pem".to_string()));
}

#[test]
fn tls_config_with_min_version() {
    let config = TlsConfig::new("cert.pem".to_string(), "key.pem".to_string())
        .with_min_version(TlsVersion::Tls13);

    assert_eq!(config.min_tls_version, TlsVersion::Tls13);
}

#[test]
fn tls_config_with_alpn() {
    let config = TlsConfig::new("cert.pem".to_string(), "key.pem".to_string())
        .with_alpn(vec!["h2".to_string(), "http/1.1".to_string()]);

    assert_eq!(config.alpn_protocols, vec!["h2", "http/1.1"]);
}

#[test]
fn tls_version_default() {
    let version = TlsVersion::default();
    assert_eq!(version, TlsVersion::Tls12);
}

#[test]
fn tls_identity_manual() {
    let identity = TlsIdentity {
        cert_chain_pem: b"cert".to_vec(),
        key_pem: b"key".to_vec(),
        common_name: Some("test.example.com".to_string()),
        expires_at: None,
    };

    assert_eq!(identity.common_name, Some("test.example.com".to_string()));
}

#[test]
fn trust_store_empty() {
    let store = TrustStore::empty();
    assert_eq!(store.cert_count, 0);
    assert!(store.ca_certs_pem.is_empty());
}

#[test]
fn tls_server_builder() {
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
fn tls_server_config_is_mtls() {
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

#[test]
fn tls_client_builder() {
    let config = TlsClientBuilder::new()
        .server_name("test.example.com")
        .min_version(TlsVersion::Tls13)
        .alpn(vec!["h2".to_string()])
        .build()
        .expect("should build");

    assert_eq!(config.server_name, Some("test.example.com".to_string()));
    assert_eq!(config.min_version, TlsVersion::Tls13);
}

// ============================================================================
// Listener Tests
// ============================================================================

#[test]
fn listener_state_basic() {
    let addr: SocketAddr = ([127, 0, 0, 1], 2379).into();
    let mut state = ListenerState::new(addr, "grpc");

    assert!(!state.active);
    assert_eq!(state.protocol, "grpc");

    state.activate();
    assert!(state.active);

    state.deactivate();
    assert!(!state.active);
}

#[test]
fn grpc_listener_config_default() {
    let config = GrpcListenerConfig::default();

    assert_eq!(config.bind_addr.port(), 2379);
    assert!(config.tls.is_none());
    assert_eq!(config.max_connections, 10000);
    assert!(config.http2_keepalive);
}

#[test]
fn grpc_listener_state() {
    let config = GrpcListenerConfig::default();
    let mut listener = GrpcListenerState::new(config);

    assert!(!listener.is_active());
    assert!(!listener.is_shutting_down());

    listener.start();
    assert!(listener.is_active());

    let handle = listener.accept_connection().expect("should accept");
    assert_eq!(listener.active_connections(), 1);

    listener.release_connection(handle);
    assert_eq!(listener.active_connections(), 0);

    listener.shutdown();
    assert!(!listener.is_active());
    assert!(listener.is_shutting_down());
}

#[test]
fn grpc_listener_state_bind_addr() {
    let config = GrpcListenerConfig::default();
    let listener = GrpcListenerState::new(config);

    assert_eq!(listener.bind_addr().port(), 2379);
}

#[test]
fn listener_metrics() {
    let config = GrpcListenerConfig::default();
    let mut listener = GrpcListenerState::new(config);
    listener.start();

    let _handle = listener.accept_connection().unwrap();
    listener.record_request();
    listener.record_throttle();

    let metrics = listener.metrics();
    assert!(metrics.is_active);
    assert_eq!(metrics.active_connections, 1);
    assert_eq!(metrics.connections_accepted, 1);
    assert_eq!(metrics.requests_processed, 1);
    assert_eq!(metrics.requests_throttled, 1);
}

#[test]
fn grpc_listener_reject_during_shutdown() {
    let config = GrpcListenerConfig::default();
    let mut listener = GrpcListenerState::new(config);

    listener.start();
    listener.shutdown();

    // Should reject connections during shutdown
    assert!(listener.accept_connection().is_err());
}

// ============================================================================
// Request Router Tests
// ============================================================================

#[test]
fn routing_table_basic() {
    let mut table = RoutingTable::new(1);
    table.set_tenant_kpg_count("acme", 4);

    assert_eq!(table.get_kpg_count("acme"), Some(4));
    assert_eq!(table.get_kpg_count("other"), None);
    assert_eq!(table.routing_epoch(), 1);
}

#[test]
fn request_router_basic() {
    let mut table = RoutingTable::new(1);
    table.set_tenant_kpg_count("acme", 4);

    let router = RequestRouter::new(table);

    let kpg = router.route_key("acme", b"key1").expect("should route");
    assert!(kpg < 4);

    // Same key should route to same KPG
    let kpg2 = router.route_key("acme", b"key1").expect("should route");
    assert_eq!(kpg, kpg2);

    // Unknown tenant should fail
    assert!(router.route_key("unknown", b"key1").is_err());
}

#[test]
fn request_router_epoch_validation() {
    let table = RoutingTable::new(5);
    let router = RequestRouter::new(table);

    assert!(router.validate_epoch(5).is_ok());
    assert!(router.validate_epoch(4).is_err());
    assert!(router.validate_epoch(6).is_err());
}

#[test]
fn request_router_current_epoch() {
    let table = RoutingTable::new(42);
    let router = RequestRouter::new(table);

    assert_eq!(router.current_epoch(), 42);
}

#[test]
fn request_router_with_default_tenant() {
    let table = RoutingTable::new(1);
    let router = RequestRouter::new(table).with_default_tenant("default");

    // Router constructed without error
    assert_eq!(router.current_epoch(), 1);
}

#[test]
fn request_router_spans_multiple_kpgs() {
    let mut table = RoutingTable::new(1);
    table.set_tenant_kpg_count("acme", 4);

    let router = RequestRouter::new(table);

    // Single key doesn't span
    let spans = router.spans_multiple_kpgs("acme", b"key1", None).unwrap();
    assert!(!spans);

    // Empty range_end (prefix search) may span multiple KPGs
    let spans = router
        .spans_multiple_kpgs("acme", b"key1", Some(b""))
        .unwrap();
    assert!(spans);
}

#[test]
fn request_router_update_table() {
    let table = RoutingTable::new(1);
    let mut router = RequestRouter::new(table);

    assert_eq!(router.current_epoch(), 1);

    let new_table = RoutingTable::new(2);
    router.update_routing_table(new_table);

    assert_eq!(router.current_epoch(), 2);
}

// ============================================================================
// Backpressure Tests
// ============================================================================

#[test]
fn backpressure_config_default() {
    let config = BackpressureConfig::default();
    assert_eq!(config.global_rps_limit, 100_000);
    assert_eq!(config.per_tenant_rps_limit, 10_000);
}

#[test]
fn backpressure_manager_basic() {
    let config = BackpressureConfig {
        global_rps_limit: 10,
        ..Default::default()
    };
    let manager = BackpressureManager::new(config);

    // Should allow initial requests
    for _ in 0..11 {
        assert!(manager.check("tenant1").is_ok());
    }

    // Should throttle after exceeding limit
    assert!(manager.check("tenant1").is_err());
    assert!(manager.is_backpressure_active());

    // Reset should allow requests again
    manager.reset_counter();
    assert!(!manager.is_backpressure_active());
    assert!(manager.check("tenant1").is_ok());
}

#[test]
fn backpressure_clustor_flow_control() {
    let manager = BackpressureManager::new(BackpressureConfig::default());

    assert!(manager.check("tenant1").is_ok());

    manager.set_clustor_backpressure(true);
    assert!(manager.check("tenant1").is_err());

    manager.set_clustor_backpressure(false);
    assert!(manager.check("tenant1").is_ok());
}

// ============================================================================
// Security Config Tests
// ============================================================================

#[test]
fn security_config_defaults() {
    let config = SecurityConfig::default();
    assert!(config.require_mtls);
    assert!(config.auto_reload);
}

#[test]
fn security_manager_basic() {
    let manager = SecurityManager::new(true);
    assert!(manager.requires_mtls());

    let manager = SecurityManager::new(false);
    assert!(!manager.requires_mtls());
}

#[test]
fn security_manager_with_config() {
    let config = SecurityConfig {
        require_mtls: true,
        expiry_warning_days: 14,
        auto_reload: false,
        check_interval: std::time::Duration::from_secs(1800),
    };
    let manager = SecurityManager::with_config(config);

    assert!(manager.requires_mtls());
}

#[test]
fn security_manager_metrics() {
    let manager = SecurityManager::new(true);

    manager.record_auth_failure();
    manager.record_auth_failure();
    manager.record_authz_failure();

    let metrics = manager.metrics();
    assert_eq!(metrics.auth_failures, 2);
    assert_eq!(metrics.authz_failures, 1);
    assert!(metrics.mtls_required);
}

#[test]
fn security_manager_credential_status() {
    let manager = SecurityManager::new(true);
    let status = manager.check_credentials();

    assert!(!status.server_expiring);
    assert!(!status.client_expiring);
    assert!(status.last_reload.is_none());
    assert_eq!(status.reload_count, 0);
}

// ============================================================================
// mTLS Identity Tests
// ============================================================================

#[test]
fn mtls_identity_manager_basic() {
    let manager = MtlsIdentityManager::new();

    assert!(!manager.has_server_identity());
    assert!(!manager.has_client_identity());
    assert!(manager.server_common_name().is_none());
}

#[test]
fn client_identity_new() {
    let identity = ClientIdentity::new("test.example.com");

    assert_eq!(identity.common_name, "test.example.com");
    assert!(identity.tenant_id.is_none());
    assert!(identity.principal_id.is_none());
}

#[test]
fn client_identity_with_tenant_principal() {
    let identity = ClientIdentity::with_tenant_principal("cn", "tenant1", "alice");

    assert_eq!(identity.common_name, "cn");
    assert_eq!(identity.tenant_id, Some("tenant1".to_string()));
    assert_eq!(identity.principal_id, Some("alice".to_string()));
}

#[test]
fn client_identity_from_common_name() {
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
fn identity_type_variants() {
    let server = IdentityType::Server;
    let client = IdentityType::Client;

    assert!(matches!(server, IdentityType::Server));
    assert!(matches!(client, IdentityType::Client));
}

// ============================================================================
// Security Event Tests
// ============================================================================

#[test]
fn security_event_credential_reload() {
    let event = SecurityEvent::CredentialReload {
        identity_type: IdentityType::Server,
        common_name: Some("test.example.com".to_string()),
    };

    match event {
        SecurityEvent::CredentialReload {
            identity_type,
            common_name,
        } => {
            assert_eq!(identity_type, IdentityType::Server);
            assert_eq!(common_name, Some("test.example.com".to_string()));
        }
        _ => panic!("expected CredentialReload"),
    }
}

#[test]
fn security_event_auth_success() {
    let identity = ClientIdentity::new("test");

    let event = SecurityEvent::AuthSuccess {
        identity: identity.clone(),
    };

    match event {
        SecurityEvent::AuthSuccess { identity } => {
            assert_eq!(identity.common_name, "test");
        }
        _ => panic!("expected AuthSuccess"),
    }
}

#[test]
fn security_event_auth_failure() {
    let event = SecurityEvent::AuthFailure {
        reason: "invalid credentials".to_string(),
    };

    match event {
        SecurityEvent::AuthFailure { reason } => {
            assert!(reason.contains("invalid"));
        }
        _ => panic!("expected AuthFailure"),
    }
}

#[test]
fn security_event_authz_failure() {
    let identity = ClientIdentity::new("test");

    let event = SecurityEvent::AuthzFailure {
        identity,
        resource: "/data/secret".to_string(),
        action: "write".to_string(),
    };

    match event {
        SecurityEvent::AuthzFailure {
            resource, action, ..
        } => {
            assert_eq!(resource, "/data/secret");
            assert_eq!(action, "write");
        }
        _ => panic!("expected AuthzFailure"),
    }
}

#[test]
fn security_event_cert_expiry_warning() {
    let event = SecurityEvent::CertExpiryWarning {
        identity_type: IdentityType::Server,
        common_name: Some("server.example.com".to_string()),
        days_remaining: 7,
    };

    match event {
        SecurityEvent::CertExpiryWarning { days_remaining, .. } => {
            assert_eq!(days_remaining, 7);
        }
        _ => panic!("expected CertExpiryWarning"),
    }
}

// ============================================================================
// Credential Status Tests
// ============================================================================

#[test]
fn credential_status_fields() {
    let status = CredentialStatus {
        server_expiring: true,
        client_expiring: false,
        last_reload: None,
        reload_count: 5,
    };

    assert!(status.server_expiring);
    assert!(!status.client_expiring);
    assert_eq!(status.reload_count, 5);
}

// ============================================================================
// Security Metrics Tests
// ============================================================================

#[test]
fn security_metrics_fields() {
    let metrics = SecurityMetrics {
        credential_reloads: 3,
        auth_failures: 10,
        authz_failures: 2,
        mtls_required: true,
    };

    assert_eq!(metrics.credential_reloads, 3);
    assert_eq!(metrics.auth_failures, 10);
    assert_eq!(metrics.authz_failures, 2);
    assert!(metrics.mtls_required);
}
