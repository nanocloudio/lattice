//! gRPC service setup for etcd v3 adapter.
//!
//! Implements the etcd v3 gRPC endpoint with:
//! - TLS/mTLS listener configuration
//! - Request context extraction and propagation
//! - kv_epoch routing validation
//! - LIN-BOUND enforcement for linearizable reads

use crate::adapters::{Adapter, AdapterConfig, AdapterHealth, AdapterState, RequestContext};
use crate::control::routing::{RoutingEpochTracker, RoutingTable};
use crate::core::error::{LatticeError, LatticeResult, LinearizabilityFailureReason};
use crate::net::security::ClientIdentity;
use crate::net::tls::TlsConfig;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use super::auth::{IdentityExtractor, IdentityVerifier};

/// Configuration for the etcd gRPC service.
#[derive(Debug, Clone)]
pub struct EtcdServiceConfig {
    /// Base adapter configuration.
    pub adapter: AdapterConfig,

    /// Cluster ID for response headers.
    pub cluster_id: u64,

    /// Member ID for response headers.
    pub member_id: u64,

    /// Whether to allow insecure (non-TLS) connections.
    pub allow_insecure: bool,

    /// Maximum message size in bytes.
    pub max_message_size: usize,

    /// Keepalive interval in milliseconds.
    pub keepalive_interval_ms: u64,

    /// Keepalive timeout in milliseconds.
    pub keepalive_timeout_ms: u64,
}

impl Default for EtcdServiceConfig {
    fn default() -> Self {
        Self {
            adapter: AdapterConfig::default(),
            cluster_id: 0,
            member_id: 0,
            allow_insecure: false,
            max_message_size: 4 * 1024 * 1024, // 4MB
            keepalive_interval_ms: 30_000,
            keepalive_timeout_ms: 20_000,
        }
    }
}

/// gRPC listener for the etcd adapter.
///
/// Handles TLS/mTLS setup and connection management.
pub struct GrpcListener {
    /// Bind address.
    bind_addr: SocketAddr,

    /// TLS configuration.
    tls_config: Option<TlsConfig>,

    /// Whether the listener is active.
    active: bool,

    /// Connection count.
    connection_count: AtomicUsize,
}

impl GrpcListener {
    /// Create a new gRPC listener.
    pub fn new(bind_addr: SocketAddr, tls_config: Option<TlsConfig>) -> Self {
        Self {
            bind_addr,
            tls_config,
            active: false,
            connection_count: AtomicUsize::new(0),
        }
    }

    /// Get the bind address.
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Check if TLS is configured.
    pub fn has_tls(&self) -> bool {
        self.tls_config.is_some()
    }

    /// Check if mTLS is configured.
    pub fn has_mtls(&self) -> bool {
        self.tls_config
            .as_ref()
            .map(|c| c.require_client_cert)
            .unwrap_or(false)
    }

    /// Get the current connection count.
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }

    /// Increment connection count.
    pub fn add_connection(&self) {
        self.connection_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement connection count.
    pub fn remove_connection(&self) {
        self.connection_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Validate TLS configuration.
    pub fn validate(&self) -> LatticeResult<()> {
        if let Some(ref tls) = self.tls_config {
            tls.validate().map_err(|e| LatticeError::InvalidRequest {
                message: e.to_string(),
            })?;
        }
        Ok(())
    }

    /// Mark listener as active.
    pub fn activate(&mut self) {
        self.active = true;
    }

    /// Mark listener as inactive.
    pub fn deactivate(&mut self) {
        self.active = false;
    }

    /// Check if listener is active.
    pub fn is_active(&self) -> bool {
        self.active
    }
}

/// Routing validator for kv_epoch validation.
///
/// Validates that incoming requests use the correct routing epoch
/// and routes keys to the appropriate KPG.
pub struct RoutingValidator {
    /// Epoch tracker for each tenant.
    epoch_trackers: RwLock<std::collections::HashMap<String, Arc<RoutingEpochTracker>>>,

    /// Routing tables for each tenant.
    routing_tables: RwLock<std::collections::HashMap<String, Arc<RoutingTable>>>,
}

impl RoutingValidator {
    /// Create a new routing validator.
    pub fn new() -> Self {
        Self {
            epoch_trackers: RwLock::new(std::collections::HashMap::new()),
            routing_tables: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Register a tenant's routing table.
    pub fn register_tenant(&self, tenant_id: &str, routing: RoutingTable) {
        let epoch = routing.kv_epoch;
        let tracker = Arc::new(RoutingEpochTracker::new(tenant_id, epoch));

        self.routing_tables
            .write()
            .unwrap()
            .insert(tenant_id.to_string(), Arc::new(routing));
        self.epoch_trackers
            .write()
            .unwrap()
            .insert(tenant_id.to_string(), tracker);
    }

    /// Get the routing table for a tenant.
    pub fn get_routing(&self, tenant_id: &str) -> Option<Arc<RoutingTable>> {
        self.routing_tables.read().unwrap().get(tenant_id).cloned()
    }

    /// Validate a request's epoch.
    pub fn validate_epoch(&self, tenant_id: &str, request_epoch: u64) -> LatticeResult<()> {
        let trackers = self.epoch_trackers.read().unwrap();
        let tracker = trackers
            .get(tenant_id)
            .ok_or_else(|| LatticeError::InvalidRequest {
                message: format!("unknown tenant: {}", tenant_id),
            })?;

        tracker.validate(request_epoch)?;
        Ok(())
    }

    /// Start epoch migration for a tenant.
    pub fn start_migration(&self, tenant_id: &str, new_epoch: u64) {
        if let Some(tracker) = self.epoch_trackers.read().unwrap().get(tenant_id) {
            tracker.start_migration(new_epoch);
        }
    }

    /// Complete epoch migration for a tenant.
    pub fn complete_migration(&self, tenant_id: &str) {
        if let Some(tracker) = self.epoch_trackers.read().unwrap().get(tenant_id) {
            tracker.complete_migration();
        }
    }

    /// Get the current epoch for a tenant.
    pub fn current_epoch(&self, tenant_id: &str) -> Option<u64> {
        self.epoch_trackers
            .read()
            .unwrap()
            .get(tenant_id)
            .map(|t| t.current_epoch())
    }
}

impl Default for RoutingValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Linearizability gate for LIN-BOUND enforcement.
///
/// Checks whether linearizable reads are available based on
/// Clustor ReadIndex eligibility and control plane state.
pub struct LinearizabilityGate {
    /// Whether linearizable reads are currently available.
    available: RwLock<bool>,

    /// Last failure reason (if any).
    last_failure_reason: RwLock<Option<LinearizabilityFailureReason>>,
}

impl LinearizabilityGate {
    /// Create a new linearizability gate.
    pub fn new() -> Self {
        Self {
            available: RwLock::new(true),
            last_failure_reason: RwLock::new(None),
        }
    }

    /// Check if linearizable reads are available.
    pub fn is_available(&self) -> bool {
        *self.available.read().unwrap()
    }

    /// Mark linearizability as unavailable.
    pub fn set_unavailable(&self, reason: LinearizabilityFailureReason) {
        *self.available.write().unwrap() = false;
        *self.last_failure_reason.write().unwrap() = Some(reason);
    }

    /// Mark linearizability as available.
    pub fn set_available(&self) {
        *self.available.write().unwrap() = true;
        *self.last_failure_reason.write().unwrap() = None;
    }

    /// Enforce LIN-BOUND for a linearizable operation.
    ///
    /// Returns an error if linearizability is not available.
    pub fn enforce(&self) -> LatticeResult<()> {
        if !self.is_available() {
            let reason = self
                .last_failure_reason
                .read()
                .unwrap()
                .unwrap_or(LinearizabilityFailureReason::NotLeader);
            return Err(LatticeError::linearizability_unavailable(reason));
        }
        Ok(())
    }

    /// Get the last failure reason.
    pub fn last_failure_reason(&self) -> Option<LinearizabilityFailureReason> {
        *self.last_failure_reason.read().unwrap()
    }
}

impl Default for LinearizabilityGate {
    fn default() -> Self {
        Self::new()
    }
}

/// Request interceptor for extracting and validating request context.
///
/// This runs before each gRPC handler to:
/// 1. Extract client identity from mTLS
/// 2. Parse request metadata for tenant/epoch/trace info
/// 3. Validate epoch against routing table
/// 4. Create request context for downstream processing
pub struct RequestInterceptor {
    /// Identity extractor.
    identity_extractor: IdentityExtractor,

    /// Identity verifier.
    identity_verifier: IdentityVerifier,

    /// Routing validator.
    routing_validator: Arc<RoutingValidator>,

    /// Linearizability gate.
    lin_gate: Arc<LinearizabilityGate>,
}

impl RequestInterceptor {
    /// Create a new request interceptor.
    pub fn new(
        identity_extractor: IdentityExtractor,
        identity_verifier: IdentityVerifier,
        routing_validator: Arc<RoutingValidator>,
        lin_gate: Arc<LinearizabilityGate>,
    ) -> Self {
        Self {
            identity_extractor,
            identity_verifier,
            routing_validator,
            lin_gate,
        }
    }

    /// Intercept a request and create context.
    pub fn intercept(
        &self,
        identity: Option<ClientIdentity>,
        tenant_id: &str,
        kv_epoch: u64,
        linearizable: bool,
    ) -> LatticeResult<RequestContext> {
        // Validate epoch
        self.routing_validator.validate_epoch(tenant_id, kv_epoch)?;

        // Check linearizability if required
        if linearizable {
            self.lin_gate.enforce()?;
        }

        // Get or create identity
        let identity = match identity {
            Some(id) => id,
            None => self.identity_extractor.anonymous_identity()?,
        };

        // Create context with verified identity
        self.identity_verifier
            .create_context(identity, tenant_id, kv_epoch)
            .map(|ctx| ctx.with_linearizable(linearizable))
    }
}

/// etcd v3 gRPC adapter.
///
/// Implements the Adapter trait for the etcd protocol.
pub struct EtcdAdapter {
    /// Configuration.
    config: EtcdServiceConfig,

    /// Current state.
    state: RwLock<AdapterState>,

    /// gRPC listener.
    listener: RwLock<GrpcListener>,

    /// Identity extractor (used during TLS connection setup).
    identity_extractor: IdentityExtractor,

    /// Identity verifier (used for request authorization).
    identity_verifier: IdentityVerifier,

    /// Routing validator.
    routing_validator: Arc<RoutingValidator>,

    /// Linearizability gate.
    lin_gate: Arc<LinearizabilityGate>,

    /// Request interceptor.
    interceptor: RequestInterceptor,

    /// Statistics.
    stats: AdapterStats,
}

/// Adapter statistics.
struct AdapterStats {
    total_requests: AtomicU64,
    failed_requests: AtomicU64,
    last_error: RwLock<Option<String>>,
}

impl AdapterStats {
    fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            last_error: RwLock::new(None),
        }
    }

    fn record_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self, message: String) {
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
        *self.last_error.write().unwrap() = Some(message);
    }
}

impl EtcdAdapter {
    /// Create a new etcd adapter.
    pub fn new(config: EtcdServiceConfig) -> Self {
        let listener = GrpcListener::new(config.adapter.bind_addr, config.adapter.tls.clone());
        let identity_extractor = IdentityExtractor::new(config.adapter.require_mtls);
        let identity_verifier = IdentityVerifier::new(true);
        let routing_validator = Arc::new(RoutingValidator::new());
        let lin_gate = Arc::new(LinearizabilityGate::new());

        let interceptor = RequestInterceptor::new(
            IdentityExtractor::new(config.adapter.require_mtls),
            IdentityVerifier::new(true),
            routing_validator.clone(),
            lin_gate.clone(),
        );

        Self {
            config,
            state: RwLock::new(AdapterState::Created),
            listener: RwLock::new(listener),
            identity_extractor,
            identity_verifier,
            routing_validator,
            lin_gate,
            interceptor,
            stats: AdapterStats::new(),
        }
    }

    /// Get the routing validator.
    pub fn routing_validator(&self) -> &Arc<RoutingValidator> {
        &self.routing_validator
    }

    /// Get the linearizability gate.
    pub fn lin_gate(&self) -> &Arc<LinearizabilityGate> {
        &self.lin_gate
    }

    /// Get the request interceptor.
    pub fn interceptor(&self) -> &RequestInterceptor {
        &self.interceptor
    }

    /// Get the configuration.
    pub fn config(&self) -> &EtcdServiceConfig {
        &self.config
    }

    /// Get the identity extractor.
    pub fn identity_extractor(&self) -> &IdentityExtractor {
        &self.identity_extractor
    }

    /// Get the identity verifier.
    pub fn identity_verifier(&self) -> &IdentityVerifier {
        &self.identity_verifier
    }

    /// Register a tenant for routing.
    pub fn register_tenant(&self, routing: RoutingTable) {
        let tenant_id = routing.tenant_id.clone();
        self.routing_validator.register_tenant(&tenant_id, routing);
    }

    /// Process a request with context extraction and validation.
    pub fn process_request(
        &self,
        identity: Option<ClientIdentity>,
        tenant_id: &str,
        kv_epoch: u64,
        linearizable: bool,
    ) -> LatticeResult<RequestContext> {
        self.stats.record_request();

        match self
            .interceptor
            .intercept(identity, tenant_id, kv_epoch, linearizable)
        {
            Ok(ctx) => Ok(ctx),
            Err(e) => {
                self.stats.record_error(e.to_string());
                Err(e)
            }
        }
    }
}

impl Adapter for EtcdAdapter {
    fn name(&self) -> &'static str {
        "etcd"
    }

    fn state(&self) -> AdapterState {
        *self.state.read().unwrap()
    }

    fn health(&self) -> AdapterHealth {
        AdapterHealth {
            state: self.state(),
            active_connections: self.listener.read().unwrap().connection_count(),
            total_requests: self.stats.total_requests.load(Ordering::Relaxed),
            failed_requests: self.stats.failed_requests.load(Ordering::Relaxed),
            last_error: self.stats.last_error.read().unwrap().clone(),
        }
    }

    fn start(&self) -> Pin<Box<dyn Future<Output = LatticeResult<()>> + Send + '_>> {
        Box::pin(async move {
            *self.state.write().unwrap() = AdapterState::Starting;

            // Validate TLS configuration
            self.listener.read().unwrap().validate()?;

            // Mark listener as active
            self.listener.write().unwrap().activate();

            *self.state.write().unwrap() = AdapterState::Running;

            tracing::info!(
                bind_addr = %self.config.adapter.bind_addr,
                mtls = self.config.adapter.require_mtls,
                "etcd adapter started"
            );

            Ok(())
        })
    }

    fn stop(&self) -> Pin<Box<dyn Future<Output = LatticeResult<()>> + Send + '_>> {
        Box::pin(async move {
            *self.state.write().unwrap() = AdapterState::ShuttingDown;

            // Mark listener as inactive
            self.listener.write().unwrap().deactivate();

            *self.state.write().unwrap() = AdapterState::Stopped;

            tracing::info!(
                bind_addr = %self.config.adapter.bind_addr,
                "etcd adapter stopped"
            );

            Ok(())
        })
    }

    fn bind_addr(&self) -> SocketAddr {
        self.config.adapter.bind_addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_listener_tls() {
        let addr: SocketAddr = "127.0.0.1:2379".parse().unwrap();
        let listener = GrpcListener::new(addr, None);

        assert_eq!(listener.bind_addr(), addr);
        assert!(!listener.has_tls());
        assert!(!listener.has_mtls());
        assert_eq!(listener.connection_count(), 0);

        listener.add_connection();
        assert_eq!(listener.connection_count(), 1);

        listener.remove_connection();
        assert_eq!(listener.connection_count(), 0);
    }

    #[test]
    fn test_routing_validator() {
        let validator = RoutingValidator::new();

        let routing = RoutingTable::new("acme".to_string(), 1, 0, 4);
        validator.register_tenant("acme", routing);

        // Valid epoch
        assert!(validator.validate_epoch("acme", 1).is_ok());

        // Invalid epoch
        assert!(validator.validate_epoch("acme", 2).is_err());

        // Unknown tenant
        assert!(validator.validate_epoch("unknown", 1).is_err());
    }

    #[test]
    fn test_linearizability_gate() {
        let gate = LinearizabilityGate::new();

        assert!(gate.is_available());
        assert!(gate.enforce().is_ok());

        gate.set_unavailable(LinearizabilityFailureReason::NotLeader);
        assert!(!gate.is_available());
        assert!(gate.enforce().is_err());
        assert_eq!(
            gate.last_failure_reason(),
            Some(LinearizabilityFailureReason::NotLeader)
        );

        gate.set_available();
        assert!(gate.is_available());
        assert!(gate.enforce().is_ok());
    }

    #[test]
    fn test_etcd_adapter_state() {
        let config = EtcdServiceConfig::default();
        let adapter = EtcdAdapter::new(config);

        assert_eq!(adapter.name(), "etcd");
        assert_eq!(adapter.state(), AdapterState::Created);

        let health = adapter.health();
        assert_eq!(health.state, AdapterState::Created);
        assert_eq!(health.active_connections, 0);
        assert_eq!(health.total_requests, 0);
    }

    #[test]
    fn test_request_interceptor() {
        let routing_validator = Arc::new(RoutingValidator::new());
        let lin_gate = Arc::new(LinearizabilityGate::new());

        // Register tenant
        let routing = RoutingTable::new("acme".to_string(), 1, 0, 4);
        routing_validator.register_tenant("acme", routing);

        let interceptor = RequestInterceptor::new(
            IdentityExtractor::new(false), // Allow anonymous
            IdentityVerifier::new(true),
            routing_validator,
            lin_gate,
        );

        // Valid request
        let ctx = interceptor
            .intercept(None, "acme", 1, false)
            .expect("should succeed");
        assert_eq!(ctx.tenant_id, "acme");
        assert_eq!(ctx.kv_epoch, 1);

        // Invalid epoch
        assert!(interceptor.intercept(None, "acme", 2, false).is_err());
    }
}
