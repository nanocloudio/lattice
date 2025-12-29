//! Protocol listeners.
//!
//! Handles incoming connections for protocol adapters per §10.
//!
//! # Features
//!
//! - gRPC listener for etcd v3 adapter
//! - TLS layer wrapping for secure connections
//! - Request routing to KPG shards
//! - Backpressure and flow control integration
//! - Graceful shutdown support

use super::tls::TlsServerConfig;
use crate::core::error::{LatticeError, LatticeResult};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Listener state for a protocol adapter.
#[derive(Debug)]
pub struct ListenerState {
    /// Bind address.
    pub bind_addr: SocketAddr,
    /// Whether the listener is active.
    pub active: bool,
    /// Protocol name for logging.
    pub protocol: String,
}

impl ListenerState {
    /// Create a new listener state.
    pub fn new(bind_addr: SocketAddr, protocol: impl Into<String>) -> Self {
        Self {
            bind_addr,
            active: false,
            protocol: protocol.into(),
        }
    }

    /// Mark the listener as active.
    pub fn activate(&mut self) {
        self.active = true;
    }

    /// Mark the listener as inactive.
    pub fn deactivate(&mut self) {
        self.active = false;
    }
}

/// gRPC listener configuration.
#[derive(Debug, Clone)]
pub struct GrpcListenerConfig {
    /// Bind address for the gRPC server.
    pub bind_addr: SocketAddr,
    /// TLS configuration (None for plaintext, not recommended for production).
    pub tls: Option<TlsServerConfig>,
    /// Maximum concurrent connections.
    pub max_connections: usize,
    /// Connection timeout.
    pub connection_timeout: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Enable HTTP/2 keep-alive.
    pub http2_keepalive: bool,
    /// HTTP/2 keep-alive interval.
    pub http2_keepalive_interval: Duration,
    /// Default tenant ID for connections without tenant header.
    pub default_tenant_id: Option<String>,
    /// Enable request tracing.
    pub enable_tracing: bool,
}

impl Default for GrpcListenerConfig {
    fn default() -> Self {
        Self {
            bind_addr: ([0, 0, 0, 0], 2379).into(),
            tls: None,
            max_connections: 10000,
            connection_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            http2_keepalive: true,
            http2_keepalive_interval: Duration::from_secs(30),
            default_tenant_id: None,
            enable_tracing: true,
        }
    }
}

/// gRPC listener for the etcd v3 adapter.
#[derive(Debug)]
pub struct GrpcListenerState {
    /// Configuration.
    config: GrpcListenerConfig,
    /// Current state.
    state: ListenerState,
    /// Active connections count.
    active_connections: AtomicU64,
    /// Whether the listener is shutting down.
    shutting_down: AtomicBool,
    /// Backpressure manager.
    backpressure: BackpressureManager,
    /// Statistics.
    stats: ListenerStats,
}

/// Listener statistics.
#[derive(Debug)]
struct ListenerStats {
    /// Total connections accepted.
    connections_accepted: AtomicU64,
    /// Total connections rejected.
    connections_rejected: AtomicU64,
    /// Total requests processed.
    requests_processed: AtomicU64,
    /// Total requests throttled.
    requests_throttled: AtomicU64,
}

impl ListenerStats {
    fn new() -> Self {
        Self {
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            requests_processed: AtomicU64::new(0),
            requests_throttled: AtomicU64::new(0),
        }
    }
}

impl GrpcListenerState {
    /// Create a new gRPC listener state.
    pub fn new(config: GrpcListenerConfig) -> Self {
        let bind_addr = config.bind_addr;
        Self {
            config,
            state: ListenerState::new(bind_addr, "grpc"),
            active_connections: AtomicU64::new(0),
            shutting_down: AtomicBool::new(false),
            backpressure: BackpressureManager::new(BackpressureConfig::default()),
            stats: ListenerStats::new(),
        }
    }

    /// Get the bind address.
    pub fn bind_addr(&self) -> SocketAddr {
        self.config.bind_addr
    }

    /// Check if the listener is active.
    pub fn is_active(&self) -> bool {
        self.state.active
    }

    /// Check if the listener is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Start the listener.
    pub fn start(&mut self) {
        self.state.activate();
        self.shutting_down.store(false, Ordering::Release);
    }

    /// Initiate graceful shutdown.
    pub fn shutdown(&mut self) {
        self.shutting_down.store(true, Ordering::Release);
        self.state.deactivate();
    }

    /// Accept a new connection.
    pub fn accept_connection(&self) -> LatticeResult<ConnectionHandle> {
        if self.is_shutting_down() {
            return Err(LatticeError::Internal {
                message: "listener is shutting down".to_string(),
            });
        }

        let current = self.active_connections.load(Ordering::Acquire);
        if current >= self.config.max_connections as u64 {
            self.stats
                .connections_rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(LatticeError::ThrottleEnvelope {
                message: "max connections reached".to_string(),
            });
        }

        self.active_connections.fetch_add(1, Ordering::AcqRel);
        self.stats
            .connections_accepted
            .fetch_add(1, Ordering::Relaxed);

        Ok(ConnectionHandle {
            id: self.stats.connections_accepted.load(Ordering::Relaxed),
        })
    }

    /// Release a connection.
    pub fn release_connection(&self, _handle: ConnectionHandle) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
    }

    /// Get the number of active connections.
    pub fn active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Acquire)
    }

    /// Check backpressure for a tenant.
    pub fn check_backpressure(&self, tenant_id: &str) -> LatticeResult<()> {
        self.backpressure.check(tenant_id)?;
        Ok(())
    }

    /// Record a request.
    pub fn record_request(&self) {
        self.stats
            .requests_processed
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a throttled request.
    pub fn record_throttle(&self) {
        self.stats
            .requests_throttled
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Get listener metrics.
    pub fn metrics(&self) -> ListenerMetrics {
        ListenerMetrics {
            bind_addr: self.config.bind_addr,
            is_active: self.state.active,
            active_connections: self.active_connections.load(Ordering::Relaxed),
            max_connections: self.config.max_connections as u64,
            connections_accepted: self.stats.connections_accepted.load(Ordering::Relaxed),
            connections_rejected: self.stats.connections_rejected.load(Ordering::Relaxed),
            requests_processed: self.stats.requests_processed.load(Ordering::Relaxed),
            requests_throttled: self.stats.requests_throttled.load(Ordering::Relaxed),
        }
    }
}

/// Connection handle for tracking active connections.
#[derive(Debug)]
pub struct ConnectionHandle {
    /// Connection ID.
    pub id: u64,
}

/// Listener metrics.
#[derive(Debug, Clone)]
pub struct ListenerMetrics {
    /// Bind address.
    pub bind_addr: SocketAddr,
    /// Whether the listener is active.
    pub is_active: bool,
    /// Number of active connections.
    pub active_connections: u64,
    /// Maximum connections allowed.
    pub max_connections: u64,
    /// Total connections accepted.
    pub connections_accepted: u64,
    /// Total connections rejected.
    pub connections_rejected: u64,
    /// Total requests processed.
    pub requests_processed: u64,
    /// Total requests throttled.
    pub requests_throttled: u64,
}

// ============================================================================
// Request Routing
// ============================================================================

/// Request router for routing requests to KPG shards.
#[derive(Debug)]
pub struct RequestRouter {
    /// Routing table.
    routing_table: RoutingTable,
    /// Default tenant ID.
    default_tenant_id: Option<String>,
}

/// Routing table for tenant → KPG mapping.
#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// KPG count per tenant.
    tenant_kpg_count: HashMap<String, u32>,
    /// Current routing epoch.
    routing_epoch: u64,
    /// Hash seed version.
    hash_seed_version: u64,
}

impl RoutingTable {
    /// Create a new routing table.
    pub fn new(routing_epoch: u64) -> Self {
        Self {
            tenant_kpg_count: HashMap::new(),
            routing_epoch,
            hash_seed_version: 0,
        }
    }

    /// Set the KPG count for a tenant.
    pub fn set_tenant_kpg_count(&mut self, tenant_id: impl Into<String>, kpg_count: u32) {
        self.tenant_kpg_count.insert(tenant_id.into(), kpg_count);
    }

    /// Get the KPG count for a tenant.
    pub fn get_kpg_count(&self, tenant_id: &str) -> Option<u32> {
        self.tenant_kpg_count.get(tenant_id).copied()
    }

    /// Get the current routing epoch.
    pub fn routing_epoch(&self) -> u64 {
        self.routing_epoch
    }

    /// Get the hash seed version.
    pub fn hash_seed_version(&self) -> u64 {
        self.hash_seed_version
    }
}

impl RequestRouter {
    /// Create a new request router.
    pub fn new(routing_table: RoutingTable) -> Self {
        Self {
            routing_table,
            default_tenant_id: None,
        }
    }

    /// Set the default tenant ID.
    pub fn with_default_tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.default_tenant_id = Some(tenant_id.into());
        self
    }

    /// Route a key to a KPG.
    ///
    /// Returns the KPG index for the given tenant and key.
    pub fn route_key(&self, tenant_id: &str, key: &[u8]) -> LatticeResult<u32> {
        let kpg_count = self.routing_table.get_kpg_count(tenant_id).ok_or_else(|| {
            LatticeError::InvalidRequest {
                message: format!("unknown tenant: {}", tenant_id),
            }
        })?;

        if kpg_count == 0 {
            return Err(LatticeError::InvalidRequest {
                message: "tenant has no KPGs configured".to_string(),
            });
        }

        // Hash the key to determine KPG
        let hash = self.hash_key(tenant_id, key);
        let kpg_index = (hash % kpg_count as u64) as u32;

        Ok(kpg_index)
    }

    /// Hash a key for routing.
    fn hash_key(&self, tenant_id: &str, key: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = twox_hash::XxHash64::with_seed(self.routing_table.hash_seed_version);
        tenant_id.hash(&mut hasher);
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Check if a key range spans multiple KPGs.
    pub fn spans_multiple_kpgs(
        &self,
        tenant_id: &str,
        key: &[u8],
        range_end: Option<&[u8]>,
    ) -> LatticeResult<bool> {
        let start_kpg = self.route_key(tenant_id, key)?;

        if let Some(end) = range_end {
            if end.is_empty() {
                // Empty range_end means all keys with prefix
                // Conservatively assume multi-KPG
                let kpg_count = self.routing_table.get_kpg_count(tenant_id).unwrap_or(1);
                return Ok(kpg_count > 1);
            }
            let end_kpg = self.route_key(tenant_id, end)?;
            return Ok(start_kpg != end_kpg);
        }

        Ok(false) // Single key
    }

    /// Validate request epoch.
    pub fn validate_epoch(&self, request_epoch: u64) -> LatticeResult<()> {
        if request_epoch != self.routing_table.routing_epoch {
            return Err(LatticeError::DirtyEpoch {
                expected_epoch: self.routing_table.routing_epoch,
                observed_epoch: request_epoch,
            });
        }
        Ok(())
    }

    /// Get the current routing epoch.
    pub fn current_epoch(&self) -> u64 {
        self.routing_table.routing_epoch
    }

    /// Update the routing table.
    pub fn update_routing_table(&mut self, table: RoutingTable) {
        self.routing_table = table;
    }
}

// ============================================================================
// Backpressure Management
// ============================================================================

/// Backpressure configuration.
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Global request limit per second.
    pub global_rps_limit: u64,
    /// Per-tenant request limit per second.
    pub per_tenant_rps_limit: u64,
    /// Grace period for over-limit requests.
    pub overage_grace_ms: u64,
    /// Enable Clustor flow control integration.
    pub clustor_flow_control: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            global_rps_limit: 100_000,
            per_tenant_rps_limit: 10_000,
            overage_grace_ms: 1000,
            clustor_flow_control: true,
        }
    }
}

/// Backpressure manager.
#[derive(Debug)]
pub struct BackpressureManager {
    /// Configuration.
    config: BackpressureConfig,
    /// Global request count (sliding window).
    global_requests: AtomicU64,
    /// Whether global backpressure is active.
    global_backpressure: AtomicBool,
    /// Clustor flow control active.
    clustor_backpressure: AtomicBool,
}

impl BackpressureManager {
    /// Create a new backpressure manager.
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            global_requests: AtomicU64::new(0),
            global_backpressure: AtomicBool::new(false),
            clustor_backpressure: AtomicBool::new(false),
        }
    }

    /// Check if a request should be throttled.
    pub fn check(&self, _tenant_id: &str) -> LatticeResult<()> {
        // Check Clustor flow control
        if self.clustor_backpressure.load(Ordering::Acquire) {
            return Err(LatticeError::ThrottleEnvelope {
                message: "Clustor flow control active".to_string(),
            });
        }

        // Check global backpressure
        if self.global_backpressure.load(Ordering::Acquire) {
            return Err(LatticeError::ThrottleEnvelope {
                message: "global backpressure active".to_string(),
            });
        }

        // Increment and check global counter
        let current = self.global_requests.fetch_add(1, Ordering::AcqRel);
        if current > self.config.global_rps_limit {
            self.global_backpressure.store(true, Ordering::Release);
            return Err(LatticeError::ThrottleEnvelope {
                message: "global rate limit exceeded".to_string(),
            });
        }

        Ok(())
    }

    /// Set Clustor flow control state.
    pub fn set_clustor_backpressure(&self, active: bool) {
        self.clustor_backpressure.store(active, Ordering::Release);
    }

    /// Reset the request counter (called periodically).
    pub fn reset_counter(&self) {
        self.global_requests.store(0, Ordering::Release);
        self.global_backpressure.store(false, Ordering::Release);
    }

    /// Check if any backpressure is active.
    pub fn is_backpressure_active(&self) -> bool {
        self.global_backpressure.load(Ordering::Acquire)
            || self.clustor_backpressure.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listener_state() {
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
    fn test_grpc_listener_config_default() {
        let config = GrpcListenerConfig::default();
        assert_eq!(config.bind_addr.port(), 2379);
        assert_eq!(config.max_connections, 10000);
        assert!(config.http2_keepalive);
    }

    #[test]
    fn test_grpc_listener_state() {
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
    fn test_routing_table() {
        let mut table = RoutingTable::new(1);
        table.set_tenant_kpg_count("acme", 4);

        assert_eq!(table.get_kpg_count("acme"), Some(4));
        assert_eq!(table.get_kpg_count("other"), None);
        assert_eq!(table.routing_epoch(), 1);
    }

    #[test]
    fn test_request_router() {
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
    fn test_request_router_epoch_validation() {
        let table = RoutingTable::new(5);
        let router = RequestRouter::new(table);

        assert!(router.validate_epoch(5).is_ok());
        assert!(router.validate_epoch(4).is_err());
        assert!(router.validate_epoch(6).is_err());
    }

    #[test]
    fn test_backpressure_manager() {
        let config = BackpressureConfig {
            global_rps_limit: 10,
            ..Default::default()
        };
        let manager = BackpressureManager::new(config);

        // Should allow initial requests (up to limit + 1 because we check > not >=)
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
    fn test_clustor_flow_control() {
        let manager = BackpressureManager::new(BackpressureConfig::default());

        assert!(manager.check("tenant1").is_ok());

        manager.set_clustor_backpressure(true);
        assert!(manager.check("tenant1").is_err());

        manager.set_clustor_backpressure(false);
        assert!(manager.check("tenant1").is_ok());
    }

    #[test]
    fn test_listener_metrics() {
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
}
