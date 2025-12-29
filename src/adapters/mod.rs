//! Protocol adapters.
//!
//! Adapters translate external protocol semantics into Lattice KV primitives
//! and Clustor durability/read gates. Each adapter enforces LIN-BOUND and
//! KVSOURCE invariants.
//!
//! # Architecture
//!
//! Each adapter implements the [`Adapter`] trait to provide a common lifecycle
//! interface while handling protocol-specific details internally:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       Protocol Adapters                         │
//! ├─────────────────┬─────────────────────┬─────────────────────────┤
//! │  etcd v3 gRPC   │  Redis (future)     │  Memcached (future)     │
//! │                 │                     │                         │
//! │  - KV           │  - GET/SET/DEL      │  - get/set/delete       │
//! │  - Watch        │  - WATCH            │  - gets/cas             │
//! │  - Lease        │  - EXPIRE/TTL       │  - touch                │
//! │  - Txn          │  - MULTI/EXEC       │                         │
//! └─────────────────┴─────────────────────┴─────────────────────────┘
//! ```
//!
//! # Invariants
//!
//! All adapters MUST enforce:
//! - **LIN-BOUND**: Linearizable ops require Clustor ReadIndex eligibility
//! - **KVSOURCE**: All KV effects derive from WAL entries
//! - **Epoch validation**: Reject requests with stale kv_epoch
//!
//! Currently implemented:
//! - [`etcd`] - etcd v3 gRPC adapter
//! - [`redis`] - Redis RESP2/RESP3 adapter
//! - [`memcached`] - Memcached ASCII/binary adapter

use crate::control::routing::RoutingTable;
use crate::core::error::LatticeResult;
use crate::net::security::ClientIdentity;
use crate::net::tls::TlsConfig;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

pub mod etcd;
pub mod memcached;
pub mod redis;
pub mod tcp;

/// Request context propagated through adapter processing.
///
/// Contains identity, routing, and tracing information extracted
/// from the incoming request.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Client identity from mTLS certificate.
    pub identity: Option<ClientIdentity>,

    /// Tenant ID for this request.
    pub tenant_id: String,

    /// Request's kv_epoch for routing validation.
    pub kv_epoch: u64,

    /// Trace ID for distributed tracing.
    pub trace_id: Option<String>,

    /// Span ID for distributed tracing.
    pub span_id: Option<String>,

    /// Whether linearizable read was requested.
    pub linearizable: bool,
}

impl RequestContext {
    /// Create a new request context.
    pub fn new(tenant_id: impl Into<String>, kv_epoch: u64) -> Self {
        Self {
            identity: None,
            tenant_id: tenant_id.into(),
            kv_epoch,
            trace_id: None,
            span_id: None,
            linearizable: true, // Default to linearizable
        }
    }

    /// Set client identity.
    pub fn with_identity(mut self, identity: ClientIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Set trace context.
    pub fn with_trace(mut self, trace_id: String, span_id: String) -> Self {
        self.trace_id = Some(trace_id);
        self.span_id = Some(span_id);
        self
    }

    /// Set linearizable flag.
    pub fn with_linearizable(mut self, linearizable: bool) -> Self {
        self.linearizable = linearizable;
        self
    }

    /// Validate this context against a routing table.
    pub fn validate(&self, routing: &RoutingTable) -> LatticeResult<()> {
        routing.validate_epoch(self.kv_epoch)
    }
}

/// Adapter lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterState {
    /// Adapter is created but not started.
    Created,
    /// Adapter is starting up.
    Starting,
    /// Adapter is running and accepting connections.
    Running,
    /// Adapter is shutting down gracefully.
    ShuttingDown,
    /// Adapter has stopped.
    Stopped,
    /// Adapter encountered a fatal error.
    Failed,
}

/// Adapter configuration common to all protocol adapters.
#[derive(Debug, Clone)]
pub struct AdapterConfig {
    /// Bind address for the adapter.
    pub bind_addr: SocketAddr,

    /// TLS configuration (required for production).
    pub tls: Option<TlsConfig>,

    /// Whether mTLS is required.
    pub require_mtls: bool,

    /// Maximum concurrent connections.
    pub max_connections: usize,

    /// Connection timeout in milliseconds.
    pub connection_timeout_ms: u64,

    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,
}

impl Default for AdapterConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:2379".parse().unwrap(),
            tls: None,
            require_mtls: true,
            max_connections: 10_000,
            connection_timeout_ms: 30_000,
            request_timeout_ms: 60_000,
        }
    }
}

/// Adapter health status.
#[derive(Debug, Clone)]
pub struct AdapterHealth {
    /// Current state.
    pub state: AdapterState,

    /// Number of active connections.
    pub active_connections: usize,

    /// Total requests processed.
    pub total_requests: u64,

    /// Failed requests.
    pub failed_requests: u64,

    /// Last error message (if any).
    pub last_error: Option<String>,
}

/// Common interface for all protocol adapters.
///
/// Each adapter translates its protocol semantics to Lattice KV operations
/// while enforcing LIN-BOUND, KVSOURCE, and epoch validation invariants.
pub trait Adapter: Send + Sync {
    /// Get the adapter name (e.g., "etcd", "redis", "memcached").
    fn name(&self) -> &'static str;

    /// Get the current adapter state.
    fn state(&self) -> AdapterState;

    /// Get health status.
    fn health(&self) -> AdapterHealth;

    /// Start the adapter.
    ///
    /// This should bind to the configured address and begin accepting
    /// connections. Returns a future that completes when startup is done.
    fn start(&self) -> Pin<Box<dyn Future<Output = LatticeResult<()>> + Send + '_>>;

    /// Stop the adapter gracefully.
    ///
    /// This should stop accepting new connections and drain existing ones.
    fn stop(&self) -> Pin<Box<dyn Future<Output = LatticeResult<()>> + Send + '_>>;

    /// Get the bind address.
    fn bind_addr(&self) -> SocketAddr;
}

/// Handle to a running adapter for management operations.
pub struct AdapterHandle {
    /// The adapter instance.
    adapter: Arc<dyn Adapter>,
}

impl AdapterHandle {
    /// Create a new adapter handle.
    pub fn new(adapter: Arc<dyn Adapter>) -> Self {
        Self { adapter }
    }

    /// Get the adapter name.
    pub fn name(&self) -> &'static str {
        self.adapter.name()
    }

    /// Get the adapter state.
    pub fn state(&self) -> AdapterState {
        self.adapter.state()
    }

    /// Get health status.
    pub fn health(&self) -> AdapterHealth {
        self.adapter.health()
    }

    /// Get the underlying adapter.
    pub fn adapter(&self) -> &Arc<dyn Adapter> {
        &self.adapter
    }
}

/// Adapter registry for managing multiple protocol adapters.
pub struct AdapterRegistry {
    /// Registered adapters.
    adapters: Vec<AdapterHandle>,
}

impl AdapterRegistry {
    /// Create a new adapter registry.
    pub fn new() -> Self {
        Self {
            adapters: Vec::new(),
        }
    }

    /// Register an adapter.
    pub fn register(&mut self, adapter: Arc<dyn Adapter>) {
        self.adapters.push(AdapterHandle::new(adapter));
    }

    /// Get all registered adapters.
    pub fn adapters(&self) -> &[AdapterHandle] {
        &self.adapters
    }

    /// Find an adapter by name.
    pub fn find(&self, name: &str) -> Option<&AdapterHandle> {
        self.adapters.iter().find(|h| h.name() == name)
    }

    /// Start all registered adapters.
    pub async fn start_all(&self) -> LatticeResult<()> {
        for handle in &self.adapters {
            handle.adapter.start().await?;
        }
        Ok(())
    }

    /// Stop all registered adapters.
    pub async fn stop_all(&self) -> LatticeResult<()> {
        for handle in &self.adapters {
            handle.adapter.stop().await?;
        }
        Ok(())
    }
}

impl Default for AdapterRegistry {
    fn default() -> Self {
        Self::new()
    }
}
