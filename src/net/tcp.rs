//! TCP listener abstraction.
//!
//! Provides a shared TCP listener implementation for protocol adapters
//! that use raw TCP connections (Redis, Memcached) rather than gRPC.

use crate::core::error::{LatticeError, LatticeResult};
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::sync::watch;

#[cfg(feature = "tcp-tls")]
use tokio_rustls::{server::TlsStream, TlsAcceptor};

/// TCP listener configuration.
#[derive(Debug, Clone)]
pub struct TcpListenerConfig {
    /// Bind address.
    pub bind_addr: SocketAddr,

    /// Maximum concurrent connections.
    pub max_connections: usize,

    /// Connection idle timeout.
    pub idle_timeout: Duration,

    /// Read buffer size.
    pub read_buffer_size: usize,

    /// Write buffer size.
    pub write_buffer_size: usize,

    /// TCP nodelay (disable Nagle's algorithm).
    pub nodelay: bool,

    /// TCP keepalive interval.
    pub keepalive: Option<Duration>,
}

impl Default for TcpListenerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:6379".parse().unwrap(),
            max_connections: 10_000,
            idle_timeout: Duration::from_secs(300),
            read_buffer_size: 64 * 1024,
            write_buffer_size: 64 * 1024,
            nodelay: true,
            keepalive: Some(Duration::from_secs(60)),
        }
    }
}

/// TCP listener state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpListenerState {
    /// Not yet started.
    Created,
    /// Binding to address.
    Binding,
    /// Accepting connections.
    Listening,
    /// Shutting down.
    ShuttingDown,
    /// Stopped.
    Stopped,
}

/// Connection identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conn-{}", self.0)
    }
}

/// TCP connection wrapper with metadata.
pub struct TcpConnection {
    /// Connection ID.
    pub id: ConnectionId,

    /// Remote address.
    pub remote_addr: SocketAddr,

    /// Underlying TCP stream.
    stream: TcpStream,

    /// Connection creation time.
    pub created_at: std::time::Instant,
}

impl TcpConnection {
    /// Create a new TCP connection.
    pub fn new(id: ConnectionId, stream: TcpStream, remote_addr: SocketAddr) -> Self {
        Self {
            id,
            remote_addr,
            stream,
            created_at: std::time::Instant::now(),
        }
    }

    /// Get the underlying stream.
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// Take the underlying stream.
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }

    /// Split into read and write halves.
    pub fn into_split(
        self,
    ) -> (
        tokio::net::tcp::OwnedReadHalf,
        tokio::net::tcp::OwnedWriteHalf,
    ) {
        self.stream.into_split()
    }
}

/// TLS-wrapped TCP connection.
#[cfg(feature = "tcp-tls")]
pub struct TlsTcpConnection {
    /// Connection ID.
    pub id: ConnectionId,

    /// Remote address.
    pub remote_addr: SocketAddr,

    /// Underlying TLS stream.
    stream: TlsStream<TcpStream>,

    /// Connection creation time.
    pub created_at: std::time::Instant,
}

#[cfg(feature = "tcp-tls")]
impl TlsTcpConnection {
    /// Create a new TLS TCP connection.
    pub fn new(id: ConnectionId, stream: TlsStream<TcpStream>, remote_addr: SocketAddr) -> Self {
        Self {
            id,
            remote_addr,
            stream,
            created_at: std::time::Instant::now(),
        }
    }

    /// Get the underlying stream.
    pub fn stream(&self) -> &TlsStream<TcpStream> {
        &self.stream
    }

    /// Take the underlying stream.
    pub fn into_stream(self) -> TlsStream<TcpStream> {
        self.stream
    }

    /// Split into read and write halves.
    pub fn into_split(
        self,
    ) -> (
        tokio::io::ReadHalf<TlsStream<TcpStream>>,
        tokio::io::WriteHalf<TlsStream<TcpStream>>,
    ) {
        tokio::io::split(self.stream)
    }
}

/// TCP listener metrics.
#[derive(Debug, Default)]
pub struct TcpListenerMetrics {
    /// Total connections accepted.
    pub connections_total: AtomicU64,

    /// Currently active connections.
    pub connections_active: AtomicU64,

    /// Connections rejected (at capacity).
    pub connections_rejected: AtomicU64,

    /// Total bytes received.
    pub bytes_received: AtomicU64,

    /// Total bytes sent.
    pub bytes_sent: AtomicU64,
}

impl TcpListenerMetrics {
    /// Record a new connection.
    pub fn connection_accepted(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a connection closed.
    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record a rejected connection.
    pub fn connection_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record bytes received.
    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes sent.
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Get active connection count.
    pub fn active_connections(&self) -> u64 {
        self.connections_active.load(Ordering::Relaxed)
    }
}

/// TCP listener for accepting connections.
pub struct TcpListener {
    /// Configuration.
    config: TcpListenerConfig,

    /// Current state.
    state: RwLock<TcpListenerState>,

    /// Next connection ID.
    next_conn_id: AtomicU64,

    /// Shutdown flag.
    shutting_down: AtomicBool,

    /// Shutdown signal receiver.
    shutdown_rx: watch::Receiver<bool>,

    /// Metrics.
    metrics: Arc<TcpListenerMetrics>,

    /// TLS acceptor (if configured).
    #[cfg(feature = "tcp-tls")]
    tls_acceptor: Option<TlsAcceptor>,
}

impl TcpListener {
    /// Create a new TCP listener.
    pub fn new(config: TcpListenerConfig, shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            config,
            state: RwLock::new(TcpListenerState::Created),
            next_conn_id: AtomicU64::new(1),
            shutting_down: AtomicBool::new(false),
            shutdown_rx,
            metrics: Arc::new(TcpListenerMetrics::default()),
            #[cfg(feature = "tcp-tls")]
            tls_acceptor: None,
        }
    }

    /// Create a new TCP listener with TLS.
    #[cfg(feature = "tcp-tls")]
    pub fn with_tls(
        config: TcpListenerConfig,
        shutdown_rx: watch::Receiver<bool>,
        tls_acceptor: TlsAcceptor,
    ) -> Self {
        Self {
            config,
            state: RwLock::new(TcpListenerState::Created),
            next_conn_id: AtomicU64::new(1),
            shutting_down: AtomicBool::new(false),
            shutdown_rx,
            metrics: Arc::new(TcpListenerMetrics::default()),
            tls_acceptor: Some(tls_acceptor),
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &TcpListenerConfig {
        &self.config
    }

    /// Get the current state.
    pub fn state(&self) -> TcpListenerState {
        *self.state.read()
    }

    /// Get metrics.
    pub fn metrics(&self) -> &Arc<TcpListenerMetrics> {
        &self.metrics
    }

    /// Check if at connection capacity.
    pub fn at_capacity(&self) -> bool {
        self.metrics.active_connections() >= self.config.max_connections as u64
    }

    /// Generate next connection ID.
    fn next_connection_id(&self) -> ConnectionId {
        ConnectionId(self.next_conn_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Bind and start listening.
    pub async fn bind(&self) -> LatticeResult<TokioTcpListener> {
        *self.state.write() = TcpListenerState::Binding;

        let listener = TokioTcpListener::bind(self.config.bind_addr)
            .await
            .map_err(|e| LatticeError::Internal {
                message: format!("failed to bind to {}: {}", self.config.bind_addr, e),
            })?;

        *self.state.write() = TcpListenerState::Listening;

        Ok(listener)
    }

    /// Accept a connection (plaintext).
    pub async fn accept(
        &self,
        listener: &TokioTcpListener,
    ) -> LatticeResult<Option<TcpConnection>> {
        if self.shutting_down.load(Ordering::Relaxed) {
            return Ok(None);
        }

        if self.at_capacity() {
            self.metrics.connection_rejected();
            return Err(LatticeError::Internal {
                message: "connection limit reached".to_string(),
            });
        }

        let mut shutdown_rx = self.shutdown_rx.clone();

        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        // Configure TCP options
                        if self.config.nodelay {
                            let _ = stream.set_nodelay(true);
                        }

                        let id = self.next_connection_id();
                        self.metrics.connection_accepted();

                        Ok(Some(TcpConnection::new(id, stream, addr)))
                    }
                    Err(e) => Err(LatticeError::Internal {
                        message: format!("accept failed: {}", e),
                    }),
                }
            }
            _ = shutdown_rx.changed() => {
                Ok(None)
            }
        }
    }

    /// Accept a connection with TLS handshake.
    #[cfg(feature = "tcp-tls")]
    pub async fn accept_tls(
        &self,
        listener: &TokioTcpListener,
    ) -> LatticeResult<Option<TlsTcpConnection>> {
        let tls_acceptor = self
            .tls_acceptor
            .as_ref()
            .ok_or_else(|| LatticeError::Internal {
                message: "TLS not configured".to_string(),
            })?;

        if self.shutting_down.load(Ordering::Relaxed) {
            return Ok(None);
        }

        if self.at_capacity() {
            self.metrics.connection_rejected();
            return Err(LatticeError::Internal {
                message: "connection limit reached".to_string(),
            });
        }

        let mut shutdown_rx = self.shutdown_rx.clone();

        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        // Configure TCP options before TLS handshake
                        if self.config.nodelay {
                            let _ = stream.set_nodelay(true);
                        }

                        // Perform TLS handshake
                        let tls_stream = tls_acceptor
                            .accept(stream)
                            .await
                            .map_err(|e| LatticeError::Internal {
                                message: format!("TLS handshake failed: {}", e),
                            })?;

                        let id = self.next_connection_id();
                        self.metrics.connection_accepted();

                        Ok(Some(TlsTcpConnection::new(id, tls_stream, addr)))
                    }
                    Err(e) => Err(LatticeError::Internal {
                        message: format!("accept failed: {}", e),
                    }),
                }
            }
            _ = shutdown_rx.changed() => {
                Ok(None)
            }
        }
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Relaxed);
        *self.state.write() = TcpListenerState::ShuttingDown;
    }

    /// Mark as stopped.
    pub fn stopped(&self) {
        *self.state.write() = TcpListenerState::Stopped;
    }

    /// Record connection closed.
    pub fn connection_closed(&self) {
        self.metrics.connection_closed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_listener_config_default() {
        let config = TcpListenerConfig::default();
        assert_eq!(config.max_connections, 10_000);
        assert_eq!(config.idle_timeout, Duration::from_secs(300));
        assert!(config.nodelay);
    }

    #[test]
    fn test_connection_id_display() {
        let id = ConnectionId(42);
        assert_eq!(format!("{}", id), "conn-42");
    }

    #[test]
    fn test_tcp_listener_metrics() {
        let metrics = TcpListenerMetrics::default();

        metrics.connection_accepted();
        assert_eq!(metrics.active_connections(), 1);
        assert_eq!(metrics.connections_total.load(Ordering::Relaxed), 1);

        metrics.connection_accepted();
        assert_eq!(metrics.active_connections(), 2);

        metrics.connection_closed();
        assert_eq!(metrics.active_connections(), 1);

        metrics.connection_rejected();
        assert_eq!(metrics.connections_rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_tcp_listener_state() {
        let (_tx, rx) = watch::channel(false);
        let config = TcpListenerConfig::default();
        let listener = TcpListener::new(config, rx);

        assert_eq!(listener.state(), TcpListenerState::Created);
        assert!(!listener.at_capacity());
    }

    #[tokio::test]
    async fn test_tcp_listener_bind() {
        let (_tx, rx) = watch::channel(false);
        let config = TcpListenerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(), // OS assigns port
            ..Default::default()
        };
        let listener = TcpListener::new(config, rx);

        let tcp_listener = listener.bind().await.unwrap();
        assert_eq!(listener.state(), TcpListenerState::Listening);

        let local_addr = tcp_listener.local_addr().unwrap();
        assert!(local_addr.port() > 0);
    }

    #[test]
    fn test_tcp_listener_shutdown() {
        let (_tx, rx) = watch::channel(false);
        let config = TcpListenerConfig::default();
        let listener = TcpListener::new(config, rx);

        listener.shutdown();
        assert_eq!(listener.state(), TcpListenerState::ShuttingDown);

        listener.stopped();
        assert_eq!(listener.state(), TcpListenerState::Stopped);
    }
}
