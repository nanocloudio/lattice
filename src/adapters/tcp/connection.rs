//! TCP connection handling.
//!
//! Provides connection abstraction for protocol adapters.

use super::codec::ProtocolCodec;
use super::{BackpressureState, ConnectionBackpressure, DecodeResult, ReadBuffer};
use crate::adapters::RequestContext;
use crate::core::error::{LatticeError, LatticeResult};
use crate::net::tcp::ConnectionId;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is active.
    Active,
    /// Connection is in transaction mode (Redis MULTI).
    InTransaction,
    /// Connection is in Pub/Sub mode.
    InPubSub,
    /// Connection is closing.
    Closing,
    /// Connection is closed.
    Closed,
}

/// Connection statistics.
#[derive(Debug, Default)]
pub struct ConnectionStats {
    /// Commands processed.
    pub commands_processed: AtomicU64,
    /// Commands failed.
    pub commands_failed: AtomicU64,
    /// Bytes received.
    pub bytes_received: AtomicU64,
    /// Bytes sent.
    pub bytes_sent: AtomicU64,
}

impl ConnectionStats {
    /// Record a command processed.
    pub fn command_processed(&self) {
        self.commands_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a command failed.
    pub fn command_failed(&self) {
        self.commands_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record bytes received.
    pub fn record_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes sent.
    pub fn record_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Protocol connection context.
///
/// This holds the state and metadata for a single client connection.
pub struct ProtocolConnection<C: ProtocolCodec> {
    /// Connection ID.
    pub id: ConnectionId,

    /// Remote address.
    pub remote_addr: SocketAddr,

    /// Connection state.
    pub state: ConnectionState,

    /// Request context (tenant, identity, etc.).
    pub context: RequestContext,

    /// Protocol codec.
    pub codec: C,

    /// Read buffer.
    pub read_buffer: ReadBuffer,

    /// Write buffer.
    pub write_buffer: BytesMut,

    /// Backpressure manager.
    pub backpressure: ConnectionBackpressure,

    /// Connection statistics.
    pub stats: Arc<ConnectionStats>,

    /// Connection name (set by CLIENT SETNAME).
    pub name: Option<String>,

    /// Connection creation time.
    pub created_at: Instant,

    /// Last activity time.
    pub last_activity: Instant,

    /// Idle timeout.
    pub idle_timeout: Duration,
}

impl<C: ProtocolCodec> ProtocolConnection<C> {
    /// Create a new protocol connection.
    pub fn new(
        id: ConnectionId,
        remote_addr: SocketAddr,
        context: RequestContext,
        codec: C,
        idle_timeout: Duration,
    ) -> Self {
        let now = Instant::now();
        Self {
            id,
            remote_addr,
            state: ConnectionState::Active,
            context,
            codec,
            read_buffer: ReadBuffer::default(),
            write_buffer: BytesMut::with_capacity(64 * 1024),
            backpressure: ConnectionBackpressure::default(),
            stats: Arc::new(ConnectionStats::default()),
            name: None,
            created_at: now,
            last_activity: now,
            idle_timeout,
        }
    }

    /// Check if connection has timed out.
    pub fn is_timed_out(&self) -> bool {
        self.last_activity.elapsed() > self.idle_timeout
    }

    /// Update last activity timestamp.
    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Check backpressure state.
    pub fn check_backpressure(&self) -> BackpressureState {
        self.backpressure.state()
    }

    /// Try to read and decode a request.
    pub fn try_decode(&mut self) -> DecodeResult<C::Request> {
        let mut buffer = BytesMut::from(self.read_buffer.data());
        let result = self.codec.decode(&mut buffer);

        // If complete, consume the bytes from read buffer
        if let DecodeResult::Complete(_) = &result {
            let consumed = self.read_buffer.len() - buffer.len();
            self.read_buffer.consume(consumed);
        }

        result
    }

    /// Encode a response to the write buffer.
    pub fn encode_response(&mut self, response: &C::Response) -> LatticeResult<()> {
        match self.codec.encode(response) {
            super::EncodeResult::Ok(bytes) => {
                self.write_buffer.extend_from_slice(&bytes);
                Ok(())
            }
            super::EncodeResult::Error(e) => Err(LatticeError::Internal { message: e }),
        }
    }

    /// Get connection age.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Get idle time.
    pub fn idle_time(&self) -> Duration {
        self.last_activity.elapsed()
    }
}

/// Read data into a connection's buffer.
pub async fn read_into_buffer<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    buffer: &mut ReadBuffer,
    max_read: usize,
) -> LatticeResult<usize> {
    let mut tmp = vec![0u8; max_read.min(buffer.remaining_capacity())];
    let n = reader
        .read(&mut tmp)
        .await
        .map_err(|e| LatticeError::Internal {
            message: format!("read error: {}", e),
        })?;

    if n == 0 {
        return Err(LatticeError::Internal {
            message: "connection closed".to_string(),
        });
    }

    buffer
        .extend(&tmp[..n])
        .map_err(|e| LatticeError::Internal { message: e })?;

    Ok(n)
}

/// Flush write buffer to the connection.
pub async fn flush_write_buffer<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    buffer: &mut BytesMut,
) -> LatticeResult<usize> {
    if buffer.is_empty() {
        return Ok(0);
    }

    let n = writer
        .write_all(buffer)
        .await
        .map(|_| buffer.len())
        .map_err(|e| LatticeError::Internal {
            message: format!("write error: {}", e),
        })?;

    buffer.clear();
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock codec for testing
    struct MockCodec;

    impl ProtocolCodec for MockCodec {
        type Request = String;
        type Response = String;

        fn decode(&self, buffer: &mut BytesMut) -> DecodeResult<Self::Request> {
            if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                let line = buffer.split_to(pos + 1);
                let s = String::from_utf8_lossy(&line[..line.len() - 1]).to_string();
                DecodeResult::Complete(s)
            } else {
                DecodeResult::Incomplete
            }
        }

        fn encode(&self, response: &Self::Response) -> super::super::EncodeResult {
            super::super::EncodeResult::Ok(format!("{}\n", response).into())
        }

        fn protocol_name(&self) -> &'static str {
            "mock"
        }
    }

    #[test]
    fn test_connection_state() {
        let context = RequestContext::new("default", 1);
        let conn: ProtocolConnection<MockCodec> = ProtocolConnection::new(
            ConnectionId(1),
            "127.0.0.1:12345".parse().unwrap(),
            context,
            MockCodec,
            Duration::from_secs(300),
        );

        assert_eq!(conn.state, ConnectionState::Active);
        assert!(conn.name.is_none());
    }

    #[test]
    fn test_connection_timeout() {
        let context = RequestContext::new("default", 1);
        let mut conn: ProtocolConnection<MockCodec> = ProtocolConnection::new(
            ConnectionId(1),
            "127.0.0.1:12345".parse().unwrap(),
            context,
            MockCodec,
            Duration::from_millis(1),
        );

        // Should timeout after 1ms
        std::thread::sleep(Duration::from_millis(10));
        assert!(conn.is_timed_out());

        // Touch should reset
        conn.touch();
        assert!(!conn.is_timed_out());
    }

    #[test]
    fn test_connection_stats() {
        let stats = ConnectionStats::default();

        stats.command_processed();
        stats.command_processed();
        stats.command_failed();

        assert_eq!(stats.commands_processed.load(Ordering::Relaxed), 2);
        assert_eq!(stats.commands_failed.load(Ordering::Relaxed), 1);
    }
}
