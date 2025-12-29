//! Shared TCP adapter utilities.
//!
//! This module provides common abstractions for TCP-based protocol adapters
//! (Redis, Memcached) including connection handling, codecs, and pipelining.

pub mod codec;
pub mod connection;
pub mod pipeline;

use bytes::{Bytes, BytesMut};

/// Result of decoding a frame from the wire.
#[derive(Debug)]
pub enum DecodeResult<T> {
    /// A complete frame was decoded.
    Complete(T),
    /// More data is needed to complete the frame.
    Incomplete,
    /// The input is invalid.
    Invalid(String),
}

/// Result of encoding a response to the wire.
#[derive(Debug)]
pub enum EncodeResult {
    /// Successfully encoded.
    Ok(Bytes),
    /// Encoding failed.
    Error(String),
}

/// Protocol detection from initial bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedProtocol {
    /// Redis RESP protocol (starts with +, -, :, $, *, etc.)
    Redis,
    /// Memcached ASCII protocol (starts with command word)
    MemcachedAscii,
    /// Memcached binary protocol (magic byte 0x80)
    MemcachedBinary,
    /// Unknown protocol.
    Unknown,
}

/// Detect protocol from initial bytes.
pub fn detect_protocol(data: &[u8]) -> DetectedProtocol {
    if data.is_empty() {
        return DetectedProtocol::Unknown;
    }

    match data[0] {
        // Memcached binary magic
        0x80 => DetectedProtocol::MemcachedBinary,

        // RESP type prefixes
        b'+' | b'-' | b':' | b'$' | b'*' | b'_' | b'#' | b',' | b'(' | b'!' | b'=' | b'%'
        | b'~' | b'|' | b'>' => DetectedProtocol::Redis,

        // ASCII letter - could be Redis inline or Memcached ASCII
        // Memcached commands: get, set, add, replace, delete, etc.
        // Redis inline: PING, QUIT, etc.
        b'a'..=b'z' | b'A'..=b'Z' => {
            // Check for common Memcached commands
            let upper: Vec<u8> = data
                .iter()
                .take(10)
                .map(|b| b.to_ascii_uppercase())
                .collect();
            if upper.starts_with(b"GET ")
                || upper.starts_with(b"SET ")
                || upper.starts_with(b"ADD ")
                || upper.starts_with(b"REPLACE ")
                || upper.starts_with(b"DELETE ")
                || upper.starts_with(b"INCR ")
                || upper.starts_with(b"DECR ")
                || upper.starts_with(b"STATS")
                || upper.starts_with(b"FLUSH")
                || upper.starts_with(b"VERSION")
                || upper.starts_with(b"QUIT")
                || upper.starts_with(b"CAS ")
                || upper.starts_with(b"GETS ")
                || upper.starts_with(b"APPEND ")
                || upper.starts_with(b"PREPEND ")
                || upper.starts_with(b"TOUCH ")
            {
                DetectedProtocol::MemcachedAscii
            } else {
                // Assume Redis inline command
                DetectedProtocol::Redis
            }
        }

        _ => DetectedProtocol::Unknown,
    }
}

/// Backpressure state for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureState {
    /// Normal operation.
    Normal,
    /// Approaching limit.
    Warning,
    /// At limit, rejecting new requests.
    Rejecting,
}

/// Connection-level backpressure manager.
#[derive(Debug)]
pub struct ConnectionBackpressure {
    /// Maximum pending commands.
    max_pending: usize,

    /// Current pending count.
    pending: std::sync::atomic::AtomicUsize,

    /// High water mark (triggers warning).
    high_water_mark: usize,
}

impl ConnectionBackpressure {
    /// Create a new backpressure manager.
    pub fn new(max_pending: usize) -> Self {
        Self {
            max_pending,
            pending: std::sync::atomic::AtomicUsize::new(0),
            high_water_mark: max_pending * 80 / 100, // 80% threshold
        }
    }

    /// Check current state.
    pub fn state(&self) -> BackpressureState {
        let pending = self.pending.load(std::sync::atomic::Ordering::Relaxed);
        if pending >= self.max_pending {
            BackpressureState::Rejecting
        } else if pending >= self.high_water_mark {
            BackpressureState::Warning
        } else {
            BackpressureState::Normal
        }
    }

    /// Try to acquire a slot for a new command.
    pub fn try_acquire(&self) -> bool {
        let pending = self.pending.load(std::sync::atomic::Ordering::Relaxed);
        if pending >= self.max_pending {
            return false;
        }
        self.pending
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    }

    /// Release a slot after command completion.
    pub fn release(&self) {
        self.pending
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get current pending count.
    pub fn pending(&self) -> usize {
        self.pending.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for ConnectionBackpressure {
    fn default() -> Self {
        Self::new(64)
    }
}

/// Read buffer for streaming protocol parsing.
#[derive(Debug)]
pub struct ReadBuffer {
    /// Internal buffer.
    buffer: BytesMut,

    /// Maximum buffer size.
    max_size: usize,
}

impl ReadBuffer {
    /// Create a new read buffer.
    pub fn new(capacity: usize, max_size: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
            max_size,
        }
    }

    /// Append data to the buffer.
    pub fn extend(&mut self, data: &[u8]) -> Result<(), String> {
        if self.buffer.len() + data.len() > self.max_size {
            return Err(format!(
                "buffer overflow: {} + {} > {}",
                self.buffer.len(),
                data.len(),
                self.max_size
            ));
        }
        self.buffer.extend_from_slice(data);
        Ok(())
    }

    /// Get a reference to buffered data.
    pub fn data(&self) -> &[u8] {
        &self.buffer
    }

    /// Consume bytes from the front of the buffer.
    pub fn consume(&mut self, count: usize) {
        let _ = self.buffer.split_to(count);
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get buffer length.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Get remaining capacity before max_size.
    pub fn remaining_capacity(&self) -> usize {
        self.max_size.saturating_sub(self.buffer.len())
    }
}

impl Default for ReadBuffer {
    fn default() -> Self {
        Self::new(64 * 1024, 512 * 1024 * 1024) // 64KB initial, 512MB max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_protocol_redis_resp() {
        assert_eq!(detect_protocol(b"+OK\r\n"), DetectedProtocol::Redis);
        assert_eq!(detect_protocol(b"-ERR\r\n"), DetectedProtocol::Redis);
        assert_eq!(detect_protocol(b":1000\r\n"), DetectedProtocol::Redis);
        assert_eq!(detect_protocol(b"$3\r\nfoo\r\n"), DetectedProtocol::Redis);
        assert_eq!(detect_protocol(b"*2\r\n"), DetectedProtocol::Redis);
    }

    #[test]
    fn test_detect_protocol_redis_inline() {
        assert_eq!(detect_protocol(b"PING\r\n"), DetectedProtocol::Redis);
    }

    #[test]
    fn test_detect_protocol_memcached_ascii() {
        assert_eq!(
            detect_protocol(b"get key\r\n"),
            DetectedProtocol::MemcachedAscii
        );
        assert_eq!(
            detect_protocol(b"SET key 0 0 5\r\n"),
            DetectedProtocol::MemcachedAscii
        );
        assert_eq!(
            detect_protocol(b"stats\r\n"),
            DetectedProtocol::MemcachedAscii
        );
    }

    #[test]
    fn test_detect_protocol_memcached_binary() {
        assert_eq!(
            detect_protocol(&[0x80, 0x00, 0x00, 0x03]),
            DetectedProtocol::MemcachedBinary
        );
    }

    #[test]
    fn test_detect_protocol_unknown() {
        assert_eq!(detect_protocol(b""), DetectedProtocol::Unknown);
        assert_eq!(detect_protocol(&[0x00]), DetectedProtocol::Unknown);
    }

    #[test]
    fn test_connection_backpressure() {
        let bp = ConnectionBackpressure::new(10);

        assert_eq!(bp.state(), BackpressureState::Normal);
        assert_eq!(bp.pending(), 0);

        // Acquire slots
        for _ in 0..8 {
            assert!(bp.try_acquire());
        }
        assert_eq!(bp.state(), BackpressureState::Warning);

        // Fill to max
        assert!(bp.try_acquire());
        assert!(bp.try_acquire());
        assert_eq!(bp.state(), BackpressureState::Rejecting);

        // Can't acquire more
        assert!(!bp.try_acquire());

        // Release one
        bp.release();
        assert_eq!(bp.state(), BackpressureState::Warning);
    }

    #[test]
    fn test_read_buffer() {
        let mut buf = ReadBuffer::new(1024, 2048);

        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);

        buf.extend(b"hello").unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.data(), b"hello");

        buf.extend(b" world").unwrap();
        assert_eq!(buf.data(), b"hello world");

        buf.consume(6);
        assert_eq!(buf.data(), b"world");

        buf.clear();
        assert!(buf.is_empty());
    }

    #[test]
    fn test_read_buffer_overflow() {
        let mut buf = ReadBuffer::new(10, 20);

        buf.extend(b"12345678901234567890").unwrap();
        assert!(buf.extend(b"x").is_err());
    }
}
