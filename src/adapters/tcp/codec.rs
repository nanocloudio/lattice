//! Protocol codec traits.
//!
//! Defines the interface for encoding/decoding protocol frames.

use super::{DecodeResult, EncodeResult};
use bytes::BytesMut;

/// Trait for protocol codecs.
///
/// A codec handles encoding responses and decoding requests for a specific
/// wire protocol (Redis RESP, Memcached ASCII/binary, etc.).
pub trait ProtocolCodec: Send + Sync {
    /// The request type decoded from the wire.
    type Request;

    /// The response type encoded to the wire.
    type Response;

    /// Attempt to decode a request from the buffer.
    ///
    /// Returns:
    /// - `Complete(request)` if a full request was decoded
    /// - `Incomplete` if more data is needed
    /// - `Invalid(error)` if the data is malformed
    ///
    /// On `Complete`, the codec should consume the decoded bytes from the buffer.
    fn decode(&self, buffer: &mut BytesMut) -> DecodeResult<Self::Request>;

    /// Encode a response to bytes.
    fn encode(&self, response: &Self::Response) -> EncodeResult;

    /// Get the protocol name.
    fn protocol_name(&self) -> &'static str;

    /// Reset any internal state (e.g., after protocol error).
    fn reset(&mut self) {}
}

/// Codec state machine for streaming parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecState {
    /// Ready to parse a new frame.
    Ready,
    /// Parsing a frame header.
    ParsingHeader,
    /// Parsing frame body.
    ParsingBody { remaining: usize },
    /// Error state, needs reset.
    Error,
}

impl Default for CodecState {
    fn default() -> Self {
        Self::Ready
    }
}

/// Frame boundary detection result.
#[derive(Debug, Clone, Copy)]
pub enum FrameBoundary {
    /// Found a complete frame of the given size.
    Complete(usize),
    /// Need more data.
    NeedMore,
    /// Invalid frame.
    Invalid,
}

/// Scan for a CRLF line ending.
pub fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2)
        .position(|window| window == b"\r\n")
        .map(|pos| pos + 2)
}

/// Scan for a LF line ending (for more lenient parsing).
pub fn find_lf(data: &[u8]) -> Option<usize> {
    data.iter().position(|&b| b == b'\n').map(|pos| pos + 1)
}

/// Parse an integer from ASCII bytes.
pub fn parse_int(data: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(data).ok()?;
    s.parse().ok()
}

/// Parse an unsigned integer from ASCII bytes.
pub fn parse_uint(data: &[u8]) -> Option<u64> {
    let s = std::str::from_utf8(data).ok()?;
    s.parse().ok()
}

/// Parse a float from ASCII bytes.
pub fn parse_float(data: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(data).ok()?;
    if s == "inf" || s == "+inf" {
        return Some(f64::INFINITY);
    }
    if s == "-inf" {
        return Some(f64::NEG_INFINITY);
    }
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_crlf() {
        assert_eq!(find_crlf(b"hello\r\nworld"), Some(7));
        assert_eq!(find_crlf(b"hello\r\n"), Some(7));
        assert_eq!(find_crlf(b"\r\n"), Some(2));
        assert_eq!(find_crlf(b"hello"), None);
        assert_eq!(find_crlf(b"hello\n"), None);
        assert_eq!(find_crlf(b"hello\r"), None);
    }

    #[test]
    fn test_find_lf() {
        assert_eq!(find_lf(b"hello\nworld"), Some(6));
        assert_eq!(find_lf(b"hello\r\n"), Some(7));
        assert_eq!(find_lf(b"\n"), Some(1));
        assert_eq!(find_lf(b"hello"), None);
    }

    #[test]
    fn test_parse_int() {
        assert_eq!(parse_int(b"123"), Some(123));
        assert_eq!(parse_int(b"-456"), Some(-456));
        assert_eq!(parse_int(b"0"), Some(0));
        assert_eq!(parse_int(b"abc"), None);
        assert_eq!(parse_int(b""), None);
    }

    #[test]
    fn test_parse_uint() {
        assert_eq!(parse_uint(b"123"), Some(123));
        assert_eq!(parse_uint(b"0"), Some(0));
        assert_eq!(parse_uint(b"-1"), None);
        assert_eq!(parse_uint(b"abc"), None);
    }

    #[test]
    fn test_parse_float() {
        assert_eq!(parse_float(b"1.5"), Some(1.5));
        assert_eq!(parse_float(b"-2.5"), Some(-2.5));
        assert_eq!(parse_float(b"inf"), Some(f64::INFINITY));
        assert_eq!(parse_float(b"+inf"), Some(f64::INFINITY));
        assert_eq!(parse_float(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_float(b"abc"), None);
    }
}
