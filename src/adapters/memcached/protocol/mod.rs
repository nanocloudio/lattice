//! Memcached protocol parsing and encoding.
//!
//! This module provides parsers and encoders for both the Memcached ASCII
//! text protocol and the binary protocol.
//!
//! # Protocol Detection
//!
//! The protocol is auto-detected from the first byte:
//! - `0x80`: Binary protocol (request magic)
//! - ASCII letters: ASCII text protocol
//!
//! # ASCII Protocol
//!
//! Commands follow the format: `<command> <key> [<args>]\r\n[<data>\r\n]`
//!
//! # Binary Protocol
//!
//! All requests and responses use a 24-byte header followed by optional body:
//! - Byte 0: Magic (0x80 request, 0x81 response)
//! - Byte 1: Opcode
//! - Bytes 2-3: Key length
//! - Byte 4: Extras length
//! - Byte 5: Data type
//! - Bytes 6-7: Status (response) / Reserved (request)
//! - Bytes 8-11: Total body length
//! - Bytes 12-15: Opaque
//! - Bytes 16-23: CAS

pub mod ascii;
pub mod binary;
pub mod codec;

pub use ascii::{AsciiEncoder, AsciiParser};
pub use binary::{BinaryEncoder, BinaryParser};
pub use codec::MemcachedCodec;

/// Parse result for protocol parsing.
#[derive(Debug)]
pub enum ParseResult {
    /// Successfully parsed a value, returns (value, bytes_consumed).
    Ok(crate::adapters::memcached::MemcachedCommand, usize),
    /// Need more data.
    Incomplete,
    /// Parse error.
    Error(String),
}

/// ASCII command line parse result.
#[derive(Debug)]
pub enum CommandLineResult {
    /// Successfully parsed command line.
    Ok {
        /// Command name.
        command: String,
        /// Arguments.
        args: Vec<String>,
        /// Bytes consumed (including \r\n).
        consumed: usize,
    },
    /// Need more data.
    Incomplete,
    /// Parse error.
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_result_variants() {
        // Just verify the enum variants exist
        let _incomplete = ParseResult::Incomplete;
        let _error = ParseResult::Error("test".to_string());
    }
}
