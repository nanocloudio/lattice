//! Memcached binary protocol parser and encoder.
//!
//! The binary protocol uses fixed-size headers followed by variable-size bodies.
//!
//! # Header Format (24 bytes)
//!
//! ```text
//! Request:
//! Byte/     0       |       1       |       2       |       3       |
//!    /              |               |               |               |
//!   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//!   +---------------+---------------+---------------+---------------+
//!  0| Magic         | Opcode        | Key length                    |
//!   +---------------+---------------+---------------+---------------+
//!  4| Extras length | Data type     | vbucket id                    |
//!   +---------------+---------------+---------------+---------------+
//!  8| Total body length                                             |
//!   +---------------+---------------+---------------+---------------+
//! 12| Opaque                                                        |
//!   +---------------+---------------+---------------+---------------+
//! 16| CAS                                                           |
//!   |                                                               |
//!   +---------------+---------------+---------------+---------------+
//!
//! Response:
//! Byte/     0       |       1       |       2       |       3       |
//!    /              |               |               |               |
//!   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//!   +---------------+---------------+---------------+---------------+
//!  0| Magic         | Opcode        | Key length                    |
//!   +---------------+---------------+---------------+---------------+
//!  4| Extras length | Data type     | Status                        |
//!   +---------------+---------------+---------------+---------------+
//!  8| Total body length                                             |
//!   +---------------+---------------+---------------+---------------+
//! 12| Opaque                                                        |
//!   +---------------+---------------+---------------+---------------+
//! 16| CAS                                                           |
//!   |                                                               |
//!   +---------------+---------------+---------------+---------------+
//! ```
//!
//! # Body Format
//!
//! Body = Extras + Key + Value
//!
//! Extras length and content vary by opcode.

use super::ParseResult;
use crate::adapters::memcached::{
    BinaryOpcode, BinaryStatus, MemcachedCommand, MemcachedError, MemcachedResponse,
    ResponseStatus, ValueResponse, BINARY_HEADER_SIZE, BINARY_REQUEST_MAGIC, BINARY_RESPONSE_MAGIC,
};
use bytes::Bytes;

/// Binary protocol parser.
#[derive(Debug, Default)]
pub struct BinaryParser;

impl BinaryParser {
    /// Create a new binary parser.
    pub fn new() -> Self {
        Self
    }

    /// Parse a command from the buffer.
    pub fn parse(&self, data: &[u8]) -> ParseResult {
        // Need at least header size
        if data.len() < BINARY_HEADER_SIZE {
            return ParseResult::Incomplete;
        }

        // Parse header
        let header = match BinaryHeader::parse(data) {
            Ok(h) => h,
            Err(e) => return ParseResult::Error(e),
        };

        // Check total frame size
        let total_size = BINARY_HEADER_SIZE + header.total_body_length as usize;
        if data.len() < total_size {
            return ParseResult::Incomplete;
        }

        // Parse body
        let body = &data[BINARY_HEADER_SIZE..total_size];

        // Extract extras, key, value from body
        let extras_end = header.extras_length as usize;
        let key_end = extras_end + header.key_length as usize;

        if body.len() < key_end {
            return ParseResult::Error("invalid body length".to_string());
        }

        let extras = &body[..extras_end];
        let key = &body[extras_end..key_end];
        let value = &body[key_end..];

        // Convert to command
        let cmd = self.build_command(&header, extras, key, value);
        match cmd {
            Ok(c) => ParseResult::Ok(c, total_size),
            Err(e) => ParseResult::Error(e),
        }
    }

    /// Build a command from parsed header and body parts.
    fn build_command(
        &self,
        header: &BinaryHeader,
        extras: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<MemcachedCommand, String> {
        let opcode = BinaryOpcode::try_from(header.opcode)
            .map_err(|op| format!("unknown opcode: 0x{:02x}", op))?;

        let mut cmd = MemcachedCommand::new(opcode.to_command_name())
            .with_opaque(header.opaque)
            .with_noreply(opcode.is_quiet());

        // Set key if present
        if !key.is_empty() {
            cmd.key = Some(Bytes::copy_from_slice(key));
        }

        // Set CAS if non-zero
        if header.cas != 0 {
            cmd.cas = Some(header.cas);
        }

        // Parse extras based on opcode
        match opcode.to_non_quiet() {
            BinaryOpcode::Get | BinaryOpcode::GetK => {
                // Get has no extras
            }

            BinaryOpcode::Set | BinaryOpcode::Add | BinaryOpcode::Replace => {
                // Extras: flags (4 bytes) + expiration (4 bytes)
                if extras.len() < 8 {
                    return Err("invalid extras length for storage command".to_string());
                }
                cmd.flags = u32::from_be_bytes([extras[0], extras[1], extras[2], extras[3]]);
                cmd.exptime = u32::from_be_bytes([extras[4], extras[5], extras[6], extras[7]]);
                cmd.data = Some(Bytes::copy_from_slice(value));
            }

            BinaryOpcode::Append | BinaryOpcode::Prepend => {
                // No extras, just value
                cmd.data = Some(Bytes::copy_from_slice(value));
            }

            BinaryOpcode::Delete => {
                // No extras
            }

            BinaryOpcode::Increment | BinaryOpcode::Decrement => {
                // Extras: delta (8 bytes) + initial (8 bytes) + expiration (4 bytes)
                if extras.len() < 20 {
                    return Err("invalid extras length for incr/decr".to_string());
                }
                cmd.delta = Some(u64::from_be_bytes([
                    extras[0], extras[1], extras[2], extras[3], extras[4], extras[5], extras[6],
                    extras[7],
                ]));
                cmd.initial = Some(u64::from_be_bytes([
                    extras[8], extras[9], extras[10], extras[11], extras[12], extras[13],
                    extras[14], extras[15],
                ]));
                cmd.exptime = u32::from_be_bytes([extras[16], extras[17], extras[18], extras[19]]);
            }

            BinaryOpcode::Touch | BinaryOpcode::Gat | BinaryOpcode::GatK => {
                // Extras: expiration (4 bytes)
                if extras.len() < 4 {
                    return Err("invalid extras length for touch/gat".to_string());
                }
                cmd.exptime = u32::from_be_bytes([extras[0], extras[1], extras[2], extras[3]]);
            }

            BinaryOpcode::Flush => {
                // Extras: optional expiration (4 bytes)
                if extras.len() >= 4 {
                    cmd.exptime = u32::from_be_bytes([extras[0], extras[1], extras[2], extras[3]]);
                }
            }

            BinaryOpcode::Stat => {
                // Key is optional stats type
            }

            BinaryOpcode::Noop | BinaryOpcode::Version | BinaryOpcode::Quit => {
                // No extras or body
            }

            BinaryOpcode::SaslListMechs => {
                // No extras
            }

            BinaryOpcode::SaslAuth | BinaryOpcode::SaslStep => {
                // Key = mechanism, value = auth data
                cmd.data = Some(Bytes::copy_from_slice(value));
            }

            _ => {}
        }

        Ok(cmd)
    }

    /// Reset parser state.
    pub fn reset(&mut self) {
        // Binary parser is stateless
    }
}

/// Binary protocol header (only fields that are actually used).
#[derive(Debug, Clone)]
struct BinaryHeader {
    opcode: u8,
    key_length: u16,
    extras_length: u8,
    total_body_length: u32,
    opaque: u32,
    cas: u64,
}

impl BinaryHeader {
    /// Parse header from bytes.
    fn parse(data: &[u8]) -> Result<Self, String> {
        if data.len() < BINARY_HEADER_SIZE {
            return Err("header too short".to_string());
        }

        let magic = data[0];
        if magic != BINARY_REQUEST_MAGIC {
            return Err(format!("invalid magic: 0x{:02x}", magic));
        }

        Ok(Self {
            opcode: data[1],
            key_length: u16::from_be_bytes([data[2], data[3]]),
            extras_length: data[4],
            // data_type at data[5] and status_or_vbucket at data[6..8] are not used
            total_body_length: u32::from_be_bytes([data[8], data[9], data[10], data[11]]),
            opaque: u32::from_be_bytes([data[12], data[13], data[14], data[15]]),
            cas: u64::from_be_bytes([
                data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23],
            ]),
        })
    }
}

/// Binary protocol encoder.
#[derive(Debug, Default)]
pub struct BinaryEncoder;

impl BinaryEncoder {
    /// Create a new binary encoder.
    pub fn new() -> Self {
        Self
    }

    /// Encode a response to bytes.
    pub fn encode(response: &MemcachedResponse, opcode: u8, opaque: u32, cas: u64) -> Vec<u8> {
        match response {
            MemcachedResponse::Status(status) => Self::encode_status(*status, opcode, opaque, cas),
            MemcachedResponse::Value(value) => Self::encode_value(value, opcode, opaque),
            MemcachedResponse::Values(values) => {
                // For binary protocol, each value is a separate response
                // This encodes just the first value; the caller should iterate
                if let Some(value) = values.first() {
                    Self::encode_value(value, opcode, opaque)
                } else {
                    Self::encode_empty(opcode, opaque, BinaryStatus::NoError)
                }
            }
            MemcachedResponse::Numeric(n) => Self::encode_numeric(*n, opcode, opaque, cas),
            MemcachedResponse::Stats(stats) => {
                // Stats are sent as multiple responses, this encodes them all
                Self::encode_stats(stats, opaque)
            }
            MemcachedResponse::Version(version) => Self::encode_version(version, opaque),
            MemcachedResponse::Error(error) => Self::encode_error(error, opcode, opaque),
            MemcachedResponse::NoResponse => Vec::new(),
        }
    }

    /// Encode a status response.
    fn encode_status(status: ResponseStatus, opcode: u8, opaque: u32, cas: u64) -> Vec<u8> {
        let binary_status = match status {
            ResponseStatus::Stored | ResponseStatus::Ok => BinaryStatus::NoError,
            ResponseStatus::NotStored => BinaryStatus::ItemNotStored,
            ResponseStatus::Exists => BinaryStatus::KeyExists,
            ResponseStatus::NotFound => BinaryStatus::KeyNotFound,
            ResponseStatus::Deleted => BinaryStatus::NoError,
            ResponseStatus::Touched => BinaryStatus::NoError,
        };

        Self::encode_empty(opcode, opaque, binary_status).tap_mut(|buf| {
            // Set CAS in header
            buf[16..24].copy_from_slice(&cas.to_be_bytes());
        })
    }

    /// Encode an empty response (just header).
    fn encode_empty(opcode: u8, opaque: u32, status: BinaryStatus) -> Vec<u8> {
        let mut buf = vec![0u8; BINARY_HEADER_SIZE];
        buf[0] = BINARY_RESPONSE_MAGIC;
        buf[1] = opcode;
        // key length = 0
        // extras length = 0
        // data type = 0
        buf[6..8].copy_from_slice(&u16::from(status).to_be_bytes());
        // total body length = 0
        buf[12..16].copy_from_slice(&opaque.to_be_bytes());
        // cas = 0
        buf
    }

    /// Encode a value response.
    fn encode_value(value: &ValueResponse, opcode: u8, opaque: u32) -> Vec<u8> {
        let include_key = matches!(
            BinaryOpcode::try_from(opcode),
            Ok(BinaryOpcode::GetK)
                | Ok(BinaryOpcode::GetKQ)
                | Ok(BinaryOpcode::GatK)
                | Ok(BinaryOpcode::GatKQ)
        );

        let extras_len = 4; // flags
        let key_len = if include_key { value.key.len() } else { 0 };
        let value_len = value.data.len();
        let total_body_len = extras_len + key_len + value_len;

        let mut buf = Vec::with_capacity(BINARY_HEADER_SIZE + total_body_len);

        // Header
        buf.push(BINARY_RESPONSE_MAGIC);
        buf.push(opcode);
        buf.extend_from_slice(&(key_len as u16).to_be_bytes());
        buf.push(extras_len as u8);
        buf.push(0); // data type
        buf.extend_from_slice(&u16::from(BinaryStatus::NoError).to_be_bytes());
        buf.extend_from_slice(&(total_body_len as u32).to_be_bytes());
        buf.extend_from_slice(&opaque.to_be_bytes());
        buf.extend_from_slice(&value.cas.unwrap_or(0).to_be_bytes());

        // Extras: flags
        buf.extend_from_slice(&value.flags.to_be_bytes());

        // Key (if GetK/GatK)
        if include_key {
            buf.extend_from_slice(&value.key);
        }

        // Value
        buf.extend_from_slice(&value.data);

        buf
    }

    /// Encode a numeric response (for incr/decr).
    fn encode_numeric(n: u64, opcode: u8, opaque: u32, cas: u64) -> Vec<u8> {
        let body_len = 8; // u64 value

        let mut buf = Vec::with_capacity(BINARY_HEADER_SIZE + body_len);

        // Header
        buf.push(BINARY_RESPONSE_MAGIC);
        buf.push(opcode);
        buf.extend_from_slice(&0u16.to_be_bytes()); // key length
        buf.push(0); // extras length
        buf.push(0); // data type
        buf.extend_from_slice(&u16::from(BinaryStatus::NoError).to_be_bytes());
        buf.extend_from_slice(&(body_len as u32).to_be_bytes());
        buf.extend_from_slice(&opaque.to_be_bytes());
        buf.extend_from_slice(&cas.to_be_bytes());

        // Body: value
        buf.extend_from_slice(&n.to_be_bytes());

        buf
    }

    /// Encode stats responses.
    fn encode_stats(stats: &[(String, String)], opaque: u32) -> Vec<u8> {
        let mut buf = Vec::new();

        for (key, value) in stats {
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            let total_body_len = key_bytes.len() + value_bytes.len();

            // Header
            buf.push(BINARY_RESPONSE_MAGIC);
            buf.push(BinaryOpcode::Stat as u8);
            buf.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
            buf.push(0); // extras length
            buf.push(0); // data type
            buf.extend_from_slice(&u16::from(BinaryStatus::NoError).to_be_bytes());
            buf.extend_from_slice(&(total_body_len as u32).to_be_bytes());
            buf.extend_from_slice(&opaque.to_be_bytes());
            buf.extend_from_slice(&0u64.to_be_bytes()); // cas

            // Body
            buf.extend_from_slice(key_bytes);
            buf.extend_from_slice(value_bytes);
        }

        // Empty stats response to signal end
        buf.extend(Self::encode_empty(
            BinaryOpcode::Stat as u8,
            opaque,
            BinaryStatus::NoError,
        ));

        buf
    }

    /// Encode version response.
    fn encode_version(version: &str, opaque: u32) -> Vec<u8> {
        let version_bytes = version.as_bytes();
        let total_body_len = version_bytes.len();

        let mut buf = Vec::with_capacity(BINARY_HEADER_SIZE + total_body_len);

        // Header
        buf.push(BINARY_RESPONSE_MAGIC);
        buf.push(BinaryOpcode::Version as u8);
        buf.extend_from_slice(&0u16.to_be_bytes()); // key length
        buf.push(0); // extras length
        buf.push(0); // data type
        buf.extend_from_slice(&u16::from(BinaryStatus::NoError).to_be_bytes());
        buf.extend_from_slice(&(total_body_len as u32).to_be_bytes());
        buf.extend_from_slice(&opaque.to_be_bytes());
        buf.extend_from_slice(&0u64.to_be_bytes()); // cas

        // Body: version string
        buf.extend_from_slice(version_bytes);

        buf
    }

    /// Encode an error response.
    fn encode_error(error: &MemcachedError, opcode: u8, opaque: u32) -> Vec<u8> {
        let (status, message) = match error {
            MemcachedError::Error => (BinaryStatus::UnknownCommand, "unknown error"),
            MemcachedError::ClientError(msg) => (BinaryStatus::InvalidArguments, msg.as_str()),
            MemcachedError::ServerError(msg) => (BinaryStatus::InternalError, msg.as_str()),
        };

        let message_bytes = message.as_bytes();
        let total_body_len = message_bytes.len();

        let mut buf = Vec::with_capacity(BINARY_HEADER_SIZE + total_body_len);

        // Header
        buf.push(BINARY_RESPONSE_MAGIC);
        buf.push(opcode);
        buf.extend_from_slice(&0u16.to_be_bytes()); // key length
        buf.push(0); // extras length
        buf.push(0); // data type
        buf.extend_from_slice(&u16::from(status).to_be_bytes());
        buf.extend_from_slice(&(total_body_len as u32).to_be_bytes());
        buf.extend_from_slice(&opaque.to_be_bytes());
        buf.extend_from_slice(&0u64.to_be_bytes()); // cas

        // Body: error message
        buf.extend_from_slice(message_bytes);

        buf
    }
}

/// Extension trait for Vec to allow tap_mut pattern.
trait VecTapMut {
    fn tap_mut<F: FnOnce(&mut Self)>(self, f: F) -> Self;
}

impl<T> VecTapMut for Vec<T> {
    fn tap_mut<F: FnOnce(&mut Self)>(mut self, f: F) -> Self {
        f(&mut self);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a binary request for testing.
    fn build_request(opcode: u8, key: &[u8], extras: &[u8], value: &[u8], cas: u64) -> Vec<u8> {
        let total_body_len = extras.len() + key.len() + value.len();
        let mut buf = Vec::with_capacity(BINARY_HEADER_SIZE + total_body_len);

        buf.push(BINARY_REQUEST_MAGIC);
        buf.push(opcode);
        buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
        buf.push(extras.len() as u8);
        buf.push(0); // data type
        buf.extend_from_slice(&0u16.to_be_bytes()); // vbucket
        buf.extend_from_slice(&(total_body_len as u32).to_be_bytes());
        buf.extend_from_slice(&42u32.to_be_bytes()); // opaque
        buf.extend_from_slice(&cas.to_be_bytes());

        buf.extend_from_slice(extras);
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);

        buf
    }

    #[test]
    fn test_parse_get() {
        let parser = BinaryParser::new();
        let request = build_request(BinaryOpcode::Get as u8, b"mykey", &[], &[], 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, consumed) => {
                assert_eq!(cmd.name, "get");
                assert_eq!(cmd.key_str(), Some("mykey"));
                assert_eq!(consumed, BINARY_HEADER_SIZE + 5);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_set() {
        let parser = BinaryParser::new();
        let extras = [
            0x00, 0x00, 0x00, 0x2A, // flags = 42
            0x00, 0x00, 0x0E, 0x10, // exptime = 3600
        ];
        let request = build_request(BinaryOpcode::Set as u8, b"mykey", &extras, b"hello", 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "set");
                assert_eq!(cmd.key_str(), Some("mykey"));
                assert_eq!(cmd.flags, 42);
                assert_eq!(cmd.exptime, 3600);
                assert_eq!(
                    cmd.data.as_ref().map(|d| d.as_ref()),
                    Some(b"hello".as_slice())
                );
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_add() {
        let parser = BinaryParser::new();
        let extras = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // flags=0, exptime=0
        let request = build_request(BinaryOpcode::Add as u8, b"newkey", &extras, b"value", 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "add");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let parser = BinaryParser::new();
        let request = build_request(BinaryOpcode::Delete as u8, b"mykey", &[], &[], 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "delete");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_increment() {
        let parser = BinaryParser::new();
        let extras = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // delta = 5
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // initial = 0
            0x00, 0x00, 0x00, 0x00, // exptime = 0
        ];
        let request = build_request(BinaryOpcode::Increment as u8, b"counter", &extras, &[], 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "incr");
                assert_eq!(cmd.delta, Some(5));
                assert_eq!(cmd.initial, Some(0));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_touch() {
        let parser = BinaryParser::new();
        let extras = [0x00, 0x00, 0x0E, 0x10]; // exptime = 3600
        let request = build_request(BinaryOpcode::Touch as u8, b"mykey", &extras, &[], 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "touch");
                assert_eq!(cmd.exptime, 3600);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_version() {
        let parser = BinaryParser::new();
        let request = build_request(BinaryOpcode::Version as u8, &[], &[], &[], 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "version");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_noop() {
        let parser = BinaryParser::new();
        let request = build_request(BinaryOpcode::Noop as u8, &[], &[], &[], 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "noop");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_quiet_opcode() {
        let parser = BinaryParser::new();
        let request = build_request(BinaryOpcode::GetQ as u8, b"mykey", &[], &[], 0);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "get");
                assert!(cmd.noreply); // Quiet opcodes set noreply
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_with_cas() {
        let parser = BinaryParser::new();
        let extras = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; // flags=0, exptime=0
        let request = build_request(BinaryOpcode::Set as u8, b"mykey", &extras, b"value", 12345);

        match parser.parse(&request) {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.cas, Some(12345));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_incomplete() {
        let parser = BinaryParser::new();
        let request = build_request(BinaryOpcode::Get as u8, b"mykey", &[], &[], 0);

        // Only provide partial data
        let result = parser.parse(&request[..10]);
        assert!(matches!(result, ParseResult::Incomplete));
    }

    #[test]
    fn test_parse_invalid_magic() {
        let parser = BinaryParser::new();
        let mut request = build_request(BinaryOpcode::Get as u8, b"mykey", &[], &[], 0);
        request[0] = 0xFF; // Invalid magic

        let result = parser.parse(&request);
        assert!(matches!(result, ParseResult::Error(_)));
    }

    #[test]
    fn test_encode_status() {
        let encoded =
            BinaryEncoder::encode_status(ResponseStatus::Stored, BinaryOpcode::Set as u8, 42, 0);

        assert_eq!(encoded.len(), BINARY_HEADER_SIZE);
        assert_eq!(encoded[0], BINARY_RESPONSE_MAGIC);
        assert_eq!(encoded[1], BinaryOpcode::Set as u8);
        // Status should be NoError
        assert_eq!(u16::from_be_bytes([encoded[6], encoded[7]]), 0);
    }

    #[test]
    fn test_encode_value() {
        let value = ValueResponse {
            key: Bytes::from("mykey"),
            flags: 42,
            data: Bytes::from("hello"),
            cas: Some(12345),
        };

        let encoded = BinaryEncoder::encode_value(&value, BinaryOpcode::Get as u8, 1);

        assert_eq!(encoded[0], BINARY_RESPONSE_MAGIC);
        assert_eq!(encoded[1], BinaryOpcode::Get as u8);
        // Extras length should be 4 (flags)
        assert_eq!(encoded[4], 4);
        // CAS should be set
        let cas = u64::from_be_bytes([
            encoded[16],
            encoded[17],
            encoded[18],
            encoded[19],
            encoded[20],
            encoded[21],
            encoded[22],
            encoded[23],
        ]);
        assert_eq!(cas, 12345);
    }

    #[test]
    fn test_encode_value_with_key() {
        let value = ValueResponse {
            key: Bytes::from("mykey"),
            flags: 0,
            data: Bytes::from("hello"),
            cas: None,
        };

        // GetK should include key in response
        let encoded = BinaryEncoder::encode_value(&value, BinaryOpcode::GetK as u8, 1);

        // Key length should be 5
        let key_len = u16::from_be_bytes([encoded[2], encoded[3]]);
        assert_eq!(key_len, 5);
    }

    #[test]
    fn test_encode_numeric() {
        let encoded = BinaryEncoder::encode_numeric(12345, BinaryOpcode::Increment as u8, 1, 0);

        assert_eq!(encoded.len(), BINARY_HEADER_SIZE + 8);
        // Body should contain the value
        let value = u64::from_be_bytes([
            encoded[24],
            encoded[25],
            encoded[26],
            encoded[27],
            encoded[28],
            encoded[29],
            encoded[30],
            encoded[31],
        ]);
        assert_eq!(value, 12345);
    }

    #[test]
    fn test_encode_version() {
        let encoded = BinaryEncoder::encode_version("1.6.0", 42);

        assert_eq!(encoded[0], BINARY_RESPONSE_MAGIC);
        assert_eq!(encoded[1], BinaryOpcode::Version as u8);
        // Body should contain version string
        assert!(encoded[BINARY_HEADER_SIZE..].starts_with(b"1.6.0"));
    }

    #[test]
    fn test_encode_error() {
        let encoded = BinaryEncoder::encode_error(
            &MemcachedError::ClientError("bad format".to_string()),
            BinaryOpcode::Set as u8,
            1,
        );

        // Status should be InvalidArguments
        let status = u16::from_be_bytes([encoded[6], encoded[7]]);
        assert_eq!(status, BinaryStatus::InvalidArguments as u16);
        // Body should contain error message
        let body = &encoded[BINARY_HEADER_SIZE..];
        assert_eq!(body, b"bad format");
    }

    #[test]
    fn test_encode_stats() {
        let stats = vec![
            ("pid".to_string(), "1234".to_string()),
            ("uptime".to_string(), "3600".to_string()),
        ];

        let encoded = BinaryEncoder::encode_stats(&stats, 42);

        // Should have 3 responses: 2 stats + 1 empty terminator
        // Each response starts with BINARY_RESPONSE_MAGIC
        let response_count = encoded
            .windows(1)
            .filter(|w| w[0] == BINARY_RESPONSE_MAGIC)
            .count();
        assert_eq!(response_count, 3);
    }

    #[test]
    fn test_encode_no_response() {
        let encoded = BinaryEncoder::encode(
            &MemcachedResponse::NoResponse,
            BinaryOpcode::SetQ as u8,
            0,
            0,
        );
        assert!(encoded.is_empty());
    }
}
