//! Unified RESP codec.
//!
//! Provides a single codec that handles both RESP2 and RESP3 protocols,
//! with automatic protocol negotiation via the HELLO command.

use super::{ParseResult, Resp2Encoder, Resp2Parser, Resp3Encoder, Resp3Parser};
use crate::adapters::redis::{ProtocolVersion, RedisCommand, RedisError, RedisValue};
use crate::adapters::tcp::codec::ProtocolCodec;
use crate::adapters::tcp::{DecodeResult, EncodeResult};
use bytes::{Bytes, BytesMut};

/// Unified RESP codec supporting both RESP2 and RESP3.
#[derive(Debug)]
pub struct RespCodec {
    /// Current protocol version.
    version: ProtocolVersion,

    /// RESP2 parser.
    resp2: Resp2Parser,

    /// RESP3 parser.
    resp3: Resp3Parser,
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl RespCodec {
    /// Create a new codec (defaults to RESP2).
    pub fn new() -> Self {
        Self {
            version: ProtocolVersion::Resp2,
            resp2: Resp2Parser::new(),
            resp3: Resp3Parser::new(),
        }
    }

    /// Create a codec with a specific protocol version.
    pub fn with_version(version: ProtocolVersion) -> Self {
        Self {
            version,
            resp2: Resp2Parser::new(),
            resp3: Resp3Parser::new(),
        }
    }

    /// Get the current protocol version.
    pub fn version(&self) -> ProtocolVersion {
        self.version
    }

    /// Set the protocol version.
    pub fn set_version(&mut self, version: ProtocolVersion) {
        self.version = version;
    }

    /// Upgrade to RESP3 (typically called after HELLO 3).
    pub fn upgrade_to_resp3(&mut self) {
        self.version = ProtocolVersion::Resp3;
    }

    /// Parse a RESP value from the buffer.
    pub fn parse(&mut self, data: &[u8]) -> ParseResult {
        match self.version {
            ProtocolVersion::Resp2 => self.resp2.parse(data),
            ProtocolVersion::Resp3 => self.resp3.parse(data),
        }
    }

    /// Encode a RESP value.
    pub fn encode(&self, value: &RedisValue) -> Vec<u8> {
        match self.version {
            ProtocolVersion::Resp2 => Resp2Encoder::encode(value),
            ProtocolVersion::Resp3 => Resp3Encoder::encode(value),
        }
    }

    /// Parse a command from a RESP value.
    pub fn parse_command(&self, value: RedisValue) -> Result<RedisCommand, RedisError> {
        match value {
            RedisValue::Array(arr) => RedisCommand::from_array(arr),
            _ => Err(RedisError::syntax()),
        }
    }
}

impl ProtocolCodec for RespCodec {
    type Request = RedisCommand;
    type Response = RedisValue;

    fn decode(&self, buffer: &mut BytesMut) -> DecodeResult<Self::Request> {
        if buffer.is_empty() {
            return DecodeResult::Incomplete;
        }

        // Create a temporary parser (we can't mutate self here)
        let result = match self.version {
            ProtocolVersion::Resp2 => {
                let mut parser = Resp2Parser::new();
                parser.parse(buffer)
            }
            ProtocolVersion::Resp3 => {
                let mut parser = Resp3Parser::new();
                parser.parse(buffer)
            }
        };

        match result {
            ParseResult::Ok(value) => {
                // Calculate consumed bytes by re-encoding
                // This is a bit inefficient but ensures correctness
                let encoded = self.encode(&value);
                let consumed = encoded.len();

                if consumed <= buffer.len() {
                    let _ = buffer.split_to(consumed);
                }

                match value {
                    RedisValue::Array(arr) => match RedisCommand::from_array(arr) {
                        Ok(cmd) => DecodeResult::Complete(cmd),
                        Err(e) => DecodeResult::Invalid(e.to_string()),
                    },
                    _ => DecodeResult::Invalid("expected array".to_string()),
                }
            }
            ParseResult::Incomplete => DecodeResult::Incomplete,
            ParseResult::Error(e) => DecodeResult::Invalid(e),
        }
    }

    fn encode(&self, response: &Self::Response) -> EncodeResult {
        let bytes = match self.version {
            ProtocolVersion::Resp2 => Resp2Encoder::encode(response),
            ProtocolVersion::Resp3 => Resp3Encoder::encode(response),
        };
        EncodeResult::Ok(Bytes::from(bytes))
    }

    fn protocol_name(&self) -> &'static str {
        match self.version {
            ProtocolVersion::Resp2 => "RESP2",
            ProtocolVersion::Resp3 => "RESP3",
        }
    }

    fn reset(&mut self) {
        // Reset to default RESP2
        self.version = ProtocolVersion::Resp2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_default_version() {
        let codec = RespCodec::new();
        assert_eq!(codec.version(), ProtocolVersion::Resp2);
        assert_eq!(codec.protocol_name(), "RESP2");
    }

    #[test]
    fn test_codec_upgrade() {
        let mut codec = RespCodec::new();
        codec.upgrade_to_resp3();
        assert_eq!(codec.version(), ProtocolVersion::Resp3);
        assert_eq!(codec.protocol_name(), "RESP3");
    }

    #[test]
    fn test_codec_parse_resp2() {
        let mut codec = RespCodec::new();

        let result = codec.parse(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Array(_))));
    }

    #[test]
    fn test_codec_parse_resp3() {
        let mut codec = RespCodec::with_version(ProtocolVersion::Resp3);

        let result = codec.parse(b"_\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Null)));
    }

    #[test]
    fn test_codec_encode_resp2() {
        let codec = RespCodec::new();
        let value = RedisValue::ok();
        assert_eq!(codec.encode(&value), b"+OK\r\n");
    }

    #[test]
    fn test_codec_encode_resp3() {
        let codec = RespCodec::with_version(ProtocolVersion::Resp3);
        let value = RedisValue::Boolean(true);
        assert_eq!(codec.encode(&value), b"#t\r\n");
    }

    #[test]
    fn test_codec_parse_command() {
        let codec = RespCodec::new();

        let value = RedisValue::Array(vec![
            RedisValue::bulk_string("SET"),
            RedisValue::bulk_string("key"),
            RedisValue::bulk_string("value"),
        ]);

        let cmd = codec.parse_command(value).unwrap();
        assert_eq!(cmd.name, "SET");
        assert_eq!(cmd.argc(), 2);
    }

    #[test]
    fn test_codec_decode() {
        let codec = RespCodec::new();
        let mut buffer = BytesMut::from(&b"*2\r\n$4\r\nPING\r\n$3\r\nfoo\r\n"[..]);

        let result = codec.decode(&mut buffer);
        assert!(matches!(result, DecodeResult::Complete(cmd) if cmd.name == "PING"));
    }

    #[test]
    fn test_codec_decode_incomplete() {
        let codec = RespCodec::new();
        let mut buffer = BytesMut::from(&b"*2\r\n$4\r\nPING"[..]);

        let result = codec.decode(&mut buffer);
        assert!(matches!(result, DecodeResult::Incomplete));
    }
}
