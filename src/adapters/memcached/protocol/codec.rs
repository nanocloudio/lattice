//! Unified Memcached codec.
//!
//! Provides a single codec that handles both ASCII and binary protocols,
//! with automatic protocol detection from the first byte.

use super::{AsciiEncoder, AsciiParser, BinaryEncoder, BinaryParser, ParseResult};
use crate::adapters::memcached::{
    BinaryOpcode, MemcachedCommand, MemcachedResponse, ProtocolType, BINARY_REQUEST_MAGIC,
};
use crate::adapters::tcp::codec::ProtocolCodec;
use crate::adapters::tcp::{DecodeResult, EncodeResult};
use bytes::{Bytes, BytesMut};

/// Unified Memcached codec supporting both ASCII and binary protocols.
#[derive(Debug)]
pub struct MemcachedCodec {
    /// Current protocol type (detected from first byte).
    protocol: Option<ProtocolType>,

    /// ASCII parser.
    ascii: AsciiParser,

    /// Binary parser.
    binary: BinaryParser,

    /// Last parsed command's opcode (for binary response encoding).
    last_opcode: u8,

    /// Last parsed command's opaque (for binary response encoding).
    last_opaque: u32,
}

impl Default for MemcachedCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl MemcachedCodec {
    /// Create a new codec (auto-detects protocol).
    pub fn new() -> Self {
        Self {
            protocol: None,
            ascii: AsciiParser::new(),
            binary: BinaryParser::new(),
            last_opcode: 0,
            last_opaque: 0,
        }
    }

    /// Create a codec with a specific protocol type.
    pub fn with_protocol(protocol: ProtocolType) -> Self {
        Self {
            protocol: Some(protocol),
            ascii: AsciiParser::new(),
            binary: BinaryParser::new(),
            last_opcode: 0,
            last_opaque: 0,
        }
    }

    /// Get the current protocol type.
    pub fn protocol(&self) -> Option<ProtocolType> {
        self.protocol
    }

    /// Parse a command from the buffer.
    pub fn parse(&mut self, data: &[u8]) -> ParseResult {
        if data.is_empty() {
            return ParseResult::Incomplete;
        }

        // Auto-detect protocol if not yet determined
        let protocol = match self.protocol {
            Some(p) => p,
            None => {
                let detected = if data[0] == BINARY_REQUEST_MAGIC {
                    ProtocolType::Binary
                } else {
                    ProtocolType::Ascii
                };
                self.protocol = Some(detected);
                detected
            }
        };

        match protocol {
            ProtocolType::Ascii => self.ascii.parse(data),
            ProtocolType::Binary => {
                let result = self.binary.parse(data);
                // Store opcode and opaque for response encoding
                if let ParseResult::Ok(ref cmd, _) = result {
                    self.last_opaque = cmd.opaque;
                    // Map command name back to opcode for response
                    self.last_opcode = self.command_name_to_opcode(&cmd.name, cmd.noreply);
                }
                result
            }
        }
    }

    /// Encode a response to bytes.
    pub fn encode(&self, response: &MemcachedResponse, cas: u64) -> Vec<u8> {
        match self.protocol {
            Some(ProtocolType::Binary) => {
                BinaryEncoder::encode(response, self.last_opcode, self.last_opaque, cas)
            }
            _ => AsciiEncoder::encode(response),
        }
    }

    /// Map command name to binary opcode.
    fn command_name_to_opcode(&self, name: &str, quiet: bool) -> u8 {
        let base = match name {
            "get" => BinaryOpcode::Get,
            "set" => BinaryOpcode::Set,
            "add" => BinaryOpcode::Add,
            "replace" => BinaryOpcode::Replace,
            "delete" => BinaryOpcode::Delete,
            "incr" => BinaryOpcode::Increment,
            "decr" => BinaryOpcode::Decrement,
            "quit" => BinaryOpcode::Quit,
            "flush_all" => BinaryOpcode::Flush,
            "noop" => BinaryOpcode::Noop,
            "version" => BinaryOpcode::Version,
            "append" => BinaryOpcode::Append,
            "prepend" => BinaryOpcode::Prepend,
            "stats" => BinaryOpcode::Stat,
            "touch" => BinaryOpcode::Touch,
            "gat" => BinaryOpcode::Gat,
            _ => BinaryOpcode::Noop,
        };

        // Convert to quiet variant if needed
        if quiet {
            match base {
                BinaryOpcode::Get => BinaryOpcode::GetQ as u8,
                BinaryOpcode::Set => BinaryOpcode::SetQ as u8,
                BinaryOpcode::Add => BinaryOpcode::AddQ as u8,
                BinaryOpcode::Replace => BinaryOpcode::ReplaceQ as u8,
                BinaryOpcode::Delete => BinaryOpcode::DeleteQ as u8,
                BinaryOpcode::Increment => BinaryOpcode::IncrementQ as u8,
                BinaryOpcode::Decrement => BinaryOpcode::DecrementQ as u8,
                BinaryOpcode::Quit => BinaryOpcode::QuitQ as u8,
                BinaryOpcode::Flush => BinaryOpcode::FlushQ as u8,
                BinaryOpcode::Append => BinaryOpcode::AppendQ as u8,
                BinaryOpcode::Prepend => BinaryOpcode::PrependQ as u8,
                BinaryOpcode::Gat => BinaryOpcode::GatQ as u8,
                _ => base as u8,
            }
        } else {
            base as u8
        }
    }

    /// Reset parser state.
    pub fn reset_parser(&mut self) {
        self.ascii.reset();
        self.binary.reset();
    }
}

impl ProtocolCodec for MemcachedCodec {
    type Request = MemcachedCommand;
    type Response = MemcachedResponse;

    fn decode(&self, buffer: &mut BytesMut) -> DecodeResult<Self::Request> {
        if buffer.is_empty() {
            return DecodeResult::Incomplete;
        }

        // We need to use a mutable parser, but the trait takes &self
        // Create temporary parsers for decoding
        let protocol = if let Some(p) = self.protocol {
            p
        } else if buffer[0] == BINARY_REQUEST_MAGIC {
            ProtocolType::Binary
        } else {
            ProtocolType::Ascii
        };

        let result = match protocol {
            ProtocolType::Ascii => {
                let mut parser = AsciiParser::new();
                parser.parse(buffer)
            }
            ProtocolType::Binary => {
                let parser = BinaryParser::new();
                parser.parse(buffer)
            }
        };

        match result {
            ParseResult::Ok(cmd, consumed) => {
                let _ = buffer.split_to(consumed);
                DecodeResult::Complete(cmd)
            }
            ParseResult::Incomplete => DecodeResult::Incomplete,
            ParseResult::Error(e) => DecodeResult::Invalid(e),
        }
    }

    fn encode(&self, response: &Self::Response) -> EncodeResult {
        let bytes = self.encode(response, 0);
        EncodeResult::Ok(Bytes::from(bytes))
    }

    fn protocol_name(&self) -> &'static str {
        match self.protocol {
            Some(ProtocolType::Ascii) => "Memcached-ASCII",
            Some(ProtocolType::Binary) => "Memcached-Binary",
            None => "Memcached",
        }
    }

    fn reset(&mut self) {
        self.protocol = None;
        self.ascii.reset();
        self.binary.reset();
        self.last_opcode = 0;
        self.last_opaque = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_auto_detect_ascii() {
        let mut codec = MemcachedCodec::new();
        let result = codec.parse(b"get mykey\r\n");

        assert!(matches!(result, ParseResult::Ok(_, _)));
        assert_eq!(codec.protocol(), Some(ProtocolType::Ascii));
    }

    #[test]
    fn test_codec_auto_detect_binary() {
        let mut codec = MemcachedCodec::new();

        // Build a binary GET request
        let mut request = vec![0u8; 24];
        request[0] = BINARY_REQUEST_MAGIC;
        request[1] = BinaryOpcode::Get as u8;
        request[2] = 0; // key length high
        request[3] = 5; // key length low = 5
                        // extras length = 0
                        // data type = 0
                        // vbucket = 0
        request[8] = 0; // total body length = 5
        request[9] = 0;
        request[10] = 0;
        request[11] = 5;
        // opaque = 0
        // cas = 0
        request.extend_from_slice(b"mykey");

        let result = codec.parse(&request);
        assert!(matches!(result, ParseResult::Ok(_, _)));
        assert_eq!(codec.protocol(), Some(ProtocolType::Binary));
    }

    #[test]
    fn test_codec_with_explicit_protocol() {
        let mut codec = MemcachedCodec::with_protocol(ProtocolType::Ascii);
        assert_eq!(codec.protocol(), Some(ProtocolType::Ascii));

        let result = codec.parse(b"get key\r\n");
        assert!(matches!(result, ParseResult::Ok(_, _)));
    }

    #[test]
    fn test_codec_encode_ascii() {
        let codec = MemcachedCodec::with_protocol(ProtocolType::Ascii);
        let response = MemcachedResponse::stored();
        let encoded = codec.encode(&response, 0);
        assert_eq!(encoded, b"STORED\r\n");
    }

    #[test]
    fn test_codec_reset() {
        let mut codec = MemcachedCodec::new();
        let _ = codec.parse(b"get key\r\n");
        assert!(codec.protocol().is_some());

        codec.reset();
        assert!(codec.protocol().is_none());
    }

    #[test]
    fn test_codec_protocol_name() {
        let codec_ascii = MemcachedCodec::with_protocol(ProtocolType::Ascii);
        assert_eq!(codec_ascii.protocol_name(), "Memcached-ASCII");

        let codec_binary = MemcachedCodec::with_protocol(ProtocolType::Binary);
        assert_eq!(codec_binary.protocol_name(), "Memcached-Binary");

        let codec_unknown = MemcachedCodec::new();
        assert_eq!(codec_unknown.protocol_name(), "Memcached");
    }

    #[test]
    fn test_codec_incomplete() {
        let mut codec = MemcachedCodec::new();
        let result = codec.parse(b"get ");
        assert!(matches!(result, ParseResult::Incomplete));
    }

    #[test]
    fn test_protocol_codec_trait_decode() {
        let codec = MemcachedCodec::new();
        let mut buffer = BytesMut::from(&b"get mykey\r\n"[..]);

        let result = codec.decode(&mut buffer);
        assert!(matches!(result, DecodeResult::Complete(cmd) if cmd.name == "get"));
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_protocol_codec_trait_decode_incomplete() {
        let codec = MemcachedCodec::new();
        let mut buffer = BytesMut::from(&b"get mykey"[..]);

        let result = codec.decode(&mut buffer);
        assert!(matches!(result, DecodeResult::Incomplete));
        assert_eq!(buffer.len(), 9); // Not consumed
    }

    #[test]
    fn test_protocol_codec_trait_encode() {
        let codec = MemcachedCodec::with_protocol(ProtocolType::Ascii);
        let response = MemcachedResponse::stored();

        let result: EncodeResult = ProtocolCodec::encode(&codec, &response);
        match result {
            EncodeResult::Ok(bytes) => assert_eq!(&bytes[..], b"STORED\r\n"),
            EncodeResult::Error(_) => panic!("Expected Ok"),
        }
    }
}
