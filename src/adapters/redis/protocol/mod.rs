//! Redis protocol parsing.
//!
//! This module implements RESP2 and RESP3 protocol parsing and encoding.

pub mod codec;
pub mod resp2;
pub mod resp3;

pub use codec::RespCodec;
pub use resp2::{Resp2Encoder, Resp2Parser};
pub use resp3::{Resp3Encoder, Resp3Parser};

use super::{ProtocolVersion, RedisValue};

/// Maximum nesting depth for arrays/maps.
pub const MAX_NESTING_DEPTH: usize = 32;

/// Maximum bulk string size (512MB).
pub const MAX_BULK_SIZE: usize = 512 * 1024 * 1024;

/// Maximum array/map elements.
pub const MAX_ELEMENTS: usize = 1_000_000;

/// RESP type byte prefixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespType {
    /// Simple string (+)
    SimpleString,
    /// Error (-)
    Error,
    /// Integer (:)
    Integer,
    /// Bulk string ($)
    BulkString,
    /// Array (*)
    Array,
    /// Null (_) - RESP3
    Null,
    /// Boolean (#) - RESP3
    Boolean,
    /// Double (,) - RESP3
    Double,
    /// Big number (() - RESP3
    BigNumber,
    /// Bulk error (!) - RESP3
    BulkError,
    /// Verbatim string (=) - RESP3
    VerbatimString,
    /// Map (%) - RESP3
    Map,
    /// Set (~) - RESP3
    Set,
    /// Attribute (|) - RESP3
    Attribute,
    /// Push (>) - RESP3
    Push,
}

impl RespType {
    /// Get the type from a prefix byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'+' => Some(Self::SimpleString),
            b'-' => Some(Self::Error),
            b':' => Some(Self::Integer),
            b'$' => Some(Self::BulkString),
            b'*' => Some(Self::Array),
            b'_' => Some(Self::Null),
            b'#' => Some(Self::Boolean),
            b',' => Some(Self::Double),
            b'(' => Some(Self::BigNumber),
            b'!' => Some(Self::BulkError),
            b'=' => Some(Self::VerbatimString),
            b'%' => Some(Self::Map),
            b'~' => Some(Self::Set),
            b'|' => Some(Self::Attribute),
            b'>' => Some(Self::Push),
            _ => None,
        }
    }

    /// Get the prefix byte for this type.
    pub fn prefix(&self) -> u8 {
        match self {
            Self::SimpleString => b'+',
            Self::Error => b'-',
            Self::Integer => b':',
            Self::BulkString => b'$',
            Self::Array => b'*',
            Self::Null => b'_',
            Self::Boolean => b'#',
            Self::Double => b',',
            Self::BigNumber => b'(',
            Self::BulkError => b'!',
            Self::VerbatimString => b'=',
            Self::Map => b'%',
            Self::Set => b'~',
            Self::Attribute => b'|',
            Self::Push => b'>',
        }
    }

    /// Check if this type is RESP3-only.
    pub fn is_resp3_only(&self) -> bool {
        matches!(
            self,
            Self::Null
                | Self::Boolean
                | Self::Double
                | Self::BigNumber
                | Self::BulkError
                | Self::VerbatimString
                | Self::Map
                | Self::Set
                | Self::Attribute
                | Self::Push
        )
    }
}

/// Parse result.
#[derive(Debug)]
pub enum ParseResult {
    /// Successfully parsed a value.
    Ok(RedisValue),
    /// Need more data.
    Incomplete,
    /// Parse error.
    Error(String),
}

/// Encode a RedisValue to bytes for the given protocol version.
pub fn encode_value(value: &RedisValue, version: ProtocolVersion) -> Vec<u8> {
    match version {
        ProtocolVersion::Resp2 => Resp2Encoder::encode(value),
        ProtocolVersion::Resp3 => Resp3Encoder::encode(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resp_type_from_byte() {
        assert_eq!(RespType::from_byte(b'+'), Some(RespType::SimpleString));
        assert_eq!(RespType::from_byte(b'-'), Some(RespType::Error));
        assert_eq!(RespType::from_byte(b':'), Some(RespType::Integer));
        assert_eq!(RespType::from_byte(b'$'), Some(RespType::BulkString));
        assert_eq!(RespType::from_byte(b'*'), Some(RespType::Array));
        assert_eq!(RespType::from_byte(b'_'), Some(RespType::Null));
        assert_eq!(RespType::from_byte(b'%'), Some(RespType::Map));
        assert_eq!(RespType::from_byte(b'x'), None);
    }

    #[test]
    fn test_resp_type_prefix() {
        assert_eq!(RespType::SimpleString.prefix(), b'+');
        assert_eq!(RespType::Array.prefix(), b'*');
        assert_eq!(RespType::Map.prefix(), b'%');
    }

    #[test]
    fn test_resp3_only() {
        assert!(!RespType::SimpleString.is_resp3_only());
        assert!(!RespType::Array.is_resp3_only());
        assert!(RespType::Null.is_resp3_only());
        assert!(RespType::Map.is_resp3_only());
        assert!(RespType::Boolean.is_resp3_only());
    }
}
