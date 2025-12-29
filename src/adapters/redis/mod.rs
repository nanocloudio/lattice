//! Redis RESP2/RESP3 protocol adapter.
//!
//! This module implements the Redis Serialization Protocol (RESP) for
//! compatibility with Redis clients. It supports both RESP2 (Redis 2.0+)
//! and RESP3 (Redis 6.0+) protocols.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Redis Adapter                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │  TCP Listener (with optional TLS)                           │
//! │    ↓                                                        │
//! │  Protocol Detection (RESP2 vs RESP3 via HELLO)              │
//! │    ↓                                                        │
//! │  Command Parser                                             │
//! │    ↓                                                        │
//! │  Command Dispatcher                                         │
//! │    ├── String Commands (GET, SET, etc.)                     │
//! │    ├── Key Commands (DEL, EXISTS, EXPIRE, etc.)             │
//! │    ├── Transaction Commands (MULTI, EXEC, WATCH)            │
//! │    ├── Server Commands (PING, INFO, AUTH)                   │
//! │    └── Pub/Sub Commands (SUBSCRIBE, PUBLISH)                │
//! │    ↓                                                        │
//! │  KPG State Machine                                          │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Protocol Versions
//!
//! - **RESP2**: Default protocol, uses arrays for most responses
//! - **RESP3**: Negotiated via HELLO command, adds maps, sets, etc.
//!
//! # LIN-BOUND Classification
//!
//! Commands are classified by their linearizability requirements:
//!
//! | Category | Commands | Requires LIN-BOUND |
//! |----------|----------|-------------------|
//! | Snapshot reads | GET, MGET, EXISTS | No |
//! | CAS operations | SET NX/XX, INCR | Yes |
//! | Transactions | WATCH, EXEC | Yes |
//! | Writes | SET, DEL | No (follows Clustor durability) |

pub mod commands;
pub mod protocol;
pub mod service;

use bytes::Bytes;

// Re-export main types
pub use commands::{CommandRouter, CommandState, KvStore, PubSubManager};
pub use service::{MockKvStore, RedisAdapter, RedisConfig};

/// Redis protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// RESP2 (Redis 2.0+)
    #[default]
    Resp2,
    /// RESP3 (Redis 6.0+)
    Resp3,
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolVersion::Resp2 => write!(f, "2"),
            ProtocolVersion::Resp3 => write!(f, "3"),
        }
    }
}

/// Redis value type (RESP3 type system).
#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    /// Simple string (+OK\r\n)
    SimpleString(String),

    /// Error (-ERR message\r\n)
    Error(RedisError),

    /// Integer (:1000\r\n)
    Integer(i64),

    /// Bulk string ($6\r\nfoobar\r\n)
    BulkString(Bytes),

    /// Array (*2\r\n...)
    Array(Vec<RedisValue>),

    /// Null (_\r\n in RESP3, $-1\r\n or *-1\r\n in RESP2)
    Null,

    /// Boolean (#t\r\n or #f\r\n) - RESP3 only
    Boolean(bool),

    /// Double (,1.23\r\n) - RESP3 only
    Double(f64),

    /// Big number ((123456789...\r\n) - RESP3 only
    BigNumber(String),

    /// Bulk error (!21\r\nSYNTAX invalid\r\n) - RESP3 only
    BulkError(RedisError),

    /// Verbatim string (=15\r\ntxt:Some text\r\n) - RESP3 only
    VerbatimString { format: String, data: Bytes },

    /// Map (%2\r\n+key\r\n:1\r\n...) - RESP3 only
    Map(Vec<(RedisValue, RedisValue)>),

    /// Set (~3\r\n+a\r\n+b\r\n+c\r\n) - RESP3 only
    Set(Vec<RedisValue>),

    /// Push (>2\r\n+message\r\n...) - RESP3 only
    Push(Vec<RedisValue>),
}

impl RedisValue {
    /// Create a simple string.
    pub fn simple_string(s: impl Into<String>) -> Self {
        Self::SimpleString(s.into())
    }

    /// Create an OK response.
    pub fn ok() -> Self {
        Self::SimpleString("OK".to_string())
    }

    /// Create a PONG response.
    pub fn pong() -> Self {
        Self::SimpleString("PONG".to_string())
    }

    /// Create an error.
    pub fn error(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error(RedisError {
            kind: kind.into(),
            message: message.into(),
        })
    }

    /// Create a generic error.
    pub fn err(message: impl Into<String>) -> Self {
        Self::error("ERR", message)
    }

    /// Create a WRONGTYPE error.
    pub fn wrong_type() -> Self {
        Self::error(
            "WRONGTYPE",
            "Operation against a key holding the wrong kind of value",
        )
    }

    /// Create an integer.
    pub fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Create a bulk string.
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Self::BulkString(data.into())
    }

    /// Create a bulk string from a string.
    pub fn bulk_string(s: impl AsRef<str>) -> Self {
        Self::BulkString(Bytes::from(s.as_ref().to_string()))
    }

    /// Create an array.
    pub fn array(items: Vec<RedisValue>) -> Self {
        Self::Array(items)
    }

    /// Create an empty array.
    pub fn empty_array() -> Self {
        Self::Array(vec![])
    }

    /// Create a map (RESP3).
    pub fn map(pairs: Vec<(RedisValue, RedisValue)>) -> Self {
        Self::Map(pairs)
    }

    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Check if this value is an error.
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_) | Self::BulkError(_))
    }

    /// Try to get as string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::SimpleString(s) => Some(s),
            Self::BulkString(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::SimpleString(s) => Some(s.as_bytes()),
            Self::BulkString(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get as integer.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Integer(n) => Some(*n),
            Self::SimpleString(s) | Self::BigNumber(s) => s.parse().ok(),
            Self::BulkString(b) => std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()),
            _ => None,
        }
    }

    /// Try to get as array.
    pub fn as_array(&self) -> Option<&[RedisValue]> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Convert to array, consuming self.
    pub fn into_array(self) -> Option<Vec<RedisValue>> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }
}

/// Redis error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisError {
    /// Error kind (ERR, WRONGTYPE, NOSCRIPT, etc.)
    pub kind: String,
    /// Error message.
    pub message: String,
}

impl RedisError {
    /// Create a new error.
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }

    /// Create a generic error.
    pub fn generic(message: impl Into<String>) -> Self {
        Self::new("ERR", message)
    }

    /// Create a syntax error.
    pub fn syntax() -> Self {
        Self::new("ERR", "syntax error")
    }

    /// Create a wrong number of arguments error.
    pub fn wrong_arity(command: &str) -> Self {
        Self::new(
            "ERR",
            format!("wrong number of arguments for '{}' command", command),
        )
    }

    /// Create a WRONGTYPE error.
    pub fn wrong_type() -> Self {
        Self::new(
            "WRONGTYPE",
            "Operation against a key holding the wrong kind of value",
        )
    }

    /// Create a NOAUTH error.
    pub fn noauth() -> Self {
        Self::new("NOAUTH", "Authentication required")
    }

    /// Create an invalid password error.
    pub fn invalid_password() -> Self {
        Self::new("WRONGPASS", "invalid username-password pair")
    }

    /// Create an OOM error.
    pub fn oom() -> Self {
        Self::new("OOM", "command not allowed when used memory > 'maxmemory'")
    }

    /// Create a BUSY error.
    pub fn busy(message: impl Into<String>) -> Self {
        Self::new("BUSY", message)
    }

    /// Create a LOADING error.
    pub fn loading() -> Self {
        Self::new("LOADING", "Redis is loading the dataset in memory")
    }

    /// Create a MASTERDOWN error (maps to LinearizabilityUnavailable).
    pub fn masterdown() -> Self {
        Self::new(
            "MASTERDOWN",
            "Link with MASTER is down and replica-serve-stale-data is set to 'no'",
        )
    }

    /// Create a READONLY error.
    pub fn readonly() -> Self {
        Self::new("READONLY", "You can't write against a read only replica")
    }

    /// Create a CROSSSLOT error.
    pub fn crossslot() -> Self {
        Self::new("CROSSSLOT", "Keys in request don't hash to the same slot")
    }

    /// Create an EXECABORT error.
    pub fn execabort() -> Self {
        Self::new(
            "EXECABORT",
            "Transaction discarded because of previous errors",
        )
    }
}

impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.kind, self.message)
    }
}

impl std::error::Error for RedisError {}

/// Redis command.
#[derive(Debug, Clone)]
pub struct RedisCommand {
    /// Command name (uppercase).
    pub name: String,

    /// Command arguments.
    pub args: Vec<Bytes>,
}

impl RedisCommand {
    /// Create a new command.
    pub fn new(name: impl Into<String>, args: Vec<Bytes>) -> Self {
        Self {
            name: name.into().to_uppercase(),
            args,
        }
    }

    /// Parse from a RESP array.
    pub fn from_array(values: Vec<RedisValue>) -> Result<Self, RedisError> {
        if values.is_empty() {
            return Err(RedisError::syntax());
        }

        let name = match &values[0] {
            RedisValue::BulkString(b) => {
                String::from_utf8(b.to_vec()).map_err(|_| RedisError::syntax())?
            }
            RedisValue::SimpleString(s) => s.clone(),
            _ => return Err(RedisError::syntax()),
        };

        let args = values
            .into_iter()
            .skip(1)
            .map(|v| match v {
                RedisValue::BulkString(b) => Ok(b),
                RedisValue::SimpleString(s) => Ok(Bytes::from(s)),
                RedisValue::Integer(n) => Ok(Bytes::from(n.to_string())),
                _ => Err(RedisError::syntax()),
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self::new(name, args))
    }

    /// Get argument as string.
    pub fn arg_str(&self, index: usize) -> Option<&str> {
        self.args
            .get(index)
            .and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Get argument as bytes.
    pub fn arg(&self, index: usize) -> Option<&Bytes> {
        self.args.get(index)
    }

    /// Get argument as integer.
    pub fn arg_int(&self, index: usize) -> Option<i64> {
        self.arg_str(index).and_then(|s| s.parse().ok())
    }

    /// Get number of arguments.
    pub fn argc(&self) -> usize {
        self.args.len()
    }

    /// Check if command has at least n arguments.
    pub fn has_args(&self, n: usize) -> bool {
        self.args.len() >= n
    }

    /// Validate argument count (exact).
    pub fn require_argc(&self, n: usize) -> Result<(), RedisError> {
        if self.args.len() != n {
            return Err(RedisError::wrong_arity(&self.name));
        }
        Ok(())
    }

    /// Validate argument count (at least).
    pub fn require_min_argc(&self, n: usize) -> Result<(), RedisError> {
        if self.args.len() < n {
            return Err(RedisError::wrong_arity(&self.name));
        }
        Ok(())
    }

    /// Validate argument count (range).
    pub fn require_argc_range(&self, min: usize, max: usize) -> Result<(), RedisError> {
        if self.args.len() < min || self.args.len() > max {
            return Err(RedisError::wrong_arity(&self.name));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_value_constructors() {
        assert!(matches!(RedisValue::ok(), RedisValue::SimpleString(s) if s == "OK"));
        assert!(matches!(RedisValue::pong(), RedisValue::SimpleString(s) if s == "PONG"));
        assert!(matches!(RedisValue::integer(42), RedisValue::Integer(42)));
        assert!(RedisValue::Null.is_null());
    }

    #[test]
    fn test_redis_value_accessors() {
        let s = RedisValue::simple_string("hello");
        assert_eq!(s.as_str(), Some("hello"));
        assert_eq!(s.as_bytes(), Some(b"hello".as_slice()));

        let n = RedisValue::integer(42);
        assert_eq!(n.as_int(), Some(42));

        let arr = RedisValue::array(vec![RedisValue::integer(1), RedisValue::integer(2)]);
        assert_eq!(arr.as_array().map(|a| a.len()), Some(2));
    }

    #[test]
    fn test_redis_error_constructors() {
        let e = RedisError::generic("something went wrong");
        assert_eq!(e.kind, "ERR");

        let e = RedisError::wrong_arity("GET");
        assert!(e.message.contains("GET"));

        let e = RedisError::crossslot();
        assert_eq!(e.kind, "CROSSSLOT");
    }

    #[test]
    fn test_redis_command_from_array() {
        let arr = vec![
            RedisValue::bulk_string("SET"),
            RedisValue::bulk_string("key"),
            RedisValue::bulk_string("value"),
        ];

        let cmd = RedisCommand::from_array(arr).unwrap();
        assert_eq!(cmd.name, "SET");
        assert_eq!(cmd.argc(), 2);
        assert_eq!(cmd.arg_str(0), Some("key"));
        assert_eq!(cmd.arg_str(1), Some("value"));
    }

    #[test]
    fn test_redis_command_validation() {
        let cmd = RedisCommand::new("GET", vec![Bytes::from("key")]);

        assert!(cmd.require_argc(1).is_ok());
        assert!(cmd.require_argc(2).is_err());
        assert!(cmd.require_min_argc(1).is_ok());
        assert!(cmd.require_min_argc(2).is_err());
        assert!(cmd.require_argc_range(1, 3).is_ok());
    }
}
