//! Memcached ASCII/binary protocol adapter.
//!
//! This module implements the Memcached protocol for compatibility with
//! Memcached clients. It supports both the ASCII text protocol and the
//! binary protocol.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Memcached Adapter                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  TCP Listener (with optional TLS)                           │
//! │    ↓                                                        │
//! │  Protocol Detection (ASCII vs Binary magic 0x80)            │
//! │    ↓                                                        │
//! │  Command Parser                                             │
//! │    ├── ASCII: <command> <key> [<args>]\r\n<data>\r\n        │
//! │    └── Binary: 24-byte header + body                        │
//! │    ↓                                                        │
//! │  Command Dispatcher                                         │
//! │    ├── Storage: set, add, replace, append, prepend, cas     │
//! │    ├── Retrieval: get, gets, gat, gats                      │
//! │    ├── Delete: delete                                       │
//! │    ├── Arithmetic: incr, decr                               │
//! │    └── Other: touch, stats, flush_all, version, quit        │
//! │    ↓                                                        │
//! │  KPG State Machine                                          │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Protocol Detection
//!
//! The adapter auto-detects the protocol based on the first byte:
//! - `0x80`: Binary protocol (request magic)
//! - ASCII letters: ASCII text protocol
//!
//! # LIN-BOUND Classification
//!
//! Commands are classified by their linearizability requirements:
//!
//! | Category | Commands | Requires LIN-BOUND |
//! |----------|----------|-------------------|
//! | Snapshot reads | get, gets | No |
//! | CAS operations | cas | Yes |
//! | Arithmetic | incr, decr | Yes |
//! | Writes | set, add, replace | No (follows Clustor durability) |

pub mod commands;
pub mod protocol;
pub mod service;

use bytes::Bytes;

// Re-export main types
pub use commands::{CommandHandler, CommandRouter, CommandState};
pub use protocol::MemcachedCodec;
pub use service::{MemcachedAdapter, MemcachedConfig};

/// Memcached protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolType {
    /// ASCII text protocol.
    #[default]
    Ascii,
    /// Binary protocol.
    Binary,
}

impl std::fmt::Display for ProtocolType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolType::Ascii => write!(f, "ASCII"),
            ProtocolType::Binary => write!(f, "Binary"),
        }
    }
}

/// Memcached command.
#[derive(Debug, Clone)]
pub struct MemcachedCommand {
    /// Command name (lowercase).
    pub name: String,

    /// Key (for single-key commands).
    pub key: Option<Bytes>,

    /// Additional keys (for multi-key commands like get).
    pub keys: Vec<Bytes>,

    /// Flags (32-bit client-defined).
    pub flags: u32,

    /// Expiration time.
    pub exptime: u32,

    /// Data/value bytes.
    pub data: Option<Bytes>,

    /// CAS token (for cas command).
    pub cas: Option<u64>,

    /// Delta value (for incr/decr).
    pub delta: Option<u64>,

    /// Initial value (for binary incr/decr).
    pub initial: Option<u64>,

    /// Noreply flag (suppress response).
    pub noreply: bool,

    /// Opaque value (binary protocol).
    pub opaque: u32,
}

impl MemcachedCommand {
    /// Create a new command.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into().to_lowercase(),
            key: None,
            keys: Vec::new(),
            flags: 0,
            exptime: 0,
            data: None,
            cas: None,
            delta: None,
            initial: None,
            noreply: false,
            opaque: 0,
        }
    }

    /// Set the key.
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Add a key (for multi-key operations).
    pub fn add_key(mut self, key: impl Into<Bytes>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Set keys directly.
    pub fn with_keys(mut self, keys: Vec<Bytes>) -> Self {
        self.keys = keys;
        self
    }

    /// Set flags.
    pub fn with_flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    /// Set expiration time.
    pub fn with_exptime(mut self, exptime: u32) -> Self {
        self.exptime = exptime;
        self
    }

    /// Set data.
    pub fn with_data(mut self, data: impl Into<Bytes>) -> Self {
        self.data = Some(data.into());
        self
    }

    /// Set CAS token.
    pub fn with_cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }

    /// Set delta (for incr/decr).
    pub fn with_delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }

    /// Set initial value (for binary incr/decr).
    pub fn with_initial(mut self, initial: u64) -> Self {
        self.initial = Some(initial);
        self
    }

    /// Set noreply flag.
    pub fn with_noreply(mut self, noreply: bool) -> Self {
        self.noreply = noreply;
        self
    }

    /// Set opaque value.
    pub fn with_opaque(mut self, opaque: u32) -> Self {
        self.opaque = opaque;
        self
    }

    /// Get the primary key as a string.
    pub fn key_str(&self) -> Option<&str> {
        self.key.as_ref().and_then(|k| std::str::from_utf8(k).ok())
    }

    /// Get all keys (primary + additional).
    pub fn all_keys(&self) -> impl Iterator<Item = &Bytes> {
        self.key.iter().chain(self.keys.iter())
    }

    /// Check if this is a storage command.
    pub fn is_storage(&self) -> bool {
        matches!(
            self.name.as_str(),
            "set" | "add" | "replace" | "append" | "prepend" | "cas"
        )
    }

    /// Check if this is a retrieval command.
    pub fn is_retrieval(&self) -> bool {
        matches!(self.name.as_str(), "get" | "gets" | "gat" | "gats")
    }

    /// Check if this command requires CAS token.
    pub fn requires_cas(&self) -> bool {
        matches!(self.name.as_str(), "cas" | "gets" | "gats")
    }

    /// Check if this command requires LIN-BOUND.
    pub fn requires_linearizable(&self) -> bool {
        matches!(self.name.as_str(), "cas" | "incr" | "decr")
    }
}

/// Memcached response.
#[derive(Debug, Clone)]
pub enum MemcachedResponse {
    /// Simple status response.
    Status(ResponseStatus),

    /// Value response (for get/gets).
    Value(ValueResponse),

    /// Multiple values response.
    Values(Vec<ValueResponse>),

    /// Numeric response (for incr/decr).
    Numeric(u64),

    /// Stats response (key-value pairs).
    Stats(Vec<(String, String)>),

    /// Version response.
    Version(String),

    /// Error response.
    Error(MemcachedError),

    /// No response (for noreply/quiet commands).
    NoResponse,
}

impl MemcachedResponse {
    /// Create a STORED response.
    pub fn stored() -> Self {
        Self::Status(ResponseStatus::Stored)
    }

    /// Create a NOT_STORED response.
    pub fn not_stored() -> Self {
        Self::Status(ResponseStatus::NotStored)
    }

    /// Create an EXISTS response.
    pub fn exists() -> Self {
        Self::Status(ResponseStatus::Exists)
    }

    /// Create a NOT_FOUND response.
    pub fn not_found() -> Self {
        Self::Status(ResponseStatus::NotFound)
    }

    /// Create a DELETED response.
    pub fn deleted() -> Self {
        Self::Status(ResponseStatus::Deleted)
    }

    /// Create a TOUCHED response.
    pub fn touched() -> Self {
        Self::Status(ResponseStatus::Touched)
    }

    /// Create an OK response.
    pub fn ok() -> Self {
        Self::Status(ResponseStatus::Ok)
    }

    /// Create a client error response.
    pub fn client_error(message: impl Into<String>) -> Self {
        Self::Error(MemcachedError::ClientError(message.into()))
    }

    /// Create a server error response.
    pub fn server_error(message: impl Into<String>) -> Self {
        Self::Error(MemcachedError::ServerError(message.into()))
    }

    /// Create a generic error response.
    pub fn error() -> Self {
        Self::Error(MemcachedError::Error)
    }

    /// Create a value response.
    pub fn value(key: Bytes, flags: u32, data: Bytes) -> Self {
        Self::Value(ValueResponse {
            key,
            flags,
            data,
            cas: None,
        })
    }

    /// Create a value response with CAS token.
    pub fn value_with_cas(key: Bytes, flags: u32, data: Bytes, cas: u64) -> Self {
        Self::Value(ValueResponse {
            key,
            flags,
            data,
            cas: Some(cas),
        })
    }
}

/// Simple status responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseStatus {
    /// Value stored successfully.
    Stored,
    /// Value not stored (add/replace failed condition).
    NotStored,
    /// CAS conflict (another client modified the key).
    Exists,
    /// Key not found.
    NotFound,
    /// Key deleted successfully.
    Deleted,
    /// Key touched successfully.
    Touched,
    /// Generic OK.
    Ok,
}

impl std::fmt::Display for ResponseStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseStatus::Stored => write!(f, "STORED"),
            ResponseStatus::NotStored => write!(f, "NOT_STORED"),
            ResponseStatus::Exists => write!(f, "EXISTS"),
            ResponseStatus::NotFound => write!(f, "NOT_FOUND"),
            ResponseStatus::Deleted => write!(f, "DELETED"),
            ResponseStatus::Touched => write!(f, "TOUCHED"),
            ResponseStatus::Ok => write!(f, "OK"),
        }
    }
}

/// Value response for get/gets.
#[derive(Debug, Clone)]
pub struct ValueResponse {
    /// Key.
    pub key: Bytes,
    /// Client flags.
    pub flags: u32,
    /// Value data.
    pub data: Bytes,
    /// CAS token (only for gets).
    pub cas: Option<u64>,
}

/// Memcached error types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemcachedError {
    /// Generic error (ERROR).
    Error,
    /// Client error with message.
    ClientError(String),
    /// Server error with message.
    ServerError(String),
}

impl std::fmt::Display for MemcachedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemcachedError::Error => write!(f, "ERROR"),
            MemcachedError::ClientError(msg) => write!(f, "CLIENT_ERROR {}", msg),
            MemcachedError::ServerError(msg) => write!(f, "SERVER_ERROR {}", msg),
        }
    }
}

impl std::error::Error for MemcachedError {}

/// Binary protocol status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum BinaryStatus {
    /// No error.
    NoError = 0x0000,
    /// Key not found.
    KeyNotFound = 0x0001,
    /// Key exists (CAS conflict).
    KeyExists = 0x0002,
    /// Value too large.
    ValueTooLarge = 0x0003,
    /// Invalid arguments.
    InvalidArguments = 0x0004,
    /// Item not stored.
    ItemNotStored = 0x0005,
    /// Incr/decr on non-numeric value.
    DeltaBadval = 0x0006,
    /// VBucket belongs to another server.
    VbucketBelongsToAnotherServer = 0x0007,
    /// Authentication error.
    AuthenticationError = 0x0008,
    /// Authentication continue.
    AuthenticationContinue = 0x0009,
    /// Unknown command.
    UnknownCommand = 0x0081,
    /// Out of memory.
    OutOfMemory = 0x0082,
    /// Not supported.
    NotSupported = 0x0083,
    /// Internal error.
    InternalError = 0x0084,
    /// Busy.
    Busy = 0x0085,
    /// Temporary failure.
    TemporaryFailure = 0x0086,
}

impl From<BinaryStatus> for u16 {
    fn from(status: BinaryStatus) -> Self {
        status as u16
    }
}

impl TryFrom<u16> for BinaryStatus {
    type Error = u16;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0x0000 => Ok(BinaryStatus::NoError),
            0x0001 => Ok(BinaryStatus::KeyNotFound),
            0x0002 => Ok(BinaryStatus::KeyExists),
            0x0003 => Ok(BinaryStatus::ValueTooLarge),
            0x0004 => Ok(BinaryStatus::InvalidArguments),
            0x0005 => Ok(BinaryStatus::ItemNotStored),
            0x0006 => Ok(BinaryStatus::DeltaBadval),
            0x0007 => Ok(BinaryStatus::VbucketBelongsToAnotherServer),
            0x0008 => Ok(BinaryStatus::AuthenticationError),
            0x0009 => Ok(BinaryStatus::AuthenticationContinue),
            0x0081 => Ok(BinaryStatus::UnknownCommand),
            0x0082 => Ok(BinaryStatus::OutOfMemory),
            0x0083 => Ok(BinaryStatus::NotSupported),
            0x0084 => Ok(BinaryStatus::InternalError),
            0x0085 => Ok(BinaryStatus::Busy),
            0x0086 => Ok(BinaryStatus::TemporaryFailure),
            _ => Err(value),
        }
    }
}

/// Binary protocol opcodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BinaryOpcode {
    Get = 0x00,
    Set = 0x01,
    Add = 0x02,
    Replace = 0x03,
    Delete = 0x04,
    Increment = 0x05,
    Decrement = 0x06,
    Quit = 0x07,
    Flush = 0x08,
    GetQ = 0x09,
    Noop = 0x0a,
    Version = 0x0b,
    GetK = 0x0c,
    GetKQ = 0x0d,
    Append = 0x0e,
    Prepend = 0x0f,
    Stat = 0x10,
    SetQ = 0x11,
    AddQ = 0x12,
    ReplaceQ = 0x13,
    DeleteQ = 0x14,
    IncrementQ = 0x15,
    DecrementQ = 0x16,
    QuitQ = 0x17,
    FlushQ = 0x18,
    AppendQ = 0x19,
    PrependQ = 0x1a,
    Touch = 0x1c,
    Gat = 0x1d,
    GatQ = 0x1e,
    GatK = 0x23,
    GatKQ = 0x24,
    // SASL commands
    SaslListMechs = 0x20,
    SaslAuth = 0x21,
    SaslStep = 0x22,
}

impl TryFrom<u8> for BinaryOpcode {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(BinaryOpcode::Get),
            0x01 => Ok(BinaryOpcode::Set),
            0x02 => Ok(BinaryOpcode::Add),
            0x03 => Ok(BinaryOpcode::Replace),
            0x04 => Ok(BinaryOpcode::Delete),
            0x05 => Ok(BinaryOpcode::Increment),
            0x06 => Ok(BinaryOpcode::Decrement),
            0x07 => Ok(BinaryOpcode::Quit),
            0x08 => Ok(BinaryOpcode::Flush),
            0x09 => Ok(BinaryOpcode::GetQ),
            0x0a => Ok(BinaryOpcode::Noop),
            0x0b => Ok(BinaryOpcode::Version),
            0x0c => Ok(BinaryOpcode::GetK),
            0x0d => Ok(BinaryOpcode::GetKQ),
            0x0e => Ok(BinaryOpcode::Append),
            0x0f => Ok(BinaryOpcode::Prepend),
            0x10 => Ok(BinaryOpcode::Stat),
            0x11 => Ok(BinaryOpcode::SetQ),
            0x12 => Ok(BinaryOpcode::AddQ),
            0x13 => Ok(BinaryOpcode::ReplaceQ),
            0x14 => Ok(BinaryOpcode::DeleteQ),
            0x15 => Ok(BinaryOpcode::IncrementQ),
            0x16 => Ok(BinaryOpcode::DecrementQ),
            0x17 => Ok(BinaryOpcode::QuitQ),
            0x18 => Ok(BinaryOpcode::FlushQ),
            0x19 => Ok(BinaryOpcode::AppendQ),
            0x1a => Ok(BinaryOpcode::PrependQ),
            0x1c => Ok(BinaryOpcode::Touch),
            0x1d => Ok(BinaryOpcode::Gat),
            0x1e => Ok(BinaryOpcode::GatQ),
            0x20 => Ok(BinaryOpcode::SaslListMechs),
            0x21 => Ok(BinaryOpcode::SaslAuth),
            0x22 => Ok(BinaryOpcode::SaslStep),
            0x23 => Ok(BinaryOpcode::GatK),
            0x24 => Ok(BinaryOpcode::GatKQ),
            _ => Err(value),
        }
    }
}

impl BinaryOpcode {
    /// Check if this is a quiet (no-response-on-success) opcode.
    pub fn is_quiet(&self) -> bool {
        matches!(
            self,
            BinaryOpcode::GetQ
                | BinaryOpcode::GetKQ
                | BinaryOpcode::SetQ
                | BinaryOpcode::AddQ
                | BinaryOpcode::ReplaceQ
                | BinaryOpcode::DeleteQ
                | BinaryOpcode::IncrementQ
                | BinaryOpcode::DecrementQ
                | BinaryOpcode::QuitQ
                | BinaryOpcode::FlushQ
                | BinaryOpcode::AppendQ
                | BinaryOpcode::PrependQ
                | BinaryOpcode::GatQ
                | BinaryOpcode::GatKQ
        )
    }

    /// Get the non-quiet version of this opcode.
    pub fn to_non_quiet(&self) -> Self {
        match self {
            BinaryOpcode::GetQ => BinaryOpcode::Get,
            BinaryOpcode::GetKQ => BinaryOpcode::GetK,
            BinaryOpcode::SetQ => BinaryOpcode::Set,
            BinaryOpcode::AddQ => BinaryOpcode::Add,
            BinaryOpcode::ReplaceQ => BinaryOpcode::Replace,
            BinaryOpcode::DeleteQ => BinaryOpcode::Delete,
            BinaryOpcode::IncrementQ => BinaryOpcode::Increment,
            BinaryOpcode::DecrementQ => BinaryOpcode::Decrement,
            BinaryOpcode::QuitQ => BinaryOpcode::Quit,
            BinaryOpcode::FlushQ => BinaryOpcode::Flush,
            BinaryOpcode::AppendQ => BinaryOpcode::Append,
            BinaryOpcode::PrependQ => BinaryOpcode::Prepend,
            BinaryOpcode::GatQ => BinaryOpcode::Gat,
            BinaryOpcode::GatKQ => BinaryOpcode::GatK,
            _ => *self,
        }
    }

    /// Convert opcode to command name.
    pub fn to_command_name(&self) -> &'static str {
        match self.to_non_quiet() {
            BinaryOpcode::Get | BinaryOpcode::GetK => "get",
            BinaryOpcode::Set => "set",
            BinaryOpcode::Add => "add",
            BinaryOpcode::Replace => "replace",
            BinaryOpcode::Delete => "delete",
            BinaryOpcode::Increment => "incr",
            BinaryOpcode::Decrement => "decr",
            BinaryOpcode::Quit => "quit",
            BinaryOpcode::Flush => "flush_all",
            BinaryOpcode::Noop => "noop",
            BinaryOpcode::Version => "version",
            BinaryOpcode::Append => "append",
            BinaryOpcode::Prepend => "prepend",
            BinaryOpcode::Stat => "stats",
            BinaryOpcode::Touch => "touch",
            BinaryOpcode::Gat | BinaryOpcode::GatK => "gat",
            BinaryOpcode::SaslListMechs => "sasl_list_mechs",
            BinaryOpcode::SaslAuth => "sasl_auth",
            BinaryOpcode::SaslStep => "sasl_step",
            _ => "unknown",
        }
    }
}

/// Binary protocol magic bytes.
pub const BINARY_REQUEST_MAGIC: u8 = 0x80;
pub const BINARY_RESPONSE_MAGIC: u8 = 0x81;

/// Binary protocol header size.
pub const BINARY_HEADER_SIZE: usize = 24;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_creation() {
        let cmd = MemcachedCommand::new("SET")
            .with_key("mykey")
            .with_flags(42)
            .with_exptime(3600)
            .with_data("myvalue");

        assert_eq!(cmd.name, "set");
        assert_eq!(cmd.key_str(), Some("mykey"));
        assert_eq!(cmd.flags, 42);
        assert_eq!(cmd.exptime, 3600);
        assert!(cmd.is_storage());
        assert!(!cmd.is_retrieval());
    }

    #[test]
    fn test_multi_key_command() {
        let cmd = MemcachedCommand::new("get")
            .with_key("key1")
            .add_key("key2")
            .add_key("key3");

        let keys: Vec<_> = cmd.all_keys().collect();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_response_status_display() {
        assert_eq!(format!("{}", ResponseStatus::Stored), "STORED");
        assert_eq!(format!("{}", ResponseStatus::NotStored), "NOT_STORED");
        assert_eq!(format!("{}", ResponseStatus::NotFound), "NOT_FOUND");
    }

    #[test]
    fn test_binary_opcode_quiet() {
        assert!(BinaryOpcode::GetQ.is_quiet());
        assert!(BinaryOpcode::SetQ.is_quiet());
        assert!(!BinaryOpcode::Get.is_quiet());
        assert!(!BinaryOpcode::Set.is_quiet());
    }

    #[test]
    fn test_binary_opcode_to_non_quiet() {
        assert_eq!(BinaryOpcode::GetQ.to_non_quiet(), BinaryOpcode::Get);
        assert_eq!(BinaryOpcode::SetQ.to_non_quiet(), BinaryOpcode::Set);
        assert_eq!(BinaryOpcode::Get.to_non_quiet(), BinaryOpcode::Get);
    }

    #[test]
    fn test_binary_status_conversion() {
        assert_eq!(u16::from(BinaryStatus::NoError), 0x0000);
        assert_eq!(u16::from(BinaryStatus::KeyNotFound), 0x0001);
        assert_eq!(
            BinaryStatus::try_from(0x0001),
            Ok(BinaryStatus::KeyNotFound)
        );
        assert!(BinaryStatus::try_from(0xFFFF).is_err());
    }
}
