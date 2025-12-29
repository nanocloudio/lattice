//! Memcached command handlers.
//!
//! This module implements command handlers for the Memcached protocol.
//! Commands are organized into categories:
//!
//! - Storage: set, add, replace, append, prepend, cas
//! - Retrieval: get, gets, gat, gats
//! - Delete: delete
//! - Arithmetic: incr, decr
//! - Other: touch, stats, flush_all, version, quit

pub mod other;
pub mod retrieval;
pub mod storage;

use crate::adapters::memcached::{MemcachedCommand, MemcachedResponse};
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

// Re-exports
pub use other::{
    DecrHandler, DeleteHandler, FlushAllHandler, IncrHandler, NoopHandler, QuitHandler,
    StatsHandler, TouchHandler, VersionHandler,
};
pub use retrieval::{GatHandler, GetHandler, GetsHandler};
pub use storage::{
    AddHandler, AppendHandler, CasHandler, PrependHandler, ReplaceHandler, SetHandler,
};

/// Command execution result.
pub type CommandResult = Result<MemcachedResponse, MemcachedResponse>;

/// Command handler trait.
pub trait CommandHandler: Send + Sync {
    /// Execute the command.
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>>;
}

/// Context for command execution.
#[derive(Debug, Clone)]
pub struct CommandContext {
    /// Tenant ID.
    pub tenant_id: String,

    /// Connection ID.
    pub connection_id: u64,

    /// Whether linearizable reads are enabled.
    pub linearizable: bool,

    /// Client address.
    pub client_addr: Option<std::net::SocketAddr>,
}

impl CommandContext {
    /// Create a new command context.
    pub fn new(tenant_id: impl Into<String>, connection_id: u64) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            connection_id,
            linearizable: false,
            client_addr: None,
        }
    }

    /// Set linearizable read mode.
    pub fn with_linearizable(mut self, linearizable: bool) -> Self {
        self.linearizable = linearizable;
        self
    }

    /// Set client address.
    pub fn with_client_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.client_addr = Some(addr);
        self
    }

    /// Build the full key with tenant prefix.
    pub fn prefixed_key(&self, key: &[u8]) -> Bytes {
        let mut prefixed = Vec::with_capacity(self.tenant_id.len() + 1 + key.len());
        prefixed.extend_from_slice(self.tenant_id.as_bytes());
        prefixed.push(b'/');
        prefixed.extend_from_slice(key);
        Bytes::from(prefixed)
    }
}

/// Shared state for command execution.
pub struct CommandState {
    /// KV store.
    pub kv: Arc<dyn KvStore>,

    /// Server version.
    pub version: String,

    /// Server start time.
    pub start_time: std::time::Instant,
}

impl CommandState {
    /// Create a new command state.
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self {
            kv,
            version: format!("Lattice/{}", env!("CARGO_PKG_VERSION")),
            start_time: std::time::Instant::now(),
        }
    }

    /// Get server uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

/// Future type alias for KvStore operations returning multiple values.
pub type KvMultiResult<'a, T> = Pin<Box<dyn Future<Output = Result<Vec<T>, String>> + Send + 'a>>;

/// KV store abstraction for command handlers.
///
/// Uses `Pin<Box<dyn Future<...>>>` pattern for dyn-compatibility.
#[allow(clippy::type_complexity)]
pub trait KvStore: Send + Sync {
    /// Get a value.
    fn get(
        &self,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Option<KvValue>, String>> + Send + '_>>;

    /// Get multiple values.
    fn get_multi(&self, keys: &[&[u8]]) -> KvMultiResult<'_, Option<KvValue>>;

    /// Set a value.
    fn set(
        &self,
        key: &[u8],
        value: KvValue,
    ) -> Pin<Box<dyn Future<Output = Result<u64, String>> + Send + '_>>;

    /// Set a value only if key doesn't exist.
    fn add(
        &self,
        key: &[u8],
        value: KvValue,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, String>> + Send + '_>>;

    /// Set a value only if key exists.
    fn replace(
        &self,
        key: &[u8],
        value: KvValue,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, String>> + Send + '_>>;

    /// Delete a key.
    fn delete(&self, key: &[u8])
        -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>>;

    /// Check-and-set: set if CAS matches.
    fn cas(
        &self,
        key: &[u8],
        value: KvValue,
        cas: u64,
    ) -> Pin<Box<dyn Future<Output = Result<CasResult, String>> + Send + '_>>;

    /// Append to existing value.
    fn append(
        &self,
        key: &[u8],
        data: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>>;

    /// Prepend to existing value.
    fn prepend(
        &self,
        key: &[u8],
        data: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>>;

    /// Increment a numeric value.
    fn incr(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        exptime: u32,
    ) -> Pin<Box<dyn Future<Output = Result<IncrResult, String>> + Send + '_>>;

    /// Decrement a numeric value.
    fn decr(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        exptime: u32,
    ) -> Pin<Box<dyn Future<Output = Result<IncrResult, String>> + Send + '_>>;

    /// Touch (update expiration).
    fn touch(
        &self,
        key: &[u8],
        exptime: u32,
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>>;

    /// Flush all keys.
    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;

    /// Get stats.
    fn stats(&self) -> KvMultiResult<'_, (String, String)>;
}

/// Value stored in KV.
#[derive(Debug, Clone)]
pub struct KvValue {
    /// Data bytes.
    pub data: Bytes,

    /// Client flags.
    pub flags: u32,

    /// Expiration time (0 = never).
    pub exptime: u32,

    /// CAS token (mod revision).
    pub cas: u64,
}

impl KvValue {
    /// Create a new KV value.
    pub fn new(data: impl Into<Bytes>, flags: u32, exptime: u32) -> Self {
        Self {
            data: data.into(),
            flags,
            exptime,
            cas: 0,
        }
    }

    /// Set CAS token.
    pub fn with_cas(mut self, cas: u64) -> Self {
        self.cas = cas;
        self
    }
}

/// CAS operation result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasResult {
    /// Successfully stored.
    Stored(u64),
    /// Key exists but CAS doesn't match.
    Exists,
    /// Key not found.
    NotFound,
}

/// Increment/decrement result.
#[derive(Debug, Clone, Copy)]
pub enum IncrResult {
    /// Successfully incremented/decremented, returns new value and CAS.
    Value(u64, u64),
    /// Key not found.
    NotFound,
    /// Value is not numeric.
    NotNumeric,
}

/// Command router.
pub struct CommandRouter {
    handlers: HashMap<&'static str, Box<dyn CommandHandler>>,
}

impl CommandRouter {
    /// Create a new command router with all handlers registered.
    pub fn new() -> Self {
        let mut router = Self {
            handlers: HashMap::new(),
        };

        // Storage commands
        router.register("set", Box::new(SetHandler));
        router.register("add", Box::new(AddHandler));
        router.register("replace", Box::new(ReplaceHandler));
        router.register("append", Box::new(AppendHandler));
        router.register("prepend", Box::new(PrependHandler));
        router.register("cas", Box::new(CasHandler));

        // Retrieval commands
        router.register("get", Box::new(GetHandler));
        router.register("gets", Box::new(GetsHandler));
        router.register("gat", Box::new(GatHandler));
        router.register("gats", Box::new(GatHandler)); // Same handler, includes CAS

        // Delete command
        router.register("delete", Box::new(DeleteHandler));

        // Arithmetic commands
        router.register("incr", Box::new(IncrHandler));
        router.register("decr", Box::new(DecrHandler));

        // Other commands
        router.register("touch", Box::new(TouchHandler));
        router.register("stats", Box::new(StatsHandler));
        router.register("flush_all", Box::new(FlushAllHandler));
        router.register("version", Box::new(VersionHandler));
        router.register("quit", Box::new(QuitHandler));
        router.register("noop", Box::new(NoopHandler));
        router.register("verbosity", Box::new(NoopHandler)); // No-op

        router
    }

    /// Register a command handler.
    pub fn register(&mut self, name: &'static str, handler: Box<dyn CommandHandler>) {
        self.handlers.insert(name, handler);
    }

    /// Execute a command.
    pub async fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> CommandResult {
        let handler = self.handlers.get(cmd.name.as_str());

        match handler {
            Some(h) => h.execute(ctx, cmd, state).await,
            None => Ok(MemcachedResponse::error()),
        }
    }
}

impl Default for CommandRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_context_prefixed_key() {
        let ctx = CommandContext::new("tenant1", 1);
        let key = ctx.prefixed_key(b"mykey");
        assert_eq!(&key[..], b"tenant1/mykey");
    }

    #[test]
    fn test_kv_value_creation() {
        let value = KvValue::new("hello", 42, 3600).with_cas(12345);
        assert_eq!(&value.data[..], b"hello");
        assert_eq!(value.flags, 42);
        assert_eq!(value.exptime, 3600);
        assert_eq!(value.cas, 12345);
    }

    #[test]
    fn test_router_has_all_handlers() {
        let router = CommandRouter::new();

        // Check all standard commands are registered
        let commands = [
            "set",
            "add",
            "replace",
            "append",
            "prepend",
            "cas",
            "get",
            "gets",
            "gat",
            "gats",
            "delete",
            "incr",
            "decr",
            "touch",
            "stats",
            "flush_all",
            "version",
            "quit",
            "noop",
        ];

        for cmd in commands {
            assert!(
                router.handlers.contains_key(cmd),
                "Missing handler for: {}",
                cmd
            );
        }
    }
}
