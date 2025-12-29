//! Redis command handlers.
//!
//! This module implements handlers for Redis commands, mapping them to
//! Lattice KPG state machine operations.
//!
//! # Command Classification
//!
//! Commands are classified by their linearizability requirements:
//!
//! | Category | Commands | Requires LIN-BOUND |
//! |----------|----------|-------------------|
//! | Snapshot reads | GET, MGET, EXISTS, KEYS | No |
//! | CAS operations | SET NX/XX, SETNX, INCR/DECR | Yes |
//! | Transactions | WATCH, EXEC | Yes |
//! | Writes | SET, DEL, MSET | No (follows Clustor durability) |

pub mod connection;
pub mod keys;
pub mod pubsub;
pub mod server;
pub mod strings;
pub mod transactions;

use crate::adapters::redis::{RedisCommand, RedisError, RedisValue};
use crate::adapters::RequestContext;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Result of command execution.
pub type CommandResult = Result<RedisValue, RedisError>;

/// Command execution context.
#[derive(Debug, Clone)]
pub struct CommandContext {
    /// Request context with tenant/auth info.
    pub request: RequestContext,

    /// Connection ID.
    pub connection_id: u64,

    /// Client name (from CLIENT SETNAME).
    pub client_name: Option<String>,

    /// Whether in transaction mode.
    pub in_transaction: bool,

    /// Whether in pub/sub mode.
    pub in_pubsub: bool,

    /// Protocol version (2 or 3).
    pub protocol_version: u8,
}

impl CommandContext {
    /// Create a new command context.
    pub fn new(request: RequestContext, connection_id: u64) -> Self {
        Self {
            request,
            connection_id,
            client_name: None,
            in_transaction: false,
            in_pubsub: false,
            protocol_version: 2,
        }
    }

    /// Set client name.
    pub fn with_client_name(mut self, name: String) -> Self {
        self.client_name = Some(name);
        self
    }

    /// Set protocol version.
    pub fn with_protocol_version(mut self, version: u8) -> Self {
        self.protocol_version = version;
        self
    }
}

/// Command handler trait.
///
/// Each command type implements this trait to handle execution.
pub trait CommandHandler: Send + Sync {
    /// Execute the command.
    ///
    /// Takes owned data to avoid lifetime issues with async execution.
    /// The context and command are cloned before being passed.
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>>;

    /// Get the command name.
    fn name(&self) -> &'static str;

    /// Get the minimum number of arguments required.
    fn min_args(&self) -> usize {
        0
    }

    /// Get the maximum number of arguments (None = unlimited).
    fn max_args(&self) -> Option<usize> {
        None
    }

    /// Whether this command requires authentication.
    fn requires_auth(&self) -> bool {
        true
    }

    /// Whether this command requires LIN-BOUND for consistency.
    fn requires_linearizable(&self) -> bool {
        false
    }

    /// Whether this command is a write operation.
    fn is_write(&self) -> bool {
        false
    }

    /// Whether this command is admin-only.
    fn is_admin(&self) -> bool {
        false
    }

    /// Whether this command can be used in a transaction.
    fn allowed_in_transaction(&self) -> bool {
        true
    }

    /// Whether this command can be used in pub/sub mode.
    fn allowed_in_pubsub(&self) -> bool {
        false
    }
}

/// Shared state for command execution.
///
/// Provides access to the KPG state machine and related services.
pub struct CommandState {
    /// Key-value storage interface.
    kv: Arc<dyn KvStore>,

    /// Pub/sub manager.
    pubsub: Arc<PubSubManager>,

    /// Server info.
    server_info: Arc<ServerInfo>,
}

impl CommandState {
    /// Create new command state.
    pub fn new(
        kv: Arc<dyn KvStore>,
        pubsub: Arc<PubSubManager>,
        server_info: Arc<ServerInfo>,
    ) -> Self {
        Self {
            kv,
            pubsub,
            server_info,
        }
    }

    /// Get the KV store.
    pub fn kv(&self) -> &Arc<dyn KvStore> {
        &self.kv
    }

    /// Get the pub/sub manager.
    pub fn pubsub(&self) -> &Arc<PubSubManager> {
        &self.pubsub
    }

    /// Get server info.
    pub fn server_info(&self) -> &Arc<ServerInfo> {
        &self.server_info
    }
}

/// Future type alias for KvStore operations returning multiple optional values.
pub type KvMgetResult<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Option<Bytes>>, RedisError>> + Send + 'a>>;

/// Key-value storage interface.
///
/// Abstracts the underlying KPG state machine for command handlers.
#[allow(clippy::type_complexity)]
pub trait KvStore: Send + Sync {
    /// Get a value by key.
    fn get(
        &self,
        tenant: &str,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Option<KvEntry>, RedisError>> + Send + '_>>;

    /// Put a value.
    fn put(
        &self,
        tenant: &str,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
    ) -> Pin<Box<dyn Future<Output = Result<PutResult, RedisError>> + Send + '_>>;

    /// Delete a key.
    fn delete(
        &self,
        tenant: &str,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, RedisError>> + Send + '_>>;

    /// Delete multiple keys.
    fn delete_many(
        &self,
        tenant: &str,
        keys: &[&[u8]],
    ) -> Pin<Box<dyn Future<Output = Result<u64, RedisError>> + Send + '_>>;

    /// Check if a key exists.
    fn exists(
        &self,
        tenant: &str,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, RedisError>> + Send + '_>>;

    /// Check if multiple keys exist.
    fn exists_many(
        &self,
        tenant: &str,
        keys: &[&[u8]],
    ) -> Pin<Box<dyn Future<Output = Result<u64, RedisError>> + Send + '_>>;

    /// Scan keys matching a pattern.
    fn scan(
        &self,
        tenant: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Pin<Box<dyn Future<Output = Result<ScanResult, RedisError>> + Send + '_>>;

    /// Get multiple values.
    fn mget(
        &self,
        tenant: &str,
        keys: &[&[u8]],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Option<Bytes>>, RedisError>> + Send + '_>>;

    /// Set multiple values atomically.
    fn mset(
        &self,
        tenant: &str,
        pairs: &[(&[u8], &[u8])],
    ) -> Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send + '_>>;

    /// Execute a transaction.
    fn transaction(
        &self,
        tenant: &str,
        tx: Transaction,
    ) -> Pin<Box<dyn Future<Output = Result<TransactionResult, RedisError>> + Send + '_>>;

    /// Get the current revision.
    fn revision(&self) -> u64;

    /// Get key count for tenant.
    fn key_count(
        &self,
        tenant: &str,
    ) -> Pin<Box<dyn Future<Output = Result<u64, RedisError>> + Send + '_>>;

    /// Get the current server tick (for TTL calculations).
    fn current_tick(&self) -> u64;
}

/// Key-value entry.
#[derive(Debug, Clone)]
pub struct KvEntry {
    /// The value.
    pub value: Bytes,

    /// Create revision.
    pub create_revision: u64,

    /// Modification revision.
    pub mod_revision: u64,

    /// Version (number of modifications).
    pub version: u64,

    /// Expiry tick (0 = no expiry).
    pub expiry_at: u64,
}

/// Put options.
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// Only set if key doesn't exist (NX).
    pub if_not_exists: bool,

    /// Only set if key exists (XX).
    pub if_exists: bool,

    /// Return previous value.
    pub get_previous: bool,

    /// Keep existing TTL.
    pub keep_ttl: bool,

    /// TTL in milliseconds (0 = no expiry).
    pub ttl_ms: u64,

    /// Compare create revision (0 = no check).
    pub compare_create_rev: u64,

    /// Compare mod revision (0 = no check).
    pub compare_mod_rev: u64,
}

/// Put result.
#[derive(Debug, Clone)]
pub struct PutResult {
    /// Whether the put succeeded.
    pub success: bool,

    /// Previous value (if requested and existed).
    pub previous: Option<KvEntry>,

    /// New revision.
    pub revision: u64,
}

/// Scan result.
#[derive(Debug, Clone)]
pub struct ScanResult {
    /// Next cursor (0 = iteration complete).
    pub cursor: u64,

    /// Keys found in this batch.
    pub keys: Vec<Bytes>,
}

/// Transaction for atomic operations.
#[derive(Debug, Clone, Default)]
pub struct Transaction {
    /// Compare predicates (all must pass for success branch).
    pub compares: Vec<Compare>,

    /// Operations to execute on success.
    pub success_ops: Vec<TxnOp>,

    /// Operations to execute on failure.
    pub failure_ops: Vec<TxnOp>,

    /// Watched key revisions (for WATCH semantics).
    pub watches: Vec<(Bytes, u64)>,
}

/// Compare predicate for transactions.
#[derive(Debug, Clone)]
pub struct Compare {
    /// Key to compare.
    pub key: Bytes,

    /// Comparison target.
    pub target: CompareTarget,

    /// Expected value.
    pub value: CompareValue,
}

/// What to compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareTarget {
    /// Compare create revision.
    CreateRevision,
    /// Compare mod revision.
    ModRevision,
    /// Compare version.
    Version,
    /// Compare value.
    Value,
}

/// Comparison value.
#[derive(Debug, Clone)]
pub enum CompareValue {
    /// Equal to.
    Equal(i64),
    /// Not equal to.
    NotEqual(i64),
    /// Greater than.
    Greater(i64),
    /// Less than.
    Less(i64),
    /// Value bytes (for value comparison).
    Bytes(Bytes),
}

/// Transaction operation.
#[derive(Debug, Clone)]
pub enum TxnOp {
    /// Get a key.
    Get(Bytes),
    /// Put a key-value pair.
    Put(Bytes, Bytes, PutOptions),
    /// Delete a key.
    Delete(Bytes),
    /// Delete a range of keys.
    DeleteRange(Bytes, Bytes),
}

/// Transaction result.
#[derive(Debug, Clone)]
pub struct TransactionResult {
    /// Whether the compare predicates succeeded.
    pub succeeded: bool,

    /// Results of executed operations.
    pub results: Vec<TxnOpResult>,

    /// Revision after transaction.
    pub revision: u64,
}

/// Result of a single transaction operation.
#[derive(Debug, Clone)]
pub enum TxnOpResult {
    /// Get result.
    Get(Option<KvEntry>),
    /// Put result.
    Put(PutResult),
    /// Delete result (number of keys deleted).
    Delete(u64),
}

/// Server information.
#[derive(Debug)]
pub struct ServerInfo {
    /// Server version string.
    pub version: String,

    /// Redis compatibility version.
    pub redis_version: String,

    /// Server mode (standalone, cluster).
    pub mode: String,

    /// OS info.
    pub os: String,

    /// Architecture.
    pub arch: String,

    /// Process ID.
    pub pid: u32,

    /// Uptime in seconds.
    pub uptime_seconds: std::sync::atomic::AtomicU64,

    /// Total connections received.
    pub total_connections: std::sync::atomic::AtomicU64,

    /// Total commands processed.
    pub total_commands: std::sync::atomic::AtomicU64,
}

impl ServerInfo {
    /// Create new server info.
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            redis_version: "7.0.0".to_string(), // Compatibility version
            mode: "standalone".to_string(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            pid: std::process::id(),
            uptime_seconds: std::sync::atomic::AtomicU64::new(0),
            total_connections: std::sync::atomic::AtomicU64::new(0),
            total_commands: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Increment uptime.
    pub fn tick_uptime(&self) {
        self.uptime_seconds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a new connection.
    pub fn connection_opened(&self) {
        self.total_connections
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a command.
    pub fn command_executed(&self) {
        self.total_commands
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Pub/Sub manager.
#[derive(Debug, Default)]
pub struct PubSubManager {
    /// Channel subscriptions: channel -> set of connection IDs.
    channels:
        parking_lot::RwLock<std::collections::HashMap<String, std::collections::HashSet<u64>>>,

    /// Pattern subscriptions: pattern -> set of connection IDs.
    patterns:
        parking_lot::RwLock<std::collections::HashMap<String, std::collections::HashSet<u64>>>,

    /// Connection subscriptions: connection ID -> channels.
    conn_channels:
        parking_lot::RwLock<std::collections::HashMap<u64, std::collections::HashSet<String>>>,

    /// Connection patterns: connection ID -> patterns.
    conn_patterns:
        parking_lot::RwLock<std::collections::HashMap<u64, std::collections::HashSet<String>>>,

    /// Message senders for each connection.
    senders: parking_lot::RwLock<
        std::collections::HashMap<u64, tokio::sync::mpsc::UnboundedSender<PubSubMessage>>,
    >,
}

impl PubSubManager {
    /// Create a new pub/sub manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe a connection to a channel.
    pub fn subscribe(&self, conn_id: u64, channel: &str) -> usize {
        let mut channels = self.channels.write();
        let mut conn_channels = self.conn_channels.write();

        channels
            .entry(channel.to_string())
            .or_default()
            .insert(conn_id);

        conn_channels
            .entry(conn_id)
            .or_default()
            .insert(channel.to_string());

        conn_channels.get(&conn_id).map(|c| c.len()).unwrap_or(0)
    }

    /// Subscribe a connection to a pattern.
    pub fn psubscribe(&self, conn_id: u64, pattern: &str) -> usize {
        let mut patterns = self.patterns.write();
        let mut conn_patterns = self.conn_patterns.write();

        patterns
            .entry(pattern.to_string())
            .or_default()
            .insert(conn_id);

        conn_patterns
            .entry(conn_id)
            .or_default()
            .insert(pattern.to_string());

        conn_patterns.get(&conn_id).map(|p| p.len()).unwrap_or(0)
    }

    /// Unsubscribe a connection from a channel.
    pub fn unsubscribe(&self, conn_id: u64, channel: &str) -> usize {
        let mut channels = self.channels.write();
        let mut conn_channels = self.conn_channels.write();

        if let Some(subs) = channels.get_mut(channel) {
            subs.remove(&conn_id);
            if subs.is_empty() {
                channels.remove(channel);
            }
        }

        if let Some(chans) = conn_channels.get_mut(&conn_id) {
            chans.remove(channel);
        }

        conn_channels.get(&conn_id).map(|c| c.len()).unwrap_or(0)
    }

    /// Unsubscribe a connection from all channels.
    pub fn unsubscribe_all(&self, conn_id: u64) -> Vec<String> {
        let mut channels = self.channels.write();
        let mut conn_channels = self.conn_channels.write();

        let removed = conn_channels.remove(&conn_id).unwrap_or_default();

        for channel in &removed {
            if let Some(subs) = channels.get_mut(channel) {
                subs.remove(&conn_id);
                if subs.is_empty() {
                    channels.remove(channel);
                }
            }
        }

        removed.into_iter().collect()
    }

    /// Unsubscribe a connection from a pattern.
    pub fn punsubscribe(&self, conn_id: u64, pattern: &str) -> usize {
        let mut patterns = self.patterns.write();
        let mut conn_patterns = self.conn_patterns.write();

        if let Some(subs) = patterns.get_mut(pattern) {
            subs.remove(&conn_id);
            if subs.is_empty() {
                patterns.remove(pattern);
            }
        }

        if let Some(pats) = conn_patterns.get_mut(&conn_id) {
            pats.remove(pattern);
        }

        conn_patterns.get(&conn_id).map(|p| p.len()).unwrap_or(0)
    }

    /// Unsubscribe a connection from all patterns.
    pub fn punsubscribe_all(&self, conn_id: u64) -> Vec<String> {
        let mut patterns = self.patterns.write();
        let mut conn_patterns = self.conn_patterns.write();

        let removed = conn_patterns.remove(&conn_id).unwrap_or_default();

        for pattern in &removed {
            if let Some(subs) = patterns.get_mut(pattern) {
                subs.remove(&conn_id);
                if subs.is_empty() {
                    patterns.remove(pattern);
                }
            }
        }

        removed.into_iter().collect()
    }

    /// Publish a message to a channel.
    pub fn publish(&self, channel: &str, message: &[u8]) -> usize {
        let channels = self.channels.read();
        let patterns = self.patterns.read();
        let senders = self.senders.read();

        let mut count = 0;

        // Direct channel subscribers
        if let Some(subscribers) = channels.get(channel) {
            for &conn_id in subscribers {
                if let Some(sender) = senders.get(&conn_id) {
                    let msg = PubSubMessage::Message {
                        channel: channel.to_string(),
                        message: Bytes::copy_from_slice(message),
                    };
                    if sender.send(msg).is_ok() {
                        count += 1;
                    }
                }
            }
        }

        // Pattern subscribers
        for (pattern, subscribers) in patterns.iter() {
            if pattern_matches(pattern, channel) {
                for &conn_id in subscribers {
                    if let Some(sender) = senders.get(&conn_id) {
                        let msg = PubSubMessage::PMessage {
                            pattern: pattern.clone(),
                            channel: channel.to_string(),
                            message: Bytes::copy_from_slice(message),
                        };
                        if sender.send(msg).is_ok() {
                            count += 1;
                        }
                    }
                }
            }
        }

        count
    }

    /// Register a message sender for a connection.
    pub fn register_sender(
        &self,
        conn_id: u64,
        sender: tokio::sync::mpsc::UnboundedSender<PubSubMessage>,
    ) {
        self.senders.write().insert(conn_id, sender);
    }

    /// Unregister a connection.
    pub fn unregister(&self, conn_id: u64) {
        self.unsubscribe_all(conn_id);
        self.punsubscribe_all(conn_id);
        self.senders.write().remove(&conn_id);
    }

    /// Get list of channels matching a pattern.
    pub fn channels(&self, pattern: Option<&str>) -> Vec<String> {
        let channels = self.channels.read();
        channels
            .keys()
            .filter(|c| pattern.map(|p| pattern_matches(p, c)).unwrap_or(true))
            .cloned()
            .collect()
    }

    /// Get subscriber count for channels.
    pub fn numsub(&self, channels_to_check: &[&str]) -> Vec<(String, usize)> {
        let channels = self.channels.read();
        channels_to_check
            .iter()
            .map(|c| {
                let count = channels.get(*c).map(|s| s.len()).unwrap_or(0);
                (c.to_string(), count)
            })
            .collect()
    }

    /// Get total pattern subscription count.
    pub fn numpat(&self) -> usize {
        self.patterns.read().values().map(|s| s.len()).sum()
    }

    /// Get subscription count for a connection.
    pub fn subscription_count(&self, conn_id: u64) -> (usize, usize) {
        let conn_channels = self.conn_channels.read();
        let conn_patterns = self.conn_patterns.read();

        let channels = conn_channels.get(&conn_id).map(|c| c.len()).unwrap_or(0);
        let patterns = conn_patterns.get(&conn_id).map(|p| p.len()).unwrap_or(0);

        (channels, patterns)
    }
}

/// Pub/Sub message.
#[derive(Debug, Clone)]
pub enum PubSubMessage {
    /// Message from direct channel subscription.
    Message { channel: String, message: Bytes },
    /// Message from pattern subscription.
    PMessage {
        pattern: String,
        channel: String,
        message: Bytes,
    },
}

/// Check if a pattern matches a string (glob-style).
pub fn pattern_matches(pattern: &str, s: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut s_chars = s.chars().peekable();

    while let Some(p) = pattern_chars.next() {
        match p {
            '*' => {
                // * matches any sequence of characters
                if pattern_chars.peek().is_none() {
                    return true; // Trailing * matches everything
                }
                // Try matching rest of pattern from each position
                let remaining_pattern: String = pattern_chars.collect();
                while s_chars.peek().is_some() {
                    let remaining_s: String = s_chars.clone().collect();
                    if pattern_matches(&remaining_pattern, &remaining_s) {
                        return true;
                    }
                    s_chars.next();
                }
                return pattern_matches(&remaining_pattern, "");
            }
            '?' => {
                // ? matches any single character
                if s_chars.next().is_none() {
                    return false;
                }
            }
            '[' => {
                // Character class
                let mut chars = Vec::new();
                let mut negated = false;

                if pattern_chars.peek() == Some(&'^') {
                    negated = true;
                    pattern_chars.next();
                }

                while let Some(c) = pattern_chars.next() {
                    if c == ']' {
                        break;
                    }
                    if c == '-' && !chars.is_empty() && pattern_chars.peek().is_some() {
                        // Range
                        let start = *chars.last().unwrap();
                        if let Some(end) = pattern_chars.next() {
                            if end != ']' {
                                for ch in start..=end {
                                    chars.push(ch);
                                }
                                continue;
                            }
                        }
                    }
                    chars.push(c);
                }

                let s_char = s_chars.next();
                if s_char.is_none() {
                    return false;
                }
                let s_char = s_char.unwrap();

                let matches = chars.contains(&s_char);
                if negated == matches {
                    return false;
                }
            }
            '\\' => {
                // Escape next character
                let escaped = pattern_chars.next();
                if escaped != s_chars.next() {
                    return false;
                }
            }
            c => {
                // Literal match
                if s_chars.next() != Some(c) {
                    return false;
                }
            }
        }
    }

    // Pattern consumed, string should also be consumed
    s_chars.next().is_none()
}

/// Command router for dispatching commands to handlers.
pub struct CommandRouter {
    /// Registered handlers.
    handlers: std::collections::HashMap<String, Arc<dyn CommandHandler>>,
}

impl CommandRouter {
    /// Create a new command router with all handlers registered.
    pub fn new() -> Self {
        let mut router = Self {
            handlers: std::collections::HashMap::new(),
        };

        // Register all handlers
        router.register_all();

        router
    }

    /// Register all command handlers.
    fn register_all(&mut self) {
        // String commands
        self.register(Arc::new(strings::GetHandler));
        self.register(Arc::new(strings::SetHandler));
        self.register(Arc::new(strings::SetNxHandler));
        self.register(Arc::new(strings::SetExHandler));
        self.register(Arc::new(strings::PSetExHandler));
        self.register(Arc::new(strings::MGetHandler));
        self.register(Arc::new(strings::MSetHandler));
        self.register(Arc::new(strings::GetExHandler));
        self.register(Arc::new(strings::GetDelHandler));
        self.register(Arc::new(strings::AppendHandler));
        self.register(Arc::new(strings::StrLenHandler));
        self.register(Arc::new(strings::IncrHandler));
        self.register(Arc::new(strings::IncrByHandler));
        self.register(Arc::new(strings::DecrHandler));
        self.register(Arc::new(strings::DecrByHandler));
        self.register(Arc::new(strings::IncrByFloatHandler));
        self.register(Arc::new(strings::GetRangeHandler));
        self.register(Arc::new(strings::SetRangeHandler));
        self.register(Arc::new(strings::GetSetHandler));

        // Key commands
        self.register(Arc::new(keys::DelHandler));
        self.register(Arc::new(keys::UnlinkHandler));
        self.register(Arc::new(keys::ExistsHandler));
        self.register(Arc::new(keys::TypeHandler));
        self.register(Arc::new(keys::KeysHandler));
        self.register(Arc::new(keys::ScanHandler));
        self.register(Arc::new(keys::ExpireHandler));
        self.register(Arc::new(keys::PExpireHandler));
        self.register(Arc::new(keys::ExpireAtHandler));
        self.register(Arc::new(keys::PExpireAtHandler));
        self.register(Arc::new(keys::TtlHandler));
        self.register(Arc::new(keys::PTtlHandler));
        self.register(Arc::new(keys::PersistHandler));
        self.register(Arc::new(keys::ExpireTimeHandler));
        self.register(Arc::new(keys::PExpireTimeHandler));
        self.register(Arc::new(keys::RenameHandler));
        self.register(Arc::new(keys::RenameNxHandler));
        self.register(Arc::new(keys::CopyHandler));
        self.register(Arc::new(keys::TouchHandler));
        self.register(Arc::new(keys::ObjectHandler));
        self.register(Arc::new(keys::DumpHandler));
        self.register(Arc::new(keys::RestoreHandler));

        // Server commands
        self.register(Arc::new(server::PingHandler));
        self.register(Arc::new(server::EchoHandler));
        self.register(Arc::new(server::QuitHandler));
        self.register(Arc::new(server::SelectHandler));
        self.register(Arc::new(server::DbSizeHandler));
        self.register(Arc::new(server::InfoHandler));
        self.register(Arc::new(server::TimeHandler));
        self.register(Arc::new(server::DebugHandler));
        self.register(Arc::new(server::CommandCmdHandler));
        self.register(Arc::new(server::ConfigHandler));
        self.register(Arc::new(server::MemoryHandler));
        self.register(Arc::new(server::LatencyHandler));
        self.register(Arc::new(server::SlowLogHandler));
        self.register(Arc::new(server::FlushDbHandler));
        self.register(Arc::new(server::FlushAllHandler));

        // Connection commands
        self.register(Arc::new(connection::AuthHandler));
        self.register(Arc::new(connection::ClientHandler));
        self.register(Arc::new(connection::HelloHandler));
        self.register(Arc::new(connection::ResetHandler));

        // Transaction commands
        self.register(Arc::new(transactions::MultiHandler));
        self.register(Arc::new(transactions::ExecHandler));
        self.register(Arc::new(transactions::DiscardHandler));
        self.register(Arc::new(transactions::WatchHandler));
        self.register(Arc::new(transactions::UnwatchHandler));

        // Pub/Sub commands
        self.register(Arc::new(pubsub::SubscribeHandler));
        self.register(Arc::new(pubsub::PSubscribeHandler));
        self.register(Arc::new(pubsub::UnsubscribeHandler));
        self.register(Arc::new(pubsub::PUnsubscribeHandler));
        self.register(Arc::new(pubsub::PublishHandler));
        self.register(Arc::new(pubsub::PubSubHandler));

        // Cluster commands (minimal compatibility)
        self.register(Arc::new(server::ClusterHandler));
        self.register(Arc::new(server::ReadOnlyHandler));
        self.register(Arc::new(server::ReadWriteHandler));
    }

    /// Register a command handler.
    fn register(&mut self, handler: Arc<dyn CommandHandler>) {
        self.handlers.insert(handler.name().to_uppercase(), handler);
    }

    /// Get a handler for a command.
    pub fn get(&self, command: &str) -> Option<&Arc<dyn CommandHandler>> {
        self.handlers.get(&command.to_uppercase())
    }

    /// Execute a command.
    pub async fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> CommandResult {
        // Look up handler
        let handler = match self.get(&cmd.name) {
            Some(h) => h,
            None => {
                return Err(RedisError::new(
                    "ERR",
                    format!("unknown command '{}', with args beginning with: ", cmd.name),
                ));
            }
        };

        // Validate argument count
        if cmd.argc() < handler.min_args() {
            return Err(RedisError::wrong_arity(&cmd.name));
        }
        if let Some(max) = handler.max_args() {
            if cmd.argc() > max {
                return Err(RedisError::wrong_arity(&cmd.name));
            }
        }

        // Check transaction mode
        if ctx.in_transaction && !handler.allowed_in_transaction() {
            return Err(RedisError::new(
                "ERR",
                format!("'{}' is not allowed inside MULTI block", cmd.name),
            ));
        }

        // Check pub/sub mode
        if ctx.in_pubsub && !handler.allowed_in_pubsub() {
            return Err(RedisError::new(
                "ERR",
                "only SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT, RESET are allowed in this context",
            ));
        }

        // Execute command
        handler.execute(ctx, cmd, state).await
    }

    /// Get all registered command names.
    pub fn commands(&self) -> Vec<&str> {
        self.handlers.keys().map(|s| s.as_str()).collect()
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
    fn test_pattern_matches_literal() {
        assert!(pattern_matches("hello", "hello"));
        assert!(!pattern_matches("hello", "world"));
        assert!(!pattern_matches("hello", "hell"));
        assert!(!pattern_matches("hello", "helloo"));
    }

    #[test]
    fn test_pattern_matches_wildcard() {
        assert!(pattern_matches("*", "anything"));
        assert!(pattern_matches("*", ""));
        assert!(pattern_matches("hello*", "hello"));
        assert!(pattern_matches("hello*", "helloworld"));
        assert!(pattern_matches("*world", "helloworld"));
        assert!(pattern_matches("*world", "world"));
        assert!(pattern_matches("h*d", "helloworld"));
        assert!(!pattern_matches("h*d", "helloworlds"));
    }

    #[test]
    fn test_pattern_matches_question() {
        assert!(pattern_matches("h?llo", "hello"));
        assert!(pattern_matches("h?llo", "hallo"));
        assert!(!pattern_matches("h?llo", "hllo"));
        assert!(!pattern_matches("h?llo", "heello"));
    }

    #[test]
    fn test_pattern_matches_brackets() {
        assert!(pattern_matches("h[ae]llo", "hello"));
        assert!(pattern_matches("h[ae]llo", "hallo"));
        assert!(!pattern_matches("h[ae]llo", "hillo"));
        assert!(pattern_matches("h[a-z]llo", "hello"));
        assert!(pattern_matches("h[^a]llo", "hello"));
        assert!(!pattern_matches("h[^a]llo", "hallo"));
    }

    #[test]
    fn test_pubsub_subscribe() {
        let manager = PubSubManager::new();

        assert_eq!(manager.subscribe(1, "channel1"), 1);
        assert_eq!(manager.subscribe(1, "channel2"), 2);
        assert_eq!(manager.subscribe(2, "channel1"), 1);

        assert_eq!(manager.subscription_count(1), (2, 0));
        assert_eq!(manager.subscription_count(2), (1, 0));
    }

    #[test]
    fn test_pubsub_unsubscribe() {
        let manager = PubSubManager::new();

        manager.subscribe(1, "channel1");
        manager.subscribe(1, "channel2");

        assert_eq!(manager.unsubscribe(1, "channel1"), 1);
        assert_eq!(manager.subscription_count(1), (1, 0));

        let removed = manager.unsubscribe_all(1);
        assert_eq!(removed.len(), 1);
        assert_eq!(manager.subscription_count(1), (0, 0));
    }

    #[test]
    fn test_pubsub_channels() {
        let manager = PubSubManager::new();

        manager.subscribe(1, "news.sports");
        manager.subscribe(1, "news.weather");
        manager.subscribe(2, "updates");

        let channels = manager.channels(None);
        assert_eq!(channels.len(), 3);

        let news_channels = manager.channels(Some("news.*"));
        assert_eq!(news_channels.len(), 2);
    }

    #[test]
    fn test_server_info() {
        let info = ServerInfo::new();
        assert!(!info.version.is_empty());
        assert_eq!(info.redis_version, "7.0.0");

        info.tick_uptime();
        assert_eq!(
            info.uptime_seconds
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        info.connection_opened();
        assert_eq!(
            info.total_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        info.command_executed();
        assert_eq!(
            info.total_commands
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
}
