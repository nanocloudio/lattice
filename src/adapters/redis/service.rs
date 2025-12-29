//! Redis adapter service.
//!
//! Implements the main Redis adapter that handles client connections
//! and dispatches commands to the appropriate handlers.

use super::commands::{
    CommandContext, CommandRouter, CommandState, KvEntry, KvStore, PubSubManager, PutOptions,
    PutResult, ScanResult, ServerInfo, Transaction, TransactionResult,
};
use super::protocol::codec::RespCodec;
use super::{ProtocolVersion, RedisCommand, RedisError, RedisValue};
use crate::adapters::{Adapter, AdapterConfig, AdapterHealth, AdapterState, RequestContext};
use crate::core::error::LatticeResult;
use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

/// Redis adapter configuration.
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Base adapter configuration.
    pub base: AdapterConfig,

    /// Maximum pipeline depth.
    pub max_pipeline_depth: usize,

    /// Command timeout in milliseconds.
    pub command_timeout_ms: u64,

    /// Whether to require authentication.
    pub require_auth: bool,

    /// Default tenant ID for unauthenticated connections.
    pub default_tenant: String,

    /// Protocol version preference.
    pub protocol_version: ProtocolVersion,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            base: AdapterConfig {
                bind_addr: "127.0.0.1:6379".parse().unwrap(),
                ..Default::default()
            },
            max_pipeline_depth: 64,
            command_timeout_ms: 30_000,
            require_auth: false,
            default_tenant: "default".to_string(),
            protocol_version: ProtocolVersion::Resp2,
        }
    }
}

/// Redis adapter.
pub struct RedisAdapter {
    /// Configuration.
    config: RedisConfig,

    /// Current state.
    state: RwLock<AdapterState>,

    /// Command router.
    router: CommandRouter,

    /// Shared command state.
    command_state: Arc<CommandState>,

    /// Active connection count.
    active_connections: AtomicU64,

    /// Total requests processed.
    total_requests: AtomicU64,

    /// Failed requests.
    failed_requests: AtomicU64,

    /// Shutdown signal sender.
    shutdown_tx: watch::Sender<bool>,

    /// Shutdown signal receiver.
    shutdown_rx: watch::Receiver<bool>,
}

impl RedisAdapter {
    /// Create a new Redis adapter.
    pub fn new(config: RedisConfig, kv_store: Arc<dyn KvStore>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let command_state = Arc::new(CommandState::new(
            kv_store,
            Arc::new(PubSubManager::new()),
            Arc::new(ServerInfo::new()),
        ));

        Self {
            config,
            state: RwLock::new(AdapterState::Created),
            router: CommandRouter::new(),
            command_state,
            active_connections: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Handle a single connection.
    ///
    /// This method is public to allow external code to manage connection acceptance.
    /// It processes Redis commands from the stream until the connection is closed
    /// or a QUIT command is received.
    pub async fn handle_connection(
        self: Arc<Self>,
        mut stream: tokio::net::TcpStream,
        _remote_addr: SocketAddr,
        connection_id: u64,
    ) {
        let mut codec = RespCodec::new();
        let mut buffer = BytesMut::with_capacity(4096);
        let mut conn_state = super::commands::connection::ConnectionState::new(connection_id);

        // Set up connection context
        let tenant_id = self.config.default_tenant.clone();
        conn_state.tenant_id = Some(tenant_id);

        // Register for pub/sub
        let (pubsub_tx, mut pubsub_rx) = tokio::sync::mpsc::unbounded_channel();
        self.command_state
            .pubsub()
            .register_sender(connection_id, pubsub_tx);

        self.command_state.server_info().connection_opened();

        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                // Read from socket
                result = stream.read_buf(&mut buffer) => {
                    match result {
                        Ok(0) => {
                            // Connection closed
                            break;
                        }
                        Ok(_) => {
                            // Process commands in buffer
                            while !buffer.is_empty() {
                                match self.decode_command(&mut codec, &mut buffer) {
                                    Ok(Some(cmd)) => {
                                        // Handle command
                                        let response = self.process_command(
                                            &cmd,
                                            &mut codec,
                                            &mut conn_state,
                                        ).await;

                                        // Send response
                                        if conn_state.should_reply() {
                                            let encoded = codec.encode(&response);
                                            if stream.write_all(&encoded).await.is_err() {
                                                break;
                                            }
                                        }

                                        // Check for QUIT
                                        if cmd.name.eq_ignore_ascii_case("QUIT") {
                                            break;
                                        }
                                    }
                                    Ok(None) => {
                                        // Need more data
                                        break;
                                    }
                                    Err(e) => {
                                        // Protocol error
                                        let error = RedisValue::err(format!("protocol error: {}", e));
                                        let encoded = codec.encode(&error);
                                        let _ = stream.write_all(&encoded).await;
                                        buffer.clear();
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            // Read error
                            break;
                        }
                    }
                }

                // Handle pub/sub messages
                Some(msg) = pubsub_rx.recv() => {
                    let response = match msg {
                        super::commands::PubSubMessage::Message { channel, message } => {
                            RedisValue::Array(vec![
                                RedisValue::BulkString(Bytes::from("message")),
                                RedisValue::BulkString(Bytes::from(channel)),
                                RedisValue::BulkString(message),
                            ])
                        }
                        super::commands::PubSubMessage::PMessage { pattern, channel, message } => {
                            RedisValue::Array(vec![
                                RedisValue::BulkString(Bytes::from("pmessage")),
                                RedisValue::BulkString(Bytes::from(pattern)),
                                RedisValue::BulkString(Bytes::from(channel)),
                                RedisValue::BulkString(message),
                            ])
                        }
                    };

                    let encoded = codec.encode(&response);
                    if stream.write_all(&encoded).await.is_err() {
                        break;
                    }
                }

                // Handle shutdown
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }

        // Cleanup
        self.command_state.pubsub().unregister(connection_id);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Decode a command from the buffer.
    fn decode_command(
        &self,
        codec: &mut RespCodec,
        buffer: &mut BytesMut,
    ) -> Result<Option<RedisCommand>, String> {
        use super::protocol::ParseResult;

        if buffer.is_empty() {
            return Ok(None);
        }

        match codec.parse(buffer) {
            ParseResult::Ok(value) => {
                // Calculate consumed bytes
                let encoded = codec.encode(&value);
                let consumed = encoded.len();
                if consumed <= buffer.len() {
                    let _ = buffer.split_to(consumed);
                }

                match value {
                    RedisValue::Array(arr) => match RedisCommand::from_array(arr) {
                        Ok(cmd) => Ok(Some(cmd)),
                        Err(e) => Err(e.to_string()),
                    },
                    _ => Err("expected array".to_string()),
                }
            }
            ParseResult::Incomplete => Ok(None),
            ParseResult::Error(e) => Err(e),
        }
    }

    /// Process a single command.
    async fn process_command(
        &self,
        cmd: &RedisCommand,
        codec: &mut RespCodec,
        conn_state: &mut super::commands::connection::ConnectionState,
    ) -> RedisValue {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.command_state.server_info().command_executed();

        let cmd_upper = cmd.name.to_uppercase();

        // Handle special commands that affect connection state
        match cmd_upper.as_str() {
            "HELLO" => {
                // Protocol negotiation
                if let Some(ver) = cmd.arg_int(0) {
                    if ver == 3 {
                        codec.upgrade_to_resp3();
                        conn_state.protocol_version = ProtocolVersion::Resp3;
                    }
                }
            }
            "MULTI" => {
                if conn_state.in_transaction {
                    return RedisValue::err("MULTI calls can not be nested");
                }
                conn_state.start_transaction();
                return RedisValue::ok();
            }
            "EXEC" => {
                if !conn_state.in_transaction {
                    return RedisValue::err("EXEC without MULTI");
                }
                return self.execute_transaction(conn_state).await;
            }
            "DISCARD" => {
                if !conn_state.in_transaction {
                    return RedisValue::err("DISCARD without MULTI");
                }
                conn_state.discard_transaction();
                return RedisValue::ok();
            }
            "WATCH" => {
                if conn_state.in_transaction {
                    return RedisValue::err("WATCH inside MULTI is not allowed");
                }
                return self.handle_watch(cmd, conn_state).await;
            }
            "UNWATCH" => {
                conn_state.unwatch_all();
                return RedisValue::ok();
            }
            "RESET" => {
                conn_state.reset();
                codec.set_version(ProtocolVersion::Resp2);
                return RedisValue::SimpleString("RESET".to_string());
            }
            "CLIENT" => {
                if let Some(subcmd) = cmd.arg_str(0) {
                    if subcmd.eq_ignore_ascii_case("SETNAME") {
                        if let Some(name) = cmd.arg_str(1) {
                            conn_state.name = Some(name.to_string());
                        }
                    }
                }
            }
            "AUTH" => {
                // Handle authentication
                if cmd.argc() >= 1 {
                    // In a real implementation, validate credentials
                    // For now, accept any auth
                    conn_state.authenticated = true;
                    if cmd.argc() == 2 {
                        // ACL style: username is tenant
                        if let Some(user) = cmd.arg_str(0) {
                            conn_state.tenant_id = Some(user.to_string());
                        }
                    }
                    return RedisValue::ok();
                }
            }
            "READONLY" => {
                conn_state.readonly = true;
                return RedisValue::ok();
            }
            "READWRITE" => {
                conn_state.readonly = false;
                return RedisValue::ok();
            }
            _ => {}
        }

        // If in transaction mode, queue command instead of executing
        if conn_state.in_transaction {
            // Check for commands that can't be queued
            let blocked = matches!(
                cmd_upper.as_str(),
                "MULTI" | "WATCH" | "SUBSCRIBE" | "PSUBSCRIBE" | "UNSUBSCRIBE" | "PUNSUBSCRIBE"
            );

            if blocked {
                return RedisValue::err(format!(
                    "'{}' is not allowed inside MULTI block",
                    cmd.name
                ));
            }

            conn_state.queue_command(cmd.clone());
            return RedisValue::SimpleString("QUEUED".to_string());
        }

        // Build command context
        let tenant = conn_state
            .tenant_id
            .clone()
            .unwrap_or_else(|| self.config.default_tenant.clone());

        let ctx = CommandContext {
            request: RequestContext::new(&tenant, 0),
            connection_id: conn_state.id,
            client_name: conn_state.name.clone(),
            in_transaction: false,
            in_pubsub: conn_state.in_pubsub,
            protocol_version: match conn_state.protocol_version {
                ProtocolVersion::Resp2 => 2,
                ProtocolVersion::Resp3 => 3,
            },
        };

        // Execute command
        match self
            .router
            .execute(ctx, cmd.clone(), Arc::clone(&self.command_state))
            .await
        {
            Ok(result) => result,
            Err(e) => {
                self.failed_requests.fetch_add(1, Ordering::Relaxed);
                RedisValue::Error(e)
            }
        }
    }

    /// Execute a transaction.
    async fn execute_transaction(
        &self,
        conn_state: &mut super::commands::connection::ConnectionState,
    ) -> RedisValue {
        let commands = std::mem::take(&mut conn_state.transaction_queue);
        let watched = std::mem::take(&mut conn_state.watched_keys);
        conn_state.in_transaction = false;

        let tenant = conn_state
            .tenant_id
            .clone()
            .unwrap_or_else(|| self.config.default_tenant.clone());

        // Check watched keys
        for (key, expected_rev) in &watched {
            match self.command_state.kv().get(&tenant, key).await {
                Ok(Some(entry)) if entry.mod_revision != *expected_rev => {
                    // Watch conflict
                    return RedisValue::Null;
                }
                Ok(None) if *expected_rev != 0 => {
                    // Key was deleted
                    return RedisValue::Null;
                }
                Err(_) => {
                    return RedisValue::Null;
                }
                _ => {}
            }
        }

        // Execute commands
        let mut results = Vec::with_capacity(commands.len());

        for cmd in commands {
            let ctx = CommandContext {
                request: RequestContext::new(&tenant, 0),
                connection_id: conn_state.id,
                client_name: conn_state.name.clone(),
                in_transaction: true,
                in_pubsub: false,
                protocol_version: match conn_state.protocol_version {
                    ProtocolVersion::Resp2 => 2,
                    ProtocolVersion::Resp3 => 3,
                },
            };

            let result = match self
                .router
                .execute(ctx, cmd, Arc::clone(&self.command_state))
                .await
            {
                Ok(v) => v,
                Err(e) => RedisValue::Error(e),
            };
            results.push(result);
        }

        RedisValue::Array(results)
    }

    /// Handle WATCH command.
    async fn handle_watch(
        &self,
        cmd: &RedisCommand,
        conn_state: &mut super::commands::connection::ConnectionState,
    ) -> RedisValue {
        let tenant = conn_state
            .tenant_id
            .clone()
            .unwrap_or_else(|| self.config.default_tenant.clone());

        for arg in &cmd.args {
            match self.command_state.kv().get(&tenant, arg).await {
                Ok(Some(entry)) => {
                    conn_state.watch_key(arg.clone(), entry.mod_revision);
                }
                Ok(None) => {
                    conn_state.watch_key(arg.clone(), 0);
                }
                Err(_) => {
                    return RedisValue::err("WATCH failed");
                }
            }
        }

        RedisValue::ok()
    }
}

impl Adapter for RedisAdapter {
    fn name(&self) -> &'static str {
        "redis"
    }

    fn state(&self) -> AdapterState {
        *self.state.read()
    }

    fn health(&self) -> AdapterHealth {
        AdapterHealth {
            state: self.state(),
            active_connections: self.active_connections.load(Ordering::Relaxed) as usize,
            total_requests: self.total_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            last_error: None,
        }
    }

    fn start(&self) -> Pin<Box<dyn Future<Output = LatticeResult<()>> + Send + '_>> {
        Box::pin(async move {
            *self.state.write() = AdapterState::Starting;

            // Note: Connection acceptance should be managed externally.
            // Use handle_connection() to process each accepted connection.

            *self.state.write() = AdapterState::Running;

            Ok(())
        })
    }

    fn stop(&self) -> Pin<Box<dyn Future<Output = LatticeResult<()>> + Send + '_>> {
        Box::pin(async move {
            *self.state.write() = AdapterState::ShuttingDown;

            // Signal shutdown
            let _ = self.shutdown_tx.send(true);

            *self.state.write() = AdapterState::Stopped;

            Ok(())
        })
    }

    fn bind_addr(&self) -> SocketAddr {
        self.config.base.bind_addr
    }
}

/// Mock KV store for testing.
#[derive(Default)]
pub struct MockKvStore {
    data: parking_lot::RwLock<std::collections::HashMap<String, KvEntry>>,
    revision: AtomicU64,
}

impl MockKvStore {
    /// Create a new mock KV store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Make a key with tenant prefix.
    fn make_key(tenant: &str, key: &[u8]) -> String {
        format!("{}:{}", tenant, String::from_utf8_lossy(key))
    }
}

impl KvStore for MockKvStore {
    fn get(
        &self,
        tenant: &str,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Option<KvEntry>, RedisError>> + Send + '_>> {
        let full_key = Self::make_key(tenant, key);
        let data = self.data.read();
        let result = data.get(&full_key).cloned();
        Box::pin(async move { Ok(result) })
    }

    fn put(
        &self,
        tenant: &str,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
    ) -> Pin<Box<dyn Future<Output = Result<PutResult, RedisError>> + Send + '_>> {
        let full_key = Self::make_key(tenant, key);
        let value = Bytes::copy_from_slice(value);
        let revision = self.revision.fetch_add(1, Ordering::Relaxed) + 1;

        let mut data = self.data.write();
        let existing = data.get(&full_key).cloned();

        // Check NX/XX conditions
        if options.if_not_exists && existing.is_some() {
            return Box::pin(async move {
                Ok(PutResult {
                    success: false,
                    previous: existing,
                    revision,
                })
            });
        }
        if options.if_exists && existing.is_none() {
            return Box::pin(async move {
                Ok(PutResult {
                    success: false,
                    previous: None,
                    revision,
                })
            });
        }

        let entry = KvEntry {
            value,
            create_revision: existing
                .as_ref()
                .map(|e| e.create_revision)
                .unwrap_or(revision),
            mod_revision: revision,
            version: existing.as_ref().map(|e| e.version + 1).unwrap_or(1),
            expiry_at: options.ttl_ms,
        };

        data.insert(full_key, entry);

        Box::pin(async move {
            Ok(PutResult {
                success: true,
                previous: existing,
                revision,
            })
        })
    }

    fn delete(
        &self,
        tenant: &str,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, RedisError>> + Send + '_>> {
        let full_key = Self::make_key(tenant, key);
        let mut data = self.data.write();
        let existed = data.remove(&full_key).is_some();
        Box::pin(async move { Ok(existed) })
    }

    fn delete_many(
        &self,
        tenant: &str,
        keys: &[&[u8]],
    ) -> Pin<Box<dyn Future<Output = Result<u64, RedisError>> + Send + '_>> {
        let mut data = self.data.write();
        let mut count = 0u64;
        for key in keys {
            let full_key = Self::make_key(tenant, key);
            if data.remove(&full_key).is_some() {
                count += 1;
            }
        }
        Box::pin(async move { Ok(count) })
    }

    fn exists(
        &self,
        tenant: &str,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, RedisError>> + Send + '_>> {
        let full_key = Self::make_key(tenant, key);
        let data = self.data.read();
        let exists = data.contains_key(&full_key);
        Box::pin(async move { Ok(exists) })
    }

    fn exists_many(
        &self,
        tenant: &str,
        keys: &[&[u8]],
    ) -> Pin<Box<dyn Future<Output = Result<u64, RedisError>> + Send + '_>> {
        let data = self.data.read();
        let mut count = 0u64;
        for key in keys {
            let full_key = Self::make_key(tenant, key);
            if data.contains_key(&full_key) {
                count += 1;
            }
        }
        Box::pin(async move { Ok(count) })
    }

    fn scan(
        &self,
        tenant: &str,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> Pin<Box<dyn Future<Output = Result<ScanResult, RedisError>> + Send + '_>> {
        let prefix = format!("{}:", tenant);
        let pattern = pattern.map(|p| p.to_string());
        let data = self.data.read();

        let keys: Vec<Bytes> = data
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .map(|k| k.strip_prefix(&prefix).unwrap_or(k))
            .filter(|k| {
                pattern
                    .as_ref()
                    .map(|p| super::commands::pattern_matches(p, k))
                    .unwrap_or(true)
            })
            .skip(cursor as usize)
            .take(count)
            .map(|k| Bytes::from(k.to_string()))
            .collect();

        let next_cursor = if keys.len() == count {
            cursor + count as u64
        } else {
            0
        };

        Box::pin(async move {
            Ok(ScanResult {
                cursor: next_cursor,
                keys,
            })
        })
    }

    fn mget(
        &self,
        tenant: &str,
        keys: &[&[u8]],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Option<Bytes>>, RedisError>> + Send + '_>> {
        let data = self.data.read();
        let results: Vec<Option<Bytes>> = keys
            .iter()
            .map(|key| {
                let full_key = Self::make_key(tenant, key);
                data.get(&full_key).map(|e| e.value.clone())
            })
            .collect();
        Box::pin(async move { Ok(results) })
    }

    fn mset(
        &self,
        tenant: &str,
        pairs: &[(&[u8], &[u8])],
    ) -> Pin<Box<dyn Future<Output = Result<(), RedisError>> + Send + '_>> {
        let revision = self.revision.fetch_add(1, Ordering::Relaxed) + 1;
        let mut data = self.data.write();

        for (key, value) in pairs {
            let full_key = Self::make_key(tenant, key);
            let existing = data.get(&full_key);

            let entry = KvEntry {
                value: Bytes::copy_from_slice(value),
                create_revision: existing.map(|e| e.create_revision).unwrap_or(revision),
                mod_revision: revision,
                version: existing.map(|e| e.version + 1).unwrap_or(1),
                expiry_at: 0,
            };

            data.insert(full_key, entry);
        }

        Box::pin(async move { Ok(()) })
    }

    fn transaction(
        &self,
        _tenant: &str,
        _tx: Transaction,
    ) -> Pin<Box<dyn Future<Output = Result<TransactionResult, RedisError>> + Send + '_>> {
        Box::pin(async move {
            Ok(TransactionResult {
                succeeded: true,
                results: Vec::new(),
                revision: 0,
            })
        })
    }

    fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }

    fn key_count(
        &self,
        tenant: &str,
    ) -> Pin<Box<dyn Future<Output = Result<u64, RedisError>> + Send + '_>> {
        let prefix = format!("{}:", tenant);
        let data = self.data.read();
        let count = data.keys().filter(|k| k.starts_with(&prefix)).count() as u64;
        Box::pin(async move { Ok(count) })
    }

    fn current_tick(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_config_default() {
        let config = RedisConfig::default();
        assert_eq!(config.max_pipeline_depth, 64);
        assert_eq!(config.command_timeout_ms, 30_000);
        assert!(!config.require_auth);
    }

    #[tokio::test]
    async fn test_mock_kv_store_get_set() {
        let store = MockKvStore::new();

        // Set a key
        let result = store
            .put("tenant1", b"key1", b"value1", PutOptions::default())
            .await
            .unwrap();
        assert!(result.success);

        // Get it back
        let entry = store.get("tenant1", b"key1").await.unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().value.as_ref(), b"value1");

        // Non-existent key
        let entry = store.get("tenant1", b"nonexistent").await.unwrap();
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn test_mock_kv_store_nx_xx() {
        let store = MockKvStore::new();

        // NX succeeds on new key
        let options = PutOptions {
            if_not_exists: true,
            ..Default::default()
        };
        let result = store.put("tenant", b"key", b"v1", options).await.unwrap();
        assert!(result.success);

        // NX fails on existing key
        let options = PutOptions {
            if_not_exists: true,
            ..Default::default()
        };
        let result = store.put("tenant", b"key", b"v2", options).await.unwrap();
        assert!(!result.success);

        // XX succeeds on existing key
        let options = PutOptions {
            if_exists: true,
            ..Default::default()
        };
        let result = store.put("tenant", b"key", b"v3", options).await.unwrap();
        assert!(result.success);

        // XX fails on new key
        let options = PutOptions {
            if_exists: true,
            ..Default::default()
        };
        let result = store.put("tenant", b"newkey", b"v", options).await.unwrap();
        assert!(!result.success);
    }

    #[tokio::test]
    async fn test_mock_kv_store_delete() {
        let store = MockKvStore::new();

        store
            .put("tenant", b"key", b"value", PutOptions::default())
            .await
            .unwrap();

        let deleted = store.delete("tenant", b"key").await.unwrap();
        assert!(deleted);

        let deleted = store.delete("tenant", b"key").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_mock_kv_store_mget_mset() {
        let store = MockKvStore::new();

        // MSET
        store
            .mset(
                "tenant",
                &[(b"k1".as_ref(), b"v1".as_ref()), (b"k2", b"v2")],
            )
            .await
            .unwrap();

        // MGET
        let values = store
            .mget("tenant", &[b"k1".as_ref(), b"k2", b"k3"])
            .await
            .unwrap();

        assert_eq!(values.len(), 3);
        assert_eq!(values[0].as_ref().map(|b| b.as_ref()), Some(b"v1".as_ref()));
        assert_eq!(values[1].as_ref().map(|b| b.as_ref()), Some(b"v2".as_ref()));
        assert!(values[2].is_none());
    }

    #[tokio::test]
    async fn test_mock_kv_store_tenant_isolation() {
        let store = MockKvStore::new();

        store
            .put("tenant1", b"key", b"value1", PutOptions::default())
            .await
            .unwrap();
        store
            .put("tenant2", b"key", b"value2", PutOptions::default())
            .await
            .unwrap();

        let v1 = store.get("tenant1", b"key").await.unwrap().unwrap();
        let v2 = store.get("tenant2", b"key").await.unwrap().unwrap();

        assert_eq!(v1.value.as_ref(), b"value1");
        assert_eq!(v2.value.as_ref(), b"value2");
    }

    #[test]
    fn test_redis_adapter_state() {
        let store = Arc::new(MockKvStore::new());
        let adapter = RedisAdapter::new(RedisConfig::default(), store);

        assert_eq!(adapter.state(), AdapterState::Created);
        assert_eq!(adapter.name(), "redis");
    }
}
