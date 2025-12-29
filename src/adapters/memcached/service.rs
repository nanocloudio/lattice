//! Memcached adapter service.
//!
//! This module implements the main MemcachedAdapter service that accepts
//! connections and processes Memcached commands.

use super::commands::{
    CasResult, CommandContext, CommandRouter, CommandState, IncrResult, KvStore, KvValue,
};
use super::protocol::MemcachedCodec;
use super::{MemcachedCommand, MemcachedResponse, ProtocolType};
use crate::adapters::{Adapter, AdapterConfig, AdapterHealth, AdapterState};
use crate::core::error::LatticeResult;
use bytes::Bytes;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::watch;

/// Memcached adapter configuration.
#[derive(Debug, Clone)]
pub struct MemcachedConfig {
    /// Base adapter configuration.
    pub base: AdapterConfig,

    /// Protocol preference (ascii, binary, auto).
    pub protocol: ProtocolPreference,

    /// Maximum value size in bytes.
    pub max_value_size: usize,

    /// Default expiration time (0 = never).
    pub default_exptime: u32,

    /// Whether to enable SASL authentication.
    pub enable_sasl: bool,
}

/// Protocol preference for connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolPreference {
    /// Accept only ASCII protocol.
    Ascii,
    /// Accept only binary protocol.
    Binary,
    /// Auto-detect protocol (default).
    #[default]
    Auto,
}

impl Default for MemcachedConfig {
    fn default() -> Self {
        Self {
            base: AdapterConfig {
                bind_addr: "127.0.0.1:11211".parse().unwrap(),
                ..Default::default()
            },
            protocol: ProtocolPreference::Auto,
            max_value_size: 1024 * 1024, // 1MB
            default_exptime: 0,
            enable_sasl: false,
        }
    }
}

/// Memcached adapter.
pub struct MemcachedAdapter {
    /// Configuration.
    config: MemcachedConfig,

    /// Current state.
    state: RwLock<AdapterState>,

    /// Command router.
    router: CommandRouter,

    /// Shared command state.
    command_state: Arc<CommandState>,

    /// Shutdown signal.
    shutdown_tx: watch::Sender<bool>,

    /// Active connections count.
    active_connections: AtomicUsize,

    /// Total requests processed.
    total_requests: AtomicU64,

    /// Failed requests.
    failed_requests: AtomicU64,

    /// Last error message.
    last_error: RwLock<Option<String>>,

    /// Connection ID counter.
    next_connection_id: AtomicU64,
}

impl MemcachedAdapter {
    /// Create a new Memcached adapter.
    pub fn new(config: MemcachedConfig, kv: Arc<dyn KvStore>) -> Self {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        Self {
            config,
            state: RwLock::new(AdapterState::Created),
            router: CommandRouter::new(),
            command_state: Arc::new(CommandState::new(kv)),
            shutdown_tx,
            active_connections: AtomicUsize::new(0),
            total_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            last_error: RwLock::new(None),
            next_connection_id: AtomicU64::new(1),
        }
    }

    /// Handle a connection.
    #[allow(dead_code)]
    async fn handle_connection(
        &self,
        _stream: tokio::net::TcpStream,
        _addr: SocketAddr,
    ) -> LatticeResult<()> {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);

        let _ctx = CommandContext::new("default", connection_id).with_client_addr(_addr);

        let mut _codec = match self.config.protocol {
            ProtocolPreference::Ascii => MemcachedCodec::with_protocol(ProtocolType::Ascii),
            ProtocolPreference::Binary => MemcachedCodec::with_protocol(ProtocolType::Binary),
            ProtocolPreference::Auto => MemcachedCodec::new(),
        };

        // Connection handling would go here
        // This is a placeholder for the actual implementation

        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }

    /// Process a command.
    #[allow(dead_code)]
    async fn process_command(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
    ) -> MemcachedResponse {
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        let result = self
            .router
            .execute(ctx, cmd.clone(), Arc::clone(&self.command_state))
            .await;

        match result {
            Ok(response) => {
                // Handle noreply
                if cmd.noreply {
                    MemcachedResponse::NoResponse
                } else {
                    response
                }
            }
            Err(response) => {
                self.failed_requests.fetch_add(1, Ordering::Relaxed);
                response
            }
        }
    }
}

impl Adapter for MemcachedAdapter {
    fn name(&self) -> &'static str {
        "memcached"
    }

    fn state(&self) -> AdapterState {
        *self.state.read().unwrap()
    }

    fn health(&self) -> AdapterHealth {
        AdapterHealth {
            state: self.state(),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_requests: self.total_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            last_error: self.last_error.read().unwrap().clone(),
        }
    }

    fn start(&self) -> Pin<Box<dyn Future<Output = LatticeResult<()>> + Send + '_>> {
        Box::pin(async move {
            *self.state.write().unwrap() = AdapterState::Starting;

            // In a full implementation, we would:
            // 1. Bind to the configured address
            // 2. Start accepting connections
            // 3. Spawn connection handlers

            *self.state.write().unwrap() = AdapterState::Running;
            Ok(())
        })
    }

    fn stop(&self) -> Pin<Box<dyn Future<Output = LatticeResult<()>> + Send + '_>> {
        Box::pin(async move {
            *self.state.write().unwrap() = AdapterState::ShuttingDown;

            // Signal shutdown
            let _ = self.shutdown_tx.send(true);

            // Wait for connections to drain (in a full implementation)

            *self.state.write().unwrap() = AdapterState::Stopped;
            Ok(())
        })
    }

    fn bind_addr(&self) -> SocketAddr {
        self.config.base.bind_addr
    }
}

/// Mock KV store for testing.
pub struct MockKvStore {
    data: Mutex<HashMap<Vec<u8>, KvValue>>,
    next_cas: Mutex<u64>,
}

impl MockKvStore {
    /// Create a new mock KV store.
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            next_cas: Mutex::new(1),
        }
    }
}

impl Default for MockKvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore for MockKvStore {
    fn get(
        &self,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<Option<KvValue>, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move { Ok(self.data.lock().unwrap().get(&key).cloned()) })
    }

    fn get_multi(
        &self,
        keys: &[&[u8]],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Option<KvValue>>, String>> + Send + '_>> {
        let keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        Box::pin(async move {
            let data = self.data.lock().unwrap();
            Ok(keys.iter().map(|k| data.get(k).cloned()).collect())
        })
    }

    fn set(
        &self,
        key: &[u8],
        value: KvValue,
    ) -> Pin<Box<dyn Future<Output = Result<u64, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move {
            let mut value = value;
            let mut cas = self.next_cas.lock().unwrap();
            value.cas = *cas;
            *cas += 1;
            self.data.lock().unwrap().insert(key, value);
            Ok(*cas - 1)
        })
    }

    fn add(
        &self,
        key: &[u8],
        value: KvValue,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move {
            let mut value = value;
            let mut data = self.data.lock().unwrap();
            if data.contains_key(&key) {
                return Ok(None);
            }
            let mut cas = self.next_cas.lock().unwrap();
            value.cas = *cas;
            *cas += 1;
            data.insert(key, value);
            Ok(Some(*cas - 1))
        })
    }

    fn replace(
        &self,
        key: &[u8],
        value: KvValue,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move {
            let mut value = value;
            let mut data = self.data.lock().unwrap();
            if !data.contains_key(&key) {
                return Ok(None);
            }
            let mut cas = self.next_cas.lock().unwrap();
            value.cas = *cas;
            *cas += 1;
            data.insert(key, value);
            Ok(Some(*cas - 1))
        })
    }

    fn delete(
        &self,
        key: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move { Ok(self.data.lock().unwrap().remove(&key).is_some()) })
    }

    fn cas(
        &self,
        key: &[u8],
        value: KvValue,
        expected_cas: u64,
    ) -> Pin<Box<dyn Future<Output = Result<CasResult, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move {
            let mut value = value;
            let mut data = self.data.lock().unwrap();
            match data.get(&key) {
                Some(existing) => {
                    if existing.cas != expected_cas {
                        return Ok(CasResult::Exists);
                    }
                    let mut cas = self.next_cas.lock().unwrap();
                    value.cas = *cas;
                    *cas += 1;
                    data.insert(key, value);
                    Ok(CasResult::Stored(*cas - 1))
                }
                None => Ok(CasResult::NotFound),
            }
        })
    }

    fn append(
        &self,
        key: &[u8],
        data: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
        let key = key.to_vec();
        let append_data = data.to_vec();
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            match data.get_mut(&key) {
                Some(value) => {
                    let mut new_data = value.data.to_vec();
                    new_data.extend_from_slice(&append_data);
                    value.data = Bytes::from(new_data);
                    let mut cas = self.next_cas.lock().unwrap();
                    value.cas = *cas;
                    *cas += 1;
                    Ok(true)
                }
                None => Ok(false),
            }
        })
    }

    fn prepend(
        &self,
        key: &[u8],
        data: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
        let key = key.to_vec();
        let prepend_data = data.to_vec();
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            match data.get_mut(&key) {
                Some(value) => {
                    let mut new_data = prepend_data;
                    new_data.extend_from_slice(&value.data);
                    value.data = Bytes::from(new_data);
                    let mut cas = self.next_cas.lock().unwrap();
                    value.cas = *cas;
                    *cas += 1;
                    Ok(true)
                }
                None => Ok(false),
            }
        })
    }

    fn incr(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        _exptime: u32,
    ) -> Pin<Box<dyn Future<Output = Result<IncrResult, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            match data.get_mut(&key) {
                Some(value) => {
                    let s = match std::str::from_utf8(&value.data) {
                        Ok(s) => s,
                        Err(_) => return Ok(IncrResult::NotNumeric),
                    };
                    let n: u64 = match s.parse() {
                        Ok(n) => n,
                        Err(_) => return Ok(IncrResult::NotNumeric),
                    };
                    let new_value = n.saturating_add(delta);
                    value.data = Bytes::from(new_value.to_string());
                    let mut cas = self.next_cas.lock().unwrap();
                    value.cas = *cas;
                    *cas += 1;
                    Ok(IncrResult::Value(new_value, value.cas))
                }
                None => {
                    if let Some(init) = initial {
                        let mut cas = self.next_cas.lock().unwrap();
                        let value = KvValue::new(init.to_string(), 0, 0).with_cas(*cas);
                        *cas += 1;
                        data.insert(key, value);
                        Ok(IncrResult::Value(init, *cas - 1))
                    } else {
                        Ok(IncrResult::NotFound)
                    }
                }
            }
        })
    }

    fn decr(
        &self,
        key: &[u8],
        delta: u64,
        initial: Option<u64>,
        _exptime: u32,
    ) -> Pin<Box<dyn Future<Output = Result<IncrResult, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            match data.get_mut(&key) {
                Some(value) => {
                    let s = match std::str::from_utf8(&value.data) {
                        Ok(s) => s,
                        Err(_) => return Ok(IncrResult::NotNumeric),
                    };
                    let n: u64 = match s.parse() {
                        Ok(n) => n,
                        Err(_) => return Ok(IncrResult::NotNumeric),
                    };
                    let new_value = n.saturating_sub(delta);
                    value.data = Bytes::from(new_value.to_string());
                    let mut cas = self.next_cas.lock().unwrap();
                    value.cas = *cas;
                    *cas += 1;
                    Ok(IncrResult::Value(new_value, value.cas))
                }
                None => {
                    if let Some(init) = initial {
                        let mut cas = self.next_cas.lock().unwrap();
                        let value = KvValue::new(init.to_string(), 0, 0).with_cas(*cas);
                        *cas += 1;
                        data.insert(key, value);
                        Ok(IncrResult::Value(init, *cas - 1))
                    } else {
                        Ok(IncrResult::NotFound)
                    }
                }
            }
        })
    }

    fn touch(
        &self,
        key: &[u8],
        _exptime: u32,
    ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
        let key = key.to_vec();
        Box::pin(async move {
            let mut data = self.data.lock().unwrap();
            match data.get_mut(&key) {
                Some(value) => {
                    let mut cas = self.next_cas.lock().unwrap();
                    value.cas = *cas;
                    *cas += 1;
                    Ok(true)
                }
                None => Ok(false),
            }
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
        Box::pin(async move {
            self.data.lock().unwrap().clear();
            Ok(())
        })
    }

    fn stats(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<(String, String)>, String>> + Send + '_>> {
        Box::pin(async move {
            let data = self.data.lock().unwrap();
            Ok(vec![
                ("curr_items".to_string(), data.len().to_string()),
                ("total_items".to_string(), data.len().to_string()),
                ("bytes".to_string(), "0".to_string()),
                ("cmd_get".to_string(), "0".to_string()),
                ("cmd_set".to_string(), "0".to_string()),
                ("get_hits".to_string(), "0".to_string()),
                ("get_misses".to_string(), "0".to_string()),
            ])
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memcached_config_default() {
        let config = MemcachedConfig::default();
        assert_eq!(config.base.bind_addr.port(), 11211);
        assert_eq!(config.protocol, ProtocolPreference::Auto);
        assert_eq!(config.max_value_size, 1024 * 1024);
    }

    #[test]
    fn test_adapter_creation() {
        let kv = Arc::new(MockKvStore::new());
        let adapter = MemcachedAdapter::new(MemcachedConfig::default(), kv);

        assert_eq!(adapter.name(), "memcached");
        assert_eq!(adapter.state(), AdapterState::Created);
    }

    #[test]
    fn test_adapter_health() {
        let kv = Arc::new(MockKvStore::new());
        let adapter = MemcachedAdapter::new(MemcachedConfig::default(), kv);

        let health = adapter.health();
        assert_eq!(health.state, AdapterState::Created);
        assert_eq!(health.active_connections, 0);
        assert_eq!(health.total_requests, 0);
    }

    #[tokio::test]
    async fn test_adapter_start_stop() {
        let kv = Arc::new(MockKvStore::new());
        let adapter = MemcachedAdapter::new(MemcachedConfig::default(), kv);

        adapter.start().await.unwrap();
        assert_eq!(adapter.state(), AdapterState::Running);

        adapter.stop().await.unwrap();
        assert_eq!(adapter.state(), AdapterState::Stopped);
    }

    #[tokio::test]
    async fn test_mock_kv_store_basic() {
        let store = MockKvStore::new();

        // Set
        let cas = store
            .set(b"key", KvValue::new("value", 0, 0))
            .await
            .unwrap();
        assert!(cas > 0);

        // Get
        let value = store.get(b"key").await.unwrap().unwrap();
        assert_eq!(&value.data[..], b"value");

        // Delete
        assert!(store.delete(b"key").await.unwrap());
        assert!(store.get(b"key").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_mock_kv_store_cas() {
        let store = MockKvStore::new();

        // Set
        let cas = store
            .set(b"key", KvValue::new("value", 0, 0))
            .await
            .unwrap();

        // CAS with correct token
        let result = store
            .cas(b"key", KvValue::new("new", 0, 0), cas)
            .await
            .unwrap();
        assert!(matches!(result, CasResult::Stored(_)));

        // CAS with incorrect token
        let result = store
            .cas(b"key", KvValue::new("newer", 0, 0), 99999)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Exists);
    }

    #[tokio::test]
    async fn test_mock_kv_store_incr_decr() {
        let store = MockKvStore::new();

        // Set numeric value
        store
            .set(b"counter", KvValue::new("10", 0, 0))
            .await
            .unwrap();

        // Increment
        let result = store.incr(b"counter", 5, None, 0).await.unwrap();
        assert!(matches!(result, IncrResult::Value(15, _)));

        // Decrement
        let result = store.decr(b"counter", 3, None, 0).await.unwrap();
        assert!(matches!(result, IncrResult::Value(12, _)));
    }

    #[tokio::test]
    async fn test_mock_kv_store_append_prepend() {
        let store = MockKvStore::new();

        // Set
        store
            .set(b"key", KvValue::new("hello", 0, 0))
            .await
            .unwrap();

        // Append
        assert!(store.append(b"key", b" world").await.unwrap());
        let value = store.get(b"key").await.unwrap().unwrap();
        assert_eq!(&value.data[..], b"hello world");

        // Prepend
        assert!(store.prepend(b"key", b"say: ").await.unwrap());
        let value = store.get(b"key").await.unwrap().unwrap();
        assert_eq!(&value.data[..], b"say: hello world");
    }
}
