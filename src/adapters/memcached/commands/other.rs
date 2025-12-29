//! Other command handlers: delete, incr, decr, touch, stats, flush_all, version, quit, noop.

use super::{CommandContext, CommandHandler, CommandResult, CommandState, IncrResult};
use crate::adapters::memcached::{MemcachedCommand, MemcachedResponse};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// DELETE command handler.
///
/// Deletes a key.
pub struct DeleteHandler;

impl CommandHandler for DeleteHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = match &cmd.key {
                Some(k) => k.clone(),
                None => return Ok(MemcachedResponse::client_error("missing key")),
            };

            let prefixed_key = ctx.prefixed_key(&key);

            match state.kv.delete(&prefixed_key).await {
                Ok(true) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::deleted())
                    }
                }
                Ok(false) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::not_found())
                    }
                }
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// INCR command handler.
///
/// Increments a numeric value.
pub struct IncrHandler;

impl CommandHandler for IncrHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = match &cmd.key {
                Some(k) => k.clone(),
                None => return Ok(MemcachedResponse::client_error("missing key")),
            };

            let delta = cmd.delta.unwrap_or(1);
            let prefixed_key = ctx.prefixed_key(&key);

            match state
                .kv
                .incr(&prefixed_key, delta, cmd.initial, cmd.exptime)
                .await
            {
                Ok(IncrResult::Value(new_value, _cas)) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::Numeric(new_value))
                    }
                }
                Ok(IncrResult::NotFound) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::not_found())
                    }
                }
                Ok(IncrResult::NotNumeric) => Ok(MemcachedResponse::client_error(
                    "cannot increment or decrement non-numeric value",
                )),
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// DECR command handler.
///
/// Decrements a numeric value.
pub struct DecrHandler;

impl CommandHandler for DecrHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = match &cmd.key {
                Some(k) => k.clone(),
                None => return Ok(MemcachedResponse::client_error("missing key")),
            };

            let delta = cmd.delta.unwrap_or(1);
            let prefixed_key = ctx.prefixed_key(&key);

            match state
                .kv
                .decr(&prefixed_key, delta, cmd.initial, cmd.exptime)
                .await
            {
                Ok(IncrResult::Value(new_value, _cas)) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::Numeric(new_value))
                    }
                }
                Ok(IncrResult::NotFound) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::not_found())
                    }
                }
                Ok(IncrResult::NotNumeric) => Ok(MemcachedResponse::client_error(
                    "cannot increment or decrement non-numeric value",
                )),
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// TOUCH command handler.
///
/// Updates a key's expiration time.
pub struct TouchHandler;

impl CommandHandler for TouchHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = match &cmd.key {
                Some(k) => k.clone(),
                None => return Ok(MemcachedResponse::client_error("missing key")),
            };

            let prefixed_key = ctx.prefixed_key(&key);

            match state.kv.touch(&prefixed_key, cmd.exptime).await {
                Ok(true) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::touched())
                    }
                }
                Ok(false) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::not_found())
                    }
                }
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// STATS command handler.
///
/// Returns server statistics.
pub struct StatsHandler;

impl CommandHandler for StatsHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Check for specific stats type
            let stats_type = cmd.key_str();

            let mut stats = vec![
                ("pid".to_string(), std::process::id().to_string()),
                ("uptime".to_string(), state.uptime_secs().to_string()),
                (
                    "time".to_string(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                        .to_string(),
                ),
                ("version".to_string(), state.version.clone()),
                ("libevent".to_string(), "Lattice".to_string()),
                (
                    "pointer_size".to_string(),
                    (std::mem::size_of::<usize>() * 8).to_string(),
                ),
            ];

            // Add KV store stats if available
            if let Ok(kv_stats) = state.kv.stats().await {
                stats.extend(kv_stats);
            }

            // Handle specific stats types
            match stats_type {
                Some("slabs") | Some("items") | Some("sizes") => {
                    // These are unsupported but we return empty stats
                    Ok(MemcachedResponse::Stats(vec![]))
                }
                Some("settings") => {
                    // Return settings
                    let settings = vec![
                        ("maxbytes".to_string(), "0".to_string()),
                        ("maxconns".to_string(), "1024".to_string()),
                        ("tcpport".to_string(), "11211".to_string()),
                        ("udpport".to_string(), "0".to_string()),
                        ("verbosity".to_string(), "0".to_string()),
                    ];
                    Ok(MemcachedResponse::Stats(settings))
                }
                _ => Ok(MemcachedResponse::Stats(stats)),
            }
        })
    }
}

/// FLUSH_ALL command handler.
///
/// Flushes all keys.
pub struct FlushAllHandler;

impl CommandHandler for FlushAllHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Note: exptime in flush_all is a delay, but we ignore it for simplicity
            match state.kv.flush().await {
                Ok(()) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::ok())
                    }
                }
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// VERSION command handler.
///
/// Returns the server version.
pub struct VersionHandler;

impl CommandHandler for VersionHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move { Ok(MemcachedResponse::Version(state.version.clone())) })
    }
}

/// QUIT command handler.
///
/// Signals connection close.
pub struct QuitHandler;

impl CommandHandler for QuitHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: MemcachedCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Return a special error that signals connection close
            Err(MemcachedResponse::NoResponse)
        })
    }
}

/// NOOP command handler.
///
/// Does nothing (used for pipelining).
pub struct NoopHandler;

impl CommandHandler for NoopHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: MemcachedCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move { Ok(MemcachedResponse::NoResponse) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::memcached::commands::{CasResult, KvStore, KvValue};
    use crate::adapters::memcached::ResponseStatus;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    /// Mock KV store for testing.
    struct MockKvStore {
        data: Mutex<HashMap<Vec<u8>, KvValue>>,
        next_cas: Mutex<u64>,
    }

    impl MockKvStore {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
                next_cas: Mutex::new(1),
            }
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
        ) -> Pin<Box<dyn Future<Output = Result<Vec<Option<KvValue>>, String>> + Send + '_>>
        {
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
            _key: &[u8],
            _value: KvValue,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, String>> + Send + '_>> {
            Box::pin(async move { unimplemented!() })
        }

        fn replace(
            &self,
            _key: &[u8],
            _value: KvValue,
        ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, String>> + Send + '_>> {
            Box::pin(async move { unimplemented!() })
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
            _key: &[u8],
            _value: KvValue,
            _cas: u64,
        ) -> Pin<Box<dyn Future<Output = Result<CasResult, String>> + Send + '_>> {
            Box::pin(async move { unimplemented!() })
        }

        fn append(
            &self,
            _key: &[u8],
            _data: &[u8],
        ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
            Box::pin(async move { unimplemented!() })
        }

        fn prepend(
            &self,
            _key: &[u8],
            _data: &[u8],
        ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
            Box::pin(async move { unimplemented!() })
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
                        // Parse existing value
                        let s = std::str::from_utf8(&value.data).map_err(|_| "not utf8")?;
                        let n: u64 = s.parse().map_err(|_| "not numeric")?;
                        let new_value = n.saturating_add(delta);
                        value.data = Bytes::from(new_value.to_string());
                        let mut cas = self.next_cas.lock().unwrap();
                        value.cas = *cas;
                        *cas += 1;
                        Ok(IncrResult::Value(new_value, value.cas))
                    }
                    None => {
                        if let Some(init) = initial {
                            // Create with initial value (binary protocol)
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
                        // Parse existing value
                        let s = std::str::from_utf8(&value.data).map_err(|_| "not utf8")?;
                        let n: u64 = s.parse().map_err(|_| "not numeric")?;
                        let new_value = n.saturating_sub(delta);
                        value.data = Bytes::from(new_value.to_string());
                        let mut cas = self.next_cas.lock().unwrap();
                        value.cas = *cas;
                        *cas += 1;
                        Ok(IncrResult::Value(new_value, value.cas))
                    }
                    None => {
                        if let Some(init) = initial {
                            // Create with initial value (binary protocol)
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
                let data = self.data.lock().unwrap();
                Ok(data.contains_key(&key))
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
        ) -> Pin<Box<dyn Future<Output = Result<Vec<(String, String)>, String>> + Send + '_>>
        {
            Box::pin(async move {
                let data = self.data.lock().unwrap();
                Ok(vec![("curr_items".to_string(), data.len().to_string())])
            })
        }
    }

    fn setup() -> (CommandContext, Arc<CommandState>) {
        let kv = Arc::new(MockKvStore::new());
        let ctx = CommandContext::new("test", 1);
        let state = Arc::new(CommandState::new(kv));
        (ctx, state)
    }

    #[tokio::test]
    async fn test_delete_existing() {
        let (ctx, state) = setup();

        // Set a key
        state
            .kv
            .set(b"test/mykey", KvValue::new("value", 0, 0))
            .await
            .unwrap();

        let handler = DeleteHandler;
        let cmd = MemcachedCommand::new("delete").with_key("mykey");

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(ResponseStatus::Deleted))
        ));
    }

    #[tokio::test]
    async fn test_delete_nonexistent() {
        let (ctx, state) = setup();

        let handler = DeleteHandler;
        let cmd = MemcachedCommand::new("delete").with_key("nonexistent");

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(ResponseStatus::NotFound))
        ));
    }

    #[tokio::test]
    async fn test_delete_noreply() {
        let (ctx, state) = setup();

        let handler = DeleteHandler;
        let cmd = MemcachedCommand::new("delete")
            .with_key("mykey")
            .with_noreply(true);

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(result, Ok(MemcachedResponse::NoResponse)));
    }

    #[tokio::test]
    async fn test_incr() {
        let (ctx, state) = setup();

        // Set a numeric key
        state
            .kv
            .set(b"test/counter", KvValue::new("10", 0, 0))
            .await
            .unwrap();

        let handler = IncrHandler;
        let cmd = MemcachedCommand::new("incr")
            .with_key("counter")
            .with_delta(5);

        let result = handler.execute(ctx, cmd, state).await;
        match result {
            Ok(MemcachedResponse::Numeric(n)) => assert_eq!(n, 15),
            _ => panic!("Expected Numeric response"),
        }
    }

    #[tokio::test]
    async fn test_incr_not_found() {
        let (ctx, state) = setup();

        let handler = IncrHandler;
        let cmd = MemcachedCommand::new("incr")
            .with_key("nonexistent")
            .with_delta(5);

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(ResponseStatus::NotFound))
        ));
    }

    #[tokio::test]
    async fn test_incr_with_initial() {
        let (ctx, state) = setup();

        let handler = IncrHandler;
        let cmd = MemcachedCommand::new("incr")
            .with_key("newcounter")
            .with_delta(5)
            .with_initial(100);

        let result = handler.execute(ctx, cmd, state).await;
        match result {
            Ok(MemcachedResponse::Numeric(n)) => assert_eq!(n, 100), // Initial value
            _ => panic!("Expected Numeric response"),
        }
    }

    #[tokio::test]
    async fn test_decr() {
        let (ctx, state) = setup();

        // Set a numeric key
        state
            .kv
            .set(b"test/counter", KvValue::new("10", 0, 0))
            .await
            .unwrap();

        let handler = DecrHandler;
        let cmd = MemcachedCommand::new("decr")
            .with_key("counter")
            .with_delta(3);

        let result = handler.execute(ctx, cmd, state).await;
        match result {
            Ok(MemcachedResponse::Numeric(n)) => assert_eq!(n, 7),
            _ => panic!("Expected Numeric response"),
        }
    }

    #[tokio::test]
    async fn test_decr_underflow() {
        let (ctx, state) = setup();

        // Set a numeric key
        state
            .kv
            .set(b"test/counter", KvValue::new("5", 0, 0))
            .await
            .unwrap();

        let handler = DecrHandler;
        let cmd = MemcachedCommand::new("decr")
            .with_key("counter")
            .with_delta(10);

        let result = handler.execute(ctx, cmd, state).await;
        match result {
            Ok(MemcachedResponse::Numeric(n)) => assert_eq!(n, 0), // Saturating subtraction
            _ => panic!("Expected Numeric response"),
        }
    }

    #[tokio::test]
    async fn test_touch() {
        let (ctx, state) = setup();

        // Set a key
        state
            .kv
            .set(b"test/mykey", KvValue::new("value", 0, 0))
            .await
            .unwrap();

        let handler = TouchHandler;
        let cmd = MemcachedCommand::new("touch")
            .with_key("mykey")
            .with_exptime(3600);

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(ResponseStatus::Touched))
        ));
    }

    #[tokio::test]
    async fn test_touch_not_found() {
        let (ctx, state) = setup();

        let handler = TouchHandler;
        let cmd = MemcachedCommand::new("touch")
            .with_key("nonexistent")
            .with_exptime(3600);

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(ResponseStatus::NotFound))
        ));
    }

    #[tokio::test]
    async fn test_stats() {
        let (ctx, state) = setup();

        // Add some data
        state
            .kv
            .set(b"test/key1", KvValue::new("value1", 0, 0))
            .await
            .unwrap();

        let handler = StatsHandler;
        let cmd = MemcachedCommand::new("stats");

        let result = handler.execute(ctx, cmd, state).await;
        match result {
            Ok(MemcachedResponse::Stats(stats)) => {
                // Should have basic stats
                assert!(stats.iter().any(|(k, _)| k == "pid"));
                assert!(stats.iter().any(|(k, _)| k == "version"));
                assert!(stats.iter().any(|(k, _)| k == "curr_items"));
            }
            _ => panic!("Expected Stats response"),
        }
    }

    #[tokio::test]
    async fn test_flush_all() {
        let (ctx, state) = setup();

        // Add some data
        state
            .kv
            .set(b"test/key1", KvValue::new("value1", 0, 0))
            .await
            .unwrap();
        state
            .kv
            .set(b"test/key2", KvValue::new("value2", 0, 0))
            .await
            .unwrap();

        let handler = FlushAllHandler;
        let cmd = MemcachedCommand::new("flush_all");

        let result = handler.execute(ctx.clone(), cmd, state.clone()).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(ResponseStatus::Ok))
        ));

        // Verify data was flushed
        assert!(state.kv.get(b"test/key1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_version() {
        let (ctx, state) = setup();

        let handler = VersionHandler;
        let cmd = MemcachedCommand::new("version");

        let result = handler.execute(ctx, cmd, state).await;
        match result {
            Ok(MemcachedResponse::Version(v)) => {
                assert!(v.starts_with("Lattice/"));
            }
            _ => panic!("Expected Version response"),
        }
    }

    #[tokio::test]
    async fn test_quit() {
        let (ctx, state) = setup();

        let handler = QuitHandler;
        let cmd = MemcachedCommand::new("quit");

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(result, Err(MemcachedResponse::NoResponse)));
    }

    #[tokio::test]
    async fn test_noop() {
        let (ctx, state) = setup();

        let handler = NoopHandler;
        let cmd = MemcachedCommand::new("noop");

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(result, Ok(MemcachedResponse::NoResponse)));
    }
}
