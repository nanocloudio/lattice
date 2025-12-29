//! Retrieval command handlers: get, gets, gat, gats.

use super::{CommandContext, CommandHandler, CommandResult, CommandState};
use crate::adapters::memcached::{MemcachedCommand, MemcachedResponse, ValueResponse};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// GET command handler.
///
/// Retrieves one or more values.
pub struct GetHandler;

impl CommandHandler for GetHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move { execute_get(ctx, cmd, state, false).await })
    }
}

/// GETS command handler.
///
/// Retrieves one or more values with CAS tokens.
pub struct GetsHandler;

impl CommandHandler for GetsHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move { execute_get(ctx, cmd, state, true).await })
    }
}

/// GAT/GATS command handler.
///
/// Gets values and updates their expiration time.
pub struct GatHandler;

impl CommandHandler for GatHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: MemcachedCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let include_cas = cmd.name == "gats";
            let exptime = cmd.exptime;

            // Get all keys
            let keys: Vec<_> = cmd.all_keys().cloned().collect();
            if keys.is_empty() {
                return Ok(MemcachedResponse::Values(vec![]));
            }

            let prefixed_keys: Vec<_> = keys.iter().map(|k| ctx.prefixed_key(k)).collect();
            let key_refs: Vec<&[u8]> = prefixed_keys.iter().map(|k| k.as_ref()).collect();

            // Touch all keys first
            for key in &key_refs {
                let _ = state.kv.touch(key, exptime).await;
            }

            // Then get the values
            let values = match state.kv.get_multi(&key_refs).await {
                Ok(v) => v,
                Err(e) => return Ok(MemcachedResponse::server_error(e)),
            };

            // Build response
            let mut responses = Vec::new();
            for (key, value) in keys.iter().zip(values.iter()) {
                if let Some(v) = value {
                    responses.push(ValueResponse {
                        key: key.clone(),
                        flags: v.flags,
                        data: v.data.clone(),
                        cas: if include_cas { Some(v.cas) } else { None },
                    });
                }
            }

            if cmd.noreply && responses.is_empty() {
                Ok(MemcachedResponse::NoResponse)
            } else {
                Ok(MemcachedResponse::Values(responses))
            }
        })
    }
}

/// Execute a GET/GETS command.
async fn execute_get(
    ctx: CommandContext,
    cmd: MemcachedCommand,
    state: Arc<CommandState>,
    include_cas: bool,
) -> CommandResult {
    // Get all keys
    let keys: Vec<_> = cmd.all_keys().cloned().collect();
    if keys.is_empty() {
        return Ok(MemcachedResponse::Values(vec![]));
    }

    let prefixed_keys: Vec<_> = keys.iter().map(|k| ctx.prefixed_key(k)).collect();
    let key_refs: Vec<&[u8]> = prefixed_keys.iter().map(|k| k.as_ref()).collect();

    let values = match state.kv.get_multi(&key_refs).await {
        Ok(v) => v,
        Err(e) => return Ok(MemcachedResponse::server_error(e)),
    };

    // Build response
    let mut responses = Vec::new();
    for (key, value) in keys.iter().zip(values.iter()) {
        if let Some(v) = value {
            responses.push(ValueResponse {
                key: key.clone(),
                flags: v.flags,
                data: v.data.clone(),
                cas: if include_cas { Some(v.cas) } else { None },
            });
        }
    }

    Ok(MemcachedResponse::Values(responses))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::memcached::commands::{CasResult, IncrResult, KvStore, KvValue};
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
            let mut value = value;
            Box::pin(async move {
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
            _key: &[u8],
        ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
            Box::pin(async move { unimplemented!() })
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
            _key: &[u8],
            _delta: u64,
            _initial: Option<u64>,
            _exptime: u32,
        ) -> Pin<Box<dyn Future<Output = Result<IncrResult, String>> + Send + '_>> {
            Box::pin(async move { unimplemented!() })
        }

        fn decr(
            &self,
            _key: &[u8],
            _delta: u64,
            _initial: Option<u64>,
            _exptime: u32,
        ) -> Pin<Box<dyn Future<Output = Result<IncrResult, String>> + Send + '_>> {
            Box::pin(async move { unimplemented!() })
        }

        fn touch(
            &self,
            _key: &[u8],
            _exptime: u32,
        ) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + '_>> {
            Box::pin(async move { Ok(true) })
        }

        fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>> {
            Box::pin(async move { Ok(()) })
        }

        fn stats(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<(String, String)>, String>> + Send + '_>>
        {
            Box::pin(async move { Ok(vec![]) })
        }
    }

    fn setup() -> (CommandContext, Arc<CommandState>) {
        let kv = Arc::new(MockKvStore::new());
        let ctx = CommandContext::new("test", 1);
        let state = Arc::new(CommandState::new(kv));
        (ctx, state)
    }

    #[tokio::test]
    async fn test_get_single_key() {
        let (ctx, state) = setup();

        // Set a key
        state
            .kv
            .set(b"test/mykey", KvValue::new("myvalue", 42, 0))
            .await
            .unwrap();

        let handler = GetHandler;
        let cmd = MemcachedCommand::new("get").with_key("mykey");

        let result = handler.execute(ctx, cmd, state).await;

        match result {
            Ok(MemcachedResponse::Values(values)) => {
                assert_eq!(values.len(), 1);
                assert_eq!(&values[0].key[..], b"mykey");
                assert_eq!(&values[0].data[..], b"myvalue");
                assert_eq!(values[0].flags, 42);
                assert!(values[0].cas.is_none()); // GET doesn't include CAS
            }
            _ => panic!("Expected Values response"),
        }
    }

    #[tokio::test]
    async fn test_get_multiple_keys() {
        let (ctx, state) = setup();

        // Set multiple keys
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

        let handler = GetHandler;
        let cmd = MemcachedCommand::new("get")
            .with_key("key1")
            .add_key(Bytes::from("key2"));

        let result = handler.execute(ctx, cmd, state).await;

        match result {
            Ok(MemcachedResponse::Values(values)) => {
                assert_eq!(values.len(), 2);
            }
            _ => panic!("Expected Values response"),
        }
    }

    #[tokio::test]
    async fn test_get_missing_key() {
        let (ctx, state) = setup();

        let handler = GetHandler;
        let cmd = MemcachedCommand::new("get").with_key("nonexistent");

        let result = handler.execute(ctx, cmd, state).await;

        match result {
            Ok(MemcachedResponse::Values(values)) => {
                assert!(values.is_empty());
            }
            _ => panic!("Expected empty Values response"),
        }
    }

    #[tokio::test]
    async fn test_get_partial_keys() {
        let (ctx, state) = setup();

        // Set only one key
        state
            .kv
            .set(b"test/key1", KvValue::new("value1", 0, 0))
            .await
            .unwrap();

        let handler = GetHandler;
        let cmd = MemcachedCommand::new("get")
            .with_key("key1")
            .add_key(Bytes::from("key2")); // This one doesn't exist

        let result = handler.execute(ctx, cmd, state).await;

        match result {
            Ok(MemcachedResponse::Values(values)) => {
                // Should only return the key that exists
                assert_eq!(values.len(), 1);
                assert_eq!(&values[0].key[..], b"key1");
            }
            _ => panic!("Expected Values response"),
        }
    }

    #[tokio::test]
    async fn test_gets_includes_cas() {
        let (ctx, state) = setup();

        // Set a key
        let cas = state
            .kv
            .set(b"test/mykey", KvValue::new("myvalue", 0, 0))
            .await
            .unwrap();

        let handler = GetsHandler;
        let cmd = MemcachedCommand::new("gets").with_key("mykey");

        let result = handler.execute(ctx, cmd, state).await;

        match result {
            Ok(MemcachedResponse::Values(values)) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].cas, Some(cas));
            }
            _ => panic!("Expected Values response"),
        }
    }

    #[tokio::test]
    async fn test_gat() {
        let (ctx, state) = setup();

        // Set a key
        state
            .kv
            .set(b"test/mykey", KvValue::new("myvalue", 42, 0))
            .await
            .unwrap();

        let handler = GatHandler;
        let cmd = MemcachedCommand::new("gat")
            .with_exptime(3600)
            .with_key("mykey");

        let result = handler.execute(ctx, cmd, state).await;

        match result {
            Ok(MemcachedResponse::Values(values)) => {
                assert_eq!(values.len(), 1);
                assert_eq!(&values[0].key[..], b"mykey");
                assert!(values[0].cas.is_none()); // GAT doesn't include CAS
            }
            _ => panic!("Expected Values response"),
        }
    }

    #[tokio::test]
    async fn test_gats_includes_cas() {
        let (ctx, state) = setup();

        // Set a key
        let cas = state
            .kv
            .set(b"test/mykey", KvValue::new("myvalue", 0, 0))
            .await
            .unwrap();

        let handler = GatHandler;
        let cmd = MemcachedCommand::new("gats")
            .with_exptime(3600)
            .with_key("mykey");

        let result = handler.execute(ctx, cmd, state).await;

        match result {
            Ok(MemcachedResponse::Values(values)) => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].cas, Some(cas));
            }
            _ => panic!("Expected Values response"),
        }
    }

    #[tokio::test]
    async fn test_get_empty_keys() {
        let (ctx, state) = setup();

        let handler = GetHandler;
        let cmd = MemcachedCommand::new("get"); // No keys

        let result = handler.execute(ctx, cmd, state).await;

        match result {
            Ok(MemcachedResponse::Values(values)) => {
                assert!(values.is_empty());
            }
            _ => panic!("Expected empty Values response"),
        }
    }
}
