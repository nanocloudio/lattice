//! Storage command handlers: set, add, replace, append, prepend, cas.

use super::{CommandContext, CommandHandler, CommandResult, CommandState, KvValue};
use crate::adapters::memcached::{MemcachedCommand, MemcachedResponse};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// SET command handler.
///
/// Stores a value unconditionally.
pub struct SetHandler;

impl CommandHandler for SetHandler {
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

            let data = match &cmd.data {
                Some(d) => d.clone(),
                None => return Ok(MemcachedResponse::client_error("missing data")),
            };

            let prefixed_key = ctx.prefixed_key(&key);
            let value = KvValue::new(data, cmd.flags, cmd.exptime);

            match state.kv.set(&prefixed_key, value).await {
                Ok(_cas) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::stored())
                    }
                }
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// ADD command handler.
///
/// Stores a value only if the key doesn't already exist.
pub struct AddHandler;

impl CommandHandler for AddHandler {
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

            let data = match &cmd.data {
                Some(d) => d.clone(),
                None => return Ok(MemcachedResponse::client_error("missing data")),
            };

            let prefixed_key = ctx.prefixed_key(&key);
            let value = KvValue::new(data, cmd.flags, cmd.exptime);

            match state.kv.add(&prefixed_key, value).await {
                Ok(Some(_cas)) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::stored())
                    }
                }
                Ok(None) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::not_stored())
                    }
                }
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// REPLACE command handler.
///
/// Stores a value only if the key already exists.
pub struct ReplaceHandler;

impl CommandHandler for ReplaceHandler {
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

            let data = match &cmd.data {
                Some(d) => d.clone(),
                None => return Ok(MemcachedResponse::client_error("missing data")),
            };

            let prefixed_key = ctx.prefixed_key(&key);
            let value = KvValue::new(data, cmd.flags, cmd.exptime);

            match state.kv.replace(&prefixed_key, value).await {
                Ok(Some(_cas)) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::stored())
                    }
                }
                Ok(None) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::not_stored())
                    }
                }
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// APPEND command handler.
///
/// Appends data to an existing value.
pub struct AppendHandler;

impl CommandHandler for AppendHandler {
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

            let data = match &cmd.data {
                Some(d) => d.clone(),
                None => return Ok(MemcachedResponse::client_error("missing data")),
            };

            let prefixed_key = ctx.prefixed_key(&key);

            match state.kv.append(&prefixed_key, &data).await {
                Ok(true) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::stored())
                    }
                }
                Ok(false) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::not_stored())
                    }
                }
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// PREPEND command handler.
///
/// Prepends data to an existing value.
pub struct PrependHandler;

impl CommandHandler for PrependHandler {
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

            let data = match &cmd.data {
                Some(d) => d.clone(),
                None => return Ok(MemcachedResponse::client_error("missing data")),
            };

            let prefixed_key = ctx.prefixed_key(&key);

            match state.kv.prepend(&prefixed_key, &data).await {
                Ok(true) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::stored())
                    }
                }
                Ok(false) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::not_stored())
                    }
                }
                Err(e) => Ok(MemcachedResponse::server_error(e)),
            }
        })
    }
}

/// CAS command handler.
///
/// Check-and-set: stores a value only if the CAS token matches.
pub struct CasHandler;

impl CommandHandler for CasHandler {
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

            let data = match &cmd.data {
                Some(d) => d.clone(),
                None => return Ok(MemcachedResponse::client_error("missing data")),
            };

            let cas = match cmd.cas {
                Some(c) => c,
                None => return Ok(MemcachedResponse::client_error("missing cas")),
            };

            let prefixed_key = ctx.prefixed_key(&key);
            let value = KvValue::new(data, cmd.flags, cmd.exptime);

            match state.kv.cas(&prefixed_key, value, cas).await {
                Ok(super::CasResult::Stored(_new_cas)) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::stored())
                    }
                }
                Ok(super::CasResult::Exists) => {
                    if cmd.noreply {
                        Ok(MemcachedResponse::NoResponse)
                    } else {
                        Ok(MemcachedResponse::exists())
                    }
                }
                Ok(super::CasResult::NotFound) => {
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
            cas: u64,
        ) -> Pin<Box<dyn Future<Output = Result<CasResult, String>> + Send + '_>> {
            let key = key.to_vec();
            Box::pin(async move {
                let mut value = value;
                let expected_cas = cas;
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
                        let mut new_data = prepend_data.to_vec();
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
            Box::pin(async move { unimplemented!() })
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
    async fn test_set() {
        let (ctx, state) = setup();
        let handler = SetHandler;

        let cmd = MemcachedCommand::new("set")
            .with_key("mykey")
            .with_data("myvalue")
            .with_flags(42)
            .with_exptime(3600);

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::Stored
            ))
        ));
    }

    #[tokio::test]
    async fn test_set_noreply() {
        let (ctx, state) = setup();
        let handler = SetHandler;

        let cmd = MemcachedCommand::new("set")
            .with_key("mykey")
            .with_data("myvalue")
            .with_noreply(true);

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(result, Ok(MemcachedResponse::NoResponse)));
    }

    #[tokio::test]
    async fn test_add_success() {
        let (ctx, state) = setup();
        let handler = AddHandler;

        let cmd = MemcachedCommand::new("add")
            .with_key("newkey")
            .with_data("newvalue");

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::Stored
            ))
        ));
    }

    #[tokio::test]
    async fn test_add_failure() {
        let (ctx, state) = setup();

        // First set a key
        state
            .kv
            .set(b"test/existing", KvValue::new("value", 0, 0))
            .await
            .unwrap();

        let handler = AddHandler;
        let cmd = MemcachedCommand::new("add")
            .with_key("existing")
            .with_data("newvalue");

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::NotStored
            ))
        ));
    }

    #[tokio::test]
    async fn test_replace_success() {
        let (ctx, state) = setup();

        // First set a key
        state
            .kv
            .set(b"test/existing", KvValue::new("value", 0, 0))
            .await
            .unwrap();

        let handler = ReplaceHandler;
        let cmd = MemcachedCommand::new("replace")
            .with_key("existing")
            .with_data("newvalue");

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::Stored
            ))
        ));
    }

    #[tokio::test]
    async fn test_replace_failure() {
        let (ctx, state) = setup();
        let handler = ReplaceHandler;

        let cmd = MemcachedCommand::new("replace")
            .with_key("nonexistent")
            .with_data("value");

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::NotStored
            ))
        ));
    }

    #[tokio::test]
    async fn test_append() {
        let (ctx, state) = setup();

        // First set a key
        state
            .kv
            .set(b"test/key", KvValue::new("hello", 0, 0))
            .await
            .unwrap();

        let handler = AppendHandler;
        let cmd = MemcachedCommand::new("append")
            .with_key("key")
            .with_data(" world");

        let result = handler.execute(ctx.clone(), cmd, state.clone()).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::Stored
            ))
        ));

        // Verify the value was appended
        let value = state.kv.get(b"test/key").await.unwrap().unwrap();
        assert_eq!(&value.data[..], b"hello world");
    }

    #[tokio::test]
    async fn test_prepend() {
        let (ctx, state) = setup();

        // First set a key
        state
            .kv
            .set(b"test/key", KvValue::new("world", 0, 0))
            .await
            .unwrap();

        let handler = PrependHandler;
        let cmd = MemcachedCommand::new("prepend")
            .with_key("key")
            .with_data("hello ");

        let result = handler.execute(ctx.clone(), cmd, state.clone()).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::Stored
            ))
        ));

        // Verify the value was prepended
        let value = state.kv.get(b"test/key").await.unwrap().unwrap();
        assert_eq!(&value.data[..], b"hello world");
    }

    #[tokio::test]
    async fn test_cas_success() {
        let (ctx, state) = setup();

        // First set a key
        let cas = state
            .kv
            .set(b"test/key", KvValue::new("value", 0, 0))
            .await
            .unwrap();

        let handler = CasHandler;
        let cmd = MemcachedCommand::new("cas")
            .with_key("key")
            .with_data("newvalue")
            .with_cas(cas);

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::Stored
            ))
        ));
    }

    #[tokio::test]
    async fn test_cas_exists() {
        let (ctx, state) = setup();

        // First set a key
        state
            .kv
            .set(b"test/key", KvValue::new("value", 0, 0))
            .await
            .unwrap();

        let handler = CasHandler;
        let cmd = MemcachedCommand::new("cas")
            .with_key("key")
            .with_data("newvalue")
            .with_cas(99999); // Wrong CAS

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::Exists
            ))
        ));
    }

    #[tokio::test]
    async fn test_cas_not_found() {
        let (ctx, state) = setup();
        let handler = CasHandler;

        let cmd = MemcachedCommand::new("cas")
            .with_key("nonexistent")
            .with_data("value")
            .with_cas(1);

        let result = handler.execute(ctx, cmd, state).await;
        assert!(matches!(
            result,
            Ok(MemcachedResponse::Status(
                crate::adapters::memcached::ResponseStatus::NotFound
            ))
        ));
    }
}
