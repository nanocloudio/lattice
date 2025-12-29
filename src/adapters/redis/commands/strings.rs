//! Redis string command handlers.
//!
//! Implements GET, SET, MGET, MSET, and related string commands.

use super::{CommandContext, CommandHandler, CommandResult, CommandState, PutOptions};
use crate::adapters::redis::{RedisCommand, RedisError, RedisValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// GET command handler.
pub struct GetHandler;

impl CommandHandler for GetHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("GET"))?;

            match state.kv().get(&ctx.request.tenant_id, key).await? {
                Some(entry) => Ok(RedisValue::BulkString(entry.value)),
                None => Ok(RedisValue::Null),
            }
        })
    }

    fn name(&self) -> &'static str {
        "GET"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// SET command handler.
pub struct SetHandler;

impl SetHandler {
    /// Parse SET command options.
    fn parse_options(cmd: &RedisCommand) -> Result<(PutOptions, bool), RedisError> {
        let mut options = PutOptions::default();
        let mut get = false;
        let mut i = 2; // Start after key and value

        while i < cmd.argc() {
            let arg = cmd
                .arg_str(i)
                .ok_or_else(RedisError::syntax)?
                .to_uppercase();

            match arg.as_str() {
                "EX" => {
                    i += 1;
                    let secs = cmd.arg_int(i).ok_or_else(|| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?;
                    if secs <= 0 {
                        return Err(RedisError::new(
                            "ERR",
                            "invalid expire time in 'set' command",
                        ));
                    }
                    options.ttl_ms = (secs as u64) * 1000;
                }
                "PX" => {
                    i += 1;
                    let ms = cmd.arg_int(i).ok_or_else(|| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?;
                    if ms <= 0 {
                        return Err(RedisError::new(
                            "ERR",
                            "invalid expire time in 'set' command",
                        ));
                    }
                    options.ttl_ms = ms as u64;
                }
                "EXAT" => {
                    i += 1;
                    let unix_secs = cmd.arg_int(i).ok_or_else(|| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?;
                    if unix_secs <= 0 {
                        return Err(RedisError::new(
                            "ERR",
                            "invalid expire time in 'set' command",
                        ));
                    }
                    // Convert absolute time to TTL (would need current time)
                    // For now, store as-is and let the KV layer handle it
                    options.ttl_ms = (unix_secs as u64) * 1000;
                }
                "PXAT" => {
                    i += 1;
                    let unix_ms = cmd.arg_int(i).ok_or_else(|| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?;
                    if unix_ms <= 0 {
                        return Err(RedisError::new(
                            "ERR",
                            "invalid expire time in 'set' command",
                        ));
                    }
                    options.ttl_ms = unix_ms as u64;
                }
                "NX" => {
                    if options.if_exists {
                        return Err(RedisError::new(
                            "ERR",
                            "XX and NX options at the same time are not compatible",
                        ));
                    }
                    options.if_not_exists = true;
                }
                "XX" => {
                    if options.if_not_exists {
                        return Err(RedisError::new(
                            "ERR",
                            "XX and NX options at the same time are not compatible",
                        ));
                    }
                    options.if_exists = true;
                }
                "KEEPTTL" => {
                    options.keep_ttl = true;
                }
                "GET" => {
                    get = true;
                    options.get_previous = true;
                }
                "IFEQ" => {
                    // Custom extension for CAS
                    i += 1;
                    let _expected = cmd.arg(i).ok_or_else(RedisError::syntax)?;
                    // Would need to implement value comparison
                }
                _ => {
                    return Err(RedisError::syntax());
                }
            }
            i += 1;
        }

        Ok((options, get))
    }
}

impl CommandHandler for SetHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("SET"))?;
            let value = cmd.arg(1).ok_or_else(|| RedisError::wrong_arity("SET"))?;

            let (options, get) = Self::parse_options(&cmd)?;

            let result = state
                .kv()
                .put(&ctx.request.tenant_id, key, value, options)
                .await?;

            if get {
                // Return previous value
                match result.previous {
                    Some(entry) => Ok(RedisValue::BulkString(entry.value)),
                    None => Ok(RedisValue::Null),
                }
            } else if result.success {
                Ok(RedisValue::ok())
            } else {
                // NX/XX condition not met
                Ok(RedisValue::Null)
            }
        })
    }

    fn name(&self) -> &'static str {
        "SET"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn is_write(&self) -> bool {
        true
    }

    fn requires_linearizable(&self) -> bool {
        // SET with NX/XX requires linearizability
        // We check this dynamically based on options
        false
    }
}

/// SETNX command handler (legacy, use SET ... NX).
pub struct SetNxHandler;

impl CommandHandler for SetNxHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("SETNX"))?;
            let value = cmd.arg(1).ok_or_else(|| RedisError::wrong_arity("SETNX"))?;

            let options = PutOptions {
                if_not_exists: true,
                ..Default::default()
            };

            let result = state
                .kv()
                .put(&ctx.request.tenant_id, key, value, options)
                .await?;

            // SETNX returns 1 if set, 0 if key existed
            Ok(RedisValue::Integer(if result.success { 1 } else { 0 }))
        })
    }

    fn name(&self) -> &'static str {
        "SETNX"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }

    fn is_write(&self) -> bool {
        true
    }

    fn requires_linearizable(&self) -> bool {
        true // CAS operation
    }
}

/// SETEX command handler (legacy, use SET ... EX).
pub struct SetExHandler;

impl CommandHandler for SetExHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("SETEX"))?;
            let seconds = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;
            let value = cmd.arg(2).ok_or_else(|| RedisError::wrong_arity("SETEX"))?;

            if seconds <= 0 {
                return Err(RedisError::new(
                    "ERR",
                    "invalid expire time in 'setex' command",
                ));
            }

            let options = PutOptions {
                ttl_ms: (seconds as u64) * 1000,
                ..Default::default()
            };

            state
                .kv()
                .put(&ctx.request.tenant_id, key, value, options)
                .await?;
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "SETEX"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// PSETEX command handler (legacy, use SET ... PX).
pub struct PSetExHandler;

impl CommandHandler for PSetExHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("PSETEX"))?;
            let milliseconds = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;
            let value = cmd
                .arg(2)
                .ok_or_else(|| RedisError::wrong_arity("PSETEX"))?;

            if milliseconds <= 0 {
                return Err(RedisError::new(
                    "ERR",
                    "invalid expire time in 'psetex' command",
                ));
            }

            let options = PutOptions {
                ttl_ms: milliseconds as u64,
                ..Default::default()
            };

            state
                .kv()
                .put(&ctx.request.tenant_id, key, value, options)
                .await?;
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "PSETEX"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// MGET command handler.
pub struct MGetHandler;

impl CommandHandler for MGetHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let keys: Vec<&[u8]> = cmd.args.iter().map(|b| b.as_ref()).collect();

            let values = state.kv().mget(&ctx.request.tenant_id, &keys).await?;

            let results: Vec<RedisValue> = values
                .into_iter()
                .map(|v| match v {
                    Some(bytes) => RedisValue::BulkString(bytes),
                    None => RedisValue::Null,
                })
                .collect();

            Ok(RedisValue::Array(results))
        })
    }

    fn name(&self) -> &'static str {
        "MGET"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// MSET command handler.
pub struct MSetHandler;

impl CommandHandler for MSetHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            if !cmd.argc().is_multiple_of(2) {
                return Err(RedisError::wrong_arity("MSET"));
            }

            let mut pairs: Vec<(&[u8], &[u8])> = Vec::new();
            for i in (0..cmd.argc()).step_by(2) {
                let key = cmd.arg(i).ok_or_else(|| RedisError::wrong_arity("MSET"))?;
                let value = cmd
                    .arg(i + 1)
                    .ok_or_else(|| RedisError::wrong_arity("MSET"))?;
                pairs.push((key.as_ref(), value.as_ref()));
            }

            state.kv().mset(&ctx.request.tenant_id, &pairs).await?;
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "MSET"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// GETEX command handler.
pub struct GetExHandler;

impl CommandHandler for GetExHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("GETEX"))?;

            // First get the value
            let entry = state.kv().get(&ctx.request.tenant_id, key).await?;

            if entry.is_none() {
                return Ok(RedisValue::Null);
            }

            let entry = entry.unwrap();
            let value = entry.value.clone();

            // Parse TTL options
            let mut ttl_ms = 0u64;
            let mut persist = false;
            let mut i = 1;

            while i < cmd.argc() {
                let arg = cmd
                    .arg_str(i)
                    .ok_or_else(RedisError::syntax)?
                    .to_uppercase();

                match arg.as_str() {
                    "EX" => {
                        i += 1;
                        let secs = cmd.arg_int(i).ok_or_else(|| {
                            RedisError::new("ERR", "value is not an integer or out of range")
                        })?;
                        ttl_ms = (secs as u64) * 1000;
                    }
                    "PX" => {
                        i += 1;
                        let ms = cmd.arg_int(i).ok_or_else(|| {
                            RedisError::new("ERR", "value is not an integer or out of range")
                        })?;
                        ttl_ms = ms as u64;
                    }
                    "EXAT" => {
                        i += 1;
                        let unix_secs = cmd.arg_int(i).ok_or_else(|| {
                            RedisError::new("ERR", "value is not an integer or out of range")
                        })?;
                        ttl_ms = (unix_secs as u64) * 1000;
                    }
                    "PXAT" => {
                        i += 1;
                        let unix_ms = cmd.arg_int(i).ok_or_else(|| {
                            RedisError::new("ERR", "value is not an integer or out of range")
                        })?;
                        ttl_ms = unix_ms as u64;
                    }
                    "PERSIST" => {
                        persist = true;
                    }
                    _ => {
                        return Err(RedisError::syntax());
                    }
                }
                i += 1;
            }

            // Update TTL if specified
            if ttl_ms > 0 || persist {
                let options = PutOptions {
                    ttl_ms: if persist { 0 } else { ttl_ms },
                    ..Default::default()
                };
                state
                    .kv()
                    .put(&ctx.request.tenant_id, key, &value, options)
                    .await?;
            }

            Ok(RedisValue::BulkString(value))
        })
    }

    fn name(&self) -> &'static str {
        "GETEX"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn is_write(&self) -> bool {
        true // Can modify TTL
    }
}

/// GETDEL command handler.
pub struct GetDelHandler;

impl CommandHandler for GetDelHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("GETDEL"))?;

            // Get the value first
            let entry = state.kv().get(&ctx.request.tenant_id, key).await?;

            if entry.is_none() {
                return Ok(RedisValue::Null);
            }

            let value = entry.unwrap().value;

            // Delete the key
            state.kv().delete(&ctx.request.tenant_id, key).await?;

            Ok(RedisValue::BulkString(value))
        })
    }

    fn name(&self) -> &'static str {
        "GETDEL"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// APPEND command handler.
pub struct AppendHandler;

impl CommandHandler for AppendHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("APPEND"))?;
            let value = cmd
                .arg(1)
                .ok_or_else(|| RedisError::wrong_arity("APPEND"))?;

            // Get existing value
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            let new_value = match existing {
                Some(entry) => {
                    let mut combined = entry.value.to_vec();
                    combined.extend_from_slice(value);
                    Bytes::from(combined)
                }
                None => value.clone(),
            };

            let len = new_value.len() as i64;

            let options = PutOptions::default();
            state
                .kv()
                .put(&ctx.request.tenant_id, key, &new_value, options)
                .await?;

            Ok(RedisValue::Integer(len))
        })
    }

    fn name(&self) -> &'static str {
        "APPEND"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// STRLEN command handler.
pub struct StrLenHandler;

impl CommandHandler for StrLenHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("STRLEN"))?;

            match state.kv().get(&ctx.request.tenant_id, key).await? {
                Some(entry) => Ok(RedisValue::Integer(entry.value.len() as i64)),
                None => Ok(RedisValue::Integer(0)),
            }
        })
    }

    fn name(&self) -> &'static str {
        "STRLEN"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// INCR command handler.
pub struct IncrHandler;

impl CommandHandler for IncrHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("INCR"))?;

            // Get existing value
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            let current: i64 = match &existing {
                Some(entry) => {
                    let s = std::str::from_utf8(&entry.value).map_err(|_| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?;
                    s.parse().map_err(|_| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?
                }
                None => 0,
            };

            let new_value = current.checked_add(1).ok_or_else(|| {
                RedisError::new(
                    "ERR",
                    "increment would produce a value outside the range of a 64-bit signed integer",
                )
            })?;

            let value_str = new_value.to_string();
            let options = PutOptions::default();
            state
                .kv()
                .put(&ctx.request.tenant_id, key, value_str.as_bytes(), options)
                .await?;

            Ok(RedisValue::Integer(new_value))
        })
    }

    fn name(&self) -> &'static str {
        "INCR"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }

    fn is_write(&self) -> bool {
        true
    }

    fn requires_linearizable(&self) -> bool {
        true // Atomic increment
    }
}

/// INCRBY command handler.
pub struct IncrByHandler;

impl CommandHandler for IncrByHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("INCRBY"))?;
            let increment = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;

            // Get existing value
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            let current: i64 = match &existing {
                Some(entry) => {
                    let s = std::str::from_utf8(&entry.value).map_err(|_| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?;
                    s.parse().map_err(|_| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?
                }
                None => 0,
            };

            let new_value = current.checked_add(increment).ok_or_else(|| {
                RedisError::new(
                    "ERR",
                    "increment would produce a value outside the range of a 64-bit signed integer",
                )
            })?;

            let value_str = new_value.to_string();
            let options = PutOptions::default();
            state
                .kv()
                .put(&ctx.request.tenant_id, key, value_str.as_bytes(), options)
                .await?;

            Ok(RedisValue::Integer(new_value))
        })
    }

    fn name(&self) -> &'static str {
        "INCRBY"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }

    fn is_write(&self) -> bool {
        true
    }

    fn requires_linearizable(&self) -> bool {
        true
    }
}

/// DECR command handler.
pub struct DecrHandler;

impl CommandHandler for DecrHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("DECR"))?;

            // Get existing value
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            let current: i64 = match &existing {
                Some(entry) => {
                    let s = std::str::from_utf8(&entry.value).map_err(|_| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?;
                    s.parse().map_err(|_| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?
                }
                None => 0,
            };

            let new_value = current.checked_sub(1).ok_or_else(|| {
                RedisError::new(
                    "ERR",
                    "decrement would produce a value outside the range of a 64-bit signed integer",
                )
            })?;

            let value_str = new_value.to_string();
            let options = PutOptions::default();
            state
                .kv()
                .put(&ctx.request.tenant_id, key, value_str.as_bytes(), options)
                .await?;

            Ok(RedisValue::Integer(new_value))
        })
    }

    fn name(&self) -> &'static str {
        "DECR"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }

    fn is_write(&self) -> bool {
        true
    }

    fn requires_linearizable(&self) -> bool {
        true
    }
}

/// DECRBY command handler.
pub struct DecrByHandler;

impl CommandHandler for DecrByHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("DECRBY"))?;
            let decrement = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;

            // Get existing value
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            let current: i64 = match &existing {
                Some(entry) => {
                    let s = std::str::from_utf8(&entry.value).map_err(|_| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?;
                    s.parse().map_err(|_| {
                        RedisError::new("ERR", "value is not an integer or out of range")
                    })?
                }
                None => 0,
            };

            let new_value = current.checked_sub(decrement).ok_or_else(|| {
                RedisError::new(
                    "ERR",
                    "decrement would produce a value outside the range of a 64-bit signed integer",
                )
            })?;

            let value_str = new_value.to_string();
            let options = PutOptions::default();
            state
                .kv()
                .put(&ctx.request.tenant_id, key, value_str.as_bytes(), options)
                .await?;

            Ok(RedisValue::Integer(new_value))
        })
    }

    fn name(&self) -> &'static str {
        "DECRBY"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }

    fn is_write(&self) -> bool {
        true
    }

    fn requires_linearizable(&self) -> bool {
        true
    }
}

/// INCRBYFLOAT command handler.
pub struct IncrByFloatHandler;

impl CommandHandler for IncrByFloatHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("INCRBYFLOAT"))?;
            let increment_str = cmd
                .arg_str(1)
                .ok_or_else(|| RedisError::wrong_arity("INCRBYFLOAT"))?;
            let increment: f64 = increment_str
                .parse()
                .map_err(|_| RedisError::new("ERR", "value is not a valid float"))?;

            if increment.is_nan() || increment.is_infinite() {
                return Err(RedisError::new("ERR", "value is not a valid float"));
            }

            // Get existing value
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            let current: f64 = match &existing {
                Some(entry) => {
                    let s = std::str::from_utf8(&entry.value)
                        .map_err(|_| RedisError::new("ERR", "value is not a valid float"))?;
                    s.parse()
                        .map_err(|_| RedisError::new("ERR", "value is not a valid float"))?
                }
                None => 0.0,
            };

            let new_value = current + increment;

            if new_value.is_nan() || new_value.is_infinite() {
                return Err(RedisError::new(
                    "ERR",
                    "increment would produce a value outside the range",
                ));
            }

            // Format with enough precision
            let value_str = format!("{}", new_value);
            let options = PutOptions::default();
            state
                .kv()
                .put(&ctx.request.tenant_id, key, value_str.as_bytes(), options)
                .await?;

            Ok(RedisValue::BulkString(Bytes::from(value_str)))
        })
    }

    fn name(&self) -> &'static str {
        "INCRBYFLOAT"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }

    fn is_write(&self) -> bool {
        true
    }

    fn requires_linearizable(&self) -> bool {
        true
    }
}

/// GETRANGE command handler.
pub struct GetRangeHandler;

impl CommandHandler for GetRangeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("GETRANGE"))?;
            let start = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;
            let end = cmd
                .arg_int(2)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;

            match state.kv().get(&ctx.request.tenant_id, key).await? {
                Some(entry) => {
                    let value = &entry.value;
                    let len = value.len() as i64;

                    // Handle negative indices
                    let start = if start < 0 {
                        (len + start).max(0) as usize
                    } else {
                        start as usize
                    };

                    let end = if end < 0 {
                        (len + end).max(0) as usize
                    } else {
                        (end as usize).min(value.len().saturating_sub(1))
                    };

                    if start > end || start >= value.len() {
                        Ok(RedisValue::BulkString(Bytes::new()))
                    } else {
                        Ok(RedisValue::BulkString(Bytes::copy_from_slice(
                            &value[start..=end],
                        )))
                    }
                }
                None => Ok(RedisValue::BulkString(Bytes::new())),
            }
        })
    }

    fn name(&self) -> &'static str {
        "GETRANGE"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }
}

/// SETRANGE command handler.
pub struct SetRangeHandler;

impl CommandHandler for SetRangeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("SETRANGE"))?;
            let offset = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;
            let value = cmd
                .arg(2)
                .ok_or_else(|| RedisError::wrong_arity("SETRANGE"))?;

            if offset < 0 {
                return Err(RedisError::new("ERR", "offset is out of range"));
            }

            let offset = offset as usize;

            // Maximum string length check (512MB)
            if offset + value.len() > 512 * 1024 * 1024 {
                return Err(RedisError::new(
                    "ERR",
                    "string exceeds maximum allowed size (512MB)",
                ));
            }

            // Get existing value
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            let mut new_value = match existing {
                Some(entry) => entry.value.to_vec(),
                None => Vec::new(),
            };

            // Extend with zeros if necessary
            if offset > new_value.len() {
                new_value.resize(offset, 0);
            }

            // Overwrite or extend
            let end = offset + value.len();
            if end > new_value.len() {
                new_value.resize(end, 0);
            }
            new_value[offset..end].copy_from_slice(value);

            let len = new_value.len() as i64;

            let options = PutOptions::default();
            state
                .kv()
                .put(&ctx.request.tenant_id, key, &new_value, options)
                .await?;

            Ok(RedisValue::Integer(len))
        })
    }

    fn name(&self) -> &'static str {
        "SETRANGE"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn max_args(&self) -> Option<usize> {
        Some(3)
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// GETSET command handler (deprecated, use SET ... GET).
pub struct GetSetHandler;

impl CommandHandler for GetSetHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("GETSET"))?;
            let value = cmd
                .arg(1)
                .ok_or_else(|| RedisError::wrong_arity("GETSET"))?;

            let options = PutOptions {
                get_previous: true,
                ..Default::default()
            };

            let result = state
                .kv()
                .put(&ctx.request.tenant_id, key, value, options)
                .await?;

            match result.previous {
                Some(entry) => Ok(RedisValue::BulkString(entry.value)),
                None => Ok(RedisValue::Null),
            }
        })
    }

    fn name(&self) -> &'static str {
        "GETSET"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }

    fn is_write(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_parse_options_nx() {
        let cmd = RedisCommand::new(
            "SET",
            vec![Bytes::from("key"), Bytes::from("value"), Bytes::from("NX")],
        );
        let (options, get) = SetHandler::parse_options(&cmd).unwrap();
        assert!(options.if_not_exists);
        assert!(!options.if_exists);
        assert!(!get);
    }

    #[test]
    fn test_set_parse_options_xx_ex() {
        let cmd = RedisCommand::new(
            "SET",
            vec![
                Bytes::from("key"),
                Bytes::from("value"),
                Bytes::from("XX"),
                Bytes::from("EX"),
                Bytes::from("60"),
            ],
        );
        let (options, get) = SetHandler::parse_options(&cmd).unwrap();
        assert!(!options.if_not_exists);
        assert!(options.if_exists);
        assert_eq!(options.ttl_ms, 60_000);
        assert!(!get);
    }

    #[test]
    fn test_set_parse_options_get() {
        let cmd = RedisCommand::new(
            "SET",
            vec![Bytes::from("key"), Bytes::from("value"), Bytes::from("GET")],
        );
        let (options, get) = SetHandler::parse_options(&cmd).unwrap();
        assert!(options.get_previous);
        assert!(get);
    }

    #[test]
    fn test_set_parse_options_conflict() {
        let cmd = RedisCommand::new(
            "SET",
            vec![
                Bytes::from("key"),
                Bytes::from("value"),
                Bytes::from("NX"),
                Bytes::from("XX"),
            ],
        );
        assert!(SetHandler::parse_options(&cmd).is_err());
    }
}
