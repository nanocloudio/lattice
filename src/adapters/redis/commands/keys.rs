//! Redis key command handlers.
//!
//! Implements DEL, EXISTS, KEYS, SCAN, EXPIRE, TTL, and related key commands.

use super::{CommandContext, CommandHandler, CommandResult, CommandState, PutOptions};
use crate::adapters::redis::{RedisCommand, RedisError, RedisValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// DEL command handler.
pub struct DelHandler;

impl CommandHandler for DelHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let keys: Vec<&[u8]> = cmd.args.iter().map(|b| b.as_ref()).collect();
            let count = state
                .kv()
                .delete_many(&ctx.request.tenant_id, &keys)
                .await?;
            Ok(RedisValue::Integer(count as i64))
        })
    }

    fn name(&self) -> &'static str {
        "DEL"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// UNLINK command handler (async delete, same as DEL in Lattice).
pub struct UnlinkHandler;

impl CommandHandler for UnlinkHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let keys: Vec<&[u8]> = cmd.args.iter().map(|b| b.as_ref()).collect();
            let count = state
                .kv()
                .delete_many(&ctx.request.tenant_id, &keys)
                .await?;
            Ok(RedisValue::Integer(count as i64))
        })
    }

    fn name(&self) -> &'static str {
        "UNLINK"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// EXISTS command handler.
pub struct ExistsHandler;

impl CommandHandler for ExistsHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let keys: Vec<&[u8]> = cmd.args.iter().map(|b| b.as_ref()).collect();
            let count = state
                .kv()
                .exists_many(&ctx.request.tenant_id, &keys)
                .await?;
            Ok(RedisValue::Integer(count as i64))
        })
    }

    fn name(&self) -> &'static str {
        "EXISTS"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// TYPE command handler.
pub struct TypeHandler;

impl CommandHandler for TypeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("TYPE"))?;

            match state.kv().exists(&ctx.request.tenant_id, key).await? {
                true => Ok(RedisValue::SimpleString("string".to_string())),
                false => Ok(RedisValue::SimpleString("none".to_string())),
            }
        })
    }

    fn name(&self) -> &'static str {
        "TYPE"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// KEYS command handler.
pub struct KeysHandler;

impl CommandHandler for KeysHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let pattern = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("KEYS"))?;

            // KEYS is a full scan - potentially expensive
            let result = state
                .kv()
                .scan(&ctx.request.tenant_id, 0, Some(pattern), 10000)
                .await?;

            let keys: Vec<RedisValue> = result
                .keys
                .into_iter()
                .map(RedisValue::BulkString)
                .collect();

            Ok(RedisValue::Array(keys))
        })
    }

    fn name(&self) -> &'static str {
        "KEYS"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// SCAN command handler.
pub struct ScanHandler;

impl CommandHandler for ScanHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let cursor = cmd
                .arg_int(0)
                .ok_or_else(|| RedisError::new("ERR", "invalid cursor"))?;

            if cursor < 0 {
                return Err(RedisError::new("ERR", "invalid cursor"));
            }

            let mut pattern: Option<&str> = None;
            let mut count: usize = 10;

            // Parse options
            let mut i = 1;
            while i < cmd.argc() {
                let opt = cmd
                    .arg_str(i)
                    .ok_or_else(RedisError::syntax)?
                    .to_uppercase();

                match opt.as_str() {
                    "MATCH" => {
                        i += 1;
                        pattern = Some(cmd.arg_str(i).ok_or_else(RedisError::syntax)?);
                    }
                    "COUNT" => {
                        i += 1;
                        count = cmd.arg_int(i).ok_or_else(|| {
                            RedisError::new("ERR", "value is not an integer or out of range")
                        })? as usize;
                    }
                    "TYPE" => {
                        // TYPE filter - we only have strings, so ignore
                        i += 1;
                        let _ = cmd.arg_str(i);
                    }
                    _ => {
                        return Err(RedisError::syntax());
                    }
                }
                i += 1;
            }

            let result = state
                .kv()
                .scan(&ctx.request.tenant_id, cursor as u64, pattern, count)
                .await?;

            let keys: Vec<RedisValue> = result
                .keys
                .into_iter()
                .map(RedisValue::BulkString)
                .collect();

            Ok(RedisValue::Array(vec![
                RedisValue::BulkString(Bytes::from(result.cursor.to_string())),
                RedisValue::Array(keys),
            ]))
        })
    }

    fn name(&self) -> &'static str {
        "SCAN"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// EXPIRE command handler.
pub struct ExpireHandler;

impl CommandHandler for ExpireHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("EXPIRE"))?;
            let seconds = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;

            // Check for NX/XX/GT/LT options (Redis 7.0+)
            let mut nx = false;
            let mut xx = false;
            let mut gt = false;
            let mut lt = false;

            for i in 2..cmd.argc() {
                let opt = cmd
                    .arg_str(i)
                    .ok_or_else(RedisError::syntax)?
                    .to_uppercase();
                match opt.as_str() {
                    "NX" => nx = true,
                    "XX" => xx = true,
                    "GT" => gt = true,
                    "LT" => lt = true,
                    _ => return Err(RedisError::syntax()),
                }
            }

            // Get existing entry
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            if existing.is_none() {
                return Ok(RedisValue::Integer(0));
            }

            let entry = existing.unwrap();
            let current_ttl = entry.expiry_at;

            // Apply NX/XX/GT/LT conditions
            if nx && current_ttl > 0 {
                return Ok(RedisValue::Integer(0)); // NX: only if no expiry
            }
            if xx && current_ttl == 0 {
                return Ok(RedisValue::Integer(0)); // XX: only if has expiry
            }

            let new_ttl_ms = if seconds <= 0 {
                0 // Delete immediately
            } else {
                (seconds as u64) * 1000
            };

            // GT/LT conditions (comparing absolute times would need current tick)
            if gt || lt {
                // Simplified: just check if new > old or new < old
                let current_ms = current_ttl;
                if gt && new_ttl_ms <= current_ms {
                    return Ok(RedisValue::Integer(0));
                }
                if lt && new_ttl_ms >= current_ms && current_ms > 0 {
                    return Ok(RedisValue::Integer(0));
                }
            }

            let options = PutOptions {
                ttl_ms: new_ttl_ms,
                ..Default::default()
            };

            state
                .kv()
                .put(&ctx.request.tenant_id, key, &entry.value, options)
                .await?;
            Ok(RedisValue::Integer(1))
        })
    }

    fn name(&self) -> &'static str {
        "EXPIRE"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// PEXPIRE command handler.
pub struct PExpireHandler;

impl CommandHandler for PExpireHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("PEXPIRE"))?;
            let milliseconds = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;

            // Get existing entry
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            if existing.is_none() {
                return Ok(RedisValue::Integer(0));
            }

            let entry = existing.unwrap();

            let new_ttl_ms = if milliseconds <= 0 {
                0
            } else {
                milliseconds as u64
            };

            let options = PutOptions {
                ttl_ms: new_ttl_ms,
                ..Default::default()
            };

            state
                .kv()
                .put(&ctx.request.tenant_id, key, &entry.value, options)
                .await?;
            Ok(RedisValue::Integer(1))
        })
    }

    fn name(&self) -> &'static str {
        "PEXPIRE"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// EXPIREAT command handler.
pub struct ExpireAtHandler;

impl CommandHandler for ExpireAtHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("EXPIREAT"))?;
            let unix_time = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;

            // Get existing entry
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            if existing.is_none() {
                return Ok(RedisValue::Integer(0));
            }

            let entry = existing.unwrap();

            // Convert unix timestamp to TTL (would need current time)
            // For now, store as absolute milliseconds
            let ttl_ms = if unix_time <= 0 {
                0
            } else {
                (unix_time as u64) * 1000
            };

            let options = PutOptions {
                ttl_ms,
                ..Default::default()
            };

            state
                .kv()
                .put(&ctx.request.tenant_id, key, &entry.value, options)
                .await?;
            Ok(RedisValue::Integer(1))
        })
    }

    fn name(&self) -> &'static str {
        "EXPIREAT"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// PEXPIREAT command handler.
pub struct PExpireAtHandler;

impl CommandHandler for PExpireAtHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("PEXPIREAT"))?;
            let unix_time_ms = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;

            // Get existing entry
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            if existing.is_none() {
                return Ok(RedisValue::Integer(0));
            }

            let entry = existing.unwrap();

            let ttl_ms = if unix_time_ms <= 0 {
                0
            } else {
                unix_time_ms as u64
            };

            let options = PutOptions {
                ttl_ms,
                ..Default::default()
            };

            state
                .kv()
                .put(&ctx.request.tenant_id, key, &entry.value, options)
                .await?;
            Ok(RedisValue::Integer(1))
        })
    }

    fn name(&self) -> &'static str {
        "PEXPIREAT"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// TTL command handler.
pub struct TtlHandler;

impl CommandHandler for TtlHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("TTL"))?;

            match state.kv().get(&ctx.request.tenant_id, key).await? {
                None => Ok(RedisValue::Integer(-2)), // Key doesn't exist
                Some(entry) => {
                    if entry.expiry_at == 0 {
                        Ok(RedisValue::Integer(-1)) // No TTL
                    } else {
                        // Calculate remaining TTL
                        let current_tick = state.kv().current_tick();
                        if entry.expiry_at <= current_tick {
                            Ok(RedisValue::Integer(-2)) // Expired
                        } else {
                            let remaining_ms = entry.expiry_at - current_tick;
                            Ok(RedisValue::Integer((remaining_ms / 1000) as i64))
                        }
                    }
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "TTL"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// PTTL command handler.
pub struct PTtlHandler;

impl CommandHandler for PTtlHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("PTTL"))?;

            match state.kv().get(&ctx.request.tenant_id, key).await? {
                None => Ok(RedisValue::Integer(-2)), // Key doesn't exist
                Some(entry) => {
                    if entry.expiry_at == 0 {
                        Ok(RedisValue::Integer(-1)) // No TTL
                    } else {
                        let current_tick = state.kv().current_tick();
                        if entry.expiry_at <= current_tick {
                            Ok(RedisValue::Integer(-2)) // Expired
                        } else {
                            let remaining_ms = entry.expiry_at - current_tick;
                            Ok(RedisValue::Integer(remaining_ms as i64))
                        }
                    }
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "PTTL"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// PERSIST command handler.
pub struct PersistHandler;

impl CommandHandler for PersistHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("PERSIST"))?;

            // Get existing entry
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            match existing {
                None => Ok(RedisValue::Integer(0)), // Key doesn't exist
                Some(entry) => {
                    if entry.expiry_at == 0 {
                        Ok(RedisValue::Integer(0)) // No TTL to remove
                    } else {
                        let options = PutOptions {
                            ttl_ms: 0, // Remove TTL
                            ..Default::default()
                        };
                        state
                            .kv()
                            .put(&ctx.request.tenant_id, key, &entry.value, options)
                            .await?;
                        Ok(RedisValue::Integer(1))
                    }
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "PERSIST"
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

/// EXPIRETIME command handler (Redis 7.0+).
pub struct ExpireTimeHandler;

impl CommandHandler for ExpireTimeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("EXPIRETIME"))?;

            match state.kv().get(&ctx.request.tenant_id, key).await? {
                None => Ok(RedisValue::Integer(-2)),
                Some(entry) => {
                    if entry.expiry_at == 0 {
                        Ok(RedisValue::Integer(-1))
                    } else {
                        // Return absolute unix timestamp in seconds
                        Ok(RedisValue::Integer((entry.expiry_at / 1000) as i64))
                    }
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "EXPIRETIME"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// PEXPIRETIME command handler (Redis 7.0+).
pub struct PExpireTimeHandler;

impl CommandHandler for PExpireTimeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("PEXPIRETIME"))?;

            match state.kv().get(&ctx.request.tenant_id, key).await? {
                None => Ok(RedisValue::Integer(-2)),
                Some(entry) => {
                    if entry.expiry_at == 0 {
                        Ok(RedisValue::Integer(-1))
                    } else {
                        Ok(RedisValue::Integer(entry.expiry_at as i64))
                    }
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "PEXPIRETIME"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// RENAME command handler.
pub struct RenameHandler;

impl CommandHandler for RenameHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("RENAME"))?;
            let newkey = cmd
                .arg(1)
                .ok_or_else(|| RedisError::wrong_arity("RENAME"))?;

            // Get existing entry
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            match existing {
                None => Err(RedisError::new("ERR", "no such key")),
                Some(entry) => {
                    // If same key, do nothing
                    if key == newkey {
                        return Ok(RedisValue::ok());
                    }

                    // Delete old key
                    state.kv().delete(&ctx.request.tenant_id, key).await?;

                    // Create new key with same value and TTL
                    let options = PutOptions {
                        ttl_ms: entry.expiry_at,
                        ..Default::default()
                    };
                    state
                        .kv()
                        .put(&ctx.request.tenant_id, newkey, &entry.value, options)
                        .await?;

                    Ok(RedisValue::ok())
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "RENAME"
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

/// RENAMENX command handler.
pub struct RenameNxHandler;

impl CommandHandler for RenameNxHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("RENAMENX"))?;
            let newkey = cmd
                .arg(1)
                .ok_or_else(|| RedisError::wrong_arity("RENAMENX"))?;

            // Get existing entry
            let existing = state.kv().get(&ctx.request.tenant_id, key).await?;

            match existing {
                None => Err(RedisError::new("ERR", "no such key")),
                Some(entry) => {
                    // If same key, return 0
                    if key == newkey {
                        return Ok(RedisValue::Integer(0));
                    }

                    // Check if newkey exists
                    if state.kv().exists(&ctx.request.tenant_id, newkey).await? {
                        return Ok(RedisValue::Integer(0));
                    }

                    // Delete old key
                    state.kv().delete(&ctx.request.tenant_id, key).await?;

                    // Create new key
                    let options = PutOptions {
                        ttl_ms: entry.expiry_at,
                        ..Default::default()
                    };
                    state
                        .kv()
                        .put(&ctx.request.tenant_id, newkey, &entry.value, options)
                        .await?;

                    Ok(RedisValue::Integer(1))
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "RENAMENX"
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
        true // Conditional operation
    }
}

/// COPY command handler.
pub struct CopyHandler;

impl CommandHandler for CopyHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let source = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("COPY"))?;
            let destination = cmd.arg(1).ok_or_else(|| RedisError::wrong_arity("COPY"))?;

            let mut replace = false;
            // let mut db = 0; // DB option is ignored

            // Parse options
            let mut i = 2;
            while i < cmd.argc() {
                let opt = cmd
                    .arg_str(i)
                    .ok_or_else(RedisError::syntax)?
                    .to_uppercase();

                match opt.as_str() {
                    "REPLACE" => {
                        replace = true;
                    }
                    "DB" => {
                        i += 1;
                        let _ = cmd.arg_int(i); // Ignore DB option
                    }
                    _ => {
                        return Err(RedisError::syntax());
                    }
                }
                i += 1;
            }

            // Get source entry
            let existing = state.kv().get(&ctx.request.tenant_id, source).await?;

            match existing {
                None => Ok(RedisValue::Integer(0)), // Source doesn't exist
                Some(entry) => {
                    // Check if destination exists
                    if !replace
                        && state
                            .kv()
                            .exists(&ctx.request.tenant_id, destination)
                            .await?
                    {
                        return Ok(RedisValue::Integer(0));
                    }

                    // Copy value
                    let options = PutOptions {
                        ttl_ms: entry.expiry_at,
                        ..Default::default()
                    };
                    state
                        .kv()
                        .put(&ctx.request.tenant_id, destination, &entry.value, options)
                        .await?;

                    Ok(RedisValue::Integer(1))
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "COPY"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn is_write(&self) -> bool {
        true
    }
}

/// TOUCH command handler.
pub struct TouchHandler;

impl CommandHandler for TouchHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // TOUCH updates last access time - Lattice doesn't track LRU,
            // so just return count of existing keys
            let keys: Vec<&[u8]> = cmd.args.iter().map(|b| b.as_ref()).collect();
            let count = state
                .kv()
                .exists_many(&ctx.request.tenant_id, &keys)
                .await?;
            Ok(RedisValue::Integer(count as i64))
        })
    }

    fn name(&self) -> &'static str {
        "TOUCH"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// OBJECT command handler.
pub struct ObjectHandler;

impl CommandHandler for ObjectHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("OBJECT"))?
                .to_uppercase();

            match subcommand.as_str() {
                "ENCODING" => {
                    let key = cmd
                        .arg(1)
                        .ok_or_else(|| RedisError::wrong_arity("OBJECT"))?;
                    match state.kv().exists(&ctx.request.tenant_id, key).await? {
                        true => Ok(RedisValue::BulkString(Bytes::from("raw"))),
                        false => Ok(RedisValue::Null),
                    }
                }
                "REFCOUNT" => {
                    let key = cmd
                        .arg(1)
                        .ok_or_else(|| RedisError::wrong_arity("OBJECT"))?;
                    match state.kv().exists(&ctx.request.tenant_id, key).await? {
                        true => Ok(RedisValue::Integer(1)),
                        false => Ok(RedisValue::Null),
                    }
                }
                "IDLETIME" | "FREQ" => {
                    // Not supported - Lattice doesn't track LRU/LFU
                    Ok(RedisValue::Null)
                }
                "HELP" => Ok(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("OBJECT ENCODING <key>")),
                    RedisValue::BulkString(Bytes::from("OBJECT REFCOUNT <key>")),
                    RedisValue::BulkString(Bytes::from("OBJECT IDLETIME <key> (not supported)")),
                    RedisValue::BulkString(Bytes::from("OBJECT FREQ <key> (not supported)")),
                ])),
                _ => Err(RedisError::new(
                    "ERR",
                    format!("unknown subcommand '{}'. Try OBJECT HELP.", subcommand),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "OBJECT"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// DUMP command handler.
pub struct DumpHandler;

impl CommandHandler for DumpHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("DUMP"))?;

            match state.kv().get(&ctx.request.tenant_id, key).await? {
                None => Ok(RedisValue::Null),
                Some(entry) => {
                    // Return a simple serialization format
                    // In real Redis this is RDB format, we use a simplified version
                    let mut serialized = Vec::new();
                    serialized.push(0); // Version byte
                    serialized.push(0); // Type: string
                    serialized.extend_from_slice(&(entry.value.len() as u32).to_le_bytes());
                    serialized.extend_from_slice(&entry.value);
                    serialized.extend_from_slice(&entry.expiry_at.to_le_bytes());

                    Ok(RedisValue::BulkString(Bytes::from(serialized)))
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "DUMP"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// RESTORE command handler.
pub struct RestoreHandler;

impl CommandHandler for RestoreHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let key = cmd
                .arg(0)
                .ok_or_else(|| RedisError::wrong_arity("RESTORE"))?;
            let ttl = cmd
                .arg_int(1)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;
            let serialized = cmd
                .arg(2)
                .ok_or_else(|| RedisError::wrong_arity("RESTORE"))?;

            let mut replace = false;

            // Parse options
            for i in 3..cmd.argc() {
                let opt = cmd
                    .arg_str(i)
                    .ok_or_else(RedisError::syntax)?
                    .to_uppercase();
                match opt.as_str() {
                    "REPLACE" => replace = true,
                    "ABSTTL" | "IDLETIME" | "FREQ" => {
                        // ABSTTL, IDLETIME, FREQ are accepted but not used
                    }
                    _ => {}
                }
            }

            // Check if key exists
            if !replace && state.kv().exists(&ctx.request.tenant_id, key).await? {
                return Err(RedisError::new("BUSYKEY", "Target key name already exists"));
            }

            // Parse serialized value (our simple format)
            if serialized.len() < 6 {
                return Err(RedisError::new(
                    "ERR",
                    "DUMP payload version or checksum are wrong",
                ));
            }

            let _version = serialized[0];
            let _value_type = serialized[1];
            let value_len =
                u32::from_le_bytes([serialized[2], serialized[3], serialized[4], serialized[5]])
                    as usize;

            if serialized.len() < 6 + value_len {
                return Err(RedisError::new(
                    "ERR",
                    "DUMP payload version or checksum are wrong",
                ));
            }

            let value = &serialized[6..6 + value_len];

            // TTL is in milliseconds; if 0 or negative, no expiration
            let ttl_ms = if ttl > 0 { ttl as u64 } else { 0 };

            let options = PutOptions {
                ttl_ms,
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
        "RESTORE"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn is_write(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_del_handler_name() {
        let handler = DelHandler;
        assert_eq!(handler.name(), "DEL");
        assert_eq!(handler.min_args(), 1);
        assert!(handler.is_write());
    }

    #[test]
    fn test_exists_handler_name() {
        let handler = ExistsHandler;
        assert_eq!(handler.name(), "EXISTS");
        assert_eq!(handler.min_args(), 1);
        assert!(!handler.is_write());
    }

    #[test]
    fn test_ttl_handler_name() {
        let handler = TtlHandler;
        assert_eq!(handler.name(), "TTL");
        assert_eq!(handler.min_args(), 1);
        assert_eq!(handler.max_args(), Some(1));
    }

    #[test]
    fn test_rename_handler_name() {
        let handler = RenameHandler;
        assert_eq!(handler.name(), "RENAME");
        assert_eq!(handler.min_args(), 2);
        assert!(handler.is_write());
    }
}
