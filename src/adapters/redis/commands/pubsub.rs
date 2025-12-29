//! Redis Pub/Sub command handlers.
//!
//! Implements SUBSCRIBE, PUBLISH, PSUBSCRIBE and related Pub/Sub commands.

use super::{CommandContext, CommandHandler, CommandResult, CommandState};
use crate::adapters::redis::{RedisCommand, RedisError, RedisValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// SUBSCRIBE command handler.
pub struct SubscribeHandler;

impl CommandHandler for SubscribeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let mut results = Vec::new();

            for arg in &cmd.args {
                let channel = std::str::from_utf8(arg)
                    .map_err(|_| RedisError::new("ERR", "invalid channel name"))?;

                // Subscribe to channel
                let count = state.pubsub().subscribe(ctx.connection_id, channel);

                // Build subscription confirmation
                results.push(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("subscribe")),
                    RedisValue::BulkString(Bytes::from(channel.to_string())),
                    RedisValue::Integer(count as i64),
                ]));
            }

            // Return array of subscription confirmations
            // For RESP3, these would be push messages
            if results.len() == 1 {
                Ok(results.remove(0))
            } else {
                Ok(RedisValue::Array(results))
            }
        })
    }

    fn name(&self) -> &'static str {
        "SUBSCRIBE"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn requires_auth(&self) -> bool {
        true
    }

    fn allowed_in_pubsub(&self) -> bool {
        true
    }

    fn allowed_in_transaction(&self) -> bool {
        false // Can't subscribe in a transaction
    }
}

/// PSUBSCRIBE command handler.
pub struct PSubscribeHandler;

impl CommandHandler for PSubscribeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let mut results = Vec::new();

            for arg in &cmd.args {
                let pattern = std::str::from_utf8(arg)
                    .map_err(|_| RedisError::new("ERR", "invalid pattern"))?;

                // Subscribe to pattern
                let count = state.pubsub().psubscribe(ctx.connection_id, pattern);

                // Build subscription confirmation
                results.push(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("psubscribe")),
                    RedisValue::BulkString(Bytes::from(pattern.to_string())),
                    RedisValue::Integer(count as i64),
                ]));
            }

            if results.len() == 1 {
                Ok(results.remove(0))
            } else {
                Ok(RedisValue::Array(results))
            }
        })
    }

    fn name(&self) -> &'static str {
        "PSUBSCRIBE"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn requires_auth(&self) -> bool {
        true
    }

    fn allowed_in_pubsub(&self) -> bool {
        true
    }

    fn allowed_in_transaction(&self) -> bool {
        false
    }
}

/// UNSUBSCRIBE command handler.
pub struct UnsubscribeHandler;

impl CommandHandler for UnsubscribeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let mut results = Vec::new();

            if cmd.args.is_empty() {
                // Unsubscribe from all channels
                let channels = state.pubsub().unsubscribe_all(ctx.connection_id);

                for channel in channels {
                    let (chan_count, pat_count) =
                        state.pubsub().subscription_count(ctx.connection_id);
                    let total = chan_count + pat_count;

                    results.push(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("unsubscribe")),
                        RedisValue::BulkString(Bytes::from(channel)),
                        RedisValue::Integer(total as i64),
                    ]));
                }

                if results.is_empty() {
                    // No subscriptions to unsubscribe from
                    results.push(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("unsubscribe")),
                        RedisValue::Null,
                        RedisValue::Integer(0),
                    ]));
                }
            } else {
                // Unsubscribe from specific channels
                for arg in &cmd.args {
                    let channel = std::str::from_utf8(arg)
                        .map_err(|_| RedisError::new("ERR", "invalid channel name"))?;

                    state.pubsub().unsubscribe(ctx.connection_id, channel);
                    let (chan_count, pat_count) =
                        state.pubsub().subscription_count(ctx.connection_id);
                    let total = chan_count + pat_count;

                    results.push(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("unsubscribe")),
                        RedisValue::BulkString(Bytes::from(channel.to_string())),
                        RedisValue::Integer(total as i64),
                    ]));
                }
            }

            if results.len() == 1 {
                Ok(results.remove(0))
            } else {
                Ok(RedisValue::Array(results))
            }
        })
    }

    fn name(&self) -> &'static str {
        "UNSUBSCRIBE"
    }

    fn requires_auth(&self) -> bool {
        true
    }

    fn allowed_in_pubsub(&self) -> bool {
        true
    }

    fn allowed_in_transaction(&self) -> bool {
        false
    }
}

/// PUNSUBSCRIBE command handler.
pub struct PUnsubscribeHandler;

impl CommandHandler for PUnsubscribeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let mut results = Vec::new();

            if cmd.args.is_empty() {
                // Unsubscribe from all patterns
                let patterns = state.pubsub().punsubscribe_all(ctx.connection_id);

                for pattern in patterns {
                    let (chan_count, pat_count) =
                        state.pubsub().subscription_count(ctx.connection_id);
                    let total = chan_count + pat_count;

                    results.push(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("punsubscribe")),
                        RedisValue::BulkString(Bytes::from(pattern)),
                        RedisValue::Integer(total as i64),
                    ]));
                }

                if results.is_empty() {
                    results.push(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("punsubscribe")),
                        RedisValue::Null,
                        RedisValue::Integer(0),
                    ]));
                }
            } else {
                // Unsubscribe from specific patterns
                for arg in &cmd.args {
                    let pattern = std::str::from_utf8(arg)
                        .map_err(|_| RedisError::new("ERR", "invalid pattern"))?;

                    state.pubsub().punsubscribe(ctx.connection_id, pattern);
                    let (chan_count, pat_count) =
                        state.pubsub().subscription_count(ctx.connection_id);
                    let total = chan_count + pat_count;

                    results.push(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("punsubscribe")),
                        RedisValue::BulkString(Bytes::from(pattern.to_string())),
                        RedisValue::Integer(total as i64),
                    ]));
                }
            }

            if results.len() == 1 {
                Ok(results.remove(0))
            } else {
                Ok(RedisValue::Array(results))
            }
        })
    }

    fn name(&self) -> &'static str {
        "PUNSUBSCRIBE"
    }

    fn requires_auth(&self) -> bool {
        true
    }

    fn allowed_in_pubsub(&self) -> bool {
        true
    }

    fn allowed_in_transaction(&self) -> bool {
        false
    }
}

/// PUBLISH command handler.
pub struct PublishHandler;

impl CommandHandler for PublishHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let channel = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("PUBLISH"))?;
            let message = cmd
                .arg(1)
                .ok_or_else(|| RedisError::wrong_arity("PUBLISH"))?;

            // Publish message and get subscriber count
            let count = state.pubsub().publish(channel, message);

            Ok(RedisValue::Integer(count as i64))
        })
    }

    fn name(&self) -> &'static str {
        "PUBLISH"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }

    fn requires_auth(&self) -> bool {
        true
    }

    fn allowed_in_pubsub(&self) -> bool {
        // In Redis, PUBLISH is not allowed in pub/sub mode
        // but we could allow it
        false
    }
}

/// PUBSUB command handler.
pub struct PubSubHandler;

impl CommandHandler for PubSubHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("PUBSUB"))?
                .to_uppercase();

            match subcommand.as_str() {
                "CHANNELS" => {
                    let pattern = cmd.arg_str(1);
                    let channels = state.pubsub().channels(pattern);

                    let result: Vec<RedisValue> = channels
                        .into_iter()
                        .map(|c| RedisValue::BulkString(Bytes::from(c)))
                        .collect();

                    Ok(RedisValue::Array(result))
                }
                "NUMSUB" => {
                    let channels: Vec<&str> = cmd.args[1..]
                        .iter()
                        .filter_map(|b| std::str::from_utf8(b).ok())
                        .collect();

                    let counts = state.pubsub().numsub(&channels);

                    let mut result = Vec::new();
                    for (channel, count) in counts {
                        result.push(RedisValue::BulkString(Bytes::from(channel)));
                        result.push(RedisValue::Integer(count as i64));
                    }

                    Ok(RedisValue::Array(result))
                }
                "NUMPAT" => {
                    let count = state.pubsub().numpat();
                    Ok(RedisValue::Integer(count as i64))
                }
                "SHARDCHANNELS" => {
                    // Sharded pub/sub - not supported, return empty
                    Ok(RedisValue::Array(vec![]))
                }
                "SHARDNUMSUB" => {
                    // Sharded pub/sub - not supported
                    Ok(RedisValue::Array(vec![]))
                }
                "HELP" => Ok(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("PUBSUB CHANNELS [<pattern>]")),
                    RedisValue::BulkString(Bytes::from("PUBSUB NUMSUB [<channel> ...]")),
                    RedisValue::BulkString(Bytes::from("PUBSUB NUMPAT")),
                ])),
                _ => Err(RedisError::new(
                    "ERR",
                    format!(
                        "Unknown PUBSUB subcommand '{}'. Try PUBSUB HELP.",
                        subcommand
                    ),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "PUBSUB"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn requires_auth(&self) -> bool {
        true
    }
}

/// SSUBSCRIBE command handler (sharded pub/sub, Redis 7.0+).
pub struct SSubscribeHandler;

impl CommandHandler for SSubscribeHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Sharded pub/sub is not supported
            // In Redis Cluster, sharded channels are routed to specific nodes
            // For Lattice, we'd need to implement cross-KPG routing
            Err(RedisError::new("ERR", "Sharded pub/sub is not supported"))
        })
    }

    fn name(&self) -> &'static str {
        "SSUBSCRIBE"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// SUNSUBSCRIBE command handler (sharded pub/sub, Redis 7.0+).
pub struct SUnsubscribeHandler;

impl CommandHandler for SUnsubscribeHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move { Err(RedisError::new("ERR", "Sharded pub/sub is not supported")) })
    }

    fn name(&self) -> &'static str {
        "SUNSUBSCRIBE"
    }
}

/// SPUBLISH command handler (sharded pub/sub, Redis 7.0+).
pub struct SPublishHandler;

impl CommandHandler for SPublishHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move { Err(RedisError::new("ERR", "Sharded pub/sub is not supported")) })
    }

    fn name(&self) -> &'static str {
        "SPUBLISH"
    }

    fn min_args(&self) -> usize {
        2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_handler_properties() {
        let handler = SubscribeHandler;
        assert_eq!(handler.name(), "SUBSCRIBE");
        assert_eq!(handler.min_args(), 1);
        assert!(handler.allowed_in_pubsub());
        assert!(!handler.allowed_in_transaction());
    }

    #[test]
    fn test_psubscribe_handler_properties() {
        let handler = PSubscribeHandler;
        assert_eq!(handler.name(), "PSUBSCRIBE");
        assert!(handler.allowed_in_pubsub());
    }

    #[test]
    fn test_publish_handler_properties() {
        let handler = PublishHandler;
        assert_eq!(handler.name(), "PUBLISH");
        assert_eq!(handler.min_args(), 2);
        assert_eq!(handler.max_args(), Some(2));
    }

    #[test]
    fn test_unsubscribe_handler_properties() {
        let handler = UnsubscribeHandler;
        assert_eq!(handler.name(), "UNSUBSCRIBE");
        assert!(handler.allowed_in_pubsub());
    }

    #[test]
    fn test_pubsub_handler_properties() {
        let handler = PubSubHandler;
        assert_eq!(handler.name(), "PUBSUB");
        assert_eq!(handler.min_args(), 1);
    }
}
