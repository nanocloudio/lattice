//! Redis transaction command handlers.
//!
//! Implements MULTI, EXEC, DISCARD, WATCH, UNWATCH commands.

use super::{CommandContext, CommandHandler, CommandResult, CommandState};
use crate::adapters::redis::{RedisCommand, RedisError, RedisValue};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// MULTI command handler.
pub struct MultiHandler;

impl CommandHandler for MultiHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            if ctx.in_transaction {
                return Err(RedisError::new("ERR", "MULTI calls can not be nested"));
            }

            // The actual state change is handled by the connection handler
            // which will set in_transaction = true after this returns
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "MULTI"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }

    fn allowed_in_transaction(&self) -> bool {
        false // Can't nest MULTI
    }
}

/// EXEC command handler.
pub struct ExecHandler;

impl CommandHandler for ExecHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            if !ctx.in_transaction {
                return Err(RedisError::new("ERR", "EXEC without MULTI"));
            }

            // The actual execution is handled by the connection handler
            // which has access to the transaction queue and watched keys
            // This handler just validates the state

            // Return empty array to indicate the transaction context
            // The connection handler will replace this with actual results
            Ok(RedisValue::Array(vec![]))
        })
    }

    fn name(&self) -> &'static str {
        "EXEC"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }

    fn allowed_in_transaction(&self) -> bool {
        true // EXEC is special - allowed but ends transaction
    }

    fn requires_linearizable(&self) -> bool {
        true // Transactions require linearizability
    }
}

/// DISCARD command handler.
pub struct DiscardHandler;

impl CommandHandler for DiscardHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            if !ctx.in_transaction {
                return Err(RedisError::new("ERR", "DISCARD without MULTI"));
            }

            // The actual cleanup is handled by the connection handler
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "DISCARD"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }

    fn allowed_in_transaction(&self) -> bool {
        true // DISCARD is special - allowed but ends transaction
    }
}

/// WATCH command handler.
pub struct WatchHandler;

impl CommandHandler for WatchHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            if ctx.in_transaction {
                return Err(RedisError::new("ERR", "WATCH inside MULTI is not allowed"));
            }

            // The actual watch registration is handled by the connection handler
            // which needs to look up the current revision for each key
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "WATCH"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn requires_linearizable(&self) -> bool {
        true // WATCH needs linearizable reads
    }

    fn allowed_in_transaction(&self) -> bool {
        false // Must be before MULTI
    }
}

/// UNWATCH command handler.
pub struct UnwatchHandler;

impl CommandHandler for UnwatchHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Clear all watched keys
            // The actual cleanup is handled by the connection handler
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "UNWATCH"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }

    fn allowed_in_transaction(&self) -> bool {
        true // Can UNWATCH while in transaction (though it's a no-op since EXEC will clear)
    }
}

/// Transaction execution context.
#[derive(Debug, Default)]
pub struct TransactionContext {
    /// Whether there were syntax errors in queued commands.
    pub has_errors: bool,

    /// Error messages for commands with syntax errors.
    pub errors: Vec<Option<RedisError>>,

    /// Queued commands.
    pub commands: Vec<RedisCommand>,

    /// Watched keys with their revisions at watch time.
    pub watched: Vec<(bytes::Bytes, u64)>,
}

impl TransactionContext {
    /// Create a new transaction context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a command.
    pub fn queue(&mut self, cmd: RedisCommand) {
        self.commands.push(cmd);
        self.errors.push(None);
    }

    /// Queue a command that had a syntax error.
    pub fn queue_error(&mut self, error: RedisError) {
        self.has_errors = true;
        self.errors.push(Some(error));
    }

    /// Add a watched key.
    pub fn watch(&mut self, key: bytes::Bytes, revision: u64) {
        // Don't add duplicates
        if !self.watched.iter().any(|(k, _)| k == &key) {
            self.watched.push((key, revision));
        }
    }

    /// Clear all watches.
    pub fn unwatch_all(&mut self) {
        self.watched.clear();
    }

    /// Check if any watched keys have been modified.
    ///
    /// Returns true if all watches are valid (no modifications).
    pub fn check_watches<F>(&self, get_revision: F) -> bool
    where
        F: Fn(&bytes::Bytes) -> Option<u64>,
    {
        for (key, expected_rev) in &self.watched {
            match get_revision(key) {
                Some(current_rev) if current_rev != *expected_rev => {
                    // Key was modified
                    return false;
                }
                None if *expected_rev != 0 => {
                    // Key was deleted (had a revision, now doesn't exist)
                    return false;
                }
                _ => {
                    // Key matches expected state
                }
            }
        }
        true
    }

    /// Get the number of queued commands.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Clear the transaction.
    pub fn clear(&mut self) {
        self.has_errors = false;
        self.errors.clear();
        self.commands.clear();
        self.watched.clear();
    }
}

/// Transaction execution result.
#[derive(Debug)]
pub struct TransactionExecResult {
    /// Whether the transaction was aborted (WATCH conflict).
    pub aborted: bool,

    /// Results for each command (if not aborted).
    pub results: Vec<CommandResult>,

    /// Error message if aborted.
    pub error: Option<String>,
}

impl TransactionExecResult {
    /// Create a successful result.
    pub fn success(results: Vec<CommandResult>) -> Self {
        Self {
            aborted: false,
            results,
            error: None,
        }
    }

    /// Create an aborted result (WATCH conflict).
    pub fn watch_conflict() -> Self {
        Self {
            aborted: true,
            results: Vec::new(),
            error: Some("WATCH conflict".to_string()),
        }
    }

    /// Create an aborted result due to errors.
    pub fn exec_abort() -> Self {
        Self {
            aborted: true,
            results: Vec::new(),
            error: Some("EXECABORT Transaction discarded because of previous errors".to_string()),
        }
    }

    /// Convert to Redis response.
    pub fn to_redis_value(self) -> RedisValue {
        if self.aborted {
            RedisValue::Null
        } else {
            let results: Vec<RedisValue> = self
                .results
                .into_iter()
                .map(|r| match r {
                    Ok(v) => v,
                    Err(e) => RedisValue::Error(e),
                })
                .collect();
            RedisValue::Array(results)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_transaction_context_queue() {
        let mut ctx = TransactionContext::new();

        ctx.queue(RedisCommand::new("GET", vec![Bytes::from("key")]));
        assert_eq!(ctx.len(), 1);
        assert!(!ctx.has_errors);

        ctx.queue(RedisCommand::new(
            "SET",
            vec![Bytes::from("key"), Bytes::from("value")],
        ));
        assert_eq!(ctx.len(), 2);
    }

    #[test]
    fn test_transaction_context_error() {
        let mut ctx = TransactionContext::new();

        ctx.queue_error(RedisError::syntax());
        assert!(ctx.has_errors);
    }

    #[test]
    fn test_transaction_context_watch() {
        let mut ctx = TransactionContext::new();

        ctx.watch(Bytes::from("key1"), 100);
        ctx.watch(Bytes::from("key2"), 200);
        assert_eq!(ctx.watched.len(), 2);

        // No duplicates
        ctx.watch(Bytes::from("key1"), 150);
        assert_eq!(ctx.watched.len(), 2);
    }

    #[test]
    fn test_transaction_context_check_watches() {
        let mut ctx = TransactionContext::new();

        ctx.watch(Bytes::from("key1"), 100);
        ctx.watch(Bytes::from("key2"), 200);

        // All watches valid
        let get_rev = |key: &Bytes| -> Option<u64> {
            match key.as_ref() {
                b"key1" => Some(100),
                b"key2" => Some(200),
                _ => None,
            }
        };
        assert!(ctx.check_watches(get_rev));

        // key1 was modified
        let get_rev = |key: &Bytes| -> Option<u64> {
            match key.as_ref() {
                b"key1" => Some(101), // Changed
                b"key2" => Some(200),
                _ => None,
            }
        };
        assert!(!ctx.check_watches(get_rev));
    }

    #[test]
    fn test_transaction_context_clear() {
        let mut ctx = TransactionContext::new();

        ctx.queue(RedisCommand::new("GET", vec![Bytes::from("key")]));
        ctx.watch(Bytes::from("key"), 100);

        ctx.clear();

        assert!(ctx.is_empty());
        assert!(ctx.watched.is_empty());
        assert!(!ctx.has_errors);
    }

    #[test]
    fn test_exec_result_success() {
        let results = vec![Ok(RedisValue::ok()), Ok(RedisValue::Integer(42))];
        let exec_result = TransactionExecResult::success(results);

        assert!(!exec_result.aborted);
        let value = exec_result.to_redis_value();
        assert!(matches!(value, RedisValue::Array(arr) if arr.len() == 2));
    }

    #[test]
    fn test_exec_result_watch_conflict() {
        let exec_result = TransactionExecResult::watch_conflict();

        assert!(exec_result.aborted);
        let value = exec_result.to_redis_value();
        assert!(matches!(value, RedisValue::Null));
    }

    #[test]
    fn test_multi_handler_name() {
        let handler = MultiHandler;
        assert_eq!(handler.name(), "MULTI");
        assert_eq!(handler.max_args(), Some(0));
        assert!(!handler.allowed_in_transaction());
    }

    #[test]
    fn test_exec_handler_requires_linearizable() {
        let handler = ExecHandler;
        assert!(handler.requires_linearizable());
    }

    #[test]
    fn test_watch_handler_properties() {
        let handler = WatchHandler;
        assert_eq!(handler.name(), "WATCH");
        assert_eq!(handler.min_args(), 1);
        assert!(handler.requires_linearizable());
        assert!(!handler.allowed_in_transaction());
    }
}
