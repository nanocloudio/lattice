//! Redis connection command handlers.
//!
//! Implements AUTH, CLIENT, HELLO, RESET and related connection commands.

use super::{CommandContext, CommandHandler, CommandResult, CommandState};
use crate::adapters::redis::{ProtocolVersion, RedisCommand, RedisError, RedisValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// AUTH command handler.
pub struct AuthHandler;

impl CommandHandler for AuthHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        // Extract data before async block to avoid lifetime issues
        let argc = cmd.argc();
        let arg0 = cmd.arg_str(0).map(|s| s.to_string());
        let arg1 = cmd.arg_str(1).map(|s| s.to_string());

        Box::pin(async move {
            // AUTH [username] password
            let (_username, _password): (Option<String>, String) = if argc == 1 {
                // Legacy: just password
                (None, arg0.ok_or_else(|| RedisError::wrong_arity("AUTH"))?)
            } else if argc == 2 {
                // ACL style: username and password
                let user = arg0.ok_or_else(|| RedisError::wrong_arity("AUTH"))?;
                let pass = arg1.ok_or_else(|| RedisError::wrong_arity("AUTH"))?;
                (Some(user), pass)
            } else {
                return Err(RedisError::wrong_arity("AUTH"));
            };

            // In Lattice, username maps to tenant_id
            // Password is validated against tenant credentials
            // This would integrate with the auth system

            // For now, accept any auth
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "AUTH"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(2)
    }

    fn requires_auth(&self) -> bool {
        false // AUTH itself doesn't require auth
    }
}

/// CLIENT command handler.
pub struct ClientHandler;

impl CommandHandler for ClientHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        // Extract data before async block to avoid lifetime issues
        let subcommand = cmd.arg_str(0).map(|s| s.to_uppercase());
        let arg1 = cmd.arg_str(1).map(|s| s.to_string());
        let connection_id = ctx.connection_id;
        let client_name = ctx.client_name.clone();

        Box::pin(async move {
            let subcommand = subcommand.ok_or_else(|| RedisError::wrong_arity("CLIENT"))?;

            match subcommand.as_str() {
                "SETNAME" => {
                    let name = arg1.ok_or_else(|| RedisError::wrong_arity("CLIENT"))?;

                    // Validate name (no spaces allowed)
                    if name.contains(' ') {
                        return Err(RedisError::new(
                            "ERR",
                            "Client names cannot contain spaces, newlines or special characters.",
                        ));
                    }

                    // Name is stored in connection state (handled by caller)
                    // Return OK to indicate we accept the name
                    Ok(RedisValue::ok())
                }
                "GETNAME" => match &client_name {
                    Some(name) => Ok(RedisValue::BulkString(Bytes::from(name.clone()))),
                    None => Ok(RedisValue::Null),
                },
                "ID" => Ok(RedisValue::Integer(connection_id as i64)),
                "LIST" => {
                    // List all connections - would need access to connection manager
                    // For now, just return info about current connection
                    let info = format!(
                        "id={} addr=127.0.0.1:0 fd=0 name={} age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\r\n",
                        connection_id,
                        client_name.as_deref().unwrap_or("")
                    );
                    Ok(RedisValue::BulkString(Bytes::from(info)))
                }
                "INFO" => {
                    // Info about current connection
                    let info = format!(
                        "id={}\r\naddr=127.0.0.1:0\r\nname={}\r\nage=0\r\nidle=0\r\nflags=N\r\ndb=0\r\nsub=0\r\npsub=0\r\nmulti=-1\r\n",
                        connection_id,
                        client_name.as_deref().unwrap_or("")
                    );
                    Ok(RedisValue::BulkString(Bytes::from(info)))
                }
                "KILL" => {
                    // Kill a client connection
                    // Would need access to connection manager
                    // For now, reject
                    Err(RedisError::new(
                        "ERR",
                        "CLIENT KILL requires admin privileges",
                    ))
                }
                "PAUSE" => {
                    // Pause client processing
                    // Would affect the connection handler
                    Ok(RedisValue::ok())
                }
                "UNPAUSE" => Ok(RedisValue::ok()),
                "REPLY" => {
                    let mode = arg1.ok_or_else(|| RedisError::wrong_arity("CLIENT"))?;
                    let mode_upper = mode.to_uppercase();

                    match mode_upper.as_str() {
                        "ON" | "OFF" | "SKIP" => Ok(RedisValue::ok()),
                        _ => Err(RedisError::syntax()),
                    }
                }
                "TRACKINGINFO" => {
                    // Client-side caching tracking info
                    Ok(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("flags")),
                        RedisValue::BulkString(Bytes::from("off")),
                        RedisValue::BulkString(Bytes::from("redirect")),
                        RedisValue::Integer(-1),
                        RedisValue::BulkString(Bytes::from("prefixes")),
                        RedisValue::Array(vec![]),
                    ]))
                }
                "GETREDIR" => {
                    // Get tracking redirection client ID
                    Ok(RedisValue::Integer(-1))
                }
                "CACHING" => {
                    // Enable/disable client-side caching
                    let mode = arg1.ok_or_else(|| RedisError::wrong_arity("CLIENT"))?;
                    let mode_upper = mode.to_uppercase();

                    match mode_upper.as_str() {
                        "YES" | "NO" => Ok(RedisValue::ok()),
                        _ => Err(RedisError::syntax()),
                    }
                }
                "NO-EVICT" => {
                    // Set no-evict mode for client
                    let mode = arg1.ok_or_else(|| RedisError::wrong_arity("CLIENT"))?;
                    let mode_upper = mode.to_uppercase();

                    match mode_upper.as_str() {
                        "ON" | "OFF" => Ok(RedisValue::ok()),
                        _ => Err(RedisError::syntax()),
                    }
                }
                "HELP" => Ok(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("CLIENT SETNAME <name>")),
                    RedisValue::BulkString(Bytes::from("CLIENT GETNAME")),
                    RedisValue::BulkString(Bytes::from("CLIENT ID")),
                    RedisValue::BulkString(Bytes::from("CLIENT LIST [TYPE <type>]")),
                    RedisValue::BulkString(Bytes::from("CLIENT INFO")),
                    RedisValue::BulkString(Bytes::from("CLIENT KILL <filter>")),
                    RedisValue::BulkString(Bytes::from("CLIENT PAUSE <timeout>")),
                    RedisValue::BulkString(Bytes::from("CLIENT UNPAUSE")),
                    RedisValue::BulkString(Bytes::from("CLIENT REPLY ON|OFF|SKIP")),
                ])),
                _ => Err(RedisError::new(
                    "ERR",
                    format!(
                        "Unknown CLIENT subcommand '{}'. Try CLIENT HELP.",
                        subcommand
                    ),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "CLIENT"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// HELLO command handler (RESP3 protocol negotiation).
pub struct HelloHandler;

impl CommandHandler for HelloHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        // Extract all data from cmd before async block
        let argc = cmd.argc();
        let args: Vec<Option<String>> = (0..argc)
            .map(|i| cmd.arg_str(i).map(|s| s.to_string()))
            .collect();
        let arg_ints: Vec<Option<i64>> = (0..argc).map(|i| cmd.arg_int(i)).collect();

        // Extract data from ctx and state
        let initial_protocol_version = ctx.protocol_version;
        let connection_id = ctx.connection_id;
        let redis_version = state.server_info().redis_version.clone();

        Box::pin(async move {
            // HELLO [protover [AUTH username password] [SETNAME clientname]]
            let mut protocol_version = initial_protocol_version;
            let mut _auth_user: Option<String> = None;
            let mut _auth_pass: Option<String> = None;
            let mut _client_name: Option<String> = None;

            let mut i = 0;

            // Parse protocol version if provided
            if i < argc {
                if let Some(ver) = arg_ints[i] {
                    if !(2..=3).contains(&ver) {
                        return Err(RedisError::new("NOPROTO", "unsupported protocol version"));
                    }
                    protocol_version = ver as u8;
                    i += 1;
                }
            }

            // Parse optional arguments
            while i < argc {
                let arg = args[i]
                    .as_ref()
                    .ok_or_else(RedisError::syntax)?
                    .to_uppercase();

                match arg.as_str() {
                    "AUTH" => {
                        i += 1;
                        _auth_user = if i < argc { args[i].clone() } else { None };
                        i += 1;
                        _auth_pass = if i < argc { args[i].clone() } else { None };
                    }
                    "SETNAME" => {
                        i += 1;
                        _client_name = if i < argc { args[i].clone() } else { None };
                    }
                    _ => {
                        return Err(RedisError::syntax());
                    }
                }
                i += 1;
            }

            // Build response inline since we can't call Self::build_response
            let response = if protocol_version >= 3 {
                // RESP3: Return a map
                RedisValue::Map(vec![
                    (
                        RedisValue::BulkString(Bytes::from("server")),
                        RedisValue::BulkString(Bytes::from("lattice")),
                    ),
                    (
                        RedisValue::BulkString(Bytes::from("version")),
                        RedisValue::BulkString(Bytes::from(redis_version)),
                    ),
                    (
                        RedisValue::BulkString(Bytes::from("proto")),
                        RedisValue::Integer(protocol_version as i64),
                    ),
                    (
                        RedisValue::BulkString(Bytes::from("id")),
                        RedisValue::Integer(connection_id as i64),
                    ),
                    (
                        RedisValue::BulkString(Bytes::from("mode")),
                        RedisValue::BulkString(Bytes::from("standalone")),
                    ),
                    (
                        RedisValue::BulkString(Bytes::from("role")),
                        RedisValue::BulkString(Bytes::from("master")),
                    ),
                    (
                        RedisValue::BulkString(Bytes::from("modules")),
                        RedisValue::Array(vec![]),
                    ),
                ])
            } else {
                // RESP2: Return an array (flattened map)
                RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("server")),
                    RedisValue::BulkString(Bytes::from("lattice")),
                    RedisValue::BulkString(Bytes::from("version")),
                    RedisValue::BulkString(Bytes::from(redis_version)),
                    RedisValue::BulkString(Bytes::from("proto")),
                    RedisValue::Integer(protocol_version as i64),
                    RedisValue::BulkString(Bytes::from("id")),
                    RedisValue::Integer(connection_id as i64),
                    RedisValue::BulkString(Bytes::from("mode")),
                    RedisValue::BulkString(Bytes::from("standalone")),
                    RedisValue::BulkString(Bytes::from("role")),
                    RedisValue::BulkString(Bytes::from("master")),
                    RedisValue::BulkString(Bytes::from("modules")),
                    RedisValue::Array(vec![]),
                ])
            };

            Ok(response)
        })
    }

    fn name(&self) -> &'static str {
        "HELLO"
    }

    fn requires_auth(&self) -> bool {
        false // HELLO can include inline auth
    }
}

/// RESET command handler.
pub struct ResetHandler;

impl CommandHandler for ResetHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // RESET clears all connection state:
            // - Exit Pub/Sub mode
            // - Discard MULTI transaction
            // - Clear WATCH keys
            // - Reset protocol to RESP2
            // - Clear client name

            // Return RESET (simple string)
            Ok(RedisValue::SimpleString("RESET".to_string()))
        })
    }

    fn name(&self) -> &'static str {
        "RESET"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }

    fn requires_auth(&self) -> bool {
        false
    }

    fn allowed_in_pubsub(&self) -> bool {
        true
    }

    fn allowed_in_transaction(&self) -> bool {
        true
    }
}

/// Connection state for a Redis client.
#[derive(Debug, Clone)]
pub struct ConnectionState {
    /// Connection ID.
    pub id: u64,

    /// Client name (from CLIENT SETNAME or HELLO SETNAME).
    pub name: Option<String>,

    /// Protocol version (2 or 3).
    pub protocol_version: ProtocolVersion,

    /// Whether authentication is complete.
    pub authenticated: bool,

    /// Authenticated tenant ID.
    pub tenant_id: Option<String>,

    /// Whether in transaction mode (MULTI).
    pub in_transaction: bool,

    /// Queued commands for transaction.
    pub transaction_queue: Vec<RedisCommand>,

    /// Watched keys with their revisions.
    pub watched_keys: Vec<(Bytes, u64)>,

    /// Whether in pub/sub mode.
    pub in_pubsub: bool,

    /// Subscribed channels.
    pub subscribed_channels: std::collections::HashSet<String>,

    /// Subscribed patterns.
    pub subscribed_patterns: std::collections::HashSet<String>,

    /// Whether READONLY mode is enabled.
    pub readonly: bool,

    /// Whether client replies are enabled.
    pub reply_mode: ReplyMode,
}

/// Client reply mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplyMode {
    /// Normal - send all replies.
    #[default]
    On,
    /// Skip next reply only.
    Skip,
    /// Don't send any replies.
    Off,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ConnectionState {
    /// Create a new connection state.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            name: None,
            protocol_version: ProtocolVersion::Resp2,
            authenticated: false,
            tenant_id: None,
            in_transaction: false,
            transaction_queue: Vec::new(),
            watched_keys: Vec::new(),
            in_pubsub: false,
            subscribed_channels: std::collections::HashSet::new(),
            subscribed_patterns: std::collections::HashSet::new(),
            readonly: false,
            reply_mode: ReplyMode::On,
        }
    }

    /// Reset the connection state.
    pub fn reset(&mut self) {
        self.name = None;
        self.protocol_version = ProtocolVersion::Resp2;
        self.in_transaction = false;
        self.transaction_queue.clear();
        self.watched_keys.clear();
        self.in_pubsub = false;
        self.subscribed_channels.clear();
        self.subscribed_patterns.clear();
        self.readonly = false;
        self.reply_mode = ReplyMode::On;
    }

    /// Start a transaction.
    pub fn start_transaction(&mut self) {
        self.in_transaction = true;
        self.transaction_queue.clear();
    }

    /// Queue a command for the transaction.
    pub fn queue_command(&mut self, cmd: RedisCommand) {
        self.transaction_queue.push(cmd);
    }

    /// Discard the transaction.
    pub fn discard_transaction(&mut self) {
        self.in_transaction = false;
        self.transaction_queue.clear();
        self.watched_keys.clear();
    }

    /// Watch a key.
    pub fn watch_key(&mut self, key: Bytes, revision: u64) {
        // Don't add duplicates
        if !self.watched_keys.iter().any(|(k, _)| k == &key) {
            self.watched_keys.push((key, revision));
        }
    }

    /// Clear all watched keys.
    pub fn unwatch_all(&mut self) {
        self.watched_keys.clear();
    }

    /// Enter pub/sub mode.
    pub fn enter_pubsub(&mut self) {
        self.in_pubsub = true;
    }

    /// Exit pub/sub mode if no subscriptions remain.
    pub fn check_exit_pubsub(&mut self) {
        if self.subscribed_channels.is_empty() && self.subscribed_patterns.is_empty() {
            self.in_pubsub = false;
        }
    }

    /// Subscribe to a channel.
    pub fn subscribe(&mut self, channel: &str) {
        self.subscribed_channels.insert(channel.to_string());
        self.in_pubsub = true;
    }

    /// Unsubscribe from a channel.
    pub fn unsubscribe(&mut self, channel: &str) {
        self.subscribed_channels.remove(channel);
        self.check_exit_pubsub();
    }

    /// Subscribe to a pattern.
    pub fn psubscribe(&mut self, pattern: &str) {
        self.subscribed_patterns.insert(pattern.to_string());
        self.in_pubsub = true;
    }

    /// Unsubscribe from a pattern.
    pub fn punsubscribe(&mut self, pattern: &str) {
        self.subscribed_patterns.remove(pattern);
        self.check_exit_pubsub();
    }

    /// Get total subscription count.
    pub fn subscription_count(&self) -> usize {
        self.subscribed_channels.len() + self.subscribed_patterns.len()
    }

    /// Check if should send reply.
    pub fn should_reply(&mut self) -> bool {
        match self.reply_mode {
            ReplyMode::On => true,
            ReplyMode::Off => false,
            ReplyMode::Skip => {
                self.reply_mode = ReplyMode::On;
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_new() {
        let state = ConnectionState::new(42);
        assert_eq!(state.id, 42);
        assert!(!state.authenticated);
        assert!(!state.in_transaction);
        assert!(!state.in_pubsub);
        assert!(state.transaction_queue.is_empty());
        assert!(state.watched_keys.is_empty());
    }

    #[test]
    fn test_connection_state_transaction() {
        let mut state = ConnectionState::new(1);

        state.start_transaction();
        assert!(state.in_transaction);

        state.queue_command(RedisCommand::new("GET", vec![Bytes::from("key")]));
        assert_eq!(state.transaction_queue.len(), 1);

        state.discard_transaction();
        assert!(!state.in_transaction);
        assert!(state.transaction_queue.is_empty());
    }

    #[test]
    fn test_connection_state_watch() {
        let mut state = ConnectionState::new(1);

        state.watch_key(Bytes::from("key1"), 100);
        state.watch_key(Bytes::from("key2"), 200);
        assert_eq!(state.watched_keys.len(), 2);

        // Don't add duplicates
        state.watch_key(Bytes::from("key1"), 150);
        assert_eq!(state.watched_keys.len(), 2);

        state.unwatch_all();
        assert!(state.watched_keys.is_empty());
    }

    #[test]
    fn test_connection_state_pubsub() {
        let mut state = ConnectionState::new(1);

        state.subscribe("channel1");
        assert!(state.in_pubsub);
        assert_eq!(state.subscription_count(), 1);

        state.psubscribe("news.*");
        assert_eq!(state.subscription_count(), 2);

        state.unsubscribe("channel1");
        assert!(state.in_pubsub); // Still subscribed to pattern

        state.punsubscribe("news.*");
        assert!(!state.in_pubsub); // Exited pub/sub mode
    }

    #[test]
    fn test_connection_state_reset() {
        let mut state = ConnectionState::new(1);
        state.name = Some("client1".to_string());
        state.protocol_version = ProtocolVersion::Resp3;
        state.start_transaction();
        state.subscribe("channel");

        state.reset();

        assert!(state.name.is_none());
        assert_eq!(state.protocol_version, ProtocolVersion::Resp2);
        assert!(!state.in_transaction);
        assert!(!state.in_pubsub);
    }

    #[test]
    fn test_reply_mode() {
        let mut state = ConnectionState::new(1);

        assert!(state.should_reply());

        state.reply_mode = ReplyMode::Skip;
        assert!(!state.should_reply());
        assert!(state.should_reply()); // Reset to On after Skip

        state.reply_mode = ReplyMode::Off;
        assert!(!state.should_reply());
        assert!(!state.should_reply()); // Stays Off
    }
}
