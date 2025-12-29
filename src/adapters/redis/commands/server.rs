//! Redis server command handlers.
//!
//! Implements PING, INFO, TIME, CONFIG, and related server commands.

use super::{CommandContext, CommandHandler, CommandResult, CommandState};
use crate::adapters::redis::{RedisCommand, RedisError, RedisValue};
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// PING command handler.
pub struct PingHandler;

impl CommandHandler for PingHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            match cmd.arg(0) {
                Some(msg) => Ok(RedisValue::BulkString(msg.clone())),
                None => Ok(RedisValue::pong()),
            }
        })
    }

    fn name(&self) -> &'static str {
        "PING"
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }

    fn requires_auth(&self) -> bool {
        false
    }

    fn allowed_in_pubsub(&self) -> bool {
        true
    }
}

/// ECHO command handler.
pub struct EchoHandler;

impl CommandHandler for EchoHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let msg = cmd.arg(0).ok_or_else(|| RedisError::wrong_arity("ECHO"))?;
            Ok(RedisValue::BulkString(msg.clone()))
        })
    }

    fn name(&self) -> &'static str {
        "ECHO"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }

    fn requires_auth(&self) -> bool {
        false
    }
}

/// QUIT command handler.
pub struct QuitHandler;

impl CommandHandler for QuitHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Return OK - connection will be closed by the handler
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "QUIT"
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
}

/// SELECT command handler.
pub struct SelectHandler;

impl CommandHandler for SelectHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let db = cmd
                .arg_int(0)
                .ok_or_else(|| RedisError::new("ERR", "value is not an integer or out of range"))?;

            // Lattice uses tenants instead of databases
            // Accept only DB 0 for compatibility
            if db != 0 {
                return Err(RedisError::new(
                    "ERR",
                    "Lattice uses tenant isolation instead of databases. Use AUTH to switch tenants.",
                ));
            }

            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "SELECT"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// DBSIZE command handler.
pub struct DbSizeHandler;

impl CommandHandler for DbSizeHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        _cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let count = state.kv().key_count(&ctx.request.tenant_id).await?;
            Ok(RedisValue::Integer(count as i64))
        })
    }

    fn name(&self) -> &'static str {
        "DBSIZE"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
}

/// INFO command handler.
pub struct InfoHandler;

impl CommandHandler for InfoHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let section = cmd.arg_str(0).map(|s| s.to_lowercase());

            let mut info = String::new();

            let server_info = state.server_info();

            // Server section
            if section.is_none()
                || section.as_deref() == Some("server")
                || section.as_deref() == Some("all")
            {
                info.push_str("# Server\r\n");
                info.push_str(&format!("redis_version:{}\r\n", server_info.redis_version));
                info.push_str(&format!("lattice_version:{}\r\n", server_info.version));
                info.push_str(&format!("os:{}\r\n", server_info.os));
                info.push_str(&format!(
                    "arch_bits:{}\r\n",
                    if server_info.arch == "x86_64" || server_info.arch == "aarch64" {
                        64
                    } else {
                        32
                    }
                ));
                info.push_str(&format!("process_id:{}\r\n", server_info.pid));
                info.push_str(&format!(
                    "uptime_in_seconds:{}\r\n",
                    server_info.uptime_seconds.load(Ordering::Relaxed)
                ));
                info.push_str("tcp_port:6379\r\n");
                info.push_str("\r\n");
            }

            // Clients section
            if section.is_none()
                || section.as_deref() == Some("clients")
                || section.as_deref() == Some("all")
            {
                info.push_str("# Clients\r\n");
                info.push_str("connected_clients:1\r\n"); // Would need actual count
                info.push_str("blocked_clients:0\r\n");
                info.push_str("\r\n");
            }

            // Memory section
            if section.is_none()
                || section.as_deref() == Some("memory")
                || section.as_deref() == Some("all")
            {
                info.push_str("# Memory\r\n");
                info.push_str("used_memory:0\r\n");
                info.push_str("used_memory_human:0B\r\n");
                info.push_str("used_memory_peak:0\r\n");
                info.push_str("used_memory_peak_human:0B\r\n");
                info.push_str("\r\n");
            }

            // Stats section
            if section.is_none()
                || section.as_deref() == Some("stats")
                || section.as_deref() == Some("all")
            {
                info.push_str("# Stats\r\n");
                info.push_str(&format!(
                    "total_connections_received:{}\r\n",
                    server_info.total_connections.load(Ordering::Relaxed)
                ));
                info.push_str(&format!(
                    "total_commands_processed:{}\r\n",
                    server_info.total_commands.load(Ordering::Relaxed)
                ));
                info.push_str("\r\n");
            }

            // Replication section
            if section.is_none()
                || section.as_deref() == Some("replication")
                || section.as_deref() == Some("all")
            {
                info.push_str("# Replication\r\n");
                info.push_str("role:master\r\n"); // Lattice is always leader for writes
                info.push_str("connected_slaves:0\r\n");
                info.push_str("\r\n");
            }

            // CPU section
            if section.is_none()
                || section.as_deref() == Some("cpu")
                || section.as_deref() == Some("all")
            {
                info.push_str("# CPU\r\n");
                info.push_str("used_cpu_sys:0.0\r\n");
                info.push_str("used_cpu_user:0.0\r\n");
                info.push_str("\r\n");
            }

            // Cluster section
            if section.is_none()
                || section.as_deref() == Some("cluster")
                || section.as_deref() == Some("all")
            {
                info.push_str("# Cluster\r\n");
                info.push_str("cluster_enabled:0\r\n");
                info.push_str("\r\n");
            }

            // Keyspace section
            if section.is_none()
                || section.as_deref() == Some("keyspace")
                || section.as_deref() == Some("all")
            {
                let key_count = state
                    .kv()
                    .key_count(&ctx.request.tenant_id)
                    .await
                    .unwrap_or(0);
                info.push_str("# Keyspace\r\n");
                if key_count > 0 {
                    info.push_str(&format!("db0:keys={},expires=0,avg_ttl=0\r\n", key_count));
                }
                info.push_str("\r\n");
            }

            // Lattice-specific section
            if section.is_none()
                || section.as_deref() == Some("lattice")
                || section.as_deref() == Some("all")
            {
                info.push_str("# Lattice\r\n");
                info.push_str(&format!("tenant_id:{}\r\n", ctx.request.tenant_id));
                info.push_str(&format!("kv_epoch:{}\r\n", ctx.request.kv_epoch));
                info.push_str(&format!(
                    "linearizable:{}\r\n",
                    if ctx.request.linearizable {
                        "yes"
                    } else {
                        "no"
                    }
                ));
                info.push_str(&format!("current_revision:{}\r\n", state.kv().revision()));
                info.push_str("\r\n");
            }

            Ok(RedisValue::BulkString(Bytes::from(info)))
        })
    }

    fn name(&self) -> &'static str {
        "INFO"
    }

    fn max_args(&self) -> Option<usize> {
        Some(1)
    }
}

/// TIME command handler.
pub struct TimeHandler;

impl CommandHandler for TimeHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Use the committed tick for determinism
            let tick_ms = state.kv().current_tick();
            let secs = tick_ms / 1000;
            let usecs = (tick_ms % 1000) * 1000;

            Ok(RedisValue::Array(vec![
                RedisValue::BulkString(Bytes::from(secs.to_string())),
                RedisValue::BulkString(Bytes::from(usecs.to_string())),
            ]))
        })
    }

    fn name(&self) -> &'static str {
        "TIME"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
}

/// DEBUG command handler.
pub struct DebugHandler;

impl CommandHandler for DebugHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("DEBUG"))?
                .to_uppercase();

            match subcommand.as_str() {
                "SLEEP" => {
                    let seconds = cmd
                        .arg_str(1)
                        .ok_or_else(|| RedisError::wrong_arity("DEBUG"))?
                        .parse::<f64>()
                        .map_err(|_| RedisError::new("ERR", "value is not a valid float"))?;

                    tokio::time::sleep(std::time::Duration::from_secs_f64(seconds)).await;
                    Ok(RedisValue::ok())
                }
                "OBJECT" => {
                    let key = cmd.arg(1).ok_or_else(|| RedisError::wrong_arity("DEBUG"))?;

                    match state.kv().get(&ctx.request.tenant_id, key).await? {
                        None => Ok(RedisValue::Null),
                        Some(entry) => {
                            let info = format!(
                                "Value at:0x0 refcount:1 encoding:raw serializedlength:{} lru:0 lru_seconds_idle:0",
                                entry.value.len()
                            );
                            Ok(RedisValue::BulkString(Bytes::from(info)))
                        }
                    }
                }
                "SEGFAULT" => {
                    // Don't actually segfault
                    Err(RedisError::new("ERR", "DEBUG SEGFAULT is disabled"))
                }
                "SET-ACTIVE-EXPIRE" => {
                    // No-op for compatibility
                    Ok(RedisValue::ok())
                }
                _ => Err(RedisError::new(
                    "ERR",
                    format!("Unknown DEBUG subcommand '{}'", subcommand),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "DEBUG"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn is_admin(&self) -> bool {
        true
    }
}

/// COMMAND command handler.
pub struct CommandCmdHandler;

impl CommandHandler for CommandCmdHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd.arg_str(0).map(|s| s.to_uppercase());

            match subcommand.as_deref() {
                None | Some("") => {
                    // Return basic command info
                    Ok(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("PING")),
                        RedisValue::BulkString(Bytes::from("GET")),
                        RedisValue::BulkString(Bytes::from("SET")),
                        // ... would include all commands
                    ]))
                }
                Some("COUNT") => {
                    // Return approximate command count
                    Ok(RedisValue::Integer(100))
                }
                Some("DOCS") => {
                    // Return command documentation
                    Ok(RedisValue::Array(vec![]))
                }
                Some("GETKEYS") => {
                    // Return keys from a command
                    Ok(RedisValue::Array(vec![]))
                }
                Some("INFO") => {
                    // Return info about specific commands
                    Ok(RedisValue::Array(vec![]))
                }
                Some("LIST") => {
                    // Return list of command names
                    Ok(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("ping")),
                        RedisValue::BulkString(Bytes::from("get")),
                        RedisValue::BulkString(Bytes::from("set")),
                        RedisValue::BulkString(Bytes::from("del")),
                        RedisValue::BulkString(Bytes::from("exists")),
                        RedisValue::BulkString(Bytes::from("keys")),
                        RedisValue::BulkString(Bytes::from("scan")),
                        RedisValue::BulkString(Bytes::from("info")),
                        RedisValue::BulkString(Bytes::from("auth")),
                        RedisValue::BulkString(Bytes::from("multi")),
                        RedisValue::BulkString(Bytes::from("exec")),
                        // ... more commands
                    ]))
                }
                Some(sub) => Err(RedisError::new(
                    "ERR",
                    format!("unknown subcommand '{}'. Try COMMAND HELP.", sub),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "COMMAND"
    }
}

/// CONFIG command handler.
pub struct ConfigHandler;

impl CommandHandler for ConfigHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("CONFIG"))?
                .to_uppercase();

            match subcommand.as_str() {
                "GET" => {
                    let pattern = cmd
                        .arg_str(1)
                        .ok_or_else(|| RedisError::wrong_arity("CONFIG"))?;

                    // Return matching config parameters
                    let mut result = Vec::new();

                    // Only expose safe, read-only parameters
                    let configs = [
                        ("maxclients", "10000"),
                        ("timeout", "0"),
                        ("tcp-keepalive", "300"),
                        ("databases", "1"),
                        ("maxmemory", "0"),
                    ];

                    for (key, value) in &configs {
                        if pattern == "*" || pattern.eq_ignore_ascii_case(key) {
                            result.push(RedisValue::BulkString(Bytes::from(*key)));
                            result.push(RedisValue::BulkString(Bytes::from(*value)));
                        }
                    }

                    Ok(RedisValue::Array(result))
                }
                "SET" => {
                    // Reject most config changes
                    let param = cmd
                        .arg_str(1)
                        .ok_or_else(|| RedisError::wrong_arity("CONFIG"))?;

                    // Allow only client-local settings
                    match param.to_lowercase().as_str() {
                        "client-output-buffer-limit" | "slowlog-max-len" => Ok(RedisValue::ok()),
                        _ => Err(RedisError::new(
                            "ERR",
                            format!("Unsupported CONFIG parameter: {}", param),
                        )),
                    }
                }
                "REWRITE" => {
                    // Not supported
                    Err(RedisError::new("ERR", "CONFIG REWRITE not supported"))
                }
                "RESETSTAT" => {
                    // Reset stats - no-op for now
                    Ok(RedisValue::ok())
                }
                _ => Err(RedisError::new(
                    "ERR",
                    format!("Unknown CONFIG subcommand '{}'", subcommand),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "CONFIG"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// MEMORY command handler.
pub struct MemoryHandler;

impl CommandHandler for MemoryHandler {
    fn execute(
        &self,
        ctx: CommandContext,
        cmd: RedisCommand,
        state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("MEMORY"))?
                .to_uppercase();

            match subcommand.as_str() {
                "USAGE" => {
                    let key = cmd
                        .arg(1)
                        .ok_or_else(|| RedisError::wrong_arity("MEMORY"))?;

                    match state.kv().get(&ctx.request.tenant_id, key).await? {
                        None => Ok(RedisValue::Null),
                        Some(entry) => {
                            // Estimate memory usage
                            let overhead = 64; // Object overhead estimate
                            let usage = overhead + key.len() + entry.value.len();
                            Ok(RedisValue::Integer(usage as i64))
                        }
                    }
                }
                "STATS" => {
                    // Return memory statistics
                    Ok(RedisValue::Array(vec![
                        RedisValue::BulkString(Bytes::from("peak.allocated")),
                        RedisValue::Integer(0),
                        RedisValue::BulkString(Bytes::from("total.allocated")),
                        RedisValue::Integer(0),
                        RedisValue::BulkString(Bytes::from("startup.allocated")),
                        RedisValue::Integer(0),
                    ]))
                }
                "DOCTOR" => Ok(RedisValue::BulkString(Bytes::from(
                    "Sam, I have no memory problems",
                ))),
                "MALLOC-SIZE" => {
                    let _ptr = cmd
                        .arg(1)
                        .ok_or_else(|| RedisError::wrong_arity("MEMORY"))?;
                    Ok(RedisValue::Integer(0))
                }
                "PURGE" => Ok(RedisValue::ok()),
                "HELP" => Ok(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("MEMORY USAGE <key> [SAMPLES <count>]")),
                    RedisValue::BulkString(Bytes::from("MEMORY STATS")),
                    RedisValue::BulkString(Bytes::from("MEMORY DOCTOR")),
                    RedisValue::BulkString(Bytes::from("MEMORY PURGE")),
                ])),
                _ => Err(RedisError::new(
                    "ERR",
                    format!("Unknown MEMORY subcommand '{}'", subcommand),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "MEMORY"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// LATENCY command handler.
pub struct LatencyHandler;

impl CommandHandler for LatencyHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("LATENCY"))?
                .to_uppercase();

            match subcommand.as_str() {
                "DOCTOR" => Ok(RedisValue::BulkString(Bytes::from(
                    "I have no latency reports to show you.",
                ))),
                "HISTORY" => Ok(RedisValue::Array(vec![])),
                "LATEST" => Ok(RedisValue::Array(vec![])),
                "RESET" => Ok(RedisValue::Integer(0)),
                "GRAPH" | "HISTOGRAM" => Ok(RedisValue::BulkString(Bytes::from(""))),
                "HELP" => Ok(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("LATENCY DOCTOR")),
                    RedisValue::BulkString(Bytes::from("LATENCY HISTORY <event>")),
                    RedisValue::BulkString(Bytes::from("LATENCY LATEST")),
                    RedisValue::BulkString(Bytes::from("LATENCY RESET [<event>...]")),
                ])),
                _ => Err(RedisError::new(
                    "ERR",
                    format!("Unknown LATENCY subcommand '{}'", subcommand),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "LATENCY"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// SLOWLOG command handler.
pub struct SlowLogHandler;

impl CommandHandler for SlowLogHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("SLOWLOG"))?
                .to_uppercase();

            match subcommand.as_str() {
                "GET" => {
                    // Return empty list for now
                    Ok(RedisValue::Array(vec![]))
                }
                "LEN" => Ok(RedisValue::Integer(0)),
                "RESET" => Ok(RedisValue::ok()),
                "HELP" => Ok(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("SLOWLOG GET [<count>]")),
                    RedisValue::BulkString(Bytes::from("SLOWLOG LEN")),
                    RedisValue::BulkString(Bytes::from("SLOWLOG RESET")),
                ])),
                _ => Err(RedisError::new(
                    "ERR",
                    format!("Unknown SLOWLOG subcommand '{}'", subcommand),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "SLOWLOG"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// FLUSHDB command handler.
pub struct FlushDbHandler;

impl CommandHandler for FlushDbHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // FLUSHDB would delete all keys for the current tenant
            // This is a dangerous operation - reject for safety
            Err(RedisError::new(
                "ERR",
                "FLUSHDB is disabled. Use DEL with explicit keys.",
            ))
        })
    }

    fn name(&self) -> &'static str {
        "FLUSHDB"
    }

    fn is_write(&self) -> bool {
        true
    }

    fn is_admin(&self) -> bool {
        true
    }
}

/// FLUSHALL command handler.
pub struct FlushAllHandler;

impl CommandHandler for FlushAllHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            Err(RedisError::new(
                "ERR",
                "FLUSHALL is disabled. Use DEL with explicit keys.",
            ))
        })
    }

    fn name(&self) -> &'static str {
        "FLUSHALL"
    }

    fn is_write(&self) -> bool {
        true
    }

    fn is_admin(&self) -> bool {
        true
    }
}

/// CLUSTER command handler (minimal compatibility).
pub struct ClusterHandler;

impl CommandHandler for ClusterHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            let subcommand = cmd
                .arg_str(0)
                .ok_or_else(|| RedisError::wrong_arity("CLUSTER"))?
                .to_uppercase();

            match subcommand.as_str() {
                "INFO" => {
                    let info = "cluster_state:ok\r\n\
                               cluster_slots_assigned:16384\r\n\
                               cluster_slots_ok:16384\r\n\
                               cluster_slots_pfail:0\r\n\
                               cluster_slots_fail:0\r\n\
                               cluster_known_nodes:1\r\n\
                               cluster_size:1\r\n";
                    Ok(RedisValue::BulkString(Bytes::from(info)))
                }
                "SLOTS" => {
                    // Single node owns all slots
                    Ok(RedisValue::Array(vec![RedisValue::Array(vec![
                        RedisValue::Integer(0),     // Start slot
                        RedisValue::Integer(16383), // End slot
                        RedisValue::Array(vec![
                            // Node info
                            RedisValue::BulkString(Bytes::from("127.0.0.1")),
                            RedisValue::Integer(6379),
                            RedisValue::BulkString(Bytes::from(
                                "0000000000000000000000000000000000000001",
                            )),
                        ]),
                    ])]))
                }
                "NODES" => {
                    let nodes = "0000000000000000000000000000000000000001 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383\r\n";
                    Ok(RedisValue::BulkString(Bytes::from(nodes)))
                }
                "KEYSLOT" => {
                    // All keys hash to the same slot in Lattice (no sharding at Redis level)
                    Ok(RedisValue::Integer(0))
                }
                "MYID" => Ok(RedisValue::BulkString(Bytes::from(
                    "0000000000000000000000000000000000000001",
                ))),
                "HELP" => Ok(RedisValue::Array(vec![
                    RedisValue::BulkString(Bytes::from("CLUSTER INFO")),
                    RedisValue::BulkString(Bytes::from("CLUSTER SLOTS")),
                    RedisValue::BulkString(Bytes::from("CLUSTER NODES")),
                    RedisValue::BulkString(Bytes::from("CLUSTER KEYSLOT <key>")),
                    RedisValue::BulkString(Bytes::from("CLUSTER MYID")),
                ])),
                _ => Err(RedisError::new(
                    "ERR",
                    format!("Unknown CLUSTER subcommand '{}'", subcommand),
                )),
            }
        })
    }

    fn name(&self) -> &'static str {
        "CLUSTER"
    }

    fn min_args(&self) -> usize {
        1
    }
}

/// READONLY command handler.
pub struct ReadOnlyHandler;

impl CommandHandler for ReadOnlyHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Enable snapshot reads on replicas
            // In Lattice context, this would enable follower reads
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "READONLY"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
}

/// READWRITE command handler.
pub struct ReadWriteHandler;

impl CommandHandler for ReadWriteHandler {
    fn execute(
        &self,
        _ctx: CommandContext,
        _cmd: RedisCommand,
        _state: Arc<CommandState>,
    ) -> Pin<Box<dyn Future<Output = CommandResult> + Send>> {
        Box::pin(async move {
            // Require leader for reads
            Ok(RedisValue::ok())
        })
    }

    fn name(&self) -> &'static str {
        "READWRITE"
    }

    fn max_args(&self) -> Option<usize> {
        Some(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_handler() {
        let handler = PingHandler;
        assert_eq!(handler.name(), "PING");
        assert!(!handler.requires_auth());
        assert!(handler.allowed_in_pubsub());
    }

    #[test]
    fn test_quit_handler() {
        let handler = QuitHandler;
        assert_eq!(handler.name(), "QUIT");
        assert!(!handler.requires_auth());
        assert!(handler.allowed_in_pubsub());
    }

    #[test]
    fn test_info_handler() {
        let handler = InfoHandler;
        assert_eq!(handler.name(), "INFO");
        assert_eq!(handler.max_args(), Some(1));
    }

    #[test]
    fn test_debug_handler_is_admin() {
        let handler = DebugHandler;
        assert_eq!(handler.name(), "DEBUG");
        assert!(handler.is_admin());
    }

    #[test]
    fn test_flushdb_disabled() {
        let handler = FlushDbHandler;
        assert!(handler.is_admin());
        assert!(handler.is_write());
    }
}
