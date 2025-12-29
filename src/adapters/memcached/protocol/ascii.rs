//! Memcached ASCII protocol parser and encoder.
//!
//! The ASCII protocol uses simple text-based commands terminated by \r\n.
//!
//! # Command Format
//!
//! ## Storage commands: `<command> <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n`
//! - set, add, replace, append, prepend
//!
//! ## CAS command: `cas <key> <flags> <exptime> <bytes> <cas> [noreply]\r\n<data>\r\n`
//!
//! ## Retrieval commands: `get <key>*\r\n` or `gets <key>*\r\n`
//!
//! ## Delete command: `delete <key> [noreply]\r\n`
//!
//! ## Increment/Decrement: `incr <key> <value> [noreply]\r\n`
//!
//! ## Touch: `touch <key> <exptime> [noreply]\r\n`
//!
//! ## Other: `stats [args]\r\n`, `flush_all [delay] [noreply]\r\n`, `version\r\n`, `quit\r\n`

use super::{CommandLineResult, ParseResult};
use crate::adapters::memcached::{
    MemcachedCommand, MemcachedError, MemcachedResponse, ResponseStatus, ValueResponse,
};
use bytes::Bytes;

/// ASCII protocol parser.
#[derive(Debug, Default)]
pub struct AsciiParser {
    /// Current parsing state.
    state: AsciiParseState,
}

/// ASCII parsing state.
#[derive(Debug, Default)]
enum AsciiParseState {
    /// Ready to parse a command line.
    #[default]
    Ready,
    /// Waiting for data block.
    WaitingData {
        /// Parsed command so far.
        command: MemcachedCommand,
        /// Expected data bytes.
        expected_bytes: usize,
        /// Command line bytes consumed.
        header_consumed: usize,
    },
}

impl AsciiParser {
    /// Create a new ASCII parser.
    pub fn new() -> Self {
        Self {
            state: AsciiParseState::Ready,
        }
    }

    /// Parse a command from the buffer.
    pub fn parse(&mut self, data: &[u8]) -> ParseResult {
        match &self.state {
            AsciiParseState::Ready => self.parse_command_line(data),
            AsciiParseState::WaitingData {
                command,
                expected_bytes,
                header_consumed,
            } => {
                let command = command.clone();
                let expected_bytes = *expected_bytes;
                let header_consumed = *header_consumed;
                self.parse_data_block(data, command, expected_bytes, header_consumed)
            }
        }
    }

    /// Parse a command line.
    fn parse_command_line(&mut self, data: &[u8]) -> ParseResult {
        match parse_command_line(data) {
            CommandLineResult::Incomplete => ParseResult::Incomplete,
            CommandLineResult::Error(e) => ParseResult::Error(e),
            CommandLineResult::Ok {
                command,
                args,
                consumed,
            } => self.process_command(command, args, consumed, data),
        }
    }

    /// Process a parsed command line.
    fn process_command(
        &mut self,
        command: String,
        args: Vec<String>,
        consumed: usize,
        data: &[u8],
    ) -> ParseResult {
        let cmd_lower = command.to_lowercase();

        match cmd_lower.as_str() {
            // Storage commands: set, add, replace, append, prepend
            "set" | "add" | "replace" | "append" | "prepend" => {
                self.parse_storage_command(&cmd_lower, &args, consumed, data)
            }

            // CAS command
            "cas" => self.parse_cas_command(&args, consumed, data),

            // Retrieval commands
            "get" | "gets" => self.parse_get_command(&cmd_lower, &args, consumed),

            // GAT commands (get and touch)
            "gat" | "gats" => self.parse_gat_command(&cmd_lower, &args, consumed),

            // Delete command
            "delete" => self.parse_delete_command(&args, consumed),

            // Increment/Decrement
            "incr" | "decr" => self.parse_incr_command(&cmd_lower, &args, consumed),

            // Touch command
            "touch" => self.parse_touch_command(&args, consumed),

            // Stats command
            "stats" => self.parse_stats_command(&args, consumed),

            // Flush command
            "flush_all" => self.parse_flush_command(&args, consumed),

            // Version command
            "version" => ParseResult::Ok(MemcachedCommand::new("version"), consumed),

            // Quit command
            "quit" => ParseResult::Ok(MemcachedCommand::new("quit"), consumed),

            // Verbosity command (no-op in Lattice)
            "verbosity" => {
                let noreply = args.last().map(|s| s == "noreply").unwrap_or(false);
                ParseResult::Ok(
                    MemcachedCommand::new("verbosity").with_noreply(noreply),
                    consumed,
                )
            }

            // Unknown command
            _ => ParseResult::Error(format!("unknown command: {}", command)),
        }
    }

    /// Parse a storage command (set, add, replace, append, prepend).
    fn parse_storage_command(
        &mut self,
        command: &str,
        args: &[String],
        consumed: usize,
        data: &[u8],
    ) -> ParseResult {
        // Format: <key> <flags> <exptime> <bytes> [noreply]
        if args.len() < 4 {
            return ParseResult::Error(format!(
                "CLIENT_ERROR bad command line format for {}",
                command
            ));
        }

        let key = args[0].clone();
        let flags: u32 = match args[1].parse() {
            Ok(f) => f,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad flags".to_string()),
        };
        let exptime: u32 = match args[2].parse() {
            Ok(e) => e,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad exptime".to_string()),
        };
        let bytes: usize = match args[3].parse() {
            Ok(b) => b,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad bytes".to_string()),
        };
        let noreply = args.get(4).map(|s| s == "noreply").unwrap_or(false);

        let cmd = MemcachedCommand::new(command)
            .with_key(key)
            .with_flags(flags)
            .with_exptime(exptime)
            .with_noreply(noreply);

        // Check if we have enough data
        self.state = AsciiParseState::WaitingData {
            command: cmd,
            expected_bytes: bytes,
            header_consumed: consumed,
        };

        self.parse_data_block(data, self.state_command().unwrap(), bytes, consumed)
    }

    /// Parse a CAS command.
    fn parse_cas_command(&mut self, args: &[String], consumed: usize, data: &[u8]) -> ParseResult {
        // Format: <key> <flags> <exptime> <bytes> <cas> [noreply]
        if args.len() < 5 {
            return ParseResult::Error("CLIENT_ERROR bad command line format for cas".to_string());
        }

        let key = args[0].clone();
        let flags: u32 = match args[1].parse() {
            Ok(f) => f,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad flags".to_string()),
        };
        let exptime: u32 = match args[2].parse() {
            Ok(e) => e,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad exptime".to_string()),
        };
        let bytes: usize = match args[3].parse() {
            Ok(b) => b,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad bytes".to_string()),
        };
        let cas: u64 = match args[4].parse() {
            Ok(c) => c,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad cas".to_string()),
        };
        let noreply = args.get(5).map(|s| s == "noreply").unwrap_or(false);

        let cmd = MemcachedCommand::new("cas")
            .with_key(key)
            .with_flags(flags)
            .with_exptime(exptime)
            .with_cas(cas)
            .with_noreply(noreply);

        self.state = AsciiParseState::WaitingData {
            command: cmd,
            expected_bytes: bytes,
            header_consumed: consumed,
        };

        self.parse_data_block(data, self.state_command().unwrap(), bytes, consumed)
    }

    /// Parse a get/gets command.
    fn parse_get_command(
        &mut self,
        command: &str,
        args: &[String],
        consumed: usize,
    ) -> ParseResult {
        if args.is_empty() {
            return ParseResult::Error(format!(
                "CLIENT_ERROR bad command line format for {}",
                command
            ));
        }

        let mut cmd = MemcachedCommand::new(command);

        // First key
        cmd.key = Some(Bytes::from(args[0].clone()));

        // Additional keys
        for key in args.iter().skip(1) {
            cmd.keys.push(Bytes::from(key.clone()));
        }

        ParseResult::Ok(cmd, consumed)
    }

    /// Parse a gat/gats command (get and touch).
    fn parse_gat_command(
        &mut self,
        command: &str,
        args: &[String],
        consumed: usize,
    ) -> ParseResult {
        // Format: <exptime> <key>+
        if args.len() < 2 {
            return ParseResult::Error(format!(
                "CLIENT_ERROR bad command line format for {}",
                command
            ));
        }

        let exptime: u32 = match args[0].parse() {
            Ok(e) => e,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad exptime".to_string()),
        };

        let mut cmd = MemcachedCommand::new(command).with_exptime(exptime);

        // First key
        cmd.key = Some(Bytes::from(args[1].clone()));

        // Additional keys
        for key in args.iter().skip(2) {
            cmd.keys.push(Bytes::from(key.clone()));
        }

        ParseResult::Ok(cmd, consumed)
    }

    /// Parse a delete command.
    fn parse_delete_command(&mut self, args: &[String], consumed: usize) -> ParseResult {
        if args.is_empty() {
            return ParseResult::Error(
                "CLIENT_ERROR bad command line format for delete".to_string(),
            );
        }

        let key = args[0].clone();
        let noreply = args.get(1).map(|s| s == "noreply").unwrap_or(false);

        let cmd = MemcachedCommand::new("delete")
            .with_key(key)
            .with_noreply(noreply);

        ParseResult::Ok(cmd, consumed)
    }

    /// Parse an incr/decr command.
    fn parse_incr_command(
        &mut self,
        command: &str,
        args: &[String],
        consumed: usize,
    ) -> ParseResult {
        // Format: <key> <value> [noreply]
        if args.len() < 2 {
            return ParseResult::Error(format!(
                "CLIENT_ERROR bad command line format for {}",
                command
            ));
        }

        let key = args[0].clone();
        let delta: u64 = match args[1].parse() {
            Ok(d) => d,
            Err(_) => {
                return ParseResult::Error(
                    "CLIENT_ERROR invalid numeric delta argument".to_string(),
                )
            }
        };
        let noreply = args.get(2).map(|s| s == "noreply").unwrap_or(false);

        let cmd = MemcachedCommand::new(command)
            .with_key(key)
            .with_delta(delta)
            .with_noreply(noreply);

        ParseResult::Ok(cmd, consumed)
    }

    /// Parse a touch command.
    fn parse_touch_command(&mut self, args: &[String], consumed: usize) -> ParseResult {
        // Format: <key> <exptime> [noreply]
        if args.len() < 2 {
            return ParseResult::Error(
                "CLIENT_ERROR bad command line format for touch".to_string(),
            );
        }

        let key = args[0].clone();
        let exptime: u32 = match args[1].parse() {
            Ok(e) => e,
            Err(_) => return ParseResult::Error("CLIENT_ERROR bad exptime".to_string()),
        };
        let noreply = args.get(2).map(|s| s == "noreply").unwrap_or(false);

        let cmd = MemcachedCommand::new("touch")
            .with_key(key)
            .with_exptime(exptime)
            .with_noreply(noreply);

        ParseResult::Ok(cmd, consumed)
    }

    /// Parse a stats command.
    fn parse_stats_command(&mut self, args: &[String], consumed: usize) -> ParseResult {
        let mut cmd = MemcachedCommand::new("stats");

        // Optional stats type argument
        if !args.is_empty() {
            cmd.key = Some(Bytes::from(args[0].clone()));
        }

        ParseResult::Ok(cmd, consumed)
    }

    /// Parse a flush_all command.
    fn parse_flush_command(&mut self, args: &[String], consumed: usize) -> ParseResult {
        let mut delay: u32 = 0;
        let mut noreply = false;

        for arg in args {
            if arg == "noreply" {
                noreply = true;
            } else if let Ok(d) = arg.parse::<u32>() {
                delay = d;
            }
        }

        let cmd = MemcachedCommand::new("flush_all")
            .with_exptime(delay)
            .with_noreply(noreply);

        ParseResult::Ok(cmd, consumed)
    }

    /// Parse a data block after the command line.
    fn parse_data_block(
        &mut self,
        data: &[u8],
        mut command: MemcachedCommand,
        expected_bytes: usize,
        header_consumed: usize,
    ) -> ParseResult {
        // Data starts after the header
        let data_start = header_consumed;

        // Need: data bytes + \r\n
        let needed = expected_bytes + 2;

        if data.len() < data_start + needed {
            // Store state and return incomplete
            self.state = AsciiParseState::WaitingData {
                command,
                expected_bytes,
                header_consumed,
            };
            return ParseResult::Incomplete;
        }

        // Extract data bytes
        let data_bytes = &data[data_start..data_start + expected_bytes];

        // Verify terminating \r\n
        if data[data_start + expected_bytes] != b'\r'
            || data[data_start + expected_bytes + 1] != b'\n'
        {
            self.state = AsciiParseState::Ready;
            return ParseResult::Error("CLIENT_ERROR bad data chunk".to_string());
        }

        command.data = Some(Bytes::copy_from_slice(data_bytes));
        self.state = AsciiParseState::Ready;

        ParseResult::Ok(command, data_start + needed)
    }

    /// Get the command from the current state (if waiting for data).
    fn state_command(&self) -> Option<MemcachedCommand> {
        match &self.state {
            AsciiParseState::WaitingData { command, .. } => Some(command.clone()),
            _ => None,
        }
    }

    /// Reset parser state.
    pub fn reset(&mut self) {
        self.state = AsciiParseState::Ready;
    }
}

/// Parse a command line from data.
fn parse_command_line(data: &[u8]) -> CommandLineResult {
    // Find \r\n
    let crlf_pos = match data.windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos,
        None => return CommandLineResult::Incomplete,
    };

    // Parse the line
    let line = match std::str::from_utf8(&data[..crlf_pos]) {
        Ok(s) => s,
        Err(_) => {
            return CommandLineResult::Error("CLIENT_ERROR bad characters in command".to_string())
        }
    };

    // Split into tokens
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.is_empty() {
        return CommandLineResult::Error("CLIENT_ERROR empty command".to_string());
    }

    let command = parts[0].to_string();
    let args: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();

    CommandLineResult::Ok {
        command,
        args,
        consumed: crlf_pos + 2,
    }
}

/// ASCII protocol encoder.
#[derive(Debug, Default)]
pub struct AsciiEncoder;

impl AsciiEncoder {
    /// Create a new ASCII encoder.
    pub fn new() -> Self {
        Self
    }

    /// Encode a response to bytes.
    pub fn encode(response: &MemcachedResponse) -> Vec<u8> {
        match response {
            MemcachedResponse::Status(status) => Self::encode_status(*status),
            MemcachedResponse::Value(value) => Self::encode_value(value, true),
            MemcachedResponse::Values(values) => Self::encode_values(values),
            MemcachedResponse::Numeric(n) => Self::encode_numeric(*n),
            MemcachedResponse::Stats(stats) => Self::encode_stats(stats),
            MemcachedResponse::Version(version) => Self::encode_version(version),
            MemcachedResponse::Error(error) => Self::encode_error(error),
            MemcachedResponse::NoResponse => Vec::new(),
        }
    }

    /// Encode a status response.
    fn encode_status(status: ResponseStatus) -> Vec<u8> {
        format!("{}\r\n", status).into_bytes()
    }

    /// Encode a value response.
    fn encode_value(value: &ValueResponse, include_end: bool) -> Vec<u8> {
        let key_str = std::str::from_utf8(&value.key).unwrap_or("<binary>");
        let mut buf = if let Some(cas) = value.cas {
            format!(
                "VALUE {} {} {} {}\r\n",
                key_str,
                value.flags,
                value.data.len(),
                cas
            )
            .into_bytes()
        } else {
            format!("VALUE {} {} {}\r\n", key_str, value.flags, value.data.len()).into_bytes()
        };
        buf.extend_from_slice(&value.data);
        buf.extend_from_slice(b"\r\n");
        if include_end {
            buf.extend_from_slice(b"END\r\n");
        }
        buf
    }

    /// Encode multiple values.
    fn encode_values(values: &[ValueResponse]) -> Vec<u8> {
        let mut buf = Vec::new();
        for value in values {
            buf.extend(Self::encode_value(value, false));
        }
        buf.extend_from_slice(b"END\r\n");
        buf
    }

    /// Encode a numeric response.
    fn encode_numeric(n: u64) -> Vec<u8> {
        format!("{}\r\n", n).into_bytes()
    }

    /// Encode stats response.
    fn encode_stats(stats: &[(String, String)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (key, value) in stats {
            buf.extend(format!("STAT {} {}\r\n", key, value).into_bytes());
        }
        buf.extend_from_slice(b"END\r\n");
        buf
    }

    /// Encode version response.
    fn encode_version(version: &str) -> Vec<u8> {
        format!("VERSION {}\r\n", version).into_bytes()
    }

    /// Encode error response.
    fn encode_error(error: &MemcachedError) -> Vec<u8> {
        format!("{}\r\n", error).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"get mykey\r\n");

        match result {
            ParseResult::Ok(cmd, consumed) => {
                assert_eq!(cmd.name, "get");
                assert_eq!(cmd.key_str(), Some("mykey"));
                assert_eq!(consumed, 11);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_get_multiple_keys() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"get key1 key2 key3\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "get");
                let keys: Vec<_> = cmd.all_keys().collect();
                assert_eq!(keys.len(), 3);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_gets() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"gets mykey\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "gets");
                assert_eq!(cmd.key_str(), Some("mykey"));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_set() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"set mykey 0 3600 5\r\nhello\r\n");

        match result {
            ParseResult::Ok(cmd, consumed) => {
                assert_eq!(cmd.name, "set");
                assert_eq!(cmd.key_str(), Some("mykey"));
                assert_eq!(cmd.flags, 0);
                assert_eq!(cmd.exptime, 3600);
                assert_eq!(
                    cmd.data.as_ref().map(|d| d.as_ref()),
                    Some(b"hello".as_slice())
                );
                assert_eq!(consumed, 27);
                assert!(!cmd.noreply);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_set_noreply() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"set mykey 42 0 3 noreply\r\nfoo\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "set");
                assert_eq!(cmd.flags, 42);
                assert!(cmd.noreply);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_set_incomplete() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"set mykey 0 3600 5\r\nhel");

        assert!(matches!(result, ParseResult::Incomplete));
    }

    #[test]
    fn test_parse_add() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"add newkey 0 0 4\r\ntest\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "add");
                assert_eq!(cmd.key_str(), Some("newkey"));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_replace() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"replace mykey 0 0 4\r\ntest\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "replace");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_append() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"append mykey 0 0 4\r\nmore\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "append");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_prepend() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"prepend mykey 0 0 4\r\nless\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "prepend");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_cas() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"cas mykey 0 3600 5 12345\r\nhello\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "cas");
                assert_eq!(cmd.cas, Some(12345));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"delete mykey\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "delete");
                assert_eq!(cmd.key_str(), Some("mykey"));
                assert!(!cmd.noreply);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_delete_noreply() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"delete mykey noreply\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "delete");
                assert!(cmd.noreply);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_incr() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"incr counter 5\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "incr");
                assert_eq!(cmd.key_str(), Some("counter"));
                assert_eq!(cmd.delta, Some(5));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_decr() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"decr counter 3\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "decr");
                assert_eq!(cmd.delta, Some(3));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_touch() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"touch mykey 3600\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "touch");
                assert_eq!(cmd.exptime, 3600);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_gat() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"gat 3600 key1 key2\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "gat");
                assert_eq!(cmd.exptime, 3600);
                let keys: Vec<_> = cmd.all_keys().collect();
                assert_eq!(keys.len(), 2);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_stats() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"stats\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "stats");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_stats_with_arg() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"stats slabs\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "stats");
                assert_eq!(cmd.key_str(), Some("slabs"));
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_flush_all() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"flush_all\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "flush_all");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_flush_all_delay() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"flush_all 30\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "flush_all");
                assert_eq!(cmd.exptime, 30);
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_version() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"version\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "version");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_quit() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"quit\r\n");

        match result {
            ParseResult::Ok(cmd, _) => {
                assert_eq!(cmd.name, "quit");
            }
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_parse_incomplete() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"get mykey");

        assert!(matches!(result, ParseResult::Incomplete));
    }

    #[test]
    fn test_parse_unknown_command() {
        let mut parser = AsciiParser::new();
        let result = parser.parse(b"unknown command\r\n");

        assert!(matches!(result, ParseResult::Error(_)));
    }

    #[test]
    fn test_encode_status() {
        assert_eq!(
            AsciiEncoder::encode(&MemcachedResponse::stored()),
            b"STORED\r\n"
        );
        assert_eq!(
            AsciiEncoder::encode(&MemcachedResponse::not_stored()),
            b"NOT_STORED\r\n"
        );
        assert_eq!(
            AsciiEncoder::encode(&MemcachedResponse::not_found()),
            b"NOT_FOUND\r\n"
        );
        assert_eq!(
            AsciiEncoder::encode(&MemcachedResponse::deleted()),
            b"DELETED\r\n"
        );
        assert_eq!(
            AsciiEncoder::encode(&MemcachedResponse::touched()),
            b"TOUCHED\r\n"
        );
    }

    #[test]
    fn test_encode_value() {
        let response = MemcachedResponse::value(Bytes::from("mykey"), 42, Bytes::from("hello"));

        let encoded = AsciiEncoder::encode(&response);
        assert_eq!(encoded, b"VALUE mykey 42 5\r\nhello\r\nEND\r\n");
    }

    #[test]
    fn test_encode_value_with_cas() {
        let response =
            MemcachedResponse::value_with_cas(Bytes::from("mykey"), 0, Bytes::from("test"), 12345);

        let encoded = AsciiEncoder::encode(&response);
        assert!(encoded.starts_with(b"VALUE mykey 0 4 12345\r\n"));
    }

    #[test]
    fn test_encode_multiple_values() {
        let values = vec![
            ValueResponse {
                key: Bytes::from("key1"),
                flags: 0,
                data: Bytes::from("val1"),
                cas: None,
            },
            ValueResponse {
                key: Bytes::from("key2"),
                flags: 0,
                data: Bytes::from("val2"),
                cas: None,
            },
        ];

        let encoded = AsciiEncoder::encode(&MemcachedResponse::Values(values));
        assert!(encoded.ends_with(b"END\r\n"));
    }

    #[test]
    fn test_encode_numeric() {
        let response = MemcachedResponse::Numeric(12345);
        assert_eq!(AsciiEncoder::encode(&response), b"12345\r\n");
    }

    #[test]
    fn test_encode_stats() {
        let stats = vec![
            ("pid".to_string(), "1234".to_string()),
            ("uptime".to_string(), "3600".to_string()),
        ];

        let encoded = AsciiEncoder::encode(&MemcachedResponse::Stats(stats));
        assert!(encoded.starts_with(b"STAT pid 1234\r\n"));
        assert!(encoded.ends_with(b"END\r\n"));
    }

    #[test]
    fn test_encode_version() {
        let response = MemcachedResponse::Version("1.6.0".to_string());
        assert_eq!(AsciiEncoder::encode(&response), b"VERSION 1.6.0\r\n");
    }

    #[test]
    fn test_encode_error() {
        assert_eq!(
            AsciiEncoder::encode(&MemcachedResponse::error()),
            b"ERROR\r\n"
        );
        assert_eq!(
            AsciiEncoder::encode(&MemcachedResponse::client_error("bad format")),
            b"CLIENT_ERROR bad format\r\n"
        );
        assert_eq!(
            AsciiEncoder::encode(&MemcachedResponse::server_error("internal")),
            b"SERVER_ERROR internal\r\n"
        );
    }

    #[test]
    fn test_encode_no_response() {
        let response = MemcachedResponse::NoResponse;
        assert_eq!(AsciiEncoder::encode(&response), b"");
    }
}
