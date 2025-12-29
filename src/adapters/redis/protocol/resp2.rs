//! RESP2 protocol parser and encoder.
//!
//! RESP2 is the default Redis protocol, supporting:
//! - Simple Strings (+)
//! - Errors (-)
//! - Integers (:)
//! - Bulk Strings ($)
//! - Arrays (*)
//! - Null (represented as $-1 or *-1)

use super::{ParseResult, RespType, MAX_BULK_SIZE, MAX_ELEMENTS, MAX_NESTING_DEPTH};
use crate::adapters::redis::{RedisError, RedisValue};
use bytes::Bytes;

/// RESP2 parser.
#[derive(Debug, Default)]
pub struct Resp2Parser {
    /// Current nesting depth.
    depth: usize,
}

impl Resp2Parser {
    /// Create a new parser.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a value from the buffer.
    ///
    /// Returns the parsed value and the number of bytes consumed.
    pub fn parse(&mut self, data: &[u8]) -> ParseResult {
        self.depth = 0;
        self.parse_value(data).0
    }

    /// Parse a value, returning (result, bytes_consumed).
    fn parse_value(&mut self, data: &[u8]) -> (ParseResult, usize) {
        if data.is_empty() {
            return (ParseResult::Incomplete, 0);
        }

        if self.depth > MAX_NESTING_DEPTH {
            return (
                ParseResult::Error("maximum nesting depth exceeded".to_string()),
                0,
            );
        }

        let type_byte = data[0];
        match RespType::from_byte(type_byte) {
            Some(RespType::SimpleString) => self.parse_simple_string(&data[1..]),
            Some(RespType::Error) => self.parse_error(&data[1..]),
            Some(RespType::Integer) => self.parse_integer(&data[1..]),
            Some(RespType::BulkString) => self.parse_bulk_string(&data[1..]),
            Some(RespType::Array) => self.parse_array(&data[1..]),
            Some(t) if t.is_resp3_only() => (
                ParseResult::Error(format!("RESP3 type {:?} not supported in RESP2", t)),
                0,
            ),
            _ => {
                // Try inline command parsing
                self.parse_inline(data)
            }
        }
    }

    /// Parse a simple string (+OK\r\n).
    fn parse_simple_string(&self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(pos) => {
                let s = String::from_utf8_lossy(&data[..pos]).to_string();
                (ParseResult::Ok(RedisValue::SimpleString(s)), pos + 3) // +1 for prefix, +2 for CRLF
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse an error (-ERR message\r\n).
    fn parse_error(&self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(pos) => {
                let s = String::from_utf8_lossy(&data[..pos]).to_string();
                let (kind, message) = parse_error_string(&s);
                (
                    ParseResult::Ok(RedisValue::Error(RedisError { kind, message })),
                    pos + 3,
                )
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse an integer (:1000\r\n).
    fn parse_integer(&self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(pos) => {
                let s = std::str::from_utf8(&data[..pos]).unwrap_or("");
                match s.parse::<i64>() {
                    Ok(n) => (ParseResult::Ok(RedisValue::Integer(n)), pos + 3),
                    Err(_) => (ParseResult::Error("invalid integer".to_string()), 0),
                }
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse a bulk string ($6\r\nfoobar\r\n).
    fn parse_bulk_string(&self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(len_pos) => {
                let len_str = std::str::from_utf8(&data[..len_pos]).unwrap_or("");
                match len_str.parse::<i64>() {
                    Ok(-1) => {
                        // Null bulk string
                        (ParseResult::Ok(RedisValue::Null), len_pos + 3)
                    }
                    Ok(len) if len >= 0 => {
                        let len = len as usize;
                        if len > MAX_BULK_SIZE {
                            return (
                                ParseResult::Error(format!(
                                    "bulk string too large: {} > {}",
                                    len, MAX_BULK_SIZE
                                )),
                                0,
                            );
                        }

                        let data_start = len_pos + 2; // After first CRLF
                        let data_end = data_start + len;

                        if data.len() < data_end + 2 {
                            // Need more data (including trailing CRLF)
                            return (ParseResult::Incomplete, 0);
                        }

                        // Verify trailing CRLF
                        if &data[data_end..data_end + 2] != b"\r\n" {
                            return (
                                ParseResult::Error("missing CRLF after bulk string".to_string()),
                                0,
                            );
                        }

                        let bytes = Bytes::copy_from_slice(&data[data_start..data_end]);
                        (
                            ParseResult::Ok(RedisValue::BulkString(bytes)),
                            data_end + 3, // +1 for prefix, +2 for trailing CRLF
                        )
                    }
                    Ok(_) => (
                        ParseResult::Error("invalid bulk string length".to_string()),
                        0,
                    ),
                    Err(_) => (
                        ParseResult::Error("invalid bulk string length".to_string()),
                        0,
                    ),
                }
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse an array (*2\r\n...).
    fn parse_array(&mut self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(len_pos) => {
                let len_str = std::str::from_utf8(&data[..len_pos]).unwrap_or("");
                match len_str.parse::<i64>() {
                    Ok(-1) => {
                        // Null array
                        (ParseResult::Ok(RedisValue::Null), len_pos + 3)
                    }
                    Ok(len) if len >= 0 => {
                        let len = len as usize;
                        if len > MAX_ELEMENTS {
                            return (
                                ParseResult::Error(format!(
                                    "array too large: {} > {}",
                                    len, MAX_ELEMENTS
                                )),
                                0,
                            );
                        }

                        self.depth += 1;

                        let mut elements = Vec::with_capacity(len);
                        let mut offset = len_pos + 2; // After first CRLF

                        for _ in 0..len {
                            if offset >= data.len() {
                                self.depth -= 1;
                                return (ParseResult::Incomplete, 0);
                            }

                            let (result, consumed) = self.parse_value(&data[offset..]);
                            match result {
                                ParseResult::Ok(value) => {
                                    elements.push(value);
                                    offset += consumed;
                                }
                                ParseResult::Incomplete => {
                                    self.depth -= 1;
                                    return (ParseResult::Incomplete, 0);
                                }
                                ParseResult::Error(e) => {
                                    self.depth -= 1;
                                    return (ParseResult::Error(e), 0);
                                }
                            }
                        }

                        self.depth -= 1;
                        (ParseResult::Ok(RedisValue::Array(elements)), offset + 1)
                        // +1 for prefix
                    }
                    Ok(_) => (ParseResult::Error("invalid array length".to_string()), 0),
                    Err(_) => (ParseResult::Error("invalid array length".to_string()), 0),
                }
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse inline command (PING\r\n or PING arg1 arg2\r\n).
    fn parse_inline(&self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(pos) => {
                let line = &data[..pos];
                let parts: Vec<&[u8]> = line
                    .split(|&b| b == b' ')
                    .filter(|p| !p.is_empty())
                    .collect();

                if parts.is_empty() {
                    return (ParseResult::Error("empty command".to_string()), 0);
                }

                let elements: Vec<RedisValue> = parts
                    .into_iter()
                    .map(|p| RedisValue::BulkString(Bytes::copy_from_slice(p)))
                    .collect();

                (ParseResult::Ok(RedisValue::Array(elements)), pos + 2)
            }
            None => (ParseResult::Incomplete, 0),
        }
    }
}

/// RESP2 encoder.
#[derive(Debug, Default)]
pub struct Resp2Encoder;

impl Resp2Encoder {
    /// Encode a value to bytes.
    pub fn encode(value: &RedisValue) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        Self::encode_into(value, &mut buf);
        buf
    }

    /// Encode a value into a buffer.
    pub fn encode_into(value: &RedisValue, buf: &mut Vec<u8>) {
        match value {
            RedisValue::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::Error(e) => {
                buf.push(b'-');
                buf.extend_from_slice(e.kind.as_bytes());
                buf.push(b' ');
                buf.extend_from_slice(e.message.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::BulkString(data) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::Array(elements) => {
                buf.push(b'*');
                buf.extend_from_slice(elements.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in elements {
                    Self::encode_into(elem, buf);
                }
            }
            RedisValue::Null => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            // RESP3 types encoded as RESP2 equivalents
            RedisValue::Boolean(b) => {
                // Encode as integer
                buf.extend_from_slice(if *b { b":1\r\n" } else { b":0\r\n" });
            }
            RedisValue::Double(d) => {
                // Encode as bulk string
                let s = if d.is_infinite() {
                    if *d > 0.0 {
                        "inf".to_string()
                    } else {
                        "-inf".to_string()
                    }
                } else {
                    d.to_string()
                };
                buf.push(b'$');
                buf.extend_from_slice(s.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::BigNumber(s) => {
                // Encode as bulk string
                buf.push(b'$');
                buf.extend_from_slice(s.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::BulkError(e) => {
                // Encode as simple error
                buf.push(b'-');
                buf.extend_from_slice(e.kind.as_bytes());
                buf.push(b' ');
                buf.extend_from_slice(e.message.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::VerbatimString { data, .. } => {
                // Encode as bulk string (drop format)
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::Map(pairs) => {
                // Encode as array of alternating key/value
                buf.push(b'*');
                buf.extend_from_slice((pairs.len() * 2).to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (k, v) in pairs {
                    Self::encode_into(k, buf);
                    Self::encode_into(v, buf);
                }
            }
            RedisValue::Set(elements) => {
                // Encode as array
                buf.push(b'*');
                buf.extend_from_slice(elements.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in elements {
                    Self::encode_into(elem, buf);
                }
            }
            RedisValue::Push(elements) => {
                // Encode as array
                buf.push(b'*');
                buf.extend_from_slice(elements.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in elements {
                    Self::encode_into(elem, buf);
                }
            }
        }
    }
}

/// Find CRLF in data, returning position of first \r.
fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

/// Parse error string into kind and message.
fn parse_error_string(s: &str) -> (String, String) {
    if let Some(pos) = s.find(' ') {
        (s[..pos].to_string(), s[pos + 1..].to_string())
    } else {
        (s.to_string(), String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut parser = Resp2Parser::new();
        let result = parser.parse(b"+OK\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::SimpleString(s)) if s == "OK"));
    }

    #[test]
    fn test_parse_error() {
        let mut parser = Resp2Parser::new();
        let result = parser.parse(b"-ERR unknown command\r\n");
        assert!(
            matches!(result, ParseResult::Ok(RedisValue::Error(e)) if e.kind == "ERR" && e.message == "unknown command")
        );
    }

    #[test]
    fn test_parse_integer() {
        let mut parser = Resp2Parser::new();

        let result = parser.parse(b":1000\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Integer(1000))));

        let result = parser.parse(b":-42\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Integer(-42))));
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut parser = Resp2Parser::new();

        let result = parser.parse(b"$6\r\nfoobar\r\n");
        assert!(
            matches!(result, ParseResult::Ok(RedisValue::BulkString(b)) if b.as_ref() == b"foobar")
        );

        let result = parser.parse(b"$0\r\n\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::BulkString(b)) if b.is_empty()));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let mut parser = Resp2Parser::new();
        let result = parser.parse(b"$-1\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Null)));
    }

    #[test]
    fn test_parse_array() {
        let mut parser = Resp2Parser::new();

        let result = parser.parse(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        if let ParseResult::Ok(RedisValue::Array(arr)) = result {
            assert_eq!(arr.len(), 2);
            assert!(matches!(&arr[0], RedisValue::BulkString(b) if b.as_ref() == b"foo"));
            assert!(matches!(&arr[1], RedisValue::BulkString(b) if b.as_ref() == b"bar"));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_parse_null_array() {
        let mut parser = Resp2Parser::new();
        let result = parser.parse(b"*-1\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Null)));
    }

    #[test]
    fn test_parse_empty_array() {
        let mut parser = Resp2Parser::new();
        let result = parser.parse(b"*0\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Array(arr)) if arr.is_empty()));
    }

    #[test]
    fn test_parse_nested_array() {
        let mut parser = Resp2Parser::new();
        let result = parser.parse(b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n");
        if let ParseResult::Ok(RedisValue::Array(arr)) = result {
            assert_eq!(arr.len(), 2);
            assert!(matches!(&arr[0], RedisValue::Array(inner) if inner.len() == 2));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_parse_inline_command() {
        let mut parser = Resp2Parser::new();

        let result = parser.parse(b"PING\r\n");
        if let ParseResult::Ok(RedisValue::Array(arr)) = result {
            assert_eq!(arr.len(), 1);
        } else {
            panic!("expected array");
        }

        let result = parser.parse(b"SET key value\r\n");
        if let ParseResult::Ok(RedisValue::Array(arr)) = result {
            assert_eq!(arr.len(), 3);
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_parse_incomplete() {
        let mut parser = Resp2Parser::new();

        assert!(matches!(parser.parse(b"+OK"), ParseResult::Incomplete));
        assert!(matches!(
            parser.parse(b"$6\r\nfoo"),
            ParseResult::Incomplete
        ));
        assert!(matches!(
            parser.parse(b"*2\r\n$3\r\nfoo\r\n"),
            ParseResult::Incomplete
        ));
    }

    #[test]
    fn test_encode_simple_string() {
        let value = RedisValue::SimpleString("OK".to_string());
        assert_eq!(Resp2Encoder::encode(&value), b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let value = RedisValue::Error(RedisError::new("ERR", "something wrong"));
        assert_eq!(Resp2Encoder::encode(&value), b"-ERR something wrong\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let value = RedisValue::Integer(42);
        assert_eq!(Resp2Encoder::encode(&value), b":42\r\n");

        let value = RedisValue::Integer(-100);
        assert_eq!(Resp2Encoder::encode(&value), b":-100\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let value = RedisValue::BulkString(Bytes::from("hello"));
        assert_eq!(Resp2Encoder::encode(&value), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null() {
        let value = RedisValue::Null;
        assert_eq!(Resp2Encoder::encode(&value), b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let value = RedisValue::Array(vec![
            RedisValue::BulkString(Bytes::from("foo")),
            RedisValue::BulkString(Bytes::from("bar")),
        ]);
        assert_eq!(
            Resp2Encoder::encode(&value),
            b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        );
    }

    #[test]
    fn test_roundtrip() {
        let mut parser = Resp2Parser::new();

        let values = vec![
            RedisValue::ok(),
            RedisValue::integer(42),
            RedisValue::bulk_string("hello"),
            RedisValue::Null,
            RedisValue::array(vec![RedisValue::integer(1), RedisValue::integer(2)]),
        ];

        for original in values {
            let encoded = Resp2Encoder::encode(&original);
            let parsed = parser.parse(&encoded);
            assert!(
                matches!(parsed, ParseResult::Ok(ref v) if v == &original),
                "roundtrip failed for {:?}",
                original
            );
        }
    }
}
