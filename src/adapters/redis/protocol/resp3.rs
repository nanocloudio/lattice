//! RESP3 protocol parser and encoder.
//!
//! RESP3 extends RESP2 with additional types:
//! - Null (_)
//! - Boolean (#)
//! - Double (,)
//! - Big Number (()
//! - Bulk Error (!)
//! - Verbatim String (=)
//! - Map (%)
//! - Set (~)
//! - Attribute (|)
//! - Push (>)

use super::{ParseResult, RespType, MAX_BULK_SIZE, MAX_ELEMENTS, MAX_NESTING_DEPTH};
use crate::adapters::redis::{RedisError, RedisValue};
use bytes::Bytes;

/// RESP3 parser.
#[derive(Debug, Default)]
pub struct Resp3Parser {
    /// Current nesting depth.
    depth: usize,
}

impl Resp3Parser {
    /// Create a new parser.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a value from the buffer.
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
            Some(RespType::SimpleString) => {
                self.parse_simple_line(&data[1..], RedisValue::SimpleString)
            }
            Some(RespType::Error) => self.parse_simple_line(&data[1..], |s| {
                let (kind, message) = parse_error_string(&s);
                RedisValue::Error(RedisError { kind, message })
            }),
            Some(RespType::Integer) => self.parse_simple_line(&data[1..], |s| {
                s.parse::<i64>()
                    .map(RedisValue::Integer)
                    .unwrap_or_else(|_| RedisValue::Error(RedisError::generic("invalid integer")))
            }),
            Some(RespType::BulkString) => self.parse_bulk(&data[1..], false),
            Some(RespType::Array) => self.parse_aggregate(&data[1..], RespType::Array),
            Some(RespType::Null) => self.parse_null(&data[1..]),
            Some(RespType::Boolean) => self.parse_boolean(&data[1..]),
            Some(RespType::Double) => self.parse_double(&data[1..]),
            Some(RespType::BigNumber) => self.parse_simple_line(&data[1..], RedisValue::BigNumber),
            Some(RespType::BulkError) => self.parse_bulk(&data[1..], true),
            Some(RespType::VerbatimString) => self.parse_verbatim(&data[1..]),
            Some(RespType::Map) => self.parse_map(&data[1..]),
            Some(RespType::Set) => self.parse_aggregate(&data[1..], RespType::Set),
            Some(RespType::Attribute) => self.parse_attribute(&data[1..]),
            Some(RespType::Push) => self.parse_aggregate(&data[1..], RespType::Push),
            None => {
                // Try inline command parsing
                self.parse_inline(data)
            }
        }
    }

    /// Parse a simple line-based value.
    fn parse_simple_line<F>(&self, data: &[u8], f: F) -> (ParseResult, usize)
    where
        F: FnOnce(String) -> RedisValue,
    {
        match find_crlf(data) {
            Some(pos) => {
                let s = String::from_utf8_lossy(&data[..pos]).to_string();
                (ParseResult::Ok(f(s)), pos + 3) // +1 prefix, +2 CRLF
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse null (_\r\n).
    fn parse_null(&self, data: &[u8]) -> (ParseResult, usize) {
        if data.len() < 2 {
            return (ParseResult::Incomplete, 0);
        }
        if &data[..2] == b"\r\n" {
            (ParseResult::Ok(RedisValue::Null), 3) // +1 prefix
        } else {
            (ParseResult::Error("invalid null".to_string()), 0)
        }
    }

    /// Parse boolean (#t\r\n or #f\r\n).
    fn parse_boolean(&self, data: &[u8]) -> (ParseResult, usize) {
        if data.len() < 3 {
            return (ParseResult::Incomplete, 0);
        }
        match &data[..3] {
            b"t\r\n" => (ParseResult::Ok(RedisValue::Boolean(true)), 4),
            b"f\r\n" => (ParseResult::Ok(RedisValue::Boolean(false)), 4),
            _ => (ParseResult::Error("invalid boolean".to_string()), 0),
        }
    }

    /// Parse double (,1.23\r\n).
    fn parse_double(&self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(pos) => {
                let s = std::str::from_utf8(&data[..pos]).unwrap_or("");
                let d = match s {
                    "inf" | "+inf" => f64::INFINITY,
                    "-inf" => f64::NEG_INFINITY,
                    "nan" => f64::NAN,
                    _ => match s.parse::<f64>() {
                        Ok(d) => d,
                        Err(_) => return (ParseResult::Error("invalid double".to_string()), 0),
                    },
                };
                (ParseResult::Ok(RedisValue::Double(d)), pos + 3)
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse bulk string or bulk error.
    fn parse_bulk(&self, data: &[u8], is_error: bool) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(len_pos) => {
                let len_str = std::str::from_utf8(&data[..len_pos]).unwrap_or("");
                match len_str.parse::<i64>() {
                    Ok(-1) => {
                        // Null
                        (ParseResult::Ok(RedisValue::Null), len_pos + 3)
                    }
                    Ok(len) if len >= 0 => {
                        let len = len as usize;
                        if len > MAX_BULK_SIZE {
                            return (ParseResult::Error("bulk string too large".to_string()), 0);
                        }

                        let data_start = len_pos + 2;
                        let data_end = data_start + len;

                        if data.len() < data_end + 2 {
                            return (ParseResult::Incomplete, 0);
                        }

                        if &data[data_end..data_end + 2] != b"\r\n" {
                            return (ParseResult::Error("missing CRLF".to_string()), 0);
                        }

                        let bytes = Bytes::copy_from_slice(&data[data_start..data_end]);
                        let value = if is_error {
                            let s = String::from_utf8_lossy(&bytes).to_string();
                            let (kind, message) = parse_error_string(&s);
                            RedisValue::BulkError(RedisError { kind, message })
                        } else {
                            RedisValue::BulkString(bytes)
                        };

                        (ParseResult::Ok(value), data_end + 3)
                    }
                    _ => (ParseResult::Error("invalid length".to_string()), 0),
                }
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse verbatim string (=15\r\ntxt:Some text\r\n).
    fn parse_verbatim(&self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(len_pos) => {
                let len_str = std::str::from_utf8(&data[..len_pos]).unwrap_or("");
                match len_str.parse::<usize>() {
                    Ok(len) if len >= 4 => {
                        let data_start = len_pos + 2;
                        let data_end = data_start + len;

                        if data.len() < data_end + 2 {
                            return (ParseResult::Incomplete, 0);
                        }

                        if &data[data_end..data_end + 2] != b"\r\n" {
                            return (ParseResult::Error("missing CRLF".to_string()), 0);
                        }

                        // Format is first 3 bytes + ':'
                        let format_bytes = &data[data_start..data_start + 3];
                        if data[data_start + 3] != b':' {
                            return (ParseResult::Error("invalid verbatim format".to_string()), 0);
                        }

                        let format = String::from_utf8_lossy(format_bytes).to_string();
                        let content = Bytes::copy_from_slice(&data[data_start + 4..data_end]);

                        (
                            ParseResult::Ok(RedisValue::VerbatimString {
                                format,
                                data: content,
                            }),
                            data_end + 3,
                        )
                    }
                    _ => (ParseResult::Error("invalid verbatim length".to_string()), 0),
                }
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse aggregate types (array, set, push).
    fn parse_aggregate(&mut self, data: &[u8], typ: RespType) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(len_pos) => {
                let len_str = std::str::from_utf8(&data[..len_pos]).unwrap_or("");
                match len_str.parse::<i64>() {
                    Ok(-1) => (ParseResult::Ok(RedisValue::Null), len_pos + 3),
                    Ok(len) if len >= 0 => {
                        let len = len as usize;
                        if len > MAX_ELEMENTS {
                            return (ParseResult::Error("too many elements".to_string()), 0);
                        }

                        self.depth += 1;
                        let mut elements = Vec::with_capacity(len);
                        let mut offset = len_pos + 2;

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
                        let value = match typ {
                            RespType::Array => RedisValue::Array(elements),
                            RespType::Set => RedisValue::Set(elements),
                            RespType::Push => RedisValue::Push(elements),
                            _ => unreachable!(),
                        };
                        (ParseResult::Ok(value), offset + 1)
                    }
                    _ => (ParseResult::Error("invalid length".to_string()), 0),
                }
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse map (%2\r\n...).
    fn parse_map(&mut self, data: &[u8]) -> (ParseResult, usize) {
        match find_crlf(data) {
            Some(len_pos) => {
                let len_str = std::str::from_utf8(&data[..len_pos]).unwrap_or("");
                match len_str.parse::<i64>() {
                    Ok(-1) => (ParseResult::Ok(RedisValue::Null), len_pos + 3),
                    Ok(len) if len >= 0 => {
                        let len = len as usize;
                        if len > MAX_ELEMENTS / 2 {
                            return (ParseResult::Error("too many elements".to_string()), 0);
                        }

                        self.depth += 1;
                        let mut pairs = Vec::with_capacity(len);
                        let mut offset = len_pos + 2;

                        for _ in 0..len {
                            // Parse key
                            if offset >= data.len() {
                                self.depth -= 1;
                                return (ParseResult::Incomplete, 0);
                            }
                            let (key_result, key_consumed) = self.parse_value(&data[offset..]);
                            let key = match key_result {
                                ParseResult::Ok(v) => v,
                                ParseResult::Incomplete => {
                                    self.depth -= 1;
                                    return (ParseResult::Incomplete, 0);
                                }
                                ParseResult::Error(e) => {
                                    self.depth -= 1;
                                    return (ParseResult::Error(e), 0);
                                }
                            };
                            offset += key_consumed;

                            // Parse value
                            if offset >= data.len() {
                                self.depth -= 1;
                                return (ParseResult::Incomplete, 0);
                            }
                            let (val_result, val_consumed) = self.parse_value(&data[offset..]);
                            let value = match val_result {
                                ParseResult::Ok(v) => v,
                                ParseResult::Incomplete => {
                                    self.depth -= 1;
                                    return (ParseResult::Incomplete, 0);
                                }
                                ParseResult::Error(e) => {
                                    self.depth -= 1;
                                    return (ParseResult::Error(e), 0);
                                }
                            };
                            offset += val_consumed;

                            pairs.push((key, value));
                        }

                        self.depth -= 1;
                        (ParseResult::Ok(RedisValue::Map(pairs)), offset + 1)
                    }
                    _ => (ParseResult::Error("invalid length".to_string()), 0),
                }
            }
            None => (ParseResult::Incomplete, 0),
        }
    }

    /// Parse attribute (|1\r\n...).
    ///
    /// Attributes precede the actual response. We parse and discard them,
    /// returning just the following value.
    fn parse_attribute(&mut self, data: &[u8]) -> (ParseResult, usize) {
        // First parse the attribute map
        let (attr_result, attr_consumed) = self.parse_map(data);
        match attr_result {
            ParseResult::Ok(_) => {
                // Now parse the actual value that follows
                if attr_consumed >= data.len() {
                    return (ParseResult::Incomplete, 0);
                }
                let (value_result, value_consumed) = self.parse_value(&data[attr_consumed - 1..]);
                match value_result {
                    ParseResult::Ok(v) => (ParseResult::Ok(v), attr_consumed + value_consumed - 1),
                    other => (other, 0),
                }
            }
            other => (other, 0),
        }
    }

    /// Parse inline command.
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

/// RESP3 encoder.
#[derive(Debug, Default)]
pub struct Resp3Encoder;

impl Resp3Encoder {
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
                buf.extend_from_slice(b"_\r\n");
            }
            RedisValue::Boolean(b) => {
                buf.push(b'#');
                buf.push(if *b { b't' } else { b'f' });
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::Double(d) => {
                buf.push(b',');
                let s = if d.is_nan() {
                    "nan".to_string()
                } else if d.is_infinite() {
                    if *d > 0.0 {
                        "inf".to_string()
                    } else {
                        "-inf".to_string()
                    }
                } else {
                    d.to_string()
                };
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::BigNumber(s) => {
                buf.push(b'(');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::BulkError(e) => {
                let msg = format!("{} {}", e.kind, e.message);
                buf.push(b'!');
                buf.extend_from_slice(msg.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(msg.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::VerbatimString { format, data } => {
                let total_len = 4 + data.len(); // format:data
                buf.push(b'=');
                buf.extend_from_slice(total_len.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(format.as_bytes());
                buf.push(b':');
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            RedisValue::Map(pairs) => {
                buf.push(b'%');
                buf.extend_from_slice(pairs.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for (k, v) in pairs {
                    Self::encode_into(k, buf);
                    Self::encode_into(v, buf);
                }
            }
            RedisValue::Set(elements) => {
                buf.push(b'~');
                buf.extend_from_slice(elements.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in elements {
                    Self::encode_into(elem, buf);
                }
            }
            RedisValue::Push(elements) => {
                buf.push(b'>');
                buf.extend_from_slice(elements.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for elem in elements {
                    Self::encode_into(elem, buf);
                }
            }
        }
    }
}

/// Find CRLF in data.
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
    fn test_parse_null() {
        let mut parser = Resp3Parser::new();
        let result = parser.parse(b"_\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Null)));
    }

    #[test]
    fn test_parse_boolean() {
        let mut parser = Resp3Parser::new();

        let result = parser.parse(b"#t\r\n");
        assert!(matches!(result, ParseResult::Ok(RedisValue::Boolean(true))));

        let result = parser.parse(b"#f\r\n");
        assert!(matches!(
            result,
            ParseResult::Ok(RedisValue::Boolean(false))
        ));
    }

    #[test]
    fn test_parse_double() {
        let mut parser = Resp3Parser::new();

        let result = parser.parse(b",1.5\r\n");
        assert!(
            matches!(result, ParseResult::Ok(RedisValue::Double(d)) if (d - 1.5).abs() < 0.001)
        );

        let result = parser.parse(b",inf\r\n");
        assert!(
            matches!(result, ParseResult::Ok(RedisValue::Double(d)) if d.is_infinite() && d > 0.0)
        );

        let result = parser.parse(b",-inf\r\n");
        assert!(
            matches!(result, ParseResult::Ok(RedisValue::Double(d)) if d.is_infinite() && d < 0.0)
        );
    }

    #[test]
    fn test_parse_big_number() {
        let mut parser = Resp3Parser::new();
        let result = parser.parse(b"(3492890328409238509324850943850943825024385\r\n");
        assert!(
            matches!(result, ParseResult::Ok(RedisValue::BigNumber(s)) if s == "3492890328409238509324850943850943825024385")
        );
    }

    #[test]
    fn test_parse_verbatim() {
        let mut parser = Resp3Parser::new();
        let result = parser.parse(b"=15\r\ntxt:Hello World\r\n");
        assert!(
            matches!(result, ParseResult::Ok(RedisValue::VerbatimString { format, data }) if format == "txt" && data.as_ref() == b"Hello World")
        );
    }

    #[test]
    fn test_parse_map() {
        let mut parser = Resp3Parser::new();
        let result = parser.parse(b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
        if let ParseResult::Ok(RedisValue::Map(pairs)) = result {
            assert_eq!(pairs.len(), 2);
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn test_parse_set() {
        let mut parser = Resp3Parser::new();
        let result = parser.parse(b"~3\r\n+orange\r\n+apple\r\n+banana\r\n");
        if let ParseResult::Ok(RedisValue::Set(elements)) = result {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("expected set");
        }
    }

    #[test]
    fn test_parse_push() {
        let mut parser = Resp3Parser::new();
        let result = parser.parse(b">2\r\n+message\r\n+hello\r\n");
        if let ParseResult::Ok(RedisValue::Push(elements)) = result {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("expected push");
        }
    }

    #[test]
    fn test_encode_null() {
        let value = RedisValue::Null;
        assert_eq!(Resp3Encoder::encode(&value), b"_\r\n");
    }

    #[test]
    fn test_encode_boolean() {
        assert_eq!(Resp3Encoder::encode(&RedisValue::Boolean(true)), b"#t\r\n");
        assert_eq!(Resp3Encoder::encode(&RedisValue::Boolean(false)), b"#f\r\n");
    }

    #[test]
    fn test_encode_double() {
        let value = RedisValue::Double(1.5);
        assert_eq!(Resp3Encoder::encode(&value), b",1.5\r\n");
    }

    #[test]
    fn test_encode_map() {
        let value = RedisValue::Map(vec![(
            RedisValue::simple_string("key"),
            RedisValue::integer(42),
        )]);
        assert_eq!(Resp3Encoder::encode(&value), b"%1\r\n+key\r\n:42\r\n");
    }

    #[test]
    fn test_encode_set() {
        let value = RedisValue::Set(vec![RedisValue::integer(1), RedisValue::integer(2)]);
        assert_eq!(Resp3Encoder::encode(&value), b"~2\r\n:1\r\n:2\r\n");
    }

    #[test]
    fn test_roundtrip() {
        let mut parser = Resp3Parser::new();

        let values = vec![
            RedisValue::Null,
            RedisValue::Boolean(true),
            RedisValue::Boolean(false),
            RedisValue::Double(std::f64::consts::PI),
            RedisValue::BigNumber("12345678901234567890".to_string()),
            RedisValue::Map(vec![(
                RedisValue::simple_string("a"),
                RedisValue::integer(1),
            )]),
            RedisValue::Set(vec![RedisValue::integer(1), RedisValue::integer(2)]),
        ];

        for original in values {
            let encoded = Resp3Encoder::encode(&original);
            let parsed = parser.parse(&encoded);
            assert!(
                matches!(parsed, ParseResult::Ok(ref v) if v == &original),
                "roundtrip failed for {:?}",
                original
            );
        }
    }
}
