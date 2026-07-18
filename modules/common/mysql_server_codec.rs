//! MySQL/MariaDB client-server protocol, SERVER side (RFC database
//! foundation §14.3 — the connector owns its wire handshake,
//! capabilities, authentication negotiation, text flow, dialect surface,
//! error mapping and result encoding).
//!
//! Dual-target facade: compiles into the no_std `mysql_edge_anchor` and
//! is host-tested here. The counterpart to `pg_server_codec`; the two
//! share `sql_exec` and nothing else, because the protocols agree on
//! almost nothing at the byte level.
//!
//! Not to be confused with `mysql_core.rs`, which is the CLIENT side
//! (the outbound `mysql_client` module talking to a real MySQL).
//!
//! ## Framing
//!
//! Every packet is `[len:u24 LE][seq:u8][payload]`, where `len` counts
//! ONLY the payload. Two consequences drive the code here:
//!
//! - **The sequence number is protocol state, not decoration.** It
//!   starts at 0 for each command, increments per packet, and a client
//!   that receives an out-of-order sequence closes the connection. So
//!   the sequence lives in the session and every writer takes it.
//! - **A payload of exactly 0xFFFFFF means "more follows".** This codec
//!   refuses to emit one rather than splitting, because a caller that
//!   did not know it had been split would produce a stream the client
//!   misreads. Nothing here approaches 16 MB.
//!
//! ## Length-encoded integers
//!
//! MySQL's variable-width integer appears in every result-set packet,
//! and its 0xFB/0xFC/0xFD/0xFE prefixes overlap with NULL and EOF
//! markers in ways that depend on context. Encoded in one place
//! ([`write_lenenc_int`]) so the overlap is reasoned about once.

// `sql_exec` is mounted here so this file, the executor and the anchor
// share ONE `relational` tree — a second `#[path]` mount would produce
// types that will not unify.
#[path = "sql_exec.rs"]
pub mod sql_exec;

pub use sql_exec::relational;

use relational::{LogicalType, Value};

// ── Packet framing ────────────────────────────────────────────────────

/// Bytes of packet header: `[len:u24 LE][seq:u8]`.
pub const PACKET_HEADER: usize = 4;

/// The payload length that means "a continuation packet follows". This
/// codec never emits one — see the module docs.
pub const MAX_PAYLOAD: usize = 0xFF_FFFF;

// ── Capability flags ──────────────────────────────────────────────────
//
// A capability claimed here CHANGES how the client frames its packets,
// so claiming one that is not implemented corrupts the stream rather
// than degrading a feature.

pub const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
pub const CLIENT_FOUND_ROWS: u32 = 0x0000_0002;
pub const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
pub const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
pub const CLIENT_NO_SCHEMA: u32 = 0x0000_0010;
pub const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
pub const CLIENT_INTERACTIVE: u32 = 0x0000_0400;
pub const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CLIENT_CONNECT_ATTRS: u32 = 0x0010_0000;
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
pub const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;

/// What the server advertises.
///
/// `CLIENT_DEPRECATE_EOF` is deliberately NOT advertised. With it, a
/// result set ends with an OK packet instead of an EOF packet, and the
/// two are distinguished by a header byte that also collides with a
/// length-encoded integer prefix. Supporting the older, unambiguous
/// framing is the honest choice while the result-set path is new; a
/// client that prefers EOF deprecation simply does not get it.
///
/// `CLIENT_PROTOCOL_41` is mandatory — every modern client requires it,
/// and the pre-4.1 framing is a different protocol.
pub const SERVER_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH;

/// Status flags in OK/EOF packets.
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;

/// Commands.
pub const COM_QUIT: u8 = 0x01;
pub const COM_INIT_DB: u8 = 0x02;
pub const COM_QUERY: u8 = 0x03;
pub const COM_FIELD_LIST: u8 = 0x04;
pub const COM_PING: u8 = 0x0E;
pub const COM_STMT_PREPARE: u8 = 0x16;
pub const COM_STMT_EXECUTE: u8 = 0x17;
pub const COM_STMT_CLOSE: u8 = 0x19;

/// Packet-leading markers.
pub const OK_HEADER: u8 = 0x00;
pub const EOF_HEADER: u8 = 0xFE;
pub const ERR_HEADER: u8 = 0xFF;
/// A length-encoded string whose length byte is 0xFB is SQL NULL. The
/// same byte begins a 2-byte length-encoded integer in other positions,
/// which is why context matters and the encoder is centralised.
pub const NULL_LENENC: u8 = 0xFB;

// ── Column types ──────────────────────────────────────────────────────
//
// A client parses a text-protocol cell using the column's declared type,
// so a wrong type makes a correct value unreadable.

pub const MYSQL_TYPE_TINY: u8 = 0x01;
pub const MYSQL_TYPE_SHORT: u8 = 0x02;
pub const MYSQL_TYPE_LONG: u8 = 0x03;
pub const MYSQL_TYPE_FLOAT: u8 = 0x04;
pub const MYSQL_TYPE_DOUBLE: u8 = 0x05;
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;
pub const MYSQL_TYPE_DATE: u8 = 0x0A;
pub const MYSQL_TYPE_TIME: u8 = 0x0B;
pub const MYSQL_TYPE_DATETIME: u8 = 0x0C;
pub const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
pub const MYSQL_TYPE_NEWDECIMAL: u8 = 0xF6;
pub const MYSQL_TYPE_BLOB: u8 = 0xFC;
pub const MYSQL_TYPE_VAR_STRING: u8 = 0xFD;
pub const MYSQL_TYPE_STRING: u8 = 0xFE;

/// Column flags.
pub const COLUMN_NOT_NULL_FLAG: u16 = 0x0001;
pub const COLUMN_PRI_KEY_FLAG: u16 = 0x0002;
pub const COLUMN_BINARY_FLAG: u16 = 0x0080;

/// Character set ids. `binary` for BLOB columns, `utf8mb4_general_ci`
/// for everything else — a text column tagged binary makes a client
/// render it as hex.
pub const CHARSET_BINARY: u16 = 63;
pub const CHARSET_UTF8MB4: u16 = 45;

// ── Writer ────────────────────────────────────────────────────────────

/// Appends MySQL packets to a buffer, owning the sequence number.
///
/// The sequence is threaded through rather than tracked globally because
/// it resets per command: a writer is created for one command's reply
/// with that command's next sequence, and hands back where it ended.
pub struct Writer<'b> {
    buf: &'b mut [u8],
    len: usize,
    seq: u8,
    /// Offset of the current packet's header, when one is open.
    open_at: Option<usize>,
    overflow: bool,
}

impl<'b> Writer<'b> {
    pub fn new(buf: &'b mut [u8], seq: u8) -> Self {
        Self {
            buf,
            len: 0,
            seq,
            open_at: None,
            overflow: false,
        }
    }

    pub fn seq(&self) -> u8 {
        self.seq
    }

    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    /// Finished bytes, or `None` if anything overflowed or a packet was
    /// left open. Both desync the client permanently, since it frames by
    /// length.
    pub fn finished(&self) -> Option<&[u8]> {
        if self.overflow || self.open_at.is_some() {
            return None;
        }
        Some(&self.buf[..self.len])
    }

    fn room(&mut self, n: usize) -> bool {
        if self.len + n > self.buf.len() {
            self.overflow = true;
            return false;
        }
        true
    }

    /// Begin a packet, reserving its header and consuming a sequence
    /// number.
    pub fn begin(&mut self) -> &mut Self {
        if self.open_at.is_some() {
            self.overflow = true;
            return self;
        }
        if !self.room(PACKET_HEADER) {
            return self;
        }
        self.open_at = Some(self.len);
        self.buf[self.len..self.len + 3].copy_from_slice(&[0, 0, 0]);
        self.buf[self.len + 3] = self.seq;
        self.seq = self.seq.wrapping_add(1);
        self.len += PACKET_HEADER;
        self
    }

    /// Close the open packet, backfilling its payload length.
    pub fn end(&mut self) -> &mut Self {
        let Some(at) = self.open_at.take() else {
            self.overflow = true;
            return self;
        };
        let body = self.len - at - PACKET_HEADER;
        if body >= MAX_PAYLOAD {
            // A 0xFFFFFF payload means "continued", and emitting one
            // without the continuation would make the client wait
            // forever for a packet that never comes.
            self.overflow = true;
            return self;
        }
        self.buf[at] = (body & 0xFF) as u8;
        self.buf[at + 1] = ((body >> 8) & 0xFF) as u8;
        self.buf[at + 2] = ((body >> 16) & 0xFF) as u8;
        self
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        if self.room(1) {
            self.buf[self.len] = v;
            self.len += 1;
        }
        self
    }

    pub fn u16(&mut self, v: u16) -> &mut Self {
        if self.room(2) {
            self.buf[self.len..self.len + 2].copy_from_slice(&v.to_le_bytes());
            self.len += 2;
        }
        self
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        if self.room(4) {
            self.buf[self.len..self.len + 4].copy_from_slice(&v.to_le_bytes());
            self.len += 4;
        }
        self
    }

    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        if self.room(v.len()) {
            self.buf[self.len..self.len + v.len()].copy_from_slice(v);
            self.len += v.len();
        }
        self
    }

    /// NUL-terminated string.
    pub fn cstr(&mut self, v: &[u8]) -> &mut Self {
        self.bytes(v).u8(0)
    }

    /// Length-encoded integer.
    pub fn lenenc_int(&mut self, v: u64) -> &mut Self {
        write_lenenc_int(self, v);
        self
    }

    /// Length-encoded string.
    pub fn lenenc_str(&mut self, v: &[u8]) -> &mut Self {
        self.lenenc_int(v.len() as u64).bytes(v)
    }

    /// A NULL cell in a text-protocol row: the single byte 0xFB, with no
    /// length and no payload. Distinct from a zero-length string, which
    /// is the byte 0x00 — `NULL` and `''` are different answers.
    pub fn null_cell(&mut self) -> &mut Self {
        self.u8(NULL_LENENC)
    }
}

/// The length-encoded integer encoding, in one place.
///
/// The prefixes collide with other markers by design of the protocol:
/// 0xFB also means NULL in a row cell, and 0xFE also begins an EOF
/// packet. Which meaning applies is positional, so centralising the
/// encoder is what keeps the reasoning in one spot rather than at every
/// call site.
fn write_lenenc_int(w: &mut Writer<'_>, v: u64) {
    if v < 0xFB {
        w.u8(v as u8);
    } else if v <= u64::from(u16::MAX) {
        w.u8(0xFC).u16(v as u16);
    } else if v <= 0xFF_FFFF {
        w.u8(0xFD).bytes(&[
            (v & 0xFF) as u8,
            ((v >> 8) & 0xFF) as u8,
            ((v >> 16) & 0xFF) as u8,
        ]);
    } else {
        w.u8(0xFE).bytes(&v.to_le_bytes());
    }
}

// ── Server packets ────────────────────────────────────────────────────

/// The initial handshake (protocol version 10).
///
/// `scramble` is the 20-byte auth challenge. It is sent even when no
/// password is configured, because the packet layout requires it and a
/// client computes its response before it can learn that authentication
/// is open.
pub fn handshake(w: &mut Writer<'_>, connection_id: u32, scramble: &[u8; 20], version: &[u8]) {
    let caps = SERVER_CAPABILITIES;
    w.begin()
        .u8(10) // protocol version
        .cstr(version)
        .u32(connection_id)
        // auth-plugin-data-part-1: the first 8 scramble bytes, then a
        // filler zero the layout requires.
        .bytes(&scramble[..8])
        .u8(0)
        .u16((caps & 0xFFFF) as u16)
        .u8(CHARSET_UTF8MB4 as u8)
        .u16(SERVER_STATUS_AUTOCOMMIT)
        .u16(((caps >> 16) & 0xFFFF) as u16)
        // auth-plugin-data length: the full scramble plus its
        // terminator. Clients validate this against what follows.
        .u8(21)
        .bytes(&[0u8; 10])
        // part-2: the remaining 12 bytes plus a NUL.
        .bytes(&scramble[8..20])
        .u8(0)
        .cstr(b"mysql_native_password")
        .end();
}

/// An OK packet.
pub fn ok_packet(w: &mut Writer<'_>, affected: u64, last_insert_id: u64, status: u16) {
    w.begin()
        .u8(OK_HEADER)
        .lenenc_int(affected)
        .lenenc_int(last_insert_id)
        .u16(status)
        .u16(0) // warnings
        .end();
}

/// An EOF packet. Sent after the column definitions and after the rows,
/// because `CLIENT_DEPRECATE_EOF` is not advertised.
pub fn eof_packet(w: &mut Writer<'_>, status: u16) {
    w.begin()
        .u8(EOF_HEADER)
        .u16(0) // warnings
        .u16(status)
        .end();
}

/// An ERR packet.
///
/// The `#` before the SQLSTATE is a literal marker the protocol
/// requires; without it a client reads the first five characters of the
/// message as the state.
pub fn err_packet(w: &mut Writer<'_>, code: u16, sqlstate: &[u8], message: &[u8]) {
    w.begin()
        .u8(ERR_HEADER)
        .u16(code)
        .u8(b'#')
        .bytes(sqlstate)
        .bytes(message)
        .end();
}

/// A column-count packet, which introduces a result set.
pub fn column_count(w: &mut Writer<'_>, n: u64) {
    w.begin().lenenc_int(n).end();
}

/// One column definition (protocol 41).
/// One column's metadata, bundled because the fields travel together
/// and an eight-long positional list is where a charset/flags
/// transposition hides.
pub struct ColumnDef<'a> {
    pub name: &'a [u8],
    pub table: &'a [u8],
    pub column_type: u8,
    pub charset: u16,
    pub length: u32,
    pub flags: u16,
    pub decimals: u8,
}

pub fn column_definition(w: &mut Writer<'_>, c: &ColumnDef<'_>) {
    let ColumnDef {
        name,
        table,
        column_type,
        charset,
        length,
        flags,
        decimals,
    } = *c;
    w.begin()
        .lenenc_str(b"def") // catalog: always this literal
        .lenenc_str(b"") // schema
        .lenenc_str(table) // table
        .lenenc_str(table) // org_table
        .lenenc_str(name) // name
        .lenenc_str(name) // org_name
        .lenenc_int(0x0C) // length of the fixed block that follows
        .u16(charset)
        .u32(length)
        .u8(column_type)
        .u16(flags)
        .u8(decimals)
        .u16(0) // filler
        .end();
}

// ── Type mapping ──────────────────────────────────────────────────────

/// MySQL's column type for a logical type.
pub fn column_type(ty: LogicalType) -> u8 {
    match ty {
        // MySQL has no boolean: it is TINYINT(1), and clients render it
        // as 0/1. Claiming anything else would make a client display a
        // value the server never sent.
        LogicalType::Boolean => MYSQL_TYPE_TINY,
        LogicalType::SmallInt => MYSQL_TYPE_SHORT,
        LogicalType::Int => MYSQL_TYPE_LONG,
        LogicalType::BigInt => MYSQL_TYPE_LONGLONG,
        LogicalType::Decimal { .. } => MYSQL_TYPE_NEWDECIMAL,
        LogicalType::Float => MYSQL_TYPE_FLOAT,
        LogicalType::Double => MYSQL_TYPE_DOUBLE,
        LogicalType::Char { .. } => MYSQL_TYPE_STRING,
        LogicalType::VarChar { .. } | LogicalType::Null => MYSQL_TYPE_VAR_STRING,
        LogicalType::Binary { .. } | LogicalType::VarBinary { .. } => MYSQL_TYPE_BLOB,
        LogicalType::Date => MYSQL_TYPE_DATE,
        LogicalType::Time => MYSQL_TYPE_TIME,
        LogicalType::Timestamp => MYSQL_TYPE_DATETIME,
        LogicalType::TimestampTz => MYSQL_TYPE_TIMESTAMP,
    }
}

/// The charset a column is declared with. Binary columns must say
/// `binary`, or a client renders their bytes as if they were text.
pub fn column_charset(ty: LogicalType) -> u16 {
    match ty {
        LogicalType::Binary { .. } | LogicalType::VarBinary { .. } => CHARSET_BINARY,
        _ => CHARSET_UTF8MB4,
    }
}

/// The declared display length.
pub fn column_length(ty: LogicalType) -> u32 {
    match ty {
        LogicalType::Boolean => 1,
        LogicalType::SmallInt => 6,
        LogicalType::Int => 11,
        LogicalType::BigInt => 20,
        LogicalType::Float => 12,
        LogicalType::Double => 22,
        LogicalType::Char { length }
        | LogicalType::VarChar { length }
        | LogicalType::Binary { length }
        | LogicalType::VarBinary { length } => {
            // utf8mb4 counts up to four bytes per character, and clients
            // size their display columns from this.
            u32::from(length) * 4
        }
        LogicalType::Decimal { precision, .. } => u32::from(precision) + 2,
        LogicalType::Date => 10,
        LogicalType::Time => 8,
        LogicalType::Timestamp | LogicalType::TimestampTz => 19,
        LogicalType::Null => 0,
    }
}

// ── Value rendering ───────────────────────────────────────────────────

fn itoa(v: i64, out: &mut [u8]) -> Option<usize> {
    if v == 0 {
        *out.get_mut(0)? = b'0';
        return Some(1);
    }
    let neg = v < 0;
    // Magnitude via u64 so i64::MIN, whose positive is not
    // representable, does not overflow.
    let mut mag = if neg {
        (v as i128).unsigned_abs() as u64
    } else {
        v as u64
    };
    let mut tmp = [0u8; 20];
    let mut n = 0usize;
    while mag > 0 {
        tmp[n] = b'0' + (mag % 10) as u8;
        mag /= 10;
        n += 1;
    }
    let total = n + usize::from(neg);
    if out.len() < total {
        return None;
    }
    let mut at = 0usize;
    if neg {
        out[0] = b'-';
        at = 1;
    }
    for i in 0..n {
        out[at + i] = tmp[n - 1 - i];
    }
    Some(total)
}

/// Render a value as MySQL text.
///
/// The connector half of the §14.3 split, and the place the two
/// connectors visibly differ: a boolean is `1`/`0` here where PostgreSQL
/// writes `t`/`f`. Binary values are sent RAW rather than hex-escaped,
/// because the column is declared with the `binary` charset and the
/// client is expected to take the bytes as they are.
pub fn render_value(ty: LogicalType, v: Value<'_>, out: &mut [u8]) -> Option<usize> {
    let _ = ty;
    match v {
        // NULL has no rendering: it is the 0xFB marker, and a caller
        // that reached here with one has confused absence with emptiness.
        Value::Null => None,
        Value::Boolean(b) => {
            *out.get_mut(0)? = if b { b'1' } else { b'0' };
            Some(1)
        }
        Value::SmallInt(x) => itoa(i64::from(x), out),
        Value::Int(x) => itoa(i64::from(x), out),
        Value::BigInt(x) => itoa(x, out),
        Value::Float(x) => render_f64(f64::from(x), out),
        Value::Double(x) => render_f64(x, out),
        Value::Text(b) | Value::Bytes(b) => {
            if out.len() < b.len() {
                return None;
            }
            out[..b.len()].copy_from_slice(b);
            Some(b.len())
        }
        Value::Decimal { unscaled, scale } => render_decimal(unscaled, scale, out),
    }
}

/// Render a decimal by inserting the point rather than dividing: the
/// digits are already exact in the unscaled integer, and dividing would
/// both lose precision and need an i128 division the PIC build has no
/// intrinsic for.
fn render_decimal(unscaled: i128, scale: u8, out: &mut [u8]) -> Option<usize> {
    let neg = unscaled < 0;
    let mut mag = if neg {
        unscaled.wrapping_neg() as u128
    } else {
        unscaled as u128
    };
    let mut digits = [0u8; 40];
    let mut n = 0usize;
    if mag == 0 {
        digits[0] = b'0';
        n = 1;
    }
    while mag > 0 {
        digits[n] = b'0' + (mag % 10) as u8;
        mag /= 10;
        n += 1;
    }
    let s = scale as usize;
    while n <= s {
        digits[n] = b'0';
        n += 1;
    }
    let need = usize::from(neg) + n + usize::from(s > 0);
    if out.len() < need {
        return None;
    }
    let mut at = 0usize;
    if neg {
        out[0] = b'-';
        at = 1;
    }
    for i in 0..n {
        if s > 0 && i == n - s {
            out[at] = b'.';
            at += 1;
        }
        out[at] = digits[n - 1 - i];
        at += 1;
    }
    Some(at)
}

fn render_f64(x: f64, out: &mut [u8]) -> Option<usize> {
    if x.is_nan() {
        return write_str(b"NaN", out);
    }
    if x == f64::INFINITY {
        return write_str(b"inf", out);
    }
    if x == f64::NEG_INFINITY {
        return write_str(b"-inf", out);
    }
    let neg = x < 0.0;
    let a = if neg { -x } else { x };
    let int_part = a as u64;
    let mut frac = a - int_part as f64;
    let mut at = 0usize;
    if neg {
        *out.get_mut(0)? = b'-';
        at = 1;
    }
    at += itoa(int_part as i64, out.get_mut(at..)?)?;
    if frac == 0.0 {
        return Some(at);
    }
    *out.get_mut(at)? = b'.';
    at += 1;
    for _ in 0..6 {
        frac *= 10.0;
        let d = frac as u64;
        *out.get_mut(at)? = b'0' + (d % 10) as u8;
        at += 1;
        frac -= d as f64;
        if frac == 0.0 {
            break;
        }
    }
    Some(at)
}

fn write_str(s: &[u8], out: &mut [u8]) -> Option<usize> {
    if out.len() < s.len() {
        return None;
    }
    out[..s.len()].copy_from_slice(s);
    Some(s.len())
}

// ── Error mapping ─────────────────────────────────────────────────────

/// MySQL's own error number for an executor outcome code.
///
/// Clients branch on these numbers, and several have well-known
/// meanings a driver acts on: 1062 tells it "duplicate key, this retry
/// will fail the same way", 1146 tells it "the table is not there".
/// Mapping everything to a generic 1105 would force every client to
/// parse English message text.
pub fn error_number(code: u8) -> u16 {
    use sql_exec as x;
    match code {
        x::ERR_SYNTAX | x::ERR_UNEXPECTED_END | x::ERR_UNTERMINATED_LITERAL => 1064, // ER_PARSE_ERROR
        x::ERR_UNKNOWN_COLUMN => 1054, // ER_BAD_FIELD_ERROR
        x::ERR_UNKNOWN_TYPE => 1064,
        x::ERR_DUPLICATE_COLUMN => 1060,    // ER_DUP_FIELDNAME
        x::ERR_NO_SUCH_TABLE => 1146,       // ER_NO_SUCH_TABLE
        x::ERR_TABLE_EXISTS => 1050,        // ER_TABLE_EXISTS_ERROR
        x::ERR_MISSING_PRIMARY_KEY => 1173, // ER_REQUIRES_PRIMARY_KEY
        x::ERR_TYPE_MISMATCH => 1366,       // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD
        x::ERR_LITERAL_TOO_LONG | x::ERR_NAME_TOO_LONG | x::ERR_TOO_LONG => 1406, // ER_DATA_TOO_LONG
        x::ERR_NOT_NULL => 1048,       // ER_BAD_NULL_ERROR
        x::ERR_DUPLICATE_KEY => 1062,  // ER_DUP_ENTRY
        x::ERR_ARITY_MISMATCH => 1136, // ER_WRONG_VALUE_COUNT_ON_ROW
        x::ERR_UNSUPPORTED => 1235,    // ER_NOT_SUPPORTED_YET
        x::ERR_TOO_MANY_ITEMS | x::ERR_RESULT_TOO_LARGE => 1104, // ER_TOO_BIG_SELECT
        x::ERR_BUSY => 1040,           // ER_CON_COUNT_ERROR
        x::ERR_CORRUPT => 1194,        // ER_CRASHED_ON_USAGE
        x::ERR_STORE => 1053,          // ER_SERVER_SHUTDOWN
        x::ERR_TIMEOUT => 1969,        // ER_STATEMENT_TIMEOUT
        _ => 1105,                     // ER_UNKNOWN_ERROR
    }
}

/// The five-character SQLSTATE that accompanies the error number.
pub fn sqlstate(code: u8) -> &'static [u8] {
    use sql_exec as x;
    match code {
        x::ERR_SYNTAX
        | x::ERR_UNEXPECTED_END
        | x::ERR_UNTERMINATED_LITERAL
        | x::ERR_UNKNOWN_TYPE => b"42000",
        x::ERR_UNKNOWN_COLUMN => b"42S22",
        x::ERR_DUPLICATE_COLUMN => b"42S21",
        x::ERR_NO_SUCH_TABLE => b"42S02",
        x::ERR_TABLE_EXISTS => b"42S01",
        x::ERR_MISSING_PRIMARY_KEY => b"42000",
        x::ERR_TYPE_MISMATCH => b"22007",
        x::ERR_LITERAL_TOO_LONG | x::ERR_NAME_TOO_LONG | x::ERR_TOO_LONG => b"22001",
        x::ERR_NOT_NULL => b"23000",
        x::ERR_DUPLICATE_KEY => b"23000",
        x::ERR_ARITY_MISMATCH => b"21S01",
        x::ERR_UNSUPPORTED => b"0A000",
        x::ERR_BUSY => b"08004",
        x::ERR_TIMEOUT => b"70100", // ER_QUERY_INTERRUPTED
        _ => b"HY000",
    }
}

// ── Client packet parsing ─────────────────────────────────────────────

/// One parsed client packet.
pub struct Packet<'a> {
    pub seq: u8,
    pub payload: &'a [u8],
}

/// Parse one client packet from the front of `buf`.
///
/// `Ok(None)` means incomplete — distinguishable from malformed, because
/// bytes split across reads are normal and closing on them would drop
/// good connections.
pub fn parse_packet(buf: &[u8]) -> Result<Option<(Packet<'_>, usize)>, ()> {
    if buf.len() < PACKET_HEADER {
        return Ok(None);
    }
    let len = usize::from(buf[0]) | (usize::from(buf[1]) << 8) | (usize::from(buf[2]) << 16);
    if len >= MAX_PAYLOAD {
        // A continued packet. Refused rather than silently treated as a
        // whole one, which would execute a truncated statement.
        return Err(());
    }
    let total = PACKET_HEADER + len;
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some((
        Packet {
            seq: buf[3],
            payload: &buf[PACKET_HEADER..total],
        },
        total,
    )))
}

/// The client's capability flags from a `HandshakeResponse41`.
pub fn response_capabilities(payload: &[u8]) -> Option<u32> {
    if payload.len() < 4 {
        return None;
    }
    Some(u32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

/// The username from a `HandshakeResponse41`.
///
/// Layout: `[caps:4][max_packet:4][charset:1][reserved:23][user\0]…`
pub fn response_username(payload: &[u8]) -> Option<&[u8]> {
    const USER_AT: usize = 4 + 4 + 1 + 23;
    let rest = payload.get(USER_AT..)?;
    let end = rest.iter().position(|&b| b == 0)?;
    Some(&rest[..end])
}

/// The SQL text of a `COM_QUERY`: everything after the command byte,
/// with no terminator.
pub fn query_sql(payload: &[u8]) -> Option<&[u8]> {
    if payload.is_empty() || payload[0] != COM_QUERY {
        return None;
    }
    Some(&payload[1..])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Split a stream into packets, checking the length convention:
    /// it counts ONLY the payload.
    fn packets(bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
        let mut out = Vec::new();
        let mut at = 0usize;
        while at < bytes.len() {
            let len = usize::from(bytes[at])
                | (usize::from(bytes[at + 1]) << 8)
                | (usize::from(bytes[at + 2]) << 16);
            let seq = bytes[at + 3];
            let payload = bytes[at + PACKET_HEADER..at + PACKET_HEADER + len].to_vec();
            out.push((seq, payload));
            at += PACKET_HEADER + len;
        }
        out
    }

    // ── Framing ──────────────────────────────────────────────────────

    #[test]
    fn packet_length_counts_only_the_payload() {
        let mut buf = [0u8; 64];
        let mut w = Writer::new(&mut buf, 1);
        ok_packet(&mut w, 0, 0, SERVER_STATUS_AUTOCOMMIT);
        let out = w.finished().unwrap();
        let ps = packets(out);
        assert_eq!(ps.len(), 1);
        assert_eq!(ps[0].0, 1, "sequence is taken from the writer");
        // header(1) + lenenc(1) + lenenc(1) + status(2) + warnings(2)
        assert_eq!(ps[0].1.len(), 7);
        assert_eq!(out.len(), PACKET_HEADER + 7);
    }

    /// The sequence increments per packet and a client closes the
    /// connection on a gap, so it is protocol state rather than
    /// decoration.
    #[test]
    fn sequence_numbers_increment_per_packet() {
        let mut buf = [0u8; 256];
        let mut w = Writer::new(&mut buf, 1);
        column_count(&mut w, 2);
        eof_packet(&mut w, 0);
        ok_packet(&mut w, 0, 0, 0);
        let ps = packets(w.finished().unwrap());
        assert_eq!(
            ps.iter().map(|p| p.0).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "a gap makes the client hang up"
        );
        assert_eq!(w.seq(), 4, "the writer hands back where it ended");
    }

    /// An unfinished or overflowed stream must not reach the socket: the
    /// client frames by length, so a truncated packet desyncs it
    /// permanently.
    #[test]
    fn unfinished_or_overflowed_output_is_not_shippable() {
        let mut buf = [0u8; 64];
        let mut w = Writer::new(&mut buf, 0);
        w.begin().u8(1);
        assert!(w.finished().is_none(), "an open packet is not shippable");

        let mut small = [0u8; 5];
        let mut w = Writer::new(&mut small, 0);
        ok_packet(&mut w, 0, 0, 0);
        assert!(w.overflowed());
        assert!(w.finished().is_none());
    }

    // ── Length-encoded integers ──────────────────────────────────────

    /// Every boundary of the four-way encoding, because the prefixes
    /// collide with NULL and EOF markers and an off-by-one changes which
    /// meaning a client reads.
    #[test]
    fn lenenc_int_boundaries() {
        for (v, want) in [
            (0u64, vec![0x00]),
            (250, vec![0xFA]),
            (251, vec![0xFC, 0xFB, 0x00]),
            (0xFFFF, vec![0xFC, 0xFF, 0xFF]),
            (0x1_0000, vec![0xFD, 0x00, 0x00, 0x01]),
            (0xFF_FFFF, vec![0xFD, 0xFF, 0xFF, 0xFF]),
            (
                0x100_0000,
                vec![0xFE, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
            ),
        ] {
            let mut buf = [0u8; 32];
            let mut w = Writer::new(&mut buf, 0);
            w.begin().lenenc_int(v).end();
            let ps = packets(w.finished().unwrap());
            assert_eq!(ps[0].1, want, "value {v}");
        }
        // 250 fits in one byte and 251 does not. That boundary is what
        // keeps a one-byte length from ever colliding with the 0xFB
        // NULL marker, so it is checked on the ENCODER rather than
        // asserted as arithmetic.
        let mut buf = [0u8; 16];
        let mut w = Writer::new(&mut buf, 0);
        w.begin().lenenc_int(250).end();
        assert_ne!(packets(w.finished().unwrap())[0].1[0], NULL_LENENC);
    }

    /// A NULL cell is the bare 0xFB byte; an empty string is a
    /// length-encoded zero. `NULL` and `''` are different answers.
    #[test]
    fn null_and_empty_cells_differ_on_the_wire() {
        let mut buf = [0u8; 32];
        let mut w = Writer::new(&mut buf, 0);
        w.begin().null_cell().lenenc_str(b"").end();
        let ps = packets(w.finished().unwrap());
        assert_eq!(ps[0].1, vec![NULL_LENENC, 0x00]);
    }

    // ── Handshake ────────────────────────────────────────────────────

    /// The advertised capabilities change how the CLIENT frames its
    /// packets, so anything claimed must actually be implemented.
    /// `CLIENT_DEPRECATE_EOF` in particular changes how a result set
    /// ends.
    #[test]
    fn advertised_capabilities_are_only_what_is_implemented() {
        assert_eq!(
            SERVER_CAPABILITIES & CLIENT_DEPRECATE_EOF,
            0,
            "EOF deprecation changes result-set framing and is not implemented"
        );
        assert_ne!(
            SERVER_CAPABILITIES & CLIENT_PROTOCOL_41,
            0,
            "every modern client requires protocol 41"
        );
        assert_ne!(SERVER_CAPABILITIES & CLIENT_PLUGIN_AUTH, 0);
    }

    /// The handshake splits the 20-byte scramble across two fields with
    /// a filler and a terminator between them; a client reassembles it
    /// and will fail auth if the halves are wrong.
    #[test]
    fn handshake_carries_the_whole_scramble_in_two_parts() {
        let scramble: [u8; 20] = core::array::from_fn(|i| (i as u8) + 1);
        let mut buf = [0u8; 256];
        let mut w = Writer::new(&mut buf, 0);
        handshake(&mut w, 7, &scramble, b"8.0.0-lattice");
        let ps = packets(w.finished().unwrap());
        assert_eq!(ps[0].0, 0, "the handshake is always sequence 0");
        let p = &ps[0].1;
        assert_eq!(p[0], 10, "protocol version 10");
        let vend = 1 + p[1..].iter().position(|&b| b == 0).unwrap();
        assert_eq!(&p[1..vend], b"8.0.0-lattice");
        let mut at = vend + 1;
        assert_eq!(
            u32::from_le_bytes(p[at..at + 4].try_into().unwrap()),
            7,
            "connection id"
        );
        at += 4;
        assert_eq!(&p[at..at + 8], &scramble[..8], "scramble part 1");
        at += 8;
        assert_eq!(p[at], 0, "filler");
        at += 1;
        let lo = u16::from_le_bytes(p[at..at + 2].try_into().unwrap());
        at += 2 + 1 + 2;
        let hi = u16::from_le_bytes(p[at..at + 2].try_into().unwrap());
        at += 2;
        assert_eq!(
            u32::from(lo) | (u32::from(hi) << 16),
            SERVER_CAPABILITIES,
            "capabilities are split across two fields"
        );
        assert_eq!(p[at], 21, "auth data length: 20 scramble + terminator");
        at += 1 + 10;
        assert_eq!(&p[at..at + 12], &scramble[8..20], "scramble part 2");
        at += 12;
        assert_eq!(p[at], 0);
        at += 1;
        assert_eq!(&p[at..at + 21], b"mysql_native_password");
    }

    // ── Errors ───────────────────────────────────────────────────────

    /// The `#` marker before the SQLSTATE is required; without it a
    /// client reads the first five characters of the MESSAGE as the
    /// state.
    #[test]
    fn err_packet_carries_the_sqlstate_marker() {
        let mut buf = [0u8; 128];
        let mut w = Writer::new(&mut buf, 1);
        err_packet(&mut w, 1146, b"42S02", b"Table doesn't exist");
        let ps = packets(w.finished().unwrap());
        let p = &ps[0].1;
        assert_eq!(p[0], ERR_HEADER);
        assert_eq!(u16::from_le_bytes([p[1], p[2]]), 1146);
        assert_eq!(p[3], b'#');
        assert_eq!(&p[4..9], b"42S02");
        assert_eq!(&p[9..], b"Table doesn't exist");
    }

    /// Drivers act on specific numbers, so the well-known ones are
    /// pinned rather than merely distinct.
    #[test]
    fn error_numbers_match_mysqls_own() {
        use sql_exec as x;
        assert_eq!(error_number(x::ERR_DUPLICATE_KEY), 1062);
        assert_eq!(error_number(x::ERR_NO_SUCH_TABLE), 1146);
        assert_eq!(error_number(x::ERR_TABLE_EXISTS), 1050);
        assert_eq!(error_number(x::ERR_SYNTAX), 1064);
        assert_eq!(error_number(x::ERR_NOT_NULL), 1048);
        assert_eq!(sqlstate(x::ERR_DUPLICATE_KEY), b"23000");
        assert_eq!(sqlstate(x::ERR_NO_SUCH_TABLE), b"42S02");
    }

    // ── Types and rendering ──────────────────────────────────────────

    /// MySQL has no boolean type: it is TINYINT, and the value renders
    /// `1`/`0`. This is the visible half of the connector split — the
    /// same value is `t`/`f` on the PostgreSQL port.
    #[test]
    fn booleans_are_tinyint_rendered_one_and_zero() {
        assert_eq!(column_type(LogicalType::Boolean), MYSQL_TYPE_TINY);
        let mut out = [0u8; 8];
        let n = render_value(LogicalType::Boolean, Value::Boolean(true), &mut out).unwrap();
        assert_eq!(&out[..n], b"1");
        let n = render_value(LogicalType::Boolean, Value::Boolean(false), &mut out).unwrap();
        assert_eq!(&out[..n], b"0");
    }

    #[test]
    fn column_types_match_mysqls_own() {
        for (ty, want) in [
            (LogicalType::SmallInt, MYSQL_TYPE_SHORT),
            (LogicalType::Int, MYSQL_TYPE_LONG),
            (LogicalType::BigInt, MYSQL_TYPE_LONGLONG),
            (LogicalType::Double, MYSQL_TYPE_DOUBLE),
            (LogicalType::VarChar { length: 8 }, MYSQL_TYPE_VAR_STRING),
            (LogicalType::VarBinary { length: 8 }, MYSQL_TYPE_BLOB),
            (LogicalType::Date, MYSQL_TYPE_DATE),
        ] {
            assert_eq!(column_type(ty), want, "{ty:?}");
        }
    }

    /// A binary column must declare the `binary` charset, or a client
    /// renders its bytes as if they were text.
    #[test]
    fn binary_columns_declare_the_binary_charset() {
        assert_eq!(
            column_charset(LogicalType::VarBinary { length: 8 }),
            CHARSET_BINARY
        );
        assert_eq!(
            column_charset(LogicalType::VarChar { length: 8 }),
            CHARSET_UTF8MB4
        );
    }

    #[test]
    fn integers_render_in_decimal_including_the_extremes() {
        let mut out = [0u8; 32];
        for (v, want) in [
            (Value::Int(0), "0"),
            (Value::Int(-1), "-1"),
            (Value::BigInt(i64::MAX), "9223372036854775807"),
            (Value::BigInt(i64::MIN), "-9223372036854775808"),
        ] {
            let n = render_value(LogicalType::BigInt, v, &mut out).unwrap();
            assert_eq!(core::str::from_utf8(&out[..n]).unwrap(), want, "{v:?}");
        }
    }

    /// Unlike PostgreSQL, binary values go out RAW: the column is
    /// declared with the binary charset, so the client takes the bytes
    /// as they are rather than parsing a hex escape.
    #[test]
    fn binary_values_are_raw_not_hex() {
        let mut out = [0u8; 16];
        let n = render_value(
            LogicalType::VarBinary { length: 8 },
            Value::Bytes(&[0x00, 0xAB, 0xFF]),
            &mut out,
        )
        .unwrap();
        assert_eq!(&out[..n], &[0x00, 0xAB, 0xFF]);
    }

    #[test]
    fn decimals_render_exactly() {
        let mut out = [0u8; 48];
        for (unscaled, scale, want) in [
            (12345i128, 2u8, "123.45"),
            (-12345, 2, "-123.45"),
            (5, 3, "0.005"),
            (0, 2, "0.00"),
        ] {
            let n = render_value(
                LogicalType::Decimal {
                    precision: 10,
                    scale,
                },
                Value::Decimal { unscaled, scale },
                &mut out,
            )
            .unwrap();
            assert_eq!(core::str::from_utf8(&out[..n]).unwrap(), want);
        }
    }

    #[test]
    fn null_has_no_rendering() {
        let mut out = [0u8; 8];
        assert!(render_value(LogicalType::Int, Value::Null, &mut out).is_none());
    }

    // ── Client parsing ───────────────────────────────────────────────

    fn client_packet(seq: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![
            (payload.len() & 0xFF) as u8,
            ((payload.len() >> 8) & 0xFF) as u8,
            ((payload.len() >> 16) & 0xFF) as u8,
            seq,
        ];
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn a_com_query_round_trips() {
        let mut p = vec![COM_QUERY];
        p.extend_from_slice(b"SELECT 1");
        let bytes = client_packet(0, &p);
        let (pkt, n) = parse_packet(&bytes).unwrap().unwrap();
        assert_eq!(n, bytes.len());
        assert_eq!(pkt.seq, 0);
        assert_eq!(query_sql(pkt.payload), Some(b"SELECT 1".as_slice()));
    }

    /// A partial read is incomplete, not malformed — bytes split across
    /// packets are normal.
    #[test]
    fn a_partial_packet_is_incomplete() {
        let bytes = client_packet(0, b"\x03SELECT 1");
        for cut in 0..bytes.len() {
            assert!(
                matches!(parse_packet(&bytes[..cut]), Ok(None)),
                "truncation to {cut} was not reported as incomplete"
            );
        }
    }

    /// A continued (0xFFFFFF) packet is refused rather than treated as a
    /// whole one, which would execute a truncated statement.
    #[test]
    fn a_continued_packet_is_refused() {
        let bytes = [0xFFu8, 0xFF, 0xFF, 0x00];
        assert!(parse_packet(&bytes).is_err());
    }

    #[test]
    fn handshake_response_fields_are_readable() {
        let mut p = Vec::new();
        p.extend_from_slice(&(CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH).to_le_bytes());
        p.extend_from_slice(&16_777_216u32.to_le_bytes());
        p.push(CHARSET_UTF8MB4 as u8);
        p.extend_from_slice(&[0u8; 23]);
        p.extend_from_slice(b"alice\0");
        assert_eq!(
            response_capabilities(&p),
            Some(CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH)
        );
        assert_eq!(response_username(&p), Some(b"alice".as_slice()));
    }

    /// A truncated response must not run off the end looking for the
    /// username terminator.
    #[test]
    fn a_truncated_handshake_response_does_not_overrun() {
        assert_eq!(response_username(&[0u8; 10]), None);
        assert_eq!(response_capabilities(&[0u8; 2]), None);
    }

    /// A packet that is not a COM_QUERY does not yield SQL, so a
    /// COM_PING cannot be mistaken for an empty statement.
    #[test]
    fn only_com_query_yields_sql() {
        assert_eq!(query_sql(&[COM_PING]), None);
        assert_eq!(query_sql(&[]), None);
        assert_eq!(query_sql(&[COM_QUERY]), Some(b"".as_slice()));
    }
}
