//! PostgreSQL v3 frontend/backend protocol, SERVER side (RFC database
//! foundation §14.3 — the connector owns "its wire handshake,
//! authentication negotiation, simple and extended query flows, …
//! dialect surface, compatibility catalogs, type aliases, error mapping,
//! and result encoding").
//!
//! Dual-target facade: compiles into the no_std `pg_edge_anchor` and is
//! host-tested here. This is the CONNECTOR half of the split — the
//! executor decided what the values are, and everything here is about
//! how PostgreSQL specifically renders them.
//!
//! Not to be confused with `pg_core.rs`, which is the CLIENT side (the
//! outbound `pg_client` module talking to a real PostgreSQL). The two
//! are different halves of the same protocol and share no code, because
//! sharing would mean one of them carried the other's assumptions about
//! who speaks first.
//!
//! ## Message framing
//!
//! Every message after startup is `[type:u8][len:i32 BE][payload]`,
//! where `len` COUNTS ITSELF but not the type byte. That off-by-one is
//! the single most common way to get this protocol wrong, so the length
//! is written in exactly one place here ([`Msg::finish`]) and never by
//! hand.
//!
//! The startup message is the exception: no type byte, just
//! `[len:i32 BE][protocol:i32 BE][key\0value\0…\0]`. A client's very
//! first bytes may instead be an `SSLRequest` or a `GSSENCRequest`, which
//! are startup-shaped but carry a magic protocol number and must be
//! answered before a real startup arrives.

// `sql_exec` is mounted here so this file, the executor and the anchor
// all share ONE `relational` tree. A `#[path]` mount is a fresh module
// tree each time, so a second mount would give the anchor two
// incompatible `LogicalType`s.
#[path = "sql_exec.rs"]
pub mod sql_exec;

pub use sql_exec::relational;

use relational::{LogicalType, Value};

/// `SSLRequest`'s magic protocol version (1234 << 16 | 5679).
pub const SSL_REQUEST_CODE: i32 = 80_877_103;
/// `GSSENCRequest`'s magic protocol version (1234 << 16 | 5680).
pub const GSSENC_REQUEST_CODE: i32 = 80_877_104;
/// `CancelRequest`'s magic protocol version (1234 << 16 | 5678).
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;
/// The only protocol version this connector speaks: 3.0.
pub const PROTOCOL_3_0: i32 = 196_608;

// ── Backend message type bytes ────────────────────────────────────────

pub const B_AUTHENTICATION: u8 = b'R';
pub const B_BACKEND_KEY_DATA: u8 = b'K';
pub const B_PARAMETER_STATUS: u8 = b'S';
pub const B_READY_FOR_QUERY: u8 = b'Z';
pub const B_ROW_DESCRIPTION: u8 = b'T';
pub const B_DATA_ROW: u8 = b'D';
pub const B_COMMAND_COMPLETE: u8 = b'C';
pub const B_EMPTY_QUERY_RESPONSE: u8 = b'I';
pub const B_ERROR_RESPONSE: u8 = b'E';
pub const B_NO_DATA: u8 = b'n';
pub const B_PARSE_COMPLETE: u8 = b'1';
pub const B_BIND_COMPLETE: u8 = b'2';
pub const B_PARAMETER_DESCRIPTION: u8 = b't';

// ── Frontend message type bytes ───────────────────────────────────────

pub const F_QUERY: u8 = b'Q';
pub const F_TERMINATE: u8 = b'X';
pub const F_SYNC: u8 = b'S';
pub const F_PARSE: u8 = b'P';
pub const F_BIND: u8 = b'B';
pub const F_DESCRIBE: u8 = b'D';
pub const F_EXECUTE: u8 = b'E';
pub const F_CLOSE: u8 = b'C';
pub const F_FLUSH: u8 = b'H';
pub const F_PASSWORD: u8 = b'p';

/// Transaction status in `ReadyForQuery`. Reported honestly: `T` claims
/// an open transaction block, and a client may change behaviour based on
/// it, so it is only sent when one is actually open.
pub const STATUS_IDLE: u8 = b'I';
pub const STATUS_IN_TRANSACTION: u8 = b'T';
pub const STATUS_FAILED: u8 = b'E';

// ── Type OIDs ─────────────────────────────────────────────────────────
//
// A client uses the OID to decide how to parse a column's text, so a
// wrong OID makes a correct value unreadable. These are PostgreSQL's own
// well-known values and must never be invented.

pub const OID_BOOL: i32 = 16;
pub const OID_BYTEA: i32 = 17;
pub const OID_INT8: i32 = 20;
pub const OID_INT2: i32 = 21;
pub const OID_INT4: i32 = 23;
pub const OID_TEXT: i32 = 25;
pub const OID_FLOAT4: i32 = 700;
pub const OID_FLOAT8: i32 = 701;
pub const OID_BPCHAR: i32 = 1042;
pub const OID_VARCHAR: i32 = 1043;
pub const OID_DATE: i32 = 1082;
pub const OID_TIME: i32 = 1083;
pub const OID_TIMESTAMP: i32 = 1114;
pub const OID_TIMESTAMPTZ: i32 = 1184;
pub const OID_NUMERIC: i32 = 1700;

// ── Writer ────────────────────────────────────────────────────────────

/// Appends backend messages to a buffer, tracking the one length field
/// that PostgreSQL defines unusually.
pub struct Writer<'b> {
    buf: &'b mut [u8],
    len: usize,
    /// Offset of the length field of the message being built.
    open_at: Option<usize>,
    /// Sticky: set when any append did not fit. A caller checks it once
    /// rather than after every push, and a partially-written message is
    /// never handed to the socket — a truncated message would desync the
    /// stream permanently, since the client frames by length.
    overflow: bool,
}

impl<'b> Writer<'b> {
    pub fn new(buf: &'b mut [u8]) -> Self {
        Self {
            buf,
            len: 0,
            open_at: None,
            overflow: false,
        }
    }

    /// Bytes written so far. Only meaningful when no message is open.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    /// Finished bytes, or `None` if anything overflowed or a message was
    /// left open. Both are stream-corrupting, so neither is shippable.
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

    /// Begin a message of type `t`, reserving its length field.
    pub fn begin(&mut self, t: u8) -> &mut Self {
        if self.open_at.is_some() {
            self.overflow = true;
            return self;
        }
        if !self.room(5) {
            return self;
        }
        self.buf[self.len] = t;
        self.len += 1;
        self.open_at = Some(self.len);
        self.buf[self.len..self.len + 4].copy_from_slice(&0i32.to_be_bytes());
        self.len += 4;
        self
    }

    /// Close the open message, backfilling its length.
    ///
    /// The length counts itself and the payload but NOT the type byte.
    /// Written here and nowhere else, because that is the rule this
    /// protocol is most often implemented wrongly.
    pub fn finish(&mut self) -> &mut Self {
        let Some(at) = self.open_at.take() else {
            self.overflow = true;
            return self;
        };
        let body = self.len - at;
        if body > i32::MAX as usize {
            self.overflow = true;
            return self;
        }
        self.buf[at..at + 4].copy_from_slice(&(body as i32).to_be_bytes());
        self
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        if self.room(1) {
            self.buf[self.len] = v;
            self.len += 1;
        }
        self
    }

    pub fn i16(&mut self, v: i16) -> &mut Self {
        if self.room(2) {
            self.buf[self.len..self.len + 2].copy_from_slice(&v.to_be_bytes());
            self.len += 2;
        }
        self
    }

    pub fn i32(&mut self, v: i32) -> &mut Self {
        if self.room(4) {
            self.buf[self.len..self.len + 4].copy_from_slice(&v.to_be_bytes());
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

    /// A NUL-terminated string.
    pub fn cstr(&mut self, v: &[u8]) -> &mut Self {
        self.bytes(v).u8(0)
    }
}

// ── Backend messages ──────────────────────────────────────────────────

/// `AuthenticationOk`.
pub fn auth_ok(w: &mut Writer<'_>) {
    w.begin(B_AUTHENTICATION).i32(0).finish();
}

/// `AuthenticationCleartextPassword`. Used when a password is configured:
/// the client sends the password and the server compares it.
///
/// Cleartext rather than SCRAM because a SCRAM exchange this server
/// cannot complete correctly would be worse than one it does not offer —
/// a client would believe its password had been verified against a
/// challenge. Cleartext is honest about what is happening, and the
/// deployment expectation is TLS termination in front (or a trusted
/// network), the same posture the Redis anchor's `requirepass` takes.
pub fn auth_cleartext(w: &mut Writer<'_>) {
    w.begin(B_AUTHENTICATION).i32(3).finish();
}

/// `ParameterStatus`. Clients read several of these at startup and some
/// will not proceed without them.
pub fn parameter_status(w: &mut Writer<'_>, key: &[u8], value: &[u8]) {
    w.begin(B_PARAMETER_STATUS).cstr(key).cstr(value).finish();
}

/// `BackendKeyData`. The pair a client would use to issue a
/// `CancelRequest`; sent because the startup sequence expects it.
pub fn backend_key_data(w: &mut Writer<'_>, pid: i32, secret: i32) {
    w.begin(B_BACKEND_KEY_DATA).i32(pid).i32(secret).finish();
}

/// `ReadyForQuery`. The message that ends every request cycle — a client
/// blocks until it arrives, so an error path that forgets it hangs the
/// client rather than failing it.
pub fn ready_for_query(w: &mut Writer<'_>, status: u8) {
    w.begin(B_READY_FOR_QUERY).u8(status).finish();
}

/// `CommandComplete` with a tag such as `SELECT 3` or `INSERT 0 2`.
pub fn command_complete(w: &mut Writer<'_>, tag: &[u8]) {
    w.begin(B_COMMAND_COMPLETE).cstr(tag).finish();
}

/// `EmptyQueryResponse`, for a statement that was only whitespace.
/// Distinct from `CommandComplete` in the protocol, and clients render
/// the two differently.
pub fn empty_query_response(w: &mut Writer<'_>) {
    w.begin(B_EMPTY_QUERY_RESPONSE).finish();
}

/// `ErrorResponse`.
///
/// `severity`, `code` (SQLSTATE) and `message` are the three fields a
/// client needs; the terminating zero byte after the last field is what
/// ends the message, and omitting it makes a client wait for more
/// fields forever.
pub fn error_response(w: &mut Writer<'_>, severity: &[u8], sqlstate: &[u8], message: &[u8]) {
    w.begin(B_ERROR_RESPONSE)
        .u8(b'S')
        .cstr(severity)
        .u8(b'V')
        .cstr(severity)
        .u8(b'C')
        .cstr(sqlstate)
        .u8(b'M')
        .cstr(message)
        .u8(0)
        .finish();
}

/// One column of a `RowDescription`.
pub struct FieldDesc<'a> {
    pub name: &'a [u8],
    pub type_oid: i32,
    /// -1 for a variable-width type.
    pub type_size: i16,
    /// Type-specific modifier, or -1. For `varchar`/`bpchar` PostgreSQL
    /// encodes the declared length as `length + 4`.
    pub type_mod: i32,
}

/// `RowDescription`.
pub fn row_description(w: &mut Writer<'_>, fields: &[FieldDesc<'_>]) {
    w.begin(B_ROW_DESCRIPTION).i16(fields.len() as i16);
    for f in fields {
        w.cstr(f.name)
            // table OID and column attribute number: 0 means "not a
            // simple column reference", which is honest — these values
            // are computed and the client uses them only for metadata.
            .i32(0)
            .i16(0)
            .i32(f.type_oid)
            .i16(f.type_size)
            .i32(f.type_mod)
            // Format code: 0 = text. Every value this connector sends is
            // text, so declaring binary anywhere would misparse.
            .i16(0);
    }
    w.finish();
}

/// Begin a `DataRow` with `n` columns. Cells follow via
/// [`data_row_cell`], then [`Writer::finish`].
pub fn data_row_begin(w: &mut Writer<'_>, n: i16) {
    w.begin(B_DATA_ROW).i16(n);
}

/// One `DataRow` cell. `None` is SQL NULL, encoded as length -1 —
/// distinct from a present zero-length value, which is length 0.
pub fn data_row_cell(w: &mut Writer<'_>, cell: Option<&[u8]>) {
    match cell {
        None => {
            w.i32(-1);
        }
        Some(b) => {
            w.i32(b.len() as i32).bytes(b);
        }
    }
}

/// The single byte answering `SSLRequest` / `GSSENCRequest` with a
/// refusal. `N` means "continue unencrypted"; a client that requires
/// encryption will then disconnect on its own, which is the correct
/// outcome rather than a silent downgrade.
pub const SSL_REFUSED: u8 = b'N';

// ── Frontend parsing ──────────────────────────────────────────────────

/// A startup-phase message.
pub enum Startup<'a> {
    /// A real `StartupMessage`, with its parameter block.
    Params {
        protocol: i32,
        params: &'a [u8],
    },
    SslRequest,
    GssEncRequest,
    CancelRequest,
}

/// Parse a startup-phase message from the front of `buf`.
///
/// `Ok(None)` means "incomplete, read more" — which must be
/// distinguishable from "malformed", because treating a partial read as
/// an error would close a perfectly good connection whose bytes were
/// merely split across packets.
pub fn parse_startup(buf: &[u8]) -> Result<Option<(Startup<'_>, usize)>, ()> {
    if buf.len() < 8 {
        return Ok(None);
    }
    let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    // The length counts itself, so a value below 8 cannot hold even the
    // protocol field.
    if !(8..=65536).contains(&len) {
        return Err(());
    }
    let total = len as usize;
    if buf.len() < total {
        return Ok(None);
    }
    let code = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    let msg = match code {
        SSL_REQUEST_CODE => Startup::SslRequest,
        GSSENC_REQUEST_CODE => Startup::GssEncRequest,
        CANCEL_REQUEST_CODE => Startup::CancelRequest,
        _ => Startup::Params {
            protocol: code,
            params: &buf[8..total],
        },
    };
    Ok(Some((msg, total)))
}

/// Look up a startup parameter by key. The block is
/// `key\0value\0…\0`, terminated by an empty key.
pub fn startup_param<'a>(params: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let mut at = 0usize;
    loop {
        let k_end = params[at..].iter().position(|&b| b == 0)? + at;
        if k_end == at {
            return None; // empty key terminates the block
        }
        let k = &params[at..k_end];
        let v_start = k_end + 1;
        let v_end = params[v_start..].iter().position(|&b| b == 0)? + v_start;
        if k == key {
            return Some(&params[v_start..v_end]);
        }
        at = v_end + 1;
        if at >= params.len() {
            return None;
        }
    }
}

/// A regular (post-startup) frontend message.
pub struct Frontend<'a> {
    pub kind: u8,
    pub payload: &'a [u8],
}

/// Parse one regular frontend message. Same `Ok(None)` = incomplete
/// discipline as [`parse_startup`].
pub fn parse_frontend(buf: &[u8]) -> Result<Option<(Frontend<'_>, usize)>, ()> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let kind = buf[0];
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    // Length counts itself but not the type byte, so 4 is the minimum
    // and the frame is `1 + len` bytes on the wire.
    if !(4..=(1 << 24)).contains(&len) {
        return Err(());
    }
    let total = 1 + len as usize;
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some((
        Frontend {
            kind,
            payload: &buf[5..total],
        },
        total,
    )))
}

/// The SQL text of a `Query` message: a single NUL-terminated string.
pub fn query_sql(payload: &[u8]) -> Option<&[u8]> {
    let end = payload.iter().position(|&b| b == 0)?;
    Some(&payload[..end])
}

// ── Type mapping ──────────────────────────────────────────────────────

/// PostgreSQL's OID for a logical type.
///
/// A client uses the OID to decide how to PARSE the column's text, so a
/// wrong OID makes a correct value unreadable — `psql` would print an
/// integer as a string, and a typed driver would fail outright. These are
/// PostgreSQL's own well-known values, never invented.
///
/// `Null` has no OID: an untyped NULL literal is a binder placeholder
/// that cannot reach a result column, and mapping it to something would
/// hide that. `text` is the closest honest answer for the one place it
/// could appear (a constant-expression select the connector answers
/// itself), so callers handle that case explicitly rather than getting it
/// by default here.
pub fn type_oid(ty: LogicalType) -> i32 {
    match ty {
        LogicalType::Null => OID_TEXT,
        LogicalType::Boolean => OID_BOOL,
        LogicalType::SmallInt => OID_INT2,
        LogicalType::Int => OID_INT4,
        LogicalType::BigInt => OID_INT8,
        LogicalType::Decimal { .. } => OID_NUMERIC,
        LogicalType::Float => OID_FLOAT4,
        LogicalType::Double => OID_FLOAT8,
        LogicalType::Char { .. } => OID_BPCHAR,
        LogicalType::VarChar { .. } => OID_VARCHAR,
        LogicalType::Binary { .. } | LogicalType::VarBinary { .. } => OID_BYTEA,
        LogicalType::Date => OID_DATE,
        LogicalType::Time => OID_TIME,
        LogicalType::Timestamp => OID_TIMESTAMP,
        LogicalType::TimestampTz => OID_TIMESTAMPTZ,
    }
}

/// The fixed byte width of a type, or -1 when it is variable.
pub fn type_size(ty: LogicalType) -> i16 {
    match ty {
        LogicalType::Boolean => 1,
        LogicalType::SmallInt => 2,
        LogicalType::Int | LogicalType::Float | LogicalType::Date => 4,
        LogicalType::BigInt
        | LogicalType::Double
        | LogicalType::Time
        | LogicalType::Timestamp
        | LogicalType::TimestampTz => 8,
        _ => -1,
    }
}

/// The type modifier PostgreSQL reports for a declared width.
///
/// For `varchar(n)` / `char(n)` the wire value is `n + 4` — PostgreSQL
/// stores the length with its own varlena header offset included, and a
/// client subtracts 4 to display the declared width. Reporting `n`
/// directly would make `psql \d` show `varchar(6)` for a
/// `varchar(10)` column.
pub fn type_mod(ty: LogicalType) -> i32 {
    match ty {
        LogicalType::Char { length } | LogicalType::VarChar { length } => i32::from(length) + 4,
        LogicalType::Decimal { precision, scale } => {
            // numeric's typmod packs precision in the high half and scale
            // in the low, plus the same varlena offset.
            ((i32::from(precision) << 16) | i32::from(scale)) + 4
        }
        _ => -1,
    }
}

// ── Value rendering ───────────────────────────────────────────────────

/// Write a decimal integer, returning its length. `no_std`, so this
/// exists rather than a format machinery call.
fn itoa(v: i64, out: &mut [u8]) -> Option<usize> {
    if v == 0 {
        *out.get_mut(0)? = b'0';
        return Some(1);
    }
    let neg = v < 0;
    // Accumulate digits from the magnitude via u64 so i64::MIN, whose
    // positive is not representable, does not overflow.
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

/// Render a value as PostgreSQL text.
///
/// This is the half of the split §14.3 assigns to the connector, and
/// booleans are why it exists: PostgreSQL writes `t`/`f` where MySQL
/// writes `1`/`0`. If the executor had produced text, one of those would
/// have had to win.
///
/// `None` when the value does not fit `out` — never a truncated
/// rendering, which would be a different value.
pub fn render_value(ty: LogicalType, v: Value<'_>, out: &mut [u8]) -> Option<usize> {
    match v {
        // NULL is not rendered: it is a length of -1 on the wire, and a
        // caller that reached here with one has confused absence with
        // emptiness.
        Value::Null => None,
        Value::Boolean(b) => {
            *out.get_mut(0)? = if b { b't' } else { b'f' };
            Some(1)
        }
        Value::SmallInt(x) => itoa(i64::from(x), out),
        Value::Int(x) => itoa(i64::from(x), out),
        Value::BigInt(x) => itoa(x, out),
        Value::Float(x) => render_f64(f64::from(x), out),
        Value::Double(x) => render_f64(x, out),
        Value::Text(b) => {
            if out.len() < b.len() {
                return None;
            }
            out[..b.len()].copy_from_slice(b);
            Some(b.len())
        }
        // PostgreSQL's text form for bytea is `\x` followed by lowercase
        // hex. The alternative (escape format) is deprecated and clients
        // parse hex by default.
        Value::Bytes(b) => {
            let need = 2 + b.len() * 2;
            if out.len() < need {
                return None;
            }
            out[0] = b'\\';
            out[1] = b'x';
            for (i, byte) in b.iter().enumerate() {
                out[2 + i * 2] = hex_nibble(byte >> 4);
                out[3 + i * 2] = hex_nibble(byte & 0x0F);
            }
            Some(need)
        }
        Value::Decimal { unscaled, scale } => {
            // `ty`'s declared scale is deliberately NOT used: the value
            // carries its own, and rescaling to the column's would
            // change the digits the caller stored.
            let _ = ty;
            render_decimal(unscaled, scale, out)
        }
    }
}

fn hex_nibble(n: u8) -> u8 {
    if n < 10 {
        b'0' + n
    } else {
        b'a' + (n - 10)
    }
}

/// Render a decimal from its unscaled integer and scale, inserting the
/// point rather than dividing — the exact digits are already there, and
/// dividing would both lose precision and pull in an i128 division the
/// PIC build has no intrinsic for.
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
        // Division by ten via the shared limb routine, for the same
        // no-compiler-rt reason.
        let (q, r) = (mag / 10, (mag % 10) as u8);
        digits[n] = b'0' + r;
        n += 1;
        mag = q;
    }
    let s = scale as usize;
    // Pad with leading zeros so the point has digits to its left.
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
        let d = digits[n - 1 - i];
        if s > 0 && i == n - s {
            out[at] = b'.';
            at += 1;
        }
        out[at] = d;
        at += 1;
    }
    Some(at)
}

/// Render a float. Deliberately plain: integral values print without a
/// point, others get a fixed six fractional digits. Not
/// shortest-round-trip — that needs a correct dtoa, and an approximate
/// one would print a value the client could not read back exactly.
fn render_f64(x: f64, out: &mut [u8]) -> Option<usize> {
    if x.is_nan() {
        return write_str(b"NaN", out);
    }
    if x == f64::INFINITY {
        return write_str(b"Infinity", out);
    }
    if x == f64::NEG_INFINITY {
        return write_str(b"-Infinity", out);
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

// ── Command tags ──────────────────────────────────────────────────────

/// Build the `CommandComplete` tag for a completed statement.
///
/// The exact spellings matter: `psql` parses `INSERT <oid> <rows>` (the
/// oid field is legacy and always 0) and prints the row count from it, and
/// a driver may key behaviour off the verb. Getting the shape wrong makes
/// a successful statement look like a failed one.
pub fn command_tag(tag: u8, affected: u64, rows: u16, out: &mut [u8]) -> Option<usize> {
    use sql_exec as x;
    match tag {
        x::TAG_CREATE_TABLE => write_str(b"CREATE TABLE", out),
        x::TAG_ALTER => write_str(b"ALTER TABLE", out),
        x::TAG_CREATE_INDEX => write_str(b"CREATE INDEX", out),
        x::TAG_DROP_INDEX => write_str(b"DROP INDEX", out),
        x::TAG_DROP_TABLE => write_str(b"DROP TABLE", out),
        x::TAG_INSERT => {
            let mut n = write_str(b"INSERT 0 ", out)?;
            n += itoa(affected as i64, out.get_mut(n..)?)?;
            Some(n)
        }
        x::TAG_SELECT => {
            let mut n = write_str(b"SELECT ", out)?;
            n += itoa(i64::from(rows), out.get_mut(n..)?)?;
            Some(n)
        }
        x::TAG_DELETE => {
            let mut n = write_str(b"DELETE ", out)?;
            n += itoa(affected as i64, out.get_mut(n..)?)?;
            Some(n)
        }
        x::TAG_UPDATE => {
            let mut n = write_str(b"UPDATE ", out)?;
            n += itoa(affected as i64, out.get_mut(n..)?)?;
            Some(n)
        }
        x::TAG_BEGIN => write_str(b"BEGIN", out),
        x::TAG_COMMIT => write_str(b"COMMIT", out),
        x::TAG_ROLLBACK => write_str(b"ROLLBACK", out),
        x::TAG_SET => write_str(b"SET", out),
        x::TAG_SHOW => write_str(b"SHOW", out),
        _ => write_str(b"", out),
    }
}

// ── Error mapping ─────────────────────────────────────────────────────

/// The SQLSTATE for an executor outcome code.
///
/// Distinct codes matter because a client's error handling branches on
/// the SQLSTATE class: `23505` tells a driver "this row already exists,
/// your retry will fail the same way", while `40001`-class conditions
/// tell it "retry may succeed". Mapping everything to a generic
/// `XX000` would force every client to read English message text to find
/// out what happened.
pub fn sqlstate(code: u8) -> &'static [u8] {
    use sql_exec as x;
    match code {
        // Syntax and access-rule violations: class 42.
        x::ERR_SYNTAX | x::ERR_UNEXPECTED_END | x::ERR_UNTERMINATED_LITERAL => b"42601",
        x::ERR_UNKNOWN_COLUMN => b"42703",
        x::ERR_UNKNOWN_TYPE => b"42704",
        x::ERR_DUPLICATE_COLUMN => b"42701",
        x::ERR_NO_SUCH_TABLE => b"42P01",
        x::ERR_TABLE_EXISTS => b"42P07",
        x::ERR_MISSING_PRIMARY_KEY => b"42P16",
        // Data exceptions: class 22.
        x::ERR_TYPE_MISMATCH => b"22P02",
        x::ERR_LITERAL_TOO_LONG | x::ERR_NAME_TOO_LONG | x::ERR_TOO_LONG => b"22001",
        // Integrity constraint violations: class 23.
        x::ERR_NOT_NULL => b"23502",
        x::ERR_DUPLICATE_KEY => b"23505",
        x::ERR_ARITY_MISMATCH => b"21000",
        // Feature not supported: class 0A.
        x::ERR_UNSUPPORTED => b"0A000",
        // Insufficient resources / operator intervention: classes 53, 57.
        x::ERR_TOO_MANY_ITEMS | x::ERR_RESULT_TOO_LARGE => b"53400",
        x::ERR_BUSY => b"53300",
        // Statement watchdog fired: query canceled by the server.
        x::ERR_TIMEOUT => b"57014",
        // Internal / data-corrupted: class XX.
        x::ERR_CORRUPT => b"XX001",
        x::ERR_STORE => b"58030",
        _ => b"XX000",
    }
}

/// A short human message for an outcome code. Deliberately terse and
/// stable; the SQLSTATE is what a program should branch on.
pub fn error_message(code: u8) -> &'static [u8] {
    use sql_exec as x;
    match code {
        x::ERR_TOO_LONG => b"statement too long",
        x::ERR_UNEXPECTED_END => b"unexpected end of statement",
        x::ERR_SYNTAX => b"syntax error",
        x::ERR_NAME_TOO_LONG => b"identifier too long",
        x::ERR_LITERAL_TOO_LONG => b"value too long for column",
        x::ERR_TOO_MANY_ITEMS => b"too many items in statement",
        x::ERR_UNTERMINATED_LITERAL => b"unterminated string literal",
        x::ERR_UNKNOWN_TYPE => b"type does not exist",
        x::ERR_UNKNOWN_COLUMN => b"column does not exist",
        x::ERR_TYPE_MISMATCH => b"invalid input value for column type",
        x::ERR_ARITY_MISMATCH => b"VALUES arity does not match column list",
        x::ERR_NOT_NULL => b"null value in column violates not-null constraint",
        x::ERR_MISSING_PRIMARY_KEY => b"table requires a primary key",
        x::ERR_DUPLICATE_COLUMN => b"duplicate column name",
        x::ERR_UNSUPPORTED => b"feature not supported",
        x::ERR_NO_SUCH_TABLE => b"relation does not exist",
        x::ERR_TABLE_EXISTS => b"relation already exists",
        x::ERR_STORE => b"storage unavailable",
        x::ERR_RESULT_TOO_LARGE => b"result set too large",
        x::ERR_BUSY => b"too many statements in flight",
        x::ERR_DUPLICATE_KEY => b"duplicate key value violates primary key",
        x::ERR_CORRUPT => b"stored object could not be decoded",
        x::ERR_TIMEOUT => b"statement timed out waiting for storage",
        _ => b"internal error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Read back a message written by `Writer`, checking the length
    /// convention the protocol defines unusually: it counts ITSELF and
    /// the payload, but not the type byte.
    fn one_message(bytes: &[u8]) -> (u8, &[u8]) {
        let kind = bytes[0];
        let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        assert_eq!(
            bytes.len(),
            1 + len,
            "length field must cover itself and the payload but not the type byte"
        );
        (kind, &bytes[5..])
    }

    #[test]
    fn message_length_excludes_the_type_byte() {
        let mut buf = [0u8; 64];
        let mut w = Writer::new(&mut buf);
        ready_for_query(&mut w, STATUS_IDLE);
        let out = w.finished().unwrap();
        // 'Z' + len(4) + status(1) = 6 bytes on the wire, len field = 5.
        assert_eq!(out.len(), 6);
        let (kind, payload) = one_message(out);
        assert_eq!(kind, B_READY_FOR_QUERY);
        assert_eq!(payload, &[STATUS_IDLE]);
    }

    #[test]
    fn auth_ok_is_an_int32_zero() {
        let mut buf = [0u8; 32];
        let mut w = Writer::new(&mut buf);
        auth_ok(&mut w);
        let (kind, payload) = one_message(w.finished().unwrap());
        assert_eq!(kind, B_AUTHENTICATION);
        assert_eq!(payload, &0i32.to_be_bytes());
    }

    #[test]
    fn parameter_status_is_two_cstrings() {
        let mut buf = [0u8; 64];
        let mut w = Writer::new(&mut buf);
        parameter_status(&mut w, b"server_version", b"15.0");
        let (kind, payload) = one_message(w.finished().unwrap());
        assert_eq!(kind, B_PARAMETER_STATUS);
        let mut want = Vec::new();
        want.extend_from_slice(b"server_version");
        want.push(0);
        want.extend_from_slice(b"15.0");
        want.push(0);
        assert_eq!(payload, want.as_slice());
    }

    /// The trailing zero after the last field is what ENDS an
    /// ErrorResponse. Without it a client waits for more fields forever,
    /// so the error becomes a hang.
    #[test]
    fn error_response_is_terminated() {
        let mut buf = [0u8; 128];
        let mut w = Writer::new(&mut buf);
        error_response(&mut w, b"ERROR", b"42601", b"syntax error");
        let (kind, payload) = one_message(w.finished().unwrap());
        assert_eq!(kind, B_ERROR_RESPONSE);
        assert_eq!(*payload.last().unwrap(), 0, "field list must be terminated");
        // The three fields a client needs are all present and typed.
        assert!(payload.starts_with(b"SERROR\0"));
        assert!(payload.windows(7).any(|w| w == b"C42601\0"));
        assert!(payload.windows(14).any(|w| w == b"Msyntax error\0"));
    }

    /// NULL is length -1 and an empty value is length 0. Collapsing them
    /// would make `NULL` and `''` indistinguishable to the client.
    #[test]
    fn null_and_empty_cells_differ_on_the_wire() {
        let mut buf = [0u8; 64];
        let mut w = Writer::new(&mut buf);
        data_row_begin(&mut w, 2);
        data_row_cell(&mut w, None);
        data_row_cell(&mut w, Some(b""));
        w.finish();
        let (kind, payload) = one_message(w.finished().unwrap());
        assert_eq!(kind, B_DATA_ROW);
        assert_eq!(&payload[0..2], &2i16.to_be_bytes());
        assert_eq!(&payload[2..6], &(-1i32).to_be_bytes(), "NULL is -1");
        assert_eq!(&payload[6..10], &0i32.to_be_bytes(), "empty is 0");
        assert_eq!(payload.len(), 10);
    }

    #[test]
    fn row_description_declares_text_format() {
        let mut buf = [0u8; 128];
        let mut w = Writer::new(&mut buf);
        row_description(
            &mut w,
            &[FieldDesc {
                name: b"id",
                type_oid: OID_INT4,
                type_size: 4,
                type_mod: -1,
            }],
        );
        let (kind, payload) = one_message(w.finished().unwrap());
        assert_eq!(kind, B_ROW_DESCRIPTION);
        assert_eq!(&payload[0..2], &1i16.to_be_bytes());
        // [count:2] then per field:
        //   name\0 + table(4) + attnum(2) + oid(4) + size(2) + mod(4) + fmt(2)
        let name_at = 2;
        let after_name = name_at + 3;
        assert_eq!(&payload[name_at..after_name], b"id\0");
        let oid_at = after_name + 4 + 2;
        assert_eq!(
            i32::from_be_bytes(payload[oid_at..oid_at + 4].try_into().unwrap()),
            OID_INT4
        );
        let fmt_at = payload.len() - 2;
        assert_eq!(
            i16::from_be_bytes(payload[fmt_at..].try_into().unwrap()),
            0,
            "every value this connector sends is text"
        );
    }

    /// A message left open, or one that overflowed, must not reach the
    /// socket: the client frames by length, so a truncated message
    /// desyncs the stream permanently.
    #[test]
    fn unfinished_or_overflowed_output_is_not_shippable() {
        let mut buf = [0u8; 64];
        let mut w = Writer::new(&mut buf);
        w.begin(B_DATA_ROW).i16(1);
        assert!(w.finished().is_none(), "an open message is not shippable");

        let mut small = [0u8; 6];
        let mut w = Writer::new(&mut small);
        parameter_status(&mut w, b"a_very_long_key", b"and_value");
        assert!(w.overflowed());
        assert!(w.finished().is_none());
    }

    // ── Startup parsing ──────────────────────────────────────────────

    fn startup_bytes(code: i32, params: &[u8]) -> Vec<u8> {
        let len = (8 + params.len()) as i32;
        let mut v = Vec::new();
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(&code.to_be_bytes());
        v.extend_from_slice(params);
        v
    }

    /// A partial read must be "incomplete", not "malformed" — bytes split
    /// across packets are normal, and treating them as an error would
    /// close a perfectly good connection.
    #[test]
    fn a_partial_startup_is_incomplete_not_an_error() {
        let full = startup_bytes(PROTOCOL_3_0, b"user\0me\0\0");
        for cut in 0..full.len() {
            assert!(
                matches!(parse_startup(&full[..cut]), Ok(None)),
                "truncation to {cut} was not reported as incomplete"
            );
        }
        assert!(matches!(
            parse_startup(&full),
            Ok(Some((Startup::Params { .. }, _)))
        ));
    }

    #[test]
    fn ssl_and_cancel_requests_are_recognised_by_their_magic() {
        assert!(matches!(
            parse_startup(&startup_bytes(SSL_REQUEST_CODE, b"")),
            Ok(Some((Startup::SslRequest, 8)))
        ));
        assert!(matches!(
            parse_startup(&startup_bytes(GSSENC_REQUEST_CODE, b"")),
            Ok(Some((Startup::GssEncRequest, 8)))
        ));
        assert!(matches!(
            parse_startup(&startup_bytes(CANCEL_REQUEST_CODE, b"\0\0\0\x01\0\0\0\x02")),
            Ok(Some((Startup::CancelRequest, _)))
        ));
    }

    /// A nonsense length is an error rather than a huge allocation or a
    /// wait that never ends.
    #[test]
    fn an_impossible_startup_length_is_an_error() {
        let mut v = 4i32.to_be_bytes().to_vec();
        v.extend_from_slice(&PROTOCOL_3_0.to_be_bytes());
        assert!(parse_startup(&v).is_err());
        let mut v = i32::MAX.to_be_bytes().to_vec();
        v.extend_from_slice(&PROTOCOL_3_0.to_be_bytes());
        assert!(parse_startup(&v).is_err());
    }

    #[test]
    fn startup_params_are_looked_up_by_key() {
        let params = b"user\0alice\0database\0shop\0application_name\0psql\0\0";
        assert_eq!(startup_param(params, b"user"), Some(b"alice".as_slice()));
        assert_eq!(startup_param(params, b"database"), Some(b"shop".as_slice()));
        assert_eq!(
            startup_param(params, b"application_name"),
            Some(b"psql".as_slice())
        );
        assert_eq!(startup_param(params, b"missing"), None);
    }

    /// A parameter block with no terminator must not run off the end.
    #[test]
    fn an_unterminated_param_block_does_not_overrun() {
        assert_eq!(startup_param(b"user\0alice", b"database"), None);
        assert_eq!(startup_param(b"user", b"user"), None);
        assert_eq!(startup_param(b"", b"user"), None);
    }

    // ── Frontend parsing ─────────────────────────────────────────────

    fn frontend_bytes(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![kind];
        v.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn a_query_round_trips() {
        let bytes = frontend_bytes(F_QUERY, b"SELECT 1\0");
        let (msg, n) = parse_frontend(&bytes).unwrap().unwrap();
        assert_eq!(n, bytes.len());
        assert_eq!(msg.kind, F_QUERY);
        assert_eq!(query_sql(msg.payload), Some(b"SELECT 1".as_slice()));
    }

    #[test]
    fn a_partial_frontend_message_is_incomplete() {
        let bytes = frontend_bytes(F_QUERY, b"SELECT 1\0");
        for cut in 0..bytes.len() {
            assert!(
                matches!(parse_frontend(&bytes[..cut]), Ok(None)),
                "truncation to {cut} was not reported as incomplete"
            );
        }
    }

    /// Two messages in one read are both consumed, in order. Clients
    /// pipeline, so a parser that handled only the first would stall.
    #[test]
    fn pipelined_messages_are_consumed_in_order() {
        let mut stream = frontend_bytes(F_QUERY, b"SELECT 1\0");
        stream.extend_from_slice(&frontend_bytes(F_TERMINATE, b""));
        let (first, n1) = parse_frontend(&stream).unwrap().unwrap();
        assert_eq!(first.kind, F_QUERY);
        let (second, n2) = parse_frontend(&stream[n1..]).unwrap().unwrap();
        assert_eq!(second.kind, F_TERMINATE);
        assert_eq!(n1 + n2, stream.len());
    }

    #[test]
    fn an_impossible_frontend_length_is_an_error() {
        assert!(parse_frontend(&[F_QUERY, 0, 0, 0, 3]).is_err());
        assert!(parse_frontend(&[F_QUERY, 0x7F, 0xFF, 0xFF, 0xFF]).is_err());
    }

    // ── Type mapping and rendering ───────────────────────────────────

    /// A wrong OID makes a CORRECT value unreadable, so each mapping is
    /// pinned to PostgreSQL's own well-known value.
    #[test]
    fn type_oids_are_postgres_own_values() {
        for (ty, oid) in [
            (LogicalType::Boolean, 16),
            (LogicalType::SmallInt, 21),
            (LogicalType::Int, 23),
            (LogicalType::BigInt, 20),
            (LogicalType::Float, 700),
            (LogicalType::Double, 701),
            (LogicalType::VarChar { length: 8 }, 1043),
            (LogicalType::Char { length: 8 }, 1042),
            (LogicalType::VarBinary { length: 8 }, 17),
            (LogicalType::Date, 1082),
            (LogicalType::Timestamp, 1114),
            (
                LogicalType::Decimal {
                    precision: 5,
                    scale: 2,
                },
                1700,
            ),
        ] {
            assert_eq!(type_oid(ty), oid, "{ty:?}");
        }
    }

    /// PostgreSQL reports a declared width as `n + 4`; a client subtracts
    /// the 4 to display it. Reporting `n` directly would make `\d` show
    /// the wrong width.
    #[test]
    fn varchar_typmod_carries_the_varlena_offset() {
        assert_eq!(type_mod(LogicalType::VarChar { length: 10 }), 14);
        assert_eq!(type_mod(LogicalType::Char { length: 3 }), 7);
        assert_eq!(type_mod(LogicalType::Int), -1);
    }

    /// The rendering difference that justifies the whole
    /// executor/connector split: PostgreSQL writes `t`/`f`.
    #[test]
    fn booleans_render_as_t_and_f() {
        let mut out = [0u8; 8];
        let n = render_value(LogicalType::Boolean, Value::Boolean(true), &mut out).unwrap();
        assert_eq!(&out[..n], b"t");
        let n = render_value(LogicalType::Boolean, Value::Boolean(false), &mut out).unwrap();
        assert_eq!(&out[..n], b"f");
    }

    #[test]
    fn integers_render_in_decimal_including_the_extremes() {
        let mut out = [0u8; 32];
        for (v, want) in [
            (Value::Int(0), "0"),
            (Value::Int(-1), "-1"),
            (Value::Int(2147483647), "2147483647"),
            (Value::Int(-2147483648), "-2147483648"),
            (Value::BigInt(i64::MAX), "9223372036854775807"),
            // i64::MIN has no representable positive; the magnitude must
            // be taken without negating.
            (Value::BigInt(i64::MIN), "-9223372036854775808"),
        ] {
            let n = render_value(LogicalType::BigInt, v, &mut out).unwrap();
            assert_eq!(core::str::from_utf8(&out[..n]).unwrap(), want, "{v:?}");
        }
    }

    /// bytea renders in hex form (`\x…`), which is what clients parse by
    /// default; the escape form is deprecated.
    #[test]
    fn bytes_render_as_hex() {
        let mut out = [0u8; 32];
        let n = render_value(
            LogicalType::VarBinary { length: 8 },
            Value::Bytes(&[0x00, 0xAB, 0xFF]),
            &mut out,
        )
        .unwrap();
        assert_eq!(&out[..n], b"\\x00abff");
    }

    #[test]
    fn text_renders_byte_exactly() {
        let mut out = [0u8; 32];
        let n = render_value(
            LogicalType::VarChar { length: 32 },
            Value::Text("héllo".as_bytes()),
            &mut out,
        )
        .unwrap();
        assert_eq!(&out[..n], "héllo".as_bytes());
    }

    /// A decimal renders by INSERTING the point, not by dividing: the
    /// exact digits are already in the unscaled integer, and dividing
    /// would both lose precision and need an i128 division the PIC build
    /// has no intrinsic for.
    #[test]
    fn decimals_render_exactly() {
        let mut out = [0u8; 48];
        for (unscaled, scale, want) in [
            (12345i128, 2u8, "123.45"),
            (-12345, 2, "-123.45"),
            (5, 3, "0.005"),
            (-5, 3, "-0.005"),
            (0, 0, "0"),
            (0, 2, "0.00"),
            (100, 0, "100"),
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
            assert_eq!(
                core::str::from_utf8(&out[..n]).unwrap(),
                want,
                "unscaled {unscaled} scale {scale}"
            );
        }
    }

    /// NULL has no text rendering — it is a length of -1 on the wire, and
    /// a caller that tried to render one has confused absence with
    /// emptiness.
    #[test]
    fn null_has_no_rendering() {
        let mut out = [0u8; 8];
        assert!(render_value(LogicalType::Int, Value::Null, &mut out).is_none());
    }

    /// A value that does not fit refuses rather than rendering short — a
    /// truncated number is a different number.
    #[test]
    fn an_undersized_buffer_refuses_rather_than_truncating() {
        let mut out = [0u8; 2];
        assert!(render_value(LogicalType::BigInt, Value::BigInt(123456), &mut out).is_none());
    }

    // ── Command tags ─────────────────────────────────────────────────

    /// `psql` parses the row count out of these strings, so the exact
    /// shapes are part of the protocol. `INSERT`'s middle field is a
    /// legacy OID and is always 0.
    #[test]
    fn command_tags_have_the_shapes_clients_parse() {
        let mut out = [0u8; 32];
        for (tag, affected, rows, want) in [
            (sql_exec::TAG_CREATE_TABLE, 0u64, 0u16, "CREATE TABLE"),
            (sql_exec::TAG_DROP_TABLE, 0, 0, "DROP TABLE"),
            (sql_exec::TAG_INSERT, 3, 0, "INSERT 0 3"),
            (sql_exec::TAG_SELECT, 0, 7, "SELECT 7"),
            (sql_exec::TAG_BEGIN, 0, 0, "BEGIN"),
            (sql_exec::TAG_COMMIT, 0, 0, "COMMIT"),
            (sql_exec::TAG_SET, 0, 0, "SET"),
        ] {
            let n = command_tag(tag, affected, rows, &mut out).unwrap();
            assert_eq!(core::str::from_utf8(&out[..n]).unwrap(), want);
        }
    }

    // ── Error mapping ────────────────────────────────────────────────

    /// A client branches on the SQLSTATE CLASS, so the important property
    /// is that conditions needing different client behaviour land in
    /// different classes — not that any particular string appears.
    #[test]
    fn error_codes_land_in_meaningful_sqlstate_classes() {
        use sql_exec as x;
        // Class 42: the statement is wrong; retrying verbatim will fail.
        for c in [x::ERR_SYNTAX, x::ERR_UNKNOWN_COLUMN, x::ERR_NO_SUCH_TABLE] {
            assert_eq!(&sqlstate(c)[..2], b"42", "code {c}");
        }
        // Class 23: integrity violation, a fact about the data.
        assert_eq!(sqlstate(x::ERR_DUPLICATE_KEY), b"23505");
        assert_eq!(sqlstate(x::ERR_NOT_NULL), b"23502");
        // Class 0A: not supported — permanently, so a client should not
        // retry.
        assert_eq!(sqlstate(x::ERR_UNSUPPORTED), b"0A000");
        // Class 53: resource condition — a retry MAY succeed, which is a
        // different instruction to the client.
        assert_eq!(&sqlstate(x::ERR_BUSY)[..2], b"53");
    }

    /// Every distinct outcome code gets a distinct message, so a log
    /// reader can tell them apart even without the SQLSTATE.
    #[test]
    fn every_error_code_has_its_own_message() {
        use sql_exec as x;
        let codes = [
            x::ERR_TOO_LONG,
            x::ERR_UNEXPECTED_END,
            x::ERR_SYNTAX,
            x::ERR_NAME_TOO_LONG,
            x::ERR_LITERAL_TOO_LONG,
            x::ERR_TOO_MANY_ITEMS,
            x::ERR_UNTERMINATED_LITERAL,
            x::ERR_UNKNOWN_TYPE,
            x::ERR_UNKNOWN_COLUMN,
            x::ERR_TYPE_MISMATCH,
            x::ERR_ARITY_MISMATCH,
            x::ERR_NOT_NULL,
            x::ERR_MISSING_PRIMARY_KEY,
            x::ERR_DUPLICATE_COLUMN,
            x::ERR_UNSUPPORTED,
            x::ERR_NO_SUCH_TABLE,
            x::ERR_TABLE_EXISTS,
            x::ERR_STORE,
            x::ERR_RESULT_TOO_LARGE,
            x::ERR_BUSY,
            x::ERR_DUPLICATE_KEY,
            x::ERR_CORRUPT,
        ];
        for (i, a) in codes.iter().enumerate() {
            for b in &codes[i + 1..] {
                assert_ne!(
                    error_message(*a),
                    error_message(*b),
                    "codes {a} and {b} share a message"
                );
            }
        }
    }

    /// A `Query` whose string is not terminated is refused rather than
    /// executed as whatever bytes happened to follow.
    #[test]
    fn an_unterminated_query_string_is_refused() {
        let bytes = frontend_bytes(F_QUERY, b"SELECT 1");
        let (msg, _) = parse_frontend(&bytes).unwrap().unwrap();
        assert_eq!(query_sql(msg.payload), None);
    }
}
