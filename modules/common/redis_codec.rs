//! Pure-logic Redis (RESP2) codec — extracted from
//! `modules/app/redis_edge_anchor/` so the same source compiles into
//! both the no_std PIC anchor and the host-side integration tests at
//! `tests/integration_redis.rs`.
//!
//! What's here:
//!
//! - RESP2 parser (`parse_one` + helpers) reading from a `&[u8]`
//!   buffer into an `Argv` of `(offset, len)` views.
//! - RESP encoders (`enc_simple_str`, `enc_error`, `enc_integer`,
//!   `enc_bulk`, `enc_null_bulk`, `enc_array_header`) appending into
//!   a caller-provided `&mut [u8]` + `&mut usize` send-length cursor.
//! - Op-body builder (`build_op_body`) translating an `Argv` over RESP
//!   into the typed `KV_OP_*` body shape consumed by `kv_state_worker`
//!   via `modules/common/kv_store.rs::apply`.
//! - Reply formatter (`encode_kv_reply`) translating a `KV_RESULT_*`
//!   code + body back into RESP wire format.
//!
//! What stays in the anchor: per-slot session state (protocol version
//! negotiated via HELLO, MULTI queueing, PUBSUB count, AUTH flag,
//! SELECT db_index) and the NET_CMD_* TCP state machine. Those are
//! genuinely stateful per connection; the codec is stateless.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor and host tests; each consumer uses a subset"
)]

#[path = "types.rs"]
mod types;

use types::{
    KV_ARRAY_ELEMENT_NULL, KV_OP_APPEND, KV_OP_DECR, KV_OP_DELETE, KV_OP_EXISTS, KV_OP_FLUSH,
    KV_OP_GET, KV_OP_INCR, KV_OP_MGET, KV_OP_MSET, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_OP_SCAN,
    KV_OP_STRLEN, KV_RESULT_ARRAY, KV_RESULT_CAS_FAILED, KV_RESULT_DIRTY_EPOCH, KV_RESULT_EXISTS,
    KV_RESULT_INTEGER, KV_RESULT_LIN_BOUND, KV_RESULT_NOT_FOUND, KV_RESULT_OK, KV_RESULT_QUOTA,
    KV_RESULT_RANGE, KV_RESULT_SCAN_CURSOR, KV_RESULT_UNAUTH, KV_RESULT_WRONG_TYPE, PUT_FLAG_GET,
    PUT_FLAG_KEEPTTL, PUT_FLAG_NX, PUT_FLAG_XX, REQ_LINEARIZABLE, REQ_SERIALIZABLE,
};

// ── Time-series (RFC database foundation §14.19, Phase 8) ────────────
//
// TS.ADD / TS.RANGE serve the time-series capability over the redis
// wire (real clients: redis-cli and every RedisTimeSeries SDK speak
// these). Samples are canonical KV records under KS_TIMESERIES_SAMPLE
// with the key layout `models::encode_timeseries_sample_key` defines:
// `[series_id:u64 BE][bucket:u64 BE][timestamp:u64 BE]`, all
// ascending — a time window is a forward range scan. The constants and
// layout are DUPLICATED here rather than mounting `models.rs` (which
// carries the whole relational tree) — `tests/integration_redis.rs`
// asserts byte-identity against the models encoder, the same tripwire
// discipline `fnv1a64` uses.
//
// §14.19's committed-input rule shows up on the wire: `TS.ADD key *`
// (server-assigned time) REFUSES — a replica-sampled timestamp is
// exactly what the contract forbids; the client states the time it
// means.

/// Byte-identical to `models::KS_TIMESERIES_SAMPLE`.
pub const KS_TIMESERIES_SAMPLE: u32 = 0x8004_0002;
/// Fixed time-bucket width, ms (1 hour). Schema-declared widths arrive
/// with the rollup worker; a fixed width changes no key ordering.
pub const TS_BUCKET_MS: u64 = 3_600_000;

/// FNV-1a 64. MUST stay byte-identical to `kv_store::fnv1a64` and
/// `partition_map::fnv1a64` (same duplication discipline, same
/// reason: this contract file must not depend on a provider file).
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// The KV user key of one sample: `[keyspace:u32 BE]` + the models
/// sample-key body. Series identity is the FNV-1a of the series name —
/// the same hash the router's dependency barrier uses.
pub fn ts_sample_user_key(out: &mut [u8], series: &[u8], timestamp: u64) -> Option<usize> {
    if out.len() < 4 + 24 {
        return None;
    }
    let sid = fnv1a64(series);
    out[0..4].copy_from_slice(&KS_TIMESERIES_SAMPLE.to_be_bytes());
    out[4..12].copy_from_slice(&sid.to_be_bytes());
    out[12..20].copy_from_slice(&(timestamp / TS_BUCKET_MS).to_be_bytes());
    out[20..28].copy_from_slice(&timestamp.to_be_bytes());
    Some(28)
}

/// Parse a decimal u64 from an argument. No sign, no '*'.
fn parse_u64(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.len() > 20 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(v)
}

// ── Argv / parser types ───────────────────────────────────────────────

/// Maximum argv count per parsed command. Larger requests fail with
/// `-ERR Protocol error: too many arguments`.
pub const MAX_ARGS: usize = 32;

#[derive(Clone, Copy)]
pub struct ArgView {
    pub offset: u16,
    pub len: u16,
}

pub struct Argv {
    pub args: [ArgView; MAX_ARGS],
    pub count: u8,
}

impl Default for Argv {
    fn default() -> Self {
        Self::new()
    }
}

impl Argv {
    pub const fn new() -> Self {
        Self {
            args: [ArgView { offset: 0, len: 0 }; MAX_ARGS],
            count: 0,
        }
    }

    /// Convenience: byte slice for argv[i], or empty if out of range.
    pub fn arg<'a>(&self, recv: &'a [u8], i: usize) -> &'a [u8] {
        if i >= self.count as usize {
            return &[];
        }
        let v = self.args[i];
        let off = v.offset as usize;
        let len = v.len as usize;
        if off + len > recv.len() {
            return &[];
        }
        &recv[off..off + len]
    }
}

pub enum ParseStep {
    /// Need more bytes.
    Incomplete,
    /// Protocol error — caller closes the connection with this reply.
    Error(&'static [u8]),
    /// Complete command. `consumed` bytes were used; `argv` offsets
    /// point into the original buffer.
    Ready { consumed: usize, argv: Argv },
}

// ── RESP parser ───────────────────────────────────────────────────────

pub fn parse_one(buf: &[u8]) -> ParseStep {
    if buf.is_empty() {
        return ParseStep::Incomplete;
    }
    match buf[0] {
        b'*' => parse_resp_array(buf),
        _ => parse_inline(buf),
    }
}

fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

pub fn parse_i64(slice: &[u8]) -> Option<i64> {
    if slice.is_empty() {
        return None;
    }
    let (neg, digits) = if slice[0] == b'-' {
        (true, &slice[1..])
    } else if slice[0] == b'+' {
        (false, &slice[1..])
    } else {
        (false, slice)
    };
    if digits.is_empty() {
        return None;
    }
    let mut acc: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    Some(if neg { -acc } else { acc })
}

fn parse_resp_array(buf: &[u8]) -> ParseStep {
    debug_assert_eq!(buf[0], b'*');
    let Some(line_end) = find_crlf(buf, 1) else {
        return ParseStep::Incomplete;
    };
    let Some(n) = parse_i64(&buf[1..line_end]) else {
        return ParseStep::Error(b"-ERR Protocol error: invalid array length\r\n");
    };
    if n < 0 {
        return ParseStep::Ready {
            consumed: line_end + 2,
            argv: Argv::new(),
        };
    }
    let arg_count = n as usize;
    if arg_count > MAX_ARGS {
        return ParseStep::Error(b"-ERR Protocol error: too many arguments\r\n");
    }

    let mut argv = Argv::new();
    let mut cursor = line_end + 2;
    let mut parsed: usize = 0;

    while parsed < arg_count {
        if cursor >= buf.len() {
            return ParseStep::Incomplete;
        }
        if buf[cursor] != b'$' {
            return ParseStep::Error(b"-ERR Protocol error: expected bulk string\r\n");
        }
        let Some(len_end) = find_crlf(buf, cursor + 1) else {
            return ParseStep::Incomplete;
        };
        let Some(blen) = parse_i64(&buf[cursor + 1..len_end]) else {
            return ParseStep::Error(b"-ERR Protocol error: invalid bulk length\r\n");
        };
        if blen < 0 {
            argv.args[parsed] = ArgView { offset: 0, len: 0 };
            parsed += 1;
            cursor = len_end + 2;
            continue;
        }
        let blen = blen as usize;
        if blen > u16::MAX as usize {
            return ParseStep::Error(b"-ERR Protocol error: bulk string too large\r\n");
        }
        let body_start = len_end + 2;
        let body_end = body_start.saturating_add(blen);
        if body_end + 2 > buf.len() {
            return ParseStep::Incomplete;
        }
        if buf[body_end] != b'\r' || buf[body_end + 1] != b'\n' {
            return ParseStep::Error(b"-ERR Protocol error: missing CRLF after bulk\r\n");
        }
        argv.args[parsed] = ArgView {
            offset: body_start as u16,
            len: blen as u16,
        };
        parsed += 1;
        cursor = body_end + 2;
    }

    argv.count = parsed as u8;
    ParseStep::Ready {
        consumed: cursor,
        argv,
    }
}

fn parse_inline(buf: &[u8]) -> ParseStep {
    let Some(line_end) = find_crlf(buf, 0) else {
        return ParseStep::Incomplete;
    };
    let mut argv = Argv::new();
    let mut start = 0usize;
    let mut count = 0usize;

    while start < line_end {
        while start < line_end && (buf[start] == b' ' || buf[start] == b'\t') {
            start += 1;
        }
        if start >= line_end {
            break;
        }
        let mut end = start;
        while end < line_end && buf[end] != b' ' && buf[end] != b'\t' {
            end += 1;
        }
        if count >= MAX_ARGS {
            return ParseStep::Error(b"-ERR Protocol error: too many arguments\r\n");
        }
        argv.args[count] = ArgView {
            offset: start as u16,
            len: (end - start) as u16,
        };
        count += 1;
        start = end;
    }
    argv.count = count as u8;
    ParseStep::Ready {
        consumed: line_end + 2,
        argv,
    }
}

// ── RESP encoders ─────────────────────────────────────────────────────

pub fn enc_raw(send: &mut [u8], send_len: &mut usize, bytes: &[u8]) -> bool {
    if bytes.len() > send.len() - *send_len {
        return false;
    }
    let n = *send_len;
    send[n..n + bytes.len()].copy_from_slice(bytes);
    *send_len = n + bytes.len();
    true
}

pub fn enc_simple_str(send: &mut [u8], send_len: &mut usize, s: &[u8]) -> bool {
    enc_raw(send, send_len, b"+") && enc_raw(send, send_len, s) && enc_raw(send, send_len, b"\r\n")
}

pub fn enc_error(send: &mut [u8], send_len: &mut usize, s: &[u8]) -> bool {
    enc_raw(send, send_len, b"-") && enc_raw(send, send_len, s) && enc_raw(send, send_len, b"\r\n")
}

pub fn enc_integer(send: &mut [u8], send_len: &mut usize, n: i64) -> bool {
    let mut tmp = [0u8; 24];
    let w = itoa(n, &mut tmp);
    enc_raw(send, send_len, b":")
        && enc_raw(send, send_len, &tmp[..w])
        && enc_raw(send, send_len, b"\r\n")
}

pub fn enc_null_bulk(send: &mut [u8], send_len: &mut usize) -> bool {
    enc_raw(send, send_len, b"$-1\r\n")
}

pub fn enc_bulk(send: &mut [u8], send_len: &mut usize, bytes: &[u8]) -> bool {
    let mut tmp = [0u8; 24];
    let w = itoa(bytes.len() as i64, &mut tmp);
    enc_raw(send, send_len, b"$")
        && enc_raw(send, send_len, &tmp[..w])
        && enc_raw(send, send_len, b"\r\n")
        && enc_raw(send, send_len, bytes)
        && enc_raw(send, send_len, b"\r\n")
}

pub fn enc_array_header(send: &mut [u8], send_len: &mut usize, n: i64) -> bool {
    let mut tmp = [0u8; 24];
    let w = itoa(n, &mut tmp);
    enc_raw(send, send_len, b"*")
        && enc_raw(send, send_len, &tmp[..w])
        && enc_raw(send, send_len, b"\r\n")
}

/// Minimal no_std integer→decimal serialisation. Writes into `out`
/// and returns the byte count. `out` must be ≥ 24 bytes.
pub fn itoa(n: i64, out: &mut [u8]) -> usize {
    if n == 0 {
        out[0] = b'0';
        return 1;
    }
    let (neg, mut v) = if n < 0 {
        (true, (-(n as i128)) as u128)
    } else {
        (false, n as u128)
    };
    let mut buf = [0u8; 24];
    let mut idx = 24;
    while v > 0 {
        idx -= 1;
        buf[idx] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    let digits = 24 - idx;
    let mut write_at = 0;
    if neg {
        out[0] = b'-';
        write_at = 1;
    }
    out[write_at..write_at + digits].copy_from_slice(&buf[idx..]);
    write_at + digits
}

pub fn eq_ascii_ci(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for i in 0..a.len() {
        if (a[i] | 0x20) != (b[i] | 0x20) {
            return false;
        }
    }
    true
}

// ── Op-body builder ───────────────────────────────────────────────────

fn put_u8(buf: &mut [u8], at: &mut usize, v: u8) -> bool {
    if *at + 1 > buf.len() {
        return false;
    }
    buf[*at] = v;
    *at += 1;
    true
}

fn put_u16(buf: &mut [u8], at: &mut usize, v: u16) -> bool {
    if *at + 2 > buf.len() {
        return false;
    }
    buf[*at..*at + 2].copy_from_slice(&v.to_le_bytes());
    *at += 2;
    true
}

fn put_u32(buf: &mut [u8], at: &mut usize, v: u32) -> bool {
    if *at + 4 > buf.len() {
        return false;
    }
    buf[*at..*at + 4].copy_from_slice(&v.to_le_bytes());
    *at += 4;
    true
}

fn put_u64(buf: &mut [u8], at: &mut usize, v: u64) -> bool {
    if *at + 8 > buf.len() {
        return false;
    }
    buf[*at..*at + 8].copy_from_slice(&v.to_le_bytes());
    *at += 8;
    true
}

fn put_i64(buf: &mut [u8], at: &mut usize, v: i64) -> bool {
    if *at + 8 > buf.len() {
        return false;
    }
    buf[*at..*at + 8].copy_from_slice(&v.to_le_bytes());
    *at += 8;
    true
}

fn put_bytes(buf: &mut [u8], at: &mut usize, bytes: &[u8]) -> bool {
    if *at + bytes.len() > buf.len() {
        return false;
    }
    buf[*at..*at + bytes.len()].copy_from_slice(bytes);
    *at += bytes.len();
    true
}

fn put_key(buf: &mut [u8], at: &mut usize, key: &[u8]) -> bool {
    if key.len() > u16::MAX as usize {
        return false;
    }
    put_u16(buf, at, key.len() as u16) && put_bytes(buf, at, key)
}

fn put_value_u32(buf: &mut [u8], at: &mut usize, value: &[u8]) -> bool {
    if value.len() > u32::MAX as usize {
        return false;
    }
    put_u32(buf, at, value.len() as u32) && put_bytes(buf, at, value)
}

/// Outcome of classifying and building the typed body for a forwarded
/// command. The caller acts on each variant in the right context.
pub enum Built {
    /// Body buffer was populated; envelope can be sent.
    Ok {
        op: u8,
        consistency: u8,
        body_len: usize,
    },
    /// Validation error — caller emits the message as a RESP error.
    BadArgs(&'static [u8]),
    /// Unrecognised command — caller emits "ERR unknown command".
    Unknown,
}

/// Build the typed KV body for a forwarded redis command.
///
/// `recv` is the source buffer the argv views point into (typically
/// the anchor's `slot.recv_buf`). `body` is the output buffer to
/// stamp the body bytes into.
///
/// Returns `Built::Ok { op, consistency, body_len }` on success; the
/// caller wraps that with the MSG_KV_REQUEST envelope and writes it
/// to `kv_out`.
pub fn build_op_body(recv: &[u8], cmd: &[u8], argv: &Argv, body: &mut [u8]) -> Built {
    if eq_ascii_ci(cmd, b"GET") {
        if argv.count != 2 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'get'");
        }
        let mut at = 0;
        if !put_key(body, &mut at, argv.arg(recv, 1)) {
            return Built::BadArgs(b"ERR key too long");
        }
        return Built::Ok {
            op: KV_OP_GET,
            consistency: REQ_SERIALIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"STRLEN") {
        if argv.count != 2 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'strlen'");
        }
        let mut at = 0;
        if !put_key(body, &mut at, argv.arg(recv, 1)) {
            return Built::BadArgs(b"ERR key too long");
        }
        return Built::Ok {
            op: KV_OP_STRLEN,
            consistency: REQ_SERIALIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"EXISTS") {
        if argv.count < 2 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'exists'");
        }
        let mut at = 0;
        if !put_key(body, &mut at, argv.arg(recv, 1)) {
            return Built::BadArgs(b"ERR key too long");
        }
        return Built::Ok {
            op: KV_OP_EXISTS,
            consistency: REQ_SERIALIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"TS.ADD") {
        // TS.ADD key timestamp value
        if argv.count != 4 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'ts.add'");
        }
        let key = argv.arg(recv, 1);
        let ts_arg = argv.arg(recv, 2);
        let value = argv.arg(recv, 3);
        let Some(ts) = parse_u64(ts_arg) else {
            // '*' lands here too, deliberately: a server-assigned
            // sample time is a replica-sampled timestamp, which
            // §14.19 forbids. The client states the time it means.
            return Built::BadArgs(
                b"ERR TS.ADD needs an explicit millisecond timestamp (committed input)",
            );
        };
        let mut skey = [0u8; 32];
        let Some(kn) = ts_sample_user_key(&mut skey, key, ts) else {
            return Built::BadArgs(b"ERR key too long");
        };
        // PUT body: [klen][key][vlen][value][flags 0][expiry 0]. The
        // sample VALUE is the raw argument bytes: self-describing,
        // rendered back verbatim by TS.RANGE.
        let need = 2 + kn + 4 + value.len() + 1 + 8;
        if need > body.len() {
            return Built::BadArgs(b"ERR value too long");
        }
        let mut at = 0;
        body[at..at + 2].copy_from_slice(&(kn as u16).to_le_bytes());
        at += 2;
        body[at..at + kn].copy_from_slice(&skey[..kn]);
        at += kn;
        body[at..at + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
        at += 4;
        body[at..at + value.len()].copy_from_slice(value);
        at += value.len();
        body[at] = 0;
        at += 1;
        body[at..at + 8].copy_from_slice(&0u64.to_le_bytes());
        at += 8;
        return Built::Ok {
            op: KV_OP_PUT,
            consistency: REQ_SERIALIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"TS.RANGE") {
        // TS.RANGE key from to  (inclusive both ends, like the module)
        if argv.count != 4 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'ts.range'");
        }
        let key = argv.arg(recv, 1);
        let (Some(from), Some(to)) = (parse_u64(argv.arg(recv, 2)), parse_u64(argv.arg(recv, 3)))
        else {
            return Built::BadArgs(b"ERR TS.RANGE needs explicit millisecond bounds");
        };
        if from > to || to == u64::MAX {
            return Built::BadArgs(b"ERR invalid range");
        }
        let mut start = [0u8; 32];
        let mut end = [0u8; 32];
        let (Some(sn), Some(en)) = (
            ts_sample_user_key(&mut start, key, from),
            // Half-open upper bound: the key at to+1 (buckets ascend
            // with the timestamp, so this is the exact successor).
            ts_sample_user_key(&mut end, key, to + 1),
        ) else {
            return Built::BadArgs(b"ERR key too long");
        };
        let need = 2 + sn + 2 + en + 10;
        if need > body.len() {
            return Built::BadArgs(b"ERR key too long");
        }
        let mut at = 0;
        body[at..at + 2].copy_from_slice(&(sn as u16).to_le_bytes());
        at += 2;
        body[at..at + sn].copy_from_slice(&start[..sn]);
        at += sn;
        body[at..at + 2].copy_from_slice(&(en as u16).to_le_bytes());
        at += 2;
        body[at..at + en].copy_from_slice(&end[..en]);
        at += en;
        body[at..at + 8].copy_from_slice(&0u64.to_le_bytes());
        at += 8;
        body[at..at + 2].copy_from_slice(&128u16.to_le_bytes());
        at += 2;
        return Built::Ok {
            op: KV_OP_RANGE_SCAN,
            consistency: REQ_SERIALIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"SET") {
        if argv.count < 3 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'set'");
        }
        let key = argv.arg(recv, 1);
        let value = argv.arg(recv, 2);
        let mut flags: u8 = 0;
        let mut expiry_ms: u64 = 0;
        let mut i = 3u8;
        while (i as usize) < argv.count as usize {
            let opt = argv.arg(recv, i as usize);
            if eq_ascii_ci(opt, b"NX") {
                flags |= PUT_FLAG_NX;
                i += 1;
            } else if eq_ascii_ci(opt, b"XX") {
                flags |= PUT_FLAG_XX;
                i += 1;
            } else if eq_ascii_ci(opt, b"GET") {
                flags |= PUT_FLAG_GET;
                i += 1;
            } else if eq_ascii_ci(opt, b"KEEPTTL") {
                flags |= PUT_FLAG_KEEPTTL;
                i += 1;
            } else if eq_ascii_ci(opt, b"EX") || eq_ascii_ci(opt, b"PX") {
                let is_seconds = eq_ascii_ci(opt, b"EX");
                if i as usize + 1 >= argv.count as usize {
                    return Built::BadArgs(b"ERR syntax error");
                }
                let val = argv.arg(recv, i as usize + 1);
                let Some(n) = parse_i64(val) else {
                    return Built::BadArgs(b"ERR value is not an integer or out of range");
                };
                if n <= 0 {
                    return Built::BadArgs(b"ERR invalid expire time in 'set' command");
                }
                expiry_ms = if is_seconds {
                    (n as u64) * 1000
                } else {
                    n as u64
                };
                i += 2;
            } else {
                return Built::BadArgs(b"ERR syntax error");
            }
        }
        if (flags & PUT_FLAG_NX) != 0 && (flags & PUT_FLAG_XX) != 0 {
            return Built::BadArgs(b"ERR XX and NX options at the same time are not compatible");
        }
        let mut at = 0;
        if !put_key(body, &mut at, key)
            || !put_value_u32(body, &mut at, value)
            || !put_u8(body, &mut at, flags)
            || !put_u64(body, &mut at, expiry_ms)
        {
            return Built::BadArgs(b"ERR command body too large");
        }
        return Built::Ok {
            op: KV_OP_PUT,
            consistency: REQ_LINEARIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"SETNX") {
        if argv.count != 3 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'setnx'");
        }
        let mut at = 0;
        if !put_key(body, &mut at, argv.arg(recv, 1))
            || !put_value_u32(body, &mut at, argv.arg(recv, 2))
            || !put_u8(body, &mut at, PUT_FLAG_NX)
            || !put_u64(body, &mut at, 0)
        {
            return Built::BadArgs(b"ERR command body too large");
        }
        return Built::Ok {
            op: KV_OP_PUT,
            consistency: REQ_LINEARIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"GETSET") {
        if argv.count != 3 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'getset'");
        }
        let mut at = 0;
        if !put_key(body, &mut at, argv.arg(recv, 1))
            || !put_value_u32(body, &mut at, argv.arg(recv, 2))
            || !put_u8(body, &mut at, PUT_FLAG_GET)
            || !put_u64(body, &mut at, 0)
        {
            return Built::BadArgs(b"ERR command body too large");
        }
        return Built::Ok {
            op: KV_OP_PUT,
            consistency: REQ_LINEARIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"DEL") || eq_ascii_ci(cmd, b"UNLINK") {
        if argv.count < 2 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'del'");
        }
        let key_count = (argv.count as usize) - 1;
        let mut at = 0;
        if !put_u16(body, &mut at, key_count as u16) {
            return Built::BadArgs(b"ERR too many keys");
        }
        let mut i = 1u8;
        while (i as usize) < argv.count as usize {
            if !put_key(body, &mut at, argv.arg(recv, i as usize)) {
                return Built::BadArgs(b"ERR command body too large");
            }
            i += 1;
        }
        return Built::Ok {
            op: KV_OP_DELETE,
            consistency: REQ_LINEARIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"INCR")
        || eq_ascii_ci(cmd, b"INCRBY")
        || eq_ascii_ci(cmd, b"DECR")
        || eq_ascii_ci(cmd, b"DECRBY")
    {
        let is_incr = eq_ascii_ci(cmd, b"INCR") || eq_ascii_ci(cmd, b"INCRBY");
        let by_idx = if eq_ascii_ci(cmd, b"INCRBY") || eq_ascii_ci(cmd, b"DECRBY") {
            if argv.count != 3 {
                return Built::BadArgs(b"ERR wrong number of arguments");
            }
            Some(2)
        } else {
            if argv.count != 2 {
                return Built::BadArgs(b"ERR wrong number of arguments");
            }
            None
        };
        let delta: i64 = match by_idx {
            Some(idx) => match parse_i64(argv.arg(recv, idx)) {
                Some(n) => n,
                None => return Built::BadArgs(b"ERR value is not an integer or out of range"),
            },
            None => 1,
        };
        let mut at = 0;
        if !put_key(body, &mut at, argv.arg(recv, 1)) || !put_i64(body, &mut at, delta) {
            return Built::BadArgs(b"ERR command body too large");
        }
        return Built::Ok {
            op: if is_incr { KV_OP_INCR } else { KV_OP_DECR },
            consistency: REQ_LINEARIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"APPEND") {
        if argv.count != 3 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'append'");
        }
        let mut at = 0;
        if !put_key(body, &mut at, argv.arg(recv, 1))
            || !put_value_u32(body, &mut at, argv.arg(recv, 2))
        {
            return Built::BadArgs(b"ERR command body too large");
        }
        return Built::Ok {
            op: KV_OP_APPEND,
            consistency: REQ_LINEARIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"MGET") {
        if argv.count < 2 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'mget'");
        }
        let key_count = (argv.count as usize) - 1;
        let mut at = 0;
        if !put_u16(body, &mut at, key_count as u16) {
            return Built::BadArgs(b"ERR too many keys");
        }
        let mut i = 1u8;
        while (i as usize) < argv.count as usize {
            if !put_key(body, &mut at, argv.arg(recv, i as usize)) {
                return Built::BadArgs(b"ERR command body too large");
            }
            i += 1;
        }
        return Built::Ok {
            op: KV_OP_MGET,
            consistency: REQ_SERIALIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"MSET") {
        if argv.count < 3 || !(argv.count - 1).is_multiple_of(2) {
            return Built::BadArgs(b"ERR wrong number of arguments for 'mset'");
        }
        let pair_count = ((argv.count as usize) - 1) / 2;
        let mut at = 0;
        if !put_u16(body, &mut at, pair_count as u16) {
            return Built::BadArgs(b"ERR too many pairs");
        }
        let mut i = 1u8;
        while (i as usize) < argv.count as usize {
            if !put_key(body, &mut at, argv.arg(recv, i as usize))
                || !put_value_u32(body, &mut at, argv.arg(recv, i as usize + 1))
            {
                return Built::BadArgs(b"ERR command body too large");
            }
            i += 2;
        }
        return Built::Ok {
            op: KV_OP_MSET,
            consistency: REQ_LINEARIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"SCAN") {
        if argv.count < 2 {
            return Built::BadArgs(b"ERR wrong number of arguments for 'scan'");
        }
        let cursor = parse_i64(argv.arg(recv, 1)).unwrap_or(0).max(0) as u64;
        let mut limit: u16 = 64;
        let mut i = 2u8;
        while (i as usize) < argv.count as usize {
            let opt = argv.arg(recv, i as usize);
            if eq_ascii_ci(opt, b"COUNT") && (i as usize) + 1 < argv.count as usize {
                if let Some(n) = parse_i64(argv.arg(recv, (i + 1) as usize)) {
                    limit = (n.max(1).min(u16::MAX as i64)) as u16;
                }
                i += 2;
            } else if eq_ascii_ci(opt, b"MATCH") && (i as usize) + 1 < argv.count as usize {
                i += 2;
            } else {
                i += 1;
            }
        }
        let mut at = 0;
        if !put_u64(body, &mut at, cursor) || !put_u16(body, &mut at, limit) {
            return Built::BadArgs(b"ERR scan body too large");
        }
        return Built::Ok {
            op: KV_OP_SCAN,
            consistency: REQ_SERIALIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"KEYS") {
        let mut at = 0;
        if !put_u64(body, &mut at, 0) || !put_u16(body, &mut at, u16::MAX) {
            return Built::BadArgs(b"ERR keys body too large");
        }
        return Built::Ok {
            op: KV_OP_SCAN,
            consistency: REQ_SERIALIZABLE,
            body_len: at,
        };
    }

    if eq_ascii_ci(cmd, b"FLUSHDB") || eq_ascii_ci(cmd, b"FLUSHALL") {
        return Built::Ok {
            op: KV_OP_FLUSH,
            consistency: REQ_LINEARIZABLE,
            body_len: 0,
        };
    }

    Built::Unknown
}

// ── Reply formatter ───────────────────────────────────────────────────

/// Translate a `KV_RESULT_*` code + body into RESP wire format,
/// appending to the caller-provided send buffer.
pub fn encode_kv_reply(send_buf: &mut [u8], send_len: &mut usize, result: u8, body: &[u8]) {
    let sb = send_buf;
    let sl = send_len;
    match result {
        KV_RESULT_OK => {
            if body.is_empty() {
                let _ = enc_simple_str(sb, sl, b"OK");
            } else {
                let _ = enc_bulk(sb, sl, body);
            }
        }
        KV_RESULT_NOT_FOUND => {
            let _ = enc_null_bulk(sb, sl);
        }
        KV_RESULT_EXISTS => {
            let _ = enc_integer(sb, sl, 1);
        }
        KV_RESULT_WRONG_TYPE => {
            let _ = enc_error(
                sb,
                sl,
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            );
        }
        KV_RESULT_CAS_FAILED => {
            let _ = enc_null_bulk(sb, sl);
        }
        KV_RESULT_QUOTA => {
            let _ = enc_error(sb, sl, b"BUSY tenant quota exceeded");
        }
        KV_RESULT_UNAUTH => {
            let _ = enc_error(sb, sl, b"NOAUTH Authentication required");
        }
        KV_RESULT_DIRTY_EPOCH => {
            let _ = enc_error(sb, sl, b"MOVED routing epoch advanced");
        }
        KV_RESULT_LIN_BOUND => {
            // Subsystem-neutral on purpose. This code means "the owning
            // authority could not answer", and it reaches here from two
            // very different places: a linearizable READ whose fence
            // never resolved, and any op — including a plain write —
            // that timed out with no completion signal, which is what a
            // write sent to a FOLLOWER does.
            //
            // The old text named the read fence unconditionally, so a
            // `SET` against a follower came back blaming a subsystem it
            // had never entered. A caller told the wrong cause looks in
            // the wrong place, which is worse than a vaguer message
            // that is true.
            let _ = enc_error(
                sb,
                sl,
                b"CLUSTERDOWN no authority available for this operation",
            );
        }
        KV_RESULT_INTEGER => {
            if body.len() >= 8 {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&body[..8]);
                let n = i64::from_le_bytes(buf);
                let _ = enc_integer(sb, sl, n);
            } else {
                let _ = enc_error(sb, sl, b"ERR malformed integer body");
            }
        }
        KV_RESULT_ARRAY => {
            if body.len() < 2 {
                let _ = enc_error(sb, sl, b"ERR malformed array body");
                return;
            }
            let count = u16::from_le_bytes([body[0], body[1]]) as usize;
            let _ = enc_array_header(sb, sl, count as i64);
            let mut off = 2;
            let mut emitted = 0;
            while emitted < count && off + 4 <= body.len() {
                let len =
                    u32::from_le_bytes([body[off], body[off + 1], body[off + 2], body[off + 3]]);
                off += 4;
                if len == KV_ARRAY_ELEMENT_NULL {
                    let _ = enc_null_bulk(sb, sl);
                } else {
                    let lu = len as usize;
                    if off + lu > body.len() {
                        return;
                    }
                    let _ = enc_bulk(sb, sl, &body[off..off + lu]);
                    off += lu;
                }
                emitted += 1;
            }
        }
        KV_RESULT_RANGE => {
            // Today the ONLY redis command that produces a RANGE
            // result is TS.RANGE, so this arm renders sample pairs:
            // an array of [timestamp, value] arrays. The timestamp is
            // the last 8 bytes of the sample KEY — self-describing,
            // like every result this codec renders.
            if body.len() < 10 {
                let _ = enc_error(sb, sl, b"ERR malformed range body");
                return;
            }
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            let _ = enc_array_header(sb, sl, count as i64);
            let mut off = 10usize;
            for _ in 0..count {
                if off + 2 > body.len() {
                    return;
                }
                let klen = u16::from_le_bytes([body[off], body[off + 1]]) as usize;
                let koff = off + 2;
                let voff = koff + klen;
                if voff + 4 > body.len() || klen < 8 {
                    return;
                }
                let vlen = u32::from_le_bytes([
                    body[voff],
                    body[voff + 1],
                    body[voff + 2],
                    body[voff + 3],
                ]) as usize;
                let vend = voff + 4 + vlen;
                if vend > body.len() {
                    return;
                }
                let ts = u64::from_be_bytes([
                    body[koff + klen - 8],
                    body[koff + klen - 7],
                    body[koff + klen - 6],
                    body[koff + klen - 5],
                    body[koff + klen - 4],
                    body[koff + klen - 3],
                    body[koff + klen - 2],
                    body[koff + klen - 1],
                ]);
                let _ = enc_array_header(sb, sl, 2);
                let _ = enc_integer(sb, sl, ts as i64);
                let _ = enc_bulk(sb, sl, &body[voff + 4..vend]);
                off = vend;
            }
        }
        KV_RESULT_SCAN_CURSOR => {
            if body.len() < 10 {
                let _ = enc_error(sb, sl, b"ERR malformed scan body");
                return;
            }
            let cursor = u64::from_le_bytes([
                body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
            ]);
            let count = u16::from_le_bytes([body[8], body[9]]) as usize;
            let _ = enc_array_header(sb, sl, 2);
            let mut cursor_buf = [0u8; 32];
            let cursor_len = itoa(cursor as i64, &mut cursor_buf);
            let _ = enc_bulk(sb, sl, &cursor_buf[..cursor_len]);
            let _ = enc_array_header(sb, sl, count as i64);
            let mut off = 10;
            let mut emitted = 0;
            while emitted < count && off + 4 <= body.len() {
                let len =
                    u32::from_le_bytes([body[off], body[off + 1], body[off + 2], body[off + 3]])
                        as usize;
                off += 4;
                if off + len > body.len() {
                    return;
                }
                let _ = enc_bulk(sb, sl, &body[off..off + len]);
                off += len;
                emitted += 1;
            }
        }
        _ => {
            // Name the code. Every result the KV layer can produce that
            // RESP has no shape for lands here, and an undifferentiated
            // "internal error" makes them indistinguishable from each
            // other and from a genuine fault — which is exactly how a
            // newly-added result code (KV_RESULT_TXN_PENDING) hid behind
            // the same six words as KV_RESULT_INTERNAL for two sessions.
            let mut buf = [0u8; 32];
            let mut n = 0;
            let prefix = b"ERR internal error (result ";
            buf[..prefix.len()].copy_from_slice(prefix);
            n += prefix.len();
            let mut digits = [0u8; 8];
            let dl = itoa(result as i64, &mut digits);
            buf[n..n + dl].copy_from_slice(&digits[..dl]);
            n += dl;
            buf[n] = b')';
            n += 1;
            let _ = enc_error(sb, sl, &buf[..n]);
        }
    }
}
