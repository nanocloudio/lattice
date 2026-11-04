//! Pure-logic Memcached (ASCII) codec — extracted from
//! `modules/app/memcached_stream_anchor/` so the same source compiles
//! into both the no_std PIC anchor and the host-side integration
//! tests at `tests/integration_memcache.rs`.
//!
//! What's here:
//!
//! - Line tokeniser (`parse_line`, `find_crlf`, `token_bytes`).
//! - Small ASCII helpers (`eq_ascii_ci`, `parse_u32_dec`,
//!   `parse_i64_dec`).
//! - `MemOp` enum identifying the originating memcached command —
//!   used by the anchor's per-corr_id inflight table so the reply
//!   formatter knows whether to emit `STORED` vs `DELETED` vs the
//!   numeric incr result.
//! - `encode_reply(send_buf, send_len, mem_op, key, result, revision, body)`
//!   translates a `KV_RESULT_*` code from `kv_state_worker` into the
//!   memcached wire byte sequence the client expects.
//! - Integer formatters (`itoa_u64`, `itoa_i64`).
//!
//! What stays in the anchor: TCP NET_CMD I/O, slot table, AnchorState,
//! the per-corr_id `InflightTable<InflightVal>`, and the per-command
//! `handle_get/storage/delete/incr/...` functions that build typed
//! KV bodies and call `send_envelope`. Those tie into channel I/O
//! and slot lifecycle; they can't be host-testable in isolation.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor and host tests; each consumer uses a subset"
)]

#[path = "types.rs"]
mod types;

use types::{
    KV_RESULT_CAS_FAILED, KV_RESULT_INTEGER, KV_RESULT_NOT_FOUND, KV_RESULT_OK,
    KV_RESULT_WRONG_TYPE,
};

// ── Tokeniser ─────────────────────────────────────────────────────────

pub const MAX_TOKENS: usize = 32;

#[derive(Clone, Copy)]
pub struct Token {
    pub offset: u16,
    pub len: u16,
}

pub struct Line {
    pub tokens: [Token; MAX_TOKENS],
    pub count: u8,
    /// Index just past the trailing `\r\n` of the parsed line.
    pub line_end: usize,
}

pub fn find_crlf(buf: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Tokenise the first CRLF-terminated line in `buf`. Returns `None`
/// if the line isn't fully present yet (caller waits for more data),
/// or if the line has more than `MAX_TOKENS` tokens.
pub fn parse_line(buf: &[u8]) -> Option<Line> {
    let crlf_at = find_crlf(buf)?;
    let mut tokens = [Token { offset: 0, len: 0 }; MAX_TOKENS];
    let mut count = 0usize;
    let mut i = 0usize;
    while i < crlf_at {
        while i < crlf_at && (buf[i] == b' ' || buf[i] == b'\t') {
            i += 1;
        }
        if i >= crlf_at {
            break;
        }
        let start = i;
        while i < crlf_at && buf[i] != b' ' && buf[i] != b'\t' {
            i += 1;
        }
        if count >= MAX_TOKENS {
            return None;
        }
        tokens[count] = Token {
            offset: start as u16,
            len: (i - start) as u16,
        };
        count += 1;
    }
    Some(Line {
        tokens,
        count: count as u8,
        line_end: crlf_at + 2,
    })
}

pub fn token_bytes(buf: &[u8], t: Token) -> &[u8] {
    &buf[t.offset as usize..t.offset as usize + t.len as usize]
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

pub fn parse_u32_dec(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() {
        return None;
    }
    let mut acc: u32 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add((b - b'0') as u32)?;
    }
    Some(acc)
}

pub fn parse_i64_dec(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, rest) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if rest.is_empty() {
        return None;
    }
    let mut acc: i64 = 0;
    for &b in rest {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    Some(if neg { -acc } else { acc })
}

// ── Integer formatters ────────────────────────────────────────────────

pub fn itoa_u64(mut n: u64, out: &mut [u8]) -> usize {
    if n == 0 {
        out[0] = b'0';
        return 1;
    }
    let mut buf = [0u8; 24];
    let mut idx = 24;
    while n > 0 {
        idx -= 1;
        buf[idx] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    let digits = 24 - idx;
    out[..digits].copy_from_slice(&buf[idx..]);
    digits
}

pub fn itoa_i64(n: i64, out: &mut [u8]) -> usize {
    if n < 0 {
        out[0] = b'-';
        1 + itoa_u64((-(n as i128)) as u64, &mut out[1..])
    } else {
        itoa_u64(n as u64, out)
    }
}

// ── Send-buffer append helper ─────────────────────────────────────────

pub fn append(send: &mut [u8], send_len: &mut usize, bytes: &[u8]) -> bool {
    if bytes.len() > send.len() - *send_len {
        return false;
    }
    let n = *send_len;
    send[n..n + bytes.len()].copy_from_slice(bytes);
    *send_len = n + bytes.len();
    true
}

pub fn append_decimal_u64(send: &mut [u8], send_len: &mut usize, n: u64) -> bool {
    let mut buf = [0u8; 24];
    let written = itoa_u64(n, &mut buf);
    append(send, send_len, &buf[..written])
}

// ── MemOp + encode_reply ──────────────────────────────────────────────

/// Originating memcached command. The anchor stamps this into the
/// per-corr_id inflight table when forwarding so the reply formatter
/// knows how to translate the KV_RESULT_* code back to memcached wire.
///
/// Numeric values match the `InflightVal::op: u8` field stored in the
/// anchor's inflight table.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MemOp {
    Get = 0,
    Gets = 1,
    Set = 2,
    Add = 3,
    Replace = 4,
    Append = 5,
    Prepend = 6,
    Cas = 7,
    Delete = 8,
    Incr = 9,
    Decr = 10,
    FlushAll = 11,
}

impl MemOp {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Get),
            1 => Some(Self::Gets),
            2 => Some(Self::Set),
            3 => Some(Self::Add),
            4 => Some(Self::Replace),
            5 => Some(Self::Append),
            6 => Some(Self::Prepend),
            7 => Some(Self::Cas),
            8 => Some(Self::Delete),
            9 => Some(Self::Incr),
            10 => Some(Self::Decr),
            11 => Some(Self::FlushAll),
            _ => None,
        }
    }
}

/// Translate a KV result into the memcached wire reply for a given
/// originating `MemOp`. `key` and `body` are the key the request was
/// for and the body returned by `kv_store::apply` respectively;
/// `revision` is the new `mod_revision` from the worker (used as the
/// CAS token for `gets`).
pub fn encode_reply(
    send_buf: &mut [u8],
    send_len: &mut usize,
    mem_op: MemOp,
    key: &[u8],
    result: u8,
    revision: u64,
    body: &[u8],
) {
    let sb = send_buf;
    let sl = send_len;

    // A write past what its path carries: memcached's own answer.
    if result == types::KV_RESULT_TOO_LARGE {
        let _ = append(sb, sl, b"SERVER_ERROR object too large for cache\r\n");
        return;
    }
    // An op the store does not implement (connector and store built
    // against different contracts): said as such, whatever the command.
    if result == types::KV_RESULT_UNSUPPORTED {
        let _ = append(sb, sl, b"SERVER_ERROR unsupported operation\r\n");
        return;
    }
    // A large value (stored as chunks under a manifest) that reached the
    // formatter unexpanded (both anchors expand a manifest GET through
    // `kv_large` first; one that could not be): too large, never a miss.
    if result == types::KV_RESULT_MANIFEST {
        let _ = append(sb, sl, b"SERVER_ERROR object too large for cache\r\n");
        return;
    }
    // A replicated write the group did not take (no leader, a superseded
    // position): not stored, safe to send again.
    if result == types::KV_RESULT_NO_LEADER {
        let _ = append(sb, sl, b"SERVER_ERROR no leader: not stored\r\n");
        return;
    }
    // A forwarded write whose leader failed before confirming it: it may
    // have been stored. Never answered NOT_STORED, which would say it was
    // not.
    if result == types::KV_RESULT_OUTCOME_UNKNOWN {
        let _ = append(
            sb,
            sl,
            b"SERVER_ERROR outcome unknown: the leader failed mid-write\r\n",
        );
        return;
    }
    match mem_op {
        MemOp::Get | MemOp::Gets => match result {
            KV_RESULT_OK if !body.is_empty() => {
                // VALUE <key> <flags=0> <bytes> [<cas>]\r\n<data>\r\n
                let _ = append(sb, sl, b"VALUE ");
                let _ = append(sb, sl, key);
                let _ = append(sb, sl, b" 0 ");
                let _ = append_decimal_u64(sb, sl, body.len() as u64);
                if matches!(mem_op, MemOp::Gets) {
                    let _ = append(sb, sl, b" ");
                    let _ = append_decimal_u64(sb, sl, revision);
                }
                let _ = append(sb, sl, b"\r\n");
                let _ = append(sb, sl, body);
                let _ = append(sb, sl, b"\r\n");
            }
            _ => {
                // Missing key — caller emits the trailing END\r\n once
                // every key in the batch has been resolved.
            }
        },
        MemOp::Set => match result {
            KV_RESULT_OK => {
                let _ = append(sb, sl, b"STORED\r\n");
            }
            _ => {
                let _ = append(sb, sl, b"NOT_STORED\r\n");
            }
        },
        MemOp::Add | MemOp::Replace => match result {
            KV_RESULT_OK => {
                let _ = append(sb, sl, b"STORED\r\n");
            }
            KV_RESULT_CAS_FAILED => {
                let _ = append(sb, sl, b"NOT_STORED\r\n");
            }
            _ => {
                let _ = append(sb, sl, b"NOT_STORED\r\n");
            }
        },
        MemOp::Append | MemOp::Prepend => match result {
            KV_RESULT_INTEGER => {
                let _ = append(sb, sl, b"STORED\r\n");
            }
            _ => {
                let _ = append(sb, sl, b"NOT_STORED\r\n");
            }
        },
        MemOp::Cas => {
            let _ = append(sb, sl, b"SERVER_ERROR not yet supported\r\n");
        }
        MemOp::Delete => {
            if result == KV_RESULT_INTEGER && body.len() >= 8 {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&body[..8]);
                let count = i64::from_le_bytes(buf);
                if count > 0 {
                    let _ = append(sb, sl, b"DELETED\r\n");
                } else {
                    let _ = append(sb, sl, b"NOT_FOUND\r\n");
                }
            } else {
                let _ = append(sb, sl, b"NOT_FOUND\r\n");
            }
        }
        MemOp::Incr | MemOp::Decr => match result {
            KV_RESULT_INTEGER if body.len() >= 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&body[..8]);
                let n = i64::from_le_bytes(buf);
                let mut tmp = [0u8; 32];
                let w = itoa_i64(n, &mut tmp);
                let _ = append(sb, sl, &tmp[..w]);
                let _ = append(sb, sl, b"\r\n");
            }
            KV_RESULT_NOT_FOUND => {
                let _ = append(sb, sl, b"NOT_FOUND\r\n");
            }
            KV_RESULT_WRONG_TYPE => {
                let _ = append(
                    sb,
                    sl,
                    b"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
                );
            }
            _ => {
                let _ = append(sb, sl, b"NOT_FOUND\r\n");
            }
        },
        MemOp::FlushAll => match result {
            KV_RESULT_OK => {
                let _ = append(sb, sl, b"OK\r\n");
            }
            _ => {
                let _ = append(sb, sl, b"SERVER_ERROR flush failed\r\n");
            }
        },
    }
}

/// The room a hit's `VALUE` block and `END` take beyond key and value:
/// `VALUE `, ` 0 `, a length, ` ` and a CAS token, two CRLFs, `END\r\n`.
pub const GET_ANSWER_OVERHEAD: usize = 6 + 3 + 20 + 1 + 20 + 2 + 2 + 5;

/// A single-key `get`/`gets` answered whole: a hit's `VALUE` block and
/// the `END` that closes it, a miss `END` alone, a refusal its own
/// `SERVER_ERROR` line (which ends the answer). A hit that `send_buf`
/// has no room for is `SERVER_ERROR object too large for cache`, never
/// a `VALUE` cut short.
pub fn encode_get_answer(
    send_buf: &mut [u8],
    send_len: &mut usize,
    gets: bool,
    key: &[u8],
    result: u8,
    revision: u64,
    body: &[u8],
) {
    let start = *send_len;
    let room = send_buf.len().saturating_sub(start);
    if result == KV_RESULT_OK && key.len() + body.len() + GET_ANSWER_OVERHEAD > room {
        let _ = append(
            send_buf,
            send_len,
            b"SERVER_ERROR object too large for cache\r\n",
        );
        return;
    }
    let op = if gets { MemOp::Gets } else { MemOp::Get };
    encode_reply(send_buf, send_len, op, key, result, revision, body);
    let written = &send_buf[start..*send_len];
    if written.is_empty() || written.starts_with(b"VALUE ") {
        let _ = append(send_buf, send_len, b"END\r\n");
    }
}

// ── UDP framing ───────────────────────────────────────────────────────
//
// Every memcached UDP datagram, request and response, opens with an
// 8-byte frame header, big-endian:
//
//   `[request_id:u16][sequence:u16][total:u16][reserved:u16 = 0]`
//
// A request is one datagram (`sequence 0`, `total 1`). A response longer
// than one datagram is cut into `total` datagrams numbered `0..total`,
// each carrying the request's id; the client reassembles them by
// sequence number in whatever order they arrive. Nothing is retransmitted:
// a client missing a datagram times out and asks again, as with memcached.

/// The frame header every datagram opens with.
pub const UDP_FRAME_HEADER: usize = 8;
/// The largest datagram a reply is cut into, header included —
/// memcached's own `UDP_MAX_PAYLOAD_SIZE`, which clients size their
/// receive buffers to.
pub const UDP_DATAGRAM_MAX: usize = 1400;
/// Reply bytes one datagram carries.
pub const UDP_DATA_MAX: usize = UDP_DATAGRAM_MAX - UDP_FRAME_HEADER;

/// A datagram's frame header: `(request_id, sequence, total)`; `None`
/// when it is shorter than one.
pub fn udp_header(dgram: &[u8]) -> Option<(u16, u16, u16)> {
    if dgram.len() < UDP_FRAME_HEADER {
        return None;
    }
    Some((
        u16::from_be_bytes([dgram[0], dgram[1]]),
        u16::from_be_bytes([dgram[2], dgram[3]]),
        u16::from_be_bytes([dgram[4], dgram[5]]),
    ))
}

/// How many datagrams a reply of `len` bytes is cut into: at least one
/// (an empty reply is still answered); `None` past what `total` counts.
pub fn udp_datagrams(len: usize) -> Option<u16> {
    let n = if len == 0 {
        1
    } else {
        len.div_ceil(UDP_DATA_MAX)
    };
    u16::try_from(n).ok()
}

/// Datagram `seq` of `reply` into `out` (header, then its piece of the
/// reply): its length, or `None` when `seq` is past the last or `out`
/// is short.
pub fn udp_frame(request_id: u16, seq: u16, reply: &[u8], out: &mut [u8]) -> Option<usize> {
    let total = udp_datagrams(reply.len())?;
    if seq >= total {
        return None;
    }
    let from = seq as usize * UDP_DATA_MAX;
    let to = (from + UDP_DATA_MAX).min(reply.len());
    let piece = reply.get(from..to)?;
    let n = UDP_FRAME_HEADER + piece.len();
    let o = out.get_mut(..n)?;
    o[0..2].copy_from_slice(&request_id.to_be_bytes());
    o[2..4].copy_from_slice(&seq.to_be_bytes());
    o[4..6].copy_from_slice(&total.to_be_bytes());
    o[6..8].copy_from_slice(&0u16.to_be_bytes());
    o[UDP_FRAME_HEADER..].copy_from_slice(piece);
    Some(n)
}
