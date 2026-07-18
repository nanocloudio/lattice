// Bounded, no_std, no-alloc RESP reply framing. Like the other `*_core.rs`
// files it carries NO inner attributes and NO test module, so it is `include!`d
// verbatim by both this crate (`lib.rs`) and the on-device Fluxor module
// (`modules/foundation/tcp_client/mod.rs`) — one source of truth, host and
// device.
//
// Redis keeps a connection open across replies, so a client must know when one
// reply is complete rather than waiting for TCP close. `resp_reply_len` reports
// the byte length of the first complete top-level reply in a buffer (or `None`
// if more bytes are needed), covering the reply shapes SET/GET produce: simple
// string (`+OK`), error (`-ERR …`), integer (`:N`), and bulk string
// (`$len\r\n…\r\n`, incl. the `$-1` nil).

/// Length of the first complete RESP reply in `buf`, or `None` if more bytes are
/// needed.
pub fn resp_reply_len(buf: &[u8]) -> Option<usize> {
    if buf.is_empty() {
        return None;
    }
    match buf[0] {
        b'+' | b'-' | b':' => resp_crlf(buf, 1).map(|end| end + 2),
        b'$' => {
            let header_end = resp_crlf(buf, 1)?; // index of the '\r'
            let n = resp_parse_int(&buf[1..header_end])?;
            if n < 0 {
                return Some(header_end + 2); // $-1\r\n (nil)
            }
            let total = header_end + 2 + n as usize + 2; // header + data + trailing CRLF
            if buf.len() >= total {
                Some(total)
            } else {
                None
            }
        }
        // Arrays / inline replies aren't produced by SET/GET; best-effort to the
        // first line so a reader never hangs on an unexpected reply.
        _ => resp_crlf(buf, 0).map(|end| end + 2),
    }
}

/// Index of the '\r' of the next CRLF at or after `from`, or `None`.
pub fn resp_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parse a signed decimal integer from ASCII bytes (a RESP bulk length line).
pub fn resp_parse_int(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, digits) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else {
        (false, bytes)
    };
    if digits.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.wrapping_mul(10).wrapping_add((c - b'0') as i64);
    }
    Some(if neg { -v } else { v })
}

/// Write `v` as decimal ASCII into `out`; returns the length, or `None` if `out`
/// is too small. No allocation, no panic path (freestanding-module safe).
pub fn itoa(v: i64, out: &mut [u8]) -> Option<usize> {
    let neg = v < 0;
    let mut u: u64 = if neg {
        (v as i128).unsigned_abs() as u64
    } else {
        v as u64
    };
    let mut tmp = [0u8; 20];
    let mut n = 0usize;
    if u == 0 {
        tmp[0] = b'0';
        n = 1;
    } else {
        while u > 0 {
            tmp[n] = b'0' + (u % 10) as u8;
            u /= 10;
            n += 1;
        }
    }
    let total = n + usize::from(neg);
    if total > out.len() {
        return None;
    }
    let mut i = 0;
    if neg {
        out[0] = b'-';
        i = 1;
    }
    let mut k = 0;
    while k < n {
        out[i + k] = tmp[n - 1 - k];
        k += 1;
    }
    Some(total)
}

/// Build a RESP `SET key value` command into `out`:
/// `*3\r\n$3\r\nSET\r\n$<klen>\r\n<key>\r\n$<vlen>\r\n<value>\r\n`. Returns the
/// length, or `None` if `out` is too small. The on-device counterpart of the
/// host redis connector's command encoding.
pub fn resp_set(key: &[u8], value: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    resp_put(out, &mut pos, b"*3\r\n$3\r\nSET\r\n")?;
    resp_bulk(out, &mut pos, key)?;
    resp_bulk(out, &mut pos, value)?;
    Some(pos)
}

fn resp_bulk(out: &mut [u8], pos: &mut usize, data: &[u8]) -> Option<()> {
    resp_put(out, pos, b"$")?;
    let mut num = [0u8; 20];
    let ln = itoa(data.len() as i64, &mut num)?;
    resp_put(out, pos, &num[..ln])?;
    resp_put(out, pos, b"\r\n")?;
    resp_put(out, pos, data)?;
    resp_put(out, pos, b"\r\n")
}

fn resp_put(out: &mut [u8], pos: &mut usize, bytes: &[u8]) -> Option<()> {
    if *pos + bytes.len() > out.len() {
        return None;
    }
    out[*pos..*pos + bytes.len()].copy_from_slice(bytes);
    *pos += bytes.len();
    Some(())
}
