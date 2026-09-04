//! Minimal HTTP/1.1 request framer + response builder — the transport a
//! Prometheus-compatible anchor terminates on. Pure logic, `no_std`, no
//! allocation: it frames a request out of a growing byte buffer exactly as
//! the RESP/memcached framers do, so it drops into an edge anchor's
//! `drain_slot` loop in place of `parse_one`.
//!
//! Scope is deliberately the subset a metrics datasource speaks:
//! `Content-Length`-delimited bodies, no chunked transfer, no keep-alive
//! pipelining assumptions. Chunked transfer encoding is reported as an
//! error rather than mis-framed; a request without `Content-Length` is
//! framed as having no body.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor module and host tests; each consumer uses a subset of the surface"
)]

/// Result of trying to frame one request from the front of a buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    /// Not enough bytes yet — wait for more and retry.
    Incomplete,
    /// A malformed request line/headers, or an unsupported framing.
    Error,
    /// A complete request occupying `consumed` bytes of the buffer.
    Ready { consumed: usize, req: Request<'a> },
}

/// A framed HTTP request. All slices borrow the input buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct Request<'a> {
    pub method: &'a [u8],
    /// Path without the query string (e.g. `/api/v1/query_range`).
    pub path: &'a [u8],
    /// Raw query string after `?` (may be empty), still percent-encoded.
    pub query: &'a [u8],
    /// Request body (exactly `Content-Length` bytes), possibly empty.
    pub body: &'a [u8],
    /// Value of `Content-Encoding`, lowercased-compared by the caller.
    pub content_encoding: &'a [u8],
}

/// Try to frame one request from `buf`. On `Ready`, the caller drops the
/// first `consumed` bytes and may frame the next.
pub fn frame(buf: &[u8]) -> Frame<'_> {
    // Find end of headers: CRLF CRLF.
    let head_end = match find(buf, b"\r\n\r\n") {
        Some(i) => i,
        None => return Frame::Incomplete,
    };
    let body_start = head_end + 4;

    // Request line is the first CRLF-terminated line of the whole buffer.
    // (With zero headers the request line's terminating CRLF IS the first
    // half of the CRLF CRLF, so `line_end == head_end`.)
    let line_end = match find(buf, b"\r\n") {
        Some(i) if i <= head_end => i,
        _ => return Frame::Error,
    };
    let line = &buf[..line_end];
    let (method, path, query) = match parse_request_line(line) {
        Some(v) => v,
        None => return Frame::Error,
    };

    // Header lines lie between the request line and the blank line. Empty
    // when there are no headers (`line_end == head_end`).
    let mut content_length: usize = 0;
    let mut has_len = false;
    let mut content_encoding: &[u8] = &[];
    let mut chunked = false;
    let mut rest: &[u8] = if head_end >= line_end + 2 {
        &buf[line_end + 2..head_end]
    } else {
        &[]
    };
    while !rest.is_empty() {
        let l_end = match find(rest, b"\r\n") {
            Some(i) => i,
            None => rest.len(), // last header, no trailing CRLF inside `head`
        };
        let line = &rest[..l_end];
        if let Some((name, value)) = split_header(line) {
            if header_eq(name, b"content-length") {
                match parse_usize(trim(value)) {
                    Some(n) => {
                        content_length = n;
                        has_len = true;
                    }
                    None => return Frame::Error,
                }
            } else if header_eq(name, b"content-encoding") {
                content_encoding = trim(value);
            } else if header_eq(name, b"transfer-encoding")
                && contains_token(trim(value), b"chunked")
            {
                chunked = true;
            }
        }
        if l_end >= rest.len() {
            break;
        }
        rest = &rest[l_end + 2..];
    }

    // Chunked transfer is out of scope — refuse rather than mis-frame.
    if chunked {
        return Frame::Error;
    }

    let body_len = if has_len { content_length } else { 0 };
    let total = body_start + body_len;
    if buf.len() < total {
        return Frame::Incomplete;
    }
    let body = &buf[body_start..total];
    Frame::Ready {
        consumed: total,
        req: Request {
            method,
            path,
            query,
            body,
            content_encoding,
        },
    }
}

/// `METHOD SP request-target SP HTTP/1.x` → (method, path, query). The
/// request-target is split at the first `?`.
fn parse_request_line(line: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    let sp1 = find(line, b" ")?;
    let method = &line[..sp1];
    let rest = &line[sp1 + 1..];
    let sp2 = find(rest, b" ")?;
    let target = &rest[..sp2];
    let version = &rest[sp2 + 1..];
    if !starts_with(version, b"HTTP/1.") {
        return None;
    }
    let (path, query) = match find(target, b"?") {
        Some(q) => (&target[..q], &target[q + 1..]),
        None => (target, &target[target.len()..]),
    };
    Some((method, path, query))
}

/// Find the value of query parameter `name` in a raw query string, returning
/// the still-percent-encoded value slice. `a=1&b=2` → `find_param(_,b"b")` =
/// `Some(b"2")`.
pub fn find_param<'a>(query: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let mut rest = query;
    while !rest.is_empty() {
        let pair_end = find(rest, b"&").unwrap_or(rest.len());
        let pair = &rest[..pair_end];
        if let Some(eq) = find(pair, b"=") {
            if &pair[..eq] == name {
                return Some(&pair[eq + 1..]);
            }
        }
        if pair_end >= rest.len() {
            break;
        }
        rest = &rest[pair_end + 1..];
    }
    None
}

/// Percent-decode `src` (with `+` → space, per form encoding) into `out`,
/// returning the decoded length. Returns `None` on a truncated/invalid
/// escape or if `out` is too small.
pub fn percent_decode(src: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut o = 0;
    let mut i = 0;
    while i < src.len() {
        let c = src[i];
        let d = match c {
            b'+' => b' ',
            b'%' => {
                let hi = hex(*src.get(i + 1)?)?;
                let lo = hex(*src.get(i + 2)?)?;
                i += 2;
                (hi << 4) | lo
            }
            other => other,
        };
        *out.get_mut(o)? = d;
        o += 1;
        i += 1;
    }
    Some(o)
}

/// Build a response into `out`: status line, `Content-Type`, `Content-Length`,
/// `Connection: close`, then `body`. Returns total length, or `None` if `out`
/// is too small.
pub fn write_response(
    out: &mut [u8],
    status: u16,
    reason: &[u8],
    content_type: &[u8],
    body: &[u8],
) -> Option<usize> {
    let mut w = Writer { buf: out, n: 0 };
    w.put(b"HTTP/1.1 ")?;
    w.put_u16(status)?;
    w.put(b" ")?;
    w.put(reason)?;
    w.put(b"\r\nContent-Type: ")?;
    w.put(content_type)?;
    w.put(b"\r\nContent-Length: ")?;
    w.put_usize(body.len())?;
    w.put(b"\r\nConnection: close\r\n\r\n")?;
    w.put(body)?;
    Some(w.n)
}

// ── small helpers ─────────────────────────────────────────────────────

struct Writer<'a> {
    buf: &'a mut [u8],
    n: usize,
}

impl Writer<'_> {
    fn put(&mut self, bytes: &[u8]) -> Option<()> {
        if self.n + bytes.len() > self.buf.len() {
            return None;
        }
        self.buf[self.n..self.n + bytes.len()].copy_from_slice(bytes);
        self.n += bytes.len();
        Some(())
    }
    fn put_u16(&mut self, v: u16) -> Option<()> {
        self.put_usize(v as usize)
    }
    fn put_usize(&mut self, v: usize) -> Option<()> {
        let mut tmp = [0u8; 20];
        let mut i = tmp.len();
        let mut n = v;
        loop {
            i -= 1;
            tmp[i] = b'0' + (n % 10) as u8;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        self.put(&tmp[i..])
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    let last = haystack.len() - needle.len();
    let mut i = 0;
    while i <= last {
        if &haystack[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn starts_with(hay: &[u8], pre: &[u8]) -> bool {
    hay.len() >= pre.len() && &hay[..pre.len()] == pre
}

fn split_header(line: &[u8]) -> Option<(&[u8], &[u8])> {
    let c = find(line, b":")?;
    Some((&line[..c], &line[c + 1..]))
}

fn header_eq(a: &[u8], lower: &[u8]) -> bool {
    if a.len() != lower.len() {
        return false;
    }
    for i in 0..a.len() {
        if a[i].to_ascii_lowercase() != lower[i] {
            return false;
        }
    }
    true
}

fn contains_token(value: &[u8], token: &[u8]) -> bool {
    find(value, token).is_some()
}

fn trim(mut s: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = s {
        if *first == b' ' || *first == b'\t' {
            s = rest;
        } else {
            break;
        }
    }
    while let [rest @ .., last] = s {
        if *last == b' ' || *last == b'\t' {
            s = rest;
        } else {
            break;
        }
    }
    s
}

fn parse_usize(s: &[u8]) -> Option<usize> {
    if s.is_empty() {
        return None;
    }
    let mut n: usize = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((c - b'0') as usize)?;
    }
    Some(n)
}

fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
