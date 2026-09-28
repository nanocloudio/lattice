//! JSON text ↔ the canonical document value tree (RFC 8259).
//!
//! The codec a JSON dialect (Kubernetes, a JSON document API) translates
//! with. It knows JSON and the tree, and nothing of any protocol that
//! carries them: no names, no framing.
//!
//! ## Parsing
//!
//! Strict RFC 8259: one value, whitespace around it, nothing else. Every
//! string is UTF-8 with its escapes decoded (a `\u` surrogate must be
//! paired); a raw control character is refused.
//!
//! | JSON | Tree |
//! |---|---|
//! | `null`, `true`, `false` | Null, Boolean |
//! | A number without fraction or exponent, in `i64` | Integer (64) |
//! | Any other number | Decimal, as its literal (exact, as written) |
//! | String | String |
//! | Array, object | Array, Object (member order kept) |
//!
//! `-0` stays the literal `-0`. A number past the tree's literal bound
//! (`doc_tree::MAX_DECIMAL_TEXT` bytes, exponent `MAX_DECIMAL_EXPONENT`),
//! an object stating a key twice, and nesting past `doc_tree::MAX_DEPTH`
//! are refused rather than approximated.
//!
//! ## Rendering
//!
//! Compact (no whitespace). A tree written by another dialect renders as
//! the JSON that means it:
//!
//! | Tree | JSON |
//! |---|---|
//! | Integer | Its decimal |
//! | Decimal literal | The literal |
//! | Decimal binary64 | The shortest decimal that reads back as it (JavaScript's spelling); NaN and the infinities are refused |
//! | Binary | Standard base64 with padding (RFC 4648 §4) |
//! | Timestamp | RFC 3339 UTC, fractional seconds without trailing zeros |
//! | Identifier | Lowercase hex |
//!
//! Strings escape `"`, `\` and the control characters; everything else is
//! written as UTF-8.

use super::doc_tree::{self, TreeError, Value, Writer};

/// Why JSON text or a tree was refused, and the byte where (the source
/// offset when parsing, the output offset when rendering).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JsonError {
    pub at: usize,
    pub reason: JsonReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonReason {
    /// Not RFC 8259 JSON.
    Syntax,
    /// A string escape that is not JSON's, or an unpaired surrogate.
    Escape,
    /// Text that is not UTF-8.
    Utf8,
    /// A number past the tree's literal bounds.
    Number,
    /// Containers nested past `doc_tree::MAX_DEPTH`.
    TooDeep,
    /// An object states a key twice.
    DuplicateKey,
    /// The output buffer is full.
    Overflow,
    /// A value JSON cannot state: a NaN or infinite binary64, a timestamp
    /// outside years 0–9999, or a tree that is not valid.
    Unrepresentable,
}

fn err(at: usize, reason: JsonReason) -> JsonError {
    JsonError { at, reason }
}

fn tree_err(at: usize, e: TreeError) -> JsonError {
    let reason = match e {
        TreeError::Overflow => JsonReason::Overflow,
        TreeError::TooDeep => JsonReason::TooDeep,
        TreeError::DuplicateKey => JsonReason::DuplicateKey,
        TreeError::BadUtf8 => JsonReason::Utf8,
        TreeError::BadNumber => JsonReason::Number,
        _ => JsonReason::Syntax,
    };
    err(at, reason)
}

// ── Parsing ───────────────────────────────────────────────────────────

fn is_ws(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\n' || c == b'\r'
}

fn skip_ws(src: &[u8], mut i: usize) -> usize {
    while src.get(i).is_some_and(|&c| is_ws(c)) {
        i += 1;
    }
    i
}

/// Parse the JSON text `src` into `out` as one tree value: its length.
pub fn parse(src: &[u8], out: &mut [u8]) -> Result<usize, JsonError> {
    let mut w = Writer::new(out);
    // Per open container: is it an object?
    let mut object = [false; doc_tree::MAX_DEPTH];
    let mut depth = 0usize;
    let mut i = skip_ws(src, 0);
    'value: loop {
        let at = i;
        match src.get(i) {
            Some(b'{') | Some(b'[') => {
                let is_obj = src.get(i) == Some(&b'{');
                if depth >= doc_tree::MAX_DEPTH {
                    return Err(err(at, JsonReason::TooDeep));
                }
                let r = if is_obj {
                    w.begin_object()
                } else {
                    w.begin_array()
                };
                r.map_err(|e| tree_err(at, e))?;
                object[depth] = is_obj;
                depth += 1;
                i = skip_ws(src, i + 1);
                let close = if is_obj { b'}' } else { b']' };
                if src.get(i) == Some(&close) {
                    // Empty: closed below.
                } else {
                    if is_obj {
                        i = member_key(src, i, &mut w)?;
                    }
                    continue 'value;
                }
            }
            Some(b'"') => {
                let (len, end) = measure_string(src, i)?;
                w.string_with(len, |d| decode_string(src, i, d))
                    .map_err(|e| tree_err(at, e))?;
                i = end;
            }
            Some(b't') => i = literal(src, i, b"true", &mut w, |w| w.boolean(true))?,
            Some(b'f') => i = literal(src, i, b"false", &mut w, |w| w.boolean(false))?,
            Some(b'n') => i = literal(src, i, b"null", &mut w, |w| w.null())?,
            Some(&c) if c == b'-' || c.is_ascii_digit() => i = number(src, i, &mut w)?,
            _ => return Err(err(at, JsonReason::Syntax)),
        }
        // After a value (or an empty container's opening): close what ends
        // here, or move to the next element.
        loop {
            i = skip_ws(src, i);
            if depth == 0 {
                break 'value;
            }
            let is_obj = object[depth - 1];
            let close = if is_obj { b'}' } else { b']' };
            match src.get(i) {
                Some(&c) if c == close => {
                    w.end().map_err(|e| tree_err(i, e))?;
                    depth -= 1;
                    i += 1;
                }
                Some(b',') => {
                    i = skip_ws(src, i + 1);
                    if is_obj {
                        i = member_key(src, i, &mut w)?;
                    }
                    continue 'value;
                }
                _ => return Err(err(i, JsonReason::Syntax)),
            }
        }
    }
    if i != src.len() {
        return Err(err(i, JsonReason::Syntax));
    }
    let n = w.finish().map_err(|e| tree_err(i, e))?;
    // The writer does not look for repeated keys; the tree's validation
    // does.
    let tree = out.get(..n).ok_or(err(i, JsonReason::Overflow))?;
    doc_tree::validate(tree).map_err(|e| tree_err(0, e))?;
    Ok(n)
}

/// An object member's key at `i`, and its `:`: the index of its value.
fn member_key(src: &[u8], i: usize, w: &mut Writer<'_>) -> Result<usize, JsonError> {
    if src.get(i) != Some(&b'"') {
        return Err(err(i, JsonReason::Syntax));
    }
    let (len, end) = measure_string(src, i)?;
    w.key_with(len, |d| decode_string(src, i, d))
        .map_err(|e| tree_err(i, e))?;
    let c = skip_ws(src, end);
    if src.get(c) != Some(&b':') {
        return Err(err(c, JsonReason::Syntax));
    }
    Ok(skip_ws(src, c + 1))
}

fn literal(
    src: &[u8],
    i: usize,
    word: &[u8],
    w: &mut Writer<'_>,
    put: impl FnOnce(&mut Writer<'_>) -> Result<(), TreeError>,
) -> Result<usize, JsonError> {
    let end = i + word.len();
    if src.get(i..end) != Some(word) {
        return Err(err(i, JsonReason::Syntax));
    }
    put(w).map_err(|e| tree_err(i, e))?;
    Ok(end)
}

fn hex_digit(c: u8) -> Option<u16> {
    match c {
        b'0'..=b'9' => Some((c - b'0') as u16),
        b'a'..=b'f' => Some((c - b'a' + 10) as u16),
        b'A'..=b'F' => Some((c - b'A' + 10) as u16),
        _ => None,
    }
}

/// The four hex digits at `i`.
fn hex4(src: &[u8], i: usize) -> Option<u16> {
    let mut v = 0u16;
    for k in 0..4 {
        v = (v << 4) | hex_digit(*src.get(i + k)?)?;
    }
    Some(v)
}

/// A `\u` escape at `i` (the backslash) and, for a high surrogate, the
/// low one after it: the code point and the index past the escape(s).
fn unicode_escape(src: &[u8], i: usize) -> Result<(u32, usize), JsonError> {
    let hi = hex4(src, i + 2).ok_or(err(i, JsonReason::Escape))?;
    match hi {
        0xD800..=0xDBFF => {
            if src.get(i + 6) != Some(&b'\\') || src.get(i + 7) != Some(&b'u') {
                return Err(err(i, JsonReason::Escape));
            }
            let lo = hex4(src, i + 8).ok_or(err(i, JsonReason::Escape))?;
            if !(0xDC00..=0xDFFF).contains(&lo) {
                return Err(err(i, JsonReason::Escape));
            }
            let cp = 0x10000 + (((hi as u32) - 0xD800) << 10) + ((lo as u32) - 0xDC00);
            Ok((cp, i + 12))
        }
        0xDC00..=0xDFFF => Err(err(i, JsonReason::Escape)),
        _ => Ok((hi as u32, i + 6)),
    }
}

fn utf8_len(cp: u32) -> usize {
    match cp {
        0..=0x7F => 1,
        0x80..=0x7FF => 2,
        0x800..=0xFFFF => 3,
        _ => 4,
    }
}

/// Check the string whose quote is at `i`: its decoded length, and the
/// index past its closing quote. Every later decode of it is infallible.
fn measure_string(src: &[u8], i: usize) -> Result<(usize, usize), JsonError> {
    let mut j = i + 1;
    let mut len = 0usize;
    loop {
        match src.get(j) {
            None => return Err(err(i, JsonReason::Syntax)),
            Some(b'"') => return Ok((len, j + 1)),
            Some(b'\\') => match src.get(j + 1) {
                Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => {
                    len += 1;
                    j += 2;
                }
                Some(b'u') => {
                    let (cp, end) = unicode_escape(src, j)?;
                    len += utf8_len(cp);
                    j = end;
                }
                _ => return Err(err(j, JsonReason::Escape)),
            },
            Some(&c) if c < 0x20 => return Err(err(j, JsonReason::Syntax)),
            Some(_) => {
                len += 1;
                j += 1;
            }
        }
    }
}

/// Decode the string whose quote is at `i` into `d`, which is exactly
/// its measured length ([`measure_string`] has checked it).
fn decode_string(src: &[u8], i: usize, d: &mut [u8]) {
    let mut j = i + 1;
    let mut o = 0usize;
    let mut put = |o: &mut usize, b: u8| {
        if let Some(x) = d.get_mut(*o) {
            *x = b;
        }
        *o += 1;
    };
    while let Some(&c) = src.get(j) {
        match c {
            b'"' => return,
            b'\\' => {
                let e = src.get(j + 1).copied().unwrap_or(0);
                let simple = match e {
                    b'b' => Some(0x08),
                    b'f' => Some(0x0C),
                    b'n' => Some(b'\n'),
                    b'r' => Some(b'\r'),
                    b't' => Some(b'\t'),
                    b'u' => None,
                    other => Some(other),
                };
                match simple {
                    Some(b) => {
                        put(&mut o, b);
                        j += 2;
                    }
                    None => {
                        let Ok((cp, end)) = unicode_escape(src, j) else {
                            return;
                        };
                        let mut buf = [0u8; 4];
                        let n = encode_utf8(cp, &mut buf);
                        for &b in buf.iter().take(n) {
                            put(&mut o, b);
                        }
                        j = end;
                    }
                }
            }
            _ => {
                put(&mut o, c);
                j += 1;
            }
        }
    }
}

fn encode_utf8(cp: u32, b: &mut [u8; 4]) -> usize {
    match utf8_len(cp) {
        1 => {
            b[0] = cp as u8;
            1
        }
        2 => {
            b[0] = 0xC0 | (cp >> 6) as u8;
            b[1] = 0x80 | (cp & 0x3F) as u8;
            2
        }
        3 => {
            b[0] = 0xE0 | (cp >> 12) as u8;
            b[1] = 0x80 | ((cp >> 6) & 0x3F) as u8;
            b[2] = 0x80 | (cp & 0x3F) as u8;
            3
        }
        _ => {
            b[0] = 0xF0 | (cp >> 18) as u8;
            b[1] = 0x80 | ((cp >> 12) & 0x3F) as u8;
            b[2] = 0x80 | ((cp >> 6) & 0x3F) as u8;
            b[3] = 0x80 | (cp & 0x3F) as u8;
            4
        }
    }
}

/// The number at `i`: an Integer when it has no fraction or exponent and
/// fits `i64` (and is not `-0`), else its literal.
fn number(src: &[u8], i: usize, w: &mut Writer<'_>) -> Result<usize, JsonError> {
    let mut j = i;
    let mut integral = true;
    while let Some(&c) = src.get(j) {
        match c {
            b'0'..=b'9' | b'-' | b'+' => {}
            b'.' | b'e' | b'E' => integral = false,
            _ => break,
        }
        j += 1;
    }
    let t = src.get(i..j).ok_or(err(i, JsonReason::Syntax))?;
    if !grammar_ok(t) {
        return Err(err(i, JsonReason::Syntax));
    }
    if integral && t != b"-0" {
        if let Some(v) = parse_i64(t) {
            w.int(v).map_err(|e| tree_err(i, e))?;
            return Ok(j);
        }
    }
    w.decimal_text(t).map_err(|e| tree_err(i, e))?;
    Ok(j)
}

/// The JSON number grammar alone (RFC 8259 §6), with no bound: a literal
/// the grammar accepts but the tree cannot hold is a `Number` refusal,
/// not a syntax error.
fn grammar_ok(t: &[u8]) -> bool {
    let mut i = 0;
    if t.first() == Some(&b'-') {
        i = 1;
    }
    let digits = |i: &mut usize| {
        let s = *i;
        while t.get(*i).is_some_and(|c| c.is_ascii_digit()) {
            *i += 1;
        }
        *i - s
    };
    let s = i;
    let n = digits(&mut i);
    if n == 0 || (n > 1 && t.get(s) == Some(&b'0')) {
        return false;
    }
    if t.get(i) == Some(&b'.') {
        i += 1;
        if digits(&mut i) == 0 {
            return false;
        }
    }
    if matches!(t.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(t.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        if digits(&mut i) == 0 {
            return false;
        }
    }
    i == t.len()
}

fn parse_i64(t: &[u8]) -> Option<i64> {
    let (neg, digits) = match t.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, t),
    };
    // Accumulate negatively so i64::MIN fits.
    let mut v: i64 = 0;
    for &c in digits {
        v = v.checked_mul(10)?.checked_sub((c - b'0') as i64)?;
    }
    if neg {
        Some(v)
    } else {
        v.checked_neg()
    }
}

// ── Rendering ─────────────────────────────────────────────────────────

struct Out<'a> {
    b: &'a mut [u8],
    at: usize,
}

impl Out<'_> {
    fn put(&mut self, bytes: &[u8]) -> Result<(), JsonError> {
        let end = self.at + bytes.len();
        let d = self
            .b
            .get_mut(self.at..end)
            .ok_or(err(self.at, JsonReason::Overflow))?;
        for (x, y) in d.iter_mut().zip(bytes) {
            *x = *y;
        }
        self.at = end;
        Ok(())
    }

    fn byte(&mut self, c: u8) -> Result<(), JsonError> {
        let d = self
            .b
            .get_mut(self.at)
            .ok_or(err(self.at, JsonReason::Overflow))?;
        *d = c;
        self.at += 1;
        Ok(())
    }

    fn unsigned(&mut self, mut v: u64, min_digits: usize) -> Result<(), JsonError> {
        let mut tmp = [0u8; 20];
        let mut n = 0;
        while v > 0 || n < min_digits.max(1) {
            tmp[19 - n] = b'0' + (v % 10) as u8;
            v /= 10;
            n += 1;
        }
        self.put(&tmp[20 - n..])
    }
}

/// Render the tree value `v` as compact JSON into `out`: its length.
pub fn render(v: Value<'_>, out: &mut [u8]) -> Result<usize, JsonError> {
    let mut o = Out { b: out, at: 0 };
    render_value(v, &mut o)?;
    Ok(o.at)
}

fn render_value(v: Value<'_>, o: &mut Out<'_>) -> Result<(), JsonError> {
    let bad = |o: &Out<'_>| err(o.at, JsonReason::Unrepresentable);
    match v.ty() {
        doc_tree::T_NULL => o.put(b"null"),
        doc_tree::T_BOOLEAN => {
            if v.as_bool().ok_or(bad(o))? {
                o.put(b"true")
            } else {
                o.put(b"false")
            }
        }
        doc_tree::T_INTEGER => {
            let i = v.as_i64().ok_or(bad(o))?;
            if i < 0 {
                o.byte(b'-')?;
            }
            o.unsigned(i.unsigned_abs(), 1)
        }
        doc_tree::T_DECIMAL => match v.decimal_text() {
            Some(t) => o.put(t),
            None => render_f64(v.as_f64().ok_or(bad(o))?, o),
        },
        doc_tree::T_STRING => render_string(v.as_str().ok_or(bad(o))?, o),
        doc_tree::T_BINARY => {
            let (_, data) = v.as_binary().ok_or(bad(o))?;
            render_base64(data, o)
        }
        doc_tree::T_TIMESTAMP => {
            let (secs, nanos) = v.as_timestamp().ok_or(bad(o))?;
            render_timestamp(secs, nanos, o)
        }
        doc_tree::T_IDENTIFIER => {
            let id = v.as_identifier().ok_or(bad(o))?;
            o.byte(b'"')?;
            for &b in id {
                o.byte(hex_char(b >> 4))?;
                o.byte(hex_char(b & 0xF))?;
            }
            o.byte(b'"')
        }
        doc_tree::T_ARRAY => {
            o.byte(b'[')?;
            for (k, e) in v.elements().enumerate() {
                if k > 0 {
                    o.byte(b',')?;
                }
                render_value(e, o)?;
            }
            o.byte(b']')
        }
        doc_tree::T_OBJECT => {
            o.byte(b'{')?;
            for (k, (key, m)) in v.members().enumerate() {
                if k > 0 {
                    o.byte(b',')?;
                }
                render_string(key, o)?;
                o.byte(b':')?;
                render_value(m, o)?;
            }
            o.byte(b'}')
        }
        _ => Err(bad(o)),
    }
}

fn hex_char(n: u8) -> u8 {
    if n < 10 {
        b'0' + n
    } else {
        b'a' + n - 10
    }
}

fn render_string(s: &[u8], o: &mut Out<'_>) -> Result<(), JsonError> {
    o.byte(b'"')?;
    escape_to(s, o)?;
    o.byte(b'"')
}

/// The UTF-8 text `s` escaped as the content of a JSON string (without
/// its quotes) into `out`: its length. For a template that places text
/// inside a string of its own.
pub fn escape(s: &[u8], out: &mut [u8]) -> Result<usize, JsonError> {
    let mut o = Out { b: out, at: 0 };
    escape_to(s, &mut o)?;
    Ok(o.at)
}

fn escape_to(s: &[u8], o: &mut Out<'_>) -> Result<(), JsonError> {
    for &c in s {
        match c {
            b'"' => o.put(b"\\\"")?,
            b'\\' => o.put(b"\\\\")?,
            b'\n' => o.put(b"\\n")?,
            b'\r' => o.put(b"\\r")?,
            b'\t' => o.put(b"\\t")?,
            0x08 => o.put(b"\\b")?,
            0x0C => o.put(b"\\f")?,
            c if c < 0x20 => {
                o.put(b"\\u00")?;
                o.byte(hex_char(c >> 4))?;
                o.byte(hex_char(c & 0xF))?;
            }
            c => o.byte(c)?,
        }
    }
    Ok(())
}

/// A binary64 as the shortest decimal that reads back as it, spelled as
/// JavaScript spells a number: plain between 1e-6 and 1e21, else with an
/// exponent.
fn render_f64(f: f64, o: &mut Out<'_>) -> Result<(), JsonError> {
    let mut d = [0u8; doc_tree::SHORTEST_DIGITS];
    let (neg, k, n) =
        doc_tree::shortest_f64(f, &mut d).ok_or(err(o.at, JsonReason::Unrepresentable))?;
    if neg {
        o.byte(b'-')?;
    }
    if k == 0 {
        return o.byte(b'0');
    }
    let digit = |i: usize| b'0' + d[i];
    if (k as i32) <= n && n <= 21 {
        for i in 0..k {
            o.byte(digit(i))?;
        }
        for _ in 0..(n - k as i32) {
            o.byte(b'0')?;
        }
    } else if 0 < n && n <= 21 {
        for i in 0..k {
            if i as i32 == n {
                o.byte(b'.')?;
            }
            o.byte(digit(i))?;
        }
    } else if -6 < n && n <= 0 {
        o.put(b"0.")?;
        for _ in 0..(-n) {
            o.byte(b'0')?;
        }
        for i in 0..k {
            o.byte(digit(i))?;
        }
    } else {
        o.byte(digit(0))?;
        if k > 1 {
            o.byte(b'.')?;
            for i in 1..k {
                o.byte(digit(i))?;
            }
        }
        o.byte(b'e')?;
        let e = n - 1;
        o.byte(if e < 0 { b'-' } else { b'+' })?;
        o.unsigned(e.unsigned_abs() as u64, 1)?;
    }
    Ok(())
}

fn b64_char(n: u8) -> u8 {
    match n {
        0..=25 => b'A' + n,
        26..=51 => b'a' + n - 26,
        52..=61 => b'0' + n - 52,
        62 => b'+',
        _ => b'/',
    }
}

fn render_base64(data: &[u8], o: &mut Out<'_>) -> Result<(), JsonError> {
    o.byte(b'"')?;
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        o.byte(b64_char(b0 >> 2))?;
        o.byte(b64_char(((b0 & 0x03) << 4) | (b1 >> 4)))?;
        if chunk.len() > 1 {
            o.byte(b64_char(((b1 & 0x0F) << 2) | (b2 >> 6)))?;
        } else {
            o.byte(b'=')?;
        }
        if chunk.len() > 2 {
            o.byte(b64_char(b2 & 0x3F))?;
        } else {
            o.byte(b'=')?;
        }
    }
    o.byte(b'"')
}

/// The civil date of a day count from 1970-01-01 (proleptic Gregorian):
/// `(year, month, day)`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn render_timestamp(secs: i64, nanos: u32, o: &mut Out<'_>) -> Result<(), JsonError> {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400) as u64;
    let (y, m, d) = civil_from_days(days);
    if !(0..=9999).contains(&y) {
        return Err(err(o.at, JsonReason::Unrepresentable));
    }
    o.byte(b'"')?;
    o.unsigned(y as u64, 4)?;
    o.byte(b'-')?;
    o.unsigned(m as u64, 2)?;
    o.byte(b'-')?;
    o.unsigned(d as u64, 2)?;
    o.byte(b'T')?;
    o.unsigned(tod / 3600, 2)?;
    o.byte(b':')?;
    o.unsigned(tod / 60 % 60, 2)?;
    o.byte(b':')?;
    o.unsigned(tod % 60, 2)?;
    if nanos != 0 {
        let mut digits = 9;
        let mut f = nanos;
        while f.is_multiple_of(10) {
            f /= 10;
            digits -= 1;
        }
        o.byte(b'.')?;
        o.unsigned(f as u64, digits)?;
    }
    o.put(b"Z\"")
}
