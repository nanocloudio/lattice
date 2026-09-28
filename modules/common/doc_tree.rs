//! The canonical document value tree.
//!
//! Connector-specific JSON or binary encodings translate to a
//! canonical versioned document value tree containing explicit null,
//! boolean, integer, decimal, string, binary, timestamp, identifier,
//! array, and object types.
//!
//! This is that tree's byte format, its validation, its total order and
//! its paths. Contracts only — no ports, no I/O, no allocation. Every
//! document the document executor stores, filters, patches or returns is
//! one of these; a dialect (Mongo's BSON, Kubernetes' JSON) translates to
//! it on the way in and renders from it on the way out, and nothing
//! between those two edges sees a wire format.
//!
//! ## Format v1
//!
//! A value is `[type:u8][payload]`, the types being
//! `models::DocValueType`'s discriminants (a contract test pins the
//! byte identity; this file mounts nothing). All integers little-endian.
//!
//! | Type | Payload |
//! |---|---|
//! | Null 1 | — |
//! | Boolean 2 | `[0 or 1]` |
//! | Integer 3 | `[width:u8 = 32 or 64][i64]` |
//! | Decimal 4 | `[form:u8]`, form 1 `[f64]`, form 2 `[len:u16][JSON number literal]` |
//! | String 5 | `[len:u32][UTF-8]` |
//! | Binary 6 | `[subtype:u8][len:u32][bytes]` |
//! | Timestamp 7 | `[secs:i64][nanos:u32]` |
//! | Identifier 8 | `[len:u8][bytes]`, at most 48 |
//! | Array 9 | `[count:u32][bytes:u32][value]…` |
//! | Object 10 | `[count:u32][bytes:u32]{[klen:u16][key][value]}…` |
//!
//! A container's `bytes` is the length of its body, so any value can be
//! skipped without being walked. Object members keep their order (both
//! BSON and JSON carry one) and a key may appear once. An integer's
//! width and a decimal's form are what the value was written as — a
//! Mongo `int32` stays an `int32`, a JSON `1.50` stays `1.50` — and
//! neither takes part in comparison.
//!
//! A stored document record is `[FORMAT_V1][object]`
//! ([`encode_record`] / [`parse_record`]).
//!
//! ## Order
//!
//! [`compare`] is a total order: by type rank, Null < numbers < String <
//! Object < Array < Binary < Identifier < Boolean < Timestamp (BSON's
//! order, so the Mongo dialect sorts as MongoDB does), then by value.
//! Integers and decimals compare numerically with each other, exactly (a
//! binary64 by its exact decimal expansion). NaN is below every
//! other number and equal to itself. Objects compare member by member —
//! the value's type rank, then the key, then the value — and arrays
//! element by element; a prefix sorts first.
//!
//! ## Paths
//!
//! A path is `[n:u8]{[len:u16][segment]}…`, at most 32 segments. A
//! segment names an object member, or, when the container is an array,
//! the element at a decimal index (`-` names the position past the end,
//! for insertion). Mongo's dotted paths and RFC 6901 pointers both
//! translate to this ([`path_from_pointer`] is the RFC's; dotted
//! spellings are the Mongo dialect's).
//!
//! ## Fail closed
//!
//! [`validate`] checks everything: types, lengths, UTF-8, number
//! grammar, integer widths, nanoseconds, identifier length, duplicate
//! keys, depth, and that nothing trails the value. Readers on a
//! validated value return `None` rather than panic when handed bytes
//! that were not.

use core::cmp::Ordering;

/// Document record format.
pub const FORMAT_V1: u8 = 1;
/// A record that carries its document's field ownership after the tree:
/// `[FORMAT_OWNED][tree_len:u32][object][ownership]`.
pub const FORMAT_OWNED: u8 = 2;
/// Deepest nesting of containers.
pub const MAX_DEPTH: usize = 32;
/// Most segments in a path.
pub const MAX_PATH_SEGMENTS: usize = 32;
/// Longest identifier (`models::MAX_MODEL_ID_LEN`).
pub const MAX_IDENTIFIER_LEN: usize = 48;
/// Longest decimal literal.
pub const MAX_DECIMAL_TEXT: usize = 64;
/// Largest decimal exponent magnitude a literal may state.
pub const MAX_DECIMAL_EXPONENT: usize = 9999;

pub const T_NULL: u8 = 1;
pub const T_BOOLEAN: u8 = 2;
pub const T_INTEGER: u8 = 3;
pub const T_DECIMAL: u8 = 4;
pub const T_STRING: u8 = 5;
pub const T_BINARY: u8 = 6;
pub const T_TIMESTAMP: u8 = 7;
pub const T_IDENTIFIER: u8 = 8;
pub const T_ARRAY: u8 = 9;
pub const T_OBJECT: u8 = 10;

pub const WIDTH_32: u8 = 32;
pub const WIDTH_64: u8 = 64;
pub const DECIMAL_BINARY64: u8 = 1;
pub const DECIMAL_TEXT: u8 = 2;

/// Container header after the type byte: `[count:u32][bytes:u32]`.
pub const CONTAINER_HEAD: usize = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeError {
    /// The bytes end inside a value.
    Truncated,
    /// A type, width, form or boolean byte that is not in the format.
    UnknownType,
    /// A length or count that disagrees with the bytes it frames.
    BadLength,
    /// Containers nested past [`MAX_DEPTH`].
    TooDeep,
    /// An object states a key twice.
    DuplicateKey,
    /// A string or key that is not UTF-8.
    BadUtf8,
    /// A decimal literal outside the JSON number grammar or its bounds,
    /// an `int32` out of range, or nanoseconds past a second.
    BadNumber,
    /// Bytes after the value.
    Trailing,
    /// The output buffer is full.
    Overflow,
    /// A writer call out of order (a value where a key is due, an
    /// unclosed container, a second root).
    Unbalanced,
    /// A record whose format byte is not [`FORMAT_V1`], or whose root is
    /// not an object.
    BadFormat,
    /// A path that is malformed or too long.
    BadPath,
}

// ── Little-endian readers ─────────────────────────────────────────────

fn rd_u16(b: &[u8], at: usize) -> Option<u16> {
    let s = b.get(at..at.checked_add(2)?)?;
    Some(u16::from_le_bytes([s[0], s[1]]))
}

fn rd_u32(b: &[u8], at: usize) -> Option<u32> {
    let s = b.get(at..at.checked_add(4)?)?;
    Some(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn rd_u64(b: &[u8], at: usize) -> Option<u64> {
    let s = b.get(at..at.checked_add(8)?)?;
    let mut a = [0u8; 8];
    for (d, x) in a.iter_mut().zip(s) {
        *d = *x;
    }
    Some(u64::from_le_bytes(a))
}

/// Length of the value at the start of `b` — its type byte, payload and,
/// for a container, its whole body — read from its header alone.
pub fn value_span(b: &[u8]) -> Option<usize> {
    let n = match *b.first()? {
        T_NULL => 1,
        T_BOOLEAN => 2,
        T_INTEGER => 10,
        T_DECIMAL => match *b.get(1)? {
            DECIMAL_BINARY64 => 10,
            DECIMAL_TEXT => 4 + rd_u16(b, 2)? as usize,
            _ => return None,
        },
        T_STRING => 5 + rd_u32(b, 1)? as usize,
        T_BINARY => 6 + rd_u32(b, 2)? as usize,
        T_TIMESTAMP => 13,
        T_IDENTIFIER => 2 + *b.get(1)? as usize,
        T_ARRAY | T_OBJECT => 1 + CONTAINER_HEAD + rd_u32(b, 5)? as usize,
        _ => return None,
    };
    if n > b.len() {
        return None;
    }
    Some(n)
}

// ── Validation ────────────────────────────────────────────────────────

/// Validate the one value `b` holds, exactly: nothing may trail it.
pub fn validate(b: &[u8]) -> Result<(), TreeError> {
    let n = validate_at(b, 0)?;
    if n != b.len() {
        return Err(TreeError::Trailing);
    }
    Ok(())
}

/// Validate the value at the start of `b` (bytes may follow it) and
/// answer its length.
pub fn validate_prefix(b: &[u8]) -> Result<usize, TreeError> {
    validate_at(b, 0)
}

fn validate_at(b: &[u8], depth: usize) -> Result<usize, TreeError> {
    let ty = *b.first().ok_or(TreeError::Truncated)?;
    let known = match ty {
        T_NULL..=T_OBJECT => {
            ty != T_DECIMAL
                || matches!(
                    b.get(1),
                    None | Some(&DECIMAL_BINARY64) | Some(&DECIMAL_TEXT)
                )
        }
        _ => false,
    };
    if !known {
        return Err(TreeError::UnknownType);
    }
    let n = value_span(b).ok_or(TreeError::Truncated)?;
    match ty {
        T_BOOLEAN => {
            if b[1] > 1 {
                return Err(TreeError::UnknownType);
            }
        }
        T_INTEGER => {
            let v = rd_u64(b, 2).ok_or(TreeError::Truncated)? as i64;
            match b[1] {
                WIDTH_64 => {}
                WIDTH_32 => {
                    if v < i32::MIN as i64 || v > i32::MAX as i64 {
                        return Err(TreeError::BadNumber);
                    }
                }
                _ => return Err(TreeError::UnknownType),
            }
        }
        T_DECIMAL => {
            if b[1] == DECIMAL_TEXT && !decimal_text_is_valid(b.get(4..n).unwrap_or(&[])) {
                return Err(TreeError::BadNumber);
            }
        }
        T_STRING => {
            if !utf8_is_valid(b.get(5..n).unwrap_or(&[])) {
                return Err(TreeError::BadUtf8);
            }
        }
        T_TIMESTAMP => {
            if rd_u32(b, 9).ok_or(TreeError::Truncated)? >= 1_000_000_000 {
                return Err(TreeError::BadNumber);
            }
        }
        T_IDENTIFIER => {
            if b[1] as usize > MAX_IDENTIFIER_LEN {
                return Err(TreeError::BadLength);
            }
        }
        T_ARRAY | T_OBJECT => {
            if depth >= MAX_DEPTH {
                return Err(TreeError::TooDeep);
            }
            let count = rd_u32(b, 1).ok_or(TreeError::Truncated)?;
            let body = b.get(1 + CONTAINER_HEAD..n).ok_or(TreeError::Truncated)?;
            validate_body(ty, count, body, depth + 1)?;
        }
        _ => {}
    }
    Ok(n)
}

fn validate_body(ty: u8, count: u32, body: &[u8], depth: usize) -> Result<(), TreeError> {
    let mut at = 0usize;
    let mut seen = 0u32;
    // Keys in strictly ascending order so far: a key above the last one
    // cannot repeat any before it, so the scan for a duplicate is skipped
    // (a large map from a client that sorts its keys — Kubernetes' do —
    // validates in one pass). The first key out of order, and every one
    // after it, is checked against every key before it, as ever.
    let mut last: Option<&[u8]> = None;
    let mut ascending = true;
    while at < body.len() {
        if ty == T_OBJECT {
            let klen = rd_u16(body, at).ok_or(TreeError::Truncated)? as usize;
            let key = body
                .get(at + 2..at + 2 + klen)
                .ok_or(TreeError::Truncated)?;
            if !utf8_is_valid(key) {
                return Err(TreeError::BadUtf8);
            }
            ascending = ascending && last.is_none_or(|l| l < key);
            if !ascending && object_has_key(body.get(..at).unwrap_or(&[]), key) {
                return Err(TreeError::DuplicateKey);
            }
            last = Some(key);
            at += 2 + klen;
        }
        let rest = body.get(at..).ok_or(TreeError::Truncated)?;
        at += validate_at(rest, depth)?;
        seen = seen.checked_add(1).ok_or(TreeError::BadLength)?;
    }
    if seen != count {
        return Err(TreeError::BadLength);
    }
    Ok(())
}

/// Does an object body (a run of members) hold `key`?
fn object_has_key(body: &[u8], key: &[u8]) -> bool {
    Members { body, at: 0 }.any(|(k, _)| k == key)
}

/// The JSON number grammar (RFC 8259 §6), within [`MAX_DECIMAL_TEXT`]
/// bytes and an exponent of at most [`MAX_DECIMAL_EXPONENT`].
pub fn decimal_text_is_valid(t: &[u8]) -> bool {
    if t.is_empty() || t.len() > MAX_DECIMAL_TEXT {
        return false;
    }
    let mut i = 0;
    if t[0] == b'-' {
        i = 1;
    }
    let int_start = i;
    while i < t.len() && t[i].is_ascii_digit() {
        i += 1;
    }
    let int_len = i - int_start;
    if int_len == 0 || (int_len > 1 && t[int_start] == b'0') {
        return false;
    }
    if i < t.len() && t[i] == b'.' {
        i += 1;
        let s = i;
        while i < t.len() && t[i].is_ascii_digit() {
            i += 1;
        }
        if i == s {
            return false;
        }
    }
    if i < t.len() && (t[i] == b'e' || t[i] == b'E') {
        i += 1;
        if i < t.len() && (t[i] == b'+' || t[i] == b'-') {
            i += 1;
        }
        let s = i;
        let mut e: i32 = 0;
        while i < t.len() && t[i].is_ascii_digit() {
            e = e.saturating_mul(10).saturating_add((t[i] - b'0') as i32);
            i += 1;
        }
        if i == s || e > MAX_DECIMAL_EXPONENT as i32 {
            return false;
        }
    }
    i == t.len()
}

/// Does `b` hold a NUL byte? A loop rather than slice `contains`, which
/// lowers to `memchr` — a symbol a module image cannot link.
#[allow(
    clippy::manual_contains,
    reason = "slice::contains links core's memchr, which PIC modules cannot resolve"
)]
pub fn has_nul(b: &[u8]) -> bool {
    b.iter().any(|c| *c == 0)
}

/// Is `b` well-formed UTF-8 (RFC 3629: no overlong forms, no surrogates,
/// nothing past U+10FFFF)? Written out rather than `core::str::from_utf8`,
/// which a module image cannot link.
pub fn utf8_is_valid(b: &[u8]) -> bool {
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let (n, lo, hi) = match c {
            0x00..=0x7F => {
                i += 1;
                continue;
            }
            0xC2..=0xDF => (1, 0x80, 0xBF),
            0xE0 => (2, 0xA0, 0xBF),
            0xE1..=0xEC | 0xEE..=0xEF => (2, 0x80, 0xBF),
            0xED => (2, 0x80, 0x9F),
            0xF0 => (3, 0x90, 0xBF),
            0xF1..=0xF3 => (3, 0x80, 0xBF),
            0xF4 => (3, 0x80, 0x8F),
            _ => return false,
        };
        // The second byte carries the lead's range; the rest are plain
        // continuations.
        match b.get(i + 1) {
            Some(&x) if (lo..=hi).contains(&x) => {}
            _ => return false,
        }
        let mut k = 2;
        while k <= n {
            match b.get(i + k) {
                Some(&x) if (0x80..=0xBF).contains(&x) => {}
                _ => return false,
            }
            k += 1;
        }
        i += n + 1;
    }
    true
}

// ── Records ───────────────────────────────────────────────────────────

/// `[FORMAT_V1][object]` into `out`.
pub fn encode_record(object: Value<'_>, out: &mut [u8]) -> Result<usize, TreeError> {
    if object.ty() != T_OBJECT {
        return Err(TreeError::BadFormat);
    }
    let v = object.bytes();
    let d = out.get_mut(..1 + v.len()).ok_or(TreeError::Overflow)?;
    d[0] = FORMAT_V1;
    for (x, y) in d[1..].iter_mut().zip(v) {
        *x = *y;
    }
    Ok(1 + v.len())
}

/// Validate a stored record and answer its root object.
pub fn parse_record(rec: &[u8]) -> Result<Value<'_>, TreeError> {
    record_parts(rec).map(|(v, _)| v)
}

/// A stored record's root object and, for a [`FORMAT_OWNED`] record, the
/// ownership bytes after it (not interpreted here).
pub fn record_parts(rec: &[u8]) -> Result<(Value<'_>, Option<&[u8]>), TreeError> {
    let (tree, owners) = match rec.first() {
        Some(&FORMAT_V1) => (rec.get(1..).unwrap_or(&[]), None),
        Some(&FORMAT_OWNED) => {
            let n = rd_u32(rec, 1).ok_or(TreeError::Truncated)? as usize;
            let tree = rec.get(5..5 + n).ok_or(TreeError::Truncated)?;
            (tree, Some(rec.get(5 + n..).unwrap_or(&[])))
        }
        Some(_) => return Err(TreeError::BadFormat),
        None => return Err(TreeError::Truncated),
    };
    let v = Value::parse(tree)?;
    if v.ty() != T_OBJECT {
        return Err(TreeError::BadFormat);
    }
    Ok((v, owners))
}

/// A [`FORMAT_OWNED`] record's head for an object of `tree_len` bytes:
/// the record is this head, the object, then the ownership bytes.
pub fn owned_record_head(tree_len: usize) -> Option<[u8; 5]> {
    let n = u32::try_from(tree_len).ok()?.to_le_bytes();
    Some([FORMAT_OWNED, n[0], n[1], n[2], n[3]])
}

// ── Reading ───────────────────────────────────────────────────────────

/// One value: exactly its bytes, type byte first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Value<'a> {
    b: &'a [u8],
}

impl<'a> Value<'a> {
    /// Validate `b` as exactly one value.
    pub fn parse(b: &'a [u8]) -> Result<Self, TreeError> {
        validate(b)?;
        Ok(Value { b })
    }

    /// Wrap bytes already validated (a member or element of a validated
    /// value, or a stored record's root). Readers still never panic on
    /// bytes that were not.
    pub const fn trusted(b: &'a [u8]) -> Self {
        Value { b }
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.b
    }

    pub fn ty(&self) -> u8 {
        self.b.first().copied().unwrap_or(0)
    }

    pub fn is_null(&self) -> bool {
        self.ty() == T_NULL
    }

    pub fn is_container(&self) -> bool {
        matches!(self.ty(), T_ARRAY | T_OBJECT)
    }

    pub fn as_bool(&self) -> Option<bool> {
        (self.ty() == T_BOOLEAN).then(|| self.b.get(1) == Some(&1))
    }

    /// An integer's value.
    pub fn as_i64(&self) -> Option<i64> {
        if self.ty() != T_INTEGER {
            return None;
        }
        Some(rd_u64(self.b, 2)? as i64)
    }

    /// An integer's written width, 32 or 64.
    pub fn int_width(&self) -> Option<u8> {
        (self.ty() == T_INTEGER).then(|| self.b.get(1).copied().unwrap_or(0))
    }

    /// A binary64 decimal's value.
    pub fn as_f64(&self) -> Option<f64> {
        if self.ty() != T_DECIMAL || self.b.get(1) != Some(&DECIMAL_BINARY64) {
            return None;
        }
        Some(f64::from_bits(rd_u64(self.b, 2)?))
    }

    /// A decimal literal's text.
    pub fn decimal_text(&self) -> Option<&'a [u8]> {
        if self.ty() != T_DECIMAL || self.b.get(1) != Some(&DECIMAL_TEXT) {
            return None;
        }
        self.b.get(4..)
    }

    pub fn as_str(&self) -> Option<&'a [u8]> {
        (self.ty() == T_STRING).then(|| self.b.get(5..).unwrap_or(&[]))
    }

    /// `(subtype, bytes)`.
    pub fn as_binary(&self) -> Option<(u8, &'a [u8])> {
        if self.ty() != T_BINARY {
            return None;
        }
        Some((*self.b.get(1)?, self.b.get(6..)?))
    }

    /// `(seconds, nanoseconds)`.
    pub fn as_timestamp(&self) -> Option<(i64, u32)> {
        if self.ty() != T_TIMESTAMP {
            return None;
        }
        Some((rd_u64(self.b, 1)? as i64, rd_u32(self.b, 9)?))
    }

    pub fn as_identifier(&self) -> Option<&'a [u8]> {
        (self.ty() == T_IDENTIFIER).then(|| self.b.get(2..).unwrap_or(&[]))
    }

    /// Members or elements of a container; 0 otherwise.
    pub fn count(&self) -> u32 {
        if !self.is_container() {
            return 0;
        }
        rd_u32(self.b, 1).unwrap_or(0)
    }

    fn body(&self) -> &'a [u8] {
        self.b.get(1 + CONTAINER_HEAD..).unwrap_or(&[])
    }

    /// An object's members in order; empty for anything else.
    pub fn members(&self) -> Members<'a> {
        let body = if self.ty() == T_OBJECT {
            self.body()
        } else {
            &[]
        };
        Members { body, at: 0 }
    }

    /// An object's members from byte offset `at` of its body (a
    /// [`Members::offset`] this object's walk reported) to its end.
    pub fn members_from(&self, at: usize) -> Members<'a> {
        let mut m = self.members();
        m.at = at.min(m.body.len());
        m
    }

    /// An array's elements in order; empty for anything else.
    pub fn elements(&self) -> Elements<'a> {
        let body = if self.ty() == T_ARRAY {
            self.body()
        } else {
            &[]
        };
        Elements { body, at: 0 }
    }

    /// An object's member `key`.
    pub fn member(&self, key: &[u8]) -> Option<Value<'a>> {
        self.members().find(|(k, _)| *k == key).map(|(_, v)| v)
    }

    /// An array's element `i`.
    pub fn element(&self, i: usize) -> Option<Value<'a>> {
        self.elements().nth(i)
    }
}

/// An object's members: `(key, value)`.
#[derive(Clone)]
pub struct Members<'a> {
    body: &'a [u8],
    at: usize,
}

impl Members<'_> {
    /// Where the next member starts in the object's body: a place
    /// [`Value::members_from`] resumes at.
    pub fn offset(&self) -> usize {
        self.at
    }
}

impl<'a> Iterator for Members<'a> {
    type Item = (&'a [u8], Value<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        let klen = rd_u16(self.body, self.at)? as usize;
        let ks = self.at + 2;
        let key = self.body.get(ks..ks + klen)?;
        let vs = ks + klen;
        let n = value_span(self.body.get(vs..)?)?;
        let v = self.body.get(vs..vs + n)?;
        self.at = vs + n;
        Some((key, Value::trusted(v)))
    }
}

/// An array's elements.
#[derive(Clone)]
pub struct Elements<'a> {
    body: &'a [u8],
    at: usize,
}

impl<'a> Iterator for Elements<'a> {
    type Item = Value<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let rest = self.body.get(self.at..)?;
        let n = value_span(rest)?;
        let v = rest.get(..n)?;
        self.at += n;
        Some(Value::trusted(v))
    }
}

// ── Writing ───────────────────────────────────────────────────────────

/// Builds one value into a caller's buffer, containers back-patched on
/// [`Writer::end`]. Inside an object every value is preceded by
/// [`Writer::key`]. Scalars are checked as the validator checks them;
/// [`Writer::value`] copies a value that is already valid; duplicate
/// keys are the caller's to avoid (a document from outside is
/// [`validate`]d, which refuses them).
pub struct Writer<'a> {
    out: &'a mut [u8],
    pos: usize,
    /// Per open container: its header position, its type, its count, and
    /// (objects) whether a key is waiting for its value.
    head: [u32; MAX_DEPTH],
    ty: [u8; MAX_DEPTH],
    count: [u32; MAX_DEPTH],
    keyed: [bool; MAX_DEPTH],
    depth: usize,
    roots: u8,
}

impl<'a> Writer<'a> {
    pub fn new(out: &'a mut [u8]) -> Self {
        Writer {
            out,
            pos: 0,
            head: [0; MAX_DEPTH],
            ty: [0; MAX_DEPTH],
            count: [0; MAX_DEPTH],
            keyed: [false; MAX_DEPTH],
            depth: 0,
            roots: 0,
        }
    }

    /// Bytes written so far.
    pub fn len(&self) -> usize {
        self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.pos == 0
    }

    fn put(&mut self, b: &[u8]) -> Result<(), TreeError> {
        let end = self.pos.checked_add(b.len()).ok_or(TreeError::Overflow)?;
        let d = self.out.get_mut(self.pos..end).ok_or(TreeError::Overflow)?;
        for (x, y) in d.iter_mut().zip(b) {
            *x = *y;
        }
        self.pos = end;
        Ok(())
    }

    /// A value is about to be written: check the position allows one.
    fn open_value(&mut self) -> Result<(), TreeError> {
        if self.depth == 0 {
            if self.roots != 0 {
                return Err(TreeError::Unbalanced);
            }
            self.roots = 1;
            return Ok(());
        }
        let d = self.depth - 1;
        if self.ty[d] == T_OBJECT {
            if !self.keyed[d] {
                return Err(TreeError::Unbalanced);
            }
            self.keyed[d] = false;
        }
        self.count[d] = self.count[d].checked_add(1).ok_or(TreeError::Overflow)?;
        Ok(())
    }

    /// An object member's key; its value follows.
    pub fn key(&mut self, k: &[u8]) -> Result<(), TreeError> {
        if self.depth == 0 {
            return Err(TreeError::Unbalanced);
        }
        let d = self.depth - 1;
        if self.ty[d] != T_OBJECT || self.keyed[d] {
            return Err(TreeError::Unbalanced);
        }
        if !utf8_is_valid(k) {
            return Err(TreeError::BadUtf8);
        }
        let n = u16::try_from(k.len()).map_err(|_| TreeError::BadLength)?;
        self.put(&n.to_le_bytes())?;
        self.put(k)?;
        self.keyed[d] = true;
        Ok(())
    }

    pub fn null(&mut self) -> Result<(), TreeError> {
        self.open_value()?;
        self.put(&[T_NULL])
    }

    pub fn boolean(&mut self, v: bool) -> Result<(), TreeError> {
        self.open_value()?;
        self.put(&[T_BOOLEAN, v as u8])
    }

    /// A 64-bit integer.
    pub fn int(&mut self, v: i64) -> Result<(), TreeError> {
        self.open_value()?;
        self.put(&[T_INTEGER, WIDTH_64])?;
        self.put(&v.to_le_bytes())
    }

    /// An integer written as 32 bits wide.
    pub fn int32(&mut self, v: i32) -> Result<(), TreeError> {
        self.open_value()?;
        self.put(&[T_INTEGER, WIDTH_32])?;
        self.put(&(v as i64).to_le_bytes())
    }

    pub fn f64(&mut self, v: f64) -> Result<(), TreeError> {
        self.open_value()?;
        self.put(&[T_DECIMAL, DECIMAL_BINARY64])?;
        self.put(&v.to_bits().to_le_bytes())
    }

    /// A decimal as its exact JSON-number literal.
    pub fn decimal_text(&mut self, t: &[u8]) -> Result<(), TreeError> {
        if !decimal_text_is_valid(t) {
            return Err(TreeError::BadNumber);
        }
        self.open_value()?;
        self.put(&[T_DECIMAL, DECIMAL_TEXT])?;
        self.put(&(t.len() as u16).to_le_bytes())?;
        self.put(t)
    }

    pub fn string(&mut self, s: &[u8]) -> Result<(), TreeError> {
        if !utf8_is_valid(s) {
            return Err(TreeError::BadUtf8);
        }
        let n = u32::try_from(s.len()).map_err(|_| TreeError::BadLength)?;
        self.open_value()?;
        self.put(&[T_STRING])?;
        self.put(&n.to_le_bytes())?;
        self.put(s)
    }

    /// A string of `len` bytes that `fill` writes in place — for a dialect
    /// that decodes its text (JSON escapes) straight into the tree. The
    /// bytes must be UTF-8.
    pub fn string_with(
        &mut self,
        len: usize,
        fill: impl FnOnce(&mut [u8]),
    ) -> Result<(), TreeError> {
        let n = u32::try_from(len).map_err(|_| TreeError::BadLength)?;
        self.open_value()?;
        self.put(&[T_STRING])?;
        self.put(&n.to_le_bytes())?;
        self.fill(len, fill)
    }

    /// An object member's key of `len` bytes that `fill` writes in place;
    /// its value follows. The bytes must be UTF-8.
    pub fn key_with(&mut self, len: usize, fill: impl FnOnce(&mut [u8])) -> Result<(), TreeError> {
        if self.depth == 0 {
            return Err(TreeError::Unbalanced);
        }
        let d = self.depth - 1;
        if self.ty[d] != T_OBJECT || self.keyed[d] {
            return Err(TreeError::Unbalanced);
        }
        let n = u16::try_from(len).map_err(|_| TreeError::BadLength)?;
        self.put(&n.to_le_bytes())?;
        self.fill(len, fill)?;
        self.keyed[d] = true;
        Ok(())
    }

    /// Reserve `len` bytes, let `fill` write them, and check they are
    /// UTF-8.
    fn fill(&mut self, len: usize, fill: impl FnOnce(&mut [u8])) -> Result<(), TreeError> {
        let end = self.pos.checked_add(len).ok_or(TreeError::Overflow)?;
        let d = self.out.get_mut(self.pos..end).ok_or(TreeError::Overflow)?;
        fill(d);
        if !utf8_is_valid(d) {
            return Err(TreeError::BadUtf8);
        }
        self.pos = end;
        Ok(())
    }

    pub fn binary(&mut self, subtype: u8, data: &[u8]) -> Result<(), TreeError> {
        let n = u32::try_from(data.len()).map_err(|_| TreeError::BadLength)?;
        self.open_value()?;
        self.put(&[T_BINARY, subtype])?;
        self.put(&n.to_le_bytes())?;
        self.put(data)
    }

    pub fn timestamp(&mut self, secs: i64, nanos: u32) -> Result<(), TreeError> {
        if nanos >= 1_000_000_000 {
            return Err(TreeError::BadNumber);
        }
        self.open_value()?;
        self.put(&[T_TIMESTAMP])?;
        self.put(&secs.to_le_bytes())?;
        self.put(&nanos.to_le_bytes())
    }

    pub fn identifier(&mut self, id: &[u8]) -> Result<(), TreeError> {
        if id.len() > MAX_IDENTIFIER_LEN {
            return Err(TreeError::BadLength);
        }
        self.open_value()?;
        self.put(&[T_IDENTIFIER, id.len() as u8])?;
        self.put(id)
    }

    /// Copy a valid value.
    pub fn value(&mut self, v: Value<'_>) -> Result<(), TreeError> {
        if v.is_container() && self.depth + value_depth(v) > MAX_DEPTH {
            return Err(TreeError::TooDeep);
        }
        self.open_value()?;
        self.put(v.bytes())
    }

    fn begin(&mut self, ty: u8) -> Result<(), TreeError> {
        if self.depth >= MAX_DEPTH {
            return Err(TreeError::TooDeep);
        }
        self.open_value()?;
        let at = self.pos;
        self.put(&[ty, 0, 0, 0, 0, 0, 0, 0, 0])?;
        self.head[self.depth] = at as u32;
        self.ty[self.depth] = ty;
        self.count[self.depth] = 0;
        self.keyed[self.depth] = false;
        self.depth += 1;
        Ok(())
    }

    pub fn begin_array(&mut self) -> Result<(), TreeError> {
        self.begin(T_ARRAY)
    }

    pub fn begin_object(&mut self) -> Result<(), TreeError> {
        self.begin(T_OBJECT)
    }

    /// Close the innermost container.
    pub fn end(&mut self) -> Result<(), TreeError> {
        if self.depth == 0 {
            return Err(TreeError::Unbalanced);
        }
        let d = self.depth - 1;
        if self.keyed[d] {
            return Err(TreeError::Unbalanced);
        }
        let at = self.head[d] as usize;
        let body = (self.pos - at - 1 - CONTAINER_HEAD) as u32;
        let h = self
            .out
            .get_mut(at + 1..at + 1 + CONTAINER_HEAD)
            .ok_or(TreeError::Overflow)?;
        for (x, y) in h.iter_mut().zip(
            self.count[d]
                .to_le_bytes()
                .iter()
                .chain(&body.to_le_bytes()),
        ) {
            *x = *y;
        }
        self.depth = d;
        Ok(())
    }

    /// The finished value's length: one root, every container closed.
    pub fn finish(&self) -> Result<usize, TreeError> {
        if self.depth != 0 || self.roots != 1 {
            return Err(TreeError::Unbalanced);
        }
        Ok(self.pos)
    }
}

/// How deeply a value's containers nest (a scalar is 0, `[]` is 1).
pub fn value_depth(v: Value<'_>) -> usize {
    match v.ty() {
        T_ARRAY => 1 + v.elements().map(value_depth).max().unwrap_or(0),
        T_OBJECT => 1 + v.members().map(|(_, m)| value_depth(m)).max().unwrap_or(0),
        _ => 0,
    }
}

// ── Order ─────────────────────────────────────────────────────────────

/// Where a type sorts: BSON's order over the canonical types.
pub fn type_rank(ty: u8) -> u8 {
    match ty {
        T_NULL => 1,
        T_INTEGER | T_DECIMAL => 2,
        T_STRING => 3,
        T_OBJECT => 4,
        T_ARRAY => 5,
        T_BINARY => 6,
        T_IDENTIFIER => 7,
        T_BOOLEAN => 8,
        T_TIMESTAMP => 9,
        _ => 0,
    }
}

/// The total order of the module docs.
pub fn compare(a: Value<'_>, b: Value<'_>) -> Ordering {
    let (ra, rb) = (type_rank(a.ty()), type_rank(b.ty()));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match a.ty() {
        T_NULL => Ordering::Equal,
        T_INTEGER | T_DECIMAL => compare_numbers(a, b),
        T_STRING => a.as_str().cmp(&b.as_str()),
        T_BOOLEAN => a.as_bool().cmp(&b.as_bool()),
        T_TIMESTAMP => a.as_timestamp().cmp(&b.as_timestamp()),
        T_IDENTIFIER => a.as_identifier().cmp(&b.as_identifier()),
        T_BINARY => {
            let (sa, da) = a.as_binary().unwrap_or((0, &[]));
            let (sb, db) = b.as_binary().unwrap_or((0, &[]));
            da.len().cmp(&db.len()).then(sa.cmp(&sb)).then(da.cmp(db))
        }
        T_ARRAY => {
            let (mut x, mut y) = (a.elements(), b.elements());
            loop {
                match (x.next(), y.next()) {
                    (None, None) => return Ordering::Equal,
                    (None, Some(_)) => return Ordering::Less,
                    (Some(_), None) => return Ordering::Greater,
                    (Some(p), Some(q)) => match compare(p, q) {
                        Ordering::Equal => {}
                        o => return o,
                    },
                }
            }
        }
        T_OBJECT => {
            let (mut x, mut y) = (a.members(), b.members());
            loop {
                match (x.next(), y.next()) {
                    (None, None) => return Ordering::Equal,
                    (None, Some(_)) => return Ordering::Less,
                    (Some(_), None) => return Ordering::Greater,
                    (Some((kp, p)), Some((kq, q))) => {
                        let o = type_rank(p.ty())
                            .cmp(&type_rank(q.ty()))
                            .then(kp.cmp(kq))
                            .then_with(|| compare(p, q));
                        if o != Ordering::Equal {
                            return o;
                        }
                    }
                }
            }
        }
        _ => a.bytes().cmp(b.bytes()),
    }
}

/// Equal under [`compare`]: `1`, `1.0` and `int32 1` are one value.
pub fn equal(a: Value<'_>, b: Value<'_>) -> bool {
    compare(a, b) == Ordering::Equal
}

/// JSON's equality (RFC 6902 `test`): as [`equal`], except that objects are
/// equal when they hold the same members in any order.
pub fn json_equal(a: Value<'_>, b: Value<'_>) -> bool {
    match (a.ty(), b.ty()) {
        (T_OBJECT, T_OBJECT) => {
            a.count() == b.count()
                && a.members()
                    .all(|(k, x)| b.member(k).is_some_and(|y| json_equal(x, y)))
        }
        (T_ARRAY, T_ARRAY) => {
            a.count() == b.count()
                && a.elements()
                    .zip(b.elements())
                    .all(|(x, y)| json_equal(x, y))
        }
        _ => equal(a, b),
    }
}

enum Num<'a> {
    Int(i64),
    Float(f64),
    Text(&'a [u8]),
}

fn num(v: Value<'_>) -> Num<'_> {
    if let Some(i) = v.as_i64() {
        Num::Int(i)
    } else if let Some(f) = v.as_f64() {
        Num::Float(f)
    } else {
        Num::Text(v.decimal_text().unwrap_or(b"0"))
    }
}

fn compare_numbers(a: Value<'_>, b: Value<'_>) -> Ordering {
    match (num(a), num(b)) {
        (Num::Int(x), Num::Int(y)) => x.cmp(&y),
        (Num::Float(x), Num::Float(y)) => cmp_f(x, y),
        (Num::Int(x), Num::Float(y)) => cmp_i_f(x, y),
        (Num::Float(x), Num::Int(y)) => cmp_i_f(y, x).reverse(),
        (Num::Text(x), Num::Text(y)) => cmp_dec(&Dec::from_text(x), &Dec::from_text(y)),
        (Num::Text(x), Num::Int(y)) => cmp_dec(&Dec::from_text(x), &Dec::from_i64(y)),
        (Num::Int(x), Num::Text(y)) => cmp_dec(&Dec::from_i64(x), &Dec::from_text(y)),
        (Num::Text(x), Num::Float(y)) => cmp_text_f(x, y),
        (Num::Float(x), Num::Text(y)) => cmp_text_f(y, x).reverse(),
    }
}

/// A decimal literal against a binary64. A literal is finite, so it sits
/// inside the infinities even where its binary64 would overflow to one.
fn cmp_text_f(t: &[u8], f: f64) -> Ordering {
    if f.is_nan() {
        return Ordering::Greater;
    }
    if f.is_infinite() {
        return if f > 0.0 {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    // Exactly: the literal's digits against the binary64's expansion.
    let a = Dec::from_text(t);
    let mut digits = [0u8; 800];
    let Some((fneg, fnum, fexp)) = exact_f64(f, &mut digits) else {
        return Ordering::Equal;
    };
    let sign = |neg: bool, n: usize| -> i8 {
        if n == 0 {
            0
        } else if neg {
            -1
        } else {
            1
        }
    };
    let (sa, sb) = (sign(a.neg, a.n), sign(fneg, fnum));
    if sa != sb || sa == 0 {
        return sa.cmp(&sb);
    }
    let mag = cmp_digits(&a.digits[..a.n], a.exp, &digits[..fnum], fexp);
    if sa < 0 {
        mag.reverse()
    } else {
        mag
    }
}

/// NaN below every number and equal to itself; otherwise IEEE order
/// (`-0.0 == 0.0`).
fn cmp_f(x: f64, y: f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
    }
}

/// An integer against a binary64, exactly.
fn cmp_i_f(i: i64, f: f64) -> Ordering {
    if f.is_nan() {
        return Ordering::Greater;
    }
    if f >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    if f < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    // In range: truncation is exact for |f| >= 2^53 (f is integral
    // there), and the fraction below that is exact too.
    let t = f as i64;
    if i != t {
        return i.cmp(&t);
    }
    let frac = f - t as f64;
    if frac > 0.0 {
        Ordering::Less
    } else if frac < 0.0 {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

/// A decimal in normal form: `0.d1d2…dn × 10^exp`, d1 ≠ 0, no trailing
/// zero digits; zero has no digits.
struct Dec {
    neg: bool,
    digits: [u8; MAX_DECIMAL_TEXT],
    n: usize,
    exp: i32,
}

impl Dec {
    fn from_text(t: &[u8]) -> Dec {
        let mut d = Dec {
            neg: false,
            digits: [0; MAX_DECIMAL_TEXT],
            n: 0,
            exp: 0,
        };
        let mut i = 0;
        if t.first() == Some(&b'-') {
            d.neg = true;
            i = 1;
        }
        // Digits of the integer and fraction parts, and where the point is.
        let mut point: i32 = 0;
        let mut seen_point = false;
        let mut leading = true;
        while i < t.len() {
            let c = t[i];
            if c == b'.' {
                seen_point = true;
            } else if c.is_ascii_digit() {
                if leading && c == b'0' {
                    if seen_point {
                        point -= 1;
                    }
                } else {
                    leading = false;
                    if d.n < MAX_DECIMAL_TEXT {
                        d.digits[d.n] = c - b'0';
                        d.n += 1;
                    }
                    if !seen_point {
                        point += 1;
                    }
                }
            } else {
                break;
            }
            i += 1;
        }
        let mut e: i32 = 0;
        if i < t.len() && (t[i] == b'e' || t[i] == b'E') {
            i += 1;
            let mut eneg = false;
            if i < t.len() && (t[i] == b'+' || t[i] == b'-') {
                eneg = t[i] == b'-';
                i += 1;
            }
            while i < t.len() && t[i].is_ascii_digit() {
                e = e.saturating_mul(10).saturating_add((t[i] - b'0') as i32);
                i += 1;
            }
            if eneg {
                e = -e;
            }
        }
        d.exp = point.saturating_add(e);
        d.trim();
        d
    }

    fn from_i64(v: i64) -> Dec {
        let mut d = Dec {
            neg: v < 0,
            digits: [0; MAX_DECIMAL_TEXT],
            n: 0,
            exp: 0,
        };
        let mut u = v.unsigned_abs();
        let mut tmp = [0u8; 20];
        let mut k = 0;
        while u > 0 {
            tmp[k] = (u % 10) as u8;
            u /= 10;
            k += 1;
        }
        for j in 0..k {
            d.digits[j] = tmp[k - 1 - j];
        }
        d.n = k;
        d.exp = k as i32;
        d.trim();
        d
    }

    fn trim(&mut self) {
        while self.n > 0 && self.digits[self.n - 1] == 0 {
            self.n -= 1;
        }
        if self.n == 0 {
            self.neg = false;
            self.exp = 0;
        }
    }
}

fn cmp_dec(a: &Dec, b: &Dec) -> Ordering {
    let sign = |d: &Dec| -> i8 {
        if d.n == 0 {
            0
        } else if d.neg {
            -1
        } else {
            1
        }
    };
    let (sa, sb) = (sign(a), sign(b));
    if sa != sb {
        return sa.cmp(&sb);
    }
    if sa == 0 {
        return Ordering::Equal;
    }
    let mag = a
        .exp
        .cmp(&b.exp)
        .then_with(|| a.digits[..a.n].cmp(&b.digits[..b.n]));
    if sa < 0 {
        mag.reverse()
    } else {
        mag
    }
}

/// Significant digits a number's key spells out. Every decimal literal
/// fits ([`MAX_DECIMAL_TEXT`]) and an integer needs at most 19; only a
/// binary64's exact expansion can run longer (0.1 is 55 digits, the
/// smallest subnormal 751), and its key then carries a mark that more
/// nonzero digits follow. Two distinct binary64 values differ within 17
/// digits, so a marked key still names exactly one value.
pub const MAX_KEY_DIGITS: usize = 64;

/// A number's ordered key: equal numbers — `1`, `1.0` (binary64), `1.00`
/// (a decimal literal) — have one key, and keys sort as [`compare`] sorts
/// numbers. Numbers are keyed by their EXACT decimal value (a binary64's
/// is finite), so two distinct integers past 2^53 never collide.
///
/// `[0x10]` NaN, `[0x20]` −∞, `[0x40][~exp:u16][~digits…][end]` negative,
/// `[0x80]` zero, `[0xC0][exp:u16][digits…][end]` positive, `[0xE0]` +∞;
/// `exp` is the power of ten of `0.d1d2… × 10^exp`, biased by 32768;
/// each digit is written as `digit + 1`, at most [`MAX_KEY_DIGITS`] of
/// them; `end` is `0x00` when the digits are the whole value and `0x0B`
/// when more follow (inverted for a negative), so a marked key sorts after
/// its exact prefix. `None` for a value that is not a number.
pub fn number_key(v: Value<'_>, out: &mut [u8]) -> Option<usize> {
    let mut digits = [0u8; 800];
    let (neg, n, exp): (bool, usize, i32) = if let Some(i) = v.as_i64() {
        let d = Dec::from_i64(i);
        digits[..d.n].copy_from_slice(&d.digits[..d.n]);
        (d.neg, d.n, d.exp)
    } else if let Some(t) = v.decimal_text() {
        let d = Dec::from_text(t);
        digits[..d.n].copy_from_slice(&d.digits[..d.n]);
        (d.neg, d.n, d.exp)
    } else {
        let f = v.as_f64()?;
        if f.is_nan() {
            *out.get_mut(0)? = 0x10;
            return Some(1);
        }
        if f.is_infinite() {
            *out.get_mut(0)? = if f < 0.0 { 0x20 } else { 0xE0 };
            return Some(1);
        }
        exact_f64(f, &mut digits)?
    };
    if n == 0 {
        *out.get_mut(0)? = 0x80;
        return Some(1);
    }
    let more = n > MAX_KEY_DIGITS;
    let n = n.min(MAX_KEY_DIGITS);
    let end = if more { 0x0B } else { 0x00 };
    let len = 1 + 2 + n + 1;
    let o = out.get_mut(..len)?;
    let e = (exp + 32768).clamp(0, 65535) as u16;
    if neg {
        o[0] = 0x40;
        o[1..3].copy_from_slice(&(!e).to_be_bytes());
        for (x, d) in o[3..3 + n].iter_mut().zip(&digits[..n]) {
            *x = 0xFF - (d + 1);
        }
        o[3 + n] = 0xFF - end;
    } else {
        o[0] = 0xC0;
        o[1..3].copy_from_slice(&e.to_be_bytes());
        for (x, d) in o[3..3 + n].iter_mut().zip(&digits[..n]) {
            *x = d + 1;
        }
        o[3 + n] = end;
    }
    Some(len)
}

/// A scalar's ordered, self-delimiting key: `[type rank][payload]`, keys
/// sorting as [`compare`] sorts the values, one key for equal values, and
/// each key ending where it ends — so a tuple of values is their keys
/// concatenated (an index entry). Strings and identifiers are escaped
/// (`0x00` → `0x00 0xFF`) and terminated (`0x00 0x01`); numbers are
/// [`number_key`]; binary is its length, subtype and bytes. `None` for an
/// array or object: a container is not a key.
pub fn value_key(v: Value<'_>, out: &mut [u8]) -> Option<usize> {
    *out.first_mut()? = type_rank(v.ty());
    let mut at = 1usize;
    match v.ty() {
        T_NULL => {}
        T_INTEGER | T_DECIMAL => {
            let mut k = [0u8; 80];
            let n = number_key(v, &mut k)?;
            key_put(out, &mut at, &k[..n])?;
        }
        T_STRING => key_escaped(out, &mut at, v.as_str()?)?,
        T_IDENTIFIER => key_escaped(out, &mut at, v.as_identifier()?)?,
        T_BINARY => {
            let (st, d) = v.as_binary()?;
            key_put(out, &mut at, &(d.len() as u32).to_be_bytes())?;
            key_put(out, &mut at, &[st])?;
            key_put(out, &mut at, d)?;
        }
        T_BOOLEAN => key_put(out, &mut at, &[v.as_bool()? as u8])?,
        T_TIMESTAMP => {
            let (secs, nanos) = v.as_timestamp()?;
            key_put(out, &mut at, &((secs as u64) ^ (1 << 63)).to_be_bytes())?;
            key_put(out, &mut at, &nanos.to_be_bytes())?;
        }
        _ => return None,
    }
    Some(at)
}

fn key_put(out: &mut [u8], at: &mut usize, b: &[u8]) -> Option<()> {
    out.get_mut(*at..*at + b.len())?.copy_from_slice(b);
    *at += b.len();
    Some(())
}

/// `b` escaped (`0x00` → `0x00 0xFF`) and terminated (`0x00 0x01`).
fn key_escaped(out: &mut [u8], at: &mut usize, b: &[u8]) -> Option<()> {
    for &c in b {
        if c == 0 {
            key_put(out, at, &[0x00, 0xFF])?;
        } else {
            key_put(out, at, &[c])?;
        }
    }
    key_put(out, at, &[0x00, 0x01])
}

/// A finite nonzero binary64's exact decimal value in normal form into
/// `out`: `(negative, digit count, exponent)` with `value = 0.d1… ×
/// 10^exponent`. `None` past the buffer (it holds every binary64).
fn exact_f64(f: f64, out: &mut [u8; 800]) -> Option<(bool, usize, i32)> {
    let bits = f.to_bits();
    let neg = bits >> 63 != 0;
    let be = ((bits >> 52) & 0x7FF) as i32;
    let frac = bits & ((1u64 << 52) - 1);
    let (m, e2) = if be == 0 {
        (frac, -1074)
    } else {
        (frac | (1u64 << 52), be - 1075)
    };
    if m == 0 {
        return Some((false, 0, 0));
    }
    let (n, exp) = exact_binary(m, e2, out)?;
    Some((neg, n, exp))
}

/// `m × 2^e2` (m nonzero) exactly, in normal form into `out`: `(digit
/// count, exponent)` with `value = 0.d1… × 10^exponent`. `None` past the
/// buffer, which holds every binary64 and the midpoints between them.
fn exact_binary(mut m: u64, mut e2: i32, out: &mut [u8; 800]) -> Option<(usize, i32)> {
    while m & 1 == 0 {
        m >>= 1;
        e2 += 1;
    }
    // Little-endian decimal digits of m, then × 2^e2 or × 5^-e2.
    let mut w = [0u8; 800];
    let mut len = 0usize;
    while m > 0 {
        w[len] = (m % 10) as u8;
        m /= 10;
        len += 1;
    }
    // Many powers per pass: 2^29 and 5^13 keep `digit × factor + carry`
    // inside a u64.
    let (factor, step, mut times) = if e2 >= 0 {
        (2u64, 29, e2)
    } else {
        (5u64, 13, -e2)
    };
    while times > 0 {
        let k = times.min(step);
        times -= k;
        let mul = factor.pow(k as u32);
        let mut carry = 0u64;
        for d in w[..len].iter_mut() {
            let x = *d as u64 * mul + carry;
            *d = (x % 10) as u8;
            carry = x / 10;
        }
        while carry > 0 {
            *w.get_mut(len)? = (carry % 10) as u8;
            carry /= 10;
            len += 1;
        }
    }
    // The integer has `len` digits; × 10^min(e2, 0) places the point.
    let exp = len as i32 + if e2 < 0 { e2 } else { 0 };
    // Big-endian, trailing zeros dropped.
    let mut n = len;
    let mut lo = 0usize;
    while lo < len && w[lo] == 0 {
        lo += 1;
    }
    n -= lo;
    for (i, x) in out[..n].iter_mut().enumerate() {
        *x = w[len - 1 - i];
    }
    Some((n, exp))
}

/// Significant digits that always suffice for a binary64 to read back
/// as itself.
pub const SHORTEST_DIGITS: usize = 17;

/// The shortest decimal that reads back as `f` under round-to-nearest-
/// even — the digits a JSON or JavaScript rendering of a double shows —
/// in normal form into `out`: `(negative, digit count, exponent)` with
/// `value = 0.d1… × 10^exponent`; zero has no digits. `None` for NaN and
/// the infinities, which no decimal names.
///
/// Exact: `f`'s rounding interval (the midpoints to its neighbours,
/// included when its significand is even) is expanded exactly, and at
/// each length the two candidates bracketing `f` are tested against it;
/// when both lie inside, the nearer one (the even one on a tie) wins.
pub fn shortest_f64(f: f64, out: &mut [u8; SHORTEST_DIGITS]) -> Option<(bool, usize, i32)> {
    if !f.is_finite() {
        return None;
    }
    let bits = f.to_bits();
    let neg = bits >> 63 != 0;
    let be = ((bits >> 52) & 0x7FF) as i32;
    let frac = bits & ((1u64 << 52) - 1);
    let (m, e2) = if be == 0 {
        (frac, -1074)
    } else {
        (frac | (1u64 << 52), be - 1075)
    };
    if m == 0 {
        return Some((neg, 0, 0));
    }
    let mut x = [0u8; 800];
    let (xn, xe) = exact_binary(m, e2, &mut x)?;
    let mut hi = [0u8; 800];
    let (hn, he) = exact_binary(2 * m + 1, e2 - 1, &mut hi)?;
    // Below a power of two the lower neighbour is half as far.
    let mut lo = [0u8; 800];
    let (ln, le) = if frac == 0 && be > 1 {
        exact_binary(4 * m - 1, e2 - 2, &mut lo)?
    } else {
        exact_binary(2 * m - 1, e2 - 1, &mut lo)?
    };
    let closed = m & 1 == 0;
    let inside = |d: &[u8], e: i32| {
        let above = cmp_digits(&lo[..ln], le, d, e);
        let below = cmp_digits(d, e, &hi[..hn], he);
        if closed {
            above != Ordering::Greater && below != Ordering::Greater
        } else {
            above == Ordering::Less && below == Ordering::Less
        }
    };
    for k in 1..=SHORTEST_DIGITS {
        // The candidate below f: its first k digits.
        let fk = k.min(xn);
        let mut down = [0u8; SHORTEST_DIGITS];
        down[..fk].copy_from_slice(&x[..fk]);
        let dn = trim_digits(&down, fk);
        if fk == xn {
            // f itself has k digits.
            out[..dn].copy_from_slice(&down[..dn]);
            return Some((neg, dn, xe));
        }
        // The candidate above: one more in the k-th digit.
        let mut up = down;
        let mut ue = xe;
        let mut at = k;
        let un = loop {
            if at == 0 {
                // 99…9 + 1: a single 1, a decade up.
                up[0] = 1;
                ue += 1;
                break 1;
            }
            at -= 1;
            if up[at] < 9 {
                up[at] += 1;
                break at + 1;
            }
            up[at] = 0;
        };
        let din = dn > 0 && inside(&down[..dn], xe);
        let uin = inside(&up[..un], ue);
        let pick_up = match (din, uin) {
            (false, false) => continue,
            (true, false) => false,
            (false, true) => true,
            // Both read back as f: the nearer, by the digits past k.
            (true, true) => match x[k].cmp(&5) {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal if xn > k + 1 => true,
                Ordering::Equal => down[k - 1] & 1 == 1,
            },
        };
        let (d, n, e) = if pick_up {
            (&up, un, ue)
        } else {
            (&down, dn, xe)
        };
        out[..n].copy_from_slice(&d[..n]);
        return Some((neg, n, e));
    }
    None
}

/// `digits[..n]` without its trailing zeros: the new count.
fn trim_digits(digits: &[u8], mut n: usize) -> usize {
    while n > 0 && digits[n - 1] == 0 {
        n -= 1;
    }
    n
}

/// Two positive decimals in normal form (`0.d1… × 10^e`, d1 ≠ 0, no
/// trailing zeros), compared.
fn cmp_digits(a: &[u8], ae: i32, b: &[u8], be: i32) -> Ordering {
    ae.cmp(&be).then_with(|| a.cmp(b))
}

/// The length of the one [`value_key`] at the start of `k`.
pub fn key_span(k: &[u8]) -> Option<usize> {
    let body = k.get(1..)?;
    let n = match *k.first()? {
        1 => 0,
        2 => match *body.first()? {
            0x10 | 0x20 | 0x80 | 0xE0 => 1,
            0x40 | 0xC0 => {
                // [sign][exp:2][digits…][end]; a digit byte is never an end.
                let neg = body[0] == 0x40;
                let digits = body.get(3..)?;
                let end = digits.iter().position(|&d| {
                    let d = if neg { 0xFF - d } else { d };
                    d == 0x00 || d == 0x0B
                })?;
                3 + end + 1
            }
            _ => return None,
        },
        3 | 7 => escaped_span(body)?,
        6 => {
            let len = u32::from_be_bytes(body.get(..4)?.try_into().ok()?) as usize;
            4 + 1 + len
        }
        8 => 1,
        9 => 12,
        _ => return None,
    };
    if body.len() < n {
        return None;
    }
    Some(1 + n)
}

/// The length of an escaped, terminated run, terminator included.
fn escaped_span(b: &[u8]) -> Option<usize> {
    let mut i = 0usize;
    loop {
        match (*b.get(i)?, b.get(i + 1).copied()) {
            (0x00, Some(0x01)) => return Some(i + 2),
            (0x00, Some(0xFF)) => i += 2,
            (0x00, _) => return None,
            _ => i += 1,
        }
    }
}

/// An escaped, terminated run decoded into `out`: its length.
pub fn key_unescape(b: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut i = 0usize;
    let mut o = 0usize;
    loop {
        let c = *b.get(i)?;
        if c == 0 {
            match b.get(i + 1)? {
                0x01 => return Some(o),
                0xFF => i += 2,
                _ => return None,
            }
        } else {
            i += 1;
        }
        *out.get_mut(o)? = c;
        o += 1;
    }
}

/// `b` escaped and terminated as a key run (`0x00` → `0x00 0xFF`, then
/// `0x00 0x01`) at `out[*at..]` — how [`value_key`] writes a string.
pub fn key_put_escaped(out: &mut [u8], at: &mut usize, b: &[u8]) -> Option<()> {
    key_escaped(out, at, b)
}

/// The value the [`value_key`] at the start of `k` names, written by `w`:
/// the key's length. A number comes back as an Integer when it is
/// integral and fits `i64`, else as its shortest decimal literal, else
/// (a binary64 whose expansion no literal holds, or a key marked as
/// longer than its digits) as the binary64 it names — found exactly.
pub fn key_value(k: &[u8], w: &mut Writer<'_>) -> Option<usize> {
    let n = key_span(k)?;
    let body = k.get(1..n)?;
    match *k.first()? {
        1 => w.null().ok()?,
        2 => number_from_key(body, w)?,
        3 => {
            // Unescaped straight into the tree: its length first.
            let mut len = 0usize;
            let mut i = 0usize;
            while *body.get(i)? != 0 || *body.get(i + 1)? != 0x01 {
                i += if *body.get(i)? == 0 { 2 } else { 1 };
                len += 1;
            }
            w.string_with(len, |d| {
                let _ = key_unescape(body, d);
            })
            .ok()?
        }
        7 => {
            let mut s = [0u8; MAX_IDENTIFIER_LEN];
            let l = key_unescape(body, &mut s)?;
            w.identifier(s.get(..l)?).ok()?
        }
        6 => {
            let st = *body.get(4)?;
            w.binary(st, body.get(5..)?).ok()?
        }
        8 => w.boolean(*body.first()? != 0).ok()?,
        9 => {
            let secs = u64::from_be_bytes(body.get(..8)?.try_into().ok()?) ^ (1 << 63);
            let nanos = u32::from_be_bytes(body.get(8..12)?.try_into().ok()?);
            w.timestamp(secs as i64, nanos).ok()?
        }
        _ => return None,
    }
    Some(n)
}

fn number_from_key(b: &[u8], w: &mut Writer<'_>) -> Option<()> {
    let neg = match *b.first()? {
        0x10 => return w.f64(f64::NAN).ok(),
        0x20 => return w.f64(f64::NEG_INFINITY).ok(),
        0xE0 => return w.f64(f64::INFINITY).ok(),
        0x80 => return w.int(0).ok(),
        0x40 => true,
        0xC0 => false,
        _ => return None,
    };
    let inv = |x: u8| if neg { 0xFF - x } else { x };
    let e = u16::from_be_bytes([inv(*b.get(1)?), inv(*b.get(2)?)]);
    let exp = e as i32 - 32768;
    let mut digits = [0u8; MAX_KEY_DIGITS];
    let mut n = 0usize;
    let mut more = false;
    for &x in b.get(3..)? {
        let x = inv(x);
        if x == 0x00 || x == 0x0B {
            more = x == 0x0B;
            break;
        }
        *digits.get_mut(n)? = x.checked_sub(1)?;
        n += 1;
    }
    let d = digits.get(..n)?;
    // Integral and within i64: an Integer.
    if !more && exp >= n as i32 && exp <= 19 {
        let mut v: i128 = 0;
        for i in 0..exp as usize {
            v = v * 10 + *d.get(i).unwrap_or(&0) as i128;
        }
        let v = if neg { -v } else { v };
        if let Ok(i) = i64::try_from(v) {
            return w.int(i).ok();
        }
    }
    if !more {
        let mut t = [0u8; 2 * MAX_DECIMAL_TEXT];
        if let Some(l) = decimal_literal(neg, d, exp, &mut t) {
            return w.decimal_text(t.get(..l)?).ok();
        }
    }
    w.f64(nearest_f64(neg, d, exp, more)?).ok()
}

/// The shortest JSON literal of `±0.d… × 10^exp`, if one within
/// [`MAX_DECIMAL_TEXT`] bytes (and the exponent bound) holds it.
fn decimal_literal(neg: bool, d: &[u8], exp: i32, out: &mut [u8]) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut tmp = [0u8; 2 * MAX_DECIMAL_TEXT];
    let n = d.len() as i32;
    for form in 0..3 {
        let mut at = 0usize;
        let mut put = |c: u8| -> Option<()> {
            *tmp.get_mut(at)? = c;
            at += 1;
            Some(())
        };
        let ok = (|| -> Option<()> {
            if neg {
                put(b'-')?;
            }
            match form {
                // Plain: digits, the point inside or zeros around.
                0 => {
                    if exp <= 0 {
                        put(b'0')?;
                        put(b'.')?;
                        for _ in 0..-exp {
                            put(b'0')?;
                        }
                        for &x in d {
                            put(b'0' + x)?;
                        }
                    } else if exp >= n {
                        for &x in d {
                            put(b'0' + x)?;
                        }
                        for _ in 0..exp - n {
                            put(b'0')?;
                        }
                    } else {
                        for (i, &x) in d.iter().enumerate() {
                            if i as i32 == exp {
                                put(b'.')?;
                            }
                            put(b'0' + x)?;
                        }
                    }
                }
                // Scientific: d.ddd e±x.
                _ => {
                    put(b'0' + *d.first()?)?;
                    if d.len() > 1 {
                        put(b'.')?;
                        for &x in d.get(1..)? {
                            put(b'0' + x)?;
                        }
                    }
                    put(b'e')?;
                    let x = exp - 1;
                    if x < 0 {
                        put(b'-')?;
                    }
                    let mut digs = [0u8; 10];
                    let mut k = 0;
                    let mut v = x.unsigned_abs();
                    loop {
                        digs[k] = b'0' + (v % 10) as u8;
                        k += 1;
                        v /= 10;
                        if v == 0 {
                            break;
                        }
                    }
                    for i in (0..k).rev() {
                        put(digs[i])?;
                    }
                }
            }
            Some(())
        })();
        if ok.is_some() && decimal_text_is_valid(tmp.get(..at)?) && best.is_none_or(|b| at < b) {
            out.get_mut(..at)?.copy_from_slice(tmp.get(..at)?);
            best = Some(at);
        }
        if form == 1 {
            break;
        }
    }
    best
}

/// 10^0 ..= 10^22, each exact in a binary64.
const POW10: [f64; 23] = {
    let mut t = [1.0f64; 23];
    let mut i = 1;
    while i < 23 {
        t[i] = t[i - 1] * 10.0;
        i += 1;
    }
    t
};

/// The binary64 nearest `±0.d… × 10^exp` (round half to even), found
/// exactly: from an estimate, step toward the value until it lies in the
/// candidate's rounding interval. `more` says nonzero digits follow `d`
/// (the value lies strictly above the digits' magnitude).
fn nearest_f64(neg: bool, d: &[u8], exp: i32, more: bool) -> Option<f64> {
    // An estimate from the first 19 digits.
    let used = d.len().min(19);
    let mut m: u64 = 0;
    for &x in d.get(..used)? {
        m = m * 10 + x as u64;
    }
    // Scaled by exact powers of ten (10^22 is the largest a binary64
    // holds exactly), in steps, so the estimate neither overflows nor
    // underflows on the way.
    let mut f = m as f64;
    let mut e = exp - used as i32;
    while e > 0 && f.is_finite() {
        let k = e.min(22);
        f *= POW10[k as usize];
        e -= k;
    }
    while e < 0 && f != 0.0 {
        let k = (-e).min(22);
        f /= POW10[k as usize];
        e += k;
    }
    if !f.is_finite() {
        f = f64::MAX;
    }
    if f == 0.0 {
        f = f64::from_bits(1);
    }
    // Compare |value| with a candidate's interval, in exact decimals.
    let place = |f: f64| -> Option<Ordering> {
        if f == 0.0 {
            return Some(Ordering::Greater);
        }
        let bits = f.to_bits() & !(1 << 63);
        let be = ((bits >> 52) & 0x7FF) as i32;
        let frac = bits & ((1u64 << 52) - 1);
        let (m, e2) = if be == 0 {
            (frac, -1074)
        } else {
            (frac | (1u64 << 52), be - 1075)
        };
        let mut lo = [0u8; 800];
        let mut hi = [0u8; 800];
        let (hn, he) = exact_binary(2 * m + 1, e2 - 1, &mut hi)?;
        let (ln, le) = if frac == 0 && be > 1 {
            exact_binary(4 * m - 1, e2 - 2, &mut lo)?
        } else {
            exact_binary(2 * m - 1, e2 - 1, &mut lo)?
        };
        let closed = m & 1 == 0;
        // The value against a bound: its digits, then `more`.
        let vs = |b: &[u8], be: i32| -> Ordering {
            match cmp_digits(d, exp, b, be) {
                // Equal digits: the value is above when more follow.
                Ordering::Equal if more => Ordering::Greater,
                o => o,
            }
        };
        let below = vs(&lo[..ln], le);
        let above = vs(&hi[..hn], he);
        Some(
            if below == Ordering::Less || (below == Ordering::Equal && !closed) {
                Ordering::Less
            } else if above == Ordering::Greater || (above == Ordering::Equal && !closed) {
                Ordering::Greater
            } else {
                Ordering::Equal
            },
        )
    };
    for _ in 0..4096 {
        match place(f)? {
            Ordering::Equal => return Some(if neg { -f } else { f }),
            Ordering::Less => f = f.next_down(),
            Ordering::Greater => {
                if f == f64::MAX {
                    return Some(if neg {
                        f64::NEG_INFINITY
                    } else {
                        f64::INFINITY
                    });
                }
                f = f.next_up()
            }
        }
        if f <= 0.0 {
            return Some(if neg { -0.0 } else { 0.0 });
        }
    }
    None
}

/// A decimal literal as the binary64 nearest it (round half to even):
/// correctly rounded, `-0` as negative zero, past the range an infinity.
pub fn decimal_text_to_f64(t: &[u8]) -> f64 {
    let d = Dec::from_text(t);
    let neg = t.first() == Some(&b'-');
    if d.n == 0 {
        return if neg { -0.0 } else { 0.0 };
    }
    // The search always ends; its bound is a guard, not a limit.
    nearest_f64(d.neg, &d.digits[..d.n], d.exp, false).unwrap_or(f64::NAN)
}

// ── Paths ─────────────────────────────────────────────────────────────

/// An encoded path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Path<'a> {
    b: &'a [u8],
}

impl<'a> Path<'a> {
    /// Validate an encoded path, exactly.
    pub fn parse(b: &'a [u8]) -> Result<Self, TreeError> {
        let n = *b.first().ok_or(TreeError::BadPath)? as usize;
        if n > MAX_PATH_SEGMENTS {
            return Err(TreeError::BadPath);
        }
        let mut at = 1;
        for _ in 0..n {
            let len = rd_u16(b, at).ok_or(TreeError::BadPath)? as usize;
            at += 2;
            let seg = b.get(at..at + len).ok_or(TreeError::BadPath)?;
            if !utf8_is_valid(seg) {
                return Err(TreeError::BadUtf8);
            }
            at += len;
        }
        if at != b.len() {
            return Err(TreeError::BadPath);
        }
        Ok(Path { b })
    }

    /// Wrap a path already validated (a descriptor's or request's, or a
    /// constant). Readers still never panic on bytes that were not.
    pub const fn trusted(b: &'a [u8]) -> Self {
        Path { b }
    }

    /// The empty path: the root.
    pub const ROOT: Path<'static> = Path { b: &[0] };

    pub fn bytes(&self) -> &'a [u8] {
        self.b
    }

    pub fn len(&self) -> usize {
        self.b.first().copied().unwrap_or(0) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn segments(&self) -> Segments<'a> {
        Segments {
            b: self.b,
            at: 1,
            left: self.len(),
        }
    }

    /// Segment `i`.
    pub fn segment(&self, i: usize) -> Option<&'a [u8]> {
        self.segments().nth(i)
    }

    /// Is `self` a prefix of (or equal to) `other`?
    pub fn is_prefix_of(&self, other: Path<'_>) -> bool {
        self.len() <= other.len() && self.segments().zip(other.segments()).all(|(a, b)| a == b)
    }
}

pub struct Segments<'a> {
    b: &'a [u8],
    at: usize,
    left: usize,
}

impl<'a> Iterator for Segments<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.left == 0 {
            return None;
        }
        let len = rd_u16(self.b, self.at)? as usize;
        let s = self.b.get(self.at + 2..self.at + 2 + len)?;
        self.at += 2 + len;
        self.left -= 1;
        Some(s)
    }
}

/// Builds an encoded path.
pub struct PathWriter<'a> {
    out: &'a mut [u8],
    pos: usize,
}

impl<'a> PathWriter<'a> {
    pub fn new(out: &'a mut [u8]) -> Result<Self, TreeError> {
        *out.first_mut().ok_or(TreeError::Overflow)? = 0;
        Ok(PathWriter { out, pos: 1 })
    }

    pub fn push(&mut self, seg: &[u8]) -> Result<(), TreeError> {
        let n = self.out[0] as usize;
        if n >= MAX_PATH_SEGMENTS {
            return Err(TreeError::BadPath);
        }
        if !utf8_is_valid(seg) {
            return Err(TreeError::BadUtf8);
        }
        let len = u16::try_from(seg.len()).map_err(|_| TreeError::BadPath)?;
        let end = self.pos + 2 + seg.len();
        let d = self.out.get_mut(self.pos..end).ok_or(TreeError::Overflow)?;
        d[0] = len as u8;
        d[1] = (len >> 8) as u8;
        for (x, y) in d[2..].iter_mut().zip(seg) {
            *x = *y;
        }
        self.pos = end;
        self.out[0] = n as u8 + 1;
        Ok(())
    }

    /// Append the segment for array index `i`.
    pub fn push_index(&mut self, i: usize) -> Result<(), TreeError> {
        let mut buf = [0u8; 20];
        let n = fmt_usize(i, &mut buf);
        self.push(&buf[..n])
    }

    pub fn finish(&self) -> usize {
        self.pos
    }
}

fn fmt_usize(mut v: usize, out: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut k = 0;
    loop {
        tmp[k] = b'0' + (v % 10) as u8;
        v /= 10;
        k += 1;
        if v == 0 {
            break;
        }
    }
    for j in 0..k {
        out[j] = tmp[k - 1 - j];
    }
    k
}

/// The array index a segment names: decimal digits, no leading zero
/// (except `0` itself). `-` and anything else are not indices.
pub fn segment_index(seg: &[u8]) -> Option<usize> {
    if seg.is_empty() || seg.len() > 10 || (seg.len() > 1 && seg[0] == b'0') {
        return None;
    }
    let mut v: usize = 0;
    for &c in seg {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as usize)?;
    }
    Some(v)
}

/// The segment naming the position past an array's end.
pub const APPEND_SEGMENT: &[u8] = b"-";

/// One step down: an object's member, or an array's element.
pub fn child<'a>(v: Value<'a>, seg: &[u8]) -> Option<Value<'a>> {
    match v.ty() {
        T_OBJECT => v.member(seg),
        T_ARRAY => v.element(segment_index(seg)?),
        _ => None,
    }
}

/// The value at `path` under `root`.
pub fn lookup<'a>(root: Value<'a>, path: Path<'_>) -> Option<Value<'a>> {
    let mut v = root;
    for seg in path.segments() {
        v = child(v, seg)?;
    }
    Some(v)
}

/// RFC 6901: a JSON pointer (`""` or `/a/b~1c/0`) as a path.
pub fn path_from_pointer(ptr: &[u8], out: &mut [u8]) -> Result<usize, TreeError> {
    let mut w = PathWriter::new(out)?;
    if ptr.is_empty() {
        return Ok(w.finish());
    }
    if ptr[0] != b'/' {
        return Err(TreeError::BadPath);
    }
    let mut seg = [0u8; 256];
    let mut i = 1;
    loop {
        let mut n = 0;
        while i < ptr.len() && ptr[i] != b'/' {
            let c = if ptr[i] == b'~' {
                i += 1;
                match ptr.get(i) {
                    Some(b'0') => b'~',
                    Some(b'1') => b'/',
                    _ => return Err(TreeError::BadPath),
                }
            } else {
                ptr[i]
            };
            *seg.get_mut(n).ok_or(TreeError::BadPath)? = c;
            n += 1;
            i += 1;
        }
        w.push(&seg[..n])?;
        if i >= ptr.len() {
            break;
        }
        i += 1; // the '/'
    }
    Ok(w.finish())
}
