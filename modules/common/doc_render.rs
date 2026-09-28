//! A JSON dialect's answers as templates over documents: views, stream
//! framing and server-side tables, all stated by the graph's schema
//! (`doc_schema.rs`'s grammar) — nanocloud's store connectors' rendering,
//! over canonical trees instead of JSON text.
//!
//! Mounted beside `doc_tree.rs`, `doc_catalog.rs`, `doc_ops.rs`,
//! `doc_fields.rs`, `json_tree.rs` and `doc_schema.rs` (reached as
//! `super::…`). Nothing here names a protocol: Kubernetes'
//! Scale view, watch events, bookmarks, list tails and Table are the
//! schema's text.
//!
//! ## Templates
//!
//! A value template (a view, a table row's envelope) is JSON with holes:
//! `{{<path>|<default>}}` is the JSON of the value at the dotted path —
//! the default's raw text when the value is absent or null, `null` without
//! one; `{{pairs:<path>}}` is an object's members as `k=v,k=v`, escaped
//! for the JSON string the template places it in.
//!
//! A framing template (`event`, `bookmark`, `list.tail`, `table.col`) has
//! holes for values only the dialect knows: `{{<name>}}` is the value;
//! `{{?<name>:<text>}}` is `text` with each `$` replaced by the value, and
//! nothing when the value is empty. Values are placed as given.
//!
//! ## Tables
//!
//! The columns are the graph's: per kind, `Kind=Header[/format]:<cell>,…`,
//! entries `;`-separated, a `*` entry serving every kind not named. The
//! envelope is the schema's `table.head` (`{{cols}}`), `table.col`
//! (`{{name}}`, `{{format}}`), `table.row` (`{{cells}}` and value holes over
//! the object) and `table.end`. A cell is `<path>|<default>` (the value's
//! text, the default when absent or empty, `<none>` without one) or text
//! with `@fn(args)` parts:
//!
//! | Function | Text |
//! |---|---|
//! | `@v(path\|default)` | The value's text |
//! | `@n(path)` | How many elements the list at path has |
//! | `@n(path?k=v)` | How many have member `k` whose text is `v` |
//! | `@sum(path:k)` | The sum of member `k`'s leading digits over them |
//! | `@age(path)` | Time since the RFC 3339 timestamp at path, as kubectl prints a duration |
//!
//! A value's text is a string's content, or the JSON of anything else.
//! Every cell is a JSON string, escaped.

use super::doc_catalog::EVERY_ELEMENT;
use super::doc_fields::{
    elements as path_elements, keyed_members, tree, Element, Entry, FieldError, FieldSet, Loc,
    Owners, OwnersWriter, PathBuf, SetBuilder, EMPTY_SET, OWN_APPLY, OWN_UPDATE,
};
use super::doc_ops::{self, list_rule, rfc3339_secs, rfc3339_utc, Keyed, ListRule};
use super::doc_schema::{directive, dotted_path};
use super::doc_tree::{
    self, key_unescape, key_value, Path, Value, Writer, MAX_DEPTH, T_ARRAY, T_NULL, T_OBJECT,
    T_STRING,
};
use super::json_tree;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenderError {
    /// The output buffer is full.
    Overflow,
    /// A template or table spec that is not the grammar's: an unclosed
    /// hole, a missing `{{cells}}`, an unknown function.
    BadTemplate,
    /// A value JSON cannot state.
    Unrepresentable,
}

/// An output buffer being appended to.
pub struct Out<'a> {
    b: &'a mut [u8],
    at: usize,
}

impl<'a> Out<'a> {
    pub fn new(b: &'a mut [u8]) -> Self {
        Out { b, at: 0 }
    }

    /// Bytes written.
    pub fn len(&self) -> usize {
        self.at
    }

    pub fn is_empty(&self) -> bool {
        self.at == 0
    }

    pub fn put(&mut self, bytes: &[u8]) -> Result<(), RenderError> {
        let end = self.at + bytes.len();
        let d = self.b.get_mut(self.at..end).ok_or(RenderError::Overflow)?;
        for (x, y) in d.iter_mut().zip(bytes) {
            *x = *y;
        }
        self.at = end;
        Ok(())
    }

    /// Append what `f` writes into the room left.
    fn with(
        &mut self,
        f: impl FnOnce(&mut [u8]) -> Result<usize, json_tree::JsonError>,
    ) -> Result<(), RenderError> {
        let room = self.b.get_mut(self.at..).ok_or(RenderError::Overflow)?;
        let n = f(room).map_err(|e| match e.reason {
            json_tree::JsonReason::Overflow => RenderError::Overflow,
            _ => RenderError::Unrepresentable,
        })?;
        self.at += n;
        Ok(())
    }

    /// `s` escaped for the inside of a JSON string.
    pub fn escaped(&mut self, s: &[u8]) -> Result<(), RenderError> {
        self.with(|d| json_tree::escape(s, d))
    }

    /// The JSON of `v`.
    pub fn json(&mut self, v: Value<'_>) -> Result<(), RenderError> {
        self.with(|d| json_tree::render(v, d))
    }

    /// Escape, in place, what was written since `from` for the inside of
    /// a JSON string: the text grows into the room after it.
    fn escape_from(&mut self, from: usize) -> Result<(), RenderError> {
        let width = |c: u8| match c {
            b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 0x08 | 0x0C => 2,
            c if c < 0x20 => 6,
            _ => 1,
        };
        let text = self.b.get(from..self.at).ok_or(RenderError::Overflow)?;
        let grown: usize = text.iter().map(|&c| width(c)).sum();
        let end = from + grown;
        if end > self.b.len() {
            return Err(RenderError::Overflow);
        }
        // Back to front, so nothing is overwritten before it is read.
        let (mut r, mut w) = (self.at, end);
        while r > from {
            r -= 1;
            let c = self.b[r];
            let mut e = [0u8; 6];
            let n = width(c);
            match c {
                b'"' => e[..2].copy_from_slice(b"\\\""),
                b'\\' => e[..2].copy_from_slice(b"\\\\"),
                b'\n' => e[..2].copy_from_slice(b"\\n"),
                b'\r' => e[..2].copy_from_slice(b"\\r"),
                b'\t' => e[..2].copy_from_slice(b"\\t"),
                0x08 => e[..2].copy_from_slice(b"\\b"),
                0x0C => e[..2].copy_from_slice(b"\\f"),
                c if c < 0x20 => {
                    e.copy_from_slice(b"\\u0000");
                    e[4] = HEX[(c >> 4) as usize];
                    e[5] = HEX[(c & 0xF) as usize];
                }
                c => e[0] = c,
            }
            w -= n;
            self.b[w..w + n].copy_from_slice(&e[..n]);
        }
        self.at = end;
        Ok(())
    }

    fn decimal(&mut self, mut v: u64) -> Result<(), RenderError> {
        let mut d = [0u8; 20];
        let mut n = 0usize;
        loop {
            d[19 - n] = b'0' + (v % 10) as u8;
            n += 1;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        self.put(d.get(20 - n..).unwrap_or(&[]))
    }
}

const HEX: [u8; 16] = *b"0123456789abcdef";

/// Where `needle` first occurs in `hay`.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// `tpl` split around the hole `hole` (e.g. `{{object}}`).
pub fn split<'a>(tpl: &'a [u8], hole: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
    let at = find(tpl, hole)?;
    Some((tpl.get(..at)?, tpl.get(at + hole.len()..)?))
}

/// Walk `tpl`'s `{{…}}` holes: literal text is copied, each hole handed
/// to `hole`.
fn holes(
    tpl: &[u8],
    o: &mut Out<'_>,
    mut hole: impl FnMut(&[u8], &mut Out<'_>) -> Result<(), RenderError>,
) -> Result<(), RenderError> {
    let mut rest = tpl;
    while let Some(open) = find(rest, b"{{") {
        o.put(rest.get(..open).unwrap_or(&[]))?;
        let inner = rest.get(open + 2..).unwrap_or(&[]);
        let close = find(inner, b"}}").ok_or(RenderError::BadTemplate)?;
        hole(inner.get(..close).unwrap_or(&[]), o)?;
        rest = inner.get(close + 2..).unwrap_or(&[]);
    }
    o.put(rest)
}

/// A framing template filled from `vars` (`(name, value)`).
pub fn fill(tpl: &[u8], vars: &[(&[u8], &[u8])], o: &mut Out<'_>) -> Result<(), RenderError> {
    let var = |name: &[u8]| -> &[u8] {
        vars.iter()
            .find(|(n, _)| *n == name)
            .map_or(&[][..], |(_, v)| *v)
    };
    holes(tpl, o, |h, o| match h.strip_prefix(b"?") {
        Some(cond) => {
            let c = cond
                .iter()
                .position(|&b| b == b':')
                .ok_or(RenderError::BadTemplate)?;
            let v = var(cond.get(..c).unwrap_or(&[]));
            if v.is_empty() {
                return Ok(());
            }
            for part in cond
                .get(c + 1..)
                .unwrap_or(&[])
                .split(|&b| b == b'$')
                .enumerate()
            {
                if part.0 > 0 {
                    o.put(v)?;
                }
                o.put(part.1)?;
            }
            Ok(())
        }
        None => o.put(var(h)),
    })
}

/// The value at the dotted `path` of `doc`.
pub fn at<'a>(doc: Value<'a>, path: &[u8]) -> Option<Value<'a>> {
    let mut p = [0u8; 512];
    let n = dotted_path(path, &mut p).ok()?;
    doc_tree::lookup(doc, Path::parse(p.get(..n)?).ok()?)
}

/// A value template over `doc`.
pub fn view(tpl: &[u8], doc: Value<'_>, o: &mut Out<'_>) -> Result<(), RenderError> {
    holes(tpl, o, |h, o| {
        if let Some(path) = h.strip_prefix(b"pairs:") {
            let Some(obj) = at(doc, path).filter(|v| v.ty() == T_OBJECT) else {
                return Ok(());
            };
            for (i, (k, v)) in obj.members().enumerate() {
                if i > 0 {
                    o.put(b",")?;
                }
                o.escaped(k)?;
                o.put(b"=")?;
                text(v, o, true)?;
            }
            return Ok(());
        }
        let (path, dflt) = match h.iter().position(|&b| b == b'|') {
            Some(b) => (h.get(..b).unwrap_or(&[]), h.get(b + 1..).unwrap_or(&[])),
            None => (h, &b"null"[..]),
        };
        match at(doc, path).filter(|v| v.ty() != T_NULL) {
            Some(v) => o.json(v),
            None => o.put(dflt),
        }
    })
}

/// A value's text: a string's content, else its JSON; `escape` for the
/// inside of a JSON string.
fn text(v: Value<'_>, o: &mut Out<'_>, escape: bool) -> Result<(), RenderError> {
    match v.as_str() {
        Some(s) if escape => o.escaped(s),
        Some(s) => o.put(s),
        None => {
            let from = o.at;
            o.json(v)?;
            if escape {
                o.escape_from(from)?;
            }
            Ok(())
        }
    }
}

/// Whether `v`'s text is `want`. A value that is not a string is compared
/// as its JSON (`true`, `3`); one whose JSON runs past 256 bytes matches
/// nothing — a `k=v` spec states scalars.
fn text_is(v: Value<'_>, want: &[u8]) -> bool {
    match v.as_str() {
        Some(s) => s == want,
        None => {
            let mut buf = [0u8; 256];
            let n = {
                let mut t = Out::new(&mut buf);
                if t.json(v).is_err() {
                    return false;
                }
                t.len()
            };
            buf.get(..n) == Some(want)
        }
    }
}

// ── Tables ──────────────────────────────────────────────────────────────

/// The column spec for `kind` (`Header:cell,…`), else the `*` entry's.
pub fn table_columns<'a>(spec: &'a [u8], kind: &[u8]) -> &'a [u8] {
    let mut any: &[u8] = &[];
    for ent in spec.split(|&b| b == b';') {
        if let Some(eq) = ent.iter().position(|&b| b == b'=') {
            let (k, v) = (
                ent.get(..eq).unwrap_or(&[]),
                ent.get(eq + 1..).unwrap_or(&[]),
            );
            if k == kind {
                return v;
            }
            if k == b"*" {
                any = v;
            }
        }
    }
    any
}

/// A column entry's `(header, format, cell)`.
fn column(c: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let (head, cell) = match c.iter().position(|&b| b == b':') {
        Some(i) => (c.get(..i).unwrap_or(&[]), c.get(i + 1..).unwrap_or(&[])),
        None => (c, &[][..]),
    };
    match head.iter().position(|&b| b == b'/') {
        Some(i) => (
            head.get(..i).unwrap_or(&[]),
            head.get(i + 1..).unwrap_or(&[]),
            cell,
        ),
        None => (head, &[][..], cell),
    }
}

fn columns(cols: &[u8]) -> impl Iterator<Item = &[u8]> {
    cols.split(|&b| b == b',').filter(|c| !c.is_empty())
}

/// The table's head through the rows' opening.
pub fn table_head(schema: &[u8], cols: &[u8], o: &mut Out<'_>) -> Result<(), RenderError> {
    let head = directive(schema, b"table.head").unwrap_or(&[]);
    let (pre, post) = split(head, b"{{cols}}").ok_or(RenderError::BadTemplate)?;
    let col = directive(schema, b"table.col").unwrap_or(&[]);
    o.put(pre)?;
    for (i, c) in columns(cols).enumerate() {
        if i > 0 {
            o.put(b",")?;
        }
        let (name, format, _) = column(c);
        fill(col, &[(b"name", name), (b"format", format)], o)?;
    }
    o.put(post)
}

/// What closes a table that is not a list.
pub fn table_end(schema: &[u8], o: &mut Out<'_>) -> Result<(), RenderError> {
    o.put(directive(schema, b"table.end").unwrap_or(&[]))
}

/// One row: the schema's `table.row` around the cells, its value holes
/// over `doc`. `now_s` is the time `@age` measures to, in Unix seconds.
pub fn table_row(
    schema: &[u8],
    cols: &[u8],
    doc: Value<'_>,
    now_s: i64,
    o: &mut Out<'_>,
) -> Result<(), RenderError> {
    let row = directive(schema, b"table.row").unwrap_or(&[]);
    let (pre, post) = split(row, b"{{cells}}").ok_or(RenderError::BadTemplate)?;
    view(pre, doc, o)?;
    for (i, c) in columns(cols).enumerate() {
        if i > 0 {
            o.put(b",")?;
        }
        o.put(b"\"")?;
        cell(column(c).2, doc, now_s, o)?;
        o.put(b"\"")?;
    }
    view(post, doc, o)
}

/// `path|default`: the value's text, else the default.
fn value_cell(arg: &[u8], none: &[u8], doc: Value<'_>, o: &mut Out<'_>) -> Result<(), RenderError> {
    let (path, dflt) = match arg.iter().position(|&b| b == b'|') {
        Some(i) => (arg.get(..i).unwrap_or(&[]), arg.get(i + 1..).unwrap_or(&[])),
        None => (arg, none),
    };
    let set = at(doc, path).filter(|v| match v.ty() {
        T_NULL => false,
        T_STRING => v.as_str().is_some_and(|s| !s.is_empty()),
        _ => true,
    });
    match set {
        Some(v) => text(v, o, true),
        None => o.escaped(dflt),
    }
}

/// One cell's text, escaped for its string.
#[allow(
    clippy::manual_contains,
    reason = "slice::contains links core's memchr, which PIC modules cannot resolve"
)]
fn cell(c: &[u8], doc: Value<'_>, now_s: i64, o: &mut Out<'_>) -> Result<(), RenderError> {
    if !c.iter().any(|&b| b == b'@') {
        return value_cell(c, b"<none>", doc, o);
    }
    let mut rest = c;
    while let Some(i) = rest.iter().position(|&b| b == b'@') {
        o.escaped(rest.get(..i).unwrap_or(&[]))?;
        let call = rest.get(i + 1..).unwrap_or(&[]);
        let open = call
            .iter()
            .position(|&b| b == b'(')
            .ok_or(RenderError::BadTemplate)?;
        let close = call
            .iter()
            .position(|&b| b == b')')
            .filter(|&e| e > open)
            .ok_or(RenderError::BadTemplate)?;
        let f = call.get(..open).unwrap_or(&[]);
        let arg = call.get(open + 1..close).unwrap_or(&[]);
        function(f, arg, doc, now_s, o)?;
        rest = call.get(close + 1..).unwrap_or(&[]);
    }
    o.escaped(rest)
}

/// The elements of the list at `path` (none when it is not a list).
fn elements<'a>(doc: Value<'a>, path: &[u8]) -> impl Iterator<Item = Value<'a>> {
    at(doc, path)
        .filter(|v| v.ty() == T_ARRAY)
        .into_iter()
        .flat_map(|v| v.elements())
}

fn function(
    f: &[u8],
    arg: &[u8],
    doc: Value<'_>,
    now_s: i64,
    o: &mut Out<'_>,
) -> Result<(), RenderError> {
    match f {
        b"v" => value_cell(arg, b"", doc, o),
        b"n" => {
            let (path, want) = match arg.iter().position(|&b| b == b'?') {
                Some(i) => (arg.get(..i).unwrap_or(&[]), arg.get(i + 1..)),
                None => (arg, None),
            };
            let n = match want {
                None => elements(doc, path).count(),
                Some(kv) => {
                    let eq = kv
                        .iter()
                        .position(|&b| b == b'=')
                        .ok_or(RenderError::BadTemplate)?;
                    let (k, v) = (kv.get(..eq).unwrap_or(&[]), kv.get(eq + 1..).unwrap_or(&[]));
                    elements(doc, path)
                        .filter(|e| e.member(k).is_some_and(|m| text_is(m, v)))
                        .count()
                }
            };
            o.decimal(n as u64)
        }
        b"sum" => {
            let c = arg
                .iter()
                .position(|&b| b == b':')
                .ok_or(RenderError::BadTemplate)?;
            let (path, k) = (arg.get(..c).unwrap_or(&[]), arg.get(c + 1..).unwrap_or(&[]));
            let sum = elements(doc, path)
                .filter_map(|e| e.member(k))
                .map(leading_digits)
                .fold(0u64, u64::saturating_add);
            o.decimal(sum)
        }
        b"age" => match at(doc, arg).and_then(unix_seconds) {
            Some(t) => human_duration(now_s.saturating_sub(t), o),
            None => o.put(b"<unknown>"),
        },
        _ => Err(RenderError::BadTemplate),
    }
}

/// A value's leading decimal digits (an integer's, or a string's), as a
/// count; 0 when it has none.
fn leading_digits(v: Value<'_>) -> u64 {
    if let Some(i) = v.as_i64() {
        return i.max(0) as u64;
    }
    let mut n = 0u64;
    for &c in v.as_str().unwrap_or(&[]) {
        if !c.is_ascii_digit() {
            break;
        }
        n = n.saturating_mul(10).saturating_add((c - b'0') as u64);
    }
    n
}

/// Unix seconds of a timestamp: a Timestamp value, or a string starting
/// `YYYY-MM-DDTHH:MM:SS` (RFC 3339, read as UTC).
pub fn unix_seconds(v: Value<'_>) -> Option<i64> {
    if let Some((secs, _)) = v.as_timestamp() {
        return Some(secs);
    }
    let ts = v.as_str()?;
    let num = |a: usize, b: usize| -> Option<i64> {
        let mut n = 0i64;
        for &c in ts.get(a..b)? {
            if !c.is_ascii_digit() {
                return None;
            }
            n = n * 10 + (c - b'0') as i64;
        }
        Some(n)
    };
    let (y, m, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (hh, mm, ss) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if ts.get(4) != Some(&b'-') || ts.get(7) != Some(&b'-') || ts.get(13) != Some(&b':') {
        return None;
    }
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + hh * 3600 + mm * 60 + ss)
}

/// A duration as kubectl prints one (`HumanDuration`): two units while
/// the larger is small, one after.
pub fn human_duration(secs: i64, o: &mut Out<'_>) -> Result<(), RenderError> {
    if secs < 0 {
        return o.put(b"0s");
    }
    let s = secs as u64;
    let mut two = |a: u64, ua: &[u8], b: u64, ub: &[u8]| -> Result<(), RenderError> {
        o.decimal(a)?;
        o.put(ua)?;
        if b != 0 {
            o.decimal(b)?;
            o.put(ub)?;
        }
        Ok(())
    };
    let (m, h) = (s / 60, s / 3600);
    let d = h / 24;
    let y = d / 365;
    if s < 120 {
        two(s, b"s", 0, b"")
    } else if m < 10 {
        two(m, b"m", s % 60, b"s")
    } else if m < 180 {
        two(m, b"m", 0, b"")
    } else if h < 8 {
        two(h, b"h", m % 60, b"m")
    } else if h < 48 {
        two(h, b"h", 0, b"")
    } else if h < 24 * 8 {
        two(d, b"d", h % 24, b"h")
    } else if d < 365 * 2 {
        two(d, b"d", 0, b"")
    } else if y < 8 {
        two(y, b"y", d % 365, b"d")
    } else {
        two(y, b"y", 0, b"")
    }
}

// ── Field sets spelled as nested objects ─────────────────────────────

/// A dialect's marks for a set's spelling as nested objects (Kubernetes'
/// `fieldsV1`: `f:`, `k:`, `v:`, `.`): each element a member named by
/// its mark and text, a field an empty object.
#[derive(Clone, Copy)]
pub struct Marks<'a> {
    pub member: &'a [u8],
    pub keyed: &'a [u8],
    pub value: &'a [u8],
    pub itself: &'a [u8],
}

/// Room one element's spelling may take while it is built.
pub const ELEMENT_TEXT: usize = 1024;

/// An element's spelling into `out`: its length. A keyed element spells
/// its key as a JSON object with members in name order, a set element
/// its value as JSON.
fn element_text(e: Element<'_>, marks: &Marks<'_>, out: &mut [u8]) -> Result<usize, FieldError> {
    let mut at = 0usize;
    let mut put = |out: &mut [u8], b: &[u8]| -> Result<(), FieldError> {
        let d = out.get_mut(at..at + b.len()).ok_or(FieldError::Overflow)?;
        for (x, y) in d.iter_mut().zip(b) {
            *x = *y;
        }
        at += b.len();
        Ok(())
    };
    match e {
        Element::Itself => put(out, marks.itself)?,
        Element::Member(m) => {
            put(out, marks.member)?;
            let room = out.get_mut(at..).ok_or(FieldError::Overflow)?;
            at += key_unescape(m, room).ok_or(FieldError::Overflow)?;
        }
        Element::Value(k) => {
            put(out, marks.value)?;
            let mut t = [0u8; ELEMENT_TEXT];
            let mut w = Writer::new(&mut t);
            key_value(k, &mut w).ok_or(FieldError::Malformed)?;
            let n = w.finish().map_err(tree)?;
            let room = out.get_mut(at..).ok_or(FieldError::Overflow)?;
            let v = Value::parse(t.get(..n).unwrap_or(&[])).map_err(tree)?;
            at += json_tree::render(v, room).map_err(|_| FieldError::Overflow)?;
        }
        Element::Keyed(b) => {
            put(out, marks.keyed)?;
            // The key as an object, members in name order.
            let mut t = [0u8; ELEMENT_TEXT];
            let mut w = Writer::new(&mut t);
            w.begin_object().map_err(tree)?;
            let mut last: Option<[u8; 256]> = None;
            let mut last_n = 0usize;
            let count = keyed_members(b).count();
            for _ in 0..count {
                // The least name above the last written.
                let mut best: Option<(&[u8], &[u8])> = None;
                let mut bn = [0u8; 256];
                let mut bl = 0usize;
                for (m, k) in keyed_members(b) {
                    let mut nm = [0u8; 256];
                    let l = key_unescape(m, &mut nm).ok_or(FieldError::Overflow)?;
                    let name = nm.get(..l).unwrap_or(&[]);
                    let above = last
                        .as_ref()
                        .is_none_or(|p| name > p.get(..last_n).unwrap_or(&[]));
                    if above && best.is_none_or(|_| name < bn.get(..bl).unwrap_or(&[])) {
                        bn = nm;
                        bl = l;
                        best = Some((m, k));
                    }
                }
                let (_, k) = best.ok_or(FieldError::Malformed)?;
                w.key(bn.get(..bl).unwrap_or(&[])).map_err(tree)?;
                key_value(k, &mut w).ok_or(FieldError::Malformed)?;
                last = Some(bn);
                last_n = bl;
            }
            w.end().map_err(tree)?;
            let n = w.finish().map_err(tree)?;
            let v = Value::parse(t.get(..n).unwrap_or(&[])).map_err(tree)?;
            let room = out.get_mut(at..).ok_or(FieldError::Overflow)?;
            at += json_tree::render(v, room).map_err(|_| FieldError::Overflow)?;
        }
    }
    Ok(at)
}

/// A set spelled as nested objects into `w`: every path's elements as
/// members named by their text, a field an empty object.
pub fn spell_fields(
    s: FieldSet<'_>,
    marks: &Marks<'_>,
    w: &mut Writer<'_>,
) -> Result<(), FieldError> {
    // Open objects: the elements of the previous path still open.
    let mut open: [(usize, usize); 2 * MAX_DEPTH] = [(0, 0); 2 * MAX_DEPTH];
    let mut depth = 0usize;
    let mut prev: &[u8] = &[];
    w.begin_object().map_err(tree)?;
    for p in s.paths() {
        // Elements shared with the previous path stay open.
        let mut spans: [(usize, usize); 2 * MAX_DEPTH] = [(0, 0); 2 * MAX_DEPTH];
        let mut k = 0usize;
        let mut at = 0usize;
        for e in path_elements(p) {
            let e = e?;
            let len = element_len(e);
            *spans.get_mut(k).ok_or(FieldError::TooDeep)? = (at, at + len);
            k += 1;
            at += len;
        }
        let mut common = 0usize;
        while common < depth
            && common < k
            && prev.get(open[common].0..open[common].1) == p.get(spans[common].0..spans[common].1)
        {
            common += 1;
        }
        while depth > common {
            w.end().map_err(tree)?;
            depth -= 1;
        }
        for &(a, b) in spans.get(common..k).unwrap_or(&[]) {
            let el = path_elements(p.get(a..b).unwrap_or(&[]))
                .next()
                .ok_or(FieldError::Malformed)??;
            let mut text = [0u8; ELEMENT_TEXT];
            let tl = element_text(el, marks, &mut text)?;
            w.key(text.get(..tl).unwrap_or(&[])).map_err(tree)?;
            w.begin_object().map_err(tree)?;
            *open.get_mut(depth).ok_or(FieldError::TooDeep)? = (a, b);
            depth += 1;
        }
        prev = p;
    }
    while depth > 0 {
        w.end().map_err(tree)?;
        depth -= 1;
    }
    w.end().map_err(tree)
}

/// The encoded length of element `e`: its tag and body.
fn element_len(e: Element<'_>) -> usize {
    match e {
        Element::Itself => 1,
        Element::Member(b) | Element::Keyed(b) | Element::Value(b) => 1 + b.len(),
    }
}

/// A set spelled as nested objects (as [`spell_fields`] writes it) back into
/// `out`. A keyed element's key is read in the list key's order, so the
/// set is the one [`fields_of`] makes whatever order the spelling uses.
pub fn read_fields(
    spelled: Value<'_>,
    marks: &Marks<'_>,
    keyed: &Keyed<'_>,
    path: &mut PathBuf<'_>,
    out: &mut SetBuilder<'_>,
) -> Result<(), FieldError> {
    let mut loc = Loc::default();
    parse_at(spelled, marks, keyed, &mut loc, path, out)
}

fn parse_at<'a>(
    obj: Value<'a>,
    marks: &Marks<'_>,
    keyed: &Keyed<'_>,
    loc: &mut Loc<'a>,
    path: &mut PathBuf<'_>,
    out: &mut SetBuilder<'_>,
) -> Result<(), FieldError> {
    if obj.ty() != T_OBJECT {
        return Err(FieldError::Malformed);
    }
    for (name, sub) in obj.members() {
        let mark = path.mark();
        let mut pushed_loc = 0usize;
        if name == marks.itself {
            path.push_self()?;
        } else if let Some(m) = name.strip_prefix(marks.member) {
            path.push_member(m)?;
            loc.push(m)?;
            pushed_loc = 1;
        } else if let Some(t) = name.strip_prefix(marks.keyed) {
            let mut kt = [0u8; ELEMENT_TEXT];
            let n = json_tree::parse(t, &mut kt).map_err(|_| FieldError::Malformed)?;
            let key = Value::parse(kt.get(..n).unwrap_or(&[])).map_err(tree)?;
            let ListRule::Keyed(lk) = list_rule(keyed, loc.get()) else {
                return Err(FieldError::Malformed);
            };
            // Only the key's own members: anything else is not this key.
            if key.members().any(|(m, _)| !lk.keys().any(|k| k == m))
                || !path.push_keyed(&lk, key)?
            {
                return Err(FieldError::Malformed);
            }
            loc.push(EVERY_ELEMENT)?;
            pushed_loc = 1;
        } else if let Some(t) = name.strip_prefix(marks.value) {
            let mut vt = [0u8; ELEMENT_TEXT];
            let n = json_tree::parse(t, &mut vt).map_err(|_| FieldError::Malformed)?;
            let v = Value::parse(vt.get(..n).unwrap_or(&[])).map_err(tree)?;
            if v.is_container() {
                return Err(FieldError::Malformed);
            }
            path.push_value(v)?;
        } else {
            return Err(FieldError::Malformed);
        }
        if sub.ty() != T_OBJECT {
            return Err(FieldError::Malformed);
        }
        if sub.count() == 0 {
            out.insert(path.get())?;
        } else {
            parse_at(sub, marks, keyed, loc, path, out)?;
        }
        for _ in 0..pushed_loc {
            loc.pop();
        }
        path.truncate(mark);
    }
    Ok(())
}

// ── Ownership entries and apply conflicts, as the schema spells them ──

/// The schema's ownership dialect: where a document's ownership entries
/// are rendered (`owners`), how one entry reads (`owners.entry`, holes
/// `manager`, `op`, `api`, `time`, `sub`, `fields`), the operation names
/// (`owners.ops=<apply>,<update>`) and the field-set marks
/// (`owners.marks=<member>,<keyed>,<value>,<itself>`).
#[derive(Clone, Copy)]
pub struct OwnersDialect<'a> {
    pub path: &'a [u8],
    pub entry: &'a [u8],
    pub apply: &'a [u8],
    pub update: &'a [u8],
    pub marks: Marks<'a>,
    /// The members an entry is read back from (`owners.keys=<manager>,
    /// <op>,<api>,<time>,<sub>,<fields>`), when the schema states them.
    pub keys: Option<[&'a [u8]; 6]>,
}

/// The schema's ownership dialect, when it states every part of it.
pub fn owners_dialect(schema: &[u8]) -> Option<OwnersDialect<'_>> {
    let path = directive(schema, b"owners")?;
    let entry = directive(schema, b"owners.entry")?;
    let mut ops = directive(schema, b"owners.ops")?.split(|&c| c == b',');
    let (apply, update) = (ops.next()?, ops.next()?);
    let mut m = directive(schema, b"owners.marks")?.split(|&c| c == b',');
    let marks = Marks {
        member: m.next()?,
        keyed: m.next()?,
        value: m.next()?,
        itself: m.next()?,
    };
    if ops.next().is_some() || m.next().is_some() {
        return None;
    }
    let keys = match directive(schema, b"owners.keys") {
        None => None,
        Some(k) => {
            let mut it = k.split(|&c| c == b',');
            let mut ks: [&[u8]; 6] = [&[]; 6];
            for slot in ks.iter_mut() {
                *slot = it.next().filter(|n| !n.is_empty())?;
            }
            if it.next().is_some() {
                return None;
            }
            Some(ks)
        }
    };
    Some(OwnersDialect {
        path,
        entry,
        apply,
        update,
        marks,
        keys,
    })
}

/// Longest manager, version or scope an entry's hole takes, escaped.
const HOLE_TEXT: usize = 768;
/// Longest field an apply conflict names, spelled and escaped.
const FIELD_TEXT: usize = 2048;
/// Conflicts an apply's refusal names (it says how many there were).
pub const CONFLICTS_NAMED: usize = 8;

fn field_err(e: FieldError) -> RenderError {
    match e {
        FieldError::Overflow => RenderError::Overflow,
        _ => RenderError::Unrepresentable,
    }
}

/// `s` escaped for the inside of a JSON string into `b`: the text.
fn escaped_into<'b>(s: &[u8], b: &'b mut [u8]) -> Result<&'b [u8], RenderError> {
    let mut o = Out::new(b);
    o.escaped(s)?;
    let n = o.len();
    Ok(b.get(..n).unwrap_or(&[]))
}

/// A document's ownership record as the schema spells it: a JSON array
/// of `owners.entry`, one per entry, its fields spelled with the marks as
/// nested objects. `scratch` holds one entry's fields while they are
/// spelled (half as a tree, half as its text).
pub fn owners_json(
    o: Owners<'_>,
    d: &OwnersDialect<'_>,
    scratch: &mut [u8],
    out: &mut Out<'_>,
) -> Result<(), RenderError> {
    out.put(b"[")?;
    for (i, e) in o.entries().enumerate() {
        if i > 0 {
            out.put(b",")?;
        }
        let half = scratch.len() / 2;
        let (tb, jb) = scratch.split_at_mut(half);
        let mut w = Writer::new(tb);
        spell_fields(e.set, &d.marks, &mut w).map_err(field_err)?;
        let n = w.finish().map_err(|_| RenderError::Overflow)?;
        let set =
            Value::parse(tb.get(..n).unwrap_or(&[])).map_err(|_| RenderError::Unrepresentable)?;
        let mut fo = Out::new(jb);
        fo.json(set)?;
        let fl = fo.len();
        let mut t = [0u8; 20];
        rfc3339_utc(e.secs, &mut t).ok_or(RenderError::Unrepresentable)?;
        let (mut mb, mut vb, mut sb) = ([0u8; HOLE_TEXT], [0u8; HOLE_TEXT], [0u8; HOLE_TEXT]);
        let manager = escaped_into(e.manager, &mut mb)?;
        let api = escaped_into(e.version, &mut vb)?;
        let sub = escaped_into(e.scope, &mut sb)?;
        let op = if e.op == OWN_APPLY { d.apply } else { d.update };
        let vars: [(&[u8], &[u8]); 6] = [
            (b"manager", manager),
            (b"op", op),
            (b"api", api),
            (b"time", &t),
            (b"sub", sub),
            (b"fields", jb.get(..fl).unwrap_or(&[])),
        ];
        fill(d.entry, &vars, out)?;
    }
    out.put(b"]")
}

/// A document's ownership entries as the schema spells them (an array of
/// `owners.entry` objects, [`owners_json`]'s output) read back into an
/// ownership record in `out`: its length. Each entry is read through
/// `owners.keys`; its fields through the marks, keyed as the collection's
/// list rules say. An entry that is not an object, or lacks its manager,
/// a known operation, a time or its fields, is unrepresentable: refused,
/// never guessed at.
pub fn owners_from_json(
    entries: Value<'_>,
    d: &OwnersDialect<'_>,
    keyed: &Keyed<'_>,
    path_buf: &mut [u8],
    out: &mut [u8],
) -> Result<usize, RenderError> {
    let keys = d.keys.ok_or(RenderError::Unrepresentable)?;
    if entries.ty() != T_ARRAY {
        return Err(RenderError::Unrepresentable);
    }
    let mut w = OwnersWriter::new(out).map_err(field_err)?;
    let empty = FieldSet::parse(&EMPTY_SET).map_err(field_err)?;
    for e in entries.elements() {
        if e.ty() != T_OBJECT {
            return Err(RenderError::Unrepresentable);
        }
        let text = |k: &[u8]| e.member(k).and_then(|v| v.as_str());
        let manager = text(keys[0])
            .filter(|m| !m.is_empty())
            .ok_or(RenderError::Unrepresentable)?;
        let op = match text(keys[1]) {
            Some(o) if o == d.apply => OWN_APPLY,
            Some(o) if o == d.update => OWN_UPDATE,
            _ => return Err(RenderError::Unrepresentable),
        };
        let secs = text(keys[3])
            .and_then(rfc3339_secs)
            .ok_or(RenderError::Unrepresentable)?;
        let fields = e.member(keys[5]).ok_or(RenderError::Unrepresentable)?;
        let head = Entry {
            manager,
            op,
            secs,
            nanos: 0,
            version: text(keys[2]).unwrap_or(&[]),
            scope: text(keys[4]).unwrap_or(&[]),
            set: empty,
        };
        w.entry(&head, |sb| {
            read_fields(fields, &d.marks, keyed, &mut PathBuf::new(path_buf), sb)
        })
        .map_err(field_err)?;
    }
    Ok(w.finish())
}

/// The buffers moving a document's ownership in and out of its JSON take:
/// one entry's fields while spelled, the entries' text, and their tree.
pub struct OwnersBufs<'b> {
    pub scratch: &'b mut [u8],
    pub text: &'b mut [u8],
    pub tree: &'b mut [u8],
}

/// The dialect's `owners` path (dotted) as a tree path, into `pb`.
fn owners_path<'p>(d: &OwnersDialect<'_>, pb: &'p mut [u8; 512]) -> Result<Path<'p>, RenderError> {
    let pl = dotted_path(d.path, pb).map_err(|_| RenderError::Unrepresentable)?;
    Path::parse(pb.get(..pl).unwrap_or(&[])).map_err(|_| RenderError::Unrepresentable)
}

/// `d` with its ownership entries where the schema keeps them (the
/// `owners` path), into `w` — the object as a client, or a store's other
/// readers, see it.
pub fn with_owners(
    d: Value<'_>,
    owners: Owners<'_>,
    dl: &OwnersDialect<'_>,
    b: &mut OwnersBufs<'_>,
    w: &mut Writer<'_>,
) -> Result<(), RenderError> {
    let mut ot = Out::new(b.text);
    owners_json(owners, dl, b.scratch, &mut ot)?;
    let tl = ot.len();
    let n = json_tree::parse(b.text.get(..tl).unwrap_or(&[]), b.tree)
        .map_err(|_| RenderError::Unrepresentable)?;
    let entries =
        Value::parse(b.tree.get(..n).unwrap_or(&[])).map_err(|_| RenderError::Unrepresentable)?;
    let mut pb = [0u8; 512];
    let path = owners_path(dl, &mut pb)?;
    doc_ops::edit(d, path, doc_ops::Edit::Set(entries), w).map_err(|_| RenderError::Overflow)
}

/// [`with_owners`]' inverse: an object as stored, split into the document
/// (the entries removed, into `w`) and its ownership record (into
/// `owners_out`: its length). An object with no entries — or entries that
/// do not read as the schema spells them (a writer that is not the engine
/// wrote them) — has none: the next engine write records its own.
pub fn split_owners(
    obj: Value<'_>,
    dl: &OwnersDialect<'_>,
    keyed: &Keyed<'_>,
    path_buf: &mut [u8],
    w: &mut Writer<'_>,
    owners_out: &mut [u8],
) -> Result<usize, RenderError> {
    let mut pb = [0u8; 512];
    let path = owners_path(dl, &mut pb)?;
    let none = |out: &mut [u8]| -> Result<usize, RenderError> {
        let d = out.get_mut(..2).ok_or(RenderError::Overflow)?;
        d.copy_from_slice(&[super::doc_fields::OWNERS_V1, 0]);
        Ok(2)
    };
    match doc_tree::lookup(obj, path) {
        None => {
            w.value(obj).map_err(|_| RenderError::Overflow)?;
            none(owners_out)
        }
        Some(entries) => {
            doc_ops::edit(obj, path, doc_ops::Edit::Remove, w)
                .map_err(|_| RenderError::Overflow)?;
            match owners_from_json(entries, dl, keyed, path_buf, owners_out) {
                Ok(n) => Ok(n),
                Err(RenderError::Overflow) => Err(RenderError::Overflow),
                Err(_) => none(owners_out),
            }
        }
    }
}

/// A field as an apply conflict names it: `.member`, a keyed element
/// `[k="v",…]` (its key's members), a set element `[=v]`; the element
/// itself adds nothing.
pub fn field_text(p: &[u8], out: &mut Out<'_>) -> Result<(), RenderError> {
    let key_json = |k: &[u8], out: &mut Out<'_>| -> Result<(), RenderError> {
        let mut t = [0u8; ELEMENT_TEXT];
        let mut w = Writer::new(&mut t);
        key_value(k, &mut w).ok_or(RenderError::Unrepresentable)?;
        let n = w.finish().map_err(|_| RenderError::Overflow)?;
        let v =
            Value::parse(t.get(..n).unwrap_or(&[])).map_err(|_| RenderError::Unrepresentable)?;
        out.json(v)
    };
    let name = |m: &[u8], out: &mut Out<'_>| -> Result<(), RenderError> {
        let mut nm = [0u8; ELEMENT_TEXT];
        let l = key_unescape(m, &mut nm).ok_or(RenderError::Overflow)?;
        out.put(nm.get(..l).unwrap_or(&[]))
    };
    for e in path_elements(p) {
        match e.map_err(field_err)? {
            Element::Itself => {}
            Element::Member(m) => {
                out.put(b".")?;
                name(m, out)?;
            }
            Element::Value(k) => {
                out.put(b"[=")?;
                key_json(k, out)?;
                out.put(b"]")?;
            }
            Element::Keyed(b) => {
                out.put(b"[")?;
                for (i, (m, k)) in keyed_members(b).enumerate() {
                    if i > 0 {
                        out.put(b",")?;
                    }
                    name(m, out)?;
                    out.put(b"=")?;
                    key_json(k, out)?;
                }
                out.put(b"]")?;
            }
        }
    }
    Ok(())
}

/// An apply's refusal as the schema words it: `conflict` with `count`
/// (how many), `many` (`1` when more than one), `list` (`conflict.item`
/// per conflict, joined by an escaped newline) and `causes`
/// (`conflict.cause` per conflict, joined by `,`); each conflict's holes
/// are `manager`, `api` and `field`, escaped for a JSON string. At most
/// [`CONFLICTS_NAMED`] of the `total` are named. `scratch` holds the list
/// and causes.
pub fn conflict_json<'c>(
    schema: &[u8],
    conflicts: impl Iterator<Item = (&'c [u8], &'c [u8], &'c [u8])>,
    total: usize,
    scratch: &mut [u8],
    out: &mut Out<'_>,
) -> Result<(), RenderError> {
    let tpl = directive(schema, b"conflict").ok_or(RenderError::BadTemplate)?;
    let item = directive(schema, b"conflict.item").unwrap_or(&[]);
    let cause = directive(schema, b"conflict.cause").unwrap_or(&[]);
    let half = scratch.len() / 2;
    let (lb, cb) = scratch.split_at_mut(half);
    let (mut list, mut causes) = (Out::new(lb), Out::new(cb));
    for (i, (manager, version, field)) in conflicts.take(CONFLICTS_NAMED).enumerate() {
        let mut fb = [0u8; FIELD_TEXT];
        let mut fo = Out::new(&mut fb);
        field_text(field, &mut fo)?;
        fo.escape_from(0)?;
        let fl = fo.len();
        let (mut mb, mut vb) = ([0u8; HOLE_TEXT], [0u8; HOLE_TEXT]);
        let vars: [(&[u8], &[u8]); 3] = [
            (b"manager", escaped_into(manager, &mut mb)?),
            (b"api", escaped_into(version, &mut vb)?),
            (b"field", fb.get(..fl).unwrap_or(&[])),
        ];
        if i > 0 {
            list.put(b"\\n")?;
            causes.put(b",")?;
        }
        fill(item, &vars, &mut list)?;
        fill(cause, &vars, &mut causes)?;
    }
    let (ll, cl) = (list.len(), causes.len());
    let mut nb = [0u8; 20];
    let mut no = Out::new(&mut nb);
    no.decimal(total as u64)?;
    let nl = no.len();
    let vars: [(&[u8], &[u8]); 4] = [
        (b"count", nb.get(..nl).unwrap_or(&[])),
        (b"many", if total > 1 { b"1" } else { b"" }),
        (b"list", lb.get(..ll).unwrap_or(&[])),
        (b"causes", cb.get(..cl).unwrap_or(&[])),
    ];
    fill(tpl, &vars, out)
}
