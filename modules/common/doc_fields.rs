//! Field sets — which fields of a document a writer owns.
//!
//! Field ownership (a collection declaring `OWNERSHIP`) records, per
//! writer, the set of fields it set: an apply owns every field its
//! configuration names; an update owns the fields it changed. This file
//! is the canonical form of those sets and their algebra, as pure
//! functions over trees. A dialect renders a set in its own spelling
//! (Kubernetes' `fieldsV1`, through `spell_fields` and `read_fields` with its
//! marks, `doc_render.rs`); the executor never sees that spelling.
//!
//! Mounted by `doc_exec.rs` beside `doc_tree`, `doc_catalog` and
//! `doc_ops` (whose list rules identify list elements, exactly as the
//! keyed merge does).
//!
//! ## Paths
//!
//! A field is a path of elements, each an ordered, self-delimiting byte
//! run:
//!
//! | Element | Bytes |
//! |---|---|
//! | The list element itself | `0x01` |
//! | Member `m` | `0x02`, `m` escaped and terminated |
//! | A keyed list's element | `0x03 [n:u8]`, then per identifying member present, in the list key's order: the member escaped and terminated, its value's `value_key` |
//! | A set's element | `0x04`, its `value_key` |
//!
//! A path is its elements concatenated, so a field's descendants are
//! exactly the paths it prefixes, and paths sort as bytes. A set is
//! `[n:u32]{[len:u16][path]}…`, ascending, without duplicates.
//!
//! ## Which fields a document sets
//!
//! [`fields_of`] walks a document: a scalar, an empty object or an empty
//! array is a field; a non-empty object's fields are its members'; a
//! non-empty array follows its list rule — a keyed list's elements are
//! each a field (the element itself) and their members' fields, a set's
//! elements each a field, and any other array one field. A keyed list
//! whose elements cannot all be named (an element that is not an object,
//! or carries no key member, or a key that is not a scalar) is one field,
//! as is a set holding a container: nothing could name its parts.

use super::doc_catalog::{Descriptor, ListKey, EVERY_ELEMENT};
use super::doc_ops::{list_rule, Keyed, ListRule};
use super::doc_tree::{
    self, key_put_escaped, key_span, value_key, Path, Value, Writer, MAX_DEPTH, T_ARRAY, T_OBJECT,
};

pub const E_SELF: u8 = 0x01;
pub const E_MEMBER: u8 = 0x02;
pub const E_KEYED: u8 = 0x03;
pub const E_VALUE: u8 = 0x04;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldError {
    /// A buffer is full.
    Overflow,
    /// Nesting past `doc_tree::MAX_DEPTH`.
    TooDeep,
    /// A set, path or dialect spelling that is not well formed.
    Malformed,
}

// ── Paths ─────────────────────────────────────────────────────────────

/// A path being built, in a caller's buffer.
pub struct PathBuf<'a> {
    b: &'a mut [u8],
    n: usize,
}

impl<'a> PathBuf<'a> {
    pub fn new(b: &'a mut [u8]) -> Self {
        PathBuf { b, n: 0 }
    }

    pub fn get(&self) -> &[u8] {
        self.b.get(..self.n).unwrap_or(&[])
    }

    /// Where the path ends now: [`PathBuf::truncate`] returns to it.
    pub fn mark(&self) -> usize {
        self.n
    }

    pub fn truncate(&mut self, mark: usize) {
        self.n = mark.min(self.n);
    }

    fn put(&mut self, bytes: &[u8]) -> Result<(), FieldError> {
        let end = self.n + bytes.len();
        let d = self.b.get_mut(self.n..end).ok_or(FieldError::Overflow)?;
        for (x, y) in d.iter_mut().zip(bytes) {
            *x = *y;
        }
        self.n = end;
        Ok(())
    }

    fn escaped(&mut self, s: &[u8]) -> Result<(), FieldError> {
        let mut at = self.n;
        key_put_escaped(self.b, &mut at, s).ok_or(FieldError::Overflow)?;
        self.n = at;
        Ok(())
    }

    fn key(&mut self, v: Value<'_>) -> Result<(), FieldError> {
        let room = self.b.get_mut(self.n..).ok_or(FieldError::Overflow)?;
        let k = value_key(v, room).ok_or(FieldError::Overflow)?;
        self.n += k;
        Ok(())
    }

    pub fn push_self(&mut self) -> Result<(), FieldError> {
        self.put(&[E_SELF])
    }

    pub fn push_member(&mut self, m: &[u8]) -> Result<(), FieldError> {
        self.put(&[E_MEMBER])?;
        self.escaped(m)
    }

    /// The element of a keyed list that carries `el`'s key; `false` (and
    /// nothing pushed) when `el` cannot be named by it.
    pub fn push_keyed(&mut self, lk: &ListKey<'_>, el: Value<'_>) -> Result<bool, FieldError> {
        if !nameable(lk, el) {
            return Ok(false);
        }
        let mark = self.n;
        let present = lk.keys().filter(|k| el.member(k).is_some()).count();
        self.put(&[E_KEYED, present as u8])?;
        for k in lk.keys() {
            if let Some(v) = el.member(k) {
                self.escaped(k)?;
                if let Err(e) = self.key(v) {
                    self.n = mark;
                    return Err(e);
                }
            }
        }
        Ok(true)
    }

    /// The element of a set equal to `v` (a scalar).
    pub fn push_value(&mut self, v: Value<'_>) -> Result<(), FieldError> {
        self.put(&[E_VALUE])?;
        self.key(v)
    }
}

/// Can `el` be named by the list key: an object carrying at least one
/// key member, every key member present a scalar.
fn nameable(lk: &ListKey<'_>, el: Value<'_>) -> bool {
    el.ty() == T_OBJECT
        && lk.keys().any(|k| el.member(k).is_some())
        && lk
            .keys()
            .all(|k| el.member(k).is_none_or(|v| !v.is_container()))
}

/// One element of an encoded path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Element<'a> {
    /// The list element itself.
    Itself,
    /// A member, escaped and terminated (decode with `key_unescape`).
    Member(&'a [u8]),
    /// A keyed element: `[n]` then n × (escaped member, value key).
    Keyed(&'a [u8]),
    /// A set element: its value key.
    Value(&'a [u8]),
}

/// The elements of an encoded path.
pub fn elements(p: &[u8]) -> impl Iterator<Item = Result<Element<'_>, FieldError>> + '_ {
    let mut at = 0usize;
    core::iter::from_fn(move || {
        let rest = p.get(at..)?;
        let (&tag, body) = rest.split_first()?;
        let r = (|| -> Option<(Element<'_>, usize)> {
            match tag {
                E_SELF => Some((Element::Itself, 1)),
                E_MEMBER => {
                    let n = escaped_len(body)?;
                    Some((Element::Member(body.get(..n)?), 1 + n))
                }
                E_KEYED => {
                    let count = *body.first()? as usize;
                    let mut i = 1usize;
                    for _ in 0..count {
                        i += escaped_len(body.get(i..)?)?;
                        i += key_span(body.get(i..)?)?;
                    }
                    Some((Element::Keyed(body.get(..i)?), 1 + i))
                }
                E_VALUE => {
                    let n = key_span(body)?;
                    Some((Element::Value(body.get(..n)?), 1 + n))
                }
                _ => None,
            }
        })();
        match r {
            Some((e, n)) => {
                at += n;
                Some(Ok(e))
            }
            None => {
                at = p.len();
                Some(Err(FieldError::Malformed))
            }
        }
    })
}

/// The length of an escaped, terminated run.
fn escaped_len(b: &[u8]) -> Option<usize> {
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

/// The key members of a keyed element: `(escaped member, value key)`.
pub fn keyed_members(b: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
    let count = b.first().copied().unwrap_or(0) as usize;
    let mut at = 1usize;
    let mut left = count;
    core::iter::from_fn(move || {
        if left == 0 {
            return None;
        }
        left -= 1;
        let m = escaped_len(b.get(at..)?)?;
        let name = b.get(at..at + m)?;
        at += m;
        let k = key_span(b.get(at..)?)?;
        let key = b.get(at..at + k)?;
        at += k;
        Some((name, key))
    })
}

/// Does `escaped` (a terminated run) spell `name`?
fn escaped_is(escaped: &[u8], name: &[u8]) -> bool {
    let mut i = 0usize;
    for &c in name {
        if c == 0 {
            if escaped.get(i..i + 2) != Some(&[0x00, 0xFF]) {
                return false;
            }
            i += 2;
        } else {
            if escaped.get(i) != Some(&c) {
                return false;
            }
            i += 1;
        }
    }
    escaped.get(i..) == Some(&[0x00, 0x01])
}

/// Does `v`'s value key equal `key`? Strings, identifiers and binary
/// are compared against the key's bytes where they stand; every other
/// scalar's key is short.
fn key_is(v: Value<'_>, key: &[u8]) -> bool {
    if v.is_container() || key.first() != Some(&doc_tree::type_rank(v.ty())) {
        return false;
    }
    let body = key.get(1..).unwrap_or(&[]);
    if let Some(s) = v.as_str().or_else(|| v.as_identifier()) {
        return escaped_is(body, s);
    }
    if let Some((st, d)) = v.as_binary() {
        return body.get(..4) == Some(&(d.len() as u32).to_be_bytes()[..])
            && body.get(4) == Some(&st)
            && body.get(5..) == Some(d);
    }
    // Null, a number (at most 80 bytes), a boolean or a timestamp.
    let mut k = [0u8; 96];
    value_key(v, &mut k).and_then(|n| k.get(..n)) == Some(key)
}

/// The element of array `arr` a keyed or set element names.
fn find_element<'a>(arr: Value<'a>, e: Element<'_>) -> Option<Value<'a>> {
    if arr.ty() != T_ARRAY {
        return None;
    }
    match e {
        Element::Keyed(b) => arr.elements().find(|el| {
            el.ty() == T_OBJECT && {
                let named = keyed_members(b).count();
                // Every named member matches; the element carries no
                // other present key member (it would be another element).
                keyed_members(b).all(|(m, k)| {
                    el.members()
                        .find(|(n, _)| escaped_is(m, n))
                        .is_some_and(|(_, v)| key_is(v, k))
                }) && named == b.first().copied().unwrap_or(0) as usize
            }
        }),
        Element::Value(k) => arr.elements().find(|el| key_is(*el, k)),
        _ => None,
    }
}

/// The value path `p` names in `doc` (the element itself for a path
/// ending in the element mark).
pub fn resolve<'a>(doc: Value<'a>, p: &[u8]) -> Option<Value<'a>> {
    let mut v = doc;
    for e in elements(p) {
        v = match e.ok()? {
            Element::Itself => v,
            Element::Member(m) => v.members().find(|(n, _)| escaped_is(m, n))?.1,
            e => find_element(v, e)?,
        };
    }
    Some(v)
}

/// Resolves many paths in one document, cheaply when they arrive in
/// order: it remembers where each depth's last member was found and
/// resumes there (wrapping to the object's start), so paths in sorted
/// order through members in that order — a Kubernetes client serialises
/// map keys sorted — each take a step instead of a scan. Any order is
/// still resolved exactly as [`resolve`] does.
pub struct Resolver<'a> {
    doc: Value<'a>,
    /// Per depth: the object last searched (its bytes' address) and the
    /// offset of the member found in it.
    hint: [(usize, usize); MAX_DEPTH],
}

impl<'a> Resolver<'a> {
    pub fn new(doc: Value<'a>) -> Self {
        Resolver {
            doc,
            hint: [(0, 0); MAX_DEPTH],
        }
    }

    /// The value path `p` names ([`resolve`]).
    pub fn resolve(&mut self, p: &[u8]) -> Option<Value<'a>> {
        let mut v = self.doc;
        for (depth, e) in elements(p).enumerate() {
            v = match e.ok()? {
                Element::Itself => v,
                Element::Member(m) => self.member(depth, v, m)?,
                e => find_element(v, e)?,
            };
        }
        Some(v)
    }

    fn member(&mut self, depth: usize, obj: Value<'a>, m: &[u8]) -> Option<Value<'a>> {
        let id = obj.bytes().as_ptr() as usize;
        let start = match self.hint.get(depth) {
            Some(&(h, at)) if h == id => at,
            _ => 0,
        };
        // From the hint to the end, then from the start up to the hint.
        let mut it = obj.members_from(start);
        loop {
            let at = it.offset();
            match it.next() {
                Some((n, val)) if escaped_is(m, n) => {
                    if let Some(h) = self.hint.get_mut(depth) {
                        *h = (id, at);
                    }
                    return Some(val);
                }
                Some(_) => {}
                None => break,
            }
        }
        let mut it = obj.members();
        while it.offset() < start {
            let at = it.offset();
            let (n, val) = it.next()?;
            if escaped_is(m, n) {
                if let Some(h) = self.hint.get_mut(depth) {
                    *h = (id, at);
                }
                return Some(val);
            }
        }
        None
    }
}

/// Does path `p` name a list element's presence (the element itself, or
/// a set's element) rather than a value?
pub fn names_presence(p: &[u8]) -> bool {
    let mut last = None;
    for e in elements(p) {
        match e {
            Ok(e) => last = Some(e),
            Err(_) => return false,
        }
    }
    matches!(last, Some(Element::Itself) | Some(Element::Value(_)))
}

// ── Sets ──────────────────────────────────────────────────────────────

/// A set of paths, over its encoding.
#[derive(Clone, Copy, Debug)]
pub struct FieldSet<'a> {
    b: &'a [u8],
}

/// The empty set's encoding.
pub const EMPTY_SET: [u8; 4] = [0, 0, 0, 0];

impl<'a> FieldSet<'a> {
    /// Validate a set: well-formed paths, ascending, no duplicates.
    pub fn parse(b: &'a [u8]) -> Result<Self, FieldError> {
        let s = FieldSet { b };
        let n = u32::from_le_bytes(
            b.get(..4)
                .ok_or(FieldError::Malformed)?
                .try_into()
                .map_err(|_| FieldError::Malformed)?,
        ) as usize;
        let mut prev: Option<&[u8]> = None;
        let mut count = 0usize;
        let mut at = 4usize;
        while at < b.len() {
            let (p, next) = record(b, at).ok_or(FieldError::Malformed)?;
            if p.is_empty() || prev.is_some_and(|q| q >= p) {
                return Err(FieldError::Malformed);
            }
            for e in elements(p) {
                e?;
            }
            prev = Some(p);
            count += 1;
            at = next;
        }
        if count != n {
            return Err(FieldError::Malformed);
        }
        Ok(s)
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.b
    }

    pub fn len(&self) -> usize {
        self.paths().count()
    }

    pub fn is_empty(&self) -> bool {
        self.paths().next().is_none()
    }

    pub fn paths(&self) -> impl Iterator<Item = &'a [u8]> + 'a {
        let b = self.b;
        let mut at = 4usize;
        core::iter::from_fn(move || {
            let (p, next) = record(b, at)?;
            at = next;
            Some(p)
        })
    }

    pub fn contains(&self, p: &[u8]) -> bool {
        self.paths().any(|q| q == p)
    }

    /// Does the set hold `p` or a path below it?
    pub fn holds_at_or_below(&self, p: &[u8]) -> bool {
        self.paths().any(|q| q.starts_with(p))
    }

    /// A membership test for paths asked in ascending order: each
    /// [`Probe::has`] moves on from where the last one stopped, so a
    /// sorted run of questions reads the set once.
    pub fn probe(&self) -> Probe<'a> {
        Probe {
            set: *self,
            at: 4,
            before: 0,
        }
    }
}

/// [`FieldSet::probe`].
pub struct Probe<'a> {
    set: FieldSet<'a>,
    /// The first record not below the last question.
    at: usize,
    /// The record just before `at` (0: none) — how a question below the
    /// last one is recognised.
    before: usize,
}

impl Probe<'_> {
    /// Does the set hold `p`? Asked in ascending order it reads the set
    /// once; a question below the last one starts over (still exact).
    pub fn has(&mut self, p: &[u8]) -> bool {
        let b = self.set.bytes();
        if self.before != 0 && record(b, self.before).is_none_or(|(q, _)| q >= p) {
            self.at = 4;
            self.before = 0;
        }
        while let Some((q, next)) = record(b, self.at) {
            if q >= p {
                return q == p;
            }
            self.before = self.at;
            self.at = next;
        }
        false
    }
}

/// The paths of two sets, merged in ascending order, once each.
fn merged<'x>(a: FieldSet<'x>, b: FieldSet<'x>) -> impl Iterator<Item = &'x [u8]> + 'x {
    let mut ia = a.paths().peekable();
    let mut ib = b.paths().peekable();
    core::iter::from_fn(move || match (ia.peek().copied(), ib.peek().copied()) {
        (Some(x), Some(y)) if x == y => {
            ia.next();
            ib.next()
        }
        (Some(x), Some(y)) if x < y => ia.next(),
        (Some(_), Some(_)) => ib.next(),
        (Some(_), None) => ia.next(),
        (None, _) => ib.next(),
    })
}

fn record(b: &[u8], at: usize) -> Option<(&[u8], usize)> {
    let l = u16::from_le_bytes([*b.get(at)?, *b.get(at + 1)?]) as usize;
    Some((b.get(at + 2..at + 2 + l)?, at + 2 + l))
}

/// Builds a set in a caller's buffer: paths inserted in any order land
/// sorted, once.
pub struct SetBuilder<'a> {
    b: &'a mut [u8],
    n: usize,
    count: u32,
    /// Where the last (greatest) record starts: an insert above it — the
    /// common, sorted case — appends without a walk.
    last: usize,
}

impl<'a> SetBuilder<'a> {
    pub fn new(b: &'a mut [u8]) -> Result<Self, FieldError> {
        let h = b.get_mut(..4).ok_or(FieldError::Overflow)?;
        for x in h.iter_mut() {
            *x = 0;
        }
        Ok(SetBuilder {
            b,
            n: 4,
            count: 0,
            last: 0,
        })
    }

    /// Insert `p` in order; a path already present is not repeated.
    pub fn insert(&mut self, p: &[u8]) -> Result<(), FieldError> {
        let len = u16::try_from(p.len()).map_err(|_| FieldError::Overflow)?;
        // Where it goes: at the end when it is above the greatest (paths
        // mostly arrive sorted), else before the first record above it.
        let mut at = 4usize;
        if self.count > 0 {
            let (q, _) = record(self.b, self.last).ok_or(FieldError::Malformed)?;
            if q == p {
                return Ok(());
            }
            if q < p {
                at = self.n;
            }
        }
        while at < self.n {
            let (q, next) = record(self.b, at).ok_or(FieldError::Malformed)?;
            if q == p {
                return Ok(());
            }
            if q > p {
                break;
            }
            at = next;
        }
        let grow = 2 + p.len();
        let end = self.n + grow;
        if end > self.b.len() {
            return Err(FieldError::Overflow);
        }
        // Open the gap, back to front.
        let mut i = self.n;
        while i > at {
            i -= 1;
            self.b[i + grow] = self.b[i];
        }
        let d = &mut self.b[at..at + grow];
        for (x, y) in d.iter_mut().zip(len.to_le_bytes().iter().chain(p)) {
            *x = *y;
        }
        // The greatest record: this one when it went to the end, else the
        // same one, moved along by the gap.
        if at + grow == end {
            self.last = at;
        } else if at <= self.last {
            self.last += grow;
        }
        self.n = end;
        self.count += 1;
        Ok(())
    }

    /// Every path of `s`.
    pub fn extend(&mut self, s: FieldSet<'_>) -> Result<(), FieldError> {
        for p in s.paths() {
            self.insert(p)?;
        }
        Ok(())
    }

    pub fn finish(self) -> usize {
        for (x, y) in self.b.iter_mut().zip(self.count.to_le_bytes()) {
            *x = y;
        }
        self.n
    }
}

// ── Which fields a document sets ──────────────────────────────────────

/// Where a walk stands: the member names from the root
/// ([`EVERY_ELEMENT`] for an element), as list rules match them.
pub struct Loc<'a> {
    segs: [&'a [u8]; 2 * MAX_DEPTH],
    n: usize,
}

impl<'a> Default for Loc<'a> {
    fn default() -> Self {
        Loc {
            segs: [&[]; 2 * MAX_DEPTH],
            n: 0,
        }
    }
}

impl<'a> Loc<'a> {
    pub fn push(&mut self, s: &'a [u8]) -> Result<(), FieldError> {
        *self.segs.get_mut(self.n).ok_or(FieldError::TooDeep)? = s;
        self.n += 1;
        Ok(())
    }
    pub fn pop(&mut self) {
        self.n = self.n.saturating_sub(1);
    }
    pub fn get(&self) -> &[&'a [u8]] {
        self.segs.get(..self.n).unwrap_or(&[])
    }
}

/// The fields no writer owns: listed, or as a collection declares them.
#[derive(Clone, Copy)]
pub enum Unowned<'a> {
    Paths(&'a [Path<'a>]),
    Declared(Descriptor<'a>),
}

impl Unowned<'_> {
    /// Is `loc` (member names only) one of them?
    fn holds(&self, loc: &[&[u8]]) -> bool {
        let is = |u: Path<'_>| u.len() == loc.len() && u.segments().zip(loc).all(|(a, b)| a == *b);
        match self {
            Unowned::Paths(ps) => ps.iter().any(|u| is(*u)),
            Unowned::Declared(d) => d.unowned_paths().any(is),
        }
    }
}

/// Every field `doc` sets into `out`, except the `unowned` paths (member
/// paths from the root: bookkeeping no writer owns).
pub fn fields_of(
    doc: Value<'_>,
    keyed: &Keyed<'_>,
    unowned: Unowned<'_>,
    path: &mut PathBuf<'_>,
    out: &mut SetBuilder<'_>,
) -> Result<(), FieldError> {
    if doc.ty() != T_OBJECT {
        return Err(FieldError::Malformed);
    }
    let mut loc = Loc {
        segs: [&[]; 2 * MAX_DEPTH],
        n: 0,
    };
    walk_object(doc, keyed, unowned, &mut loc, path, out)
}

fn walk_object<'a>(
    obj: Value<'a>,
    keyed: &Keyed<'_>,
    unowned: Unowned<'_>,
    loc: &mut Loc<'a>,
    path: &mut PathBuf<'_>,
    out: &mut SetBuilder<'_>,
) -> Result<(), FieldError> {
    for (m, v) in obj.members() {
        loc.push(m)?;
        let mark = path.mark();
        path.push_member(m)?;
        if !unowned.holds(loc.get()) {
            walk_value(v, keyed, unowned, loc, path, out)?;
        }
        path.truncate(mark);
        loc.pop();
    }
    Ok(())
}

fn walk_value<'a>(
    v: Value<'a>,
    keyed: &Keyed<'_>,
    unowned: Unowned<'_>,
    loc: &mut Loc<'a>,
    path: &mut PathBuf<'_>,
    out: &mut SetBuilder<'_>,
) -> Result<(), FieldError> {
    match v.ty() {
        T_OBJECT if v.count() > 0 => walk_object(v, keyed, unowned, loc, path, out),
        T_ARRAY if v.count() > 0 => walk_list(v, keyed, unowned, loc, path, out),
        _ => out.insert(path.get()),
    }
}

fn walk_list<'a>(
    arr: Value<'a>,
    keyed: &Keyed<'_>,
    unowned: Unowned<'_>,
    loc: &mut Loc<'a>,
    path: &mut PathBuf<'_>,
    out: &mut SetBuilder<'_>,
) -> Result<(), FieldError> {
    match list_rule(keyed, loc.get()) {
        ListRule::Keyed(lk) if arr.elements().all(|el| nameable(&lk, el)) => {
            loc.push(EVERY_ELEMENT)?;
            for el in arr.elements() {
                let mark = path.mark();
                path.push_keyed(&lk, el)?;
                let at_el = path.mark();
                path.push_self()?;
                out.insert(path.get())?;
                path.truncate(at_el);
                walk_object(el, keyed, unowned, loc, path, out)?;
                path.truncate(mark);
            }
            loc.pop();
            Ok(())
        }
        ListRule::Set if arr.elements().all(|el| !el.is_container()) => {
            for el in arr.elements() {
                let mark = path.mark();
                path.push_value(el)?;
                out.insert(path.get())?;
                path.truncate(mark);
            }
            Ok(())
        }
        _ => out.insert(path.get()),
    }
}

// ── Removing a field ──────────────────────────────────────────────────

/// `doc` without the field at `p` into `w`: a member removed from its
/// object, a keyed or set element (a path ending in the element or its
/// mark) from its list. A path that names nothing changes nothing.
pub fn without(doc: Value<'_>, p: &[u8], w: &mut Writer<'_>) -> Result<(), FieldError> {
    // The path's elements, the trailing element mark folded into the
    // element it marks.
    let mut els: [Element<'_>; 2 * MAX_DEPTH] = [Element::Itself; 2 * MAX_DEPTH];
    let mut n = 0usize;
    for e in elements(p) {
        let e = e?;
        if e == Element::Itself {
            continue;
        }
        *els.get_mut(n).ok_or(FieldError::TooDeep)? = e;
        n += 1;
    }
    rebuild(doc, els.get(..n).unwrap_or(&[]), w)
}

fn rebuild(v: Value<'_>, els: &[Element<'_>], w: &mut Writer<'_>) -> Result<(), FieldError> {
    let Some((&e, rest)) = els.split_first() else {
        return w.value(v).map_err(tree);
    };
    match (e, v.ty()) {
        (Element::Member(m), T_OBJECT) => {
            w.begin_object().map_err(tree)?;
            for (k, mv) in v.members() {
                let hit = escaped_is(m, k);
                if hit && rest.is_empty() {
                    continue;
                }
                w.key(k).map_err(tree)?;
                if hit {
                    rebuild(mv, rest, w)?;
                } else {
                    w.value(mv).map_err(tree)?;
                }
            }
            w.end().map_err(tree)
        }
        (Element::Keyed(_) | Element::Value(_), T_ARRAY) => {
            let target = find_element(v, e);
            w.begin_array().map_err(tree)?;
            for el in v.elements() {
                let hit = target.is_some_and(|t| t.bytes().as_ptr() == el.bytes().as_ptr());
                if hit && rest.is_empty() {
                    continue;
                }
                if hit {
                    rebuild(el, rest, w)?;
                } else {
                    w.value(el).map_err(tree)?;
                }
            }
            w.end().map_err(tree)
        }
        // The path runs through something that is not what it names:
        // nothing to remove.
        _ => w.value(v).map_err(tree),
    }
}

pub fn tree(e: doc_tree::TreeError) -> FieldError {
    match e {
        doc_tree::TreeError::Overflow => FieldError::Overflow,
        doc_tree::TreeError::TooDeep => FieldError::TooDeep,
        _ => FieldError::Malformed,
    }
}

// ── Ownership ─────────────────────────────────────────────────────────

/// The fields an update changed: every field of `new` whose value (or,
/// for a list element, whose presence) `old` does not already have.
pub fn changed(
    old: Option<Value<'_>>,
    new: Value<'_>,
    keyed: &Keyed<'_>,
    unowned: Unowned<'_>,
    path: &mut PathBuf<'_>,
    all: &mut [u8],
    out: &mut SetBuilder<'_>,
) -> Result<(), FieldError> {
    let mut b = SetBuilder::new(all)?;
    fields_of(new, keyed, unowned, path, &mut b)?;
    let n = b.finish();
    let set = FieldSet::parse(all.get(..n).unwrap_or(&[]))?;
    // The set is sorted: each document is read once along it.
    let mut in_old = old.map(Resolver::new);
    let mut in_new = Resolver::new(new);
    for p in set.paths() {
        let same = in_old
            .as_mut()
            .is_some_and(|o| match (o.resolve(p), in_new.resolve(p)) {
                (Some(_), Some(_)) if names_presence(p) => true,
                (Some(x), Some(y)) => doc_tree::equal(x, y),
                _ => false,
            });
        if !same {
            out.insert(p)?;
        }
    }
    Ok(())
}

/// A field an apply would take from another owner: one both hold that
/// names a value (not a list element's presence) the apply sets
/// differently from the stored document.
pub fn conflicts<'s: 'x, 'x>(
    applied: FieldSet<'x>,
    other: FieldSet<'s>,
    config: Value<'x>,
    stored: Value<'x>,
) -> impl Iterator<Item = &'s [u8]> + 'x {
    other.paths().filter(move |p| {
        !names_presence(p)
            && applied.contains(p)
            && match (resolve(config, p), resolve(stored, p)) {
                (Some(x), Some(y)) => !doc_tree::equal(x, y),
                _ => false,
            }
    })
}

/// The fields an apply removes: those the manager applied before and no
/// longer names, when neither its new configuration nor any other owner
/// holds them or anything below them — except a keyed element's key
/// members, which go only with their element. `others` is every other
/// owner's fields together.
pub fn removals<'s: 'x, 'x>(
    before: FieldSet<'s>,
    now: FieldSet<'x>,
    others: FieldSet<'x>,
) -> impl Iterator<Item = &'s [u8]> + 'x {
    before.paths().filter(move |p| {
        let base = base_of(p);
        !is_key_member(p) && !now.holds_at_or_below(base) && !others.holds_at_or_below(base)
    })
}

/// Does `p` name a keyed element's key member? It identifies the element,
/// so it goes only with the element (the element's own mark), never by
/// itself from an element that stays.
fn is_key_member(p: &[u8]) -> bool {
    let mut prev = None;
    let mut last = None;
    for e in elements(p) {
        let Ok(e) = e else {
            return false;
        };
        prev = last;
        last = Some(e);
    }
    match (prev, last) {
        (Some(Element::Keyed(k)), Some(Element::Member(m))) => {
            keyed_members(k).any(|(name, _)| name == m)
        }
        _ => false,
    }
}

/// A path without its trailing element mark: the element it marks.
fn base_of(p: &[u8]) -> &[u8] {
    match p.split_last() {
        Some((&E_SELF, rest)) => rest,
        _ => p,
    }
}

// ── The ownership record ──────────────────────────────────────────────
//
// What a document's record carries after its tree (`FORMAT_OWNED`): one
// entry per writer and operation —
//
// `[OWNERS_V1][n:u8]{[mlen:u8][manager][op:u8][secs:i64][nanos:u32]
//  [vlen:u8][version][slen:u8][scope][setlen:u32][set]}…`
//
// `op` is `OWN_APPLY` or `OWN_UPDATE`; the time is the entry's last write;
// `version` is the text at the collection's version path when it wrote
// (Kubernetes' `apiVersion`); `scope` the request's scope label (a
// Kubernetes subresource). An entry owns at least one field.

pub const OWNERS_V1: u8 = 1;
pub const OWN_APPLY: u8 = 1;
pub const OWN_UPDATE: u8 = 2;

/// One writer's entry.
#[derive(Clone, Copy, Debug)]
pub struct Entry<'a> {
    pub manager: &'a [u8],
    pub op: u8,
    pub secs: i64,
    pub nanos: u32,
    pub version: &'a [u8],
    pub scope: &'a [u8],
    pub set: FieldSet<'a>,
}

/// A document's ownership record.
#[derive(Clone, Copy, Debug)]
pub struct Owners<'a> {
    b: &'a [u8],
}

/// The record with no entries.
pub const NO_OWNERS: [u8; 2] = [OWNERS_V1, 0];

impl<'a> Owners<'a> {
    pub fn parse(b: &'a [u8]) -> Result<Self, FieldError> {
        if b.first() != Some(&OWNERS_V1) {
            return Err(FieldError::Malformed);
        }
        let o = Owners { b };
        let n = *b.get(1).ok_or(FieldError::Malformed)? as usize;
        let mut at = 2usize;
        for _ in 0..n {
            let (e, next) = entry_at(b, at).ok_or(FieldError::Malformed)?;
            FieldSet::parse(e.set.b)?;
            if !matches!(e.op, OWN_APPLY | OWN_UPDATE) || e.manager.is_empty() {
                return Err(FieldError::Malformed);
            }
            at = next;
        }
        if at != b.len() {
            return Err(FieldError::Malformed);
        }
        Ok(o)
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.b
    }

    pub fn entries(&self) -> impl Iterator<Item = Entry<'a>> + 'a {
        let b = self.b;
        let n = b.get(1).copied().unwrap_or(0) as usize;
        let mut at = 2usize;
        let mut left = n;
        core::iter::from_fn(move || {
            if left == 0 {
                return None;
            }
            left -= 1;
            let (e, next) = entry_at(b, at)?;
            at = next;
            Some(e)
        })
    }
}

fn entry_at(b: &[u8], at: usize) -> Option<(Entry<'_>, usize)> {
    let ml = *b.get(at)? as usize;
    let manager = b.get(at + 1..at + 1 + ml)?;
    let mut i = at + 1 + ml;
    let op = *b.get(i)?;
    let secs = i64::from_le_bytes(b.get(i + 1..i + 9)?.try_into().ok()?);
    let nanos = u32::from_le_bytes(b.get(i + 9..i + 13)?.try_into().ok()?);
    i += 13;
    let vl = *b.get(i)? as usize;
    let version = b.get(i + 1..i + 1 + vl)?;
    i += 1 + vl;
    let sl = *b.get(i)? as usize;
    let scope = b.get(i + 1..i + 1 + sl)?;
    i += 1 + sl;
    let setlen = u32::from_le_bytes(b.get(i..i + 4)?.try_into().ok()?) as usize;
    let set = FieldSet {
        b: b.get(i + 4..i + 4 + setlen)?,
    };
    Some((
        Entry {
            manager,
            op,
            secs,
            nanos,
            version,
            scope,
            set,
        },
        i + 4 + setlen,
    ))
}

/// Builds an ownership record.
pub struct OwnersWriter<'a> {
    b: &'a mut [u8],
    n: usize,
    count: u8,
}

impl<'a> OwnersWriter<'a> {
    pub fn new(b: &'a mut [u8]) -> Result<Self, FieldError> {
        let h = b.get_mut(..2).ok_or(FieldError::Overflow)?;
        h[0] = OWNERS_V1;
        h[1] = 0;
        Ok(OwnersWriter { b, n: 2, count: 0 })
    }

    fn put(&mut self, bytes: &[u8]) -> Result<(), FieldError> {
        let end = self.n + bytes.len();
        let d = self.b.get_mut(self.n..end).ok_or(FieldError::Overflow)?;
        for (x, y) in d.iter_mut().zip(bytes) {
            *x = *y;
        }
        self.n = end;
        Ok(())
    }

    /// An entry whose set is written by `fill` into the room left: an
    /// entry left owning nothing is not kept.
    pub fn entry(
        &mut self,
        head: &Entry<'_>,
        fill: impl FnOnce(&mut SetBuilder<'_>) -> Result<(), FieldError>,
    ) -> Result<(), FieldError> {
        let mark = self.n;
        let short = |s: &[u8]| u8::try_from(s.len()).map_err(|_| FieldError::Overflow);
        self.put(&[short(head.manager)?])?;
        self.put(head.manager)?;
        self.put(&[head.op])?;
        self.put(&head.secs.to_le_bytes())?;
        self.put(&head.nanos.to_le_bytes())?;
        self.put(&[short(head.version)?])?;
        self.put(head.version)?;
        self.put(&[short(head.scope)?])?;
        self.put(head.scope)?;
        let len_at = self.n;
        self.put(&[0; 4])?;
        let room = self.b.get_mut(self.n..).ok_or(FieldError::Overflow)?;
        let mut sb = SetBuilder::new(room)?;
        fill(&mut sb)?;
        let empty = sb.count == 0;
        let sl = sb.finish();
        if empty {
            self.n = mark;
            return Ok(());
        }
        let l = u32::try_from(sl).map_err(|_| FieldError::Overflow)?;
        for (x, y) in self.b[len_at..len_at + 4].iter_mut().zip(l.to_le_bytes()) {
            *x = y;
        }
        self.n += sl;
        self.count = self.count.checked_add(1).ok_or(FieldError::Overflow)?;
        Ok(())
    }

    pub fn finish(self) -> usize {
        self.b[1] = self.count;
        self.n
    }
}

/// Who writes, and when.
#[derive(Clone, Copy)]
pub struct Actor<'a> {
    pub manager: &'a [u8],
    /// The request's scope label (empty: the whole document).
    pub scope: &'a [u8],
    pub secs: i64,
    pub nanos: u32,
}

/// What an ownership transition reads the document by: the list rules,
/// the unowned paths and the version path.
#[derive(Clone, Copy)]
pub struct Schema<'a, 'p> {
    pub keyed: &'a Keyed<'a>,
    pub unowned: Unowned<'p>,
    pub version: Option<Path<'a>>,
}

/// Room a transition works in: three set-sized buffers and a path.
pub struct Scratch<'a> {
    pub a: &'a mut [u8],
    pub b: &'a mut [u8],
    pub c: &'a mut [u8],
    pub path: &'a mut [u8],
}

/// The version text an entry records: the string at the version path.
fn version_of<'d>(doc: Value<'d>, s: &Schema<'_, '_>) -> &'d [u8] {
    s.version
        .and_then(|p| doc_tree::lookup(doc, p))
        .and_then(|v| v.as_str())
        .filter(|v| v.len() <= u8::MAX as usize)
        .unwrap_or(&[])
}

/// The ownership after an update of `old` (absent: a create) into `new`
/// by `who`: the fields it changed become its (the `OWN_UPDATE` entry of
/// its manager and scope), taken from whoever held them; every entry
/// loses the fields `new` no longer has; an entry owning nothing goes.
/// Into `out`: its length.
pub fn record_update(
    owners: Owners<'_>,
    old: Option<Value<'_>>,
    new: Value<'_>,
    who: &Actor<'_>,
    s: &Schema<'_, '_>,
    sc: &mut Scratch<'_>,
    out: &mut [u8],
) -> Result<usize, FieldError> {
    // The changed fields, into `b` (`a` is changed()'s working room).
    let cl = {
        let mut cb = SetBuilder::new(sc.b)?;
        changed(
            old,
            new,
            s.keyed,
            s.unowned,
            &mut PathBuf::new(sc.path),
            sc.a,
            &mut cb,
        )?;
        cb.finish()
    };
    let taken = FieldSet::parse(sc.b.get(..cl).unwrap_or(&[]))?;
    let version = version_of(new, s);
    let mine =
        |e: &Entry<'_>| e.op == OWN_UPDATE && e.manager == who.manager && e.scope == who.scope;
    let mut w = OwnersWriter::new(out)?;
    let mut had = false;
    // Every walk below is in sorted order: the sets merge and probe in
    // one pass each, and the document resolves along them.
    for e in owners.entries() {
        let mut in_new = Resolver::new(new);
        if mine(&e) {
            had = true;
            let head = Entry {
                secs: who.secs,
                nanos: who.nanos,
                version,
                ..e
            };
            w.entry(&head, |b| {
                for p in merged(e.set, taken) {
                    if in_new.resolve(p).is_some() {
                        b.insert(p)?;
                    }
                }
                Ok(())
            })?;
        } else {
            let mut held = taken.probe();
            w.entry(&e, |b| {
                for p in e.set.paths() {
                    if !held.has(p) && in_new.resolve(p).is_some() {
                        b.insert(p)?;
                    }
                }
                Ok(())
            })?;
        }
    }
    if !had {
        let head = Entry {
            manager: who.manager,
            op: OWN_UPDATE,
            secs: who.secs,
            nanos: who.nanos,
            version,
            scope: who.scope,
            set: FieldSet { b: &EMPTY_SET },
        };
        w.entry(&head, |b| b.extend(taken))?;
    }
    Ok(w.finish())
}

/// One conflict an apply met: the field, and the owner holding it.
#[derive(Clone, Copy, Debug)]
pub struct Conflict<'a> {
    pub manager: &'a [u8],
    pub op: u8,
    pub version: &'a [u8],
    pub path: &'a [u8],
}

/// What an apply comes to.
#[derive(Debug, PartialEq, Eq)]
pub enum Applied {
    /// The document (`doc_out`) and its ownership (`out`): their lengths.
    Done { doc: usize, owners: usize },
    /// Fields other owners set differently: how many (the first ones
    /// reported through `on_conflict`).
    Conflicts(usize),
}

/// The ownership after `who` applies `config` to `stored` (absent: a
/// create), `merged` being the configuration merged over it:
///
/// - a field `config` sets that another owner holds with a different
///   value is a conflict — refused, unless `force`, which takes it;
/// - a field `who` applied before and `config` no longer names is
///   removed from the document, unless another owner holds it (or a
///   field below it);
/// - `who`'s `OWN_APPLY` entry then owns exactly `config`'s fields.
///
/// The document with the removals goes to `doc_out`, the ownership to
/// `out`. Conflicts are handed to `on_conflict` as found.
#[allow(
    clippy::too_many_arguments,
    reason = "an apply's inputs (the owners, stored, config and merged documents, who, force) and its two outputs"
)]
pub fn record_apply<'o>(
    owners: Owners<'o>,
    stored: Option<Value<'_>>,
    config: Value<'_>,
    merged: Value<'_>,
    who: &Actor<'_>,
    force: bool,
    s: &Schema<'_, '_>,
    sc: &mut Scratch<'_>,
    doc_out: (&mut [u8], &mut [u8]),
    out: &mut [u8],
    mut on_conflict: impl FnMut(Conflict<'o>),
) -> Result<Applied, FieldError> {
    // The configuration's fields: `a`.
    let al = {
        let mut ab = SetBuilder::new(sc.a)?;
        fields_of(
            config,
            s.keyed,
            s.unowned,
            &mut PathBuf::new(sc.path),
            &mut ab,
        )?;
        ab.finish()
    };
    let applied = FieldSet::parse(sc.a.get(..al).unwrap_or(&[]))?;
    let mine =
        |e: &Entry<'_>| e.op == OWN_APPLY && e.manager == who.manager && e.scope == who.scope;
    // Conflicts, against the stored document.
    let mut total = 0usize;
    if let Some(st) = stored {
        for e in owners.entries().filter(|e| !mine(e)) {
            for p in conflicts(applied, e.set, config, st) {
                total += 1;
                on_conflict(Conflict {
                    manager: e.manager,
                    op: e.op,
                    version: e.version,
                    path: p,
                });
            }
        }
    }
    if total > 0 && !force {
        return Ok(Applied::Conflicts(total));
    }
    // Every other owner's fields together (what they still hold after a
    // forced apply takes its conflicts): `b`.
    let ol = {
        let mut ob = SetBuilder::new(sc.b)?;
        for e in owners.entries().filter(|e| !mine(e)) {
            for p in e.set.paths() {
                let taken = force
                    && stored
                        .is_some_and(|st| conflicts(applied, e.set, config, st).any(|q| q == p));
                if !taken {
                    ob.insert(p)?;
                }
            }
        }
        ob.finish()
    };
    let others = FieldSet::parse(sc.b.get(..ol).unwrap_or(&[]))?;
    // Removals from the merged document, through the two buffers.
    let (da, db) = doc_out;
    let mut len = {
        let d = da
            .get_mut(..merged.bytes().len())
            .ok_or(FieldError::Overflow)?;
        for (x, y) in d.iter_mut().zip(merged.bytes()) {
            *x = *y;
        }
        merged.bytes().len()
    };
    let mut in_a = true;
    let before = owners.entries().find(|e| mine(e)).map(|e| e.set);
    if let Some(before) = before {
        for p in removals(before, applied, others) {
            let (src, dst): (&[u8], &mut [u8]) = if in_a {
                (&da[..len], &mut db[..])
            } else {
                (&db[..len], &mut da[..])
            };
            let mut w = Writer::new(dst);
            without(Value::trusted(src), p, &mut w)?;
            len = w.finish().map_err(tree)?;
            in_a = !in_a;
        }
    }
    if !in_a {
        let (d, s2) = (&mut *da, &*db);
        for (x, y) in d.iter_mut().zip(s2.get(..len).unwrap_or(&[])) {
            *x = *y;
        }
    }
    let doc = Value::trusted(da.get(..len).unwrap_or(&[]));
    let version = version_of(doc, s);
    // The record: others keep what they still hold and the document still
    // has; this manager's apply entry owns the configuration's fields.
    let mut w = OwnersWriter::new(out)?;
    let mut had = false;
    for e in owners.entries() {
        if mine(&e) {
            had = true;
            let head = Entry {
                secs: who.secs,
                nanos: who.nanos,
                version,
                ..e
            };
            w.entry(&head, |b| {
                for p in applied.paths() {
                    if resolve(doc, p).is_some() {
                        b.insert(p)?;
                    }
                }
                Ok(())
            })?;
        } else {
            w.entry(&e, |b| {
                for p in e.set.paths() {
                    if others.contains(p) && resolve(doc, p).is_some() {
                        b.insert(p)?;
                    }
                }
                Ok(())
            })?;
        }
    }
    if !had {
        let head = Entry {
            manager: who.manager,
            op: OWN_APPLY,
            secs: who.secs,
            nanos: who.nanos,
            version,
            scope: who.scope,
            set: FieldSet { b: &EMPTY_SET },
        };
        w.entry(&head, |b| {
            for p in applied.paths() {
                if resolve(doc, p).is_some() {
                    b.insert(p)?;
                }
            }
            Ok(())
        })?;
    }
    Ok(Applied::Done {
        doc: len,
        owners: w.finish(),
    })
}
