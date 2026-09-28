//! Document operations.
//!
//! The document executor's logic as pure functions over canonical value
//! trees (`doc_tree.rs`): filters, projections, the canonical mutations,
//! and the lifecycle a collection's descriptor (`doc_catalog.rs`)
//! declares — create stamps, generated ids, values kept across a
//! replace, identity paths, write scopes. No ports, no I/O, no clock and
//! no randomness of its own: the executor supplies the time and the
//! random bytes, so every function here is deterministic and
//! host-tested. It stands to `document_executor` as `sql_core.rs` stands
//! to `relational_executor`.
//!
//! Mounted by `doc_exec.rs` beside `doc_tree` and `doc_catalog`.
//!
//! ## Edits
//!
//! Every change to a document is an [`Edit`] at a path, applied by
//! rebuilding the tree into a second buffer: untouched subtrees are
//! copied as bytes, and only the containers along the path are
//! re-framed. [`Buffers`] holds the two buffers and swaps them, so a
//! sequence of edits (an update's mutations, a create's stamps) runs in
//! fixed memory. The rules:
//!
//! - **Set** creates missing members along the path as objects (a
//!   numeric segment under a missing member names an object member, as
//!   MongoDB does); an index past an array's end pads with nulls, and
//!   [`APPEND_SEGMENT`] appends. A path through a scalar is refused.
//! - **Remove** takes a member out of its object or an element out of
//!   its array; **NullElement** removes a member but nulls an array
//!   element in place. Removing what is absent changes nothing.
//! - **Numeric** increments, multiplies, or takes the minimum/maximum.
//!   Integer arithmetic stays integral (two 32-bit operands stay 32 bits
//!   while the result fits) and refuses a 64-bit overflow; a binary64 on
//!   either side makes a binary64. A decimal literal takes no
//!   arithmetic — it is refused rather than rounded. A missing field
//!   takes the operand (a product takes zero).
//! - **Array** pushes, adds when no equal element exists, pulls every
//!   equal element, or pops the first or last. A missing field becomes
//!   `[operand]` for a push; a pull or pop of it changes nothing.

use core::cmp::Ordering;

use super::doc_catalog::{
    Descriptor, Generator, Index, ListKey, EVERY_ELEMENT, FORM_TEXT, FORM_VALUE, GEN_ID,
    GEN_LITERAL, GEN_NOW, GEN_UUID,
};
use super::doc_tree::{
    self, compare, equal, json_equal, lookup, segment_index, type_rank, Path, PathWriter,
    TreeError, Value, Writer, APPEND_SEGMENT, MAX_PATH_SEGMENTS, T_ARRAY, T_DECIMAL, T_INTEGER,
    T_OBJECT, WIDTH_32,
};
use super::{
    Directives, Filter, Mutation, Mutations, PathList, ARR_ADD_TO_SET, ARR_POP_FIRST, ARR_POP_LAST,
    ARR_PULL, ARR_PUSH, CMP_EQ, CMP_GT, CMP_GTE, CMP_LT, CMP_LTE, CMP_NE, MUT_ARRAY, MUT_NUMERIC,
    MUT_PATH_SET, MUT_PATH_UNSET, NUM_INC, NUM_MAX, NUM_MIN, UNSET_NULL_ELEMENT,
};

// ── Errors ────────────────────────────────────────────────────────────

/// Why an operation could not apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// The path runs through a scalar.
    NotContainer,
    /// A numeric operation found a value that is not a number.
    NotNumber,
    /// An array operation found a value that is not an array.
    NotArray,
    /// Integer arithmetic past 64 bits.
    IntegerOverflow,
    /// Arithmetic on a decimal literal.
    DecimalArithmetic,
    /// The result does not fit its buffer.
    TooLarge,
    /// The result nests past the tree's depth.
    TooDeep,
    /// A path, generator or tree the operation cannot use.
    Malformed,
    /// An RFC 6902 operation names a path that does not exist (or an index
    /// past an array's end).
    PathMissing,
    /// An RFC 6902 `test` did not hold.
    TestFailed,
    /// A value an index cannot key: an object, arrays at two of a compound
    /// index's paths, or values past an index entry's size.
    NotIndexable,
}

/// An operation's refusal: why, and at which path (the whole path of the
/// edit; `depth` is how many of its segments resolved).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpError {
    pub reason: Reason,
    pub depth: u8,
}

impl OpError {
    fn at(reason: Reason, depth: usize) -> Self {
        OpError {
            reason,
            depth: depth as u8,
        }
    }
}

impl From<TreeError> for OpError {
    fn from(e: TreeError) -> Self {
        let reason = match e {
            TreeError::Overflow => Reason::TooLarge,
            TreeError::TooDeep => Reason::TooDeep,
            _ => Reason::Malformed,
        };
        OpError { reason, depth: 0 }
    }
}

// ── Filters ───────────────────────────────────────────────────────────

/// The segments of a path, collected (a path holds at most 32).
fn segments<'a>(p: Path<'a>, out: &mut [&'a [u8]; MAX_PATH_SEGMENTS]) -> usize {
    let mut n = 0;
    for s in p.segments() {
        if n == MAX_PATH_SEGMENTS {
            break;
        }
        out[n] = s;
        n += 1;
    }
    n
}

/// Visit every value a path reaches. With `fan` an array met where the
/// path names a member is entered element by element (and a numeric
/// segment is also tried as an index), and an array at the end of the
/// path is visited itself and element by element: MongoDB's reading of a
/// query path. Without it the path is followed exactly. `f` answers
/// whether to stop.
pub fn reach<'a, F: FnMut(Value<'a>) -> bool>(
    root: Value<'a>,
    path: Path<'_>,
    fan: bool,
    f: &mut F,
) -> bool {
    let mut segs: [&[u8]; MAX_PATH_SEGMENTS] = [&[]; MAX_PATH_SEGMENTS];
    let n = segments(path, &mut segs);
    reach_at(root, &segs[..n], fan, f)
}

fn reach_at<'a, F: FnMut(Value<'a>) -> bool>(
    v: Value<'a>,
    segs: &[&[u8]],
    fan: bool,
    f: &mut F,
) -> bool {
    let Some((seg, rest)) = segs.split_first() else {
        if f(v) {
            return true;
        }
        if fan && v.ty() == T_ARRAY {
            for e in v.elements() {
                if f(e) {
                    return true;
                }
            }
        }
        return false;
    };
    match v.ty() {
        T_OBJECT => match v.member(seg) {
            Some(m) => reach_at(m, rest, fan, f),
            None => false,
        },
        T_ARRAY => {
            if let Some(e) = segment_index(seg).and_then(|i| v.element(i)) {
                if reach_at(e, rest, fan, f) {
                    return true;
                }
            }
            if fan {
                for e in v.elements() {
                    if e.ty() == T_OBJECT && reach_at(e, segs, fan, f) {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

fn cmp_holds(op: u8, candidate: Value<'_>, operand: Value<'_>) -> bool {
    if op == CMP_EQ {
        return equal(candidate, operand);
    }
    // Ordering comparisons hold within one type rank only.
    if type_rank(candidate.ty()) != type_rank(operand.ty()) {
        return false;
    }
    let o = compare(candidate, operand);
    match op {
        CMP_LT => o == Ordering::Less,
        CMP_LTE => o != Ordering::Greater,
        CMP_GT => o == Ordering::Greater,
        CMP_GTE => o != Ordering::Less,
        _ => false,
    }
}

/// Does `doc` satisfy the filter? A missing path satisfies no comparison
/// but `NE`, and no membership but `NIN` (a dialect that reads `{a:
/// null}` as "null or missing" states both).
pub fn matches(f: Filter<'_>, doc: Value<'_>) -> bool {
    match f {
        Filter::True => true,
        Filter::Group(and, _, _) => {
            let mut kids = f.children();
            if and {
                kids.all(|k| matches(k, doc))
            } else {
                kids.any(|k| matches(k, doc))
            }
        }
        Filter::Not(_) => !f.children().next().is_some_and(|k| matches(k, doc)),
        Filter::Cmp {
            op,
            any,
            path,
            value,
        } => {
            if op == CMP_NE {
                return !reach(doc, path, any, &mut |c| equal(c, value));
            }
            reach(doc, path, any, &mut |c| cmp_holds(op, c, value))
        }
        Filter::In {
            negated,
            any,
            path,
            set,
        } => {
            let hit = reach(doc, path, any, &mut |c| set.elements().any(|s| equal(c, s)));
            hit != negated
        }
        Filter::Exists { want, path } => reach(doc, path, true, &mut |_| true) == want,
    }
}

// ── Projection ────────────────────────────────────────────────────────

/// Write `doc` projected through `list`. An inclusion keeps exactly its
/// paths — a dialect that always answers the id (MongoDB) lists it —
/// and keeps an object it passes through even when nothing under it
/// matched. Paths into arrays project each element that is an object.
pub fn project(doc: Value<'_>, list: PathList<'_>, w: &mut Writer<'_>) -> Result<(), OpError> {
    let mut prefix: [&[u8]; MAX_PATH_SEGMENTS] = [&[]; MAX_PATH_SEGMENTS];
    project_object(doc, &list, &mut prefix, 0, w)
}

/// How the paths bear on the member at `prefix[..depth]`: 2 = a path
/// ends exactly there, 1 = a path runs below it, 0 = none reaches it.
fn bearing(list: &PathList<'_>, prefix: &[&[u8]]) -> u8 {
    let mut best = 0;
    let mut consider = |p: Path<'_>| {
        if p.len() < prefix.len() {
            return;
        }
        if !p.segments().zip(prefix.iter()).all(|(a, b)| a == *b) {
            return;
        }
        best = best.max(if p.len() == prefix.len() { 2 } else { 1 });
    };
    for p in list.paths() {
        consider(p);
    }
    best
}

fn project_object<'a>(
    v: Value<'a>,
    list: &PathList<'_>,
    prefix: &mut [&'a [u8]; MAX_PATH_SEGMENTS],
    depth: usize,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    let include = list.include();
    w.begin_object()?;
    for (k, m) in v.members() {
        if depth >= MAX_PATH_SEGMENTS {
            break;
        }
        prefix[depth] = k;
        let b = bearing(list, &prefix[..depth + 1]);
        match (include, b) {
            (true, 0) | (false, 2) => {}
            (true, 2) | (false, 0) => {
                w.key(k)?;
                w.value(m)?;
            }
            _ => {
                // A path runs below this member. Below a scalar there is
                // nothing: an inclusion drops it, an exclusion keeps it.
                if !m.is_container() {
                    if !include {
                        w.key(k)?;
                        w.value(m)?;
                    }
                    continue;
                }
                w.key(k)?;
                project_below(m, list, prefix, depth + 1, w)?;
            }
        }
    }
    w.end()?;
    Ok(())
}

fn project_below<'a>(
    m: Value<'a>,
    list: &PathList<'_>,
    prefix: &mut [&'a [u8]; MAX_PATH_SEGMENTS],
    depth: usize,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    match m.ty() {
        T_OBJECT => project_object(m, list, prefix, depth, w),
        T_ARRAY => {
            w.begin_array()?;
            for e in m.elements() {
                match e.ty() {
                    T_OBJECT | T_ARRAY => project_below(e, list, prefix, depth, w)?,
                    // A scalar element has nothing below it: an inclusion
                    // drops it, an exclusion keeps it.
                    _ if !list.include() => w.value(e)?,
                    _ => {}
                }
            }
            w.end()?;
            Ok(())
        }
        // Only containers are projected below (scalars are settled by
        // the caller).
        _ => Ok(w.value(m)?),
    }
}

// ── Edits ─────────────────────────────────────────────────────────────

/// One change at a path.
#[derive(Clone, Copy, Debug)]
pub enum Edit<'a> {
    Set(Value<'a>),
    /// Set, except that in an array it inserts before the addressed
    /// element (RFC 6902's `add`); past the end it appends.
    Insert(Value<'a>),
    Remove,
    NullElement,
    /// `(NUM_*, operand)`.
    Numeric(u8, Value<'a>),
    /// `(ARR_*, operand)`.
    Array(u8, Option<Value<'a>>),
}

impl Edit<'_> {
    /// Does this edit leave an absent path absent?
    fn keeps_absent(&self) -> bool {
        matches!(
            self,
            Edit::Remove
                | Edit::NullElement
                | Edit::Array(ARR_PULL | ARR_POP_FIRST | ARR_POP_LAST, _)
        )
    }
}

/// Apply `e` at `path` of `doc` into `w`.
pub fn edit(
    doc: Value<'_>,
    path: Path<'_>,
    e: Edit<'_>,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    let mut segs: [&[u8]; MAX_PATH_SEGMENTS] = [&[]; MAX_PATH_SEGMENTS];
    let n = segments(path, &mut segs);
    if n == 0 {
        // The root: only a whole-document set applies.
        return match e {
            Edit::Set(v) | Edit::Insert(v) if v.ty() == T_OBJECT => Ok(w.value(v)?),
            _ => Err(OpError::at(Reason::Malformed, 0)),
        };
    }
    if e.keeps_absent() && lookup(doc, path).is_none() {
        // Nothing to remove or pop: the document stands. (A path through
        // a scalar is still refused for a pop or pull: the value is not
        // an array. For a removal it is simply absent.)
        return Ok(w.value(doc)?);
    }
    edit_at(Some(doc), &segs[..n], 0, e, w)
}

fn edit_at(
    v: Option<Value<'_>>,
    segs: &[&[u8]],
    depth: usize,
    e: Edit<'_>,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    let Some((seg, rest)) = segs.split_first() else {
        return leaf(v, e, depth, w);
    };
    let last = rest.is_empty();
    match v {
        Some(v) if v.ty() == T_OBJECT => {
            w.begin_object()?;
            let mut found = false;
            for (k, m) in v.members() {
                if k != *seg {
                    w.key(k)?;
                    w.value(m)?;
                    continue;
                }
                found = true;
                if last && matches!(e, Edit::Remove | Edit::NullElement) {
                    continue;
                }
                w.key(k)?;
                edit_at(Some(m), rest, depth + 1, e, w)?;
            }
            if !found && !e.keeps_absent() {
                w.key(seg)?;
                edit_at(None, rest, depth + 1, e, w)?;
            }
            w.end()?;
            Ok(())
        }
        Some(v) if v.ty() == T_ARRAY => {
            let len = v.count() as usize;
            let idx = if *seg == APPEND_SEGMENT {
                Some(len)
            } else {
                segment_index(seg)
            };
            let Some(idx) = idx else {
                return Err(OpError::at(Reason::NotContainer, depth));
            };
            w.begin_array()?;
            for (i, el) in v.elements().enumerate() {
                if i != idx {
                    w.value(el)?;
                    continue;
                }
                if last && matches!(e, Edit::Remove) {
                    continue;
                }
                if last && matches!(e, Edit::NullElement) {
                    w.null()?;
                    continue;
                }
                if let (true, Edit::Insert(x)) = (last, e) {
                    w.value(x)?;
                    w.value(el)?;
                    continue;
                }
                edit_at(Some(el), rest, depth + 1, e, w)?;
            }
            if idx >= len && !e.keeps_absent() {
                for _ in len..idx {
                    w.null()?;
                }
                edit_at(None, rest, depth + 1, e, w)?;
            }
            w.end()?;
            Ok(())
        }
        Some(_) => Err(OpError::at(Reason::NotContainer, depth)),
        None => {
            // Creating: every missing level is an object.
            w.begin_object()?;
            w.key(seg)?;
            edit_at(None, rest, depth + 1, e, w)?;
            w.end()?;
            Ok(())
        }
    }
}

fn leaf(
    v: Option<Value<'_>>,
    e: Edit<'_>,
    depth: usize,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    match e {
        Edit::Set(x) | Edit::Insert(x) => Ok(w.value(x)?),
        // Removals are applied by the parent; reaching a leaf means the
        // value is present and stays.
        Edit::Remove | Edit::NullElement => match v {
            Some(x) => Ok(w.value(x)?),
            None => Ok(w.null()?),
        },
        Edit::Numeric(sub, x) => numeric(v, sub, x, depth, w),
        Edit::Array(sub, x) => array(v, sub, x, depth, w),
    }
}

fn is_number(v: Value<'_>) -> bool {
    matches!(v.ty(), T_INTEGER | T_DECIMAL)
}

fn numeric(
    v: Option<Value<'_>>,
    sub: u8,
    x: Value<'_>,
    depth: usize,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    if matches!(sub, NUM_MIN | NUM_MAX) {
        let pick = match v {
            None => x,
            Some(cur) => {
                let o = compare(x, cur);
                let replace = if sub == NUM_MIN {
                    o == Ordering::Less
                } else {
                    o == Ordering::Greater
                };
                if replace {
                    x
                } else {
                    cur
                }
            }
        };
        return Ok(w.value(pick)?);
    }
    if !is_number(x) {
        return Err(OpError::at(Reason::NotNumber, depth));
    }
    let cur = match v {
        None => {
            // A missing field takes the increment, or the product's zero.
            if sub == NUM_INC {
                return Ok(w.value(x)?);
            }
            return zero_like(x, w);
        }
        Some(c) if is_number(c) => c,
        Some(_) => return Err(OpError::at(Reason::NotNumber, depth)),
    };
    if cur.decimal_text().is_some() || x.decimal_text().is_some() {
        return Err(OpError::at(Reason::DecimalArithmetic, depth));
    }
    match (cur.as_i64(), x.as_i64()) {
        (Some(a), Some(b)) => {
            let r = if sub == NUM_INC {
                a.checked_add(b)
            } else {
                a.checked_mul(b)
            }
            .ok_or(OpError::at(Reason::IntegerOverflow, depth))?;
            let narrow = cur.int_width() == Some(WIDTH_32) && x.int_width() == Some(WIDTH_32);
            match i32::try_from(r) {
                Ok(r32) if narrow => Ok(w.int32(r32)?),
                _ => Ok(w.int(r)?),
            }
        }
        _ => {
            let a = as_f64(cur);
            let b = as_f64(x);
            Ok(w.f64(if sub == NUM_INC { a + b } else { a * b })?)
        }
    }
}

fn as_f64(v: Value<'_>) -> f64 {
    v.as_f64()
        .or_else(|| v.as_i64().map(|i| i as f64))
        .unwrap_or(0.0)
}

fn zero_like(x: Value<'_>, w: &mut Writer<'_>) -> Result<(), OpError> {
    if x.ty() == T_DECIMAL {
        return if x.decimal_text().is_some() {
            Ok(w.decimal_text(b"0")?)
        } else {
            Ok(w.f64(0.0)?)
        };
    }
    if x.int_width() == Some(WIDTH_32) {
        Ok(w.int32(0)?)
    } else {
        Ok(w.int(0)?)
    }
}

fn array(
    v: Option<Value<'_>>,
    sub: u8,
    x: Option<Value<'_>>,
    depth: usize,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    let cur = match v {
        None => {
            // Only a push or add reaches an absent leaf (the others keep
            // it absent before the walk).
            w.begin_array()?;
            if let Some(x) = x {
                w.value(x)?;
            }
            w.end()?;
            return Ok(());
        }
        Some(c) if c.ty() == T_ARRAY => c,
        Some(_) => return Err(OpError::at(Reason::NotArray, depth)),
    };
    let n = cur.count() as usize;
    w.begin_array()?;
    for (i, el) in cur.elements().enumerate() {
        let drop = match sub {
            ARR_PULL => x.is_some_and(|x| equal(el, x)),
            ARR_POP_FIRST => i == 0,
            ARR_POP_LAST => i + 1 == n,
            _ => false,
        };
        if !drop {
            w.value(el)?;
        }
    }
    match (sub, x) {
        (ARR_PUSH, Some(x)) => w.value(x)?,
        (ARR_ADD_TO_SET, Some(x)) if !cur.elements().any(|el| equal(el, x)) => w.value(x)?,
        _ => {}
    }
    w.end()?;
    Ok(())
}

/// The edit a canonical mutation makes.
pub fn mutation_edit<'a>(m: &Mutation<'a>) -> Option<Edit<'a>> {
    match m.kind {
        MUT_PATH_SET => Some(Edit::Set(m.operand?)),
        MUT_PATH_UNSET if m.sub == UNSET_NULL_ELEMENT => Some(Edit::NullElement),
        MUT_PATH_UNSET => Some(Edit::Remove),
        MUT_NUMERIC => Some(Edit::Numeric(m.sub, m.operand?)),
        MUT_ARRAY => Some(Edit::Array(m.sub, m.operand)),
        _ => None,
    }
}

// ── Buffers ───────────────────────────────────────────────────────────

/// Two buffers a sequence of edits alternates between.
pub struct Buffers<'b> {
    a: &'b mut [u8],
    b: &'b mut [u8],
    /// Which holds the current document: false = `a`.
    in_b: bool,
    len: usize,
}

impl<'b> Buffers<'b> {
    pub fn new(a: &'b mut [u8], b: &'b mut [u8]) -> Self {
        Buffers {
            a,
            b,
            in_b: false,
            len: 0,
        }
    }

    /// Start from `doc`.
    pub fn load(&mut self, doc: Value<'_>) -> Result<(), OpError> {
        let src = doc.bytes();
        let d = self
            .a
            .get_mut(..src.len())
            .ok_or(OpError::at(Reason::TooLarge, 0))?;
        for (x, y) in d.iter_mut().zip(src) {
            *x = *y;
        }
        self.in_b = false;
        self.len = src.len();
        Ok(())
    }

    /// The current document.
    pub fn current(&self) -> Value<'_> {
        let buf: &[u8] = if self.in_b { self.b } else { self.a };
        Value::trusted(buf.get(..self.len).unwrap_or(&[]))
    }

    /// Replace the current document by `f(current, writer)`'s output.
    pub fn apply<F>(&mut self, f: F) -> Result<(), OpError>
    where
        F: FnOnce(Value<'_>, &mut Writer<'_>) -> Result<(), OpError>,
    {
        let (src, dst): (&[u8], &mut [u8]) = if self.in_b {
            (&*self.b, &mut *self.a)
        } else {
            (&*self.a, &mut *self.b)
        };
        let cur = Value::trusted(src.get(..self.len).unwrap_or(&[]));
        let mut w = Writer::new(dst);
        f(cur, &mut w)?;
        self.len = w.finish()?;
        self.in_b = !self.in_b;
        Ok(())
    }

    /// Apply one edit.
    pub fn edit(&mut self, path: Path<'_>, e: Edit<'_>) -> Result<(), OpError> {
        self.apply(|cur, w| edit(cur, path, e, w))
    }
}

/// Apply an update's mutations in order. The error names the failing
/// mutation's position.
pub fn apply_mutations(
    bufs: &mut Buffers<'_>,
    muts: Mutations<'_>,
) -> Result<(), (usize, OpError)> {
    for (i, m) in muts.iter().enumerate() {
        let e = mutation_edit(&m).ok_or((i, OpError::at(Reason::Malformed, 0)))?;
        bufs.edit(m.path, e).map_err(|err| (i, err))?;
    }
    Ok(())
}

// ── Patches ───────────────────────────────────────────────────────────

/// What a keyed patch knows beyond the patch: the collection's list keys
/// and the dialect's directive names.
#[derive(Clone, Copy)]
pub struct Keyed<'d> {
    pub desc: Descriptor<'d>,
    pub dir: Directives<'d>,
}

/// How a keyed patch merges the array at a location.
pub enum ListRule<'d> {
    /// Replaced whole.
    Atomic,
    /// A set of scalars: stored order, then what the patch adds.
    Set,
    /// Element by element, identified by the key's members.
    Keyed(ListKey<'d>),
}

/// The location's rule: the longest list-key path that is a suffix of it.
pub fn list_rule<'d>(k: &Keyed<'d>, loc: &[&[u8]]) -> ListRule<'d> {
    let mut best: Option<(usize, ListKey<'d>)> = None;
    for lk in k.desc.list_keys() {
        let pl = lk.path.len();
        if pl > loc.len() {
            continue;
        }
        let tail = &loc[loc.len() - pl..];
        if lk.path.segments().zip(tail.iter()).all(|(a, b)| a == *b)
            && best.as_ref().is_none_or(|(l, _)| pl > *l)
        {
            best = Some((pl, lk));
        }
    }
    match best {
        None => ListRule::Atomic,
        Some((_, lk)) if lk.is_set() => ListRule::Set,
        Some((_, lk)) => ListRule::Keyed(lk),
    }
}

/// An object's patch directive (`replace`, `delete`, `merge`), if it
/// speaks one.
fn directive_of<'v>(k: &Keyed<'_>, v: Value<'v>) -> Option<&'v [u8]> {
    if v.ty() != T_OBJECT || k.dir.patch.is_empty() {
        return None;
    }
    v.member(k.dir.patch)?.as_str()
}

/// Is member `m` a directive rather than data?
fn is_directive(k: &Keyed<'_>, m: &[u8]) -> bool {
    (!k.dir.prefix.is_empty() && m.starts_with(k.dir.prefix))
        || (!k.dir.patch.is_empty() && m == k.dir.patch)
        || (!k.dir.retain.is_empty() && m == k.dir.retain)
}

/// The scalars a patch object drops from its set member `list`.
fn delete_list<'v>(k: &Keyed<'_>, pobj: Value<'v>, list: &[u8]) -> Option<Value<'v>> {
    let pre = k.dir.delete_from_set;
    if pre.is_empty() {
        return None;
    }
    pobj.members()
        .find(|(m, _)| m.len() == pre.len() + list.len() && m.starts_with(pre) && m.ends_with(list))
        .map(|(_, v)| v)
        .filter(|v| v.ty() == T_ARRAY)
}

/// A location: member names from the root, [`EVERY_ELEMENT`] for an array
/// element.
struct Loc<'a> {
    segs: [&'a [u8]; MAX_PATH_SEGMENTS],
    n: usize,
}

impl<'a> Loc<'a> {
    fn push(&mut self, s: &'a [u8]) -> Result<(), OpError> {
        let slot = self
            .segs
            .get_mut(self.n)
            .ok_or(OpError::at(Reason::TooDeep, self.n))?;
        *slot = s;
        self.n += 1;
        Ok(())
    }
    fn pop(&mut self) {
        self.n = self.n.saturating_sub(1);
    }
    fn get(&self) -> &[&'a [u8]] {
        &self.segs[..self.n]
    }
}

/// RFC 7386 merge patch (`keyed` = None), or the keyed merge a collection's
/// list keys and a dialect's directives describe (`keyed` = Some): objects
/// merge member by member, `null` removes, other values replace; under
/// keyed, arrays merge by their rule — element by element on their keys,
/// as sets, or whole — and directives replace or delete an object, keep
/// only listed members, or drop scalars from a set. The target's members
/// keep their order; new ones follow in the patch's.
pub fn merge_patch(
    target: Value<'_>,
    patch: Value<'_>,
    keyed: Option<&Keyed<'_>>,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    let mut loc = Loc {
        segs: [&[]; MAX_PATH_SEGMENTS],
        n: 0,
    };
    merge_at(Some(target), patch, keyed, &mut loc, w)
}

fn merge_at<'p>(
    target: Option<Value<'p>>,
    patch: Value<'p>,
    keyed: Option<&Keyed<'_>>,
    loc: &mut Loc<'p>,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    if patch.ty() != T_OBJECT {
        // Not an object: the patch's value is the result.
        return Ok(w.value(patch)?);
    }
    let replace = keyed.is_some_and(|k| directive_of(k, patch) == Some(b"replace"));
    let target = target.filter(|t| !replace && t.ty() == T_OBJECT);
    let retain = keyed.and_then(|k| {
        if k.dir.retain.is_empty() {
            None
        } else {
            patch.member(k.dir.retain).filter(|r| r.ty() == T_ARRAY)
        }
    });
    let deletes = |v: Value<'_>| keyed.is_some_and(|k| directive_of(k, v) == Some(b"delete"));
    w.begin_object()?;
    if let Some(t) = target {
        for (m, tv) in t.members() {
            if retain.is_some_and(|r| !r.elements().any(|e| e.as_str() == Some(m))) {
                continue;
            }
            match patch.member(m) {
                None => {
                    w.key(m)?;
                    let drop = keyed
                        .filter(|_| tv.ty() == T_ARRAY)
                        .and_then(|k| delete_list(k, patch, m));
                    match drop {
                        Some(d) => {
                            w.begin_array()?;
                            for e in tv.elements() {
                                if !d.elements().any(|x| equal(x, e)) {
                                    w.value(e)?;
                                }
                            }
                            w.end()?;
                        }
                        None => w.value(tv)?,
                    }
                }
                Some(pv) if pv.is_null() || deletes(pv) => {}
                Some(pv) => {
                    w.key(m)?;
                    merge_value(Some(tv), pv, m, patch, keyed, loc, w)?;
                }
            }
        }
    }
    for (m, pv) in patch.members() {
        if keyed.is_some_and(|k| is_directive(k, m)) {
            continue;
        }
        if target.is_some_and(|t| t.member(m).is_some()) {
            continue;
        }
        if pv.is_null() || deletes(pv) {
            continue;
        }
        w.key(m)?;
        merge_value(None, pv, m, patch, keyed, loc, w)?;
    }
    w.end()?;
    Ok(())
}

/// One member's merged value: stored `tv`, patched `pv`, under member `m`
/// of the patch object `pobj`.
fn merge_value<'p>(
    tv: Option<Value<'p>>,
    pv: Value<'p>,
    m: &'p [u8],
    pobj: Value<'p>,
    keyed: Option<&Keyed<'_>>,
    loc: &mut Loc<'p>,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    loc.push(m)?;
    let r = match keyed {
        Some(k) if pv.ty() == T_ARRAY => {
            let stored = tv.filter(|t| t.ty() == T_ARRAY);
            match list_rule(k, loc.get()) {
                ListRule::Keyed(lk) => merge_list(stored, pv, &lk, k, loc, w),
                ListRule::Set => {
                    let drop = delete_list(k, pobj, m);
                    (|| {
                        w.begin_array()?;
                        if let Some(t) = stored {
                            for e in t.elements() {
                                if !drop.is_some_and(|d| d.elements().any(|x| equal(x, e))) {
                                    w.value(e)?;
                                }
                            }
                        }
                        for e in pv.elements() {
                            if !stored.is_some_and(|t| t.elements().any(|x| equal(x, e))) {
                                w.value(e)?;
                            }
                        }
                        w.end()?;
                        Ok(())
                    })()
                }
                ListRule::Atomic => (|| {
                    // Replaced whole; the patch's own elements written as
                    // merged into nothing (their nulls and directives gone).
                    w.begin_array()?;
                    loc.push(EVERY_ELEMENT)?;
                    for e in pv.elements() {
                        merge_at(None, e, keyed, loc, w)?;
                    }
                    loc.pop();
                    w.end()?;
                    Ok(())
                })(),
            }
        }
        _ => merge_at(tv, pv, keyed, loc, w),
    };
    loc.pop();
    r
}

/// Do two elements carry the same key (every identifying member present
/// in both and equal)?
fn same_key(lk: &ListKey<'_>, a: Value<'_>, b: Value<'_>) -> bool {
    let mut any = false;
    for key in lk.keys() {
        match (a.member(key), b.member(key)) {
            (Some(x), Some(y)) if json_equal(x, y) => any = true,
            (None, None) => {}
            _ => return false,
        }
    }
    any
}

/// A keyed list: stored elements in order — merged with the patch's
/// element of the same key, or dropped by a delete directive — then the
/// patch's new elements. A replace-directive element makes the patch's
/// list the result.
fn merge_list<'p>(
    stored: Option<Value<'p>>,
    pl: Value<'p>,
    lk: &ListKey<'_>,
    k: &Keyed<'_>,
    loc: &mut Loc<'p>,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    let replace = pl
        .elements()
        .any(|e| directive_of(k, e) == Some(b"replace"));
    let stored = stored.filter(|_| !replace);
    loc.push(EVERY_ELEMENT)?;
    w.begin_array()?;
    if let Some(t) = stored {
        for te in t.elements() {
            match pl.elements().find(|pe| same_key(lk, te, *pe)) {
                Some(pe) if directive_of(k, pe) == Some(b"delete") => {}
                Some(pe) => merge_at(Some(te), pe, Some(k), loc, w)?,
                None => w.value(te)?,
            }
        }
    }
    for pe in pl.elements() {
        let d = directive_of(k, pe);
        if d == Some(b"replace") || d == Some(b"delete") {
            continue;
        }
        if stored.is_some_and(|t| t.elements().any(|te| same_key(lk, te, pe))) {
            continue;
        }
        merge_at(None, pe, Some(k), loc, w)?;
    }
    w.end()?;
    loc.pop();
    Ok(())
}

/// RFC 6902 JSON Patch: `ops` (an array of operation objects) applied in
/// order. `tmp` holds a value `move` and `copy` carry. The error names the
/// failing operation's position.
pub fn json_patch(
    bufs: &mut Buffers<'_>,
    ops: Value<'_>,
    tmp: &mut [u8],
) -> Result<(), (usize, OpError)> {
    if ops.ty() != T_ARRAY {
        return Err((0, OpError::at(Reason::Malformed, 0)));
    }
    for (i, op) in ops.elements().enumerate() {
        json_op(bufs, op, tmp).map_err(|e| (i, e))?;
    }
    Ok(())
}

fn pointer_member(op: Value<'_>, name: &[u8], out: &mut [u8]) -> Result<usize, OpError> {
    let p = op
        .member(name)
        .and_then(|v| v.as_str())
        .ok_or(OpError::at(Reason::Malformed, 0))?;
    doc_tree::path_from_pointer(p, out).map_err(|_| OpError::at(Reason::Malformed, 0))
}

fn json_op(bufs: &mut Buffers<'_>, op: Value<'_>, tmp: &mut [u8]) -> Result<(), OpError> {
    let malformed = OpError::at(Reason::Malformed, 0);
    let name = op.member(b"op").and_then(|v| v.as_str()).ok_or(malformed)?;
    let mut pb = [0u8; 1024];
    let pn = pointer_member(op, b"path", &mut pb)?;
    let path = Path::trusted(&pb[..pn]);
    match name {
        b"add" => json_add(bufs, path, op.member(b"value").ok_or(malformed)?),
        b"remove" => {
            if path.is_empty() || lookup(bufs.current(), path).is_none() {
                return Err(OpError::at(Reason::PathMissing, 0));
            }
            bufs.edit(path, Edit::Remove)
        }
        b"replace" => {
            if lookup(bufs.current(), path).is_none() {
                return Err(OpError::at(Reason::PathMissing, 0));
            }
            bufs.edit(path, Edit::Set(op.member(b"value").ok_or(malformed)?))
        }
        b"test" => {
            let want = op.member(b"value").ok_or(malformed)?;
            match lookup(bufs.current(), path) {
                Some(v) if json_equal(v, want) => Ok(()),
                _ => Err(OpError::at(Reason::TestFailed, 0)),
            }
        }
        b"move" | b"copy" => {
            let mut fb = [0u8; 1024];
            let fl = pointer_member(op, b"from", &mut fb)?;
            let from = Path::trusted(&fb[..fl]);
            if name == b"move" && from.len() < path.len() && from.is_prefix_of(path) {
                // Into its own child.
                return Err(malformed);
            }
            let n = {
                let v = lookup(bufs.current(), from).ok_or(OpError::at(Reason::PathMissing, 0))?;
                let b = v.bytes();
                tmp.get_mut(..b.len())
                    .ok_or(OpError::at(Reason::TooLarge, 0))?
                    .copy_from_slice(b);
                b.len()
            };
            if name == b"move" {
                if from == path {
                    return Ok(());
                }
                bufs.edit(from, Edit::Remove)?;
            }
            json_add(bufs, path, Value::trusted(&tmp[..n]))
        }
        _ => Err(malformed),
    }
}

/// RFC 6902 `add`: the parent must exist; into an array it inserts (or
/// appends at `-`), into an object it sets.
fn json_add(bufs: &mut Buffers<'_>, path: Path<'_>, v: Value<'_>) -> Result<(), OpError> {
    let n = path.len();
    if n == 0 {
        return bufs.edit(path, Edit::Set(v));
    }
    let mut parent = [0u8; 1024];
    let mut pw = PathWriter::new(&mut parent)?;
    for s in path.segments().take(n - 1) {
        pw.push(s)?;
    }
    let pl = pw.finish();
    let last = path.segment(n - 1).unwrap_or(&[]);
    let container = lookup(bufs.current(), Path::trusted(&parent[..pl]))
        .ok_or(OpError::at(Reason::PathMissing, 0))?;
    match container.ty() {
        T_OBJECT => bufs.edit(path, Edit::Set(v)),
        T_ARRAY => {
            let len = container.count() as usize;
            let ok = last == APPEND_SEGMENT || segment_index(last).is_some_and(|i| i <= len);
            if !ok {
                return Err(OpError::at(Reason::PathMissing, 0));
            }
            bufs.edit(path, Edit::Insert(v))
        }
        _ => Err(OpError::at(Reason::NotContainer, 0)),
    }
}

// ── Lifecycle: carry, identity ────────────────────────────────────────

/// Carry each path's value from `from` into the current document: set
/// where `from` has one, removed where it has none. A replace keeps its
/// declared paths this way (`from` = the stored document); a write scope
/// is the same move in one direction or the other.
pub fn carry<'p>(
    bufs: &mut Buffers<'_>,
    from: Value<'_>,
    paths: impl Iterator<Item = Path<'p>>,
) -> Result<(), OpError> {
    for p in paths {
        match lookup(from, p) {
            Some(v) => bufs.edit(p, Edit::Set(v))?,
            None => bufs.edit(p, Edit::Remove)?,
        }
    }
    Ok(())
}

/// The first of `paths` whose value differs between `before` and
/// `after` (present in one only, or unequal).
pub fn changed_path<'p>(
    before: Value<'_>,
    after: Value<'_>,
    paths: impl Iterator<Item = Path<'p>>,
) -> Option<Path<'p>> {
    for p in paths {
        let same = match (lookup(before, p), lookup(after, p)) {
            (None, None) => true,
            (Some(a), Some(b)) => equal(a, b) && a.ty() == b.ty(),
            _ => false,
        };
        if !same {
            return Some(p);
        }
    }
    None
}

// ── Guarded delete ────────────────────────────────────────────────────

/// Does the collection's guard hold `doc` (a non-empty array at the
/// guard path)?
pub fn guard_holds(desc: &Descriptor<'_>, doc: Value<'_>) -> bool {
    desc.guard_path()
        .and_then(|p| lookup(doc, p))
        .is_some_and(|g| g.ty() == T_ARRAY && g.count() > 0)
}

/// Is `doc` marked deleting (the first declared deleting stamp present)?
pub fn marked_deleting(desc: &Descriptor<'_>, doc: Value<'_>) -> bool {
    desc.deleting_stamps()
        .next()
        .is_some_and(|s| lookup(doc, s.path).is_some())
}

// ── Index entries ─────────────────────────────────────────────────────

/// Longest run of value keys one index entry holds
/// (`models::MAX_DOC_INDEX_VALUE_LEN`).
pub const MAX_INDEX_VALUES: usize = 256;

/// Call `f` with each index entry's values key for `doc` under `ix`: the
/// value keys of its paths concatenated. A missing path indexes as null;
/// an array at one path makes one entry per distinct element (a multikey
/// index). `f` answers false to stop.
pub fn index_values<F: FnMut(&[u8]) -> bool>(
    ix: &Index<'_>,
    doc: Value<'_>,
    f: &mut F,
) -> Result<(), OpError> {
    let refuse = OpError::at(Reason::NotIndexable, 0);
    let null = [T_NULL_BYTE];
    let null_v = Value::trusted(&null);
    let mut vals: [Option<Value<'_>>; super::doc_catalog::MAX_INDEX_PATHS] =
        [None; super::doc_catalog::MAX_INDEX_PATHS];
    let mut n = 0usize;
    let mut multi: Option<usize> = None;
    for p in ix.paths() {
        let v = lookup(doc, p).unwrap_or(null_v);
        match v.ty() {
            T_OBJECT => return Err(refuse),
            T_ARRAY => {
                if multi.is_some() {
                    return Err(refuse);
                }
                multi = Some(n);
            }
            _ => {}
        }
        *vals.get_mut(n).ok_or(refuse)? = Some(v);
        n += 1;
    }
    let mut key = [0u8; MAX_INDEX_VALUES];
    let write = |vals: &[Option<Value<'_>>],
                 elem: Option<Value<'_>>,
                 key: &mut [u8]|
     -> Result<usize, OpError> {
        let mut at = 0usize;
        for (i, v) in vals.iter().enumerate() {
            let v = match (Some(i) == multi, elem) {
                (true, Some(e)) => e,
                _ => v.ok_or(refuse)?,
            };
            if v.is_container() {
                return Err(refuse);
            }
            at += doc_tree::value_key(v, key.get_mut(at..).ok_or(refuse)?).ok_or(refuse)?;
        }
        Ok(at)
    };
    match multi {
        None => {
            let k = write(&vals[..n], None, &mut key)?;
            f(&key[..k]);
        }
        Some(m) => {
            let arr = vals[m].ok_or(refuse)?;
            if arr.count() == 0 {
                let k = write(&vals[..n], Some(null_v), &mut key)?;
                f(&key[..k]);
            }
            for (i, e) in arr.elements().enumerate() {
                // Each distinct element once.
                if arr.elements().take(i).any(|x| equal(x, e)) {
                    continue;
                }
                let k = write(&vals[..n], Some(e), &mut key)?;
                if !f(&key[..k]) {
                    break;
                }
            }
        }
    }
    Ok(())
}

/// The tree byte of null.
const T_NULL_BYTE: u8 = doc_tree::T_NULL;

// ── Generators ────────────────────────────────────────────────────────

/// What a generator draws on: the executor's clock and entropy, and the
/// document's id.
#[derive(Clone, Copy, Debug)]
pub struct StampContext<'a> {
    /// Seconds and nanoseconds since the Unix epoch.
    pub now: (i64, u32),
    /// Sixteen random bytes (a UUID's).
    pub random: [u8; 16],
    pub id: Option<Value<'a>>,
}

/// Write the value a generator makes.
pub fn generate(
    g: &Generator<'_>,
    ctx: &StampContext<'_>,
    w: &mut Writer<'_>,
) -> Result<(), OpError> {
    match (g.kind, g.form) {
        (GEN_UUID, FORM_VALUE) => Ok(w.identifier(&uuid_v4(ctx.random))?),
        (GEN_UUID, FORM_TEXT) => {
            let mut t = [0u8; 36];
            uuid_text(&uuid_v4(ctx.random), &mut t);
            Ok(w.string(&t)?)
        }
        (GEN_NOW, FORM_VALUE) => Ok(w.timestamp(ctx.now.0, ctx.now.1)?),
        (GEN_NOW, FORM_TEXT) => {
            let mut t = [0u8; 20];
            rfc3339_utc(ctx.now.0, &mut t).ok_or(OpError::at(Reason::Malformed, 0))?;
            Ok(w.string(&t)?)
        }
        (GEN_LITERAL, FORM_VALUE) => Ok(w.value(Value::trusted(g.literal))?),
        (GEN_ID, FORM_VALUE) => match ctx.id {
            Some(id) => Ok(w.value(id)?),
            None => Err(OpError::at(Reason::Malformed, 0)),
        },
        _ => Err(OpError::at(Reason::Malformed, 0)),
    }
}

/// RFC 4122 version 4: the random bytes with the version and variant set.
pub fn uuid_v4(mut r: [u8; 16]) -> [u8; 16] {
    r[6] = (r[6] & 0x0F) | 0x40;
    r[8] = (r[8] & 0x3F) | 0x80;
    r
}

/// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, lower case.
pub fn uuid_text(u: &[u8; 16], out: &mut [u8; 36]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut j = 0;
    for (i, b) in u.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out[j] = b'-';
            j += 1;
        }
        out[j] = HEX[(b >> 4) as usize];
        out[j + 1] = HEX[(b & 0x0F) as usize];
        j += 2;
    }
}

/// `YYYY-MM-DDTHH:MM:SSZ` for seconds since the epoch (years 0..=9999).
pub fn rfc3339_utc(secs: i64, out: &mut [u8; 20]) -> Option<()> {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    // Civil from days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    if !(0..=9999).contains(&y) {
        return None;
    }
    let put = |out: &mut [u8; 20], at: usize, v: i64, n: usize| {
        let mut v = v;
        for k in (0..n).rev() {
            out[at + k] = b'0' + (v % 10) as u8;
            v /= 10;
        }
    };
    put(out, 0, y, 4);
    out[4] = b'-';
    put(out, 5, m, 2);
    out[7] = b'-';
    put(out, 8, d, 2);
    out[10] = b'T';
    put(out, 11, sod / 3600, 2);
    out[13] = b':';
    put(out, 14, (sod / 60) % 60, 2);
    out[16] = b':';
    put(out, 17, sod % 60, 2);
    out[19] = b'Z';
    Some(())
}

/// `YYYY-MM-DDTHH:MM:SSZ` back into seconds since the epoch — the inverse of
/// [`rfc3339_utc`] (a fractional second, `.fff…`, before the `Z` is
/// accepted and dropped). `None` for any other text or an impossible date.
pub fn rfc3339_secs(t: &[u8]) -> Option<i64> {
    let digits = |from: usize, n: usize| -> Option<i64> {
        let mut v = 0i64;
        for k in 0..n {
            let c = *t.get(from + k)?;
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + i64::from(c - b'0');
        }
        Some(v)
    };
    let sep = |at: usize, c: u8| t.get(at) == Some(&c);
    if !(sep(4, b'-') && sep(7, b'-') && sep(10, b'T') && sep(13, b':') && sep(16, b':')) {
        return None;
    }
    let (y, m, d) = (digits(0, 4)?, digits(5, 2)?, digits(8, 2)?);
    let (hh, mm, ss) = (digits(11, 2)?, digits(14, 2)?, digits(17, 2)?);
    let mut end = 19usize;
    if t.get(end) == Some(&b'.') {
        end += 1;
        while t.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
    }
    if t.get(end) != Some(&b'Z') || end + 1 != t.len() {
        return None;
    }
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    // Days from civil (Howard Hinnant's algorithm), the inverse of the one
    // `rfc3339_utc` uses.
    let y2 = if m <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + hh * 3600 + mm * 60 + ss;
    // A day past its month's end (Feb 30) does not round-trip: refused.
    let mut back = [0u8; 20];
    rfc3339_utc(secs, &mut back)?;
    (back.get(..10) == t.get(..10)).then_some(secs)
}

/// A generated id: `prefix` then `suffix` characters of `alphabet`, one
/// per random byte (modulo the alphabet; the executor's entropy is
/// uniform enough for names, and a collision is refused as `EXISTS`).
pub fn generated_name(
    prefix: &[u8],
    suffix: usize,
    alphabet: &[u8],
    random: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    if alphabet.is_empty() || random.len() < suffix {
        return None;
    }
    let n = prefix.len() + suffix;
    let d = out.get_mut(..n)?;
    for (x, y) in d.iter_mut().zip(prefix) {
        *x = *y;
    }
    for (i, r) in random.iter().take(suffix).enumerate() {
        d[prefix.len() + i] = alphabet[*r as usize % alphabet.len()];
    }
    Some(n)
}

/// Is `v` usable as a document id? A scalar of a type the key encoding
/// orders — string, integer, identifier, binary or timestamp — or, for a
/// compound id, a non-empty array of them.
pub fn id_is_usable(v: Value<'_>) -> bool {
    let scalar = |v: Value<'_>| {
        matches!(
            v.ty(),
            doc_tree::T_STRING
                | T_INTEGER
                | doc_tree::T_IDENTIFIER
                | doc_tree::T_BINARY
                | doc_tree::T_TIMESTAMP
        )
    };
    scalar(v) || (v.ty() == T_ARRAY && v.count() > 0 && v.elements().all(scalar))
}

/// An id's key component: `[rank][ordered bytes]`, so ids sort by the
/// tree's order within a type and by type rank across types, and `1`
/// (int32) and `1` (int64) are one id. Strings and identifiers are their
/// bytes; integers big-endian with the sign flipped; binary its length,
/// subtype and bytes; timestamps seconds (sign flipped) then nanos.
pub fn id_key(v: Value<'_>, out: &mut [u8]) -> Option<usize> {
    let mut w = 0usize;
    let mut put = |b: &[u8], w: &mut usize| -> Option<()> {
        let d = out.get_mut(*w..*w + b.len())?;
        for (x, y) in d.iter_mut().zip(b) {
            *x = *y;
        }
        *w += b.len();
        Some(())
    };
    put(&[type_rank(v.ty())], &mut w)?;
    match v.ty() {
        // A compound id: its components' ordered, self-delimiting keys,
        // so the first components of an id are a prefix of its key.
        T_ARRAY => {
            for c in v.elements() {
                if !id_is_usable(c) || c.is_container() {
                    return None;
                }
                let room = out.get_mut(w..)?;
                w += doc_tree::value_key(c, room)?;
            }
        }
        doc_tree::T_STRING => put(v.as_str()?, &mut w)?,
        doc_tree::T_IDENTIFIER => put(v.as_identifier()?, &mut w)?,
        T_INTEGER => put(&((v.as_i64()? as u64) ^ (1 << 63)).to_be_bytes(), &mut w)?,
        doc_tree::T_BINARY => {
            let (st, data) = v.as_binary()?;
            put(&(data.len() as u32).to_be_bytes(), &mut w)?;
            put(&[st], &mut w)?;
            put(data, &mut w)?;
        }
        doc_tree::T_TIMESTAMP => {
            let (s, n) = v.as_timestamp()?;
            put(&((s as u64) ^ (1 << 63)).to_be_bytes(), &mut w)?;
            put(&n.to_be_bytes(), &mut w)?;
        }
        _ => return None,
    }
    Some(w)
}

/// The id an id key encodes, written as a value — [`id_key`]'s inverse.
/// An integer comes back 64 bits wide (the key does not keep the width:
/// ids of either width are one id).
pub fn id_from_key(k: &[u8], w: &mut Writer<'_>) -> Option<()> {
    let (&rank, rest) = k.split_first()?;
    let be8 = |b: &[u8]| -> Option<u64> { Some(u64::from_be_bytes(b.get(..8)?.try_into().ok()?)) };
    match rank {
        r if r == type_rank(T_ARRAY) => {
            w.begin_array().ok()?;
            let mut at = 0usize;
            while at < rest.len() {
                at += doc_tree::key_value(rest.get(at..)?, w)?;
            }
            w.end().ok()
        }
        r if r == type_rank(doc_tree::T_STRING) => w.string(rest).ok(),
        r if r == type_rank(doc_tree::T_IDENTIFIER) => w.identifier(rest).ok(),
        r if r == type_rank(T_INTEGER) => {
            (rest.len() == 8).then_some(())?;
            w.int((be8(rest)? ^ (1 << 63)) as i64).ok()
        }
        r if r == type_rank(doc_tree::T_BINARY) => {
            let n = u32::from_be_bytes(rest.get(..4)?.try_into().ok()?) as usize;
            let st = *rest.get(4)?;
            let data = rest.get(5..)?;
            (data.len() == n).then_some(())?;
            w.binary(st, data).ok()
        }
        r if r == type_rank(doc_tree::T_TIMESTAMP) => {
            (rest.len() == 12).then_some(())?;
            let secs = (be8(rest)? ^ (1 << 63)) as i64;
            let nanos = u32::from_be_bytes(rest.get(8..12)?.try_into().ok()?);
            w.timestamp(secs, nanos).ok()
        }
        _ => None,
    }
}

// ── Path keys (`CODEC_PATH_JSON`) ─────────────────────────────────────

/// Is `c` a usable path-key component: a non-empty string holding no `/`
/// and no NUL (a Kubernetes name is one)?
#[allow(
    clippy::manual_contains,
    reason = "slice::contains links core's memchr, which PIC modules cannot resolve"
)]
fn path_component(c: Value<'_>) -> Option<&[u8]> {
    let s = c.as_str()?;
    (!s.is_empty() && !s.iter().any(|b| *b == b'/' || *b == 0)).then_some(s)
}

/// `/<collection>/` then `id`'s components, each followed by `/` when
/// `open` (a prefix) and joined by `/` otherwise, into `out`.
#[allow(
    clippy::manual_contains,
    reason = "slice::contains links core's memchr, which PIC modules cannot resolve"
)]
fn path_write<'v>(
    collection: &[u8],
    parts: impl Iterator<Item = Value<'v>>,
    open: bool,
    out: &mut [u8],
) -> Option<usize> {
    let mut w = 0usize;
    let mut put = |b: &[u8], w: &mut usize| -> Option<()> {
        let d = out.get_mut(*w..*w + b.len())?;
        for (x, y) in d.iter_mut().zip(b) {
            *x = *y;
        }
        *w += b.len();
        Some(())
    };
    if collection.is_empty() || collection.iter().any(|b| *b == b'/') {
        return None;
    }
    put(b"/", &mut w)?;
    put(collection, &mut w)?;
    put(b"/", &mut w)?;
    let mut first = true;
    for c in parts {
        if !first && !open {
            put(b"/", &mut w)?;
        }
        put(path_component(c)?, &mut w)?;
        if open {
            put(b"/", &mut w)?;
        }
        first = false;
    }
    (!first || open).then_some(w)
}

/// A document's key under [`CODEC_PATH_JSON`](super::doc_catalog::CODEC_PATH_JSON):
/// `/<collection>/<id>`, a compound id's components joined by `/`
/// (`/pods/default/web-0`). `None` for an id that is not strings a path
/// can hold.
pub fn path_key(collection: &[u8], id: Value<'_>, out: &mut [u8]) -> Option<usize> {
    if id.ty() == T_ARRAY {
        return path_write(collection, id.elements(), false, out);
    }
    path_write(collection, core::iter::once(id), false, out)
}

/// The keys under a collection, or under an id's leading components
/// (`leading`, an array; empty: the whole collection): `/<collection>/`
/// or `/pods/default/`.
pub fn path_prefix(collection: &[u8], leading: Option<Value<'_>>, out: &mut [u8]) -> Option<usize> {
    match leading {
        Some(l) if l.ty() == T_ARRAY => path_write(collection, l.elements(), true, out),
        Some(l) => path_write(collection, core::iter::once(l), true, out),
        None => path_write(collection, core::iter::empty(), true, out),
    }
}

/// The id a path key names, written as a value: a string, or (`compound`)
/// an array of `parts` strings. [`path_key`]'s inverse; `None` for a key
/// that is not the collection's or has another shape.
pub fn path_id(collection: &[u8], key: &[u8], compound: usize, w: &mut Writer<'_>) -> Option<()> {
    let rest = key
        .strip_prefix(b"/")?
        .strip_prefix(collection)?
        .strip_prefix(b"/")?;
    let mut parts = rest.split(|b| *b == b'/');
    if compound < 2 {
        let only = parts.next()?;
        return (parts.next().is_none() && !only.is_empty()).then(|| w.string(only).ok())?;
    }
    w.begin_array().ok()?;
    let mut n = 0usize;
    for p in parts {
        if p.is_empty() {
            return None;
        }
        w.string(p).ok()?;
        n += 1;
    }
    (n == compound).then_some(())?;
    w.end().ok()
}

/// A document's id key, from the values at the collection's id paths: a
/// plain id's [`id_key`], or a compound id's (the array of its values).
/// `None` when a component is missing or not usable.
pub fn id_key_of(desc: &Descriptor<'_>, doc: Value<'_>, out: &mut [u8]) -> Option<usize> {
    if !desc.compound_id() {
        let id = lookup(doc, desc.id_path())?;
        return id_is_usable(id).then(|| id_key(id, out))?;
    }
    *out.first_mut()? = type_rank(T_ARRAY);
    let mut w = 1usize;
    for p in desc.id_paths() {
        let c = lookup(doc, p)?;
        if c.is_container() || !id_is_usable(c) {
            return None;
        }
        w += doc_tree::value_key(c, out.get_mut(w..)?)?;
    }
    Some(w)
}

/// A document's id as a value into `w`: the value at the id path, or a
/// compound id's array of values.
pub fn doc_id(desc: &Descriptor<'_>, doc: Value<'_>, w: &mut Writer<'_>) -> Option<()> {
    if !desc.compound_id() {
        return w.value(lookup(doc, desc.id_path())?).ok();
    }
    w.begin_array().ok()?;
    for p in desc.id_paths() {
        w.value(lookup(doc, p)?).ok()?;
    }
    w.end().ok()
}

/// Give the document in `bufs` the id it lacks: each id path it does not
/// set takes `from`'s value there (a replace that states no id keeps the
/// stored one).
pub fn carry_id(
    desc: &Descriptor<'_>,
    from: Value<'_>,
    bufs: &mut Buffers<'_>,
) -> Result<(), OpError> {
    for p in desc.id_paths() {
        if lookup(bufs.current(), p).is_none() {
            if let Some(v) = lookup(from, p) {
                bufs.edit(p, Edit::Set(v))?;
            }
        }
    }
    Ok(())
}

/// Set an id — a plain id's value, or a compound id's array spread over
/// the id paths — on the document in `bufs`.
pub fn set_id(desc: &Descriptor<'_>, id: Value<'_>, bufs: &mut Buffers<'_>) -> Result<(), OpError> {
    if !desc.compound_id() {
        return bufs.edit(desc.id_path(), Edit::Set(id));
    }
    let malformed = OpError::from(TreeError::BadFormat);
    if id.ty() != T_ARRAY || id.count() as usize != desc.id_paths().count() {
        return Err(malformed);
    }
    for (p, v) in desc.id_paths().zip(id.elements()) {
        bufs.edit(p, Edit::Set(v))?;
    }
    Ok(())
}

// ── Planning ──────────────────────────────────────────────────────────

/// A filter an index can answer: an equality the filter requires (the
/// whole filter, or a conjunct of a top-level AND) on the one path of a
/// READY index, with a scalar value. `(index id, unique, values key
/// length)` with the values key in `vals`; a unique index is preferred.
/// The documents it yields are candidates: the filter is still applied to
/// each. `None` for a collection with a shard path (its documents' keys
/// need the shard value).
pub fn index_plan(
    desc: &Descriptor<'_>,
    f: Filter<'_>,
    vals: &mut [u8],
) -> Option<(u32, bool, usize)> {
    if desc.shard_path().is_some() {
        return None;
    }
    let mut best: Option<(u32, bool, usize)> = None;
    let mut consider = |c: Filter<'_>, vals: &mut [u8]| {
        let Filter::Cmp {
            op: CMP_EQ,
            path,
            value,
            ..
        } = c
        else {
            return;
        };
        for ix in desc.indexes() {
            if !ix.ready() || ix.paths().count() != 1 {
                continue;
            }
            if ix.paths().next().map(|p| p.bytes()) != Some(path.bytes()) {
                continue;
            }
            if best.is_some_and(|(_, u, _)| u || !ix.unique()) {
                continue;
            }
            if value.is_container() {
                return;
            }
            if let Some(n) = doc_tree::value_key(value, vals) {
                best = Some((ix.id, ix.unique(), n));
            }
        }
    };
    match f {
        Filter::Group(true, _, _) => {
            for c in f.children() {
                consider(c, vals);
            }
        }
        c => consider(c, vals),
    }
    best
}
