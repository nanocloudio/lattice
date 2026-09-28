//! The MongoDB dialect of the document model.
//!
//! A dialect translates and renders; the document executor executes
//! (`doc_exec.rs`). This file is the Mongo dialect's translation, as pure
//! functions `doc_edge_anchor` calls and the host tests drive:
//!
//! - **Values.** BSON ↔ the canonical tree (`doc_tree.rs`). `int32` and
//!   `int64` are integers of their width, `double` a binary64, `string` a
//!   string, `ObjectId` a 12-byte identifier, a UTC `datetime` a
//!   timestamp (milliseconds), `binary` a binary of its subtype, `null`,
//!   `bool`, documents and arrays themselves. Types the tree has no
//!   canonical form for — `regex`, `javascript`, `decimal128`, the
//!   internal `timestamp`, `minKey`/`maxKey`, the deprecated types — are
//!   refused by name rather than approximated. Rendering back: a
//!   timestamp keeps its milliseconds (finer precision other dialects
//!   wrote is truncated), a decimal literal becomes the nearest double,
//!   an identifier that is not 12 bytes a binary.
//! - **Queries.** The query language → canonical filters. A path
//!   `{a.b: v}` reaches through arrays (`CMP_ANY`), as MongoDB's does.
//!   `{a: null}` matches null or missing, and `$ne`/`$nin` are the
//!   negations of their positives — MongoDB's null rules, spelled in the
//!   canonical nodes. Operators with no canonical meaning (`$regex`,
//!   `$elemMatch`, `$size`, `$type`, `$all`, `$mod`, `$expr`, `$where`,
//!   `$text`, geo) are refused by name.
//! - **Updates.** Operator documents → canonical mutations: `$set`,
//!   `$unset` (MongoDB nulls array elements in place), `$inc`, `$mul`,
//!   `$min`, `$max`, `$push`/`$addToSet` with `$each`, `$pull` by
//!   equality, `$pop`. `$setOnInsert` seeds an upsert. A document with no
//!   operators is a replacement. `$rename`, `$currentDate`, the
//!   positional operators, `$push` modifiers and pipeline updates are
//!   refused by name.
//! - **Projections.** `{a: 1}` includes (with `_id` unless `_id: 0`),
//!   `{a: 0}` excludes.
//! - **Collections.** Mongo's: `_id` identifies, nothing else declared;
//!   created implicitly by the first write.
//! - **Identifiers.** A document inserted without `_id` gets an ObjectId
//!   minted here, as a MongoDB server does.
//!
//! Mounts `doc_server_codec` (OP_MSG framing, the BSON reader, the reply
//! shapes) and `doc_exec` — one mount chain, so the anchor mounts only
//! this.

#![allow(
    dead_code,
    reason = "shared via #[path] into the anchor module and host tests; each consumer uses a subset"
)]

#[path = "doc_server_codec.rs"]
pub mod codec;

#[path = "doc_exec.rs"]
pub mod doc_exec;

use codec::mongo_core::bson;
use doc_exec::doc_tree::{
    self, decimal_text_to_f64, PathWriter, TreeError, Value, Writer, T_ARRAY, T_BINARY, T_BOOLEAN,
    T_DECIMAL, T_IDENTIFIER, T_INTEGER, T_NULL, T_OBJECT, T_STRING, T_TIMESTAMP, WIDTH_32,
};
use doc_exec::*;

// The SDK's pointer-free name tables (`runtime/names.rs`): the module's
// crate root includes them with the runtime, the host tests directly.
use crate::{name_table_len, NameTable};

pub use codec::{
    elements, find, int_value, str_value, truthy, BsonError, B_DATETIME, B_NULL, B_OBJECT_ID,
};

/// The MongoDB error codes the dialect answers.
pub const CODE_BAD_VALUE: i32 = 2;
pub const CODE_FAILED_TO_PARSE: i32 = 9;
pub const CODE_NAMESPACE_NOT_FOUND: i32 = 26;
pub const CODE_NAMESPACE_EXISTS: i32 = 48;
pub const CODE_MAX_TIME_EXPIRED: i32 = 50;
pub const CODE_COMMAND_NOT_FOUND: i32 = 59;
pub const CODE_IMMUTABLE_FIELD: i32 = 66;
pub const CODE_HOST_UNREACHABLE: i32 = 6;
pub const CODE_WRITE_CONFLICT: i32 = 112;
pub const CODE_CURSOR_NOT_FOUND: i32 = 43;
pub const CODE_BSON_TOO_LARGE: i32 = 10334;
pub const CODE_DUPLICATE_KEY: i32 = 11000;
pub const CODE_TYPE_MISMATCH: i32 = 14;
pub const CODE_OPERATION_FAILED: i32 = 96;

/// A translation's refusal: a MongoDB error code and its message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub code: i32,
    pub msg: &'static [u8],
}

// Messages: each its own function (PIC relocates no slice tables).
#[inline(never)]
pub fn msg_unsupported_type() -> &'static [u8] {
    b"lattice: this BSON type has no canonical document form"
}
#[inline(never)]
pub fn msg_unsupported_operator() -> &'static [u8] {
    b"lattice: unsupported query operator"
}
#[inline(never)]
pub fn msg_unsupported_update() -> &'static [u8] {
    b"lattice: unsupported update operator"
}
#[inline(never)]
pub fn msg_malformed() -> &'static [u8] {
    b"malformed BSON"
}
#[inline(never)]
pub fn msg_too_large() -> &'static [u8] {
    b"object to insert too large"
}
#[inline(never)]
pub fn msg_mixed_update() -> &'static [u8] {
    b"an update mixes operators and fields"
}
#[inline(never)]
pub fn msg_bad_path() -> &'static [u8] {
    b"a field path is empty or too deep"
}
#[inline(never)]
pub fn msg_empty_update() -> &'static [u8] {
    b"'update' has no operators"
}

fn refuse(code: i32, msg: &'static [u8]) -> Refusal {
    Refusal { code, msg }
}

fn malformed() -> Refusal {
    refuse(CODE_FAILED_TO_PARSE, msg_malformed())
}

fn too_large() -> Refusal {
    refuse(CODE_BSON_TOO_LARGE, msg_too_large())
}

impl From<TreeError> for Refusal {
    fn from(e: TreeError) -> Self {
        match e {
            TreeError::Overflow | TreeError::TooDeep => too_large(),
            _ => malformed(),
        }
    }
}

// ── BSON reading ──────────────────────────────────────────────────────

impl From<BsonError> for Refusal {
    fn from(e: BsonError) -> Self {
        match e {
            BsonError::Unsupported => refuse(CODE_BAD_VALUE, msg_unsupported_type()),
            BsonError::Malformed => malformed(),
        }
    }
}

fn rd_i32(b: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn rd_i64(b: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

// ── BSON → tree ───────────────────────────────────────────────────────

/// Write a BSON value as a tree value.
pub fn value_to_tree(ty: u8, v: &[u8], w: &mut Writer<'_>) -> Result<(), Refusal> {
    match ty {
        bson::DOUBLE => w.f64(f64::from_bits(rd_i64(v, 0).ok_or_else(malformed)? as u64))?,
        bson::STRING => w.string(str_value(v).ok_or_else(malformed)?)?,
        bson::DOC => doc_to_tree(v, w)?,
        bson::ARRAY => {
            w.begin_array()?;
            for e in elements(v)? {
                let (_, t, x) = e?;
                value_to_tree(t, x, w)?;
            }
            w.end()?;
        }
        bson::BINARY => {
            let n =
                usize::try_from(rd_i32(v, 0).ok_or_else(malformed)?).map_err(|_| malformed())?;
            let st = *v.get(4).ok_or_else(malformed)?;
            w.binary(st, v.get(5..5 + n).ok_or_else(malformed)?)?;
        }
        B_OBJECT_ID => w.identifier(v)?,
        bson::BOOL => w.boolean(v.first() == Some(&1))?,
        B_DATETIME => {
            let ms = rd_i64(v, 0).ok_or_else(malformed)?;
            w.timestamp(
                ms.div_euclid(1000),
                (ms.rem_euclid(1000) * 1_000_000) as u32,
            )?;
        }
        B_NULL => w.null()?,
        bson::INT32 => w.int32(rd_i32(v, 0).ok_or_else(malformed)?)?,
        bson::INT64 => w.int(rd_i64(v, 0).ok_or_else(malformed)?)?,
        _ => return Err(refuse(CODE_BAD_VALUE, msg_unsupported_type())),
    }
    Ok(())
}

/// Write a BSON document as a tree object.
pub fn doc_to_tree(doc: &[u8], w: &mut Writer<'_>) -> Result<(), Refusal> {
    w.begin_object()?;
    for e in elements(doc)? {
        let (name, ty, v) = e?;
        w.key(name)?;
        value_to_tree(ty, v, w)?;
    }
    w.end()?;
    Ok(())
}

/// A BSON document as a tree, into `out`; its length.
pub fn doc_tree_bytes(doc: &[u8], out: &mut [u8]) -> Result<usize, Refusal> {
    let mut w = Writer::new(out);
    doc_to_tree(doc, &mut w)?;
    let n = w.finish()?;
    // A document from a client may repeat a key; the tree refuses it.
    doc_tree::validate(out.get(..n).ok_or_else(malformed)?).map_err(|_| malformed())?;
    Ok(n)
}

/// A single BSON value as a tree value, into `out`.
pub fn value_tree_bytes(ty: u8, v: &[u8], out: &mut [u8]) -> Result<usize, Refusal> {
    let mut w = Writer::new(out);
    value_to_tree(ty, v, &mut w)?;
    let n = w.finish()?;
    doc_tree::validate(out.get(..n).ok_or_else(malformed)?).map_err(|_| malformed())?;
    Ok(n)
}

// ── Tree → BSON ───────────────────────────────────────────────────────

fn bput(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Result<(), Refusal> {
    let d = out.get_mut(*pos..*pos + b.len()).ok_or_else(too_large)?;
    d.copy_from_slice(b);
    *pos += b.len();
    Ok(())
}

fn index_name(i: usize, buf: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut v = i;
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
        buf[j] = tmp[k - 1 - j];
    }
    k
}

/// Write one element `[type][name\0][value]` for a tree value.
pub fn write_element(
    name: &[u8],
    v: Value<'_>,
    out: &mut [u8],
    pos: &mut usize,
) -> Result<(), Refusal> {
    if doc_tree::has_nul(name) {
        return Err(refuse(CODE_BAD_VALUE, msg_unsupported_type()));
    }
    let head = |ty: u8, out: &mut [u8], pos: &mut usize| -> Result<(), Refusal> {
        bput(out, pos, &[ty])?;
        bput(out, pos, name)?;
        bput(out, pos, &[0])
    };
    match v.ty() {
        T_NULL => head(B_NULL, out, pos),
        T_BOOLEAN => {
            head(bson::BOOL, out, pos)?;
            bput(out, pos, &[(v.as_bool() == Some(true)) as u8])
        }
        T_INTEGER => {
            let i = v.as_i64().ok_or_else(malformed)?;
            match (v.int_width(), i32::try_from(i)) {
                (Some(WIDTH_32), Ok(x)) => {
                    head(bson::INT32, out, pos)?;
                    bput(out, pos, &x.to_le_bytes())
                }
                _ => {
                    head(bson::INT64, out, pos)?;
                    bput(out, pos, &i.to_le_bytes())
                }
            }
        }
        T_DECIMAL => {
            let f = match v.as_f64() {
                Some(f) => f,
                None => decimal_text_to_f64(v.decimal_text().unwrap_or(b"0")),
            };
            head(bson::DOUBLE, out, pos)?;
            bput(out, pos, &f.to_bits().to_le_bytes())
        }
        T_STRING => {
            let s = v.as_str().ok_or_else(malformed)?;
            head(bson::STRING, out, pos)?;
            bput(out, pos, &((s.len() + 1) as i32).to_le_bytes())?;
            bput(out, pos, s)?;
            bput(out, pos, &[0])
        }
        T_BINARY => {
            let (st, d) = v.as_binary().ok_or_else(malformed)?;
            head(bson::BINARY, out, pos)?;
            bput(out, pos, &(d.len() as i32).to_le_bytes())?;
            bput(out, pos, &[st])?;
            bput(out, pos, d)
        }
        T_TIMESTAMP => {
            let (s, n) = v.as_timestamp().ok_or_else(malformed)?;
            let ms = s
                .saturating_mul(1000)
                .saturating_add((n / 1_000_000) as i64);
            head(B_DATETIME, out, pos)?;
            bput(out, pos, &ms.to_le_bytes())
        }
        T_IDENTIFIER => {
            let id = v.as_identifier().ok_or_else(malformed)?;
            if id.len() == 12 {
                head(B_OBJECT_ID, out, pos)?;
                bput(out, pos, id)
            } else {
                head(bson::BINARY, out, pos)?;
                bput(out, pos, &(id.len() as i32).to_le_bytes())?;
                bput(out, pos, &[0])?;
                bput(out, pos, id)
            }
        }
        T_ARRAY => {
            head(bson::ARRAY, out, pos)?;
            let start = *pos;
            bput(out, pos, &[0, 0, 0, 0])?;
            for (i, e) in v.elements().enumerate() {
                let mut nb = [0u8; 20];
                let nl = index_name(i, &mut nb);
                write_element(&nb[..nl], e, out, pos)?;
            }
            close_doc(out, pos, start)
        }
        T_OBJECT => {
            head(bson::DOC, out, pos)?;
            write_doc_body(v, out, pos)
        }
        _ => Err(malformed()),
    }
}

fn close_doc(out: &mut [u8], pos: &mut usize, start: usize) -> Result<(), Refusal> {
    bput(out, pos, &[0])?;
    let len = (*pos - start) as i32;
    out.get_mut(start..start + 4)
        .ok_or_else(too_large)?
        .copy_from_slice(&len.to_le_bytes());
    Ok(())
}

fn write_doc_body(v: Value<'_>, out: &mut [u8], pos: &mut usize) -> Result<(), Refusal> {
    let start = *pos;
    bput(out, pos, &[0, 0, 0, 0])?;
    for (k, m) in v.members() {
        write_element(k, m, out, pos)?;
    }
    close_doc(out, pos, start)
}

/// A tree object as a BSON document into `out[pos..]`.
pub fn tree_to_bson(v: Value<'_>, out: &mut [u8], pos: &mut usize) -> Result<(), Refusal> {
    if v.ty() != T_OBJECT {
        return Err(malformed());
    }
    write_doc_body(v, out, pos)
}

// ── Paths ─────────────────────────────────────────────────────────────

/// A dotted field path (`a.b.0`) as an encoded path. An empty segment or
/// a `$` segment (the positional operators) is refused.
pub fn dotted_path(name: &[u8], out: &mut [u8]) -> Result<usize, Refusal> {
    let mut w = PathWriter::new(out).map_err(|_| too_large())?;
    if name.is_empty() {
        return Err(refuse(CODE_BAD_VALUE, msg_bad_path()));
    }
    for seg in name.split(|c| *c == b'.') {
        if seg.is_empty() || seg.first() == Some(&b'$') {
            return Err(refuse(CODE_BAD_VALUE, msg_bad_path()));
        }
        w.push(seg)
            .map_err(|_| refuse(CODE_BAD_VALUE, msg_bad_path()))?;
    }
    Ok(w.finish())
}

// ── Filters ───────────────────────────────────────────────────────────

/// Builds a canonical filter program.
struct Prog<'a> {
    out: &'a mut [u8],
    pos: usize,
}

impl Prog<'_> {
    fn byte(&mut self, b: u8) -> Result<(), Refusal> {
        bput(self.out, &mut self.pos, &[b])
    }

    /// Open a group; its count is patched by `close`.
    fn group(&mut self, kind: u8) -> Result<usize, Refusal> {
        self.byte(kind)?;
        let at = self.pos;
        self.byte(0)?;
        Ok(at)
    }

    fn close(&mut self, at: usize, n: usize) -> Result<(), Refusal> {
        let n = u8::try_from(n).map_err(|_| too_large())?;
        *self.out.get_mut(at).ok_or_else(too_large)? = n;
        Ok(())
    }

    fn path(&mut self, name: &[u8]) -> Result<(), Refusal> {
        let mut p = [0u8; 1024];
        let n = dotted_path(name, &mut p)?;
        bput(self.out, &mut self.pos, &(n as u16).to_le_bytes())?;
        bput(self.out, &mut self.pos, &p[..n])
    }

    fn value(&mut self, ty: u8, v: &[u8]) -> Result<(), Refusal> {
        let rest = self.out.get_mut(self.pos..).ok_or_else(too_large)?;
        let n = value_tree_bytes(ty, v, rest)?;
        self.pos += n;
        Ok(())
    }

    fn cmp(&mut self, op: u8, name: &[u8], ty: u8, v: &[u8]) -> Result<(), Refusal> {
        self.byte(F_CMP)?;
        self.byte(op | CMP_ANY)?;
        self.path(name)?;
        self.value(ty, v)
    }

    fn exists(&mut self, want: bool, name: &[u8]) -> Result<(), Refusal> {
        self.byte(F_EXISTS)?;
        self.byte(want as u8)?;
        self.path(name)
    }

    /// `{name: v}`: equality, with `null` also matching a missing field.
    fn eq(&mut self, name: &[u8], ty: u8, v: &[u8]) -> Result<(), Refusal> {
        if ty == B_NULL {
            let g = self.group(F_OR)?;
            self.cmp(CMP_EQ, name, ty, v)?;
            self.exists(false, name)?;
            return self.close(g, 2);
        }
        if ty == bson::DOC && is_operator_doc(v)? {
            return Err(refuse(CODE_BAD_VALUE, msg_unsupported_operator()));
        }
        self.cmp(CMP_EQ, name, ty, v)
    }

    /// `{name: {$in: [...]}}`; a null in the set also matches missing.
    fn in_set(&mut self, name: &[u8], set: &[u8]) -> Result<(), Refusal> {
        let mut has_null = false;
        for e in elements(set)? {
            let (_, t, x) = e?;
            if t == B_NULL {
                has_null = true;
            }
            if t == bson::DOC && is_operator_doc(x)? {
                return Err(refuse(CODE_BAD_VALUE, msg_unsupported_operator()));
            }
        }
        let g = if has_null {
            Some(self.group(F_OR)?)
        } else {
            None
        };
        self.byte(F_IN)?;
        self.byte(1)?;
        self.path(name)?;
        self.value(bson::ARRAY, set)?;
        if let Some(g) = g {
            self.exists(false, name)?;
            self.close(g, 2)?;
        }
        Ok(())
    }

    /// One `{$op: operand}` of a field's operator document.
    fn operator(&mut self, name: &[u8], op: &[u8], ty: u8, v: &[u8]) -> Result<(), Refusal> {
        let cmp = |o: &[u8]| -> Option<u8> {
            match o {
                b"$gt" => Some(CMP_GT),
                b"$gte" => Some(CMP_GTE),
                b"$lt" => Some(CMP_LT),
                b"$lte" => Some(CMP_LTE),
                _ => None,
            }
        };
        if let Some(c) = cmp(op) {
            return self.cmp(c, name, ty, v);
        }
        match op {
            b"$eq" => self.eq(name, ty, v),
            b"$ne" => {
                self.byte(F_NOT)?;
                self.eq(name, ty, v)
            }
            b"$in" if ty == bson::ARRAY => self.in_set(name, v),
            b"$nin" if ty == bson::ARRAY => {
                self.byte(F_NOT)?;
                self.in_set(name, v)
            }
            b"$exists" => self.exists(truthy(ty, v), name),
            b"$not" if ty == bson::DOC && is_operator_doc(v)? => {
                self.byte(F_NOT)?;
                self.field(name, bson::DOC, v)
            }
            _ => Err(refuse(CODE_BAD_VALUE, msg_unsupported_operator())),
        }
    }

    /// `{name: cond}`.
    fn field(&mut self, name: &[u8], ty: u8, v: &[u8]) -> Result<(), Refusal> {
        if ty == bson::DOC && is_operator_doc(v)? {
            let n = elements(v)?.count();
            let g = if n == 1 {
                None
            } else {
                Some(self.group(F_AND)?)
            };
            for e in elements(v)? {
                let (op, t, x) = e?;
                self.operator(name, op, t, x)?;
            }
            if let Some(g) = g {
                self.close(g, n)?;
            }
            return Ok(());
        }
        self.eq(name, ty, v)
    }

    /// A whole query document.
    fn query(&mut self, doc: &[u8], depth: usize) -> Result<(), Refusal> {
        if depth > 8 {
            return Err(too_large());
        }
        let n = elements(doc)?.count();
        if n == 0 {
            return self.byte(F_TRUE);
        }
        let g = if n == 1 {
            None
        } else {
            Some(self.group(F_AND)?)
        };
        for e in elements(doc)? {
            let (name, ty, v) = e?;
            match name {
                b"$and" | b"$or" | b"$nor" if ty == bson::ARRAY => {
                    if name == b"$nor" {
                        self.byte(F_NOT)?;
                    }
                    let kind = if name == b"$and" { F_AND } else { F_OR };
                    let cnt = elements(v)?.count();
                    let at = self.group(kind)?;
                    for c in elements(v)? {
                        let (_, ct, cv) = c?;
                        if ct != bson::DOC {
                            return Err(malformed());
                        }
                        self.query(cv, depth + 1)?;
                    }
                    self.close(at, cnt)?;
                }
                _ if name.first() == Some(&b'$') => {
                    return Err(refuse(CODE_BAD_VALUE, msg_unsupported_operator()));
                }
                _ => self.field(name, ty, v)?,
            }
        }
        if let Some(g) = g {
            self.close(g, n)?;
        }
        Ok(())
    }
}

/// Is a document an operator document (its first key begins with `$`)?
fn is_operator_doc(v: &[u8]) -> Result<bool, Refusal> {
    Ok(match elements(v)?.next() {
        Some(e) => e?.0.first() == Some(&b'$'),
        None => false,
    })
}

/// A query document as a canonical filter into `out`; its length.
pub fn translate_filter(query: &[u8], out: &mut [u8]) -> Result<usize, Refusal> {
    let mut p = Prog { out, pos: 0 };
    p.query(query, 0)?;
    let n = p.pos;
    if !filter_is_valid(&p.out[..n]) {
        return Err(too_large());
    }
    Ok(n)
}

/// The `_id` a query names exactly (`{_id: v}` with a scalar `v`), for a
/// point read.
pub fn exact_id(query: &[u8]) -> Result<Option<(u8, &[u8])>, Refusal> {
    let mut it = elements(query)?;
    let Some(first) = it.next() else {
        return Ok(None);
    };
    let (name, ty, v) = first?;
    if name != b"_id" || it.next().is_some() {
        return Ok(None);
    }
    let scalar = !matches!(ty, bson::DOC | bson::ARRAY | B_NULL);
    Ok(scalar.then_some((ty, v)))
}

// ── Updates ───────────────────────────────────────────────────────────

/// Is an update document an operator update (vs. a replacement)?
pub fn is_operator_update(u: &[u8]) -> Result<bool, Refusal> {
    let mut ops = 0;
    let mut fields = 0;
    for e in elements(u)? {
        let (name, _, _) = e?;
        if name.first() == Some(&b'$') {
            ops += 1;
        } else {
            fields += 1;
        }
    }
    if ops > 0 && fields > 0 {
        return Err(refuse(CODE_FAILED_TO_PARSE, msg_mixed_update()));
    }
    Ok(ops > 0)
}

struct Muts<'a> {
    out: &'a mut [u8],
    pos: usize,
    n: u16,
}

impl Muts<'_> {
    fn one(
        &mut self,
        kind: u8,
        sub: u8,
        name: &[u8],
        operand: Option<(u8, &[u8])>,
    ) -> Result<(), Refusal> {
        if self.n as usize >= MAX_MUTATIONS {
            return Err(too_large());
        }
        bput(self.out, &mut self.pos, &[kind, sub])?;
        let mut p = [0u8; 1024];
        let n = dotted_path(name, &mut p)?;
        bput(self.out, &mut self.pos, &(n as u16).to_le_bytes())?;
        bput(self.out, &mut self.pos, &p[..n])?;
        if let Some((ty, v)) = operand {
            let rest = self.out.get_mut(self.pos..).ok_or_else(too_large)?;
            let n = value_tree_bytes(ty, v, rest)?;
            self.pos += n;
        }
        self.n += 1;
        Ok(())
    }

    /// `$push`/`$addToSet` operand: a value, or `{$each: [...]}`.
    fn each(&mut self, sub: u8, name: &[u8], ty: u8, v: &[u8]) -> Result<(), Refusal> {
        if ty == bson::DOC && is_operator_doc(v)? {
            for e in elements(v)? {
                let (k, t, x) = e?;
                if k != b"$each" || t != bson::ARRAY {
                    return Err(refuse(CODE_BAD_VALUE, msg_unsupported_update()));
                }
                for item in elements(x)? {
                    let (_, it, iv) = item?;
                    self.one(MUT_ARRAY, sub, name, Some((it, iv)))?;
                }
            }
            return Ok(());
        }
        self.one(MUT_ARRAY, sub, name, Some((ty, v)))
    }
}

/// An operator update as canonical mutations into `out`; its length, or
/// `None` when the update has no mutations (only `$setOnInsert`).
pub fn translate_update(u: &[u8], out: &mut [u8]) -> Result<Option<usize>, Refusal> {
    let mut m = Muts { out, pos: 2, n: 0 };
    let mut any = false;
    for e in elements(u)? {
        let (op, ty, v) = e?;
        any = true;
        if op == b"$setOnInsert" {
            continue;
        }
        if ty != bson::DOC {
            return Err(refuse(CODE_FAILED_TO_PARSE, msg_malformed()));
        }
        for f in elements(v)? {
            let (name, t, x) = f?;
            match op {
                b"$set" => m.one(MUT_PATH_SET, 0, name, Some((t, x)))?,
                b"$unset" => m.one(MUT_PATH_UNSET, UNSET_NULL_ELEMENT, name, None)?,
                b"$inc" => m.one(MUT_NUMERIC, NUM_INC, name, Some((t, x)))?,
                b"$mul" => m.one(MUT_NUMERIC, NUM_MUL, name, Some((t, x)))?,
                b"$min" => m.one(MUT_NUMERIC, NUM_MIN, name, Some((t, x)))?,
                b"$max" => m.one(MUT_NUMERIC, NUM_MAX, name, Some((t, x)))?,
                b"$push" => m.each(ARR_PUSH, name, t, x)?,
                b"$addToSet" => m.each(ARR_ADD_TO_SET, name, t, x)?,
                b"$pull" => {
                    if t == bson::DOC && is_operator_doc(x)? {
                        return Err(refuse(CODE_BAD_VALUE, msg_unsupported_update()));
                    }
                    m.one(MUT_ARRAY, ARR_PULL, name, Some((t, x)))?
                }
                b"$pop" => {
                    let sub = match int_value(t, x) {
                        Some(1) => ARR_POP_LAST,
                        Some(-1) => ARR_POP_FIRST,
                        _ => return Err(refuse(CODE_FAILED_TO_PARSE, msg_malformed())),
                    };
                    m.one(MUT_ARRAY, sub, name, None)?
                }
                _ => return Err(refuse(CODE_FAILED_TO_PARSE, msg_unsupported_update())),
            }
        }
    }
    if !any {
        return Err(refuse(CODE_FAILED_TO_PARSE, msg_empty_update()));
    }
    if m.n == 0 {
        return Ok(None);
    }
    let n = m.n;
    m.out[..2].copy_from_slice(&n.to_le_bytes());
    let len = m.pos;
    if Mutations::parse(&m.out[..len]).is_none() {
        return Err(refuse(CODE_FAILED_TO_PARSE, msg_malformed()));
    }
    Ok(Some(len))
}

/// An upsert's seed: the query's top-level equalities (`{k: v}` and
/// `{k: {$eq: v}}`, dotted paths nested), then `$setOnInsert`'s fields,
/// as MongoDB builds the document an upsert inserts. Operators other
/// than `$eq` contribute nothing.
pub fn upsert_seed(
    query: &[u8],
    update: Option<&[u8]>,
    out: &mut [u8],
    scratch: &mut [u8],
) -> Result<usize, Refusal> {
    let half = scratch.len() / 2;
    let (a, b) = scratch.split_at_mut(half);
    let mut bufs = doc_exec::doc_ops::Buffers::new(a, b);
    let empty = [T_OBJECT, 0, 0, 0, 0, 0, 0, 0, 0];
    bufs.load(Value::trusted(&empty)).map_err(|_| too_large())?;
    let set = |name: &[u8],
               ty: u8,
               v: &[u8],
               bufs: &mut doc_exec::doc_ops::Buffers<'_>|
     -> Result<(), Refusal> {
        let mut vb = [0u8; 4096];
        let vn = value_tree_bytes(ty, v, &mut vb)?;
        let mut p = [0u8; 1024];
        let pn = dotted_path(name, &mut p)?;
        let path = doc_tree::Path::parse(&p[..pn]).map_err(|_| malformed())?;
        bufs.edit(
            path,
            doc_exec::doc_ops::Edit::Set(Value::trusted(&vb[..vn])),
        )
        .map_err(|_| refuse(CODE_BAD_VALUE, msg_bad_path()))
    };
    for e in elements(query)? {
        let (name, ty, v) = e?;
        if name.first() == Some(&b'$') {
            continue;
        }
        if ty == bson::DOC && is_operator_doc(v)? {
            if let Some((t, x)) = find(v, b"$eq")? {
                set(name, t, x, &mut bufs)?;
            }
            continue;
        }
        set(name, ty, v, &mut bufs)?;
    }
    if let Some(u) = update {
        if let Some((bson::DOC, soi)) = find(u, b"$setOnInsert")? {
            for f in elements(soi)? {
                let (name, t, x) = f?;
                set(name, t, x, &mut bufs)?;
            }
        }
    }
    let cur = bufs.current().bytes();
    out.get_mut(..cur.len())
        .ok_or_else(too_large)?
        .copy_from_slice(cur);
    Ok(cur.len())
}

// ── Projections ───────────────────────────────────────────────────────

/// A projection document as a canonical path list into `out`, or `None`
/// for no projection. `{a: 1}` includes `_id` too unless `{_id: 0}`.
pub fn translate_projection(p: &[u8], out: &mut [u8]) -> Result<Option<usize>, Refusal> {
    let mut include: Option<bool> = None;
    let mut id_off = false;
    let mut count = 0usize;
    for e in elements(p)? {
        let (name, ty, v) = e?;
        let on = truthy(ty, v);
        if name == b"_id" {
            id_off = !on;
            continue;
        }
        if !matches!(ty, bson::BOOL | bson::INT32 | bson::INT64 | bson::DOUBLE) {
            return Err(refuse(CODE_BAD_VALUE, msg_unsupported_operator()));
        }
        match include {
            None => include = Some(on),
            Some(i) if i != on => return Err(refuse(CODE_BAD_VALUE, msg_unsupported_operator())),
            _ => {}
        }
        count += 1;
    }
    let include = match include {
        Some(i) => i,
        // Only `_id` named: `{_id: 0}` excludes it, `{_id: 1}` keeps all.
        None if id_off => false,
        None => return Ok(None),
    };
    let with_id = include && !id_off;
    let without_id = !include && id_off;
    let n = count + (with_id || without_id) as usize;
    if n > MAX_PROJECTION_PATHS {
        return Err(too_large());
    }
    let mut pos = 0usize;
    bput(
        out,
        &mut pos,
        &[if include { MODE_INCLUDE } else { MODE_EXCLUDE }, n as u8],
    )?;
    let push = |name: &[u8], pos: &mut usize, out: &mut [u8]| -> Result<(), Refusal> {
        let mut pb = [0u8; 1024];
        let pn = dotted_path(name, &mut pb)?;
        bput(out, pos, &(pn as u16).to_le_bytes())?;
        bput(out, pos, &pb[..pn])
    };
    if with_id || without_id {
        push(b"_id", &mut pos, out)?;
    }
    for e in elements(p)? {
        let (name, _, _) = e?;
        if name != b"_id" {
            push(name, &mut pos, out)?;
        }
    }
    if PathList::parse(&out[..pos]).is_none() {
        return Err(malformed());
    }
    Ok(Some(pos))
}

// ── Identifiers ───────────────────────────────────────────────────────

/// An ObjectId: `[seconds:u32 BE][random:5][counter:u24 BE]`.
pub fn object_id(secs: u32, random: [u8; 5], counter: u32) -> [u8; 12] {
    let mut id = [0u8; 12];
    id[..4].copy_from_slice(&secs.to_be_bytes());
    id[4..9].copy_from_slice(&random);
    id[9..].copy_from_slice(&counter.to_be_bytes()[1..]);
    id
}

/// Write `doc` (BSON) as a tree with `_id` first: the document's own, or
/// `minted` when it has none — as a MongoDB server stores an insert.
pub fn insert_tree(doc: &[u8], minted: &[u8; 12], out: &mut [u8]) -> Result<usize, Refusal> {
    let mut w = Writer::new(out);
    w.begin_object()?;
    match find(doc, b"_id")? {
        Some((ty, v)) => {
            if matches!(ty, bson::ARRAY) {
                return Err(refuse(CODE_BAD_VALUE, msg_unsupported_type()));
            }
            w.key(b"_id")?;
            value_to_tree(ty, v, &mut w)?;
        }
        None => {
            w.key(b"_id")?;
            w.identifier(minted)?;
        }
    }
    for e in elements(doc)? {
        let (name, ty, v) = e?;
        if name == b"_id" {
            continue;
        }
        w.key(name)?;
        value_to_tree(ty, v, &mut w)?;
    }
    w.end()?;
    let n = w.finish()?;
    doc_tree::validate(out.get(..n).ok_or_else(malformed)?).map_err(|_| malformed())?;
    Ok(n)
}

// ── Answers ───────────────────────────────────────────────────────────

// An outcome's MongoDB message: one table (see `outcome_refusal`).
name_table!(
    OUTCOME_MESSAGES = [
        b"E11000 duplicate key error",
        b"ns not found",
        b"Collection already exists",
        b"WriteConflict: the document changed during the write",
        b"the update cannot apply to the document",
        b"object to insert too large",
        b"operation exceeded time limit",
        b"malformed BSON",
        b"the server is busy; retry",
        b"the store is unavailable",
    ]
);

/// The MongoDB error an executor outcome answers as. The choice is a code
/// and a table index — integers — and the message is looked up after:
/// a `match` returning message literals would lower to a table of
/// absolute pointers, which a module image cannot hold.
pub fn outcome_refusal(outcome: u8) -> Refusal {
    let (code, i) = match outcome {
        OUT_EXISTS => (CODE_DUPLICATE_KEY, 0),
        OUT_NO_COLLECTION => (CODE_NAMESPACE_NOT_FOUND, 1),
        OUT_COLLECTION_EXISTS => (CODE_NAMESPACE_EXISTS, 2),
        OUT_CONFLICT => (CODE_WRITE_CONFLICT, 3),
        OUT_INVALID => (CODE_BAD_VALUE, 4),
        OUT_TOO_LARGE => (CODE_BSON_TOO_LARGE, 5),
        OUT_TIMEOUT => (CODE_MAX_TIME_EXPIRED, 6),
        OUT_MALFORMED => (CODE_FAILED_TO_PARSE, 7),
        OUT_BUSY => (CODE_WRITE_CONFLICT, 8),
        _ => (CODE_HOST_UNREACHABLE, 9),
    };
    Refusal {
        code,
        msg: OUTCOME_MESSAGES.get(i),
    }
}

// ── Indexes ───────────────────────────────────────────────────────────

pub const CODE_INDEX_OPTIONS_CONFLICT: i32 = 85;
pub const CODE_INDEX_KEY_SPECS_CONFLICT: i32 = 86;
pub const CODE_INVALID_OPTIONS: i32 = 72;
pub const CODE_INDEX_NOT_FOUND: i32 = 27;

#[inline(never)]
pub fn msg_unsupported_index() -> &'static [u8] {
    b"lattice: only ascending indexes (key: 1), with `unique`, are supported"
}

/// One `createIndexes` spec (`{key: {a: 1, …}, name, unique}`) as a
/// canonical index body (the executor sets its id and state). Keys are
/// ascending (`1`); a descending, text, hashed or geo key, and the
/// options with no canonical meaning (`sparse`, `partialFilterExpression`,
/// `expireAfterSeconds`, `collation`, …), are refused by name.
pub fn translate_index_spec(spec: &[u8], out: &mut [u8]) -> Result<usize, Refusal> {
    let unsupported = || refuse(CODE_INVALID_OPTIONS, msg_unsupported_index());
    let mut unique = false;
    let mut name: Option<&[u8]> = None;
    let mut key: Option<&[u8]> = None;
    for e in elements(spec)? {
        let (n, ty, v) = e?;
        match n {
            b"key" if ty == bson::DOC => key = Some(v),
            b"name" if ty == bson::STRING => name = Some(str_value(v).ok_or_else(malformed)?),
            b"unique" => unique = truthy(ty, v),
            // Accepted and meaningless here: the index format version.
            b"v" => {}
            _ => return Err(unsupported()),
        }
    }
    let (Some(key), Some(name)) = (key, name) else {
        return Err(malformed());
    };
    let mut paths = [[0u8; 1024]; doc_exec::doc_catalog::MAX_INDEX_PATHS];
    let mut lens = [0usize; doc_exec::doc_catalog::MAX_INDEX_PATHS];
    let mut n = 0usize;
    for e in elements(key)? {
        let (field, ty, v) = e?;
        if int_value(ty, v) != Some(1) {
            return Err(unsupported());
        }
        let slot = paths.get_mut(n).ok_or_else(too_large)?;
        lens[n] = dotted_path(field, slot)?;
        n += 1;
    }
    if n == 0 {
        return Err(malformed());
    }
    let mut refs: [&[u8]; doc_exec::doc_catalog::MAX_INDEX_PATHS] =
        [&[]; doc_exec::doc_catalog::MAX_INDEX_PATHS];
    for i in 0..n {
        refs[i] = &paths[i][..lens[i]];
    }
    let flags = if unique {
        doc_exec::doc_catalog::INDEX_UNIQUE
    } else {
        0
    };
    doc_exec::doc_catalog::encode_index(
        out,
        0,
        flags,
        doc_exec::doc_catalog::INDEX_BUILDING,
        name,
        &refs[..n],
    )
    .ok_or_else(too_large)
}

/// A path as a dotted field name.
fn write_dotted(p: doc_tree::Path<'_>, out: &mut [u8]) -> Option<usize> {
    let mut at = 0usize;
    for (i, seg) in p.segments().enumerate() {
        if i > 0 {
            *out.get_mut(at)? = b'.';
            at += 1;
        }
        out.get_mut(at..at + seg.len())?.copy_from_slice(seg);
        at += seg.len();
    }
    Some(at)
}

/// One `listIndexes` entry: `{v: 2, key: {…: 1}, name, unique?}` into
/// `out[pos..]`. `ix` None is the implicit `_id_` index.
pub fn write_index_doc(
    ix: Option<&doc_exec::doc_catalog::Index<'_>>,
    out: &mut [u8],
    pos: &mut usize,
) -> Option<()> {
    use codec::mongo_core::{bson_bool, bson_close, bson_i32, bson_open, bson_str};
    let d = bson_open(out, pos)?;
    bson_i32(out, pos, b"v", 2)?;
    *out.get_mut(*pos)? = bson::DOC;
    *pos += 1;
    out.get_mut(*pos..*pos + 4)?.copy_from_slice(b"key\0");
    *pos += 4;
    let k = bson_open(out, pos)?;
    match ix {
        None => bson_i32(out, pos, b"_id", 1)?,
        Some(ix) => {
            for p in ix.paths() {
                let mut f = [0u8; 1024];
                let n = write_dotted(p, &mut f)?;
                bson_i32(out, pos, &f[..n], 1)?;
            }
        }
    }
    bson_close(out, pos, k)?;
    match ix {
        None => bson_str(out, pos, b"name", b"_id_")?,
        Some(ix) => {
            bson_str(out, pos, b"name", ix.name)?;
            if ix.unique() {
                bson_bool(out, pos, b"unique", true)?;
            }
        }
    }
    bson_close(out, pos, d).map(|_| ())
}
