//! Document execution wire.
//!
//! The boundary between a document dialect (a connector: Mongo's
//! OP_MSG, the Kubernetes API) and the shared `document_executor`, as
//! `sql_exec.rs` is the boundary between the SQL connectors and
//! `relational_executor`. A dialect translates its wire into canonical
//! operations over canonical value trees (`doc_tree.rs`) and renders the
//! answers; the executor runs the operations against a collection as its
//! descriptor (`doc_catalog.rs`) declares it. Neither side sees the
//! other's concerns: no BSON or JSON crosses this wire, and no KV key
//! does either.
//!
//! This file mounts `doc_tree`, `doc_catalog` and `doc_ops` (one mount
//! point, one type — see `sql_exec.rs` for why), so a module mounts only
//! this.
//!
//! ## `MSG_DOC_REQUEST` (0xC9)
//!
//! ```text
//! [corr:u64][dialect:u8][tenant:u32][conn:u8][op:u8][flags:u8]
//! {[field:u8][len:u32][body]}…
//! ```
//!
//! ## `MSG_DOC_RESPONSE` (0xF0)
//!
//! ```text
//! [corr:u64][conn:u8][outcome:u8][flags:u8][revision:u64]
//! {[field:u8][len:u32][body]}…
//! ```
//!
//! All integers little-endian. A field a request or response states is
//! validated when it is decoded — trees, paths, filters, mutations — so
//! an executor that has a decoded request has a well-formed one, and an
//! unknown field is refused rather than ignored.
//!
//! ## Filters
//!
//! A filter is a small tree of nodes:
//!
//! | Node | Layout |
//! |---|---|
//! | `F_TRUE` | `[8]` — every document |
//! | `F_AND` / `F_OR` | `[1 or 2][n:u8][node]…` |
//! | `F_NOT` | `[3][node]` |
//! | `F_CMP` | `[4][op:u8][plen:u16][path][value]` |
//! | `F_IN` / `F_NIN` | `[5 or 6][any:u8][plen:u16][path][array]` |
//! | `F_EXISTS` | `[7][want:u8][plen:u16][path]` |
//!
//! A comparison's `op` is `CMP_EQ`..`CMP_GTE`, with `CMP_ANY` set when
//! an array at the path matches if any element does (Mongo's reading of
//! `{tags: "x"}`); without it the comparison is of the value itself. A
//! comparison is between values of one type rank — `{n: {$gt: "a"}}`
//! matches no number — as MongoDB does; equality needs no rank rule.
//!
//! ## Mutations
//!
//! `[n:u16]{[kind:u8][sub:u8][plen:u16][path][operand]}…` with `kind`
//! a `models::DocMutationKind`:
//!
//! | Kind | Sub | Operand |
//! |---|---|---|
//! | PathSet 2 | 0 | value |
//! | PathUnset 3 | `UNSET_REMOVE`: the member or element is removed; `UNSET_NULL_ELEMENT`: a member is removed, an array element becomes null in place (MongoDB's `$unset`) | — |
//! | NumericUpdate 4 | `NUM_INC`, `NUM_MUL`, `NUM_MIN`, `NUM_MAX` | number (MIN/MAX: any value) |
//! | ArrayUpdate 5 | `ARR_PUSH`, `ARR_ADD_TO_SET`, `ARR_PULL` | value |
//! | ArrayUpdate 5 | `ARR_POP_FIRST`, `ARR_POP_LAST` | — |
//!
//! ## Projections and write scopes
//!
//! `[mode:u8][n:u8]{[plen:u16][path]}…`: `MODE_INCLUDE` keeps only the
//! paths (a dialect that always answers the id lists it), `MODE_EXCLUDE`
//! drops them. A write scope has the
//! same form: `MODE_INCLUDE` lets a write change only those paths (a
//! Kubernetes `/status` write), `MODE_EXCLUDE` keeps those paths as
//! stored whatever the write says (a main-resource write and `status`).

#[path = "doc_tree.rs"]
pub mod doc_tree;

#[path = "doc_catalog.rs"]
pub mod doc_catalog;

#[path = "doc_ops.rs"]
pub mod doc_ops;

#[path = "doc_fields.rs"]
pub mod doc_fields;

use doc_catalog::Descriptor;
use doc_tree::{Path, Value};

// ── Envelope ──────────────────────────────────────────────────────────

pub const DOC_REQUEST_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1;
pub const DOC_RESPONSE_HEAD: usize = 8 + 1 + 1 + 1 + 8;
/// Largest request payload: one channel frame (the SDK's
/// `CHANNEL_BUFFER_SIZE`, 8192, which is also a cross-core slot) less
/// the envelope's 3-byte header. Nothing larger crosses an edge.
pub const DOC_REQUEST_MAX: usize = 8189;
/// Largest response payload: one frame, as a request. A list answers
/// what fits and a continuation; a single document past it is
/// `OUT_TOO_LARGE`.
pub const DOC_RESPONSE_MAX: usize = 8189;
/// Field framing: `[field:u8][len:u32]`.
pub const FIELD_HEAD: usize = 5;

/// `dialect` byte: which connector sent the request. The executor's
/// behaviour does not depend on it — descriptors carry every difference
/// — it is recorded for metrics and ownership provenance.
pub const DIALECT_MONGO: u8 = 1;
pub const DIALECT_KUBERNETES: u8 = 2;

pub const OP_COLL_CREATE: u8 = 1;
pub const OP_COLL_DROP: u8 = 2;
pub const OP_COLL_GET: u8 = 3;
pub const OP_COLL_LIST: u8 = 4;
pub const OP_INSERT: u8 = 5;
pub const OP_REPLACE: u8 = 6;
pub const OP_UPDATE: u8 = 7;
pub const OP_PATCH_MERGE: u8 = 8;
pub const OP_PATCH_JSON: u8 = 9;
pub const OP_PATCH_KEYED: u8 = 10;
pub const OP_APPLY: u8 = 11;
pub const OP_DELETE: u8 = 12;
pub const OP_GET: u8 = 13;
pub const OP_LIST: u8 = 14;
pub const OP_WATCH: u8 = 15;
/// Add a secondary index (`FIELD_INDEX`) and build it over the stored
/// documents; a unique one that the documents violate is refused.
pub const OP_INDEX_CREATE: u8 = 16;
/// Drop the index `FIELD_INDEX_NAME` and its entries.
pub const OP_INDEX_DROP: u8 = 17;
const OP_LAST: u8 = OP_INDEX_DROP;

/// Compute and answer; store nothing.
pub const FLAG_DRY_RUN: u8 = 0x01;
/// REPLACE/UPDATE/APPLY: create the document when nothing matches.
pub const FLAG_UPSERT: u8 = 0x02;
/// APPLY: take ownership of conflicting fields.
pub const FLAG_FORCE: u8 = 0x04;
/// UPDATE/DELETE by filter: every match, not the first.
pub const FLAG_MULTI: u8 = 0x08;
/// A write to a collection that does not exist creates it with the
/// default descriptor (MongoDB's implicit creation).
pub const FLAG_AUTO_CREATE: u8 = 0x10;
/// WATCH: keep the watch open — every later change in its span is answered
/// on the same `corr` (each answer with `RFLAG_MORE`) until an answer
/// without it (an error, `compacted`) or a cancel ends it.
pub const FLAG_FOLLOW: u8 = 0x20;
/// WATCH: end the followed watch this request's `corr` and `conn` name
/// (from the same dialect); answered `OUT_OK`.
pub const FLAG_CANCEL: u8 = 0x40;
const FLAGS_KNOWN: u8 = 0x7F;

pub const FIELD_COLLECTION: u8 = 1;
pub const FIELD_DESCRIPTOR: u8 = 2;
pub const FIELD_ID: u8 = 3;
pub const FIELD_DOC: u8 = 4;
pub const FIELD_FILTER: u8 = 5;
pub const FIELD_PROJECTION: u8 = 6;
pub const FIELD_MUTATIONS: u8 = 7;
pub const FIELD_PATCH: u8 = 8;
pub const FIELD_MANAGER: u8 = 9;
pub const FIELD_IF_REVISION: u8 = 10;
pub const FIELD_LIMIT: u8 = 11;
pub const FIELD_CONTINUE: u8 = 12;
pub const FIELD_AT_REVISION: u8 = 13;
pub const FIELD_WRITE_SCOPE: u8 = 14;
pub const FIELD_PRECONDITION: u8 = 15;
/// `[u32]`: the bytes of documents the dialect can render in one answer.
/// A list stops before a document that would pass it (always answering
/// at least one); the dialect sizes it by how much larger its own
/// encoding of a tree can be.
pub const FIELD_MAX_BYTES: u8 = 16;
/// A keyed patch's directive names, as the dialect spells them:
/// `{[len:u8][name]}` × 4 — the member naming an object's patch directive
/// (whose value `replace`, `delete` or `merge` acts on the object), the
/// member listing the members an object keeps, the prefix of a member
/// listing scalars to drop from a set, and the prefix every directive
/// member starts with (never data). An empty name: that directive is not
/// spoken.
pub const FIELD_DIRECTIVES: u8 = 17;
/// An index to create: a descriptor's `INDEX` entry body (its id and
/// state are the executor's to set).
pub const FIELD_INDEX: u8 = 18;
/// An index's name.
pub const FIELD_INDEX_NAME: u8 = 19;
/// The label a write's ownership entry records beside its manager — the
/// part of the document the write addresses (a Kubernetes subresource:
/// `status`, `scale`). Absent: the whole document.
pub const FIELD_SCOPE_NAME: u8 = 20;
/// LIST/WATCH: the leading components of a compound id (an array) — only
/// the documents whose ids begin with them (a Kubernetes namespace).
pub const FIELD_ID_PREFIX: u8 = 21;
const FIELD_LAST: u8 = FIELD_ID_PREFIX;

/// Longest writer (field manager) name.
pub const MAX_MANAGER_LEN: usize = 128;
/// Longest scope label (`FIELD_SCOPE_NAME`); an ownership entry holds it
/// in a byte-length.
pub const MAX_SCOPE_LEN: usize = 64;
/// Most documents one INSERT carries.
pub const MAX_INSERT_DOCS: usize = 64;

// Response.
pub const OUT_OK: u8 = 0;
pub const OUT_NOT_FOUND: u8 = 1;
/// A create found the id taken.
pub const OUT_EXISTS: u8 = 2;
/// A revision or value precondition did not hold, or a CAS lost its race
/// past the retry budget.
pub const OUT_CONFLICT: u8 = 3;
/// An apply found fields other writers own (`RESP_CONFLICT` lists them).
pub const OUT_OWNERSHIP: u8 = 4;
/// The request is well formed but the operation cannot apply: a path
/// through a scalar, an identity path changed, a patch test failed, a
/// non-numeric increment. `RESP_ERROR` names the path.
pub const OUT_INVALID: u8 = 5;
pub const OUT_TOO_LARGE: u8 = 6;
/// A read or watch from a revision the store has compacted.
pub const OUT_COMPACTED: u8 = 7;
pub const OUT_BUSY: u8 = 8;
/// The store did not answer, or refused for a reason that is not the
/// client's.
pub const OUT_UNAVAILABLE: u8 = 9;
pub const OUT_NO_COLLECTION: u8 = 10;
/// A stored record failed to decode.
pub const OUT_CORRUPT: u8 = 11;
pub const OUT_TIMEOUT: u8 = 12;
/// A malformed request (an unknown op, field or flag; a bad tree).
pub const OUT_MALFORMED: u8 = 13;
pub const OUT_COLLECTION_EXISTS: u8 = 14;

/// A continuation follows; the answer is partial.
pub const RFLAG_MORE: u8 = 0x01;
/// Nothing was stored.
pub const RFLAG_DRY_RUN: u8 = 0x02;
/// A delete the collection's guard holds: the document is marked
/// deleting (its declared deleting stamps) and kept.
pub const RFLAG_HELD: u8 = 0x04;
/// A write to a document marked deleting left its guard empty: the
/// document is gone (the answer shows it as the write left it).
pub const RFLAG_REMOVED: u8 = 0x08;
/// The write created the document (an insert, or an apply of an absent
/// one): what a client answers differently from a change (HTTP 201).
pub const RFLAG_CREATED: u8 = 0x10;

/// `[rev:u64][value]`: a document and the revision that wrote it.
pub const RESP_DOC: u8 = 1;
/// `[matched:u64][modified:u64]`.
pub const RESP_COUNT: u8 = 2;
pub const RESP_CONTINUE: u8 = 3;
/// `[index:u32][plen:u16][path][message]`: which document (an insert's
/// position, else 0), which path, and why.
pub const RESP_ERROR: u8 = 4;
/// `[mlen:u8][manager][op:u8][vlen:u8][version][field]`: one ownership
/// conflict — the owner (its manager, operation and the version it wrote
/// at, as its ownership entry records them) and the field, a field-set
/// path (`doc_fields.rs`) the dialect spells.
pub const RESP_CONFLICT: u8 = 5;
pub const RESP_DESCRIPTOR: u8 = 6;
/// The id an upsert or a generated-id create chose.
pub const RESP_ID: u8 = 7;
/// `[rev:u64][kind:u8][value]`: one change of a watch — `EVENT_CREATE`
/// (the revision created the document) or `EVENT_PUT` (it changed one that
/// existed) with the document as that revision wrote it, or `EVENT_DELETE`
/// with the deleted document's id. A watch answers its events in revision
/// order.
pub const RESP_EVENT: u8 = 8;
/// The ownership record (`doc_fields.rs`) of the document in the
/// `RESP_DOC` or `RESP_EVENT` before it, for a collection that tracks
/// ownership.
pub const RESP_OWNERS: u8 = 9;
const RESP_LAST: u8 = RESP_OWNERS;

pub const EVENT_PUT: u8 = 1;
pub const EVENT_DELETE: u8 = 2;
pub const EVENT_CREATE: u8 = 3;

// Filters.
pub const F_AND: u8 = 1;
pub const F_OR: u8 = 2;
pub const F_NOT: u8 = 3;
pub const F_CMP: u8 = 4;
pub const F_IN: u8 = 5;
pub const F_NIN: u8 = 6;
pub const F_EXISTS: u8 = 7;
pub const F_TRUE: u8 = 8;
pub const CMP_EQ: u8 = 1;
pub const CMP_NE: u8 = 2;
pub const CMP_LT: u8 = 3;
pub const CMP_LTE: u8 = 4;
pub const CMP_GT: u8 = 5;
pub const CMP_GTE: u8 = 6;
pub const CMP_ANY: u8 = 0x80;
/// Deepest filter nesting, and most nodes.
pub const MAX_FILTER_DEPTH: usize = 16;
pub const MAX_FILTER_NODES: usize = 128;

// Mutations (kinds are `models::DocMutationKind`).
pub const MUT_PATH_SET: u8 = 2;
pub const MUT_PATH_UNSET: u8 = 3;
pub const MUT_NUMERIC: u8 = 4;
pub const MUT_ARRAY: u8 = 5;
pub const UNSET_REMOVE: u8 = 0;
pub const UNSET_NULL_ELEMENT: u8 = 1;
pub const NUM_INC: u8 = 1;
pub const NUM_MUL: u8 = 2;
pub const NUM_MIN: u8 = 3;
pub const NUM_MAX: u8 = 4;
pub const ARR_PUSH: u8 = 1;
pub const ARR_ADD_TO_SET: u8 = 2;
pub const ARR_PULL: u8 = 3;
pub const ARR_POP_FIRST: u8 = 4;
pub const ARR_POP_LAST: u8 = 5;
/// Most mutations in one update.
pub const MAX_MUTATIONS: usize = 32;

// Projections and write scopes.
pub const MODE_INCLUDE: u8 = 1;
pub const MODE_EXCLUDE: u8 = 2;
pub const MAX_PROJECTION_PATHS: usize = 32;

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

/// `[plen:u16][path][rest]` → `(path, rest)`.
pub fn split_path(b: &[u8]) -> Option<(Path<'_>, &[u8])> {
    let n = rd_u16(b, 0)? as usize;
    let p = Path::parse(b.get(2..2 + n)?).ok()?;
    Some((p, b.get(2 + n..).unwrap_or(&[])))
}

/// Fields of a request or response body, in order.
#[derive(Clone)]
pub struct Fields<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Fields<'a> {
    /// The fields of `b`, a body in this wire's framing (a contract that
    /// shares it, `doc_json.rs`, reads its own with this).
    pub fn of(b: &'a [u8]) -> Self {
        Fields { b, at: 0 }
    }
}

impl<'a> Iterator for Fields<'a> {
    type Item = (u8, &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        let tag = *self.b.get(self.at)?;
        let len = rd_u32(self.b, self.at + 1)? as usize;
        let s = self.at + FIELD_HEAD;
        let body = self.b.get(s..s.checked_add(len)?)?;
        self.at = s + len;
        Some((tag, body))
    }
}

// ── Filters ───────────────────────────────────────────────────────────

/// Validate a filter node at the start of `b`; answer its length.
fn filter_node(b: &[u8], depth: usize, nodes: &mut usize) -> Option<usize> {
    if depth > MAX_FILTER_DEPTH {
        return None;
    }
    *nodes += 1;
    if *nodes > MAX_FILTER_NODES {
        return None;
    }
    match *b.first()? {
        F_TRUE => Some(1),
        F_AND | F_OR => {
            let n = *b.get(1)? as usize;
            let mut at = 2;
            for _ in 0..n {
                at += filter_node(b.get(at..)?, depth + 1, nodes)?;
            }
            Some(at)
        }
        F_NOT => Some(1 + filter_node(b.get(1..)?, depth + 1, nodes)?),
        F_CMP => {
            let op = *b.get(1)?;
            if !(CMP_EQ..=CMP_GTE).contains(&(op & !CMP_ANY)) {
                return None;
            }
            let (_, rest) = split_path(b.get(2..)?)?;
            let vlen = doc_tree::validate_prefix(rest).ok()?;
            Some(b.len() - rest.len() + vlen)
        }
        F_IN | F_NIN => {
            if *b.get(1)? > 1 {
                return None;
            }
            let (_, rest) = split_path(b.get(2..)?)?;
            let vlen = doc_tree::validate_prefix(rest).ok()?;
            if rest.first() != Some(&doc_tree::T_ARRAY) {
                return None;
            }
            Some(b.len() - rest.len() + vlen)
        }
        F_EXISTS => {
            if *b.get(1)? > 1 {
                return None;
            }
            let (_, rest) = split_path(b.get(2..)?)?;
            Some(b.len() - rest.len())
        }
        _ => None,
    }
}

/// Validate a whole filter.
pub fn filter_is_valid(b: &[u8]) -> bool {
    let mut nodes = 0;
    filter_node(b, 0, &mut nodes) == Some(b.len())
}

/// One decoded filter node over a validated filter.
#[derive(Clone, Copy, Debug)]
pub enum Filter<'a> {
    True,
    /// `(is_and, children)` — iterate with [`Filter::children`].
    Group(bool, &'a [u8], u8),
    Not(&'a [u8]),
    Cmp {
        op: u8,
        any: bool,
        path: Path<'a>,
        value: Value<'a>,
    },
    In {
        negated: bool,
        any: bool,
        path: Path<'a>,
        set: Value<'a>,
    },
    Exists {
        want: bool,
        path: Path<'a>,
    },
}

impl<'a> Filter<'a> {
    /// Decode the node at the start of a validated filter.
    pub fn decode(b: &'a [u8]) -> Option<Filter<'a>> {
        match *b.first()? {
            F_TRUE => Some(Filter::True),
            k @ (F_AND | F_OR) => Some(Filter::Group(k == F_AND, b.get(2..)?, *b.get(1)?)),
            F_NOT => Some(Filter::Not(b.get(1..)?)),
            F_CMP => {
                let op = *b.get(1)?;
                let (path, rest) = split_path(b.get(2..)?)?;
                let n = doc_tree::value_span(rest)?;
                Some(Filter::Cmp {
                    op: op & !CMP_ANY,
                    any: op & CMP_ANY != 0,
                    path,
                    value: Value::trusted(rest.get(..n)?),
                })
            }
            k @ (F_IN | F_NIN) => {
                let any = *b.get(1)? != 0;
                let (path, rest) = split_path(b.get(2..)?)?;
                let n = doc_tree::value_span(rest)?;
                Some(Filter::In {
                    negated: k == F_NIN,
                    any,
                    path,
                    set: Value::trusted(rest.get(..n)?),
                })
            }
            F_EXISTS => {
                let want = *b.get(1)? != 0;
                let (path, _) = split_path(b.get(2..)?)?;
                Some(Filter::Exists { want, path })
            }
            _ => None,
        }
    }

    /// A group's children (`Group`), or a negation's one child (`Not`).
    pub fn children(&self) -> FilterChildren<'a> {
        match *self {
            Filter::Group(_, b, n) => FilterChildren {
                b,
                left: n as usize,
            },
            Filter::Not(b) => FilterChildren { b, left: 1 },
            _ => FilterChildren { b: &[], left: 0 },
        }
    }
}

pub struct FilterChildren<'a> {
    b: &'a [u8],
    left: usize,
}

impl<'a> Iterator for FilterChildren<'a> {
    type Item = Filter<'a>;
    fn next(&mut self) -> Option<Filter<'a>> {
        if self.left == 0 {
            return None;
        }
        let mut nodes = 0;
        let n = filter_node(self.b, 0, &mut nodes)?;
        let f = Filter::decode(self.b.get(..n)?)?;
        self.b = self.b.get(n..)?;
        self.left -= 1;
        Some(f)
    }
}

// ── Mutations ─────────────────────────────────────────────────────────

/// One decoded mutation.
#[derive(Clone, Copy, Debug)]
pub struct Mutation<'a> {
    pub kind: u8,
    pub sub: u8,
    pub path: Path<'a>,
    /// Absent for unset and pops.
    pub operand: Option<Value<'a>>,
}

fn mutation_has_operand(kind: u8, sub: u8) -> Option<bool> {
    match (kind, sub) {
        (MUT_PATH_SET, 0) => Some(true),
        (MUT_PATH_UNSET, UNSET_REMOVE | UNSET_NULL_ELEMENT) => Some(false),
        (MUT_NUMERIC, NUM_INC..=NUM_MAX) => Some(true),
        (MUT_ARRAY, ARR_PUSH..=ARR_PULL) => Some(true),
        (MUT_ARRAY, ARR_POP_FIRST | ARR_POP_LAST) => Some(false),
        _ => None,
    }
}

/// Decode the mutation at the start of `b`; answer it and its length.
fn mutation_at(b: &[u8]) -> Option<(Mutation<'_>, usize)> {
    let kind = *b.first()?;
    let sub = *b.get(1)?;
    let with = mutation_has_operand(kind, sub)?;
    let (path, rest) = split_path(b.get(2..)?)?;
    if path.is_empty() {
        return None;
    }
    let head = b.len() - rest.len();
    let (operand, n) = if with {
        let n = doc_tree::validate_prefix(rest).ok()?;
        let v = Value::trusted(rest.get(..n)?);
        if kind == MUT_NUMERIC
            && matches!(sub, NUM_INC | NUM_MUL)
            && !matches!(v.ty(), doc_tree::T_INTEGER | doc_tree::T_DECIMAL)
        {
            return None;
        }
        (Some(v), n)
    } else {
        (None, 0)
    };
    Some((
        Mutation {
            kind,
            sub,
            path,
            operand,
        },
        head + n,
    ))
}

/// A validated mutation list.
#[derive(Clone, Copy, Debug)]
pub struct Mutations<'a> {
    b: &'a [u8],
}

impl<'a> Mutations<'a> {
    pub fn parse(b: &'a [u8]) -> Option<Self> {
        // An empty list is an update that changes nothing (a MongoDB
        // `$setOnInsert`-only update on a match).
        let n = rd_u16(b, 0)? as usize;
        if n > MAX_MUTATIONS {
            return None;
        }
        let mut at = 2;
        for _ in 0..n {
            let (_, len) = mutation_at(b.get(at..)?)?;
            at += len;
        }
        (at == b.len()).then_some(Mutations { b })
    }

    pub fn len(&self) -> usize {
        rd_u16(self.b, 0).unwrap_or(0) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = Mutation<'a>> + 'a {
        let b = self.b;
        let mut at = 2usize;
        let mut left = self.len();
        core::iter::from_fn(move || {
            if left == 0 {
                return None;
            }
            let (m, n) = mutation_at(b.get(at..)?)?;
            at += n;
            left -= 1;
            Some(m)
        })
    }
}

// ── Path lists (projections, write scopes) ────────────────────────────

/// A validated `[mode:u8][n:u8]{[plen:u16][path]}…`.
#[derive(Clone, Copy, Debug)]
pub struct PathList<'a> {
    b: &'a [u8],
}

impl<'a> PathList<'a> {
    pub fn parse(b: &'a [u8]) -> Option<Self> {
        let mode = *b.first()?;
        if mode != MODE_INCLUDE && mode != MODE_EXCLUDE {
            return None;
        }
        let n = *b.get(1)? as usize;
        if n > MAX_PROJECTION_PATHS {
            return None;
        }
        let mut rest = b.get(2..)?;
        for _ in 0..n {
            let (_, r) = split_path(rest)?;
            rest = r;
        }
        rest.is_empty().then_some(PathList { b })
    }

    pub fn include(&self) -> bool {
        self.b.first() == Some(&MODE_INCLUDE)
    }

    pub fn paths(&self) -> impl Iterator<Item = Path<'a>> + 'a {
        let mut rest = self.b.get(2..).unwrap_or(&[]);
        let mut left = self.b.get(1).copied().unwrap_or(0) as usize;
        core::iter::from_fn(move || {
            if left == 0 {
                return None;
            }
            let (p, r) = split_path(rest)?;
            rest = r;
            left -= 1;
            Some(p)
        })
    }
}

// ── Keyed-patch directives ──────────────────────────────────────────

/// The names a dialect spells a keyed patch's directives with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Directives<'a> {
    /// The member whose value (`replace`, `delete`, `merge`) acts on its
    /// object.
    pub patch: &'a [u8],
    /// The member listing the members an object keeps.
    pub retain: &'a [u8],
    /// The prefix of a member (`<prefix><list>`) listing scalars to drop
    /// from the set `<list>`.
    pub delete_from_set: &'a [u8],
    /// What every directive member starts with; such members are never
    /// data.
    pub prefix: &'a [u8],
}

impl<'a> Directives<'a> {
    pub const NONE: Directives<'static> = Directives {
        patch: &[],
        retain: &[],
        delete_from_set: &[],
        prefix: &[],
    };

    pub fn parse(b: &'a [u8]) -> Option<Self> {
        let mut at = 0usize;
        let mut names: [&'a [u8]; 4] = [&[]; 4];
        for n in names.iter_mut() {
            let l = *b.get(at)? as usize;
            let s = b.get(at + 1..at + 1 + l)?;
            if !doc_tree::utf8_is_valid(s) {
                return None;
            }
            *n = s;
            at += 1 + l;
        }
        (at == b.len()).then_some(Directives {
            patch: names[0],
            retain: names[1],
            delete_from_set: names[2],
            prefix: names[3],
        })
    }
}

// ── Requests ──────────────────────────────────────────────────────────

/// A decoded, validated `MSG_DOC_REQUEST`.
#[derive(Clone, Copy, Debug)]
pub struct DocRequest<'a> {
    pub corr_id: u64,
    pub dialect: u8,
    pub tenant_id: u32,
    pub conn_id: u8,
    pub op: u8,
    pub flags: u8,
    fields: &'a [u8],
}

impl<'a> DocRequest<'a> {
    /// Decode and validate every field. `None` for anything malformed:
    /// an executor answers that `OUT_MALFORMED` without reading further.
    /// A request is at most one frame ([`DOC_REQUEST_MAX`]).
    pub fn decode(p: &'a [u8]) -> Option<Self> {
        Self::decode_up_to(p, DOC_REQUEST_MAX)
    }

    /// [`DocRequest::decode`] for a receiver whose policy takes requests
    /// up to `max` bytes (reassembled from `wire::MSG_PART` parts).
    pub fn decode_up_to(p: &'a [u8], max: usize) -> Option<Self> {
        if p.len() < DOC_REQUEST_HEAD || p.len() > max {
            return None;
        }
        let op = p[14];
        let flags = p[15];
        if op == 0 || op > OP_LAST || flags & !FLAGS_KNOWN != 0 {
            return None;
        }
        if op != OP_WATCH && flags & (FLAG_FOLLOW | FLAG_CANCEL) != 0 {
            return None;
        }
        let fields = p.get(DOC_REQUEST_HEAD..)?;
        let mut seen = 0u32;
        let mut docs = 0usize;
        let mut it = Fields { b: fields, at: 0 };
        let mut end = 0usize;
        for (tag, body) in it.by_ref() {
            if tag == 0 || tag > FIELD_LAST {
                return None;
            }
            let repeats = matches!(tag, FIELD_DOC | FIELD_PRECONDITION);
            if !repeats && seen & (1 << tag) != 0 {
                return None;
            }
            seen |= 1 << tag;
            if tag == FIELD_DOC {
                docs += 1;
                if docs > MAX_INSERT_DOCS {
                    return None;
                }
            }
            if !field_is_valid(tag, body) {
                return None;
            }
            end += FIELD_HEAD + body.len();
        }
        if end != fields.len() {
            return None;
        }
        Some(DocRequest {
            corr_id: rd_u64(p, 0)?,
            dialect: p[8],
            tenant_id: rd_u32(p, 9)?,
            conn_id: p[13],
            op,
            flags,
            fields,
        })
    }

    pub fn fields(&self) -> Fields<'a> {
        Fields {
            b: self.fields,
            at: 0,
        }
    }

    fn field(&self, tag: u8) -> Option<&'a [u8]> {
        self.fields().find(|(t, _)| *t == tag).map(|(_, b)| b)
    }

    pub fn has(&self, tag: u8) -> bool {
        self.field(tag).is_some()
    }

    pub fn collection(&self) -> Option<&'a [u8]> {
        self.field(FIELD_COLLECTION)
    }

    pub fn descriptor(&self) -> Option<Descriptor<'a>> {
        Descriptor::parse(self.field(FIELD_DESCRIPTOR)?).ok()
    }

    pub fn id(&self) -> Option<Value<'a>> {
        self.field(FIELD_ID).map(Value::trusted)
    }

    /// The documents, in order.
    pub fn docs(&self) -> impl Iterator<Item = Value<'a>> + 'a {
        self.fields()
            .filter(|(t, _)| *t == FIELD_DOC)
            .map(|(_, b)| Value::trusted(b))
    }

    pub fn doc(&self) -> Option<Value<'a>> {
        self.docs().next()
    }

    /// The filter, or `None` for every document.
    pub fn filter(&self) -> Option<Filter<'a>> {
        Filter::decode(self.field(FIELD_FILTER)?)
    }

    pub fn projection(&self) -> Option<PathList<'a>> {
        PathList::parse(self.field(FIELD_PROJECTION)?)
    }

    pub fn mutations(&self) -> Option<Mutations<'a>> {
        Mutations::parse(self.field(FIELD_MUTATIONS)?)
    }

    pub fn patch(&self) -> Option<Value<'a>> {
        self.field(FIELD_PATCH).map(Value::trusted)
    }

    pub fn manager(&self) -> Option<&'a [u8]> {
        self.field(FIELD_MANAGER)
    }

    pub fn scope_name(&self) -> Option<&'a [u8]> {
        self.field(FIELD_SCOPE_NAME)
    }

    pub fn id_prefix(&self) -> Option<Value<'a>> {
        self.field(FIELD_ID_PREFIX).map(Value::trusted)
    }

    pub fn if_revision(&self) -> Option<u64> {
        self.field(FIELD_IF_REVISION).and_then(|b| rd_u64(b, 0))
    }

    pub fn limit(&self) -> Option<u32> {
        self.field(FIELD_LIMIT).and_then(|b| rd_u32(b, 0))
    }

    pub fn continuation(&self) -> Option<&'a [u8]> {
        self.field(FIELD_CONTINUE)
    }

    /// The index to create.
    pub fn index(&self) -> Option<doc_catalog::Index<'a>> {
        doc_catalog::Index::parse(self.field(FIELD_INDEX)?)
    }

    /// The index `FIELD_INDEX` carries, as its entry body.
    pub fn index_bytes(&self) -> Option<&'a [u8]> {
        self.field(FIELD_INDEX)
    }

    pub fn index_name(&self) -> Option<&'a [u8]> {
        self.field(FIELD_INDEX_NAME)
    }

    /// A keyed patch's directive names (none spoken when absent).
    pub fn directives(&self) -> Directives<'a> {
        self.field(FIELD_DIRECTIVES)
            .and_then(Directives::parse)
            .unwrap_or(Directives::NONE)
    }

    pub fn max_bytes(&self) -> Option<u32> {
        self.field(FIELD_MAX_BYTES).and_then(|b| rd_u32(b, 0))
    }

    pub fn at_revision(&self) -> Option<u64> {
        self.field(FIELD_AT_REVISION).and_then(|b| rd_u64(b, 0))
    }

    pub fn write_scope(&self) -> Option<PathList<'a>> {
        PathList::parse(self.field(FIELD_WRITE_SCOPE)?)
    }

    /// `(path, value)` each: the stored document's value at path must
    /// equal value.
    pub fn preconditions(&self) -> impl Iterator<Item = (Path<'a>, Value<'a>)> + 'a {
        self.fields()
            .filter(|(t, _)| *t == FIELD_PRECONDITION)
            .filter_map(|(_, b)| {
                let (p, v) = split_path(b)?;
                Some((p, Value::trusted(v)))
            })
    }
}

/// A `RESP_CONFLICT` body's `(manager, op, version, field)`.
fn conflict_parts(b: &[u8]) -> Option<(&[u8], u8, &[u8], &[u8])> {
    let m = *b.first()? as usize;
    let manager = b.get(1..1 + m)?;
    let op = *b.get(1 + m)?;
    let v = *b.get(2 + m)? as usize;
    let version = b.get(3 + m..3 + m + v)?;
    let field = b.get(3 + m + v..)?;
    if manager.is_empty() || field.is_empty() || doc_fields::elements(field).any(|e| e.is_err()) {
        return None;
    }
    Some((manager, op, version, field))
}

fn field_is_valid(tag: u8, b: &[u8]) -> bool {
    match tag {
        FIELD_COLLECTION => doc_catalog::name_is_valid(b),
        FIELD_DESCRIPTOR => Descriptor::parse(b).is_ok(),
        FIELD_ID => Value::parse(b).is_ok_and(|v| !v.is_container() || doc_ops::id_is_usable(v)),
        FIELD_ID_PREFIX => {
            Value::parse(b).is_ok_and(|v| v.ty() == doc_tree::T_ARRAY && doc_ops::id_is_usable(v))
        }
        FIELD_DOC => Value::parse(b).is_ok_and(|v| v.ty() == doc_tree::T_OBJECT),
        FIELD_PATCH => Value::parse(b).is_ok(),
        FIELD_FILTER => filter_is_valid(b),
        FIELD_PROJECTION | FIELD_WRITE_SCOPE => PathList::parse(b).is_some(),
        FIELD_MUTATIONS => Mutations::parse(b).is_some(),
        FIELD_MANAGER => !b.is_empty() && b.len() <= MAX_MANAGER_LEN && doc_tree::utf8_is_valid(b),
        FIELD_IF_REVISION | FIELD_AT_REVISION => b.len() == 8,
        FIELD_LIMIT | FIELD_MAX_BYTES => b.len() == 4,
        FIELD_DIRECTIVES => Directives::parse(b).is_some(),
        FIELD_INDEX => doc_catalog::Index::parse(b).is_some(),
        FIELD_INDEX_NAME => !b.is_empty() && b.len() <= 255 && doc_tree::utf8_is_valid(b),
        FIELD_SCOPE_NAME => !b.is_empty() && b.len() <= MAX_SCOPE_LEN && doc_tree::utf8_is_valid(b),
        FIELD_CONTINUE => continuation_is_valid(b),
        FIELD_PRECONDITION => split_path(b).is_some_and(|(_, v)| Value::parse(v).is_ok()),
        _ => false,
    }
}

/// Builds a request or response body: a head, then fields.
pub struct FieldWriter<'a> {
    out: &'a mut [u8],
    pos: usize,
}

impl<'a> FieldWriter<'a> {
    fn put(&mut self, b: &[u8]) -> Option<()> {
        let end = self.pos.checked_add(b.len())?;
        let d = self.out.get_mut(self.pos..end)?;
        for (x, y) in d.iter_mut().zip(b) {
            *x = *y;
        }
        self.pos = end;
        Some(())
    }

    /// A `MSG_DOC_REQUEST` head.
    pub fn request(
        out: &'a mut [u8],
        corr_id: u64,
        dialect: u8,
        tenant_id: u32,
        conn_id: u8,
        op: u8,
        flags: u8,
    ) -> Option<Self> {
        let mut w = FieldWriter { out, pos: 0 };
        w.put(&corr_id.to_le_bytes())?;
        w.put(&[dialect])?;
        w.put(&tenant_id.to_le_bytes())?;
        w.put(&[conn_id, op, flags])?;
        Some(w)
    }

    /// A `MSG_DOC_RESPONSE` head.
    pub fn response(
        out: &'a mut [u8],
        corr_id: u64,
        conn_id: u8,
        outcome: u8,
        flags: u8,
        revision: u64,
    ) -> Option<Self> {
        let mut w = FieldWriter { out, pos: 0 };
        w.put(&corr_id.to_le_bytes())?;
        w.put(&[conn_id, outcome, flags])?;
        w.put(&revision.to_le_bytes())?;
        Some(w)
    }

    /// One field from its parts, concatenated as its body.
    pub fn field(&mut self, tag: u8, parts: &[&[u8]]) -> Option<()> {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        let len32 = u32::try_from(len).ok()?;
        if self.pos + FIELD_HEAD + len > self.out.len() {
            return None;
        }
        self.put(&[tag])?;
        self.put(&len32.to_le_bytes())?;
        for p in parts {
            self.put(p)?;
        }
        Some(())
    }

    /// Room left for another field's body.
    pub fn room(&self) -> usize {
        self.out.len().saturating_sub(self.pos + FIELD_HEAD)
    }

    /// Rewrite a response's outcome and flags after the fields.
    pub fn set_outcome(&mut self, outcome: u8, flags: u8) {
        if let Some(d) = self.out.get_mut(9..11) {
            d[0] = outcome;
            d[1] = flags;
        }
    }

    pub fn finish(&self) -> usize {
        self.pos
    }
}

// ── Responses ─────────────────────────────────────────────────────────

/// A decoded `MSG_DOC_RESPONSE`.
#[derive(Clone, Copy, Debug)]
pub struct DocResponse<'a> {
    pub corr_id: u64,
    pub conn_id: u8,
    pub outcome: u8,
    pub flags: u8,
    pub revision: u64,
    fields: &'a [u8],
}

impl<'a> DocResponse<'a> {
    /// A response of at most one frame ([`DOC_RESPONSE_MAX`]).
    pub fn decode(p: &'a [u8]) -> Option<Self> {
        Self::decode_up_to(p, DOC_RESPONSE_MAX)
    }

    /// [`DocResponse::decode`] for a receiver whose policy takes answers up
    /// to `max` bytes (reassembled from `wire::MSG_PART` parts).
    pub fn decode_up_to(p: &'a [u8], max: usize) -> Option<Self> {
        if p.len() < DOC_RESPONSE_HEAD || p.len() > max {
            return None;
        }
        let fields = p.get(DOC_RESPONSE_HEAD..)?;
        let mut end = 0usize;
        for (tag, body) in (Fields { b: fields, at: 0 }) {
            let ok = match tag {
                RESP_DOC => body.len() > 8 && Value::parse(&body[8..]).is_ok(),
                RESP_COUNT => body.len() == 16,
                RESP_CONTINUE => continuation_is_valid(body),
                RESP_ERROR => body.len() >= 4 && split_path(&body[4..]).is_some(),
                RESP_CONFLICT => conflict_parts(body).is_some(),
                RESP_OWNERS => doc_fields::Owners::parse(body).is_ok(),
                RESP_DESCRIPTOR => Descriptor::parse(body).is_ok(),
                RESP_ID => Value::parse(body).is_ok(),
                RESP_EVENT => {
                    body.len() > 9
                        && matches!(body[8], EVENT_PUT | EVENT_DELETE | EVENT_CREATE)
                        && Value::parse(&body[9..]).is_ok()
                }
                _ => false,
            };
            if !ok || tag > RESP_LAST {
                return None;
            }
            end += FIELD_HEAD + body.len();
        }
        if end != fields.len() {
            return None;
        }
        Some(DocResponse {
            corr_id: rd_u64(p, 0)?,
            conn_id: p[8],
            outcome: p[9],
            flags: p[10],
            revision: rd_u64(p, 11)?,
            fields,
        })
    }

    pub fn fields(&self) -> Fields<'a> {
        Fields {
            b: self.fields,
            at: 0,
        }
    }

    /// `(revision, document)` each.
    pub fn docs(&self) -> impl Iterator<Item = (u64, Value<'a>)> + 'a {
        self.fields()
            .filter(|(t, _)| *t == RESP_DOC)
            .filter_map(|(_, b)| Some((rd_u64(b, 0)?, Value::trusted(b.get(8..)?))))
    }

    /// `(matched, modified)`.
    pub fn count(&self) -> Option<(u64, u64)> {
        let b = self.fields().find(|(t, _)| *t == RESP_COUNT)?.1;
        Some((rd_u64(b, 0)?, rd_u64(b, 8)?))
    }

    pub fn continuation(&self) -> Option<&'a [u8]> {
        self.fields()
            .find(|(t, _)| *t == RESP_CONTINUE)
            .map(|(_, b)| b)
    }

    /// `(index, path, message)`.
    pub fn error(&self) -> Option<(u32, Path<'a>, &'a [u8])> {
        let b = self.fields().find(|(t, _)| *t == RESP_ERROR)?.1;
        let (p, msg) = split_path(b.get(4..)?)?;
        Some((rd_u32(b, 0)?, p, msg))
    }

    /// Each conflict: `(manager, op, version, field)`.
    pub fn conflicts(&self) -> impl Iterator<Item = (&'a [u8], u8, &'a [u8], &'a [u8])> + 'a {
        self.fields()
            .filter(|(t, _)| *t == RESP_CONFLICT)
            .filter_map(|(_, b)| conflict_parts(b))
    }

    /// The ownership record after the document at field `i` (a
    /// `RESP_DOC` or `RESP_EVENT`), if one follows it.
    pub fn owners_of(&self, doc_field: usize) -> Option<doc_fields::Owners<'a>> {
        let (t, b) = self.fields().nth(doc_field + 1)?;
        (t == RESP_OWNERS).then(|| doc_fields::Owners::parse(b).ok())?
    }

    pub fn descriptors(&self) -> impl Iterator<Item = Descriptor<'a>> + 'a {
        self.fields()
            .filter(|(t, _)| *t == RESP_DESCRIPTOR)
            .filter_map(|(_, b)| Descriptor::parse(b).ok())
    }

    /// `(revision, kind, value)` each, in revision order.
    pub fn events(&self) -> impl Iterator<Item = (u64, u8, Value<'a>)> + 'a {
        self.fields()
            .filter(|(t, _)| *t == RESP_EVENT)
            .filter_map(|(_, b)| Some((rd_u64(b, 0)?, *b.get(8)?, Value::trusted(b.get(9..)?))))
    }

    pub fn id(&self) -> Option<Value<'a>> {
        self.fields()
            .find(|(t, _)| *t == RESP_ID)
            .map(|(_, b)| Value::trusted(b))
    }
}

// ── Continuations ─────────────────────────────────────────────────────

/// Continuation format: `[1][revision:u64][klen:u16][last key]` — a list
/// resumes after `last key` at `revision`, so every page of one list
/// reads one snapshot.
pub const CONTINUE_V1: u8 = 1;
/// Longest key a continuation carries.
pub const MAX_CONTINUE_KEY: usize = 256;

pub fn continuation_is_valid(b: &[u8]) -> bool {
    decode_continuation(b).is_some()
}

pub fn encode_continuation(out: &mut [u8], revision: u64, last_key: &[u8]) -> Option<usize> {
    if last_key.len() > MAX_CONTINUE_KEY {
        return None;
    }
    let n = 11 + last_key.len();
    let d = out.get_mut(..n)?;
    d[0] = CONTINUE_V1;
    for (x, y) in d[1..9].iter_mut().zip(revision.to_le_bytes()) {
        *x = y;
    }
    for (x, y) in d[9..11]
        .iter_mut()
        .zip((last_key.len() as u16).to_le_bytes())
    {
        *x = y;
    }
    for (x, y) in d[11..].iter_mut().zip(last_key) {
        *x = *y;
    }
    Some(n)
}

/// `(revision, last key)`.
pub fn decode_continuation(b: &[u8]) -> Option<(u64, &[u8])> {
    if *b.first()? != CONTINUE_V1 {
        return None;
    }
    let rev = rd_u64(b, 1)?;
    let n = rd_u16(b, 9)? as usize;
    if n > MAX_CONTINUE_KEY || b.len() != 11 + n {
        return None;
    }
    Some((rev, b.get(11..)?))
}
