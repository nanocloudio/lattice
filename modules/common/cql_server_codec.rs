//! Server-side CQL native-protocol framing for the wide-column
//! capability (RFC database foundation §14.17, Phase 8).
//!
//! The wide-column capability's first connector speaks the Cassandra
//! CQL native protocol v4, because that is what real wide-column
//! clients speak. This file owns the CONNECTOR half per the §14.3
//! split: frame parsing, the STARTUP/OPTIONS handshake surface, and
//! RESULT/ERROR encoding. It does NOT own cell semantics — those are
//! `models.rs`'s (§14.15: models share foundations, not meanings) —
//! nor CQL text parsing, which reuses the ONE SQL front end
//! (`sql_core`) exactly as §14.2 built it to be reused.
//!
//! Frame layer comes from `cql_core.rs` — the OUTBOUND cassandra
//! client's own core, one mount chain — so the two sides of the
//! protocol share one byte layout by construction, and the tests are
//! differential: requests built with the client side, parsed with
//! this one.

#![allow(
    dead_code,
    reason = "shared via #[path] into the anchor module and host tests; each consumer uses a subset"
)]

#[path = "cql_core.rs"]
pub mod cql_core;

#[path = "models.rs"]
pub mod models;

use cql_core::cql_op;

/// Response frames carry the version's response bit.
pub const CQL_VERSION_RESPONSE: u8 = 0x84;

/// RESULT kinds.
pub const RESULT_VOID: i32 = 0x0001;
pub const RESULT_ROWS: i32 = 0x0002;

/// CQL wire type ids for the column specs this slice serves.
pub const TYPE_INT: u16 = 0x0009;
pub const TYPE_BIGINT: u16 = 0x0002;
pub const TYPE_VARCHAR: u16 = 0x000D;
pub const TYPE_BOOLEAN: u16 = 0x0004;
pub const TYPE_DOUBLE: u16 = 0x0007;

/// CQL error codes (the ones this capability produces).
pub const ERR_SERVER: i32 = 0x0000;
pub const ERR_PROTOCOL: i32 = 0x000A;
pub const ERR_SYNTAX: i32 = 0x2000;
pub const ERR_INVALID: i32 = 0x2200;
pub const ERR_ALREADY_EXISTS: i32 = 0x2400;

fn put(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Option<()> {
    if *pos + b.len() > out.len() {
        return None;
    }
    out[*pos..*pos + b.len()].copy_from_slice(b);
    *pos += b.len();
    Some(())
}

/// Frame a RESPONSE around a body already in `body`, into `out`.
/// Mirrors `cql_core::cql_frame` with the response version bit.
pub fn frame_response(opcode: u8, stream: i16, body: &[u8], out: &mut [u8]) -> Option<usize> {
    let total = 9 + body.len();
    if out.len() < total {
        return None;
    }
    out[0] = CQL_VERSION_RESPONSE;
    out[1] = 0; // flags
    out[2..4].copy_from_slice(&stream.to_be_bytes());
    out[4] = opcode;
    out[5..9].copy_from_slice(&(body.len() as u32).to_be_bytes());
    out[9..total].copy_from_slice(body);
    Some(total)
}

/// READY (empty body).
pub fn ready(stream: i16, out: &mut [u8]) -> Option<usize> {
    frame_response(cql_op::READY, stream, &[], out)
}

/// SUPPORTED: `{"CQL_VERSION": ["3.4.5"], "COMPRESSION": []}` as a
/// string multimap.
pub fn supported(stream: i16, out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 128];
    let mut p = 0usize;
    put(&mut body, &mut p, &2u16.to_be_bytes())?; // two keys
    put(&mut body, &mut p, &11u16.to_be_bytes())?;
    put(&mut body, &mut p, b"CQL_VERSION")?;
    put(&mut body, &mut p, &1u16.to_be_bytes())?;
    put(&mut body, &mut p, &5u16.to_be_bytes())?;
    put(&mut body, &mut p, b"3.4.5")?;
    put(&mut body, &mut p, &11u16.to_be_bytes())?;
    put(&mut body, &mut p, b"COMPRESSION")?;
    put(&mut body, &mut p, &0u16.to_be_bytes())?;
    frame_response(cql_op::SUPPORTED, stream, &body[..p], out)
}

/// ERROR: `[code:i32][string message]`.
pub fn error(stream: i16, code: i32, msg: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 256];
    let mut p = 0usize;
    put(&mut body, &mut p, &code.to_be_bytes())?;
    put(&mut body, &mut p, &(msg.len() as u16).to_be_bytes())?;
    put(&mut body, &mut p, msg)?;
    frame_response(cql_op::ERROR, stream, &body[..p], out)
}

/// RESULT Void — DDL and writes.
pub fn result_void(stream: i16, out: &mut [u8]) -> Option<usize> {
    let body = RESULT_VOID.to_be_bytes();
    frame_response(cql_op::RESULT, stream, &body, out)
}

/// The QUERY body: `[long string cql][consistency:u16][flags:u8]…`.
/// Only the text matters to this capability; parameters/paging flags
/// refuse (named — a silently ignored page size returns wrong pages).
pub fn parse_query(body: &[u8]) -> Result<&[u8], ()> {
    if body.len() < 4 {
        return Err(());
    }
    let n = i32::from_be_bytes(body[0..4].try_into().map_err(|_| ())?) as usize;
    let text = body.get(4..4 + n).ok_or(())?;
    let rest = &body[4 + n..];
    if rest.len() < 3 {
        return Err(());
    }
    let flags = rest[2];
    // 0x01 values, 0x04 page size, 0x08 paging state, 0x10 serial
    // consistency: all unsupported in this slice — refuse, never drop.
    if flags & 0x1D != 0 {
        return Err(());
    }
    Ok(text)
}

/// Incremental RESULT Rows builder: metadata (column specs), then row
/// values, then the finished body. One bounded batch, like every
/// result surface in this system.
pub struct RowsResult<'a> {
    out: &'a mut [u8],
    pos: usize,
    rows: u32,
    row_count_at: usize,
    columns: u16,
}

impl<'a> RowsResult<'a> {
    /// `columns`: `(name, cql_type)` per column; every row must push
    /// exactly this many values.
    pub fn new(
        out: &'a mut [u8],
        keyspace: &[u8],
        table: &[u8],
        columns: &[(&[u8], u16)],
    ) -> Option<Self> {
        let mut pos = 0usize;
        put(out, &mut pos, &RESULT_ROWS.to_be_bytes())?;
        // metadata: flags = 0x0001 (global table spec), column count.
        put(out, &mut pos, &1i32.to_be_bytes())?;
        put(out, &mut pos, &(columns.len() as i32).to_be_bytes())?;
        put(out, &mut pos, &(keyspace.len() as u16).to_be_bytes())?;
        put(out, &mut pos, keyspace)?;
        put(out, &mut pos, &(table.len() as u16).to_be_bytes())?;
        put(out, &mut pos, table)?;
        for (name, ty) in columns {
            put(out, &mut pos, &(name.len() as u16).to_be_bytes())?;
            put(out, &mut pos, name)?;
            put(out, &mut pos, &ty.to_be_bytes())?;
        }
        let row_count_at = pos;
        put(out, &mut pos, &0i32.to_be_bytes())?; // patched in finish
        Some(Self {
            out,
            pos,
            rows: 0,
            row_count_at,
            columns: columns.len() as u16,
        })
    }

    /// One value of the current row: `Some(bytes)` or `None` (null,
    /// encoded as length -1).
    pub fn value(&mut self, v: Option<&[u8]>) -> Option<()> {
        match v {
            Some(b) => {
                put(self.out, &mut self.pos, &(b.len() as i32).to_be_bytes())?;
                put(self.out, &mut self.pos, b)
            }
            None => put(self.out, &mut self.pos, &(-1i32).to_be_bytes()),
        }
    }

    /// Mark one row complete (values must have been pushed).
    pub fn end_row(&mut self) {
        self.rows += 1;
    }

    /// Patch the row count and return the body length.
    pub fn finish(self) -> usize {
        let rc = (self.rows as i32).to_be_bytes();
        self.out[self.row_count_at..self.row_count_at + 4].copy_from_slice(&rc);
        self.pos
    }
}

// ── CQL text parsing (the capability's own micro-front-end) ──────────
//
// Deliberately NOT the shared SQL front end: `sql_core`'s tree mounts
// `relational` as a sibling of `models`, and a consumer needing both
// would hold two non-unifying type trees (the #[path] trap). More to
// the point, CQL is not SQL — §14.15's rule is that models share
// foundations, not meanings, and a shared parser would be the first
// step toward the rejected generic engine. The subset here is small
// enough that a dedicated recognizer is the honest cost:
//
//   CREATE TABLE t (col type [, …] , PRIMARY KEY (col))
//   CREATE TABLE t (col type PRIMARY KEY [, …])
//   INSERT INTO t (col [, …]) VALUES (lit [, …])
//   SELECT * | col[, …] FROM t [WHERE pkcol = lit]
//
// Everything else refuses by name.

pub use models::LogicalType;

/// Bounded shapes.
pub const CQL_MAX_COLS: usize = 16;
pub const CQL_NAME_MAX: usize = 48;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CqlLit<'a> {
    Int(i64),
    Str(&'a [u8]),
    Bool(bool),
}

pub struct CqlCreate<'a> {
    pub table: &'a [u8],
    pub cols: [(&'a [u8], LogicalType); CQL_MAX_COLS],
    pub col_count: usize,
    /// Index into `cols` of the partition key column.
    pub pk: usize,
    /// Indices into `cols` of the clustering columns, in key order. A
    /// composite `PRIMARY KEY (partition, c1, c2)` makes `c1, c2` clustering
    /// columns: rows sharing a partition are ordered by them.
    pub clustering: [usize; CQL_MAX_CLUSTERING],
    pub clustering_count: usize,
}

/// The most clustering columns a wide-column primary key may declare.
pub const CQL_MAX_CLUSTERING: usize = 4;

pub struct CqlInsert<'a> {
    pub table: &'a [u8],
    pub cols: [&'a [u8]; CQL_MAX_COLS],
    pub values: [CqlLit<'a>; CQL_MAX_COLS],
    pub count: usize,
}

pub struct CqlSelect<'a> {
    pub table: &'a [u8],
    /// Empty = `*`.
    pub cols: [&'a [u8]; CQL_MAX_COLS],
    pub col_count: usize,
    pub where_pk: Option<(&'a [u8], CqlLit<'a>)>,
    /// Additional `AND col = lit` equality predicates — clustering-key
    /// components, matched against the schema's clustering order at
    /// execution (prefix rule).
    pub where_ck: [(&'a [u8], CqlLit<'a>); CQL_MAX_CLUSTERING],
    pub where_ck_count: usize,
}

/// `UPDATE t SET c = v [, c = v]… WHERE pk = k`. An upsert of the named
/// columns at a new statement version, like INSERT of a partial row.
pub struct CqlUpdate<'a> {
    pub table: &'a [u8],
    pub cols: [&'a [u8]; CQL_MAX_COLS],
    pub values: [CqlLit<'a>; CQL_MAX_COLS],
    pub count: usize,
    /// The partition key the SET applies to. Required.
    pub where_pk: (&'a [u8], CqlLit<'a>),
    /// `AND col = lit` clustering-key predicates. On a clustered table
    /// the FULL clustering key is required to name one row.
    pub where_ck: [(&'a [u8], CqlLit<'a>); CQL_MAX_CLUSTERING],
    pub where_ck_count: usize,
}

/// `DELETE FROM t WHERE pk = k [AND ck = v]…`. Removes every cell in
/// the named partition — or, with clustering predicates, every cell in
/// the clustering-prefix span they name (one row when the full
/// clustering key is given).
pub struct CqlDelete<'a> {
    pub table: &'a [u8],
    /// The partition key to remove. Required — no whole-table delete here.
    pub where_pk: (&'a [u8], CqlLit<'a>),
    /// `AND col = lit` clustering-key predicates (prefix rule).
    pub where_ck: [(&'a [u8], CqlLit<'a>); CQL_MAX_CLUSTERING],
    pub where_ck_count: usize,
}

#[allow(
    clippy::large_enum_variant,
    reason = "no_std, allocation-free: the inline fixed arrays are the point (same rationale as sql_core::Statement)"
)]
pub enum CqlStatement<'a> {
    Create(CqlCreate<'a>),
    Insert(CqlInsert<'a>),
    Select(CqlSelect<'a>),
    Update(CqlUpdate<'a>),
    Delete(CqlDelete<'a>),
}

struct Lexer<'a> {
    s: &'a [u8],
    at: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Tok<'a> {
    Word(&'a [u8]),
    Str(&'a [u8]),
    Int(i64),
    Punct(u8),
}

impl<'a> Lexer<'a> {
    fn new(s: &'a [u8]) -> Self {
        Self { s, at: 0 }
    }
    fn next(&mut self) -> Result<Option<Tok<'a>>, ()> {
        while self.at < self.s.len() && self.s[self.at].is_ascii_whitespace() {
            self.at += 1;
        }
        if self.at >= self.s.len() {
            return Ok(None);
        }
        let c = self.s[self.at];
        if c == b'\'' {
            let start = self.at + 1;
            let mut i = start;
            while i < self.s.len() && self.s[i] != b'\'' {
                i += 1;
            }
            if i >= self.s.len() {
                return Err(()); // unterminated
            }
            self.at = i + 1;
            return Ok(Some(Tok::Str(&self.s[start..i])));
        }
        if c.is_ascii_digit()
            || (c == b'-' && self.s.get(self.at + 1).is_some_and(|d| d.is_ascii_digit()))
        {
            let start = self.at;
            self.at += 1;
            while self.at < self.s.len() && self.s[self.at].is_ascii_digit() {
                self.at += 1;
            }
            let mut v: i64 = 0;
            let neg = self.s[start] == b'-';
            for &d in &self.s[if neg { start + 1 } else { start }..self.at] {
                v = v
                    .checked_mul(10)
                    .ok_or(())?
                    .checked_add((d - b'0') as i64)
                    .ok_or(())?;
            }
            return Ok(Some(Tok::Int(if neg { -v } else { v })));
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = self.at;
            while self.at < self.s.len()
                && (self.s[self.at].is_ascii_alphanumeric() || self.s[self.at] == b'_')
            {
                self.at += 1;
            }
            return Ok(Some(Tok::Word(&self.s[start..self.at])));
        }
        if matches!(c, b'(' | b')' | b',' | b'=' | b'*' | b';' | b'.') {
            self.at += 1;
            return Ok(Some(Tok::Punct(c)));
        }
        Err(())
    }
    fn expect_word(&mut self, kw: &[u8]) -> Result<(), ()> {
        match self.next()? {
            Some(Tok::Word(w)) if eq_kw(w, kw) => Ok(()),
            _ => Err(()),
        }
    }
    fn expect_punct(&mut self, p: u8) -> Result<(), ()> {
        match self.next()? {
            Some(Tok::Punct(c)) if c == p => Ok(()),
            _ => Err(()),
        }
    }
    fn word(&mut self) -> Result<&'a [u8], ()> {
        match self.next()? {
            Some(Tok::Word(w)) if w.len() <= CQL_NAME_MAX => Ok(w),
            _ => Err(()),
        }
    }
}

fn eq_kw(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_ascii_lowercase() == *y)
}

fn cql_type(w: &[u8]) -> Result<LogicalType, ()> {
    if eq_kw(w, b"int") {
        Ok(LogicalType::Int)
    } else if eq_kw(w, b"bigint") {
        Ok(LogicalType::BigInt)
    } else if eq_kw(w, b"text") || eq_kw(w, b"varchar") {
        Ok(LogicalType::VarChar {
            length: models::MAX_TEXT_LEN as u16,
        })
    } else if eq_kw(w, b"boolean") {
        Ok(LogicalType::Boolean)
    } else if eq_kw(w, b"double") {
        Ok(LogicalType::Double)
    } else {
        Err(()) // counter, uuid, collections…: named refusals
    }
}

fn literal<'a>(t: Option<Tok<'a>>) -> Result<CqlLit<'a>, ()> {
    match t {
        Some(Tok::Int(v)) => Ok(CqlLit::Int(v)),
        Some(Tok::Str(s)) => Ok(CqlLit::Str(s)),
        Some(Tok::Word(w)) if eq_kw(w, b"true") => Ok(CqlLit::Bool(true)),
        Some(Tok::Word(w)) if eq_kw(w, b"false") => Ok(CqlLit::Bool(false)),
        _ => Err(()),
    }
}

/// Parse one CQL statement of the supported subset.
pub fn parse_cql(text: &[u8]) -> Result<CqlStatement<'_>, ()> {
    let mut lx = Lexer::new(text);
    match lx.next()? {
        Some(Tok::Word(w)) if eq_kw(w, b"create") => {
            lx.expect_word(b"table")?;
            let table = lx.word()?;
            lx.expect_punct(b'(')?;
            let mut c = CqlCreate {
                table,
                cols: [(b"".as_slice(), LogicalType::Int); CQL_MAX_COLS],
                col_count: 0,
                pk: usize::MAX,
                clustering: [0; CQL_MAX_CLUSTERING],
                clustering_count: 0,
            };
            loop {
                match lx.next()? {
                    // PRIMARY KEY (partition [, clustering…]) tail form.
                    Some(Tok::Word(w)) if eq_kw(w, b"primary") => {
                        lx.expect_word(b"key")?;
                        lx.expect_punct(b'(')?;
                        if c.pk != usize::MAX {
                            return Err(());
                        }
                        // First key column is the partition key; the rest are
                        // clustering columns, in declared order.
                        let mut first = true;
                        loop {
                            let name = lx.word()?;
                            let Some(i) =
                                c.cols[..c.col_count].iter().position(|(n, _)| *n == name)
                            else {
                                return Err(());
                            };
                            if first {
                                c.pk = i;
                                first = false;
                            } else {
                                if c.clustering_count >= CQL_MAX_CLUSTERING {
                                    return Err(());
                                }
                                c.clustering[c.clustering_count] = i;
                                c.clustering_count += 1;
                            }
                            match lx.next()? {
                                Some(Tok::Punct(b',')) => continue,
                                Some(Tok::Punct(b')')) => break,
                                _ => return Err(()),
                            }
                        }
                        match lx.next()? {
                            Some(Tok::Punct(b')')) => break,
                            Some(Tok::Punct(b',')) => continue,
                            _ => return Err(()),
                        }
                    }
                    Some(Tok::Word(name)) => {
                        if c.col_count >= CQL_MAX_COLS || name.len() > CQL_NAME_MAX {
                            return Err(());
                        }
                        let ty = cql_type(lx.word()?)?;
                        c.cols[c.col_count] = (name, ty);
                        c.col_count += 1;
                        // Optional inline PRIMARY KEY.
                        match lx.next()? {
                            Some(Tok::Word(w)) if eq_kw(w, b"primary") => {
                                lx.expect_word(b"key")?;
                                if c.pk != usize::MAX {
                                    return Err(());
                                }
                                c.pk = c.col_count - 1;
                                match lx.next()? {
                                    Some(Tok::Punct(b')')) => break,
                                    Some(Tok::Punct(b',')) => continue,
                                    _ => return Err(()),
                                }
                            }
                            Some(Tok::Punct(b')')) => break,
                            Some(Tok::Punct(b',')) => continue,
                            _ => return Err(()),
                        }
                    }
                    _ => return Err(()),
                }
            }
            end(&mut lx)?;
            if c.pk == usize::MAX || c.col_count == 0 {
                return Err(());
            }
            Ok(CqlStatement::Create(c))
        }
        Some(Tok::Word(w)) if eq_kw(w, b"insert") => {
            lx.expect_word(b"into")?;
            let table = lx.word()?;
            lx.expect_punct(b'(')?;
            let mut ins = CqlInsert {
                table,
                cols: [b"".as_slice(); CQL_MAX_COLS],
                values: [CqlLit::Int(0); CQL_MAX_COLS],
                count: 0,
            };
            loop {
                let name = lx.word()?;
                if ins.count >= CQL_MAX_COLS {
                    return Err(());
                }
                ins.cols[ins.count] = name;
                ins.count += 1;
                match lx.next()? {
                    Some(Tok::Punct(b',')) => continue,
                    Some(Tok::Punct(b')')) => break,
                    _ => return Err(()),
                }
            }
            lx.expect_word(b"values")?;
            lx.expect_punct(b'(')?;
            for i in 0..ins.count {
                ins.values[i] = literal(lx.next()?)?;
                let sep = lx.next()?;
                match sep {
                    Some(Tok::Punct(b',')) if i + 1 < ins.count => {}
                    Some(Tok::Punct(b')')) if i + 1 == ins.count => {}
                    _ => return Err(()),
                }
            }
            end(&mut lx)?;
            Ok(CqlStatement::Insert(ins))
        }
        Some(Tok::Word(w)) if eq_kw(w, b"select") => {
            let mut sel = CqlSelect {
                table: b"",
                cols: [b"".as_slice(); CQL_MAX_COLS],
                col_count: 0,
                where_pk: None,
                where_ck: [(b"".as_slice(), CqlLit::Int(0)); CQL_MAX_CLUSTERING],
                where_ck_count: 0,
            };
            match lx.next()? {
                Some(Tok::Punct(b'*')) => {
                    lx.expect_word(b"from")?;
                }
                Some(Tok::Word(first)) => {
                    sel.cols[0] = first;
                    sel.col_count = 1;
                    loop {
                        match lx.next()? {
                            Some(Tok::Punct(b',')) => {
                                if sel.col_count >= CQL_MAX_COLS {
                                    return Err(());
                                }
                                sel.cols[sel.col_count] = lx.word()?;
                                sel.col_count += 1;
                            }
                            Some(Tok::Word(w)) if eq_kw(w, b"from") => break,
                            _ => return Err(()),
                        }
                    }
                }
                _ => return Err(()),
            }
            sel.table = lx.word()?;
            match lx.next()? {
                None => {}
                Some(Tok::Punct(b';')) => end_after_semi(&mut lx)?,
                Some(Tok::Word(w)) if eq_kw(w, b"where") => {
                    let col = lx.word()?;
                    lx.expect_punct(b'=')?;
                    let lit = literal(lx.next()?)?;
                    sel.where_pk = Some((col, lit));
                    let (ck, ckn) = where_and_chain(&mut lx)?;
                    sel.where_ck = ck;
                    sel.where_ck_count = ckn;
                }
                _ => return Err(()),
            }
            Ok(CqlStatement::Select(sel))
        }
        Some(Tok::Word(w)) if eq_kw(w, b"update") => {
            let table = lx.word()?;
            lx.expect_word(b"set")?;
            let mut cols = [b"".as_slice(); CQL_MAX_COLS];
            let mut values = [CqlLit::Int(0); CQL_MAX_COLS];
            let mut count = 0usize;
            loop {
                if count >= CQL_MAX_COLS {
                    return Err(());
                }
                cols[count] = lx.word()?;
                lx.expect_punct(b'=')?;
                values[count] = literal(lx.next()?)?;
                count += 1;
                match lx.next()? {
                    Some(Tok::Punct(b',')) => continue,
                    Some(Tok::Word(w)) if eq_kw(w, b"where") => break,
                    _ => return Err(()),
                }
            }
            let col = lx.word()?;
            lx.expect_punct(b'=')?;
            let lit = literal(lx.next()?)?;
            let (where_ck, where_ck_count) = where_and_chain(&mut lx)?;
            Ok(CqlStatement::Update(CqlUpdate {
                table,
                cols,
                values,
                count,
                where_pk: (col, lit),
                where_ck,
                where_ck_count,
            }))
        }
        Some(Tok::Word(w)) if eq_kw(w, b"delete") => {
            lx.expect_word(b"from")?;
            let table = lx.word()?;
            lx.expect_word(b"where")?;
            let col = lx.word()?;
            lx.expect_punct(b'=')?;
            let lit = literal(lx.next()?)?;
            let (where_ck, where_ck_count) = where_and_chain(&mut lx)?;
            Ok(CqlStatement::Delete(CqlDelete {
                table,
                where_pk: (col, lit),
                where_ck,
                where_ck_count,
            }))
        }
        _ => Err(()),
    }
}

/// Parse `[AND col = lit]…` to the end of the statement (consuming the
/// terminator like `end` does). Bounded by `CQL_MAX_CLUSTERING`.
#[allow(
    clippy::type_complexity,
    reason = "no_std, allocation-free: the fixed pair array is the point"
)]
fn where_and_chain<'a>(
    lx: &mut Lexer<'a>,
) -> Result<([(&'a [u8], CqlLit<'a>); CQL_MAX_CLUSTERING], usize), ()> {
    let mut ck = [(b"".as_slice(), CqlLit::Int(0)); CQL_MAX_CLUSTERING];
    let mut n = 0usize;
    loop {
        match lx.next()? {
            None => return Ok((ck, n)),
            Some(Tok::Punct(b';')) => {
                end_after_semi(lx)?;
                return Ok((ck, n));
            }
            Some(Tok::Word(w)) if eq_kw(w, b"and") => {
                if n >= CQL_MAX_CLUSTERING {
                    return Err(());
                }
                let col = lx.word()?;
                lx.expect_punct(b'=')?;
                let lit = literal(lx.next()?)?;
                ck[n] = (col, lit);
                n += 1;
            }
            _ => return Err(()),
        }
    }
}

fn end(lx: &mut Lexer<'_>) -> Result<(), ()> {
    match lx.next()? {
        None => Ok(()),
        Some(Tok::Punct(b';')) => end_after_semi(lx),
        _ => Err(()),
    }
}

fn end_after_semi(lx: &mut Lexer<'_>) -> Result<(), ()> {
    match lx.next()? {
        None => Ok(()),
        _ => Err(()),
    }
}

// ── Wide-table schema record (the capability's catalog value) ────────
//
// Stored under `models::encode_model_catalog_key(db, Table, id)` in
// KS_MODEL_CATALOG. Versioned like every persistent record:
//
// ```text
// [version:u16][payload_len:u16]
// [table_id:u32][pk_index:u8][col_count:u8]
// col_count × [name_len:u8][name…][type_tag:u8][type_param:u16]
// ```
//
// type_tag: 1=Int 2=BigInt 3=VarChar(param=length) 4=Boolean 5=Double.

pub const WIDE_SCHEMA_VERSION: u16 = 1;
pub const WIDE_SCHEMA_MAX: usize = 8 + CQL_MAX_COLS * (1 + CQL_NAME_MAX + 3);

pub struct WideSchema {
    pub table_id: u32,
    pub pk: usize,
    pub cols: [([u8; CQL_NAME_MAX], u8, LogicalType); CQL_MAX_COLS],
    pub col_count: usize,
    pub clustering: [usize; CQL_MAX_CLUSTERING],
    pub clustering_count: usize,
}

impl WideSchema {
    pub const EMPTY: WideSchema = WideSchema {
        table_id: 0,
        pk: 0,
        cols: [([0; CQL_NAME_MAX], 0, LogicalType::Int); CQL_MAX_COLS],
        col_count: 0,
        clustering: [0; CQL_MAX_CLUSTERING],
        clustering_count: 0,
    };

    /// Is column index `i` a clustering column?
    pub fn is_clustering(&self, i: usize) -> bool {
        self.clustering[..self.clustering_count].contains(&i)
    }

    pub fn col_name(&self, i: usize) -> &[u8] {
        &self.cols[i].0[..self.cols[i].1 as usize]
    }

    pub fn col_type(&self, i: usize) -> LogicalType {
        self.cols[i].2
    }

    pub fn col_by_name(&self, name: &[u8]) -> Option<usize> {
        (0..self.col_count).find(|&i| self.col_name(i) == name)
    }

    pub fn from_create(table_id: u32, c: &CqlCreate<'_>) -> Option<Self> {
        let mut s = Self::EMPTY;
        s.table_id = table_id;
        s.pk = c.pk;
        s.col_count = c.col_count;
        s.clustering_count = c.clustering_count;
        s.clustering[..c.clustering_count].copy_from_slice(&c.clustering[..c.clustering_count]);
        for i in 0..c.col_count {
            let (name, ty) = c.cols[i];
            if name.len() > CQL_NAME_MAX {
                return None;
            }
            s.cols[i].0[..name.len()].copy_from_slice(name);
            s.cols[i].1 = name.len() as u8;
            s.cols[i].2 = ty;
        }
        Some(s)
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if self.col_count == 0 || self.col_count > CQL_MAX_COLS || self.pk >= self.col_count {
            return None;
        }
        let mut n = 4usize;
        *out.get_mut(n..n + 4)?.first_chunk_mut::<4>()? = self.table_id.to_le_bytes();
        n += 4;
        *out.get_mut(n)? = self.pk as u8;
        n += 1;
        *out.get_mut(n)? = self.col_count as u8;
        n += 1;
        for i in 0..self.col_count {
            let name = self.col_name(i);
            *out.get_mut(n)? = name.len() as u8;
            n += 1;
            out.get_mut(n..n + name.len())?.copy_from_slice(name);
            n += name.len();
            let (tag, param): (u8, u16) = match self.col_type(i) {
                LogicalType::Int => (1, 0),
                LogicalType::BigInt => (2, 0),
                LogicalType::VarChar { length } => (3, length),
                LogicalType::Boolean => (4, 0),
                LogicalType::Double => (5, 0),
                _ => return None,
            };
            *out.get_mut(n)? = tag;
            n += 1;
            *out.get_mut(n..n + 2)?.first_chunk_mut::<2>()? = param.to_le_bytes();
            n += 2;
        }
        // Clustering columns: [count][idx…].
        *out.get_mut(n)? = self.clustering_count as u8;
        n += 1;
        for i in 0..self.clustering_count {
            *out.get_mut(n)? = self.clustering[i] as u8;
            n += 1;
        }
        out.get_mut(0..2)?
            .copy_from_slice(&WIDE_SCHEMA_VERSION.to_le_bytes());
        out.get_mut(2..4)?
            .copy_from_slice(&((n - 4) as u16).to_le_bytes());
        Some(n)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        let version = u16::from_le_bytes(src.get(0..2)?.try_into().ok()?);
        if version != WIDE_SCHEMA_VERSION {
            return None;
        }
        let payload = u16::from_le_bytes(src.get(2..4)?.try_into().ok()?) as usize;
        if src.len() != 4 + payload {
            return None;
        }
        let mut s = Self::EMPTY;
        s.table_id = u32::from_le_bytes(src.get(4..8)?.try_into().ok()?);
        let pk = *src.get(8)? as usize;
        let col_count = *src.get(9)? as usize;
        if col_count == 0 || col_count > CQL_MAX_COLS || pk >= col_count {
            return None;
        }
        s.pk = pk;
        s.col_count = col_count;
        let mut n = 10usize;
        for i in 0..col_count {
            let nl = *src.get(n)? as usize;
            n += 1;
            if nl == 0 || nl > CQL_NAME_MAX {
                return None;
            }
            s.cols[i].0[..nl].copy_from_slice(src.get(n..n + nl)?);
            s.cols[i].1 = nl as u8;
            n += nl;
            let tag = *src.get(n)?;
            n += 1;
            let param = u16::from_le_bytes(src.get(n..n + 2)?.try_into().ok()?);
            n += 2;
            s.cols[i].2 = match tag {
                1 => LogicalType::Int,
                2 => LogicalType::BigInt,
                3 => LogicalType::VarChar { length: param },
                4 => LogicalType::Boolean,
                5 => LogicalType::Double,
                _ => return None,
            };
        }
        // Clustering columns.
        let cc = *src.get(n)? as usize;
        n += 1;
        if cc > CQL_MAX_CLUSTERING {
            return None;
        }
        s.clustering_count = cc;
        for i in 0..cc {
            let idx = *src.get(n)? as usize;
            n += 1;
            if idx >= col_count {
                return None;
            }
            s.clustering[i] = idx;
        }
        if n != src.len() {
            return None;
        }
        Some(s)
    }
}

/// The CQL wire type id a schema column renders as.
pub fn cql_type_of(ty: LogicalType) -> u16 {
    match ty {
        LogicalType::Int => TYPE_INT,
        LogicalType::BigInt => TYPE_BIGINT,
        LogicalType::Boolean => TYPE_BOOLEAN,
        LogicalType::Double => TYPE_DOUBLE,
        _ => TYPE_VARCHAR,
    }
}
