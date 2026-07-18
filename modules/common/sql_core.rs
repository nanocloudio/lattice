//! SQL front end for the relational capability (RFC database
//! foundation §14.2: "name resolution, binding, and semantic
//! validation" as a SHARED module, not a per-connector one).
//!
//! Dual-target facade: the same source compiles into the no_std PIC
//! `relational_executor` and is host-tested here. Heap-free — every
//! bound form is a fixed-size value and every literal borrows the
//! original SQL text.
//!
//! ## Why the connectors do not each parse
//!
//! §14.3 lets a connector own its dialect syntax but forbids it from
//! introducing "connector-specific table formats, transaction managers,
//! indexes, catalogs of record, or persistence paths". If PostgreSQL and
//! MySQL each had their own parser AND binder, they would inevitably
//! disagree about what `INSERT INTO t VALUES (1)` means for a table with
//! two columns — and the disagreement would be invisible until someone
//! compared the two. So the dialect surface is a [`Dialect`] flag on ONE
//! parser, and binding is shared outright.
//!
//! ## Parse and bind are separate passes, deliberately
//!
//! Parsing produces a [`Statement`] that names things; binding resolves
//! those names against a [`TableDescriptor`] and produces typed values.
//! They cannot be one pass because the parser does not have the catalog
//! — the executor has to read the descriptor out of the store first, and
//! that is an I/O round trip. Keeping them separate is also what makes
//! `INSERT INTO t VALUES (…)` bind its literals to the COLUMN's declared
//! type rather than guessing a type from the literal's syntax: `1` binds
//! to SMALLINT, INT, BIGINT or DOUBLE depending on the column, and
//! refuses for TEXT rather than stringifying itself.
//!
//! ## What this subset covers, and what it refuses
//!
//! `CREATE TABLE`, `INSERT`, `SELECT` with an optional equality
//! predicate, plus the session statements a real `psql` or `mariadb`
//! client sends before it will let a user type anything. Everything else
//! is [`Statement::Unsupported`] — a NAMED refusal that the connector
//! turns into a proper error. That matters more than it sounds: §14.3
//! requires compatibility behaviour that cannot be represented honestly
//! to be "rejected or reported as unsupported rather than silently
//! approximated", and the most tempting approximation in a SQL front end
//! is to ignore the clause you do not understand. Ignoring a `WHERE`
//! returns the wrong rows; ignoring a `JOIN` returns rows from one table
//! and calls it an answer. Both are worse than an error.

#[path = "relational.rs"]
pub mod relational;

use relational::{
    ColumnDescriptor, LogicalType, Name, TableDescriptor, MAX_COLUMNS, MAX_KEY_COLUMNS,
    MAX_NAME_LEN, MAX_TEXT_LEN,
};

// ── Capacities ────────────────────────────────────────────────────────

/// Longest statement text accepted. A `CREATE TABLE` with 32 columns
/// and comfortable names fits well inside this.
pub const MAX_SQL_LEN: usize = 4096;

/// Columns nameable in one projection or insert column list.
pub const MAX_LIST_COLS: usize = 32;

/// `VALUES` tuples in one `INSERT`. Bounded because each is executed as
/// its own durable write and the executor holds them all in state while
/// the batch is in flight.
pub const MAX_INSERT_ROWS: usize = 8;

// ── Dialect ───────────────────────────────────────────────────────────

/// Which connector's syntax this text came from. The parser is shared;
/// this only selects the small set of places the two genuinely differ.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    MySql,
}

// ── Errors ────────────────────────────────────────────────────────────

/// Why a statement could not be parsed or bound.
///
/// Every variant is a distinct, reportable condition. There is no
/// catch-all `Invalid`, because a connector has to map these to protocol
/// error codes — `UnknownColumn` is a different SQLSTATE from
/// `TypeMismatch`, and a client's error handling depends on the
/// difference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlError {
    /// The text is longer than [`MAX_SQL_LEN`].
    TooLong,
    /// Ran out of input mid-statement.
    UnexpectedEnd,
    /// A token the grammar does not allow here.
    Syntax,
    /// An identifier longer than [`MAX_NAME_LEN`].
    NameTooLong,
    /// A string literal longer than [`MAX_TEXT_LEN`].
    LiteralTooLong,
    /// More list columns / value tuples / table columns than the fixed
    /// arrays hold. A refusal, never a truncation: a silently dropped
    /// column would be a silently wrong row.
    TooManyItems,
    /// An unclosed string literal or quoted identifier.
    UnterminatedLiteral,
    /// A type name the logical type system does not have.
    UnknownType,
    /// A column name not in the table.
    UnknownColumn,
    /// A literal that cannot inhabit its column's declared type.
    TypeMismatch,
    /// `VALUES` arity does not match the column list (explicit or
    /// implied).
    ArityMismatch,
    /// A NULL, or an omitted column, where the schema forbids it.
    NotNull,
    /// No primary key was declared. Required: a table whose rows have no
    /// key has no addressable row.
    MissingPrimaryKey,
    /// A duplicate column name in `CREATE TABLE`, or a duplicate target
    /// in an `INSERT` column list.
    DuplicateColumn,
    /// The statement parsed but this subset does not implement it.
    /// Named rather than ignored — see the module docs.
    Unsupported,
}

// ── Lexer ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Tok<'a> {
    /// A bare or quoted identifier. Bare identifiers are NOT
    /// case-folded here; comparison is case-insensitive instead (see
    /// [`eq_ident`]), which keeps a quoted `"Foo"` distinguishable from
    /// a bare `foo` if a later dialect rule needs it.
    Ident(&'a [u8]),
    /// A keyword, upper-cased for comparison at the call site via
    /// [`kw`].
    Word(&'a [u8]),
    Int(i64),
    Float(f64),
    Str(&'a [u8]),
    Punct(u8),
    /// `?` (MySQL) or `$n` (Postgres) placeholder. Recognised so the
    /// error can say "parameters unsupported" rather than "syntax
    /// error", which is a much more useful thing to tell a client whose
    /// driver just used a prepared statement.
    Param,
}

struct Lexer<'a> {
    src: &'a [u8],
    at: usize,
}

fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

fn is_digit(b: u8) -> bool {
    b.is_ascii_digit()
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// ASCII-case-insensitive byte comparison. SQL identifiers and keywords
/// are case-insensitive; only ASCII folds, which is correct because a
/// non-ASCII identifier can only arrive quoted and quoted identifiers
/// are compared exactly.
pub fn eq_ident(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// True iff `t` is the keyword `k` (case-insensitive).
fn kw(t: Tok<'_>, k: &[u8]) -> bool {
    match t {
        Tok::Word(w) | Tok::Ident(w) => eq_ident(w, k),
        _ => false,
    }
}

impl<'a> Lexer<'a> {
    fn new(src: &'a [u8]) -> Self {
        Self { src, at: 0 }
    }

    fn skip_trivia(&mut self) {
        loop {
            while self.at < self.src.len() && is_space(self.src[self.at]) {
                self.at += 1;
            }
            // `-- line comment`
            if self.at + 1 < self.src.len()
                && self.src[self.at] == b'-'
                && self.src[self.at + 1] == b'-'
            {
                while self.at < self.src.len() && self.src[self.at] != b'\n' {
                    self.at += 1;
                }
                continue;
            }
            // `/* block comment */`
            if self.at + 1 < self.src.len()
                && self.src[self.at] == b'/'
                && self.src[self.at + 1] == b'*'
            {
                self.at += 2;
                while self.at + 1 < self.src.len()
                    && !(self.src[self.at] == b'*' && self.src[self.at + 1] == b'/')
                {
                    self.at += 1;
                }
                self.at = (self.at + 2).min(self.src.len());
                continue;
            }
            return;
        }
    }

    fn next(&mut self) -> Result<Option<Tok<'a>>, SqlError> {
        self.skip_trivia();
        if self.at >= self.src.len() {
            return Ok(None);
        }
        let b = self.src[self.at];

        // Quoted identifier: `"pg"` or `` `mysql` ``. Both accepted in
        // both dialects — accepting the other's quoting costs nothing
        // and refusing it would reject valid-looking client SQL for no
        // benefit.
        if b == b'"' || b == b'`' {
            let close = b;
            self.at += 1;
            let start = self.at;
            while self.at < self.src.len() && self.src[self.at] != close {
                self.at += 1;
            }
            if self.at >= self.src.len() {
                return Err(SqlError::UnterminatedLiteral);
            }
            let s = &self.src[start..self.at];
            self.at += 1;
            if s.len() > MAX_NAME_LEN {
                return Err(SqlError::NameTooLong);
            }
            return Ok(Some(Tok::Ident(s)));
        }

        // String literal. `''` inside is an escaped quote, so a literal
        // containing a quote must not terminate early — but this subset
        // cannot represent the unescaped form in a borrowed slice, so it
        // refuses rather than returning the wrong bytes.
        if b == b'\'' {
            self.at += 1;
            let start = self.at;
            while self.at < self.src.len() {
                if self.src[self.at] == b'\'' {
                    if self.at + 1 < self.src.len() && self.src[self.at + 1] == b'\'' {
                        // Escaped quote: unrepresentable without a copy,
                        // and returning the raw `''` would hand back a
                        // value with a character the user did not write.
                        return Err(SqlError::Unsupported);
                    }
                    break;
                }
                self.at += 1;
            }
            if self.at >= self.src.len() {
                return Err(SqlError::UnterminatedLiteral);
            }
            let s = &self.src[start..self.at];
            self.at += 1;
            if s.len() > MAX_TEXT_LEN {
                return Err(SqlError::LiteralTooLong);
            }
            return Ok(Some(Tok::Str(s)));
        }

        if is_ident_start(b) {
            let start = self.at;
            while self.at < self.src.len() && is_ident_char(self.src[self.at]) {
                self.at += 1;
            }
            let s = &self.src[start..self.at];
            if s.len() > MAX_NAME_LEN {
                return Err(SqlError::NameTooLong);
            }
            return Ok(Some(Tok::Word(s)));
        }

        if is_digit(b) || (b == b'-' && self.peek_digit(1)) {
            return self.number();
        }

        // Placeholders.
        if b == b'?' {
            self.at += 1;
            return Ok(Some(Tok::Param));
        }
        if b == b'$' && self.peek_digit(1) {
            self.at += 1;
            while self.at < self.src.len() && is_digit(self.src[self.at]) {
                self.at += 1;
            }
            return Ok(Some(Tok::Param));
        }

        self.at += 1;
        Ok(Some(Tok::Punct(b)))
    }

    fn peek_digit(&self, ahead: usize) -> bool {
        self.src.get(self.at + ahead).copied().is_some_and(is_digit)
    }

    fn number(&mut self) -> Result<Option<Tok<'a>>, SqlError> {
        let start = self.at;
        if self.src[self.at] == b'-' {
            self.at += 1;
        }
        let mut is_float = false;
        while self.at < self.src.len() {
            let c = self.src[self.at];
            if is_digit(c) {
                self.at += 1;
            } else if c == b'.' && !is_float {
                is_float = true;
                self.at += 1;
            } else {
                break;
            }
        }
        let text = &self.src[start..self.at];
        if is_float {
            Ok(Some(Tok::Float(parse_f64(text).ok_or(SqlError::Syntax)?)))
        } else {
            Ok(Some(Tok::Int(parse_i64(text).ok_or(SqlError::Syntax)?)))
        }
    }
}

/// Parse a decimal integer, rejecting overflow rather than wrapping. A
/// wrapped literal would silently store a different number than the user
/// wrote.
fn parse_i64(text: &[u8]) -> Option<i64> {
    let (neg, digits) = match text.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, text),
    };
    if digits.is_empty() {
        return None;
    }
    let mut acc: i64 = 0;
    for &d in digits {
        if !is_digit(d) {
            return None;
        }
        acc = acc.checked_mul(10)?;
        // Accumulate negatives directly so `i64::MIN` is representable —
        // negating a positive accumulator would overflow on that one
        // value.
        acc = if neg {
            acc.checked_sub((d - b'0') as i64)?
        } else {
            acc.checked_add((d - b'0') as i64)?
        };
    }
    Some(acc)
}

/// Parse a decimal float. Deliberately simple (no exponent form): the
/// forms this subset accepts are the forms it can round-trip, and an
/// exponent literal is `Unsupported` rather than mis-parsed.
fn parse_f64(text: &[u8]) -> Option<f64> {
    let (neg, rest) = match text.split_first() {
        Some((b'-', r)) => (true, r),
        _ => (false, text),
    };
    let mut int_part: f64 = 0.0;
    let mut frac: f64 = 0.0;
    let mut scale: f64 = 1.0;
    let mut seen_dot = false;
    let mut digits = 0usize;
    for &c in rest {
        if c == b'.' {
            if seen_dot {
                return None;
            }
            seen_dot = true;
            continue;
        }
        if !is_digit(c) {
            return None;
        }
        digits += 1;
        if seen_dot {
            scale *= 10.0;
            frac = frac * 10.0 + (c - b'0') as f64;
        } else {
            int_part = int_part * 10.0 + (c - b'0') as f64;
        }
    }
    if digits == 0 {
        return None;
    }
    let v = int_part + frac / scale;
    Some(if neg { -v } else { v })
}

// ── Token stream with one-token lookahead ─────────────────────────────

struct Parser<'a> {
    lex: Lexer<'a>,
    peeked: Option<Option<Tok<'a>>>,
    dialect: Dialect,
}

impl<'a> Parser<'a> {
    fn new(src: &'a [u8], dialect: Dialect) -> Self {
        Self {
            lex: Lexer::new(src),
            peeked: None,
            dialect,
        }
    }

    fn peek(&mut self) -> Result<Option<Tok<'a>>, SqlError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.lex.next()?);
        }
        Ok(self.peeked.unwrap())
    }

    fn bump(&mut self) -> Result<Option<Tok<'a>>, SqlError> {
        match self.peeked.take() {
            Some(t) => Ok(t),
            None => self.lex.next(),
        }
    }

    fn need(&mut self) -> Result<Tok<'a>, SqlError> {
        self.bump()?.ok_or(SqlError::UnexpectedEnd)
    }

    fn eat_punct(&mut self, c: u8) -> Result<bool, SqlError> {
        if self.peek()? == Some(Tok::Punct(c)) {
            self.bump()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn expect_punct(&mut self, c: u8) -> Result<(), SqlError> {
        if self.eat_punct(c)? {
            Ok(())
        } else {
            Err(SqlError::Syntax)
        }
    }

    fn eat_kw(&mut self, k: &[u8]) -> Result<bool, SqlError> {
        match self.peek()? {
            Some(t) if kw(t, k) => {
                self.bump()?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn expect_kw(&mut self, k: &[u8]) -> Result<(), SqlError> {
        if self.eat_kw(k)? {
            Ok(())
        } else {
            Err(SqlError::Syntax)
        }
    }

    /// An identifier or a non-reserved word used as a name.
    fn ident(&mut self) -> Result<&'a [u8], SqlError> {
        match self.need()? {
            Tok::Ident(s) | Tok::Word(s) => Ok(s),
            _ => Err(SqlError::Syntax),
        }
    }

    /// A possibly-qualified name (`schema.table`, `db.table`). The
    /// qualifier is DISCARDED, and that is a deliberate limitation
    /// rather than an oversight: this build has one schema, so accepting
    /// `public.t` and treating it as `t` matches what the client means,
    /// while pretending to support multiple schemas would be the silent
    /// approximation §14.3 forbids. A qualifier that is not the
    /// well-known default is refused below.
    fn table_name(&mut self) -> Result<&'a [u8], SqlError> {
        let first = self.ident()?;
        if self.eat_punct(b'.')? {
            // `qualifier.name` — only the conventional defaults are
            // accepted, so a genuine multi-schema query fails loudly.
            if !(eq_ident(first, b"public") || eq_ident(first, b"def")) {
                return Err(SqlError::Unsupported);
            }
            let second = self.ident()?;
            if self.eat_punct(b'.')? {
                return self.ident();
            }
            return Ok(second);
        }
        Ok(first)
    }
}

// ── Parsed (unbound) statements ───────────────────────────────────────

/// A literal exactly as written, before it knows its column's type.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Literal<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'a [u8]),
}

/// One column in a `CREATE TABLE`.
#[derive(Clone, Copy)]
pub struct ColumnSpec<'a> {
    pub name: &'a [u8],
    pub ty: LogicalType,
    pub nullable: bool,
    pub primary_key: bool,
}

impl ColumnSpec<'_> {
    const EMPTY: ColumnSpec<'static> = ColumnSpec {
        name: b"",
        ty: LogicalType::Null,
        nullable: true,
        primary_key: false,
    };
}

pub struct CreateTable<'a> {
    pub name: &'a [u8],
    /// `IF NOT EXISTS` was present, so an existing table is a success
    /// rather than an error.
    pub if_not_exists: bool,
    cols: [ColumnSpec<'a>; MAX_COLUMNS],
    col_count: usize,
    /// Key columns by position in `cols`, in declaration order — which
    /// is the order a composite key sorts in, so it must be preserved.
    pk: [u16; MAX_KEY_COLUMNS],
    pk_count: usize,
}

impl<'a> CreateTable<'a> {
    pub fn columns(&self) -> &[ColumnSpec<'a>] {
        &self.cols[..self.col_count]
    }

    pub fn primary_key_positions(&self) -> &[u16] {
        &self.pk[..self.pk_count]
    }
}

pub struct Insert<'a> {
    pub table: &'a [u8],
    /// Explicit column list, empty when the statement used positional
    /// `VALUES` over every column.
    cols: [&'a [u8]; MAX_LIST_COLS],
    col_count: usize,
    rows: [[Literal<'a>; MAX_LIST_COLS]; MAX_INSERT_ROWS],
    row_widths: [usize; MAX_INSERT_ROWS],
    row_count: usize,
}

impl<'a> Insert<'a> {
    pub fn columns(&self) -> &[&'a [u8]] {
        &self.cols[..self.col_count]
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn row(&self, i: usize) -> Option<&[Literal<'a>]> {
        if i >= self.row_count {
            return None;
        }
        Some(&self.rows[i][..self.row_widths[i]])
    }
}

/// What a `SELECT` projects.
///
/// `Columns` carries its fixed name array inline, which makes the enum
/// as large as that array. Deliberate: this is a no_std, allocation-free
/// path, so the alternative to an inline array is not a smaller enum but
/// a lifetime tied to some caller-owned buffer that every consumer would
/// have to thread through. One large stack value beats that.
#[allow(
    clippy::large_enum_variant,
    reason = "no_std, allocation-free: the inline fixed array is the point, and boxing is not available"
)]
pub enum Projection<'a> {
    /// `SELECT *`
    Star,
    Columns {
        names: [&'a [u8]; MAX_LIST_COLS],
        count: usize,
    },
    /// A projection carrying at least one aggregate. A bare column is a
    /// group column (it must appear in `GROUP BY`); a call is an
    /// aggregate. The two render side by side: `SELECT city, COUNT(*)`.
    Aggregate {
        items: [AggItem<'a>; MAX_LIST_COLS],
        count: usize,
    },
}

/// The aggregate functions this subset evaluates.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// One item in an aggregate projection: either a bare (group) column, or
/// an aggregate call over a column — or `COUNT(*)`.
#[derive(Clone, Copy)]
pub struct AggItem<'a> {
    /// `None` = a bare column (a group key). `Some` = an aggregate call.
    pub func: Option<AggFunc>,
    /// The column the item names. Unused when `star` is set.
    pub column: &'a [u8],
    /// `COUNT(*)` — a count of rows, not of a column's non-nulls.
    pub star: bool,
    /// Optional `AS` alias for the result column header.
    pub alias: Option<&'a [u8]>,
}

/// A comparison operator in a `WHERE column OP literal`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
    /// `IS NULL` — true when the column is absent or NULL. The predicate's
    /// `value` is unused.
    IsNull,
    /// `IS NOT NULL` — true when the column is present and non-NULL.
    IsNotNull,
}

/// `column OP literal` — the predicate forms this subset supports.
///
/// Equality on a leading single primary-key column is a point read; a
/// comparison on it is a bounded range scan; anything on a non-key
/// column is a scan with a filter the executor applies. A `WHERE` the
/// executor silently ignored would return the wrong rows, so an
/// unsupported shape refuses by name rather than dropping the clause.
#[derive(Clone, Copy)]
pub struct Predicate<'a> {
    pub column: &'a [u8],
    pub op: CmpOp,
    pub value: Literal<'a>,
}

/// The most conjuncts a `WHERE` may carry. A conjunction longer than this
/// refuses by name rather than silently dropping the tail.
pub const MAX_PREDICATES: usize = 4;

/// The most values an `IN (…)` list may carry.
pub const MAX_IN_VALUES: usize = 16;

/// A `WHERE` as a conjunction of simple predicates (`p AND p AND …`),
/// optionally including one `col IN (…)` list. `count == 0 && !in_active`
/// means no `WHERE`. `OR` is refused — it has no single-scan plan.
#[derive(Clone, Copy)]
pub struct Where<'a> {
    pub preds: [Predicate<'a>; MAX_PREDICATES],
    pub count: usize,
    /// One `col IN (v…)` / `col NOT IN (v…)` conjunct, kept out of `preds`
    /// because it is a disjunction over its own list, not a simple compare.
    pub in_active: bool,
    pub in_negate: bool,
    pub in_col: &'a [u8],
    pub in_vals: [Literal<'a>; MAX_IN_VALUES],
    pub in_count: usize,
}

impl<'a> Where<'a> {
    pub const EMPTY: Where<'a> = Where {
        preds: [Predicate {
            column: b"",
            op: CmpOp::Eq,
            value: Literal::Null,
        }; MAX_PREDICATES],
        count: 0,
        in_active: false,
        in_negate: false,
        in_col: b"",
        in_vals: [Literal::Null; MAX_IN_VALUES],
        in_count: 0,
    };
    pub fn as_slice(&self) -> &[Predicate<'a>] {
        &self.preds[..self.count]
    }
}

pub struct Select<'a> {
    pub table: &'a [u8],
    pub projection: Projection<'a>,
    pub where_: Where<'a>,
    /// `GROUP BY column`, if given. Required when the projection mixes a
    /// bare column with an aggregate; forbidden otherwise.
    pub group_by: Option<&'a [u8]>,
    /// `SELECT DISTINCT` — duplicate result rows are collapsed.
    pub distinct: bool,
    /// `OFFSET n` — skip the first `n` result rows (after ORDER BY).
    pub offset: Option<u32>,
    /// `ORDER BY col [ASC|DESC]`, if given. The executor accepts it only
    /// when `column` is the single-column primary key and `desc` is false,
    /// because the store returns a range in ascending key order already;
    /// any other ordering is refused rather than silently ignored.
    pub order: Option<OrderBy<'a>>,
    /// `LIMIT n`, if given.
    pub limit: Option<u32>,
}

/// `ORDER BY column [ASC|DESC]`. A single sort key — the subset does not
/// build multi-column orderings.
#[derive(Clone, Copy)]
pub struct OrderBy<'a> {
    pub column: &'a [u8],
    pub desc: bool,
}

/// `DELETE FROM t [WHERE col = literal]`. No predicate means every row —
/// the same shape as `DROP TABLE`'s scan, but the table survives.
pub struct Delete<'a> {
    pub table: &'a [u8],
    pub where_: Where<'a>,
}

/// One `column = literal` in an UPDATE's SET list.
#[derive(Clone, Copy)]
pub struct Assignment<'a> {
    pub column: &'a [u8],
    pub value: Literal<'a>,
}

/// `UPDATE t SET col = literal [, …] [WHERE col = literal]`. Bounded to
/// [`MAX_LIST_COLS`] assignments, the same inline-array bound the column
/// list uses.
pub struct Update<'a> {
    pub table: &'a [u8],
    pub sets: [Assignment<'a>; MAX_LIST_COLS],
    pub set_count: usize,
    pub where_: Where<'a>,
}

/// A statement the client sent that this subset answers without touching
/// the catalog. Real clients send several of these during connection
/// setup and will not present a prompt until they are answered.
pub enum SessionStatement<'a> {
    /// `BEGIN` / `START TRANSACTION`
    Begin,
    Commit,
    Rollback,
    /// `SET …` — accepted and ignored. Safe to ignore because the
    /// variables clients set at startup (`client_encoding`,
    /// `autocommit`, timezone display) do not change any semantics this
    /// build implements. A `SET` that WOULD change semantics would have
    /// to be refused instead, which is why this is a distinct variant
    /// rather than folded into a generic no-op.
    Set,
    /// A constant-expression select (`SELECT 1`, `SELECT version()`)
    /// with an optional alias. Answered from the connector.
    SelectConstant {
        value: Literal<'a>,
        alias: Option<&'a [u8]>,
    },
    /// `SHOW …` — the connector answers from its compatibility surface.
    Show(&'a [u8]),
}

/// Sized by its largest variant (`CreateTable`, which holds the column
/// array). Same rationale as [`Projection`]: allocation-free parsing
/// means the bound forms live inline.
#[allow(
    clippy::large_enum_variant,
    reason = "no_std, allocation-free: the inline fixed arrays are the point, and boxing is not available"
)]
pub enum Statement<'a> {
    CreateTable(CreateTable<'a>),
    /// `DROP TABLE [IF EXISTS] name`
    DropTable {
        name: &'a [u8],
        if_exists: bool,
    },
    /// `CREATE INDEX name ON table (column)` — one column, non-unique.
    /// Multi-column and expression indexes refuse BY NAME (§14.4's
    /// encoding supports composites, the surface grows deliberately).
    CreateIndex {
        name: &'a [u8],
        table: &'a [u8],
        column: &'a [u8],
    },
    /// `DROP INDEX [IF EXISTS] name`
    DropIndex {
        name: &'a [u8],
        if_exists: bool,
    },
    Insert(Insert<'a>),
    Select(Select<'a>),
    Delete(Delete<'a>),
    Update(Update<'a>),
    /// `ALTER TABLE t ADD [COLUMN] name type` — append a nullable column.
    /// Existing rows lack it and read back NULL (the row predates it).
    AlterTableAddColumn {
        table: &'a [u8],
        column: &'a [u8],
        ty: LogicalType,
    },
    Session(SessionStatement<'a>),
    /// Whitespace, comments, or a bare `;`. A real client sends these.
    Empty,
}

impl core::fmt::Debug for Statement<'_> {
    /// Variant name only. The bound forms hold fixed arrays whose full
    /// contents would swamp a test failure message; the variant is what
    /// a mis-parse gets wrong.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match self {
            Self::CreateTable(_) => "CreateTable",
            Self::DropTable { .. } => "DropTable",
            Self::CreateIndex { .. } => "CreateIndex",
            Self::DropIndex { .. } => "DropIndex",
            Self::Insert(_) => "Insert",
            Self::Select(_) => "Select",
            Self::Delete(_) => "Delete",
            Self::Update(_) => "Update",
            Self::AlterTableAddColumn { .. } => "AlterTableAddColumn",
            Self::Session(_) => "Session",
            Self::Empty => "Empty",
        };
        f.write_str(name)
    }
}

// ── Type names ────────────────────────────────────────────────────────

/// A type name resolved as far as it can be before its modifier is
/// read.
///
/// Sized types are deliberately NOT collapsed to a default width here.
/// `VARCHAR(10)` and `VARCHAR(200)` are different types in the logical
/// system, and the descriptor carries the length — so discarding the
/// modifier would store a column whose declared width disagrees with
/// what the client asked for, and the disagreement would only surface
/// when a value was rejected or accepted against the wrong bound.
enum TypeCtor {
    /// A type with no modifier.
    Fixed(LogicalType),
    /// A type whose modifier is a byte/character length.
    Sized(fn(u16) -> LogicalType),
    /// `DECIMAL(p, s)` — two modifiers, and both are meaningful.
    Decimal,
}

/// Default length for a sized type written without a modifier. `TEXT`
/// and bare `VARCHAR` have no width in either dialect, so one has to be
/// chosen; the logical maximum is the only choice that cannot reject a
/// value the client's declaration allowed.
const DEFAULT_SIZED_LEN: u16 = MAX_TEXT_LEN as u16;

/// Map a SQL type name to a constructor, accepting both dialects'
/// spellings for the same logical type. Shared rather than
/// per-connector: if each connector mapped its own names, `INT` could
/// mean different widths on the two ports of one database.
fn type_ctor(name: &[u8]) -> Option<TypeCtor> {
    use LogicalType as L;
    use TypeCtor::{Decimal, Fixed, Sized};
    if eq_ident(name, b"boolean") || eq_ident(name, b"bool") {
        return Some(Fixed(L::Boolean));
    }
    if eq_ident(name, b"smallint") || eq_ident(name, b"int2") {
        return Some(Fixed(L::SmallInt));
    }
    if eq_ident(name, b"int") || eq_ident(name, b"integer") || eq_ident(name, b"int4") {
        return Some(Fixed(L::Int));
    }
    if eq_ident(name, b"bigint") || eq_ident(name, b"int8") {
        return Some(Fixed(L::BigInt));
    }
    if eq_ident(name, b"real") || eq_ident(name, b"float4") {
        return Some(Fixed(L::Float));
    }
    if eq_ident(name, b"double") || eq_ident(name, b"float8") || eq_ident(name, b"float") {
        return Some(Fixed(L::Double));
    }
    if eq_ident(name, b"decimal") || eq_ident(name, b"numeric") {
        return Some(Decimal);
    }
    if eq_ident(name, b"char") || eq_ident(name, b"character") {
        return Some(Sized(|length| L::Char { length }));
    }
    if eq_ident(name, b"varchar") || eq_ident(name, b"text") {
        return Some(Sized(|length| L::VarChar { length }));
    }
    if eq_ident(name, b"binary") {
        return Some(Sized(|length| L::Binary { length }));
    }
    if eq_ident(name, b"varbinary") || eq_ident(name, b"blob") || eq_ident(name, b"bytea") {
        return Some(Sized(|length| L::VarBinary { length }));
    }
    if eq_ident(name, b"date") {
        return Some(Fixed(L::Date));
    }
    if eq_ident(name, b"time") {
        return Some(Fixed(L::Time));
    }
    if eq_ident(name, b"timestamp") || eq_ident(name, b"datetime") {
        return Some(Fixed(L::Timestamp));
    }
    if eq_ident(name, b"timestamptz") {
        return Some(Fixed(L::TimestampTz));
    }
    // `SERIAL` is a Postgres shorthand for "integer with a sequence
    // default". The sequence half is not implemented, so accepting it as
    // a plain integer would leave a column the client expects to
    // auto-populate silently requiring a value on every insert. Refuse
    // by name.
    None
}

/// Parse a type name plus its optional `(n)` / `(p, s)` modifier.
fn parse_type(p: &mut Parser<'_>) -> Result<LogicalType, SqlError> {
    let name = p.ident()?;
    // Postgres spells these as two words.
    let name: &[u8] = if eq_ident(name, b"double") && p.eat_kw(b"precision")? {
        b"double"
    } else if eq_ident(name, b"character") && p.eat_kw(b"varying")? {
        b"varchar"
    } else {
        name
    };
    let ctor = type_ctor(name).ok_or(SqlError::UnknownType)?;

    let mut mods = [0i64; 2];
    let mut nmods = 0usize;
    if p.eat_punct(b'(')? {
        loop {
            match p.need()? {
                Tok::Int(v) if v >= 0 => {
                    if nmods >= mods.len() {
                        return Err(SqlError::TooManyItems);
                    }
                    mods[nmods] = v;
                    nmods += 1;
                }
                _ => return Err(SqlError::Syntax),
            }
            if !p.eat_punct(b',')? {
                break;
            }
        }
        p.expect_punct(b')')?;
    }

    let ty = match ctor {
        TypeCtor::Fixed(t) => {
            if nmods != 0 {
                // A modifier on a type that has none would be silently
                // dropped, and the client would believe it applied.
                return Err(SqlError::Unsupported);
            }
            t
        }
        TypeCtor::Sized(make) => {
            let length = match nmods {
                0 => DEFAULT_SIZED_LEN,
                1 => u16::try_from(mods[0]).map_err(|_| SqlError::UnknownType)?,
                _ => return Err(SqlError::Syntax),
            };
            make(length)
        }
        TypeCtor::Decimal => {
            let (precision, scale) = match nmods {
                0 => (relational::MAX_DECIMAL_PRECISION, 0),
                1 => (u8::try_from(mods[0]).map_err(|_| SqlError::UnknownType)?, 0),
                _ => (
                    u8::try_from(mods[0]).map_err(|_| SqlError::UnknownType)?,
                    u8::try_from(mods[1]).map_err(|_| SqlError::UnknownType)?,
                ),
            };
            LogicalType::Decimal { precision, scale }
        }
    };
    if !ty.is_valid() {
        // The logical system's own bound (precision range, length cap)
        // rejected it. Better here than at the first insert.
        return Err(SqlError::UnknownType);
    }
    Ok(ty)
}

// ── Parse ─────────────────────────────────────────────────────────────

/// Parse one statement. A trailing `;` is accepted; anything after it
/// must be blank, because a multi-statement string executed as one would
/// silently run something the caller did not see.
pub fn parse(sql: &[u8], dialect: Dialect) -> Result<Statement<'_>, SqlError> {
    if sql.len() > MAX_SQL_LEN {
        return Err(SqlError::TooLong);
    }
    let mut p = Parser::new(sql, dialect);
    let Some(first) = p.peek()? else {
        return Ok(Statement::Empty);
    };
    if first == Tok::Punct(b';') {
        p.bump()?;
        return finish(&mut p, Statement::Empty);
    }

    if p.eat_kw(b"create")? {
        return parse_create(&mut p);
    }
    if p.eat_kw(b"drop")? {
        return parse_drop(&mut p);
    }
    if p.eat_kw(b"insert")? {
        return parse_insert(&mut p);
    }
    if p.eat_kw(b"select")? {
        return parse_select(&mut p);
    }
    if p.eat_kw(b"delete")? {
        return parse_delete(&mut p);
    }
    if p.eat_kw(b"update")? {
        return parse_update(&mut p);
    }
    if p.eat_kw(b"alter")? {
        return parse_alter(&mut p);
    }
    if p.eat_kw(b"begin")? {
        // Postgres `BEGIN [TRANSACTION]`.
        let _ = p.eat_kw(b"transaction")?;
        let _ = p.eat_kw(b"work")?;
        return finish(&mut p, Statement::Session(SessionStatement::Begin));
    }
    if p.eat_kw(b"start")? {
        p.expect_kw(b"transaction")?;
        return finish(&mut p, Statement::Session(SessionStatement::Begin));
    }
    if p.eat_kw(b"commit")? {
        let _ = p.eat_kw(b"transaction")?;
        let _ = p.eat_kw(b"work")?;
        return finish(&mut p, Statement::Session(SessionStatement::Commit));
    }
    if p.eat_kw(b"rollback")? {
        let _ = p.eat_kw(b"transaction")?;
        let _ = p.eat_kw(b"work")?;
        return finish(&mut p, Statement::Session(SessionStatement::Rollback));
    }
    if p.eat_kw(b"set")? {
        // Consume the rest without interpreting it — see the
        // `SessionStatement::Set` doc for why ignoring is safe HERE and
        // would not be in general.
        while p.bump()?.is_some() {}
        return Ok(Statement::Session(SessionStatement::Set));
    }
    if p.eat_kw(b"show")? {
        let what = match p.peek()? {
            Some(Tok::Word(w)) | Some(Tok::Ident(w)) => {
                p.bump()?;
                w
            }
            _ => b"",
        };
        while p.bump()?.is_some() {}
        return Ok(Statement::Session(SessionStatement::Show(what)));
    }
    Err(SqlError::Unsupported)
}

/// Consume an optional trailing `;` and require the input to end.
fn finish<'a>(p: &mut Parser<'a>, stmt: Statement<'a>) -> Result<Statement<'a>, SqlError> {
    let _ = p.eat_punct(b';')?;
    match p.peek()? {
        None => Ok(stmt),
        // Trailing `;` after `;`, or a stray one — a real client sends
        // `SELECT 1;` and psql's own probes sometimes end `;;`.
        Some(Tok::Punct(b';')) => {
            while p.eat_punct(b';')? {}
            if p.peek()?.is_none() {
                Ok(stmt)
            } else {
                Err(SqlError::Unsupported)
            }
        }
        // Anything else after a complete statement is a second
        // statement. Executing it silently is the failure mode to avoid.
        Some(_) => Err(SqlError::Unsupported),
    }
}

fn parse_create<'a>(p: &mut Parser<'a>) -> Result<Statement<'a>, SqlError> {
    if p.eat_kw(b"unique")? {
        // UNIQUE INDEX needs the §14.4 ownership record wired through
        // the write path; refuse by name until it is.
        return Err(SqlError::Unsupported);
    }
    if p.eat_kw(b"index")? {
        let name = p.table_name()?;
        p.expect_kw(b"on")?;
        let table = p.table_name()?;
        p.expect_punct(b'(')?;
        let column = p.ident()?;
        if p.eat_punct(b',')? {
            // Composite indexes: the encoding supports them, the
            // surface doesn't yet. A named refusal, not a truncation
            // to the first column.
            return Err(SqlError::Unsupported);
        }
        p.expect_punct(b')')?;
        return finish(
            p,
            Statement::CreateIndex {
                name,
                table,
                column,
            },
        );
    }
    if !p.eat_kw(b"table")? {
        return Err(SqlError::Unsupported); // CREATE VIEW/…
    }
    let if_not_exists = if p.eat_kw(b"if")? {
        p.expect_kw(b"not")?;
        p.expect_kw(b"exists")?;
        true
    } else {
        false
    };
    let name = p.table_name()?;
    p.expect_punct(b'(')?;

    let mut ct = CreateTable {
        name,
        if_not_exists,
        cols: [ColumnSpec::EMPTY; MAX_COLUMNS],
        col_count: 0,
        pk: [0; MAX_KEY_COLUMNS],
        pk_count: 0,
    };

    loop {
        // A table-level `PRIMARY KEY (a, b)` constraint.
        if p.eat_kw(b"primary")? {
            p.expect_kw(b"key")?;
            p.expect_punct(b'(')?;
            loop {
                let cname = p.ident()?;
                let pos = ct
                    .columns()
                    .iter()
                    .position(|c| eq_ident(c.name, cname))
                    .ok_or(SqlError::UnknownColumn)?;
                if ct.pk_count >= MAX_KEY_COLUMNS {
                    return Err(SqlError::TooManyItems);
                }
                if ct.pk[..ct.pk_count].contains(&(pos as u16)) {
                    return Err(SqlError::DuplicateColumn);
                }
                ct.pk[ct.pk_count] = pos as u16;
                ct.pk_count += 1;
                if !p.eat_punct(b',')? {
                    break;
                }
            }
            p.expect_punct(b')')?;
        } else {
            let cname = p.ident()?;
            if ct.columns().iter().any(|c| eq_ident(c.name, cname)) {
                return Err(SqlError::DuplicateColumn);
            }
            let ty = parse_type(p)?;

            // Column constraints. A key column is implicitly NOT NULL:
            // a NULL in a key would put two different rows at one key
            // position with no honest answer for which a lookup means,
            // and the descriptor rejects it outright.
            let mut nullable = true;
            let mut is_pk = false;
            loop {
                if p.eat_kw(b"not")? {
                    p.expect_kw(b"null")?;
                    nullable = false;
                } else if p.eat_kw(b"null")? {
                    nullable = true;
                } else if p.eat_kw(b"primary")? {
                    p.expect_kw(b"key")?;
                    is_pk = true;
                    nullable = false;
                } else if p.eat_kw(b"unique")?
                    || p.eat_kw(b"default")?
                    || p.eat_kw(b"references")?
                    || p.eat_kw(b"auto_increment")?
                {
                    // Constraints and defaults this build does not
                    // enforce. Accepting them silently is worse than
                    // refusing: a client would RELY on a UNIQUE that did
                    // nothing, or omit a column expecting a DEFAULT that
                    // was never applied.
                    return Err(SqlError::Unsupported);
                } else {
                    break;
                }
            }

            if ct.col_count >= MAX_COLUMNS {
                return Err(SqlError::TooManyItems);
            }
            ct.cols[ct.col_count] = ColumnSpec {
                name: cname,
                ty,
                nullable,
                primary_key: is_pk,
            };
            if is_pk {
                if ct.pk_count >= MAX_KEY_COLUMNS {
                    return Err(SqlError::TooManyItems);
                }
                ct.pk[ct.pk_count] = ct.col_count as u16;
                ct.pk_count += 1;
            }
            ct.col_count += 1;
        }
        if !p.eat_punct(b',')? {
            break;
        }
    }
    p.expect_punct(b')')?;

    // MySQL table options (`ENGINE=InnoDB`, `DEFAULT CHARSET=…`) trail
    // the column list. They select storage and collation behaviour this
    // build does not implement, so they are consumed rather than obeyed
    // — a client that always emits `ENGINE=InnoDB` must not be refused
    // over it, and there is no InnoDB-specific SEMANTIC to honour.
    while let Some(t) = p.peek()? {
        if t == Tok::Punct(b';') {
            break;
        }
        p.bump()?;
    }

    if ct.pk_count == 0 {
        return Err(SqlError::MissingPrimaryKey);
    }
    if ct.col_count == 0 {
        return Err(SqlError::Syntax);
    }
    // A table-level constraint may have named a column declared after
    // it, so nullability of key columns is settled here rather than
    // inline.
    for i in 0..ct.pk_count {
        ct.cols[ct.pk[i] as usize].nullable = false;
    }
    finish(p, Statement::CreateTable(ct))
}

fn parse_drop<'a>(p: &mut Parser<'a>) -> Result<Statement<'a>, SqlError> {
    if p.eat_kw(b"index")? {
        let if_exists = if p.eat_kw(b"if")? {
            p.expect_kw(b"exists")?;
            true
        } else {
            false
        };
        let name = p.table_name()?;
        return finish(p, Statement::DropIndex { name, if_exists });
    }
    if !p.eat_kw(b"table")? {
        return Err(SqlError::Unsupported);
    }
    let if_exists = if p.eat_kw(b"if")? {
        p.expect_kw(b"exists")?;
        true
    } else {
        false
    };
    let name = p.table_name()?;
    finish(p, Statement::DropTable { name, if_exists })
}

fn literal<'a>(p: &mut Parser<'a>) -> Result<Literal<'a>, SqlError> {
    match p.need()? {
        Tok::Int(v) => Ok(Literal::Int(v)),
        Tok::Float(v) => Ok(Literal::Float(v)),
        Tok::Str(s) => Ok(Literal::Str(s)),
        Tok::Word(w) if eq_ident(w, b"null") => Ok(Literal::Null),
        Tok::Word(w) if eq_ident(w, b"true") => Ok(Literal::Bool(true)),
        Tok::Word(w) if eq_ident(w, b"false") => Ok(Literal::Bool(false)),
        // A driver's prepared statement. Named so the client is told
        // "parameters unsupported" rather than "syntax error".
        Tok::Param => Err(SqlError::Unsupported),
        _ => Err(SqlError::Syntax),
    }
}

fn parse_insert<'a>(p: &mut Parser<'a>) -> Result<Statement<'a>, SqlError> {
    p.expect_kw(b"into")?;
    let table = p.table_name()?;
    let mut ins = Insert {
        table,
        cols: [b""; MAX_LIST_COLS],
        col_count: 0,
        rows: [[Literal::Null; MAX_LIST_COLS]; MAX_INSERT_ROWS],
        row_widths: [0; MAX_INSERT_ROWS],
        row_count: 0,
    };

    if p.eat_punct(b'(')? {
        loop {
            let c = p.ident()?;
            if ins.col_count >= MAX_LIST_COLS {
                return Err(SqlError::TooManyItems);
            }
            if ins.columns().iter().any(|e| eq_ident(e, c)) {
                return Err(SqlError::DuplicateColumn);
            }
            ins.cols[ins.col_count] = c;
            ins.col_count += 1;
            if !p.eat_punct(b',')? {
                break;
            }
        }
        p.expect_punct(b')')?;
    }

    if !(p.eat_kw(b"values")? || p.eat_kw(b"value")?) {
        // `INSERT … SELECT` is a different execution shape entirely.
        return Err(SqlError::Unsupported);
    }

    loop {
        p.expect_punct(b'(')?;
        if ins.row_count >= MAX_INSERT_ROWS {
            return Err(SqlError::TooManyItems);
        }
        let mut w = 0usize;
        loop {
            if w >= MAX_LIST_COLS {
                return Err(SqlError::TooManyItems);
            }
            ins.rows[ins.row_count][w] = literal(p)?;
            w += 1;
            if !p.eat_punct(b',')? {
                break;
            }
        }
        p.expect_punct(b')')?;
        ins.row_widths[ins.row_count] = w;
        ins.row_count += 1;
        if !p.eat_punct(b',')? {
            break;
        }
    }
    finish(p, Statement::Insert(ins))
}

/// Parse an optional `WHERE column = literal` — the one predicate form
/// the executor resolves, shared by SELECT, DELETE, and UPDATE. A second
/// conjunct (`AND`/`OR`) is refused rather than silently applying only
/// the first, which would touch rows the client's WHERE excluded.
fn parse_where<'a>(p: &mut Parser<'a>) -> Result<Where<'a>, SqlError> {
    let mut w = Where::EMPTY;
    if !p.eat_kw(b"where")? {
        return Ok(w);
    }
    loop {
        let column = p.ident()?;
        // `col [NOT] IN (v, …)` — a disjunction over its own list.
        let mut negate_in = false;
        let is_in = if p.eat_kw(b"in")? {
            true
        } else if p.eat_kw(b"not")? {
            // Only NOT IN follows a bare NOT in this subset.
            if !p.eat_kw(b"in")? {
                return Err(SqlError::Unsupported);
            }
            negate_in = true;
            true
        } else {
            false
        };
        if is_in {
            if w.in_active {
                // One IN list per query keeps the filter bounded.
                return Err(SqlError::Unsupported);
            }
            p.expect_punct(b'(')?;
            let mut n = 0usize;
            loop {
                if n >= MAX_IN_VALUES {
                    return Err(SqlError::TooManyItems);
                }
                w.in_vals[n] = literal(p)?;
                n += 1;
                if p.eat_punct(b')')? {
                    break;
                }
                p.expect_punct(b',')?;
            }
            w.in_active = true;
            w.in_negate = negate_in;
            w.in_col = column;
            w.in_count = n;
        } else if p.eat_kw(b"between")? {
            // `col BETWEEN lo AND hi` is the conjunction `col >= lo AND
            // col <= hi` — two ordinary predicates the plan already carries.
            let lo = literal(p)?;
            p.expect_kw(b"and")?;
            let hi = literal(p)?;
            push_pred(
                &mut w,
                Predicate {
                    column,
                    op: CmpOp::Ge,
                    value: lo,
                },
            )?;
            push_pred(
                &mut w,
                Predicate {
                    column,
                    op: CmpOp::Le,
                    value: hi,
                },
            )?;
        } else if p.eat_kw(b"is")? {
            // `col IS [NOT] NULL`.
            let negate = p.eat_kw(b"not")?;
            if !p.eat_kw(b"null")? {
                return Err(SqlError::Syntax);
            }
            let op = if negate {
                CmpOp::IsNotNull
            } else {
                CmpOp::IsNull
            };
            push_pred(
                &mut w,
                Predicate {
                    column,
                    op,
                    value: Literal::Null,
                },
            )?;
        } else {
            let op = parse_cmp_op(p)?;
            let value = literal(p)?;
            push_pred(&mut w, Predicate { column, op, value })?;
        }
        if p.eat_kw(b"or")? {
            // OR needs a union of scans this subset does not build.
            return Err(SqlError::Unsupported);
        }
        if !p.eat_kw(b"and")? {
            break;
        }
    }
    Ok(w)
}

/// Append one predicate to a conjunction, refusing (never dropping) when
/// it would exceed the plan's fixed capacity.
fn push_pred<'a>(w: &mut Where<'a>, pred: Predicate<'a>) -> Result<(), SqlError> {
    if w.count >= MAX_PREDICATES {
        return Err(SqlError::TooManyItems);
    }
    w.preds[w.count] = pred;
    w.count += 1;
    Ok(())
}

/// Parse `= | < | <= | > | >=`. `<>`/`!=` refuse by name — a
/// not-equals has no bounded scan and the filter path does not carry it.
fn parse_cmp_op(p: &mut Parser<'_>) -> Result<CmpOp, SqlError> {
    if p.eat_punct(b'=')? {
        return Ok(CmpOp::Eq);
    }
    if p.eat_punct(b'<')? {
        if p.eat_punct(b'=')? {
            return Ok(CmpOp::Le);
        }
        if p.eat_punct(b'>')? {
            return Err(SqlError::Unsupported); // `<>`
        }
        return Ok(CmpOp::Lt);
    }
    if p.eat_punct(b'>')? {
        if p.eat_punct(b'=')? {
            return Ok(CmpOp::Ge);
        }
        return Ok(CmpOp::Gt);
    }
    Err(SqlError::Syntax)
}

/// `DELETE FROM t [WHERE col = literal]`.
fn parse_delete<'a>(p: &mut Parser<'a>) -> Result<Statement<'a>, SqlError> {
    p.expect_kw(b"from")?;
    let table = p.table_name()?;
    let predicate = parse_where(p)?;
    finish(
        p,
        Statement::Delete(Delete {
            table,
            where_: predicate,
        }),
    )
}

/// `ALTER TABLE t ADD [COLUMN] name type`. Only ADD COLUMN — DROP/ALTER
/// COLUMN would rewrite or reinterpret stored rows and refuse by name.
fn parse_alter<'a>(p: &mut Parser<'a>) -> Result<Statement<'a>, SqlError> {
    p.expect_kw(b"table")?;
    let table = p.table_name()?;
    p.expect_kw(b"add")?;
    let _ = p.eat_kw(b"column")?; // optional COLUMN keyword
    let column = p.ident()?;
    let ty = parse_type(p)?;
    finish(p, Statement::AlterTableAddColumn { table, column, ty })
}

/// `UPDATE t SET col = literal [, …] [WHERE col = literal]`.
fn parse_update<'a>(p: &mut Parser<'a>) -> Result<Statement<'a>, SqlError> {
    let table = p.table_name()?;
    p.expect_kw(b"set")?;
    let mut sets = [Assignment {
        column: b"",
        value: Literal::Null,
    }; MAX_LIST_COLS];
    let mut set_count = 0usize;
    loop {
        if set_count >= MAX_LIST_COLS {
            return Err(SqlError::TooManyItems);
        }
        let column = p.ident()?;
        p.expect_punct(b'=')?;
        let value = literal(p)?;
        sets[set_count] = Assignment { column, value };
        set_count += 1;
        if !p.eat_punct(b',')? {
            break;
        }
    }
    let predicate = parse_where(p)?;
    finish(
        p,
        Statement::Update(Update {
            table,
            sets,
            set_count,
            where_: predicate,
        }),
    )
}

fn parse_select<'a>(p: &mut Parser<'a>) -> Result<Statement<'a>, SqlError> {
    // A constant select has no FROM. Recognise the shapes clients
    // actually send during startup before committing to a table query.
    if let Some(Tok::Int(_) | Tok::Float(_) | Tok::Str(_)) = p.peek()? {
        let value = literal(p)?;
        let alias = select_alias(p)?;
        if p.eat_kw(b"from")? {
            // `SELECT 1 FROM t` is a real query over stored rows, not a
            // constant, and answering it with the constant would
            // fabricate a result.
            return Err(SqlError::Unsupported);
        }
        return finish(
            p,
            Statement::Session(SessionStatement::SelectConstant { value, alias }),
        );
    }

    // `SELECT DISTINCT` collapses duplicate result rows.
    let distinct = p.eat_kw(b"distinct")?;

    // Parse the select list into aggregate items. A bare column, an
    // aggregate call, or (with no FROM) connection-setup chatter — the
    // last of which becomes a session constant rather than a table query.
    let is_star = p.eat_punct(b'*')?;
    let mut items = [AggItem {
        func: None,
        column: b"",
        star: false,
        alias: None,
    }; MAX_LIST_COLS];
    let mut names = [b"" as &[u8]; MAX_LIST_COLS];
    let mut count = 0usize;
    let mut has_agg = false;
    let mut non_agg_call_alias: Option<&[u8]> = None;
    if !is_star {
        loop {
            let c = p.ident()?;
            if p.peek()? == Some(Tok::Punct(b'(')) {
                if let Some(func) = agg_func(c) {
                    p.expect_punct(b'(')?;
                    let (column, star): (&[u8], bool) = if p.eat_punct(b'*')? {
                        (b"", true)
                    } else {
                        (p.ident()?, false)
                    };
                    p.expect_punct(b')')?;
                    // `*` counts rows; only COUNT takes it.
                    if star && func != AggFunc::Count {
                        return Err(SqlError::Unsupported);
                    }
                    let alias = select_alias(p)?;
                    if count >= MAX_LIST_COLS {
                        return Err(SqlError::TooManyItems);
                    }
                    items[count] = AggItem {
                        func: Some(func),
                        column,
                        star,
                        alias,
                    };
                    count += 1;
                    has_agg = true;
                } else {
                    // A non-aggregate function (`version()`, …). With no
                    // FROM it is chatter answered from the compatibility
                    // surface; with a FROM it is a row-wise function this
                    // subset cannot evaluate, refused below.
                    skip_balanced_parens(p)?;
                    let _ = select_alias(p)?;
                    non_agg_call_alias = Some(c);
                }
            } else {
                let alias = select_alias(p)?;
                if count >= MAX_LIST_COLS {
                    return Err(SqlError::TooManyItems);
                }
                names[count] = c;
                items[count] = AggItem {
                    func: None,
                    column: c,
                    star: false,
                    alias,
                };
                count += 1;
            }
            if !p.eat_punct(b',')? {
                break;
            }
        }
        if let Some(alias) = non_agg_call_alias {
            if p.peek()?.is_some_and(|t| kw(t, b"from")) {
                return Err(SqlError::Unsupported);
            }
            while p.bump()?.is_some() {}
            return Ok(Statement::Session(SessionStatement::SelectConstant {
                value: Literal::Null,
                alias: Some(alias),
            }));
        }
    }

    if !p.eat_kw(b"from")? {
        // A lone aggregate call with no FROM is connector chatter (a real
        // `psql` sends `SELECT count(*)` during setup), answered with a
        // constant. A bare column with no FROM cannot be resolved.
        if has_agg {
            return Ok(Statement::Session(SessionStatement::SelectConstant {
                value: Literal::Null,
                alias: items[0].alias,
            }));
        }
        return Err(SqlError::Unsupported);
    }
    let table = p.table_name()?;

    let predicate = parse_where(p)?;

    // `GROUP BY column`. A grouped query is an aggregate query even if it
    // names no aggregate call (`SELECT city FROM t GROUP BY city` is the
    // distinct cities).
    let group_by = if p.eat_kw(b"group")? {
        if !p.eat_kw(b"by")? {
            return Err(SqlError::Syntax);
        }
        Some(p.ident()?)
    } else {
        None
    };

    // A bare column beside an aggregate needs a GROUP BY, and every bare
    // column must be the group column — anything else is a query whose
    // answer this subset cannot form.
    let grouped = has_agg || group_by.is_some();
    if grouped {
        for item in &items[..count] {
            if item.func.is_none() {
                match group_by {
                    Some(g) if item.column == g => {}
                    _ => return Err(SqlError::Unsupported),
                }
            }
        }
    }
    let projection = if is_star {
        Projection::Star
    } else if grouped {
        Projection::Aggregate { items, count }
    } else {
        Projection::Columns { names, count }
    };

    // `ORDER BY col [ASC|DESC]`. Parsed into structure so the executor can
    // decide whether it can honour it (ascending on the primary key, which
    // the ordered store already delivers) or must refuse it by name. A
    // dropped clause would return rows in an order the client was promised
    // and did not get.
    let order = if p.eat_kw(b"order")? {
        if !p.eat_kw(b"by")? {
            return Err(SqlError::Syntax);
        }
        let column = p.ident()?;
        let desc = if p.eat_kw(b"asc")? {
            false
        } else {
            // No `asc`: `desc` sets descending, anything else defaults to
            // ascending — `eat_kw` yields exactly that bool.
            p.eat_kw(b"desc")?
        };
        Some(OrderBy { column, desc })
    } else {
        None
    };

    // `LIMIT n [OFFSET m]` or `OFFSET m` on its own — either order of the
    // two clauses is accepted, as real clients send both.
    let mut limit = None;
    let mut offset = None;
    loop {
        if limit.is_none() && p.eat_kw(b"limit")? {
            limit = Some(match p.need()? {
                Tok::Int(v) if (0..=u32::MAX as i64).contains(&v) => v as u32,
                _ => return Err(SqlError::Syntax),
            });
        } else if offset.is_none() && p.eat_kw(b"offset")? {
            offset = Some(match p.need()? {
                Tok::Int(v) if (0..=u32::MAX as i64).contains(&v) => v as u32,
                _ => return Err(SqlError::Syntax),
            });
        } else {
            break;
        }
    }

    finish(
        p,
        Statement::Select(Select {
            table,
            projection,
            where_: predicate,
            group_by,
            distinct,
            order,
            limit,
            offset,
        }),
    )
}

/// Map an identifier to an aggregate function, case-insensitively.
fn agg_func(name: &[u8]) -> Option<AggFunc> {
    if eq_ident(name, b"count") {
        Some(AggFunc::Count)
    } else if eq_ident(name, b"sum") {
        Some(AggFunc::Sum)
    } else if eq_ident(name, b"min") {
        Some(AggFunc::Min)
    } else if eq_ident(name, b"max") {
        Some(AggFunc::Max)
    } else if eq_ident(name, b"avg") {
        Some(AggFunc::Avg)
    } else {
        None
    }
}

/// Consume a balanced `( … )` group, so an expression this subset
/// cannot evaluate can still be stepped over without the nested commas
/// and parens being mistaken for list structure.
fn skip_balanced_parens(p: &mut Parser<'_>) -> Result<(), SqlError> {
    p.expect_punct(b'(')?;
    let mut depth = 1usize;
    while depth > 0 {
        match p.need()? {
            Tok::Punct(b'(') => depth += 1,
            Tok::Punct(b')') => depth -= 1,
            _ => {}
        }
    }
    Ok(())
}

/// `[AS] alias` after a projected expression.
fn select_alias<'a>(p: &mut Parser<'a>) -> Result<Option<&'a [u8]>, SqlError> {
    if p.eat_kw(b"as")? {
        return Ok(Some(p.ident()?));
    }
    match p.peek()? {
        Some(Tok::Ident(s)) => {
            p.bump()?;
            Ok(Some(s))
        }
        Some(Tok::Word(w))
            if !eq_ident(w, b"from")
                && !eq_ident(w, b"where")
                && !eq_ident(w, b"limit")
                && !eq_ident(w, b"order") =>
        {
            p.bump()?;
            Ok(Some(w))
        }
        _ => Ok(None),
    }
}

// ── Bind ──────────────────────────────────────────────────────────────

/// Build a [`TableDescriptor`] from a parsed `CREATE TABLE`.
///
/// `table_id` is supplied by the caller because allocating it is a
/// catalog mutation, and the parser has no catalog. Column ids are
/// assigned by DECLARATION ORDER starting at 1, which is what makes
/// `INSERT` without a column list mean what a user expects.
pub fn bind_create_table(
    ct: &CreateTable<'_>,
    table_id: u32,
    database_id: u32,
    catalog_revision: u64,
) -> Result<TableDescriptor, SqlError> {
    let mut td = TableDescriptor::new(table_id, database_id, catalog_revision, ct.name)
        .ok_or(SqlError::NameTooLong)?;
    for (i, c) in ct.columns().iter().enumerate() {
        let col = ColumnDescriptor::new((i + 1) as u16, c.ty, c.nullable, false, c.name)
            .ok_or(SqlError::UnknownType)?;
        td.add_column(col).map_err(|_| SqlError::TooManyItems)?;
    }
    let mut pk = [0u16; MAX_KEY_COLUMNS];
    for (i, pos) in ct.primary_key_positions().iter().enumerate() {
        pk[i] = *pos + 1; // position → column id
    }
    td.set_primary_key(&pk[..ct.primary_key_positions().len()])
        .map_err(|_| SqlError::MissingPrimaryKey)?;
    td.check_invariants().map_err(|_| SqlError::Syntax)?;
    Ok(td)
}

/// Resolve a column name to its descriptor.
pub fn resolve_column<'t>(
    td: &'t TableDescriptor,
    name: &[u8],
) -> Result<&'t ColumnDescriptor, SqlError> {
    td.columns()
        .iter()
        .find(|c| eq_ident(c.name.as_bytes(), name))
        .ok_or(SqlError::UnknownColumn)
}

/// Convert a literal to a [`Value`] of the column's DECLARED type.
///
/// The direction matters: the type comes from the schema, not from the
/// literal's syntax. `1` becomes a SMALLINT, INT, BIGINT, FLOAT or
/// DOUBLE depending on the column, and is REFUSED for a text column
/// rather than stringified — an integer written into a text column would
/// sort and compare as text, so accepting it would produce a row the
/// client cannot find again.
///
/// Widening within a family is allowed (an integer literal into a
/// floating column); narrowing that would lose information is not (a
/// literal outside `i16` range into a SMALLINT is a refusal, not a
/// wrap).
pub fn bind_value<'a>(
    lit: Literal<'a>,
    ty: LogicalType,
    nullable: bool,
) -> Result<relational::Value<'a>, SqlError> {
    use relational::Value as V;
    match lit {
        Literal::Null => {
            if nullable {
                Ok(V::Null)
            } else {
                Err(SqlError::NotNull)
            }
        }
        Literal::Bool(b) => match ty {
            LogicalType::Boolean => Ok(V::Boolean(b)),
            _ => Err(SqlError::TypeMismatch),
        },
        Literal::Int(v) => match ty {
            LogicalType::SmallInt => i16::try_from(v)
                .map(V::SmallInt)
                .map_err(|_| SqlError::TypeMismatch),
            LogicalType::Int | LogicalType::Date => i32::try_from(v)
                .map(V::Int)
                .map_err(|_| SqlError::TypeMismatch),
            LogicalType::BigInt
            | LogicalType::Time
            | LogicalType::Timestamp
            | LogicalType::TimestampTz => Ok(V::BigInt(v)),
            LogicalType::Float => Ok(V::Float(v as f32)),
            LogicalType::Double => Ok(V::Double(v as f64)),
            // A boolean column given 0/1 is the one integer→other
            // coercion both dialects agree on, and MySQL clients rely
            // on it.
            LogicalType::Boolean if v == 0 || v == 1 => Ok(V::Boolean(v == 1)),
            _ => Err(SqlError::TypeMismatch),
        },
        Literal::Float(v) => match ty {
            LogicalType::Float => Ok(V::Float(v as f32)),
            LogicalType::Double => Ok(V::Double(v)),
            _ => Err(SqlError::TypeMismatch),
        },
        // The declared LENGTH is enforced here, which is the payoff for
        // carrying it through `parse_type` instead of defaulting it. A
        // value longer than the column allows is refused rather than
        // truncated: a truncated string is a different value, and the
        // client would have no way to know it was stored short.
        Literal::Str(s) => match ty {
            LogicalType::Char { length } | LogicalType::VarChar { length } => {
                if s.len() > length as usize {
                    return Err(SqlError::LiteralTooLong);
                }
                Ok(V::Text(s))
            }
            LogicalType::Binary { length } | LogicalType::VarBinary { length } => {
                if s.len() > length as usize {
                    return Err(SqlError::LiteralTooLong);
                }
                Ok(V::Bytes(s))
            }
            _ => Err(SqlError::TypeMismatch),
        },
    }
}

/// One bound insert column: the id to store under, its type, and the
/// typed value.
#[derive(Clone, Copy)]
pub struct BoundColumn<'a> {
    pub column_id: u16,
    pub ty: LogicalType,
    pub value: relational::Value<'a>,
}

/// Bind one `VALUES` tuple against a table.
///
/// Produces columns in ASCENDING column-id order, which `encode_row`
/// requires — and it requires it so that a duplicate is structurally
/// impossible rather than merely checked.
///
/// Every column of the table is accounted for. A column the statement
/// omitted is bound to NULL if it is nullable and REFUSED if it is not:
/// there are no defaults in this subset, so silently omitting a
/// NOT NULL column would write a row the schema forbids.
pub fn bind_insert_row<'a>(
    td: &TableDescriptor,
    ins: &Insert<'a>,
    row_index: usize,
    out: &mut [BoundColumn<'a>],
) -> Result<usize, SqlError> {
    let row = ins.row(row_index).ok_or(SqlError::ArityMismatch)?;
    let named = ins.columns();
    let cols = td.columns();
    if out.len() < cols.len() {
        return Err(SqlError::TooManyItems);
    }

    if named.is_empty() {
        // Positional: the tuple must cover every column, in declaration
        // order. A short tuple is an arity error, not a partial row —
        // the client would not know which columns it had filled.
        if row.len() != cols.len() {
            return Err(SqlError::ArityMismatch);
        }
        for (i, c) in cols.iter().enumerate() {
            out[i] = BoundColumn {
                column_id: c.column_id,
                ty: c.ty,
                value: bind_value(row[i], c.ty, c.nullable)?,
            };
        }
        return Ok(cols.len());
    }

    if row.len() != named.len() {
        return Err(SqlError::ArityMismatch);
    }
    // Resolve every named column FIRST. Doing this after the walk would
    // report the wrong error: a statement naming only an unknown column
    // necessarily omits every real one, so the walk would fail on the
    // first NOT NULL column and blame that instead of the name the user
    // actually got wrong.
    for name in named {
        resolve_column(td, name)?;
    }
    // Then walk the table's columns in id order, so the output is sorted
    // by construction rather than by a later sort.
    let mut n = 0usize;
    for c in cols {
        let supplied = named
            .iter()
            .position(|name| eq_ident(c.name.as_bytes(), name));
        let value = match supplied {
            Some(i) => bind_value(row[i], c.ty, c.nullable)?,
            None => {
                if !c.nullable {
                    return Err(SqlError::NotNull);
                }
                relational::Value::Null
            }
        };
        out[n] = BoundColumn {
            column_id: c.column_id,
            ty: c.ty,
            value,
        };
        n += 1;
    }
    Ok(n)
}

/// Extract the primary-key values from a bound row, in key order.
///
/// Key order is the descriptor's `primary_key()` order, NOT column-id
/// order — a composite key `(b, a)` sorts by `b` first, and taking the
/// values in id order would build a key that sorts differently from the
/// one the schema declared.
pub fn bound_row_key_values<'a>(
    td: &TableDescriptor,
    bound: &[BoundColumn<'a>],
    out: &mut [relational::Value<'a>],
) -> Result<usize, SqlError> {
    let pk = td.primary_key();
    if out.len() < pk.len() {
        return Err(SqlError::TooManyItems);
    }
    for (i, id) in pk.iter().enumerate() {
        let bc = bound
            .iter()
            .find(|b| b.column_id == *id)
            .ok_or(SqlError::UnknownColumn)?;
        if bc.value.is_null() {
            // Unreachable through `bind_insert_row` (key columns are
            // never nullable), but a key built from a NULL would put two
            // rows at one position, so it fails closed here too.
            return Err(SqlError::NotNull);
        }
        out[i] = bc.value;
    }
    Ok(pk.len())
}

/// The columns a `SELECT` projects, as descriptor references in output
/// order. `Star` yields every column in declaration order.
pub fn bind_projection<'t>(
    td: &'t TableDescriptor,
    proj: &Projection<'_>,
    out: &mut [&'t ColumnDescriptor],
) -> Result<usize, SqlError> {
    match proj {
        Projection::Star => {
            let cols = td.columns();
            if out.len() < cols.len() {
                return Err(SqlError::TooManyItems);
            }
            for (i, c) in cols.iter().enumerate() {
                out[i] = c;
            }
            Ok(cols.len())
        }
        Projection::Columns { names, count } => {
            if out.len() < *count {
                return Err(SqlError::TooManyItems);
            }
            for i in 0..*count {
                out[i] = resolve_column(td, names[i])?;
            }
            Ok(*count)
        }
        // An aggregate projection's result columns are COMPUTED, not stored
        // columns, so the executor builds them directly rather than through
        // this stored-column resolver.
        Projection::Aggregate { .. } => Err(SqlError::Unsupported),
    }
}

/// How a bound `SELECT` predicate can be executed.
pub enum Access<'a> {
    /// Every row of the table: a scan of the `[table_id]` prefix.
    FullScan,
    /// The predicate pins the complete primary key — a point read.
    PointRead(relational::Value<'a>),
    /// The predicate names a leading key column but not the whole key:
    /// a bounded prefix scan.
    KeyPrefix(relational::Value<'a>),
    /// A comparison on the single-column primary key: a bounded scan of
    /// the half of the table the operator names.
    KeyRange {
        op: CmpOp,
        value: relational::Value<'a>,
    },
    /// The predicate names a non-key column. Executable only as a scan
    /// with a post-filter, which the executor must apply — reported
    /// distinctly so it cannot be mistaken for `FullScan` and have the
    /// filter dropped.
    FilteredScan {
        column_id: u16,
        ty: LogicalType,
        op: CmpOp,
        value: relational::Value<'a>,
    },
}

/// Decide how to execute a `SELECT`'s predicate.
pub fn bind_access<'a>(
    td: &TableDescriptor,
    pred: Option<&Predicate<'a>>,
) -> Result<Access<'a>, SqlError> {
    let Some(pred) = pred else {
        return Ok(Access::FullScan);
    };
    let col = resolve_column(td, pred.column)?;
    // A null test is a filter over any scan, never a key bound, and its
    // `value` is unused (so it needs no binding).
    if matches!(pred.op, CmpOp::IsNull | CmpOp::IsNotNull) {
        return Ok(Access::FilteredScan {
            column_id: col.column_id,
            ty: col.ty,
            op: pred.op,
            value: relational::Value::Null,
        });
    }
    let value = bind_value(pred.value, col.ty, col.nullable)?;
    let pk = td.primary_key();
    if pk.first() == Some(&col.column_id) {
        return match pred.op {
            // Equality pins a point of the key.
            CmpOp::Eq => Ok(if pk.len() == 1 {
                Access::PointRead(value)
            } else {
                Access::KeyPrefix(value)
            }),
            // A comparison bounds a range — but only on a single-column
            // key, where the key order IS the column order. On a
            // composite key a range on the first column is a prefix
            // range this cut does not build; refuse by name.
            _ if pk.len() == 1 => Ok(Access::KeyRange { op: pred.op, value }),
            _ => Err(SqlError::Unsupported),
        };
    }
    Ok(Access::FilteredScan {
        column_id: col.column_id,
        ty: col.ty,
        op: pred.op,
        value,
    })
}

/// Choose the scan for a conjunction. One predicate drives the access
/// path; the executor applies EVERY predicate as a residual filter (the
/// driving one redundantly, which is cheap and keeps the plan simple), so
/// the answer is the full conjunction regardless of which predicate is
/// picked here.
///
/// Preference order, most selective first: an equality on the primary key
/// (a point read), then a range on the single-column key (bounded scan), a
/// composite-key prefix equality, any single non-key predicate (a filtered
/// scan the executor may serve from an index), and finally a full scan.
pub fn plan_access<'a>(td: &TableDescriptor, w: &Where<'a>) -> Result<Access<'a>, SqlError> {
    if w.count == 0 {
        return Ok(Access::FullScan);
    }
    // Validate every predicate binds (unknown column / type mismatch is an
    // error even if some other predicate could drive the scan). A null test
    // carries no value to bind.
    for p in w.as_slice() {
        let col = resolve_column(td, p.column)?;
        if !matches!(p.op, CmpOp::IsNull | CmpOp::IsNotNull) {
            bind_value(p.value, col.ty, col.nullable)?;
        }
    }
    let pk = td.primary_key();
    let is_pk = |p: &Predicate<'_>| {
        resolve_column(td, p.column).ok().map(|c| c.column_id) == pk.first().copied()
    };
    let is_range = |op: CmpOp| matches!(op, CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge);
    // 1. PK equality → point read (or composite prefix).
    if let Some(p) = w.as_slice().iter().find(|p| is_pk(p) && p.op == CmpOp::Eq) {
        return bind_access(td, Some(p));
    }
    // 2. PK range on a single-column key → bounded range. A null test on
    // the key does not bound it, so it is not a driver.
    if pk.len() == 1 {
        if let Some(p) = w.as_slice().iter().find(|p| is_pk(p) && is_range(p.op)) {
            return bind_access(td, Some(p));
        }
    }
    // 3. Any single predicate as a filtered/prefix scan.
    bind_access(td, Some(&w.preds[0]))
}

/// Map a front-end error to its wire code (`sql_exec::ERR_*`).
///
/// Exhaustive by construction — a new `SqlError` variant will not
/// compile until it has a code, which is what stops a new condition from
/// silently arriving at a client as some existing error.
pub const fn error_code(e: SqlError) -> u8 {
    match e {
        SqlError::TooLong => 1,
        SqlError::UnexpectedEnd => 2,
        SqlError::Syntax => 3,
        SqlError::NameTooLong => 4,
        SqlError::LiteralTooLong => 5,
        SqlError::TooManyItems => 6,
        SqlError::UnterminatedLiteral => 7,
        SqlError::UnknownType => 8,
        SqlError::UnknownColumn => 9,
        SqlError::TypeMismatch => 10,
        SqlError::ArityMismatch => 11,
        SqlError::NotNull => 12,
        SqlError::MissingPrimaryKey => 13,
        SqlError::DuplicateColumn => 14,
        SqlError::Unsupported => 15,
    }
}

/// The name a `Name` carries, as bytes.
pub fn name_bytes(n: &Name) -> &[u8] {
    n.as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg(sql: &str) -> Result<Statement<'_>, SqlError> {
        parse(sql.as_bytes(), Dialect::Postgres)
    }

    fn my(sql: &str) -> Result<Statement<'_>, SqlError> {
        parse(sql.as_bytes(), Dialect::MySql)
    }

    fn table(sql: &str) -> TableDescriptor {
        match pg(sql).unwrap() {
            Statement::CreateTable(ct) => bind_create_table(&ct, 1, 0, 1).unwrap(),
            _ => panic!("not a CREATE TABLE"),
        }
    }

    // ── Lexing ───────────────────────────────────────────────────────

    /// Every error variant has a DISTINCT code. A collision would make
    /// two different conditions indistinguishable to a client, which is
    /// the whole reason the enum has no catch-all variant.
    #[test]
    fn error_codes_are_distinct_and_nonzero() {
        let all = [
            SqlError::TooLong,
            SqlError::UnexpectedEnd,
            SqlError::Syntax,
            SqlError::NameTooLong,
            SqlError::LiteralTooLong,
            SqlError::TooManyItems,
            SqlError::UnterminatedLiteral,
            SqlError::UnknownType,
            SqlError::UnknownColumn,
            SqlError::TypeMismatch,
            SqlError::ArityMismatch,
            SqlError::NotNull,
            SqlError::MissingPrimaryKey,
            SqlError::DuplicateColumn,
            SqlError::Unsupported,
        ];
        let mut codes = [0u8; 15];
        for (i, e) in all.iter().enumerate() {
            let c = error_code(*e);
            assert_ne!(c, 0, "{e:?} collides with OUTCOME_OK");
            assert!(
                !codes[..i].contains(&c),
                "{e:?} shares code {c} with an earlier variant"
            );
            codes[i] = c;
        }
    }

    #[test]
    fn comments_and_whitespace_are_trivia() {
        assert!(matches!(
            pg("-- just a comment\n").unwrap(),
            Statement::Empty
        ));
        assert!(matches!(pg("/* block */ ;").unwrap(), Statement::Empty));
        assert!(matches!(pg("   ").unwrap(), Statement::Empty));
    }

    #[test]
    fn integers_refuse_to_wrap() {
        // i64::MIN must parse (accumulating negatives directly is why).
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        // One past the top is a refusal, not a wrap.
        assert_eq!(parse_i64(b"9223372036854775808"), None);
    }

    #[test]
    fn both_dialects_quoting_is_accepted() {
        for sql in [
            "CREATE TABLE \"t\" (id INT PRIMARY KEY)",
            "CREATE TABLE `t` (id INT PRIMARY KEY)",
        ] {
            assert!(
                matches!(pg(sql).unwrap(), Statement::CreateTable(_)),
                "{sql}"
            );
        }
    }

    #[test]
    fn unterminated_literals_refuse() {
        assert_eq!(
            pg("SELECT 'abc").unwrap_err(),
            SqlError::UnterminatedLiteral
        );
        assert_eq!(
            pg("CREATE TABLE \"t (id INT)").unwrap_err(),
            SqlError::UnterminatedLiteral
        );
    }

    /// An escaped quote cannot be represented in a borrowed slice, so it
    /// is refused rather than returned with the escape still in it —
    /// which would hand back a value the user did not write.
    #[test]
    fn escaped_quote_is_refused_not_mangled() {
        assert_eq!(pg("SELECT 'it''s'").unwrap_err(), SqlError::Unsupported);
    }

    // ── CREATE TABLE ─────────────────────────────────────────────────

    #[test]
    fn create_table_assigns_ids_by_declaration_order() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64), n BIGINT)");
        let cols = td.columns();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].column_id, 1);
        assert_eq!(cols[0].name.as_bytes(), b"id");
        assert_eq!(cols[1].column_id, 2);
        assert_eq!(cols[2].column_id, 3);
        assert_eq!(td.primary_key(), &[1]);
    }

    #[test]
    fn key_columns_are_not_nullable_even_when_declared_late() {
        // The table-level constraint names a column declared before it;
        // nullability must still be forced off.
        let td = table("CREATE TABLE t (a INT, b INT, PRIMARY KEY (b, a))");
        assert_eq!(td.primary_key(), &[2, 1], "key order is declaration order");
        for c in td.columns() {
            assert!(!c.nullable, "key column {:?} stayed nullable", c.name);
        }
    }

    #[test]
    fn a_table_without_a_primary_key_is_refused() {
        assert_eq!(
            pg("CREATE TABLE t (a INT)").unwrap_err(),
            SqlError::MissingPrimaryKey
        );
    }

    #[test]
    fn duplicate_column_names_are_refused() {
        assert_eq!(
            pg("CREATE TABLE t (a INT PRIMARY KEY, a INT)").unwrap_err(),
            SqlError::DuplicateColumn
        );
    }

    /// Unenforced constraints are refused by NAME rather than accepted
    /// and ignored — a client would rely on a UNIQUE that did nothing.
    #[test]
    fn unenforceable_constraints_are_refused() {
        for sql in [
            "CREATE TABLE t (a INT PRIMARY KEY, b INT UNIQUE)",
            "CREATE TABLE t (a INT PRIMARY KEY, b INT DEFAULT 3)",
            "CREATE TABLE t (a INT PRIMARY KEY, b INT REFERENCES o (id))",
            "CREATE TABLE t (a SERIAL PRIMARY KEY)",
        ] {
            let e = pg(sql).unwrap_err();
            assert!(
                matches!(e, SqlError::Unsupported | SqlError::UnknownType),
                "{sql} gave {e:?}"
            );
        }
    }

    /// MySQL trails table options after the column list. They select
    /// behaviour this build does not implement and carry no semantics to
    /// honour, so a client that always emits them must not be refused.
    #[test]
    fn mysql_table_options_are_tolerated() {
        let st = my("CREATE TABLE t (id INT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        assert!(matches!(st.unwrap(), Statement::CreateTable(_)));
    }

    #[test]
    fn if_not_exists_is_recorded() {
        match pg("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)").unwrap() {
            Statement::CreateTable(ct) => assert!(ct.if_not_exists),
            _ => panic!(),
        }
    }

    #[test]
    fn type_aliases_from_both_dialects_map_to_one_logical_type() {
        for (sql, want) in [
            ("int4", LogicalType::Int),
            ("integer", LogicalType::Int),
            ("int8", LogicalType::BigInt),
            (
                "text",
                LogicalType::VarChar {
                    length: DEFAULT_SIZED_LEN,
                },
            ),
            (
                "blob",
                LogicalType::VarBinary {
                    length: DEFAULT_SIZED_LEN,
                },
            ),
            (
                "bytea",
                LogicalType::VarBinary {
                    length: DEFAULT_SIZED_LEN,
                },
            ),
            ("datetime", LogicalType::Timestamp),
        ] {
            let td = table(&format!("CREATE TABLE t (k INT PRIMARY KEY, v {sql})"));
            assert_eq!(td.columns()[1].ty, want, "{sql}");
        }
    }

    // ── INSERT ───────────────────────────────────────────────────────

    #[test]
    fn positional_insert_covers_every_column_in_order() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))");
        let sql = "INSERT INTO t VALUES (1, 'ada')";
        let Statement::Insert(ins) = pg(sql).unwrap() else {
            panic!()
        };
        let mut bound = [BoundColumn {
            column_id: 0,
            ty: LogicalType::Null,
            value: relational::Value::Null,
        }; MAX_COLUMNS];
        let n = bind_insert_row(&td, &ins, 0, &mut bound).unwrap();
        assert_eq!(n, 2);
        assert_eq!(bound[0].column_id, 1);
        assert_eq!(bound[0].value, relational::Value::Int(1));
        assert_eq!(bound[1].value, relational::Value::Text(b"ada"));
    }

    /// A short positional tuple is an arity error, not a partial row:
    /// the client would not know which columns it had filled.
    #[test]
    fn short_positional_tuple_is_an_arity_error() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))");
        let Statement::Insert(ins) = pg("INSERT INTO t VALUES (1)").unwrap() else {
            panic!()
        };
        let mut bound = [BoundColumn {
            column_id: 0,
            ty: LogicalType::Null,
            value: relational::Value::Null,
        }; MAX_COLUMNS];
        assert_eq!(
            bind_insert_row(&td, &ins, 0, &mut bound).unwrap_err(),
            SqlError::ArityMismatch
        );
    }

    /// A named insert produces columns in ASCENDING id order regardless
    /// of the order the statement listed them — `encode_row` requires
    /// it, and requires it so a duplicate is structurally impossible.
    #[test]
    fn named_insert_output_is_sorted_by_column_id() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
        let Statement::Insert(ins) = pg("INSERT INTO t (b, id, a) VALUES (3, 1, 2)").unwrap()
        else {
            panic!()
        };
        let mut bound = [BoundColumn {
            column_id: 0,
            ty: LogicalType::Null,
            value: relational::Value::Null,
        }; MAX_COLUMNS];
        let n = bind_insert_row(&td, &ins, 0, &mut bound).unwrap();
        assert_eq!(n, 3);
        let ids: Vec<u16> = bound[..n].iter().map(|b| b.column_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        // And each value went to the column the NAME selected, not the
        // position it appeared in.
        assert_eq!(bound[0].value, relational::Value::Int(1)); // id
        assert_eq!(bound[1].value, relational::Value::Int(2)); // a
        assert_eq!(bound[2].value, relational::Value::Int(3)); // b
    }

    /// An omitted NOT NULL column is refused. There are no defaults in
    /// this subset, so accepting it would write a row the schema forbids.
    #[test]
    fn omitted_not_null_column_is_refused() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32) NOT NULL)");
        let Statement::Insert(ins) = pg("INSERT INTO t (id) VALUES (1)").unwrap() else {
            panic!()
        };
        let mut bound = [BoundColumn {
            column_id: 0,
            ty: LogicalType::Null,
            value: relational::Value::Null,
        }; MAX_COLUMNS];
        assert_eq!(
            bind_insert_row(&td, &ins, 0, &mut bound).unwrap_err(),
            SqlError::NotNull
        );
    }

    #[test]
    fn omitted_nullable_column_becomes_null() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32))");
        let Statement::Insert(ins) = pg("INSERT INTO t (id) VALUES (1)").unwrap() else {
            panic!()
        };
        let mut bound = [BoundColumn {
            column_id: 0,
            ty: LogicalType::Null,
            value: relational::Value::Null,
        }; MAX_COLUMNS];
        let n = bind_insert_row(&td, &ins, 0, &mut bound).unwrap();
        assert_eq!(n, 2);
        assert!(bound[1].value.is_null());
    }

    #[test]
    fn unknown_insert_column_is_named_as_such() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY)");
        let Statement::Insert(ins) = pg("INSERT INTO t (nope) VALUES (1)").unwrap() else {
            panic!()
        };
        let mut bound = [BoundColumn {
            column_id: 0,
            ty: LogicalType::Null,
            value: relational::Value::Null,
        }; MAX_COLUMNS];
        assert_eq!(
            bind_insert_row(&td, &ins, 0, &mut bound).unwrap_err(),
            SqlError::UnknownColumn
        );
    }

    #[test]
    fn multi_row_insert_keeps_each_tuple() {
        let Statement::Insert(ins) =
            pg("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap()
        else {
            panic!()
        };
        assert_eq!(ins.row_count(), 3);
        assert_eq!(ins.row(1).unwrap()[1], Literal::Str(b"b"));
        assert!(ins.row(3).is_none());
    }

    // ── Binding types ────────────────────────────────────────────────

    /// The type comes from the SCHEMA, not the literal's syntax. `1`
    /// becomes whatever the column declared.
    #[test]
    fn one_literal_binds_to_the_columns_type() {
        use relational::Value as V;
        assert_eq!(
            bind_value(Literal::Int(1), LogicalType::SmallInt, false).unwrap(),
            V::SmallInt(1)
        );
        assert_eq!(
            bind_value(Literal::Int(1), LogicalType::BigInt, false).unwrap(),
            V::BigInt(1)
        );
        assert_eq!(
            bind_value(Literal::Int(1), LogicalType::Double, false).unwrap(),
            V::Double(1.0)
        );
    }

    /// An integer into a text column is REFUSED, not stringified. A
    /// stringified integer would sort and compare as text, so the row
    /// could not be found again by its number.
    #[test]
    fn integer_into_text_is_refused_not_stringified() {
        assert_eq!(
            bind_value(Literal::Int(1), LogicalType::VarChar { length: 32 }, false).unwrap_err(),
            SqlError::TypeMismatch
        );
    }

    /// Narrowing that would lose information refuses rather than wraps.
    #[test]
    fn out_of_range_narrowing_refuses() {
        assert_eq!(
            bind_value(Literal::Int(40_000), LogicalType::SmallInt, false).unwrap_err(),
            SqlError::TypeMismatch
        );
        assert_eq!(
            bind_value(Literal::Int(i64::MAX), LogicalType::Int, false).unwrap_err(),
            SqlError::TypeMismatch
        );
    }

    /// The declared length is enforced, which is the payoff for carrying
    /// the modifier through `parse_type`. A value longer than the column
    /// allows is refused, not truncated — a truncated string is a
    /// different value and the client would never know.
    #[test]
    fn declared_length_is_enforced_not_truncated() {
        let ty = LogicalType::VarChar { length: 3 };
        assert_eq!(
            bind_value(Literal::Str(b"abcd"), ty, false).unwrap_err(),
            SqlError::LiteralTooLong
        );
        assert_eq!(
            bind_value(Literal::Str(b"abc"), ty, false).unwrap(),
            relational::Value::Text(b"abc")
        );
    }

    /// The length reaches the descriptor, so a table declared
    /// `VARCHAR(10)` is a different type from `VARCHAR(200)` — dropping
    /// the modifier would silently store a column whose width disagrees
    /// with the declaration.
    #[test]
    fn declared_length_reaches_the_descriptor() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(10))");
        assert_eq!(td.columns()[1].ty, LogicalType::VarChar { length: 10 });
    }

    /// A modifier on a type that has none would be silently dropped and
    /// the client would believe it applied.
    #[test]
    fn a_modifier_on_an_unsized_type_is_refused() {
        assert_eq!(
            pg("CREATE TABLE t (id INT(11) PRIMARY KEY)").unwrap_err(),
            SqlError::Unsupported
        );
    }

    /// An aggregate over a table parses into an aggregate projection;
    /// without a FROM the same call is connection-setup chatter answered
    /// with a constant, NOT a table query.
    #[test]
    fn aggregate_over_a_table_parses_chatter_stays_constant() {
        let Statement::Select(s) = pg("SELECT count(*) FROM t").unwrap() else {
            panic!("aggregate must be a table SELECT")
        };
        let Projection::Aggregate { items, count } = s.projection else {
            panic!("expected an aggregate projection")
        };
        assert_eq!(count, 1);
        assert_eq!(items[0].func, Some(AggFunc::Count));
        assert!(items[0].star);

        // Same function, no FROM: connector chatter, answerable.
        assert!(matches!(
            pg("SELECT count(*)").unwrap(),
            Statement::Session(SessionStatement::SelectConstant { .. })
        ));
    }

    #[test]
    fn aggregate_functions_and_group_by_parse() {
        let Statement::Select(s) = pg(
            "SELECT city, COUNT(*), SUM(age), MIN(age), MAX(age), AVG(age) FROM t GROUP BY city",
        )
        .unwrap() else {
            panic!()
        };
        assert_eq!(s.group_by, Some(&b"city"[..]));
        let Projection::Aggregate { items, count } = s.projection else {
            panic!("expected aggregate projection")
        };
        assert_eq!(count, 6);
        assert_eq!(items[0].func, None, "the group column is a bare item");
        assert_eq!(items[0].column, b"city");
        assert_eq!(
            [
                items[1].func,
                items[2].func,
                items[3].func,
                items[4].func,
                items[5].func
            ],
            [
                Some(AggFunc::Count),
                Some(AggFunc::Sum),
                Some(AggFunc::Min),
                Some(AggFunc::Max),
                Some(AggFunc::Avg),
            ]
        );
    }

    #[test]
    fn between_desugars_to_two_conjuncts_and_is_null_parses() {
        let Statement::Select(s) = pg("SELECT * FROM t WHERE age BETWEEN 10 AND 20").unwrap()
        else {
            panic!()
        };
        assert_eq!(s.where_.count, 2, "BETWEEN is two predicates");
        assert_eq!(
            (s.where_.preds[0].op, s.where_.preds[0].value),
            (CmpOp::Ge, Literal::Int(10))
        );
        assert_eq!(
            (s.where_.preds[1].op, s.where_.preds[1].value),
            (CmpOp::Le, Literal::Int(20))
        );

        let Statement::Select(s) = pg("SELECT * FROM t WHERE name IS NULL").unwrap() else {
            panic!()
        };
        assert_eq!(s.where_.preds[0].op, CmpOp::IsNull);
        let Statement::Select(s) =
            pg("SELECT * FROM t WHERE name IS NOT NULL AND age > 5").unwrap()
        else {
            panic!()
        };
        assert_eq!(s.where_.count, 2);
        assert_eq!(s.where_.preds[0].op, CmpOp::IsNotNull);
        assert_eq!(s.where_.preds[1].op, CmpOp::Gt);
    }

    #[test]
    fn in_lists_and_distinct_offset_parse() {
        let Statement::Select(s) = pg("SELECT * FROM t WHERE id IN (1, 2, 3)").unwrap() else {
            panic!()
        };
        assert!(s.where_.in_active && !s.where_.in_negate);
        assert_eq!(s.where_.in_col, b"id");
        assert_eq!(s.where_.in_count, 3);

        let Statement::Select(s) = pg("SELECT * FROM t WHERE name NOT IN ('a')").unwrap() else {
            panic!()
        };
        assert!(s.where_.in_active && s.where_.in_negate && s.where_.in_count == 1);

        let Statement::Select(s) =
            pg("SELECT DISTINCT name FROM t ORDER BY name DESC LIMIT 5 OFFSET 2").unwrap()
        else {
            panic!()
        };
        assert!(s.distinct);
        assert_eq!(s.limit, Some(5));
        assert_eq!(s.offset, Some(2));
        assert!(s.order.unwrap().desc);

        // Two IN lists refuse — one per query keeps the filter bounded.
        assert_eq!(
            pg("SELECT * FROM t WHERE a IN (1) AND b IN (2)").unwrap_err(),
            SqlError::Unsupported
        );
    }

    #[test]
    fn a_bare_column_beside_an_aggregate_needs_group_by() {
        // Without GROUP BY, `city` is neither grouped nor aggregated.
        assert_eq!(
            pg("SELECT city, COUNT(*) FROM t").unwrap_err(),
            SqlError::Unsupported
        );
        // A non-group bare column WITH a group by a different column refuses.
        assert_eq!(
            pg("SELECT name, COUNT(*) FROM t GROUP BY city").unwrap_err(),
            SqlError::Unsupported
        );
        // `SUM(*)` is nonsense — only COUNT takes a star.
        assert_eq!(
            pg("SELECT SUM(*) FROM t").unwrap_err(),
            SqlError::Unsupported
        );
    }

    #[test]
    fn postgres_two_word_type_names_parse() {
        let td = table(
            "CREATE TABLE t (id INT PRIMARY KEY, a DOUBLE PRECISION, b CHARACTER VARYING(8))",
        );
        assert_eq!(td.columns()[1].ty, LogicalType::Double);
        assert_eq!(td.columns()[2].ty, LogicalType::VarChar { length: 8 });
    }

    #[test]
    fn null_into_a_not_null_column_refuses() {
        assert_eq!(
            bind_value(Literal::Null, LogicalType::Int, false).unwrap_err(),
            SqlError::NotNull
        );
        assert!(bind_value(Literal::Null, LogicalType::Int, true)
            .unwrap()
            .is_null());
    }

    // ── SELECT ───────────────────────────────────────────────────────

    #[test]
    fn star_projects_every_column_in_declaration_order() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
        let Statement::Select(s) = pg("SELECT * FROM t").unwrap() else {
            panic!()
        };
        let mut out = [&td.columns()[0]; MAX_LIST_COLS];
        let n = bind_projection(&td, &s.projection, &mut out).unwrap();
        assert_eq!(n, 3);
        assert_eq!(out[0].name.as_bytes(), b"id");
        assert_eq!(out[2].name.as_bytes(), b"b");
    }

    #[test]
    fn named_projection_keeps_the_requested_order() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
        let Statement::Select(s) = pg("SELECT b, id FROM t").unwrap() else {
            panic!()
        };
        let mut out = [&td.columns()[0]; MAX_LIST_COLS];
        let n = bind_projection(&td, &s.projection, &mut out).unwrap();
        assert_eq!(n, 2);
        assert_eq!(out[0].name.as_bytes(), b"b");
        assert_eq!(out[1].name.as_bytes(), b"id");
    }

    #[test]
    fn a_single_key_equality_is_a_point_read() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, a INT)");
        let Statement::Select(s) = pg("SELECT * FROM t WHERE id = 7").unwrap() else {
            panic!()
        };
        assert!(matches!(
            plan_access(&td, &s.where_).unwrap(),
            Access::PointRead(relational::Value::Int(7))
        ));
    }

    #[test]
    fn delete_parses_with_and_without_a_predicate() {
        let Statement::Delete(d) = pg("DELETE FROM t WHERE id = 7").unwrap() else {
            panic!("not a delete")
        };
        assert_eq!(d.table, b"t");
        assert_eq!(d.where_.count, 1);
        let p = d.where_.preds[0];
        assert_eq!(p.column, b"id");
        assert_eq!(p.value, Literal::Int(7));

        let Statement::Delete(d) = pg("DELETE FROM t").unwrap() else {
            panic!("not a delete")
        };
        assert_eq!(d.where_.count, 0, "an unqualified DELETE has no predicate");
    }

    #[test]
    fn update_parses_multiple_assignments() {
        let Statement::Update(u) = pg("UPDATE t SET a = 5, b = 'x' WHERE id = 7").unwrap() else {
            panic!("not an update")
        };
        assert_eq!(u.table, b"t");
        assert_eq!(u.set_count, 2);
        assert_eq!(
            (u.sets[0].column, u.sets[0].value),
            (&b"a"[..], Literal::Int(5))
        );
        assert_eq!(u.sets[1].column, b"b");
        assert!(matches!(u.sets[1].value, Literal::Str(_)));
        assert_eq!(u.where_.preds[0].column, b"id");

        // No WHERE = every row.
        let Statement::Update(u) = pg("UPDATE t SET a = 1").unwrap() else {
            panic!("not an update")
        };
        assert_eq!(u.set_count, 1);
        assert_eq!(u.where_.count, 0);
    }

    #[test]
    fn conjunctions_parse_and_disjunctions_refuse() {
        // AND is a conjunction the plan carries as residual filters.
        let Statement::Delete(d) = pg("DELETE FROM t WHERE a = 1 AND b = 2").unwrap() else {
            panic!("not a delete")
        };
        assert_eq!(d.where_.count, 2);
        assert_eq!(d.where_.preds[0].column, b"a");
        assert_eq!(d.where_.preds[1].column, b"b");

        // OR has no single-scan plan here: refuse, never drop.
        assert_eq!(
            my("UPDATE t SET a = 1 WHERE a = 1 OR b = 2").unwrap_err(),
            SqlError::Unsupported
        );
        // A conjunction longer than the plan can carry refuses, too.
        assert_eq!(
            pg("SELECT * FROM t WHERE a=1 AND b=2 AND c=3 AND d=4 AND e=5").unwrap_err(),
            SqlError::TooManyItems
        );
    }

    /// A leading key column of a COMPOSITE key is a prefix scan, not a
    /// point read — the predicate does not pin a unique row.
    #[test]
    fn a_leading_composite_key_equality_is_a_prefix_scan() {
        let td = table("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))");
        let Statement::Select(s) = pg("SELECT * FROM t WHERE a = 7").unwrap() else {
            panic!()
        };
        assert!(matches!(
            plan_access(&td, &s.where_).unwrap(),
            Access::KeyPrefix(_)
        ));
    }

    /// A non-key predicate is reported DISTINCTLY from a full scan, so
    /// the executor cannot mistake it for one and drop the filter —
    /// which would return rows the client's WHERE excluded.
    #[test]
    fn a_non_key_predicate_is_a_filtered_scan() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY, a INT)");
        let Statement::Select(s) = pg("SELECT * FROM t WHERE a = 7").unwrap() else {
            panic!()
        };
        match plan_access(&td, &s.where_).unwrap() {
            Access::FilteredScan { column_id, .. } => assert_eq!(column_id, 2),
            _ => panic!("a non-key filter must not degrade to FullScan"),
        }
    }

    #[test]
    fn no_predicate_is_a_full_scan() {
        let td = table("CREATE TABLE t (id INT PRIMARY KEY)");
        let Statement::Select(s) = pg("SELECT * FROM t").unwrap() else {
            panic!()
        };
        assert!(matches!(
            plan_access(&td, &s.where_).unwrap(),
            Access::FullScan
        ));
    }

    #[test]
    fn limit_is_carried() {
        let Statement::Select(s) = pg("SELECT * FROM t LIMIT 10").unwrap() else {
            panic!()
        };
        assert_eq!(s.limit, Some(10));
    }

    /// Clauses that would change the answer are refused rather than
    /// ignored. An ignored WHERE returns extra rows; an ignored JOIN or
    /// GROUP BY answers a different question entirely.
    #[test]
    fn answer_changing_clauses_are_refused_not_ignored() {
        for sql in [
            "SELECT * FROM t JOIN u ON t.a = u.a",
            "SELECT * FROM t WHERE a = 1 OR b = 2",
        ] {
            let e = pg(sql).unwrap_err();
            assert!(
                matches!(e, SqlError::Unsupported | SqlError::Syntax),
                "{sql} gave {e:?}"
            );
        }
    }

    /// Every comparison operator parses into a predicate carrying that
    /// operator — the executor decides point-read vs range from it.
    #[test]
    fn comparison_operators_parse_into_the_predicate() {
        for (sql, want) in [
            ("SELECT * FROM t WHERE a = 1", CmpOp::Eq),
            ("SELECT * FROM t WHERE a < 1", CmpOp::Lt),
            ("SELECT * FROM t WHERE a <= 1", CmpOp::Le),
            ("SELECT * FROM t WHERE a > 1", CmpOp::Gt),
            ("SELECT * FROM t WHERE a >= 1", CmpOp::Ge),
        ] {
            let Statement::Select(s) = pg(sql).unwrap() else {
                panic!("{sql}")
            };
            assert_eq!(s.where_.preds[0].op, want, "{sql}");
        }
    }

    /// `<>` is not in the executable subset: an inequality is a full scan
    /// with a negated filter this cut does not build, so refuse it.
    #[test]
    fn not_equal_is_refused() {
        assert!(matches!(
            pg("SELECT * FROM t WHERE a <> 1"),
            Err(SqlError::Unsupported | SqlError::Syntax)
        ));
    }

    /// ORDER BY parses into structure — direction and column — so the
    /// executor can honour ascending-on-key and refuse the rest.
    #[test]
    fn order_by_parses_with_direction() {
        let Statement::Select(s) = pg("SELECT * FROM t ORDER BY a").unwrap() else {
            panic!()
        };
        let ob = s.order.unwrap();
        assert_eq!(ob.column, b"a");
        assert!(!ob.desc, "no direction defaults to ASC");

        let Statement::Select(s) = pg("SELECT * FROM t ORDER BY a DESC").unwrap() else {
            panic!()
        };
        assert!(s.order.unwrap().desc);

        // ORDER BY still composes with LIMIT.
        let Statement::Select(s) = pg("SELECT * FROM t ORDER BY a ASC LIMIT 5").unwrap() else {
            panic!()
        };
        assert!(!s.order.unwrap().desc);
        assert_eq!(s.limit, Some(5));
    }

    /// A second statement after a complete one is refused. Executing it
    /// silently would run something the caller never saw.
    #[test]
    fn a_second_statement_is_refused() {
        assert_eq!(
            pg("SELECT * FROM t; DROP TABLE t").unwrap_err(),
            SqlError::Unsupported
        );
    }

    #[test]
    fn trailing_semicolons_are_fine() {
        assert!(matches!(
            pg("SELECT * FROM t;").unwrap(),
            Statement::Select(_)
        ));
        assert!(matches!(
            pg("SELECT * FROM t;;").unwrap(),
            Statement::Select(_)
        ));
    }

    // ── Session statements a real client sends ────────────────────────

    #[test]
    fn startup_chatter_is_answerable() {
        use SessionStatement as S;
        assert!(matches!(
            pg("SELECT 1").unwrap(),
            Statement::Session(S::SelectConstant { .. })
        ));
        assert!(matches!(pg("BEGIN").unwrap(), Statement::Session(S::Begin)));
        assert!(matches!(
            pg("START TRANSACTION").unwrap(),
            Statement::Session(S::Begin)
        ));
        assert!(matches!(
            pg("COMMIT").unwrap(),
            Statement::Session(S::Commit)
        ));
        assert!(matches!(
            pg("ROLLBACK").unwrap(),
            Statement::Session(S::Rollback)
        ));
        assert!(matches!(
            pg("SET client_encoding TO 'UTF8'").unwrap(),
            Statement::Session(S::Set)
        ));
        assert!(matches!(
            my("SET autocommit=1").unwrap(),
            Statement::Session(S::Set)
        ));
        match pg("SHOW TRANSACTION ISOLATION LEVEL").unwrap() {
            Statement::Session(S::Show(w)) => assert!(eq_ident(w, b"transaction")),
            _ => panic!(),
        }
    }

    #[test]
    fn select_constant_keeps_its_alias() {
        match pg("SELECT 1 AS one").unwrap() {
            Statement::Session(SessionStatement::SelectConstant { value, alias }) => {
                assert_eq!(value, Literal::Int(1));
                assert_eq!(alias, Some(b"one".as_slice()));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn a_function_call_projection_is_a_connector_constant() {
        assert!(matches!(
            pg("SELECT version()").unwrap(),
            Statement::Session(SessionStatement::SelectConstant { .. })
        ));
    }

    /// A driver's prepared statement is named as such, so the client is
    /// told "parameters unsupported" rather than "syntax error".
    #[test]
    fn placeholders_are_named_not_syntax_errors() {
        assert_eq!(
            pg("INSERT INTO t VALUES ($1)").unwrap_err(),
            SqlError::Unsupported
        );
        assert_eq!(
            my("INSERT INTO t VALUES (?)").unwrap_err(),
            SqlError::Unsupported
        );
    }

    #[test]
    fn qualified_names_drop_the_default_schema_and_refuse_others() {
        assert!(matches!(
            pg("SELECT * FROM public.t").unwrap(),
            Statement::Select(_)
        ));
        assert_eq!(
            pg("SELECT * FROM other.t").unwrap_err(),
            SqlError::Unsupported
        );
    }

    #[test]
    fn drop_table_parses_with_if_exists() {
        match pg("DROP TABLE IF EXISTS t").unwrap() {
            Statement::DropTable { name, if_exists } => {
                assert_eq!(name, b"t");
                assert!(if_exists);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn oversized_input_is_refused_by_length() {
        let long = "x".repeat(MAX_SQL_LEN + 1);
        assert_eq!(pg(&long).unwrap_err(), SqlError::TooLong);
    }

    // ── Key values ───────────────────────────────────────────────────

    /// Key values come out in the KEY's order, not column-id order — a
    /// composite key `(b, a)` sorts by `b` first, so taking them in id
    /// order would build a key that sorts differently from the schema.
    #[test]
    fn key_values_follow_key_order_not_column_order() {
        let td = table("CREATE TABLE t (a INT, b INT, PRIMARY KEY (b, a))");
        let Statement::Insert(ins) = pg("INSERT INTO t VALUES (10, 20)").unwrap() else {
            panic!()
        };
        let mut bound = [BoundColumn {
            column_id: 0,
            ty: LogicalType::Null,
            value: relational::Value::Null,
        }; MAX_COLUMNS];
        let n = bind_insert_row(&td, &ins, 0, &mut bound).unwrap();
        let mut kv = [relational::Value::Null; MAX_KEY_COLUMNS];
        let k = bound_row_key_values(&td, &bound[..n], &mut kv).unwrap();
        assert_eq!(k, 2);
        // Key is (b, a) so b's value (20) comes first.
        assert_eq!(kv[0], relational::Value::Int(20));
        assert_eq!(kv[1], relational::Value::Int(10));
    }
}
