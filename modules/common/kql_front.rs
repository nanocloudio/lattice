//! KQL (Kusto) front-end — the genericity stress test. Kusto is a *tabular
//! pipe* language, structurally unlike PromQL, yet a metrics query in it lowers
//! onto the very same `tsquery_core::TsQuery` the PromQL front-end targets. If
//! it does, the core is proven language-agnostic rather than merely asserted to
//! be. Pure logic, `no_std`, no allocation.
//!
//! ## The v1 subset
//!
//! ```text
//!   <table> [ | where <pred> [and <pred>]* ]
//!            [ | summarize <agg> [ ( [<col>] ) ] [by <keys>] ]
//!   <pred> := <ident> (== | !=) "string"
//!   <agg>  := sum | avg | min | max | count
//!   <keys> := <ident-or-bin> [, <ident-or-bin>]*   (bin(...) is the time axis)
//! ```
//!
//! `where` lowers to label matchers; `summarize <agg> by <labels>` lowers to
//! the cross-series spatial aggregation; a `bin(Timestamp, ...)` key is the
//! time axis and is satisfied by the API grid, so it is dropped from the group
//! set. Anything outside this — `join`, `mv-expand`, `project`, `extend`,
//! string operators, non-metric tables — is **refused by name**.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor module and host tests; each consumer uses a subset of the surface"
)]

#[path = "tsquery_core.rs"]
pub mod core;

use core::{metric_id, Grouping, MatchOp, Matcher, SpatialOp, TemporalOp, TsQuery, Unsupported};

/// A parsed KQL query, grid-independent. Finish with [`Parsed::into_query`].
#[derive(Debug)]
pub struct Parsed<'a> {
    table: &'a [u8],
    n_matchers: usize,
    combine: Option<SpatialOp>,
    n_group: usize,
}

impl<'a> Parsed<'a> {
    #[allow(
        clippy::wrong_self_convention,
        reason = "`into_query` borrows `self`'s parse output into the caller's grid buffers rather than consuming it; the name reads as the finishing step of the parse, matching promql_front::Parsed"
    )]
    pub fn into_query(
        &self,
        matcher_buf: &'a [Matcher<'a>],
        group_buf: &'a [&'a [u8]],
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> TsQuery<'a> {
        let grouping = self.combine.map(|combine| Grouping {
            combine,
            group: &group_buf[..self.n_group],
            without: false,
        });
        TsQuery {
            metric_id: metric_id(self.table),
            matchers: &matcher_buf[..self.n_matchers],
            temporal: TemporalOp::Last,
            range_ms: step_ms,
            start_ms,
            end_ms,
            step_ms,
            grouping,
        }
    }
}

/// Parse and lower a KQL metrics query. Matchers and group labels are written
/// into caller scratch (borrowing `input`).
pub fn lower<'a>(
    input: &'a [u8],
    matcher_buf: &mut [Matcher<'a>],
    group_buf: &mut [&'a [u8]],
) -> Result<Parsed<'a>, Unsupported> {
    let mut p = Parser { s: input, i: 0 };
    p.ws();
    let table = p.ident().ok_or(Unsupported::Construct)?;
    let mut n_matchers = 0usize;
    let mut combine = None;
    let mut n_group = 0usize;

    loop {
        p.ws();
        if p.i >= p.s.len() {
            break;
        }
        if !p.eat(b'|') {
            return Err(Unsupported::Construct);
        }
        p.ws();
        let op = p.ident().ok_or(Unsupported::Construct)?;
        if op == b"where" {
            n_matchers = p.where_clause(matcher_buf, n_matchers)?;
        } else if op == b"summarize" {
            let (c, g) = p.summarize_clause(group_buf)?;
            combine = Some(c);
            n_group = g;
        } else {
            // project / extend / join / mv-expand / take / … — not metric ops.
            return Err(Unsupported::Construct);
        }
    }

    Ok(Parsed {
        table,
        n_matchers,
        combine,
        n_group,
    })
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }
    fn eat(&mut self, b: u8) -> bool {
        if self.peek() == Some(b) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn ident(&mut self) -> Option<&'a [u8]> {
        let start = self.i;
        match self.peek() {
            Some(c) if is_ident_start(c) => self.i += 1,
            _ => return None,
        }
        while let Some(c) = self.peek() {
            if is_ident_cont(c) {
                self.i += 1;
            } else {
                break;
            }
        }
        Some(&self.s[start..self.i])
    }
    fn string(&mut self) -> Result<&'a [u8], Unsupported> {
        if !self.eat(b'"') {
            return Err(Unsupported::Construct);
        }
        let start = self.i;
        while let Some(c) = self.peek() {
            if c == b'"' {
                let out = &self.s[start..self.i];
                self.i += 1;
                return Ok(out);
            }
            if c == b'\\' {
                return Err(Unsupported::Construct);
            }
            self.i += 1;
        }
        Err(Unsupported::Malformed)
    }

    /// `where <ident> (== | !=) "v" [and …]`.
    fn where_clause(
        &mut self,
        matcher_buf: &mut [Matcher<'a>],
        mut n: usize,
    ) -> Result<usize, Unsupported> {
        loop {
            self.ws();
            let name = self.ident().ok_or(Unsupported::Construct)?;
            self.ws();
            let op = if self.eat(b'=') {
                if self.eat(b'=') {
                    MatchOp::Eq
                } else {
                    return Err(Unsupported::Construct);
                }
            } else if self.eat(b'!') {
                if self.eat(b'=') {
                    MatchOp::Ne
                } else {
                    return Err(Unsupported::Construct);
                }
            } else {
                return Err(Unsupported::Construct);
            };
            self.ws();
            let value = self.string()?;
            if n >= matcher_buf.len() {
                return Err(Unsupported::TooWide);
            }
            matcher_buf[n] = Matcher { name, op, value };
            n += 1;
            self.ws();
            // `and` chains predicates; anything else ends the clause.
            if self.looking_at_kw(b"and") {
                self.i += 3;
                continue;
            }
            return Ok(n);
        }
    }

    /// `summarize <agg>(<col>) [by <key> [, <key>]*]`. A `bin(...)` key is the
    /// time axis and is dropped from the group set.
    fn summarize_clause(
        &mut self,
        group_buf: &mut [&'a [u8]],
    ) -> Result<(SpatialOp, usize), Unsupported> {
        self.ws();
        let agg = self.ident().ok_or(Unsupported::Construct)?;
        let combine = aggregator(agg).ok_or(Unsupported::Construct)?;
        self.ws();
        // Optional `(col)`; the ident itself is optional too, so a bare
        // `count()` parses.
        if self.eat(b'(') {
            self.ws();
            let _ = self.ident(); // column name (or `*` for count) — ignored
            self.ws();
            if !self.eat(b')') {
                // No closing paren: malformed call.
                return Err(Unsupported::Construct);
            }
        }
        self.ws();
        let mut n = 0usize;
        if self.looking_at_kw(b"by") {
            self.i += 2;
            loop {
                self.ws();
                let key = self.ident().ok_or(Unsupported::Construct)?;
                self.ws();
                if key == b"bin" {
                    // `bin(Timestamp, <dur>)` — the time axis. Consume the
                    // parenthesised args and drop it from the group set.
                    if !self.eat(b'(') {
                        return Err(Unsupported::Construct);
                    }
                    self.consume_to_close_paren()?;
                } else if n < group_buf.len() {
                    group_buf[n] = key;
                    n += 1;
                } else {
                    return Err(Unsupported::TooWide);
                }
                self.ws();
                if self.eat(b',') {
                    continue;
                }
                break;
            }
        }
        Ok((combine, n))
    }

    fn consume_to_close_paren(&mut self) -> Result<(), Unsupported> {
        let mut depth = 1;
        while let Some(c) = self.peek() {
            self.i += 1;
            match c {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
        Err(Unsupported::Malformed)
    }

    fn looking_at_kw(&self, kw: &[u8]) -> bool {
        let end = self.i + kw.len();
        if end > self.s.len() || &self.s[self.i..end] != kw {
            return false;
        }
        !self.s.get(end).copied().map(is_ident_cont).unwrap_or(false)
    }
}

fn aggregator(id: &[u8]) -> Option<SpatialOp> {
    match id {
        b"sum" => Some(SpatialOp::Sum),
        b"avg" => Some(SpatialOp::Avg),
        b"min" => Some(SpatialOp::Min),
        b"max" => Some(SpatialOp::Max),
        b"count" => Some(SpatialOp::Count),
        _ => None,
    }
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}
fn is_ident_cont(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}
