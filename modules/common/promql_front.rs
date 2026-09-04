//! PromQL front-end — parses the dashboard-query subset of PromQL and lowers
//! it onto the language-agnostic `tsquery_core::TsQuery`. Pure logic,
//! `no_std`, no allocation: it borrows all its bytes from the request string
//! and writes matchers/group-labels into caller-provided scratch, so a
//! lowered query holds no heap.
//!
//! This is one of several front-ends (MetricsQL, KQL are siblings); each
//! parses a dialect and targets the *same* core. The engine knows no
//! language; the language lives here.
//!
//! ## The v1 subset
//!
//! ```text
//!   <agg> ( <selector-or-rate> ) [by|without (<labels>)]
//!   <selector-or-rate>
//!   rate ( <selector> "[" <duration> "]" )
//!   <selector> := metric | metric{ l op "v", ... }
//!   <agg>      := sum | avg | min | max | count
//!   op         := = | !=
//!   duration   := <int>(ms|s|m|h)
//! ```
//!
//! The grid (start/end/step) comes from the query API parameters, not the
//! expression; the bracketed duration on a `rate(...)` sets the per-step
//! lookback window and is required here (the MetricsQL front-end supplies
//! a default when it is omitted). Anything outside this subset —
//! subqueries, `@`/offset, regex matchers, binary-operator trees,
//! `histogram_quantile`, nested functions — is **refused**
//! (`Unsupported::Construct`), never silently mis-evaluated.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor module and host tests; each consumer uses a subset of the surface"
)]

#[path = "tsquery_core.rs"]
pub mod core;

use core::{metric_id, Grouping, MatchOp, Matcher, SpatialOp, TemporalOp, TsQuery, Unsupported};

/// A parsed-but-not-yet-gridded query: the selector, the temporal choice,
/// and any spatial grouping. The caller supplies the grid (start/end/step
/// from the API) to finish it into a `TsQuery` via [`Parsed::into_query`].
#[derive(Debug)]
pub struct Parsed<'a> {
    metric: &'a [u8],
    n_matchers: usize,
    temporal: TemporalOp,
    range_ms: u64,
    combine: Option<SpatialOp>,
    without: bool,
    n_group: usize,
}

impl<'a> Parsed<'a> {
    /// Finish into a `TsQuery` over the given grid. `matcher_buf`/`group_buf`
    /// are the same scratch buffers passed to [`lower`]; they hold the
    /// borrowed matcher/label slices.
    #[allow(
        clippy::wrong_self_convention,
        reason = "`into_query` borrows `self`'s parse output into the caller's grid buffers rather than consuming it; the name reads as the finishing step of the parse, and every consumer holds the `Parsed` only to call this"
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
            without: self.without,
        });
        // For an instant selector (no rate) the lookback window is one step.
        let range_ms = if self.range_ms == 0 {
            step_ms
        } else {
            self.range_ms
        };
        TsQuery {
            metric_id: metric_id(self.metric),
            matchers: &matcher_buf[..self.n_matchers],
            temporal: self.temporal,
            range_ms,
            start_ms,
            end_ms,
            step_ms,
            grouping,
        }
    }
}

/// Parse `input` and lower it, writing matchers into `matcher_buf` and
/// grouping labels into `group_buf` (both borrow from `input`). Returns the
/// parse result; combine with the API grid via [`Parsed::into_query`].
pub fn lower<'a>(
    input: &'a [u8],
    matcher_buf: &mut [Matcher<'a>],
    group_buf: &mut [&'a [u8]],
) -> Result<Parsed<'a>, Unsupported> {
    let mut p = Parser { s: input, i: 0 };
    p.ws();
    let parsed = p.expr(matcher_buf, group_buf)?;
    p.ws();
    if p.i != p.s.len() {
        // Trailing tokens (a binary-operator tail, an `@`/offset, …).
        return Err(Unsupported::Construct);
    }
    Ok(parsed)
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

    /// An identifier: `[a-zA-Z_:][a-zA-Z0-9_:]*`.
    fn ident(&mut self) -> Option<&'a [u8]> {
        let start = self.i;
        if let Some(c) = self.peek() {
            if !is_ident_start(c) {
                return None;
            }
        } else {
            return None;
        }
        self.i += 1;
        while let Some(c) = self.peek() {
            if is_ident_cont(c) {
                self.i += 1;
            } else {
                break;
            }
        }
        Some(&self.s[start..self.i])
    }

    /// `<agg>(...) [by|without(...)]` or a bare selector / `rate(...)`.
    fn expr(
        &mut self,
        matcher_buf: &mut [Matcher<'a>],
        group_buf: &mut [&'a [u8]],
    ) -> Result<Parsed<'a>, Unsupported> {
        let save = self.i;
        // Try an aggregation head: an identifier immediately followed by `(`
        // or `by`/`without`, where the identifier is an aggregator name.
        if let Some(id) = self.ident() {
            if let Some(combine) = aggregator(id) {
                self.ws();
                // Two forms: `sum(...) by(...)` and `sum by(...) (...)`.
                let mut without = false;
                let mut n_group = 0usize;
                if self.looking_at_kw(b"by") || self.looking_at_kw(b"without") {
                    let (w, n) = self.grouping_clause(group_buf)?;
                    without = w;
                    n_group = n;
                    self.ws();
                }
                if !self.eat(b'(') {
                    return Err(Unsupported::Construct);
                }
                self.ws();
                let mut inner = self.selector_or_rate(matcher_buf)?;
                self.ws();
                if !self.eat(b')') {
                    return Err(Unsupported::Construct);
                }
                self.ws();
                if n_group == 0 && (self.looking_at_kw(b"by") || self.looking_at_kw(b"without")) {
                    let (w, n) = self.grouping_clause(group_buf)?;
                    without = w;
                    n_group = n;
                }
                inner.combine = Some(combine);
                inner.without = without;
                inner.n_group = n_group;
                return Ok(inner);
            }
        }
        // Not an aggregation: rewind and parse a bare selector / rate.
        self.i = save;
        self.selector_or_rate(matcher_buf)
    }

    fn looking_at_kw(&self, kw: &[u8]) -> bool {
        let end = self.i + kw.len();
        if end > self.s.len() {
            return false;
        }
        if &self.s[self.i..end] != kw {
            return false;
        }
        // Must not be part of a longer identifier.
        !self.s.get(end).copied().map(is_ident_cont).unwrap_or(false)
    }

    /// `by (a, b)` / `without (a, b)` → (without?, count). Assumes a keyword
    /// is present.
    fn grouping_clause(
        &mut self,
        group_buf: &mut [&'a [u8]],
    ) -> Result<(bool, usize), Unsupported> {
        let without = if self.looking_at_kw(b"without") {
            self.i += 7;
            true
        } else {
            self.i += 2; // "by"
            false
        };
        self.ws();
        if !self.eat(b'(') {
            return Err(Unsupported::Construct);
        }
        let mut n = 0usize;
        loop {
            self.ws();
            if self.eat(b')') {
                break;
            }
            let name = self.ident().ok_or(Unsupported::Construct)?;
            if n >= group_buf.len() {
                return Err(Unsupported::TooWide);
            }
            group_buf[n] = name;
            n += 1;
            self.ws();
            if self.eat(b',') {
                continue;
            }
            if self.eat(b')') {
                break;
            }
            return Err(Unsupported::Construct);
        }
        Ok((without, n))
    }

    /// `rate(<selector>[<dur>])` or a bare `<selector>`.
    fn selector_or_rate(
        &mut self,
        matcher_buf: &mut [Matcher<'a>],
    ) -> Result<Parsed<'a>, Unsupported> {
        let save = self.i;
        if let Some(id) = self.ident() {
            if id == b"rate" {
                self.ws();
                if !self.eat(b'(') {
                    return Err(Unsupported::Construct);
                }
                self.ws();
                let (metric, n_matchers) = self.selector(matcher_buf)?;
                self.ws();
                // Required range `[<dur>]`.
                if !self.eat(b'[') {
                    return Err(Unsupported::Construct);
                }
                let range_ms = self.duration()?;
                if !self.eat(b']') {
                    return Err(Unsupported::Construct);
                }
                self.ws();
                if !self.eat(b')') {
                    return Err(Unsupported::Construct);
                }
                return Ok(Parsed {
                    metric,
                    n_matchers,
                    temporal: TemporalOp::Rate {
                        range_secs: (range_ms as f64) / 1000.0,
                    },
                    range_ms,
                    combine: None,
                    without: false,
                    n_group: 0,
                });
            }
            if aggregator(id).is_some() {
                // An aggregator where a selector was expected → malformed
                // here (handled one level up); refuse rather than guess.
                return Err(Unsupported::Construct);
            }
        }
        // Bare selector.
        self.i = save;
        let (metric, n_matchers) = self.selector(matcher_buf)?;
        // A range on a bare selector (`metric[5m]`) with no function is a
        // raw range vector — refuse; the API returns instant/stepped values.
        self.ws();
        if self.peek() == Some(b'[') {
            return Err(Unsupported::Construct);
        }
        Ok(Parsed {
            metric,
            n_matchers,
            temporal: TemporalOp::Last,
            range_ms: 0,
            combine: None,
            without: false,
            n_group: 0,
        })
    }

    /// `metric` or `metric{ l op "v", ... }`. The leading metric name is
    /// required; the brace-only `{__name__="metric"}` form is refused.
    fn selector(
        &mut self,
        matcher_buf: &mut [Matcher<'a>],
    ) -> Result<(&'a [u8], usize), Unsupported> {
        let metric = self.ident().ok_or(Unsupported::Construct)?;
        let mut n = 0usize;
        self.ws();
        if self.eat(b'{') {
            loop {
                self.ws();
                if self.eat(b'}') {
                    break;
                }
                let name = self.ident().ok_or(Unsupported::Construct)?;
                self.ws();
                let op = if self.eat(b'=') {
                    if self.eat(b'~') {
                        // Regex matcher — outside v1; refuse by name.
                        return Err(Unsupported::Construct);
                    }
                    MatchOp::Eq
                } else if self.eat(b'!') {
                    if self.eat(b'=') {
                        MatchOp::Ne
                    } else {
                        // `!~` or a lone `!` — refuse.
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
                if self.eat(b',') {
                    continue;
                }
                if self.eat(b'}') {
                    break;
                }
                return Err(Unsupported::Construct);
            }
        }
        Ok((metric, n))
    }

    /// A double-quoted string with no escapes (the common case). An escape
    /// is refused rather than mis-decoded.
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

    /// `<int>(ms|s|m|h)` → milliseconds.
    fn duration(&mut self) -> Result<u64, Unsupported> {
        let start = self.i;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                self.i += 1;
            } else {
                break;
            }
        }
        if self.i == start {
            return Err(Unsupported::Malformed);
        }
        let mut n: u64 = 0;
        for &c in &self.s[start..self.i] {
            n = n.checked_mul(10).ok_or(Unsupported::Malformed)?;
            n = n
                .checked_add((c - b'0') as u64)
                .ok_or(Unsupported::Malformed)?;
        }
        // Unit.
        let mult = if self.looking_at_kw_raw(b"ms") {
            self.i += 2;
            1
        } else if self.eat(b's') {
            1000
        } else if self.eat(b'm') {
            60_000
        } else if self.eat(b'h') {
            3_600_000
        } else {
            return Err(Unsupported::Malformed);
        };
        n.checked_mul(mult).ok_or(Unsupported::Malformed)
    }

    fn looking_at_kw_raw(&self, kw: &[u8]) -> bool {
        let end = self.i + kw.len();
        end <= self.s.len() && &self.s[self.i..end] == kw
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
    c.is_ascii_alphabetic() || c == b'_' || c == b':'
}

fn is_ident_cont(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b':'
}
