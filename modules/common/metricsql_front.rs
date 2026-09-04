//! MetricsQL front-end — VictoriaMetrics' PromQL *superset*. It is the
//! cheapest of the language front-ends and the direct demonstration that a new
//! query language is a parser delta, not a new engine: the shared grammar is
//! handled by the PromQL front-end over the same core, and MetricsQL's
//! extensions are a small normalisation on top.
//!
//! v1 delta: MetricsQL allows a rollup like `rate(m)` **without** an explicit
//! `[range]` (PromQL requires one); the range then defaults. This front-end
//! normalises a bare `rate(<selector>)` to `rate(<selector>[<default>])` and
//! then lowers with the PromQL parser — same `TsQuery`, same evaluator. A
//! PromQL query with an explicit range passes through unchanged.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor module and host tests; each consumer uses a subset of the surface"
)]

#[path = "promql_front.rs"]
pub mod promql;

pub use promql::core;
use promql::core::{Matcher, Unsupported};
use promql::Parsed;

/// MetricsQL default rollup lookback when a range is omitted.
pub const DEFAULT_RANGE: &[u8] = b"5m";

/// Normalise MetricsQL sugar into `scratch`, then lower with the PromQL parser.
/// `scratch` must outlive the returned `Parsed` (it borrows the normalised
/// bytes, as do the matcher/group buffers).
pub fn lower<'a>(
    input: &[u8],
    scratch: &'a mut [u8],
    matcher_buf: &mut [Matcher<'a>],
    group_buf: &mut [&'a [u8]],
) -> Result<Parsed<'a>, Unsupported> {
    let n = normalize(input, scratch)?;
    promql::lower(&scratch[..n], matcher_buf, group_buf)
}

/// Copy `input` to `out`, injecting a default `[range]` into any
/// rollup-shaped call that has no `[` before its closing `)`. Returns the
/// written length. Bounded single pass; refuses on overflow. Of the
/// recognised rollup names only `rate` survives the downstream parser;
/// the others are normalised the same way and then refused there, so a
/// normalised-but-unsupported rollup still fails closed.
fn normalize(input: &[u8], out: &mut [u8]) -> Result<usize, Unsupported> {
    let mut o = 0usize;
    let mut i = 0usize;
    while i < input.len() {
        // Detect a rollup-function call head: `<ident>(` where ident is a
        // known rollup that takes a range vector.
        if let Some((name_end, open)) = rollup_head(input, i) {
            // Copy `<ident>(`.
            let head = &input[i..open + 1];
            push(out, &mut o, head)?;
            i = open + 1;
            // Scan the argument to the matching `)`, tracking `{}`/`[` and
            // whether a range `[` appears at depth 0.
            let arg_start = o;
            let mut depth = 0i32;
            let mut brace = 0i32;
            let mut saw_range = false;
            while i < input.len() {
                let c = input[i];
                match c {
                    b'{' => brace += 1,
                    b'}' => brace -= 1,
                    b'[' if brace == 0 => saw_range = true,
                    b'(' => depth += 1,
                    b')' if depth == 0 => break,
                    b')' => depth -= 1,
                    _ => {}
                }
                push(out, &mut o, &[c])?;
                i += 1;
            }
            let _ = (name_end, arg_start);
            if !saw_range {
                // Inject `[DEFAULT]` before the closing `)`.
                push(out, &mut o, b"[")?;
                push(out, &mut o, DEFAULT_RANGE)?;
                push(out, &mut o, b"]")?;
            }
            // Copy the closing `)` (if present).
            if i < input.len() && input[i] == b')' {
                push(out, &mut o, b")")?;
                i += 1;
            }
            continue;
        }
        push(out, &mut o, &[input[i]])?;
        i += 1;
    }
    Ok(o)
}

/// If a rollup-function call head begins at `i` (`<rollup-ident>(`), return
/// (name_end, open_paren_index).
fn rollup_head(s: &[u8], i: usize) -> Option<(usize, usize)> {
    if !is_ident_start(*s.get(i)?) {
        return None;
    }
    let mut j = i;
    while j < s.len() && is_ident_cont(s[j]) {
        j += 1;
    }
    let name = &s[i..j];
    if !is_rollup(name) {
        return None;
    }
    // Skip whitespace to `(`.
    let mut k = j;
    while k < s.len() && matches!(s[k], b' ' | b'\t') {
        k += 1;
    }
    if s.get(k) == Some(&b'(') {
        Some((j, k))
    } else {
        None
    }
}

fn is_rollup(name: &[u8]) -> bool {
    matches!(
        name,
        b"rate"
            | b"irate"
            | b"increase"
            | b"delta"
            | b"rollup_rate"
            | b"avg_over_time"
            | b"sum_over_time"
    )
}

fn push(out: &mut [u8], o: &mut usize, bytes: &[u8]) -> Result<(), Unsupported> {
    if *o + bytes.len() > out.len() {
        return Err(Unsupported::TooWide);
    }
    out[*o..*o + bytes.len()].copy_from_slice(bytes);
    *o += bytes.len();
    Ok(())
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || c == b':'
}
fn is_ident_cont(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b':'
}
