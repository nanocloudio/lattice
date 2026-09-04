//! Language-agnostic metrics-query core — the one IR and evaluator every
//! metrics dialect (PromQL, MetricsQL, KQL) lowers onto. Pure logic,
//! `no_std`, no allocation: it compiles into the no_std PIC metrics anchor
//! AND is `#[path]`-mounted into host tests, so a regression is caught by
//! `cargo test` before any module ELF runs.
//!
//! ## What lives here
//!
//! A metrics query, whatever the surface language, reduces to three
//! operations and no more:
//!
//!   1. **Series selection** — a metric-name id plus label matchers,
//!      resolving to a bounded set of series. A series' stable identity is
//!      `(metric_id, labels_digest)`; the sample keyspace is addressed by a
//!      derived `series_id`. See `metric_id`, `labels_digest`, `series_id`.
//!   2. **Per-series temporal transform** — fold one series' samples over a
//!      time window (`TemporalOp`): the *within-series* axis. This is what
//!      the existing `TS.AGG` computes.
//!   3. **Cross-series spatial aggregation** — combine across series at each
//!      aligned timestamp, grouped by a label subset (`SpatialOp`): the
//!      *across-series* axis `TS.AGG` cannot express.
//!
//! Keeping the two aggregation axes distinct is the correctness point of the
//! whole surface: `sum(rate(x[5m])) by (job)` is a temporal transform (op 2)
//! *then* a spatial combine (op 3). This module owns that split once, for
//! every language; the front-ends only parse and lower.
//!
//! Everything is bounded. A grid wider than `MAX_POINTS` steps is refused
//! (`Unsupported::TooManyPoints`) rather than truncated or unboundedly
//! materialised. `MAX_SERIES` and `MAX_GROUPS` are the contract ceilings a
//! consumer sizes its buffers against; the shipped anchor enforces its own
//! (smaller) live caps at series-resolution time.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor module and host tests; each consumer uses a subset of the surface"
)]

// ── Bounds ────────────────────────────────────────────────────────────
//
// Fixed caps make evaluation bounded work per call. They are generous for
// dashboard queries and small enough that the worst case is a fixed amount
// of stack. A query that exceeds any of them is refused, not clipped.

/// Maximum series a single query may select.
pub const MAX_SERIES: usize = 256;
/// Maximum aligned points (steps) in a range result, per series.
pub const MAX_POINTS: usize = 1024;
/// Maximum distinct groups a `by`/`without` aggregation may produce.
pub const MAX_GROUPS: usize = 128;
/// Maximum label matchers in one selector.
pub const MAX_MATCHERS: usize = 16;
/// Maximum labels on one series.
pub const MAX_LABELS: usize = 32;

/// Why a query could not be lowered/evaluated. Every front-end maps its own
/// "cannot serve this" to one of these, and the anchor renders it as the
/// language's native error — never a silent wrong answer.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Unsupported {
    /// A construct outside the v1 subset (subquery, `histogram_quantile`,
    /// a general binary-operator tree, a non-metric KQL operator, …).
    Construct,
    /// The selector matched more than `MAX_SERIES` series.
    TooManySeries,
    /// The window spans more than `MAX_POINTS` aligned steps.
    TooManyPoints,
    /// The grouping would produce more than `MAX_GROUPS` groups.
    TooManyGroups,
    /// A selector carried more than `MAX_MATCHERS` matchers, or a series
    /// more than `MAX_LABELS` labels.
    TooWide,
    /// Malformed input (bad label encoding, inverted window, …).
    Malformed,
}

// ── Series identity ───────────────────────────────────────────────────
//
// A series is `metric{l1="v1",l2="v2",…}`. Its identity must be a bounded,
// stable, order-independent function of the metric name and the label set.
// We fold the name to a 32-bit metric id and the (canonically ordered)
// label set to a 128-bit digest; the sample keyspace is addressed by a
// 64-bit `series_id` derived from both. The digest's preimage — the actual
// labels — is stored in the series record so results can render labels and
// matchers can be evaluated against them.

/// 32-bit FNV-1a. Stable across builds and platforms (defined by the
/// algorithm, not the compiler), which is what a persisted identity needs.
pub const fn fnv1a_32(bytes: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash
}

/// 64-bit FNV-1a, for wider identities.
pub const fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        i += 1;
    }
    hash
}

/// The metric-name component of a series identity.
pub fn metric_id(metric: &[u8]) -> u32 {
    fnv1a_32(metric)
}

/// A label pair, borrowed. Names and values are raw bytes; the caller owns
/// storage. Canonical ordering is by name (see `labels_sorted`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Label<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}

/// True if `labels` is sorted by name with no duplicate names — the
/// canonical order a digest is defined over (duplicates are rejected, so
/// values never need comparing). The digest is only stable if callers
/// canonicalise first; this predicate lets a caller assert it.
pub fn labels_sorted(labels: &[Label<'_>]) -> bool {
    let mut i = 1;
    while i < labels.len() {
        let (a, b) = (labels[i - 1], labels[i]);
        let ord = cmp_bytes(a.name, b.name);
        if ord == Ordering::Greater {
            return false;
        }
        if ord == Ordering::Equal {
            // Duplicate label name is malformed, not just unsorted.
            return false;
        }
        i += 1;
    }
    true
}

/// 128-bit digest of a canonically ordered label set. Order-independent
/// identity is the caller's job (sort first); this hashes the exact bytes.
/// Two halves of FNV-1a-64 over distinct salted streams give 128 bits with
/// no external dependency.
pub fn labels_digest(labels: &[Label<'_>]) -> [u8; 16] {
    // Two independently-salted 64-bit accumulators. Each label contributes
    // `name \x00 value \x00` so `a=""` and `="a"` cannot collide.
    let mut lo: u64 = 0xcbf2_9ce4_8422_2325;
    let mut hi: u64 = 0x9e37_79b9_7f4a_7c15;
    for l in labels {
        lo = fold64(lo, l.name);
        lo = fold64(lo, &[0]);
        lo = fold64(lo, l.value);
        lo = fold64(lo, &[0]);
        hi = fold64(hi ^ 0x00ff_00ff_00ff_00ff, l.name);
        hi = fold64(hi, &[1]);
        hi = fold64(hi, l.value);
        hi = fold64(hi, &[1]);
    }
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&lo.to_be_bytes());
    out[8..].copy_from_slice(&hi.to_be_bytes());
    out
}

const fn fold64(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        i += 1;
    }
    hash
}

/// The 64-bit key the sample keyspace is addressed by, derived from the
/// full identity. Deterministic (no registry round-trip): the same
/// metric+labels always yields the same `series_id`.
pub fn series_id(metric_id: u32, digest: &[u8; 16]) -> u64 {
    let mut buf = [0u8; 4 + 16];
    buf[..4].copy_from_slice(&metric_id.to_be_bytes());
    buf[4..].copy_from_slice(digest);
    fnv1a_64(&buf)
}

/// Convenience: identity for a canonically ordered label set. Returns the
/// `(metric_id, digest, series_id)` triple, or `TooWide` if over the label
/// cap, or `Malformed` if the labels are not canonically ordered.
pub fn identify(metric: &[u8], labels: &[Label<'_>]) -> Result<(u32, [u8; 16], u64), Unsupported> {
    if labels.len() > MAX_LABELS {
        return Err(Unsupported::TooWide);
    }
    if labels.len() > 1 && !labels_sorted(labels) {
        return Err(Unsupported::Malformed);
    }
    let mid = metric_id(metric);
    let dig = labels_digest(labels);
    let sid = series_id(mid, &dig);
    Ok((mid, dig, sid))
}

// ── Label matchers (series selection) ─────────────────────────────────

/// A label-matcher operator: literal equality and inequality. Anything a
/// front-end cannot reduce to these (regex matchers included) is refused
/// as `Construct`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MatchOp {
    /// `label = "value"`.
    Eq,
    /// `label != "value"`.
    Ne,
}

/// One matcher: `name <op> value`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Matcher<'a> {
    pub name: &'a [u8],
    pub op: MatchOp,
    pub value: &'a [u8],
}

/// True if `labels` (a series' label set) satisfies `matcher`. A missing
/// label is treated as the empty string, per PromQL semantics: `l=""`
/// matches a series without label `l`, and `l!=""` requires it present.
pub fn matcher_matches(matcher: &Matcher<'_>, labels: &[Label<'_>]) -> bool {
    let mut found: &[u8] = &[];
    for l in labels {
        if bytes_eq(l.name, matcher.name) {
            found = l.value;
            break;
        }
    }
    let eq = bytes_eq(found, matcher.value);
    match matcher.op {
        MatchOp::Eq => eq,
        MatchOp::Ne => !eq,
    }
}

/// True if a series satisfies every matcher in a selector.
pub fn selector_matches(matchers: &[Matcher<'_>], labels: &[Label<'_>]) -> bool {
    for m in matchers {
        if !matcher_matches(m, labels) {
            return false;
        }
    }
    true
}

// ── Aggregation: the two axes ─────────────────────────────────────────

/// The within-series temporal fold applied to one series' samples inside a
/// step window. Mirrors the existing `TS.AGG` reductions plus the
/// instantaneous "last value" a bare selector yields.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TemporalOp {
    /// The most recent sample at or before the step (an instant vector).
    Last,
    /// Number of samples in the window.
    Count,
    /// Sum of sample values in the window.
    Sum,
    /// Minimum sample value in the window.
    Min,
    /// Maximum sample value in the window.
    Max,
    /// Arithmetic mean of sample values in the window.
    Avg,
    /// Per-second average rate of increase over the window, counter-reset
    /// aware: sums positive deltas between consecutive samples and divides
    /// by `range_secs` — the requested range, not the span the samples
    /// happen to cover. `range_secs` must be > 0.
    Rate { range_secs: f64 },
}

/// A single `(timestamp, value)` sample. Timestamps are the store's u64
/// (milliseconds by convention); values are decoded to `f64`.
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Sample {
    pub ts: u64,
    pub value: f64,
}

/// Fold one series' window samples (ascending by ts) with `op`. Returns
/// `None` for an empty window (no point emitted at this step), matching
/// Prometheus' "no data → gap" behaviour.
pub fn temporal_fold(op: TemporalOp, window: &[Sample]) -> Option<f64> {
    if window.is_empty() {
        return None;
    }
    match op {
        TemporalOp::Last => Some(window[window.len() - 1].value),
        TemporalOp::Count => Some(window.len() as f64),
        TemporalOp::Sum => {
            let mut s = 0.0;
            for x in window {
                s += x.value;
            }
            Some(s)
        }
        TemporalOp::Min => {
            let mut m = window[0].value;
            for x in &window[1..] {
                if x.value < m {
                    m = x.value;
                }
            }
            Some(m)
        }
        TemporalOp::Max => {
            let mut m = window[0].value;
            for x in &window[1..] {
                if x.value > m {
                    m = x.value;
                }
            }
            Some(m)
        }
        TemporalOp::Avg => {
            let mut s = 0.0;
            for x in window {
                s += x.value;
            }
            Some(s / window.len() as f64)
        }
        TemporalOp::Rate { range_secs } => {
            if range_secs <= 0.0 || window.len() < 2 {
                return None;
            }
            // Counter-reset aware: a decrease means the counter reset, so
            // the post-reset value IS the increase since the reset point.
            let mut increase = 0.0;
            let mut prev = window[0].value;
            for x in &window[1..] {
                if x.value >= prev {
                    increase += x.value - prev;
                } else {
                    increase += x.value; // reset: count from zero
                }
                prev = x.value;
            }
            Some(increase / range_secs)
        }
    }
}

/// The across-series spatial combine applied to per-series values at one
/// aligned timestamp, within one group.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SpatialOp {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

/// Combine a group's per-series values (all at the same timestamp) with
/// `op`. `values` is never empty (a group exists because a series fell in
/// it).
pub fn spatial_combine(op: SpatialOp, values: &[f64]) -> f64 {
    match op {
        SpatialOp::Count => values.len() as f64,
        SpatialOp::Sum => {
            let mut s = 0.0;
            for v in values {
                s += *v;
            }
            s
        }
        SpatialOp::Avg => {
            let mut s = 0.0;
            for v in values {
                s += *v;
            }
            s / values.len() as f64
        }
        SpatialOp::Min => {
            let mut m = values[0];
            for v in &values[1..] {
                if *v < m {
                    m = *v;
                }
            }
            m
        }
        SpatialOp::Max => {
            let mut m = values[0];
            for v in &values[1..] {
                if *v > m {
                    m = *v;
                }
            }
            m
        }
    }
}

// ── The composed query (the lowering target) ─────────────────────────
//
// A front-end (PromQL/MetricsQL/KQL) parses its dialect and produces a
// `TsQuery`. The anchor executes it by streaming — resolve the selector to
// series, then for each step fold the window (`temporal_fold`) and, if a
// grouping is present, combine across series (`spatial_combine`). The
// struct is bounded (fixed matcher/group caps) so lowering is bounded work,
// and it borrows its bytes from the parsed request — no allocation.

/// An optional cross-series grouping. `group` names the labels the result
/// is keyed by; `without` inverts it (group by all labels *except* these,
/// PromQL `without(...)`). Absent grouping means "no spatial stage": each
/// selected series is returned on its own.
#[derive(Copy, Clone, Debug)]
pub struct Grouping<'a> {
    pub combine: SpatialOp,
    pub group: &'a [&'a [u8]],
    pub without: bool,
}

/// A lowered metrics query, language-independent.
#[derive(Copy, Clone, Debug)]
pub struct TsQuery<'a> {
    /// Metric-name id the selector pins (`__name__`).
    pub metric_id: u32,
    /// Label matchers, all of which a series must satisfy.
    pub matchers: &'a [Matcher<'a>],
    /// Per-series temporal fold applied at each step.
    pub temporal: TemporalOp,
    /// The lookback window each step folds over, in the store's time unit
    /// (ms). For an instant selector this is the step width.
    pub range_ms: u64,
    /// Range grid start/end (inclusive) and step, in ms.
    pub start_ms: u64,
    pub end_ms: u64,
    pub step_ms: u64,
    /// Optional cross-series aggregation. `None` = return each series.
    pub grouping: Option<Grouping<'a>>,
}

impl TsQuery<'_> {
    /// Validate the shape and return the number of steps in the grid, or the
    /// reason it is refused. Bounds the grid to `MAX_POINTS` and the
    /// selector to `MAX_MATCHERS` — a query outside those is refused, never
    /// clipped.
    pub fn step_count(&self) -> Result<usize, Unsupported> {
        if self.matchers.len() > MAX_MATCHERS {
            return Err(Unsupported::TooWide);
        }
        if self.step_ms == 0 || self.end_ms < self.start_ms {
            return Err(Unsupported::Malformed);
        }
        if let Some(g) = self.grouping {
            if g.group.len() > MAX_LABELS {
                return Err(Unsupported::TooWide);
            }
        }
        // Inclusive grid: start, start+step, …, ≤ end.
        let span = self.end_ms - self.start_ms;
        let steps = (span / self.step_ms) as usize + 1;
        if steps > MAX_POINTS {
            return Err(Unsupported::TooManyPoints);
        }
        Ok(steps)
    }

    /// Timestamp of grid step `i` (0-based). Caller keeps `i < step_count`.
    pub fn step_ts(&self, i: usize) -> u64 {
        self.start_ms + (i as u64) * self.step_ms
    }
}

/// The sub-slice of `samples` (ascending by ts) that falls in the half-open
/// lookback window `(step_ts - range_ms, step_ts]` — the samples a step
/// folds. Bounded linear scan; returns an empty slice for a gap.
pub fn window_slice(samples: &[Sample], step_ts: u64, range_ms: u64) -> &[Sample] {
    let lo = step_ts.saturating_sub(range_ms);
    // First index with ts > lo.
    let mut start = 0;
    while start < samples.len() && samples[start].ts <= lo {
        start += 1;
    }
    // First index with ts > step_ts.
    let mut end = start;
    while end < samples.len() && samples[end].ts <= step_ts {
        end += 1;
    }
    &samples[start..end]
}

/// Whether two label sets belong to the same output group under `grouping`.
/// Two series group together iff they agree on every grouping label (for
/// `by`) or on every label outside the grouping set (for `without`). This
/// is the key comparison the anchor uses to fold series into result groups.
pub fn same_group(grouping: &Grouping<'_>, a: &[Label<'_>], b: &[Label<'_>]) -> bool {
    if grouping.without {
        // Agree on all labels NOT named in `group`.
        label_sets_agree_excluding(a, b, grouping.group)
    } else {
        // Agree on every named label.
        for name in grouping.group {
            if !label_value(a, name)
                .map(|va| Some(va) == label_value(b, name))
                .unwrap_or_else(|| label_value(b, name).is_none())
            {
                return false;
            }
        }
        true
    }
}

fn label_value<'a>(labels: &'a [Label<'a>], name: &[u8]) -> Option<&'a [u8]> {
    for l in labels {
        if bytes_eq(l.name, name) {
            return Some(l.value);
        }
    }
    None
}

fn is_named(name: &[u8], set: &[&[u8]]) -> bool {
    for n in set {
        if bytes_eq(n, name) {
            return true;
        }
    }
    false
}

/// True if `a` and `b` agree on every label whose name is NOT in `exclude`.
fn label_sets_agree_excluding(a: &[Label<'_>], b: &[Label<'_>], exclude: &[&[u8]]) -> bool {
    // Every non-excluded label of `a` must be present with the same value in
    // `b`, and vice versa.
    for l in a {
        if is_named(l.name, exclude) {
            continue;
        }
        if label_value(b, l.name) != Some(l.value) {
            return false;
        }
    }
    for l in b {
        if is_named(l.name, exclude) {
            continue;
        }
        if label_value(a, l.name) != Some(l.value) {
            return false;
        }
    }
    true
}

// ── Byte helpers (no_std, no alloc) ───────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq)]
enum Ordering {
    Less,
    Equal,
    Greater,
}

fn cmp_bytes(a: &[u8], b: &[u8]) -> Ordering {
    let n = if a.len() < b.len() { a.len() } else { b.len() };
    let mut i = 0;
    while i < n {
        if a[i] < b[i] {
            return Ordering::Less;
        }
        if a[i] > b[i] {
            return Ordering::Greater;
        }
        i += 1;
    }
    if a.len() < b.len() {
        Ordering::Less
    } else if a.len() > b.len() {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

fn bytes_eq(a: &[u8], b: &[u8]) -> bool {
    matches!(cmp_bytes(a, b), Ordering::Equal)
}
