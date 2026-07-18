//! Placement recommendation contracts — RFC database foundation §12.4
//! and Phase 10's hot-range policy.
//!
//! §12.4 draws the line this file exists to make executable:
//!
//! > Automatic split and merge recommendations may consider size, key
//! > count, write rate, scan pressure, compaction debt, request
//! > latency, and replica capacity. **Execution remains bounded by
//! > operator policy**: minimum range size, cooldown, maximum
//! > concurrent operations, reserved partition capacity, and
//! > per-tenant limits.
//!
//! So this layer RECOMMENDS and never executes. That is not timidity;
//! it is the §12.4 requirement, and the split/merge machinery it would
//! otherwise drive is the same machinery an operator drives today by
//! declaring an operation in a config. A recommender that could act
//! would turn every policy bug into a topology change.
//!
//! ## The refusal that matters most
//!
//! > Hot-key load cannot always be solved by splitting and should be
//! > reported as such.
//!
//! A range whose load concentrates on ONE key cannot be split into two
//! less-loaded ranges — the hot key lands wholly on one side. Splitting
//! it costs a topology change and buys nothing, so
//! [`recommend`] answers [`Recommendation::HotKeyNotSplittable`]
//! rather than a split nobody should run. Reporting "I cannot help"
//! is a real answer; a split recommendation that will not help is a
//! wrong one.

#![allow(
    dead_code,
    reason = "shared via #[path] into modules and host tests; each consumer uses a subset"
)]

/// Operator policy bounds (§12.4). Every field is a REFUSAL threshold,
/// never a target: the recommender may only say "no" more often as
/// these tighten.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadPolicy {
    /// A range below this key count is never recommended for a split,
    /// however hot: splitting it would produce ranges too small to be
    /// worth the topology change.
    pub min_split_keys: u64,
    /// Two adjacent ranges whose COMBINED key count exceeds this are
    /// never recommended for a merge — the merge would immediately
    /// re-qualify for a split.
    pub max_merge_keys: u64,
    /// Writes per second above which a range is considered hot.
    pub hot_write_rate: u32,
    /// Fraction (percent) of a range's writes landing on its single
    /// busiest key, at or above which the load is a HOT KEY rather
    /// than a hot range. 100 means "only a perfectly concentrated key
    /// counts", which is why the default is well below it.
    pub hot_key_percent: u8,
    /// Operations already in flight; the recommender refuses while any
    /// is, so recommendations cannot pile onto a moving topology.
    pub max_concurrent: u8,
    /// Milliseconds a range must be quiet since its last lifecycle
    /// operation. Cooldown is what stops split/merge oscillation.
    pub cooldown_ms: u64,
    /// Recommend a split on SIZE alone, without waiting for a write
    /// rate. §12.4 lists "size, key count" among the valid inputs, and
    /// a range past its size ceiling is worth splitting whether or not
    /// it is currently busy — a quiet oversized range is still an
    /// oversized range, and it will be busy eventually.
    ///
    /// Kept as a switch rather than assumed because an operator whose
    /// write-rate signal IS wired may prefer to split only on load.
    pub split_on_size: bool,
}

impl LoadPolicy {
    /// Conservative defaults: recommend rarely, refuse readily.
    pub const DEFAULT: LoadPolicy = LoadPolicy {
        min_split_keys: 1024,
        max_merge_keys: 512,
        hot_write_rate: 500,
        hot_key_percent: 50,
        max_concurrent: 1,
        cooldown_ms: 60_000,
        split_on_size: true,
    };
}

/// What one range currently measures. Every field is an OBSERVATION;
/// none is a decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeLoad {
    pub range_index: u8,
    pub key_count: u64,
    pub write_rate: u32,
    /// Percent of this range's writes landing on its busiest single
    /// key. `0` when unmeasured — and unmeasured must not read as
    /// "not hot", so [`recommend`] treats 0 as "no hot-key evidence"
    /// and still refuses on the other grounds.
    pub busiest_key_percent: u8,
    /// Milliseconds since this range's last lifecycle operation.
    pub since_last_op_ms: u64,
}

/// What the recommender is willing to say. Each variant is a complete
/// statement including the ones that decline to act.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recommendation {
    /// Nothing to do; the range is within policy.
    None,
    /// Split this range. The recommender does NOT choose a split key —
    /// that needs the key distribution, which belongs to whoever holds
    /// the data, not to a policy layer.
    Split { range_index: u8 },
    /// Merge this range with its right-hand neighbour.
    Merge { range_index: u8 },
    /// The range is hot, and splitting WILL NOT HELP because the load
    /// is concentrated on one key (§12.4). Reported as its own answer
    /// so an operator is told the truth rather than handed a split.
    HotKeyNotSplittable { range_index: u8, percent: u8 },
    /// Within cooldown since its last operation.
    Cooldown { range_index: u8, remaining_ms: u64 },
    /// An operation is already in flight.
    OperationInFlight,
}

impl Recommendation {
    /// Does this recommendation ask for a topology change? Useful to
    /// assert that the advisory path never grows an execution path by
    /// accident.
    pub const fn is_actionable(&self) -> bool {
        matches!(self, Self::Split { .. } | Self::Merge { .. })
    }
}

/// The §12.4 decision, in the order the clauses must be applied.
///
/// Ordering is the contract, not a style choice:
///
/// 1. **In-flight first.** A recommendation computed against a
///    topology that is already changing is stale before it is read.
/// 2. **Cooldown next.** Even a correct recommendation must wait, or
///    split and merge oscillate around a threshold.
/// 3. **Hot-key before split.** A hot range whose load is one key must
///    be reported as unsplittable; checking splittability afterwards
///    would recommend the split first and explain later.
/// 4. **Split before merge.** A range can be both above the split
///    floor and below the merge ceiling under sloppy policy; growth
///    pressure wins, because merging something that will re-split is
///    strictly wasted work.
pub fn recommend(
    load: &RangeLoad,
    neighbour: Option<&RangeLoad>,
    policy: &LoadPolicy,
    operations_in_flight: u8,
) -> Recommendation {
    if operations_in_flight >= policy.max_concurrent {
        return Recommendation::OperationInFlight;
    }
    if load.since_last_op_ms < policy.cooldown_ms {
        return Recommendation::Cooldown {
            range_index: load.range_index,
            remaining_ms: policy.cooldown_ms - load.since_last_op_ms,
        };
    }

    let hot = load.write_rate >= policy.hot_write_rate;

    // Size OR load, per policy. Requiring BOTH made the split half
    // unreachable wherever a write rate is unmeasured — not a
    // conservative default but a silent one: an oversized range that
    // nothing can measure would never be recommended at all.
    let would_split = load.key_count >= policy.min_split_keys && (hot || policy.split_on_size);

    // The hot-key refusal GUARDS the split rather than sitting beside
    // it. Placing it beside the split made the answer non-monotone:
    // raising `hot_write_rate` (caring LESS about load) flipped a
    // "cannot help" refusal into a size-driven split while the key was
    // still concentrated — which is precisely the recommendation §12.4
    // forbids. The property test caught it; the ordering is the fix.
    if would_split
        && load.busiest_key_percent > 0
        && load.busiest_key_percent >= policy.hot_key_percent
    {
        return Recommendation::HotKeyNotSplittable {
            range_index: load.range_index,
            percent: load.busiest_key_percent,
        };
    }

    if would_split {
        return Recommendation::Split {
            range_index: load.range_index,
        };
    }

    if let Some(n) = neighbour {
        let combined = load.key_count.saturating_add(n.key_count);
        // Both sides must be quiet: merging into a hot neighbour just
        // moves the problem, and the neighbour's own cooldown is its
        // own to respect.
        if combined <= policy.max_merge_keys
            && !hot
            && n.write_rate < policy.hot_write_rate
            && n.since_last_op_ms >= policy.cooldown_ms
        {
            return Recommendation::Merge {
                range_index: load.range_index,
            };
        }
    }

    Recommendation::None
}

// ── Capacity forecasting (Phase 10) ──────────────────────────────────

/// How long until a range reaches a ceiling, or why that question has
/// no answer. Every non-numeric variant is a REFUSAL to forecast, and
/// each is distinct because they demand different responses from an
/// operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Forecast {
    /// No growth signal exists yet (fewer than two observations). NOT
    /// the same as "not growing": nothing has been measured, so
    /// nothing can be projected.
    Unmeasured,
    /// Measured, and flat or shrinking. A linear projection from
    /// zero growth is infinity, and reporting infinity as a lead time
    /// invites reading it as "safe forever" — so it is its own answer.
    NotGrowing,
    /// Already at or past the ceiling. The lead time is not negative,
    /// it is GONE, and that distinction is what an operator acts on.
    AlreadyExceeded,
    /// Seconds until the ceiling at the currently observed rate.
    /// Linear by construction: this projects the trend that was
    /// actually measured and claims nothing about acceleration.
    Seconds(u64),
}

impl Forecast {
    /// Does this forecast justify acting now, given a lead time an
    /// operator needs to schedule the work?
    pub const fn is_urgent(&self, lead_seconds: u64) -> bool {
        match self {
            Self::AlreadyExceeded => true,
            Self::Seconds(s) => *s <= lead_seconds,
            // A refusal to forecast is never urgency. Treating "I
            // cannot tell" as "act now" would make every unmeasured
            // range an emergency.
            Self::Unmeasured | Self::NotGrowing => false,
        }
    }
}

/// Project when `key_count` reaches `ceiling` at `growth_per_sec`.
///
/// `measured` distinguishes "no observations yet" from "observed zero
/// growth" — the caller knows which it has, and collapsing them here
/// would destroy the difference between an unknown and a fact.
pub fn forecast_to_ceiling(
    key_count: u64,
    growth_per_sec: u32,
    ceiling: u64,
    measured: bool,
) -> Forecast {
    if key_count >= ceiling {
        // Checked BEFORE the growth clauses: a range already over the
        // ceiling is over it whether or not anyone has measured a
        // trend, and saying "unmeasured" there would hide a fact
        // behind a missing signal.
        return Forecast::AlreadyExceeded;
    }
    if !measured {
        return Forecast::Unmeasured;
    }
    if growth_per_sec == 0 {
        return Forecast::NotGrowing;
    }
    let remaining = ceiling - key_count;
    Forecast::Seconds(remaining / u64::from(growth_per_sec))
}
