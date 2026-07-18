//! Shared module-scope telemetry emit helper for lattice app modules.
//!
//! Each app module keeps a small set of monotonic counters and emits
//! them on its `metrics` output port at a coarse cadence. Metric ids
//! follow the module manifest's `[observability] metrics` order (id N =
//! the Nth declared name), matching the fluxor foundation-module
//! convention (see `standards/observability.md` §3 and the foundation
//! `ip`/`http` emit blocks).
//!
//! This file is path-mounted (`#[path]`) into each module, which has
//! already `include!`d the SDK `runtime.rs` and `abi` at its crate root;
//! the helper reaches those through `crate::`. It is emit-only, `no_std`,
//! and carries no inline tests.

/// Emit `values[i]` as counter id `i` on `chan`. No-op when `chan < 0`
/// (the metrics port is unwired) or the self-index syscall fails.
/// `values` MUST be in the module manifest's `[observability] metrics`
/// declaration order so ids line up with names at the collector.
#[allow(
    dead_code,
    reason = "emit-side helper mounted into every app module; a module that declares fewer counters than the shared slice simply passes a shorter slice"
)]
pub(crate) unsafe fn emit_counters(sys: &crate::abi::SyscallTable, chan: i32, values: &[u64]) {
    if chan < 0 {
        return;
    }
    let me = crate::dev_self_index(sys);
    if me < 0 {
        return;
    }
    let midx = me as u16;
    let t = crate::dev_micros(sys);
    let counter = crate::abi::contracts::telemetry::METRIC_COUNTER;
    let mut id: u16 = 0;
    while (id as usize) < values.len() {
        crate::dev_telemetry_metric(sys, chan, midx, t, counter, id, values[id as usize]);
        id += 1;
    }
}

/// Emit `values[i]` as UpDownCounter (gauge) id `base + i` on `chan`.
///
/// The companion to [`emit_counters`] for values that are a *current
/// state* rather than a monotonic total — phase discriminants, queue
/// depths, latched errnos. Emitting those as `METRIC_COUNTER` would let
/// a collector interpret a decrease as a counter reset and report a
/// bogus rate; `METRIC_UPDOWN` is the gauge type (same choice fat32
/// makes for its `file_count`).
///
/// `base` is the manifest id of the first value, so a module can emit
/// its counters as ids `0..n` and its gauges as `n..m` in one pass over
/// the single `[observability] metrics` declaration order.
#[allow(
    dead_code,
    reason = "emit-side helper mounted into every app module; modules without gauges never call it"
)]
pub(crate) unsafe fn emit_gauges(
    sys: &crate::abi::SyscallTable,
    chan: i32,
    base: u16,
    values: &[u64],
) {
    if chan < 0 {
        return;
    }
    let me = crate::dev_self_index(sys);
    if me < 0 {
        return;
    }
    let midx = me as u16;
    let t = crate::dev_micros(sys);
    let updown = crate::abi::contracts::telemetry::METRIC_UPDOWN;
    let mut i: u16 = 0;
    while (i as usize) < values.len() {
        crate::dev_telemetry_metric(sys, chan, midx, t, updown, base + i, values[i as usize]);
        i += 1;
    }
}
