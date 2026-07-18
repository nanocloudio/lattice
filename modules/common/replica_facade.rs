//! Pure-logic facade for Lattice module state.
//!
//! Pulled into each module via `#[path]` AND into host-side tests
//! (`tests/facade.rs`). Contains no `unsafe`, no syscalls, no I/O.
//! Mirrors `clustor/modules/common/replica_facade.rs` in spirit: the
//! types here are the contract every Lattice module agrees on for the
//! KV / watch / lease state machines, so we can unit-test them on the
//! host toolchain without spawning a fluxor binary.
//!
//! This file is listed in `fluxor.toml [[ci.hygiene.exemption]]` to
//! permit the `#[cfg(test)]` block below.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset of the surface"
)]

#[path = "types.rs"]
mod types;

use types::{
    CorrelationId, KpgId, KvEpoch, Revision, RoutingEpoch, SessionEpoch, SessionId, TenantId,
};

/// Hard cap on a single KV command payload. Mirrors clustor's
/// `MAX_COMMAND_BYTES = 4096` so the WAL entry size budget composes
/// with the substrate. Anchors enforce this *before* routing.
pub const MAX_COMMAND_BYTES: usize = 4096;

/// Hard cap on a single KV response payload (worker → router → anchor).
pub const MAX_RESPONSE_BYTES: usize = 4096;

/// Hard cap on a watch event payload (worker → fanout → anchor).
pub const MAX_WATCH_EVENT_BYTES: usize = 4096;

// ── Routing decision input ─────────────────────────────────────────────

/// Inputs the router needs to classify a request and gate it against
/// CP-Raft freshness / epoch / quota before submitting downstream.
#[derive(Clone, Copy, Debug)]
pub struct RoutingInputs {
    pub tenant: TenantId,
    pub kpg: KpgId,
    pub kv_epoch: KvEpoch,
    pub route_epoch: RoutingEpoch,
    pub linearizable: bool,
    pub cas_required: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutingDecision {
    /// Forward to the local KV worker.
    Local,
    /// Reject with `KV_RESULT_DIRTY_EPOCH`.
    DirtyEpoch,
    /// Reject with `KV_RESULT_LIN_BOUND`.
    LinBound,
    /// Reject with `KV_RESULT_QUOTA`.
    Quota,
    /// Reject with `KV_RESULT_INTERNAL` (router policy says no).
    Internal,
}

/// Pure decision: does the router accept this request now? Inputs are
/// the request's stamped epochs plus the router's current view of
/// freshness and quota state. No I/O.
pub fn route_decision(
    request: RoutingInputs,
    current_kv_epoch: KvEpoch,
    current_route_epoch: RoutingEpoch,
    lin_bound_available: bool,
    quota_available: bool,
) -> RoutingDecision {
    if request.kv_epoch != current_kv_epoch || request.route_epoch != current_route_epoch {
        return RoutingDecision::DirtyEpoch;
    }
    if (request.linearizable || request.cas_required) && !lin_bound_available {
        return RoutingDecision::LinBound;
    }
    if !quota_available {
        return RoutingDecision::Quota;
    }
    RoutingDecision::Local
}

// ── Session state (watch / lease shared shape) ─────────────────────────

#[derive(Clone, Copy, Debug)]
pub struct SessionRecord {
    pub id: SessionId,
    pub epoch: SessionEpoch,
    pub tenant: TenantId,
    pub last_observed_revision: Revision,
}

impl SessionRecord {
    pub const fn new(id: SessionId, tenant: TenantId) -> Self {
        Self {
            id,
            epoch: 0,
            tenant,
            last_observed_revision: 0,
        }
    }

    /// Advance the epoch on rebind. Mirrors the
    /// `control_plane.binding_events` flow: every time a session is
    /// re-attached to a new worker, the epoch bumps and the anchor
    /// must include the new value on subsequent control frames.
    pub fn rebind(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// Compute the replay plan after rebind: send `last_observed_rev + 1`
    /// through `current_committed` to the fanout module. Returns
    /// `None` if no events need replay.
    pub fn replay_plan(&self, current_committed: Revision) -> Option<(Revision, Revision)> {
        if current_committed <= self.last_observed_revision {
            return None;
        }
        Some((
            self.last_observed_revision.saturating_add(1),
            current_committed,
        ))
    }
}

// ── Inflight request matching ──────────────────────────────────────────

/// Per-request metadata held by an edge anchor while a command is
/// in-flight through the router/worker pipeline. Allows correlation of
/// the eventual response back to the originating connection slot and
/// pipeline position. Kept tiny so an anchor's in-flight table can hold
/// thousands of entries without bloating arena usage.
#[derive(Clone, Copy, Debug)]
pub struct InflightEntry {
    pub corr_id: CorrelationId,
    pub conn_id: u8,
    pub pipeline_index: u32,
    pub deadline_ms: u64,
}

impl InflightEntry {
    pub const fn new(
        corr_id: CorrelationId,
        conn_id: u8,
        pipeline_index: u32,
        deadline_ms: u64,
    ) -> Self {
        Self {
            corr_id,
            conn_id,
            pipeline_index,
            deadline_ms,
        }
    }

    /// Has the entry exceeded its deadline at `now_ms`?
    pub fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= self.deadline_ms
    }
}

// Host-side tests for the pure-logic facade. Gated off the no_std
// module build via `#[cfg(test)]`. This file is listed in
// fluxor.toml::ci.hygiene.exemption.
#[cfg(test)]
mod tests {
    use super::*;

    fn req(tenant: TenantId, kpg: KpgId, kv: KvEpoch, route: RoutingEpoch) -> RoutingInputs {
        RoutingInputs {
            tenant,
            kpg,
            kv_epoch: kv,
            route_epoch: route,
            linearizable: false,
            cas_required: false,
        }
    }

    #[test]
    fn route_accepts_matching_epochs() {
        let r = req(7, 0, 1, 100);
        assert_eq!(
            route_decision(r, 1, 100, true, true),
            RoutingDecision::Local
        );
    }

    #[test]
    fn route_rejects_stale_kv_epoch() {
        let r = req(7, 0, 1, 100);
        assert_eq!(
            route_decision(r, 2, 100, true, true),
            RoutingDecision::DirtyEpoch
        );
    }

    #[test]
    fn route_rejects_stale_route_epoch() {
        let r = req(7, 0, 1, 100);
        assert_eq!(
            route_decision(r, 1, 101, true, true),
            RoutingDecision::DirtyEpoch
        );
    }

    #[test]
    fn route_requires_lin_bound_for_linearizable() {
        let mut r = req(7, 0, 1, 100);
        r.linearizable = true;
        assert_eq!(
            route_decision(r, 1, 100, false, true),
            RoutingDecision::LinBound
        );
        assert_eq!(
            route_decision(r, 1, 100, true, true),
            RoutingDecision::Local
        );
    }

    #[test]
    fn route_requires_lin_bound_for_cas() {
        let mut r = req(7, 0, 1, 100);
        r.cas_required = true;
        assert_eq!(
            route_decision(r, 1, 100, false, true),
            RoutingDecision::LinBound
        );
    }

    #[test]
    fn route_rejects_quota_exhausted() {
        let r = req(7, 0, 1, 100);
        assert_eq!(
            route_decision(r, 1, 100, true, false),
            RoutingDecision::Quota
        );
    }

    #[test]
    fn session_rebind_bumps_epoch() {
        let mut s = SessionRecord::new(42, 1);
        assert_eq!(s.epoch, 0);
        s.rebind();
        assert_eq!(s.epoch, 1);
        s.rebind();
        assert_eq!(s.epoch, 2);
    }

    #[test]
    fn session_replay_plan_skips_when_caught_up() {
        let s = SessionRecord {
            id: 1,
            epoch: 0,
            tenant: 0,
            last_observed_revision: 10,
        };
        assert_eq!(s.replay_plan(10), None);
        assert_eq!(s.replay_plan(8), None);
    }

    #[test]
    fn session_replay_plan_spans_gap() {
        let s = SessionRecord {
            id: 1,
            epoch: 0,
            tenant: 0,
            last_observed_revision: 10,
        };
        assert_eq!(s.replay_plan(15), Some((11, 15)));
    }

    #[test]
    fn inflight_entry_expiry() {
        let e = InflightEntry::new(99, 0, 0, 1_000);
        assert!(!e.is_expired(999));
        assert!(e.is_expired(1_000));
        assert!(e.is_expired(1_500));
    }
}
