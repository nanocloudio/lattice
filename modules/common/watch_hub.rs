//! Pure-logic watch hub for Lattice.
//!
//! Mirrors `modules/common/kv_store.rs` / `etcd_codec.rs`: the same
//! source compiles into the no_std PIC `watch_registry` /
//! `watch_fanout` modules AND is `#[path]`-mounted into host-side
//! tests under `tests/integration_watch.rs`. A regression in the
//! state machine — replay-fence math, key-prefix matching,
//! filter-evaluation — is caught by `cargo test` before any module
//! ELF runs.
//!
//! ## Roles
//!
//! - **`watch_registry`** owns persisted state: which `WatchID` is
//!   active, the per-watch key range / filter / start-revision /
//!   `last_sent_revision`, the binding to a client-facing
//!   `etcd_edge_anchor` slot, and the `session_epoch` that fences
//!   stale frames after a rebind.
//! - **`watch_fanout`** consumes durable mutation events from
//!   `kv_state_worker`, walks the watch table, picks matching
//!   watches, batches per-anchor frames, and hands them off to
//!   `etcd_edge_anchor` for the wire.
//!
//! Both modules use this facade. Registry calls are mutating;
//! fanout calls are mostly read-only (it consults watch records to
//! decide what to ship and reports back via `mark_delivered`).
//!
//! ## What this facade does NOT own
//!
//! - HPACK / gRPC framing — that lives in `etcd_codec.rs`.
//! - Event buffering — events flow through immediately; if a slow
//!   consumer needs replay, the registry consults the
//!   `kv_state_worker` snapshot via `replay_plan` (which today is a
//!   thin slot for what becomes a snapshot-restore in Phase 6).
//! - Lease attachment — that lives in `lease_manager` and only
//!   surfaces here through the `LeaseId` carried on a `KeyValue`.
//!
//! Heap-free, `no_std`-compatible, dual-target.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset of the surface so single-module rustc invocations see unused items"
)]

use core::cmp::Ordering;

// ── Capacities ────────────────────────────────────────────────────────

/// Active watches across all anchors. Sized for the bare-metal Pi 5
/// target: 256 × ~96 B = ~24 KiB of state. The registry imposes its
/// own per-tenant cap on top of this.
pub const MAX_WATCHES: usize = 256;

/// Max key bytes in a watch's range. etcd's `range_end` lets a
/// watch span a key range; Lattice's subset only supports
/// single-key watches plus the simple `b"x"..b"y"` prefix form
/// (where `range_end` is the next byte after the last byte of `key`).
pub const WATCH_KEY_MAX: usize = 96;

/// Reserved sentinel for "no anchor binding" (e.g. a watch whose
/// anchor connection dropped — kept alive until session timeout
/// for resume).
pub const ANCHOR_UNBOUND: u32 = 0xFFFF_FFFF;

// ── Event kinds (mirrors etcd v3 mvccpb.Event.EventType) ──────────────

pub const WATCH_EVENT_PUT: u8 = 0;
pub const WATCH_EVENT_DELETE: u8 = 1;

// ── Filter bits (etcd's WatchCreateRequest.filters) ───────────────────

/// Suppress PUT events (`NOPUT` filter).
pub const WATCH_FILTER_NOPUT: u8 = 1 << 0;
/// Suppress DELETE events (`NODELETE` filter).
pub const WATCH_FILTER_NODELETE: u8 = 1 << 1;

// ── Anchor binding ────────────────────────────────────────────────────

/// `(conn_id, stream_id)` packed into a single u32 so the watch
/// table doesn't need a separate field per axis. `conn_id` is u8
/// (matches `types::ConnId`); `stream_id` is u24 (HTTP/2 stream
/// ids are u31 on the wire but Lattice caps practically at ~16
/// concurrent streams per conn, so 24 bits is generous).
pub const fn pack_binding(conn_id: u8, stream_id: u32) -> u32 {
    ((conn_id as u32) << 24) | (stream_id & 0x00FF_FFFF)
}

pub const fn binding_conn_id(b: u32) -> u8 {
    (b >> 24) as u8
}

pub const fn binding_stream_id(b: u32) -> u32 {
    b & 0x00FF_FFFF
}

// ── WatchRecord ───────────────────────────────────────────────────────

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WatchRecord {
    /// Stable per-watch identifier issued by the registry. 0 marks
    /// the slot as free.
    pub watch_id: u64,
    /// Tenant the watch belongs to. Cross-tenant fan-out is
    /// rejected at admission.
    pub tenant_id: u32,
    /// Start revision (exclusive — events with rev > start_rev are
    /// delivered).
    pub start_revision: i64,
    /// Highest revision delivered to this watch's client. Bumped
    /// only after the fanout confirms the frame is on the wire.
    pub last_sent_revision: i64,
    /// Generation counter — bumped on resume / rebind. Stale
    /// frames carrying an older epoch are dropped.
    pub session_epoch: u32,
    /// `pack_binding(conn_id, stream_id)`. `ANCHOR_UNBOUND` when
    /// the anchor connection has dropped and we're holding the
    /// state for a resume.
    pub anchor_binding: u32,
    /// `(NOPUT | NODELETE)` bitset.
    pub filters: u8,
    /// True iff the watcher asked for progress notifications.
    pub progress_notify: bool,
    /// Length of `key` and `range_end` in `key_buf`. The key
    /// occupies `key_buf[..key_len]`; `range_end` occupies
    /// `key_buf[key_len..key_len + range_end_len]`. `range_end_len`
    /// of 0 means a single-key watch.
    pub key_len: u16,
    pub range_end_len: u16,
    /// Single-key (`key_len`) or prefix range (`[key, range_end)`).
    /// Capped at `WATCH_KEY_MAX` bytes total across both.
    pub key_buf: [u8; WATCH_KEY_MAX],
}

impl WatchRecord {
    pub const fn free() -> Self {
        Self {
            watch_id: 0,
            tenant_id: 0,
            start_revision: 0,
            last_sent_revision: 0,
            session_epoch: 0,
            anchor_binding: ANCHOR_UNBOUND,
            filters: 0,
            progress_notify: false,
            key_len: 0,
            range_end_len: 0,
            key_buf: [0; WATCH_KEY_MAX],
        }
    }

    /// True iff this slot is in use.
    pub fn in_use(&self) -> bool {
        self.watch_id != 0
    }

    /// Borrowed key bytes (single-key form) or `[key, range_end)`
    /// prefix when `range_end_len > 0`.
    pub fn key(&self) -> &[u8] {
        let len = self.key_len as usize;
        self.key_buf.get(..len).unwrap_or(&[])
    }

    pub fn range_end(&self) -> &[u8] {
        let start = self.key_len as usize;
        let end = start + self.range_end_len as usize;
        self.key_buf.get(start..end).unwrap_or(&[])
    }

    /// True iff this watch should receive an event for `event_key`.
    /// Single-key watches match exact bytes; range watches match
    /// every key with `key <= k < range_end`.
    pub fn matches_key(&self, event_key: &[u8]) -> bool {
        let key = self.key();
        if self.range_end_len == 0 {
            return key == event_key;
        }
        let range_end = self.range_end();
        // event_key in [key, range_end) — lexicographic.
        if event_key < key {
            return false;
        }
        event_key < range_end
    }

    /// True iff `event_kind` (PUT/DELETE) passes this watch's
    /// filter set.
    pub fn passes_filter(&self, event_kind: u8) -> bool {
        match event_kind {
            WATCH_EVENT_PUT => self.filters & WATCH_FILTER_NOPUT == 0,
            WATCH_EVENT_DELETE => self.filters & WATCH_FILTER_NODELETE == 0,
            _ => false,
        }
    }
}

// ── WatchHub ──────────────────────────────────────────────────────────

/// Errors that can bubble up to the anchor as a watch-create / watch-
/// resume failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchError {
    /// Caller tried to register a watch with id 0 (the free-slot
    /// sentinel) or one that's already in use.
    InvalidId,
    /// All slots are taken — caller should backpressure.
    TableFull,
    /// Key or range_end exceeds `WATCH_KEY_MAX`.
    KeyTooLarge,
    /// Watch id is not registered (cancel / resume / mark_delivered).
    NotFound,
    /// Caller tried to apply a filter byte with unknown bits set.
    BadFilter,
}

/// Fixed-capacity table of `WatchRecord` indexed by `watch_id`
/// (linear scan). Capacity `MAX_WATCHES` is enough for hundreds of
/// concurrent etcd Watch streams; finer-grained tenancy is enforced
/// at the registry's admission layer (`max_watches_per_tenant`).
#[repr(C)]
pub struct WatchHub {
    pub records: [WatchRecord; MAX_WATCHES],
    /// Monotonic per-hub revision the registry stamps on every
    /// successful state mutation. Used to detect rebind churn.
    pub registry_revision: u64,
    /// Next `watch_id` to issue when the caller asks for an
    /// auto-assigned id. Starts at 1; never returns 0 (the free
    /// sentinel).
    pub next_id: u64,
    /// Cluster-wide placement epoch as last reported by the substrate
    /// `control_plane`. Each newly-registered watch stamps this
    /// into its `session_epoch`; an `advance_cluster_epoch` bump
    /// fences every active watch by promoting their epoch in lockstep.
    /// Initial value 1 covers the pre-substrate single-node case
    /// (until the first `MSG_PLACEMENT_EPOCH_EVENT` lands).
    pub cluster_epoch: u32,
}

impl WatchHub {
    pub const fn new() -> Self {
        Self {
            records: [WatchRecord::free(); MAX_WATCHES],
            registry_revision: 0,
            next_id: 1,
            cluster_epoch: 1,
        }
    }

    pub fn init(&mut self) {
        let mut i = 0;
        while i < MAX_WATCHES {
            self.records[i] = WatchRecord::free();
            i += 1;
        }
        self.registry_revision = 0;
        self.next_id = 1;
        self.cluster_epoch = 1;
    }

    /// Advance the hub's notion of cluster epoch and bump every active
    /// watch's `session_epoch` to the new value. Returns the count of
    /// active watches fenced. No-op when `new_epoch <= cluster_epoch`
    /// (epochs are monotonic — out-of-order or duplicate events get
    /// ignored).
    pub fn advance_cluster_epoch(&mut self, new_epoch: u32) -> usize {
        if new_epoch <= self.cluster_epoch {
            return 0;
        }
        self.cluster_epoch = new_epoch;
        let mut fenced = 0usize;
        let mut i = 0;
        while i < MAX_WATCHES {
            if self.records[i].in_use() {
                self.records[i].session_epoch = new_epoch;
                fenced += 1;
            }
            i += 1;
        }
        if fenced > 0 {
            self.registry_revision = self.registry_revision.wrapping_add(1);
        }
        fenced
    }

    /// Number of watches currently active.
    pub fn len(&self) -> usize {
        self.records.iter().filter(|r| r.in_use()).count()
    }

    pub fn is_empty(&self) -> bool {
        self.records.iter().all(|r| !r.in_use())
    }

    /// Lookup by `watch_id`. None when not found.
    pub fn get(&self, watch_id: u64) -> Option<&WatchRecord> {
        if watch_id == 0 {
            return None;
        }
        self.records.iter().find(|r| r.watch_id == watch_id)
    }

    pub fn get_mut(&mut self, watch_id: u64) -> Option<&mut WatchRecord> {
        if watch_id == 0 {
            return None;
        }
        self.records.iter_mut().find(|r| r.watch_id == watch_id)
    }

    /// Issue the next free `watch_id`. Always non-zero. Wraps if
    /// counter ever overflows (~2^64 distinct watches in this
    /// hub's lifetime — practically never).
    pub fn issue_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        if self.next_id == 0 {
            self.next_id = 1;
        }
        id
    }

    /// Find the free slot index, if any.
    fn find_free(&self) -> Option<usize> {
        self.records.iter().position(|r| !r.in_use())
    }

    /// Find the slot index for `watch_id`, if registered.
    fn index_of(&self, watch_id: u64) -> Option<usize> {
        if watch_id == 0 {
            return None;
        }
        self.records.iter().position(|r| r.watch_id == watch_id)
    }

    /// Register a new watch. `watch_id` of 0 asks the hub to issue
    /// one (returned via `Ok(id)`). Non-zero `watch_id` is honoured
    /// if the slot is free — useful for the resume path where the
    /// caller already knows the id from a prior `(watch_id,
    /// session_epoch)` pair.
    ///
    /// `tenant_id`, `start_revision`, `filters`, `progress_notify`,
    /// and `anchor_binding` populate the fresh record.
    /// `last_sent_revision` is initialised to `start_revision`.
    /// `session_epoch` is stamped with the hub's current
    /// `cluster_epoch` (set by [`advance_cluster_epoch`] when a
    /// `control_plane` event lands; default 1 pre-substrate).
    #[expect(
        clippy::too_many_arguments,
        reason = "facade is intentionally explicit — each field has independent semantics, packing into a struct would obscure the wire contract"
    )]
    pub fn register(
        &mut self,
        watch_id: u64,
        tenant_id: u32,
        key: &[u8],
        range_end: &[u8],
        start_revision: i64,
        filters: u8,
        progress_notify: bool,
        anchor_binding: u32,
    ) -> Result<u64, WatchError> {
        if filters & !(WATCH_FILTER_NOPUT | WATCH_FILTER_NODELETE) != 0 {
            return Err(WatchError::BadFilter);
        }
        if key.len() + range_end.len() > WATCH_KEY_MAX {
            return Err(WatchError::KeyTooLarge);
        }
        if key.len() > u16::MAX as usize || range_end.len() > u16::MAX as usize {
            return Err(WatchError::KeyTooLarge);
        }
        let id = if watch_id == 0 {
            self.issue_id()
        } else {
            if self.index_of(watch_id).is_some() {
                return Err(WatchError::InvalidId);
            }
            watch_id
        };
        let idx = self.find_free().ok_or(WatchError::TableFull)?;
        let rec = &mut self.records[idx];
        *rec = WatchRecord::free();
        rec.watch_id = id;
        rec.tenant_id = tenant_id;
        rec.start_revision = start_revision;
        rec.last_sent_revision = start_revision;
        // Stamp the current substrate-provided placement epoch so the
        // ack carries the right value without a follow-up lookup.
        rec.session_epoch = self.cluster_epoch;
        rec.anchor_binding = anchor_binding;
        rec.filters = filters;
        rec.progress_notify = progress_notify;
        rec.key_len = key.len() as u16;
        rec.range_end_len = range_end.len() as u16;
        rec.key_buf[..key.len()].copy_from_slice(key);
        rec.key_buf[key.len()..key.len() + range_end.len()].copy_from_slice(range_end);
        self.registry_revision = self.registry_revision.wrapping_add(1);
        Ok(id)
    }

    /// Cancel a watch. Returns the record's `last_sent_revision` so
    /// the registry can include it in the cancel-ack envelope. The
    /// slot is freed.
    pub fn cancel(&mut self, watch_id: u64) -> Result<i64, WatchError> {
        let idx = self.index_of(watch_id).ok_or(WatchError::NotFound)?;
        let last = self.records[idx].last_sent_revision;
        self.records[idx] = WatchRecord::free();
        self.registry_revision = self.registry_revision.wrapping_add(1);
        Ok(last)
    }

    /// Rebind a watch's `anchor_binding` (e.g. after a reconnect on
    /// the same client). Bumps `session_epoch` so any in-flight
    /// frames using the old binding get dropped at the anchor.
    /// Returns the new epoch.
    pub fn rebind(&mut self, watch_id: u64, new_binding: u32) -> Result<u32, WatchError> {
        let idx = self.index_of(watch_id).ok_or(WatchError::NotFound)?;
        let rec = &mut self.records[idx];
        rec.session_epoch = rec.session_epoch.wrapping_add(1);
        if rec.session_epoch == 0 {
            rec.session_epoch = 1;
        }
        rec.anchor_binding = new_binding;
        self.registry_revision = self.registry_revision.wrapping_add(1);
        Ok(rec.session_epoch)
    }

    /// Unbind without cancelling — the anchor connection dropped
    /// but the client may resume later. Slot stays allocated.
    pub fn unbind(&mut self, watch_id: u64) -> Result<(), WatchError> {
        let idx = self.index_of(watch_id).ok_or(WatchError::NotFound)?;
        self.records[idx].anchor_binding = ANCHOR_UNBOUND;
        self.registry_revision = self.registry_revision.wrapping_add(1);
        Ok(())
    }

    /// Bump `last_sent_revision` once the fanout confirms `rev` has
    /// been written to the wire for `watch_id`. The registry uses
    /// the new value as the resume point if the connection drops.
    pub fn mark_delivered(&mut self, watch_id: u64, rev: i64) -> Result<(), WatchError> {
        let idx = self.index_of(watch_id).ok_or(WatchError::NotFound)?;
        let rec = &mut self.records[idx];
        if rev > rec.last_sent_revision {
            rec.last_sent_revision = rev;
            self.registry_revision = self.registry_revision.wrapping_add(1);
        }
        Ok(())
    }

    /// Compute the lowest `last_sent_revision` across all active
    /// watches — `compaction_coordinator` consults this to pick a
    /// safe retention floor (compacting below it would orphan
    /// replay). Returns `None` if the hub is empty.
    pub fn retention_floor(&self) -> Option<i64> {
        self.records
            .iter()
            .filter(|r| r.in_use())
            .map(|r| r.last_sent_revision)
            .min()
    }
}

impl Default for WatchHub {
    fn default() -> Self {
        Self::new()
    }
}

// ── Event matcher ─────────────────────────────────────────────────────

/// One matched watch — returned by `match_event` so the caller can
/// shape per-anchor frames without copying out the whole record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MatchedWatch {
    pub watch_id: u64,
    pub session_epoch: u32,
    pub anchor_binding: u32,
}

/// Iterate every active watch and call `sink` for those whose
/// `(key_range × filter × start_revision)` admits this event.
/// `event_revision` is the worker-side commit revision; we drop
/// the event for any watch where `event_revision <=
/// start_revision`.
pub fn match_event<F: FnMut(MatchedWatch)>(
    hub: &WatchHub,
    tenant_id: u32,
    event_revision: i64,
    event_kind: u8,
    event_key: &[u8],
    mut sink: F,
) {
    for rec in hub.records.iter() {
        if !rec.in_use() {
            continue;
        }
        if rec.tenant_id != tenant_id {
            continue;
        }
        if event_revision <= rec.start_revision {
            continue;
        }
        if !rec.passes_filter(event_kind) {
            continue;
        }
        if !rec.matches_key(event_key) {
            continue;
        }
        sink(MatchedWatch {
            watch_id: rec.watch_id,
            session_epoch: rec.session_epoch,
            anchor_binding: rec.anchor_binding,
        });
    }
}

// ── Replay planning ───────────────────────────────────────────────────

/// Plan returned to `watch_fanout` after a `resume` — describes the
/// revision interval the fanout needs to replay from the KV worker's
/// snapshot before resuming live tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayPlan {
    pub watch_id: u64,
    pub from_revision: i64,
    pub to_revision: i64,
    pub session_epoch: u32,
}

/// Build a replay plan after a `resume`. `current_revision` is the
/// worker's latest committed revision. If the watch is already
/// up-to-date (`last_sent_revision == current_revision`) the plan
/// is `Ordering::Equal`-edge — the caller can skip the replay step
/// and tail live.
pub fn replay_plan(hub: &WatchHub, watch_id: u64, current_revision: i64) -> Option<ReplayPlan> {
    let rec = hub.get(watch_id)?;
    let from = rec.last_sent_revision;
    let to = current_revision;
    match from.cmp(&to) {
        Ordering::Less => Some(ReplayPlan {
            watch_id,
            from_revision: from,
            to_revision: to,
            session_epoch: rec.session_epoch,
        }),
        Ordering::Equal | Ordering::Greater => Some(ReplayPlan {
            watch_id,
            from_revision: to,
            to_revision: to,
            session_epoch: rec.session_epoch,
        }),
    }
}

// ── Host-side unit tests (gated off the no_std module build) ──────────

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> WatchHub {
        let mut h = WatchHub::new();
        h.init();
        h
    }

    #[test]
    fn empty_hub_has_no_records() {
        let h = fresh();
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
        assert_eq!(h.retention_floor(), None);
    }

    #[test]
    fn register_single_key_then_match() {
        let mut h = fresh();
        let id = h
            .register(0, 0, b"foo", b"", 0, 0, false, pack_binding(1, 3))
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(h.len(), 1);

        let mut hits = std::vec::Vec::new();
        match_event(&h, 0, 7, WATCH_EVENT_PUT, b"foo", |m| hits.push(m));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].watch_id, id);
        assert_eq!(hits[0].session_epoch, 1);
        assert_eq!(hits[0].anchor_binding, pack_binding(1, 3));

        let mut other = std::vec::Vec::new();
        match_event(&h, 0, 7, WATCH_EVENT_PUT, b"bar", |m| other.push(m));
        assert!(other.is_empty());
    }

    #[test]
    fn register_prefix_range_matches_in_range() {
        let mut h = fresh();
        // [b"a", b"f") — matches "a", "b", … "e"
        h.register(0, 0, b"a", b"f", 0, 0, false, pack_binding(1, 1))
            .unwrap();
        let mut hits = std::vec::Vec::new();
        for k in [&b"a"[..], b"d", b"e", b"f", b"foo"] {
            match_event(&h, 0, 1, WATCH_EVENT_PUT, k, |_| hits.push(k));
        }
        assert_eq!(hits, std::vec![&b"a"[..], b"d", b"e"]);
    }

    #[test]
    fn start_revision_gates_old_events() {
        let mut h = fresh();
        let id = h
            .register(0, 0, b"k", b"", 5, 0, false, pack_binding(1, 1))
            .unwrap();
        let mut hits = std::vec::Vec::new();
        match_event(&h, 0, 5, WATCH_EVENT_PUT, b"k", |m| hits.push(m));
        match_event(&h, 0, 6, WATCH_EVENT_PUT, b"k", |m| hits.push(m));
        assert_eq!(hits.len(), 1, "rev==start filtered, rev>start delivered");
        assert_eq!(hits[0].watch_id, id);
    }

    #[test]
    fn filters_suppress_specific_event_kinds() {
        let mut h = fresh();
        h.register(0, 0, b"k", b"", 0, WATCH_FILTER_NOPUT, false, 0)
            .unwrap();
        let mut hits = std::vec::Vec::new();
        match_event(&h, 0, 1, WATCH_EVENT_PUT, b"k", |m| hits.push(m));
        match_event(&h, 0, 2, WATCH_EVENT_DELETE, b"k", |m| hits.push(m));
        assert_eq!(hits.len(), 1, "PUT suppressed, DELETE delivered");
    }

    #[test]
    fn rebind_bumps_session_epoch() {
        let mut h = fresh();
        let id = h
            .register(0, 0, b"k", b"", 0, 0, false, pack_binding(1, 3))
            .unwrap();
        assert_eq!(h.get(id).unwrap().session_epoch, 1);
        let new_epoch = h.rebind(id, pack_binding(2, 5)).unwrap();
        assert_eq!(new_epoch, 2);
        let rec = h.get(id).unwrap();
        assert_eq!(rec.anchor_binding, pack_binding(2, 5));
    }

    #[test]
    fn unbind_keeps_record_alive_for_resume() {
        let mut h = fresh();
        let id = h
            .register(0, 0, b"k", b"", 0, 0, false, pack_binding(1, 3))
            .unwrap();
        h.unbind(id).unwrap();
        let rec = h.get(id).unwrap();
        assert_eq!(rec.anchor_binding, ANCHOR_UNBOUND);
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn mark_delivered_only_advances() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, 0).unwrap();
        h.mark_delivered(id, 5).unwrap();
        assert_eq!(h.get(id).unwrap().last_sent_revision, 5);
        h.mark_delivered(id, 3).unwrap(); // older — must NOT regress
        assert_eq!(h.get(id).unwrap().last_sent_revision, 5);
        h.mark_delivered(id, 7).unwrap();
        assert_eq!(h.get(id).unwrap().last_sent_revision, 7);
    }

    #[test]
    fn cancel_frees_slot_and_returns_last_sent() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, 0).unwrap();
        h.mark_delivered(id, 42).unwrap();
        let last = h.cancel(id).unwrap();
        assert_eq!(last, 42);
        assert_eq!(h.len(), 0);
        assert!(h.get(id).is_none());
    }

    #[test]
    fn retention_floor_is_min_last_sent() {
        let mut h = fresh();
        let a = h.register(0, 0, b"a", b"", 0, 0, false, 0).unwrap();
        let b = h.register(0, 0, b"b", b"", 0, 0, false, 0).unwrap();
        h.mark_delivered(a, 10).unwrap();
        h.mark_delivered(b, 7).unwrap();
        assert_eq!(h.retention_floor(), Some(7));
    }

    #[test]
    fn replay_plan_describes_resume_interval() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, 0).unwrap();
        h.mark_delivered(id, 3).unwrap();
        let plan = replay_plan(&h, id, 10).unwrap();
        assert_eq!(plan.from_revision, 3);
        assert_eq!(plan.to_revision, 10);
        assert_eq!(plan.watch_id, id);
    }

    #[test]
    fn replay_plan_for_up_to_date_watch_is_noop() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, 0).unwrap();
        h.mark_delivered(id, 5).unwrap();
        let plan = replay_plan(&h, id, 5).unwrap();
        assert_eq!(plan.from_revision, plan.to_revision);
    }

    #[test]
    fn register_rejects_bad_filter_bits() {
        let mut h = fresh();
        let err = h.register(0, 0, b"k", b"", 0, 0xFF, false, 0).unwrap_err();
        assert_eq!(err, WatchError::BadFilter);
    }

    #[test]
    fn register_rejects_oversize_key() {
        let mut h = fresh();
        let big = std::vec::Vec::from([b'k'; WATCH_KEY_MAX + 1]);
        let err = h.register(0, 0, &big, b"", 0, 0, false, 0).unwrap_err();
        assert_eq!(err, WatchError::KeyTooLarge);
    }

    #[test]
    fn cross_tenant_does_not_leak_events() {
        let mut h = fresh();
        h.register(0, 0, b"k", b"", 0, 0, false, pack_binding(1, 0))
            .unwrap();
        h.register(0, 1, b"k", b"", 0, 0, false, pack_binding(2, 0))
            .unwrap();
        let mut for_t0 = std::vec::Vec::new();
        let mut for_t1 = std::vec::Vec::new();
        match_event(&h, 0, 1, WATCH_EVENT_PUT, b"k", |m| for_t0.push(m));
        match_event(&h, 1, 1, WATCH_EVENT_PUT, b"k", |m| for_t1.push(m));
        assert_eq!(for_t0.len(), 1);
        assert_eq!(for_t1.len(), 1);
        assert_ne!(
            for_t0[0].anchor_binding, for_t1[0].anchor_binding,
            "events MUST stay tenant-scoped",
        );
    }

    #[test]
    fn table_full_returns_err() {
        let mut h = fresh();
        let mut i = 0;
        while i < MAX_WATCHES {
            h.register(0, 0, b"k", b"", 0, 0, false, 0).unwrap();
            i += 1;
        }
        let err = h.register(0, 0, b"k", b"", 0, 0, false, 0).unwrap_err();
        assert_eq!(err, WatchError::TableFull);
    }

    #[test]
    fn binding_pack_unpack_round_trip() {
        let cases = [(0u8, 0u32), (1, 1), (255, 0x00FF_FFFF), (42, 12345)];
        for (c, s) in cases {
            let packed = pack_binding(c, s);
            assert_eq!(binding_conn_id(packed), c);
            assert_eq!(binding_stream_id(packed), s);
        }
    }

    #[test]
    fn cluster_epoch_advance_bumps_all_active_sessions() {
        let mut h = fresh();
        let a = h.register(0, 0, b"k1", b"", 0, 0, false, 0).unwrap();
        let b = h.register(0, 0, b"k2", b"", 0, 0, false, 0).unwrap();
        assert_eq!(h.get(a).unwrap().session_epoch, 1);
        assert_eq!(h.get(b).unwrap().session_epoch, 1);

        let fenced = h.advance_cluster_epoch(5);
        assert_eq!(fenced, 2);
        assert_eq!(h.cluster_epoch, 5);
        assert_eq!(h.get(a).unwrap().session_epoch, 5);
        assert_eq!(h.get(b).unwrap().session_epoch, 5);
    }

    #[test]
    fn cluster_epoch_advance_is_monotonic() {
        let mut h = fresh();
        h.register(0, 0, b"k", b"", 0, 0, false, 0).unwrap();
        assert_eq!(h.advance_cluster_epoch(3), 1);
        // Stale / equal events are a no-op.
        assert_eq!(h.advance_cluster_epoch(3), 0);
        assert_eq!(h.advance_cluster_epoch(2), 0);
        assert_eq!(h.cluster_epoch, 3);
    }

    #[test]
    fn newly_registered_watch_stamps_current_cluster_epoch() {
        let mut h = fresh();
        h.advance_cluster_epoch(7);
        let id = h.register(0, 0, b"k", b"", 0, 0, false, 0).unwrap();
        assert_eq!(h.get(id).unwrap().session_epoch, 7);
    }
}
