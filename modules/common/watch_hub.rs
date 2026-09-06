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
//!   `last_sent_revision`, and the session binding — Fluxor's
//!   `session_id` / `anchor_id` / `session_epoch` triple
//!   (`session_core::SessionBinding`) that names the anchor fronting
//!   the watch and fences stale frames after a rebind.
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
//!   `kv_state_worker` snapshot via `replay_plan`).
//! - Lease attachment — that lives in `lease_manager` and only
//!   surfaces here through the `LeaseId` carried on a `KeyValue`.
//!
//! Heap-free, `no_std`-compatible, dual-target.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset of the surface so single-module rustc invocations see unused items"
)]

use core::cmp::Ordering;

#[path = "session_core.rs"]
#[allow(
    clippy::duplicate_mod,
    reason = "dual-target core mounted per consumer; each uses a subset"
)]
pub mod session_core;

use session_core::{AnchorId, PlacementEpoch, SessionBinding, SessionId};

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

// ── Event kinds (mirrors etcd v3 mvccpb.Event.EventType) ──────────────

pub const WATCH_EVENT_PUT: u8 = 0;
pub const WATCH_EVENT_DELETE: u8 = 1;

// ── Filter bits (etcd's WatchCreateRequest.filters) ───────────────────

/// Suppress PUT events (`NOPUT` filter).
pub const WATCH_FILTER_NOPUT: u8 = 1 << 0;
/// Suppress DELETE events (`NODELETE` filter).
pub const WATCH_FILTER_NODELETE: u8 = 1 << 1;

// ── Session phases (continuity) ───────────────────────────────────────

/// Bound and producing events.
pub const WATCH_PHASE_ACTIVE: u8 = 0;
/// `CMD_SC_DRAIN` received: no new events are emitted; the record
/// declares `DRAINED` once every emitted event has been acked.
pub const WATCH_PHASE_DRAINING: u8 = 1;
/// `DRAINED` declared and state exported: this worker consumes nothing
/// for the session until told to (`CMD_SC_RESUME` at the current
/// epoch returns it to service; at a higher epoch it is gone).
pub const WATCH_PHASE_DRAINED: u8 = 2;
/// Imported from a handoff and dormant until `CMD_SC_RESUME` commits
/// it at the new epoch.
pub const WATCH_PHASE_IMPORTED: u8 = 3;

// ── Handoff blob ──────────────────────────────────────────────────────

/// Magic prefix of an exported watch record.
pub const WATCH_EXPORT_MAGIC: &[u8; 4] = b"LWR1";
/// Fixed header bytes of an exported watch record, before the key bytes.
pub const WATCH_EXPORT_HDR: usize = 4 + 8 + 4 + 8 + 8 + 8 + 8 + 4 + 1 + 1 + 2 + 2 + 16 + 8;
/// Largest exported watch record.
pub const WATCH_EXPORT_MAX: usize = WATCH_EXPORT_HDR + WATCH_KEY_MAX;

fn arr8(buf: &[u8], at: usize) -> [u8; 8] {
    let mut out = [0u8; 8];
    if at + 8 <= buf.len() {
        out.copy_from_slice(&buf[at..at + 8]);
    }
    out
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
    /// Fluxor session identity: `session_id`, the `anchor_id`
    /// fronting the watch, and the per-session `session_epoch` that
    /// advances on every authoritative rebind of this watch. Stale
    /// frames carrying an older epoch are dropped. Presence
    /// (`binding.bound`) is tracked apart from the anchor id.
    pub binding: SessionBinding,
    /// Continuity phase of the session this record is (see
    /// `WATCH_PHASE_*`). Only an `ACTIVE` record produces events.
    pub phase: u8,
    /// Delivery cursor, inbound: control envelopes the anchor
    /// forwarded for this session that this record accounts for.
    pub in_consumed: u64,
    /// Events this registry emitted toward the fanout for this watch.
    pub out_emitted: u64,
    /// Events the fanout confirmed it framed for this watch. Advances
    /// in the same handler as `last_sent_revision`, so the delivery
    /// cursor and the acked revision always describe the same prefix.
    pub out_acked: u64,
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
            binding: SessionBinding::empty(),
            phase: WATCH_PHASE_ACTIVE,
            in_consumed: 0,
            out_emitted: 0,
            out_acked: 0,
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

    /// The session's current epoch.
    pub fn session_epoch(&self) -> u32 {
        self.binding.session_epoch
    }

    /// True iff the session is in service: bound, active, not mid-handoff.
    pub fn produces_events(&self) -> bool {
        self.in_use() && self.phase == WATCH_PHASE_ACTIVE && self.binding.bound
    }

    /// True iff every event emitted for this watch has been acked by
    /// the fanout — the point at which the delivery cursor and
    /// `last_sent_revision` describe the same prefix.
    pub fn outbound_settled(&self) -> bool {
        self.out_emitted == self.out_acked
    }

    /// Serialise the record for `CMD_SC_EXPORT_*`. Layout (all LE
    /// except identity bytes, which are copied raw):
    ///   `[magic "LWR1":4][watch_id:8][tenant:4][start_rev:8]
    ///    [last_sent:8][in_consumed:8][out_acked:8][epoch:4]
    ///    [filters:1][progress:1][key_len:2][range_end_len:2]
    ///    [session_id:16][anchor_id:8][key_buf: key_len+range_end_len]`
    /// Returns the bytes written, or 0 when `out` is too short.
    pub fn export(&self, out: &mut [u8]) -> usize {
        let kl = self.key_len as usize + self.range_end_len as usize;
        let need = WATCH_EXPORT_HDR + kl;
        if out.len() < need {
            return 0;
        }
        out[0..4].copy_from_slice(WATCH_EXPORT_MAGIC);
        out[4..12].copy_from_slice(&self.watch_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.tenant_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.start_revision.to_le_bytes());
        out[24..32].copy_from_slice(&self.last_sent_revision.to_le_bytes());
        out[32..40].copy_from_slice(&self.in_consumed.to_le_bytes());
        out[40..48].copy_from_slice(&self.out_acked.to_le_bytes());
        out[48..52].copy_from_slice(&self.binding.session_epoch.to_le_bytes());
        out[52] = self.filters;
        out[53] = self.progress_notify as u8;
        out[54..56].copy_from_slice(&self.key_len.to_le_bytes());
        out[56..58].copy_from_slice(&self.range_end_len.to_le_bytes());
        out[58..74].copy_from_slice(&self.binding.session_id);
        out[74..82].copy_from_slice(&self.binding.anchor_id);
        out[82..82 + kl].copy_from_slice(&self.key_buf[..kl]);
        need
    }

    /// Rebuild a record from an exported blob. The result is
    /// `WATCH_PHASE_IMPORTED`, bound to the exporter's anchor at the
    /// exporter's epoch; `out_emitted` equals `out_acked` because the
    /// exporter only exports once settled. `None` on a malformed blob.
    pub fn import(src: &[u8]) -> Option<Self> {
        if src.len() < WATCH_EXPORT_HDR || &src[0..4] != WATCH_EXPORT_MAGIC {
            return None;
        }
        let key_len = u16::from_le_bytes([src[54], src[55]]);
        let range_end_len = u16::from_le_bytes([src[56], src[57]]);
        let kl = key_len as usize + range_end_len as usize;
        if kl > WATCH_KEY_MAX || src.len() < WATCH_EXPORT_HDR + kl {
            return None;
        }
        let watch_id = u64::from_le_bytes(arr8(src, 4));
        if watch_id == 0 {
            return None;
        }
        let mut session_id: SessionId = [0; 16];
        session_id.copy_from_slice(&src[58..74]);
        let mut anchor_id: AnchorId = [0; 8];
        anchor_id.copy_from_slice(&src[74..82]);
        let epoch = u32::from_le_bytes([src[48], src[49], src[50], src[51]]);
        let binding = SessionBinding::imported(session_id, anchor_id, epoch)?;
        let filters = src[52];
        if filters & !(WATCH_FILTER_NOPUT | WATCH_FILTER_NODELETE) != 0 {
            return None;
        }
        let mut rec = WatchRecord::free();
        rec.watch_id = watch_id;
        rec.tenant_id = u32::from_le_bytes([src[12], src[13], src[14], src[15]]);
        rec.start_revision = i64::from_le_bytes(arr8(src, 16));
        rec.last_sent_revision = i64::from_le_bytes(arr8(src, 24));
        rec.in_consumed = u64::from_le_bytes(arr8(src, 32));
        rec.out_acked = u64::from_le_bytes(arr8(src, 40));
        rec.out_emitted = rec.out_acked;
        rec.binding = binding;
        rec.phase = WATCH_PHASE_IMPORTED;
        rec.filters = filters;
        rec.progress_notify = src[53] != 0;
        rec.key_len = key_len;
        rec.range_end_len = range_end_len;
        rec.key_buf[..kl].copy_from_slice(&src[82..82 + kl]);
        Some(rec)
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
    /// `control_plane`. Held here, never stamped into a record: a
    /// placement event advances this and causes the directory to
    /// rebind the sessions whose placement moved, each of which
    /// advances its own `session_epoch` by one.
    pub placement_epoch: u32,
}

impl WatchHub {
    pub const fn new() -> Self {
        Self {
            records: [WatchRecord::free(); MAX_WATCHES],
            registry_revision: 0,
            next_id: 1,
            placement_epoch: PlacementEpoch::new().current,
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
        self.placement_epoch = PlacementEpoch::new().current;
    }

    /// Advance the hub's placement epoch. Touches no record: session
    /// epochs move only through [`WatchHub::rebind`]. Returns whether
    /// the event was a real advance (duplicates and out-of-order
    /// events are ignored).
    pub fn advance_placement_epoch(&mut self, new_epoch: u32) -> bool {
        let mut p = PlacementEpoch {
            current: self.placement_epoch,
        };
        let advanced = p.advance(new_epoch);
        self.placement_epoch = p.current;
        advanced
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

    /// Lookup by session identity.
    pub fn find_session(&self, session_id: &SessionId) -> Option<&WatchRecord> {
        self.records
            .iter()
            .find(|r| r.in_use() && r.binding.is(session_id))
    }

    pub fn find_session_mut(&mut self, session_id: &SessionId) -> Option<&mut WatchRecord> {
        self.records
            .iter_mut()
            .find(|r| r.in_use() && r.binding.is(session_id))
    }

    /// Adopt an imported record into a free slot. Refused when the
    /// watch id or the session id is already held here.
    pub fn adopt(&mut self, rec: WatchRecord) -> Result<(), WatchError> {
        if !rec.in_use() {
            return Err(WatchError::InvalidId);
        }
        if self.index_of(rec.watch_id).is_some()
            || self.find_session(&rec.binding.session_id).is_some()
        {
            return Err(WatchError::InvalidId);
        }
        let idx = self.find_free().ok_or(WatchError::TableFull)?;
        self.records[idx] = rec;
        self.registry_revision = self.registry_revision.wrapping_add(1);
        Ok(())
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
    /// `tenant_id`, `start_revision`, `filters` and `progress_notify`
    /// populate the fresh record; `session_id` / `anchor_id` mint its
    /// binding at `FIRST_EPOCH`. `last_sent_revision` is initialised to
    /// `start_revision`. A `session_id` already held here is refused.
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
        session_id: SessionId,
        anchor_id: AnchorId,
    ) -> Result<u64, WatchError> {
        if filters & !(WATCH_FILTER_NOPUT | WATCH_FILTER_NODELETE) != 0 {
            return Err(WatchError::BadFilter);
        }
        if self.find_session(&session_id).is_some() {
            return Err(WatchError::InvalidId);
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
        rec.binding = SessionBinding::attach(session_id, anchor_id);
        rec.phase = WATCH_PHASE_ACTIVE;
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

    /// Authoritative rebind of a watch onto `anchor_id`. The session
    /// epoch advances by one so any in-flight frames under the old
    /// generation are dropped at the anchor. Returns the new epoch.
    pub fn rebind(&mut self, watch_id: u64, anchor_id: AnchorId) -> Result<u32, WatchError> {
        let idx = self.index_of(watch_id).ok_or(WatchError::NotFound)?;
        let epoch = self.records[idx].binding.rebind(anchor_id);
        self.registry_revision = self.registry_revision.wrapping_add(1);
        Ok(epoch)
    }

    /// Unbind without cancelling — the anchor connection dropped
    /// but the client may resume later. Slot stays allocated;
    /// identity and epoch are kept, only presence is cleared.
    pub fn unbind(&mut self, watch_id: u64) -> Result<(), WatchError> {
        let idx = self.index_of(watch_id).ok_or(WatchError::NotFound)?;
        self.records[idx].binding.unbind();
        self.registry_revision = self.registry_revision.wrapping_add(1);
        Ok(())
    }

    /// Bump `last_sent_revision` once the fanout confirms `rev` has
    /// been written to the wire for `watch_id`. The registry uses
    /// the new value as the resume point if the connection drops.
    ///
    /// One ack is one framed event, so the delivery cursor
    /// (`out_acked`) and the acked revision advance together here and
    /// nowhere else — they cannot describe different prefixes.
    pub fn mark_delivered(&mut self, watch_id: u64, rev: i64) -> Result<(), WatchError> {
        let idx = self.index_of(watch_id).ok_or(WatchError::NotFound)?;
        let rec = &mut self.records[idx];
        rec.out_acked = rec.out_acked.wrapping_add(1);
        if rev > rec.last_sent_revision {
            rec.last_sent_revision = rev;
        }
        self.registry_revision = self.registry_revision.wrapping_add(1);
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
    pub session_id: SessionId,
    pub session_epoch: u32,
}

/// Iterate every in-service watch (bound, active, not mid-handoff)
/// and call `sink` for those whose `(key_range × filter ×
/// start_revision)` admits this event.
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
        if !rec.produces_events() {
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
            session_id: rec.binding.session_id,
            session_epoch: rec.binding.session_epoch,
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
            session_epoch: rec.binding.session_epoch,
        }),
        Ordering::Equal | Ordering::Greater => Some(ReplayPlan {
            watch_id,
            from_revision: to,
            to_revision: to,
            session_epoch: rec.binding.session_epoch,
        }),
    }
}

// ── Host-side unit tests (gated off the no_std module build) ──────────

#[cfg(test)]
mod tests {
    use super::*;

    const A: AnchorId = *b"ANCHOR-A";
    const B: AnchorId = *b"ANCHOR-B";

    fn sid(n: u8) -> SessionId {
        let mut s = [0u8; 16];
        s[..8].copy_from_slice(&A);
        s[15] = n;
        s
    }

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
            .register(0, 0, b"foo", b"", 0, 0, false, sid(1), A)
            .unwrap();
        assert_eq!(id, 1);
        assert_eq!(h.len(), 1);

        let mut hits = std::vec::Vec::new();
        match_event(&h, 0, 7, WATCH_EVENT_PUT, b"foo", |m| hits.push(m));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].watch_id, id);
        assert_eq!(hits[0].session_epoch, 1);
        assert_eq!(hits[0].session_id, sid(1));

        let mut other = std::vec::Vec::new();
        match_event(&h, 0, 7, WATCH_EVENT_PUT, b"bar", |m| other.push(m));
        assert!(other.is_empty());
    }

    #[test]
    fn register_prefix_range_matches_in_range() {
        let mut h = fresh();
        // [b"a", b"f") — matches "a", "b", … "e"
        h.register(0, 0, b"a", b"f", 0, 0, false, sid(1), A)
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
        let id = h.register(0, 0, b"k", b"", 5, 0, false, sid(1), A).unwrap();
        let mut hits = std::vec::Vec::new();
        match_event(&h, 0, 5, WATCH_EVENT_PUT, b"k", |m| hits.push(m));
        match_event(&h, 0, 6, WATCH_EVENT_PUT, b"k", |m| hits.push(m));
        assert_eq!(hits.len(), 1, "rev==start filtered, rev>start delivered");
        assert_eq!(hits[0].watch_id, id);
    }

    #[test]
    fn filters_suppress_specific_event_kinds() {
        let mut h = fresh();
        h.register(0, 0, b"k", b"", 0, WATCH_FILTER_NOPUT, false, sid(1), A)
            .unwrap();
        let mut hits = std::vec::Vec::new();
        match_event(&h, 0, 1, WATCH_EVENT_PUT, b"k", |m| hits.push(m));
        match_event(&h, 0, 2, WATCH_EVENT_DELETE, b"k", |m| hits.push(m));
        assert_eq!(hits.len(), 1, "PUT suppressed, DELETE delivered");
    }

    #[test]
    fn rebind_advances_session_epoch_and_names_the_anchor() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, sid(1), A).unwrap();
        assert_eq!(h.get(id).unwrap().session_epoch(), 1);
        let new_epoch = h.rebind(id, B).unwrap();
        assert_eq!(new_epoch, 2);
        let rec = h.get(id).unwrap();
        assert_eq!(rec.binding.anchor_id, B);
        assert!(rec.binding.bound);
    }

    #[test]
    fn unbind_keeps_record_alive_for_resume() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, sid(1), A).unwrap();
        h.unbind(id).unwrap();
        let rec = h.get(id).unwrap();
        assert!(!rec.binding.bound);
        assert_eq!(rec.binding.anchor_id, A, "identity survives unbind");
        assert_eq!(h.len(), 1);
        // An unbound watch is not in service.
        let mut hits = std::vec::Vec::new();
        match_event(&h, 0, 1, WATCH_EVENT_PUT, b"k", |m| hits.push(m));
        assert!(hits.is_empty());
    }

    #[test]
    fn duplicate_session_id_is_refused() {
        let mut h = fresh();
        h.register(0, 0, b"k", b"", 0, 0, false, sid(1), A).unwrap();
        let err = h
            .register(0, 0, b"k2", b"", 0, 0, false, sid(1), A)
            .unwrap_err();
        assert_eq!(err, WatchError::InvalidId);
    }

    #[test]
    fn mark_delivered_advances_revision_and_cursor_together() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, sid(1), A).unwrap();
        h.mark_delivered(id, 5).unwrap();
        assert_eq!(h.get(id).unwrap().last_sent_revision, 5);
        assert_eq!(h.get(id).unwrap().out_acked, 1);
        h.mark_delivered(id, 3).unwrap(); // older revision — must NOT regress
        assert_eq!(h.get(id).unwrap().last_sent_revision, 5);
        assert_eq!(
            h.get(id).unwrap().out_acked,
            2,
            "but the frame was delivered"
        );
        h.mark_delivered(id, 7).unwrap();
        assert_eq!(h.get(id).unwrap().last_sent_revision, 7);
        assert_eq!(h.get(id).unwrap().out_acked, 3);
    }

    #[test]
    fn cancel_frees_slot_and_returns_last_sent() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, sid(1), A).unwrap();
        h.mark_delivered(id, 42).unwrap();
        let last = h.cancel(id).unwrap();
        assert_eq!(last, 42);
        assert_eq!(h.len(), 0);
        assert!(h.get(id).is_none());
    }

    #[test]
    fn retention_floor_is_min_last_sent() {
        let mut h = fresh();
        let a = h.register(0, 0, b"a", b"", 0, 0, false, sid(1), A).unwrap();
        let b = h.register(0, 0, b"b", b"", 0, 0, false, sid(2), A).unwrap();
        h.mark_delivered(a, 10).unwrap();
        h.mark_delivered(b, 7).unwrap();
        assert_eq!(h.retention_floor(), Some(7));
    }

    #[test]
    fn replay_plan_describes_resume_interval() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, sid(1), A).unwrap();
        h.mark_delivered(id, 3).unwrap();
        let plan = replay_plan(&h, id, 10).unwrap();
        assert_eq!(plan.from_revision, 3);
        assert_eq!(plan.to_revision, 10);
        assert_eq!(plan.watch_id, id);
    }

    #[test]
    fn replay_plan_for_up_to_date_watch_is_noop() {
        let mut h = fresh();
        let id = h.register(0, 0, b"k", b"", 0, 0, false, sid(1), A).unwrap();
        h.mark_delivered(id, 5).unwrap();
        let plan = replay_plan(&h, id, 5).unwrap();
        assert_eq!(plan.from_revision, plan.to_revision);
    }

    #[test]
    fn register_rejects_bad_filter_bits() {
        let mut h = fresh();
        let err = h
            .register(0, 0, b"k", b"", 0, 0xFF, false, sid(1), A)
            .unwrap_err();
        assert_eq!(err, WatchError::BadFilter);
    }

    #[test]
    fn register_rejects_oversize_key() {
        let mut h = fresh();
        let big = std::vec::Vec::from([b'k'; WATCH_KEY_MAX + 1]);
        let err = h
            .register(0, 0, &big, b"", 0, 0, false, sid(1), A)
            .unwrap_err();
        assert_eq!(err, WatchError::KeyTooLarge);
    }

    #[test]
    fn cross_tenant_does_not_leak_events() {
        let mut h = fresh();
        h.register(0, 0, b"k", b"", 0, 0, false, sid(1), A).unwrap();
        h.register(0, 1, b"k", b"", 0, 0, false, sid(2), A).unwrap();
        let mut for_t0 = std::vec::Vec::new();
        let mut for_t1 = std::vec::Vec::new();
        match_event(&h, 0, 1, WATCH_EVENT_PUT, b"k", |m| for_t0.push(m));
        match_event(&h, 1, 1, WATCH_EVENT_PUT, b"k", |m| for_t1.push(m));
        assert_eq!(for_t0.len(), 1);
        assert_eq!(for_t1.len(), 1);
        assert_ne!(
            for_t0[0].session_id, for_t1[0].session_id,
            "events MUST stay tenant-scoped",
        );
    }

    #[test]
    fn table_full_returns_err() {
        let mut h = fresh();
        let mut i = 0;
        while i < MAX_WATCHES {
            let mut s = sid(0);
            s[14] = (i >> 8) as u8;
            s[15] = i as u8;
            h.register(0, 0, b"k", b"", 0, 0, false, s, A).unwrap();
            i += 1;
        }
        let mut s = sid(0);
        s[13] = 1;
        let err = h.register(0, 0, b"k", b"", 0, 0, false, s, A).unwrap_err();
        assert_eq!(err, WatchError::TableFull);
    }

    #[test]
    fn placement_advance_touches_no_session() {
        let mut h = fresh();
        let a = h
            .register(0, 0, b"k1", b"", 0, 0, false, sid(1), A)
            .unwrap();
        let b = h
            .register(0, 0, b"k2", b"", 0, 0, false, sid(2), A)
            .unwrap();
        assert!(h.advance_placement_epoch(5));
        assert_eq!(h.placement_epoch, 5);
        assert_eq!(h.get(a).unwrap().session_epoch(), 1);
        assert_eq!(h.get(b).unwrap().session_epoch(), 1);
        // Stale / equal events are a no-op.
        assert!(!h.advance_placement_epoch(5));
        assert!(!h.advance_placement_epoch(2));
        assert_eq!(h.placement_epoch, 5);
    }

    #[test]
    fn export_import_round_trips_a_record() {
        let mut h = fresh();
        let id = h
            .register(
                0,
                3,
                b"ab",
                b"ac",
                4,
                WATCH_FILTER_NODELETE,
                true,
                sid(7),
                A,
            )
            .unwrap();
        h.rebind(id, B).unwrap();
        h.mark_delivered(id, 9).unwrap();
        h.get_mut(id).unwrap().in_consumed = 3;
        let mut blob = [0u8; WATCH_EXPORT_MAX];
        let n = h.get(id).unwrap().export(&mut blob);
        assert_eq!(n, WATCH_EXPORT_HDR + 4);
        let rec = WatchRecord::import(&blob[..n]).unwrap();
        assert_eq!(rec.watch_id, id);
        assert_eq!(rec.tenant_id, 3);
        assert_eq!(rec.start_revision, 4);
        assert_eq!(rec.last_sent_revision, 9);
        assert_eq!(rec.in_consumed, 3);
        assert_eq!(rec.out_acked, 1);
        assert_eq!(rec.out_emitted, 1);
        assert_eq!(rec.binding.session_id, sid(7));
        assert_eq!(rec.binding.anchor_id, B);
        assert_eq!(rec.session_epoch(), 2);
        assert_eq!(rec.phase, WATCH_PHASE_IMPORTED);
        assert_eq!(rec.filters, WATCH_FILTER_NODELETE);
        assert!(rec.progress_notify);
        assert_eq!(rec.key(), b"ab");
        assert_eq!(rec.range_end(), b"ac");
        // A dormant import produces nothing until resumed.
        let mut h2 = fresh();
        h2.adopt(rec).unwrap();
        let mut hits = std::vec::Vec::new();
        match_event(&h2, 3, 10, WATCH_EVENT_PUT, b"ab", |m| hits.push(m));
        assert!(hits.is_empty());
        h2.get_mut(id).unwrap().phase = WATCH_PHASE_ACTIVE;
        match_event(&h2, 3, 10, WATCH_EVENT_PUT, b"ab", |m| hits.push(m));
        assert_eq!(hits.len(), 1);
        // Adopting the same identity twice is refused.
        assert_eq!(h2.adopt(rec), Err(WatchError::InvalidId));
    }

    #[test]
    fn import_refuses_malformed_blobs() {
        assert!(WatchRecord::import(b"LWR1").is_none());
        let mut blob = [0u8; WATCH_EXPORT_HDR];
        blob[..4].copy_from_slice(b"XXXX");
        assert!(WatchRecord::import(&blob).is_none());
        blob[..4].copy_from_slice(WATCH_EXPORT_MAGIC);
        // watch_id 0 and epoch 0 are both refused.
        assert!(WatchRecord::import(&blob).is_none());
        blob[4] = 1;
        assert!(WatchRecord::import(&blob).is_none());
        blob[48] = 1;
        assert!(WatchRecord::import(&blob).is_some());
        blob[54] = 200; // key_len past WATCH_KEY_MAX
        assert!(WatchRecord::import(&blob).is_none());
    }
}
