//! Pure-logic watch hub for Lattice.
//!
//! Mirrors `modules/common/kv_store.rs` / `etcd_codec.rs`: the same
//! source compiles into the no_std PIC `watch_registry` module AND is
//! `#[path]`-mounted into the host suites under `tests/harness/tests/`
//! (`integration_watch.rs` runs the inline tests below). A regression in
//! the state machine — replay-fence math, key-prefix matching,
//! filter-evaluation — is caught by `cargo test` before any module
//! ELF runs.
//!
//! ## Roles
//!
//! - **`watch_registry`** owns the table and is this facade's one PIC
//!   consumer: which `WatchID` is active, the per-watch key range /
//!   filter / start-revision / `last_sent_revision`, the session binding
//!   — Fluxor's `session_id` / `anchor_id` / `session_epoch` triple
//!   (`session_core::SessionBinding`) that names the anchor fronting the
//!   watch and fences stale frames after a rebind — and the matching of
//!   each mutation `kv_state_worker` commits against every watch
//!   ([`match_event_from`]).
//! - **`watch_fanout`** holds no watch table. It frames the events the
//!   registry matched toward `etcd_edge_anchor`, acks each one back
//!   (the registry's [`WatchHub::mark_delivered`]), and serves the
//!   registry's replay plans from the store's history
//!   (`watch_replay.rs`).
//!
//! ## What this facade does NOT own
//!
//! - HPACK / gRPC framing — that lives in `etcd_codec.rs`.
//! - History beyond the [`MutationTail`] — a watch owed revisions the
//!   tail no longer holds is replayed from the store by `watch_fanout`
//!   (`MSG_WATCH_REPLAY_PLAN`, `watch_replay.rs`).
//! - Lease attachment — that lives in `lease_manager` and only
//!   surfaces here through the `LeaseId` carried on a `KeyValue`.
//!
//! Heap-free, `no_std`-compatible, dual-target.

#![allow(
    unexpected_cfgs,
    reason = "capacity profiles: `profile_rp2350` / `profile_server` are features the [[variant]]s of the modules that build them declare; a module without variants builds the Pi 5 default and declares neither"
)]
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

/// Active watches across all anchors, by capacity profile. A
/// route-catalogue director watches one stable catalogue shard at a time,
/// not one endpoint: a shard is a service id's leading byte under one
/// tenant (`rc/<tenant>/<byte>…`), so a tenant's catalogue is 256 shards,
/// whatever its size — Pi 5's 4,096 routes and 32,768 endpoints are 16
/// routes and 128 endpoints a shard, a server's 65,536 and 1,048,576 are
/// 256 and 4,096. The watch count is then shards × tenants served: Pi 5
/// admits four tenants' directors (1,024), a server thirty-two (8,192),
/// the RP2350 one tenant's 64 shards (its 16 routes and 64 endpoints need
/// no more). Each record is `size_of::<WatchRecord>()` (392 B with a
/// 256-byte key): ~392 KiB, ~3.1 MiB and ~25 KiB of records, each watch
/// also taking a slot of the registry's session worker. Every module of a
/// graph is built in one profile.
#[cfg(not(any(feature = "profile_rp2350", feature = "profile_server")))]
pub const MAX_WATCHES: usize = 1024;
#[cfg(feature = "profile_rp2350")]
pub const MAX_WATCHES: usize = 64;
#[cfg(feature = "profile_server")]
pub const MAX_WATCHES: usize = 8192;

/// Bytes a watch's `key` and `range_end` take together
/// (`WatchRecord::key_buf`). etcd's `range_end` lets a watch span a key
/// range: a single key, a `[key, range_end)` range (a prefix watch is the
/// range whose end is the prefix with its last byte incremented), or
/// `range_end` `\0` for every key from `key` on. Lattice's key ceiling:
/// a 256-byte single-key watch is held whole.
pub const WATCH_KEY_MAX: usize = 256;

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
    /// only after the fanout confirms the frame is on the wire, and
    /// once a drain settles, to `drained_through` (see
    /// [`WatchRecord::settle_drain`]).
    pub last_sent_revision: i64,
    /// The registry's mutation head when the record left service: every
    /// mutation at or below it was matched against the watch, and each
    /// that matched was emitted — or, while a replay is open, is owed by
    /// the catch-up that replay continues into ([`catch_up_step`]), which
    /// a draining record runs up to here and no further. Local to this
    /// registry; never exported.
    pub drained_through: i64,
    /// Live matching floor: a mutation at or below it is not matched —
    /// the client already has it, or a replay or tail catch-up covered
    /// it. Local; never exported.
    pub live_from: i64,
    /// Replay plans this registry issued for the watch that the fanout
    /// has not yet answered as finished. While one is open nothing is
    /// matched live for the watch: the replay's frames reach the client
    /// first, and its close continues the catch-up from where it ended
    /// ([`WatchHub::replay_done`], [`catch_up_step`]). Local; never
    /// exported.
    pub replays_open: u16,
    /// The revision the open replay is known to cover through: its upper
    /// bound, or for an open-ended replay its lower one (the fanout
    /// reports the ceiling it pinned when it closes). Local; never
    /// exported.
    pub replay_through: i64,
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
    /// Frames owed toward the anchor for this watch: live events this
    /// registry emitted to the fanout, plus the frames each finished
    /// replay reported sending.
    pub out_emitted: u64,
    /// Frames the fanout confirmed it put on the wire toward the anchor
    /// for this watch, live and replayed alike: the session's outbound
    /// delivery cursor, which the anchor counts as it reads them.
    /// Advances in the same handler as `last_sent_revision`, so the
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
            drained_through: 0,
            live_from: 0,
            replays_open: 0,
            replay_through: 0,
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

    /// True iff nothing more is coming toward the anchor for this
    /// watch: no replay is open, and every frame owed — live or replayed —
    /// has been acked by the fanout. The point at which the delivery
    /// cursor counts every frame the session will produce here, and
    /// `last_sent_revision` describes the same prefix.
    pub fn outbound_settled(&self) -> bool {
        self.replays_open == 0 && self.out_emitted == self.out_acked
    }

    /// True iff this registry serves the record: in service, or draining
    /// out of it. Fanout acks fan out to every registry; only the one
    /// serving a watch counts them, so a record dormant here (imported, or
    /// drained and exported) is never advanced by frames another registry
    /// produced.
    pub fn serving(&self) -> bool {
        self.in_use() && matches!(self.phase, WATCH_PHASE_ACTIVE | WATCH_PHASE_DRAINING)
    }

    /// Put the record in service; returns the revision the client has
    /// everything up to: `last_sent_revision`, once a drain settles (see
    /// [`WatchRecord::settle_drain`]). Live matching starts above it.
    ///
    /// From here on the registry matches only mutations it has yet to
    /// read. Those it read before, above the returned revision, are owed
    /// first: the caller delivers them from its [`MutationTail`], or from
    /// a store replay ([`WatchRecord::begin_replay`]), which holds live
    /// matching off until it closes.
    ///
    /// `None` when a replay is still open (a drain refused before it
    /// settled): the record is back in service, the client's position is
    /// still being advanced by that replay, and its close carries the
    /// catch-up on to the registry's head — nothing is to be planned now.
    pub fn enter_service(&mut self) -> Option<i64> {
        self.phase = WATCH_PHASE_ACTIVE;
        if self.replays_open > 0 {
            return None;
        }
        self.settle_drain();
        self.live_from = self.last_sent_revision;
        Some(self.last_sent_revision)
    }

    /// A replay plan over `(from, to]` was issued for the watch (`to` 0:
    /// up to the store's present). Live matching is held off until it
    /// closes.
    pub fn begin_replay(&mut self, from: i64, to: i64) {
        self.replays_open = self.replays_open.saturating_add(1);
        self.replay_through = if to == 0 { from } else { to };
    }

    /// True iff a mutation (`event_kind` on `event_key`, for `tenant_id`)
    /// is one this watch reports, revision aside: in service, the
    /// watch's tenant, not filtered out, and in its key span.
    pub fn wants(&self, tenant_id: u32, event_kind: u8, event_key: &[u8]) -> bool {
        self.produces_events() && self.reports(tenant_id, event_kind, event_key)
    }

    /// True iff a mutation the registry has already read is owed to the
    /// client by a catch-up: as [`WatchRecord::wants`], but for a record
    /// draining out of service too — its catch-up runs to the drain cut.
    pub fn owes(&self, tenant_id: u32, event_kind: u8, event_key: &[u8]) -> bool {
        self.serving() && self.binding.bound && self.reports(tenant_id, event_kind, event_key)
    }

    /// The watch's tenant, not filtered out, and in its key span.
    fn reports(&self, tenant_id: u32, event_kind: u8, event_key: &[u8]) -> bool {
        self.tenant_id == tenant_id && self.passes_filter(event_kind) && self.matches_key(event_key)
    }

    /// Take the record out of service with the registry's mutation head
    /// at `head`: it produces nothing more until it is resumed.
    pub fn begin_drain(&mut self, head: i64) {
        self.phase = WATCH_PHASE_DRAINING;
        self.drained_through = head;
    }

    /// Advance `last_sent_revision` to the revision the watch has missed
    /// nothing up to as it leaves service: `drained_through`. A revision
    /// at or below it either did not match the watch — the client was
    /// never owed it — or did and was emitted, so the fanout frames it
    /// whatever happens to the session. The resume point the record
    /// exports, and the replay plan it is reinstated with, therefore
    /// start there. Left at the last delivered event, a watch whose key
    /// has been quiet (one that has seen no event at all sits at 0)
    /// would ask for history back to that point: a window the client
    /// does not need, which a store without that history refuses as
    /// compacted — ending the watch.
    pub fn settle_drain(&mut self) {
        if self.drained_through > self.last_sent_revision {
            self.last_sent_revision = self.drained_through;
        }
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
        // Exported settled: no replay open, every owed frame acked.
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
    /// every key with `key <= k < range_end`; a range_end of `\0` is
    /// etcd's "every key from `key` on" (with key `\0`: every key).
    pub fn matches_key(&self, event_key: &[u8]) -> bool {
        let key = self.key();
        if self.range_end_len == 0 {
            return key == event_key;
        }
        let range_end = self.range_end();
        if range_end == [0] {
            return event_key >= key;
        }
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
/// (linear scan), `MAX_WATCHES` slots. A cancelled watch's slot is freed
/// in place: no record moves, so a record's index is stable for as long
/// as it lives ([`match_event_from`] resumes by index).
#[repr(C)]
pub struct WatchHub {
    pub records: [WatchRecord; MAX_WATCHES],
    /// Monotonic per-hub counter, bumped on every successful state
    /// mutation (register, adopt, cancel, rebind, unbind, delivery ack).
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

    /// Cancel a watch. Returns the record's `last_sent_revision`. The
    /// slot is freed in place.
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
    ///
    /// Counted only by the registry serving the watch
    /// ([`WatchRecord::serving`]); elsewhere `NotFound`.
    pub fn mark_delivered(&mut self, watch_id: u64, rev: i64) -> Result<(), WatchError> {
        let idx = self.serving_index(watch_id).ok_or(WatchError::NotFound)?;
        let rec = &mut self.records[idx];
        rec.out_acked = rec.out_acked.wrapping_add(1);
        if rev > rec.last_sent_revision {
            rec.last_sent_revision = rev;
        }
        self.registry_revision = self.registry_revision.wrapping_add(1);
        Ok(())
    }

    /// A replay plan issued for `watch_id` is finished with, having put
    /// `frames` frames on the wire (each already acked through
    /// [`WatchHub::mark_delivered`]) and covered the window through
    /// `through` (the ceiling the fanout scanned to; 0 when it reports
    /// none): they join the frames owed, and the plan is no longer open.
    /// Counted only by the serving registry.
    ///
    /// Returns, once no replay is left open, the revision the client now
    /// has every owed event through: what the catch-up continues from
    /// ([`catch_up_step`]). The fanout sends a plan's frames ahead of its
    /// close, so every frame up to it is already on the wire.
    pub fn replay_done(
        &mut self,
        watch_id: u64,
        frames: u64,
        through: i64,
    ) -> Result<Option<i64>, WatchError> {
        let idx = self.serving_index(watch_id).ok_or(WatchError::NotFound)?;
        let rec = &mut self.records[idx];
        rec.replays_open = rec.replays_open.saturating_sub(1);
        rec.out_emitted = rec.out_emitted.wrapping_add(frames);
        if rec.replays_open > 0 {
            return Ok(None);
        }
        Ok(Some(rec.replay_through.max(through)))
    }

    /// The slot of `watch_id` when this registry serves it.
    fn serving_index(&self, watch_id: u64) -> Option<usize> {
        self.index_of(watch_id)
            .filter(|&i| self.records[i].serving())
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
/// start_revision`, or `<= live_from` (a replay covers it), and for any
/// watch with a replay open: everything it reads meanwhile is delivered
/// by the catch-up that follows the replay, in revision order.
///
/// Matching skips by revision, not by read order: when several workers'
/// mutation streams fan into one registry, a revision at or below
/// `live_from` read after live matching opened is not delivered.
pub fn match_event<F: FnMut(MatchedWatch)>(
    hub: &WatchHub,
    tenant_id: u32,
    event_revision: i64,
    event_kind: u8,
    event_key: &[u8],
    mut sink: F,
) {
    for rec in hub.records.iter() {
        if event_revision <= rec.start_revision
            || event_revision <= rec.live_from
            || rec.replays_open > 0
        {
            continue;
        }
        if !rec.wants(tenant_id, event_kind, event_key) {
            continue;
        }
        sink(MatchedWatch {
            watch_id: rec.watch_id,
            session_id: rec.binding.session_id,
            session_epoch: rec.binding.session_epoch,
        });
    }
}

/// [`match_event`] in bounded batches: from record index `from`, hand
/// each matching watch to `sink` until it answers `false` (its batch is
/// full). `None` when every record was visited; `Some(next)` the index
/// the next batch starts at, with the record that was refused included
/// (it is handed over again). A registry that must bound the matches it
/// holds at once drives this until `None`, so no matching watch is
/// passed over.
pub fn match_event_from<F: FnMut(MatchedWatch) -> bool>(
    hub: &WatchHub,
    from: usize,
    tenant_id: u32,
    event_revision: i64,
    event_kind: u8,
    event_key: &[u8],
    mut sink: F,
) -> Option<usize> {
    for (i, rec) in hub.records.iter().enumerate().skip(from) {
        if event_revision <= rec.start_revision
            || event_revision <= rec.live_from
            || rec.replays_open > 0
        {
            continue;
        }
        if !rec.wants(tenant_id, event_kind, event_key) {
            continue;
        }
        let keep = sink(MatchedWatch {
            watch_id: rec.watch_id,
            session_id: rec.binding.session_id,
            session_epoch: rec.binding.session_epoch,
        });
        if !keep {
            return Some(i);
        }
    }
    None
}

// ── Catch-up after a replay ───────────────────────────────────────────

/// What a watch whose replay just closed, covering it through `covered`,
/// is owed next ([`catch_up_step`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatchUp {
    /// Nothing more is owed: match live above this revision.
    Live(i64),
    /// Draining, and everything up to the drain cut is delivered.
    Settled,
    /// Deliver the mutation tail's entries in `(from, to]` that the watch
    /// reports, in the order read; then, in service, match live above
    /// `to`.
    Tail { from: i64, to: i64 },
    /// Replay `(from, to]` from the store, live matching still held off;
    /// its close is the next step.
    Replay { from: i64, to: i64 },
}

/// The next step of a watch's catch-up once its replay has closed with
/// the client covered through `covered`, the registry having read every
/// mutation up to `head`.
///
/// The mutations read while the replay ran were not matched live (see
/// [`match_event`]); they lie above `covered` and are owed now, ahead of
/// anything matched live. A record in service is owed them up to `head`;
/// a draining one up to its drain cut. Read recently enough, they are
/// still in the mutation tail and are delivered from it at once;
/// otherwise a further store replay covers them, and the loop repeats
/// when it closes. Each round starts where the last ended, so revisions
/// reach the client in order with none repeated or skipped, and the loop
/// ends at the first round whose gap the tail still holds — once the
/// replay finishes faster than [`MUTATION_TAIL_BYTES`] of new mutations
/// arrive (always, for a draining record, whose cut is fixed).
pub fn catch_up_step(rec: &WatchRecord, covered: i64, head: i64, tail: &MutationTail) -> CatchUp {
    let target = match rec.phase {
        WATCH_PHASE_ACTIVE => head,
        WATCH_PHASE_DRAINING => rec.drained_through,
        _ => return CatchUp::Settled,
    };
    if covered >= target {
        return if rec.phase == WATCH_PHASE_ACTIVE {
            CatchUp::Live(covered)
        } else {
            CatchUp::Settled
        };
    }
    if tail.covers(covered) {
        CatchUp::Tail {
            from: covered,
            to: target,
        }
    } else {
        CatchUp::Replay {
            from: covered,
            to: target,
        }
    }
}

// ── Mutation tail ─────────────────────────────────────────────────────

/// Bytes of mutation stream a registry retains in its [`MutationTail`].
pub const MUTATION_TAIL_BYTES: usize = 16384;

/// The most recent mutations a registry has read, in the order read, each
/// as its `MSG_WATCH_EVENT` payload
/// (`[kpg:2][revision:8][op:1][key_len:2][key][…]`).
///
/// A watch session handed to a registry has missed the mutations that
/// arrived between the cut its previous registry drained at and this
/// registry taking it over. This registry read them all: the tail lets it
/// deliver them from what it read rather than asking the store for
/// history — which a store that keeps none cannot give. It covers every
/// mutation above [`MutationTail::floor`] that the registry has read; the
/// oldest are evicted (raising the floor) to make room.
#[repr(C)]
pub struct MutationTail {
    /// Entries `[len:2][payload]`, oldest first, in `buf[..len]`.
    buf: [u8; MUTATION_TAIL_BYTES],
    len: usize,
    /// Revision the tail is complete above; meaningful once `seeded`.
    floor: i64,
    /// Whether any mutation has been read since the registry started.
    seeded: bool,
}

impl MutationTail {
    pub const fn new() -> Self {
        Self {
            buf: [0; MUTATION_TAIL_BYTES],
            len: 0,
            floor: 0,
            seeded: false,
        }
    }

    pub fn init(&mut self) {
        self.len = 0;
        self.floor = 0;
        self.seeded = false;
    }

    /// The revision the tail is complete above: every mutation above it
    /// the registry has read is held. `None` before the first.
    pub fn floor(&self) -> Option<i64> {
        self.seeded.then_some(self.floor)
    }

    /// True iff every mutation above `from` that the registry has read
    /// is held.
    pub fn covers(&self, from: i64) -> bool {
        self.seeded && from >= self.floor
    }

    /// Hold one mutation read at `revision`. The first one read seeds the
    /// floor just below it: nothing earlier was read here.
    pub fn push(&mut self, revision: i64, payload: &[u8]) {
        if !self.seeded {
            self.seeded = true;
            self.floor = revision.saturating_sub(1);
        }
        let need = 2 + payload.len();
        if payload.len() > u16::MAX as usize || need > MUTATION_TAIL_BYTES {
            // Too large to hold: nothing at or below it is complete.
            self.len = 0;
            self.floor = self.floor.max(revision);
            return;
        }
        while self.len + need > MUTATION_TAIL_BYTES {
            self.evict_oldest();
        }
        self.buf[self.len..self.len + 2].copy_from_slice(&(payload.len() as u16).to_le_bytes());
        self.buf[self.len + 2..self.len + need].copy_from_slice(payload);
        self.len += need;
    }

    fn evict_oldest(&mut self) {
        let n = u16::from_le_bytes([self.buf[0], self.buf[1]]) as usize;
        let at = (2 + n).min(self.len);
        self.floor = self.floor.max(Self::revision_of(&self.buf[2..at]));
        self.buf.copy_within(at..self.len, 0);
        self.len -= at;
    }

    fn revision_of(payload: &[u8]) -> i64 {
        i64::from_le_bytes(arr8(payload, 2))
    }

    /// The held mutation at offset `at` (0 for the oldest) as
    /// `(revision, payload, offset of the next)`; `None` past the newest.
    pub fn entry(&self, at: usize) -> Option<(i64, &[u8], usize)> {
        if at + 2 > self.len {
            return None;
        }
        let n = u16::from_le_bytes([self.buf[at], self.buf[at + 1]]) as usize;
        let p = self.buf.get(at + 2..at + 2 + n)?;
        Some((Self::revision_of(p), p, at + 2 + n))
    }

    /// Call `f(revision, payload)` for each held mutation above `from`, in
    /// the order read.
    pub fn for_each_above<F: FnMut(i64, &[u8])>(&self, from: i64, mut f: F) {
        let mut at = 0usize;
        while let Some((rev, p, next)) = self.entry(at) {
            if rev > from {
                f(rev, p);
            }
            at = next;
        }
    }
}

impl Default for MutationTail {
    fn default() -> Self {
        Self::new()
    }
}

// ── Replay planning ───────────────────────────────────────────────────

/// The revision interval `(from_revision, to_revision]` a watch would
/// need replayed to reach `to_revision` from what its client has
/// ([`replay_plan`]). The registry plans its replays through
/// [`WatchRecord::enter_service`] and [`catch_up_step`] instead, bounded
/// at its own mutation head; this is the record-only form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayPlan {
    pub watch_id: u64,
    pub from_revision: i64,
    pub to_revision: i64,
    pub session_epoch: u32,
}

/// The replay that takes `watch_id` from its `last_sent_revision` to
/// `current_revision`, the latest committed revision. A watch already
/// there (or past it) gets the empty interval `(current, current]` — the
/// caller skips the replay and tails live. `None` for an unknown watch.
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
