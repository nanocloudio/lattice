//! Pure-logic per-kpg compaction-floor aggregator, and the MVCC GC
//! floor decision machine — RFC database foundation §18, §21 inv. 13.
//!
//! Dual-target: PIC `compaction_coordinator` mounts this; host tests
//! run the aggregator under `cargo test`.
//!
//! # Two floors, one file, deliberately different rules
//!
//! The first half ([`CompactionTable`]) is the pre-existing advisory
//! aggregator: sources declare numbers, the coordinator takes a min and
//! forwards it. It is a hint. Nothing irreversible hangs off it.
//!
//! The second half ([`ClaimTable`]) is the MVCC GC floor, and it is a
//! different kind of object because reclaiming history cannot be
//! undone. §18:
//!
//! > The compaction coordinator collects signed/source-fenced claims,
//! > calculates a candidate floor, **proposes the durable floor
//! > transition, and only then schedules physical reclamation. Missing
//! > or stale claim sources block unsafe advancement.**
//!
//! Three rules follow, and this file makes all three executable:
//!
//! 1. **The floor is never advanced locally.** [`ClaimTable::
//!    plan_propose`] produces a record to propose and moves to
//!    `AwaitingCommit`; only [`ClaimTable::observe_committed_floor`]
//!    moves `committed_floor`. A caller that reclaims on the strength
//!    of a proposal has broken invariant 13, and no method here returns
//!    anything that would let it.
//! 2. **Fail closed on a missing or stale claim.** A required source
//!    that has never spoken, or whose claim is past its expiry, yields
//!    [`FloorBlocked`] — NOT "claims nothing, advance freely". Silence
//!    is the dangerous case: a crashed watch worker looks exactly like
//!    a watch worker with no cursors to protect, and only one of those
//!    is safe.
//! 3. **The candidate is clamped to the allocator's COMMITTED high
//!    water** and never derived from a leader-local issued watermark.
//!    `modules/common/mvcc.rs` keeps `committed_high_water` (advanced
//!    only by committed records) separate from `issued_high_water`
//!    (how far the current leader has handed out) precisely so this
//!    clamp has something safe to read. Using the issued watermark
//!    would let a leader that had issued past its committed reserve
//!    authorize the destruction of versions a surviving reader was
//!    promised — the reserve can be lost by a leadership change, the
//!    reclamation cannot.

#![allow(dead_code, reason = "consumed by both PIC and host paths")]

pub const MAX_SOURCES: usize = 4;
pub const MAX_KPG: usize = 32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct KpgFloor {
    pub kpg_id: u16,
    pub sources: [u64; MAX_SOURCES],
    pub last_emitted: u64,
    pub seen: bool,
}

impl KpgFloor {
    pub const fn empty() -> Self {
        Self {
            kpg_id: 0,
            sources: [u64::MAX; MAX_SOURCES],
            last_emitted: 0,
            seen: false,
        }
    }
}

#[repr(C)]
pub struct CompactionTable {
    pub floors: [KpgFloor; MAX_KPG],
    pub len: u16,
}

impl CompactionTable {
    pub fn init(&mut self) {
        let mut i = 0;
        while i < MAX_KPG {
            self.floors[i] = KpgFloor::empty();
            i += 1;
        }
        self.len = 0;
    }
}

pub fn find_or_alloc(t: &mut CompactionTable, kpg_id: u16) -> Option<usize> {
    let len = t.len as usize;
    let mut i = 0;
    while i < len {
        if t.floors[i].kpg_id == kpg_id {
            return Some(i);
        }
        i += 1;
    }
    if len >= MAX_KPG {
        return None;
    }
    t.floors[len] = KpgFloor::empty();
    t.floors[len].kpg_id = kpg_id;
    t.len += 1;
    Some(len)
}

pub fn aggregate(f: &KpgFloor) -> Option<u64> {
    if !f.seen {
        return None;
    }
    let mut m = u64::MAX;
    let mut i = 0;
    while i < MAX_SOURCES {
        if f.sources[i] < m {
            m = f.sources[i];
        }
        i += 1;
    }
    if m == u64::MAX {
        None
    } else {
        Some(m)
    }
}

/// Apply a `(source, kpg, floor)` and return
/// `Some((kpg, aggregated_floor))` when the per-kpg aggregate changed.
pub fn apply(
    t: &mut CompactionTable,
    source: u8,
    kpg_id: u16,
    declared: u64,
) -> Option<(u16, u64)> {
    let src = source as usize;
    if src >= MAX_SOURCES {
        return None;
    }
    let idx = find_or_alloc(t, kpg_id)?;
    t.floors[idx].sources[src] = declared;
    t.floors[idx].seen = true;
    let agg = aggregate(&t.floors[idx])?;
    if agg != t.floors[idx].last_emitted {
        t.floors[idx].last_emitted = agg;
        Some((kpg_id, agg))
    } else {
        None
    }
}

// ── MVCC GC floor: claims, records, and the decision machine ─────────

/// Version of the [`RetentionClaim`] wire encoding. Bump on ANY layout
/// change; decoders fail closed on an unknown version.
pub const GC_CLAIM_VERSION: u16 = 1;
/// Fixed wire length of one encoded [`RetentionClaim`].
pub const GC_CLAIM_WIRE_LEN: usize = 40;

/// Version of the [`GcFloorRecord`] wire encoding. This one is a
/// REPLICATED, DURABLE format — a change is a format break requiring a
/// version bump plus an explicit migration, never a silent edit.
pub const GC_FLOOR_VERSION: u16 = 1;
/// Fixed wire length of one encoded [`GcFloorRecord`].
pub const GC_FLOOR_WIRE_LEN: usize = 32;

// ── Claim sources (§18's claim set, single-range subset) ─────────────
//
// §18 lists seven claim categories. Phase-2 slice B implements the two
// that exist today and RESERVES the slots for the rest, so adding one
// later is a mask bit and a producer — not a re-layout. The slot index
// IS the wire `source` byte; never renumber.

/// Reads in flight and unresolved intents: the oldest revision any
/// active reader on this range still needs. Required.
pub const CLAIM_SOURCE_ACTIVE_READ: u8 = 0;
/// Configured operator retention / legal hold. Required — an operator
/// floor of 0 is a real, expressible answer ("retain everything"), so
/// its ABSENCE must never be read as that.
pub const CLAIM_SOURCE_OPERATOR: u8 = 1;
/// Watch and change-feed cursors (§15). Reserved for slice C — the
/// watch registry produces it once resume-from-history lands.
pub const CLAIM_SOURCE_WATCH: u8 = 2;
/// Backups, restores and application snapshots still eligible for
/// restore (§17). Reserved.
pub const CLAIM_SOURCE_BACKUP: u8 = 3;

/// Bit set of sources that MUST have a fresh claim before the floor may
/// advance. Everything not in the mask is optional: if it speaks its
/// claim counts, and if it is silent it is genuinely absent from the
/// deployment rather than merely quiet.
pub const fn source_bit(source: u8) -> u8 {
    1u8 << source
}

/// Default required set: the two sources that exist in slice B.
pub const DEFAULT_REQUIRED_SOURCES: u8 =
    source_bit(CLAIM_SOURCE_ACTIVE_READ) | source_bit(CLAIM_SOURCE_OPERATOR);

/// One fenced retention claim (`MSG_RETENTION_CLAIM`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionClaim {
    pub kpg_id: u16,
    pub source: u8,
    /// Holder identity within `source`. A source aggregates its own
    /// holders and republishes one claim; `claim_id` changing means a
    /// new holder took over and the `seq` monotonicity restarts.
    pub claim_id: u64,
    /// Oldest revision this source still needs retained.
    pub floor_revision: u64,
    /// Freshness bound, unix ms. `0` = never expires (a static
    /// operator floor). Non-zero and passed ⇒ the claim is STALE and
    /// blocks advancement.
    pub expiry_unix_ms: u64,
    /// Monotone within one `claim_id`; an older `seq` is dropped so a
    /// reordered or replayed frame cannot walk a claim backwards.
    pub seq: u32,
}

impl RetentionClaim {
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < GC_CLAIM_WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&GC_CLAIM_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((GC_CLAIM_WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..6].copy_from_slice(&self.kpg_id.to_le_bytes());
        out[6] = self.source;
        out[7] = 0;
        out[8..16].copy_from_slice(&self.claim_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.floor_revision.to_le_bytes());
        out[24..32].copy_from_slice(&self.expiry_unix_ms.to_le_bytes());
        out[32..36].copy_from_slice(&self.seq.to_le_bytes());
        out[36..40].copy_from_slice(&0u32.to_le_bytes());
        Some(GC_CLAIM_WIRE_LEN)
    }

    /// Parse exactly one claim. Fails closed on any other length, an
    /// unknown version, a declared length that disagrees with the fixed
    /// layout, or a source outside the slot table.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != GC_CLAIM_WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes([src[0], src[1]]) != GC_CLAIM_VERSION {
            return None;
        }
        if u16::from_le_bytes([src[2], src[3]]) as usize != GC_CLAIM_WIRE_LEN - 4 {
            return None;
        }
        let source = src[6];
        if source as usize >= MAX_SOURCES {
            return None;
        }
        Some(Self {
            kpg_id: u16::from_le_bytes([src[4], src[5]]),
            source,
            claim_id: u64::from_le_bytes([
                src[8], src[9], src[10], src[11], src[12], src[13], src[14], src[15],
            ]),
            floor_revision: u64::from_le_bytes([
                src[16], src[17], src[18], src[19], src[20], src[21], src[22], src[23],
            ]),
            expiry_unix_ms: u64::from_le_bytes([
                src[24], src[25], src[26], src[27], src[28], src[29], src[30], src[31],
            ]),
            seq: u32::from_le_bytes([src[32], src[33], src[34], src[35]]),
        })
    }
}

/// The replicated GC floor record, identical on the propose
/// (`MSG_GC_FLOOR_PROPOSE`) and committed (`MSG_GC_FLOOR_COMMITTED`)
/// paths.
///
/// Wire layout (`GC_FLOOR_VERSION` 1, all integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]     // 0..2, 2..4   payload_len = 28
/// [kpg_id:u16][rsvd:u16]             // 4..8
/// [floor_revision:u64]               // 8..16
/// [committed_high_water:u64]         // 16..24
/// [proposer_id:u32]                  // 24..28
/// [claim_generation:u32]             // 28..32
/// ```
///
/// `committed_high_water` is carried in the record — not merely used to
/// derive `floor_revision` — so the durable history says WHICH
/// allocator position the decision was clamped against. Without it a
/// future reader cannot tell a floor that respected the clamp from one
/// that was computed some other way, and invariant 13 would be
/// unauditable after the fact.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcFloorRecord {
    pub kpg_id: u16,
    pub floor_revision: u64,
    pub committed_high_water: u64,
    pub proposer_id: u32,
    /// Bumped by the proposer on every distinct proposal, so a stale
    /// committed record from a superseded proposal is identifiable.
    pub claim_generation: u32,
}

impl GcFloorRecord {
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < GC_FLOOR_WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&GC_FLOOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((GC_FLOOR_WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..6].copy_from_slice(&self.kpg_id.to_le_bytes());
        out[6..8].copy_from_slice(&0u16.to_le_bytes());
        out[8..16].copy_from_slice(&self.floor_revision.to_le_bytes());
        out[16..24].copy_from_slice(&self.committed_high_water.to_le_bytes());
        out[24..28].copy_from_slice(&self.proposer_id.to_le_bytes());
        out[28..32].copy_from_slice(&self.claim_generation.to_le_bytes());
        Some(GC_FLOOR_WIRE_LEN)
    }

    /// Parse exactly one record. Fails closed on any other length, an
    /// unknown version, a disagreeing declared length, or a floor above
    /// the high water it claims to have been clamped against (that
    /// combination is self-contradictory, so it cannot be trusted to
    /// authorize reclamation).
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != GC_FLOOR_WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes([src[0], src[1]]) != GC_FLOOR_VERSION {
            return None;
        }
        if u16::from_le_bytes([src[2], src[3]]) as usize != GC_FLOOR_WIRE_LEN - 4 {
            return None;
        }
        let rec = Self {
            kpg_id: u16::from_le_bytes([src[4], src[5]]),
            floor_revision: u64::from_le_bytes([
                src[8], src[9], src[10], src[11], src[12], src[13], src[14], src[15],
            ]),
            committed_high_water: u64::from_le_bytes([
                src[16], src[17], src[18], src[19], src[20], src[21], src[22], src[23],
            ]),
            proposer_id: u32::from_le_bytes([src[24], src[25], src[26], src[27]]),
            claim_generation: u32::from_le_bytes([src[28], src[29], src[30], src[31]]),
        };
        if rec.floor_revision > rec.committed_high_water {
            return None;
        }
        Some(rec)
    }
}

/// Why a floor advance could not be computed. Every variant is a
/// refusal to advance; there is no variant that means "advance
/// anyway". A caller that receives one holds the floor where it is,
/// which is always safe — history is merely retained longer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FloorBlocked {
    /// No claim table entry for this kpg yet.
    UnknownKpg,
    /// A REQUIRED source has never published a claim. Fail closed:
    /// silence is indistinguishable from a crashed holder.
    SourceMissing { source: u8 },
    /// A REQUIRED source's claim is past its expiry. Same reasoning —
    /// a stale claim proves the holder stopped reporting, not that it
    /// stopped needing history.
    SourceStale { source: u8 },
    /// No committed allocator high water has been observed, so there is
    /// nothing safe to clamp against (§10 / `mvcc.rs`).
    AllocatorUnestablished,
    /// The candidate does not move the floor forward. Not an error —
    /// just nothing to propose.
    NoAdvance { candidate: u64, committed: u64 },
    /// A proposal for this kpg is already outstanding; one at a time,
    /// exactly like the allocator's reserve advance.
    ProposalOutstanding,
}

/// One source's slot in a kpg's claim set.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ClaimSlot {
    pub claim_id: u64,
    pub floor_revision: u64,
    pub expiry_unix_ms: u64,
    pub seq: u32,
    pub present: bool,
    pub _pad: [u8; 3],
}

impl ClaimSlot {
    pub const fn empty() -> Self {
        Self {
            claim_id: 0,
            floor_revision: 0,
            expiry_unix_ms: 0,
            seq: 0,
            present: false,
            _pad: [0; 3],
        }
    }

    /// A claim with a non-zero expiry that `now_ms` has reached is
    /// stale. Freshness only; never an ordering input.
    pub fn is_stale(&self, now_ms: u64) -> bool {
        self.expiry_unix_ms != 0 && now_ms >= self.expiry_unix_ms
    }
}

/// Per-kpg claim set plus the floor decision state.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KpgClaims {
    pub kpg_id: u16,
    pub seen: bool,
    pub awaiting_commit: bool,
    pub claim_generation: u32,
    /// The floor last observed as COMMITTED. The only value a caller
    /// may reclaim behind.
    pub committed_floor: u64,
    /// The floor of the outstanding proposal, if any. Grants nothing.
    pub proposed_floor: u64,
    pub slots: [ClaimSlot; MAX_SOURCES],
}

impl KpgClaims {
    pub const fn empty() -> Self {
        Self {
            kpg_id: 0,
            seen: false,
            awaiting_commit: false,
            claim_generation: 0,
            committed_floor: 0,
            proposed_floor: 0,
            slots: [ClaimSlot::empty(); MAX_SOURCES],
        }
    }
}

/// The GC floor decision machine for up to [`MAX_KPG`] ranges.
#[repr(C)]
pub struct ClaimTable {
    pub entries: [KpgClaims; MAX_KPG],
    pub len: u16,
    /// Bit set of sources that must be fresh — see [`source_bit`].
    pub required_sources: u8,
    pub high_water_established: bool,
    /// The timestamp allocator's COMMITTED high water (`mvcc.rs`
    /// `ReserveState::committed_high_water`, i.e. the `interval_end` of
    /// a committed `MSG_TS_LEASE`). Monotone. **Never** the issued
    /// watermark — see the module docs, rule 3.
    pub committed_high_water: u64,
}

impl ClaimTable {
    pub fn init(&mut self, required_sources: u8) {
        let mut i = 0;
        while i < MAX_KPG {
            self.entries[i] = KpgClaims::empty();
            i += 1;
        }
        self.len = 0;
        self.required_sources = required_sources;
        self.high_water_established = false;
        self.committed_high_water = 0;
    }

    fn find(&self, kpg_id: u16) -> Option<usize> {
        let len = self.len as usize;
        let mut i = 0;
        while i < len {
            if self.entries[i].kpg_id == kpg_id {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    fn find_or_alloc(&mut self, kpg_id: u16) -> Option<usize> {
        if let Some(i) = self.find(kpg_id) {
            return Some(i);
        }
        let len = self.len as usize;
        if len >= MAX_KPG {
            return None;
        }
        self.entries[len] = KpgClaims::empty();
        self.entries[len].kpg_id = kpg_id;
        self.entries[len].seen = true;
        self.len += 1;
        Some(len)
    }

    /// Fold one COMMITTED allocator high water in. Monotone: a lower
    /// value is ignored rather than applied, because the clamp may only
    /// ever be as permissive as the most durable thing we have seen.
    pub fn observe_high_water(&mut self, committed_high_water: u64) {
        if committed_high_water > self.committed_high_water {
            self.committed_high_water = committed_high_water;
        }
        self.high_water_established = true;
    }

    /// Record one claim. Returns `false` when the claim is dropped:
    /// an unknown source, a full table, or a `seq` that does not move
    /// forward within the same `claim_id` (a replayed or reordered
    /// frame must not walk a claim backwards).
    pub fn observe_claim(&mut self, claim: &RetentionClaim) -> bool {
        let src = claim.source as usize;
        if src >= MAX_SOURCES {
            return false;
        }
        let Some(idx) = self.find_or_alloc(claim.kpg_id) else {
            return false;
        };
        let slot = &mut self.entries[idx].slots[src];
        if slot.present && slot.claim_id == claim.claim_id && claim.seq < slot.seq {
            return false;
        }
        slot.present = true;
        slot.claim_id = claim.claim_id;
        slot.floor_revision = claim.floor_revision;
        slot.expiry_unix_ms = claim.expiry_unix_ms;
        slot.seq = claim.seq;
        true
    }

    /// The floor the claims would permit, or why they permit nothing.
    ///
    /// Order of checks is the order of severity, so the reported reason
    /// names the most fundamental problem: required sources first (a
    /// missing claim means we do not KNOW the answer), then the
    /// allocator clamp (we know the claims but have no safe ceiling),
    /// then whether it actually moves.
    pub fn candidate_floor(&self, kpg_id: u16, now_ms: u64) -> Result<u64, FloorBlocked> {
        let Some(idx) = self.find(kpg_id) else {
            return Err(FloorBlocked::UnknownKpg);
        };
        let e = &self.entries[idx];
        let mut s = 0usize;
        while s < MAX_SOURCES {
            if self.required_sources & source_bit(s as u8) != 0 {
                let slot = &e.slots[s];
                if !slot.present {
                    return Err(FloorBlocked::SourceMissing { source: s as u8 });
                }
                if slot.is_stale(now_ms) {
                    return Err(FloorBlocked::SourceStale { source: s as u8 });
                }
            }
            s += 1;
        }
        if !self.high_water_established {
            return Err(FloorBlocked::AllocatorUnestablished);
        }
        // Min over every claim that is present AND fresh. An optional
        // source that has gone stale still constrains us to whatever it
        // last asked for: dropping a stale claim's floor would make
        // staleness *raise* the floor, which is backwards.
        //
        // Tracked with an explicit `any` flag rather than a `u64::MAX`
        // sentinel: `u64::MAX` is a legitimate claim value ("I need
        // nothing retained"), and conflating it with "no claims at all"
        // would turn the most permissive claim into a hard block.
        let mut candidate = u64::MAX;
        let mut any = false;
        let mut s = 0usize;
        while s < MAX_SOURCES {
            let slot = &e.slots[s];
            if slot.present {
                any = true;
                if slot.floor_revision < candidate {
                    candidate = slot.floor_revision;
                }
            }
            s += 1;
        }
        if !any {
            // No claims at all and none required: there is no evidence
            // to advance on. Hold.
            return Err(FloorBlocked::SourceMissing { source: 0 });
        }
        // Rule 3: clamp to the COMMITTED allocator high water.
        if candidate > self.committed_high_water {
            candidate = self.committed_high_water;
        }
        if candidate <= e.committed_floor {
            return Err(FloorBlocked::NoAdvance {
                candidate,
                committed: e.committed_floor,
            });
        }
        Ok(candidate)
    }

    /// Build the record to PROPOSE. Marks the kpg `awaiting_commit`;
    /// the returned record is deliberately NOT applied to
    /// `committed_floor` — only [`observe_committed_floor`](Self::
    /// observe_committed_floor) does that, and only when consensus
    /// hands the record back.
    pub fn plan_propose(
        &mut self,
        kpg_id: u16,
        now_ms: u64,
        proposer_id: u32,
    ) -> Result<GcFloorRecord, FloorBlocked> {
        let candidate = self.candidate_floor(kpg_id, now_ms)?;
        let high_water = self.committed_high_water;
        let Some(idx) = self.find(kpg_id) else {
            return Err(FloorBlocked::UnknownKpg);
        };
        if self.entries[idx].awaiting_commit {
            return Err(FloorBlocked::ProposalOutstanding);
        }
        let e = &mut self.entries[idx];
        e.claim_generation = e.claim_generation.wrapping_add(1);
        e.proposed_floor = candidate;
        e.awaiting_commit = true;
        Ok(GcFloorRecord {
            kpg_id,
            floor_revision: candidate,
            committed_high_water: high_water,
            proposer_id,
            claim_generation: e.claim_generation,
        })
    }

    /// Fold a COMMITTED floor record in. This is the ONLY path that
    /// advances `committed_floor`, and therefore the only thing that
    /// can ever authorize physical reclamation.
    ///
    /// Returns `Some(floor)` when the committed floor moved forward.
    /// A record at-or-below the current floor is accepted and ignored
    /// (floors never retreat); a record for an unknown kpg establishes
    /// the entry, because a committed decision is authoritative even
    /// for a coordinator that has just started and has no claims yet.
    pub fn observe_committed_floor(&mut self, rec: &GcFloorRecord) -> Option<u64> {
        if rec.floor_revision > rec.committed_high_water {
            return None; // self-contradictory; never trust it
        }
        let idx = self.find_or_alloc(rec.kpg_id)?;
        let e = &mut self.entries[idx];
        // Our own outstanding proposal is settled by any committed
        // record at-or-above it: either it committed, or a different
        // one superseded it. Either way we are free to propose again.
        if e.awaiting_commit && rec.floor_revision >= e.proposed_floor {
            e.awaiting_commit = false;
            e.proposed_floor = 0;
        }
        if rec.floor_revision > e.committed_floor {
            e.committed_floor = rec.floor_revision;
            return Some(e.committed_floor);
        }
        None
    }

    /// The floor a caller may reclaim behind for `kpg_id`: the last
    /// COMMITTED one, and `0` (reclaim nothing) if there has never been
    /// one. There is deliberately no accessor for the proposed floor.
    pub fn committed_floor(&self, kpg_id: u16) -> u64 {
        match self.find(kpg_id) {
            Some(i) => self.entries[i].committed_floor,
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> CompactionTable {
        let mut t = CompactionTable {
            floors: [KpgFloor::empty(); MAX_KPG],
            len: 0,
        };
        t.init();
        t
    }

    #[test]
    fn first_declaration_emits_floor() {
        let mut t = fresh();
        assert_eq!(apply(&mut t, 0, 1, 100), Some((1, 100)));
    }

    #[test]
    fn aggregate_takes_min_across_sources() {
        let mut t = fresh();
        apply(&mut t, 0, 1, 100);
        assert_eq!(apply(&mut t, 1, 1, 50), Some((1, 50)));
    }

    #[test]
    fn raising_min_source_raises_aggregate() {
        let mut t = fresh();
        apply(&mut t, 0, 1, 100);
        apply(&mut t, 1, 1, 50);
        assert_eq!(apply(&mut t, 1, 1, 200), Some((1, 100)));
    }

    #[test]
    fn duplicate_aggregate_suppressed() {
        let mut t = fresh();
        apply(&mut t, 0, 1, 100);
        assert_eq!(apply(&mut t, 0, 1, 100), None);
    }

    #[test]
    fn distinct_kpgs_independent() {
        let mut t = fresh();
        apply(&mut t, 0, 1, 100);
        apply(&mut t, 0, 2, 200);
        assert_eq!(apply(&mut t, 1, 1, 50), Some((1, 50)));
        assert_eq!(apply(&mut t, 1, 2, 300), None);
    }
}
