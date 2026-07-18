//! Partition-map contracts — RFC database foundation §11.
//!
//! Versioned, bounded, `no_std` schemas for range descriptors and the
//! two explicit routing forms (§11.2): the ordered range map and the
//! hash-slot map. This file is contracts only — no routing wiring, no
//! module behavior. Everything is fixed-layout, fixed-capacity, and
//! fail-closed, in the Phase 0 style of `internal_key.rs` /
//! `db_context.rs`.
//!
//! Design rules encoded here:
//!
//! - **Logical identity outlives physical placement** (§11.3):
//!   `range_id` is wide and globally unique; the Clustor
//!   `partition_id` is a cluster-local physical binding fenced by
//!   `partition_incarnation` and `placement_epoch`.
//! - **The routing form is declared, never inferred** (§11.2, §11.4):
//!   a keyspace's routing kind comes from its `KeyspaceRouting`
//!   record, not from sniffing key bytes.
//! - **Ranges are half-open `[start_key, end_key)`** over USER key
//!   bounds (§5). An empty `start_key` means MIN; an empty `end_key`
//!   means MAX.
//! - **Invariants 4 and 5 (§21) are executable**: an ordered range map
//!   validates gap-free, non-overlapping, ordered, MIN..MAX coverage
//!   by all-Active descriptors, returning a typed violation naming the
//!   offending index.
//! - **Stale routing cannot mutate current state** (§21 invariant 6):
//!   checked lookups fence on descriptor generation and routing epoch
//!   and return a typed error that maps onto
//!   `db_context::RetryClass::StaleRoute`.
//!
//! Golden vectors: `tests/contract_partition_map.rs`. Changing them is
//! a persistent-format break and requires a version bump plus an
//! explicit migration — never an update to the vector.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

#[path = "db_context.rs"]
mod db_context;

pub use db_context::RetryClass;

/// Version of the range-descriptor wire encoding. Bump on ANY layout
/// change.
pub const RANGE_DESCRIPTOR_VERSION: u16 = 1;
/// Version of the `KeyspaceRouting` mini-record wire encoding.
pub const KEYSPACE_ROUTING_VERSION: u16 = 1;

/// Maximum length of one user-key range bound, bytes.
pub const MAX_KEY_BOUND_LEN: usize = 256;
/// Maximum replicas per range (§11.5: three voters default, five max;
/// learners/observers fit within the same bound in transition states).
pub const MAX_REPLICAS: usize = 5;
/// Fixed capacity of one `OrderedRangeMap`.
pub const MAX_RANGES: usize = 64;
/// Fixed logical hash-slot count. `u16` slots sized to match the
/// existing `types::KpgId` (`u16`) hash-routing scale: a comfortable
/// multiple of the partition-group counts pi5-class clusters run,
/// while keeping the whole binding array bounded (~10 KiB) and slot
/// arithmetic a single mask-free modulo.
pub const SLOT_COUNT: usize = 1024;

// ── Enums (fail closed on unknown discriminants) ──────────────────────

/// Declared routing form of a partition map (§11.2). Never inferred
/// from key bytes.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RoutingKind {
    /// Gap-free, non-overlapping ordered key intervals.
    OrderedRange = 1,
    /// Fixed logical hash slots mapped to partition groups.
    HashSlot = 2,
}

impl RoutingKind {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::OrderedRange),
            2 => Some(Self::HashSlot),
            _ => None,
        }
    }
}

/// Range lifecycle state (§12). Every transition is an explicit state
/// machine with generation fencing; routing serves only `Active`.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Lifecycle {
    Pending = 1,
    Active = 2,
    Splitting = 3,
    Merging = 4,
    Relocating = 5,
    Tombstoned = 6,
}

impl Lifecycle {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Pending),
            2 => Some(Self::Active),
            3 => Some(Self::Splitting),
            4 => Some(Self::Merging),
            5 => Some(Self::Relocating),
            6 => Some(Self::Tombstoned),
            _ => None,
        }
    }
}

/// Replica role (§11.5).
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplicaRole {
    /// Quorum participant.
    Voter = 1,
    /// Catch-up / relocation replica; no quorum vote.
    Learner = 2,
    /// Explicitly stale read-only replica; no quorum vote.
    Observer = 3,
}

impl ReplicaRole {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Voter),
            2 => Some(Self::Learner),
            3 => Some(Self::Observer),
            _ => None,
        }
    }
}

// ── Replica and physical binding ──────────────────────────────────────

/// One replica of a range (§11.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Replica {
    pub node_id: u32,
    pub role: ReplicaRole,
    pub failure_domain: u16,
}

impl Replica {
    pub const EMPTY: Replica = Replica {
        node_id: 0,
        role: ReplicaRole::Voter,
        failure_domain: 0,
    };
}

/// Physical partition binding (§11.3): the current Clustor placement of
/// a logical range. `partition_id` matches `types::KpgId` (`u16`);
/// `partition_incarnation` prevents delayed messages or files from a
/// previously reused physical ID from becoming valid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Binding {
    pub cluster_id: u32,
    pub partition_id: u16,
    pub partition_incarnation: u32,
    pub placement_epoch: u32,
}

impl Binding {
    pub const EMPTY: Binding = Binding {
        cluster_id: 0,
        partition_id: 0,
        partition_incarnation: 0,
        placement_epoch: 0,
    };
}

// ── Range descriptor ──────────────────────────────────────────────────

/// Versioned statement of range bounds, logical identity, physical
/// binding, replicas, and lifecycle (§11.3).
///
/// Wire layout (`RANGE_DESCRIPTOR_VERSION` 1, all integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [database_id:u32][partition_map_id:u32][routing_kind:u8]
/// [range_id:16]
/// [start_key_len:u16][start_key...][end_key_len:u16][end_key...]
/// [generation:u32][lifecycle:u8]
/// [cluster_id:u32][partition_id:u16][partition_incarnation:u32][placement_epoch:u32]
/// [replica_count:u8] replica_count × [node_id:u32][role:u8][failure_domain:u16]
/// [leaseholder:u32]
/// ```
///
/// `start_key`/`end_key` are USER key bounds, half-open `[start, end)`;
/// empty start = MIN, empty end = MAX. Decode fails closed on unknown
/// version, truncation, trailing bytes, unknown enum discriminants,
/// oversized key bounds, and replica counts outside `1..=MAX_REPLICAS`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeDescriptor {
    pub database_id: u32,
    pub partition_map_id: u32,
    pub routing_kind: RoutingKind,
    /// Wide, globally unique logical range identity (§11.3). Outlives
    /// every physical placement.
    pub range_id: [u8; 16],
    start_key: [u8; MAX_KEY_BOUND_LEN],
    start_key_len: u16,
    end_key: [u8; MAX_KEY_BOUND_LEN],
    end_key_len: u16,
    /// Descriptor generation; bumped by every lifecycle operation and
    /// fenced on every routed request (§11.2, §12).
    pub generation: u32,
    pub lifecycle: Lifecycle,
    pub binding: Binding,
    replica_count: u8,
    replicas: [Replica; MAX_REPLICAS],
    /// Node currently holding the range lease.
    pub leaseholder: u32,
}

/// Fixed wire overhead of a descriptor: header + every field except
/// the variable key-bound bytes and replica entries.
pub const DESCRIPTOR_FIXED_WIRE_LEN: usize = 4 + 25 + 2 + 2 + 5 + 14 + 1 + 4;
/// Wire length of one replica entry.
pub const REPLICA_WIRE_LEN: usize = 7;
/// Worst-case encoded descriptor length.
pub const DESCRIPTOR_MAX_WIRE_LEN: usize =
    DESCRIPTOR_FIXED_WIRE_LEN + 2 * MAX_KEY_BOUND_LEN + MAX_REPLICAS * REPLICA_WIRE_LEN;

impl RangeDescriptor {
    pub const EMPTY: RangeDescriptor = RangeDescriptor {
        database_id: 0,
        partition_map_id: 0,
        routing_kind: RoutingKind::OrderedRange,
        range_id: [0; 16],
        start_key: [0; MAX_KEY_BOUND_LEN],
        start_key_len: 0,
        end_key: [0; MAX_KEY_BOUND_LEN],
        end_key_len: 0,
        generation: 0,
        lifecycle: Lifecycle::Pending,
        binding: Binding::EMPTY,
        replica_count: 0,
        replicas: [Replica::EMPTY; MAX_REPLICAS],
        leaseholder: 0,
    };

    /// Build a descriptor. Fails closed on an oversized key bound or a
    /// replica set outside `1..=MAX_REPLICAS`.
    #[allow(
        clippy::too_many_arguments,
        reason = "contract constructor mirrors the §11.3 record shape one-to-one"
    )]
    pub fn new(
        database_id: u32,
        partition_map_id: u32,
        routing_kind: RoutingKind,
        range_id: [u8; 16],
        start_key: &[u8],
        end_key: &[u8],
        generation: u32,
        lifecycle: Lifecycle,
        binding: Binding,
        replicas: &[Replica],
        leaseholder: u32,
    ) -> Option<Self> {
        if start_key.len() > MAX_KEY_BOUND_LEN || end_key.len() > MAX_KEY_BOUND_LEN {
            return None;
        }
        if replicas.is_empty() || replicas.len() > MAX_REPLICAS {
            return None;
        }
        let mut d = Self::EMPTY;
        d.database_id = database_id;
        d.partition_map_id = partition_map_id;
        d.routing_kind = routing_kind;
        d.range_id = range_id;
        d.start_key[..start_key.len()].copy_from_slice(start_key);
        d.start_key_len = start_key.len() as u16;
        d.end_key[..end_key.len()].copy_from_slice(end_key);
        d.end_key_len = end_key.len() as u16;
        d.generation = generation;
        d.lifecycle = lifecycle;
        d.binding = binding;
        d.replicas[..replicas.len()].copy_from_slice(replicas);
        d.replica_count = replicas.len() as u8;
        d.leaseholder = leaseholder;
        Some(d)
    }

    /// USER-key lower bound (inclusive). Empty = MIN.
    pub fn start_key(&self) -> &[u8] {
        &self.start_key[..self.start_key_len as usize]
    }

    /// USER-key upper bound (exclusive). Empty = MAX.
    pub fn end_key(&self) -> &[u8] {
        &self.end_key[..self.end_key_len as usize]
    }

    pub fn replicas(&self) -> &[Replica] {
        &self.replicas[..self.replica_count as usize]
    }

    /// Half-open containment: `start_key <= key < end_key`, empty end
    /// = MAX.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.start_key() <= key && (self.end_key_len == 0 || key < self.end_key())
    }

    /// Exact encoded length of this descriptor.
    pub fn wire_len(&self) -> usize {
        DESCRIPTOR_FIXED_WIRE_LEN
            + self.start_key_len as usize
            + self.end_key_len as usize
            + self.replica_count as usize * REPLICA_WIRE_LEN
    }

    /// Serialize to the versioned wire form. Returns the encoded
    /// length, or `None` if `out` is too small (fail closed — never
    /// truncate).
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&RANGE_DESCRIPTOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        let mut n = 4;
        out[n..n + 4].copy_from_slice(&self.database_id.to_le_bytes());
        n += 4;
        out[n..n + 4].copy_from_slice(&self.partition_map_id.to_le_bytes());
        n += 4;
        out[n] = self.routing_kind as u8;
        n += 1;
        out[n..n + 16].copy_from_slice(&self.range_id);
        n += 16;
        out[n..n + 2].copy_from_slice(&self.start_key_len.to_le_bytes());
        n += 2;
        out[n..n + self.start_key_len as usize].copy_from_slice(self.start_key());
        n += self.start_key_len as usize;
        out[n..n + 2].copy_from_slice(&self.end_key_len.to_le_bytes());
        n += 2;
        out[n..n + self.end_key_len as usize].copy_from_slice(self.end_key());
        n += self.end_key_len as usize;
        out[n..n + 4].copy_from_slice(&self.generation.to_le_bytes());
        n += 4;
        out[n] = self.lifecycle as u8;
        n += 1;
        out[n..n + 4].copy_from_slice(&self.binding.cluster_id.to_le_bytes());
        n += 4;
        out[n..n + 2].copy_from_slice(&self.binding.partition_id.to_le_bytes());
        n += 2;
        out[n..n + 4].copy_from_slice(&self.binding.partition_incarnation.to_le_bytes());
        n += 4;
        out[n..n + 4].copy_from_slice(&self.binding.placement_epoch.to_le_bytes());
        n += 4;
        out[n] = self.replica_count;
        n += 1;
        for r in self.replicas() {
            out[n..n + 4].copy_from_slice(&r.node_id.to_le_bytes());
            n += 4;
            out[n] = r.role as u8;
            n += 1;
            out[n..n + 2].copy_from_slice(&r.failure_domain.to_le_bytes());
            n += 2;
        }
        out[n..n + 4].copy_from_slice(&self.leaseholder.to_le_bytes());
        n += 4;
        debug_assert_eq!(n, total);
        Some(n)
    }

    /// Parse the versioned wire form. `src` must be exactly one
    /// encoded descriptor. Fails closed on unknown version, declared-
    /// length mismatch, truncation, trailing bytes, unknown enum
    /// discriminants, oversized key bounds, or a replica count outside
    /// `1..=MAX_REPLICAS`.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < 4 {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != RANGE_DESCRIPTOR_VERSION {
            return None;
        }
        let payload_len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if src.len() != 4 + payload_len {
            return None; // truncated or trailing bytes
        }
        fn take<'a>(p: &'a [u8], n: &mut usize, k: usize) -> Option<&'a [u8]> {
            let s = p.get(*n..n.checked_add(k)?)?;
            *n += k;
            Some(s)
        }
        let p = &src[4..];
        let mut n = 0usize;
        let take = |n: &mut usize, k: usize| take(p, n, k);
        let database_id = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        let partition_map_id = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        let routing_kind = RoutingKind::from_u8(take(&mut n, 1)?[0])?;
        let range_id: [u8; 16] = take(&mut n, 16)?.try_into().ok()?;
        let start_key_len = u16::from_le_bytes(take(&mut n, 2)?.try_into().ok()?) as usize;
        if start_key_len > MAX_KEY_BOUND_LEN {
            return None;
        }
        let start_key = take(&mut n, start_key_len)?;
        let end_key_len = u16::from_le_bytes(take(&mut n, 2)?.try_into().ok()?) as usize;
        if end_key_len > MAX_KEY_BOUND_LEN {
            return None;
        }
        let end_key = take(&mut n, end_key_len)?;
        // Borrow of `p` ends here; copy bounds out before continuing.
        let mut skey = [0u8; MAX_KEY_BOUND_LEN];
        skey[..start_key_len].copy_from_slice(start_key);
        let mut ekey = [0u8; MAX_KEY_BOUND_LEN];
        ekey[..end_key_len].copy_from_slice(end_key);
        let generation = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        let lifecycle = Lifecycle::from_u8(take(&mut n, 1)?[0])?;
        let binding = Binding {
            cluster_id: u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?),
            partition_id: u16::from_le_bytes(take(&mut n, 2)?.try_into().ok()?),
            partition_incarnation: u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?),
            placement_epoch: u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?),
        };
        let replica_count = take(&mut n, 1)?[0] as usize;
        if replica_count == 0 || replica_count > MAX_REPLICAS {
            return None;
        }
        let mut replicas = [Replica::EMPTY; MAX_REPLICAS];
        for r in replicas.iter_mut().take(replica_count) {
            r.node_id = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
            r.role = ReplicaRole::from_u8(take(&mut n, 1)?[0])?;
            r.failure_domain = u16::from_le_bytes(take(&mut n, 2)?.try_into().ok()?);
        }
        let leaseholder = u32::from_le_bytes(take(&mut n, 4)?.try_into().ok()?);
        if n != payload_len {
            return None; // declared length disagrees with content
        }
        Some(Self {
            database_id,
            partition_map_id,
            routing_kind,
            range_id,
            start_key: skey,
            start_key_len: start_key_len as u16,
            end_key: ekey,
            end_key_len: end_key_len as u16,
            generation,
            lifecycle,
            binding,
            replica_count: replica_count as u8,
            replicas,
            leaseholder,
        })
    }
}

// ── Typed errors ──────────────────────────────────────────────────────

/// A §21 invariant-4/5 violation found by `OrderedRangeMap::validate`.
/// Each variant names the violation class and the offending descriptor
/// index (boundary violations report the index of the *second*
/// descriptor of the offending pair).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapViolation {
    /// The map has no descriptors; it cannot cover MIN..MAX.
    EmptyMap,
    /// First descriptor's start bound is not MIN (empty).
    FirstNotMin { index: usize },
    /// Last descriptor's end bound is not MAX (empty).
    LastNotMax { index: usize },
    /// Descriptor is not `Lifecycle::Active` (invariant 4: routing
    /// serves only active descriptors).
    NotActive { index: usize },
    /// Descriptor declares a routing kind other than `OrderedRange`.
    WrongKind { index: usize },
    /// `start_key >= end_key` (with a non-MAX end): an empty or
    /// inverted interval.
    InvertedBounds { index: usize },
    /// Descriptors are not in strictly ascending start-key order.
    NotOrdered { index: usize },
    /// Previous descriptor ends before this one starts: uncovered keys.
    Gap { index: usize },
    /// Previous descriptor ends after this one starts: doubly owned
    /// keys.
    Overlap { index: usize },
}

/// Typed routing-check failure. Every variant maps onto
/// `db_context::RetryClass::StaleRoute`: the caller's descriptor cache
/// is stale (or the map itself is mid-update) and must be refreshed
/// before retrying — stale metadata must never mutate current state
/// (§21 invariant 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteError {
    /// No active descriptor covers the key.
    NoRoute,
    /// Presented descriptor generation does not match the current one.
    StaleGeneration { current: u32, presented: u32 },
    /// Presented routing epoch does not match the map's epoch.
    StaleEpoch { current: u32, presented: u32 },
}

impl RouteError {
    /// The `RetryClass` this error maps onto — always `StaleRoute`.
    pub const fn retry_class(&self) -> RetryClass {
        match self {
            RouteError::NoRoute
            | RouteError::StaleGeneration { .. }
            | RouteError::StaleEpoch { .. } => RetryClass::StaleRoute,
        }
    }
}

// ── Ordered range map ─────────────────────────────────────────────────

/// Fixed-capacity ordered range map (§11.2): a sorted array of gap-
/// free, non-overlapping, all-Active descriptors covering MIN..MAX.
///
/// `validate()` makes §21 invariants 4 and 5 executable; `lookup`
/// assumes a validated map (it is still safe on an unvalidated one —
/// it just may return `None`).
pub struct OrderedRangeMap {
    /// Routing epoch fenced by `lookup_generation_checked` (§11.2:
    /// "the owning range validates every request's descriptor
    /// generation and routing epoch").
    pub routing_epoch: u32,
    len: usize,
    ranges: [RangeDescriptor; MAX_RANGES],
}

impl OrderedRangeMap {
    pub const fn new(routing_epoch: u32) -> Self {
        Self {
            routing_epoch,
            len: 0,
            ranges: [RangeDescriptor::EMPTY; MAX_RANGES],
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn ranges(&self) -> &[RangeDescriptor] {
        &self.ranges[..self.len]
    }

    /// Append a descriptor (callers append in ascending start-key
    /// order; `validate` enforces it). Fails closed at capacity.
    pub fn push(&mut self, d: RangeDescriptor) -> Option<()> {
        if self.len >= MAX_RANGES {
            return None;
        }
        self.ranges[self.len] = d;
        self.len += 1;
        Some(())
    }

    /// Enforce §21 invariants 4 and 5: every live key belongs to
    /// exactly one active descriptor; active descriptors are ordered,
    /// gap-free, and non-overlapping, and together cover MIN..MAX.
    pub fn validate(&self) -> Result<(), MapViolation> {
        if self.len == 0 {
            return Err(MapViolation::EmptyMap);
        }
        let ranges = self.ranges();
        for (index, d) in ranges.iter().enumerate() {
            if d.routing_kind != RoutingKind::OrderedRange {
                return Err(MapViolation::WrongKind { index });
            }
            if d.lifecycle != Lifecycle::Active {
                return Err(MapViolation::NotActive { index });
            }
            if d.end_key_len != 0 && d.start_key() >= d.end_key() {
                return Err(MapViolation::InvertedBounds { index });
            }
        }
        if !ranges[0].start_key().is_empty() {
            return Err(MapViolation::FirstNotMin { index: 0 });
        }
        let last = self.len - 1;
        if !ranges[last].end_key().is_empty() {
            return Err(MapViolation::LastNotMax { index: last });
        }
        for index in 1..self.len {
            let prev = &ranges[index - 1];
            let cur = &ranges[index];
            if cur.start_key() <= prev.start_key() {
                return Err(MapViolation::NotOrdered { index });
            }
            if prev.end_key().is_empty() {
                // prev covers to MAX but is not last: everything after
                // it is doubly owned.
                return Err(MapViolation::Overlap { index });
            }
            if prev.end_key() < cur.start_key() {
                return Err(MapViolation::Gap { index });
            }
            if prev.end_key() > cur.start_key() {
                return Err(MapViolation::Overlap { index });
            }
        }
        Ok(())
    }

    /// Binary-search the descriptor owning `key` (half-open
    /// `[start, end)` semantics). On a validated map this is always
    /// `Some`.
    pub fn lookup(&self, key: &[u8]) -> Option<&RangeDescriptor> {
        let ranges = self.ranges();
        // First index whose start_key is > key; the candidate owner is
        // the descriptor just before it.
        let idx = ranges.partition_point(|d| d.start_key() <= key);
        if idx == 0 {
            return None;
        }
        let d = &ranges[idx - 1];
        if d.contains(key) {
            Some(d)
        } else {
            None
        }
    }

    /// `lookup` plus the §11.2 fences: the presented routing epoch must
    /// match the map's and the presented generation must match the
    /// owning descriptor's. Any mismatch — behind OR ahead — is a
    /// stale route: the caller refreshes metadata and retries.
    pub fn lookup_generation_checked(
        &self,
        key: &[u8],
        expected_generation: u32,
        routing_epoch: u32,
    ) -> Result<&RangeDescriptor, RouteError> {
        if routing_epoch != self.routing_epoch {
            return Err(RouteError::StaleEpoch {
                current: self.routing_epoch,
                presented: routing_epoch,
            });
        }
        let d = self.lookup(key).ok_or(RouteError::NoRoute)?;
        if d.generation != expected_generation {
            return Err(RouteError::StaleGeneration {
                current: d.generation,
                presented: expected_generation,
            });
        }
        Ok(d)
    }

    /// The contiguous run of descriptor indices whose ranges intersect
    /// the half-open span `[start, end)` (empty `end` = MAX), as an
    /// inclusive index pair `(first, last)`.
    ///
    /// This is §20's "resolve all descriptors covering the span" step
    /// for a multi-range scan: on a validated map the returned run is
    /// gap-free by construction, so a scan that visits exactly these
    /// ranges in order visits every key of the span exactly once.
    /// `None` when the map is empty or no range intersects the span
    /// (an empty span: `start >= end`).
    pub fn covering(&self, start: &[u8], end: &[u8]) -> Option<(usize, usize)> {
        if self.len == 0 {
            return None;
        }
        if !end.is_empty() && start >= end {
            return None; // empty span
        }
        let ranges = self.ranges();
        // First range containing `start` — on a validated map that is
        // the last range whose start_key <= start.
        let first = ranges.partition_point(|d| d.start_key() <= start);
        if first == 0 {
            return None;
        }
        let first = first - 1;
        if !ranges[first].contains(start) {
            return None;
        }
        // Last range whose start_key < end (span end exclusive): a
        // range starting AT `end` contains no key below it.
        let last = if end.is_empty() {
            self.len - 1
        } else {
            let after = ranges.partition_point(|d| d.start_key() < end);
            // `after` >= first+1 here because ranges[first].start <=
            // start < end.
            after - 1
        };
        Some((first, last))
    }

    /// Replace this map with the contents of one `RANGE_MAP_UPDATE_V1`
    /// frame, atomically: the frame is fully decoded into a fresh map
    /// and `validate()`d BEFORE anything mutates, so a bad frame — a
    /// truncated descriptor, an unknown version, a map with a gap or an
    /// overlap — changes nothing. Full-map replacement rather than
    /// per-range patching, because the map's own invariant (gap-free
    /// MIN..MAX coverage) is a whole-map property: a partial patch
    /// could not be validated without the rest anyway.
    ///
    /// Returns the maximum descriptor generation in the new map, or
    /// `None` (fail closed, map untouched).
    pub fn apply_full_update(&mut self, src: &[u8]) -> Option<u32> {
        let mut fresh = OrderedRangeMap::new(self.routing_epoch);
        let mut max_generation = 0u32;
        let mut n = RANGE_MAP_UPDATE_HEADER_LEN;
        if src.len() < n {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != RANGE_MAP_UPDATE_VERSION {
            return None;
        }
        let count = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if count == 0 || count > MAX_RANGES {
            return None;
        }
        for _ in 0..count {
            // Each descriptor is self-delimiting: [version][payload_len].
            if src.len() < n + 4 {
                return None;
            }
            let payload_len = u16::from_le_bytes(src[n + 2..n + 4].try_into().ok()?) as usize;
            let total = 4 + payload_len;
            if src.len() < n + total {
                return None;
            }
            let d = RangeDescriptor::decode(&src[n..n + total])?;
            if d.generation > max_generation {
                max_generation = d.generation;
            }
            fresh.push(d)?;
            n += total;
        }
        if n != src.len() {
            return None; // trailing bytes
        }
        fresh.validate().ok()?;
        *self = fresh;
        Some(max_generation)
    }
}

// ── RANGE_MAP_UPDATE_V1 frame ─────────────────────────────────────────

/// Version of the full-ordered-map publication frame.
pub const RANGE_MAP_UPDATE_VERSION: u16 = 1;
/// Envelope message type carrying a `RANGE_MAP_UPDATE_V1` frame on the
/// KV router's `map_update` input port (sibling of [`MSG_MAP_UPDATE`],
/// same 0xE0 operational band). The two frame kinds share the port; the
/// envelope type byte says which map form a frame feeds — never the
/// bytes themselves (§11.2's declared-not-inferred rule, applied to the
/// control plane too).
pub const MSG_RANGE_MAP_UPDATE: u8 = 0xE3;
/// `[version:u16][count:u16]`, followed by `count` self-delimiting
/// encoded [`RangeDescriptor`]s.
pub const RANGE_MAP_UPDATE_HEADER_LEN: usize = 4;

/// Serialize a full-map `RANGE_MAP_UPDATE_V1` frame. Fails closed on an
/// empty batch, more than [`MAX_RANGES`] descriptors, or an undersized
/// `out`. The DESCRIPTOR ORDER of `descriptors` is the map order; the
/// applier validates it, so an unordered batch round-trips into a
/// rejected frame, not a reordered map.
pub fn encode_range_map_update(descriptors: &[RangeDescriptor], out: &mut [u8]) -> Option<usize> {
    if descriptors.is_empty() || descriptors.len() > MAX_RANGES {
        return None;
    }
    if out.len() < RANGE_MAP_UPDATE_HEADER_LEN {
        return None;
    }
    out[0..2].copy_from_slice(&RANGE_MAP_UPDATE_VERSION.to_le_bytes());
    out[2..4].copy_from_slice(&(descriptors.len() as u16).to_le_bytes());
    let mut n = RANGE_MAP_UPDATE_HEADER_LEN;
    for d in descriptors {
        n += d.encode(out.get_mut(n..)?)?;
    }
    Some(n)
}

// ── SPAN_BARRIER frame (§12 cutover barrier) ──────────────────────────

/// Envelope message type carrying a `SPAN_BARRIER_V1` frame on the KV
/// router's `map_update` port (0xE0 operational band, sibling of
/// [`MSG_MAP_UPDATE`] / [`MSG_RANGE_MAP_UPDATE`]).
///
/// §12.1 step 6's cutover barrier, in router form: while a barrier is
/// set, ordered-mode WRITES whose key falls in the half-open span
/// refuse with a retryable result — reads are untouched. The range
/// supervisor sets it before the final catch-up copy pass (so no
/// write can land in the span after the pass reads it) and clears it
/// at publication. Reject-and-retry rather than queue-and-hold:
/// bounded memory beats an unbounded parked-write pool, and the
/// barrier window is the copy tail, not the copy.
pub const MSG_SPAN_BARRIER: u8 = 0xE4;
/// Version of the span-barrier wire encoding.
pub const SPAN_BARRIER_VERSION: u16 = 1;

/// One decoded span barrier: `set` raises it, `!set` clears it.
/// Layout (`SPAN_BARRIER_VERSION` 1, integers LE):
///
/// ```text
/// [version:u16][set:u8]
/// [start_len:u16][start…][end_len:u16][end…]
/// ```
///
/// A clear must carry the SAME span it set — a mismatched clear is
/// dropped fail-closed rather than clearing "whatever is up", so a
/// delayed clear from an abandoned operation cannot lift a successor's
/// barrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanBarrier<'a> {
    pub set: bool,
    pub start: &'a [u8],
    pub end: &'a [u8],
}

impl<'a> SpanBarrier<'a> {
    /// Serialize. Fails closed on oversized bounds or a short buffer.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if self.start.len() > MAX_KEY_BOUND_LEN || self.end.len() > MAX_KEY_BOUND_LEN {
            return None;
        }
        let total = 3 + 2 + self.start.len() + 2 + self.end.len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&SPAN_BARRIER_VERSION.to_le_bytes());
        out[2] = u8::from(self.set);
        let mut n = 3;
        out[n..n + 2].copy_from_slice(&(self.start.len() as u16).to_le_bytes());
        n += 2;
        out[n..n + self.start.len()].copy_from_slice(self.start);
        n += self.start.len();
        out[n..n + 2].copy_from_slice(&(self.end.len() as u16).to_le_bytes());
        n += 2;
        out[n..n + self.end.len()].copy_from_slice(self.end);
        n += self.end.len();
        Some(n)
    }

    /// Parse. Fails closed on unknown version, truncation, trailing
    /// bytes, an oversized bound, or a set-byte outside {0, 1}.
    pub fn decode(src: &'a [u8]) -> Option<Self> {
        if src.len() < 3 {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != SPAN_BARRIER_VERSION {
            return None;
        }
        let set = match src[2] {
            0 => false,
            1 => true,
            _ => return None,
        };
        let slen = u16::from_le_bytes(src.get(3..5)?.try_into().ok()?) as usize;
        if slen > MAX_KEY_BOUND_LEN {
            return None;
        }
        let start = src.get(5..5 + slen)?;
        let eoff = 5 + slen;
        let elen = u16::from_le_bytes(src.get(eoff..eoff + 2)?.try_into().ok()?) as usize;
        if elen > MAX_KEY_BOUND_LEN {
            return None;
        }
        let end = src.get(eoff + 2..eoff + 2 + elen)?;
        if eoff + 2 + elen != src.len() {
            return None; // trailing bytes
        }
        Some(Self { set, start, end })
    }

    /// Half-open containment: is `key` inside the barred span?
    /// Empty `end` is unbounded above (same convention as descriptors).
    pub fn covers(&self, key: &[u8]) -> bool {
        self.start <= key && (self.end.is_empty() || key < self.end)
    }
}

// ── Multi-range scan-cursor tagging ───────────────────────────────────
//
// A resumable scan's cursor is provider-defined and provider-LOCAL: the
// memory provider's slot index, the disk provider's entry ordinal.
// Neither says which RANGE a multi-range scan should resume in, and the
// router must know that without holding per-scan state across client
// round trips. So the router tags the continuation into the cursor's
// top bytes: `[range_idx:u8][epoch_low:u8][provider_cursor:48]`. Both
// providers' cursors are tiny (a slot index below 1024; an ordinal
// bounded by span entry count), so 48 bits is not a practical limit —
// but the tagging still fails closed rather than truncating.
//
// The epoch byte is §20's "retries on generation change" made
// concrete: a continuation minted under one map must not resume under
// another, because the range INDEX it names may cover different keys
// there — resuming it would silently skip or repeat a stretch of the
// span. The router stamps the low byte of its routing epoch at mint
// and refuses a resume whose stamp disagrees (a stale route; the
// caller restarts the scan). One byte of a monotonically bumped epoch
// wraps at 256 — a scan would have to sleep across 256 map
// publications to alias, and the failure mode of the fence is a
// spurious restart, never a wrong result.
//
// The zero cursor stays the wire's "scan complete" sentinel: the
// tagger is only ever called when there IS more work, and the router
// keeps the sentinel unambiguous by minting epoch_low 0 as 256's
// congruent non-zero stamp never being needed for range 0/cursor 0 —
// concretely, `tag_scan_cursor(0, 0, 0) == 0` is documented and the
// router never mints it for a live continuation (a mid-range
// continuation has a non-zero provider cursor; a next-range
// continuation has a non-zero range index).

/// Bit position of the range-index byte inside a multi-range cursor.
pub const SCAN_CURSOR_RANGE_SHIFT: u32 = 56;
/// Bit position of the epoch stamp inside a multi-range cursor.
pub const SCAN_CURSOR_EPOCH_SHIFT: u32 = 48;
/// Largest provider cursor a tagged cursor can carry.
pub const SCAN_CURSOR_PROVIDER_MAX: u64 = (1u64 << SCAN_CURSOR_EPOCH_SHIFT) - 1;

/// Compose a multi-range scan cursor. Fails closed when the range index
/// exceeds a byte or the provider cursor exceeds 48 bits.
pub fn tag_scan_cursor(range_idx: usize, epoch_low: u8, provider_cursor: u64) -> Option<u64> {
    if range_idx > u8::MAX as usize || provider_cursor > SCAN_CURSOR_PROVIDER_MAX {
        return None;
    }
    Some(
        ((range_idx as u64) << SCAN_CURSOR_RANGE_SHIFT)
            | ((epoch_low as u64) << SCAN_CURSOR_EPOCH_SHIFT)
            | provider_cursor,
    )
}

/// Split a multi-range scan cursor into
/// `(range_idx, epoch_low, provider_cursor)`.
pub const fn split_scan_cursor(cursor: u64) -> (usize, u8, u64) {
    (
        (cursor >> SCAN_CURSOR_RANGE_SHIFT) as usize,
        (cursor >> SCAN_CURSOR_EPOCH_SHIFT) as u8,
        cursor & SCAN_CURSOR_PROVIDER_MAX,
    )
}

// ── Hash-slot map ─────────────────────────────────────────────────────

/// FNV-1a 64-bit hash.
///
/// MUST stay byte-for-byte identical to `kv_store.rs::fnv1a64` — the
/// KV router hashes keys with that copy, and any divergence would
/// silently split the keyspace between two hash functions. Duplicated
/// rather than imported because `kv_store.rs` is a state-store
/// provider file with independent ownership; this contract file must
/// not depend on it.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Physical binding of one logical hash slot (§11.4). Mirrors the
/// partition-binding fields of `Binding` that fence hash-routed
/// requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotBinding {
    /// Clustor physical partition (matches `types::KpgId`).
    pub partition_id: u16,
    /// Fences stale physical-ID reuse (§11.3).
    pub partition_incarnation: u32,
    /// Binding generation, fenced on every checked lookup.
    pub generation: u32,
}

impl SlotBinding {
    pub const EMPTY: SlotBinding = SlotBinding {
        partition_id: 0,
        partition_incarnation: 0,
        generation: 0,
    };
}

/// Fixed-slot hash map (§11.2, §11.4): the canonical key maps to one
/// of `SLOT_COUNT` logical slots, and each slot maps to a partition
/// group. Preserves the existing hash-to-KPG behavior for point
/// workloads that deliberately forgo ordered cross-partition scans.
pub struct HashSlotMap {
    /// Routing epoch fenced by `lookup_generation_checked`.
    pub routing_epoch: u32,
    slots: [SlotBinding; SLOT_COUNT],
}

impl HashSlotMap {
    pub const fn new(routing_epoch: u32) -> Self {
        Self {
            routing_epoch,
            slots: [SlotBinding::EMPTY; SLOT_COUNT],
        }
    }

    /// Deterministic slot for a canonical key: `fnv1a64(key) mod
    /// SLOT_COUNT`. Golden-locked in `tests/contract_partition_map.rs`
    /// — changing the hash or slot count is a placement-format break.
    pub fn slot_for_key(key: &[u8]) -> u16 {
        (fnv1a64(key) % SLOT_COUNT as u64) as u16
    }

    /// Bind one slot. Fails closed on an out-of-range slot.
    pub fn bind_slot(&mut self, slot: u16, binding: SlotBinding) -> Option<()> {
        let b = self.slots.get_mut(slot as usize)?;
        *b = binding;
        Some(())
    }

    pub fn slot_binding(&self, slot: u16) -> Option<&SlotBinding> {
        self.slots.get(slot as usize)
    }

    /// Unchecked lookup: the binding of the key's slot.
    pub fn lookup(&self, key: &[u8]) -> &SlotBinding {
        &self.slots[Self::slot_for_key(key) as usize]
    }

    /// `lookup` plus the same generation/epoch fences as the ordered
    /// map. Any mismatch is a stale route.
    pub fn lookup_generation_checked(
        &self,
        key: &[u8],
        expected_generation: u32,
        routing_epoch: u32,
    ) -> Result<&SlotBinding, RouteError> {
        self.lookup_slot_generation_checked(
            Self::slot_for_key(key),
            expected_generation,
            routing_epoch,
        )
    }

    /// Slot-indexed variant of `lookup_generation_checked`, for callers
    /// that already hold the key's FNV-1a hash (the KV router computes
    /// it for its per-conn dependency barrier; `hash % SLOT_COUNT` is
    /// exactly `slot_for_key`). Identical fence semantics: epoch first,
    /// then binding generation; any mismatch is a stale route. An
    /// out-of-range slot is `NoRoute`.
    pub fn lookup_slot_generation_checked(
        &self,
        slot: u16,
        expected_generation: u32,
        routing_epoch: u32,
    ) -> Result<&SlotBinding, RouteError> {
        if routing_epoch != self.routing_epoch {
            return Err(RouteError::StaleEpoch {
                current: self.routing_epoch,
                presented: routing_epoch,
            });
        }
        let b = self.slot_binding(slot).ok_or(RouteError::NoRoute)?;
        if b.generation != expected_generation {
            return Err(RouteError::StaleGeneration {
                current: b.generation,
                presented: expected_generation,
            });
        }
        Ok(b)
    }

    /// Apply one validated `MAP_UPDATE_V1` frame atomically. The whole
    /// frame is validated (version, count bounds, exact length, every
    /// slot in range) BEFORE any slot mutates — a bad frame changes
    /// nothing. Returns the maximum binding generation in the batch, or
    /// `None` (fail closed) on any validation failure.
    pub fn apply_update(&mut self, src: &[u8]) -> Option<u32> {
        let rd = MapUpdateReader::new(src)?;
        // Pre-validate every slot index before mutating anything.
        let mut i = 0;
        while i < rd.count() {
            if rd.entry(i).slot as usize >= SLOT_COUNT {
                return None;
            }
            i += 1;
        }
        let mut max_generation = 0u32;
        let mut i = 0;
        while i < rd.count() {
            let e = rd.entry(i);
            self.slots[e.slot as usize] = e.binding;
            if e.binding.generation > max_generation {
                max_generation = e.binding.generation;
            }
            i += 1;
        }
        Some(max_generation)
    }
}

// ── MAP_UPDATE_V1 frame ───────────────────────────────────────────────

/// Version of the slot-binding update-batch wire encoding. Bump on ANY
/// layout change; decoders fail closed on an unknown version.
pub const MAP_UPDATE_VERSION: u16 = 1;
/// Envelope message type carrying a `MAP_UPDATE_V1` frame on the KV
/// router's `map_update` input port. Allocated in the 0xE0 operational
/// band of `modules/common/wire.rs` (0xE0/0xE1 floors, 0xE5 applied
/// pos); defined here rather than there because the frame layout and
/// its discriminant are one contract.
pub const MSG_MAP_UPDATE: u8 = 0xE2;
/// `[version:u16][count:u16]`.
pub const MAP_UPDATE_HEADER_LEN: usize = 4;
/// `[slot:u16][partition:u16][incarnation:u32][generation:u32]`.
pub const MAP_UPDATE_ENTRY_LEN: usize = 12;
/// Bounded frames: at most this many slot rebinds per frame
/// (header + 256×12 = 3 076 bytes, inside every module's 4 KiB
/// scratch and the u16 envelope length).
pub const MAP_UPDATE_MAX_ENTRIES: usize = 256;
/// Worst-case encoded frame length.
pub const MAP_UPDATE_MAX_WIRE_LEN: usize =
    MAP_UPDATE_HEADER_LEN + MAP_UPDATE_MAX_ENTRIES * MAP_UPDATE_ENTRY_LEN;

/// One decoded `MAP_UPDATE_V1` entry: rebind `slot` to `binding`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MapUpdateEntry {
    pub slot: u16,
    pub binding: SlotBinding,
}

/// Serialize a `MAP_UPDATE_V1` frame (integers LE):
///
/// ```text
/// [version:u16][count:u16]
/// count × [slot:u16][partition:u16][incarnation:u32][generation:u32]
/// ```
///
/// Fails closed on an empty batch, a batch over
/// `MAP_UPDATE_MAX_ENTRIES`, or an undersized `out`.
pub fn encode_map_update(entries: &[MapUpdateEntry], out: &mut [u8]) -> Option<usize> {
    if entries.is_empty() || entries.len() > MAP_UPDATE_MAX_ENTRIES {
        return None;
    }
    let total = MAP_UPDATE_HEADER_LEN + entries.len() * MAP_UPDATE_ENTRY_LEN;
    if out.len() < total {
        return None;
    }
    out[0..2].copy_from_slice(&MAP_UPDATE_VERSION.to_le_bytes());
    out[2..4].copy_from_slice(&(entries.len() as u16).to_le_bytes());
    let mut n = MAP_UPDATE_HEADER_LEN;
    for e in entries {
        out[n..n + 2].copy_from_slice(&e.slot.to_le_bytes());
        out[n + 2..n + 4].copy_from_slice(&e.binding.partition_id.to_le_bytes());
        out[n + 4..n + 8].copy_from_slice(&e.binding.partition_incarnation.to_le_bytes());
        out[n + 8..n + 12].copy_from_slice(&e.binding.generation.to_le_bytes());
        n += MAP_UPDATE_ENTRY_LEN;
    }
    debug_assert_eq!(n, total);
    Some(n)
}

/// Validating reader over one encoded `MAP_UPDATE_V1` frame. `new`
/// fails closed on an unknown version, a zero or oversized count, or
/// any length other than exactly `header + count × entry` — a frame
/// that fails here must be dropped whole, never partially applied.
/// (Slot-range validation is the applier's job: `apply_update` checks
/// every slot before mutating.)
pub struct MapUpdateReader<'a> {
    entries: &'a [u8],
    count: usize,
}

impl<'a> MapUpdateReader<'a> {
    pub fn new(src: &'a [u8]) -> Option<Self> {
        if src.len() < MAP_UPDATE_HEADER_LEN {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != MAP_UPDATE_VERSION {
            return None;
        }
        let count = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if count == 0 || count > MAP_UPDATE_MAX_ENTRIES {
            return None;
        }
        if src.len() != MAP_UPDATE_HEADER_LEN + count * MAP_UPDATE_ENTRY_LEN {
            return None; // truncated or trailing bytes
        }
        Some(Self {
            entries: &src[MAP_UPDATE_HEADER_LEN..],
            count,
        })
    }

    pub fn count(&self) -> usize {
        self.count
    }

    /// Decode entry `i` (`i < count()`; a validated frame cannot fail
    /// here, and an out-of-bounds index panics like any slice misuse).
    pub fn entry(&self, i: usize) -> MapUpdateEntry {
        let n = i * MAP_UPDATE_ENTRY_LEN;
        let e = &self.entries[n..n + MAP_UPDATE_ENTRY_LEN];
        MapUpdateEntry {
            slot: u16::from_le_bytes([e[0], e[1]]),
            binding: SlotBinding {
                partition_id: u16::from_le_bytes([e[2], e[3]]),
                partition_incarnation: u32::from_le_bytes([e[4], e[5], e[6], e[7]]),
                generation: u32::from_le_bytes([e[8], e[9], e[10], e[11]]),
            },
        }
    }
}

// ── Keyspace routing declaration ──────────────────────────────────────

/// The §11.2 routing-form rule as a record: a keyspace's map form is
/// DECLARED in keyspace metadata — one of these per keyspace — and
/// never inferred from key bytes.
///
/// Wire layout (`KEYSPACE_ROUTING_VERSION` 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [keyspace_id:u32][routing_kind:u8][map_generation:u32]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyspaceRouting {
    pub keyspace_id: u32,
    pub routing_kind: RoutingKind,
    /// Generation of the partition map serving this keyspace.
    pub map_generation: u32,
}

impl KeyspaceRouting {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + 9;

    /// Serialize to the versioned wire form.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&KEYSPACE_ROUTING_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.keyspace_id.to_le_bytes());
        out[8] = self.routing_kind as u8;
        out[9..13].copy_from_slice(&self.map_generation.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// length mismatch, or an unknown routing kind.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != KEYSPACE_ROUTING_VERSION {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != Self::WIRE_LEN - 4 {
            return None;
        }
        Some(Self {
            keyspace_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            routing_kind: RoutingKind::from_u8(src[8])?,
            map_generation: u32::from_le_bytes(src[9..13].try_into().ok()?),
        })
    }
}
