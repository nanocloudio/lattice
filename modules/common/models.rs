//! Multi-model capability contracts — RFC database foundation
//! §14.15-§14.23, §21 invariants 18/19/20, delivery Phase 8.
//!
//! Phase 8 slice 1: the contract layer every model worker (document,
//! wide-column, graph, time-series, search, vector) composes over. This
//! file is contracts only — no ports, no I/O, no clocks, no module. It
//! stands in the same relationship to the Phase-8 model workers that
//! `relational.rs` does to the Phase-7 connectors and `db_ops.rs` does
//! to the Phase-6 workers.
//!
//! ## 1. The governing rule: shared foundations, separate meanings
//!
//! §14.15:
//!
//! > “Multi-model” does not mean that a graph traversal, text ranking,
//! > vector-nearest-neighbour query, time window, and relational join
//! > are represented as one vague generic operation.
//!
//! and §28 rejects the alternative outright:
//!
//! > **One generic NoSQL semantic engine.** Erases important
//! > differences in missing document fields, cell timestamps, graph
//! > traversal, time windows, text ranking, and approximate vector
//! > results. Models share foundations, not meanings.
//!
//! So this file is deliberately **not** a generic document/graph/vector
//! engine. It is exactly two things:
//!
//! - **Shared**: the ownership, derivation, and freshness contracts
//!   every model obeys (§14.22, §14.23), plus one generic *byte*
//!   helper — [`KeyWriter`] / [`KeyReader`] — because key composition
//!   is a foundation, not a meaning.
//! - **Per-model**: each capability's canonical key encoding and its
//!   own declared semantic contract, so that authoritative mutations
//!   compile into canonical Lattice keys (§14.15) and no model can
//!   bypass another's invariants (§14.23).
//!
//! The split is drawn at exactly that line. A cell timestamp
//! ([`cell_timestamp_is_committed_input`]), a traversal bound
//! ([`TraversalBounds`]), a late sample ([`classify_sample`]), a
//! posting generation ([`analyzer_change_plan`]) and an approximate
//! vector label ([`label_is_honest`]) are six different checks with six
//! different failure modes, and collapsing them into one
//! `check_model_operation(...) -> bool` would be the rejected
//! alternative wearing a Rust type.
//!
//! ## 2. What this file does NOT own
//!
//! - Physical key framing, escaping, MVCC suffix: `internal_key` (§9.1).
//! - Secondary-index composites, unique ownership, [`IndexFreshness`],
//!   change-feed cursors, retention-claim arithmetic: `db_ops` (§14,
//!   §15, §18). [`DerivedState`] **embeds** an `IndexFreshness` rather
//!   than restating its fields, and [`derived_satisfies`] delegates to
//!   `index_satisfies_freshness` before applying §14.22's extra rules.
//! - The SQL type system and the ordered value encoding: `relational`
//!   (§14.2, §14.4). The wide-column clustering encoder
//!   ([`encode_wide_key`]) calls `relational::encode_value_ordered`;
//!   it does not grow a second ordered encoder.
//! - Foreign-protocol refusal for *relational* keyspaces:
//!   `relational::check_object_write`. [`check_model_write`] is the
//!   same gate widened to the six model keyspaces, and it delegates the
//!   relational clause rather than restating it.
//!
//! One `#[path]` mount, `relational`, which already mounts `db_ops` →
//! `internal_key` → `range_lifecycle` → `partition_map` → `db_context`
//! beneath it. A second mount would be the same file under two module
//! paths, so the escape/terminator constants this file needs are
//! re-declared below with a documented byte-identity requirement — the
//! same accommodation `relational.rs` makes.
//!
//! ## 3. Keyspace mutual unambiguity (§14.15, §14.23)
//!
//! Every model key family gets its own reserved `keyspace: u32`
//! ([`KS_DOCUMENT_DATA`] … [`KS_VECTOR_JOB`]). `internal_key` writes
//! that keyspace as a fixed-width big-endian `u32` at byte offset 8 of
//! every physical key, so **two keys from different families differ
//! inside their first 12 bytes**: neither can equal the other and
//! neither can be a byte prefix of the other. Within one family, every
//! component is either fixed width or escaped-and-terminated, which is
//! §9.1's prefix-ambiguity rule one level down. Together those two
//! facts are the whole proof that a document write can never land in
//! graph space and a search posting scan can never sweep up vector
//! embeddings. `tests/contract_models.rs` makes it executable with a
//! seeded cross-family property test.
//!
//! ## 4. Fail closed, everywhere
//!
//! Unknown discriminant, unknown version, oversized component,
//! undersized output, a derived structure claiming freshness it has not
//! reached, an approximate vector result labelled exact, a partition
//! over bounds with no declared bucketing strategy, a retention
//! decision based on local file age: all refusals, none of them
//! best-effort.
//!
//! Golden vectors: `tests/contract_models.rs`. Every key encoding and
//! every record here is persistent or replicated, so if a change makes
//! a vector fail, that change is a format break requiring the
//! corresponding version bump plus an explicit migration — never an
//! update to the vector.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

// Only `relational` is mounted. It already mounts `db_ops` (and
// `internal_key`, `range_lifecycle`, `partition_map`, `db_context`)
// beneath it, so a second mount here would be the same file in two
// module paths.
#[path = "relational.rs"]
mod relational;

#[allow(
    unused_imports,
    reason = "re-export surface: a model worker composes the model layer with the relational, index, and key layers beneath it without further #[path] includes"
)]
pub use relational::{
    check_object_write, decode_value_ordered, encode_value_ordered, index_satisfies_freshness,
    AccessPath, CatalogError, DecodedKey, FreshnessError, IndexExactness, IndexFreshness,
    LogicalType, ModelOwner, Timestamp, UniqueOwnership, Value, ValueKind, MAX_KEY_COLUMNS,
    MAX_ORDERED_VALUE_LEN, MAX_TEXT_LEN,
};

/// The relational catalog's own object-kind enum (`Database`, `Table`,
/// `Index`, …), re-exported under an unambiguous name. It is the
/// `<object-kind>` byte of a *relational catalog key*, a different
/// concept from this file's [`ObjectKind`], which names a **model
/// object** in the shared cross-model catalog (§14.23).
#[allow(
    unused_imports,
    reason = "re-export surface: a model worker that also touches the relational catalog needs both object-kind enums from one import"
)]
pub use relational::ObjectKind as RelationalObjectKind;

// ══════════════════════════════════════════════════════════════════════
// 0. Shared key composition (§14.15 “models share foundations”)
// ══════════════════════════════════════════════════════════════════════

/// Escape sequence for a literal `0x00` inside a variable-length model
/// key component. Byte-identical to `internal_key::KEY_ESCAPE` by
/// requirement, not coincidence: a model composite becomes a *user key*
/// to `internal_key`, so both levels must agree that `0x00` is the only
/// special byte. The golden vectors pin the identity.
pub const COMPONENT_ESCAPE: [u8; 2] = [0x00, 0xFF];
/// Component terminator inside a model composite. Byte-identical to
/// `internal_key::KEY_TERMINATOR`. Terminator (`0x01`) sorts below
/// escape (`0xFF`), which is what makes a proper prefix sort before its
/// extensions — vertex `"a"` never sweeps up vertex `"ab"`.
pub const COMPONENT_TERMINATOR: [u8; 2] = [0x00, 0x01];

/// Maximum length of a variable-length model identifier: document id,
/// shard key, vertex id, entity id, search term. A format bound —
/// widening it is a version bump, because the worst-case key lengths
/// below are derived from it.
pub const MAX_MODEL_ID_LEN: usize = 48;

/// Worst-case encoded length of one variable component: every byte
/// escapes to two, plus the terminator.
pub const fn var_component_max_len(n: usize) -> usize {
    n * 2 + 2
}

/// Append-only writer for a model **user key** composite.
///
/// This is the one generic helper the six models share, and it is
/// deliberately about *bytes*, not meaning: fixed-width big-endian
/// integers (trivially order-preserving and self-delimiting) and
/// escaped-terminated variable components (§9.1's discipline). The
/// resulting composite is handed to `internal_key::encode` with the
/// family's reserved keyspace, which supplies the identity triple, the
/// outer escape, and the MVCC suffix.
///
/// Every method fails closed: a component that does not fit is a
/// refusal, never a truncation. Once a write has failed the writer is
/// poisoned, so a caller that ignores one `None` cannot produce a
/// short-but-plausible key.
pub struct KeyWriter<'a> {
    out: &'a mut [u8],
    n: usize,
    poisoned: bool,
}

impl<'a> KeyWriter<'a> {
    pub fn new(out: &'a mut [u8]) -> Self {
        Self {
            out,
            n: 0,
            poisoned: false,
        }
    }

    fn raw(&mut self, src: &[u8]) -> Option<()> {
        if self.poisoned || self.out.len() < self.n + src.len() {
            self.poisoned = true;
            return None;
        }
        self.out[self.n..self.n + src.len()].copy_from_slice(src);
        self.n += src.len();
        Some(())
    }

    /// Fixed-width big-endian `u32` component.
    pub fn u32(&mut self, v: u32) -> Option<()> {
        self.raw(&v.to_be_bytes())
    }

    /// Fixed-width big-endian `u64` component: ascending order.
    pub fn u64(&mut self, v: u64) -> Option<()> {
        self.raw(&v.to_be_bytes())
    }

    /// Bit-inverted big-endian `u64` component: **descending** order, so
    /// the newest version of a cell sorts first under ascending byte
    /// order. Same device `internal_key` uses for the MVCC timestamp.
    pub fn u64_desc(&mut self, v: u64) -> Option<()> {
        self.raw(&(!v).to_be_bytes())
    }

    /// Fixed-width opaque digest component (e.g. a label-set digest).
    pub fn digest(&mut self, v: &[u8; 16]) -> Option<()> {
        self.raw(v)
    }

    /// Variable-length component: `esc(src)` then the terminator.
    /// Refuses a component longer than `max` — a bound is part of the
    /// format, so exceeding it is a refusal, not a truncation.
    pub fn var(&mut self, src: &[u8], max: usize) -> Option<()> {
        if self.poisoned || src.len() > max {
            self.poisoned = true;
            return None;
        }
        for &b in src {
            if b == 0x00 {
                self.raw(&COMPONENT_ESCAPE)?;
            } else {
                self.raw(&[b])?;
            }
        }
        self.raw(&COMPONENT_TERMINATOR)
    }

    /// Bytes written so far, or `None` if any write failed.
    pub fn finish(self) -> Option<usize> {
        if self.poisoned {
            None
        } else {
            Some(self.n)
        }
    }
}

/// Cursor over a model user-key composite, the exact inverse of
/// [`KeyWriter`]. Fails closed on truncation, an unknown escape, a
/// missing terminator, an oversized component, or trailing bytes
/// ([`KeyReader::end`]) — a composite that does not consume exactly its
/// declared shape is not this family's key.
pub struct KeyReader<'a> {
    src: &'a [u8],
    i: usize,
}

impl<'a> KeyReader<'a> {
    pub fn new(src: &'a [u8]) -> Self {
        Self { src, i: 0 }
    }

    fn take(&mut self, w: usize) -> Option<&'a [u8]> {
        let s = self.src.get(self.i..self.i + w)?;
        self.i += w;
        Some(s)
    }

    pub fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    pub fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    pub fn u64_desc(&mut self) -> Option<u64> {
        Some(!u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    pub fn digest(&mut self) -> Option<[u8; 16]> {
        self.take(16)?.try_into().ok()
    }

    /// Unescape one terminator-delimited component into `out`; returns
    /// its plain length.
    pub fn var(&mut self, out: &mut [u8]) -> Option<usize> {
        let mut k = 0usize;
        loop {
            let a = *self.src.get(self.i)?;
            let b = *self.src.get(self.i + 1)?;
            match (a, b) {
                (0x00, 0x01) => {
                    self.i += 2;
                    return Some(k);
                }
                (0x00, 0xFF) => {
                    *out.get_mut(k)? = 0x00;
                    k += 1;
                    self.i += 2;
                }
                (0x00, _) => return None, // invalid escape
                (x, _) => {
                    *out.get_mut(k)? = x;
                    k += 1;
                    self.i += 1;
                }
            }
        }
    }

    /// Remaining unconsumed bytes.
    pub fn remaining(&self) -> &'a [u8] {
        &self.src[self.i.min(self.src.len())..]
    }

    /// Assert the composite is fully consumed. Trailing bytes are a
    /// refusal.
    pub fn end(&self) -> Option<()> {
        if self.i == self.src.len() {
            Some(())
        } else {
            None
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// 1. Reserved model keyspaces (§14.16-§14.21)
// ══════════════════════════════════════════════════════════════════════

/// The reserved model keyspaces, one per key family named by the RFC.
///
/// The high bit is set on every one, as with the relational keyspaces:
/// catalog-assigned user keyspaces are allocated below `0x8000_0000`,
/// so [`keyspace_owner`] is a bounded table lookup and a foreign
/// protocol cannot collide into model space by accident. The second
/// byte selects the model, which keeps the allocation readable and
/// leaves room for each model to add families without renumbering:
///
/// ```text
/// 0x8000_00xx  relational (owned by relational.rs)
/// 0x8001_00xx  document
/// 0x8002_00xx  wide-column
/// 0x8003_00xx  graph
/// 0x8004_00xx  time-series
/// 0x8005_00xx  search
/// 0x8006_00xx  vector
/// 0x8007_00xx  shared cross-model catalog
/// ```
pub const KS_DOCUMENT_CATALOG: u32 = 0x8001_0001;
pub const KS_DOCUMENT_DATA: u32 = 0x8001_0002;
pub const KS_DOCUMENT_INDEX: u32 = 0x8001_0003;
pub const KS_DOCUMENT_JOB: u32 = 0x8001_0004;
pub const KS_DOCUMENT_STATS: u32 = 0x8001_0005;

pub const KS_WIDE_TABLE: u32 = 0x8002_0001;

pub const KS_GRAPH_VERTEX: u32 = 0x8003_0001;
pub const KS_GRAPH_EDGE_OUT: u32 = 0x8003_0002;
pub const KS_GRAPH_EDGE_IN: u32 = 0x8003_0003;
pub const KS_GRAPH_INDEX: u32 = 0x8003_0004;

pub const KS_TIMESERIES_SERIES: u32 = 0x8004_0001;
pub const KS_TIMESERIES_SAMPLE: u32 = 0x8004_0002;
pub const KS_TIMESERIES_ROLLUP: u32 = 0x8004_0003;

pub const KS_SEARCH_DEFINITION: u32 = 0x8005_0001;
pub const KS_SEARCH_POSTING: u32 = 0x8005_0002;
pub const KS_SEARCH_TERM_STATS: u32 = 0x8005_0003;
pub const KS_SEARCH_JOB: u32 = 0x8005_0004;

pub const KS_VECTOR_EMBEDDING: u32 = 0x8006_0001;
pub const KS_VECTOR_MANIFEST: u32 = 0x8006_0002;
pub const KS_VECTOR_JOB: u32 = 0x8006_0003;
/// LSH bucket postings for approximate search (`VECTOR.ANN`): a
/// denormalised copy of each embedding keyed by its bucket, so a query
/// scans one bucket instead of the whole index. A separate keyspace from
/// the authoritative embeddings — the exact path never sees it, so a
/// stale posting (from re-embedding into a new bucket) only ever costs
/// the APPROXIMATE path a little recall, never a wrong exact answer.
pub const KS_VECTOR_ANN: u32 = 0x8006_0004;
/// `vector/meta` — per-entity metadata tags for filtered nearest-neighbour.
pub const KS_VECTOR_META: u32 = 0x8006_0005;

/// The shared cross-model catalog (§14.23) and its projection
/// declarations. Owned by no single model: it is the record of *which*
/// model owns what, so it is written by the catalog authority alone.
pub const KS_MODEL_CATALOG: u32 = 0x8007_0001;
pub const KS_MODEL_PROJECTION: u32 = 0x8007_0002;

/// Redis hash fields: `hash/<hash-id>/<field>`. Deliberately UNOWNED (not a
/// reserved model keyspace) — a hash is a key-value surface feature, not a
/// separate model, so `keyspace_owner` returns `None` for it and it is written
/// on the ordinary KV path. Layout `[hash_id:u32 BE][field bytes]`, field to
/// the end; a `HGETALL` is the prefix scan of one `hash_id`.
pub const KS_HASH: u32 = 0x800a_0001;

/// `hash/<hash-id>/<field>` user key: `[hash_id:u32 BE][field]`.
pub fn encode_hash_field_key(out: &mut [u8], hash_id: u32, field: &[u8]) -> Option<usize> {
    let need = 4 + field.len();
    if out.len() < need {
        return None;
    }
    out[0..4].copy_from_slice(&hash_id.to_be_bytes());
    out[4..need].copy_from_slice(field);
    Some(need)
}

/// Redis list elements: `list/<list-id>/<index>`. UNOWNED, like the hash
/// keyspace. `[list_id:u32 BE][index]` where the signed index is encoded
/// order-preserving (sign bit flipped) so a forward scan yields elements
/// head→tail — an `LPUSH` grows the index downward (into negatives), an
/// `RPUSH` upward.
pub const KS_LIST: u32 = 0x800a_0002;
/// Redis list head/tail hint: `list-meta/<list-id>` → `[head:i64 LE][tail:i64
/// LE]`. Only a hint — the correctness guard is a CAS that the target element
/// slot is absent, so a stale hint costs a retry, never a lost element.
pub const KS_LIST_META: u32 = 0x800a_0003;

/// Order-preserving encoding of a signed list index.
pub fn list_index_key_component(index: i64) -> [u8; 8] {
    ((index as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

/// `list/<list-id>/<index>` user key: `[list_id:u32 BE][index:8 order-preserving]`.
pub fn encode_list_entry_key(out: &mut [u8], list_id: u32, index: i64) -> Option<usize> {
    if out.len() < 12 {
        return None;
    }
    out[0..4].copy_from_slice(&list_id.to_be_bytes());
    out[4..12].copy_from_slice(&list_index_key_component(index));
    Some(12)
}

/// `list-meta/<list-id>` user key: `[list_id:u32 BE]`.
pub fn encode_list_meta_key(out: &mut [u8], list_id: u32) -> Option<usize> {
    if out.len() < 4 {
        return None;
    }
    out[0..4].copy_from_slice(&list_id.to_be_bytes());
    Some(4)
}

/// Redis sorted-set member→score directory: `zmember/<set-id>/<member>` →
/// `[score:i64 LE]`. UNOWNED. `ZSCORE` is a point read; `ZADD` reads it to
/// find a member's prior score before re-indexing.
pub const KS_ZMEMBER: u32 = 0x800a_0004;
/// Redis sorted-set score index: `zscore/<set-id>/<score>/<member>` (empty
/// value). UNOWNED. `[set_id:u32 BE][score:8 order-preserving][member]`, so a
/// forward scan yields members in ascending score order — `ZRANGE`.
pub const KS_ZSCORE: u32 = 0x800a_0005;

/// `zmember/<set-id>/<member>` user key: `[set_id:u32 BE][member]`.
pub fn encode_zmember_key(out: &mut [u8], set_id: u32, member: &[u8]) -> Option<usize> {
    let need = 4 + member.len();
    if out.len() < need {
        return None;
    }
    out[0..4].copy_from_slice(&set_id.to_be_bytes());
    out[4..need].copy_from_slice(member);
    Some(need)
}

/// `zscore/<set-id>/<score>/<member>` user key:
/// `[set_id:u32 BE][score:8 order-preserving][member]`.
pub fn encode_zscore_key(out: &mut [u8], set_id: u32, score: i64, member: &[u8]) -> Option<usize> {
    let need = 12 + member.len();
    if out.len() < need {
        return None;
    }
    out[0..4].copy_from_slice(&set_id.to_be_bytes());
    out[4..12].copy_from_slice(&list_index_key_component(score));
    out[12..need].copy_from_slice(member);
    Some(need)
}

/// Which capability owns writes to `keyspace`, or `None` if it is not a
/// reserved model keyspace.
///
/// This is the executable form of §14.23's “raw KV connectors cannot
/// write model-owned keyspaces”: a keyspace with an owner is closed to
/// every other writer except through a declared projection, and the
/// relational keyspaces are delegated to `relational` so there is one
/// table, not two.
pub const fn keyspace_owner(keyspace: u32) -> Option<ModelOwner> {
    if relational::is_reserved_relational(keyspace) {
        return Some(ModelOwner::Relational);
    }
    match keyspace {
        KS_DOCUMENT_CATALOG | KS_DOCUMENT_DATA | KS_DOCUMENT_INDEX | KS_DOCUMENT_JOB
        | KS_DOCUMENT_STATS => Some(ModelOwner::Document),
        KS_WIDE_TABLE => Some(ModelOwner::WideColumn),
        KS_GRAPH_VERTEX | KS_GRAPH_EDGE_OUT | KS_GRAPH_EDGE_IN | KS_GRAPH_INDEX => {
            Some(ModelOwner::Graph)
        }
        KS_TIMESERIES_SERIES | KS_TIMESERIES_SAMPLE | KS_TIMESERIES_ROLLUP => {
            Some(ModelOwner::TimeSeries)
        }
        KS_SEARCH_DEFINITION | KS_SEARCH_POSTING | KS_SEARCH_TERM_STATS | KS_SEARCH_JOB => {
            Some(ModelOwner::Search)
        }
        KS_VECTOR_EMBEDDING | KS_VECTOR_MANIFEST | KS_VECTOR_JOB | KS_VECTOR_ANN
        | KS_VECTOR_META => Some(ModelOwner::Vector),
        _ => None,
    }
}

/// Is `keyspace` reserved by any capability (model or relational)?
pub const fn is_reserved_model(keyspace: u32) -> bool {
    keyspace_owner(keyspace).is_some()
}

// ══════════════════════════════════════════════════════════════════════
// 2. Shared catalog and model ownership (§14.23, §21 invariant 18)
// ══════════════════════════════════════════════════════════════════════

/// Version of the [`CatalogObject`] wire encoding.
pub const CATALOG_OBJECT_VERSION: u16 = 1;
/// Version of the [`ProjectionDescriptor`] wire encoding.
pub const PROJECTION_DESCRIPTOR_VERSION: u16 = 1;
/// Version of the [`DerivedState`] wire encoding.
pub const DERIVED_STATE_VERSION: u16 = 1;
/// Version of the [`TraversalBounds`] wire encoding.
pub const TRAVERSAL_BOUNDS_VERSION: u16 = 1;
/// Version of the [`SearchResultLabel`] wire encoding.
pub const SEARCH_RESULT_LABEL_VERSION: u16 = 1;
/// Version of the [`VectorResponseLabel`] wire encoding.
pub const VECTOR_RESPONSE_LABEL_VERSION: u16 = 1;

/// A model object in the shared cross-model catalog (§14.23).
///
/// Distinct from `relational::ObjectKind` (re-exported here as
/// [`RelationalObjectKind`]), which enumerates the *relational
/// catalog's* internal object types. This enum enumerates the things
/// §14.23's catalog listing names — one variant per capability, plus
/// `KvKeyspace` so a plain KV keyspace is expressible and therefore
/// checkable rather than invisible.
///
/// Discriminants start at 1 so a zeroed buffer never decodes as a kind.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectKind {
    DocumentCollection = 1,
    WideColumnTable = 2,
    Graph = 3,
    TimeSeries = 4,
    SearchIndex = 5,
    VectorIndex = 6,
    RelationalTable = 7,
    KvKeyspace = 8,
}

impl ObjectKind {
    pub const ALL: [Self; 8] = [
        Self::DocumentCollection,
        Self::WideColumnTable,
        Self::Graph,
        Self::TimeSeries,
        Self::SearchIndex,
        Self::VectorIndex,
        Self::RelationalTable,
        Self::KvKeyspace,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::DocumentCollection),
            2 => Some(Self::WideColumnTable),
            3 => Some(Self::Graph),
            4 => Some(Self::TimeSeries),
            5 => Some(Self::SearchIndex),
            6 => Some(Self::VectorIndex),
            7 => Some(Self::RelationalTable),
            8 => Some(Self::KvKeyspace),
            _ => None,
        }
    }

    /// The **one** capability that may write this kind of object
    /// directly (§14.23: “each object has exactly one write-owning
    /// capability”). The mapping is total and injective-by-model, which
    /// is what makes a `CatalogObject` whose declared `owner` disagrees
    /// with its `kind` structurally malformed rather than merely odd.
    pub const fn owning_capability(self) -> ModelOwner {
        match self {
            Self::DocumentCollection => ModelOwner::Document,
            Self::WideColumnTable => ModelOwner::WideColumn,
            Self::Graph => ModelOwner::Graph,
            Self::TimeSeries => ModelOwner::TimeSeries,
            Self::SearchIndex => ModelOwner::Search,
            Self::VectorIndex => ModelOwner::Vector,
            Self::RelationalTable => ModelOwner::Relational,
            Self::KvKeyspace => ModelOwner::KeyValue,
        }
    }
}

/// Whether an object's state is the source of truth or a materialized
/// consequence of one (§14.22).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateAuthority {
    /// Source documents, rows/cells, vertices/edges, samples, canonical
    /// embeddings, catalog definitions, transaction decisions, index
    /// definitions, durable job progress. Full transaction, snapshot,
    /// and Clustor evidence path.
    Authoritative = 1,
    /// Async postings, approximate vector structures, rollups, query
    /// statistics, cached neighbourhoods, denormalized projections.
    /// Must declare a [`DerivedState`].
    Derived = 2,
}

impl StateAuthority {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Authoritative),
            2 => Some(Self::Derived),
            _ => None,
        }
    }
}

/// Every way a model contract refuses. No catch-all: a refusal names
/// what it refused (house rule, and the reason §14.23's ownership rule
/// is auditable at all).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModelError {
    /// The object's declared owner is not the one its kind implies.
    /// §14.23: exactly one write-owning capability, and it is a
    /// property of the kind.
    OwnerKindMismatch {
        kind: ObjectKind,
        declared: ModelOwner,
        implied: ModelOwner,
    },
    /// The object's keyspace belongs to a different capability than its
    /// declared owner.
    KeyspaceOwnerMismatch {
        keyspace: u32,
        keyspace_owner: ModelOwner,
        declared: ModelOwner,
    },
    /// §14.23: “raw KV connectors cannot write model-owned keyspaces”.
    ForeignProtocolWrite { keyspace: u32, writer: ModelOwner },
    /// §21 invariant 18: a second capability tried to write an object
    /// it does not own, with no declared projection.
    NotWriteOwner {
        owner: ModelOwner,
        writer: ModelOwner,
    },
    /// A write claimed a declared projection, but the projection does
    /// not name this source/target pair, or does not grant this writer
    /// authority, or its target keyspace is not the one being written.
    ProjectionNotDeclared,
    /// The projection's own shape is invalid: source equals target, an
    /// asynchronous projection claiming synchronous consistency, or a
    /// non-rebuildable projection with a rebuild-based failure policy.
    ProjectionMalformed,
    /// The delegated `relational` catalog gate refused.
    Catalog(CatalogError),
    /// The delegated `db_ops` freshness gate refused.
    Freshness(FreshnessError),
    /// A derived structure declared `Synchronous` maintenance but
    /// reports a resolved timestamp behind its source: synchronous
    /// maintenance happens inside the writing transaction, so there is
    /// nothing to lag behind. Refused rather than believed.
    SynchronousDerivedLags {
        source_timestamp: Timestamp,
        resolved_timestamp: Timestamp,
    },
    /// §21 invariant 19 in its rawest form: a derived structure's
    /// declared frontier is **ahead** of the source it has consumed.
    /// That is a claim of freshness it has not reached, stored on disk.
    ResolvedAheadOfSource {
        source_timestamp: Timestamp,
        resolved_timestamp: Timestamp,
    },
    /// The query demanded synchronous exactness and this structure's
    /// mode cannot provide it. Exactness is a property of the
    /// maintenance mode, not of how recently the structure happened to
    /// catch up.
    ExactnessUnavailable { mode: DerivedMode },
    /// A record field violated a declared bound, or an output buffer
    /// was too small.
    OutOfBounds,
    /// A structurally invalid record: an unknown discriminant, a
    /// version mismatch, a zero bound where a bound is mandatory.
    Malformed,
    /// §14.17: a logical partition exceeded its configured bounds with
    /// no declared bucketing strategy — the “silently unbounded hot
    /// range” this check exists to prevent.
    PartitionUnbounded {
        observed_rows: u64,
        max_rows: u64,
        observed_bytes: u64,
        max_bytes: u64,
    },
    /// §14.17: a cell timestamp was sampled by a replica rather than
    /// supplied as a committed input. That is §21 invariant 3's
    /// determinism rule in wide-column clothing.
    CellTimestampSampled,
    /// §14.19: a retention decision was based on local file age.
    RetentionBasisNotCommitted { basis: RetentionBasis },
    /// §14.19: a sample fell outside the accepted window and the
    /// series' late policy rejects it.
    SampleTooLate {
        sample_timestamp: Timestamp,
        watermark: Timestamp,
    },
    /// §14.20: a posting was offered for an index generation whose
    /// analyzer version differs from the writer's — mixing incompatible
    /// postings, which §14.20 forbids in favour of a rebuild.
    AnalyzerGenerationMismatch {
        generation_analyzer_version: u32,
        writer_analyzer_version: u32,
    },
    /// §14.18: a traversal did not declare one of its mandatory bounds.
    TraversalBoundMissing { bound: TraversalBound },
    /// §14.18: the edge mutation plan is not the complete bounded set —
    /// a missing adjacency direction, a missing change record, or more
    /// entries than the format permits.
    EdgePlanIncomplete,
    /// §14.21 / §21 invariant 20: a response label claims more than the
    /// execution achieved.
    DishonestLabel { claim: LabelClaim },
}

/// Which mandatory traversal bound was absent (§14.18).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraversalBound {
    Depth = 1,
    Frontier = 2,
    Visited = 3,
    Bytes = 4,
    Work = 5,
    Deadline = 6,
}

/// Which part of a response label was dishonest (§21 invariant 20).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LabelClaim {
    /// Labelled exact, executed approximate.
    ExactnessOverstated = 1,
    /// Claimed a source timestamp ahead of what the index has consumed.
    FreshnessOverstated = 2,
    /// Named a generation other than the one that answered.
    GenerationMisreported = 3,
    /// Named a metric other than the one that was computed.
    MetricMisreported = 4,
    /// Named a reranking mode other than the one that ran.
    RerankingMisreported = 5,
    /// Reported no degradation while degraded.
    DegradationSuppressed = 6,
}

/// A catalog object: identity, kind, its one write owner, the keyspace
/// its authoritative records live in, and whether that state is the
/// source of truth (§14.22, §14.23).
///
/// Wire layout ([`CATALOG_OBJECT_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [database_id:u32][object_id:u32]
/// [kind:u8][owner:u8][authority:u8]
/// [keyspace:u32]
/// [source_object_id:u32]
/// ```
///
/// `source_object_id` is `0` for an object with no source and the
/// source object's id for §14.23's `source:` objects (a search index
/// over a collection, a vector index over a field). It is *not* an
/// authority grant — the projection is (see [`ProjectionDescriptor`]);
/// it is the catalog's record of the relationship.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogObject {
    pub database_id: u32,
    pub object_id: u32,
    pub kind: ObjectKind,
    pub owner: ModelOwner,
    pub authority: StateAuthority,
    pub keyspace: u32,
    pub source_object_id: u32,
}

impl CatalogObject {
    /// Header (4) + 4 + 4 + 1 + 1 + 1 + 4 + 4.
    pub const WIRE_LEN: usize = 4 + 19;

    /// Build an object whose owner is the one its kind implies. This is
    /// the only constructor a model worker should need: §14.23's rule
    /// is that the kind determines the owner, so making the owner an
    /// input invites the mismatch [`check_invariants`] then has to
    /// reject.
    ///
    /// [`check_invariants`]: CatalogObject::check_invariants
    pub const fn new(
        database_id: u32,
        object_id: u32,
        kind: ObjectKind,
        authority: StateAuthority,
        keyspace: u32,
        source_object_id: u32,
    ) -> Self {
        Self {
            database_id,
            object_id,
            kind,
            owner: kind.owning_capability(),
            authority,
            keyspace,
            source_object_id,
        }
    }

    /// §14.23's structural rules: the declared owner must be the one the
    /// kind implies, and if the keyspace is reserved it must be reserved
    /// by that same capability. A `KvKeyspace` object in an unreserved
    /// keyspace is fine — that is what a plain KV keyspace *is*.
    pub fn check_invariants(&self) -> Result<(), ModelError> {
        let implied = self.kind.owning_capability();
        if self.owner != implied {
            return Err(ModelError::OwnerKindMismatch {
                kind: self.kind,
                declared: self.owner,
                implied,
            });
        }
        if let Some(ks_owner) = keyspace_owner(self.keyspace) {
            if ks_owner != self.owner {
                return Err(ModelError::KeyspaceOwnerMismatch {
                    keyspace: self.keyspace,
                    keyspace_owner: ks_owner,
                    declared: self.owner,
                });
            }
        }
        Ok(())
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&CATALOG_OBJECT_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.database_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.object_id.to_le_bytes());
        out[12] = self.kind as u8;
        out[13] = self.owner as u8;
        out[14] = self.authority as u8;
        out[15..19].copy_from_slice(&self.keyspace.to_le_bytes());
        out[19..23].copy_from_slice(&self.source_object_id.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Fails closed on unknown version, length mismatch, unknown
    /// discriminant, or a record that violates its own invariants —
    /// a catalog object that claims the wrong owner cannot even be read
    /// back off disk.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != CATALOG_OBJECT_VERSION {
            return None;
        }
        if u16::from_le_bytes(src[2..4].try_into().ok()?) as usize != Self::WIRE_LEN - 4 {
            return None;
        }
        let o = Self {
            database_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            object_id: u32::from_le_bytes(src[8..12].try_into().ok()?),
            kind: ObjectKind::from_u8(src[12])?,
            owner: ModelOwner::from_u8(src[13])?,
            authority: StateAuthority::from_u8(src[14])?,
            keyspace: u32::from_le_bytes(src[15..19].try_into().ok()?),
            source_object_id: u32::from_le_bytes(src[19..23].try_into().ok()?),
        };
        o.check_invariants().ok()?;
        Some(o)
    }
}

/// `model/catalog/<database-id>/<kind>/<object-id>` user key:
/// `[database_id:u32 BE][kind:u8][object_id:u32 BE]`. Fixed width
/// throughout, so a scan of `[database_id][kind]` yields every object of
/// one kind in id order. Hand to `internal_key::encode` with
/// [`KS_MODEL_CATALOG`].
pub const MODEL_CATALOG_KEY_LEN: usize = 4 + 1 + 4;

pub fn encode_model_catalog_key(
    out: &mut [u8],
    database_id: u32,
    kind: ObjectKind,
    object_id: u32,
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(database_id)?;
    w.raw(&[kind as u8])?;
    w.u32(object_id)?;
    w.finish()
}

pub fn decode_model_catalog_key(src: &[u8]) -> Option<(u32, ObjectKind, u32)> {
    let mut r = KeyReader::new(src);
    let db = r.u32()?;
    let kind = ObjectKind::from_u8(*r.take(1)?.first()?)?;
    let id = r.u32()?;
    r.end()?;
    Some((db, kind, id))
}

/// §14.23 + §21 invariant 18 as one executable gate, widened from
/// `relational::check_object_write` to every model keyspace.
///
/// Clause order is the contract:
///
/// 1. the object's own shape must be valid — an object that misstates
///    its owner cannot be used to authorize anything;
/// 2. a raw KV writer addressing a *model-owned* keyspace directly is
///    refused unconditionally, regardless of any descriptor, because a
///    Redis `SET` into `graph/edge-out/...` bypasses adjacency,
///    indexes, and the change record (§14.23);
/// 3. only then does ownership matter, and only a **declared**
///    projection ([`AccessPath::DeclaredProjection`]) excuses a
///    cross-model write. Declaring it here is not sufficient by itself:
///    [`check_projection_write`] is the gate that checks the projection
///    actually permits *this* write.
///
/// Clause 2 is a strict generalization of
/// `relational::check_object_write`'s foreign-protocol clause — same
/// rule, wider keyspace table, and [`keyspace_owner`] delegates the
/// relational half of that table to `relational::is_reserved_relational`
/// so there is one list, not two. The delegated call at the end is a
/// belt-and-braces equivalence assertion: if the relational gate ever
/// refuses something this function permitted, the divergence is a
/// refusal rather than a silently wider door.
pub fn check_model_write(
    object: &CatalogObject,
    writer: ModelOwner,
    via: AccessPath,
) -> Result<(), ModelError> {
    object.check_invariants()?;
    if writer == ModelOwner::KeyValue
        && via == AccessPath::Direct
        && keyspace_owner(object.keyspace).is_some_and(|o| o != ModelOwner::KeyValue)
    {
        return Err(ModelError::ForeignProtocolWrite {
            keyspace: object.keyspace,
            writer,
        });
    }
    if writer != object.owner && via != AccessPath::DeclaredProjection {
        return Err(ModelError::NotWriteOwner {
            owner: object.owner,
            writer,
        });
    }
    relational::check_object_write(object.keyspace, object.owner, writer, via)
        .map_err(ModelError::Catalog)
}

/// How a projection's target is maintained relative to its source
/// (§14.23).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Maintenance {
    /// Maintained inside the source's writing transaction. The target
    /// is exact at every committed source timestamp.
    Synchronous = 1,
    /// Maintained from the source's change stream. The target reports a
    /// frontier and can never claim currency it has not reached
    /// (§14.22). §14.23: “broad derived fanout normally consumes change
    /// streams rather than expanding foreground transactions”.
    Asynchronous = 2,
}

impl Maintenance {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Synchronous),
            2 => Some(Self::Asynchronous),
            _ => None,
        }
    }
}

/// The consistency a projection's target claims (§14.23).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionConsistency {
    /// Target reads are consistent with the source at every committed
    /// timestamp. Only expressible with [`Maintenance::Synchronous`].
    TransactionallyConsistent = 1,
    /// Target reads are consistent as of a reported resolved timestamp.
    ResolvedTimestamp = 2,
    /// Target reads are eventually consistent and say so; a query with
    /// a freshness requirement must be refused or reported, never
    /// silently served as current.
    Eventual = 3,
}

impl ProjectionConsistency {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::TransactionallyConsistent),
            2 => Some(Self::ResolvedTimestamp),
            3 => Some(Self::Eventual),
            _ => None,
        }
    }
}

/// What happens to the target when the projection must be rebuilt
/// (§14.23, §14.22).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RebuildBehaviour {
    /// Rebuild from a source snapshot plus change stream, resumable
    /// across failure. The normal case.
    ResumableFromSnapshot = 1,
    /// Rebuild only from a full source scan; not resumable, so a
    /// failure restarts it.
    FullScanOnly = 2,
    /// Cannot be rebuilt from the source. Then the target is not
    /// derived at all — it holds unique state, which §14.22 forbids for
    /// derived structures and §28 lists as a rejected alternative
    /// (“make derived indexes authoritative by accident”). Expressible
    /// so that it is visibly wrong.
    NotRebuildable = 3,
}

impl RebuildBehaviour {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::ResumableFromSnapshot),
            2 => Some(Self::FullScanOnly),
            3 => Some(Self::NotRebuildable),
            _ => None,
        }
    }
}

/// What the projection does when maintenance fails (§14.23).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionFailurePolicy {
    /// Fail the source mutation. Only coherent for synchronous
    /// maintenance: an asynchronous consumer cannot retroactively fail
    /// a committed source write.
    FailSourceWrite = 1,
    /// Stop advancing the frontier and report. The target keeps serving
    /// at its last resolved timestamp, honestly labelled.
    PauseAndReport = 2,
    /// Mark the target invalid and schedule a rebuild.
    InvalidateAndRebuild = 3,
}

impl ProjectionFailurePolicy {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::FailSourceWrite),
            2 => Some(Self::PauseAndReport),
            3 => Some(Self::InvalidateAndRebuild),
            _ => None,
        }
    }
}

/// §14.23's explicit cross-model access path.
///
/// > Cross-model access uses an explicit view or projection declaring
/// > source, target, write authority, synchronous or asynchronous
/// > maintenance, consistency, resolved timestamp, rebuild behavior,
/// > and failure policy. This permits document-to-search,
/// > document-to-vector, row-to-graph, or sample-to-rollup projections
/// > **without allowing either side to bypass the source model's
/// > invariants**.
///
/// That last clause is why this is a record and not a boolean: the
/// projection names exactly one source, one target, one authority, and
/// one target keyspace, and [`check_projection_write`] permits exactly
/// that and nothing else.
///
/// Wire layout ([`PROJECTION_DESCRIPTOR_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [projection_id:u32][database_id:u32]
/// [source_object_id:u32][target_object_id:u32]
/// [target_keyspace:u32]
/// [write_authority:u8][maintenance:u8][consistency:u8]
/// [rebuild:u8][failure_policy:u8]
/// ```
///
/// The resolved timestamp §14.23 also lists is deliberately **not** a
/// field here: it is per-instant state, not descriptor state, and it
/// lives in [`DerivedState::resolved_timestamp`]. Putting a moving
/// value in a durable descriptor is how a descriptor starts lying.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectionDescriptor {
    pub projection_id: u32,
    pub database_id: u32,
    pub source_object_id: u32,
    pub target_object_id: u32,
    /// The keyspace the projection is permitted to write. A projection
    /// that could write “the target object, wherever that is” would be
    /// a capability, not a grant.
    pub target_keyspace: u32,
    /// The capability permitted to perform the projection write. Usually
    /// the target's owner (a search worker maintaining its own postings
    /// from document changes); occasionally the source's owner
    /// (synchronous document-to-search maintenance inside the document
    /// mutation's transaction).
    pub write_authority: ModelOwner,
    pub maintenance: Maintenance,
    pub consistency: ProjectionConsistency,
    pub rebuild: RebuildBehaviour,
    pub failure_policy: ProjectionFailurePolicy,
}

impl ProjectionDescriptor {
    /// Header (4) + 4 + 4 + 4 + 4 + 4 + 1 + 1 + 1 + 1 + 1.
    pub const WIRE_LEN: usize = 4 + 25;

    /// The four self-contradictions a projection can express, all
    /// refused:
    ///
    /// - source is its own target — a projection with no direction;
    /// - asynchronous maintenance claiming transactional consistency —
    ///   §14.22's “never an unreported second authority” in descriptor
    ///   form;
    /// - asynchronous maintenance with `FailSourceWrite` — a change-
    ///   stream consumer cannot fail a write that already committed;
    /// - `NotRebuildable` combined with `InvalidateAndRebuild` — a
    ///   policy that names an operation the descriptor says is
    ///   impossible.
    pub fn check_invariants(&self) -> Result<(), ModelError> {
        if self.source_object_id == self.target_object_id {
            return Err(ModelError::ProjectionMalformed);
        }
        if self.maintenance == Maintenance::Asynchronous
            && self.consistency == ProjectionConsistency::TransactionallyConsistent
        {
            return Err(ModelError::ProjectionMalformed);
        }
        if self.maintenance == Maintenance::Asynchronous
            && self.failure_policy == ProjectionFailurePolicy::FailSourceWrite
        {
            return Err(ModelError::ProjectionMalformed);
        }
        if self.rebuild == RebuildBehaviour::NotRebuildable
            && self.failure_policy == ProjectionFailurePolicy::InvalidateAndRebuild
        {
            return Err(ModelError::ProjectionMalformed);
        }
        Ok(())
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&PROJECTION_DESCRIPTOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.projection_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.database_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.source_object_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.target_object_id.to_le_bytes());
        out[20..24].copy_from_slice(&self.target_keyspace.to_le_bytes());
        out[24] = self.write_authority as u8;
        out[25] = self.maintenance as u8;
        out[26] = self.consistency as u8;
        out[27] = self.rebuild as u8;
        out[28] = self.failure_policy as u8;
        Some(Self::WIRE_LEN)
    }

    /// Fails closed on unknown version, length mismatch, unknown
    /// discriminant, or a self-contradictory descriptor.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != PROJECTION_DESCRIPTOR_VERSION {
            return None;
        }
        if u16::from_le_bytes(src[2..4].try_into().ok()?) as usize != Self::WIRE_LEN - 4 {
            return None;
        }
        let p = Self {
            projection_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            database_id: u32::from_le_bytes(src[8..12].try_into().ok()?),
            source_object_id: u32::from_le_bytes(src[12..16].try_into().ok()?),
            target_object_id: u32::from_le_bytes(src[16..20].try_into().ok()?),
            target_keyspace: u32::from_le_bytes(src[20..24].try_into().ok()?),
            write_authority: ModelOwner::from_u8(src[24])?,
            maintenance: Maintenance::from_u8(src[25])?,
            consistency: ProjectionConsistency::from_u8(src[26])?,
            rebuild: RebuildBehaviour::from_u8(src[27])?,
            failure_policy: ProjectionFailurePolicy::from_u8(src[28])?,
        };
        p.check_invariants().ok()?;
        Some(p)
    }
}

/// `model/projection/<database-id>/<projection-id>` user key.
pub const MODEL_PROJECTION_KEY_LEN: usize = 4 + 4;

pub fn encode_projection_key(
    out: &mut [u8],
    database_id: u32,
    projection_id: u32,
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(database_id)?;
    w.u32(projection_id)?;
    w.finish()
}

pub fn decode_projection_key(src: &[u8]) -> Option<(u32, u32)> {
    let mut r = KeyReader::new(src);
    let db = r.u32()?;
    let id = r.u32()?;
    r.end()?;
    Some((db, id))
}

/// Permit **exactly** what the projection declares, and nothing else.
///
/// A cross-model write is legal only when all of the following hold:
/// the projection is well formed; it names this source and this target;
/// it grants this writer authority; and the target object it names is
/// the one being written, in the keyspace it names. Any deviation is
/// [`ModelError::ProjectionNotDeclared`] — the point of §14.23's
/// projection is that it is a *grant of a specific edge*, not a general
/// exemption from ownership.
///
/// The source object is checked too: a projection cannot be used to
/// write the source (that would be the target model bypassing the
/// source model's invariants, which §14.23 forbids by name).
pub fn check_projection_write(
    projection: &ProjectionDescriptor,
    source: &CatalogObject,
    target: &CatalogObject,
    writer: ModelOwner,
) -> Result<(), ModelError> {
    projection.check_invariants()?;
    source.check_invariants()?;
    target.check_invariants()?;
    if projection.database_id != source.database_id
        || projection.database_id != target.database_id
        || projection.source_object_id != source.object_id
        || projection.target_object_id != target.object_id
        || projection.target_keyspace != target.keyspace
        || projection.write_authority != writer
    {
        return Err(ModelError::ProjectionNotDeclared);
    }
    // The grant covers the target only. Writing the source through a
    // projection is exactly the bypass §14.23 rules out.
    check_model_write(target, writer, AccessPath::DeclaredProjection)
}

// ══════════════════════════════════════════════════════════════════════
// 3. Authoritative and derived state (§14.22, §21 invariant 19)
// ══════════════════════════════════════════════════════════════════════

/// The four shapes derived state may take (§14.22: “synchronous,
/// asynchronous with an explicit frontier, rebuildable from a
/// snapshot/change stream, or a disposable cache”).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DerivedMode {
    /// Maintained inside the source transaction: exact at every
    /// committed source timestamp, and therefore lags by definition
    /// zero. A `Synchronous` record reporting lag is refused.
    Synchronous = 1,
    /// Maintained from the change stream with an explicit frontier.
    /// Serves reads only at or below [`DerivedState::resolved_timestamp`].
    AsynchronousWithFrontier = 2,
    /// Reconstructible from a source snapshot plus change stream. Its
    /// retention requirement survives compaction until a rebuild-safe
    /// frontier exists.
    Rebuildable = 3,
    /// A cache with no authority whatsoever: droppable at any moment,
    /// makes no retention claim, and can never satisfy an exactness
    /// requirement.
    DisposableCache = 4,
}

impl DerivedMode {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Synchronous),
            2 => Some(Self::AsynchronousWithFrontier),
            3 => Some(Self::Rebuildable),
            4 => Some(Self::DisposableCache),
            _ => None,
        }
    }
}

/// What a query gets when the derived structure is behind (§14.22's
/// `stale_query_policy`).
///
/// None of the three is “silently serve stale as current”. The
/// difference between [`Report`] and [`Serve`] is whether the caller
/// must surface the staleness to its user, not whether the staleness is
/// known: both return the reached frontier, because §21 invariant 19
/// forbids a derived structure from claiming freshness it has not
/// achieved regardless of policy.
///
/// [`Report`]: StaleQueryPolicy::Report
/// [`Serve`]: StaleQueryPolicy::Serve
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaleQueryPolicy {
    /// Serve, and require the caller to surface the reported frontier.
    Report = 1,
    /// Refuse the query outright.
    Refuse = 2,
    /// Serve at the reached frontier, labelled with it.
    Serve = 3,
}

impl StaleQueryPolicy {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Report),
            2 => Some(Self::Refuse),
            3 => Some(Self::Serve),
            _ => None,
        }
    }
}

/// §14.22's `derived_state:` block as a record.
///
/// The RFC's YAML has `source_object`, `source_revision`, `generation`,
/// `mode`, `rebuildable`, `resolved_timestamp`, `stale_query_policy`.
/// Four of those (source revision, source timestamp, range generation,
/// exactness) are already `db_ops::IndexFreshness`, so this record
/// **embeds** it rather than restating it: one freshness record, one
/// freshness check, in one place. The mapping is:
///
/// | §14.22 field | here |
/// |---|---|
/// | `source_object` | [`source_object_id`] + [`source_kind`] |
/// | `source_revision` | `freshness.source_revision` |
/// | (source frontier) | `freshness.source_timestamp` |
/// | `generation` | [`generation`] (structure generation) |
/// | `mode` | [`mode`] |
/// | `rebuildable` | `mode == Rebuildable` |
/// | `resolved_timestamp` | [`resolved_timestamp`] |
/// | `stale_query_policy` | [`stale_query_policy`] |
///
/// `freshness.range_generation` remains the *range descriptor*
/// generation (§11.3) that `db_ops` checks; [`generation`] is the
/// derived structure's own build generation (§14.20's index generation,
/// §14.21's manifest generation). Two different numbers with two
/// different jobs, which is why both are present.
///
/// Wire layout ([`DERIVED_STATE_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [source_object_id:u32][source_kind:u8]
/// [freshness:IndexFreshness::WIRE_LEN bytes]
/// [generation:u32][mode:u8]
/// [resolved_timestamp:u64][rebuild_safe_snapshot:u64]
/// [stale_query_policy:u8]
/// ```
///
/// [`source_object_id`]: DerivedState::source_object_id
/// [`source_kind`]: DerivedState::source_kind
/// [`generation`]: DerivedState::generation
/// [`mode`]: DerivedState::mode
/// [`resolved_timestamp`]: DerivedState::resolved_timestamp
/// [`stale_query_policy`]: DerivedState::stale_query_policy
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DerivedState {
    pub source_object_id: u32,
    pub source_kind: ObjectKind,
    /// The `db_ops` freshness record. Supplies source revision, source
    /// timestamp, range generation, exactness, and advertised lag.
    pub freshness: IndexFreshness,
    /// The derived structure's own build generation.
    pub generation: u32,
    pub mode: DerivedMode,
    /// The frontier the structure will answer at. Never above
    /// `freshness.source_timestamp` — a structure cannot resolve past
    /// what it has consumed.
    pub resolved_timestamp: Timestamp,
    /// MVCC timestamp of a snapshot from which this structure can be
    /// rebuilt without replaying older history. `0` means **none
    /// exists**, which is the fail-closed default: with no rebuild-safe
    /// snapshot, compaction must preserve history back to the
    /// structure's cursor (§14.22).
    pub rebuild_safe_snapshot: Timestamp,
    pub stale_query_policy: StaleQueryPolicy,
}

impl DerivedState {
    /// Header (4) + 4 + 1 + freshness + 4 + 1 + 8 + 8 + 1.
    pub const WIRE_LEN: usize = 4 + 4 + 1 + IndexFreshness::WIRE_LEN + 4 + 1 + 8 + 8 + 1;

    /// §14.22's structural rules:
    ///
    /// - a `Synchronous` structure cannot lag (it is maintained inside
    ///   the writing transaction), so a resolved timestamp behind its
    ///   consumed source timestamp is a contradiction;
    /// - a resolved timestamp *ahead* of the consumed source timestamp
    ///   is the invariant-19 violation this whole section exists to
    ///   prevent, in stored form.
    pub fn check_invariants(&self) -> Result<(), ModelError> {
        if self.resolved_timestamp > self.freshness.source_timestamp {
            return Err(ModelError::ResolvedAheadOfSource {
                source_timestamp: self.freshness.source_timestamp,
                resolved_timestamp: self.resolved_timestamp,
            });
        }
        if self.mode == DerivedMode::Synchronous
            && self.resolved_timestamp != self.freshness.source_timestamp
        {
            return Err(ModelError::SynchronousDerivedLags {
                source_timestamp: self.freshness.source_timestamp,
                resolved_timestamp: self.resolved_timestamp,
            });
        }
        Ok(())
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&DERIVED_STATE_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.source_object_id.to_le_bytes());
        out[8] = self.source_kind as u8;
        let f_end = 9 + IndexFreshness::WIRE_LEN;
        self.freshness.encode(&mut out[9..f_end])?;
        out[f_end..f_end + 4].copy_from_slice(&self.generation.to_le_bytes());
        out[f_end + 4] = self.mode as u8;
        out[f_end + 5..f_end + 13].copy_from_slice(&self.resolved_timestamp.to_le_bytes());
        out[f_end + 13..f_end + 21].copy_from_slice(&self.rebuild_safe_snapshot.to_le_bytes());
        out[f_end + 21] = self.stale_query_policy as u8;
        Some(Self::WIRE_LEN)
    }

    /// Fails closed on unknown version, length mismatch, unknown
    /// discriminant, an unreadable embedded freshness record, or a
    /// record that violates its own invariants — a derived structure
    /// that claims a frontier ahead of its cursor cannot be read back
    /// off disk.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != DERIVED_STATE_VERSION {
            return None;
        }
        if u16::from_le_bytes(src[2..4].try_into().ok()?) as usize != Self::WIRE_LEN - 4 {
            return None;
        }
        let f_end = 9 + IndexFreshness::WIRE_LEN;
        let d = Self {
            source_object_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            source_kind: ObjectKind::from_u8(src[8])?,
            freshness: IndexFreshness::decode(src.get(9..f_end)?)?,
            generation: u32::from_le_bytes(src[f_end..f_end + 4].try_into().ok()?),
            mode: DerivedMode::from_u8(src[f_end + 4])?,
            resolved_timestamp: u64::from_le_bytes(src[f_end + 5..f_end + 13].try_into().ok()?),
            rebuild_safe_snapshot: u64::from_le_bytes(src[f_end + 13..f_end + 21].try_into().ok()?),
            stale_query_policy: StaleQueryPolicy::from_u8(src[f_end + 21])?,
        };
        d.check_invariants().ok()?;
        Some(d)
    }
}

/// What a query asks a derived structure for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryFreshness {
    /// The MVCC timestamp the answer must reflect.
    pub required_timestamp: Timestamp,
    /// The range descriptor generation the answer must be built under
    /// (§11.3).
    pub required_generation: u32,
    /// `Synchronous` means “exact, no frontier” — only a synchronous
    /// derived structure can satisfy it.
    pub required_exactness: IndexExactness,
}

/// A derived structure's honest answer that it *can* serve the query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Served {
    /// The frontier the answer reflects. Always reported, never
    /// implied: an answer carries its own timestamp so a caller can
    /// label it (§14.20, §14.21).
    pub resolved_timestamp: Timestamp,
    /// The derived structure's build generation, for the caller's label.
    pub generation: u32,
    /// True only for [`DerivedMode::Synchronous`].
    pub exact: bool,
}

/// The honest ways a derived structure fails to serve a query at the
/// requested freshness. Every variant carries the numbers, because
/// §21 invariant 19 is about *reporting*, not about failing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Degradation {
    /// The structure refuses: policy is [`StaleQueryPolicy::Refuse`], or
    /// the request is unsatisfiable in principle (a disposable cache
    /// asked for exactness, a generation mismatch, a synchronous
    /// structure advertising lag).
    Refused {
        reason: ModelError,
        reached: Timestamp,
        generation: u32,
    },
    /// Served behind the request under [`StaleQueryPolicy::Report`]: the
    /// caller **must** surface `reached` to its user.
    StaleReported {
        required: Timestamp,
        reached: Timestamp,
        generation: u32,
    },
    /// Served behind the request under [`StaleQueryPolicy::Serve`]: the
    /// answer is labelled with `reached` but the caller is not obliged
    /// to surface it. Still not “current” — nothing here ever claims
    /// currency it lacks.
    StaleServed {
        required: Timestamp,
        reached: Timestamp,
        generation: u32,
    },
}

/// §21 invariant 19 as one function: **a derived structure can never
/// silently claim freshness or exactness it has not reached.**
///
/// The order of the gates is the contract:
///
/// 1. the structure's own record must be self-consistent;
/// 2. `db_ops::index_satisfies_freshness` runs first and unmodified —
///    the generation rule and the “synchronous cannot lag” rule already
///    live there and are not restated here;
/// 3. an exactness requirement of `Synchronous` is satisfiable only by
///    [`DerivedMode::Synchronous`] — including for a disposable cache,
///    which has no defined relationship to any source timestamp at all;
/// 4. only then does the frontier comparison happen, and its outcome is
///    routed by the structure's own declared
///    [`StaleQueryPolicy`] — never by the caller's preference.
///
/// `Ok(Served)` means the structure genuinely reached the request.
/// Everything else is a [`Degradation`] carrying the reached frontier,
/// so no path exists on which a caller can be told “fine” without a
/// timestamp.
pub fn derived_satisfies(
    query: &QueryFreshness,
    derived: &DerivedState,
) -> Result<Served, Degradation> {
    let reached = derived.resolved_timestamp;
    let generation = derived.generation;
    let refuse = |reason: ModelError| Degradation::Refused {
        reason,
        reached,
        generation,
    };

    derived.check_invariants().map_err(refuse)?;

    // Gate 2: delegate the shared index rules to db_ops. Note the
    // *resolved* timestamp is what a derived structure answers at, so
    // the delegated check is run against a copy whose source timestamp
    // is the frontier. That is deliberate: `freshness.source_timestamp`
    // is what the structure has consumed, `resolved_timestamp` is what
    // it will answer at, and only the latter may be claimed.
    let mut effective = derived.freshness;
    effective.source_timestamp = reached;
    if let Err(e) = index_satisfies_freshness(
        &effective,
        query.required_timestamp,
        query.required_generation,
    ) {
        match e {
            // A pure frontier shortfall is not a hard refusal yet — the
            // structure's stale policy decides. Every other delegated
            // error (generation mismatch, synchronous-claims-lag) is.
            FreshnessError::BehindRequiredTimestamp { .. } => {}
            other => return Err(refuse(ModelError::Freshness(other))),
        }
    }

    // Gate 3: exactness is a property of the mode, not of luck.
    if query.required_exactness == IndexExactness::Synchronous
        && derived.mode != DerivedMode::Synchronous
    {
        // Covers the disposable cache too: it has no defined
        // relationship to any source timestamp, so it can never be
        // exact, and saying so with the same error keeps one rule.
        return Err(refuse(ModelError::ExactnessUnavailable {
            mode: derived.mode,
        }));
    }

    // Gate 4: the frontier, routed by the structure's declared policy.
    if reached >= query.required_timestamp {
        return Ok(Served {
            resolved_timestamp: reached,
            generation,
            exact: derived.mode == DerivedMode::Synchronous,
        });
    }
    Err(match derived.stale_query_policy {
        StaleQueryPolicy::Refuse => refuse(ModelError::Freshness(
            FreshnessError::BehindRequiredTimestamp {
                required: query.required_timestamp,
                reached,
            },
        )),
        StaleQueryPolicy::Report => Degradation::StaleReported {
            required: query.required_timestamp,
            reached,
            generation,
        },
        StaleQueryPolicy::Serve => Degradation::StaleServed {
            required: query.required_timestamp,
            reached,
            generation,
        },
    })
}

/// §14.22: “Compaction preserves its source and cursor requirements
/// until a rebuild-safe snapshot or frontier exists.”
///
/// Returns the **oldest MVCC timestamp compaction must keep** on this
/// structure's behalf. `Timestamp::MAX` means “no claim”: this
/// structure constrains nothing. Feed the result into `db_ops`'s
/// retention arithmetic as one more bound; this function owns only the
/// per-structure rule, not the fleet minimum.
///
/// | mode | floor | why |
/// |---|---|---|
/// | `Synchronous` | `MAX` | maintained in-transaction; it never replays history |
/// | `DisposableCache` | `MAX` | droppable by definition; §14.22 grants it no claim |
/// | `AsynchronousWithFrontier` | cursor | it must replay from where it stopped |
/// | `Rebuildable`, no safe snapshot | cursor | fail closed: with no snapshot the change stream is the only route back |
/// | `Rebuildable`, safe snapshot | `min(snapshot, cursor)` | the snapshot is the new floor, but never above the cursor it still has to replay from |
///
/// The cursor is `min(resolved_timestamp, freshness.source_timestamp)`
/// — the older of “what it will answer at” and “what it has consumed”.
/// Taking the minimum is the fail-closed choice: if the two disagree,
/// the structure needs history back to the earlier of them.
pub fn compaction_floor_for_derived(derived: &DerivedState) -> Timestamp {
    let cursor = if derived.resolved_timestamp < derived.freshness.source_timestamp {
        derived.resolved_timestamp
    } else {
        derived.freshness.source_timestamp
    };
    match derived.mode {
        DerivedMode::Synchronous | DerivedMode::DisposableCache => Timestamp::MAX,
        DerivedMode::AsynchronousWithFrontier => cursor,
        DerivedMode::Rebuildable => {
            if derived.rebuild_safe_snapshot == 0 {
                cursor
            } else if derived.rebuild_safe_snapshot < cursor {
                derived.rebuild_safe_snapshot
            } else {
                cursor
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// 4. Document capability (§14.16)
// ══════════════════════════════════════════════════════════════════════

/// Maximum encoded index-value bytes in a document index key. Matches
/// the relational index bound: a document index value is the same kind
/// of ordered tuple.
pub const MAX_DOC_INDEX_VALUE_LEN: usize = 256;

/// Worst case `document/data/<collection>/<shard-key>/<doc-id>`:
/// collection id, plus two escaped-and-terminated variable components.
pub const DOCUMENT_DATA_KEY_MAX_LEN: usize = 4 + var_component_max_len(MAX_MODEL_ID_LEN) * 2;
/// Worst case `document/index/<index-id>/<encoded-values>/<doc-id>`.
pub const DOCUMENT_INDEX_KEY_MAX_LEN: usize =
    4 + var_component_max_len(MAX_DOC_INDEX_VALUE_LEN) + var_component_max_len(MAX_MODEL_ID_LEN);

/// §14.16's canonical document value tree types.
///
/// > Connector-specific JSON or binary encodings translate to a
/// > canonical versioned document value tree containing explicit null,
/// > boolean, integer, decimal, string, binary, timestamp, identifier,
/// > array, and object types.
///
/// Ten types, closed set, explicit `Null`. “Explicit null” is the whole
/// point and it is the first difference §28 says a generic engine
/// erases: a field present with value null and a field that is absent
/// are different facts, and only a type tag can carry that difference.
/// `Absent` is therefore deliberately **not** a variant — absence is
/// the lack of an entry, not a value.
///
/// Discriminants start at 1 so a zeroed buffer never decodes as a type.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocValueType {
    Null = 1,
    Boolean = 2,
    Integer = 3,
    Decimal = 4,
    String = 5,
    Binary = 6,
    Timestamp = 7,
    /// A document identifier (the `_id`-shaped opaque handle), distinct
    /// from `String` and from `Binary` because equality and index
    /// ordering for identifiers are identifier semantics, not text
    /// semantics.
    Identifier = 8,
    Array = 9,
    Object = 10,
}

impl DocValueType {
    pub const ALL: [Self; 10] = [
        Self::Null,
        Self::Boolean,
        Self::Integer,
        Self::Decimal,
        Self::String,
        Self::Binary,
        Self::Timestamp,
        Self::Identifier,
        Self::Array,
        Self::Object,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Null),
            2 => Some(Self::Boolean),
            3 => Some(Self::Integer),
            4 => Some(Self::Decimal),
            5 => Some(Self::String),
            6 => Some(Self::Binary),
            7 => Some(Self::Timestamp),
            8 => Some(Self::Identifier),
            9 => Some(Self::Array),
            10 => Some(Self::Object),
            _ => None,
        }
    }

    /// Can a value of this type be a key or index component? Containers
    /// cannot: an ordered encoding of a nested container is not a
    /// component, it is a traversal.
    pub const fn is_scalar(self) -> bool {
        !matches!(self, Self::Array | Self::Object)
    }
}

/// §14.16's canonical deterministic mutations. Named individually
/// because they are individually deterministic: “update” as a single
/// opaque verb is the generic-engine failure §28 rejects.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocMutationKind {
    Replace = 1,
    PathSet = 2,
    PathUnset = 3,
    NumericUpdate = 4,
    ArrayUpdate = 5,
    CompareAndPatch = 6,
    Delete = 7,
}

impl DocMutationKind {
    pub const ALL: [Self; 7] = [
        Self::Replace,
        Self::PathSet,
        Self::PathUnset,
        Self::NumericUpdate,
        Self::ArrayUpdate,
        Self::CompareAndPatch,
        Self::Delete,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Replace),
            2 => Some(Self::PathSet),
            3 => Some(Self::PathUnset),
            4 => Some(Self::NumericUpdate),
            5 => Some(Self::ArrayUpdate),
            6 => Some(Self::CompareAndPatch),
            7 => Some(Self::Delete),
            _ => None,
        }
    }
}

/// How the mutation reached storage (§14.16's closing prohibition).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationOrigin {
    /// One canonical Lattice transaction, fenced (§14.15).
    FencedTransaction = 1,
    /// A connector read the document, computed a new one, and wrote it
    /// back with no fence. §14.16: “It must not be implemented as an
    /// unfenced connector-side read followed by write.” Representable
    /// precisely so it can be refused.
    UnfencedConnectorReadWrite = 2,
}

impl MutationOrigin {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::FencedTransaction),
            2 => Some(Self::UnfencedConnectorReadWrite),
            _ => None,
        }
    }
}

/// One document mutation, described by what its transaction covers.
///
/// The fields are the four things §14.16 requires the *same*
/// transaction to update, plus the origin. It is a description, not an
/// engine: the document worker builds one of these and asks
/// [`document_mutation_is_atomic_unit`] whether it is the atomic unit
/// the RFC demands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocumentMutation {
    pub collection_id: u32,
    pub kind: DocMutationKind,
    pub origin: MutationOrigin,
    /// The document record itself is in the transaction.
    pub updates_document: bool,
    /// Number of synchronous indexes the collection declares.
    pub synchronous_index_count: u16,
    /// Number of those the transaction actually updates.
    pub index_updates_in_txn: u16,
    /// Number of uniqueness records the collection declares.
    pub uniqueness_record_count: u16,
    /// Number of those the transaction actually writes.
    pub uniqueness_records_in_txn: u16,
    /// The change record is in the transaction (§15).
    pub writes_change_record: bool,
}

/// §14.16's atomicity rule as one predicate:
///
/// > A document mutation transactionally updates the document,
/// > synchronous indexes, uniqueness records, and change record. It
/// > must not be implemented as an unfenced connector-side read
/// > followed by write.
///
/// So: a fenced transaction, containing the document, **every**
/// declared synchronous index, **every** declared uniqueness record,
/// and the change record. “Most of the indexes” is not an atomic unit;
/// it is a partially maintained index nobody will notice until a query
/// misses a committed document, so the counts must match exactly rather
/// than merely being nonzero.
///
/// A `Delete` still updates the document (as a tombstone), still
/// removes index and uniqueness entries, and still emits a change
/// record, so it is not special-cased.
pub fn document_mutation_is_atomic_unit(m: &DocumentMutation) -> bool {
    m.origin == MutationOrigin::FencedTransaction
        && m.updates_document
        && m.index_updates_in_txn == m.synchronous_index_count
        && m.uniqueness_records_in_txn == m.uniqueness_record_count
        && m.writes_change_record
}

/// `document/data/<collection-id>/<shard-key>/<document-id>` user key:
///
/// ```text
/// [collection_id:u32 BE][esc(shard_key)][0x00 0x01][esc(doc_id)][0x00 0x01]
/// ```
///
/// Hand to `internal_key::encode` with [`KS_DOCUMENT_DATA`].
///
/// The shard key precedes the document id because §14.16 routes on it:
/// “complete shard keys route directly; incomplete keys produce an
/// explicit bounded fanout plan”. With the shard key in front, a
/// complete shard key is a *prefix scan of one point*, and an
/// incomplete one is visibly a fanout rather than a hidden full scan —
/// the layout makes the difference structural instead of advisory.
pub fn encode_document_data_key(
    out: &mut [u8],
    collection_id: u32,
    shard_key: &[u8],
    document_id: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(collection_id)?;
    w.var(shard_key, MAX_MODEL_ID_LEN)?;
    w.var(document_id, MAX_MODEL_ID_LEN)?;
    w.finish()
}

/// Decode a document data key. Returns
/// `(collection_id, shard_key_len, document_id_len)` with the plain
/// bytes written into the two output buffers.
pub fn decode_document_data_key(
    src: &[u8],
    shard_out: &mut [u8],
    doc_out: &mut [u8],
) -> Option<(u32, usize, usize)> {
    let mut r = KeyReader::new(src);
    let collection_id = r.u32()?;
    let s = r.var(shard_out)?;
    let d = r.var(doc_out)?;
    r.end()?;
    Some((collection_id, s, d))
}

/// The scan prefix for one collection's documents, or for one shard key
/// within it when `shard_key` is `Some` (§14.16's “complete shard keys
/// route directly”).
pub fn encode_document_data_prefix(
    out: &mut [u8],
    collection_id: u32,
    shard_key: Option<&[u8]>,
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(collection_id)?;
    if let Some(sk) = shard_key {
        w.var(sk, MAX_MODEL_ID_LEN)?;
    }
    w.finish()
}

/// `document/index/<index-id>/<encoded-values>/<document-id>` user key:
///
/// ```text
/// [index_id:u32 BE][esc(encoded_values)][0x00 0x01][esc(doc_id)][0x00 0x01]
/// ```
///
/// `encoded_values` is a `relational::encode_index_value` tuple — the
/// document layer does not grow a second ordered encoder (§14.2's type
/// system is the shared one). The document id sits *inside* the key for
/// the same reason a primary key does in `db_ops::IndexEntry`: a prefix
/// scan of one index value yields that value's documents, in document
/// id order, with no secondary filter.
pub fn encode_document_index_key(
    out: &mut [u8],
    index_id: u32,
    encoded_values: &[u8],
    document_id: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.var(encoded_values, MAX_DOC_INDEX_VALUE_LEN)?;
    w.var(document_id, MAX_MODEL_ID_LEN)?;
    w.finish()
}

pub fn decode_document_index_key(
    src: &[u8],
    values_out: &mut [u8],
    doc_out: &mut [u8],
) -> Option<(u32, usize, usize)> {
    let mut r = KeyReader::new(src);
    let index_id = r.u32()?;
    let v = r.var(values_out)?;
    let d = r.var(doc_out)?;
    r.end()?;
    Some((index_id, v, d))
}

/// `document/index/<index-id>/<encoded-values>/` scan prefix: exactly
/// the documents carrying one index value, in document-id order.
pub fn encode_document_index_prefix(
    out: &mut [u8],
    index_id: u32,
    encoded_values: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.var(encoded_values, MAX_DOC_INDEX_VALUE_LEN)?;
    w.finish()
}

// ══════════════════════════════════════════════════════════════════════
// 5. Wide-column capability (§14.17)
// ══════════════════════════════════════════════════════════════════════

/// Maximum clustering-key columns in one wide-column table. Matches
/// `relational::MAX_KEY_COLUMNS`: a clustering key is a key.
pub const MAX_CLUSTERING_COLUMNS: usize = MAX_KEY_COLUMNS;

/// Worst case
/// `wide/<table>/<partition-key>/<clustering-key>/<column>/<version>`.
pub const WIDE_KEY_MAX_LEN: usize = 4
    + var_component_max_len(MAX_MODEL_ID_LEN)
    + MAX_CLUSTERING_COLUMNS * MAX_ORDERED_VALUE_LEN
    + 4
    + 8;

/// Clustering order for one clustering column. §14.17: “Partition-key
/// locality and clustering order are explicit schema properties.”
/// Explicit means the encoder takes it — a table declaring
/// `CLUSTERING ORDER BY (ts DESC)` must physically sort that way, not
/// re-sort after the scan.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Ascending = 1,
    Descending = 2,
}

impl SortDirection {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Ascending),
            2 => Some(Self::Descending),
            _ => None,
        }
    }
}

/// Encode one clustering component, honouring its direction.
///
/// Ascending is `relational::encode_value_ordered` verbatim. Descending
/// is the **bitwise complement of those same bytes**, which is exactly
/// order-reversing (`a < b` byte-wise implies `!a > !b` byte-wise) and
/// preserves self-delimitation: the terminator `0x00 0x01` becomes
/// `0xFF 0xFE` and the escape `0x00 0xFF` becomes `0xFF 0x00`, so a
/// complementing reader finds the same boundaries. Note that the
/// prefix rule flips with the order, as it must: under descending, a
/// proper prefix sorts *after* its extensions, because the complemented
/// terminator (`0xFE`) is above the complemented escape byte (`0x00`).
///
/// Complement rather than a separate descending encoder because the
/// alternative is a second ordered encoding that has to agree with the
/// first forever.
fn encode_clustering_component(
    out: &mut [u8],
    ty: LogicalType,
    v: Value<'_>,
    dir: SortDirection,
) -> Option<usize> {
    let n = encode_value_ordered(out, ty, v)?;
    if dir == SortDirection::Descending {
        for b in out[..n].iter_mut() {
            *b = !*b;
        }
    }
    Some(n)
}

/// A clustering key: the typed column values and, per column, the
/// schema's declared sort direction.
///
/// The three slices travel together because they must agree in arity —
/// a clustering key encoded against the wrong direction list sorts
/// wrong forever, and there is no later check that can detect it. One
/// struct, one arity check, in [`ClusteringKey::check`].
///
/// `values` may be **shorter** than `types`/`directions`: that is the
/// prefix form, and it is exactly the scan bound for “every cell whose
/// leading clustering columns equal these”.
#[derive(Clone, Copy, Debug)]
pub struct ClusteringKey<'a> {
    pub types: &'a [LogicalType],
    pub values: &'a [Value<'a>],
    pub directions: &'a [SortDirection],
}

impl ClusteringKey<'_> {
    /// Arity and bound agreement. `full` demands a complete key (every
    /// declared column present), which a cell key requires and a scan
    /// prefix does not.
    pub fn check(&self, full: bool) -> Option<()> {
        if self.types.len() != self.directions.len()
            || self.values.len() > self.types.len()
            || self.types.len() > MAX_CLUSTERING_COLUMNS
        {
            return None;
        }
        if full && self.values.len() != self.types.len() {
            return None;
        }
        Some(())
    }
}

/// `wide/<table-id>/<partition-key>/<clustering-key>/<column-id>/<version>`
/// user key:
///
/// ```text
/// [table_id:u32 BE]
/// [esc(partition_key)][0x00 0x01]
/// [dir(ordered(c0))][dir(ordered(c1))]...
/// [column_id:u32 BE]
/// [!version:u64 BE]
/// ```
///
/// Hand to `internal_key::encode` with [`KS_WIDE_TABLE`].
///
/// Three ordering properties, all load-bearing:
///
/// - one partition's cells are contiguous, because the fixed-width
///   table id and the escaped-terminated partition key are a strict
///   prefix of every one of them (§14.17's “partition-key locality”);
/// - within a partition, cells sort by the clustering key in the
///   schema's declared per-column direction;
/// - within a cell, `!version` puts the **newest** logical cell version
///   first, matching `internal_key`'s treatment of the MVCC timestamp.
///   The version here is the §14.17 *logical cell timestamp*, a
///   committed input — see [`cell_timestamp_is_committed_input`].
///
/// A complete clustering key is required ([`ClusteringKey::check`] with
/// `full`): a cell key with a partial clustering key is not a cell key,
/// it is a scan bound, and the two must not be confusable.
pub fn encode_wide_key(
    out: &mut [u8],
    table_id: u32,
    partition_key: &[u8],
    clustering: &ClusteringKey<'_>,
    column_id: u32,
    version: u64,
) -> Option<usize> {
    clustering.check(true)?;
    let n = encode_wide_clustering_prefix(out, table_id, partition_key, clustering)?;
    let mut w = KeyWriter::new(out.get_mut(n..)?);
    w.u32(column_id)?;
    w.u64_desc(version)?;
    Some(n + w.finish()?)
}

/// The partial form: table id, partition key, and the first
/// `clustering.values.len()` clustering columns. This is the scan bound
/// for “every cell whose leading clustering columns equal these”, and
/// with no values it is the whole-partition bound.
pub fn encode_wide_clustering_prefix(
    out: &mut [u8],
    table_id: u32,
    partition_key: &[u8],
    clustering: &ClusteringKey<'_>,
) -> Option<usize> {
    clustering.check(false)?;
    let mut w = KeyWriter::new(out);
    w.u32(table_id)?;
    w.var(partition_key, MAX_MODEL_ID_LEN)?;
    let mut n = w.finish()?;
    for (i, v) in clustering.values.iter().enumerate() {
        n += encode_clustering_component(
            out.get_mut(n..)?,
            clustering.types[i],
            *v,
            clustering.directions[i],
        )?;
    }
    if n > WIDE_KEY_MAX_LEN {
        return None;
    }
    Some(n)
}

/// Scratch bytes [`decode_wide_key`] needs: one full unescape window per
/// clustering column, exactly as `relational::primary_key_scratch_len`.
pub const fn wide_scratch_len(clustering_columns: usize) -> usize {
    clustering_columns * MAX_TEXT_LEN
}

/// Decode a complete wide-column cell key.
///
/// Returns `(table_id, partition_key_len, clustering_columns,
/// column_id, version)`. Descending components are complemented back
/// into a bounded stack window before being handed to
/// `relational::decode_value_ordered`, which is the exact inverse of
/// the encode path. Fails closed on truncation, arity disagreement,
/// undersized scratch, or trailing bytes.
pub fn decode_wide_key<'b>(
    src: &[u8],
    types: &[LogicalType],
    directions: &[SortDirection],
    partition_out: &mut [u8],
    text_out: &'b mut [u8],
    values_out: &mut [Value<'b>],
) -> Option<(u32, usize, usize, u32, u64)> {
    if types.len() != directions.len()
        || types.len() > values_out.len()
        || types.len() > MAX_CLUSTERING_COLUMNS
        || text_out.len() < wide_scratch_len(types.len())
    {
        return None;
    }
    let mut r = KeyReader::new(src);
    let table_id = r.u32()?;
    let partition_len = r.var(partition_out)?;

    let mut rest: &'b mut [u8] = text_out;
    let mut consumed = 0usize;
    // One complement window, reused per descending column.
    let mut flip = [0u8; MAX_ORDERED_VALUE_LEN];
    {
        let body = r.remaining();
        for (i, ty) in types.iter().enumerate() {
            let (window, tail) = core::mem::take(&mut rest).split_at_mut(MAX_TEXT_LEN);
            rest = tail;
            let tailbytes = body.get(consumed..)?;
            let used = match directions[i] {
                SortDirection::Ascending => {
                    let (v, used) = decode_value_ordered(tailbytes, *ty, window)?;
                    values_out[i] = v;
                    used
                }
                SortDirection::Descending => {
                    let w = tailbytes.len().min(MAX_ORDERED_VALUE_LEN);
                    for k in 0..w {
                        flip[k] = !tailbytes[k];
                    }
                    let (v, used) = decode_value_ordered(&flip[..w], *ty, window)?;
                    values_out[i] = v;
                    used
                }
            };
            consumed += used;
        }
    }
    let tail = r.remaining().get(consumed..)?;
    let mut t = KeyReader::new(tail);
    let column_id = t.u32()?;
    let version = t.u64_desc()?;
    t.end()?;
    Some((table_id, partition_len, types.len(), column_id, version))
}

/// Where a cell's logical timestamp came from (§14.17).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellTimestampSource {
    /// Supplied by the client and committed through the log: every
    /// replica applies the same number.
    CommittedInput = 1,
    /// Read from a replica's local clock at apply time. §14.17 forbids
    /// it (“never sampled independently by replicas”) and §21 invariant
    /// 3 says why: local time must not affect logical outcome.
    /// Representable so it can be refused, not so it can be used.
    ReplicaSampled = 2,
}

impl CellTimestampSource {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::CommittedInput),
            2 => Some(Self::ReplicaSampled),
            _ => None,
        }
    }
}

/// One wide-column cell's identity and timestamp provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cell {
    pub table_id: u32,
    pub column_id: u32,
    /// §14.17's logical cell timestamp: the value that decides last-
    /// write-wins between two cells.
    pub logical_timestamp: u64,
    pub timestamp_source: CellTimestampSource,
}

/// §14.17: “Logical cell timestamps are committed inputs and never
/// sampled independently by replicas.”
///
/// A single predicate rather than a `Result` because there is exactly
/// one way to fail it and the caller's response is always the same
/// refusal ([`ModelError::CellTimestampSampled`]). A zero timestamp is
/// also rejected: a cell whose last-write-wins comparand is the default
/// value of an uninitialized field is not a committed input either.
pub fn cell_timestamp_is_committed_input(cell: &Cell) -> bool {
    cell.timestamp_source == CellTimestampSource::CommittedInput && cell.logical_timestamp != 0
}

/// How a table splits a logical partition that would otherwise grow
/// without bound (§14.17).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketingStrategy {
    /// No bucketing. Legal only while the partition stays within its
    /// declared bounds; the moment it does not,
    /// [`check_partition_bounds`] refuses, which is precisely §14.17's
    /// “they cannot silently become unbounded single-range hot spots”.
    None = 1,
    /// Partition key is extended with a time bucket of
    /// [`PartitionBounds::bucket_parameter`] milliseconds.
    TimeBucket = 2,
    /// Partition key is extended with `hash(clustering) %
    /// bucket_parameter`.
    HashBucket = 3,
    /// Partition key is extended with a monotonically increasing
    /// sequence number that rolls when the bound is reached.
    SequenceBucket = 4,
}

impl BucketingStrategy {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::None),
            2 => Some(Self::TimeBucket),
            3 => Some(Self::HashBucket),
            4 => Some(Self::SequenceBucket),
            _ => None,
        }
    }
}

/// A table's declared logical-partition bounds (§14.17).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionBounds {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub strategy: BucketingStrategy,
    /// Strategy parameter: bucket width in ms, bucket count, or rows
    /// per sequence bucket. Must be nonzero for every strategy other
    /// than [`BucketingStrategy::None`].
    pub bucket_parameter: u64,
}

impl PartitionBounds {
    /// A bound of zero is not a bound, and a bucketing strategy with no
    /// parameter is not a strategy. Both are refused at declaration
    /// time, because §14.17 requires *configured* bounds and a
    /// *declared* strategy — the failure mode this prevents is a table
    /// that appears to have a policy and does not.
    pub fn check_invariants(&self) -> Result<(), ModelError> {
        if self.max_rows == 0 || self.max_bytes == 0 {
            return Err(ModelError::Malformed);
        }
        if self.strategy != BucketingStrategy::None && self.bucket_parameter == 0 {
            return Err(ModelError::Malformed);
        }
        Ok(())
    }
}

/// §14.17's partition-size rule: a logical partition over its declared
/// bounds is legal **only** when the table declares a bucketing
/// strategy that will split it. With [`BucketingStrategy::None`] the
/// same overflow is [`ModelError::PartitionUnbounded`] — the silent hot
/// range, made loud.
///
/// Returns `Ok(())` for a partition within bounds regardless of
/// strategy; bucketing is a growth plan, not a requirement to bucket
/// small partitions.
pub fn check_partition_bounds(
    bounds: &PartitionBounds,
    observed_rows: u64,
    observed_bytes: u64,
) -> Result<(), ModelError> {
    bounds.check_invariants()?;
    let over = observed_rows > bounds.max_rows || observed_bytes > bounds.max_bytes;
    if over && bounds.strategy == BucketingStrategy::None {
        return Err(ModelError::PartitionUnbounded {
            observed_rows,
            max_rows: bounds.max_rows,
            observed_bytes,
            max_bytes: bounds.max_bytes,
        });
    }
    Ok(())
}

// ══════════════════════════════════════════════════════════════════════
// 6. Graph capability (§14.18)
// ══════════════════════════════════════════════════════════════════════

/// Maximum length of a vertex identifier. Shorter than
/// [`MAX_MODEL_ID_LEN`] because an edge key holds **two** of them and
/// the mutation plan holds several edge keys at once, on the stack.
pub const MAX_VERTEX_ID_LEN: usize = 32;

/// Worst case `graph/edge-out/<graph>/<src>/<label>/<dst>`:
/// graph id, escaped source, label id, escaped target.
pub const GRAPH_EDGE_KEY_MAX_LEN: usize = 4 + var_component_max_len(MAX_VERTEX_ID_LEN) * 2 + 4;
/// Worst case `graph/vertex/<graph>/<vertex>`.
pub const GRAPH_VERTEX_KEY_MAX_LEN: usize = 4 + var_component_max_len(MAX_VERTEX_ID_LEN);

/// Maximum synchronous property indexes one edge mutation maintains.
/// A format bound: §21 invariant 14 requires bounded work, so an edge
/// write with more declared indexes than this is a typed refusal rather
/// than an unbounded plan.
pub const MAX_GRAPH_SYNC_INDEXES: usize = 4;

/// Entries in a complete edge mutation plan: out-adjacency,
/// in-adjacency, and up to [`MAX_GRAPH_SYNC_INDEXES`] property-index
/// entries.
pub const MAX_EDGE_PLAN_ENTRIES: usize = 2 + MAX_GRAPH_SYNC_INDEXES;

/// `graph/vertex/<graph-id>/<vertex-id>` user key:
/// `[graph_id:u32 BE][esc(vertex_id)][0x00 0x01]`.
pub fn encode_graph_vertex_key(out: &mut [u8], graph_id: u32, vertex_id: &[u8]) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(graph_id)?;
    w.var(vertex_id, MAX_VERTEX_ID_LEN)?;
    w.finish()
}

pub fn decode_graph_vertex_key(src: &[u8], vertex_out: &mut [u8]) -> Option<(u32, usize)> {
    let mut r = KeyReader::new(src);
    let graph_id = r.u32()?;
    let v = r.var(vertex_out)?;
    r.end()?;
    Some((graph_id, v))
}

/// `graph/edge-out/<graph-id>/<source-id>/<label>/<target-id>` user key:
///
/// ```text
/// [graph_id:u32 BE][esc(source_id)][0x00 0x01][label_id:u32 BE][esc(target_id)][0x00 0x01]
/// ```
///
/// A prefix scan of `[graph_id][esc(source)][TERM]` is “all out-edges of
/// this vertex”, and extending it with `[label_id]` narrows to one
/// label — bounded adjacency expansion (§14.18) as a scan bound rather
/// than a filter.
pub fn encode_graph_edge_out_key(
    out: &mut [u8],
    graph_id: u32,
    source_id: &[u8],
    label_id: u32,
    target_id: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(graph_id)?;
    w.var(source_id, MAX_VERTEX_ID_LEN)?;
    w.u32(label_id)?;
    w.var(target_id, MAX_VERTEX_ID_LEN)?;
    w.finish()
}

/// `graph/edge-in/<graph-id>/<target-id>/<label>/<source-id>` user key.
/// Byte-identical layout to [`encode_graph_edge_out_key`] with the two
/// endpoints swapped — that symmetry is the adjacency invariant, and
/// [`edge_in_from_out`] / [`edge_out_from_in`] make it executable.
pub fn encode_graph_edge_in_key(
    out: &mut [u8],
    graph_id: u32,
    target_id: &[u8],
    label_id: u32,
    source_id: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(graph_id)?;
    w.var(target_id, MAX_VERTEX_ID_LEN)?;
    w.u32(label_id)?;
    w.var(source_id, MAX_VERTEX_ID_LEN)?;
    w.finish()
}

/// Decode either adjacency key. Returns
/// `(graph_id, first_len, label_id, second_len)`; “first” is the source
/// for an out-edge and the target for an in-edge, per the keyspace the
/// key came from.
pub fn decode_graph_edge_key(
    src: &[u8],
    first_out: &mut [u8],
    second_out: &mut [u8],
) -> Option<(u32, usize, u32, usize)> {
    let mut r = KeyReader::new(src);
    let graph_id = r.u32()?;
    let a = r.var(first_out)?;
    let label_id = r.u32()?;
    let b = r.var(second_out)?;
    r.end()?;
    Some((graph_id, a, label_id, b))
}

/// An edge, as the mutation planner sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Edge {
    pub graph_id: u32,
    pub label_id: u32,
    source: [u8; MAX_VERTEX_ID_LEN],
    source_len: u8,
    target: [u8; MAX_VERTEX_ID_LEN],
    target_len: u8,
}

impl Edge {
    /// Fails closed on an endpoint longer than [`MAX_VERTEX_ID_LEN`] or
    /// an empty endpoint — an edge with an anonymous end is not an edge.
    pub fn new(graph_id: u32, source: &[u8], label_id: u32, target: &[u8]) -> Option<Self> {
        if source.is_empty()
            || target.is_empty()
            || source.len() > MAX_VERTEX_ID_LEN
            || target.len() > MAX_VERTEX_ID_LEN
        {
            return None;
        }
        let mut e = Self {
            graph_id,
            label_id,
            source: [0u8; MAX_VERTEX_ID_LEN],
            source_len: source.len() as u8,
            target: [0u8; MAX_VERTEX_ID_LEN],
            target_len: target.len() as u8,
        };
        e.source[..source.len()].copy_from_slice(source);
        e.target[..target.len()].copy_from_slice(target);
        Some(e)
    }

    pub fn source(&self) -> &[u8] {
        &self.source[..self.source_len as usize]
    }

    pub fn target(&self) -> &[u8] {
        &self.target[..self.target_len as usize]
    }
}

/// Derive an edge's in-adjacency key from its out-adjacency key.
///
/// This is §14.18's adjacency invariant in its sharpest form: the two
/// directions carry the same three facts, so either key determines the
/// other, and a plan that writes one without the other is detectably
/// incomplete rather than merely unlucky.
pub fn edge_in_from_out(out_key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let mut a = [0u8; MAX_VERTEX_ID_LEN];
    let mut b = [0u8; MAX_VERTEX_ID_LEN];
    let (graph_id, alen, label_id, blen) = decode_graph_edge_key(out_key, &mut a, &mut b)?;
    encode_graph_edge_in_key(dst, graph_id, &b[..blen], label_id, &a[..alen])
}

/// Derive an edge's out-adjacency key from its in-adjacency key. The
/// exact inverse of [`edge_in_from_out`].
pub fn edge_out_from_in(in_key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let mut a = [0u8; MAX_VERTEX_ID_LEN];
    let mut b = [0u8; MAX_VERTEX_ID_LEN];
    let (graph_id, alen, label_id, blen) = decode_graph_edge_key(in_key, &mut a, &mut b)?;
    encode_graph_edge_out_key(dst, graph_id, &b[..blen], label_id, &a[..alen])
}

/// What one planned key in an edge mutation is for.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeEntryRole {
    /// The edge record and its out-adjacency: one key, because in this
    /// layout the out-adjacency key *is* where the edge's properties
    /// live. The in-adjacency is a pure mirror.
    EdgeRecordAndOutAdjacency = 1,
    /// The in-adjacency mirror.
    InAdjacency = 2,
    /// A synchronous property-index entry.
    PropertyIndex = 3,
}

/// Whether the plan creates or deletes.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeOp {
    Create = 1,
    Delete = 2,
}

/// One key an edge mutation must touch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EdgePlanEntry {
    pub role: EdgeEntryRole,
    pub keyspace: u32,
    key: [u8; GRAPH_EDGE_KEY_MAX_LEN],
    key_len: u16,
}

impl EdgePlanEntry {
    pub fn key(&self) -> &[u8] {
        &self.key[..self.key_len as usize]
    }
}

/// §14.18's bounded mutation set:
///
/// > Creating or deleting an edge atomically maintains the edge record,
/// > incoming and outgoing adjacency, synchronous property indexes, and
/// > change record.
///
/// A fixed array, never a growable list: §21 invariant 14 requires
/// bounded work, so an edge with more synchronous indexes than
/// [`MAX_GRAPH_SYNC_INDEXES`] is a refusal, not a bigger plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EdgeMutationPlan {
    pub op: EdgeOp,
    entries: [EdgePlanEntry; MAX_EDGE_PLAN_ENTRIES],
    len: u8,
    /// The change record (§15) is in the same transaction. It is a flag
    /// rather than an entry because it is not a key in a model
    /// keyspace — the change feed owns its own encoding — and inventing
    /// a fake key for it would be less honest, not more.
    pub writes_change_record: bool,
}

impl EdgeMutationPlan {
    pub fn entries(&self) -> &[EdgePlanEntry] {
        &self.entries[..self.len as usize]
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Does the plan contain a key in this role?
    pub fn has_role(&self, role: EdgeEntryRole) -> bool {
        self.entries().iter().any(|e| e.role == role)
    }

    /// The complete-set check §14.18 demands: exactly one out entry,
    /// exactly one in entry, every declared property index, and the
    /// change record. Anything less is
    /// [`ModelError::EdgePlanIncomplete`] — a graph whose in-adjacency
    /// silently lags its out-adjacency answers reverse traversals
    /// wrongly with no way to notice.
    pub fn check_complete(&self, declared_indexes: usize) -> Result<(), ModelError> {
        let out_count = self
            .entries()
            .iter()
            .filter(|e| e.role == EdgeEntryRole::EdgeRecordAndOutAdjacency)
            .count();
        let in_count = self
            .entries()
            .iter()
            .filter(|e| e.role == EdgeEntryRole::InAdjacency)
            .count();
        let idx_count = self
            .entries()
            .iter()
            .filter(|e| e.role == EdgeEntryRole::PropertyIndex)
            .count();
        if out_count != 1 || in_count != 1 || idx_count != declared_indexes {
            return Err(ModelError::EdgePlanIncomplete);
        }
        if !self.writes_change_record {
            return Err(ModelError::EdgePlanIncomplete);
        }
        Ok(())
    }
}

/// Build the complete bounded key set one edge create/delete must touch
/// (§14.18).
///
/// `index_values` supplies one already-encoded ordered value per
/// declared synchronous property index, paired with the index id. The
/// plan's property-index keys reuse the *document* index layout
/// (`[index_id][esc(value)][TERM][esc(entity)][TERM]`) because a graph
/// property index and a document index are the same shape — a value to
/// an entity — and that similarity is a foundation, not a meaning.
///
/// Returns `None` on an oversized endpoint, more indexes than
/// [`MAX_GRAPH_SYNC_INDEXES`], or an oversized index value.
pub fn edge_mutation_plan(
    edge: &Edge,
    op: EdgeOp,
    index_values: &[(u32, &[u8])],
) -> Option<EdgeMutationPlan> {
    if index_values.len() > MAX_GRAPH_SYNC_INDEXES {
        return None;
    }
    let blank = EdgePlanEntry {
        role: EdgeEntryRole::PropertyIndex,
        keyspace: 0,
        key: [0u8; GRAPH_EDGE_KEY_MAX_LEN],
        key_len: 0,
    };
    let mut plan = EdgeMutationPlan {
        op,
        entries: [blank; MAX_EDGE_PLAN_ENTRIES],
        len: 0,
        writes_change_record: true,
    };
    let mut push = |role: EdgeEntryRole, keyspace: u32, key: &[u8]| -> Option<()> {
        let i = plan.len as usize;
        if i >= MAX_EDGE_PLAN_ENTRIES || key.len() > GRAPH_EDGE_KEY_MAX_LEN {
            return None;
        }
        plan.entries[i].role = role;
        plan.entries[i].keyspace = keyspace;
        plan.entries[i].key[..key.len()].copy_from_slice(key);
        plan.entries[i].key_len = key.len() as u16;
        plan.len += 1;
        Some(())
    };

    let mut buf = [0u8; GRAPH_EDGE_KEY_MAX_LEN];
    let n = encode_graph_edge_out_key(
        &mut buf,
        edge.graph_id,
        edge.source(),
        edge.label_id,
        edge.target(),
    )?;
    push(
        EdgeEntryRole::EdgeRecordAndOutAdjacency,
        KS_GRAPH_EDGE_OUT,
        &buf[..n],
    )?;

    let n = encode_graph_edge_in_key(
        &mut buf,
        edge.graph_id,
        edge.target(),
        edge.label_id,
        edge.source(),
    )?;
    push(EdgeEntryRole::InAdjacency, KS_GRAPH_EDGE_IN, &buf[..n])?;

    for (index_id, value) in index_values {
        let mut w = KeyWriter::new(&mut buf);
        w.u32(*index_id)?;
        w.var(value, MAX_DOC_INDEX_VALUE_LEN.min(MAX_MODEL_ID_LEN))?;
        w.var(edge.source(), MAX_VERTEX_ID_LEN)?;
        let n = w.finish()?;
        push(EdgeEntryRole::PropertyIndex, KS_GRAPH_INDEX, &buf[..n])?;
    }
    Some(plan)
}

/// What a traversal does when it hits one of its bounds (§14.18's
/// “partial-result policy”).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartialResultPolicy {
    /// Refuse the traversal. The caller gets an error, never a
    /// truncated answer it might mistake for a complete one.
    Refuse = 1,
    /// Return what was found, explicitly marked truncated with the
    /// bound that stopped it.
    ReportTruncated = 2,
}

impl PartialResultPolicy {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Refuse),
            2 => Some(Self::ReportTruncated),
            _ => None,
        }
    }
}

/// §14.18: “Every traversal declares maximum depth, frontier size,
/// visited state, bytes, work, deadline, and partial-result policy.”
///
/// Seven fields, all mandatory, because a traversal is the one model
/// operation whose cost is not bounded by its input size — this record
/// is what stops “find related things” from becoming a whole-graph
/// scan. `0` is not a legal value for any of the six numeric bounds:
/// an unset bound and an infinite bound are the same thing, and §21
/// invariant 14 forbids both.
///
/// Wire layout ([`TRAVERSAL_BOUNDS_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [max_depth:u16][max_frontier:u32][max_visited:u32]
/// [max_bytes:u64][max_work:u64][deadline_ms:u32]
/// [partial_result_policy:u8]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraversalBounds {
    pub max_depth: u16,
    pub max_frontier: u32,
    pub max_visited: u32,
    pub max_bytes: u64,
    pub max_work: u64,
    pub deadline_ms: u32,
    pub partial_result_policy: PartialResultPolicy,
}

impl TraversalBounds {
    /// Header (4) + 2 + 4 + 4 + 8 + 8 + 4 + 1.
    pub const WIRE_LEN: usize = 4 + 31;

    /// Names the *first* missing bound rather than returning a bare
    /// `false`, so an operator reading the refusal knows which
    /// declaration to add.
    pub fn check_declared(&self) -> Result<(), ModelError> {
        let missing = if self.max_depth == 0 {
            Some(TraversalBound::Depth)
        } else if self.max_frontier == 0 {
            Some(TraversalBound::Frontier)
        } else if self.max_visited == 0 {
            Some(TraversalBound::Visited)
        } else if self.max_bytes == 0 {
            Some(TraversalBound::Bytes)
        } else if self.max_work == 0 {
            Some(TraversalBound::Work)
        } else if self.deadline_ms == 0 {
            Some(TraversalBound::Deadline)
        } else {
            None
        };
        match missing {
            Some(bound) => Err(ModelError::TraversalBoundMissing { bound }),
            None => Ok(()),
        }
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&TRAVERSAL_BOUNDS_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..6].copy_from_slice(&self.max_depth.to_le_bytes());
        out[6..10].copy_from_slice(&self.max_frontier.to_le_bytes());
        out[10..14].copy_from_slice(&self.max_visited.to_le_bytes());
        out[14..22].copy_from_slice(&self.max_bytes.to_le_bytes());
        out[22..30].copy_from_slice(&self.max_work.to_le_bytes());
        out[30..34].copy_from_slice(&self.deadline_ms.to_le_bytes());
        out[34] = self.partial_result_policy as u8;
        Some(Self::WIRE_LEN)
    }

    /// Fails closed on unknown version, length mismatch, unknown
    /// policy discriminant, or an undeclared bound — a traversal spec
    /// with a hole in it cannot be read back off the wire.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != TRAVERSAL_BOUNDS_VERSION {
            return None;
        }
        if u16::from_le_bytes(src[2..4].try_into().ok()?) as usize != Self::WIRE_LEN - 4 {
            return None;
        }
        let b = Self {
            max_depth: u16::from_le_bytes(src[4..6].try_into().ok()?),
            max_frontier: u32::from_le_bytes(src[6..10].try_into().ok()?),
            max_visited: u32::from_le_bytes(src[10..14].try_into().ok()?),
            max_bytes: u64::from_le_bytes(src[14..22].try_into().ok()?),
            max_work: u64::from_le_bytes(src[22..30].try_into().ok()?),
            deadline_ms: u32::from_le_bytes(src[30..34].try_into().ok()?),
            partial_result_policy: PartialResultPolicy::from_u8(src[34])?,
        };
        b.check_declared().ok()?;
        Some(b)
    }
}

// ══════════════════════════════════════════════════════════════════════
// 7. Time-series capability (§14.19)
// ══════════════════════════════════════════════════════════════════════

/// Worst case `timeseries/series/<metric>/<labels-digest>`:
/// metric id plus a 16-byte digest.
pub const TIMESERIES_SERIES_KEY_LEN: usize = 4 + 16;
/// `timeseries/sample/<series-id>/<time-bucket>/<timestamp>`.
pub const TIMESERIES_SAMPLE_KEY_LEN: usize = 8 + 8 + 8;
/// `timeseries/rollup/<series-id>/<resolution>/<time-bucket>`.
pub const TIMESERIES_ROLLUP_KEY_LEN: usize = 8 + 4 + 8;

/// `timeseries/series/<metric>/<labels-digest>` user key:
/// `[metric_id:u32 BE][labels_digest:16]`.
///
/// The label set is a fixed-width digest, not the labels themselves: a
/// series key must be a bounded, stable identity, and label sets are
/// neither. The digest's preimage lives in the series record.
pub fn encode_timeseries_series_key(
    out: &mut [u8],
    metric_id: u32,
    labels_digest: &[u8; 16],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(metric_id)?;
    w.digest(labels_digest)?;
    w.finish()
}

pub fn decode_timeseries_series_key(src: &[u8]) -> Option<(u32, [u8; 16])> {
    let mut r = KeyReader::new(src);
    let metric_id = r.u32()?;
    let digest = r.digest()?;
    r.end()?;
    Some((metric_id, digest))
}

/// `timeseries/sample/<series-id>/<time-bucket>/<timestamp>` user key:
/// `[series_id:u64 BE][bucket:u64 BE][timestamp:u64 BE]`.
///
/// All three ascending — and this is the one place the house's
/// newest-first convention is deliberately **not** applied. A time
/// window is a forward range scan (`WHERE ts BETWEEN a AND b`), so
/// samples must sort oldest-first within a bucket. Newest-first is
/// right for a *version chain* (`internal_key`, and the wide-column
/// cell version) because there the newest is the answer; here every
/// sample is an answer. Two different orders for two different
/// meanings — §14.15's rule in one field.
pub fn encode_timeseries_sample_key(
    out: &mut [u8],
    series_id: u64,
    bucket: u64,
    timestamp: Timestamp,
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u64(series_id)?;
    w.u64(bucket)?;
    w.u64(timestamp)?;
    w.finish()
}

pub fn decode_timeseries_sample_key(src: &[u8]) -> Option<(u64, u64, Timestamp)> {
    let mut r = KeyReader::new(src);
    let series_id = r.u64()?;
    let bucket = r.u64()?;
    let timestamp = r.u64()?;
    r.end()?;
    Some((series_id, bucket, timestamp))
}

/// `timeseries/rollup/<series-id>/<resolution>/<time-bucket>` user key:
/// `[series_id:u64 BE][resolution_ms:u32 BE][bucket:u64 BE]`.
///
/// Resolution precedes bucket so one resolution's buckets are
/// contiguous and a query picks a resolution with a prefix rather than
/// a filter.
pub fn encode_timeseries_rollup_key(
    out: &mut [u8],
    series_id: u64,
    resolution_ms: u32,
    bucket: u64,
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u64(series_id)?;
    w.u32(resolution_ms)?;
    w.u64(bucket)?;
    w.finish()
}

pub fn decode_timeseries_rollup_key(src: &[u8]) -> Option<(u64, u32, u64)> {
    let mut r = KeyReader::new(src);
    let series_id = r.u64()?;
    let resolution_ms = r.u32()?;
    let bucket = r.u64()?;
    r.end()?;
    Some((series_id, resolution_ms, bucket))
}

/// What a series does with a sample older than its watermark (§14.19's
/// out-of-order policy).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LatePolicy {
    /// Refuse the sample. The window is closed.
    Reject = 1,
    /// Accept and repair every affected rollup in the same transaction.
    /// §14.19: “Backfill and late samples define whether affected
    /// rollups are repaired synchronously…”
    AcceptRepairSynchronously = 2,
    /// Accept, and move the affected rollups' resolved timestamp back
    /// so they report the repair as pending. “…or become current
    /// through a reported resolved timestamp.”
    AcceptReportResolvedTimestamp = 3,
}

impl LatePolicy {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Reject),
            2 => Some(Self::AcceptRepairSynchronously),
            3 => Some(Self::AcceptReportResolvedTimestamp),
            _ => None,
        }
    }
}

/// What happens to one submitted sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SampleAdmission {
    /// At or after the watermark: an ordinary append.
    Accepted,
    /// Before the watermark, accepted, and every rollup covering it
    /// must be repaired inside this transaction.
    AcceptedRepairRollups { affected_bucket: u64 },
    /// Before the watermark, accepted, and the affected rollups must
    /// move their resolved timestamp back to `regressed_resolved` so
    /// they never claim to include a sample they have not folded in
    /// (§21 invariant 19).
    AcceptedReportStale {
        affected_bucket: u64,
        regressed_resolved: Timestamp,
    },
}

/// §14.19's out-of-order/late-sample rule as one function.
///
/// A sample at or after `watermark` is ordinary. Below it, the series'
/// declared [`LatePolicy`] decides, and two of the three outcomes
/// impose obligations on the *rollups* rather than on the sample — a
/// late sample that quietly landed while a rollup went on reporting its
/// old value is exactly the “unreported second authority” §14.22
/// forbids.
///
/// `bucket_width_ms` must be nonzero (a zero-width bucket is not a
/// bucket).
pub fn classify_sample(
    sample_timestamp: Timestamp,
    watermark: Timestamp,
    bucket_width_ms: u64,
    policy: LatePolicy,
) -> Result<SampleAdmission, ModelError> {
    if bucket_width_ms == 0 {
        return Err(ModelError::Malformed);
    }
    if sample_timestamp >= watermark {
        return Ok(SampleAdmission::Accepted);
    }
    let affected_bucket = sample_timestamp / bucket_width_ms;
    match policy {
        LatePolicy::Reject => Err(ModelError::SampleTooLate {
            sample_timestamp,
            watermark,
        }),
        LatePolicy::AcceptRepairSynchronously => {
            Ok(SampleAdmission::AcceptedRepairRollups { affected_bucket })
        }
        LatePolicy::AcceptReportResolvedTimestamp => Ok(SampleAdmission::AcceptedReportStale {
            affected_bucket,
            // The rollups can no longer claim currency past the point
            // the late sample landed at.
            regressed_resolved: sample_timestamp,
        }),
    }
}

/// A time-series schema, reduced to the two facts this contract layer
/// needs to decide authority and retention.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeSeriesSchema {
    pub metric_id: u32,
    pub bucket_width_ms: u64,
    pub late_policy: LatePolicy,
    /// §14.19: “rollups are derived **unless a schema explicitly makes
    /// them authoritative**.” Explicit means this flag; the default is
    /// `false`, and §28 lists making derived state authoritative by
    /// accident as a rejected alternative.
    pub rollups_authoritative: bool,
}

/// §14.19's rollup-authority rule.
///
/// Samples and series metadata are always authoritative; compressed
/// segments are physical materialization and carry no authority at all.
/// Rollups are the only ambiguous case, and the ambiguity is resolved
/// by one declared flag rather than by whichever component wrote last.
pub fn rollup_authority(schema: &TimeSeriesSchema) -> StateAuthority {
    if schema.rollups_authoritative {
        StateAuthority::Authoritative
    } else {
        StateAuthority::Derived
    }
}

/// What a retention decision was based on (§14.19).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionBasis {
    /// A retention policy committed through the log. Replicated,
    /// deterministic.
    CommittedPolicy = 1,
    /// A protected timestamp held by a backup, watch, or derived
    /// structure (§18, §21 invariant 13).
    ProtectedTimestamp = 2,
    /// The mtime of a file on one node's disk. §14.19: “Retention
    /// advances through committed policy and protected timestamps, not
    /// local file age.” Representable so it can be refused; §21
    /// invariant 3 is the underlying reason — local state must not
    /// affect logical outcome.
    LocalFileAge = 3,
}

impl RetentionBasis {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::CommittedPolicy),
            2 => Some(Self::ProtectedTimestamp),
            3 => Some(Self::LocalFileAge),
            _ => None,
        }
    }
}

/// §14.19's retention rule: only a committed policy or a protected
/// timestamp may advance a retention horizon.
pub fn check_retention_basis(basis: RetentionBasis) -> Result<(), ModelError> {
    match basis {
        RetentionBasis::CommittedPolicy | RetentionBasis::ProtectedTimestamp => Ok(()),
        RetentionBasis::LocalFileAge => Err(ModelError::RetentionBasisNotCommitted { basis }),
    }
}

// ══════════════════════════════════════════════════════════════════════
// 8. Search capability (§14.20)
// ══════════════════════════════════════════════════════════════════════

/// Maximum indexed term length, bytes. A format bound; a longer token
/// is truncated by the *analyzer*, deliberately and visibly, never by
/// the encoder.
pub const MAX_TERM_LEN: usize = 48;

/// Worst case `search/posting/<index>/<field>/<term>/<doc>`.
pub const SEARCH_POSTING_KEY_MAX_LEN: usize =
    4 + 4 + var_component_max_len(MAX_TERM_LEN) + var_component_max_len(MAX_MODEL_ID_LEN);
/// Worst case `search/term-stats/<index>/<field>/<term>`.
pub const SEARCH_TERM_STATS_KEY_MAX_LEN: usize = 4 + 4 + var_component_max_len(MAX_TERM_LEN);

/// `search/posting/<index-id>/<field>/<term>/<document-id>` user key:
///
/// ```text
/// [index_id:u32 BE][field_id:u32 BE][esc(term)][0x00 0x01][esc(doc_id)][0x00 0x01]
/// ```
///
/// A posting list is the prefix scan of `[index][field][esc(term)][TERM]`,
/// yielding that term's documents in document-id order — which is what
/// makes an intersection of two posting lists a merge rather than a
/// sort. Term before document is therefore not a stylistic choice; it
/// is the shape the query operator depends on.
///
/// Note the *generation* is not in the key: postings for two generations
/// live in two index ids, because §14.20 requires a rebuild rather than
/// mixed postings, and a rebuild that shared a keyspace with the index
/// it replaces could not be abandoned cleanly.
pub fn encode_search_posting_key(
    out: &mut [u8],
    index_id: u32,
    field_id: u32,
    term: &[u8],
    document_id: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.u32(field_id)?;
    w.var(term, MAX_TERM_LEN)?;
    w.var(document_id, MAX_MODEL_ID_LEN)?;
    w.finish()
}

pub fn decode_search_posting_key(
    src: &[u8],
    term_out: &mut [u8],
    doc_out: &mut [u8],
) -> Option<(u32, u32, usize, usize)> {
    let mut r = KeyReader::new(src);
    let index_id = r.u32()?;
    let field_id = r.u32()?;
    let t = r.var(term_out)?;
    let d = r.var(doc_out)?;
    r.end()?;
    Some((index_id, field_id, t, d))
}

/// The posting-list scan prefix for one term.
pub fn encode_search_posting_prefix(
    out: &mut [u8],
    index_id: u32,
    field_id: u32,
    term: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.u32(field_id)?;
    w.var(term, MAX_TERM_LEN)?;
    w.finish()
}

/// `search/term-stats/<index-id>/<field>/<term>` user key. Same leading
/// components as a posting key, so a term's statistics and its postings
/// are found with the same composed prefix in two keyspaces.
pub fn encode_search_term_stats_key(
    out: &mut [u8],
    index_id: u32,
    field_id: u32,
    term: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.u32(field_id)?;
    w.var(term, MAX_TERM_LEN)?;
    w.finish()
}

pub fn decode_search_term_stats_key(src: &[u8], term_out: &mut [u8]) -> Option<(u32, u32, usize)> {
    let mut r = KeyReader::new(src);
    let index_id = r.u32()?;
    let field_id = r.u32()?;
    let t = r.var(term_out)?;
    r.end()?;
    Some((index_id, field_id, t))
}

/// One search index generation: which analyzer built it, and which
/// index id its postings live under (§14.20's versioning).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchGeneration {
    pub index_id: u32,
    pub generation: u32,
    pub analyzer_id: u32,
    pub analyzer_version: u32,
}

/// What an analyzer change implies (§14.20).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchTransition {
    /// Same analyzer, same version: no rebuild.
    NoChange,
    /// §14.20: “Changing an analyzer starts a resumable rebuild rather
    /// than mixing incompatible postings.” The new generation is built
    /// alongside, and the swap is a catalog change, not a merge.
    ResumableRebuild {
        from_generation: u32,
        to_generation: u32,
    },
}

/// Decide what a proposed analyzer means for a live index.
///
/// The only two outcomes are “nothing” and “resumable rebuild”. There
/// is deliberately no “reindex in place” — postings produced by two
/// analyzers are not comparable, so merging them produces a ranking
/// nobody can reason about and a recall nobody can measure. Fails
/// closed if the generation counter would not advance.
pub fn analyzer_change_plan(
    current: &SearchGeneration,
    proposed_analyzer_id: u32,
    proposed_analyzer_version: u32,
    next_generation: u32,
) -> Result<SearchTransition, ModelError> {
    if current.analyzer_id == proposed_analyzer_id
        && current.analyzer_version == proposed_analyzer_version
    {
        return Ok(SearchTransition::NoChange);
    }
    if next_generation <= current.generation {
        return Err(ModelError::Malformed);
    }
    Ok(SearchTransition::ResumableRebuild {
        from_generation: current.generation,
        to_generation: next_generation,
    })
}

/// §14.20's anti-mixing rule: a writer may only add postings to a
/// generation built by the same analyzer version it is running. A
/// mismatch means the writer is on the wrong side of a rebuild, and its
/// postings would be incomparable with the ones already there.
pub fn check_posting_write(
    generation: &SearchGeneration,
    writer_analyzer_id: u32,
    writer_analyzer_version: u32,
) -> Result<(), ModelError> {
    if generation.analyzer_id != writer_analyzer_id
        || generation.analyzer_version != writer_analyzer_version
    {
        return Err(ModelError::AnalyzerGenerationMismatch {
            generation_analyzer_version: generation.analyzer_version,
            writer_analyzer_version,
        });
    }
    Ok(())
}

/// §14.20: “Query results identify index generation and resolved source
/// timestamp. Ranking is deterministic for a declared
/// algorithm/version and supplies a stable tie-breaker.”
///
/// Every field is mandatory, which is the point: a search result
/// without a generation is a result nobody can reproduce, and a ranking
/// without an algorithm version is a number nobody can explain.
///
/// Wire layout ([`SEARCH_RESULT_LABEL_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [index_id:u32][generation:u32]
/// [analyzer_id:u32][analyzer_version:u32]
/// [source_resolved_timestamp:u64]
/// [ranking_algorithm:u32][ranking_version:u32]
/// [tie_breaker:u8]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchResultLabel {
    pub index_id: u32,
    pub generation: u32,
    pub analyzer_id: u32,
    pub analyzer_version: u32,
    /// The source frontier the answer reflects — a
    /// [`DerivedState::resolved_timestamp`], never “now”.
    pub source_resolved_timestamp: Timestamp,
    pub ranking_algorithm: u32,
    pub ranking_version: u32,
    pub tie_breaker: SearchTieBreaker,
}

/// The declared stable tie-breaker (§14.20). “Whatever order the scan
/// returned” is not one of the options: an unstable tie-break makes
/// pagination lose and duplicate documents.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchTieBreaker {
    /// Ascending document id.
    DocumentIdAscending = 1,
    /// Descending source timestamp, then ascending document id.
    NewestThenDocumentId = 2,
}

impl SearchTieBreaker {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::DocumentIdAscending),
            2 => Some(Self::NewestThenDocumentId),
            _ => None,
        }
    }
}

impl SearchResultLabel {
    /// Header (4) + 4 + 4 + 4 + 4 + 8 + 4 + 4 + 1.
    pub const WIRE_LEN: usize = 4 + 33;

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&SEARCH_RESULT_LABEL_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.index_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.generation.to_le_bytes());
        out[12..16].copy_from_slice(&self.analyzer_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.analyzer_version.to_le_bytes());
        out[20..28].copy_from_slice(&self.source_resolved_timestamp.to_le_bytes());
        out[28..32].copy_from_slice(&self.ranking_algorithm.to_le_bytes());
        out[32..36].copy_from_slice(&self.ranking_version.to_le_bytes());
        out[36] = self.tie_breaker as u8;
        Some(Self::WIRE_LEN)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != SEARCH_RESULT_LABEL_VERSION {
            return None;
        }
        if u16::from_le_bytes(src[2..4].try_into().ok()?) as usize != Self::WIRE_LEN - 4 {
            return None;
        }
        Some(Self {
            index_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            generation: u32::from_le_bytes(src[8..12].try_into().ok()?),
            analyzer_id: u32::from_le_bytes(src[12..16].try_into().ok()?),
            analyzer_version: u32::from_le_bytes(src[16..20].try_into().ok()?),
            source_resolved_timestamp: u64::from_le_bytes(src[20..28].try_into().ok()?),
            ranking_algorithm: u32::from_le_bytes(src[28..32].try_into().ok()?),
            ranking_version: u32::from_le_bytes(src[32..36].try_into().ok()?),
            tie_breaker: SearchTieBreaker::from_u8(src[36])?,
        })
    }

    /// §14.20's honesty check, the search analogue of
    /// [`label_is_honest`]: the label must name the generation and
    /// analyzer that actually answered, and may not claim a source
    /// frontier ahead of the one the index reached.
    pub fn is_honest(&self, actual: &SearchGeneration, actual_resolved: Timestamp) -> bool {
        self.index_id == actual.index_id
            && self.generation == actual.generation
            && self.analyzer_id == actual.analyzer_id
            && self.analyzer_version == actual.analyzer_version
            && self.source_resolved_timestamp <= actual_resolved
    }
}

// ══════════════════════════════════════════════════════════════════════
// 9. Vector capability (§14.21, §21 invariant 20)
// ══════════════════════════════════════════════════════════════════════

/// `vector/embedding/<index-id>/<entity-id>` worst case.
pub const VECTOR_EMBEDDING_KEY_MAX_LEN: usize = 4 + var_component_max_len(MAX_MODEL_ID_LEN);
/// `vector/index-manifest/<index-id>/<generation>`.
pub const VECTOR_MANIFEST_KEY_LEN: usize = 4 + 4;

/// `vector/embedding/<index-id>/<entity-id>` user key:
/// `[index_id:u32 BE][esc(entity_id)][0x00 0x01]`.
///
/// §14.21: the canonical embedding and its source association are
/// **authoritative Lattice records**. So this key is an ordinary
/// authoritative keyspace, and the approximate structure built over it
/// is not addressable here at all — “their private graph/list/
/// quantization representation is not part of the public Lattice record
/// contract”. Only the manifest ([`encode_vector_manifest_key`]) is.
pub fn encode_vector_embedding_key(
    out: &mut [u8],
    index_id: u32,
    entity_id: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.var(entity_id, MAX_MODEL_ID_LEN)?;
    w.finish()
}

pub fn decode_vector_embedding_key(src: &[u8], entity_out: &mut [u8]) -> Option<(u32, usize)> {
    let mut r = KeyReader::new(src);
    let index_id = r.u32()?;
    let e = r.var(entity_out)?;
    r.end()?;
    Some((index_id, e))
}

/// `vector/meta/<index-id>/<tag>/<entity>` user key: a metadata tag on an
/// entity, so a filtered nearest-neighbour query can restrict to entities
/// carrying a tag. `[index_id:u32 BE][esc(tag)][esc(entity)]`.
pub fn encode_vector_meta_key(
    out: &mut [u8],
    index_id: u32,
    tag: &[u8],
    entity_id: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.var(tag, MAX_MODEL_ID_LEN)?;
    w.var(entity_id, MAX_MODEL_ID_LEN)?;
    w.finish()
}

/// The `[index_id][tag]` scan prefix for all entities carrying `tag`.
pub fn encode_vector_meta_prefix(out: &mut [u8], index_id: u32, tag: &[u8]) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.var(tag, MAX_MODEL_ID_LEN)?;
    w.finish()
}

pub fn decode_vector_meta_key(src: &[u8], entity_out: &mut [u8]) -> Option<usize> {
    let mut r = KeyReader::new(src);
    let _index_id = r.u32()?;
    let mut tag = [0u8; MAX_MODEL_ID_LEN];
    let _t = r.var(&mut tag)?;
    let e = r.var(entity_out)?;
    r.end()?;
    Some(e)
}

/// `vector/ann/<index-id>/<lsh-bucket>/<entity>` user key:
/// `[index_id:u32 BE][bucket:u32 BE][entity]`. The bucket sits between
/// the index and the entity so `[index][bucket]` is a valid scan prefix
/// for one bucket (`VECTOR.ANN`) while `[index]` still covers them all.
pub fn encode_vector_ann_key(
    out: &mut [u8],
    index_id: u32,
    bucket: u32,
    entity_id: &[u8],
) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.u32(bucket)?;
    w.var(entity_id, MAX_MODEL_ID_LEN)?;
    w.finish()
}

pub fn decode_vector_ann_key(src: &[u8], entity_out: &mut [u8]) -> Option<(u32, u32, usize)> {
    let mut r = KeyReader::new(src);
    let index_id = r.u32()?;
    let bucket = r.u32()?;
    let e = r.var(entity_out)?;
    r.end()?;
    Some((index_id, bucket, e))
}

/// `vector/index-manifest/<index-id>/<generation>` user key:
/// `[index_id:u32 BE][generation:u32 BE]`. Ascending generation, so a
/// forward scan is build history and the last entry is current.
pub fn encode_vector_manifest_key(out: &mut [u8], index_id: u32, generation: u32) -> Option<usize> {
    let mut w = KeyWriter::new(out);
    w.u32(index_id)?;
    w.u32(generation)?;
    w.finish()
}

pub fn decode_vector_manifest_key(src: &[u8]) -> Option<(u32, u32)> {
    let mut r = KeyReader::new(src);
    let index_id = r.u32()?;
    let generation = r.u32()?;
    r.end()?;
    Some((index_id, generation))
}

/// Distance metric (§14.21). Part of the label because a cosine result
/// and an L2 result over the same vectors are different answers, and a
/// caller that assumed the wrong one gets silently wrong neighbours.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistanceMetric {
    L2 = 1,
    InnerProduct = 2,
    Cosine = 3,
}

impl DistanceMetric {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::L2),
            2 => Some(Self::InnerProduct),
            3 => Some(Self::Cosine),
            _ => None,
        }
    }
}

/// Exact versus approximate execution (§14.21).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorExecution {
    /// Every candidate was scored. Recall is 1 by construction.
    Exact = 1,
    /// An approximate structure selected candidates. Recall is a
    /// measured property, not a guarantee.
    Approximate = 2,
}

impl VectorExecution {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Exact),
            2 => Some(Self::Approximate),
            _ => None,
        }
    }
}

/// How candidates were rescored (§14.21).
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RerankingMode {
    None = 1,
    /// Candidates rescored against the canonical embeddings with the
    /// declared metric.
    ExactRerank = 2,
    /// Candidates rescored by the approximate provider itself, e.g.
    /// against a quantized representation.
    ProviderRerank = 3,
}

impl RerankingMode {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::None),
            2 => Some(Self::ExactRerank),
            3 => Some(Self::ProviderRerank),
            _ => None,
        }
    }
}

/// §14.21's degradation state.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DegradationState {
    /// The query ran as declared.
    None = 1,
    /// Candidate retrieval stopped at a resource bound; fewer
    /// candidates were scored than the plan asked for.
    PartialCandidates = 2,
    /// The approximate provider was unavailable and the exact bounded
    /// fallback ran instead (§14.21's “exact bounded fallback may be
    /// offered when its resource and latency limits permit it”).
    ExactFallback = 3,
    /// The index is behind its source and the answer reflects an older
    /// frontier.
    StaleIndex = 4,
    /// A rebuild is in progress and the serving generation is not the
    /// newest one.
    RebuildInProgress = 5,
}

impl DegradationState {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::None),
            2 => Some(Self::PartialCandidates),
            3 => Some(Self::ExactFallback),
            4 => Some(Self::StaleIndex),
            5 => Some(Self::RebuildInProgress),
            _ => None,
        }
    }
}

/// §14.21: “Every vector response declares metric, index generation,
/// source resolved timestamp, exact versus approximate execution,
/// reranking mode, and degradation state.”
///
/// Six mandatory fields, exactly as listed. This record is the response
/// contract; [`label_is_honest`] is §21 invariant 20.
///
/// Wire layout ([`VECTOR_RESPONSE_LABEL_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [index_id:u32][generation:u32]
/// [source_resolved_timestamp:u64]
/// [metric:u8][execution:u8][reranking:u8][degradation:u8]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorResponseLabel {
    pub index_id: u32,
    pub generation: u32,
    pub source_resolved_timestamp: Timestamp,
    pub metric: DistanceMetric,
    pub execution: VectorExecution,
    pub reranking: RerankingMode,
    pub degradation: DegradationState,
}

impl VectorResponseLabel {
    /// Header (4) + 4 + 4 + 8 + 1 + 1 + 1 + 1.
    pub const WIRE_LEN: usize = 4 + 20;

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&VECTOR_RESPONSE_LABEL_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.index_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.generation.to_le_bytes());
        out[12..20].copy_from_slice(&self.source_resolved_timestamp.to_le_bytes());
        out[20] = self.metric as u8;
        out[21] = self.execution as u8;
        out[22] = self.reranking as u8;
        out[23] = self.degradation as u8;
        Some(Self::WIRE_LEN)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        if u16::from_le_bytes(src[0..2].try_into().ok()?) != VECTOR_RESPONSE_LABEL_VERSION {
            return None;
        }
        if u16::from_le_bytes(src[2..4].try_into().ok()?) as usize != Self::WIRE_LEN - 4 {
            return None;
        }
        Some(Self {
            index_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            generation: u32::from_le_bytes(src[8..12].try_into().ok()?),
            source_resolved_timestamp: u64::from_le_bytes(src[12..20].try_into().ok()?),
            metric: DistanceMetric::from_u8(src[20])?,
            execution: VectorExecution::from_u8(src[21])?,
            reranking: RerankingMode::from_u8(src[22])?,
            degradation: DegradationState::from_u8(src[23])?,
        })
    }
}

/// What the vector engine actually did, as observed by the engine
/// itself. The label is a *claim*; this is the fact it is checked
/// against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorExecutionFacts {
    pub index_id: u32,
    /// The generation that answered.
    pub generation: u32,
    /// The frontier that generation has actually reached.
    pub index_resolved_timestamp: Timestamp,
    pub metric: DistanceMetric,
    pub execution: VectorExecution,
    pub reranking: RerankingMode,
    pub degradation: DegradationState,
}

/// §21 invariant 20 as a predicate, and §14.21's two named prohibitions
/// made mechanical:
///
/// > An approximate result cannot be labelled exact, and a lagging
/// > index cannot claim current visibility.
///
/// Six clauses, each naming one way a label can overstate. The
/// asymmetry in the freshness clause is deliberate: a label may claim
/// **less** currency than the index reached (a conservative label is
/// honest) but never more. Every other field must match exactly — there
/// is no conservative direction for “which metric did you use”.
///
/// [`why_dishonest`] returns the specific claim for a caller that must
/// report it.
pub fn label_is_honest(label: &VectorResponseLabel, actual: &VectorExecutionFacts) -> bool {
    why_dishonest(label, actual).is_none()
}

/// The first way `label` overstates `actual`, or `None` if it is
/// honest. Clause order is stable so a refusal is reproducible.
pub fn why_dishonest(
    label: &VectorResponseLabel,
    actual: &VectorExecutionFacts,
) -> Option<LabelClaim> {
    if label.execution == VectorExecution::Exact && actual.execution != VectorExecution::Exact {
        return Some(LabelClaim::ExactnessOverstated);
    }
    if label.index_id != actual.index_id || label.generation != actual.generation {
        return Some(LabelClaim::GenerationMisreported);
    }
    if label.source_resolved_timestamp > actual.index_resolved_timestamp {
        return Some(LabelClaim::FreshnessOverstated);
    }
    if label.metric != actual.metric {
        return Some(LabelClaim::MetricMisreported);
    }
    if label.reranking != actual.reranking {
        return Some(LabelClaim::RerankingMisreported);
    }
    if label.degradation == DegradationState::None && actual.degradation != DegradationState::None {
        return Some(LabelClaim::DegradationSuppressed);
    }
    None
}

/// [`label_is_honest`] in `Result` form, for a caller that must refuse
/// rather than inspect.
pub fn check_label_honest(
    label: &VectorResponseLabel,
    actual: &VectorExecutionFacts,
) -> Result<(), ModelError> {
    match why_dishonest(label, actual) {
        Some(claim) => Err(ModelError::DishonestLabel { claim }),
        None => Ok(()),
    }
}
