//! Database-operation contracts — RFC database foundation §14.4, §15,
//! §17.2, §18, §23.
//!
//! Phase 6 slice 1: the four record families and rule sets that every
//! Phase-6 worker (index maintainer, change-feed publisher, backup
//! coordinator, compaction coordinator) composes over. This file is
//! contracts only — no ports, no I/O, no clocks, no module. It stands
//! in the same relationship to the Phase-6 modules that `txn.rs` does
//! to the transaction coordinator and `range_lifecycle.rs` does to the
//! range supervisor.
//!
//! ## 1. Secondary index encoding (§14, §14.4)
//!
//! > Secondary indexes are ordinary ordered keyspaces with an encoding
//! > such as `index_key | primary_key -> stored index payload`.
//!
//! The whole point of putting the primary key *inside* the index key is
//! that a prefix scan of one index value yields that value's primary
//! keys, in primary-key order, with no secondary filter. That only
//! works if the index-value component is **prefix-unambiguous**: index
//! value `"a"` must not sweep up entries for `"ab"`. §9.1 already
//! solved that problem for user keys, so this file reuses the exact
//! same escape discipline ([`internal_key::KEY_ESCAPE`] /
//! [`internal_key::KEY_TERMINATOR`]) one level down, inside the user
//! key, and then hands the result to `internal_key` as an ordinary user
//! key. See [`index_prefix_for`] for the layout and the bleed proof.
//!
//! Unique indexes get [`UniqueOwnership`]: the "intent or ownership
//! record that detects conflicting primary keys" §14 asks for.
//! Derived indexes get [`IndexFreshness`], which makes §21 invariant 19
//! executable — an index cannot claim a freshness or exactness it has
//! not reached.
//!
//! ## 2. Change-feed cursors (§15)
//!
//! [`FeedCursor`] carries exactly §15's resume-token field list:
//! database, partition-map identity, range identity **and generation**,
//! timestamp/revision, and format version.
//!
//! Two rules matter and both are fail-closed:
//!
//! - A cursor below the retention floor is [`Resumability::Unrecoverable`],
//!   never `Ok`. The history it needs has been reclaimed; resuming at a
//!   later point would silently drop events, and a change-feed consumer
//!   has no way to detect that. Losing the feed loudly is strictly
//!   better than a gap it cannot see.
//! - A cursor whose range generation changed is
//!   [`Resumability::NeedsRefresh`], because §15's split/merge topology
//!   transition replaces **one cursor with a cursor set**.
//!   [`successor_cursors`] computes that replacement — the children of
//!   a split tile the parent's key span exactly, and the survivor of a
//!   merge covers the source's span — so the consumer crosses the
//!   transition "without gaps or duplicates beyond its declared
//!   delivery contract".
//!
//! ## 3. Backup manifest and state machine (§17.2)
//!
//! [`BackupManifest`] is §17.1's manifest shape adapted from one range
//! to a whole database, and [`BackupPhase`] is §17.2's numbered list
//! made into a lattice. The two rules the machine exists to enforce:
//!
//! - **Step 5**: a backup becomes `complete` **only** when every
//!   required range and digest is present. `complete` is never a field
//!   a caller sets; it is a consequence of reaching
//!   [`BackupPhase::AllRangesPresent`], and
//!   [`BackupManifest::decode`] re-checks it, so a manifest that claims
//!   completeness without its digests cannot even be read back off
//!   disk.
//! - **Step 6**: protection is released **only** after the final
//!   manifest is durable, or the operation is explicitly aborted. So a
//!   crash at any phase leaves either a resumable operation or an
//!   explicitly aborted one — see [`BackupManifest::recovery`] — and
//!   never a released protected timestamp with an incomplete backup.
//!
//! Restore ([`validate_restore`]) enforces §17.2's closing sentence:
//! every artifact is validated before publication, and a live
//! incarnation is never overwritten in place.
//!
//! ## 4. Retention claims (§18)
//!
//! [`effective_floor`] is the pure claim arithmetic §18 describes: the
//! effective floor is bounded by the **oldest** requirement across
//! every claim source, and "missing or stale claim sources block unsafe
//! advancement". That last clause is §21 invariant 13 and it is why
//! this function returns `Result`: an unreported source is not "no
//! claims", it is *unknown*, and unknown blocks.
//!
//! It takes `now_unix_ms` as a parameter and performs no I/O: the
//! compaction coordinator owns collection, proposal, and the durable
//! floor transition; this file owns only the arithmetic it applies.
//!
//! Golden vectors: `tests/contract_db_ops.rs`. Every record here is
//! persistent or replicated, so if a change makes a vector fail, that
//! change is a format break requiring the corresponding version bump
//! plus an explicit migration — never an update to the vector.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

#[path = "internal_key.rs"]
mod internal_key;
#[path = "range_lifecycle.rs"]
mod range_lifecycle;

#[allow(
    unused_imports,
    reason = "re-export surface: consumers of this contract file need the composed types without a second #[path] include"
)]
pub use internal_key::{DecodedKey, ValueKind, IDENTITY_PREFIX_LEN, KEY_FORMAT_VERSION};
#[allow(
    unused_imports,
    reason = "re-export surface: consumers of this contract file need the composed types without a second #[path] include"
)]
pub use range_lifecycle::{
    Binding, Lifecycle, LifecycleError, LifecycleKind, LifecycleOperation, MergePhase,
    OrderedRangeMap, Phase, RangeDescriptor, RelocatePhase, Replica, ReplicaRole, RoutingKind,
    SplitPhase, TargetPlacement, MAX_KEY_BOUND_LEN, MAX_RANGES, MAX_REPLICAS,
};

/// Logical MVCC timestamp (§10). A plain `u64` everywhere in the
/// contracts; named here because the §18 arithmetic reads better in
/// terms of it.
pub type Timestamp = u64;

// ══════════════════════════════════════════════════════════════════════
// 1. Secondary index encoding (§14, §14.4)
// ══════════════════════════════════════════════════════════════════════

/// Version of the index-key composite layout. Bump on ANY layout
/// change; an index built under one version cannot be scanned under
/// another, so a bump is a full index rebuild.
pub const INDEX_ENTRY_VERSION: u16 = 1;
/// Version of the [`UniqueOwnership`] wire encoding.
pub const UNIQUE_OWNERSHIP_VERSION: u16 = 1;
/// Version of the [`IndexFreshness`] wire encoding.
pub const INDEX_FRESHNESS_VERSION: u16 = 1;

/// Maximum encoded index-value length, bytes. This is the *pre-escape*
/// value produced by the relational encoder for one index tuple.
pub const MAX_INDEX_VALUE_LEN: usize = 256;
/// Maximum primary-key length, bytes. Matches
/// [`MAX_KEY_BOUND_LEN`] because a primary key must be able to be a
/// range bound: a key that cannot be a bound cannot be indexed.
pub const MAX_PRIMARY_KEY_LEN: usize = MAX_KEY_BOUND_LEN;

/// Width of the index identifier inside the composite key, bytes.
/// A fixed-width big-endian `u32`, so it is trivially order-preserving
/// and prefix-unambiguous — the same reasoning §9.1 applies to the
/// tenant/database/keyspace triple.
pub const INDEX_ID_LEN: usize = 4;

/// Worst-case length of the composite *user key* handed to
/// `internal_key`: index id, plus every index-value byte escaped to
/// two, plus a terminator, plus every primary-key byte escaped to two,
/// plus a terminator.
pub const INDEX_COMPOSITE_MAX_LEN: usize =
    INDEX_ID_LEN + 2 * MAX_INDEX_VALUE_LEN + 2 + 2 * MAX_PRIMARY_KEY_LEN + 2;

/// Worst-case encoded index-entry key length: `internal_key`'s
/// worst case for a composite user key of [`INDEX_COMPOSITE_MAX_LEN`]
/// bytes (the composite is escaped a second time by the outer
/// encoding — see [`index_prefix_for`]).
pub const INDEX_ENTRY_MAX_WIRE_LEN: usize = internal_key::encoded_max_len(INDEX_COMPOSITE_MAX_LEN);

/// Worst-case length of a scan prefix produced by
/// [`index_prefix_for`]: identity triple plus the doubly escaped
/// index id, index value, and component terminator.
pub const INDEX_PREFIX_MAX_LEN: usize =
    IDENTITY_PREFIX_LEN + 2 * (INDEX_ID_LEN + 2 * MAX_INDEX_VALUE_LEN + 2);

/// Escape sequence emitted for a literal `0x00` inside an index
/// component. Byte-identical to [`internal_key::KEY_ESCAPE`] on
/// purpose: the composite is a user key to the layer below, so the two
/// levels must agree that `0x00` is the only special byte.
const ESC: [u8; 2] = internal_key::KEY_ESCAPE;
/// Component terminator inside the composite. Byte-identical to
/// [`internal_key::KEY_TERMINATOR`]. Terminator (`0x01`) sorts below
/// escape (`0xFF`), which is what makes a proper prefix sort before its
/// extensions.
const TERM: [u8; 2] = internal_key::KEY_TERMINATOR;

/// Append `src` to `out` at offset `n`, escaping `0x00` as `0x00 0xFF`.
/// Returns the new offset, or `None` if `out` is too small — fail
/// closed, never a truncated key.
fn esc_into(out: &mut [u8], mut n: usize, src: &[u8]) -> Option<usize> {
    for &b in src {
        if b == 0x00 {
            if out.len() < n + 2 {
                return None;
            }
            out[n] = ESC[0];
            out[n + 1] = ESC[1];
            n += 2;
        } else {
            if out.len() < n + 1 {
                return None;
            }
            out[n] = b;
            n += 1;
        }
    }
    Some(n)
}

/// Append the component terminator at offset `n`.
fn term_into(out: &mut [u8], n: usize) -> Option<usize> {
    if out.len() < n + 2 {
        return None;
    }
    out[n] = TERM[0];
    out[n + 1] = TERM[1];
    Some(n + 2)
}

/// Unescape one terminator-delimited component of `src` starting at
/// `i`, writing the plain bytes into `out`. Returns
/// `(bytes_written, index just past the terminator)`.
///
/// Fails closed on a missing terminator, an unknown escape (`0x00`
/// followed by anything other than `0x01` or `0xFF`), or a component
/// larger than `out`.
fn unesc_component(src: &[u8], mut i: usize, out: &mut [u8]) -> Option<(usize, usize)> {
    let mut k = 0usize;
    loop {
        if i + 1 >= src.len() {
            return None; // ran past the end without a terminator
        }
        match (src[i], src[i + 1]) {
            (0x00, 0x01) => return Some((k, i + 2)),
            (0x00, 0xFF) => {
                if k >= out.len() {
                    return None;
                }
                out[k] = 0x00;
                k += 1;
                i += 2;
            }
            (0x00, _) => return None, // invalid escape
            (b, _) => {
                if k >= out.len() {
                    return None;
                }
                out[k] = b;
                k += 1;
                i += 1;
            }
        }
    }
}

/// Build the composite user key
/// `[index_id:u32 BE][esc(index_value)][TERM][esc(primary_key)][TERM]`
/// into `out`. Returns its length.
fn build_composite(
    out: &mut [u8],
    index_id: u32,
    index_value: &[u8],
    primary_key: &[u8],
) -> Option<usize> {
    if out.len() < INDEX_ID_LEN {
        return None;
    }
    out[0..INDEX_ID_LEN].copy_from_slice(&index_id.to_be_bytes());
    let mut n = INDEX_ID_LEN;
    n = esc_into(out, n, index_value)?;
    n = term_into(out, n)?;
    n = esc_into(out, n, primary_key)?;
    term_into(out, n)
}

/// One secondary-index entry, in the §14 form
/// `index_key | primary_key -> payload`.
///
/// The record itself is never serialized as a blob: it *is* a key, and
/// [`encode_index_entry`] produces the canonical internal key. The
/// payload is whatever the index stores (covering columns, or nothing);
/// it is opaque here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub tenant: u32,
    pub database: u32,
    /// The reserved `relational/index/...` keyspace (§14.4). Index
    /// entries live in their own keyspace, not the table's.
    pub keyspace: u32,
    /// Catalog-assigned index identity.
    pub index_id: u32,
    index_value: [u8; MAX_INDEX_VALUE_LEN],
    index_value_len: u16,
    primary_key: [u8; MAX_PRIMARY_KEY_LEN],
    primary_key_len: u16,
    /// MVCC timestamp of the index mutation. The primary row and its
    /// synchronous index entries share one transaction, so they share
    /// one commit timestamp (§14).
    pub mvcc_timestamp: Timestamp,
    pub value_kind: ValueKind,
}

impl IndexEntry {
    pub const EMPTY: IndexEntry = IndexEntry {
        tenant: 0,
        database: 0,
        keyspace: 0,
        index_id: 0,
        index_value: [0; MAX_INDEX_VALUE_LEN],
        index_value_len: 0,
        primary_key: [0; MAX_PRIMARY_KEY_LEN],
        primary_key_len: 0,
        mvcc_timestamp: 0,
        value_kind: ValueKind::Value,
    };

    /// Build an entry. Fails closed on an oversized component or an
    /// empty primary key — an index entry with no primary key names no
    /// row and could never be dereferenced.
    #[allow(
        clippy::too_many_arguments,
        reason = "contract constructor mirrors the §14 record shape one-to-one"
    )]
    pub fn new(
        tenant: u32,
        database: u32,
        keyspace: u32,
        index_id: u32,
        index_value: &[u8],
        primary_key: &[u8],
        mvcc_timestamp: Timestamp,
        value_kind: ValueKind,
    ) -> Option<Self> {
        if index_value.len() > MAX_INDEX_VALUE_LEN || primary_key.len() > MAX_PRIMARY_KEY_LEN {
            return None;
        }
        if primary_key.is_empty() {
            return None;
        }
        let mut e = Self::EMPTY;
        e.tenant = tenant;
        e.database = database;
        e.keyspace = keyspace;
        e.index_id = index_id;
        e.index_value[..index_value.len()].copy_from_slice(index_value);
        e.index_value_len = index_value.len() as u16;
        e.primary_key[..primary_key.len()].copy_from_slice(primary_key);
        e.primary_key_len = primary_key.len() as u16;
        e.mvcc_timestamp = mvcc_timestamp;
        e.value_kind = value_kind;
        Some(e)
    }

    pub fn index_value(&self) -> &[u8] {
        &self.index_value[..self.index_value_len as usize]
    }

    pub fn primary_key(&self) -> &[u8] {
        &self.primary_key[..self.primary_key_len as usize]
    }
}

/// Encode an index entry into its canonical internal key.
///
/// The composite user key is
///
/// ```text
/// [index_id:u32 BE][esc(index_value)][0x00 0x01][esc(primary_key)][0x00 0x01]
/// ```
///
/// and that composite is then handed to [`internal_key::encode`] as an
/// ordinary user key, producing
///
/// ```text
/// [tenant:u32 BE][database:u32 BE][keyspace:u32 BE]
/// [esc(composite)][0x00 0x01]
/// [!mvcc_timestamp:u64 BE][value_kind:u8]
/// ```
///
/// The double escape is deliberate, not an accident of layering. The
/// inner escape makes the *index value* prefix-unambiguous against
/// other index values; the outer escape makes the *whole composite*
/// prefix-unambiguous against other user keys in the keyspace. Both
/// are order-preserving byte-for-byte, so ascending byte order over
/// encoded entries equals
/// `(index_id, index_value, primary_key, newest-first version)` order.
///
/// Fails closed if `out` is smaller than the encoding needs.
pub fn encode_index_entry(entry: &IndexEntry, out: &mut [u8]) -> Option<usize> {
    let mut composite = [0u8; INDEX_COMPOSITE_MAX_LEN];
    let n = build_composite(
        &mut composite,
        entry.index_id,
        entry.index_value(),
        entry.primary_key(),
    )?;
    internal_key::encode(
        out,
        entry.tenant,
        entry.database,
        entry.keyspace,
        &composite[..n],
        entry.mvcc_timestamp,
        entry.value_kind,
    )
}

/// A decoded index entry. The variable components are unescaped into
/// caller-supplied buffers; the lengths written are reported here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedIndexEntry {
    pub tenant: u32,
    pub database: u32,
    pub keyspace: u32,
    pub index_id: u32,
    pub index_value_len: usize,
    pub primary_key_len: usize,
    pub mvcc_timestamp: Timestamp,
    pub value_kind: ValueKind,
}

/// Decode a complete encoded index-entry key, unescaping the index
/// value into `value_out` and the primary key into `key_out`.
///
/// Fails closed on anything malformed: a truncated internal key, an
/// unknown value kind, a composite shorter than the fixed index id,
/// a missing component terminator, an invalid escape, trailing bytes
/// after the primary-key terminator, or a component larger than its
/// output buffer.
pub fn decode_index_entry(
    src: &[u8],
    value_out: &mut [u8],
    key_out: &mut [u8],
) -> Option<DecodedIndexEntry> {
    let mut composite = [0u8; INDEX_COMPOSITE_MAX_LEN];
    let dk = internal_key::decode(src, &mut composite)?;
    let comp = composite.get(..dk.user_key_len)?;
    if comp.len() < INDEX_ID_LEN + TERM.len() * 2 {
        return None;
    }
    let index_id = u32::from_be_bytes(comp[0..INDEX_ID_LEN].try_into().ok()?);
    let (index_value_len, i) = unesc_component(comp, INDEX_ID_LEN, value_out)?;
    let (primary_key_len, i) = unesc_component(comp, i, key_out)?;
    if i != comp.len() {
        return None; // trailing bytes inside the composite
    }
    if primary_key_len == 0 {
        return None; // an entry naming no row is not constructible
    }
    Some(DecodedIndexEntry {
        tenant: dk.tenant,
        database: dk.database,
        keyspace: dk.keyspace,
        index_id,
        index_value_len,
        primary_key_len,
        mvcc_timestamp: dk.mvcc_timestamp,
        value_kind: dk.kind,
    })
}

/// The scan prefix whose range contains **exactly** the entries of one
/// index value, in primary-key order.
///
/// ```text
/// [tenant:u32 BE][database:u32 BE][keyspace:u32 BE]
/// [esc( [index_id:u32 BE][esc(index_value)][0x00 0x01] )]
/// ```
///
/// ### Why this cannot bleed
///
/// The prefix ends with the escaped form of the index-value
/// terminator, `0x00 0xFF 0x01`. Consider index values `"a"` and
/// `"ab"` under the same index id:
///
/// ```text
/// prefix("a")   = ...esc(id) 'a' 00 FF 01
/// entry("ab")   = ...esc(id) 'a' 'b' 00 FF 01 ...
/// ```
///
/// At the byte where the prefix requires `0x00`, the `"ab"` entry has
/// `'b'`, so the `"ab"` entry is not in the `"a"` prefix range. The
/// general statement: the escape guarantees `0x00` never occurs in
/// escaped payload except as the first byte of `0x00 0xFF`, and the
/// terminator `0x00 0x01` sorts strictly below it, so an index value
/// is a byte prefix of another value's encoding only when the two
/// values are equal. This is §9.1's prefix-ambiguity rule applied one
/// level down, and it is why the composite escapes its components
/// rather than length-prefixing them (a length prefix would sort short
/// values before long ones by *length*, breaking the ordered scan the
/// index exists to provide).
///
/// Fails closed on an oversized index value or an undersized `out`.
pub fn index_prefix_for(
    tenant: u32,
    database: u32,
    keyspace: u32,
    index_id: u32,
    index_value: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    if index_value.len() > MAX_INDEX_VALUE_LEN {
        return None;
    }
    // Inner composite prefix: id + escaped value + terminator.
    let mut inner = [0u8; INDEX_ID_LEN + 2 * MAX_INDEX_VALUE_LEN + 2];
    inner
        .get_mut(0..INDEX_ID_LEN)?
        .copy_from_slice(&index_id.to_be_bytes());
    let mut m = INDEX_ID_LEN;
    m = esc_into(&mut inner, m, index_value)?;
    m = term_into(&mut inner, m)?;
    // Outer: identity triple + escaped composite prefix. No outer
    // terminator — this is a prefix, not a complete key.
    if out.len() < IDENTITY_PREFIX_LEN {
        return None;
    }
    out[0..4].copy_from_slice(&tenant.to_be_bytes());
    out[4..8].copy_from_slice(&database.to_be_bytes());
    out[8..12].copy_from_slice(&keyspace.to_be_bytes());
    esc_into(out, IDENTITY_PREFIX_LEN, &inner[..m])
}

/// The scan prefix covering an entire index (every value, every
/// primary key), in `(index_value, primary_key)` order. Same layout as
/// [`index_prefix_for`] minus the value and its terminator; used by
/// backfill and drop.
pub fn index_id_prefix_for(
    tenant: u32,
    database: u32,
    keyspace: u32,
    index_id: u32,
    out: &mut [u8],
) -> Option<usize> {
    if out.len() < IDENTITY_PREFIX_LEN {
        return None;
    }
    out[0..4].copy_from_slice(&tenant.to_be_bytes());
    out[4..8].copy_from_slice(&database.to_be_bytes());
    out[8..12].copy_from_slice(&keyspace.to_be_bytes());
    esc_into(out, IDENTITY_PREFIX_LEN, &index_id.to_be_bytes())
}

// ── USER-key composite forms (executor-side) ─────────────────────────
//
// The relational executor speaks USER keys — `[keyspace:u32 BE][body]`
// through the KV API; the engine's `internal_key` wrapping (identity
// triple, MVCC suffix) happens inside the state store. The three
// helpers above produce ENGINE-level forms. These two produce the
// user-key BODY for the same composite, with the identical escape
// discipline, so `(index_id, index_value, primary_key)` order over
// bodies equals the engine's order over encoded keys.

/// User-key body of one index entry:
/// `[index_id:u32 BE][esc(index_value)][TERM][esc(primary_key)][TERM]`.
pub fn index_user_key(
    index_id: u32,
    index_value: &[u8],
    primary_key: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    if index_value.len() > MAX_INDEX_VALUE_LEN || primary_key.len() > MAX_PRIMARY_KEY_LEN {
        return None;
    }
    out.get_mut(0..INDEX_ID_LEN)?
        .copy_from_slice(&index_id.to_be_bytes());
    let mut n = INDEX_ID_LEN;
    n = esc_into(out, n, index_value)?;
    n = term_into(out, n)?;
    n = esc_into(out, n, primary_key)?;
    term_into(out, n)
}

/// User-key scan prefix for one `(index_id, index_value)` pair —
/// exactly the entries whose [`index_user_key`] starts with it, no
/// value bleed (the terminator closes the value component).
pub fn index_user_prefix(index_id: u32, index_value: &[u8], out: &mut [u8]) -> Option<usize> {
    if index_value.len() > MAX_INDEX_VALUE_LEN {
        return None;
    }
    out.get_mut(0..INDEX_ID_LEN)?
        .copy_from_slice(&index_id.to_be_bytes());
    let mut n = INDEX_ID_LEN;
    n = esc_into(out, n, index_value)?;
    term_into(out, n)
}

/// Split an [`index_user_key`] body back into
/// `(index_id, primary_key_bytes)` — the read side of an index scan,
/// which needs the PRIMARY KEY to fetch the row. The index value is
/// implied by the scan prefix and not re-derived. Fails closed on any
/// malformed escape.
pub fn index_user_key_primary(body: &[u8], pk_out: &mut [u8]) -> Option<(u32, usize)> {
    if body.len() < INDEX_ID_LEN {
        return None;
    }
    let index_id = u32::from_be_bytes(body.get(0..4)?.try_into().ok()?);
    // Walk past the escaped value component to its terminator.
    let mut at = INDEX_ID_LEN;
    let mut component = 0usize;
    let mut pk_len = 0usize;
    while at < body.len() {
        if body[at] == 0x00 {
            let next = *body.get(at + 1)?;
            match next {
                // KEY_TERMINATOR
                0x01 => {
                    component += 1;
                    at += 2;
                    if component == 2 {
                        return if at == body.len() {
                            Some((index_id, pk_len))
                        } else {
                            None // trailing bytes
                        };
                    }
                    continue;
                }
                // KEY_ESCAPE: a literal 0x00
                0xFF => {
                    if component == 1 {
                        *pk_out.get_mut(pk_len)? = 0x00;
                        pk_len += 1;
                    }
                    at += 2;
                    continue;
                }
                _ => return None,
            }
        }
        if component == 1 {
            *pk_out.get_mut(pk_len)? = body[at];
            pk_len += 1;
        }
        at += 1;
    }
    None // ran out before the closing terminator
}

// ── Unique-index ownership (§14) ──────────────────────────────────────

/// The §14 "intent or ownership record that detects conflicting primary
/// keys" for a unique index.
///
/// One record per `(index_id, index_value)`. The primary key it names
/// is the value's owner; any other primary key attempting to claim the
/// same value conflicts, and the transaction must abort rather than
/// produce two rows with the same unique key.
///
/// Wire layout ([`UNIQUE_OWNERSHIP_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [index_id:u32]
/// [txn_epoch:u64]
/// [index_value_len:u16][index_value...]
/// [primary_key_len:u16][primary_key...]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UniqueOwnership {
    pub index_id: u32,
    index_value: [u8; MAX_INDEX_VALUE_LEN],
    index_value_len: u16,
    owning_primary_key: [u8; MAX_PRIMARY_KEY_LEN],
    owning_primary_key_len: u16,
    /// The transaction epoch that installed this ownership. A claim
    /// presented at an older epoch is a delayed message from a
    /// superseded attempt and cannot mutate current state (§21
    /// invariant 6).
    pub txn_epoch: u64,
}

/// Fixed wire overhead of a [`UniqueOwnership`].
pub const UNIQUE_OWNERSHIP_FIXED_WIRE_LEN: usize = 4 + 4 + 8 + 2 + 2;
/// Worst-case encoded [`UniqueOwnership`] length.
pub const UNIQUE_OWNERSHIP_MAX_WIRE_LEN: usize =
    UNIQUE_OWNERSHIP_FIXED_WIRE_LEN + MAX_INDEX_VALUE_LEN + MAX_PRIMARY_KEY_LEN;

impl UniqueOwnership {
    pub const EMPTY: UniqueOwnership = UniqueOwnership {
        index_id: 0,
        index_value: [0; MAX_INDEX_VALUE_LEN],
        index_value_len: 0,
        owning_primary_key: [0; MAX_PRIMARY_KEY_LEN],
        owning_primary_key_len: 0,
        txn_epoch: 0,
    };

    /// Build an ownership record. Fails closed on oversized components
    /// or an empty owning primary key.
    pub fn new(
        index_id: u32,
        index_value: &[u8],
        owning_primary_key: &[u8],
        txn_epoch: u64,
    ) -> Option<Self> {
        if index_value.len() > MAX_INDEX_VALUE_LEN
            || owning_primary_key.len() > MAX_PRIMARY_KEY_LEN
            || owning_primary_key.is_empty()
        {
            return None;
        }
        let mut r = Self::EMPTY;
        r.index_id = index_id;
        r.index_value[..index_value.len()].copy_from_slice(index_value);
        r.index_value_len = index_value.len() as u16;
        r.owning_primary_key[..owning_primary_key.len()].copy_from_slice(owning_primary_key);
        r.owning_primary_key_len = owning_primary_key.len() as u16;
        r.txn_epoch = txn_epoch;
        Some(r)
    }

    pub fn index_value(&self) -> &[u8] {
        &self.index_value[..self.index_value_len as usize]
    }

    pub fn owning_primary_key(&self) -> &[u8] {
        &self.owning_primary_key[..self.owning_primary_key_len as usize]
    }

    pub fn wire_len(&self) -> usize {
        UNIQUE_OWNERSHIP_FIXED_WIRE_LEN
            + self.index_value_len as usize
            + self.owning_primary_key_len as usize
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&UNIQUE_OWNERSHIP_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.index_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.txn_epoch.to_le_bytes());
        let mut n = 16;
        out[n..n + 2].copy_from_slice(&self.index_value_len.to_le_bytes());
        n += 2;
        out[n..n + self.index_value_len as usize].copy_from_slice(self.index_value());
        n += self.index_value_len as usize;
        out[n..n + 2].copy_from_slice(&self.owning_primary_key_len.to_le_bytes());
        n += 2;
        out[n..n + self.owning_primary_key_len as usize].copy_from_slice(self.owning_primary_key());
        n += self.owning_primary_key_len as usize;
        debug_assert_eq!(n, total);
        Some(n)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// declared-length mismatch, truncation, trailing bytes, oversized
    /// components, or an empty owning primary key.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < 4 {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != UNIQUE_OWNERSHIP_VERSION {
            return None;
        }
        let payload_len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if src.len() != 4 + payload_len {
            return None;
        }
        let p = &src[4..];
        if p.len() < 12 {
            return None;
        }
        let index_id = u32::from_le_bytes(p[0..4].try_into().ok()?);
        let txn_epoch = u64::from_le_bytes(p[4..12].try_into().ok()?);
        let mut n = 12;
        let vlen = u16::from_le_bytes(p.get(n..n + 2)?.try_into().ok()?) as usize;
        n += 2;
        if vlen > MAX_INDEX_VALUE_LEN {
            return None;
        }
        let value = p.get(n..n + vlen)?;
        n += vlen;
        let klen = u16::from_le_bytes(p.get(n..n + 2)?.try_into().ok()?) as usize;
        n += 2;
        if klen == 0 || klen > MAX_PRIMARY_KEY_LEN {
            return None;
        }
        let key = p.get(n..n + klen)?;
        n += klen;
        if n != payload_len {
            return None;
        }
        let mut r = Self::EMPTY;
        r.index_id = index_id;
        r.txn_epoch = txn_epoch;
        r.index_value[..vlen].copy_from_slice(value);
        r.index_value_len = vlen as u16;
        r.owning_primary_key[..klen].copy_from_slice(key);
        r.owning_primary_key_len = klen as u16;
        Some(r)
    }

    /// Do two records describe the same unique-index slot?
    pub fn same_slot(&self, other: &UniqueOwnership) -> bool {
        self.index_id == other.index_id && self.index_value() == other.index_value()
    }
}

/// Outcome of presenting a candidate ownership against the record
/// currently stored for a unique-index slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UniqueOutcome {
    /// No record exists for this slot: the candidate may take it.
    Vacant,
    /// The stored record already names this primary key. Re-asserting
    /// ownership is idempotent — §21 invariant 17's "every remote
    /// mutation is idempotently identifiable" applies to index
    /// maintenance too.
    OwnedBySelf,
    /// A different primary key owns the value. The candidate
    /// transaction must abort; unique means unique, and ownership does
    /// not transfer by being newer. It transfers only when the current
    /// owner's entry is removed and the slot becomes `Vacant`.
    Conflict,
    /// The candidate presents an epoch older than the stored record: a
    /// delayed message from a superseded attempt. Refused rather than
    /// applied (§21 invariant 6).
    StaleEpoch { current: u64, presented: u64 },
    /// The two records describe different `(index_id, index_value)`
    /// slots. The caller compared the wrong pair; there is no safe
    /// answer, so this is a refusal, not a `Vacant`.
    SlotMismatch,
}

/// The §14 unique-index conflict check.
///
/// Ordering of the clauses is the contract: slot identity first (a
/// mismatched comparison is a caller bug and must not read as
/// "available"), then the epoch fence, then ownership. In particular a
/// candidate with a *newer* epoch and a *different* primary key is
/// still a [`UniqueOutcome::Conflict`] — being newer never wins a
/// uniqueness argument.
pub fn check_unique_ownership(
    existing: Option<&UniqueOwnership>,
    candidate: &UniqueOwnership,
) -> UniqueOutcome {
    let Some(existing) = existing else {
        return UniqueOutcome::Vacant;
    };
    if !existing.same_slot(candidate) {
        return UniqueOutcome::SlotMismatch;
    }
    if candidate.txn_epoch < existing.txn_epoch {
        return UniqueOutcome::StaleEpoch {
            current: existing.txn_epoch,
            presented: candidate.txn_epoch,
        };
    }
    if existing.owning_primary_key() == candidate.owning_primary_key() {
        UniqueOutcome::OwnedBySelf
    } else {
        UniqueOutcome::Conflict
    }
}

// ── Index freshness (§14, §21 invariant 19) ───────────────────────────

/// How exact an index's contents are relative to its source.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexExactness {
    /// Maintained inside the writing transaction: exact at every
    /// committed timestamp of the source (§14).
    Synchronous = 1,
    /// Maintained asynchronously: exact only up to
    /// [`IndexFreshness::source_timestamp`], and it must advertise its
    /// lag (§14).
    Derived = 2,
}

impl IndexExactness {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Synchronous),
            2 => Some(Self::Derived),
            _ => None,
        }
    }
}

/// §21 invariant 19 as a record: "every derived index reports its
/// source revision/timestamp and generation; it cannot silently claim
/// freshness or exactness it has not achieved."
///
/// Wire layout ([`INDEX_FRESHNESS_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [index_id:u32]
/// [source_revision:u64][source_timestamp:u64]
/// [range_generation:u32][exactness:u8][lag_ms:u64]
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexFreshness {
    pub index_id: u32,
    /// Apply-order revision of the source the index has consumed
    /// through.
    pub source_revision: u64,
    /// MVCC timestamp the index is exact as of.
    pub source_timestamp: Timestamp,
    /// Descriptor generation the index was built against (§11.3): an
    /// index built before a topology change cannot answer for the
    /// span after it.
    pub range_generation: u32,
    pub exactness: IndexExactness,
    /// Advertised lag, milliseconds. Observability only — decisions use
    /// `source_timestamp` (§21 invariant 3 forbids local time from
    /// affecting logical outcome).
    pub lag_ms: u64,
}

impl IndexFreshness {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + 33;

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&INDEX_FRESHNESS_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.index_id.to_le_bytes());
        out[8..16].copy_from_slice(&self.source_revision.to_le_bytes());
        out[16..24].copy_from_slice(&self.source_timestamp.to_le_bytes());
        out[24..28].copy_from_slice(&self.range_generation.to_le_bytes());
        out[28] = self.exactness as u8;
        out[29..37].copy_from_slice(&self.lag_ms.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// length mismatch, or an unknown exactness discriminant.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != INDEX_FRESHNESS_VERSION {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != Self::WIRE_LEN - 4 {
            return None;
        }
        Some(Self {
            index_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            source_revision: u64::from_le_bytes(src[8..16].try_into().ok()?),
            source_timestamp: u64::from_le_bytes(src[16..24].try_into().ok()?),
            range_generation: u32::from_le_bytes(src[24..28].try_into().ok()?),
            exactness: IndexExactness::from_u8(src[28])?,
            lag_ms: u64::from_le_bytes(src[29..37].try_into().ok()?),
        })
    }
}

/// Ways an index can fail to satisfy a freshness contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FreshnessError {
    /// The index was built against a different descriptor generation.
    GenerationMismatch { current: u32, index: u32 },
    /// The index has not consumed the source through the required
    /// timestamp. §14: "asynchronous derived indexes ... cannot satisfy
    /// a freshness contract they have not reached."
    BehindRequiredTimestamp {
        required: Timestamp,
        reached: Timestamp,
    },
    /// A synchronous index advertising nonzero lag is self-
    /// contradictory: synchronous maintenance happens inside the
    /// writing transaction, so there is nothing to lag behind. Refused
    /// rather than believed (§21 invariant 19).
    SynchronousClaimsLag { lag_ms: u64 },
}

/// Can this index answer a read at `required_timestamp` under
/// `required_generation`? Fail closed — an index that cannot prove its
/// freshness does not get to serve the read.
pub fn index_satisfies_freshness(
    f: &IndexFreshness,
    required_timestamp: Timestamp,
    required_generation: u32,
) -> Result<(), FreshnessError> {
    if f.exactness == IndexExactness::Synchronous && f.lag_ms != 0 {
        return Err(FreshnessError::SynchronousClaimsLag { lag_ms: f.lag_ms });
    }
    if f.range_generation != required_generation {
        return Err(FreshnessError::GenerationMismatch {
            current: required_generation,
            index: f.range_generation,
        });
    }
    if f.source_timestamp < required_timestamp {
        return Err(FreshnessError::BehindRequiredTimestamp {
            required: required_timestamp,
            reached: f.source_timestamp,
        });
    }
    Ok(())
}

// ══════════════════════════════════════════════════════════════════════
// 2. Change-feed cursors (§15)
// ══════════════════════════════════════════════════════════════════════

/// Version of the [`FeedCursor`] resume-token wire encoding. Bump on
/// ANY layout change; decoders fail closed on an unknown version.
pub const FEED_CURSOR_VERSION: u16 = 1;

/// Version of the change-feed **event** stream format the cursor was
/// issued against. Carried inside the token (§15's "format version")
/// and separate from [`FEED_CURSOR_VERSION`]: the token layout and the
/// event layout evolve independently, and a consumer holding a token
/// for an event format this build no longer emits cannot be resumed.
pub const FEED_EVENT_FORMAT_VERSION: u16 = 1;

/// Maximum successors one topology transition can produce. Two,
/// because a split produces two children and no §12 operation produces
/// more. This is a format bound; widening it (an N-way split) is a
/// version bump.
pub const MAX_SUCCESSOR_CURSORS: usize = 2;

/// A change-feed resume token (§15).
///
/// > A resume token includes database, partition-map identity, range
/// > identity and generation, timestamp/revision, and format version.
///
/// Wire layout ([`FEED_CURSOR_VERSION`] 1, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [database_id:u32][partition_map_id:u32]
/// [range_id:16][range_generation:u32]
/// [revision:u64][timestamp:u64]
/// [format_version:u16]
/// ```
///
/// `revision` is the range's apply-order position and `timestamp` is
/// the MVCC timestamp of the last delivered event: §15 orders per-range
/// events "by MVCC timestamp and range apply order", so a resume point
/// needs both. Retention decisions use `timestamp`, because the MVCC
/// GC floor is a timestamp (§18).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeedCursor {
    pub database_id: u32,
    pub partition_map_id: u32,
    pub range_id: [u8; 16],
    pub range_generation: u32,
    pub revision: u64,
    pub timestamp: Timestamp,
    pub format_version: u16,
}

impl FeedCursor {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + 46;

    pub const EMPTY: FeedCursor = FeedCursor {
        database_id: 0,
        partition_map_id: 0,
        range_id: [0; 16],
        range_generation: 0,
        revision: 0,
        timestamp: 0,
        format_version: FEED_EVENT_FORMAT_VERSION,
    };

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&FEED_CURSOR_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..8].copy_from_slice(&self.database_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.partition_map_id.to_le_bytes());
        out[12..28].copy_from_slice(&self.range_id);
        out[28..32].copy_from_slice(&self.range_generation.to_le_bytes());
        out[32..40].copy_from_slice(&self.revision.to_le_bytes());
        out[40..48].copy_from_slice(&self.timestamp.to_le_bytes());
        out[48..50].copy_from_slice(&self.format_version.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown token
    /// version, length mismatch, or an all-zero range identity (no
    /// range has the null identity, so a zeroed buffer must not decode
    /// as a usable cursor).
    ///
    /// The embedded `format_version` is deliberately NOT validated
    /// here — it is data the token carries, and judging it is
    /// [`cursor_is_resumable`]'s job, which can report it as an
    /// unrecoverable resume rather than an unreadable token.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != FEED_CURSOR_VERSION {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != Self::WIRE_LEN - 4 {
            return None;
        }
        let range_id: [u8; 16] = src[12..28].try_into().ok()?;
        if range_id == [0u8; 16] {
            return None;
        }
        Some(Self {
            database_id: u32::from_le_bytes(src[4..8].try_into().ok()?),
            partition_map_id: u32::from_le_bytes(src[8..12].try_into().ok()?),
            range_id,
            range_generation: u32::from_le_bytes(src[28..32].try_into().ok()?),
            revision: u64::from_le_bytes(src[32..40].try_into().ok()?),
            timestamp: u64::from_le_bytes(src[40..48].try_into().ok()?),
            format_version: u16::from_le_bytes(src[48..50].try_into().ok()?),
        })
    }
}

/// Why a cursor needs new metadata before it can resume. Every variant
/// is recoverable: the consumer refetches the descriptor set (and, for
/// a topology transition, calls [`successor_cursors`]) and continues
/// from the same timestamp/revision, so no event is skipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshReason {
    /// §15's topology transition: the range generation moved, so this
    /// one cursor is replaced by a cursor SET.
    RangeGenerationChanged { current: u32, cursor: u32 },
    /// The descriptor exists but is no longer serving — the range was
    /// tombstoned by a merge or a completed split. Same remedy: fetch
    /// the successor set.
    RangeRetired { lifecycle: Lifecycle },
}

/// Why a cursor can never resume. Every variant means the consumer's
/// stream is broken and must be restarted from a fresh snapshot; none
/// of them may be silently downgraded to a later resume point, because
/// that is a gap the consumer cannot detect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedFailure {
    /// §18: the history this cursor needs has been reclaimed. The
    /// single most important fail-closed rule in this section.
    BelowRetentionFloor { floor: Timestamp, cursor: Timestamp },
    /// The token was issued against an event format this build no
    /// longer emits.
    UnknownFormatVersion { version: u16 },
    /// The cursor names a different database.
    DatabaseMismatch { current: u32, cursor: u32 },
    /// The cursor names a different partition map.
    PartitionMapMismatch { current: u32, cursor: u32 },
    /// The descriptor supplied is not the cursor's range at all.
    RangeMismatch,
}

/// The answer [`cursor_is_resumable`] gives. Deliberately three-valued:
/// collapsing `NeedsRefresh` into `Unrecoverable` would break feeds
/// on every split, and collapsing `Unrecoverable` into `Ok` would hide
/// data loss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resumability {
    /// Resume from `cursor.revision` / `cursor.timestamp` as-is.
    Ok,
    /// Refetch metadata (and successors) first; no events are lost.
    NeedsRefresh(RefreshReason),
    /// The stream is broken. Restart from a fresh snapshot.
    Unrecoverable(FeedFailure),
}

/// Classify a resume attempt (§15, §18).
///
/// `retention_floor` is the MVCC GC safe timestamp for the cursor's
/// range: history strictly below it may already have been reclaimed.
/// A cursor exactly *at* the floor is resumable — the floor is the
/// oldest **retained** timestamp, not the oldest reclaimed one.
///
/// ### Check order is part of the contract
///
/// 1. identity (database, partition map, range) — a cursor compared
///    against the wrong descriptor gets a refusal, never an answer;
/// 2. event format version;
/// 3. **retention floor**;
/// 4. range generation / lifecycle.
///
/// The floor is checked *before* the generation on purpose. A cursor
/// that is both below the floor and generation-stale is
/// `Unrecoverable`, not `NeedsRefresh`: refreshing metadata would
/// produce successors positioned at a timestamp whose history is gone,
/// which is precisely the undetectable gap this ordering exists to
/// prevent.
pub fn cursor_is_resumable(
    cursor: &FeedCursor,
    current: &RangeDescriptor,
    retention_floor: Timestamp,
) -> Resumability {
    if cursor.database_id != current.database_id {
        return Resumability::Unrecoverable(FeedFailure::DatabaseMismatch {
            current: current.database_id,
            cursor: cursor.database_id,
        });
    }
    if cursor.partition_map_id != current.partition_map_id {
        return Resumability::Unrecoverable(FeedFailure::PartitionMapMismatch {
            current: current.partition_map_id,
            cursor: cursor.partition_map_id,
        });
    }
    if cursor.range_id != current.range_id {
        return Resumability::Unrecoverable(FeedFailure::RangeMismatch);
    }
    if cursor.format_version != FEED_EVENT_FORMAT_VERSION {
        return Resumability::Unrecoverable(FeedFailure::UnknownFormatVersion {
            version: cursor.format_version,
        });
    }
    if cursor.timestamp < retention_floor {
        return Resumability::Unrecoverable(FeedFailure::BelowRetentionFloor {
            floor: retention_floor,
            cursor: cursor.timestamp,
        });
    }
    if cursor.range_generation != current.generation {
        return Resumability::NeedsRefresh(RefreshReason::RangeGenerationChanged {
            current: current.generation,
            cursor: cursor.range_generation,
        });
    }
    if current.lifecycle != Lifecycle::Active {
        return Resumability::NeedsRefresh(RefreshReason::RangeRetired {
            lifecycle: current.lifecycle,
        });
    }
    Resumability::Ok
}

// ── Topology transitions: one cursor becomes a cursor set ─────────────

/// One replacement cursor plus the descriptor it now follows. The
/// descriptor is returned, not just the identity, because the key span
/// is what proves the replacement set is gapless — and because the
/// consumer needs the new generation and binding anyway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SuccessorCursor {
    pub cursor: FeedCursor,
    pub descriptor: RangeDescriptor,
}

impl SuccessorCursor {
    pub const EMPTY: SuccessorCursor = SuccessorCursor {
        cursor: FeedCursor::EMPTY,
        descriptor: RangeDescriptor::EMPTY,
    };
}

/// The bounded set of cursors that replaces one cursor across a
/// topology transition (§15).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorSet {
    count: usize,
    items: [SuccessorCursor; MAX_SUCCESSOR_CURSORS],
}

impl CursorSet {
    pub const EMPTY: CursorSet = CursorSet {
        count: 0,
        items: [SuccessorCursor::EMPTY; MAX_SUCCESSOR_CURSORS],
    };

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn items(&self) -> &[SuccessorCursor] {
        &self.items[..self.count]
    }

    fn push(&mut self, item: SuccessorCursor) -> Option<()> {
        if self.count >= MAX_SUCCESSOR_CURSORS {
            return None;
        }
        self.items[self.count] = item;
        self.count += 1;
        Some(())
    }

    /// Do the successors tile a single contiguous span with no gap and
    /// no overlap, and does that span contain `[start, end)`?
    ///
    /// This is the executable form of §15's "without gaps or
    /// duplicates": a gap would mean events for some key are delivered
    /// to no successor, and an overlap would mean they are delivered to
    /// two. [`successor_cursors`] refuses to return a set that fails
    /// this, so a caller cannot receive a lossy replacement.
    pub fn covers(&self, start: &[u8], end: &[u8]) -> bool {
        if self.count == 0 {
            return false;
        }
        let items = self.items();
        // Contiguity: each successor starts exactly where the previous
        // one ended. Bounds are the half-open USER-key bounds of §11.
        for i in 1..items.len() {
            let prev = &items[i - 1].descriptor;
            let cur = &items[i].descriptor;
            if prev.end_key().is_empty() || prev.end_key() != cur.start_key() {
                return false;
            }
        }
        let first = &items[0].descriptor;
        let last = &items[items.len() - 1].descriptor;
        // Containment of [start, end) in [first.start, last.end).
        if first.start_key() > start {
            return false;
        }
        if last.end_key().is_empty() {
            return true; // covers to MAX
        }
        if end.is_empty() {
            return false; // needs MAX, successor stops short
        }
        last.end_key() >= end
    }
}

/// Ways a topology-crossing request can be refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedError {
    /// The cursor's range is not in the supplied pre-operation map.
    CursorRangeNotInMap,
    /// The operation does not name the cursor's range as a source (nor,
    /// for a merge, as the surviving target). The cursor is unaffected
    /// and must not be replaced — replacing an unaffected cursor would
    /// duplicate its stream.
    CursorNotAffected,
    /// The merge target named by the operation is not in the map.
    MergeTargetNotInMap,
    /// The operation is not a legal §12 operation over this map. The
    /// wrapped error comes from `range_lifecycle`'s locked validators.
    Lifecycle(LifecycleError),
    /// The successor set could not be constructed within
    /// [`MAX_SUCCESSOR_CURSORS`].
    SuccessorCapacity,
    /// The computed successors do not cover the cursor's span with no
    /// gap and no overlap. Refused rather than returned (§15).
    SuccessorSpanIncomplete,
    /// A successor descriptor could not be constructed (an oversized
    /// bound or a bad replica set inherited from the source).
    SuccessorMalformed,
}

/// The cursor set that replaces `cursor` when `op` publishes (§15).
///
/// - **Split**: the one cursor becomes **two**, one per child, each
///   positioned at the parent's `revision`/`timestamp` and carrying the
///   operation's `next_generation`. The children tile `[parent.start,
///   parent.end)` exactly — `[start, split_key)` and `[split_key,
///   end)` — so every key the consumer was following continues to be
///   followed by exactly one successor.
/// - **Merge**: the cursor — whether it followed the source or the
///   surviving target — becomes **one** cursor on the target, whose
///   span is the union of the two adjacent spans. This is the inverse
///   of the split case: two cursors collapse to one, and the consumer
///   deduplicates the overlap according to its declared delivery
///   contract.
/// - **Relocate**: §12.3 changes replicas, not bounds, so the cursor is
///   replaced by **one** cursor on the same range at the new generation
///   and binding. The span is unchanged.
///
/// The replacement cursors deliberately keep the parent's `revision`
/// and `timestamp`. That is what makes the crossing gapless: the
/// consumer resumes each successor at exactly the point it stopped, and
/// re-delivery of events at that boundary is the "duplicates within its
/// declared delivery contract" §15 permits.
///
/// `map` is the **pre-operation** published map. It is read, never
/// copied — building the post-operation map is `range_lifecycle`'s job
/// and would cost a full `OrderedRangeMap` frame.
pub fn successor_cursors(
    cursor: &FeedCursor,
    op: &LifecycleOperation,
    map: &OrderedRangeMap,
) -> Result<CursorSet, FeedError> {
    map.validate()
        .map_err(|violation| FeedError::Lifecycle(LifecycleError::InputMapInvalid { violation }))?;
    let source =
        descriptor_by_id(map, op.source_range_id()).ok_or(FeedError::CursorRangeNotInMap)?;

    let mut set = CursorSet::EMPTY;
    match op.kind() {
        LifecycleKind::Split => {
            if &cursor.range_id != op.source_range_id() {
                return Err(FeedError::CursorNotAffected);
            }
            range_lifecycle::validate_split(source, op.split_key())
                .map_err(FeedError::Lifecycle)?;
            let targets = op.targets();
            let lo = child_descriptor(
                source,
                &targets[0],
                source.start_key(),
                op.split_key(),
                op.next_generation,
            )
            .ok_or(FeedError::SuccessorMalformed)?;
            let hi = child_descriptor(
                source,
                &targets[1],
                op.split_key(),
                source.end_key(),
                op.next_generation,
            )
            .ok_or(FeedError::SuccessorMalformed)?;
            set.push(successor(cursor, lo))
                .ok_or(FeedError::SuccessorCapacity)?;
            set.push(successor(cursor, hi))
                .ok_or(FeedError::SuccessorCapacity)?;
        }
        LifecycleKind::Merge => {
            let target_id = op.targets()[0].range_id;
            if cursor.range_id != *op.source_range_id() && cursor.range_id != target_id {
                return Err(FeedError::CursorNotAffected);
            }
            let target = descriptor_by_id(map, &target_id).ok_or(FeedError::MergeTargetNotInMap)?;
            range_lifecycle::validate_merge(source, target).map_err(FeedError::Lifecycle)?;
            // Orientation mirrors `range_lifecycle::merge_order`: the
            // shared bound must be non-empty, because an empty end
            // bound means MAX and nothing starts at MAX.
            let (lo, hi) = if !source.end_key().is_empty() && source.end_key() == target.start_key()
            {
                (source, target)
            } else if !target.end_key().is_empty() && target.end_key() == source.start_key() {
                (target, source)
            } else {
                return Err(FeedError::Lifecycle(LifecycleError::NotAdjacent));
            };
            let combined = child_descriptor(
                target,
                &op.targets()[0],
                lo.start_key(),
                hi.end_key(),
                op.next_generation,
            )
            .ok_or(FeedError::SuccessorMalformed)?;
            set.push(successor(cursor, combined))
                .ok_or(FeedError::SuccessorCapacity)?;
        }
        LifecycleKind::Relocate => {
            if &cursor.range_id != op.source_range_id() {
                return Err(FeedError::CursorNotAffected);
            }
            let moved = child_descriptor(
                source,
                &op.targets()[0],
                source.start_key(),
                source.end_key(),
                op.next_generation,
            )
            .ok_or(FeedError::SuccessorMalformed)?;
            set.push(successor(cursor, moved))
                .ok_or(FeedError::SuccessorCapacity)?;
        }
    }

    // The gapless proof, enforced rather than asserted: the successors
    // must cover the span the cursor was following.
    let followed = descriptor_by_id(map, &cursor.range_id).ok_or(FeedError::CursorRangeNotInMap)?;
    if !set.covers(followed.start_key(), followed.end_key()) {
        return Err(FeedError::SuccessorSpanIncomplete);
    }
    Ok(set)
}

/// Index a map by logical range identity.
fn descriptor_by_id<'a>(map: &'a OrderedRangeMap, id: &[u8; 16]) -> Option<&'a RangeDescriptor> {
    map.ranges().iter().find(|d| &d.range_id == id)
}

/// A successor descriptor derived from `base`: same database,
/// partition map, routing kind, replica set, and leaseholder; new
/// logical identity, physical binding, bounds, and generation.
fn child_descriptor(
    base: &RangeDescriptor,
    placement: &TargetPlacement,
    start_key: &[u8],
    end_key: &[u8],
    generation: u32,
) -> Option<RangeDescriptor> {
    let mut binding = base.binding;
    binding.partition_id = placement.partition_id;
    binding.partition_incarnation = placement.partition_incarnation;
    RangeDescriptor::new(
        base.database_id,
        base.partition_map_id,
        base.routing_kind,
        placement.range_id,
        start_key,
        end_key,
        generation,
        Lifecycle::Active,
        binding,
        base.replicas(),
        base.leaseholder,
    )
}

/// A successor cursor: the parent's resume position carried onto a new
/// descriptor.
fn successor(cursor: &FeedCursor, descriptor: RangeDescriptor) -> SuccessorCursor {
    SuccessorCursor {
        cursor: FeedCursor {
            database_id: descriptor.database_id,
            partition_map_id: descriptor.partition_map_id,
            range_id: descriptor.range_id,
            range_generation: descriptor.generation,
            revision: cursor.revision,
            timestamp: cursor.timestamp,
            format_version: cursor.format_version,
        },
        descriptor,
    }
}

// ══════════════════════════════════════════════════════════════════════
// 3. Backup manifest and state machine (§17.2, §23)
// ══════════════════════════════════════════════════════════════════════

/// Version of the [`BackupManifest`] wire encoding. Bump on ANY layout
/// change; decoders fail closed on an unknown version.
pub const BACKUP_MANIFEST_VERSION: u16 = 1;

/// Version of the exported range-artifact format the manifest
/// describes. Distinct from the manifest version: a restore target
/// that cannot read the artifact format refuses even when it can read
/// the manifest.
pub const BACKUP_ARTIFACT_FORMAT_VERSION: u16 = 1;

/// Maximum ranges named by one backup manifest.
///
/// Matches [`range_lifecycle::MAX_RANGES`] (64) — a database backup
/// names the whole descriptor set, so the bound must be the map's
/// bound, not a smaller one. Worst-case encoding is
/// [`BACKUP_MANIFEST_MAX_WIRE_LEN`].
pub const MAX_BACKUP_RANGES: usize = range_lifecycle::MAX_RANGES;

/// Digest width, bytes. SHA-256, matching §17.1's `sha256:` manifest
/// fields.
pub const DIGEST_LEN: usize = 32;

/// The all-zero digest is reserved to mean **absent**. A range entry
/// carrying it has not been exported, whatever its state byte claims.
pub const DIGEST_ABSENT: [u8; DIGEST_LEN] = [0; DIGEST_LEN];

/// §23: "encryption at rest is applied below the engine with key
/// identity in manifests; rotation is an observable, resumable
/// operation."
///
/// The manifest records which key wrapped the artifacts, at which
/// version, under which rotation epoch — so a restore can prove the
/// target still holds that key material rather than discovering it
/// cannot decrypt after publication.
///
/// Wire layout (integers LE): `[key_id:16][key_version:u32][rotation_epoch:u32]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncryptionKeyIdentity {
    /// Stable identity of the key lineage (not the key material).
    pub key_id: [u8; 16],
    /// Monotone version within the lineage; each rotation mints the
    /// next version.
    pub key_version: u32,
    /// Rotation epoch the artifacts were written under. Observability
    /// and fencing (§21 invariant 11).
    pub rotation_epoch: u32,
}

impl EncryptionKeyIdentity {
    pub const WIRE_LEN: usize = 24;

    pub const EMPTY: EncryptionKeyIdentity = EncryptionKeyIdentity {
        key_id: [0; 16],
        key_version: 0,
        rotation_epoch: 0,
    };

    /// Same lineage? Version and epoch may differ.
    pub fn same_lineage(&self, other: &EncryptionKeyIdentity) -> bool {
        self.key_id == other.key_id
    }
}

/// §23 key-rotation phases. Rotation is "observable and resumable", so
/// it is a durable lattice like every other Phase-6 operation.
///
/// ```text
/// Announced     new key version minted and published to the fleet
/// Rewrapping    artifacts being rewrapped under the new key
/// NewKeyActive  new key writes everything; old key still unwraps
/// OldKeyRetired old key material released; terminal
/// Aborted       rotation abandoned before the new key went active
/// ```
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RotationPhase {
    Announced = 1,
    Rewrapping = 2,
    NewKeyActive = 3,
    OldKeyRetired = 4,
    Aborted = 5,
}

impl RotationPhase {
    pub const ALL: [Self; 5] = [
        Self::Announced,
        Self::Rewrapping,
        Self::NewKeyActive,
        Self::OldKeyRetired,
        Self::Aborted,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Announced),
            2 => Some(Self::Rewrapping),
            3 => Some(Self::NewKeyActive),
            4 => Some(Self::OldKeyRetired),
            5 => Some(Self::Aborted),
            _ => None,
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::OldKeyRetired | Self::Aborted)
    }
}

/// Legal rotation transitions. Forward one step, or abort — but only
/// before the new key goes active: once `NewKeyActive` has been
/// observed, artifacts exist under the new key and unwinding would
/// strand them.
pub fn rotation_transition(from: RotationPhase, to: RotationPhase) -> Result<(), BackupError> {
    let ok = matches!(
        (from, to),
        (RotationPhase::Announced, RotationPhase::Rewrapping)
            | (RotationPhase::Rewrapping, RotationPhase::NewKeyActive)
            | (RotationPhase::NewKeyActive, RotationPhase::OldKeyRetired)
            | (RotationPhase::Announced, RotationPhase::Aborted)
            | (RotationPhase::Rewrapping, RotationPhase::Aborted)
    );
    if ok {
        Ok(())
    } else {
        Err(BackupError::IllegalRotationTransition { from, to })
    }
}

/// Export state of one range inside a backup.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeExportState {
    /// Discovered in the descriptor set (§17.2 step 2) but not yet
    /// exported. Digest and size are meaningless in this state.
    Required = 1,
    /// Exported at the protected timestamp with a recorded digest and
    /// byte size (§17.2 steps 3-4).
    Exported = 2,
}

impl RangeExportState {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Required),
            2 => Some(Self::Exported),
            _ => None,
        }
    }
}

/// One range's entry in a backup manifest — §17.1's `files:` list
/// lifted to database granularity.
///
/// Wire layout (integers LE):
/// `[range_id:16][range_generation:u32][state:u8][digest:32][byte_size:u64]`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeBackupEntry {
    pub range_id: [u8; 16],
    /// The descriptor generation the export was taken at. A restore
    /// that cannot match generations is restoring a different topology.
    pub range_generation: u32,
    pub state: RangeExportState,
    pub digest: [u8; DIGEST_LEN],
    pub byte_size: u64,
}

/// Wire length of one [`RangeBackupEntry`].
pub const RANGE_ENTRY_WIRE_LEN: usize = 16 + 4 + 1 + DIGEST_LEN + 8;

impl RangeBackupEntry {
    pub const EMPTY: RangeBackupEntry = RangeBackupEntry {
        range_id: [0; 16],
        range_generation: 0,
        state: RangeExportState::Required,
        digest: DIGEST_ABSENT,
        byte_size: 0,
    };

    /// A range entry counts as present only when it is `Exported`
    /// **and** carries a non-absent digest. §17.2 step 5 says "every
    /// required range **and digest**"; a state byte alone is not
    /// evidence.
    pub fn is_present(&self) -> bool {
        self.state == RangeExportState::Exported && self.digest != DIGEST_ABSENT
    }
}

/// §17.2's numbered steps as a phase lattice.
///
/// ```text
/// ProtectedTimestampAcquired  1. protected timestamp acquired
/// DescriptorSetDiscovered     2. complete descriptor set at a metadata revision
/// RangesExporting             3-4. ranges exported at that timestamp; metadata recorded
/// AllRangesPresent            5. every required range AND digest present — `complete` set HERE
/// ManifestDurable             final manifest fsynced
/// ProtectionReleased          6. protection released; terminal
/// Aborted                     6's alternative: explicitly aborted; terminal
/// ```
///
/// Discriminants start at 1 so a zeroed buffer never decodes as a
/// phase.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackupPhase {
    ProtectedTimestampAcquired = 1,
    DescriptorSetDiscovered = 2,
    RangesExporting = 3,
    AllRangesPresent = 4,
    ManifestDurable = 5,
    ProtectionReleased = 6,
    Aborted = 7,
}

impl BackupPhase {
    pub const ALL: [Self; 7] = [
        Self::ProtectedTimestampAcquired,
        Self::DescriptorSetDiscovered,
        Self::RangesExporting,
        Self::AllRangesPresent,
        Self::ManifestDurable,
        Self::ProtectionReleased,
        Self::Aborted,
    ];

    pub const INITIAL: Self = Self::ProtectedTimestampAcquired;

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::ProtectedTimestampAcquired),
            2 => Some(Self::DescriptorSetDiscovered),
            3 => Some(Self::RangesExporting),
            4 => Some(Self::AllRangesPresent),
            5 => Some(Self::ManifestDurable),
            6 => Some(Self::ProtectionReleased),
            7 => Some(Self::Aborted),
            _ => None,
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::ProtectionReleased | Self::Aborted)
    }

    /// Phases at or after which the manifest must claim completeness.
    pub const fn implies_complete(self) -> bool {
        matches!(
            self,
            Self::AllRangesPresent | Self::ManifestDurable | Self::ProtectionReleased
        )
    }
}

/// Every way a backup / rotation contract check can refuse. No
/// catch-all: a refusal must name what it refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackupError {
    /// A transition outside the legal lattice, including any
    /// transition out of a terminal phase.
    IllegalTransition { from: BackupPhase, to: BackupPhase },
    /// §17.2 step 5: completeness was claimed while some range is still
    /// `Required` or carries an absent digest. `index` names the first
    /// offender.
    RangeNotPresent { index: usize },
    /// A backup with no ranges cannot be complete — an empty descriptor
    /// set means discovery never ran.
    NoRangesDiscovered,
    /// §17.2 step 6: protection release was attempted before the final
    /// manifest was durable and without an explicit abort.
    ProtectionReleasedEarly { phase: BackupPhase },
    /// A manifest whose `complete` flag disagrees with its phase or its
    /// entries. This is what a false-complete manifest decodes to —
    /// which is to say, it does not decode at all.
    CompletenessInconsistent { phase: BackupPhase, complete: bool },
    /// The manifest already names [`MAX_BACKUP_RANGES`] ranges.
    RangeCapacityExceeded,
    /// A range id already present in the manifest was discovered twice.
    DuplicateRange { index: usize },
    /// The range being marked exported is not in the manifest.
    RangeNotDiscovered,
    /// An export was recorded with the reserved absent digest.
    DigestAbsent,
    /// An illegal §23 key-rotation transition.
    IllegalRotationTransition {
        from: RotationPhase,
        to: RotationPhase,
    },
}

/// What a crashed backup operation may do when it is reloaded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackupRecovery {
    /// Resume by driving the machine to `next`. The protected
    /// timestamp is still held, which is exactly why resuming is safe.
    Resumable { next: BackupPhase },
    /// The operation was explicitly aborted; protection may be
    /// released and artifacts reclaimed.
    Aborted,
    /// The operation ran to completion and released protection.
    Finished,
}

/// The durable record of one database backup (§17.2), shaped after
/// §17.1's per-range snapshot manifest.
///
/// Wire layout ([`BACKUP_MANIFEST_VERSION`] 1, all integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [backup_id:16]
/// [database_id:u32]
/// [protected_timestamp:u64]
/// [metadata_revision:u64]
/// [artifact_format_version:u16]
/// [key_id:16][key_version:u32][rotation_epoch:u32]
/// [phase:u8][complete:u8]
/// [entry_count:u8]
/// entry_count × [range_id:16][range_generation:u32][state:u8][digest:32][byte_size:u64]
/// ```
///
/// `complete` is a `u8` restricted to `0` or `1`; any other byte fails
/// closed. [`decode`](BackupManifest::decode) additionally runs
/// [`check_invariants`](BackupManifest::check_invariants), so a
/// manifest that survived a crash mid-write and *claims* completeness
/// without its digests cannot be read back as a valid manifest. That
/// is the durable half of §17.2 step 5.
///
/// Not `Copy`: at [`MAX_BACKUP_RANGES`] = 64 entries the record is
/// several kilobytes, and an implicit copy on every use would be a
/// stack hazard in a PIC module. Callers hold one and pass `&`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupManifest {
    /// Globally unique backup identity. All-zero is rejected.
    pub backup_id: [u8; 16],
    pub database_id: u32,
    /// §17.2 step 1: the timestamp protection is held at. Every range
    /// is exported at exactly this timestamp.
    pub protected_timestamp: Timestamp,
    /// §17.2 step 2: the metadata revision at which the complete
    /// descriptor set was discovered. A restore that cannot reproduce
    /// this revision is restoring a different descriptor set.
    pub metadata_revision: u64,
    /// Format of the exported artifacts (not of this manifest).
    pub artifact_format_version: u16,
    /// §23: which key wrapped the artifacts.
    pub key: EncryptionKeyIdentity,
    phase: BackupPhase,
    complete: bool,
    entry_count: u8,
    entries: [RangeBackupEntry; MAX_BACKUP_RANGES],
}

/// Fixed wire overhead of a manifest: header plus every field except
/// the range entries.
pub const BACKUP_MANIFEST_FIXED_WIRE_LEN: usize =
    4 + 16 + 4 + 8 + 8 + 2 + EncryptionKeyIdentity::WIRE_LEN + 1 + 1 + 1;
/// Worst-case encoded manifest length.
pub const BACKUP_MANIFEST_MAX_WIRE_LEN: usize =
    BACKUP_MANIFEST_FIXED_WIRE_LEN + MAX_BACKUP_RANGES * RANGE_ENTRY_WIRE_LEN;

impl BackupManifest {
    pub const EMPTY: BackupManifest = BackupManifest {
        backup_id: [0; 16],
        database_id: 0,
        protected_timestamp: 0,
        metadata_revision: 0,
        artifact_format_version: BACKUP_ARTIFACT_FORMAT_VERSION,
        key: EncryptionKeyIdentity::EMPTY,
        phase: BackupPhase::INITIAL,
        complete: false,
        entry_count: 0,
        entries: [RangeBackupEntry::EMPTY; MAX_BACKUP_RANGES],
    };

    /// §17.2 step 1: begin a backup by recording the protected
    /// timestamp it holds. Fails closed on a null backup id.
    pub fn begin(
        backup_id: [u8; 16],
        database_id: u32,
        protected_timestamp: Timestamp,
        key: EncryptionKeyIdentity,
    ) -> Option<Self> {
        if backup_id == [0u8; 16] {
            return None;
        }
        let mut m = Self::EMPTY;
        m.backup_id = backup_id;
        m.database_id = database_id;
        m.protected_timestamp = protected_timestamp;
        m.key = key;
        Some(m)
    }

    pub fn phase(&self) -> BackupPhase {
        self.phase
    }

    /// §17.2 step 5's flag. Never settable by a caller: it is a
    /// consequence of reaching [`BackupPhase::AllRangesPresent`].
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn entries(&self) -> &[RangeBackupEntry] {
        &self.entries[..self.entry_count as usize]
    }

    /// §17.2 step 2: record one discovered range. Refuses duplicates
    /// and refuses once the manifest is past discovery — the
    /// descriptor set is fixed at one metadata revision, so it cannot
    /// grow after export starts.
    pub fn discover_range(
        &mut self,
        range_id: [u8; 16],
        range_generation: u32,
    ) -> Result<(), BackupError> {
        if !matches!(
            self.phase,
            BackupPhase::ProtectedTimestampAcquired | BackupPhase::DescriptorSetDiscovered
        ) {
            return Err(BackupError::IllegalTransition {
                from: self.phase,
                to: BackupPhase::DescriptorSetDiscovered,
            });
        }
        if let Some(index) = self.index_of(&range_id) {
            return Err(BackupError::DuplicateRange { index });
        }
        if self.entry_count as usize >= MAX_BACKUP_RANGES {
            return Err(BackupError::RangeCapacityExceeded);
        }
        let e = &mut self.entries[self.entry_count as usize];
        *e = RangeBackupEntry::EMPTY;
        e.range_id = range_id;
        e.range_generation = range_generation;
        self.entry_count += 1;
        Ok(())
    }

    /// §17.2 steps 3-4: record one range's export. Refuses a reserved
    /// absent digest and refuses a range that was never discovered.
    pub fn record_export(
        &mut self,
        range_id: &[u8; 16],
        digest: [u8; DIGEST_LEN],
        byte_size: u64,
    ) -> Result<(), BackupError> {
        if self.phase != BackupPhase::RangesExporting {
            return Err(BackupError::IllegalTransition {
                from: self.phase,
                to: BackupPhase::RangesExporting,
            });
        }
        if digest == DIGEST_ABSENT {
            return Err(BackupError::DigestAbsent);
        }
        let index = self
            .index_of(range_id)
            .ok_or(BackupError::RangeNotDiscovered)?;
        let e = &mut self.entries[index];
        e.state = RangeExportState::Exported;
        e.digest = digest;
        e.byte_size = byte_size;
        Ok(())
    }

    /// Index of the first entry that is not present (§17.2 step 5).
    pub fn first_missing(&self) -> Option<usize> {
        self.entries().iter().position(|e| !e.is_present())
    }

    fn index_of(&self, range_id: &[u8; 16]) -> Option<usize> {
        self.entries().iter().position(|e| &e.range_id == range_id)
    }

    /// Drive the §17.2 machine.
    ///
    /// Legal edges, and the guards that are the point of the whole
    /// section:
    ///
    /// ```text
    /// ProtectedTimestampAcquired → DescriptorSetDiscovered   (≥1 range discovered)
    /// DescriptorSetDiscovered    → RangesExporting
    /// RangesExporting            → AllRangesPresent          (EVERY range present — sets `complete`)
    /// AllRangesPresent           → ManifestDurable
    /// ManifestDurable            → ProtectionReleased        (§17.2 step 6)
    /// {1,2,3,4,5}                → Aborted                   (step 6's explicit alternative)
    /// ```
    ///
    /// Everything else is [`BackupError::IllegalTransition`]: no
    /// skipping, no going backwards, nothing out of a terminal phase.
    /// In particular there is no edge to `ProtectionReleased` from
    /// anywhere but `ManifestDurable`, which is exactly step 6.
    pub fn transition(&mut self, to: BackupPhase) -> Result<(), BackupError> {
        let from = self.phase;
        if from.is_terminal() {
            return Err(BackupError::IllegalTransition { from, to });
        }
        if to == BackupPhase::Aborted {
            // An abort retracts completeness. A backup aborted after
            // `AllRangesPresent` still has all its digests recorded,
            // but its artifacts are now reclaimable, so it must not
            // read as a restorable artifact set — and leaving the flag
            // set would violate `check_invariants`, making the record
            // undecodable after a crash.
            self.phase = BackupPhase::Aborted;
            self.complete = false;
            return Ok(());
        }
        let legal = matches!(
            (from, to),
            (
                BackupPhase::ProtectedTimestampAcquired,
                BackupPhase::DescriptorSetDiscovered
            ) | (
                BackupPhase::DescriptorSetDiscovered,
                BackupPhase::RangesExporting
            ) | (BackupPhase::RangesExporting, BackupPhase::AllRangesPresent)
                | (BackupPhase::AllRangesPresent, BackupPhase::ManifestDurable)
                | (
                    BackupPhase::ManifestDurable,
                    BackupPhase::ProtectionReleased
                )
        );
        if !legal {
            return Err(BackupError::IllegalTransition { from, to });
        }
        match to {
            BackupPhase::DescriptorSetDiscovered => {
                if self.entry_count == 0 {
                    return Err(BackupError::NoRangesDiscovered);
                }
            }
            BackupPhase::AllRangesPresent => {
                // §17.2 step 5, the whole rule in three lines.
                if self.entry_count == 0 {
                    return Err(BackupError::NoRangesDiscovered);
                }
                if let Some(index) = self.first_missing() {
                    return Err(BackupError::RangeNotPresent { index });
                }
                self.complete = true;
            }
            _ => {}
        }
        self.phase = to;
        Ok(())
    }

    /// §17.2 step 6 as a predicate: protection may be released only
    /// after the final manifest is durable, or after an explicit abort.
    pub fn may_release_protection(&self) -> bool {
        matches!(
            self.phase,
            BackupPhase::ManifestDurable | BackupPhase::Aborted
        )
    }

    /// The typed form of [`may_release_protection`].
    pub fn check_release_protection(&self) -> Result<(), BackupError> {
        if self.may_release_protection() {
            Ok(())
        } else {
            Err(BackupError::ProtectionReleasedEarly { phase: self.phase })
        }
    }

    /// What a process that crashed at this phase may do on reload.
    /// There is no fourth answer: every phase is either resumable, or
    /// explicitly aborted, or finished — a false-complete manifest is
    /// not representable, because `complete` is only ever set by the
    /// guarded transition and is re-checked on decode.
    pub fn recovery(&self) -> BackupRecovery {
        match self.phase {
            BackupPhase::Aborted => BackupRecovery::Aborted,
            BackupPhase::ProtectionReleased => BackupRecovery::Finished,
            BackupPhase::ProtectedTimestampAcquired => BackupRecovery::Resumable {
                next: BackupPhase::DescriptorSetDiscovered,
            },
            BackupPhase::DescriptorSetDiscovered => BackupRecovery::Resumable {
                next: BackupPhase::RangesExporting,
            },
            BackupPhase::RangesExporting => BackupRecovery::Resumable {
                next: BackupPhase::AllRangesPresent,
            },
            BackupPhase::AllRangesPresent => BackupRecovery::Resumable {
                next: BackupPhase::ManifestDurable,
            },
            BackupPhase::ManifestDurable => BackupRecovery::Resumable {
                next: BackupPhase::ProtectionReleased,
            },
        }
    }

    /// The manifest's self-consistency rules, checked on every decode:
    ///
    /// 1. `complete` implies at least one range and every range
    ///    present;
    /// 2. a phase that implies completeness must carry `complete`;
    /// 3. a phase that does not imply completeness must not carry it.
    ///
    /// Rule 1 is the durable form of §17.2 step 5. Rules 2 and 3 make
    /// the flag and the phase inseparable, so a torn write cannot
    /// produce a manifest that reads as complete.
    pub fn check_invariants(&self) -> Result<(), BackupError> {
        if self.complete {
            if self.entry_count == 0 {
                return Err(BackupError::NoRangesDiscovered);
            }
            if let Some(index) = self.first_missing() {
                return Err(BackupError::RangeNotPresent { index });
            }
        }
        if self.phase.implies_complete() != self.complete {
            return Err(BackupError::CompletenessInconsistent {
                phase: self.phase,
                complete: self.complete,
            });
        }
        Ok(())
    }

    /// Exact encoded length of this manifest.
    pub fn wire_len(&self) -> usize {
        BACKUP_MANIFEST_FIXED_WIRE_LEN + self.entry_count as usize * RANGE_ENTRY_WIRE_LEN
    }

    /// Serialize to the versioned wire form. Fails closed on an
    /// undersized `out`.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let total = self.wire_len();
        if out.len() < total {
            return None;
        }
        out[0..2].copy_from_slice(&BACKUP_MANIFEST_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((total - 4) as u16).to_le_bytes());
        let mut n = 4;
        out[n..n + 16].copy_from_slice(&self.backup_id);
        n += 16;
        out[n..n + 4].copy_from_slice(&self.database_id.to_le_bytes());
        n += 4;
        out[n..n + 8].copy_from_slice(&self.protected_timestamp.to_le_bytes());
        n += 8;
        out[n..n + 8].copy_from_slice(&self.metadata_revision.to_le_bytes());
        n += 8;
        out[n..n + 2].copy_from_slice(&self.artifact_format_version.to_le_bytes());
        n += 2;
        out[n..n + 16].copy_from_slice(&self.key.key_id);
        n += 16;
        out[n..n + 4].copy_from_slice(&self.key.key_version.to_le_bytes());
        n += 4;
        out[n..n + 4].copy_from_slice(&self.key.rotation_epoch.to_le_bytes());
        n += 4;
        out[n] = self.phase as u8;
        n += 1;
        out[n] = u8::from(self.complete);
        n += 1;
        out[n] = self.entry_count;
        n += 1;
        for e in self.entries() {
            out[n..n + 16].copy_from_slice(&e.range_id);
            n += 16;
            out[n..n + 4].copy_from_slice(&e.range_generation.to_le_bytes());
            n += 4;
            out[n] = e.state as u8;
            n += 1;
            out[n..n + DIGEST_LEN].copy_from_slice(&e.digest);
            n += DIGEST_LEN;
            out[n..n + 8].copy_from_slice(&e.byte_size.to_le_bytes());
            n += 8;
        }
        debug_assert_eq!(n, total);
        Some(n)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// declared-length mismatch, truncation, trailing bytes, unknown
    /// phase/state discriminants, a `complete` byte other than 0 or 1,
    /// an entry count over [`MAX_BACKUP_RANGES`], a null backup id,
    /// **or any [`check_invariants`](BackupManifest::check_invariants)
    /// violation** — a manifest that claims completeness it cannot
    /// substantiate does not decode.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < 4 {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != BACKUP_MANIFEST_VERSION {
            return None;
        }
        let payload_len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if src.len() != 4 + payload_len {
            return None;
        }
        let p = &src[4..];
        let mut n = 0usize;
        let backup_id: [u8; 16] = p.get(n..n + 16)?.try_into().ok()?;
        n += 16;
        if backup_id == [0u8; 16] {
            return None;
        }
        let database_id = u32::from_le_bytes(p.get(n..n + 4)?.try_into().ok()?);
        n += 4;
        let protected_timestamp = u64::from_le_bytes(p.get(n..n + 8)?.try_into().ok()?);
        n += 8;
        let metadata_revision = u64::from_le_bytes(p.get(n..n + 8)?.try_into().ok()?);
        n += 8;
        let artifact_format_version = u16::from_le_bytes(p.get(n..n + 2)?.try_into().ok()?);
        n += 2;
        let key_id: [u8; 16] = p.get(n..n + 16)?.try_into().ok()?;
        n += 16;
        let key_version = u32::from_le_bytes(p.get(n..n + 4)?.try_into().ok()?);
        n += 4;
        let rotation_epoch = u32::from_le_bytes(p.get(n..n + 4)?.try_into().ok()?);
        n += 4;
        let phase = BackupPhase::from_u8(*p.get(n)?)?;
        n += 1;
        let complete = match *p.get(n)? {
            0 => false,
            1 => true,
            _ => return None,
        };
        n += 1;
        let entry_count = *p.get(n)? as usize;
        n += 1;
        if entry_count > MAX_BACKUP_RANGES {
            return None;
        }
        let mut entries = [RangeBackupEntry::EMPTY; MAX_BACKUP_RANGES];
        for e in entries.iter_mut().take(entry_count) {
            e.range_id = p.get(n..n + 16)?.try_into().ok()?;
            n += 16;
            e.range_generation = u32::from_le_bytes(p.get(n..n + 4)?.try_into().ok()?);
            n += 4;
            e.state = RangeExportState::from_u8(*p.get(n)?)?;
            n += 1;
            e.digest = p.get(n..n + DIGEST_LEN)?.try_into().ok()?;
            n += DIGEST_LEN;
            e.byte_size = u64::from_le_bytes(p.get(n..n + 8)?.try_into().ok()?);
            n += 8;
        }
        if n != payload_len {
            return None; // declared length disagrees with content
        }
        let m = Self {
            backup_id,
            database_id,
            protected_timestamp,
            metadata_revision,
            artifact_format_version,
            key: EncryptionKeyIdentity {
                key_id,
                key_version,
                rotation_epoch,
            },
            phase,
            complete,
            entry_count: entry_count as u8,
            entries,
        };
        m.check_invariants().ok()?;
        Some(m)
    }
}

// ── Restore (§17.2) ───────────────────────────────────────────────────

/// One artifact the restore target has fetched and authenticated
/// **before** publication (§17.2: "validates all artifacts before
/// publication").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerifiedArtifact {
    pub range_id: [u8; 16],
    pub digest: [u8; DIGEST_LEN],
}

impl VerifiedArtifact {
    pub const EMPTY: VerifiedArtifact = VerifiedArtifact {
        range_id: [0; 16],
        digest: DIGEST_ABSENT,
    };
}

/// The state a restore is being validated against.
///
/// This is not persisted — it is the coordinator's view of the target
/// database at validation time — so it has no wire form and no version.
#[derive(Clone, Debug)]
pub struct RestoreTarget {
    pub database_id: u32,
    /// Artifact format this build can read.
    pub artifact_format_version: u16,
    /// The key the target currently holds.
    pub key: EncryptionKeyIdentity,
    /// Oldest key version in the lineage whose material is still
    /// retained. §23's rotation is resumable and observable, so a
    /// restore under a retired key version must fail closed rather
    /// than discover it cannot decrypt after publication.
    pub oldest_retained_key_version: u32,
    live_count: usize,
    live: [[u8; 16]; MAX_BACKUP_RANGES],
    verified_count: usize,
    verified: [VerifiedArtifact; MAX_BACKUP_RANGES],
}

impl RestoreTarget {
    pub const EMPTY: RestoreTarget = RestoreTarget {
        database_id: 0,
        artifact_format_version: BACKUP_ARTIFACT_FORMAT_VERSION,
        key: EncryptionKeyIdentity::EMPTY,
        oldest_retained_key_version: 0,
        live_count: 0,
        live: [[0; 16]; MAX_BACKUP_RANGES],
        verified_count: 0,
        verified: [VerifiedArtifact::EMPTY; MAX_BACKUP_RANGES],
    };

    pub const fn new(
        database_id: u32,
        artifact_format_version: u16,
        key: EncryptionKeyIdentity,
        oldest_retained_key_version: u32,
    ) -> Self {
        let mut t = Self::EMPTY;
        t.database_id = database_id;
        t.artifact_format_version = artifact_format_version;
        t.key = key;
        t.oldest_retained_key_version = oldest_retained_key_version;
        t
    }

    /// Declare a range incarnation that is currently live in the
    /// target. Restore may never publish over one of these.
    pub fn add_live_incarnation(&mut self, range_id: [u8; 16]) -> Option<()> {
        if self.live_count >= MAX_BACKUP_RANGES {
            return None;
        }
        self.live[self.live_count] = range_id;
        self.live_count += 1;
        Some(())
    }

    /// Declare an artifact fetched and authenticated for restore.
    pub fn add_verified_artifact(&mut self, artifact: VerifiedArtifact) -> Option<()> {
        if self.verified_count >= MAX_BACKUP_RANGES {
            return None;
        }
        self.verified[self.verified_count] = artifact;
        self.verified_count += 1;
        Some(())
    }

    pub fn live_incarnations(&self) -> &[[u8; 16]] {
        &self.live[..self.live_count]
    }

    pub fn verified_artifacts(&self) -> &[VerifiedArtifact] {
        &self.verified[..self.verified_count]
    }
}

/// Every way a restore can be refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreError {
    /// The manifest does not claim completeness. An incomplete backup
    /// is not a restorable artifact set (§17.2 step 5).
    ManifestNotComplete { phase: BackupPhase },
    /// The manifest names ranges but claims none, or vice versa.
    EmptyManifest,
    /// The manifest is for a different database.
    DatabaseMismatch { manifest: u32, target: u32 },
    /// The target cannot read this artifact format.
    IncompatibleFormatVersion { manifest: u16, target: u16 },
    /// The manifest was wrapped under a different key lineage.
    KeyLineageMismatch,
    /// The manifest's key version is outside the target's retained
    /// window: the material needed to unwrap it is gone (§23).
    KeyVersionRetired {
        manifest: u32,
        oldest_retained: u32,
        current: u32,
    },
    /// §17.2: restore "never overwrites a live incarnation in place".
    /// `index` names the manifest entry whose range is live in the
    /// target.
    LiveIncarnationPresent { index: usize },
    /// No verified artifact was supplied for a manifest entry: the
    /// restore would publish something it never validated.
    ArtifactMissing { index: usize },
    /// The verified artifact's digest disagrees with the manifest's.
    DigestMismatch { index: usize },
    /// A manifest entry carries the reserved absent digest despite the
    /// manifest claiming completeness. (Unreachable through the state
    /// machine; reachable through hand-built bytes, so it is checked.)
    DigestAbsent { index: usize },
}

/// §17.2's restore preconditions, all of them, fail-closed.
///
/// > Restore creates new range incarnations and validates all artifacts
/// > before publication. It never overwrites a live incarnation in
/// > place.
///
/// Check order: manifest completeness → database identity → artifact
/// format → encryption key → per-entry live-incarnation and artifact
/// validation. Nothing here publishes anything; it returns `Ok` only
/// when publication would be safe, and the caller then creates **new**
/// incarnations.
pub fn validate_restore(
    manifest: &BackupManifest,
    target: &RestoreTarget,
) -> Result<(), RestoreError> {
    if !manifest.is_complete() {
        return Err(RestoreError::ManifestNotComplete {
            phase: manifest.phase(),
        });
    }
    if manifest.entries().is_empty() {
        return Err(RestoreError::EmptyManifest);
    }
    if manifest.database_id != target.database_id {
        return Err(RestoreError::DatabaseMismatch {
            manifest: manifest.database_id,
            target: target.database_id,
        });
    }
    if manifest.artifact_format_version != target.artifact_format_version {
        return Err(RestoreError::IncompatibleFormatVersion {
            manifest: manifest.artifact_format_version,
            target: target.artifact_format_version,
        });
    }
    if !manifest.key.same_lineage(&target.key) {
        return Err(RestoreError::KeyLineageMismatch);
    }
    if manifest.key.key_version < target.oldest_retained_key_version
        || manifest.key.key_version > target.key.key_version
    {
        return Err(RestoreError::KeyVersionRetired {
            manifest: manifest.key.key_version,
            oldest_retained: target.oldest_retained_key_version,
            current: target.key.key_version,
        });
    }
    for (index, e) in manifest.entries().iter().enumerate() {
        if e.digest == DIGEST_ABSENT {
            return Err(RestoreError::DigestAbsent { index });
        }
        if target.live_incarnations().contains(&e.range_id) {
            return Err(RestoreError::LiveIncarnationPresent { index });
        }
        let artifact = target
            .verified_artifacts()
            .iter()
            .find(|a| a.range_id == e.range_id)
            .ok_or(RestoreError::ArtifactMissing { index })?;
        if artifact.digest != e.digest {
            return Err(RestoreError::DigestMismatch { index });
        }
    }
    Ok(())
}

// ══════════════════════════════════════════════════════════════════════
// 4. Retention claims (§18, §21 invariant 13)
// ══════════════════════════════════════════════════════════════════════

/// Version of the [`RetentionClaim`] wire encoding. Bump on ANY layout
/// change; decoders fail closed on an unknown version.
pub const RETENTION_CLAIM_VERSION: u16 = 1;

/// Maximum claims considered in one floor computation. Bounded because
/// §21 invariant 14 forbids unbounded allocation; a coordinator with
/// more claims than this must aggregate per source before calling.
pub const MAX_RETENTION_CLAIMS: usize = 64;

/// Number of distinct claim sources — §18's bullet list, one per line.
pub const CLAIM_SOURCE_COUNT: usize = 8;

/// An expiry of zero means "never expires". A legal hold with a wall-
/// clock expiry is a policy decision; a legal hold with none is the
/// common case, and zero is the encoding of it.
pub const NEVER_EXPIRES: u64 = 0;

/// §18's claim sources, in the order the RFC lists them.
///
/// Discriminants start at 1 so a zeroed buffer never decodes as a
/// source.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimSource {
    /// Active transaction reads and unresolved intents.
    ActiveRead = 1,
    /// Watches and change-feed cursors.
    WatchCursor = 2,
    /// Backups and restores.
    Backup = 3,
    /// Range split, merge, and relocation operations.
    LifecycleOp = 4,
    /// Schema and index backfills.
    SchemaJob = 5,
    /// Application snapshots still eligible for restore.
    SnapshotRestorable = 6,
    /// Configured operator retention.
    OperatorHold = 7,
    /// Legal holds.
    LegalHold = 8,
}

impl ClaimSource {
    pub const ALL: [Self; CLAIM_SOURCE_COUNT] = [
        Self::ActiveRead,
        Self::WatchCursor,
        Self::Backup,
        Self::LifecycleOp,
        Self::SchemaJob,
        Self::SnapshotRestorable,
        Self::OperatorHold,
        Self::LegalHold,
    ];

    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::ActiveRead),
            2 => Some(Self::WatchCursor),
            3 => Some(Self::Backup),
            4 => Some(Self::LifecycleOp),
            5 => Some(Self::SchemaJob),
            6 => Some(Self::SnapshotRestorable),
            7 => Some(Self::OperatorHold),
            8 => Some(Self::LegalHold),
            _ => None,
        }
    }

    /// Dense index into [`SourceRegistry`]'s array.
    pub const fn index(self) -> usize {
        self as usize - 1
    }
}

/// One retention claim (§18): "the compaction coordinator collects
/// signed/source-fenced claims".
///
/// Wire layout ([`RETENTION_CLAIM_VERSION`] 1, all integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]
/// [claim_id:16]
/// [source:u8]
/// [protected_timestamp:u64]
/// [holder:16]
/// [expires_unix_ms:u64]
/// [source_epoch:u64]
/// ```
///
/// `expires_unix_ms` is wall time and is the ONLY wall-clock input in
/// this file. It is never read from a local clock here: `now` is a
/// parameter of [`effective_floor`], so the arithmetic is deterministic
/// and replayable (§21 invariant 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionClaim {
    /// Unique claim identity. All-zero is rejected.
    pub claim_id: [u8; 16],
    pub source: ClaimSource,
    /// The timestamp this claim protects: history at or after it must
    /// be retained.
    pub protected_timestamp: Timestamp,
    /// Identity of whoever holds the claim (session, operation, or
    /// operator principal). Carried so a stuck floor names its cause.
    pub holder: [u8; 16],
    /// Wall-clock expiry, or [`NEVER_EXPIRES`].
    pub expires_unix_ms: u64,
    /// The source's fence/epoch at the time the claim was issued
    /// (§21 invariant 11: "a storage fence is valid only with the
    /// correct source identity and epoch").
    pub source_epoch: u64,
}

impl RetentionClaim {
    /// Encoded wire size: header + fixed payload.
    pub const WIRE_LEN: usize = 4 + 57;

    pub const EMPTY: RetentionClaim = RetentionClaim {
        claim_id: [0; 16],
        source: ClaimSource::ActiveRead,
        protected_timestamp: 0,
        holder: [0; 16],
        expires_unix_ms: NEVER_EXPIRES,
        source_epoch: 0,
    };

    /// Is this claim still live at `now_unix_ms`?
    ///
    /// A claim is excluded ONLY when `now` proves it expired. Equality
    /// counts as expired (`expires_unix_ms` is the first instant the
    /// claim no longer holds), and [`NEVER_EXPIRES`] never expires.
    pub fn is_live_at(&self, now_unix_ms: u64) -> bool {
        self.expires_unix_ms == NEVER_EXPIRES || now_unix_ms < self.expires_unix_ms
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&RETENTION_CLAIM_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        out[4..20].copy_from_slice(&self.claim_id);
        out[20] = self.source as u8;
        out[21..29].copy_from_slice(&self.protected_timestamp.to_le_bytes());
        out[29..45].copy_from_slice(&self.holder);
        out[45..53].copy_from_slice(&self.expires_unix_ms.to_le_bytes());
        out[53..61].copy_from_slice(&self.source_epoch.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// length mismatch, an unknown source discriminant, or a null claim
    /// id.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != Self::WIRE_LEN {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != RETENTION_CLAIM_VERSION {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len != Self::WIRE_LEN - 4 {
            return None;
        }
        let claim_id: [u8; 16] = src[4..20].try_into().ok()?;
        if claim_id == [0u8; 16] {
            return None;
        }
        Some(Self {
            claim_id,
            source: ClaimSource::from_u8(src[20])?,
            protected_timestamp: u64::from_le_bytes(src[21..29].try_into().ok()?),
            holder: src[29..45].try_into().ok()?,
            expires_unix_ms: u64::from_le_bytes(src[45..53].try_into().ok()?),
            source_epoch: u64::from_le_bytes(src[53..61].try_into().ok()?),
        })
    }
}

/// What the coordinator knows about one claim source at collection
/// time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceState {
    /// The source must report before the floor may advance. An
    /// unreported required source is [`FloorError::SourceMissing`] —
    /// silence is not "no claims", it is "unknown".
    pub required: bool,
    /// The source reported this round (possibly with zero claims).
    pub reported: bool,
    /// The epoch the source reported at.
    pub reported_epoch: u64,
    /// The source's current authoritative epoch.
    pub current_epoch: u64,
}

impl SourceState {
    pub const EMPTY: SourceState = SourceState {
        required: false,
        reported: false,
        reported_epoch: 0,
        current_epoch: 0,
    };
}

/// The per-source collection state §18's coordinator assembles before
/// computing a candidate floor.
///
/// Not persisted — it is the coordinator's in-flight view — so it has
/// no wire form.
#[derive(Clone, Copy, Debug)]
pub struct SourceRegistry {
    states: [SourceState; CLAIM_SOURCE_COUNT],
}

impl SourceRegistry {
    pub const fn new() -> Self {
        Self {
            states: [SourceState::EMPTY; CLAIM_SOURCE_COUNT],
        }
    }

    /// Mark a source as one that MUST report, at its current epoch.
    pub fn require(&mut self, source: ClaimSource, current_epoch: u64) {
        let s = &mut self.states[source.index()];
        s.required = true;
        s.current_epoch = current_epoch;
    }

    /// Record that a source reported, at the epoch it claims. A report
    /// whose epoch differs from the source's current epoch is stale and
    /// blocks — that check lives in [`effective_floor`] so the refusal
    /// is typed rather than silent.
    pub fn report(&mut self, source: ClaimSource, reported_epoch: u64) {
        let s = &mut self.states[source.index()];
        s.reported = true;
        s.reported_epoch = reported_epoch;
    }

    pub fn state(&self, source: ClaimSource) -> &SourceState {
        &self.states[source.index()]
    }
}

impl Default for SourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Every way the floor computation refuses to advance. §18: "missing or
/// stale claim sources block unsafe advancement" — so each of these is
/// a refusal, never a smaller floor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FloorError {
    /// A required source did not report. §21 invariant 13.
    SourceMissing { source: ClaimSource },
    /// A source reported at an epoch other than its current one: its
    /// claim set may predate a fence and cannot be trusted.
    SourceStale {
        source: ClaimSource,
        current_epoch: u64,
        reported_epoch: u64,
    },
    /// A claim arrived from a source that did not report. Its claim set
    /// is by definition incomplete, so its claims cannot be used to
    /// bound anything.
    ClaimFromUnreportedSource { index: usize, source: ClaimSource },
    /// A claim carries an epoch other than its source's current one:
    /// an unfenced claim (§21 invariant 11).
    ClaimEpochStale {
        index: usize,
        source: ClaimSource,
        current_epoch: u64,
        claim_epoch: u64,
    },
    /// A claim with a null identity — a zeroed buffer read as a claim.
    ClaimIdentityNull { index: usize },
    /// More claims than [`MAX_RETENTION_CLAIMS`] (§21 invariant 14:
    /// bounded refusal, not unbounded work).
    TooManyClaims { count: usize },
}

/// §18's floor arithmetic: **the effective floor is bounded by the
/// oldest requirement.**
///
/// ```text
/// floor = min(candidate, min over live claims of protected_timestamp)
/// ```
///
/// with three fail-closed guards ahead of it:
///
/// 1. every **required** source must have reported — silence blocks;
/// 2. every reporting source's epoch must be current — a stale report
///    blocks;
/// 3. every claim must come from a reporting source at that source's
///    current epoch — an unfenced claim blocks.
///
/// A claim is excluded from the minimum only when `now_unix_ms` proves
/// it expired ([`RetentionClaim::is_live_at`]). `now` is a parameter,
/// never a clock read: this function is pure, and the compaction
/// coordinator owns collection, the durable floor transition, and the
/// physical reclamation that follows it.
///
/// The returned floor is `<= candidate` and `<=` every live claim's
/// protected timestamp, which is the executable form of §21 invariant
/// 13 ("watch and backup retention cannot be advanced past an active
/// protected claim").
pub fn effective_floor(
    claims: &[RetentionClaim],
    registry: &SourceRegistry,
    candidate: Timestamp,
    now_unix_ms: u64,
) -> Result<Timestamp, FloorError> {
    if claims.len() > MAX_RETENTION_CLAIMS {
        return Err(FloorError::TooManyClaims {
            count: claims.len(),
        });
    }
    // Guard 1 and 2: source coverage and source freshness. Checked in
    // §18's listed order so a refusal is reproducible.
    for source in ClaimSource::ALL {
        let s = registry.state(source);
        if s.required && !s.reported {
            return Err(FloorError::SourceMissing { source });
        }
        if s.reported && s.reported_epoch != s.current_epoch {
            return Err(FloorError::SourceStale {
                source,
                current_epoch: s.current_epoch,
                reported_epoch: s.reported_epoch,
            });
        }
    }
    // Guard 3, then the minimum. One pass; claims are bounded above.
    let mut floor = candidate;
    for (index, c) in claims.iter().enumerate() {
        if c.claim_id == [0u8; 16] {
            return Err(FloorError::ClaimIdentityNull { index });
        }
        let s = registry.state(c.source);
        if !s.reported {
            return Err(FloorError::ClaimFromUnreportedSource {
                index,
                source: c.source,
            });
        }
        if c.source_epoch != s.current_epoch {
            return Err(FloorError::ClaimEpochStale {
                index,
                source: c.source,
                current_epoch: s.current_epoch,
                claim_epoch: c.source_epoch,
            });
        }
        if c.is_live_at(now_unix_ms) && c.protected_timestamp < floor {
            floor = c.protected_timestamp;
        }
    }
    Ok(floor)
}
