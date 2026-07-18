//! Canonical internal key encoding — RFC database-foundation §9.1.
//!
//! One versioned, order-preserving byte encoding shared by every
//! state-store provider (memory and disk) and every future sorted-file
//! format. The whole design goal is that **plain bytewise comparison of
//! encoded keys equals the logical tuple ordering**:
//!
//! ```text
//! tenant | database | keyspace | user_key | descending_mvcc_timestamp | value_kind
//! ```
//!
//! Layout (`KEY_FORMAT_VERSION` 1):
//!
//! ```text
//! [tenant:u32 BE][database:u32 BE][keyspace:u32 BE]
//! [escaped user_key][0x00 0x01]
//! [!mvcc_timestamp:u64 BE][value_kind:u8]
//! ```
//!
//! - Identity components are fixed-width big-endian integers: trivially
//!   order-preserving and prefix-unambiguous, and they match the bounded
//!   catalog-resolved IDs the PIC modules already use (`TenantId` is
//!   `u32`; database/keyspace IDs are catalog-assigned `u32`).
//! - `user_key` is escaped, not length-prefixed: a length prefix sorts
//!   short keys before long ones *by length*, which breaks lexical
//!   ordering. Escaping preserves it: `0x00` in the key becomes
//!   `0x00 0xFF`, and the component ends with the terminator
//!   `0x00 0x01`. Terminator (`0x01`) < escape (`0xFF`) guarantees a
//!   proper prefix sorts before its extensions and no encoded key is a
//!   prefix of another (prefix-ambiguity rule in §9.1).
//! - The MVCC timestamp is stored bit-inverted so **newer versions sort
//!   before older versions** of the same user key under ascending byte
//!   order.
//! - `value_kind` is the final byte and distinguishes live values,
//!   tombstones, intents, transaction records, and internal metadata.
//!
//! The version constant is recorded in engine manifests / snapshot
//! headers — not in every key — and any layout change requires a bump
//! plus an explicit format migration (§9.1). Golden vectors live in
//! `tests/contract_internal_key.rs`; changing them is a format break.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

/// Version of the encoding described in this file. Recorded in engine
/// manifests and snapshot headers; bump on ANY layout change.
pub const KEY_FORMAT_VERSION: u16 = 1;

/// Fixed identity prefix: tenant + database + keyspace, u32 BE each.
pub const IDENTITY_PREFIX_LEN: usize = 12;
/// Component terminator after the escaped user key.
pub const KEY_TERMINATOR: [u8; 2] = [0x00, 0x01];
/// Escape sequence emitted for a literal 0x00 inside the user key.
pub const KEY_ESCAPE: [u8; 2] = [0x00, 0xFF];
/// Version suffix: inverted timestamp (8) + value kind (1).
pub const VERSION_SUFFIX_LEN: usize = 9;

/// Worst-case encoded length for a user key of `n` bytes: every byte
/// escapes to two, plus prefix, terminator, and version suffix.
pub const fn encoded_max_len(user_key_len: usize) -> usize {
    IDENTITY_PREFIX_LEN + user_key_len * 2 + KEY_TERMINATOR.len() + VERSION_SUFFIX_LEN
}

// ── Value kind ────────────────────────────────────────────────────────

/// Discriminates what an internal key's payload *is* (§9.1). The byte
/// participates in ordering only as a stable tie-breaker after the
/// timestamp; semantic visibility rules live in the engine, not here.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ValueKind {
    /// Live user value.
    Value = 0x01,
    /// Point tombstone: the key was deleted at this timestamp.
    PointTombstone = 0x02,
    /// Start marker of a range tombstone (span metadata in payload).
    RangeTombstone = 0x03,
    /// Provisional transactional intent (§13.2).
    Intent = 0x04,
    /// Authoritative transaction record (home range, §13.2).
    TxnRecord = 0x05,
    /// Engine-internal metadata record.
    Metadata = 0x06,
}

impl ValueKind {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Value),
            0x02 => Some(Self::PointTombstone),
            0x03 => Some(Self::RangeTombstone),
            0x04 => Some(Self::Intent),
            0x05 => Some(Self::TxnRecord),
            0x06 => Some(Self::Metadata),
            _ => None,
        }
    }
}

// ── Encode ────────────────────────────────────────────────────────────

/// Encode a full internal key into `out`. Returns the encoded length,
/// or `None` if `out` is too small (fail closed — never truncate a
/// key). `mvcc_timestamp` is the logical MVCC timestamp (§10); it is
/// stored inverted so newer sorts first.
pub fn encode(
    out: &mut [u8],
    tenant: u32,
    database: u32,
    keyspace: u32,
    user_key: &[u8],
    mvcc_timestamp: u64,
    kind: ValueKind,
) -> Option<usize> {
    let mut n = encode_prefix(out, tenant, database, keyspace, user_key)?;
    if out.len() < n + VERSION_SUFFIX_LEN {
        return None;
    }
    out[n..n + 8].copy_from_slice(&(!mvcc_timestamp).to_be_bytes());
    n += 8;
    out[n] = kind as u8;
    Some(n + 1)
}

/// Encode only the identity + user-key prefix (through the terminator,
/// no version suffix). This is the *seek key* for "newest version of
/// `user_key`": every version of the key sorts ≥ this prefix, newest
/// first. Also the building block for span bounds.
pub fn encode_prefix(
    out: &mut [u8],
    tenant: u32,
    database: u32,
    keyspace: u32,
    user_key: &[u8],
) -> Option<usize> {
    if out.len() < IDENTITY_PREFIX_LEN {
        return None;
    }
    out[0..4].copy_from_slice(&tenant.to_be_bytes());
    out[4..8].copy_from_slice(&database.to_be_bytes());
    out[8..12].copy_from_slice(&keyspace.to_be_bytes());
    let mut n = IDENTITY_PREFIX_LEN;
    for &b in user_key {
        if b == 0x00 {
            if out.len() < n + 2 {
                return None;
            }
            out[n] = KEY_ESCAPE[0];
            out[n + 1] = KEY_ESCAPE[1];
            n += 2;
        } else {
            if out.len() < n + 1 {
                return None;
            }
            out[n] = b;
            n += 1;
        }
    }
    if out.len() < n + 2 {
        return None;
    }
    out[n] = KEY_TERMINATOR[0];
    out[n + 1] = KEY_TERMINATOR[1];
    Some(n + 2)
}

// ── Decode ────────────────────────────────────────────────────────────

/// A decoded internal key. `user_key_len` is the unescaped length; the
/// caller supplies the buffer the key bytes were unescaped into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedKey {
    pub tenant: u32,
    pub database: u32,
    pub keyspace: u32,
    pub user_key_len: usize,
    pub mvcc_timestamp: u64,
    pub kind: ValueKind,
}

/// Decode `src` (a complete encoded internal key), unescaping the user
/// key into `key_out`. Fails closed on any malformed input: truncated
/// components, an unknown escape byte, an unknown value kind, or a
/// user key larger than `key_out`.
pub fn decode(src: &[u8], key_out: &mut [u8]) -> Option<DecodedKey> {
    if src.len() < IDENTITY_PREFIX_LEN + KEY_TERMINATOR.len() + VERSION_SUFFIX_LEN {
        return None;
    }
    let tenant = u32::from_be_bytes(src[0..4].try_into().ok()?);
    let database = u32::from_be_bytes(src[4..8].try_into().ok()?);
    let keyspace = u32::from_be_bytes(src[8..12].try_into().ok()?);

    // Unescape the user key up to the terminator.
    let mut i = IDENTITY_PREFIX_LEN;
    let mut k = 0usize;
    let key_end;
    loop {
        if i + 1 >= src.len() {
            return None; // ran past the end without a terminator
        }
        match (src[i], src[i + 1]) {
            (0x00, 0x01) => {
                key_end = i + 2;
                break;
            }
            (0x00, 0xFF) => {
                if k >= key_out.len() {
                    return None;
                }
                key_out[k] = 0x00;
                k += 1;
                i += 2;
            }
            (0x00, _) => return None, // invalid escape
            (b, _) => {
                if k >= key_out.len() {
                    return None;
                }
                key_out[k] = b;
                k += 1;
                i += 1;
            }
        }
    }

    if src.len() != key_end + VERSION_SUFFIX_LEN {
        return None; // trailing garbage or truncated suffix
    }
    let inv_ts = u64::from_be_bytes(src[key_end..key_end + 8].try_into().ok()?);
    let kind = ValueKind::from_u8(src[key_end + 8])?;
    Some(DecodedKey {
        tenant,
        database,
        keyspace,
        user_key_len: k,
        mvcc_timestamp: !inv_ts,
        kind,
    })
}
