//! Durable watch cursors: where a subscriber of the store's change feed
//! (`store_provider.rs`, the `storage.namespace` contract's `CHANGES`)
//! had safely reached, kept in the store itself so a restart of the
//! subscriber, of the provider, or of both resumes exactly after it —
//! or says, explicitly, that it cannot.
//!
//! A cursor is one record at a reserved key, written by compare-and-swap
//! on its `mod_revision` witness exactly as the CDC pump's checkpoint
//! pointer is (`cdc_feed.rs`: `\x00cdc/ckpt/…`), so two holders of one
//! owner key cannot both advance it unseen. The record:
//!
//! ```text
//! magic:[u8;4] = "LWC1"
//! version:u8 = 1
//! shard:u16 LE
//! last_safe_rev:u64 LE      every change at or before it is held
//! subscription_generation:u32 LE
//! digest:u32 LE             FNV-1a 32 over everything before it
//! ```
//!
//! Its key, under the reserved system prefix (`\x00…`, which no
//! protocol key starts with):
//!
//! ```text
//! \x00lattice/watch-cursor/<owner>/<shard hex4>
//! ```
//!
//! `owner` is the subscriber's own name (1..=32 bytes of printable ASCII,
//! no `/`), chosen by it and stable across its restarts, and `shard` the
//! catalogue shard the subscription covers, so one subscriber holds one
//! cursor per shard it watches. Hex, not raw bytes, so the key is a
//! store key like any other (valid UTF-8, listable).
//!
//! [`resume`] is the decision a provider makes when a subscriber asks to
//! go on from its cursor: answer from the change log or the store's
//! history when either covers the window, else an explicit resync — never
//! a silent gap. Never LOST either: LOST answers a window the subscriber
//! named itself that compaction took, not a resume from its cursor.
//!
//! Pure: no allocation, no I/O, no clock. Mounted beside
//! `store_provider.rs` by the modules that build it (`kv_state_worker`,
//! `kv_state_worker_store`), which reaches it as `super::watch_cursor`, and
//! by the host suite (`tests/harness/tests/contract_watch_cursor.rs`).

#![allow(
    dead_code,
    reason = "shared via #[path] into several consumers; each uses a subset of the surface"
)]

/// The record's magic.
pub const MAGIC: [u8; 4] = *b"LWC1";
/// The record's version. A decoder refuses any other.
pub const VERSION: u8 = 1;
/// The record's length: fixed, with no trailing bytes.
pub const LEN: usize = 4 + 1 + 2 + 8 + 4 + 4;
/// The reserved key's head.
pub const KEY_HEAD: &[u8] = b"\x00lattice/watch-cursor/";
/// The longest owner name a cursor key carries.
pub const OWNER_MAX: usize = 32;
/// The longest cursor key: the head, an owner, `/`, four hex digits.
pub const KEY_MAX: usize = KEY_HEAD.len() + OWNER_MAX + 1 + 4;
const _: () = assert!(KEY_HEAD.len() == 22);
const _: () = assert!(KEY_MAX == 59);

/// The history floor of a store that keeps no history at all (the memory
/// store): no window from before the change log's base is answerable.
pub const NO_HISTORY: u64 = u64::MAX;

/// A durable cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct WatchCursor {
    pub shard: u16,
    /// Every change at or before it is held by the subscriber.
    pub last_safe_rev: u64,
    /// Bumped each time the subscription is made anew by its owner (a
    /// restart), so a stale holder's advance is visible as such.
    pub subscription_generation: u32,
}

/// Why a cursor record was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorError {
    /// Not [`LEN`] bytes.
    Length,
    /// Not [`MAGIC`].
    Magic,
    /// A version this decoder does not read.
    Version(u8),
    /// The digest does not cover the bytes.
    Digest,
}

/// Why a resume cannot be answered from the cursor: the subscriber must
/// take a fresh snapshot (a relist) and go on from its revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResyncReason {
    /// No cursor is held under the owner key: a first subscription, or
    /// one whose record was removed.
    NoCursor,
    /// The store's history no longer reaches back to the cursor:
    /// `floor` is the earliest revision it answers from.
    HistoryCompacted { cursor: u64, floor: u64 },
    /// The cursor is past the store's revision: the store was restored
    /// from before the cursor was written.
    AheadOfStore { cursor: u64, store: u64 },
    /// The cursor record is not one this code reads (a torn or foreign
    /// write).
    Corrupt(CursorError),
    /// Another holder advanced the cursor since this subscriber read it:
    /// the subscriber is deposed and must take a fresh view.
    Conflict,
}

/// The decision for a subscriber going on from its cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resume {
    /// Answer `CHANGES` from this revision on (strictly after it), as a
    /// bare-revision token would: from the change log, or across a gap
    /// from the store's history.
    FromRevision(u64),
    /// Say so, with why.
    Resync(ResyncReason),
}

/// The wire codes a provider answers a resync with (the page's reason
/// byte), one per [`ResyncReason`].
pub const REASON_NO_CURSOR: u8 = 1;
pub const REASON_HISTORY_COMPACTED: u8 = 2;
pub const REASON_AHEAD_OF_STORE: u8 = 3;
pub const REASON_CORRUPT: u8 = 4;
pub const REASON_CONFLICT: u8 = 5;
/// The subscription the cursor token named is not durable.
pub const REASON_NOT_DURABLE: u8 = 6;

impl ResyncReason {
    /// The reason's wire code.
    pub fn code(self) -> u8 {
        match self {
            ResyncReason::NoCursor => REASON_NO_CURSOR,
            ResyncReason::HistoryCompacted { .. } => REASON_HISTORY_COMPACTED,
            ResyncReason::AheadOfStore { .. } => REASON_AHEAD_OF_STORE,
            ResyncReason::Corrupt(_) => REASON_CORRUPT,
            ResyncReason::Conflict => REASON_CONFLICT,
        }
    }
}

/// FNV-1a 32 (the repo's checksum discipline; byte-identical to every
/// other `fnv1a` in `modules/common`, duplicated per core by design).
pub fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811C_9DC5;
    for &b in bytes {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

impl WatchCursor {
    /// Encode into `out`: [`LEN`] bytes, or `None` when `out` is shorter.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let o = out.get_mut(..LEN)?;
        o[0..4].copy_from_slice(&MAGIC);
        o[4] = VERSION;
        o[5..7].copy_from_slice(&self.shard.to_le_bytes());
        o[7..15].copy_from_slice(&self.last_safe_rev.to_le_bytes());
        o[15..19].copy_from_slice(&self.subscription_generation.to_le_bytes());
        let d = fnv1a32(&o[..19]);
        o[19..23].copy_from_slice(&d.to_le_bytes());
        Some(LEN)
    }

    /// Decode a record: exactly [`LEN`] bytes, the magic, a version this
    /// code reads, and a digest that covers the rest.
    pub fn decode(b: &[u8]) -> Result<Self, CursorError> {
        if b.len() != LEN {
            return Err(CursorError::Length);
        }
        if b[0..4] != MAGIC {
            return Err(CursorError::Magic);
        }
        if b[4] != VERSION {
            return Err(CursorError::Version(b[4]));
        }
        let want = u32::from_le_bytes([b[19], b[20], b[21], b[22]]);
        if fnv1a32(&b[..19]) != want {
            return Err(CursorError::Digest);
        }
        Ok(WatchCursor {
            shard: u16::from_le_bytes([b[5], b[6]]),
            last_safe_rev: u64::from_le_bytes([
                b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14],
            ]),
            subscription_generation: u32::from_le_bytes([b[15], b[16], b[17], b[18]]),
        })
    }
}

/// Whether `owner` may name a cursor: 1..=[`OWNER_MAX`] bytes of printable
/// ASCII (`0x21..=0x7E`) with no `/` — a key the store lists and a name
/// an operator can read, with no decoder needed in a PIC build.
pub fn owner_ok(owner: &[u8]) -> bool {
    !owner.is_empty()
        && owner.len() <= OWNER_MAX
        && owner
            .iter()
            .all(|&c| (0x21..=0x7E).contains(&c) && c != b'/')
}

/// The cursor key for `owner` and `shard` into `out`: its length, or
/// `None` for an owner [`owner_ok`] refuses or an `out` too short.
pub fn key(owner: &[u8], shard: u16, out: &mut [u8]) -> Option<usize> {
    if !owner_ok(owner) {
        return None;
    }
    let n = KEY_HEAD.len() + owner.len() + 1 + 4;
    let o = out.get_mut(..n)?;
    o[..KEY_HEAD.len()].copy_from_slice(KEY_HEAD);
    let mut p = KEY_HEAD.len();
    o[p..p + owner.len()].copy_from_slice(owner);
    p += owner.len();
    o[p] = b'/';
    p += 1;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for i in 0..4 {
        let nib = (shard >> (12 - 4 * i)) & 0xF;
        o[p + i] = HEX[nib as usize];
    }
    Some(n)
}

/// The decision for a subscriber going on from `cursor`.
///
/// `base` is the change log's base (the earliest revision it answers a
/// window from); `floor` the earliest revision the store's history
/// answers from ([`NO_HISTORY`] for a store that keeps none; 0 for one
/// that keeps all it has); `head` the store's revision.
///
/// A window strictly after `cursor` is answerable when the log holds it
/// (`cursor >= base`) or the history reaches it (`cursor >= floor`: a
/// history walk covers `(cursor, head]` and must stand at the cursor to
/// know what the first wanted revision changed). A cursor past `head` is
/// a store restored from before it; neither covered is a compacted
/// history. Both are a resync with that reason — the provider then
/// answers `RESYNC_REQUIRED`, never a page with a hole in it.
pub fn resume(cursor: Option<&WatchCursor>, base: u64, floor: u64, head: u64) -> Resume {
    let Some(c) = cursor else {
        return Resume::Resync(ResyncReason::NoCursor);
    };
    let r = c.last_safe_rev;
    if r > head {
        return Resume::Resync(ResyncReason::AheadOfStore {
            cursor: r,
            store: head,
        });
    }
    if r >= base {
        return Resume::FromRevision(r);
    }
    if floor != NO_HISTORY && r >= floor {
        return Resume::FromRevision(r);
    }
    Resume::Resync(ResyncReason::HistoryCompacted { cursor: r, floor })
}
