//! Pure-logic KV state machine — the worker brain, extracted so it
//! compiles into both the no_std PIC module (`modules/app/kv_state_worker/`)
//! and the host-side integration tests (`tests/integration_kv.rs`).
//!
//! Mirrors `clustor/modules/common/replica_facade.rs` in spirit: no
//! syscalls, no `*const SyscallTable`, no channel I/O — just typed
//! body parsing, the deterministic state machine, and typed result
//! body generation.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC module and host tests; each consumer uses a subset"
)]

#[path = "kv_chunks.rs"]
pub mod kv_chunks;

#[path = "types.rs"]
mod types;

#[path = "disk_store.rs"]
pub mod disk_store;

#[path = "db_context.rs"]
pub mod db_context;

use disk_store::internal_key::{self, ValueKind};
#[path = "txn.rs"]
mod txn;

use disk_store::run_storage::RunStorage;
use disk_store::state_store::{KvStateStore, Progress, StoreError};
use disk_store::{DiskStore, BATCH_FORMAT_VERSION, BATCH_HDR_LEN, BATCH_MAGIC, RECORD_FIXED};
use types::KV_OP_CHUNK_SWEEP;
use types::{
    DELETE_META_FLAG_MANIFESTS, KV_OP_GET_CHUNK, KV_OP_PUT_CHUNK, KV_OP_PUT_MANIFEST,
    KV_RESULT_BAD_CHUNK, KV_RESULT_MANIFEST, KV_RESULT_MISSING_CHUNK, RANGE_META_FLAG_MANIFESTS,
    VERSIONS_FLAG_MANIFESTS,
};
use types::{KVS_MORE_LIMIT, KVS_MORE_PARTIAL};
use types::{
    KV_ARRAY_ELEMENT_NULL, KV_OP_APPEND, KV_OP_CAS, KV_OP_DECR, KV_OP_DELETE, KV_OP_DELETE_META,
    KV_OP_EXISTS, KV_OP_FLUSH, KV_OP_GET, KV_OP_GET_AT, KV_OP_IDEMPOTENT, KV_OP_IDEMPOTENT_LOOKUP,
    KV_OP_INCR, KV_OP_MGET, KV_OP_MSET, KV_OP_PREPEND, KV_OP_PUT, KV_OP_RANGE, KV_OP_RANGE_META,
    KV_OP_RANGE_SCAN, KV_OP_SCAN, KV_OP_SCAN_AT, KV_OP_SCAN_VERSIONS, KV_OP_SNAPSHOT_VERSIONS,
    KV_OP_STRLEN, KV_OP_TXN, KV_OP_TXN_PREPARE, KV_OP_TXN_RECORD, KV_OP_TXN_RESOLVE,
    KV_RESULT_ARRAY, KV_RESULT_CAS_FAILED, KV_RESULT_COMPACTED, KV_RESULT_FUTURE_REV,
    KV_RESULT_IDEMPOTENT_LOOKUP, KV_RESULT_INTEGER, KV_RESULT_INTERNAL, KV_RESULT_KVS,
    KV_RESULT_NOT_FOUND, KV_RESULT_OK, KV_RESULT_PAUSED, KV_RESULT_QUOTA, KV_RESULT_RANGE,
    KV_RESULT_SCAN_CURSOR, KV_RESULT_TXN, KV_RESULT_TXN_PENDING, KV_RESULT_VERSIONS,
    KV_RESULT_WRONG_TYPE, PUT_FLAG2_MANIFESTS, PUT_FLAG_GET, PUT_FLAG_IGNORE_LEASE,
    PUT_FLAG_IGNORE_VALUE, PUT_FLAG_KEEPTTL, PUT_FLAG_LEASE, PUT_FLAG_NX, PUT_FLAG_PREV_META,
    PUT_FLAG_XX, TXN_CMP_MOD_EQUAL, TXN_CMP_MOD_GREATER, TXN_CMP_MOD_LESS, TXN_CMP_MOD_NOT_EQUAL,
    VERSIONS_FLAG_META, VERSION_KIND_DELETE, VERSION_KIND_PUT,
};
use types::{
    KV_OP_COMPACT, KV_OP_ETCD_TXN, KV_OP_HASH_KV, KV_OP_LEASE_KEYS, KV_OP_LEASE_REVOKE,
    KV_RESULT_DUPLICATE, KV_RESULT_ETCD_TXN,
};

// ── Capacities ────────────────────────────────────────────────────────

/// State-machine snapshot body magic ("LKVS") + format version. Bump
/// the version on any layout change; `snapshot_decode` refuses bodies
/// it doesn't recognise rather than mis-parsing them.
pub const SNAPSHOT_MAGIC: u32 = 0x4C4B_5653;
/// Format 2 adds each record's flags; format 1 bodies still decode (no
/// record in them is flagged).
pub const SNAPSHOT_FORMAT_VERSION: u16 = 2;
/// `[magic:4][format:2][flags:2][revision:8][count:4][rsvd:4][clock:8]`
///
/// `clock` is the committed clock frontier — the state machine's `now`
/// at the position this snapshot was taken. It belongs in the snapshot
/// for the same reason the revision counter does: a restore that put
/// the records back but restarted the clock at zero would make every
/// record whose deadline had already passed VISIBLE AGAIN, and would
/// then re-age the survivors from zero.
pub const SNAPSHOT_HDR_LEN: usize = 32;
/// Body-header flag: this body is one batch of a span copy and another
/// batch of the same copy follows. The installer merges each batch into the
/// store the copy's RESET emptied; the copy is whole at the first body
/// without the flag.
pub const SNAPSHOT_FLAG_SPAN_MORE: u16 = 0x0001;
/// Per-record fixed prefix ahead of the key/value bytes:
/// `[key_len:2][value_len:4][create_rev:8][mod_rev:8][version:8][lease:8][expiry:8][flags:1]`
pub const SNAPSHOT_REC_FIXED: usize = 47;
/// Format 1's prefix: the same without `flags`.
const SNAPSHOT_REC_FIXED_V1: usize = 46;

pub const MAX_KEYS: usize = 1024;
/// Longest physical key: the 12-byte identity prefix, a keyspace, and the
/// longest key a model writes — a document index entry at the document
/// id bound (`IDENT_LEN + 4 + models::DOCUMENT_KEY_MAX_LEN`; a
/// contract test holds the two together). A buffer capacity: what one
/// replicated write carries is the consensus entry's bound.
pub const MAX_KEY_LEN: usize = 2584;
pub const MAX_VALUE_LEN: usize = 4096;

/// Bytes of watch events one command may journal.
pub const JOURNAL_BYTES: usize = 16384;
use types::{JOURNAL_PREV, JOURNAL_PREV_MANIFEST};

// ── Event journal ─────────────────────────────────────────────────────

/// The watch events one command produced, in the order it produced
/// them, as the store saw them: each key's metadata after the write and
/// the version it replaced (etcd's `prev_kv`). The hosting worker frames
/// `MSG_WATCH_EVENT`s from it. A provider journals only while its
/// [`Materializer::journal`] answers one.
///
/// Entry: `[op:1][klen:2][key][vlen:2][value][create:8][version:8]
/// [lease:8][prev:1]`, then when `prev` is not 0
/// `[pvlen:2][pvalue][pcreate:8][pmod:8][pversion:8][please:8]` —
/// [`JOURNAL_PREV_MANIFEST`] when the replaced value is a large value's
/// manifest.
/// `op` is `KV_OP_PUT` or `KV_OP_DELETE`; a `create` of 0 on a put means
/// the write created the key (its create revision is the event's).
#[repr(C)]
pub struct EventJournal {
    pub buf: [u8; JOURNAL_BYTES],
    pub len: usize,
    /// An event did not fit: the journal is incomplete for this command.
    pub overflow: bool,
}

impl EventJournal {
    pub fn clear(&mut self) {
        self.len = 0;
        self.overflow = false;
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one journal entry: every argument is one of its fields"
    )]
    fn push(
        &mut self,
        op: u8,
        key: &[u8],
        value: &[u8],
        create: u64,
        version: u64,
        lease: u64,
        prev: Option<(&[u8], &RecMeta)>,
    ) {
        let need =
            1 + 2 + key.len() + 2 + value.len() + 25 + prev.map_or(0, |(v, _)| 2 + v.len() + 32);
        let at = self.len;
        if self.overflow
            || key.len() > u16::MAX as usize
            || value.len() > u16::MAX as usize
            || at + need > JOURNAL_BYTES
        {
            self.overflow = true;
            return;
        }
        let b = &mut self.buf[at..at + need];
        let mut p = 0usize;
        let mut put = |bytes: &[u8]| {
            b[p..p + bytes.len()].copy_from_slice(bytes);
            p += bytes.len();
        };
        put(&[op]);
        put(&(key.len() as u16).to_le_bytes());
        put(key);
        put(&(value.len() as u16).to_le_bytes());
        put(value);
        put(&create.to_le_bytes());
        put(&version.to_le_bytes());
        put(&lease.to_le_bytes());
        match prev {
            Some((v, m)) => {
                put(&[if m.flags & RECORD_MANIFEST != 0 {
                    JOURNAL_PREV_MANIFEST
                } else {
                    JOURNAL_PREV
                }]);
                put(&(v.len() as u16).to_le_bytes());
                put(v);
                put(&m.create_revision.to_le_bytes());
                put(&m.mod_revision.to_le_bytes());
                put(&m.version.to_le_bytes());
                put(&m.lease_id.to_le_bytes());
            }
            None => put(&[0]),
        }
        self.len = at + need;
    }
}

// ── Key log ───────────────────────────────────────────────────────────

/// Bytes of keys one command may stage in its [`KeyLog`].
pub const KEYLOG_BYTES: usize = 32 * 1024;
/// A key-log entry's bytes besides its key: `[rev u64][hint u8][kl u8]`.
pub const KEYLOG_ENTRY_FIXED: usize = 10;
/// A key-log entry's hint: what the write did to the key, as far as the
/// store saw (`storage.namespace`'s change kinds).
pub const KEY_CREATED: u8 = 0;
pub const KEY_MODIFIED: u8 = 1;
pub const KEY_DELETED: u8 = 2;

/// The keys one command changed, key-only, staged while it applies and
/// handed whole to the store provider's change log once the command
/// stands (or dropped with it when it is undone). Unlike the watch journal
/// it holds no values, and it is fed by the materializer itself — every
/// create, overwrite and removal of a record in either store notes its key
/// (the store's own bookkeeping records, and a live record's expiry, lease
/// or flags rewritten in place, are not changes of its content) — so no
/// op the interpreter grows can bypass it.
///
/// A command whose keys do not fit, or that clears the keyspace whole
/// (`FLUSH`), is marked [`overflow`](Self::overflow): its key list is not
/// whole, and the provider takes it as a gap rather than a list.
///
/// Only the keyspace the provider serves is noted ([`want`](Self::want)):
/// another keyspace's writes are not its changes.
pub struct KeyLog {
    pub buf: [u8; KEYLOG_BYTES],
    pub len: usize,
    /// A key did not fit, or the keyspace was cleared whole: the list is
    /// not this command's every change.
    pub overflow: bool,
    /// Whether keys are noted at all (a store provider is served here).
    pub on: bool,
    /// The keyspace noted: `(tenant, database, keyspace)`.
    pub want: (u32, u32, u32),
}

impl KeyLog {
    pub fn clear(&mut self) {
        self.len = 0;
        self.overflow = false;
    }

    /// Note that a write at `rev` did `hint` to `key` in `identity`.
    pub fn note(&mut self, identity: (u32, u32, u32), rev: u64, hint: u8, key: &[u8]) {
        if !self.on || identity != self.want || self.overflow {
            return;
        }
        let at = self.len;
        let end = at + KEYLOG_ENTRY_FIXED + key.len();
        if key.len() > u8::MAX as usize || end > KEYLOG_BYTES {
            self.overflow = true;
            return;
        }
        self.buf[at..at + 8].copy_from_slice(&rev.to_le_bytes());
        self.buf[at + 8] = hint;
        self.buf[at + 9] = key.len() as u8;
        self.buf[at + KEYLOG_ENTRY_FIXED..end].copy_from_slice(key);
        self.len = end;
    }

    /// Note that `identity`'s keyspace was changed whole (`FLUSH`).
    pub fn whole(&mut self, identity: (u32, u32, u32)) {
        if self.on && identity == self.want {
            self.overflow = true;
        }
    }

    /// The staged entries, in the order they were noted: `(rev, hint, key)`.
    pub fn entries(&self) -> KeyLogEntries<'_> {
        KeyLogEntries {
            buf: &self.buf[..self.len],
            at: 0,
        }
    }
}

/// [`KeyLog::entries`].
pub struct KeyLogEntries<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Iterator for KeyLogEntries<'a> {
    type Item = (u64, u8, &'a [u8]);

    fn next(&mut self) -> Option<(u64, u8, &'a [u8])> {
        let at = self.at;
        let head = self.buf.get(at..at + KEYLOG_ENTRY_FIXED)?;
        let rev = u64::from_le_bytes([
            head[0], head[1], head[2], head[3], head[4], head[5], head[6], head[7],
        ]);
        let kl = head[9] as usize;
        let key = self
            .buf
            .get(at + KEYLOG_ENTRY_FIXED..at + KEYLOG_ENTRY_FIXED + kl)?;
        self.at = at + KEYLOG_ENTRY_FIXED + kl;
        Some((rev, head[8], key))
    }
}

/// Remove `key`, journaling the version it deletes when the provider
/// journals.
fn remove_noted<M: Materializer>(store: &mut M, key: &[u8], now_ms: u64) -> Option<bool> {
    let mut prev = [0u8; MAX_VALUE_LEN];
    let found = match store.get_live(key, now_ms, &mut prev) {
        GetOutcome::Found { value_len, meta } => Some((value_len, meta)),
        GetOutcome::Absent => None,
        GetOutcome::TooBig | GetOutcome::Fault => return None,
    };
    let r = store.remove(key, now_ms);
    if let (Some(true), Some((n, m))) = (r, found) {
        // A deleted manifest releases its value's chunks.
        if let Some(root) = manifest_root(&prev[..n], &m) {
            if !release_root(store, &root, now_ms) {
                return None;
            }
        }
        if let Some(j) = store.journal() {
            j.push(KV_OP_DELETE, key, &[], 0, 0, 0, Some((&prev[..n], &m)));
        }
    }
    r
}

// ── Record ────────────────────────────────────────────────────────────

/// Record flag: the value is a large value's manifest
/// (`kv_chunks::Manifest`), written only by `KV_OP_PUT_MANIFEST` — never
/// inferred from the bytes.
pub const RECORD_MANIFEST: u8 = types::RECORD_FLAG_MANIFEST;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct KvRecord {
    pub used: bool,
    /// [`RECORD_MANIFEST`]: the value is a large value's manifest.
    pub flags: u8,
    pub _pad: [u8; 2],
    pub key_len: u16,
    pub value_len: u32,
    pub create_revision: u64,
    pub mod_revision: u64,
    pub version: u64,
    pub lease_id: u64,
    pub expiry_ms: u64,
    /// MVCC commit timestamp of the write that last touched this
    /// record; `0` = no established timestamp authority.
    pub commit_ts: u64,
    pub key: [u8; MAX_KEY_LEN],
    pub value: [u8; MAX_VALUE_LEN],
}

impl KvRecord {
    pub const fn empty() -> Self {
        Self {
            used: false,
            flags: 0,
            _pad: [0; 2],
            key_len: 0,
            value_len: 0,
            create_revision: 0,
            mod_revision: 0,
            version: 0,
            lease_id: 0,
            expiry_ms: 0,
            commit_ts: 0,
            key: [0; MAX_KEY_LEN],
            value: [0; MAX_VALUE_LEN],
        }
    }

    pub fn key_bytes(&self) -> &[u8] {
        &self.key[..self.key_len as usize]
    }

    pub fn value_bytes(&self) -> &[u8] {
        &self.value[..self.value_len as usize]
    }
}

// ── FNV-1a 64-bit hash ────────────────────────────────────────────────

pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

// ── Canonical key identity ────────────────────────────────────────────
//
// Tenant identity is part of the canonical key: the engine carries a
// `(tenant, database, keyspace)` triple set per command by the worker
// (`Materializer::set_key_identity`); both providers fold it into the
// physical key so two tenants sharing a user key never collide. The
// wire and disk encodings agree byte-for-byte with
// `internal_key`'s 12-byte prefix `[tenant:4 BE][database:4 BE]
// [keyspace:4 BE]`, so the disk provider simply passes the triple to
// `internal_key::encode`, and the memory provider prepends the same 12
// bytes to its stored key. The default `(0, 0, 0)` is the single-tenant
// layout: a store serving one tenant keys everything under it.

/// Width of the canonical identity prefix — `internal_key`'s.
pub const IDENT_LEN: usize = internal_key::IDENTITY_PREFIX_LEN;

/// The 12 canonical-prefix bytes for a `(tenant, database, keyspace)`.
pub fn ident_bytes(tenant: u32, database: u32, keyspace: u32) -> [u8; IDENT_LEN] {
    let mut b = [0u8; IDENT_LEN];
    b[0..4].copy_from_slice(&tenant.to_be_bytes());
    b[4..8].copy_from_slice(&database.to_be_bytes());
    b[8..12].copy_from_slice(&keyspace.to_be_bytes());
    b
}

/// The next prefix above `prefix` — the exclusive upper bound of one
/// tenant's key span, for scans with no user-supplied end. `None` when
/// the prefix is all-`0xFF` (no successor; treat as unbounded above,
/// which for a real identity never happens).
fn ident_successor(prefix: &[u8; IDENT_LEN]) -> Option<[u8; IDENT_LEN]> {
    let mut s = *prefix;
    let mut i = IDENT_LEN;
    while i > 0 {
        i -= 1;
        if s[i] == 0xFF {
            s[i] = 0;
        } else {
            s[i] += 1;
            return Some(s);
        }
    }
    None
}

// ── KvStore ───────────────────────────────────────────────────────────

#[repr(C)]
pub struct KvStore {
    pub revision: u64,
    pub used_count: u32,
    /// Canonical identity prefix for the command in flight, set
    /// per command by the worker. Zeroed = the single-tenant `(0, 0, 0)`
    /// layout. Not part of the snapshot: it is per-command
    /// state, re-established before every apply, never persisted.
    pub key_identity: [u8; IDENT_LEN],
    /// MVCC commit timestamp for the command in flight.
    /// Same per-command discipline as `key_identity`: set before every
    /// apply from the committed command head, never persisted.
    pub pending_commit_ts: u64,
    /// A write batch (etcd's Txn, one revision): 0 none, 1 open, 2 open
    /// and its revision taken. Per-command state, never persisted.
    pub batch: u8,
    /// Whether this command journals its watch events into `events`.
    /// Per-command state, never persisted.
    pub journaling: bool,
    pub events: EventJournal,
    /// The keys the commands in flight changed, for a store provider's
    /// change log. Never persisted.
    pub keylog: KeyLog,
    /// The revision a Compact retired history below. The memory provider
    /// holds no history, so it only answers repeat and future compacts.
    pub compact_rev: u64,
    pub records: [KvRecord; MAX_KEYS],
}

impl KvStore {
    /// Initialise an existing storage struct. For the PIC build the
    /// kernel has zeroed the state arena; for the host build we get
    /// here via `Box::<KvStore>::new_zeroed()` + `assume_init`.
    pub fn init(&mut self) {
        self.revision = 0;
        self.used_count = 0;
        self.key_identity = [0u8; IDENT_LEN];
        self.pending_commit_ts = 0;
        self.batch = 0;
        self.journaling = false;
        self.events.clear();
        self.keylog.clear();
        self.keylog.on = false;
        self.compact_rev = 0;
        let mut i = 0;
        while i < MAX_KEYS {
            self.records[i] = KvRecord::empty();
            i += 1;
        }
    }

    /// Compose the physical key for the current identity: the 12-byte
    /// prefix followed by `user_key`. `None` if it would exceed a slot
    /// (the prefix costs 12 of `MAX_KEY_LEN`). The default identity
    /// yields `[0; 12] ++ user_key`, which is what the memory provider
    /// stores for every single-tenant key.
    fn compose_key<'b>(&self, user_key: &[u8], buf: &'b mut [u8; MAX_KEY_LEN]) -> Option<&'b [u8]> {
        let total = IDENT_LEN + user_key.len();
        if total > MAX_KEY_LEN {
            return None;
        }
        buf[..IDENT_LEN].copy_from_slice(&self.key_identity);
        buf[IDENT_LEN..total].copy_from_slice(user_key);
        Some(&buf[..total])
    }

    // Linear-probed lookup. Returns `Ok(slot)` if the key is present,
    // `Err(slot)` if it isn't (slot is the first free slot to insert
    // into, or the slot at which probing wrapped without finding one).
    pub fn lookup(&self, key: &[u8]) -> Result<usize, usize> {
        let hash = fnv1a64(key);
        let mut idx = (hash as usize) % MAX_KEYS;
        let start = idx;
        let mut first_free: Option<usize> = None;
        loop {
            let slot = &self.records[idx];
            if !slot.used {
                if first_free.is_none() {
                    first_free = Some(idx);
                }
                return Err(first_free.unwrap_or(idx));
            }
            if slot.key_bytes() == key {
                return Ok(idx);
            }
            idx = (idx + 1) % MAX_KEYS;
            if idx == start {
                return Err(first_free.unwrap_or(start));
            }
        }
    }

    pub fn lookup_present(&self, key: &[u8]) -> Option<usize> {
        self.lookup(key).ok()
    }

    /// Look up a slot by USER key under the current identity — the
    /// composed-key counterpart of [`lookup_present`] for callers that
    /// hold a user key rather than the physical `[identity][user_key]`
    /// the store keys on. With the default identity this is
    /// `[0; 12] ++ user_key`.
    pub fn user_lookup_present(&self, user_key: &[u8]) -> Option<usize> {
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let ck = self.compose_key(user_key, &mut kbuf)?;
        self.lookup_present(ck)
    }

    pub fn insert_fresh(&mut self, slot_idx: usize, key: &[u8], value: &[u8]) -> bool {
        if key.len() > MAX_KEY_LEN || value.len() > MAX_VALUE_LEN {
            return false;
        }
        // Fail closed when the table is full. `lookup` returns
        // `Err(first_free)` normally, but with no free slot it wraps and
        // returns `Err(start)` — an OCCUPIED slot. Writing there would
        // silently destroy an unrelated key AND inflate `used_count`
        // past `MAX_KEYS`. Every caller already treats `false` as
        // `KV_RESULT_INTERNAL`, so a full store rejects the write
        // instead of corrupting a live record.
        //
        // Without this check, writing 2500 distinct keys into the
        // 1024-slot table leaves earlier keys silently absent while
        // their SETs are all acked OK — observed live on the rig.
        if self.records[slot_idx].used {
            return false;
        }
        let rev = self.advance_revision();
        let slot = &mut self.records[slot_idx];
        slot.used = true;
        slot.key_len = key.len() as u16;
        slot.value_len = value.len() as u32;
        slot.create_revision = rev;
        slot.mod_revision = rev;
        slot.version = 1;
        slot.lease_id = 0;
        slot.expiry_ms = 0;
        slot.flags = 0;
        slot.commit_ts = self.pending_commit_ts;
        slot.key[..key.len()].copy_from_slice(key);
        slot.value[..value.len()].copy_from_slice(value);
        self.used_count += 1;
        true
    }

    /// The revision a write takes: a new one, or — inside a batch whose
    /// revision is already taken — the batch's.
    pub fn advance_revision(&mut self) -> u64 {
        if self.batch != 2 {
            self.revision = self.revision.wrapping_add(1);
            if self.batch == 1 {
                self.batch = 2;
            }
        }
        self.revision
    }

    pub fn update_value(&mut self, slot_idx: usize, value: &[u8]) -> bool {
        if value.len() > MAX_VALUE_LEN {
            return false;
        }
        let rev = self.advance_revision();
        let ts = self.pending_commit_ts;
        let slot = &mut self.records[slot_idx];
        slot.value_len = value.len() as u32;
        slot.value[..value.len()].copy_from_slice(value);
        slot.mod_revision = rev;
        slot.version = slot.version.saturating_add(1);
        slot.commit_ts = ts;
        // A write states its value afresh: what it is (a manifest) is
        // set by the op that wrote one, after it.
        slot.flags = 0;
        true
    }

    /// Delete the record at `slot_idx` and compact any probe-chain
    /// records that may have skipped over it.
    pub fn delete_at(&mut self, slot_idx: usize) -> bool {
        if !self.records[slot_idx].used {
            return false;
        }
        self.records[slot_idx] = KvRecord::empty();
        self.used_count = self.used_count.saturating_sub(1);

        // Re-insert any keys that were probed past this slot.
        let mut j = (slot_idx + 1) % MAX_KEYS;
        while self.records[j].used {
            let stash = self.records[j];
            self.records[j] = KvRecord::empty();
            let key_len = stash.key_len as usize;
            let hash = fnv1a64(&stash.key[..key_len]);
            let mut k = (hash as usize) % MAX_KEYS;
            while self.records[k].used {
                k = (k + 1) % MAX_KEYS;
            }
            self.records[k] = stash;
            j = (j + 1) % MAX_KEYS;
        }
        true
    }

    /// Release the record the expiry queue named, if the committed
    /// clock has already put it past its deadline. Returns how many
    /// slots were freed (0 or 1 in the ordinary case; more only if two
    /// live keys collide on the same hash).
    ///
    /// `key_hash` is `fnv1a64(key)` — the same value the slot lookup
    /// uses — so this walks the probe chain the record must be on
    /// rather than scanning an unrelated slice of the table. That is
    /// what makes a single expiry event actually reclaim the single
    /// record it was raised for.
    ///
    /// Reclamation only, on the same terms as [`Self::reap_expired`]:
    /// the record is already absent from every read on every replica,
    /// so freeing it allocates no revision and emits no event.
    pub fn reap_expired_hash(&mut self, now_ms: u64, key_hash: u64) -> u32 {
        let mut released = 0u32;
        let mut idx = (key_hash as usize) % MAX_KEYS;
        let mut visited = 0usize;
        while visited < MAX_KEYS {
            let r = &self.records[idx];
            if !r.used {
                // The probe chain ends at the first free slot: no key
                // hashing here can live beyond it.
                break;
            }
            let klen = r.key_len as usize;
            if fnv1a64(&r.key[..klen]) == key_hash && is_expired(r, now_ms) {
                if self.delete_at(idx) {
                    released += 1;
                }
                // `delete_at` rehashes the chain behind this slot, so
                // re-examine `idx` rather than stepping over whatever
                // moved into it.
            } else {
                idx = (idx + 1) % MAX_KEYS;
            }
            visited += 1;
        }
        released
    }

    /// Release slots whose absolute deadline has already passed under
    /// `now_ms`, visiting at most `budget` slots from `cursor`. Returns
    /// `(next_cursor, released)`.
    ///
    /// Reclamation only. A record past its deadline is already absent
    /// from every read on every replica, so releasing it allocates no
    /// revision, writes no tombstone, and emits no mutation event —
    /// which is what allows the sweep to run at a different moment on
    /// each replica without the observable states diverging.
    pub fn reap_expired(&mut self, now_ms: u64, cursor: usize, budget: usize) -> (usize, u32) {
        let mut idx = cursor % MAX_KEYS;
        let mut released = 0u32;
        let mut visited = 0usize;
        while visited < budget && visited < MAX_KEYS {
            if self.records[idx].used && is_expired(&self.records[idx], now_ms) {
                if self.delete_at(idx) {
                    released += 1;
                }
                // `delete_at` rehashes the probe chain behind this slot,
                // so the slot is re-examined on the next pass rather
                // than advancing over a record that moved into it.
            } else {
                idx = (idx + 1) % MAX_KEYS;
            }
            visited += 1;
        }
        (idx, released)
    }

    pub fn flush(&mut self) {
        let mut i = 0;
        while i < MAX_KEYS {
            if self.records[i].used {
                self.records[i] = KvRecord::empty();
            }
            i += 1;
        }
        self.used_count = 0;
        self.revision = self.revision.wrapping_add(1);
    }

    /// Serialise the live state into `out` as a state-machine snapshot
    /// body. Only USED records are written, so the encoded
    /// size tracks live data rather than the fixed
    /// `MAX_KEYS * sizeof(KvRecord)` arena.
    ///
    /// Returns `Some(len)` on success, or **`None` if the state does not
    /// fit** in `out`. Callers MUST treat `None` as "no snapshot this
    /// round" and leave the WAL authoritative — never persist a truncated
    /// body. Compaction is gated on a durable snapshot, so failing closed
    /// here simply means the log keeps growing, which is safe; emitting a
    /// short body would silently lose keys on restore.
    ///
    /// Layout (little-endian):
    /// ```text
    /// header  [magic:u32][format:u16][flags:u16][revision:u64][count:u32]
    ///         [rsvd:u32][clock_ms:u64]
    /// record  [key_len:u16][value_len:u32][create_rev:u64][mod_rev:u64]
    ///         [version:u64][lease_id:u64][expiry_ms:u64][key][value]
    /// ```
    ///
    /// `clock_ms` is the caller's committed clock frontier. Record
    /// deadlines are absolute against that clock, so a body carrying
    /// the records without it is not a restorable state — see
    /// [`snapshot_clock_ms`].
    pub fn snapshot_encode(&self, out: &mut [u8], clock_ms: u64) -> Option<usize> {
        if out.len() < SNAPSHOT_HDR_LEN {
            return None;
        }
        let mut p = SNAPSHOT_HDR_LEN;
        let mut count: u32 = 0;

        let mut i = 0;
        while i < MAX_KEYS {
            let r = &self.records[i];
            if r.used {
                let klen = r.key_len as usize;
                let vlen = r.value_len as usize;
                // Defensive: a record whose lengths exceed the arena is
                // corrupt; refuse the whole snapshot rather than emit it.
                if klen > MAX_KEY_LEN || vlen > MAX_VALUE_LEN {
                    return None;
                }
                let need = SNAPSHOT_REC_FIXED + klen + vlen;
                if p + need > out.len() {
                    return None; // fail closed — see doc comment
                }
                out[p..p + 2].copy_from_slice(&r.key_len.to_le_bytes());
                out[p + 2..p + 6].copy_from_slice(&r.value_len.to_le_bytes());
                out[p + 6..p + 14].copy_from_slice(&r.create_revision.to_le_bytes());
                out[p + 14..p + 22].copy_from_slice(&r.mod_revision.to_le_bytes());
                out[p + 22..p + 30].copy_from_slice(&r.version.to_le_bytes());
                out[p + 30..p + 38].copy_from_slice(&r.lease_id.to_le_bytes());
                out[p + 38..p + 46].copy_from_slice(&r.expiry_ms.to_le_bytes());
                out[p + 46] = r.flags;
                p += SNAPSHOT_REC_FIXED;
                out[p..p + klen].copy_from_slice(&r.key[..klen]);
                p += klen;
                out[p..p + vlen].copy_from_slice(&r.value[..vlen]);
                p += vlen;
                count += 1;
            }
            i += 1;
        }

        out[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
        out[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&0u16.to_le_bytes()); // flags
        out[8..16].copy_from_slice(&self.revision.to_le_bytes());
        out[16..20].copy_from_slice(&count.to_le_bytes());
        out[20..24].copy_from_slice(&0u32.to_le_bytes()); // reserved
        out[24..32].copy_from_slice(&clock_ms.to_le_bytes());
        Some(p)
    }

    /// Encode ONLY the records whose key lies in the half-open span
    /// `[start, end)` — the elastic split's span-scoped snapshot.
    ///
    /// The elastic split ships a span's bytes to a demand-provisioned
    /// partition (possibly on another node) by streaming this body over
    /// the app-snapshot chunk path, then restoring it with
    /// [`snapshot_restore`] on the target. The wire format is byte-identical
    /// to [`snapshot_encode`] — a span snapshot is a whole-store snapshot of
    /// exactly the span — so the target installs it with no new decode path.
    ///
    /// Bounds are USER-key bytes, ordered lexicographically. An empty
    /// `start` is unbounded below, an empty `end` unbounded above (so an
    /// empty/empty span is the whole store, matching `snapshot_encode`).
    /// Fails closed (returns `None`) on a corrupt record or an undersized
    /// `out`, exactly as `snapshot_encode` does — a truncated span body
    /// would silently drop keys the cutover then loses.
    pub fn snapshot_encode_span(
        &self,
        out: &mut [u8],
        clock_ms: u64,
        start: &[u8],
        end: &[u8],
    ) -> Option<usize> {
        if out.len() < SNAPSHOT_HDR_LEN {
            return None;
        }
        let mut p = SNAPSHOT_HDR_LEN;
        let mut count: u32 = 0;

        let mut i = 0;
        while i < MAX_KEYS {
            let r = &self.records[i];
            if r.used {
                let klen = r.key_len as usize;
                let vlen = r.value_len as usize;
                if klen > MAX_KEY_LEN || vlen > MAX_VALUE_LEN {
                    return None;
                }
                let key = &r.key[..klen];
                // Half-open [start, end): >= start, and < end unless end
                // is empty (unbounded above).
                let in_span = key >= start && (end.is_empty() || key < end);
                if in_span {
                    let need = SNAPSHOT_REC_FIXED + klen + vlen;
                    if p + need > out.len() {
                        return None; // fail closed
                    }
                    out[p..p + 2].copy_from_slice(&r.key_len.to_le_bytes());
                    out[p + 2..p + 6].copy_from_slice(&r.value_len.to_le_bytes());
                    out[p + 6..p + 14].copy_from_slice(&r.create_revision.to_le_bytes());
                    out[p + 14..p + 22].copy_from_slice(&r.mod_revision.to_le_bytes());
                    out[p + 22..p + 30].copy_from_slice(&r.version.to_le_bytes());
                    out[p + 30..p + 38].copy_from_slice(&r.lease_id.to_le_bytes());
                    out[p + 38..p + 46].copy_from_slice(&r.expiry_ms.to_le_bytes());
                    out[p + 46] = r.flags;
                    p += SNAPSHOT_REC_FIXED;
                    out[p..p + klen].copy_from_slice(&r.key[..klen]);
                    p += klen;
                    out[p..p + vlen].copy_from_slice(&r.value[..vlen]);
                    p += vlen;
                    count += 1;
                }
            }
            i += 1;
        }

        out[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
        out[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&0u16.to_le_bytes()); // flags
        out[8..16].copy_from_slice(&self.revision.to_le_bytes());
        out[16..20].copy_from_slice(&count.to_le_bytes());
        out[20..24].copy_from_slice(&0u32.to_le_bytes()); // reserved
        out[24..32].copy_from_slice(&clock_ms.to_le_bytes());
        Some(p)
    }

    /// One batch of a span copy: the records whose key lies in
    /// `[from, end)`, in key order, as many as fit `out`, in the span body
    /// format ([`Self::snapshot_encode_span`]). Returns the body length and,
    /// when records of the span remain past this batch, the length of the
    /// key the next batch starts from (an inclusive bound), written to
    /// `next`. A batch with more to follow carries
    /// [`SNAPSHOT_FLAG_SPAN_MORE`].
    ///
    /// A span larger than one body ships as successive batches, each
    /// encoded when the previous one has been taken. Key order makes the
    /// resume point a key rather than a slot, so it holds across the
    /// probe-chain compaction a delete performs between batches.
    ///
    /// Fails closed (`None`) on a corrupt record, or when `out` or `next`
    /// cannot hold the record a batch must carry.
    pub fn snapshot_encode_span_batch(
        &self,
        out: &mut [u8],
        clock_ms: u64,
        from: &[u8],
        end: &[u8],
        next: &mut [u8],
    ) -> Option<(usize, Option<usize>)> {
        if out.len() < SNAPSHOT_HDR_LEN {
            return None;
        }
        let mut order = [0u16; MAX_KEYS];
        let mut n = 0usize;
        let mut i = 0;
        while i < MAX_KEYS {
            let r = &self.records[i];
            if r.used {
                let klen = r.key_len as usize;
                if klen > MAX_KEY_LEN || r.value_len as usize > MAX_VALUE_LEN {
                    return None;
                }
                let key = &r.key[..klen];
                if key >= from && (end.is_empty() || key < end) {
                    order[n] = i as u16;
                    n += 1;
                }
            }
            i += 1;
        }
        sort_slots_by_key(&self.records, &mut order[..n]);

        let mut p = SNAPSHOT_HDR_LEN;
        let mut count: u32 = 0;
        let mut more = None;
        for &slot in &order[..n] {
            let r = &self.records[slot as usize];
            let klen = r.key_len as usize;
            let vlen = r.value_len as usize;
            let need = SNAPSHOT_REC_FIXED + klen + vlen;
            if p + need > out.len() {
                if count == 0 || klen > next.len() {
                    return None;
                }
                next[..klen].copy_from_slice(&r.key[..klen]);
                more = Some(klen);
                break;
            }
            out[p..p + 2].copy_from_slice(&r.key_len.to_le_bytes());
            out[p + 2..p + 6].copy_from_slice(&r.value_len.to_le_bytes());
            out[p + 6..p + 14].copy_from_slice(&r.create_revision.to_le_bytes());
            out[p + 14..p + 22].copy_from_slice(&r.mod_revision.to_le_bytes());
            out[p + 22..p + 30].copy_from_slice(&r.version.to_le_bytes());
            out[p + 30..p + 38].copy_from_slice(&r.lease_id.to_le_bytes());
            out[p + 38..p + 46].copy_from_slice(&r.expiry_ms.to_le_bytes());
            out[p + 46] = r.flags;
            p += SNAPSHOT_REC_FIXED;
            out[p..p + klen].copy_from_slice(&r.key[..klen]);
            p += klen;
            out[p..p + vlen].copy_from_slice(&r.value[..vlen]);
            p += vlen;
            count += 1;
        }

        let flags = if more.is_some() {
            SNAPSHOT_FLAG_SPAN_MORE
        } else {
            0
        };
        out[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
        out[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&flags.to_le_bytes());
        out[8..16].copy_from_slice(&self.revision.to_le_bytes());
        out[16..20].copy_from_slice(&count.to_le_bytes());
        out[20..24].copy_from_slice(&0u32.to_le_bytes()); // reserved
        out[24..32].copy_from_slice(&clock_ms.to_le_bytes());
        Some((p, more))
    }

    /// Merge one span-copy batch into the store: [`Self::snapshot_decode`]
    /// without the clear, the store's revision raised to the body's when
    /// the body's is higher. A key the store already holds is a malformed
    /// copy (a span's batches are disjoint): like every failed decode it
    /// leaves the store CLEARED.
    pub fn snapshot_merge(&mut self, src: &[u8]) -> bool {
        let ok = self.decode_records(src, false);
        if !ok {
            self.init();
        }
        ok
    }

    /// `(deadline_ms, key_hash)` for the record in `slot`, if it holds
    /// one with an absolute expiry deadline.
    ///
    /// Slot-addressed rather than a closure walk so the caller can
    /// rebuild the expiry queue INCREMENTALLY, a budget at a time,
    /// resuming where it left off. The queue that schedules reclamation
    /// lives in `ttl_scheduler`'s arena and is in no snapshot, so after
    /// a restore it knows about none of these records; re-registering
    /// them is what puts it back in agreement with the store.
    pub fn expiring_at(&self, slot: usize) -> Option<(u64, u64)> {
        if slot >= MAX_KEYS {
            return None;
        }
        let r = &self.records[slot];
        if !r.used || r.expiry_ms == 0 {
            return None;
        }
        Some((r.expiry_ms, fnv1a64(&r.key[..r.key_len as usize])))
    }

    /// The committed clock frontier a snapshot body was taken at, or
    /// `0` if the body is too short to carry one. Read this alongside
    /// [`Self::snapshot_decode`] and latch it as the restored state
    /// machine's `now`: the records' deadlines are absolute against it.
    pub fn snapshot_clock_ms(src: &[u8]) -> u64 {
        if src.len() < SNAPSHOT_HDR_LEN {
            return 0;
        }
        u64::from_le_bytes([
            src[24], src[25], src[26], src[27], src[28], src[29], src[30], src[31],
        ])
    }

    /// Restore state from a snapshot body produced by
    /// `snapshot_encode`. The store is cleared first, so this is a
    /// REPLACE (clustor's app-snapshot contract discards current
    /// state), not a merge.
    ///
    /// Records are re-inserted through the normal hash/probe path so the
    /// linear-probe invariants are rebuilt correctly rather than assumed
    /// from slot positions; the per-record revision metadata is then
    /// restored verbatim and `revision` is set last (plain inserts would
    /// otherwise bump it).
    ///
    /// Returns false — leaving the store CLEARED — on a malformed body
    /// (bad magic/format, truncation, over-long key/value, duplicate or
    /// unindexable key). A partial restore must never be mistaken for a
    /// good one; the caller fails closed and replays from the log.
    pub fn snapshot_decode(&mut self, src: &[u8]) -> bool {
        self.decode_records(src, true)
    }

    /// The records of a snapshot body into the store: cleared first when
    /// `replace`, the body's revision adopted then; otherwise added to what
    /// the store holds, its revision kept unless the body's is higher.
    fn decode_records(&mut self, src: &[u8], replace: bool) -> bool {
        if src.len() < SNAPSHOT_HDR_LEN {
            return false;
        }
        let magic = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
        let format = u16::from_le_bytes([src[4], src[5]]);
        if magic != SNAPSHOT_MAGIC || !(1..=SNAPSHOT_FORMAT_VERSION).contains(&format) {
            return false;
        }
        let fixed = if format == 1 {
            SNAPSHOT_REC_FIXED_V1
        } else {
            SNAPSHOT_REC_FIXED
        };
        let revision = u64::from_le_bytes([
            src[8], src[9], src[10], src[11], src[12], src[13], src[14], src[15],
        ]);
        let count = u32::from_le_bytes([src[16], src[17], src[18], src[19]]) as usize;
        if count > MAX_KEYS {
            return false;
        }

        let held_revision = self.revision;
        if replace {
            self.init();
        }

        let mut p = SNAPSHOT_HDR_LEN;
        let mut done = 0usize;
        while done < count {
            if p + fixed > src.len() {
                self.init();
                return false;
            }
            let key_len = u16::from_le_bytes([src[p], src[p + 1]]) as usize;
            let value_len =
                u32::from_le_bytes([src[p + 2], src[p + 3], src[p + 4], src[p + 5]]) as usize;
            let create_revision = u64::from_le_bytes([
                src[p + 6],
                src[p + 7],
                src[p + 8],
                src[p + 9],
                src[p + 10],
                src[p + 11],
                src[p + 12],
                src[p + 13],
            ]);
            let mod_revision = u64::from_le_bytes([
                src[p + 14],
                src[p + 15],
                src[p + 16],
                src[p + 17],
                src[p + 18],
                src[p + 19],
                src[p + 20],
                src[p + 21],
            ]);
            let version = u64::from_le_bytes([
                src[p + 22],
                src[p + 23],
                src[p + 24],
                src[p + 25],
                src[p + 26],
                src[p + 27],
                src[p + 28],
                src[p + 29],
            ]);
            let lease_id = u64::from_le_bytes([
                src[p + 30],
                src[p + 31],
                src[p + 32],
                src[p + 33],
                src[p + 34],
                src[p + 35],
                src[p + 36],
                src[p + 37],
            ]);
            let expiry_ms = u64::from_le_bytes([
                src[p + 38],
                src[p + 39],
                src[p + 40],
                src[p + 41],
                src[p + 42],
                src[p + 43],
                src[p + 44],
                src[p + 45],
            ]);
            let flags = if format == 1 { 0 } else { src[p + 46] };
            p += fixed;

            if key_len > MAX_KEY_LEN || value_len > MAX_VALUE_LEN {
                self.init();
                return false;
            }
            if p + key_len + value_len > src.len() {
                self.init();
                return false;
            }
            let key_end = p + key_len;
            let val_end = key_end + value_len;

            // Index through the normal probe path. A key already present
            // means a duplicate in the body (corrupt); `Err(slot)` with a
            // full table means unindexable. Both fail closed.
            let slot = {
                let key = &src[p..key_end];
                match self.lookup(key) {
                    Ok(_) => {
                        self.init();
                        return false;
                    }
                    Err(slot) => slot,
                }
            };
            if self.records[slot].used {
                self.init();
                return false;
            }

            {
                let (key, value) = src[p..val_end].split_at(key_len);
                if !self.insert_fresh(slot, key, value) {
                    self.init();
                    return false;
                }
            }
            // insert_fresh stamped fresh revisions; restore the originals.
            let rec = &mut self.records[slot];
            rec.create_revision = create_revision;
            rec.mod_revision = mod_revision;
            rec.version = version;
            rec.lease_id = lease_id;
            rec.expiry_ms = expiry_ms;
            rec.flags = flags;

            p = val_end;
            done += 1;
        }

        // Set last: the inserts above each bumped `revision`.
        self.revision = if replace {
            revision
        } else {
            revision.max(held_revision)
        };
        true
    }
}

/// Order `order`'s slot indices by their records' keys (heapsort: in
/// place, no allocation, `n log n` whatever the input).
fn sort_slots_by_key(records: &[KvRecord], order: &mut [u16]) {
    let n = order.len();
    let mut i = n / 2;
    while i > 0 {
        i -= 1;
        sift_slot(records, order, i, n);
    }
    let mut end = n;
    while end > 1 {
        end -= 1;
        order.swap(0, end);
        sift_slot(records, order, 0, end);
    }
}

fn sift_slot(records: &[KvRecord], order: &mut [u16], mut root: usize, end: usize) {
    let key = |s: u16| {
        let r = &records[s as usize];
        &r.key[..r.key_len as usize]
    };
    loop {
        let mut child = 2 * root + 1;
        if child >= end {
            return;
        }
        if child + 1 < end && key(order[child]) < key(order[child + 1]) {
            child += 1;
        }
        if key(order[root]) >= key(order[child]) {
            return;
        }
        order.swap(root, child);
        root = child;
    }
}

// ── Body readers ──────────────────────────────────────────────────────

fn read_key<'a>(body: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
    if body.len() < *offset + 2 {
        return None;
    }
    let klen = u16::from_le_bytes([body[*offset], body[*offset + 1]]) as usize;
    *offset += 2;
    if body.len() < *offset + klen {
        return None;
    }
    let key = &body[*offset..*offset + klen];
    *offset += klen;
    Some(key)
}

fn read_value<'a>(body: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
    if body.len() < *offset + 4 {
        return None;
    }
    let vlen = u32::from_le_bytes([
        body[*offset],
        body[*offset + 1],
        body[*offset + 2],
        body[*offset + 3],
    ]) as usize;
    *offset += 4;
    if body.len() < *offset + vlen {
        return None;
    }
    let value = &body[*offset..*offset + vlen];
    *offset += vlen;
    Some(value)
}

fn read_i64(body: &[u8], offset: &mut usize) -> Option<i64> {
    if body.len() < *offset + 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&body[*offset..*offset + 8]);
    *offset += 8;
    Some(i64::from_le_bytes(buf))
}

fn read_u64(body: &[u8], offset: &mut usize) -> Option<u64> {
    if body.len() < *offset + 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&body[*offset..*offset + 8]);
    *offset += 8;
    Some(u64::from_le_bytes(buf))
}

fn read_u16(body: &[u8], offset: &mut usize) -> Option<u16> {
    if body.len() < *offset + 2 {
        return None;
    }
    let v = u16::from_le_bytes([body[*offset], body[*offset + 1]]);
    *offset += 2;
    Some(v)
}

fn read_u8(body: &[u8], offset: &mut usize) -> Option<u8> {
    if body.len() < *offset + 1 {
        return None;
    }
    let v = body[*offset];
    *offset += 1;
    Some(v)
}

/// Recover `(key_hash, ttl_ms)` from a `KV_OP_PUT` body: the identity
/// under which the expiry queue tracks the record, and the relative TTL
/// the command carried (`0` = no expiry).
///
/// `ident` is the command's canonical identity prefix. The hash
/// covers `ident ++ key`, exactly as the stored record's key does, for
/// two reasons: two tenants writing the same user key must not
/// register, cancel and reclaim each OTHER's entries, and the hash is
/// what an expiry event hands back to the store to find the record it
/// named — a hash taken over anything but the canonical key would land
/// on the wrong probe chain.
///
/// The queue entry it feeds is a reclamation hint. The record's own
/// absolute `expiry_ms` is the authority for visibility, so a hint that
/// is late, early, or absent changes when a slot is released and
/// nothing else.
pub fn put_ttl_registration(body: &[u8], ident: &[u8; IDENT_LEN]) -> Option<(u64, u64)> {
    let mut off = 0;
    let key = read_key(body, &mut off)?;
    read_value(body, &mut off)?;
    read_u8(body, &mut off)?;
    let ttl_ms = read_u64(body, &mut off).unwrap_or(0);
    Some((canonical_key_hash(ident, key), ttl_ms))
}

/// `fnv1a64` over the canonical key — the identity prefix followed by
/// the user key — computed without materialising the concatenation.
/// This is the SAME value as `fnv1a64(stored_key)` for the record the
/// command writes, which is what lets an expiry event name a slot.
pub fn canonical_key_hash(ident: &[u8; IDENT_LEN], key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in ident.iter().chain(key.iter()) {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

// ── Decimal int parse/format for INCR/DECR ────────────────────────────

fn parse_decimal_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let (neg, rest) = if bytes[0] == b'-' {
        (true, &bytes[1..])
    } else if bytes[0] == b'+' {
        (false, &bytes[1..])
    } else {
        (false, bytes)
    };
    if rest.is_empty() {
        return None;
    }
    let mut acc: i64 = 0;
    for &b in rest {
        if !b.is_ascii_digit() {
            return None;
        }
        acc = acc.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    Some(if neg { -acc } else { acc })
}

fn format_decimal_i64(n: i64, out: &mut [u8]) -> usize {
    if n == 0 {
        out[0] = b'0';
        return 1;
    }
    let neg = n < 0;
    let mut v: u64 = if neg { (-(n as i128)) as u64 } else { n as u64 };
    let mut buf = [0u8; 32];
    let mut idx = 32;
    while v > 0 {
        idx -= 1;
        buf[idx] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    let digits = 32 - idx;
    let mut write_at = 0;
    if neg {
        out[0] = b'-';
        write_at = 1;
    }
    out[write_at..write_at + digits].copy_from_slice(&buf[idx..]);
    write_at + digits
}

// ── Materializer: the physical provider beneath the interpreter ──────
//
// ONE semantic engine (`apply_mat`
// below is the single command interpreter), with replaceable physical
// materialization providers selected at graph construction. The
// interpreter performs every materialization read/write through this
// small trait; the two implementations are the bounded in-memory hash
// table (`KvStore`) and the
// ordered disk provider (`DiskMaterializer` over `disk_store.rs`).
// The trait carries no command semantics — comparisons, flag
// evaluation, revision policy and result encoding all live in the
// interpreter, so command meaning cannot drift between providers.

/// Per-record etcd-style bookkeeping shared by both providers. On the
/// memory provider these are `KvRecord` fields; on the disk provider
/// they are persisted in the fixed `DISK_META_LEN`-byte prefix of the
/// value payload (layout below).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecMeta {
    pub create_revision: u64,
    pub mod_revision: u64,
    pub version: u64,
    pub lease_id: u64,
    pub expiry_ms: u64,
    /// MVCC commit timestamp of the write that produced this version
    ///. `0` = written without an established timestamp
    /// authority. Distinct from `mod_revision`: the revision is the
    /// per-partition apply order, the commit timestamp is the
    /// cluster-wide MVCC domain that survives topology changes.
    pub commit_ts: u64,
    /// [`RECORD_MANIFEST`] when the value is a large value's manifest.
    pub flags: u8,
}

/// Outcome of a live (expiry-filtered) point lookup.
pub enum GetOutcome {
    /// Key absent (or expired) — reads report not-found.
    Absent,
    /// Present: the value was copied into the caller's buffer.
    Found { value_len: usize, meta: RecMeta },
    /// Value larger than the caller's buffer (caller fails closed).
    TooBig,
    /// Provider fault (disk only) — caller fails closed with
    /// `KV_RESULT_INTERNAL`, never an empty result.
    Fault,
}

/// Outcome of a historical (as-of-revision) point lookup.
///
/// Deliberately a SEPARATE enum from [`GetOutcome`] rather than a new
/// variant on it: `Compacted` is reachable only on the historical path,
/// and keeping it out of `GetOutcome` means none of the fifteen latest-
/// read call sites can accidentally treat "I cannot answer" as "absent".
pub enum HistOutcome {
    /// Not present at that revision — never written yet, or the winning
    /// version at-or-below it is a tombstone.
    Absent,
    /// Present at that revision; value copied into the caller's buffer.
    Found { value_len: usize, meta: RecMeta },
    /// The provider does not retain that revision, so it CANNOT answer.
    /// Never downgrade this to `Absent` or to a latest read — that
    /// would be a wrong answer to a historical question.
    Compacted,
    /// Value larger than the caller's buffer (caller fails closed).
    TooBig,
    /// Provider fault — caller fails closed with `KV_RESULT_INTERNAL`.
    Fault,
}

/// Physical record store surface used by the interpreter. Every
/// method is bounded; mutation methods that assign revisions do so
/// with the provider's engine-revision counter so both providers
/// stamp identical `create_revision`/`mod_revision`/`version` values
/// for identical command streams.
///
/// Revision assignment (verified against the memory provider, which
/// defines the byte contract): `insert_new`/`update_existing` claim
/// ONE fresh revision per materialized write — so a single command
/// may claim SEVERAL revisions (MSET claims one per pair, TXN one
/// per mutating sub-op write) — while DELETE claims one shared
/// revision for every tombstone it writes plus the single op-end
/// bump, FLUSH one shared revision for all its tombstones, and
/// `set_expiry` claims none (it rewrites a version in place). Replay
/// idempotence on the disk provider keys off exactly this
/// per-materialized-write assignment (see `DiskMaterializer`).
pub trait Materializer {
    /// Bind the canonical identity `(tenant, database, keyspace)`
    /// for the command about to be applied. Every physical key this
    /// materializer reads or writes until the next call is scoped to it,
    /// so two tenants sharing a user key never collide. The default no-op
    /// keeps the single-tenant `(0, 0, 0)` layout for any provider that
    /// does not model identity.
    fn set_key_identity(&mut self, _tenant: u32, _database: u32, _keyspace: u32) {}
    /// The identity bound by [`set_key_identity`](Self::set_key_identity).
    fn key_identity(&self) -> (u32, u32, u32) {
        (0, 0, 0)
    }
    /// Bind the MVCC commit timestamp for the command about to be
    /// applied. Every version the materializer writes
    /// until the next call carries it. Same per-command discipline as
    /// [`set_key_identity`](Self::set_key_identity); the transaction
    /// resolve path re-binds it per staged op from the resolve record.
    /// Default no-op keeps timestamp-less providers valid.
    fn set_commit_ts(&mut self, _ts: u64) {}
    /// The journal this command's watch events go to, when the host
    /// keeps one ([`EventJournal`]). Default: none.
    fn journal(&mut self) -> Option<&mut EventJournal> {
        None
    }
    /// The key log this command's changed keys are staged in, when the host
    /// keeps one ([`KeyLog`]). Default: none.
    fn keylog(&mut self) -> Option<&mut KeyLog> {
        None
    }
    /// Note one changed key in the key log, if any: the write at `rev` did
    /// `hint` to `key` in the bound identity.
    fn note_key(&mut self, rev: u64, hint: u8, key: &[u8]) {
        let identity = self.key_identity();
        if let Some(k) = self.keylog() {
            k.note(identity, rev, hint, key);
        }
    }
    /// Note that the bound identity's keyspace is being changed whole.
    fn note_whole(&mut self) {
        let identity = self.key_identity();
        if let Some(k) = self.keylog() {
            k.whole(identity);
        }
    }
    /// Current engine revision (returned in `MSG_KV_APPLIED`).
    fn revision(&self) -> u64;
    /// Bump the engine revision without writing a record (the DELETE
    /// batch-end / FLUSH bump).
    fn bump_revision(&mut self);
    /// Open a write batch: every write until [`end_batch`](Self::end_batch)
    /// takes ONE revision (the first write's), as etcd's Txn commits.
    fn begin_batch(&mut self);
    /// Close the write batch.
    fn end_batch(&mut self);
    /// Live point lookup: newest version, expired records reported
    /// `Absent` (the memory provider also lazily reclaims the slot —
    /// a physical, non-observable difference).
    fn get_live(&mut self, key: &[u8], now_ms: u64, val_out: &mut [u8]) -> GetOutcome;
    /// Create a record that does not currently exist: fresh meta
    /// (`create = mod = new revision`, `version = 1`), bumps the
    /// engine revision. `false` = capacity/fault (caller fails closed).
    fn insert_new(&mut self, key: &[u8], value: &[u8]) -> bool;
    /// Overwrite an existing record's value: bumps the engine
    /// revision, `mod = new revision`, `version += 1`, preserves
    /// `create_revision`/`lease`/`expiry`.
    fn update_existing(&mut self, key: &[u8], value: &[u8]) -> bool;
    /// Set the absolute expiry deadline without touching revisions.
    fn set_expiry(&mut self, key: &[u8], expiry_ms: u64) -> bool;
    /// Set the lease a live record is attached to (0 = none) without
    /// touching revisions; the put that carries it has already bumped them.
    fn set_lease(&mut self, key: &[u8], lease_id: u64) -> bool;
    /// Set a live record's flags ([`RECORD_MANIFEST`]) without touching
    /// revisions; the write that carries them has already bumped them.
    fn set_flags(&mut self, key: &[u8], flags: u8) -> bool;
    /// Write one of the store's own bookkeeping records (large values'
    /// chunks and their references) as part of the op in progress: at the
    /// revision it takes (batch-aware — every such write of one op shares
    /// it). One op never writes a bookkeeping key twice with different
    /// kinds: a disk store holds one version per key and revision.
    fn put_meta(&mut self, key: &[u8], value: &[u8]) -> bool;
    /// Remove a bookkeeping record the same way.
    fn remove_meta(&mut self, key: &[u8]) -> bool;
    /// Is a write batch open ([`begin_batch`](Self::begin_batch))?
    fn batch_open(&self) -> bool;
    /// The revision the op in progress writes at: the batch's, once taken,
    /// else the next.
    fn op_revision(&self) -> u64;
    /// Remove a live record. Expiry-filtered: DELETE of an expired key
    /// reports `false` (the key is logically gone — Redis semantics —
    /// and the count must be a function of logical state, never of
    /// whether a physical reap happened to run first). Does not bump
    /// the revision; the interpreter bumps once per DELETE op that
    /// removed anything. `None` = provider fault.
    fn remove(&mut self, key: &[u8], now_ms: u64) -> Option<bool>;
    /// The SCAN/RANGE op. Cursor semantics are provider-defined
    /// (opaque to clients per the redis SCAN contract): the memory
    /// provider walks hash slots, the disk provider walks the sorted
    /// key space by visible ordinal.
    fn scan_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize);
    /// The RANGE_SCAN op: a half-open `[start, end)` walk that emits
    /// key AND value pairs. Same provider-defined cursor discipline as
    /// [`scan_op`](Self::scan_op) — complete and resumable, order not
    /// promised — and the same `(result_code, len)` shape, with a
    /// `KV_RESULT_RANGE` body.
    fn range_scan_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize);
    /// Walk the live keys of `[start, end)` (an empty `end` is unbounded
    /// above) as of `at_rev` (0 = the current revision), calling
    /// `f(user_key, value, meta)` for each until it returns `false`.
    /// The order is the provider's ([`walk_sorted`](Self::walk_sorted)
    /// says whether it is ascending key order). Returns `KV_RESULT_OK`,
    /// or `KV_RESULT_COMPACTED` when `at_rev` is below what the provider
    /// retains, or `KV_RESULT_INTERNAL` on a fault. The shared
    /// [`KV_OP_RANGE_META`] op is built on it.
    fn walk_live_meta<F: FnMut(&[u8], &[u8], &RecMeta) -> bool>(
        &mut self,
        start: &[u8],
        end: &[u8],
        at_rev: u64,
        now_ms: u64,
        f: F,
    ) -> u8;
    /// Whether [`walk_live_meta`](Self::walk_live_meta) walks in
    /// ascending key order (the disk provider's sorted runs do; the
    /// memory provider's hash slots do not).
    fn walk_sorted(&self) -> bool {
        false
    }
    /// The SCAN_VERSIONS op: every version in a span whose revision
    /// falls in `(from, to]`, tombstones included. Same
    /// `(result_code, len)` shape, with a `KV_RESULT_VERSIONS` body,
    /// or `(KV_RESULT_COMPACTED, 0)` when the provider does not retain
    /// the window.
    ///
    /// This is the one op where the two providers differ in CAPABILITY
    /// rather than only in cost: the memory provider holds no version
    /// history at all and can answer only the empty window. It says
    /// `Compacted` for anything else rather than returning the current
    /// state under a historical label — the same fail-closed rule
    /// `get_as_of` follows.
    fn scan_versions_op(&mut self, body: &[u8], out: &mut [u8]) -> (u8, usize);
    /// The SNAPSHOT_VERSIONS op: live rows as-of a
    /// revision, emitted in the [`KV_RESULT_VERSIONS`] entry format so
    /// each row carries its `mod_revision` and MVCC `commit_ts`.
    /// `(KV_RESULT_COMPACTED, 0)` when the provider does not retain
    /// the requested revision — the same fail-closed rule as
    /// [`get_as_of`](Self::get_as_of).
    fn snapshot_versions_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize);
    /// FLUSHDB: drop every record and bump the revision once.
    fn flush_all(&mut self) -> bool;

    /// Record slots the provider can still accept as NEW keys. The txn
    /// interpreter reads this to keep `KV_OP_TXN` all-or-nothing: it
    /// refuses a multi-put transaction whose inserts would not all fit
    /// BEFORE applying any op, rather than tearing at the put that fills
    /// the store (an INSERT that wrote its row but not its index entry
    /// would corrupt the secondary index). Chunk uploads and the chunk
    /// sweep read it the same way ([`SWEEP_ROOM`]). Providers with no
    /// fixed ceiling — the disk store — return `usize::MAX` and are never
    /// pre-flighted.
    fn free_slots(&self) -> usize {
        usize::MAX
    }

    // ── Historical (MVCC as-of) reads ───────────────────────────────
    //
    // These are the two methods where the providers are allowed to
    // DIFFER in capability, and the only place the engine tolerates a
    // provider answering "I can't". They still may not differ in
    // MEANING: where both can answer they must answer identically, and
    // where one cannot it must say `Compacted` rather than substitute
    // a latest read. `tests/contract_state_store.rs` asserts exactly
    // that pair of properties.

    /// Lowest MVCC revision this provider can still answer a read at.
    /// A request strictly below it is `Compacted`.
    ///
    /// - Disk provider: the committed GC floor — everything above
    ///   it is retained history.
    /// - Memory provider: its CURRENT revision. It materializes only
    ///   current values, so the only "history" it holds is the present.
    fn read_horizon(&self) -> u64;

    /// The revision at or below which a record's ABSENCE proves nothing,
    /// because floor-respecting compaction may already have reclaimed it.
    /// This is NOT [`read_horizon`](Self::read_horizon): that is the MVCC
    /// history depth (the memory provider reports its current revision for
    /// it), whereas this is the garbage-collection reclaim floor.
    ///
    /// - Memory provider: `0`. It holds one live version per key and runs
    ///   no floor-driven reclamation, so a live record — an idempotency
    ///   identity among them — is present until overwritten or deleted;
    ///   absence therefore always means "never written".
    /// - Disk provider: the committed GC floor it was last handed.
    ///
    /// Used by `txn::interpret_lookup` to separate `DidNotHappen` from
    /// `Indeterminate`.
    fn reclaim_floor(&self) -> u64 {
        0
    }

    /// The revision a Compact ([`KV_OP_COMPACT`]) last retired history
    /// below (0 = none).
    fn compact_revision(&self) -> u64;

    /// Retire history below `rev`: reads under it are refused from now.
    fn set_compact_revision(&mut self, rev: u64);

    /// Point read of `key` as of `revision` (`0` = latest, and then
    /// byte-identical to [`get_live`](Self::get_live)). Expiry-filtered
    /// with the same rule as the latest path.
    fn get_as_of(
        &mut self,
        key: &[u8],
        revision: u64,
        now_ms: u64,
        val_out: &mut [u8],
    ) -> HistOutcome;

    /// The SCAN walk as of `revision` (`0` = latest, and then
    /// byte-identical to [`scan_op`](Self::scan_op)). Returns the same
    /// `(result_code, len)` shape as `scan_op`, or
    /// `(KV_RESULT_COMPACTED, 0)` when the provider cannot answer.
    fn scan_as_of(
        &mut self,
        revision: u64,
        cursor: u64,
        limit: u16,
        out: &mut [u8],
        now_ms: u64,
    ) -> (u8, usize);
}

// ── Apply ─────────────────────────────────────────────────────────────

/// Apply one op to the (memory) store. Reads typed body, writes typed
/// result body into `out_body`, returns `(result_code, body_byte_count)`.
///
/// Thin wrapper over [`apply_mat`] with the memory provider: the worker's
/// memory path and the host tests apply to a [`KvStore`] directly.
///
/// `now_ms` is the wall-clock millisecond reading (monotonic is fine
/// in production; tests typically pass `0` to disable TTL filtering
/// and pass increasing values to exercise expiry). Records whose
/// absolute `expiry_ms` deadline has passed are treated as
/// `KV_RESULT_NOT_FOUND` by reads and lazily reclaimed on the next
/// write/lookup that touches their slot.
pub fn apply(
    store: &mut KvStore,
    op: u8,
    body: &[u8],
    out_body: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    apply_mat(store, op, body, out_body, now_ms)
}

/// The read policy a command is served under (the
/// `RequestContext`). Only the two fields that change what a read
/// SEES are carried; everything else in the context is the router's
/// and the anchor's business, not the state machine's.
///
/// `Consistency::Snapshot` is the one policy that redirects a read
/// away from latest: it names an explicit MVCC timestamp and the
/// engine must serve at it or refuse. Every other policy differs only
/// in what fence had to be satisfied BEFORE the command reached the
/// worker, which is settled upstream — by the time a command is here
/// those all read latest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadPolicy {
    /// `db_context::Consistency` byte.
    pub consistency: u8,
    /// `db_context::RequestContext::read_timestamp`; `0` = latest.
    pub read_timestamp: u64,
}

impl ReadPolicy {
    /// Serve at latest — the policy every non-Snapshot request gets.
    pub const LATEST: ReadPolicy = ReadPolicy {
        consistency: db_context::Consistency::Linearizable as u8,
        read_timestamp: 0,
    };

    /// Build a Snapshot policy at `read_timestamp`.
    pub const fn snapshot(read_timestamp: u64) -> Self {
        Self {
            consistency: db_context::Consistency::Snapshot as u8,
            read_timestamp,
        }
    }

    /// The MVCC revision reads must be served at: the context's
    /// `read_timestamp` under `Consistency::Snapshot`, `0` (latest)
    /// under every other policy. A Snapshot request with a zero
    /// timestamp means "snapshot of now" and is also latest.
    pub const fn read_at(&self) -> u64 {
        if self.consistency == db_context::Consistency::Snapshot as u8 {
            self.read_timestamp
        } else {
            0
        }
    }
}

/// THE semantic engine over any materialization provider, at the
/// default (latest) read policy. Identical `(result_code,
/// result_body)` bytes on every provider for identical command
/// streams — the differential guarantee `tests/integration_kv_disk.rs`
/// asserts.
pub fn apply_mat<M: Materializer>(
    store: &mut M,
    op: u8,
    body: &[u8],
    out_body: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    apply_mat_ctx(store, op, body, out_body, now_ms, ReadPolicy::LATEST)
}

/// THE semantic engine, with the request's read policy.
///
/// There are two ways a caller expresses "read at revision R", and
/// they exist because two different callers can express two different
/// things:
///
/// - **The native ops** `KV_OP_GET_AT` / `KV_OP_SCAN_AT` carry the
///   revision in their op body. They are Snapshot reads BY
///   CONSTRUCTION, so an adapter that has no way to populate a
///   `RequestContext` — every compatibility protocol, since the
///   context is the native surface — can still ask a historical
///   question. This is the path etcd's `Range{revision > 0}` takes.
/// - **The request context**: `Consistency::Snapshot` plus a non-zero
///   `read_timestamp` makes ordinary `GET`/`SCAN`/`RANGE` historical,
///   which is how the native database surface expresses it
///   without a second op code per read.
///
/// Both land on the same two provider methods, so there is exactly one
/// definition of what a historical read means.
pub fn apply_mat_ctx<M: Materializer>(
    store: &mut M,
    op: u8,
    body: &[u8],
    out_body: &mut [u8],
    now_ms: u64,
    policy: ReadPolicy,
) -> (u8, usize) {
    // Snapshot policy redirects the plain read ops onto the historical
    // path. Writes are unaffected: a mutation is ordered by consensus,
    // never by a read timestamp.
    let at = policy.read_at();
    if at != 0 {
        match op {
            KV_OP_GET => return apply_get_revision(store, body, out_body, now_ms, at),
            KV_OP_SCAN | KV_OP_RANGE => {
                let mut off = 0;
                let cursor = read_u64(body, &mut off).unwrap_or(0);
                let limit = read_u16(body, &mut off).unwrap_or(64);
                return store.scan_as_of(at, cursor, limit, out_body, now_ms);
            }
            _ => {}
        }
    }
    match op {
        KV_OP_GET => apply_get(store, body, out_body, now_ms),
        // Ops that may keep large values' references run as one batch:
        // their bookkeeping takes the write's own revision.
        KV_OP_PUT => batched(store, |s| apply_put(s, body, out_body, now_ms, false)),
        KV_OP_PUT_MANIFEST => batched(store, |s| apply_put(s, body, out_body, now_ms, true)),
        KV_OP_PUT_CHUNK | KV_OP_CHUNK_SWEEP if store.batch_open() => {
            // A chunk op is an op of its own: inside another's batch it
            // could write a key that op already wrote.
            (KV_RESULT_INTERNAL, 0)
        }
        KV_OP_PUT_CHUNK => batched(store, |s| apply_put_chunk(s, body, out_body, now_ms)),
        KV_OP_GET_CHUNK => apply_get_chunk(store, body, out_body, now_ms),
        KV_OP_CHUNK_SWEEP => batched(store, |s| apply_chunk_sweep(s, body, out_body, now_ms)),
        KV_OP_DELETE => batched(store, |s| apply_delete(s, body, out_body, now_ms)),
        KV_OP_INCR => apply_incr_or_decr(store, body, out_body, true, now_ms),
        KV_OP_DECR => apply_incr_or_decr(store, body, out_body, false, now_ms),
        KV_OP_APPEND => apply_append(store, body, out_body, now_ms),
        KV_OP_PREPEND => apply_prepend(store, body, out_body, now_ms),
        KV_OP_EXISTS => apply_exists(store, body, out_body, now_ms),
        KV_OP_STRLEN => apply_strlen(store, body, out_body, now_ms),
        KV_OP_MGET => apply_mget(store, body, out_body, now_ms),
        KV_OP_MSET => apply_mset(store, body, out_body, now_ms),
        KV_OP_SCAN | KV_OP_RANGE => apply_scan(store, body, out_body, now_ms),
        KV_OP_RANGE_SCAN => store.range_scan_op(body, out_body, now_ms),
        KV_OP_RANGE_META => apply_range_meta(store, body, out_body, now_ms),
        KV_OP_DELETE_META => batched(store, |s| apply_delete_meta(s, body, out_body, now_ms)),
        KV_OP_LEASE_REVOKE => batched(store, |s| apply_lease_keys(s, body, out_body, now_ms, true)),
        KV_OP_COMPACT => apply_compact(store, body),
        KV_OP_HASH_KV => apply_hash_kv(store, body, out_body, now_ms),
        KV_OP_LEASE_KEYS => apply_lease_keys(store, body, out_body, now_ms, false),
        KV_OP_ETCD_TXN => {
            // One revision for the whole transaction, nested ones included.
            store.begin_batch();
            let r = apply_etcd_txn(store, body, out_body, now_ms, 0);
            store.end_batch();
            r
        }
        KV_OP_SCAN_VERSIONS => store.scan_versions_op(body, out_body),
        KV_OP_SNAPSHOT_VERSIONS => store.snapshot_versions_op(body, out_body, now_ms),
        KV_OP_GET_AT => apply_get_at(store, body, out_body, now_ms),
        KV_OP_SCAN_AT => apply_scan_at(store, body, out_body, now_ms),
        KV_OP_CAS => batched(store, |s| apply_cas(s, body, out_body, now_ms)),
        KV_OP_TXN => apply_txn(store, body, out_body, now_ms),
        KV_OP_TXN_PREPARE => apply_txn_prepare(store, body, out_body, now_ms),
        KV_OP_TXN_RESOLVE => apply_txn_resolve(store, body, out_body, now_ms),
        KV_OP_TXN_RECORD => apply_txn_record(store, body, out_body, now_ms),
        KV_OP_IDEMPOTENT => apply_idempotent(store, body, out_body, now_ms),
        KV_OP_IDEMPOTENT_LOOKUP => apply_idempotent_lookup(store, body, out_body, now_ms),
        KV_OP_FLUSH => {
            if store.flush_all() {
                (KV_RESULT_OK, 0)
            } else {
                (KV_RESULT_INTERNAL, 0)
            }
        }
        _ => (KV_RESULT_INTERNAL, 0),
    }
}

/// `true` iff the record carries an absolute deadline that has passed.
/// Records with `expiry_ms == 0` never expire.
fn is_expired(record: &KvRecord, now_ms: u64) -> bool {
    record.expiry_ms != 0 && now_ms >= record.expiry_ms
}

/// Filter `lookup_present` through the TTL gate: if the slot is
/// present but expired, lazily delete it and report not-found.
fn lookup_live(store: &mut KvStore, key: &[u8], now_ms: u64) -> Option<usize> {
    let idx = store.lookup_present(key)?;
    if is_expired(&store.records[idx], now_ms) {
        let _ = store.delete_at(idx);
        return None;
    }
    Some(idx)
}

// ── Memory materializer (the historical KvStore behaviour) ───────────

impl Materializer for KvStore {
    fn set_key_identity(&mut self, tenant: u32, database: u32, keyspace: u32) {
        self.key_identity = ident_bytes(tenant, database, keyspace);
    }

    fn key_identity(&self) -> (u32, u32, u32) {
        let b = &self.key_identity;
        let rd = |i: usize| u32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]]);
        (rd(0), rd(4), rd(8))
    }

    fn journal(&mut self) -> Option<&mut EventJournal> {
        if self.journaling {
            Some(&mut self.events)
        } else {
            None
        }
    }

    fn keylog(&mut self) -> Option<&mut KeyLog> {
        Some(&mut self.keylog)
    }

    fn compact_revision(&self) -> u64 {
        self.compact_rev
    }

    fn set_compact_revision(&mut self, rev: u64) {
        self.compact_rev = rev;
    }

    fn set_commit_ts(&mut self, ts: u64) {
        self.pending_commit_ts = ts;
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn bump_revision(&mut self) {
        self.advance_revision();
    }

    fn begin_batch(&mut self) {
        self.batch = 1;
    }

    fn end_batch(&mut self) {
        self.batch = 0;
    }

    fn get_live(&mut self, key: &[u8], now_ms: u64, val_out: &mut [u8]) -> GetOutcome {
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return GetOutcome::Absent;
        };
        let Some(idx) = lookup_live(self, key, now_ms) else {
            return GetOutcome::Absent;
        };
        let r = &self.records[idx];
        let n = r.value_len as usize;
        if n > val_out.len() {
            return GetOutcome::TooBig;
        }
        val_out[..n].copy_from_slice(r.value_bytes());
        GetOutcome::Found {
            value_len: n,
            meta: RecMeta {
                create_revision: r.create_revision,
                mod_revision: r.mod_revision,
                version: r.version,
                lease_id: r.lease_id,
                expiry_ms: r.expiry_ms,
                commit_ts: r.commit_ts,
                flags: r.flags,
            },
        }
    }

    fn insert_new(&mut self, key: &[u8], value: &[u8]) -> bool {
        let user = key;
        let rev = self.op_revision();
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return false;
        };
        let done = match self.lookup(key) {
            // The interpreter only inserts keys it just observed as
            // absent; a present key here is a logic fault — fail closed.
            Ok(_) => false,
            Err(idx) => self.insert_fresh(idx, key, value),
        };
        if done {
            self.note_key(rev, KEY_CREATED, user);
        }
        done
    }

    fn update_existing(&mut self, key: &[u8], value: &[u8]) -> bool {
        let user = key;
        let rev = self.op_revision();
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return false;
        };
        let done = match self.lookup(key) {
            Ok(idx) => self.update_value(idx, value),
            Err(_) => false,
        };
        if done {
            self.note_key(rev, KEY_MODIFIED, user);
        }
        done
    }

    fn set_expiry(&mut self, key: &[u8], expiry_ms: u64) -> bool {
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return false;
        };
        match self.lookup_present(key) {
            Some(idx) => {
                self.records[idx].expiry_ms = expiry_ms;
                true
            }
            None => false,
        }
    }

    fn set_lease(&mut self, key: &[u8], lease_id: u64) -> bool {
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return false;
        };
        match self.lookup_present(key) {
            Some(idx) => {
                self.records[idx].lease_id = lease_id;
                true
            }
            None => false,
        }
    }

    fn set_flags(&mut self, key: &[u8], flags: u8) -> bool {
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return false;
        };
        match self.lookup_present(key) {
            Some(idx) => {
                self.records[idx].flags = flags;
                true
            }
            None => false,
        }
    }

    fn put_meta(&mut self, key: &[u8], value: &[u8]) -> bool {
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return false;
        };
        if value.len() > MAX_VALUE_LEN {
            return false;
        }
        let idx = match self.lookup(key) {
            Ok(idx) => idx,
            Err(slot) if !self.records[slot].used => {
                let r = &mut self.records[slot];
                r.used = true;
                r.key_len = key.len() as u16;
                r.key[..key.len()].copy_from_slice(key);
                r.version = 0;
                r.lease_id = 0;
                r.expiry_ms = 0;
                r.commit_ts = 0;
                r.flags = 0;
                r.create_revision = 0;
                self.used_count += 1;
                slot
            }
            Err(_) => return false,
        };
        let rev = self.advance_revision();
        let r = &mut self.records[idx];
        if r.create_revision == 0 {
            r.create_revision = rev;
        }
        r.value_len = value.len() as u32;
        r.value[..value.len()].copy_from_slice(value);
        r.mod_revision = rev;
        r.version = r.version.saturating_add(1);
        true
    }

    fn remove_meta(&mut self, key: &[u8]) -> bool {
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return false;
        };
        match self.lookup_present(key) {
            Some(idx) => {
                self.advance_revision();
                self.delete_at(idx)
            }
            None => true,
        }
    }

    fn batch_open(&self) -> bool {
        self.batch != 0
    }

    fn op_revision(&self) -> u64 {
        if self.batch == 2 {
            self.revision
        } else {
            self.revision.wrapping_add(1)
        }
    }

    fn remove(&mut self, key: &[u8], now_ms: u64) -> Option<bool> {
        let user = key;
        let rev = self.op_revision();
        let mut kbuf = [0u8; MAX_KEY_LEN];
        let Some(key) = self.compose_key(key, &mut kbuf) else {
            return Some(false);
        };
        let removed = match lookup_live(self, key, now_ms) {
            Some(idx) => self.delete_at(idx),
            None => false,
        };
        if removed {
            self.note_key(rev, KEY_DELETED, user);
        }
        Some(removed)
    }

    fn scan_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize) {
        let mut off = 0;
        let cursor = read_u64(body, &mut off).unwrap_or(0);
        let limit = read_u16(body, &mut off).unwrap_or(64);
        self.scan_slots(cursor, limit, out, now_ms)
    }

    fn range_scan_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize) {
        let Some(q) = parse_range_scan_body(body) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        self.range_slots(&q, out, now_ms)
    }

    fn walk_live_meta<F: FnMut(&[u8], &[u8], &RecMeta) -> bool>(
        &mut self,
        start: &[u8],
        end: &[u8],
        at_rev: u64,
        now_ms: u64,
        mut f: F,
    ) -> u8 {
        // Only the present is held: a past revision is compacted.
        if at_rev != 0 && at_rev < self.revision {
            return KV_RESULT_COMPACTED;
        }
        for idx in 0..MAX_KEYS {
            let record = &self.records[idx];
            if !record.used || is_expired(record, now_ms) {
                continue;
            }
            let Some(key) = record.key_bytes().strip_prefix(&self.key_identity[..]) else {
                continue;
            };
            if key < start || (!end.is_empty() && key >= end) {
                continue;
            }
            let meta = RecMeta {
                create_revision: record.create_revision,
                mod_revision: record.mod_revision,
                version: record.version,
                lease_id: record.lease_id,
                expiry_ms: record.expiry_ms,
                commit_ts: record.commit_ts,
                flags: record.flags,
            };
            if !f(key, record.value_bytes(), &meta) {
                break;
            }
        }
        KV_RESULT_OK
    }

    fn scan_versions_op(&mut self, body: &[u8], out: &mut [u8]) -> (u8, usize) {
        let Some(w) = parse_scan_versions_body(body) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        // The empty window is the one honest answer this provider has:
        // nothing happened in it, and that is true without any history.
        // Every other window is a question about the past, which a
        // provider that materializes only current values cannot answer.
        if w.from >= normalize_to(w.to, self.revision) {
            if out.len() < 10 {
                return (KV_RESULT_INTERNAL, 0);
            }
            out[0..10].fill(0);
            return (KV_RESULT_VERSIONS, 10);
        }
        (KV_RESULT_COMPACTED, 0)
    }

    fn snapshot_versions_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize) {
        // The memory provider materializes only current values, so it
        // can answer the snapshot question only AT the present — the
        // same fail-closed capability rule as `get_as_of`.
        let Some((at_rev, q)) = parse_snapshot_versions_body(body) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        if at_rev != 0 && at_rev < self.revision {
            return (KV_RESULT_COMPACTED, 0);
        }
        if out.len() < 10 {
            return (KV_RESULT_INTERNAL, 0);
        }
        let (start, end, cursor, limit) = (q.start, q.end, q.cursor, q.limit);
        let mut out_off = 10usize;
        // Linear non-wrapping slot cursor — the exactly-once walk
        // discipline of `range_slots`, which this mirrors.
        let mut idx = cursor as usize;
        let mut emitted: u16 = 0;
        let mut new_cursor: u64 = 0;
        while idx < MAX_KEYS {
            let record = &self.records[idx];
            if record.used && !is_expired(record, now_ms) {
                let key = match record.key_bytes().strip_prefix(&self.key_identity[..]) {
                    Some(k) => k,
                    None => {
                        idx += 1;
                        continue;
                    }
                };
                let in_span = key >= start && (end.is_empty() || key < end);
                if in_span {
                    let val = record.value_bytes();
                    let need = 8 + 8 + 1 + 2 + key.len() + 4 + val.len();
                    if out_off + need > out.len() {
                        new_cursor = idx as u64;
                        break;
                    }
                    out[out_off..out_off + 8].copy_from_slice(&record.mod_revision.to_le_bytes());
                    out_off += 8;
                    out[out_off..out_off + 8].copy_from_slice(&record.commit_ts.to_le_bytes());
                    out_off += 8;
                    out[out_off] = VERSION_KIND_PUT;
                    out_off += 1;
                    out[out_off..out_off + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
                    out_off += 2;
                    out[out_off..out_off + key.len()].copy_from_slice(key);
                    out_off += key.len();
                    out[out_off..out_off + 4].copy_from_slice(&(val.len() as u32).to_le_bytes());
                    out_off += 4;
                    out[out_off..out_off + val.len()].copy_from_slice(val);
                    out_off += val.len();
                    emitted += 1;
                    if emitted >= limit {
                        new_cursor = (idx + 1) as u64;
                        if new_cursor as usize >= MAX_KEYS {
                            new_cursor = 0;
                        }
                        break;
                    }
                }
            }
            idx += 1;
        }
        out[0..8].copy_from_slice(&new_cursor.to_le_bytes());
        out[8..10].copy_from_slice(&emitted.to_le_bytes());
        (KV_RESULT_VERSIONS, out_off)
    }

    fn flush_all(&mut self) -> bool {
        self.note_whole();
        // FLUSH clears THIS identity's keys only. Delete every slot
        // whose stored key carries the current identity prefix; the
        // probe-chain compaction in `delete_at` keeps the table valid as
        // we go, so re-scan from the top after each removal.
        loop {
            let mut hit: Option<usize> = None;
            let mut i = 0;
            while i < MAX_KEYS {
                if self.records[i].used
                    && self.records[i]
                        .key_bytes()
                        .starts_with(&self.key_identity[..])
                {
                    hit = Some(i);
                    break;
                }
                i += 1;
            }
            match hit {
                Some(idx) => {
                    self.delete_at(idx);
                }
                None => break,
            }
        }
        self.revision = self.revision.wrapping_add(1);
        true
    }

    /// The fixed-size hash table has `MAX_KEYS` slots; `used_count`
    /// tracks the live ones, so the remainder is what a txn's inserts
    /// may still claim.
    fn free_slots(&self) -> usize {
        (MAX_KEYS as u32).saturating_sub(self.used_count) as usize
    }

    /// The memory provider holds ONLY current values, so the oldest
    /// revision it can speak for is the one it is at right now.
    fn read_horizon(&self) -> u64 {
        self.revision
    }

    /// Historical point read. Answerable only when the requested
    /// revision is at-or-above the current one — i.e. when "as of R" and
    /// "latest" are the same state. For anything older this provider
    /// has nothing retained and says `Compacted` rather than serving
    /// the present under a past label. That refusal IS the contract:
    /// a memory composition is honestly history-free, not quietly wrong.
    fn get_as_of(
        &mut self,
        key: &[u8],
        revision: u64,
        now_ms: u64,
        val_out: &mut [u8],
    ) -> HistOutcome {
        if revision != 0 && revision < self.read_horizon() {
            return HistOutcome::Compacted;
        }
        match self.get_live(key, now_ms, val_out) {
            GetOutcome::Absent => HistOutcome::Absent,
            GetOutcome::Found { value_len, meta } => HistOutcome::Found { value_len, meta },
            GetOutcome::TooBig => HistOutcome::TooBig,
            GetOutcome::Fault => HistOutcome::Fault,
        }
    }

    fn scan_as_of(
        &mut self,
        revision: u64,
        cursor: u64,
        limit: u16,
        out: &mut [u8],
        now_ms: u64,
    ) -> (u8, usize) {
        if revision != 0 && revision < self.read_horizon() {
            return (KV_RESULT_COMPACTED, 0);
        }
        self.scan_slots(cursor, limit, out, now_ms)
    }
}

impl KvStore {
    /// The hash-slot walk behind both `scan_op` and `scan_as_of`.
    fn scan_slots(&mut self, cursor: u64, limit: u16, out: &mut [u8], now_ms: u64) -> (u8, usize) {
        if out.len() < 10 {
            return (KV_RESULT_INTERNAL, 0);
        }
        let mut out_off = 0;
        let cursor_at = out_off;
        out_off += 8;
        let count_at = out_off;
        out_off += 2;

        let mut idx = (cursor as usize) % MAX_KEYS;
        let start = idx;
        let mut emitted: u16 = 0;
        let mut new_cursor: u64 = cursor;

        loop {
            let record = &self.records[idx];
            if record.used && !is_expired(record, now_ms) {
                let stored = record.key_bytes();
                // This walk visits every slot, so it must skip keys
                // that are not this identity's and hand back the USER
                // key (prefix stripped) — never another tenant's data,
                // never the internal prefix.
                if let Some(key) = stored.strip_prefix(&self.key_identity[..]) {
                    if out_off + 4 + key.len() > out.len() {
                        break;
                    }
                    let len = key.len() as u32;
                    out[out_off..out_off + 4].copy_from_slice(&len.to_le_bytes());
                    out_off += 4;
                    out[out_off..out_off + key.len()].copy_from_slice(key);
                    out_off += key.len();
                    emitted += 1;
                    if emitted >= limit {
                        new_cursor = ((idx + 1) % MAX_KEYS) as u64;
                        break;
                    }
                }
            }
            idx = (idx + 1) % MAX_KEYS;
            if idx == start {
                new_cursor = 0;
                break;
            }
        }

        out[cursor_at..cursor_at + 8].copy_from_slice(&new_cursor.to_le_bytes());
        out[count_at..count_at + 2].copy_from_slice(&emitted.to_le_bytes());
        (KV_RESULT_SCAN_CURSOR, out_off)
    }

    /// The hash-slot walk behind `range_scan_op`, filtered to
    /// `[start, end)` and emitting values alongside keys.
    ///
    /// The memory provider is a hash table, so it cannot seek to
    /// `start` — it visits every slot and discards the ones outside the
    /// span. That is a COST difference from the ordered provider, not a
    /// semantic one: both emit exactly the live keys of the span, once
    /// each, across a cursor loop. The slot index remains the cursor,
    /// which is what keeps the walk resumable even though nothing about
    /// it is sorted.
    fn range_slots(&mut self, q: &RangeQuery<'_>, out: &mut [u8], now_ms: u64) -> (u8, usize) {
        let (start, end, cursor, limit) = (q.start, q.end, q.cursor, q.limit);
        if out.len() < 10 {
            return (KV_RESULT_INTERNAL, 0);
        }
        let mut out_off = 10usize;
        // The cursor is a LINEAR slot index, and the walk deliberately
        // does not wrap. `scan_op`'s cursor does wrap, because Redis
        // SCAN is explicitly allowed to return a key more than once —
        // but this op promises exactly-once, and a wrapping cursor
        // cannot: a page that resumed at slot N and ran back round to N
        // would re-emit everything below N. Refusing to wrap costs
        // nothing (the table is a fixed array) and makes the guarantee
        // structural rather than something the caller must dedupe.
        let mut idx = cursor as usize;
        let mut emitted: u16 = 0;
        let mut new_cursor: u64 = 0;

        while idx < MAX_KEYS {
            let record = &self.records[idx];
            if record.used && !is_expired(record, now_ms) {
                // Strip the identity prefix and compare in USER-key
                // space. A key under a different identity strips to
                // `None` and is skipped — another tenant's row is not in
                // any span this identity can name.
                let key = match record.key_bytes().strip_prefix(&self.key_identity[..]) {
                    Some(k) => k,
                    None => {
                        idx += 1;
                        continue;
                    }
                };
                // Half-open: `start` inclusive, `end` exclusive. An
                // empty bound is unbounded on that side — an empty
                // `end` must NOT be read as "everything is >= it",
                // which is why the emptiness check comes first.
                let in_span = key >= start && (end.is_empty() || key < end);
                if in_span {
                    let val = record.value_bytes();
                    let need = 2 + key.len() + 4 + val.len();
                    if out_off + need > out.len() {
                        // Page is byte-bound rather than limit-bound.
                        // Resume AT this slot, not after it — this
                        // entry has not been emitted.
                        new_cursor = idx as u64;
                        break;
                    }
                    out[out_off..out_off + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
                    out_off += 2;
                    out[out_off..out_off + key.len()].copy_from_slice(key);
                    out_off += key.len();
                    out[out_off..out_off + 4].copy_from_slice(&(val.len() as u32).to_le_bytes());
                    out_off += 4;
                    out[out_off..out_off + val.len()].copy_from_slice(val);
                    out_off += val.len();
                    emitted += 1;
                    if emitted >= limit {
                        new_cursor = (idx + 1) as u64;
                        break;
                    }
                }
            }
            idx += 1;
        }

        // Running off the end of the table IS exhaustion, and a cursor
        // of `MAX_KEYS` would be indistinguishable from more work. The
        // walk above leaves `new_cursor` at 0 in that case because it
        // is only ever set on an early break.
        if new_cursor as usize >= MAX_KEYS {
            new_cursor = 0;
        }
        out[0..8].copy_from_slice(&new_cursor.to_le_bytes());
        out[8..10].copy_from_slice(&emitted.to_le_bytes());
        (KV_RESULT_RANGE, out_off)
    }
}

// ── Disk materializer (ordered disk provider) ────────────────────────

/// Version of the disk record-metadata prefix layout below. Bump on
/// ANY layout change — the prefix is persisted inside every disk
/// value payload, so a change is a persistent-format break.
pub const DISK_META_FORMAT: u16 = 1;

/// Fixed metadata prefix stored at the head of every disk value
/// payload (little-endian):
///
/// ```text
/// [format:u16 = DISK_META_FORMAT][flags:u8][rsvd:u8 × 3 = 0]
/// [create_revision:u64][mod_revision:u64][version:u64]
/// [lease_id:u64][expiry_ms:u64][commit_ts:u64]           (54 bytes)
/// ```
///
/// The user value follows immediately. `disk_store::MAX_VALUE_LEN`
/// budgets for `MAX_VALUE_LEN + DISK_META_LEN` so a full-size user
/// value still fits. A payload shorter than this prefix, or one whose
/// format field is unknown, is a storage fault (fail closed).
pub const DISK_META_LEN: usize = 54;

fn encode_disk_meta(out: &mut [u8], meta: &RecMeta) {
    out[0..2].copy_from_slice(&DISK_META_FORMAT.to_le_bytes());
    out[2] = meta.flags;
    out[3..6].copy_from_slice(&[0; 3]);
    out[6..14].copy_from_slice(&meta.create_revision.to_le_bytes());
    out[14..22].copy_from_slice(&meta.mod_revision.to_le_bytes());
    out[22..30].copy_from_slice(&meta.version.to_le_bytes());
    out[30..38].copy_from_slice(&meta.lease_id.to_le_bytes());
    out[38..46].copy_from_slice(&meta.expiry_ms.to_le_bytes());
    out[46..54].copy_from_slice(&meta.commit_ts.to_le_bytes());
}

fn decode_disk_meta(src: &[u8]) -> Option<RecMeta> {
    if src.len() < DISK_META_LEN {
        return None;
    }
    let mut off = 0usize;
    let format = u16::from_le_bytes([src[0], src[1]]);
    if format != DISK_META_FORMAT {
        return None;
    }
    let flags = src[2];
    off += 6; // format, flags, rsvd
    let rd = |o: &mut usize| {
        let mut b = [0u8; 8];
        b.copy_from_slice(&src[*o..*o + 8]);
        *o += 8;
        u64::from_le_bytes(b)
    };
    Some(RecMeta {
        create_revision: rd(&mut off),
        mod_revision: rd(&mut off),
        version: rd(&mut off),
        lease_id: rd(&mut off),
        expiry_ms: rd(&mut off),
        commit_ts: rd(&mut off),
        flags,
    })
}

/// Worst-case one-op committed batch (header + record framing + a
/// full encoded key + a full meta-prefixed value).
const DISK_BATCH_MAX: usize =
    BATCH_HDR_LEN + RECORD_FIXED + disk_store::MAX_ENCODED_KEY + disk_store::MAX_VALUE_LEN;

/// Scan staging buffer: at least one worst-case provider scan entry.
const DISK_SCAN_BUF: usize = RECORD_FIXED + disk_store::MAX_ENCODED_KEY + disk_store::MAX_VALUE_LEN;

/// Bytes a `KV_RESULT_VERSIONS` entry is guaranteed to be SMALLER than
/// the raw record it is transcoded from. Raw: `[klen:2][vlen:4][key]
/// [value]` where the encoded key carries the 12-byte identity, the
/// 2-byte terminator and the 9-byte version suffix around the user key
/// (escaping only lengthens it). Entry: 23 fixed bytes + user key +
/// user value. So raw − entry ≥ 6 + 23 − 23 = 6 for a bare tombstone,
/// and ≥ 60 for a put, whose value also drops `DISK_META_LEN`. The
/// minimum is what the paging arithmetic may rely on.
const VERSIONS_TRANSCODE_SLACK: usize = 6;

/// Internal read-latest outcome (pre-expiry-filter).
enum ReadLatest {
    Absent,
    /// Payload length (meta prefix + value) and the decoded meta; the
    /// payload bytes are in the caller's buffer.
    Found(usize, RecMeta),
    Fault,
}

/// Compaction-step safety valve: `disk_store` bounds live records at
/// `(MAX_RUNS + 1) * MEMTABLE_MAX_ENTRIES` = 8704, i.e. at most 68
/// `COMPACT_STEP_RECORDS`-sized steps; anything past this is a
/// provider bug and fails closed rather than spinning.
const DISK_COMPACT_STEP_CAP: u32 = 512;

/// Disk materialization provider: the ordered disk state-store
/// (`disk_store.rs`) beneath the single semantic engine. All engine
/// keys are encoded with tenant/database/keyspace = 0/0/0 (single
/// implicit namespace for now) and `mvcc_timestamp` = the committed
/// engine revision that produced the version.
///
/// `fault` latches on any storage fault: every subsequent op fails
/// closed with `KV_RESULT_INTERNAL` until the hosting module
/// quarantines/recovers — an error is never downgraded to an
/// empty/absent result.
///
/// ## Replay idempotence across the recovery boundary
///
/// After `recover()` the materialization already reflects every write
/// with an assigned revision `<= replay_floor` (the recovered
/// `highest_revision`), and the WAL then re-feeds the committed
/// command stream from the start. The engine re-runs every replayed
/// command IN FULL — the revision counter, control flow and expiry
/// bookkeeping advance exactly as they did originally — while this
/// provider makes the physical side idempotent:
///
/// - **Reads are as-of**: while `revision < replay_floor`, every
///   lookup/scan is served at MVCC revision = the current engine
///   revision, so a replayed command observes exactly the state its
///   original execution observed (this is what keeps data-dependent
///   control flow — INCR arithmetic, NX/XX, CAS witnesses, TXN
///   branches — deterministic). Requires history retention at or
///   below the replay window: the compaction floor must not have
///   advanced past un-replayed history (the GC-floor contract ties
///   floor advancement to durable snapshot coverage).
/// - **Writes below the floor are suppressed** (`ts < replay_floor`):
///   the flush order guarantees every distinct-revision write below
///   the recovered high-water is already durable (flush persists the
///   whole memtable, and writes are revision-ordered), so re-applying
///   is pure churn.
/// - **Writes AT the floor re-apply idempotently** (`ts ==
///   replay_floor`): several sibling writes can share one revision
///   (multi-key DELETE / FLUSH tombstones — see the revision
///   assignment note on `Materializer`), and an in-line flush between
///   siblings can persist some but not all of them. Re-applying is an
///   exact-encoded-key overwrite (deterministic replay per the
///   provider contract) for the persisted ones and heals the lost
///   ones.
/// - **Same-revision overwrites** (`set_expiry` rewrites a version in
///   place, claiming no new revision) are never ts-suppressed: the
///   as-of read plus the existing equality short-circuit skips them
///   when the materialized bytes already match and re-issues them
///   when a crash lost the overwrite.
///
/// Caveat (pre-existing, shared with the memory provider's
/// replay-from-empty path): `now_ms` during replay is the CURRENT
/// clock, so TTL decisions can drift from the original execution for
/// records whose deadline falls inside the outage window.
pub struct DiskMaterializer<'a, S: RunStorage> {
    pub store: DiskStore<'a, S>,
    /// Engine revision mirror (seed from `DiskState::highest_revision`
    /// after `recover()`/install; read back after each command).
    pub revision: u64,
    /// Recovered high-water revision: writes with an assigned
    /// revision strictly below it are already materialized and are
    /// suppressed during WAL replay; reads while `revision <
    /// replay_floor` are served as-of the current engine revision.
    /// `0` = fresh store, no replay window.
    pub replay_floor: u64,
    /// Advisory: the provider wants a flush cycle (memtable at the
    /// high-water mark). The hosting module drives `store.flush()` at
    /// a step boundary.
    pub flush_wanted: bool,
    /// Latched storage fault — fail closed on everything.
    pub fault: bool,
    /// MVCC commit timestamp for the command in flight;
    /// stamped into every version this command writes. Per-command
    /// state like `key_identity`, never persisted on its own.
    pub pending_commit_ts: u64,
    /// Latched RETRYABLE refusal: the memtable filled while a chunked
    /// flush was already draining it. Distinct from `fault` — nothing
    /// is wrong with the store, the write simply arrived during the
    /// one window where no more room can be made. The hosting module
    /// counts it as flush backpressure.
    pub backpressure: bool,
    /// The revision this command's reply is AS-OF, when that differs
    /// from the engine revision: a version scan pins its window ceiling
    /// at open and answers every page of the window against it, so the
    /// reply must name that ceiling, not whatever has landed since. `0`
    /// = no override (report the engine revision).
    pub reply_revision: u64,
    /// Canonical identity `(tenant, database, keyspace)` for the
    /// command in flight. Fed straight to `internal_key::encode`, so
    /// every physical key this materializer writes and every scan bound
    /// it builds is scoped to it. Default `(0, 0, 0)` is the historical
    /// single-tenant layout.
    pub key_identity: (u32, u32, u32),
    /// A write batch (etcd's Txn): 0 none, 1 open, 2 open and its revision
    /// taken.
    pub batch: u8,
    /// Where this command's watch events are journaled, when the host
    /// keeps a journal.
    pub events: Option<&'a mut EventJournal>,
    /// Where this command's changed keys are staged, when the host serves a
    /// store provider.
    pub keylog: Option<&'a mut KeyLog>,
}

impl<'a, S: RunStorage> DiskMaterializer<'a, S> {
    pub fn new(store: DiskStore<'a, S>, revision: u64, replay_floor: u64) -> Self {
        // One command, one memo. Clearing here means the memo can never
        // outlive the command that filled it.
        store.state.memo_clear();
        Self {
            store,
            revision,
            replay_floor,
            flush_wanted: false,
            fault: false,
            backpressure: false,
            reply_revision: 0,
            key_identity: (0, 0, 0),
            pending_commit_ts: 0,
            batch: 0,
            events: None,
            keylog: None,
        }
    }

    /// The revision a write takes (see `KvStore::advance_revision`): a new
    /// one, or inside a batch whose revision is taken, the batch's.
    fn batch_ts(&self) -> u64 {
        if self.batch == 2 {
            self.revision
        } else {
            self.revision.wrapping_add(1)
        }
    }

    fn batch_taken(&mut self) {
        if self.batch == 1 {
            self.batch = 2;
        }
    }

    /// The identity span `[prefix, successor)` for scans this identity
    /// must not walk past: `(12-byte prefix, Some(12-byte successor))`,
    /// or `None` for the successor when the prefix is all-`0xFF`.
    fn ident_span(&self) -> ([u8; IDENT_LEN], Option<[u8; IDENT_LEN]>) {
        let (t, d, k) = self.key_identity;
        let lo = ident_bytes(t, d, k);
        let hi = ident_successor(&lo);
        (lo, hi)
    }

    /// True while replayed committed commands are being re-run over
    /// an already-materialized prefix.
    fn in_replay(&self) -> bool {
        self.revision < self.replay_floor
    }

    /// The MVCC revision reads must be served at: as-of the current
    /// engine revision inside the replay window, latest (`0`)
    /// otherwise. Callers must special-case `revision == 0` inside
    /// the window (nothing exists before the first committed write,
    /// but the provider treats `0` as "latest").
    fn read_revision(&self) -> u64 {
        if self.in_replay() {
            self.revision
        } else {
            0
        }
    }

    /// Make room for one more batch: flush the memtable; if the run
    /// set is full, drive a same-floor compaction to completion
    /// (bounded by the provider's fixed capacities — see
    /// `DISK_COMPACT_STEP_CAP`) and flush again. `false` latches
    /// `fault`.
    fn make_room(&mut self) -> bool {
        // A chunked flush is already working on this memtable. Running
        // an unbounded `flush()` here would re-create the exact defect
        // chunking exists to remove — a whole-memtable write inside one
        // command step — and it would collide with the in-flight run
        // besides. The memtable genuinely being at capacity while a
        // flush is mid-flight IS backpressure: the write is refused,
        // the client retries, and the flush finishes on its own steps.
        //
        // Reported as backpressure rather than a fault so the module
        // counts it on `disk_flush_backpressure` and logs it, instead
        // of quarantining the store over a condition that resolves
        // itself in a few steps.
        if self.store.flush_in_progress() {
            self.backpressure = true;
            return false;
        }
        // Driven only as far as it goes without waiting on the device: a
        // storage action the device cannot take now, or a fence still
        // pending, is backpressure too — the flush (or merge) stays in
        // flight and the maintenance steps finish it. Never a spin: the
        // device moves only between steps.
        match self.store.flush_drive() {
            Ok(()) => true,
            Err(StoreError::Backpressure) if self.store.flush_in_progress() => {
                self.backpressure = true;
                false
            }
            Err(StoreError::Backpressure) => {
                // The run set is full: merge. A compaction already under
                // way (maintenance's, at a committed floor) is the one
                // driven to its end — one at another floor would abort it.
                let (floor, mut cursor) = self
                    .store
                    .state
                    .compaction_in_flight()
                    .unwrap_or((self.store.state.compaction_floor, 0));
                let mut steps = 0u32;
                loop {
                    match self.store.compact(floor, cursor) {
                        Ok(Progress::Done) => break,
                        Ok(Progress::InProgress { cursor: c }) => {
                            if self.store.state.io_wait {
                                // A fence the device has not made durable
                                // yet: the merge finishes on later steps.
                                self.backpressure = true;
                                return false;
                            }
                            cursor = c;
                            steps += 1;
                            if steps > DISK_COMPACT_STEP_CAP {
                                self.fault = true;
                                return false;
                            }
                        }
                        Err(StoreError::Backpressure) if self.store.state.io_wait => {
                            self.backpressure = true;
                            return false;
                        }
                        Err(_) => {
                            self.fault = true;
                            return false;
                        }
                    }
                }
                match self.store.flush_drive() {
                    Ok(()) => true,
                    Err(StoreError::Backpressure) if self.store.state.io_wait => {
                        self.backpressure = true;
                        false
                    }
                    Err(_) => {
                        self.fault = true;
                        false
                    }
                }
            }
            Err(_) => {
                self.fault = true;
                false
            }
        }
    }

    /// Materialize one version through the committed-batch contract.
    /// Handles memtable `Backpressure` by flushing (and compacting if
    /// the run set is full) and retrying once — bounded, never a loop.
    fn put_version(&mut self, user_key: &[u8], ts: u64, kind: ValueKind, payload: &[u8]) -> bool {
        if self.fault {
            return false;
        }
        if ts <= self.replay_floor && self.replay_written(user_key, ts, kind, payload) {
            // WAL replay of a write the recovered materialization
            // already reflects byte-for-byte — suppress. Anything
            // that does NOT verify (a same-revision sibling lost to a
            // mid-command flush boundary, a lost same-ts expiry
            // overwrite, or a materialization that disagrees with the
            // authoritative log) falls through and re-applies as an
            // exact-encoded-key overwrite — deterministic replay per
            // the provider contract, never a silent suppression.
            return true;
        }
        let mut kbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let (it, id, ik) = self.key_identity;
        let Some(klen) = internal_key::encode(&mut kbuf, it, id, ik, user_key, ts, kind) else {
            self.fault = true;
            return false;
        };
        let mut b = [0u8; DISK_BATCH_MAX];
        b[0..4].copy_from_slice(&BATCH_MAGIC.to_le_bytes());
        b[4..6].copy_from_slice(&BATCH_FORMAT_VERSION.to_le_bytes());
        b[6..8].copy_from_slice(&1u16.to_le_bytes());
        let mut p = BATCH_HDR_LEN;
        b[p..p + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        b[p + 2..p + 6].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        p += RECORD_FIXED;
        b[p..p + klen].copy_from_slice(&kbuf[..klen]);
        p += klen;
        b[p..p + payload.len()].copy_from_slice(payload);
        p += payload.len();
        let mut retried = false;
        loop {
            match self.store.apply(&b[..p]) {
                Ok(r) => {
                    // REFRESH the memo rather than clear it. The write
                    // just established the latest view of this key, so
                    // the memo can hold exactly what was written — which
                    // is both correct for a read-after-write inside the
                    // same command and the thing that makes
                    // `apply_put`'s unconditional trailing `set_expiry`
                    // free instead of a third full run scan.
                    //
                    // Clearing here would be safe but wasteful; storing
                    // anything OTHER than the bytes just written would
                    // be corruption, so the two arms below mirror the
                    // two kinds `put_version` can write and nothing else.
                    if !self.in_replay() {
                        let prefix = &kbuf[..klen - internal_key::VERSION_SUFFIX_LEN];
                        match kind {
                            ValueKind::Value => {
                                self.store.state.memo_put(prefix, Some(payload));
                            }
                            ValueKind::PointTombstone => {
                                self.store.state.memo_put(prefix, None);
                            }
                            // Any other kind is not a point-visible
                            // latest-view record, so the memo cannot
                            // describe the result: drop it rather than
                            // guess.
                            _ => self.store.state.memo_clear(),
                        }
                    }
                    if r.flush_wanted {
                        self.flush_wanted = true;
                    }
                    return true;
                }
                Err(StoreError::Backpressure) if !retried => {
                    if !self.make_room() {
                        return false;
                    }
                    retried = true;
                }
                Err(_) => {
                    self.fault = true;
                    return false;
                }
            }
        }
    }

    /// Replay verification: does the materialization, viewed as-of
    /// `ts`, already reflect this write byte-for-byte? For a value
    /// write that means the visible version's payload (meta prefix
    /// included, hence `mod_revision == ts`) equals the bytes being
    /// written; for a tombstone it means the key is already absent
    /// as-of `ts` (tombstoned at-or-before, or never existed — both
    /// match the post-write state). Any read fault or mismatch
    /// answers `false` so the caller re-applies rather than
    /// suppressing — fail closed, never a silent drop.
    fn replay_written(
        &mut self,
        user_key: &[u8],
        ts: u64,
        kind: ValueKind,
        payload: &[u8],
    ) -> bool {
        let mut pbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let (it, id, ik) = self.key_identity;
        let Some(plen) = internal_key::encode_prefix(&mut pbuf, it, id, ik, user_key) else {
            return false;
        };
        let mut have = [0u8; disk_store::MAX_VALUE_LEN];
        match self.store.get_at(&pbuf[..plen], ts, &mut have) {
            Ok(n) => kind == ValueKind::Value && n == payload.len() && have[..n] == *payload,
            Err(StoreError::NotFound) => kind == ValueKind::PointTombstone,
            Err(_) => false,
        }
    }

    /// Newest visible version of `user_key` — at latest normally, or
    /// as-of the current engine revision inside the replay window
    /// (see the struct docs) — expiry NOT yet filtered. Copies the
    /// whole payload (meta prefix + value) into `payload_out` (sized
    /// `disk_store::MAX_VALUE_LEN`).
    fn read_latest(&mut self, user_key: &[u8], payload_out: &mut [u8]) -> ReadLatest {
        if self.fault {
            return ReadLatest::Fault;
        }
        if self.in_replay() && self.revision == 0 {
            // Before the first committed write nothing existed; the
            // provider can't express "as-of revision 0" (0 = latest).
            return ReadLatest::Absent;
        }
        let mut pbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let (it, id, ik) = self.key_identity;
        let Some(plen) = internal_key::encode_prefix(&mut pbuf, it, id, ik, user_key) else {
            self.fault = true;
            return ReadLatest::Fault;
        };
        // Memo only covers the LATEST view. An as-of read (replay
        // window) asks a different question and must never be served
        // from, or written into, a latest-view cache.
        let memoizable = !self.in_replay();
        if memoizable {
            match self.store.state.memo_get(&pbuf[..plen], payload_out) {
                Some(Some(n)) => {
                    return match decode_disk_meta(&payload_out[..n.min(DISK_META_LEN)]) {
                        Some(meta) if n >= DISK_META_LEN => ReadLatest::Found(n, meta),
                        _ => ReadLatest::Fault,
                    };
                }
                Some(None) => return ReadLatest::Absent,
                None => {}
            }
        }
        match self
            .store
            .get_at(&pbuf[..plen], self.read_revision(), payload_out)
        {
            Ok(n) => match decode_disk_meta(&payload_out[..n.min(DISK_META_LEN)]) {
                Some(meta) if n >= DISK_META_LEN => {
                    if memoizable {
                        self.store
                            .state
                            .memo_put(&pbuf[..plen], Some(&payload_out[..n]));
                    }
                    ReadLatest::Found(n, meta)
                }
                _ => {
                    self.fault = true;
                    ReadLatest::Fault
                }
            },
            Err(StoreError::NotFound) => {
                if memoizable {
                    self.store.state.memo_put(&pbuf[..plen], None);
                }
                ReadLatest::Absent
            }
            Err(_) => {
                self.fault = true;
                ReadLatest::Fault
            }
        }
    }
}

impl<S: RunStorage> Materializer for DiskMaterializer<'_, S> {
    fn set_key_identity(&mut self, tenant: u32, database: u32, keyspace: u32) {
        self.key_identity = (tenant, database, keyspace);
    }

    fn key_identity(&self) -> (u32, u32, u32) {
        self.key_identity
    }

    fn journal(&mut self) -> Option<&mut EventJournal> {
        self.events.as_deref_mut()
    }

    fn keylog(&mut self) -> Option<&mut KeyLog> {
        self.keylog.as_deref_mut()
    }

    fn compact_revision(&self) -> u64 {
        self.store.state.compact_revision
    }

    fn set_compact_revision(&mut self, rev: u64) {
        self.store.state.compact_revision = rev;
    }

    fn set_commit_ts(&mut self, ts: u64) {
        self.pending_commit_ts = ts;
    }

    fn revision(&self) -> u64 {
        self.revision
    }

    fn bump_revision(&mut self) {
        if self.batch != 2 {
            self.revision = self.revision.wrapping_add(1);
            self.batch_taken();
        }
    }

    fn begin_batch(&mut self) {
        self.batch = 1;
    }

    fn end_batch(&mut self) {
        self.batch = 0;
    }

    fn get_live(&mut self, key: &[u8], now_ms: u64, val_out: &mut [u8]) -> GetOutcome {
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        match self.read_latest(key, &mut payload) {
            ReadLatest::Absent => GetOutcome::Absent,
            ReadLatest::Fault => GetOutcome::Fault,
            ReadLatest::Found(n, meta) => {
                if meta.expiry_ms != 0 && now_ms >= meta.expiry_ms {
                    // Expired: absent to every reader. No physical
                    // reap — the version is shadowed by the next write
                    // and reclaimed by floor-respecting compaction.
                    return GetOutcome::Absent;
                }
                let vlen = n - DISK_META_LEN;
                if vlen > val_out.len() {
                    return GetOutcome::TooBig;
                }
                val_out[..vlen].copy_from_slice(&payload[DISK_META_LEN..n]);
                GetOutcome::Found {
                    value_len: vlen,
                    meta,
                }
            }
        }
    }

    fn insert_new(&mut self, key: &[u8], value: &[u8]) -> bool {
        if key.len() > MAX_KEY_LEN || value.len() > MAX_VALUE_LEN {
            return false; // mirror the memory provider's capacity gate
        }
        let ts = self.batch_ts();
        let meta = RecMeta {
            create_revision: ts,
            mod_revision: ts,
            version: 1,
            lease_id: 0,
            expiry_ms: 0,
            commit_ts: self.pending_commit_ts,
            flags: 0,
        };
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        encode_disk_meta(&mut payload, &meta);
        payload[DISK_META_LEN..DISK_META_LEN + value.len()].copy_from_slice(value);
        if !self.put_version(
            key,
            ts,
            ValueKind::Value,
            &payload[..DISK_META_LEN + value.len()],
        ) {
            return false;
        }
        self.revision = ts;
        self.batch_taken();
        self.note_key(ts, KEY_CREATED, key);
        true
    }

    fn update_existing(&mut self, key: &[u8], value: &[u8]) -> bool {
        if key.len() > MAX_KEY_LEN || value.len() > MAX_VALUE_LEN {
            return false;
        }
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        let old = match self.read_latest(key, &mut payload) {
            ReadLatest::Found(_, meta) => meta,
            _ => return false, // caller observed it live; fail closed
        };
        let ts = self.batch_ts();
        let meta = RecMeta {
            create_revision: old.create_revision,
            mod_revision: ts,
            version: old.version.saturating_add(1),
            lease_id: old.lease_id,
            expiry_ms: old.expiry_ms,
            commit_ts: self.pending_commit_ts,
            // A write states its value afresh (see `update_value`).
            flags: 0,
        };
        encode_disk_meta(&mut payload, &meta);
        payload[DISK_META_LEN..DISK_META_LEN + value.len()].copy_from_slice(value);
        if !self.put_version(
            key,
            ts,
            ValueKind::Value,
            &payload[..DISK_META_LEN + value.len()],
        ) {
            return false;
        }
        self.revision = ts;
        self.batch_taken();
        self.note_key(ts, KEY_MODIFIED, key);
        true
    }

    fn set_expiry(&mut self, key: &[u8], expiry_ms: u64) -> bool {
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        let (n, meta) = match self.read_latest(key, &mut payload) {
            ReadLatest::Found(n, meta) => (n, meta),
            _ => return false,
        };
        if meta.expiry_ms == expiry_ms {
            return true; // already the requested deadline
        }
        // Rewrite the SAME version (same encoded key: identical
        // timestamp and kind) with the new deadline — the provider
        // treats an exact-key overwrite as deterministic replay.
        // expiry_ms sits at meta offsets 38..46 (commit_ts follows it).
        payload[38..46].copy_from_slice(&expiry_ms.to_le_bytes());
        self.put_version(key, meta.mod_revision, ValueKind::Value, &payload[..n])
    }

    fn set_lease(&mut self, key: &[u8], lease_id: u64) -> bool {
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        let (n, meta) = match self.read_latest(key, &mut payload) {
            ReadLatest::Found(n, meta) => (n, meta),
            _ => return false,
        };
        if meta.lease_id == lease_id {
            return true;
        }
        // The same version rewritten, as `set_expiry` does; the lease
        // sits at meta offsets 30..38.
        payload[30..38].copy_from_slice(&lease_id.to_le_bytes());
        self.put_version(key, meta.mod_revision, ValueKind::Value, &payload[..n])
    }

    fn set_flags(&mut self, key: &[u8], flags: u8) -> bool {
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        let (n, meta) = match self.read_latest(key, &mut payload) {
            ReadLatest::Found(n, meta) => (n, meta),
            _ => return false,
        };
        if meta.flags == flags {
            return true;
        }
        // The same version rewritten; the flags sit at meta offset 2.
        payload[2] = flags;
        self.put_version(key, meta.mod_revision, ValueKind::Value, &payload[..n])
    }

    fn put_meta(&mut self, key: &[u8], value: &[u8]) -> bool {
        if key.len() > MAX_KEY_LEN || value.len() > MAX_VALUE_LEN {
            return false;
        }
        let ts = self.batch_ts();
        let meta = RecMeta {
            create_revision: ts,
            mod_revision: ts,
            version: 1,
            lease_id: 0,
            expiry_ms: 0,
            commit_ts: 0,
            flags: 0,
        };
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        encode_disk_meta(&mut payload, &meta);
        payload[DISK_META_LEN..DISK_META_LEN + value.len()].copy_from_slice(value);
        if !self.put_version(
            key,
            ts,
            ValueKind::Value,
            &payload[..DISK_META_LEN + value.len()],
        ) {
            return false;
        }
        self.revision = ts;
        self.batch_taken();
        true
    }

    fn remove_meta(&mut self, key: &[u8]) -> bool {
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        match self.read_latest(key, &mut payload) {
            ReadLatest::Absent => true,
            ReadLatest::Fault => false,
            ReadLatest::Found(..) => {
                let ts = self.batch_ts();
                if !self.put_version(key, ts, ValueKind::PointTombstone, &[]) {
                    return false;
                }
                self.revision = ts;
                self.batch_taken();
                true
            }
        }
    }

    fn batch_open(&self) -> bool {
        self.batch != 0
    }

    fn op_revision(&self) -> u64 {
        self.batch_ts()
    }

    fn remove(&mut self, key: &[u8], now_ms: u64) -> Option<bool> {
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        match self.read_latest(key, &mut payload) {
            ReadLatest::Absent => Some(false),
            ReadLatest::Fault => None,
            ReadLatest::Found(_, meta) if meta.expiry_ms != 0 && now_ms >= meta.expiry_ms => {
                Some(false) // expired = logically gone already
            }
            ReadLatest::Found(_, _) => {
                // Tombstone at the revision the interpreter's
                // batch-end bump will publish (DELETE bumps once for
                // the whole op, matching the memory provider).
                //
                // A timestamped delete carries its commit timestamp as
                // the tombstone's 8-byte payload — the
                // one version kind with no meta prefix to hold it. An
                // untimestamped delete stays empty, byte-identical to
                // the historical layout.
                let ts = self.batch_ts();
                let cts = self.pending_commit_ts;
                let payload_bytes = cts.to_le_bytes();
                let tomb: &[u8] = if cts != 0 { &payload_bytes } else { &[] };
                if self.put_version(key, ts, ValueKind::PointTombstone, tomb) {
                    self.note_key(ts, KEY_DELETED, key);
                    Some(true)
                } else {
                    None
                }
            }
        }
    }

    fn scan_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize) {
        let mut off = 0;
        let cursor = read_u64(body, &mut off).unwrap_or(0);
        let limit = read_u16(body, &mut off).unwrap_or(64);
        // As-of "before the first committed write" is empty (the
        // provider can't express revision 0; see `read_revision`).
        let empty = self.in_replay() && self.revision == 0;
        self.scan_from(self.read_revision(), empty, cursor, limit, out, now_ms)
    }

    fn range_scan_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize) {
        let Some(q) = parse_range_scan_body(body) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let empty = self.in_replay() && self.revision == 0;
        self.range_from(self.read_revision(), empty, &q, out, now_ms)
    }

    fn walk_live_meta<F: FnMut(&[u8], &[u8], &RecMeta) -> bool>(
        &mut self,
        start: &[u8],
        end: &[u8],
        at_rev: u64,
        now_ms: u64,
        mut f: F,
    ) -> u8 {
        if self.in_replay() && self.revision == 0 {
            return KV_RESULT_OK;
        }
        let at = if at_rev == 0 {
            self.read_revision()
        } else {
            at_rev
        };
        let (it, id, ik) = self.key_identity;
        let (_, ident_hi) = self.ident_span();
        let mut startbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let mut endbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let Some(slen) = internal_key::encode_prefix(&mut startbuf, it, id, ik, start) else {
            return KV_RESULT_INTERNAL;
        };
        // An empty `end` is unbounded above WITHIN this identity (see
        // `range_from`).
        let elen = if end.is_empty() {
            match ident_hi {
                Some(hi) => {
                    endbuf[..IDENT_LEN].copy_from_slice(&hi);
                    IDENT_LEN
                }
                None => 0,
            }
        } else {
            match internal_key::encode_prefix(&mut endbuf, it, id, ik, end) {
                Some(n) => n,
                None => return KV_RESULT_INTERNAL,
            }
        };
        let span = disk_store::state_store::KeySpan {
            start: &startbuf[..slen],
            end: &endbuf[..elen],
        };
        let mut resume = 0u64;
        let mut sbuf = [0u8; DISK_SCAN_BUF];
        loop {
            let p = match self.store.scan_at(span, at, resume, 64, &mut sbuf) {
                Ok(p) => p,
                Err(disk_store::state_store::StoreError::Compacted) => return KV_RESULT_COMPACTED,
                Err(_) => {
                    self.fault = true;
                    return KV_RESULT_INTERNAL;
                }
            };
            let mut at_off = 0usize;
            for _ in 0..p.entries {
                let klen = u16::from_le_bytes([sbuf[at_off], sbuf[at_off + 1]]) as usize;
                let vlen = u32::from_le_bytes([
                    sbuf[at_off + 2],
                    sbuf[at_off + 3],
                    sbuf[at_off + 4],
                    sbuf[at_off + 5],
                ]) as usize;
                at_off += 6;
                let key = &sbuf[at_off..at_off + klen];
                let val = &sbuf[at_off + klen..at_off + klen + vlen];
                at_off += klen + vlen;
                resume += 1;
                let mut ubuf = [0u8; MAX_KEY_LEN];
                let Some(dec) = internal_key::decode(key, &mut ubuf) else {
                    self.fault = true;
                    return KV_RESULT_INTERNAL;
                };
                let Some(meta) = decode_disk_meta(val) else {
                    self.fault = true;
                    return KV_RESULT_INTERNAL;
                };
                if meta.expiry_ms != 0 && now_ms >= meta.expiry_ms {
                    continue;
                }
                if !f(&ubuf[..dec.user_key_len], &val[DISK_META_LEN..], &meta) {
                    return KV_RESULT_OK;
                }
            }
            match p.progress {
                Progress::Done => return KV_RESULT_OK,
                Progress::InProgress { .. } => {
                    if p.entries == 0 {
                        self.fault = true;
                        return KV_RESULT_INTERNAL;
                    }
                }
            }
        }
    }

    fn walk_sorted(&self) -> bool {
        true
    }

    fn scan_versions_op(&mut self, body: &[u8], out: &mut [u8]) -> (u8, usize) {
        let Some(w) = parse_scan_versions_body(body) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        if out.len() < 10 {
            return (KV_RESULT_INTERNAL, 0);
        }
        let (it, id, ik) = self.key_identity;
        let (_, ident_hi) = self.ident_span();
        let mut startbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let mut endbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let Some(slen) = internal_key::encode_prefix(&mut startbuf, it, id, ik, w.start) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        // An empty end is unbounded WITHIN this identity, capped at the
        // next identity's prefix so the walk cannot cross into another
        // tenant. If there is no successor (all-`0xFF`) it stays
        // unbounded, which for a real identity never arises.
        let elen = if w.end.is_empty() {
            match ident_hi {
                Some(hi) => {
                    endbuf[..IDENT_LEN].copy_from_slice(&hi);
                    IDENT_LEN
                }
                None => 0,
            }
        } else {
            match internal_key::encode_prefix(&mut endbuf, it, id, ik, w.end) {
                Some(n) => n,
                None => return (KV_RESULT_INTERNAL, 0),
            }
        };
        let span = disk_store::state_store::KeySpan {
            start: &startbuf[..slen],
            end: &endbuf[..elen],
        };
        // `to = 0` means "up to now", and "now" is PINNED at the
        // window's open to the highest revision materialized: the walk
        // may pause across steps with writes landing in between, and a
        // window whose ceiling moved with them would count its ordinals
        // against a different set of records on every call. While the
        // provider's scan memo holds the window — a re-drive of a
        // paused page, or the next page — an unversioned request
        // resolves to the ceiling it was opened with. The pin is memo-
        // scoped, so a caller that will retry or page past the walk's
        // completion must name the ceiling explicitly; it is reported
        // as the reply's revision for exactly that. A first page opens
        // a new window at "now" unless it is its own paused re-drive.
        let hi = if w.to != 0 {
            w.to
        } else if let Some(pinned) = self.store.state.scan_window_ceiling(span, w.from, w.cursor) {
            pinned
        } else if self.in_replay() {
            self.revision
        } else {
            self.store.state.highest_revision
        };
        self.reply_revision = hi;
        let page_key = disk_store::ScanPageKey {
            from: w.from,
            hi,
            ordinal: w.cursor,
            limit: w.limit,
            shape: w.flags,
        };

        let mut out_off = 10usize;
        let mut emitted: u16 = 0;
        let mut resume = w.cursor;
        let mut done = false;
        // A re-drive of a paused request takes back the prefix of the
        // page already transcoded before the pause and continues it; a
        // retry of a request already answered (the reply lost to a
        // timeout upstream) is answered again from the parked page,
        // without moving the walk.
        if let Some((parked, n, complete, next_cursor)) =
            self.store
                .state
                .scan_page_take(span, &page_key, &mut out[10..])
        {
            if complete {
                out[0..8].copy_from_slice(&next_cursor.to_le_bytes());
                out[8..10].copy_from_slice(&parked.to_le_bytes());
                return (KV_RESULT_VERSIONS, 10 + n);
            }
            emitted = parked;
            out_off += n;
            resume = w.cursor + u64::from(parked);
        }
        // The page is capped at what can be parked across a pause, so
        // parking can never refuse. Each store request is sized to the
        // room left in the page, so a record the store hands over
        // always fits; one that would not is pulled but STASHED in the
        // provider (`scan.pending_*`) with the ordinal held at the
        // page's, so the next call yields it first and the cursor never
        // runs ahead of the caller. A transcoded
        // entry is at least `VERSIONS_TRANSCODE_SLACK` bytes smaller
        // than its raw record, so that much is added to the room
        // without risk. When the room is smaller than one raw record
        // the page closes — unless it is still empty, in which case
        // the record is fetched whole and refused only if it truly
        // does not fit (a reply buffer below the entry ceiling: a
        // caller-sizing error, not a paging state).
        let cap = out.len().min(10 + disk_store::SCAN_PAGE_MAX);
        let mut sbuf = [0u8; DISK_SCAN_BUF];
        let mut whole = false;
        'outer: while !done && emitted < w.limit {
            let room = if whole {
                DISK_SCAN_BUF
            } else {
                (cap - out_off + VERSIONS_TRANSCODE_SLACK).min(DISK_SCAN_BUF)
            };
            // A whole-buffer fetch is for exactly ONE record: the page
            // has no room for a second, and a pulled record must land.
            let take = if whole {
                1
            } else {
                usize::from(w.limit - emitted)
            };
            let p =
                match self
                    .store
                    .scan_versions(span, w.from, hi, resume, take, &mut sbuf[..room])
                {
                    Ok(p) => p,
                    Err(disk_store::state_store::StoreError::Compacted) => {
                        return (KV_RESULT_COMPACTED, 0)
                    }
                    Err(disk_store::state_store::StoreError::OutputTooSmall) if out_off > 10 => {
                        // The next record does not fit this page; the
                        // provider stashed it against the held ordinal,
                        // and the next page starts with it. This page
                        // closes here.
                        break 'outer;
                    }
                    Err(disk_store::state_store::StoreError::OutputTooSmall) if !whole => {
                        whole = true;
                        continue 'outer;
                    }
                    Err(_) => {
                        self.fault = true;
                        return (KV_RESULT_INTERNAL, 0);
                    }
                };
            whole = false;
            let mut at = 0usize;
            for _ in 0..p.entries {
                let klen = u16::from_le_bytes([sbuf[at], sbuf[at + 1]]) as usize;
                let vlen =
                    u32::from_le_bytes([sbuf[at + 2], sbuf[at + 3], sbuf[at + 4], sbuf[at + 5]])
                        as usize;
                at += 6;
                let key = &sbuf[at..at + klen];
                let val = &sbuf[at + klen..at + klen + vlen];
                at += klen + vlen;
                resume += 1;
                let mut ubuf = [0u8; MAX_KEY_LEN];
                let Some(dec) = internal_key::decode(key, &mut ubuf) else {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                };
                // Expiry is deliberately NOT filtered here. A version is
                // a historical fact: the write happened at its revision
                // regardless of whether the record has since expired,
                // and dropping it would make a resumed stream disagree
                // with the continuous one that delivered it live.
                let is_tomb = dec.kind == internal_key::ValueKind::PointTombstone;
                let (kind, commit_ts, uval, rec_meta) = if is_tomb {
                    // A timestamped tombstone carries its commit
                    // timestamp as an 8-byte payload;
                    // the emitted delete version has no user value
                    // either way.
                    let cts = if val.len() == 8 {
                        u64::from_le_bytes([
                            val[0], val[1], val[2], val[3], val[4], val[5], val[6], val[7],
                        ])
                    } else {
                        0
                    };
                    (VERSION_KIND_DELETE, cts, &[][..], None)
                } else {
                    match decode_disk_meta(val) {
                        Some(meta) => (
                            VERSION_KIND_PUT,
                            meta.commit_ts,
                            &val[DISK_META_LEN..],
                            Some(meta),
                        ),
                        None => {
                            self.fault = true;
                            return (KV_RESULT_INTERNAL, 0);
                        }
                    }
                };
                let ulen = dec.user_key_len;
                // With META, a put's entry grows by its 24-byte metadata:
                // well inside a put's transcode slack (raw − entry ≥ 60).
                let meta_len = if w.meta && rec_meta.is_some() { 24 } else { 0 };
                let rec_flags = rec_meta.map_or(0, |m| m.flags);
                if rec_flags & RECORD_MANIFEST != 0 && !w.manifests {
                    // A manifest's bytes are not its value: a reader that
                    // does not read manifests is refused. The record was
                    // pulled, so the walk's position is forgotten.
                    self.store.state.scan_reset();
                    return (KV_RESULT_MANIFEST, 0);
                }
                let flags_len = usize::from(w.manifests);
                let need = 8 + 8 + 1 + 2 + ulen + 4 + uval.len() + meta_len + flags_len;
                if out_off + need > cap {
                    // Only reachable for a page that cannot hold even
                    // one record (a caller buffer smaller than the
                    // entry ceiling). The record has been pulled, so the
                    // store's position is a record ahead of any cursor
                    // the caller holds: forget it, and refuse.
                    self.store.state.scan_reset();
                    return (KV_RESULT_INTERNAL, 0);
                }
                out[out_off..out_off + 8].copy_from_slice(&dec.mvcc_timestamp.to_le_bytes());
                out_off += 8;
                out[out_off..out_off + 8].copy_from_slice(&commit_ts.to_le_bytes());
                out_off += 8;
                out[out_off] = kind;
                out_off += 1;
                out[out_off..out_off + 2].copy_from_slice(&(ulen as u16).to_le_bytes());
                out_off += 2;
                out[out_off..out_off + ulen].copy_from_slice(&ubuf[..ulen]);
                out_off += ulen;
                out[out_off..out_off + 4].copy_from_slice(&(uval.len() as u32).to_le_bytes());
                out_off += 4;
                out[out_off..out_off + uval.len()].copy_from_slice(uval);
                out_off += uval.len();
                if let (true, Some(m)) = (meta_len != 0, rec_meta) {
                    for v in [m.create_revision, m.version, m.lease_id] {
                        out[out_off..out_off + 8].copy_from_slice(&v.to_le_bytes());
                        out_off += 8;
                    }
                }
                if w.manifests {
                    out[out_off] = rec_flags;
                    out_off += 1;
                }
                emitted += 1;
                if emitted >= w.limit {
                    break 'outer;
                }
            }
            match p.progress {
                Progress::Done => done = true,
                Progress::InProgress { .. } if p.paused => {
                    // Step budget spent mid-page. Park what has been
                    // transcoded so far under this request's cursor and
                    // hand the command back to the host unanswered; it
                    // re-drives the same command next step, the parked
                    // prefix is taken back above, and the provider
                    // continues from where it stood.
                    self.store.state.scan_page_park(
                        &page_key,
                        emitted,
                        &out[10..out_off],
                        false,
                        0,
                    );
                    return (KV_RESULT_PAUSED, 0);
                }
                Progress::InProgress { .. } => {
                    if p.entries == 0 {
                        self.fault = true;
                        return (KV_RESULT_INTERNAL, 0);
                    }
                }
            }
        }
        let new_cursor: u64 = if done { 0 } else { resume };
        out[0..8].copy_from_slice(&new_cursor.to_le_bytes());
        out[8..10].copy_from_slice(&emitted.to_le_bytes());
        self.store
            .state
            .scan_page_park(&page_key, emitted, &out[10..out_off], true, new_cursor);
        (KV_RESULT_VERSIONS, out_off)
    }

    fn snapshot_versions_op(&mut self, body: &[u8], out: &mut [u8], now_ms: u64) -> (u8, usize) {
        let Some((at_rev, q)) = parse_snapshot_versions_body(body) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        if out.len() < 10 {
            return (KV_RESULT_INTERNAL, 0);
        }
        // As-of semantics follow `range_from`: 0 = latest (or the
        // replay-window as-of), explicit revisions answered from
        // retained history or refused COMPACTED by the store.
        let at = if at_rev != 0 {
            at_rev
        } else {
            self.read_revision()
        };
        let force_empty = at_rev == 0 && self.in_replay() && self.revision == 0;
        let (it, id, ik) = self.key_identity;
        let (_, ident_hi) = self.ident_span();
        let mut startbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let mut endbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let Some(slen) = internal_key::encode_prefix(&mut startbuf, it, id, ik, q.start) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let elen = if q.end.is_empty() {
            match ident_hi {
                Some(hi) => {
                    endbuf[..IDENT_LEN].copy_from_slice(&hi);
                    IDENT_LEN
                }
                None => 0,
            }
        } else {
            match internal_key::encode_prefix(&mut endbuf, it, id, ik, q.end) {
                Some(n) => n,
                None => return (KV_RESULT_INTERNAL, 0),
            }
        };
        let span = disk_store::state_store::KeySpan {
            start: &startbuf[..slen],
            end: &endbuf[..elen],
        };

        let mut out_off = 10usize;
        let mut emitted: u16 = 0;
        let mut resume = q.cursor;
        let mut done = force_empty;
        let mut sbuf = [0u8; DISK_SCAN_BUF];
        'outer: while !done && emitted < q.limit {
            let p = match self.store.scan_at(
                span,
                at,
                resume,
                usize::from(q.limit - emitted),
                &mut sbuf,
            ) {
                Ok(p) => p,
                Err(disk_store::state_store::StoreError::Compacted) => {
                    return (KV_RESULT_COMPACTED, 0)
                }
                Err(_) => {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                }
            };
            let mut at_b = 0usize;
            for _ in 0..p.entries {
                let klen = u16::from_le_bytes([sbuf[at_b], sbuf[at_b + 1]]) as usize;
                let vlen = u32::from_le_bytes([
                    sbuf[at_b + 2],
                    sbuf[at_b + 3],
                    sbuf[at_b + 4],
                    sbuf[at_b + 5],
                ]) as usize;
                at_b += 6;
                let key = &sbuf[at_b..at_b + klen];
                let val = &sbuf[at_b + klen..at_b + klen + vlen];
                at_b += klen + vlen;
                resume += 1;
                let mut ubuf = [0u8; MAX_KEY_LEN];
                let Some(dec) = internal_key::decode(key, &mut ubuf) else {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                };
                let Some(meta) = decode_disk_meta(val) else {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                };
                if meta.expiry_ms != 0 && now_ms >= meta.expiry_ms {
                    continue; // expired — invisible, ordinal consumed
                }
                let uval = &val[DISK_META_LEN..];
                let ulen = dec.user_key_len;
                let need = 8 + 8 + 1 + 2 + ulen + 4 + uval.len();
                if out_off + need > out.len() {
                    resume -= 1; // not emitted — do not consume the ordinal
                    break 'outer;
                }
                out[out_off..out_off + 8].copy_from_slice(&meta.mod_revision.to_le_bytes());
                out_off += 8;
                out[out_off..out_off + 8].copy_from_slice(&meta.commit_ts.to_le_bytes());
                out_off += 8;
                out[out_off] = VERSION_KIND_PUT;
                out_off += 1;
                out[out_off..out_off + 2].copy_from_slice(&(ulen as u16).to_le_bytes());
                out_off += 2;
                out[out_off..out_off + ulen].copy_from_slice(&ubuf[..ulen]);
                out_off += ulen;
                out[out_off..out_off + 4].copy_from_slice(&(uval.len() as u32).to_le_bytes());
                out_off += 4;
                out[out_off..out_off + uval.len()].copy_from_slice(uval);
                out_off += uval.len();
                emitted += 1;
                if emitted >= q.limit {
                    break 'outer;
                }
            }
            match p.progress {
                Progress::Done => done = true,
                Progress::InProgress { .. } => {
                    if p.entries == 0 {
                        self.fault = true;
                        return (KV_RESULT_INTERNAL, 0);
                    }
                }
            }
        }
        let new_cursor: u64 = if done { 0 } else { resume };
        out[0..8].copy_from_slice(&new_cursor.to_le_bytes());
        out[8..10].copy_from_slice(&emitted.to_le_bytes());
        (KV_RESULT_VERSIONS, out_off)
    }

    fn flush_all(&mut self) -> bool {
        if self.fault {
            return false;
        }
        self.note_whole();
        // Tombstone every currently-visible key at revision + 1, then
        // publish the bump — the disk equivalent of the memory
        // provider's clear-plus-bump. Bounded by the provider's
        // logical capacity (≤ 8704 versions); worst-case cost is
        // documented on the worker's step budget.
        let revn = self.revision;
        let ts = self.revision.wrapping_add(1);
        // Same rule as the single-key delete: a timestamped tombstone
        // carries its commit timestamp as the 8-byte payload. A bare
        // tombstone decodes to commit_ts 0 on the versions surface, and
        // the CDC consumer contract discards any event at or below the
        // key's applied timestamp — a flush that dropped the stamp
        // would emit delete events every conforming consumer throws
        // away.
        let cts = self.pending_commit_ts;
        let cts_bytes = cts.to_le_bytes();
        let tomb: &[u8] = if cts != 0 { &cts_bytes } else { &[] };
        if revn > 0 {
            // FLUSH clears THIS identity's keys, not the whole
            // store — a tenant's flush must not tombstone another's.
            let (ident_lo, ident_hi) = self.ident_span();
            let hibuf = ident_hi.unwrap_or([0u8; IDENT_LEN]);
            let span = disk_store::state_store::KeySpan {
                start: &ident_lo[..],
                end: if ident_hi.is_some() { &hibuf[..] } else { b"" },
            };
            let mut resume = 0u64;
            let mut sbuf = [0u8; DISK_SCAN_BUF];
            loop {
                // Scan pinned at the pre-flush revision so the
                // tombstones we add (ts = revn + 1) stay invisible to
                // the walk and the visible-ordinal cursor stays stable.
                let p = match self
                    .store
                    .scan_at(span, revn, resume, usize::MAX, &mut sbuf)
                {
                    Ok(p) => p,
                    Err(_) => {
                        self.fault = true;
                        return false;
                    }
                };
                let mut at = 0usize;
                for _ in 0..p.entries {
                    let klen = u16::from_le_bytes([sbuf[at], sbuf[at + 1]]) as usize;
                    let vlen = u32::from_le_bytes([
                        sbuf[at + 2],
                        sbuf[at + 3],
                        sbuf[at + 4],
                        sbuf[at + 5],
                    ]) as usize;
                    at += 6;
                    let key = &sbuf[at..at + klen];
                    at += klen + vlen;
                    let mut ubuf = [0u8; MAX_KEY_LEN];
                    let Some(dec) = internal_key::decode(key, &mut ubuf) else {
                        self.fault = true;
                        return false;
                    };
                    if !self.put_version(
                        &ubuf[..dec.user_key_len],
                        ts,
                        ValueKind::PointTombstone,
                        tomb,
                    ) {
                        return false;
                    }
                    resume += 1;
                }
                match p.progress {
                    Progress::Done => break,
                    Progress::InProgress { .. } => {
                        if p.entries == 0 {
                            self.fault = true;
                            return false;
                        }
                    }
                }
            }
        }
        self.revision = ts;
        true
    }

    /// The disk provider retains every version above the committed GC
    /// floor it was last handed (the floor is trusted, never
    /// computed here).
    fn read_horizon(&self) -> u64 {
        self.store.state.compaction_floor
    }

    /// The committed GC floor: below it, floor-respecting compaction may
    /// have reclaimed a record, so its absence is `Indeterminate`.
    fn reclaim_floor(&self) -> u64 {
        self.store.state.compaction_floor
    }

    fn get_as_of(
        &mut self,
        key: &[u8],
        revision: u64,
        now_ms: u64,
        val_out: &mut [u8],
    ) -> HistOutcome {
        if revision == 0 {
            // Latest — take the exact `get_live` path so KV_OP_GET_AT
            // at revision 0 is byte-identical to KV_OP_GET.
            return match self.get_live(key, now_ms, val_out) {
                GetOutcome::Absent => HistOutcome::Absent,
                GetOutcome::Found { value_len, meta } => HistOutcome::Found { value_len, meta },
                GetOutcome::TooBig => HistOutcome::TooBig,
                GetOutcome::Fault => HistOutcome::Fault,
            };
        }
        if self.fault {
            return HistOutcome::Fault;
        }
        if revision < self.read_horizon() {
            return HistOutcome::Compacted;
        }
        let mut pbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let (it, id, ik) = self.key_identity;
        let Some(plen) = internal_key::encode_prefix(&mut pbuf, it, id, ik, key) else {
            self.fault = true;
            return HistOutcome::Fault;
        };
        let mut payload = [0u8; disk_store::MAX_VALUE_LEN];
        match self.store.get_at(&pbuf[..plen], revision, &mut payload) {
            Ok(n) => {
                let Some(meta) = decode_disk_meta(&payload[..n.min(DISK_META_LEN)]) else {
                    self.fault = true;
                    return HistOutcome::Fault;
                };
                if n < DISK_META_LEN {
                    self.fault = true;
                    return HistOutcome::Fault;
                }
                if meta.expiry_ms != 0 && now_ms >= meta.expiry_ms {
                    return HistOutcome::Absent;
                }
                let vlen = n - DISK_META_LEN;
                if vlen > val_out.len() {
                    return HistOutcome::TooBig;
                }
                val_out[..vlen].copy_from_slice(&payload[DISK_META_LEN..n]);
                HistOutcome::Found {
                    value_len: vlen,
                    meta,
                }
            }
            Err(StoreError::NotFound) => HistOutcome::Absent,
            // The floor can advance between the check above and here
            // (it does not advance mid-command, but the provider is the
            // authority either way) — carry its refusal through.
            Err(StoreError::Compacted) => HistOutcome::Compacted,
            Err(_) => {
                self.fault = true;
                HistOutcome::Fault
            }
        }
    }

    fn scan_as_of(
        &mut self,
        revision: u64,
        cursor: u64,
        limit: u16,
        out: &mut [u8],
        now_ms: u64,
    ) -> (u8, usize) {
        if revision == 0 {
            let empty = self.in_replay() && self.revision == 0;
            return self.scan_from(self.read_revision(), empty, cursor, limit, out, now_ms);
        }
        if revision < self.read_horizon() {
            return (KV_RESULT_COMPACTED, 0);
        }
        self.scan_from(revision, false, cursor, limit, out, now_ms)
    }
}

impl<S: RunStorage> DiskMaterializer<'_, S> {
    /// Span export for the elastic split, disk provider: stream the
    /// CURRENT record of every key in the half-open span `[start, end)`
    /// into the portable span-snapshot body
    /// ([`KvStore::snapshot_encode_span`]'s wire format, so the target
    /// installs it with no new decode path). Bounds are STORED-key bytes
    /// (12-byte identity ++ user key), exactly as the memory provider
    /// takes them; empty = unbounded.
    ///
    /// MVCC history does not travel: the destination partition's
    /// revision space is independent of the source's, so porting version
    /// history would be semantically wrong — a span moves as its current
    /// state, the same contract the memory provider ships. Historical
    /// reads of the moved span on the target answer from the move
    /// onward.
    ///
    /// Fails closed (`None`) on a provider fault, a corrupt record, or
    /// an out-of-budget body — a truncated span would silently drop keys
    /// the cutover then loses.
    pub fn snapshot_encode_span(
        &mut self,
        out: &mut [u8],
        clock_ms: u64,
        start: &[u8],
        end: &[u8],
    ) -> Option<usize> {
        let mut next = [0u8; MAX_KEY_LEN];
        match self.snapshot_encode_span_batch(out, clock_ms, start, end, &mut next)? {
            (n, None) => Some(n),
            (_, Some(_)) => None, // the span outgrows one body
        }
    }

    /// One batch of a span copy, disk provider: the CURRENT records of
    /// `[from, end)` in key order, as many as fit `out`, with the resume
    /// key (inclusive, STORED-key form) in `next` when more remain —
    /// [`KvStore::snapshot_encode_span_batch`]'s contract, body flag
    /// included.
    pub fn snapshot_encode_span_batch(
        &mut self,
        out: &mut [u8],
        clock_ms: u64,
        from: &[u8],
        end: &[u8],
        next: &mut [u8],
    ) -> Option<(usize, Option<usize>)> {
        if out.len() < SNAPSHOT_HDR_LEN {
            return None;
        }
        // A stored-key bound (identity ++ user key) converts to the
        // provider's encoded-key form: escaping is order-preserving and
        // the identity prefix rides fixed-width at the front of both
        // forms, so the half-open semantics carry over unchanged.
        fn encoded_bound(bound: &[u8], buf: &mut [u8]) -> Option<usize> {
            if bound.is_empty() {
                return Some(0);
            }
            if bound.len() < IDENT_LEN {
                return None;
            }
            let t = u32::from_be_bytes(bound[0..4].try_into().ok()?);
            let d = u32::from_be_bytes(bound[4..8].try_into().ok()?);
            let k = u32::from_be_bytes(bound[8..12].try_into().ok()?);
            internal_key::encode_prefix(buf, t, d, k, &bound[IDENT_LEN..])
        }
        let mut sb = [0u8; disk_store::MAX_ENCODED_KEY];
        let mut eb = [0u8; disk_store::MAX_ENCODED_KEY];
        let sn = encoded_bound(from, &mut sb)?;
        let en = encoded_bound(end, &mut eb)?;
        let span = disk_store::state_store::KeySpan {
            start: &sb[..sn],
            end: &eb[..en],
        };

        let mut p = SNAPSHOT_HDR_LEN;
        let mut count: u32 = 0;
        let mut more = None;
        let mut resume = 0u64;
        let mut sbuf = [0u8; DISK_SCAN_BUF];
        'scan: loop {
            let prog = match self.store.scan_at(span, 0, resume, usize::MAX, &mut sbuf) {
                Ok(v) => v,
                Err(_) => {
                    self.fault = true;
                    return None;
                }
            };
            let mut at = 0usize;
            for _ in 0..prog.entries {
                let klen = u16::from_le_bytes([sbuf[at], sbuf[at + 1]]) as usize;
                let vlen =
                    u32::from_le_bytes([sbuf[at + 2], sbuf[at + 3], sbuf[at + 4], sbuf[at + 5]])
                        as usize;
                at += 6;
                let key = &sbuf[at..at + klen];
                let val = &sbuf[at + klen..at + klen + vlen];
                at += klen + vlen;
                resume += 1;
                let mut ubuf = [0u8; MAX_KEY_LEN];
                let dec = internal_key::decode(key, &mut ubuf)?;
                let meta = decode_disk_meta(val)?;
                let user = &ubuf[..dec.user_key_len];
                let public = &val[DISK_META_LEN..];
                let stored_klen = IDENT_LEN + user.len();
                if stored_klen > MAX_KEY_LEN {
                    return None; // uninstallable on any target — refuse whole
                }
                let need = SNAPSHOT_REC_FIXED + stored_klen + public.len();
                if p + need > out.len() {
                    // The batch is full: the next one starts at this key.
                    if count == 0 || stored_klen > next.len() {
                        return None; // fail closed — never a truncated span
                    }
                    next[0..4].copy_from_slice(&dec.tenant.to_be_bytes());
                    next[4..8].copy_from_slice(&dec.database.to_be_bytes());
                    next[8..12].copy_from_slice(&dec.keyspace.to_be_bytes());
                    next[12..stored_klen].copy_from_slice(user);
                    more = Some(stored_klen);
                    break 'scan;
                }
                out[p..p + 2].copy_from_slice(&(stored_klen as u16).to_le_bytes());
                out[p + 2..p + 6].copy_from_slice(&(public.len() as u32).to_le_bytes());
                out[p + 6..p + 14].copy_from_slice(&meta.create_revision.to_le_bytes());
                out[p + 14..p + 22].copy_from_slice(&meta.mod_revision.to_le_bytes());
                out[p + 22..p + 30].copy_from_slice(&meta.version.to_le_bytes());
                out[p + 30..p + 38].copy_from_slice(&meta.lease_id.to_le_bytes());
                out[p + 38..p + 46].copy_from_slice(&meta.expiry_ms.to_le_bytes());
                out[p + 46] = meta.flags;
                p += SNAPSHOT_REC_FIXED;
                out[p..p + 4].copy_from_slice(&dec.tenant.to_be_bytes());
                out[p + 4..p + 8].copy_from_slice(&dec.database.to_be_bytes());
                out[p + 8..p + 12].copy_from_slice(&dec.keyspace.to_be_bytes());
                out[p + 12..p + 12 + user.len()].copy_from_slice(user);
                p += stored_klen;
                out[p..p + public.len()].copy_from_slice(public);
                p += public.len();
                count += 1;
            }
            match prog.progress {
                Progress::Done => break,
                Progress::InProgress { .. } => {
                    if prog.entries == 0 {
                        // No forward progress possible — refuse rather
                        // than spin.
                        return None;
                    }
                }
            }
        }
        out[0..4].copy_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
        out[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
        let flags = if more.is_some() {
            SNAPSHOT_FLAG_SPAN_MORE
        } else {
            0
        };
        out[6..8].copy_from_slice(&flags.to_le_bytes());
        out[8..16].copy_from_slice(&self.revision.to_le_bytes());
        out[16..20].copy_from_slice(&count.to_le_bytes());
        out[20..24].copy_from_slice(&0u32.to_le_bytes()); // reserved
        out[24..32].copy_from_slice(&clock_ms.to_le_bytes());
        Some((p, more))
    }

    /// The merged ordered walk behind both `scan_op` and `scan_as_of`.
    /// `at_rev` is the MVCC revision to serve at (`0` = latest);
    /// `force_empty` short-circuits the "nothing existed yet" case the
    /// provider cannot express as a revision.
    fn scan_from(
        &mut self,
        at_rev: u64,
        force_empty: bool,
        cursor: u64,
        limit: u16,
        out: &mut [u8],
        now_ms: u64,
    ) -> (u8, usize) {
        if out.len() < 10 {
            return (KV_RESULT_INTERNAL, 0);
        }
        let mut out_off = 10usize;
        let mut emitted: u16 = 0;
        // Provider ordinal of the next visible entry to inspect. The
        // cursor is provider-defined and opaque to clients, exactly as
        // the memory provider's slot cursor is.
        let mut resume = cursor;
        let mut done = force_empty;
        // The full SCAN is bounded to this identity's span, not the
        // whole store — otherwise a Redis SCAN would walk every tenant.
        let (ident_lo, ident_hi) = self.ident_span();
        let hibuf = ident_hi.unwrap_or([0u8; IDENT_LEN]);
        let span = disk_store::state_store::KeySpan {
            start: &ident_lo[..],
            end: if ident_hi.is_some() { &hibuf[..] } else { b"" },
        };
        let mut sbuf = [0u8; DISK_SCAN_BUF];
        'outer: while !done && emitted < limit {
            let p = match self.store.scan_at(
                span,
                at_rev,
                resume,
                usize::from(limit - emitted),
                &mut sbuf,
            ) {
                Ok(p) => p,
                Err(_) => {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                }
            };
            let mut at = 0usize;
            for _ in 0..p.entries {
                let klen = u16::from_le_bytes([sbuf[at], sbuf[at + 1]]) as usize;
                let vlen =
                    u32::from_le_bytes([sbuf[at + 2], sbuf[at + 3], sbuf[at + 4], sbuf[at + 5]])
                        as usize;
                at += 6;
                let key = &sbuf[at..at + klen];
                let val = &sbuf[at + klen..at + klen + vlen];
                at += klen + vlen;
                resume += 1;
                let mut ubuf = [0u8; MAX_KEY_LEN];
                let Some(dec) = internal_key::decode(key, &mut ubuf) else {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                };
                let Some(meta) = decode_disk_meta(val) else {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                };
                if meta.expiry_ms != 0 && now_ms >= meta.expiry_ms {
                    continue; // expired — invisible, ordinal consumed
                }
                let ulen = dec.user_key_len;
                if out_off + 4 + ulen > out.len() {
                    break 'outer;
                }
                out[out_off..out_off + 4].copy_from_slice(&(ulen as u32).to_le_bytes());
                out_off += 4;
                out[out_off..out_off + ulen].copy_from_slice(&ubuf[..ulen]);
                out_off += ulen;
                emitted += 1;
                if emitted >= limit {
                    break 'outer;
                }
            }
            match p.progress {
                Progress::Done => done = true,
                Progress::InProgress { .. } => {
                    if p.entries == 0 {
                        // No forward progress possible (an entry larger
                        // than the staging buffer would be a provider
                        // bound violation) — fail closed, never spin.
                        self.fault = true;
                        return (KV_RESULT_INTERNAL, 0);
                    }
                }
            }
        }
        let new_cursor: u64 = if done { 0 } else { resume };
        out[0..8].copy_from_slice(&new_cursor.to_le_bytes());
        out[8..10].copy_from_slice(&emitted.to_le_bytes());
        (KV_RESULT_SCAN_CURSOR, out_off)
    }

    /// The merged ordered walk behind `range_scan_op`: `[start, end)`
    /// over user keys, emitting key AND value.
    ///
    /// Unlike the memory provider this genuinely SEEKS — `span.start`
    /// is handed to the merge iterator, which positions each run by its
    /// sparse block index instead of reading from the beginning. That
    /// is the property a relational table scan needs: cost proportional
    /// to the table, not to the database.
    ///
    /// Both bounds are encoded through `internal_key::encode_prefix`,
    /// which is what makes the half-open comparison meaningful. The
    /// prefix of `end` sorts strictly BELOW every version of `end`
    /// itself (the version suffix follows the terminator), so comparing
    /// encoded keys against it excludes `end` exactly as an exclusive
    /// bound must.
    fn range_from(
        &mut self,
        at_rev: u64,
        force_empty: bool,
        q: &RangeQuery<'_>,
        out: &mut [u8],
        now_ms: u64,
    ) -> (u8, usize) {
        let (start, end, cursor, limit) = (q.start, q.end, q.cursor, q.limit);
        if out.len() < 10 {
            return (KV_RESULT_INTERNAL, 0);
        }
        let (it, id, ik) = self.key_identity;
        let (_, ident_hi) = self.ident_span();
        let mut startbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let mut endbuf = [0u8; disk_store::MAX_ENCODED_KEY];
        let Some(slen) = internal_key::encode_prefix(&mut startbuf, it, id, ik, start) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        // An empty `end` is unbounded above WITHIN this identity, capped
        // at the next identity's prefix so the scan cannot cross into
        // another tenant — NOT the encoding of the empty key,
        // which would be a bound below every real key and yield nothing.
        let elen = if end.is_empty() {
            match ident_hi {
                Some(hi) => {
                    endbuf[..IDENT_LEN].copy_from_slice(&hi);
                    IDENT_LEN
                }
                None => 0,
            }
        } else {
            match internal_key::encode_prefix(&mut endbuf, it, id, ik, end) {
                Some(n) => n,
                None => return (KV_RESULT_INTERNAL, 0),
            }
        };
        let span = disk_store::state_store::KeySpan {
            start: &startbuf[..slen],
            end: &endbuf[..elen],
        };

        let mut out_off = 10usize;
        let mut emitted: u16 = 0;
        let mut resume = cursor;
        let mut done = force_empty;
        let mut sbuf = [0u8; DISK_SCAN_BUF];
        'outer: while !done && emitted < limit {
            let p = match self.store.scan_at(
                span,
                at_rev,
                resume,
                usize::from(limit - emitted),
                &mut sbuf,
            ) {
                Ok(p) => p,
                Err(disk_store::state_store::StoreError::Compacted) => {
                    return (KV_RESULT_COMPACTED, 0)
                }
                Err(_) => {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                }
            };
            let mut at = 0usize;
            for _ in 0..p.entries {
                let klen = u16::from_le_bytes([sbuf[at], sbuf[at + 1]]) as usize;
                let vlen =
                    u32::from_le_bytes([sbuf[at + 2], sbuf[at + 3], sbuf[at + 4], sbuf[at + 5]])
                        as usize;
                at += 6;
                let key = &sbuf[at..at + klen];
                let val = &sbuf[at + klen..at + klen + vlen];
                at += klen + vlen;
                resume += 1;
                let mut ubuf = [0u8; MAX_KEY_LEN];
                let Some(dec) = internal_key::decode(key, &mut ubuf) else {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                };
                let Some(meta) = decode_disk_meta(val) else {
                    self.fault = true;
                    return (KV_RESULT_INTERNAL, 0);
                };
                if meta.expiry_ms != 0 && now_ms >= meta.expiry_ms {
                    continue; // expired — invisible, ordinal consumed
                }
                let uval = &val[DISK_META_LEN..];
                let ulen = dec.user_key_len;
                let need = 2 + ulen + 4 + uval.len();
                if out_off + need > out.len() {
                    // Byte-bound: this entry is NOT emitted, so back the
                    // ordinal off by one or the next page would skip it.
                    resume -= 1;
                    break 'outer;
                }
                out[out_off..out_off + 2].copy_from_slice(&(ulen as u16).to_le_bytes());
                out_off += 2;
                out[out_off..out_off + ulen].copy_from_slice(&ubuf[..ulen]);
                out_off += ulen;
                out[out_off..out_off + 4].copy_from_slice(&(uval.len() as u32).to_le_bytes());
                out_off += 4;
                out[out_off..out_off + uval.len()].copy_from_slice(uval);
                out_off += uval.len();
                emitted += 1;
                if emitted >= limit {
                    break 'outer;
                }
            }
            match p.progress {
                Progress::Done => done = true,
                Progress::InProgress { .. } => {
                    if p.entries == 0 {
                        self.fault = true;
                        return (KV_RESULT_INTERNAL, 0);
                    }
                }
            }
        }
        let new_cursor: u64 = if done { 0 } else { resume };
        out[0..8].copy_from_slice(&new_cursor.to_le_bytes());
        out[8..10].copy_from_slice(&emitted.to_le_bytes());
        (KV_RESULT_RANGE, out_off)
    }
}

/// Parse a [`KV_OP_SNAPSHOT_VERSIONS`] body into
/// `(at_rev, RangeQuery)`. Same refusal rule as the range-scan parser.
fn parse_snapshot_versions_body(body: &[u8]) -> Option<(u64, RangeQuery<'_>)> {
    let mut off = 0usize;
    let at_rev = read_u64(body, &mut off)?;
    let start = read_key(body, &mut off)?;
    let end = read_key(body, &mut off)?;
    let cursor = read_u64(body, &mut off)?;
    let limit = read_u16(body, &mut off)?;
    Some((
        at_rev,
        RangeQuery {
            start,
            end,
            cursor,
            limit,
        },
    ))
}

/// Parse a [`KV_OP_RANGE_SCAN`] body into `(start, end, cursor, limit)`.
///
/// A malformed body is `None` rather than a defaulted span: guessing
/// the bounds of a range scan would silently widen it, and a scan that
/// returns more than it was asked for is worse than one that refuses.
fn parse_range_scan_body(body: &[u8]) -> Option<RangeQuery<'_>> {
    let mut off = 0usize;
    let start = read_key(body, &mut off)?;
    let end = read_key(body, &mut off)?;
    let cursor = read_u64(body, &mut off)?;
    let limit = read_u16(body, &mut off)?;
    Some(RangeQuery {
        start,
        end,
        cursor,
        limit,
    })
}

/// A decoded [`KV_OP_RANGE_SCAN`] request: the half-open span and where
/// to resume. Bundled rather than passed positionally because the four
/// fields travel together through every layer of the walk, and splitting
/// them across an argument list is where a start/end transposition hides.
struct RangeQuery<'a> {
    start: &'a [u8],
    end: &'a [u8],
    cursor: u64,
    limit: u16,
}

/// Parse a [`KV_OP_SCAN_VERSIONS`] body into
/// `(start, end, from_rev, to_rev, cursor, limit)`. Same refuse-rather-
/// than-default rule as `parse_range_scan_body`, and for a sharper
/// reason: a defaulted `from` of 0 would ask for the whole retained
/// history instead of the window the caller meant.
fn parse_scan_versions_body(body: &[u8]) -> Option<VersionWindow<'_>> {
    let mut off = 0usize;
    let start = read_key(body, &mut off)?;
    let end = read_key(body, &mut off)?;
    let from = read_u64(body, &mut off)?;
    let to = read_u64(body, &mut off)?;
    let cursor = read_u64(body, &mut off)?;
    let limit = read_u16(body, &mut off)?;
    let flags = read_u8(body, &mut off).unwrap_or(0);
    Some(VersionWindow {
        start,
        end,
        from,
        to,
        cursor,
        limit,
        meta: flags & VERSIONS_FLAG_META != 0,
        manifests: flags & VERSIONS_FLAG_MANIFESTS != 0,
        flags,
    })
}

/// A decoded [`KV_OP_SCAN_VERSIONS`] request: the key span, the
/// revision window, and where to resume.
struct VersionWindow<'a> {
    start: &'a [u8],
    end: &'a [u8],
    /// Exclusive lower bound.
    from: u64,
    /// Inclusive upper bound; `0` means "up to now".
    to: u64,
    cursor: u64,
    limit: u16,
    /// PUT entries carry their metadata ([`VERSIONS_FLAG_META`]).
    meta: bool,
    /// Every entry ends with its record's flags
    /// ([`VERSIONS_FLAG_MANIFESTS`]); without it a manifest is refused.
    manifests: bool,
    /// The request's flags byte: its entries' shape.
    flags: u8,
}

/// Resolve a window's upper bound: `0` means "up to now", matching the
/// `revision = 0 means latest` convention every other historical op
/// uses. A watcher resuming without a known ceiling wants everything
/// that has happened since it left, and that is what 0 asks for.
fn normalize_to(to: u64, current: u64) -> u64 {
    if to == 0 {
        current
    } else {
        to
    }
}

/// A parsed [`KV_OP_RANGE_META`] body.
struct RangeMetaQ<'a> {
    start: &'a [u8],
    end: &'a [u8],
    at_rev: u64,
    limit: u32,
    flags: u8,
    target: u8,
    order: u8,
    min_mod: u64,
    max_mod: u64,
    min_create: u64,
    max_create: u64,
}

fn parse_range_meta_body(body: &[u8]) -> Option<RangeMetaQ<'_>> {
    let mut off = 0usize;
    let start = read_key(body, &mut off)?;
    let end = read_key(body, &mut off)?;
    let at_rev = read_u64(body, &mut off)?;
    let limit = u32::from_le_bytes(body.get(off..off + 4)?.try_into().ok()?);
    off += 4;
    let flags = read_u8(body, &mut off)?;
    let target = read_u8(body, &mut off)?;
    let order = read_u8(body, &mut off)?;
    Some(RangeMetaQ {
        start,
        end,
        at_rev,
        limit,
        flags,
        target,
        order,
        min_mod: read_u64(body, &mut off)?,
        max_mod: read_u64(body, &mut off)?,
        min_create: read_u64(body, &mut off)?,
        max_create: read_u64(body, &mut off)?,
    })
}

/// The order a [`KV_OP_RANGE_META`] sorts by: `target` first (0 key,
/// 1 version, 2 create, 3 mod, 4 value), then the key.
fn range_meta_cmp(
    target: u8,
    a: (&[u8], &[u8], &RecMeta),
    b: (&[u8], &[u8], &RecMeta),
) -> core::cmp::Ordering {
    let primary = match target {
        1 => a.2.version.cmp(&b.2.version),
        2 => a.2.create_revision.cmp(&b.2.create_revision),
        3 => a.2.mod_revision.cmp(&b.2.mod_revision),
        4 => a.1.cmp(b.1),
        _ => core::cmp::Ordering::Equal,
    };
    primary.then(a.0.cmp(b.0))
}

/// One [`KV_RESULT_KVS`] entry at `out[*at..]`; false when it does not fit.
fn range_meta_put(
    out: &mut [u8],
    at: &mut usize,
    key: &[u8],
    val: &[u8],
    meta: &RecMeta,
    keys_only: bool,
) -> bool {
    range_meta_put_as(out, at, key, val, meta, keys_only, false)
}

/// [`range_meta_put`], with the record's flags after the entry when the
/// reader asked for them (it reads manifests).
fn range_meta_put_as(
    out: &mut [u8],
    at: &mut usize,
    key: &[u8],
    val: &[u8],
    meta: &RecMeta,
    keys_only: bool,
    with_flags: bool,
) -> bool {
    let val = if keys_only { &[][..] } else { val };
    let need = 2 + key.len() + 4 + val.len() + 32 + usize::from(with_flags);
    if *at + need > out.len() {
        return false;
    }
    let mut p = *at;
    out[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    out[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    out[p..p + 4].copy_from_slice(&(val.len() as u32).to_le_bytes());
    p += 4;
    out[p..p + val.len()].copy_from_slice(val);
    p += val.len();
    for v in [
        meta.create_revision,
        meta.mod_revision,
        meta.version,
        meta.lease_id,
    ] {
        out[p..p + 8].copy_from_slice(&v.to_le_bytes());
        p += 8;
    }
    if with_flags {
        out[p] = meta.flags;
        p += 1;
    }
    *at = p;
    true
}

/// [`KV_OP_RANGE_META`]: count every match, then emit them in the asked
/// order — one streaming pass when the provider already walks in it (the
/// disk provider, ascending keys), else by repeated selection (each pass
/// finds the next entry after the last one emitted; cheap over the
/// memory provider's table, and bounded on any provider by the answer's
/// buffer).
fn apply_range_meta<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let Some(q) = parse_range_meta_body(body) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    if out.len() < 11 {
        return (KV_RESULT_INTERNAL, 0);
    }
    if q.at_rev != 0 && q.at_rev > store.revision() {
        return (KV_RESULT_FUTURE_REV, 0);
    }
    // etcd's spans: no end = the one key; `\0` = every key from start.
    let mut succ = [0u8; MAX_KEY_LEN + 1];
    let (start, end): (&[u8], &[u8]) = if q.end.is_empty() {
        let n = q.start.len();
        if n >= succ.len() {
            return (KV_RESULT_INTERNAL, 0);
        }
        succ[..n].copy_from_slice(q.start);
        succ[n] = 0;
        (q.start, &succ[..n + 1])
    } else if q.end == [0] {
        (q.start, &[])
    } else {
        (q.start, q.end)
    };
    let passes = |m: &RecMeta| {
        (q.min_mod == 0 || m.mod_revision >= q.min_mod)
            && (q.max_mod == 0 || m.mod_revision <= q.max_mod)
            && (q.min_create == 0 || m.create_revision >= q.min_create)
            && (q.max_create == 0 || m.create_revision <= q.max_create)
    };
    let mut total = 0u64;
    let r = store.walk_live_meta(start, end, q.at_rev, now_ms, |_, _, m| {
        if passes(m) {
            total += 1;
        }
        true
    });
    if r != KV_RESULT_OK {
        return (r, 0);
    }
    let keys_only = q.flags & 1 != 0;
    let count_only = q.flags & 2 != 0;
    // A reader that reads manifests gets each entry's flags; one that
    // does not is refused a manifest's bytes (keys and counts are fine).
    let manifests = q.flags & RANGE_META_FLAG_MANIFESTS != 0;
    let mut refused = false;
    let limit = if q.limit == 0 {
        u64::MAX
    } else {
        u64::from(q.limit)
    };
    let mut at = 11usize;
    let mut emitted = 0u64;
    let desc = q.order == 2;
    if !count_only && q.target == 0 && !desc && store.walk_sorted() {
        let r = store.walk_live_meta(start, end, q.at_rev, now_ms, |k, v, m| {
            if !passes(m) {
                return true;
            }
            if m.flags & RECORD_MANIFEST != 0 && !manifests && !keys_only {
                refused = true;
                return false;
            }
            if emitted >= limit || !range_meta_put_as(out, &mut at, k, v, m, keys_only, manifests) {
                return false;
            }
            emitted += 1;
            true
        });
        if r != KV_RESULT_OK {
            return (r, 0);
        }
    } else if !count_only {
        let mut last_key = [0u8; MAX_KEY_LEN];
        let mut last_val = [0u8; MAX_VALUE_LEN];
        let mut last: Option<(usize, usize, RecMeta)> = None;
        let mut best_key = [0u8; MAX_KEY_LEN];
        let mut best_val = [0u8; MAX_VALUE_LEN];
        while emitted < limit {
            let mut best: Option<(usize, usize, RecMeta)> = None;
            let r = store.walk_live_meta(start, end, q.at_rev, now_ms, |k, v, m| {
                if !passes(m) || k.len() > MAX_KEY_LEN || v.len() > MAX_VALUE_LEN {
                    return true;
                }
                // Refused as above; and a large value's bytes are not its
                // value, so it cannot be sorted by them.
                if m.flags & RECORD_MANIFEST != 0 && ((!manifests && !keys_only) || q.target == 4) {
                    refused = true;
                    return false;
                }
                let e = (k, v, m);
                let fwd = |o: core::cmp::Ordering| if desc { o.reverse() } else { o };
                if let Some((kl, vl, lm)) = last {
                    let after = fwd(range_meta_cmp(
                        q.target,
                        e,
                        (&last_key[..kl], &last_val[..vl], &lm),
                    ));
                    if after != core::cmp::Ordering::Greater {
                        return true;
                    }
                }
                let better = match best {
                    None => true,
                    Some((kl, vl, bm)) => {
                        fwd(range_meta_cmp(
                            q.target,
                            e,
                            (&best_key[..kl], &best_val[..vl], &bm),
                        )) == core::cmp::Ordering::Less
                    }
                };
                if better {
                    best_key[..k.len()].copy_from_slice(k);
                    best_val[..v.len()].copy_from_slice(v);
                    best = Some((k.len(), v.len(), *m));
                }
                true
            });
            if r != KV_RESULT_OK {
                return (r, 0);
            }
            let Some((kl, vl, bm)) = best else {
                break;
            };
            if refused {
                break;
            }
            if !range_meta_put_as(
                out,
                &mut at,
                &best_key[..kl],
                &best_val[..vl],
                &bm,
                keys_only,
                manifests,
            ) {
                break;
            }
            emitted += 1;
            last_key[..kl].copy_from_slice(&best_key[..kl]);
            last_val[..vl].copy_from_slice(&best_val[..vl]);
            last = Some((kl, vl, bm));
        }
    }
    if refused {
        return (KV_RESULT_MANIFEST, 0);
    }
    // Cut by the limit, or by the answer's space before the limit.
    let more = if count_only || emitted >= total {
        0
    } else if emitted < total.min(limit) {
        KVS_MORE_PARTIAL
    } else {
        KVS_MORE_LIMIT
    };
    out[0..8].copy_from_slice(&total.to_le_bytes());
    out[8] = more;
    out[9..11].copy_from_slice(&(emitted as u16).to_le_bytes());
    (KV_RESULT_KVS, at)
}

fn apply_get<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(key) = read_key(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    // A key under an unresolved cross-range intent has no answer yet.
    // Name the deciding authority and refuse retryably rather than
    // serve the pre-intent value (a stale read) or absent (an invented
    // delete). See `KV_RESULT_TXN_PENDING`.
    if let Some((id, home)) = intent_on(store, key, now_ms) {
        return pending_body(out, id, home);
    }
    let mut scratch = [0u8; MAX_VALUE_LEN];
    match store.get_live(key, now_ms, &mut scratch) {
        GetOutcome::Absent => (KV_RESULT_NOT_FOUND, 0),
        GetOutcome::Found { value_len: n, meta } => {
            if n > out.len() {
                return (KV_RESULT_INTERNAL, 0);
            }
            out[..n].copy_from_slice(&scratch[..n]);
            // A manifest is answered as one: the reader fetches its
            // chunks, and a reader that does not is not misled.
            let code = if meta.flags & RECORD_MANIFEST != 0 {
                KV_RESULT_MANIFEST
            } else {
                KV_RESULT_OK
            };
            (code, n)
        }
        GetOutcome::TooBig | GetOutcome::Fault => (KV_RESULT_INTERNAL, 0),
    }
}

/// `KV_OP_GET_AT` — body `[revision:u64 LE][key_len:u16 LE][key…]`.
fn apply_get_at<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(revision) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    apply_get_revision(store, &body[off..], out, now_ms, revision)
}

/// The historical point read itself, shared by `KV_OP_GET_AT` (which
/// parses the revision out of its body) and by a `Consistency::
/// Snapshot` `KV_OP_GET` (which takes it from the request context).
/// `body` is the `[key_len:u16 LE][key…]` shape in both cases.
fn apply_get_revision<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
    revision: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(key) = read_key(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut scratch = [0u8; MAX_VALUE_LEN];
    match store.get_as_of(key, revision, now_ms, &mut scratch) {
        HistOutcome::Absent => (KV_RESULT_NOT_FOUND, 0),
        HistOutcome::Found { value_len: n, meta } => {
            if n > out.len() {
                return (KV_RESULT_INTERNAL, 0);
            }
            out[..n].copy_from_slice(&scratch[..n]);
            let code = if meta.flags & RECORD_MANIFEST != 0 {
                KV_RESULT_MANIFEST
            } else {
                KV_RESULT_OK
            };
            (code, n)
        }
        // The whole point of the slice: an honest refusal, never a
        // wrong value and never a bare NOT_FOUND (which a client would
        // read as "the key did not exist then").
        HistOutcome::Compacted => (KV_RESULT_COMPACTED, 0),
        HistOutcome::TooBig | HistOutcome::Fault => (KV_RESULT_INTERNAL, 0),
    }
}

/// `KV_OP_SCAN_AT` — body `[revision:u64 LE][cursor:u64 LE][limit:u16 LE]`.
fn apply_scan_at<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(revision) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let cursor = read_u64(body, &mut off).unwrap_or(0);
    let limit = read_u16(body, &mut off).unwrap_or(64);
    store.scan_as_of(revision, cursor, limit, out, now_ms)
}

/// KV_OP_PUT, and (`manifest`) KV_OP_PUT_MANIFEST: the same write, of a
/// value that is a large value's manifest — checked, its root chunk
/// required, and the record flagged.
fn apply_put<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
    manifest: bool,
) -> (u8, usize) {
    let mut off = 0;
    let Some(key) = read_key(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(value) = read_value(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(flags) = read_u8(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let ttl_ms = read_u64(body, &mut off).unwrap_or(0);
    let lease = if flags & PUT_FLAG_LEASE != 0 {
        let Some(l) = read_u64(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        Some(l)
    } else {
        None
    };
    let reads_manifests = read_u8(body, &mut off).unwrap_or(0) & PUT_FLAG2_MANIFESTS != 0;
    let prev_meta = flags & PUT_FLAG_PREV_META != 0;
    let ignore_value = flags & PUT_FLAG_IGNORE_VALUE != 0;
    let ignore_lease = flags & PUT_FLAG_IGNORE_LEASE != 0;

    let nx = (flags & PUT_FLAG_NX) != 0;
    let xx = (flags & PUT_FLAG_XX) != 0;
    let get = (flags & PUT_FLAG_GET) != 0;
    let keepttl = (flags & PUT_FLAG_KEEPTTL) != 0;

    let mut key_copy = [0u8; MAX_KEY_LEN];
    let kn = key.len().min(MAX_KEY_LEN);
    key_copy[..kn].copy_from_slice(&key[..kn]);
    let key_slice = &key_copy[..kn];

    // The live lookup treats an expired record as absent before NX/XX
    // evaluation, so callers never overwrite a TTL'd entry through XX.
    let mut prev = [0u8; MAX_VALUE_LEN];
    let existing = match store.get_live(key_slice, now_ms, &mut prev) {
        GetOutcome::Absent => None,
        GetOutcome::Found { value_len, meta } => Some((value_len, meta)),
        GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
    };
    let exists = existing.is_some();
    let was_manifest = existing.is_some_and(|(_, m)| m.flags & RECORD_MANIFEST != 0);
    // A manifest's bytes are never handed back as a value, unless the
    // reader reads manifests.
    if was_manifest && (get || prev_meta) && !reads_manifests {
        return (KV_RESULT_MANIFEST, 0);
    }
    if manifest {
        let Some(m) = kv_chunks::Manifest::parse(value) else {
            return (KV_RESULT_BAD_CHUNK, 0);
        };
        if ignore_value {
            return (KV_RESULT_BAD_CHUNK, 0);
        }
        match chunk_stored(store, &m.root, now_ms) {
            Some(true) => {}
            Some(false) => return (KV_RESULT_MISSING_CHUNK, 0),
            None => return (KV_RESULT_INTERNAL, 0),
        }
    }

    if nx && exists {
        return (KV_RESULT_CAS_FAILED, 0);
    }
    if xx && !exists {
        return (KV_RESULT_CAS_FAILED, 0);
    }
    // etcd's ignore_value / ignore_lease need something to keep.
    if (ignore_value || ignore_lease) && !exists {
        return (KV_RESULT_NOT_FOUND, 0);
    }
    // The previous KeyValue (etcd's prev_kv): `[present:u8]` + an entry.
    let mut prev_meta_len = 0usize;
    if prev_meta {
        if out.is_empty() {
            return (KV_RESULT_INTERNAL, 0);
        }
        out[0] = u8::from(exists);
        prev_meta_len = 1;
        if let Some((n, m)) = existing {
            let mut at = 1usize;
            if !range_meta_put_as(
                out,
                &mut at,
                key_slice,
                &prev[..n],
                &m,
                false,
                reads_manifests,
            ) {
                return (KV_RESULT_INTERNAL, 0);
            }
            prev_meta_len = at;
        }
    }
    // ignore_value: the stored value is written again (a new version).
    let mut kept = [0u8; MAX_VALUE_LEN];
    let value: &[u8] = match existing {
        Some((n, _)) if ignore_value => {
            kept[..n].copy_from_slice(&prev[..n]);
            &kept[..n]
        }
        _ => value,
    };

    let prev_value_bytes: Option<usize> = if get && exists {
        let n = existing.map(|(n, _)| n).unwrap_or(0);
        if n > out.len() {
            return (KV_RESULT_INTERNAL, 0);
        }
        out[..n].copy_from_slice(&prev[..n]);
        Some(n)
    } else {
        None
    };

    let prior_expiry = existing.map(|(_, m)| m.expiry_ms).unwrap_or(0);

    if exists {
        if !store.update_existing(key_slice, value) {
            return (KV_RESULT_INTERNAL, 0);
        }
    } else if !store.insert_new(key_slice, value) {
        return (KV_RESULT_INTERNAL, 0);
    }

    // Expiry policy:
    //   KEEPTTL + existing key  → retain prior deadline
    //   ttl_ms > 0              → absolute deadline = now_ms + ttl_ms
    //   ttl_ms == 0 (and no KEEPTTL) → clear (sticky-set with no TTL)
    let new_expiry = if keepttl && exists {
        prior_expiry
    } else if ttl_ms > 0 {
        now_ms.saturating_add(ttl_ms)
    } else {
        0
    };
    if !store.set_expiry(key_slice, new_expiry) {
        return (KV_RESULT_INTERNAL, 0);
    }
    // The lease the key carries now: the put's, or (ignore_lease) the
    // one it had. A put without the lease flag leaves it as it was.
    let new_lease = if ignore_lease {
        existing.map(|(_, m)| m.lease_id)
    } else {
        lease
    };
    if let Some(l) = new_lease {
        if !store.set_lease(key_slice, l) {
            return (KV_RESULT_INTERNAL, 0);
        }
    }
    // What the record is now: a manifest when this write put one, or
    // kept one's value (ignore_value).
    if (manifest || (ignore_value && was_manifest)) && !store.set_flags(key_slice, RECORD_MANIFEST)
    {
        return (KV_RESULT_INTERNAL, 0);
    }
    // References: the new manifest holds its root; the one it replaced
    // (unless the value was kept) releases its own.
    if manifest {
        let Some(m) = kv_chunks::Manifest::parse(value) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        if !incref_chunk(store, &m.root, now_ms) {
            return (KV_RESULT_INTERNAL, 0);
        }
    }
    if let Some((n, m)) = existing.filter(|_| !ignore_value) {
        if let Some(root) = manifest_root(&prev[..n], &m) {
            if !release_root(store, &root, now_ms) {
                return (KV_RESULT_INTERNAL, 0);
            }
        }
    }
    if let Some(j) = store.journal() {
        let (create, version) = existing.map_or((0, 1), |(_, m)| {
            (m.create_revision, m.version.saturating_add(1))
        });
        let lease_now = new_lease.or(existing.map(|(_, m)| m.lease_id)).unwrap_or(0);
        let prev_kv = existing.map(|(n, m)| (&prev[..n], m));
        // A manifest's event says so: its value is the manifest, which a
        // watcher resolves (or refuses), never renders as the value.
        j.push(
            if manifest {
                KV_OP_PUT_MANIFEST
            } else {
                KV_OP_PUT
            },
            key_slice,
            value,
            create,
            version,
            lease_now,
            prev_kv.as_ref().map(|(v, m)| (*v, m)),
        );
    }
    if prev_meta {
        return (KV_RESULT_OK, prev_meta_len);
    }

    match prev_value_bytes {
        // The previous value was a manifest, which this reader reads.
        Some(n) if was_manifest => (KV_RESULT_MANIFEST, n),
        Some(n) => (KV_RESULT_OK, n),
        None => (KV_RESULT_OK, 0),
    }
}

// ── Large values: chunks ──────────────────────────────────────────────

/// Run `f` with the store bound to the command's tenant and database in
/// the chunk keyspace `ks`, then restore the command's identity.
fn in_chunk_space<M: Materializer, R>(store: &mut M, ks: u32, f: impl FnOnce(&mut M) -> R) -> R {
    let (t, d, k) = store.key_identity();
    store.set_key_identity(t, d, ks);
    let r = f(store);
    store.set_key_identity(t, d, k);
    r
}

/// Is the chunk `hash` stored? `None` on a provider fault.
fn chunk_stored<M: Materializer>(
    store: &mut M,
    hash: &kv_chunks::Hash,
    now_ms: u64,
) -> Option<bool> {
    in_chunk_space(store, kv_chunks::KS_CHUNK_DATA, |s| {
        let mut b = [0u8; kv_chunks::CHUNK_MAX];
        match s.get_live(kv_chunks::hash_key(hash), now_ms, &mut b) {
            GetOutcome::Found { .. } => Some(true),
            GetOutcome::Absent => Some(false),
            GetOutcome::TooBig | GetOutcome::Fault => None,
        }
    })
}

/// The root a manifest record names, when it is one.
fn manifest_root(value: &[u8], meta: &RecMeta) -> Option<kv_chunks::Hash> {
    if meta.flags & RECORD_MANIFEST == 0 {
        return None;
    }
    kv_chunks::Manifest::parse(value).map(|m| m.root)
}

/// The revision bookkeeping is stamped with: the op's own.
fn chunk_rev<M: Materializer>(store: &M) -> u64 {
    store.op_revision()
}

/// The most memtable entries a write op can add, from its body alone —
/// what a disk store must have free before the op is admitted, so the op
/// never meets a full memtable part-way (its earlier writes applied, the
/// rest refused). `None`: not bounded by its body (a sweep, a range
/// delete, a lease revoke); such an op is admitted only while no flush
/// holds the memtable, when making room is a synchronous flush.
///
/// Each count is every version write the op's apply path makes (an
/// overwrite of a version the op already wrote adds nothing):
/// - `PUT` / `PUT_MANIFEST`: the value, its root's reference and queue
///   entry, and the released root of the value it replaced.
/// - `DELETE`: per key, its tombstone and a released root.
/// - `PUT_CHUNK`: the chunk, its reference count and its queue entry;
///   a node also takes a reference on each child — the count and the
///   queue entry it leaves ([`kv_chunks::FANOUT`] children at most).
/// - `TXN`: the larger of its two branches.
pub fn write_bound(op: u8, body: &[u8]) -> Option<usize> {
    const PUT_BOUND: usize = 8;
    match op {
        KV_OP_PUT | KV_OP_PUT_MANIFEST => Some(PUT_BOUND),
        KV_OP_DELETE => {
            let n = body.get(..2).map(|b| u16::from_le_bytes([b[0], b[1]]))?;
            Some(2 + 4 * usize::from(n))
        }
        KV_OP_PUT_CHUNK => {
            let len = body
                .get(32..34)
                .map(|b| usize::from(u16::from_le_bytes([b[0], b[1]])))?;
            let node = body.get(34) == Some(&kv_chunks::KIND_NODE);
            let children = if node {
                len.saturating_sub(1) / kv_chunks::HASH_LEN
            } else {
                0
            };
            Some(3 + 2 * children)
        }
        KV_OP_TXN => txn_write_bound(body),
        _ => None,
    }
}

/// [`write_bound`] of a `KV_OP_TXN` body (layout: `apply_txn`).
fn txn_write_bound(body: &[u8]) -> Option<usize> {
    let mut off = 0usize;
    let cmps = read_u16(body, &mut off)?;
    for _ in 0..cmps {
        read_u8(body, &mut off)?;
        read_key(body, &mut off)?;
        read_u64(body, &mut off)?;
    }
    let mut branch = || -> Option<usize> {
        let n = read_u16(body, &mut off)?;
        let mut sum = 0usize;
        for _ in 0..n {
            let op = read_u8(body, &mut off)?;
            let len = usize::from(read_u16(body, &mut off)?);
            let inner = body.get(off..off.checked_add(len)?)?;
            off += len;
            sum = sum.checked_add(write_bound(op, inner)?)?;
        }
        Some(sum)
    };
    let then = branch()?;
    let other = branch()?;
    Some(then.max(other))
}

/// Run `f` as one op in one revision: inside the batch already open, or
/// in its own.
fn batched<M: Materializer, R>(store: &mut M, f: impl FnOnce(&mut M) -> R) -> R {
    if store.batch_open() {
        return f(store);
    }
    store.begin_batch();
    let r = f(store);
    store.end_batch();
    r
}

/// A chunk's `(references, since)`; `Ok(None)` when it has no record,
/// `Err` on a provider fault.
fn chunk_refs<M: Materializer>(
    store: &mut M,
    hash: &kv_chunks::Hash,
    now_ms: u64,
) -> Result<Option<(u64, u64)>, ()> {
    in_chunk_space(store, kv_chunks::KS_CHUNK_REF, |s| {
        let mut b = [0u8; kv_chunks::REF_LEN];
        match s.get_live(kv_chunks::hash_key(hash), now_ms, &mut b) {
            GetOutcome::Found { value_len, .. } => {
                kv_chunks::decode_ref(&b[..value_len]).map(Some).ok_or(())
            }
            GetOutcome::Absent => Ok(None),
            _ => Err(()),
        }
    })
}

fn set_chunk_refs<M: Materializer>(
    store: &mut M,
    hash: &kv_chunks::Hash,
    refs: u64,
    since: u64,
) -> bool {
    let r = kv_chunks::encode_ref(refs, since);
    in_chunk_space(store, kv_chunks::KS_CHUNK_REF, |s| {
        s.put_meta(kv_chunks::hash_key(hash), &r)
    })
}

/// Queue the current identity's `hash` at `rev` in a chunk queue
/// (`KS_CHUNK_RETIRE` or `KS_CHUNK_ZERO`, both store-wide), or take it
/// out. An unreferenced chunk is queued with the committed clock, which
/// its grace runs from.
fn chunk_queue<M: Materializer>(
    store: &mut M,
    ks: u32,
    rev: u64,
    hash: &kv_chunks::Hash,
    on: bool,
    now_ms: u64,
) -> bool {
    let (t, d, k) = store.key_identity();
    let key = kv_chunks::queued_key_of(rev, t, d, hash);
    let stamp = now_ms.to_le_bytes();
    let value: &[u8] = if ks == kv_chunks::KS_CHUNK_ZERO {
        &stamp
    } else {
        &[]
    };
    store.set_key_identity(kv_chunks::QUEUE_TENANT, kv_chunks::QUEUE_DATABASE, ks);
    let ok = if on {
        store.put_meta(&key, value)
    } else {
        store.remove_meta(&key)
    };
    store.set_key_identity(t, d, k);
    ok
}

/// Run `f` as the identity `(tenant, database)` a queue entry names.
fn as_identity<M: Materializer, R>(
    store: &mut M,
    tenant: u32,
    database: u32,
    f: impl FnOnce(&mut M) -> R,
) -> R {
    let (t, d, k) = store.key_identity();
    store.set_key_identity(tenant, database, k);
    let r = f(store);
    store.set_key_identity(t, d, k);
    r
}

/// One more reference to a stored chunk (a record's root, a node's
/// child): out of the unreferenced queue when it was there.
fn incref_chunk<M: Materializer>(store: &mut M, hash: &kv_chunks::Hash, now_ms: u64) -> bool {
    let rev = chunk_rev(store);
    match chunk_refs(store, hash, now_ms) {
        Ok(Some((0, since))) => {
            chunk_queue(store, kv_chunks::KS_CHUNK_ZERO, since, hash, false, now_ms)
                && set_chunk_refs(store, hash, 1, rev)
        }
        Ok(Some((refs, _))) => set_chunk_refs(store, hash, refs + 1, rev),
        _ => false,
    }
}

/// One reference fewer: a chunk left with none waits in the unreferenced
/// queue for the floor to pass this revision.
fn decref_chunk<M: Materializer>(store: &mut M, hash: &kv_chunks::Hash, now_ms: u64) -> bool {
    let rev = chunk_rev(store);
    match chunk_refs(store, hash, now_ms) {
        Ok(Some((refs, _))) if refs > 0 => {
            set_chunk_refs(store, hash, refs - 1, rev)
                && (refs > 1
                    || chunk_queue(store, kv_chunks::KS_CHUNK_ZERO, rev, hash, true, now_ms))
        }
        _ => false,
    }
}

/// A manifest a write superseded or deleted: its root's reference goes
/// once the committed floor passes the superseding revision (history may
/// read the old manifest until then) — queued at the superseding write's
/// own revision (the op's).
fn release_root<M: Materializer>(store: &mut M, root: &kv_chunks::Hash, now_ms: u64) -> bool {
    let rev = store.op_revision();
    chunk_queue(store, kv_chunks::KS_CHUNK_RETIRE, rev, root, true, now_ms)
}

/// KV_OP_PUT_CHUNK: `[hash:32][len:u16][chunk]`.
fn apply_put_chunk<M: Materializer>(
    store: &mut M,
    body: &[u8],
    _out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let (Some(hash), Some(len)) = (
        body.get(..kv_chunks::HASH_LEN)
            .and_then(|h| <kv_chunks::Hash>::try_from(h).ok()),
        body.get(32..34)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize),
    ) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(chunk) = body.get(34..34 + len).filter(|_| body.len() == 34 + len) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some((kind, payload)) = kv_chunks::parse_chunk(chunk) else {
        return (KV_RESULT_BAD_CHUNK, 0);
    };
    if kv_chunks::sha256(chunk) != hash {
        return (KV_RESULT_BAD_CHUNK, 0);
    }
    let rev = chunk_rev(store);
    match chunk_refs(store, &hash, now_ms) {
        // Content-addressed: the same bytes, already here. One nothing
        // references yet starts its wait afresh, so a producer about to
        // name it is not raced by the sweep.
        Ok(Some((0, since))) => {
            let ok = chunk_queue(store, kv_chunks::KS_CHUNK_ZERO, since, &hash, false, now_ms)
                && set_chunk_refs(store, &hash, 0, rev)
                && chunk_queue(store, kv_chunks::KS_CHUNK_ZERO, rev, &hash, true, now_ms);
            return if ok {
                (KV_RESULT_OK, 0)
            } else {
                (KV_RESULT_INTERNAL, 0)
            };
        }
        Ok(Some(_)) => return (KV_RESULT_OK, 0),
        Ok(None) => {}
        Err(()) => return (KV_RESULT_INTERNAL, 0),
    }
    // A node is accepted only over its whole subtree: every child it
    // names is stored, so a stored root is a whole value.
    if kind == kv_chunks::KIND_NODE {
        for child in kv_chunks::children(payload) {
            match chunk_stored(store, &child, now_ms) {
                Some(true) => {}
                Some(false) => return (KV_RESULT_MISSING_CHUNK, 0),
                None => return (KV_RESULT_INTERNAL, 0),
            }
        }
    }
    // A new chunk takes its records whole or not at all, and leaves the
    // sweep its room ([`SWEEP_ROOM`]).
    if store.free_slots() < CHUNK_RECORDS + SWEEP_ROOM {
        return (KV_RESULT_INTERNAL, 0);
    }
    let stored = in_chunk_space(store, kv_chunks::KS_CHUNK_DATA, |s| {
        s.put_meta(kv_chunks::hash_key(&hash), chunk)
    });
    let ok = stored
        && set_chunk_refs(store, &hash, 0, rev)
        && chunk_queue(store, kv_chunks::KS_CHUNK_ZERO, rev, &hash, true, now_ms)
        && (kind != kv_chunks::KIND_NODE
            || kv_chunks::children(payload).all(|c| incref_chunk(store, &c, now_ms)));
    if !ok {
        return (KV_RESULT_INTERNAL, 0);
    }
    (KV_RESULT_OK, 0)
}

/// Most queue entries one sweep takes.
pub const SWEEP_BUDGET_MAX: usize = 64;

/// A due-list of queue entries.
type Due = [kv_chunks::Queued; SWEEP_BUDGET_MAX];

const NO_ENTRY: kv_chunks::Queued = kv_chunks::Queued {
    rev: 0,
    tenant: 0,
    database: 0,
    hash: [0; kv_chunks::HASH_LEN],
};

/// Which entries of a chunk queue a walk takes.
#[derive(Clone, Copy)]
struct DueAt {
    /// Queued at or below the floor…
    floor: u64,
    /// …by an earlier op than this one (an op never writes a key it
    /// wrote already)…
    before: u64,
    /// …and, for an unreferenced chunk, queued on the committed clock
    /// before this.
    queued_before_ms: u64,
}

/// The entries of the chunk queue `ks` due by `due` (every entry, with
/// `None`), at most `left`, into `got`: how many. `None` on a provider
/// fault. A sorted walk stops at the first entry not due: the queue
/// sorts by revision, and the committed clock never runs backwards
/// along the log, so nothing after it is due either.
fn due_entries<M: Materializer>(
    store: &mut M,
    ks: u32,
    due: Option<DueAt>,
    left: usize,
    got: &mut Due,
) -> Option<usize> {
    let mut n = 0usize;
    let sorted = store.walk_sorted();
    let (t, d, k) = store.key_identity();
    store.set_key_identity(kv_chunks::QUEUE_TENANT, kv_chunks::QUEUE_DATABASE, ks);
    let r = store.walk_live_meta(&[], &[], 0, 0, |key, value, _| {
        if n >= left {
            return false;
        }
        let Some(q) = kv_chunks::queued_key(key) else {
            return true;
        };
        let taken = due.is_none_or(|d| {
            q.rev <= d.floor
                && q.rev < d.before
                && (ks != kv_chunks::KS_CHUNK_ZERO
                    || <[u8; kv_chunks::ZERO_VALUE_LEN]>::try_from(value)
                        .is_ok_and(|b| u64::from_le_bytes(b) < d.queued_before_ms))
        });
        if taken {
            if let Some(g) = got.get_mut(n) {
                *g = q;
            }
            n += 1;
        } else if sorted {
            return false;
        }
        n < left
    });
    store.set_key_identity(t, d, k);
    (r == KV_RESULT_OK).then_some(n)
}

/// A sweep's body length.
pub const SWEEP_BODY_LEN: usize = 8 + 8 + 2;

/// The sweep's answer: entries are due at its floor now (another sweep
/// takes them).
pub const SWEEP_MORE: u8 = 0x01;
/// The sweep's answer: entries wait in the queues (for the floor, or for
/// their grace).
pub const SWEEP_WAITING: u8 = 0x02;

/// KV_OP_CHUNK_SWEEP: `[floor:u64][queued_before_ms:u64][budget:u16]`,
/// store-wide. Releases due at or below the floor drop their references;
/// chunks unreferenced since at or below it, and queued on the committed
/// clock before `queued_before_ms`, go (a node's children lose a
/// reference each and start their own wait). Answered `KV_RESULT_OK` with
/// `[flags:u8]`: [`SWEEP_MORE`], [`SWEEP_WAITING`].
///
/// Both bounds travel in the committed command, so every replica sweeps
/// alike; its proposer names only a floor that committed, and a clock
/// bound its graph's grace allows.
fn apply_chunk_sweep<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let u64_at = |at: usize| {
        body.get(at..at + 8)
            .and_then(|b| <[u8; 8]>::try_from(b).ok())
            .map(u64::from_le_bytes)
    };
    let (Some(floor), Some(queued_before_ms), Some(budget)) = (
        u64_at(0),
        u64_at(8),
        body.get(16..18)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize),
    ) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    if body.len() != SWEEP_BODY_LEN {
        return (KV_RESULT_INTERNAL, 0);
    }
    let mut left = budget.min(SWEEP_BUDGET_MAX);
    let due = DueAt {
        floor,
        before: store.op_revision(),
        queued_before_ms,
    };
    let mut got: Due = [NO_ENTRY; SWEEP_BUDGET_MAX];
    // Releases: a superseded manifest's root loses its reference.
    let Some(n) = due_entries(store, kv_chunks::KS_CHUNK_RETIRE, Some(due), left, &mut got) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    for q in &got[..n] {
        let ok = as_identity(store, q.tenant, q.database, |s| {
            chunk_queue(s, kv_chunks::KS_CHUNK_RETIRE, q.rev, &q.hash, false, now_ms)
                && decref_chunk(s, &q.hash, now_ms)
        });
        if !ok {
            return (KV_RESULT_INTERNAL, 0);
        }
    }
    left -= n;
    // Unreferenced chunks: gone, a node's children released. A node the
    // store has no room to take apart stays queued; the pass goes on to
    // the rest, whose removals make room.
    let Some(n) = due_entries(store, kv_chunks::KS_CHUNK_ZERO, Some(due), left, &mut got) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut short_of_room = false;
    for q in &got[..n] {
        match as_identity(store, q.tenant, q.database, |s| {
            remove_chunk(s, q.rev, &q.hash, now_ms)
        }) {
            Removal::Done => {}
            Removal::NoRoom => short_of_room = true,
            Removal::Fault => return (KV_RESULT_INTERNAL, 0),
        }
    }
    // What is left: due now (this pass's own queueing included — a later
    // revision may take it), or waiting. A pass short of room says only
    // that work waits: what it left needs room it could not make, so
    // proposing again at once would find the same.
    let mut probe: Due = [NO_ENTRY; SWEEP_BUDGET_MAX];
    let mut any = |store: &mut M, ks: u32, due: Option<DueAt>| {
        due_entries(store, ks, due, 1, &mut probe).map(|n| n > 0)
    };
    let later = Some(DueAt {
        before: u64::MAX,
        ..due
    });
    let (Some(r_due), Some(z_due), Some(r_any), Some(z_any)) = (
        any(store, kv_chunks::KS_CHUNK_RETIRE, later),
        any(store, kv_chunks::KS_CHUNK_ZERO, later),
        any(store, kv_chunks::KS_CHUNK_RETIRE, None),
        any(store, kv_chunks::KS_CHUNK_ZERO, None),
    ) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(o) = out.first_mut() else {
        return (KV_RESULT_INTERNAL, 0);
    };
    *o = if (r_due || z_due) && !short_of_room {
        SWEEP_MORE
    } else {
        0
    } | if r_any || z_any { SWEEP_WAITING } else { 0 };
    (KV_RESULT_OK, 1)
}

/// What taking one unreferenced chunk apart came to.
enum Removal {
    Done,
    /// Left as it was: the store has no room for the entries its
    /// children would queue.
    NoRoom,
    Fault,
}

/// Records the removal of one chunk frees: its data, its references and
/// its entry in the unreferenced queue.
const CHUNK_RECORDS: usize = 3;

/// Room a chunk upload leaves the store: enough for a sweep to take any
/// node apart — a node's removal queues an entry for each of up to
/// `FANOUT` children before its own records are reused — with the record
/// writes that name the uploads to spare. A provider with a fixed number
/// of records ([`Materializer::free_slots`]) would otherwise fill with
/// superseded trees the sweep has no room to take apart, and refuse every
/// upload for want of the room only the sweep makes.
pub const SWEEP_ROOM: usize = kv_chunks::FANOUT;

/// An unreferenced chunk, queued at `since`, goes: its record and
/// references, and a node's reference on each child. A node goes only
/// when the store has room for an entry per child: all of it, or none.
fn remove_chunk<M: Materializer>(
    store: &mut M,
    since: u64,
    h: &kv_chunks::Hash,
    now_ms: u64,
) -> Removal {
    let mut chunk = [0u8; kv_chunks::CHUNK_MAX];
    let len = in_chunk_space(store, kv_chunks::KS_CHUNK_DATA, |s| {
        match s.get_live(kv_chunks::hash_key(h), now_ms, &mut chunk) {
            GetOutcome::Found { value_len, .. } => Some(value_len),
            _ => None,
        }
    });
    let Some(len) = len else {
        return Removal::Fault;
    };
    let node = match kv_chunks::parse_chunk(&chunk[..len]) {
        Some((kv_chunks::KIND_NODE, payload)) => Some(payload),
        _ => None,
    };
    let children = node.map_or(0, |p| kv_chunks::children(p).count());
    if store.free_slots().saturating_add(CHUNK_RECORDS) < children {
        return Removal::NoRoom;
    }
    let ok = chunk_queue(store, kv_chunks::KS_CHUNK_ZERO, since, h, false, now_ms)
        && in_chunk_space(store, kv_chunks::KS_CHUNK_REF, |s| {
            s.remove_meta(kv_chunks::hash_key(h))
        })
        && in_chunk_space(store, kv_chunks::KS_CHUNK_DATA, |s| {
            s.remove_meta(kv_chunks::hash_key(h))
        })
        && node.is_none_or(|p| kv_chunks::children(p).all(|c| decref_chunk(store, &c, now_ms)));
    if ok {
        Removal::Done
    } else {
        Removal::Fault
    }
}

/// KV_OP_GET_CHUNK: `[hash:32]`.
fn apply_get_chunk<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let Some(hash) = body
        .get(..kv_chunks::HASH_LEN)
        .filter(|_| body.len() == kv_chunks::HASH_LEN)
        .and_then(|h| <kv_chunks::Hash>::try_from(h).ok())
    else {
        return (KV_RESULT_INTERNAL, 0);
    };
    in_chunk_space(store, kv_chunks::KS_CHUNK_DATA, |s| {
        let mut b = [0u8; kv_chunks::CHUNK_MAX];
        match s.get_live(kv_chunks::hash_key(&hash), now_ms, &mut b) {
            GetOutcome::Found { value_len: n, .. } if n <= out.len() => {
                out[..n].copy_from_slice(&b[..n]);
                (KV_RESULT_OK, n)
            }
            GetOutcome::Absent => (KV_RESULT_NOT_FOUND, 0),
            _ => (KV_RESULT_INTERNAL, 0),
        }
    })
}

fn apply_cas<M: Materializer>(
    store: &mut M,
    body: &[u8],
    _out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(key) = read_key(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(witness) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(value) = read_value(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };

    let mut key_copy = [0u8; MAX_KEY_LEN];
    let kn = key.len().min(MAX_KEY_LEN);
    key_copy[..kn].copy_from_slice(&key[..kn]);
    let key_slice = &key_copy[..kn];

    // The live lookup sees an expired record as absent, so the
    // witness check sees a fresh world.
    let mut scratch = [0u8; MAX_VALUE_LEN];
    match store.get_live(key_slice, now_ms, &mut scratch) {
        GetOutcome::Found { meta, value_len } => {
            // Existing record: witness must equal the current mod_revision.
            if meta.mod_revision != witness {
                return (KV_RESULT_CAS_FAILED, 0);
            }
            let old_root = manifest_root(&scratch[..value_len], &meta);
            if !store.update_existing(key_slice, value) {
                return (KV_RESULT_INTERNAL, 0);
            }
            if let Some(root) = old_root {
                if !release_root(store, &root, now_ms) {
                    return (KV_RESULT_INTERNAL, 0);
                }
            }
            // CAS doesn't carry an expiry field — preserve the prior deadline.
            if !store.set_expiry(key_slice, meta.expiry_ms) {
                return (KV_RESULT_INTERNAL, 0);
            }
            (KV_RESULT_OK, 0)
        }
        GetOutcome::Absent => {
            // No existing record: witness of 0 means create-if-absent.
            if witness != 0 {
                return (KV_RESULT_CAS_FAILED, 0);
            }
            if !store.insert_new(key_slice, value) {
                return (KV_RESULT_INTERNAL, 0);
            }
            (KV_RESULT_OK, 0)
        }
        GetOutcome::TooBig | GetOutcome::Fault => (KV_RESULT_INTERNAL, 0),
    }
}

fn apply_txn<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    // Layout:
    //   [cmp_count:u16] then per cmp: [cmp_op:u8][key_len:u16][key][witness:u64]
    //   [then_count:u16] then per op: [op:u8][body_len:u16][body…]
    //   [else_count:u16] then per op: [op:u8][body_len:u16][body…]
    let mut off = 0;
    let Some(cmp_count) = read_u16(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let cmps_start = off;
    let mut succeeded = true;
    let mut c = 0u16;
    while c < cmp_count {
        let Some(cmp_op) = read_u8(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let Some(key) = read_key(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let Some(witness) = read_u64(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        // Absent or expired keys have an effective mod_revision of 0.
        let mut scratch = [0u8; MAX_VALUE_LEN];
        let current_rev = match store.get_live(key, now_ms, &mut scratch) {
            GetOutcome::Found { meta, .. } => meta.mod_revision,
            GetOutcome::Absent => 0,
            GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
        };
        let cmp_ok = match cmp_op {
            TXN_CMP_MOD_EQUAL => current_rev == witness,
            TXN_CMP_MOD_NOT_EQUAL => current_rev != witness,
            TXN_CMP_MOD_GREATER => current_rev > witness,
            TXN_CMP_MOD_LESS => current_rev < witness,
            _ => return (KV_RESULT_INTERNAL, 0),
        };
        if !cmp_ok {
            succeeded = false;
            // Don't early-exit: we still need to walk the body to find
            // the THEN and ELSE op lists.
        }
        c += 1;
    }
    let _ = cmps_start;

    // Skip past the THEN list when ELSE will run, and vice versa.
    let Some(then_count) = read_u16(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let then_start = off;
    // Walk the THEN list to find its end.
    let mut t = 0u16;
    while t < then_count {
        if read_u8(body, &mut off).is_none() {
            return (KV_RESULT_INTERNAL, 0);
        }
        let Some(blen) = read_u16(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        if off + blen as usize > body.len() {
            return (KV_RESULT_INTERNAL, 0);
        }
        off += blen as usize;
        t += 1;
    }
    let then_end = off;
    let Some(else_count) = read_u16(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let else_start = off;

    let (branch_count, branch_start, branch_end) = if succeeded {
        (then_count, then_start, then_end)
    } else {
        // For ELSE we also need to bound the slice for safety; the
        // body should end after the last ELSE op, but compute via walk.
        let mut e = 0u16;
        let mut local_off = else_start;
        while e < else_count {
            if read_u8(body, &mut local_off).is_none() {
                return (KV_RESULT_INTERNAL, 0);
            }
            let Some(blen) = read_u16(body, &mut local_off) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            if local_off + blen as usize > body.len() {
                return (KV_RESULT_INTERNAL, 0);
            }
            local_off += blen as usize;
            e += 1;
        }
        (else_count, else_start, local_off)
    };

    // Pre-flight capacity, so a multi-put transaction is all-or-nothing.
    // Count the NEW keys the chosen branch's puts would insert and refuse
    // the whole txn up front if they would not all fit, rather than
    // tearing at the op that fills the store: an INSERT that persisted its
    // row but not its index entry leaves a corrupt secondary index that
    // reports rows the scan cannot find. Overwrites cost no slot, so an
    // UPDATE against a near-full store still commits. Providers with no
    // ceiling (`usize::MAX`) skip the walk entirely.
    let free = store.free_slots();
    if free != usize::MAX {
        let mut new_keys = 0usize;
        let mut o = branch_start;
        let mut n = 0u16;
        while n < branch_count {
            let Some(op_byte) = read_u8(body, &mut o) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            let Some(blen) = read_u16(body, &mut o) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            let bend = o + blen as usize;
            if bend > branch_end {
                return (KV_RESULT_INTERNAL, 0);
            }
            // Slot-consuming ops lead with `[key_len:u16][key…]`; only a
            // PUT/INCR of an ABSENT key claims a slot.
            if op_byte == KV_OP_PUT || op_byte == KV_OP_INCR {
                let mut ko = o;
                if let Some(k) = read_key(body, &mut ko) {
                    let mut probe = [0u8; MAX_VALUE_LEN];
                    if let GetOutcome::Absent = store.get_live(k, now_ms, &mut probe) {
                        new_keys += 1;
                    }
                }
            }
            o = bend;
            n += 1;
        }
        if new_keys > free {
            return (KV_RESULT_INTERNAL, 0);
        }
    }

    // Header: [succeeded:u8][op_count:u16 LE]
    if out.len() < 3 {
        return (KV_RESULT_INTERNAL, 0);
    }
    out[0] = if succeeded { 1 } else { 0 };
    out[1..3].copy_from_slice(&branch_count.to_le_bytes());
    let mut out_off = 3usize;

    // Execute the chosen branch's ops, accumulating per-op results.
    let mut op_buf_off = branch_start;
    let mut k = 0u16;
    while k < branch_count {
        let Some(op_byte) = read_u8(body, &mut op_buf_off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let Some(blen) = read_u16(body, &mut op_buf_off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let bend = op_buf_off + blen as usize;
        if bend > branch_end {
            return (KV_RESULT_INTERNAL, 0);
        }
        let sub_body = &body[op_buf_off..bend];
        op_buf_off = bend;

        // Cap per-op result body length so we can fit a u16 header.
        let mut sub_out = [0u8; 8192];
        let (sub_result, sub_len) = apply_mat(store, op_byte, sub_body, &mut sub_out, now_ms);

        if out_off + 1 + 2 + sub_len > out.len() {
            return (KV_RESULT_INTERNAL, 0);
        }
        out[out_off] = sub_result;
        out_off += 1;
        out[out_off..out_off + 2].copy_from_slice(&(sub_len as u16).to_le_bytes());
        out_off += 2;
        out[out_off..out_off + sub_len].copy_from_slice(&sub_out[..sub_len]);
        out_off += sub_len;

        k += 1;
    }

    (KV_RESULT_TXN, out_off)
}

/// etcd's span rules: no `end` = the one key (`[key, key‖0)`), `\0` =
/// unbounded above, else `[start, end)`. The successor is built in `succ`.
fn etcd_span<'a>(
    start: &'a [u8],
    end: &'a [u8],
    succ: &'a mut [u8; MAX_KEY_LEN + 1],
) -> Option<(&'a [u8], &'a [u8])> {
    if end.is_empty() {
        let n = start.len();
        if n >= succ.len() {
            return None;
        }
        succ[..n].copy_from_slice(start);
        succ[n] = 0;
        Some((start, &succ[..n + 1]))
    } else if end == [0] {
        Some((start, &[]))
    } else {
        Some((start, end))
    }
}

/// [`KV_OP_DELETE_META`]: gather the span's live keys (and, with prev_kv,
/// values) into the answer, then delete them all and bump the revision
/// once — one revision for the whole range, as etcd's DeleteRange.
/// A KVS entry at `out[e..]` (`[klen:2][key][vlen:4][value][meta:32]`,
/// then `[flags:1]` when `marked`): its key and where it ends.
fn kvs_entry_at(out: &[u8], e: usize, marked: bool) -> Option<(&[u8], usize)> {
    let kl = u16::from_le_bytes([*out.get(e)?, *out.get(e + 1)?]) as usize;
    let key = out.get(e + 2..e + 2 + kl)?;
    let vl = u32::from_le_bytes(out.get(e + 2 + kl..e + 6 + kl)?.try_into().ok()?) as usize;
    let end = e + 6 + kl + vl + 32 + usize::from(marked);
    out.get(..end)?;
    Some((key, end))
}

/// Order the `n` KVS entries in `out[from..to]` by key, in place: each
/// place takes the least of the entries after it, rotated into it.
/// `false` when the entries are not the shape they claim.
fn sort_kvs_entries(out: &mut [u8], from: usize, to: usize, n: usize, marked: bool) -> bool {
    let mut p = from;
    for _ in 0..n {
        // The least entry from `p` on.
        let (mut best, mut best_end) = (p, 0usize);
        let mut e = p;
        while e < to {
            let Some((key, end)) = kvs_entry_at(out, e, marked) else {
                return false;
            };
            let less =
                best_end == 0 || kvs_entry_at(out, best, marked).is_some_and(|(bk, _)| key < bk);
            if less {
                best = e;
                best_end = end;
            }
            e = end;
        }
        if best_end == 0 {
            return false;
        }
        // Rotate it to the front of what is left (three reversals: no
        // panic path, unlike `rotate_right`).
        let k = best_end - best;
        let Some(span) = out.get_mut(p..best_end) else {
            return false;
        };
        span.reverse();
        match span.split_at_mut_checked(k) {
            Some((head, tail)) => {
                head.reverse();
                tail.reverse();
            }
            None => return false,
        }
        p += k;
    }
    p == to
}

fn apply_delete_meta<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0usize;
    let (Some(start), Some(end), Some(flags)) = (
        read_key(body, &mut off),
        read_key(body, &mut off),
        read_u8(body, &mut off),
    ) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let prev_kv = flags & 1 != 0;
    // As RANGE_META: previous values that are manifests only for a
    // reader that reads them.
    let manifests = flags & DELETE_META_FLAG_MANIFESTS != 0;
    let mut refused = false;
    let mut succ = [0u8; MAX_KEY_LEN + 1];
    let Some((start, end)) = etcd_span(start, end, &mut succ) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    if out.len() < 11 {
        return (KV_RESULT_INTERNAL, 0);
    }
    let mut at = 11usize;
    let mut n = 0u64;
    let mut full = false;
    let r = store.walk_live_meta(start, end, 0, now_ms, |k, v, m| {
        if prev_kv && !manifests && m.flags & RECORD_MANIFEST != 0 {
            refused = true;
            return false;
        }
        if range_meta_put_as(out, &mut at, k, v, m, !prev_kv, manifests) {
            n += 1;
            true
        } else {
            full = true;
            false
        }
    });
    if r != KV_RESULT_OK {
        return (r, 0);
    }
    if refused {
        // Nothing deleted: the gather comes first.
        return (KV_RESULT_MANIFEST, 0);
    }
    if full || n > u64::from(u16::MAX) {
        return (KV_RESULT_QUOTA, 0);
    }
    // In key order, as etcd answers (and so its watchers see the deletes),
    // whatever order the provider walked in.
    if !store.walk_sorted() && !sort_kvs_entries(out, 11, at, n as usize, manifests) {
        return (KV_RESULT_INTERNAL, 0);
    }
    // Delete what was gathered: the keys are read back from the answer.
    let mut p = 11usize;
    let mut deleted = 0u64;
    for _ in 0..n {
        let klen = u16::from_le_bytes([out[p], out[p + 1]]) as usize;
        let mut key = [0u8; MAX_KEY_LEN];
        key[..klen].copy_from_slice(&out[p + 2..p + 2 + klen]);
        let vlen_at = p + 2 + klen;
        let vlen = u32::from_le_bytes([
            out[vlen_at],
            out[vlen_at + 1],
            out[vlen_at + 2],
            out[vlen_at + 3],
        ]) as usize;
        p = vlen_at + 4 + vlen + 32 + usize::from(manifests);
        match remove_noted(store, &key[..klen], now_ms) {
            Some(true) => deleted += 1,
            Some(false) => {}
            None => return (KV_RESULT_INTERNAL, 0),
        }
    }
    if deleted > 0 {
        store.bump_revision();
    }
    out[0..8].copy_from_slice(&deleted.to_le_bytes());
    // The KVS `more` byte carries the prev_kv flag here: whether the
    // entries' values are the deleted values (the answer says what it is).
    out[8] = u8::from(prev_kv);
    out[9..11].copy_from_slice(&(n as u16).to_le_bytes());
    (KV_RESULT_KVS, at)
}

/// [`KV_OP_LEASE_REVOKE`] (`delete`) / [`KV_OP_LEASE_KEYS`]: the live keys
/// attached to a lease, gathered into the answer as far as it holds them
/// (`more` = 1 when some did not fit) and, for a revoke, deleted at one
/// revision.
fn apply_lease_keys<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
    delete: bool,
) -> (u8, usize) {
    let mut off = 0usize;
    let Some(lease) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    if lease == 0 || out.len() < 11 {
        return (KV_RESULT_INTERNAL, 0);
    }
    if !delete {
        // A listing pages in key order from its cursor.
        let after = read_key(body, &mut off).unwrap_or(&[]);
        return lease_keys_page(store, lease, after, out, now_ms);
    }
    let mut at = 11usize;
    let mut n = 0u64;
    let mut more = false;
    let r = store.walk_live_meta(&[], &[], 0, now_ms, |k, v, m| {
        if m.lease_id != lease {
            return true;
        }
        if n < u64::from(u16::MAX) && range_meta_put(out, &mut at, k, v, m, true) {
            n += 1;
            true
        } else {
            more = true;
            false
        }
    });
    if r != KV_RESULT_OK {
        return (r, 0);
    }
    let mut deleted = 0u64;
    if delete {
        let mut p = 11usize;
        for _ in 0..n {
            let klen = u16::from_le_bytes([out[p], out[p + 1]]) as usize;
            let mut key = [0u8; MAX_KEY_LEN];
            key[..klen].copy_from_slice(&out[p + 2..p + 2 + klen]);
            // Key-only entries: no value bytes follow the length.
            p += 2 + klen + 4 + 32;
            match remove_noted(store, &key[..klen], now_ms) {
                Some(true) => deleted += 1,
                Some(false) => {}
                None => return (KV_RESULT_INTERNAL, 0),
            }
        }
        if deleted > 0 {
            store.bump_revision();
        }
    }
    out[0..8].copy_from_slice(&(if delete { deleted } else { n }).to_le_bytes());
    out[8] = u8::from(more);
    out[9..11].copy_from_slice(&(n as u16).to_le_bytes());
    (KV_RESULT_KVS, at)
}

/// [`KV_OP_COMPACT`]: etcd's Compact, refused as etcd refuses it — a
/// revision already compacted, or one the store has not reached.
fn apply_compact<M: Materializer>(store: &mut M, body: &[u8]) -> (u8, usize) {
    let mut off = 0usize;
    let Some(rev) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    if rev <= store.compact_revision() {
        return (KV_RESULT_COMPACTED, 0);
    }
    if rev > store.revision() {
        return (KV_RESULT_FUTURE_REV, 0);
    }
    store.set_compact_revision(rev);
    (KV_RESULT_OK, 0)
}

/// [`KV_OP_HASH_KV`]: an order-free digest of the live keys at a
/// revision (each entry hashed alone, the hashes summed), and their size.
fn apply_hash_kv<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0usize;
    let Some(rev) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    if rev > store.revision() {
        return (KV_RESULT_FUTURE_REV, 0);
    }
    if rev != 0 && rev < store.compact_revision() {
        return (KV_RESULT_COMPACTED, 0);
    }
    if out.len() < 36 {
        return (KV_RESULT_INTERNAL, 0);
    }
    let (mut sum, mut bytes, mut keys) = (0u64, 0u64, 0u64);
    let r = store.walk_live_meta(&[], &[], rev, now_ms, |k, v, m| {
        let mut h = fnv1a64(k) ^ fnv1a64(v).rotate_left(17);
        for x in [m.create_revision, m.mod_revision, m.version, m.lease_id] {
            h = (h ^ x).wrapping_mul(0x0000_0100_0000_01B3);
        }
        sum = sum.wrapping_add(h);
        bytes += (k.len() + v.len()) as u64;
        keys += 1;
        true
    });
    if r != KV_RESULT_OK {
        return (r, 0);
    }
    let hash = (sum ^ (sum >> 32)) as u32;
    let hash_rev = if rev == 0 { store.revision() } else { rev };
    out[0..4].copy_from_slice(&hash.to_le_bytes());
    out[4..12].copy_from_slice(&store.compact_revision().to_le_bytes());
    out[12..20].copy_from_slice(&hash_rev.to_le_bytes());
    out[20..28].copy_from_slice(&bytes.to_le_bytes());
    out[28..36].copy_from_slice(&keys.to_le_bytes());
    (KV_RESULT_OK, 36)
}

/// One page of [`KV_OP_LEASE_KEYS`]: the lease's keys after `after`, in
/// ascending key order, as many as the answer holds (`more` = 1 when
/// keys remain: the caller asks again from the last key it got). A
/// provider that walks in key order is read once; one that does not
/// (the memory provider's hash slots) is walked once per key, taking
/// the least key past the last each time.
fn lease_keys_page<M: Materializer>(
    store: &mut M,
    lease: u64,
    after: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut at = 11usize;
    let mut n = 0u64;
    let mut more = false;
    let mut last = [0u8; MAX_KEY_LEN];
    let mut last_len = after.len().min(MAX_KEY_LEN);
    last[..last_len].copy_from_slice(&after[..last_len]);
    let mut started = !after.is_empty();
    let meta_of = |lease_id: u64| RecMeta {
        create_revision: 0,
        mod_revision: 0,
        version: 0,
        lease_id,
        expiry_ms: 0,
        commit_ts: 0,
        flags: 0,
    };
    if store.walk_sorted() {
        let r = store.walk_live_meta(&[], &[], 0, now_ms, |k, _v, m| {
            if m.lease_id != lease || (started && k <= &last[..last_len]) {
                return true;
            }
            if n < u64::from(u16::MAX)
                && range_meta_put(out, &mut at, k, &[], &meta_of(lease), true)
            {
                n += 1;
                true
            } else {
                more = true;
                false
            }
        });
        if r != KV_RESULT_OK {
            return (r, 0);
        }
    } else {
        loop {
            let mut best = [0u8; MAX_KEY_LEN];
            let mut best_len = 0usize;
            let mut found = false;
            let r = store.walk_live_meta(&[], &[], 0, now_ms, |k, _v, m| {
                if m.lease_id != lease
                    || k.len() > MAX_KEY_LEN
                    || (started && k <= &last[..last_len])
                    || (found && k >= &best[..best_len])
                {
                    return true;
                }
                best[..k.len()].copy_from_slice(k);
                best_len = k.len();
                found = true;
                true
            });
            if r != KV_RESULT_OK {
                return (r, 0);
            }
            if !found {
                break;
            }
            if n >= u64::from(u16::MAX)
                || !range_meta_put(out, &mut at, &best[..best_len], &[], &meta_of(lease), true)
            {
                more = true;
                break;
            }
            n += 1;
            last[..best_len].copy_from_slice(&best[..best_len]);
            last_len = best_len;
            started = true;
        }
    }
    out[0..8].copy_from_slice(&n.to_le_bytes());
    out[8] = u8::from(more);
    out[9..11].copy_from_slice(&(n as u16).to_le_bytes());
    (KV_RESULT_KVS, at)
}

/// One compare of a [`KV_OP_ETCD_TXN`] body, parsed.
struct EtcdCmp<'a> {
    target: u8,
    result: u8,
    key: &'a [u8],
    end: &'a [u8],
    operand: u64,
    value: &'a [u8],
}

fn parse_etcd_cmp<'a>(body: &'a [u8], off: &mut usize) -> Option<EtcdCmp<'a>> {
    let target = read_u8(body, off)?;
    let result = read_u8(body, off)?;
    let key = read_key(body, off)?;
    let end = read_key(body, off)?;
    let operand = read_u64(body, off)?;
    let vlen = u32::from_le_bytes(body.get(*off..*off + 4)?.try_into().ok()?) as usize;
    *off += 4;
    let value = body.get(*off..*off + vlen)?;
    *off += vlen;
    Some(EtcdCmp {
        target,
        result,
        key,
        end,
        operand,
        value,
    })
}

/// Whether one key (its value and metadata, or `None` when absent)
/// satisfies `c`: an absent key is all zeros and fails any value compare.
fn etcd_cmp_one(c: &EtcdCmp<'_>, kv: Option<(&[u8], &RecMeta)>) -> bool {
    use core::cmp::Ordering;
    let ord = match (c.target, kv) {
        (3, None) => return false,
        (3, Some((v, _))) => v.cmp(c.value),
        (t, kv) => {
            let actual = match (t, kv.map(|(_, m)| *m)) {
                (_, None) => 0,
                (0, Some(m)) => m.version,
                (1, Some(m)) => m.create_revision,
                (2, Some(m)) => m.mod_revision,
                (_, Some(m)) => m.lease_id,
            };
            actual.cmp(&c.operand)
        }
    };
    match c.result {
        0 => ord == Ordering::Equal,
        1 => ord == Ordering::Greater,
        2 => ord == Ordering::Less,
        _ => ord != Ordering::Equal,
    }
}

/// Whether compare `c` holds over its key or range (every key must; no
/// key compares as one absent key). `None` on a store fault.
fn etcd_cmp_holds<M: Materializer>(store: &mut M, c: &EtcdCmp<'_>, now_ms: u64) -> Option<bool> {
    let mut succ = [0u8; MAX_KEY_LEN + 1];
    let (start, end) = etcd_span(c.key, c.end, &mut succ)?;
    let mut seen = false;
    let mut all = true;
    // A value compare against a large value reads its chunks, which the
    // walk cannot while it holds the store: the walk stops at one, the
    // compare reads it, and the walk resumes after its key.
    let mut from = [0u8; MAX_KEY_LEN + 1];
    let mut from_len = start.len();
    from[..from_len].copy_from_slice(start);
    loop {
        let mut large: Option<(kv_chunks::Manifest, usize, [u8; MAX_KEY_LEN])> = None;
        let r = store.walk_live_meta(&from[..from_len], end, 0, now_ms, |k, v, m| {
            seen = true;
            if c.target == 3 && m.flags & RECORD_MANIFEST != 0 {
                let mut kb = [0u8; MAX_KEY_LEN];
                kb[..k.len()].copy_from_slice(k);
                large = kv_chunks::Manifest::parse(v).map(|mf| (mf, k.len(), kb));
                if large.is_none() {
                    all = false;
                }
                return false;
            }
            if !etcd_cmp_one(c, Some((v, m))) {
                all = false;
                return false;
            }
            true
        });
        if r != KV_RESULT_OK {
            return None;
        }
        let Some((mf, kl, kb)) = large else {
            break;
        };
        let ord = cmp_large_value(store, &mf, c.value, now_ms)?;
        if !etcd_result_holds(c.result, ord) {
            all = false;
            break;
        }
        // Resume just after the large value's key.
        from[..kl].copy_from_slice(&kb[..kl]);
        from[kl] = 0;
        from_len = kl + 1;
    }
    Some(if seen { all } else { etcd_cmp_one(c, None) })
}

/// Does an etcd compare `result` accept `ord` (the key's against the
/// operand)?
fn etcd_result_holds(result: u8, ord: core::cmp::Ordering) -> bool {
    use core::cmp::Ordering;
    match result {
        0 => ord == Ordering::Equal,
        1 => ord == Ordering::Greater,
        2 => ord == Ordering::Less,
        _ => ord != Ordering::Equal,
    }
}

/// A large value against `operand`, byte-wise, reading only the leading
/// chunks a difference (or the operand's end) needs. `None` on a
/// provider fault or a tree that does not read back.
fn cmp_large_value<M: Materializer>(
    store: &mut M,
    m: &kv_chunks::Manifest,
    operand: &[u8],
    now_ms: u64,
) -> Option<core::cmp::Ordering> {
    use core::cmp::Ordering;
    let mut r = kv_chunks::Reader::new(*m);
    let mut at = 0usize;
    let mut chunk = [0u8; kv_chunks::CHUNK_MAX];
    while let Some(h) = r.want() {
        let n = in_chunk_space(store, kv_chunks::KS_CHUNK_DATA, |s| {
            match s.get_live(kv_chunks::hash_key(&h), now_ms, &mut chunk) {
                GetOutcome::Found { value_len, .. } => Some(value_len),
                _ => None,
            }
        })?;
        if let kv_chunks::Step::Data(d) = r.take(&chunk[..n])? {
            let rest = operand.get(at..).unwrap_or(&[]);
            let k = d.len().min(rest.len());
            match d[..k].cmp(&rest[..k]) {
                Ordering::Equal => {}
                o => return Some(o),
            }
            if rest.len() <= d.len() {
                // The operand ended within (or exactly at the end of)
                // this chunk; the value is longer than it.
                return Some(
                    if (at + d.len()) as u64 == m.total && rest.len() == d.len() {
                        Ordering::Equal
                    } else {
                        Ordering::Greater
                    },
                );
            }
            at += d.len();
        }
    }
    // The value ended before the operand did.
    Some(Ordering::Less)
}

/// The key a Txn write op writes (a Put's, or a single-key DeleteRange's).
fn etcd_op_key(kind: u8, body: &[u8]) -> Option<&[u8]> {
    let mut off = 0usize;
    match kind {
        types::ETCD_OP_PUT | types::ETCD_OP_PUT_MANIFEST => read_key(body, &mut off),
        3 => {
            let key = read_key(body, &mut off)?;
            let end = read_key(body, &mut off)?;
            end.is_empty().then_some(key)
        }
        _ => None,
    }
}

/// `[kind:u8][len:u32 LE]` of the op at `ops[p..]`.
fn etcd_op_head(ops: &[u8], p: usize) -> Option<(u8, usize)> {
    let h = ops.get(p..p + 5)?;
    Some((h[0], u32::from_le_bytes([h[1], h[2], h[3], h[4]]) as usize))
}

/// [`KV_OP_ETCD_TXN`]: evaluate the compares, then apply the chosen
/// branch's ops in order, each answering into the reply (a Range after a
/// Put sees it, as in etcd).
fn apply_etcd_txn<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
    depth: u8,
) -> (u8, usize) {
    if usize::from(depth) > types::ETCD_TXN_DEPTH_MAX || out.len() < 2 {
        return (KV_RESULT_INTERNAL, 0);
    }
    let mut off = 0usize;
    let Some(ncmp) = read_u8(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut succeeded = true;
    for _ in 0..ncmp {
        let Some(c) = parse_etcd_cmp(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        if succeeded {
            match etcd_cmp_holds(store, &c, now_ms) {
                Some(h) => succeeded = h,
                None => return (KV_RESULT_INTERNAL, 0),
            }
        }
    }
    // The branches: `then`, then `else`; the chosen one is `[from, to)`.
    let mut branches = [(0usize, 0usize, 0u8); 2];
    for b in &mut branches {
        let Some(n) = read_u8(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let from = off;
        for _ in 0..n {
            let Some((_, len)) = etcd_op_head(body, off) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            off += 5 + len;
            if off > body.len() {
                return (KV_RESULT_INTERNAL, 0);
            }
        }
        *b = (from, off, n);
    }
    let (from, to, n) = branches[if succeeded { 0 } else { 1 }];
    let ops = &body[from..to];
    // etcd refuses a branch that writes one key twice, before applying.
    let mut p = 0usize;
    for i in 0..n {
        let Some((kind, len)) = etcd_op_head(ops, p) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let key = etcd_op_key(kind, &ops[p + 5..p + 5 + len]);
        let mut q = p + 5 + len;
        if let Some(k) = key {
            for _ in i + 1..n {
                let Some((k2, l2)) = etcd_op_head(ops, q) else {
                    return (KV_RESULT_INTERNAL, 0);
                };
                if etcd_op_key(k2, &ops[q + 5..q + 5 + l2]) == Some(k) {
                    return (KV_RESULT_DUPLICATE, 0);
                }
                q += 5 + l2;
            }
        }
        p += 5 + len;
    }
    out[0] = u8::from(succeeded);
    out[1] = n;
    let mut at = 2usize;
    let mut p = 0usize;
    for _ in 0..n {
        let Some((kind, len)) = etcd_op_head(ops, p) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let op_body = &ops[p + 5..p + 5 + len];
        p += 5 + len;
        if at + 6 > out.len() {
            return (KV_RESULT_INTERNAL, 0);
        }
        let sub = &mut out[at + 6..];
        let (r, rn) = match kind {
            1 => apply_range_meta(store, op_body, sub, now_ms),
            types::ETCD_OP_PUT => apply_put(store, op_body, sub, now_ms, false),
            types::ETCD_OP_PUT_MANIFEST => apply_put(store, op_body, sub, now_ms, true),
            3 => apply_delete_meta(store, op_body, sub, now_ms),
            4 => apply_etcd_txn(store, op_body, sub, now_ms, depth + 1),
            _ => (KV_RESULT_INTERNAL, 0),
        };
        if !matches!(r, KV_RESULT_OK | KV_RESULT_KVS | KV_RESULT_ETCD_TXN) {
            // An op the store refused ends the transaction with its reason.
            return (r, 0);
        }
        out[at] = kind;
        out[at + 1] = r;
        out[at + 2..at + 6].copy_from_slice(&(rn as u32).to_le_bytes());
        at += 6 + rn;
    }
    (KV_RESULT_ETCD_TXN, at)
}

fn apply_delete<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(count) = read_u16(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut deleted: i64 = 0;
    let mut i = 0;
    while i < count {
        let Some(key) = read_key(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        match remove_noted(store, key, now_ms) {
            Some(true) => deleted += 1,
            Some(false) => {}
            None => return (KV_RESULT_INTERNAL, 0),
        }
        i += 1;
    }
    if deleted > 0 {
        store.bump_revision();
    }
    if out.len() < 8 {
        return (KV_RESULT_INTERNAL, 0);
    }
    out[..8].copy_from_slice(&deleted.to_le_bytes());
    (KV_RESULT_INTEGER, 8)
}

fn apply_incr_or_decr<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    is_incr: bool,
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(key) = read_key(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(delta_raw) = read_i64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let delta = if is_incr { delta_raw } else { -delta_raw };

    let mut key_copy = [0u8; MAX_KEY_LEN];
    let kn = key.len().min(MAX_KEY_LEN);
    key_copy[..kn].copy_from_slice(&key[..kn]);
    let key_slice = &key_copy[..kn];

    // The live lookup sees an expired record as absent, so INCR on an
    // expired key starts fresh from 0.
    let mut scratch = [0u8; MAX_VALUE_LEN];
    let (exists, current): (bool, i64) = match store.get_live(key_slice, now_ms, &mut scratch) {
        GetOutcome::Found { value_len, .. } => match parse_decimal_i64(&scratch[..value_len]) {
            Some(n) => (true, n),
            None => return (KV_RESULT_WRONG_TYPE, 0),
        },
        GetOutcome::Absent => (false, 0),
        GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
    };
    let new_value = match current.checked_add(delta) {
        Some(v) => v,
        None => return (KV_RESULT_WRONG_TYPE, 0),
    };

    let mut buf = [0u8; 32];
    let n = format_decimal_i64(new_value, &mut buf);
    let formatted = &buf[..n];

    if exists {
        if !store.update_existing(key_slice, formatted) {
            return (KV_RESULT_INTERNAL, 0);
        }
    } else if !store.insert_new(key_slice, formatted) {
        return (KV_RESULT_INTERNAL, 0);
    }

    if out.len() < 8 {
        return (KV_RESULT_INTERNAL, 0);
    }
    out[..8].copy_from_slice(&new_value.to_le_bytes());
    (KV_RESULT_INTEGER, 8)
}

fn apply_append<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    apply_concat(store, body, out, now_ms, false)
}

fn apply_prepend<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    apply_concat(store, body, out, now_ms, true)
}

/// APPEND/PREPEND share everything except which side the new bytes
/// join. Both preserve the record's TTL, bump the revision, and
/// create-on-absent with just the new bytes.
fn apply_concat<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
    prepend: bool,
) -> (u8, usize) {
    let mut off = 0;
    let Some(key) = read_key(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(add) = read_value(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut key_copy = [0u8; MAX_KEY_LEN];
    let kn = key.len().min(MAX_KEY_LEN);
    key_copy[..kn].copy_from_slice(&key[..kn]);
    let key_slice = &key_copy[..kn];

    // The live lookup sees an expired record as absent, so
    // APPEND/PREPEND start fresh rather than extending a stale value.
    let mut scratch = [0u8; MAX_VALUE_LEN];
    let new_len: usize = match store.get_live(key_slice, now_ms, &mut scratch) {
        // A large value is not extended byte-wise in place.
        GetOutcome::Found { meta, .. } if meta.flags & RECORD_MANIFEST != 0 => {
            return (KV_RESULT_MANIFEST, 0);
        }
        GetOutcome::Found {
            value_len: existing_len,
            ..
        } => {
            let combined_len = existing_len + add.len();
            if combined_len > MAX_VALUE_LEN {
                return (KV_RESULT_INTERNAL, 0);
            }
            // Stage the combined value so neither side is clobbered
            // mid-copy.
            let mut tmp = [0u8; MAX_VALUE_LEN];
            if prepend {
                tmp[..add.len()].copy_from_slice(add);
                tmp[add.len()..combined_len].copy_from_slice(&scratch[..existing_len]);
            } else {
                tmp[..existing_len].copy_from_slice(&scratch[..existing_len]);
                tmp[existing_len..combined_len].copy_from_slice(add);
            }
            if !store.update_existing(key_slice, &tmp[..combined_len]) {
                return (KV_RESULT_INTERNAL, 0);
            }
            combined_len
        }
        GetOutcome::Absent => {
            if !store.insert_new(key_slice, add) {
                return (KV_RESULT_INTERNAL, 0);
            }
            add.len()
        }
        GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
    };

    if out.len() < 8 {
        return (KV_RESULT_INTERNAL, 0);
    }
    out[..8].copy_from_slice(&(new_len as i64).to_le_bytes());
    (KV_RESULT_INTEGER, 8)
}

fn apply_exists<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(key) = read_key(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut scratch = [0u8; MAX_VALUE_LEN];
    let count: i64 = match store.get_live(key, now_ms, &mut scratch) {
        GetOutcome::Found { .. } => 1,
        GetOutcome::Absent => 0,
        GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
    };
    if out.len() < 8 {
        return (KV_RESULT_INTERNAL, 0);
    }
    out[..8].copy_from_slice(&count.to_le_bytes());
    (KV_RESULT_INTEGER, 8)
}

fn apply_strlen<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(key) = read_key(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut scratch = [0u8; MAX_VALUE_LEN];
    let len: i64 = match store.get_live(key, now_ms, &mut scratch) {
        // A large value's length is its manifest's total.
        GetOutcome::Found { value_len, meta } if meta.flags & RECORD_MANIFEST != 0 => {
            match kv_chunks::Manifest::parse(&scratch[..value_len]) {
                Some(m) => m.total as i64,
                None => return (KV_RESULT_INTERNAL, 0),
            }
        }
        GetOutcome::Found { value_len, .. } => value_len as i64,
        GetOutcome::Absent => 0,
        GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
    };
    if out.len() < 8 {
        return (KV_RESULT_INTERNAL, 0);
    }
    out[..8].copy_from_slice(&len.to_le_bytes());
    (KV_RESULT_INTEGER, 8)
}

fn apply_mget<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(count) = read_u16(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    if out.len() < 2 {
        return (KV_RESULT_INTERNAL, 0);
    }
    let mut out_off = 0;
    out[out_off..out_off + 2].copy_from_slice(&count.to_le_bytes());
    out_off += 2;
    // The flags byte after the keys: a reader that reads manifests.
    let mut end = off;
    for _ in 0..count {
        if read_key(body, &mut end).is_none() {
            return (KV_RESULT_INTERNAL, 0);
        }
    }
    let marked = body
        .get(end)
        .is_some_and(|f| f & types::MGET_FLAG_MANIFESTS != 0);
    let mut scratch = [0u8; MAX_VALUE_LEN];
    let mut i = 0;
    while i < count {
        let Some(key) = read_key(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let flags = match store.get_live(key, now_ms, &mut scratch) {
            // Unmarked, a manifest refuses the whole op rather than being
            // answered as its bytes.
            GetOutcome::Found { meta, .. } if meta.flags & RECORD_MANIFEST != 0 && !marked => {
                return (KV_RESULT_MANIFEST, 0);
            }
            GetOutcome::Found { value_len: n, meta } => {
                if out_off + 4 + n > out.len() {
                    return (KV_RESULT_INTERNAL, 0);
                }
                out[out_off..out_off + 4].copy_from_slice(&(n as u32).to_le_bytes());
                out_off += 4;
                out[out_off..out_off + n].copy_from_slice(&scratch[..n]);
                out_off += n;
                meta.flags & RECORD_MANIFEST
            }
            GetOutcome::Absent => {
                if out_off + 4 > out.len() {
                    return (KV_RESULT_INTERNAL, 0);
                }
                out[out_off..out_off + 4].copy_from_slice(&KV_ARRAY_ELEMENT_NULL.to_le_bytes());
                out_off += 4;
                0
            }
            GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
        };
        if marked {
            let Some(o) = out.get_mut(out_off) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            *o = flags;
            out_off += 1;
        }
        i += 1;
    }
    (KV_RESULT_ARRAY, out_off)
}

fn apply_mset<M: Materializer>(
    store: &mut M,
    body: &[u8],
    _out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0;
    let Some(count) = read_u16(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    // The marks after the pairs: which values are manifests. Each is
    // checked — its root stored — before any pair is written.
    let pairs_at = off;
    let mut end = off;
    for _ in 0..count {
        if read_key(body, &mut end).is_none() || read_value(body, &mut end).is_none() {
            return (KV_RESULT_INTERNAL, 0);
        }
    }
    let marks = match body.get(end..) {
        Some([]) => None,
        Some(m) if m.len() == usize::from(count) => Some(m),
        _ => return (KV_RESULT_INTERNAL, 0),
    };
    let marked = |i: u16| marks.is_some_and(|m| m.get(usize::from(i)) == Some(&1));
    if marks.is_some() {
        let mut p = pairs_at;
        for i in 0..count {
            let (_, Some(value)) = (read_key(body, &mut p), read_value(body, &mut p)) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            if marked(i) {
                let Some(m) = kv_chunks::Manifest::parse(value) else {
                    return (KV_RESULT_BAD_CHUNK, 0);
                };
                match chunk_stored(store, &m.root, now_ms) {
                    Some(true) => {}
                    Some(false) => return (KV_RESULT_MISSING_CHUNK, 0),
                    None => return (KV_RESULT_INTERNAL, 0),
                }
            }
        }
    }
    let mut scratch = [0u8; MAX_VALUE_LEN];
    let mut i = 0;
    while i < count {
        let Some(key) = read_key(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let Some(value) = read_value(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let manifest = if marked(i) {
            kv_chunks::Manifest::parse(value)
        } else {
            None
        };
        let mut key_copy = [0u8; MAX_KEY_LEN];
        let kn = key.len().min(MAX_KEY_LEN);
        key_copy[..kn].copy_from_slice(&key[..kn]);
        let key_slice = &key_copy[..kn];

        let (exists, old_root) = match store.get_live(key_slice, now_ms, &mut scratch) {
            GetOutcome::Found { value_len, meta } => {
                (true, manifest_root(&scratch[..value_len], &meta))
            }
            GetOutcome::Absent => (false, None),
            GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
        };
        // Each key its own revision, as before; a released manifest's
        // bookkeeping in the key's.
        let ok = batched(store, |s| {
            let wrote = if exists {
                s.update_existing(key_slice, value)
            } else {
                s.insert_new(key_slice, value)
            };
            // A manifest holds its root; the one replaced releases its own.
            wrote
                && manifest.is_none_or(|m| {
                    s.set_flags(key_slice, RECORD_MANIFEST) && incref_chunk(s, &m.root, now_ms)
                })
                && old_root.is_none_or(|root| release_root(s, &root, now_ms))
        });
        if !ok {
            return (KV_RESULT_INTERNAL, 0);
        }
        // MSET clears TTL — Redis semantics.
        if !store.set_expiry(key_slice, 0) {
            return (KV_RESULT_INTERNAL, 0);
        }
        i += 1;
    }
    (KV_RESULT_OK, 0)
}

fn apply_scan<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    store.scan_op(body, out, now_ms)
}

// ── Body builders (host-side convenience for building op bodies) ─────
//
// The PIC anchor builds bodies inline in its `build_op_body` function.
// Host tests benefit from typed helpers; expose them here so a test
// can write `body_put(...)` rather than re-derive the body shape.

pub fn body_put_get(out: &mut [u8], key: &[u8]) -> Option<usize> {
    if out.len() < 2 + key.len() {
        return None;
    }
    out[..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    out[2..2 + key.len()].copy_from_slice(key);
    Some(2 + key.len())
}

pub fn body_put_set(
    out: &mut [u8],
    key: &[u8],
    value: &[u8],
    flags: u8,
    expiry_ms: u64,
) -> Option<usize> {
    let need = 2 + key.len() + 4 + value.len() + 1 + 8;
    if out.len() < need {
        return None;
    }
    out[..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    let mut p = 2;
    out[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    out[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    p += 4;
    out[p..p + value.len()].copy_from_slice(value);
    p += value.len();
    out[p] = flags;
    p += 1;
    out[p..p + 8].copy_from_slice(&expiry_ms.to_le_bytes());
    Some(need)
}

/// `KV_OP_GET_AT` body: `[revision:u64 LE][key_len:u16 LE][key…]`.
pub fn body_get_at(out: &mut [u8], revision: u64, key: &[u8]) -> Option<usize> {
    let need = 8 + 2 + key.len();
    if out.len() < need {
        return None;
    }
    out[..8].copy_from_slice(&revision.to_le_bytes());
    out[8..10].copy_from_slice(&(key.len() as u16).to_le_bytes());
    out[10..need].copy_from_slice(key);
    Some(need)
}

/// `KV_OP_SCAN_AT` body: `[revision:u64][cursor:u64][limit:u16]`, LE.
pub fn body_scan_at(out: &mut [u8], revision: u64, cursor: u64, limit: u16) -> Option<usize> {
    if out.len() < 18 {
        return None;
    }
    out[..8].copy_from_slice(&revision.to_le_bytes());
    out[8..16].copy_from_slice(&cursor.to_le_bytes());
    out[16..18].copy_from_slice(&limit.to_le_bytes());
    Some(18)
}

/// One comparison clause inside a TXN body. `witness` is the
/// `mod_revision` to compare against (`0` matches an absent key).
pub struct TxnCmp<'a> {
    pub cmp_op: u8,
    pub key: &'a [u8],
    pub witness: u64,
}

/// One pre-encoded op inside a TXN branch. `body` must already be in
/// the per-op layout (e.g. produced by `body_put_get`, `body_put_set`,
/// etc.). Use these in `body_put_txn`'s `then_ops` / `else_ops`.
pub struct TxnOp<'a> {
    pub op: u8,
    pub body: &'a [u8],
}

/// Build a `KV_OP_TXN` body. See `KV_OP_TXN` in
/// `modules/common/types.rs`.
pub fn body_put_txn(
    out: &mut [u8],
    cmps: &[TxnCmp<'_>],
    then_ops: &[TxnOp<'_>],
    else_ops: &[TxnOp<'_>],
) -> Option<usize> {
    if cmps.len() > u16::MAX as usize
        || then_ops.len() > u16::MAX as usize
        || else_ops.len() > u16::MAX as usize
    {
        return None;
    }
    let mut p = 0usize;
    if out.len() < 2 {
        return None;
    }
    out[p..p + 2].copy_from_slice(&(cmps.len() as u16).to_le_bytes());
    p += 2;
    for c in cmps {
        let need = 1 + 2 + c.key.len() + 8;
        if p + need > out.len() {
            return None;
        }
        out[p] = c.cmp_op;
        p += 1;
        out[p..p + 2].copy_from_slice(&(c.key.len() as u16).to_le_bytes());
        p += 2;
        out[p..p + c.key.len()].copy_from_slice(c.key);
        p += c.key.len();
        out[p..p + 8].copy_from_slice(&c.witness.to_le_bytes());
        p += 8;
    }
    for list in [then_ops, else_ops] {
        if p + 2 > out.len() {
            return None;
        }
        out[p..p + 2].copy_from_slice(&(list.len() as u16).to_le_bytes());
        p += 2;
        for op in list {
            if op.body.len() > u16::MAX as usize {
                return None;
            }
            let need = 1 + 2 + op.body.len();
            if p + need > out.len() {
                return None;
            }
            out[p] = op.op;
            p += 1;
            out[p..p + 2].copy_from_slice(&(op.body.len() as u16).to_le_bytes());
            p += 2;
            out[p..p + op.body.len()].copy_from_slice(op.body);
            p += op.body.len();
        }
    }
    Some(p)
}

/// Decode a `KV_RESULT_TXN` body into `(succeeded, op_results)` where
/// each op result is `(result_code, body)`. Host-side convenience for
/// tests / anchors that re-emit per-op responses.
///
/// Host-only (uses `alloc::vec::Vec`). Gated off the no_std PIC build
/// via the `#[cfg]` below, same pattern as `read_result_array`.
#[cfg(test)]
pub fn read_result_txn(body: &[u8]) -> Option<(bool, alloc::vec::Vec<(u8, alloc::vec::Vec<u8>)>)> {
    use alloc::vec::Vec;
    if body.len() < 3 {
        return None;
    }
    let succeeded = body[0] != 0;
    let op_count = u16::from_le_bytes([body[1], body[2]]) as usize;
    let mut out: Vec<(u8, Vec<u8>)> = Vec::with_capacity(op_count);
    let mut p = 3usize;
    for _ in 0..op_count {
        if p + 3 > body.len() {
            return None;
        }
        let code = body[p];
        let blen = u16::from_le_bytes([body[p + 1], body[p + 2]]) as usize;
        p += 3;
        if p + blen > body.len() {
            return None;
        }
        out.push((code, body[p..p + blen].to_vec()));
        p += blen;
    }
    Some((succeeded, out))
}

pub fn body_put_cas(out: &mut [u8], key: &[u8], witness: u64, value: &[u8]) -> Option<usize> {
    let need = 2 + key.len() + 8 + 4 + value.len();
    if out.len() < need {
        return None;
    }
    out[..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    let mut p = 2;
    out[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    out[p..p + 8].copy_from_slice(&witness.to_le_bytes());
    p += 8;
    out[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    p += 4;
    out[p..p + value.len()].copy_from_slice(value);
    Some(need)
}

pub fn body_put_delete_one(out: &mut [u8], key: &[u8]) -> Option<usize> {
    if out.len() < 2 + 2 + key.len() {
        return None;
    }
    out[..2].copy_from_slice(&1u16.to_le_bytes());
    out[2..4].copy_from_slice(&(key.len() as u16).to_le_bytes());
    out[4..4 + key.len()].copy_from_slice(key);
    Some(4 + key.len())
}

pub fn body_put_incr(out: &mut [u8], key: &[u8], delta: i64) -> Option<usize> {
    if out.len() < 2 + key.len() + 8 {
        return None;
    }
    out[..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    out[2..2 + key.len()].copy_from_slice(key);
    out[2 + key.len()..2 + key.len() + 8].copy_from_slice(&delta.to_le_bytes());
    Some(2 + key.len() + 8)
}

pub fn body_put_append(out: &mut [u8], key: &[u8], suffix: &[u8]) -> Option<usize> {
    let need = 2 + key.len() + 4 + suffix.len();
    if out.len() < need {
        return None;
    }
    out[..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    let mut p = 2;
    out[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    out[p..p + 4].copy_from_slice(&(suffix.len() as u32).to_le_bytes());
    p += 4;
    out[p..p + suffix.len()].copy_from_slice(suffix);
    Some(need)
}

pub fn body_put_mget(out: &mut [u8], keys: &[&[u8]]) -> Option<usize> {
    let total_keys_bytes: usize = keys.iter().map(|k| 2 + k.len()).sum();
    let need = 2 + total_keys_bytes;
    if out.len() < need {
        return None;
    }
    out[..2].copy_from_slice(&(keys.len() as u16).to_le_bytes());
    let mut p = 2;
    for key in keys {
        out[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        p += 2;
        out[p..p + key.len()].copy_from_slice(key);
        p += key.len();
    }
    Some(need)
}

pub fn body_put_mset(out: &mut [u8], pairs: &[(&[u8], &[u8])]) -> Option<usize> {
    let total: usize = pairs.iter().map(|(k, v)| 2 + k.len() + 4 + v.len()).sum();
    let need = 2 + total;
    if out.len() < need {
        return None;
    }
    out[..2].copy_from_slice(&(pairs.len() as u16).to_le_bytes());
    let mut p = 2;
    for (key, value) in pairs {
        out[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        p += 2;
        out[p..p + key.len()].copy_from_slice(key);
        p += key.len();
        out[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
        p += 4;
        out[p..p + value.len()].copy_from_slice(value);
        p += value.len();
    }
    Some(need)
}

// ── Result body readers (host-side decoders) ─────────────────────────

pub fn read_result_integer(body: &[u8]) -> Option<i64> {
    if body.len() < 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&body[..8]);
    Some(i64::from_le_bytes(buf))
}

/// Decode a KV_RESULT_ARRAY body into a vec of `Option<Vec<u8>>` where
/// `None` represents the array's null-bulk sentinel.
///
/// Host-only (uses `alloc::vec::Vec`). Gated off the no_std PIC build
/// via the `#[cfg]` below.
#[cfg(test)]
pub fn read_result_array(body: &[u8]) -> Option<alloc::vec::Vec<Option<alloc::vec::Vec<u8>>>> {
    if body.len() < 2 {
        return None;
    }
    let count = u16::from_le_bytes([body[0], body[1]]) as usize;
    let mut out = alloc::vec::Vec::with_capacity(count);
    let mut off = 2;
    for _ in 0..count {
        if off + 4 > body.len() {
            return None;
        }
        let len = u32::from_le_bytes([body[off], body[off + 1], body[off + 2], body[off + 3]]);
        off += 4;
        if len == KV_ARRAY_ELEMENT_NULL {
            out.push(None);
        } else {
            let n = len as usize;
            if off + n > body.len() {
                return None;
            }
            out.push(Some(body[off..off + n].to_vec()));
            off += n;
        }
    }
    Some(out)
}

#[cfg(test)]
extern crate alloc;

// ── Cross-range transactions ───────────────────────────────────────────
//
// Two-phase commit built entirely out of ordinary replicated records, so
// that both state-store providers get it identically and every existing
// crash-injection and differential test covers it for free.
//
// An intent is a record at `KS_TXN_INTENT || user_key` whose value is an
// encoded `IntentRecord` followed by the STAGED OP VERBATIM — the same
// `[op_byte][body]` bytes that a single-range `KV_OP_TXN` would have
// applied. Resolution replays those bytes through `apply_mat`, which
// means a committed cross-range write executes down exactly the code
// path its single-range twin does. There is no second write path to
// keep in agreement with the first.
//
// The home `TransactionRecord` lives at `KS_TXN_RECORD || txn_id` and is
// the only authority on the outcome. Participants never decide; they
// stage, and they resolve when told.

/// Keyspace holding write intents, keyed by the user key they shadow.
pub const KS_TXN_INTENT: u32 = 0x8009_0001;
/// Keyspace holding home transaction records, keyed by transaction id.
pub const KS_TXN_RECORD: u32 = 0x8009_0002;

/// 4-byte big-endian keyspace prefix, matching `sql_exec::kv_key`.
const KEYSPACE_PREFIX_LEN: usize = 4;

/// Build `KS_TXN_INTENT || key`. `None` if the result would exceed the
/// key bound — a fail-closed refusal, never a truncated key: a truncated
/// intent key would shadow the WRONG user key.
fn intent_key(out: &mut [u8; MAX_KEY_LEN], key: &[u8]) -> Option<usize> {
    let total = KEYSPACE_PREFIX_LEN + key.len();
    if total > MAX_KEY_LEN {
        return None;
    }
    out[0..KEYSPACE_PREFIX_LEN].copy_from_slice(&KS_TXN_INTENT.to_be_bytes());
    out[KEYSPACE_PREFIX_LEN..total].copy_from_slice(key);
    Some(total)
}

/// Build `KS_TXN_RECORD || txn_id`.
fn txn_record_key(out: &mut [u8; MAX_KEY_LEN], txn_id: u128) -> usize {
    out[0..KEYSPACE_PREFIX_LEN].copy_from_slice(&KS_TXN_RECORD.to_be_bytes());
    out[KEYSPACE_PREFIX_LEN..KEYSPACE_PREFIX_LEN + 16].copy_from_slice(&txn_id.to_le_bytes());
    KEYSPACE_PREFIX_LEN + 16
}

/// The single key a stageable op writes. `None` means "this op has no
/// single provable key", which refuses the prepare rather than staging
/// an intent that shadows nothing.
fn txn_op_key<'a>(op: u8, body: &'a [u8]) -> Option<&'a [u8]> {
    let read_at = |off: usize| -> Option<&'a [u8]> {
        let klen = u16::from_le_bytes([*body.get(off)?, *body.get(off + 1)?]) as usize;
        body.get(off + 2..off + 2 + klen)
    };
    match op {
        KV_OP_PUT | KV_OP_CAS | KV_OP_INCR | KV_OP_DECR | KV_OP_APPEND | KV_OP_PREPEND => {
            read_at(0)
        }
        // A DELETE body is a key LIST; only the single-key form has one
        // provable key.
        KV_OP_DELETE => {
            if body.len() >= 2 && u16::from_le_bytes([body[0], body[1]]) == 1 {
                read_at(2)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Read the intent shadowing `key`, if any, into `buf`.
fn read_intent<M: Materializer>(
    store: &mut M,
    key: &[u8],
    now_ms: u64,
    buf: &mut [u8; MAX_VALUE_LEN],
) -> Option<usize> {
    let mut ik = [0u8; MAX_KEY_LEN];
    let ik_len = intent_key(&mut ik, key)?;
    match store.get_live(&ik[..ik_len], now_ms, buf) {
        GetOutcome::Found { value_len, .. } => Some(value_len),
        _ => None,
    }
}

/// Does `key` carry an unresolved intent? Returns the deciding
/// authority so the caller can name it in a `KV_RESULT_TXN_PENDING`.
///
/// Called on every point read. The cost is one extra lookup per GET,
/// paid so that a reader can never see a staged write as though it were
/// committed, nor see the pre-intent value as though nothing were in
/// flight.
fn intent_on<M: Materializer>(store: &mut M, key: &[u8], now_ms: u64) -> Option<(u128, [u8; 16])> {
    // The transaction keyspaces are never themselves shadowed. A
    // coordinator reads intents and records directly, and recursing
    // would only ever miss — one wasted lookup per read of the very
    // keys the resolution path reads most.
    if key.len() >= KEYSPACE_PREFIX_LEN {
        let ks = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
        if ks == KS_TXN_INTENT || ks == KS_TXN_RECORD {
            return None;
        }
    }
    let mut buf = [0u8; MAX_VALUE_LEN];
    let vlen = read_intent(store, key, now_ms, &mut buf)?;
    let rec = txn::IntentRecord::decode(buf.get(..txn::INTENT_WIRE_LEN)?)?;

    // HELPING. Before reporting the key undecided, try
    // to decide it here. If the home record happens to live in this
    // partition — which is every transaction whose home range is the
    // one being read, and the common case for a reader that arrives
    // after the coordinator died — the authority is a local lookup
    // away, and `resolve_intent` turns it into an answer.
    //
    // This is helping in the narrow sense: the reader
    // does not DECIDE anything. It reads the record that already
    // decided, and applies it. A record that is still Pending or
    // Staging, or that is not here at all, leaves the intent exactly
    // as it was and the caller still gets TXN_PENDING. Nothing about
    // this path can turn an undecided transaction into a value.
    let mut rk = [0u8; MAX_KEY_LEN];
    let rk_len = txn_record_key(&mut rk, rec.transaction_id);
    // Sized to the record, not to MAX_VALUE_LEN. This sits on the
    // point-read path inside a no_std module with a bounded stack, and
    // a transaction record can never exceed its own maximum wire form.
    let mut rbuf = [0u8; txn::TXN_RECORD_MAX_WIRE_LEN];
    if let GetOutcome::Found { value_len, .. } = store.get_live(&rk[..rk_len], now_ms, &mut rbuf) {
        if let Some(record) = txn::TransactionRecord::decode(&rbuf[..value_len]) {
            match txn::resolve_intent(&rec, &record) {
                txn::IntentResolution::Commit(_) => {
                    // Publish the staged op, exactly as TXN_RESOLVE
                    // would — same bytes, same apply path.
                    let staged = buf.get(txn::INTENT_WIRE_LEN..vlen)?;
                    let (&op_byte, sub) = staged.split_first()?;
                    let mut sub_out = [0u8; 1024];
                    let (r, _) = apply_mat(store, op_byte, sub, &mut sub_out, now_ms);
                    if r == KV_RESULT_INTERNAL {
                        // Could not publish. Leave the intent alone and
                        // report undecided rather than half-resolve it.
                        return Some((rec.transaction_id, rec.home_range_id));
                    }
                    clear_intent(store, key, now_ms);
                    return None;
                }
                txn::IntentResolution::Abort => {
                    clear_intent(store, key, now_ms);
                    return None;
                }
                // Undecided, wrong authority, or an abandoned epoch:
                // not this reader's to settle.
                _ => {}
            }
        }
    }
    Some((rec.transaction_id, rec.home_range_id))
}

/// Drop a spent intent. A failure here is not fatal to the read: the
/// value is already correct, and the stale intent will be cleared by
/// the next resolve or the next helping reader.
fn clear_intent<M: Materializer>(store: &mut M, key: &[u8], now_ms: u64) {
    let mut ik = [0u8; MAX_KEY_LEN];
    if let Some(n) = intent_key(&mut ik, key) {
        let _ = store.remove(&ik[..n], now_ms);
    }
}

/// Emit the `KV_RESULT_TXN_PENDING` body: who decides this key.
fn pending_body(out: &mut [u8], txn_id: u128, home: [u8; 16]) -> (u8, usize) {
    if out.len() < 32 {
        return (KV_RESULT_INTERNAL, 0);
    }
    out[0..16].copy_from_slice(&txn_id.to_le_bytes());
    out[16..32].copy_from_slice(&home);
    (KV_RESULT_TXN_PENDING, 32)
}

/// `KV_OP_TXN_PREPARE`: evaluate this participant's comparisons and
/// stage its writes, or vote to abort having staged nothing.
fn apply_txn_prepare<M: Materializer>(
    store: &mut M,
    body: &[u8],
    _out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    if body.len() < 16 + 16 + 8 + 4 {
        return (KV_RESULT_INTERNAL, 0);
    }
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&body[0..16]);
    let txn_id = u128::from_le_bytes(id_bytes);
    let mut home = [0u8; 16];
    home.copy_from_slice(&body[16..32]);
    let mut off = 32usize;
    let Some(provisional_ts) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let epoch = u32::from_le_bytes([body[40], body[41], body[42], body[43]]);
    off += 4;
    let txn_body = &body[off..];

    // A zero id or an unleased timestamp is not a transaction. Refuse
    // rather than stage intents no coordinator can be found for.
    if txn_id == 0 || provisional_ts == 0 {
        return (KV_RESULT_INTERNAL, 0);
    }

    let intent = txn::IntentRecord {
        transaction_id: txn_id,
        home_range_id: home,
        provisional_timestamp: provisional_ts,
        epoch,
        kind: txn::IntentKind::Write,
    };
    let mut intent_hdr = [0u8; txn::INTENT_WIRE_LEN];
    if intent.encode(&mut intent_hdr).is_none() {
        return (KV_RESULT_INTERNAL, 0);
    }

    // ── Comparisons, against live state ──────────────────────────────
    let mut coff = 0usize;
    let Some(cmp_count) = read_u16(txn_body, &mut coff) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut c = 0u16;
    while c < cmp_count {
        let Some(cmp_op) = read_u8(txn_body, &mut coff) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let Some(key) = read_key(txn_body, &mut coff) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let Some(witness) = read_u64(txn_body, &mut coff) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        // A key under someone else's unresolved intent cannot be
        // compared: its value is not yet decided. Vote abort rather
        // than compare against a value that may be about to change.
        if let Some((other, _)) = intent_on(store, key, now_ms) {
            if other != txn_id {
                return (KV_RESULT_CAS_FAILED, 0);
            }
        }
        let mut scratch = [0u8; MAX_VALUE_LEN];
        let current_rev = match store.get_live(key, now_ms, &mut scratch) {
            GetOutcome::Found { meta, .. } => meta.mod_revision,
            GetOutcome::Absent => 0,
            GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
        };
        let ok = match cmp_op {
            TXN_CMP_MOD_EQUAL => current_rev == witness,
            TXN_CMP_MOD_NOT_EQUAL => current_rev != witness,
            TXN_CMP_MOD_GREATER => current_rev > witness,
            TXN_CMP_MOD_LESS => current_rev < witness,
            _ => return (KV_RESULT_INTERNAL, 0),
        };
        if !ok {
            // Vote abort. Nothing staged, so nothing to unwind.
            return (KV_RESULT_CAS_FAILED, 0);
        }
        c += 1;
    }

    // ── Stage the THEN branch ────────────────────────────────────────
    //
    // Only THEN. A cross-range transaction whose comparisons hold takes
    // the THEN branch by construction; the ELSE branch is the
    // single-range conditional form and has no meaning once the
    // coordinator has already decided to prepare.
    let Some(then_count) = read_u16(txn_body, &mut coff) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    // Two passes: prove every op stageable and unconflicted BEFORE
    // writing any intent. Prepare is all-or-nothing within the
    // participant — a partial stage leaves intents that the abort path
    // does not know the keys of.
    let scan_start = coff;
    for pass in 0..2 {
        let mut soff = scan_start;
        let mut k = 0u16;
        while k < then_count {
            let Some(op_byte) = read_u8(txn_body, &mut soff) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            let Some(blen) = read_u16(txn_body, &mut soff) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            let Some(sub) = txn_body.get(soff..soff + blen as usize) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            soff += blen as usize;
            let Some(key) = txn_op_key(op_byte, sub) else {
                return (KV_RESULT_CAS_FAILED, 0);
            };
            let mut ik = [0u8; MAX_KEY_LEN];
            let Some(ik_len) = intent_key(&mut ik, key) else {
                return (KV_RESULT_CAS_FAILED, 0);
            };
            if pass == 0 {
                // Write-write conflict: someone else's intent already
                // shadows this key. First writer wins; this
                // participant votes abort.
                if let Some((other, _)) = intent_on(store, key, now_ms) {
                    if other != txn_id {
                        return (KV_RESULT_CAS_FAILED, 0);
                    }
                }
            } else {
                let total = txn::INTENT_WIRE_LEN + 1 + sub.len();
                if total > MAX_VALUE_LEN {
                    return (KV_RESULT_CAS_FAILED, 0);
                }
                let mut val = [0u8; MAX_VALUE_LEN];
                val[..txn::INTENT_WIRE_LEN].copy_from_slice(&intent_hdr);
                val[txn::INTENT_WIRE_LEN] = op_byte;
                val[txn::INTENT_WIRE_LEN + 1..total].copy_from_slice(sub);
                let ik = &ik[..ik_len];
                let mut probe = [0u8; MAX_VALUE_LEN];
                let existed = matches!(
                    store.get_live(ik, now_ms, &mut probe),
                    GetOutcome::Found { .. }
                );
                let wrote = if existed {
                    store.update_existing(ik, &val[..total])
                } else {
                    store.insert_new(ik, &val[..total])
                };
                if !wrote {
                    return (KV_RESULT_INTERNAL, 0);
                }
            }
            k += 1;
        }
    }

    (KV_RESULT_OK, 0)
}

/// `KV_OP_TXN_RESOLVE`: make staged intents real, or drop them.
fn apply_txn_resolve<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    if body.len() < 16 + 16 + 4 + 1 + 8 + 2 {
        return (KV_RESULT_INTERNAL, 0);
    }
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&body[0..16]);
    let txn_id = u128::from_le_bytes(id_bytes);
    let mut home = [0u8; 16];
    home.copy_from_slice(&body[16..32]);
    let epoch = u32::from_le_bytes([body[32], body[33], body[34], body[35]]);
    let committed = body[36] != 0;
    // The transaction's MVCC commit timestamp: every
    // version a committed resolve materializes carries it, so a
    // multi-range transaction's events share one timestamp on the
    // change feed regardless of which range resolved when.
    let commit_ts = u64::from_le_bytes([
        body[37], body[38], body[39], body[40], body[41], body[42], body[43], body[44],
    ]);
    let mut off = 45usize;
    let Some(key_count) = read_u16(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };

    let mut resolved: i64 = 0;
    let mut k = 0u16;
    while k < key_count {
        let Some(key) = read_key(body, &mut off) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let mut ik = [0u8; MAX_KEY_LEN];
        let Some(ik_len) = intent_key(&mut ik, key) else {
            return (KV_RESULT_INTERNAL, 0);
        };
        let mut val = [0u8; MAX_VALUE_LEN];
        let vlen = match store.get_live(&ik[..ik_len], now_ms, &mut val) {
            GetOutcome::Found { value_len, .. } => value_len,
            // No intent: already resolved, or never staged here.
            // Resolution is retried until acked, so this is the
            // expected steady state of a redelivery, not an error.
            GetOutcome::Absent => {
                k += 1;
                continue;
            }
            GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
        };
        let Some(rec) = val
            .get(..txn::INTENT_WIRE_LEN)
            .and_then(txn::IntentRecord::decode)
        else {
            return (KV_RESULT_INTERNAL, 0);
        };
        // Identity before outcome. An intent belonging to a different
        // transaction, or to a different incarnation of this one, is
        // LEFT IN PLACE: resolving it would decide someone else's
        // transaction from this one's authority.
        if rec.transaction_id != txn_id || rec.home_range_id != home || rec.epoch != epoch {
            k += 1;
            continue;
        }
        if committed {
            let staged = &val[txn::INTENT_WIRE_LEN..vlen];
            let Some((&op_byte, sub)) = staged.split_first() else {
                return (KV_RESULT_INTERNAL, 0);
            };
            let mut sub_out = [0u8; 1024];
            // The staged op materializes under the TRANSACTION's commit
            // timestamp, not the resolve command's own — this is the
            // stamp that makes intents ordinary records with the right
            // MVCC identity.
            store.set_commit_ts(commit_ts);
            let (r, _) = apply_mat(store, op_byte, sub, &mut sub_out, now_ms);
            // The staged op already passed its comparisons at prepare
            // time and the coordinator has committed. A failure here is
            // a fault in this participant, not a transaction outcome.
            if r == KV_RESULT_INTERNAL {
                return (KV_RESULT_INTERNAL, 0);
            }
        }
        // Whether committed or aborted, the intent is spent.
        if store.remove(&ik[..ik_len], now_ms).is_none() {
            return (KV_RESULT_INTERNAL, 0);
        }
        resolved += 1;
        k += 1;
    }

    if out.len() < 8 {
        return (KV_RESULT_INTERNAL, 0);
    }
    out[0..8].copy_from_slice(&resolved.to_le_bytes());
    (KV_RESULT_INTEGER, 8)
}

/// `KV_OP_TXN_RECORD`: write or advance the home transaction record,
/// refusing any status move the status lattice forbids.
fn apply_txn_record<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let Some(incoming) = txn::TransactionRecord::decode(body) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let mut rk = [0u8; MAX_KEY_LEN];
    let rk_len = txn_record_key(&mut rk, incoming.transaction_id);
    let rk = &rk[..rk_len];

    let mut cur = [0u8; MAX_VALUE_LEN];
    let existing = match store.get_live(rk, now_ms, &mut cur) {
        GetOutcome::Found { value_len, .. } => {
            match txn::TransactionRecord::decode(&cur[..value_len]) {
                Some(r) => Some(r),
                None => return (KV_RESULT_INTERNAL, 0),
            }
        }
        GetOutcome::Absent => None,
        GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
    };

    if let Some(prev) = existing {
        // This is where `Committed → Aborted` dies. A stale retry, an
        // expiry sweep working from an old read, and a recovered
        // participant replaying an abort all arrive here and all bounce.
        if txn::validate_status_transition(prev.status, incoming.status).is_err() {
            // Carry the EXISTING status back in the reply body. A refused
            // reopening `Pending` write is how a restarted coordinator
            // discovers a decision it made and then crashed mid-resolve;
            // the status byte lets it RESUME resolution rather than
            // conclude the outcome is unknown. A caller reading only the
            // result code ignores the extra byte.
            if let Some(slot) = out.first_mut() {
                *slot = prev.status as u8;
                return (KV_RESULT_CAS_FAILED, 1);
            }
            return (KV_RESULT_CAS_FAILED, 0);
        }
        if !store.update_existing(rk, body) {
            return (KV_RESULT_INTERNAL, 0);
        }
    } else if !store.insert_new(rk, body) {
        return (KV_RESULT_INTERNAL, 0);
    }
    (KV_RESULT_OK, 0)
}

// ── Exactly-once execution under a caller identity ────────────────────

/// Keyspace holding idempotency records, keyed by the caller's identity.
pub const KS_IDEMPOTENCY: u32 = 0x800A_0001;

/// Build `KS_IDEMPOTENCY || idempotency_key`.
fn idempotency_key_bytes(out: &mut [u8; MAX_KEY_LEN], id: u64) -> usize {
    out[0..KEYSPACE_PREFIX_LEN].copy_from_slice(&KS_IDEMPOTENCY.to_be_bytes());
    out[KEYSPACE_PREFIX_LEN..KEYSPACE_PREFIX_LEN + 8].copy_from_slice(&id.to_le_bytes());
    KEYSPACE_PREFIX_LEN + 8
}

/// `KV_OP_IDEMPOTENT`: run the inner op exactly once for this identity.
///
/// The record is written AFTER the mutation and in the same apply, so
/// the two are one replicated decision. Writing it first would let a
/// crash between the two suppress an op that never ran — which turns a
/// lost reply into a lost write, the failure this op exists to prevent.
fn apply_idempotent<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0usize;
    let Some(id) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(&inner_op) = body.get(off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    off += 1;
    let Some(inner) = body.get(off..) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    // A zero identity is not an identity. Refuse rather than collapse
    // every unidentified caller onto one record, which would make the
    // first such op suppress all the others.
    if id == 0 {
        return (KV_RESULT_INTERNAL, 0);
    }

    let mut ik = [0u8; MAX_KEY_LEN];
    let ik_len = idempotency_key_bytes(&mut ik, id);
    let mut rbuf = [0u8; txn::IDEMPOTENCY_RECORD_WIRE_LEN];
    match store.get_live(&ik[..ik_len], now_ms, &mut rbuf) {
        GetOutcome::Found { value_len, .. } => {
            // Already committed under this identity. Replay the answer
            // and do NOT re-execute.
            let Some(rec) = txn::IdempotencyRecord::decode(&rbuf[..value_len]) else {
                return (KV_RESULT_INTERNAL, 0);
            };
            return (rec.result, 0);
        }
        GetOutcome::Absent => {}
        GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
    }

    let (result, len) = apply_mat(store, inner_op, inner, out, now_ms);
    // Record only what actually happened. Recording a failed op would
    // make its retry replay the failure forever, and a retry after a
    // transient fault is exactly the case that must be allowed to
    // succeed.
    if result == KV_RESULT_INTERNAL {
        return (result, len);
    }
    let rec = txn::IdempotencyRecord {
        idempotency_key: id,
        committed_revision: store.revision(),
        result,
    };
    let mut enc = [0u8; txn::IDEMPOTENCY_RECORD_WIRE_LEN];
    if rec.encode(&mut enc).is_none() || !store.insert_new(&ik[..ik_len], &enc) {
        return (KV_RESULT_INTERNAL, 0);
    }
    (result, len)
}

/// `KV_OP_IDEMPOTENT_LOOKUP`: read an idempotency identity WITHOUT running
/// anything. The rediscovery route — a caller that lost its reply
/// but kept its intent asks whether the mutation committed, and is told
/// `Committed`/`DidNotHappen`/`Indeterminate` rather than made to re-submit
/// the whole op to find out. Purely read-only; writes no record.
fn apply_idempotent_lookup<M: Materializer>(
    store: &mut M,
    body: &[u8],
    out: &mut [u8],
    now_ms: u64,
) -> (u8, usize) {
    let mut off = 0usize;
    let Some(id) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    let Some(identity_revision) = read_u64(body, &mut off) else {
        return (KV_RESULT_INTERNAL, 0);
    };
    // A zero identity is not an identity — the same refusal the capture
    // path makes, for the same reason (every unidentified caller would
    // collapse onto one record).
    if id == 0 {
        return (KV_RESULT_INTERNAL, 0);
    }

    let mut ik = [0u8; MAX_KEY_LEN];
    let ik_len = idempotency_key_bytes(&mut ik, id);
    let mut rbuf = [0u8; txn::IDEMPOTENCY_RECORD_WIRE_LEN];
    let record = match store.get_live(&ik[..ik_len], now_ms, &mut rbuf) {
        GetOutcome::Found { value_len, .. } => {
            match txn::IdempotencyRecord::decode(&rbuf[..value_len]) {
                Some(rec) => Some(rec),
                None => return (KV_RESULT_INTERNAL, 0),
            }
        }
        GetOutcome::Absent => None,
        GetOutcome::TooBig | GetOutcome::Fault => return (KV_RESULT_INTERNAL, 0),
    };

    // The reclaim floor — NOT the read horizon — is what separates a
    // truthful "did not happen" from the honest "I can no longer tell".
    let outcome = txn::interpret_lookup(record.as_ref(), identity_revision, store.reclaim_floor());
    match outcome.encode(out) {
        Some(n) => (KV_RESULT_IDEMPOTENT_LOOKUP, n),
        None => (KV_RESULT_INTERNAL, 0),
    }
}
