//! Lattice's store as fluxor's store: the `storage.object` (0x14) and
//! `storage.namespace` (0x13) contracts served over the state worker's own
//! store, synchronously, inside the calling module's provider call.
//!
//! A graph whose store is lattice's (nanocloud's control plane) keeps every
//! connector as it is — they reach the store by contract — and gains one
//! clock, one history and one store for them, lattice's own surfaces and
//! the document executor alike. The wire is the host store's
//! (`fluxor/src/platform/linux/store.rs`) where a consumer can observe it,
//! and stricter where the host is lax: a short argument is `EINVAL`, never
//! a panic, and a LIST cursor is the key the next page starts at, stable
//! across writes (the host's is an index into a list that moves).
//!
//! Every contract operation is one or more lattice KV operations applied
//! through [`Backend::apply`] exactly as the worker applies a command, so a
//! provider write is journaled and swept like any other. A value past one
//! chunk is uploaded as a chunk tree straight from the caller's buffer and
//! read back whole through the large-value layer (`kv_large.rs`), in the
//! call.
//!
//! Subscriptions are notify + pull: after every write (the provider's own,
//! and each command the worker applies — the worker calls [`pump`]) the
//! changes since the last pump are read from the store's history, put in
//! revision order, and each is pushed, as the host pushes it, to every
//! subscriber whose prefix holds its key. A subscriber whose channel is
//! full is owed a LOST sentinel (read again) and is told as soon as its
//! channel has room. An event carries a value of at most one chunk; a
//! larger one is announced with [`EVENT_FLAG_VALUE_ELIDED`] and read by key
//! (every consumer takes an event as a wake-up).
//!
//! On a replica of a replicated store a write is not applied in the call:
//! it is proposed to the group's log under this replica's origin
//! (`origin.rs`), decided where it lands, and answered `EINPROGRESS` until
//! it has applied here and a position marker covers it — then with its
//! outcome and a `ReplicatedDurable` fence. The caller asks again with the
//! same request, which finds the same write: writes in flight are keyed by
//! the caller and the request's bytes. Reads, LIST, CHANGES and
//! subscriptions are this replica's, at the revision it has applied.
//!
//! History is the store's: CHANGES and subscriptions need a store that
//! keeps it (the disk provider). The memory provider answers every
//! question about the past LOST — honestly, and uselessly for a change
//! feed — so a worker on it does not take the provider role.
//!
//! Handles: object read slots are `0..SLOTS`, subscriptions from `SLOTS`,
//! so a fence query (`QUERY_OP`, which both contracts carry and which
//! reaches the provider with its tag stripped) names one thing.
//!
//! Mounted beside `types.rs`, `kv_large.rs`, `doc_tree.rs` and `origin.rs`
//! (reached as `super::…`).
//! No allocation and no panic path: the worker twin that mounts it is a
//! PIC module.

use super::doc_tree::utf8_is_valid;
use super::kv_large::kv_chunks::{
    sha256_of, Builder, Hash, Manifest, CHUNK_MAX, DATA_MAX, HASH_LEN, MANIFEST_LEN,
};
use super::kv_large::{self, Expander, Need};
use super::origin::{self, Outcome, Record, IMPORT_MARK_LEN, NO_OUTCOME, RING};
use super::types::{
    KV_OP_DELETE, KV_OP_ORIGIN_CHUNK, KV_OP_ORIGIN_IMPORT, KV_OP_ORIGIN_REGISTER,
    KV_OP_ORIGIN_WRITE, KV_OP_PUT, KV_OP_PUT_MANIFEST, KV_OP_RANGE_META, KV_OP_SCAN_VERSIONS,
    KV_OP_TXN, KV_RESULT_COMPACTED, KV_RESULT_INTEGER, KV_RESULT_KVS, KV_RESULT_MISSING_CHUNK,
    KV_RESULT_OK, KV_RESULT_PAUSED, KV_RESULT_TOO_LARGE, KV_RESULT_TXN, KV_RESULT_VERSIONS,
    RANGE_META_FLAG_MANIFESTS, RECORD_FLAG_MANIFEST, TXN_CMP_MOD_EQUAL, VERSIONS_FLAG_MANIFESTS,
    VERSIONS_FLAG_META, VERSION_KIND_DELETE,
};

// ── The contracts' wire ─────────────────────────────────────────────

pub const CONTRACT_NAMESPACE: u16 = 0x0013;
pub const CONTRACT_OBJECT: u16 = 0x0014;

pub const OBJ_PUT: u32 = 0x1420;
pub const OBJ_GET: u32 = 0x1421;
pub const OBJ_HEAD: u32 = 0x1422;
pub const OBJ_RANGE_GET: u32 = 0x1423;
pub const OBJ_DELETE: u32 = 0x1424;
pub const OBJ_CLOSE: u32 = 0x1425;
pub const NS_LIST: u32 = 0x1302;
pub const NS_SUBSCRIBE: u32 = 0x1305;
pub const NS_CLOSE: u32 = 0x1306;
pub const NS_CHANGES: u32 = 0x1307;
pub const NS_CAPS: u32 = 0x13FF;
/// `fence::QUERY_OP`: the fence of what a handle observed.
pub const QUERY_OP: u32 = 0x00FE;
/// `caps::SUBSCRIBE | caps::CHANGES`.
pub const NS_CAPS_SERVED: i32 = 0x18;

pub const ERROR: i32 = -1;
pub const ENXIO: i32 = -6;
pub const EAGAIN: i32 = -11;
pub const ENOMEM: i32 = -12;
pub const EEXIST: i32 = -17;
pub const EINVAL: i32 = -22;
/// A write taken and not yet decided: ask again with the same request.
pub const EINPROGRESS: i32 = -36;
pub const ENOSYS: i32 = -38;
pub const EOVERFLOW: i32 = -75;

pub const PRE_ANY: u8 = 0;
pub const PRE_ABSENT: u8 = 1;
pub const PRE_ETAG: u8 = 2;

/// Handle tags: `(tag << 26) | slot`.
pub const TAG_NAMESPACE: i32 = 22;
pub const TAG_OBJECT: i32 = 23;
const TAG_SHIFT: u32 = 26;
const SLOT_MASK: i32 = (1 << TAG_SHIFT) - 1;

/// The source fences and events carry: the host store's, so revisions a
/// consumer recorded against one remain comparable against the other.
pub const SOURCE: [u8; 16] = *b"fluxor-cp-store\0";
/// One store, one durability domain (the host store's constant).
pub const STORE_DEVICE_ID: u64 = 1;
pub const CT_NAMESPACE_CHANGE: u8 = 0x23;
pub const EVENT_HEAD: usize = 32;
/// An event's header `flags`: the change's value is not carried (it is
/// larger than an event holds); read it by key.
pub const EVENT_FLAG_VALUE_ELIDED: u8 = 0x01;
pub const KIND_ADDED: u8 = 0;
pub const KIND_MODIFIED: u8 = 1;
pub const KIND_DELETED: u8 = 2;

pub const FENCE_LOCAL_DURABLE: u8 = 1;
/// `[tag][source 16][commit_index u64][epoch u32][quorum u8][witness 32]`.
pub const FENCE_REPLICATED_DURABLE: u8 = 2;
const FENCE_REPLICATED_LEN: usize = 62;
pub const FENCE_REVISION_MONOTONE: u8 = 4;
pub const FENCE_VIEW_CONSISTENT: u8 = 5;
const FENCE_REVISION_LEN: usize = 25;
const FENCE_DURABLE_LEN: usize = 9;
/// `fence::WIRE_MAX_LEN`: a fence query's buffer holds any fence.
pub const FENCE_WIRE_MAX: usize = 62;

/// HEAD's record: `[size u64][mtime u64][ct_len u8][etag_len u8][etag]`.
pub const HEAD_LEN: usize = 50;
const ETAG_LEN: usize = 32;

/// LIST's page trailer `[FF][FF][cursor_len u8]`, and the most a cursor
/// (a key) adds to it.
const TRAILER_MARK: u8 = 0xFF;
const TRAILER_MAX: usize = 3 + KEY_MAX;

// ── Bounds ───────────────────────────────────────────────────────────

/// Longest key: a LIST entry and a cursor carry its length in a byte.
pub const KEY_MAX: usize = 255;
/// Read slots open at once (the host store's count).
pub const SLOTS: usize = 32;
/// Subscriptions at once (the host store's count).
pub const SUBSCRIPTIONS: usize = 256;
/// A value a read slot or an event holds itself: one chunk's worth.
pub const INLINE_MAX: usize = DATA_MAX;
/// Entries one store page asks for (LIST, CHANGES, pumps).
const PAGE_KEYS: u16 = 64;
/// A CHANGES answer's index entry while it is put in revision order.
const ORDER_ENTRY: usize = 16;
/// Re-drives of one paused store walk before the provider gives up on it
/// (each moves the walk on by the store's step budget).
const REDRIVE_MAX: u32 = 1 << 20;
/// A pump record's kind, marked: its value was not carried.
const KIND_ELIDED: u8 = 0x80;
/// The largest event: header, change head, key and an inline value.
pub const EVENT_MAX: usize = EVENT_HEAD + 15 + KEY_MAX + INLINE_MAX;
/// The request buffer's floor: the largest request built (a write with an
/// inline value) or event.
pub const REQ_MIN: usize = EVENT_MAX + 64;
/// The answer buffer's floor: a store page of one entry with an inline
/// value.
pub const ANS_MIN: usize = 4096;
/// The feed buffer's floor: one pump's changes, in order. A pump whose
/// changes outgrow the feed tells its subscribers to read again.
pub const FEED_MIN: usize = 16 * EVENT_MAX;
/// The most an origin entry's op body may be: the largest proposal the
/// replicated path carries (`wire::CONSENSUS_PROPOSAL_BODY_MAX`, 2040)
/// less the entry tag and the command head (the worker holds it to them).
/// A chunk entry fills it exactly.
pub const ENTRY_BODY_MAX: usize = kv_large::ENTRY_BODY_MAX;
/// Writes a replica holds undecided for its callers at once.
pub const PENDING_WRITES: usize = 32;
/// Steps a proposal goes unanswered before it is offered again (the
/// worker steps at the graph's tick: 2 s at 1 kHz).
pub const REOFFER_STEPS: u32 = 2000;
/// Steps a write goes unasked before it is let go (one in the log is let
/// go only once decided).
pub const IDLE_STEPS: u32 = 60_000;

// ── What the provider runs on ────────────────────────────────────────

/// The worker's side of the provider: its store, and the channels its
/// subscribers read.
pub trait Backend {
    /// Apply one KV operation now, as a command is applied: `(result,
    /// answer length)`.
    fn apply(&mut self, op: u8, body: &[u8], out: &mut [u8]) -> (u8, usize);
    /// The store's revision.
    fn revision(&self) -> u64;
    /// The history a change read may reach back, in revisions (0: all
    /// the store still holds). A read from before `revision - history`
    /// is LOST however much the store has yet to compact: the window is
    /// the store's promise, not an accident of when compaction runs.
    fn history(&self) -> u64 {
        0
    }
    /// Whether an answered write is on non-volatile media.
    fn durable(&self) -> bool;
    /// Whether the last write was refused for now (the store is making
    /// room): the caller asks again.
    fn busy(&self) -> bool;
    /// Write `bytes` to channel `chan`, whole or not at all: the count
    /// written, or negative.
    fn write(&mut self, chan: i32, bytes: &[u8]) -> i32;
    /// Whether writes go through the group's log (this store is one
    /// replica of it) rather than to this store.
    fn replicated(&self) -> bool {
        false
    }
    /// Offer one entry to the group's log, whole or not at all: `op` under
    /// origin `corr`, its body `parts` one after another.
    fn propose(&mut self, _op: u8, _corr: u64, _parts: &[&[u8]]) -> bool {
        false
    }
    /// The module whose call is being served, as the kernel names it (its
    /// owner record); all zero when there is none.
    fn caller(&self) -> [u8; 8] {
        [0; 8]
    }
    /// The group, as a decided write's fence names it.
    fn group(&self) -> Group {
        Group {
            epoch: 0,
            quorum: 1,
            durable: false,
        }
    }
}

/// The group a replicated store is one replica of.
#[derive(Clone, Copy)]
pub struct Group {
    /// The group's configuration generation.
    pub epoch: u32,
    /// Voters a commit needs.
    pub quorum: u8,
    /// A commit is on a quorum's non-volatile media (the log is a disk
    /// WAL a quorum fsyncs): a write's fence is `ReplicatedDurable`, else
    /// it is ordered only (`RevisionMonotone`).
    pub durable: bool,
}

/// The buffers a call works in (the worker's).
pub struct Bufs<'b> {
    /// A request as built, or an event ([`REQ_MIN`] at least).
    pub req: &'b mut [u8],
    /// An answer as the store gives it ([`ANS_MIN`] at least).
    pub ans: &'b mut [u8],
    /// An answer expanded (large values whole): the largest object and
    /// its page. Also the value a large read slot holds.
    pub cache: &'b mut [u8],
    /// One pump's changes ([`FEED_MIN`] at least).
    pub feed: &'b mut [u8],
}

/// An open read: the value of `key` at revision `rev`.
#[derive(Clone, Copy)]
struct Slot {
    used: bool,
    large: bool,
    key_len: u8,
    rev: u64,
    len: u32,
    key: [u8; KEY_MAX],
    inline: [u8; INLINE_MAX],
}

const NO_SLOT: Slot = Slot {
    used: false,
    large: false,
    key_len: 0,
    rev: 0,
    len: 0,
    key: [0; KEY_MAX],
    inline: [0; INLINE_MAX],
};

/// A subscriber: the prefix it watches, the channel it reads, the next
/// event's sequence, and whether it is owed a LOST sentinel.
#[derive(Clone, Copy)]
struct Sub {
    used: bool,
    lost: bool,
    /// The scope's head on every key it hears (a scoped prefix): stripped
    /// from each event's key.
    strip: u8,
    prefix_len: u8,
    sink: i32,
    seq: u32,
    prefix: [u8; KEY_MAX],
}

const NO_SUB: Sub = Sub {
    used: false,
    lost: false,
    strip: 0,
    prefix_len: 0,
    sink: -1,
    seq: 0,
    prefix: [0; KEY_MAX],
};

impl Sub {
    fn watches(&self, key: &[u8]) -> bool {
        self.used && key.starts_with(self.prefix.get(..self.prefix_len as usize).unwrap_or(&[]))
    }
}

// ── Scope: a node's private records ──────────────────────────────────

/// Where a scope's records are kept: `/~node/<scope><key>`.
pub const SCOPE_ROOT: &[u8] = b"/~node/";
/// A scope's name, at most.
pub const SCOPE_NAME_MAX: usize = 63;
/// Its prefixes, comma-separated, at most.
pub const SCOPED_MAX: usize = 512;
const SCOPE_HEAD_MAX: usize = SCOPE_ROOT.len() + SCOPE_NAME_MAX;

/// A replica's scope: the keys under its prefixes are this node's own.
/// Its graph names them plainly; the store keeps them under
/// [`SCOPE_ROOT`] and the scope's name, so every replica holds them, apart
/// from every other node's. A namespace operation on a prefix under a
/// scoped one is answered in the graph's names; one on a broader prefix
/// sees the store as it is.
struct Scope {
    head: [u8; SCOPE_HEAD_MAX],
    head_len: u8,
    prefixes: [u8; SCOPED_MAX],
    prefixes_len: u16,
}

const NO_SCOPE: Scope = Scope {
    head: [0; SCOPE_HEAD_MAX],
    head_len: 0,
    prefixes: [0; SCOPED_MAX],
    prefixes_len: 0,
};

impl Scope {
    fn head(&self) -> &[u8] {
        self.head.get(..self.head_len as usize).unwrap_or(&[])
    }

    /// Whether `key` (or a prefix) is under one of the scope's prefixes.
    fn holds(&self, key: &[u8]) -> bool {
        self.head_len != 0
            && self
                .prefixes
                .get(..self.prefixes_len as usize)
                .unwrap_or(&[])
                .split(|&b| b == b',')
                .any(|p| !p.is_empty() && key.starts_with(p))
    }

    /// `key` as the store keeps it, into `out`: its length, or `None` when
    /// the scoped name is past [`KEY_MAX`].
    fn inward(&self, key: &[u8], out: &mut [u8; KEY_MAX]) -> Option<usize> {
        let head = if self.holds(key) { self.head() } else { &[] };
        let n = head.len() + key.len();
        if n > KEY_MAX {
            return None;
        }
        copy(out, head);
        copy(out.get_mut(head.len()..)?, key);
        Some(n)
    }
}

/// The provider's state, in the worker's.
pub struct Provider {
    slots: [Slot; SLOTS],
    subs: [Sub; SUBSCRIPTIONS],
    /// Delivery, counted (telemetry): pumps whose history read came back
    /// incomplete (every subscriber then told LOST), events told, events a
    /// subscriber's channel refused.
    pub m_pump_partial: u32,
    pub m_told: u32,
    pub m_refused: u32,
    /// The revision subscribers have been told of.
    seen: u64,
    /// The store is serving: subscribers are told of changes from `seen`
    /// on. Before, a subscription is only registered (see
    /// [`needs_store`]).
    opened: bool,
    /// The large slot whose value `Bufs::cache` holds (index + 1; 0:
    /// none), and where in it the value lies.
    cached: u8,
    cache_at: u32,
    /// Writes through the group's log (a replica's).
    rep: Replication,
    /// This node's private records (a replica's).
    scope: Scope,
}

impl Provider {
    pub const fn new() -> Self {
        Provider {
            slots: [NO_SLOT; SLOTS],
            subs: [NO_SUB; SUBSCRIPTIONS],
            m_pump_partial: 0,
            m_told: 0,
            m_refused: 0,
            seen: 0,
            opened: false,
            cached: 0,
            cache_at: 0,
            rep: NO_REPLICATION,
            scope: NO_SCOPE,
        }
    }

    /// Empty a provider where it lies: it is integers and flags, and all
    /// zero is its empty state (a heap block need not be built on a
    /// stack first).
    ///
    /// # Safety
    /// `p` names `size_of::<Provider>()` writable bytes.
    pub unsafe fn clear(p: *mut Provider) {
        core::ptr::write_bytes(p, 0, 1);
    }

    /// Start at the store's revision: subscribers hear of what happens
    /// from here on.
    pub fn open<B: Backend>(&mut self, b: &B) {
        self.seen = b.revision();
        self.opened = true;
    }

    /// Subscriptions held.
    pub fn subscriptions(&self) -> usize {
        self.subs.iter().filter(|s| s.used).count()
    }

    /// Subscriptions owed a LOST sentinel.
    pub fn owed(&self) -> usize {
        self.subs.iter().filter(|s| s.used && s.lost).count()
    }

    /// Keep the keys under `prefixes` (comma-separated, each from `/`) as
    /// scope `name`'s own (see [`Scope`]). `false`, and no scope, for a
    /// name with a `/` or past [`SCOPE_NAME_MAX`], or a prefix list past
    /// [`SCOPED_MAX`] or naming a prefix not from `/`.
    #[allow(
        clippy::manual_contains,
        reason = "slice::contains links core's memchr, which PIC modules cannot resolve"
    )]
    pub fn scope(&mut self, name: &[u8], prefixes: &[u8]) -> bool {
        self.scope = NO_SCOPE;
        let ok = !name.is_empty()
            && name.len() <= SCOPE_NAME_MAX
            && !name.iter().any(|&b| b == b'/')
            && utf8_is_valid(name)
            && prefixes.len() <= SCOPED_MAX
            && prefixes
                .split(|&b| b == b',')
                .all(|p| p.first() == Some(&b'/') && key_ok(p));
        if !ok {
            return false;
        }
        let sc = &mut self.scope;
        copy(&mut sc.head, SCOPE_ROOT);
        copy(sc.head.get_mut(SCOPE_ROOT.len()..).unwrap_or(&mut []), name);
        sc.head_len = (SCOPE_ROOT.len() + name.len()) as u8;
        copy(&mut sc.prefixes, prefixes);
        sc.prefixes_len = prefixes.len() as u16;
        true
    }

    /// Write through the group's log as `replica`, registering with
    /// `nonce` (random: a restarted replica's must differ).
    pub fn replicate(&mut self, replica: u16, nonce: u64) {
        let r = &mut self.rep;
        r.on = true;
        r.replica = replica;
        r.nonce = nonce;
    }

    /// The replica's origin, once the log has registered it:
    /// `(replica, incarnation)`.
    pub fn origin(&self) -> Option<(u16, u16)> {
        (self.rep.on && self.rep.registered).then_some((self.rep.replica, self.rep.inc))
    }

    /// Writes held for callers: `(undecided, in the log)`.
    pub fn writes(&self) -> (usize, usize) {
        let w = &self.rep.writes;
        (
            w.iter().filter(|x| x.state != W_FREE).count(),
            self.rep.outstanding(),
        )
    }

    /// Another process registered this replica's id: two replicas share
    /// it, and this one's writes can no longer apply.
    pub fn superseded(&self) -> bool {
        self.rep.superseded
    }

    /// Where this replica's writes stand.
    pub fn report(&self) -> ReplicaReport {
        let r = &self.rep;
        let (held, in_log) = self.writes();
        ReplicaReport {
            registered: r.registered,
            inc: r.inc,
            hwm: r.hwm,
            next_seq: r.next_seq,
            held: held as u32,
            in_log: in_log as u32,
            uploaded: if r.upload_for != 0 { r.upload.fed() } else { 0 },
            step: r.step,
            reg_tries: r.reg_tries,
            reg_sent: r.reg_sent,
            reg_seen: r.reg_seen,
            reg_miss: r.reg_miss,
        }
    }
}

/// Where a replica's writes stand ([`Provider::report`]).
pub struct ReplicaReport {
    pub registered: bool,
    pub inc: u16,
    /// The highest seq of this incarnation applied here, and the next.
    pub hwm: u32,
    pub next_seq: u32,
    /// Writes held for callers, and of them those in the log.
    pub held: u32,
    pub in_log: u32,
    /// Bytes of the value being uploaded fed so far (0: none).
    pub uploaded: u64,
    /// Origin steps taken, and registration as counted in `Replication`.
    pub step: u32,
    pub reg_tries: u32,
    pub reg_sent: u32,
    pub reg_seen: u32,
    pub reg_miss: u32,
}

impl Default for Provider {
    fn default() -> Self {
        Self::new()
    }
}

// ── Byte helpers ─────────────────────────────────────────────────────

fn rd16(b: &[u8], at: usize) -> Option<usize> {
    Some(u16::from_le_bytes([*b.get(at)?, *b.get(at + 1)?]) as usize)
}

fn rd32(b: &[u8], at: usize) -> Option<usize> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?) as usize)
}

fn rd64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// `src` into `dst` at `*at`, advancing it; `None` past the end.
fn put(dst: &mut [u8], at: &mut usize, src: &[u8]) -> Option<()> {
    let d = dst.get_mut(*at..at.checked_add(src.len())?)?;
    for (x, y) in d.iter_mut().zip(src) {
        *x = *y;
    }
    *at += src.len();
    Some(())
}

/// `src` over the front of `dst` (as much as fits).
fn copy(dst: &mut [u8], src: &[u8]) {
    for (x, y) in dst.iter_mut().zip(src) {
        *x = *y;
    }
}

/// `len` bytes at a caller's address, as the contract passes it.
///
/// # Safety
/// The contract promises `ptr` names `len` readable bytes.
unsafe fn caller_bytes<'a>(ptr: u64, len: usize) -> &'a [u8] {
    if ptr == 0 || len == 0 {
        return &[];
    }
    core::slice::from_raw_parts(ptr as usize as *const u8, len)
}

/// # Safety
/// The contract promises `ptr` names `len` writable bytes.
unsafe fn caller_out<'a>(ptr: u64, len: usize) -> &'a mut [u8] {
    if ptr == 0 || len == 0 {
        return &mut [];
    }
    core::slice::from_raw_parts_mut(ptr as usize as *mut u8, len)
}

fn tagged(tag: i32, slot: usize) -> i32 {
    (tag << TAG_SHIFT) | slot as i32
}

fn slot_of(handle: i32) -> usize {
    if handle < 0 {
        return usize::MAX;
    }
    (handle & SLOT_MASK) as usize
}

/// A fence into the caller's `[ptr, cap)`: `kind` at `rev`; its length,
/// 0 when it does not fit (and is not written, as the host store does).
///
/// # Safety
/// As [`caller_out`].
unsafe fn write_fence(kind: u8, rev: u64, ptr: u64, cap: usize) -> usize {
    let len = if kind == FENCE_LOCAL_DURABLE {
        FENCE_DURABLE_LEN
    } else {
        FENCE_REVISION_LEN
    };
    if ptr == 0 || cap < len {
        return 0;
    }
    let out = caller_out(ptr, len);
    let mut at = 0usize;
    let _ = put(out, &mut at, &[kind]);
    if kind == FENCE_LOCAL_DURABLE {
        let _ = put(out, &mut at, &STORE_DEVICE_ID.to_le_bytes());
    } else {
        let _ = put(out, &mut at, &SOURCE);
        let _ = put(out, &mut at, &rev.to_le_bytes());
    }
    len
}

/// What a write achieved: on media, or ordered only.
fn write_fence_kind<B: Backend>(b: &B) -> u8 {
    if b.durable() {
        FENCE_LOCAL_DURABLE
    } else {
        FENCE_REVISION_MONOTONE
    }
}

/// The end of `prefix`'s span: `[prefix, end)` holds exactly the keys
/// starting with it. An empty prefix (or one of all `FF`) spans to the
/// end of the keyspace: `\0`, "every key from `start` on".
fn prefix_end(prefix: &[u8], out: &mut [u8; KEY_MAX + 1]) -> usize {
    copy(out, prefix);
    let mut n = prefix.len().min(KEY_MAX);
    while n > 0 {
        let last = out[n - 1];
        if last != 0xFF {
            out[n - 1] = last + 1;
            return n;
        }
        n -= 1;
    }
    out[0] = 0;
    1
}

/// A key or prefix off the wire: UTF-8 (as the host requires) and within
/// [`KEY_MAX`].
fn key_ok(k: &[u8]) -> bool {
    k.len() <= KEY_MAX && utf8_is_valid(k)
}

/// `[len u16][bytes]` at `a[*p..]`.
fn field16<'a>(a: &'a [u8], p: &mut usize) -> Option<&'a [u8]> {
    let n = rd16(a, *p)?;
    let f = a.get(*p + 2..*p + 2 + n)?;
    *p += 2 + n;
    Some(f)
}

// ── KV requests and answers ──────────────────────────────────────────

/// A `KV_OP_RANGE_META` body for `[start, end)` (`end` empty: `start`
/// alone) at `at_rev` (0: now), reading manifests.
fn range_meta(
    out: &mut [u8],
    start: &[u8],
    end: &[u8],
    at_rev: u64,
    limit: u32,
    keys_only: bool,
) -> Option<usize> {
    let mut p = 0usize;
    put(out, &mut p, &(start.len() as u16).to_le_bytes())?;
    put(out, &mut p, start)?;
    put(out, &mut p, &(end.len() as u16).to_le_bytes())?;
    put(out, &mut p, end)?;
    put(out, &mut p, &at_rev.to_le_bytes())?;
    put(out, &mut p, &limit.to_le_bytes())?;
    put(
        out,
        &mut p,
        &[RANGE_META_FLAG_MANIFESTS | u8::from(keys_only), 0, 0],
    )?;
    put(out, &mut p, &[0u8; 32])?;
    Some(p)
}

/// A `KV_OP_SCAN_VERSIONS` body: every change in `[start, end)` within
/// `(from, to]`, with metadata, reading manifests.
fn scan_versions(
    out: &mut [u8],
    start: &[u8],
    end: &[u8],
    from: u64,
    to: u64,
    cursor: u64,
) -> Option<usize> {
    let mut p = 0usize;
    put(out, &mut p, &(start.len() as u16).to_le_bytes())?;
    put(out, &mut p, start)?;
    put(out, &mut p, &(end.len() as u16).to_le_bytes())?;
    put(out, &mut p, end)?;
    put(out, &mut p, &from.to_le_bytes())?;
    put(out, &mut p, &to.to_le_bytes())?;
    put(out, &mut p, &cursor.to_le_bytes())?;
    put(out, &mut p, &PAGE_KEYS.to_le_bytes())?;
    put(out, &mut p, &[VERSIONS_FLAG_META | VERSIONS_FLAG_MANIFESTS])?;
    Some(p)
}

/// Apply `op` to its end: a walk the store paused on its step budget is
/// driven on (the same request continues it) until it answers.
fn run<B: Backend>(b: &mut B, op: u8, body: &[u8], out: &mut [u8]) -> (u8, usize) {
    for _ in 0..REDRIVE_MAX {
        let (r, n) = b.apply(op, body, out);
        if r != KV_RESULT_PAUSED {
            return (r, n);
        }
    }
    (KV_RESULT_PAUSED, 0)
}

/// A read, answered and expanded into `bufs.cache`: large values whole,
/// entries unmarked. `(result, answer length)`; an answer that is not a
/// page (a refusal) is copied as it is.
fn read<B: Backend>(
    b: &mut B,
    bufs: &mut Bufs<'_>,
    op: u8,
    len: usize,
) -> Result<(u8, usize), i32> {
    let req = bufs.req.get(..len).ok_or(EINVAL)?;
    let (r, n) = run(b, op, req, bufs.ans);
    let ans = bufs.ans.get(..n).ok_or(ERROR)?;
    if !Expander::wants(op, r) {
        copy(bufs.cache, ans);
        return Ok((r, n.min(bufs.cache.len())));
    }
    let mut ex = Expander::new();
    ex.start(op, req, r, ans, bufs.cache).map_err(|_| ERROR)?;
    let mut chunk = [0u8; CHUNK_MAX + 8];
    loop {
        match ex.step().map_err(|_| ERROR)? {
            Need::Chunk(h) => {
                let (cr, cn) =
                    b.apply(kv_large::FETCH_OP, &kv_large::chunk_request(&h), &mut chunk);
                if cr != KV_RESULT_OK {
                    return Err(ERROR);
                }
                ex.take(chunk.get(..cn).ok_or(ERROR)?, bufs.cache)
                    .map_err(|_| ERROR)?;
            }
            Need::Done(result, n) => return Ok((result, n)),
        }
    }
}

/// An entry of an expanded `KV_RESULT_KVS` page, `[kl][key][vl u32]
/// [value][create][mod][version][lease]`: `(key, value, value offset,
/// mod)` and where the next begins.
fn kvs_at(b: &[u8], at: usize) -> Option<(&[u8], &[u8], usize, u64, usize)> {
    let mut p = at;
    let key = field16(b, &mut p)?;
    let vl = rd32(b, p)?;
    let val = b.get(p + 4..p + 4 + vl)?;
    let m = p + 4 + vl;
    Some((key, val, p + 4, rd64(b, m + 8)?, m + 32))
}

/// A change of an expanded `KV_RESULT_VERSIONS` page, `[rev][ts][kind]
/// [kl][key][vl u32][value]` (`[create][version][lease]` after a put):
/// `(rev, contract kind, key, value)` and where the next begins.
fn version_at(b: &[u8], at: usize) -> Option<(u64, u8, &[u8], &[u8], usize)> {
    let rev = rd64(b, at)?;
    let del = *b.get(at + 16)? == VERSION_KIND_DELETE;
    let mut p = at + 17;
    let key = field16(b, &mut p)?;
    let vl = rd32(b, p)?;
    let val = b.get(p + 4..p + 4 + vl)?;
    let m = p + 4 + vl;
    if del {
        return Some((rev, KIND_DELETED, key, &[], m));
    }
    // Created by this very write: it did not exist before.
    let kind = if rd64(b, m)? == rev {
        KIND_ADDED
    } else {
        KIND_MODIFIED
    };
    Some((rev, kind, key, val, m + 24))
}

/// A KVS page's head `[total u64][more u8][count u16]`: whether more
/// follows, and how many entries it holds.
fn kvs_head(b: &[u8]) -> Option<(bool, usize)> {
    Some((*b.get(8)? != 0, rd16(b, 9)?))
}

/// The live `key`: `(revision, length)` from its unexpanded entry,
/// without reading a large value; `None` when absent.
fn lookup<B: Backend>(
    b: &mut B,
    bufs: &mut Bufs<'_>,
    key: &[u8],
) -> Result<Option<(u64, u32)>, i32> {
    let n = range_meta(bufs.req, key, &[], 0, 1, false).ok_or(EINVAL)?;
    let (r, an) = run(
        b,
        KV_OP_RANGE_META,
        bufs.req.get(..n).ok_or(EINVAL)?,
        bufs.ans,
    );
    if r != KV_RESULT_KVS {
        return Err(ERROR);
    }
    let a = bufs.ans.get(..an).ok_or(ERROR)?;
    if kvs_head(a).ok_or(ERROR)?.1 == 0 {
        return Ok(None);
    }
    // A marked entry ends `[flags u8]` after its metadata.
    let (_, val, _, modr, end) = kvs_at(a, 11).ok_or(ERROR)?;
    if a.get(end).ok_or(ERROR)? & RECORD_FLAG_MANIFEST != 0 {
        let m = Manifest::parse(val).ok_or(ERROR)?;
        return Ok(Some((modr, u32::try_from(m.total).map_err(|_| EOVERFLOW)?)));
    }
    Ok(Some((modr, val.len() as u32)))
}

/// Upload `value`'s chunks straight from the caller's buffer: its
/// manifest.
fn upload<B: Backend>(b: &mut B, bufs: &mut Bufs<'_>, value: &[u8]) -> Result<Manifest, i32> {
    let mut builder = Builder::new(value.len() as u64).ok_or(EOVERFLOW)?;
    let mut chunk = [0u8; CHUNK_MAX];
    let mut fed = 0usize;
    loop {
        while let Some((n, h)) = builder.next(&mut chunk) {
            let mut p = 0usize;
            put(bufs.req, &mut p, &h).ok_or(ERROR)?;
            put(bufs.req, &mut p, &(n as u16).to_le_bytes()).ok_or(ERROR)?;
            put(bufs.req, &mut p, chunk.get(..n).ok_or(ERROR)?).ok_or(ERROR)?;
            let (r, _) = b.apply(
                kv_large::UPLOAD_OP,
                bufs.req.get(..p).ok_or(ERROR)?,
                bufs.ans,
            );
            // A store making room refuses the chunk for now, as it refuses
            // a commit: not yet, so asked again (`EAGAIN` would say an etag
            // was lost). The chunks already sent are stored, so the same
            // value uploaded again resends them as no-ops.
            if r != KV_RESULT_OK {
                return Err(if b.busy() { EINPROGRESS } else { ERROR });
            }
        }
        if fed == value.len() {
            return builder.manifest().ok_or(ERROR);
        }
        let take = builder.room().min(value.len() - fed);
        builder
            .feed(value.get(fed..fed + take).ok_or(ERROR)?)
            .ok_or(ERROR)?;
        fed += take;
    }
}

/// What a write puts: a value inline, or a large value's manifest.
enum Put<'a> {
    Value(&'a [u8]),
    Manifest([u8; MANIFEST_LEN]),
}

/// A `KV_OP_TXN` body: `key` compared at `cmp` (its revision; 0: absent)
/// when `guarded`, then put or (`None`) deleted.
fn txn(
    out: &mut [u8],
    key: &[u8],
    guarded: bool,
    cmp: u64,
    value: Option<&Put<'_>>,
) -> Option<usize> {
    let mut p = 0usize;
    put(out, &mut p, &u16::from(guarded).to_le_bytes())?;
    if guarded {
        put(out, &mut p, &[TXN_CMP_MOD_EQUAL])?;
        put(out, &mut p, &(key.len() as u16).to_le_bytes())?;
        put(out, &mut p, key)?;
        put(out, &mut p, &cmp.to_le_bytes())?;
    }
    put(out, &mut p, &1u16.to_le_bytes())?;
    match value {
        Some(v) => {
            let (op, bytes): (u8, &[u8]) = match v {
                Put::Value(v) => (KV_OP_PUT, v),
                Put::Manifest(m) => (KV_OP_PUT_MANIFEST, m),
            };
            let body = u16::try_from(2 + key.len() + 4 + bytes.len() + 1 + 8).ok()?;
            put(out, &mut p, &[op])?;
            put(out, &mut p, &body.to_le_bytes())?;
            put(out, &mut p, &(key.len() as u16).to_le_bytes())?;
            put(out, &mut p, key)?;
            put(out, &mut p, &(bytes.len() as u32).to_le_bytes())?;
            put(out, &mut p, bytes)?;
            put(out, &mut p, &[0])?;
            put(out, &mut p, &0u64.to_le_bytes())?;
        }
        None => {
            put(out, &mut p, &[KV_OP_DELETE])?;
            put(out, &mut p, &((4 + key.len()) as u16).to_le_bytes())?;
            put(out, &mut p, &1u16.to_le_bytes())?;
            put(out, &mut p, &(key.len() as u16).to_le_bytes())?;
            put(out, &mut p, key)?;
        }
    }
    put(out, &mut p, &0u16.to_le_bytes())?;
    Some(p)
}

/// Run a write: `Ok(true)` when its comparison held and it applied,
/// `Ok(false)` when the key had moved.
fn commit<B: Backend>(b: &mut B, bufs: &mut Bufs<'_>, len: usize, put: bool) -> Result<bool, i32> {
    let (r, n) = b.apply(KV_OP_TXN, bufs.req.get(..len).ok_or(EINVAL)?, bufs.ans);
    if r != KV_RESULT_TXN {
        // Not applied: a store making room takes it when asked again.
        return Err(if b.busy() { EINPROGRESS } else { ERROR });
    }
    // `[succeeded u8][op_count u16]`, then each op's `[result][len u16]
    // [answer]`: a write is one op, and it must have done what it said.
    let a = bufs.ans.get(..n).unwrap_or(&[]);
    if a.first() != Some(&1) {
        return Ok(false);
    }
    match a.get(3).copied() {
        Some(r) if op_done(put, r) => Ok(true),
        _ => Err(ERROR),
    }
}

/// A write's op did what it said: a put put (`OK`), a delete deleted
/// (the count it deleted, none included).
fn op_done(put: bool, result: u8) -> bool {
    result == if put { KV_RESULT_OK } else { KV_RESULT_INTEGER }
}

// ── storage.object ───────────────────────────────────────────────────

/// A precondition `[kind][etag_len][etag]` at `a[*p..]`: its kind, and
/// the revision it names (0: absent).
fn precondition(a: &[u8], p: &mut usize) -> Option<(u8, u64)> {
    let kind = *a.get(*p)?;
    let el = *a.get(*p + 1)? as usize;
    let e = a.get(*p + 2..*p + 2 + el)?;
    *p += 2 + el;
    match kind {
        PRE_ANY | PRE_ABSENT => Some((kind, 0)),
        PRE_ETAG if el >= 8 => Some((kind, rd64(e, 0)?)),
        _ => None,
    }
}

/// `[key_len u16][key]` at `a[*p..]`: a key the store can hold.
fn key_at<'a>(a: &'a [u8], p: &mut usize) -> Option<&'a [u8]> {
    let k = field16(a, p)?;
    (!k.is_empty() && key_ok(k)).then_some(k)
}

/// A write's arguments, as the contract passes them.
struct WriteArgs<'a> {
    key: &'a [u8],
    /// The value a PUT puts (`None`: a DELETE).
    value: Option<&'a [u8]>,
    pre: u8,
    /// The revision an ETAG names.
    rev: u64,
    fence_ptr: u64,
    fence_cap: usize,
    /// A record of a host log's import: how far the import has come once
    /// it is in (`origin::IMPORTED_KEY`'s value).
    mark: Option<[u8; IMPORT_MARK_LEN]>,
}

/// `[key_len u16][key][ct_len u8][ct][body_ptr u64][body_len u64]
/// [precondition][fence_ptr u64][fence_cap u16]`.
///
/// # Safety
/// The contract names `body_len` readable bytes at `body_ptr`.
unsafe fn put_args(a: &[u8]) -> Result<WriteArgs<'_>, i32> {
    let mut p = 0usize;
    let key = key_at(a, &mut p).ok_or(EINVAL)?;
    // The content type is not kept (nor does the host store keep it).
    let ctl = *a.get(p).ok_or(EINVAL)?;
    p += 1 + ctl as usize;
    let (Some(body_ptr), Some(body_len)) = (rd64(a, p), rd64(a, p + 8)) else {
        return Err(EINVAL);
    };
    p += 16;
    let (pre, rev) = precondition(a, &mut p).ok_or(EINVAL)?;
    let body_len = usize::try_from(body_len).map_err(|_| EOVERFLOW)?;
    Ok(WriteArgs {
        key,
        value: Some(caller_bytes(body_ptr, body_len)),
        pre,
        rev,
        fence_ptr: rd64(a, p).unwrap_or(0),
        fence_cap: rd16(a, p + 8).unwrap_or(0),
        mark: None,
    })
}

/// `[key_len u16][key][precondition][fence_ptr u64][fence_cap u16]`.
fn delete_args(a: &[u8]) -> Result<WriteArgs<'_>, i32> {
    let mut p = 0usize;
    let key = key_at(a, &mut p).ok_or(EINVAL)?;
    let (pre, rev) = precondition(a, &mut p).ok_or(EINVAL)?;
    Ok(WriteArgs {
        key,
        value: None,
        pre,
        rev,
        fence_ptr: rd64(a, p).unwrap_or(0),
        fence_cap: rd16(a, p + 8).unwrap_or(0),
        mark: None,
    })
}

fn obj_put<B: Backend>(pv: &mut Provider, b: &mut B, bufs: &mut Bufs<'_>, a: &[u8]) -> i32 {
    // SAFETY: the contract names the value's bytes at its pointer.
    let w = match unsafe { put_args(a) } {
        Ok(w) => w,
        Err(e) => return e,
    };
    if b.replicated() {
        return replicated_write(pv, b, &w);
    }
    match put_value(pv, b, bufs, w.key, w.value.unwrap_or(&[]), w.pre, w.rev) {
        Err(e) => e,
        Ok(()) => {
            // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
            unsafe { write_fence(write_fence_kind(b), b.revision(), w.fence_ptr, w.fence_cap) };
            0
        }
    }
}

/// Put `value` at `key` under a precondition (`pre`, and the revision an
/// ETAG names): `Ok` once applied and told, or the contract's errno —
/// `EEXIST` when an ABSENT create lost, `EAGAIN` when the key moved.
pub fn put_value<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    key: &[u8],
    value: &[u8],
    pre: u8,
    rev: u64,
) -> Result<(), i32> {
    if key.is_empty() || !key_ok(key) {
        return Err(EINVAL);
    }
    let what = if value.len() > INLINE_MAX {
        Put::Manifest(upload(b, bufs, value)?.encode())
    } else {
        Put::Value(value)
    };
    let n = txn(bufs.req, key, pre != PRE_ANY, rev, Some(&what)).ok_or(EINVAL)?;
    match commit(b, bufs, n, true)? {
        // Somebody else created it: a create has lost.
        false if pre == PRE_ABSENT => Err(EEXIST),
        // It moved under a compare-and-swap: read again and retry.
        false => Err(EAGAIN),
        true => {
            pump(pv, b, bufs);
            Ok(())
        }
    }
}

/// Delete `key` under a precondition, as [`put_value`] puts; an absent key
/// under ANY or ABSENT is done.
pub fn delete_key<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    key: &[u8],
    pre: u8,
    rev: u64,
) -> Result<(), i32> {
    if key.is_empty() || !key_ok(key) {
        return Err(EINVAL);
    }
    let n = txn(bufs.req, key, pre != PRE_ANY, rev, None).ok_or(EINVAL)?;
    if !commit(b, bufs, n, false)? {
        return Err(EAGAIN);
    }
    pump(pv, b, bufs);
    Ok(())
}

/// One record of a host store's log imported into a replicated store: put
/// `value` at `key` (`None`: delete it) as this replica's write, carrying
/// how far the import has come once it is in — `offset` into the log and
/// the CRC state `crc` of the bytes before it — which every replica
/// records under `origin::imported_key` as it applies the record. `EINPROGRESS`
/// until it is decided here (asked again with the same record, it is the
/// same write), then 0 or what refused it.
pub fn import_record<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    key: &[u8],
    value: Option<&[u8]>,
    offset: u64,
    crc: u32,
) -> i32 {
    if key.is_empty() || !key_ok(key) {
        return EINVAL;
    }
    // In the store's names, as a graph's write would be.
    let mut name = [0u8; KEY_MAX];
    let Some(n) = pv.scope.inward(key, &mut name) else {
        return EINVAL;
    };
    let key = name.get(..n).unwrap_or(&[]);
    let mut mark = [0u8; IMPORT_MARK_LEN];
    mark[..8].copy_from_slice(&offset.to_le_bytes());
    mark[8..].copy_from_slice(&crc.to_le_bytes());
    replicated_write(
        pv,
        b,
        &WriteArgs {
            key,
            value,
            pre: PRE_ANY,
            rev: 0,
            fence_ptr: 0,
            fence_cap: 0,
            mark: Some(mark),
        },
    )
}

/// A DELETE: an absent key is a no-op under ANY and ABSENT alike.
fn obj_delete<B: Backend>(pv: &mut Provider, b: &mut B, bufs: &mut Bufs<'_>, a: &[u8]) -> i32 {
    let w = match delete_args(a) {
        Ok(w) => w,
        Err(e) => return e,
    };
    if b.replicated() {
        return replicated_write(pv, b, &w);
    }
    match delete_key(pv, b, bufs, w.key, w.pre, w.rev) {
        Err(e) => e,
        Ok(()) => {
            // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
            unsafe { write_fence(write_fence_kind(b), b.revision(), w.fence_ptr, w.fence_cap) };
            0
        }
    }
}

/// Read `key` now, expanded: `(mod revision, value offset in the cache,
/// value length)`, `None` when absent.
fn read_value<B: Backend>(
    b: &mut B,
    bufs: &mut Bufs<'_>,
    key: &[u8],
) -> Result<Option<(u64, usize, usize)>, i32> {
    let n = range_meta(bufs.req, key, &[], 0, 1, false).ok_or(EINVAL)?;
    let (r, an) = read(b, bufs, KV_OP_RANGE_META, n)?;
    match r {
        KV_RESULT_KVS => {}
        KV_RESULT_TOO_LARGE => return Err(ENOMEM),
        _ => return Err(ERROR),
    }
    let page = bufs.cache.get(..an).ok_or(ERROR)?;
    if kvs_head(page).ok_or(ERROR)?.1 == 0 {
        return Ok(None);
    }
    let (_, val, at, modr, _) = kvs_at(page, 11).ok_or(ERROR)?;
    Ok(Some((modr, at, val.len())))
}

/// `arg` = the key: a read slot on its value now.
fn obj_get<B: Backend>(pv: &mut Provider, b: &mut B, bufs: &mut Bufs<'_>, a: &[u8]) -> i32 {
    if a.is_empty() || !key_ok(a) {
        return EINVAL;
    }
    let Some(i) = pv.slots.iter().position(|s| !s.used) else {
        return ENOMEM;
    };
    let (modr, at, len) = match read_value(b, bufs, a) {
        Err(e) => return e,
        Ok(None) => return ENXIO,
        Ok(Some(v)) => v,
    };
    let Ok(len32) = u32::try_from(len) else {
        return EOVERFLOW;
    };
    let large = len > INLINE_MAX;
    let s = &mut pv.slots[i];
    s.used = true;
    s.large = large;
    s.rev = modr;
    s.len = len32;
    s.key_len = a.len() as u8;
    copy(&mut s.key, a);
    if large {
        pv.cached = (i + 1) as u8;
        pv.cache_at = at as u32;
    } else {
        copy(&mut s.inline, bufs.cache.get(at..at + len).unwrap_or(&[]));
        // The cache no longer holds the large slot it held.
        pv.cached = 0;
    }
    tagged(TAG_OBJECT, i)
}

/// Bring large slot `i`'s value back into the cache: the key read now,
/// which must still be at the slot's revision (a memory store keeps no
/// point reads in the past). A slot whose value has since been replaced
/// cannot be served: `ERROR`, and the caller opens the key again.
fn recache<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    i: usize,
) -> Result<(), i32> {
    let s = pv.slots[i];
    let key = s.key.get(..s.key_len as usize).ok_or(ERROR)?;
    let Some((modr, at, len)) = read_value(b, bufs, key)? else {
        pv.cached = 0;
        return Err(ERROR);
    };
    if modr != s.rev || len != s.len as usize {
        pv.cached = 0;
        return Err(ERROR);
    }
    pv.cached = (i + 1) as u8;
    pv.cache_at = at as u32;
    Ok(())
}

/// `[off u64][len u32][out_ptr u64]`: bytes of the slot's value.
fn obj_range_get<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    slot: usize,
    a: &[u8],
) -> i32 {
    if slot >= SLOTS || !pv.slots[slot].used {
        return ENOSYS;
    }
    let (Some(off), Some(len), Some(out_ptr)) = (rd64(a, 0), rd32(a, 8), rd64(a, 12)) else {
        return EINVAL;
    };
    let s = pv.slots[slot];
    let total = u64::from(s.len);
    if off >= total {
        return 0;
    }
    let n = (len as u64).min(total - off) as usize;
    let from = off as usize;
    if s.large && pv.cached != (slot + 1) as u8 {
        if let Err(e) = recache(pv, b, bufs, slot) {
            return e;
        }
    }
    let src: &[u8] = if s.large {
        let at = pv.cache_at as usize + from;
        bufs.cache.get(at..at + n).unwrap_or(&[])
    } else {
        pv.slots[slot].inline.get(from..from + n).unwrap_or(&[])
    };
    if src.len() != n {
        return ERROR;
    }
    // SAFETY: the contract names `len` writable bytes at `out_ptr`.
    copy(unsafe { caller_out(out_ptr, n) }, src);
    n as i32
}

/// `[key_len u16][key][out_ptr u64][out_cap u32][fence_ptr u64]
/// [fence_cap u16]`: the 50-byte record.
fn obj_head<B: Backend>(b: &mut B, bufs: &mut Bufs<'_>, a: &[u8]) -> i32 {
    let mut p = 0usize;
    let Some(key) = key_at(a, &mut p) else {
        return EINVAL;
    };
    let out_ptr = rd64(a, p).unwrap_or(0);
    let out_cap = rd32(a, p + 8).unwrap_or(0);
    let fence_ptr = rd64(a, p + 12).unwrap_or(0);
    let fence_cap = rd16(a, p + 20).unwrap_or(0);
    let (rev, len) = match lookup(b, bufs, key) {
        Err(e) => return e,
        Ok(None) => return ENXIO,
        Ok(Some(v)) => v,
    };
    if out_ptr != 0 && out_cap >= HEAD_LEN {
        let mut rec = [0u8; HEAD_LEN];
        let mut at = 0usize;
        let _ = put(&mut rec, &mut at, &u64::from(len).to_le_bytes());
        let _ = put(&mut rec, &mut at, &0u64.to_le_bytes());
        let _ = put(&mut rec, &mut at, &[0, ETAG_LEN as u8]);
        let _ = put(&mut rec, &mut at, &rev.to_le_bytes());
        // SAFETY: the contract names `out_cap` writable bytes.
        copy(unsafe { caller_out(out_ptr, HEAD_LEN) }, &rec);
    }
    // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
    unsafe { write_fence(FENCE_VIEW_CONSISTENT, b.revision(), fence_ptr, fence_cap) };
    HEAD_LEN as i32
}

// ── storage.namespace ────────────────────────────────────────────────

/// `[prefix_len u16][prefix][cursor_len u16][cursor][out_ptr u64]
/// [out_cap u32][fence_ptr u64][fence_cap u16]`: a page of `[len u8]
/// [kind 0][key]` entries, then `[FF][FF][cursor_len u8][cursor]` — the
/// key the next page starts at, none at the end.
fn ns_list<B: Backend>(b: &mut B, bufs: &mut Bufs<'_>, a: &[u8]) -> i32 {
    let mut p = 0usize;
    let Some(prefix) = field16(a, &mut p).filter(|k| key_ok(k)) else {
        return EINVAL;
    };
    let Some(cursor) = field16(a, &mut p) else {
        return EINVAL;
    };
    // A cursor is a key under the prefix; anything else is not ours.
    if !cursor.is_empty() && (!key_ok(cursor) || !cursor.starts_with(prefix)) {
        return EINVAL;
    }
    // The whole fixed tail, so a mis-stated cursor length is caught.
    if a.len() < p + 22 {
        return EINVAL;
    }
    let (Some(out_ptr), Some(out_cap), Some(fence_ptr), Some(fence_cap)) =
        (rd64(a, p), rd32(a, p + 8), rd64(a, p + 12), rd16(a, p + 20))
    else {
        return EINVAL;
    };
    // SAFETY: the contract names `out_cap` writable bytes at `out_ptr`.
    let out = unsafe { caller_out(out_ptr, out_cap) };
    let mut end = [0u8; KEY_MAX + 1];
    let en = prefix_end(prefix, &mut end);
    let first = if cursor.is_empty() { prefix } else { cursor };
    let mut next = [0u8; KEY_MAX];
    let mut next_len = 0usize;
    let mut at = 0usize;
    let read_rev = b.revision();
    let walked = current(
        b,
        bufs,
        first,
        &end[..en],
        read_rev,
        true,
        &mut |_, _, _, key, _| {
            if key.len() > KEY_MAX {
                next_len = usize::MAX;
                return false;
            }
            // Room for this entry and, after it, any page's trailer.
            if at + 2 + key.len() + TRAILER_MAX > out_cap {
                next_len = key.len();
                copy(&mut next, key);
                return false;
            }
            let _ = put(out, &mut at, &[key.len() as u8, 0]);
            let _ = put(out, &mut at, key);
            true
        },
    );
    match walked {
        Ok(_) if next_len == usize::MAX => return EOVERFLOW,
        Ok(_) => {}
        Err(e) => return e,
    }
    if at + 3 + next_len > out_cap {
        // Too small for one entry and a trailer: paging cannot help.
        return ENOMEM;
    }
    let _ = put(out, &mut at, &[TRAILER_MARK, TRAILER_MARK, next_len as u8]);
    let _ = put(out, &mut at, &next[..next_len]);
    // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
    unsafe { write_fence(FENCE_VIEW_CONSISTENT, read_rev, fence_ptr, fence_cap) };
    at as i32
}

/// One change, `[rev][kind][klen u16][vlen u32][key][value]`.
fn change_into(
    out: &mut [u8],
    at: &mut usize,
    rev: u64,
    kind: u8,
    key: &[u8],
    val: &[u8],
) -> Option<()> {
    put(out, at, &rev.to_le_bytes())?;
    put(out, at, &[kind])?;
    put(out, at, &(key.len() as u16).to_le_bytes())?;
    put(out, at, &(val.len() as u32).to_le_bytes())?;
    put(out, at, key)?;
    put(out, at, val)
}

/// `[prefix_len u16][prefix][since u64][out_ptr u64][out_cap u32]
/// [fence_ptr u64][fence_cap u16]`: `[status][count u32]`, then every
/// change after `since` (since 0: every entry now, as Added). Status 1
/// (LOST, no changes) when the history no longer reaches `since`. The
/// whole answer fits `out_cap` or it is `ENOMEM`.
fn ns_changes<B: Backend>(b: &mut B, bufs: &mut Bufs<'_>, a: &[u8]) -> i32 {
    let mut p = 0usize;
    let Some(prefix) = field16(a, &mut p).filter(|k| key_ok(k)) else {
        return EINVAL;
    };
    let (Some(since), Some(out_ptr), Some(out_cap), Some(fence_ptr), Some(fence_cap)) = (
        rd64(a, p),
        rd64(a, p + 8),
        rd32(a, p + 16),
        rd64(a, p + 20),
        rd16(a, p + 28),
    ) else {
        return EINVAL;
    };
    // SAFETY: the contract names `out_cap` writable bytes at `out_ptr`.
    let out = unsafe { caller_out(out_ptr, out_cap) };
    let mut end = [0u8; KEY_MAX + 1];
    let en = prefix_end(prefix, &mut end);
    let now = b.revision();
    let (lost, count, at) = if since == 0 {
        // The present: every entry, in key order, as Added.
        let mut at = 5usize;
        let mut count = 0u32;
        let walked = current(
            b,
            bufs,
            prefix,
            &end[..en],
            now,
            false,
            &mut |_, _, rev, key, val| {
                count += 1;
                change_into(out, &mut at, rev, KIND_ADDED, key, val).is_some()
            },
        );
        match walked {
            Ok(Walk::Done) => (false, count, at),
            Ok(Walk::Lost) => (true, 0, 5),
            Ok(Walk::Stopped) => return ENOMEM,
            Err(e) => return e,
        }
    } else if b.history() > 0 && since < now.saturating_sub(b.history()) {
        (true, 0, 5)
    } else {
        match changes_in_order(b, bufs, prefix, &end[..en], since, now, out) {
            Ok(Some((count, at))) => (false, count, at),
            Ok(None) => (true, 0, 5),
            Err(e) => return e,
        }
    };
    let mut h = 0usize;
    if put(out, &mut h, &[u8::from(lost)]).is_none()
        || put(out, &mut h, &count.to_le_bytes()).is_none()
    {
        return ENOMEM;
    }
    // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
    unsafe { write_fence(FENCE_VIEW_CONSISTENT, now, fence_ptr, fence_cap) };
    at as i32
}

/// Every change in `[start, end)` within `(since, now]` into `out` from
/// byte 5, oldest first (a revision's changes in the store's walk order):
/// `Some((count, end))`, `None` when the history no longer reaches `since`.
///
/// The store walks a window key by key, so the answer is laid out, not
/// sorted in place: a first walk notes each change's revision and size in
/// an index at the far end of `out` (16 bytes a change), the index is put
/// in revision order and turned into each change's place, and a second
/// walk writes every change straight to its place. Nothing but `out`
/// holds it, so an answer is as large as the caller's buffer allows.
fn changes_in_order<B: Backend>(
    b: &mut B,
    bufs: &mut Bufs<'_>,
    start: &[u8],
    end: &[u8],
    since: u64,
    now: u64,
    out: &mut [u8],
) -> Result<Option<(u32, usize)>, i32> {
    // Pass 1: `(rev, ordinal, length)` per change, from the end of `out`.
    let cap = out.len();
    let mut count = 0usize;
    let mut total = 0usize;
    let mut room = true;
    let walked = history(
        b,
        bufs,
        start,
        end,
        since,
        now,
        &mut |_, _, rev, _, key, val| {
            let len = 15 + key.len() + val.len();
            total += len;
            count += 1;
            let need = 5 + total + count * ORDER_ENTRY;
            if need > cap {
                room = false;
                return false;
            }
            let entry = (u128::from(rev) << 64) | ((count as u128 - 1) << 32) | len as u128;
            let slot = cap - count * ORDER_ENTRY;
            if let Some(d) = out.get_mut(slot..slot + ORDER_ENTRY) {
                copy(d, &entry.to_le_bytes());
            }
            true
        },
    );
    match walked {
        Ok(Walk::Done) => {}
        Ok(Walk::Lost) => return Ok(None),
        Ok(Walk::Stopped) if !room => return Err(ENOMEM),
        Ok(Walk::Stopped) => return Err(ERROR),
        Err(e) => return Err(e),
    }
    let n = count;
    let Some(index) = out.get_mut(cap - n * ORDER_ENTRY..) else {
        return Err(ERROR);
    };
    // The index was written from the end backwards: ordinal 0 is last.
    // Revision order (walk order within a revision)…
    heap_sort(index, n);
    // …gives each change its place; re-keyed by ordinal, sorted back.
    let mut place = 5u64;
    for i in 0..n {
        let e = entry(index, i);
        let ordinal = (e >> 32) as u32;
        let len = e as u32;
        set_entry(index, i, (u128::from(ordinal) << 64) | u128::from(place));
        place += u64::from(len);
    }
    heap_sort(index, n);
    // Pass 2: every change to its place.
    let mut i = 0usize;
    let mut ok = true;
    let walked = history(
        b,
        bufs,
        start,
        end,
        since,
        now,
        &mut |_, _, rev, kind, key, val| {
            let e = entry(index_of(out, cap, n), i);
            i += 1;
            let mut at = e as u64 as usize;
            if i > n || change_into(out, &mut at, rev, kind, key, val).is_none() {
                ok = false;
                return false;
            }
            true
        },
    );
    match walked {
        Ok(Walk::Done) if ok && i == n => Ok(Some((n as u32, 5 + total))),
        // The window is fixed (`now` pinned), so the second walk sees what
        // the first did; anything else is the store failing.
        Ok(Walk::Lost) => Ok(None),
        Err(e) => Err(e),
        _ => Err(ERROR),
    }
}

/// The index `changes_in_order` keeps at the end of `out`.
fn index_of(out: &[u8], cap: usize, n: usize) -> &[u8] {
    out.get(cap - n * ORDER_ENTRY..).unwrap_or(&[])
}

/// Put `count` change records (`[rev][kind][klen u16][vlen u32][key]
/// [value]`, all of `records`) in revision order, oldest first, keeping
/// the walk's order within one revision. `scratch` holds an index and a
/// copy of the records; `false` when it cannot.
fn by_revision(records: &mut [u8], count: usize, scratch: &mut [u8]) -> bool {
    let body = records.len();
    let fits = count
        .checked_mul(ORDER_ENTRY)
        .and_then(|n| n.checked_add(body))
        .is_some_and(|n| n <= scratch.len());
    if !fits {
        return false;
    }
    let (index, sorted) = scratch.split_at_mut(count * ORDER_ENTRY);
    // Each record's `(rev, offset, length)`, packed so that ordering the
    // numbers orders the records.
    let mut p = 0usize;
    for i in 0..count {
        let (Some(rev), Some(kl), Some(vl)) = (
            rd64(records, p),
            rd16(records, p + 9),
            rd32(records, p + 11),
        ) else {
            return false;
        };
        let len = 15 + kl + vl;
        let entry = (u128::from(rev) << 64) | ((p as u128) << 32) | len as u128;
        set_entry(index, i, entry);
        p += len;
    }
    if p != body {
        return false;
    }
    heap_sort(index, count);
    let mut q = 0usize;
    for i in 0..count {
        let e = entry(index, i);
        let (off, len) = ((e >> 32) as u32 as usize, e as u32 as usize);
        if put(sorted, &mut q, records.get(off..off + len).unwrap_or(&[])).is_none() {
            return false;
        }
    }
    copy(records, sorted.get(..q).unwrap_or(&[]));
    true
}

fn entry(index: &[u8], i: usize) -> u128 {
    let at = i * ORDER_ENTRY;
    index
        .get(at..at + ORDER_ENTRY)
        .and_then(|b| b.try_into().ok())
        .map_or(0, u128::from_le_bytes)
}

fn set_entry(index: &mut [u8], i: usize, v: u128) {
    let at = i * ORDER_ENTRY;
    if let Some(d) = index.get_mut(at..at + ORDER_ENTRY) {
        copy(d, &v.to_le_bytes());
    }
}

/// Heap sort, ascending, of `n` index entries: no recursion, no heap.
fn heap_sort(index: &mut [u8], n: usize) {
    let sift = |index: &mut [u8], mut root: usize, end: usize| loop {
        let mut child = 2 * root + 1;
        if child >= end {
            return;
        }
        if child + 1 < end && entry(index, child) < entry(index, child + 1) {
            child += 1;
        }
        let (r, c) = (entry(index, root), entry(index, child));
        if r >= c {
            return;
        }
        set_entry(index, root, c);
        set_entry(index, child, r);
        root = child;
    };
    for start in (0..n / 2).rev() {
        sift(index, start, n);
    }
    for end in (1..n).rev() {
        let (a, z) = (entry(index, 0), entry(index, end));
        set_entry(index, 0, z);
        set_entry(index, end, a);
        sift(index, 0, end);
    }
}

/// How a walk ended.
enum Walk {
    Done,
    /// The history no longer reaches the start.
    Lost,
    /// The visitor stopped it.
    Stopped,
}

/// Every entry in `[start, end)` at revision `at_rev`, in key order:
/// `f(backend, scratch, mod revision, key, value)`, which stops the walk
/// by answering `false`. The scratch is the request buffer, free while a
/// page is visited. (Visitors are generic, never `dyn`: a vtable is a
/// table of absolute pointers, which a PIC module cannot hold.)
fn current<B: Backend, F: FnMut(&mut B, &mut [u8], u64, &[u8], &[u8]) -> bool>(
    b: &mut B,
    bufs: &mut Bufs<'_>,
    start: &[u8],
    end: &[u8],
    at_rev: u64,
    keys_only: bool,
    f: &mut F,
) -> Result<Walk, i32> {
    let mut from = [0u8; KEY_MAX + 1];
    let mut from_len = start.len();
    copy(&mut from, start);
    loop {
        let n = range_meta(
            bufs.req,
            &from[..from_len],
            end,
            at_rev,
            u32::from(PAGE_KEYS),
            keys_only,
        )
        .ok_or(EINVAL)?;
        let (r, an) = read(b, bufs, KV_OP_RANGE_META, n)?;
        match r {
            KV_RESULT_KVS => {}
            KV_RESULT_TOO_LARGE => return Err(ENOMEM),
            _ => return Err(ERROR),
        }
        let Bufs { req, cache, .. } = &mut *bufs;
        let page = cache.get(..an).ok_or(ERROR)?;
        let (more, count) = kvs_head(page).ok_or(ERROR)?;
        let mut e = 11usize;
        for _ in 0..count {
            let (key, val, _, modr, nx) = kvs_at(page, e).ok_or(ERROR)?;
            e = nx;
            if !f(b, req, modr, key, val) {
                return Ok(Walk::Stopped);
            }
            if key.len() > KEY_MAX {
                return Err(EOVERFLOW);
            }
            // The next page starts just past this key.
            copy(&mut from, key);
            from[key.len()] = 0;
            from_len = key.len() + 1;
        }
        if !more || count == 0 {
            return Ok(Walk::Done);
        }
    }
}

/// Every change in `[start, end)` within `(from, to]`, as the store walks
/// them (key by key): `f(backend, scratch, rev, kind, key, value)`.
#[allow(clippy::too_many_arguments, reason = "one walk's span and window")]
fn history<B: Backend, F: FnMut(&mut B, &mut [u8], u64, u8, &[u8], &[u8]) -> bool>(
    b: &mut B,
    bufs: &mut Bufs<'_>,
    start: &[u8],
    end: &[u8],
    from: u64,
    to: u64,
    f: &mut F,
) -> Result<Walk, i32> {
    let mut cursor = 0u64;
    loop {
        let n = scan_versions(bufs.req, start, end, from, to, cursor).ok_or(EINVAL)?;
        let (r, an) = read(b, bufs, KV_OP_SCAN_VERSIONS, n)?;
        match r {
            KV_RESULT_VERSIONS => {}
            KV_RESULT_COMPACTED => return Ok(Walk::Lost),
            KV_RESULT_TOO_LARGE => return Err(ENOMEM),
            _ => return Err(ERROR),
        }
        let Bufs { req, cache, .. } = &mut *bufs;
        let page = cache.get(..an).ok_or(ERROR)?;
        // `[next_cursor u64][count u16]`, then the changes.
        let next = rd64(page, 0).ok_or(ERROR)?;
        let count = rd16(page, 8).ok_or(ERROR)?;
        let mut e = 10usize;
        for _ in 0..count {
            let (rev, kind, key, val, nx) = version_at(page, e).ok_or(ERROR)?;
            e = nx;
            if !f(b, req, rev, kind, key, val) {
                return Ok(Walk::Stopped);
            }
        }
        if next == 0 {
            return Ok(Walk::Done);
        }
        cursor = next;
    }
}

// ── Subscriptions ────────────────────────────────────────────────────

/// One event to `s`, built in `ev`: `true` when its channel took it
/// whole.
fn event_to<B: Backend>(
    s: &mut Sub,
    b: &mut B,
    ev: &mut [u8],
    rev: u64,
    kind: u8,
    key: &[u8],
    val: &[u8],
) -> bool {
    if val.len() > INLINE_MAX {
        return event_with(s, b, ev, rev, kind, key, &[], EVENT_FLAG_VALUE_ELIDED);
    }
    event_with(s, b, ev, rev, kind, key, val, 0)
}

/// One event to `s` with header `flags`.
#[allow(clippy::too_many_arguments, reason = "an event's fields")]
fn event_with<B: Backend>(
    s: &mut Sub,
    b: &mut B,
    ev: &mut [u8],
    rev: u64,
    kind: u8,
    key: &[u8],
    val: &[u8],
    flags: u8,
) -> bool {
    // A scoped subscription hears its keys in the graph's names.
    let key = key.get(s.strip as usize..).unwrap_or(key);
    let payload = (15 + key.len() + val.len()) as u16;
    let mut at = 0usize;
    let framed = put(ev, &mut at, &SOURCE).is_some()
        && put(ev, &mut at, &s.seq.to_le_bytes()).is_some()
        && put(ev, &mut at, &0u64.to_le_bytes()).is_some()
        && put(ev, &mut at, &[CT_NAMESPACE_CHANGE, flags]).is_some()
        && put(ev, &mut at, &payload.to_le_bytes()).is_some()
        && change_into(ev, &mut at, rev, kind, key, val).is_some();
    if !framed || b.write(s.sink, &ev[..at]) != at as i32 {
        return false;
    }
    s.seq = s.seq.wrapping_add(1);
    true
}

/// Bring subscribers up to the store: every change since the last pump,
/// oldest first, to each subscriber whose prefix holds its key (one whose
/// channel is full is owed LOST instead, and hears nothing more until
/// told); then the LOST sentinel (kind Deleted, empty key, at the store's
/// revision) to each owed one, as its channel takes it. The provider's
/// writes pump themselves; the worker pumps after every command it
/// applies, and on its step.
pub fn pump<B: Backend>(pv: &mut Provider, b: &mut B, bufs: &mut Bufs<'_>) {
    if !pv.opened {
        return;
    }
    let now = b.revision();
    if now > pv.seen && pv.subs.iter().any(|s| s.used && !s.lost) {
        // The pages go through the cache.
        pv.cached = 0;
        let whole = feed(pv, b, bufs, now);
        if !whole {
            pv.m_pump_partial = pv.m_pump_partial.wrapping_add(1);
            // What happened is not (or no longer) wholly readable here:
            // every subscriber reads again.
            for s in pv.subs.iter_mut().filter(|s| s.used) {
                s.lost = true;
            }
        }
    }
    pv.seen = now;
    for s in pv.subs.iter_mut().filter(|s| s.used && s.lost) {
        if event_to(s, b, bufs.req, now, KIND_DELETED, &[], &[]) {
            s.lost = false;
        }
    }
}

/// One pump's changes, `(seen, now]`: collected into the feed (only those
/// a subscriber watches, values past an event's elided), put in revision
/// order, and told. `false` when they could not all be read.
fn feed<B: Backend>(pv: &mut Provider, b: &mut B, bufs: &mut Bufs<'_>, now: u64) -> bool {
    let Bufs {
        req,
        ans,
        cache,
        feed,
    } = bufs;
    let mut walk = Bufs {
        req,
        ans,
        cache,
        feed: &mut [],
    };
    let subs = &pv.subs;
    let mut fed = 0usize;
    let mut count = 0usize;
    let walked = history(
        b,
        &mut walk,
        &[],
        &[],
        pv.seen,
        now,
        &mut |_, _, rev, kind, key, val| {
            if !subs.iter().any(|s| !s.lost && s.watches(key)) {
                return true;
            }
            let (kind, val) = if val.len() > INLINE_MAX {
                (kind | KIND_ELIDED, &[][..])
            } else {
                (kind, val)
            };
            count += 1;
            change_into(feed, &mut fed, rev, kind, key, val).is_some()
        },
    );
    if !matches!(walked, Ok(Walk::Done)) {
        return false;
    }
    let records = feed.get_mut(..fed).unwrap_or(&mut []);
    if !by_revision(records, count, walk.cache) {
        return false;
    }
    let mut p = 0usize;
    for _ in 0..count {
        let Some((rev, kind, key, val, next)) = record_at(records, p) else {
            return false;
        };
        p = next;
        for s in pv.subs.iter_mut().filter(|s| !s.lost && s.watches(key)) {
            let told = if kind & KIND_ELIDED != 0 {
                elided_to(s, b, walk.req, rev, kind & !KIND_ELIDED, key)
            } else {
                event_to(s, b, walk.req, rev, kind, key, val)
            };
            if told {
                pv.m_told = pv.m_told.wrapping_add(1);
            } else {
                pv.m_refused = pv.m_refused.wrapping_add(1);
                s.lost = true;
            }
        }
    }
    true
}

/// A change record `[rev][kind][klen u16][vlen u32][key][value]` at `at`,
/// and where the next begins.
fn record_at(b: &[u8], at: usize) -> Option<(u64, u8, &[u8], &[u8], usize)> {
    let rev = rd64(b, at)?;
    let kind = *b.get(at + 8)?;
    let kl = rd16(b, at + 9)?;
    let vl = rd32(b, at + 11)?;
    let key = b.get(at + 15..at + 15 + kl)?;
    let val = b.get(at + 15 + kl..at + 15 + kl + vl)?;
    Some((rev, kind, key, val, at + 15 + kl + vl))
}

/// A change whose value an event does not carry, announced.
fn elided_to<B: Backend>(
    s: &mut Sub,
    b: &mut B,
    ev: &mut [u8],
    rev: u64,
    kind: u8,
    key: &[u8],
) -> bool {
    event_with(s, b, ev, rev, kind, key, &[], EVENT_FLAG_VALUE_ELIDED)
}

/// `[prefix_len u16][prefix][sink u32][flags u8]` (bit 0: every entry
/// now first, as Added): a subscription.
fn ns_subscribe<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    a: &[u8],
    strip: usize,
) -> i32 {
    let mut p = 0usize;
    let Some(prefix) = field16(a, &mut p).filter(|k| key_ok(k)) else {
        return EINVAL;
    };
    let Some(sink) = rd32(a, p) else {
        return EINVAL;
    };
    let initial = a.get(p + 4).is_some_and(|f| f & 1 != 0);
    if initial && !pv.opened {
        return EAGAIN;
    }
    // Everyone else first: the new subscriber hears only what follows.
    pump(pv, b, bufs);
    let Some(i) = pv.subs.iter().position(|s| !s.used) else {
        return ENOMEM;
    };
    let mut s = NO_SUB;
    s.used = true;
    s.strip = strip as u8;
    s.sink = sink as i32;
    s.prefix_len = prefix.len() as u8;
    copy(&mut s.prefix, prefix);
    if initial {
        let mut end = [0u8; KEY_MAX + 1];
        let en = prefix_end(prefix, &mut end);
        let now = b.revision();
        let walked = current(
            b,
            bufs,
            prefix,
            &end[..en],
            now,
            false,
            &mut |b, ev, rev, key, val| event_to(&mut s, b, ev, rev, KIND_ADDED, key, val),
        );
        match walked {
            Ok(Walk::Done) => {}
            // A watcher set up on part of the state would believe it had
            // seen all of it: refused, as the host store refuses.
            Ok(_) => return EAGAIN,
            Err(e) => return e,
        }
    }
    pv.subs[i] = s;
    tagged(TAG_NAMESPACE, SLOTS + i)
}

// ── Writes through the group's log ───────────────────────────────────
//
// A replica's write is a `Write`: taken when first asked, given its
// `KV_OP_TXN` body and a seq, offered to the log (its large value's
// chunks first), applied here when it comes back (`origin_applied`),
// covered by the next position marker (`origin_position`), and collected
// by the ask after. Writes are offered in seq order, at most `WINDOW` of
// them in the log at once; one found lost — a later one applied as a gap,
// or nothing applied for `REOFFER_STEPS` — is offered again with every
// write after it, and the origin record makes a second offer of an
// applied one nothing.
//
// A large value's chunks are offered `CHUNK_WINDOW` at a time, each held
// until it has applied here and offered again (in upload order) when none
// has for `REOFFER_STEPS`; its manifest is offered once every chunk has
// applied. The windows keep a replica within its share of what a leader
// takes in at once: a leader that has too much uncommitted drops what it
// is relayed, and the windows make such a loss cost one offer again.

/// Writes in the log, undecided, at once (no more than the origin record
/// keeps outcomes for).
pub const WINDOW: usize = 8;
const _: () = assert!(WINDOW <= RING);
/// A large value's chunks in flight at once.
pub const CHUNK_WINDOW: usize = 4;

const W_FREE: u8 = 0;
/// Taken: no place in the log yet.
const W_QUEUED: u8 = 1;
/// Its value's chunks are being offered (the upload builder is its).
const W_UPLOAD: u8 = 2;
/// Offered (or to be offered) under its seq.
const W_PROPOSED: u8 = 3;
/// Applied here; answered once a position marker covers it.
const W_APPLIED: u8 = 4;
/// Applied and covered, or refused here: the next ask collects it.
const W_DECIDED: u8 = 5;

#[derive(Clone, Copy)]
struct Write {
    state: u8,
    /// A PUT (else a DELETE), and its precondition's kind.
    put: bool,
    pre: u8,
    /// A record of a host log's import: its body starts with the import's
    /// marker (`KV_OP_ORIGIN_IMPORT`).
    import: bool,
    /// Offered since it was last found lost.
    offered: bool,
    /// The caller and the request it is.
    id: Hash,
    seq: u32,
    /// Steps since its caller last asked.
    idle: u32,
    /// Refused here, never proposed: the answer (0: none).
    refused: i32,
    outcome: Outcome,
    /// The log position that covers it.
    index: u64,
    term: u64,
    len: u16,
    /// Its `KV_OP_TXN` body, after an import's marker.
    body: [u8; ENTRY_BODY_MAX],
}

const NO_WRITE: Write = Write {
    state: W_FREE,
    put: false,
    pre: 0,
    import: false,
    offered: false,
    id: [0; HASH_LEN],
    seq: 0,
    idle: 0,
    refused: 0,
    outcome: NO_OUTCOME,
    index: 0,
    term: 0,
    len: 0,
    body: [0; ENTRY_BODY_MAX],
};

/// A chunk of the value being uploaded, offered and not yet applied here.
#[derive(Clone, Copy)]
struct Chunk {
    used: bool,
    offered: bool,
    /// Its place in the upload (children before parents).
    ord: u32,
    len: u16,
    /// Its `KV_OP_PUT_CHUNK` body: `[hash 32][len u16][chunk]`.
    body: [u8; ENTRY_BODY_MAX],
}

const NO_CHUNK: Chunk = Chunk {
    used: false,
    offered: false,
    ord: 0,
    len: 0,
    body: [0; ENTRY_BODY_MAX],
};

/// A replica's side of its writes: its origin, its writes, and the one
/// large value being uploaded.
struct Replication {
    on: bool,
    replica: u16,
    nonce: u64,
    /// The incarnation the log gave this replica (once `registered`).
    inc: u16,
    registered: bool,
    /// Another process registered this replica's id.
    superseded: bool,
    /// A registration has been offered, last at step `register_at`.
    register_offered: bool,
    register_at: u32,
    /// Registration, counted (telemetry): offers tried, offers the log
    /// took, our REGISTERs seen applied, and of them nonce mismatches.
    reg_tries: u32,
    reg_sent: u32,
    reg_seen: u32,
    reg_miss: u32,
    /// The seq the next write takes (`u32::MAX`: none left in this
    /// incarnation).
    next_seq: u32,
    /// The highest seq of this incarnation applied here.
    hwm: u32,
    step: u32,
    /// The step the log last took one of this replica's writes (or one
    /// was first offered).
    progress_at: u32,
    /// Writes were offered again since the log last took one.
    reoffered: bool,
    /// The write the builder uploads for (index + 1; 0: none).
    upload_for: u8,
    /// Its chunks in flight, the next one's place, and the step one last
    /// applied here (or was first offered).
    chunks: [Chunk; CHUNK_WINDOW],
    chunk_ord: u32,
    chunk_progress_at: u32,
    upload: Builder,
    writes: [Write; PENDING_WRITES],
}

const NO_REPLICATION: Replication = Replication {
    on: false,
    replica: 0,
    nonce: 0,
    inc: 0,
    registered: false,
    superseded: false,
    register_offered: false,
    register_at: 0,
    reg_tries: 0,
    reg_sent: 0,
    reg_seen: 0,
    reg_miss: 0,
    next_seq: 0,
    hwm: 0,
    step: 0,
    progress_at: 0,
    reoffered: false,
    upload_for: 0,
    chunks: [NO_CHUNK; CHUNK_WINDOW],
    chunk_ord: 0,
    chunk_progress_at: 0,
    upload: Builder::IDLE,
    writes: [NO_WRITE; PENDING_WRITES],
};

impl Replication {
    /// Writes in the log, undecided.
    fn outstanding(&self) -> usize {
        self.writes
            .iter()
            .filter(|x| matches!(x.state, W_PROPOSED | W_APPLIED))
            .count()
    }

    /// The upload ends (done, refused or let go): the builder is free.
    fn end_upload(&mut self) {
        self.upload_for = 0;
        for c in self.chunks.iter_mut() {
            c.used = false;
        }
    }

    /// Refuse write `i` here, with `rc`.
    fn refuse(&mut self, i: usize, rc: i32) {
        if self.upload_for as usize == i + 1 {
            self.end_upload();
        }
        let x = &mut self.writes[i];
        x.state = W_DECIDED;
        x.refused = rc;
    }

    /// Write `i`, its body `n` bytes, takes the next seq.
    fn sequence(&mut self, i: usize, n: usize) {
        let seq = self.next_seq;
        self.next_seq = seq.saturating_add(1);
        let x = &mut self.writes[i];
        x.seq = seq;
        x.len = n as u16;
        x.state = W_PROPOSED;
        x.offered = false;
    }

    /// May a write take a seq now?
    fn may_sequence(&self) -> bool {
        self.registered && self.next_seq != u32::MAX && self.outstanding() < WINDOW
    }

    /// Every write the log has not taken is offered again, from the
    /// first.
    fn reoffer(&mut self) {
        let hwm = self.hwm;
        for x in self
            .writes
            .iter_mut()
            .filter(|x| x.state == W_PROPOSED && x.seq > hwm)
        {
            x.offered = false;
        }
        self.reoffered = true;
        self.progress_at = self.step;
    }
}

/// A write asked of a replica: taken, moved on as far as it goes now, and
/// answered — `EINPROGRESS` until it is decided.
fn replicated_write<B: Backend>(pv: &mut Provider, b: &mut B, w: &WriteArgs<'_>) -> i32 {
    let rep = &mut pv.rep;
    if rep.superseded {
        return ERROR;
    }
    let id = write_id(&b.caller(), w);
    let found = rep
        .writes
        .iter()
        .position(|x| x.state != W_FREE && x.id == id);
    let i = match found {
        Some(i) => i,
        None => {
            let Some(i) = rep.writes.iter().position(|x| x.state == W_FREE) else {
                // No room to take it yet: asked again, it is taken then.
                return EINPROGRESS;
            };
            let x = &mut rep.writes[i];
            x.state = W_QUEUED;
            x.put = w.value.is_some();
            x.pre = w.pre;
            x.import = w.mark.is_some();
            x.offered = false;
            x.id = id;
            x.seq = 0;
            x.refused = 0;
            x.outcome = NO_OUTCOME;
            x.index = 0;
            x.term = 0;
            x.len = 0;
            i
        }
    };
    rep.writes[i].idle = 0;
    advance(rep, b, i, w);
    offer(rep, b);
    let x = &rep.writes[i];
    if x.state != W_DECIDED {
        return EINPROGRESS;
    }
    let rc = if x.refused != 0 {
        x.refused
    } else {
        match (x.outcome.succeeded, x.outcome.result) {
            (true, r) if op_done(x.put, r) => 0,
            (true, _) => ERROR,
            // Somebody else created it: a create has lost.
            (false, KV_RESULT_TXN) if x.put && x.pre == PRE_ABSENT => EEXIST,
            // It moved under a compare-and-swap: read again and retry.
            (false, KV_RESULT_TXN) => EAGAIN,
            (false, _) => ERROR,
        }
    };
    if rc == 0 {
        // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
        unsafe { write_replicated_fence(b, x, w.fence_ptr, w.fence_cap) };
    }
    rep.writes[i].state = W_FREE;
    rc
}

/// Which write a request is: its caller and what it asks — the operation,
/// key, precondition and value — not where the caller keeps those bytes. A
/// caller asking again rebuilds the request; wherever its buffers sit then,
/// it is the same write.
fn write_id(caller: &[u8], w: &WriteArgs<'_>) -> Hash {
    let head = [u8::from(w.value.is_some()), w.pre];
    let mark = w.mark.unwrap_or([0; IMPORT_MARK_LEN]);
    sha256_of(&[
        caller,
        &head,
        &[u8::from(w.mark.is_some())],
        &mark,
        &w.rev.to_le_bytes(),
        &(w.key.len() as u32).to_le_bytes(),
        w.key,
        w.value.unwrap_or(&[]),
    ])
}

/// Lay write `x`'s entry body: an import's marker, then the
/// `KV_OP_TXN` body for `w` putting `what` (`None`: deleting). Its length,
/// or `None` when it does not fit one entry.
fn write_body(x: &mut Write, w: &WriteArgs<'_>, what: Option<&Put<'_>>) -> Option<usize> {
    let at = match w.mark {
        Some(m) => {
            x.body.get_mut(..IMPORT_MARK_LEN)?.copy_from_slice(&m);
            IMPORT_MARK_LEN
        }
        None => 0,
    };
    let n = txn(x.body.get_mut(at..)?, w.key, w.pre != PRE_ANY, w.rev, what)?;
    Some(at + n)
}

/// Move write `i` on: a body and a seq once the log has room for it, its
/// value's chunks first when the value is not carried inline.
fn advance<B: Backend>(rep: &mut Replication, b: &mut B, i: usize, w: &WriteArgs<'_>) {
    if rep.writes[i].state == W_QUEUED {
        if !rep.may_sequence() {
            return;
        }
        let inline = match w.value {
            None => write_body(&mut rep.writes[i], w, None),
            Some(v) if v.len() <= INLINE_MAX => {
                write_body(&mut rep.writes[i], w, Some(&Put::Value(v)))
            }
            Some(_) => None,
        };
        match (inline, w.value) {
            (Some(n), _) => {
                rep.sequence(i, n);
                return;
            }
            // A delete is a key and a comparison: it always fits.
            (None, None) => {
                rep.refuse(i, EINVAL);
                return;
            }
            (None, Some(v)) => {
                // One value uploads at a time.
                if rep.upload_for != 0 {
                    return;
                }
                if !rep.upload.start(v.len() as u64) {
                    rep.refuse(i, EOVERFLOW);
                    return;
                }
                rep.end_upload();
                rep.upload_for = (i + 1) as u8;
                rep.chunk_progress_at = rep.step;
                rep.writes[i].state = W_UPLOAD;
            }
        }
    }
    if rep.writes[i].state == W_UPLOAD && rep.upload_for as usize == i + 1 {
        upload_chunks(rep, b, i, w);
    }
}

/// Move write `i`'s upload on: chunks cut from its value into the free
/// places of the window, every chunk not yet offered offered (as far as
/// the log takes them); once every chunk has applied here, its manifest's
/// body and a seq.
fn upload_chunks<B: Backend>(rep: &mut Replication, b: &mut B, i: usize, w: &WriteArgs<'_>) {
    let Some(value) = w.value else {
        return;
    };
    // The value a write was taken with is the one its caller asks with
    // again; a length that changed is a request that did.
    if rep.upload.total() != value.len() as u64 {
        rep.refuse(i, EINVAL);
        return;
    }
    loop {
        if !offer_chunks(rep, b) {
            return;
        }
        let Some(k) = rep.chunks.iter().position(|c| !c.used) else {
            // The window is full: chunks apply before more are cut.
            return;
        };
        let c = &mut rep.chunks[k];
        let Some(out) = c
            .body
            .get_mut(HASH_LEN + 2..)
            .and_then(|o| <&mut [u8; CHUNK_MAX]>::try_from(o).ok())
        else {
            rep.refuse(i, ERROR);
            return;
        };
        if let Some((n, h)) = rep.upload.next(out) {
            c.body[..HASH_LEN].copy_from_slice(&h);
            c.body[HASH_LEN..HASH_LEN + 2].copy_from_slice(&(n as u16).to_le_bytes());
            c.len = (HASH_LEN + 2 + n) as u16;
            c.used = true;
            c.offered = false;
            c.ord = rep.chunk_ord;
            rep.chunk_ord = rep.chunk_ord.wrapping_add(1);
            continue;
        }
        let fed = rep.upload.fed() as usize;
        if fed < value.len() {
            let take = rep.upload.room().min(value.len() - fed);
            let fits = value
                .get(fed..fed + take)
                .and_then(|bytes| rep.upload.feed(bytes));
            if fits.is_none() {
                rep.refuse(i, ERROR);
                return;
            }
            continue;
        }
        // Every chunk is cut: the manifest goes in the write once they
        // have all applied.
        if rep.chunks.iter().any(|c| c.used) {
            return;
        }
        let Some(m) = rep.upload.manifest() else {
            rep.refuse(i, ERROR);
            return;
        };
        if !rep.may_sequence() {
            return;
        }
        let Some(n) = write_body(&mut rep.writes[i], w, Some(&Put::Manifest(m.encode()))) else {
            rep.refuse(i, EINVAL);
            return;
        };
        rep.end_upload();
        rep.sequence(i, n);
        return;
    }
}

/// Offer every chunk in flight not yet offered, in upload order: `false`
/// when the log took not all of them.
fn offer_chunks<B: Backend>(rep: &mut Replication, b: &mut B) -> bool {
    let corr = origin::corr(rep.replica, rep.inc, 0);
    loop {
        let next = (0..CHUNK_WINDOW)
            .filter(|&k| rep.chunks[k].used && !rep.chunks[k].offered)
            .min_by_key(|&k| rep.chunks[k].ord);
        let Some(k) = next else {
            return true;
        };
        let c = &rep.chunks[k];
        let body = c.body.get(..c.len as usize).unwrap_or(&[]);
        if !b.propose(KV_OP_ORIGIN_CHUNK, corr, &[body]) {
            return false;
        }
        rep.chunks[k].offered = true;
    }
}

/// Offer every write not yet offered, in seq order, as far as the log
/// takes them.
fn offer<B: Backend>(rep: &mut Replication, b: &mut B) {
    loop {
        let next = (0..PENDING_WRITES)
            .filter(|&i| rep.writes[i].state == W_PROPOSED && !rep.writes[i].offered)
            .min_by_key(|&i| rep.writes[i].seq);
        let Some(i) = next else {
            return;
        };
        let waiting = rep
            .writes
            .iter()
            .any(|x| x.state == W_PROPOSED && x.offered);
        let x = &rep.writes[i];
        let corr = origin::corr(rep.replica, rep.inc, x.seq);
        let body = x.body.get(..x.len as usize).unwrap_or(&[]);
        let op = if x.import {
            KV_OP_ORIGIN_IMPORT
        } else {
            KV_OP_ORIGIN_WRITE
        };
        if !b.propose(op, corr, &[body]) {
            return;
        }
        rep.writes[i].offered = true;
        if !waiting {
            rep.progress_at = rep.step;
        }
    }
}

/// A decided write's fence: `ReplicatedDurable` at the position that
/// covers it, or (a group whose commits are not on media) its revision.
///
/// # Safety
/// As [`caller_out`].
unsafe fn write_replicated_fence<B: Backend>(b: &B, x: &Write, ptr: u64, cap: usize) {
    let g = b.group();
    if !g.durable {
        write_fence(FENCE_REVISION_MONOTONE, x.outcome.revision, ptr, cap);
        return;
    }
    if ptr == 0 || cap < FENCE_REPLICATED_LEN {
        return;
    }
    let index = x.index.to_le_bytes();
    // The commitment: which entry of which log, by its term.
    let witness = sha256_of(&[&SOURCE, &index, &x.term.to_le_bytes()]);
    let out = caller_out(ptr, FENCE_REPLICATED_LEN);
    let mut at = 0usize;
    let _ = put(out, &mut at, &[FENCE_REPLICATED_DURABLE]);
    let _ = put(out, &mut at, &SOURCE);
    let _ = put(out, &mut at, &index);
    let _ = put(out, &mut at, &g.epoch.to_le_bytes());
    let _ = put(out, &mut at, &[g.quorum]);
    let _ = put(out, &mut at, &witness);
}

/// What an origin entry came to where it applied here (`origin.rs`), told
/// to this replica's writes.
pub fn origin_applied(pv: &mut Provider, a: &origin::Applied) {
    let rep = &mut pv.rep;
    if !rep.on {
        return;
    }
    match *a {
        origin::Applied::Registered {
            replica,
            inc,
            nonce,
        } if replica == rep.replica => {
            rep.reg_seen = rep.reg_seen.wrapping_add(1);
            if nonce != rep.nonce {
                rep.reg_miss = rep.reg_miss.wrapping_add(1);
            }
            if nonce == rep.nonce {
                registered(rep, inc);
            } else if rep.registered {
                rep.superseded = true;
            }
        }
        origin::Applied::Wrote {
            replica,
            inc,
            seq,
            outcome,
            ..
        } if rep.registered && replica == rep.replica && inc == rep.inc => {
            rep.hwm = rep.hwm.max(seq);
            rep.progress_at = rep.step;
            rep.reoffered = false;
            took(rep, seq, outcome);
        }
        origin::Applied::Chunk {
            replica,
            hash,
            result,
        } if replica == rep.replica && result == KV_RESULT_OK => {
            // One of this replica's chunks is stored: its place is free.
            // (One refused — a node whose child is lost — waits to be
            // offered again after the child.)
            for c in rep.chunks.iter_mut() {
                if c.used && c.body.get(..HASH_LEN) == Some(&hash[..]) {
                    c.used = false;
                    rep.chunk_progress_at = rep.step;
                }
            }
        }
        origin::Applied::Skipped {
            replica,
            inc,
            verdict: origin::Verdict::Gap,
            ..
        } if rep.registered && replica == rep.replica && inc == rep.inc => {
            // An earlier write was lost: offer again from it, once until
            // the log takes one (every write behind the lost one applies
            // as a gap too).
            if !rep.reoffered {
                rep.reoffer();
            }
        }
        _ => {}
    }
}

/// The log registered this replica as incarnation `inc`.
fn registered(rep: &mut Replication, inc: u16) {
    if rep.registered && rep.inc == inc {
        return;
    }
    rep.inc = inc;
    rep.registered = true;
    rep.next_seq = 1;
    rep.hwm = 0;
}

/// Write `seq` applied here with `outcome`. A manifest whose chunks the
/// log lost applied as a refusal: the write uploads again, under a new
/// seq.
fn took(rep: &mut Replication, seq: u32, outcome: Outcome) {
    let Some(x) = rep
        .writes
        .iter_mut()
        .find(|x| x.state == W_PROPOSED && x.seq == seq)
    else {
        return;
    };
    if outcome.result == KV_RESULT_MISSING_CHUNK {
        x.state = W_QUEUED;
        x.seq = 0;
        return;
    }
    x.outcome = outcome;
    x.state = W_APPLIED;
}

/// The apply stream reached log position `(term, index)`: every write
/// applied before it is decided there.
pub fn origin_position(pv: &mut Provider, term: u64, index: u64) {
    for x in pv.rep.writes.iter_mut().filter(|x| x.state == W_APPLIED) {
        x.state = W_DECIDED;
        x.index = index;
        x.term = term;
    }
}

/// The store was replaced by a snapshot at `(term, index)`, past entries
/// this replica never applied: what became of its writes is in its
/// origin record, `rec`.
pub fn origin_resync(pv: &mut Provider, rec: Option<&Record>, term: u64, index: u64) {
    let rep = &mut pv.rep;
    if !rep.on {
        return;
    }
    let Some(r) = rec.filter(|r| r.nonce == rep.nonce) else {
        if rep.registered {
            rep.superseded = true;
        }
        return;
    };
    registered(rep, r.inc);
    rep.hwm = rep.hwm.max(r.hwm);
    for k in 0..PENDING_WRITES {
        let x = rep.writes[k];
        if x.state != W_PROPOSED || x.seq > r.hwm {
            continue;
        }
        match r.outcome(x.seq) {
            Some(o) => took(rep, x.seq, o),
            // Past what the record keeps: no more than `RING` writes are
            // in the log at once, so this is not one of them.
            None => rep.refuse(k, ERROR),
        }
    }
    origin_position(pv, term, index);
}

/// Once a step, on a replica: register, offer again what the log has
/// not taken in `REOFFER_STEPS`, offer what waits, and let go of writes
/// nobody asks for.
pub fn origin_step<B: Backend>(pv: &mut Provider, b: &mut B) {
    let rep = &mut pv.rep;
    if !rep.on || rep.superseded {
        return;
    }
    rep.step = rep.step.wrapping_add(1);
    for k in 0..PENDING_WRITES {
        let x = &mut rep.writes[k];
        if x.state == W_FREE {
            continue;
        }
        x.idle = x.idle.saturating_add(1);
        if x.idle > IDLE_STEPS && matches!(x.state, W_QUEUED | W_UPLOAD | W_DECIDED) {
            if rep.upload_for as usize == k + 1 {
                rep.end_upload();
            }
            rep.writes[k].state = W_FREE;
        }
    }
    // An incarnation's seqs are spent: once its last write is decided,
    // register a new one.
    if rep.registered && rep.next_seq == u32::MAX && rep.outstanding() == 0 {
        rep.registered = false;
        rep.register_offered = false;
        rep.nonce = rep.nonce.wrapping_add(1);
    }
    let since_register = rep.step.wrapping_sub(rep.register_at);
    if !rep.registered && (!rep.register_offered || since_register >= REOFFER_STEPS) {
        let corr = origin::corr(rep.replica, 0, 0);
        rep.reg_tries = rep.reg_tries.wrapping_add(1);
        if b.propose(KV_OP_ORIGIN_REGISTER, corr, &[&rep.nonce.to_le_bytes()]) {
            rep.reg_sent = rep.reg_sent.wrapping_add(1);
            rep.register_offered = true;
            rep.register_at = rep.step;
        }
    }
    let waiting = rep
        .writes
        .iter()
        .any(|x| x.state == W_PROPOSED && x.offered);
    if waiting && rep.step.wrapping_sub(rep.progress_at) >= REOFFER_STEPS {
        rep.reoffer();
    }
    offer(rep, b);
    // Chunks in flight that none of has applied for a while: offered
    // again, in upload order.
    let chunk_waiting = rep.chunks.iter().any(|c| c.used && c.offered);
    if chunk_waiting && rep.step.wrapping_sub(rep.chunk_progress_at) >= REOFFER_STEPS {
        for c in rep.chunks.iter_mut().filter(|c| c.used) {
            c.offered = false;
        }
        rep.chunk_progress_at = rep.step;
    }
    offer_chunks(rep, b);
}

// ── A host store's log (`store.log`) ─────────────────────────────────

/// fluxor's host store appends `[rev:u64][op:u8][key_len:u16][val_len:u32]
/// [key][val]` per write (`fluxor/src/platform/linux/store.rs`); a store
/// moving onto lattice replays it once (the worker's import).
pub const STORE_LOG_HEAD: usize = 15;
pub const STORE_LOG_PUT: u8 = 1;
pub const STORE_LOG_DELETE: u8 = 2;

/// A host store log record's head: `(op, key length, value length)`.
pub fn store_log_head(h: &[u8]) -> Option<(u8, usize, usize)> {
    // As the host replays it: a delete, and anything else a put.
    Some((*h.get(8)?, rd16(h, 9)?, rd32(h, 11)?))
}

// ── Dispatch ─────────────────────────────────────────────────────────

/// Whether a call needs the store (its data or history). One that does
/// not — a subscription with no initial listing, a close, the caps — is
/// served while the store is still recovering, so a consumer that
/// subscribes at start is registered, not refused (it would not ask
/// again, and would never be woken).
///
/// # Safety
/// `arg` names `arg_len` bytes (or is null).
pub unsafe fn needs_store(op: u32, arg: *const u8, arg_len: usize) -> bool {
    match op {
        OBJ_CLOSE | NS_CLOSE | NS_CAPS => false,
        NS_SUBSCRIBE => {
            let a: &[u8] = if arg.is_null() {
                &[]
            } else {
                core::slice::from_raw_parts(arg, arg_len)
            };
            // `[plen u16][prefix][sink u32][flags u8]`: bit 0 lists first.
            let flags = rd16(a, 0).and_then(|pl| a.get(2 + pl + 4)).copied();
            flags.is_some_and(|f| f & 1 != 0)
        }
        _ => true,
    }
}

/// A contract call, answered in the call. `handle` arrives with its tag
/// stripped.
///
/// # Safety
/// `arg` names `arg_len` bytes; the pointers the arguments carry name
/// what the contract says they do (the caller's buffers).
/// The most a key-bearing argument takes once its names are the store's.
const SCOPED_ARG_MAX: usize = 1024;

/// Move `len` bytes from `from` to `to` (`to <= from`) within `b`.
fn shift_down(b: &mut [u8], to: usize, from: usize, len: usize) {
    for i in 0..len {
        if let Some(&x) = b.get(from + i) {
            if let Some(y) = b.get_mut(to + i) {
                *y = x;
            }
        }
    }
}

/// `a` with its leading `[len u16][name]` field (and, for a LIST, the
/// cursor field after it) in the store's names, into `out`: its length,
/// and whether the name was a scoped one (the answer then names keys the
/// graph's way once stripped). `Err` for an argument too short or a scoped
/// name past [`KEY_MAX`].
fn scoped_arg(
    sc: &Scope,
    op: u32,
    a: &[u8],
    out: &mut [u8; SCOPED_ARG_MAX],
) -> Result<(usize, bool), i32> {
    let mut name = [0u8; KEY_MAX];
    if op == OBJ_GET {
        let n = sc.inward(a, &mut name).ok_or(EINVAL)?;
        copy(out, name.get(..n).unwrap_or(&[]));
        return Ok((n, sc.holds(a)));
    }
    let mut p = 0usize;
    let first = field16(a, &mut p).ok_or(EINVAL)?;
    let scoped = sc.holds(first);
    let mut at = 0usize;
    let n = sc.inward(first, &mut name).ok_or(EINVAL)?;
    put(out, &mut at, &(n as u16).to_le_bytes()).ok_or(EINVAL)?;
    put(out, &mut at, name.get(..n).unwrap_or(&[])).ok_or(EINVAL)?;
    if op == NS_LIST {
        let cursor = field16(a, &mut p).ok_or(EINVAL)?;
        let c = if scoped && !cursor.is_empty() {
            sc.inward(cursor, &mut name).ok_or(EINVAL)?
        } else {
            copy(&mut name, cursor);
            cursor.len()
        };
        put(out, &mut at, &(c as u16).to_le_bytes()).ok_or(EINVAL)?;
        put(out, &mut at, name.get(..c).unwrap_or(&[])).ok_or(EINVAL)?;
    }
    put(out, &mut at, a.get(p..).unwrap_or(&[])).ok_or(EINVAL)?;
    Ok((at, scoped))
}

/// A LIST page in the graph's names: `head` bytes off every entry's key
/// and off the next page's cursor. Its new length.
fn strip_list(out: &mut [u8], len: usize, head: usize) -> usize {
    let (mut r, mut w) = (0usize, 0usize);
    while r + 2 < len + 1 {
        let (Some(&kl), Some(&kind)) = (out.get(r), out.get(r + 1)) else {
            break;
        };
        if kl == TRAILER_MARK && kind == TRAILER_MARK {
            let nl = out.get(r + 2).copied().unwrap_or(0) as usize;
            let keep = nl.saturating_sub(head);
            if let Some(x) = out.get_mut(w) {
                *x = TRAILER_MARK;
            }
            if let Some(x) = out.get_mut(w + 1) {
                *x = TRAILER_MARK;
            }
            if let Some(x) = out.get_mut(w + 2) {
                *x = keep as u8;
            }
            shift_down(out, w + 3, r + 3 + (nl - keep), keep);
            return w + 3 + keep;
        }
        let kl = kl as usize;
        let keep = kl.saturating_sub(head);
        if let Some(x) = out.get_mut(w) {
            *x = keep as u8;
        }
        if let Some(x) = out.get_mut(w + 1) {
            *x = kind;
        }
        shift_down(out, w + 2, r + 2 + (kl - keep), keep);
        r += 2 + kl;
        w += 2 + keep;
    }
    w
}

/// A CHANGES answer in the graph's names: `head` bytes off every change's
/// key. Its new length.
fn strip_changes(out: &mut [u8], len: usize, head: usize) -> usize {
    let count = match out.get(1..5).and_then(|c| c.try_into().ok()) {
        Some(c) => u32::from_le_bytes(c),
        None => return len,
    };
    let (mut r, mut w) = (5usize, 5usize);
    for _ in 0..count {
        let (Some(kl), Some(vl)) = (rd16(out, r + 9), rd32(out, r + 11)) else {
            break;
        };
        let keep = kl.saturating_sub(head);
        shift_down(out, w, r, 9);
        let k = (keep as u16).to_le_bytes();
        if let Some(x) = out.get_mut(w + 9) {
            *x = k[0];
        }
        if let Some(x) = out.get_mut(w + 10) {
            *x = k[1];
        }
        shift_down(out, w + 11, r + 11, 4);
        shift_down(out, w + 15, r + 15 + (kl - keep), keep + vl);
        r += 15 + kl + vl;
        w += 15 + keep + vl;
    }
    w
}

pub unsafe fn dispatch<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    handle: i32,
    op: u32,
    arg: *mut u8,
    arg_len: usize,
) -> i32 {
    let raw: &[u8] = if arg.is_null() {
        &[]
    } else {
        core::slice::from_raw_parts(arg as *const u8, arg_len)
    };
    // A scope's names: the key (or prefix) in the store's, the answer back
    // in the graph's.
    let mut tr = [0u8; SCOPED_ARG_MAX];
    let mut strip = 0usize;
    let keyed = matches!(
        op,
        OBJ_PUT | OBJ_GET | OBJ_HEAD | OBJ_DELETE | NS_LIST | NS_CHANGES | NS_SUBSCRIBE
    );
    let a: &[u8] = if pv.scope.head_len != 0 && keyed {
        match scoped_arg(&pv.scope, op, raw, &mut tr) {
            Ok((n, scoped)) => {
                if scoped {
                    strip = pv.scope.head_len as usize;
                }
                tr.get(..n).unwrap_or(&[])
            }
            Err(e) => return e,
        }
    } else {
        raw
    };
    let slot = slot_of(handle);
    // Every read but a slot's own passes through the cache.
    if !matches!(
        op,
        OBJ_RANGE_GET | OBJ_CLOSE | OBJ_HEAD | QUERY_OP | NS_CAPS
    ) {
        pv.cached = 0;
    }
    match op {
        QUERY_OP => {
            if arg.is_null() || arg_len < FENCE_WIRE_MAX {
                return EINVAL;
            }
            let sub = slot.checked_sub(SLOTS).and_then(|i| pv.subs.get(i));
            let rev = if slot < SLOTS && pv.slots[slot].used {
                pv.slots[slot].rev
            } else if sub.is_some_and(|s| s.used) {
                b.revision()
            } else {
                return ENOSYS;
            };
            write_fence(FENCE_VIEW_CONSISTENT, rev, arg as u64, arg_len) as i32
        }
        OBJ_PUT => obj_put(pv, b, bufs, a),
        OBJ_GET => obj_get(pv, b, bufs, a),
        OBJ_RANGE_GET => obj_range_get(pv, b, bufs, slot, a),
        OBJ_HEAD => obj_head(b, bufs, a),
        OBJ_DELETE => obj_delete(pv, b, bufs, a),
        OBJ_CLOSE => {
            if slot < SLOTS {
                pv.slots[slot].used = false;
                if pv.cached == (slot + 1) as u8 {
                    pv.cached = 0;
                }
            }
            0
        }
        NS_CAPS => NS_CAPS_SERVED,
        NS_LIST | NS_CHANGES => {
            let rc = if op == NS_LIST {
                ns_list(b, bufs, a)
            } else {
                ns_changes(b, bufs, a)
            };
            if rc <= 0 || strip == 0 {
                return rc;
            }
            // `[prefix][cursor (a LIST's)][out_ptr u64][out_cap u32]…`
            let mut p = 0usize;
            let _ = field16(a, &mut p);
            if op == NS_LIST {
                let _ = field16(a, &mut p);
            } else {
                p += 8;
            }
            let (Some(out_ptr), Some(out_cap)) = (rd64(a, p), rd32(a, p + 8)) else {
                return rc;
            };
            let out = caller_out(out_ptr, out_cap);
            let n = if op == NS_LIST {
                strip_list(out, rc as usize, strip)
            } else {
                strip_changes(out, rc as usize, strip)
            };
            n as i32
        }
        NS_SUBSCRIBE => ns_subscribe(pv, b, bufs, a, strip),
        NS_CLOSE => {
            if let Some(s) = slot.checked_sub(SLOTS).and_then(|i| pv.subs.get_mut(i)) {
                *s = NO_SUB;
            }
            0
        }
        _ => ENOSYS,
    }
}
