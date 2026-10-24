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
//! Subscriptions are level-triggered doorbells: a subscription names the
//! subscriber's own kernel event, and the provider never writes a
//! consumer's channel. Every command that stands notes the keys it changed
//! (`kv_store::KeyLog`); the provider takes them into its change log
//! (`change_ring.rs`) when it pumps, matches each against the registry of
//! watched prefixes (`store_registry.rs`), and rings each matching
//! subscriber once — a rung bit, and a signal — until the subscriber takes
//! it (`TAKE_RUNG`) and reads what changed (`CHANGES`, one bounded page per
//! call, served from the change log, or from the store's history across a
//! gap). Nothing is ever pushed, copied per subscriber, or lost to a full
//! channel.
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
//! The change log answers for the revisions since it began (or since its
//! last gap); before that, history is the store's: the disk provider keeps
//! it, the memory provider answers every question about the past LOST —
//! honestly, and uselessly across a gap — so a worker on it does not take
//! the provider role.
//!
//! Handles: object read slots are `0..SLOTS`, subscriptions from
//! [`SUB_BAND`] (`SUB_BAND + id`, a registry id), so a fence query
//! (`QUERY_OP`, which both contracts carry and which reaches the provider
//! with its tag stripped) names one thing. Every handle answers the owner
//! that opened it alone: to any other caller a read slot or subscription is
//! as if it were not open, as the host stores answer. (The system owner's record is all zero — slot 0,
//! generation 0 — so all zero is an owner like any other.)
//!
//! Fault injection (TEST ONLY, off unless configured): [`Provider::
//! fault_read_eagain_every`] makes the provider answer reads `EAGAIN` the
//! way a board's store does while its device is busy, so a Linux E2E can
//! prove its consumers never read "not yet" as absence. See [`Fault`].
//!
//! Mounted beside `types.rs`, `kv_large.rs`, `doc_tree.rs`, `origin.rs`,
//! `change_ring.rs` and `store_registry.rs` (reached as `super::…`).
//! No allocation and no panic path: the worker twin that mounts it is a
//! PIC module.

use super::change_ring::{ChangeRing, Cursor, ORD_END};
use super::doc_tree::utf8_is_valid;
use super::kv_large::kv_chunks::{
    sha256_of, Builder, Hash, Manifest, CHUNK_MAX, DATA_MAX, HASH_LEN, MANIFEST_LEN,
};
use super::kv_large::{self, Expander, Need};
use super::origin::{self, Outcome, Record, IMPORT_MARK_LEN, NO_OUTCOME, RING};
use super::store_registry::{Registry, Table, NIL as SUB_NIL};
use super::types::{
    KV_OP_DELETE, KV_OP_ORIGIN_CHUNK, KV_OP_ORIGIN_IMPORT, KV_OP_ORIGIN_REGISTER,
    KV_OP_ORIGIN_WRITE, KV_OP_PUT, KV_OP_PUT_MANIFEST, KV_OP_RANGE_META, KV_OP_SCAN_VERSIONS,
    KV_OP_TXN, KV_RESULT_COMPACTED, KV_RESULT_INTEGER, KV_RESULT_INTERNAL, KV_RESULT_KVS,
    KV_RESULT_MISSING_CHUNK, KV_RESULT_OK, KV_RESULT_PAUSED, KV_RESULT_TOO_LARGE, KV_RESULT_TXN,
    KV_RESULT_VERSIONS, RANGE_META_FLAG_MANIFESTS, RECORD_FLAG_MANIFEST, TXN_CMP_MOD_EQUAL,
    VERSIONS_FLAG_MANIFESTS, VERSIONS_FLAG_META, VERSION_KIND_DELETE,
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
/// Which of the caller's subscriptions on an event have rung.
pub const NS_TAKE_RUNG: u32 = 0x1309;
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
/// Read slots open at once, at most: a format bound (a slot's index is its
/// handle). Slots are made from the heap as reads open, so the count in use
/// is the memory they hold, not this figure.
pub const SLOTS: usize = 4096;
/// Subscription handles' band in the slot field: `SUB_BAND + id`.
pub const SUB_BAND: usize = 1 << 24;
/// SUBSCRIBE flag bit 0: ring once now.
const SUB_RING_NOW: u8 = 1;
/// A workload owner's subscriptions at most, by default (the worker's
/// `store_sub_quota` param sets it). The system owner is not held to it.
pub const DEFAULT_SUB_QUOTA: u32 = 256;
/// A key-log hint: the write created the key (`kv_store::KEY_CREATED`).
pub const KEY_CREATED: u8 = 0;
/// A key-log hint: the write deleted the key (`kv_store::KEY_DELETED`).
pub const KEY_DELETED: u8 = 2;
/// TAKE_RUNG's answer head: `[more u8][count u16]`.
const TAKE_HEAD: usize = 3;
/// A CHANGES page: head `[status][more][safe_rev u64][count u32]`, a
/// record's fixed bytes, the trailer's fixed bytes (`[token_len u16]`).
const CH_HEAD: usize = 14;
const CH_REC: usize = 15;
const CH_TRAILER: usize = 2;
/// The longest CHANGES token, and a bare revision token's length.
const TOKEN_MAX: usize = 320;
const TOKEN_REV: usize = 8;
/// Tokens this provider issues: inside the change log, a relist, a history
/// walk.
const T_LOG: u8 = 1;
const T_RELIST: u8 = 2;
const T_HIST: u8 = 3;
/// Change-log entries one CHANGES call scans at most.
const LOG_SCAN_MAX: u32 = 4096;
/// Store versions one CHANGES call's history walk visits at most, and all
/// callers' walks in one worker step.
const HIST_VISIT_MAX: u32 = 1024;
pub const WALK_STEP_BUDGET: u32 = 4096;
/// A history page's key scratch: `[kl u8][key KEY_MAX][rev u64][kind u8]`
/// per key.
const HIST_ENTRY_FIXED: usize = 10;
const HIST_KEYS: usize = 32;
/// Change-log entries one pump matches at most, and subscribers a gap
/// rings in one pump.
const PUMP_MAX: u32 = 4096;
const GAP_RING_MAX: usize = 512;
/// A value a read slot or an event holds itself: one chunk's worth.
pub const INLINE_MAX: usize = DATA_MAX;
/// Entries one store page asks for (LIST, CHANGES, pumps).
const PAGE_KEYS: u16 = 64;
/// Re-drives of one paused store walk before the provider gives up on it
/// (each moves the walk on by the store's step budget).
const REDRIVE_MAX: u32 = 1 << 20;
/// The request buffer's floor: the largest request built — a guarded put
/// of an inline value ([`txn`]), which carries its key twice, in the
/// comparison and in the put: three `u16` counts, 11 bytes of comparison
/// besides its key, and 18 of put besides its key and value. A chunk upload
/// (`[hash][len u16][chunk]`) and every read's body are smaller.
pub const REQ_MIN: usize = 3 * 2 + 11 + 18 + 2 * KEY_MAX + INLINE_MAX;
const _: () = assert!(REQ_MIN >= HASH_LEN + 2 + CHUNK_MAX);
/// The answer buffer's floor: a store page of one entry with an inline
/// value.
pub const ANS_MIN: usize = 4096;
/// The change log's bytes by default (the worker's `store_change_log`
/// param sets it): a few seconds of a busy control plane's writes.
pub const CHANGE_LOG_DEFAULT: usize = 256 * 1024;
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
/// A single store's write answers held for their callers at once
/// ([`Provider::answers`]): one for each caller writing at the same time.
/// Full, a new write is not taken (`EINPROGRESS`) until one is collected or
/// expires — never by dropping another caller's.
pub const LOCAL_ANSWERS: usize = 1024;

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
    /// room, or its device made the op wait): the caller asks again.
    fn busy(&self) -> bool;
    /// Whether the store's own log holds a record the device has not
    /// finished with — not yet written, or not yet durable — after asking
    /// it on once more. A write whose record that is must not be answered
    /// as decided until this says `false`.
    fn log_pending(&mut self) -> bool {
        false
    }
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
    /// The module occupancy whose call is being served (`CALLER_MODULE`:
    /// `[idx u16][0][generation u32]`, which may be all zero — module 0,
    /// generation 0); `None` when there is none.
    fn module(&self) -> Option<[u8; 8]> {
        None
    }
    /// Whether a module occupancy still stands (`MODULE_LIVE`).
    fn module_live(&self, _module: [u8; 8]) -> bool {
        false
    }
    /// The module occupancy owning event `event` (`EVENT_OWNER`).
    fn event_owner(&self, _event: i32) -> Option<[u8; 8]> {
        None
    }
    /// Signal event `event` (`event::SIGNAL`): 0, or negative.
    fn signal(&mut self, _event: i32) -> i32 {
        ENOSYS
    }
    /// `bytes` of fresh heap that live as long as the provider (registry
    /// pages), or null when the heap is spent.
    fn alloc(&mut self, _bytes: usize) -> *mut u8 {
        core::ptr::null_mut()
    }
    /// The keys the commands that stood since the last call changed, in the
    /// order they changed (`f(rev, hint, key)`, the key log's entries), and
    /// cleared: `Some((complete, rev))` — `rev` the revision every one of
    /// them is settled up to, `complete` false when the list is not whole (a
    /// command's keys overflowed, or it changed the keyspace whole). `None`
    /// while a command is mid-way or its record is not yet durable: nothing
    /// is taken.
    fn take_keys<F: FnMut(u64, u8, &[u8])>(&mut self, _f: F) -> Option<(bool, u64)> {
        None
    }
    /// Count a subscription refused for want of capacity against the
    /// kernel's ledger (`resource::LEDGER_DENY` on `store_subs`).
    fn ledger_deny(&mut self) {}
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
}

/// An open read: the value of `key` at revision `rev`, answering only the
/// owner that opened it.
#[derive(Clone, Copy)]
struct Slot {
    used: bool,
    large: bool,
    /// The opener's owner record ([`Backend::caller`]).
    owner: [u8; 8],
    key_len: u8,
    rev: u64,
    len: u32,
    key: [u8; KEY_MAX],
    inline: [u8; INLINE_MAX],
}

const NO_SLOT: Slot = Slot {
    used: false,
    large: false,
    owner: [0; 8],
    key_len: 0,
    rev: 0,
    len: 0,
    key: [0; KEY_MAX],
    inline: [0; INLINE_MAX],
};

impl Slot {
    /// Whether this slot is open and `caller` opened it: a slot answers its
    /// opener alone, and to anyone else is as if it were not open (the host
    /// stores' answer, which says nothing of another owner's handles).
    fn opened_by(&self, caller: [u8; 8]) -> bool {
        self.used && self.owner == caller
    }
}

/// The provider's counts (telemetry).
#[derive(Clone, Copy, Default)]
pub struct Counters {
    /// Subscribers rung, rung bits taken, gaps rung to every subscriber,
    /// subscriptions dropped (their module or event gone), subscriptions
    /// refused (capacity or quota), SUBSCRIBEs naming an event not the
    /// caller's, history reads refused for the step's walk budget.
    pub rung: u32,
    pub taken: u32,
    pub gaps: u32,
    pub reaped: u32,
    pub refused: u32,
    pub owner_checks_failed: u32,
    pub walk_refused: u32,
}

impl Counters {
    const ZERO: Counters = Counters {
        rung: 0,
        taken: 0,
        gaps: 0,
        reaped: 0,
        refused: 0,
        owner_checks_failed: 0,
        walk_refused: 0,
    };
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
    slots: Table<Slot, SLOTS>,
    /// Subscriptions: watched prefixes and their subscribers.
    reg: Registry,
    /// The change log subscriptions are rung from and CHANGES is served
    /// from.
    ring: ChangeRing,
    /// The pump's place in the change log, and the log's gap count it last
    /// rang.
    pump_at: Cursor,
    pump_gaps: u32,
    /// A gap is being rung to every subscriber, a batch a pump: the last one
    /// rung.
    gap_ringing: bool,
    gap_at: u32,
    /// The worker's steps (from 1), the step whose refused readers were last
    /// rung again, the history-walk budget left this step, and the readers
    /// holding first claim on it this step.
    epoch: u32,
    deferred_epoch: u32,
    walk_left: u32,
    prio_pending: u32,
    /// A workload owner's subscriptions at most (0: [`DEFAULT_SUB_QUOTA`]).
    quota: u32,
    /// A history page's keys, gathered before their present state is read.
    hist_keys: [u8; HIST_KEYS * (HIST_ENTRY_FIXED + KEY_MAX)],
    /// Counted (telemetry).
    pub m: Counters,
    /// The store is serving: the change log starts at its revision. Before,
    /// a subscription is only registered (see [`needs_store`]).
    opened: bool,
    /// The large slot whose value `Bufs::cache` holds (index + 1; 0:
    /// none), and where in it the value lies.
    cached: u32,
    cache_at: u32,
    /// Writes through the group's log (a replica's).
    rep: Replication,
    /// This node's private records (a replica's).
    scope: Scope,
    /// A single store's writes that were applied while their records were
    /// not yet durable in its own log, each answer kept until its caller
    /// collects it: keyed by the caller and the request's bytes as a
    /// replica keys its writes, so asking again is a second look at the
    /// same write, whoever else wrote meanwhile (see [`local_write`]).
    answers: Table<Answer, LOCAL_ANSWERS>,
    /// The identity of each caller's latest write request, so an identical
    /// re-ask is known without hashing its value again ([`identity`]).
    idents: Table<Ident, LOCAL_ANSWERS>,
    /// What the identities' checksums are keyed with (set at `open`).
    ident_seed: u64,
    /// Write requests hashed whole (telemetry: once per request, not per
    /// re-ask).
    pub m_ident_hashed: u32,
    /// A single store's large value being uploaded, resumed where it stopped
    /// when its write is asked again ([`staged_upload`]).
    stage: Stage,
    /// Injected read waits (test only; off unless configured).
    fault: Fault,
}

/// TEST-ONLY fault injection: reads answered `EAGAIN`, as a board's store
/// answers them while its device is busy with a write; and (`hold`) a
/// write's log record kept pending, as a board's device keeps it while it
/// writes.
///
/// With `every = N` (N >= 2), one step of the store in N is BUSY: from the
/// worker's step that opens it ([`Provider::step`], every Nth) to its
/// next, every read the provider is asked — `GET`, `HEAD`, `RANGE_GET`,
/// `LIST` and `CHANGES`, by any module — answers `EAGAIN`, as the board's
/// store answers every read while its device is taken by a write. A read
/// answered so has moved nothing: no handle is opened, none is closed (a
/// `RANGE_GET`'s handle stays valid, at the offset it was asked), no page
/// is written, no fence is written, and nothing else of the provider's
/// state changes — but for what the contract asks of a waiting read: a
/// `CHANGES` that names one of the caller's subscriptions has it rung again
/// once the wait clears. So a busy step is invisible to a consumer that
/// keeps the contract — it holds the read and asks again on a later step,
/// or when its event rings — and fatal to one that reads `EAGAIN` as
/// absent, empty or failed (a NotFound for a key it has just written), or
/// asks again inside the same step (it spins: the step stays busy until the
/// worker steps, which it never does while the spinner holds the core).
///
/// Whole steps, not every Nth read: every module meets the wait alike (a
/// read count would land on whichever modules step late in the order, every
/// step, and starve them), and N - 1 steps in N are clear, so a pass that
/// restarts from its first read still gets through. Every other call —
/// writes, `CLOSE`, `SUBSCRIBE`, `TAKE_RUNG`, the fence query and `CAPS` —
/// is never faulted: a write has its own "not yet" (`EINPROGRESS`), and
/// `EAGAIN` on a write means an etag was lost.
/// Deterministic: busy steps are the worker's step count modulo N, from the
/// knob's setting. `every = 1` would make every step busy and never let a
/// read through: refused. 0 is off.
///
/// Where a busy step meets a read: at dispatch, before the provider looks
/// at it (the default); or, with `deep`, in the store itself — every store
/// op the read applies answers `KV_RESULT_INTERNAL` with
/// [`Backend::busy`] set, exactly as the worker's store answers while its
/// device makes the op wait, so the provider's own read paths are what
/// must answer `EAGAIN`.
///
/// `hold`: every single-store write applied keeps the log pending for
/// this many further worker steps, so another caller's write lands between
/// the write and its re-ask (the board's device holding the record).
#[derive(Clone, Copy)]
struct Fault {
    every: u32,
    /// The worker's steps since the knob was set.
    steps: u32,
    /// This step is busy: every read answers `EAGAIN`.
    busy: bool,
    /// A busy step reaches the store rather than ending at dispatch.
    deep: bool,
    /// Steps a write's log record is held pending, and those left of it.
    hold: u32,
    held: u32,
    /// Reads a busy step met by injection (telemetry): at dispatch, each
    /// one answered `EAGAIN`; in the store, each one sent there.
    injected: u32,
}

const NO_FAULT: Fault = Fault {
    every: 0,
    steps: 0,
    busy: false,
    deep: false,
    hold: 0,
    held: 0,
    injected: 0,
};

/// Where an injected wait meets a read (see [`Fault`]).
#[derive(PartialEq, Eq)]
enum Injected {
    No,
    AtDispatch,
    InStore,
}

/// A single store's write answer kept for its caller (see
/// [`local_write`]).
#[derive(Clone, Copy)]
struct Answer {
    used: bool,
    /// Its log record is durable: the answer may be collected. At most one
    /// answer is not (a write is taken only once the log has settled).
    durable: bool,
    /// The decision (0, or the errno), and the revision a success is at.
    rc: i32,
    rev: u64,
    /// Steps since it was decided or last asked for.
    idle: u32,
    caller: [u8; 8],
    id: Hash,
}

const NO_ANSWER: Answer = Answer {
    used: false,
    durable: false,
    rc: 0,
    rev: 0,
    idle: 0,
    caller: [0; 8],
    id: [0u8; HASH_LEN],
};

/// A write request's identity, kept for its caller (see [`identity`]):
/// what the request asks, exactly, but for its value, which is known by
/// its length and a checksum.
#[derive(Clone, Copy)]
struct Ident {
    used: bool,
    caller: [u8; 8],
    put: bool,
    pre: u8,
    rev: u64,
    import: bool,
    mark: [u8; IMPORT_MARK_LEN],
    key: [u8; KEY_MAX],
    key_len: u8,
    len: u64,
    check: [u64; 2],
    /// Steps since it was last asked (the least recently asked is the one
    /// a new caller's request replaces).
    idle: u32,
    id: Hash,
}

const NO_IDENT: Ident = Ident {
    used: false,
    caller: [0; 8],
    put: false,
    pre: 0,
    rev: 0,
    import: false,
    mark: [0; IMPORT_MARK_LEN],
    key: [0; KEY_MAX],
    key_len: 0,
    len: 0,
    check: [0; 2],
    idle: 0,
    id: [0u8; HASH_LEN],
};

/// A single store's large value being uploaded (see [`staged_upload`]).
struct Stage {
    active: bool,
    /// The write it is for.
    id: Hash,
    /// Value bytes the builder has been fed.
    fed: u64,
    builder: Builder,
    /// A chunk built and not yet taken by the store (it was making room).
    held: bool,
    held_len: usize,
    held_hash: Hash,
    held_chunk: [u8; CHUNK_MAX],
    /// Every chunk is stored: the value's manifest.
    done: bool,
    manifest: [u8; MANIFEST_LEN],
}

const NO_STAGE: Stage = Stage {
    active: false,
    id: [0u8; HASH_LEN],
    fed: 0,
    builder: Builder::IDLE,
    held: false,
    held_len: 0,
    held_hash: [0u8; HASH_LEN],
    held_chunk: [0; CHUNK_MAX],
    done: false,
    manifest: [0; MANIFEST_LEN],
};

impl Provider {
    pub const fn new() -> Self {
        Provider {
            slots: Table::new(),
            reg: Registry::new(),
            ring: ChangeRing::new(),
            pump_at: Cursor::START,
            pump_gaps: 0,
            gap_ringing: false,
            gap_at: SUB_NIL,
            epoch: 0,
            deferred_epoch: 0,
            walk_left: 0,
            prio_pending: 0,
            quota: 0,
            hist_keys: [0; HIST_KEYS * (HIST_ENTRY_FIXED + KEY_MAX)],
            m: Counters::ZERO,
            opened: false,
            cached: 0,
            cache_at: 0,
            rep: NO_REPLICATION,
            scope: NO_SCOPE,
            answers: Table::new(),
            idents: Table::new(),
            ident_seed: 0,
            m_ident_hashed: 0,
            stage: NO_STAGE,
            fault: NO_FAULT,
        }
    }

    /// TEST ONLY: make one store step in `every` busy, every read in it
    /// answered `EAGAIN` (see [`Fault`]). 0 is off; 1 is refused (`false`,
    /// and off): every step would be busy.
    pub fn fault_read_eagain_every(&mut self, every: u32) -> bool {
        let f = &mut self.fault;
        f.every = 0;
        f.steps = 0;
        f.busy = false;
        f.injected = 0;
        if every == 1 {
            return false;
        }
        f.every = every;
        true
    }

    /// TEST ONLY: whether a busy step reaches the store (`true`) or ends
    /// at dispatch (see [`Fault`]).
    pub fn fault_busy_reaches_store(&mut self, deep: bool) {
        self.fault.deep = deep;
    }

    /// TEST ONLY: keep the log pending `steps` worker steps after each
    /// single-store write (see [`Fault`]). 0 is off.
    pub fn fault_log_hold_steps(&mut self, steps: u32) {
        self.fault.hold = steps;
        self.fault.held = 0;
    }

    /// The worker has stepped: the next step is busy or clear, a held log
    /// record is a step nearer settling, and an answer nobody asks for a
    /// step nearer expiring.
    pub fn step(&mut self) {
        self.epoch = self.epoch.wrapping_add(1).max(1);
        self.walk_left = WALK_STEP_BUDGET;
        self.prio_pending = 0;
        let f = &mut self.fault;
        if f.every != 0 {
            f.steps = f.steps.wrapping_add(1);
            f.busy = f.steps.is_multiple_of(f.every);
        }
        f.held = f.held.saturating_sub(1);
        self.idents.each_mut(|x| {
            if x.used {
                x.idle = x.idle.saturating_add(1);
            }
        });
        self.answers.each_mut(|x| {
            if x.used {
                x.idle = x.idle.saturating_add(1);
                if x.idle > IDLE_STEPS {
                    // Its caller stopped asking: the write stands (it is in
                    // the log), only its answer goes uncollected.
                    *x = NO_ANSWER;
                }
            }
        });
    }

    /// Reads answered `EAGAIN` by injection, and whether it is on.
    pub fn fault_injected(&self) -> (bool, u32) {
        (self.fault.every != 0, self.fault.injected)
    }

    /// Single-store write answers held for their callers.
    pub fn answers_held(&self) -> usize {
        self.answers.iter().filter(|x| x.used).count()
    }

    /// Whether, and where, this read meets an injected wait: the step is
    /// busy.
    fn fault_read(&mut self, op: u32, _a: &[u8]) -> Injected {
        let f = &mut self.fault;
        if !f.busy {
            return Injected::No;
        }
        let read = matches!(
            op,
            OBJ_GET | OBJ_HEAD | OBJ_RANGE_GET | NS_LIST | NS_CHANGES
        );
        if !read {
            return Injected::No;
        }
        f.injected = f.injected.wrapping_add(1);
        if f.deep {
            Injected::InStore
        } else {
            Injected::AtDispatch
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

    /// Start at the store's revision: the change log answers for what
    /// happens from here on, and every subscriber registered before is rung
    /// to read (it may have missed anything before now).
    pub fn open<B: Backend>(&mut self, b: &B) {
        self.ring.reset(b.revision());
        self.pump_at = self.ring.tip();
        self.pump_gaps = self.ring.gaps;
        self.gap_ringing = self.reg.counts().0 > 0;
        self.gap_at = SUB_NIL;
        self.opened = true;
        self.ident_seed = b.revision() ^ (self as *const Self as usize as u64).rotate_left(17);
    }

    /// Give the change log its bytes: `cap` of them at `buf`, from the
    /// worker's heap, for the provider's life.
    ///
    /// # Safety
    /// `buf` must be valid for `cap` bytes for as long as the provider is.
    pub unsafe fn attach_log(&mut self, buf: *mut u8, cap: usize) {
        self.ring.attach(buf, cap);
    }

    /// Hold a workload owner to `quota` subscriptions (0: the default).
    pub fn set_quota(&mut self, quota: u32) {
        self.quota = quota;
    }

    /// Subscriptions held.
    pub fn subscriptions(&self) -> usize {
        self.reg.counts().0 as usize
    }

    /// Subscriptions rung and not yet taken.
    pub fn rung(&self) -> usize {
        let mut n = 0;
        self.reg.each_sub(|id| {
            n += usize::from(self.reg.sub(id).is_some_and(|s| s.rung));
            true
        });
        n
    }

    /// The change log: its base and frontier, and the bytes it holds.
    pub fn log_span(&self) -> (u64, u64, u64) {
        (self.ring.base(), self.ring.frontier(), self.ring.used())
    }

    /// Registry pages held (each [`store_registry::PAGE`] bytes of heap).
    pub fn registry_pages(&self) -> usize {
        self.reg.counts().2
    }

    /// A gap the worker knows of (an install, an import): changes up to
    /// `rev` are not all in the change log, and every subscriber reads
    /// again.
    pub fn log_gap(&mut self, rev: u64) {
        if self.opened {
            self.ring.gap(rev);
        }
    }

    /// An owner is released: its subscriptions go now (the lazy checks
    /// before every signal would drop them anyway; this returns their
    /// memory early).
    pub fn owner_released(&mut self, owner: [u8; 8]) -> u32 {
        let mut gone = 0;
        let mut dead = [SUB_NIL; 64];
        loop {
            let mut n = 0;
            self.reg.each_sub(|id| {
                if self.reg.sub(id).is_some_and(|s| s.owner == owner) && n < dead.len() {
                    dead[n] = id;
                    n += 1;
                }
                n < dead.len()
            });
            if n == 0 {
                return gone;
            }
            for &id in &dead[..n] {
                self.reg.remove_sub(id);
            }
            gone += n as u32;
        }
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
            chunks_held: r.chunks.iter().filter(|c| c.used).count() as u32,
            chunks_offered: r.chunks.iter().filter(|c| c.used && c.offered).count() as u32,
            chunk_refused: r.chunk_refused,
            chunk_idle: r.step.wrapping_sub(r.chunk_progress_at),
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
    /// The upload's chunks in flight, and of them those offered; chunk
    /// applies this replica saw refused; steps since one last applied.
    pub chunks_held: u32,
    pub chunks_offered: u32,
    pub chunk_refused: u32,
    pub chunk_idle: u32,
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

/// A read the store did not answer now: its device made the read wait
/// (`busy`, whatever result came with it), or its walk is still paused
/// after every re-drive (the store keeps its place). Either way nothing
/// moved, and the contract's word is `EAGAIN`: the caller asks again with
/// the same request. Every read path checks this before it reads the
/// answer — a busy store's answer is not an answer, and read as one an
/// object that exists would be told absent.
fn read_waits<B: Backend>(b: &B, r: u8) -> bool {
    r == KV_RESULT_PAUSED || b.busy()
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
    if read_waits(b, r) {
        return Err(EAGAIN);
    }
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
                if read_waits(b, cr) {
                    return Err(EAGAIN);
                }
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
    if read_waits(b, r) {
        return Err(EAGAIN);
    }
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
    if r == KV_RESULT_MISSING_CHUNK {
        return Err(RESTAGE);
    }
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
        Some(KV_RESULT_MISSING_CHUNK) => Err(RESTAGE),
        _ => Err(ERROR),
    }
}

/// [`commit`]'s word, never a caller's: the manifest names a chunk the store
/// no longer holds (see [`put_staged`]).
const RESTAGE: i32 = i32::MIN;

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
    local_write(pv, b, &w, |pv, b, id| put_staged(pv, b, bufs, &w, Some(id)))
}

/// A single store's write: decided in the call — and answered so once the
/// store's own log holds its record durably. A write whose record the
/// device has not made durable yet is answered `EINPROGRESS` and its
/// decision kept for its caller ([`Provider::answers`]); the same request
/// asked again is answered from it once the log is durable, never applied a
/// second time.
///
/// Why each part holds:
/// - Keyed by `(caller, request bytes)`: another caller's write never meets
///   this one's answer, so a write asked between a decision and its re-ask
///   (a second module writing, which nothing stops) cannot lose it.
/// - One write in the log at a time: while the log is pending no write is
///   taken (`EINPROGRESS`: asked again, taken then). A write is applied
///   only once every record before it is durable, so writes reach the store
///   and its log in the order they are taken, each answer's revision is
///   exactly its own, and at most one held answer is not yet durable — once
///   the log settles, every held answer is (`durable`), and is collected
///   even while a later write's record is pending.
/// - Bounded: [`LOCAL_ANSWERS`]. Full of other callers' answers, a write is
///   not taken (`EINPROGRESS`) — never by dropping one of theirs.
/// - An answer goes when its caller collects it; when that caller asks a
///   different write, which is taken (a caller asks one write at a time:
///   it has stopped asking for the old one); or after [`IDLE_STEPS`]
///   steps unasked ([`Provider::step`]). The write stands either way (it is
///   in the log); only its answer goes uncollected, and the same request
///   asked after that is a new write.
fn local_write<B: Backend, F: FnOnce(&mut Provider, &mut B, &Hash) -> Result<(), i32>>(
    pv: &mut Provider,
    b: &mut B,
    w: &WriteArgs<'_>,
    apply: F,
) -> i32 {
    let caller = b.caller();
    let id = identity(pv, b, caller, w);
    if let Some(k) = pv.answers.position(|x| x.used && x.id == id) {
        let durable = pv.answers.get_mut(k).is_none_or(|x| {
            x.idle = 0;
            x.durable
        });
        if !durable {
            if log_pending(pv, b) {
                return EINPROGRESS;
            }
            settled(pv);
        }
        let Some(slot) = pv.answers.get_mut(k) else {
            return ERROR;
        };
        let x = *slot;
        *slot = NO_ANSWER;
        if x.rc == 0 {
            // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
            unsafe { write_fence(write_fence_kind(b), x.rev, w.fence_ptr, w.fence_cap) };
        }
        return x.rc;
    }
    if log_pending(pv, b) {
        // The log is finishing another record; nothing is applied now.
        return EINPROGRESS;
    }
    settled(pv);
    // A room for its answer, should its record wait: one of this caller's
    // own (it asks something else now) or a free one.
    let room = match pv.answers.position(|x| x.used && x.caller == caller) {
        Some(k) => Some(k),
        None => {
            let mut alloc = |n: usize| b.alloc(n);
            pv.answers.take(&mut alloc, NO_ANSWER, |x| x.used)
        }
    };
    let Some(room) = room else {
        // Every answer held is another caller's, uncollected, and the heap
        // holds no room for another: not taken.
        return EINPROGRESS;
    };
    let rc = match apply(pv, b, &id) {
        Err(e) => e,
        Ok(()) => 0,
    };
    if rc == EINPROGRESS {
        // Not taken: the store is making room, or its device made the
        // write wait (and it was undone). Asked again, it is taken then.
        return EINPROGRESS;
    }
    // Taken: this caller's earlier answers go uncollected.
    pv.answers.each_mut(|x| {
        if x.used && x.caller == caller {
            *x = NO_ANSWER;
        }
    });
    pv.fault.held = pv.fault.hold;
    if log_pending(pv, b) {
        let Some(slot) = pv.answers.get_mut(room) else {
            return EINPROGRESS;
        };
        *slot = Answer {
            used: true,
            durable: false,
            rc,
            rev: b.revision(),
            idle: 0,
            caller,
            id,
        };
        return EINPROGRESS;
    }
    if rc == 0 {
        // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
        unsafe { write_fence(write_fence_kind(b), b.revision(), w.fence_ptr, w.fence_cap) };
    }
    rc
}

/// Whether the store's log holds a record not yet durable (asking it on),
/// or one an injected hold keeps (test only, see [`Fault`]).
fn log_pending<B: Backend>(pv: &Provider, b: &mut B) -> bool {
    let pending = b.log_pending();
    pending || pv.fault.held > 0
}

/// The log has settled: every answer held is durable.
fn settled(pv: &mut Provider) {
    pv.answers.each_mut(|x| {
        if x.used {
            x.durable = true;
        }
    });
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
    let w = WriteArgs {
        key,
        value: Some(value),
        pre,
        rev,
        fence_ptr: 0,
        fence_cap: 0,
        mark: None,
    };
    put_staged(pv, b, bufs, &w, None)
}

/// [`put_value`] for write `w`; a large value uploaded through the stage
/// when the write has an identity (`id`), so a write the store refuses for
/// now resumes its upload when asked again rather than starting it over.
fn put_staged<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    w: &WriteArgs<'_>,
    id: Option<&Hash>,
) -> Result<(), i32> {
    let (key, value, pre, rev) = (w.key, w.value.unwrap_or(&[]), w.pre, w.rev);
    if key.is_empty() || !key_ok(key) {
        return Err(EINVAL);
    }
    let what = if value.len() <= INLINE_MAX {
        Put::Value(value)
    } else if let Some(id) = id {
        Put::Manifest(staged_upload(pv, b, bufs, id, value)?)
    } else {
        Put::Manifest(upload(b, bufs, value)?.encode())
    };
    let n = txn(bufs.req, key, pre != PRE_ANY, rev, Some(&what)).ok_or(EINVAL)?;
    let held = match commit(b, bufs, n, true) {
        // The chunks the stage stored are gone (collected while the write
        // waited past their grace): uploaded again when it is asked again.
        Err(RESTAGE) => {
            pv.stage.active = false;
            return Err(if id.is_some() { EINPROGRESS } else { ERROR });
        }
        // Not taken now: the stage keeps the manifest for the re-ask.
        Err(e) => return Err(e),
        Ok(held) => held,
    };
    // Decided: the stage has done its work.
    if id.is_some_and(|id| pv.stage.active && pv.stage.id == *id) {
        pv.stage.active = false;
    }
    match held {
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

/// Upload write `id`'s large `value` through the stage: its manifest once
/// every chunk is stored. A chunk the store refuses for now (it is making
/// room) is kept, and the write asked again resumes from it — the chunks
/// before it are neither built nor sent again, and once all are stored the
/// manifest is kept until the write is decided. Another write's upload
/// takes the stage over; this one, asked again, starts over (its chunks
/// already stored are no-ops to send again).
fn staged_upload<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    id: &Hash,
    value: &[u8],
) -> Result<[u8; MANIFEST_LEN], i32> {
    let st = &mut pv.stage;
    if !st.active || st.id != *id {
        st.builder = Builder::new(value.len() as u64).ok_or(EOVERFLOW)?;
        st.active = true;
        st.id = *id;
        st.fed = 0;
        st.held = false;
        st.done = false;
    }
    if st.done {
        return Ok(st.manifest);
    }
    loop {
        if !st.held {
            match st.builder.next(&mut st.held_chunk) {
                Some((n, h)) => {
                    st.held = true;
                    st.held_len = n;
                    st.held_hash = h;
                }
                None => {
                    let fed = st.fed as usize;
                    if fed == value.len() {
                        let m = st.builder.manifest().ok_or(ERROR)?.encode();
                        st.done = true;
                        st.manifest = m;
                        return Ok(m);
                    }
                    let take = st.builder.room().min(value.len() - fed);
                    let piece = value.get(fed..fed + take).ok_or(ERROR)?;
                    st.builder.feed(piece).ok_or(ERROR)?;
                    st.fed += take as u64;
                    continue;
                }
            }
        }
        let mut p = 0usize;
        put(bufs.req, &mut p, &st.held_hash).ok_or(ERROR)?;
        put(bufs.req, &mut p, &(st.held_len as u16).to_le_bytes()).ok_or(ERROR)?;
        put(
            bufs.req,
            &mut p,
            st.held_chunk.get(..st.held_len).ok_or(ERROR)?,
        )
        .ok_or(ERROR)?;
        let (r, _) = b.apply(
            kv_large::UPLOAD_OP,
            bufs.req.get(..p).ok_or(ERROR)?,
            bufs.ans,
        );
        if r != KV_RESULT_OK {
            if b.busy() {
                // Not now: the chunk stays held for the re-ask.
                return Err(EINPROGRESS);
            }
            st.active = false;
            return Err(ERROR);
        }
        st.held = false;
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
    copy(&mut mark, &offset.to_le_bytes());
    copy(mark.get_mut(8..).unwrap_or(&mut []), &crc.to_le_bytes());
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
    local_write(pv, b, &w, |pv, b, _| {
        delete_key(pv, b, bufs, w.key, w.pre, w.rev)
    })
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
    let owner = b.caller();
    let mut alloc = |n: usize| b.alloc(n);
    let Some(i) = pv.slots.take(&mut alloc, NO_SLOT, |s| s.used) else {
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
    let Some(s) = pv.slots.get_mut(i) else {
        return ENOMEM;
    };
    s.used = true;
    s.owner = owner;
    s.large = large;
    s.rev = modr;
    s.len = len32;
    s.key_len = a.len() as u8;
    copy(&mut s.key, a);
    if large {
        pv.cached = (i + 1) as u32;
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
    let s = *pv.slots.get(i).ok_or(ERROR)?;
    let key = s.key.get(..s.key_len as usize).ok_or(ERROR)?;
    let Some((modr, at, len)) = read_value(b, bufs, key)? else {
        pv.cached = 0;
        return Err(ERROR);
    };
    if modr != s.rev || len != s.len as usize {
        pv.cached = 0;
        return Err(ERROR);
    }
    pv.cached = (i + 1) as u32;
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
    if !pv.slots.get(slot).is_some_and(|s| s.opened_by(b.caller())) {
        return ENOSYS;
    }
    let (Some(off), Some(len), Some(out_ptr)) = (rd64(a, 0), rd32(a, 8), rd64(a, 12)) else {
        return EINVAL;
    };
    let Some(&s) = pv.slots.get(slot) else {
        return ENOSYS;
    };
    let total = u64::from(s.len);
    if off >= total {
        return 0;
    }
    let n = (len as u64).min(total - off) as usize;
    let from = off as usize;
    if s.large && pv.cached != (slot + 1) as u32 {
        if let Err(e) = recache(pv, b, bufs, slot) {
            return e;
        }
    }
    let src: &[u8] = if s.large {
        let at = pv.cache_at as usize + from;
        bufs.cache.get(at..at + n).unwrap_or(&[])
    } else {
        pv.slots
            .get(slot)
            .and_then(|s| s.inline.get(from..from + n))
            .unwrap_or(&[])
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

// ── Subscriptions: doorbells on the subscriber's own event ───────────

/// Whether `handle`'s slot names a subscription: its record id.
fn sub_id(slot: usize) -> Option<u32> {
    slot.checked_sub(SUB_BAND)
        .and_then(|i| u32::try_from(i).ok())
}

/// Ring subscriber `id`: set its rung bit and signal its event, once until
/// it is taken. Before every signal the subscriber's module occupancy must
/// still stand and its event still be that module's — a subscription whose
/// module is gone, or whose event slot another module now holds, is dropped
/// and never signalled. `false` when it was dropped.
fn ring_sub<B: Backend>(reg: &mut Registry, m: &mut Counters, b: &mut B, id: u32) -> bool {
    let Some(s) = reg.sub(id) else {
        return false;
    };
    if s.rung {
        return true;
    }
    let (module, event) = (s.module, s.event);
    if !b.module_live(module) || b.event_owner(event) != Some(module) || b.signal(event) < 0 {
        reg.remove_sub(id);
        m.reaped = m.reaped.wrapping_add(1);
        return false;
    }
    if let Some(s) = reg.sub_mut(id) {
        s.rung = true;
    }
    m.rung = m.rung.wrapping_add(1);
    true
}

/// `[prefix_len u16][prefix][event i32][flags u8]` (bit 0: ring now): a
/// subscription that rings the caller's event whenever something under the
/// prefix changes. Idempotent on (owner, module occupancy, event, prefix).
/// `EINVAL` for an event that is not the calling module's, or one that
/// refuses the signal a ring-now sends (the subscription is not kept).
fn ns_subscribe<B: Backend>(pv: &mut Provider, b: &mut B, a: &[u8], strip: usize) -> i32 {
    let mut p = 0usize;
    let Some(prefix) = field16(a, &mut p).filter(|k| key_ok(k)) else {
        return EINVAL;
    };
    let (Some(event), Some(&flags)) = (rd32(a, p), a.get(p + 4)) else {
        return EINVAL;
    };
    if a.len() != p + 5 {
        return EINVAL;
    }
    let event = event as i32;
    let owner = b.caller();
    let Some(module) = b.module() else {
        return EINVAL;
    };
    // The event must be the calling module's own: a subscription never
    // wakes anyone else.
    if b.event_owner(event) != Some(module) {
        pv.m.owner_checks_failed = pv.m.owner_checks_failed.wrapping_add(1);
        return EINVAL;
    }
    let ring_now = flags & SUB_RING_NOW != 0;
    // The same subscription asked again: the same handle.
    if let Some(w) = pv.reg.find_watch(prefix) {
        let mut s = pv.reg.watch(w).map_or(SUB_NIL, |w| w.subs);
        while let Some(rec) = pv.reg.sub(s) {
            if rec.owner == owner
                && rec.module == module
                && rec.event == event
                && rec.strip == strip as u8
            {
                if ring_now {
                    if let Some(r) = pv.reg.sub_mut(s) {
                        r.rung = false;
                    }
                    // Its event refused the signal: the subscription is
                    // gone, and no handle names it.
                    if !ring_sub(&mut pv.reg, &mut pv.m, b, s) {
                        return EINVAL;
                    }
                }
                return tagged(TAG_NAMESPACE, SUB_BAND + s as usize);
            }
            s = rec.next;
        }
    }
    // A workload owner's quota (the system owner, slot 0, holds the node's
    // own controllers): its dead subscriptions are swept before it is told
    // no.
    if owner[0] != 0 || owner[1] != 0 {
        let quota = if pv.quota == 0 {
            DEFAULT_SUB_QUOTA
        } else {
            pv.quota
        };
        if owned_subs(&mut pv.reg, b, owner) >= quota {
            pv.m.refused = pv.m.refused.wrapping_add(1);
            b.ledger_deny();
            return ENOMEM;
        }
    }
    let Provider { reg, m, .. } = pv;
    let mut alloc = |n: usize| b.alloc(n);
    let added = reg.watch_for(prefix, &mut alloc).and_then(|w| {
        reg.add_sub(w, &mut alloc, |s| {
            s.owner = owner;
            s.module = module;
            s.event = event;
            s.strip = strip as u8;
        })
    });
    let Ok(id) = added else {
        m.refused = m.refused.wrapping_add(1);
        b.ledger_deny();
        return ENOMEM;
    };
    if ring_now && !ring_sub(reg, m, b, id) {
        return EINVAL;
    }
    tagged(TAG_NAMESPACE, SUB_BAND + id as usize)
}

/// The subscriptions `owner` holds, after dropping those whose module is
/// gone.
fn owned_subs<B: Backend>(reg: &mut Registry, b: &mut B, owner: [u8; 8]) -> u32 {
    let mut dead = [SUB_NIL; 32];
    loop {
        let mut n = 0u32;
        let mut nd = 0usize;
        reg.each_sub(|id| {
            if let Some(s) = reg.sub(id) {
                if s.owner == owner {
                    if !b.module_live(s.module) && nd < dead.len() {
                        dead[nd] = id;
                        nd += 1;
                    } else {
                        n += 1;
                    }
                }
            }
            true
        });
        if nd == 0 {
            return n;
        }
        for &id in &dead[..nd] {
            reg.remove_sub(id);
        }
    }
}

/// `[event i32][out_ptr u64][out_cap u32]`: the caller's subscriptions on
/// `event` that have rung since it last asked, their bits cleared —
/// `[more u8][count u16][handle i32]…`. When they do not all fit, `more`
/// is set and the event signalled again, so the rest is taken on the next
/// wake.
fn ns_take_rung<B: Backend>(pv: &mut Provider, b: &mut B, a: &[u8]) -> i32 {
    let (Some(event), Some(out_ptr), Some(out_cap)) = (rd32(a, 0), rd64(a, 4), rd32(a, 12)) else {
        return EINVAL;
    };
    if a.len() != 16 || out_ptr == 0 {
        return EINVAL;
    }
    let event = event as i32;
    if out_cap < TAKE_HEAD + 4 {
        return ENOMEM;
    }
    // SAFETY: the contract names `out_cap` writable bytes at `out_ptr`.
    let out = unsafe { caller_out(out_ptr, out_cap) };
    let owner = b.caller();
    let Some(module) = b.module() else {
        return EINVAL;
    };
    let room = (out.len() - TAKE_HEAD) / 4;
    let mut count = 0usize;
    let mut more = false;
    let reg = &pv.reg;
    reg.each_sub(|id| {
        let Some(s) = reg.sub(id) else {
            return true;
        };
        if !(s.rung && s.event == event && s.owner == owner && s.module == module) {
            return true;
        }
        if count == room {
            more = true;
            return false;
        }
        let h = tagged(TAG_NAMESPACE, SUB_BAND + id as usize);
        let at = TAKE_HEAD + count * 4;
        copy(&mut out[at..at + 4], &h.to_le_bytes());
        count += 1;
        true
    });
    for i in 0..count {
        let at = TAKE_HEAD + i * 4;
        let h = rd32(out, at).unwrap_or(0) as i32;
        if let Some(id) = sub_id(slot_of(h)) {
            if let Some(s) = pv.reg.sub_mut(id) {
                s.rung = false;
            }
        }
    }
    pv.m.taken = pv.m.taken.wrapping_add(count as u32);
    out[0] = u8::from(more);
    copy(&mut out[1..3], &(count as u16).to_le_bytes());
    if more {
        let _ = b.signal(event);
    }
    (TAKE_HEAD + count * 4) as i32
}

/// Bring subscribers up to the change log: take the keys the worker's
/// commands settled, ring every subscriber whose prefix holds one, ring
/// everyone after a gap, and re-ring the subscribers whose reads were
/// refused for now. Bounded per call; the worker pumps after every command
/// it applies and on its step, and the provider's own writes pump
/// themselves.
pub fn pump<B: Backend>(pv: &mut Provider, b: &mut B, _bufs: &mut Bufs<'_>) {
    if !pv.ring.attached() {
        return;
    }
    take(pv, b);
    if !pv.opened {
        return;
    }
    // A refused read's subscriber, once a step: rung again, with first
    // claim on this step's walk budget.
    if pv.deferred_epoch != pv.epoch {
        pv.deferred_epoch = pv.epoch;
        let epoch = pv.epoch;
        let Provider {
            reg,
            m,
            prio_pending,
            ..
        } = pv;
        reg.drain_deferred(|reg: &mut Registry, id| {
            if let Some(s) = reg.sub_mut(id) {
                s.rung = false;
                s.prio = epoch;
            }
            if ring_sub(reg, m, b, id) {
                *prio_pending += 1;
            }
        });
    }
    // A gap: every subscriber reads again.
    if pv.ring.gaps != pv.pump_gaps {
        pv.pump_gaps = pv.ring.gaps;
        pv.pump_at = pv.ring.tip();
        pv.gap_ringing = true;
        pv.gap_at = SUB_NIL;
        pv.m.gaps = pv.m.gaps.wrapping_add(1);
    }
    if pv.gap_ringing {
        let mut batch = [SUB_NIL; GAP_RING_MAX];
        let n = pv.reg.subs_after(pv.gap_at, &mut batch);
        for &id in &batch[..n] {
            pv.gap_at = id;
            ring_sub(&mut pv.reg, &mut pv.m, b, id);
        }
        if n < GAP_RING_MAX {
            pv.gap_ringing = false;
        }
    }
    // The log's new entries, each to the subscribers whose prefix holds it.
    let mut key = [0u8; KEY_MAX];
    for _ in 0..PUMP_MAX {
        let kl = match pv.ring.next(&mut pv.pump_at) {
            Some(e) => {
                copy(&mut key, e.key);
                e.key.len()
            }
            None => break,
        };
        let Provider { reg, m, .. } = &mut *pv;
        reg.matches_mut(&key[..kl], |reg, id| {
            ring_sub(reg, m, b, id);
        });
    }
    if pv.pump_at.lost {
        // The pump fell behind the ring's eviction: what it missed cannot be
        // matched, so everyone reads again.
        pv.pump_at = pv.ring.tip();
        pv.gap_ringing = true;
        pv.gap_at = SUB_NIL;
        pv.m.gaps = pv.m.gaps.wrapping_add(1);
    }
}

/// Take the keys the worker's settled commands changed into the change log
/// (nothing is rung: the next pump does that). The worker calls it after
/// every command that stands, outside any provider call, so a step that
/// applies many commands never holds more than one command's keys staged;
/// [`pump`] calls it first.
pub fn take<B: Backend>(pv: &mut Provider, b: &mut B) {
    if !pv.ring.attached() {
        return;
    }
    let Provider { ring, opened, .. } = pv;
    let taken = b.take_keys(|rev, hint, key| {
        if *opened && !ring.append(rev, hint, key) {
            ring.gap(rev);
        }
    });
    if let Some((complete, rev)) = taken {
        if pv.opened {
            if complete {
                pv.ring.advance(rev);
            } else {
                pv.ring.gap(rev);
            }
        }
    }
}

// ── CHANGES: one bounded page per call ───────────────────────────────

/// One `CHANGES` page being written: `[status][more][safe_rev u64]
/// [count u32]`, records, then `[token_len u16][token]` — the head last,
/// with room for the trailer kept in reserve so a page can always end.
struct PageOut<'a> {
    out: &'a mut [u8],
    pos: usize,
    count: u32,
    /// Bytes a record may not use: the trailer's.
    reserve: usize,
    /// Bytes of the scope's head every record's key loses.
    strip: usize,
}

impl<'a> PageOut<'a> {
    fn new(out: &'a mut [u8], token_reserve: usize, strip: usize) -> Option<Self> {
        let reserve = CH_TRAILER + token_reserve;
        (out.len() >= CH_HEAD + reserve).then_some(PageOut {
            out,
            pos: CH_HEAD,
            count: 0,
            reserve,
            strip,
        })
    }

    /// Append a record; `false` when it does not fit beside the trailer.
    fn push(&mut self, mod_rev: u64, kind: u8, key: &[u8], val: &[u8]) -> bool {
        self.push_then(mod_rev, kind, key, val, 0)
    }

    /// [`push`](Self::push), keeping room for a token of `token_after`
    /// bytes should the page end after this record (a cursor naming it).
    fn push_then(
        &mut self,
        mod_rev: u64,
        kind: u8,
        key: &[u8],
        val: &[u8],
        token_after: usize,
    ) -> bool {
        let key = key.get(self.strip..).unwrap_or(key);
        let end = self.pos + CH_REC + key.len() + val.len();
        if end + self.reserve.max(CH_TRAILER + token_after) > self.out.len() {
            return false;
        }
        let mut at = self.pos;
        if change_into(self.out, &mut at, mod_rev, kind, key, val).is_none() {
            return false;
        }
        self.pos = at;
        self.count += 1;
        true
    }

    fn finish(self, more: bool, safe_rev: u64, token: &[u8]) -> i32 {
        let o = self.out;
        o[0] = 0;
        o[1] = u8::from(more);
        copy(&mut o[2..10], &safe_rev.to_le_bytes());
        copy(&mut o[10..14], &self.count.to_le_bytes());
        let mut at = self.pos;
        if put(o, &mut at, &(token.len() as u16).to_le_bytes()).is_none()
            || put(o, &mut at, token).is_none()
        {
            return ENOMEM;
        }
        at as i32
    }
}

/// A LOST page.
fn page_lost(out: &mut [u8]) -> i32 {
    let Some(o) = out.get_mut(..CH_HEAD + CH_TRAILER) else {
        return ENOMEM;
    };
    o.fill(0);
    o[0] = 1;
    (CH_HEAD + CH_TRAILER) as i32
}

/// One change record, `[rev][kind][klen u16][vlen u32][key][value]`.
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

/// `[prefix_len u16][prefix][sub i32][token_len u16][token][out_ptr u64]
/// [out_cap u32][fence_ptr u64][fence_cap u16]`: what changed under the
/// prefix from the token on, one bounded page (the contract's `CHANGES`).
///
/// The token says where: empty, a relist (every entry now, by key); eight
/// bytes, a bare revision `since`; else one this provider issued — `[T_LOG]
/// [rev][ord]` inside the change log, `[T_RELIST][pinned][cursor]` inside a
/// relist, `[T_HIST][since][to][cursor]` inside a history walk. A window the
/// change log answers is served from it; one it cannot (below its base: a
/// gap) from the store's history, a page at a time; past that, LOST.
fn ns_changes<B: Backend>(
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
    let Some(sub) = rd32(a, p).map(|s| s as i32) else {
        return EINVAL;
    };
    p += 4;
    let Some(token) = field16(a, &mut p).filter(|t| t.len() <= TOKEN_MAX) else {
        return EINVAL;
    };
    if a.len() != p + 22 {
        return EINVAL;
    }
    let (Some(out_ptr), Some(out_cap), Some(fence_ptr), Some(fence_cap)) =
        (rd64(a, p), rd32(a, p + 8), rd64(a, p + 12), rd16(a, p + 20))
    else {
        return EINVAL;
    };
    if out_ptr == 0 || fence_ptr == 0 || fence_cap < FENCE_WIRE_MAX {
        return EINVAL;
    }
    // SAFETY: the contract names `out_cap` writable bytes at `out_ptr`.
    let out = unsafe { caller_out(out_ptr, out_cap) };
    let sub = reader_sub(pv, b.caller(), sub);
    let rc = if !pv.opened || !pv.ring.attached() {
        EAGAIN
    } else {
        match token.len() {
            0 => relist_page(pv, b, bufs, prefix, pv.ring.frontier(), &[], out, strip),
            TOKEN_REV => {
                let since = rd64(token, 0).unwrap_or(0);
                from_revision(pv, b, bufs, prefix, since, ORD_END, sub, out, strip)
            }
            _ => match token[0] {
                T_LOG if token.len() == 13 => {
                    let rev = rd64(token, 1).unwrap_or(0);
                    let ord = rd32(token, 9).unwrap_or(0) as u32;
                    from_revision(pv, b, bufs, prefix, rev, ord, sub, out, strip)
                }
                T_RELIST if token.len() >= 9 => {
                    let pinned = rd64(token, 1).unwrap_or(0);
                    relist_page(pv, b, bufs, prefix, pinned, &token[9..], out, strip)
                }
                T_HIST if token.len() >= 17 => {
                    let since = rd64(token, 1).unwrap_or(0);
                    let to = rd64(token, 9).unwrap_or(0);
                    hist_page(
                        pv,
                        b,
                        bufs,
                        prefix,
                        since,
                        to,
                        &token[17..],
                        sub,
                        out,
                        strip,
                    )
                }
                _ => EINVAL,
            },
        }
    };
    if rc == EAGAIN {
        if let Some(id) = sub {
            pv.reg.defer(id);
        }
        return EAGAIN;
    }
    if rc >= 0 {
        // SAFETY: the contract names `fence_cap` bytes at `fence_ptr`.
        unsafe { write_fence(FENCE_VIEW_CONSISTENT, b.revision(), fence_ptr, fence_cap) };
    }
    rc
}

/// The subscription a `CHANGES` read names (`sub`, its handle), when it is
/// `owner`'s: a read refused for now rings it again once the cause clears.
fn reader_sub(pv: &Provider, owner: [u8; 8], sub: i32) -> Option<u32> {
    sub_id(slot_of(sub)).filter(|&id| pv.reg.sub(id).is_some_and(|s| s.owner == owner))
}

/// From `(rev, ord)` on: the change log when it answers; the store's history
/// when the window starts below its base; LOST past the frontier.
#[allow(
    clippy::too_many_arguments,
    reason = "one read's prefix, place, subscriber and page"
)]
fn from_revision<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    prefix: &[u8],
    rev: u64,
    ord: u32,
    sub: Option<u32>,
    out: &mut [u8],
    strip: usize,
) -> i32 {
    let f = pv.ring.frontier();
    if rev > f {
        // Not a point this provider ever stood at.
        return page_lost(out);
    }
    if pv.ring.answers(rev, ord) {
        return log_page(pv, b, bufs, prefix, rev, ord, out, strip);
    }
    // Below the base: a gap. The store's history may still hold the
    // window; a walk covers whole revisions, so one resumed inside `rev`
    // walks all of it again (reporting twice is harmless).
    let since = if ord == ORD_END {
        rev
    } else {
        rev.saturating_sub(1)
    };
    hist_page(pv, b, bufs, prefix, since, f, &[], sub, out, strip)
}

/// One page of the change log from `(rev, ord)`: a record for every entry
/// under the prefix, read at the store's current state — present, at the
/// entry that last wrote it (its `mod_rev`), with its value; absent, at the
/// entry that deleted it, as Deleted. An entry whose key a later write has
/// since changed is left to that write's entry, so no record's revision is
/// past what the page delivered. Never omitted, and applying them by key is
/// idempotent.
#[allow(
    clippy::too_many_arguments,
    reason = "one read's prefix, place and page"
)]
fn log_page<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    prefix: &[u8],
    rev: u64,
    ord: u32,
    out: &mut [u8],
    strip: usize,
) -> i32 {
    let Some(mut cur) = pv.ring.seek(rev, ord) else {
        return page_lost(out);
    };
    let Some(mut page) = PageOut::new(out, 13, strip) else {
        return ENOMEM;
    };
    let frontier = pv.ring.frontier();
    let mut key = [0u8; KEY_MAX];
    // The last entry consumed, and whether the page stopped before the log's
    // end (where the next read resumes).
    let mut resume: Option<(u64, u32)> = None;
    for _ in 0..LOG_SCAN_MAX {
        let before = cur;
        let (erev, eord, hint, kl) = match pv.ring.next(&mut cur) {
            Some(e) if e.rev <= frontier => {
                copy(&mut key, e.key);
                (e.rev, e.ord, e.hint, e.key.len())
            }
            _ => break,
        };
        let k = &key[..kl];
        if k.starts_with(prefix) {
            let fits = match read_value(b, bufs, k) {
                Err(e) => return e,
                Ok(Some((modr, at, len))) if modr == erev => {
                    let kind = if hint == KEY_CREATED {
                        KIND_ADDED
                    } else {
                        KIND_MODIFIED
                    };
                    let val = bufs.cache.get(at..at + len).unwrap_or(&[]);
                    page.push(modr, kind, k, val)
                }
                // Changed again since: that write's entry says it.
                Ok(Some(_)) => true,
                Ok(None) if hint == KEY_DELETED => page.push(erev, KIND_DELETED, k, &[]),
                // Absent, but this entry did not delete it: the entry that
                // did comes later.
                Ok(None) => true,
            };
            if !fits {
                if page.count == 0 {
                    return ENOMEM;
                }
                resume = Some((erev, eord));
                cur = before;
                break;
            }
        }
    }
    if resume.is_none() {
        // Stopped on the scan budget (or the end): resume at the next
        // unread entry, if there is one.
        let mut peek = cur;
        if let Some(e) = pv.ring.next(&mut peek) {
            if e.rev <= frontier {
                resume = Some((e.rev, e.ord));
            }
        }
    }
    match resume {
        Some((r, o)) => {
            // Delivered whole: every revision before the one it resumes in.
            let mut t = [0u8; 13];
            t[0] = T_LOG;
            copy(&mut t[1..9], &r.to_le_bytes());
            copy(&mut t[9..13], &o.to_le_bytes());
            page.finish(true, r.saturating_sub(1), &t)
        }
        None => page.finish(false, frontier, &frontier.to_le_bytes()),
    }
}

/// One page of a relist: every entry under the prefix now, in key order,
/// from just past `cursor`, as Added with its value. `pinned` is the log's
/// frontier when the relist began — the revision it resumes `CHANGES` from.
/// An entry written after `pinned` is passed over: the `CHANGES` from
/// `pinned` that follows the relist reports it (from the log, or across a
/// gap from history), so no relist record carries a `mod_rev` past the
/// revision the relist hands back.
#[allow(
    clippy::too_many_arguments,
    reason = "one read's prefix, place and page"
)]
fn relist_page<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    prefix: &[u8],
    pinned: u64,
    cursor: &[u8],
    out: &mut [u8],
    strip: usize,
) -> i32 {
    if !cursor.is_empty() && (!key_ok(cursor) || !cursor.starts_with(prefix)) {
        return EINVAL;
    }
    // Room for the shortest token; a record keeps room for the cursor that
    // would name it.
    let Some(mut page) = PageOut::new(out, TOKEN_REV, strip) else {
        return ENOMEM;
    };
    let mut end = [0u8; KEY_MAX + 1];
    let en = prefix_end(prefix, &mut end);
    // Strictly after the cursor.
    let mut from = [0u8; KEY_MAX + 1];
    let from_len = if cursor.is_empty() {
        copy(&mut from, prefix);
        prefix.len()
    } else {
        copy(&mut from, cursor);
        from[cursor.len()] = 0;
        cursor.len() + 1
    };
    // The last key the page holds, in the store's names: where the next
    // page resumes.
    let mut lastk = [0u8; KEY_MAX];
    let mut last_len = 0usize;
    let mut overflow = false;
    let read_rev = b.revision();
    let walked = current(
        b,
        bufs,
        &from[..from_len],
        &end[..en],
        read_rev,
        false,
        &mut |_, _, modr, key, val| {
            if key.len() > KEY_MAX {
                overflow = true;
                return false;
            }
            if modr > pinned {
                return true;
            }
            if !page.push_then(modr, KIND_ADDED, key, val, 9 + key.len()) {
                return false;
            }
            last_len = key.len();
            copy(&mut lastk, key);
            true
        },
    );
    let _ = pv;
    match walked {
        Ok(_) if overflow => EOVERFLOW,
        Ok(Walk::Stopped) if page.count == 0 => ENOMEM,
        Ok(Walk::Stopped) => {
            let mut t = [0u8; 9 + KEY_MAX];
            t[0] = T_RELIST;
            copy(&mut t[1..9], &pinned.to_le_bytes());
            copy(&mut t[9..9 + last_len], &lastk[..last_len]);
            // Nothing is delivered whole until the relist ends.
            page.finish(true, 0, &t[..9 + last_len])
        }
        Ok(_) => page.finish(false, pinned, &pinned.to_le_bytes()),
        Err(e) => e,
    }
}

/// One page of a history walk over `(since, to]`, key by key from `cursor`:
/// the store's versions say which keys changed; each is reported as the log
/// would (present, at its last write — or left to `CHANGES` from `to` when
/// written again since; absent, at its delete). Pages stop at a key boundary
/// within this call's and this step's walk budgets; the token moves to `to`
/// only when the walk is done.
#[allow(
    clippy::too_many_arguments,
    reason = "one read's prefix, window, subscriber and page"
)]
fn hist_page<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    prefix: &[u8],
    since: u64,
    to: u64,
    cursor: &[u8],
    sub: Option<u32>,
    out: &mut [u8],
    strip: usize,
) -> i32 {
    if !cursor.is_empty() && (!key_ok(cursor) || !cursor.starts_with(prefix)) {
        return EINVAL;
    }
    if b.history() > 0 && since < b.revision().saturating_sub(b.history()) {
        return page_lost(out);
    }
    // The step's walk budget, first claim to the subscribers refused last
    // step.
    let prio = sub
        .and_then(|id| pv.reg.sub(id))
        .is_some_and(|s| s.prio == pv.epoch && s.prio != 0);
    if pv.walk_left == 0 || (pv.prio_pending > 0 && !prio) {
        pv.m.walk_refused = pv.m.walk_refused.wrapping_add(1);
        return EAGAIN;
    }
    if prio {
        pv.prio_pending = pv.prio_pending.saturating_sub(1);
        if let Some(s) = sub.and_then(|id| pv.reg.sub_mut(id)) {
            s.prio = 0;
        }
    }
    let mut end = [0u8; KEY_MAX + 1];
    let en = prefix_end(prefix, &mut end);
    let start = if cursor.is_empty() { prefix } else { cursor };
    // Collect the window's keys (each with its latest version in it) into
    // the provider's scratch: a walk's page holds the cache, so their present
    // state is read only after it.
    let scratch = &mut pv.hist_keys;
    let mut used = 0usize;
    let mut keys = 0usize;
    let mut visits = 0u32;
    let budget = pv.walk_left.min(HIST_VISIT_MAX);
    let mut next_key = [0u8; KEY_MAX];
    let mut next_len = usize::MAX;
    let mut overflow = false;
    let walked = history(
        b,
        bufs,
        start,
        &end[..en],
        since,
        to,
        &mut |_, _, rev, kind, key, _| {
            visits += 1;
            if key.len() > KEY_MAX {
                overflow = true;
                return false;
            }
            // The same key as the last one noted: its newer version.
            let last_at = used.checked_sub(HIST_ENTRY_FIXED + KEY_MAX);
            if keys > 0 {
                if let Some(at) = last_at {
                    let kl = scratch[at] as usize;
                    if &scratch[at + 1..at + 1 + kl] == key {
                        // Its newest version in the window wins.
                        if rev >= rd64(scratch, at + 1 + KEY_MAX).unwrap_or(0) {
                            copy(
                                &mut scratch[at + 1 + KEY_MAX..at + 9 + KEY_MAX],
                                &rev.to_le_bytes(),
                            );
                            scratch[at + 9 + KEY_MAX] = kind;
                        }
                        return true;
                    }
                }
            }
            // A new key: stop here when a budget is spent, so the next page
            // starts at it whole.
            if visits > budget || used + HIST_ENTRY_FIXED + KEY_MAX > scratch.len() {
                next_len = key.len();
                copy(&mut next_key, key);
                return false;
            }
            scratch[used] = key.len() as u8;
            copy(&mut scratch[used + 1..used + 1 + key.len()], key);
            copy(
                &mut scratch[used + 1 + KEY_MAX..used + 9 + KEY_MAX],
                &rev.to_le_bytes(),
            );
            scratch[used + 9 + KEY_MAX] = kind;
            used += HIST_ENTRY_FIXED + KEY_MAX;
            keys += 1;
            true
        },
    );
    pv.walk_left = pv.walk_left.saturating_sub(visits);
    let done = match walked {
        // A key no token can name: the window cannot be paged past it.
        Ok(_) if overflow => return EOVERFLOW,
        Ok(Walk::Done) => true,
        Ok(Walk::Stopped) => false,
        Ok(Walk::Lost) => return page_lost(out),
        Err(e) => return e,
    };
    // Room for the shortest token; a record keeps room for a cursor naming
    // whichever key the page may end before once it is in — any key
    // gathered after it, or the one the walk stopped at — so a page that
    // holds a record can always end. `later[i]`: the longest of those for
    // record `i`.
    let Some(mut page) = PageOut::new(out, TOKEN_REV, strip) else {
        return ENOMEM;
    };
    let stride = HIST_ENTRY_FIXED + KEY_MAX;
    let mut later = [0u8; HIST_KEYS];
    let mut longest = if next_len == usize::MAX {
        0
    } else {
        next_len as u8
    };
    for (i, l) in later.iter_mut().enumerate().take(keys).rev() {
        *l = longest;
        longest = longest.max(pv.hist_keys.get(i * stride).copied().unwrap_or(0));
    }
    let mut resume_key = [0u8; KEY_MAX];
    let mut resume_len = usize::MAX;
    let mut key = [0u8; KEY_MAX];
    for i in 0..keys {
        let at = i * stride;
        // 0: no key follows, and a page that ends here ends the walk.
        let token_after = match later.get(i).map_or(KEY_MAX, |&l| usize::from(l)) {
            0 => 0,
            l => 17 + l,
        };
        let kl = pv.hist_keys[at] as usize;
        copy(&mut key, &pv.hist_keys[at + 1..at + 1 + kl]);
        let vrev = rd64(&pv.hist_keys, at + 1 + KEY_MAX).unwrap_or(0);
        let vkind = pv.hist_keys[at + 9 + KEY_MAX];
        let k = &key[..kl];
        let fits = match read_value(b, bufs, k) {
            Err(e) => return e,
            Ok(Some((modr, at, len))) if modr == vrev => {
                let val = bufs.cache.get(at..at + len).unwrap_or(&[]);
                page.push_then(modr, vkind, k, val, token_after)
            }
            Ok(Some(_)) => true,
            Ok(None) if vkind == KIND_DELETED => {
                page.push_then(vrev, KIND_DELETED, k, &[], token_after)
            }
            Ok(None) => true,
        };
        if !fits {
            if page.count == 0 {
                return ENOMEM;
            }
            resume_len = kl;
            copy(&mut resume_key, k);
            break;
        }
    }
    if resume_len == usize::MAX && !done && next_len != usize::MAX {
        resume_len = next_len;
        copy(&mut resume_key, &next_key[..next_len]);
    }
    if resume_len == usize::MAX {
        // The walk is done: the window is delivered whole, and the next read
        // goes on from `to`.
        return page.finish(false, to, &to.to_le_bytes());
    }
    let mut t = [0u8; 17 + KEY_MAX];
    t[0] = T_HIST;
    copy(&mut t[1..9], &since.to_le_bytes());
    copy(&mut t[9..17], &to.to_le_bytes());
    copy(&mut t[17..17 + resume_len], &resume_key[..resume_len]);
    page.finish(true, since, &t[..17 + resume_len])
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
    /// This replica's chunk applies the store refused (telemetry).
    chunk_refused: u32,
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
    chunk_refused: 0,
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
    if pv.rep.superseded {
        return ERROR;
    }
    let caller = b.caller();
    let id = identity(pv, b, caller, w);
    let rep = &mut pv.rep;
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

/// Write request `w`'s identity ([`write_id`]), computed once per request:
/// an identical re-ask — the same caller, operation, precondition, key and
/// value length, and a value with the same checksum — is the request its
/// caller's [`Ident`] already names, and its identity is not hashed again
/// (SHA-256 over a 1 MiB value on every re-ask would keep a loaded store's
/// callers waiting). The checksum is two 64-bit lanes keyed per provider:
/// it tells a re-ask from a caller's buffer reused for a different value,
/// the one case the exact fields cannot. A request not matched is hashed
/// whole and replaces its caller's (or the least recently asked) entry.
fn identity<B: Backend>(pv: &mut Provider, b: &mut B, caller: [u8; 8], w: &WriteArgs<'_>) -> Hash {
    let value = w.value.unwrap_or(&[]);
    let check = checksum(pv.ident_seed, value);
    let mark = w.mark.unwrap_or([0; IMPORT_MARK_LEN]);
    let same = |x: &Ident| {
        x.used
            && x.caller == caller
            && x.put == w.value.is_some()
            && x.pre == w.pre
            && x.rev == w.rev
            && x.import == w.mark.is_some()
            && x.mark == mark
            && x.len == value.len() as u64
            && x.check == check
            && x.key.get(..x.key_len as usize) == Some(w.key)
    };
    if let Some(k) = pv.idents.position(|x| same(x)) {
        if let Some(x) = pv.idents.get_mut(k) {
            x.idle = 0;
            return x.id;
        }
    }
    let id = write_id(&caller, w);
    pv.m_ident_hashed = pv.m_ident_hashed.wrapping_add(1);
    if w.key.len() <= KEY_MAX {
        let k = match pv.idents.position(|x| x.used && x.caller == caller) {
            Some(k) => Some(k),
            None => {
                let mut alloc = |n: usize| b.alloc(n);
                pv.idents
                    .take(&mut alloc, NO_IDENT, |x| x.used)
                    // The heap holds no more: the longest unasked goes.
                    .or_else(|| {
                        (0..pv.idents.len()).max_by_key(|&k| pv.idents.get(k).map_or(0, |x| x.idle))
                    })
            }
        };
        if let Some(x) = k.and_then(|k| pv.idents.get_mut(k)) {
            *x = Ident {
                used: true,
                caller,
                put: w.value.is_some(),
                pre: w.pre,
                rev: w.rev,
                import: w.mark.is_some(),
                mark,
                key: [0; KEY_MAX],
                key_len: w.key.len() as u8,
                len: value.len() as u64,
                check,
                idle: 0,
                id,
            };
            copy(&mut x.key, w.key);
        }
    }
    id
}

/// A keyed two-lane checksum of `v`, word at a time: tens of times cheaper
/// than SHA-256, and only ever compared within one caller's requests of one
/// key and length (see [`identity`]).
fn checksum(seed: u64, v: &[u8]) -> [u64; 2] {
    const K0: u64 = 0x9E37_79B9_7F4A_7C15;
    const K1: u64 = 0xBF58_476D_1CE4_E5B9;
    const K2: u64 = 0x94D0_49BB_1331_11EB;
    let mut a = seed ^ K0;
    let mut c = seed.rotate_left(32) ^ K2;
    let mut words = v.chunks_exact(8);
    for word in &mut words {
        let x = u64::from_le_bytes(<[u8; 8]>::try_from(word).unwrap_or([0; 8]));
        a = (a ^ x).wrapping_mul(K1);
        a ^= a >> 31;
        c = c.wrapping_add(x).rotate_left(27).wrapping_mul(K0);
    }
    let mut tail = [0u8; 8];
    copy(&mut tail, words.remainder());
    let x = u64::from_le_bytes(tail) ^ ((v.len() as u64) << 3);
    a = (a ^ x).wrapping_mul(K1);
    c = c.wrapping_add(x).rotate_left(27).wrapping_mul(K0);
    a ^= a >> 33;
    c ^= c >> 29;
    [a.wrapping_mul(K2), c.wrapping_mul(K1)]
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
            copy(x.body.get_mut(..IMPORT_MARK_LEN)?, &m);
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
        origin::Applied::Chunk { replica, .. } if replica == rep.replica => {
            rep.chunk_refused = rep.chunk_refused.wrapping_add(1);
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
/// not — a subscription (it only registers and rings), a take, a close,
/// the caps — is served while the store is still recovering, so a consumer
/// that subscribes at start is registered, not refused (it would not ask
/// again, and would never be woken), and reads once the store serves.
///
/// # Safety
/// `arg` names `arg_len` bytes (or is null).
pub unsafe fn needs_store(op: u32, _arg: *const u8, _arg_len: usize) -> bool {
    !matches!(
        op,
        OBJ_CLOSE | NS_CLOSE | NS_CAPS | NS_SUBSCRIBE | NS_TAKE_RUNG
    )
}

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

/// A contract call, answered in the call. `handle` arrives with its tag
/// stripped.
///
/// # Safety
/// `arg` names `arg_len` bytes; the pointers the arguments carry name
/// what the contract says they do (the caller's buffers).
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
    // Injected read waits (test only): answered before anything moves, or
    // met by the read in the store itself.
    match pv.fault_read(op, a) {
        Injected::No => serve(pv, b, bufs, handle, op, a, strip, arg, arg_len),
        Injected::AtDispatch => {
            // Nothing moves, but what the contract asks of a waiting read:
            // the subscription a CHANGES names is rung again once the step
            // clears, as a store that waits rings it.
            if op == NS_CHANGES {
                let mut p = 0usize;
                let sub = field16(a, &mut p).and_then(|_| rd32(a, p));
                if let Some(id) = sub.and_then(|s| reader_sub(pv, b.caller(), s as i32)) {
                    pv.reg.defer(id);
                }
            }
            EAGAIN
        }
        Injected::InStore => serve(
            pv,
            &mut StoreBusy(b),
            bufs,
            handle,
            op,
            a,
            strip,
            arg,
            arg_len,
        ),
    }
}

/// TEST ONLY: a store whose device makes every op wait, as the worker's
/// answers then — `KV_RESULT_INTERNAL`, nothing applied, and
/// [`Backend::busy`] set (see [`Fault`]).
struct StoreBusy<'b, B: Backend>(&'b mut B);

impl<B: Backend> Backend for StoreBusy<'_, B> {
    fn apply(&mut self, _op: u8, _body: &[u8], _out: &mut [u8]) -> (u8, usize) {
        (KV_RESULT_INTERNAL, 0)
    }
    fn revision(&self) -> u64 {
        self.0.revision()
    }
    fn history(&self) -> u64 {
        self.0.history()
    }
    fn durable(&self) -> bool {
        self.0.durable()
    }
    fn busy(&self) -> bool {
        true
    }
    fn log_pending(&mut self) -> bool {
        self.0.log_pending()
    }
    fn write(&mut self, chan: i32, bytes: &[u8]) -> i32 {
        self.0.write(chan, bytes)
    }
    fn replicated(&self) -> bool {
        self.0.replicated()
    }
    fn propose(&mut self, op: u8, corr: u64, parts: &[&[u8]]) -> bool {
        self.0.propose(op, corr, parts)
    }
    fn caller(&self) -> [u8; 8] {
        self.0.caller()
    }
    fn group(&self) -> Group {
        self.0.group()
    }
    fn module(&self) -> Option<[u8; 8]> {
        self.0.module()
    }
    fn module_live(&self, module: [u8; 8]) -> bool {
        self.0.module_live(module)
    }
    fn event_owner(&self, event: i32) -> Option<[u8; 8]> {
        self.0.event_owner(event)
    }
    fn signal(&mut self, event: i32) -> i32 {
        self.0.signal(event)
    }
    fn alloc(&mut self, bytes: usize) -> *mut u8 {
        self.0.alloc(bytes)
    }
    fn take_keys<F: FnMut(u64, u8, &[u8])>(&mut self, f: F) -> Option<(bool, u64)> {
        self.0.take_keys(f)
    }
    fn ledger_deny(&mut self) {
        self.0.ledger_deny()
    }
}

/// A call's raw argument bytes.
///
/// # Safety
/// `arg` names `arg_len` bytes (or is null).
unsafe fn raw_arg<'a>(arg: *mut u8, arg_len: usize) -> &'a [u8] {
    if arg.is_null() {
        &[]
    } else {
        core::slice::from_raw_parts(arg as *const u8, arg_len)
    }
}

/// Serve one call, its argument in the store's names (`strip`: the scope
/// head an answer's names lose).
#[allow(
    clippy::too_many_arguments,
    reason = "dispatch's own arguments, handed on whole to the store it serves through"
)]
unsafe fn serve<B: Backend>(
    pv: &mut Provider,
    b: &mut B,
    bufs: &mut Bufs<'_>,
    handle: i32,
    op: u32,
    a: &[u8],
    strip: usize,
    arg: *mut u8,
    arg_len: usize,
) -> i32 {
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
            let caller = b.caller();
            let module = b.module();
            let sub = sub_id(slot).and_then(|id| pv.reg.sub(id));
            let read = pv
                .slots
                .get(slot)
                .filter(|s| s.opened_by(caller))
                .map(|s| s.rev);
            let rev = if let Some(rev) = read {
                rev
            } else if sub.is_some_and(|s| s.owner == caller && Some(s.module) == module) {
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
            let caller = b.caller();
            if let Some(s) = pv.slots.get_mut(slot).filter(|s| s.opened_by(caller)) {
                s.used = false;
                if pv.cached == (slot + 1) as u32 {
                    pv.cached = 0;
                }
            }
            0
        }
        NS_CAPS => NS_CAPS_SERVED,
        NS_LIST => {
            let rc = ns_list(b, bufs, a);
            if rc <= 0 || strip == 0 {
                return rc;
            }
            // `[prefix][cursor][out_ptr u64][out_cap u32]…`
            let mut p = 0usize;
            let _ = field16(a, &mut p);
            let _ = field16(a, &mut p);
            let (Some(out_ptr), Some(out_cap)) = (rd64(a, p), rd32(a, p + 8)) else {
                return rc;
            };
            let out = caller_out(out_ptr, out_cap);
            strip_list(out, rc as usize, strip) as i32
        }
        // A page's keys are written in the graph's names as it is built.
        NS_CHANGES => ns_changes(pv, b, bufs, a, strip),
        NS_SUBSCRIBE => ns_subscribe(pv, b, a, strip),
        NS_TAKE_RUNG => ns_take_rung(pv, b, raw_arg(arg, arg_len)),
        NS_CLOSE => {
            let (caller, module) = (b.caller(), b.module());
            if let Some(id) = sub_id(slot) {
                if pv
                    .reg
                    .sub(id)
                    .is_some_and(|s| s.owner == caller && Some(s.module) == module)
                {
                    pv.reg.remove_sub(id);
                }
            }
            0
        }
        _ => ENOSYS,
    }
}
