//! kv_request_router — adapter-neutral KV ingress router.
//!
//! ## Responsibilities
//!
//! - Drain MSG_KV_REQUEST envelopes from every adapter ingress port
//!   (etcd_in, redis_in, memcached_in, memcached_udp_in).
//! - Validate routing inputs (Phase 1: single KPG, no quota/auth/CP
//!   freshness gating; those land in Phase 5 when their producer
//!   modules become real).
//! - Translate to MSG_KV_COMMAND and emit on `kv_out`. Remember
//!   `corr_id → protocol` in a small inflight table so the response
//!   path knows which adapter output to send the eventual reply on.
//! - Drain MSG_KV_APPLIED from `kv_in`, translate to MSG_KV_RESPONSE,
//!   send to the right protocol output port.
//!
//! ## Phase 1 scope
//!
//! Single KPG (kpg_id = 0). No route-plan resolution, no quota
//! gating, no CP freshness check. The router is the only Phase 1
//! module that needs to track per-corr_id state (the protocol →
//! output mapping); other modules either preserve corr_id verbatim
//! (worker) or extract conn_id directly from the envelope (anchor).

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    clippy::duplicate_mod,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, ABI fns carry a fixed arity, and the PIC build #[path]-remounts shared SDK/common code"
)]
#![allow(
    clippy::manual_memcpy,
    clippy::needless_range_loop,
    reason = "hand-written index loops build wire envelopes byte-by-byte throughout these modules; the explicit form is the module idiom"
)]
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/collections.rs"]
mod collections;

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[path = "../../common/partition_map.rs"]
mod partition_map;

// For cross-partition transaction helping only: the home record's
// decoder and status. Contracts, no I/O — the same reason the
// executor mounts sql_exec.
#[allow(
    clippy::duplicate_mod,
    reason = "txn.rs mounts db_context/internal_key/mvcc beneath it; other mounts in this module's tree carry their own copies, and nothing passes values between the trees"
)]
#[path = "../../common/txn.rs"]
mod txn;

use collections::InflightTable;
use partition_map::{
    HashSlotMap, KeyspaceRouting, OrderedRangeMap, RoutingKind, SlotBinding, MSG_MAP_UPDATE,
    MSG_RANGE_MAP_UPDATE,
};
use types::{
    KvEpoch, RoutingEpoch, KV_OP_RANGE_SCAN, KV_OP_SCAN_VERSIONS, KV_RESULT_CROSS_RANGE,
    KV_RESULT_DIRTY_EPOCH, KV_RESULT_INTERNAL, KV_RESULT_LIN_BOUND, KV_RESULT_RANGE, PROTO_ETCD,
    PROTO_INTERNAL_SQL, PROTO_INTERNAL_WATCH, PROTO_MEMCACHED, PROTO_MEMCACHED_UDP, PROTO_REDIS,
    PROTO_UNKNOWN, REQ_LINEARIZABLE,
};
use wire::{
    CLIENT_REJECT_NOT_LEADER, LATTICE_ENTRY_TAG, MSG_APP_APPLIED_POS, MSG_CLIENT_PROPOSAL,
    MSG_CLIENT_READ_REQUEST, MSG_CLIENT_READ_RESPONSE, MSG_CLIENT_REJECT,
    MSG_CLIENT_REJECT_INTERNAL, MSG_KV_APPLIED, MSG_KV_COMMAND, MSG_KV_REQUEST, MSG_KV_RESPONSE,
};

const SCRATCH_BUF_SIZE: usize = 4096 + 64;

/// Inflight table capacity. Tunable later via params.
const MAX_INFLIGHT: usize = 256;

/// Phase 1 hardcodes one KPG. Multi-KPG routing lands when
/// control_plane exposes route plans.
const KPG_ID_DEFAULT: u16 = 0;

// ── Phase-3 ROUTING slice: partition-map fencing (RFC §11.2/§11.4, §20)
//
// `routing_mode = 1` layers the §11 METADATA + FENCING contract over
// the existing single-range runtime: every request resolves through the
// router's `HashSlotMap` (the declared `RoutingKind::HashSlot` form —
// never inferred from key bytes) and is fenced on binding generation +
// routing epoch. A mismatch is a stale route (`RetryClass::StaleRoute`)
// and takes the router's existing reject-synth path as
// KV_RESULT_DIRTY_EPOCH (redis: `MOVED routing epoch advanced`).
// Mode 0 (default) is the legacy hash-routing path, byte-identical.

/// Router mode 0: legacy hash routing (no map lookup, KPG_ID_DEFAULT).
const ROUTING_MODE_LEGACY: u8 = 0;
/// Router mode 1: every request fenced through the partition map.
const ROUTING_MODE_FENCED: u8 = 1;
/// Router mode 2: ORDERED multi-range routing (Phase 3, RFC §11.2/§20).
/// Every request resolves through the `OrderedRangeMap` by its KEY
/// BYTES (never a hash — the map form is declared, §11.2), the owning
/// range's partition selects the worker/proposal PORT PAIR, and a
/// `RANGE_SCAN` spanning several ranges is stitched gap-free across
/// them by tagging the owning range index and a routing-epoch stamp
/// into the continuation cursor (see `partition_map.rs`, scan-cursor
/// tagging). Requires the `range_map` param (the static map — this is
/// supervisor-free provisioning; the Phase 4 supervisor later
/// publishes `MSG_RANGE_MAP_UPDATE` frames onto `map_update`).
const ROUTING_MODE_ORDERED: u8 = 2;

/// How many partitions the ordered mode can address: partition `p`
/// uses the `p`th worker/proposal port pair. A map binding a partition
/// at or above this refuses to load — a range nobody can reach must
/// fail at init, not at first use.
///
/// Two, not more: fluxor caps a module at 16 output ports and the
/// router spends 15 (per-protocol replies, consensus, fences, the
/// lifecycle pair). Wider fan-out is §11.6 dense-hosting territory —
/// partition-multiplexed ports, not one static pair per range.
const MAX_PARTITION_PORTS: usize = 2;
/// Ranges whose write counts are tracked individually. Beyond this the
/// counts are simply not attributed — dropping the attribution is
/// honest, whereas folding the tail into range 0 would report a hot
/// range that does not exist.
const MAX_TRACKED_RANGES: usize = 8;
/// Heavy-hitter slots per tracked range (§12.4's hot-KEY clause).
///
/// ## KNOWN BLIND SPOT — read before trusting this signal
///
/// Only ops with a SINGLE extractable key are sampled: `op_key_bytes`
/// yields nothing for a multi-op `KV_OP_TXN`, and in lattice a txn
/// carries most real writes (a SQL row plus its index entries, a graph
/// edge pair, a document insert, a wide-column row). On a txn-heavy
/// workload the busiest-key share is therefore an UNDER-estimate.
///
/// That is the dangerous direction, and it was demonstrated live
/// rather than reasoned about: 400 writes driven at one key produced
/// no hot-key report at all, and the advisor recommended a SPLIT for a
/// range whose load a split cannot relieve — the precise mistake
/// §12.4 names. Space-Saving's own error is an over-estimate and safe;
/// this sampling gap sits in front of it and reverses the sign.
///
/// Attributing a multi-key txn to one of its keys is NOT the fix: that
/// invents concentration that does not exist and makes the recommender
/// refuse splits on a fiction. The fix is to sample every key a txn
/// touches, which needs the txn body walked at dispatch.
///
/// Space-Saving with a tiny K. The algorithm's guarantee is the reason
/// it is safe here: an evicted key's count is INHERITED by its
/// replacement, so every slot's count is an OVER-estimate of the true
/// count, never an under-estimate. Over-estimating concentration can
/// only make the advisor report `HotKeyNotSplittable` — a REFUSAL to
/// recommend a split — so the error direction makes the recommender
/// quieter, which is the discipline every §12.4 knob follows. An
/// under-estimate would have been the dangerous direction: it would
/// let a genuinely unsplittable hot key be recommended for a split.
const HOT_KEY_SLOTS: usize = 4;

/// Upper bound on the `range_map` param blob (an encoded
/// `RANGE_MAP_UPDATE_V1` frame). Sized for a handful of statically
/// provisioned ranges with realistic key bounds, not for MAX_RANGES ×
/// max-bound descriptors — a static YAML map of that size would be a
/// design smell, not a use case.
const RANGE_MAP_PARAM_MAX: usize = 2048;

/// Initial map generation of the static single-partition default map
/// (every slot → partition 0, this generation).
const MAP_GENERATION_DEFAULT: u32 = 1;
/// Initial routing epoch. Mirrors `KV_EPOCH_DEFAULT`'s convention for
/// the sibling `types::RoutingEpoch` axis (see `replica_facade::
/// route_decision`, which fences the same pair of epochs).
const ROUTING_EPOCH_DEFAULT: RoutingEpoch = 1;

/// Slot presented for wildcard ops (multi-key / range / whole-store):
/// in the single-partition reality every slot carries an identical
/// binding, so slot 0 stands for "the map as a whole". Multi-range
/// wildcard split is future §20 work (scan descriptor resolution).
const WILDCARD_SLOT: u16 = 0;

/// Static single-partition default binding: every slot → partition 0.
const SLOT_BINDING_DEFAULT: SlotBinding = SlotBinding {
    partition_id: KPG_ID_DEFAULT,
    partition_incarnation: 0,
    generation: MAP_GENERATION_DEFAULT,
};

/// Linearizable reads stashed while their ReadIndex fence is in
/// flight. Sized for the fence RTT (probe → apply-horizon, a few
/// ticks) at bench rates; a full table degrades the read to the
/// snapshot path rather than blocking.
const LIN_READ_SLOTS: usize = 32;
/// Stashed MSG_KV_COMMAND envelope cap (3-byte header + 27-byte head,
/// identity included + key body). A GET larger than this degrades to
/// the snapshot path.
const LIN_READ_BUF: usize = 300;

/// Fenced-read stash lifecycle. `AWAITING` until consensus answers;
/// `RELEASED`/`REJECTED` slots retry their (kv_out forward | LIN_BOUND
/// error synth) across steps under backpressure, and only a completed
/// retry decrements the conn's fence count (session order depends on the
/// fenced read reaching the worker BEFORE any held op replays).
const LIN_FREE: u8 = 0;
const LIN_AWAITING: u8 = 1;
const LIN_RELEASED: u8 = 2;
const LIN_REJECTED: u8 = 3;
/// Consensus granted the read, but the worker has not yet applied
/// through the fence index. This state is the applied-index half of
/// RFC §4.3's fence: a grant proves CONSENSUS applied through
/// `required_commit`, and the store the read queries is a separate
/// module fed over a channel. Releasing on the grant alone lets a
/// linearizable read observe a false absence — the write is committed
/// and durable, and simply has not reached this store yet. A slot sits
/// here until the worker's reported applied index catches up, or until
/// its deadline expires and it fails closed like any other unsatisfied
/// fence.
const LIN_FENCED: u8 = 4;

#[repr(C)]
#[derive(Clone, Copy)]
struct PendingLinRead {
    corr_id: u64,
    /// Submission stamp — resolved slots complete in THIS order, never
    /// slot-scan order (slot reuse would otherwise let a later fenced
    /// read's release overtake an earlier one's).
    seq: u32,
    /// Total envelope bytes staged in `buf`. 0 = slot free.
    len: u16,
    state: u8,
    /// Originating protocol + conn — needed to settle the right conn's
    /// ordering accounting and to route a fail-closed error on reject.
    proto: u8,
    conn: u8,
    _pad: u8,
    /// The partition this read is fenced against — its applied index is
    /// the one `LIN_FENCED` waits on. Set when the read routes.
    fence_partition: u16,
    /// Wall-clock stash deadline: an AWAITING slot older than this is
    /// treated as REJECTED (fail closed). Covers the residue where
    /// consensus's reject for an evicted/lost read never arrived —
    /// without it the slot (and its conn's ordering barrier) leaked
    /// forever. Set past apply's READ_TIMEOUT_MS so the normal reject
    /// always wins when it does arrive.
    deadline_ms: u64,
    /// Raft index this read linearizes at, as named by consensus in its
    /// grant. Meaningful only in `LIN_FENCED`. The worker must report an
    /// applied index at least this high before the read may be served.
    fence_index: u64,
    buf: [u8; LIN_READ_BUF],
}

impl PendingLinRead {
    const fn empty() -> Self {
        PendingLinRead {
            corr_id: 0,
            seq: 0,
            len: 0,
            state: LIN_FREE,
            proto: 0,
            conn: 0,
            _pad: 0,
            fence_partition: 0,
            deadline_ms: 0,
            fence_index: 0,
            buf: [0; LIN_READ_BUF],
        }
    }
}

/// Stash-slot deadline: apply's READ_TIMEOUT_MS is 5 s; anything still
/// AWAITING at 6.5 s lost its reject and must fail closed locally.
const LIN_STASH_TIMEOUT_MS: u64 = 6_500;

/// Session-order hold pool: while a conn has a fence in flight, every
/// subsequent request from that conn is parked here RAW and replayed
/// through the normal dispatch once the fence resolves. This preserves
/// worker arrival order — hence per-conn response order — without any
/// response buffering, and it is the SEMANTICALLY required behaviour:
/// ops after a linearizable read on a session must observe state ≥ the
/// read's fence, so they cannot be allowed to overtake it. Cross-conn
/// traffic is unaffected; a conn pays the hold only while ITS fence is
/// pending. Pool overflow fails closed (LIN_BOUND error), never
/// reorders.
const HOLD_SLOTS: usize = 48;
/// Held-request byte cap. Full-size requests (4 KiB SETs) are rare
/// inside a barrier window; an oversize hold fails closed (LIN_BOUND)
/// rather than growing the pool 4×.
const HOLD_BUF: usize = 1088;

#[repr(C)]
#[derive(Clone, Copy)]
struct HeldRequest {
    /// Global arrival stamp — replay order within a conn. 0 = free.
    seq: u32,
    len: u16,
    proto: u8,
    conn: u8,
    buf: [u8; HOLD_BUF],
}

impl HeldRequest {
    const fn empty() -> Self {
        HeldRequest {
            seq: 0,
            len: 0,
            proto: 0,
            conn: 0,
            buf: [0; HOLD_BUF],
        }
    }
}

/// Dense (protocol, conn) key space for the per-conn ordering state:
/// 6 protocols × 256 conn ids. Four client protocols plus the two
/// internal callers, which need ordering state for the same reason a
/// client does — their requests pipeline and must not overtake their
/// own earlier ones across a path transition.
const FENCE_KEYS: usize = 6 * 256;

/// Dispatch paths. Each path is internally FIFO (worker arrival order,
/// consensus commit order, one-fence-at-a-time), so per-conn response
/// order is broken ONLY at path transitions — which is exactly what the
/// barrier serializes. Same-path ops pipeline freely.
/// The durability class a reply may claim, given the path its request
/// took. §21 invariant 2: only an authority that observed the proof may
/// claim the class, so this returns the strongest SOUND answer and not
/// the most flattering one.
///
/// A consensus-path write reached the worker through the apply bridge,
/// which means its entry was committed — replicated to a quorum. That
/// is `ReplicatedVolatile` exactly. `ReplicatedDurable` needs the fsync
/// proof, which lives in clustor's durability module and is not
/// something this router ever sees; claiming it here would be an
/// assertion nothing in this process can support.
///
/// Everything else is `Volatile`. A read makes nothing durable, and a
/// direct-path write is a single-node acknowledgement.
const fn durability_for_path(path: u8) -> u8 {
    match path {
        PATH_CONSENSUS => 0x02, // Durability::ReplicatedVolatile
        _ => 0x01,              // Durability::Volatile
    }
}

/// `kv_store::KS_TXN_RECORD`, inlined. **Byte-identical by
/// requirement** — the router must not mount the whole state machine
/// for one keyspace id; `tests/contract_txn_map_placement.rs` pins the
/// agreement so the duplication cannot drift.
const KS_TXN_RECORD: u32 = 0x8009_0002;

/// Longest blocking key the router will stash for helping. Longer keys
/// simply are not helped; their intents still resolve via the
/// coordinator or a same-partition reader.
const HELP_KEY_MAX: usize = 64;

/// Concurrent cross-partition helps. Small on purpose: helping is a
/// LIVENESS assist for coordinator death, not a throughput path, and
/// each help is two extra KV round trips.
const HELP_SLOTS: usize = 8;

const HELP_PHASE_GET_HOME: u8 = 1;
const HELP_PHASE_RESOLVING: u8 = 2;

const PATH_DIRECT: u8 = 1;
const PATH_CONSENSUS: u8 = 2;
const PATH_FENCE: u8 = 3;

/// `dispatch_parsed` outcomes.
const DISPATCH_OK: i32 = 1;
const DISPATCH_DROPPED: i32 = 0;
/// The op would change dispatch path while same-conn ops are still
/// outstanding on another path (or older ops are already held) —
/// caller must park it for ordered replay.
const DISPATCH_BLOCKED: i32 = -1;

/// Per-corr bookkeeping for the response path: output port selection
/// (proto) + per-conn ordering accounting (conn, path) + the per-key
/// dependency key (task #17) so the barrier can tell whether a later
/// same-conn op on a different path actually conflicts.
#[derive(Clone, Copy)]
struct InflightMeta {
    proto: u8,
    conn: u8,
    path: u8,
    /// FNV-1a hash of the op's key. Only meaningful when `wildcard` is
    /// false. Two ops on the same conn+path never conflict (each path is
    /// FIFO); across paths they conflict iff they touch a common key or
    /// either is a wildcard.
    key_hash: u64,
    /// The op touches an unknown / multi / whole-store key set
    /// (MSET, SCAN/RANGE, TXN, FLUSH, multi-key DELETE/MGET) — treat as
    /// conflicting with every other op on the conn, conservatively.
    wildcard: bool,
    /// Ordered-mode RANGE_SCAN continuation bookkeeping. `scan` marks a
    /// response whose cursor must be rewritten; `scan_range` is the
    /// range index the sub-scan ran in; `scan_next` is the pre-computed
    /// continuation to substitute when the worker reports its sub-span
    /// exhausted (0 = the sub-span was the last covering range, so
    /// exhausted IS complete).
    scan: bool,
    scan_range: u8,
    /// Low byte of the routing epoch the scan was ROUTED under — the
    /// stamp minted into a mid-range continuation. Stamping at route
    /// time (not response time) is what makes a map change mid-flight
    /// retire the continuation instead of re-tagging it as fresh.
    scan_epoch: u8,
    scan_next: u64,
    /// The op's single provable key, kept so a TXN_PENDING reply can
    /// start a cross-partition help — the reply carries the txn id and
    /// home range, but not the key whose intent blocked it, and by
    /// reply time the request body is gone. Zero-length when the op has
    /// no single key or it exceeds HELP_KEY_MAX; such ops are simply
    /// not helped.
    help_key: [u8; HELP_KEY_MAX],
    help_key_len: u8,
    /// Canonical identity (§23) the op was issued under, kept so a
    /// cross-partition help resolves the intent under the SAME
    /// `(tenant, database, keyspace)` — otherwise the courier's writes
    /// would land under a different tenant and the read that triggered
    /// the help would never see the resolution.
    id_tenant: u32,
    id_database: u32,
    id_keyspace: u32,
    /// Wall-clock ms after which this op is answered fail-closed.
    ///
    /// Without it a request whose completion signal never arrives —
    /// most importantly a write proposed on a node that is NOT the
    /// leader — sits here forever and the caller waits forever. A
    /// follower accepting a connection and then saying nothing is
    /// §28's rejected transparent FIFO: the caller cannot tell "not
    /// the leader" from "still working". See `sweep_inflight_deadlines`.
    deadline_ms: u64,
}

/// True for protocols whose anchor keeps a per-conn response reorder
/// buffer (redis, task #17). For these the router may let independent
/// same-conn ops pipeline across paths — the anchor restores FIFO. Other
/// protocols keep the conservative all-transition barrier, since their
/// anchors assume responses arrive in issue order.
#[inline]
fn proto_reorders(proto: u8) -> bool {
    proto == PROTO_REDIS
}

/// FNV-1a 64-bit (matches `kv_store::fnv1a64`). The router only compares
/// hashes it computed itself, so any deterministic hash is sound; using
/// the same one keeps the scheme consistent across the codebase.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// True for every op that mutates the keyspace — the set that must
/// travel the CONSENSUS write path (when `proposal_out` is wired) so it
/// replicates, rather than the read path. Op codes per
/// `modules/common/types.rs`. TXN counts as a write because its
/// then/else branches can mutate; classifying it read-side would skip
/// replication of a committed txn.
#[inline]
fn op_is_write(op: u8) -> bool {
    matches!(
        op,
        0x02 /*PUT*/    | 0x03 /*DELETE*/ | 0x05 /*TXN*/   | 0x06 /*CAS*/
        | 0x07 /*INCR*/ | 0x08 /*DECR*/   | 0x09 /*APPEND*/ | 0x0C /*FLUSH*/
        | 0x0E /*MSET*/ | 0x10 /*PREPEND*/
        // Cross-range transaction ops (§13). All three MUTATE: prepare
        // stages intents, resolve publishes or drops them, and the
        // record write is the decision itself. Omitting them here sent
        // them down the read path, where on a replicated graph they
        // were never proposed and so did nothing at all — while the
        // coordinator dutifully retried a write that could not land.
        | 0x16 /*TXN_PREPARE*/ | 0x17 /*TXN_RESOLVE*/ | 0x18 /*TXN_RECORD*/
        // Exactly-once execution (§14). Classified as a write even
        // though a REDELIVERY performs no mutation: the classification
        // has to be a property of the op, not of what this particular
        // delivery turns out to do, and the first delivery mutates.
        // Routing a redelivery down the read path would also skip
        // consensus for the one caller most likely to be retrying
        // after a failure.
        | 0x19 /*IDEMPOTENT*/
    )
}

/// Extract the dependency key of an op from its MSG_KV_REQUEST body.
/// Returns `(key_hash, wildcard)`. Single-key ops hash their one key;
/// multi/range/whole-store ops (and anything unrecognised) are wildcards
/// that conflict with every same-conn op across paths. Body layouts are
/// documented in `modules/common/types.rs`.
fn op_key(op: u8, body: &[u8]) -> (u64, bool) {
    // Read a `[key_len:u16 LE][key…]` at `off`; None if malformed.
    let read_key_at = |b: &[u8], off: usize| -> Option<u64> {
        if b.len() < off + 2 {
            return None;
        }
        let klen = u16::from_le_bytes([b[off], b[off + 1]]) as usize;
        if b.len() < off + 2 + klen {
            return None;
        }
        Some(fnv1a64(&b[off + 2..off + 2 + klen]))
    };
    // Op codes (see modules/common/types.rs). GET/PUT/DELETE known here
    // already (0x01/0x02/0x03); the rest per that file.
    match op {
        // Key-first single-key ops: [key_len][key] …
        0x01 /*GET*/ | 0x02 /*PUT*/ | 0x06 /*CAS*/ | 0x07 /*INCR*/
        | 0x08 /*DECR*/ | 0x09 /*APPEND*/ | 0x0A /*EXISTS*/
        | 0x0F /*STRLEN*/ | 0x10 /*PREPEND*/ => match read_key_at(body, 0) {
            Some(h) => (h, false),
            None => (0, true),
        },
        // [key_count][keys…]: extract iff exactly one key, else wildcard.
        0x03 /*DELETE*/ | 0x0D /*MGET*/ => {
            if body.len() >= 2 && u16::from_le_bytes([body[0], body[1]]) == 1 {
                match read_key_at(body, 2) {
                    Some(h) => (h, false),
                    None => (0, true),
                }
            } else {
                (0, true)
            }
        }
        // MSET (0x0E), SCAN/RANGE (0x0B/0x04), RANGE_SCAN (0x13),
        // TXN (0x05), FLUSH (0x0C), and anything unknown →
        // conservative wildcard. RANGE_SCAN touches a whole span, so a
        // single dependency key could not represent it even in
        // principle: it conflicts with any concurrent write on the same
        // connection, which is exactly what a wildcard says.
        _ => (0, true),
    }
}

/// Ordered-mode rewrite buffer: a `RANGE_SCAN` body whose bounds were
/// clamped to the owning range. Original client bounds plus one
/// `MAX_KEY_BOUND_LEN` range bound each way, with header slack.
const SCAN_REWRITE_BUF: usize = 2048;

/// `map_update` drain buffer: holds either a MAP_UPDATE_V1 batch
/// (≤ 3 076 bytes) or a RANGE_MAP_UPDATE_V1 full map. A full map
/// beyond this refuses whole (fail closed, counted) — the same bound
/// as the `range_map` init param, since both carry the same frame.
const MAP_UPDATE_DRAIN_BUF: usize = 4096;

/// Resolve every key a TXN touches to one partition, or refuse.
///
/// Body: `[cmp_count]` cmps of `[op:u8][klen:u16][key][witness:u64]`,
/// then `[then_count]` and `[else_count]` op lists of
/// `[op:u8][blen:u16][body]`. A nested op with no extractable single
/// key (a scan, a multi-key op) refuses — its key set cannot be proved
/// single-range.
fn route_txn_ordered(router: &RouterState, body: &[u8]) -> RouteOrdered {
    let map = &router.range_map;
    let reject = RouteOrdered::Reject {
        result: KV_RESULT_CROSS_RANGE,
        stale: false,
    };
    let mut partition: Option<u16> = None;
    let mut domain: Option<u32> = None;
    let mut check = |key: &[u8]| -> bool {
        match map.lookup(key) {
            Some(d) => match partition {
                None => {
                    partition = Some(d.binding.partition_id);
                    domain = Some(d.binding.cluster_id);
                    true
                }
                Some(p) => p == d.binding.partition_id,
            },
            None => false,
        }
    };
    let mut at = 0usize;
    let take_u16 = |b: &[u8], at: &mut usize| -> Option<u16> {
        let v = u16::from_le_bytes([*b.get(*at)?, *b.get(*at + 1)?]);
        *at += 2;
        Some(v)
    };
    let Some(cmp_count) = take_u16(body, &mut at) else {
        return reject;
    };
    for _ in 0..cmp_count {
        // [cmp_op][klen][key][witness]
        at += 1;
        let Some(klen) = take_u16(body, &mut at) else {
            return reject;
        };
        let Some(key) = body.get(at..at + klen as usize) else {
            return reject;
        };
        at += klen as usize;
        if !check(key) {
            return reject;
        }
        at += 8;
    }
    for _branch in 0..2 {
        let Some(op_count) = take_u16(body, &mut at) else {
            return reject;
        };
        for _ in 0..op_count {
            let Some(&inner_op) = body.get(at) else {
                return reject;
            };
            at += 1;
            let Some(blen) = take_u16(body, &mut at) else {
                return reject;
            };
            let Some(inner_body) = body.get(at..at + blen as usize) else {
                return reject;
            };
            at += blen as usize;
            match op_key_bytes(inner_op, inner_body) {
                Some(key) => {
                    if !check(key) {
                        return reject;
                    }
                }
                None => return reject,
            }
        }
    }
    match partition {
        Some(p) => route_txn_single(router, p, domain.unwrap_or(router.local_cluster_domain)),
        // A key-free TXN (comparisons only over nothing) has no
        // range to belong to; the first range serves it.
        None => route_single(router, &map.ranges()[0]),
    }
}

/// Is `key` inside the active span barrier? (Caller checks
/// `barrier_set` first; empty end = unbounded above.)
fn barrier_covers(router: &RouterState, key: &[u8]) -> bool {
    let start = &router.barrier_start[..router.barrier_start_len as usize];
    let end = &router.barrier_end[..router.barrier_end_len as usize];
    start <= key && (end.is_empty() || key < end)
}

/// One lowercase/uppercase hex digit → nibble.
fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Every LOCAL-domain range's partition binding has a live worker port
/// (and, on a replicated graph, a live proposal port). A range in
/// another cluster domain (§11.3/§26.5) is reached by redirect, not by a
/// port on this router, so its `partition_id` — cluster-local to its own
/// domain — is not required to be wired here.
fn ordered_partitions_reachable(router: &RouterState) -> bool {
    router
        .range_map
        .ranges()
        .iter()
        .filter(|d| d.binding.cluster_id == router.local_cluster_domain)
        .all(|d| {
            // Fanout mode: partition 0 keeps its direct port; every other
            // partition rides the element-1 demux feed. A range is reachable
            // iff its port (and, on a replicated graph, its proposal port) is
            // wired — the per-partition port cap does not apply above 0.
            if router.partition_fanout != 0 {
                let idx = if d.binding.partition_id == 0 { 0 } else { 1 };
                return router.kv_outs[idx] >= 0
                    && (router.replicated == 0 || router.proposal_outs[idx] >= 0);
            }
            let p = d.binding.partition_id as usize;
            p < MAX_PARTITION_PORTS
                && router.kv_outs[p] >= 0
                && (router.replicated == 0 || router.proposal_outs[p] >= 0)
        })
}

/// Outcome of ordered-mode route resolution.
enum RouteOrdered {
    /// Whole op belongs to one partition; body forwarded untouched.
    Single { partition: u16 },
    /// A `RANGE_SCAN` sub-scan: body rewritten (clamped bounds,
    /// provider-local cursor) into the caller's buffer; the response
    /// path re-tags the continuation.
    Scan {
        partition: u16,
        range_idx: u8,
        next: u64,
        rewritten_len: usize,
    },
    /// The resolved range's home partition is in another Clustor cluster
    /// domain (§11.3/§26.5): the caller is redirected to `domain` rather
    /// than served a wrong local answer.
    CrossDomain { domain: u32 },
    /// Fail-closed refusal, before any side effect. `stale` selects
    /// the metric: a stale route (refresh and retry) vs a cross-range
    /// key set (no coordinator until Phase 5).
    Reject { result: u8, stale: bool },
}

/// Route to a resolved descriptor's partition — unless its binding names
/// a different cluster domain than this router serves, in which case the
/// caller is redirected (§11.3/§26.5). The single place the domain check
/// lives, so every ordered single-key resolution honours it identically.
fn route_single(router: &RouterState, d: &partition_map::RangeDescriptor) -> RouteOrdered {
    if d.binding.cluster_id != router.local_cluster_domain {
        RouteOrdered::CrossDomain {
            domain: d.binding.cluster_id,
        }
    } else {
        RouteOrdered::Single {
            partition: d.binding.partition_id,
        }
    }
}

/// The transaction-path counterpart: the keys already agreed on one
/// `partition`; redirect if that partition's `domain` is not this
/// router's, otherwise route locally (§11.3/§26.5).
fn route_txn_single(router: &RouterState, partition: u16, domain: u32) -> RouteOrdered {
    if domain != router.local_cluster_domain {
        RouteOrdered::CrossDomain { domain }
    } else {
        RouteOrdered::Single { partition }
    }
}

/// Extract the single key of a key-first op's body, if it has one.
/// Mirrors `op_key`'s layout knowledge byte for byte — the two MUST
/// classify identically, or the barrier and the route would disagree
/// about what an op touches.
fn op_key_bytes(op: u8, body: &[u8]) -> Option<&[u8]> {
    let read_at = |off: usize| -> Option<&[u8]> {
        let klen = u16::from_le_bytes([*body.get(off)?, *body.get(off + 1)?]) as usize;
        body.get(off + 2..off + 2 + klen)
    };
    match op {
        0x01 | 0x02 | 0x06 | 0x07 | 0x08 | 0x09 | 0x0A | 0x0F | 0x10 => read_at(0),
        0x03 | 0x0D => {
            if body.len() >= 2 && u16::from_le_bytes([body[0], body[1]]) == 1 {
                read_at(2)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Parse the span head of a RANGE_SCAN / SCAN_VERSIONS body:
/// `[start_len:u16][start…][end_len:u16][end…]` → (start, end, rest).
fn parse_span(body: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    let slen = u16::from_le_bytes([*body.first()?, *body.get(1)?]) as usize;
    let start = body.get(2..2 + slen)?;
    let eoff = 2 + slen;
    let elen = u16::from_le_bytes([*body.get(eoff)?, *body.get(eoff + 1)?]) as usize;
    let end = body.get(eoff + 2..eoff + 2 + elen)?;
    let rest = body.get(eoff + 2 + elen..)?;
    Some((start, end, rest))
}

/// Resolve an op against the ordered range map (routing_mode 2).
///
/// Single-key ops route by their key bytes. A `RANGE_SCAN` becomes a
/// sub-scan of the range the continuation names (or the first covering
/// range on a fresh cursor), with bounds clamped to that range and the
/// cross-range continuation minted for the response path — §20's
/// "resolve all descriptors covering the span, merge ordered subscans"
/// with the merge state carried in the CURSOR rather than in router
/// memory. A `SCAN_VERSIONS` must fit one range: two partitions have
/// independent revision spaces, so a cross-range version window is
/// unanswerable, not just unimplemented. Multi-key writes and
/// whole-store ops on a multi-range map refuse with
/// `KV_RESULT_CROSS_RANGE` until the Phase 5 coordinator exists.
fn route_ordered(
    router: &RouterState,
    op: u8,
    body: &[u8],
    scan_buf: &mut [u8; SCAN_REWRITE_BUF],
) -> RouteOrdered {
    let map = &router.range_map;
    let epoch_low = router.routing_epoch as u8;
    let reject_internal = RouteOrdered::Reject {
        result: KV_RESULT_INTERNAL,
        stale: false,
    };
    if map.is_empty() {
        // module_new refuses ordered mode without a map; belt over
        // braces.
        return reject_internal;
    }

    if op == KV_OP_RANGE_SCAN {
        let Some((start, end, rest)) = parse_span(body) else {
            return reject_internal;
        };
        if rest.len() != 10 {
            return reject_internal;
        }
        let cursor = u64::from_le_bytes(rest[..8].try_into().unwrap_or([0; 8]));
        let limit = &rest[8..10];
        let Some((first, last)) = map.covering(start, end) else {
            // Empty span: no range owns any of it. Forward to the
            // first range untouched — the provider answers an empty
            // page with cursor 0, which is the correct result.
            return route_single(router, &map.ranges()[0]);
        };
        let (range_idx, provider_cursor) = if cursor == 0 {
            (first, 0u64)
        } else {
            let (idx, stamp, pcur) = partition_map::split_scan_cursor(cursor);
            if stamp != epoch_low || idx < first || idx > last {
                // The map changed under the scan (or the continuation
                // is garbage): a resume would skip or repeat part of
                // the span. Stale route — the caller restarts.
                return RouteOrdered::Reject {
                    result: KV_RESULT_DIRTY_EPOCH,
                    stale: true,
                };
            }
            (idx, pcur)
        };
        let d = &map.ranges()[range_idx];
        // Clamp the sub-span to the owning range: max(start, d.start)
        // and min(end, d.end) with the empty-is-unbounded convention.
        let sub_start = if d.start_key() > start {
            d.start_key()
        } else {
            start
        };
        let sub_end = if d.end_key().is_empty() {
            end
        } else if end.is_empty() || d.end_key() < end {
            d.end_key()
        } else {
            end
        };
        let next = if range_idx < last {
            match partition_map::tag_scan_cursor(range_idx + 1, epoch_low, 0) {
                Some(t) => t,
                None => return reject_internal,
            }
        } else {
            0
        };
        // Rewrite: [sub_start][sub_end][provider_cursor][limit].
        let need = 2 + sub_start.len() + 2 + sub_end.len() + 10;
        if need > scan_buf.len() {
            return reject_internal;
        }
        let mut n = 0;
        scan_buf[n..n + 2].copy_from_slice(&(sub_start.len() as u16).to_le_bytes());
        n += 2;
        scan_buf[n..n + sub_start.len()].copy_from_slice(sub_start);
        n += sub_start.len();
        scan_buf[n..n + 2].copy_from_slice(&(sub_end.len() as u16).to_le_bytes());
        n += 2;
        scan_buf[n..n + sub_end.len()].copy_from_slice(sub_end);
        n += sub_end.len();
        scan_buf[n..n + 8].copy_from_slice(&provider_cursor.to_le_bytes());
        n += 8;
        scan_buf[n..n + 2].copy_from_slice(limit);
        n += 2;
        let range_u8 = match u8::try_from(range_idx) {
            Ok(r) => r,
            Err(_) => return reject_internal,
        };
        // A sub-scan of a range in another domain redirects, same as a
        // point op — the partition it names is not reachable here.
        if d.binding.cluster_id != router.local_cluster_domain {
            return RouteOrdered::CrossDomain {
                domain: d.binding.cluster_id,
            };
        }
        return RouteOrdered::Scan {
            partition: d.binding.partition_id,
            range_idx: range_u8,
            next,
            rewritten_len: n,
        };
    }

    if op == KV_OP_SCAN_VERSIONS {
        let Some((start, end, _rest)) = parse_span(body) else {
            return reject_internal;
        };
        return match map.covering(start, end) {
            Some((first, last)) if first == last => route_single(router, &map.ranges()[first]),
            Some(_) => RouteOrdered::Reject {
                result: KV_RESULT_CROSS_RANGE,
                stale: false,
            },
            None => route_single(router, &map.ranges()[0]),
        };
    }

    if let Some(key) = op_key_bytes(op, body) {
        return match map.lookup(key) {
            Some(d) => route_single(router, d),
            // A validated map covers MIN..MAX; None cannot happen on
            // one. Fail closed as a stale route regardless.
            None => RouteOrdered::Reject {
                result: KV_RESULT_DIRTY_EPOCH,
                stale: true,
            },
        };
    }

    // A TXN whose every key — comparisons and both branches — lives in
    // ONE range executes there atomically (§13.1 single-range
    // transactions need no coordinator). Any key that cannot be
    // resolved, or a second partition, refuses CROSS_RANGE: half a
    // transaction is worse than none.
    if op == 0x05 {
        return route_txn_ordered(router, body);
    }

    // The two-phase family. These arrive from the transaction
    // coordinator via KV_OP_TARGETED — but ALSO from remote compute
    // over `lattice.data`, whose surface defines Prepare/Commit
    // (§14.8) and whose anchor refuses TARGETED by design (explicit
    // partition addressing is a lifecycle tool, not a client
    // capability). Without these arms a remote participant prepare
    // fell through to CROSS_RANGE below, which made 2PC over the data
    // surface impossible in ordered mode — every op it needs, refused.
    //
    // Each routes by the keys the op actually touches, refusing if
    // they span partitions: a PARTICIPANT prepare is single-range by
    // construction (that is what a participant is), and a resolve
    // carries an explicit key list.
    if op == types::KV_OP_TXN_PREPARE {
        return route_txn_prepare_ordered(router, body);
    }
    if op == types::KV_OP_TXN_RESOLVE {
        return route_txn_resolve_ordered(router, body);
    }
    if op == types::KV_OP_TXN_RECORD {
        // A record routes by ITS OWN key, [KS_TXN_RECORD || txn_id] —
        // the same derivation the store uses and the placement the
        // helper's map lookup depends on. The txn id is the record
        // body's first 16 bytes.
        let Some(id) = body.get(0..16) else {
            return RouteOrdered::Reject {
                result: KV_RESULT_INTERNAL,
                stale: false,
            };
        };
        let mut rk = [0u8; 20];
        rk[0..4].copy_from_slice(&KS_TXN_RECORD.to_be_bytes());
        rk[4..20].copy_from_slice(id);
        return match map.lookup(&rk) {
            Some(d) => route_single(router, d),
            None => RouteOrdered::Reject {
                result: KV_RESULT_DIRTY_EPOCH,
                stale: true,
            },
        };
    }

    // Multi-key / whole-store ops (MSET, TXN, FLUSH, multi-key
    // DELETE/MGET, keys-only SCAN forms, historical span reads). On a
    // single-range map they are exactly the ops the single-partition
    // graph always served; on a multi-range map they have no
    // cross-range execution until Phase 5 and refuse BY NAME.
    if map.len() == 1 {
        return route_single(router, &map.ranges()[0]);
    }
    RouteOrdered::Reject {
        result: KV_RESULT_CROSS_RANGE,
        stale: false,
    }
}

/// Route a participant PREPARE by every key it stages or compares.
/// Body: `[txn:16][home:16][prov_ts:8][epoch:4]` then
/// `[cmp_count][{op,klen,key,witness}…][then_count][{op,blen,body}…]`.
fn route_txn_prepare_ordered(router: &RouterState, body: &[u8]) -> RouteOrdered {
    let reject = RouteOrdered::Reject {
        result: KV_RESULT_CROSS_RANGE,
        stale: false,
    };
    let map = &router.range_map;
    let mut partition: Option<u16> = None;
    let mut domain: Option<u32> = None;
    let mut check = |key: &[u8]| -> bool {
        match map.lookup(key) {
            Some(d) => match partition {
                None => {
                    partition = Some(d.binding.partition_id);
                    domain = Some(d.binding.cluster_id);
                    true
                }
                Some(p) => p == d.binding.partition_id,
            },
            None => false,
        }
    };
    let take_u16 = |b: &[u8], at: &mut usize| -> Option<u16> {
        let v = u16::from_le_bytes([*b.get(*at)?, *b.get(*at + 1)?]);
        *at += 2;
        Some(v)
    };
    let mut at = 16 + 16 + 8 + 4;
    let Some(cmp_count) = take_u16(body, &mut at) else {
        return reject;
    };
    for _ in 0..cmp_count {
        at += 1; // cmp op
        let Some(klen) = take_u16(body, &mut at) else {
            return reject;
        };
        let Some(key) = body.get(at..at + klen as usize) else {
            return reject;
        };
        if !check(key) {
            return reject;
        }
        at += klen as usize + 8; // key + witness
    }
    let Some(then_count) = take_u16(body, &mut at) else {
        return reject;
    };
    for _ in 0..then_count {
        let Some(&op_byte) = body.get(at) else {
            return reject;
        };
        at += 1;
        let Some(blen) = take_u16(body, &mut at) else {
            return reject;
        };
        let Some(sub) = body.get(at..at + blen as usize) else {
            return reject;
        };
        at += blen as usize;
        match op_key_bytes(op_byte, sub) {
            Some(key) if check(key) => {}
            _ => return reject,
        }
    }
    match partition {
        Some(p) => route_txn_single(router, p, domain.unwrap_or(router.local_cluster_domain)),
        None => reject,
    }
}

/// Route a RESOLVE by its explicit key list. Body:
/// `[txn:16][home:16][epoch:4][committed:1][commit_ts:8][count][{klen,key}…]`.
fn route_txn_resolve_ordered(router: &RouterState, body: &[u8]) -> RouteOrdered {
    let reject = RouteOrdered::Reject {
        result: KV_RESULT_CROSS_RANGE,
        stale: false,
    };
    let map = &router.range_map;
    let mut partition: Option<u16> = None;
    let mut domain: Option<u32> = None;
    let mut at = 45usize;
    let Some(count) = body.get(at..at + 2) else {
        return reject;
    };
    let count = u16::from_le_bytes([count[0], count[1]]);
    at += 2;
    for _ in 0..count {
        let Some(l) = body.get(at..at + 2) else {
            return reject;
        };
        let klen = u16::from_le_bytes([l[0], l[1]]) as usize;
        at += 2;
        let Some(key) = body.get(at..at + klen) else {
            return reject;
        };
        at += klen;
        match map.lookup(key) {
            Some(d) => match partition {
                None => {
                    partition = Some(d.binding.partition_id);
                    domain = Some(d.binding.cluster_id);
                }
                Some(p) if p == d.binding.partition_id => {}
                _ => return reject,
            },
            None => return reject,
        }
    }
    match partition {
        Some(p) => route_txn_single(router, p, domain.unwrap_or(router.local_cluster_domain)),
        None => reject,
    }
}

#[inline]
fn fence_key(proto: u8, conn: u8) -> Option<usize> {
    let p = match proto {
        PROTO_ETCD => 0usize,
        PROTO_REDIS => 1,
        PROTO_MEMCACHED => 2,
        PROTO_MEMCACHED_UDP => 3,
        _ => return None,
    };
    Some(p * 256 + conn as usize)
}

define_params! {
    RouterState;

    // 1 = classify ALL reads as linearizable (take the ReadIndex fence
    // when `lin_read_out`/`read_release` are wired). Bench A/B lever —
    // production classification honors the per-request consistency byte
    // (REQ_LINEARIZABLE), which the etcd adapter stamps for
    // `serializable=false` Ranges.
    1, lin_reads, u8, 0
        => |s, d, len| { s.lin_reads = p_u8(d, len, 0, 0); };

    // 1 = this graph replicates writes through Raft. Declares INTENT, and
    // is checked against the wiring at init: with `replicated = 1` and
    // `proposal_out` unwired, `module_new` fails and the graph refuses to
    // start.
    //
    // Without it the mode is inferred purely from whether the edge exists
    // (`use_consensus = is_write && proposal_out >= 0`), which is
    // fail-OPEN: omit one line of YAML in a replicated deployment and
    // every write silently takes the direct local path, bypassing Raft
    // with no error, no warning and no metric. Fluxor cannot catch this
    // for us — `validate_required_inputs_wired` skips anything that is
    // not an input, so a required OUTPUT is not expressible in the
    // manifest.
    //
    // Default 0 preserves single-node / direct-apply graphs unchanged.
    2, replicated, u8, 0
        => |s, d, len| { s.replicated = p_u8(d, len, 0, 0); };

    // 0 = legacy hash routing (default, byte-identical Phase-1 path).
    // 1 = partition-map-fenced: every request resolves through the
    // hash-slot map with a generation/epoch check; a mismatch rejects
    // with KV_RESULT_DIRTY_EPOCH (StaleRoute semantics) instead of
    // dispatching. See the Phase-3 ROUTING block above.
    3, routing_mode, u8, 0
        => |s, d, len| { s.routing_mode = p_u8(d, len, 0, 0); };

    // The STATIC ordered range map (routing_mode 2): one encoded
    // RANGE_MAP_UPDATE_V1 frame as a HEX string, decoded and applied
    // at init. Present-but-invalid fails `module_new` — a graph with a
    // bad map must refuse to start, not serve NoRoute on every key.
    // `str` params arrive TLV-chunked at 255 bytes with the same tag,
    // so the apply APPENDS; overflow latches the poisoned length.
    // 1 = the static `range_map` is a BOOT map that may be stale (a
    // lifecycle supervisor owns the live map and republishes it at
    // boot): refuse ordered-mode client requests retryably until the
    // first runtime map publication arrives. Without this, the window
    // between restart and republication serves routes from a map the
    // last split already superseded — reads land on a tombstoned span
    // and report absence that is not true (§21 invariant 6, applied
    // to restart). Lifecycle traffic is exempt: the supervisor must
    // read its own record to publish at all.
    5, await_map, u8, 0
        => |s, d, len| { s.await_map = p_u8(d, len, 0, 0); };

    // §11.3/§26.5: which Clustor cluster domain THIS router serves. A
    // `partition_id` is cluster-local, so a range descriptor whose
    // binding names a DIFFERENT `cluster_id` lives in another domain
    // this router cannot reach — its keys refuse with a
    // KV_RESULT_CROSS_DOMAIN redirect rather than a wrong local answer.
    // Default 0 is the single-domain deployment: every existing map
    // binds cluster_id 0, so nothing routes cross-domain.
    // Dense hosting. 0 = classic per-partition ports (partition p
    // uses the p-th kv_out/proposal_out pair, capped at MAX_PARTITION_PORTS).
    // 1 = fanout: partition 0 keeps its direct ports; every frame bound
    // for a higher partition is prefixed with `[partition_id:u16 LE]` and
    // written to the p1 port pair (kv1_out / proposal1_out), which a
    // `partition_demux` fans to N per-partition instances — lifting the
    // 2-partition ceiling the 16-output-port cap imposes on the keyed
    // data path (TARGETED lifecycle ops still address direct ports only).
    // Default 0 keeps the classic port layout, untagged.
    7, partition_fanout, u8, 0
        => |s, d, len| { s.partition_fanout = p_u8(d, len, 0, 0); };

    6, local_cluster_domain, u32, 0
        => |s, d, len| { s.local_cluster_domain = p_u32(d, len, 0, 0); };

    4, range_map, str, 0
        => |s, d, len| {
            let at = s.range_map_param_len as usize;
            if s.range_map_param_len == u16::MAX || at + len > RANGE_MAP_PARAM_MAX * 2 {
                s.range_map_param_len = u16::MAX;
            } else {
                unsafe {
                    for i in 0..len { s.range_map_param[at + i] = *d.add(i); }
                }
                s.range_map_param_len = (at + len) as u16;
            }
        };
}

const KV_EPOCH_DEFAULT: KvEpoch = 1;

#[repr(C)]
struct RouterState {
    syscalls: *const SyscallTable,

    // Inputs (manifest order: etcd_in, redis_in, memcached_in,
    // memcached_udp_in, route_plan, cp_proof, quota_decision, kv_in)
    etcd_in: i32,
    redis_in: i32,
    memcached_in: i32,
    memcached_udp_in: i32,
    route_plan_in: i32,
    cp_proof_in: i32,
    quota_decision_in: i32,
    kv_in: i32,
    /// Internal callers (watch_fanout backfill, relational executor).
    /// -1 when the graph has no such module wired.
    internal_watch_in: i32,
    internal_sql_in: i32,

    // Outputs (manifest order: kv_out, watch_ctrl, lease_ctrl,
    // etcd_out, redis_out, memcached_out, memcached_udp_out, metrics,
    // proposal_out)
    kv_out: i32,
    watch_ctrl_out: i32,
    lease_ctrl_out: i32,
    etcd_out: i32,
    redis_out: i32,
    memcached_out: i32,
    memcached_udp_out: i32,
    metrics_out: i32,
    internal_watch_out: i32,
    compute_out: i32,
    /// Phase 7 consensus bridge. -1 when unwired (Phase 1 fallback —
    /// write commands go straight to the worker via `kv_out`).
    proposal_out: i32,
    /// Linearizable-read fence submission → consensus.read.
    /// -1 when unwired (all reads stay on the snapshot path).
    lin_read_out: i32,
    /// Fence releases ← consensus.applied.
    read_release_in: i32,

    /// Runtime slot-binding update batches (MAP_UPDATE_V1 frames).
    /// -1 when unwired (the static single-partition default map stands).
    map_update_in: i32,

    /// Param `lin_reads`: 1 = classify ALL reads linearizable (bench
    /// A/B lever); 0 = honor only the envelope's REQ_LINEARIZABLE.
    lin_reads: u8,
    /// Param `replicated`: 1 = this graph replicates writes through
    /// Raft. Cross-checked against `proposal_out` wiring at init.
    replicated: u8,
    /// Param `routing_mode`: 0 = legacy, 1 = partition-map-fenced.
    routing_mode: u8,
    /// Param `local_cluster_domain` (§11.3/§26.5): the Clustor cluster
    /// domain this router serves. A descriptor whose `binding.cluster_id`
    /// differs lives in another domain and refuses CROSS_DOMAIN. Default
    /// 0 = the single-domain deployment (every existing map binds 0).
    local_cluster_domain: u32,

    /// Declared routing form + the map generation the router currently
    /// expects (§11.2: the routing form comes from keyspace metadata,
    /// never key sniffing). Advanced by MAP_UPDATE_V1 batches: after an
    /// applied batch the expected generation is the max generation seen,
    /// so slots the batch did NOT cover fence as stale until a follow-up
    /// covers them — stale metadata fails closed, it never routes.
    keyspace_routing: KeyspaceRouting,
    /// The router's current routing-epoch view (`types::RoutingEpoch`),
    /// presented on every fenced lookup against the map's own epoch.
    routing_epoch: RoutingEpoch,
    /// The §11.4 hash-slot map. Static single-partition default: every
    /// slot → partition 0, generation MAP_GENERATION_DEFAULT.
    slot_map: HashSlotMap,
    /// One-shot-log latch for dropped map_update frames.
    logged_map_update_drop: u8,

    /// The §11.2 ORDERED range map (routing_mode 2). Loaded from the
    /// `range_map` param at init; replaced whole by
    /// `MSG_RANGE_MAP_UPDATE` frames on `map_update` at runtime (each
    /// replacement bumps `routing_epoch`, which retires every
    /// outstanding scan continuation — their epoch stamp no longer
    /// matches, so a resume is a stale route, never a wrong range).
    range_map: OrderedRangeMap,
    /// `range_map` param HEX characters, accumulated across TLV chunks
    /// by `parse_tlv` and decoded by `module_new` (params parse before
    /// ports exist, and a bad map must fail init). `u16::MAX` =
    /// oversized (refuse).
    range_map_param: [u8; RANGE_MAP_PARAM_MAX * 2],
    range_map_param_len: u16,
    /// Range-supervisor caller pair (PROTO_INTERNAL_LIFECYCLE).
    /// -1 when the graph has no supervisor wired.
    internal_lifecycle_in: i32,
    internal_lifecycle_out: i32,
    /// Param `await_map` + the latch it waits on.
    await_map: u8,
    map_published: u8,
    /// Phase 6 job-worker caller pair (PROTO_INTERNAL_JOB). -1 when no
    /// worker is wired.
    internal_job_in: i32,
    /// Phase 9 `lattice_data_anchor` ingress (PROTO_INTERNAL_DATA).
    /// Replies leave on `compute_out`, shared with the in-graph SQL
    /// executor — see the init refusal for why that is sound.
    internal_data_in: i32,
    /// `gateway.responses` — the gateway's reject feedback. See
    /// `drain_gateway_rejects`. Unwired (-1) in single-node graphs,
    /// where the local gateway is always the leader and never rejects.
    gateway_reject_in: i32,

    /// Cross-partition transaction helping (Phase 5). See
    /// `maybe_start_help`. Each slot is one in-flight help: read the
    /// home record from its partition, and if it holds a DECISION,
    /// carry that decision to the stuck partition. The router never
    /// decides — it is a courier for a record that already did.
    helps: [Help; HELP_SLOTS],
    next_help_corr: u64,
    /// Helps started / helps that carried a decision. The gap between
    /// them is homes still pending — normal while a coordinator lives.
    m_helps_started: u64,
    m_helps_resolved: u64,
    internal_job_out: i32,
    /// §12 cutover barrier (SPAN_BARRIER_V1 on `map_update`). While
    /// set, ordered-mode writes into `[barrier_start, barrier_end)`
    /// refuse retryably. One barrier at a time: one lifecycle
    /// operation runs at a time, and the supervisor owns both facts.
    barrier_set: u8,
    barrier_start: [u8; partition_map::MAX_KEY_BOUND_LEN],
    barrier_start_len: u16,
    barrier_end: [u8; partition_map::MAX_KEY_BOUND_LEN],
    barrier_end_len: u16,
    /// Worker command ports by partition: partition p → element p.
    /// Element 0 is `kv_out`; 1..4 are `kv1..kv3_out` (-1 unwired).
    kv_outs: [i32; MAX_PARTITION_PORTS],
    /// Proposal ports by partition, same scheme (element 0 =
    /// `proposal_out`).
    proposal_outs: [i32; MAX_PARTITION_PORTS],
    /// Dense hosting: when 1, every partition-bound frame is tagged
    /// `[partition_id:u16]` and written to the element-1 port pair (the
    /// `partition_demux` feed) instead of a per-partition port.
    partition_fanout: u8,
    /// Ordered-mode refusals of cross-range key sets (metric 6).
    m_cross_range_rejects: u64,
    /// Requests refused because their key's home range lives in another
    /// Clustor cluster domain (§11.3/§26.5) — answered with a
    /// KV_RESULT_CROSS_DOMAIN redirect, counted so a misconfigured
    /// map-vs-`local_cluster_domain` shows up as a signal, not silence.
    m_cross_domain_rejects: u64,
    /// Requests refused on the consensus branch (oversize command, or
    /// backpressure on the proposal port). Counted because these used
    /// to be silent, and a silent drop is invisible in every metric.
    m_proposal_drops: u64,

    /// Reads awaiting their ReadIndex fence.
    pending_lin: [PendingLinRead; LIN_READ_SLOTS],
    /// LIN-BOUND rejects surfaced to clients (fail-closed error, spec
    /// Phase 5 — the anchor maps KV_RESULT_LIN_BOUND to its protocol's
    /// unavailability error).
    lin_rejects: u32,
    /// Per-conn ordering: count of ops outstanding (dispatched, no
    /// response yet) and the path they're on. An op wanting a DIFFERENT
    /// path while count > 0 is held; same-path ops pipeline.
    conn_outstanding: [u8; FENCE_KEYS],
    conn_path: [u8; FENCE_KEYS],
    /// Held requests per conn — once anything is held, ALL later
    /// same-conn ops hold too (nothing may overtake the queue).
    held_count: [u8; FENCE_KEYS],
    /// Recovery valve: wall-clock deadline (ms) after which a conn's
    /// outstanding count force-resets. An op whose completion signal is
    /// structurally unwired (e.g. throttle-rejected proposals in graphs
    /// without the codec response path) must not wedge its conn's
    /// ordering slot forever. Refreshed on every dispatch/settle; a
    /// forced reset can only reorder flows that were already lost in a
    /// timeout, never healthy traffic.
    conn_deadline_ms: [u64; FENCE_KEYS],
    /// Session-order hold pool (see `HeldRequest`).
    held: [HeldRequest; HOLD_SLOTS],
    /// Monotonic arrival stamp for held-request replay order.
    hold_seq: u32,
    /// Held requests dropped because the pool was full → the client got
    /// a fail-closed LIN_BOUND error instead of a reordered response.
    hold_overflows: u32,
    /// Step counter gating the O(FENCE_KEYS) recovery sweeps — those
    /// are timeout backstops, not hot-path work, so running them every
    /// step needlessly burned ~1024 iterations/tick and pushed the
    /// domain over budget in multi-node graphs. Swept every
    /// `SWEEP_INTERVAL` steps instead.
    sweep_ctr: u16,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=routed, 1=rejects, 2=lin_reads,
    // 3=stale_route_rejects, 4=map_generation, 5=map_update_drops).
    m_routed: u64,
    /// Per-range write counts (§12.4 load signal). Monotonic; a
    /// consumer differences them over time to get a rate, exactly as
    /// every other counter on this port is used.
    m_range_writes: [u64; MAX_TRACKED_RANGES],
    /// Per-range Space-Saving table: key hashes and their (over-
    /// estimated) counts.
    hot_key_hash: [[u64; HOT_KEY_SLOTS]; MAX_TRACKED_RANGES],
    hot_key_count: [[u64; HOT_KEY_SLOTS]; MAX_TRACKED_RANGES],
    m_rejects: u64,
    m_lin_reads: u64,
    /// Highest applied index the state worker has reported. The
    /// applied-index half of the linearizable-read fence compares
    /// against this; until the worker reports, it is 0 and every grant
    /// waits, which is the correct direction to be wrong in.
    ///
    /// One value, not one per partition: the fence path releases to
    /// Per-partition applied index, indexed by `partition_id % MAX_TRACKED_RANGES`.
    /// Each worker stamps its partition into `MSG_APP_APPLIED_POS`, so a
    /// fenced read is satisfied by the SAME partition it must observe — not
    /// the max across a dense host's partitions, which could release a read
    /// against a store that has not applied the write it must see.
    worker_applied_index: [u64; MAX_TRACKED_RANGES],
    /// The term each position was reached in. Paired with the index
    /// because §21 invariant 11 wants a fence's epoch travelling with
    /// it: an applied index from a stale term is a stale fence.
    worker_applied_term: [u64; MAX_TRACKED_RANGES],
    /// Linearizable reads that reached their deadline still waiting for
    /// the worker to apply through the fence index, and failed closed.
    /// A persistently non-zero value means apply is lagging the commit
    /// horizon by more than the stash timeout.
    m_fence_timeouts: u64,
    /// Ops answered fail-closed because no completion signal arrived.
    /// A rising count on a healthy cluster means requests are reaching
    /// a node that cannot commit them — most often a follower.
    m_inflight_timeouts: u64,
    /// Writes failed fast because the gateway said NOT_LEADER. On a
    /// leader this stays zero; on a follower it counts every write a
    /// client sent to the wrong node. Its ratio to `inflight_timeouts`
    /// says whether refusals are arriving typed (here) or by timeout
    /// (there) — the second means the reject wire is down.
    m_not_leader_rejects: u64,
    /// Requests rejected by the mode-1 generation/epoch fence.
    m_stale_rejects: u64,
    /// MAP_UPDATE_V1 frames dropped fail-closed (unknown version,
    /// malformed frame, wrong envelope type, oversize).
    m_map_update_drops: u64,
    step_ctr: u64,

    /// corr_id → (proto, conn, path). Replies carry corr_id; the router
    /// picks the output port from proto and settles the per-conn
    /// ordering accounting from (conn, path).
    inflight: InflightTable<InflightMeta, MAX_INFLIGHT>,

    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl RouterState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.etcd_in = -1;
        self.redis_in = -1;
        self.memcached_in = -1;
        self.memcached_udp_in = -1;
        self.route_plan_in = -1;
        self.cp_proof_in = -1;
        self.quota_decision_in = -1;
        self.kv_in = -1;
        self.kv_out = -1;
        self.watch_ctrl_out = -1;
        self.lease_ctrl_out = -1;
        self.etcd_out = -1;
        self.redis_out = -1;
        self.memcached_out = -1;
        self.memcached_udp_out = -1;
        self.metrics_out = -1;
        self.proposal_out = -1;
        self.lin_read_out = -1;
        self.read_release_in = -1;
        self.map_update_in = -1;
        self.internal_watch_in = -1;
        self.internal_sql_in = -1;
        self.internal_watch_out = -1;
        self.compute_out = -1;
        self.lin_reads = 0;
        self.replicated = 0;
        self.routing_mode = ROUTING_MODE_LEGACY;
        self.local_cluster_domain = 0;
        self.keyspace_routing = KeyspaceRouting {
            keyspace_id: 0,
            routing_kind: RoutingKind::HashSlot,
            map_generation: MAP_GENERATION_DEFAULT,
        };
        self.routing_epoch = ROUTING_EPOCH_DEFAULT;
        self.slot_map = HashSlotMap::new(ROUTING_EPOCH_DEFAULT);
        {
            // Static single-partition default: every slot → partition 0,
            // generation MAP_GENERATION_DEFAULT (a valid, matching map —
            // mode 1 over it behaves exactly like the legacy path).
            let mut s: u16 = 0;
            while (s as usize) < partition_map::SLOT_COUNT {
                let _ = self.slot_map.bind_slot(s, SLOT_BINDING_DEFAULT);
                s += 1;
            }
        }
        self.logged_map_update_drop = 0;
        self.internal_lifecycle_in = -1;
        self.internal_lifecycle_out = -1;
        self.await_map = 0;
        self.map_published = 0;
        self.internal_job_in = -1;
        self.internal_data_in = -1;
        self.gateway_reject_in = -1;
        for h in &mut self.helps {
            h.in_use = false;
        }
        self.next_help_corr = HELP_CORR_BASE;
        self.m_helps_started = 0;
        self.m_helps_resolved = 0;
        self.internal_job_out = -1;
        self.barrier_set = 0;
        self.barrier_start = [0; partition_map::MAX_KEY_BOUND_LEN];
        self.barrier_start_len = 0;
        self.barrier_end = [0; partition_map::MAX_KEY_BOUND_LEN];
        self.barrier_end_len = 0;
        self.range_map = OrderedRangeMap::new(ROUTING_EPOCH_DEFAULT);
        self.range_map_param = [0; RANGE_MAP_PARAM_MAX * 2];
        self.range_map_param_len = 0;
        self.kv_outs = [-1; MAX_PARTITION_PORTS];
        self.proposal_outs = [-1; MAX_PARTITION_PORTS];
        self.partition_fanout = 0;
        self.m_cross_range_rejects = 0;
        self.m_cross_domain_rejects = 0;
        self.m_proposal_drops = 0;
        self.pending_lin = [PendingLinRead::empty(); LIN_READ_SLOTS];
        self.lin_rejects = 0;
        self.conn_outstanding = [0; FENCE_KEYS];
        self.conn_path = [0; FENCE_KEYS];
        self.held_count = [0; FENCE_KEYS];
        self.conn_deadline_ms = [0; FENCE_KEYS];
        self.held = [HeldRequest::empty(); HOLD_SLOTS];
        self.hold_seq = 0;
        self.hold_overflows = 0;
        self.sweep_ctr = 0;
        self.m_routed = 0;
        self.m_range_writes = [0; MAX_TRACKED_RANGES];
        self.hot_key_hash = [[0; HOT_KEY_SLOTS]; MAX_TRACKED_RANGES];
        self.hot_key_count = [[0; HOT_KEY_SLOTS]; MAX_TRACKED_RANGES];
        self.m_rejects = 0;
        self.m_lin_reads = 0;
        self.worker_applied_index = [0; MAX_TRACKED_RANGES];
        self.worker_applied_term = [0; MAX_TRACKED_RANGES];
        self.m_fence_timeouts = 0;
        self.m_inflight_timeouts = 0;
        self.m_not_leader_rejects = 0;
        self.m_stale_rejects = 0;
        self.m_map_update_drops = 0;
        self.step_ctr = 0;
        self.inflight = InflightTable::new();
    }
}

// ── Ingress drain ──────────────────────────────────────────────────────

/// Read one envelope from `chan` into `scratch`. Returns
/// `Some((msg_type, payload_slice))` on a complete envelope, else
/// `None`. `payload_slice` borrows `scratch`.
unsafe fn read_one_envelope<'a>(
    sys: &SyscallTable,
    chan: i32,
    scratch: &'a mut [u8],
) -> Option<(u8, &'a [u8])> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; 3];
    let n = (sys.channel_read)(chan, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return None;
    }
    let msg_type = hdr[0];
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len == 0 {
        return Some((msg_type, &scratch[..0]));
    }
    if payload_len > scratch.len() {
        return None;
    }
    let n2 = (sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return None;
    }
    Some((msg_type, &scratch[..payload_len]))
}

/// Write a 3-byte-framed envelope to `chan` from `scratch[..total]`.
/// Returns true on success.
unsafe fn write_envelope_raw(
    sys: &SyscallTable,
    chan: i32,
    scratch: &mut [u8],
    total: usize,
) -> bool {
    if chan < 0 {
        return false;
    }
    let n = (sys.channel_write)(chan, scratch.as_mut_ptr(), total);
    n == total as i32
}

/// Dense hosting: the worker (KV command) port and the partition tag for
/// `partition`. In fanout mode every partition rides the element-1
/// demux-feed port, tagged with its id; classic mode returns the
/// per-partition port with no tag.
fn worker_port_tag(router: &RouterState, partition: u16) -> (i32, Option<u16>) {
    if router.partition_fanout != 0 && partition != 0 {
        // Overflow partitions (>0) ride the element-1 demux feed, tagged;
        // partition 0 keeps its own direct port so the primary `kv_out`
        // is never left idle.
        (router.kv_outs[1], Some(partition))
    } else {
        (
            router.kv_outs[partition as usize % MAX_PARTITION_PORTS],
            None,
        )
    }
}

/// Dense hosting: the proposal port and partition tag for `partition`, mirroring
/// [`worker_port_tag`].
fn proposal_port_tag(router: &RouterState, partition: u16) -> (i32, Option<u16>) {
    if router.partition_fanout != 0 && partition != 0 {
        (router.proposal_outs[1], Some(partition))
    } else {
        (
            router.proposal_outs[partition as usize % MAX_PARTITION_PORTS],
            None,
        )
    }
}

/// Write a router-framed envelope with, or without, a `[partition:u16]`
/// tag spliced onto its payload for the `partition_demux`.
/// `src[..total]` holds a finished `[msg_type][len][payload]`. With no tag
/// it is written verbatim; with a tag the frame is rebuilt in `dest` with
/// the tag after the header and the length bumped by 2, so the demux can
/// route it and hand the downstream module the ORIGINAL bytes. `dest` must
/// differ from `src`.
unsafe fn write_envelope_maybe_tagged(
    sys: &SyscallTable,
    chan: i32,
    src: &mut [u8],
    total: usize,
    tag: Option<u16>,
    dest: &mut [u8],
) -> bool {
    let Some(partition) = tag else {
        return write_envelope_raw(sys, chan, src, total);
    };
    if chan < 0 || total < wire::ENVELOPE_HDR {
        return false;
    }
    let payload_len = total - wire::ENVELOPE_HDR;
    let new_payload = payload_len + 2;
    let new_total = wire::ENVELOPE_HDR + new_payload;
    if new_payload > u16::MAX as usize || new_total > dest.len() {
        return false;
    }
    dest[0] = src[0]; // msg_type unchanged
    dest[1] = (new_payload & 0xFF) as u8;
    dest[2] = ((new_payload >> 8) & 0xFF) as u8;
    dest[wire::ENVELOPE_HDR..wire::ENVELOPE_HDR + 2].copy_from_slice(&partition.to_le_bytes());
    dest[wire::ENVELOPE_HDR + 2..new_total].copy_from_slice(&src[wire::ENVELOPE_HDR..total]);
    let n = (sys.channel_write)(chan, dest.as_mut_ptr(), new_total);
    n == new_total as i32
}

/// Drain one MSG_KV_REQUEST from a given ingress port. Returns true
/// if something was processed.
unsafe fn drain_ingress(router: &mut RouterState, ingress_chan: i32, protocol: u8) -> bool {
    if ingress_chan < 0 {
        return false;
    }
    let sys_ptr = router.syscalls;
    if sys_ptr.is_null() {
        return false;
    }

    // Step 1: read the request envelope into a stack buffer (the
    // router's `scratch` field is reserved for outbound writes; doing
    // both at the same time would alias).
    let sys = &*sys_ptr;
    let poll = (sys.channel_poll)(ingress_chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    let n = (sys.channel_read)(ingress_chan, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    if hdr[0] != MSG_KV_REQUEST {
        return false; // unexpected envelope; consume and drop the header
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    // MSG_KV_REQUEST head = [corr:8][proto:1][tenant:4][conn:1][cons:1][op:1][body_len:2] = 18
    const REQ_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    if payload_len < REQ_HEAD || payload_len > router.scratch.len() {
        return false;
    }
    let mut in_buf = [0u8; SCRATCH_BUF_SIZE];
    let n2 = (sys.channel_read)(ingress_chan, in_buf.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }
    let req = &in_buf[..payload_len];

    // Session-order fast-path check: once anything from this conn is
    // held, everything later must queue behind it. The path-transition
    // check itself lives in `dispatch_parsed` (it needs the parsed op).
    const CONN_OFF: usize = 13;
    if payload_len > CONN_OFF {
        if let Some(key) = fence_key(protocol, req[CONN_OFF]) {
            if router.held_count[key] > 0 {
                hold_request(router, protocol, req);
                return true; // envelope consumed either way
            }
        }
    }

    if dispatch_parsed(router, protocol, req) == DISPATCH_BLOCKED {
        hold_request(router, protocol, req);
    }
    true
}

/// Everything after envelope ingestion: parse the MSG_KV_REQUEST
/// payload, enforce the per-conn path-transition barrier, register the
/// inflight corr, and route to the consensus / fenced-read / snapshot
/// path. Callable both from a fresh envelope (`drain_ingress`) and from
/// held-request replay — MUST stay free of channel reads.
unsafe fn dispatch_parsed(router: &mut RouterState, protocol: u8, req: &[u8]) -> i32 {
    let sys_ptr = router.syscalls;
    if sys_ptr.is_null() {
        return DISPATCH_DROPPED;
    }
    let sys = &*sys_ptr;
    let payload_len = req.len();
    // MSG_KV_REQUEST head = [corr:8][proto:1][tenant:4][conn:1][cons:1][op:1][body_len:2] = 18
    const REQ_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    if payload_len < REQ_HEAD {
        return DISPATCH_DROPPED;
    }
    let corr_id = u64::from_le_bytes([
        req[0], req[1], req[2], req[3], req[4], req[5], req[6], req[7],
    ]);
    let _embedded_proto = req[8]; // already known from ingress port
                                  // Canonical identity (§23). The tenant rides in the fixed head; the
                                  // database and keyspace ride in an OPTIONAL 8-byte tail after the
                                  // body, appended only by anchors that carry a native RequestContext
                                  // (today, `lattice_data_anchor`). A point-protocol envelope with no
                                  // tail is `(tenant, 0, 0)` — correct for a single flat keyspace.
    let id_tenant = u32::from_le_bytes([req[9], req[10], req[11], req[12]]);
    let conn_id = req[13];
    let consistency = req[14];
    let op = req[15];
    let body_len = u16::from_le_bytes([req[16], req[17]]) as usize;
    let body_off = 18;
    if body_off + body_len > payload_len {
        return DISPATCH_DROPPED;
    }
    let (id_database, id_keyspace) = if payload_len == body_off + body_len + 8 {
        let t = body_off + body_len;
        (
            u32::from_le_bytes([req[t], req[t + 1], req[t + 2], req[t + 3]]),
            u32::from_le_bytes([req[t + 4], req[t + 5], req[t + 6], req[t + 7]]),
        )
    } else {
        (0, 0)
    };

    // KV_OP_TARGETED unwrap (Phase 4 lifecycle bootstrap): the range
    // supervisor — and ONLY the range supervisor, on its own ingress —
    // may address a partition explicitly, because a split's child
    // bootstrap must write into the target while the map still names
    // the parent. From any other ingress, or outside ordered mode, the
    // wrapper refuses: a client that picks its partition can silently
    // split a keyspace in two.
    let mut op = op;
    let mut body_off = body_off;
    let mut body_len = body_len;
    let mut forced_partition: Option<u16> = None;
    if op == types::KV_OP_TARGETED {
        if router.routing_mode != ROUTING_MODE_ORDERED
            || protocol != types::PROTO_INTERNAL_LIFECYCLE
            || body_len < 3
        {
            router.m_cross_range_rejects = router.m_cross_range_rejects.wrapping_add(1);
            emit_reject(router, protocol, corr_id, conn_id, KV_RESULT_CROSS_RANGE);
            return DISPATCH_DROPPED;
        }
        let partition = u16::from_le_bytes([req[body_off], req[body_off + 1]]);
        if partition as usize >= MAX_PARTITION_PORTS {
            emit_reject(router, protocol, corr_id, conn_id, KV_RESULT_INTERNAL);
            return DISPATCH_DROPPED;
        }
        forced_partition = Some(partition);
        op = req[body_off + 2];
        body_off += 3;
        body_len -= 3;
    }

    // Route classification up front — the ordering barrier needs the
    // prospective path BEFORE any side effects.
    // A write is ANY mutating op — not just PUT/DELETE. Getting this
    // wrong (the old `op == 0x02 || op == 0x03`) sent INCR/DECR/APPEND/
    // PREPEND/MSET/CAS/FLUSH/TXN down the read path: on a replicated
    // deployment they bypassed consensus and mutated only the local
    // worker (no replication — task #23), and on a fence-wired config
    // they took the ReadIndex fence and returned CLUSTERDOWN. Classify
    // by op so every mutation goes through consensus when it is wired.
    let is_write = op_is_write(op);
    // Hash of the op's key, for §12.4's hot-KEY tracking. Ops with no
    // single key (scans, txns spanning many) contribute nothing rather
    // than contributing a wrong key — a txn attributed to one of its
    // keys would invent concentration that is not there.
    let dispatch_key_hash: HotKeys<'_> = if !is_write {
        HotKeys::None
    } else if op == types::KV_OP_TXN {
        HotKeys::Txn(&req[body_off..body_off + body_len])
    } else {
        match op_key_bytes(op, &req[body_off..body_off + body_len]) {
            Some(k) => HotKeys::Single(fnv1a64(k)),
            None => HotKeys::None,
        }
    };

    // Dependency key for the per-conn barrier (task #17).
    let (key_hash, wildcard) = op_key(op, &req[body_off..body_off + body_len]);

    // Phase-3 routing fence (§11.2, §21 invariant 6): in fenced mode
    // every request resolves through the hash-slot map before ANY side
    // effect (no barrier interaction, no inflight slot, no channel
    // write). The slot is `key_hash % SLOT_COUNT` — identical to
    // `HashSlotMap::slot_for_key`, since `op_key` uses the same FNV-1a.
    // A generation/epoch mismatch is a stale route
    // (`RouteError::retry_class() == RetryClass::StaleRoute`): reject
    // through the existing synth path as KV_RESULT_DIRTY_EPOCH (redis
    // renders `MOVED routing epoch advanced`) so the client refreshes
    // and retries — stale metadata must never mutate current state.
    let mut kpg_id: u16 = KPG_ID_DEFAULT;
    if router.routing_mode == ROUTING_MODE_FENCED {
        let slot = if wildcard {
            WILDCARD_SLOT
        } else {
            (key_hash % partition_map::SLOT_COUNT as u64) as u16
        };
        match router.slot_map.lookup_slot_generation_checked(
            slot,
            router.keyspace_routing.map_generation,
            router.routing_epoch,
        ) {
            Ok(b) => kpg_id = b.partition_id,
            Err(_stale) => {
                router.m_stale_rejects = router.m_stale_rejects.wrapping_add(1);
                emit_reject(router, protocol, corr_id, conn_id, KV_RESULT_DIRTY_EPOCH);
                // The envelope is consumed and answered; held-replay
                // callers treat non-BLOCKED as completed.
                return DISPATCH_DROPPED;
            }
        }
    }

    // Phase-3 ORDERED routing (§11.2/§20): resolve the owning range by
    // KEY BYTES, pick the partition's port pair, and — for a
    // `RANGE_SCAN` — clamp the sub-span to the owning range and mint
    // the cross-range continuation. Fail-closed refusals (a stale
    // continuation, a cross-range key set with no coordinator) reject
    // here, before any side effect, like the fenced block above.
    let mut scan = false;
    let mut scan_range = 0u8;
    let mut scan_next = 0u64;
    let mut scan_buf = [0u8; SCAN_REWRITE_BUF];
    let mut body_src_len = body_len;
    let mut body_in_scan_buf = false;
    if let Some(partition) = forced_partition {
        // Explicit addressing: no map lookup, no scan stitching (a
        // TARGETED RANGE_SCAN pages one partition with a raw provider
        // cursor — exactly what a bootstrap copy loop wants), and no
        // span barrier (the supervisor's own writes are the reason the
        // barrier exists).
        kpg_id = partition;
    } else if router.routing_mode == ROUTING_MODE_ORDERED {
        // Boot-map quarantine: a graph whose live map belongs to a
        // supervisor refuses client traffic retryably until that map
        // arrives — the static param map may predate a published
        // lifecycle operation.
        if router.await_map != 0
            && router.map_published == 0
            && protocol != types::PROTO_INTERNAL_LIFECYCLE
        {
            emit_reject(router, protocol, corr_id, conn_id, types::KV_RESULT_QUOTA);
            return DISPATCH_DROPPED;
        }
        // §12.1 step 6, enforcement half: while a cutover barrier is
        // up, WRITES into the moving span refuse retryably (QUOTA →
        // redis BUSY, SQL retryable error). Reads pass — the parent
        // still owns the span until publication. A wildcard write
        // (multi-key/whole-store) cannot prove it misses the span, so
        // it refuses conservatively while a barrier is up.
        if router.barrier_set != 0 && is_write {
            let barred = match op_key_bytes(op, &req[body_off..body_off + body_len]) {
                Some(key) => barrier_covers(router, key),
                None => true,
            };
            if barred {
                emit_reject(router, protocol, corr_id, conn_id, types::KV_RESULT_QUOTA);
                return DISPATCH_DROPPED;
            }
        }
        match route_ordered(
            router,
            op,
            &req[body_off..body_off + body_len],
            &mut scan_buf,
        ) {
            RouteOrdered::Single { partition } => kpg_id = partition,
            RouteOrdered::Scan {
                partition,
                range_idx,
                next,
                rewritten_len,
            } => {
                kpg_id = partition;
                scan = true;
                scan_range = range_idx;
                scan_next = next;
                body_src_len = rewritten_len;
                body_in_scan_buf = true;
            }
            RouteOrdered::CrossDomain { domain } => {
                // §11.3/§26.5: the key's home partition is in another
                // Clustor cluster domain. Redirect the caller to it,
                // carrying the domain so the redirect is actionable.
                router.m_cross_domain_rejects = router.m_cross_domain_rejects.wrapping_add(1);
                emit_reject_with_body(
                    router,
                    protocol,
                    corr_id,
                    conn_id,
                    types::KV_RESULT_CROSS_DOMAIN,
                    &domain.to_le_bytes(),
                );
                return DISPATCH_DROPPED;
            }
            RouteOrdered::Reject { result, stale } => {
                if stale {
                    router.m_stale_rejects = router.m_stale_rejects.wrapping_add(1);
                } else {
                    router.m_cross_range_rejects = router.m_cross_range_rejects.wrapping_add(1);
                }
                emit_reject(router, protocol, corr_id, conn_id, result);
                return DISPATCH_DROPPED;
            }
        }
    }
    let body_len = body_src_len;

    // Port pair of the owning partition. Element 0 aliases the classic
    // `kv_out`/`proposal_out`, so legacy and fenced modes see exactly
    // the bytes they always did. In fanout mode a partition above 0
    // rides the element-1 demux feed, tagged with `kpg_id`.
    let (kv_port, kv_tag) = worker_port_tag(router, kpg_id);
    let (proposal_port, proposal_tag) = proposal_port_tag(router, kpg_id);
    if kv_port < 0 {
        // A validated map never binds an unwired partition; this is
        // structural corruption, answered rather than dropped.
        emit_reject(router, protocol, corr_id, conn_id, KV_RESULT_INTERNAL);
        return DISPATCH_DROPPED;
    }

    let use_consensus = is_write && proposal_port >= 0;
    let lin_read = !is_write
        && (router.lin_reads != 0 || consistency == REQ_LINEARIZABLE)
        && router.lin_read_out >= 0
        && router.read_release_in >= 0;
    let path = if use_consensus {
        PATH_CONSENSUS
    } else if lin_read {
        PATH_FENCE
    } else {
        PATH_DIRECT
    };

    // Per-conn ordering barrier.
    //
    // Reordering protocols (redis): only serialize a genuine hazard — a
    // same-conn op on a DIFFERENT path that shares a key (or where either
    // side is a wildcard). Independent ops (different keys) pipeline
    // freely and the anchor's reorder buffer restores per-conn FIFO. This
    // preserves read-your-writes and no-reflecting-later-writes (same-key
    // cross-path ops still serialize) while recovering the throughput the
    // blanket barrier cost.
    //
    // Non-reordering protocols: keep the conservative barrier — any path
    // transition while ops are outstanding blocks, because their anchors
    // assume responses arrive already in issue order.
    let conn_key = fence_key(protocol, conn_id);
    if proto_reorders(protocol) {
        let conflict = router.inflight.iter().any(|e| {
            e.conn == conn_id
                && e.proto == protocol
                && e.path != path
                && (e.wildcard || wildcard || e.key_hash == key_hash)
        });
        if conflict {
            return DISPATCH_BLOCKED;
        }
    } else if let Some(key) = conn_key {
        if router.conn_outstanding[key] > 0 && router.conn_path[key] != path {
            return DISPATCH_BLOCKED;
        }
    }

    // Remember corr → (proto, conn, path) so the response path can pick
    // the right output port and settle the ordering accounting. If the
    // table is full we drop the request — anchors will time out and
    // clients will retry. Reasonable for Phase 1; Phase 5 wires this
    // into quota_manager.
    let mut help_key = [0u8; HELP_KEY_MAX];
    // For a rewritten scan the body lives in `scan_buf` and `body_len`
    // is the REWRITTEN length — slicing `req` with it runs past the
    // request. Scans have no single provable key anyway (helping is
    // for point ops), so they stash nothing. The first version sliced
    // unconditionally and panicked the router on every ordered-mode
    // range scan, which took down exactly the two-range graphs.
    let help_key_len = if body_in_scan_buf {
        0
    } else {
        match op_key_bytes(op, &req[body_off..body_off + body_len]) {
            Some(k) if k.len() <= HELP_KEY_MAX => {
                help_key[..k.len()].copy_from_slice(k);
                k.len() as u8
            }
            _ => 0,
        }
    };
    let mut meta = InflightMeta {
        proto: protocol,
        conn: conn_id,
        path,
        key_hash,
        wildcard,
        scan,
        scan_range,
        scan_epoch: router.routing_epoch as u8,
        scan_next,
        help_key,
        help_key_len,
        id_tenant,
        id_database,
        id_keyspace,
        deadline_ms: unsafe { dev_millis(&*router.syscalls) }.wrapping_add(INFLIGHT_TIMEOUT_MS),
    };
    if router.inflight.insert(corr_id, meta).is_err() {
        return DISPATCH_DROPPED;
    }

    // Step 2: build MSG_KV_COMMAND in router.scratch. The head layout
    // (kpg + §23 identity) is owned by wire::KvCommandHead.
    let cmd_payload_len = wire::KvCommandHead::LEN + body_len;
    if cmd_payload_len > u16::MAX as usize {
        // Roll back the inflight slot we reserved above.
        let _ = router.inflight.remove(corr_id);
        return DISPATCH_DROPPED;
    }
    let cmd_total = 3 + cmd_payload_len;
    if cmd_total > router.scratch.len() {
        let _ = router.inflight.remove(corr_id);
        return DISPATCH_DROPPED;
    }

    let out = &mut router.scratch[..cmd_total];
    out[0] = MSG_KV_COMMAND;
    out[1] = (cmd_payload_len & 0xFF) as u8;
    out[2] = ((cmd_payload_len >> 8) & 0xFF) as u8;
    // Mode 0: KPG_ID_DEFAULT. Mode 1: the fenced lookup's binding
    // (identical bytes under the single-partition default map).
    wire::KvCommandHead {
        corr_id,
        kpg_id,
        conn_id,
        consistency,
        op,
        tenant: id_tenant,
        database: id_database,
        keyspace: id_keyspace,
        // Not stamped by the router: per-write commit timestamps are
        // assigned by the worker from in-band replicated leases (CDC
        // RFC A-1), in committed-log order. A non-zero value here is a
        // proposer-side override the worker honours verbatim.
        commit_ts: 0,
        body_len: body_len as u16,
    }
    .encode(&mut out[3..]);
    let p = 3 + wire::KvCommandHead::LEN;
    if body_in_scan_buf {
        out[p..p + body_len].copy_from_slice(&scan_buf[..body_len]);
    } else {
        out[p..p + body_len].copy_from_slice(&req[body_off..body_off + body_len]);
    }

    if use_consensus {
        // Build the MSG_CLIENT_PROPOSAL envelope for clustor's
        // gateway.client_requests input. Payload shape:
        //   [conn_id:u16 LE][LATTICE_ENTRY_TAG:u8][kv_command_payload…]
        // (conn ids are u16 on every clustor client surface; lattice's
        // own conn ids are u8-ranged, so the high byte is zero.)
        // where the body is the MSG_KV_COMMAND PAYLOAD bytes
        // (cmd_payload_len bytes from offset 3 of router.scratch).
        // The 3-byte envelope prefix on the outer MSG_CLIENT_PROPOSAL
        // wraps it back up for the channel write.
        //
        // The gateway strips `conn_id` and frames `[corr_id:u64][body]`;
        // consensus strips the correlation tag, so the Raft entry body
        // is exactly `[LATTICE_ENTRY_TAG][kv_command_payload…]`. The tag
        // is what keeps byte 0 out of clustor's marker space — see
        // `wire::LATTICE_ENTRY_TAG` for why omitting it removes the node
        // from its own voter set on the 460th write.
        let cp_payload_len = 3 + cmd_payload_len;
        let cp_total = 3 + cp_payload_len;
        if cp_payload_len > u16::MAX as usize || cp_total > router.scratch.len() {
            // ANSWER it. This used to drop silently, which is
            // indistinguishable from a lost message and strands any
            // caller without its own timeout — every other refusal in
            // this router emits a typed result, and there is no reason
            // for these two to be the exception.
            let _ = router.inflight.remove(corr_id);
            router.m_proposal_drops = router.m_proposal_drops.wrapping_add(1);
            emit_reject(router, protocol, corr_id, conn_id, KV_RESULT_INTERNAL);
            return DISPATCH_DROPPED;
        }
        // Lay out the proposal envelope into a stack buffer; we can't
        // reuse router.scratch because the MSG_KV_COMMAND payload
        // lives there and we need it for the fallback if the write
        // races backpressure.
        let mut cp_buf = [0u8; SCRATCH_BUF_SIZE];
        cp_buf[0] = MSG_CLIENT_PROPOSAL;
        cp_buf[1] = (cp_payload_len & 0xFF) as u8;
        cp_buf[2] = ((cp_payload_len >> 8) & 0xFF) as u8;
        cp_buf[3..5].copy_from_slice(&u16::from(conn_id).to_le_bytes());
        cp_buf[5] = LATTICE_ENTRY_TAG;
        // Copy MSG_KV_COMMAND payload (everything after the 3-byte
        // outer header) into the proposal body.
        cp_buf[6..6 + cmd_payload_len].copy_from_slice(&router.scratch[3..3 + cmd_payload_len]);
        let mut cp_tag_buf = [0u8; SCRATCH_BUF_SIZE];
        if !write_envelope_maybe_tagged(
            sys,
            proposal_port,
            &mut cp_buf[..],
            cp_total,
            proposal_tag,
            &mut cp_tag_buf[..],
        ) {
            // Same reasoning as above: backpressure on the proposal
            // port is a retryable condition the caller can act on, and
            // silence is not.
            let _ = router.inflight.remove(corr_id);
            router.m_proposal_drops = router.m_proposal_drops.wrapping_add(1);
            emit_reject(router, protocol, corr_id, conn_id, types::KV_RESULT_QUOTA);
            return DISPATCH_DROPPED;
        }
        note_dispatched(
            router,
            conn_key,
            PATH_CONSENSUS,
            scan_range as usize,
            is_write,
            dispatch_key_hash,
        );
        return DISPATCH_OK;
    }

    // Phase 5 read split: a linearizable read takes the ReadIndex fence
    // (stash the built command; submit MSG_CLIENT_READ_REQUEST; forward
    // to the worker only on MSG_CLIENT_READ_RESPONSE). Degrades — stash
    // full, command oversize, fence backpressured — fall through to the
    // snapshot path (the request still gets an answer, just unfenced;
    // the inflight meta is re-pointed at PATH_DIRECT so the ordering
    // accounting matches where the op actually went).
    if lin_read && cmd_total <= LIN_READ_BUF {
        let mut slot: Option<usize> = None;
        for (i, s) in router.pending_lin.iter().enumerate() {
            if s.state == LIN_FREE {
                slot = Some(i);
                break;
            }
        }
        if let Some(i) = slot {
            // Submit the fence FIRST — only stash once the request is
            // actually in flight, so a backpressured fence degrades
            // cleanly instead of leaking a slot.
            // In fanout mode the probe rides a `partition_demux` to the
            // owning partition's consensus instance, so it carries the
            // partition tag the demux strips; the grant needs no tag —
            // it echoes `corr_id`, which is attribution enough for the
            // fanned-in `read_release` stream.
            let mut rr = [0u8; 3 + 10];
            let tagged = router.partition_fanout != 0;
            let plen: usize = if tagged { 10 } else { 8 };
            rr[0] = MSG_CLIENT_READ_REQUEST;
            rr[1] = plen as u8;
            rr[2] = 0;
            if tagged {
                rr[3..5].copy_from_slice(&kpg_id.to_le_bytes());
                rr[5..13].copy_from_slice(&corr_id.to_le_bytes());
            } else {
                rr[3..11].copy_from_slice(&corr_id.to_le_bytes());
            }
            if write_envelope_raw(sys, router.lin_read_out, &mut rr[..], 3 + plen) {
                // Admitted a ReadIndex-fenced linearizable read (before the
                // pending_lin sub-borrow below).
                router.m_lin_reads = router.m_lin_reads.wrapping_add(1);
                router.hold_seq = router.hold_seq.wrapping_add(1).max(1);
                let seq = router.hold_seq;
                let p = &mut router.pending_lin[i];
                p.corr_id = corr_id;
                p.seq = seq;
                p.len = cmd_total as u16;
                p.state = LIN_AWAITING;
                p.proto = protocol;
                p.conn = conn_id;
                p.fence_partition = kpg_id;
                p.deadline_ms = dev_millis(sys) + LIN_STASH_TIMEOUT_MS;
                p.buf[..cmd_total].copy_from_slice(&router.scratch[..cmd_total]);
                note_dispatched(
                    router,
                    conn_key,
                    PATH_FENCE,
                    scan_range as usize,
                    is_write,
                    dispatch_key_hash,
                );
                return DISPATCH_OK;
            }
        }
        // Fence unavailable → this op is now a DIRECT-path snapshot
        // read; fix the accounting before falling through.
        meta.path = PATH_DIRECT;
        let _ = router.inflight.remove(corr_id);
        if router.inflight.insert(corr_id, meta).is_err() {
            return DISPATCH_DROPPED;
        }
    }

    let mut kv_tag_buf = [0u8; SCRATCH_BUF_SIZE];
    if !write_envelope_maybe_tagged(
        sys,
        kv_port,
        &mut router.scratch[..],
        cmd_total,
        kv_tag,
        &mut kv_tag_buf[..],
    ) {
        // Backpressure: undo inflight and let the anchor's retry path
        // surface to the client. Phase 1 drops; Phase 5 would emit a
        // throttle event.
        let _ = router.inflight.remove(corr_id);
        return DISPATCH_DROPPED;
    }
    note_dispatched(
        router,
        conn_key,
        PATH_DIRECT,
        scan_range as usize,
        is_write,
        dispatch_key_hash,
    );
    DISPATCH_OK
}

/// One observation for a range's Space-Saving table (§12.4 hot key).
///
/// Space-Saving proper: a hit increments; a miss replaces the SMALLEST
/// slot and inherits its count. Inheriting is what bounds the error
/// and what makes every count an over-estimate — see `HOT_KEY_SLOTS`
/// for why that direction is the safe one.
fn note_hot_key(router: &mut RouterState, range_index: usize, hash: u64) {
    let hashes = &mut router.hot_key_hash[range_index];
    let counts = &mut router.hot_key_count[range_index];
    let mut min_at = 0usize;
    for i in 0..HOT_KEY_SLOTS {
        if hashes[i] == hash && counts[i] != 0 {
            counts[i] = counts[i].saturating_add(1);
            return;
        }
        if counts[i] < counts[min_at] {
            min_at = i;
        }
    }
    hashes[min_at] = hash;
    counts[min_at] = counts[min_at].saturating_add(1);
}

/// Busiest slot's share of a range's writes, as a percent. `0` means
/// "no evidence" — no writes seen — and the placement contract already
/// treats 0 as unmeasured rather than as proven-low concentration.
fn busiest_key_percent(router: &RouterState, range_index: usize) -> u64 {
    let total = router.m_range_writes[range_index];
    if total == 0 {
        return 0;
    }
    let mut max = 0u64;
    for i in 0..HOT_KEY_SLOTS {
        if router.hot_key_count[range_index][i] > max {
            max = router.hot_key_count[range_index][i];
        }
    }
    (max.saturating_mul(100) / total).min(100)
}

/// What a dispatched op offers the hot-key sampler.
enum HotKeys<'a> {
    /// A key-first op: exactly one key.
    Single(u64),
    /// A txn: its body, to be walked for every key it writes.
    Txn(&'a [u8]),
    /// Nothing samplable (a read, or a body that did not parse).
    None,
}

/// Sample EVERY key a txn's then-branch writes into the hot-key table.
///
/// Written as a deliberate twin of `route_txn_ordered`'s walk rather
/// than a shared helper: that function must REJECT a malformed body
/// (routing on a half-parsed txn would send bytes to the wrong range),
/// while this one must simply stop sampling. Merging them would give
/// one caller the other's failure mode. They share a body layout, not
/// a purpose, and the layout is the thing to keep in step.
///
/// Only the THEN branch is sampled. An else-branch op did not run, and
/// counting a write that never happened would invent load.
fn sample_txn_keys(router: &mut RouterState, range_index: usize, body: &[u8]) {
    let mut at = 0usize;
    let take_u16 = |b: &[u8], at: &mut usize| -> Option<u16> {
        let v = u16::from_le_bytes([*b.get(*at)?, *b.get(*at + 1)?]);
        *at += 2;
        Some(v)
    };
    let Some(cmp_count) = take_u16(body, &mut at) else {
        return;
    };
    for _ in 0..cmp_count {
        at += 1;
        let Some(klen) = take_u16(body, &mut at) else {
            return;
        };
        at += klen as usize + 8;
    }
    let Some(op_count) = take_u16(body, &mut at) else {
        return;
    };
    for _ in 0..op_count {
        let Some(&inner_op) = body.get(at) else {
            return;
        };
        at += 1;
        let Some(blen) = take_u16(body, &mut at) else {
            return;
        };
        let Some(inner_body) = body.get(at..at + blen as usize) else {
            return;
        };
        at += blen as usize;
        if op_is_write(inner_op) {
            if let Some(key) = op_key_bytes(inner_op, inner_body) {
                let h = fnv1a64(key);
                note_hot_key(router, range_index, h);
            }
        }
    }
}

/// Ordering accounting for a successfully dispatched op.
unsafe fn note_dispatched(
    router: &mut RouterState,
    conn_key: Option<usize>,
    path: u8,
    range_index: usize,
    is_write: bool,
    key_hash: HotKeys<'_>,
) {
    // Single chokepoint for every successfully dispatched op (consensus /
    // fenced-read / direct paths all funnel through here).
    router.m_routed = router.m_routed.wrapping_add(1);
    // §12.4's load signal at its only honest source. The router is the
    // one place that sees every write AND the range it was routed to;
    // anything downstream has already lost the range, and anything
    // upstream has not yet chosen it. Counted here, a hot range is a
    // measurement rather than an inference from key growth — which is
    // all the placement advisor can manage on its own, and which
    // cannot see overwrites at all.
    if is_write && range_index < MAX_TRACKED_RANGES {
        router.m_range_writes[range_index] = router.m_range_writes[range_index].wrapping_add(1);
        match key_hash {
            HotKeys::Single(h) => note_hot_key(router, range_index, h),
            // A txn's keys are all of them, not one of them: sampling
            // a single key would UNDER-report concentration, and
            // under-reporting is what lets a genuinely unsplittable
            // hot key be recommended for a split it cannot benefit
            // from — §12.4's named mistake, demonstrated live before
            // this existed.
            HotKeys::Txn(body) => sample_txn_keys(router, range_index, body),
            HotKeys::None => {}
        }
    }
    if let Some(key) = conn_key {
        router.conn_outstanding[key] = router.conn_outstanding[key].saturating_add(1);
        router.conn_path[key] = path;
        if !router.syscalls.is_null() {
            router.conn_deadline_ms[key] = dev_millis(&*router.syscalls) + CONN_ORDER_TIMEOUT_MS;
        }
    }
}

/// See `conn_deadline_ms`. Generous: an order of magnitude above the
/// slowest healthy op (quorum write + fence, tens of ms).
const CONN_ORDER_TIMEOUT_MS: u64 = 2_000;

/// Router-originated correlation ids live in their own half of the u64
/// space. Anchor corrs count up from 1; a collision would cross-wire a
/// help reply into a client op.
const HELP_CORR_BASE: u64 = 0x8000_0000_0000_0000;

#[derive(Clone, Copy)]
struct Help {
    in_use: bool,
    phase: u8,
    txn_id: u128,
    /// The home range id, echoed verbatim into the RESOLVE body. Not
    /// used for routing: the home PARTITION comes from routing the
    /// record key `[KS_TXN_RECORD][txn_id]` through the ordinary key
    /// map, which is also what pins the record's placement.
    home: [u8; 16],
    /// Partition whose intent blocked the caller.
    stuck_partition: u16,
    key: [u8; HELP_KEY_MAX],
    key_len: u8,
    corr: u64,
    /// Canonical identity (§23) inherited from the read that triggered
    /// the help; every courier command carries it so the record GET and
    /// the resolve write stay in the requester's tenant.
    id_tenant: u32,
    id_database: u32,
    id_keyspace: u32,
}

/// A TXN_PENDING reply came back: try to start a help.
///
/// The reader's own worker already handles the same-partition case
/// (`kv_store`'s local helping); what reaches here is the remainder —
/// an intent whose home record lives in ANOTHER partition, which the
/// worker must not reach for (workers do not route). The router can:
/// it routes for a living, and the reply body carries the txn id and
/// home while the inflight meta kept the blocking key.
///
/// The caller is still answered TXN_PENDING exactly as before — helps
/// run beside the reply path, not in it, so a full help table or a
/// still-pending home costs nobody anything. The caller's RETRY is
/// what the help pays off.
unsafe fn maybe_start_help(
    router: &mut RouterState,
    pending_body: &[u8],
    stuck_partition: u16,
    meta: &InflightMeta,
) {
    if pending_body.len() < 32 || meta.help_key_len == 0 {
        return;
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&pending_body[0..16]);
    let txn_id = u128::from_le_bytes(id);
    let mut home = [0u8; 16];
    home.copy_from_slice(&pending_body[16..32]);

    // One help per transaction: many readers hit the same intent.
    if router.helps.iter().any(|h| h.in_use && h.txn_id == txn_id) {
        return;
    }
    let Some(slot) = router.helps.iter().position(|h| !h.in_use) else {
        return;
    };

    // Route the record key like any other key; the answer is the home
    // partition. If it routes to the STUCK partition the worker there
    // would already have helped locally, so a same-partition answer
    // here means the map disagrees with the worker — leave it alone.
    let mut rk = [0u8; 4 + 16];
    rk[0..4].copy_from_slice(&KS_TXN_RECORD.to_be_bytes());
    rk[4..20].copy_from_slice(&txn_id.to_le_bytes());
    // Single-partition graphs cannot have a CROSS-partition intent;
    // the worker's local helping owns that case entirely.
    let Some(desc) = router.range_map.lookup(&rk) else {
        return;
    };
    // §11.3/§26.5: a record whose home range is in another cluster
    // domain cannot be helped from here — its own domain's router
    // couriers it. Helping locally would route the courier to a local
    // partition that does not hold the record.
    if desc.binding.cluster_id != router.local_cluster_domain {
        return;
    }
    let home_partition = desc.binding.partition_id;
    if home_partition == stuck_partition {
        return;
    }
    let (kv_port, kv_tag) = worker_port_tag(router, home_partition);
    if kv_port < 0 {
        return;
    }

    router.next_help_corr = router.next_help_corr.wrapping_add(1) | HELP_CORR_BASE;
    let corr = router.next_help_corr;
    let mut h = Help {
        in_use: true,
        phase: HELP_PHASE_GET_HOME,
        txn_id,
        home,
        stuck_partition,
        key: [0u8; HELP_KEY_MAX],
        key_len: meta.help_key_len,
        corr,
        id_tenant: meta.id_tenant,
        id_database: meta.id_database,
        id_keyspace: meta.id_keyspace,
    };
    h.key[..meta.help_key_len as usize]
        .copy_from_slice(&meta.help_key[..meta.help_key_len as usize]);

    // GET body: [key_len:u16][key]. The record is an ordinary value.
    let mut body = [0u8; 2 + 4 + 16];
    body[0..2].copy_from_slice(&(20u16).to_le_bytes());
    body[2..22].copy_from_slice(&rk);
    if !send_help_command(
        router,
        kv_port,
        kv_tag,
        corr,
        types::KV_OP_GET,
        &body,
        (meta.id_tenant, meta.id_database, meta.id_keyspace),
    ) {
        return;
    }
    router.helps[slot] = h;
    router.m_helps_started = router.m_helps_started.wrapping_add(1);
}

/// A reply addressed to a help corr. Returns true when consumed.
unsafe fn on_help_reply(router: &mut RouterState, corr: u64, result: u8, body: &[u8]) -> bool {
    let Some(slot) = router.helps.iter().position(|h| h.in_use && h.corr == corr) else {
        return false;
    };
    let h = router.helps[slot];
    match h.phase {
        HELP_PHASE_GET_HOME => {
            router.helps[slot].in_use = false;
            if result != types::KV_RESULT_OK {
                // No record: nothing to carry. The intent's fate stays
                // with the coordinator or a later reader.
                return true;
            }
            let Some(record) = txn::TransactionRecord::decode(body) else {
                return true;
            };
            let committed = match record.status {
                // Only a DECISION travels. Pending/Staging are not
                // decisions, and helping them would be deciding.
                txn::TxnStatus::Committed => true,
                txn::TxnStatus::Aborted => false,
                _ => return true,
            };
            let (stuck_port, stuck_tag) = worker_port_tag(router, h.stuck_partition);
            if stuck_port < 0 {
                return true;
            }
            // RESOLVE body:
            // [txn:16][home:16][epoch:4][committed:1][commit_ts:8]
            // [count:u16][klen:u16][key] — the one key we know blocked
            // a caller. Other intents of the same transaction resolve
            // through their own readers or the coordinator; carrying
            // one decision for one key is the whole job here.
            let klen = h.key_len as usize;
            let mut body2 = [0u8; 16 + 16 + 4 + 1 + 8 + 2 + 2 + HELP_KEY_MAX];
            let mut p = 0;
            body2[p..p + 16].copy_from_slice(&h.txn_id.to_le_bytes());
            p += 16;
            body2[p..p + 16].copy_from_slice(&h.home);
            p += 16;
            body2[p..p + 4].copy_from_slice(&record.epoch.to_le_bytes());
            p += 4;
            body2[p] = committed as u8;
            p += 1;
            body2[p..p + 8].copy_from_slice(&record.provisional_commit_timestamp.to_le_bytes());
            p += 8;
            body2[p..p + 2].copy_from_slice(&1u16.to_le_bytes());
            p += 2;
            body2[p..p + 2].copy_from_slice(&(klen as u16).to_le_bytes());
            p += 2;
            body2[p..p + klen].copy_from_slice(&h.key[..klen]);
            p += klen;

            router.next_help_corr = router.next_help_corr.wrapping_add(1) | HELP_CORR_BASE;
            let corr2 = router.next_help_corr;
            if send_help_command(
                router,
                stuck_port,
                stuck_tag,
                corr2,
                types::KV_OP_TXN_RESOLVE,
                &body2[..p],
                (h.id_tenant, h.id_database, h.id_keyspace),
            ) {
                router.helps[slot] = Help {
                    in_use: true,
                    phase: HELP_PHASE_RESOLVING,
                    corr: corr2,
                    ..h
                };
            }
            true
        }
        _ => {
            // RESOLVE answered. Whatever it said, this help is spent.
            router.helps[slot].in_use = false;
            if result == types::KV_RESULT_INTEGER || result == types::KV_RESULT_OK {
                router.m_helps_resolved = router.m_helps_resolved.wrapping_add(1);
            }
            true
        }
    }
}

/// Frame a router-originated MSG_KV_COMMAND. Help ops carry kpg in the
/// envelope like any routed command; consistency 0 (serializable) —
/// helping needs no fence, the record IS the authority.
unsafe fn send_help_command(
    router: &mut RouterState,
    kv_port: i32,
    kv_tag: Option<u16>,
    corr: u64,
    op: u8,
    body: &[u8],
    identity: (u32, u32, u32),
) -> bool {
    let sys_ptr = router.syscalls;
    if sys_ptr.is_null() {
        return false;
    }
    let sys = &*sys_ptr;
    // The same command head the ordinary path builds — owned by
    // wire::KvCommandHead. Router-originated: conn 0, serializable, and
    // kpg 0 (the help routes to an explicit port, not by kpg).
    let payload_len = wire::KvCommandHead::LEN + body.len();
    let total = 3 + payload_len;
    if total > router.scratch.len() || payload_len > u16::MAX as usize {
        return false;
    }
    let out = &mut router.scratch[..total];
    out[0] = MSG_KV_COMMAND;
    out[1] = (payload_len & 0xFF) as u8;
    out[2] = ((payload_len >> 8) & 0xFF) as u8;
    wire::KvCommandHead {
        corr_id: corr,
        kpg_id: 0,
        conn_id: 0,
        consistency: 0,
        op,
        tenant: identity.0,
        database: identity.1,
        keyspace: identity.2,
        commit_ts: 0,
        body_len: body.len() as u16,
    }
    .encode(&mut out[3..]);
    let p = 3 + wire::KvCommandHead::LEN;
    out[p..p + body.len()].copy_from_slice(body);
    let mut tag_buf = [0u8; SCRATCH_BUF_SIZE];
    write_envelope_maybe_tagged(
        sys,
        kv_port,
        &mut router.scratch[..],
        total,
        kv_tag,
        &mut tag_buf[..],
    )
}

/// How long an op may sit in the inflight table before it is answered
/// fail-closed. An order of magnitude above the slowest healthy op
/// (a quorum write plus fence is tens of ms), so this only ever fires
/// for a completion signal that is never coming.
const INFLIGHT_TIMEOUT_MS: u64 = 5_000;

/// Answer every in-flight op whose deadline passed.
///
/// `sweep_conn_deadlines` below force-settles the ORDERING state for a
/// wedged conn, which unblocks the queue but tells the CALLER nothing —
/// it goes on waiting for a reply that no longer has anything behind
/// it. It also only understands four protocols, because it recovers
/// proto/conn from a key encoding that predates the internal callers,
/// so PROTO_INTERNAL_SQL and PROTO_INTERNAL_DATA were never swept at
/// all.
///
/// This sweep answers instead. `emit_reject` routes by the protocol
/// byte held in the entry, so every caller is covered — including the
/// data surface, where `KV_RESULT_LIN_BOUND` becomes
/// `DataRetryClass::UnavailableAuthority`, which is the truthful
/// classification for "this node is not the leader".
///
/// ## Verified, after two wrong readings
///
/// The failure it prevents is real and measured. With this sweep, a
/// write sent to a FOLLOWER is refused at ~5.0s with
/// `-CLUSTERDOWN`. Without it, the connection is CLOSED at ~8.6s with
/// no reply at all — the caller gets a hangup, and §14.8 is explicit
/// that "network loss cannot be reduced to FIFO hangup" because a
/// hangup is not an outcome.
///
/// It took two bad readings to establish that. First the probe used a
/// 5s read timeout and saw the 5.0s refusal land just past its own
/// deadline, so the working path looked silent. Then the control
/// counted the 8.6s EOF as "an answer" — `read()` returning zero bytes
/// is a close, not a reply — so disabling the sweep looked like a
/// no-op. Both errors were in the measurement, in opposite directions,
/// and each on its own was enough to draw the wrong conclusion.
///
/// `tests/cluster.rs::a_follower_answers_a_write_it_cannot_commit` now
/// rejects EOF explicitly and fails when this sweep is disabled.
unsafe fn sweep_inflight_deadlines(router: &mut RouterState) {
    if router.syscalls.is_null() || router.inflight.is_empty() {
        return;
    }
    let now = dev_millis(&*router.syscalls);
    // Collect first: `emit_reject` mutates the table, so it cannot run
    // while an iterator borrows it. Bounded by one sweep's worth.
    const MAX_PER_SWEEP: usize = 32;
    let mut expired: [(u64, u8, u8); MAX_PER_SWEEP] = [(0, 0, 0); MAX_PER_SWEEP];
    let mut n = 0;
    for (corr_id, meta) in router.inflight.iter_entries() {
        if n == MAX_PER_SWEEP {
            break;
        }
        if now >= meta.deadline_ms {
            expired[n] = (corr_id, meta.proto, meta.conn);
            n += 1;
        }
    }
    for &(corr_id, proto, conn) in &expired[..n] {
        // Fail closed. `emit_reject` removes the entry, so an op is
        // answered exactly once even if the real reply arrives later —
        // that late reply finds no entry and is dropped, which is the
        // same path a reply for a closed connection already takes.
        let _ = emit_reject(router, proto, corr_id, conn, KV_RESULT_LIN_BOUND);
        router.m_inflight_timeouts = router.m_inflight_timeouts.wrapping_add(1);
    }
}

/// Drain the gateway's reject feedback and answer the ops it refused.
///
/// The gateway checks leadership BEFORE assigning a correlation id, so
/// its reject can only name the CONN — there is no corr_id to match.
/// Correlating by conn is still exact for the case that matters:
///
/// - NOT_LEADER is a property of the NODE, not of one proposal. Every
///   consensus-path op this conn has in flight was proposed to the same
///   non-leader and none will ever complete, so all of them are
///   answered at once. Sibling rejects for ops failed here find their
///   entries already gone, which is the same late-arrival path a reply
///   for a closed connection takes.
/// - Any other reject status is per-proposal (throttle, oversize).
///   Rejects leave the gateway in the order proposals arrived — both
///   directions are FIFO channels — so the OLDEST consensus-path op
///   for that conn is the one refused, and only it is answered.
///
/// Without this the rejection is dropped and the caller waits out the
/// 5s inflight sweep. tests/cluster.rs pins the difference: a write to
/// a follower must now fail in milliseconds, not at the timeout.
unsafe fn drain_gateway_rejects(router: &mut RouterState) -> bool {
    if router.gateway_reject_in < 0 || router.syscalls.is_null() {
        return false;
    }
    let sys = &*router.syscalls;
    let mut progressed = false;
    // Bounded: a burst of rejects (a pipelined client on a follower)
    // must not starve the ingress drains this shares a step with.
    for _ in 0..16 {
        let poll = (sys.channel_poll)(router.gateway_reject_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            break;
        }
        // `gateway/surface.rs::send_response` framing — conn-tagged,
        // NOT the standard 3-byte envelope:
        //   [conn_id:u16 LE][msg_type:u8][len:u16 LE][payload…]
        // Lattice conn ids are u8-ranged, so the u16 narrows losslessly.
        let mut hdr = [0u8; 5];
        if (sys.channel_read)(router.gateway_reject_in, hdr.as_mut_ptr(), 5) < 5 {
            break;
        }
        let conn_id = (u16::from_le_bytes([hdr[0], hdr[1]]) & 0xFF) as u8;
        let msg_type = hdr[2];
        let plen = u16::from_le_bytes([hdr[3], hdr[4]]) as usize;
        let mut payload = [0u8; 64];
        let take = plen.min(payload.len());
        if take > 0
            && ((sys.channel_read)(router.gateway_reject_in, payload.as_mut_ptr(), take) as usize)
                < take
        {
            break;
        }
        progressed = true;
        // This port also carries the gateway's other client responses
        // (admin, read grants for graphs that route them here). Only
        // rejects are ours to act on; the rest is drained so the ring
        // never fills — a full ring would make the gateway drop the
        // NEXT reject, and this port exists so rejects are not dropped.
        if msg_type != MSG_CLIENT_REJECT || take < 2 {
            continue;
        }
        let status = payload[0];
        // payload[1] is the believed leader id when status is
        // NOT_LEADER — the re-dial hint a failover-capable client
        // needs. Not yet surfaced downstream: MSG_KV_RESPONSE has no
        // field that could carry it honestly, and inventing one is
        // lattice_data_client failover work, not this fix.

        if status == CLIENT_REJECT_NOT_LEADER {
            // Fail every consensus-path op this conn has in flight.
            const MAX_PER_REJECT: usize = 32;
            let mut victims: [(u64, u8); MAX_PER_REJECT] = [(0, 0); MAX_PER_REJECT];
            let mut n = 0;
            for (corr, meta) in router.inflight.iter_entries() {
                if n == MAX_PER_REJECT {
                    break;
                }
                if meta.conn == conn_id && meta.path == PATH_CONSENSUS {
                    victims[n] = (corr, meta.proto);
                    n += 1;
                }
            }
            for &(corr, proto) in &victims[..n] {
                if emit_reject(router, proto, corr, conn_id, KV_RESULT_LIN_BOUND) {
                    op_settled(router, proto, conn_id);
                }
                router.m_not_leader_rejects = router.m_not_leader_rejects.wrapping_add(1);
            }
        } else {
            // Per-proposal refusal: answer the oldest consensus-path
            // op for this conn, retryably.
            let mut oldest: Option<(u64, u8, u64)> = None;
            for (corr, meta) in router.inflight.iter_entries() {
                if meta.conn == conn_id && meta.path == PATH_CONSENSUS {
                    match oldest {
                        Some((_, _, d)) if d <= meta.deadline_ms => {}
                        _ => oldest = Some((corr, meta.proto, meta.deadline_ms)),
                    }
                }
            }
            if let Some((corr, proto, _)) = oldest {
                if emit_reject(router, proto, corr, conn_id, types::KV_RESULT_QUOTA) {
                    op_settled(router, proto, conn_id);
                }
            }
        }
    }
    progressed
}

/// Recovery sweep: force-settle conns whose outstanding ops missed
/// their deadline (completion signal structurally lost). Runs once per
/// step; 1 KiB linear scan.
unsafe fn sweep_conn_deadlines(router: &mut RouterState) {
    if router.syscalls.is_null() {
        return;
    }
    let now = dev_millis(&*router.syscalls);
    for key in 0..FENCE_KEYS {
        if router.conn_outstanding[key] > 0 && now >= router.conn_deadline_ms[key] {
            router.conn_outstanding[key] = 0;
            // Replay whatever is parked (proto/conn recoverable from the
            // key encoding).
            let proto = match key / 256 {
                0 => PROTO_ETCD,
                1 => PROTO_REDIS,
                2 => PROTO_MEMCACHED,
                _ => PROTO_MEMCACHED_UDP,
            };
            op_settled_replay_only(router, proto, (key % 256) as u8);
        }
    }
}

/// The replay half of `op_settled`, for callers that already zeroed the
/// outstanding count.
unsafe fn op_settled_replay_only(router: &mut RouterState, proto: u8, conn: u8) {
    let Some(key) = fence_key(proto, conn) else {
        return;
    };
    loop {
        let mut best: Option<usize> = None;
        let mut best_seq = u32::MAX;
        for (i, h) in router.held.iter().enumerate() {
            if h.seq != 0 && h.proto == proto && h.conn == conn && h.seq < best_seq {
                best_seq = h.seq;
                best = Some(i);
            }
        }
        let Some(i) = best else { return };
        let len = router.held[i].len as usize;
        let mut req = [0u8; HOLD_BUF];
        req[..len].copy_from_slice(&router.held[i].buf[..len]);
        if dispatch_parsed(router, proto, &req[..len]) == DISPATCH_BLOCKED {
            return;
        }
        router.held[i] = HeldRequest::empty();
        router.held_count[key] = router.held_count[key].saturating_sub(1);
    }
}

// ── Linearizable-read fence releases + session-order replay ──────────

/// Park a request whose conn has a fence in flight. On pool overflow
/// the request is answered with a fail-closed KV_RESULT_LIN_BOUND
/// error — bounded state may cost availability, never ordering.
unsafe fn hold_request(router: &mut RouterState, protocol: u8, req: &[u8]) {
    let conn = req[13];
    if req.len() > HOLD_BUF {
        // Oversize op during a barrier window: fail closed rather than
        // reorder (see HOLD_BUF).
        router.hold_overflows = router.hold_overflows.saturating_add(1);
        let corr = u64::from_le_bytes([
            req[0], req[1], req[2], req[3], req[4], req[5], req[6], req[7],
        ]);
        emit_lin_bound_error(router, protocol, corr, conn);
        return;
    }
    for h in router.held.iter_mut() {
        if h.seq == 0 {
            router.hold_seq = router.hold_seq.wrapping_add(1).max(1);
            h.seq = router.hold_seq;
            h.len = req.len() as u16;
            h.proto = protocol;
            h.conn = conn;
            h.buf[..req.len()].copy_from_slice(req);
            if let Some(key) = fence_key(protocol, conn) {
                router.held_count[key] = router.held_count[key].saturating_add(1);
            }
            return;
        }
    }
    router.hold_overflows = router.hold_overflows.saturating_add(1);
    let corr = u64::from_le_bytes([
        req[0], req[1], req[2], req[3], req[4], req[5], req[6], req[7],
    ]);
    emit_lin_bound_error(router, protocol, corr, conn);
}

/// Synthesize a MSG_KV_RESPONSE carrying KV_RESULT_LIN_BOUND straight
/// to the request's protocol port — the anchor maps it to its
/// protocol's unavailability error (redis: CLUSTERDOWN). Best-effort
/// on backpressure: the alternative is an unbounded retry queue for
/// error paths, and a dropped error degrades to the client timeout we
/// had before.
unsafe fn emit_lin_bound_error(router: &mut RouterState, protocol: u8, corr_id: u64, conn_id: u8) {
    if emit_reject(router, protocol, corr_id, conn_id, KV_RESULT_LIN_BOUND) {
        router.lin_rejects = router.lin_rejects.saturating_add(1);
    }
}

/// Shared fail-closed reject synth: an empty-body MSG_KV_RESPONSE with
/// `result` straight to the request's protocol port (the anchor maps
/// result → protocol error: LIN_BOUND → CLUSTERDOWN, DIRTY_EPOCH →
/// MOVED). Best-effort on backpressure — see `emit_lin_bound_error`.
/// Returns whether a reject was actually synthesized (false when the
/// syscall table or destination port is unavailable).
unsafe fn emit_reject(
    router: &mut RouterState,
    protocol: u8,
    corr_id: u64,
    conn_id: u8,
    result: u8,
) -> bool {
    emit_reject_with_body(router, protocol, corr_id, conn_id, result, &[])
}

/// Longest body a router-originated refusal carries: the cross-domain
/// redirect's `[cluster_domain_id:u32]`. Every other refusal is empty.
const REJECT_BODY_MAX: usize = 4;

/// The refusal path, with an optional typed body — a redirect payload
/// like `KV_RESULT_CROSS_DOMAIN`'s target domain. `emit_reject` is this
/// with an empty body, so every existing caller is byte-for-byte
/// unchanged.
unsafe fn emit_reject_with_body(
    router: &mut RouterState,
    protocol: u8,
    corr_id: u64,
    conn_id: u8,
    result: u8,
    body: &[u8],
) -> bool {
    let sys_ptr = router.syscalls;
    if sys_ptr.is_null() {
        return false;
    }
    let sys = &*sys_ptr;
    if body.len() > REJECT_BODY_MAX {
        return false;
    }
    let dest = match protocol {
        PROTO_ETCD => router.etcd_out,
        PROTO_REDIS => router.redis_out,
        PROTO_MEMCACHED => router.memcached_out,
        PROTO_MEMCACHED_UDP => router.memcached_udp_out,
        PROTO_INTERNAL_WATCH => router.internal_watch_out,
        PROTO_INTERNAL_SQL => router.compute_out,
        types::PROTO_INTERNAL_DATA => router.compute_out,
        types::PROTO_INTERNAL_LIFECYCLE => router.internal_lifecycle_out,
        types::PROTO_INTERNAL_JOB => router.internal_job_out,
        _ => -1,
    };
    if dest < 0 {
        return false;
    }
    const RESP_HEAD: usize = wire::KvResponseHead::LEN;
    // The fence tail is part of the envelope, so a rejection carries it
    // too. Uniform shape is the point: a consumer that had to work out
    // whether the tail is there before reading it would need a rule for
    // deciding, and the only rule available — payload length — is the
    // same thing it would be trying to check. The body (if any) sits
    // between the head and the tail, exactly where a full response's
    // body does.
    let resp_len = RESP_HEAD + body.len() + wire::FenceTail::LEN;
    let mut env = [0u8; 3 + RESP_HEAD + REJECT_BODY_MAX + wire::KV_RESPONSE_FENCE_TAIL_LEN];
    env[0] = MSG_KV_RESPONSE;
    env[1] = resp_len as u8;
    env[2] = (resp_len >> 8) as u8;
    // revision 0; body_len is the redirect payload length (if any).
    wire::KvResponseHead {
        corr_id,
        conn_id,
        result,
        revision: 0,
        body_len: body.len() as u16,
    }
    .encode(&mut env[3..]);
    if !body.is_empty() {
        env[3 + RESP_HEAD..3 + RESP_HEAD + body.len()].copy_from_slice(body);
    }
    // The rejected request never reached a range, so there is no group
    // that answered and no durability achieved: source_id and the
    // durability byte stay at their zero/weakest values. The applied
    // position IS reported — it is a property of this router's worker,
    // not of the request that was turned away, and a caller refreshing
    // its view after a rejection is exactly who needs it. A turned-away
    // request never resolved a partition, so partition 0's position (the
    // single-partition value) is the honest representative here.
    wire::FenceTail {
        applied_index: router.worker_applied_index[0],
        applied_term: router.worker_applied_term[0],
        source_id: 0,
        durability: 0x01, // Durability::Volatile
        catalog_generation: 0,
        commit_frontier: 0,
    }
    .encode(&mut env[3 + RESP_HEAD + body.len()..]);
    let _ = router.inflight.remove(corr_id);
    let total = 3 + resp_len;
    let _ = write_envelope_raw(sys, dest, &mut env[..], total);
    // Single chokepoint for fail-closed rejects (hold-pool overflow,
    // oversize held op, fence rejected/timed out, stale route).
    router.m_rejects = router.m_rejects.wrapping_add(1);
    true
}

/// An outstanding op for `(proto, conn)` finished (response forwarded,
/// or fail-closed error emitted) — settle the ordering accounting and,
/// once the conn drains to zero outstanding, replay its held requests
/// in arrival order. Replay stops when a replayed op BLOCKS again (it
/// dispatched onto a path and the next held op needs a different one) —
/// the remainder stays parked. Bounded: ≤ `HOLD_SLOTS` dispatches.
unsafe fn op_settled(router: &mut RouterState, proto: u8, conn: u8) {
    let Some(key) = fence_key(proto, conn) else {
        return;
    };
    router.conn_outstanding[key] = router.conn_outstanding[key].saturating_sub(1);
    if proto_reorders(proto) {
        // Under per-key dependency (task #17), only conflicting ops are
        // ever held — independent ops dispatch immediately and never
        // enter the pool, so the held set for a conn is a same-key /
        // wildcard dependent chain. A settle may have cleared the
        // conflict that blocked the chain's head, so replay it NOW even
        // though other independent ops are still in flight (count > 0).
        // FIFO replay preserves the dependent order.
        op_settled_replay_only(router, proto, conn);
        if router.conn_outstanding[key] > 0 && !router.syscalls.is_null() {
            router.conn_deadline_ms[key] = dev_millis(&*router.syscalls) + CONN_ORDER_TIMEOUT_MS;
        }
        return;
    }
    if router.conn_outstanding[key] > 0 {
        // Ops still in flight — refresh the recovery deadline so a
        // steadily-progressing conn never trips the valve.
        if !router.syscalls.is_null() {
            router.conn_deadline_ms[key] = dev_millis(&*router.syscalls) + CONN_ORDER_TIMEOUT_MS;
        }
        return;
    }
    // Dispatch failure during replay matches fresh-envelope semantics
    // under backpressure (request dropped, client retries) — a held
    // request is not more retryable than a fresh one. BLOCKED means the
    // previous replayed op claimed a different path: the rest stays
    // parked; the next settle resumes the replay.
    op_settled_replay_only(router, proto, conn);
}

/// Drain `read_release` (consensus.applied) and complete stashed
/// fenced reads. The stream interleaves read releases, LIN-BOUND
/// rejects, and per-entry write acks (ignored — lattice write
/// responses ride the worker path).
///
/// Completion is two-phase: a slot marked RELEASED/REJECTED retries
/// its forward/error across steps under backpressure, and only the
/// COMPLETED action lowers the session barrier (`fence_resolved`) —
/// held ops must never overtake the fenced read into the worker.
unsafe fn drain_read_release(router: &mut RouterState) {
    let sys_ptr = router.syscalls;
    if sys_ptr.is_null() || router.read_release_in < 0 {
        return;
    }
    let sys = &*sys_ptr;
    for _ in 0..8 {
        let poll = (sys.channel_poll)(router.read_release_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            break;
        }
        let mut hdr = [0u8; 3];
        if (sys.channel_read)(router.read_release_in, hdr.as_mut_ptr(), 3) < 3 {
            break;
        }
        let plen = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
        let mut body = [0u8; 64];
        let take = plen.min(body.len());
        if take > 0
            && ((sys.channel_read)(router.read_release_in, body.as_mut_ptr(), take) as usize) < take
        {
            break;
        }
        match hdr[0] {
            MSG_CLIENT_READ_RESPONSE | MSG_CLIENT_REJECT_INTERNAL if take >= 8 => {
                let corr = u64::from_le_bytes([
                    body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
                ]);
                // A grant carries `[corr_id:u64][fence_index:u64]`.
                // Consensus applied through `fence_index`; this worker
                // may not have. Move to LIN_FENCED and let the applied
                // -index check below decide when the read may be served.
                let explicit_index = if take >= 16 {
                    Some(u64::from_le_bytes([
                        body[8], body[9], body[10], body[11], body[12], body[13], body[14],
                        body[15],
                    ]))
                } else {
                    None
                };
                // A grant without an index cannot be fenced against
                // anything explicit. Rather than silently degrade to the
                // old release-on-grant behaviour — the false-absence bug
                // this closes — demand the worker be caught up to
                // everything it has told us about FOR THIS READ'S
                // PARTITION. `Copy` snapshot so the fence read below does
                // not alias the `pending_lin` mutable borrow.
                let applied = router.worker_applied_index;
                for p in router.pending_lin.iter_mut() {
                    if p.state == LIN_AWAITING && p.corr_id == corr {
                        if hdr[0] == MSG_CLIENT_READ_RESPONSE {
                            p.state = LIN_FENCED;
                            p.fence_index = explicit_index.unwrap_or_else(|| {
                                applied[p.fence_partition as usize % MAX_TRACKED_RANGES]
                            });
                        } else {
                            p.state = LIN_REJECTED;
                        }
                        break;
                    }
                }
            }
            _ => {} // write acks / unknown — consume and ignore
        }
    }

    // Complete resolved slots in SUBMISSION order (bounded; retries
    // survive backpressure and preserve order — a backpressured older
    // release blocks younger ones rather than being overtaken).
    loop {
        let mut best: Option<usize> = None;
        let mut best_seq = u32::MAX;
        for (i, p) in router.pending_lin.iter().enumerate() {
            // A LIN_FENCED slot is resolvable only once the worker has
            // applied through its fence index. Until then it is not a
            // candidate — and because completion runs in submission
            // order, it also blocks younger slots, which is what keeps
            // a caught-up younger read from overtaking an older one
            // still waiting on apply.
            let resolvable = match p.state {
                LIN_RELEASED | LIN_REJECTED => true,
                LIN_FENCED => {
                    router.worker_applied_index[p.fence_partition as usize % MAX_TRACKED_RANGES]
                        >= p.fence_index
                }
                _ => false,
            };
            if resolvable && p.seq < best_seq {
                best_seq = p.seq;
                best = Some(i);
            }
        }
        let Some(i) = best else { return };
        let (state, len, proto, conn, corr) = {
            let p = &router.pending_lin[i];
            (p.state, p.len as usize, p.proto, p.conn, p.corr_id)
        };
        if state == LIN_RELEASED || state == LIN_FENCED {
            let mut env = [0u8; LIN_READ_BUF];
            env[..len].copy_from_slice(&router.pending_lin[i].buf[..len]);
            // The released read goes to ITS OWN partition's worker —
            // via the tagged demux feed in fanout mode, exactly as the
            // unfenced dispatch would have sent it. Forwarding to the
            // direct `kv_out` would serve every fenced read from
            // partition 0's store, absence and all.
            let fence_partition = router.pending_lin[i].fence_partition;
            let (port, tag) = worker_port_tag(router, fence_partition);
            let mut tagged = [0u8; LIN_READ_BUF + 8];
            if !write_envelope_maybe_tagged(sys, port, &mut env[..len], len, tag, &mut tagged) {
                return; // retry next step, keeping order
            }
            // Forwarded to the worker: the op is STILL outstanding on
            // the fence path until its worker response settles it in
            // drain_kv_in — held ops must not overtake it.
            router.pending_lin[i] = PendingLinRead::empty();
        } else {
            // Fail closed: the client gets LinearizabilityUnavailable,
            // never a possibly-stale answer. Error emission is
            // best-effort (see emit_lin_bound_error); the op terminates
            // here, so settle its ordering slot now.
            emit_lin_bound_error(router, proto, corr, conn);
            router.pending_lin[i] = PendingLinRead::empty();
            op_settled(router, proto, conn);
        }
    }
}

// ── Partition-map update drain ────────────────────────────────────────

/// Drain the `map_update` input: bounded MAP_UPDATE_V1 slot-binding
/// batches (see `partition_map.rs`). An applied batch rebinds its slots
/// atomically and advances the router's expected map generation to the
/// batch max (monotonic) — slots the batch left behind then fence as
/// stale routes until covered. ANY invalid frame (wrong envelope type,
/// unknown version, malformed/oversize body) is dropped whole, counted
/// (`map_update_drops`), and logged once — fail closed, never partially
/// applied.
unsafe fn drain_map_update(router: &mut RouterState) {
    let sys_ptr = router.syscalls;
    if sys_ptr.is_null() || router.map_update_in < 0 {
        return;
    }
    let sys = &*sys_ptr;
    for _ in 0..4 {
        let poll = (sys.channel_poll)(router.map_update_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return;
        }
        let mut hdr = [0u8; 3];
        if (sys.channel_read)(router.map_update_in, hdr.as_mut_ptr(), 3) < 3 {
            return;
        }
        let plen = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
        let mut buf = [0u8; MAP_UPDATE_DRAIN_BUF];
        let take = plen.min(buf.len());
        if take > 0
            && ((sys.channel_read)(router.map_update_in, buf.as_mut_ptr(), take) as usize) < take
        {
            return;
        }
        // Oversize (plen > buf: body truncated → length check fails),
        // wrong envelope type, a frame kind that doesn't match the
        // declared routing mode, or an invalid frame all land here.
        // The frame KIND is the envelope type byte, never sniffed from
        // the payload (§11.2's declared-not-inferred rule).
        let applied = if take != plen {
            None
        } else if hdr[0] == MSG_MAP_UPDATE && router.routing_mode == ROUTING_MODE_FENCED {
            router.slot_map.apply_update(&buf[..plen])
        } else if hdr[0] == partition_map::MSG_SPAN_BARRIER
            && router.routing_mode == ROUTING_MODE_ORDERED
        {
            match partition_map::SpanBarrier::decode(&buf[..plen]) {
                Some(b) if b.set && router.barrier_set == 0 => {
                    router.barrier_start[..b.start.len()].copy_from_slice(b.start);
                    router.barrier_start_len = b.start.len() as u16;
                    router.barrier_end[..b.end.len()].copy_from_slice(b.end);
                    router.barrier_end_len = b.end.len() as u16;
                    router.barrier_set = 1;
                    Some(router.keyspace_routing.map_generation)
                }
                Some(b)
                    if !b.set
                        && router.barrier_set != 0
                        && b.start
                            == &router.barrier_start[..router.barrier_start_len as usize]
                        && b.end == &router.barrier_end[..router.barrier_end_len as usize] =>
                {
                    // A clear must name the exact span it set — a
                    // delayed clear from an abandoned operation must
                    // not lift a successor's barrier.
                    router.barrier_set = 0;
                    Some(router.keyspace_routing.map_generation)
                }
                _ => None,
            }
        } else if hdr[0] == MSG_RANGE_MAP_UPDATE && router.routing_mode == ROUTING_MODE_ORDERED {
            match router.range_map.apply_full_update(&buf[..plen]) {
                Some(max_generation) => {
                    // A replaced map retires every outstanding scan
                    // continuation: bump the routing epoch, so resumes
                    // minted under the old map fence as stale routes
                    // instead of resuming into renumbered ranges. The
                    // map's own epoch moves with the router's — the
                    // two are one fence.
                    router.routing_epoch = router.routing_epoch.wrapping_add(1);
                    router.range_map.routing_epoch = router.routing_epoch;
                    // Ranges bound to unreachable partitions fail
                    // closed per-op (INTERNAL reject) and loudly here.
                    if !ordered_partitions_reachable(router) {
                        let msg = b"[router] range_map binds an unwired partition";
                        dev_log(sys, 3, msg.as_ptr(), msg.len());
                    }
                    router.map_published = 1;
                    Some(max_generation)
                }
                None => None,
            }
        } else {
            None
        };
        match applied {
            Some(max_generation) => {
                if max_generation > router.keyspace_routing.map_generation {
                    router.keyspace_routing.map_generation = max_generation;
                }
            }
            None => {
                router.m_map_update_drops = router.m_map_update_drops.wrapping_add(1);
                if router.logged_map_update_drop == 0 {
                    router.logged_map_update_drop = 1;
                    let msg = b"[router] map_update frame dropped (bad version/frame)";
                    dev_log(sys, 3, msg.as_ptr(), msg.len());
                }
            }
        }
    }
}

// ── Reply drain (worker → anchor) ─────────────────────────────────────

unsafe fn drain_kv_in(router: &mut RouterState) -> bool {
    let sys_ptr = router.syscalls;
    if sys_ptr.is_null() || router.kv_in < 0 {
        return false;
    }
    let sys = &*sys_ptr;
    let poll = (sys.channel_poll)(router.kv_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    let n = (sys.channel_read)(router.kv_in, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    if hdr[0] == MSG_APP_APPLIED_POS {
        // The worker's applied position: `[partition_id:u16][term:u64]
        // [index:u64]`. This is the applied-index half of the
        // linearizable-read fence — see `LIN_FENCED`. Consume the payload
        // rather than returning early on the header, or the next read
        // starts mid-message.
        let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
        if payload_len != 18 {
            return false;
        }
        let mut pos = [0u8; 18];
        if ((sys.channel_read)(router.kv_in, pos.as_mut_ptr(), 18) as usize) < 18 {
            return false;
        }
        let partition = u16::from_le_bytes([pos[0], pos[1]]) as usize % MAX_TRACKED_RANGES;
        let term = u64::from_le_bytes([
            pos[2], pos[3], pos[4], pos[5], pos[6], pos[7], pos[8], pos[9],
        ]);
        let index = u64::from_le_bytes([
            pos[10], pos[11], pos[12], pos[13], pos[14], pos[15], pos[16], pos[17],
        ]);
        // Monotonic PER PARTITION: a report that went backwards (a
        // restored snapshot republishing an older position) must never
        // retire a fence it does not actually satisfy.
        if index > router.worker_applied_index[partition] {
            router.worker_applied_index[partition] = index;
            // The term moves with the index and only with it. Taking a
            // newer term while rejecting its index would pair a fresh
            // epoch with a stale position, which is exactly the
            // misattributed fence §21 invariant 11 forbids.
            router.worker_applied_term[partition] = term;
        }
        return true;
    }
    if hdr[0] != MSG_KV_APPLIED {
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    // MSG_KV_APPLIED head layout owned by wire::KvAppliedHead.
    if payload_len < wire::KvAppliedHead::LEN || payload_len > router.scratch.len() {
        return false;
    }
    let mut in_buf = [0u8; SCRATCH_BUF_SIZE];
    let n2 = (sys.channel_read)(router.kv_in, in_buf.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    let app = &in_buf[..payload_len];
    let Some(ah) = wire::KvAppliedHead::decode(app) else {
        return false;
    };
    let corr_id = ah.corr_id;
    let kpg_id = ah.kpg_id;
    let conn_id = ah.conn_id;
    let result = ah.result;
    let revision = ah.revision;
    let commit_frontier = ah.commit_frontier;
    let mut body_len = ah.body_len as usize;
    let body_off = wire::KvAppliedHead::LEN;
    if body_off + body_len > payload_len {
        return false;
    }
    // The worker appends `[catalog_generation:u64 LE]` after the body
    // (see kv_state_worker). Absent on a reply from a worker that does
    // not send one; zero then, and a zero generation is one no cache
    // will ever match, so a compute-side cache degrades to always
    // revalidating rather than to trusting something unverified.
    let catalog_generation = {
        let at = body_off + body_len;
        match app.get(at..at + 8) {
            Some(g) => u64::from_le_bytes([g[0], g[1], g[2], g[3], g[4], g[5], g[6], g[7]]),
            None => 0,
        }
    };

    // Router-originated help ops carry their own corr space and never
    // touch the inflight table; divert them before the lookup.
    if corr_id & HELP_CORR_BASE != 0 {
        let body = &in_buf[body_off..body_off + body_len];
        // Copy out: on_help_reply writes into router.scratch.
        let mut hb = [0u8; 512];
        let hl = body.len().min(hb.len());
        hb[..hl].copy_from_slice(&body[..hl]);
        return on_help_reply(router, corr_id, result, &hb[..hl]);
    }

    // Look up (proto, conn, path) and remove the inflight entry.
    let Some(meta) = router.inflight.remove(corr_id) else {
        // Unknown corr_id (worker reply for a request the router
        // didn't make, or the inflight was evicted). Drop.
        return false;
    };
    let protocol = meta.proto;

    // A cross-partition intent blocked this op: the worker could not
    // help locally (the home record is elsewhere), so the router does
    // what the worker must not — routes. The caller still gets its
    // TXN_PENDING below unchanged; the help pays off the RETRY.
    if result == types::KV_RESULT_TXN_PENDING {
        let pb_start = body_off;
        let pb_end = body_off + body_len;
        let mut pb = [0u8; 32];
        if body_len >= 32 {
            pb.copy_from_slice(&in_buf[pb_start..pb_start + 32]);
            let _ = pb_end;
            maybe_start_help(router, &pb, kpg_id, &meta);
        }
    }

    let mut result = result;

    // Ordered-mode scan stitching, response half: the worker's cursor
    // is provider-local to ITS sub-span; re-tag it into the
    // cross-range continuation. A non-zero provider cursor stays in
    // this range (tag it with the range index and the epoch stamp the
    // scan was routed under); zero means the sub-span is exhausted, so
    // substitute the pre-computed next-range continuation — 0 exactly
    // when this range was the last covering one, which is the wire's
    // own "scan complete".
    if meta.scan && result == KV_RESULT_RANGE && body_len >= 10 {
        let cur_off = body_off;
        let provider_cursor = u64::from_le_bytes([
            in_buf[cur_off],
            in_buf[cur_off + 1],
            in_buf[cur_off + 2],
            in_buf[cur_off + 3],
            in_buf[cur_off + 4],
            in_buf[cur_off + 5],
            in_buf[cur_off + 6],
            in_buf[cur_off + 7],
        ]);
        let stitched = if provider_cursor == 0 {
            Some(meta.scan_next)
        } else {
            partition_map::tag_scan_cursor(
                meta.scan_range as usize,
                meta.scan_epoch,
                provider_cursor,
            )
        };
        match stitched {
            Some(c) => in_buf[cur_off..cur_off + 8].copy_from_slice(&c.to_le_bytes()),
            // A provider cursor beyond 48 bits cannot be tagged;
            // forwarding it raw would resume in the wrong range.
            // Structural, and practically unreachable (both providers'
            // cursors are tiny) — answer INTERNAL rather than wrong.
            None => {
                result = KV_RESULT_INTERNAL;
                body_len = 0;
            }
        }
    }

    let dest = match protocol {
        PROTO_ETCD => router.etcd_out,
        PROTO_REDIS => router.redis_out,
        PROTO_MEMCACHED => router.memcached_out,
        PROTO_MEMCACHED_UDP => router.memcached_udp_out,
        PROTO_INTERNAL_WATCH => router.internal_watch_out,
        PROTO_INTERNAL_SQL => router.compute_out,
        types::PROTO_INTERNAL_DATA => router.compute_out,
        types::PROTO_INTERNAL_LIFECYCLE => router.internal_lifecycle_out,
        types::PROTO_INTERNAL_JOB => router.internal_job_out,
        _ => -1,
    };
    if dest < 0 {
        return false;
    }

    // Build MSG_KV_RESPONSE — head and fence tail owned by
    // wire::KvResponseHead / wire::FenceTail.
    let resp_payload_len = wire::KvResponseHead::LEN + body_len + wire::FenceTail::LEN;
    if resp_payload_len > u16::MAX as usize {
        return false;
    }
    let resp_total = 3 + resp_payload_len;
    if resp_total > router.scratch.len() {
        return false;
    }

    let out = &mut router.scratch[..resp_total];
    out[0] = MSG_KV_RESPONSE;
    out[1] = (resp_payload_len & 0xFF) as u8;
    out[2] = ((resp_payload_len >> 8) & 0xFF) as u8;
    wire::KvResponseHead {
        corr_id,
        conn_id,
        result,
        revision,
        body_len: body_len as u16,
    }
    .encode(&mut out[3..]);
    let mut p = 3 + wire::KvResponseHead::LEN;
    out[p..p + body_len].copy_from_slice(&in_buf[body_off..body_off + body_len]);
    p += body_len;

    // Fence tail. The identity is the group that actually answered —
    // taken from this reply, not from what the caller asked for — and the
    // applied position is that same partition's, so a separated compute
    // reading its own write fences against the partition that served it.
    wire::FenceTail {
        applied_index: router.worker_applied_index[kpg_id as usize % MAX_TRACKED_RANGES],
        applied_term: router.worker_applied_term[kpg_id as usize % MAX_TRACKED_RANGES],
        source_id: kpg_id as u32,
        durability: durability_for_path(meta.path),
        catalog_generation,
        commit_frontier,
    }
    .encode(&mut out[p..]);

    let forwarded = write_envelope_raw(sys, dest, &mut router.scratch[..], resp_total);
    // The op completed (response forwarded, or dropped on egress
    // backpressure — either way it is no longer outstanding): settle
    // the per-conn ordering slot, which may replay held requests.
    op_settled(router, meta.proto, meta.conn);
    forwarded
}

// ── Module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<RouterState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if state.is_null() || syscalls.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<RouterState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let router = unsafe { &mut *state.cast::<RouterState>() };
    router.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(router, params, params_len) };
    }

    // The kernel's `in_chan`/`out_chan` are port 0 of each direction
    // (etcd_in / kv_out). Discover the rest by index.
    router.etcd_in = in_chan;
    router.kv_out = out_chan;

    unsafe {
        let sys = &*sys_ptr;
        // inputs:  etcd_in[0], redis_in[1], memcached_in[2],
        //          memcached_udp_in[3], route_plan[4], cp_proof[5],
        //          quota_decision[6], kv_in[7]
        router.redis_in = dev_channel_port(sys, 0, 1);
        router.memcached_in = dev_channel_port(sys, 0, 2);
        router.memcached_udp_in = dev_channel_port(sys, 0, 3);
        router.route_plan_in = dev_channel_port(sys, 0, 4);
        router.cp_proof_in = dev_channel_port(sys, 0, 5);
        router.quota_decision_in = dev_channel_port(sys, 0, 6);
        router.kv_in = dev_channel_port(sys, 0, 7);

        // outputs: kv_out[0], watch_ctrl[1], lease_ctrl[2],
        //          etcd_out[3], redis_out[4], memcached_out[5],
        //          memcached_udp_out[6], metrics[7]
        router.watch_ctrl_out = dev_channel_port(sys, 1, 1);
        router.lease_ctrl_out = dev_channel_port(sys, 1, 2);
        router.etcd_out = dev_channel_port(sys, 1, 3);
        router.redis_out = dev_channel_port(sys, 1, 4);
        router.memcached_out = dev_channel_port(sys, 1, 5);
        router.memcached_udp_out = dev_channel_port(sys, 1, 6);
        router.metrics_out = dev_channel_port(sys, 1, 7);
        router.proposal_out = dev_channel_port(sys, 1, 8);
        router.lin_read_out = dev_channel_port(sys, 1, 9);
        router.read_release_in = dev_channel_port(sys, 0, 8);
        // map_update is input index 9 (declared after read_release in
        // the manifest). Unwired (-1) = the static default map stands.
        router.map_update_in = dev_channel_port(sys, 0, 9);
        router.internal_watch_in = dev_channel_port(sys, 0, 10);
        router.internal_sql_in = dev_channel_port(sys, 0, 11);
        router.internal_lifecycle_in = dev_channel_port(sys, 0, 12);
        router.internal_job_in = dev_channel_port(sys, 0, 13);
        router.internal_data_in = dev_channel_port(sys, 0, 14);
        router.gateway_reject_in = dev_channel_port(sys, 0, 15);
        router.internal_watch_out = dev_channel_port(sys, 1, 10);
        router.compute_out = dev_channel_port(sys, 1, 11);
        // Ordered-mode partition port pairs. Element 0 aliases the
        // classic single-partition ports; 1..4 are the additional
        // statically provisioned partitions (manifest indices 12..17).
        router.kv_outs[0] = router.kv_out;
        router.kv_outs[1] = dev_channel_port(sys, 1, 12);
        router.proposal_outs[0] = router.proposal_out;
        router.proposal_outs[1] = dev_channel_port(sys, 1, 13);
        router.internal_lifecycle_out = dev_channel_port(sys, 1, 14);
        router.internal_job_out = dev_channel_port(sys, 1, 15);
    }

    // Ordered mode boots from its static map or not at all: a missing,
    // oversized, or invalid `range_map` param — or a map that binds a
    // partition with no wired port pair — refuses construction. A graph
    // whose router cannot reach every declared range must fail here,
    // not NoRoute at first use.
    if router.routing_mode == ROUTING_MODE_ORDERED {
        let hex_len = router.range_map_param_len;
        if hex_len == 0 || hex_len == u16::MAX || hex_len % 2 != 0 {
            return -1;
        }
        let hex_len = hex_len as usize;
        let mut frame = [0u8; RANGE_MAP_PARAM_MAX];
        let byte_len = hex_len / 2;
        for i in 0..byte_len {
            let hi = hex_nibble(router.range_map_param[i * 2]);
            let lo = hex_nibble(router.range_map_param[i * 2 + 1]);
            let (Some(hi), Some(lo)) = (hi, lo) else {
                return -1;
            };
            frame[i] = (hi << 4) | lo;
        }
        let Some(max_generation) = router.range_map.apply_full_update(&frame[..byte_len]) else {
            return -1;
        };
        if !ordered_partitions_reachable(router) {
            return -1;
        }
        router.keyspace_routing = KeyspaceRouting {
            keyspace_id: 0,
            routing_kind: RoutingKind::OrderedRange,
            map_generation: max_generation,
        };
    }

    // Fail closed on a declared-but-unwired replication path. Returning
    // non-zero here aborts module construction, so the graph does not
    // start at all — far better than serving unreplicated writes that
    // look successful. See the `replicated` param for why this cannot be
    // expressed as a manifest `required` port.
    if router.replicated != 0 && router.proposal_out < 0 {
        return -1;
    }
    // Same shape for the linearizable-read fence: asking for lin reads
    // without the fence wiring would silently downgrade every read to an
    // unfenced local snapshot.
    if router.lin_reads != 0 && (router.lin_read_out < 0 || router.read_release_in < 0) {
        return -1;
    }
    // The fence has two halves, and both must be partition-aware before
    // lin reads may span local partitions. The applied-index half is:
    // every worker stamps its `partition_id` into `MSG_APP_APPLIED_POS`,
    // `worker_applied_index` is an array, and a `LIN_FENCED` read waits
    // on its own partition's slot (`fence_partition`). The ReadIndex-
    // GRANT half is partition-aware only in FANOUT mode, where the probe
    // carries a partition tag and a `partition_demux` routes it to the
    // owning partition's `consensus.read` (grants fan back in and match
    // by corr id). With direct multi-partition ports the probe still
    // reaches a single consensus instance — a partition-1 read would
    // fence against partition-0's grant index (meaningless across
    // independent index spaces) — so that combination stays refused.
    if router.lin_reads != 0 && router.kv_outs[1] >= 0 && router.partition_fanout == 0 {
        return -1;
    }
    // One graph, one compute placement.
    //
    // `internal_sql_in` is compute placed IN this graph; `internal_data_in`
    // is the same compute placed remotely and reaching us through the
    // Phase 9 data anchor. Both reply on `compute_out`, because the
    // router's outputs sit at fluxor's 16-port cap and there is no
    // seventeenth to give the second one.
    //
    // Sharing is only sound while the port has one owner. Wired
    // together, MSG_KV_RESPONSE carries no protocol byte, so both
    // consumers would receive every reply and each would have to guess
    // which were its own from corr_id alone — two modules answering the
    // same client, or neither. Refuse the combination rather than serve
    // a reply path that is only sometimes addressed.
    //
    // This costs nothing §30 needs: it compares a combined placement
    // against a separated one by running each and comparing histories,
    // which is two graphs either way.
    if router.internal_sql_in >= 0 && router.internal_data_in >= 0 {
        return -1;
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let router = unsafe { &mut *state.cast::<RouterState>() };

    // Drain ingress + kv_in in a bounded per-tick loop. The previous
    // 1-per-port-per-tick contract capped end-to-end throughput at
    // `tick_hz` (~1000 op/s at tick_us=1000), regardless of how many
    // clients were active. Each `drain_*` function returns whether it
    // consumed an envelope; we keep iterating round-robin across all
    // ports until every channel is empty or we hit the budget. The
    // budget bounds wall time per tick (router is dispatch-only so
    // each iteration is cheap — well under a tick at 32×4 envelopes).
    const PER_TICK_ROUTER_BUDGET: u32 = 64;
    let mut budget = PER_TICK_ROUTER_BUDGET;
    while budget > 0 {
        let progressed = unsafe {
            let a = drain_ingress(router, router.etcd_in, PROTO_ETCD);
            let b = drain_ingress(router, router.redis_in, PROTO_REDIS);
            let c = drain_ingress(router, router.memcached_in, PROTO_MEMCACHED);
            let d = drain_ingress(router, router.memcached_udp_in, PROTO_MEMCACHED_UDP);
            let f = drain_ingress(router, router.internal_watch_in, PROTO_INTERNAL_WATCH);
            let g = drain_ingress(router, router.internal_sql_in, PROTO_INTERNAL_SQL);
            let h = drain_ingress(
                router,
                router.internal_lifecycle_in,
                types::PROTO_INTERNAL_LIFECYCLE,
            );
            let i = drain_ingress(router, router.internal_job_in, types::PROTO_INTERNAL_JOB);
            let j = drain_ingress(router, router.internal_data_in, types::PROTO_INTERNAL_DATA);
            let k = drain_gateway_rejects(router);
            let e = drain_kv_in(router);
            a || b || c || d || e || f || g || h || i || j || k
        };
        if !progressed {
            break;
        }
        budget -= 1;
    }

    // Fence releases run every step (bounded, hot path). The two
    // timeout-recovery sweeps are O(FENCE_KEYS)/O(LIN_READ_SLOTS) and
    // only need coarse cadence — every SWEEP_INTERVAL steps (~64 ms at
    // tick_us=1000). Deadlines are seconds-scale, so this is ample.
    unsafe {
        drain_map_update(router);
        drain_read_release(router);
        router.sweep_ctr = router.sweep_ctr.wrapping_add(1);
        if router.sweep_ctr % SWEEP_INTERVAL == 0 {
            sweep_stash_deadlines(router);
            sweep_conn_deadlines(router);
            sweep_inflight_deadlines(router);
        }
        router.step_ctr = router.step_ctr.wrapping_add(1);
        if router.step_ctr.is_multiple_of(5000) && !router.syscalls.is_null() {
            telemetry::emit_counters(
                &*router.syscalls,
                router.metrics_out,
                &[
                    router.m_routed,
                    router.m_rejects,
                    router.m_lin_reads,
                    // §12.4 load signal: writes attributed to range 0
                    // and range 1. Monotonic; a consumer differences
                    // them for a rate.
                    router.m_range_writes[0],
                    router.m_range_writes[1],
                    // id 5: range 0's busiest-key share, percent.
                    busiest_key_percent(router, 0),
                    router.m_stale_rejects,
                    // Gauge-shaped but monotonic (generation never
                    // decreases), so the counter emit contract holds.
                    u64::from(router.keyspace_routing.map_generation),
                    router.m_map_update_drops,
                    router.m_cross_range_rejects,
                    // id 11: linearizable reads failed closed because
                    // apply never reached the fence index in time.
                    router.m_fence_timeouts,
                    router.m_inflight_timeouts,
                    router.m_not_leader_rejects,
                    router.m_helps_started,
                    router.m_helps_resolved,
                ],
            );
        }
    }
    0
}

/// Cadence for the O(FENCE_KEYS) recovery sweeps (see `sweep_ctr`).
const SWEEP_INTERVAL: u16 = 64;

/// Fail-closed cleanup for AWAITING fence stashes whose apply-side
/// resolution was lost (see `PendingLinRead::deadline_ms`).
unsafe fn sweep_stash_deadlines(router: &mut RouterState) {
    if router.syscalls.is_null() {
        return;
    }
    let now = dev_millis(&*router.syscalls);
    let mut fence_timeouts = 0u64;
    for p in router.pending_lin.iter_mut() {
        if now < p.deadline_ms {
            continue;
        }
        if p.state == LIN_AWAITING {
            p.state = LIN_REJECTED;
        } else if p.state == LIN_FENCED {
            // Granted by consensus, but apply never caught up to the
            // fence index within the stash window. Serving it now would
            // answer from a store known to be behind the linearization
            // point — the false absence this fence exists to prevent —
            // so it fails closed like a lost grant. Counted separately
            // because the two mean different things operationally: a
            // lost grant is a consensus problem, this is apply lag.
            p.state = LIN_REJECTED;
            fence_timeouts += 1;
        }
    }
    router.m_fence_timeouts = router.m_fence_timeouts.wrapping_add(fence_timeouts);
}
