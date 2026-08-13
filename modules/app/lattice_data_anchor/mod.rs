//! lattice_data_anchor — storage-side terminator of `lattice.data`.
//!
//! RFC database foundation §14.8, §14.10, §26 phase 9. §14.10 states
//! the whole job in one sentence: "terminate the internal multiplexed
//! data surface and admit requests into range routing."
//!
//! ## This is not a protocol anchor
//!
//! Its peer is a `lattice_data_client` in ANOTHER graph — separated
//! compute — and the bytes are `data_surface.rs` frames, not RESP.
//! Three consequences shape the module:
//!
//! - **No response ordering.** The surface is multiplexed and every
//!   response carries its own `request_id`, so the peer demultiplexes.
//!   `redis_edge_anchor` needs a per-connection reorder ring because
//!   RESP has nothing to correlate with but arrival order; this has an
//!   identity on every message and needs none.
//! - **No session, retry, route cache, cancellation, or continuation
//!   state.** §14.9 puts all five in the data client, and it is the
//!   client that owns them precisely so that losing this graph loses
//!   nothing a caller was relying on.
//! - **No parsing decisions.** The compute layer already compiled the
//!   operation into a canonical KV opcode (§14.2, §19); the frame
//!   carries it. This module never infers what a `DataOp::Write` meant.
//!
//! ## What it refuses, and why refusing is the feature
//!
//! Nine of the 23 operations are refused with
//! `UnsupportedCapability`, by name, at the point of admission:
//! session lifecycle (`open`, `close`, `session_close`), capability
//! and catalog negotiation (`capabilities`, `catalog_revision`),
//! subscriptions (`subscribe_topology`, `subscribe_catalog`), route
//! resolution (`resolve_span`), and cancellation (`cancel`).
//!
//! Every one needs machinery that is genuinely not in a storage graph.
//! §28 rejects treating this surface as a transparent FIFO exactly
//! because a caller must be able to tell "not supported here" from
//! "your request was lost", and a silent drop or a plausible-looking
//! empty success would be indistinguishable from both. The match on
//! `op_disposition` is total over `DataOp::ALL`, so an operation added
//! later cannot fall into a default and quietly become one of these.
//!
//! ## What a response can prove, and what it cannot
//!
//! §14.8 requires every response to carry "applied index and
//! source-aware consistency/durability fences". `MSG_KV_RESPONSE`
//! carries a fence tail for exactly this: the worker's applied
//! index and term, the partition group that answered, and the
//! durability class the router's path can support.
//!
//! So `observed_revision`, `applied_index`, the fence identity and the
//! fence epoch are all real. Two fields are still zero, and stay zero
//! deliberately:
//!
//! - `resolved_timestamp` — the KV envelope does not carry the MVCC
//!   timestamp a read resolved at.
//! - `serving_range_id` / `serving_range_generation` — the envelope
//!   does not say which range served. Echoing the request's range back
//!   would be the caller's own claim wearing the costume of an
//!   observation, and §21 invariant 16 compares these fields across
//!   placements, so a fabricated one would make the comparison pass by
//!   construction.
//!
//! `durability_achieved` is whatever the router reports and is checked
//! against §21 invariant 2 before it leaves: a claim of
//! `ReplicatedDurable` from a source that cannot observe a quorum
//! durable proof is refused rather than forwarded. An unrecognised
//! durability byte is refused too, instead of being read as
//! `Volatile` — silently downgrading a future stronger claim is how
//! two builds come to disagree about what was promised.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    clippy::duplicate_mod,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, ABI fns carry a fixed arity, and the PIC build #[path]-remounts shared SDK/common code"
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

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[path = "../../common/data_surface.rs"]
mod data_surface;

#[path = "../../common/net_proto.rs"]
mod net_proto;
use net_proto::{
    net_conn_id, NET_CMD_BIND, NET_CMD_CLOSE, NET_CMD_SEND, NET_CONN_LEN, NET_MSG_ACCEPTED,
    NET_MSG_BOUND, NET_MSG_CLOSED, NET_MSG_DATA, NET_MSG_ERROR,
};

use data_surface::{
    adapter_requirement, admit_request, decode_request_frame, encode_response_frame, DataOp,
    DataRequest, DataResponse, DataRetryClass, Durability, FenceSource, ObservedFences, Outcome,
    SourceFence, DATA_FRAME_MAX_PAYLOAD, REQUEST_FRAME_HEADER_LEN, RESPONSE_FRAME_HEADER_LEN,
};
use types::{
    KV_OP_APPEND, KV_OP_CAS, KV_OP_DECR, KV_OP_DELETE, KV_OP_FLUSH, KV_OP_INCR, KV_OP_MSET,
    KV_OP_PREPEND, KV_OP_PUT, KV_OP_TARGETED, KV_OP_TXN, KV_OP_TXN_PREPARE, KV_OP_TXN_RECORD,
    KV_OP_TXN_RESOLVE, KV_RESULT_ARRAY, KV_RESULT_CAS_FAILED, KV_RESULT_COMPACTED,
    KV_RESULT_CROSS_DOMAIN, KV_RESULT_CROSS_RANGE, KV_RESULT_DIRTY_EPOCH, KV_RESULT_EXISTS,
    KV_RESULT_INTEGER, KV_RESULT_INTERNAL, KV_RESULT_LIN_BOUND, KV_RESULT_NOT_FOUND, KV_RESULT_OK,
    KV_RESULT_QUOTA, KV_RESULT_RANGE, KV_RESULT_SCAN_CURSOR, KV_RESULT_TXN, KV_RESULT_TXN_PENDING,
    KV_RESULT_UNAUTH, KV_RESULT_VERSIONS, KV_RESULT_WRONG_TYPE, PROTO_INTERNAL_DATA,
};
use wire::{KV_RESPONSE_FENCE_TAIL_LEN, MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Capacities ────────────────────────────────────────────────────────

/// Peer connections. A compute graph opens one data client; a handful
/// of compute graphs may share one storage graph. Sixteen is generous
/// for that and keeps the state arena small — unlike a client-facing
/// anchor, connection count here is not the throughput axis.
const MAX_CONNS: usize = 16;

/// Per-slot recv buffer. Must hold one whole maximal frame, because a
/// frame is only parsed once complete: header + the payload bound.
const RECV_BUF_SIZE: usize = REQUEST_FRAME_HEADER_LEN + DATA_FRAME_MAX_PAYLOAD + 256;

/// Per-slot send buffer. Sized for several maximal responses queued
/// behind one flush.
const SEND_BUF_SIZE: usize = (RESPONSE_FRAME_HEADER_LEN + DATA_FRAME_MAX_PAYLOAD) * 2 + 256;

const SCRATCH_BUF_SIZE: usize = RECV_BUF_SIZE + 64;

/// In-flight requests awaiting a router reply, across all connections.
const MAX_INFLIGHT: usize = 128;

const DEFAULT_LISTEN_PORT: u16 = 7432;

/// u16 conn-id sentinel: the ip stack's ids are monotone u16s, so
/// 0xFFFF is unreachable long before the tables bind.
const SLOT_FREE: u16 = 0xFFFF;

define_params! {
    AnchorState;

    // TCP port the anchor binds for data clients. Overridable so a
    // multi-replica localhost bring-up does not collide.
    1, listen_port, u16, DEFAULT_LISTEN_PORT
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, DEFAULT_LISTEN_PORT); };
}

// ── State ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum AnchorPhase {
    Init = 0,
    WaitBound = 1,
    Listening = 2,
    Error = 3,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum SlotPhase {
    Open = 0,
    Closing = 1,
}

#[repr(C)]
struct Slot {
    conn_id: u16,
    phase: SlotPhase,
    recv_len: usize,
    send_len: usize,
    recv_buf: [u8; RECV_BUF_SIZE],
    send_buf: [u8; SEND_BUF_SIZE],
}

/// One request handed to the router, reduced to what building its
/// response needs. The router echoes `corr_id` and `conn_id`, but a
/// `DataResponse` also needs the identities the surface correlates on,
/// and those exist only in the request frame.
#[repr(C)]
struct Inflight {
    in_use: bool,
    conn_id: u16,
    corr_id: u64,
    request_id: u64,
    session_id: u64,
}

#[repr(C)]
struct AnchorState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,

    phase: AnchorPhase,
    server_conn_id: u16,
    listen_port: u16,
    next_corr: u64,

    slots: [Slot; MAX_CONNS],
    inflight: [Inflight; MAX_INFLIGHT],

    m_frames: u64,
    m_forwarded: u64,
    m_admission_refusals: u64,
    m_unsupported_refusals: u64,
    m_net_errors: u64,
    m_inflight_full: u64,
    step_ctr: u64,

    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl AnchorState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.net_in = -1;
        self.net_out = -1;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.phase = AnchorPhase::Init;
        self.server_conn_id = SLOT_FREE;
        self.listen_port = DEFAULT_LISTEN_PORT;
        self.next_corr = 1;
        for i in 0..MAX_CONNS {
            self.slots[i].conn_id = SLOT_FREE;
            self.slots[i].phase = SlotPhase::Open;
            self.slots[i].recv_len = 0;
            self.slots[i].send_len = 0;
        }
        for i in 0..MAX_INFLIGHT {
            self.inflight[i].in_use = false;
        }
        self.m_frames = 0;
        self.m_forwarded = 0;
        self.m_admission_refusals = 0;
        self.m_unsupported_refusals = 0;
        self.m_net_errors = 0;
        self.m_inflight_full = 0;
        self.step_ctr = 0;
    }

    fn next_corr(&mut self) -> u64 {
        let c = self.next_corr;
        self.next_corr = self.next_corr.wrapping_add(1);
        if self.next_corr == 0 {
            self.next_corr = 1;
        }
        c
    }

    fn find_slot(&self, conn_id: u16) -> Option<usize> {
        (0..MAX_CONNS).find(|&i| self.slots[i].conn_id == conn_id)
    }

    fn alloc_slot(&mut self, conn_id: u16) -> Option<usize> {
        let i = (0..MAX_CONNS).find(|&i| self.slots[i].conn_id == SLOT_FREE)?;
        self.slots[i].conn_id = conn_id;
        self.slots[i].phase = SlotPhase::Open;
        self.slots[i].recv_len = 0;
        self.slots[i].send_len = 0;
        Some(i)
    }

    fn free_slot(&mut self, idx: usize) {
        let conn_id = self.slots[idx].conn_id;
        self.slots[idx].conn_id = SLOT_FREE;
        self.slots[idx].phase = SlotPhase::Open;
        self.slots[idx].recv_len = 0;
        self.slots[idx].send_len = 0;
        // Drop this connection's in-flight entries. Their replies will
        // still arrive; without this they would keep their table slots
        // until the counter wrapped, and a busy peer that reconnects
        // often would exhaust the table with requests no one is waiting
        // for. `handle_kv_response` already tolerates a reply with no
        // entry — that is exactly this case.
        for i in 0..MAX_INFLIGHT {
            if self.inflight[i].in_use && self.inflight[i].conn_id == conn_id {
                self.inflight[i].in_use = false;
            }
        }
    }

    fn track(&mut self, corr_id: u64, conn_id: u16, request_id: u64, session_id: u64) -> bool {
        let Some(i) = (0..MAX_INFLIGHT).find(|&i| !self.inflight[i].in_use) else {
            return false;
        };
        self.inflight[i] = Inflight {
            in_use: true,
            conn_id,
            corr_id,
            request_id,
            session_id,
        };
        true
    }

    fn take_inflight(&mut self, corr_id: u64) -> Option<(u16, u64, u64)> {
        let i = (0..MAX_INFLIGHT)
            .find(|&i| self.inflight[i].in_use && self.inflight[i].corr_id == corr_id)?;
        self.inflight[i].in_use = false;
        Some((
            self.inflight[i].conn_id,
            self.inflight[i].request_id,
            self.inflight[i].session_id,
        ))
    }
}

// ── Operation disposition ─────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Disposition {
    /// Compiles to a canonical KV command; hand it to the router.
    Forward,
    /// Answered here, with no canonical execution behind it.
    Inline,
    /// Needs machinery a storage graph does not have. Refused by name.
    Unsupported,
}

/// Total over `DataOp::ALL` — no wildcard arm. An operation added to
/// the surface without a disposition fails to compile, which is the
/// only way to stop it silently becoming whatever the default was.
const fn op_disposition(op: DataOp) -> Disposition {
    match op {
        // Reads and transactional work compile to canonical KV
        // commands, which is exactly what the router routes.
        DataOp::GetAt
        | DataOp::BatchGet
        | DataOp::ScanOpen
        | DataOp::ScanNext
        | DataOp::ScanClose
        | DataOp::FeedRead
        | DataOp::Begin
        | DataOp::TxnGet
        | DataOp::TxnScan
        | DataOp::Write
        | DataOp::Prepare
        | DataOp::Commit
        | DataOp::Abort
        | DataOp::Status => Disposition::Forward,

        // A keep-alive asks whether the surface is up. Answering it
        // here is the answer; forwarding it would make liveness of the
        // anchor depend on liveness of the range.
        DataOp::KeepAlive => Disposition::Inline,

        // Session lifecycle (§14.9: the session directory lives with
        // compute), capability and catalog negotiation, subscriptions,
        // route resolution, and cancellation. Each needs a component
        // that is genuinely absent here, and each is refused by name so
        // the caller can tell "not here" from "lost".
        DataOp::Open
        | DataOp::Capabilities
        | DataOp::Close
        | DataOp::SessionClose
        | DataOp::ResolveSpan
        | DataOp::SubscribeTopology
        | DataOp::CatalogRevision
        | DataOp::SubscribeCatalog
        | DataOp::Cancel => Disposition::Unsupported,
    }
}

/// Does this canonical KV opcode change durable state?
///
/// Used for one check: a non-mutating `DataOp` must not arrive carrying
/// a mutating KV opcode. The two halves of a frame are supplied by the
/// same peer, so agreement between them is precisely what a confused or
/// hostile client could get wrong, and admission is where it is caught.
const fn kv_op_mutates(kv_op: u8) -> bool {
    matches!(
        kv_op,
        KV_OP_PUT
            | KV_OP_DELETE
            | KV_OP_CAS
            | KV_OP_INCR
            | KV_OP_DECR
            | KV_OP_APPEND
            | KV_OP_PREPEND
            | KV_OP_MSET
            | KV_OP_FLUSH
            | KV_OP_TXN
            | KV_OP_TXN_PREPARE
            | KV_OP_TXN_RESOLVE
            | KV_OP_TXN_RECORD
    )
}

/// Map a canonical KV result to a surface outcome.
///
/// The split is "did the authority answer", not "did the caller like
/// the answer". A miss, a CAS that did not hold, and a transaction
/// result are all answers — the operation ran, and the response frame's
/// `kv_result` byte carries which one. Only a failure to produce an
/// answer becomes `Refused`, and then the retry class says what would
/// have to change for a retry to do better.
const fn map_result(result: u8) -> (Outcome, Option<DataRetryClass>) {
    match result {
        KV_RESULT_OK
        | KV_RESULT_NOT_FOUND
        | KV_RESULT_EXISTS
        | KV_RESULT_CAS_FAILED
        | KV_RESULT_INTEGER
        | KV_RESULT_ARRAY
        | KV_RESULT_SCAN_CURSOR
        | KV_RESULT_TXN
        | KV_RESULT_RANGE
        | KV_RESULT_VERSIONS => (Outcome::Ok, None),

        // The route this request named is not the route that owns the
        // key any more. Refresh descriptors and retry. `CROSS_DOMAIN` is
        // the same at the cluster-domain granularity (§11.3/§26.5): the
        // key's home range is in another domain, and the response
        // payload carries which one so the caller can redirect there.
        KV_RESULT_CROSS_RANGE | KV_RESULT_DIRTY_EPOCH | KV_RESULT_CROSS_DOMAIN => {
            (Outcome::Refused, Some(DataRetryClass::StaleRoute))
        }

        // Contention: an intent is in the way, or the tenant is over
        // budget. The same request later may well succeed.
        KV_RESULT_TXN_PENDING | KV_RESULT_QUOTA => {
            (Outcome::Refused, Some(DataRetryClass::RetryableContention))
        }

        // The owning authority could not answer: the linearizable
        // fence did not resolve, or the worker failed internally.
        KV_RESULT_LIN_BOUND | KV_RESULT_INTERNAL => {
            (Outcome::Refused, Some(DataRetryClass::UnavailableAuthority))
        }

        // The request is wrong in a way retrying identically cannot
        // fix: wrong type for the key, not permitted, or reading below
        // the retention floor.
        KV_RESULT_WRONG_TYPE | KV_RESULT_UNAUTH | KV_RESULT_COMPACTED => {
            (Outcome::Refused, Some(DataRetryClass::Malformed))
        }

        // An unknown result code. Not `Ok` — we cannot say an operation
        // succeeded on the strength of a byte this build does not
        // recognise — and not `Indeterminate` either, since the router
        // did reply and the reply is decisive about something. Refused
        // and unavailable is the honest reading: this composition could
        // not turn the answer into one the caller can act on.
        _ => (Outcome::Refused, Some(DataRetryClass::UnavailableAuthority)),
    }
}

/// The fences a response reports, built from `MSG_KV_RESPONSE`'s fence
/// tail (see `wire::MSG_KV_RESPONSE`).
///
/// `resolved_timestamp` and the serving range identity are still zero.
/// The envelope does not carry them and this module will not invent
/// them: a serving range echoed back from the request would be the
/// caller's own claim wearing the costume of an observation.
///
/// `durability_achieved` comes from the router, which reports the
/// strongest class its path can support. If the byte is one this build
/// does not recognise the whole response is refused rather than
/// downgraded, because silently reading an unknown durability as
/// `Volatile` would turn a future stronger claim into a weaker one
/// without anyone noticing.
fn fences_for(
    observed_revision: u64,
    applied_index: u64,
    applied_term: u64,
    source_id: u32,
    durability: u8,
) -> Option<ObservedFences> {
    let fences = ObservedFences {
        serving_range_id: [0; 16],
        serving_range_generation: 0,
        observed_revision,
        resolved_timestamp: 0,
        applied_index,
        fence: SourceFence {
            kind: FenceSource::RangeLeader,
            source_id,
            // The Raft term the applied position was reached in. §21
            // invariant 11: identity and epoch travel together, so a
            // fence with the right group and a stale term is still
            // stale and the caller can see that it is.
            source_epoch: applied_term,
        },
        durability_achieved: Durability::from_u8(durability)?,
    };
    // §21 invariant 2, checked rather than assumed. A follower or
    // observer can never have watched a quorum durable proof, so a
    // response claiming `ReplicatedDurable` from one is not a strong
    // answer — it is a false one, and it must not leave this module.
    if !fences.durability_claim_is_sound() {
        return None;
    }
    Some(fences)
}

/// The fences a message that never reached a range reports: none.
const fn no_fences() -> ObservedFences {
    ObservedFences {
        serving_range_id: [0; 16],
        serving_range_generation: 0,
        observed_revision: 0,
        resolved_timestamp: 0,
        applied_index: 0,
        fence: SourceFence {
            kind: FenceSource::RangeLeader,
            source_id: 0,
            // Zero fails `is_valid_for` against any real expectation,
            // which is the correct answer to "prove this fence": no.
            source_epoch: 0,
        },
        durability_achieved: Durability::Volatile,
    }
}

// ── Response emission ─────────────────────────────────────────────────

/// Queue a response frame on the slot's send buffer.
fn emit_response(
    anchor: &mut AnchorState,
    slot_idx: usize,
    response: &DataResponse,
    kv_result: u8,
    payload: &[u8],
) -> bool {
    let mut frame = [0u8; RESPONSE_FRAME_HEADER_LEN + 512];
    let n = match encode_response_frame(response, kv_result, payload, &mut frame) {
        Some(n) => n,
        None => return false,
    };
    let slot = &mut anchor.slots[slot_idx];
    if slot.send_len + n > SEND_BUF_SIZE {
        // The peer is not draining. Closing is the only bounded
        // option: buffering further would make this module's memory a
        // function of the peer's behaviour.
        slot.phase = SlotPhase::Closing;
        return false;
    }
    let at = slot.send_len;
    slot.send_buf[at..at + n].copy_from_slice(&frame[..n]);
    slot.send_len = at + n;
    true
}

/// Emit a refusal carrying `class`. Refusals have no canonical
/// execution behind them, so `kv_result` is zero and the payload empty.
fn refuse(
    anchor: &mut AnchorState,
    slot_idx: usize,
    request_id: u64,
    session_id: u64,
    class: DataRetryClass,
) {
    let response = DataResponse {
        request_id,
        session_id,
        outcome: Outcome::Refused,
        retry: Some(class),
        fences: no_fences(),
        catalog_generation: 0,
        continuation: None,
    };
    let _ = emit_response(anchor, slot_idx, &response, KV_RESULT_OK, &[]);
}

// ── Frame handling ────────────────────────────────────────────────────

/// Handle one complete request frame.
fn handle_frame(anchor: &mut AnchorState, slot_idx: usize, frame: &[u8]) {
    anchor.m_frames = anchor.m_frames.wrapping_add(1);

    let Some(decoded) = decode_request_frame(frame) else {
        // Unparseable. There is no `request_id` to answer with — the
        // identity a response correlates on lives inside the record
        // that did not decode — so there is no response that could
        // reach the right caller. Close the connection: a peer that
        // framed one bad request has lost stream sync and every later
        // byte offset is suspect.
        anchor.slots[slot_idx].phase = SlotPhase::Closing;
        return;
    };
    let request = decoded.request;
    let kv_op = decoded.kv_op;
    let request_id = request.context.request_id;
    let session_id = request.session_id;

    // Admission first (§14.8, §21 invariants 14 and 17). The contract
    // classifies its own refusals; this module does not get an opinion.
    if let Err(err) = admit_request(&request) {
        anchor.m_admission_refusals = anchor.m_admission_refusals.wrapping_add(1);
        refuse(
            anchor,
            slot_idx,
            request_id,
            session_id,
            err.refusal_class(),
        );
        return;
    }

    // The two halves of the frame come from the same peer, so their
    // agreement is what a confused peer gets wrong. A read that arrives
    // carrying a mutating opcode is refused rather than executed.
    if !request.op.is_mutation() && kv_op_mutates(kv_op) {
        anchor.m_admission_refusals = anchor.m_admission_refusals.wrapping_add(1);
        refuse(
            anchor,
            slot_idx,
            request_id,
            session_id,
            DataRetryClass::Malformed,
        );
        return;
    }

    // Explicit partition addressing is a lifecycle-bootstrap tool that
    // only the range supervisor may use (see `KV_OP_TARGETED`). A
    // remote compute peer reaching for it is refused here rather than
    // at the router, so the refusal names a capability instead of
    // arriving as a generic cross-range rejection.
    if kv_op == KV_OP_TARGETED {
        anchor.m_unsupported_refusals = anchor.m_unsupported_refusals.wrapping_add(1);
        refuse(
            anchor,
            slot_idx,
            request_id,
            session_id,
            DataRetryClass::UnsupportedCapability,
        );
        return;
    }

    match op_disposition(request.op) {
        Disposition::Unsupported => {
            anchor.m_unsupported_refusals = anchor.m_unsupported_refusals.wrapping_add(1);
            refuse(
                anchor,
                slot_idx,
                request_id,
                session_id,
                DataRetryClass::UnsupportedCapability,
            );
        }
        Disposition::Inline => {
            let response = DataResponse {
                request_id,
                session_id,
                outcome: Outcome::Ok,
                retry: None,
                // A keep-alive is answered here and observed nothing.
                fences: no_fences(),
                catalog_generation: 0,
                continuation: None,
            };
            let _ = emit_response(anchor, slot_idx, &response, KV_RESULT_OK, &[]);
        }
        Disposition::Forward => {
            forward(anchor, slot_idx, &request, kv_op, decoded.payload);
        }
    }
}

/// Wrap the frame's canonical command in a `MSG_KV_REQUEST` envelope.
fn forward(
    anchor: &mut AnchorState,
    slot_idx: usize,
    request: &DataRequest,
    kv_op: u8,
    payload: &[u8],
) {
    let request_id = request.context.request_id;
    let session_id = request.session_id;

    if anchor.kv_out < 0 {
        refuse(
            anchor,
            slot_idx,
            request_id,
            session_id,
            DataRetryClass::UnavailableAuthority,
        );
        return;
    }

    // Envelope head: [corr:8][proto:1][tenant:4][conn:1][cons:1][op:1][body_len:2] = 18
    const HEAD_LEN: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    // §23 canonical identity: the tenant rides in the head; the database
    // and keyspace ride in an 8-byte tail after the body. This anchor
    // has the native RequestContext, so it always appends the tail (the
    // router reads it and forwards all three to the store).
    const IDENT_TAIL_LEN: usize = 4 + 4;
    let body_len = payload.len();
    let payload_len = HEAD_LEN + body_len + IDENT_TAIL_LEN;
    let total = 3 + payload_len;
    if payload_len > u16::MAX as usize || total > SCRATCH_BUF_SIZE {
        refuse(
            anchor,
            slot_idx,
            request_id,
            session_id,
            DataRetryClass::Malformed,
        );
        return;
    }

    let corr = anchor.next_corr();
    let conn_id = anchor.slots[slot_idx].conn_id;

    // Reserve the in-flight entry BEFORE writing. A reply whose entry
    // does not exist cannot be answered, so the table must never be the
    // thing that fails after the router already has the work.
    if !anchor.track(corr, conn_id, request_id, session_id) {
        anchor.m_inflight_full = anchor.m_inflight_full.wrapping_add(1);
        refuse(
            anchor,
            slot_idx,
            request_id,
            session_id,
            DataRetryClass::RetryableContention,
        );
        return;
    }

    {
        let scratch = &mut anchor.scratch[..total];
        scratch[0] = MSG_KV_REQUEST;
        scratch[1] = (payload_len & 0xFF) as u8;
        scratch[2] = ((payload_len >> 8) & 0xFF) as u8;
        let mut p = 3;
        scratch[p..p + 8].copy_from_slice(&corr.to_le_bytes());
        p += 8;
        scratch[p] = PROTO_INTERNAL_DATA;
        p += 1;
        scratch[p..p + 4].copy_from_slice(&request.context.tenant_id.to_le_bytes());
        p += 4;
        // The head's conn byte is this anchor's SLOT INDEX (a caller-
        // chosen routing slot the router echoes) — NOT the net conn id,
        // which is u16 now and does not fit the byte. Reply routing
        // resolves through the inflight table by corr_id anyway.
        scratch[p] = slot_idx as u8;
        p += 1;
        // Back to `types::REQ_*` — the router's vocabulary, not the
        // surface's. `data_surface::adapter_requirement` is the inverse
        // of the client's `read_policy`, and they must stay inverses.
        scratch[p] = adapter_requirement(request.context.consistency);
        p += 1;
        scratch[p] = kv_op;
        p += 1;
        scratch[p] = (body_len & 0xFF) as u8;
        scratch[p + 1] = ((body_len >> 8) & 0xFF) as u8;
        p += 2;
        scratch[p..p + body_len].copy_from_slice(payload);
        p += body_len;
        // §23 identity tail: [database:4][keyspace:4].
        scratch[p..p + 4].copy_from_slice(&request.context.database_id.to_le_bytes());
        p += 4;
        scratch[p..p + 4].copy_from_slice(&request.context.keyspace_id.to_le_bytes());
    }

    let sys = anchor.syscalls;
    let kv_out = anchor.kv_out;
    if sys.is_null() {
        let _ = anchor.take_inflight(corr);
        return;
    }
    let written = unsafe { ((*sys).channel_write)(kv_out, anchor.scratch.as_mut_ptr(), total) };
    if written != total as i32 {
        // Backpressure. Release the entry we reserved and tell the
        // caller to come back — `RetryableContention` rather than
        // `UnavailableAuthority` because nothing is down, the queue is
        // simply full right now.
        let _ = anchor.take_inflight(corr);
        refuse(
            anchor,
            slot_idx,
            request_id,
            session_id,
            DataRetryClass::RetryableContention,
        );
        return;
    }
    anchor.m_forwarded = anchor.m_forwarded.wrapping_add(1);
}

// ── Router replies ────────────────────────────────────────────────────

unsafe fn poll_kv_in(anchor: &mut AnchorState) -> bool {
    if anchor.kv_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    const PER_TICK_KV_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    while processed < PER_TICK_KV_BUDGET {
        let poll = ((*sys).channel_poll)(anchor.kv_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return false;
        }
        if !handle_kv_response(anchor) {
            return false;
        }
        processed += 1;
    }
    true
}

unsafe fn handle_kv_response(anchor: &mut AnchorState) -> bool {
    let sys = anchor.syscalls;
    let mut hdr = [0u8; 3];
    if ((*sys).channel_read)(anchor.kv_in, hdr.as_mut_ptr(), 3) < 3 {
        return false;
    }
    if hdr[0] != MSG_KV_RESPONSE {
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    // Head + fence-tail layouts owned by wire::KvResponseHead / FenceTail.
    if !(wire::KvResponseHead::LEN..=SCRATCH_BUF_SIZE).contains(&payload_len) {
        return false;
    }
    if (((*sys).channel_read)(anchor.kv_in, anchor.scratch.as_mut_ptr(), payload_len) as usize)
        < payload_len
    {
        return false;
    }

    let Some(rh) = wire::KvResponseHead::decode(&anchor.scratch[..payload_len]) else {
        return false;
    };
    let corr = rh.corr_id;
    let result = rh.result;
    let revision = rh.revision;
    let body_len = rh.body_len as usize;
    let body_off = wire::KvResponseHead::LEN;
    // The fence tail is not optional. A router that does not send one
    // is not an older router to be tolerated — it is a router this
    // build cannot build a §14.8 response from, and treating its
    // replies as unfenced would answer callers with a fence-shaped
    // hole they have no way to distinguish from a real zero.
    let tail_off = body_off + body_len;
    if tail_off + wire::FenceTail::LEN > payload_len {
        return false;
    }
    let Some(ft) = wire::FenceTail::decode(&anchor.scratch[tail_off..]) else {
        return false;
    };
    let (applied_index, applied_term, source_id, durability, catalog_generation) = (
        ft.applied_index,
        ft.applied_term,
        ft.source_id,
        ft.durability,
        ft.catalog_generation,
    );

    // A reply with no entry is a late arrival for a connection that has
    // since gone away. Dropping it is correct — nothing is waiting.
    let Some((conn_id, request_id, session_id)) = anchor.take_inflight(corr) else {
        return true;
    };
    let Some(slot_idx) = anchor.find_slot(conn_id) else {
        return true;
    };

    // Copy the body out of scratch before borrowing the slot mutably.
    let mut body = [0u8; DATA_FRAME_MAX_PAYLOAD];
    let blen = body_len.min(DATA_FRAME_MAX_PAYLOAD);
    body[..blen].copy_from_slice(&anchor.scratch[body_off..body_off + blen]);

    let (outcome, retry) = map_result(result);
    // A refusal observed nothing, so it reports nothing. An answer
    // reports what the tail said — and if the tail is unreadable the
    // answer is withdrawn rather than served fenceless, because a
    // caller cannot tell "no fence" from "fence of zero".
    let (outcome, retry, fences) = if outcome == Outcome::Ok {
        match fences_for(revision, applied_index, applied_term, source_id, durability) {
            Some(f) => (outcome, retry, f),
            None => (
                Outcome::Refused,
                Some(DataRetryClass::UnavailableAuthority),
                no_fences(),
            ),
        }
    } else {
        (outcome, retry, no_fences())
    };
    let response = DataResponse {
        request_id,
        session_id,
        outcome,
        retry,
        fences,
        // Relayed from the KV fence tail so the compute-side catalog
        // cache can validate a cached mapping across the boundary. A
        // refusal reports 0 (see the other two constructors), which the
        // cache reads as "no generation" and does not trust.
        catalog_generation,
        continuation: None,
    };
    // A refusal describes no execution, so it carries no rows — EXCEPT a
    // cross-domain redirect, whose body is not rows but the owning
    // cluster domain the caller must go to (§11.3/§26.5). Forwarding it
    // is the difference between "wrong domain" and "wrong domain, go to
    // N".
    let payload: &[u8] = if outcome == Outcome::Ok || result == KV_RESULT_CROSS_DOMAIN {
        &body[..blen]
    } else {
        &[]
    };
    let _ = emit_response(anchor, slot_idx, &response, result, payload);
    true
}

// ── NET side ──────────────────────────────────────────────────────────

unsafe fn poll_net_in(anchor: &mut AnchorState) -> bool {
    if anchor.net_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    const PER_TICK_NET_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    while processed < PER_TICK_NET_BUDGET {
        let poll = ((*sys).channel_poll)(anchor.net_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return false;
        }
        let buf = anchor.scratch.as_mut_ptr();
        let (msg_type, payload_len) = net_read_frame(&*sys, anchor.net_in, buf, SCRATCH_BUF_SIZE);
        if msg_type == 0 && payload_len == 0 {
            return false;
        }
        let mut tmp = [0u8; SCRATCH_BUF_SIZE];
        let copy_len = payload_len.min(SCRATCH_BUF_SIZE);
        core::ptr::copy_nonoverlapping(buf.add(NET_FRAME_HDR), tmp.as_mut_ptr(), copy_len);
        dispatch_net_frame(anchor, msg_type, &tmp[..copy_len]);
        processed += 1;
    }
    true
}

unsafe fn dispatch_net_frame(anchor: &mut AnchorState, msg_type: u8, payload: &[u8]) {
    let sys = anchor.syscalls;
    match msg_type {
        NET_MSG_BOUND => {
            // `net_out` is broadcast in multi-anchor graphs, so claim
            // only the listener whose port is ours.
            // BOUND payload: [conn_id:u16 LE][local_port:u16 LE].
            if anchor.phase == AnchorPhase::WaitBound && payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = net_conn_id(payload).unwrap_or(SLOT_FREE);
                    anchor.phase = AnchorPhase::Listening;
                    dev_log(&*sys, 3, b"[data_anc] bound".as_ptr(), 16);
                }
            } else if anchor.phase == AnchorPhase::WaitBound && payload.len() >= NET_CONN_LEN {
                // Single-anchor provider (no port in payload): claim
                // the first BOUND we see.
                anchor.server_conn_id = net_conn_id(payload).unwrap_or(SLOT_FREE);
                anchor.phase = AnchorPhase::Listening;
                dev_log(&*sys, 3, b"[data_anc] bound".as_ptr(), 16);
            }
        }
        NET_MSG_ACCEPTED => {
            // ACCEPTED payload: [conn_id:u16 LE][local_port:u16 LE].
            if payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port != anchor.listen_port {
                    return;
                }
            }
            if let Some(new_id) = net_conn_id(payload) {
                if anchor.alloc_slot(new_id).is_none() {
                    let _ = net_send_close_pic(anchor, new_id);
                    dev_log(&*sys, 2, b"[data_anc] slots full".as_ptr(), 21);
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() > NET_CONN_LEN {
                if let Some(conn_id) = net_conn_id(payload) {
                    if let Some(idx) = anchor.find_slot(conn_id) {
                        handle_client_data(anchor, idx, &payload[NET_CONN_LEN..]);
                    }
                }
            }
        }
        NET_MSG_CLOSED => {
            if let Some(conn_id) = net_conn_id(payload) {
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                }
            }
        }
        NET_MSG_ERROR => {
            // Broadcast: this carries errors for connections other
            // anchors own too. Only react to our own.
            if let Some(conn_id) = net_conn_id(payload) {
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                    anchor.m_net_errors = anchor.m_net_errors.wrapping_add(1);
                }
            }
        }
        _ => {}
    }
}

unsafe fn net_send_bind_pic(anchor: &mut AnchorState) -> bool {
    let sys = anchor.syscalls;
    let out_chan = anchor.net_out;
    if sys.is_null() || out_chan < 0 {
        return false;
    }
    let port = anchor.listen_port.to_le_bytes();
    let payload = [port[0], port[1]];
    let scratch = anchor.scratch.as_mut_ptr();
    net_write_frame(
        &*sys,
        out_chan,
        NET_CMD_BIND,
        payload.as_ptr(),
        2,
        scratch,
        SCRATCH_BUF_SIZE,
    ) > 0
}

unsafe fn net_send_close_pic(anchor: &mut AnchorState, conn_id: u16) -> bool {
    let sys = anchor.syscalls;
    let out_chan = anchor.net_out;
    if sys.is_null() || out_chan < 0 {
        return false;
    }
    let payload = conn_id.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    net_write_frame(
        &*sys,
        out_chan,
        NET_CMD_CLOSE,
        payload.as_ptr(),
        NET_CONN_LEN,
        scratch,
        SCRATCH_BUF_SIZE,
    ) > 0
}

unsafe fn net_send_data_pic(anchor: &mut AnchorState, conn_id: u16, data: &[u8]) -> bool {
    let sys = anchor.syscalls;
    let out_chan = anchor.net_out;
    if sys.is_null() || out_chan < 0 {
        return false;
    }
    let payload_len = NET_CONN_LEN + data.len();
    if payload_len + NET_FRAME_HDR > SCRATCH_BUF_SIZE {
        return false;
    }
    let id = conn_id.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    *scratch = NET_CMD_SEND;
    *scratch.add(1) = (payload_len & 0xFF) as u8;
    *scratch.add(2) = ((payload_len >> 8) & 0xFF) as u8;
    *scratch.add(NET_FRAME_HDR) = id[0];
    *scratch.add(NET_FRAME_HDR + 1) = id[1];
    core::ptr::copy_nonoverlapping(
        data.as_ptr(),
        scratch.add(NET_FRAME_HDR + NET_CONN_LEN),
        data.len(),
    );
    let total = NET_FRAME_HDR + payload_len;
    ((*sys).channel_write)(out_chan, scratch, total) == total as i32
}

/// Accumulate bytes and parse out every complete frame.
///
/// A frame's length is not carried by an outer prefix: the fixed
/// `DataRequest` record comes first, and the payload length sits at a
/// known offset just past it. So the parser needs
/// `REQUEST_FRAME_HEADER_LEN` bytes before it can know how many more to
/// wait for, and that is the whole state machine.
fn handle_client_data(anchor: &mut AnchorState, slot_idx: usize, data: &[u8]) {
    {
        let slot = &mut anchor.slots[slot_idx];
        if data.len() > RECV_BUF_SIZE - slot.recv_len {
            // The peer sent more than one maximal frame's worth without
            // a frame becoming parseable, which means it is not
            // speaking this protocol.
            slot.phase = SlotPhase::Closing;
            return;
        }
        let n = slot.recv_len;
        slot.recv_buf[n..n + data.len()].copy_from_slice(data);
        slot.recv_len = n + data.len();
    }

    loop {
        let (ready, frame_len) = {
            let slot = &anchor.slots[slot_idx];
            if slot.recv_len < REQUEST_FRAME_HEADER_LEN {
                (false, 0)
            } else {
                let off = DataRequest::WIRE_LEN + 1;
                let payload_len =
                    u16::from_le_bytes([slot.recv_buf[off], slot.recv_buf[off + 1]]) as usize;
                if payload_len > DATA_FRAME_MAX_PAYLOAD {
                    // Refusing here rather than letting `decode_request_frame`
                    // do it, because an absurd length would otherwise make
                    // the parser wait for bytes that will never come.
                    (false, usize::MAX)
                } else {
                    let total = REQUEST_FRAME_HEADER_LEN + payload_len;
                    (slot.recv_len >= total, total)
                }
            }
        };
        if frame_len == usize::MAX {
            anchor.slots[slot_idx].phase = SlotPhase::Closing;
            anchor.slots[slot_idx].recv_len = 0;
            return;
        }
        if !ready {
            break;
        }

        let mut frame = [0u8; REQUEST_FRAME_HEADER_LEN + DATA_FRAME_MAX_PAYLOAD];
        frame[..frame_len].copy_from_slice(&anchor.slots[slot_idx].recv_buf[..frame_len]);
        handle_frame(anchor, slot_idx, &frame[..frame_len]);

        let slot = &mut anchor.slots[slot_idx];
        if frame_len >= slot.recv_len {
            slot.recv_len = 0;
        } else {
            slot.recv_buf.copy_within(frame_len..slot.recv_len, 0);
            slot.recv_len -= frame_len;
        }
        if slot.phase == SlotPhase::Closing {
            break;
        }
    }
}

unsafe fn flush_slots(anchor: &mut AnchorState) {
    for i in 0..MAX_CONNS {
        let (conn_id, send_len, phase) = {
            let s = &anchor.slots[i];
            (s.conn_id, s.send_len, s.phase)
        };
        if conn_id == SLOT_FREE {
            continue;
        }
        if send_len > 0 {
            // One NET_CMD_SEND per flush is bounded by the scratch
            // buffer, so send at most what fits and keep the rest.
            let take = send_len.min(SCRATCH_BUF_SIZE - NET_FRAME_HDR - NET_CONN_LEN);
            let mut tmp = [0u8; SEND_BUF_SIZE];
            tmp[..take].copy_from_slice(&anchor.slots[i].send_buf[..take]);
            if net_send_data_pic(anchor, conn_id, &tmp[..take]) {
                let s = &mut anchor.slots[i];
                if take >= s.send_len {
                    s.send_len = 0;
                } else {
                    s.send_buf.copy_within(take..s.send_len, 0);
                    s.send_len -= take;
                }
            }
        }
        if phase == SlotPhase::Closing && anchor.slots[i].send_len == 0 {
            let _ = net_send_close_pic(anchor, conn_id);
            anchor.free_slot(i);
        }
    }
}

// ── Module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<AnchorState>() as u32
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
    if state_size < core::mem::size_of::<AnchorState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let anchor = unsafe { &mut *state.cast::<AnchorState>() };
    anchor.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(anchor, params, params_len) };
    }

    anchor.net_in = in_chan;
    anchor.net_out = out_chan;

    // input order  (manifest): net_in[0], kv_in[1]
    // output order (manifest): net_out[0], kv_out[1], metrics[2]
    unsafe {
        let sys = &*sys_ptr;
        anchor.kv_in = dev_channel_port(sys, 0, 1);
        anchor.kv_out = dev_channel_port(sys, 1, 1);
        anchor.metrics_out = dev_channel_port(sys, 1, 2);
    }
    0
}

/// Scheduler re-step signal (see fluxor `StepOutcome::Burst`).
const STEP_BURST: i32 = 2;

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let anchor = unsafe { &mut *state.cast::<AnchorState>() };

    match anchor.phase {
        AnchorPhase::Init => unsafe {
            if net_send_bind_pic(anchor) {
                anchor.phase = AnchorPhase::WaitBound;
            }
        },
        AnchorPhase::WaitBound | AnchorPhase::Listening => {}
        AnchorPhase::Error => return -1,
    }

    let more = unsafe {
        let m_net = poll_net_in(anchor);
        let m_kv = poll_kv_in(anchor);
        flush_slots(anchor);

        anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
        if anchor.step_ctr.is_multiple_of(5000) && !anchor.syscalls.is_null() {
            telemetry::emit_counters(
                &*anchor.syscalls,
                anchor.metrics_out,
                &[
                    anchor.m_frames,
                    anchor.m_forwarded,
                    anchor.m_admission_refusals,
                    anchor.m_unsupported_refusals,
                    anchor.m_net_errors,
                    anchor.m_inflight_full,
                ],
            );
        }
        m_net || m_kv
    };
    if more {
        STEP_BURST
    } else {
        0
    }
}
