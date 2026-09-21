//! lattice_data_client — compute-side originator of `lattice.data`.
//!
//! RFC database foundation §14.8, §14.9, §26 phase 9.
//!
//! ## A drop-in for the router
//!
//! `relational_executor` and the model workers already speak
//! `MSG_KV_REQUEST` / `MSG_KV_RESPONSE` to `kv_request_router`. This
//! module accepts exactly those envelopes, carries them to a remote
//! storage graph as `lattice.data` frames, and hands back exactly those
//! envelopes with the fence tail intact.
//!
//! That is deliberate and it is the point. §30 asks whether combined
//! and separated placements produce equivalent histories, and the
//! cleanest way to make that answerable is for the layer above the
//! boundary to be unable to tell which side of it it is on. Same
//! executor, same envelopes, one wiring change. A bespoke compute-side
//! interface would have made the two placements different in a second
//! way, and then any difference in the histories would have had two
//! candidate explanations instead of none.
//!
//! It is also the first module in this repository that DIALS rather
//! than binds, because its peer is a storage graph rather than a
//! client.
//!
//! ## What it owns
//!
//! - **Multiplexing.** Many requests in flight on one connection, each
//!   correlated by `request_id`. Responses may return in any order.
//! - **Fence validation.** §21 invariant 11 says a fence is valid only
//!   with the right source identity and a live epoch. A response whose
//!   `source_epoch` is BELOW one already seen from this connection came
//!   from an authority that has since been superseded, and serving it
//!   would let a caller observe the past after observing the present.
//!   Those are refused and counted, not forwarded.
//! - **Retry classification.** Every refusal arrives as a typed
//!   `DataRetryClass` and leaves as the canonical KV result that means
//!   the same thing to compute.
//!
//! ## What it does not own yet, and why each is named
//!
//! §14.9 also assigns this module a routing cache, cancellation, and
//! continuation. None is silently faked:
//!
//! - **Routing cache.** `resolve_span` is the operation that produces a
//!   route, and the storage anchor refuses it — a storage graph has no
//!   topology service to answer from. So the range identity comes from
//!   the `range_id` / `range_generation` parameters. In a single-range
//!   deployment that IS the correct route, and it is the honest shape
//!   of "no cache yet": one configured entry rather than an invented
//!   lookup. When `subscribe_topology` exists, this is what it replaces.
//! - **Session identity.** §14.9 puts the session directory in compute,
//!   and it does not exist yet, so `session_id` is a parameter too.
//!   `admit_request` requires one for every operation but `open`, and
//!   supplying a configured value is the difference between "this
//!   deployment has one session" and "admission was made to pass".
//! - **Cancellation.** There is no cancel path from compute to cancel
//!   through, and `cancel` is refused by the anchor regardless.
//! - **Automatic resend.** Refusals are classified but not re-issued.
//!   Resending needs the request retained, and the caller already
//!   retains it — the executor still has the statement. Keeping a
//!   second copy here to re-drive would put a second retry policy in
//!   the system, and two retry policies disagree eventually.
//!
//! Each of these is a real gap. What matters is that a caller can tell
//! it is a gap: a refused `resolve_span` says `UnsupportedCapability`,
//! not "no route found".

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

#[path = "../../common/authority.rs"]
mod authority;

use abi::contracts::net::net_proto::{
    conn_id, connected_parts, error_parts, CMD_CLOSE as NET_CMD_CLOSE,
    CMD_CONNECT_TO as NET_CMD_CONNECT_TO, CMD_SEND as NET_CMD_SEND, CONNECT_TO_MAX, CONN_ID_LEN,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_CONNECTED as NET_MSG_CONNECTED, MSG_DATA as NET_MSG_DATA,
    MSG_ERROR as NET_MSG_ERROR, REQUESTER_TAG_NONE,
};
use authority::{Authority, PORT_LATTICE_DATA};

use data_surface::{
    decode_response_frame, encode_request_frame, isolation_for, read_policy, Consistency, DataOp,
    DataRequest, DataRetryClass, Durability, Isolation, Outcome, RequestContext,
    DATA_FRAME_MAX_PAYLOAD, REQUEST_FRAME_HEADER_LEN, REQUEST_FRAME_MAX_LEN,
    RESPONSE_FRAME_HEADER_LEN,
};
use types::{
    KV_OP_APPEND, KV_OP_CAS, KV_OP_DECR, KV_OP_DELETE, KV_OP_EXISTS, KV_OP_FLUSH, KV_OP_GET,
    KV_OP_GET_AT, KV_OP_INCR, KV_OP_MGET, KV_OP_MSET, KV_OP_PREPEND, KV_OP_PUT, KV_OP_RANGE,
    KV_OP_RANGE_SCAN, KV_OP_SCAN, KV_OP_SCAN_AT, KV_OP_SCAN_VERSIONS, KV_OP_STRLEN, KV_OP_TXN,
    KV_OP_TXN_PREPARE, KV_OP_TXN_RECORD, KV_OP_TXN_RESOLVE, KV_RESULT_CROSS_RANGE,
    KV_RESULT_INTERNAL, KV_RESULT_LIN_BOUND, KV_RESULT_OK, KV_RESULT_QUOTA,
};
use wire::{KV_RESPONSE_FENCE_TAIL_LEN, MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Capacities ────────────────────────────────────────────────────────

/// Requests in flight on the one connection to the storage graph.
const MAX_INFLIGHT: usize = 128;

/// Reassembly buffer for inbound response frames.
const RECV_BUF_SIZE: usize = RESPONSE_FRAME_HEADER_LEN + DATA_FRAME_MAX_PAYLOAD + 256;

const SCRATCH_BUF_SIZE: usize = REQUEST_FRAME_MAX_LEN + 256;

/// Steps between dial attempts while disconnected. At `tick_us=1000`
/// this is about two seconds, matching `peer_router`'s reconnect pace —
/// slow enough not to spin on a storage graph that is still booting.
const REDIAL_STEPS: u32 = 2000;

/// u16 conn-id sentinel: the ip stack's ids are monotone u16s, so
/// 0xFFFF is unreachable long before the tables bind.
const NO_CONN: u16 = 0xFFFF;

define_params! {
    ClientState;

    // Tags 1, 2, 8, 9, 10 and 11 are retired.

    // The route this client addresses. Stands in for the routing cache
    // §14.9 will own — see the module header. Written into the low 8
    // bytes of the 16-byte range identity.
    // u32 rather than u64 because `define_params!` has no u64 type.
    // Not a constraint worth working around: this is a configured
    // stand-in for a routing cache, not a real range identity, and the
    // real one arrives as 16 bytes from `subscribe_topology`.
    3, range_id, u32, 1
        => |s, d, len| { s.range_id = p_u32(d, len, 0, 1); };

    4, range_generation, u32, 1
        => |s, d, len| { s.range_generation = p_u32(d, len, 0, 1); };

    // Session identity. Stands in for the session directory §14.9 puts
    // in compute — see the module header.
    5, session_id, u32, 1
        => |s, d, len| { s.session_id = p_u32(d, len, 0, 1); };

    6, session_epoch, u32, 1
        => |s, d, len| { s.session_epoch = p_u32(d, len, 0, 1); };

    // `admit_request` refuses a request with no database identity.
    7, database_id, u32, 1
        => |s, d, len| { s.database_id = p_u32(d, len, 0, 1); };

    // The storage graph's data anchor, `host[:port]`; the port defaults
    // to 7432. Required: a client with nowhere to dial is a wiring
    // error, and saying so at init is cheaper than a graph that looks
    // healthy and serves nobody.
    12, authority, str, 0 => |s, d, len| {
        if len > 0 {
            s.peers[0].set(core::slice::from_raw_parts(d, len));
        }
    };

    // Additional storage replicas (RFC §30's three-replica range), each
    // an optional `host[:port]` with the same default port.
    //
    // The client talks to ONE peer at a time and rotates on evidence —
    // see `note_authority_refusal`. It does not fan out: the leader is
    // the only replica that can serve writes, and probing all three
    // per request would triple load to learn what one typed refusal
    // already says.
    13, authority2, str, 0 => |s, d, len| {
        if len > 0 {
            s.peers[1].set(core::slice::from_raw_parts(d, len));
        }
    };
    14, authority3, str, 0 => |s, d, len| {
        if len > 0 {
            s.peers[2].set(core::slice::from_raw_parts(d, len));
        }
    };
}

// ── State ─────────────────────────────────────────────────────────────

/// One request handed to the storage graph, reduced to what returning
/// its answer to compute needs. `corr_id` and `conn_id` are the
/// caller's and travel back verbatim, exactly as the router preserves
/// them on the combined path.
#[repr(C)]
struct Inflight {
    in_use: bool,
    request_id: u64,
    corr_id: u64,
    conn_id: u8,
    /// The op changes durable state. Only mutation outcomes drive peer
    /// rotation — see `note_authority_refusal` for why reads must not
    /// vote.
    mutation: bool,
}

#[repr(C)]
struct ClientState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,

    /// The configured storage peers; peer 0 is required, the others
    /// exist iff configured.
    peers: [Authority; 3],
    /// Index of the peer currently dialled (0..3).
    peer_idx: u8,
    /// Requester tag on every dial; `MSG_CONNECTED` / `MSG_ERROR` echo it.
    tag: u8,
    /// Consecutive MUTATION refusals with `UnavailableAuthority`.
    /// Rotation evidence — see the handler in `handle_response_frame`.
    authority_refusals: u8,
    /// A dial went out and no MSG_CONNECTED has come back. If it is still set
    /// when the next redial period arrives, the peer is unreachable and
    /// rotation advances past it. Without this a DEAD peer is a trap:
    /// a failed outbound dial produces no event, so nothing else ever
    /// moves `peer_idx` off the corpse — observed as a compute graph
    /// re-dialling its killed storage node every two seconds forever
    /// while two healthy replicas sat next to it.
    dial_unanswered: bool,
    range_id: u32,
    range_generation: u32,
    session_id: u32,
    session_epoch: u32,
    database_id: u32,

    /// Connection to the storage graph, or `NO_CONN` while dialling.
    conn_id: u16,
    /// Steps since the last dial attempt.
    since_dial: u32,
    next_request_id: u64,

    /// Highest fence epoch observed on this connection. A response
    /// below it came from a superseded authority (§21 invariant 11).
    seen_epoch: u64,

    recv_len: usize,
    recv_buf: [u8; RECV_BUF_SIZE],

    inflight: [Inflight; MAX_INFLIGHT],

    m_requests: u64,
    m_responses: u64,
    m_dials: u64,
    m_disconnects: u64,
    m_inflight_full: u64,
    m_stale_fences: u64,
    m_refusals: u64,
    /// Peer rotations. Zero in steady state; one per leader change.
    /// Climbing without leader changes means the threshold is too low
    /// or a peer list names a permanently dead node.
    m_rotations: u64,
    step_ctr: u64,

    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl ClientState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.net_in = -1;
        self.net_out = -1;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.peers = [Authority::empty(), Authority::empty(), Authority::empty()];
        self.peer_idx = 0;
        self.tag = 0;
        self.authority_refusals = 0;
        self.dial_unanswered = false;
        self.range_id = 1;
        self.range_generation = 1;
        self.session_id = 1;
        self.session_epoch = 1;
        self.database_id = 1;
        self.conn_id = NO_CONN;
        // Dial on the first step rather than after a full interval.
        self.since_dial = REDIAL_STEPS;
        self.next_request_id = 1;
        self.seen_epoch = 0;
        self.recv_len = 0;
        for i in 0..MAX_INFLIGHT {
            self.inflight[i].in_use = false;
        }
        self.m_requests = 0;
        self.m_responses = 0;
        self.m_dials = 0;
        self.m_disconnects = 0;
        self.m_inflight_full = 0;
        self.m_stale_fences = 0;
        self.m_refusals = 0;
        self.m_rotations = 0;
        self.step_ctr = 0;
    }

    fn next_request_id(&mut self) -> u64 {
        let n = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        if self.next_request_id == 0 {
            self.next_request_id = 1;
        }
        n
    }

    /// The `i`th configured peer, or None. Peer 0 always exists.
    fn peer(&self, i: u8) -> Option<&Authority> {
        match i {
            0 => Some(&self.peers[0]),
            1 | 2 if self.peers[i as usize].is_set() => Some(&self.peers[i as usize]),
            _ => None,
        }
    }

    fn peer_count(&self) -> u8 {
        1 + self.peers[1].is_set() as u8 + self.peers[2].is_set() as u8
    }

    /// Advance to the next configured peer and dial it on the next
    /// step. Called on rotation evidence and on disconnect.
    fn next_peer(&mut self) {
        self.peer_idx = (self.peer_idx + 1) % self.peer_count();
        self.authority_refusals = 0;
        self.since_dial = REDIAL_STEPS;
    }

    fn track(&mut self, request_id: u64, corr_id: u64, conn_id: u8, mutation: bool) -> bool {
        let Some(i) = (0..MAX_INFLIGHT).find(|&i| !self.inflight[i].in_use) else {
            return false;
        };
        self.inflight[i] = Inflight {
            in_use: true,
            request_id,
            corr_id,
            conn_id,
            mutation,
        };
        true
    }

    fn take(&mut self, request_id: u64) -> Option<(u64, u8, bool)> {
        let i = (0..MAX_INFLIGHT)
            .find(|&i| self.inflight[i].in_use && self.inflight[i].request_id == request_id)?;
        self.inflight[i].in_use = false;
        Some((
            self.inflight[i].corr_id,
            self.inflight[i].conn_id,
            self.inflight[i].mutation,
        ))
    }

    /// The 16-byte range identity this client addresses.
    fn range_bytes(&self) -> [u8; 16] {
        let mut r = [0u8; 16];
        r[..4].copy_from_slice(&self.range_id.to_le_bytes());
        r
    }
}

// ── KV opcode → surface operation ─────────────────────────────────────

/// Which `DataOp` carries this canonical KV opcode.
///
/// The mapping is coarse on purpose. `DataOp::Write` covers put,
/// delete, CAS and increment because the surface's job is to say what
/// KIND of thing is happening — a mutation that must be discoverable if
/// its response is lost — while the opcode beside it says exactly which.
/// Splitting `Write` into four would duplicate the opcode's information
/// in a second place, and the two would drift.
///
/// `None` is an opcode with no surface meaning. It is refused rather
/// than guessed: `KV_OP_TARGETED` is a lifecycle-bootstrap tool that
/// only the range supervisor may issue, and it must not become
/// reachable just because a compute module happened to emit it.
const fn op_for(kv_op: u8) -> Option<DataOp> {
    match kv_op {
        KV_OP_GET | KV_OP_GET_AT | KV_OP_EXISTS | KV_OP_STRLEN => Some(DataOp::GetAt),
        KV_OP_MGET => Some(DataOp::BatchGet),
        KV_OP_SCAN | KV_OP_SCAN_AT | KV_OP_RANGE | KV_OP_RANGE_SCAN | KV_OP_SCAN_VERSIONS => {
            Some(DataOp::ScanOpen)
        }
        KV_OP_PUT | KV_OP_DELETE | KV_OP_CAS | KV_OP_INCR | KV_OP_DECR | KV_OP_APPEND
        | KV_OP_PREPEND | KV_OP_MSET | KV_OP_FLUSH | KV_OP_TXN | KV_OP_TXN_RECORD => {
            Some(DataOp::Write)
        }
        KV_OP_TXN_PREPARE => Some(DataOp::Prepare),
        KV_OP_TXN_RESOLVE => Some(DataOp::Commit),
        _ => None,
    }
}

/// The canonical KV result that means to compute what this refusal
/// class means on the surface.
///
/// The mapping is lossy in one direction and that is exactly why the
/// surface has its own vocabulary: `DataRetryClass` distinguishes
/// cancellation from a deadline from an ambiguous delivery, and the KV
/// result codes — designed for a local call that either returns or does
/// not — have no way to say those. What survives is the part compute
/// acts on: refresh your route, back off, the authority is unavailable,
/// or stop.
const fn kv_result_for(class: DataRetryClass) -> u8 {
    match class {
        // Compute already knows this one: re-resolve and retry.
        DataRetryClass::StaleRoute => KV_RESULT_CROSS_RANGE,
        // "Back off and come back" — the same action a quota refusal
        // asks for. Not TXN_PENDING, which carries an intent record
        // compute would try to read.
        DataRetryClass::RetryableContention => KV_RESULT_QUOTA,
        // The owning authority could not answer.
        DataRetryClass::UnavailableAuthority | DataRetryClass::DeadlineExpired => {
            KV_RESULT_LIN_BOUND
        }
        // Nothing compute can do differently. `AmbiguousDelivery` lands
        // here too, which UNDERSTATES it — the mutation may have
        // applied — but the surface response carries the truth and the
        // KV envelope has no code for "maybe". Discovering the real
        // outcome is what the idempotency identity is for.
        DataRetryClass::Malformed
        | DataRetryClass::UnsupportedCapability
        | DataRetryClass::Cancelled
        | DataRetryClass::AmbiguousDelivery => KV_RESULT_INTERNAL,
    }
}

// ── Compute → storage ─────────────────────────────────────────────────

unsafe fn poll_kv_in(client: &mut ClientState) -> bool {
    if client.kv_in < 0 {
        return false;
    }
    let sys = client.syscalls;
    if sys.is_null() {
        return false;
    }
    const PER_TICK_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    while processed < PER_TICK_BUDGET {
        let poll = ((*sys).channel_poll)(client.kv_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return false;
        }
        if !handle_kv_request(client) {
            return false;
        }
        processed += 1;
    }
    true
}

unsafe fn handle_kv_request(client: &mut ClientState) -> bool {
    let sys = client.syscalls;
    let mut hdr = [0u8; 3];
    if ((*sys).channel_read)(client.kv_in, hdr.as_mut_ptr(), 3) < 3 {
        return false;
    }
    if hdr[0] != MSG_KV_REQUEST {
        diag(client, b"[data_cli] mt?", hdr[0], 0);
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    // [corr:8][proto:1][tenant:4][conn:1][cons:1][op:1][body_len:2] = 18
    const REQ_HEAD: usize = 18;
    if !(REQ_HEAD..=SCRATCH_BUF_SIZE).contains(&payload_len) {
        return false;
    }
    if (((*sys).channel_read)(client.kv_in, client.scratch.as_mut_ptr(), payload_len) as usize)
        < payload_len
    {
        return false;
    }

    let (corr_id, tenant_id, conn_id, consistency, kv_op, body_len) = {
        let p = &client.scratch[..payload_len];
        (
            u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]]),
            u32::from_le_bytes([p[9], p[10], p[11], p[12]]),
            p[13],
            p[14],
            p[15],
            u16::from_le_bytes([p[16], p[17]]) as usize,
        )
    };
    if REQ_HEAD + body_len > payload_len || body_len > DATA_FRAME_MAX_PAYLOAD {
        diag(client, b"[data_cli] len", kv_op, 0);
        reply_refused(client, corr_id, conn_id, KV_RESULT_INTERNAL);
        return true;
    }

    let Some(op) = op_for(kv_op) else {
        // An opcode with no surface meaning. Refusing beats guessing:
        // the alternative is sending something the storage side will
        // interpret as an operation the caller never asked for.
        diag(client, b"[data_cli] op?", kv_op, 0);
        reply_refused(client, corr_id, conn_id, KV_RESULT_INTERNAL);
        return true;
    };
    // The envelope's byte is `types::REQ_*`, NOT `Consistency` — two
    // vocabularies that agree at 0x01 and nowhere else. `data_surface`
    // owns the mapping so this module and the anchor cannot drift.
    let Some(policy) = read_policy(consistency) else {
        diag(client, b"[data_cli] cons?", consistency, kv_op);
        reply_refused(client, corr_id, conn_id, KV_RESULT_INTERNAL);
        return true;
    };
    let isolation = isolation_for(consistency);

    if client.conn_id == NO_CONN {
        // Not connected. `UnavailableAuthority` shaped: nothing is
        // wrong with the request, the storage graph is simply not
        // reachable right now.
        diag(client, b"[data_cli] noconn", kv_op, 0);
        reply_refused(client, corr_id, conn_id, KV_RESULT_LIN_BOUND);
        return true;
    }

    let request_id = client.next_request_id();
    let mut request = DataRequest::EMPTY;
    request.op = op;
    request.isolation = isolation;
    request.session_id = client.session_id as u64;
    request.session_epoch = client.session_epoch;
    request.partition_map_revision = client.range_generation;
    request.range_id = client.range_bytes();
    request.range_generation = client.range_generation;
    request.partition_incarnation = 1;
    request.max_chunks = 1;
    request.context.tenant_id = if tenant_id == 0 { 1 } else { tenant_id };
    request.context.database_id = client.database_id;
    request.context.keyspace_id = 1;
    request.context.request_id = request_id;
    request.context.consistency = policy;
    request.context.durability = Durability::ReplicatedDurable;
    request.context.routing_epoch = client.range_generation;
    request.context.range_generation = client.range_generation;
    // A deadline is required for everything but a subscription. The
    // surface's deadline is wall-clock and this module has no clock, so
    // it sends the far future: the operation IS bounded, by the
    // caller's own deadline above and by the storage graph's, and a
    // fabricated near deadline would expire operations that had not.
    request.context.deadline_unix_ms = u64::MAX;
    if op.returns_rows() {
        request.max_rows = u32::MAX;
        request.max_work_units = u32::MAX;
        request.context.max_response_bytes = DATA_FRAME_MAX_PAYLOAD as u32;
        request.context.max_keys = u32::MAX;
    }
    if op.is_mutation() {
        // §21 invariant 17: a retryable mutation must be discoverable.
        // The caller's correlation identity is the natural key — it is
        // already unique per request on this path and it is what the
        // caller would use to ask "did mine apply?".
        request.context.idempotency_key = corr_id;
    }
    if op.requires_transaction() {
        request.context.transaction_id = corr_id;
        request.transaction_epoch = 1;
    }

    // Reserve before sending: a response with no entry cannot be
    // answered, so the table must never be what fails after the remote
    // graph already has the work.
    if !client.track(request_id, corr_id, conn_id, op.is_mutation()) {
        client.m_inflight_full = client.m_inflight_full.wrapping_add(1);
        reply_refused(client, corr_id, conn_id, KV_RESULT_QUOTA);
        return true;
    }

    let mut body = [0u8; DATA_FRAME_MAX_PAYLOAD];
    body[..body_len].copy_from_slice(&client.scratch[REQ_HEAD..REQ_HEAD + body_len]);

    let mut frame = [0u8; REQUEST_FRAME_MAX_LEN];
    let Some(n) = encode_request_frame(&request, kv_op, &body[..body_len], &mut frame) else {
        diag(client, b"[data_cli] enc", kv_op, 0);
        let _ = client.take(request_id);
        reply_refused(client, corr_id, conn_id, KV_RESULT_INTERNAL);
        return true;
    };
    if !net_send_data(client, &frame[..n]) {
        diag(client, b"[data_cli] send", kv_op, 0);
        let _ = client.take(request_id);
        reply_refused(client, corr_id, conn_id, KV_RESULT_QUOTA);
        return true;
    }
    client.m_requests = client.m_requests.wrapping_add(1);
    true
}

// ── Storage → compute ─────────────────────────────────────────────────

/// Emit a `MSG_KV_RESPONSE` back to compute.
unsafe fn reply(
    client: &mut ClientState,
    corr_id: u64,
    conn_id: u8,
    result: u8,
    revision: u64,
    body: &[u8],
    applied_index: u64,
    applied_term: u64,
    source_id: u32,
    durability: u8,
    catalog_generation: u64,
) -> bool {
    let sys = client.syscalls;
    if sys.is_null() || client.kv_out < 0 {
        return false;
    }
    const RESP_HEAD: usize = 20;
    let payload_len = RESP_HEAD + body.len() + KV_RESPONSE_FENCE_TAIL_LEN;
    let total = 3 + payload_len;
    if payload_len > u16::MAX as usize || total > SCRATCH_BUF_SIZE {
        return false;
    }
    {
        let out = &mut client.scratch[..total];
        out[0] = MSG_KV_RESPONSE;
        out[1] = (payload_len & 0xFF) as u8;
        out[2] = ((payload_len >> 8) & 0xFF) as u8;
        let mut p = 3;
        out[p..p + 8].copy_from_slice(&corr_id.to_le_bytes());
        p += 8;
        out[p] = conn_id;
        p += 1;
        out[p] = result;
        p += 1;
        out[p..p + 8].copy_from_slice(&revision.to_le_bytes());
        p += 8;
        out[p..p + 2].copy_from_slice(&(body.len() as u16).to_le_bytes());
        p += 2;
        out[p..p + body.len()].copy_from_slice(body);
        p += body.len();
        out[p..p + 8].copy_from_slice(&applied_index.to_le_bytes());
        p += 8;
        out[p..p + 8].copy_from_slice(&applied_term.to_le_bytes());
        p += 8;
        out[p..p + 4].copy_from_slice(&source_id.to_le_bytes());
        p += 4;
        out[p] = durability;
        p += 1;
        // Catalog generation, relayed from the DataResponse. Zero when
        // the responder sent none — the value the executor's cache
        // reads as "no generation available, do not trust a cached
        // mapping". (These bytes were once left UNWRITTEN, handing the
        // executor scratch garbage that two equal reads validated a
        // stale table id across compute graphs. Always written now.)
        out[p..p + 8].copy_from_slice(&catalog_generation.to_le_bytes());
    }
    ((*sys).channel_write)(client.kv_out, client.scratch.as_mut_ptr(), total) == total as i32
}

/// Emit one diagnostic line naming a refusal site and its inputs.
///
/// Present because the alternative when this path fails is guessing
/// which of several refusal sites fired: compute sees one SQLSTATE for
/// every one of them, and a module that cannot report its own reason
/// makes silence ambiguous. The same argument `relational_executor`
/// makes for its own `diag`.
unsafe fn diag(client: &ClientState, tag: &[u8], a: u8, b: u8) {
    let sys = client.syscalls;
    if sys.is_null() {
        return;
    }
    let mut line = [0u8; 64];
    let n = tag.len().min(40);
    line[..n].copy_from_slice(&tag[..n]);
    let mut p = n;
    for v in [a, b] {
        line[p] = b' ';
        p += 1;
        for shift in [4u32, 0] {
            let d = (v >> shift) & 0xF;
            line[p] = if d < 10 { b'0' + d } else { b'a' + (d - 10) };
            p += 1;
        }
    }
    dev_log(&*sys, 2, line.as_ptr(), p);
}

/// A refusal this module produced itself — no remote execution, so no
/// observation to report.
unsafe fn reply_refused(client: &mut ClientState, corr_id: u64, conn_id: u8, result: u8) {
    client.m_refusals = client.m_refusals.wrapping_add(1);
    let _ = reply(client, corr_id, conn_id, result, 0, &[], 0, 0, 0, 0x01, 0);
}

/// Handle one complete response frame.
unsafe fn handle_response_frame(client: &mut ClientState, frame: &[u8]) {
    let Some(decoded) = decode_response_frame(frame) else {
        // Unparseable. There is no `request_id` to answer with, and the
        // stream offset is now suspect, so drop the connection and let
        // the in-flight requests fail closed on redial.
        drop_connection(client);
        return;
    };
    let response = decoded.response;
    let Some((corr_id, conn_id, mutation)) = client.take(response.request_id) else {
        // Late arrival for a request abandoned at disconnect.
        return;
    };
    client.m_responses = client.m_responses.wrapping_add(1);

    // ── Rotation evidence (§30's leader replacement) ──────────────────
    //
    // A refusal with `UnavailableAuthority` on a MUTATION is the typed
    // "this node is not the leader" signal (the gateway's NOT_LEADER
    // reject, relayed by the router in milliseconds). Three in a row
    // and the client moves to the next configured peer.
    //
    // Only mutations vote, in both directions. A follower serves DIRECT
    // -path reads happily, so on the wrong node a SQL statement's flow
    // is: name lookup (read) succeeds, then the write is refused — and
    // a counter that reads reset would oscillate 1,0,1,0 forever and
    // never rotate. Symmetrically, a read refusal proves nothing about
    // leadership. The counter is therefore mutation-only.
    //
    // Three, not one: `UnavailableAuthority` also arrives for transient
    // causes (a fence timeout under load), and rotating on a single
    // transient would bounce the client between healthy peers.
    if mutation && client.peer_count() > 1 {
        let authority_refused = response.outcome == Outcome::Refused
            && response.retry == Some(DataRetryClass::UnavailableAuthority);
        if authority_refused {
            client.authority_refusals = client.authority_refusals.saturating_add(1);
            if client.authority_refusals >= 3 {
                // Answer this op first (below), then abandon the peer.
                // `drop_connection` fails the remaining inflight closed
                // and resets the fence epoch — a new peer is a new
                // authority.
                client.m_rotations = client.m_rotations.wrapping_add(1);
                drop_connection(client);
                client.next_peer();
            }
        } else if response.outcome == Outcome::Ok {
            client.authority_refusals = 0;
        }
    }

    let f = &response.fences;

    // §21 invariant 11. An epoch below one already seen came from an
    // authority that has since been superseded; forwarding it would let
    // compute observe the past after observing the present. Refuse
    // rather than serve, and count it — a rising count is a leader
    // flapping, which is worth seeing.
    if f.fence.source_epoch != 0 && f.fence.source_epoch < client.seen_epoch {
        client.m_stale_fences = client.m_stale_fences.wrapping_add(1);
        reply_refused(client, corr_id, conn_id, KV_RESULT_CROSS_RANGE);
        return;
    }
    if f.fence.source_epoch > client.seen_epoch {
        client.seen_epoch = f.fence.source_epoch;
    }

    let result = match response.outcome {
        // The authority answered; the frame's `kv_result` is what it
        // said, and it travels through unchanged.
        Outcome::Ok => decoded.kv_result,
        Outcome::Refused | Outcome::Indeterminate | Outcome::Cancelled => {
            client.m_refusals = client.m_refusals.wrapping_add(1);
            diag(
                client,
                b"[data_cli] remote-refused",
                response.outcome as u8,
                match response.retry {
                    Some(c) => c as u8,
                    None => 0,
                },
            );
            match response.retry {
                Some(class) => kv_result_for(class),
                // `Ok` is the only outcome that admits no retry class,
                // so this is unreachable through `decode` — which
                // enforces the agreement — and INTERNAL is the right
                // answer to a response that got here anyway.
                None => KV_RESULT_INTERNAL,
            }
        }
    };

    let payload = decoded.payload;
    let _ = reply(
        client,
        corr_id,
        conn_id,
        result,
        f.observed_revision,
        payload,
        f.applied_index,
        f.fence.source_epoch,
        f.fence.source_id,
        f.durability_achieved as u8,
        response.catalog_generation,
    );
}

/// Accumulate bytes and parse out every complete response frame.
unsafe fn handle_wire_data(client: &mut ClientState, data: &[u8]) {
    if data.len() > RECV_BUF_SIZE - client.recv_len {
        // More than one maximal frame's worth arrived without a frame
        // becoming parseable: the peer is not speaking this protocol.
        drop_connection(client);
        return;
    }
    let n = client.recv_len;
    client.recv_buf[n..n + data.len()].copy_from_slice(data);
    client.recv_len = n + data.len();

    loop {
        if client.recv_len < RESPONSE_FRAME_HEADER_LEN {
            return;
        }
        let off = RESPONSE_FRAME_HEADER_LEN - 2;
        let payload_len =
            u16::from_le_bytes([client.recv_buf[off], client.recv_buf[off + 1]]) as usize;
        if payload_len > DATA_FRAME_MAX_PAYLOAD {
            drop_connection(client);
            return;
        }
        let total = RESPONSE_FRAME_HEADER_LEN + payload_len;
        if client.recv_len < total {
            return;
        }
        let mut frame = [0u8; RESPONSE_FRAME_HEADER_LEN + DATA_FRAME_MAX_PAYLOAD];
        frame[..total].copy_from_slice(&client.recv_buf[..total]);
        handle_response_frame(client, &frame[..total]);
        if client.conn_id == NO_CONN {
            return;
        }
        if total >= client.recv_len {
            client.recv_len = 0;
        } else {
            client.recv_buf.copy_within(total..client.recv_len, 0);
            client.recv_len -= total;
        }
    }
}

// ── Connection ────────────────────────────────────────────────────────

/// Abandon the connection and every request riding on it.
///
/// The in-flight entries are cleared rather than left to time out. A
/// request whose connection is gone has no answer coming, and holding
/// its slot would leak the table across a flapping link.
unsafe fn drop_connection(client: &mut ClientState) {
    if client.conn_id != NO_CONN {
        let payload = client.conn_id.to_le_bytes();
        let scratch = client.scratch.as_mut_ptr();
        let sys = client.syscalls;
        if !sys.is_null() && client.net_out >= 0 {
            let _ = net_write_frame(
                &*sys,
                client.net_out,
                NET_CMD_CLOSE,
                payload.as_ptr(),
                CONN_ID_LEN,
                scratch,
                SCRATCH_BUF_SIZE,
            );
        }
    }
    client.conn_id = NO_CONN;
    client.recv_len = 0;
    client.m_disconnects = client.m_disconnects.wrapping_add(1);
    // Fail every outstanding request closed. `UnavailableAuthority`
    // shaped: the request may or may not have applied, and the caller's
    // idempotency identity is how it finds out — which is exactly the
    // discovery path §28 requires instead of a silent hang.
    for i in 0..MAX_INFLIGHT {
        if !client.inflight[i].in_use {
            continue;
        }
        let (corr, conn) = (client.inflight[i].corr_id, client.inflight[i].conn_id);
        client.inflight[i].in_use = false;
        let _ = reply(
            client,
            corr,
            conn,
            KV_RESULT_LIN_BOUND,
            0,
            &[],
            0,
            0,
            0,
            0x01,
            0,
        );
    }
    // A new connection is a new authority; epoch comparisons start over.
    client.seen_epoch = 0;
}

unsafe fn dial(client: &mut ClientState) {
    let sys = client.syscalls;
    if sys.is_null() || client.net_out < 0 {
        return;
    }
    let poll = ((*sys).channel_poll)(client.net_out, POLL_OUT);
    if poll <= 0 || (poll as u32) & POLL_OUT == 0 {
        return;
    }
    let mut payload = [0u8; CONNECT_TO_MAX];
    let n = match client.peer(client.peer_idx) {
        Some(peer) => peer.connect_record(&mut payload, Some(client.tag)),
        None => {
            client.peer_idx = 0;
            return;
        }
    };
    if n == 0 {
        return;
    }
    let scratch = client.scratch.as_mut_ptr();
    let _ = net_write_frame(
        &*sys,
        client.net_out,
        NET_CMD_CONNECT_TO,
        payload.as_ptr(),
        n,
        scratch,
        SCRATCH_BUF_SIZE,
    );
    client.m_dials = client.m_dials.wrapping_add(1);
}

unsafe fn net_send_data(client: &mut ClientState, data: &[u8]) -> bool {
    let sys = client.syscalls;
    if sys.is_null() || client.net_out < 0 || client.conn_id == NO_CONN {
        return false;
    }
    let payload_len = CONN_ID_LEN + data.len();
    if payload_len + NET_FRAME_HDR > SCRATCH_BUF_SIZE {
        return false;
    }
    let id = client.conn_id.to_le_bytes();
    let scratch = client.scratch.as_mut_ptr();
    *scratch = NET_CMD_SEND;
    *scratch.add(1) = (payload_len & 0xFF) as u8;
    *scratch.add(2) = ((payload_len >> 8) & 0xFF) as u8;
    *scratch.add(NET_FRAME_HDR) = id[0];
    *scratch.add(NET_FRAME_HDR + 1) = id[1];
    core::ptr::copy_nonoverlapping(
        data.as_ptr(),
        scratch.add(NET_FRAME_HDR + CONN_ID_LEN),
        data.len(),
    );
    let total = NET_FRAME_HDR + payload_len;
    ((*sys).channel_write)(client.net_out, scratch, total) == total as i32
}

unsafe fn poll_net_in(client: &mut ClientState) -> bool {
    if client.net_in < 0 {
        return false;
    }
    let sys = client.syscalls;
    if sys.is_null() {
        return false;
    }
    const PER_TICK_BUDGET: u32 = 32;
    let mut processed: u32 = 0;
    while processed < PER_TICK_BUDGET {
        let poll = ((*sys).channel_poll)(client.net_in, POLL_IN);
        if poll <= 0 || (poll as u32) & POLL_IN == 0 {
            return false;
        }
        let buf = client.scratch.as_mut_ptr();
        let (msg_type, payload_len) = net_read_frame(&*sys, client.net_in, buf, SCRATCH_BUF_SIZE);
        if msg_type == 0 && payload_len == 0 {
            return false;
        }
        let mut tmp = [0u8; SCRATCH_BUF_SIZE];
        let copy_len = payload_len.min(SCRATCH_BUF_SIZE);
        core::ptr::copy_nonoverlapping(buf.add(NET_FRAME_HDR), tmp.as_mut_ptr(), copy_len);
        dispatch_net_frame(client, msg_type, &tmp[..copy_len]);
        processed += 1;
    }
    true
}

unsafe fn dispatch_net_frame(client: &mut ClientState, msg_type: u8, payload: &[u8]) {
    match msg_type {
        NET_MSG_CONNECTED => {
            // `net_out` is broadcast: claim only the dial that carries
            // our tag.
            if client.conn_id == NO_CONN && payload.len() >= CONN_ID_LEN {
                let (new_id, tag) = connected_parts(payload);
                if tag == client.tag || tag == REQUESTER_TAG_NONE {
                    client.conn_id = new_id;
                    client.recv_len = 0;
                    client.dial_unanswered = false;
                    let sys = client.syscalls;
                    if !sys.is_null() {
                        dev_log(&*sys, 3, b"[data_cli] connected".as_ptr(), 20);
                    }
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() > CONN_ID_LEN && conn_id(payload) == client.conn_id {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                let n = (payload.len() - CONN_ID_LEN).min(SCRATCH_BUF_SIZE);
                tmp[..n].copy_from_slice(&payload[CONN_ID_LEN..CONN_ID_LEN + n]);
                handle_wire_data(client, &tmp[..n]);
            }
        }
        // `net_out` is broadcast, so both of these also carry events for
        // connections other modules own. Only react to our own.
        NET_MSG_CLOSED => {
            if payload.len() >= CONN_ID_LEN && conn_id(payload) == client.conn_id {
                lost_connection(client);
            }
        }
        NET_MSG_ERROR => {
            // A tagged error is a failed dial and is ours by tag alone;
            // an untagged one names an established connection by conn
            // id.
            if payload.len() > CONN_ID_LEN {
                let (id, _errno, tag) = error_parts(payload);
                if client.conn_id == NO_CONN {
                    if tag == client.tag || tag == REQUESTER_TAG_NONE {
                        // The redial period's rotation moves past the
                        // peer; nothing to tear down.
                        let sys = client.syscalls;
                        if !sys.is_null() {
                            dev_log(&*sys, 2, b"[data_cli] dial refused".as_ptr(), 23);
                        }
                    }
                } else if tag == REQUESTER_TAG_NONE && id == client.conn_id {
                    lost_connection(client);
                }
            }
        }
        _ => {}
    }
}

/// The connection is gone. A dead leader presents as a disconnect, not
/// as a refusal: move on rather than re-dial a corpse; if the peer was
/// healthy the rotation comes back round to it.
unsafe fn lost_connection(client: &mut ClientState) {
    drop_connection(client);
    if client.peer_count() > 1 {
        client.next_peer();
    }
}

// ── Module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ClientState>() as u32
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
    if state_size < core::mem::size_of::<ClientState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let client = unsafe { &mut *state.cast::<ClientState>() };
    client.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(client, params, params_len) };
    }

    client.net_in = in_chan;
    client.net_out = out_chan;

    // input order  (manifest): net_in[0], kv_in[1]
    // output order (manifest): net_out[0], kv_out[1], metrics[2]
    unsafe {
        let sys = &*sys_ptr;
        client.kv_in = dev_channel_port(sys, 0, 1);
        client.kv_out = dev_channel_port(sys, 1, 1);
        client.metrics_out = dev_channel_port(sys, 1, 2);
        client.tag = dev_requester_tag(sys);
        if !client.peers[0].adopt(PORT_LATTICE_DATA) {
            let m = b"[data_cli] refusing to construct: authority (host[:port]) is required";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if client.peers[1].offered() && !client.peers[1].adopt(PORT_LATTICE_DATA) {
            let m = b"[data_cli] refusing to construct: authority2 is not host[:port]";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if client.peers[2].offered() && !client.peers[2].adopt(PORT_LATTICE_DATA) {
            let m = b"[data_cli] refusing to construct: authority3 is not host[:port]";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
    }

    // A client with no compute attached would dial a storage graph and
    // sit there. Refuse: the wiring is wrong and saying so at init is
    // cheaper than a graph that looks healthy and serves nobody.
    if client.kv_in < 0 || client.kv_out < 0 {
        return -1;
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
    let client = unsafe { &mut *state.cast::<ClientState>() };
    if client.syscalls.is_null() {
        return 0;
    }

    let more = unsafe {
        if client.conn_id == NO_CONN {
            client.since_dial = client.since_dial.saturating_add(1);
            if client.since_dial >= REDIAL_STEPS {
                client.since_dial = 0;
                // A whole redial period with no MSG_CONNECTED: the peer is
                // unreachable (dead node, refused connect). Move on —
                // a healthy peer that was skipped comes back round.
                if client.dial_unanswered && client.peer_count() > 1 {
                    client.peer_idx = (client.peer_idx + 1) % client.peer_count();
                    client.authority_refusals = 0;
                }
                client.dial_unanswered = true;
                dial(client);
            }
        }

        let m_net = poll_net_in(client);
        // Requests are drained even while disconnected: each gets a
        // typed refusal instead of queueing behind a link that may
        // never come back. A caller that is told "unavailable" can
        // decide; a caller that is told nothing cannot.
        let m_kv = poll_kv_in(client);

        client.step_ctr = client.step_ctr.wrapping_add(1);
        if client.step_ctr.is_multiple_of(5000) {
            telemetry::emit_counters(
                &*client.syscalls,
                client.metrics_out,
                &[
                    client.m_requests,
                    client.m_responses,
                    client.m_dials,
                    client.m_disconnects,
                    client.m_inflight_full,
                    client.m_stale_fences,
                    client.m_refusals,
                    client.m_rotations,
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
