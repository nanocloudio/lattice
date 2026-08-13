//! etcd_edge_anchor — gRPC/HTTP2 edge anchor for etcd v3 KV.
//!
//! Continuity (per `.context/native_fluxor.md`):
//! - Watch + LeaseKeepAlive: `edge_anchored` (deferred — Phase 4)
//! - Unary KV (Range / Put / DeleteRange): `drain_only`
//!
//! ## What this module owns
//!
//! - TCP bind + accept against foundation/ip (same NET_CMD state
//!   machine the redis anchor uses; collapses to a thin facade once
//!   foundation `stream_anchor_core` is promoted).
//! - Per-connection slot table with HTTP/2 state: preface check,
//!   frame parser, HPACK decoder, per-stream request body buffer.
//! - Translation of the etcd v3 KV subset
//!   (`/etcdserverpb.KV/{Range,Put,DeleteRange}`) into protocol-neutral
//!   `KV_OP_*` envelopes on `kv_out`. Responses arrive on `routed_in`
//!   as `MSG_KV_RESPONSE` and get re-encoded back as gRPC HEADERS +
//!   DATA + trailing HEADERS frames.
//!
//! ## What this module does NOT own
//!
//! - HPACK dynamic table tracking — we advertise
//!   `SETTINGS_HEADER_TABLE_SIZE = 0` so a conforming gRPC client
//!   never emits dynamic-table references and the decoder stays a
//!   pure function over the static table + literals (see
//!   `modules/common/etcd_codec.rs §HPACK`).
//! - Watch / Lease streams — those land in Phase 4 once the
//!   `control_plane.epoch_events` substrate enhancement is in
//!   place. The manifest exposes `watch_in` / `lease_in` ports so
//!   the wiring is forward-compatible.
//! - HTTP/2 strict compliance — PRIORITY / PUSH_PROMISE /
//!   CONTINUATION are not implemented. etcd-client doesn't emit them.
//! - TLS — handled upstream by foundation/tls when the secure
//!   provides capability is wired in front of us.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface. unreachable_patterns: defensive `_ =>` fallthroughs keep the parser strict against unexpected envelopes"
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

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/etcd_codec.rs"]
mod etcd_codec;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use etcd_codec::{
    build_grpc_trailers_block, build_lease_grant_response, build_lease_keepalive_response,
    build_lease_revoke_response, build_put_response, build_range_response,
    build_response_headers_block, build_watch_response_header, preface_matches, write_watch_event,
    DeleteRangeRequest, FrameHeader, FrameType, GrpcMessageHeader, HpackDecoder, LeaseGrantRequest,
    LeaseKeepAliveRequest, LeaseRevokeRequest, PutRequest, RangeRequest, SettingsIter,
    WatchEventKind, WatchOp, WatchRequest, ERROR_NO_ERROR, ERROR_PROTOCOL_ERROR, FLAG_ACK,
    FLAG_END_HEADERS, FLAG_END_STREAM, FRAME_HEADER_LEN, GRPC_FRAME_HEADER_LEN,
    GRPC_STATUS_COMPACTED, HTTP2_PREFACE_LEN,
};
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_GET_AT, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_RESULT_COMPACTED,
    KV_RESULT_INTEGER, KV_RESULT_NOT_FOUND, KV_RESULT_OK, KV_RESULT_RANGE, PROTO_ETCD,
};
use wire::{
    MSG_KV_REQUEST, MSG_KV_RESPONSE, MSG_LEASE_CTRL, MSG_LEASE_STATE, MSG_WATCH_CTRL,
    MSG_WATCH_FRAME,
};

const LEASE_CTRL_GRANT: u8 = 0;
const LEASE_CTRL_REVOKE: u8 = 1;
const LEASE_CTRL_KEEPALIVE: u8 = 2;

// MSG_WATCH_CTRL ctrl byte (see modules/common/wire.rs).
const WATCH_CTRL_CREATE: u8 = 0;
const WATCH_CTRL_CANCEL: u8 = 2;

// ── NET protocol constants (foundation/ip Stream Surface v1) ──────────

#[path = "../../common/net_proto.rs"]
mod net_proto;
use net_proto::{
    net_conn_id, NET_CMD_BIND, NET_CMD_CLOSE, NET_CMD_SEND, NET_CONN_LEN, NET_MSG_ACCEPTED,
    NET_MSG_BOUND, NET_MSG_CLOSED, NET_MSG_DATA, NET_MSG_ERROR,
};

// NET_CMD_* live in modules/common/net_proto.rs (imported above).

// ── Capacities ────────────────────────────────────────────────────────

/// Active client slots. Matched to the redis anchor (64) so the
/// 32-/64-client concurrent loadtests don't hit the anchor's own
/// slot cap before they hit the per-tick channel-hop ceiling in
/// `kv_state_worker`. State arena cost per slot is dominated by
/// the per-stream body_buf table (8 × 2 KiB = 16 KiB) plus
/// recv+send buffers — total ≈ 8 KiB + 16 KiB = 24 KiB per slot,
/// 64 × 24 KiB ≈ 1.5 MiB resident.
const MAX_CONNS: usize = 64;

/// Concurrent in-flight streams per connection. gRPC clients
/// typically pipeline a handful at a time; etcd-client uses 1-4.
const MAX_STREAMS_PER_CONN: usize = 8;

/// Per-slot recv buffer (frames accumulate here before parsing).
const RECV_BUF_SIZE: usize = 4096;

/// Per-slot send buffer (response frames awaiting wire delivery).
const SEND_BUF_SIZE: usize = 4096;

/// Per-stream request body buffer. Sized to hold an etcd Put with
/// a 2 KiB value (we cap KV values at 4 KiB in `kv_store`, but
/// most clients use much smaller values; growing this past 2 KiB
/// would balloon `MAX_CONNS × MAX_STREAMS_PER_CONN × STREAM_BODY_MAX`
/// past the arena budget — 64 × 8 × 2 KiB = 1 MiB today).
const STREAM_BODY_MAX: usize = 2048;

/// HPACK scratch — large enough for several KB of decoded headers
/// (one HEADERS block worth, since we reset between blocks).
const HPACK_SCRATCH_LEN: usize = 4096;

/// Scratch for NET_CMD frames and KV envelopes.
const SCRATCH_BUF_SIZE: usize = 4096 + 64;

const DEFAULT_LISTEN_PORT: u16 = 2379;
const DEFAULT_TENANT: u32 = 0;

const SLOT_FREE: u16 = 0xFFFF;
const STREAM_FREE: u32 = 0;

// HPACK static-table indices for the response header set.
// `:status 200` is index 8 (we emit it as Indexed) — see
// `etcd_codec::HPACK_STATIC_TABLE`.

/// gRPC method discriminator stored on a `StreamSlot`. The path
/// string (`/etcdserverpb.KV/Range`, …) is compared once at HEADERS
/// time; subsequent DATA frames just append to body_buf.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum GrpcMethod {
    Unknown = 0,
    Range = 1,
    Put = 2,
    DeleteRange = 3,
    /// `/etcdserverpb.Lease/LeaseGrant` — unary; forwarded to
    /// `lease_manager` via `lease_ctrl_out` (MSG_LEASE_CTRL ctrl=grant).
    LeaseGrant = 4,
    /// `/etcdserverpb.Lease/LeaseRevoke` — unary; forwarded as
    /// MSG_LEASE_CTRL ctrl=revoke.
    LeaseRevoke = 5,
    /// `/etcdserverpb.Watch/Watch` — server-streaming. The client
    /// sends `WatchRequest{CreateRequest|CancelRequest}` DATA frames
    /// on a single HTTP/2 stream; the anchor pushes `WatchResponse`
    /// DATA frames back without `END_STREAM` until cancel.
    Watch = 6,
    /// `/etcdserverpb.Lease/LeaseKeepAlive` — bidi-streaming. Each
    /// inbound `LeaseKeepAliveRequest{ID}` triggers one outbound
    /// `LeaseKeepAliveResponse{ID, TTL}`; the stream stays open until
    /// the client closes its half.
    LeaseKeepAlive = 7,
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotPhase {
    Open = 0,
    Closing = 1,
}

#[repr(C)]
struct StreamSlot {
    /// HTTP/2 stream id (always odd, client-initiated). 0 = free.
    stream_id: u32,
    method: GrpcMethod,
    end_stream_seen: bool,
    headers_done: bool,
    /// corr_id of the inflight KV envelope. 0 = none.
    pending_corr: u64,
    /// lease_id of the inflight Lease envelope (LeaseGrant/LeaseRevoke).
    /// 0 = no lease op outstanding. Disjoint from `pending_corr` —
    /// at most one of the two is non-zero per stream.
    pending_lease_id: u64,
    /// For server-streaming Watch: anchor-assigned watch_id. 0 = no
    /// active watch on this stream. The stream stays open while this
    /// is non-zero; `MSG_WATCH_FRAME` events route here via lookup.
    watch_id: i64,
    /// Whether the initial response HEADERS frame has been sent. Set
    /// on the first WatchResponse so subsequent events skip HEADERS.
    /// Unary RPCs leave this false (write_response_frames sends
    /// HEADERS itself per request).
    response_headers_sent: bool,
    body_buf: [u8; STREAM_BODY_MAX],
    body_len: usize,
}

impl StreamSlot {
    const fn free() -> Self {
        Self {
            stream_id: STREAM_FREE,
            method: GrpcMethod::Unknown,
            end_stream_seen: false,
            headers_done: false,
            pending_corr: 0,
            pending_lease_id: 0,
            watch_id: 0,
            response_headers_sent: false,
            body_buf: [0; STREAM_BODY_MAX],
            body_len: 0,
        }
    }

    fn reset(&mut self) {
        self.stream_id = STREAM_FREE;
        self.method = GrpcMethod::Unknown;
        self.end_stream_seen = false;
        self.headers_done = false;
        self.pending_corr = 0;
        self.pending_lease_id = 0;
        self.watch_id = 0;
        self.response_headers_sent = false;
        self.body_len = 0;
    }
}

#[repr(C)]
struct Slot {
    conn_id: u16,
    phase: SlotPhase,
    preface_seen: bool,
    settings_sent: bool,

    /// Highest stream id we've seen (per RFC 7540, ids on a
    /// connection must monotonically increase). Used for GOAWAY's
    /// `last_stream_id` field.
    last_stream_id: u32,

    recv_buf: [u8; RECV_BUF_SIZE],
    recv_len: usize,

    send_buf: [u8; SEND_BUF_SIZE],
    send_len: usize,

    streams: [StreamSlot; MAX_STREAMS_PER_CONN],
}

impl Slot {
    const fn free() -> Self {
        Self {
            conn_id: SLOT_FREE,
            phase: SlotPhase::Open,
            preface_seen: false,
            settings_sent: false,
            last_stream_id: 0,
            recv_buf: [0; RECV_BUF_SIZE],
            recv_len: 0,
            send_buf: [0; SEND_BUF_SIZE],
            send_len: 0,
            streams: [const { StreamSlot::free() }; MAX_STREAMS_PER_CONN],
        }
    }

    fn reset_session(&mut self) {
        self.preface_seen = false;
        self.settings_sent = false;
        self.last_stream_id = 0;
        self.recv_len = 0;
        self.send_len = 0;
        let mut i = 0;
        while i < MAX_STREAMS_PER_CONN {
            self.streams[i].reset();
            i += 1;
        }
    }

    fn alloc_stream(&mut self, stream_id: u32) -> Option<usize> {
        // Reuse if already allocated (HEADERS sometimes precedes DATA
        // with the same stream id across multiple frames).
        for (i, s) in self.streams.iter().enumerate() {
            if s.stream_id == stream_id {
                return Some(i);
            }
        }
        for (i, s) in self.streams.iter_mut().enumerate() {
            if s.stream_id == STREAM_FREE {
                s.reset();
                s.stream_id = stream_id;
                return Some(i);
            }
        }
        None
    }

    fn find_stream(&self, stream_id: u32) -> Option<usize> {
        self.streams.iter().position(|s| s.stream_id == stream_id)
    }

    fn find_stream_by_corr(&self, corr_id: u64) -> Option<usize> {
        self.streams
            .iter()
            .position(|s| s.stream_id != STREAM_FREE && s.pending_corr == corr_id)
    }

    fn find_stream_by_lease(&self, lease_id: u64) -> Option<usize> {
        self.streams
            .iter()
            .position(|s| s.stream_id != STREAM_FREE && s.pending_lease_id == lease_id)
    }

    fn find_stream_by_watch(&self, watch_id: i64) -> Option<usize> {
        self.streams
            .iter()
            .position(|s| s.stream_id != STREAM_FREE && s.watch_id == watch_id)
    }

    fn free_stream(&mut self, idx: usize) {
        if idx < MAX_STREAMS_PER_CONN {
            self.streams[idx].reset();
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum AnchorPhase {
    Init = 0,
    WaitBound = 1,
    Listening = 2,
    Error = 0xFF,
}

#[repr(C)]
struct AnchorState {
    syscalls: *const SyscallTable,

    net_in: i32,
    net_out: i32,
    routed_in: i32,
    watch_in: i32,
    lease_in: i32,
    kv_out: i32,
    watch_ctrl_out: i32,
    lease_ctrl_out: i32,
    metrics_out: i32,

    listen_port: u16,
    phase: AnchorPhase,
    _pad0: u8,

    server_conn_id: u16,
    _pad1: [u8; 3],

    corr_seq: u64,
    lease_seq: u64,
    watch_seq: u64,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=requests, 1=forwarded, 2=net_errors).
    m_requests: u64,
    m_forwarded: u64,
    m_net_errors: u64,
    step_ctr: u64,

    slots: [Slot; MAX_CONNS],
    scratch: [u8; SCRATCH_BUF_SIZE],
    hpack_scratch: [u8; HPACK_SCRATCH_LEN],
    /// Scratch arena for the currently-parsed HTTP/2 frame body.
    /// Lives in the kernel-allocated state rather than on the stack
    /// to keep `module_step`'s stack frame tiny (the runtime stack
    /// on the linux platform is generous but bare-metal targets
    /// budget tens of KiB total per module call).
    frame_payload: [u8; RECV_BUF_SIZE],
    /// Scratch arena for the gRPC response body (DATA frame payload).
    response_data: [u8; STREAM_BODY_MAX],
}

impl AnchorState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.net_in = -1;
        self.net_out = -1;
        self.routed_in = -1;
        self.watch_in = -1;
        self.lease_in = -1;
        self.kv_out = -1;
        self.watch_ctrl_out = -1;
        self.lease_ctrl_out = -1;
        self.metrics_out = -1;

        self.listen_port = DEFAULT_LISTEN_PORT;
        self.phase = AnchorPhase::Init;
        self._pad0 = 0;
        self.server_conn_id = 0;
        self._pad1 = [0; 3];
        self.corr_seq = 0;
        self.lease_seq = 0;
        self.watch_seq = 0;
        self.m_requests = 0;
        self.m_forwarded = 0;
        self.m_net_errors = 0;
        self.step_ctr = 0;

        let mut i = 0;
        while i < MAX_CONNS {
            self.slots[i] = Slot::free();
            i += 1;
        }
    }

    fn alloc_slot(&mut self, conn_id: u16) -> Option<usize> {
        for (i, slot) in self.slots.iter_mut().enumerate() {
            if slot.conn_id == SLOT_FREE {
                slot.conn_id = conn_id;
                slot.phase = SlotPhase::Open;
                slot.reset_session();
                return Some(i);
            }
        }
        None
    }

    fn find_slot(&self, conn_id: u16) -> Option<usize> {
        self.slots.iter().position(|s| s.conn_id == conn_id)
    }

    fn free_slot(&mut self, idx: usize) {
        if idx < MAX_CONNS {
            self.slots[idx].conn_id = SLOT_FREE;
            self.slots[idx].phase = SlotPhase::Open;
            self.slots[idx].reset_session();
        }
    }

    fn next_corr(&mut self) -> u64 {
        // Top byte = protocol id; see redis_edge_anchor::next_corr for
        // why each anchor must namespace its own corr_id range so the
        // shared router inflight table doesn't collide across anchors.
        self.corr_seq = self.corr_seq.wrapping_add(1);
        if self.corr_seq == 0 {
            self.corr_seq = 1; // 0 is the "no inflight" sentinel
        }
        (u64::from(PROTO_ETCD) << 56) | (self.corr_seq & 0x00FF_FFFF_FFFF_FFFF)
    }

    /// Locate the slot+stream that issued `corr_id`. Linear scan over
    /// at most `MAX_CONNS * MAX_STREAMS_PER_CONN` = 1024 entries.
    fn find_inflight(&self, corr_id: u64) -> Option<(usize, usize)> {
        for (si, slot) in self.slots.iter().enumerate() {
            if slot.conn_id == SLOT_FREE {
                continue;
            }
            if let Some(strm) = slot.find_stream_by_corr(corr_id) {
                return Some((si, strm));
            }
        }
        None
    }

    /// Watch counterpart to `find_inflight`. `MSG_WATCH_FRAME` carries
    /// the body of a server-streaming `WatchResponse` for a given
    /// `watch_id`; the anchor finds the stream via its anchor-assigned
    /// `watch_id` and pushes the frame as a DATA frame.
    fn find_watch_stream(&self, watch_id: i64) -> Option<(usize, usize)> {
        for (si, slot) in self.slots.iter().enumerate() {
            if slot.conn_id == SLOT_FREE {
                continue;
            }
            if let Some(strm) = slot.find_stream_by_watch(watch_id) {
                return Some((si, strm));
            }
        }
        None
    }

    /// Allocate a fresh `watch_id`. Monotonic counter; `0` is the
    /// "no active watch" sentinel so we skip it on wrap.
    fn next_watch_id(&mut self) -> i64 {
        self.watch_seq = self.watch_seq.wrapping_add(1);
        if self.watch_seq == 0 {
            self.watch_seq = 1;
        }
        self.watch_seq as i64
    }

    /// Lease counterpart to `find_inflight`. `MSG_LEASE_STATE` carries
    /// `lease_id` (not `corr_id`); the stream that issued the grant /
    /// revoke stashed it in `pending_lease_id` so we can rendezvous.
    fn find_lease_inflight(&self, lease_id: u64) -> Option<(usize, usize)> {
        for (si, slot) in self.slots.iter().enumerate() {
            if slot.conn_id == SLOT_FREE {
                continue;
            }
            if let Some(strm) = slot.find_stream_by_lease(lease_id) {
                return Some((si, strm));
            }
        }
        None
    }

    /// Generate a fresh lease ID when the client passes ID=0 in
    /// `LeaseGrantRequest`. Real etcd derives lease IDs from member
    /// metadata; for single-node Lattice a monotonic counter with the
    /// high nibble set is sufficient (collisions with client-supplied
    /// IDs are improbable in practice, and the lease_manager rejects
    /// duplicates by returning epoch=0).
    fn next_lease_id(&mut self) -> u64 {
        self.lease_seq = self.lease_seq.wrapping_add(1);
        if self.lease_seq == 0 {
            self.lease_seq = 1;
        }
        // High nibble = 0xA distinguishes anchor-generated IDs from
        // client-supplied ones at a glance in logs / pcaps.
        0xA000_0000_0000_0000 | self.lease_seq
    }
}

// ── Frame send helpers (into slot.send_buf, flushed in batch) ─────────

/// Append a full HTTP/2 frame (header + payload) to `slot.send_buf`.
/// Returns false if the buffer is full.
fn append_frame(
    slot: &mut Slot,
    frame_type: u8,
    flags: u8,
    stream_id: u32,
    payload: &[u8],
) -> bool {
    let need = FRAME_HEADER_LEN + payload.len();
    if slot.send_len + need > SEND_BUF_SIZE {
        return false;
    }
    let hdr = FrameHeader {
        length: payload.len() as u32,
        frame_type,
        flags,
        stream_id,
    };
    if hdr
        .encode(&mut slot.send_buf[slot.send_len..slot.send_len + FRAME_HEADER_LEN])
        .is_none()
    {
        return false;
    }
    slot.send_len += FRAME_HEADER_LEN;
    slot.send_buf[slot.send_len..slot.send_len + payload.len()].copy_from_slice(payload);
    slot.send_len += payload.len();
    true
}

fn send_settings(slot: &mut Slot) -> bool {
    // SETTINGS we send to the peer (non-ACK):
    //   HEADER_TABLE_SIZE = 0 (peer never references our dyn table)
    //   ENABLE_PUSH = 0
    //   MAX_CONCURRENT_STREAMS = MAX_STREAMS_PER_CONN
    let mut body = [0u8; 18];
    let mut off = 0;
    write_setting(&mut body, &mut off, 0x1, 0); // HEADER_TABLE_SIZE = 0
    write_setting(&mut body, &mut off, 0x2, 0); // ENABLE_PUSH = 0
    write_setting(&mut body, &mut off, 0x3, MAX_STREAMS_PER_CONN as u32);
    append_frame(slot, FrameType::Settings as u8, 0, 0, &body[..off])
}

fn write_setting(out: &mut [u8], off: &mut usize, id: u16, value: u32) {
    out[*off] = (id >> 8) as u8;
    out[*off + 1] = (id & 0xFF) as u8;
    out[*off + 2] = (value >> 24) as u8;
    out[*off + 3] = ((value >> 16) & 0xFF) as u8;
    out[*off + 4] = ((value >> 8) & 0xFF) as u8;
    out[*off + 5] = (value & 0xFF) as u8;
    *off += 6;
}

fn send_settings_ack(slot: &mut Slot) -> bool {
    append_frame(slot, FrameType::Settings as u8, FLAG_ACK, 0, &[])
}

fn send_ping_ack(slot: &mut Slot, opaque: &[u8]) -> bool {
    if opaque.len() < 8 {
        return false;
    }
    append_frame(slot, FrameType::Ping as u8, FLAG_ACK, 0, &opaque[..8])
}

fn send_goaway(slot: &mut Slot, last_stream_id: u32, error_code: u32) -> bool {
    let mut body = [0u8; 8];
    let sid = last_stream_id & 0x7FFF_FFFF;
    body[0] = (sid >> 24) as u8;
    body[1] = ((sid >> 16) & 0xFF) as u8;
    body[2] = ((sid >> 8) & 0xFF) as u8;
    body[3] = (sid & 0xFF) as u8;
    body[4] = (error_code >> 24) as u8;
    body[5] = ((error_code >> 16) & 0xFF) as u8;
    body[6] = ((error_code >> 8) & 0xFF) as u8;
    body[7] = (error_code & 0xFF) as u8;
    append_frame(slot, FrameType::Goaway as u8, 0, 0, &body)
}

fn send_window_update(slot: &mut Slot, stream_id: u32, increment: u32) -> bool {
    let mut body = [0u8; 4];
    let inc = increment & 0x7FFF_FFFF;
    body[0] = (inc >> 24) as u8;
    body[1] = ((inc >> 16) & 0xFF) as u8;
    body[2] = ((inc >> 8) & 0xFF) as u8;
    body[3] = (inc & 0xFF) as u8;
    append_frame(slot, FrameType::WindowUpdate as u8, 0, stream_id, &body)
}

// ── HEADERS path resolution ───────────────────────────────────────────

/// Match an etcd v3 `:path` to a method. Returns
/// `GrpcMethod::Unknown` for paths the anchor doesn't handle (the
/// caller responds with grpc-status = UNIMPLEMENTED).
fn resolve_method(path: &[u8]) -> GrpcMethod {
    if path == b"/etcdserverpb.KV/Range" {
        GrpcMethod::Range
    } else if path == b"/etcdserverpb.KV/Put" {
        GrpcMethod::Put
    } else if path == b"/etcdserverpb.KV/DeleteRange" {
        GrpcMethod::DeleteRange
    } else if path == b"/etcdserverpb.Lease/LeaseGrant" {
        GrpcMethod::LeaseGrant
    } else if path == b"/etcdserverpb.Lease/LeaseRevoke" {
        GrpcMethod::LeaseRevoke
    } else if path == b"/etcdserverpb.Watch/Watch" {
        GrpcMethod::Watch
    } else if path == b"/etcdserverpb.Lease/LeaseKeepAlive" {
        GrpcMethod::LeaseKeepAlive
    } else {
        GrpcMethod::Unknown
    }
}

/// Decode the request HEADERS block out of `block_bytes` and pull
/// out `:path`. Returns the parsed method, or Unknown on any error
/// (HPACK parse failure, missing `:path`, unsupported path).
fn parse_request_headers(block_bytes: &[u8], hpack_scratch: &mut [u8]) -> Option<GrpcMethod> {
    let mut decoder = HpackDecoder::new();
    let mut method = GrpcMethod::Unknown;
    let mut path_buf = [0u8; 64];
    let mut path_len: usize = 0;
    let mut path_seen = false;
    decoder
        .decode_block(block_bytes, hpack_scratch, |name, value| {
            if name == b":path" && !path_seen {
                let n = value.len().min(path_buf.len());
                path_buf[..n].copy_from_slice(&value[..n]);
                path_len = n;
                path_seen = true;
            }
        })
        .ok()?;
    if path_seen {
        method = resolve_method(&path_buf[..path_len]);
    }
    Some(method)
}

// ── KV envelope path ──────────────────────────────────────────────────

/// Translate one gRPC request (method + protobuf body) into a
/// KV_OP_* body shape (see `modules/common/types.rs` body layouts).
/// Returns `(op, body_len)` on success; `None` on a protobuf parse
/// failure (the caller responds with grpc-status = INVALID_ARGUMENT).
fn build_kv_op_body(method: GrpcMethod, grpc_body: &[u8], out: &mut [u8]) -> Option<(u8, usize)> {
    // Strip the 5-byte gRPC message prefix.
    if grpc_body.len() < GRPC_FRAME_HEADER_LEN {
        return None;
    }
    let hdr = GrpcMessageHeader::parse(&grpc_body[..GRPC_FRAME_HEADER_LEN])?;
    if hdr.compressed != 0 {
        return None;
    }
    let body_start = GRPC_FRAME_HEADER_LEN;
    let body_end = body_start.checked_add(hdr.length as usize)?;
    if body_end > grpc_body.len() {
        return None;
    }
    let proto_body = &grpc_body[body_start..body_end];

    match method {
        GrpcMethod::Range => {
            let req = RangeRequest::decode(proto_body)?;
            if !req.range_end.is_empty() {
                // RANGED read (prefix scans included): a real
                // `KV_OP_RANGE_SCAN`, bounded to one page with an
                // honest `more` flag. Historical ranged reads refuse
                // typed (there is no ranged GET_AT) — an empty answer
                // pretending to be complete is the one thing this
                // path must never produce.
                if req.revision > 0 {
                    return None;
                }
                let n = etcd_codec::encode_range_scan_body(&req, out)?;
                return Some((KV_OP_RANGE_SCAN, n));
            }
            // `revision > 0` is a historical read (RFC §10/§20
            // snapshot). The codec picks the body shape; we pick the
            // op byte that matches it.
            let (historical, n) = etcd_codec::encode_range_kv_body(&req, out)?;
            Some((if historical { KV_OP_GET_AT } else { KV_OP_GET }, n))
        }
        GrpcMethod::Put => {
            let req = PutRequest::decode(proto_body)?;
            // KV_OP_PUT body:
            // [key_len:u16 LE][key…][value_len:u32 LE][value…]
            // [put_flags:u8][expiry_ms:u64 LE]
            let key = req.key;
            let value = req.value;
            let need = 2 + key.len() + 4 + value.len() + 1 + 8;
            if need > out.len() || key.len() > u16::MAX as usize {
                return None;
            }
            let mut p = 0;
            out[p] = (key.len() & 0xFF) as u8;
            out[p + 1] = ((key.len() >> 8) & 0xFF) as u8;
            p += 2;
            out[p..p + key.len()].copy_from_slice(key);
            p += key.len();
            out[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
            p += 4;
            out[p..p + value.len()].copy_from_slice(value);
            p += value.len();
            out[p] = 0; // put_flags — etcd Put has no NX/XX
            p += 1;
            out[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
            p += 8;
            Some((KV_OP_PUT, p))
        }
        GrpcMethod::DeleteRange => {
            let req = DeleteRangeRequest::decode(proto_body)?;
            if !req.range_end.is_empty() {
                // Ranged deletes are not implemented; refuse typed
                // (INVALID_ARGUMENT) rather than silently deleting
                // only `key` and reporting success.
                return None;
            }
            // KV_OP_DELETE body: [key_count:u16 LE] then per key
            // [key_len:u16 LE][key…]. We emit a single key (etcd
            // single-key delete via empty range_end).
            let key = req.key;
            let need = 2 + 2 + key.len();
            if need > out.len() || key.len() > u16::MAX as usize {
                return None;
            }
            let mut p = 0;
            out[p] = 1;
            out[p + 1] = 0;
            p += 2;
            out[p] = (key.len() & 0xFF) as u8;
            out[p + 1] = ((key.len() >> 8) & 0xFF) as u8;
            p += 2;
            out[p..p + key.len()].copy_from_slice(key);
            p += key.len();
            Some((KV_OP_DELETE, p))
        }
        GrpcMethod::Unknown
        | GrpcMethod::LeaseGrant
        | GrpcMethod::LeaseRevoke
        | GrpcMethod::LeaseKeepAlive
        | GrpcMethod::Watch => None,
    }
}

/// Variant of `build_grpc_response_body` that knows the request key,
/// so Range responses can echo it back in the KeyValue. For Delete,
/// the kv_body shape is the KV_RESULT_INTEGER 8-byte LE i64 holding
/// the deleted count.
fn build_grpc_response_with_key(
    method: GrpcMethod,
    result: u8,
    revision: i64,
    key: &[u8],
    kv_body: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    if out.len() < GRPC_FRAME_HEADER_LEN {
        return None;
    }
    let mut proto_off = GRPC_FRAME_HEADER_LEN;
    match method {
        GrpcMethod::Range => {
            if result == KV_RESULT_RANGE {
                // Ranged read: the scan body carries the keys.
                etcd_codec::build_range_scan_response(out, &mut proto_off, revision, kv_body)?;
            } else {
                let kv = if result == KV_RESULT_OK {
                    Some((key, kv_body, revision, 1i64))
                } else {
                    None
                };
                build_range_response(out, &mut proto_off, revision, kv)?;
            }
        }
        GrpcMethod::Put => {
            build_put_response(out, &mut proto_off, revision)?;
        }
        GrpcMethod::DeleteRange => {
            // kv_state_worker returns KV_RESULT_INTEGER with body
            // = 8-byte LE i64 holding the deleted count.
            let deleted = if result == KV_RESULT_INTEGER && kv_body.len() >= 8 {
                let mut b = [0u8; 8];
                b.copy_from_slice(&kv_body[..8]);
                i64::from_le_bytes(b)
            } else if result == KV_RESULT_OK {
                1
            } else {
                0
            };
            etcd_codec::build_delete_range_response(out, &mut proto_off, revision, deleted)?;
        }
        GrpcMethod::Unknown
        | GrpcMethod::LeaseGrant
        | GrpcMethod::LeaseRevoke
        | GrpcMethod::LeaseKeepAlive
        | GrpcMethod::Watch => return None,
    }
    let proto_len = proto_off - GRPC_FRAME_HEADER_LEN;
    let grpc_hdr = GrpcMessageHeader {
        compressed: 0,
        length: proto_len as u32,
    };
    grpc_hdr.encode(&mut out[..GRPC_FRAME_HEADER_LEN])?;
    Some(proto_off)
}

// ── Frame dispatch ────────────────────────────────────────────────────

#[derive(PartialEq, Eq)]
enum FrameAction {
    /// Frame fully processed; continue to next.
    Done,
    /// Need more data to complete this frame.
    Wait,
    /// Protocol-level error; tear down connection.
    Fatal,
}

/// Attempt to consume one frame from `slot.recv_buf`. On success
/// updates `slot.recv_buf` (drains the frame) and returns Done.
///
/// To stay within a tiny stack budget, the frame payload is parked
/// in `anchor.frame_payload` (a state-arena buffer) rather than a
/// stack array. dispatch_frame borrows `&self.frame_payload[..len]`
/// with a re-borrow trick (slot table and frame_payload are
/// independent fields so the borrow checker accepts a split mut +
/// shared borrow at call sites that need both).
fn process_one_frame(anchor: &mut AnchorState, slot_idx: usize) -> FrameAction {
    let recv_len = anchor.slots[slot_idx].recv_len;
    if recv_len < FRAME_HEADER_LEN {
        return FrameAction::Wait;
    }
    let hdr = {
        let slot = &anchor.slots[slot_idx];
        let Some(h) = FrameHeader::parse(&slot.recv_buf[..FRAME_HEADER_LEN]) else {
            return FrameAction::Fatal;
        };
        h
    };
    let total = hdr.total_len();
    if total > RECV_BUF_SIZE {
        return FrameAction::Fatal;
    }
    if recv_len < total {
        return FrameAction::Wait;
    }

    let payload_len = hdr.length as usize;
    if payload_len > anchor.frame_payload.len() {
        return FrameAction::Fatal;
    }
    anchor.frame_payload[..payload_len].copy_from_slice(
        &anchor.slots[slot_idx].recv_buf[FRAME_HEADER_LEN..FRAME_HEADER_LEN + payload_len],
    );

    {
        let slot = &mut anchor.slots[slot_idx];
        slot.recv_buf.copy_within(total..slot.recv_len, 0);
        slot.recv_len -= total;
    }

    dispatch_frame(anchor, slot_idx, hdr, payload_len)
}

fn dispatch_frame(
    anchor: &mut AnchorState,
    slot_idx: usize,
    hdr: FrameHeader,
    payload_len: usize,
) -> FrameAction {
    let Some(ft) = FrameType::from_u8(hdr.frame_type) else {
        return FrameAction::Done;
    };
    match ft {
        FrameType::Settings => {
            if hdr.has_flag(FLAG_ACK) {
                FrameAction::Done
            } else {
                // Walk the iterator for side effects (today: none).
                let payload = &anchor.frame_payload[..payload_len];
                for _ in SettingsIter::new(payload) {}
                let slot = &mut anchor.slots[slot_idx];
                if !send_settings_ack(slot) {
                    return FrameAction::Fatal;
                }
                FrameAction::Done
            }
        }
        FrameType::Ping => {
            if hdr.has_flag(FLAG_ACK) {
                FrameAction::Done
            } else {
                // Copy ping payload to a tiny local buffer so we
                // can release the frame_payload borrow before
                // borrowing the slot for send.
                let mut opaque = [0u8; 8];
                let take = payload_len.min(8);
                opaque[..take].copy_from_slice(&anchor.frame_payload[..take]);
                let slot = &mut anchor.slots[slot_idx];
                if !send_ping_ack(slot, &opaque) {
                    return FrameAction::Fatal;
                }
                FrameAction::Done
            }
        }
        FrameType::WindowUpdate => FrameAction::Done,
        FrameType::Headers => handle_headers(anchor, slot_idx, hdr, payload_len),
        FrameType::Data => handle_data(anchor, slot_idx, hdr, payload_len),
        FrameType::Goaway => {
            anchor.slots[slot_idx].phase = SlotPhase::Closing;
            FrameAction::Done
        }
        FrameType::RstStream => {
            let stream_id = hdr.stream_id;
            let slot = &mut anchor.slots[slot_idx];
            if let Some(strm) = slot.find_stream(stream_id) {
                slot.free_stream(strm);
            }
            FrameAction::Done
        }
        FrameType::Priority => FrameAction::Done,
        FrameType::PushPromise | FrameType::Continuation => FrameAction::Fatal,
    }
}

fn handle_headers(
    anchor: &mut AnchorState,
    slot_idx: usize,
    hdr: FrameHeader,
    payload_len: usize,
) -> FrameAction {
    let stream_id = hdr.stream_id;
    if stream_id == 0 || (stream_id & 1) == 0 {
        return FrameAction::Fatal;
    }
    let flags = hdr.flags;
    const FLAG_PADDED: u8 = 0x08;
    const FLAG_PRIORITY: u8 = 0x20;
    // Compute the (start, end) slice of the HEADERS block inside
    // frame_payload after stripping pad-length and priority bytes.
    let mut start = 0usize;
    let mut end = payload_len;
    if flags & FLAG_PADDED != 0 {
        if payload_len == 0 {
            return FrameAction::Fatal;
        }
        let pad_len = anchor.frame_payload[0] as usize;
        start = 1;
        if pad_len + 1 > payload_len {
            return FrameAction::Fatal;
        }
        end = payload_len - pad_len;
    }
    if flags & FLAG_PRIORITY != 0 {
        if end - start < 5 {
            return FrameAction::Fatal;
        }
        start += 5;
    }

    let strm = {
        let slot = &mut anchor.slots[slot_idx];
        match slot.alloc_stream(stream_id) {
            Some(s) => s,
            None => {
                let _ = send_goaway(slot, slot.last_stream_id, ERROR_PROTOCOL_ERROR);
                return FrameAction::Fatal;
            }
        }
    };

    if stream_id > anchor.slots[slot_idx].last_stream_id {
        anchor.slots[slot_idx].last_stream_id = stream_id;
    }

    // Decode HPACK using a split-borrow: hpack_scratch and
    // frame_payload are distinct fields of AnchorState.
    let method = {
        let hpack = &mut anchor.hpack_scratch[..];
        let block = &anchor.frame_payload[start..end];
        parse_request_headers(block, hpack).unwrap_or(GrpcMethod::Unknown)
    };

    {
        let stream = &mut anchor.slots[slot_idx].streams[strm];
        stream.method = method;
        stream.headers_done = (flags & FLAG_END_HEADERS) != 0;
        stream.end_stream_seen = (flags & FLAG_END_STREAM) != 0;
    }

    if anchor.slots[slot_idx].streams[strm].end_stream_seen {
        dispatch_request(anchor, slot_idx, strm);
    }
    FrameAction::Done
}

fn handle_data(
    anchor: &mut AnchorState,
    slot_idx: usize,
    hdr: FrameHeader,
    payload_len: usize,
) -> FrameAction {
    let stream_id = hdr.stream_id;
    let flags = hdr.flags;
    const FLAG_PADDED: u8 = 0x08;
    let mut start = 0usize;
    let mut end = payload_len;
    if flags & FLAG_PADDED != 0 {
        if payload_len == 0 {
            return FrameAction::Fatal;
        }
        let pad_len = anchor.frame_payload[0] as usize;
        start = 1;
        if pad_len + 1 > payload_len {
            return FrameAction::Fatal;
        }
        end = payload_len - pad_len;
    }
    let body_len_now = end - start;

    let Some(strm) = anchor.slots[slot_idx].find_stream(stream_id) else {
        return FrameAction::Done;
    };

    {
        let stream = &mut anchor.slots[slot_idx].streams[strm];
        if stream.body_len + body_len_now > STREAM_BODY_MAX {
            return FrameAction::Fatal;
        }
        stream.body_buf[stream.body_len..stream.body_len + body_len_now]
            .copy_from_slice(&anchor.frame_payload[start..end]);
        stream.body_len += body_len_now;
        if (flags & FLAG_END_STREAM) != 0 {
            stream.end_stream_seen = true;
        }
    }

    let inc = body_len_now as u32;
    if inc > 0 {
        let slot = &mut anchor.slots[slot_idx];
        let _ = send_window_update(slot, 0, inc);
        let _ = send_window_update(slot, stream_id, inc);
    }

    // Watch + LeaseKeepAlive are bidi-streaming: client DATA frames
    // don't carry END_STREAM. Dispatch each complete request as it lands.
    let method = anchor.slots[slot_idx].streams[strm].method;
    let streaming = matches!(method, GrpcMethod::Watch | GrpcMethod::LeaseKeepAlive);
    if streaming && anchor.slots[slot_idx].streams[strm].body_len >= GRPC_FRAME_HEADER_LEN {
        match method {
            GrpcMethod::Watch => dispatch_watch_message(anchor, slot_idx, strm),
            GrpcMethod::LeaseKeepAlive => dispatch_lease_keepalive_message(anchor, slot_idx, strm),
            _ => {}
        }
    } else if anchor.slots[slot_idx].streams[strm].end_stream_seen {
        dispatch_request(anchor, slot_idx, strm);
    }
    FrameAction::Done
}

/// Stream has reached END_STREAM. Translate to KV envelope and emit
/// on `kv_out`. Marks the stream's `pending_corr` so the response
/// path can find it. Uses `anchor.scratch` for the outbound KV body
/// build (the gRPC request body stays in the stream's body_buf).
fn dispatch_request(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    anchor.m_requests += 1;
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let method = anchor.slots[slot_idx].streams[strm].method;

    if method == GrpcMethod::Unknown {
        send_grpc_error(anchor, slot_idx, stream_id, 12);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }

    // Lease ops route to lease_manager via lease_ctrl_out, not the
    // KV worker. The response correlates by lease_id, not corr_id.
    if matches!(method, GrpcMethod::LeaseGrant | GrpcMethod::LeaseRevoke) {
        dispatch_lease(anchor, slot_idx, strm);
        return;
    }
    // Watch / LeaseKeepAlive are bidi-streaming; handled out of band by
    // dispatch_watch_message / dispatch_lease_keepalive_message
    // (called per inbound DATA frame, no END_STREAM gate). If we somehow
    // reach here with a body, drain it.
    if matches!(method, GrpcMethod::Watch | GrpcMethod::LeaseKeepAlive) {
        if anchor.slots[slot_idx].streams[strm].body_len >= GRPC_FRAME_HEADER_LEN {
            match method {
                GrpcMethod::Watch => dispatch_watch_message(anchor, slot_idx, strm),
                GrpcMethod::LeaseKeepAlive => {
                    dispatch_lease_keepalive_message(anchor, slot_idx, strm)
                }
                _ => {}
            }
        }
        return;
    }

    let body_len = anchor.slots[slot_idx].streams[strm].body_len;
    let corr = anchor.next_corr();

    // Build the MSG_KV_REQUEST envelope directly into anchor.scratch.
    // The envelope head is 18 bytes; the KV op body is appended
    // starting at offset (3 + 18) = 21. build_kv_op_body decodes the
    // request body (which is in the stream's body_buf) into that
    // region of scratch.
    const ENVELOPE_HDR: usize = 3;
    const REQ_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    let op_body_start = ENVELOPE_HDR + REQ_HEAD;
    if op_body_start > anchor.scratch.len() {
        send_grpc_error(anchor, slot_idx, stream_id, 13);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    // build_kv_op_body needs a fresh-mut slice into scratch starting
    // at op_body_start, plus an immutable slice into the stream's
    // body_buf. Use split-mut via field-disjoint borrows.
    let kv_result = {
        let scratch_tail = &mut anchor.scratch[op_body_start..];
        let request_body = &anchor.slots[slot_idx].streams[strm].body_buf[..body_len];
        build_kv_op_body(method, request_body, scratch_tail)
    };
    let Some((op, op_body_len)) = kv_result else {
        send_grpc_error(anchor, slot_idx, stream_id, 3);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    };

    let payload_len = REQ_HEAD + op_body_len;
    let total = ENVELOPE_HDR + payload_len;
    if total > anchor.scratch.len() {
        send_grpc_error(anchor, slot_idx, stream_id, 13);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }

    // Fill in the envelope header + KV request head fields. Body is
    // already at offset 21 from build_kv_op_body.
    anchor.scratch[0] = MSG_KV_REQUEST;
    anchor.scratch[1] = (payload_len & 0xFF) as u8;
    anchor.scratch[2] = ((payload_len >> 8) & 0xFF) as u8;
    let mut p = ENVELOPE_HDR;
    anchor.scratch[p..p + 8].copy_from_slice(&corr.to_le_bytes());
    p += 8;
    anchor.scratch[p] = PROTO_ETCD;
    p += 1;
    anchor.scratch[p..p + 4].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());
    p += 4;
    // The head's conn byte is this anchor's SLOT INDEX — the net conn
    // id is u16 now and does not fit the byte. Replies are matched by
    // `find_inflight(corr)`, so the byte is informational only.
    anchor.scratch[p] = slot_idx as u8;
    p += 1;
    anchor.scratch[p] = 0;
    p += 1;
    anchor.scratch[p] = op;
    p += 1;
    anchor.scratch[p] = (op_body_len & 0xFF) as u8;
    anchor.scratch[p + 1] = ((op_body_len >> 8) & 0xFF) as u8;

    let sys = anchor.syscalls;
    let kv_out = anchor.kv_out;
    if sys.is_null() || kv_out < 0 {
        send_grpc_error(anchor, slot_idx, stream_id, 14);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    let written = unsafe { ((*sys).channel_write)(kv_out, anchor.scratch.as_mut_ptr(), total) };
    if written != total as i32 {
        send_grpc_error(anchor, slot_idx, stream_id, 14);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }

    anchor.m_forwarded += 1;

    // Stash the request key in body_buf for the response path
    // (only needed for Range — the response carries the key back).
    anchor.slots[slot_idx].streams[strm].pending_corr = corr;
    if method == GrpcMethod::Range && op != KV_OP_RANGE_SCAN {
        // The KV body for GET starts with [key_len:u16 LE][key…];
        // GET_AT prefixes it with the 8-byte revision. It's currently
        // at scratch[op_body_start..].
        let key_at = op_body_start + if op == KV_OP_GET_AT { 8 } else { 0 };
        let head = key_at - op_body_start;
        if op_body_len >= head + 2 {
            let key_len_lo = anchor.scratch[key_at];
            let key_len_hi = anchor.scratch[key_at + 1];
            let key_len = u16::from_le_bytes([key_len_lo, key_len_hi]) as usize;
            if head + 2 + key_len <= op_body_len && key_len <= STREAM_BODY_MAX {
                // Move the key from scratch into body_buf. Note:
                // scratch and stream are disjoint memory regions.
                let key_src_start = key_at + 2;
                let key_src_end = key_src_start + key_len;
                let stream = &mut anchor.slots[slot_idx].streams[strm];
                stream.body_len = key_len;
                stream.body_buf[..key_len]
                    .copy_from_slice(&anchor.scratch[key_src_start..key_src_end]);
            } else {
                anchor.slots[slot_idx].streams[strm].body_len = 0;
            }
        } else {
            anchor.slots[slot_idx].streams[strm].body_len = 0;
        }
    } else {
        anchor.slots[slot_idx].streams[strm].body_len = 0;
    }
}

/// Translate an `END_STREAM`-completed Lease request into an
/// `MSG_LEASE_CTRL` envelope on `lease_ctrl_out` and stash the
/// `lease_id` on the stream so `poll_lease_in` can rendezvous when
/// the response (`MSG_LEASE_STATE`) lands. Lease ops bypass the KV
/// worker entirely — there is no kv_request_router hop, so the
/// per-tick channel-hop count is two (anchor → manager → anchor).
fn dispatch_lease(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let method = anchor.slots[slot_idx].streams[strm].method;
    let body_len = anchor.slots[slot_idx].streams[strm].body_len;

    // Strip the 5-byte gRPC message prefix and decode the protobuf body.
    let request_body = &anchor.slots[slot_idx].streams[strm].body_buf[..body_len];
    if request_body.len() < GRPC_FRAME_HEADER_LEN {
        send_grpc_error(anchor, slot_idx, stream_id, 3);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    let Some(hdr) = GrpcMessageHeader::parse(&request_body[..GRPC_FRAME_HEADER_LEN]) else {
        send_grpc_error(anchor, slot_idx, stream_id, 3);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    };
    if hdr.compressed != 0 {
        send_grpc_error(anchor, slot_idx, stream_id, 12);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    let body_start = GRPC_FRAME_HEADER_LEN;
    let body_end = match body_start.checked_add(hdr.length as usize) {
        Some(e) if e <= request_body.len() => e,
        _ => {
            send_grpc_error(anchor, slot_idx, stream_id, 3);
            anchor.slots[slot_idx].free_stream(strm);
            return;
        }
    };
    let proto = &request_body[body_start..body_end];

    let (ctrl, lease_id, ttl_ms) = match method {
        GrpcMethod::LeaseGrant => {
            let Some(req) = LeaseGrantRequest::decode(proto) else {
                send_grpc_error(anchor, slot_idx, stream_id, 3);
                anchor.slots[slot_idx].free_stream(strm);
                return;
            };
            // etcd's wire TTL is in seconds; lease_manager wants ms.
            let ttl_ms_u32: u32 = req.ttl.max(0).saturating_mul(1000).min(u32::MAX as i64) as u32;
            let id: u64 = if req.id == 0 {
                anchor.next_lease_id()
            } else {
                req.id as u64
            };
            (LEASE_CTRL_GRANT, id, ttl_ms_u32)
        }
        GrpcMethod::LeaseRevoke => {
            let Some(req) = LeaseRevokeRequest::decode(proto) else {
                send_grpc_error(anchor, slot_idx, stream_id, 3);
                anchor.slots[slot_idx].free_stream(strm);
                return;
            };
            (LEASE_CTRL_REVOKE, req.id as u64, 0u32)
        }
        _ => return,
    };

    // MSG_LEASE_CTRL payload: [ctrl:1][lease_id:8][ttl_ms:4][tenant_id:4] = 17 bytes
    const ENVELOPE_HDR: usize = 3;
    const CTRL_BODY: usize = 1 + 8 + 4 + 4;
    let total = ENVELOPE_HDR + CTRL_BODY;
    if total > anchor.scratch.len() {
        send_grpc_error(anchor, slot_idx, stream_id, 13);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    anchor.scratch[0] = MSG_LEASE_CTRL;
    anchor.scratch[1] = (CTRL_BODY & 0xFF) as u8;
    anchor.scratch[2] = ((CTRL_BODY >> 8) & 0xFF) as u8;
    let mut p = ENVELOPE_HDR;
    anchor.scratch[p] = ctrl;
    p += 1;
    anchor.scratch[p..p + 8].copy_from_slice(&lease_id.to_le_bytes());
    p += 8;
    anchor.scratch[p..p + 4].copy_from_slice(&ttl_ms.to_le_bytes());
    p += 4;
    anchor.scratch[p..p + 4].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());

    let sys = anchor.syscalls;
    let lease_out = anchor.lease_ctrl_out;
    if sys.is_null() || lease_out < 0 {
        send_grpc_error(anchor, slot_idx, stream_id, 14);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    let written = unsafe { ((*sys).channel_write)(lease_out, anchor.scratch.as_mut_ptr(), total) };
    if written != total as i32 {
        send_grpc_error(anchor, slot_idx, stream_id, 14);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }

    // Stash lease_id for rendezvous when MSG_LEASE_STATE returns.
    // ttl_ms is also stashed in body_buf[0..4] for the response builder
    // (lease_manager's MSG_LEASE_STATE echoes ttl_ms, but for revoke
    // it returns 0; the response encoder uses what's on the stream).
    anchor.slots[slot_idx].streams[strm].pending_lease_id = lease_id;
    let bb = &mut anchor.slots[slot_idx].streams[strm].body_buf;
    bb[0..4].copy_from_slice(&ttl_ms.to_le_bytes());
    anchor.slots[slot_idx].streams[strm].body_len = 4;
}

/// Pull one complete `LeaseKeepAliveRequest{ID}` off the stream's
/// `body_buf` and emit `MSG_LEASE_CTRL ctrl=keepalive` for it. The
/// stream stays open; the response (`MSG_LEASE_STATE`) is rendezvoused
/// by `pending_lease_id` in `poll_lease_in`. Unlike grant/revoke,
/// pending_lease_id is cleared (not the whole stream) once the ack
/// lands, so the next inbound keepalive on the same stream can stash
/// its own lease_id.
fn dispatch_lease_keepalive_message(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let body_len = anchor.slots[slot_idx].streams[strm].body_len;

    let request_body = &anchor.slots[slot_idx].streams[strm].body_buf[..body_len];
    if request_body.len() < GRPC_FRAME_HEADER_LEN {
        return;
    }
    let Some(hdr) = GrpcMessageHeader::parse(&request_body[..GRPC_FRAME_HEADER_LEN]) else {
        anchor.slots[slot_idx].streams[strm].body_len = 0;
        return;
    };
    if hdr.compressed != 0 {
        anchor.slots[slot_idx].streams[strm].body_len = 0;
        return;
    }
    let proto_start = GRPC_FRAME_HEADER_LEN;
    let proto_end = match proto_start.checked_add(hdr.length as usize) {
        Some(e) if e <= request_body.len() => e,
        _ => return, // incomplete — wait for more DATA
    };
    let lease_id = {
        let proto = &request_body[proto_start..proto_end];
        match LeaseKeepAliveRequest::decode(proto) {
            Some(req) => req.id as u64,
            None => {
                anchor.slots[slot_idx].streams[strm].body_len = 0;
                return;
            }
        }
    };

    // Consume the request bytes whether we honour it or not — leaving
    // them would cause infinite re-dispatch.
    if proto_end == body_len {
        anchor.slots[slot_idx].streams[strm].body_len = 0;
    } else {
        let tail_len = body_len - proto_end;
        let buf = &mut anchor.slots[slot_idx].streams[strm].body_buf;
        buf.copy_within(proto_end..body_len, 0);
        anchor.slots[slot_idx].streams[strm].body_len = tail_len;
    }

    // Build MSG_LEASE_CTRL ctrl=keepalive (ttl_ms=0; manager keeps the
    // grant-time TTL — see lease_manager LEASE_CTRL_KEEPALIVE).
    const ENVELOPE_HDR: usize = 3;
    const CTRL_BODY: usize = 1 + 8 + 4 + 4;
    let total = ENVELOPE_HDR + CTRL_BODY;
    if total > anchor.scratch.len() {
        return;
    }
    anchor.scratch[0] = MSG_LEASE_CTRL;
    anchor.scratch[1] = (CTRL_BODY & 0xFF) as u8;
    anchor.scratch[2] = ((CTRL_BODY >> 8) & 0xFF) as u8;
    let mut p = ENVELOPE_HDR;
    anchor.scratch[p] = LEASE_CTRL_KEEPALIVE;
    p += 1;
    anchor.scratch[p..p + 8].copy_from_slice(&lease_id.to_le_bytes());
    p += 8;
    anchor.scratch[p..p + 4].copy_from_slice(&0u32.to_le_bytes());
    p += 4;
    anchor.scratch[p..p + 4].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());

    let sys = anchor.syscalls;
    let lease_out = anchor.lease_ctrl_out;
    if sys.is_null() || lease_out < 0 {
        return;
    }
    let _ = unsafe { ((*sys).channel_write)(lease_out, anchor.scratch.as_mut_ptr(), total) };

    // Stash for rendezvous. Unlike grant/revoke, the stream stays open
    // across multiple keepalives; pending_lease_id is cleared in
    // poll_lease_in once the ack lands.
    anchor.slots[slot_idx].streams[strm].pending_lease_id = lease_id;
    let _ = stream_id;
}

/// Send a 3-frame response (response HEADERS + DATA + trailing
/// HEADERS) carrying just a grpc-status (no payload). Used when we
/// can't / won't honour a request.
fn send_grpc_error(anchor: &mut AnchorState, slot_idx: usize, stream_id: u32, grpc_status: u8) {
    let mut hdr_block = [0u8; 64];
    let mut hoff = 0;
    if build_response_headers_block(&mut hdr_block, &mut hoff).is_none() {
        return;
    }
    let mut trail_block = [0u8; 64];
    let mut toff = 0;
    if build_grpc_trailers_block(&mut trail_block, &mut toff, grpc_status).is_none() {
        return;
    }
    let slot = &mut anchor.slots[slot_idx];
    let _ = append_frame(
        slot,
        FrameType::Headers as u8,
        FLAG_END_HEADERS,
        stream_id,
        &hdr_block[..hoff],
    );
    // Empty DATA with END_STREAM? We use the trailing HEADERS for
    // END_STREAM instead per gRPC.
    let _ = append_frame(
        slot,
        FrameType::Headers as u8,
        FLAG_END_HEADERS | FLAG_END_STREAM,
        stream_id,
        &trail_block[..toff],
    );
}

// ── KV reply path ─────────────────────────────────────────────────────

unsafe fn poll_routed_in(anchor: &mut AnchorState) -> bool {
    if anchor.routed_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let poll = ((*sys).channel_poll)(anchor.routed_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }

    let mut hdr = [0u8; 3];
    let n = ((*sys).channel_read)(anchor.routed_in, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    if hdr[0] != MSG_KV_RESPONSE {
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    const RESP_HEAD: usize = 8 + 1 + 1 + 8 + 2;
    if payload_len < RESP_HEAD || payload_len > anchor.scratch.len() {
        return false;
    }
    let n2 = ((*sys).channel_read)(anchor.routed_in, anchor.scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    // Parse the response head from scratch. We then immediately
    // consume the body before doing anything with scratch (the
    // write_response_frames path needs scratch as encode workspace).
    let corr;
    let result;
    let revision;
    let body_len;
    let body_off = RESP_HEAD;
    {
        let p = &anchor.scratch[..payload_len];
        corr = u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]]);
        let _conn_id = p[8];
        result = p[9];
        revision =
            u64::from_le_bytes([p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17]]) as i64;
        body_len = u16::from_le_bytes([p[18], p[19]]) as usize;
    }
    if body_off + body_len > payload_len {
        return false;
    }

    let Some((slot_idx, strm)) = anchor.find_inflight(corr) else {
        return true;
    };

    // Move the response body from scratch into response_data so
    // scratch is free for any subsequent envelope work, and so the
    // borrow checker can split frame-construction borrows cleanly.
    if body_len > anchor.response_data.len() {
        return true;
    }
    anchor.response_data[..body_len]
        .copy_from_slice(&anchor.scratch[body_off..body_off + body_len]);

    let (method, key_off, key_len, stream_id) = {
        let s = &anchor.slots[slot_idx].streams[strm];
        (s.method, 0usize, s.body_len, s.stream_id)
    };
    let _ = key_off;

    write_response_frames(
        anchor, slot_idx, strm, stream_id, method, result, revision, key_len, body_len,
    );
    anchor.slots[slot_idx].free_stream(strm);
    true
}

fn write_response_frames(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    stream_id: u32,
    method: GrpcMethod,
    result: u8,
    revision: i64,
    key_len: usize,
    kv_body_len: usize,
) {
    // Map KV_RESULT_* to gRPC status codes.
    //   OK / NOT_FOUND → 0 (the etcd Range / Put / Delete responses
    //   handle "absent key" inside the protobuf body, not via status).
    //   INTEGER → 0 — KV_OP_DELETE returns INTEGER carrying the
    //   deleted count, which the response builder folds into the
    //   DeleteRangeResponse.deleted field.
    //   Anything else → 13 (INTERNAL).
    //   COMPACTED → 11 (OUT_OF_RANGE) — etcd's `ErrCompacted`. A
    //   historical Range below the retained floor is a real, expected
    //   client-visible condition with a defined recovery (re-read at a
    //   current revision), NOT a server fault; reporting it as 13 would
    //   tell every etcd client the server was broken.
    let grpc_status: u8 = match result {
        KV_RESULT_OK | KV_RESULT_NOT_FOUND | KV_RESULT_INTEGER | KV_RESULT_RANGE => 0,
        KV_RESULT_COMPACTED => GRPC_STATUS_COMPACTED,
        _ => 13,
    };

    // Encode the gRPC DATA payload into a fresh region of scratch
    // (offset 0). The split-borrow here uses three disjoint fields:
    // scratch (write), stream body_buf (read for key), response_data
    // (read for kv body).
    let data_len = {
        let scratch = &mut anchor.scratch[..];
        let key = &anchor.slots[slot_idx].streams[strm].body_buf[..key_len];
        let kv_body = &anchor.response_data[..kv_body_len];
        build_grpc_response_with_key(method, result, revision, key, kv_body, scratch).unwrap_or(0)
    };

    let mut hdr_block = [0u8; 64];
    let mut hoff = 0;
    if build_response_headers_block(&mut hdr_block, &mut hoff).is_none() {
        return;
    }
    let mut trail_block = [0u8; 64];
    let mut toff = 0;
    if build_grpc_trailers_block(&mut trail_block, &mut toff, grpc_status).is_none() {
        return;
    }

    // Copy DATA out of scratch BEFORE borrowing slot.send_buf for
    // append_frame — those two would alias otherwise. We use the
    // response_data region (scratch was just written there).
    let mut data_holder = [0u8; STREAM_BODY_MAX];
    let copy_len = data_len.min(STREAM_BODY_MAX);
    data_holder[..copy_len].copy_from_slice(&anchor.scratch[..copy_len]);

    let slot = &mut anchor.slots[slot_idx];
    let _ = append_frame(
        slot,
        FrameType::Headers as u8,
        FLAG_END_HEADERS,
        stream_id,
        &hdr_block[..hoff],
    );
    if copy_len > 0 {
        let _ = append_frame(
            slot,
            FrameType::Data as u8,
            0,
            stream_id,
            &data_holder[..copy_len],
        );
    }
    let _ = append_frame(
        slot,
        FrameType::Headers as u8,
        FLAG_END_HEADERS | FLAG_END_STREAM,
        stream_id,
        &trail_block[..toff],
    );
}

// ── Lease reply path ──────────────────────────────────────────────────

/// Drain one `MSG_LEASE_STATE` envelope from `lease_in`, rendezvous
/// with the stream that issued the grant / revoke (via `lease_id`),
/// and emit the gRPC response frames. Returns `true` if an envelope
/// was consumed so the per-tick loop in `module_step` can keep
/// draining until the channel is empty.
///
/// Wire shape (`MSG_LEASE_STATE` payload, see `wire.rs`):
///   `[lease_id:8][session_epoch:4][ttl_ms:4]
///    [granted_at:8][keepalive_deadline:8]` = 32 bytes
///
/// `session_epoch` = 0 is the manager's "lease no longer exists" reply.
/// For a grant op that's an allocation failure → gRPC INTERNAL (13);
/// for a revoke op that's the expected success path.
unsafe fn poll_lease_in(anchor: &mut AnchorState) -> bool {
    if anchor.lease_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let poll = ((*sys).channel_poll)(anchor.lease_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    let n = ((*sys).channel_read)(anchor.lease_in, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    if hdr[0] != MSG_LEASE_STATE {
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    const LEASE_STATE_LEN: usize = 32;
    if payload_len < LEASE_STATE_LEN || payload_len > anchor.scratch.len() {
        return false;
    }
    let n2 = ((*sys).channel_read)(anchor.lease_in, anchor.scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    let lease_id = {
        let p = &anchor.scratch[..payload_len];
        u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]])
    };
    let session_epoch = u32::from_le_bytes([
        anchor.scratch[8],
        anchor.scratch[9],
        anchor.scratch[10],
        anchor.scratch[11],
    ]);
    let ttl_from_mgr = u32::from_le_bytes([
        anchor.scratch[12],
        anchor.scratch[13],
        anchor.scratch[14],
        anchor.scratch[15],
    ]);

    let Some((slot_idx, strm)) = anchor.find_lease_inflight(lease_id) else {
        return true; // unmatched / late arrival — keep draining
    };
    let method = anchor.slots[slot_idx].streams[strm].method;
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;

    // Failure handling: epoch=0 means the manager rejected (alloc
    // failed for grant, or lease didn't exist for revoke). Revoke
    // treats epoch=0 as success (already gone is fine); grant treats
    // it as INTERNAL.
    let grpc_status: u8 = match (method, session_epoch) {
        (GrpcMethod::LeaseGrant, 0) => 13,
        _ => 0,
    };

    write_lease_response_frames(
        anchor,
        slot_idx,
        strm,
        stream_id,
        method,
        grpc_status,
        lease_id,
        ttl_from_mgr,
    );
    anchor.slots[slot_idx].streams[strm].pending_lease_id = 0;
    // LeaseKeepAlive is bidi-streaming: the stream stays open for
    // further keepalives until the client closes its half. Unary lease
    // ops (Grant / Revoke) close after their single response.
    if !matches!(method, GrpcMethod::LeaseKeepAlive) {
        anchor.slots[slot_idx].free_stream(strm);
    }
    true
}

fn write_lease_response_frames(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    stream_id: u32,
    method: GrpcMethod,
    grpc_status: u8,
    lease_id: u64,
    ttl_from_mgr: u32,
) {
    // Build the protobuf payload into scratch. Revision is irrelevant
    // for Lease ops (etcd echoes its current cluster revision); pass 1.
    let data_len = {
        let scratch = &mut anchor.scratch[..];
        // Reserve the 5-byte gRPC message header; encode the protobuf
        // body, then back-patch the length prefix.
        if scratch.len() < GRPC_FRAME_HEADER_LEN {
            0
        } else {
            let mut proto_off: usize = 0;
            let proto_buf = &mut scratch[GRPC_FRAME_HEADER_LEN..];
            let ok = match method {
                GrpcMethod::LeaseGrant => {
                    // For grant, prefer the ttl the manager returned (which
                    // round-tripped through it); fall back to what we
                    // stashed when the manager echoes 0.
                    let stashed_ttl_ms = {
                        let bb = &anchor.slots[slot_idx].streams[strm].body_buf[..4];
                        u32::from_le_bytes([bb[0], bb[1], bb[2], bb[3]])
                    };
                    let ttl_ms = if ttl_from_mgr != 0 {
                        ttl_from_mgr
                    } else {
                        stashed_ttl_ms
                    };
                    let ttl_sec = (ttl_ms / 1000) as i64;
                    build_lease_grant_response(
                        proto_buf,
                        &mut proto_off,
                        1,
                        lease_id as i64,
                        ttl_sec,
                    )
                    .is_some()
                }
                GrpcMethod::LeaseRevoke => {
                    build_lease_revoke_response(proto_buf, &mut proto_off, 1).is_some()
                }
                GrpcMethod::LeaseKeepAlive => {
                    // Manager echoes ttl_ms; convert to seconds.
                    // ttl=0 from manager means "no such lease" — propagate
                    // (etcd clients treat ttl=0 as a revoke signal).
                    let ttl_sec = (ttl_from_mgr / 1000) as i64;
                    build_lease_keepalive_response(
                        proto_buf,
                        &mut proto_off,
                        1,
                        lease_id as i64,
                        ttl_sec,
                    )
                    .is_some()
                }
                _ => false,
            };
            if !ok {
                0
            } else {
                // Back-patch the gRPC message header
                // [compressed:1][length:4 BE] over the reserved bytes.
                scratch[0] = 0;
                scratch[1] = ((proto_off >> 24) & 0xFF) as u8;
                scratch[2] = ((proto_off >> 16) & 0xFF) as u8;
                scratch[3] = ((proto_off >> 8) & 0xFF) as u8;
                scratch[4] = (proto_off & 0xFF) as u8;
                GRPC_FRAME_HEADER_LEN + proto_off
            }
        }
    };

    let streaming = matches!(method, GrpcMethod::LeaseKeepAlive);

    let mut data_holder = [0u8; STREAM_BODY_MAX];
    let copy_len = data_len.min(STREAM_BODY_MAX);
    data_holder[..copy_len].copy_from_slice(&anchor.scratch[..copy_len]);

    // First response on a streaming RPC sends the response HEADERS;
    // subsequent responses skip them. Unary RPCs always send HEADERS.
    let need_headers = if streaming {
        !anchor.slots[slot_idx].streams[strm].response_headers_sent
    } else {
        true
    };

    if need_headers {
        let mut hdr_block = [0u8; 64];
        let mut hoff = 0;
        if build_response_headers_block(&mut hdr_block, &mut hoff).is_none() {
            return;
        }
        let slot = &mut anchor.slots[slot_idx];
        let _ = append_frame(
            slot,
            FrameType::Headers as u8,
            FLAG_END_HEADERS,
            stream_id,
            &hdr_block[..hoff],
        );
        if streaming {
            anchor.slots[slot_idx].streams[strm].response_headers_sent = true;
        }
    }

    if grpc_status == 0 && copy_len > 0 {
        let slot = &mut anchor.slots[slot_idx];
        let _ = append_frame(
            slot,
            FrameType::Data as u8,
            0,
            stream_id,
            &data_holder[..copy_len],
        );
    }

    if !streaming {
        let mut trail_block = [0u8; 64];
        let mut toff = 0;
        if build_grpc_trailers_block(&mut trail_block, &mut toff, grpc_status).is_none() {
            return;
        }
        let slot = &mut anchor.slots[slot_idx];
        let _ = append_frame(
            slot,
            FrameType::Headers as u8,
            FLAG_END_HEADERS | FLAG_END_STREAM,
            stream_id,
            &trail_block[..toff],
        );
    }
}

// ── Watch reply path ──────────────────────────────────────────────────

/// Process one bidi-streaming `WatchRequest` sitting in the stream's
/// body_buf. Strips the 5-byte gRPC framing, parses the protobuf,
/// and routes by `WatchOp`:
///
/// * `Create` — anchor allocates a fresh `watch_id`, forwards
///   `MSG_WATCH_CTRL` ctrl=create on `watch_ctrl_out`, and emits
///   the initial `WatchResponse{created=true, watch_id}` as a DATA
///   frame. The stream stays open for events.
/// * `Cancel` — forwards `MSG_WATCH_CTRL` ctrl=cancel and emits
///   `WatchResponse{canceled=true, watch_id}` followed by trailing
///   HEADERS with `END_STREAM` to close the stream.
///
/// After dispatch the request body is consumed so subsequent
/// WatchRequests on the same stream don't see stale bytes.
fn dispatch_watch_message(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let body_len = anchor.slots[slot_idx].streams[strm].body_len;

    // Peel the 5-byte gRPC message header off the front; need a
    // complete message length to proceed.
    let request_body = &anchor.slots[slot_idx].streams[strm].body_buf[..body_len];
    if request_body.len() < GRPC_FRAME_HEADER_LEN {
        return;
    }
    let Some(hdr) = GrpcMessageHeader::parse(&request_body[..GRPC_FRAME_HEADER_LEN]) else {
        anchor.slots[slot_idx].streams[strm].body_len = 0;
        return;
    };
    if hdr.compressed != 0 {
        anchor.slots[slot_idx].streams[strm].body_len = 0;
        return;
    }
    let proto_start = GRPC_FRAME_HEADER_LEN;
    let proto_end = match proto_start.checked_add(hdr.length as usize) {
        Some(e) if e <= request_body.len() => e,
        _ => return, // incomplete — wait for more DATA
    };
    // Decode + copy out everything we need before borrowing `anchor`
    // mutably for state updates / channel writes.
    let proto = &request_body[proto_start..proto_end];
    let (op, key_buf, key_len, cancel_id) = {
        let req = match WatchRequest::decode(proto) {
            Some(r) => r,
            None => {
                anchor.slots[slot_idx].streams[strm].body_len = 0;
                return;
            }
        };
        let mut kb = [0u8; 256];
        let kl = req.key.len().min(kb.len());
        kb[..kl].copy_from_slice(&req.key[..kl]);
        (req.op, kb, kl, req.cancel_watch_id)
    };

    // Consume the request bytes whether we honour it or not — leaving
    // them in body_buf would cause infinite re-dispatch.
    if proto_end == body_len {
        anchor.slots[slot_idx].streams[strm].body_len = 0;
    } else {
        let tail_len = body_len - proto_end;
        let buf = &mut anchor.slots[slot_idx].streams[strm].body_buf;
        buf.copy_within(proto_end..body_len, 0);
        anchor.slots[slot_idx].streams[strm].body_len = tail_len;
    }

    match op {
        Some(WatchOp::Create) => {
            let wid = anchor.next_watch_id();
            anchor.slots[slot_idx].streams[strm].watch_id = wid;
            // The registry's filter shape (see
            // `watch_registry::handle_create`) is structured:
            //   [start_rev:8][filter_bits:1][progress_notify:1]
            //   [conn:1][stream:4][key_len:2][key…]
            //   [range_end_len:2][range_end…]
            // We emit the minimal "prefix-empty / no filters /
            // current revision" variant — the anchor doesn't track
            // start_revision on the open stream today so 0 means
            // "current". Build the blob in a local buffer and hand
            // it to send_watch_ctrl.
            let mut filter = [0u8; 320];
            let mut fp = 0usize;
            // start_revision = 0 (current)
            filter[fp..fp + 8].copy_from_slice(&0i64.to_le_bytes());
            fp += 8;
            filter[fp] = 0; // filter_bits
            fp += 1;
            filter[fp] = 0; // progress_notify
            fp += 1;
            // The registry's conn byte is this anchor's SLOT INDEX —
            // the net conn id is u16 now and does not fit the byte.
            // Watch frames rendezvous by `watch_id`, so the byte is
            // informational only.
            filter[fp] = slot_idx as u8;
            fp += 1;
            filter[fp..fp + 4].copy_from_slice(&stream_id.to_le_bytes());
            fp += 4;
            let k = key_len.min(filter.len().saturating_sub(fp + 4));
            filter[fp..fp + 2].copy_from_slice(&(k as u16).to_le_bytes());
            fp += 2;
            filter[fp..fp + k].copy_from_slice(&key_buf[..k]);
            fp += k;
            filter[fp..fp + 2].copy_from_slice(&0u16.to_le_bytes()); // range_end_len = 0
            fp += 2;
            send_watch_ctrl(anchor, WATCH_CTRL_CREATE, wid, &filter[..fp]);
            send_watch_response(anchor, slot_idx, strm, stream_id, wid, true, false, None);
        }
        Some(WatchOp::Cancel) => {
            let wid = if cancel_id != 0 {
                cancel_id
            } else {
                anchor.slots[slot_idx].streams[strm].watch_id
            };
            send_watch_ctrl(anchor, WATCH_CTRL_CANCEL, wid, &[]);
            send_watch_response(anchor, slot_idx, strm, stream_id, wid, false, true, None);
            // Trailing HEADERS with END_STREAM closes the bidi stream.
            let mut trail_block = [0u8; 64];
            let mut toff = 0;
            if build_grpc_trailers_block(&mut trail_block, &mut toff, 0).is_some() {
                let slot = &mut anchor.slots[slot_idx];
                let _ = append_frame(
                    slot,
                    FrameType::Headers as u8,
                    FLAG_END_HEADERS | FLAG_END_STREAM,
                    stream_id,
                    &trail_block[..toff],
                );
            }
            anchor.slots[slot_idx].free_stream(strm);
        }
        Some(WatchOp::Progress) | None => {
            // Progress / unknown — no-op for now (no events to flush).
        }
    }
}

/// Emit `MSG_WATCH_CTRL` on `watch_ctrl_out`.
///
/// Payload (per modules/common/wire.rs):
///   `[ctrl:u8][session_id:u64 LE][session_epoch:u32 LE]
///    [tenant_id:u32 LE][kpg_id:u16 LE][filter_len:u16 LE][filter…]`
///
/// `session_id` carries the anchor-assigned `watch_id` so the
/// registry's reply (or fanout's `MSG_WATCH_FRAME`) can be matched
/// back. `filter` is the watched key prefix.
fn send_watch_ctrl(anchor: &mut AnchorState, ctrl: u8, watch_id: i64, filter: &[u8]) {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.watch_ctrl_out < 0 {
        return;
    }
    let key_len = filter.len().min(u16::MAX as usize);
    const ENVELOPE_HDR: usize = 3;
    let body_len = 1 + 8 + 4 + 4 + 2 + 2 + key_len;
    let total = ENVELOPE_HDR + body_len;
    if total > anchor.scratch.len() {
        return;
    }
    anchor.scratch[0] = MSG_WATCH_CTRL;
    anchor.scratch[1] = (body_len & 0xFF) as u8;
    anchor.scratch[2] = ((body_len >> 8) & 0xFF) as u8;
    let mut p = ENVELOPE_HDR;
    anchor.scratch[p] = ctrl;
    p += 1;
    anchor.scratch[p..p + 8].copy_from_slice(&(watch_id as u64).to_le_bytes());
    p += 8;
    // session_epoch (anchor side: bump on rebind; 1 today).
    anchor.scratch[p..p + 4].copy_from_slice(&1u32.to_le_bytes());
    p += 4;
    anchor.scratch[p..p + 4].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());
    p += 4;
    // kpg_id 0 — single placement group today.
    anchor.scratch[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
    p += 2;
    anchor.scratch[p..p + 2].copy_from_slice(&(key_len as u16).to_le_bytes());
    p += 2;
    anchor.scratch[p..p + key_len].copy_from_slice(&filter[..key_len]);
    unsafe {
        let _ = ((*sys).channel_write)(anchor.watch_ctrl_out, anchor.scratch.as_mut_ptr(), total);
    }
}

/// Encode a `WatchResponse` and write it as HTTP/2 frames on the
/// open stream. The first response on the stream sends a response
/// HEADERS frame; subsequent events skip HEADERS (single open stream).
fn send_watch_response(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    stream_id: u32,
    watch_id: i64,
    created: bool,
    canceled: bool,
    event: Option<(WatchEventKind, &[u8], &[u8], i64, i64)>,
) {
    // Encode protobuf into scratch (after 5-byte gRPC frame header).
    let data_len = {
        let scratch = &mut anchor.scratch[..];
        if scratch.len() < GRPC_FRAME_HEADER_LEN {
            0
        } else {
            let mut proto_off: usize = 0;
            let proto_buf = &mut scratch[GRPC_FRAME_HEADER_LEN..];
            let ok = build_watch_response_header(
                proto_buf,
                &mut proto_off,
                1,
                watch_id,
                created,
                canceled,
            )
            .is_some();
            let ok = if ok {
                if let Some((kind, key, value, mr, ver)) = event {
                    write_watch_event(proto_buf, &mut proto_off, kind, key, value, mr, ver)
                        .is_some()
                } else {
                    true
                }
            } else {
                false
            };
            if !ok {
                0
            } else {
                scratch[0] = 0;
                scratch[1] = ((proto_off >> 24) & 0xFF) as u8;
                scratch[2] = ((proto_off >> 16) & 0xFF) as u8;
                scratch[3] = ((proto_off >> 8) & 0xFF) as u8;
                scratch[4] = (proto_off & 0xFF) as u8;
                GRPC_FRAME_HEADER_LEN + proto_off
            }
        }
    };
    if data_len == 0 {
        return;
    }

    let mut data_holder = [0u8; STREAM_BODY_MAX];
    let copy_len = data_len.min(STREAM_BODY_MAX);
    data_holder[..copy_len].copy_from_slice(&anchor.scratch[..copy_len]);

    let need_headers = !anchor.slots[slot_idx].streams[strm].response_headers_sent;
    if need_headers {
        let mut hdr_block = [0u8; 64];
        let mut hoff = 0;
        if build_response_headers_block(&mut hdr_block, &mut hoff).is_some() {
            let slot = &mut anchor.slots[slot_idx];
            let _ = append_frame(
                slot,
                FrameType::Headers as u8,
                FLAG_END_HEADERS,
                stream_id,
                &hdr_block[..hoff],
            );
            anchor.slots[slot_idx].streams[strm].response_headers_sent = true;
        }
    }

    let slot = &mut anchor.slots[slot_idx];
    let _ = append_frame(
        slot,
        FrameType::Data as u8,
        0,
        stream_id,
        &data_holder[..copy_len],
    );
}

/// Drain one `MSG_WATCH_FRAME` envelope and push it as a DATA frame
/// on the stream tracking that `watch_id`.
///
/// `MSG_WATCH_FRAME` is anchor-agnostic — its body is the protocol-
/// native frame (an etcd v3 `WatchResponse` here). The body starts
/// with `[watch_id:u64 LE]` so we can rendezvous; the rest is the
/// already-encoded protobuf payload.
unsafe fn poll_watch_in(anchor: &mut AnchorState) -> bool {
    if anchor.watch_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let poll = ((*sys).channel_poll)(anchor.watch_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    let n = ((*sys).channel_read)(anchor.watch_in, hdr.as_mut_ptr(), 3);
    if n < 3 {
        return false;
    }
    if hdr[0] != MSG_WATCH_FRAME {
        return false;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    // Min envelope: 8 watch_id + 2 kpg + 8 rev + 1 op + 2 klen + 2 vlen = 23
    const MIN_WATCH_FRAME_LEN: usize = 8 + 2 + 8 + 1 + 2 + 2;
    if payload_len < MIN_WATCH_FRAME_LEN || payload_len > anchor.scratch.len() {
        return false;
    }
    let n2 = ((*sys).channel_read)(anchor.watch_in, anchor.scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    // Wire shape (per `watch_registry::handle_mutation`):
    //   [watch_id:8][kpg:2][rev:8][op:1][klen:2][k][vlen:2][v]
    // We copy the relevant slices into stack buffers before mutating
    // `anchor.scratch` for the protobuf encode below.
    let watch_id = i64::from_le_bytes([
        anchor.scratch[0],
        anchor.scratch[1],
        anchor.scratch[2],
        anchor.scratch[3],
        anchor.scratch[4],
        anchor.scratch[5],
        anchor.scratch[6],
        anchor.scratch[7],
    ]);
    let _kpg = u16::from_le_bytes([anchor.scratch[8], anchor.scratch[9]]);
    let revision = i64::from_le_bytes([
        anchor.scratch[10],
        anchor.scratch[11],
        anchor.scratch[12],
        anchor.scratch[13],
        anchor.scratch[14],
        anchor.scratch[15],
        anchor.scratch[16],
        anchor.scratch[17],
    ]);
    let op_byte = anchor.scratch[18];
    let key_len = u16::from_le_bytes([anchor.scratch[19], anchor.scratch[20]]) as usize;
    let key_off = 21;
    if key_off + key_len + 2 > payload_len {
        return false;
    }
    let vlen_off = key_off + key_len;
    let value_len =
        u16::from_le_bytes([anchor.scratch[vlen_off], anchor.scratch[vlen_off + 1]]) as usize;
    let value_off = vlen_off + 2;
    if value_off + value_len > payload_len {
        return false;
    }

    // Copy key/value out before reusing scratch for proto encode.
    let mut key_buf = [0u8; 256];
    let mut val_buf = [0u8; 4096];
    let key_clip = key_len.min(key_buf.len());
    let val_clip = value_len.min(val_buf.len());
    key_buf[..key_clip].copy_from_slice(&anchor.scratch[key_off..key_off + key_clip]);
    val_buf[..val_clip].copy_from_slice(&anchor.scratch[value_off..value_off + val_clip]);

    let Some((slot_idx, strm)) = anchor.find_watch_stream(watch_id) else {
        return true;
    };
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;

    // Map the worker's KV_OP_* op byte onto the etcd `mvccpb.EventType`.
    let event_kind = match op_byte {
        KV_OP_DELETE => WatchEventKind::Delete,
        _ => WatchEventKind::Put,
    };

    send_watch_response(
        anchor,
        slot_idx,
        strm,
        stream_id,
        watch_id,
        false,
        false,
        Some((
            event_kind,
            &key_buf[..key_clip],
            &val_buf[..val_clip],
            revision,
            1,
        )),
    );
    true
}

// ── NET-side handling (mirrors redis_edge_anchor) ─────────────────────

unsafe fn poll_net_in(anchor: &mut AnchorState) -> bool {
    if anchor.net_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let poll = ((*sys).channel_poll)(anchor.net_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let buf = anchor.scratch.as_mut_ptr();
    let (msg_type, payload_len) = net_read_frame(&*sys, anchor.net_in, buf, SCRATCH_BUF_SIZE);
    if msg_type == 0 && payload_len == 0 {
        return false;
    }
    // The NET frame payload sits in anchor.scratch starting at
    // offset NET_FRAME_HDR. Extract the small metadata (u16 LE
    // conn_id, optional data) before dispatching — we copy bytes
    // we still need into the slot's recv_buf so scratch is free
    // for downstream NET_CMD_SEND framing.
    let payload_start = NET_FRAME_HDR;
    let payload_end = (payload_start + payload_len).min(anchor.scratch.len());
    // Leading conn id (u16 LE) of the frame, when present.
    let frame_conn = net_conn_id(&anchor.scratch[payload_start..payload_end]);

    match msg_type {
        NET_MSG_BOUND => {
            // Multi-anchor: BOUND payload is `[conn:u16 LE][port:2 LE]`.
            // Only claim the listener whose port matches ours.
            if anchor.phase == AnchorPhase::WaitBound && payload_len >= 4 {
                let port = u16::from_le_bytes([
                    anchor.scratch[payload_start + 2],
                    anchor.scratch[payload_start + 3],
                ]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = frame_conn.unwrap_or(SLOT_FREE);
                    anchor.phase = AnchorPhase::Listening;
                    dev_log(&*sys, 3, b"[etcd_anc] bound".as_ptr(), 16);
                }
            } else if anchor.phase == AnchorPhase::WaitBound && payload_len >= NET_CONN_LEN {
                anchor.server_conn_id = frame_conn.unwrap_or(SLOT_FREE);
                anchor.phase = AnchorPhase::Listening;
                dev_log(&*sys, 3, b"[etcd_anc] bound".as_ptr(), 16);
            }
        }
        NET_MSG_ACCEPTED => {
            // Per-port filter — ACCEPTED carries the parent listener's
            // local_port at payload[2..4]; see redis/memcache anchors
            // for rationale.
            if payload_len >= 4 {
                let port = u16::from_le_bytes([
                    anchor.scratch[payload_start + 2],
                    anchor.scratch[payload_start + 3],
                ]);
                if port != anchor.listen_port {
                    return true;
                }
            }
            if let Some(new_id) = frame_conn {
                if anchor.alloc_slot(new_id).is_some() {
                    dev_log(&*sys, 3, b"[etcd_anc] accepted".as_ptr(), 19);
                } else {
                    let _ = net_send_close_pic(anchor, new_id);
                    dev_log(
                        &*sys,
                        2,
                        b"[etcd_anc] slots full: closed new client".as_ptr(),
                        40,
                    );
                }
            }
        }
        NET_MSG_DATA => {
            if payload_len > NET_CONN_LEN {
                let Some(conn_id) = frame_conn else {
                    return true;
                };
                let data_start = payload_start + NET_CONN_LEN;
                let data_end = payload_end;
                if let Some(idx) = anchor.find_slot(conn_id) {
                    // Move data from scratch into the slot's recv_buf
                    // before doing anything else (scratch is needed
                    // by downstream send paths). Then run the frame
                    // processor against the slot.
                    let avail = RECV_BUF_SIZE - anchor.slots[idx].recv_len;
                    let want = data_end - data_start;
                    if want > avail {
                        let slot = &mut anchor.slots[idx];
                        let last = slot.last_stream_id;
                        let _ = send_goaway(slot, last, ERROR_PROTOCOL_ERROR);
                        slot.phase = SlotPhase::Closing;
                    } else {
                        let n = anchor.slots[idx].recv_len;
                        let (scratch_src, slots_dst) = split_borrow(anchor);
                        slots_dst[idx].recv_buf[n..n + want]
                            .copy_from_slice(&scratch_src[data_start..data_end]);
                        slots_dst[idx].recv_len = n + want;
                        process_slot_recv(anchor, idx);
                    }
                }
            }
        }
        NET_MSG_CLOSED => {
            if let Some(conn_id) = frame_conn {
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                    dev_log(&*sys, 3, b"[etcd_anc] client closed".as_ptr(), 24);
                }
            }
        }
        NET_MSG_ERROR => {
            // MSG_ERROR is BROADCAST to every anchor sharing linux_net's
            // net_out, so it also carries errors for conns we don't own
            // (notably peer_router's outbound-dial failures on a
            // Raft-dialing node). A blanket `phase = Error` let one such
            // foreign error permanently stop this anchor from serving —
            // dead on every dialing node, fine on pure acceptors. Only
            // react to an error for a conn WE own: free that slot, keep
            // listening. See redis_edge_anchor for the same fix + trace.
            if let Some(conn_id) = frame_conn {
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                    anchor.m_net_errors += 1;
                }
            }
        }
        _ => {}
    }
    true
}

/// Split-borrow helper: returns disjoint references to `scratch`
/// (shared) and `slots` (exclusive), which the borrow checker can't
/// infer through field projection in some call shapes.
fn split_borrow(anchor: &mut AnchorState) -> (&[u8], &mut [Slot; MAX_CONNS]) {
    let scratch_ptr = anchor.scratch.as_ptr();
    let scratch_len = anchor.scratch.len();
    // SAFETY: We re-borrow `scratch` as shared from a raw pointer
    // taken before reborrowing `slots` as exclusive. The two fields
    // are disjoint memory regions on `AnchorState`, so this is sound
    // for the lifetime of the returned tuple.
    let scratch: &[u8] = unsafe { core::slice::from_raw_parts(scratch_ptr, scratch_len) };
    let slots: &mut [Slot; MAX_CONNS] = &mut anchor.slots;
    (scratch, slots)
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
    let wrote = net_write_frame(
        &*sys,
        out_chan,
        NET_CMD_BIND,
        payload.as_ptr(),
        2,
        scratch,
        SCRATCH_BUF_SIZE,
    );
    wrote > 0
}

unsafe fn net_send_close_pic(anchor: &mut AnchorState, conn_id: u16) -> bool {
    let sys = anchor.syscalls;
    let out_chan = anchor.net_out;
    if sys.is_null() || out_chan < 0 {
        return false;
    }
    let payload = conn_id.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    let wrote = net_write_frame(
        &*sys,
        out_chan,
        NET_CMD_CLOSE,
        payload.as_ptr(),
        NET_CONN_LEN,
        scratch,
        SCRATCH_BUF_SIZE,
    );
    wrote > 0
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
    let n = ((*sys).channel_write)(out_chan, scratch, total);
    n == total as i32
}

/// Process whatever's in `slot.recv_buf`: validate the HTTP/2
/// preface on first call, then loop consuming frames until nothing
/// more fits or a fatal error fires.
fn process_slot_recv(anchor: &mut AnchorState, slot_idx: usize) {
    if !anchor.slots[slot_idx].preface_seen {
        let slot = &mut anchor.slots[slot_idx];
        if slot.recv_len < HTTP2_PREFACE_LEN {
            return;
        }
        if !preface_matches(&slot.recv_buf[..HTTP2_PREFACE_LEN]) {
            let _ = send_goaway(slot, 0, ERROR_PROTOCOL_ERROR);
            slot.phase = SlotPhase::Closing;
            return;
        }
        slot.recv_buf
            .copy_within(HTTP2_PREFACE_LEN..slot.recv_len, 0);
        slot.recv_len -= HTTP2_PREFACE_LEN;
        slot.preface_seen = true;
        if !send_settings(slot) {
            let _ = send_goaway(slot, 0, ERROR_PROTOCOL_ERROR);
            slot.phase = SlotPhase::Closing;
            return;
        }
        slot.settings_sent = true;
    }

    loop {
        match process_one_frame(anchor, slot_idx) {
            FrameAction::Done => continue,
            FrameAction::Wait => break,
            FrameAction::Fatal => {
                let slot = &mut anchor.slots[slot_idx];
                let last = slot.last_stream_id;
                let _ = send_goaway(slot, last, ERROR_PROTOCOL_ERROR);
                slot.phase = SlotPhase::Closing;
                break;
            }
        }
    }
}

unsafe fn flush_slots(anchor: &mut AnchorState) {
    let mut i = 0;
    while i < MAX_CONNS {
        let (conn_id, send_len, phase) = {
            let s = &anchor.slots[i];
            (s.conn_id, s.send_len, s.phase)
        };
        if conn_id == SLOT_FREE {
            i += 1;
            continue;
        }
        if send_len > 0 {
            // Net_send_data_pic uses anchor.scratch internally; we
            // need a different staging area to avoid aliasing.
            // frame_payload is large enough and otherwise idle at
            // this point in the tick (between poll_net_in and the
            // next process_one_frame).
            if send_len <= anchor.frame_payload.len() {
                let (src, dst) = split_send_borrow(anchor, i);
                dst[..send_len].copy_from_slice(&src[..send_len]);
                if net_send_data_pic_from_frame(anchor, conn_id, send_len) {
                    anchor.slots[i].send_len = 0;
                }
            }
        }
        if phase == SlotPhase::Closing && anchor.slots[i].send_len == 0 {
            let _ = net_send_close_pic(anchor, conn_id);
            anchor.free_slot(i);
        }
        i += 1;
    }
}

/// Returns (slot send_buf prefix, frame_payload) without aliasing.
fn split_send_borrow(anchor: &mut AnchorState, slot_idx: usize) -> (&[u8], &mut [u8]) {
    let src_ptr = anchor.slots[slot_idx].send_buf.as_ptr();
    let src_len = anchor.slots[slot_idx].send_buf.len();
    // SAFETY: send_buf and frame_payload are disjoint memory
    // regions on `AnchorState`. Taking a raw pointer to send_buf
    // first, then borrowing frame_payload exclusively, is sound
    // because the regions don't overlap.
    let src: &[u8] = unsafe { core::slice::from_raw_parts(src_ptr, src_len) };
    let dst: &mut [u8] = &mut anchor.frame_payload[..];
    (src, dst)
}

/// Variant of net_send_data_pic that reads the payload from
/// anchor.frame_payload[..data_len] (not a borrowed slice).
unsafe fn net_send_data_pic_from_frame(
    anchor: &mut AnchorState,
    conn_id: u16,
    data_len: usize,
) -> bool {
    let sys = anchor.syscalls;
    let out_chan = anchor.net_out;
    if sys.is_null() || out_chan < 0 {
        return false;
    }
    let payload_len = NET_CONN_LEN + data_len;
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
        anchor.frame_payload.as_ptr(),
        scratch.add(NET_FRAME_HDR + NET_CONN_LEN),
        data_len,
    );
    let total = NET_FRAME_HDR + payload_len;
    let n = ((*sys).channel_write)(out_chan, scratch, total);
    n == total as i32
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
    _params: *const u8,
    _params_len: usize,
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

    anchor.net_in = in_chan;
    anchor.net_out = out_chan;

    // Manifest port order:
    // inputs:  net_in[0], routed_in[1], watch_in[2], lease_in[3]
    // outputs: net_out[0], kv_out[1], watch_ctrl[2], lease_ctrl[3], metrics[4]
    unsafe {
        let sys = &*sys_ptr;
        anchor.routed_in = dev_channel_port(sys, 0, 1);
        anchor.watch_in = dev_channel_port(sys, 0, 2);
        anchor.lease_in = dev_channel_port(sys, 0, 3);
        anchor.kv_out = dev_channel_port(sys, 1, 1);
        anchor.watch_ctrl_out = dev_channel_port(sys, 1, 2);
        anchor.lease_ctrl_out = dev_channel_port(sys, 1, 3);
        anchor.metrics_out = dev_channel_port(sys, 1, 4);
    }
    0
}

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

    // Drain both channels in a per-tick loop so we don't gate every
    // SET on the 1-frame-per-tick poll ceiling. HTTP/2 splits one
    // logical request across multiple frames (preface / SETTINGS /
    // HEADERS / DATA / END_STREAM), so 1 envelope/tick caps the
    // anchor at `tick_hz / frames_per_op` — well below the worker.
    const PER_TICK_ANCHOR_BUDGET: u32 = 64;
    let mut budget = PER_TICK_ANCHOR_BUDGET;
    unsafe {
        while budget > 0 {
            let net = poll_net_in(anchor);
            let routed = poll_routed_in(anchor);
            let lease = poll_lease_in(anchor);
            let watch = poll_watch_in(anchor);
            if !net && !routed && !lease && !watch {
                break;
            }
            budget -= 1;
        }
        flush_slots(anchor);

        // Phase-14: emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
        if anchor.step_ctr.is_multiple_of(5000) && !anchor.syscalls.is_null() {
            telemetry::emit_counters(
                &*anchor.syscalls,
                anchor.metrics_out,
                &[anchor.m_requests, anchor.m_forwarded, anchor.m_net_errors],
            );
        }
    }
    0
}
