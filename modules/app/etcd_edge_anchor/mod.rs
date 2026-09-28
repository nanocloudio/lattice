//! etcd_edge_anchor — gRPC/HTTP2 edge anchor for the etcd v3 API.
//!
//! Continuity classes:
//! - Unary KV (Range / Put / DeleteRange): `drain_only`. CAS
//!   correctness comes from `kv_state_worker` revision fences, not from
//!   connection continuity.
//! - Watch and LeaseKeepAlive: `edge_anchored`. Each is a SessionCtrlV1
//!   session the anchor fronts; the watch and lease state live in a
//!   worker (`watch_registry` / `lease_manager`) that can be handed off
//!   to a standby without the client's stream breaking.
//!
//! ## What this module owns
//!
//! - TCP bind + accept against foundation/ip.
//! - Per-connection slot table with HTTP/2 state: preface check, frame
//!   parser, HPACK decoder, per-stream request body buffer.
//! - Translation of the etcd v3 KV subset
//!   (`/etcdserverpb.KV/{Range,Put,DeleteRange}`) into protocol-neutral
//!   `KV_OP_*` envelopes on `kv_out`. Responses arrive on `routed_in`
//!   as `MSG_KV_RESPONSE` and are re-encoded as gRPC HEADERS + DATA +
//!   trailing HEADERS frames.
//! - The watch and lease session layers (`wsa` / `lsa`): identity
//!   minting, ATTACH / DETACH, forwarding control to the serving
//!   worker, and relaying a handoff between the active worker and a
//!   standby. Watch events arrive framed on `watch_in` and route by
//!   `watch_id`; lease state arrives on `lease_in` and routes by
//!   `lease_id`.
//!
//! ## What this module does NOT own
//!
//! - HPACK dynamic-table tracking — it advertises
//!   `SETTINGS_HEADER_TABLE_SIZE = 0`, so a conforming gRPC client never
//!   emits dynamic-table references and the decoder stays a pure
//!   function over the static table + literals (see
//!   `modules/common/etcd_codec.rs`).
//! - HTTP/2 strict compliance — PRIORITY / PUSH_PROMISE / CONTINUATION
//!   are not implemented; etcd-client does not emit them.
//! - TLS — terminated upstream by foundation/tls when a secure provider
//!   is wired in front.

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
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/kv_large.rs"]
mod kv_large;

#[path = "../../common/etcd_auth.rs"]
mod etcd_auth;
use etcd_auth::sha::Sha256;
use etcd_auth::{AuthCache, Name, Perm, Role, User, AUTH_KEYSPACE, NEED_READ, NEED_WRITE};

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/etcd_codec.rs"]
mod etcd_codec;

#[path = "../../common/telemetry.rs"]
mod telemetry;

#[path = "../../common/session_anchor.rs"]
mod session_anchor;

use etcd_codec::{
    build_grpc_trailers_block, build_lease_grant_response, build_lease_keepalive_response,
    build_lease_revoke_response, build_put_response, build_range_response,
    build_response_headers_block, build_watch_response_header_full, preface_matches,
    write_watch_event, DeleteRangeRequest, FrameHeader, FrameType, GrpcMessageHeader, HpackDecoder,
    LeaseGrantRequest, LeaseKeepAliveRequest, LeaseRevokeRequest, LeaseTimeToLiveRequest,
    PutRequest, RangeRequest, SettingsIter, WatchEventKind, WatchOp, WatchRequest, ERROR_NO_ERROR,
    ERROR_PROTOCOL_ERROR, FLAG_ACK, FLAG_END_HEADERS, FLAG_END_STREAM, FRAME_HEADER_LEN,
    GRPC_FRAME_HEADER_LEN, GRPC_STATUS_COMPACTED, HTTP2_PREFACE_LEN,
};
use session_anchor::session_core::session_ctrl as sc;
use session_anchor::session_core::{
    anchor_id as make_anchor_id, mint_session_id, session_app_id, CLASS_LEASE, CLASS_WATCH,
    FIRST_EPOCH, NO_WORKER,
};
use session_anchor::{AnchorAction, SessionAnchor, Target};
use types::{KV_OP_COMPACT, KV_OP_HASH_KV, KV_OP_LEASE_KEYS, KV_OP_LEASE_REVOKE};
use types::{
    KV_OP_DELETE, KV_OP_DELETE_META, KV_OP_GET, KV_OP_GET_AT, KV_OP_PUT, KV_OP_RANGE_META,
    KV_OP_RANGE_SCAN, KV_RESULT_COMPACTED, KV_RESULT_FUTURE_REV, KV_RESULT_INTEGER, KV_RESULT_KVS,
    KV_RESULT_NOT_FOUND, KV_RESULT_OK, KV_RESULT_RANGE, PROTO_ETCD, PUT_FLAG_IGNORE_LEASE,
    PUT_FLAG_IGNORE_VALUE, PUT_FLAG_LEASE, PUT_FLAG_PREV_META,
};
use types::{KV_OP_ETCD_TXN, KV_RESULT_DUPLICATE, KV_RESULT_ETCD_TXN};
use wire::MSG_LEASE_REVOKE;
use wire::{
    MSG_KV_REQUEST, MSG_KV_RESPONSE, MSG_LEASE_CTRL, MSG_LEASE_STATE, MSG_WATCH_CTRL,
    MSG_WATCH_FRAME, WATCH_CTRL_DEFINE, WATCH_FRAME_OP_COMPACTED,
};

const LEASE_CTRL_GRANT: u8 = 0;
const LEASE_CTRL_REVOKE: u8 = 1;
const LEASE_CTRL_KEEPALIVE: u8 = 2;
const LEASE_CTRL_TIME_TO_LIVE: u8 = 3;

/// etcd's ErrLeaseNotFound, as its clients read it.
const LEASE_NOT_FOUND: &[u8] = b"etcdserver: requested lease not found";

/// The cluster this anchor's members belong to, as etcd names it in
/// every `ResponseHeader` ("lattice" and a version byte).
const CLUSTER_ID: u64 = 0x6C61_7474_6963_6501;

/// The etcd version the surface implements, as `Status` reports it.
const ETCD_VERSION: &[u8] = b"3.5.0";

/// Alarms the anchor holds at once (etcd's NOSPACE / CORRUPT per member).
const ALARMS_MAX: usize = 4;

/// A token's longest form: 16 random characters, `.`, a sequence.
const TOKEN_MAX: usize = 32;
/// `token_len` of a token that cannot be one of ours.
const TOKEN_BAD: u8 = 0xFF;
/// Tokens the anchor holds at once; the oldest makes way.
const TOKENS_MAX: usize = 64;
/// How long a token lives unused (etcd's simple-token TTL).
const TOKEN_TTL_MS: u64 = 300_000;
/// One auth change's records, staged until its Txn commits.
const AUTH_CHANGES_MAX: usize = 3072;

/// An issued token: the user it names and when it lapses.
#[derive(Clone, Copy)]
struct Token {
    len: u8,
    bytes: [u8; TOKEN_MAX],
    user: Name,
    expires_ms: u64,
}

impl Token {
    const FREE: Token = Token {
        len: 0,
        bytes: [0; TOKEN_MAX],
        user: Name::EMPTY,
        expires_ms: 0,
    };
}

/// Reading a keyspace of the anchor's own records at start (the auth
/// records, the lease records), page by page in key order.
#[repr(C)]
struct RecordLoad {
    /// 0 to start, 1 a page in flight, 2 loaded.
    state: u8,
    corr: u64,
    next: [u8; 258],
    next_len: usize,
    /// When a failed load is retried.
    retry_ms: u64,
    /// A read of the records has been refused: until one succeeds the
    /// store, not the load, is why requests wait.
    failed: bool,
}

/// The one auth change in flight: its stream, and the records its Txn
/// writes, applied to the cache once it commits
/// (`[kind:u8 1 put / 0 delete][klen:u8][key][vlen:u16][value]` each).
#[repr(C)]
struct AuthJob {
    active: bool,
    slot: usize,
    strm: usize,
    stream_id: u32,
    corr: u64,
    len: usize,
    changes: [u8; AUTH_CHANGES_MAX],
}

/// Keys a Snapshot reads per page (a page also stops where the store's
/// answer fills).
const SNAP_PAGE_KEYS: i64 = 16;
/// Blob bytes per SnapshotResponse message.
const SNAP_CHUNK: usize = 1536;
/// One page of a Snapshot, as the store answered it.
const SNAP_BUF: usize = REPLY_BUF_SIZE;

/// The Snapshot stream in progress (one per anchor).
#[repr(C)]
struct SnapState {
    active: bool,
    /// A page request is in flight.
    waiting: bool,
    /// The store holds keys past the page in hand.
    more: bool,
    slot: usize,
    strm: usize,
    stream_id: u32,
    /// The revision every page is read at (0 until the first answers).
    rev: u64,
    /// Where the next page starts: the last key sent, then a 0 byte.
    next: [u8; 258],
    next_len: usize,
    /// The page in hand, and how much of it has gone out.
    buf: [u8; SNAP_BUF],
    len: usize,
    sent: usize,
    /// Blob bytes streamed so far.
    total: u64,
    hash: Sha256,
}

/// The reserved keyspace etcd's lease records live in: `[id:u64 BE]` →
/// `[ttl_ms:u32 LE]`, one per live lease (etcd's family `0x800B_00xx`;
/// the auth records are `etcd_auth::AUTH_KEYSPACE`, `0x800B_0001`). A
/// lease is written before its grant is answered and deleted after its
/// keys, so a restart re-grants exactly the leases that were live.
const LEASE_KEYSPACE: u32 = 0x800B_0002;

/// The anchor's record loads.
const LOAD_AUTH: u8 = 0;
const LOAD_LEASES: u8 = 1;

/// A grant's record is being written (`body_buf[8]`).
const GRANT_PERSISTING: u8 = 3;
/// A revoke's record is being deleted, its keys gone (`body_buf[8]`).
const REVOKE_RECORD: u8 = 4;

/// Ended leases whose keys wait to be deleted.
const LEASE_END_QUEUE: usize = 32;

// ── NET protocol constants (foundation/ip Stream Surface v1) ──────────

use abi::contracts::net::net_proto::{
    conn_id, CMD_BIND as NET_CMD_BIND, CMD_CLOSE as NET_CMD_CLOSE, CMD_SEND as NET_CMD_SEND,
    CONN_ID_LEN, MSG_ACCEPTED as NET_MSG_ACCEPTED, MSG_BOUND as NET_MSG_BOUND,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_DATA as NET_MSG_DATA, MSG_ERROR as NET_MSG_ERROR,
};

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

/// Per-slot recv buffer (frames accumulate here before parsing): one
/// frame as large as HTTP/2 lets a peer send by default
/// (SETTINGS_MAX_FRAME_SIZE 16384, plus its 9-byte header) and the start
/// of the next.
const RECV_BUF_SIZE: usize = 20489;

/// Per-slot send buffer (response frames awaiting wire delivery): the
/// largest response (`RESP_OUT_MAX`) with room for the watch events and
/// acks queued beside it. Flushed to the network in NET-frame chunks.
const SEND_BUF_SIZE: usize = 32768;

/// Per-stream request body buffer: an etcd request as large as the store
/// takes (a Put of a 4 KiB value under a 256-byte key, in its gRPC
/// frame). A larger body is refused as etcd refuses one: "request is
/// too large".
const STREAM_BODY_MAX: usize = 4608;

/// One encoded gRPC response: a worker answer as large as it builds
/// (8 KiB) re-encoded as protobuf, in one DATA frame (at most 16384).
const RESP_OUT_MAX: usize = 16000;

/// HPACK scratch — large enough for several KB of decoded headers
/// (one HEADERS block worth, since we reset between blocks).
const HPACK_SCRATCH_LEN: usize = 4096;

/// Scratch for NET_CMD frames and KV envelopes.
const SCRATCH_BUF_SIZE: usize = 4096 + 64;

/// One inbound envelope of the KV or watch path: the worker's largest
/// answer (8192 bytes) behind the router's head and fence tail, or the
/// largest watch frame (`wire::WATCH_EVENT_MAX` behind its watch id).
const REPLY_BUF_SIZE: usize = 8832;

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
    /// `/etcdserverpb.KV/Txn` — unary; one `KV_OP_ETCD_TXN`.
    Txn = 8,
    /// `/etcdserverpb.Lease/LeaseTimeToLive` — unary; the lease's
    /// manager answers the TTL, the store its keys.
    LeaseTimeToLive = 9,
    /// `/etcdserverpb.Lease/LeaseLeases` — unary; answered from the
    /// anchor's lease sessions.
    LeaseLeases = 10,
    /// `/etcdserverpb.KV/Compact` — unary; one `KV_OP_COMPACT`.
    Compact = 11,
    /// `/etcdserverpb.Maintenance/Status` — the store's size by one
    /// `KV_OP_HASH_KV`, the rest the anchor's.
    Status = 12,
    /// `/etcdserverpb.Maintenance/Alarm` — the anchor's alarm table.
    Alarm = 13,
    /// `/etcdserverpb.Maintenance/Defragment` — nothing to reclaim: the
    /// providers compact as maintenance.
    Defragment = 14,
    /// `/etcdserverpb.Maintenance/Hash` and `HashKV` — `KV_OP_HASH_KV`.
    Hash = 15,
    HashKv = 16,
    /// `/etcdserverpb.Maintenance/MoveLeader` — one member leads.
    MoveLeader = 17,
    /// `/etcdserverpb.Cluster/Member*` — the anchor answers for the one
    /// member it is; membership itself is the graph's.
    MemberList = 18,
    MemberAdd = 19,
    MemberRemove = 20,
    MemberUpdate = 21,
    MemberPromote = 22,
    /// `/etcdserverpb.Maintenance/Snapshot` — server-streaming: the live
    /// keys at one revision, page by page, then their SHA-256.
    Snapshot = 23,
    /// `/etcdserverpb.Maintenance/Downgrade` — refused: one version runs.
    Downgrade = 24,
    /// `/etcdserverpb.Auth/*` — the anchor's auth cache answers reads;
    /// a change commits to the auth keyspace as one Txn first.
    AuthEnable = 25,
    AuthDisable = 26,
    AuthStatus = 27,
    Authenticate = 28,
    UserAdd = 29,
    UserGet = 30,
    UserList = 31,
    UserDelete = 32,
    UserChangePassword = 33,
    UserGrantRole = 34,
    UserRevokeRole = 35,
    RoleAdd = 36,
    RoleGet = 37,
    RoleList = 38,
    RoleDelete = 39,
    RoleGrantPermission = 40,
    RoleRevokePermission = 41,
}

impl GrpcMethod {
    /// An Auth-service RPC.
    fn is_auth(self) -> bool {
        (self as u8) >= GrpcMethod::AuthEnable as u8
            && (self as u8) <= GrpcMethod::RoleRevokePermission as u8
    }

    /// Needs etcd's root role while auth is enabled: membership,
    /// maintenance that reads or rewrites the whole store, and every auth
    /// change.
    fn is_admin(self) -> bool {
        matches!(
            self,
            GrpcMethod::Compact
                | GrpcMethod::Defragment
                | GrpcMethod::Snapshot
                | GrpcMethod::Hash
                | GrpcMethod::HashKv
                | GrpcMethod::MoveLeader
                | GrpcMethod::Downgrade
                | GrpcMethod::MemberAdd
                | GrpcMethod::MemberRemove
                | GrpcMethod::MemberUpdate
                | GrpcMethod::MemberPromote
        ) || (self.is_auth() && self != GrpcMethod::Authenticate)
    }
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
    /// For the bidirectional Watch stream: its watches. A client
    /// multiplexes several on one stream (etcd's clients do), each
    /// known to the registry by the anchor's session id (`app`, which
    /// `MSG_WATCH_FRAME` routes by) and to the client by its own id.
    watches: [WatchEnt; WATCHES_PER_STREAM],
    /// Whether the initial response HEADERS frame has been sent. Set
    /// on the first WatchResponse so subsequent events skip HEADERS.
    /// Unary RPCs leave this false (write_response_frames sends
    /// HEADERS itself per request).
    response_headers_sent: bool,
    /// The request's `token` metadata (0 = none, `TOKEN_BAD` = one that
    /// cannot be ours).
    token: [u8; TOKEN_MAX],
    token_len: u8,
    body_buf: [u8; STREAM_BODY_MAX],
    body_len: usize,
    /// The request's body length, kept after it is sent: a store that
    /// answers "a large value" sends it again through the layer.
    kept_len: usize,
    /// Go through the large-value layer (a retry of a direct request).
    lg_retry: bool,
    /// Waiting for a free layer session; `body_len` still holds it.
    deferred: bool,
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
            watches: [WatchEnt::FREE; WATCHES_PER_STREAM],
            response_headers_sent: false,
            token: [0; TOKEN_MAX],
            token_len: 0,
            body_buf: [0; STREAM_BODY_MAX],
            body_len: 0,
            kept_len: 0,
            lg_retry: false,
            deferred: false,
        }
    }

    fn reset(&mut self) {
        self.stream_id = STREAM_FREE;
        self.method = GrpcMethod::Unknown;
        self.end_stream_seen = false;
        self.headers_done = false;
        self.pending_corr = 0;
        self.pending_lease_id = 0;
        self.watches = [WatchEnt::FREE; WATCHES_PER_STREAM];
        self.response_headers_sent = false;
        self.token_len = 0;
        self.body_len = 0;
        self.kept_len = 0;
        self.lg_retry = false;
        self.deferred = false;
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

    /// The stream holding the watch with session id `app`, and the id
    /// the client knows it by.
    fn find_stream_by_watch(&self, app: i64) -> Option<(usize, i64)> {
        self.streams.iter().enumerate().find_map(|(i, s)| {
            if s.stream_id == STREAM_FREE {
                return None;
            }
            s.watches
                .iter()
                .find(|w| w.app == app)
                .map(|w| (i, w.client))
        })
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

/// A watch's key and range_end together, as the watch hub holds them
/// (`watch_hub::WATCH_KEY_MAX`).
const WATCH_KEY_MAX: usize = 96;

/// The longest key the store holds for this surface: the store's
/// physical key less its 12-byte identity (`kv_store::MAX_KEY_LEN` −
/// `IDENT_LEN`; a contract test holds them together). Keys the store
/// answers are copied whole, never clipped.
const STORE_KEY_MAX: usize = 2572;

/// The store's PUT flag bits, as the codec (mounted without `types`)
/// takes them.
const PUT_BITS: etcd_codec::PutFlagBits = etcd_codec::PutFlagBits {
    lease: PUT_FLAG_LEASE,
    prev: PUT_FLAG_PREV_META,
    ignore_value: PUT_FLAG_IGNORE_VALUE,
    ignore_lease: PUT_FLAG_IGNORE_LEASE,
};

/// Watches one Watch stream multiplexes.
const WATCHES_PER_STREAM: usize = 8;

/// One watch on a stream: the registry's session id (`app`, 0 = free)
/// and the id the client knows it by.
#[derive(Clone, Copy)]
struct WatchEnt {
    app: i64,
    client: i64,
    /// Whether its events carry the version they replaced.
    prev_kv: bool,
}

impl WatchEnt {
    const FREE: WatchEnt = WatchEnt {
        app: 0,
        client: 0,
        prev_kv: false,
    };
}

/// One watch event as the client gets it: the key's metadata is
/// `[create, mod, version, lease]`, and `prev` the version it replaced.
#[derive(Clone, Copy)]
struct WatchEv<'a> {
    kind: WatchEventKind,
    key: &'a [u8],
    value: &'a [u8],
    meta: [u64; 4],
    prev: Option<(&'a [u8], [u64; 4])>,
}

/// Lease sessions the anchor can front (one per active lease).
const LEASE_SESSIONS: usize = 128;
/// Watch sessions the anchor can front (one per open watch stream).
const WATCH_SESSIONS: usize = 64;

/// Requests carried through the large-value layer at once: a large
/// value's upload, a manifest's expansion, a Txn that reads. A request
/// that finds every one busy waits in its stream.
const LG_SESSIONS: usize = 8;
/// A request marked for the layer: the largest op body, grown by its
/// marks.
const LG_WORK: usize = kv_large::mark_room(SCRATCH_BUF_SIZE);
/// A layer exchange that has not come back in this long is abandoned and
/// its stream answered unavailable.
const LG_STALL_MS: u64 = 30_000;
/// A session's `corr` while its next exchange waits for room on `kv_out`
/// (no real corr is this: those carry the protocol in their top byte).
const LG_UNSENT: u64 = 1;
/// The head's conn byte for chunk exchanges (no slot index is this). A
/// chunk is content-addressed and immutable and no client sees its
/// exchanges, so they take no connection's order and no read fence —
/// held behind a client's fence, an upload would outgrow the router's
/// hold. Only the request itself keeps its connection and consistency.
const LG_CHUNK_CONN: u8 = u8::MAX;

/// The longest value a watch event carries once its manifest is fetched:
/// it and the value it replaced, with a key, fit one encoded response.
const WATCH_VALUE_MAX: usize = (RESP_OUT_MAX - STORE_KEY_MAX - 512) / 2;

/// A watch event whose value, or the value it replaced, is a large
/// value's manifest, held while the value is fetched. One at a time:
/// events behind it wait in `watch_in`, so every watch keeps its order.
struct WatchLg {
    /// The fetch in flight; [`LG_UNSENT`] waiting for room; 0 = idle.
    corr: u64,
    since_ms: u64,
    frame: [u8; REPLY_BUF_SIZE],
    frame_len: usize,
    /// The manifests to fetch: the event's value's and the replaced
    /// value's; which is being fetched (0, 1), and its reader.
    manifests: [Option<kv_large::kv_chunks::Manifest>; 2],
    part: usize,
    reader: Option<kv_large::kv_chunks::Reader>,
    /// The chunk the fetch in flight asks for.
    want: kv_large::kv_chunks::Hash,
    values: [[u8; WATCH_VALUE_MAX]; 2],
    lens: [usize; 2],
}

/// A `MSG_WATCH_FRAME`'s parts: offsets into it, and the metadata its
/// tail carries (or a first version's, without one).
struct WatchFrame {
    watch_id: i64,
    revision: i64,
    op: u8,
    key: (usize, usize),
    value: (usize, usize),
    meta: [u64; 4],
    /// The replaced version: its value, its metadata, and whether the
    /// value is a manifest.
    prev: Option<((usize, usize), [u64; 4], bool)>,
}

/// Parse a watch frame:
/// `[watch_id:8][kpg:2][rev:8][op:1][klen:2][k][vlen:2][v]` and the
/// optional tail `[create:8][version:8][lease:8][prev:1]` then, when
/// `prev` is not 0, `[pvlen:2][pv][pcreate:8][pmod:8][pversion:8]
/// [please:8]` (`prev` 2: `pv` is a manifest).
fn parse_watch_frame(f: &[u8]) -> Option<WatchFrame> {
    let rd8 = |at: usize| -> u64 {
        f.get(at..at + 8)
            .and_then(|x| x.try_into().ok())
            .map_or(0, u64::from_le_bytes)
    };
    let watch_id = rd8(0) as i64;
    let revision = rd8(10) as i64;
    let op = *f.get(18)?;
    let kl = u16::from_le_bytes([*f.get(19)?, *f.get(20)?]) as usize;
    let key = (21, 21 + kl);
    let vo = key.1;
    let vl = u16::from_le_bytes([*f.get(vo)?, *f.get(vo + 1)?]) as usize;
    let value = (vo + 2, vo + 2 + vl);
    f.get(..value.1)?;
    let rev = revision as u64;
    let delete = op == KV_OP_DELETE;
    let mut meta = if delete {
        [0, rev, 0, 0]
    } else {
        [rev, rev, 1, 0]
    };
    let mut prev = None;
    let t = value.1;
    if t + 25 <= f.len() {
        if !delete {
            meta = [rd8(t), rev, rd8(t + 8), rd8(t + 16)];
        }
        let marker = *f.get(t + 24)?;
        if marker != 0 && f.len() >= t + 27 {
            let pl = u16::from_le_bytes([*f.get(t + 25)?, *f.get(t + 26)?]) as usize;
            let pv = (t + 27, t + 27 + pl);
            let m = pv.1;
            if m + 32 <= f.len() {
                prev = Some((
                    pv,
                    [rd8(m), rd8(m + 8), rd8(m + 16), rd8(m + 24)],
                    marker == types::JOURNAL_PREV_MANIFEST,
                ));
            }
        }
    }
    Some(WatchFrame {
        watch_id,
        revision,
        op,
        key,
        value,
        meta,
        prev,
    })
}

/// A stream's request carried through the large-value layer.
struct LgSlot {
    session: kv_large::Session,
    /// The exchange in flight; 0 = the slot is free.
    corr: u64,
    slot_idx: usize,
    strm: usize,
    stream_id: u32,
    consistency: u8,
    /// The answer's revision: the request's own reply's.
    revision: i64,
    /// The exchange waiting to be sent (`corr` = [`LG_UNSENT`]).
    unsent_op: u8,
    unsent_len: usize,
    since_ms: u64,
    req_len: usize,
    req: [u8; SCRATCH_BUF_SIZE],
    work: [u8; LG_WORK],
    send: [u8; LG_WORK],
    out: [u8; REPLY_BUF_SIZE],
}

/// Write one `[msg][len:2 LE][payload…]` envelope to `chan`.
unsafe fn write_env(sys: *const SyscallTable, chan: i32, msg: u8, payload: &[u8]) -> bool {
    if sys.is_null() || chan < 0 || payload.len() > u16::MAX as usize {
        return false;
    }
    let mut buf = [0u8; SCRATCH_BUF_SIZE + 3];
    let total = 3 + payload.len();
    if total > buf.len() {
        return false;
    }
    buf[0] = msg;
    buf[1] = (payload.len() & 0xFF) as u8;
    buf[2] = ((payload.len() >> 8) & 0xFF) as u8;
    buf[3..total].copy_from_slice(payload);
    ((*sys).channel_write)(chan, buf.as_mut_ptr(), total) == total as i32
}

/// A `SessionAnchor` sink over the two watch_registry control channels.
macro_rules! watch_sink {
    ($a:expr) => {{
        let sys = $a.syscalls;
        let c0 = $a.watch_ctrl_out;
        let c1 = $a.watch_ctrl2_out;
        move |t: Target, m: u8, p: &[u8]| -> bool {
            let chan = match t {
                Target::Worker(w) => {
                    if w == 0 {
                        c0
                    } else if w == 1 {
                        c1
                    } else {
                        -1
                    }
                }
                Target::Directory => -1,
            };
            unsafe { write_env(sys, chan, m, p) }
        }
    }};
}

/// A `SessionAnchor` sink over the two lease_manager control channels.
macro_rules! lease_sink {
    ($a:expr) => {{
        let sys = $a.syscalls;
        let c0 = $a.lease_ctrl_out;
        let c1 = $a.lease_ctrl2_out;
        move |t: Target, m: u8, p: &[u8]| -> bool {
            let chan = match t {
                Target::Worker(w) => {
                    if w == 0 {
                        c0
                    } else if w == 1 {
                        c1
                    } else {
                        -1
                    }
                }
                Target::Directory => -1,
            };
            unsafe { write_env(sys, chan, m, p) }
        }
    }};
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
    watch_ctrl2_out: i32,
    lease_ctrl_out: i32,
    lease_ctrl2_out: i32,
    metrics_out: i32,

    listen_port: u16,
    phase: AnchorPhase,
    _pad0: u8,

    /// This anchor's 8-byte SessionCtrlV1 identity, from its listen
    /// port. Watch and lease sessions are minted under it.
    anchor_id: [u8; 8],
    /// SessionCtrlV1 replies from the two lease_managers (per worker),
    /// and the relocation command that drives a lease handoff.
    lease_sess0_in: i32,
    lease_sess1_in: i32,
    watch_sess0_in: i32,
    watch_sess1_in: i32,
    relocate_in: i32,
    /// `MSG_LEASE_REVOKE` from the lease manager: a lease ended.
    lease_revoke_in: i32,
    /// Leases that ended (expired or revoked) whose keys the store has
    /// yet to delete, a ring served one `KV_OP_LEASE_REVOKE` at a time;
    /// `lease_end_corr` is the one in flight (0 = none).
    lease_end: [u64; LEASE_END_QUEUE],
    lease_end_head: usize,
    lease_end_len: usize,
    lease_end_corr: u64,
    /// The head ended lease's step: 0 its keys, 1 its record.
    lease_end_phase: u8,
    /// The session layer for watch sessions.
    wsa: SessionAnchor<WATCH_SESSIONS>,
    /// The session layer for lease sessions: identity, worker binding,
    /// and the handoff relay that moves a lease (and its live keepalive
    /// stream) between managers without expiring it.
    lsa: SessionAnchor<LEASE_SESSIONS>,

    server_conn_id: u16,
    _pad1: [u8; 3],

    corr_seq: u64,
    lease_seq: u64,
    watch_seq: u64,
    /// The newest store revision the anchor has seen (KV replies, watch
    /// frames): what a watch response's header and a progress response
    /// report.
    last_rev: i64,
    /// The applied position the newest KV reply's fence tail proved: what
    /// `Status` reports as the raft index, and every header as the term.
    last_index: u64,
    last_term: u64,
    /// Active alarms, `(member, alarm)`; NOSPACE refuses writes as etcd's
    /// does.
    alarms: [(u64, u64); ALARMS_MAX],
    alarm_n: usize,
    snap: SnapState,
    /// etcd's users, roles and whether auth is on: the auth keyspace's
    /// records, read at start and kept current by the changes committed.
    auth: AuthCache,
    auth_load: RecordLoad,
    /// The lease records, read at start and re-granted.
    lease_load: RecordLoad,
    auth_job: AuthJob,
    tokens: [Token; TOKENS_MAX],
    token_seq: u32,

    // Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=requests, 1=forwarded, 2=net_errors).
    m_requests: u64,
    m_forwarded: u64,
    m_net_errors: u64,
    step_ctr: u64,

    slots: [Slot; MAX_CONNS],
    scratch: [u8; SCRATCH_BUF_SIZE],
    /// One KV reply as the router forwards it: a worker answer up to its
    /// full 8192 bytes, re-headed.
    reply: [u8; REPLY_BUF_SIZE],
    hpack_scratch: [u8; HPACK_SCRATCH_LEN],
    /// Scratch arena for the currently-parsed HTTP/2 frame body.
    /// Lives in the kernel-allocated state rather than on the stack
    /// to keep `module_step`'s stack frame tiny (the runtime stack
    /// on the linux platform is generous but bare-metal targets
    /// budget tens of KiB total per module call).
    frame_payload: [u8; RECV_BUF_SIZE],
    /// The store's answer being encoded (a copy out of `reply`).
    response_data: [u8; REPLY_BUF_SIZE],
    /// One encoded gRPC response message (`[0][len:4]` + protobuf).
    resp_out: [u8; RESP_OUT_MAX],
    /// Requests through the large-value layer, and one envelope for
    /// their exchanges; streams waiting for a session (a hint).
    lg: [LgSlot; LG_SESSIONS],
    lg_env: [u8; 3 + wire::KV_REQUEST_PAYLOAD_MAX],
    lg_waiting: usize,
    wlg: WatchLg,
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
        self.watch_ctrl2_out = -1;
        self.lease_ctrl_out = -1;
        self.lease_ctrl2_out = -1;
        self.metrics_out = -1;

        self.listen_port = DEFAULT_LISTEN_PORT;
        self.phase = AnchorPhase::Init;
        self._pad0 = 0;
        self.anchor_id = [0; 8];
        self.lease_sess0_in = -1;
        self.lease_sess1_in = -1;
        self.watch_sess0_in = -1;
        self.watch_sess1_in = -1;
        self.relocate_in = -1;
        self.lease_revoke_in = -1;
        self.lease_end = [0; LEASE_END_QUEUE];
        self.lease_end_head = 0;
        self.lease_end_len = 0;
        self.lease_end_corr = 0;
        self.server_conn_id = 0;
        self._pad1 = [0; 3];
        self.corr_seq = 0;
        self.lease_seq = 0;
        self.watch_seq = 0;
        self.last_rev = 0;
        self.last_index = 0;
        self.last_term = 0;
        self.alarms = [(0, 0); ALARMS_MAX];
        self.alarm_n = 0;
        self.snap.active = false;
        self.snap.waiting = false;
        self.auth.init();
        self.auth_load.state = 0;
        self.auth_load.corr = 0;
        self.auth_load.retry_ms = 0;
        self.auth_load.failed = false;
        self.lease_load.state = 0;
        self.lease_load.corr = 0;
        self.lease_load.retry_ms = 0;
        self.lease_load.failed = false;
        self.lease_end_phase = 0;
        self.auth_job.active = false;
        self.auth_job.corr = 0;
        for t in self.tokens.iter_mut() {
            *t = Token::FREE;
        }
        self.token_seq = 0;
        self.m_requests = 0;
        self.m_forwarded = 0;
        self.m_net_errors = 0;
        self.step_ctr = 0;

        let mut i = 0;
        while i < MAX_CONNS {
            self.slots[i] = Slot::free();
            i += 1;
        }
        for l in self.lg.iter_mut() {
            l.corr = 0;
            l.session.reset();
        }
        self.lg_waiting = 0;
        self.wlg.corr = 0;
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
    fn find_watch_stream(&self, app: i64) -> Option<(usize, usize, i64)> {
        for (si, slot) in self.slots.iter().enumerate() {
            if slot.conn_id == SLOT_FREE {
                continue;
            }
            if let Some((strm, client)) = slot.find_stream_by_watch(app) {
                return Some((si, strm, client));
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
    /// metadata; here a monotonic counter under a fixed prefix. etcd's
    /// ID is an int64 its clients treat as positive, so the top bit
    /// stays clear.
    fn next_lease_id(&mut self) -> u64 {
        self.lease_seq = self.lease_seq.wrapping_add(1);
        if self.lease_seq == 0 {
            self.lease_seq = 1;
        }
        0x7A00_0000_0000_0000 | (self.lease_seq & 0x00FF_FFFF_FFFF_FFFF)
    }

    /// This member's etcd ID: a digest of the anchor's identity, stable
    /// across restarts of the same anchor.
    fn member_id(&self) -> u64 {
        let mut h: u64 = 0xCBF2_9CE4_8422_2325;
        for b in self.anchor_id {
            h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01B3);
        }
        h | 1
    }

    /// What every `ResponseHeader` from this anchor names.
    fn header_ids(&self) -> etcd_codec::HeaderIds {
        etcd_codec::HeaderIds {
            cluster_id: CLUSTER_ID,
            member_id: self.member_id(),
            raft_term: self.last_term.max(1),
        }
    }

    /// Whether a NOSPACE alarm is raised (writes are refused).
    fn nospace(&self) -> bool {
        self.alarms[..self.alarm_n].iter().any(|a| a.1 == 1)
    }

    /// The live session fronting lease `id`: one not already on its way
    /// out, so a revoked or expired lease reads as gone at once.
    fn live_lease(&self, id: u64) -> Option<usize> {
        let i = self.lsa.find_app(CLASS_LEASE, id)?;
        (self.lsa.sessions[i].phase != session_anchor::APHASE_DETACH_WAIT).then_some(i)
    }

    /// Queue lease `id`'s keys for deletion, once.
    fn queue_lease_end(&mut self, id: u64) {
        let queued = (0..self.lease_end_len)
            .any(|i| self.lease_end[(self.lease_end_head + i) % LEASE_END_QUEUE] == id);
        if queued || self.lease_end_len == LEASE_END_QUEUE {
            return;
        }
        self.lease_end[(self.lease_end_head + self.lease_end_len) % LEASE_END_QUEUE] = id;
        self.lease_end_len += 1;
    }
}

define_params! {
    AnchorState;

    // TCP port the anchor binds for etcd clients (etcd's default 2379).
    // Overridable so several anchors — or several graphs on one host —
    // do not collide.
    1, listen_port, u16, 2379
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 2379); };
}

// ── SessionCtrlV1 session layer ───────────────────────────────────────
//
// Every watch stream and every lease is a session. Its identity is
// deterministic — `session_id = [anchor_id:8][app_id:8 BE]`, where
// `app_id` is the watch id or the lease id — so any stream can address
// the session without per-stream bookkeeping. The anchor mints at
// `FIRST_EPOCH` and does not rebind on its own; a worker move is driven
// by a relocation command on `relocate_in`. Every session-scoped data
// envelope opens with `[session_id:16][epoch:4 LE]`, and the worker
// admits it only at the session's current epoch.

/// Write `[msg][len][session_id:16][epoch:4]…]` framing prefix for a
/// session identified by `app_id` into `anchor.scratch`, returning the
/// offset just past the session header (envelope header + session
/// header). The caller appends the body and calls `sc_finish`.
fn sc_begin(anchor: &mut AnchorState, msg: u8, app_id: u64) -> usize {
    let sid = mint_session_id(&anchor.anchor_id, app_id);
    anchor.scratch[0] = msg;
    // len patched by sc_finish
    let hdr = sc::put_session_header(&mut anchor.scratch[3..], &sid, FIRST_EPOCH);
    3 + hdr
}

/// Patch the length and write the envelope to `chan`. `end` is the
/// total offset in `anchor.scratch` (including the 3-byte envelope
/// header). Returns whether the channel took it.
fn sc_finish(anchor: &mut AnchorState, chan: i32, end: usize) -> bool {
    let body_len = end - 3;
    anchor.scratch[1] = (body_len & 0xFF) as u8;
    anchor.scratch[2] = ((body_len >> 8) & 0xFF) as u8;
    let sys = anchor.syscalls;
    if sys.is_null() || chan < 0 || end > anchor.scratch.len() {
        return false;
    }
    unsafe { ((*sys).channel_write)(chan, anchor.scratch.as_mut_ptr(), end) == end as i32 }
}

/// Send `CMD_SC_ATTACH` for `app_id` on `chan`. Idempotent at the
/// worker: a re-attach at the same epoch is accepted without reset.
fn sc_attach(anchor: &mut AnchorState, chan: i32, app_id: u64) {
    let sid = mint_session_id(&anchor.anchor_id, app_id);
    let aid = anchor.anchor_id;
    let mut body = [0u8; sc::ATTACH_PAYLOAD_LEN];
    let n = sc::put_attach(
        &mut body,
        &sid,
        &aid,
        FIRST_EPOCH,
        sc::CC_EDGE_ANCHORED,
        &NO_WORKER,
    );
    anchor.scratch[0] = sc::CMD_SC_ATTACH;
    anchor.scratch[1] = (n & 0xFF) as u8;
    anchor.scratch[2] = ((n >> 8) & 0xFF) as u8;
    anchor.scratch[3..3 + n].copy_from_slice(&body[..n]);
    let _ = sc_finish(anchor, chan, 3 + n);
}

/// Send `CMD_SC_DETACH` for `app_id` on `chan`.
fn sc_detach(anchor: &mut AnchorState, chan: i32, app_id: u64, reason: u8) {
    let end = sc_begin(anchor, sc::CMD_SC_DETACH, app_id);
    anchor.scratch[end] = reason;
    let _ = sc_finish(anchor, chan, end + 1);
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
    } else if path == b"/etcdserverpb.KV/Compact" {
        GrpcMethod::Compact
    } else if path == b"/etcdserverpb.Maintenance/Status" {
        GrpcMethod::Status
    } else if path == b"/etcdserverpb.Maintenance/Alarm" {
        GrpcMethod::Alarm
    } else if path == b"/etcdserverpb.Maintenance/Defragment" {
        GrpcMethod::Defragment
    } else if path == b"/etcdserverpb.Maintenance/Hash" {
        GrpcMethod::Hash
    } else if path == b"/etcdserverpb.Maintenance/HashKV" {
        GrpcMethod::HashKv
    } else if path == b"/etcdserverpb.Maintenance/MoveLeader" {
        GrpcMethod::MoveLeader
    } else if path == b"/etcdserverpb.Cluster/MemberList" {
        GrpcMethod::MemberList
    } else if path == b"/etcdserverpb.Cluster/MemberAdd" {
        GrpcMethod::MemberAdd
    } else if path == b"/etcdserverpb.Cluster/MemberRemove" {
        GrpcMethod::MemberRemove
    } else if path == b"/etcdserverpb.Cluster/MemberUpdate" {
        GrpcMethod::MemberUpdate
    } else if path == b"/etcdserverpb.Cluster/MemberPromote" {
        GrpcMethod::MemberPromote
    } else if path == b"/etcdserverpb.Maintenance/Snapshot" {
        GrpcMethod::Snapshot
    } else if path == b"/etcdserverpb.Maintenance/Downgrade" {
        GrpcMethod::Downgrade
    } else if let Some(m) = path.strip_prefix(b"/etcdserverpb.Auth/") {
        resolve_auth_method(m)
    } else if path == b"/etcdserverpb.KV/Txn" {
        GrpcMethod::Txn
    } else if path == b"/etcdserverpb.Lease/LeaseGrant" {
        GrpcMethod::LeaseGrant
    } else if path == b"/etcdserverpb.Lease/LeaseRevoke" {
        GrpcMethod::LeaseRevoke
    } else if path == b"/etcdserverpb.Watch/Watch" {
        GrpcMethod::Watch
    } else if path == b"/etcdserverpb.Lease/LeaseKeepAlive" {
        GrpcMethod::LeaseKeepAlive
    } else if path == b"/etcdserverpb.Lease/LeaseTimeToLive" {
        GrpcMethod::LeaseTimeToLive
    } else if path == b"/etcdserverpb.Lease/LeaseLeases" {
        GrpcMethod::LeaseLeases
    } else {
        GrpcMethod::Unknown
    }
}

/// An `/etcdserverpb.Auth/<m>` method.
fn resolve_auth_method(m: &[u8]) -> GrpcMethod {
    if m == b"AuthEnable" {
        GrpcMethod::AuthEnable
    } else if m == b"AuthDisable" {
        GrpcMethod::AuthDisable
    } else if m == b"AuthStatus" {
        GrpcMethod::AuthStatus
    } else if m == b"Authenticate" {
        GrpcMethod::Authenticate
    } else if m == b"UserAdd" {
        GrpcMethod::UserAdd
    } else if m == b"UserGet" {
        GrpcMethod::UserGet
    } else if m == b"UserList" {
        GrpcMethod::UserList
    } else if m == b"UserDelete" {
        GrpcMethod::UserDelete
    } else if m == b"UserChangePassword" {
        GrpcMethod::UserChangePassword
    } else if m == b"UserGrantRole" {
        GrpcMethod::UserGrantRole
    } else if m == b"UserRevokeRole" {
        GrpcMethod::UserRevokeRole
    } else if m == b"RoleAdd" {
        GrpcMethod::RoleAdd
    } else if m == b"RoleGet" {
        GrpcMethod::RoleGet
    } else if m == b"RoleList" {
        GrpcMethod::RoleList
    } else if m == b"RoleDelete" {
        GrpcMethod::RoleDelete
    } else if m == b"RoleGrantPermission" {
        GrpcMethod::RoleGrantPermission
    } else if m == b"RoleRevokePermission" {
        GrpcMethod::RoleRevokePermission
    } else {
        GrpcMethod::Unknown
    }
}

/// Decode the request HEADERS block out of `block_bytes` and pull
/// out `:path`. Returns the parsed method, or Unknown on any error
/// (HPACK parse failure, missing `:path`, unsupported path).
/// The method, and the `token` (or `authorization`) metadata etcd's
/// clients authenticate with (copied into `token`; its length, or `TOKEN_BAD` when it cannot
/// be one of ours).
fn parse_request_headers(
    block_bytes: &[u8],
    hpack_scratch: &mut [u8],
    token: &mut [u8; TOKEN_MAX],
) -> Option<(GrpcMethod, u8)> {
    let mut decoder = HpackDecoder::new();
    let mut method = GrpcMethod::Unknown;
    let mut path_buf = [0u8; 64];
    let mut path_len: usize = 0;
    let mut path_seen = false;
    let mut token_len = 0u8;
    decoder
        .decode_block(block_bytes, hpack_scratch, |name, value| {
            if name == b":path" && !path_seen {
                let n = value.len().min(path_buf.len());
                path_buf[..n].copy_from_slice(&value[..n]);
                path_len = n;
                path_seen = true;
            } else if name == b"token" || name == b"authorization" {
                // etcd reads either: `token` (its gRPC clients),
                // `authorization` (its gateway, and etcd-client).
                if value.is_empty() || value.len() > TOKEN_MAX {
                    token_len = TOKEN_BAD;
                } else {
                    token[..value.len()].copy_from_slice(value);
                    token_len = value.len() as u8;
                }
            }
        })
        .ok()?;
    if path_seen {
        method = resolve_method(&path_buf[..path_len]);
    }
    Some((method, token_len))
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
            // Every Range — one key, a range, a prefix, from-key, at a
            // revision, sorted, limited, counted — is one
            // `KV_OP_RANGE_META`: the store answers it with each key's
            // own metadata, in the asked order, with the true count.
            let req = RangeRequest::decode(proto_body)?;
            let n = etcd_codec::encode_range_meta_body(&req, out)?;
            Some((KV_OP_RANGE_META, n))
        }
        GrpcMethod::Put => {
            let req = PutRequest::decode(proto_body)?;
            let n = etcd_codec::encode_put_body(&req, &PUT_BITS, out)?;
            Some((KV_OP_PUT, n))
        }
        GrpcMethod::Txn => {
            let n = etcd_codec::encode_txn_body(proto_body, &PUT_BITS, out, 0)?;
            Some((KV_OP_ETCD_TXN, n))
        }
        GrpcMethod::Compact => {
            let rev = etcd_codec::decode_compaction_revision(proto_body)?;
            out.get_mut(..8)?
                .copy_from_slice(&(rev.max(0) as u64).to_le_bytes());
            Some((KV_OP_COMPACT, 8))
        }
        // Status and Hash digest the current state; HashKV names its
        // revision.
        GrpcMethod::Status | GrpcMethod::Hash | GrpcMethod::HashKv => {
            let rev = if method == GrpcMethod::HashKv {
                etcd_codec::decode_field1_varint(proto_body)?
            } else {
                0
            };
            out.get_mut(..8)?.copy_from_slice(&rev.to_le_bytes());
            Some((KV_OP_HASH_KV, 8))
        }
        GrpcMethod::DeleteRange => {
            // One key or a span (prefix, from-key, range), deleted
            // atomically at one revision by `KV_OP_DELETE_META`.
            let req = DeleteRangeRequest::decode(proto_body)?;
            let n = etcd_codec::encode_delete_meta_body(&req, out)?;
            Some((KV_OP_DELETE_META, n))
        }
        GrpcMethod::Unknown
        | GrpcMethod::LeaseGrant
        | GrpcMethod::LeaseRevoke
        | GrpcMethod::LeaseKeepAlive
        | GrpcMethod::LeaseTimeToLive
        | GrpcMethod::LeaseLeases
        | GrpcMethod::Alarm
        | GrpcMethod::Defragment
        | GrpcMethod::MoveLeader
        | GrpcMethod::MemberList
        | GrpcMethod::MemberAdd
        | GrpcMethod::MemberRemove
        | GrpcMethod::MemberUpdate
        | GrpcMethod::MemberPromote
        | GrpcMethod::Snapshot
        | GrpcMethod::Downgrade
        | GrpcMethod::Watch => None,
        _ => None,
    }
}

/// Variant of `build_grpc_response_body` that knows the request key,
/// so Range responses can echo it back in the KeyValue. For Delete,
/// the kv_body shape is the KV_RESULT_INTEGER 8-byte LE i64 holding
/// the deleted count.
#[allow(
    clippy::too_many_arguments,
    reason = "one response: its method and result, what the store answered, and who answers"
)]
fn build_grpc_response_with_key(
    method: GrpcMethod,
    result: u8,
    revision: i64,
    key: &[u8],
    kv_body: &[u8],
    ctx: &RespCtx,
    out: &mut [u8],
) -> Option<usize> {
    if out.len() < GRPC_FRAME_HEADER_LEN {
        return None;
    }
    let mut proto_off = GRPC_FRAME_HEADER_LEN;
    match method {
        GrpcMethod::Range => {
            if result == KV_RESULT_KVS {
                etcd_codec::build_range_meta_response(out, &mut proto_off, revision, kv_body)?;
            } else if result == KV_RESULT_RANGE {
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
            etcd_codec::build_put_response_prev(out, &mut proto_off, revision, kv_body)?;
        }
        GrpcMethod::DeleteRange if result == KV_RESULT_KVS => {
            etcd_codec::build_delete_range_meta_response(out, &mut proto_off, revision, kv_body)?;
        }
        GrpcMethod::Txn => {
            etcd_codec::build_txn_response(out, &mut proto_off, revision, kv_body)?;
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
        // Both answer a bare ResponseHeader.
        GrpcMethod::LeaseRevoke | GrpcMethod::Compact => {
            etcd_codec::build_lease_revoke_response(out, &mut proto_off, revision)?;
        }
        GrpcMethod::LeaseTimeToLive => {
            // `key` holds [id:8][ttl_s:8][granted_s:8], then the lease's
            // keys when they were asked for (`[n:u16]{[len:u16][key]}`).
            let rd = |at: usize| -> Option<i64> {
                Some(i64::from_le_bytes(key.get(at..at + 8)?.try_into().ok()?))
            };
            etcd_codec::build_lease_ttl_response(
                out,
                &mut proto_off,
                revision,
                rd(0)?,
                rd(8)?,
                rd(16)?,
                key.get(24..).unwrap_or(&[]),
            )?;
        }
        GrpcMethod::LeaseLeases => {
            // `key` holds the live lease ids, 8 bytes LE each.
            let mut ids = [0u64; LEASE_SESSIONS];
            let n = (key.len() / 8).min(LEASE_SESSIONS);
            for (i, c) in key.chunks_exact(8).take(n).enumerate() {
                ids[i] = u64::from_le_bytes(c.try_into().ok()?);
            }
            etcd_codec::build_lease_leases_response(out, &mut proto_off, revision, &ids[..n])?;
        }
        GrpcMethod::Unknown
        | GrpcMethod::LeaseGrant
        | GrpcMethod::LeaseKeepAlive
        | GrpcMethod::Snapshot
        | GrpcMethod::Downgrade
        | GrpcMethod::Watch => return None,
        // `key` holds `[enabled:u8][auth_revision:u64 LE]`.
        GrpcMethod::AuthStatus => {
            etcd_codec::build_auth_status_response(
                out,
                &mut proto_off,
                revision,
                key.first() == Some(&1),
                rd_le64(key, 1),
            )?;
        }
        // `key` holds the strings the response lists, `[len:u8][s]` each.
        GrpcMethod::Authenticate
        | GrpcMethod::UserGet
        | GrpcMethod::UserList
        | GrpcMethod::RoleList => {
            etcd_codec::build_strings_response(out, &mut proto_off, revision, key)?;
        }
        // `key` holds the role's permissions in its record's form.
        GrpcMethod::RoleGet => {
            etcd_codec::build_role_get_response(out, &mut proto_off, revision, key)?;
        }
        GrpcMethod::AuthEnable
        | GrpcMethod::AuthDisable
        | GrpcMethod::UserAdd
        | GrpcMethod::UserDelete
        | GrpcMethod::UserChangePassword
        | GrpcMethod::UserGrantRole
        | GrpcMethod::UserRevokeRole
        | GrpcMethod::RoleAdd
        | GrpcMethod::RoleDelete
        | GrpcMethod::RoleGrantPermission
        | GrpcMethod::RoleRevokePermission => {
            etcd_codec::build_lease_revoke_response(out, &mut proto_off, revision)?;
        }
        GrpcMethod::Status => {
            let bytes = rd_le64(kv_body, 20);
            let st = etcd_codec::MemberStatus {
                db_size: bytes,
                db_size_in_use: bytes,
                leader: ctx.ids.member_id,
                raft_index: ctx.raft_index,
                raft_term: ctx.ids.raft_term,
                raft_applied_index: ctx.raft_index,
            };
            etcd_codec::build_status_response(out, &mut proto_off, revision, ETCD_VERSION, &st)?;
        }
        GrpcMethod::Hash | GrpcMethod::HashKv => {
            let hash = kv_body
                .get(..4)
                .map_or(0, |b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]));
            let (compact, at) = if method == GrpcMethod::HashKv {
                (rd_le64(kv_body, 4), rd_le64(kv_body, 12))
            } else {
                (0, 0)
            };
            etcd_codec::build_hash_response(out, &mut proto_off, revision, hash, compact, at)?;
        }
        GrpcMethod::Alarm => {
            // `key` holds the alarms to list, `(member, alarm)` 16 bytes each.
            let mut alarms = [(0u64, 0u64); ALARMS_MAX];
            let n = (key.len() / 16).min(ALARMS_MAX);
            for (i, a) in alarms.iter_mut().enumerate().take(n) {
                *a = (rd_le64(key, i * 16), rd_le64(key, i * 16 + 8));
            }
            etcd_codec::build_alarm_response(out, &mut proto_off, revision, &alarms[..n])?;
        }
        GrpcMethod::MemberList => {
            etcd_codec::build_member_list_response(
                out,
                &mut proto_off,
                revision,
                ctx.ids.member_id,
                b"default",
                &[],
                &ctx.client_url[..ctx.client_len],
            )?;
        }
        // A bare ResponseHeader.
        GrpcMethod::Defragment
        | GrpcMethod::MoveLeader
        | GrpcMethod::MemberAdd
        | GrpcMethod::MemberRemove
        | GrpcMethod::MemberUpdate
        | GrpcMethod::MemberPromote => {
            etcd_codec::build_lease_revoke_response(out, &mut proto_off, revision)?;
        }
    }
    // Who answers: the builders wrote a placeholder identity.
    let stamped = etcd_codec::stamp_header(
        out.get_mut(GRPC_FRAME_HEADER_LEN..)?,
        proto_off - GRPC_FRAME_HEADER_LEN,
        &ctx.ids,
    )?;
    proto_off = GRPC_FRAME_HEADER_LEN + stamped;
    let proto_len = proto_off - GRPC_FRAME_HEADER_LEN;
    let grpc_hdr = GrpcMessageHeader {
        compressed: 0,
        length: proto_len as u32,
    };
    grpc_hdr.encode(&mut out[..GRPC_FRAME_HEADER_LEN])?;
    Some(proto_off)
}

/// What a response names beyond the store's answer: who answers, and
/// the member facts `Status` and `MemberList` report.
struct RespCtx {
    ids: etcd_codec::HeaderIds,
    raft_index: u64,
    client_url: [u8; 32],
    client_len: usize,
}

impl AnchorState {
    fn resp_ctx(&self) -> RespCtx {
        let mut client_url = [0u8; 32];
        let n = put_msg(&mut client_url, b"http://127.0.0.1:");
        let port = fmt_dec(&mut client_url[n..], u64::from(self.listen_port));
        RespCtx {
            ids: self.header_ids(),
            raft_index: self.last_index,
            client_url,
            client_len: n + port,
        }
    }
}

/// `v` in decimal into `out`; the digits written.
fn fmt_dec(out: &mut [u8], mut v: u64) -> usize {
    let mut d = [0u8; 20];
    let mut n = 0usize;
    loop {
        d[n] = b'0' + (v % 10) as u8;
        n += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    let n = n.min(out.len());
    for i in 0..n {
        out[i] = d[n - 1 - i];
    }
    n
}

/// The little-endian u64 at `at` in `b` (0 past its end).
fn rd_le64(b: &[u8], at: usize) -> u64 {
    b.get(at..at + 8)
        .and_then(|x| x.try_into().ok())
        .map_or(0, u64::from_le_bytes)
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
            if let Some(strm) = anchor.slots[slot_idx].find_stream(stream_id) {
                drop_stream_watches(anchor, slot_idx, strm);
                anchor.slots[slot_idx].free_stream(strm);
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
    let mut token = [0u8; TOKEN_MAX];
    let (method, token_len) = {
        let hpack = &mut anchor.hpack_scratch[..];
        let block = &anchor.frame_payload[start..end];
        parse_request_headers(block, hpack, &mut token).unwrap_or((GrpcMethod::Unknown, 0))
    };

    {
        let stream = &mut anchor.slots[slot_idx].streams[strm];
        stream.method = method;
        stream.token = token;
        stream.token_len = token_len;
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
            // etcd's ErrRequestTooLarge; the stream's further DATA finds
            // no stream and is dropped.
            let mut m = [0u8; 96];
            let n = msg_request_too_large(&mut m);
            send_grpc_error_msg(anchor, slot_idx, stream_id, 3, &m[..n]);
            anchor.slots[slot_idx].free_stream(strm);
            let slot = &mut anchor.slots[slot_idx];
            let _ = send_window_update(slot, 0, body_len_now as u32);
            return FrameAction::Done;
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
    // With auth on, the request's user must be allowed it.
    if let Some((status, m, n)) = authorize(anchor, slot_idx, strm) {
        send_grpc_error_msg(anchor, slot_idx, stream_id, status, &m[..n]);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    if method.is_auth() {
        answer_auth(anchor, slot_idx, strm);
        return;
    }

    // Lease ops route to lease_manager via lease_ctrl_out, not the
    // KV worker. The response correlates by lease_id, not corr_id.
    if matches!(
        method,
        GrpcMethod::LeaseGrant | GrpcMethod::LeaseRevoke | GrpcMethod::LeaseTimeToLive
    ) {
        dispatch_lease(anchor, slot_idx, strm);
        return;
    }
    if method == GrpcMethod::LeaseLeases {
        answer_lease_leases(anchor, slot_idx, strm);
        return;
    }
    if method == GrpcMethod::Snapshot {
        start_snapshot(anchor, slot_idx, strm);
        return;
    }
    if matches!(
        method,
        GrpcMethod::Alarm
            | GrpcMethod::Downgrade
            | GrpcMethod::Defragment
            | GrpcMethod::MoveLeader
            | GrpcMethod::MemberList
            | GrpcMethod::MemberAdd
            | GrpcMethod::MemberRemove
            | GrpcMethod::MemberUpdate
            | GrpcMethod::MemberPromote
    ) {
        answer_member(anchor, slot_idx, strm);
        return;
    }
    // A raised NOSPACE alarm refuses writes, as etcd's quota does.
    if anchor.nospace()
        && matches!(
            method,
            GrpcMethod::Put | GrpcMethod::Txn | GrpcMethod::LeaseGrant
        )
    {
        let mut m = [0u8; 96];
        let n = msg_no_space(&mut m);
        send_grpc_error_msg(anchor, slot_idx, stream_id, 8, &m[..n]);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    // A Put naming a lease needs the lease to exist: etcd's
    // ErrLeaseNotFound.
    if method == GrpcMethod::Put {
        let lease = {
            let b = &anchor.slots[slot_idx].streams[strm].body_buf
                [..anchor.slots[slot_idx].streams[strm].body_len];
            grpc_proto(b).and_then(PutRequest::decode).map_or(0, |r| {
                if r.ignore_lease {
                    0
                } else {
                    r.lease
                }
            })
        };
        if lease != 0 && anchor.live_lease(lease as u64).is_none() {
            send_grpc_error_msg(anchor, slot_idx, stream_id, 5, LEASE_NOT_FOUND);
            anchor.slots[slot_idx].free_stream(strm);
            return;
        }
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

    dispatch_kv(anchor, slot_idx, strm);
}

/// Send a stream's KV request (its body in `body_buf`): directly, or
/// through the large-value layer when it carries a large value, is a Txn
/// that reads, or is a direct request the store answered "a large value".
fn dispatch_kv(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let method = anchor.slots[slot_idx].streams[strm].method;
    let body_len = anchor.slots[slot_idx].streams[strm].body_len;
    anchor.slots[slot_idx].streams[strm].kept_len = body_len;
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
    let consistency = read_consistency(anchor, slot_idx, strm, method);
    let via_layer = {
        let body = &anchor.scratch[op_body_start..op_body_start + op_body_len];
        anchor.slots[slot_idx].streams[strm].lg_retry
            || kv_large::carries_large(op, body)
            || (op == types::KV_OP_ETCD_TXN && kv_large::etcd_txn_reads(body))
    };
    if via_layer {
        lg_begin(anchor, slot_idx, strm, op, op_body_len, consistency);
        return;
    }

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
    anchor.scratch[p] = consistency;
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
    if method == GrpcMethod::Range && (op == KV_OP_GET || op == KV_OP_GET_AT) {
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
        // The key stash overwrote the request: no retry from it.
        anchor.slots[slot_idx].streams[strm].kept_len = 0;
    } else {
        anchor.slots[slot_idx].streams[strm].body_len = 0;
    }
}

/// The read's consistency, as etcd asks it: a Range is linearizable
/// unless the client set `serializable` (the router takes the ReadIndex
/// fence for it where the graph wires one). Writes are committed through
/// consensus whatever this says.
fn read_consistency(anchor: &AnchorState, slot_idx: usize, strm: usize, method: GrpcMethod) -> u8 {
    let serializable = method == GrpcMethod::Range && {
        let s = &anchor.slots[slot_idx].streams[strm];
        grpc_proto(&s.body_buf[..s.body_len])
            .and_then(RangeRequest::decode)
            .is_some_and(|r| r.serializable)
    };
    if serializable {
        types::REQ_SERIALIZABLE
    } else {
        types::REQ_LINEARIZABLE
    }
}

// ── Large values ──────────────────────────────────────────────────────

/// Carry a stream's request (its op body at `scratch[21..]`) through a
/// large-value session; with none free, the request waits in its stream.
fn lg_begin(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    op: u8,
    len: usize,
    consistency: u8,
) {
    let Some(i) = anchor.lg.iter().position(|l| l.corr == 0) else {
        let s = &mut anchor.slots[slot_idx].streams[strm];
        s.body_len = s.kept_len;
        s.deferred = true;
        anchor.lg_waiting += 1;
        return;
    };
    const OP_BODY_AT: usize = 3 + 18;
    let now = unsafe { dev_millis(&*anchor.syscalls) };
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let act = {
        let AnchorState { lg, scratch, .. } = &mut *anchor;
        let Some(l) = lg.get_mut(i) else {
            return;
        };
        let Some(body) = scratch.get(OP_BODY_AT..OP_BODY_AT + len) else {
            return;
        };
        if len > l.req.len() {
            None
        } else {
            l.req[..len].copy_from_slice(body);
            l.req_len = len;
            l.slot_idx = slot_idx;
            l.strm = strm;
            l.stream_id = stream_id;
            l.consistency = consistency;
            l.revision = 0;
            l.since_ms = now;
            let LgSlot {
                session,
                req,
                work,
                send,
                ..
            } = l;
            Some(session.begin(op, &req[..len], work, send))
        }
    };
    // The request is on its way (or answered): the stream no longer needs
    // its body.
    anchor.slots[slot_idx].streams[strm].body_len = 0;
    anchor.slots[slot_idx].streams[strm].lg_retry = false;
    match act {
        Some(act) => lg_act(anchor, i, act, None),
        None => lg_finish(anchor, i, types::KV_RESULT_TOO_LARGE, 0, 0),
    }
}

/// Carry out what a session asks: send its next exchange, or answer.
/// `as_is`: where the reply that ended it sits in `reply` (`(off, len,
/// result, revision)`).
fn lg_act(
    anchor: &mut AnchorState,
    i: usize,
    act: Result<kv_large::Act, kv_large::LargeError>,
    as_is: Option<(usize, usize, u8, i64)>,
) {
    match act {
        Ok(kv_large::Act::Send(op, n)) => lg_send(anchor, i, op, n),
        Ok(kv_large::Act::Done(result, n)) => {
            let rev = anchor.lg[i].revision;
            let n = n.min(REPLY_BUF_SIZE);
            let AnchorState {
                lg, response_data, ..
            } = &mut *anchor;
            response_data[..n].copy_from_slice(&lg[i].out[..n]);
            lg_finish(anchor, i, result, rev, n);
        }
        Ok(kv_large::Act::AsIs) => {
            let Some((off, len, result, rev)) = as_is else {
                lg_finish(anchor, i, types::KV_RESULT_INTERNAL, 0, 0);
                return;
            };
            let len = len.min(REPLY_BUF_SIZE);
            let AnchorState {
                reply,
                response_data,
                ..
            } = &mut *anchor;
            let Some(body) = reply.get(off..off + len) else {
                lg_finish(anchor, i, types::KV_RESULT_INTERNAL, 0, 0);
                return;
            };
            response_data[..len].copy_from_slice(body);
            lg_finish(anchor, i, result, rev, len);
        }
        Err(kv_large::LargeError::Overflow) => {
            lg_finish(anchor, i, types::KV_RESULT_TOO_LARGE, 0, 0)
        }
        Err(_) => lg_finish(anchor, i, types::KV_RESULT_INTERNAL, 0, 0),
    }
}

/// Send session `i`'s next exchange; with no room on `kv_out` it waits
/// ([`lg_step`] sends it again).
fn lg_send(anchor: &mut AnchorState, i: usize, op: u8, n: usize) {
    let corr = anchor.next_corr();
    let (slot_idx, strm) = (anchor.lg[i].slot_idx, anchor.lg[i].strm);
    if lg_emit(anchor, i, corr, op, n) {
        anchor.lg[i].corr = corr;
        anchor.slots[slot_idx].streams[strm].pending_corr = corr;
    } else {
        anchor.lg[i].corr = LG_UNSENT;
        anchor.lg[i].unsent_op = op;
        anchor.lg[i].unsent_len = n;
        anchor.slots[slot_idx].streams[strm].pending_corr = 0;
    }
}

/// One exchange of session `i`: `op` with the body in its `send`.
fn lg_emit(anchor: &mut AnchorState, i: usize, corr: u64, op: u8, n: usize) -> bool {
    const REQ_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    let payload = REQ_HEAD + n;
    if payload > wire::KV_REQUEST_PAYLOAD_MAX {
        return false;
    }
    let chunk = op == kv_large::UPLOAD_OP || op == kv_large::FETCH_OP;
    let (slot_byte, consistency) = if chunk {
        (LG_CHUNK_CONN, types::REQ_SERIALIZABLE)
    } else {
        (anchor.lg[i].slot_idx as u8, anchor.lg[i].consistency)
    };
    let AnchorState { lg, lg_env, .. } = &mut *anchor;
    let Some(body) = lg[i].send.get(..n) else {
        return false;
    };
    lg_env[0] = MSG_KV_REQUEST;
    lg_env[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
    lg_env[3..11].copy_from_slice(&corr.to_le_bytes());
    lg_env[11] = PROTO_ETCD;
    lg_env[12..16].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());
    lg_env[16] = slot_byte;
    lg_env[17] = consistency;
    lg_env[18] = op;
    lg_env[19..21].copy_from_slice(&(n as u16).to_le_bytes());
    lg_env[21..21 + n].copy_from_slice(body);
    let total = 3 + payload;
    let (sys, kv_out) = (anchor.syscalls, anchor.kv_out);
    if sys.is_null() || kv_out < 0 {
        return false;
    }
    unsafe { ((*sys).channel_write)(kv_out, anchor.lg_env.as_mut_ptr(), total) == total as i32 }
}

/// Answer session `i`'s stream (its answer in `response_data[..n]`) and
/// free the session. A stream gone meanwhile is answered by no one.
fn lg_finish(anchor: &mut AnchorState, i: usize, result: u8, revision: i64, n: usize) {
    let (slot_idx, strm, stream_id) = (
        anchor.lg[i].slot_idx,
        anchor.lg[i].strm,
        anchor.lg[i].stream_id,
    );
    anchor.lg[i].corr = 0;
    anchor.lg[i].session.reset();
    if !lg_stream_alive(anchor, slot_idx, strm, stream_id) {
        return;
    }
    let method = anchor.slots[slot_idx].streams[strm].method;
    write_response_frames(
        anchor, slot_idx, strm, stream_id, method, result, revision, 0, n,
    );
    anchor.slots[slot_idx].free_stream(strm);
}

fn lg_stream_alive(anchor: &AnchorState, slot_idx: usize, strm: usize, stream_id: u32) -> bool {
    anchor
        .slots
        .get(slot_idx)
        .filter(|s| s.conn_id != SLOT_FREE)
        .and_then(|s| s.streams.get(strm))
        .is_some_and(|s| s.stream_id == stream_id)
}

/// A reply to session `i`'s exchange, at `reply[off..off + len]`.
fn lg_reply(anchor: &mut AnchorState, i: usize, result: u8, revision: i64, off: usize, len: usize) {
    let (slot_idx, strm, stream_id) = (
        anchor.lg[i].slot_idx,
        anchor.lg[i].strm,
        anchor.lg[i].stream_id,
    );
    if !lg_stream_alive(anchor, slot_idx, strm, stream_id) {
        anchor.lg[i].corr = 0;
        anchor.lg[i].session.reset();
        return;
    }
    if anchor.lg[i].session.awaits_request() {
        anchor.lg[i].revision = revision;
    }
    let act = {
        let AnchorState { lg, reply, .. } = &mut *anchor;
        let l = &mut lg[i];
        let body = reply.get(off..off + len).unwrap_or(&[]);
        let LgSlot {
            session,
            req,
            req_len,
            work,
            send,
            out,
            ..
        } = l;
        session.reply(&req[..*req_len], work, result, body, send, out)
    };
    lg_act(anchor, i, act, Some((off, len, result, revision)));
}

// ── Watch events of large values ──────────────────────────────────────

/// Hold the watch frame in `reply[..len]` and fetch its manifests' values.
fn wlg_begin(anchor: &mut AnchorState, len: usize) {
    use kv_large::kv_chunks::{Manifest, Reader};
    let now = unsafe { dev_millis(&*anchor.syscalls) };
    let parsed = {
        let AnchorState { wlg, reply, .. } = &mut *anchor;
        let (Some(dst), Some(src)) = (wlg.frame.get_mut(..len), reply.get(..len)) else {
            return;
        };
        dst.copy_from_slice(src);
        wlg.frame_len = len;
        parse_watch_frame(&wlg.frame[..len])
    };
    let Some(f) = parsed else {
        return;
    };
    let frame = &anchor.wlg.frame[..len];
    let value_m = (f.op == types::KV_OP_PUT_MANIFEST)
        .then(|| frame.get(f.value.0..f.value.1).and_then(Manifest::parse));
    let prev_m = f
        .prev
        .filter(|p| p.2)
        .map(|((a, b), _, _)| frame.get(a..b).and_then(Manifest::parse));
    let (Some(value_m), Some(prev_m)) = (
        value_m.map_or(Some(None), |m| m.map(Some)),
        prev_m.map_or(Some(None), |m| m.map(Some)),
    ) else {
        wlg_cancel(
            anchor,
            f.watch_id,
            f.revision,
            b"lattice: a large value's manifest does not parse",
        );
        return;
    };
    let too_large = [value_m, prev_m]
        .iter()
        .flatten()
        .any(|m| m.total > WATCH_VALUE_MAX as u64);
    if too_large {
        wlg_cancel(
            anchor,
            f.watch_id,
            f.revision,
            b"lattice: a value larger than one watch event carries",
        );
        return;
    }
    let w = &mut anchor.wlg;
    w.manifests = [value_m, prev_m];
    w.lens = [0, 0];
    w.part = 0;
    w.reader = None;
    w.since_ms = now;
    w.corr = LG_UNSENT;
    wlg_next(anchor);
}

/// Fetch the next chunk, or — every value whole — deliver the event.
fn wlg_next(anchor: &mut AnchorState) {
    use kv_large::kv_chunks::Reader;
    loop {
        let w = &mut anchor.wlg;
        if w.part == 2 {
            wlg_deliver(anchor);
            return;
        }
        let Some(m) = w.manifests[w.part] else {
            w.part += 1;
            continue;
        };
        let r = w.reader.get_or_insert_with(|| Reader::new(m));
        if let Some(h) = r.want() {
            w.want = h;
            wlg_fetch(anchor);
            return;
        }
        let whole = r.complete() && w.lens[w.part] as u64 == m.total;
        w.reader = None;
        if !whole {
            let (id, rev) = wlg_ids(anchor);
            wlg_cancel(
                anchor,
                id,
                rev,
                b"lattice: a large value does not read back",
            );
            return;
        }
        w.part += 1;
    }
}

fn wlg_ids(anchor: &AnchorState) -> (i64, i64) {
    parse_watch_frame(&anchor.wlg.frame[..anchor.wlg.frame_len])
        .map_or((0, 0), |f| (f.watch_id, f.revision))
}

/// Ask for the chunk `wlg.want` (again, when `kv_out` had no room).
fn wlg_fetch(anchor: &mut AnchorState) {
    let corr = anchor.next_corr();
    let hash = anchor.wlg.want;
    if emit_chunk_op(anchor, corr, kv_large::FETCH_OP, &hash) {
        anchor.wlg.corr = corr;
    } else {
        anchor.wlg.corr = LG_UNSENT;
    }
}

/// A chunk the held event asked for.
fn wlg_reply(anchor: &mut AnchorState, result: u8, off: usize, len: usize) {
    let took = result == types::KV_RESULT_OK && {
        let AnchorState { wlg, reply, .. } = &mut *anchor;
        let chunk = reply.get(off..off + len).unwrap_or(&[]);
        let part = wlg.part;
        match wlg.reader.as_mut().and_then(|r| r.take(chunk)) {
            Some(kv_large::kv_chunks::Step::Data(d)) => {
                let at = wlg.lens[part];
                match wlg.values[part].get_mut(at..at + d.len()) {
                    Some(dst) => {
                        dst.copy_from_slice(d);
                        wlg.lens[part] = at + d.len();
                        true
                    }
                    None => false,
                }
            }
            Some(_) => true,
            None => false,
        }
    };
    if took {
        wlg_next(anchor);
    } else {
        let (id, rev) = wlg_ids(anchor);
        wlg_cancel(
            anchor,
            id,
            rev,
            b"lattice: a large value does not read back",
        );
    }
}

/// Deliver the held event, its values whole.
fn wlg_deliver(anchor: &mut AnchorState) {
    anchor.wlg.corr = 0;
    let Some(f) = parse_watch_frame(&anchor.wlg.frame[..anchor.wlg.frame_len]) else {
        return;
    };
    let Some((slot_idx, strm, client)) = anchor.find_watch_stream(f.watch_id) else {
        return;
    };
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let wants_prev = anchor.slots[slot_idx].streams[strm]
        .watches
        .iter()
        .any(|w| w.app == f.watch_id && w.prev_kv);
    let ids = anchor.header_ids();
    let data_len = {
        let AnchorState { resp_out, wlg, .. } = &mut *anchor;
        let frame = &wlg.frame[..wlg.frame_len];
        let value = if wlg.manifests[0].is_some() {
            &wlg.values[0][..wlg.lens[0]]
        } else {
            frame.get(f.value.0..f.value.1).unwrap_or(&[])
        };
        let prev = f.prev.filter(|_| wants_prev).map(|((a, b), m, manifest)| {
            let v = if manifest {
                &wlg.values[1][..wlg.lens[1]]
            } else {
                frame.get(a..b).unwrap_or(&[])
            };
            (v, m)
        });
        let kind = if f.op == KV_OP_DELETE {
            WatchEventKind::Delete
        } else {
            WatchEventKind::Put
        };
        encode_watch_response(
            resp_out,
            &ids,
            f.meta[1] as i64,
            client,
            false,
            false,
            Some(WatchEv {
                kind,
                key: frame.get(f.key.0..f.key.1).unwrap_or(&[]),
                value,
                meta: f.meta,
                prev,
            }),
            0,
            b"",
        )
    };
    send_encoded_watch(anchor, slot_idx, strm, stream_id, data_len);
}

/// End the held event's watch, canceled with `reason`.
fn wlg_cancel(anchor: &mut AnchorState, watch_id: i64, revision: i64, reason: &[u8]) {
    anchor.wlg.corr = 0;
    let Some((slot_idx, strm, client)) = anchor.find_watch_stream(watch_id) else {
        return;
    };
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    if let Some(i) = anchor.slots[slot_idx].streams[strm]
        .watches
        .iter()
        .position(|w| w.app == watch_id)
    {
        drop_watch(anchor, slot_idx, strm, i);
    }
    send_watch_response_full(
        anchor, slot_idx, strm, stream_id, client, false, true, None, revision, reason,
    );
}

/// One chunk exchange (`FETCH_OP` of a hash, or an upload) on `kv_out`,
/// outside every client's order (see [`LG_CHUNK_CONN`]).
fn emit_chunk_op(anchor: &mut AnchorState, corr: u64, op: u8, body: &[u8]) -> bool {
    const REQ_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    let n = body.len();
    let payload = REQ_HEAD + n;
    let total = 3 + payload;
    let env = &mut anchor.lg_env;
    if total > env.len() {
        return false;
    }
    env[0] = MSG_KV_REQUEST;
    env[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
    env[3..11].copy_from_slice(&corr.to_le_bytes());
    env[11] = PROTO_ETCD;
    env[12..16].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());
    env[16] = LG_CHUNK_CONN;
    env[17] = types::REQ_SERIALIZABLE;
    env[18] = op;
    env[19..21].copy_from_slice(&(n as u16).to_le_bytes());
    env[21..total].copy_from_slice(body);
    let (sys, kv_out) = (anchor.syscalls, anchor.kv_out);
    if sys.is_null() || kv_out < 0 {
        return false;
    }
    unsafe { ((*sys).channel_write)(kv_out, anchor.lg_env.as_mut_ptr(), total) == total as i32 }
}

/// Each step: streams waiting for a session take the ones free; an
/// exchange that never came back is abandoned.
fn lg_step(anchor: &mut AnchorState) {
    // The held watch event's fetch: sent again when `kv_out` had no
    // room; abandoned (its watch canceled) when it never came back.
    if anchor.wlg.corr != 0 {
        let now = unsafe { dev_millis(&*anchor.syscalls) };
        if now.saturating_sub(anchor.wlg.since_ms) > LG_STALL_MS {
            let (id, rev) = wlg_ids(anchor);
            wlg_cancel(anchor, id, rev, b"lattice: a large value's fetch timed out");
        } else if anchor.wlg.corr == LG_UNSENT {
            wlg_fetch(anchor);
        }
    }
    if anchor.lg.iter().any(|l| l.corr != 0) {
        let now = unsafe { dev_millis(&*anchor.syscalls) };
        for i in 0..LG_SESSIONS {
            let l = &anchor.lg[i];
            if l.corr == 0 {
                continue;
            }
            if !lg_stream_alive(anchor, l.slot_idx, l.strm, l.stream_id) {
                anchor.lg[i].corr = 0;
                anchor.lg[i].session.reset();
            } else if l.corr == LG_UNSENT {
                let (op, n) = (l.unsent_op, l.unsent_len);
                lg_send(anchor, i, op, n);
            } else if now.saturating_sub(l.since_ms) > LG_STALL_MS {
                let (si, st, id) = (l.slot_idx, l.strm, l.stream_id);
                anchor.lg[i].corr = 0;
                anchor.lg[i].session.reset();
                send_grpc_error(anchor, si, id, 14);
                anchor.slots[si].free_stream(st);
            }
        }
    }
    if anchor.lg_waiting == 0 {
        return;
    }
    let mut waiting = 0usize;
    for si in 0..MAX_CONNS {
        if anchor.slots[si].conn_id == SLOT_FREE {
            continue;
        }
        for st in 0..MAX_STREAMS_PER_CONN {
            if !anchor.slots[si].streams[st].deferred {
                continue;
            }
            if anchor.lg.iter().all(|l| l.corr != 0) {
                waiting += 1;
                continue;
            }
            anchor.slots[si].streams[st].deferred = false;
            anchor.slots[si].streams[st].lg_retry = true;
            dispatch_kv(anchor, si, st);
            if anchor.slots[si].streams[st].deferred {
                waiting += 1;
            }
        }
    }
    anchor.lg_waiting = waiting;
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
        GrpcMethod::LeaseTimeToLive => {
            let Some(req) = LeaseTimeToLiveRequest::decode(proto) else {
                send_grpc_error(anchor, slot_idx, stream_id, 3);
                anchor.slots[slot_idx].free_stream(strm);
                return;
            };
            // Whether the keys were asked for rides in the ttl word.
            (LEASE_CTRL_TIME_TO_LIVE, req.id as u64, u32::from(req.keys))
        }
        _ => return,
    };
    if ctrl != LEASE_CTRL_GRANT && anchor.live_lease(lease_id).is_none() {
        // No such lease: a revoke is etcd's ErrLeaseNotFound; a
        // TimeToLive answers TTL -1.
        if ctrl == LEASE_CTRL_REVOKE {
            send_grpc_error_msg(anchor, slot_idx, stream_id, 5, LEASE_NOT_FOUND);
            anchor.slots[slot_idx].free_stream(strm);
        } else {
            answer_lease_ttl(anchor, slot_idx, strm, lease_id, -1, 0);
        }
        return;
    }

    if anchor.lease_ctrl_out < 0 {
        send_grpc_error(anchor, slot_idx, stream_id, 14);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    // Under auth, a revoke deletes keys: a non-root user must be allowed
    // to write every one. They are checked page by page first.
    if ctrl == LEASE_CTRL_REVOKE && anchor.auth.enabled {
        let root = matches!(
            stream_user(anchor, slot_idx, strm),
            Ok(Some(u)) if anchor.auth.is_root(u)
        );
        if !root {
            let bb = &mut anchor.slots[slot_idx].streams[strm].body_buf;
            bb[0..8].copy_from_slice(&lease_id.to_le_bytes());
            bb[8] = REVOKE_CHECKING;
            request_lease_keys(anchor, slot_idx, strm, lease_id, &[]);
            return;
        }
    }
    send_lease_ctrl(anchor, slot_idx, strm, ctrl, lease_id, ttl_ms);
}

/// A page of a lease's keys a TimeToLive lists: each gathered onto the
/// stream (read permission on each checked, under auth), then the next
/// page, or the answer.
fn ttl_keys_page(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    result: u8,
    body_off: usize,
    body_len: usize,
) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    anchor.slots[slot_idx].streams[strm].pending_corr = 0;
    let lease_id = rd_le64(&anchor.slots[slot_idx].streams[strm].body_buf, 0);
    // Under auth, a non-root user must be able to read every key.
    let reader = if anchor.auth.enabled {
        match stream_user(anchor, slot_idx, strm) {
            Ok(Some(u)) if anchor.auth.is_root(u) => None,
            Ok(Some(u)) => Some(u),
            _ => {
                send_denied(anchor, slot_idx, strm);
                return;
            }
        }
    } else {
        None
    };
    if result != KV_RESULT_KVS || body_len < 11 {
        send_grpc_error(anchor, slot_idx, stream_id, 13);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    let b_n = u16::from_le_bytes([anchor.reply[body_off + 9], anchor.reply[body_off + 10]]);
    let more = anchor.reply[body_off + 8] != 0;
    let mut p = body_off + 11;
    let end = body_off + body_len;
    let mut last = (0usize, 0usize);
    for _ in 0..b_n {
        let Some(kl) = anchor
            .reply
            .get(p..p + 2)
            .map(|x| u16::from_le_bytes([x[0], x[1]]) as usize)
        else {
            break;
        };
        if p + 2 + kl > end {
            break;
        }
        let k0 = p + 2;
        if let Some(u) = reader {
            if !anchor
                .auth
                .permitted(u, &anchor.reply[k0..k0 + kl], &[], NEED_READ)
            {
                send_denied(anchor, slot_idx, strm);
                return;
            }
        }
        let s = &mut anchor.slots[slot_idx].streams[strm];
        let at = s.body_len;
        if at + 2 + kl > STREAM_BODY_MAX {
            let mut m = [0u8; 96];
            let n = msg_lease_keys_too_many(&mut m);
            send_grpc_error_msg(anchor, slot_idx, stream_id, 8, &m[..n]);
            anchor.slots[slot_idx].free_stream(strm);
            return;
        }
        s.body_buf[at..at + 2].copy_from_slice(&(kl as u16).to_le_bytes());
        s.body_buf[at + 2..at + 2 + kl].copy_from_slice(&anchor.reply[k0..k0 + kl]);
        s.body_len = at + 2 + kl;
        let count = u16::from_le_bytes([s.body_buf[24], s.body_buf[25]]) + 1;
        s.body_buf[24..26].copy_from_slice(&count.to_le_bytes());
        last = (k0, kl);
        // Key-only entries: the value length (0) and the metadata follow.
        p = k0 + kl + 4 + 32;
    }
    if more && b_n > 0 {
        let mut after = [0u8; STORE_KEY_MAX];
        let (at, kl) = last;
        if kl > after.len() {
            return;
        }
        after[..kl].copy_from_slice(&anchor.reply[at..at + kl]);
        request_lease_keys(anchor, slot_idx, strm, lease_id, &after[..kl]);
        return;
    }
    let rev = anchor.last_rev.max(1);
    let len = anchor.slots[slot_idx].streams[strm].body_len;
    write_response_frames(
        anchor,
        slot_idx,
        strm,
        stream_id,
        GrpcMethod::LeaseTimeToLive,
        KV_RESULT_OK,
        rev,
        len,
        0,
    );
    anchor.slots[slot_idx].free_stream(strm);
}

/// Refuse the stream's request: permission denied.
fn send_denied(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let mut m = [0u8; 96];
    let n = msg_permission_denied(&mut m);
    send_grpc_error_msg(anchor, slot_idx, stream_id, 7, &m[..n]);
    anchor.slots[slot_idx].free_stream(strm);
}

/// A grant's record committed (or did not): the grant is answered, or
/// the lease is revoked again and the client told to retry.
fn grant_recorded(anchor: &mut AnchorState, slot_idx: usize, strm: usize, result: u8) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    anchor.slots[slot_idx].streams[strm].pending_corr = 0;
    let (id, ttl_ms) = {
        let bb = &anchor.slots[slot_idx].streams[strm].body_buf;
        (
            rd_le64(bb, 16),
            u32::from_le_bytes([bb[24], bb[25], bb[26], bb[27]]),
        )
    };
    if result == KV_RESULT_OK {
        write_lease_response_frames(
            anchor,
            slot_idx,
            strm,
            stream_id,
            GrpcMethod::LeaseGrant,
            0,
            id,
            ttl_ms,
        );
    } else {
        lease_session_ctrl(anchor, LEASE_CTRL_REVOKE, id, 0);
        send_grpc_error(anchor, slot_idx, stream_id, 14);
    }
    anchor.slots[slot_idx].free_stream(strm);
}

/// A revoke's key check is in flight on the stream (`body_buf[8]`).
const REVOKE_CHECKING: u8 = 1;

/// Ask for the next page of a lease's keys after `after`, for the
/// stream (a revoke's permission check, or a TimeToLive's listing).
fn request_lease_keys(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    lease_id: u64,
    after: &[u8],
) {
    let mut body = [0u8; 8 + 2 + STORE_KEY_MAX];
    body[0..8].copy_from_slice(&lease_id.to_le_bytes());
    let n = after.len();
    if n > STORE_KEY_MAX {
        return;
    }
    body[8..10].copy_from_slice(&(n as u16).to_le_bytes());
    body[10..10 + n].copy_from_slice(&after[..n]);
    let corr = anchor.next_corr();
    if send_kv_op(
        anchor,
        corr,
        slot_idx as u8,
        KV_OP_LEASE_KEYS,
        &body[..10 + n],
    ) {
        anchor.slots[slot_idx].streams[strm].pending_corr = corr;
    } else {
        let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
        send_grpc_error(anchor, slot_idx, stream_id, 14);
        anchor.slots[slot_idx].free_stream(strm);
    }
}

/// A page of the keys a revoke would delete: every one must be the
/// user's to write. Then the next page, or the revoke itself.
fn revoke_check_page(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    result: u8,
    body_off: usize,
    body_len: usize,
) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    anchor.slots[slot_idx].streams[strm].pending_corr = 0;
    let lease_id = rd_le64(&anchor.slots[slot_idx].streams[strm].body_buf, 0);
    let user = stream_user(anchor, slot_idx, strm);
    let u = match user {
        Ok(Some(u)) if result == KV_RESULT_KVS && body_len >= 11 => u,
        _ => {
            let mut m = [0u8; 96];
            let n = msg_permission_denied(&mut m);
            send_grpc_error_msg(anchor, slot_idx, stream_id, 7, &m[..n]);
            anchor.slots[slot_idx].free_stream(strm);
            return;
        }
    };
    let b = &anchor.reply[body_off..body_off + body_len];
    let n = u16::from_le_bytes([b[9], b[10]]) as usize;
    let more = b[8] != 0;
    let mut p = 11usize;
    let mut last = (0usize, 0usize);
    let mut allowed = true;
    for _ in 0..n {
        let Some(kl) = b
            .get(p..p + 2)
            .map(|x| u16::from_le_bytes([x[0], x[1]]) as usize)
        else {
            break;
        };
        let Some(key) = b.get(p + 2..p + 2 + kl) else {
            break;
        };
        if !anchor.auth.permitted(u, key, &[], NEED_WRITE) {
            allowed = false;
            break;
        }
        last = (body_off + p + 2, kl);
        // Key-only entries: the value length (0) and the metadata follow.
        p += 2 + kl + 4 + 32;
    }
    if !allowed {
        let mut m = [0u8; 96];
        let n = msg_permission_denied(&mut m);
        send_grpc_error_msg(anchor, slot_idx, stream_id, 7, &m[..n]);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    if more && n > 0 {
        let mut after = [0u8; STORE_KEY_MAX];
        let (at, kl) = last;
        if kl > after.len() {
            return;
        }
        after[..kl].copy_from_slice(&anchor.reply[at..at + kl]);
        request_lease_keys(anchor, slot_idx, strm, lease_id, &after[..kl]);
        return;
    }
    anchor.slots[slot_idx].streams[strm].body_buf[8] = 0;
    if anchor.live_lease(lease_id).is_none() {
        send_grpc_error_msg(anchor, slot_idx, stream_id, 5, LEASE_NOT_FOUND);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    send_lease_ctrl(anchor, slot_idx, strm, LEASE_CTRL_REVOKE, lease_id, 0);
}

/// Send a lease control op for a stream, and stash what its answer needs.
fn send_lease_ctrl(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    ctrl: u8,
    lease_id: u64,
    ttl_ms: u32,
) {
    lease_session_ctrl(
        anchor,
        ctrl,
        lease_id,
        if ctrl == LEASE_CTRL_GRANT { ttl_ms } else { 0 },
    );

    // Stash lease_id for rendezvous when MSG_LEASE_STATE returns.
    // ttl_ms is also stashed in body_buf[0..4] for the response builder
    // (lease_manager's MSG_LEASE_STATE echoes ttl_ms, but for revoke
    // it returns 0; the response encoder uses what's on the stream).
    anchor.slots[slot_idx].streams[strm].pending_lease_id = lease_id;
    let bb = &mut anchor.slots[slot_idx].streams[strm].body_buf;
    bb[0..4].copy_from_slice(&ttl_ms.to_le_bytes());
    bb[8] = 0;
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

    // Session-framed keepalive routed to the lease's current manager.
    // ATTACH is idempotent, so a keepalive on a fresh stream (or after a
    // manager move) re-establishes the session; the manager fences it by
    // the session epoch.
    if anchor.lease_ctrl_out < 0 {
        return;
    }
    if anchor.live_lease(lease_id).is_none() {
        // A lease that is gone answers TTL 0, which etcd's clients read
        // as expiry.
        write_lease_response_frames(
            anchor,
            slot_idx,
            strm,
            stream_id,
            GrpcMethod::LeaseKeepAlive,
            0,
            lease_id,
            0,
        );
        return;
    }
    lease_session_ctrl(anchor, LEASE_CTRL_KEEPALIVE, lease_id, 0);

    // Stash for rendezvous. Unlike grant/revoke, the stream stays open
    // across multiple keepalives; pending_lease_id is cleared in
    // poll_lease_in once the ack lands.
    anchor.slots[slot_idx].streams[strm].pending_lease_id = lease_id;
    let _ = stream_id;
}

/// The protobuf message inside one uncompressed gRPC frame.
fn grpc_proto(b: &[u8]) -> Option<&[u8]> {
    let hdr = GrpcMessageHeader::parse(b.get(..GRPC_FRAME_HEADER_LEN)?)?;
    if hdr.compressed != 0 {
        return None;
    }
    b.get(GRPC_FRAME_HEADER_LEN..GRPC_FRAME_HEADER_LEN.checked_add(hdr.length as usize)?)
}

/// Send one internal `MSG_KV_REQUEST` (`op` over `body`) under `corr`.
fn send_kv_op(anchor: &mut AnchorState, corr: u64, slot_byte: u8, op: u8, body: &[u8]) -> bool {
    send_kv_op_in(anchor, corr, slot_byte, op, body, 0)
}

/// [`send_kv_op_in`] as a linearizable read: answered once the store has
/// applied everything committed. The anchor's record loads take it, so
/// a restarted graph's records are read after its log has replayed.
fn send_kv_read_lin(
    anchor: &mut AnchorState,
    corr: u64,
    op: u8,
    body: &[u8],
    keyspace: u32,
) -> bool {
    send_kv_op_with(
        anchor,
        corr,
        0xFF,
        op,
        body,
        keyspace,
        types::REQ_LINEARIZABLE,
    )
}

/// [`send_kv_op`] in `keyspace` (database 0), for the anchor's own
/// records; 0 is the protocol's flat keyspace.
fn send_kv_op_in(
    anchor: &mut AnchorState,
    corr: u64,
    slot_byte: u8,
    op: u8,
    body: &[u8],
    keyspace: u32,
) -> bool {
    send_kv_op_with(
        anchor,
        corr,
        slot_byte,
        op,
        body,
        keyspace,
        types::REQ_SERIALIZABLE,
    )
}

/// One internal `MSG_KV_REQUEST` with its read consistency.
#[allow(
    clippy::too_many_arguments,
    reason = "one request envelope: each argument is one of its head fields"
)]
fn send_kv_op_with(
    anchor: &mut AnchorState,
    corr: u64,
    slot_byte: u8,
    op: u8,
    body: &[u8],
    keyspace: u32,
    consistency: u8,
) -> bool {
    const REQ_HEAD: usize = 8 + 1 + 4 + 1 + 1 + 1 + 2;
    const BODY_MAX: usize = 4000;
    let mut req = [0u8; REQ_HEAD + BODY_MAX + 8];
    if body.len() > BODY_MAX {
        return false;
    }
    req[0..8].copy_from_slice(&corr.to_le_bytes());
    req[8] = PROTO_ETCD;
    req[9..13].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());
    req[13] = slot_byte;
    req[14] = consistency;
    req[15] = op;
    req[16..18].copy_from_slice(&(body.len() as u16).to_le_bytes());
    req[REQ_HEAD..REQ_HEAD + body.len()].copy_from_slice(body);
    let mut len = REQ_HEAD + body.len();
    if keyspace != 0 {
        // The identity tail: `[database:u32][keyspace:u32]`.
        req[len..len + 4].copy_from_slice(&0u32.to_le_bytes());
        req[len + 4..len + 8].copy_from_slice(&keyspace.to_le_bytes());
        len += 8;
    }
    unsafe { write_env(anchor.syscalls, anchor.kv_out, MSG_KV_REQUEST, &req[..len]) }
}

/// Ask the store for lease `id`'s keys (or to delete them) on behalf of
/// a stream; the reply finds the stream by its `pending_corr`.
fn stream_lease_op(anchor: &mut AnchorState, slot_idx: usize, strm: usize, op: u8, id: u64) {
    let corr = anchor.next_corr();
    if send_kv_op(anchor, corr, slot_idx as u8, op, &id.to_le_bytes()) {
        anchor.slots[slot_idx].streams[strm].pending_corr = corr;
    } else {
        let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
        send_grpc_error(anchor, slot_idx, stream_id, 14);
        anchor.slots[slot_idx].free_stream(strm);
    }
}

/// Answer a LeaseTimeToLive with no keys to list.
fn answer_lease_ttl(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    id: u64,
    ttl_s: i64,
    granted_s: i64,
) {
    stash_lease_ttl(anchor, slot_idx, strm, id, ttl_s, granted_s);
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let rev = anchor.last_rev;
    write_response_frames(
        anchor,
        slot_idx,
        strm,
        stream_id,
        GrpcMethod::LeaseTimeToLive,
        KV_RESULT_OK,
        rev,
        24,
        0,
    );
    anchor.slots[slot_idx].free_stream(strm);
}

/// Hold a TimeToLive answer's numbers on the stream, where the response
/// builder reads them.
fn stash_lease_ttl(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    id: u64,
    ttl_s: i64,
    granted_s: i64,
) {
    let s = &mut anchor.slots[slot_idx].streams[strm];
    s.body_buf[0..8].copy_from_slice(&id.to_le_bytes());
    s.body_buf[8..16].copy_from_slice(&ttl_s.to_le_bytes());
    s.body_buf[16..24].copy_from_slice(&granted_s.to_le_bytes());
    s.body_len = 24;
}

/// Answer a LeaseLeases from the anchor's live lease sessions.
fn answer_lease_leases(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    let mut n = 0usize;
    for i in 0..LEASE_SESSIONS {
        let sess = &anchor.lsa.sessions[i];
        if !sess.in_use
            || sess.class != CLASS_LEASE
            || sess.phase == session_anchor::APHASE_DETACH_WAIT
        {
            continue;
        }
        let id = sess.app_id;
        if (n + 1) * 8 > STREAM_BODY_MAX {
            break;
        }
        anchor.slots[slot_idx].streams[strm].body_buf[n * 8..n * 8 + 8]
            .copy_from_slice(&id.to_le_bytes());
        n += 1;
    }
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let rev = anchor.last_rev;
    write_response_frames(
        anchor,
        slot_idx,
        strm,
        stream_id,
        GrpcMethod::LeaseLeases,
        KV_RESULT_OK,
        rev,
        n * 8,
        0,
    );
    anchor.slots[slot_idx].free_stream(strm);
}

/// Send the next ended lease's key deletion, when none is in flight.
fn serve_lease_end(anchor: &mut AnchorState) {
    if anchor.lease_end_corr != 0 || anchor.lease_end_len == 0 || anchor.kv_out < 0 {
        return;
    }
    let id = anchor.lease_end[anchor.lease_end_head];
    let corr = anchor.next_corr();
    let sent = if anchor.lease_end_phase == 0 {
        send_kv_op(anchor, corr, 0xFF, KV_OP_LEASE_REVOKE, &id.to_le_bytes())
    } else {
        send_lease_record(anchor, corr, 0xFF, id, None)
    };
    if sent {
        anchor.lease_end_corr = corr;
    }
}

/// Drain one `MSG_LEASE_REVOKE`: the lease ended (a revoke, or its TTL
/// passed). Its session goes, and its keys join the deletion queue.
unsafe fn poll_lease_revoke(anchor: &mut AnchorState) -> bool {
    if anchor.lease_revoke_in < 0 || anchor.lease_end_len == LEASE_END_QUEUE {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let poll = ((*sys).channel_poll)(anchor.lease_revoke_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    if ((*sys).channel_read)(anchor.lease_revoke_in, hdr.as_mut_ptr(), 3) < 3 {
        return false;
    }
    let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if len == 0 || len > anchor.scratch.len() {
        return len == 0;
    }
    if (((*sys).channel_read)(anchor.lease_revoke_in, anchor.scratch.as_mut_ptr(), len) as usize)
        < len
    {
        return false;
    }
    if hdr[0] != MSG_LEASE_REVOKE || len < 8 {
        return true;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&anchor.scratch[..8]);
    let id = u64::from_le_bytes(b);
    if let Some(sess) = anchor.live_lease(id) {
        let mut sink = lease_sink!(anchor);
        anchor.lsa.detach(sess, sc::DETACH_NORMAL, &mut sink);
    }
    anchor.queue_lease_end(id);
    true
}

/// Send a 3-frame response (response HEADERS + DATA + trailing
/// HEADERS) carrying just a grpc-status (no payload). Used when we
/// can't / won't honour a request.
fn send_grpc_error(anchor: &mut AnchorState, slot_idx: usize, stream_id: u32, grpc_status: u8) {
    send_grpc_error_msg(anchor, slot_idx, stream_id, grpc_status, &[]);
}

/// [`send_grpc_error`] with a `grpc-message`: the text etcd's clients
/// print, and match on, for its named errors.
fn send_grpc_error_msg(
    anchor: &mut AnchorState,
    slot_idx: usize,
    stream_id: u32,
    grpc_status: u8,
    message: &[u8],
) {
    let mut hdr_block = [0u8; 64];
    let mut hoff = 0;
    if build_response_headers_block(&mut hdr_block, &mut hoff).is_none() {
        return;
    }
    let mut trail_block = [0u8; 160];
    let mut toff = 0;
    if build_grpc_trailers_block(&mut trail_block, &mut toff, grpc_status).is_none() {
        return;
    }
    if !message.is_empty()
        && etcd_codec::hpack_encode_literal(&mut trail_block, &mut toff, b"grpc-message", message)
            .is_none()
    {
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
    if payload_len < RESP_HEAD || payload_len > anchor.reply.len() {
        return false;
    }
    let n2 = ((*sys).channel_read)(anchor.routed_in, anchor.reply.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    // Parse the response head from the reply buffer; the body is moved
    // out before anything answers (the answer path encodes in scratch).
    let corr;
    let result;
    let revision;
    let body_len;
    let body_off = RESP_HEAD;
    {
        let p = &anchor.reply[..payload_len];
        corr = u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]]);
        let _conn_id = p[8];
        result = p[9];
        revision =
            u64::from_le_bytes([p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17]]) as i64;
        body_len = u16::from_le_bytes([p[18], p[19]]) as usize;
    }
    anchor.last_rev = anchor.last_rev.max(revision);
    if body_off + body_len > payload_len {
        return false;
    }
    // The fence tail: `[applied_index:8][applied_term:8]…`.
    let tail = body_off + body_len;
    if tail + 16 <= payload_len {
        let mut b = [0u8; 8];
        b.copy_from_slice(&anchor.reply[tail..tail + 8]);
        anchor.last_index = anchor.last_index.max(u64::from_le_bytes(b));
        b.copy_from_slice(&anchor.reply[tail + 8..tail + 16]);
        anchor.last_term = anchor.last_term.max(u64::from_le_bytes(b));
    }

    let more = body_len > 8 && result == KV_RESULT_KVS && anchor.reply[body_off + 8] != 0;
    if corr != 0 && corr == anchor.lease_end_corr {
        // An ended lease: its keys (repeated while some remain), then its
        // record.
        anchor.lease_end_corr = 0;
        if anchor.lease_end_phase == 0 {
            if !more {
                anchor.lease_end_phase = 1;
            }
        } else {
            anchor.lease_end_phase = 0;
            anchor.lease_end_head = (anchor.lease_end_head + 1) % LEASE_END_QUEUE;
            anchor.lease_end_len -= 1;
        }
        return true;
    }

    if corr != 0 && corr == anchor.auth_load.corr {
        anchor.auth_load.corr = 0;
        record_load_page(anchor, LOAD_AUTH, result, body_off, body_len);
        return true;
    }
    if corr != 0 && corr == anchor.lease_load.corr {
        anchor.lease_load.corr = 0;
        record_load_page(anchor, LOAD_LEASES, result, body_off, body_len);
        return true;
    }
    if corr != 0 && corr == anchor.auth_job.corr {
        anchor.auth_job.corr = 0;
        auth_job_done(anchor, result, revision);
        return true;
    }
    // An exchange of a request carried through the large-value layer, or
    // of a watch event's value.
    if corr != 0 && corr == anchor.wlg.corr {
        wlg_reply(anchor, result, body_off, body_len);
        return true;
    }
    if corr != 0 {
        if let Some(i) = anchor.lg.iter().position(|l| l.corr == corr) {
            lg_reply(anchor, i, result, revision, body_off, body_len);
            return true;
        }
    }
    let Some((slot_idx, strm)) = anchor.find_inflight(corr) else {
        return true;
    };
    if anchor.slots[slot_idx].streams[strm].method == GrpcMethod::LeaseRevoke
        && anchor.slots[slot_idx].streams[strm].body_buf[8] == REVOKE_CHECKING
    {
        revoke_check_page(anchor, slot_idx, strm, result, body_off, body_len);
        return true;
    }
    if anchor.slots[slot_idx].streams[strm].method == GrpcMethod::LeaseGrant
        && anchor.slots[slot_idx].streams[strm].body_buf[8] == GRANT_PERSISTING
    {
        grant_recorded(anchor, slot_idx, strm, result);
        return true;
    }
    // A revoke whose keys are all gone deletes its record, then answers.
    if !more
        && result == KV_RESULT_KVS
        && anchor.slots[slot_idx].streams[strm].method == GrpcMethod::LeaseRevoke
        && anchor.slots[slot_idx].streams[strm].body_buf[8] != REVOKE_RECORD
    {
        let s = &mut anchor.slots[slot_idx].streams[strm];
        s.body_buf[8] = REVOKE_RECORD;
        let id = rd_le64(&s.body_buf, 0);
        let corr = anchor.next_corr();
        if send_lease_record(anchor, corr, slot_idx as u8, id, None) {
            anchor.slots[slot_idx].streams[strm].pending_corr = corr;
        } else {
            let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
            send_grpc_error(anchor, slot_idx, stream_id, 14);
            anchor.slots[slot_idx].free_stream(strm);
        }
        return true;
    }
    if anchor.slots[slot_idx].streams[strm].method == GrpcMethod::LeaseTimeToLive {
        ttl_keys_page(anchor, slot_idx, strm, result, body_off, body_len);
        return true;
    }
    if anchor.slots[slot_idx].streams[strm].method == GrpcMethod::Snapshot {
        snapshot_page(anchor, slot_idx, strm, result, revision, body_off, body_len);
        return true;
    }
    if more && anchor.slots[slot_idx].streams[strm].method == GrpcMethod::LeaseRevoke {
        // A revoke answers once all its keys are gone.
        let s = &anchor.slots[slot_idx].streams[strm];
        let id = u64::from_le_bytes(s.body_buf[0..8].try_into().unwrap_or([0; 8]));
        stream_lease_op(anchor, slot_idx, strm, KV_OP_LEASE_REVOKE, id);
        return true;
    }

    // A direct request that met a large value (refused, nothing applied)
    // goes again, through the large-value layer.
    if result == types::KV_RESULT_MANIFEST {
        let s = &mut anchor.slots[slot_idx].streams[strm];
        if s.kept_len > 0
            && matches!(
                s.method,
                GrpcMethod::Range | GrpcMethod::Put | GrpcMethod::DeleteRange
            )
        {
            s.body_len = s.kept_len;
            s.lg_retry = true;
            s.pending_corr = 0;
            dispatch_kv(anchor, slot_idx, strm);
            return true;
        }
    }

    // Move the response body from the reply buffer into response_data so
    // the reply buffer is free for the next reply, and so the
    // borrow checker can split frame-construction borrows cleanly.
    if body_len > anchor.response_data.len() {
        return true;
    }
    anchor.response_data[..body_len].copy_from_slice(&anchor.reply[body_off..body_off + body_len]);

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
        // A Txn branch writing one key twice: etcd's ErrDuplicateKey.
        KV_RESULT_DUPLICATE => 3,
        // Past what the write path carries: etcd's ErrRequestTooLarge.
        types::KV_RESULT_TOO_LARGE => 3,
        // A Put that needed a stored key (ignore_value / ignore_lease)
        // and found none: etcd's ErrKeyNotFound, InvalidArgument.
        KV_RESULT_NOT_FOUND if method == GrpcMethod::Put => 3,
        KV_RESULT_OK | KV_RESULT_NOT_FOUND | KV_RESULT_INTEGER | KV_RESULT_RANGE
        | KV_RESULT_KVS | KV_RESULT_ETCD_TXN => 0,
        // A read ahead of the store: etcd's ErrFutureRev, OUT_OF_RANGE.
        KV_RESULT_COMPACTED | KV_RESULT_FUTURE_REV => GRPC_STATUS_COMPACTED,
        // No authority answered in time (a write on a follower, a read
        // fence unresolved): etcd's ErrTimeout, UNAVAILABLE — retryable.
        types::KV_RESULT_LIN_BOUND => 14,
        // The routing epoch moved under the request: etcd's
        // ErrLeaderChanged, UNAVAILABLE — retry after re-resolving.
        types::KV_RESULT_DIRTY_EPOCH => 14,
        // Throttled or backpressured: etcd's ErrTooManyRequests.
        types::KV_RESULT_QUOTA => 8,
        // An operation spanning ranges the range map splits: it cannot
        // run as asked on this graph.
        types::KV_RESULT_CROSS_RANGE => 9,
        _ => 13,
    };

    // A Range the store's answer could not hold whole is refused, never
    // sent as the whole range: the client narrows it with a limit and
    // pages.
    let partial = (method == GrpcMethod::Range && result == KV_RESULT_KVS)
        || (method == GrpcMethod::Txn && result == KV_RESULT_ETCD_TXN);
    if partial
        && etcd_codec::answer_is_partial(
            &anchor.response_data[..kv_body_len],
            method == GrpcMethod::Txn,
            0,
        )
    {
        let mut m = [0u8; 96];
        let n = msg_range_too_large(&mut m);
        send_grpc_error_msg(anchor, slot_idx, stream_id, 8, &m[..n]);
        return;
    }
    // Encode the gRPC DATA payload into resp_out. The split borrow uses
    // three disjoint fields: resp_out (write), the stream's body_buf
    // (read, for the key) and response_data (read, for the answer).
    let ctx = anchor.resp_ctx();
    let data_len = {
        let out = &mut anchor.resp_out[..];
        let key = &anchor.slots[slot_idx].streams[strm].body_buf[..key_len];
        let kv_body = &anchor.response_data[..kv_body_len];
        build_grpc_response_with_key(method, result, revision, key, kv_body, &ctx, out).unwrap_or(0)
    };

    let mut hdr_block = [0u8; 64];
    let mut hoff = 0;
    if build_response_headers_block(&mut hdr_block, &mut hoff).is_none() {
        return;
    }
    let mut trail_block = [0u8; 160];
    let mut toff = 0;
    if build_grpc_trailers_block(&mut trail_block, &mut toff, grpc_status).is_none() {
        return;
    }
    // etcd's error text, which its clients print and match on.
    let mut msg = [0u8; 96];
    let mlen = if grpc_status == 0 {
        0
    } else if result == KV_RESULT_COMPACTED {
        msg_compacted(&mut msg)
    } else if result == KV_RESULT_FUTURE_REV {
        msg_future_rev(&mut msg)
    } else if result == KV_RESULT_DUPLICATE {
        msg_duplicate_key(&mut msg)
    } else if result == KV_RESULT_NOT_FOUND {
        msg_key_not_found(&mut msg)
    } else if result == types::KV_RESULT_TOO_LARGE {
        msg_request_too_large(&mut msg)
    } else if result == types::KV_RESULT_LIN_BOUND {
        msg_timed_out(&mut msg)
    } else if result == types::KV_RESULT_DIRTY_EPOCH {
        msg_leader_changed(&mut msg)
    } else if result == types::KV_RESULT_QUOTA {
        msg_too_many_requests(&mut msg)
    } else if result == types::KV_RESULT_CROSS_RANGE {
        msg_cross_range(&mut msg)
    } else {
        0
    };
    if mlen > 0
        && etcd_codec::hpack_encode_literal(
            &mut trail_block,
            &mut toff,
            b"grpc-message",
            &msg[..mlen],
        )
        .is_none()
    {
        return;
    }

    // The whole response goes out, or none of it: a response that does
    // not fit the send buffer now is answered as the overload it is.
    let need = 3 * FRAME_HEADER_LEN + hoff + data_len + toff;
    if anchor.slots[slot_idx].send_len + need > SEND_BUF_SIZE {
        anchor.m_net_errors += 1;
        send_grpc_error(anchor, slot_idx, stream_id, 8);
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
    if data_len > 0 {
        let _ = append_frame(
            slot,
            FrameType::Data as u8,
            0,
            stream_id,
            &anchor.resp_out[..data_len],
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

// etcd's error messages. One function per message, never inlined: a
// branch that picked among byte literals directly could be lowered to a
// table of pointers, which the PIC loader does not relocate.

#[inline(never)]
fn put_msg(out: &mut [u8], m: &[u8]) -> usize {
    let n = m.len().min(out.len());
    out[..n].copy_from_slice(&m[..n]);
    n
}

#[inline(never)]
fn msg_compacted(out: &mut [u8]) -> usize {
    put_msg(
        out,
        b"etcdserver: mvcc: required revision has been compacted",
    )
}

#[inline(never)]
fn msg_future_rev(out: &mut [u8]) -> usize {
    put_msg(
        out,
        b"etcdserver: mvcc: required revision is a future revision",
    )
}

#[inline(never)]
fn msg_duplicate_key(out: &mut [u8]) -> usize {
    put_msg(out, b"etcdserver: duplicate key given in txn request")
}

#[inline(never)]
fn msg_key_not_found(out: &mut [u8]) -> usize {
    put_msg(out, b"etcdserver: key not found")
}

#[inline(never)]
fn msg_range_too_large(out: &mut [u8]) -> usize {
    put_msg(
        out,
        b"lattice: the range does not fit one response; set a limit and page",
    )
}

#[inline(never)]
fn msg_lease_keys_too_many(out: &mut [u8]) -> usize {
    put_msg(out, b"lattice: the lease's keys do not fit one response")
}

#[inline(never)]
fn msg_request_too_large(out: &mut [u8]) -> usize {
    put_msg(out, b"etcdserver: request is too large")
}

#[inline(never)]
fn msg_no_space(out: &mut [u8]) -> usize {
    put_msg(out, b"etcdserver: mvcc: database space exceeded")
}

#[inline(never)]
fn msg_bad_transferee(out: &mut [u8]) -> usize {
    put_msg(out, b"etcdserver: bad leader transferee")
}

#[inline(never)]
fn msg_member_not_found(out: &mut [u8]) -> usize {
    put_msg(out, b"etcdserver: member not found")
}

#[inline(never)]
fn msg_not_learner(out: &mut [u8]) -> usize {
    put_msg(out, b"etcdserver: can only promote a learner member")
}

#[inline(never)]
fn msg_last_member(out: &mut [u8]) -> usize {
    put_msg(
        out,
        b"etcdserver: re-configuration failed due to not enough started members",
    )
}

#[inline(never)]
fn msg_invalid_downgrade(out: &mut [u8]) -> usize {
    put_msg(out, b"etcdserver: invalid downgrade target version")
}

#[inline(never)]
fn msg_membership_graph(out: &mut [u8]) -> usize {
    put_msg(
        out,
        b"etcdserver: unsupported: lattice membership is configured by its graph",
    )
}

/// Open the Snapshot stream: the first page is read at the store's
/// current revision, and every later one at that same revision.
fn start_snapshot(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    if anchor.snap.active {
        let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
        send_grpc_error(anchor, slot_idx, stream_id, 8);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    let sn = &mut anchor.snap;
    sn.active = true;
    sn.waiting = false;
    sn.more = true;
    sn.slot = slot_idx;
    sn.strm = strm;
    sn.stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    sn.rev = 0;
    sn.next[0] = 0;
    sn.next_len = 1;
    sn.len = 0;
    sn.sent = 0;
    sn.total = 0;
    sn.hash = Sha256::new();
    anchor.slots[slot_idx].streams[strm].body_len = 0;
    request_snapshot_page(anchor);
}

/// Ask the store for the next page: keys from `snap.next` on, in key
/// order, at the pinned revision.
fn request_snapshot_page(anchor: &mut AnchorState) {
    let (slot_idx, strm) = (anchor.snap.slot, anchor.snap.strm);
    let mut body = [0u8; 600];
    let n = {
        let sn = &anchor.snap;
        let req = RangeRequest {
            key: &sn.next[..sn.next_len],
            range_end: b"\0",
            limit: SNAP_PAGE_KEYS,
            revision: sn.rev as i64,
            sort_order: 1,
            ..Default::default()
        };
        etcd_codec::encode_range_meta_body(&req, &mut body)
    };
    let corr = anchor.next_corr();
    let sent =
        n.is_some_and(|n| send_kv_op(anchor, corr, slot_idx as u8, KV_OP_RANGE_META, &body[..n]));
    if sent {
        anchor.slots[slot_idx].streams[strm].pending_corr = corr;
        anchor.snap.waiting = true;
    } else {
        end_snapshot(anchor, 14);
    }
}

/// A page arrived: hold it for the stream, and note where the next one
/// starts.
#[allow(
    clippy::too_many_arguments,
    reason = "one store answer: its stream, its result, and where its body sits"
)]
fn snapshot_page(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    result: u8,
    revision: i64,
    body_off: usize,
    body_len: usize,
) {
    anchor.slots[slot_idx].streams[strm].pending_corr = 0;
    anchor.snap.waiting = false;
    if result != KV_RESULT_KVS || body_len < 11 || body_len - 11 > SNAP_BUF {
        end_snapshot(
            anchor,
            if result == KV_RESULT_COMPACTED {
                GRPC_STATUS_COMPACTED
            } else {
                13
            },
        );
        return;
    }
    if anchor.snap.rev == 0 {
        anchor.snap.rev = revision.max(1) as u64;
    }
    let body = &anchor.reply[body_off..body_off + body_len];
    let n = u16::from_le_bytes([body[9], body[10]]) as usize;
    let more = body[8] != 0;
    let entries = &body[11..];
    // The last key of the page: where the next page starts, past it.
    let mut p = 0usize;
    let mut last = (0usize, 0usize);
    for _ in 0..n {
        let Some(kl) = entries
            .get(p..p + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
        else {
            break;
        };
        let Some(vl) = entries
            .get(p + 2 + kl..p + 6 + kl)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        else {
            break;
        };
        last = (p + 2, kl);
        p += 6 + kl + vl + 32;
    }
    let sn = &mut anchor.snap;
    let (at, kl) = last;
    if kl < sn.next.len() && n > 0 {
        sn.next[..kl].copy_from_slice(&entries[at..at + kl]);
        sn.next[kl] = 0;
        sn.next_len = kl + 1;
    }
    let take = p.min(entries.len());
    sn.buf[..take].copy_from_slice(&entries[..take]);
    sn.len = take;
    sn.sent = 0;
    sn.more = more && n > 0;
}

/// Drive the Snapshot stream one step: send what the page in hand has
/// left, as the connection's send buffer takes it; then ask for the
/// next page, or end with the digest.
fn serve_snapshot(anchor: &mut AnchorState) {
    if !anchor.snap.active {
        return;
    }
    let (slot_idx, strm, stream_id) = (anchor.snap.slot, anchor.snap.strm, anchor.snap.stream_id);
    // The client went away: the stream is gone.
    if anchor.slots[slot_idx].conn_id == SLOT_FREE
        || anchor.slots[slot_idx].streams[strm].stream_id != stream_id
    {
        anchor.snap.active = false;
        return;
    }
    if anchor.snap.waiting {
        return;
    }
    while anchor.snap.sent < anchor.snap.len {
        let take = (anchor.snap.len - anchor.snap.sent).min(SNAP_CHUNK);
        let mut chunk = [0u8; SNAP_CHUNK];
        let at = anchor.snap.sent;
        chunk[..take].copy_from_slice(&anchor.snap.buf[at..at + take]);
        if !send_snapshot_message(anchor, &chunk[..take], 1) {
            return; // the send buffer is full; the next step goes on
        }
        anchor.snap.hash.update(&chunk[..take]);
        anchor.snap.sent += take;
        anchor.snap.total += take as u64;
    }
    if anchor.snap.more {
        request_snapshot_page(anchor);
        return;
    }
    // The entries end at a zero key length, padded with zeros to whole
    // 512-byte pages: etcd's clients find the digest by that alignment.
    let pad = (512 - (anchor.snap.total % 512) as usize) % 512;
    if pad > 0 {
        let zeros = [0u8; 512];
        if !send_snapshot_message(anchor, &zeros[..pad], 1) {
            return;
        }
        anchor.snap.hash.update(&zeros[..pad]);
        anchor.snap.total += pad as u64;
    }
    // The last message carries the SHA-256 of everything before it.
    let digest = anchor.snap.hash.clone().finalize();
    if send_snapshot_message(anchor, &digest, 0) {
        end_snapshot(anchor, 0);
    }
}

/// One SnapshotResponse on the stream, when the send buffer holds it.
fn send_snapshot_message(anchor: &mut AnchorState, blob: &[u8], remaining: u64) -> bool {
    let (slot_idx, strm, stream_id) = (anchor.snap.slot, anchor.snap.strm, anchor.snap.stream_id);
    let ids = anchor.header_ids();
    let rev = anchor.snap.rev as i64;
    let len = {
        let scratch = &mut anchor.scratch[..];
        let mut off = 0usize;
        let proto = &mut scratch[GRPC_FRAME_HEADER_LEN..];
        let built =
            etcd_codec::build_snapshot_response(proto, &mut off, rev, remaining, blob).is_some();
        match built
            .then(|| etcd_codec::stamp_header(proto, off, &ids))
            .flatten()
        {
            Some(n) => {
                scratch[0] = 0;
                scratch[1..5].copy_from_slice(&(n as u32).to_be_bytes());
                GRPC_FRAME_HEADER_LEN + n
            }
            None => return false,
        }
    };
    let need_headers = !anchor.slots[slot_idx].streams[strm].response_headers_sent;
    let room = SEND_BUF_SIZE - anchor.slots[slot_idx].send_len;
    if room < len + FRAME_HEADER_LEN * 2 + 64 {
        return false;
    }
    let mut data = [0u8; SNAP_CHUNK + 128];
    data[..len].copy_from_slice(&anchor.scratch[..len]);
    if need_headers {
        let mut hdr_block = [0u8; 64];
        let mut hoff = 0;
        if build_response_headers_block(&mut hdr_block, &mut hoff).is_none() {
            return false;
        }
        let _ = append_frame(
            &mut anchor.slots[slot_idx],
            FrameType::Headers as u8,
            FLAG_END_HEADERS,
            stream_id,
            &hdr_block[..hoff],
        );
        anchor.slots[slot_idx].streams[strm].response_headers_sent = true;
    }
    append_frame(
        &mut anchor.slots[slot_idx],
        FrameType::Data as u8,
        0,
        stream_id,
        &data[..len],
    )
}

/// Close the Snapshot stream with `grpc_status`.
fn end_snapshot(anchor: &mut AnchorState, grpc_status: u8) {
    let (slot_idx, strm, stream_id) = (anchor.snap.slot, anchor.snap.strm, anchor.snap.stream_id);
    anchor.snap.active = false;
    if anchor.slots[slot_idx].streams[strm].stream_id != stream_id {
        return;
    }
    if grpc_status != 0 || !anchor.slots[slot_idx].streams[strm].response_headers_sent {
        send_grpc_error(anchor, slot_idx, stream_id, grpc_status);
    } else {
        let mut trail_block = [0u8; 64];
        let mut toff = 0;
        if build_grpc_trailers_block(&mut trail_block, &mut toff, 0).is_some() {
            let _ = append_frame(
                &mut anchor.slots[slot_idx],
                FrameType::Headers as u8,
                FLAG_END_HEADERS | FLAG_END_STREAM,
                stream_id,
                &trail_block[..toff],
            );
        }
    }
    anchor.slots[slot_idx].free_stream(strm);
}

// ── Auth ──────────────────────────────────────────────────────────────

/// One etcd error message per function, never inlined (see `put_msg`).
macro_rules! etcd_msg {
    ($name:ident, $text:expr) => {
        #[inline(never)]
        fn $name(out: &mut [u8]) -> usize {
            put_msg(out, $text)
        }
    };
}

etcd_msg!(msg_permission_denied, b"etcdserver: permission denied");
etcd_msg!(msg_invalid_token, b"etcdserver: invalid auth token");
etcd_msg!(msg_user_empty, b"etcdserver: user name is empty");
etcd_msg!(msg_role_empty, b"etcdserver: role name is empty");
etcd_msg!(msg_auth_not_ready, b"etcdserver: auth state is loading");
etcd_msg!(
    msg_auth_store_unavailable,
    b"etcdserver: the store is not answering (auth records unread)"
);
etcd_msg!(
    msg_auth_not_enabled,
    b"etcdserver: authentication is not enabled"
);
etcd_msg!(
    msg_auth_failed,
    b"etcdserver: authentication failed, invalid user ID or password"
);
etcd_msg!(msg_user_exists, b"etcdserver: user name already exists");
etcd_msg!(msg_user_not_found, b"etcdserver: user name not found");
etcd_msg!(msg_role_exists, b"etcdserver: role name already exists");
etcd_msg!(msg_role_not_found, b"etcdserver: role name not found");
etcd_msg!(
    msg_role_not_granted,
    b"etcdserver: role is not granted to the user"
);
etcd_msg!(
    msg_perm_not_granted,
    b"etcdserver: permission is not granted to the role"
);
etcd_msg!(msg_root_missing, b"etcdserver: root user does not exist");
etcd_msg!(
    msg_root_role_missing,
    b"etcdserver: root user does not have root role"
);
etcd_msg!(
    msg_invalid_auth_mgmt,
    b"etcdserver: invalid auth management"
);
etcd_msg!(msg_password_empty, b"etcdserver: password is empty");
etcd_msg!(msg_auth_busy, b"etcdserver: too many requests");
etcd_msg!(msg_timed_out, b"etcdserver: request timed out");
etcd_msg!(msg_leader_changed, b"etcdserver: leader changed");
etcd_msg!(msg_too_many_requests, b"etcdserver: too many requests");
etcd_msg!(
    msg_cross_range,
    b"lattice: the operation spans ranges this graph keeps apart"
);

/// A refusal: gRPC status, and etcd's message in `m[..n]`.
type Refusal = (u8, [u8; 96], usize);

fn refuse(status: u8, msg: fn(&mut [u8]) -> usize) -> Option<Refusal> {
    let mut m = [0u8; 96];
    let n = msg(&mut m);
    Some((status, m, n))
}

/// The user a stream's token names: `Ok(None)` without a token, `Err`
/// for one that is not (or no longer) valid. A token in use lives on.
fn stream_user(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
) -> Result<Option<usize>, ()> {
    let s = &anchor.slots[slot_idx].streams[strm];
    let len = s.token_len;
    if len == 0 {
        return Ok(None);
    }
    if len == TOKEN_BAD {
        return Err(());
    }
    let tok = s.token;
    let now = if anchor.syscalls.is_null() {
        0
    } else {
        unsafe { dev_millis(&*anchor.syscalls) }
    };
    let Some(t) = anchor
        .tokens
        .iter_mut()
        .find(|t| t.len == len && t.bytes[..len as usize] == tok[..len as usize])
    else {
        return Err(());
    };
    if t.expires_ms <= now {
        *t = Token::FREE;
        return Err(());
    }
    t.expires_ms = now + TOKEN_TTL_MS;
    let user = t.user;
    anchor.auth.user(user.as_bytes()).map(Some).ok_or(())
}

/// Whether the request may run, auth considered: `None` to go on, or
/// the refusal. Before the auth records have loaded nothing runs; with
/// auth off everything does; with it on, a request needs a valid token,
/// admin requests the root role, and KV requests their keys' permission.
fn authorize(anchor: &mut AnchorState, slot_idx: usize, strm: usize) -> Option<Refusal> {
    let method = anchor.slots[slot_idx].streams[strm].method;
    if !anchor.auth.loaded || anchor.lease_load.state != 2 {
        return if anchor.auth_load.failed || anchor.lease_load.failed {
            refuse(14, msg_auth_store_unavailable)
        } else {
            refuse(14, msg_auth_not_ready)
        };
    }
    if !anchor.auth.enabled {
        return None;
    }
    // Open to anyone: signing in, and what a health check reads.
    if matches!(
        method,
        GrpcMethod::Authenticate | GrpcMethod::MemberList | GrpcMethod::Status | GrpcMethod::Alarm
    ) {
        return None;
    }
    let u = match stream_user(anchor, slot_idx, strm) {
        Err(()) => return refuse(16, msg_invalid_token),
        Ok(None) => return refuse(3, msg_user_empty),
        Ok(Some(u)) => u,
    };
    if method.is_admin() {
        // A user may read their own record (etcd's UserGet of self).
        let own = method == GrpcMethod::UserGet && {
            let s = &anchor.slots[slot_idx].streams[strm];
            grpc_proto(&s.body_buf[..s.body_len])
                .and_then(etcd_codec::auth_fields)
                .is_some_and(|f| f.s[0] == anchor.auth.users[u].name.as_bytes())
        };
        return if own || anchor.auth.is_root(u) {
            None
        } else {
            refuse(7, msg_permission_denied)
        };
    }
    let s = &anchor.slots[slot_idx].streams[strm];
    let Some(proto) = grpc_proto(&s.body_buf[..s.body_len]) else {
        return None; // not a KV request, or refused as malformed later
    };
    let auth = &anchor.auth;
    let ok = match method {
        GrpcMethod::Range => RangeRequest::decode(proto)
            .is_some_and(|r| auth.permitted(u, r.key, r.range_end, NEED_READ)),
        GrpcMethod::Put => PutRequest::decode(proto).is_some_and(|r| {
            auth.permitted(u, r.key, &[], NEED_WRITE)
                && (!r.prev_kv || auth.permitted(u, r.key, &[], NEED_READ))
        }),
        GrpcMethod::DeleteRange => DeleteRangeRequest::decode(proto).is_some_and(|r| {
            auth.permitted(u, r.key, r.range_end, NEED_WRITE)
                && (!r.prev_kv || auth.permitted(u, r.key, r.range_end, NEED_READ))
        }),
        GrpcMethod::Txn => {
            let mut check = |k: &[u8], e: &[u8], a: u8| {
                let need = if a == etcd_codec::ACCESS_READ {
                    NEED_READ
                } else {
                    NEED_WRITE
                };
                auth.permitted(u, k, e, need)
            };
            etcd_codec::txn_walk(proto, 0, &mut check) == Some(true)
        }
        _ => true,
    };
    if ok {
        None
    } else {
        refuse(7, msg_permission_denied)
    }
}

fn record_load(anchor: &mut AnchorState, sel: u8) -> &mut RecordLoad {
    if sel == LOAD_AUTH {
        &mut anchor.auth_load
    } else {
        &mut anchor.lease_load
    }
}

/// Read one of the anchor's record keyspaces: at start, and again after
/// a failed read.
fn serve_record_load(anchor: &mut AnchorState, sel: u8) {
    if record_load(anchor, sel).state != 0 || anchor.kv_out < 0 || anchor.syscalls.is_null() {
        return;
    }
    let now = unsafe { dev_millis(&*anchor.syscalls) };
    if now < record_load(anchor, sel).retry_ms {
        return;
    }
    if sel == LOAD_AUTH {
        anchor.auth.init();
    }
    let rl = record_load(anchor, sel);
    rl.next[0] = 0;
    rl.next_len = 1;
    request_record_page(anchor, sel);
}

/// Ask for the next page of records, in key order.
fn request_record_page(anchor: &mut AnchorState, sel: u8) {
    let mut body = [0u8; 600];
    let n = {
        let rl = record_load(anchor, sel);
        let req = RangeRequest {
            key: &rl.next[..rl.next_len],
            range_end: b"\0",
            limit: 32,
            sort_order: 1,
            ..Default::default()
        };
        etcd_codec::encode_range_meta_body(&req, &mut body)
    };
    let keyspace = if sel == LOAD_AUTH {
        AUTH_KEYSPACE
    } else {
        LEASE_KEYSPACE
    };
    let corr = anchor.next_corr();
    let sent =
        n.is_some_and(|n| send_kv_read_lin(anchor, corr, KV_OP_RANGE_META, &body[..n], keyspace));
    if sent {
        let rl = record_load(anchor, sel);
        rl.corr = corr;
        rl.state = 1;
    } else {
        record_load_failed(anchor, sel);
    }
}

fn record_load_failed(anchor: &mut AnchorState, sel: u8) {
    let retry = if anchor.syscalls.is_null() {
        0
    } else {
        unsafe { dev_millis(&*anchor.syscalls) + 1000 }
    };
    let rl = record_load(anchor, sel);
    rl.state = 0;
    rl.failed = true;
    rl.retry_ms = retry;
}

/// A page of records: each applied, then the next page or done.
fn record_load_page(
    anchor: &mut AnchorState,
    sel: u8,
    result: u8,
    body_off: usize,
    body_len: usize,
) {
    if result != KV_RESULT_KVS || body_len < 11 {
        record_load_failed(anchor, sel);
        return;
    }
    let (n, more) = {
        let b = &anchor.reply[body_off..body_off + body_len];
        (u16::from_le_bytes([b[9], b[10]]) as usize, b[8] != 0)
    };
    let mut p = body_off + 11;
    let end = body_off + body_len;
    let mut last = (0usize, 0usize);
    for _ in 0..n {
        let rd = |b: &[u8], at: usize, w: usize| -> Option<usize> {
            let x = b.get(at..at + w)?;
            Some(if w == 2 {
                u16::from_le_bytes([x[0], x[1]]) as usize
            } else {
                u32::from_le_bytes([x[0], x[1], x[2], x[3]]) as usize
            })
        };
        let Some(kl) = rd(&anchor.reply[..end], p, 2) else {
            break;
        };
        let Some(vl) = rd(&anchor.reply[..end], p + 2 + kl, 4) else {
            break;
        };
        let (k0, v0) = (p + 2, p + 6 + kl);
        if v0 + vl > end {
            break;
        }
        let mut key = [0u8; 258];
        let mut val = [0u8; 2048];
        if kl > key.len() || vl > val.len() {
            break;
        }
        key[..kl].copy_from_slice(&anchor.reply[k0..k0 + kl]);
        val[..vl].copy_from_slice(&anchor.reply[v0..v0 + vl]);
        if sel == LOAD_AUTH {
            anchor.auth.apply(&key[..kl], Some(&val[..vl]));
        } else {
            restore_lease(anchor, &key[..kl], &val[..vl]);
        }
        last = (k0, kl);
        p = v0 + vl + 32;
    }
    if more && n > 0 && last.1 < 257 {
        let (k0, kl) = last;
        let mut next = [0u8; 258];
        next[..kl].copy_from_slice(&anchor.reply[k0..k0 + kl]);
        let rl = record_load(anchor, sel);
        rl.next[..kl].copy_from_slice(&next[..kl]);
        rl.next[kl] = 0;
        rl.next_len = kl + 1;
        request_record_page(anchor, sel);
    } else {
        record_load(anchor, sel).state = 2;
        if sel == LOAD_AUTH {
            anchor.auth.loaded = true;
        }
    }
}

/// A lease that was live when the graph stopped: granted again, with its
/// full TTL (etcd extends every lease by its TTL when it restarts).
fn restore_lease(anchor: &mut AnchorState, key: &[u8], value: &[u8]) {
    let (Some(k), Some(v)) = (key.get(..8), value.get(..4)) else {
        return;
    };
    let id = u64::from_be_bytes([k[0], k[1], k[2], k[3], k[4], k[5], k[6], k[7]]);
    let ttl_ms = u32::from_le_bytes([v[0], v[1], v[2], v[3]]);
    lease_session_ctrl(anchor, LEASE_CTRL_GRANT, id, ttl_ms);
    // IDs the anchor mints never repeat one it minted before.
    if id >> 56 == 0x7A {
        anchor.lease_seq = anchor.lease_seq.max(id & 0x00FF_FFFF_FFFF_FFFF);
    }
}

/// Write a lease's record (`LEASE_KEYSPACE`, via a KV_OP_PUT body) or,
/// `ttl_ms` `None`, delete it (a KV_OP_DELETE body), under `corr`.
fn send_lease_record(
    anchor: &mut AnchorState,
    corr: u64,
    slot_byte: u8,
    id: u64,
    ttl_ms: Option<u32>,
) -> bool {
    let key = id.to_be_bytes();
    let mut body = [0u8; 32];
    let n = match ttl_ms {
        Some(ttl) => {
            // [klen:2][key][vlen:4][value][flags:1][ttl:8]
            body[0..2].copy_from_slice(&8u16.to_le_bytes());
            body[2..10].copy_from_slice(&key);
            body[10..14].copy_from_slice(&4u32.to_le_bytes());
            body[14..18].copy_from_slice(&ttl.to_le_bytes());
            27
        }
        None => {
            // [count:2][klen:2][key]
            body[0..2].copy_from_slice(&1u16.to_le_bytes());
            body[2..4].copy_from_slice(&8u16.to_le_bytes());
            body[4..12].copy_from_slice(&key);
            12
        }
    };
    let op = if ttl_ms.is_some() {
        KV_OP_PUT
    } else {
        KV_OP_DELETE
    };
    send_kv_op_in(anchor, corr, slot_byte, op, &body[..n], LEASE_KEYSPACE)
}

/// Stage one record change of the auth job.
fn stage(job: &mut AuthJob, key: &[u8], value: Option<&[u8]>) -> bool {
    let v = value.unwrap_or(&[]);
    let need = 2 + key.len() + 2 + v.len();
    if key.len() > u8::MAX as usize || job.len + need > AUTH_CHANGES_MAX {
        return false;
    }
    let b = &mut job.changes[job.len..job.len + need];
    b[0] = u8::from(value.is_some());
    b[1] = key.len() as u8;
    b[2..2 + key.len()].copy_from_slice(key);
    b[2 + key.len()..4 + key.len()].copy_from_slice(&(v.len() as u16).to_le_bytes());
    b[4 + key.len()..].copy_from_slice(v);
    job.len += need;
    true
}

/// Stage a user's record.
fn stage_user(job: &mut AuthJob, u: &User) -> bool {
    let mut key = [0u8; 80];
    let mut val = [0u8; 1024];
    let (Some(kl), Some(vl)) = (
        etcd_auth::record_key(b"u/", u.name.as_bytes(), &mut key),
        etcd_auth::encode_user(u, &mut val),
    ) else {
        return false;
    };
    stage(job, &key[..kl], Some(&val[..vl]))
}

/// Stage a role's record.
fn stage_role(job: &mut AuthJob, r: &Role) -> bool {
    let mut key = [0u8; 80];
    let mut val = [0u8; 2200];
    let (Some(kl), Some(vl)) = (
        etcd_auth::record_key(b"r/", r.name.as_bytes(), &mut key),
        etcd_auth::encode_role(r, &mut val),
    ) else {
        return false;
    };
    stage(job, &key[..kl], Some(&val[..vl]))
}

/// A fresh salt and a password's hash under it.
fn new_password(anchor: &AnchorState, u: &mut User, password: &[u8]) -> bool {
    if anchor.syscalls.is_null() {
        return false;
    }
    let mut salt = [0u8; 16];
    if unsafe { dev_csprng_fill(&*anchor.syscalls, salt.as_mut_ptr(), salt.len()) } < 0 {
        return false;
    }
    u.salt = salt;
    u.hash = etcd_auth::hash_password(password, &salt);
    true
}

/// A new token for user `name`, in `out` (`[len:u8][token]`).
fn issue_token(anchor: &mut AnchorState, name: &Name, out: &mut [u8]) -> Option<usize> {
    const ALPHABET: [u8; 52] = *b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    if anchor.syscalls.is_null() {
        return None;
    }
    let mut r = [0u8; 16];
    if unsafe { dev_csprng_fill(&*anchor.syscalls, r.as_mut_ptr(), r.len()) } < 0 {
        return None;
    }
    let now = unsafe { dev_millis(&*anchor.syscalls) };
    anchor.token_seq = anchor.token_seq.wrapping_add(1);
    let mut t = Token::FREE;
    for (i, b) in r.iter().enumerate() {
        t.bytes[i] = ALPHABET[(*b as usize) % ALPHABET.len()];
    }
    t.bytes[16] = b'.';
    let n = fmt_dec(&mut t.bytes[17..], u64::from(anchor.token_seq));
    t.len = (17 + n) as u8;
    t.user = *name;
    t.expires_ms = now + TOKEN_TTL_MS;
    // A free or lapsed slot, else the one closest to lapsing.
    let mut at = 0usize;
    for (i, x) in anchor.tokens.iter().enumerate() {
        if x.len == 0 || x.expires_ms <= now {
            at = i;
            break;
        }
        if x.expires_ms < anchor.tokens[at].expires_ms {
            at = i;
        }
    }
    anchor.tokens[at] = t;
    let l = t.len as usize;
    *out.get_mut(0)? = t.len;
    out.get_mut(1..1 + l)?.copy_from_slice(&t.bytes[..l]);
    Some(1 + l)
}

/// Names, sorted, as `[len:u8][name]` each into `out`.
fn list_names<'a, I: Iterator<Item = &'a Name>>(names: I, out: &mut [u8]) -> Option<usize> {
    let mut all = [Name::EMPTY; etcd_auth::USERS_MAX + etcd_auth::ROLES_MAX];
    let mut n = 0usize;
    for x in names {
        *all.get_mut(n)? = *x;
        n += 1;
    }
    // Insertion sort: a few dozen names.
    for i in 1..n {
        let mut j = i;
        while j > 0 && all[j - 1].as_bytes() > all[j].as_bytes() {
            all.swap(j - 1, j);
            j -= 1;
        }
    }
    let mut p = 0usize;
    for x in &all[..n] {
        *out.get_mut(p)? = x.len;
        out.get_mut(p + 1..p + 1 + x.len as usize)?
            .copy_from_slice(x.as_bytes());
        p += 1 + x.len as usize;
    }
    Some(p)
}

/// Answer an Auth-service RPC: reads from the cache at once; a change is
/// staged and committed as one Txn to the auth keyspace, and answered
/// when it has.
fn answer_auth(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let method = anchor.slots[slot_idx].streams[strm].method;
    let mut req = [0u8; STREAM_BODY_MAX];
    let req_len = {
        let s = &anchor.slots[slot_idx].streams[strm];
        match grpc_proto(&s.body_buf[..s.body_len]) {
            Some(p) => {
                req[..p.len()].copy_from_slice(p);
                p.len()
            }
            None => usize::MAX,
        }
    };
    let Some(f) = (req_len != usize::MAX)
        .then(|| etcd_codec::auth_fields(&req[..req_len]))
        .flatten()
    else {
        send_grpc_error(anchor, slot_idx, stream_id, 3);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    };
    match auth_request(anchor, slot_idx, strm, method, &f) {
        Ok(Some(stash_len)) => {
            let rev = anchor.last_rev.max(1);
            write_response_frames(
                anchor,
                slot_idx,
                strm,
                stream_id,
                method,
                KV_RESULT_OK,
                rev,
                stash_len,
                0,
            );
            anchor.slots[slot_idx].free_stream(strm);
        }
        // Committing: answered when the Txn lands.
        Ok(None) => {}
        Err((status, m, n)) => {
            send_grpc_error_msg(anchor, slot_idx, stream_id, status, &m[..n]);
            anchor.slots[slot_idx].free_stream(strm);
        }
    }
}

/// The work of one Auth RPC. `Ok(Some(n))`: answer now, with `n` bytes
/// staged in the stream's body for the response; `Ok(None)`: a change
/// is committing; `Err`: refused.
fn auth_request(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    method: GrpcMethod,
    f: &etcd_codec::AuthFields<'_>,
) -> Result<Option<usize>, Refusal> {
    let err = |status: u8, msg: fn(&mut [u8]) -> usize| -> Refusal {
        let mut m = [0u8; 96];
        let n = msg(&mut m);
        (status, m, n)
    };
    let auth = &anchor.auth;
    // ── reads ──
    match method {
        GrpcMethod::AuthStatus => {
            let bb = &mut anchor.slots[slot_idx].streams[strm].body_buf;
            bb[0] = u8::from(auth.enabled);
            bb[1..9].copy_from_slice(&auth.revision.to_le_bytes());
            return Ok(Some(9));
        }
        GrpcMethod::Authenticate => {
            if !auth.enabled {
                return Err(err(9, msg_auth_not_enabled));
            }
            let ok = auth
                .user(f.s[0])
                .filter(|&i| etcd_auth::check_password(&auth.users[i], f.s[1]));
            let Some(i) = ok else {
                return Err(err(3, msg_auth_failed));
            };
            let name = auth.users[i].name;
            let mut out = [0u8; 40];
            let Some(n) = issue_token(anchor, &name, &mut out) else {
                return Err(err(14, msg_auth_busy));
            };
            anchor.slots[slot_idx].streams[strm].body_buf[..n].copy_from_slice(&out[..n]);
            return Ok(Some(n));
        }
        GrpcMethod::UserGet => {
            let Some(i) = auth.user(f.s[0]) else {
                return Err(err(9, msg_user_not_found));
            };
            let u = auth.users[i];
            let bb = &mut anchor.slots[slot_idx].streams[strm].body_buf;
            return list_names(u.roles().iter(), bb)
                .map(Some)
                .ok_or_else(|| err(8, msg_auth_busy));
        }
        GrpcMethod::UserList | GrpcMethod::RoleList => {
            let mut out = [0u8; STREAM_BODY_MAX];
            let n = if method == GrpcMethod::UserList {
                list_names(
                    auth.users.iter().filter(|u| u.used).map(|u| &u.name),
                    &mut out,
                )
            } else {
                list_names(
                    auth.roles.iter().filter(|r| r.used).map(|r| &r.name),
                    &mut out,
                )
            };
            let Some(n) = n else {
                return Err(err(8, msg_auth_busy));
            };
            anchor.slots[slot_idx].streams[strm].body_buf[..n].copy_from_slice(&out[..n]);
            return Ok(Some(n));
        }
        GrpcMethod::RoleGet => {
            let Some(i) = auth.role(f.s[0]) else {
                return Err(err(9, msg_role_not_found));
            };
            let r = auth.roles[i];
            let mut out = [0u8; 2200];
            let Some(n) = etcd_auth::encode_role(&r, &mut out).filter(|&n| n <= STREAM_BODY_MAX)
            else {
                return Err(err(8, msg_auth_busy));
            };
            // The record's form after its count byte: the permissions.
            anchor.slots[slot_idx].streams[strm].body_buf[..n - 1].copy_from_slice(&out[1..n]);
            return Ok(Some(n - 1));
        }
        _ => {}
    }
    // ── changes ──
    if anchor.auth_job.active {
        return Err(err(14, msg_auth_busy));
    }
    anchor.auth_job.len = 0;
    let enabled = auth.enabled;
    let root = etcd_auth::ROOT;
    let staged = match method {
        GrpcMethod::AuthEnable => {
            if enabled {
                return Ok(Some(0));
            }
            let Some(i) = auth.user(root) else {
                return Err(err(9, msg_root_missing));
            };
            if !auth.users[i].has_role(root) {
                return Err(err(9, msg_root_role_missing));
            }
            stage(&mut anchor.auth_job, etcd_auth::KEY_ENABLED, Some(&[1]))
        }
        GrpcMethod::AuthDisable => {
            if !enabled {
                return Ok(Some(0));
            }
            stage(&mut anchor.auth_job, etcd_auth::KEY_ENABLED, Some(&[0]))
        }
        GrpcMethod::UserAdd => {
            let Some(name) = Name::from(f.s[0]) else {
                return Err(err(3, msg_user_empty));
            };
            if auth.user(f.s[0]).is_some() {
                return Err(err(9, msg_user_exists));
            }
            if auth.free_user().is_none() {
                return Err(err(8, msg_auth_busy));
            }
            // UserAddOptions { bool no_password = 1; }
            let no_password = etcd_codec::decode_field1_varint(f.s[2]).unwrap_or(0) != 0;
            let mut u = User::EMPTY;
            u.used = true;
            u.name = name;
            u.no_password = no_password;
            if !no_password {
                if f.s[1].is_empty() {
                    return Err(err(3, msg_password_empty));
                }
                if !new_password(anchor, &mut u, f.s[1]) {
                    return Err(err(14, msg_auth_busy));
                }
            }
            stage_user(&mut anchor.auth_job, &u)
        }
        GrpcMethod::UserDelete => {
            if auth.user(f.s[0]).is_none() {
                return Err(err(9, msg_user_not_found));
            }
            if enabled && f.s[0] == root {
                return Err(err(9, msg_invalid_auth_mgmt));
            }
            let mut key = [0u8; 80];
            let Some(kl) = etcd_auth::record_key(b"u/", f.s[0], &mut key) else {
                return Err(err(3, msg_user_empty));
            };
            stage(&mut anchor.auth_job, &key[..kl], None)
        }
        GrpcMethod::UserChangePassword => {
            let Some(i) = auth.user(f.s[0]) else {
                return Err(err(9, msg_user_not_found));
            };
            let mut u = auth.users[i];
            if f.s[1].is_empty() {
                return Err(err(3, msg_password_empty));
            }
            u.no_password = false;
            if !new_password(anchor, &mut u, f.s[1]) {
                return Err(err(14, msg_auth_busy));
            }
            stage_user(&mut anchor.auth_job, &u)
        }
        GrpcMethod::UserGrantRole | GrpcMethod::UserRevokeRole => {
            let Some(i) = auth.user(f.s[0]) else {
                return Err(err(9, msg_user_not_found));
            };
            let mut u = auth.users[i];
            if method == GrpcMethod::UserGrantRole {
                if auth.role(f.s[1]).is_none() {
                    return Err(err(9, msg_role_not_found));
                }
                if !u.grant(f.s[1]) {
                    return Err(err(8, msg_auth_busy));
                }
            } else {
                if enabled && f.s[0] == root && f.s[1] == root {
                    return Err(err(9, msg_invalid_auth_mgmt));
                }
                if !u.revoke(f.s[1]) {
                    return Err(err(9, msg_role_not_granted));
                }
            }
            stage_user(&mut anchor.auth_job, &u)
        }
        GrpcMethod::RoleAdd => {
            let Some(name) = Name::from(f.s[0]) else {
                return Err(err(3, msg_role_empty));
            };
            if auth.role(f.s[0]).is_some() {
                return Err(err(9, msg_role_exists));
            }
            if auth.free_role().is_none() {
                return Err(err(8, msg_auth_busy));
            }
            let mut r = Role::EMPTY;
            r.used = true;
            r.name = name;
            stage_role(&mut anchor.auth_job, &r)
        }
        GrpcMethod::RoleDelete => {
            if auth.role(f.s[0]).is_none() {
                return Err(err(9, msg_role_not_found));
            }
            if enabled && f.s[0] == root {
                return Err(err(9, msg_invalid_auth_mgmt));
            }
            let mut key = [0u8; 80];
            let Some(kl) = etcd_auth::record_key(b"r/", f.s[0], &mut key) else {
                return Err(err(3, msg_role_empty));
            };
            let mut ok = stage(&mut anchor.auth_job, &key[..kl], None);
            // The role leaves every user who held it.
            for i in 0..etcd_auth::USERS_MAX {
                let mut u = anchor.auth.users[i];
                if u.used && u.revoke(f.s[0]) {
                    ok &= stage_user(&mut anchor.auth_job, &u);
                }
            }
            ok
        }
        GrpcMethod::RoleGrantPermission | GrpcMethod::RoleRevokePermission => {
            let Some(i) = auth.role(f.s[0]) else {
                return Err(err(9, msg_role_not_found));
            };
            let mut r = auth.roles[i];
            if method == GrpcMethod::RoleGrantPermission {
                // Permission { Type permType = 1; bytes key = 2; bytes range_end = 3; }
                let Some(pf) = etcd_codec::auth_fields(f.s[1]) else {
                    return Err(err(3, msg_perm_not_granted));
                };
                let Some(p) = Perm::new(pf.v[0] as u8, pf.s[1], pf.s[2]) else {
                    return Err(err(3, msg_perm_not_granted));
                };
                if !r.grant(p) {
                    return Err(err(8, msg_auth_busy));
                }
            } else if !r.revoke(f.s[1], f.s[2]) {
                return Err(err(9, msg_perm_not_granted));
            }
            stage_role(&mut anchor.auth_job, &r)
        }
        _ => return Ok(Some(0)),
    };
    // Every change moves the auth revision, but enabling (etcd's
    // AuthEnable reads the revision; AuthDisable commits one).
    let rev = (anchor.auth.revision + 1).to_le_bytes();
    let bump = method != GrpcMethod::AuthEnable;
    if !staged || (bump && !stage(&mut anchor.auth_job, etcd_auth::KEY_REV, Some(&rev))) {
        return Err(err(8, msg_auth_busy));
    }
    if commit_auth(anchor, slot_idx, strm) {
        Ok(None)
    } else {
        Err(err(14, msg_auth_busy))
    }
}

/// Send the staged changes as one Txn to the auth keyspace.
fn commit_auth(anchor: &mut AnchorState, slot_idx: usize, strm: usize) -> bool {
    let mut txn = [0u8; 4000];
    let mut off = 0usize;
    let job = &anchor.auth_job;
    let mut p = 0usize;
    while p < job.len {
        let c = &job.changes[p..job.len];
        let kl = c[1] as usize;
        let vl = u16::from_le_bytes([c[2 + kl], c[3 + kl]]) as usize;
        let key = &c[2..2 + kl];
        let value = (c[0] == 1).then(|| &c[4 + kl..4 + kl + vl]);
        if etcd_codec::txn_success_op(&mut txn, &mut off, key, value).is_none() {
            return false;
        }
        p += 4 + kl + vl;
    }
    let mut body = [0u8; 4000];
    let Some(n) = etcd_codec::encode_txn_body(&txn[..off], &PUT_BITS, &mut body, 0) else {
        return false;
    };
    let corr = anchor.next_corr();
    if !send_kv_op_in(
        anchor,
        corr,
        slot_idx as u8,
        KV_OP_ETCD_TXN,
        &body[..n],
        AUTH_KEYSPACE,
    ) {
        return false;
    }
    let job = &mut anchor.auth_job;
    job.active = true;
    job.corr = corr;
    job.slot = slot_idx;
    job.strm = strm;
    job.stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    true
}

/// The auth change's Txn landed: into the cache, and answered.
fn auth_job_done(anchor: &mut AnchorState, result: u8, revision: i64) {
    anchor.auth_job.active = false;
    let (slot_idx, strm, stream_id) = (
        anchor.auth_job.slot,
        anchor.auth_job.strm,
        anchor.auth_job.stream_id,
    );
    let live = anchor.slots[slot_idx].conn_id != SLOT_FREE
        && anchor.slots[slot_idx].streams[strm].stream_id == stream_id;
    if result != KV_RESULT_ETCD_TXN {
        if live {
            send_grpc_error(anchor, slot_idx, stream_id, 13);
            anchor.slots[slot_idx].free_stream(strm);
        }
        return;
    }
    let mut p = 0usize;
    while p < anchor.auth_job.len {
        let mut key = [0u8; 256];
        let mut val = [0u8; 2200];
        let (put, kl, vl) = {
            let c = &anchor.auth_job.changes[p..anchor.auth_job.len];
            let kl = c[1] as usize;
            let vl = u16::from_le_bytes([c[2 + kl], c[3 + kl]]) as usize;
            key[..kl].copy_from_slice(&c[2..2 + kl]);
            val[..vl].copy_from_slice(&c[4 + kl..4 + kl + vl]);
            (c[0] == 1, kl, vl)
        };
        anchor.auth.apply(&key[..kl], put.then_some(&val[..vl]));
        // A deleted user's tokens go with them.
        if !put {
            if let Some(name) = key[..kl].strip_prefix(b"u/") {
                for t in anchor.tokens.iter_mut() {
                    if t.len != 0 && t.user.as_bytes() == name {
                        *t = Token::FREE;
                    }
                }
            }
        }
        p += 4 + kl + vl;
    }
    if live {
        let method = anchor.slots[slot_idx].streams[strm].method;
        write_response_frames(
            anchor,
            slot_idx,
            strm,
            stream_id,
            method,
            KV_RESULT_OK,
            revision.max(1),
            0,
            0,
        );
        anchor.slots[slot_idx].free_stream(strm);
    }
}

/// Answer the Maintenance and Cluster RPCs the anchor holds the answers
/// to: alarms, defragment, leadership, and the one member it is.
fn answer_member(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let method = anchor.slots[slot_idx].streams[strm].method;
    let (alarm, id) = {
        let s = &anchor.slots[slot_idx].streams[strm];
        match grpc_proto(&s.body_buf[..s.body_len]) {
            Some(p) => (
                etcd_codec::decode_alarm_request(p),
                etcd_codec::decode_field1_varint(p),
            ),
            None => (None, None),
        }
    };
    let (Some(alarm), Some(id)) = (alarm, id) else {
        send_grpc_error(anchor, slot_idx, stream_id, 3);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    };
    let me = anchor.member_id();
    // A refusal: its status and etcd's message for it.
    let mut m = [0u8; 96];
    let refusal = match method {
        GrpcMethod::MoveLeader if id != me => Some((9, msg_bad_transferee(&mut m))),
        GrpcMethod::MemberRemove | GrpcMethod::MemberUpdate | GrpcMethod::MemberPromote
            if id != me =>
        {
            Some((5, msg_member_not_found(&mut m)))
        }
        GrpcMethod::MemberRemove => Some((9, msg_last_member(&mut m))),
        GrpcMethod::MemberPromote => Some((9, msg_not_learner(&mut m))),
        GrpcMethod::MemberAdd | GrpcMethod::MemberUpdate => Some((9, msg_membership_graph(&mut m))),
        GrpcMethod::Downgrade => Some((9, msg_invalid_downgrade(&mut m))),
        _ => None,
    };
    if let Some((status, n)) = refusal {
        send_grpc_error_msg(anchor, slot_idx, stream_id, status, &m[..n]);
        anchor.slots[slot_idx].free_stream(strm);
        return;
    }
    // The alarms the response lists, staged where the builder reads them.
    let mut listed = 0usize;
    if method == GrpcMethod::Alarm {
        let (action, member, kind) = alarm;
        let member = if member == 0 { me } else { member };
        let mut out = [(0u64, 0u64); ALARMS_MAX];
        match action {
            // ACTIVATE: raised once per (member, alarm).
            1 if kind != 0 => {
                let n = anchor.alarm_n;
                #[allow(
                    clippy::manual_contains,
                    reason = "slice contains lowers to a routine the PIC link does not carry"
                )]
                let have = anchor.alarms[..n].iter().any(|a| *a == (member, kind));
                if !have && n < ALARMS_MAX {
                    anchor.alarms[n] = (member, kind);
                    anchor.alarm_n = n + 1;
                }
                out[0] = (member, kind);
                listed = 1;
            }
            // DEACTIVATE: lists the alarm it cleared.
            2 => {
                let n = anchor.alarm_n;
                if let Some(i) = anchor.alarms[..n].iter().position(|a| *a == (member, kind)) {
                    anchor.alarms[i] = anchor.alarms[n - 1];
                    anchor.alarm_n = n - 1;
                    out[0] = (member, kind);
                    listed = 1;
                }
            }
            // GET: every alarm raised.
            _ => {
                listed = anchor.alarm_n;
                out[..listed].copy_from_slice(&anchor.alarms[..listed]);
            }
        }
        let bb = &mut anchor.slots[slot_idx].streams[strm].body_buf;
        for (i, (mbr, k)) in out.iter().take(listed).enumerate() {
            bb[i * 16..i * 16 + 8].copy_from_slice(&mbr.to_le_bytes());
            bb[i * 16 + 8..i * 16 + 16].copy_from_slice(&k.to_le_bytes());
        }
    }
    let rev = anchor.last_rev.max(1);
    write_response_frames(
        anchor,
        slot_idx,
        strm,
        stream_id,
        method,
        KV_RESULT_OK,
        rev,
        listed * 16,
        0,
    );
    anchor.slots[slot_idx].free_stream(strm);
}

/// Route a lease control op (grant/keepalive/revoke) to the lease's
/// current manager through the session layer, so the lease — and its
/// live keepalive stream — can be handed off between managers without
/// expiring.
fn lease_session_ctrl(anchor: &mut AnchorState, ctrl: u8, lease_id: u64, ttl_ms: u32) {
    if ctrl == LEASE_CTRL_GRANT {
        let mut sink = lease_sink!(anchor);
        let _ = anchor.lsa.attach(CLASS_LEASE, lease_id, 0, 0, &mut sink);
    }
    let Some(sess) = anchor.lsa.find_app(CLASS_LEASE, lease_id) else {
        return;
    };
    // Body after the 20-byte session header (stamped by forward):
    //   [ctrl:1][lease_id:8][ttl_ms:4][tenant:4]
    let mut body = [0u8; sc::SESSION_HEADER + 1 + 8 + 4 + 4];
    let mut end = sc::SESSION_HEADER;
    body[end] = ctrl;
    end += 1;
    body[end..end + 8].copy_from_slice(&lease_id.to_le_bytes());
    end += 8;
    body[end..end + 4].copy_from_slice(&ttl_ms.to_le_bytes());
    end += 4;
    body[end..end + 4].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());
    end += 4;
    {
        let mut sink = lease_sink!(anchor);
        let _ = anchor
            .lsa
            .forward(sess, MSG_LEASE_CTRL, &body[..end], &mut sink);
    }
    if ctrl == LEASE_CTRL_REVOKE {
        let mut sink = lease_sink!(anchor);
        anchor.lsa.detach(sess, sc::DETACH_NORMAL, &mut sink);
    }
}

/// Drain one SessionCtrlV1 frame from lease manager `worker_idx` and
/// drive the lease handoff relay.
unsafe fn poll_lease_session(anchor: &mut AnchorState, worker_idx: u8) -> bool {
    let chan = if worker_idx == 0 {
        anchor.lease_sess0_in
    } else {
        anchor.lease_sess1_in
    };
    if chan < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let poll = ((*sys).channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    if ((*sys).channel_read)(chan, hdr.as_mut_ptr(), 3) < 3 {
        return false;
    }
    let mt = hdr[0];
    let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if len == 0 || len > anchor.scratch.len() {
        return len == 0;
    }
    if (((*sys).channel_read)(chan, anchor.scratch.as_mut_ptr(), len) as usize) < len {
        return false;
    }
    if (0x70..=0x9F).contains(&mt) {
        let mut tmp = [0u8; SCRATCH_BUF_SIZE];
        tmp[..len].copy_from_slice(&anchor.scratch[..len]);
        let mut sink = lease_sink!(anchor);
        let _ = anchor.lsa.on_frame(worker_idx, mt, &tmp[..len], &mut sink);
    }
    true
}

/// Drain one SessionCtrlV1 frame from watch_registry `worker_idx` and
/// drive the watch handoff relay. Watch events themselves flow on the
/// separate `watch_in` path; this is control-plane only.
unsafe fn poll_watch_session(anchor: &mut AnchorState, worker_idx: u8) -> bool {
    let chan = if worker_idx == 0 {
        anchor.watch_sess0_in
    } else {
        anchor.watch_sess1_in
    };
    if chan < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let poll = ((*sys).channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    if ((*sys).channel_read)(chan, hdr.as_mut_ptr(), 3) < 3 {
        return false;
    }
    let mt = hdr[0];
    let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if len == 0 || len > anchor.scratch.len() {
        return len == 0;
    }
    if (((*sys).channel_read)(chan, anchor.scratch.as_mut_ptr(), len) as usize) < len {
        return false;
    }
    if (0x70..=0x9F).contains(&mt) {
        let mut tmp = [0u8; SCRATCH_BUF_SIZE];
        tmp[..len].copy_from_slice(&anchor.scratch[..len]);
        let mut sink = watch_sink!(anchor);
        let _ = anchor.wsa.on_frame(worker_idx, mt, &tmp[..len], &mut sink);
    }
    true
}

/// Drain one relocation command and drive the lease handoff.
unsafe fn poll_relocate_lease(anchor: &mut AnchorState) -> bool {
    if anchor.relocate_in < 0 {
        return false;
    }
    let sys = anchor.syscalls;
    if sys.is_null() {
        return false;
    }
    let poll = ((*sys).channel_poll)(anchor.relocate_in, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return false;
    }
    let mut hdr = [0u8; 3];
    if ((*sys).channel_read)(anchor.relocate_in, hdr.as_mut_ptr(), 3) < 3 {
        return false;
    }
    let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if len == 0 || len > anchor.scratch.len() {
        return len == 0;
    }
    if (((*sys).channel_read)(anchor.relocate_in, anchor.scratch.as_mut_ptr(), len) as usize) < len
    {
        return false;
    }
    if hdr[0] == wire::MSG_SESSION_RELOCATE && len >= 1 {
        let target = anchor.scratch[0];
        let _ = anchor.lsa.relocate(target);
        let _ = anchor.wsa.relocate(target);
    }
    true
}

/// Drive the lease and watch handoff machines one tick.
fn lease_step(anchor: &mut AnchorState) {
    if anchor.lease_ctrl_out >= 0 {
        let mut sink = lease_sink!(anchor);
        let _ = anchor.lsa.step(&mut sink);
    }
    if anchor.watch_ctrl_out >= 0 {
        let mut sink = watch_sink!(anchor);
        let _ = anchor.wsa.step(&mut sink);
    }
}

// ── Lease reply path ──────────────────────────────────────────────────

/// Drain one `MSG_LEASE_STATE` envelope from `lease_in`, rendezvous
/// with the stream that issued the grant / revoke (via `lease_id`),
/// and emit the gRPC response frames. Returns `true` if an envelope
/// was consumed so the per-tick loop in `module_step` can keep
/// draining until the channel is empty.
///
/// Wire shape (`MSG_LEASE_STATE` payload, see `wire.rs`):
///   `[session_id:16][epoch:4][lease_id:8][status:1][ttl_ms:4]
///    [granted_at:8][keepalive_deadline:8]` = 49 bytes
///
/// `status = LEASE_STATE_GONE` is the manager's "lease no longer
/// exists" reply. For a grant that's an allocation failure → gRPC
/// INTERNAL (13); for a revoke it is the expected success path.
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
    // MSG_LEASE_STATE: [session_id:16][epoch:4][lease_id:8][status:1]
    //                  [ttl_ms:4][granted:8][deadline:8] = 49 bytes.
    const LEASE_STATE_LEN: usize = sc::SESSION_HEADER + 8 + 1 + 4 + 8 + 8;
    if payload_len < LEASE_STATE_LEN || payload_len > anchor.scratch.len() {
        return false;
    }
    let n2 = ((*sys).channel_read)(anchor.lease_in, anchor.scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    let at = sc::SESSION_HEADER;
    let lease_id = {
        let p = &anchor.scratch[at..at + 8];
        u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]])
    };
    let lease_status = anchor.scratch[at + 8];
    let ttl_from_mgr = u32::from_le_bytes([
        anchor.scratch[at + 9],
        anchor.scratch[at + 10],
        anchor.scratch[at + 11],
        anchor.scratch[at + 12],
    ]);

    let rd8 = |at: usize| -> u64 {
        let mut b = [0u8; 8];
        b.copy_from_slice(&anchor.scratch[at..at + 8]);
        u64::from_le_bytes(b)
    };
    let deadline = rd8(at + 21);
    // The manager's clock, where it sends it.
    let now = if payload_len >= LEASE_STATE_LEN + 8 {
        rd8(at + 29)
    } else {
        deadline
    };

    let Some((slot_idx, strm)) = anchor.find_lease_inflight(lease_id) else {
        return true; // unmatched / late arrival — keep draining
    };
    let method = anchor.slots[slot_idx].streams[strm].method;
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;

    match method {
        GrpcMethod::LeaseRevoke => {
            // The lease is gone; its keys go before the answer.
            anchor.slots[slot_idx].streams[strm].pending_lease_id = 0;
            anchor.slots[slot_idx].streams[strm].body_buf[0..8]
                .copy_from_slice(&lease_id.to_le_bytes());
            stream_lease_op(anchor, slot_idx, strm, KV_OP_LEASE_REVOKE, lease_id);
            return true;
        }
        GrpcMethod::LeaseTimeToLive => {
            anchor.slots[slot_idx].streams[strm].pending_lease_id = 0;
            let keys = anchor.slots[slot_idx].streams[strm].body_buf[0] != 0;
            if lease_status == wire::LEASE_STATE_GONE {
                answer_lease_ttl(anchor, slot_idx, strm, lease_id, -1, 0);
            } else {
                let ttl_s = (deadline.saturating_sub(now) / 1000) as i64;
                let granted_s = i64::from(ttl_from_mgr / 1000);
                if keys {
                    stash_lease_ttl(anchor, slot_idx, strm, lease_id, ttl_s, granted_s);
                    let s = &mut anchor.slots[slot_idx].streams[strm];
                    s.body_buf[24..26].copy_from_slice(&0u16.to_le_bytes());
                    s.body_len = 26;
                    request_lease_keys(anchor, slot_idx, strm, lease_id, &[]);
                } else {
                    answer_lease_ttl(anchor, slot_idx, strm, lease_id, ttl_s, granted_s);
                }
            }
            return true;
        }
        _ => {}
    }

    // status GONE means the manager has no such lease: for grant that's
    // an allocation failure (gRPC INTERNAL); for revoke / keepalive the
    // lease is simply gone, which the encoder maps to ttl 0.
    let grpc_status: u8 = match (method, lease_status) {
        (GrpcMethod::LeaseGrant, wire::LEASE_STATE_GONE) => 13,
        _ => 0,
    };
    // A granted lease is written to its record before it is answered, so
    // a restart re-grants every lease a client was told it holds.
    if method == GrpcMethod::LeaseGrant && grpc_status == 0 {
        anchor.slots[slot_idx].streams[strm].pending_lease_id = 0;
        let ttl_ms = if ttl_from_mgr != 0 {
            ttl_from_mgr
        } else {
            let bb = &anchor.slots[slot_idx].streams[strm].body_buf;
            u32::from_le_bytes([bb[0], bb[1], bb[2], bb[3]])
        };
        let bb = &mut anchor.slots[slot_idx].streams[strm].body_buf;
        bb[8] = GRANT_PERSISTING;
        bb[16..24].copy_from_slice(&lease_id.to_le_bytes());
        bb[24..28].copy_from_slice(&ttl_ms.to_le_bytes());
        let corr = anchor.next_corr();
        if send_lease_record(anchor, corr, slot_idx as u8, lease_id, Some(ttl_ms)) {
            anchor.slots[slot_idx].streams[strm].pending_corr = corr;
        } else {
            lease_session_ctrl(anchor, LEASE_CTRL_REVOKE, lease_id, 0);
            send_grpc_error(anchor, slot_idx, stream_id, 14);
            anchor.slots[slot_idx].free_stream(strm);
        }
        return true;
    }

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
    // Build the protobuf payload into scratch. A lease op's header names
    // the store's current revision, as etcd's does.
    let ids = anchor.header_ids();
    let rev = anchor.last_rev.max(1);
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
                        rev,
                        lease_id as i64,
                        ttl_sec,
                    )
                    .is_some()
                }
                GrpcMethod::LeaseRevoke => {
                    build_lease_revoke_response(proto_buf, &mut proto_off, rev).is_some()
                }
                GrpcMethod::LeaseKeepAlive => {
                    // Manager echoes ttl_ms; convert to seconds.
                    // ttl=0 from manager means "no such lease" — propagate
                    // (etcd clients treat ttl=0 as a revoke signal).
                    let ttl_sec = (ttl_from_mgr / 1000) as i64;
                    build_lease_keepalive_response(
                        proto_buf,
                        &mut proto_off,
                        rev,
                        lease_id as i64,
                        ttl_sec,
                    )
                    .is_some()
                }
                _ => false,
            };
            // Who answers: the builders wrote a placeholder identity.
            let stamped = if ok {
                etcd_codec::stamp_header(&mut scratch[GRPC_FRAME_HEADER_LEN..], proto_off, &ids)
            } else {
                None
            };
            if let Some(proto_off) = stamped {
                // Back-patch the gRPC message header
                // [compressed:1][length:4 BE] over the reserved bytes.
                scratch[0] = 0;
                scratch[1] = ((proto_off >> 24) & 0xFF) as u8;
                scratch[2] = ((proto_off >> 16) & 0xFF) as u8;
                scratch[3] = ((proto_off >> 8) & 0xFF) as u8;
                scratch[4] = (proto_off & 0xFF) as u8;
                GRPC_FRAME_HEADER_LEN + proto_off
            } else {
                0
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
    let (
        op,
        key_buf,
        key_len,
        cancel_id,
        want_id,
        re_buf,
        re_len,
        filters,
        progress,
        start,
        prev_kv,
    ) = {
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
        let mut rb = [0u8; 256];
        let rl = req.range_end.len().min(rb.len());
        rb[..rl].copy_from_slice(&req.range_end[..rl]);
        // A key or range the hub cannot hold whole is refused, never
        // watched truncated (a cut key watches a different key).
        let fits = req.key.len() + req.range_end.len() <= WATCH_KEY_MAX;
        (
            req.op,
            kb,
            if fits { kl } else { usize::MAX },
            req.cancel_watch_id,
            req.create_watch_id,
            rb,
            rl,
            req.filters,
            req.progress_notify,
            req.start_revision,
            req.prev_kv,
        )
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
            // With auth on, a watch needs read permission on its range.
            if anchor.auth.enabled && key_len != usize::MAX {
                let allowed = match stream_user(anchor, slot_idx, strm) {
                    Ok(Some(u)) => {
                        anchor
                            .auth
                            .permitted(u, &key_buf[..key_len], &re_buf[..re_len], NEED_READ)
                    }
                    _ => false,
                };
                if !allowed {
                    // etcd's refusal: created and canceled at once, as
                    // watch -1.
                    let mut m = [0u8; 96];
                    let n = msg_permission_denied(&mut m);
                    send_watch_response_full(
                        anchor,
                        slot_idx,
                        strm,
                        stream_id,
                        -1,
                        true,
                        true,
                        None,
                        0,
                        &m[..n],
                    );
                    return;
                }
            }
            // The client's own id when it names one (etcd clients may),
            // else the session id; either way unique on this stream.
            let wid = anchor.next_watch_id();
            let client = if want_id != 0 { want_id } else { wid };
            let taken = anchor.slots[slot_idx].streams[strm]
                .watches
                .iter()
                .any(|w| w.app != 0 && w.client == client);
            let free = anchor.slots[slot_idx].streams[strm]
                .watches
                .iter()
                .position(|w| w.app == 0);
            let Some(free) = free.filter(|_| !taken && key_len != usize::MAX) else {
                // A duplicate id, or the stream's watches are all in use:
                // refused as upstream refuses — created and canceled at once.
                send_watch_response(anchor, slot_idx, strm, stream_id, client, true, true, None);
                return;
            };
            anchor.slots[slot_idx].streams[strm].watches[free] = WatchEnt {
                app: wid,
                client,
                prev_kv,
            };
            let app = wid as u64;
            // The watch is a session. ATTACH it on the active registry;
            // forward holds the DEFINE until ATTACHED lands, then the
            // registry admits it. A handoff can later move the watch to
            // the standby registry without breaking the stream.
            {
                let mut sink = watch_sink!(anchor);
                let _ = anchor
                    .wsa
                    .attach(CLASS_WATCH, app, slot_idx as u16, stream_id, &mut sink);
            }
            let Some(sess) = anchor.wsa.find_app(CLASS_WATCH, app) else {
                send_watch_response(anchor, slot_idx, strm, stream_id, client, true, false, None);
                return;
            };
            // MSG_WATCH_CTRL define body, after the 20-byte session header
            // (stamped by forward):
            //   [ctrl=DEFINE:1][watch_id:8][tenant:4][start_rev:8]
            //   [filters:1][progress:1][key_len:2][key][re_len:2]
            let mut body = [0u8; 384];
            let mut end = sc::SESSION_HEADER;
            let k = key_len;
            body[end] = WATCH_CTRL_DEFINE;
            end += 1;
            body[end..end + 8].copy_from_slice(&(wid as u64).to_le_bytes());
            end += 8;
            body[end..end + 4].copy_from_slice(&DEFAULT_TENANT.to_le_bytes());
            end += 4;
            // etcd's start_revision, as the client gave it: INCLUSIVE
            // ("from revision N"), 0 = from now. The registry turns it
            // into the hub's last-seen form.
            body[end..end + 8].copy_from_slice(&start.max(0).to_le_bytes());
            end += 8;
            body[end] = filters;
            end += 1;
            body[end] = progress as u8;
            end += 1;
            body[end..end + 2].copy_from_slice(&(k as u16).to_le_bytes());
            end += 2;
            body[end..end + k].copy_from_slice(&key_buf[..k]);
            end += k;
            body[end..end + 2].copy_from_slice(&(re_len as u16).to_le_bytes());
            end += 2;
            body[end..end + re_len].copy_from_slice(&re_buf[..re_len]);
            end += re_len;
            {
                let mut sink = watch_sink!(anchor);
                let _ = anchor
                    .wsa
                    .forward(sess, MSG_WATCH_CTRL, &body[..end], &mut sink);
            }
            send_watch_response(anchor, slot_idx, strm, stream_id, client, true, false, None);
        }
        Some(WatchOp::Cancel) => {
            // One watch ends; the stream — and its other watches — stay.
            let at = anchor.slots[slot_idx].streams[strm]
                .watches
                .iter()
                .position(|w| w.app != 0 && w.client == cancel_id);
            if let Some(i) = at {
                drop_watch(anchor, slot_idx, strm, i);
            }
            send_watch_response(
                anchor, slot_idx, strm, stream_id, cancel_id, false, true, None,
            );
        }
        // A progress request: the revision every watch on the stream has
        // reached — a response with no events, not created nor canceled
        // (etcd's WatchResponse.IsProgressNotify), watch id -1.
        Some(WatchOp::Progress) => {
            send_watch_response(anchor, slot_idx, strm, stream_id, -1, false, false, None);
        }
        None => {}
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
    event: Option<WatchEv<'_>>,
) {
    send_watch_response_full(
        anchor, slot_idx, strm, stream_id, watch_id, created, canceled, event, 0, b"",
    );
}

/// As [`send_watch_response`], with a canceled watch's compaction revision
/// and reason.
#[allow(
    clippy::too_many_arguments,
    reason = "one WatchResponse: its stream, its head fields and its event"
)]
fn send_watch_response_full(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    stream_id: u32,
    watch_id: i64,
    created: bool,
    canceled: bool,
    event: Option<WatchEv<'_>>,
    compact_revision: i64,
    cancel_reason: &[u8],
) {
    // The header's revision: an event's own, else the newest the anchor
    // has seen (at least 1: 0 reads as "no header").
    let header_rev = match event {
        Some(ev) => ev.meta[1] as i64,
        None => anchor.last_rev.max(1),
    };
    let ids = anchor.header_ids();
    let data_len = encode_watch_response(
        &mut anchor.resp_out,
        &ids,
        header_rev,
        watch_id,
        created,
        canceled,
        event,
        compact_revision,
        cancel_reason,
    );
    send_encoded_watch(anchor, slot_idx, strm, stream_id, data_len);
}

/// A `WatchResponse` encoded into `resp_out` as one gRPC message: its
/// length (0 when it does not encode).
#[allow(
    clippy::too_many_arguments,
    reason = "one WatchResponse: its head fields and its event"
)]
fn encode_watch_response(
    resp_out: &mut [u8],
    ids: &etcd_codec::HeaderIds,
    header_rev: i64,
    watch_id: i64,
    created: bool,
    canceled: bool,
    event: Option<WatchEv<'_>>,
    compact_revision: i64,
    cancel_reason: &[u8],
) -> usize {
    // Encode protobuf into scratch (after 5-byte gRPC frame header).
    {
        let scratch = resp_out;
        if scratch.len() < GRPC_FRAME_HEADER_LEN {
            0
        } else {
            let mut proto_off: usize = 0;
            let proto_buf = &mut scratch[GRPC_FRAME_HEADER_LEN..];
            let ok = build_watch_response_header_full(
                proto_buf,
                &mut proto_off,
                header_rev,
                watch_id,
                created,
                canceled,
                compact_revision,
                cancel_reason,
            )
            .is_some();
            let ok = if ok {
                if let Some(ev) = event {
                    etcd_codec::write_watch_event_full(
                        proto_buf,
                        &mut proto_off,
                        ev.kind,
                        ev.key,
                        ev.value,
                        ev.meta,
                        ev.prev,
                    )
                    .is_some()
                } else {
                    true
                }
            } else {
                false
            };
            let stamped = if ok {
                etcd_codec::stamp_header(&mut scratch[GRPC_FRAME_HEADER_LEN..], proto_off, ids)
            } else {
                None
            };
            if let Some(proto_off) = stamped {
                scratch[0] = 0;
                scratch[1] = ((proto_off >> 24) & 0xFF) as u8;
                scratch[2] = ((proto_off >> 16) & 0xFF) as u8;
                scratch[3] = ((proto_off >> 8) & 0xFF) as u8;
                scratch[4] = (proto_off & 0xFF) as u8;
                GRPC_FRAME_HEADER_LEN + proto_off
            } else {
                0
            }
        }
    }
}

/// Write the `WatchResponse` encoded in `resp_out[..data_len]` as HTTP/2
/// frames on the open stream (HEADERS first, once).
fn send_encoded_watch(
    anchor: &mut AnchorState,
    slot_idx: usize,
    strm: usize,
    stream_id: u32,
    data_len: usize,
) {
    if data_len == 0 {
        // A response that could not be encoded is a fault, counted —
        // never a silent gap in the stream.
        anchor.m_net_errors += 1;
        return;
    }

    let copy_len = data_len;
    let need = 2 * FRAME_HEADER_LEN + 64 + copy_len;
    if anchor.slots[slot_idx].send_len + need > SEND_BUF_SIZE {
        // The client is not draining its stream: the event cannot be
        // queued, and a watch that silently skips one would lie.
        anchor.m_net_errors += 1;
        // Its connection closes; etcd's clients reconnect and resume each
        // watch from the last revision they saw.
        let last = anchor.slots[slot_idx].last_stream_id;
        let _ = send_goaway(
            &mut anchor.slots[slot_idx],
            last,
            etcd_codec::ERROR_ENHANCE_YOUR_CALM,
        );
        anchor.slots[slot_idx].phase = SlotPhase::Closing;
        return;
    }

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
        &anchor.resp_out[..copy_len],
    );
}

/// End watch `i` of a stream: its registry session detached (no more
/// frames fan to it) and its entry freed.
fn drop_watch(anchor: &mut AnchorState, slot_idx: usize, strm: usize, i: usize) {
    let app = anchor.slots[slot_idx].streams[strm].watches[i].app;
    if app != 0 {
        if let Some(sess) = anchor.wsa.find_app(CLASS_WATCH, app as u64) {
            let mut sink = watch_sink!(anchor);
            anchor.wsa.detach(sess, sc::DETACH_NORMAL, &mut sink);
        }
    }
    anchor.slots[slot_idx].streams[strm].watches[i] = WatchEnt::FREE;
}

/// End every watch a stream holds — reset by the client, or its
/// connection gone.
fn drop_stream_watches(anchor: &mut AnchorState, slot_idx: usize, strm: usize) {
    for i in 0..WATCHES_PER_STREAM {
        drop_watch(anchor, slot_idx, strm, i);
    }
}

/// End every watch a connection's streams hold.
fn drop_slot_watches(anchor: &mut AnchorState, slot_idx: usize) {
    for strm in 0..MAX_STREAMS_PER_CONN {
        drop_stream_watches(anchor, slot_idx, strm);
    }
}

/// Drain one `MSG_WATCH_FRAME` envelope and push it as a DATA frame
/// on the stream tracking that `watch_id`.
///
/// `MSG_WATCH_FRAME` is anchor-agnostic — its body is the protocol-
/// native frame (an etcd v3 `WatchResponse` here). The body starts
/// with `[watch_id:u64 LE]` so we can rendezvous; the rest is the
/// already-encoded protobuf payload.
unsafe fn poll_watch_in(anchor: &mut AnchorState) -> bool {
    // An event being resolved holds the ones behind it.
    if anchor.watch_in < 0 || anchor.wlg.corr != 0 {
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
    if payload_len < MIN_WATCH_FRAME_LEN || payload_len > anchor.reply.len() {
        return false;
    }
    let n2 = ((*sys).channel_read)(anchor.watch_in, anchor.reply.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return false;
    }

    // Wire shape (per `watch_registry::handle_mutation`):
    //   [watch_id:8][kpg:2][rev:8][op:1][klen:2][k][vlen:2][v]
    // We copy the relevant slices into stack buffers before mutating
    // `anchor.reply` for the protobuf encode below.
    let watch_id = i64::from_le_bytes([
        anchor.reply[0],
        anchor.reply[1],
        anchor.reply[2],
        anchor.reply[3],
        anchor.reply[4],
        anchor.reply[5],
        anchor.reply[6],
        anchor.reply[7],
    ]);
    let _kpg = u16::from_le_bytes([anchor.reply[8], anchor.reply[9]]);
    let revision = i64::from_le_bytes([
        anchor.reply[10],
        anchor.reply[11],
        anchor.reply[12],
        anchor.reply[13],
        anchor.reply[14],
        anchor.reply[15],
        anchor.reply[16],
        anchor.reply[17],
    ]);
    let op_byte = anchor.reply[18];
    anchor.last_rev = anchor.last_rev.max(revision);
    // A compaction refusal: the watch's replay window fell behind the
    // compaction floor. Cancel the client stream with the compact
    // revision rather than resuming it short.
    if op_byte == WATCH_FRAME_OP_COMPACTED {
        // That watch ends canceled; the stream carries on for the others.
        if let Some((slot_idx, strm, client)) = anchor.find_watch_stream(watch_id) {
            let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
            if let Some(i) = anchor.slots[slot_idx].streams[strm]
                .watches
                .iter()
                .position(|w| w.app == watch_id)
            {
                drop_watch(anchor, slot_idx, strm, i);
            }
            // What etcd says: canceled, with the compaction revision
            // (ErrCompacted to the client) and upstream's reason.
            send_watch_response_full(
                anchor,
                slot_idx,
                strm,
                stream_id,
                client,
                false,
                true,
                None,
                revision,
                b"mvcc: required revision has been compacted",
            );
        }
        return true;
    }
    let key_len = u16::from_le_bytes([anchor.reply[19], anchor.reply[20]]) as usize;
    let key_off = 21;
    if key_off + key_len + 2 > payload_len {
        return false;
    }
    let vlen_off = key_off + key_len;
    let value_len =
        u16::from_le_bytes([anchor.reply[vlen_off], anchor.reply[vlen_off + 1]]) as usize;
    let value_off = vlen_off + 2;
    if value_off + value_len > payload_len {
        return false;
    }

    // A large value's manifest — the event's value, or the value it
    // replaced: fetched first, then the event is delivered whole.
    let prev_manifest = anchor
        .reply
        .get(value_off + value_len + 24)
        .is_some_and(|&m| m == types::JOURNAL_PREV_MANIFEST);
    if op_byte == types::KV_OP_PUT_MANIFEST || prev_manifest {
        wlg_begin(anchor, payload_len);
        return true;
    }
    // Copy key/value out before reusing scratch for proto encode: whole
    // (a key or value longer than the store holds is not an event).
    let mut key_buf = [0u8; STORE_KEY_MAX];
    let mut val_buf = [0u8; 4096];
    if key_len > key_buf.len() || value_len > val_buf.len() {
        return false;
    }
    let key_clip = key_len;
    let val_clip = value_len;
    key_buf[..key_clip].copy_from_slice(&anchor.reply[key_off..key_off + key_clip]);
    val_buf[..val_clip].copy_from_slice(&anchor.reply[value_off..value_off + val_clip]);

    // Map the worker's KV_OP_* op byte onto the etcd `mvccpb.EventType`.
    let delete = op_byte == KV_OP_DELETE;
    let event_kind = if delete {
        WatchEventKind::Delete
    } else {
        WatchEventKind::Put
    };
    // The metadata tail (`MSG_WATCH_EVENT`): the key's create revision,
    // version and lease, and the version the event replaced. A frame
    // without it (a replayed version) reads as a put's first version.
    let rd8 = |b: &[u8], at: usize| -> u64 {
        b.get(at..at + 8)
            .and_then(|x| x.try_into().ok())
            .map_or(0, u64::from_le_bytes)
    };
    let rev = revision as u64;
    let tail = value_off + value_len;
    let mut meta = if delete {
        [0, rev, 0, 0]
    } else {
        [rev, rev, 1, 0]
    };
    let mut prev_meta = [0u64; 4];
    let mut prev_len = 0usize;
    let mut has_prev = false;
    if tail + 25 <= payload_len {
        let t = &anchor.reply[tail..payload_len];
        if !delete {
            meta = [rd8(t, 0), rev, rd8(t, 8), rd8(t, 16)];
        }
        if t[24] == 1 && t.len() >= 27 {
            let pl = u16::from_le_bytes([t[25], t[26]]) as usize;
            if 27 + pl + 32 <= t.len() && pl <= anchor.response_data.len() {
                anchor.response_data[..pl].copy_from_slice(&t[27..27 + pl]);
                let m = 27 + pl;
                prev_meta = [rd8(t, m), rd8(t, m + 8), rd8(t, m + 16), rd8(t, m + 24)];
                prev_len = pl;
                has_prev = true;
            }
        }
    }

    let Some((slot_idx, strm, client)) = anchor.find_watch_stream(watch_id) else {
        return true;
    };
    let stream_id = anchor.slots[slot_idx].streams[strm].stream_id;
    let wants_prev = anchor.slots[slot_idx].streams[strm]
        .watches
        .iter()
        .any(|w| w.app == watch_id && w.prev_kv);
    let mut prev_buf = [0u8; STREAM_BODY_MAX];
    prev_buf[..prev_len].copy_from_slice(&anchor.response_data[..prev_len]);

    send_watch_response(
        anchor,
        slot_idx,
        strm,
        stream_id,
        client,
        false,
        false,
        Some(WatchEv {
            kind: event_kind,
            key: &key_buf[..key_clip],
            value: &val_buf[..val_clip],
            meta,
            prev: (wants_prev && has_prev).then(|| (&prev_buf[..prev_len], prev_meta)),
        }),
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
    let frame_conn = if payload_end - payload_start >= CONN_ID_LEN {
        Some(conn_id(&anchor.scratch[payload_start..payload_end]))
    } else {
        None
    };

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
            } else if anchor.phase == AnchorPhase::WaitBound && payload_len >= CONN_ID_LEN {
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
            if payload_len > CONN_ID_LEN {
                let Some(conn_id) = frame_conn else {
                    return true;
                };
                let data_start = payload_start + CONN_ID_LEN;
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
                    drop_slot_watches(anchor, idx);
                    anchor.free_slot(idx);
                    dev_log(&*sys, 3, b"[etcd_anc] client closed".as_ptr(), 24);
                }
            }
        }
        NET_MSG_ERROR => {
            // MSG_ERROR is BROADCAST to every anchor sharing linux_net's
            // net_out, so it also carries errors for conns this anchor
            // does not own — notably peer_router's outbound-dial failures
            // on a Raft-dialing node. Reacting to a foreign error would
            // stop this anchor from serving on that node, so only an
            // error for a conn this anchor owns is handled: free that
            // slot and keep listening.
            if let Some(conn_id) = frame_conn {
                if let Some(idx) = anchor.find_slot(conn_id) {
                    drop_slot_watches(anchor, idx);
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
        CONN_ID_LEN,
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
    let payload_len = CONN_ID_LEN + data.len();
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
        scratch.add(NET_FRAME_HDR + CONN_ID_LEN),
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
        // Out in NET-frame-sized chunks, as far as the channel takes
        // them; what it refuses waits for the next step.
        // net_send_data_pic_from_frame stages through anchor.scratch, so
        // each chunk is copied to frame_payload first (idle between
        // poll_net_in and the next process_one_frame).
        const NET_CHUNK: usize = SCRATCH_BUF_SIZE - NET_FRAME_HDR - CONN_ID_LEN;
        let mut sent = 0usize;
        while sent < send_len {
            let n = (send_len - sent).min(NET_CHUNK);
            {
                let (src, dst) = split_send_borrow(anchor, i);
                dst[..n].copy_from_slice(&src[sent..sent + n]);
            }
            if !net_send_data_pic_from_frame(anchor, conn_id, n) {
                break;
            }
            sent += n;
        }
        if sent > 0 {
            // What was not sent moves to the front.
            let buf = &mut anchor.slots[i].send_buf;
            #[allow(
                clippy::needless_range_loop,
                reason = "a shift within one buffer; copy_within is not in the PIC link"
            )]
            for k in 0..send_len - sent {
                buf[k] = buf[k + sent];
            }
            anchor.slots[i].send_len = send_len - sent;
        }
        if phase == SlotPhase::Closing && anchor.slots[i].send_len == 0 {
            let _ = net_send_close_pic(anchor, conn_id);
            drop_slot_watches(anchor, i);
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
    let payload_len = CONN_ID_LEN + data_len;
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
        scratch.add(NET_FRAME_HDR + CONN_ID_LEN),
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
    anchor.anchor_id = make_anchor_id(PROTO_ETCD, anchor.listen_port, 0);

    unsafe {
        let sys = &*sys_ptr;
        anchor.routed_in = dev_channel_port(sys, 0, 1);
        anchor.watch_in = dev_channel_port(sys, 0, 2);
        anchor.lease_in = dev_channel_port(sys, 0, 3);
        anchor.lease_sess0_in = dev_channel_port(sys, 0, 4);
        anchor.lease_sess1_in = dev_channel_port(sys, 0, 5);
        anchor.relocate_in = dev_channel_port(sys, 0, 6);
        anchor.watch_sess0_in = dev_channel_port(sys, 0, 7);
        anchor.watch_sess1_in = dev_channel_port(sys, 0, 8);
        anchor.lease_revoke_in = dev_channel_port(sys, 0, 9);
        anchor.kv_out = dev_channel_port(sys, 1, 1);
        anchor.watch_ctrl_out = dev_channel_port(sys, 1, 2);
        anchor.lease_ctrl_out = dev_channel_port(sys, 1, 3);
        anchor.lease_ctrl2_out = dev_channel_port(sys, 1, 4);
        anchor.metrics_out = dev_channel_port(sys, 1, 5);
        anchor.watch_ctrl2_out = dev_channel_port(sys, 1, 6);
    }
    // Lease + watch session layers: worker 0 is the active target,
    // worker 1 the standby a handoff moves sessions onto.
    {
        let lwired = [anchor.lease_ctrl_out >= 0, anchor.lease_ctrl2_out >= 0];
        anchor.lsa.init(anchor.anchor_id, lwired, false);
        let mut sink = lease_sink!(anchor);
        anchor.lsa.hello(&mut sink);
    }
    {
        let wwired = [anchor.watch_ctrl_out >= 0, anchor.watch_ctrl2_out >= 0];
        anchor.wsa.init(anchor.anchor_id, wwired, false);
        let mut sink = watch_sink!(anchor);
        anchor.wsa.hello(&mut sink);
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
            let ls0 = poll_lease_session(anchor, 0);
            let ls1 = poll_lease_session(anchor, 1);
            let ws0 = poll_watch_session(anchor, 0);
            let ws1 = poll_watch_session(anchor, 1);
            let rel = poll_relocate_lease(anchor);
            let ended = poll_lease_revoke(anchor);
            if !net && !routed && !lease && !watch && !ls0 && !ls1 && !ws0 && !ws1 && !rel && !ended
            {
                break;
            }
            budget -= 1;
        }
        lease_step(anchor);
        lg_step(anchor);
        serve_lease_end(anchor);
        serve_snapshot(anchor);
        serve_record_load(anchor, LOAD_AUTH);
        serve_record_load(anchor, LOAD_LEASES);
        flush_slots(anchor);

        // Emit module-scope counters on `metrics_out` at a
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
