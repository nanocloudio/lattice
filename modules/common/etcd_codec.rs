//! Pure-logic etcd-v3 wire codec — HTTP/2 + HPACK + gRPC framing +
//! the protobuf subset Lattice needs to translate `etcdserverpb.KV`
//! requests into `KV_OP_*` envelopes for `kv_state_worker`.
//!
//! Mirrors `modules/common/redis_codec.rs` and `memcached_codec.rs`:
//! same source compiles into the no_std PIC `etcd_edge_anchor` module
//! AND is `#[path]`-mounted into the host integration tests, so a
//! regression in any of the layers below is caught by `cargo test`
//! before any module ELF runs.
//!
//! ## Layering
//!
//! ```text
//!  TCP byte stream
//!   │
//!   ├── HTTP/2 connection preface  ("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
//!   │
//!   ├── HTTP/2 frames               (this file: `frame.rs`-flavoured §)
//!   │     DATA / HEADERS / SETTINGS / PING / GOAWAY / WINDOW_UPDATE
//!   │
//!   ├── HPACK header blocks         (static table only — §)
//!   │     :method, :path, :scheme, :authority, content-type, te
//!   │
//!   ├── gRPC frames                 (5-byte prefix + protobuf body)
//!   │     [compressed:u8][length:u32 BE][message bytes]
//!   │
//!   └── etcdserverpb.KV.{Range,Put,DeleteRange}  (protobuf subset)
//!         → KV_OP_GET / KV_OP_PUT / KV_OP_DELETE bodies for the
//!           shared `kv_state_worker`.
//! ```
//!
//! ## Phase status
//!
//! This file lands in slices. The current slice (the **frame layer**)
//! ships the HTTP/2 connection preface check and a frame-header
//! parser / serializer with typed payload accessors. HPACK, gRPC, and
//! the protobuf KV subset land in follow-up commits, each behind its
//! own `mod` section below.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC module and host tests; each consumer uses a subset"
)]

// ── HTTP/2 connection preface ─────────────────────────────────────────
//
// RFC 7540 §3.5: every client connection starts with the literal byte
// sequence
//   "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
// (24 bytes). Servers MUST receive it before processing any frames.

pub const HTTP2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
pub const HTTP2_PREFACE_LEN: usize = 24;

/// Returns `true` if `buf` starts with the HTTP/2 connection preface.
/// Caller is expected to advance past `HTTP2_PREFACE_LEN` bytes on a
/// match, then begin frame-level processing.
pub fn preface_matches(buf: &[u8]) -> bool {
    buf.len() >= HTTP2_PREFACE_LEN && &buf[..HTTP2_PREFACE_LEN] == HTTP2_PREFACE
}

// ── HTTP/2 frame layer ────────────────────────────────────────────────
//
// RFC 7540 §4.1: every frame begins with a fixed 9-byte header:
//
//   +-----------------------------------------------+
//   |                 Length (24)                   |
//   +---------------+---------------+---------------+
//   |   Type (8)    |   Flags (8)   |
//   +-+-------------+---------------+-------------------------------+
//   |R|                 Stream Identifier (31)                      |
//   +=+=============================================================+
//   |                   Frame Payload (0...)                      ...
//   +---------------------------------------------------------------+
//
// All multi-byte integers are big-endian. The R bit is reserved and
// MUST be zero on send, ignored on receive (RFC 7540 §4.1).

pub const FRAME_HEADER_LEN: usize = 9;

/// Frame type codes (RFC 7540 §11.2). Only the subset Lattice
/// implements is named; the rest are passed through as `Other`.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FrameType {
    Data = 0x0,
    Headers = 0x1,
    Priority = 0x2,
    RstStream = 0x3,
    Settings = 0x4,
    PushPromise = 0x5,
    Ping = 0x6,
    Goaway = 0x7,
    WindowUpdate = 0x8,
    Continuation = 0x9,
}

impl FrameType {
    pub fn from_u8(byte: u8) -> Option<Self> {
        Some(match byte {
            0x0 => Self::Data,
            0x1 => Self::Headers,
            0x2 => Self::Priority,
            0x3 => Self::RstStream,
            0x4 => Self::Settings,
            0x5 => Self::PushPromise,
            0x6 => Self::Ping,
            0x7 => Self::Goaway,
            0x8 => Self::WindowUpdate,
            0x9 => Self::Continuation,
            _ => return None,
        })
    }
}

// Frame flags (RFC 7540 §6) — values overload across frame types, so
// we expose the constants as raw u8.
pub const FLAG_END_STREAM: u8 = 0x01; // DATA, HEADERS
pub const FLAG_ACK: u8 = 0x01; // SETTINGS, PING
pub const FLAG_END_HEADERS: u8 = 0x04; // HEADERS, PUSH_PROMISE, CONTINUATION
pub const FLAG_PADDED: u8 = 0x08; // DATA, HEADERS, PUSH_PROMISE
pub const FLAG_PRIORITY: u8 = 0x20; // HEADERS

/// Parsed frame header. The `payload` slice references the *body* of
/// the frame (i.e. starting after the 9-byte header), bounded by
/// `length`.
#[derive(Clone, Copy, Debug)]
pub struct FrameHeader {
    pub length: u32,
    pub frame_type: u8,
    pub flags: u8,
    pub stream_id: u32,
}

impl FrameHeader {
    /// Parse a frame header out of `buf`. Returns `None` if `buf` is
    /// too short. The reserved R bit on the stream identifier is
    /// masked off per RFC 7540 §4.1.
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < FRAME_HEADER_LEN {
            return None;
        }
        let length = (u32::from(buf[0]) << 16) | (u32::from(buf[1]) << 8) | u32::from(buf[2]);
        let frame_type = buf[3];
        let flags = buf[4];
        let stream_id = ((u32::from(buf[5]) << 24)
            | (u32::from(buf[6]) << 16)
            | (u32::from(buf[7]) << 8)
            | u32::from(buf[8]))
            & 0x7FFF_FFFF;
        Some(Self {
            length,
            frame_type,
            flags,
            stream_id,
        })
    }

    /// Total bytes consumed by this frame (header + payload). Saturates
    /// on overflow rather than wrapping; callers should bail with a
    /// FRAME_SIZE_ERROR if the result exceeds their max-frame-size
    /// setting.
    pub fn total_len(&self) -> usize {
        FRAME_HEADER_LEN.saturating_add(self.length as usize)
    }

    /// True iff `flags & flag_bit` is set.
    pub fn has_flag(&self, flag_bit: u8) -> bool {
        (self.flags & flag_bit) != 0
    }

    /// Encode the header into the first 9 bytes of `out`. Returns
    /// `None` if `out` is too short. `length` must fit in 24 bits.
    pub fn encode(&self, out: &mut [u8]) -> Option<()> {
        if out.len() < FRAME_HEADER_LEN || self.length > 0x00FF_FFFF {
            return None;
        }
        out[0] = ((self.length >> 16) & 0xFF) as u8;
        out[1] = ((self.length >> 8) & 0xFF) as u8;
        out[2] = (self.length & 0xFF) as u8;
        out[3] = self.frame_type;
        out[4] = self.flags;
        let sid = self.stream_id & 0x7FFF_FFFF;
        out[5] = ((sid >> 24) & 0xFF) as u8;
        out[6] = ((sid >> 16) & 0xFF) as u8;
        out[7] = ((sid >> 8) & 0xFF) as u8;
        out[8] = (sid & 0xFF) as u8;
        Some(())
    }
}

// ── SETTINGS frame body ───────────────────────────────────────────────
//
// RFC 7540 §6.5. SETTINGS payload is a sequence of (identifier:u16,
// value:u32) pairs; SETTINGS with ACK flag carries an empty payload.

pub const SETTINGS_PARAM_LEN: usize = 6;

/// SETTINGS parameter identifiers (RFC 7540 §6.5.2). Values are u32 BE.
pub const SETTINGS_HEADER_TABLE_SIZE: u16 = 0x1;
pub const SETTINGS_ENABLE_PUSH: u16 = 0x2;
pub const SETTINGS_MAX_CONCURRENT_STREAMS: u16 = 0x3;
pub const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x4;
pub const SETTINGS_MAX_FRAME_SIZE: u16 = 0x5;
pub const SETTINGS_MAX_HEADER_LIST_SIZE: u16 = 0x6;

/// Iterator over the `(id, value)` pairs in a SETTINGS frame payload.
pub struct SettingsIter<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> SettingsIter<'a> {
    pub fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }
}

impl Iterator for SettingsIter<'_> {
    type Item = (u16, u32);
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset + SETTINGS_PARAM_LEN > self.payload.len() {
            return None;
        }
        let p = &self.payload[self.offset..self.offset + SETTINGS_PARAM_LEN];
        let id = (u16::from(p[0]) << 8) | u16::from(p[1]);
        let value = (u32::from(p[2]) << 24)
            | (u32::from(p[3]) << 16)
            | (u32::from(p[4]) << 8)
            | u32::from(p[5]);
        self.offset += SETTINGS_PARAM_LEN;
        Some((id, value))
    }
}

/// Build an empty SETTINGS frame whose body acknowledges a peer
/// SETTINGS — RFC 7540 §6.5.3. Writes `FRAME_HEADER_LEN` bytes into
/// `out`. Stream id is always 0 for connection-level frames.
pub fn build_settings_ack(out: &mut [u8]) -> Option<()> {
    let hdr = FrameHeader {
        length: 0,
        frame_type: FrameType::Settings as u8,
        flags: FLAG_ACK,
        stream_id: 0,
    };
    hdr.encode(out)
}

// ── PING frame body ───────────────────────────────────────────────────
//
// RFC 7540 §6.7. PING payload is exactly 8 opaque bytes. The server
// must echo a PING with the ACK flag and the same payload.

pub const PING_PAYLOAD_LEN: usize = 8;

/// Build a PING ACK frame echoing the peer's 8-byte opaque payload.
/// Writes `FRAME_HEADER_LEN + PING_PAYLOAD_LEN = 17` bytes into `out`.
pub fn build_ping_ack(out: &mut [u8], opaque: &[u8]) -> Option<()> {
    if out.len() < FRAME_HEADER_LEN + PING_PAYLOAD_LEN || opaque.len() < PING_PAYLOAD_LEN {
        return None;
    }
    let hdr = FrameHeader {
        length: PING_PAYLOAD_LEN as u32,
        frame_type: FrameType::Ping as u8,
        flags: FLAG_ACK,
        stream_id: 0,
    };
    hdr.encode(&mut out[..FRAME_HEADER_LEN])?;
    out[FRAME_HEADER_LEN..FRAME_HEADER_LEN + PING_PAYLOAD_LEN]
        .copy_from_slice(&opaque[..PING_PAYLOAD_LEN]);
    Some(())
}

// ── GOAWAY frame body ─────────────────────────────────────────────────
//
// RFC 7540 §6.8. Body layout:
//   [reserved+last_stream_id:u32 BE][error_code:u32 BE][debug_data...]
//
// Lattice sends GOAWAY on connection-level protocol errors. We emit
// empty debug_data for now.

pub const ERROR_NO_ERROR: u32 = 0x0;
pub const ERROR_PROTOCOL_ERROR: u32 = 0x1;
pub const ERROR_INTERNAL_ERROR: u32 = 0x2;
pub const ERROR_FLOW_CONTROL_ERROR: u32 = 0x3;
pub const ERROR_FRAME_SIZE_ERROR: u32 = 0x6;
pub const ERROR_REFUSED_STREAM: u32 = 0x7;

pub fn build_goaway(out: &mut [u8], last_stream_id: u32, error_code: u32) -> Option<()> {
    const BODY: usize = 8;
    if out.len() < FRAME_HEADER_LEN + BODY {
        return None;
    }
    let hdr = FrameHeader {
        length: BODY as u32,
        frame_type: FrameType::Goaway as u8,
        flags: 0,
        stream_id: 0,
    };
    hdr.encode(&mut out[..FRAME_HEADER_LEN])?;
    let sid = last_stream_id & 0x7FFF_FFFF;
    let mut p = FRAME_HEADER_LEN;
    out[p] = ((sid >> 24) & 0xFF) as u8;
    out[p + 1] = ((sid >> 16) & 0xFF) as u8;
    out[p + 2] = ((sid >> 8) & 0xFF) as u8;
    out[p + 3] = (sid & 0xFF) as u8;
    p += 4;
    out[p] = ((error_code >> 24) & 0xFF) as u8;
    out[p + 1] = ((error_code >> 16) & 0xFF) as u8;
    out[p + 2] = ((error_code >> 8) & 0xFF) as u8;
    out[p + 3] = (error_code & 0xFF) as u8;
    Some(())
}

// ── WINDOW_UPDATE frame body ──────────────────────────────────────────
//
// RFC 7540 §6.9. Body is one 4-byte big-endian increment with the high
// bit reserved. Sent connection-level (stream_id=0) or per-stream.
//
// Lattice currently advertises a generous initial window and doesn't
// shrink it, so we mostly only need to *emit* WINDOW_UPDATE to keep
// the peer's flow-control window from clamping us.

pub fn build_window_update(out: &mut [u8], stream_id: u32, increment: u32) -> Option<()> {
    const BODY: usize = 4;
    if out.len() < FRAME_HEADER_LEN + BODY {
        return None;
    }
    let hdr = FrameHeader {
        length: BODY as u32,
        frame_type: FrameType::WindowUpdate as u8,
        flags: 0,
        stream_id,
    };
    hdr.encode(&mut out[..FRAME_HEADER_LEN])?;
    let inc = increment & 0x7FFF_FFFF;
    let p = FRAME_HEADER_LEN;
    out[p] = ((inc >> 24) & 0xFF) as u8;
    out[p + 1] = ((inc >> 16) & 0xFF) as u8;
    out[p + 2] = ((inc >> 8) & 0xFF) as u8;
    out[p + 3] = (inc & 0xFF) as u8;
    Some(())
}

// ── gRPC framing ──────────────────────────────────────────────────────
//
// gRPC over HTTP/2: each request / response message inside a DATA
// frame is preceded by a 5-byte prefix:
//
//   [compressed:u8 (0 = uncompressed, 1 = compressed)]
//   [length:u32 BE]
//
// followed by `length` bytes of protobuf-encoded message. Multiple
// messages MAY share a single DATA frame; in practice etcd-client
// sends one message per stream. Lattice does not negotiate the
// compression algorithm (it only declares `grpc-encoding: identity`)
// so the compressed byte is always 0 on send and rejected on receive.

pub const GRPC_FRAME_HEADER_LEN: usize = 5;

/// Parsed gRPC message header. Body slice is `[GRPC_FRAME_HEADER_LEN..
/// GRPC_FRAME_HEADER_LEN + length as usize]` of the containing DATA
/// payload.
#[derive(Clone, Copy, Debug)]
pub struct GrpcMessageHeader {
    pub compressed: u8,
    pub length: u32,
}

impl GrpcMessageHeader {
    pub fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() < GRPC_FRAME_HEADER_LEN {
            return None;
        }
        let length = (u32::from(buf[1]) << 24)
            | (u32::from(buf[2]) << 16)
            | (u32::from(buf[3]) << 8)
            | u32::from(buf[4]);
        Some(Self {
            compressed: buf[0],
            length,
        })
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<()> {
        if out.len() < GRPC_FRAME_HEADER_LEN {
            return None;
        }
        out[0] = self.compressed;
        out[1] = ((self.length >> 24) & 0xFF) as u8;
        out[2] = ((self.length >> 16) & 0xFF) as u8;
        out[3] = ((self.length >> 8) & 0xFF) as u8;
        out[4] = (self.length & 0xFF) as u8;
        Some(())
    }
}

// ── Protobuf wire primitives ──────────────────────────────────────────
//
// All etcd v3 KV protobuf messages use a tiny subset of the protobuf
// wire format. We hand-roll the encoder/decoder here rather than
// pulling in `prost` (which depends on `alloc` and is hostile to the
// no_std PIC build).
//
// Wire types we need:
//   0 (VARINT)        — int32/int64/uint32/uint64/bool/enum
//   2 (LENGTH_DELIM)  — bytes, embedded messages, packed repeated
//
// Fields have a header byte (`tag<<3 | wire_type`); tags > 15 spill
// into a multi-byte varint.

pub const WIRE_VARINT: u8 = 0;
pub const WIRE_LENGTH_DELIMITED: u8 = 2;

/// Decode an unsigned little-endian base-128 varint. Returns
/// `(value, bytes_consumed)`. `None` on truncation or > 10-byte varint
/// (which can't fit in a u64).
pub fn read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut i = 0;
    while i < buf.len() {
        let byte = buf[i];
        i += 1;
        if shift >= 64 {
            return None;
        }
        value |= u64::from(byte & 0x7F) << shift;
        if (byte & 0x80) == 0 {
            return Some((value, i));
        }
        shift += 7;
        if i > 10 {
            return None;
        }
    }
    None
}

/// Write a varint into `out` at offset `off`. Returns the new offset,
/// or `None` if the buffer is full.
pub fn write_varint(out: &mut [u8], off: &mut usize, mut v: u64) -> Option<()> {
    while v >= 0x80 {
        if *off >= out.len() {
            return None;
        }
        out[*off] = (v as u8 & 0x7F) | 0x80;
        *off += 1;
        v >>= 7;
    }
    if *off >= out.len() {
        return None;
    }
    out[*off] = v as u8;
    *off += 1;
    Some(())
}

/// Decode one protobuf field tag header. Returns `(field_number,
/// wire_type, bytes_consumed)`.
pub fn read_tag(buf: &[u8]) -> Option<(u32, u8, usize)> {
    let (key, n) = read_varint(buf)?;
    if key > u64::from(u32::MAX) {
        return None;
    }
    let field_number = (key >> 3) as u32;
    let wire_type = (key & 0x7) as u8;
    Some((field_number, wire_type, n))
}

/// Write `field_number << 3 | wire_type` as a varint key. Used by the
/// response encoder.
pub fn write_tag(out: &mut [u8], off: &mut usize, field_number: u32, wire_type: u8) -> Option<()> {
    let key = (u64::from(field_number) << 3) | u64::from(wire_type & 0x7);
    write_varint(out, off, key)
}

/// Read a length-delimited field body: `[varint length][bytes...]`.
/// Returns `(slice, bytes_consumed_total)`.
pub fn read_length_delimited(buf: &[u8]) -> Option<(&[u8], usize)> {
    let (len, n) = read_varint(buf)?;
    let len = len as usize;
    if buf.len() < n + len {
        return None;
    }
    Some((&buf[n..n + len], n + len))
}

/// Write a length-delimited field: tag + length varint + data.
pub fn write_length_delimited(
    out: &mut [u8],
    off: &mut usize,
    field_number: u32,
    data: &[u8],
) -> Option<()> {
    write_tag(out, off, field_number, WIRE_LENGTH_DELIMITED)?;
    write_varint(out, off, data.len() as u64)?;
    if *off + data.len() > out.len() {
        return None;
    }
    out[*off..*off + data.len()].copy_from_slice(data);
    *off += data.len();
    Some(())
}

/// Skip an unknown field given its wire type. Used when we encounter
/// fields we don't understand inside a protobuf message — protobuf
/// requires forward compatibility (RFC 0001 §3).
pub fn skip_field(buf: &[u8], wire_type: u8) -> Option<usize> {
    match wire_type {
        WIRE_VARINT => read_varint(buf).map(|(_, n)| n),
        WIRE_LENGTH_DELIMITED => read_length_delimited(buf).map(|(_, n)| n),
        // We don't expect 32-bit, 64-bit, or groups in the etcd v3 KV
        // surface; reject them so the parser bails rather than skipping
        // by an unknown amount.
        _ => None,
    }
}

// ── etcd v3 KV protobuf subset ────────────────────────────────────────
//
// We implement only the fields Lattice actually uses today. Unknown
// fields are skipped via `skip_field` so adding fields later doesn't
// break wire compatibility. Field numbers are pinned to
// `etcdserverpb.KV` (`api/etcdserverpb/rpc.proto` upstream).
//
// ## RangeRequest (→ KV_OP_GET)
//
//   message RangeRequest {
//     bytes  key            = 1;
//     bytes  range_end      = 2;   // (range queries — out of scope)
//     int64  limit          = 3;
//     int64  revision       = 4;
//     ...
//   }
//
// Lattice currently translates only single-key Range to KV_OP_GET.
// Range-end and limit are accepted on the wire but ignored.
//
// ## PutRequest (→ KV_OP_PUT)
//
//   message PutRequest {
//     bytes  key            = 1;
//     bytes  value          = 2;
//     int64  lease          = 3;
//     bool   prev_kv        = 4;
//     bool   ignore_value   = 5;
//     bool   ignore_lease   = 6;
//   }
//
// Lattice maps key+value directly. lease, prev_kv, ignore_* are
// not yet supported and produce `KV_RESULT_INTERNAL` if non-default.
//
// ## DeleteRangeRequest (→ KV_OP_DELETE)
//
//   message DeleteRangeRequest {
//     bytes key       = 1;
//     bytes range_end = 2;       // (range deletes — out of scope)
//     bool  prev_kv   = 3;
//   }

/// Subset of `etcdserverpb.RangeRequest` Lattice consumes.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct RangeRequest<'a> {
    pub key: &'a [u8],
    pub range_end: &'a [u8],
    pub limit: i64,
    pub revision: i64,
}

impl<'a> RangeRequest<'a> {
    pub fn decode(mut buf: &'a [u8]) -> Option<Self> {
        let mut req = Self::default();
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf)?;
            buf = &buf[n..];
            match (field, wire) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let (key, n) = read_length_delimited(buf)?;
                    req.key = key;
                    buf = &buf[n..];
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    let (range_end, n) = read_length_delimited(buf)?;
                    req.range_end = range_end;
                    buf = &buf[n..];
                }
                (3, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.limit = v as i64;
                    buf = &buf[n..];
                }
                (4, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.revision = v as i64;
                    buf = &buf[n..];
                }
                _ => {
                    let n = skip_field(buf, wire)?;
                    buf = &buf[n..];
                }
            }
        }
        Some(req)
    }
}

/// gRPC status code etcd uses for a read below the compaction floor:
/// `rpctypes.ErrGRPCCompacted` is `codes.OutOfRange` (11) with the
/// message "etcdserver: mvcc: required revision has been compacted".
///
/// This is the status Lattice returns for `KV_RESULT_COMPACTED`, and it
/// matters that it is 11 and not 13 (INTERNAL): etcd clients treat
/// OUT_OF_RANGE on a Range as "re-read from the current revision" and
/// INTERNAL as "the server is broken". A watch or a mirror that gets
/// the wrong one either spins or gives up. Lattice's trailers carry the
/// status only, not the message string — see `build_grpc_trailers_block`.
pub const GRPC_STATUS_COMPACTED: u8 = 11;

/// Encode a decoded `RangeRequest` into the native KV op body.
///
/// Returns `(historical, body_len)`:
///
/// - `historical == false` → the body is `[key_len:u16 LE][key…]`, the
///   `KV_OP_GET` shape (latest read, the pre-existing behaviour).
/// - `historical == true` → `revision > 0` was set, and the body is
///   `[revision:u64 LE][key_len:u16 LE][key…]`, the `KV_OP_GET_AT`
///   shape. This is RFC §26 Phase 2's "etcd behavior gains real
///   retained revisions": before this, `revision` was decoded off the
///   wire and then dropped, so `Range{revision: 5}` silently answered
///   with the CURRENT value — a wrong answer to a historical question.
///
/// etcd treats a negative `revision` as an error; here it is clamped to
/// "latest" rather than accepted, because a negative revision cannot
/// name a version and must not be reinterpreted as a large positive one
/// by a cast.
///
/// Lives here rather than in the anchor so the mapping is testable
/// without a PIC build (`tests/integration_etcd.rs`); the anchor's only
/// remaining job is choosing the op byte from the returned flag.
pub fn encode_range_kv_body(req: &RangeRequest<'_>, out: &mut [u8]) -> Option<(bool, usize)> {
    let key = req.key;
    if key.len() > u16::MAX as usize {
        return None;
    }
    let historical = req.revision > 0;
    let head = if historical { 8 } else { 0 };
    let need = head + 2 + key.len();
    if need > out.len() {
        return None;
    }
    if historical {
        out[0..8].copy_from_slice(&(req.revision as u64).to_le_bytes());
    }
    let klen = (key.len() as u16).to_le_bytes();
    out[head] = klen[0];
    out[head + 1] = klen[1];
    // Byte loop, not `copy_from_slice`: the PIC build is `no_std` with
    // no panic machinery, and `copy_from_slice`'s length-mismatch
    // branch pulls in `len_mismatch_fail`, which fails to link.
    let mut i = 0usize;
    while i < key.len() {
        out[head + 2 + i] = key[i];
        i += 1;
    }
    Some((historical, need))
}

/// Cap on entries per ranged Range page. The anchor's response body
/// budget is 2 KiB (`STREAM_BODY_MAX`); a small page with an honest
/// `more` flag beats a large one the transport silently drops.
pub const RANGE_SCAN_PAGE_MAX: u16 = 8;

/// Encode a RANGED `RangeRequest` (non-empty `range_end`) into a
/// `KV_OP_RANGE_SCAN` body:
/// `[start_len:u16][start…][end_len:u16][end…][cursor:u64 = 0]
///  [limit:u16]`. Historical ranged reads are the caller's job to
/// refuse (there is no ranged `GET_AT`).
pub fn encode_range_scan_body(req: &RangeRequest<'_>, out: &mut [u8]) -> Option<usize> {
    let (start, end) = (req.key, req.range_end);
    if start.len() > u16::MAX as usize || end.len() > u16::MAX as usize {
        return None;
    }
    let limit: u16 = if req.limit > 0 && req.limit < i64::from(RANGE_SCAN_PAGE_MAX) {
        req.limit as u16
    } else {
        RANGE_SCAN_PAGE_MAX
    };
    let need = 2 + start.len() + 2 + end.len() + 8 + 2;
    if need > out.len() {
        return None;
    }
    let mut p = 0usize;
    out[p..p + 2].copy_from_slice(&(start.len() as u16).to_le_bytes());
    p += 2;
    let mut i = 0;
    while i < start.len() {
        out[p + i] = start[i];
        i += 1;
    }
    p += start.len();
    out[p..p + 2].copy_from_slice(&(end.len() as u16).to_le_bytes());
    p += 2;
    i = 0;
    while i < end.len() {
        out[p + i] = end[i];
        i += 1;
    }
    p += end.len();
    out[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
    p += 8;
    out[p..p + 2].copy_from_slice(&limit.to_le_bytes());
    p += 2;
    // The cursor, not the estimate: `need` sized the buffer, `p` counts
    // what was written. They agree by construction, and returning the
    // one the writes produced keeps every advance load-bearing — a
    // field appended after the last one cannot then land at a stale
    // offset.
    Some(p)
}

/// Build an `etcdserverpb.RangeResponse` from a `KV_RESULT_RANGE`
/// body (`[cursor:u64][count:u16]` then per entry
/// `[key_len:u16][key…][value_len:u32][value…]`). Every entry that
/// fits the output budget is written; a non-zero provider cursor OR a
/// budget stop sets `more = true`, so a truncated page is announced,
/// never passed off as complete. Per-entry `mod_revision` is
/// approximated by the response head revision (the native scan body
/// does not carry per-key revisions).
pub fn build_range_scan_response(
    out: &mut [u8],
    off: &mut usize,
    revision: i64,
    body: &[u8],
) -> Option<()> {
    if body.len() < 10 {
        return None;
    }
    let cursor = u64::from_le_bytes([
        body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
    ]);
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    write_response_header(out, off, revision)?;
    // Tail budget: `more` (≤2 bytes) + `count` (≤11 bytes).
    let tail_budget = 16usize;
    let mut p = 10usize;
    let mut written: i64 = 0;
    let mut budget_stop = false;
    for _ in 0..count {
        if body.len() < p + 2 {
            return None;
        }
        let klen = u16::from_le_bytes([body[p], body[p + 1]]) as usize;
        p += 2;
        let key = body.get(p..p + klen)?;
        p += klen;
        let vlen = u32::from_le_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]]) as usize;
        p += 4;
        let value = body.get(p..p + vlen)?;
        p += vlen;
        // Conservative encoded bound: key + value + tags/varints.
        if *off + klen + vlen + 40 + tail_budget > out.len() {
            budget_stop = true;
            break;
        }
        write_keyvalue(out, off, 2, key, value, revision, 1)?;
        written += 1;
    }
    if cursor != 0 || budget_stop {
        // bool `more = 3`.
        write_tag(out, off, 3, WIRE_VARINT)?;
        write_varint(out, off, 1)?;
    }
    // int64 `count = 4`.
    write_tag(out, off, 4, WIRE_VARINT)?;
    write_varint(out, off, written as u64)?;
    Some(())
}

/// Subset of `etcdserverpb.PutRequest`.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct PutRequest<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub lease: i64,
    pub prev_kv: bool,
    pub ignore_value: bool,
    pub ignore_lease: bool,
}

impl<'a> PutRequest<'a> {
    pub fn decode(mut buf: &'a [u8]) -> Option<Self> {
        let mut req = Self::default();
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf)?;
            buf = &buf[n..];
            match (field, wire) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let (key, n) = read_length_delimited(buf)?;
                    req.key = key;
                    buf = &buf[n..];
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    let (value, n) = read_length_delimited(buf)?;
                    req.value = value;
                    buf = &buf[n..];
                }
                (3, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.lease = v as i64;
                    buf = &buf[n..];
                }
                (4, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.prev_kv = v != 0;
                    buf = &buf[n..];
                }
                (5, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.ignore_value = v != 0;
                    buf = &buf[n..];
                }
                (6, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.ignore_lease = v != 0;
                    buf = &buf[n..];
                }
                _ => {
                    let n = skip_field(buf, wire)?;
                    buf = &buf[n..];
                }
            }
        }
        Some(req)
    }
}

/// Subset of `etcdserverpb.DeleteRangeRequest`.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct DeleteRangeRequest<'a> {
    pub key: &'a [u8],
    pub range_end: &'a [u8],
    pub prev_kv: bool,
}

impl<'a> DeleteRangeRequest<'a> {
    pub fn decode(mut buf: &'a [u8]) -> Option<Self> {
        let mut req = Self::default();
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf)?;
            buf = &buf[n..];
            match (field, wire) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let (key, n) = read_length_delimited(buf)?;
                    req.key = key;
                    buf = &buf[n..];
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    let (range_end, n) = read_length_delimited(buf)?;
                    req.range_end = range_end;
                    buf = &buf[n..];
                }
                (3, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.prev_kv = v != 0;
                    buf = &buf[n..];
                }
                _ => {
                    let n = skip_field(buf, wire)?;
                    buf = &buf[n..];
                }
            }
        }
        Some(req)
    }
}

// ── etcd v3 Lease service request decoders ───────────────────────────
//
// Wire-level protos (etcdserverpb.rpc.proto / Lease subset):
//
//   message LeaseGrantRequest  { int64 TTL = 1; int64 ID = 2; }
//   message LeaseRevokeRequest { int64 ID  = 1; }
//
// Both are unary RPCs; LeaseKeepAlive / LeaseTimeToLive are
// streaming and live in the (deferred) bidi-anchor path.

/// Subset of `etcdserverpb.LeaseGrantRequest`.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct LeaseGrantRequest {
    /// TTL in seconds (the etcd wire-level unit).
    pub ttl: i64,
    /// Caller-supplied lease ID. 0 = server assigns.
    pub id: i64,
}

impl LeaseGrantRequest {
    pub fn decode(mut buf: &[u8]) -> Option<Self> {
        let mut req = Self::default();
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf)?;
            buf = &buf[n..];
            match (field, wire) {
                (1, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.ttl = v as i64;
                    buf = &buf[n..];
                }
                (2, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.id = v as i64;
                    buf = &buf[n..];
                }
                _ => {
                    let n = skip_field(buf, wire)?;
                    buf = &buf[n..];
                }
            }
        }
        Some(req)
    }
}

/// Subset of `etcdserverpb.LeaseRevokeRequest`.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct LeaseRevokeRequest {
    pub id: i64,
}

impl LeaseRevokeRequest {
    pub fn decode(mut buf: &[u8]) -> Option<Self> {
        let mut req = Self::default();
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf)?;
            buf = &buf[n..];
            match (field, wire) {
                (1, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.id = v as i64;
                    buf = &buf[n..];
                }
                _ => {
                    let n = skip_field(buf, wire)?;
                    buf = &buf[n..];
                }
            }
        }
        Some(req)
    }
}

/// Subset of `etcdserverpb.LeaseKeepAliveRequest`. The wire shape is
/// `{ int64 ID = 1; }`. Streamed: each request on the bidi stream
/// asks the server to refresh `ID`'s TTL.
#[derive(Default, Debug, PartialEq, Eq)]
pub struct LeaseKeepAliveRequest {
    pub id: i64,
}

impl LeaseKeepAliveRequest {
    pub fn decode(mut buf: &[u8]) -> Option<Self> {
        let mut req = Self::default();
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf)?;
            buf = &buf[n..];
            match (field, wire) {
                (1, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf)?;
                    req.id = v as i64;
                    buf = &buf[n..];
                }
                _ => {
                    let n = skip_field(buf, wire)?;
                    buf = &buf[n..];
                }
            }
        }
        Some(req)
    }
}

// ── etcd v3 Watch service request decoder ────────────────────────────
//
// Wire-level protos (etcdserverpb.rpc.proto / Watch subset):
//
//   message WatchRequest {
//     oneof request_union {
//       WatchCreateRequest   create_request   = 1;
//       WatchCancelRequest   cancel_request   = 2;
//       WatchProgressRequest progress_request = 3;
//     }
//   }
//   message WatchCreateRequest {
//     bytes  key             = 1;
//     bytes  range_end       = 2;
//     int64  start_revision  = 3;
//     bool   progress_notify = 4;
//     repeated FilterType filters = 5;
//     bool   prev_kv         = 6;
//     int64  watch_id        = 7;
//     bool   fragment        = 8;
//   }
//   message WatchCancelRequest { int64 watch_id = 1; }
//
// `oneof` is wire-encoded as separate fields (only one set per message).

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WatchOp {
    Create = 1,
    Cancel = 2,
    Progress = 3,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct WatchRequest<'a> {
    pub op: Option<WatchOp>,
    pub key: &'a [u8],
    pub range_end: &'a [u8],
    pub start_revision: i64,
    pub progress_notify: bool,
    pub prev_kv: bool,
    pub create_watch_id: i64,
    pub cancel_watch_id: i64,
}

impl<'a> WatchRequest<'a> {
    pub fn decode(mut buf: &'a [u8]) -> Option<Self> {
        let mut req = Self::default();
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf)?;
            buf = &buf[n..];
            match (field, wire) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    req.op = Some(WatchOp::Create);
                    let (sub, n) = read_length_delimited(buf)?;
                    decode_create(sub, &mut req)?;
                    buf = &buf[n..];
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    req.op = Some(WatchOp::Cancel);
                    let (sub, n) = read_length_delimited(buf)?;
                    req.cancel_watch_id = decode_cancel(sub)?;
                    buf = &buf[n..];
                }
                (3, WIRE_LENGTH_DELIMITED) => {
                    req.op = Some(WatchOp::Progress);
                    let (_sub, n) = read_length_delimited(buf)?;
                    buf = &buf[n..];
                }
                _ => {
                    let n = skip_field(buf, wire)?;
                    buf = &buf[n..];
                }
            }
        }
        Some(req)
    }
}

fn decode_create<'a>(mut buf: &'a [u8], req: &mut WatchRequest<'a>) -> Option<()> {
    while !buf.is_empty() {
        let (field, wire, n) = read_tag(buf)?;
        buf = &buf[n..];
        match (field, wire) {
            (1, WIRE_LENGTH_DELIMITED) => {
                let (key, n) = read_length_delimited(buf)?;
                req.key = key;
                buf = &buf[n..];
            }
            (2, WIRE_LENGTH_DELIMITED) => {
                let (range_end, n) = read_length_delimited(buf)?;
                req.range_end = range_end;
                buf = &buf[n..];
            }
            (3, WIRE_VARINT) => {
                let (v, n) = read_varint(buf)?;
                req.start_revision = v as i64;
                buf = &buf[n..];
            }
            (4, WIRE_VARINT) => {
                let (v, n) = read_varint(buf)?;
                req.progress_notify = v != 0;
                buf = &buf[n..];
            }
            (6, WIRE_VARINT) => {
                let (v, n) = read_varint(buf)?;
                req.prev_kv = v != 0;
                buf = &buf[n..];
            }
            (7, WIRE_VARINT) => {
                let (v, n) = read_varint(buf)?;
                req.create_watch_id = v as i64;
                buf = &buf[n..];
            }
            _ => {
                let n = skip_field(buf, wire)?;
                buf = &buf[n..];
            }
        }
    }
    Some(())
}

fn decode_cancel(mut buf: &[u8]) -> Option<i64> {
    let mut id: i64 = 0;
    while !buf.is_empty() {
        let (field, wire, n) = read_tag(buf)?;
        buf = &buf[n..];
        if field == 1 && wire == WIRE_VARINT {
            let (v, n) = read_varint(buf)?;
            id = v as i64;
            buf = &buf[n..];
        } else {
            let n = skip_field(buf, wire)?;
            buf = &buf[n..];
        }
    }
    Some(id)
}

// ── etcd v3 KV response encoders ──────────────────────────────────────
//
// Counterpart to the decoders above. Lattice replies stay minimal: a
// ResponseHeader with revision, plus method-specific bodies.

/// Subset of `etcdserverpb.ResponseHeader`.
///
///   message ResponseHeader {
///     uint64 cluster_id = 1;
///     uint64 member_id  = 2;
///     int64  revision   = 3;
///     uint64 raft_term  = 4;
///   }
///
/// Cluster/member IDs are emitted as 1 today (single-node, no Raft).
pub fn write_response_header(out: &mut [u8], off: &mut usize, revision: i64) -> Option<()> {
    // Build header bytes into a temporary buffer so we can wrap them in
    // the outer length-delimited tag with the right length prefix.
    let mut buf = [0u8; 32];
    let mut sub = 0;
    write_tag(&mut buf, &mut sub, 1, WIRE_VARINT)?;
    write_varint(&mut buf, &mut sub, 1)?; // cluster_id
    write_tag(&mut buf, &mut sub, 2, WIRE_VARINT)?;
    write_varint(&mut buf, &mut sub, 1)?; // member_id
    write_tag(&mut buf, &mut sub, 3, WIRE_VARINT)?;
    write_varint(&mut buf, &mut sub, revision as u64)?;
    write_tag(&mut buf, &mut sub, 4, WIRE_VARINT)?;
    write_varint(&mut buf, &mut sub, 1)?; // raft_term
    write_length_delimited(out, off, 1, &buf[..sub])
}

/// Build a `KeyValue` message — etcd's wire `mvccpb.KeyValue`:
///   bytes key = 1;
///   int64 create_revision = 2;
///   int64 mod_revision = 3;
///   int64 version = 4;
///   bytes value = 5;
///   int64 lease = 6;
pub fn write_keyvalue(
    out: &mut [u8],
    off: &mut usize,
    field_number: u32,
    key: &[u8],
    value: &[u8],
    mod_revision: i64,
    version: i64,
) -> Option<()> {
    let mut buf = [0u8; 4096];
    let mut sub = 0;
    write_length_delimited(&mut buf, &mut sub, 1, key)?;
    write_tag(&mut buf, &mut sub, 2, WIRE_VARINT)?;
    write_varint(&mut buf, &mut sub, mod_revision as u64)?;
    write_tag(&mut buf, &mut sub, 3, WIRE_VARINT)?;
    write_varint(&mut buf, &mut sub, mod_revision as u64)?;
    write_tag(&mut buf, &mut sub, 4, WIRE_VARINT)?;
    write_varint(&mut buf, &mut sub, version as u64)?;
    write_length_delimited(&mut buf, &mut sub, 5, value)?;
    write_length_delimited(out, off, field_number, &buf[..sub])
}

/// Build an `etcdserverpb.RangeResponse` carrying either zero or one
/// key-value pair (the subset Lattice's `KV_OP_GET` path produces).
///
///   message RangeResponse {
///     ResponseHeader header = 1;
///     repeated KeyValue kvs = 2;
///     bool   more           = 3;
///     int64  count          = 4;
///   }
pub fn build_range_response(
    out: &mut [u8],
    off: &mut usize,
    revision: i64,
    kv: Option<(&[u8], &[u8], i64, i64)>, // (key, value, mod_revision, version)
) -> Option<()> {
    write_response_header(out, off, revision)?;
    let count = if let Some((key, value, mr, ver)) = kv {
        write_keyvalue(out, off, 2, key, value, mr, ver)?;
        1
    } else {
        0
    };
    // bool `more = 3` (omit when false — protobuf default elision).
    // int64 `count = 4`
    write_tag(out, off, 4, WIRE_VARINT)?;
    write_varint(out, off, count)?;
    Some(())
}

/// Build an `etcdserverpb.PutResponse`:
///   message PutResponse {
///     ResponseHeader header = 1;
///     KeyValue       prev_kv = 2;   // (optional — not emitted today)
///   }
pub fn build_put_response(out: &mut [u8], off: &mut usize, revision: i64) -> Option<()> {
    write_response_header(out, off, revision)
}

/// Build an `etcdserverpb.DeleteRangeResponse`:
///   message DeleteRangeResponse {
///     ResponseHeader header = 1;
///     int64          deleted = 2;
///     repeated KeyValue prev_kvs = 3;   // (optional)
///   }
pub fn build_delete_range_response(
    out: &mut [u8],
    off: &mut usize,
    revision: i64,
    deleted: i64,
) -> Option<()> {
    write_response_header(out, off, revision)?;
    write_tag(out, off, 2, WIRE_VARINT)?;
    write_varint(out, off, deleted as u64)?;
    Some(())
}

/// Build an `etcdserverpb.LeaseGrantResponse`:
///   message LeaseGrantResponse {
///     ResponseHeader header = 1;
///     int64          ID      = 2;
///     int64          TTL     = 3;
///     string         error   = 4;
///   }
/// `error` is omitted when empty (protobuf default elision).
pub fn build_lease_grant_response(
    out: &mut [u8],
    off: &mut usize,
    revision: i64,
    lease_id: i64,
    ttl: i64,
) -> Option<()> {
    write_response_header(out, off, revision)?;
    write_tag(out, off, 2, WIRE_VARINT)?;
    write_varint(out, off, lease_id as u64)?;
    write_tag(out, off, 3, WIRE_VARINT)?;
    write_varint(out, off, ttl as u64)?;
    Some(())
}

/// Build an `etcdserverpb.LeaseRevokeResponse`:
///   message LeaseRevokeResponse {
///     ResponseHeader header = 1;
///   }
pub fn build_lease_revoke_response(out: &mut [u8], off: &mut usize, revision: i64) -> Option<()> {
    write_response_header(out, off, revision)
}

/// Build an `etcdserverpb.LeaseKeepAliveResponse`:
///   message LeaseKeepAliveResponse {
///     ResponseHeader header = 1;
///     int64          ID     = 2;
///     int64          TTL    = 3;
///   }
/// TTL is in seconds (etcd wire unit). TTL=0 means the lease no longer
/// exists — clients treat that as a revoke and stop keep-aliving.
pub fn build_lease_keepalive_response(
    out: &mut [u8],
    off: &mut usize,
    revision: i64,
    lease_id: i64,
    ttl: i64,
) -> Option<()> {
    write_response_header(out, off, revision)?;
    write_tag(out, off, 2, WIRE_VARINT)?;
    write_varint(out, off, lease_id as u64)?;
    write_tag(out, off, 3, WIRE_VARINT)?;
    write_varint(out, off, ttl as u64)?;
    Some(())
}

/// Build an `etcdserverpb.WatchResponse` header (no events). Events
/// are appended by [`write_watch_event`] after this returns.
///
///   message WatchResponse {
///     ResponseHeader header = 1;
///     int64  watch_id        = 2;
///     bool   created         = 3;
///     bool   canceled        = 4;
///     int64  compact_revision = 5;
///     string cancel_reason   = 6;
///     bool   fragment        = 7;
///     repeated mvccpb.Event events = 11;
///   }
/// `created` / `canceled` are bool-elided when false.
pub fn build_watch_response_header(
    out: &mut [u8],
    off: &mut usize,
    revision: i64,
    watch_id: i64,
    created: bool,
    canceled: bool,
) -> Option<()> {
    write_response_header(out, off, revision)?;
    write_tag(out, off, 2, WIRE_VARINT)?;
    write_varint(out, off, watch_id as u64)?;
    if created {
        write_tag(out, off, 3, WIRE_VARINT)?;
        write_varint(out, off, 1)?;
    }
    if canceled {
        write_tag(out, off, 4, WIRE_VARINT)?;
        write_varint(out, off, 1)?;
    }
    Some(())
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WatchEventKind {
    Put = 0,
    Delete = 1,
}

/// Append one `mvccpb.Event` (field 11) to a `WatchResponse` payload.
///   message Event { EventType type=1; KeyValue kv=2; KeyValue prev_kv=3; }
pub fn write_watch_event(
    out: &mut [u8],
    off: &mut usize,
    kind: WatchEventKind,
    key: &[u8],
    value: &[u8],
    mod_revision: i64,
    version: i64,
) -> Option<()> {
    let mut ebuf = [0u8; 4096];
    let mut eoff = 0;
    if kind as u8 != 0 {
        write_tag(&mut ebuf, &mut eoff, 1, WIRE_VARINT)?;
        write_varint(&mut ebuf, &mut eoff, kind as u64)?;
    }
    write_keyvalue(&mut ebuf, &mut eoff, 2, key, value, mod_revision, version)?;
    write_length_delimited(out, off, 11, &ebuf[..eoff])
}

// ── HPACK (RFC 7541) ──────────────────────────────────────────────────
//
// HPACK is the header-block compression used by HTTP/2. Lattice
// implements the decoder and a minimal literal-only encoder. Dynamic-
// table support is intentionally minimal: we advertise
// SETTINGS_HEADER_TABLE_SIZE = 0 to the peer, so a conforming encoder
// will not emit indexed references into the dynamic table. The
// decoder still parses all four representation forms (Indexed,
// Literal Incremental, Literal Never-Indexed, Literal No-Indexing)
// plus Dynamic-Table-Size-Update; literals with "Incremental Indexing"
// are output normally but their would-be table entry is discarded
// (the table size is 0, so they evict immediately per RFC 7541 §4.4).
//
// The decoder handles Huffman-coded string literals (RFC 7541 §5.2)
// because gRPC clients routinely Huffman-encode :authority,
// user-agent, content-type. The encoder emits raw literals (no
// Huffman) — the response payload Lattice sends is tiny (status,
// content-type, grpc-status[, grpc-message]) so the missing
// compression is moot.
//
// ## Static table (RFC 7541 Appendix A)
//
// 61 entries; indices are 1-based on the wire. Entries with no
// "value" column have an empty byte string as their value.

/// Static-table entry: name + value byte strings.
#[derive(Clone, Copy)]
pub struct HpackStaticEntry {
    pub name: &'static [u8],
    pub value: &'static [u8],
}

/// Number of entries in the RFC 7541 Appendix A static table,
/// 1-based.  Lookups must satisfy `1 <= idx <= HPACK_STATIC_TABLE_LEN`.
pub const HPACK_STATIC_TABLE_LEN: usize = 61;

/// Look up an entry in the HPACK static table (RFC 7541 Appendix A).
///
/// Returns `None` for index 0 or out-of-range.
///
/// Implementation note: the obvious shape is a `static [Entry; 62]`
/// array with embedded `&'static [u8]` slices. That works on hosted
/// targets but the resulting `.data.rel.ro` section relies on R_*_ABS64
/// relocations into `.rodata`, which the fluxor PIC loader doesn't yet
/// resolve — leaving the stored pointers reading whatever happens to
/// sit at offset 0 of the module's mapped image (→ SIGSEGV on first
/// deref). Wrapping the lookup in a function with one match arm per
/// entry sidesteps the issue: each `b"..."` is loaded via a
/// PC-relative `ADRP/ADD` at the match arm's code site (R_*_ADR_PREL21
/// / R_*_ADD_ABS_LO12_NC), which the PIC loader already handles for
/// every other byte literal in the codebase.
pub fn hpack_static_entry(idx: u8) -> Option<HpackStaticEntry> {
    let (name, value): (&'static [u8], &'static [u8]) = match idx {
        1 => (b":authority", b""),
        2 => (b":method", b"GET"),
        3 => (b":method", b"POST"),
        4 => (b":path", b"/"),
        5 => (b":path", b"/index.html"),
        6 => (b":scheme", b"http"),
        7 => (b":scheme", b"https"),
        8 => (b":status", b"200"),
        9 => (b":status", b"204"),
        10 => (b":status", b"206"),
        11 => (b":status", b"304"),
        12 => (b":status", b"400"),
        13 => (b":status", b"404"),
        14 => (b":status", b"500"),
        15 => (b"accept-charset", b""),
        16 => (b"accept-encoding", b"gzip, deflate"),
        17 => (b"accept-language", b""),
        18 => (b"accept-ranges", b""),
        19 => (b"accept", b""),
        20 => (b"access-control-allow-origin", b""),
        21 => (b"age", b""),
        22 => (b"allow", b""),
        23 => (b"authorization", b""),
        24 => (b"cache-control", b""),
        25 => (b"content-disposition", b""),
        26 => (b"content-encoding", b""),
        27 => (b"content-language", b""),
        28 => (b"content-length", b""),
        29 => (b"content-location", b""),
        30 => (b"content-range", b""),
        31 => (b"content-type", b""),
        32 => (b"cookie", b""),
        33 => (b"date", b""),
        34 => (b"etag", b""),
        35 => (b"expect", b""),
        36 => (b"expires", b""),
        37 => (b"from", b""),
        38 => (b"host", b""),
        39 => (b"if-match", b""),
        40 => (b"if-modified-since", b""),
        41 => (b"if-none-match", b""),
        42 => (b"if-range", b""),
        43 => (b"if-unmodified-since", b""),
        44 => (b"last-modified", b""),
        45 => (b"link", b""),
        46 => (b"location", b""),
        47 => (b"max-forwards", b""),
        48 => (b"proxy-authenticate", b""),
        49 => (b"proxy-authorization", b""),
        50 => (b"range", b""),
        51 => (b"referer", b""),
        52 => (b"refresh", b""),
        53 => (b"retry-after", b""),
        54 => (b"server", b""),
        55 => (b"set-cookie", b""),
        56 => (b"strict-transport-security", b""),
        57 => (b"transfer-encoding", b""),
        58 => (b"user-agent", b""),
        59 => (b"vary", b""),
        60 => (b"via", b""),
        61 => (b"www-authenticate", b""),
        _ => return None,
    };
    Some(HpackStaticEntry { name, value })
}

/// RFC 7541 Appendix B — canonical Huffman code table. 256 entries
/// (one per byte) plus EOS at index 256. Each entry is (code,
/// code_length_in_bits). Codes are right-aligned in the u32.
#[derive(Clone, Copy)]
pub struct HuffEntry {
    pub code: u32,
    pub bits: u8,
}

const fn h(code: u32, bits: u8) -> HuffEntry {
    HuffEntry { code, bits }
}

/// The HPACK Huffman table. Indices 0..=255 map to byte values;
/// index 256 is the End-of-String marker (used as padding only).
/// `static` (not `const`) for the same reason as
/// [`HPACK_STATIC_TABLE`]: ensure one canonical copy of the 2 KiB
/// table in the binary.
pub static HPACK_HUFFMAN_TABLE: [HuffEntry; 257] = [
    // 0..15
    h(0x1ff8, 13),
    h(0x7fffd8, 23),
    h(0xfffffe2, 28),
    h(0xfffffe3, 28),
    h(0xfffffe4, 28),
    h(0xfffffe5, 28),
    h(0xfffffe6, 28),
    h(0xfffffe7, 28),
    h(0xfffffe8, 28),
    h(0xffffea, 24),
    h(0x3ffffffc, 30),
    h(0xfffffe9, 28),
    h(0xfffffea, 28),
    h(0x3ffffffd, 30),
    h(0xfffffeb, 28),
    h(0xfffffec, 28),
    // 16..31
    h(0xfffffed, 28),
    h(0xfffffee, 28),
    h(0xfffffef, 28),
    h(0xffffff0, 28),
    h(0xffffff1, 28),
    h(0xffffff2, 28),
    h(0x3ffffffe, 30),
    h(0xffffff3, 28),
    h(0xffffff4, 28),
    h(0xffffff5, 28),
    h(0xffffff6, 28),
    h(0xffffff7, 28),
    h(0xffffff8, 28),
    h(0xffffff9, 28),
    h(0xffffffa, 28),
    h(0xffffffb, 28),
    // 32..47
    h(0x14, 6),
    h(0x3f8, 10),
    h(0x3f9, 10),
    h(0xffa, 12),
    h(0x1ff9, 13),
    h(0x15, 6),
    h(0xf8, 8),
    h(0x7fa, 11),
    h(0x3fa, 10),
    h(0x3fb, 10),
    h(0xf9, 8),
    h(0x7fb, 11),
    h(0xfa, 8),
    h(0x16, 6),
    h(0x17, 6),
    h(0x18, 6),
    // 48..63 ('0'..'?')
    h(0x0, 5),
    h(0x1, 5),
    h(0x2, 5),
    h(0x19, 6),
    h(0x1a, 6),
    h(0x1b, 6),
    h(0x1c, 6),
    h(0x1d, 6),
    h(0x1e, 6),
    h(0x1f, 6),
    h(0x5c, 7),
    h(0xfb, 8),
    h(0x7ffc, 15),
    h(0x20, 6),
    h(0xffb, 12),
    h(0x3fc, 10),
    // 64..79 ('@'..'O')
    h(0x1ffa, 13),
    h(0x21, 6), // 'A' — code 100001, 6 bits per RFC 7541 Appx B
    h(0x5d, 7),
    h(0x5e, 7),
    h(0x5f, 7),
    h(0x60, 7),
    h(0x61, 7),
    h(0x62, 7),
    h(0x63, 7),
    h(0x64, 7),
    h(0x65, 7),
    h(0x66, 7),
    h(0x67, 7),
    h(0x68, 7),
    h(0x69, 7),
    h(0x6a, 7),
    // 80..95 ('P'..'_')
    h(0x6b, 7),
    h(0x6c, 7),
    h(0x6d, 7),
    h(0x6e, 7),
    h(0x6f, 7),
    h(0x70, 7),
    h(0x71, 7),
    h(0x72, 7),
    h(0xfc, 8),
    h(0x73, 7),
    h(0xfd, 8),
    h(0x1ffb, 13),
    h(0x7fff0, 19),
    h(0x1ffc, 13),
    h(0x3ffc, 14),
    h(0x22, 6),
    // 96..111 ('`'..'o')
    h(0x7ffd, 15),
    h(0x3, 5),
    h(0x23, 6),
    h(0x4, 5),
    h(0x24, 6),
    h(0x5, 5),
    h(0x25, 6),
    h(0x26, 6),
    h(0x27, 6),
    h(0x6, 5),
    h(0x74, 7),
    h(0x75, 7),
    h(0x28, 6),
    h(0x29, 6),
    h(0x2a, 6),
    h(0x7, 5),
    // 112..127 ('p'..'~' + DEL)
    h(0x2b, 6),
    h(0x76, 7),
    h(0x2c, 6),
    h(0x8, 5),
    h(0x9, 5),
    h(0x2d, 6),
    h(0x77, 7),
    h(0x78, 7),
    h(0x79, 7),
    h(0x7a, 7),
    h(0x7b, 7),
    h(0x7ffe, 15),
    h(0x7fc, 11),
    h(0x3ffd, 14),
    h(0x1ffd, 13),
    h(0xffffffc, 28),
    // 128..143
    h(0xfffe6, 20),
    h(0x3fffd2, 22),
    h(0xfffe7, 20),
    h(0xfffe8, 20),
    h(0x3fffd3, 22),
    h(0x3fffd4, 22),
    h(0x3fffd5, 22),
    h(0x7fffd9, 23),
    h(0x3fffd6, 22),
    h(0x7fffda, 23),
    h(0x7fffdb, 23),
    h(0x7fffdc, 23),
    h(0x7fffdd, 23),
    h(0x7fffde, 23),
    h(0xffffeb, 24),
    h(0x7fffdf, 23),
    // 144..159
    h(0xffffec, 24),
    h(0xffffed, 24),
    h(0x3fffd7, 22),
    h(0x7fffe0, 23),
    h(0xffffee, 24),
    h(0x7fffe1, 23),
    h(0x7fffe2, 23),
    h(0x7fffe3, 23),
    h(0x7fffe4, 23),
    h(0x1fffdc, 21),
    h(0x3fffd8, 22),
    h(0x7fffe5, 23),
    h(0x3fffd9, 22),
    h(0x7fffe6, 23),
    h(0x7fffe7, 23),
    h(0xffffef, 24),
    // 160..175
    h(0x3fffda, 22),
    h(0x1fffdd, 21),
    h(0xfffe9, 20),
    h(0x3fffdb, 22),
    h(0x3fffdc, 22),
    h(0x7fffe8, 23),
    h(0x7fffe9, 23),
    h(0x1fffde, 21),
    h(0x7fffea, 23),
    h(0x3fffdd, 22),
    h(0x3fffde, 22),
    h(0xfffff0, 24),
    h(0x1fffdf, 21),
    h(0x3fffdf, 22),
    h(0x7fffeb, 23),
    h(0x7fffec, 23),
    // 176..191
    h(0x1fffe0, 21),
    h(0x1fffe1, 21),
    h(0x3fffe0, 22),
    h(0x1fffe2, 21),
    h(0x7fffed, 23),
    h(0x3fffe1, 22),
    h(0x7fffee, 23),
    h(0x7fffef, 23),
    h(0xfffea, 20),
    h(0x3fffe2, 22),
    h(0x3fffe3, 22),
    h(0x3fffe4, 22),
    h(0x7ffff0, 23),
    h(0x3fffe5, 22),
    h(0x3fffe6, 22),
    h(0x7ffff1, 23),
    // 192..207
    h(0x3ffffe0, 26),
    h(0x3ffffe1, 26),
    h(0xfffeb, 20),
    h(0x7fff1, 19),
    h(0x3fffe7, 22),
    h(0x7ffff2, 23),
    h(0x3fffe8, 22),
    h(0x1ffffec, 25),
    h(0x3ffffe2, 26),
    h(0x3ffffe3, 26),
    h(0x3ffffe4, 26),
    h(0x7ffffde, 27),
    h(0x7ffffdf, 27),
    h(0x3ffffe5, 26),
    h(0xfffff1, 24),
    h(0x1ffffed, 25),
    // 208..223
    h(0x7fff2, 19),
    h(0x1fffe3, 21),
    h(0x3ffffe6, 26),
    h(0x7ffffe0, 27),
    h(0x7ffffe1, 27),
    h(0x3ffffe7, 26),
    h(0x7ffffe2, 27),
    h(0xfffff2, 24),
    h(0x1fffe4, 21),
    h(0x1fffe5, 21),
    h(0x3ffffe8, 26),
    h(0x3ffffe9, 26),
    h(0xffffffd, 28),
    h(0x7ffffe3, 27),
    h(0x7ffffe4, 27),
    h(0x7ffffe5, 27),
    // 224..239
    h(0xfffec, 20),
    h(0xfffff3, 24),
    h(0xfffed, 20),
    h(0x1fffe6, 21),
    h(0x3fffe9, 22),
    h(0x1fffe7, 21),
    h(0x1fffe8, 21),
    h(0x7ffff3, 23),
    h(0x3fffea, 22),
    h(0x3fffeb, 22),
    h(0x1ffffee, 25),
    h(0x1ffffef, 25),
    h(0xfffff4, 24),
    h(0xfffff5, 24),
    h(0x3ffffea, 26),
    h(0x7ffff4, 23),
    // 240..255
    h(0x3ffffeb, 26),
    h(0x7ffffe6, 27),
    h(0x3ffffec, 26),
    h(0x3ffffed, 26),
    h(0x7ffffe7, 27),
    h(0x7ffffe8, 27),
    h(0x7ffffe9, 27),
    h(0x7ffffea, 27),
    h(0x7ffffeb, 27),
    h(0xffffffe, 28),
    h(0x7ffffec, 27),
    h(0x7ffffed, 27),
    h(0x7ffffee, 27),
    h(0x7ffffef, 27),
    h(0x7fffff0, 27),
    h(0x3ffffee, 26),
    // 256 — EOS
    h(0x3fffffff, 30),
];

/// HPACK errors that bubble up to the anchor as connection-level
/// PROTOCOL_ERROR (sent as GOAWAY).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HpackError {
    Truncated,
    InvalidIndex,
    InvalidHuffman,
    IntegerOverflow,
    OutputOverflow,
}

/// Decode an HPACK integer (RFC 7541 §5.1). `prefix_bits` is the
/// number of bits in the first byte that hold the value (1..=8). The
/// caller already provides the first byte with the prefix bits
/// extracted (i.e. `first = byte & ((1 << prefix_bits) - 1)`).
/// Returns `(value, bytes_consumed_after_first_byte)`.
pub fn hpack_decode_int(
    first: u8,
    prefix_bits: u8,
    rest: &[u8],
) -> Result<(u64, usize), HpackError> {
    let mask = (1u64 << prefix_bits) - 1;
    let mut value = u64::from(first) & mask;
    if value < mask {
        return Ok((value, 0));
    }
    let mut shift: u32 = 0;
    let mut consumed = 0;
    while let Some(&b) = rest.get(consumed) {
        consumed += 1;
        if shift >= 64 {
            return Err(HpackError::IntegerOverflow);
        }
        value = value
            .checked_add((u64::from(b & 0x7F)) << shift)
            .ok_or(HpackError::IntegerOverflow)?;
        if (b & 0x80) == 0 {
            return Ok((value, consumed));
        }
        shift += 7;
    }
    Err(HpackError::Truncated)
}

/// Encode an HPACK integer (RFC 7541 §5.1). `prefix_bits` is 1..=8.
/// `flags` is the OR'd into the prefix-bit-aligned high bits of the
/// first byte (typically the representation discriminator like
/// 0b1000_0000 for Indexed Header Field). Writes 1..=11 bytes.
pub fn hpack_encode_int(
    out: &mut [u8],
    off: &mut usize,
    mut value: u64,
    prefix_bits: u8,
    flags: u8,
) -> Option<()> {
    let mask = (1u64 << prefix_bits) - 1;
    let slot = out.get_mut(*off)?;
    if value < mask {
        *slot = flags | (value as u8);
        *off += 1;
        return Some(());
    }
    *slot = flags | (mask as u8);
    *off += 1;
    value -= mask;
    while value >= 0x80 {
        let s = out.get_mut(*off)?;
        *s = ((value & 0x7F) as u8) | 0x80;
        *off += 1;
        value >>= 7;
    }
    let s = out.get_mut(*off)?;
    *s = value as u8;
    *off += 1;
    Some(())
}

/// Decode a Huffman-encoded byte string per RFC 7541 §5.2 into `out`,
/// advancing `out_off`. Padding bits at the end must be the most-
/// significant bits of EOS (all 1s); any other trailing pattern is
/// invalid.
pub fn huffman_decode(input: &[u8], out: &mut [u8], out_off: &mut usize) -> Result<(), HpackError> {
    let mut bits: u64 = 0;
    let mut nbits: u8 = 0;
    let mut pos = 0;
    loop {
        // Refill the bit buffer.
        while nbits <= 56 {
            let Some(&byte) = input.get(pos) else {
                break;
            };
            bits = (bits << 8) | u64::from(byte);
            nbits += 8;
            pos += 1;
        }
        if nbits == 0 {
            return Ok(());
        }
        // Try to match a prefix. Shortest HPACK code is 5 bits; longest
        // (non-EOS) byte code is 30 bits. We scan from short to long
        // and emit the first match (codes are prefix-free).
        let mut matched = false;
        for try_len in 5u8..=30 {
            if nbits < try_len {
                break;
            }
            let code = (bits >> (nbits - try_len)) & ((1u64 << try_len) - 1);
            for (byte, entry) in HPACK_HUFFMAN_TABLE.iter().enumerate().take(256) {
                if entry.bits == try_len && u64::from(entry.code) == code {
                    let slot = out.get_mut(*out_off).ok_or(HpackError::OutputOverflow)?;
                    *slot = byte as u8;
                    *out_off += 1;
                    nbits -= try_len;
                    bits &= (1u64 << nbits).wrapping_sub(1);
                    matched = true;
                    break;
                }
            }
            if matched {
                break;
            }
        }
        if matched {
            continue;
        }
        // No prefix matched. The leftover bits may be EOS padding —
        // valid iff `nbits < 8`, input is fully consumed, AND those
        // bits are the most-significant bits of EOS (which is all-1s).
        // Per RFC 7541 §5.2 padding strictly less than 8 bits long
        // must equal the corresponding MSBs of EOS.
        if pos == input.len() && nbits < 8 {
            let pad_mask = (1u64 << nbits) - 1;
            let pad = bits & pad_mask;
            if pad == pad_mask {
                return Ok(());
            }
        }
        return Err(HpackError::InvalidHuffman);
    }
}

/// Decode an HPACK string literal (RFC 7541 §5.2) starting at
/// `bytes[0]`. Returns `(slice_into_temp_or_input, bytes_consumed)`.
/// When the literal is Huffman-coded the decoder writes the decoded
/// bytes into `scratch`; the returned slice borrows from `scratch`.
/// When the literal is raw, the slice borrows from `bytes` directly.
pub fn hpack_decode_string<'a>(
    bytes: &'a [u8],
    scratch: &'a mut [u8],
    scratch_off: &mut usize,
) -> Result<(&'a [u8], usize), HpackError> {
    let &first = bytes.first().ok_or(HpackError::Truncated)?;
    let huffman = (first & 0x80) != 0;
    let rest = bytes.get(1..).ok_or(HpackError::Truncated)?;
    let (len, n) = hpack_decode_int(first & 0x7F, 7, rest)?;
    let header_len = 1 + n;
    let body_end = header_len
        .checked_add(len as usize)
        .ok_or(HpackError::IntegerOverflow)?;
    let raw = bytes
        .get(header_len..body_end)
        .ok_or(HpackError::Truncated)?;
    if huffman {
        let start = *scratch_off;
        huffman_decode(raw, scratch, scratch_off)?;
        let end = *scratch_off;
        let slice = scratch.get(start..end).ok_or(HpackError::OutputOverflow)?;
        Ok((slice, body_end))
    } else {
        // Copy into scratch so the caller can mix raw + huffman
        // strings in one block without lifetime gymnastics.
        let start = *scratch_off;
        let end = start
            .checked_add(raw.len())
            .ok_or(HpackError::OutputOverflow)?;
        let dst = scratch
            .get_mut(start..end)
            .ok_or(HpackError::OutputOverflow)?;
        dst.copy_from_slice(raw);
        *scratch_off = end;
        let slice = scratch.get(start..end).ok_or(HpackError::OutputOverflow)?;
        Ok((slice, body_end))
    }
}

/// HPACK header-block decoder. Allocates no dynamic table (we
/// advertise SETTINGS_HEADER_TABLE_SIZE = 0 to the peer); dynamic
/// references would therefore be a protocol error.
pub struct HpackDecoder;

impl HpackDecoder {
    pub const fn new() -> Self {
        Self
    }

    /// Iterate the instructions in `block`, calling `sink(name, value)`
    /// for each emitted header. Names and values borrow either from
    /// `block` (raw literal) or from `scratch` (Huffman literal or
    /// post-resolution static name). `scratch` must be large enough to
    /// hold all decoded strings simultaneously (a few KB is plenty
    /// for gRPC HEADERS).
    pub fn decode_block(
        &mut self,
        block: &[u8],
        scratch: &mut [u8],
        mut sink: impl FnMut(&[u8], &[u8]),
    ) -> Result<(), HpackError> {
        let mut off = 0;
        let mut scratch_off: usize = 0;
        while let Some(&b) = block.get(off) {
            let next_rest = block.get(off + 1..).ok_or(HpackError::Truncated)?;
            if (b & 0x80) != 0 {
                // 6.1 Indexed Header Field Representation
                let (idx, n) = hpack_decode_int(b & 0x7F, 7, next_rest)?;
                off += 1 + n;
                if idx > u64::from(u8::MAX) {
                    return Err(HpackError::InvalidIndex);
                }
                let e = hpack_static_entry(idx as u8).ok_or(HpackError::InvalidIndex)?;
                sink(e.name, e.value);
            } else if (b & 0xC0) == 0x40 {
                // 6.2.1 Literal Header Field with Incremental Indexing
                let (idx, n) = hpack_decode_int(b & 0x3F, 6, next_rest)?;
                off += 1 + n;
                self.decode_literal(idx, &mut off, block, scratch, &mut scratch_off, &mut sink)?;
            } else if (b & 0xE0) == 0x20 {
                // 6.3 Dynamic Table Size Update — we don't keep a
                // table; sanity-check the value fits in u32 and skip.
                let (_size, n) = hpack_decode_int(b & 0x1F, 5, next_rest)?;
                off += 1 + n;
            } else if (b & 0xF0) == 0x10 {
                // 6.2.3 Literal Header Field Never Indexed
                let (idx, n) = hpack_decode_int(b & 0x0F, 4, next_rest)?;
                off += 1 + n;
                self.decode_literal(idx, &mut off, block, scratch, &mut scratch_off, &mut sink)?;
            } else {
                // 6.2.2 Literal Header Field without Indexing (0000 xxxx)
                let (idx, n) = hpack_decode_int(b & 0x0F, 4, next_rest)?;
                off += 1 + n;
                self.decode_literal(idx, &mut off, block, scratch, &mut scratch_off, &mut sink)?;
            }
        }
        Ok(())
    }

    fn decode_literal(
        &self,
        name_idx: u64,
        off: &mut usize,
        block: &[u8],
        scratch: &mut [u8],
        scratch_off: &mut usize,
        sink: &mut impl FnMut(&[u8], &[u8]),
    ) -> Result<(), HpackError> {
        if name_idx == 0 {
            // Name is a literal string. Track scratch offsets before
            // each parse so we can re-borrow both slices after the
            // second parse drops the first borrow.
            let name_start = *scratch_off;
            let rest1 = block.get(*off..).ok_or(HpackError::Truncated)?;
            let (_, n_consumed) = hpack_decode_string(rest1, scratch, scratch_off)?;
            let name_end = *scratch_off;
            *off += n_consumed;
            let value_start = *scratch_off;
            let rest2 = block.get(*off..).ok_or(HpackError::Truncated)?;
            let (_, v_consumed) = hpack_decode_string(rest2, scratch, scratch_off)?;
            let value_end = *scratch_off;
            *off += v_consumed;
            // Re-borrow without aliasing: borrow the joint region
            // and split it at the name/value boundary.
            let region = scratch
                .get(name_start..value_end)
                .ok_or(HpackError::OutputOverflow)?;
            let mid = name_end
                .checked_sub(name_start)
                .ok_or(HpackError::OutputOverflow)?;
            let name_part = region.get(..mid).ok_or(HpackError::OutputOverflow)?;
            let v_mid = value_start
                .checked_sub(name_start)
                .ok_or(HpackError::OutputOverflow)?;
            let value_part = region.get(v_mid..).ok_or(HpackError::OutputOverflow)?;
            sink(name_part, value_part);
            Ok(())
        } else {
            if name_idx > u64::from(u8::MAX) {
                return Err(HpackError::InvalidIndex);
            }
            let entry = hpack_static_entry(name_idx as u8).ok_or(HpackError::InvalidIndex)?;
            let rest = block.get(*off..).ok_or(HpackError::Truncated)?;
            let (value, v_consumed) = hpack_decode_string(rest, scratch, scratch_off)?;
            *off += v_consumed;
            sink(entry.name, value);
            Ok(())
        }
    }
}

impl Default for HpackDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode a "Literal Header Field without Indexing" (RFC 7541 §6.2.2)
/// with both name and value as raw (non-Huffman) literal strings.
/// This is the only representation Lattice's encoder uses; it's the
/// most permissive and the response payload is small enough that
/// Huffman encoding wouldn't pay for itself.
pub fn hpack_encode_literal(
    out: &mut [u8],
    off: &mut usize,
    name: &[u8],
    value: &[u8],
) -> Option<()> {
    hpack_encode_int(out, off, 0, 4, 0x00)?;
    hpack_encode_int(out, off, name.len() as u64, 7, 0x00)?;
    let n_end = (*off).checked_add(name.len())?;
    out.get_mut(*off..n_end)?.copy_from_slice(name);
    *off = n_end;
    hpack_encode_int(out, off, value.len() as u64, 7, 0x00)?;
    let v_end = (*off).checked_add(value.len())?;
    out.get_mut(*off..v_end)?.copy_from_slice(value);
    *off = v_end;
    Some(())
}

/// Encode an "Indexed Header Field" (RFC 7541 §6.1) referencing a
/// static-table entry. Used for the common `:status 200` /
/// `content-type application/grpc` headers in responses.
pub fn hpack_encode_indexed(out: &mut [u8], off: &mut usize, index: u64) -> Option<()> {
    hpack_encode_int(out, off, index, 7, 0x80)
}

// ── gRPC trailer + response framing helpers ───────────────────────────
//
// gRPC over HTTP/2 server responses are exactly two HEADERS frames
// surrounding a sequence of DATA frames:
//
//   HEADERS  : :status 200, content-type application/grpc (END_HEADERS)
//   DATA     : gRPC framed protobuf payload (END_STREAM == 0)
//   …
//   HEADERS  : grpc-status 0[, grpc-message …] (END_HEADERS, END_STREAM)
//
// All three frames are stream-scoped (non-zero stream_id matching the
// request).

/// Build a "response headers" HEADERS frame block body. Does NOT
/// include the 9-byte frame header — the caller wraps the returned
/// `[..off]` slice in a FrameHeader. `:status` and `content-type`
/// are emitted as static-indexed entries (HPACK indices 8 and 31).
/// content-type is sent as a literal because the static value
/// (`application/grpc`) doesn't exist in the static table.
pub fn build_response_headers_block(out: &mut [u8], off: &mut usize) -> Option<()> {
    // :status 200
    hpack_encode_indexed(out, off, 8)?;
    // content-type: application/grpc
    hpack_encode_literal(out, off, b"content-type", b"application/grpc")?;
    Some(())
}

/// Build the "gRPC trailers" HEADERS frame block body.
/// grpc-status = 0 is the only success case; non-zero is mapped from
/// `KV_RESULT_*` at the anchor. Per the gRPC spec, the value is the
/// **decimal ASCII** of the integer — so 12 (UNIMPLEMENTED) is the
/// two-byte string "12", not the single byte "2".
pub fn build_grpc_trailers_block(out: &mut [u8], off: &mut usize, grpc_status: u8) -> Option<()> {
    let mut digits = [0u8; 3];
    let mut len = 0;
    if grpc_status == 0 {
        digits[0] = b'0';
        len = 1;
    } else {
        let mut v = grpc_status;
        let mut tmp = [0u8; 3];
        let mut t = 0;
        while v > 0 {
            *tmp.get_mut(t)? = b'0' + (v % 10);
            t += 1;
            v /= 10;
        }
        while len < t {
            *digits.get_mut(len)? = *tmp.get(t - 1 - len)?;
            len += 1;
        }
    }
    hpack_encode_literal(out, off, b"grpc-status", digits.get(..len)?)
}

// ── Tests (host-only) ─────────────────────────────────────────────────
//
// PIC builds gate these behind `#[cfg(test)]`; the file is listed as
// an inline-tests exemption in `fluxor.toml` (see
// `standards/tests.md §3`).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn huffman_decodes_lease_keep_alive_path() {
        // Captured from tonic on the wire: 24 bytes of Huffman for
        // `/etcdserverpb.Lease/LeaseKeepAlive`. Used as a regression
        // for the `'A'` table entry — RFC 7541 Appendix B records
        // `'A'` as a 6-bit code (`100001`); an earlier draft of this
        // table had it as 7 bits (`0100001`) and rejected every header
        // value containing an uppercase 'A'.
        let bytes: &[u8] = &[
            0x60, 0xa9, 0x24, 0x88, 0x2d, 0x9d, 0xcb, 0x65, 0x71, 0xaf, 0x9c, 0xa3, 0x41, 0x58,
            0xce, 0x51, 0xa0, 0xb9, 0x8a, 0x5a, 0xe1, 0xa0, 0xdd, 0xcb,
        ];
        let mut out = [0u8; 128];
        let mut off = 0;
        huffman_decode(bytes, &mut out, &mut off).expect("decode");
        let s = core::str::from_utf8(&out[..off]).expect("utf8");
        assert_eq!(s, "/etcdserverpb.Lease/LeaseKeepAlive");
    }

    #[test]
    fn preface_round_trip() {
        assert!(preface_matches(HTTP2_PREFACE));
        assert!(!preface_matches(b"GET / HTTP/1.1\r\n"));
        assert!(!preface_matches(&HTTP2_PREFACE[..10]));
    }

    #[test]
    fn frame_header_round_trip() {
        let original = FrameHeader {
            length: 0x12_3456,
            frame_type: FrameType::Headers as u8,
            flags: FLAG_END_HEADERS | FLAG_END_STREAM,
            stream_id: 0x0EAD_BEEF,
        };
        let mut buf = [0u8; FRAME_HEADER_LEN];
        original.encode(&mut buf).unwrap();
        let parsed = FrameHeader::parse(&buf).unwrap();
        assert_eq!(parsed.length, original.length);
        assert_eq!(parsed.frame_type, original.frame_type);
        assert_eq!(parsed.flags, original.flags);
        assert_eq!(parsed.stream_id, original.stream_id);
    }

    #[test]
    fn frame_header_masks_reserved_bit() {
        let mut buf = [0u8; FRAME_HEADER_LEN];
        buf[5] = 0x80; // R bit set
        buf[6..9].copy_from_slice(&[0x00, 0x00, 0x01]);
        let parsed = FrameHeader::parse(&buf).unwrap();
        assert_eq!(parsed.stream_id, 1, "reserved bit must be masked");
    }

    #[test]
    fn settings_iter_walks_pairs() {
        // Two pairs: (MAX_CONCURRENT_STREAMS, 100), (INITIAL_WINDOW_SIZE, 65535)
        let payload = [
            0x00, 0x03, 0x00, 0x00, 0x00, 0x64, // (3, 100)
            0x00, 0x04, 0x00, 0x00, 0xFF, 0xFF, // (4, 65535)
        ];
        let mut iter = SettingsIter::new(&payload);
        assert_eq!(iter.next(), Some((SETTINGS_MAX_CONCURRENT_STREAMS, 100)));
        assert_eq!(iter.next(), Some((SETTINGS_INITIAL_WINDOW_SIZE, 65535)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn settings_ack_is_empty_and_acked() {
        let mut out = [0u8; FRAME_HEADER_LEN];
        build_settings_ack(&mut out).unwrap();
        let h = FrameHeader::parse(&out).unwrap();
        assert_eq!(h.length, 0);
        assert_eq!(h.frame_type, FrameType::Settings as u8);
        assert!(h.has_flag(FLAG_ACK));
        assert_eq!(h.stream_id, 0);
    }

    #[test]
    fn ping_ack_echoes_payload() {
        let opaque = [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
        let mut out = [0u8; FRAME_HEADER_LEN + PING_PAYLOAD_LEN];
        build_ping_ack(&mut out, &opaque).unwrap();
        let h = FrameHeader::parse(&out).unwrap();
        assert_eq!(h.length, PING_PAYLOAD_LEN as u32);
        assert_eq!(h.frame_type, FrameType::Ping as u8);
        assert!(h.has_flag(FLAG_ACK));
        assert_eq!(&out[FRAME_HEADER_LEN..], &opaque);
    }

    #[test]
    fn goaway_carries_error_code() {
        let mut out = [0u8; FRAME_HEADER_LEN + 8];
        build_goaway(&mut out, 7, ERROR_PROTOCOL_ERROR).unwrap();
        let h = FrameHeader::parse(&out).unwrap();
        assert_eq!(h.frame_type, FrameType::Goaway as u8);
        assert_eq!(h.length, 8);
        // body: last_stream_id, error_code
        assert_eq!(&out[FRAME_HEADER_LEN..FRAME_HEADER_LEN + 4], &[0, 0, 0, 7]);
        assert_eq!(
            &out[FRAME_HEADER_LEN + 4..FRAME_HEADER_LEN + 8],
            &[0, 0, 0, ERROR_PROTOCOL_ERROR as u8]
        );
    }

    #[test]
    fn window_update_encodes_increment() {
        let mut out = [0u8; FRAME_HEADER_LEN + 4];
        build_window_update(&mut out, 0, 65536).unwrap();
        let h = FrameHeader::parse(&out).unwrap();
        assert_eq!(h.frame_type, FrameType::WindowUpdate as u8);
        assert_eq!(h.stream_id, 0);
        // increment = 65536 (0x00010000)
        assert_eq!(&out[FRAME_HEADER_LEN..], &[0x00, 0x01, 0x00, 0x00]);
    }

    #[test]
    fn grpc_header_round_trip() {
        let original = GrpcMessageHeader {
            compressed: 0,
            length: 123,
        };
        let mut buf = [0u8; GRPC_FRAME_HEADER_LEN];
        original.encode(&mut buf).unwrap();
        let parsed = GrpcMessageHeader::parse(&buf).unwrap();
        assert_eq!(parsed.compressed, 0);
        assert_eq!(parsed.length, 123);
    }

    #[test]
    fn varint_round_trip() {
        for v in [0u64, 1, 127, 128, 300, 16_383, 16_384, u64::MAX] {
            let mut buf = [0u8; 16];
            let mut off = 0;
            write_varint(&mut buf, &mut off, v).unwrap();
            let (got, n) = read_varint(&buf[..off]).unwrap();
            assert_eq!(got, v, "round-trip failed for {v}");
            assert_eq!(n, off);
        }
    }

    #[test]
    fn varint_rejects_overlong() {
        let buf = [0xFF; 11]; // 11 bytes never terminate
        assert!(read_varint(&buf).is_none());
    }

    #[test]
    fn tag_decode_handles_field_number_and_wire_type() {
        // (field 5, wire 2) → key = 5<<3 | 2 = 42 = 0x2A
        let buf = [0x2A];
        let (f, w, n) = read_tag(&buf).unwrap();
        assert_eq!(f, 5);
        assert_eq!(w, WIRE_LENGTH_DELIMITED);
        assert_eq!(n, 1);

        // (field 16, wire 0) → key = 128, varint two bytes
        let buf = [0x80, 0x01];
        let (f, w, _) = read_tag(&buf).unwrap();
        assert_eq!(f, 16);
        assert_eq!(w, WIRE_VARINT);
    }

    #[test]
    fn length_delimited_round_trip() {
        let mut buf = [0u8; 32];
        let mut off = 0;
        write_length_delimited(&mut buf, &mut off, 1, b"hello").unwrap();
        // Read back: tag, then length-delimited
        let (f, w, n) = read_tag(&buf[..off]).unwrap();
        assert_eq!(f, 1);
        assert_eq!(w, WIRE_LENGTH_DELIMITED);
        let (data, _) = read_length_delimited(&buf[n..off]).unwrap();
        assert_eq!(data, b"hello");
    }

    #[test]
    fn skip_field_advances_correctly() {
        // varint field
        let buf = [0x96, 0x01]; // 150
        assert_eq!(skip_field(&buf, WIRE_VARINT), Some(2));

        // length-delimited
        let buf = [0x03, b'a', b'b', b'c'];
        assert_eq!(skip_field(&buf, WIRE_LENGTH_DELIMITED), Some(4));

        // unknown wire type → reject
        assert_eq!(skip_field(&buf, 1), None); // 64-bit
    }

    // ── KV protobuf subset round-trips ────────────────────────────────

    #[test]
    fn range_request_decodes_key_only() {
        // Encode `{ key = "foo" }` by hand: tag 1 + length-delimited.
        let mut buf = [0u8; 16];
        let mut off = 0;
        write_length_delimited(&mut buf, &mut off, 1, b"foo").unwrap();
        let req = RangeRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.key, b"foo");
        assert_eq!(req.range_end, b"");
        assert_eq!(req.limit, 0);
        assert_eq!(req.revision, 0);
    }

    #[test]
    fn range_request_decodes_key_and_limit() {
        let mut buf = [0u8; 32];
        let mut off = 0;
        write_length_delimited(&mut buf, &mut off, 1, b"k").unwrap();
        write_tag(&mut buf, &mut off, 3, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 42).unwrap();
        let req = RangeRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.key, b"k");
        assert_eq!(req.limit, 42);
    }

    #[test]
    fn range_request_skips_unknown_fields() {
        // Inject a field 99 (varint) between known fields — decoder
        // must skip it cleanly.
        let mut buf = [0u8; 32];
        let mut off = 0;
        write_length_delimited(&mut buf, &mut off, 1, b"k").unwrap();
        write_tag(&mut buf, &mut off, 99, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 0xDEAD).unwrap();
        write_tag(&mut buf, &mut off, 4, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 7).unwrap();
        let req = RangeRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.key, b"k");
        assert_eq!(req.revision, 7);
    }

    #[test]
    fn put_request_decodes_key_value() {
        let mut buf = [0u8; 32];
        let mut off = 0;
        write_length_delimited(&mut buf, &mut off, 1, b"k").unwrap();
        write_length_delimited(&mut buf, &mut off, 2, b"v").unwrap();
        let req = PutRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.key, b"k");
        assert_eq!(req.value, b"v");
        assert_eq!(req.lease, 0);
        assert!(!req.prev_kv);
        assert!(!req.ignore_value);
    }

    #[test]
    fn put_request_decodes_options() {
        let mut buf = [0u8; 32];
        let mut off = 0;
        write_length_delimited(&mut buf, &mut off, 1, b"k").unwrap();
        write_length_delimited(&mut buf, &mut off, 2, b"v").unwrap();
        write_tag(&mut buf, &mut off, 3, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 11).unwrap();
        write_tag(&mut buf, &mut off, 4, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 1).unwrap();
        let req = PutRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.lease, 11);
        assert!(req.prev_kv);
    }

    #[test]
    fn delete_range_request_decodes_key_only() {
        let mut buf = [0u8; 16];
        let mut off = 0;
        write_length_delimited(&mut buf, &mut off, 1, b"k").unwrap();
        let req = DeleteRangeRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.key, b"k");
        assert!(!req.prev_kv);
    }

    #[test]
    fn range_response_builds_with_kv() {
        let mut out = [0u8; 128];
        let mut off = 0;
        build_range_response(&mut out, &mut off, 42, Some((b"k", b"v", 41, 2))).unwrap();
        // Just round-trip the top-level fields through the decoder
        // logic — header (field 1) length-delimited; we won't model
        // the full RangeResponse decoder, but verify the bytes parse
        // as protobuf without overruns.
        let mut buf = &out[..off];
        let mut header_seen = false;
        let mut count_seen = false;
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf).unwrap();
            buf = &buf[n..];
            match (field, wire) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let (_, n) = read_length_delimited(buf).unwrap();
                    header_seen = true;
                    buf = &buf[n..];
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    let (_, n) = read_length_delimited(buf).unwrap();
                    buf = &buf[n..];
                }
                (4, WIRE_VARINT) => {
                    let (v, n) = read_varint(buf).unwrap();
                    assert_eq!(v, 1, "count must equal 1");
                    count_seen = true;
                    buf = &buf[n..];
                }
                _ => {
                    let n = skip_field(buf, wire).unwrap();
                    buf = &buf[n..];
                }
            }
        }
        assert!(header_seen);
        assert!(count_seen);
    }

    #[test]
    fn range_response_builds_with_no_kv() {
        let mut out = [0u8; 64];
        let mut off = 0;
        build_range_response(&mut out, &mut off, 0, None).unwrap();
        // count = 0 must still be present (final field).
        let mut buf = &out[..off];
        let mut count_seen = false;
        while !buf.is_empty() {
            let (field, wire, n) = read_tag(buf).unwrap();
            buf = &buf[n..];
            if field == 4 && wire == WIRE_VARINT {
                let (v, n) = read_varint(buf).unwrap();
                assert_eq!(v, 0);
                count_seen = true;
                buf = &buf[n..];
            } else {
                let n = skip_field(buf, wire).unwrap();
                buf = &buf[n..];
            }
        }
        assert!(count_seen);
    }

    #[test]
    fn put_response_carries_only_header() {
        let mut out = [0u8; 32];
        let mut off = 0;
        build_put_response(&mut out, &mut off, 99).unwrap();
        let (field, wire, _) = read_tag(&out[..off]).unwrap();
        assert_eq!(field, 1);
        assert_eq!(wire, WIRE_LENGTH_DELIMITED);
    }

    #[test]
    fn delete_range_response_carries_deleted_count() {
        let mut out = [0u8; 64];
        let mut off = 0;
        build_delete_range_response(&mut out, &mut off, 1, 3).unwrap();
        // Skip header; expect field 2 = deleted = 3.
        let mut buf = &out[..off];
        let (field, _wire, n) = read_tag(buf).unwrap();
        assert_eq!(field, 1);
        let (_, m) = read_length_delimited(&buf[n..]).unwrap();
        buf = &buf[n + m..];
        let (field, wire, n) = read_tag(buf).unwrap();
        assert_eq!(field, 2);
        assert_eq!(wire, WIRE_VARINT);
        let (v, _) = read_varint(&buf[n..]).unwrap();
        assert_eq!(v, 3);
    }

    // ── HPACK round-trips (RFC 7541 Appendix C examples) ─────────────

    #[test]
    fn hpack_integer_round_trips() {
        // RFC 7541 Appendix C.1.1: 10 in a 5-bit prefix → 0x0A.
        let mut out = [0u8; 8];
        let mut off = 0;
        hpack_encode_int(&mut out, &mut off, 10, 5, 0x00).unwrap();
        assert_eq!(&out[..off], &[0x0A]);
        let (v, n) = hpack_decode_int(out[0] & 0x1F, 5, &out[1..off]).unwrap();
        assert_eq!(v, 10);
        assert_eq!(n, 0);

        // C.1.2: 1337 in a 5-bit prefix → 0x1F 0x9A 0x0A.
        let mut out = [0u8; 8];
        let mut off = 0;
        hpack_encode_int(&mut out, &mut off, 1337, 5, 0x00).unwrap();
        assert_eq!(&out[..off], &[0x1F, 0x9A, 0x0A]);
        let (v, n) = hpack_decode_int(out[0] & 0x1F, 5, &out[1..off]).unwrap();
        assert_eq!(v, 1337);
        assert_eq!(n, 2);

        // C.1.3: 42 in 8-bit prefix → 0x2A.
        let mut out = [0u8; 8];
        let mut off = 0;
        hpack_encode_int(&mut out, &mut off, 42, 8, 0x00).unwrap();
        assert_eq!(&out[..off], &[0x2A]);
        let (v, n) = hpack_decode_int(out[0], 8, &out[1..off]).unwrap();
        assert_eq!(v, 42);
        assert_eq!(n, 0);
    }

    #[test]
    fn hpack_decoder_handles_indexed_header() {
        // RFC 7541 Appendix C.2.4: indexed `:method GET` (static 2).
        let block = [0x82];
        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 64];
        let mut headers: std::vec::Vec<(std::vec::Vec<u8>, std::vec::Vec<u8>)> =
            std::vec::Vec::new();
        decoder
            .decode_block(&block, &mut scratch, |n, v| {
                headers.push((n.to_vec(), v.to_vec()));
            })
            .unwrap();
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0.as_slice(), b":method");
        assert_eq!(headers[0].1.as_slice(), b"GET");
    }

    #[test]
    fn hpack_decoder_handles_literal_with_indexing_unhuffmaned() {
        // RFC 7541 Appendix C.2.1: literal header field with incremental
        // indexing, "custom-key: custom-header".
        //
        //   40                                       | == Literal indexed ==
        //   0a                                       |   Literal name (len = 10)
        //   63 75 73 74 6f 6d 2d 6b 65 79            | custom-key
        //   0d                                       |   Literal value (len = 13)
        //   63 75 73 74 6f 6d 2d 68 65 61 64 65 72   | custom-header
        let block = [
            0x40, 0x0a, b'c', b'u', b's', b't', b'o', b'm', b'-', b'k', b'e', b'y', 0x0d, b'c',
            b'u', b's', b't', b'o', b'm', b'-', b'h', b'e', b'a', b'd', b'e', b'r',
        ];
        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 128];
        let mut name = [0u8; 32];
        let mut value = [0u8; 32];
        let mut name_len = 0;
        let mut value_len = 0;
        let mut count = 0;
        decoder
            .decode_block(&block, &mut scratch, |n, v| {
                name[..n.len()].copy_from_slice(n);
                name_len = n.len();
                value[..v.len()].copy_from_slice(v);
                value_len = v.len();
                count += 1;
            })
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(&name[..name_len], b"custom-key");
        assert_eq!(&value[..value_len], b"custom-header");
    }

    #[test]
    fn hpack_decoder_handles_literal_name_indexed_value_raw() {
        // RFC 7541 Appendix C.2.2: literal header field without
        // indexing, name indexed from static (:path = 4), value raw.
        //
        //   04                                       | name index 4 (in 4-bit prefix)
        //   0c                                       |   value len 12
        //   2f 73 61 6d 70 6c 65 2f 70 61 74 68      | /sample/path
        let block = [
            0x04, 0x0c, b'/', b's', b'a', b'm', b'p', b'l', b'e', b'/', b'p', b'a', b't', b'h',
        ];
        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 64];
        let mut got_name = [0u8; 16];
        let mut got_value = [0u8; 32];
        let mut nl = 0;
        let mut vl = 0;
        decoder
            .decode_block(&block, &mut scratch, |n, v| {
                got_name[..n.len()].copy_from_slice(n);
                nl = n.len();
                got_value[..v.len()].copy_from_slice(v);
                vl = v.len();
            })
            .unwrap();
        assert_eq!(&got_name[..nl], b":path");
        assert_eq!(&got_value[..vl], b"/sample/path");
    }

    #[test]
    fn hpack_huffman_decodes_known_string() {
        // RFC 7541 Appendix C.4.1 (request 1): :authority value
        // "www.example.com" Huffman-encoded as 12 bytes:
        //   f1 e3 c2 e5 f2 3a 6b a0 ab 90 f4 ff
        let raw = [
            0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
        ];
        let mut out = [0u8; 32];
        let mut off = 0;
        huffman_decode(&raw, &mut out, &mut off).unwrap();
        assert_eq!(&out[..off], b"www.example.com");
    }

    #[test]
    fn hpack_decoder_handles_huffman_literal_string() {
        // RFC 7541 Appendix C.4.1: a literal header field with
        // indexing where the value (:authority) is Huffman-encoded.
        //   41                                  | name index = 1 (:authority)
        //   8c                                  | H=1, len=12
        //   f1 e3 c2 e5 f2 3a 6b a0 ab 90 f4 ff | "www.example.com"
        let block = [
            0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff,
        ];
        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 64];
        let mut name = [0u8; 32];
        let mut value = [0u8; 32];
        let mut nl = 0;
        let mut vl = 0;
        decoder
            .decode_block(&block, &mut scratch, |n, v| {
                name[..n.len()].copy_from_slice(n);
                nl = n.len();
                value[..v.len()].copy_from_slice(v);
                vl = v.len();
            })
            .unwrap();
        assert_eq!(&name[..nl], b":authority");
        assert_eq!(&value[..vl], b"www.example.com");
    }

    #[test]
    fn hpack_decoder_handles_dynamic_table_size_update() {
        // 0x20 = `001_00000` Dynamic Table Size Update with value 0
        // (no continuation byte). The decoder must consume it cleanly
        // and emit no headers.
        let block = [0x20, 0x82];
        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 16];
        let mut count = 0;
        let mut name = [0u8; 16];
        let mut value = [0u8; 16];
        let mut nl = 0;
        let mut vl = 0;
        decoder
            .decode_block(&block, &mut scratch, |n, v| {
                name[..n.len()].copy_from_slice(n);
                nl = n.len();
                value[..v.len()].copy_from_slice(v);
                vl = v.len();
                count += 1;
            })
            .unwrap();
        assert_eq!(
            count, 1,
            "second instruction (indexed :method GET) emits 1 header"
        );
        assert_eq!(&name[..nl], b":method");
        assert_eq!(&value[..vl], b"GET");
    }

    #[test]
    fn hpack_decoder_full_request_headers() {
        // A realistic gRPC client HEADERS block — five pseudo-headers
        // plus content-type, te, grpc-encoding. Pre-encoded by hand:
        //
        //   83                                       | :method POST (static 3)
        //   86                                       | :scheme http (static 6)
        //   44 0c 2f h.K.V/Range                     | :path /etcdserverpb.KV/Range
        //   41 0e example.host:80                    | :authority literal
        //   5f h0 application/grpc                   | content-type literal
        //   40 02 te 08 trailers                     | te: trailers literal name+value
        //
        // Verify the decoder yields all six in order.
        let block = [
            0x83, 0x86, 0x04, 0x16, b'/', b'e', b't', b'c', b'd', b's', b'e', b'r', b'v', b'e',
            b'r', b'p', b'b', b'.', b'K', b'V', b'/', b'R', b'a', b'n', b'g', b'e', 0x41, 0x0f,
            b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'h', b'o', b's', b't', b':', b'8',
            b'0',
            // content-type literal-without-indexing, name index 31 (4-bit prefix).
            0x0f, 0x10, 0x10, b'a', b'p', b'p', b'l', b'i', b'c', b'a', b't', b'i', b'o', b'n',
            b'/', b'g', b'r', b'p', b'c',
            // te: trailers as literal-without-indexing both literal.
            0x00, 0x02, b't', b'e', 0x08, b't', b'r', b'a', b'i', b'l', b'e', b'r', b's',
        ];
        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 256];
        let mut headers: std::vec::Vec<(std::vec::Vec<u8>, std::vec::Vec<u8>)> =
            std::vec::Vec::new();
        decoder
            .decode_block(&block, &mut scratch, |n, v| {
                headers.push((n.to_vec(), v.to_vec()));
            })
            .unwrap();
        assert_eq!(headers.len(), 6);
        assert_eq!(headers[0].0.as_slice(), b":method");
        assert_eq!(headers[0].1.as_slice(), b"POST");
        assert_eq!(headers[1].0.as_slice(), b":scheme");
        assert_eq!(headers[1].1.as_slice(), b"http");
        assert_eq!(headers[2].0.as_slice(), b":path");
        assert_eq!(headers[2].1.as_slice(), b"/etcdserverpb.KV/Range");
        assert_eq!(headers[3].0.as_slice(), b":authority");
        assert_eq!(headers[3].1.as_slice(), b"example.host:80");
        assert_eq!(headers[4].0.as_slice(), b"content-type");
        assert_eq!(headers[4].1.as_slice(), b"application/grpc");
        assert_eq!(headers[5].0.as_slice(), b"te");
        assert_eq!(headers[5].1.as_slice(), b"trailers");
    }

    #[test]
    fn hpack_encoder_response_headers() {
        // Encode the standard gRPC server response header set, then
        // decode and confirm the round-trip yields the same values.
        let mut out = [0u8; 64];
        let mut off = 0;
        build_response_headers_block(&mut out, &mut off).unwrap();

        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 64];
        let mut got_name = [0u8; 32];
        let mut got_value = [0u8; 32];
        let mut nl = 0;
        let mut vl = 0;
        let mut count = 0;
        decoder
            .decode_block(&out[..off], &mut scratch, |n, v| {
                if count == 0 {
                    assert_eq!(n, b":status");
                    assert_eq!(v, b"200");
                } else {
                    got_name[..n.len()].copy_from_slice(n);
                    nl = n.len();
                    got_value[..v.len()].copy_from_slice(v);
                    vl = v.len();
                }
                count += 1;
            })
            .unwrap();
        assert_eq!(count, 2);
        assert_eq!(&got_name[..nl], b"content-type");
        assert_eq!(&got_value[..vl], b"application/grpc");
    }

    #[test]
    fn hpack_encoder_grpc_trailers() {
        let mut out = [0u8; 32];
        let mut off = 0;
        build_grpc_trailers_block(&mut out, &mut off, 0).unwrap();
        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 64];
        let mut name = [0u8; 16];
        let mut value = [0u8; 16];
        let mut nl = 0;
        let mut vl = 0;
        decoder
            .decode_block(&out[..off], &mut scratch, |n, v| {
                name[..n.len()].copy_from_slice(n);
                nl = n.len();
                value[..v.len()].copy_from_slice(v);
                vl = v.len();
            })
            .unwrap();
        assert_eq!(&name[..nl], b"grpc-status");
        assert_eq!(&value[..vl], b"0");
    }

    #[test]
    fn grpc_trailers_encode_multi_digit_status() {
        // Regression for the bug where grpc-status 12 (UNIMPLEMENTED)
        // was encoded as the single character "2" — tonic interpreted
        // the response as grpc-status 2 (UNKNOWN). Per the gRPC spec
        // the value is decimal ASCII of the integer.
        let cases: [(u8, &[u8]); 5] = [
            (0, b"0"),
            (3, b"3"),
            (12, b"12"),
            (99, b"99"),
            (255, b"255"),
        ];
        for (status, expected) in cases {
            let mut out = [0u8; 32];
            let mut off = 0;
            build_grpc_trailers_block(&mut out, &mut off, status).unwrap();
            let mut decoder = HpackDecoder::new();
            let mut scratch = [0u8; 64];
            let mut got: std::vec::Vec<u8> = std::vec::Vec::new();
            decoder
                .decode_block(&out[..off], &mut scratch, |n, v| {
                    if n == b"grpc-status" {
                        got.extend_from_slice(v);
                    }
                })
                .unwrap();
            assert_eq!(got.as_slice(), expected, "status {status}");
        }
    }

    #[test]
    fn hpack_huffman_decodes_captured_127_0_0_1_2379() {
        // Captured :authority value bytes (10 bytes, Huffman-encoded
        // "127.0.0.1:2379") from a real tonic-emitted etcd request.
        let raw = [0x08, 0x9d, 0x5c, 0x0b, 0x81, 0x70, 0xdc, 0x13, 0x2e, 0xbf];
        let mut out = [0u8; 64];
        let mut off = 0;
        huffman_decode(&raw, &mut out, &mut off).unwrap();
        assert_eq!(&out[..off], b"127.0.0.1:2379");
    }

    #[test]
    fn hpack_huffman_decodes_captured_path() {
        // Captured :path value bytes (16 bytes, Huffman-encoded
        // "/etcdserverpb.KV/Range") from a real tonic emit.
        let raw = [
            0x60, 0xa9, 0x24, 0x88, 0x2d, 0x9d, 0xcb, 0x65, 0x71, 0xaf, 0x9b, 0x8b, 0x1b, 0x47,
            0x54, 0xc5,
        ];
        let mut out = [0u8; 64];
        let mut off = 0;
        huffman_decode(&raw, &mut out, &mut off).unwrap();
        assert_eq!(&out[..off], b"/etcdserverpb.KV/Range");
    }

    #[test]
    fn hpack_decoder_handles_captured_tonic_path_block() {
        // Real bytes captured off the wire from etcd-client 0.14
        // (tonic 0.12 + h2) doing a `client.get("loadtest:none")`:
        //
        //   83                                            :method POST
        //   86                                            :scheme http
        //   41 8a 08 9d 5c 0b 81 70 dc 13 2e bf           :authority "127.0.0.1:2379" (huffman)
        //   04 90 60 a9 24 88 2d 9d cb 65 71 af 9b 8b 1b 47 54 c5
        //                                                 :path "/etcdserverpb.KV/Range" (huffman)
        //   40 82 49 7f 86 4d 83 35 05 b1 1f             literal-incremental
        //                                                 te: trailers (incremental indexing)
        //   5f 8b 1d 75 d0 62 0d 26 3d 4c 4d 65 64 7a 89 49 ea 31
        //                                                 content-type: application/grpc
        //   18 02                                         (extra — unknown follow-on)
        //   e1 12 ec ff                                   user-agent suffix (huffman)
        //
        // The single header we MUST extract correctly is :path —
        // resolve_method() compares the literal bytes.
        let block = [
            0x83, 0x86, 0x41, 0x8a, 0x08, 0x9d, 0x5c, 0x0b, 0x81, 0x70, 0xdc, 0x13, 0x2e, 0xbf,
            0x04, 0x90, 0x60, 0xa9, 0x24, 0x88, 0x2d, 0x9d, 0xcb, 0x65, 0x71, 0xaf, 0x9b, 0x8b,
            0x1b, 0x47, 0x54, 0xc5, 0x40, 0x82, 0x49, 0x7f, 0x86, 0x4d, 0x83, 0x35, 0x05, 0xb1,
            0x1f, 0x5f, 0x8b, 0x1d, 0x75, 0xd0, 0x62, 0x0d, 0x26, 0x3d, 0x4c, 0x4d, 0x65, 0x64,
            0x7a, 0x89, 0x49, 0xea, 0x31, 0x18, 0x02, 0xe1, 0x12, 0xec, 0xff,
        ];
        let mut decoder = HpackDecoder::new();
        let mut scratch = [0u8; 1024];
        let mut path: Option<std::vec::Vec<u8>> = None;
        let mut content_type: Option<std::vec::Vec<u8>> = None;
        decoder
            .decode_block(&block, &mut scratch, |n, v| {
                if n == b":path" && path.is_none() {
                    path = Some(v.to_vec());
                }
                if n == b"content-type" && content_type.is_none() {
                    content_type = Some(v.to_vec());
                }
            })
            .unwrap();
        assert_eq!(
            path.as_deref(),
            Some(&b"/etcdserverpb.KV/Range"[..]),
            "Huffman-decoded :path must match the real tonic-emitted path"
        );
        assert_eq!(
            content_type.as_deref(),
            Some(&b"application/grpc"[..]),
            "Huffman-decoded content-type must match"
        );
    }

    // ── Lease service codec ───────────────────────────────────────

    #[test]
    fn lease_grant_request_decodes_ttl_and_id() {
        // Encode manually: { TTL=30, ID=0x1234 }
        let mut buf = [0u8; 32];
        let mut off = 0;
        write_tag(&mut buf, &mut off, 1, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 30).unwrap();
        write_tag(&mut buf, &mut off, 2, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 0x1234).unwrap();
        let req = LeaseGrantRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.ttl, 30);
        assert_eq!(req.id, 0x1234);
    }

    #[test]
    fn lease_grant_request_defaults_id_when_absent() {
        let mut buf = [0u8; 8];
        let mut off = 0;
        write_tag(&mut buf, &mut off, 1, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 5).unwrap();
        let req = LeaseGrantRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.ttl, 5);
        assert_eq!(req.id, 0);
    }

    #[test]
    fn lease_revoke_request_decodes_id() {
        let mut buf = [0u8; 16];
        let mut off = 0;
        write_tag(&mut buf, &mut off, 1, WIRE_VARINT).unwrap();
        write_varint(&mut buf, &mut off, 0xdead_beef).unwrap();
        let req = LeaseRevokeRequest::decode(&buf[..off]).unwrap();
        assert_eq!(req.id, 0xdead_beef);
    }

    #[test]
    fn lease_grant_response_encodes_header_id_ttl() {
        let mut out = [0u8; 64];
        let mut off = 0;
        build_lease_grant_response(&mut out, &mut off, 7, 0xCAFE_BABE, 30).unwrap();
        // Header tag 1 length-delimited (response_header).
        // Then tag 2 varint (ID), tag 3 varint (TTL).
        // Parse it back as a generic field walk to validate.
        let mut slice = &out[..off];
        let mut saw_header = false;
        let mut saw_id: i64 = -1;
        let mut saw_ttl: i64 = -1;
        while !slice.is_empty() {
            let (field, wire, n) = read_tag(slice).unwrap();
            slice = &slice[n..];
            match (field, wire) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let (h, n) = read_length_delimited(slice).unwrap();
                    saw_header = !h.is_empty();
                    slice = &slice[n..];
                }
                (2, WIRE_VARINT) => {
                    let (v, n) = read_varint(slice).unwrap();
                    saw_id = v as i64;
                    slice = &slice[n..];
                }
                (3, WIRE_VARINT) => {
                    let (v, n) = read_varint(slice).unwrap();
                    saw_ttl = v as i64;
                    slice = &slice[n..];
                }
                _ => {
                    let n = skip_field(slice, wire).unwrap();
                    slice = &slice[n..];
                }
            }
        }
        assert!(saw_header, "response header must be present");
        assert_eq!(saw_id, 0xCAFE_BABE_i64);
        assert_eq!(saw_ttl, 30);
    }

    #[test]
    fn lease_revoke_response_encodes_header_only() {
        let mut out = [0u8; 64];
        let mut off = 0;
        build_lease_revoke_response(&mut out, &mut off, 9).unwrap();
        // Just the response header (tag 1 length-delimited). No tag 2.
        assert!(off > 0);
        let (field, wire, _) = read_tag(&out[..off]).unwrap();
        assert_eq!(field, 1);
        assert_eq!(wire, WIRE_LENGTH_DELIMITED);
    }

    // ── Watch service codec ───────────────────────────────────────

    #[test]
    fn watch_request_decodes_create_with_key_and_revision() {
        // Build: WatchRequest { create_request { key="foo", start_revision=42 } }
        let mut inner = [0u8; 32];
        let mut ioff = 0;
        write_length_delimited(&mut inner, &mut ioff, 1, b"foo").unwrap();
        write_tag(&mut inner, &mut ioff, 3, WIRE_VARINT).unwrap();
        write_varint(&mut inner, &mut ioff, 42).unwrap();

        let mut outer = [0u8; 48];
        let mut ooff = 0;
        write_length_delimited(&mut outer, &mut ooff, 1, &inner[..ioff]).unwrap();

        let req = WatchRequest::decode(&outer[..ooff]).unwrap();
        assert_eq!(req.op, Some(WatchOp::Create));
        assert_eq!(req.key, b"foo");
        assert_eq!(req.start_revision, 42);
    }

    #[test]
    fn watch_request_decodes_cancel_with_id() {
        // WatchRequest { cancel_request { watch_id = 99 } }
        let mut inner = [0u8; 8];
        let mut ioff = 0;
        write_tag(&mut inner, &mut ioff, 1, WIRE_VARINT).unwrap();
        write_varint(&mut inner, &mut ioff, 99).unwrap();
        let mut outer = [0u8; 16];
        let mut ooff = 0;
        write_length_delimited(&mut outer, &mut ooff, 2, &inner[..ioff]).unwrap();
        let req = WatchRequest::decode(&outer[..ooff]).unwrap();
        assert_eq!(req.op, Some(WatchOp::Cancel));
        assert_eq!(req.cancel_watch_id, 99);
    }

    #[test]
    fn watch_response_header_encodes_created() {
        let mut out = [0u8; 64];
        let mut off = 0;
        build_watch_response_header(&mut out, &mut off, 5, 7, true, false).unwrap();
        // Walk fields: header (1, LD), watch_id (2, varint=7), created (3, varint=1).
        let mut slice = &out[..off];
        let mut saw_header = false;
        let mut saw_id: i64 = -1;
        let mut saw_created = false;
        while !slice.is_empty() {
            let (field, wire, n) = read_tag(slice).unwrap();
            slice = &slice[n..];
            match (field, wire) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let (h, n) = read_length_delimited(slice).unwrap();
                    saw_header = !h.is_empty();
                    slice = &slice[n..];
                }
                (2, WIRE_VARINT) => {
                    let (v, n) = read_varint(slice).unwrap();
                    saw_id = v as i64;
                    slice = &slice[n..];
                }
                (3, WIRE_VARINT) => {
                    let (v, n) = read_varint(slice).unwrap();
                    saw_created = v != 0;
                    slice = &slice[n..];
                }
                _ => {
                    let n = skip_field(slice, wire).unwrap();
                    slice = &slice[n..];
                }
            }
        }
        assert!(saw_header);
        assert_eq!(saw_id, 7);
        assert!(saw_created);
    }

    #[test]
    fn watch_event_encodes_put_with_kv() {
        let mut out = [0u8; 64];
        let mut off = 0;
        write_watch_event(&mut out, &mut off, WatchEventKind::Put, b"k", b"v", 3, 1).unwrap();
        // Outer tag is field 11 length-delimited.
        let (field, wire, n) = read_tag(&out[..off]).unwrap();
        assert_eq!(field, 11);
        assert_eq!(wire, WIRE_LENGTH_DELIMITED);
        let (body, _) = read_length_delimited(&out[n..off]).unwrap();
        // Inner: kv (field 2 LD). type=0 is elided for Put.
        let (f, w, _) = read_tag(body).unwrap();
        assert_eq!(f, 2);
        assert_eq!(w, WIRE_LENGTH_DELIMITED);
    }
}
