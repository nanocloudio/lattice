//! The CDC event envelope a `cdc_pump` publishes.
//!
//! This file owns the ENVELOPE and nothing else. The transport it rides —
//! the `stream.ordered_ack` port pair — is a fluxor SDK contract
//! (`modules/sdk/contracts/exchange.rs`), mounted and re-exported below so
//! existing `cdc_wire::Publish` call sites keep resolving.
//!
//! The surface moved because it was never a CDC concept: quantum implements
//! it for MQTT, Kafka and AMQP, wave for HTTP, and the CDC RFC forbids
//! lattice depending on quantum — so a contract owned here was one those
//! providers could not name. CDC is ONE producer of the surface, and the
//! envelope below is one possible `payload`.
//!
//! Dual-target facade: `#[path]`-mounted by the PIC modules (`cdc_pump`,
//! `loopback_sink`) and by the host conformance suite
//! (`tests/harness/tests/contract_cdc_wire.rs`). This file is the ONE place
//! the ENVELOPE layout lives; the frame layout lives in the SDK contract, and
//! a provider that hand-rolls either has already failed conformance.
//!
//! # The envelope
//!
//! One envelope per event, little-endian, identity fields first and
//! fixed so a consumer's staleness check needs no full decode:
//!
//! ```text
//! [format:u16 = CDC_ENVELOPE_FORMAT]
//! [kind:u8]                 1=put 2=delete 3=resolved
//!                           4=backfill_complete 5=topology
//! [flags:u8]                bit0 = backfill phase; bit1 = before-image
//!                           present (reserved, never set in v1)
//! [feed_id:u64]             ┐ identity
//! [table_id:u32]            │   0 for raw-KV feeds
//! [key_len:u16][key…]       │
//! [commit_ts:u64]           ┘ MVCC commit timestamp
//! [range_id:u64]            ┐ diagnostics — explicitly NON-identity;
//! [range_generation:u32]    │ change across topology transitions
//! [revision:u64]            ┘ per-partition apply revision
//! [value_len:u32][value…]   row image / raw value; empty for delete
//!                           and control kinds
//! ```
//!
//! A `resolved` event carries `[frontier_ts:u64 LE]` as its 8-byte
//! value with an empty key, and is published with
//! [`FLAG_BROADCAST`] (the watermark must reach EVERY
//! ordering unit of the destination). `backfill_complete` and
//! `topology` carry an empty key; `topology`'s value is
//! provider-opaque notice bytes.
//!
//! The event identity is `(feed_id, table_id, key, commit_ts)` —
//! deliberately NOT range/revision, which change across split/merge
//! exactly where re-delivery is permitted. The normative
//! consumer rule: an event whose `commit_ts` is ≤ the highest applied
//! for the same `(table_id, key)` is STALE — discard it, don't merely
//! dedupe — because per-key backend order can be violated across
//! failover/replay boundaries.
//!
//! # The sink contract
//!
//! Not defined here. See `modules/sdk/contracts/exchange.rs` for the
//! authoritative port pair, the optional `reply_out` that makes the surface
//! an exchange, the status vocabulary, and the size envelope
//! (`PAYLOAD_MAX` and friends). A provider declares
//! `capabilities = ["stream.ordered_ack"]` and the terms it offers as
//! capability facts.
//!
//! What follows is the CDC-specific reading of that surface — what a pump
//! puts in a publish and what it does with an ack.
//!
//! `corr` is never 0 on a publish. Ack statuses: 0 = durably accepted
//! by the backend at the strongest level its configuration offers;
//! 1..=15 typed refusals; and two `corr = 0` LINK-STATE signals that
//! are not replies: [`STATUS_LINK_DOWN`] (every unacked corr is
//! now unknowable — the pump MUST re-publish all of them after the
//! next LINK_UP) and [`STATUS_LINK_UP`]. Contract terms a
//! conforming provider must satisfy (the conformance vectors assert
//! them): ack = durable acceptance; per-key order within a connection;
//! no silent drops (every non-zero corr answered or invalidated by
//! LINK_DOWN); backpressure by channel, never by dropping; broadcast
//! delivered to every ordering unit and acked once after the slowest.
//!
//! The sink message key is `[table_id:u32 BE][key…]` so per-key
//! ordering maps onto partitioned backends without the provider
//! understanding lattice keys.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

/// Envelope layout version. Bumped on ANY layout change; consumers
/// refuse unknown versions rather than guessing offsets.
pub const CDC_ENVELOPE_FORMAT: u16 = 1;

/// The capability string a conforming sink provider declares.

// ── Event kinds ───────────────────────────────────────────────────────

pub const CDC_KIND_PUT: u8 = 1;
pub const CDC_KIND_DELETE: u8 = 2;
pub const CDC_KIND_RESOLVED: u8 = 3;
pub const CDC_KIND_BACKFILL_COMPLETE: u8 = 4;
pub const CDC_KIND_TOPOLOGY: u8 = 5;

// ── Event flags ───────────────────────────────────────────────────────

/// The event was produced by a backfill scan, not the stream.
pub const CDC_FLAG_BACKFILL: u8 = 0x01;
/// Before-image present. Reserved: never set in v1.
pub const CDC_FLAG_BEFORE_IMAGE: u8 = 0x02;

/// Bounds. Key mirrors the store's user-key bound; value mirrors
/// `kv_store::MAX_VALUE_LEN`.
pub const CDC_MAX_KEY_LEN: usize = 256;
pub const CDC_MAX_VALUE_LEN: usize = 4096;

/// Fixed head bytes before the key: format(2)+kind(1)+flags(1)+
/// feed_id(8)+table_id(4)+key_len(2).
const HEAD_TO_KEY: usize = 2 + 1 + 1 + 8 + 4 + 2;
/// Fixed bytes between key and value: commit_ts(8)+range_id(8)+
/// range_generation(4)+revision(8)+value_len(4).
const MID_LEN: usize = 8 + 8 + 4 + 8 + 4;

/// Worst-case encoded envelope.
pub const CDC_ENVELOPE_MAX: usize = HEAD_TO_KEY + CDC_MAX_KEY_LEN + MID_LEN + CDC_MAX_VALUE_LEN;

/// One CDC event, borrowed from its transport buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CdcEvent<'a> {
    pub kind: u8,
    pub flags: u8,
    pub feed_id: u64,
    pub table_id: u32,
    pub key: &'a [u8],
    pub commit_ts: u64,
    pub range_id: u64,
    pub range_generation: u32,
    pub revision: u64,
    pub value: &'a [u8],
}

impl<'a> CdcEvent<'a> {
    /// Encoded length of this event.
    pub fn wire_len(&self) -> usize {
        HEAD_TO_KEY + self.key.len() + MID_LEN + self.value.len()
    }

    /// Encode into `out`. Returns the byte count, or `None` when the
    /// buffer is too small or a bound is exceeded — fail closed, never
    /// truncate (an oversize event is refused, not cut).
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if self.key.len() > CDC_MAX_KEY_LEN || self.value.len() > CDC_MAX_VALUE_LEN {
            return None;
        }
        let need = self.wire_len();
        if out.len() < need {
            return None;
        }
        let mut p = 0usize;
        out[p..p + 2].copy_from_slice(&CDC_ENVELOPE_FORMAT.to_le_bytes());
        p += 2;
        out[p] = self.kind;
        p += 1;
        out[p] = self.flags;
        p += 1;
        out[p..p + 8].copy_from_slice(&self.feed_id.to_le_bytes());
        p += 8;
        out[p..p + 4].copy_from_slice(&self.table_id.to_le_bytes());
        p += 4;
        out[p..p + 2].copy_from_slice(&(self.key.len() as u16).to_le_bytes());
        p += 2;
        out[p..p + self.key.len()].copy_from_slice(self.key);
        p += self.key.len();
        out[p..p + 8].copy_from_slice(&self.commit_ts.to_le_bytes());
        p += 8;
        out[p..p + 8].copy_from_slice(&self.range_id.to_le_bytes());
        p += 8;
        out[p..p + 4].copy_from_slice(&self.range_generation.to_le_bytes());
        p += 4;
        out[p..p + 8].copy_from_slice(&self.revision.to_le_bytes());
        p += 8;
        out[p..p + 4].copy_from_slice(&(self.value.len() as u32).to_le_bytes());
        p += 4;
        out[p..p + self.value.len()].copy_from_slice(self.value);
        p += self.value.len();
        Some(p)
    }

    /// Decode from `src`. `None` on an unknown format version, any
    /// truncation, or trailing garbage — a partially-decoded event is
    /// a different event.
    pub fn decode(src: &'a [u8]) -> Option<Self> {
        if src.len() < HEAD_TO_KEY + MID_LEN {
            return None;
        }
        let format = u16::from_le_bytes([src[0], src[1]]);
        if format != CDC_ENVELOPE_FORMAT {
            return None;
        }
        let kind = src[2];
        let flags = src[3];
        let feed_id = u64::from_le_bytes(src[4..12].try_into().ok()?);
        let table_id = u32::from_le_bytes(src[12..16].try_into().ok()?);
        let key_len = u16::from_le_bytes([src[16], src[17]]) as usize;
        if key_len > CDC_MAX_KEY_LEN {
            return None;
        }
        let mut p = HEAD_TO_KEY;
        let key = src.get(p..p + key_len)?;
        p += key_len;
        let commit_ts = u64::from_le_bytes(src.get(p..p + 8)?.try_into().ok()?);
        p += 8;
        let range_id = u64::from_le_bytes(src.get(p..p + 8)?.try_into().ok()?);
        p += 8;
        let range_generation = u32::from_le_bytes(src.get(p..p + 4)?.try_into().ok()?);
        p += 4;
        let revision = u64::from_le_bytes(src.get(p..p + 8)?.try_into().ok()?);
        p += 8;
        let value_len = u32::from_le_bytes(src.get(p..p + 4)?.try_into().ok()?) as usize;
        if value_len > CDC_MAX_VALUE_LEN {
            return None;
        }
        p += 4;
        let value = src.get(p..p + value_len)?;
        p += value_len;
        if p != src.len() {
            return None; // trailing garbage
        }
        Some(Self {
            kind,
            flags,
            feed_id,
            table_id,
            key,
            commit_ts,
            range_id,
            range_generation,
            revision,
            value,
        })
    }
}

/// Build a `resolved` watermark event. Published with
/// [`FLAG_BROADCAST`]; the frontier rides as the 8-byte value.
pub fn resolved_event(feed_id: u64, frontier_ts: u64, scratch: &mut [u8; 8]) -> CdcEvent<'_> {
    *scratch = frontier_ts.to_le_bytes();
    CdcEvent {
        kind: CDC_KIND_RESOLVED,
        flags: 0,
        feed_id,
        table_id: 0,
        key: &[],
        commit_ts: frontier_ts,
        range_id: 0,
        range_generation: 0,
        revision: 0,
        value: scratch,
    }
}

/// The frontier a `resolved` event carries, if it is one.
pub fn resolved_frontier(ev: &CdcEvent<'_>) -> Option<u64> {
    if ev.kind != CDC_KIND_RESOLVED || ev.value.len() != 8 {
        return None;
    }
    Some(u64::from_le_bytes(ev.value.try_into().ok()?))
}

// ── The sink message key ──────────────────────────────────────────────

/// Sink message key: `[table_id:u32 BE][key…]`. Per-key ordering maps
/// onto partitioned backends (partition by message key) without the
/// provider understanding lattice keys. Returns the length.
pub fn sink_msg_key(table_id: u32, key: &[u8], out: &mut [u8]) -> Option<usize> {
    let need = 4 + key.len();
    if out.len() < need {
        return None;
    }
    out[0..4].copy_from_slice(&table_id.to_be_bytes());
    out[4..need].copy_from_slice(key);
    Some(need)
}

// ── The surface this envelope rides ───────────────────────────────────
//
// NOT DEFINED HERE, and deliberately not re-exported either. The
// `stream.ordered_ack` frames live in the fluxor SDK
// (`modules/sdk/contracts/exchange.rs`) and every consumer mounts them
// DIRECTLY:
//
//   #[path = "…/sdk/contracts/exchange.rs"]
//   mod exchange;
//
// An earlier version re-exported them from here so call sites would not have
// to change when the contract moved. That worked, but it left two paths to
// one set of types — `cdc_wire::Publish` and `exchange::Publish` — and a
// lattice-flavoured alias (`MSG_CDC_ACK` for `MSG_ACK`) that made a portable
// contract look like a CDC one. Both are gone: this file owns the ENVELOPE,
// the SDK owns the FRAME, and nothing owns both.
//
// `PUBLISH_FRAME_MAX` from the contract is the worst-case frame; there is no
// CDC-specific ceiling to state, because the envelope is bounded by the
// payload the contract already sizes.
