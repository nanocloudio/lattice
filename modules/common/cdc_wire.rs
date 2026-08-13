//! CDC egress wire contracts — the event envelope a
//! `cdc_pump` publishes and the `stream.sink.ordered_ack` port-pair
//! contract a sink provider implements.
//!
//! Dual-target facade: `#[path]`-mounted by the PIC modules
//! (`cdc_pump`, `loopback_sink`, out-of-repo providers) and by the
//! host conformance suite (`tests/harness/tests/contract_cdc_wire.rs`,
//! plus the provider-repo conformance vectors). This file is the ONE
//! place either layout lives; providers that hand-roll offsets have
//! already failed conformance.
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
//! [`SINK_FLAG_BROADCAST`] (the watermark must reach EVERY
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
//! Declared in a provider manifest as
//! `capabilities = ["stream.sink.ordered_ack"]`. Port pair:
//!
//! ```text
//! publish_in (input):  [corr:u64][flags:u8][msg_key_len:u16][msg_key…]
//!                      [payload_len:u16][payload…]
//! ack_out    (output): [corr:u64][status:u8]
//! ```
//!
//! `corr` is never 0 on a publish. Ack statuses: 0 = durably accepted
//! by the backend at the strongest level its configuration offers;
//! 1..=15 typed refusals; and two `corr = 0` LINK-STATE signals that
//! are not replies: [`SINK_STATUS_LINK_DOWN`] (every unacked corr is
//! now unknowable — the pump MUST re-publish all of them after the
//! next LINK_UP) and [`SINK_STATUS_LINK_UP`]. Contract terms a
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
pub const CAP_STREAM_SINK_ORDERED_ACK: &str = "stream.sink.ordered_ack";

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
/// [`SINK_FLAG_BROADCAST`]; the frontier rides as the 8-byte value.
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

// ── stream.sink.ordered_ack port frames ─────────────────────────

/// Publish flags.
pub const SINK_FLAG_BROADCAST: u8 = 0x01;

/// Ack statuses. `0` = durably accepted; `1..=15` typed refusals;
/// `16`/`17` link-state signals carried with `corr = 0`.
pub const SINK_STATUS_OK: u8 = 0;
pub const SINK_REFUSE_OVERSIZE: u8 = 1;
pub const SINK_REFUSE_UNROUTABLE: u8 = 2;
/// Connection lost: every corr issued and not yet acked is now
/// unknowable; the pump MUST re-publish all of them after LINK_UP.
pub const SINK_STATUS_LINK_DOWN: u8 = 16;
/// (Re)connected and writable.
pub const SINK_STATUS_LINK_UP: u8 = 17;

/// A publish frame on `publish_in`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SinkPublish<'a> {
    /// Correlation id; never 0.
    pub corr: u64,
    pub flags: u8,
    pub msg_key: &'a [u8],
    pub payload: &'a [u8],
}

/// Fixed publish-frame overhead: corr(8)+flags(1)+klen(2)+plen(2).
pub const SINK_PUBLISH_OVERHEAD: usize = 8 + 1 + 2 + 2;
/// Worst-case publish frame (message key = table id + max key).
pub const SINK_PUBLISH_MAX: usize = SINK_PUBLISH_OVERHEAD + 4 + CDC_MAX_KEY_LEN + CDC_ENVELOPE_MAX;

impl<'a> SinkPublish<'a> {
    pub fn wire_len(&self) -> usize {
        SINK_PUBLISH_OVERHEAD + self.msg_key.len() + self.payload.len()
    }

    /// Encode. `None` on a zero corr, an over-length field, or a
    /// too-small buffer.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if self.corr == 0
            || self.msg_key.len() > u16::MAX as usize
            || self.payload.len() > u16::MAX as usize
        {
            return None;
        }
        let need = self.wire_len();
        if out.len() < need {
            return None;
        }
        let mut p = 0usize;
        out[p..p + 8].copy_from_slice(&self.corr.to_le_bytes());
        p += 8;
        out[p] = self.flags;
        p += 1;
        out[p..p + 2].copy_from_slice(&(self.msg_key.len() as u16).to_le_bytes());
        p += 2;
        out[p..p + 2].copy_from_slice(&(self.payload.len() as u16).to_le_bytes());
        p += 2;
        out[p..p + self.msg_key.len()].copy_from_slice(self.msg_key);
        p += self.msg_key.len();
        out[p..p + self.payload.len()].copy_from_slice(self.payload);
        p += self.payload.len();
        Some(p)
    }

    pub fn decode(src: &'a [u8]) -> Option<Self> {
        if src.len() < SINK_PUBLISH_OVERHEAD {
            return None;
        }
        let corr = u64::from_le_bytes(src[0..8].try_into().ok()?);
        if corr == 0 {
            return None;
        }
        let flags = src[8];
        let klen = u16::from_le_bytes([src[9], src[10]]) as usize;
        let plen = u16::from_le_bytes([src[11], src[12]]) as usize;
        let p = SINK_PUBLISH_OVERHEAD;
        let msg_key = src.get(p..p + klen)?;
        let payload = src.get(p + klen..p + klen + plen)?;
        if p + klen + plen != src.len() {
            return None;
        }
        Some(Self {
            corr,
            flags,
            msg_key,
            payload,
        })
    }
}

/// An ack (or link-state) frame on `ack_out`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SinkAck {
    /// `0` = link-state signal, otherwise the answered publish.
    pub corr: u64,
    pub status: u8,
}

pub const SINK_ACK_WIRE_LEN: usize = 9;

impl SinkAck {
    /// A per-publish reply. Refuses the reserved link-state statuses —
    /// those never answer a corr.
    pub fn reply(corr: u64, status: u8) -> Option<Self> {
        if corr == 0 || status >= SINK_STATUS_LINK_DOWN {
            return None;
        }
        Some(Self { corr, status })
    }

    /// A link-state signal (corr 0).
    pub fn link(status: u8) -> Option<Self> {
        if status != SINK_STATUS_LINK_DOWN && status != SINK_STATUS_LINK_UP {
            return None;
        }
        Some(Self { corr: 0, status })
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < SINK_ACK_WIRE_LEN {
            return None;
        }
        out[0..8].copy_from_slice(&self.corr.to_le_bytes());
        out[8] = self.status;
        Some(SINK_ACK_WIRE_LEN)
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() != SINK_ACK_WIRE_LEN {
            return None;
        }
        Some(Self {
            corr: u64::from_le_bytes(src[0..8].try_into().ok()?),
            status: src[8],
        })
    }

    /// Is this a link-state signal rather than a reply?
    pub fn is_link_state(&self) -> bool {
        self.corr == 0
    }
}

// ── Channel message types ─────────────────────────────────────────────
//
// The pump↔sink pair rides the standard 3-byte envelope
// (`[msg_type:u8][len:u16 LE]`). These live in lattice's 0xC0..0xEF
// band; providers in other repos inline the two byte values (the
// pattern the bridge uses for mvcc constants).

/// A `SinkPublish` frame, pump → sink.
pub const MSG_CDC_PUBLISH: u8 = 0xED;
/// A `SinkAck` frame, sink → pump.
pub const MSG_CDC_ACK: u8 = 0xEE;
