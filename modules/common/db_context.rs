//! Native database request context, consistency/durability classes, and
//! retry classification — RFC database foundation §8.
//!
//! Protocol adapters translate into this native contract; database
//! features are never designed as special cases of RESP/etcd/Memcached.
//! The context is a bounded, versioned, fixed-layout struct with a TLV
//! wire form so unknown OPTIONAL fields skip cleanly and unknown
//! REQUIRED features fail closed.
//!
//! Golden vectors: `tests/contract_db_context.rs`.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules and host tests; each consumer uses a subset of the surface"
)]

/// Version of the request-context wire encoding.
pub const DB_CONTEXT_VERSION: u16 = 1;

// ── Consistency / durability classes ──────────────────────────────────

/// Read consistency policy (§20). Explicit — a request that does not
/// meet its policy's fence conditions fails closed rather than being
/// silently served weaker.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Consistency {
    /// Owning group's ReadIndex + applied-index fence.
    Linearizable = 0x01,
    /// Serve only if timestamp and observed-revision bounds are met.
    BoundedStale = 0x02,
    /// Serve at the supplied MVCC timestamp; protects required history.
    Snapshot = 0x03,
    /// Follower/observer read; reports its resolved timestamp.
    Eventual = 0x04,
}

/// Acknowledgement durability class (§6). A signal named or consumed as
/// a quorum durability proof is valid only for `ReplicatedDurable`.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Durability {
    /// Volatile single-node acknowledgement.
    Volatile = 0x01,
    /// Replicated to a quorum's memory, not fsynced.
    ReplicatedVolatile = 0x02,
    /// Quorum durable proof observed (the only class that may be
    /// described as "durable" to a client).
    ReplicatedDurable = 0x03,
}

impl Consistency {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Linearizable),
            0x02 => Some(Self::BoundedStale),
            0x03 => Some(Self::Snapshot),
            0x04 => Some(Self::Eventual),
            _ => None,
        }
    }
}

impl Durability {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::Volatile),
            0x02 => Some(Self::ReplicatedVolatile),
            0x03 => Some(Self::ReplicatedDurable),
            _ => None,
        }
    }
}

// ── Retry classification ──────────────────────────────────────────────

/// Typed outcome classification (§8). Every failure a client can see
/// maps to exactly one class; ambiguity ("indeterminate") is a first-
/// class result, never collapsed into success or failure.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RetryClass {
    /// Route metadata was stale; refresh descriptors and retry.
    StaleRoute = 0x01,
    /// Retryable contention (conflict, push, lock timeout).
    RetryableContention = 0x02,
    /// Owning quorum unavailable.
    UnavailableQuorum = 0x03,
    /// Deadline expired before a decisive outcome.
    DeadlineExpired = 0x04,
    /// Malformed input; retrying identically cannot succeed.
    Malformed = 0x05,
    /// Required capability not supported by this composition.
    UnsupportedCapability = 0x06,
    /// The operation may have committed but the response was lost.
    /// Outcome is discoverable via the idempotency identity.
    IndeterminateDelivery = 0x07,
}

impl RetryClass {
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(Self::StaleRoute),
            0x02 => Some(Self::RetryableContention),
            0x03 => Some(Self::UnavailableQuorum),
            0x04 => Some(Self::DeadlineExpired),
            0x05 => Some(Self::Malformed),
            0x06 => Some(Self::UnsupportedCapability),
            0x07 => Some(Self::IndeterminateDelivery),
            _ => None,
        }
    }
}

// ── Request context ───────────────────────────────────────────────────

/// Bounded native request context (§8). Fixed layout; every field is a
/// catalog-resolved bounded identifier or an explicit limit. Adapters
/// fill defaults for protocols that cannot express a field.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestContext {
    pub tenant_id: u32,
    pub database_id: u32,
    pub keyspace_id: u32,
    /// Correlation identity carried through adapter → router → proposal
    /// → apply → response (§24).
    pub request_id: u64,
    /// Absolute deadline, unix ms. 0 = no deadline.
    pub deadline_unix_ms: u64,
    pub consistency: Consistency,
    pub durability: Durability,
    /// Routing epoch + range generation fences (§11.3).
    pub routing_epoch: u32,
    pub range_generation: u32,
    /// 0 = not transactional.
    pub transaction_id: u64,
    /// MVCC read timestamp for Snapshot reads; 0 = latest.
    pub read_timestamp: u64,
    /// Idempotency identity for retryable mutations; 0 = none.
    pub idempotency_key: u64,
    /// Response size bound, bytes.
    pub max_response_bytes: u32,
    /// Result cardinality bound.
    pub max_keys: u32,
}

impl RequestContext {
    /// Encoded wire size: `[version:2][len:2]` header + fixed payload.
    pub const WIRE_LEN: usize = 4 + 70;

    /// Serialize to the versioned wire form.
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < Self::WIRE_LEN {
            return None;
        }
        out[0..2].copy_from_slice(&DB_CONTEXT_VERSION.to_le_bytes());
        out[2..4].copy_from_slice(&((Self::WIRE_LEN - 4) as u16).to_le_bytes());
        let p = &mut out[4..];
        p[0..4].copy_from_slice(&self.tenant_id.to_le_bytes());
        p[4..8].copy_from_slice(&self.database_id.to_le_bytes());
        p[8..12].copy_from_slice(&self.keyspace_id.to_le_bytes());
        p[12..20].copy_from_slice(&self.request_id.to_le_bytes());
        p[20..28].copy_from_slice(&self.deadline_unix_ms.to_le_bytes());
        p[28] = self.consistency as u8;
        p[29] = self.durability as u8;
        p[30..34].copy_from_slice(&self.routing_epoch.to_le_bytes());
        p[34..38].copy_from_slice(&self.range_generation.to_le_bytes());
        p[38..46].copy_from_slice(&self.transaction_id.to_le_bytes());
        p[46..54].copy_from_slice(&self.read_timestamp.to_le_bytes());
        p[54..62].copy_from_slice(&self.idempotency_key.to_le_bytes());
        p[62..66].copy_from_slice(&self.max_response_bytes.to_le_bytes());
        p[66..70].copy_from_slice(&self.max_keys.to_le_bytes());
        Some(Self::WIRE_LEN)
    }

    /// Parse the versioned wire form. Fails closed on unknown version,
    /// short payload, or invalid enum bytes.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < Self::WIRE_LEN {
            return None;
        }
        let version = u16::from_le_bytes(src[0..2].try_into().ok()?);
        if version != DB_CONTEXT_VERSION {
            return None;
        }
        let len = u16::from_le_bytes(src[2..4].try_into().ok()?) as usize;
        if len < Self::WIRE_LEN - 4 {
            return None;
        }
        let p = &src[4..];
        Some(Self {
            tenant_id: u32::from_le_bytes(p[0..4].try_into().ok()?),
            database_id: u32::from_le_bytes(p[4..8].try_into().ok()?),
            keyspace_id: u32::from_le_bytes(p[8..12].try_into().ok()?),
            request_id: u64::from_le_bytes(p[12..20].try_into().ok()?),
            deadline_unix_ms: u64::from_le_bytes(p[20..28].try_into().ok()?),
            consistency: Consistency::from_u8(p[28])?,
            durability: Durability::from_u8(p[29])?,
            routing_epoch: u32::from_le_bytes(p[30..34].try_into().ok()?),
            range_generation: u32::from_le_bytes(p[34..38].try_into().ok()?),
            transaction_id: u64::from_le_bytes(p[38..46].try_into().ok()?),
            read_timestamp: u64::from_le_bytes(p[46..54].try_into().ok()?),
            idempotency_key: u64::from_le_bytes(p[54..62].try_into().ok()?),
            max_response_bytes: u32::from_le_bytes(p[62..66].try_into().ok()?),
            max_keys: u32::from_le_bytes(p[66..70].try_into().ok()?),
        })
    }
}
