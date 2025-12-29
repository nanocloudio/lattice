//! Retry deduplication.
//!
//! Per §9.3, adapter retries MUST be idempotent using an internal
//! `IdemKey → {ack_revision, outcome_digest}` table with bounded TTL.
//!
//! Eviction of idempotency records MUST be WAL-backed or the outcome
//! MUST be deterministically recomputable from replayed WAL entries.

use serde::{Deserialize, Serialize};

/// Idempotency record for retry deduplication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    /// The idempotency key provided by the adapter.
    pub key: Vec<u8>,

    /// Revision at which the operation was acknowledged.
    pub ack_revision: u64,

    /// Digest of the operation outcome for verification.
    pub outcome_digest: OutcomeDigest,

    /// Tick at which this record was created.
    pub created_at_tick_ms: u64,

    /// TTL for this record in milliseconds.
    pub ttl_ms: u64,
}

impl IdempotencyRecord {
    /// Create a new idempotency record.
    pub fn new(
        key: Vec<u8>,
        ack_revision: u64,
        outcome_digest: OutcomeDigest,
        created_at_tick_ms: u64,
        ttl_ms: u64,
    ) -> Self {
        Self {
            key,
            ack_revision,
            outcome_digest,
            created_at_tick_ms,
            ttl_ms,
        }
    }

    /// Check if this record has expired at the given tick.
    pub fn is_expired_at(&self, tick_ms: u64) -> bool {
        tick_ms >= self.created_at_tick_ms + self.ttl_ms
    }

    /// Get the expiry tick in milliseconds.
    pub fn expiry_tick_ms(&self) -> u64 {
        self.created_at_tick_ms + self.ttl_ms
    }
}

/// Digest of an operation outcome.
///
/// This is used to verify that a retry produces the same result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeDigest {
    /// Whether the operation succeeded.
    pub success: bool,

    /// Resulting revision (if successful).
    pub revision: Option<u64>,

    /// Hash of the response payload.
    pub payload_hash: u64,
}

impl OutcomeDigest {
    /// Create a successful outcome digest.
    pub fn success(revision: u64, payload_hash: u64) -> Self {
        Self {
            success: true,
            revision: Some(revision),
            payload_hash,
        }
    }

    /// Create a failed outcome digest.
    pub fn failure(payload_hash: u64) -> Self {
        Self {
            success: false,
            revision: None,
            payload_hash,
        }
    }
}

/// Idempotency check result.
#[derive(Debug, Clone)]
pub enum IdempotencyCheck {
    /// No existing record; proceed with the operation.
    NotFound,
    /// Found existing record with matching outcome.
    Match(IdempotencyRecord),
    /// Found existing record with different outcome (conflict).
    Conflict {
        existing: IdempotencyRecord,
        expected: OutcomeDigest,
    },
}

/// Idempotency table for retry deduplication.
///
/// Per §9.3, this table stores outcomes of operations indexed by
/// client-provided idempotency keys. Eviction MUST be WAL-backed
/// to ensure determinism across replicas.
pub struct IdempotencyTable {
    /// Records indexed by idempotency key.
    records: std::collections::HashMap<Vec<u8>, IdempotencyRecord>,

    /// Default TTL for new records in milliseconds.
    default_ttl_ms: u64,
}

impl IdempotencyTable {
    /// Create a new idempotency table with default TTL.
    pub fn new() -> Self {
        Self {
            records: std::collections::HashMap::new(),
            default_ttl_ms: 300_000, // 5 minutes default
        }
    }

    /// Create a new idempotency table with custom TTL.
    pub fn with_ttl(ttl_ms: u64) -> Self {
        Self {
            records: std::collections::HashMap::new(),
            default_ttl_ms: ttl_ms,
        }
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Check for an existing idempotency record.
    pub fn check(&self, key: &[u8], expected_digest: &OutcomeDigest) -> IdempotencyCheck {
        match self.records.get(key) {
            None => IdempotencyCheck::NotFound,
            Some(record) => {
                if record.outcome_digest == *expected_digest {
                    IdempotencyCheck::Match(record.clone())
                } else {
                    IdempotencyCheck::Conflict {
                        existing: record.clone(),
                        expected: expected_digest.clone(),
                    }
                }
            }
        }
    }

    /// Get a record by key.
    pub fn get(&self, key: &[u8]) -> Option<&IdempotencyRecord> {
        self.records.get(key)
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.records.contains_key(key)
    }

    /// Insert a new idempotency record.
    pub fn insert(
        &mut self,
        key: Vec<u8>,
        ack_revision: u64,
        outcome_digest: OutcomeDigest,
        current_tick_ms: u64,
    ) -> Option<IdempotencyRecord> {
        let record = IdempotencyRecord::new(
            key.clone(),
            ack_revision,
            outcome_digest,
            current_tick_ms,
            self.default_ttl_ms,
        );
        self.records.insert(key, record)
    }

    /// Insert a record with custom TTL.
    pub fn insert_with_ttl(
        &mut self,
        key: Vec<u8>,
        ack_revision: u64,
        outcome_digest: OutcomeDigest,
        current_tick_ms: u64,
        ttl_ms: u64,
    ) -> Option<IdempotencyRecord> {
        let record = IdempotencyRecord::new(
            key.clone(),
            ack_revision,
            outcome_digest,
            current_tick_ms,
            ttl_ms,
        );
        self.records.insert(key, record)
    }

    /// Remove a record by key.
    ///
    /// Returns true if the record existed and was removed.
    pub fn remove(&mut self, key: &[u8]) -> bool {
        self.records.remove(key).is_some()
    }

    /// Collect expired keys at the given tick.
    ///
    /// Returns the keys that should be evicted (via WAL entry).
    pub fn collect_expired(&self, current_tick_ms: u64) -> Vec<Vec<u8>> {
        self.records
            .iter()
            .filter(|(_, r)| r.is_expired_at(current_tick_ms))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Evict expired records.
    ///
    /// NOTE: This should only be called when processing an IdemEvict WAL entry
    /// to maintain determinism per §9.3.
    pub fn evict(&mut self, keys: &[Vec<u8>]) -> usize {
        let mut evicted = 0;
        for key in keys {
            if self.records.remove(key).is_some() {
                evicted += 1;
            }
        }
        evicted
    }

    /// Get all records.
    pub fn all_records(&self) -> impl Iterator<Item = &IdempotencyRecord> {
        self.records.values()
    }

    /// Get statistics.
    pub fn stats(&self, current_tick_ms: u64) -> IdempotencyStats {
        let expired_count = self
            .records
            .values()
            .filter(|r| r.is_expired_at(current_tick_ms))
            .count();

        IdempotencyStats {
            total_entries: self.records.len(),
            expired_entries: expired_count,
            default_ttl_ms: self.default_ttl_ms,
        }
    }
}

impl Default for IdempotencyTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Idempotency table statistics.
#[derive(Debug, Clone)]
pub struct IdempotencyStats {
    /// Total number of entries.
    pub total_entries: usize,
    /// Entries that are expired and pending eviction.
    pub expired_entries: usize,
    /// Default TTL for new entries.
    pub default_ttl_ms: u64,
}
