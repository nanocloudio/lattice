//! KV state machine core.
//!
//! Each KPG hosts the same state machine per §7.1:
//! - KV records with MVCC versioning
//! - Lease records for TTL management
//! - Watch state for event delivery

use crate::core::time::Tick;
use serde::{Deserialize, Serialize};

/// A key-value record in the state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvRecord {
    /// The key (byte string).
    pub key: Vec<u8>,

    /// The value (byte string).
    pub value: Vec<u8>,

    /// Revision when this key was created.
    pub create_revision: u64,

    /// Revision of the last modification (Raft log index).
    pub mod_revision: u64,

    /// Version counter, increments on each mutation.
    pub version: u64,

    /// Associated lease ID (if any).
    pub lease_id: Option<i64>,

    /// Expiry time as a deterministic tick (if TTL set).
    pub expiry_at: Option<Tick>,

    /// Metadata flags for the key.
    pub flags: KvFlags,
}

bitflags::bitflags! {
    /// Metadata flags for a key-value record.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct KvFlags: u32 {
        /// Key has been deleted (tombstone).
        const DELETED = 0b0000_0001;
        /// Key was created in current transaction.
        const CREATED_IN_TXN = 0b0000_0010;
    }
}

impl Default for KvFlags {
    fn default() -> Self {
        Self::empty()
    }
}

impl KvRecord {
    /// Create a new KV record.
    pub fn new(key: Vec<u8>, value: Vec<u8>, create_revision: u64, mod_revision: u64) -> Self {
        Self {
            key,
            value,
            create_revision,
            mod_revision,
            version: 1,
            lease_id: None,
            expiry_at: None,
            flags: KvFlags::empty(),
        }
    }

    /// Check if this record is a tombstone (deleted).
    pub fn is_deleted(&self) -> bool {
        self.flags.contains(KvFlags::DELETED)
    }

    /// Check if this record has expired at the given tick.
    pub fn is_expired_at(&self, tick: Tick) -> bool {
        self.expiry_at
            .is_some_and(|expiry| tick.is_at_or_after(expiry))
    }

    /// Check if this record has an associated lease.
    pub fn has_lease(&self) -> bool {
        self.lease_id.is_some()
    }
}

/// Event type for watch notifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// Key was created or updated.
    Put,
    /// Key was deleted.
    Delete,
}

/// A key-value event for watch delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvEvent {
    /// Type of event.
    pub event_type: EventType,

    /// The key-value record (current state for Put, previous state for Delete).
    pub kv: KvRecord,

    /// Previous key-value record (for updates).
    pub prev_kv: Option<KvRecord>,
}

impl KvEvent {
    /// Create a Put event.
    pub fn put(kv: KvRecord, prev_kv: Option<KvRecord>) -> Self {
        Self {
            event_type: EventType::Put,
            kv,
            prev_kv,
        }
    }

    /// Create a Delete event.
    pub fn delete(prev_kv: KvRecord) -> Self {
        Self {
            event_type: EventType::Delete,
            kv: prev_kv.clone(),
            prev_kv: Some(prev_kv),
        }
    }
}

/// KV state machine core.
///
/// Maintains the in-memory KV index with MVCC versioning per §7.1.
/// All state changes MUST derive from WAL entries per KVSOURCE invariant.
pub struct KvStateMachine {
    /// Current revision (commit index from Raft).
    current_revision: u64,

    /// In-memory index: key → current KvRecord.
    index: std::collections::BTreeMap<Vec<u8>, KvRecord>,

    /// MVCC history: revision → list of (key, record) pairs.
    /// Used for historical reads and watch replay.
    history: std::collections::BTreeMap<u64, Vec<(Vec<u8>, KvRecord)>>,

    /// Compaction floor revision (history below this is discarded).
    compaction_floor: u64,

    /// Pending events for watch delivery.
    pending_events: Vec<KvEvent>,

    /// Current deterministic tick for TTL evaluation.
    current_tick: Tick,

    /// Total key count (excluding tombstones).
    live_key_count: usize,
}

impl KvStateMachine {
    /// Create a new KV state machine.
    pub fn new() -> Self {
        Self {
            current_revision: 0,
            index: std::collections::BTreeMap::new(),
            history: std::collections::BTreeMap::new(),
            compaction_floor: 0,
            pending_events: Vec::new(),
            current_tick: Tick::zero(),
            live_key_count: 0,
        }
    }

    /// Get the current revision.
    pub fn current_revision(&self) -> u64 {
        self.current_revision
    }

    /// Get the compaction floor.
    pub fn compaction_floor(&self) -> u64 {
        self.compaction_floor
    }

    /// Get the current tick.
    pub fn current_tick(&self) -> Tick {
        self.current_tick
    }

    /// Advance the current tick (called during tick WAL entry processing).
    pub fn advance_tick(&mut self, tick: Tick) {
        self.current_tick = tick;
    }

    /// Get a key's current value.
    pub fn get(&self, key: &[u8]) -> Option<&KvRecord> {
        self.index.get(key).filter(|r| !r.is_deleted())
    }

    /// Get a key's value at a specific revision.
    ///
    /// Returns `Err(true)` if the revision is compacted.
    /// Returns `Err(false)` if the key doesn't exist at that revision.
    pub fn get_at_revision(&self, key: &[u8], revision: u64) -> Result<Option<&KvRecord>, bool> {
        if revision < self.compaction_floor {
            return Err(true); // Compacted
        }

        // If revision >= current_revision, use current state
        if revision >= self.current_revision {
            return Ok(self.get(key));
        }

        // Walk history backwards from revision
        for (rev, entries) in self.history.range(..=revision).rev() {
            for (k, record) in entries {
                if k == key {
                    if record.is_deleted() {
                        return Ok(None);
                    }
                    return Ok(Some(record));
                }
            }
            // Stop if we've gone past when the key was last modified
            if *rev < self.compaction_floor {
                break;
            }
        }

        // Check current state if key existed before requested revision
        if let Some(record) = self.index.get(key) {
            if record.create_revision <= revision && !record.is_deleted() {
                return Ok(Some(record));
            }
        }

        Ok(None)
    }

    /// Range query over keys.
    pub fn range(&self, start: &[u8], end: Option<&[u8]>) -> Vec<&KvRecord> {
        let mut result = Vec::new();

        let range_iter: Box<dyn Iterator<Item = (&Vec<u8>, &KvRecord)>> = match end {
            Some(end_key) => Box::new(self.index.range(start.to_vec()..end_key.to_vec())),
            None => Box::new(self.index.range(start.to_vec()..)),
        };

        for (_, record) in range_iter {
            if !record.is_deleted() {
                result.push(record);
            }
        }

        result
    }

    /// Range query with limit.
    pub fn range_limit(&self, start: &[u8], end: Option<&[u8]>, limit: usize) -> Vec<&KvRecord> {
        let mut result = Vec::new();

        let range_iter: Box<dyn Iterator<Item = (&Vec<u8>, &KvRecord)>> = match end {
            Some(end_key) => Box::new(self.index.range(start.to_vec()..end_key.to_vec())),
            None => Box::new(self.index.range(start.to_vec()..)),
        };

        for (_, record) in range_iter {
            if !record.is_deleted() {
                result.push(record);
                if result.len() >= limit {
                    break;
                }
            }
        }

        result
    }

    /// Count keys in a range.
    pub fn count_range(&self, start: &[u8], end: Option<&[u8]>) -> usize {
        let range_iter: Box<dyn Iterator<Item = (&Vec<u8>, &KvRecord)>> = match end {
            Some(end_key) => Box::new(self.index.range(start.to_vec()..end_key.to_vec())),
            None => Box::new(self.index.range(start.to_vec()..)),
        };

        range_iter.filter(|(_, r)| !r.is_deleted()).count()
    }

    /// Put a key-value pair.
    ///
    /// Returns the previous value if it existed.
    pub fn put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        revision: u64,
        lease_id: Option<i64>,
    ) -> Option<KvRecord> {
        self.current_revision = revision;

        let prev = self.index.get(&key).cloned();
        let is_update = prev.as_ref().is_some_and(|p| !p.is_deleted());

        let (create_revision, version) = if is_update {
            let p = prev.as_ref().unwrap();
            (p.create_revision, p.version + 1)
        } else {
            (revision, 1)
        };

        let mut record = KvRecord::new(key.clone(), value, create_revision, revision);
        record.version = version;
        record.lease_id = lease_id;

        // Track live key count
        if !is_update {
            self.live_key_count += 1;
        }

        // Generate event
        let event = KvEvent::put(record.clone(), prev.clone());
        self.pending_events.push(event);

        // Store in history for MVCC
        self.history
            .entry(revision)
            .or_default()
            .push((key.clone(), record.clone()));

        self.index.insert(key, record);

        prev.filter(|p| !p.is_deleted())
    }

    /// Delete a key.
    ///
    /// Returns the deleted record if it existed.
    pub fn delete(&mut self, key: &[u8], revision: u64) -> Option<KvRecord> {
        self.current_revision = revision;

        let prev = self.index.get(key).cloned();

        if let Some(ref p) = prev {
            if !p.is_deleted() {
                // Create tombstone
                let mut tombstone = p.clone();
                tombstone.mod_revision = revision;
                tombstone.flags.insert(KvFlags::DELETED);

                // Generate event
                let event = KvEvent::delete(p.clone());
                self.pending_events.push(event);

                // Store in history
                self.history
                    .entry(revision)
                    .or_default()
                    .push((key.to_vec(), tombstone.clone()));

                self.index.insert(key.to_vec(), tombstone);
                self.live_key_count = self.live_key_count.saturating_sub(1);

                return Some(p.clone());
            }
        }

        None
    }

    /// Delete a range of keys.
    ///
    /// Returns the deleted records.
    pub fn delete_range(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        revision: u64,
    ) -> Vec<KvRecord> {
        // Collect keys to delete first to avoid borrow issues
        let keys_to_delete: Vec<Vec<u8>> = {
            let range_iter: Box<dyn Iterator<Item = (&Vec<u8>, &KvRecord)>> = match end {
                Some(end_key) => Box::new(self.index.range(start.to_vec()..end_key.to_vec())),
                None => Box::new(self.index.range(start.to_vec()..)),
            };

            range_iter
                .filter(|(_, r)| !r.is_deleted())
                .map(|(k, _)| k.clone())
                .collect()
        };

        let mut deleted = Vec::new();
        for key in keys_to_delete {
            if let Some(record) = self.delete(&key, revision) {
                deleted.push(record);
            }
        }

        deleted
    }

    /// Take pending events for watch delivery.
    pub fn take_events(&mut self) -> Vec<KvEvent> {
        std::mem::take(&mut self.pending_events)
    }

    /// Peek at pending events without consuming them.
    pub fn peek_events(&self) -> &[KvEvent] {
        &self.pending_events
    }

    /// Compact history up to the given revision.
    ///
    /// Returns the number of entries removed.
    pub fn compact(&mut self, revision: u64) -> usize {
        if revision <= self.compaction_floor {
            return 0;
        }

        let mut removed = 0;
        let keys_to_remove: Vec<u64> = self.history.range(..revision).map(|(k, _)| *k).collect();

        for key in keys_to_remove {
            if let Some(entries) = self.history.remove(&key) {
                removed += entries.len();
            }
        }

        self.compaction_floor = revision;
        removed
    }

    /// Get statistics about the state machine.
    pub fn stats(&self) -> KvStateMachineStats {
        KvStateMachineStats {
            current_revision: self.current_revision,
            compaction_floor: self.compaction_floor,
            live_key_count: self.live_key_count,
            index_size: self.index.len(),
            history_revisions: self.history.len(),
            pending_events: self.pending_events.len(),
        }
    }

    /// Check if a key exists (not deleted).
    pub fn contains(&self, key: &[u8]) -> bool {
        self.index.get(key).is_some_and(|r| !r.is_deleted())
    }

    /// Get the create revision for a key.
    pub fn create_revision(&self, key: &[u8]) -> Option<u64> {
        self.get(key).map(|r| r.create_revision)
    }

    /// Get the mod revision for a key.
    pub fn mod_revision(&self, key: &[u8]) -> Option<u64> {
        self.get(key).map(|r| r.mod_revision)
    }

    /// Get the version for a key.
    pub fn version(&self, key: &[u8]) -> Option<u64> {
        self.get(key).map(|r| r.version)
    }
}

impl Default for KvStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for a KV state machine.
#[derive(Debug, Clone)]
pub struct KvStateMachineStats {
    /// Current revision.
    pub current_revision: u64,
    /// Compaction floor.
    pub compaction_floor: u64,
    /// Number of live keys (not deleted).
    pub live_key_count: usize,
    /// Total index size (including tombstones).
    pub index_size: usize,
    /// Number of revisions in history.
    pub history_revisions: usize,
    /// Number of pending events.
    pub pending_events: usize,
}

/// Snapshot of a KV state machine for persistence/restoration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvSnapshot {
    /// Current revision at snapshot time.
    pub revision: u64,
    /// Compaction floor at snapshot time.
    pub compaction_floor: u64,
    /// Current tick at snapshot time.
    pub tick_ms: u64,
    /// All key-value records (including tombstones for recent deletes).
    pub records: Vec<KvRecord>,
    /// MVCC history entries.
    pub history: Vec<HistoryEntry>,
}

/// A history entry for MVCC snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Revision of this entry.
    pub revision: u64,
    /// Key-value pairs modified at this revision.
    pub entries: Vec<(Vec<u8>, KvRecord)>,
}

impl KvSnapshot {
    /// Create a snapshot from a state machine.
    pub fn from_state_machine(sm: &KvStateMachine) -> Self {
        let records: Vec<KvRecord> = sm.index.values().cloned().collect();

        let history: Vec<HistoryEntry> = sm
            .history
            .iter()
            .map(|(rev, entries)| HistoryEntry {
                revision: *rev,
                entries: entries.clone(),
            })
            .collect();

        Self {
            revision: sm.current_revision,
            compaction_floor: sm.compaction_floor,
            tick_ms: sm.current_tick.ms,
            records,
            history,
        }
    }

    /// Restore a state machine from this snapshot.
    pub fn restore(&self) -> KvStateMachine {
        let mut index = std::collections::BTreeMap::new();
        let mut live_key_count = 0;

        for record in &self.records {
            if !record.is_deleted() {
                live_key_count += 1;
            }
            index.insert(record.key.clone(), record.clone());
        }

        let mut history = std::collections::BTreeMap::new();
        for entry in &self.history {
            history.insert(entry.revision, entry.entries.clone());
        }

        KvStateMachine {
            current_revision: self.revision,
            index,
            history,
            compaction_floor: self.compaction_floor,
            pending_events: Vec::new(),
            current_tick: Tick::new(self.tick_ms),
            live_key_count,
        }
    }

    /// Get the size of this snapshot in bytes (approximate).
    pub fn size_bytes(&self) -> usize {
        let mut size = 32; // Header fields

        for record in &self.records {
            size += record.key.len() + record.value.len() + 64; // Metadata overhead
        }

        for entry in &self.history {
            for (k, v) in &entry.entries {
                size += k.len() + v.key.len() + v.value.len() + 64;
            }
        }

        size
    }

    /// Get the number of records in this snapshot.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

impl KvStateMachine {
    /// Create a snapshot of the current state.
    pub fn snapshot(&self) -> KvSnapshot {
        KvSnapshot::from_state_machine(self)
    }

    /// Restore from a snapshot.
    pub fn restore_from_snapshot(snapshot: &KvSnapshot) -> Self {
        snapshot.restore()
    }

    /// Serialize the state machine to bytes.
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        let snapshot = self.snapshot();
        bincode::serialize(&snapshot)
    }

    /// Deserialize a state machine from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, bincode::Error> {
        let snapshot: KvSnapshot = bincode::deserialize(bytes)?;
        Ok(snapshot.restore())
    }
}
