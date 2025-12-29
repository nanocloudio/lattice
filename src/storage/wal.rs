//! Write-ahead log and snapshot management.
//!
//! This module wraps Clustor's WAL/snapshot backend, enforcing the
//! KVSOURCE invariant: all KV effects derive from WAL entries or
//! signed snapshots.
//!
//! # WAL Entry Types
//!
//! The WAL stores the following entry types:
//! - `Put`: Key-value write operations
//! - `Delete`: Key deletion operations
//! - `Tick`: Deterministic time advancement
//! - `LeaseGrant`: Lease creation
//! - `LeaseRevoke`: Lease revocation
//! - `LeaseKeepalive`: Lease renewal
//! - `Compaction`: Compaction floor advancement
//!
//! # Segment Management
//!
//! WAL entries are organized into segments for efficient rotation and archival.
//! Each segment has a maximum size, after which a new segment is created.

use crate::core::time::Tick;
use crate::kpg::lease::LeaseRecord;
use crate::kpg::state_machine::KvRecord;
use crate::kpg::watch::WatchState;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Default maximum segment size (64 MB).
const DEFAULT_SEGMENT_MAX_SIZE: u64 = 64 * 1024 * 1024;

/// WAL entry types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntryType {
    /// Key-value put operation.
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: Option<i64>,
    },
    /// Key deletion operation.
    Delete { key: Vec<u8> },
    /// Range deletion operation.
    DeleteRange {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
    /// Deterministic tick for time advancement.
    Tick { ms: u64 },
    /// Lease grant operation.
    LeaseGrant {
        lease_id: i64,
        ttl_ms: u64,
        granted_at_ms: u64,
    },
    /// Lease revoke operation.
    LeaseRevoke { lease_id: i64 },
    /// Lease keepalive operation.
    LeaseKeepalive { lease_id: i64, tick_ms: u64 },
    /// Compaction floor advancement.
    Compaction { floor_revision: u64 },
    /// Transaction containing multiple operations.
    Transaction { operations: Vec<WalEntryType> },
    /// No-op entry (used for leader election).
    Noop,
}

/// A single WAL entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Entry index (Raft commit index / revision).
    pub index: u64,
    /// Entry term.
    pub term: u64,
    /// Entry type and payload.
    pub entry_type: WalEntryType,
    /// Timestamp when entry was created (for diagnostics).
    pub timestamp_ms: u64,
}

impl WalEntry {
    /// Create a new WAL entry.
    pub fn new(index: u64, term: u64, entry_type: WalEntryType) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            index,
            term,
            entry_type,
            timestamp_ms,
        }
    }

    /// Create a Put entry.
    pub fn put(index: u64, term: u64, key: Vec<u8>, value: Vec<u8>, lease_id: Option<i64>) -> Self {
        Self::new(
            index,
            term,
            WalEntryType::Put {
                key,
                value,
                lease_id,
            },
        )
    }

    /// Create a Delete entry.
    pub fn delete(index: u64, term: u64, key: Vec<u8>) -> Self {
        Self::new(index, term, WalEntryType::Delete { key })
    }

    /// Create a Tick entry.
    pub fn tick(index: u64, term: u64, tick: Tick) -> Self {
        Self::new(index, term, WalEntryType::Tick { ms: tick.ms })
    }

    /// Create a LeaseGrant entry.
    pub fn lease_grant(
        index: u64,
        term: u64,
        lease_id: i64,
        ttl_ms: u64,
        granted_at: Tick,
    ) -> Self {
        Self::new(
            index,
            term,
            WalEntryType::LeaseGrant {
                lease_id,
                ttl_ms,
                granted_at_ms: granted_at.ms,
            },
        )
    }

    /// Serialize this entry to bytes.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("failed to serialize WAL entry")
    }

    /// Deserialize an entry from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("failed to deserialize WAL entry")
    }
}

/// WAL segment state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    /// Segment is open for writing.
    Open,
    /// Segment is sealed and immutable.
    Sealed,
    /// Segment has been archived.
    Archived,
}

/// A WAL segment file.
#[derive(Debug)]
pub struct WalSegment {
    /// Segment ID (sequential).
    pub id: u64,
    /// First entry index in this segment.
    pub start_index: u64,
    /// Last entry index in this segment (exclusive).
    pub end_index: u64,
    /// Segment file path.
    pub path: PathBuf,
    /// Current size in bytes.
    pub size_bytes: u64,
    /// Maximum size before rotation.
    pub max_size_bytes: u64,
    /// Segment state.
    pub state: SegmentState,
}

impl WalSegment {
    /// Create a new segment.
    pub fn new(id: u64, start_index: u64, path: PathBuf, max_size_bytes: u64) -> Self {
        Self {
            id,
            start_index,
            end_index: start_index,
            path,
            size_bytes: 0,
            max_size_bytes,
            state: SegmentState::Open,
        }
    }

    /// Check if this segment is full.
    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.max_size_bytes
    }

    /// Check if this segment is open for writing.
    pub fn is_open(&self) -> bool {
        self.state == SegmentState::Open
    }

    /// Seal this segment (make it immutable).
    pub fn seal(&mut self) {
        self.state = SegmentState::Sealed;
    }

    /// Mark this segment as archived.
    pub fn archive(&mut self) {
        self.state = SegmentState::Archived;
    }

    /// Get the entry count in this segment.
    pub fn entry_count(&self) -> u64 {
        self.end_index - self.start_index
    }
}

/// Snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// Snapshot index (revision).
    pub index: u64,
    /// Snapshot term.
    pub term: u64,
    /// Snapshot file path.
    pub path: PathBuf,
    /// Creation timestamp.
    pub created_at_ms: u64,
    /// Snapshot size in bytes.
    pub size_bytes: u64,
    /// Whether the snapshot is signed (for DR verification).
    pub signed: bool,
}

impl SnapshotMeta {
    /// Create a new snapshot metadata.
    pub fn new(index: u64, term: u64, path: PathBuf) -> Self {
        Self {
            index,
            term,
            path,
            created_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            size_bytes: 0,
            signed: false,
        }
    }
}

/// KV state snapshot for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot metadata.
    pub meta: SnapshotMeta,
    /// KV records.
    pub kv_records: Vec<KvRecord>,
    /// Lease records.
    pub leases: Vec<LeaseRecord>,
    /// Watch states (for etcd conformance).
    pub watches: Vec<WatchState>,
    /// Current compaction floor.
    pub compaction_floor: u64,
    /// Current tick value.
    pub current_tick: Tick,
    /// Idempotency table (key -> outcome digest).
    pub idempotency_table: HashMap<String, Vec<u8>>,
}

impl Snapshot {
    /// Create a new empty snapshot.
    pub fn new(index: u64, term: u64, path: PathBuf) -> Self {
        Self {
            meta: SnapshotMeta::new(index, term, path),
            kv_records: Vec::new(),
            leases: Vec::new(),
            watches: Vec::new(),
            compaction_floor: 0,
            current_tick: Tick::zero(),
            idempotency_table: HashMap::new(),
        }
    }

    /// Serialize the snapshot to bytes.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("failed to serialize snapshot")
    }

    /// Deserialize a snapshot from bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).context("failed to deserialize snapshot")
    }

    /// Write the snapshot to a file.
    pub fn write_to_file(&self, path: &PathBuf) -> Result<()> {
        let file = std::fs::File::create(path).context("failed to create snapshot file")?;
        let mut writer = BufWriter::new(file);
        let bytes = self.serialize()?;
        writer
            .write_all(&bytes)
            .context("failed to write snapshot")?;
        writer.flush().context("failed to flush snapshot")?;
        Ok(())
    }

    /// Read a snapshot from a file.
    pub fn read_from_file(path: &PathBuf) -> Result<Self> {
        let file = std::fs::File::open(path).context("failed to open snapshot file")?;
        let mut reader = BufReader::new(file);
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .context("failed to read snapshot")?;
        Self::deserialize(&bytes)
    }
}

/// Clustor storage wrapper for Lattice.
///
/// All durable state flows through this wrapper, ensuring KVSOURCE compliance.
pub struct ClustorStorage {
    /// Storage directory path.
    storage_dir: PathBuf,

    /// WAL directory path.
    wal_dir: PathBuf,

    /// Snapshot directory path.
    snapshot_dir: PathBuf,

    /// Current WAL segment.
    current_segment: Arc<RwLock<Option<WalSegment>>>,

    /// Segment counter for generating IDs.
    segment_counter: AtomicU64,

    /// Last written index.
    last_index: AtomicU64,

    /// Last written term.
    last_term: AtomicU64,

    /// Maximum segment size.
    max_segment_size: u64,
}

impl ClustorStorage {
    /// Create a new Clustor storage wrapper.
    pub fn new(storage_dir: PathBuf) -> Self {
        let wal_dir = storage_dir.join("wal");
        let snapshot_dir = storage_dir.join("snapshots");

        Self {
            storage_dir,
            wal_dir,
            snapshot_dir,
            current_segment: Arc::new(RwLock::new(None)),
            segment_counter: AtomicU64::new(0),
            last_index: AtomicU64::new(0),
            last_term: AtomicU64::new(0),
            max_segment_size: DEFAULT_SEGMENT_MAX_SIZE,
        }
    }

    /// Create storage with custom segment size.
    pub fn with_segment_size(mut self, max_size: u64) -> Self {
        self.max_segment_size = max_size;
        self
    }

    /// Get the storage directory path.
    pub fn storage_dir(&self) -> &PathBuf {
        &self.storage_dir
    }

    /// Get the WAL directory path.
    pub fn wal_dir(&self) -> &PathBuf {
        &self.wal_dir
    }

    /// Get the snapshot directory path.
    pub fn snapshot_dir(&self) -> &PathBuf {
        &self.snapshot_dir
    }

    /// Get the last written index.
    pub fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::Acquire)
    }

    /// Get the last written term.
    pub fn last_term(&self) -> u64 {
        self.last_term.load(Ordering::Acquire)
    }

    /// Initialize storage, creating directories if needed.
    pub fn initialize(&self) -> Result<()> {
        std::fs::create_dir_all(&self.storage_dir).context("failed to create storage directory")?;
        std::fs::create_dir_all(&self.wal_dir).context("failed to create WAL directory")?;
        std::fs::create_dir_all(&self.snapshot_dir)
            .context("failed to create snapshot directory")?;

        // Scan existing segments to recover state
        self.recover_state()?;

        Ok(())
    }

    /// Recover state from existing WAL segments.
    fn recover_state(&self) -> Result<()> {
        // Find the highest segment ID and entry index
        let mut max_segment_id = 0u64;
        let max_index = 0u64;

        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_stem() {
                    if let Some(name_str) = name.to_str() {
                        if let Some(id_str) = name_str.strip_prefix("segment_") {
                            if let Ok(id) = id_str.parse::<u64>() {
                                max_segment_id = max_segment_id.max(id);
                            }
                        }
                    }
                }
            }
        }

        self.segment_counter
            .store(max_segment_id + 1, Ordering::Release);
        self.last_index.store(max_index, Ordering::Release);

        Ok(())
    }

    /// Append an entry to the WAL.
    pub fn append(&self, entry: &WalEntry) -> Result<()> {
        let bytes = entry.serialize()?;

        // Ensure we have an open segment
        self.ensure_open_segment(entry.index)?;

        // Write to segment file
        let mut segment_guard = self.current_segment.write().unwrap();
        if let Some(ref mut segment) = *segment_guard {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&segment.path)
                .context("failed to open segment file")?;

            let mut writer = BufWriter::new(file);

            // Write length-prefixed entry
            let len = bytes.len() as u32;
            writer
                .write_all(&len.to_le_bytes())
                .context("failed to write entry length")?;
            writer
                .write_all(&bytes)
                .context("failed to write entry data")?;
            writer.flush().context("failed to flush segment")?;

            segment.size_bytes += 4 + bytes.len() as u64;
            segment.end_index = entry.index + 1;

            self.last_index.store(entry.index, Ordering::Release);
            self.last_term.store(entry.term, Ordering::Release);
        }

        Ok(())
    }

    /// Ensure there is an open segment available.
    fn ensure_open_segment(&self, next_index: u64) -> Result<()> {
        let mut segment_guard = self.current_segment.write().unwrap();

        // Check if current segment needs rotation
        let needs_new = match &*segment_guard {
            None => true,
            Some(segment) => segment.is_full() || !segment.is_open(),
        };

        if needs_new {
            // Seal the old segment
            if let Some(ref mut old_segment) = *segment_guard {
                old_segment.seal();
            }

            // Create new segment
            let segment_id = self.segment_counter.fetch_add(1, Ordering::AcqRel);
            let segment_path = self.wal_dir.join(format!("segment_{:016}.wal", segment_id));
            let new_segment =
                WalSegment::new(segment_id, next_index, segment_path, self.max_segment_size);

            *segment_guard = Some(new_segment);
        }

        Ok(())
    }

    /// Read entries from start_index (inclusive) to end_index (exclusive).
    pub fn read_entries(&self, start_index: u64, end_index: u64) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();

        // Read from segment files
        if let Ok(dir_entries) = std::fs::read_dir(&self.wal_dir) {
            let mut segment_paths: Vec<_> = dir_entries
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
                .map(|e| e.path())
                .collect();
            segment_paths.sort();

            for path in segment_paths {
                let segment_entries = self.read_segment_entries(&path, start_index, end_index)?;
                entries.extend(segment_entries);
            }
        }

        Ok(entries)
    }

    /// Read entries from a single segment file.
    fn read_segment_entries(
        &self,
        path: &PathBuf,
        start_index: u64,
        end_index: u64,
    ) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();

        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(entries),
        };

        let mut reader = BufReader::new(file);
        let mut len_buf = [0u8; 4];

        loop {
            // Read entry length
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e).context("failed to read entry length"),
            }

            let len = u32::from_le_bytes(len_buf) as usize;
            let mut entry_buf = vec![0u8; len];
            reader
                .read_exact(&mut entry_buf)
                .context("failed to read entry data")?;

            let entry = WalEntry::deserialize(&entry_buf)?;

            if entry.index >= end_index {
                break;
            }
            if entry.index >= start_index {
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    /// Create a snapshot from the current state.
    pub fn create_snapshot(&self, snapshot: &Snapshot) -> Result<SnapshotMeta> {
        let snapshot_path = self.snapshot_dir.join(format!(
            "snapshot_{:016}_{:016}.snap",
            snapshot.meta.index, snapshot.meta.term
        ));

        snapshot.write_to_file(&snapshot_path)?;

        let file_size = std::fs::metadata(&snapshot_path)
            .map(|m| m.len())
            .unwrap_or(0);

        let mut meta = snapshot.meta.clone();
        meta.path = snapshot_path;
        meta.size_bytes = file_size;

        Ok(meta)
    }

    /// Load a snapshot from disk.
    pub fn load_snapshot(&self, meta: &SnapshotMeta) -> Result<Snapshot> {
        Snapshot::read_from_file(&meta.path)
    }

    /// List available snapshots, sorted by index (newest first).
    pub fn list_snapshots(&self) -> Result<Vec<SnapshotMeta>> {
        let mut snapshots = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&self.snapshot_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "snap") {
                    // Parse snapshot metadata from filename
                    if let Some(name) = path.file_stem() {
                        if let Some(name_str) = name.to_str() {
                            if let Some(rest) = name_str.strip_prefix("snapshot_") {
                                let parts: Vec<&str> = rest.split('_').collect();
                                if parts.len() == 2 {
                                    if let (Ok(index), Ok(term)) =
                                        (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                                    {
                                        let file_size =
                                            std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

                                        let mut meta = SnapshotMeta::new(index, term, path);
                                        meta.size_bytes = file_size;
                                        snapshots.push(meta);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort by index descending (newest first)
        snapshots.sort_by(|a, b| b.index.cmp(&a.index));

        Ok(snapshots)
    }

    /// Get the latest snapshot metadata.
    pub fn latest_snapshot(&self) -> Result<Option<SnapshotMeta>> {
        let snapshots = self.list_snapshots()?;
        Ok(snapshots.into_iter().next())
    }

    /// Delete snapshots older than the given index.
    pub fn cleanup_snapshots(&self, keep_index: u64) -> Result<u64> {
        let mut deleted = 0;

        if let Ok(entries) = std::fs::read_dir(&self.snapshot_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "snap") {
                    if let Some(name) = path.file_stem() {
                        if let Some(name_str) = name.to_str() {
                            if let Some(rest) = name_str.strip_prefix("snapshot_") {
                                let parts: Vec<&str> = rest.split('_').collect();
                                if !parts.is_empty() {
                                    if let Ok(index) = parts[0].parse::<u64>() {
                                        if index < keep_index && std::fs::remove_file(&path).is_ok()
                                        {
                                            deleted += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(deleted)
    }

    /// Shutdown the storage layer.
    pub fn shutdown(&self) -> Result<()> {
        // Seal the current segment
        let mut segment_guard = self.current_segment.write().unwrap();
        if let Some(ref mut segment) = *segment_guard {
            segment.seal();
        }
        *segment_guard = None;

        Ok(())
    }
}
