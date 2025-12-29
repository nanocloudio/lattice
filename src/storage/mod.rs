//! Storage layer.
//!
//! This module wraps Clustor's WAL and snapshot functionality, enforcing
//! the KVSOURCE invariant: all KV effects derive from WAL entries or
//! signed snapshots.
//!
//! # Modules
//!
//! - [`wal`] - Write-ahead log and snapshot management
//! - [`compaction`] - Log compaction and cleanup
//!
//! # Invariants
//!
//! - **KVSOURCE**: All KV state changes flow through the WAL or snapshots
//! - **Compaction floor**: KV compaction floor never exceeds Clustor's floor
//! - **Snapshot signing**: DR snapshots must be signed for verification

pub mod compaction;
pub mod wal;

// Re-exports for convenience
pub use compaction::{CompactionCoordinator, CompactionPolicy, WatchRevisionTracker};
pub use wal::{ClustorStorage, Snapshot, SnapshotMeta, WalEntry, WalEntryType, WalSegment};
