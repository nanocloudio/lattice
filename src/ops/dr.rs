//! Disaster recovery.
//!
//! Per §15, DR is performed via Clustor snapshot export/import.
//! Under unfenced DR, linearizable guarantees are unavailable until
//! fenced promotion completes and Clustor read gates pass.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Instant;

/// DR state tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrState {
    /// Normal operation.
    Normal,
    /// Unfenced DR in progress (linearizable unavailable).
    UnfencedDr,
    /// Fenced promotion in progress.
    FencedPromotion,
    /// DR recovery complete.
    Recovered,
}

impl DrState {
    /// Check if linearizable operations are available.
    pub fn allows_linearizable(&self) -> bool {
        matches!(self, Self::Normal | Self::Recovered)
    }

    /// Check if writes are allowed.
    pub fn allows_writes(&self) -> bool {
        matches!(self, Self::Normal | Self::Recovered)
    }

    /// Check if reads are allowed.
    pub fn allows_reads(&self) -> bool {
        // Reads are allowed in all states (with weaker guarantees during DR)
        true
    }
}

impl std::fmt::Display for DrState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::UnfencedDr => write!(f, "unfenced_dr"),
            Self::FencedPromotion => write!(f, "fenced_promotion"),
            Self::Recovered => write!(f, "recovered"),
        }
    }
}

/// DR snapshot export request.
#[derive(Debug, Clone)]
pub struct SnapshotExportRequest {
    /// Destination path.
    pub destination: String,
    /// Include all tenants (or specific list).
    pub tenants: Option<Vec<String>>,
    /// Compress the snapshot.
    pub compress: bool,
    /// Encrypt the snapshot.
    pub encrypt: bool,
}

impl SnapshotExportRequest {
    /// Create a new export request.
    pub fn new(destination: impl Into<String>) -> Self {
        Self {
            destination: destination.into(),
            tenants: None,
            compress: true,
            encrypt: false,
        }
    }

    /// Export specific tenants.
    pub fn with_tenants(mut self, tenants: Vec<String>) -> Self {
        self.tenants = Some(tenants);
        self
    }

    /// Set encryption.
    pub fn with_encryption(mut self, encrypt: bool) -> Self {
        self.encrypt = encrypt;
        self
    }

    /// Check if this is a full export.
    pub fn is_full_export(&self) -> bool {
        self.tenants.is_none()
    }
}

/// DR snapshot import request.
#[derive(Debug, Clone)]
pub struct SnapshotImportRequest {
    /// Source path.
    pub source: String,
    /// Whether this is a fenced import.
    pub fenced: bool,
    /// Verify checksum before import.
    pub verify_checksum: bool,
    /// Skip tenant existence check.
    pub skip_tenant_check: bool,
}

impl SnapshotImportRequest {
    /// Create a new import request.
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            fenced: true, // Default to fenced import
            verify_checksum: true,
            skip_tenant_check: false,
        }
    }

    /// Create an unfenced import request.
    pub fn unfenced(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            fenced: false,
            verify_checksum: true,
            skip_tenant_check: false,
        }
    }

    /// Skip checksum verification.
    pub fn without_checksum_verification(mut self) -> Self {
        self.verify_checksum = false;
        self
    }
}

/// Snapshot metadata.
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    /// Snapshot ID.
    pub snapshot_id: String,
    /// Snapshot revision.
    pub revision: u64,
    /// Creation timestamp.
    pub created_at: String,
    /// Cluster ID.
    pub cluster_id: String,
    /// Tenants included.
    pub tenants: Vec<String>,
    /// Total size in bytes.
    pub size_bytes: u64,
    /// Checksum.
    pub checksum: String,
    /// Compressed.
    pub compressed: bool,
    /// Encrypted.
    pub encrypted: bool,
}

impl SnapshotMetadata {
    /// Create new metadata.
    pub fn new(snapshot_id: impl Into<String>, revision: u64) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            revision,
            created_at: chrono::Utc::now().to_rfc3339(),
            cluster_id: String::new(),
            tenants: Vec::new(),
            size_bytes: 0,
            checksum: String::new(),
            compressed: false,
            encrypted: false,
        }
    }

    /// Set cluster ID.
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = cluster_id.into();
        self
    }

    /// Set tenants.
    pub fn with_tenants(mut self, tenants: Vec<String>) -> Self {
        self.tenants = tenants;
        self
    }

    /// Set size.
    pub fn with_size(mut self, size_bytes: u64) -> Self {
        self.size_bytes = size_bytes;
        self
    }

    /// Set checksum.
    pub fn with_checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = checksum.into();
        self
    }
}

/// DR coordinator.
#[derive(Debug)]
pub struct DrCoordinator {
    /// Current DR state.
    state: RwLock<DrState>,
    /// State transition time.
    state_changed_at: RwLock<Option<Instant>>,
    /// Current snapshot metadata (during import/export).
    current_snapshot: RwLock<Option<SnapshotMetadata>>,
    /// Statistics.
    stats: DrStats,
}

#[derive(Debug)]
struct DrStats {
    exports_completed: AtomicU64,
    exports_failed: AtomicU64,
    imports_completed: AtomicU64,
    imports_failed: AtomicU64,
    fenced_promotions: AtomicU64,
    unfenced_dr_events: AtomicU64,
}

impl DrStats {
    fn new() -> Self {
        Self {
            exports_completed: AtomicU64::new(0),
            exports_failed: AtomicU64::new(0),
            imports_completed: AtomicU64::new(0),
            imports_failed: AtomicU64::new(0),
            fenced_promotions: AtomicU64::new(0),
            unfenced_dr_events: AtomicU64::new(0),
        }
    }
}

impl DrCoordinator {
    /// Create a new DR coordinator.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(DrState::Normal),
            state_changed_at: RwLock::new(None),
            current_snapshot: RwLock::new(None),
            stats: DrStats::new(),
        }
    }

    /// Get current DR state.
    pub fn state(&self) -> DrState {
        *self.state.read().unwrap()
    }

    /// Set DR state.
    pub fn set_state(&self, state: DrState) {
        *self.state.write().unwrap() = state;
        *self.state_changed_at.write().unwrap() = Some(Instant::now());

        // Update statistics
        match state {
            DrState::UnfencedDr => {
                self.stats
                    .unfenced_dr_events
                    .fetch_add(1, Ordering::Relaxed);
            }
            DrState::FencedPromotion => {
                self.stats.fenced_promotions.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Check if linearizable operations are allowed.
    pub fn allows_linearizable(&self) -> bool {
        self.state().allows_linearizable()
    }

    /// Check if writes are allowed.
    pub fn allows_writes(&self) -> bool {
        self.state().allows_writes()
    }

    /// Start an export.
    pub fn start_export(&self, request: &SnapshotExportRequest) -> DrExportHandle {
        let snapshot_id = format!("export-{}", chrono::Utc::now().timestamp());
        let metadata = SnapshotMetadata::new(&snapshot_id, 0)
            .with_tenants(request.tenants.clone().unwrap_or_default());

        *self.current_snapshot.write().unwrap() = Some(metadata.clone());

        DrExportHandle {
            snapshot_id,
            destination: request.destination.clone(),
            started_at: Instant::now(),
        }
    }

    /// Complete an export.
    pub fn complete_export(&self, handle: DrExportHandle, success: bool) -> DrExportResult {
        *self.current_snapshot.write().unwrap() = None;

        if success {
            self.stats.exports_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.exports_failed.fetch_add(1, Ordering::Relaxed);
        }

        DrExportResult {
            snapshot_id: handle.snapshot_id,
            success,
            duration_ms: handle.started_at.elapsed().as_millis() as u64,
        }
    }

    /// Start an import.
    pub fn start_import(&self, request: &SnapshotImportRequest) -> DrImportHandle {
        // Set state based on fenced/unfenced
        if request.fenced {
            self.set_state(DrState::FencedPromotion);
        } else {
            self.set_state(DrState::UnfencedDr);
        }

        DrImportHandle {
            source: request.source.clone(),
            fenced: request.fenced,
            started_at: Instant::now(),
        }
    }

    /// Complete an import.
    pub fn complete_import(&self, handle: DrImportHandle, success: bool) -> DrImportResult {
        if success {
            self.set_state(DrState::Recovered);
            self.stats.imports_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.set_state(DrState::Normal);
            self.stats.imports_failed.fetch_add(1, Ordering::Relaxed);
        }

        DrImportResult {
            source: handle.source,
            success,
            duration_ms: handle.started_at.elapsed().as_millis() as u64,
            fenced: handle.fenced,
        }
    }

    /// Finalize recovery (transition from Recovered to Normal).
    pub fn finalize_recovery(&self) {
        if self.state() == DrState::Recovered {
            self.set_state(DrState::Normal);
        }
    }

    /// Get current snapshot metadata.
    pub fn current_snapshot(&self) -> Option<SnapshotMetadata> {
        self.current_snapshot.read().unwrap().clone()
    }

    /// Get time since last state change.
    pub fn time_in_current_state(&self) -> Option<std::time::Duration> {
        self.state_changed_at.read().unwrap().map(|t| t.elapsed())
    }

    /// Get statistics.
    pub fn statistics(&self) -> DrStatistics {
        DrStatistics {
            exports_completed: self.stats.exports_completed.load(Ordering::Relaxed),
            exports_failed: self.stats.exports_failed.load(Ordering::Relaxed),
            imports_completed: self.stats.imports_completed.load(Ordering::Relaxed),
            imports_failed: self.stats.imports_failed.load(Ordering::Relaxed),
            fenced_promotions: self.stats.fenced_promotions.load(Ordering::Relaxed),
            unfenced_dr_events: self.stats.unfenced_dr_events.load(Ordering::Relaxed),
        }
    }
}

impl Default for DrCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle for an ongoing export operation.
#[derive(Debug)]
pub struct DrExportHandle {
    /// Snapshot ID.
    pub snapshot_id: String,
    /// Destination path.
    pub destination: String,
    /// Start time.
    pub started_at: Instant,
}

/// Result of an export operation.
#[derive(Debug, Clone)]
pub struct DrExportResult {
    /// Snapshot ID.
    pub snapshot_id: String,
    /// Success status.
    pub success: bool,
    /// Duration in milliseconds.
    pub duration_ms: u64,
}

/// Handle for an ongoing import operation.
#[derive(Debug)]
pub struct DrImportHandle {
    /// Source path.
    pub source: String,
    /// Whether fenced.
    pub fenced: bool,
    /// Start time.
    pub started_at: Instant,
}

/// Result of an import operation.
#[derive(Debug, Clone)]
pub struct DrImportResult {
    /// Source path.
    pub source: String,
    /// Success status.
    pub success: bool,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Whether it was a fenced import.
    pub fenced: bool,
}

/// DR statistics.
#[derive(Debug, Clone)]
pub struct DrStatistics {
    /// Completed exports.
    pub exports_completed: u64,
    /// Failed exports.
    pub exports_failed: u64,
    /// Completed imports.
    pub imports_completed: u64,
    /// Failed imports.
    pub imports_failed: u64,
    /// Fenced promotions.
    pub fenced_promotions: u64,
    /// Unfenced DR events.
    pub unfenced_dr_events: u64,
}

impl DrStatistics {
    /// Total exports.
    pub fn total_exports(&self) -> u64 {
        self.exports_completed + self.exports_failed
    }

    /// Total imports.
    pub fn total_imports(&self) -> u64 {
        self.imports_completed + self.imports_failed
    }

    /// Export success rate.
    pub fn export_success_rate(&self) -> f64 {
        let total = self.total_exports();
        if total == 0 {
            1.0
        } else {
            self.exports_completed as f64 / total as f64
        }
    }

    /// Import success rate.
    pub fn import_success_rate(&self) -> f64 {
        let total = self.total_imports();
        if total == 0 {
            1.0
        } else {
            self.imports_completed as f64 / total as f64
        }
    }
}

/// Replication lag tracker for standby clusters.
#[derive(Debug)]
pub struct ReplicationLagTracker {
    /// Last observed primary revision.
    primary_revision: RwLock<u64>,
    /// Last observed local revision.
    local_revision: RwLock<u64>,
    /// Last update time.
    last_update: RwLock<Option<Instant>>,
}

impl ReplicationLagTracker {
    /// Create a new tracker.
    pub fn new() -> Self {
        Self {
            primary_revision: RwLock::new(0),
            local_revision: RwLock::new(0),
            last_update: RwLock::new(None),
        }
    }

    /// Update primary revision.
    pub fn update_primary(&self, revision: u64) {
        *self.primary_revision.write().unwrap() = revision;
        *self.last_update.write().unwrap() = Some(Instant::now());
    }

    /// Update local revision.
    pub fn update_local(&self, revision: u64) {
        *self.local_revision.write().unwrap() = revision;
    }

    /// Get revision lag.
    pub fn lag(&self) -> u64 {
        let primary = *self.primary_revision.read().unwrap();
        let local = *self.local_revision.read().unwrap();
        primary.saturating_sub(local)
    }

    /// Check if caught up (lag is zero).
    pub fn is_caught_up(&self) -> bool {
        self.lag() == 0
    }

    /// Get time since last update.
    pub fn time_since_update(&self) -> Option<std::time::Duration> {
        self.last_update.read().unwrap().map(|t| t.elapsed())
    }
}

impl Default for ReplicationLagTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dr_state_allows_linearizable() {
        assert!(DrState::Normal.allows_linearizable());
        assert!(!DrState::UnfencedDr.allows_linearizable());
        assert!(!DrState::FencedPromotion.allows_linearizable());
        assert!(DrState::Recovered.allows_linearizable());
    }

    #[test]
    fn test_dr_state_allows_writes() {
        assert!(DrState::Normal.allows_writes());
        assert!(!DrState::UnfencedDr.allows_writes());
        assert!(!DrState::FencedPromotion.allows_writes());
        assert!(DrState::Recovered.allows_writes());
    }

    #[test]
    fn test_dr_coordinator_state() {
        let coord = DrCoordinator::new();
        assert_eq!(coord.state(), DrState::Normal);

        coord.set_state(DrState::UnfencedDr);
        assert_eq!(coord.state(), DrState::UnfencedDr);
        assert!(!coord.allows_linearizable());
    }

    #[test]
    fn test_dr_coordinator_export() {
        let coord = DrCoordinator::new();

        let request = SnapshotExportRequest::new("/backup/snapshot.tar");
        let handle = coord.start_export(&request);
        assert!(!handle.snapshot_id.is_empty());

        let result = coord.complete_export(handle, true);
        assert!(result.success);
        assert_eq!(coord.statistics().exports_completed, 1);
    }

    #[test]
    fn test_dr_coordinator_import_fenced() {
        let coord = DrCoordinator::new();

        let request = SnapshotImportRequest::new("/backup/snapshot.tar");
        let handle = coord.start_import(&request);

        assert_eq!(coord.state(), DrState::FencedPromotion);
        assert!(handle.fenced);

        let result = coord.complete_import(handle, true);
        assert!(result.success);
        assert_eq!(coord.state(), DrState::Recovered);
    }

    #[test]
    fn test_dr_coordinator_import_unfenced() {
        let coord = DrCoordinator::new();

        let request = SnapshotImportRequest::unfenced("/backup/snapshot.tar");
        let handle = coord.start_import(&request);

        assert_eq!(coord.state(), DrState::UnfencedDr);
        assert!(!handle.fenced);

        let result = coord.complete_import(handle, true);
        assert!(result.success);
        assert_eq!(coord.state(), DrState::Recovered);
    }

    #[test]
    fn test_dr_coordinator_finalize_recovery() {
        let coord = DrCoordinator::new();
        coord.set_state(DrState::Recovered);

        coord.finalize_recovery();
        assert_eq!(coord.state(), DrState::Normal);
    }

    #[test]
    fn test_snapshot_export_request() {
        let req = SnapshotExportRequest::new("/backup/snap.tar")
            .with_tenants(vec!["tenant1".to_string()])
            .with_encryption(true);

        assert!(!req.is_full_export());
        assert!(req.encrypt);
    }

    #[test]
    fn test_snapshot_import_request() {
        let req = SnapshotImportRequest::new("/backup/snap.tar");
        assert!(req.fenced);
        assert!(req.verify_checksum);

        let req2 = SnapshotImportRequest::unfenced("/backup/snap.tar");
        assert!(!req2.fenced);
    }

    #[test]
    fn test_snapshot_metadata() {
        let meta = SnapshotMetadata::new("snap-123", 100)
            .with_cluster_id("cluster1")
            .with_tenants(vec!["t1".to_string(), "t2".to_string()])
            .with_size(1000)
            .with_checksum("abc123");

        assert_eq!(meta.snapshot_id, "snap-123");
        assert_eq!(meta.revision, 100);
        assert_eq!(meta.cluster_id, "cluster1");
        assert_eq!(meta.tenants.len(), 2);
        assert_eq!(meta.size_bytes, 1000);
        assert_eq!(meta.checksum, "abc123");
    }

    #[test]
    fn test_dr_statistics() {
        let coord = DrCoordinator::new();

        // Simulate some operations
        let req = SnapshotExportRequest::new("/backup/1");
        let handle = coord.start_export(&req);
        coord.complete_export(handle, true);

        let handle2 = coord.start_export(&req);
        coord.complete_export(handle2, false);

        let stats = coord.statistics();
        assert_eq!(stats.total_exports(), 2);
        assert_eq!(stats.exports_completed, 1);
        assert_eq!(stats.exports_failed, 1);
        assert_eq!(stats.export_success_rate(), 0.5);
    }

    #[test]
    fn test_replication_lag_tracker() {
        let tracker = ReplicationLagTracker::new();

        tracker.update_primary(100);
        tracker.update_local(80);

        assert_eq!(tracker.lag(), 20);
        assert!(!tracker.is_caught_up());

        tracker.update_local(100);
        assert_eq!(tracker.lag(), 0);
        assert!(tracker.is_caught_up());
    }

    #[test]
    fn test_replication_lag_tracker_time() {
        let tracker = ReplicationLagTracker::new();
        assert!(tracker.time_since_update().is_none());

        tracker.update_primary(100);
        assert!(tracker.time_since_update().is_some());
    }
}
