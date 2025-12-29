//! Core infrastructure tests.

mod common;

use lattice::core::config::Config;
use lattice::core::error::{LatticeError, LinearizabilityFailureReason};
use lattice::core::time::{Tick, TickSourceConfig, TickSourceType};
use std::io::Write;
use tempfile::NamedTempFile;

// ============================================================================
// Config tests
// ============================================================================

#[test]
fn parse_minimal_config() {
    let config_content = r#"
[control_plane]
mode = "embedded"

[listeners]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config_content.as_bytes()).unwrap();

    let config = Config::from_file(file.path()).unwrap();
    assert_eq!(config.control_plane.mode, "embedded");
    assert_eq!(config.durability.durability_mode, "strict");
}

#[test]
fn validate_invalid_cp_mode() {
    let config_content = r#"
[control_plane]
mode = "invalid"

[listeners]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config_content.as_bytes()).unwrap();

    let result = Config::from_file(file.path());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("mode"));
}

#[test]
fn validate_external_mode_requires_endpoints() {
    let config_content = r#"
[control_plane]
mode = "external"
endpoints = []

[listeners]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config_content.as_bytes()).unwrap();

    let result = Config::from_file(file.path());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("endpoints"));
}

#[test]
fn validate_lease_ttl_bounds() {
    let config_content = r#"
[control_plane]
mode = "embedded"

[listeners]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"

[tenants]
lease_ttl_default_ms = 700000
lease_ttl_max_ms = 600000
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config_content.as_bytes()).unwrap();

    let result = Config::from_file(file.path());
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    // Either "lease_ttl" or the actual values should be in the error
    assert!(
        err_msg.contains("lease_ttl") || err_msg.contains("700000") || err_msg.contains("600000"),
        "Error should mention lease TTL bounds: {}",
        err_msg
    );
}

// ============================================================================
// Error tests
// ============================================================================

#[test]
fn dirty_epoch_error_format() {
    let err = LatticeError::dirty_epoch(5, 3);
    let msg = err.to_string();
    assert!(msg.contains("5"));
    assert!(msg.contains("3"));
}

#[test]
fn linearizability_unavailable_error_format() {
    let err =
        LatticeError::linearizability_unavailable(LinearizabilityFailureReason::StrictFallback);
    let msg = err.to_string();
    assert!(msg.contains("LinearizabilityUnavailable"));
    assert!(msg.contains("StrictFallback"));
}

#[test]
fn txn_cross_shard_is_stable_identifier() {
    let err = LatticeError::TxnCrossShardUnsupported;
    assert!(err.is_stable_identifier());
    assert_eq!(err.to_string(), "TxnCrossShardUnsupported");
}

#[test]
fn linearizability_unavailable_is_stable_identifier() {
    let err =
        LatticeError::linearizability_unavailable(LinearizabilityFailureReason::ProofMismatch);
    assert!(err.is_stable_identifier());
}

#[test]
fn throttle_is_retriable() {
    let err = LatticeError::throttle("rate limit exceeded");
    assert!(err.is_retriable());
}

#[test]
fn linearizability_unavailable_is_retriable() {
    let err = LatticeError::linearizability_unavailable(LinearizabilityFailureReason::NotLeader);
    assert!(err.is_retriable());
}

#[test]
fn key_not_found_is_not_retriable() {
    let err = LatticeError::KeyNotFound;
    assert!(!err.is_retriable());
}

// ============================================================================
// Time tests
// ============================================================================

#[test]
fn tick_ordering() {
    let t1 = Tick::new(100);
    let t2 = Tick::new(200);
    assert!(t1 < t2);
    assert!(t2 > t1);
    assert!(t1.is_before(t2));
    assert!(t2.is_at_or_after(t1));
}

#[test]
fn tick_arithmetic() {
    let t = Tick::new(100);
    assert_eq!(t.add_ms(50).ms, 150);
    assert_eq!(t.sub_ms(50).ms, 50);
    assert_eq!(t.sub_ms(150).ms, 0); // Saturating subtraction
}

#[test]
fn tick_ms_until() {
    let now = Tick::new(100);
    let deadline = Tick::new(250);
    assert_eq!(now.ms_until(deadline), 150);

    // Past deadline returns 0
    let past = Tick::new(50);
    assert_eq!(now.ms_until(past), 0);
}

#[test]
fn tick_expiry_check() {
    let now = Tick::new(100);
    let past_deadline = Tick::new(50);
    let future_deadline = Tick::new(150);

    assert!(now.is_at_or_after(past_deadline));
    assert!(!now.is_at_or_after(future_deadline));
}

#[test]
fn tick_source_config_defaults() {
    let config = TickSourceConfig::default();
    assert_eq!(config.source_type, TickSourceType::WalTick);
    assert_eq!(config.period_ms, 1000);
}

#[test]
fn tick_source_config_validation() {
    let mut config = TickSourceConfig::default();
    assert!(config.validate().is_ok());

    config.period_ms = 0;
    assert!(config.validate().is_err());
}

// ============================================================================
// Tick source tests
// ============================================================================

use lattice::core::time::{TickSource, TickSourceRegistry, WalTickSource};

#[test]
fn wal_tick_source_basic() {
    let source = WalTickSource::new(1000);
    assert_eq!(source.source_type(), TickSourceType::WalTick);
    assert_eq!(source.period_ms(), 1000);

    // Current tick should be non-zero (wall clock time)
    let tick = source.current_tick();
    assert!(tick.ms > 0);
}

#[test]
fn wal_tick_source_should_emit_first() {
    let source = WalTickSource::new(1000);

    // First tick should always be emitted
    let tick = source.should_emit(None);
    assert!(tick.is_some());
}

#[test]
fn wal_tick_source_should_not_emit_too_soon() {
    let source = WalTickSource::new(1000);

    // Get current time
    let now = source.current_tick();

    // If we just emitted, should not emit again
    let tick = source.should_emit(Some(now));
    assert!(tick.is_none());
}

#[test]
fn tick_source_registry_from_config() {
    let config = TickSourceConfig {
        source_type: TickSourceType::WalTick,
        period_ms: 500,
    };

    let registry = TickSourceRegistry::from_config(&config);
    assert_eq!(registry.source_type(), TickSourceType::WalTick);
}

// ============================================================================
// Error mapping tests
// ============================================================================

use lattice::core::error::{EtcdErrorMapping, GrpcCode};

#[test]
fn etcd_error_mapping_dirty_epoch() {
    let err = LatticeError::dirty_epoch(5, 3);
    let code = EtcdErrorMapping::to_grpc_code(&err);
    assert_eq!(code, GrpcCode::FailedPrecondition);
}

#[test]
fn etcd_error_mapping_linearizability_unavailable() {
    let err = LatticeError::linearizability_unavailable(LinearizabilityFailureReason::NotLeader);
    let code = EtcdErrorMapping::to_grpc_code(&err);
    assert_eq!(code, GrpcCode::Unavailable);
}

#[test]
fn etcd_error_mapping_throttle() {
    let err = LatticeError::throttle("rate limited");
    let code = EtcdErrorMapping::to_grpc_code(&err);
    assert_eq!(code, GrpcCode::ResourceExhausted);
}

#[test]
fn etcd_error_mapping_key_not_found() {
    let err = LatticeError::KeyNotFound;
    let code = EtcdErrorMapping::to_grpc_code(&err);
    assert_eq!(code, GrpcCode::NotFound);
}

#[test]
fn etcd_error_message_formatting() {
    let err = LatticeError::dirty_epoch(10, 5);
    let msg = EtcdErrorMapping::to_error_message(&err);
    assert!(msg.contains("epoch"));
    assert!(msg.contains("10"));
    assert!(msg.contains("5"));
}

// ============================================================================
// Config override tests
// ============================================================================

use lattice::core::config::ConfigOverrides;

#[test]
fn config_overrides_apply() {
    let config_content = r#"
[control_plane]
mode = "embedded"

[listeners]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"

[telemetry]
log_level = "info"
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config_content.as_bytes()).unwrap();

    let mut config = Config::from_file(file.path()).unwrap();
    assert_eq!(config.telemetry.log_level, "info");

    let overrides = ConfigOverrides {
        log_level: Some("debug".to_string()),
        storage_dir: Some("/custom/path".to_string()),
        ..Default::default()
    };

    config.apply_overrides(&overrides);
    assert_eq!(config.telemetry.log_level, "debug");
    assert_eq!(config.paths.storage_dir, "/custom/path");
}

// ============================================================================
// Runtime tests
// ============================================================================

use lattice::core::runtime::{ComponentHealth, Runtime, RuntimeHealth};

#[test]
fn runtime_health_default_is_starting() {
    let health = RuntimeHealth::default();
    assert_eq!(health.storage, ComponentHealth::Starting);
    assert_eq!(health.control_plane, ComponentHealth::Starting);
    assert!(!health.is_ready());
    assert!(health.is_alive());
}

#[test]
fn runtime_health_ready_when_all_healthy() {
    let health = RuntimeHealth {
        storage: ComponentHealth::Healthy,
        control_plane: ComponentHealth::Healthy,
        kpg_runtime: ComponentHealth::Healthy,
        adapters: ComponentHealth::Healthy,
        listeners: ComponentHealth::Healthy,
    };
    assert!(health.is_ready());
    assert!(health.is_alive());
}

#[test]
fn runtime_health_not_alive_when_failed() {
    let health = RuntimeHealth {
        storage: ComponentHealth::Failed,
        control_plane: ComponentHealth::Healthy,
        kpg_runtime: ComponentHealth::Healthy,
        adapters: ComponentHealth::Healthy,
        listeners: ComponentHealth::Healthy,
    };
    assert!(!health.is_alive());
    assert!(!health.is_ready());
}

#[test]
fn runtime_health_ready_with_degraded_cp() {
    let health = RuntimeHealth {
        storage: ComponentHealth::Healthy,
        control_plane: ComponentHealth::Degraded,
        kpg_runtime: ComponentHealth::Healthy,
        adapters: ComponentHealth::Healthy,
        listeners: ComponentHealth::Healthy,
    };
    assert!(health.is_ready()); // Degraded CP still allows ready
    assert!(health.is_alive());
}

#[tokio::test]
async fn runtime_creates_with_valid_config() {
    let config_content = r#"
[control_plane]
mode = "embedded"

[listeners]

[durability]
durability_mode = "strict"
commit_visibility = "durable_only"
quorum_size = 1
replica_id = "test"

[paths]
storage_dir = "/tmp/lattice-test"
"#;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(config_content.as_bytes()).unwrap();

    let config = Config::from_file(file.path()).unwrap();
    let runtime = Runtime::new(config);
    assert!(runtime.is_ok());
}

// ============================================================================
// Storage WAL tests
// ============================================================================

use lattice::storage::wal::{ClustorStorage, Snapshot, WalEntry, WalEntryType};

#[test]
fn clustor_storage_creates_directories() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage_dir = tempdir.path().join("storage");

    let storage = ClustorStorage::new(storage_dir.clone());
    storage.initialize().unwrap();

    assert!(storage_dir.exists());
    assert!(storage_dir.join("wal").exists());
    assert!(storage_dir.join("snapshots").exists());
}

#[test]
fn wal_entry_serialization_roundtrip() {
    let entry = WalEntry::put(1, 1, b"key".to_vec(), b"value".to_vec(), None);

    let bytes = entry.serialize().unwrap();
    let deserialized = WalEntry::deserialize(&bytes).unwrap();

    assert_eq!(deserialized.index, 1);
    assert_eq!(deserialized.term, 1);
    match deserialized.entry_type {
        WalEntryType::Put {
            key,
            value,
            lease_id,
        } => {
            assert_eq!(key, b"key");
            assert_eq!(value, b"value");
            assert!(lease_id.is_none());
        }
        _ => panic!("wrong entry type"),
    }
}

#[test]
fn wal_entry_tick_serialization() {
    let tick = Tick::new(12345);
    let entry = WalEntry::tick(5, 2, tick);

    let bytes = entry.serialize().unwrap();
    let deserialized = WalEntry::deserialize(&bytes).unwrap();

    match deserialized.entry_type {
        WalEntryType::Tick { ms } => assert_eq!(ms, 12345),
        _ => panic!("wrong entry type"),
    }
}

#[test]
fn wal_entry_lease_grant_serialization() {
    let tick = Tick::new(1000);
    let entry = WalEntry::lease_grant(10, 3, 42, 60000, tick);

    let bytes = entry.serialize().unwrap();
    let deserialized = WalEntry::deserialize(&bytes).unwrap();

    match deserialized.entry_type {
        WalEntryType::LeaseGrant {
            lease_id,
            ttl_ms,
            granted_at_ms,
        } => {
            assert_eq!(lease_id, 42);
            assert_eq!(ttl_ms, 60000);
            assert_eq!(granted_at_ms, 1000);
        }
        _ => panic!("wrong entry type"),
    }
}

#[test]
fn clustor_storage_append_and_read() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage = ClustorStorage::new(tempdir.path().to_path_buf());
    storage.initialize().unwrap();

    // Append entries
    for i in 1..=5 {
        let entry = WalEntry::put(
            i,
            1,
            format!("key{}", i).into_bytes(),
            b"value".to_vec(),
            None,
        );
        storage.append(&entry).unwrap();
    }

    assert_eq!(storage.last_index(), 5);

    // Read entries
    let entries = storage.read_entries(1, 6).unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0].index, 1);
    assert_eq!(entries[4].index, 5);
}

#[test]
fn snapshot_serialization_roundtrip() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("test.snap");

    let mut snapshot = Snapshot::new(100, 5, path.clone());
    snapshot.compaction_floor = 50;
    snapshot.current_tick = Tick::new(5000);

    let bytes = snapshot.serialize().unwrap();
    let deserialized = Snapshot::deserialize(&bytes).unwrap();

    assert_eq!(deserialized.meta.index, 100);
    assert_eq!(deserialized.meta.term, 5);
    assert_eq!(deserialized.compaction_floor, 50);
    assert_eq!(deserialized.current_tick.ms, 5000);
}

#[test]
fn snapshot_write_and_read_file() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("snapshot.snap");

    let mut snapshot = Snapshot::new(200, 10, path.clone());
    snapshot.compaction_floor = 100;

    snapshot.write_to_file(&path).unwrap();
    let loaded = Snapshot::read_from_file(&path).unwrap();

    assert_eq!(loaded.meta.index, 200);
    assert_eq!(loaded.meta.term, 10);
    assert_eq!(loaded.compaction_floor, 100);
}

// ============================================================================
// Compaction tests
// ============================================================================

use lattice::storage::compaction::{CompactionCoordinator, CompactionPolicy, WatchRevisionTracker};

#[test]
fn compaction_policy_default() {
    let policy = CompactionPolicy::default();
    assert!(policy.enabled);
    assert_eq!(policy.min_retain_revisions, 1000);
    assert_eq!(policy.min_retain_ms, 300_000);
}

#[test]
fn compaction_policy_propose_floor() {
    let mut policy = CompactionPolicy::default();
    policy.floor.clustor_floor = 1000;
    policy.floor.kv_floor = 0;

    // Propose compaction at revision 500 with current at 1500
    let proposed = policy.propose_floor(500, 1500);
    assert_eq!(proposed, Some(500));

    // Advance the floor
    assert!(policy.advance_floor(500));
    assert_eq!(policy.floor.kv_floor, 500);

    // Can't propose below current floor
    let proposed2 = policy.propose_floor(300, 1500);
    assert!(proposed2.is_none());
}

#[test]
fn compaction_policy_respects_min_retain() {
    let mut policy = CompactionPolicy::with_retention(100, 60_000);
    policy.floor.clustor_floor = 1000;

    // Current revision is 500, so min floor is 400
    // Proposing 200 takes the min of 200 and 400, so result is 200
    let proposed = policy.propose_floor(200, 500);
    assert_eq!(proposed, Some(200));

    // Proposing 500 should clamp to 400 (current 500 - retain 100)
    let proposed2 = policy.propose_floor(500, 500);
    assert_eq!(proposed2, Some(400));
}

#[test]
fn compaction_policy_disabled() {
    let mut policy = CompactionPolicy::default();
    policy.disable();

    assert!(!policy.can_compact(0));
    assert!(policy.propose_floor(500, 1000).is_none());
}

#[test]
fn watch_tracker_registration() {
    let tracker = WatchRevisionTracker::new();

    assert!(tracker.min_required_revision().is_none());
    assert_eq!(tracker.watch_count(), 0);

    tracker.register_watch(1, 100);
    assert_eq!(tracker.min_required_revision(), Some(100));
    assert_eq!(tracker.watch_count(), 1);

    tracker.register_watch(2, 50);
    assert_eq!(tracker.min_required_revision(), Some(50));
    assert_eq!(tracker.watch_count(), 2);
}

#[test]
fn watch_tracker_update() {
    let tracker = WatchRevisionTracker::new();
    tracker.register_watch(1, 100);

    // Update to higher revision
    tracker.update_watch(1, 200);
    assert_eq!(tracker.min_required_revision(), Some(200));

    // Update to lower revision should be ignored
    tracker.update_watch(1, 150);
    assert_eq!(tracker.min_required_revision(), Some(200));
}

#[test]
fn watch_tracker_unregister() {
    let tracker = WatchRevisionTracker::new();
    tracker.register_watch(1, 100);
    tracker.register_watch(2, 200);

    tracker.unregister_watch(1);
    assert_eq!(tracker.min_required_revision(), Some(200));
    assert_eq!(tracker.watch_count(), 1);

    tracker.unregister_watch(2);
    assert!(tracker.min_required_revision().is_none());
    assert_eq!(tracker.watch_count(), 0);
}

#[test]
fn compaction_coordinator_respects_watches() {
    let mut policy = CompactionPolicy::with_retention(50, 60_000);
    policy.floor.clustor_floor = 500;
    let coordinator = CompactionCoordinator::new(policy);
    coordinator.set_current_revision(1000);

    // Register a watch at revision 300
    let tracker = coordinator.watch_tracker();
    tracker.register_watch(1, 300);

    // Try to compact to 400 - effective target is min(400, 300-1) = 299
    // With min_retain of 50, min_floor is 950, so proposed is min(299, 950) = 299
    // But clamped to clustor_floor (500), so clamped = min(299, 500) = 299
    // Since 299 > 0 (current floor), it should succeed
    let result = coordinator.try_compact(400, 1000);
    assert!(result.is_some());
    assert!(result.unwrap() < 300); // Should be 299

    // Unregister watch and try again with higher target
    tracker.unregister_watch(1);
    let result2 = coordinator.try_compact(400, 2000);
    assert!(result2.is_some());
}

#[test]
fn compaction_coordinator_stats() {
    let coordinator = CompactionCoordinator::default();
    coordinator.set_current_revision(500);
    coordinator.update_clustor_floor(1000);

    let tracker = coordinator.watch_tracker();
    tracker.register_watch(1, 100);

    let stats = coordinator.stats();
    assert_eq!(stats.current_revision, 500);
    assert_eq!(stats.clustor_floor, 1000);
    assert_eq!(stats.active_watches, 1);
    assert_eq!(stats.min_watch_revision, Some(100));
    assert!(stats.enabled);
}
