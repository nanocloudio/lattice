//! Operations module integration tests.
//!
//! Tests for observability, telemetry, audit, DR, and version gates.

mod common;

use lattice::ops::audit::{
    AuditEvent, AuditEventType, AuditLog, AuditLogConfig, AuditResult, AuditSeverity,
};
use lattice::ops::dr::{
    DrCoordinator, DrState, ReplicationLagTracker, SnapshotExportRequest, SnapshotImportRequest,
    SnapshotMetadata,
};
use lattice::ops::observability::{
    HealthProbe, HealthStatus, LatticeMetrics, MetricsRegistry, ReadinessProbe, ReadinessStatus,
};
use lattice::ops::telemetry::{LogEntry, LogLevel, Span, TelemetryCollector, TelemetryConfig};
use lattice::ops::version::{
    CompatibilityCheck, FaultHook, FaultType, FeatureGate, VersionGateRegistry, VersionInfo,
};

// ============================================================================
// MetricsRegistry Tests
// ============================================================================

#[test]
fn metrics_registry_basic() {
    let registry = MetricsRegistry::new();

    // Test counter operations
    registry.counter_inc("requests_total");
    registry.counter_inc("requests_total");
    registry.counter_inc("requests_total");

    let value = registry.counter_get("requests_total");
    assert_eq!(value, 3);
}

#[test]
fn metrics_registry_gauges() {
    let registry = MetricsRegistry::new();

    registry.gauge_set("connections", 42);
    assert_eq!(registry.gauge_get("connections"), 42);

    registry.gauge_set("connections", 100);
    assert_eq!(registry.gauge_get("connections"), 100);
}

#[test]
fn metrics_registry_gauge_inc_dec() {
    let registry = MetricsRegistry::new();

    registry.gauge_set("active", 10);
    registry.gauge_inc("active");
    assert_eq!(registry.gauge_get("active"), 11);

    registry.gauge_dec("active");
    assert_eq!(registry.gauge_get("active"), 10);
}

#[test]
fn metrics_registry_histograms() {
    let registry = MetricsRegistry::new();

    registry.histogram_observe("latency_ms", 10.0);
    registry.histogram_observe("latency_ms", 20.0);
    registry.histogram_observe("latency_ms", 30.0);

    let snapshot = registry.histogram_get("latency_ms");
    assert!(snapshot.is_some());
    let snap = snapshot.unwrap();
    assert_eq!(snap.count, 3);
    assert_eq!(snap.sum, 60.0);
    assert_eq!(snap.min, 10.0);
    assert_eq!(snap.max, 30.0);
}

#[test]
fn metrics_registry_counter_add() {
    let registry = MetricsRegistry::new();

    registry.counter_add("bytes_total", 100);
    registry.counter_add("bytes_total", 50);

    assert_eq!(registry.counter_get("bytes_total"), 150);
}

#[test]
fn metrics_registry_prometheus_export() {
    let registry = MetricsRegistry::new();

    registry.counter_inc("test_counter");
    registry.gauge_set("test_gauge", 42);

    let output = registry.export_prometheus();
    assert!(output.contains("test_counter"));
    assert!(output.contains("test_gauge"));
}

// ============================================================================
// LatticeMetrics Tests
// ============================================================================

#[test]
fn lattice_metrics_basic() {
    let metrics = LatticeMetrics::new();

    metrics.record_adapter_request("etcd", "range");
    metrics.record_adapter_request("etcd", "put");

    // The counter should have recorded requests
    assert!(
        metrics
            .registry()
            .counter_get("lattice.adapter.etcd.requests_total.range")
            >= 1
    );
}

#[test]
fn lattice_metrics_kv() {
    let metrics = LatticeMetrics::new();

    metrics.set_kv_revision(100);
    metrics.set_compaction_floor(50);
    metrics.set_ttl_queue_depth(25);

    assert_eq!(metrics.registry().gauge_get("lattice.kv.revision"), 100);
    assert_eq!(
        metrics
            .registry()
            .gauge_get("lattice.kv.compaction_floor_revision"),
        50
    );
    assert_eq!(
        metrics
            .registry()
            .gauge_get("lattice.ttl.expiry_queue_depth"),
        25
    );
}

#[test]
fn lattice_metrics_cluster() {
    let metrics = LatticeMetrics::new();

    metrics.record_can_linearize(true);
    assert_eq!(
        metrics
            .registry()
            .gauge_get("lattice.lin_bound.can_linearize"),
        1
    );

    metrics.record_can_linearize(false);
    assert_eq!(
        metrics
            .registry()
            .gauge_get("lattice.lin_bound.can_linearize"),
        0
    );
}

#[test]
fn lattice_metrics_adapter_latency() {
    let metrics = LatticeMetrics::new();

    metrics.record_adapter_latency("etcd", "range", 5.0);
    metrics.record_adapter_latency("etcd", "range", 10.0);

    let snapshot = metrics
        .registry()
        .histogram_get("lattice.adapter.etcd.latency_ms.range");
    assert!(snapshot.is_some());
    assert_eq!(snapshot.unwrap().count, 2);
}

#[test]
fn lattice_metrics_quota() {
    let metrics = LatticeMetrics::new();

    metrics.record_quota_throttle("tenant1");
    metrics.record_quota_throttle("tenant1");

    assert!(
        metrics
            .registry()
            .counter_get("lattice.quota.throttle_total")
            >= 2
    );
}

// ============================================================================
// Health and Readiness Tests
// ============================================================================

#[test]
fn readiness_probe_basic() {
    let probe = ReadinessProbe::new();

    assert!(!probe.is_ready());

    probe.set_ready(true);
    assert!(probe.is_ready());

    probe.set_ready(false);
    assert!(!probe.is_ready());
}

#[test]
fn readiness_status_default() {
    let status = ReadinessStatus::default();
    assert!(!status.ready);
}

#[test]
fn readiness_probe_routing_cache() {
    let probe = ReadinessProbe::new();

    probe.set_routing_cache_age(5.5);
    assert_eq!(probe.status().routing_cache_age_seconds, 5.5);
}

#[test]
fn health_probe_basic() {
    let probe = HealthProbe::new();

    assert!(probe.is_healthy()); // Default to healthy

    probe.set_unhealthy("test error");
    assert!(!probe.is_healthy());
    assert_eq!(probe.status().message, "test error");

    probe.set_healthy();
    assert!(probe.is_healthy());
}

#[test]
fn health_status_constructors() {
    let healthy = HealthStatus::healthy();
    assert!(healthy.healthy);
    assert_eq!(healthy.message, "OK");

    let unhealthy = HealthStatus::unhealthy("service unavailable");
    assert!(!unhealthy.healthy);
    assert_eq!(unhealthy.message, "service unavailable");
}

// ============================================================================
// TelemetryCollector Tests
// ============================================================================

#[test]
fn telemetry_collector_basic() {
    let config = TelemetryConfig::default();
    let collector = TelemetryCollector::new(config);

    collector.info("test", "Test message");
    collector.error("test", "Error message");

    let stats = collector.stats();
    assert!(stats.log_counts.total() >= 2);
}

#[test]
fn telemetry_collector_spans() {
    let config = TelemetryConfig::default();
    let collector = TelemetryCollector::new(config);

    collector.record_span_start(true);
    collector.record_span_end();

    let stats = collector.stats();
    assert!(stats.spans_started >= 1);
    assert!(stats.spans_ended >= 1);
}

#[test]
fn telemetry_config_defaults() {
    let config = TelemetryConfig::default();
    assert_eq!(config.log_level, "info");
    assert!(!config.json_output);
}

#[test]
fn telemetry_config_production() {
    let config = TelemetryConfig::production();
    assert!(config.json_output);
    assert_eq!(config.trace_sample_rate, 0.1);
}

#[test]
fn telemetry_config_development() {
    let config = TelemetryConfig::development();
    assert!(!config.json_output);
    assert_eq!(config.trace_sample_rate, 1.0);
    assert!(config.include_source_location);
}

#[test]
fn log_entry_creation() {
    let entry = LogEntry::new(LogLevel::Info, "test.component", "Test message");

    assert_eq!(entry.level, "INFO");
    assert_eq!(entry.target, "test.component");
    assert_eq!(entry.message, "Test message");
}

#[test]
fn log_entry_with_fields() {
    let entry = LogEntry::new(LogLevel::Info, "test", "message")
        .with_field("key", "value")
        .with_field("number", 42);

    assert_eq!(entry.fields.len(), 2);
}

#[test]
fn log_entry_formats() {
    let entry = LogEntry::new(LogLevel::Info, "test", "test message");

    let json = entry.to_json();
    assert!(json.contains("INFO"));
    assert!(json.contains("test message"));

    let text = entry.to_text();
    assert!(text.contains("INFO"));
    assert!(text.contains("[test]"));
}

#[test]
fn span_basic() {
    let span = Span::new("test_operation");

    assert_eq!(span.name(), "test_operation");
    assert!(span.is_sampled());

    std::thread::sleep(std::time::Duration::from_millis(10));
    assert!(span.elapsed_ms() >= 10.0);
}

#[test]
fn span_attributes() {
    let mut span = Span::new("test");
    span.set_attribute("key", "value");

    // Span has attributes, verify it doesn't panic
    assert_eq!(span.name(), "test");
}

#[test]
fn span_child() {
    let parent = Span::new("parent");
    let child = parent.child("child");

    assert_eq!(child.name(), "child");
    assert_eq!(child.is_sampled(), parent.is_sampled());
}

#[test]
fn telemetry_level_filtering() {
    let config = TelemetryConfig {
        log_level: "warn".to_string(),
        ..Default::default()
    };
    let collector = TelemetryCollector::new(config);

    assert!(!collector.is_enabled(LogLevel::Debug));
    assert!(!collector.is_enabled(LogLevel::Info));
    assert!(collector.is_enabled(LogLevel::Warn));
    assert!(collector.is_enabled(LogLevel::Error));
}

// ============================================================================
// AuditLog Tests
// ============================================================================

#[test]
fn audit_log_events() {
    let config = AuditLogConfig::default();
    let log = AuditLog::new(config);

    let event =
        AuditEvent::new(AuditEventType::AuthSuccess, "user1", "login").with_tenant("tenant1");

    log.log(event);

    let events = log.get_recent(10);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, AuditEventType::AuthSuccess);
}

#[test]
fn audit_log_severity() {
    let config = AuditLogConfig {
        min_severity: AuditSeverity::Warning,
        ..Default::default()
    };
    let log = AuditLog::new(config);

    // Info event should be filtered
    log.log(AuditEvent::new(
        AuditEventType::AuthSuccess,
        "user1",
        "login",
    ));

    // Warning event should be recorded
    log.log(
        AuditEvent::new(AuditEventType::PermissionDenied, "user1", "access")
            .with_severity(AuditSeverity::Warning),
    );

    let events = log.get_recent(10);
    assert_eq!(events.len(), 1);
}

#[test]
fn audit_event_builder() {
    let event = AuditEvent::new(AuditEventType::BreakGlassActivated, "admin", "activate")
        .with_tenant("tenant1")
        .with_target("/data/secret")
        .with_source_ip("192.168.1.100")
        .with_severity(AuditSeverity::Critical);

    assert_eq!(event.event_type, AuditEventType::BreakGlassActivated);
    assert_eq!(event.tenant_id.as_deref(), Some("tenant1"));
    assert_eq!(event.target.as_deref(), Some("/data/secret"));
    assert_eq!(event.severity, AuditSeverity::Critical);
}

#[test]
fn audit_severity_ordering() {
    assert!(AuditSeverity::Critical > AuditSeverity::Error);
    assert!(AuditSeverity::Error > AuditSeverity::Warning);
    assert!(AuditSeverity::Warning > AuditSeverity::Info);
}

#[test]
fn audit_log_auth() {
    let log = AuditLog::default();

    log.log_auth("user1", true, None);
    log.log_auth("user2", false, Some("invalid password"));

    let events = log.get_recent(10);
    assert_eq!(events.len(), 2);

    let failures = log.get_by_type(AuditEventType::AuthFailure, 10);
    assert_eq!(failures.len(), 1);
}

#[test]
fn audit_log_permission_check() {
    let log = AuditLog::default();

    log.log_permission_check("user1", "/data/public", "read", true);
    log.log_permission_check("user1", "/data/secret", "write", false);

    let denied = log.get_by_type(AuditEventType::PermissionDenied, 10);
    assert_eq!(denied.len(), 1);
}

#[test]
fn audit_log_by_tenant() {
    let log = AuditLog::default();

    log.log(AuditEvent::new(AuditEventType::UserCreated, "admin", "create").with_tenant("tenant1"));
    log.log(AuditEvent::new(AuditEventType::UserCreated, "admin", "create").with_tenant("tenant2"));
    log.log(AuditEvent::new(AuditEventType::UserCreated, "admin", "create").with_tenant("tenant1"));

    let tenant1_events = log.get_by_tenant("tenant1", 10);
    assert_eq!(tenant1_events.len(), 2);
}

#[test]
fn audit_log_by_actor() {
    let log = AuditLog::default();

    log.log(AuditEvent::new(
        AuditEventType::AuthSuccess,
        "user1",
        "login",
    ));
    log.log(AuditEvent::new(
        AuditEventType::AuthSuccess,
        "user2",
        "login",
    ));
    log.log(AuditEvent::new(
        AuditEventType::ConfigChanged,
        "user1",
        "change",
    ));

    let user1_events = log.get_by_actor("user1", 10);
    assert_eq!(user1_events.len(), 2);
}

#[test]
fn audit_log_stats() {
    let log = AuditLog::default();

    log.log(AuditEvent::new(
        AuditEventType::AuthSuccess,
        "user1",
        "login",
    ));
    log.log(
        AuditEvent::new(AuditEventType::AuthFailure, "user2", "login")
            .with_severity(AuditSeverity::Warning),
    );
    log.log(
        AuditEvent::new(AuditEventType::BreakGlassActivated, "admin", "activate")
            .with_severity(AuditSeverity::Critical),
    );

    let stats = log.stats();
    assert_eq!(stats.total_events, 3);
}

#[test]
fn audit_log_buffer_overflow() {
    let config = AuditLogConfig {
        buffer_size: 2,
        ..Default::default()
    };
    let log = AuditLog::new(config);

    log.log(AuditEvent::new(
        AuditEventType::AuthSuccess,
        "user1",
        "login",
    ));
    log.log(AuditEvent::new(
        AuditEventType::AuthSuccess,
        "user2",
        "login",
    ));
    log.log(AuditEvent::new(
        AuditEventType::AuthSuccess,
        "user3",
        "login",
    ));

    assert_eq!(log.len(), 2);
    assert_eq!(log.stats().dropped_events, 1);
}

#[test]
fn audit_event_result() {
    let event = AuditEvent::new(AuditEventType::AuthAttempt, "user1", "login")
        .with_result(AuditResult::Success);
    assert_eq!(event.result, AuditResult::Success);

    let event = AuditEvent::new(AuditEventType::PermissionCheck, "user1", "read")
        .with_result(AuditResult::Denied);
    assert_eq!(event.result, AuditResult::Denied);
}

// ============================================================================
// DR Coordinator Tests
// ============================================================================

#[test]
fn dr_coordinator_state_transitions() {
    let coordinator = DrCoordinator::new();

    assert_eq!(coordinator.state(), DrState::Normal);

    coordinator.set_state(DrState::UnfencedDr);
    assert_eq!(coordinator.state(), DrState::UnfencedDr);
    assert!(!coordinator.allows_linearizable());

    coordinator.set_state(DrState::Normal);
    assert_eq!(coordinator.state(), DrState::Normal);
    assert!(coordinator.allows_linearizable());
}

#[test]
fn dr_coordinator_export() {
    let coordinator = DrCoordinator::new();

    let request = SnapshotExportRequest::new("/backup/snapshot.tar");
    let handle = coordinator.start_export(&request);
    assert!(!handle.snapshot_id.is_empty());

    let result = coordinator.complete_export(handle, true);
    assert!(result.success);
    assert_eq!(coordinator.statistics().exports_completed, 1);
}

#[test]
fn dr_coordinator_export_failure() {
    let coordinator = DrCoordinator::new();

    let request = SnapshotExportRequest::new("/backup/snapshot.tar");
    let handle = coordinator.start_export(&request);

    let result = coordinator.complete_export(handle, false);
    assert!(!result.success);
    assert_eq!(coordinator.statistics().exports_failed, 1);
}

#[test]
fn dr_coordinator_import_fenced() {
    let coordinator = DrCoordinator::new();

    let request = SnapshotImportRequest::new("/backup/snapshot.tar");
    let handle = coordinator.start_import(&request);

    assert_eq!(coordinator.state(), DrState::FencedPromotion);
    assert!(handle.fenced);

    let result = coordinator.complete_import(handle, true);
    assert!(result.success);
    assert_eq!(coordinator.state(), DrState::Recovered);
}

#[test]
fn dr_coordinator_import_unfenced() {
    let coordinator = DrCoordinator::new();

    let request = SnapshotImportRequest::unfenced("/backup/snapshot.tar");
    let handle = coordinator.start_import(&request);

    assert_eq!(coordinator.state(), DrState::UnfencedDr);
    assert!(!handle.fenced);

    let result = coordinator.complete_import(handle, true);
    assert!(result.success);
    assert_eq!(coordinator.state(), DrState::Recovered);
}

#[test]
fn dr_coordinator_finalize_recovery() {
    let coordinator = DrCoordinator::new();
    coordinator.set_state(DrState::Recovered);

    coordinator.finalize_recovery();
    assert_eq!(coordinator.state(), DrState::Normal);
}

#[test]
fn replication_lag_tracker_basic() {
    let tracker = ReplicationLagTracker::new();

    tracker.update_primary(1000);
    tracker.update_local(900);

    assert_eq!(tracker.lag(), 100);
    assert!(!tracker.is_caught_up());

    tracker.update_local(1000);
    assert_eq!(tracker.lag(), 0);
    assert!(tracker.is_caught_up());
}

#[test]
fn replication_lag_tracker_time() {
    let tracker = ReplicationLagTracker::new();
    assert!(tracker.time_since_update().is_none());

    tracker.update_primary(100);
    assert!(tracker.time_since_update().is_some());
}

#[test]
fn snapshot_export_request() {
    let req = SnapshotExportRequest::new("/backup/snap.tar")
        .with_tenants(vec!["tenant1".to_string()])
        .with_encryption(true);

    assert!(!req.is_full_export());
    assert!(req.encrypt);
}

#[test]
fn snapshot_import_request() {
    let req = SnapshotImportRequest::new("/backup/snap.tar");
    assert!(req.fenced);
    assert!(req.verify_checksum);

    let req2 = SnapshotImportRequest::unfenced("/backup/snap.tar");
    assert!(!req2.fenced);
}

#[test]
fn snapshot_metadata() {
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
fn dr_state_allows() {
    assert!(DrState::Normal.allows_linearizable());
    assert!(DrState::Normal.allows_writes());
    assert!(DrState::Normal.allows_reads());

    assert!(!DrState::UnfencedDr.allows_linearizable());
    assert!(!DrState::UnfencedDr.allows_writes());
    assert!(DrState::UnfencedDr.allows_reads());

    assert!(DrState::Recovered.allows_linearizable());
    assert!(DrState::Recovered.allows_writes());
}

#[test]
fn dr_statistics() {
    let coord = DrCoordinator::new();

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

// ============================================================================
// Version Gate Tests
// ============================================================================

#[test]
fn version_info_current() {
    let version = VersionInfo::current();

    assert_eq!(version.major, 0);
    assert_eq!(version.minor, 1);
    assert_eq!(version.patch, 0);
    assert_eq!(version.semver(), "0.1.0");
}

#[test]
fn version_info_compatibility() {
    let v1 = VersionInfo::new(0, 1, 0);
    let v2 = VersionInfo::new(0, 1, 5);
    let v3 = VersionInfo::new(0, 2, 0);
    let v4 = VersionInfo::new(1, 0, 0);

    // Same major, higher minor - compatible
    assert!(v2.is_compatible_with(&v1));

    // Same major, lower minor - not compatible
    assert!(!v1.is_compatible_with(&v3));

    // Different major - not compatible
    assert!(!v1.is_compatible_with(&v4));
}

#[test]
fn version_info_full_version() {
    let mut version = VersionInfo::new(0, 1, 0);
    version.git_commit = Some("abc1234".to_string());

    assert_eq!(version.full_version(), "0.1.0-abc1234");
}

#[test]
fn version_info_display() {
    let mut version = VersionInfo::new(1, 2, 3);
    version.git_commit = Some("abc".to_string());

    assert_eq!(format!("{}", version), "1.2.3-abc");
}

#[test]
fn feature_gate_basic() {
    let version = VersionInfo::new(0, 2, 0);

    let gate = FeatureGate::new("test_feature", true).with_min_version(0, 1);
    assert!(gate.passes(&version));

    let gate2 = FeatureGate::new("future_feature", true).with_min_version(0, 3);
    assert!(!gate2.passes(&version));

    let disabled_gate = FeatureGate::new("disabled_feature", false);
    assert!(!disabled_gate.passes(&version));
}

#[test]
fn feature_gate_description() {
    let gate = FeatureGate::new("test", true).with_description("A test feature");

    assert_eq!(gate.description, "A test feature");
}

#[test]
fn version_gate_registry_default_gates() {
    let registry = VersionGateRegistry::new();

    // etcd_adapter should be enabled by default
    assert!(registry.check("etcd_adapter"));

    // redis_adapter requires min_version 0.2, so should fail at 0.1
    assert!(!registry.check("redis_adapter"));

    // nonexistent gate should fail
    assert!(!registry.check("nonexistent"));
}

#[test]
fn version_gate_registry_enable_disable() {
    let registry = VersionGateRegistry::new();

    assert!(registry.check("etcd_adapter"));

    registry.disable("etcd_adapter");
    assert!(!registry.check("etcd_adapter"));

    registry.enable("etcd_adapter");
    assert!(registry.check("etcd_adapter"));
}

#[test]
fn version_gate_registry_custom_gate() {
    let registry = VersionGateRegistry::new();

    let custom_gate =
        FeatureGate::new("custom_feature", true).with_description("A custom feature for testing");

    registry.register(custom_gate);
    assert!(registry.check("custom_feature"));
}

#[test]
fn version_gate_registry_all_gates() {
    let registry = VersionGateRegistry::new();

    let gates = registry.all_gates();
    assert!(!gates.is_empty());
}

#[test]
fn version_gate_registry_version() {
    let registry = VersionGateRegistry::new();
    let version = registry.version();

    assert_eq!(version.major, 0);
    assert_eq!(version.minor, 1);
}

// ============================================================================
// Fault Injection Tests
// ============================================================================

#[test]
fn fault_hook_basic() {
    let hook = FaultHook::new("test_fault", FaultType::Error).enable();
    assert!(hook.should_trigger());

    let disabled_hook = FaultHook::new("disabled_fault", FaultType::Error);
    assert!(!disabled_hook.should_trigger());
}

#[test]
fn fault_hook_max_triggers() {
    let mut hook = FaultHook::new("limited_fault", FaultType::Error)
        .enable()
        .with_max_triggers(2);

    assert!(hook.should_trigger());
    hook.record_trigger();

    assert!(hook.should_trigger());
    hook.record_trigger();

    // Max triggers reached
    assert!(!hook.should_trigger());
}

#[test]
fn fault_hook_probability() {
    let hook = FaultHook::new("prob_fault", FaultType::Error)
        .enable()
        .with_probability(1.0);

    assert!(hook.should_trigger());

    let zero_prob = FaultHook::new("zero_fault", FaultType::Error)
        .enable()
        .with_probability(0.0);

    assert!(!zero_prob.should_trigger());
}

#[test]
fn fault_hook_registry() {
    let registry = VersionGateRegistry::new();

    let hook = FaultHook::new("test_fault", FaultType::Delay).enable();
    registry.register_fault_hook(hook);

    assert!(registry.should_inject_fault("test_fault"));
    assert!(!registry.should_inject_fault("nonexistent"));
}

#[test]
fn fault_hook_list_and_clear() {
    let registry = VersionGateRegistry::new();

    let hook1 = FaultHook::new("fault1", FaultType::Error).enable();
    let hook2 = FaultHook::new("fault2", FaultType::Delay).enable();

    registry.register_fault_hook(hook1);
    registry.register_fault_hook(hook2);

    assert_eq!(registry.list_fault_hooks().len(), 2);

    registry.clear_fault_hooks();
    assert_eq!(registry.list_fault_hooks().len(), 0);
}

#[test]
fn fault_type_display() {
    assert_eq!(format!("{}", FaultType::Delay), "delay");
    assert_eq!(format!("{}", FaultType::Error), "error");
    assert_eq!(format!("{}", FaultType::Panic), "panic");
    assert_eq!(format!("{}", FaultType::Drop), "drop");
    assert_eq!(format!("{}", FaultType::Corrupt), "corrupt");
    assert_eq!(format!("{}", FaultType::Timeout), "timeout");
}

// ============================================================================
// Compatibility Check Tests
// ============================================================================

#[test]
fn compatibility_check_compatible() {
    let local = VersionInfo::new(0, 2, 0);
    let remote = VersionInfo::new(0, 1, 0);

    let check = CompatibilityCheck::check(&local, &remote);
    assert!(check.compatible);
    assert!(check.reason.is_none());
}

#[test]
fn compatibility_check_incompatible() {
    let local = VersionInfo::new(0, 1, 0);
    let remote = VersionInfo::new(1, 0, 0);

    let check = CompatibilityCheck::check(&local, &remote);
    assert!(!check.compatible);
    assert!(check.reason.is_some());
}
