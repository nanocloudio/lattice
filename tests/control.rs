//! Control plane tests.

mod common;

use lattice::control::cache::{CacheState, CachedValue};
use lattice::control::routing::{KpgId, RoutingTable};
use lattice::core::error::LatticeError;
use std::thread::sleep;
use std::time::Duration;

// ============================================================================
// Cache tests
// ============================================================================

#[test]
fn cache_state_fresh_allows_linearizable() {
    assert!(CacheState::Fresh.allows_linearizable());
    assert!(CacheState::Cached.allows_linearizable());
    assert!(!CacheState::Stale.allows_linearizable());
    assert!(!CacheState::Expired.allows_linearizable());
}

#[test]
fn cache_state_stale_forces_strict_fallback() {
    assert!(!CacheState::Fresh.forces_strict_fallback());
    assert!(!CacheState::Cached.forces_strict_fallback());
    assert!(CacheState::Stale.forces_strict_fallback());
    assert!(CacheState::Expired.forces_strict_fallback());
}

#[test]
fn cached_value_state_transitions() {
    // Short TTL for testing
    let value = CachedValue::new("test", 20, 20); // 20ms TTL, 20ms grace

    // Should start Fresh
    assert_eq!(value.state(), CacheState::Fresh);

    // After half TTL, should be Cached
    sleep(Duration::from_millis(15));
    assert_eq!(value.state(), CacheState::Cached);

    // After TTL, should be Stale (within grace)
    sleep(Duration::from_millis(15));
    assert_eq!(value.state(), CacheState::Stale);

    // After TTL + grace, should be Expired
    sleep(Duration::from_millis(25));
    assert_eq!(value.state(), CacheState::Expired);
}

#[test]
fn cached_value_update_resets_timestamp() {
    let mut value = CachedValue::new("old", 100, 100);
    sleep(Duration::from_millis(60));
    assert_eq!(value.state(), CacheState::Cached);

    value.update("new");
    assert_eq!(value.state(), CacheState::Fresh);
    assert_eq!(*value.get(), "new");
}

// ============================================================================
// Routing tests
// ============================================================================

#[test]
fn key_routing_deterministic() {
    let table = RoutingTable::new("tenant1".to_string(), 1, 12345, 8);

    let key = b"my-key";
    let kpg1 = table.route_key(key);
    let kpg2 = table.route_key(key);

    assert_eq!(kpg1, kpg2);
    assert!(kpg1 < 8);
}

#[test]
fn key_routing_distributes_across_kpgs() {
    let table = RoutingTable::new("tenant1".to_string(), 1, 12345, 16);

    let mut kpg_counts = vec![0u32; 16];
    for i in 0..1000 {
        let key = format!("key-{}", i);
        let kpg = table.route_key(key.as_bytes());
        kpg_counts[kpg as usize] += 1;
    }

    // All KPGs should have some keys (probabilistic, but very likely with 1000 keys)
    for count in &kpg_counts {
        assert!(*count > 0, "KPG should have at least one key");
    }
}

#[test]
fn routing_different_tenants_different_results() {
    let table1 = RoutingTable::new("tenant1".to_string(), 1, 12345, 8);
    let table2 = RoutingTable::new("tenant2".to_string(), 1, 12345, 8);

    // With different tenants and the same key, routing should differ for most keys
    let mut different_count = 0;
    for i in 0..100 {
        let key = format!("key-{}", i);
        let kpg1 = table1.route_key(key.as_bytes());
        let kpg2 = table2.route_key(key.as_bytes());
        if kpg1 != kpg2 {
            different_count += 1;
        }
    }

    // Most keys should route differently (probabilistic)
    assert!(
        different_count > 50,
        "Different tenants should route differently"
    );
}

#[test]
fn epoch_validation_success() {
    let table = RoutingTable::new("tenant1".to_string(), 5, 12345, 8);
    assert!(table.validate_epoch(5).is_ok());
}

#[test]
fn epoch_validation_failure() {
    let table = RoutingTable::new("tenant1".to_string(), 5, 12345, 8);
    let result = table.validate_epoch(3);

    assert!(result.is_err());
    match result.unwrap_err() {
        LatticeError::DirtyEpoch {
            expected_epoch,
            observed_epoch,
        } => {
            assert_eq!(expected_epoch, 5);
            assert_eq!(observed_epoch, 3);
        }
        _ => panic!("Expected DirtyEpoch error"),
    }
}

#[test]
fn single_key_range_routes_to_one_kpg() {
    let table = RoutingTable::new("tenant1".to_string(), 1, 12345, 8);
    let kpgs = table.route_range(b"my-key", None);

    assert_eq!(kpgs.len(), 1);
}

#[test]
fn prefix_range_routes_to_all_kpgs() {
    let table = RoutingTable::new("tenant1".to_string(), 1, 12345, 8);
    let kpgs = table.route_range(b"prefix", Some(&[])); // Empty end = prefix

    assert_eq!(kpgs.len(), 8);
}

#[test]
fn kpg_id_display() {
    let id = KpgId::new("tenant1", 3);
    assert_eq!(id.to_string(), "tenant1:3");
}

// ============================================================================
// API types tests
// ============================================================================

use lattice::control::api::{AdapterPolicy, QuotaConfig, TenantManifest, TokenBucket};
use lattice::control::capabilities::AdapterType;

#[test]
fn tenant_manifest_new() {
    let manifest = TenantManifest::new("test-tenant", 4);

    assert_eq!(manifest.tenant_id, "test-tenant");
    assert_eq!(manifest.kpg_count, 4);
    assert_eq!(manifest.routing_epoch, 1);
    assert!(manifest.is_adapter_enabled(AdapterType::Etcd));
    assert!(!manifest.is_adapter_enabled(AdapterType::Redis));
    assert!(manifest.active);
}

#[test]
fn tenant_manifest_adapter_toggle() {
    let mut manifest = TenantManifest::new("test-tenant", 4);

    manifest.enable_adapter(AdapterType::Redis);
    assert!(manifest.is_adapter_enabled(AdapterType::Redis));

    manifest.disable_adapter(AdapterType::Etcd);
    assert!(!manifest.is_adapter_enabled(AdapterType::Etcd));
}

#[test]
fn tenant_manifest_epoch_increment() {
    let mut manifest = TenantManifest::new("test-tenant", 4);
    assert_eq!(manifest.routing_epoch, 1);

    manifest.increment_routing_epoch();
    assert_eq!(manifest.routing_epoch, 2);

    manifest.increment_routing_epoch();
    assert_eq!(manifest.routing_epoch, 3);
}

#[test]
fn adapter_policy_command_allowlist() {
    let mut policy = AdapterPolicy::default();

    // Empty allowlist means all allowed
    assert!(policy.is_command_allowed("PUT"));
    assert!(policy.is_command_allowed("GET"));

    // Add to allowlist
    policy.command_allowlist.insert("GET".to_string());
    assert!(policy.is_command_allowed("GET"));
    assert!(!policy.is_command_allowed("PUT"));

    // Denylist overrides allowlist
    policy.command_denylist.insert("GET".to_string());
    assert!(!policy.is_command_allowed("GET"));
}

#[test]
fn adapter_policy_size_validation() {
    let policy = AdapterPolicy::default();

    assert!(policy.is_key_size_valid(100));
    assert!(policy.is_key_size_valid(8192));
    assert!(!policy.is_key_size_valid(10000));

    assert!(policy.is_value_size_valid(1000));
    assert!(policy.is_value_size_valid(1_048_576));
    assert!(!policy.is_value_size_valid(2_000_000));
}

#[test]
fn quota_config_default() {
    let quota = QuotaConfig::default();

    assert!(quota.enabled);
    assert_eq!(quota.requests_per_second, 10_000);
    assert_eq!(quota.bytes_per_second, 100_000_000);
}

#[test]
fn quota_config_unlimited() {
    let quota = QuotaConfig::unlimited();

    assert!(!quota.enabled);
    assert_eq!(quota.requests_per_second, u64::MAX);
}

#[test]
fn token_bucket_basic() {
    let mut bucket = TokenBucket::new(100, 200);

    // Should start full
    assert!(bucket.try_consume(100));

    // Should have 100 left
    assert!(bucket.try_consume(100));

    // Should be empty
    assert!(!bucket.try_consume(1));
}

#[test]
fn token_bucket_refill() {
    let mut bucket = TokenBucket::new(1000, 1000); // 1000/sec

    // Drain it
    bucket.tokens = 0;

    // Wait a bit for refill
    std::thread::sleep(Duration::from_millis(50));
    bucket.refill();

    // Should have some tokens (at least 40, allowing for timing variance)
    assert!(bucket.tokens >= 40);
}

// ============================================================================
// Cache agent tests
// ============================================================================

use lattice::control::cache::CpCacheAgent;

#[test]
fn cache_agent_manifest_storage() {
    let agent = CpCacheAgent::default();
    let manifest = TenantManifest::new("test-tenant", 4);

    agent.update_manifest(manifest);

    let retrieved = agent.get_manifest("test-tenant");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().kpg_count, 4);
}

#[test]
fn cache_agent_routing_auto_update() {
    let agent = CpCacheAgent::default();
    let manifest = TenantManifest::new("test-tenant", 4);

    agent.update_manifest(manifest);

    // Routing should be auto-populated from manifest
    let routing = agent.get_routing("test-tenant");
    assert!(routing.is_some());
    let routing = routing.unwrap();
    assert_eq!(routing.tenant_id, "test-tenant");
    assert_eq!(routing.kpg_count, 4);
}

#[test]
fn cache_agent_lin_bound_fresh() {
    let agent = CpCacheAgent::default();
    let manifest = TenantManifest::new("test-tenant", 4);

    agent.update_manifest(manifest);

    // Fresh cache should allow linearizable
    assert!(agent.allows_linearizable());
    assert!(agent.require_linearizable().is_ok());
}

#[test]
fn cache_agent_stats() {
    let agent = CpCacheAgent::default();
    agent.update_manifest(TenantManifest::new("tenant1", 4));
    agent.update_manifest(TenantManifest::new("tenant2", 2));

    let stats = agent.stats();
    assert_eq!(stats.tenant_count, 2);
    assert_eq!(stats.manifests_fresh, 2);
}

#[test]
fn cache_agent_remove_tenant() {
    let agent = CpCacheAgent::default();
    agent.update_manifest(TenantManifest::new("test-tenant", 4));

    assert!(agent.get_manifest("test-tenant").is_some());

    agent.remove_tenant("test-tenant");

    assert!(agent.get_manifest("test-tenant").is_none());
}

// ============================================================================
// Epoch migration tests
// ============================================================================

use lattice::control::routing::{EpochMigrationState, EpochValidation, RoutingEpochTracker};

#[test]
fn epoch_tracker_initial_state() {
    let tracker = RoutingEpochTracker::new("test-tenant", 1);

    assert_eq!(tracker.current_epoch(), 1);
    assert_eq!(tracker.state(), EpochMigrationState::Stable);
}

#[test]
fn epoch_tracker_migration() {
    let tracker = RoutingEpochTracker::new("test-tenant", 1);

    tracker.start_migration(2);

    assert_eq!(tracker.current_epoch(), 2);
    assert_eq!(tracker.state(), EpochMigrationState::Transitioning);

    // Should accept both epochs during transition
    assert!(matches!(tracker.validate(2), Ok(EpochValidation::Current)));
    assert!(matches!(
        tracker.validate(1),
        Ok(EpochValidation::PreviousDuringTransition)
    ));

    // Complete migration
    tracker.complete_migration();
    assert_eq!(tracker.state(), EpochMigrationState::Stable);

    // Should now reject old epoch
    assert!(tracker.validate(1).is_err());
}

#[test]
fn epoch_tracker_reject_stale() {
    let tracker = RoutingEpochTracker::new("test-tenant", 5);

    // Validate current epoch - should pass
    assert!(tracker.validate(5).is_ok());

    // Validate old epoch - should fail
    let result = tracker.validate(3);
    assert!(result.is_err());
}

#[test]
fn routing_validate_single_kpg() {
    let table = RoutingTable::new("tenant1".to_string(), 1, 12345, 8);

    // Single key - should succeed
    let keys: Vec<&[u8]> = vec![b"key1"];
    assert!(table.validate_single_kpg(&keys).is_ok());

    // Empty - should succeed
    let empty: Vec<&[u8]> = vec![];
    assert!(table.validate_single_kpg(&empty).is_ok());
}

#[test]
fn routing_cross_shard_detection() {
    let table = RoutingTable::new("tenant1".to_string(), 1, 12345, 8);

    // Find two keys that route to different KPGs
    let mut key1 = b"key1".to_vec();
    let mut key2 = b"key2".to_vec();

    // Generate keys until we find ones that route differently
    for i in 0..1000 {
        key1 = format!("key-a-{}", i).into_bytes();
        key2 = format!("key-b-{}", i).into_bytes();
        if table.route_key(&key1) != table.route_key(&key2) {
            break;
        }
    }

    if table.route_key(&key1) != table.route_key(&key2) {
        let keys: Vec<&[u8]> = vec![&key1, &key2];
        let result = table.validate_single_kpg(&keys);
        assert!(matches!(
            result,
            Err(LatticeError::TxnCrossShardUnsupported)
        ));
    }
}

// ============================================================================
// Capability registry tests (Task 36)
// ============================================================================

use lattice::control::capabilities::{CapabilityRegistry, FeatureGate, TenantCapabilities};

#[test]
fn tenant_capabilities_new() {
    let caps = TenantCapabilities::new("test-tenant");

    assert_eq!(caps.tenant_id, "test-tenant");
    assert!(caps.is_adapter_enabled(AdapterType::Etcd));
    assert!(!caps.is_adapter_enabled(AdapterType::Redis));
}

#[test]
fn tenant_capabilities_command_allowlist() {
    let mut caps = TenantCapabilities::new("test-tenant");

    // Empty allowlist means all allowed
    assert!(caps.is_command_allowed(AdapterType::Etcd, "PUT"));
    assert!(caps.is_command_allowed(AdapterType::Etcd, "GET"));

    // Set allowlist
    let mut allowlist = std::collections::HashSet::new();
    allowlist.insert("GET".to_string());
    caps.set_command_allowlist(AdapterType::Etcd, allowlist);

    assert!(caps.is_command_allowed(AdapterType::Etcd, "GET"));
    assert!(!caps.is_command_allowed(AdapterType::Etcd, "PUT"));
}

#[test]
fn tenant_capabilities_feature_gates() {
    let mut caps = TenantCapabilities::new("test-tenant");

    assert!(!caps.is_feature_enabled(FeatureGate::Resp3));

    caps.enable_feature(FeatureGate::Resp3);
    assert!(caps.is_feature_enabled(FeatureGate::Resp3));

    caps.disable_feature(FeatureGate::Resp3);
    assert!(!caps.is_feature_enabled(FeatureGate::Resp3));
}

#[test]
fn capability_registry_basic() {
    let registry = CapabilityRegistry::new();

    let caps = TenantCapabilities::new("tenant1");
    registry.update(caps);

    assert!(registry.is_adapter_enabled("tenant1", AdapterType::Etcd));
    assert!(!registry.is_adapter_enabled("tenant1", AdapterType::Redis));
    assert!(!registry.is_adapter_enabled("unknown", AdapterType::Etcd));
}

#[test]
fn capability_registry_remove() {
    let registry = CapabilityRegistry::new();

    registry.update(TenantCapabilities::new("tenant1"));
    assert_eq!(registry.tenant_count(), 1);

    registry.remove("tenant1");
    assert_eq!(registry.tenant_count(), 0);
}

// ============================================================================
// Placement manager tests (Tasks 37-38)
// ============================================================================

use lattice::control::placement::{
    MembershipChangeType, PendingMembershipChange, PlacementManager, PlacementRecord,
    PlacementTable, RebalancePhase,
};

#[test]
fn placement_record_checks() {
    let record = PlacementRecord {
        kpg_id: KpgId::new("tenant1", 0),
        leader: Some("node1".to_string()),
        voters: vec!["node1".to_string(), "node2".to_string()],
        learners: vec!["node3".to_string()],
        placement_epoch: 1,
    };

    assert!(record.is_leader("node1"));
    assert!(!record.is_leader("node2"));
    assert!(record.is_voter("node1"));
    assert!(record.is_voter("node2"));
    assert!(!record.is_voter("node3"));
    assert!(record.is_learner("node3"));
    assert!(record.involves_node("node1"));
    assert!(record.involves_node("node3"));
    assert!(!record.involves_node("node4"));
}

#[test]
fn placement_table_queries() {
    let mut table = PlacementTable::new("tenant1");

    table.update(
        0,
        PlacementRecord {
            kpg_id: KpgId::new("tenant1", 0),
            leader: Some("node1".to_string()),
            voters: vec!["node1".to_string()],
            learners: vec![],
            placement_epoch: 1,
        },
    );

    table.update(
        1,
        PlacementRecord {
            kpg_id: KpgId::new("tenant1", 1),
            leader: Some("node2".to_string()),
            voters: vec!["node1".to_string(), "node2".to_string()],
            learners: vec![],
            placement_epoch: 1,
        },
    );

    let node1_partitions = table.partitions_for_node("node1");
    assert_eq!(node1_partitions.len(), 2);

    let node1_led = table.partitions_led_by_node("node1");
    assert_eq!(node1_led.len(), 1);
    assert!(node1_led.contains(&0));
}

#[test]
fn placement_manager_local_queries() {
    let manager = PlacementManager::new("local-node");

    let mut table = PlacementTable::new("tenant1");
    table.update(
        0,
        PlacementRecord {
            kpg_id: KpgId::new("tenant1", 0),
            leader: Some("local-node".to_string()),
            voters: vec!["local-node".to_string()],
            learners: vec![],
            placement_epoch: 1,
        },
    );
    manager.update_placement(table);

    assert!(manager.is_local_leader("tenant1", 0));
    assert!(manager.is_local_voter("tenant1", 0));
    assert!(!manager.is_local_leader("tenant1", 1));

    let leader_kpgs = manager.local_leader_kpgs();
    assert_eq!(leader_kpgs.len(), 1);
}

#[test]
fn rebalance_phase_properties() {
    assert!(!RebalancePhase::Planning.blocks_writes());
    assert!(RebalancePhase::EpochFlip.blocks_writes());
    assert!(RebalancePhase::Planning.allows_new_connections());
    assert!(!RebalancePhase::EpochFlip.allows_new_connections());
}

#[test]
fn pending_membership_change_lifecycle() {
    let mut change =
        PendingMembershipChange::new(MembershipChangeType::AddVoter, "new-node", "tenant1", 0);

    assert_eq!(change.phase, RebalancePhase::Planning);
    assert!(!change.is_complete());

    change.advance_phase();
    assert_eq!(change.phase, RebalancePhase::LearnerCatchUp);

    change.advance_phase();
    change.advance_phase();
    change.advance_phase();
    assert!(change.is_complete());
}

#[test]
fn placement_manager_write_blocking() {
    let manager = PlacementManager::new("local-node");

    let mut change =
        PendingMembershipChange::new(MembershipChangeType::AddVoter, "new-node", "tenant1", 0);
    change.phase = RebalancePhase::EpochFlip;
    manager.add_pending_change(change);

    assert!(manager.is_write_blocked("tenant1", 0));
    assert!(!manager.is_write_blocked("tenant1", 1));
}

// ============================================================================
// Quota enforcement tests (Task 40)
// ============================================================================

use lattice::control::api::{QuotaEnforcer, QuotaResult};

#[test]
fn quota_enforcer_allows_within_budget() {
    let enforcer = QuotaEnforcer::new(1000);
    let quota = QuotaConfig::default();

    enforcer.init_tenant("tenant1", &quota);

    let result = enforcer.try_consume("tenant1", 1, 100);
    assert_eq!(result, QuotaResult::Allowed);
}

#[test]
fn quota_enforcer_unlimited_tenant() {
    let enforcer = QuotaEnforcer::new(1000);

    // Tenant without quota config = unlimited
    let result = enforcer.try_consume("unknown", 1, 100);
    assert_eq!(result, QuotaResult::Allowed);
}

#[test]
fn quota_result_checks() {
    assert!(QuotaResult::Allowed.is_allowed());
    assert!(QuotaResult::GracePeriod { remaining_ms: 500 }.is_allowed());
    assert!(!QuotaResult::Throttled.is_allowed());
    assert!(QuotaResult::Throttled.is_throttled());
}

// ============================================================================
// PKI and RBAC cache tests (Task 41)
// ============================================================================

use lattice::control::api::{PermissionOp, PkiRbacCache, RolePermission, TenantPki, TenantRole};

#[test]
fn pki_cache_mtls_auth() {
    let cache = PkiRbacCache::new();

    let mut pki = TenantPki::no_mtls("tenant1");
    pki.ca_chain_pem = Some("-----BEGIN CERTIFICATE-----...".to_string());
    pki.allowed_cns.insert("client.example.com".to_string());

    cache.update_pki(pki);

    assert!(cache.authenticate_mtls("tenant1", "client.example.com"));
    assert!(!cache.authenticate_mtls("tenant1", "other.example.com"));
    assert!(!cache.authenticate_mtls("unknown", "client.example.com"));
}

#[test]
fn rbac_role_permission_check() {
    let mut role = TenantRole::new("reader");
    role.add_permission(RolePermission::prefix(
        "/data/",
        [PermissionOp::Read, PermissionOp::Watch],
    ));

    assert!(role.has_permission(b"/data/foo", PermissionOp::Read));
    assert!(role.has_permission(b"/data/bar", PermissionOp::Watch));
    assert!(!role.has_permission(b"/data/foo", PermissionOp::Write));
    assert!(!role.has_permission(b"/other/foo", PermissionOp::Read));
}

#[test]
fn rbac_cache_user_permissions() {
    let cache = PkiRbacCache::new();

    let mut role = TenantRole::new("reader");
    role.add_permission(RolePermission::prefix("/data/", [PermissionOp::Read]));
    cache.update_role("tenant1", role);

    cache.bind_user("tenant1", "user1", vec!["reader".to_string()]);

    assert!(cache.has_permission("tenant1", "user1", b"/data/foo", PermissionOp::Read));
    assert!(!cache.has_permission("tenant1", "user1", b"/data/foo", PermissionOp::Write));
    assert!(!cache.has_permission("tenant1", "user2", b"/data/foo", PermissionOp::Read));
}

// ============================================================================
// Break-glass override tests (Task 42)
// ============================================================================

use lattice::control::api::{BreakGlassLedger, BreakGlassOverride, BreakGlassPolicy};

#[test]
fn break_glass_policy_default() {
    let policy = BreakGlassPolicy::default();

    assert!(!policy.enabled);
    assert_eq!(policy.required_approvals, 2);
    assert_eq!(policy.max_duration_seconds, 3600);
}

#[test]
fn break_glass_override_lifecycle() {
    let mut override_req = BreakGlassOverride::new("tenant1", "admin", "Emergency fix", 3600);

    assert!(!override_req.active);
    assert!(!override_req.has_enough_approvals(2));

    override_req.add_approval("approver1");
    assert!(!override_req.has_enough_approvals(2));

    override_req.add_approval("approver2");
    assert!(override_req.has_enough_approvals(2));

    assert!(override_req.try_activate(2));
    assert!(override_req.active);
    assert!(override_req.is_valid());

    override_req.revoke();
    assert!(!override_req.is_valid());
}

#[test]
fn break_glass_ledger_flow() {
    let ledger = BreakGlassLedger::new();

    // Set up policy
    let policy = BreakGlassPolicy {
        enabled: true,
        required_approvals: 2,
        ..Default::default()
    };
    ledger.set_policy("tenant1", policy);

    // Request override
    let override_req = BreakGlassOverride::new("tenant1", "admin", "Emergency fix", 3600);
    let override_id = override_req.override_id.clone();
    assert!(ledger.request_override(override_req));

    // Not yet active
    assert!(!ledger.has_active_override("tenant1"));

    // Approve
    ledger.approve_override("tenant1", &override_id, "approver1");
    assert!(!ledger.has_active_override("tenant1")); // Still needs more approvals

    ledger.approve_override("tenant1", &override_id, "approver2");
    assert!(ledger.has_active_override("tenant1")); // Now active

    // Audit log should have entries
    let log = ledger.get_audit_log(10);
    assert!(log.len() >= 3); // Requested, 2x Approved, Activated

    // Revoke
    ledger.revoke_all("tenant1", "admin");
    assert!(!ledger.has_active_override("tenant1"));
}

#[test]
fn break_glass_requires_enabled_policy() {
    let ledger = BreakGlassLedger::new();

    // No policy set - should fail
    let override_req = BreakGlassOverride::new("tenant1", "admin", "Emergency", 3600);
    assert!(!ledger.request_override(override_req));

    // Disabled policy - should fail
    ledger.set_policy("tenant1", BreakGlassPolicy::default()); // enabled: false
    let override_req2 = BreakGlassOverride::new("tenant1", "admin", "Emergency", 3600);
    assert!(!ledger.request_override(override_req2));
}
