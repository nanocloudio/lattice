//! Tests for the KPG (Key-Value Partition Group) runtime.

use lattice::control::cache::CacheState;
use lattice::control::capabilities::TtlBounds;
use lattice::core::error::LinearizabilityFailureReason;
use lattice::core::time::Tick;
use lattice::kpg::apply_loop::{
    ApplyLoop, CompactEntry, Compare, CompareEvaluator, CompareResult, CompareTarget, DeleteEntry,
    LeaseGrantEntry, LeaseRevokeEntry, PutEntry, TickEntry, TxnEntry, TxnExecutor, TxnOp, WalEntry,
};
use lattice::kpg::idempotency::{IdempotencyCheck, IdempotencyTable, OutcomeDigest};
use lattice::kpg::lease::LeaseManager;
use lattice::kpg::lin_bound::{
    can_linearize, can_snapshot_read, DurabilityMode, FollowerReadDecision, FollowerReadRouter,
    GatedOperation, KpgRole, LinBoundGate, ReadSemantics, SessionMonotonicity, SnapshotReadPath,
    StrictFallbackCoupling,
};
use lattice::kpg::revision::{
    CompactionFloor, ReadIndexFence, RevisionAvailability, RevisionFence, RevisionRange,
    RevisionTracker,
};
use lattice::kpg::state_machine::{EventType, KvEvent, KvRecord, KvStateMachine};
use lattice::kpg::ttl::{
    ExpirationProcessor, ExpirationQueue, LeaseDeadlineQueue, TickEmitter, TickEmitterConfig,
    TtlBoundsViolation, TtlEnforcer, TtlExceedsBehavior, TtlManager,
};
use lattice::kpg::watch::{
    WatchCreateRequest, WatchFilters, WatchManager, WatchSemantics, WatchState,
};

// ============================================================================
// State Machine Tests
// ============================================================================

#[test]
fn state_machine_new() {
    let sm = KvStateMachine::new();
    assert_eq!(sm.current_revision(), 0);
    assert_eq!(sm.compaction_floor(), 0);
    assert!(sm.get(b"foo").is_none());
}

#[test]
fn state_machine_put_and_get() {
    let mut sm = KvStateMachine::new();

    let prev = sm.put(b"key1".to_vec(), b"value1".to_vec(), 1, None);
    assert!(prev.is_none());
    assert_eq!(sm.current_revision(), 1);

    let record = sm.get(b"key1").unwrap();
    assert_eq!(record.value, b"value1");
    assert_eq!(record.create_revision, 1);
    assert_eq!(record.mod_revision, 1);
    assert_eq!(record.version, 1);
}

#[test]
fn state_machine_put_update() {
    let mut sm = KvStateMachine::new();

    sm.put(b"key1".to_vec(), b"value1".to_vec(), 1, None);
    let prev = sm.put(b"key1".to_vec(), b"value2".to_vec(), 2, None);

    assert!(prev.is_some());
    assert_eq!(prev.unwrap().value, b"value1");

    let record = sm.get(b"key1").unwrap();
    assert_eq!(record.value, b"value2");
    assert_eq!(record.create_revision, 1); // create_revision preserved
    assert_eq!(record.mod_revision, 2);
    assert_eq!(record.version, 2);
}

#[test]
fn state_machine_delete() {
    let mut sm = KvStateMachine::new();

    sm.put(b"key1".to_vec(), b"value1".to_vec(), 1, None);
    let deleted = sm.delete(b"key1", 2);

    assert!(deleted.is_some());
    assert_eq!(deleted.unwrap().value, b"value1");
    assert!(sm.get(b"key1").is_none());
    assert!(!sm.contains(b"key1"));
}

#[test]
fn state_machine_delete_nonexistent() {
    let mut sm = KvStateMachine::new();
    let deleted = sm.delete(b"key1", 1);
    assert!(deleted.is_none());
}

#[test]
fn state_machine_range() {
    let mut sm = KvStateMachine::new();

    sm.put(b"a".to_vec(), b"1".to_vec(), 1, None);
    sm.put(b"b".to_vec(), b"2".to_vec(), 2, None);
    sm.put(b"c".to_vec(), b"3".to_vec(), 3, None);
    sm.put(b"d".to_vec(), b"4".to_vec(), 4, None);

    // Range from b to d (exclusive)
    let results = sm.range(b"b", Some(b"d"));
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].key, b"b");
    assert_eq!(results[1].key, b"c");
}

#[test]
fn state_machine_range_limit() {
    let mut sm = KvStateMachine::new();

    sm.put(b"a".to_vec(), b"1".to_vec(), 1, None);
    sm.put(b"b".to_vec(), b"2".to_vec(), 2, None);
    sm.put(b"c".to_vec(), b"3".to_vec(), 3, None);

    let results = sm.range_limit(b"a", None, 2);
    assert_eq!(results.len(), 2);
}

#[test]
fn state_machine_delete_range() {
    let mut sm = KvStateMachine::new();

    sm.put(b"a".to_vec(), b"1".to_vec(), 1, None);
    sm.put(b"b".to_vec(), b"2".to_vec(), 2, None);
    sm.put(b"c".to_vec(), b"3".to_vec(), 3, None);
    sm.put(b"d".to_vec(), b"4".to_vec(), 4, None);

    let deleted = sm.delete_range(b"b", Some(b"d"), 5);
    assert_eq!(deleted.len(), 2);
    assert!(sm.contains(b"a"));
    assert!(!sm.contains(b"b"));
    assert!(!sm.contains(b"c"));
    assert!(sm.contains(b"d"));
}

#[test]
fn state_machine_events() {
    let mut sm = KvStateMachine::new();

    sm.put(b"key1".to_vec(), b"value1".to_vec(), 1, None);
    sm.put(b"key1".to_vec(), b"value2".to_vec(), 2, None);
    sm.delete(b"key1", 3);

    let events = sm.take_events();
    assert_eq!(events.len(), 3);

    assert_eq!(events[0].event_type, EventType::Put);
    assert!(events[0].prev_kv.is_none());

    assert_eq!(events[1].event_type, EventType::Put);
    assert!(events[1].prev_kv.is_some());

    assert_eq!(events[2].event_type, EventType::Delete);
}

#[test]
fn state_machine_compact() {
    let mut sm = KvStateMachine::new();

    sm.put(b"key1".to_vec(), b"v1".to_vec(), 1, None);
    sm.put(b"key1".to_vec(), b"v2".to_vec(), 2, None);
    sm.put(b"key1".to_vec(), b"v3".to_vec(), 3, None);

    let removed = sm.compact(2);
    assert_eq!(removed, 1); // Removed revision 1
    assert_eq!(sm.compaction_floor(), 2);
}

#[test]
fn state_machine_stats() {
    let mut sm = KvStateMachine::new();

    sm.put(b"key1".to_vec(), b"v1".to_vec(), 1, None);
    sm.put(b"key2".to_vec(), b"v2".to_vec(), 2, None);
    sm.delete(b"key1", 3);

    let stats = sm.stats();
    assert_eq!(stats.current_revision, 3);
    assert_eq!(stats.live_key_count, 1);
    assert_eq!(stats.index_size, 2); // Including tombstone
}

// ============================================================================
// Snapshot Tests
// ============================================================================

#[test]
fn snapshot_create_and_restore() {
    let mut sm = KvStateMachine::new();

    sm.put(b"key1".to_vec(), b"value1".to_vec(), 1, None);
    sm.put(b"key2".to_vec(), b"value2".to_vec(), 2, None);
    sm.take_events(); // Clear events

    let snapshot = sm.snapshot();
    assert_eq!(snapshot.revision, 2);
    assert_eq!(snapshot.record_count(), 2);

    let restored = KvStateMachine::restore_from_snapshot(&snapshot);
    assert_eq!(restored.current_revision(), 2);
    assert_eq!(restored.get(b"key1").unwrap().value, b"value1");
    assert_eq!(restored.get(b"key2").unwrap().value, b"value2");
}

#[test]
fn snapshot_serialization() {
    let mut sm = KvStateMachine::new();

    sm.put(b"key1".to_vec(), b"value1".to_vec(), 1, None);
    sm.put(b"key2".to_vec(), b"value2".to_vec(), 2, None);

    let bytes = sm.serialize().unwrap();
    let restored = KvStateMachine::deserialize(&bytes).unwrap();

    assert_eq!(restored.current_revision(), 2);
    assert_eq!(restored.get(b"key1").unwrap().value, b"value1");
}

// ============================================================================
// Revision Tests
// ============================================================================

#[test]
fn revision_fence_basic() {
    let fence = RevisionFence::new(10);
    assert_eq!(fence.get(), 10);
    assert!(fence.is_at_or_after(5));
    assert!(fence.is_at_or_after(10));
    assert!(!fence.is_at_or_after(15));
}

#[test]
fn compaction_floor_advance() {
    let mut floor = CompactionFloor::new(0, 100);
    assert!(!floor.is_compacted(5));

    assert!(floor.advance_to(50));
    assert!(floor.is_compacted(5));
    assert!(floor.is_compacted(49));
    assert!(!floor.is_compacted(50));

    // Can't advance beyond clustor floor
    assert!(!floor.advance_to(150));
}

#[test]
fn revision_tracker_basic() {
    let mut tracker = RevisionTracker::new();
    assert_eq!(tracker.committed(), 0);

    tracker.advance(10);
    assert_eq!(tracker.committed(), 10);
    assert_eq!(tracker.fence().get(), 10);
}

#[test]
fn revision_tracker_availability() {
    let mut tracker = RevisionTracker::new();
    tracker.advance(100);
    tracker.advance_kv_floor(50);

    assert_eq!(tracker.is_available(25), RevisionAvailability::Compacted);
    assert_eq!(tracker.is_available(75), RevisionAvailability::Available);
    assert_eq!(
        tracker.is_available(150),
        RevisionAvailability::NotYetCommitted
    );
}

#[test]
fn revision_tracker_pending_fences() {
    let mut tracker = RevisionTracker::new();
    tracker.advance(10);

    tracker.add_pending_fence(15, 1);
    tracker.add_pending_fence(20, 2);
    tracker.add_pending_fence(5, 3); // Already satisfied

    let satisfied = tracker.take_satisfied_fences();
    assert_eq!(satisfied.len(), 1);
    assert_eq!(satisfied[0].request_id, 3);

    tracker.advance(20);
    let satisfied = tracker.take_satisfied_fences();
    assert_eq!(satisfied.len(), 2);
}

#[test]
fn read_index_fence_satisfaction() {
    let mut fence = ReadIndexFence::new(RevisionFence::new(100), 42);

    assert!(!fence.check_satisfied(50));
    assert!(!fence.satisfied);

    assert!(fence.check_satisfied(100));
    assert!(fence.satisfied);
}

#[test]
fn revision_range_contains() {
    let range = RevisionRange::between(10, 20);
    assert!(!range.contains(5));
    assert!(range.contains(10));
    assert!(range.contains(15));
    assert!(!range.contains(20));

    let open_range = RevisionRange::from(10);
    assert!(open_range.contains(100));
}

// ============================================================================
// Lease Manager Tests
// ============================================================================

#[test]
fn lease_manager_grant() {
    let mut mgr = LeaseManager::new();
    let tick = Tick::new(1000);

    let id = mgr.grant(0, 5000, tick); // Auto-generate ID
    assert_eq!(id, 1);
    assert!(mgr.exists(1));
    assert_eq!(mgr.lease_count(), 1);
}

#[test]
fn lease_manager_grant_with_id() {
    let mut mgr = LeaseManager::new();
    let tick = Tick::new(1000);

    let id = mgr.grant(42, 5000, tick);
    assert_eq!(id, 42);
    assert!(mgr.exists(42));
}

#[test]
fn lease_manager_revoke() {
    let mut mgr = LeaseManager::new();
    let tick = Tick::new(1000);

    mgr.grant(1, 5000, tick);
    let revoked = mgr.revoke(1);

    assert!(revoked.is_some());
    assert!(!mgr.exists(1));
}

#[test]
fn lease_manager_keepalive() {
    let mut mgr = LeaseManager::new();
    let tick1 = Tick::new(1000);
    let tick2 = Tick::new(3000);

    mgr.grant(1, 5000, tick1);

    // Keepalive extends the deadline
    assert!(mgr.keepalive(1, tick2));

    let lease = mgr.get(1).unwrap();
    assert_eq!(lease.keepalive_deadline.ms, 8000); // 3000 + 5000
}

#[test]
fn lease_manager_attach_key() {
    let mut mgr = LeaseManager::new();
    let tick = Tick::new(1000);

    mgr.grant(1, 5000, tick);
    mgr.attach_key(1, b"key1".to_vec());
    mgr.attach_key(1, b"key2".to_vec());

    let lease = mgr.get(1).unwrap();
    assert_eq!(lease.attached_key_count(), 2);
}

#[test]
fn lease_manager_collect_expired() {
    let mut mgr = LeaseManager::new();
    let tick1 = Tick::new(1000);

    mgr.grant(1, 5000, tick1); // Expires at 6000
    mgr.grant(2, 10000, tick1); // Expires at 11000

    let tick_after = Tick::new(7000);
    let expired = mgr.collect_expired(tick_after);

    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0], 1);
}

// ============================================================================
// Watch Manager Tests
// ============================================================================

#[test]
fn watch_manager_create() {
    let mut mgr = WatchManager::new();

    let request = WatchCreateRequest {
        key: b"key".to_vec(),
        range_end: vec![],
        start_revision: 0,
        progress_notify: true,
        filters: WatchFilters::empty(),
        prev_kv: false,
        watch_id: 0,
        fragment: false,
    };

    let (id, success) = mgr.create(request, 100, WatchSemantics::Linearizable);
    assert!(success);
    assert_eq!(id, 1);
    assert_eq!(mgr.watch_count(), 1);

    let responses = mgr.take_responses();
    assert_eq!(responses.len(), 1);
    assert!(responses[0].created);
}

#[test]
fn watch_manager_cancel() {
    let mut mgr = WatchManager::new();

    let request = WatchCreateRequest {
        key: b"key".to_vec(),
        range_end: vec![],
        start_revision: 0,
        progress_notify: false,
        filters: WatchFilters::empty(),
        prev_kv: false,
        watch_id: 0,
        fragment: false,
    };

    let (id, _) = mgr.create(request, 100, WatchSemantics::Linearizable);
    mgr.take_responses(); // Clear create response

    assert!(mgr.cancel(id, Some("test".to_string())));
    assert_eq!(mgr.watch_count(), 0);

    let responses = mgr.take_responses();
    assert_eq!(responses.len(), 1);
    assert!(responses[0].canceled);
}

#[test]
fn watch_manager_process_events() {
    let mut mgr = WatchManager::new();

    // Create watch for "key"
    let request = WatchCreateRequest {
        key: b"key".to_vec(),
        range_end: vec![],
        start_revision: 0,
        progress_notify: false,
        filters: WatchFilters::empty(),
        prev_kv: false,
        watch_id: 0,
        fragment: false,
    };
    mgr.create(request, 100, WatchSemantics::Linearizable);
    mgr.take_responses();

    // Process matching event
    let record = KvRecord::new(b"key".to_vec(), b"value".to_vec(), 101, 101);
    let event = KvEvent::put(record, None);
    mgr.process_events(&[event], 101);

    let responses = mgr.take_responses();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].events.len(), 1);
}

#[test]
fn watch_manager_filter_events() {
    let mut mgr = WatchManager::new();

    // Create watch with NOPUT filter
    let request = WatchCreateRequest {
        key: b"key".to_vec(),
        range_end: vec![],
        start_revision: 0,
        progress_notify: false,
        filters: WatchFilters::NOPUT,
        prev_kv: false,
        watch_id: 0,
        fragment: false,
    };
    mgr.create(request, 100, WatchSemantics::Linearizable);
    mgr.take_responses();

    // PUT event should be filtered
    let record = KvRecord::new(b"key".to_vec(), b"value".to_vec(), 101, 101);
    let event = KvEvent::put(record, None);
    mgr.process_events(&[event], 101);

    let responses = mgr.take_responses();
    assert!(responses.is_empty());
}

#[test]
fn watch_state_matches_key() {
    let state = WatchState::new(1, b"foo".to_vec(), vec![], 1);
    assert!(state.matches_key(b"foo"));
    assert!(!state.matches_key(b"bar"));

    // Prefix watch (range_end = 0x00)
    let prefix_state = WatchState::new(2, b"foo".to_vec(), vec![0], 1);
    assert!(prefix_state.matches_key(b"foo"));
    assert!(prefix_state.matches_key(b"foobar"));
    assert!(!prefix_state.matches_key(b"bar"));

    // Range watch
    let range_state = WatchState::new(3, b"a".to_vec(), b"d".to_vec(), 1);
    assert!(range_state.matches_key(b"a"));
    assert!(range_state.matches_key(b"b"));
    assert!(range_state.matches_key(b"c"));
    assert!(!range_state.matches_key(b"d"));
}

// ============================================================================
// Idempotency Table Tests
// ============================================================================

#[test]
fn idempotency_table_insert_and_check() {
    let mut table = IdempotencyTable::new();

    let digest = OutcomeDigest::success(100, 12345);
    table.insert(b"key1".to_vec(), 100, digest.clone(), 1000);

    assert!(table.contains(b"key1"));
    assert_eq!(table.len(), 1);

    let check = table.check(b"key1", &digest);
    assert!(matches!(check, IdempotencyCheck::Match(_)));
}

#[test]
fn idempotency_table_conflict() {
    let mut table = IdempotencyTable::new();

    let digest1 = OutcomeDigest::success(100, 12345);
    let digest2 = OutcomeDigest::success(100, 99999);

    table.insert(b"key1".to_vec(), 100, digest1, 1000);

    let check = table.check(b"key1", &digest2);
    assert!(matches!(check, IdempotencyCheck::Conflict { .. }));
}

#[test]
fn idempotency_table_expired() {
    let mut table = IdempotencyTable::with_ttl(1000);

    let digest = OutcomeDigest::success(100, 12345);
    table.insert(b"key1".to_vec(), 100, digest, 1000);

    // Before expiry
    let expired = table.collect_expired(1500);
    assert!(expired.is_empty());

    // After expiry
    let expired = table.collect_expired(3000);
    assert_eq!(expired.len(), 1);
}

#[test]
fn idempotency_table_evict() {
    let mut table = IdempotencyTable::new();

    let digest = OutcomeDigest::success(100, 12345);
    table.insert(b"key1".to_vec(), 100, digest.clone(), 1000);
    table.insert(b"key2".to_vec(), 101, digest, 1000);

    let evicted = table.evict(&[b"key1".to_vec()]);
    assert_eq!(evicted, 1);
    assert!(!table.contains(b"key1"));
    assert!(table.contains(b"key2"));
}

// ============================================================================
// Compare Evaluator Tests
// ============================================================================

#[test]
fn compare_evaluator_mod_revision() {
    let mut sm = KvStateMachine::new();
    sm.put(b"key".to_vec(), b"value".to_vec(), 10, None);
    sm.take_events();

    let compare = Compare {
        key: b"key".to_vec(),
        target: CompareTarget::ModRevision(10),
        result: CompareResult::Equal,
    };

    assert!(CompareEvaluator::evaluate(&compare, &sm));

    let compare_ne = Compare {
        key: b"key".to_vec(),
        target: CompareTarget::ModRevision(5),
        result: CompareResult::Greater,
    };

    assert!(CompareEvaluator::evaluate(&compare_ne, &sm));
}

#[test]
fn compare_evaluator_value() {
    let mut sm = KvStateMachine::new();
    sm.put(b"key".to_vec(), b"hello".to_vec(), 10, None);
    sm.take_events();

    let compare = Compare {
        key: b"key".to_vec(),
        target: CompareTarget::Value(b"hello".to_vec()),
        result: CompareResult::Equal,
    };

    assert!(CompareEvaluator::evaluate(&compare, &sm));
}

#[test]
fn compare_evaluator_nonexistent_key() {
    let sm = KvStateMachine::new();

    // Non-existent key should have mod_revision = 0
    let compare = Compare {
        key: b"missing".to_vec(),
        target: CompareTarget::ModRevision(0),
        result: CompareResult::Equal,
    };

    assert!(CompareEvaluator::evaluate(&compare, &sm));
}

// ============================================================================
// Transaction Executor Tests
// ============================================================================

#[test]
fn txn_executor_success_branch() {
    let mut sm = KvStateMachine::new();
    sm.put(b"key".to_vec(), b"value".to_vec(), 1, None);
    sm.take_events();

    let txn = TxnEntry {
        compares: vec![Compare {
            key: b"key".to_vec(),
            target: CompareTarget::Version(1),
            result: CompareResult::Equal,
        }],
        success_ops: vec![TxnOp::Put(PutEntry {
            key: b"key".to_vec(),
            value: b"new_value".to_vec(),
            lease_id: None,
            prev_kv: false,
            idempotency_key: None,
        })],
        failure_ops: vec![],
        idempotency_key: None,
    };

    let (result, success) = TxnExecutor::execute(&txn, &mut sm, 2);

    assert!(success);
    assert!(result.success);
    assert_eq!(sm.get(b"key").unwrap().value, b"new_value");
}

#[test]
fn txn_executor_failure_branch() {
    let mut sm = KvStateMachine::new();
    sm.put(b"key".to_vec(), b"value".to_vec(), 1, None);
    sm.take_events();

    let txn = TxnEntry {
        compares: vec![Compare {
            key: b"key".to_vec(),
            target: CompareTarget::Version(999), // Will fail
            result: CompareResult::Equal,
        }],
        success_ops: vec![TxnOp::Put(PutEntry {
            key: b"key".to_vec(),
            value: b"success_value".to_vec(),
            lease_id: None,
            prev_kv: false,
            idempotency_key: None,
        })],
        failure_ops: vec![TxnOp::Put(PutEntry {
            key: b"key".to_vec(),
            value: b"failure_value".to_vec(),
            lease_id: None,
            prev_kv: false,
            idempotency_key: None,
        })],
        idempotency_key: None,
    };

    let (result, success) = TxnExecutor::execute(&txn, &mut sm, 2);

    assert!(!success);
    assert!(result.success); // Result is still "successful" in terms of execution
    assert_eq!(sm.get(b"key").unwrap().value, b"failure_value");
}

// ============================================================================
// Apply Loop Tests
// ============================================================================

#[test]
fn apply_loop_put() {
    let mut apply = ApplyLoop::new();

    let entry = WalEntry::Put(PutEntry {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        lease_id: None,
        prev_kv: false,
        idempotency_key: None,
    });

    let result = apply.apply(entry, 1);
    assert!(result.success);
    assert_eq!(result.revision, 1);
    assert_eq!(result.affected_count, 1);

    assert_eq!(apply.state_machine().get(b"key").unwrap().value, b"value");
}

#[test]
fn apply_loop_delete() {
    let mut apply = ApplyLoop::new();

    // First put
    let put = WalEntry::Put(PutEntry {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        lease_id: None,
        prev_kv: false,
        idempotency_key: None,
    });
    apply.apply(put, 1);

    // Then delete
    let delete = WalEntry::Delete(DeleteEntry {
        key: b"key".to_vec(),
        range_end: None,
        prev_kv: true,
        idempotency_key: None,
    });
    let result = apply.apply(delete, 2);

    assert!(result.success);
    assert_eq!(result.affected_count, 1);
    assert_eq!(result.prev_kvs.len(), 1);
    assert!(apply.state_machine().get(b"key").is_none());
}

#[test]
fn apply_loop_lease_grant_and_revoke() {
    let mut apply = ApplyLoop::new();

    // Grant lease
    let tick = WalEntry::Tick(TickEntry { tick_ms: 1000 });
    apply.apply(tick, 1);

    let grant = WalEntry::LeaseGrant(LeaseGrantEntry {
        lease_id: 1,
        ttl_ms: 5000,
    });
    apply.apply(grant, 2);

    assert!(apply.lease_manager().exists(1));

    // Put with lease
    let put = WalEntry::Put(PutEntry {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        lease_id: Some(1),
        prev_kv: false,
        idempotency_key: None,
    });
    apply.apply(put, 3);

    // Revoke lease (should delete attached keys)
    let revoke = WalEntry::LeaseRevoke(LeaseRevokeEntry { lease_id: 1 });
    let result = apply.apply(revoke, 4);

    assert!(!apply.lease_manager().exists(1));
    assert!(apply.state_machine().get(b"key").is_none());
    assert_eq!(result.affected_count, 1);
}

#[test]
fn apply_loop_compact() {
    let mut apply = ApplyLoop::new();

    // Put some keys
    for i in 1..=5 {
        let put = WalEntry::Put(PutEntry {
            key: format!("key{}", i).into_bytes(),
            value: b"value".to_vec(),
            lease_id: None,
            prev_kv: false,
            idempotency_key: None,
        });
        apply.apply(put, i);
    }

    // Compact
    let compact = WalEntry::Compact(CompactEntry { revision: 3 });
    let result = apply.apply(compact, 6);

    assert!(result.success);
    assert_eq!(apply.state_machine().compaction_floor(), 3);
}

#[test]
fn apply_loop_stats() {
    let mut apply = ApplyLoop::new();

    let put = WalEntry::Put(PutEntry {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        lease_id: None,
        prev_kv: false,
        idempotency_key: None,
    });
    apply.apply(put, 1);

    let stats = apply.stats();
    assert_eq!(stats.last_applied, 1);
    assert_eq!(stats.state_machine_stats.live_key_count, 1);
}

// ============================================================================
// LIN-BOUND Predicate Tests (Tasks 63, 64)
// ============================================================================

#[test]
fn can_linearize_leader_fresh() {
    let result = can_linearize(KpgRole::Leader, true, CacheState::Fresh);
    assert!(result.available);
    assert!(result.failure_reason.is_none());
    assert_eq!(result.role, KpgRole::Leader);
}

#[test]
fn can_linearize_leader_cached() {
    let result = can_linearize(KpgRole::Leader, true, CacheState::Cached);
    assert!(result.available);
}

#[test]
fn can_linearize_leader_stale() {
    let result = can_linearize(KpgRole::Leader, true, CacheState::Stale);
    assert!(!result.available);
    assert_eq!(
        result.failure_reason,
        Some(LinearizabilityFailureReason::StrictFallback)
    );
}

#[test]
fn can_linearize_leader_expired() {
    let result = can_linearize(KpgRole::Leader, true, CacheState::Expired);
    assert!(!result.available);
    assert_eq!(
        result.failure_reason,
        Some(LinearizabilityFailureReason::StrictFallback)
    );
}

#[test]
fn can_linearize_follower() {
    let result = can_linearize(KpgRole::Follower, true, CacheState::Fresh);
    assert!(!result.available);
    assert_eq!(
        result.failure_reason,
        Some(LinearizabilityFailureReason::NotLeader)
    );
}

#[test]
fn can_linearize_read_gate_fails() {
    let result = can_linearize(KpgRole::Leader, false, CacheState::Fresh);
    assert!(!result.available);
    assert_eq!(
        result.failure_reason,
        Some(LinearizabilityFailureReason::ProofMismatch)
    );
}

#[test]
fn can_snapshot_read_leader() {
    let result = can_snapshot_read(KpgRole::Leader, CacheState::Fresh, false);
    assert!(result.available);
}

#[test]
fn can_snapshot_read_follower_configured() {
    let result = can_snapshot_read(KpgRole::Follower, CacheState::Fresh, true);
    assert!(result.available);
    assert!(result.follower_reads_allowed);
}

#[test]
fn can_snapshot_read_follower_not_configured() {
    let result = can_snapshot_read(KpgRole::Follower, CacheState::Fresh, false);
    assert!(!result.available);
    assert!(!result.follower_reads_allowed);
}

#[test]
fn can_snapshot_read_follower_stale_cache() {
    let result = can_snapshot_read(KpgRole::Follower, CacheState::Stale, true);
    assert!(!result.available);
}

// ============================================================================
// LIN-BOUND Gate Tests (Tasks 65, 66, 67)
// ============================================================================

#[test]
fn lin_bound_gate_linearizable_read_pass() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Leader);
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Fresh);

    assert!(gate.gate_linearizable_read().is_ok());
    assert_eq!(gate.stats().checks(GatedOperation::LinearizableRead), 1);
    assert_eq!(gate.stats().passes(GatedOperation::LinearizableRead), 1);
}

#[test]
fn lin_bound_gate_linearizable_read_fail() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Follower);
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Fresh);

    let result = gate.gate_linearizable_read();
    assert!(result.is_err());
    assert_eq!(gate.stats().checks(GatedOperation::LinearizableRead), 1);
    assert_eq!(gate.stats().passes(GatedOperation::LinearizableRead), 0);
}

#[test]
fn lin_bound_gate_compare_pass() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Leader);
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Fresh);

    assert!(gate.gate_compare().is_ok());
    assert_eq!(gate.stats().checks(GatedOperation::Compare), 1);
}

#[test]
fn lin_bound_gate_compare_fail_stale() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Leader);
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Stale);

    let result = gate.gate_compare();
    assert!(result.is_err());
}

#[test]
fn lin_bound_gate_txn_fence_pass() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Leader);
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Cached);

    assert!(gate.gate_transaction_fence().is_ok());
    assert_eq!(gate.stats().checks(GatedOperation::TransactionFence), 1);
}

#[test]
fn lin_bound_gate_txn_fence_fail() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Leader);
    gate.set_read_gate(false); // Read gate fails
    gate.override_cache_state(CacheState::Fresh);

    let result = gate.gate_transaction_fence();
    assert!(result.is_err());
}

#[test]
fn lin_bound_gate_watch_start_pass() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Leader);
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Fresh);

    assert!(gate.gate_watch_start().is_ok());
    assert_eq!(gate.stats().checks(GatedOperation::WatchStart), 1);
}

// ============================================================================
// Snapshot Read Path Tests (Task 68)
// ============================================================================

#[test]
fn snapshot_read_path_basic() {
    let mut path = SnapshotReadPath::default();
    assert!(path.is_enabled());

    path.update_revision(100);
    assert_eq!(path.snapshot_revision(), 100);

    path.record_read("etcd");
    assert_eq!(path.stats().total_reads(), 1);
    assert_eq!(path.stats().adapter_reads("etcd"), 1);
}

#[test]
fn snapshot_read_stats_by_adapter() {
    let path = SnapshotReadPath::default();

    path.record_read("etcd");
    path.record_read("etcd");
    path.record_read("redis");

    assert_eq!(path.stats().total_reads(), 3);
    assert_eq!(path.stats().adapter_reads("etcd"), 2);
    assert_eq!(path.stats().adapter_reads("redis"), 1);
    assert_eq!(path.stats().adapter_reads("memcached"), 0);
}

// ============================================================================
// Strict Fallback Coupling Tests (Task 69)
// ============================================================================

#[test]
fn strict_fallback_coupling_activate() {
    let mut coupling = StrictFallbackCoupling::new();
    assert!(!coupling.is_active());
    assert_eq!(coupling.durability_mode(), DurabilityMode::Normal);

    coupling.activate();
    assert!(coupling.is_active());
    assert_eq!(coupling.durability_mode(), DurabilityMode::Strict);
    assert!(coupling.should_fail_lin_bound());
}

#[test]
fn strict_fallback_coupling_deactivate() {
    let mut coupling = StrictFallbackCoupling::new();
    coupling.activate();
    coupling.deactivate();

    assert!(!coupling.is_active());
    assert_eq!(coupling.durability_mode(), DurabilityMode::Normal);
}

#[test]
fn strict_fallback_coupling_from_cache() {
    let mut coupling = StrictFallbackCoupling::new();

    coupling.update_from_cache(CacheState::Fresh);
    assert!(!coupling.is_active());

    coupling.update_from_cache(CacheState::Stale);
    assert!(coupling.is_active());

    coupling.update_from_cache(CacheState::Cached);
    assert!(!coupling.is_active());
}

// ============================================================================
// Follower Read Router Tests (Task 70)
// ============================================================================

#[test]
fn follower_read_router_leader() {
    let mut router = FollowerReadRouter::new(true);
    router.set_role(KpgRole::Leader);
    router.set_cache_state(CacheState::Fresh);

    assert_eq!(
        router.route_read(ReadSemantics::Linearizable),
        FollowerReadDecision::ServeLocal
    );
    assert_eq!(
        router.route_read(ReadSemantics::SnapshotOnly),
        FollowerReadDecision::ServeLocal
    );
}

#[test]
fn follower_read_router_follower_linearizable() {
    let mut router = FollowerReadRouter::new(true);
    router.set_role(KpgRole::Follower);
    router.set_cache_state(CacheState::Fresh);

    assert_eq!(
        router.route_read(ReadSemantics::Linearizable),
        FollowerReadDecision::RedirectToLeader
    );
    assert!(router.should_redirect(ReadSemantics::Linearizable));
}

#[test]
fn follower_read_router_follower_snapshot_allowed() {
    let mut router = FollowerReadRouter::new(true);
    router.set_role(KpgRole::Follower);
    router.set_cache_state(CacheState::Fresh);

    assert_eq!(
        router.route_read(ReadSemantics::SnapshotOnly),
        FollowerReadDecision::ServeLocal
    );
}

#[test]
fn follower_read_router_follower_snapshot_not_configured() {
    let mut router = FollowerReadRouter::new(false);
    router.set_role(KpgRole::Follower);
    router.set_cache_state(CacheState::Fresh);

    assert_eq!(
        router.route_read(ReadSemantics::SnapshotOnly),
        FollowerReadDecision::RedirectToLeader
    );
}

#[test]
fn follower_read_router_unknown_role() {
    let router = FollowerReadRouter::new(true);
    // Default role is Unknown

    assert_eq!(
        router.route_read(ReadSemantics::SnapshotOnly),
        FollowerReadDecision::Fail
    );
}

// ============================================================================
// Session Monotonicity Tests (Task 71)
// ============================================================================

#[test]
fn session_monotonicity_record_and_check() {
    let mut mono = SessionMonotonicity::new(100);

    mono.record(1, 100);
    assert_eq!(mono.last_revision(1), Some(100));
    assert!(mono.satisfies_monotonicity(1, 100));
    assert!(mono.satisfies_monotonicity(1, 150));
    assert!(!mono.satisfies_monotonicity(1, 50));
}

#[test]
fn session_monotonicity_unknown_session() {
    let mono = SessionMonotonicity::new(100);

    assert!(mono.last_revision(999).is_none());
    assert!(mono.satisfies_monotonicity(999, 0)); // Unknown sessions always satisfy
}

#[test]
fn session_monotonicity_revision_only_increases() {
    let mut mono = SessionMonotonicity::new(100);

    mono.record(1, 100);
    mono.record(1, 50); // Should not decrease
    assert_eq!(mono.last_revision(1), Some(100));

    mono.record(1, 150);
    assert_eq!(mono.last_revision(1), Some(150));
}

#[test]
fn session_monotonicity_remove_session() {
    let mut mono = SessionMonotonicity::new(100);

    mono.record(1, 100);
    mono.remove_session(1);
    assert!(mono.last_revision(1).is_none());
}

#[test]
fn session_monotonicity_eviction() {
    let mut mono = SessionMonotonicity::new(2); // Max 2 sessions

    mono.record(1, 100);
    mono.record(2, 200);
    assert_eq!(mono.session_count(), 2);

    mono.record(3, 300); // Should evict oldest
    assert_eq!(mono.session_count(), 2);
}

// ============================================================================
// Telemetry Tests (Task 72)
// ============================================================================

#[test]
fn lin_bound_stats_tracking() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Leader);
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Fresh);

    // Multiple operations
    gate.gate_linearizable_read().unwrap();
    gate.gate_linearizable_read().unwrap();
    gate.gate_compare().unwrap();
    gate.gate_transaction_fence().unwrap();

    let stats = gate.stats();
    assert_eq!(stats.checks(GatedOperation::LinearizableRead), 2);
    assert_eq!(stats.passes(GatedOperation::LinearizableRead), 2);
    assert_eq!(stats.checks(GatedOperation::Compare), 1);
    assert_eq!(stats.passes(GatedOperation::Compare), 1);
    assert_eq!(stats.checks(GatedOperation::TransactionFence), 1);
}

#[test]
fn lin_bound_stats_failure_reasons() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Follower); // Will fail with NotLeader
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Fresh);

    let _ = gate.gate_linearizable_read();
    let _ = gate.gate_compare();

    let stats = gate.stats();
    assert_eq!(
        stats.failures_by_reason(LinearizabilityFailureReason::NotLeader),
        2
    );
    assert_eq!(stats.total_failures(), 2);
}

#[test]
fn lin_bound_stats_can_linearize_rate() {
    let mut gate = LinBoundGate::new();
    gate.set_role(KpgRole::Leader);
    gate.set_read_gate(true);
    gate.override_cache_state(CacheState::Fresh);

    // All pass
    gate.gate_linearizable_read().unwrap();
    gate.gate_linearizable_read().unwrap();

    assert_eq!(gate.stats().can_linearize_rate(), 1.0);

    // Now fail some
    gate.set_read_gate(false);
    let _ = gate.gate_linearizable_read();
    let _ = gate.gate_linearizable_read();

    // 2 passes, 2 fails = 50%
    assert_eq!(gate.stats().can_linearize_rate(), 0.5);
}

#[test]
fn kpg_role_display() {
    assert_eq!(format!("{}", KpgRole::Leader), "leader");
    assert_eq!(format!("{}", KpgRole::Follower), "follower");
    assert_eq!(format!("{}", KpgRole::Learner), "learner");
    assert_eq!(format!("{}", KpgRole::Unknown), "unknown");
}

#[test]
fn gated_operation_display() {
    assert_eq!(
        format!("{}", GatedOperation::LinearizableRead),
        "linearizable_read"
    );
    assert_eq!(format!("{}", GatedOperation::Compare), "compare");
    assert_eq!(
        format!("{}", GatedOperation::TransactionFence),
        "transaction_fence"
    );
    assert_eq!(format!("{}", GatedOperation::WatchStart), "watch_start");
}

// ============================================================================
// Tick Emitter Tests (Task 74)
// ============================================================================

#[test]
fn tick_emitter_default() {
    let emitter = TickEmitter::default();
    assert!(!emitter.is_leader());
    assert!(emitter.last_emitted().is_none());
    assert_eq!(emitter.period_ms(), 1000);
}

#[test]
fn tick_emitter_leader_only() {
    let mut emitter = TickEmitter::default();

    // Should not emit when not leader
    assert!(emitter.should_emit().is_none());

    // Should emit when leader
    emitter.set_leader(true);
    assert!(emitter.should_emit().is_some());
}

#[test]
fn tick_emitter_record_emitted() {
    let mut emitter = TickEmitter::default();
    emitter.set_leader(true);

    let tick_ms = 1000;
    emitter.record_emitted(tick_ms);

    assert_eq!(emitter.last_emitted(), Some(Tick::new(tick_ms)));
    assert_eq!(emitter.ticks_emitted(), 1);
}

#[test]
fn tick_emitter_stats() {
    let mut emitter = TickEmitter::new(TickEmitterConfig {
        period_ms: 500,
        enabled: true,
    });
    emitter.set_leader(true);
    emitter.record_emitted(1000);

    let stats = emitter.stats();
    assert!(stats.is_leader);
    assert!(stats.enabled);
    assert_eq!(stats.period_ms, 500);
    assert_eq!(stats.last_emitted_ms, Some(1000));
    assert_eq!(stats.ticks_emitted, 1);
}

// ============================================================================
// Expiration Queue Tests (Task 78)
// ============================================================================

#[test]
fn expiration_queue_add_and_collect() {
    let mut queue = ExpirationQueue::new();

    queue.add(b"key1".to_vec(), 100, 1);
    queue.add(b"key2".to_vec(), 200, 2);
    queue.add(b"key3".to_vec(), 150, 3);

    assert_eq!(queue.depth(), 3);

    // Collect at tick 100 - only key1 should expire
    let expired = queue.collect_expired(100);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0], b"key1".to_vec());
    assert_eq!(queue.depth(), 2);

    // Collect at tick 200 - key2 and key3 should expire
    let expired = queue.collect_expired(200);
    assert_eq!(expired.len(), 2);
    assert_eq!(queue.depth(), 0);
}

#[test]
fn expiration_queue_remove() {
    let mut queue = ExpirationQueue::new();

    queue.add(b"key1".to_vec(), 100, 1);
    queue.add(b"key2".to_vec(), 200, 2);

    assert!(queue.remove(b"key1"));
    assert_eq!(queue.depth(), 1);

    // key1 should not be collected even at tick 100
    let expired = queue.collect_expired(100);
    assert!(expired.is_empty());
}

#[test]
fn expiration_queue_update_key() {
    let mut queue = ExpirationQueue::new();

    queue.add(b"key1".to_vec(), 100, 1);
    // Update key1 to expire later - this removes old entry first
    queue.add(b"key1".to_vec(), 300, 2);

    assert_eq!(queue.depth(), 1);

    // Collect at tick 100 - stale entry in heap should be skipped since key was removed
    let expired = queue.collect_expired(100);
    // The old entry (100ms) was removed when we added the new one, so nothing expires
    assert!(expired.is_empty());

    // Should expire at tick 300
    let expired = queue.collect_expired(300);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0], b"key1".to_vec());
}

#[test]
fn expiration_queue_peek_next_expiry() {
    let mut queue = ExpirationQueue::new();

    assert!(queue.peek_next_expiry().is_none());

    queue.add(b"key1".to_vec(), 200, 1);
    queue.add(b"key2".to_vec(), 100, 2);
    queue.add(b"key3".to_vec(), 300, 3);

    assert_eq!(queue.peek_next_expiry(), Some(100));
}

// ============================================================================
// TTL Enforcer Tests (Task 80)
// ============================================================================

#[test]
fn ttl_enforcer_default_bounds() {
    let enforcer = TtlEnforcer::new();
    let bounds = enforcer.bounds();

    assert_eq!(bounds.min_ms, 0);
    assert_eq!(bounds.max_ms, 600_000); // 10 minutes
    assert_eq!(bounds.default_ms, 60_000); // 1 minute
}

#[test]
fn ttl_enforcer_within_bounds() {
    let enforcer = TtlEnforcer::new();

    // Within bounds
    assert_eq!(enforcer.enforce(30_000).unwrap(), 30_000);
    assert_eq!(enforcer.enforce(0).unwrap(), 0);
    assert_eq!(enforcer.enforce(600_000).unwrap(), 600_000);
}

#[test]
fn ttl_enforcer_clamp_above_max() {
    let enforcer = TtlEnforcer::with_bounds(TtlBounds {
        min_ms: 1000,
        max_ms: 60_000,
        default_ms: 10_000,
    });

    // Above max should be clamped
    assert_eq!(enforcer.enforce(100_000).unwrap(), 60_000);
}

#[test]
fn ttl_enforcer_clamp_below_min() {
    let enforcer = TtlEnforcer::with_bounds(TtlBounds {
        min_ms: 1000,
        max_ms: 60_000,
        default_ms: 10_000,
    });

    // Below min should be clamped
    assert_eq!(enforcer.enforce(500).unwrap(), 1000);
}

#[test]
fn ttl_enforcer_reject_behavior() {
    let enforcer = TtlEnforcer::with_behavior(
        TtlBounds {
            min_ms: 1000,
            max_ms: 60_000,
            default_ms: 10_000,
        },
        TtlExceedsBehavior::Reject,
    );

    // Above max should be rejected
    let result = enforcer.enforce(100_000);
    assert!(matches!(
        result,
        Err(TtlBoundsViolation::AboveMaximum { .. })
    ));

    // Below min should be rejected
    let result = enforcer.enforce(500);
    assert!(matches!(
        result,
        Err(TtlBoundsViolation::BelowMinimum { .. })
    ));
}

#[test]
fn ttl_enforcer_default_ttl() {
    let enforcer = TtlEnforcer::with_bounds(TtlBounds {
        min_ms: 1000,
        max_ms: 60_000,
        default_ms: 10_000,
    });

    assert_eq!(enforcer.default_ttl(), 10_000);
    assert_eq!(enforcer.with_default(None), 10_000);
    assert_eq!(enforcer.with_default(Some(5000)), 5000);
}

#[test]
fn ttl_bounds_violation_display() {
    let below = TtlBoundsViolation::BelowMinimum {
        requested: 500,
        minimum: 1000,
    };
    assert_eq!(format!("{}", below), "TTL 500ms is below minimum 1000ms");

    let above = TtlBoundsViolation::AboveMaximum {
        requested: 100_000,
        maximum: 60_000,
    };
    assert_eq!(
        format!("{}", above),
        "TTL 100000ms is above maximum 60000ms"
    );
}

// ============================================================================
// Expiration Processor Tests (Tasks 75, 76, 77)
// ============================================================================

#[test]
fn expiration_processor_advance_tick() {
    let mut processor = ExpirationProcessor::new();

    assert_eq!(processor.current_tick(), Tick::zero());

    processor.advance_tick(Tick::new(1000));
    assert_eq!(processor.current_tick(), Tick::new(1000));
    assert_eq!(processor.stats().ticks_processed, 1);
}

#[test]
fn expiration_processor_schedule_and_collect() {
    let mut processor = ExpirationProcessor::new();

    processor.schedule_expiry(b"key1".to_vec(), 100, 1);
    processor.schedule_expiry(b"key2".to_vec(), 200, 2);

    assert_eq!(processor.queue_depth(), 2);

    processor.advance_tick(Tick::new(150));
    let expired = processor.collect_expired_keys();

    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0], b"key1".to_vec());
}

#[test]
fn expiration_processor_mutation_triggered() {
    let processor = ExpirationProcessor::new();

    // Not expired
    assert!(!processor.is_expired_at_access(Some(Tick::new(1000))));

    // Expired
    let mut processor = ExpirationProcessor::new();
    processor.advance_tick(Tick::new(1000));
    assert!(processor.is_expired_at_access(Some(Tick::new(500))));

    // No expiry set
    assert!(!processor.is_expired_at_access(None));
}

#[test]
fn expiration_processor_cancel_expiry() {
    let mut processor = ExpirationProcessor::new();

    processor.schedule_expiry(b"key1".to_vec(), 100, 1);
    assert_eq!(processor.queue_depth(), 1);

    processor.cancel_expiry(b"key1");
    assert_eq!(processor.queue_depth(), 0);
}

// ============================================================================
// Lease Deadline Queue Tests (Task 79)
// ============================================================================

#[test]
fn lease_deadline_queue_upsert() {
    let mut queue = LeaseDeadlineQueue::new();

    queue.upsert(1, 100);
    queue.upsert(2, 200);

    assert_eq!(queue.depth(), 2);

    // Update deadline for lease 1
    queue.upsert(1, 300);
    assert_eq!(queue.depth(), 2);
}

#[test]
fn lease_deadline_queue_collect_expired() {
    let mut queue = LeaseDeadlineQueue::new();

    queue.upsert(1, 100);
    queue.upsert(2, 200);
    queue.upsert(3, 150);

    let expired = queue.collect_expired(150);
    assert_eq!(expired.len(), 2); // Lease 1 and 3

    let expired = queue.collect_expired(200);
    assert_eq!(expired.len(), 1); // Lease 2
}

#[test]
fn lease_deadline_queue_remove() {
    let mut queue = LeaseDeadlineQueue::new();

    queue.upsert(1, 100);
    queue.upsert(2, 200);

    assert!(queue.remove(1));
    assert_eq!(queue.depth(), 1);

    // Lease 1 should not be collected
    let expired = queue.collect_expired(100);
    assert!(expired.is_empty());
}

// ============================================================================
// TTL Manager Tests (Combined)
// ============================================================================

#[test]
fn ttl_manager_default() {
    let manager = TtlManager::new();
    let stats = manager.stats();

    assert!(!stats.tick_emitter.is_leader);
    assert_eq!(stats.key_expiry_queue_depth, 0);
    assert_eq!(stats.lease_deadline_queue_depth, 0);
}

#[test]
fn ttl_manager_process_tick() {
    let mut manager = TtlManager::new();

    // Schedule some expirations
    manager.schedule_key_expiry(b"key1".to_vec(), 100, 1);
    manager.schedule_key_expiry(b"key2".to_vec(), 200, 2);
    manager.schedule_lease_deadline(1, 150);

    // Process tick at 150ms
    let result = manager.process_tick(150);

    assert_eq!(result.expired_keys.len(), 1); // key1
    assert_eq!(result.expired_leases.len(), 1); // lease 1
}

#[test]
fn ttl_manager_set_leader() {
    let mut manager = TtlManager::new();

    assert!(manager.should_emit_tick().is_none());

    manager.set_leader(true);
    assert!(manager.should_emit_tick().is_some());
}

#[test]
fn ttl_manager_update_bounds() {
    let mut manager = TtlManager::new();

    manager.update_bounds(TtlBounds {
        min_ms: 5000,
        max_ms: 300_000,
        default_ms: 30_000,
    });

    // Enforce should use new bounds
    assert_eq!(manager.ttl_enforcer.enforce(1000).unwrap(), 5000); // Clamped to min
}
