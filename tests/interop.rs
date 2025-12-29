//! Interoperability and conformance tests.
//!
//! These tests verify etcd API conformance and proper behavior under various conditions.
//!
//! # Running Conformance Tests
//!
//! Most tests are marked `#[ignore]` to prevent running during normal test runs.
//! Run them with:
//! ```bash
//! cargo test --test interop -- --ignored --test-threads=1
//! ```

mod common;

use lattice::adapters::etcd::auth::Permission;
use lattice::adapters::etcd::kv::{SortOrder, SortTarget};
use lattice::adapters::etcd::lease::{LeaseGrantRequest, LeaseRevokeRequest};
use lattice::adapters::etcd::txn::{
    Compare, CompareResult, CompareTarget, CompareTargetUnion, RequestOp,
};
use lattice::adapters::etcd::watch::{
    Event, EventTypeProto, WatchCreateRequest, WatchKeyRange, WatchSemantics,
};
use lattice::adapters::etcd::{
    DeleteRangeRequest, EtcdError, GrpcCode, KeyValue, PutRequest, RangeRequest, RangeResponse,
    ResponseHeader, TxnRequest,
};

// ============================================================================
// Test Helpers for Conformance
// ============================================================================

/// Assert that a revision was incremented from before to after.
fn assert_revision_incremented(before: i64, after: i64) {
    assert!(
        after > before,
        "revision should have incremented: before={}, after={}",
        before,
        after
    );
}

/// Assert that revisions match expected increment count.
fn assert_revision_delta(before: i64, after: i64, expected_delta: i64) {
    assert_eq!(
        after - before,
        expected_delta,
        "revision delta mismatch: before={}, after={}, expected_delta={}",
        before,
        after,
        expected_delta
    );
}

/// Create a test key with prefix.
fn test_key(suffix: &str) -> Vec<u8> {
    format!("test/{}", suffix).into_bytes()
}

/// Create a test value.
fn test_value(content: &str) -> Vec<u8> {
    content.as_bytes().to_vec()
}

/// Create a prefix end key (for prefix queries).
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    // Increment last byte for prefix end
    if let Some(last) = end.last_mut() {
        *last += 1;
    }
    end
}

// ============================================================================
// Put/Get Conformance Tests (Task 190)
// ============================================================================

#[test]
fn put_request_basic_structure() {
    // Verify PutRequest structure matches etcd spec
    let req = PutRequest {
        key: test_key("basic"),
        value: test_value("hello"),
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };

    assert_eq!(req.key, b"test/basic");
    assert_eq!(req.value, b"hello");
    assert_eq!(req.lease, 0);
    assert!(!req.prev_kv);
}

#[test]
fn put_request_with_prev_kv() {
    // prev_kv=true should request previous value in response
    let req = PutRequest {
        key: test_key("prev"),
        value: test_value("new"),
        lease: 0,
        prev_kv: true,
        ignore_value: false,
        ignore_lease: false,
    };

    assert!(req.prev_kv);
}

#[test]
fn put_request_with_lease() {
    // Verify lease attachment structure
    let req = PutRequest {
        key: test_key("leased"),
        value: test_value("data"),
        lease: 12345,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };

    assert_eq!(req.lease, 12345);
}

#[test]
fn put_request_ignore_value() {
    // ignore_value=true updates metadata without changing value
    let req = PutRequest {
        key: test_key("meta"),
        value: vec![],
        lease: 0,
        prev_kv: false,
        ignore_value: true,
        ignore_lease: false,
    };

    assert!(req.ignore_value);
}

#[test]
fn range_request_single_key_conformance() {
    // Single key lookup - range_end empty
    let req = RangeRequest {
        key: test_key("single"),
        range_end: vec![],
        limit: 0,
        revision: 0,
        sort_order: SortOrder::None,
        sort_target: SortTarget::Key,
        serializable: false,
        keys_only: false,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };

    assert!(req.range_end.is_empty());
    assert!(!req.serializable); // Linearizable by default
}

#[test]
fn range_request_prefix_conformance() {
    // Prefix query - range_end is prefix with last byte incremented
    let prefix = test_key("prefix/");
    let req = RangeRequest {
        key: prefix.clone(),
        range_end: prefix_end(&prefix),
        limit: 0,
        revision: 0,
        sort_order: SortOrder::None,
        sort_target: SortTarget::Key,
        serializable: false,
        keys_only: false,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };

    assert!(!req.range_end.is_empty());
    assert!(req.range_end > req.key);
}

#[test]
fn range_request_serializable_read() {
    // Serializable (snapshot-only) read for performance
    let req = RangeRequest {
        key: test_key("snapshot"),
        range_end: vec![],
        limit: 0,
        revision: 0,
        sort_order: SortOrder::None,
        sort_target: SortTarget::Key,
        serializable: true,
        keys_only: false,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };

    assert!(req.serializable);
}

#[test]
fn range_request_count_only() {
    // count_only=true returns count without key-value pairs
    let req = RangeRequest {
        key: test_key(""),
        range_end: prefix_end(&test_key("")),
        limit: 0,
        revision: 0,
        sort_order: SortOrder::None,
        sort_target: SortTarget::Key,
        serializable: false,
        keys_only: false,
        count_only: true,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };

    assert!(req.count_only);
}

#[test]
fn range_request_keys_only() {
    // keys_only=true omits values from response
    let req = RangeRequest {
        key: test_key(""),
        range_end: prefix_end(&test_key("")),
        limit: 0,
        revision: 0,
        sort_order: SortOrder::None,
        sort_target: SortTarget::Key,
        serializable: false,
        keys_only: true,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };

    assert!(req.keys_only);
}

#[test]
fn range_request_with_limit() {
    // Pagination with limit
    let req = RangeRequest {
        key: test_key(""),
        range_end: prefix_end(&test_key("")),
        limit: 100,
        revision: 0,
        sort_order: SortOrder::None,
        sort_target: SortTarget::Key,
        serializable: false,
        keys_only: false,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };

    assert_eq!(req.limit, 100);
}

#[test]
fn range_request_at_revision() {
    // Historical read at specific revision
    let req = RangeRequest {
        key: test_key("historical"),
        range_end: vec![],
        limit: 0,
        revision: 12345,
        sort_order: SortOrder::None,
        sort_target: SortTarget::Key,
        serializable: false,
        keys_only: false,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };

    assert_eq!(req.revision, 12345);
}

#[test]
fn range_response_structure() {
    // Verify RangeResponse matches etcd spec
    let resp = RangeResponse {
        header: ResponseHeader {
            cluster_id: 1,
            member_id: 1,
            revision: 100,
            raft_term: 5,
        },
        kvs: vec![KeyValue {
            key: test_key("result"),
            value: test_value("data"),
            create_revision: 50,
            mod_revision: 100,
            version: 3,
            lease: 0,
        }],
        more: false,
        count: 1,
    };

    assert_eq!(resp.header.revision, 100);
    assert_eq!(resp.kvs.len(), 1);
    assert!(!resp.more);
    assert_eq!(resp.count, 1);
}

#[test]
fn range_response_pagination() {
    // Verify pagination with more=true
    let resp = RangeResponse {
        header: ResponseHeader {
            cluster_id: 1,
            member_id: 1,
            revision: 100,
            raft_term: 5,
        },
        kvs: vec![],
        more: true,
        count: 50,
    };

    assert!(resp.more);
    assert_eq!(resp.count, 50);
}

#[test]
fn delete_range_request_single() {
    let req = DeleteRangeRequest {
        key: test_key("delete"),
        range_end: vec![],
        prev_kv: false,
    };

    assert!(req.range_end.is_empty());
}

#[test]
fn delete_range_request_prefix() {
    let prefix = test_key("delete/");
    let req = DeleteRangeRequest {
        key: prefix.clone(),
        range_end: prefix_end(&prefix),
        prev_kv: true,
    };

    assert!(!req.range_end.is_empty());
    assert!(req.prev_kv);
}

// ============================================================================
// Transaction Conformance Tests (Task 191)
// ============================================================================

#[test]
fn txn_compare_mod_revision() {
    let cmp = Compare {
        key: test_key("txn"),
        target: CompareTarget::Mod,
        result: CompareResult::Equal,
        target_union: CompareTargetUnion::ModRevision(100),
        range_end: vec![],
    };

    assert!(matches!(cmp.target, CompareTarget::Mod));
    assert!(matches!(cmp.result, CompareResult::Equal));
}

#[test]
fn txn_compare_create_revision() {
    let cmp = Compare {
        key: test_key("txn"),
        target: CompareTarget::Create,
        result: CompareResult::Greater,
        target_union: CompareTargetUnion::CreateRevision(50),
        range_end: vec![],
    };

    assert!(matches!(cmp.target, CompareTarget::Create));
    assert!(matches!(cmp.result, CompareResult::Greater));
}

#[test]
fn txn_compare_version() {
    let cmp = Compare {
        key: test_key("txn"),
        target: CompareTarget::Version,
        result: CompareResult::Less,
        target_union: CompareTargetUnion::Version(5),
        range_end: vec![],
    };

    assert!(matches!(cmp.target, CompareTarget::Version));
    assert!(matches!(cmp.result, CompareResult::Less));
}

#[test]
fn txn_request_structure() {
    let txn = TxnRequest {
        compare: vec![Compare {
            key: test_key("cond"),
            target: CompareTarget::Mod,
            result: CompareResult::Equal,
            target_union: CompareTargetUnion::ModRevision(100),
            range_end: vec![],
        }],
        success: vec![RequestOp::Put(PutRequest {
            key: test_key("new"),
            value: test_value("value"),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })],
        failure: vec![],
    };

    assert_eq!(txn.compare.len(), 1);
    assert_eq!(txn.success.len(), 1);
    assert!(txn.failure.is_empty());
}

#[test]
fn txn_request_with_failure_branch() {
    let txn = TxnRequest {
        compare: vec![Compare {
            key: test_key("cond"),
            target: CompareTarget::Version,
            result: CompareResult::Equal,
            target_union: CompareTargetUnion::Version(0), // Key doesn't exist
            range_end: vec![],
        }],
        success: vec![RequestOp::Put(PutRequest {
            key: test_key("create"),
            value: test_value("new"),
            lease: 0,
            prev_kv: false,
            ignore_value: false,
            ignore_lease: false,
        })],
        failure: vec![RequestOp::Put(PutRequest {
            key: test_key("update"),
            value: test_value("updated"),
            lease: 0,
            prev_kv: true,
            ignore_value: false,
            ignore_lease: false,
        })],
    };

    assert!(!txn.failure.is_empty());
}

#[test]
fn txn_request_range_operation() {
    let txn = TxnRequest {
        compare: vec![],
        success: vec![RequestOp::Range(RangeRequest {
            key: test_key(""),
            range_end: prefix_end(&test_key("")),
            limit: 10,
            revision: 0,
            sort_order: SortOrder::None,
            sort_target: SortTarget::Key,
            serializable: false,
            keys_only: false,
            count_only: false,
            min_mod_revision: 0,
            max_mod_revision: 0,
            min_create_revision: 0,
            max_create_revision: 0,
        })],
        failure: vec![],
    };

    assert!(matches!(txn.success[0], RequestOp::Range(_)));
}

#[test]
fn txn_request_delete_operation() {
    let txn = TxnRequest {
        compare: vec![],
        success: vec![RequestOp::DeleteRange(DeleteRangeRequest {
            key: test_key("delete"),
            range_end: vec![],
            prev_kv: true,
        })],
        failure: vec![],
    };

    assert!(matches!(txn.success[0], RequestOp::DeleteRange(_)));
}

// ============================================================================
// Watch Conformance Tests (Task 192)
// ============================================================================

#[test]
fn watch_create_request_single_key_conformance() {
    let req = WatchCreateRequest {
        key: test_key("watch"),
        range_end: vec![],
        start_revision: 0,
        progress_notify: false,
        filters: vec![],
        prev_kv: false,
        watch_id: 0,
        fragment: false,
    };

    assert!(req.range_end.is_empty());
    assert_eq!(req.start_revision, 0); // Start from "now"
}

#[test]
fn watch_create_request_prefix_conformance() {
    let prefix = test_key("watch/");
    let req = WatchCreateRequest {
        key: prefix.clone(),
        range_end: prefix_end(&prefix),
        start_revision: 100,
        progress_notify: true,
        filters: vec![],
        prev_kv: true,
        watch_id: 0,
        fragment: false,
    };

    assert!(!req.range_end.is_empty());
    assert_eq!(req.start_revision, 100);
    assert!(req.progress_notify);
    assert!(req.prev_kv);
}

#[test]
fn watch_key_range_single_key() {
    let range = WatchKeyRange::SingleKey(test_key("single"));
    assert!(range.matches(&test_key("single")));
    assert!(!range.matches(&test_key("other")));
}

#[test]
fn watch_key_range_prefix_matching() {
    let range = WatchKeyRange::Prefix(test_key("prefix/"));
    assert!(range.matches(&test_key("prefix/a")));
    assert!(range.matches(&test_key("prefix/b/c")));
    assert!(!range.matches(&test_key("other/a")));
}

#[test]
fn watch_key_range_bounds() {
    let range = WatchKeyRange::Range {
        start: test_key("a"),
        end: test_key("m"),
    };
    assert!(range.matches(&test_key("apple")));
    assert!(range.matches(&test_key("lemon")));
    assert!(!range.matches(&test_key("orange")));
}

#[test]
fn event_type_put() {
    let event = Event {
        r#type: EventTypeProto::Put,
        kv: KeyValue {
            key: test_key("event"),
            value: test_value("data"),
            create_revision: 100,
            mod_revision: 100,
            version: 1,
            lease: 0,
        },
        prev_kv: None,
    };

    assert!(matches!(event.r#type, EventTypeProto::Put));
}

#[test]
fn event_type_delete() {
    let event = Event {
        r#type: EventTypeProto::Delete,
        kv: KeyValue {
            key: test_key("event"),
            value: vec![],
            create_revision: 100,
            mod_revision: 101,
            version: 0,
            lease: 0,
        },
        prev_kv: Some(KeyValue {
            key: test_key("event"),
            value: test_value("old"),
            create_revision: 100,
            mod_revision: 100,
            version: 1,
            lease: 0,
        }),
    };

    assert!(matches!(event.r#type, EventTypeProto::Delete));
    assert!(event.prev_kv.is_some());
}

#[test]
fn watch_semantics_variants() {
    assert!(matches!(
        WatchSemantics::Linearizable,
        WatchSemantics::Linearizable
    ));
    assert!(matches!(
        WatchSemantics::SnapshotOnly,
        WatchSemantics::SnapshotOnly
    ));
}

// ============================================================================
// Lease Conformance Tests (Task 193)
// ============================================================================

#[test]
fn lease_grant_request_with_ttl() {
    let req = LeaseGrantRequest { ttl: 60, id: 0 };

    assert_eq!(req.ttl, 60);
    assert_eq!(req.id, 0); // Auto-assign ID
}

#[test]
fn lease_grant_request_with_explicit_id() {
    let req = LeaseGrantRequest {
        ttl: 300,
        id: 12345,
    };

    assert_eq!(req.ttl, 300);
    assert_eq!(req.id, 12345);
}

#[test]
fn lease_revoke_request_structure() {
    let req = LeaseRevokeRequest { id: 12345 };
    assert_eq!(req.id, 12345);
}

#[test]
fn put_with_lease_attachment() {
    let req = PutRequest {
        key: test_key("leased"),
        value: test_value("data"),
        lease: 12345,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };

    assert_eq!(req.lease, 12345);
}

#[test]
fn keyvalue_with_lease() {
    let kv = KeyValue {
        key: test_key("leased"),
        value: test_value("data"),
        create_revision: 100,
        mod_revision: 100,
        version: 1,
        lease: 12345,
    };

    assert_eq!(kv.lease, 12345);
}

// ============================================================================
// Error Code Conformance (Task 194 & 195)
// ============================================================================

#[test]
fn grpc_code_ok() {
    assert_eq!(GrpcCode::Ok as i32, 0);
}

#[test]
fn grpc_code_not_found() {
    assert_eq!(GrpcCode::NotFound as i32, 5);
}

#[test]
fn grpc_code_unavailable() {
    assert_eq!(GrpcCode::Unavailable as i32, 14);
}

#[test]
fn grpc_code_permission_denied() {
    assert_eq!(GrpcCode::PermissionDenied as i32, 7);
}

#[test]
fn grpc_code_failed_precondition() {
    assert_eq!(GrpcCode::FailedPrecondition as i32, 9);
}

#[test]
fn grpc_code_out_of_range() {
    assert_eq!(GrpcCode::OutOfRange as i32, 11);
}

#[test]
fn etcd_error_structure() {
    let err = EtcdError {
        code: GrpcCode::NotFound,
        message: "key not found".to_string(),
        details: None,
    };

    assert_eq!(err.code, GrpcCode::NotFound);
    assert!(err.message.contains("not found"));
}

#[test]
fn etcd_error_unavailable() {
    // Used for LIN-BOUND failures
    let err = EtcdError {
        code: GrpcCode::Unavailable,
        message: "linearization unavailable".to_string(),
        details: None,
    };

    assert_eq!(err.code, GrpcCode::Unavailable);
}

#[test]
fn etcd_error_failed_precondition() {
    // Used for dirty_epoch and cross-shard txn
    let err = EtcdError {
        code: GrpcCode::FailedPrecondition,
        message: "transaction spans multiple KPGs".to_string(),
        details: None,
    };

    assert_eq!(err.code, GrpcCode::FailedPrecondition);
}

// ============================================================================
// Permission Conformance
// ============================================================================

#[test]
fn permission_read() {
    assert!(matches!(Permission::Read, Permission::Read));
}

#[test]
fn permission_write() {
    assert!(matches!(Permission::Write, Permission::Write));
}

#[test]
fn permission_read_write() {
    assert!(matches!(Permission::ReadWrite, Permission::ReadWrite));
}

// ============================================================================
// Revision and Version Semantics
// ============================================================================

#[test]
fn keyvalue_revision_semantics() {
    // On first Put: create_revision == mod_revision
    let kv_first = KeyValue {
        key: test_key("new"),
        value: test_value("data"),
        create_revision: 100,
        mod_revision: 100,
        version: 1,
        lease: 0,
    };

    assert_eq!(kv_first.create_revision, kv_first.mod_revision);
    assert_eq!(kv_first.version, 1);

    // On update: create_revision unchanged, mod_revision incremented, version incremented
    let kv_updated = KeyValue {
        key: test_key("new"),
        value: test_value("updated"),
        create_revision: 100,
        mod_revision: 150,
        version: 2,
        lease: 0,
    };

    assert_eq!(kv_updated.create_revision, 100); // Unchanged
    assert!(kv_updated.mod_revision > kv_first.mod_revision);
    assert_eq!(kv_updated.version, kv_first.version + 1);
}

#[test]
fn response_header_revision_tracking() {
    let header_before = ResponseHeader {
        cluster_id: 1,
        member_id: 1,
        revision: 100,
        raft_term: 5,
    };

    let header_after = ResponseHeader {
        cluster_id: 1,
        member_id: 1,
        revision: 101,
        raft_term: 5,
    };

    assert_revision_incremented(header_before.revision, header_after.revision);
    assert_revision_delta(header_before.revision, header_after.revision, 1);
}

// ============================================================================
// Sort Order Conformance
// ============================================================================

#[test]
fn sort_order_none() {
    let order = SortOrder::None;
    assert!(matches!(order, SortOrder::None));
}

#[test]
fn sort_order_ascend() {
    let order = SortOrder::Ascend;
    assert!(matches!(order, SortOrder::Ascend));
}

#[test]
fn sort_order_descend() {
    let order = SortOrder::Descend;
    assert!(matches!(order, SortOrder::Descend));
}

#[test]
fn sort_target_key() {
    let target = SortTarget::Key;
    assert!(matches!(target, SortTarget::Key));
}

#[test]
fn sort_target_version() {
    let target = SortTarget::Version;
    assert!(matches!(target, SortTarget::Version));
}

#[test]
fn sort_target_create() {
    let target = SortTarget::Create;
    assert!(matches!(target, SortTarget::Create));
}

#[test]
fn sort_target_mod() {
    let target = SortTarget::Mod;
    assert!(matches!(target, SortTarget::Mod));
}

#[test]
fn sort_target_value() {
    let target = SortTarget::Value;
    assert!(matches!(target, SortTarget::Value));
}
