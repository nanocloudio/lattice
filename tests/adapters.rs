//! Adapter integration tests.
//!
//! Tests for etcd adapter and protocol handling.

mod common;

use lattice::adapters::etcd::auth::{Permission, Role};
use lattice::adapters::etcd::kv::{SortOrder, SortTarget};
use lattice::adapters::etcd::lease::{
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseRevokeRequest,
};
use lattice::adapters::etcd::watch::{
    Event, EventTypeProto, WatchCreateRequest, WatchKeyRange, WatchSemantics,
};
use lattice::adapters::etcd::{
    DeleteRangeRequest, DeleteRangeResponse, EtcdError, GrpcCode, KeyValue, PutRequest,
    PutResponse, RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
};

// ============================================================================
// EtcdError Tests
// ============================================================================

#[test]
fn etcd_error_basic() {
    let err = EtcdError {
        code: GrpcCode::NotFound,
        message: "key not found".to_string(),
        details: None,
    };
    assert!(err.message.contains("not found"));
    assert_eq!(err.code, GrpcCode::NotFound);
}

#[test]
fn etcd_error_grpc_codes() {
    assert_eq!(GrpcCode::Ok as i32, 0);
    assert_eq!(GrpcCode::NotFound as i32, 5);
    assert_eq!(GrpcCode::Unavailable as i32, 14);
    assert_eq!(GrpcCode::PermissionDenied as i32, 7);
}

// ============================================================================
// RangeRequest Tests
// ============================================================================

#[test]
fn range_request_single_key() {
    let request = RangeRequest {
        key: b"my-key".to_vec(),
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

    assert_eq!(request.key, b"my-key");
    assert!(request.range_end.is_empty());
}

#[test]
fn range_request_with_options() {
    let request = RangeRequest {
        key: b"prefix/".to_vec(),
        range_end: b"prefix0".to_vec(), // Prefix query
        limit: 100,
        revision: 12345,
        sort_order: SortOrder::Ascend,
        sort_target: SortTarget::Mod,
        serializable: true,
        keys_only: true,
        count_only: false,
        min_mod_revision: 0,
        max_mod_revision: 0,
        min_create_revision: 0,
        max_create_revision: 0,
    };

    assert_eq!(request.limit, 100);
    assert_eq!(request.revision, 12345);
    assert!(request.serializable);
    assert!(request.keys_only);
}

#[test]
fn range_response_structure() {
    let response = RangeResponse {
        header: ResponseHeader {
            cluster_id: 12345,
            member_id: 1,
            revision: 100,
            raft_term: 42,
        },
        kvs: vec![KeyValue {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            create_revision: 50,
            mod_revision: 100,
            version: 3,
            lease: 0,
        }],
        more: false,
        count: 1,
    };

    assert_eq!(response.header.revision, 100);
    assert_eq!(response.kvs.len(), 1);
    assert_eq!(response.kvs[0].key, b"key1");
    assert_eq!(response.count, 1);
}

// ============================================================================
// PutRequest Tests
// ============================================================================

#[test]
fn put_request_basic() {
    let request = PutRequest {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        lease: 0,
        prev_kv: false,
        ignore_value: false,
        ignore_lease: false,
    };

    assert_eq!(request.key, b"key");
    assert_eq!(request.value, b"value");
    assert_eq!(request.lease, 0);
    assert!(!request.prev_kv);
}

#[test]
fn put_request_with_lease() {
    let request = PutRequest {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        lease: 42,
        prev_kv: true,
        ignore_value: false,
        ignore_lease: false,
    };

    assert_eq!(request.lease, 42);
    assert!(request.prev_kv);
}

#[test]
fn put_response_structure() {
    let response = PutResponse {
        header: ResponseHeader {
            cluster_id: 12345,
            member_id: 1,
            revision: 101,
            raft_term: 42,
        },
        prev_kv: Some(KeyValue {
            key: b"key".to_vec(),
            value: b"old-value".to_vec(),
            create_revision: 50,
            mod_revision: 100,
            version: 2,
            lease: 0,
        }),
    };

    assert_eq!(response.header.revision, 101);
    assert!(response.prev_kv.is_some());
    assert_eq!(response.prev_kv.as_ref().unwrap().value, b"old-value");
}

// ============================================================================
// DeleteRangeRequest Tests
// ============================================================================

#[test]
fn delete_range_request_single() {
    let request = DeleteRangeRequest {
        key: b"key".to_vec(),
        range_end: vec![],
        prev_kv: false,
    };

    assert_eq!(request.key, b"key");
    assert!(request.range_end.is_empty());
}

#[test]
fn delete_range_request_prefix() {
    let request = DeleteRangeRequest {
        key: b"prefix/".to_vec(),
        range_end: b"prefix0".to_vec(),
        prev_kv: true,
    };

    assert!(!request.range_end.is_empty());
    assert!(request.prev_kv);
}

#[test]
fn delete_range_response_structure() {
    let response = DeleteRangeResponse {
        header: ResponseHeader {
            cluster_id: 12345,
            member_id: 1,
            revision: 102,
            raft_term: 42,
        },
        deleted: 5,
        prev_kvs: vec![],
    };

    assert_eq!(response.deleted, 5);
    assert_eq!(response.header.revision, 102);
}

// ============================================================================
// Transaction Tests
// ============================================================================

#[test]
fn txn_request_empty() {
    let txn = TxnRequest {
        compare: vec![],
        success: vec![],
        failure: vec![],
    };

    assert!(txn.compare.is_empty());
    assert!(txn.success.is_empty());
    assert!(txn.failure.is_empty());
}

#[test]
fn txn_response_success() {
    let response = TxnResponse {
        header: ResponseHeader {
            cluster_id: 12345,
            member_id: 1,
            revision: 103,
            raft_term: 42,
        },
        succeeded: true,
        responses: vec![],
    };

    assert!(response.succeeded);
    assert_eq!(response.header.revision, 103);
}

// ============================================================================
// Lease Tests
// ============================================================================

#[test]
fn lease_grant_request() {
    let request = LeaseGrantRequest { ttl: 60, id: 0 };

    assert_eq!(request.ttl, 60);
    assert_eq!(request.id, 0);
}

#[test]
fn lease_grant_request_with_id() {
    let request = LeaseGrantRequest { ttl: 60, id: 12345 };

    assert_eq!(request.ttl, 60);
    assert_eq!(request.id, 12345);
}

#[test]
fn lease_grant_response() {
    let response = LeaseGrantResponse {
        header: ResponseHeader {
            cluster_id: 12345,
            member_id: 1,
            revision: 100,
            raft_term: 42,
        },
        id: 99,
        ttl: 60,
        error: String::new(),
    };

    assert_eq!(response.id, 99);
    assert_eq!(response.ttl, 60);
    assert!(response.error.is_empty());
    assert_eq!(response.header.revision, 100);
}

#[test]
fn lease_keep_alive_request() {
    let request = LeaseKeepAliveRequest { id: 42 };
    assert_eq!(request.id, 42);
}

#[test]
fn lease_revoke_request() {
    let request = LeaseRevokeRequest { id: 42 };
    assert_eq!(request.id, 42);
}

// ============================================================================
// Watch Tests
// ============================================================================

#[test]
fn watch_create_request_single_key() {
    let request = WatchCreateRequest {
        key: b"key".to_vec(),
        range_end: vec![],
        start_revision: 0,
        progress_notify: false,
        filters: vec![],
        prev_kv: false,
        watch_id: 0,
        fragment: false,
    };

    assert_eq!(request.key, b"key");
    assert!(request.range_end.is_empty());
    assert_eq!(request.start_revision, 0);
}

#[test]
fn watch_create_request_prefix() {
    let request = WatchCreateRequest {
        key: b"prefix/".to_vec(),
        range_end: b"prefix0".to_vec(),
        start_revision: 100,
        progress_notify: true,
        filters: vec![],
        prev_kv: true,
        watch_id: 0,
        fragment: false,
    };

    assert!(!request.range_end.is_empty());
    assert_eq!(request.start_revision, 100);
    assert!(request.progress_notify);
    assert!(request.prev_kv);
}

#[test]
fn watch_key_range_exact() {
    let range = WatchKeyRange::SingleKey(b"key".to_vec());
    assert!(range.matches(b"key"));
    assert!(!range.matches(b"other"));
}

#[test]
fn watch_key_range_prefix() {
    let range = WatchKeyRange::Prefix(b"prefix/".to_vec());
    assert!(range.matches(b"prefix/foo"));
    assert!(range.matches(b"prefix/bar/baz"));
    assert!(!range.matches(b"other/foo"));
}

#[test]
fn watch_key_range_range() {
    let range = WatchKeyRange::Range {
        start: b"a".to_vec(),
        end: b"m".to_vec(),
    };
    assert!(range.matches(b"apple"));
    assert!(range.matches(b"lemon"));
    assert!(!range.matches(b"orange"));
}

#[test]
fn watch_key_range_parse() {
    // Empty range_end = single key
    let range = WatchKeyRange::parse(b"key", b"");
    assert!(matches!(range, WatchKeyRange::SingleKey(_)));

    // Range end [0] = prefix
    let range = WatchKeyRange::parse(b"prefix/", &[0]);
    assert!(matches!(range, WatchKeyRange::Prefix(_)));

    // Other range_end = range
    let range = WatchKeyRange::parse(b"a", b"m");
    assert!(matches!(range, WatchKeyRange::Range { .. }));
}

#[test]
fn watch_semantics_variants() {
    let linearizable = WatchSemantics::Linearizable;
    let snapshot_only = WatchSemantics::SnapshotOnly;

    assert!(matches!(linearizable, WatchSemantics::Linearizable));
    assert!(matches!(snapshot_only, WatchSemantics::SnapshotOnly));
}

#[test]
fn event_put() {
    let kv = KeyValue {
        key: b"key".to_vec(),
        value: b"value".to_vec(),
        create_revision: 100,
        mod_revision: 100,
        version: 1,
        lease: 0,
    };

    let event = Event {
        r#type: EventTypeProto::Put,
        kv,
        prev_kv: None,
    };

    assert!(matches!(event.r#type, EventTypeProto::Put));
}

#[test]
fn event_delete() {
    let kv = KeyValue {
        key: b"key".to_vec(),
        value: vec![],
        create_revision: 100,
        mod_revision: 101,
        version: 0,
        lease: 0,
    };

    let prev_kv = KeyValue {
        key: b"key".to_vec(),
        value: b"old-value".to_vec(),
        create_revision: 100,
        mod_revision: 100,
        version: 1,
        lease: 0,
    };

    let event = Event {
        r#type: EventTypeProto::Delete,
        kv,
        prev_kv: Some(prev_kv),
    };

    assert!(matches!(event.r#type, EventTypeProto::Delete));
    assert!(event.prev_kv.is_some());
}

// ============================================================================
// Auth Tests
// ============================================================================

#[test]
fn permission_variants() {
    assert!(matches!(Permission::Read, Permission::Read));
    assert!(matches!(Permission::Write, Permission::Write));
    assert!(matches!(Permission::ReadWrite, Permission::ReadWrite));
}

#[test]
fn role_basic() {
    let role = Role {
        name: "reader".to_string(),
        permissions: vec![],
    };

    assert_eq!(role.name, "reader");
    assert!(role.permissions.is_empty());
}

#[test]
fn role_new() {
    let role = Role::new("writer");
    assert_eq!(role.name, "writer");
    assert!(role.permissions.is_empty());
}

// ============================================================================
// KeyValue Tests
// ============================================================================

#[test]
fn key_value_structure() {
    let kv = KeyValue {
        key: b"test-key".to_vec(),
        value: b"test-value".to_vec(),
        create_revision: 100,
        mod_revision: 150,
        version: 5,
        lease: 42,
    };

    assert_eq!(kv.key, b"test-key");
    assert_eq!(kv.value, b"test-value");
    assert_eq!(kv.create_revision, 100);
    assert_eq!(kv.mod_revision, 150);
    assert_eq!(kv.version, 5);
    assert_eq!(kv.lease, 42);
}

// ============================================================================
// ResponseHeader Tests
// ============================================================================

#[test]
fn response_header_structure() {
    let header = ResponseHeader {
        cluster_id: 1234567890,
        member_id: 42,
        revision: 999,
        raft_term: 10,
    };

    assert_eq!(header.cluster_id, 1234567890);
    assert_eq!(header.member_id, 42);
    assert_eq!(header.revision, 999);
    assert_eq!(header.raft_term, 10);
}
