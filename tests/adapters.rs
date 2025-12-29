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

// ============================================================================
// Memcached Protocol Tests
// ============================================================================

use bytes::Bytes;
use lattice::adapters::memcached::protocol::{
    AsciiEncoder, AsciiParser, BinaryEncoder, BinaryParser, MemcachedCodec, ParseResult,
};
use lattice::adapters::memcached::{
    BinaryOpcode, BinaryStatus, MemcachedCommand, MemcachedError, MemcachedResponse, ProtocolType,
    ResponseStatus, ValueResponse, BINARY_REQUEST_MAGIC, BINARY_RESPONSE_MAGIC,
};

#[test]
fn memcached_command_structure() {
    let cmd = MemcachedCommand::new("set")
        .with_key(Bytes::from_static(b"mykey"))
        .with_data(Bytes::from_static(b"myvalue"))
        .with_flags(0)
        .with_exptime(3600);

    assert_eq!(cmd.name, "set");
    assert_eq!(cmd.key_str(), Some("mykey"));
    assert_eq!(cmd.exptime, 3600);
}

#[test]
fn memcached_response_stored() {
    let response = MemcachedResponse::stored();
    assert!(matches!(
        response,
        MemcachedResponse::Status(ResponseStatus::Stored)
    ));
}

#[test]
fn memcached_response_not_stored() {
    let response = MemcachedResponse::not_stored();
    assert!(matches!(
        response,
        MemcachedResponse::Status(ResponseStatus::NotStored)
    ));
}

#[test]
fn memcached_response_exists() {
    let response = MemcachedResponse::exists();
    assert!(matches!(
        response,
        MemcachedResponse::Status(ResponseStatus::Exists)
    ));
}

#[test]
fn memcached_response_not_found() {
    let response = MemcachedResponse::not_found();
    assert!(matches!(
        response,
        MemcachedResponse::Status(ResponseStatus::NotFound)
    ));
}

#[test]
fn memcached_response_deleted() {
    let response = MemcachedResponse::deleted();
    assert!(matches!(
        response,
        MemcachedResponse::Status(ResponseStatus::Deleted)
    ));
}

#[test]
fn memcached_response_touched() {
    let response = MemcachedResponse::touched();
    assert!(matches!(
        response,
        MemcachedResponse::Status(ResponseStatus::Touched)
    ));
}

#[test]
fn memcached_response_error() {
    let response = MemcachedResponse::error();
    assert!(matches!(
        response,
        MemcachedResponse::Error(MemcachedError::Error)
    ));
}

#[test]
fn memcached_response_client_error() {
    let response = MemcachedResponse::client_error("bad format");
    match response {
        MemcachedResponse::Error(MemcachedError::ClientError(msg)) => {
            assert_eq!(msg, "bad format");
        }
        _ => panic!("Expected ClientError"),
    }
}

#[test]
fn memcached_response_server_error() {
    let response = MemcachedResponse::server_error("out of memory");
    match response {
        MemcachedResponse::Error(MemcachedError::ServerError(msg)) => {
            assert_eq!(msg, "out of memory");
        }
        _ => panic!("Expected ServerError"),
    }
}

#[test]
fn memcached_response_value() {
    let response = MemcachedResponse::value(
        Bytes::from_static(b"mykey"),
        0,
        Bytes::from_static(b"myvalue"),
    );
    match response {
        MemcachedResponse::Value(val) => {
            assert_eq!(val.key, Bytes::from_static(b"mykey"));
            assert_eq!(val.data, Bytes::from_static(b"myvalue"));
        }
        _ => panic!("Expected Value"),
    }
}

#[test]
fn memcached_response_values() {
    let values = vec![
        ValueResponse {
            key: Bytes::from_static(b"key1"),
            flags: 0,
            data: Bytes::from_static(b"value1"),
            cas: None,
        },
        ValueResponse {
            key: Bytes::from_static(b"key2"),
            flags: 1,
            data: Bytes::from_static(b"value2"),
            cas: None,
        },
    ];
    let response = MemcachedResponse::Values(values);
    match response {
        MemcachedResponse::Values(vals) => assert_eq!(vals.len(), 2),
        _ => panic!("Expected Values"),
    }
}

#[test]
fn memcached_response_numeric() {
    let response = MemcachedResponse::Numeric(42);
    match response {
        MemcachedResponse::Numeric(n) => assert_eq!(n, 42),
        _ => panic!("Expected Numeric"),
    }
}

#[test]
fn memcached_response_version() {
    let response = MemcachedResponse::Version("1.0.0".to_string());
    match response {
        MemcachedResponse::Version(ver) => assert_eq!(ver, "1.0.0"),
        _ => panic!("Expected Version"),
    }
}

#[test]
fn memcached_response_ok() {
    let response = MemcachedResponse::ok();
    assert!(matches!(
        response,
        MemcachedResponse::Status(ResponseStatus::Ok)
    ));
}

// ============================================================================
// Binary Opcode Tests
// ============================================================================

#[test]
fn binary_opcode_values() {
    assert_eq!(BinaryOpcode::Get as u8, 0x00);
    assert_eq!(BinaryOpcode::Set as u8, 0x01);
    assert_eq!(BinaryOpcode::Add as u8, 0x02);
    assert_eq!(BinaryOpcode::Replace as u8, 0x03);
    assert_eq!(BinaryOpcode::Delete as u8, 0x04);
    assert_eq!(BinaryOpcode::Increment as u8, 0x05);
    assert_eq!(BinaryOpcode::Decrement as u8, 0x06);
    assert_eq!(BinaryOpcode::Quit as u8, 0x07);
    assert_eq!(BinaryOpcode::Flush as u8, 0x08);
    assert_eq!(BinaryOpcode::Noop as u8, 0x0a);
    assert_eq!(BinaryOpcode::Version as u8, 0x0b);
}

#[test]
fn binary_opcode_quiet_variants() {
    assert_eq!(BinaryOpcode::GetQ as u8, 0x09);
    assert_eq!(BinaryOpcode::GetK as u8, 0x0c);
    assert_eq!(BinaryOpcode::GetKQ as u8, 0x0d);
    assert_eq!(BinaryOpcode::SetQ as u8, 0x11);
    assert_eq!(BinaryOpcode::AddQ as u8, 0x12);
    assert_eq!(BinaryOpcode::ReplaceQ as u8, 0x13);
    assert_eq!(BinaryOpcode::DeleteQ as u8, 0x14);
}

#[test]
fn binary_opcode_is_quiet() {
    assert!(BinaryOpcode::GetQ.is_quiet());
    assert!(BinaryOpcode::SetQ.is_quiet());
    assert!(!BinaryOpcode::Get.is_quiet());
    assert!(!BinaryOpcode::Set.is_quiet());
}

#[test]
fn binary_opcode_to_non_quiet() {
    assert_eq!(BinaryOpcode::GetQ.to_non_quiet(), BinaryOpcode::Get);
    assert_eq!(BinaryOpcode::SetQ.to_non_quiet(), BinaryOpcode::Set);
    assert_eq!(BinaryOpcode::Get.to_non_quiet(), BinaryOpcode::Get);
}

#[test]
fn binary_opcode_to_command_name() {
    assert_eq!(BinaryOpcode::Get.to_command_name(), "get");
    assert_eq!(BinaryOpcode::Set.to_command_name(), "set");
    assert_eq!(BinaryOpcode::Delete.to_command_name(), "delete");
    assert_eq!(BinaryOpcode::Increment.to_command_name(), "incr");
    assert_eq!(BinaryOpcode::Decrement.to_command_name(), "decr");
}

#[test]
fn binary_status_values() {
    assert_eq!(BinaryStatus::NoError as u16, 0x0000);
    assert_eq!(BinaryStatus::KeyNotFound as u16, 0x0001);
    assert_eq!(BinaryStatus::KeyExists as u16, 0x0002);
    assert_eq!(BinaryStatus::ValueTooLarge as u16, 0x0003);
    assert_eq!(BinaryStatus::InvalidArguments as u16, 0x0004);
    assert_eq!(BinaryStatus::ItemNotStored as u16, 0x0005);
    assert_eq!(BinaryStatus::DeltaBadval as u16, 0x0006);
    assert_eq!(BinaryStatus::UnknownCommand as u16, 0x0081);
    assert_eq!(BinaryStatus::OutOfMemory as u16, 0x0082);
}

#[test]
fn binary_status_conversion() {
    assert_eq!(u16::from(BinaryStatus::NoError), 0x0000);
    assert_eq!(u16::from(BinaryStatus::KeyNotFound), 0x0001);
    assert_eq!(
        BinaryStatus::try_from(0x0001),
        Ok(BinaryStatus::KeyNotFound)
    );
    assert!(BinaryStatus::try_from(0xFFFF).is_err());
}

// ============================================================================
// ASCII Protocol Parser Tests
// ============================================================================

#[test]
fn ascii_parser_get() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"get mykey\r\n");

    match result {
        ParseResult::Ok(cmd, consumed) => {
            assert_eq!(cmd.name, "get");
            assert_eq!(cmd.key_str(), Some("mykey"));
            assert_eq!(consumed, 11);
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_gets() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"gets key1 key2 key3\r\n");

    match result {
        ParseResult::Ok(cmd, consumed) => {
            assert_eq!(cmd.name, "gets");
            // First key is in cmd.key, rest in cmd.keys
            let all_keys: Vec<_> = cmd.all_keys().collect();
            assert_eq!(all_keys.len(), 3);
            assert_eq!(consumed, 21);
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_set() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"set mykey 0 3600 5\r\nvalue\r\n");

    match result {
        ParseResult::Ok(cmd, consumed) => {
            assert_eq!(cmd.name, "set");
            assert_eq!(cmd.key_str(), Some("mykey"));
            assert_eq!(cmd.flags, 0);
            assert_eq!(cmd.exptime, 3600);
            assert!(cmd.data.is_some());
            assert_eq!(consumed, 27);
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_set_noreply() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"set mykey 0 3600 5 noreply\r\nvalue\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "set");
            assert!(cmd.noreply);
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_add() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"add mykey 0 3600 5\r\nvalue\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "add");
            assert_eq!(cmd.key_str(), Some("mykey"));
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_replace() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"replace mykey 0 3600 5\r\nvalue\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "replace");
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_cas() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"cas mykey 0 3600 5 12345\r\nvalue\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "cas");
            assert_eq!(cmd.cas, Some(12345));
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_delete() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"delete mykey\r\n");

    match result {
        ParseResult::Ok(cmd, consumed) => {
            assert_eq!(cmd.name, "delete");
            assert_eq!(cmd.key_str(), Some("mykey"));
            assert_eq!(consumed, 14);
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_incr() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"incr counter 10\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "incr");
            assert_eq!(cmd.key_str(), Some("counter"));
            assert_eq!(cmd.delta, Some(10));
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_decr() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"decr counter 5\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "decr");
            assert_eq!(cmd.delta, Some(5));
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_touch() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"touch mykey 3600\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "touch");
            assert_eq!(cmd.exptime, 3600);
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_stats() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"stats\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "stats");
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_flush_all() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"flush_all\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "flush_all");
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_version() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"version\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "version");
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_quit() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"quit\r\n");

    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "quit");
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn ascii_parser_incomplete() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"get mykey");
    assert!(matches!(result, ParseResult::Incomplete));
}

#[test]
fn ascii_parser_unknown_command() {
    let mut parser = AsciiParser::new();
    let result = parser.parse(b"unknown command\r\n");
    assert!(matches!(result, ParseResult::Error(_)));
}

// ============================================================================
// ASCII Encoder Tests
// ============================================================================

#[test]
fn ascii_encoder_stored() {
    let response = MemcachedResponse::stored();
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"STORED\r\n");
}

#[test]
fn ascii_encoder_not_stored() {
    let response = MemcachedResponse::not_stored();
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"NOT_STORED\r\n");
}

#[test]
fn ascii_encoder_exists() {
    let response = MemcachedResponse::exists();
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"EXISTS\r\n");
}

#[test]
fn ascii_encoder_not_found() {
    let response = MemcachedResponse::not_found();
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"NOT_FOUND\r\n");
}

#[test]
fn ascii_encoder_deleted() {
    let response = MemcachedResponse::deleted();
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"DELETED\r\n");
}

#[test]
fn ascii_encoder_touched() {
    let response = MemcachedResponse::touched();
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"TOUCHED\r\n");
}

#[test]
fn ascii_encoder_error() {
    let response = MemcachedResponse::error();
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"ERROR\r\n");
}

#[test]
fn ascii_encoder_client_error() {
    let response = MemcachedResponse::client_error("bad format");
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"CLIENT_ERROR bad format\r\n");
}

#[test]
fn ascii_encoder_server_error() {
    let response = MemcachedResponse::server_error("internal error");
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"SERVER_ERROR internal error\r\n");
}

#[test]
fn ascii_encoder_value() {
    let response = MemcachedResponse::value(
        Bytes::from_static(b"mykey"),
        0,
        Bytes::from_static(b"myvalue"),
    );
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"VALUE mykey 0 7\r\nmyvalue\r\nEND\r\n");
}

#[test]
fn ascii_encoder_value_with_cas() {
    let response = MemcachedResponse::value_with_cas(
        Bytes::from_static(b"mykey"),
        0,
        Bytes::from_static(b"myvalue"),
        12345,
    );
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"VALUE mykey 0 7 12345\r\nmyvalue\r\nEND\r\n");
}

#[test]
fn ascii_encoder_numeric() {
    let response = MemcachedResponse::Numeric(42);
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"42\r\n");
}

#[test]
fn ascii_encoder_version() {
    let response = MemcachedResponse::Version("1.0.0".to_string());
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"VERSION 1.0.0\r\n");
}

#[test]
fn ascii_encoder_ok() {
    let response = MemcachedResponse::ok();
    let encoded = AsciiEncoder::encode(&response);
    assert_eq!(encoded, b"OK\r\n");
}

// ============================================================================
// Binary Protocol Parser Tests
// ============================================================================

#[test]
fn binary_parser_get() {
    let parser = BinaryParser::new();

    // Build a binary GET request
    let mut request = vec![0u8; 24];
    request[0] = BINARY_REQUEST_MAGIC;
    request[1] = BinaryOpcode::Get as u8;
    request[2] = 0; // key length high
    request[3] = 5; // key length low = 5
                    // extras length = 0
                    // data type = 0
                    // vbucket = 0
    request[8] = 0; // total body length
    request[9] = 0;
    request[10] = 0;
    request[11] = 5;
    request.extend_from_slice(b"mykey");

    let result = parser.parse(&request);
    match result {
        ParseResult::Ok(cmd, consumed) => {
            assert_eq!(cmd.name, "get");
            assert_eq!(cmd.key_str(), Some("mykey"));
            assert_eq!(consumed, 29);
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn binary_parser_set() {
    let parser = BinaryParser::new();

    // Build a binary SET request
    let mut request = vec![0u8; 24];
    request[0] = BINARY_REQUEST_MAGIC;
    request[1] = BinaryOpcode::Set as u8;
    request[2] = 0; // key length high
    request[3] = 5; // key length low = 5
    request[4] = 8; // extras length = 8 (flags + exptime)
                    // data type = 0
                    // vbucket = 0
    request[8] = 0; // total body length = 8 + 5 + 7 = 20
    request[9] = 0;
    request[10] = 0;
    request[11] = 20;

    // Extras: flags (4 bytes) + exptime (4 bytes)
    request.extend_from_slice(&[0, 0, 0, 0]); // flags = 0
    request.extend_from_slice(&[0, 0, 0x0e, 0x10]); // exptime = 3600

    // Key
    request.extend_from_slice(b"mykey");

    // Value
    request.extend_from_slice(b"myvalue");

    let result = parser.parse(&request);
    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "set");
            assert_eq!(cmd.key_str(), Some("mykey"));
            assert!(cmd.data.is_some());
            assert_eq!(cmd.exptime, 3600);
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn binary_parser_delete() {
    let parser = BinaryParser::new();

    let mut request = vec![0u8; 24];
    request[0] = BINARY_REQUEST_MAGIC;
    request[1] = BinaryOpcode::Delete as u8;
    request[2] = 0;
    request[3] = 5;
    request[8] = 0;
    request[9] = 0;
    request[10] = 0;
    request[11] = 5;
    request.extend_from_slice(b"mykey");

    let result = parser.parse(&request);
    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "delete");
            assert_eq!(cmd.key_str(), Some("mykey"));
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn binary_parser_increment() {
    let parser = BinaryParser::new();

    let mut request = vec![0u8; 24];
    request[0] = BINARY_REQUEST_MAGIC;
    request[1] = BinaryOpcode::Increment as u8;
    request[2] = 0;
    request[3] = 7; // key length = 7
    request[4] = 20; // extras length = 20 (delta:8 + initial:8 + exptime:4)
    request[8] = 0;
    request[9] = 0;
    request[10] = 0;
    request[11] = 27; // total body = 20 + 7

    // Extras: delta (8) + initial (8) + exptime (4)
    request.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 10]); // delta = 10
    request.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // initial = 0
    request.extend_from_slice(&[0, 0, 0, 0]); // exptime = 0

    request.extend_from_slice(b"counter");

    let result = parser.parse(&request);
    match result {
        ParseResult::Ok(cmd, _) => {
            assert_eq!(cmd.name, "incr");
            assert_eq!(cmd.key_str(), Some("counter"));
            assert_eq!(cmd.delta, Some(10));
        }
        _ => panic!("Expected Ok result"),
    }
}

#[test]
fn binary_parser_incomplete() {
    let parser = BinaryParser::new();
    let result = parser.parse(&[BINARY_REQUEST_MAGIC]);
    assert!(matches!(result, ParseResult::Incomplete));
}

#[test]
fn binary_parser_invalid_magic() {
    let parser = BinaryParser::new();
    let mut request = vec![0u8; 24];
    request[0] = 0xFF; // Invalid magic
    let result = parser.parse(&request);
    assert!(matches!(result, ParseResult::Error(_)));
}

// ============================================================================
// Binary Encoder Tests
// ============================================================================

#[test]
fn binary_encoder_stored() {
    let response = MemcachedResponse::stored();
    let encoded = BinaryEncoder::encode(&response, BinaryOpcode::Set as u8, 12345, 0);

    assert_eq!(encoded[0], BINARY_RESPONSE_MAGIC);
    assert_eq!(encoded[1], BinaryOpcode::Set as u8);
    let status = u16::from_be_bytes([encoded[6], encoded[7]]);
    assert_eq!(status, BinaryStatus::NoError as u16);
    let opaque = u32::from_be_bytes([encoded[12], encoded[13], encoded[14], encoded[15]]);
    assert_eq!(opaque, 12345);
}

#[test]
fn binary_encoder_not_found() {
    let response = MemcachedResponse::not_found();
    let encoded = BinaryEncoder::encode(&response, BinaryOpcode::Get as u8, 0, 0);

    let status = u16::from_be_bytes([encoded[6], encoded[7]]);
    assert_eq!(status, BinaryStatus::KeyNotFound as u16);
}

#[test]
fn binary_encoder_exists() {
    let response = MemcachedResponse::exists();
    let encoded = BinaryEncoder::encode(&response, BinaryOpcode::Add as u8, 0, 0);

    let status = u16::from_be_bytes([encoded[6], encoded[7]]);
    assert_eq!(status, BinaryStatus::KeyExists as u16);
}

#[test]
fn binary_encoder_value() {
    let response = MemcachedResponse::value_with_cas(
        Bytes::from_static(b"mykey"),
        42,
        Bytes::from_static(b"myvalue"),
        99999,
    );
    let encoded = BinaryEncoder::encode(&response, BinaryOpcode::Get as u8, 0, 99999);

    assert_eq!(encoded[0], BINARY_RESPONSE_MAGIC);
    // Check extras length = 4 (flags)
    assert_eq!(encoded[4], 4);
    // Check body contains value
    assert!(encoded.len() > 24);
}

#[test]
fn binary_encoder_numeric() {
    let response = MemcachedResponse::Numeric(42);
    let encoded = BinaryEncoder::encode(&response, BinaryOpcode::Increment as u8, 0, 0);

    assert_eq!(encoded[0], BINARY_RESPONSE_MAGIC);
    // Counter value is in the body as 8-byte big-endian
    let body_len = u32::from_be_bytes([encoded[8], encoded[9], encoded[10], encoded[11]]);
    assert_eq!(body_len, 8);
    let counter = u64::from_be_bytes([
        encoded[24],
        encoded[25],
        encoded[26],
        encoded[27],
        encoded[28],
        encoded[29],
        encoded[30],
        encoded[31],
    ]);
    assert_eq!(counter, 42);
}

// ============================================================================
// Unified Codec Tests
// ============================================================================

#[test]
fn codec_auto_detect_ascii() {
    let mut codec = MemcachedCodec::new();
    let result = codec.parse(b"get mykey\r\n");

    assert!(matches!(result, ParseResult::Ok(_, _)));
    assert_eq!(codec.protocol(), Some(ProtocolType::Ascii));
}

#[test]
fn codec_auto_detect_binary() {
    let mut codec = MemcachedCodec::new();

    let mut request = vec![0u8; 24];
    request[0] = BINARY_REQUEST_MAGIC;
    request[1] = BinaryOpcode::Get as u8;
    request[3] = 5;
    request[11] = 5;
    request.extend_from_slice(b"mykey");

    let result = codec.parse(&request);
    assert!(matches!(result, ParseResult::Ok(_, _)));
    assert_eq!(codec.protocol(), Some(ProtocolType::Binary));
}

#[test]
fn codec_explicit_protocol() {
    let mut codec = MemcachedCodec::with_protocol(ProtocolType::Ascii);
    assert_eq!(codec.protocol(), Some(ProtocolType::Ascii));

    let result = codec.parse(b"get key\r\n");
    assert!(matches!(result, ParseResult::Ok(_, _)));
}

#[test]
fn codec_encode_ascii() {
    let codec = MemcachedCodec::with_protocol(ProtocolType::Ascii);
    let response = MemcachedResponse::stored();
    let encoded = codec.encode(&response, 0);
    assert_eq!(encoded, b"STORED\r\n");
}

#[test]
fn codec_encode_binary() {
    let mut codec = MemcachedCodec::with_protocol(ProtocolType::Binary);

    // Parse a command first to set up the opaque
    let mut request = vec![0u8; 24];
    request[0] = BINARY_REQUEST_MAGIC;
    request[1] = BinaryOpcode::Set as u8;
    request[3] = 5;
    request[4] = 8;
    request[11] = 20;
    request.extend_from_slice(&[0; 8]); // extras
    request.extend_from_slice(b"mykey");
    request.extend_from_slice(b"myvalue");
    let _ = codec.parse(&request);

    let response = MemcachedResponse::stored();
    let encoded = codec.encode(&response, 0);
    assert_eq!(encoded[0], BINARY_RESPONSE_MAGIC);
}

#[test]
fn codec_reset() {
    let mut codec = MemcachedCodec::new();
    let _ = codec.parse(b"get key\r\n");
    assert!(codec.protocol().is_some());

    codec.reset_parser();
    // Protocol detection is preserved, only parser state is reset
}

// ============================================================================
// Protocol Type Tests
// ============================================================================

#[test]
fn protocol_type_variants() {
    assert!(matches!(ProtocolType::Ascii, ProtocolType::Ascii));
    assert!(matches!(ProtocolType::Binary, ProtocolType::Binary));
}

#[test]
fn protocol_constants() {
    assert_eq!(BINARY_REQUEST_MAGIC, 0x80);
    assert_eq!(BINARY_RESPONSE_MAGIC, 0x81);
}

// ============================================================================
// Value Response Tests
// ============================================================================

#[test]
fn value_response_structure() {
    let value = ValueResponse {
        key: Bytes::from_static(b"testkey"),
        flags: 123,
        data: Bytes::from_static(b"testdata"),
        cas: Some(456),
    };

    assert_eq!(value.key, Bytes::from_static(b"testkey"));
    assert_eq!(value.flags, 123);
    assert_eq!(value.data, Bytes::from_static(b"testdata"));
    assert_eq!(value.cas, Some(456));
}

#[test]
fn value_response_without_cas() {
    let value = ValueResponse {
        key: Bytes::from_static(b"key"),
        flags: 0,
        data: Bytes::from_static(b"data"),
        cas: None,
    };

    assert!(value.cas.is_none());
}

// ============================================================================
// MemcachedCommand Tests
// ============================================================================

#[test]
fn memcached_command_builder() {
    let cmd = MemcachedCommand::new("set")
        .with_key(Bytes::from_static(b"mykey"))
        .with_data(Bytes::from_static(b"myvalue"))
        .with_flags(42)
        .with_exptime(3600);

    assert_eq!(cmd.name, "set");
    assert_eq!(cmd.key_str(), Some("mykey"));
    assert_eq!(cmd.flags, 42);
    assert_eq!(cmd.exptime, 3600);
    assert!(cmd.is_storage());
    assert!(!cmd.is_retrieval());
}

#[test]
fn memcached_command_multi_key() {
    let cmd = MemcachedCommand::new("get")
        .with_key(Bytes::from_static(b"key1"))
        .add_key(Bytes::from_static(b"key2"))
        .add_key(Bytes::from_static(b"key3"));

    let keys: Vec<_> = cmd.all_keys().collect();
    assert_eq!(keys.len(), 3);
}

#[test]
fn memcached_command_classification() {
    let get = MemcachedCommand::new("get");
    assert!(get.is_retrieval());
    assert!(!get.is_storage());

    let set = MemcachedCommand::new("set");
    assert!(set.is_storage());
    assert!(!set.is_retrieval());

    let cas = MemcachedCommand::new("cas");
    assert!(cas.requires_cas());
    assert!(cas.requires_linearizable());

    let incr = MemcachedCommand::new("incr");
    assert!(incr.requires_linearizable());
}
