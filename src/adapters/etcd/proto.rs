//! Protobuf encoding for etcd types.
//!
//! This module provides manual prost::Message implementations for etcd types
//! to enable gRPC communication with etcdctl without proto codegen.

use prost::{DecodeError, Message};

// ============================================================================
// ResponseHeader
// ============================================================================

/// Wire-format ResponseHeader matching etcd's etcdserverpb.ResponseHeader.
#[derive(Clone, Default, Debug)]
pub struct ResponseHeader {
    pub cluster_id: u64, // field 1
    pub member_id: u64,  // field 2
    pub revision: i64,   // field 3
    pub raft_term: u64,  // field 4
}

impl Message for ResponseHeader {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
    where
        Self: Sized,
    {
        if self.cluster_id != 0 {
            prost::encoding::uint64::encode(1, &self.cluster_id, buf);
        }
        if self.member_id != 0 {
            prost::encoding::uint64::encode(2, &self.member_id, buf);
        }
        if self.revision != 0 {
            prost::encoding::int64::encode(3, &self.revision, buf);
        }
        if self.raft_term != 0 {
            prost::encoding::uint64::encode(4, &self.raft_term, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        match tag {
            1 => prost::encoding::uint64::merge(wire_type, &mut self.cluster_id, buf, ctx),
            2 => prost::encoding::uint64::merge(wire_type, &mut self.member_id, buf, ctx),
            3 => prost::encoding::int64::merge(wire_type, &mut self.revision, buf, ctx),
            4 => prost::encoding::uint64::merge(wire_type, &mut self.raft_term, buf, ctx),
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if self.cluster_id != 0 {
            len += prost::encoding::uint64::encoded_len(1, &self.cluster_id);
        }
        if self.member_id != 0 {
            len += prost::encoding::uint64::encoded_len(2, &self.member_id);
        }
        if self.revision != 0 {
            len += prost::encoding::int64::encoded_len(3, &self.revision);
        }
        if self.raft_term != 0 {
            len += prost::encoding::uint64::encoded_len(4, &self.raft_term);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// KeyValue
// ============================================================================

/// Wire-format KeyValue matching etcd's mvccpb.KeyValue.
#[derive(Clone, Default, Debug)]
pub struct KeyValue {
    pub key: Vec<u8>,         // field 1
    pub create_revision: i64, // field 2
    pub mod_revision: i64,    // field 3
    pub version: i64,         // field 4
    pub value: Vec<u8>,       // field 5
    pub lease: i64,           // field 6
}

impl Message for KeyValue {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
    where
        Self: Sized,
    {
        if !self.key.is_empty() {
            prost::encoding::bytes::encode(1, &self.key, buf);
        }
        if self.create_revision != 0 {
            prost::encoding::int64::encode(2, &self.create_revision, buf);
        }
        if self.mod_revision != 0 {
            prost::encoding::int64::encode(3, &self.mod_revision, buf);
        }
        if self.version != 0 {
            prost::encoding::int64::encode(4, &self.version, buf);
        }
        if !self.value.is_empty() {
            prost::encoding::bytes::encode(5, &self.value, buf);
        }
        if self.lease != 0 {
            prost::encoding::int64::encode(6, &self.lease, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        match tag {
            1 => prost::encoding::bytes::merge(wire_type, &mut self.key, buf, ctx),
            2 => prost::encoding::int64::merge(wire_type, &mut self.create_revision, buf, ctx),
            3 => prost::encoding::int64::merge(wire_type, &mut self.mod_revision, buf, ctx),
            4 => prost::encoding::int64::merge(wire_type, &mut self.version, buf, ctx),
            5 => prost::encoding::bytes::merge(wire_type, &mut self.value, buf, ctx),
            6 => prost::encoding::int64::merge(wire_type, &mut self.lease, buf, ctx),
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if !self.key.is_empty() {
            len += prost::encoding::bytes::encoded_len(1, &self.key);
        }
        if self.create_revision != 0 {
            len += prost::encoding::int64::encoded_len(2, &self.create_revision);
        }
        if self.mod_revision != 0 {
            len += prost::encoding::int64::encoded_len(3, &self.mod_revision);
        }
        if self.version != 0 {
            len += prost::encoding::int64::encoded_len(4, &self.version);
        }
        if !self.value.is_empty() {
            len += prost::encoding::bytes::encoded_len(5, &self.value);
        }
        if self.lease != 0 {
            len += prost::encoding::int64::encoded_len(6, &self.lease);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// RangeRequest
// ============================================================================

/// Wire-format RangeRequest matching etcd's etcdserverpb.RangeRequest.
#[derive(Clone, Default, Debug)]
pub struct RangeRequest {
    pub key: Vec<u8>,             // field 1
    pub range_end: Vec<u8>,       // field 2
    pub limit: i64,               // field 3
    pub revision: i64,            // field 4
    pub sort_order: i32,          // field 5 (enum)
    pub sort_target: i32,         // field 6 (enum)
    pub serializable: bool,       // field 7
    pub keys_only: bool,          // field 8
    pub count_only: bool,         // field 9
    pub min_mod_revision: i64,    // field 10
    pub max_mod_revision: i64,    // field 11
    pub min_create_revision: i64, // field 12
    pub max_create_revision: i64, // field 13
}

impl Message for RangeRequest {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
    where
        Self: Sized,
    {
        if !self.key.is_empty() {
            prost::encoding::bytes::encode(1, &self.key, buf);
        }
        if !self.range_end.is_empty() {
            prost::encoding::bytes::encode(2, &self.range_end, buf);
        }
        if self.limit != 0 {
            prost::encoding::int64::encode(3, &self.limit, buf);
        }
        if self.revision != 0 {
            prost::encoding::int64::encode(4, &self.revision, buf);
        }
        if self.sort_order != 0 {
            prost::encoding::int32::encode(5, &self.sort_order, buf);
        }
        if self.sort_target != 0 {
            prost::encoding::int32::encode(6, &self.sort_target, buf);
        }
        if self.serializable {
            prost::encoding::bool::encode(7, &self.serializable, buf);
        }
        if self.keys_only {
            prost::encoding::bool::encode(8, &self.keys_only, buf);
        }
        if self.count_only {
            prost::encoding::bool::encode(9, &self.count_only, buf);
        }
        if self.min_mod_revision != 0 {
            prost::encoding::int64::encode(10, &self.min_mod_revision, buf);
        }
        if self.max_mod_revision != 0 {
            prost::encoding::int64::encode(11, &self.max_mod_revision, buf);
        }
        if self.min_create_revision != 0 {
            prost::encoding::int64::encode(12, &self.min_create_revision, buf);
        }
        if self.max_create_revision != 0 {
            prost::encoding::int64::encode(13, &self.max_create_revision, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        match tag {
            1 => prost::encoding::bytes::merge(wire_type, &mut self.key, buf, ctx),
            2 => prost::encoding::bytes::merge(wire_type, &mut self.range_end, buf, ctx),
            3 => prost::encoding::int64::merge(wire_type, &mut self.limit, buf, ctx),
            4 => prost::encoding::int64::merge(wire_type, &mut self.revision, buf, ctx),
            5 => prost::encoding::int32::merge(wire_type, &mut self.sort_order, buf, ctx),
            6 => prost::encoding::int32::merge(wire_type, &mut self.sort_target, buf, ctx),
            7 => prost::encoding::bool::merge(wire_type, &mut self.serializable, buf, ctx),
            8 => prost::encoding::bool::merge(wire_type, &mut self.keys_only, buf, ctx),
            9 => prost::encoding::bool::merge(wire_type, &mut self.count_only, buf, ctx),
            10 => prost::encoding::int64::merge(wire_type, &mut self.min_mod_revision, buf, ctx),
            11 => prost::encoding::int64::merge(wire_type, &mut self.max_mod_revision, buf, ctx),
            12 => prost::encoding::int64::merge(wire_type, &mut self.min_create_revision, buf, ctx),
            13 => prost::encoding::int64::merge(wire_type, &mut self.max_create_revision, buf, ctx),
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if !self.key.is_empty() {
            len += prost::encoding::bytes::encoded_len(1, &self.key);
        }
        if !self.range_end.is_empty() {
            len += prost::encoding::bytes::encoded_len(2, &self.range_end);
        }
        if self.limit != 0 {
            len += prost::encoding::int64::encoded_len(3, &self.limit);
        }
        if self.revision != 0 {
            len += prost::encoding::int64::encoded_len(4, &self.revision);
        }
        if self.sort_order != 0 {
            len += prost::encoding::int32::encoded_len(5, &self.sort_order);
        }
        if self.sort_target != 0 {
            len += prost::encoding::int32::encoded_len(6, &self.sort_target);
        }
        if self.serializable {
            len += prost::encoding::bool::encoded_len(7, &self.serializable);
        }
        if self.keys_only {
            len += prost::encoding::bool::encoded_len(8, &self.keys_only);
        }
        if self.count_only {
            len += prost::encoding::bool::encoded_len(9, &self.count_only);
        }
        if self.min_mod_revision != 0 {
            len += prost::encoding::int64::encoded_len(10, &self.min_mod_revision);
        }
        if self.max_mod_revision != 0 {
            len += prost::encoding::int64::encoded_len(11, &self.max_mod_revision);
        }
        if self.min_create_revision != 0 {
            len += prost::encoding::int64::encoded_len(12, &self.min_create_revision);
        }
        if self.max_create_revision != 0 {
            len += prost::encoding::int64::encoded_len(13, &self.max_create_revision);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// RangeResponse
// ============================================================================

/// Wire-format RangeResponse matching etcd's etcdserverpb.RangeResponse.
#[derive(Clone, Default, Debug)]
pub struct RangeResponse {
    pub header: Option<ResponseHeader>, // field 1
    pub kvs: Vec<KeyValue>,             // field 2
    pub more: bool,                     // field 3
    pub count: i64,                     // field 4
}

impl Message for RangeResponse {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
    where
        Self: Sized,
    {
        if let Some(ref header) = self.header {
            prost::encoding::message::encode(1, header, buf);
        }
        for kv in &self.kvs {
            prost::encoding::message::encode(2, kv, buf);
        }
        if self.more {
            prost::encoding::bool::encode(3, &self.more, buf);
        }
        if self.count != 0 {
            prost::encoding::int64::encode(4, &self.count, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        match tag {
            1 => {
                let mut header = self.header.take().unwrap_or_default();
                prost::encoding::message::merge(wire_type, &mut header, buf, ctx)?;
                self.header = Some(header);
                Ok(())
            }
            2 => {
                let mut kv = KeyValue::default();
                prost::encoding::message::merge(wire_type, &mut kv, buf, ctx)?;
                self.kvs.push(kv);
                Ok(())
            }
            3 => prost::encoding::bool::merge(wire_type, &mut self.more, buf, ctx),
            4 => prost::encoding::int64::merge(wire_type, &mut self.count, buf, ctx),
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if let Some(ref header) = self.header {
            len += prost::encoding::message::encoded_len(1, header);
        }
        for kv in &self.kvs {
            len += prost::encoding::message::encoded_len(2, kv);
        }
        if self.more {
            len += prost::encoding::bool::encoded_len(3, &self.more);
        }
        if self.count != 0 {
            len += prost::encoding::int64::encoded_len(4, &self.count);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// PutRequest
// ============================================================================

/// Wire-format PutRequest matching etcd's etcdserverpb.PutRequest.
#[derive(Clone, Default, Debug)]
pub struct PutRequest {
    pub key: Vec<u8>,       // field 1
    pub value: Vec<u8>,     // field 2
    pub lease: i64,         // field 3
    pub prev_kv: bool,      // field 4
    pub ignore_value: bool, // field 5
    pub ignore_lease: bool, // field 6
}

impl Message for PutRequest {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
    where
        Self: Sized,
    {
        if !self.key.is_empty() {
            prost::encoding::bytes::encode(1, &self.key, buf);
        }
        if !self.value.is_empty() {
            prost::encoding::bytes::encode(2, &self.value, buf);
        }
        if self.lease != 0 {
            prost::encoding::int64::encode(3, &self.lease, buf);
        }
        if self.prev_kv {
            prost::encoding::bool::encode(4, &self.prev_kv, buf);
        }
        if self.ignore_value {
            prost::encoding::bool::encode(5, &self.ignore_value, buf);
        }
        if self.ignore_lease {
            prost::encoding::bool::encode(6, &self.ignore_lease, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        match tag {
            1 => prost::encoding::bytes::merge(wire_type, &mut self.key, buf, ctx),
            2 => prost::encoding::bytes::merge(wire_type, &mut self.value, buf, ctx),
            3 => prost::encoding::int64::merge(wire_type, &mut self.lease, buf, ctx),
            4 => prost::encoding::bool::merge(wire_type, &mut self.prev_kv, buf, ctx),
            5 => prost::encoding::bool::merge(wire_type, &mut self.ignore_value, buf, ctx),
            6 => prost::encoding::bool::merge(wire_type, &mut self.ignore_lease, buf, ctx),
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if !self.key.is_empty() {
            len += prost::encoding::bytes::encoded_len(1, &self.key);
        }
        if !self.value.is_empty() {
            len += prost::encoding::bytes::encoded_len(2, &self.value);
        }
        if self.lease != 0 {
            len += prost::encoding::int64::encoded_len(3, &self.lease);
        }
        if self.prev_kv {
            len += prost::encoding::bool::encoded_len(4, &self.prev_kv);
        }
        if self.ignore_value {
            len += prost::encoding::bool::encoded_len(5, &self.ignore_value);
        }
        if self.ignore_lease {
            len += prost::encoding::bool::encoded_len(6, &self.ignore_lease);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// PutResponse
// ============================================================================

/// Wire-format PutResponse matching etcd's etcdserverpb.PutResponse.
#[derive(Clone, Default, Debug)]
pub struct PutResponse {
    pub header: Option<ResponseHeader>, // field 1
    pub prev_kv: Option<KeyValue>,      // field 2
}

impl Message for PutResponse {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
    where
        Self: Sized,
    {
        if let Some(ref header) = self.header {
            prost::encoding::message::encode(1, header, buf);
        }
        if let Some(ref prev_kv) = self.prev_kv {
            prost::encoding::message::encode(2, prev_kv, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        match tag {
            1 => {
                let mut header = self.header.take().unwrap_or_default();
                prost::encoding::message::merge(wire_type, &mut header, buf, ctx)?;
                self.header = Some(header);
                Ok(())
            }
            2 => {
                let mut kv = self.prev_kv.take().unwrap_or_default();
                prost::encoding::message::merge(wire_type, &mut kv, buf, ctx)?;
                self.prev_kv = Some(kv);
                Ok(())
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if let Some(ref header) = self.header {
            len += prost::encoding::message::encoded_len(1, header);
        }
        if let Some(ref prev_kv) = self.prev_kv {
            len += prost::encoding::message::encoded_len(2, prev_kv);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// DeleteRangeRequest
// ============================================================================

/// Wire-format DeleteRangeRequest matching etcd's etcdserverpb.DeleteRangeRequest.
#[derive(Clone, Default, Debug)]
pub struct DeleteRangeRequest {
    pub key: Vec<u8>,       // field 1
    pub range_end: Vec<u8>, // field 2
    pub prev_kv: bool,      // field 3
}

impl Message for DeleteRangeRequest {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
    where
        Self: Sized,
    {
        if !self.key.is_empty() {
            prost::encoding::bytes::encode(1, &self.key, buf);
        }
        if !self.range_end.is_empty() {
            prost::encoding::bytes::encode(2, &self.range_end, buf);
        }
        if self.prev_kv {
            prost::encoding::bool::encode(3, &self.prev_kv, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        match tag {
            1 => prost::encoding::bytes::merge(wire_type, &mut self.key, buf, ctx),
            2 => prost::encoding::bytes::merge(wire_type, &mut self.range_end, buf, ctx),
            3 => prost::encoding::bool::merge(wire_type, &mut self.prev_kv, buf, ctx),
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if !self.key.is_empty() {
            len += prost::encoding::bytes::encoded_len(1, &self.key);
        }
        if !self.range_end.is_empty() {
            len += prost::encoding::bytes::encoded_len(2, &self.range_end);
        }
        if self.prev_kv {
            len += prost::encoding::bool::encoded_len(3, &self.prev_kv);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

// ============================================================================
// DeleteRangeResponse
// ============================================================================

/// Wire-format DeleteRangeResponse matching etcd's etcdserverpb.DeleteRangeResponse.
#[derive(Clone, Default, Debug)]
pub struct DeleteRangeResponse {
    pub header: Option<ResponseHeader>, // field 1
    pub deleted: i64,                   // field 2
    pub prev_kvs: Vec<KeyValue>,        // field 3
}

impl Message for DeleteRangeResponse {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
    where
        Self: Sized,
    {
        if let Some(ref header) = self.header {
            prost::encoding::message::encode(1, header, buf);
        }
        if self.deleted != 0 {
            prost::encoding::int64::encode(2, &self.deleted, buf);
        }
        for kv in &self.prev_kvs {
            prost::encoding::message::encode(3, kv, buf);
        }
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        match tag {
            1 => {
                let mut header = self.header.take().unwrap_or_default();
                prost::encoding::message::merge(wire_type, &mut header, buf, ctx)?;
                self.header = Some(header);
                Ok(())
            }
            2 => prost::encoding::int64::merge(wire_type, &mut self.deleted, buf, ctx),
            3 => {
                let mut kv = KeyValue::default();
                prost::encoding::message::merge(wire_type, &mut kv, buf, ctx)?;
                self.prev_kvs.push(kv);
                Ok(())
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        if let Some(ref header) = self.header {
            len += prost::encoding::message::encoded_len(1, header);
        }
        if self.deleted != 0 {
            len += prost::encoding::int64::encoded_len(2, &self.deleted);
        }
        for kv in &self.prev_kvs {
            len += prost::encoding::message::encoded_len(3, kv);
        }
        len
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_request_roundtrip() {
        let req = RangeRequest {
            key: b"foo".to_vec(),
            range_end: vec![],
            limit: 10,
            ..Default::default()
        };

        let encoded = req.encode_to_vec();
        let decoded = RangeRequest::decode(&encoded[..]).unwrap();

        assert_eq!(decoded.key, b"foo");
        assert_eq!(decoded.limit, 10);
    }

    #[test]
    fn test_put_request_roundtrip() {
        let req = PutRequest {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            lease: 123,
            prev_kv: true,
            ..Default::default()
        };

        let encoded = req.encode_to_vec();
        let decoded = PutRequest::decode(&encoded[..]).unwrap();

        assert_eq!(decoded.key, b"key");
        assert_eq!(decoded.value, b"value");
        assert_eq!(decoded.lease, 123);
        assert!(decoded.prev_kv);
    }

    #[test]
    fn test_range_response_roundtrip() {
        let resp = RangeResponse {
            header: Some(ResponseHeader {
                cluster_id: 1,
                member_id: 2,
                revision: 100,
                raft_term: 5,
            }),
            kvs: vec![KeyValue {
                key: b"foo".to_vec(),
                value: b"bar".to_vec(),
                create_revision: 50,
                mod_revision: 100,
                version: 3,
                lease: 0,
            }],
            more: false,
            count: 1,
        };

        let encoded = resp.encode_to_vec();
        let decoded = RangeResponse::decode(&encoded[..]).unwrap();

        assert_eq!(decoded.header.as_ref().unwrap().revision, 100);
        assert_eq!(decoded.kvs.len(), 1);
        assert_eq!(decoded.kvs[0].key, b"foo");
        assert_eq!(decoded.count, 1);
    }
}
