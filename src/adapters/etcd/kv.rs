//! etcd KV operations.
//!
//! Implements Range (Get), Put, and Delete per §10.1.3 and §10.1.4.
//! - Linearizable reads require can_linearize=true with ReadIndex fence
//! - serializable=true maps to snapshot-only semantics
//! - Multi-KPG ranges are rejected for linearizable reads in v0.1

use serde::{Deserialize, Serialize};

/// Range request (Get).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeRequest {
    /// Key to get or start of range.
    pub key: Vec<u8>,
    /// End of range (exclusive). Empty for single key.
    pub range_end: Vec<u8>,
    /// Maximum number of keys to return.
    pub limit: i64,
    /// Revision to read at (0 for latest).
    pub revision: i64,
    /// Sort order.
    pub sort_order: SortOrder,
    /// Sort target.
    pub sort_target: SortTarget,
    /// Serializable read (snapshot-only if true).
    pub serializable: bool,
    /// Only return keys, not values.
    pub keys_only: bool,
    /// Only return count of keys.
    pub count_only: bool,
    /// Minimum mod_revision filter.
    pub min_mod_revision: i64,
    /// Maximum mod_revision filter.
    pub max_mod_revision: i64,
    /// Minimum create_revision filter.
    pub min_create_revision: i64,
    /// Maximum create_revision filter.
    pub max_create_revision: i64,
}

impl Default for RangeRequest {
    fn default() -> Self {
        Self {
            key: Vec::new(),
            range_end: Vec::new(),
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
        }
    }
}

/// Sort order for range results.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum SortOrder {
    #[default]
    None,
    Ascend,
    Descend,
}

/// Sort target for range results.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum SortTarget {
    #[default]
    Key,
    Version,
    Create,
    Mod,
    Value,
}

/// Range response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeResponse {
    /// Response header.
    pub header: ResponseHeader,
    /// Key-value pairs.
    pub kvs: Vec<KeyValue>,
    /// More results available (pagination).
    pub more: bool,
    /// Total count of keys matching the range.
    pub count: i64,
}

/// Put request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutRequest {
    /// Key to put.
    pub key: Vec<u8>,
    /// Value to put.
    pub value: Vec<u8>,
    /// Lease ID to attach.
    pub lease: i64,
    /// Return previous key-value.
    pub prev_kv: bool,
    /// Ignore value (update lease only).
    pub ignore_value: bool,
    /// Ignore lease (update value only).
    pub ignore_lease: bool,
}

/// Put response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutResponse {
    /// Response header.
    pub header: ResponseHeader,
    /// Previous key-value (if requested).
    pub prev_kv: Option<KeyValue>,
}

/// Delete request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRangeRequest {
    /// Key to delete or start of range.
    pub key: Vec<u8>,
    /// End of range (exclusive). Empty for single key.
    pub range_end: Vec<u8>,
    /// Return previous key-values.
    pub prev_kv: bool,
}

/// Delete response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRangeResponse {
    /// Response header.
    pub header: ResponseHeader,
    /// Number of keys deleted.
    pub deleted: i64,
    /// Previous key-values (if requested).
    pub prev_kvs: Vec<KeyValue>,
}

/// Response header included in all etcd responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseHeader {
    /// Cluster ID.
    pub cluster_id: u64,
    /// Member ID.
    pub member_id: u64,
    /// Revision at which the operation was performed.
    pub revision: i64,
    /// Raft term.
    pub raft_term: u64,
}

/// Key-value pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValue {
    /// Key.
    pub key: Vec<u8>,
    /// Revision when created.
    pub create_revision: i64,
    /// Revision of last modification.
    pub mod_revision: i64,
    /// Version (number of modifications).
    pub version: i64,
    /// Value.
    pub value: Vec<u8>,
    /// Attached lease ID.
    pub lease: i64,
}
