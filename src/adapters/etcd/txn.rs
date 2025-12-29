//! etcd transaction operations.
//!
//! Implements Txn (Compare/Then/Else) per §10.1.5.
//! - Transactions evaluated at single revision fence (CAS-FENCE)
//! - Cross-KPG Txn rejected with TxnCrossShardUnsupported
//! - Under LIN-BOUND failure, fail closed with UNAVAILABLE

use super::kv::{
    DeleteRangeRequest, DeleteRangeResponse, KeyValue, PutRequest, PutResponse, RangeRequest,
    RangeResponse, ResponseHeader,
};
use serde::{Deserialize, Serialize};

/// Transaction request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnRequest {
    /// Compare predicates (all must pass for success branch).
    pub compare: Vec<Compare>,
    /// Operations to execute if all compares succeed.
    pub success: Vec<RequestOp>,
    /// Operations to execute if any compare fails.
    pub failure: Vec<RequestOp>,
}

/// Compare predicate for transactions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Compare {
    /// Compare result type.
    pub result: CompareResult,
    /// Compare target type.
    pub target: CompareTarget,
    /// Key to compare.
    pub key: Vec<u8>,
    /// Value for comparison (interpretation depends on target).
    pub target_union: CompareTargetUnion,
    /// Range end for multi-key compares.
    pub range_end: Vec<u8>,
}

/// Compare result type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompareResult {
    Equal,
    Greater,
    Less,
    NotEqual,
}

/// Compare target type.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompareTarget {
    Version,
    Create,
    Mod,
    Value,
    Lease,
}

/// Compare target value union.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompareTargetUnion {
    /// Version to compare.
    Version(i64),
    /// Create revision to compare.
    CreateRevision(i64),
    /// Mod revision to compare.
    ModRevision(i64),
    /// Value to compare.
    Value(Vec<u8>),
    /// Lease to compare.
    Lease(i64),
}

/// Request operation within a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestOp {
    /// Range (Get) operation.
    Range(RangeRequest),
    /// Put operation.
    Put(PutRequest),
    /// Delete operation.
    DeleteRange(DeleteRangeRequest),
    /// Nested transaction.
    Txn(TxnRequest),
}

/// Response operation from a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseOp {
    /// Range response.
    Range(RangeResponse),
    /// Put response.
    Put(PutResponse),
    /// Delete response.
    DeleteRange(DeleteRangeResponse),
    /// Nested transaction response.
    Txn(TxnResponse),
}

/// Transaction response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnResponse {
    /// Response header.
    pub header: ResponseHeader,
    /// Whether the compare succeeded.
    pub succeeded: bool,
    /// Responses from executed operations.
    pub responses: Vec<ResponseOp>,
}

/// Evaluated compare result.
#[derive(Debug, Clone)]
pub struct EvaluatedCompare {
    /// The original compare.
    pub compare: Compare,
    /// Whether the compare passed.
    pub passed: bool,
    /// Actual value found (for debugging).
    pub actual_value: Option<CompareTargetUnion>,
}

impl Compare {
    /// Evaluate this compare against a key-value record.
    pub fn evaluate(&self, kv: Option<&KeyValue>) -> bool {
        match (&self.target, &self.target_union, kv) {
            // Version compare
            (CompareTarget::Version, CompareTargetUnion::Version(expected), Some(kv)) => {
                compare_values(kv.version, *expected, self.result)
            }
            (CompareTarget::Version, CompareTargetUnion::Version(expected), None) => {
                compare_values(0, *expected, self.result)
            }

            // Create revision compare
            (CompareTarget::Create, CompareTargetUnion::CreateRevision(expected), Some(kv)) => {
                compare_values(kv.create_revision, *expected, self.result)
            }
            (CompareTarget::Create, CompareTargetUnion::CreateRevision(expected), None) => {
                compare_values(0, *expected, self.result)
            }

            // Mod revision compare
            (CompareTarget::Mod, CompareTargetUnion::ModRevision(expected), Some(kv)) => {
                compare_values(kv.mod_revision, *expected, self.result)
            }
            (CompareTarget::Mod, CompareTargetUnion::ModRevision(expected), None) => {
                compare_values(0, *expected, self.result)
            }

            // Value compare
            (CompareTarget::Value, CompareTargetUnion::Value(expected), Some(kv)) => {
                compare_bytes(&kv.value, expected, self.result)
            }
            (CompareTarget::Value, CompareTargetUnion::Value(expected), None) => {
                compare_bytes(&[], expected, self.result)
            }

            // Lease compare
            (CompareTarget::Lease, CompareTargetUnion::Lease(expected), Some(kv)) => {
                compare_values(kv.lease, *expected, self.result)
            }
            (CompareTarget::Lease, CompareTargetUnion::Lease(expected), None) => {
                compare_values(0, *expected, self.result)
            }

            // Mismatched target and union
            _ => false,
        }
    }
}

fn compare_values(actual: i64, expected: i64, result: CompareResult) -> bool {
    match result {
        CompareResult::Equal => actual == expected,
        CompareResult::NotEqual => actual != expected,
        CompareResult::Greater => actual > expected,
        CompareResult::Less => actual < expected,
    }
}

fn compare_bytes(actual: &[u8], expected: &[u8], result: CompareResult) -> bool {
    match result {
        CompareResult::Equal => actual == expected,
        CompareResult::NotEqual => actual != expected,
        CompareResult::Greater => actual > expected,
        CompareResult::Less => actual < expected,
    }
}

// ============================================================================
// Transaction Validation (Task 101)
// ============================================================================

/// Transaction validation error.
#[derive(Debug, Clone)]
pub enum TxnValidationError {
    /// Empty transaction (no compares and no operations).
    EmptyTransaction,
    /// Too many operations in a single transaction.
    TooManyOperations { count: usize, max: usize },
    /// Nested transaction depth exceeded.
    NestingTooDeep { depth: usize, max: usize },
    /// Invalid compare predicate.
    InvalidCompare { reason: String },
    /// Invalid operation.
    InvalidOperation { reason: String },
}

impl std::fmt::Display for TxnValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyTransaction => write!(f, "empty transaction not allowed"),
            Self::TooManyOperations { count, max } => {
                write!(f, "too many operations: {} > {}", count, max)
            }
            Self::NestingTooDeep { depth, max } => {
                write!(f, "nesting too deep: {} > {}", depth, max)
            }
            Self::InvalidCompare { reason } => write!(f, "invalid compare: {}", reason),
            Self::InvalidOperation { reason } => write!(f, "invalid operation: {}", reason),
        }
    }
}

impl std::error::Error for TxnValidationError {}

/// Transaction validation configuration.
#[derive(Debug, Clone)]
pub struct TxnValidationConfig {
    /// Maximum number of operations in a transaction.
    pub max_operations: usize,
    /// Maximum nesting depth for nested transactions.
    pub max_nesting_depth: usize,
    /// Allow empty transactions.
    pub allow_empty: bool,
}

impl Default for TxnValidationConfig {
    fn default() -> Self {
        Self {
            max_operations: 128,
            max_nesting_depth: 2,
            allow_empty: false,
        }
    }
}

impl TxnRequest {
    /// Validate the transaction structure.
    pub fn validate(&self, config: &TxnValidationConfig) -> Result<(), TxnValidationError> {
        self.validate_depth(config, 0)
    }

    fn validate_depth(
        &self,
        config: &TxnValidationConfig,
        depth: usize,
    ) -> Result<(), TxnValidationError> {
        // Check nesting depth
        if depth > config.max_nesting_depth {
            return Err(TxnValidationError::NestingTooDeep {
                depth,
                max: config.max_nesting_depth,
            });
        }

        // Check empty transaction
        if !config.allow_empty
            && self.compare.is_empty()
            && self.success.is_empty()
            && self.failure.is_empty()
        {
            return Err(TxnValidationError::EmptyTransaction);
        }

        // Count total operations
        let op_count = self.success.len() + self.failure.len();
        if op_count > config.max_operations {
            return Err(TxnValidationError::TooManyOperations {
                count: op_count,
                max: config.max_operations,
            });
        }

        // Validate nested transactions
        for op in &self.success {
            if let RequestOp::Txn(nested) = op {
                nested.validate_depth(config, depth + 1)?;
            }
        }
        for op in &self.failure {
            if let RequestOp::Txn(nested) = op {
                nested.validate_depth(config, depth + 1)?;
            }
        }

        Ok(())
    }

    /// Extract all keys referenced in this transaction.
    ///
    /// Used for cross-KPG detection and routing validation.
    pub fn all_keys(&self) -> Vec<&[u8]> {
        let mut keys = Vec::new();

        // Keys from compares
        for compare in &self.compare {
            keys.push(compare.key.as_slice());
        }

        // Keys from success ops
        self.collect_op_keys(&self.success, &mut keys);

        // Keys from failure ops
        self.collect_op_keys(&self.failure, &mut keys);

        keys
    }

    fn collect_op_keys<'a>(&'a self, ops: &'a [RequestOp], keys: &mut Vec<&'a [u8]>) {
        for op in ops {
            match op {
                RequestOp::Range(req) => {
                    keys.push(&req.key);
                }
                RequestOp::Put(req) => {
                    keys.push(&req.key);
                }
                RequestOp::DeleteRange(req) => {
                    keys.push(&req.key);
                }
                RequestOp::Txn(nested) => {
                    keys.extend(nested.all_keys());
                }
            }
        }
    }
}

// ============================================================================
// Transaction Executor (Tasks 103-105, 108-110)
// ============================================================================

use crate::control::routing::RoutingTable;
use crate::core::error::{LatticeError, LatticeResult, LinearizabilityFailureReason};
use crate::kpg::idempotency::IdempotencyTable;

/// Transaction executor context.
///
/// Holds the state needed to execute a transaction including
/// revision fence and key-value lookup capability.
pub struct TxnExecutorContext<'a> {
    /// Current revision fence for CAS-FENCE evaluation.
    pub revision_fence: i64,

    /// Whether linearizable operations are available.
    pub can_linearize: bool,

    /// Routing table for cross-KPG detection.
    pub routing: Option<&'a RoutingTable>,

    /// Idempotency table for replay detection.
    pub idempotency: Option<&'a IdempotencyTable>,

    /// Cluster ID for response headers.
    pub cluster_id: u64,

    /// Member ID for response headers.
    pub member_id: u64,

    /// Raft term for response headers.
    pub raft_term: u64,
}

impl<'a> TxnExecutorContext<'a> {
    /// Create a new executor context.
    pub fn new(revision_fence: i64, can_linearize: bool) -> Self {
        Self {
            revision_fence,
            can_linearize,
            routing: None,
            idempotency: None,
            cluster_id: 0,
            member_id: 0,
            raft_term: 0,
        }
    }

    /// Set the routing table.
    pub fn with_routing(mut self, routing: &'a RoutingTable) -> Self {
        self.routing = Some(routing);
        self
    }

    /// Set the idempotency table.
    pub fn with_idempotency(mut self, idempotency: &'a IdempotencyTable) -> Self {
        self.idempotency = Some(idempotency);
        self
    }

    /// Set header information.
    pub fn with_header_info(mut self, cluster_id: u64, member_id: u64, raft_term: u64) -> Self {
        self.cluster_id = cluster_id;
        self.member_id = member_id;
        self.raft_term = raft_term;
        self
    }

    /// Create a response header at the current revision.
    pub fn make_header(&self) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision: self.revision_fence,
            raft_term: self.raft_term,
        }
    }
}

/// Transaction executor.
///
/// Executes transactions with CAS-FENCE semantics per §10.1.5.
pub struct TxnExecutor {
    /// Validation configuration.
    validation_config: TxnValidationConfig,
}

impl TxnExecutor {
    /// Create a new transaction executor.
    pub fn new() -> Self {
        Self {
            validation_config: TxnValidationConfig::default(),
        }
    }

    /// Create with custom validation config.
    pub fn with_config(validation_config: TxnValidationConfig) -> Self {
        Self { validation_config }
    }

    /// Execute a transaction.
    ///
    /// This evaluates all compares at the single revision fence (CAS-FENCE),
    /// then executes either the success or failure branch.
    pub fn execute<F>(
        &self,
        txn: &TxnRequest,
        ctx: &TxnExecutorContext,
        lookup_key: F,
    ) -> LatticeResult<TxnExecutorResult>
    where
        F: Fn(&[u8]) -> Option<KeyValue>,
    {
        // Validate transaction structure
        txn.validate(&self.validation_config)
            .map_err(|e| LatticeError::InvalidRequest {
                message: e.to_string(),
            })?;

        // Check LIN-BOUND for CAS operations
        if !txn.compare.is_empty() && !ctx.can_linearize {
            return Err(LatticeError::linearizability_unavailable(
                LinearizabilityFailureReason::NotLeader,
            ));
        }

        // Check for cross-KPG transaction
        if let Some(routing) = ctx.routing {
            let keys = txn.all_keys();
            routing.validate_single_kpg(&keys)?;
        }

        // Evaluate all compares at the revision fence
        let compare_results: Vec<EvaluatedCompare> = txn
            .compare
            .iter()
            .map(|c| {
                let kv = lookup_key(&c.key);
                let passed = c.evaluate(kv.as_ref());
                EvaluatedCompare {
                    compare: c.clone(),
                    passed,
                    actual_value: kv.map(|kv| match c.target {
                        CompareTarget::Version => CompareTargetUnion::Version(kv.version),
                        CompareTarget::Create => {
                            CompareTargetUnion::CreateRevision(kv.create_revision)
                        }
                        CompareTarget::Mod => CompareTargetUnion::ModRevision(kv.mod_revision),
                        CompareTarget::Value => CompareTargetUnion::Value(kv.value.clone()),
                        CompareTarget::Lease => CompareTargetUnion::Lease(kv.lease),
                    }),
                }
            })
            .collect();

        // Determine which branch to execute
        let all_passed = compare_results.iter().all(|c| c.passed);
        let branch = if all_passed {
            &txn.success
        } else {
            &txn.failure
        };

        // Collect operations to execute
        let operations: Vec<TxnOperation> = branch
            .iter()
            .map(|op| match op {
                RequestOp::Range(req) => TxnOperation::Range(req.clone()),
                RequestOp::Put(req) => TxnOperation::Put(req.clone()),
                RequestOp::DeleteRange(req) => TxnOperation::DeleteRange(req.clone()),
                RequestOp::Txn(nested) => TxnOperation::Txn(nested.clone()),
            })
            .collect();

        Ok(TxnExecutorResult {
            succeeded: all_passed,
            compare_results,
            operations,
            revision_fence: ctx.revision_fence,
        })
    }
}

impl Default for TxnExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of transaction execution.
#[derive(Debug, Clone)]
pub struct TxnExecutorResult {
    /// Whether all compares passed.
    pub succeeded: bool,
    /// Individual compare results.
    pub compare_results: Vec<EvaluatedCompare>,
    /// Operations to execute (from success or failure branch).
    pub operations: Vec<TxnOperation>,
    /// Revision fence used for evaluation.
    pub revision_fence: i64,
}

/// Operation to execute as part of a transaction.
#[derive(Debug, Clone)]
pub enum TxnOperation {
    /// Range (Get) operation.
    Range(RangeRequest),
    /// Put operation.
    Put(PutRequest),
    /// Delete operation.
    DeleteRange(DeleteRangeRequest),
    /// Nested transaction.
    Txn(TxnRequest),
}

// ============================================================================
// Idempotency Support (Task 110)
// ============================================================================

use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

/// Transaction idempotency key.
///
/// Used to detect and replay duplicate transaction requests.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TxnIdempotencyKey {
    /// Client ID.
    pub client_id: String,
    /// Request sequence number.
    pub seq: u64,
}

impl TxnIdempotencyKey {
    /// Create a new idempotency key.
    pub fn new(client_id: impl Into<String>, seq: u64) -> Self {
        Self {
            client_id: client_id.into(),
            seq,
        }
    }

    /// Compute a hash of the transaction for outcome verification.
    pub fn compute_request_hash(txn: &TxnRequest) -> u64 {
        let mut hasher = XxHash64::with_seed(0);

        // Hash compares
        for compare in &txn.compare {
            compare.key.hash(&mut hasher);
        }

        // Hash operations
        for op in &txn.success {
            hash_request_op(op, &mut hasher);
        }
        for op in &txn.failure {
            hash_request_op(op, &mut hasher);
        }

        hasher.finish()
    }
}

fn hash_request_op<H: Hasher>(op: &RequestOp, hasher: &mut H) {
    match op {
        RequestOp::Range(req) => {
            0u8.hash(hasher);
            req.key.hash(hasher);
        }
        RequestOp::Put(req) => {
            1u8.hash(hasher);
            req.key.hash(hasher);
            req.value.hash(hasher);
        }
        RequestOp::DeleteRange(req) => {
            2u8.hash(hasher);
            req.key.hash(hasher);
        }
        RequestOp::Txn(nested) => {
            3u8.hash(hasher);
            for compare in &nested.compare {
                compare.key.hash(hasher);
            }
        }
    }
}

/// Cached transaction outcome for idempotent replay.
#[derive(Debug, Clone)]
pub struct TxnCachedOutcome {
    /// Whether the transaction succeeded.
    pub succeeded: bool,
    /// Revision at which the transaction was executed.
    pub revision: i64,
    /// Hash of the original request for verification.
    pub request_hash: u64,
}

// ============================================================================
// Response Construction (Task 109)
// ============================================================================

impl TxnResponse {
    /// Create a new transaction response.
    pub fn new(header: ResponseHeader, succeeded: bool, responses: Vec<ResponseOp>) -> Self {
        Self {
            header,
            succeeded,
            responses,
        }
    }

    /// Create a response from executor result.
    pub fn from_result(ctx: &TxnExecutorContext, result: &TxnExecutorResult) -> Self {
        Self {
            header: ctx.make_header(),
            succeeded: result.succeeded,
            responses: Vec::new(), // Operations need to be executed to get responses
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_kv(key: &[u8], value: &[u8], version: i64, mod_rev: i64, create_rev: i64) -> KeyValue {
        KeyValue {
            key: key.to_vec(),
            create_revision: create_rev,
            mod_revision: mod_rev,
            version,
            value: value.to_vec(),
            lease: 0,
        }
    }

    #[test]
    fn test_txn_validation_empty() {
        let txn = TxnRequest {
            compare: vec![],
            success: vec![],
            failure: vec![],
        };
        let config = TxnValidationConfig::default();
        assert!(matches!(
            txn.validate(&config),
            Err(TxnValidationError::EmptyTransaction)
        ));
    }

    #[test]
    fn test_txn_validation_too_many_ops() {
        let config = TxnValidationConfig {
            max_operations: 2,
            ..Default::default()
        };

        let txn = TxnRequest {
            compare: vec![],
            success: vec![
                RequestOp::Put(PutRequest {
                    key: b"a".to_vec(),
                    value: vec![],
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                }),
                RequestOp::Put(PutRequest {
                    key: b"b".to_vec(),
                    value: vec![],
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                }),
                RequestOp::Put(PutRequest {
                    key: b"c".to_vec(),
                    value: vec![],
                    lease: 0,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                }),
            ],
            failure: vec![],
        };

        assert!(matches!(
            txn.validate(&config),
            Err(TxnValidationError::TooManyOperations { .. })
        ));
    }

    #[test]
    fn test_txn_all_keys() {
        let txn = TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal,
                target: CompareTarget::Version,
                key: b"key1".to_vec(),
                target_union: CompareTargetUnion::Version(1),
                range_end: vec![],
            }],
            success: vec![RequestOp::Put(PutRequest {
                key: b"key2".to_vec(),
                value: vec![],
                lease: 0,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            })],
            failure: vec![RequestOp::DeleteRange(DeleteRangeRequest {
                key: b"key3".to_vec(),
                range_end: vec![],
                prev_kv: false,
            })],
        };

        let keys = txn.all_keys();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&b"key1".as_slice()));
        assert!(keys.contains(&b"key2".as_slice()));
        assert!(keys.contains(&b"key3".as_slice()));
    }

    #[test]
    fn test_txn_executor_success_branch() {
        let executor = TxnExecutor::new();
        let ctx = TxnExecutorContext::new(100, true);

        let txn = TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal,
                target: CompareTarget::Version,
                key: b"key1".to_vec(),
                target_union: CompareTargetUnion::Version(1),
                range_end: vec![],
            }],
            success: vec![RequestOp::Put(PutRequest {
                key: b"key2".to_vec(),
                value: b"value".to_vec(),
                lease: 0,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            })],
            failure: vec![],
        };

        let lookup = |key: &[u8]| -> Option<KeyValue> {
            if key == b"key1" {
                Some(make_kv(b"key1", b"v1", 1, 50, 10))
            } else {
                None
            }
        };

        let result = executor.execute(&txn, &ctx, lookup).unwrap();
        assert!(result.succeeded);
        assert_eq!(result.operations.len(), 1);
    }

    #[test]
    fn test_txn_executor_failure_branch() {
        let executor = TxnExecutor::new();
        let ctx = TxnExecutorContext::new(100, true);

        let txn = TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal,
                target: CompareTarget::Version,
                key: b"key1".to_vec(),
                target_union: CompareTargetUnion::Version(99), // Won't match
                range_end: vec![],
            }],
            success: vec![],
            failure: vec![RequestOp::Put(PutRequest {
                key: b"key2".to_vec(),
                value: b"fallback".to_vec(),
                lease: 0,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            })],
        };

        let lookup = |key: &[u8]| -> Option<KeyValue> {
            if key == b"key1" {
                Some(make_kv(b"key1", b"v1", 1, 50, 10))
            } else {
                None
            }
        };

        let result = executor.execute(&txn, &ctx, lookup).unwrap();
        assert!(!result.succeeded);
        assert_eq!(result.operations.len(), 1);
    }

    #[test]
    fn test_txn_executor_lin_bound_failure() {
        let executor = TxnExecutor::new();
        let ctx = TxnExecutorContext::new(100, false); // can_linearize = false

        let txn = TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal,
                target: CompareTarget::Version,
                key: b"key1".to_vec(),
                target_union: CompareTargetUnion::Version(1),
                range_end: vec![],
            }],
            success: vec![],
            failure: vec![],
        };

        let lookup = |_: &[u8]| -> Option<KeyValue> { None };

        let result = executor.execute(&txn, &ctx, lookup);
        assert!(matches!(
            result,
            Err(LatticeError::LinearizabilityUnavailable { .. })
        ));
    }

    #[test]
    fn test_txn_idempotency_key_hash() {
        let txn1 = TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal,
                target: CompareTarget::Version,
                key: b"key1".to_vec(),
                target_union: CompareTargetUnion::Version(1),
                range_end: vec![],
            }],
            success: vec![],
            failure: vec![],
        };

        let txn2 = TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal,
                target: CompareTarget::Version,
                key: b"key1".to_vec(),
                target_union: CompareTargetUnion::Version(1),
                range_end: vec![],
            }],
            success: vec![],
            failure: vec![],
        };

        let hash1 = TxnIdempotencyKey::compute_request_hash(&txn1);
        let hash2 = TxnIdempotencyKey::compute_request_hash(&txn2);
        assert_eq!(hash1, hash2);

        // Different key should give different hash
        let txn3 = TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal,
                target: CompareTarget::Version,
                key: b"key2".to_vec(), // Different key
                target_union: CompareTargetUnion::Version(1),
                range_end: vec![],
            }],
            success: vec![],
            failure: vec![],
        };

        let hash3 = TxnIdempotencyKey::compute_request_hash(&txn3);
        assert_ne!(hash1, hash3);
    }
}
