//! Error types and adapter-specific mapping.
//!
//! Lattice defines common error conditions that map to protocol-specific errors
//! per the specification (§11). Error identifiers marked as "stable string constant"
//! are part of the API contract.

use thiserror::Error;

/// Common Lattice error conditions.
///
/// These errors map to adapter-specific protocol errors per §11.2.
#[derive(Debug, Error)]
pub enum LatticeError {
    /// Request references an older kv_epoch/routing epoch or is routed to the wrong KPG.
    #[error("dirty_epoch: expected {expected_epoch}, observed {observed_epoch}")]
    DirtyEpoch {
        expected_epoch: u64,
        observed_epoch: u64,
    },

    /// LIN-BOUND failure: linearizability unavailable.
    ///
    /// This is a stable string constant per §11 error identifier stability policy.
    #[error("LinearizabilityUnavailable: {reason}")]
    LinearizabilityUnavailable {
        reason: LinearizabilityFailureReason,
    },

    /// Backpressure from Clustor flow control or per-tenant quotas.
    #[error("ThrottleEnvelope: {message}")]
    ThrottleEnvelope { message: String },

    /// Transaction spans multiple KPGs, which is not supported in v0.1.
    ///
    /// This is a stable string constant per §11 error identifier stability policy.
    #[error("TxnCrossShardUnsupported")]
    TxnCrossShardUnsupported,

    /// Requested revision has been compacted.
    #[error("revision {revision} has been compacted; compaction floor is {compaction_floor}")]
    RevisionCompacted {
        revision: u64,
        compaction_floor: u64,
    },

    /// Lease not found.
    #[error("lease {lease_id} not found")]
    LeaseNotFound { lease_id: i64 },

    /// Key not found.
    #[error("key not found")]
    KeyNotFound,

    /// Permission denied.
    #[error("permission denied: {message}")]
    PermissionDenied { message: String },

    /// Authentication required.
    #[error("authentication required")]
    AuthenticationRequired,

    /// Authentication failed.
    #[error("authentication failed: {message}")]
    AuthenticationFailed { message: String },

    /// Control plane unavailable.
    #[error("control plane unavailable: {message}")]
    ControlPlaneUnavailable { message: String },

    /// Invalid request.
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },

    /// Internal error.
    #[error("internal error: {message}")]
    Internal { message: String },

    /// Partition is quarantined and unavailable.
    #[error("partition quarantined: {kpg_id}")]
    PartitionQuarantined { kpg_id: String },
}

/// Reason for linearizability failure, aligned with Clustor error taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinearizabilityFailureReason {
    /// Control plane cache is stale or expired.
    ControlPlaneUnavailable,
    /// Clustor strict fallback is active.
    StrictFallback,
    /// ReadIndex proof mismatch or unavailable.
    ProofMismatch,
    /// Not the leader for this KPG.
    NotLeader,
}

impl std::fmt::Display for LinearizabilityFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ControlPlaneUnavailable => write!(f, "ControlPlaneUnavailable"),
            Self::StrictFallback => write!(f, "StrictFallback"),
            Self::ProofMismatch => write!(f, "ProofMismatch"),
            Self::NotLeader => write!(f, "NotLeader"),
        }
    }
}

impl LatticeError {
    /// Create a LinearizabilityUnavailable error with the given reason.
    pub fn linearizability_unavailable(reason: LinearizabilityFailureReason) -> Self {
        Self::LinearizabilityUnavailable { reason }
    }

    /// Create a DirtyEpoch error.
    pub fn dirty_epoch(expected: u64, observed: u64) -> Self {
        Self::DirtyEpoch {
            expected_epoch: expected,
            observed_epoch: observed,
        }
    }

    /// Create a ThrottleEnvelope error.
    pub fn throttle(message: impl Into<String>) -> Self {
        Self::ThrottleEnvelope {
            message: message.into(),
        }
    }

    /// Check if this error indicates the operation should be retried.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::LinearizabilityUnavailable { .. } | Self::ThrottleEnvelope { .. }
        )
    }

    /// Check if this error is a stable string constant.
    pub fn is_stable_identifier(&self) -> bool {
        matches!(
            self,
            Self::LinearizabilityUnavailable { .. } | Self::TxnCrossShardUnsupported
        )
    }
}

/// Result type using LatticeError.
pub type LatticeResult<T> = Result<T, LatticeError>;

// ============================================================================
// Adapter-specific error mapping
// ============================================================================

/// gRPC status codes for etcd v3 adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcCode {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

/// Trait for mapping LatticeError to adapter-specific error representations.
pub trait AdapterErrorMapping {
    /// The adapter-specific error type.
    type Error;

    /// Map a LatticeError to the adapter-specific error type.
    fn map_error(error: &LatticeError) -> Self::Error;
}

/// etcd v3 gRPC error mapping.
pub struct EtcdErrorMapping;

impl EtcdErrorMapping {
    /// Map a LatticeError to a gRPC status code per §11.2.
    pub fn to_grpc_code(error: &LatticeError) -> GrpcCode {
        match error {
            LatticeError::DirtyEpoch { .. } => GrpcCode::FailedPrecondition,
            LatticeError::LinearizabilityUnavailable { .. } => GrpcCode::Unavailable,
            LatticeError::ThrottleEnvelope { .. } => GrpcCode::ResourceExhausted,
            LatticeError::TxnCrossShardUnsupported => GrpcCode::FailedPrecondition,
            LatticeError::RevisionCompacted { .. } => GrpcCode::OutOfRange,
            LatticeError::LeaseNotFound { .. } => GrpcCode::NotFound,
            LatticeError::KeyNotFound => GrpcCode::NotFound,
            LatticeError::PermissionDenied { .. } => GrpcCode::PermissionDenied,
            LatticeError::AuthenticationRequired => GrpcCode::Unauthenticated,
            LatticeError::AuthenticationFailed { .. } => GrpcCode::Unauthenticated,
            LatticeError::ControlPlaneUnavailable { .. } => GrpcCode::Unavailable,
            LatticeError::InvalidRequest { .. } => GrpcCode::InvalidArgument,
            LatticeError::Internal { .. } => GrpcCode::Internal,
            LatticeError::PartitionQuarantined { .. } => GrpcCode::Unavailable,
        }
    }

    /// Get a structured error message suitable for etcd clients.
    pub fn to_error_message(error: &LatticeError) -> String {
        match error {
            LatticeError::DirtyEpoch {
                expected_epoch,
                observed_epoch,
            } => {
                format!(
                    "etcdserver: request epoch mismatch (expected={}, got={})",
                    expected_epoch, observed_epoch
                )
            }
            LatticeError::LinearizabilityUnavailable { reason } => {
                format!("etcdserver: linearizability unavailable: {}", reason)
            }
            LatticeError::TxnCrossShardUnsupported => {
                "etcdserver: transaction spans multiple partitions".to_string()
            }
            LatticeError::RevisionCompacted {
                revision,
                compaction_floor,
            } => {
                format!(
                    "etcdserver: mvcc: required revision has been compacted (revision={}, floor={})",
                    revision, compaction_floor
                )
            }
            LatticeError::LeaseNotFound { lease_id } => {
                format!("etcdserver: lease not found (id={})", lease_id)
            }
            _ => error.to_string(),
        }
    }
}

/// Future: Redis RESP error mapping placeholder.
pub struct RedisErrorMapping;

impl RedisErrorMapping {
    /// Map a LatticeError to a Redis error prefix.
    pub fn to_error_prefix(error: &LatticeError) -> &'static str {
        match error {
            LatticeError::KeyNotFound => "WRONGTYPE",
            LatticeError::PermissionDenied { .. } => "NOPERM",
            LatticeError::AuthenticationRequired => "NOAUTH",
            LatticeError::ThrottleEnvelope { .. } => "BUSY",
            _ => "ERR",
        }
    }
}

/// Future: Memcached error mapping placeholder.
pub struct MemcachedErrorMapping;

impl MemcachedErrorMapping {
    /// Map a LatticeError to a Memcached status code.
    pub fn to_status_code(error: &LatticeError) -> u16 {
        match error {
            LatticeError::KeyNotFound => 0x0001,            // Key not found
            LatticeError::InvalidRequest { .. } => 0x0004,  // Invalid arguments
            LatticeError::AuthenticationRequired => 0x0020, // Auth error
            _ => 0x0081,                                    // Unknown command (generic error)
        }
    }
}
