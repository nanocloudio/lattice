//! etcd adapter error mapping.
//!
//! Per §11.2, Lattice errors map to gRPC status codes:
//! - dirty_epoch → FAILED_PRECONDITION
//! - LinearizabilityUnavailable → UNAVAILABLE
//! - TxnCrossShardUnsupported → FAILED_PRECONDITION

use crate::core::error::{LatticeError, LinearizabilityFailureReason};

/// Convert a LatticeError to an etcd-compatible error response.
///
/// This implements the error mapping per §11.2.
pub fn to_etcd_error(err: &LatticeError) -> EtcdError {
    match err {
        LatticeError::DirtyEpoch {
            expected_epoch,
            observed_epoch,
        } => EtcdError {
            code: GrpcCode::FailedPrecondition,
            message: format!(
                "routing epoch mismatch: expected {}, observed {}",
                expected_epoch, observed_epoch
            ),
            details: Some(EtcdErrorDetails::DirtyEpoch {
                expected_epoch: *expected_epoch,
                observed_epoch: *observed_epoch,
            }),
        },

        LatticeError::LinearizabilityUnavailable { reason } => EtcdError {
            code: GrpcCode::Unavailable,
            message: format!("linearizability unavailable: {}", reason),
            details: Some(EtcdErrorDetails::LinearizabilityUnavailable { reason: *reason }),
        },

        LatticeError::TxnCrossShardUnsupported => EtcdError {
            code: GrpcCode::FailedPrecondition,
            message: "transaction spans multiple shards".to_string(),
            details: Some(EtcdErrorDetails::TxnCrossShardUnsupported),
        },

        LatticeError::ThrottleEnvelope { message } => EtcdError {
            code: GrpcCode::ResourceExhausted,
            message: format!("throttled: {}", message),
            details: None,
        },

        LatticeError::RevisionCompacted {
            revision,
            compaction_floor,
        } => EtcdError {
            code: GrpcCode::OutOfRange,
            message: format!(
                "revision {} has been compacted; current compaction floor is {}",
                revision, compaction_floor
            ),
            details: None,
        },

        LatticeError::LeaseNotFound { lease_id } => EtcdError {
            code: GrpcCode::NotFound,
            message: format!("lease {} not found", lease_id),
            details: None,
        },

        LatticeError::KeyNotFound => EtcdError {
            code: GrpcCode::NotFound,
            message: "key not found".to_string(),
            details: None,
        },

        LatticeError::PermissionDenied { message } => EtcdError {
            code: GrpcCode::PermissionDenied,
            message: message.clone(),
            details: None,
        },

        LatticeError::AuthenticationRequired => EtcdError {
            code: GrpcCode::Unauthenticated,
            message: "authentication required".to_string(),
            details: None,
        },

        LatticeError::AuthenticationFailed { message } => EtcdError {
            code: GrpcCode::Unauthenticated,
            message: message.clone(),
            details: None,
        },

        LatticeError::ControlPlaneUnavailable { message } => EtcdError {
            code: GrpcCode::Unavailable,
            message: message.clone(),
            details: None,
        },

        LatticeError::InvalidRequest { message } => EtcdError {
            code: GrpcCode::InvalidArgument,
            message: message.clone(),
            details: None,
        },

        LatticeError::Internal { message } => EtcdError {
            code: GrpcCode::Internal,
            message: message.clone(),
            details: None,
        },

        LatticeError::PartitionQuarantined { kpg_id } => EtcdError {
            code: GrpcCode::Unavailable,
            message: format!("partition {} is quarantined", kpg_id),
            details: None,
        },
    }
}

/// gRPC status codes used by etcd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcCode {
    Ok,
    Cancelled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

/// etcd error response.
#[derive(Debug, Clone)]
pub struct EtcdError {
    /// gRPC status code.
    pub code: GrpcCode,
    /// Error message.
    pub message: String,
    /// Structured error details (if any).
    pub details: Option<EtcdErrorDetails>,
}

/// Structured error details for etcd responses.
#[derive(Debug, Clone)]
pub enum EtcdErrorDetails {
    /// dirty_epoch details.
    DirtyEpoch {
        expected_epoch: u64,
        observed_epoch: u64,
    },
    /// LinearizabilityUnavailable details.
    LinearizabilityUnavailable {
        reason: LinearizabilityFailureReason,
    },
    /// TxnCrossShardUnsupported marker.
    TxnCrossShardUnsupported,
}
